# Recovery Scheduler Productionization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn Stratum's existing bounded durable recovery scheduler into an explicitly configurable, observable, shutdown-safe production component while preserving manual recovery and existing persisted claim/lease fencing.

**Architecture:** Keep the scheduler in `src/server/mod.rs` as the bounded worker coordinator for pre-visibility, post-CAS, durable FS mutation, and non-destructive object cleanup phases. Add runtime/server config that controls enabled posture, tick limits, interval, lease duration, and shutdown drain timeout; expose that posture through `GET /vcs/recovery`; and prove concurrent workers are safe through the existing durable recovery claim stores instead of adding a distributed lock. Server startup keeps one in-process scheduler handle per store/repo, while multi-node correctness is demonstrated by concurrent direct ticks using persisted claim/lease fencing.

**Tech Stack:** Rust 2024, Tokio, Axum, Serde JSON, existing durable recovery workers/stores, Postgres feature-gated adapters and migrations, `stratum_server` graceful shutdown, existing recovery route tests, and current live-gate scripts.

---

## Required Context

- `markdownfs_v2_cto_architecture_plan.md` defines v2 as a durable cloud storage/control layer and keeps Postgres metadata plus object storage as the production backend.
- `docs/plans/2026-05-15-backend-roadmap.md` Slice 5 requires lifecycle, drain, status, and multi-node-safe controls for the existing scheduler.
- `docs/project-status.md` says scheduler status already reports started time, last tick, redacted outcome/error, and phase counters, but there are no hosted shutdown/control-plane commands.
- `docs/http-api-guide.md` already documents `GET /vcs/recovery` and `POST /vcs/recovery/run` as bounded, redacted, admin-only operator surfaces.
- `extract pieces.md` accepts SMFS bounded drain/status wording and rejects SMFS latest-wins queues, sibling error files, Mirage process-local job storage, and unauthenticated shutdown controls.
- Previous recovery plans require recovery state to stay keyed by repo/ref/operation/idempotency/commit identity, not mutable paths, and require claim owner/token/expiry fencing for completion, failure, and poison transitions.

## Current Baseline

- `src/server/mod.rs` starts a scheduler automatically when guarded durable stores or durable-core stores are attached.
- The scheduler runs phases in order: pre-visibility, post-CAS, durable FS mutation, then object cleanup readiness.
- It deduplicates in-process startup by store pointer identity plus repo id.
- It uses fixed constants: 5 second interval, tick limit 10, lease duration 30 seconds, and fixed lease owners.
- `GET /vcs/recovery` exposes scheduler status only when a handle is attached.
- `POST /vcs/recovery/run` remains the bounded manual operator drain route.
- Object cleanup is non-destructive by default and reports `deleted_final_objects: 0`.

## Required Skills And Agent Discipline

Implementation and review subagents must be instructed to use these Rust skills before touching or reviewing code:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

The main session owns integration, local review, verification, commits, merges, and pushes. Subagents may implement or review scoped tasks, but the main session must inspect diffs, fix integration issues, and rerun gates before accepting work. When waiting on long tests or subagents, use long waits or sleep intervals rather than tight polling to preserve context and rate limits.

## Non-Goals

- Do not enable broad durable-cloud defaults or remove guarded runtime gates.
- Do not expose destructive object deletion controls.
- Do not add broad unreachable commit/object deletion.
- Do not add Redis, Postgres advisory locks, or another distributed lock service.
- Do not schedule idempotency retention sweeps in this slice.
- Do not change local-state runtime behavior.
- Do not make durable-cloud recovery operator routes supported unless explicitly changed and documented; current fail-closed behavior should remain.

## Design Decisions

- Add explicit scheduler env/config controls with conservative defaults:
  - `STRATUM_RECOVERY_SCHEDULER=enabled|disabled` defaults to `enabled` when guarded durable/durable-core stores exist.
  - `STRATUM_RECOVERY_SCHEDULER_INTERVAL_MS` defaults to `5000`.
  - `STRATUM_RECOVERY_SCHEDULER_TICK_LIMIT` defaults to `10` and caps at `100`.
  - `STRATUM_RECOVERY_SCHEDULER_LEASE_MS` defaults to `30000`.
  - `STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN=enabled|disabled` defaults to `disabled`.
  - `STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS` defaults to a small bounded timeout, for example `2500`, and caps at `30000`.
- Attach a scheduler handle even when disabled if durable stores are configured, but do not spawn a background loop. This lets `GET /vcs/recovery` report `enabled: false`, `state: "disabled"`, and redacted config posture while manual `POST /vcs/recovery/run` remains available.
- Preserve existing response fields and add status fields rather than replacing the current JSON shape.
- Keep one process-local scheduler handle per store/repo. Config is first-writer-wins for a live in-process handle; startup should log/report reuse without spawning another loop.
- Shutdown drain requests stop the background loop, mark drain state observable, run bounded immediate ticks until no due work is attempted or timeout expires, and record `completed`, `timed_out`, or `failed` without blocking process exit beyond the configured timeout.
- Multi-node safety is proved by concurrent ticks that bypass the in-process registry and race on the same persisted/in-memory recovery stores. The expected protection is existing claim owner/token/expiry and idempotent completion semantics.

## Task 1: Plan Document

**Files:**
- Create: `docs/plans/2026-05-19-recovery-scheduler-productionization.md`

**Step 1: Save this plan**

Write this file before implementation.

**Step 2: Verify the docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-19-recovery-scheduler-productionization.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 3: Commit**

```bash
git add docs/plans/2026-05-19-recovery-scheduler-productionization.md
git commit -m "docs: plan recovery scheduler productionization"
```

## Task 2: Runtime And Server Scheduler Config

**Files:**
- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing config tests**

Add tests proving:

- default durable guarded startup has scheduler config `enabled`;
- `STRATUM_RECOVERY_SCHEDULER=disabled` parses and does not spawn a background loop;
- invalid scheduler mode fails with a fixed redacted message;
- zero/oversized interval, tick limit, lease duration, and shutdown timeout are rejected without echoing raw env values;
- `Debug` for runtime config redacts sensitive adjacent settings and does not include DB URLs, secrets, endpoints, object keys, raw SQL, or raw token material.

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected before implementation: new scheduler config tests fail because no config exists.

**Step 2: Add runtime config types**

In `src/backend/runtime.rs`, add:

```rust
pub const RECOVERY_SCHEDULER_ENV: &str = "STRATUM_RECOVERY_SCHEDULER";
pub const RECOVERY_SCHEDULER_INTERVAL_MS_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_INTERVAL_MS";
pub const RECOVERY_SCHEDULER_TICK_LIMIT_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_TICK_LIMIT";
pub const RECOVERY_SCHEDULER_LEASE_MS_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_LEASE_MS";
pub const RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV: &str =
    "STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN";
pub const RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV: &str =
    "STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySchedulerMode {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySchedulerRuntimeConfig {
    mode: RecoverySchedulerMode,
    interval: Duration,
    tick_limit: usize,
    lease_duration: Duration,
    shutdown_drain_enabled: bool,
    shutdown_drain_timeout: Duration,
}
```

Add `RecoverySchedulerRuntimeConfig::from_lookup`, getters, validation helpers, and a `BackendRuntimeConfig::recovery_scheduler()` getter. Use existing runtime parsing style and fixed messages such as `invalid STRATUM_RECOVERY_SCHEDULER; expected enabled or disabled`.

**Step 3: Thread config into server stores**

Add a server-side scheduler config to `ServerStores` and `ServerRouterConfig`. `ServerStores::open_local` should carry a disabled/default config but no durable stores. Durable store opening should copy `runtime.recovery_scheduler()` into `ServerStores`.

**Step 4: Verify**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
git diff --check
```

## Task 3: Scheduler Lifecycle, Disabled Posture, And Status JSON

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_capabilities.rs` only if existing manifest fixtures require scheduler status wording updates.
- Test: `src/server/mod.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing lifecycle/status tests**

Add or extend tests proving:

- disabled scheduler returns an attached status with `enabled: false`, `state: "disabled"`, no task, and no background tick after a short wait;
- enabled scheduler reports `enabled: true`, `state: "running"`, interval, tick limit, lease duration, shutdown drain config, and last tick fields;
- duplicate startup for the same store/repo returns the same live handle and does not spawn a second loop;
- startup for the same store set but different repo id is distinct;
- status JSON includes the new fields and preserves existing `present`, `started_at_millis`, `last_tick_at_millis`, `last_outcome`, `last_error`, and `phases` fields;
- no status field leaks raw backend errors, URLs, endpoints, object keys, SQL, idempotency keys, tokens, or commit messages.

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

Expected before implementation: tests fail because the scheduler cannot be disabled or report config posture.

**Step 2: Replace fixed constants with handle config**

Keep default constants, but move runtime values into a `DurableRecoverySchedulerConfig` used by:

- `start_durable_recovery_scheduler_for_repo`;
- `durable_recovery_scheduler_tick`;
- status constructors;
- tests that drive direct ticks.

The tick must still apply one total bounded limit across phases by subtracting attempted work from the remaining limit.

**Step 3: Expand handle/status types**

Extend `DurableRecoverySchedulerHandle` and `DurableRecoverySchedulerStatus` with fields equivalent to:

```rust
enabled: bool,
state: "disabled" | "running" | "draining" | "stopped",
interval_millis: u64,
tick_limit: usize,
lease_millis: u64,
shutdown_drain_enabled: bool,
shutdown_drain_timeout_millis: u64,
last_tick_started_at_millis: Option<u64>,
last_tick_completed_at_millis: Option<u64>,
last_tick_duration_millis: Option<u64>,
shutdown_drain: Option<DurableRecoverySchedulerDrainStatus>,
```

Use a Rust enum for internal state and serialize to fixed lowercase strings in `routes_vcs.rs`. Keep raw errors redacted to fixed markers already used by phase failures.

**Step 4: Preserve manual run**

Do not gate `POST /vcs/recovery/run` on scheduler enabled state. The route should continue to use guarded durable route availability and admin authorization exactly as today.

**Step 5: Verify**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
git diff --check
```

## Task 4: Bounded Shutdown Drain

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/bin/stratum_server.rs`
- Modify: `tests/server_startup.rs`
- Test: `src/server/mod.rs`

**Step 1: Write failing shutdown tests**

Add tests proving:

- `request_shutdown_drain()` on a disabled scheduler records a skipped/disabled drain and returns quickly;
- enabled drain stops or pauses the background loop, marks `state: "draining"`, runs bounded immediate ticks, then marks `state: "stopped"` or resumes only if explicitly requested by test helper;
- timeout records `timed_out: true` and does not hang beyond the configured timeout;
- drain status includes started/completed/timeout timestamps and last outcome without raw backend errors;
- `stratum_server` wires the shutdown future to call bounded drain when the config enables it, before local-state save, and still exits when drain times out.

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected before implementation: shutdown drain APIs do not exist.

**Step 2: Add cooperative stop/drain primitives**

Use a small coordinator inside `DurableRecoverySchedulerHandle`:

- a shutdown flag or `watch` channel observed by the background loop;
- a tick mutex so drain and background ticks do not overlap;
- a `Notify` or abort-safe task stop path;
- an async `request_shutdown_drain()` method that wraps drain work in `tokio::time::timeout`.

Drain loop:

1. Mark drain requested.
2. Stop the background loop or prevent another tick from starting.
3. Repeatedly run immediate ticks with the configured tick limit until a tick attempts zero work or timeout fires.
4. Record `completed`, `timed_out`, or `failed` using fixed redacted outcomes.

**Step 3: Wire binary shutdown**

Change `stratum_server` to keep a clone of the optional scheduler handle when building the app. On Ctrl-C, call the bounded drain only when `shutdown_drain_enabled` is true, then let Axum graceful shutdown proceed and finally save local state as today.

If the router build helpers need to preserve their public shape, add new helper functions returning a small built-app struct while keeping existing helper wrappers for tests.

**Step 4: Verify**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
git diff --check
```

## Task 5: Multi-Node Safety Proof With Existing Fencing

**Files:**
- Modify: `src/server/mod.rs`
- Modify as needed for test seams only: `src/backend/core_transaction.rs`
- Test: `src/server/mod.rs`
- Test: `src/backend/core_transaction.rs`
- Test: `src/backend/object_cleanup.rs`

**Step 1: Write failing/strengthening tests**

Add tests proving:

- two concurrent scheduler ticks against the same stores do not duplicate durable FS mutation audit effects;
- two concurrent post-CAS workers or scheduler ticks cannot both complete the same post-CAS target;
- concurrent cleanup ticks keep object cleanup non-destructive and do not delete final object bytes;
- poisoned/backing-off rows are counted and do not starve claimable rows within the configured bounded limit;
- a different lease owner/token cannot complete, fail, or poison another worker's active claim.

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
```

Expected: some existing fencing tests pass already; new direct concurrent scheduler proof tests fail or are absent before implementation.

**Step 2: Add direct tick test seam**

Expose a crate-private test helper, not a public operator API, that can run one configured scheduler tick without using the process-global registry. Use it only to simulate separate nodes racing over shared stores.

**Step 3: Preserve existing lease/fencing model**

Do not add locks. If a duplicate side effect appears, fix the relevant worker/store idempotency or test setup so claim owner/token/expiry and completion checks are the correctness boundary.

**Step 4: Verify**

Run the focused commands from Step 1 and:

```bash
git diff --check
```

## Task 6: Docs And Operator Guidance

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify if needed: `docs/plans/2026-05-15-backend-roadmap.md`

**Step 1: Update HTTP guide**

Document:

- scheduler env controls and defaults;
- disabled mode behavior;
- manual recovery run availability when disabled;
- shutdown drain semantics and timeout wording;
- status JSON additions;
- multi-node safety statement: persisted claims/fencing, not distributed locks;
- object cleanup remains non-destructive by default;
- durable-cloud unsupported recovery operator routes remain fail-closed if unchanged.

**Step 2: Update project status**

Record what landed, what remains out of scope, verification commands, and local live-gate skip/provider-verified wording. Do not claim fresh live Postgres/R2 runs unless credentials are present and commands actually run.

**Step 3: Verify docs**

Run:

```bash
git diff --check
```

## Task 7: Review Fixes, Full Gates, Merge, Push

**Files:**
- Modify as needed after reviews.

**Step 1: Spec/correctness review**

Dispatch a review subagent with the plan, acceptance criteria, and diff. It must look for under-built lifecycle controls, shutdown drain gaps, multi-node proof gaps, manual route regressions, durable-cloud fail-closed regressions, and redaction issues.

**Step 2: Code-quality/security review**

Dispatch a separate review subagent. It must look for async shutdown hangs, task leaks, global registry races, brittle timing tests, public API churn, raw-error leakage, excessive status output, and lock misuse.

**Step 3: Fix findings locally**

The main session owns fixes and must inspect all diffs before accepting subagent output.

**Step 4: Run required gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Only run live Postgres/R2 gates if credentials are present locally. Otherwise record that local live gates skipped and protected-main CI has provider-verified green live gates.

**Step 5: Commit and push**

Keep commits small:

1. plan doc;
2. runtime/server config;
3. lifecycle/status;
4. shutdown drain;
5. multi-node proof and docs/review fixes if not already split.

Push `v2/foundation`, merge to `main` using a temporary clean worktree if local main remains dirty, then push `main`.
