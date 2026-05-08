# Bounded Guarded Commit Repair Worker Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a bounded, operator-triggered repair worker that claims persisted guarded durable commit post-CAS recovery rows and completes only the side effects it can repair from persisted, fenced context.

**Architecture:** Keep broad durable core routing fail-closed and build the worker at the existing guarded commit seam. Extend recovery claims with redacted, durable repair context for workspace-head, audit, and idempotency work; then add a bounded admin control endpoint that claims due rows, repairs one step at a time under lease fencing, records retry/backoff or poison, and never guesses route state from `commit_id` alone.

**Tech Stack:** Rust, Tokio, Axum, existing durable commit transaction types, `DurableCorePostCasRecoveryClaimStore`, Postgres migrations/adapters behind the `postgres` feature, in-memory contract tests, route tests, and `/usr/bin/time -l` release perf.

---

## Required Skills

Implementation and review subagents must use:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Use TDD for behavior changes: write the failing test first, run it, then implement.

## CTO Plan And Status Read

The CTO plan keeps Rust as the core for durable filesystem, VCS, object storage, FUSE, CLI, and correctness-sensitive concurrency. Current `docs/project-status.md` says guarded durable `POST /vcs/commit` can persist post-CAS recovery claims and expose `GET /vcs/recovery`, but no repair worker loop, wakeup/drain control, or post-restart envelope reconstruction exists yet.

`extract pieces.md` says SMFS is useful for claim/finalize/backoff/poison queue semantics and bounded worker/wakeup shape. Do not copy SMFS latest-wins filepath queues. Stratum recovery must stay keyed by repo/ref/commit/step identity, preserve immutable commit visibility semantics, and avoid duplicate audit/workspace/idempotency effects.

## Scope

In scope:

- Add persisted repair context to post-CAS recovery claims, likely with a migration `0004_guarded_commit_recovery_context.sql`.
- Extend `DurableCorePostCasRecoveryClaimStore` so claims can carry optional repair context.
- Keep existing no-context rows inspectable; the worker must not falsely repair them. It may poison or back off unsupported no-context rows with redacted diagnostics.
- Add a bounded repair worker in `src/backend/core_transaction.rs`, near `DurableCorePostCasRecoveryTarget`, `DurableCorePostCasRecoveryClaimStore`, and `DurableCoreCommitPostCasEnvelope`.
- Add worker support for:
  - workspace-head update using the persisted workspace id and expected-head fence;
  - audit append using persisted `NewAuditEvent`, with an idempotence check before append;
  - idempotency completion using persisted reservation identity and an explicit response kind: full commit response or redacted partial response.
- Add route wiring in `src/server/routes_vcs.rs` beside `GET /vcs/recovery`, likely `POST /vcs/recovery/run`, admin-only and guarded by `state.core.guarded_durable_commit_route()`.
- Keep route execution bounded by request limit, lease duration, backoff, and explicit summary response.
- Preserve `STRATUM_CORE_RUNTIME=durable-cloud` fail-closed behavior.
- Update `docs/project-status.md` after implementation and verification.

Out of scope:

- No broad durable filesystem/VCS serving cutover.
- No durable auth/session source path.
- No background daemon scheduler, long-lived worker pool, or automatic startup drain.
- No pre-visibility recovery queue persistence.
- No guessing workspace id, audit actor, idempotency reservation, or response body from commit metadata alone.
- No final-object deletion or distributed lock service.
- No web UI.

## Design Constraints

- A recovery row with only `repo_id`, `main`, `commit_id`, and `step` is not a repair envelope. It must not be used to replay audit, workspace, or idempotency work by inference.
- Workspace repair must use `WorkspaceMetadataStore::update_head_commit_if_current` and treat already-advanced workspace heads as non-rollback success, matching the existing envelope behavior.
- Audit repair must not blindly append after an ack-loss failure. Add a narrow audit-store idempotence check for the VCS commit audit resource before append, then complete the claim if the event is already present.
- Idempotency repair must not overwrite an existing partial replay with a later full `200` replay. Persist an explicit idempotency recovery response kind for rows that need idempotency repair.
- Worker transitions must be fenced by lease owner, token, and lease expiry. Stale workers cannot complete, fail, or poison.
- All operator and debug output must redact lease tokens, reservation tokens, raw idempotency keys, commit messages in errors, paths, object bytes, R2 keys, and raw Postgres messages.
- Keep async work non-blocking. The repair worker should use async store APIs and must not snapshot or rebuild local filesystem state.

## Acceptance

- `POST /vcs/recovery/run` claims at most the requested bounded number of due rows and returns a redacted summary.
- No-context recovery rows are not falsely completed as repaired.
- Workspace-head recovery completes idempotently under the expected-head fence and does not roll back a newer workspace head.
- Workspace-head recovery enqueues the follow-on audit recovery before completing its own claim, so a crash between workspace completion and audit scheduling can be retried.
- Audit recovery appends at most one VCS commit audit event for the commit even across ack-loss and retry.
- Idempotency recovery can complete either the original full commit response or the redacted partial response, based only on persisted response kind.
- A stale lease owner/token cannot complete, fail, or poison a retry.
- Non-admin users cannot run recovery. Disabled guarded durable capability returns fail-closed.
- `STRATUM_CORE_RUNTIME=durable-cloud` remains fail-closed.
- Perf remains in the current warm release band.

## Implementation Result

Landed on `v2/foundation` as a staged slice:

- `08710cb` - plan guarded commit repair worker
- `e2511c8` - add bounded post-cas repair worker core
- `c7eeca7` - persist post-cas recovery context
- `3847e00` - add idempotent repair primitives
- `a47c6f6` - repair post-cas completion claims
- `677b6d5` - wire guarded recovery run route

The final implementation keeps no-context rows inspectable but unsupported for repair, persists route-bound context for workspace/audit/idempotency repairs, adds idempotent audit and idempotency repair primitives, runs the bounded worker through admin-only `POST /vcs/recovery/run`, and keeps broad durable core runtime fail-closed.

Review found and fixed two route-level issues: direct idempotency-completion failures must replay the same redacted partial response returned to the caller, and guarded post-CAS side effects must use the same guarded capability store bundle as the repair worker. Final re-review found no blockers.

## Task 1: Plan Commit

**Files:**

- Create: `docs/plans/2026-05-07-bounded-guarded-commit-repair-worker.md`

**Steps:**

1. Save this plan.
2. Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

3. Commit:

```bash
git add docs/plans/2026-05-07-bounded-guarded-commit-repair-worker.md
git commit -m "docs: plan guarded commit repair worker"
```

## Task 2: Add RED Core Repair Tests

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Tests to add first:**

- `durable_core_commit_post_cas_repair_worker::repair_worker_missing_context_does_not_repair_side_effects`
- `durable_core_commit_post_cas_repair_worker::repair_worker_workspace_step_updates_head_and_enqueues_audit_before_completing_claim`
- `durable_core_commit_post_cas_repair_worker::repair_worker_audit_step_is_idempotent_after_ack_loss`
- `durable_core_commit_post_cas_repair_worker::repair_worker_idempotency_step_uses_persisted_partial_response_kind`
- `durable_core_commit_post_cas_repair_worker::repair_worker_stale_claim_token_cannot_finalize_retry`
- `durable_core_commit_post_cas_repair_worker::repair_worker_summary_debug_redacts_private_context`

Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_repair_worker --lib -- --nocapture
```

Expected before implementation: tests fail because repair context and worker APIs do not exist.

## Task 3: Persist Repair Context On Claims

**Files:**

- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Create: `migrations/postgres/0004_guarded_commit_recovery_context.sql`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Implementation shape:**

- Add `DurableCorePostCasRecoveryContext` with only the fields needed for repair:
  - `workspace_id: Option<Uuid>`;
  - `expected_workspace_head: Option<String>`;
  - persisted `NewAuditEvent`;
  - optional idempotency context made from scope, key hash, request fingerprint, reservation token, and response kind.
- Add `DurableCorePostCasIdempotencyResponseKind::{FullCommit, Partial}`.
- Add a redacted `Debug` implementation for context types.
- Extend claim entries and statuses so `DurableCorePostCasRecoveryClaim` can expose an optional context to the worker without leaking private fields.
- Add `enqueue_with_context(target, context, now_millis)` while preserving `enqueue(target, now_millis)` for existing no-context rows and tests.
- Add `context_json JSONB` in migration 0004. Context must be nullable so existing rows stay readable and explicitly unsupported by the worker.
- Serialize/deserialize context through the Postgres adapter. Corrupt context must return redacted `CorruptStore`.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
```

Do not claim live Postgres coverage when `STRATUM_POSTGRES_TEST_URL` is unset.

## Task 4: Add Idempotent Audit And Idempotency Repair Primitives

**Files:**

- Modify: `src/audit.rs`
- Modify: `src/idempotency.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/core_transaction.rs`

**Implementation shape:**

- Add a narrow audit-store method that can answer whether a VCS commit audit event for a commit resource is already present.
- Implement it for in-memory, local, and Postgres audit stores by matching `AuditAction::VcsCommit` plus `AuditResourceKind::Commit` and commit id.
- Add a narrow idempotency-store method that completes a pending reservation or treats an already-completed matching replay as success.
- Implement it for in-memory, local, and Postgres idempotency stores without exposing raw idempotency keys.
- Add `IdempotencyReservation::for_store_parts(...)` or equivalent `pub(crate)` constructor so recovery can reconstruct a reservation from persisted hash/token fields after validation.

**Verification:**

```bash
cargo test --locked audit::tests idempotency::tests --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

## Task 5: Implement Bounded Repair Worker

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Implementation shape:**

- Add a bounded worker type/function, for example `DurableCorePostCasRepairWorker`.
- Inputs should be explicit trait references: commits, refs if needed, workspaces, audit, idempotency, recovery store, lease owner, lease duration, now clock, and limit.
- The worker should list bounded actionable rows, claim due targets, and process up to `limit`.
- For missing context, record terminal poison or redacted failure; do not repair.
- For `WorkspaceHeadUpdate`:
  - apply the expected-head fenced workspace update;
  - enqueue `AuditAppend` with the same context before completing the workspace claim;
  - if enqueue fails after workspace update, leave the claim retryable rather than losing the audit follow-up.
- For `AuditAppend`:
  - check whether the commit audit event already exists;
  - append if missing;
  - complete the claim only after append/existence is confirmed.
- For `IdempotencyCompletion`:
  - reconstruct the reservation from persisted idempotency context;
  - use `FullCommit` to build `{ hash, message, author }` from durable commit metadata;
  - use `Partial` for `DurableCoreCommittedResponse::partial_body()`;
  - complete-or-match the replay and then complete the claim.
- On repair failure, call `record_failure` with a bounded redacted diagnostic and bounded backoff. Poison only unsupported or corrupt recovery context.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_repair_worker --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas --lib -- --nocapture
```

## Task 6: Wire Guarded Route Enqueue Context And Run Control

**Files:**

- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/core.rs` only if the capability needs a helper accessor
- Modify: `src/server/mod.rs` only if startup store wiring needs adjustment
- Modify: `docs/http-api-guide.md` if route docs already cover recovery status

**Implementation shape:**

- Build recovery context from the route-bound post-CAS envelope, not from request globals after the fact.
- On `DurableCorePostCasOutcome::Partial`:
  - enqueue failed workspace/audit rows with repair context that excludes final idempotency repair;
  - enqueue `IdempotencyCompletion` with a response kind matching the response the route returns to the caller. In this slice, confirmed post-CAS idempotency failures return `202 Accepted` partial, so they enqueue `Partial` rather than upgrading later replay to a full `200 OK`;
  - if partial replay completion fails, enqueue `IdempotencyCompletion` with `Partial`.
- Add admin-only `POST /vcs/recovery/run`.
- Request shape: optional `limit`, default small, hard cap 100. Ignore caller-supplied lease owner; server owns the worker identity.
- Response shape: redacted summary with attempted, completed, failed/backing_off, poisoned, skipped, and limit.
- Existing `GET /vcs/recovery` remains redacted and bounded.

**Verification:**

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

## Task 7: Add Route And Postgres Contract Coverage

**Files:**

- Modify: `src/server/routes_vcs.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Tests:**

- Non-admin `POST /vcs/recovery/run` is denied.
- Disabled guarded durable commit capability returns `501`.
- Run endpoint repairs an enqueued workspace row and returns a redacted summary.
- Run endpoint does not expose commit message, author token, lease token, reservation token, raw idempotency key, or backend detail.
- Postgres recovery contract round-trips context, claims context under lease, preserves no-context rows, and persists redacted failure/poison.

**Verification:**

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
git diff --check
```

## Task 8: Review, Full Gates, Merge, And Push

Run subagent spec review first, then code-quality/security/correctness review. Fix findings locally after inspecting diffs.

Run v2 gates:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Commit implementation and docs separately where practical. Push `v2/foundation`, merge to `main`, rerun main gates, push `main`.
