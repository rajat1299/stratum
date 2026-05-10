# Recovery Observability And Operator Readiness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn the existing guarded durable recovery state into an admin-only operator surface that answers what is unhealthy, blocked, due, stuck, poisoned, retryable, and still remaining after a bounded recovery run.

**Architecture:** Keep durable runtime semantics unchanged and layer a bounded, redacted observability read model over the existing pre-visibility, post-CAS, durable FS mutation, scheduler, and object-cleanup claim stores. The route remains `GET /vcs/recovery` plus `POST /vcs/recovery/run`, with a stable top-level health/readiness block, phase summaries, per-ref blockers, age/stale classification, cleanup-claim visibility, and redacted run correlation IDs.

**Tech Stack:** Rust, Axum, Tokio, Serde JSON, existing Stratum backend store traits, optional Postgres adapter behind the `postgres` feature.

---

## Reference Material Used

### Required Stratum Context

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

The plan explicitly used these `extract pieces.md` sections:

- `Recovery Observability And Operator Readiness Addendum`
- `Durable recovery/status state machine shape`
- `Observability and session event records`
- `CLI daemon and local control plane`
- `Recommended Reference Order -> recovery/status`

### SMFS/Mirage Files Used

- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/daemon/protocol.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs/src/cmd/status.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs/src/cmd/sync.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs/src/cmd/daemon_runtime.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/sync/push.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/schema.sql`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/db.rs`
- `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/server/src/routers/health.ts`
- `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/core/src/observe/observer.ts`
- `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/core/src/observe/log_entry.ts`
- `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/server/src/routers/execute.ts`

### Accepted Patterns

- SMFS daemon protocol's small scriptable control commands map well to Stratum's `health`, `recovery status`, and `recovery run` operator actions.
- SMFS `status --json` supports a future human/JSON CLI split; this slice keeps JSON stable on the HTTP side and documents human operator wording.
- SMFS bounded drain wording is accepted: report attempted/completed/backing-off/poisoned/skipped/remaining, and never imply convergence when persisted work remains.
- SMFS stuck-age tiering is accepted as classification vocabulary, adapted to Stratum lease/backoff timestamps rather than remote document polling.
- Mirage health route is accepted as a minimal readiness shape: status, uptime, and live subsystem counts.
- Mirage observer self-path suppression is accepted for future `.stratum/status*` and `.stratum/recovery*` virtual views, but only documented here because this slice does not add virtual files.
- Mirage log preview limits are accepted as a strict operator-preview principle; Stratum recovery diagnostics stay redacted and bounded.
- Mirage execute route's job/correlation ID shape is accepted as a support/debug correlation pattern for recovery runs.

### Rejected Patterns

- Reject SMFS latest-wins `push_queue` semantics. Stratum recovery must stay keyed by repo/ref/operation/idempotency/commit identity and preserve auditability.
- Reject SMFS sibling `.smfs-error.txt` files. Stratum must not write raw server rejection bodies into workspaces.
- Reject Mirage process-local `JobTable` as storage. Stratum recovery state must remain persisted and multi-process safe.
- Reject Mirage unauthenticated shutdown controls for hosted Stratum. Any future daemon control must have explicit auth, tenancy, and deployment design.
- Reject Mirage JSONL observer as production audit. Stratum audit remains append-only, durable, authorized, and content-free.

## Current Stratum Baseline

- `GET /vcs/recovery` is admin-only and guarded by `state.core.guarded_durable_commit_route()`.
- Current status includes flat post-CAS `recovery`, `pre_visibility`, and `fs_mutations` rows with bounded `limit: 100`.
- Current status has counts and redacted diagnostics, but no stable top-level readiness block, scheduler last outcome, phase normalization, age/stale classification, ref blockers, or cleanup-claim visibility.
- `POST /vcs/recovery/run` drains pre-visibility, then post-CAS, then FS mutation recovery with the remaining limit, but the top-level response is post-CAS-shaped and does not return correlation ID or remaining-by-phase.
- `ObjectCleanupClaimStore` has transition methods only: `claim`, `complete`, and `record_failure`; it needs bounded read APIs before cleanup claims can be surfaced.
- Scheduler startup deduplicates one loop per store set but does not currently expose last tick/run outcome.

## Non-Goals

- Do not enable broad `STRATUM_CORE_RUNTIME=durable-cloud`.
- Do not change durable fencing, idempotency, ref CAS, or recovery execution semantics.
- Do not implement final-object deletion, unreachable commit GC, or a cleanup worker.
- Do not add web console, FUSE/sparse mount behavior, execution runner, distributed locks, or audit event bus.

## Target HTTP Shape

`GET /vcs/recovery` should continue to preserve existing fields for compatibility while adding these stable fields:

```json
{
  "health": {
    "status": "degraded",
    "backend_mode": "durable",
    "guarded_durable_enabled": true,
    "scheduler": {
      "present": true,
      "last_tick_at_millis": 1778371200000,
      "last_outcome": "completed",
      "last_error": null
    },
    "stores": {
      "post_cas": { "available": true },
      "pre_visibility": { "available": true },
      "fs_mutations": { "available": true },
      "object_cleanup": { "available": true }
    }
  },
  "phases": {
    "pre_visibility": { "counts": {}, "oldest_age_millis": 1200, "rows": [] },
    "post_cas": { "counts": {}, "oldest_age_millis": 900, "rows": [] },
    "fs_mutations": { "counts": {}, "oldest_age_millis": 700, "rows": [] },
    "object_cleanup": { "counts": {}, "oldest_age_millis": 500, "rows": [] }
  },
  "blockers": {
    "refs": [
      {
        "repo_id": "local",
        "ref_name": "main",
        "blocked": true,
        "reason": "poisoned_recovery",
        "phases": ["post_cas"],
        "poisoned": 1,
        "stale_active": 0,
        "retryable": 0,
        "next_retry_at_millis": null
      }
    ],
    "workspaces": []
  },
  "limit": 100
}
```

`POST /vcs/recovery/run` should add:

```json
{
  "correlation_id": "rec_...",
  "requested_limit": 10,
  "attempted": 3,
  "completed": 2,
  "backing_off": 1,
  "poisoned": 0,
  "skipped": 0,
  "remaining": 4,
  "converged": false,
  "phases": {
    "pre_visibility": {},
    "post_cas": {},
    "fs_mutations": {},
    "object_cleanup": { "attempted": 0, "completed": 0, "remaining": 2 }
  },
  "message": "bounded recovery run completed with persisted work remaining"
}
```

## Implementation Tasks

### Task 1: Add Object Cleanup Claim Status Read Model

**Files:**
- Modify: `src/backend/object_cleanup.rs`
- Modify: `src/backend/postgres.rs`
- Test: `src/backend/object_cleanup.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Write failing tests**

Add tests that prove cleanup claims can be listed and counted without exposing lease tokens or raw failure messages.

Run:

```bash
cargo test --locked backend::object_cleanup --lib -- --nocapture
```

Expected: fails because list/count status APIs do not exist yet.

**Step 2: Add read-only status types and trait methods**

Add bounded read APIs to `ObjectCleanupClaimStore`:

- `list(&self, limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError>`
- `counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError>`

Status fields should include repo ID, claim kind, object kind, object ID, attempts, lease expiry, completed time, last failure marker, and age fields. Do not include canonical object keys, lease tokens, object payloads, or raw failure text on the HTTP surface.

**Step 3: Implement in-memory store**

Preserve existing transition behavior. Classification should be derived read-only:

- `active` if incomplete and lease expiry is in the future.
- `stale_active` if incomplete and lease expiry is in the past.
- `completed` if completed.
- `failed` if last error exists and incomplete.

**Step 4: Implement Postgres adapter**

Use `object_cleanup_claims` from migrations `0001` and `0008`. Keep query results bounded and ordered by incomplete first, older `updated_at` first, then repo/key for deterministic output.

**Step 5: Verify**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

### Task 2: Add Recovery Status Age And Stale Classification Helpers

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Optionally modify: `src/backend/core_transaction.rs`
- Optionally modify: `src/backend/durable_mutation.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing route tests**

Add tests under existing `vcs_recovery` route tests proving rows include:

- `age_millis`
- `stale_active`
- `due`
- `retryable`
- `stuck_tier`
- `next_retry_at_millis`

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

Expected: fails because those fields are missing.

**Step 2: Implement route-local read-model helpers**

Add helper functions that derive fields from existing timestamps without mutating stores:

- pending age from `first_seen_at_millis`, `last_seen_at_millis`, or best available phase timestamp.
- active stale state from `lease_expires_at_millis < now`.
- backing-off due state from `retry_after_millis <= now`.
- poisoned age from `terminal_at_millis` where available.
- stuck tier using SMFS-inspired buckets: `ok`, `info`, `warn`, `stuck`.

Keep thresholds constants in the route module unless shared by tests.

**Step 3: Verify**

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo fmt --all -- --check
```

### Task 3: Normalize `GET /vcs/recovery` Into Operator Phases And Blockers

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add route tests that verify:

- existing legacy fields still exist;
- new `health`, `phases`, and `blockers` fields exist;
- rows are bounded to 100;
- poisoned or stale-active rows on `main` produce a per-ref blocker;
- durable FS mutation rows produce a workspace blocker when `workspace_scope` is present.

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

Expected: fails because normalized fields are missing.

**Step 2: Build phase summaries**

Create route-local structs or `serde_json::Value` helpers for:

- `pre_visibility`
- `post_cas`
- `fs_mutations`
- `object_cleanup`

Each phase should include `available`, `counts`, `count`, `page_count`, `oldest_age_millis`, `due_count`, `stale_active_count`, `poisoned_count`, `rows`.

**Step 3: Build blocker summaries**

Aggregate only bounded, redacted summaries:

- per ref: repo ID, ref name, blocked boolean, reason, phases, pending, active, stale-active, backing-off, poisoned, retryable, next retry.
- per workspace: workspace scope, target ref, operation count, poisoned/stale/retryable.

Do not expose request bodies, idempotency keys, lease tokens, DB URLs, object payloads, raw object-store errors, or private store errors.

**Step 4: Verify**

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
git diff --check
```

### Task 4: Expose Scheduler Readiness And Last Tick Outcome

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_vcs.rs`
- Test: `src/server/mod.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add tests proving:

- duplicate routers still reuse one scheduler handle;
- `GET /vcs/recovery.health.scheduler.present` is true when the scheduler is attached;
- scheduler state records last tick time and per-phase outcome after a tick.

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

Expected: fails because the handle has no observable status.

**Step 2: Add scheduler status to the handle**

Add a small `DurableRecoverySchedulerStatus` behind `Arc<Mutex<_>>` or equivalent. Capture:

- `started_at_millis`
- `last_tick_at_millis`
- `last_outcome`
- per-phase attempted/completed/backing-off/poisoned/skipped if summaries are available
- redacted `last_error`

Do not change tick ordering, limits, leases, or worker semantics.

**Step 3: Read scheduler status in `GET /vcs/recovery`**

Use the existing `Extension<Arc<DurableRecoverySchedulerHandle>>` if present. If absent, return `present: false`.

**Step 4: Verify**

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo fmt --all -- --check
```

### Task 5: Improve Recovery Run Summary And Correlation ID

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add route tests proving:

- response includes a redacted `correlation_id`;
- `requested_limit` is the caller limit, not just the post-CAS residual limit;
- `remaining` is present by phase and total;
- `converged` is false when bounded work remains;
- invalid JSON and caller-supplied lease identity remain rejected/ignored.

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

Expected: fails because response lacks these fields.

**Step 2: Add a correlation ID**

Generate a bounded visible ID such as `rec_<uuid-simple>` or `rec_<uuid-v4>`, include it in the JSON body and an `X-Stratum-Recovery-Correlation-Id` response header.

**Step 3: Compute remaining**

After the run, re-read counts for each phase. Remaining should count unresolved work only:

- pre-visibility: pending + active + backing_off + poisoned
- post-CAS: pending + active + backing_off + poisoned
- FS mutations: pending + active + backing_off + poisoned
- cleanup claims: active + stale_active + failed, but attempted/completed remain zero because deletion is out of scope

**Step 4: Preserve compatibility**

Keep existing top-level post-CAS fields for callers that already consume them. Add normalized `phases` and aggregate totals without removing old keys.

**Step 5: Verify**

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
git diff --check
```

### Task 6: Documentation And Project Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP guide**

Add response examples for:

- `GET /vcs/recovery` with `health`, `phases`, `blockers`, and cleanup visibility.
- `POST /vcs/recovery/run` with correlation ID, remaining, and non-convergence wording.

Include operator wording:

- `pending`: queued, no action unless age grows.
- `backing_off`: wait until retry time or run manually after dependency fix.
- `poisoned`: manual investigation required; retry will not proceed automatically.
- `stale_active`: previous worker lease expired; retryable by scheduler or manual run.
- cleanup claims: object deletion is not implemented; claims identify recoverable orphan risk only.

**Step 2: Update project status**

Add a new completed slice section after implementation. Note that runtime semantics and final-object deletion remain unchanged.

**Step 3: Verify docs**

Run:

```bash
git diff --check
```

### Task 7: Review And Verification

**Files:**
- All changed files.

**Step 1: Focused tests**

Run:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::durable_mutation --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
```

**Step 2: Full required gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 3: Subagent reviews**

Run two review subagents after implementation:

- Spec/correctness review against this plan and the handoff requirements.
- Code-quality/security review focused on redaction, bounded rows, admin-only recovery controls, stale classification correctness, and preserving durable recovery semantics.

Fix findings locally in the main session, then rerun affected focused gates.

## Commit Plan

Keep commits small:

1. `feat: expose cleanup recovery status`
2. `feat: add recovery readiness status`
3. `feat: summarize bounded recovery runs`
4. `docs: document recovery operator readiness`

Only merge to `main` after review findings are fixed and the required verification gates pass.
