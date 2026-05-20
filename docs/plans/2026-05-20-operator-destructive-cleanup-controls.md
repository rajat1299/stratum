# Operator Destructive Cleanup Controls Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expose destructive CAS-lost final-object cleanup only through an explicit bounded operator run while preserving non-destructive scheduler and default recovery behavior.

**Architecture:** Reuse the existing cleanup-claim, metadata-fence, readiness, hold-window, and destructive worker phases. Add a narrow `POST /vcs/recovery/run` request parser that can opt one admin run into `ObjectCleanupDeletionMode::Destructive`, keep all missing/default requests non-destructive, and keep broad unreachable commit/object GC reported as dry-run/protocol-only. Add provider-backed R2 proof for the final-byte deletion seam before claiming production readiness.

**Tech Stack:** Rust 2024, Axum, Tokio, Serde JSON, existing durable recovery stores, `ObjectCleanupWorker`, `BlobObjectStore`, R2/S3-compatible byte storage, Postgres feature-gated cleanup claim adapters, Bash live-gate scripts, and existing recovery docs.

---

## Required Skills

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare against this plan, the user acceptance criteria, and:

- `docs/plans/2026-05-15-destructive-final-object-deletion-unreachable-gc-protocol.md`
- `docs/plans/2026-05-13-final-object-deletion-unreachable-object-gc.md`
- `docs/plans/2026-05-19-recovery-scheduler-productionization.md`

## Current Baseline

- The destructive worker path already exists behind `ObjectCleanupDeletionMode::Destructive`.
- `ObjectCleanupWorker::new(...)` defaults to `ObjectCleanupDeletionMode::NonDestructive`.
- `POST /vcs/recovery/run` instantiates the worker without destructive mode and reports `deletion_enabled: false`.
- The background scheduler in `src/server/mod.rs` also instantiates the worker without destructive mode.
- `GET /vcs/recovery` reports deletion-ready, held, deleted, deferred, poisoned, and remaining counts, but keeps `deletion_enabled: false`.
- Broad unreachable commit/object GC is exposed as `gc_dry_run` with `deletion_enabled: false`.
- The R2 live gate proves byte-store put/get/list/delete and `BlobObjectStore` put/get, but does not yet prove the destructive cleanup protocol or final-object delete seam.

## Non-Negotiable Constraints

- No final object bytes are deleted unless an admin calls `POST /vcs/recovery/run` with the explicit destructive cleanup control.
- Scheduler ticks remain non-destructive in all configurations.
- Missing body, empty body, `destructive_final_object_deletion: false`, or absent destructive fields remain non-destructive.
- Malformed destructive requests fail closed with fixed redacted errors.
- If `final_object_deletion_hold_seconds` is present while destructive cleanup is not explicitly enabled, reject the request as a partial destructive cleanup request.
- Destructive mode is enabled for that single bounded run only; it is not persisted in runtime config or scheduler config.
- Actual deletion still requires persisted readiness, hold-window expiry, an active cleanup claim, an active metadata fence, matching metadata snapshot, repeated reachability proof, and immediate claim/fence revalidation.
- Broad unreachable commit/object deletion remains dry-run/protocol-visible only.
- Public errors, status, docs examples, script output, and test assertions must not expose DB URLs, R2 endpoints, bucket names, object keys, raw backend/provider errors, SQL, migration SQL, request bodies, idempotency keys, tokens, lease tokens, commit messages, or secrets.

## Operator Request Shape

`POST /vcs/recovery/run` keeps the existing request body and adds:

```json
{
  "limit": 10,
  "destructive_final_object_deletion": true,
  "final_object_deletion_hold_seconds": 0
}
```

Rules:

- `destructive_final_object_deletion` must be exactly `true` to enable destructive final-object deletion for this run.
- `final_object_deletion_hold_seconds` is optional when destructive deletion is true and defaults to `ObjectCleanupDeletionMode::DEFAULT_NON_DESTRUCTIVE_HOLD_WINDOW`.
- Tests may use `0` to prove the two-run readiness-then-delete flow without sleeping.
- Operator requests cap hold seconds at `604800` seconds.
- Oversized or malformed hold values return a fixed field-name-only `400` error.
- A hold value without `destructive_final_object_deletion: true` returns `400`.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-20-operator-destructive-cleanup-controls.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-20-operator-destructive-cleanup-controls.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-20-operator-destructive-cleanup-controls.md
git commit -m "docs: plan operator destructive cleanup controls"
```

## Task 2: Parse Explicit Destructive Cleanup Run Controls

**Files:**

- Modify: `src/server/routes_vcs.rs`

**Step 1: Write failing parser/route tests**

Add focused tests under `server::routes_vcs::tests::vcs_recovery`:

- `vcs_recovery_run_rejects_hold_window_without_destructive_gate`
- `vcs_recovery_run_rejects_oversized_destructive_hold_window`
- `vcs_recovery_run_keeps_default_non_destructive_when_gate_absent`
- `vcs_recovery_run_rejects_destructive_cleanup_without_admin`

Expected RED:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery_run_rejects_hold_window_without_destructive_gate --lib -- --nocapture
```

The first new test fails because `RecoveryRunRequest` ignores `final_object_deletion_hold_seconds`.

**Step 2: Add request fields and parsing helper**

Extend `RecoveryRunRequest`:

```rust
pub destructive_final_object_deletion: Option<bool>,
pub final_object_deletion_hold_seconds: Option<u64>,
```

Add a small internal options struct:

```rust
struct RecoveryRunOptions {
    limit: usize,
    object_cleanup_deletion_mode: ObjectCleanupDeletionMode,
    object_cleanup_deletion_enabled: bool,
}
```

Replace `recovery_run_limit_from_body` with `recovery_run_options_from_body`.

Parsing rules:

- Empty body returns default limit, non-destructive mode, deletion enabled false.
- Invalid JSON returns `VfsError::InvalidArgs { message: "invalid recovery run request" }`.
- Limit keeps the existing default and max cap.
- `final_object_deletion_hold_seconds` with missing/false destructive flag returns `VfsError::InvalidArgs { message: "invalid destructive cleanup request" }`.
- Hold seconds greater than `604800` returns `VfsError::InvalidArgs { message: "invalid final_object_deletion_hold_seconds" }`.
- Destructive true with no hold seconds uses the backend default hold window.

**Step 3: Wire run options into the route**

In `vcs_recovery_run`:

- Keep `require_admin` as the first gate.
- Use `options.limit` everywhere the route currently uses `limit`.
- Pass `options.object_cleanup_deletion_mode` into the worker with `.with_deletion_mode(...)`.
- Set both `phases.object_cleanup.deletion_enabled` and top-level `object_cleanup.deletion_enabled` from `options.object_cleanup_deletion_enabled`.
- Do not echo the request body or hold value in error responses.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests::vcs_recovery_run_rejects_hold_window_without_destructive_gate --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery_run_rejects_oversized_destructive_hold_window --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery_run_keeps_default_non_destructive_when_gate_absent --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery_run_rejects_destructive_cleanup_without_admin --lib -- --nocapture
git diff --check
git add src/server/routes_vcs.rs
git commit -m "feat: parse destructive cleanup operator request"
```

## Task 3: Expose Bounded Destructive Cleanup Through Manual Recovery Run Only

**Files:**

- Modify: `src/server/routes_vcs.rs`
- Modify tests in: `src/server/mod.rs`

**Step 1: Write failing behavior tests**

Add or extend tests:

- `vcs_recovery_run_requires_two_destructive_runs_before_deleting_final_object`
- `vcs_recovery_run_reports_deletion_enabled_deleted_and_remaining_when_gate_enabled`
- `vcs_recovery_run_never_deletes_broad_unreachable_records`
- `durable_recovery_scheduler_never_runs_destructive_deletion`

Expected RED:

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery_run_requires_two_destructive_runs_before_deleting_final_object --lib -- --nocapture
```

The route still reports `deletion_enabled: false` and does not pass destructive mode into `ObjectCleanupWorker`.

**Step 2: Implement manual-run destructive behavior**

Use the existing worker behavior:

- First destructive run against an eligible claim with no readiness persists readiness and returns `deletion_ready: 1`, `deleted_final_objects: 0`.
- A later destructive run after hold expiry deletes final bytes, deletes fenced metadata, completes the claim, and returns `deleted_final_objects: 1`.
- With `final_object_deletion_hold_seconds: 0`, tests should still require two route calls: first readiness, second deletion.

Assert after the second route call:

```rust
assert!(stores.objects.get(&repo_id, lost_object, ObjectKind::Blob).await.unwrap().is_none());
assert!(stores.object_metadata.get(&repo_id, lost_object).await.unwrap().is_none());
assert_eq!(stores.object_cleanup.counts().await.unwrap().completed(), 1);
```

**Step 3: Preserve scheduler default**

Keep `src/server/mod.rs` scheduler construction as:

```rust
let object_cleanup_worker = ObjectCleanupWorker::new(...);
```

Do not add `.with_deletion_mode(...)` in the scheduler.

Add or keep assertions that scheduler status has:

```rust
assert_eq!(status.phases.object_cleanup.completed, Some(0));
assert_eq!(status.phases.object_cleanup.deleted_final_objects, Some(0));
```

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
git diff --check
git add src/server/routes_vcs.rs src/server/mod.rs
git commit -m "feat: enable bounded destructive cleanup runs"
```

## Task 4: Add Provider-Backed R2 Destructive Cleanup Evidence

**Files:**

- Modify: `src/remote/blob.rs`
- Modify: `scripts/check-r2-object-store.sh`
- Modify only if needed for imports/visibility: `src/backend/object_cleanup.rs`

**Step 1: Write failing live smoke test**

Add a live-gated test in `src/remote/blob.rs`:

- `r2_blob_store_live_destructive_cleanup_protocol`

The test should skip under the same `STRATUM_R2_TEST_ENABLED` / `STRATUM_R2_TEST_REQUIRED` rules as the existing live R2 integration test.

It should:

1. Build an `R2BlobStore` with a unique test prefix.
2. Wrap it in `BlobObjectStore` with `InMemoryObjectMetadataStore`.
3. Seed in-memory commit/ref/workspace/review/idempotency/recovery/cleanup stores.
4. Put one unreferenced final blob object.
5. Create and release a `DurableMutationCasLostObjectCleanup` claim.
6. Run `ObjectCleanupWorker` in `Destructive { hold_window: Duration::ZERO }` once to persist readiness.
7. Run it a second time to delete.
8. Assert final bytes and metadata are absent.
9. Assert the rendered summary/status contains no R2 endpoint, bucket, object key, access key, or secret.

Expected RED:

```bash
STRATUM_R2_TEST_ENABLED= cargo test --locked remote::blob::tests::r2_blob_store_live_destructive_cleanup_protocol -- --nocapture
```

The new test is absent.

**Step 2: Implement the live smoke**

Prefer existing local helper patterns from `remote::blob::tests::r2_blob_store_live_integration`. Keep the test self-contained and provider-safe:

- Use a unique prefix.
- Do not print raw config.
- Use only redacted/fixed assertion messages.
- Clean up through the destructive path; if setup fails after writing, attempt direct `delete_bytes` cleanup on the known test key without printing it.

**Step 3: Update the R2 live gate script**

Change `scripts/check-r2-object-store.sh` from the single exact test to a live R2 selector that includes both tests:

```bash
cargo test --locked remote::blob::tests::r2_blob_store_live -- --nocapture
```

Default skip behavior and required-mode failure behavior stay unchanged.

**Step 4: Verify and commit**

Run:

```bash
bash -n scripts/check-r2-object-store.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo fmt --all -- --check
git diff --check
git add src/remote/blob.rs scripts/check-r2-object-store.sh src/backend/object_cleanup.rs
git commit -m "test: cover destructive cleanup on R2 bytes"
```

If local R2 credentials are unavailable, the script must skip cleanly. Do not claim provider-backed destructive cleanup evidence until this test runs against real credentials locally or in protected CI.

## Task 5: Document Operator Usage, Rollback, Soak, And Evidence

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP guide**

Document near the recovery section:

- Default `GET /vcs/recovery` and default `POST /vcs/recovery/run` remain non-destructive.
- Destructive final-object cleanup is only enabled for one admin `POST /vcs/recovery/run` request with `destructive_final_object_deletion: true`.
- A first eligible run records readiness and hold state; deletion requires a later hold-expired destructive run.
- `final_object_deletion_hold_seconds` is bounded and rejected if supplied without the destructive flag.
- Only cleanup-claim-owned CAS-lost final objects are eligible.
- Broad unreachable commit/object GC remains dry-run/protocol-only.
- Rollback is to stop sending the destructive request field; no runtime flag needs to be changed.
- Status/run outputs remain redacted and identify cleanup rows by repo, kind, and short object id, not canonical object key.

**Step 2: Define soak/provider evidence language**

In `docs/project-status.md`, record:

- What landed.
- The exact local commands run.
- Whether `scripts/check-r2-object-store.sh` skipped locally or ran with real credentials.
- If local credentials were absent, state that protected CI with the new live destructive cleanup smoke must be inspected before claiming provider-backed completion.

Use this wording pattern if provider credentials are absent:

```text
Local live R2 destructive cleanup verification was not run because local R2 credentials were unavailable. Completion of the provider-backed destructive-control acceptance criterion requires either a local run with real R2 credentials or inspected protected CI evidence for the new R2 destructive cleanup smoke.
```

**Step 3: Verify and commit**

Run:

```bash
rg -n "destructive_final_object_deletion|final_object_deletion_hold_seconds|protocol-only|R2 destructive" docs/http-api-guide.md docs/project-status.md
git diff --check
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document destructive cleanup operator controls"
```

## Task 6: Review, Full Gates, Push, And Merge

**Files:**

- No planned edits unless reviews find issues.

**Step 1: Spec review**

Dispatch a fresh review subagent. It must check:

- Explicit admin/operator control exists.
- Defaults and scheduler remain non-destructive.
- Partial/malformed/unauthorized destructive requests fail closed.
- Destructive run is bounded and reports deletion enabled/ready/held/deleted/deferred/poisoned/remaining.
- Broad unreachable GC remains dry-run/protocol-only.
- Live R2 proof covers actual final-byte deletion behavior or docs honestly say it is still pending.

**Step 2: Code-quality/security review**

Dispatch a fresh review subagent. It must check:

- Redaction of errors, status, logs, docs, and script output.
- Rust API shape and ownership/cloning.
- Race/crash retry behavior around readiness, byte deletion, metadata deletion, and claim completion.
- No new scheduler/config persistence path can silently enable deletion.
- Tests are focused and deterministic.

**Step 3: Fix findings locally and rerun focused gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
```

**Step 4: Run full gates**

Run:

```bash
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

**Step 5: Push and merge**

After local verification:

```bash
git status --short --branch
git push origin v2/foundation
```

If local main remains dirty, use a temporary clean main worktree:

```bash
git worktree add /tmp/stratum-main-slice7 main
cd /tmp/stratum-main-slice7
git fetch origin
git checkout main
git reset --hard origin/main
git merge --no-ff origin/v2/foundation
git push origin main
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation
```

Inspect protected CI after pushing main before claiming live provider evidence for the new destructive cleanup smoke.
