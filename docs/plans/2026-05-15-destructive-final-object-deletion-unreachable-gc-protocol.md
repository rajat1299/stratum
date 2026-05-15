# Destructive Final-Object Deletion And Broad Unreachable GC Protocol Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an explicit opt-in destructive deletion protocol for cleanup-claim-owned CAS-lost final objects, while keeping broad unreachable commit/object GC protocol-visible and non-destructive.

**Architecture:** Build on the existing dry-run reachability scanner, cleanup claims, and final-object metadata fences. A first eligible pass persists deletion readiness and a hold-window snapshot; a later explicitly destructive run must reacquire the cleanup claim, reacquire and validate the metadata fence, re-run reachability, verify the persisted metadata snapshot, delete final bytes, delete fenced metadata, prove both are gone, and only then complete the cleanup claim. Broad unreachable commit/object record deletion remains reported as protocol-only tombstone/readiness state, not executed.

**Tech Stack:** Rust 2024, Tokio, async-trait, existing durable backend store traits, `BlobObjectStore`, `RemoteBlobStore`, Postgres metadata migrations/adapters, `/vcs/recovery` and `/vcs/recovery/run`, existing recovery/idempotency/workspace/review stores.

---

## Required Skills

Implementation subagents must use:

- `/Users/rajattiwari/.codex/superpowers/skills/test-driven-development/SKILL.md`
- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`

Review subagents must compare against this plan, the prior GC plan, and the user acceptance criteria.

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-13-final-object-deletion-unreachable-object-gc.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-14-hosted-storage-operations-hardening.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

## Reference Guidance Applied

- Keep Stratum cleanup keyed by repo, object identity, cleanup claim, metadata fence, durable refs, workspaces, recovery ledgers, idempotency records, and review roots.
- Use SMFS only for bounded worker/backoff/drain/status vocabulary: attempted, completed, held, deferred, backing off, poisoned, remaining, stale active.
- Do not import SMFS latest-wins queue semantics, timestamp freshness, SQLite inode/chunk cache, or mutable filepath reconciliation.
- Keep object keys, lease tokens, DB URLs, R2 endpoints, raw backend errors, commit messages, and request bodies out of public status/run responses.

## Current Baseline

- `ObjectGcDryRun` reports unreachable commits and objects without mutation.
- `ObjectCleanupWorker` can prove a CAS-lost final object is `deletion_ready`, but it releases the claim and leaves `deleted_final_objects = 0`.
- `ObjectMetadataStore` has store-backed final-object metadata fences and fenced metadata deletion APIs.
- `BlobObjectStore::cleanup_final_objects(..., FinalObjectsMissingMetadataDelete)` still fails closed.
- `/vcs/recovery` and `/vcs/recovery/run` expose dry-run/deletion-ready counts, with `deletion_enabled: false`.
- The scheduler runs object cleanup as a bounded fourth phase, but must not perform destructive deletion without an explicit operator gate.

## Acceptance Checklist

- No final object bytes are deleted unless destructive mode is explicitly enabled for that run.
- First eligible pass persists `deletion_ready_at`, `delete_after`, and a metadata snapshot before byte deletion.
- Actual byte deletion requires hold-window expiry.
- Crash after readiness, after byte deletion, or before metadata deletion is retry-safe and operator-visible.
- Delete path requires active cleanup claim, active metadata fence, repeated reachability proof, matching metadata snapshot, and claim/fence revalidation immediately before deleting.
- Reachable, blocked, metadata-missing, active-claim, recovery-rooted, idempotency-rooted, review-rooted, and workspace-rooted objects are preserved.
- Only `DurableMutationCasLostObjectCleanup` final objects owned by the cleanup claim are destructively deleted in this slice.
- Cleanup claim completion happens only after final bytes are proven deleted and fenced metadata is proven deleted.
- Broad unreachable commit/object record deletion is visible as protocol/tombstone status but never deletes commit metadata or object records in this slice.
- `/vcs/recovery` and `/vcs/recovery/run` show deletion enabled, ready, held, deleted, deferred, poisoned, and remaining counts without sensitive values.

## Task 1: Plan Commit

**Files:**
- Create: `docs/plans/2026-05-15-destructive-final-object-deletion-unreachable-gc-protocol.md`

**Steps:**

1. Save this plan.
2. Verify:

```bash
git diff --check -- docs/plans/2026-05-15-destructive-final-object-deletion-unreachable-gc-protocol.md
```

3. Commit:

```bash
git add docs/plans/2026-05-15-destructive-final-object-deletion-unreachable-gc-protocol.md
git commit -m "docs: plan destructive object cleanup protocol"
```

## Task 2: Persist Deletion Readiness And Hold State

**Worker ownership:**
- Modify: `src/backend/object_cleanup.rs`
- Modify only as needed for metadata snapshot helpers: `src/backend/blob_object.rs`

**Step 1: Write failing tests**

Add tests under `src/backend/object_cleanup.rs`:

- `cleanup_worker_persists_deletion_ready_and_delete_after_before_delete`
- `cleanup_worker_requires_hold_window_expiry_before_delete`
- `cleanup_worker_keeps_metadata_missing_final_object_repairable`
- `cleanup_worker_preserves_reachable_recovery_idempotency_review_workspace_roots`
- `cleanup_worker_status_redacts_ready_snapshot_and_object_key`

Expected RED: claim status has no persisted readiness/hold fields and the worker only returns an in-memory `deletion_ready` count.

**Step 2: Add readiness model and store API**

Add narrow types in `src/backend/object_cleanup.rs`:

- `FinalObjectDeletionSnapshot`
  - `object_key: String` stored internally only
  - `size: u64`
  - `sha256: String`
- `FinalObjectDeletionReadiness`
  - `deletion_ready_at: SystemTime`
  - `delete_after: SystemTime`
  - `snapshot: FinalObjectDeletionSnapshot`
- `ObjectCleanupDeletionMode`
  - `NonDestructive`
  - `Destructive { hold_window: Duration }`

Extend `ObjectCleanupClaimStatus` and `ObjectCleanupClaimStatusInput` with redacted-safe accessors:

- `deletion_ready_at() -> Option<SystemTime>`
- `delete_after() -> Option<SystemTime>`
- `final_object_bytes_deleted_at() -> Option<SystemTime>`
- `final_object_metadata_deleted_at() -> Option<SystemTime>`
- `is_deletion_held(now: SystemTime) -> bool`

Do not include object keys, lease tokens, or snapshot hashes in `Debug`.

Extend `ObjectCleanupClaimStore`:

```rust
async fn mark_deletion_ready(
    &self,
    claim: &ObjectCleanupClaim,
    readiness: FinalObjectDeletionReadiness,
) -> Result<(), VfsError>;

async fn clear_deletion_ready(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError>;
```

In-memory behavior:

- `mark_deletion_ready` requires the active matching claim.
- It persists the ready timestamp, delete-after timestamp, and metadata snapshot.
- Re-marking with the same active claim is idempotent if the snapshot matches.
- If metadata changed, clear readiness and return a retry/conflict error.
- `release` must preserve readiness state.
- `record_failure` must preserve readiness unless the failure is due to metadata snapshot mismatch; then the worker calls `clear_deletion_ready`.

**Step 3: Change worker first-pass behavior**

In `ObjectCleanupWorker`:

- Default to `ObjectCleanupDeletionMode::NonDestructive`.
- Keep only CAS-lost cleanup claims eligible.
- When a candidate is unreachable and fenced, verify metadata matches the claim.
- Persist readiness with `delete_after = now + hold_window`.
- Release the claim and metadata fence after readiness.
- Return summary counts:
  - `deletion_ready`
  - `deletion_held`
  - `deferred`
  - `deleted_final_objects = 0`

Use a conservative default hold window in tests such as 60 seconds. Allow tests to configure zero/short hold windows through a test-only or builder-style constructor.

**Step 4: Verify and commit**

```bash
cargo fmt --all -- --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
git diff --check
git add src/backend/object_cleanup.rs src/backend/blob_object.rs
git commit -m "feat: persist final object deletion readiness"
```

## Task 3: Destructive Final Byte Deletion API And Retry-Safe Phases

**Worker ownership:**
- Modify: `src/backend/mod.rs`
- Modify: `src/backend/blob_object.rs`
- Modify: `src/backend/object_cleanup.rs`
- Modify: `src/remote/blob.rs` only if a byte-store delete/missing helper needs redaction hardening

**Step 1: Write failing tests**

Add tests:

- `cleanup_worker_does_not_delete_when_destructive_mode_is_disabled`
- `cleanup_worker_deletes_final_bytes_only_after_hold_expiry_and_explicit_gate`
- `cleanup_worker_does_not_delete_when_claim_or_fence_changes_after_readiness`
- `cleanup_worker_retries_metadata_delete_after_crash_following_byte_delete`
- `cleanup_worker_completes_claim_only_after_bytes_and_metadata_are_gone`
- `cleanup_worker_delete_errors_are_redacted`

Expected RED: `ObjectStore` has no final-byte delete contract and worker completion is non-destructive.

**Step 2: Add object-store deletion seam**

Extend `ObjectStore` with default fail-closed methods:

```rust
async fn delete_final_object_bytes(
    &self,
    repo_id: &RepoId,
    id: ObjectId,
    expected_kind: ObjectKind,
    expected_key: &str,
) -> Result<(), VfsError>;

async fn final_object_bytes_present(
    &self,
    repo_id: &RepoId,
    id: ObjectId,
    expected_kind: ObjectKind,
    expected_key: &str,
) -> Result<bool, VfsError>;
```

Implement for:

- `BlobObjectStore`: validate `expected_key == canonical_final_object_key(repo_id, kind, id)`, call `RemoteBlobStore::delete_bytes`, and treat missing bytes as idempotently deleted where the remote store reports missing. Map errors to fixed redacted messages.
- `LocalMemoryObjectStore`: remove/check the repo/object/kind tuple for unit tests.

Do not expose final object keys in returned errors.

**Step 3: Add byte and metadata phase markers**

Extend `ObjectCleanupClaimStore`:

```rust
async fn mark_final_object_bytes_deleted(
    &self,
    claim: &ObjectCleanupClaim,
) -> Result<(), VfsError>;

async fn mark_final_object_metadata_deleted(
    &self,
    claim: &ObjectCleanupClaim,
) -> Result<(), VfsError>;
```

These require the active matching claim and preserve the readiness snapshot. Completion remains separate.

**Step 4: Implement destructive phase**

When `ObjectCleanupDeletionMode::Destructive` is active and the persisted `delete_after` has expired:

1. Reacquire the cleanup claim.
2. Re-run dry-run reachability with the current claim allowlisted.
3. Acquire a final-object metadata fence.
4. Verify current metadata matches the persisted readiness snapshot and the fence metadata identity.
5. Validate the cleanup claim.
6. Validate the metadata fence.
7. Re-run dry-run reachability immediately before deleting.
8. Delete final bytes with `delete_final_object_bytes`.
9. Verify `final_object_bytes_present == false`.
10. Persist `final_object_bytes_deleted_at`.
11. Delete metadata through `delete_with_final_object_metadata_fence`.
12. Verify metadata is absent.
13. Persist `final_object_metadata_deleted_at`.
14. Complete the cleanup claim.
15. Release the fence idempotently.

If the process crashes after byte deletion but before metadata deletion, retry must skip the byte delete when bytes are already absent, reacquire the fence, delete metadata, prove metadata is gone, and complete the claim.

If metadata is missing before byte deletion and no persisted byte-deleted phase exists, fail closed as repairable and do not delete bytes.

**Step 5: Verify and commit**

```bash
cargo fmt --all -- --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
git diff --check
git add src/backend/mod.rs src/backend/blob_object.rs src/backend/object_cleanup.rs src/remote/blob.rs
git commit -m "feat: delete fenced final objects explicitly"
```

## Task 4: Postgres Deletion State And Conformance

**Worker ownership:**
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Create: `migrations/postgres/0011_object_cleanup_deletion_state.sql`

**Step 1: Write failing Postgres tests**

Add feature-gated tests under `src/backend/postgres.rs`:

- `postgres_cleanup_claim_persists_deletion_ready_hold_state`
- `postgres_cleanup_claim_preserves_ready_state_across_release_and_reclaim`
- `postgres_cleanup_worker_deletes_only_after_hold_and_explicit_gate`
- `postgres_cleanup_worker_retries_after_byte_deleted_before_metadata_deleted`
- `postgres_cleanup_claim_completion_requires_byte_and_metadata_markers`
- `postgres_cleanup_status_redacts_deletion_snapshot`

Expected RED: schema lacks deletion state columns and adapter methods.

**Step 2: Add migration**

Add nullable columns to `object_cleanup_claims`:

- `deletion_ready_at TIMESTAMPTZ`
- `delete_after TIMESTAMPTZ`
- `deletion_snapshot_object_key TEXT`
- `deletion_snapshot_size_bytes BIGINT`
- `deletion_snapshot_sha256 TEXT`
- `final_object_bytes_deleted_at TIMESTAMPTZ`
- `final_object_metadata_deleted_at TIMESTAMPTZ`

Add checks:

- snapshot fields are all null or all non-null.
- `delete_after` requires `deletion_ready_at`.
- phase markers require `deletion_ready_at`.
- `completed_at` for `durable_mutation_cas_lost_object_cleanup` with deletion readiness requires both phase markers.
- snapshot key must match the canonical final key when present.

Add a due-delete index scoped to incomplete CAS-lost claims ordered by `delete_after`.

**Step 3: Implement adapter methods**

Use transactions and claim-token predicates for readiness and phase updates. Preserve existing redacted `postgres_error` behavior.

`list`, `list_for_repo`, and `list_claimable_for_repo_and_kind` must hydrate the new status fields. Claimable ordering should process non-poisoned due/held claims before poisoned rows and avoid starving newly claimable work.

**Step 4: Verify and commit**

```bash
cargo fmt --all -- --check
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
git diff --check
git add src/backend/postgres.rs src/backend/postgres_migrations.rs migrations/postgres/0011_object_cleanup_deletion_state.sql
git commit -m "feat: persist destructive cleanup state"
```

## Task 5: Recovery Routes, Scheduler Visibility, And Broad GC Protocol Status

**Worker ownership:**
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/mod.rs`
- Modify docs only if route examples need immediate update: `docs/http-api-guide.md`

**Step 1: Write failing route/scheduler tests**

Add tests:

- `vcs_recovery_run_requires_explicit_destructive_deletion_gate`
- `vcs_recovery_run_reports_ready_held_deleted_deferred_poisoned_remaining`
- `vcs_recovery_run_deletes_after_hold_when_gate_enabled`
- `vcs_recovery_run_never_deletes_broad_unreachable_records`
- `vcs_recovery_status_reports_protocol_only_broad_unreachable_gc`
- `vcs_recovery_status_redacts_deletion_snapshot_keys_and_raw_errors`
- `durable_recovery_scheduler_never_runs_destructive_deletion`

Expected RED: run request has no destructive gate, status lacks held/deleted protocol fields, and broad GC has no protocol-only section.

**Step 2: Extend recovery run request**

Extend `RecoveryRunRequest`:

```rust
pub destructive_final_object_deletion: Option<bool>,
pub final_object_deletion_hold_seconds: Option<u64>,
```

Rules:

- Missing/false destructive flag means `ObjectCleanupDeletionMode::NonDestructive`.
- True enables destructive mode only for this operator-triggered run.
- Scheduler always uses `NonDestructive`.
- Hold seconds defaults to the backend default. Tests may use `0`; production request parsing should cap it to a safe bounded range and reject oversized values by field name only.
- Do not echo request bodies.

**Step 3: Update route summaries**

For `/vcs/recovery`:

- keep existing object-cleanup rows redacted.
- include claim counts for `deletion_ready`, `deletion_held`, `deleted_final_objects`, `deferred`, `poisoned`, `remaining`.
- include `object_gc.broad_unreachable` with `mode: "protocol_only"`, `deletion_enabled: false`, unreachable commit/object counts, and tombstone/deletion status as non-destructive.

For `/vcs/recovery/run`:

- include per-phase and top-level deletion fields:
  - `deletion_enabled`
  - `deletion_ready`
  - `deletion_held`
  - `deleted_final_objects`
  - `deferred`
  - `poisoned`
  - `remaining`
- `completed` for object cleanup means destructive final object deletion completed, not readiness.
- If destructive mode is enabled but hold has not expired, report held/deferred, not deleted.
- If broad unreachable commit/object records exist, report them under protocol-only status and do not mutate them.

**Step 4: Update scheduler status**

`DurableRecoverySchedulerPhaseStatus::from_object_cleanup_summary` must include ready/held/deleted/deferred counts. The scheduler must instantiate the worker in non-destructive mode.

**Step 5: Verify and commit**

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
git diff --check
git add src/server/routes_vcs.rs src/server/mod.rs docs/http-api-guide.md
git commit -m "feat: expose destructive cleanup recovery protocol"
```

## Task 6: Docs And Project Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Steps:**

1. Document `/vcs/recovery` and `/vcs/recovery/run` deletion fields.
2. State that destructive deletion is explicit per operator run and default off.
3. State that first eligible pass persists readiness/hold state; deletion requires a later hold-expired destructive run.
4. State that only cleanup-claim-owned CAS-lost final objects are deletable in this slice.
5. State that broad unreachable commit/object record deletion remains protocol-visible but non-destructive.
6. Update project status completed slice, residual risks, and recommended next slices.
7. Add verification results after final gates finish.

**Verify and commit:**

```bash
git diff --check
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: record destructive cleanup protocol"
```

## Task 7: Reviews, Fixes, Verification, Push, Merge

**Review sequence:**

1. Main session inspects each implementation diff locally.
2. Run focused tests for the touched area.
3. Dispatch spec/correctness review with `gpt-5.5` high or xhigh.
4. Fix all Critical/Important findings locally or through scoped worker fixes.
5. Dispatch code-quality/security review with `gpt-5.5` high or xhigh.
6. Fix all Critical/Important findings locally.
7. Run final full gates.

**Required final gates:**

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Push and merge:**

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git fetch origin
git checkout main
git merge --ff-only v2/foundation
git push origin main
```

Do not remove or revert unrelated untracked files in the main worktree.
