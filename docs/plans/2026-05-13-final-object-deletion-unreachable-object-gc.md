# Final-Object Deletion And Unreachable Object GC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add safe, bounded, auditable dry-run reachability and deletion-readiness fencing for CAS-lost durable final objects, while reporting unreachable durable commit/object records without enabling broad hosted cleanup.

**Architecture:** Build a Stratum-native reachability scanner from durable refs, workspace/session refs, recovery ledgers, retained idempotency records, review/change-request records, commits, and tree objects. Deletion is narrower than detection: only cleanup-claim-owned final objects may advance to deletion readiness, and only after an active lease, a metadata deletion fence that blocks metadata repair, and repeated immediate reachability/claim/fence checks. General unreachable commit metadata and object records are dry-run reported in this slice. Destructive byte/metadata deletion remains disabled until a later crash/retry-safe delete protocol is implemented.

**Tech Stack:** Rust, Tokio, async-trait, existing durable backend store traits, Postgres metadata migrations/adapters, `BlobObjectStore`, `ObjectCleanupClaimStore`, route-level `/vcs/recovery` status/run surfaces, and existing recovery/idempotency/workspace/review stores.

---

## Required Skills

Implementation and review subagents must use:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Use TDD for behavior changes: write the failing test, run it red, implement the smallest green change, then refactor.

## Context

Durable mounted-session mutations and guarded durable VCS operations now create durable object cleanup claims for CAS-lost object writes. Those claims are operator-visible through `/vcs/recovery`, but no worker deletes final object bytes or unreachable durable commit/object records.

Existing final-object repair is intentionally conservative: final bytes missing metadata are repaired under `FinalObjectMetadataRepair` cleanup claims, active claims are skipped, hash mismatches are recorded, and `FinalObjectsMissingMetadataDelete` fails closed. This slice must preserve that behavior.

`extract pieces.md` contributes only bounded worker/status patterns from SMFS: claim/finalize, bounded drains, backoff, poison, status, and stale-active vocabulary. Do not copy SMFS latest-wins sync, timestamp freshness, SQLite inode/chunk cache, or mutable filepath reconciliation into Stratum commit/ref/object cleanup.

## Implementation Outcome

The landed slice implements reachability dry-run, store-backed final-object metadata fences, and a bounded non-destructive cleanup-readiness worker. The worker can prove a CAS-lost cleanup candidate is deletion-ready only after an active cleanup claim, a metadata fence, matching final-object metadata, and repeated root/reachability/claim/fence validation. It then releases the claim so the readiness signal is repeatable, reports `deletion_ready`, and keeps `deleted_final_objects` at `0`.

Destructive final-object byte deletion, metadata deletion, cleanup-claim completion after deletion, and broad unreachable commit/object record deletion remain disabled. They need a later crash/retry-safe delete protocol that preserves the same fences and revalidation checks.

## Scope

In scope:

- Dry-run durable reachability analysis from refs, workspace heads/session refs, recovery ledgers, retained idempotency records, active cleanup claims, and review/change-request refs.
- Tree/object graph walking from reachable commits to root trees, tree children, and blobs.
- Reporting unreachable commits and objects in bounded redacted summaries.
- Store-backed final-object deletion fence that prevents metadata repair from racing deletion.
- Bounded non-destructive cleanup-readiness worker for `DurableMutationCasLostObjectCleanup` candidates only.
- Retry/backoff/poison behavior for cleanup claims.
- Admin `/vcs/recovery` and `/vcs/recovery/run` integration with dry-run and deletion summaries.
- Postgres migration and adapter support.
- Docs/status updates.

Out of scope:

- Idempotency retention/quota.
- Secret-safe replay storage.
- Hosted TLS/KMS/pooling/secrets hardening.
- Sparse FUSE.
- Semantic search.
- Web console.
- Execution runner.
- Broad production `STRATUM_CORE_RUNTIME=durable-cloud` rollout.
- Destructive final-object byte/metadata deletion and broad unreachable commit/object record deletion.

## Design Constraints

- Never delete a final object solely because metadata is missing. Missing metadata is repairable unless a deletion fence is held and reachability proves the object is not retained.
- Cleanup claims coordinate work but are not enough for deletion. Deletion requires a current cleanup claim lease and a store-backed metadata fence.
- Revalidate immediately before deleting bytes: object metadata, refs, workspaces, recovery, idempotency, reviews, and cleanup claims.
- Treat active cleanup claims as reachability roots unless the current worker owns the exact cleanup claim being processed.
- Preserve objects referenced by refs, workspace heads/session refs, recovery ledgers, retained idempotency records, or change requests.
- Do not expose canonical object keys, raw object-store errors, raw SQL errors, idempotency keys, auth tokens, request bodies, or commit messages on HTTP status/run surfaces.
- Keep worker runs bounded by caller limit. No busy waiting in route handlers.
- Keep local memory stores useful for unit tests and Postgres adapters authoritative for migration/schema behavior.

## Task 1: Plan Commit

**Files:**

- Create: `docs/plans/2026-05-13-final-object-deletion-unreachable-object-gc.md`

**Steps:**

1. Save this plan.
2. Run:

```bash
cargo fmt --all -- --check
git diff --check
```

3. Commit:

```bash
git add docs/plans/2026-05-13-final-object-deletion-unreachable-object-gc.md
git commit -m "docs: plan final object gc"
```

## Task 2: Reachability Dry-Run Model

**Worker ownership:**

- Modify: `src/backend/object_cleanup.rs`
- Modify only if needed for reusable tree walking: `src/backend/core_transaction.rs`
- Modify only if needed for store root enumeration defaults: `src/idempotency.rs`
- Modify tests in the same files only

**Step 1: Write failing tests**

Add focused tests under `src/backend/object_cleanup.rs`:

- `gc_dry_run_reports_unreachable_commit_and_objects`
- `gc_dry_run_preserves_ref_workspace_recovery_idempotency_and_review_roots`
- `gc_dry_run_treats_active_cleanup_claims_as_roots_except_current_claim`
- `gc_dry_run_is_bounded_and_redacted_when_tree_walk_fails`

The tests should seed local memory stores with:

- a reachable `main` ref commit;
- a workspace `head_commit` and `session_ref`;
- an open change request with `base_commit` and `head_commit`;
- recovery rows for post-CAS, pre-visibility, and FS mutation commit ids;
- retained idempotency replay data containing known commit ids;
- an unreachable commit/root tree/blob trio;
- a CAS-lost final object cleanup claim.

Expected RED: tests fail because no dry-run scanner/root model exists.

**Step 2: Implement dry-run data model**

Add narrow backend structs in `src/backend/object_cleanup.rs`:

- `ObjectGcDryRun`
- `ObjectGcRoots`
- `ObjectGcDryRunReport`
- `UnreachableCommitCandidate`
- `UnreachableObjectCandidate`
- `ObjectGcBlockerSummary`

The dry-run should:

- collect commit roots from `RefStore::list`;
- collect workspace `head_commit`, `base_ref`, and `session_ref` roots through `WorkspaceMetadataStore::list_workspaces_for_repo` and ref resolution;
- collect review roots from `ReviewStore::list_change_requests_for_repo` using both `base_commit` and `head_commit`;
- collect recovery roots from post-CAS, pre-visibility, and FS mutation recovery status rows;
- collect retained idempotency roots through a bounded redacted helper that parses known commit-id fields and treats pending repo-scoped idempotency as a blocker;
- collect active cleanup claim object roots, with an allowlist for the currently leased claim;
- walk commit parents through `CommitStore::get`;
- walk root trees through `ObjectStore::get(..., ObjectKind::Tree)` and `TreeObject::deserialize`;
- mark tree and blob object ids reachable from reachable commits;
- report unreachable commit records from `CommitStore::list`;
- report object ids from cleanup claims and store metadata APIs when they are not reachable.

Keep the first implementation conservative: if a root source cannot be listed or a tree walk is corrupt, add a redacted blocker and skip deletion for that candidate.

**Step 3: Verify GREEN**

```bash
cargo test --locked backend::object_cleanup --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/backend/object_cleanup.rs src/backend/core_transaction.rs src/idempotency.rs
git commit -m "feat: add durable object gc dry run"
```

## Task 3: Store-Backed Final Object Metadata Fence

**Worker ownership:**

- Modify: `src/backend/object_cleanup.rs`
- Modify: `src/backend/blob_object.rs`
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/postgres.rs`
- Create migration under `migrations/postgres/`
- Modify: `src/backend/postgres_migrations.rs`
- Modify focused Postgres smoke/tests only as needed

**Step 1: Write failing tests**

Add tests proving:

- metadata repair cannot insert metadata while a final-object deletion fence is active;
- stale/expired fence tokens cannot delete metadata or bytes;
- deletion fence can be reacquired after expiry;
- deleting a metadata-missing final object requires both an active cleanup claim and the fence;
- existing `FinalObjectMetadataFence` failure semantics remain unchanged unless explicitly fenced.

Expected RED: tests fail because `FinalObjectMetadataFence` is only a marker and `ObjectMetadataStore` has no fence/delete contract.

**Step 2: Implement minimal fence contract**

Add a store-backed fence API that keeps the existing `FinalObjectMetadataFence` name in the deletion contract. The fence should carry repo id, object kind/id, canonical final key, owner, token, expiry, and created/updated timestamps. It must not expose object keys or raw errors in `Debug`.

Required behavior:

- `ObjectMetadataStore::put` fails closed or returns a stable retry/conflict error while an active deletion fence exists for the same repo/kind/id.
- Fence acquisition validates the canonical final object key and lease owner.
- Fence acquisition can create a fence for metadata-missing final objects.
- Fence acquisition for a metadata-present object must snapshot the metadata identity so a later delete protocol can verify it did not change.
- Future metadata deletion must require the matching active fence token.
- Fence release is idempotent for this non-destructive readiness slice; future completion must remain idempotent after successful byte deletion.

Postgres migration should add a small `object_deletion_fences` table keyed by `(repo_id, object_kind, object_id)` rather than relying on the `objects` row existing. `ObjectMetadataStore::put` must check this table in the same SQL transaction before inserting/updating metadata.

**Step 3: Verify GREEN**

```bash
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
```

**Step 4: Commit**

```bash
git add src/backend/object_cleanup.rs src/backend/blob_object.rs src/backend/core_transaction.rs src/backend/postgres.rs src/backend/postgres_migrations.rs migrations/postgres tests/postgres
git commit -m "feat: add final object deletion fence"
```

## Task 4: Bounded CAS-Lost Object Cleanup Readiness Worker

**Worker ownership:**

- Modify: `src/backend/object_cleanup.rs`
- Modify: `src/backend/blob_object.rs`
- Modify: `src/backend/durable_mutation.rs` only if claim metadata needs adjustment
- Modify focused tests in those files

**Step 1: Write failing tests**

Add tests:

- `cleanup_worker_marks_cas_lost_object_ready_only_when_unreachable_and_fenced`
- `cleanup_worker_preserves_object_reachable_from_ref_workspace_recovery_idempotency_or_review`
- `cleanup_worker_revalidates_after_fence_before_deletion_ready`
- `cleanup_worker_records_backoff_and_poison_without_raw_errors`
- `cleanup_worker_is_bounded_by_limit`

Expected RED: no worker exists and deletion readiness is not reported.

**Step 2: Implement worker**

Add `ObjectCleanupWorker` and summary types in `src/backend/object_cleanup.rs`.

Worker behavior:

- list due cleanup candidates bounded by repo, kind, and limit;
- process only `DurableMutationCasLostObjectCleanup` in this slice;
- acquire/reacquire a cleanup claim lease before action;
- run dry-run reachability;
- acquire `FinalObjectMetadataFence`;
- re-run reachability with the current claim allowlisted;
- verify object metadata exists and still matches the cleanup claim;
- treat metadata-missing final objects as repairable and block deletion readiness;
- validate the cleanup claim and fence immediately before reporting deletion readiness;
- release the cleanup claim after readiness so the dry-run signal is repeatable;
- leave final bytes, metadata, and cleanup-claim completion untouched in this slice;
- record failure/backoff with a redacted fixed diagnostic;
- poison after a conservative max-attempt threshold and avoid letting max-attempt rows starve newer claimable cleanup work.

Keep destructive final-object deletion and general unreachable commit/object deletion in dry-run/report-only mode until a later task adds a fully fenced, crash/retry-safe delete contract.

**Step 3: Verify GREEN**

```bash
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::durable_mutation --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/backend/object_cleanup.rs src/backend/blob_object.rs src/backend/durable_mutation.rs
git commit -m "feat: add fenced object cleanup dry-run worker"
```

## Task 5: Postgres GC Conformance

**Worker ownership:**

- Modify: `src/backend/postgres.rs`
- Modify: `migrations/postgres/`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Step 1: Write failing tests**

Add Postgres feature tests:

- `postgres_gc_dry_run_reports_unreachable_candidates_without_mutation`
- `postgres_final_object_fence_blocks_metadata_repair_race`
- `postgres_cleanup_worker_marks_only_unreachable_fenced_cas_lost_object_ready`
- `postgres_cleanup_claim_backoff_and_poison_are_fenced`

Expected RED: migration/API support is missing or incomplete.

**Step 2: Implement migration/adapters**

Add a new migration after `0009_durable_auth_session_foundation.sql` for:

- deletion fences;
- retry/backoff/poison columns on `object_cleanup_claims` if needed;
- due-candidate indexes.

Adapter behavior must preserve:

- canonical key constraints;
- repo-scoped operations;
- lease owner/token fencing;
- redacted errors;
- transaction boundaries around fence checks and metadata writes.

**Step 3: Verify GREEN**

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
```

**Step 4: Commit**

```bash
git add src/backend/postgres.rs src/backend/postgres_migrations.rs migrations/postgres tests/postgres
git commit -m "feat: persist fenced object cleanup gc"
```

## Task 6: Operator Route And Scheduler Integration

**Worker ownership:**

- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/mod.rs`
- Modify route tests in `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add route/scheduler tests:

- `vcs_recovery_status_includes_gc_dry_run_and_deletion_summary`
- `vcs_recovery_run_processes_bounded_object_cleanup_when_fenced`
- `vcs_recovery_run_keeps_deletion_dry_run_when_fences_fail`
- `vcs_recovery_status_redacts_object_keys_and_raw_store_errors`
- `durable_recovery_scheduler_drains_object_cleanup_phase_bounded`

Expected RED: `/vcs/recovery/run` currently reports object cleanup remaining but always has `attempted = 0`.

**Step 2: Implement integration**

Extend `GET /vcs/recovery`:

- include bounded `object_gc` dry-run summary;
- include counts for unreachable commits, unreachable objects, retained/blocker counts, deletion eligible count, deletion deferred count;
- keep existing compatibility fields.

Extend `POST /vcs/recovery/run`:

- after pre-visibility, post-CAS, and FS mutation phases, run object cleanup with remaining limit;
- report `limit`, `scanned/listed`, `attempted/processed`, `completed`, `deleted_final_objects`, `deletion_ready`, `backing_off`, `retryable_failures`, `poisoned`, `skipped/deferred`, and `remaining`;
- return a redacted correlation id as today;
- keep `deletion_enabled: false`; if blockers or fences fail, report skipped/deferred rather than deleting.

Extend scheduler tick:

- include object cleanup as a fourth bounded phase;
- update scheduler health/status serialization.

**Step 3: Verify GREEN**

```bash
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/server/routes_vcs.rs src/server/mod.rs
git commit -m "feat: run bounded object cleanup recovery"
```

## Task 7: Docs And Status

**Files:**

- Modify: `docs/project-status.md`
- Modify docs only if route shape changes require it: `docs/http-api-guide.md`

**Steps:**

- Record exactly what can become deletion-ready: only CAS-lost final objects with a durable cleanup claim, active worker lease, store-backed metadata fence, and immediate reachability revalidation.
- Record what remains dry-run only: destructive final-object byte/metadata deletion, general unreachable commit metadata, and any object retained by refs, workspaces, recovery, idempotency, reviews, or active non-current cleanup claims.
- Record that production hosted rollout, idempotency retention/quota, and secret-safe replay storage remain out of scope.
- Record final verification and perf numbers after gates finish.

**Verify and commit:**

```bash
git diff --check
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: update final object gc status"
```

## Task 8: Reviews, Verification, Merge

**Review sequence:**

1. Main session inspects each worker diff locally.
2. Run focused tests for the touched area.
3. Dispatch spec/correctness review with `gpt-5.5` high or xhigh.
4. Fix findings locally or send scoped fixes back to the owning worker.
5. Dispatch code-quality/security review with `gpt-5.5` high or xhigh.
6. Fix findings locally.

**Required final gates:**

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked backend::durable_mutation --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Then push `v2/foundation`, merge to `main`, rerun main verification if the merge is non-trivial, and push `main`.
