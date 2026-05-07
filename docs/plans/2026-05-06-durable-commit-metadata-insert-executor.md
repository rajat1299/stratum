# Durable Commit Metadata Insert Executor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the internal durable commit metadata insert executor that records an unreachable `CommitRecord` only after planned object convergence has proven the root tree exists.

**Architecture:** Keep the executor scoped to `src/backend/core_transaction.rs` beside the existing metadata preflight, write-plan, and object convergence types. The executor derives the commit parent list from the plan source snapshot, builds the local-compatible commit ID from commit metadata, validates parent commit metadata, inserts through `CommitStore::insert`, validates the returned record, and returns a redacted summary. It does not accept or touch ref, workspace, audit, idempotency, auth, or route state, so the inserted commit remains unreachable until the later ref CAS visibility slice.

**Tech Stack:** Rust, Tokio async tests, existing `CommitStore`, `CommitRecord`, `DurableCoreCommitObjectTreeWritePlan`, `DurableCoreObjectConvergence`, local in-memory stores, focused fake stores, cargo gates, and release perf measurement with `/usr/bin/time -l`.

---

## CTO Plan And Status Read

The CTO plan's durable commit path requires durable object bytes and tree metadata before commit metadata insertion, then makes the commit visible only through a later ref compare-and-swap. Current project status confirms object convergence now writes planned blob/tree objects through `ObjectStore` and verifies `root_tree_id` as a durable tree object. The next safe slice is therefore commit metadata insertion only: insert a `CommitRecord` after convergence, validate parent metadata, keep the commit unreachable by refs, and stop before ref CAS, workspace-head, audit, idempotency, repair, and live routing.

SMFS extraction guidance remains mostly deferred. Its durable queue claim/finalize/backoff semantics map to the later post-CAS completion/recovery envelope, not this metadata insert. Its dirty/source freshness idea is already represented here by Stratum's parent commit/ref-version source snapshot, not timestamps. Do not copy SMFS latest-wins or SQLite cache behavior.

## Scope

In scope:

- Add an internal async commit metadata insert method for `DurableCoreCommitObjectTreeWritePlan`.
- Require a `DurableCoreObjectConvergence` whose repo ID and root tree ID match the plan.
- Build a deterministic `CommitRecord` using:
  - repo ID from convergence;
  - root tree ID from convergence/plan;
  - parents from `DurableCoreCommitSourceSnapshot`;
  - timestamp, author, and message supplied by the caller;
  - changed paths from the plan.
- Compute the commit ID using the existing local VCS commit-object identity rule: serialize a `CommitObject` with a zero placeholder ID, then hash those bytes.
- Check existing parent commit metadata with `CommitStore::contains` before insert.
- Insert only through `CommitStore::insert`.
- Treat identical duplicate commit metadata as idempotent success.
- Sanitize conflicting duplicate inserts, missing parent metadata, missing root-tree FK/store failures, and mismatched returned records without echoing commit messages, authors, paths, raw bytes, backend SQL, or secret-bearing store errors.
- Return a redacted summary with repo ID, commit ID, root tree ID, parent IDs, changed-path count, and timestamp.
- Add focused tests for unborn-parent success, existing-parent success, missing parent rejection, idempotent duplicate insert, conflicting duplicate redaction, missing root-tree/FK failure redaction, mismatched returned record rejection, debug redaction, and route fail-closed boundaries.
- Update `docs/project-status.md` after implementation and review fixes.

Out of scope:

- No ref CAS visibility.
- No workspace-head update.
- No audit append.
- No idempotency completion/replay.
- No repair scheduling, distributed lock/fencing, or cleanup policy.
- No live durable `POST /vcs/commit` route execution.
- No durable auth/session path, durable filesystem/search/tree serving, hosted R2 routing cutover, or Postgres-specific route wiring.
- No local `StratumDb` route behavior change.

## Performance Rules For This Slice

- After each meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

- Record warm wall/user/sys time, maximum resident set size, and peak memory footprint.
- Keep insert allocation-light: clone changed paths only when building `CommitRecord`, clone author/message once, avoid hydrating refs/workspaces/audit/idempotency, and prefer `CommitStore::contains` for parent validation.
- GPU efficiency is not applicable to this metadata-only backend path.

## Task 1: Add RED Tests For Commit Metadata Insert

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add a sibling test module such as `backend::core_transaction::tests::durable_core_commit_metadata_insert`:

- `metadata_insert_records_unborn_commit_after_object_convergence_without_ref_visibility`
  - build a `VirtualFs` with private file content and nested paths.
  - build an unborn `DurableCoreCommitObjectTreeWritePlan`.
  - converge objects into `LocalMemoryObjectStore`.
  - call the new metadata insert method with `LocalMemoryCommitStore`.
  - assert the returned redacted summary has repo ID, commit ID, root tree ID, zero parents, changed-path count, and timestamp.
  - load the commit from `CommitStore::get` and assert root tree, parents, author, message, timestamp, and changed paths match.
  - assert a separate `LocalMemoryRefStore` remains empty to prove no visibility step occurred.
- `metadata_insert_records_existing_parent_after_parent_validation`
  - seed `LocalMemoryCommitStore` with a parent `CommitRecord`.
  - build a plan whose source parent state references that parent and version.
  - converge objects and insert metadata.
  - assert the inserted record parents contain only the parent commit ID.
- `metadata_insert_rejects_missing_parent_without_inserting`
  - build an existing-parent plan but do not seed that parent in `CommitStore`.
  - assert a redacted `CorruptStore` such as `durable commit parent metadata is missing`.
  - assert `CommitStore::list` remains empty.
- `metadata_insert_is_idempotent_for_matching_existing_commit`
  - insert the same converged plan twice with identical author/message/timestamp.
  - assert both summaries match and the commit list contains one record.
- `metadata_insert_rejects_conflicting_duplicate_without_leaking_inputs`
  - compute or obtain the expected commit ID.
  - pre-seed a `LocalMemoryCommitStore` with the same commit ID but a conflicting record containing a private message/path-like string.
  - call metadata insert and assert the error is redacted and does not include commit ID, message, author, path, or raw bytes.
- `metadata_insert_wraps_root_tree_fk_failure_without_leaking_store_message`
  - use a fake `CommitStore` whose `insert` returns a leaky `InvalidArgs`/`CorruptStore` simulating a missing root-tree FK.
  - assert the returned error is a fixed redacted metadata-insert failure.
- `metadata_insert_rejects_store_returning_mismatched_record`
  - use a fake `CommitStore` whose `insert` returns a different `CommitRecord`.
  - assert a fixed redacted corruption error and no leaked inputs.
- `metadata_insert_debug_redacts_message_author_paths_and_bytes`
  - format the returned summary with `Debug`.
  - assert IDs/counts/timestamp are visible, but commit message, author, file paths, and file bytes are not.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_metadata_insert --lib -- --nocapture
```

Expected before implementation: tests fail to compile or fail because the metadata insert method/result types do not exist.

## Task 2: Implement The Metadata Insert Executor

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Add summary type**

Add a redacted summary near the convergence types:

- `DurableCoreCommitMetadataInsert`
  - fields: `repo_id: RepoId`, `commit_id: CommitId`, `root_tree_id: ObjectId`, `parents: Vec<CommitId>`, `changed_path_count: usize`, `timestamp: u64`.
  - helpers: `repo_id()`, `commit_id()`, `root_tree_id()`, `parents()`, `changed_path_count()`, `timestamp()`.
  - custom `Debug` that prints IDs/counts/timestamp only.

**Step 2: Add commit ID/record helper**

Add a private helper that builds the expected `CommitRecord`:

```rust
fn durable_commit_record_for_metadata_insert(
    repo_id: RepoId,
    plan: &DurableCoreCommitObjectTreeWritePlan,
    timestamp: u64,
    author: &str,
    message: &str,
) -> CommitRecord
```

Requirements:

- parents are `[]` for `DurableCoreCommitParentState::Unborn`.
- parents are `[target]` for `DurableCoreCommitParentState::Existing { target, .. }`.
- commit ID is derived by serializing `CommitObject { id: ObjectId::from_bytes(&[0; 32]), tree: plan.root_tree_id(), parent: first_parent_object_id, timestamp, message, author, changed_paths: plan.changed_paths().to_vec() }` and hashing those bytes.
- The `CommitRecord` root tree must equal `plan.root_tree_id()`.

**Step 3: Add async insert method**

Implement:

```rust
impl DurableCoreCommitObjectTreeWritePlan {
    pub(crate) async fn insert_commit_metadata(
        &self,
        convergence: &DurableCoreObjectConvergence,
        commit_store: &dyn CommitStore,
        timestamp: u64,
        author: &str,
        message: &str,
    ) -> Result<DurableCoreCommitMetadataInsert, VfsError>
}
```

Implementation requirements:

- Verify `convergence.root_tree_id() == self.root_tree_id()` and `convergence.object_count() == self.planned_objects().len()`.
- Use `convergence.repo_id()` as the repo ID.
- Build the expected `CommitRecord`.
- For each parent ID, call `commit_store.contains(convergence.repo_id(), parent).await`.
- If a parent check errors, return fixed redacted `CorruptStore { message: "durable commit parent metadata check failed" }`.
- If a parent is missing, return fixed redacted `CorruptStore { message: "durable commit parent metadata is missing" }`.
- Call `commit_store.insert(record.clone()).await`.
- If insertion returns a store error, map it to fixed redacted `CorruptStore { message: "durable commit metadata insert failed" }`, except a conflicting duplicate may be sanitized to `AlreadyExists { path: "commit".to_string() }` if that matches local conventions better. Do not return raw commit IDs, messages, authors, paths, SQL details, or adapter messages.
- If insertion returns a record different from the expected record, return fixed redacted `CorruptStore { message: "durable commit metadata insert returned mismatched record" }`.
- Return `DurableCoreCommitMetadataInsert`.
- Do not call `RefStore`, workspace metadata, audit, idempotency, server route code, or local `StratumDb`.

**Step 4: Verify GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_metadata_insert --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_object_convergence --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: metadata insert and existing durable core tests pass, route execution remains fail-closed, and release perf remains in the current backend band.

**Step 5: Commit implementation**

```bash
git add src/backend/core_transaction.rs
git commit -m "feat: add durable commit metadata insert executor"
```

## Task 3: Review Fixes And Docs

**Files:**

- Modify: `src/backend/core_transaction.rs` if reviews find issues.
- Modify: `docs/project-status.md`.

**Step 1: Review**

Request two reviews:

- Spec/correctness review:
  - Does metadata insert require object convergence first?
  - Does it validate existing parent commit metadata?
  - Does it keep the new commit unreachable by refs?
  - Does it stop before ref CAS, workspace-head, audit, idempotency, repair, auth, and routes?
  - Are duplicate/conflict/FK/store errors redacted?
- Code-quality/performance/security review:
  - Is commit ID derivation aligned with local VCS commit identity?
  - Are clones limited to record construction?
  - Is `CommitStore::contains` used for parent validation?
  - Is summary `Debug` safe?
  - Are fake-store tests tight and maintainable?

Fix findings locally and rerun focused tests plus release perf after each meaningful diff.

**Step 2: Update status**

Record that:

- Durable commit metadata insert now creates an unreachable durable `CommitRecord` after object convergence.
- It validates parent commit metadata and root-tree/convergence consistency first.
- It treats identical duplicate insert as idempotent success.
- It sanitizes conflicting duplicate, missing parent, FK/store, and mismatched-return errors.
- It still stops before ref CAS visibility, workspace-head, audit, idempotency, repair, and live durable routing.
- The next slice is durable ref CAS visibility.

**Step 3: Commit docs/status**

```bash
git add docs/project-status.md
git commit -m "docs: record durable commit metadata insert"
```

## Task 4: Full Gates, Push, And Main Merge

Run from the v2 worktree:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

Push and merge:

```bash
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git fetch origin
git merge --no-edit origin/v2/foundation
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
git push origin main
```

Expected:

- v2 and main pass gates.
- The pre-existing untracked `.claude/` in the main worktree remains untouched.
- Durable commit routing remains disabled after metadata insert lands.
