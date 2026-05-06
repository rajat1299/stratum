# Durable Planned Object Convergence Executor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the internal durable planned object convergence executor that writes an existing commit object/tree write plan through `ObjectStore` and proves the planned root tree exists before any commit metadata insert.

**Architecture:** Keep convergence scoped to `src/backend/core_transaction.rs` and accept only `RepoId`, `ObjectStore`, and `DurableCoreCommitObjectTreeWritePlan`. The executor writes the plan's already-validated blob/tree objects in deterministic order with `ObjectStore::put`, verifies each returned stored object matches the plan, confirms the final root tree exists, and returns a redacted convergence summary. It does not receive or touch commit, ref, workspace, audit, idempotency, auth, or route state.

**Tech Stack:** Rust, async trait object calls over existing `ObjectStore`, durable transaction planning types, local in-memory object store tests, focused fake object-store tests, cargo gates, and release perf measurement with `/usr/bin/time -l`.

---

## CTO Plan And Status Read

The CTO plan's durable commit path requires object bytes and tree objects to exist before commit metadata is inserted, because refs only make a commit visible after object and commit metadata are durable. Current project status confirms the read-only write-plan preflight exists and that `commits.root_tree_id` has a Postgres foreign key to `objects(repo_id, kind, object_id)`. The next safe slice is therefore object convergence only: write the plan's blob/tree objects, prove `root_tree_id` exists as a durable tree object, and stop before `CommitStore::insert`.

## Scope

In scope:

- Add an internal async convergence method for `DurableCoreCommitObjectTreeWritePlan`.
- Write only planned blob/tree objects by calling `ObjectStore::put` with existing `ObjectWrite` data.
- Preserve the plan's child-before-parent order.
- Treat matching pre-existing objects as success through the object store's idempotent `put` contract.
- Verify every returned `StoredObject` has the expected repo, object ID, kind, and bytes.
- Verify `plan.root_tree_id()` exists as `ObjectKind::Tree` after convergence.
- Return a redacted convergence summary with repo ID, root tree ID, object count, and per-object kind/id/byte length metadata.
- Add focused tests for success, idempotent replay, corrupt store mismatch, missing root after writes, redacted debug/error behavior, and route fail-closed boundaries.
- Update `docs/project-status.md` after implementation and review fixes.

Out of scope:

- No live durable `POST /vcs/commit` route execution.
- No `CommitStore::insert`.
- No ref CAS, workspace-head update, audit append, idempotency completion/replay, repair scheduling, distributed lock/fencing, or cleanup policy.
- No R2-specific object routing change; this uses the existing `ObjectStore` contract only.
- No local `StratumDb` route behavior change.

## Performance Rules For This Slice

- After each meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

- Record warm wall/user/sys time, maximum resident set size, and peak memory footprint.
- Keep convergence allocation-light: reuse planned object order, clone bytes only when constructing `ObjectWrite`, and avoid hydrating commit/ref/workspace state.
- Do not add background tasks, caches, or route startup behavior.
- GPU efficiency is not applicable to this metadata/object-storage path.

## Task 1: Add Red Tests For Planned Object Convergence

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add focused tests in `backend::core_transaction::tests::durable_core_commit_write_plan` or a sibling `durable_core_commit_object_convergence` module:

- `convergence_writes_planned_objects_and_confirms_root_tree`
  - builds a `VirtualFs` with nested blob/tree content.
  - builds a `DurableCoreCommitObjectTreeWritePlan`.
  - converges the plan into `LocalMemoryObjectStore`.
  - asserts the returned summary has the repo ID, root tree ID, and planned object count.
  - asserts every planned object can be read from the object store with matching kind and bytes.
  - asserts `contains(repo, plan.root_tree_id(), ObjectKind::Tree)` is true.
- `convergence_is_idempotent_for_matching_existing_objects`
  - converges the same plan twice into the same object store.
  - asserts both summaries match and all planned objects still round-trip.
- `convergence_rejects_store_returning_wrong_object_without_leaking_bytes`
  - uses a small fake `ObjectStore` whose `put` returns a `StoredObject` with the wrong kind, ID, repo, or bytes.
  - asserts `VfsError::CorruptStore` or `InvalidArgs`.
  - asserts the error string does not contain planned file bytes, paths, or raw tree serialization.
- `convergence_rejects_missing_root_after_puts_without_commit_side_effects`
  - uses a fake `ObjectStore` whose `put` appears successful but whose `contains(root_tree_id, Tree)` returns false.
  - asserts a redacted corruption-style error.
- `convergence_summary_debug_redacts_object_bytes_and_paths`
  - formats the convergence summary with `Debug`.
  - asserts it includes object counts/IDs but not file contents, file paths, or serialized tree bytes.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_object_convergence --lib -- --nocapture
```

Expected before implementation: tests fail to compile or fail because the convergence summary/executor does not exist.

## Task 2: Implement The Convergence Executor

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Add summary types**

Add redacted summary types near the write-plan types:

- `DurableCoreConvergedObject`
  - fields: `kind: ObjectKind`, `id: ObjectId`, `byte_len: usize`.
  - custom `Debug` that prints only kind, ID, and byte length.
- `DurableCoreObjectConvergence`
  - fields: `repo_id: RepoId`, `root_tree_id: ObjectId`, `objects: Vec<DurableCoreConvergedObject>`.
  - helpers: `repo_id()`, `root_tree_id()`, `objects()`, `object_count()`.
  - custom `Debug` that prints counts and IDs only.

**Step 2: Add async convergence method**

Implement:

```rust
impl DurableCoreCommitObjectTreeWritePlan {
    pub(crate) async fn converge_objects(
        &self,
        repo_id: &RepoId,
        object_store: &dyn ObjectStore,
    ) -> Result<DurableCoreObjectConvergence, VfsError>
}
```

Implementation requirements:

- Iterate `self.planned_objects()` in order.
- For each planned object, call `object_store.put(planned.object_write_for_repo(repo_id)).await`.
- Validate the returned `StoredObject`:
  - `stored.repo_id == *repo_id`
  - `stored.id == planned.id()`
  - `stored.kind == planned.kind()`
  - `stored.bytes == planned.bytes()`
- If validation fails, return a redacted error such as `VfsError::CorruptStore { message: "object convergence returned mismatched object".to_string() }`.
- Push only redacted `DurableCoreConvergedObject` entries into the summary.
- After all puts, call `object_store.contains(repo_id, self.root_tree_id(), ObjectKind::Tree).await`.
- If the root tree is missing, return a redacted corruption-style error such as `VfsError::CorruptStore { message: "object convergence did not persist root tree".to_string() }`.
- Return `DurableCoreObjectConvergence`.
- Do not call any commit, ref, workspace, audit, idempotency, or server route code.

**Step 3: Verify GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_object_convergence --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: convergence and existing write-plan tests pass, durable route execution remains fail-closed, and release perf remains in the existing backend band.

**Step 4: Commit implementation**

```bash
git add src/backend/core_transaction.rs
git commit -m "feat: add durable object convergence executor"
```

## Task 3: Review Fixes And Docs

**Files:**

- Modify: `src/backend/core_transaction.rs` if reviews find issues.
- Modify: `docs/project-status.md`.

**Step 1: Review**

Request two reviews:

- Spec/correctness review:
  - Does convergence stop before commit metadata, ref CAS, workspace, audit, and idempotency mutations?
  - Is the root-tree existence check sufficient for the next commit metadata FK slice?
  - Are store mismatch and missing-root errors redacted?
- Code-quality/performance review:
  - Is async trait-object usage clean?
  - Are clones limited to `ObjectWrite` ownership?
  - Is the summary safe to debug/log?
  - Are fake-store tests tight and maintainable?

Fix findings locally and rerun focused tests plus release perf after each meaningful diff.

**Step 2: Update status**

Record that:

- Planned object convergence now writes planned blob/tree objects through `ObjectStore`.
- It is idempotent for existing matching objects.
- It confirms `root_tree_id` exists as a durable tree object.
- It still stops before commit metadata insert, ref CAS visibility, workspace-head, audit, idempotency, repair, and live durable routing.
- The next slice is durable commit metadata insert executor.

**Step 3: Commit docs/status**

```bash
git add docs/project-status.md
git commit -m "docs: record durable object convergence"
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
- Durable commit routing remains disabled after object convergence lands.
