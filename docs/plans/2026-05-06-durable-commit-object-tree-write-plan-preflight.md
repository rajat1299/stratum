# Durable Commit Object/Tree Write-Plan Preflight Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the first read-only durable commit write-plan preflight for object and tree identity construction, without writing objects, inserting commit metadata, updating refs, or enabling live durable commit routing.

**Architecture:** Build a deterministic internal plan from a source filesystem snapshot and an explicit base path-record snapshot. The plan normalizes changed paths, computes blob and tree object IDs from the same serialization primitives as the local VCS path, emits an ordered object write set suitable for later idempotent object convergence, and records the final root tree ID. This slice keeps durable `POST /vcs/commit` execution fail-closed; it only creates a pure preflight artifact that later executor slices can consume.

**Tech Stack:** Rust, existing `VirtualFs`, `ObjectId`, `TreeObject`, `ChangedPath`, durable transaction planning types, Tokio/cargo tests, and release perf gates with `/usr/bin/time -l`.

---

## CTO Plan And Status Read

The CTO plan prioritizes replacing single-process state with durable Postgres metadata plus object storage while preserving local-state compatibility. Its commit model is explicit: commits have parent(s), root tree, and write set; refs are compare-and-swap visible pointers; durable writes must stage changed blobs, create tree objects and a commit, then update the target ref only under the expected parent/version.

The current project status says durable Phase 2 is underway, not complete. Durable `create_ref` and `update_ref` can execute internally, and commit metadata preflight validates the parent/read-side metadata. Live durable commit execution remains fail-closed. The Postgres schema requires `commits.root_tree_id` to reference an existing object row, so commit metadata insertion cannot be the next write step. The next slice is therefore the read-only object/tree write-plan preflight.

## Scope

In scope:

- Add internal durable commit object/tree write-plan types beside the existing durable transaction preflight types.
- Accept an explicit base/source path-record snapshot so parent source freshness is represented by commit/ref version state, not wall-clock timestamps.
- Traverse a source `VirtualFs` snapshot and compute blob, symlink-blob, directory tree, and root tree object IDs.
- Serialize tree objects with the same object identity rules as the existing local VCS path.
- Produce a deterministic ordered write set with child blobs/subtrees before parent trees and the final root tree last.
- Deduplicate identical object IDs when kind and bytes match, and reject cross-kind or cross-byte object ID collisions because current object stores key identity by raw object ID.
- Produce normalized `ChangedPath` output from the explicit base path records and current source snapshot records.
- Provide a helper that maps planned objects into existing `ObjectWrite` records for a repo, without calling `ObjectStore::put`.
- Keep durable commit route execution, object convergence, commit metadata insertion, ref CAS, workspace-head update, audit append, idempotency completion, repair scheduling, and startup/auth serving fail-closed.

Out of scope:

- No live durable `POST /vcs/commit` route execution.
- No `stores.objects.put`, R2 byte-store writes, or Postgres object metadata writes.
- No `CommitStore::insert`.
- No ref mutation or commit visibility.
- No workspace-head mutation, audit/idempotency mutation, distributed lock/fencing, repair worker, or cleanup policy.
- No attempt to copy SMFS cache/queue code. SMFS only informs edge-case test language around source freshness and atomic-save style write-set shape.

## Performance Rules For This Slice

- After each meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

- Record warm wall/user/sys time, maximum resident set size, and peak memory footprint.
- Keep planning read-only and allocation-conscious: one deterministic filesystem traversal, no store calls, no background tasks, no durable runtime startup.
- Deduplicate planned object bodies by raw `ObjectId` when kind and bytes match to avoid repeated object-write payloads for identical content, and reject raw-ID collisions that cannot converge in the existing object-store contract.
- Preserve deterministic output ordering for stable tests and later object convergence.
- GPU efficiency is not applicable to this backend metadata/object planning path.

## Task 1: Add Red Tests For Write-Plan Preflight

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add focused tests in `backend::core_transaction::tests::durable_core_commit_write_plan`:

- `preflight_plans_blobs_trees_and_root_without_store_writes`
  - builds a `VirtualFs` with a file, nested directory, and symlink.
  - passes an empty base path-record snapshot.
  - asserts the plan contains changed paths for the created paths.
  - asserts planned blob/symlink/tree IDs match `ObjectId::from_bytes` of the planned bytes.
  - asserts root tree is the last planned object and equals `plan.root_tree_id`.
  - asserts no object-store API is needed.
- `preflight_orders_children_before_parent_trees`
  - builds a nested tree.
  - asserts every tree object appears after its child object IDs in the planned write set.
- `preflight_deduplicates_identical_planned_objects`
  - builds two files with identical bytes.
  - asserts the blob object ID appears once while both file paths are represented in the root tree or child tree serialization.
- `preflight_normalizes_source_snapshot_changed_paths`
  - passes a base snapshot containing an older file, a renamed/deleted path, and a metadata-only variant.
  - builds the current source snapshot with create, update, delete, rename-equivalent create/delete, and metadata-only cases.
  - asserts changed paths are sorted and use existing `ChangedPath`/`ChangeKind` semantics.
- `preflight_converts_plan_to_repo_object_writes_without_mutating`
  - maps planned objects to `ObjectWrite` values for `RepoId::local()`.
  - asserts repo, kind, id, and bytes are preserved.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
```

Expected before implementation: the tests fail to compile or fail because the durable object/tree write-plan types and planner do not exist.

## Task 2: Implement The Read-Only Planner

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Add planning types**

Add durable planning structures near the existing commit metadata preflight types:

- `DurableCoreCommitSourceSnapshot`
  - owns the explicit base path records used to compare the source snapshot.
  - records the parent/ref source contract already established by metadata preflight.
- `DurableCorePlannedObject`
  - fields: `kind: ObjectKind`, `id: ObjectId`, `bytes: Vec<u8>`.
  - helper: `into_object_write(repo_id: RepoId) -> ObjectWrite`.
- `DurableCoreCommitObjectTreeWritePlan`
  - fields: source snapshot contract, `root_tree_id`, ordered planned objects, current path records, changed paths.
  - helpers: `planned_objects()`, `changed_paths()`, `root_tree_id()`, and `object_writes_for_repo(&RepoId)`.

Use existing public or crate-visible `PathRecord`, `ChangedPath`, `diff_path_maps`, and `worktree_path_records` APIs instead of inventing a parallel diff model.

**Step 2: Add deterministic snapshot traversal**

Implement a pure helper that mirrors the local VCS snapshot identity rules:

- For regular files, planned object bytes are file contents and kind is `ObjectKind::Blob`.
- For symlinks, planned object bytes are symlink target bytes and kind is `ObjectKind::Blob`.
- For directories, recursively plan children first, then serialize a `TreeObject` with `TreeEntry` values using existing metadata and object IDs.
- The root directory returns the final root tree ID.
- Planned objects are deduplicated by raw object ID while preserving first post-order occurrence. A repeated ID is accepted only when kind and bytes match; same-ID/different-kind or same-ID/different-bytes cases return a redacted planning error.
- No store traits are called.

**Step 3: Build the write plan**

Add a constructor such as:

```rust
impl DurableCoreCommitObjectTreeWritePlan {
    pub(crate) fn build(
        source: DurableCoreCommitSourceSnapshot,
        fs: &VirtualFs,
    ) -> Result<Self, VfsError>
}
```

Implementation requirements:

- Convert the source base records into a `PathMap`.
- Compute current path records from `worktree_path_records(fs)`.
- Compute `changed_paths` through `diff_path_maps`.
- Run deterministic object/tree planning from `fs.root_id()`.
- Return the plan without mutating stores or durable runtime state.

**Step 4: Verify GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: focused tests pass, and release perf remains in the same band because the slice does not touch perf-test hot paths.

**Step 5: Commit implementation**

```bash
git add src/backend/core_transaction.rs
git commit -m "feat: add durable commit object tree write plan"
```

## Task 3: Preserve Durable Route Fail-Closed Boundaries

**Files:**

- Modify: `src/server/core.rs` only if focused tests expose a route-boundary regression.

**Step 1: Verify existing fail-closed tests**

Run:

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Expected:

- Durable `commit_as` remains `NotSupported`.
- Durable auth/startup/HTTP serving remain fail-closed.
- `list_refs`, filesystem/search/tree, log/status/diff/revert, and broad route execution are still disabled.

**Step 2: Commit only if route-boundary code or tests changed**

```bash
git add src/server/core.rs tests/server_startup.rs
git commit -m "test: preserve durable commit route boundary"
```

Skip this commit if no route-boundary changes are needed.

## Task 4: Docs And Status

**Files:**

- Modify: `docs/project-status.md`

**Step 1: Update status**

Record that:

- Durable commit object/tree write-plan preflight now exists internally.
- It computes planned blob/tree object IDs and the final root tree ID.
- It remains read-only and does not write objects, insert commit metadata, update refs, mutate workspace head, append audit records, complete idempotency records, or enable live durable commit routing.
- The next slice is planned object convergence through `ObjectStore`.

**Step 2: Run docs/perf verification**

Run:

```bash
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 3: Commit docs/status**

```bash
git add docs/project-status.md
git commit -m "docs: record durable commit write plan"
```

## Task 5: Review, Full Gates, Push, And Main Merge

**Files:**

- Inspect all diffs locally before committing or merging.

**Step 1: Review**

- Request one spec/correctness review focused on transaction staging boundaries:
  - Does the plan stop before all writes and visibility changes?
  - Does the source snapshot contract preserve parent/ref freshness assumptions?
  - Are object IDs and changed paths consistent with local VCS semantics?
- Request one code-quality review focused on Rust maintainability/performance:
  - Are clones and allocations limited to owned plan outputs?
  - Is tree planning deterministic?
  - Is deduplication correct without changing tree semantics?

**Step 2: Run v2 gates**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo test --locked --features postgres backend::postgres -- --nocapture
cargo test --locked --test fuser_smoke -- --nocapture
./scripts/smoke_postgres_migrations.sh
./scripts/smoke_postgres_backend.sh
./scripts/smoke_audit.sh
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

Record release perf real/user/sys, max RSS, and peak memory footprint.

**Step 3: Push v2**

```bash
git status --short --branch
git push origin v2/foundation
```

**Step 4: Merge to main and verify main**

Run:

```bash
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

- v2 and main both pass gates.
- `main` keeps the pre-existing untracked `.claude/` untouched.
- Live durable commit routing remains disabled after the read-only plan slice.
