# Durable Core Read Source Cutover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Serve committed filesystem reads from durable commit/ref/object stores under the durable guarded backend path, while keeping durable mutable writes and broad `durable-cloud` startup out of scope.

**Architecture:** Add an async committed-tree reader over existing `CommitStore`, `RefStore`, `ObjectStore`, `TreeObject`, and blob encodings. Wire the reader into `DurableCoreRuntime` read methods and expose those methods through the existing guarded durable route overlay, so `STRATUM_BACKEND=durable` plus `STRATUM_DURABLE_COMMIT_ROUTE=1` reads the committed durable `main` tree instead of local `.vfs/state.bin` state. Keep `STRATUM_CORE_RUNTIME=durable-cloud` fail-closed; local staging for guarded commits remains local until durable mutations land.

**Tech Stack:** Rust, Tokio, async-trait, Axum route tests, existing durable backend stores, existing VCS tree/blob encodings, `Session` permission helpers, and release perf measurement with `/usr/bin/time -l`.

---

## Required Skills

Implementation and review subagents must use:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Use TDD for behavior changes: write the failing test, run it red, implement the smallest green change, then refactor.

## Context

The current durable backend can write guarded durable commits and read durable VCS metadata through the explicit guarded capability. Filesystem reads, tree rendering, find, grep, status, diff, and revert still come from local `StratumDb` when the route-facing core is `LocalCoreRuntime`.

This slice cuts over only committed read/source behavior for the guarded durable path:

- Durable committed reads use `main -> CommitRecord.root_tree -> TreeObject/Blob`.
- Guarded durable `POST /vcs/commit` still uses local `StratumDb` as the desired source snapshot.
- Durable FS write/mkdir/delete/copy/move remain local staging and are not persisted to durable stores.
- Durable status/diff/revert remain unsupported under the guarded durable committed-read overlay because there is no durable mutable workspace yet.

## Scope

In scope:

- New durable committed-tree read primitive over `CommitStore + RefStore + ObjectStore`.
- `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, and plain regex `grep_as` for committed durable `main`.
- Durable `CoreDb::list_refs` and `CoreDb::vcs_log_as` through durable stores.
- Guarded durable overlay routing for filesystem/search/tree reads.
- Tests proving durable reads ignore local filesystem/VCS state after a guarded commit and work with a fresh local DB.
- Fail-closed durable `status`, `diff`, and `revert` under the guarded durable overlay.
- Docs/status updates that call out local staging for writes and guarded commit source snapshots.

Out of scope:

- No durable filesystem mutations.
- No durable mutable workspace/session runtime.
- No durable revert/status/diff implementation beyond clear fail-closed behavior.
- No durable auth/session source.
- No broad `STRATUM_CORE_RUNTIME=durable-cloud` startup enablement.
- No semantic search or indexing.
- No new object/tree encoding.
- No Postgres schema change unless implementation proves it unavoidable.

## Design Constraints

- Reuse `TreeObject::deserialize`, `TreeEntry`, `TreeEntryKind`, and `ObjectKind::Blob/Tree`.
- Resolve the current committed root from durable `main`; do not consult local `StratumDb` for read content.
- Synthesize `StatInfo` deterministically from tree entry metadata and commit timestamp:
  - root: directory, mode `0o755`, uid/gid `0`, size = entry count, nlink `2`;
  - file/symlink: size from blob length, nlink `1`;
  - directory: size from child entry count, nlink `2`;
  - block size `4096`, blocks `size.div_ceil(512)` for nonzero size;
  - timestamps from the durable commit timestamp.
- Preserve ACL behavior with existing `Session::is_path_allowed` and `Session::has_permission_bits`.
- For `cat_with_stat_as`, follow symlinks with a bounded loop guard; for `stat_as`, report the symlink itself.
- Hide unreadable entries from `ls`, `tree`, `find`, and recursive `grep`; do not descend directories lacking execute permission.
- Sanitize store/codec failures to fixed durable read errors that do not include paths, object keys, request bodies, DB URLs, R2 keys, or raw backend errors.
- Keep default local behavior unchanged when no guarded durable capability is present.

## Acceptance

- Guarded durable commits can be read back through `GET /fs`, `GET /fs?stat=true`, `GET /tree`, `GET /search/find`, and `GET /search/grep` from durable stores.
- After local filesystem state diverges from a durable commit, guarded read routes still return the committed durable content.
- A fresh empty local `StratumDb` plus the same durable stores can serve the committed read routes, proving reads do not require local VCS/FS state.
- Guarded durable `GET /vcs/log` and `GET /vcs/refs` still read durable metadata.
- Guarded durable `GET /vcs/status`, `GET /vcs/diff`, and `POST /vcs/revert` fail closed with clear unsupported durable-workspace messages.
- Existing local runtime behavior remains unchanged.
- Existing guarded durable commit tests still pass.

## Task 1: Plan Commit

**Files:**

- Create: `docs/plans/2026-05-08-durable-core-read-source-cutover.md`

**Steps:**

1. Save this plan.
2. Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

3. Commit:

```bash
git add docs/plans/2026-05-08-durable-core-read-source-cutover.md
git commit -m "docs: plan durable core read source cutover"
```

## Task 2: Add RED Durable Reader Tests

**Files:**

- Create: `src/backend/committed_read.rs`
- Modify: `src/backend/mod.rs`

**Tests to add first:**

- `reads_file_stat_list_and_tree_from_durable_main`
  - Seed `StratumStores::local_memory()` with a root tree containing a file and directory.
  - Insert a `CommitRecord` whose `root_tree` points at that tree.
  - Point durable `main` at the commit.
  - Assert `cat_with_stat`, `stat`, `ls`, and `tree` use durable objects and metadata.

- `find_and_grep_traverse_committed_tree_with_permissions`
  - Seed readable and unreadable files.
  - Assert unreadable paths are hidden and grep only scans readable file blobs.

- `read_errors_are_redacted_for_missing_or_corrupt_objects`
  - Point `main` at a missing root tree or bad tree bytes.
  - Assert the rendered error contains a fixed durable read message and omits object IDs/secret markers.

**Expected RED command:**

```bash
cargo test --locked backend::committed_read --lib -- --nocapture
```

Expected before implementation: compile failure because the module/reader does not exist.

## Task 3: Implement Durable Committed-Tree Reader

**Files:**

- Create: `src/backend/committed_read.rs`
- Modify: `src/backend/mod.rs`

**Implementation shape:**

- Add `DurableCommittedFsReader<'a>` with borrowed `RepoId`, `RefStore`, `CommitStore`, and `ObjectStore`.
- Resolve `main` with `RefStore::get`, then hydrate `CommitRecord` with `CommitStore::get`.
- Load trees with `ObjectStore::get(repo, id, ObjectKind::Tree)` and `TreeObject::deserialize`.
- Load blobs with `ObjectStore::get(repo, id, ObjectKind::Blob)` and `ObjectStore::object_len` where bytes are not needed.
- Add methods:
  - `cat_with_stat_as(path, session) -> (Vec<u8>, StatInfo)`
  - `ls_as(path, session) -> Vec<LsEntry>`
  - `stat_as(path, session) -> StatInfo`
  - `tree_as(path, session) -> String`
  - `find_as(path, pattern, session) -> Vec<String>`
  - `grep_as(pattern, path, recursive, session) -> Vec<GrepResult>`
- Keep helper functions private and deterministic:
  - normalize absolute paths;
  - traverse components from root tree;
  - synthesize `StatInfo`;
  - render tree with the existing tree glyph format;
  - glob-match names for find;
  - compile regex once for grep;
  - sanitize all store/codec errors into fixed messages.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::committed_read --lib -- --nocapture
```

## Task 4: Wire Durable CoreDb Reads

**Files:**

- Modify: `src/server/core.rs`

**Tests to add first:**

- `durable_core_runtime_reads_committed_tree_without_local_state`
  - Seed durable stores, instantiate `DurableCoreRuntime`, call `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, and `grep_as`.

- `durable_core_runtime_status_diff_revert_fail_closed`
  - Assert status/diff/revert return redacted `NotSupported`.

**Implementation shape:**

- Add private `DurableCoreRuntime::committed_reader()`.
- Implement `CoreDb` for `DurableCoreRuntime`:
  - `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, `grep_as` delegate to the committed reader.
  - `list_refs` delegates to `durable_list_refs`.
  - `vcs_log_as` delegates to `durable_vcs_log_as`.
  - `status`, `diff`, `revert`, writes, auth, and mutable methods stay fail-closed.
- Add matching wrapper methods on `GuardedDurableCommitRoute` for the FS/search/tree reads.

**Verification:**

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

## Task 5: Route Guarded Durable Reads

**Files:**

- Modify: `src/server/core.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`

**Tests to add first:**

- `guarded_durable_fs_reads_use_committed_durable_tree`
  - Create a guarded durable commit from local content `committed`.
  - Mutate local file to `uncommitted`.
  - Call `get_fs`, `get_fs?stat=true`, and `get_tree`.
  - Assert responses still show durable committed content and metadata.

- `guarded_durable_fs_reads_survive_fresh_local_db`
  - Reuse the same durable stores with a fresh empty `StratumDb::open_memory()`.
  - Build a new guarded state.
  - Assert `GET /fs/{path}` and `/tree` still work.

- `guarded_durable_find_and_grep_use_committed_tree`
  - Assert find/grep see durable committed files and not later local-only files.

- `guarded_durable_status_diff_revert_fail_closed`
  - Assert `vcs_status`, `vcs_diff`, and `vcs_revert` return unsupported durable-workspace errors when the guarded capability is present.

**Implementation shape:**

- In `LocalCoreRuntime` read methods, if `guarded_durable_commit_route` is present, delegate FS/search/tree reads to it.
- In `LocalCoreRuntime::vcs_status_as`, `vcs_diff_as`, and `revert_as_with_path_check`, fail closed with a fixed durable mutable-workspace unsupported error when the guarded durable capability is present.
- Keep local writes and guarded commit source behavior unchanged.
- `routes_fs.rs` should need little or no routing change because it already calls `state.core`.
- `routes_vcs.rs` should only need route-test changes and maybe error expectation updates.

**Verification:**

```bash
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

## Task 6: Focused Review Fixes

**Files:**

- As indicated by reviewers.

**Review passes:**

- Spec/correctness review:
  - durable source of truth;
  - no local state dependence for committed reads;
  - status/diff/revert honesty;
  - no accidental durable write enablement.
- Code-quality/security review:
  - redaction;
  - permission filtering;
  - async traversal allocation/clone behavior;
  - no raw object keys, DB URLs, R2 credentials, request bodies, or tokens in errors/logs.

**Verification after fixes:**

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::committed_read --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

## Task 7: Docs, Full Verification, Merge

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Docs requirements:**

- Mark durable committed FS reads/list/stat/tree/find/grep as live only under the guarded durable backend path.
- Explicitly state local staging remains the source for guarded commit target snapshots.
- Explicitly state durable writes, mutable workspace status/diff/revert, auth/session source, and broad `durable-cloud` startup remain out of scope.
- Record warm perf real/user/sys, max RSS, and peak memory footprint.

**Required verification:**

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

**Commit sequence:**

```bash
git add docs/plans/2026-05-08-durable-core-read-source-cutover.md
git commit -m "docs: plan durable core read source cutover"
git add src/backend/committed_read.rs src/backend/mod.rs
git commit -m "feat: add durable committed read primitives"
git add src/server/core.rs src/server/routes_fs.rs src/server/routes_vcs.rs
git commit -m "feat: route guarded durable committed reads"
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: record durable committed read cutover"
git push origin v2/foundation
git checkout main
git merge --ff-only v2/foundation
cargo fmt --all -- --check
git diff --check
cargo test --locked --lib --tests
git push origin main
```
