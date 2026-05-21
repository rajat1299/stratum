# Sparse VFS Cache Schema Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a local SQLite schema/model foundation for a future sparse Stratum VFS cache without enabling sparse FUSE, hydration, or durable-cloud mount behavior.

**Architecture:** Keep the cache foundation in the `stratum` application/runtime crate for now, because `stratum-core` currently owns only neutral domain types. Add a small `sparse_cache` module that can create and version a local SQLite database and model cache views scoped by Stratum durable identities: repo id, commit/root tree id, optional ref name/version, object id, normalized path, and inode identity. The module is schema/model only and is not wired into HTTP, durable committed reads, local `.vfs/state.bin`, or `stratum-mount`.

**Tech Stack:** Rust 2024, Cargo workspace, `rusqlite` with bundled SQLite, Serde JSON for bounded metadata maps, Stratum `RepoId`/`CommitId`/`ObjectId`/`RefName` types, existing POSIX `StatInfo` shape, existing optional `fuser` mount compile gate, and local provider-free cargo tests.

---

## Required Skills And Agent Discipline

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare against this plan, the Slice 10 acceptance criteria, `docs/http-api-guide.md`, and `docs/project-status.md`.

The main session owns integration, local review, verification, commits, merge to `main`, and pushes. Subagents may implement or review scoped work, but the main session must inspect diffs and rerun gates before accepting work.

## Required Context Read

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-21-crate-split-foundation.md`
- `docs/plans/2026-05-21-distributed-lock-service.md`
- `docs/plans/2026-05-20-pre-cutover-load-and-chaos-suite.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/vfs/traits.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/vfs/types.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/vfs/path.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/schema.sql`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/db.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/fs.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/hydration.rs`

## Current Inventory

Durable identity model:

- `RepoId` is a bounded durable namespace identifier and `ObjectStore` keys reads by `RepoId`, `ObjectId`, and `ObjectKind` (`src/backend/mod.rs:53-162`).
- `CommitRecord` stores `repo_id`, commit id, `root_tree`, parents, timestamp, message, author, and changed paths (`src/backend/mod.rs:164-183`).
- Ref visibility is CAS/versioned through `RefRecord { repo_id, name, target, version }` and source-checked update shapes (`src/backend/mod.rs:198-267`).
- Durable committed reads resolve the current visible root by repo/ref/session ref, then load commit/root tree/object data through durable stores (`src/backend/committed_read.rs:438-583`).
- Durable compare/status tracks explicit base/head commit and base/head root tree identity (`src/backend/committed_read.rs:35-51`, `src/backend/committed_read.rs:87-150`).

Tree and POSIX metadata model:

- Durable tree entries currently contain name, kind (`Blob`, `Tree`, `Symlink`), object id, mode, uid, gid, optional MIME type, and custom attrs (`src/store/tree.rs:5-29`).
- Local POSIX inodes model file/directory/symlink kinds, mode, uid/gid, nlink, block size, timestamp nanos, MIME type, and custom attrs (`src/fs/inode.rs:25-59`).
- Local hardlink/orphan semantics already exist: unlink decrements nlink and pending-delete keeps nlink-zero open inodes until release (`src/fs/mod.rs:350-372`, `src/fs/mod.rs:650-752`), while FUSE exposes hardlink creation through `link` (`src/fuse_mount.rs:369-396`).
- FUSE currently converts existing `StatInfo` into `FileAttr`, including inode id, size, blocks, mode, nlink, uid/gid, timestamps, and block size (`src/fuse_mount.rs:627-649`).

Mount/runtime boundaries:

- `stratum-mount` still rejects durable-cloud/non-server runtime surfaces before opening state, then mounts `db.snapshot_fs()` (`src/bin/stratum_mount.rs:23-35`).
- This slice must not wire sparse cache into `stratum-mount`, HTTP routes, durable-cloud runtime selection, local `.vfs/state.bin`, MCP, or REPL.

smfs cache ideas to adapt:

- Adapt SQLite local cache primitives: WAL-like setup, config/schema version, inode metadata, dentry mapping, chunk rows, symlink targets, and statfs counts (`smfs .../cache/schema.sql:7-53`).
- Use path normalization rules that collapse repeated slash and `.`, reject root escape through `..`, reject NUL, and bound component length (`smfs .../vfs/path.rs:12-42`).
- Keep read-side cache/hydration as future work. The useful invariant is that hot VFS paths should not await network IO, but this slice only adds schema/model tests (`smfs .../cache/hydration.rs:1-20`).

smfs semantics to reject:

- Do not import `dirty_since > remote updatedAt` local-wins behavior (`smfs .../cache/schema.sql:5-22`).
- Do not import path-primary latest-wins push queue coalescing, which drops intermediate writes by filepath (`smfs .../cache/schema.sql:67-88`, `smfs .../cache/db.rs:261-330`).
- Do not let SQLite cache state become durable-cloud source of truth. Stratum visibility remains durable commit metadata plus ref CAS/source-check/idempotency/recovery semantics.

## Proposed Schema

Add `src/sparse_cache/` with an embedded `schema.sql` and a thin Rust model API. Tables:

- `sparse_cache_config`: key/value rows for `schema_version` and `chunk_size`.
- `sparse_cache_views`: one row per immutable or ref-versioned view, keyed by `repo_id`, `root_tree_id`, optional `commit_id`, optional `ref_name`, and optional `ref_version`. Mutable ref views must include a ref version and root tree identity.
- `sparse_cache_inodes`: per-view inode metadata with inode id, node kind, optional durable object id/kind, mode, uid/gid, nlink, size, block size, blocks, timestamp seconds/nanos, optional MIME type, JSON custom attrs, and lookup count.
- `sparse_cache_dentries`: per-view directory entries mapping `(parent_inode_id, name)` to `child_inode_id`, with a normalized absolute path and a unique `(view_id, path)`.
- `sparse_cache_chunks`: immutable file data chunks keyed by `(repo_id, object_id, chunk_index)`, with offset, byte length, and bytes. Do not key committed content chunks only by path or mutable inode.
- `sparse_cache_symlinks`: symlink target rows keyed by `(view_id, inode_id)`, with optional target object id when the target came from a durable symlink blob.
- `sparse_cache_statfs`: per-view derived counters for inode count, file count, directory count, symlink count, bytes used, blocks used, and block size.

Use `PRAGMA foreign_keys = ON`, `PRAGMA busy_timeout = 5000`, `PRAGMA synchronous = NORMAL`, and request WAL journal mode for file-backed caches. Tests may use in-memory SQLite without requiring WAL to persist.

## Safety And Redaction

- All cache operation errors that could include SQLite text, paths, local DB paths, SQL, or backend internals must map to fixed redacted `VfsError` messages.
- Tests must not require Postgres, R2, durable-cloud env, or network.
- Do not add route/status/log output that exposes DB URLs, R2 endpoints, object keys, raw backend errors, commit messages, request bodies, idempotency keys, lease tokens, SQL, migration SQL, advisory lock ids, or secrets.
- The cache stores object ids and repo ids as normal internal identity fields, but no public surface is added in this slice.

## Rollback Boundary

Rollback is deleting the `sparse_cache` module and removing the `rusqlite` dependency. Because the module is not wired into mount, HTTP, durable-cloud, local persistence, idempotency, audit, recovery, or object cleanup flows, rollback does not require data migration and does not change existing runtime behavior.

## Non-Goals

- Do not implement a hydration scheduler.
- Do not change `stratum-mount`; it remains snapshot-only.
- Do not enable durable-cloud FUSE/MCP/REPL or sparse mount serving.
- Do not route committed reads through SQLite.
- Do not add NFS/macOS fallback, mount daemon UX, read-through IO, write-back, flush, or commit staging.
- Do not change durable-cloud defaults, HTTP route behavior, recovery/idempotency/audit semantics, or object cleanup semantics.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-21-sparse-vfs-cache-schema.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-21-sparse-vfs-cache-schema.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-21-sparse-vfs-cache-schema.md
git commit -m "docs: plan sparse vfs cache schema"
```

## Task 2: Add SQLite Dependency And Empty Schema Module

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/lib.rs`
- Create: `src/sparse_cache/mod.rs`
- Create: `src/sparse_cache/schema.sql`

**Step 1: Write failing schema creation/version tests**

Add tests in `src/sparse_cache/mod.rs`:

- `creates_schema_and_records_version`
- `reopens_existing_cache_without_recreating_identity_rows`

Expected RED:

```bash
cargo test --locked sparse_cache::tests::creates_schema_and_records_version --lib -- --exact --nocapture
```

It should fail before the module/dependency exists.

**Step 2: Implement minimal schema open**

Add `rusqlite = { version = "0.32", features = ["bundled"] }`.

Implement:

- `SparseCache::open(path: &Path) -> Result<Self, VfsError>`
- `SparseCache::open_in_memory() -> Result<Self, VfsError>`
- `SparseCache::schema_version(&self) -> Result<u32, VfsError>`
- `SparseCache::chunk_size(&self) -> Result<u32, VfsError>`

Keep SQL errors redacted.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked sparse_cache::tests::creates_schema_and_records_version --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::reopens_existing_cache_without_recreating_identity_rows --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/sparse_cache
git commit -m "feat: add sparse cache sqlite schema"
```

## Task 3: Add Identity, Path, Metadata, And Entry Model Tests

**Files:**

- Modify: `src/sparse_cache/mod.rs`
- Modify: `src/sparse_cache/schema.sql`

**Step 1: Write failing model tests**

Add tests:

- `view_identity_includes_repo_root_commit_and_ref_version`
- `normalizes_absolute_paths_and_rejects_root_escape`
- `metadata_round_trips_for_file_directory_and_symlink`
- `tree_entries_can_reference_tree_blob_and_symlink_objects`
- `hardlinks_are_multiple_dentries_to_one_inode_with_nlink`

Expected RED:

```bash
cargo test --locked sparse_cache::tests::view_identity_includes_repo_root_commit_and_ref_version --lib -- --exact --nocapture
```

**Step 2: Implement model methods**

Implement model structs and DB helpers:

- `CacheViewIdentity`
- `CachedInode`
- `CachedNodeKind`
- `CachedDentry`
- `normalize_cache_path`
- `SparseCache::insert_view`
- `SparseCache::get_view_identity`
- `SparseCache::put_inode`
- `SparseCache::get_inode`
- `SparseCache::put_dentry`
- `SparseCache::list_dentries`

Do not add remote/object-store clients. Do not infer visibility from path-only rows.

**Step 3: Verify and commit**

Run:

```bash
cargo test --locked sparse_cache::tests::view_identity_includes_repo_root_commit_and_ref_version --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::normalizes_absolute_paths_and_rejects_root_escape --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::metadata_round_trips_for_file_directory_and_symlink --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::tree_entries_can_reference_tree_blob_and_symlink_objects --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::hardlinks_are_multiple_dentries_to_one_inode_with_nlink --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/sparse_cache
git commit -m "feat: model sparse cache identities and inodes"
```

## Task 4: Add Chunk, Symlink, Statfs, And Forget Tests

**Files:**

- Modify: `src/sparse_cache/mod.rs`
- Modify: `src/sparse_cache/schema.sql`

**Step 1: Write failing representation tests**

Add tests:

- `chunks_are_keyed_by_repo_object_and_chunk_index`
- `symlink_targets_round_trip_without_hydrating_target`
- `statfs_counters_round_trip_for_a_view`
- `forget_decrements_lookup_count_without_touching_durable_identity`
- `forgotten_unlinked_inode_can_be_pruned_after_lookup_count_reaches_zero`

Expected RED:

```bash
cargo test --locked sparse_cache::tests::chunks_are_keyed_by_repo_object_and_chunk_index --lib -- --exact --nocapture
```

**Step 2: Implement representation helpers**

Implement:

- `CachedChunk`
- `CachedSymlink`
- `CachedStatfs`
- `SparseCache::put_chunk`
- `SparseCache::get_chunk`
- `SparseCache::put_symlink`
- `SparseCache::get_symlink`
- `SparseCache::put_statfs`
- `SparseCache::get_statfs`
- `SparseCache::record_lookup`
- `SparseCache::forget`
- `SparseCache::prune_forgotten_unlinked`

Forget semantics are local cache reference-count semantics only. They must not delete durable objects, change refs, enqueue cleanup, or call remote providers.

**Step 3: Verify and commit**

Run:

```bash
cargo test --locked sparse_cache::tests::chunks_are_keyed_by_repo_object_and_chunk_index --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::symlink_targets_round_trip_without_hydrating_target --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::statfs_counters_round_trip_for_a_view --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::forget_decrements_lookup_count_without_touching_durable_identity --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::forgotten_unlinked_inode_can_be_pruned_after_lookup_count_reaches_zero --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/sparse_cache
git commit -m "feat: model sparse cache chunks and lookup refs"
```

## Task 5: Document Slice 10 Status

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Step 1: Update docs**

In `docs/project-status.md`, add a Slice 10 section stating:

- the completed scope is a provider-free SQLite schema/model foundation;
- cache views are keyed by repo/root tree/commit/ref version/object/path/inode identities;
- tree entries, inode metadata, chunks, symlinks, statfs counters, hardlinks, and forget semantics are represented;
- no hydration scheduler, sparse FUSE, route/runtime cutover, local `.vfs/state.bin` replacement, or durable-cloud mount serving was added.

In `docs/http-api-guide.md`, add a compact note under Backend Durability Status:

- the sparse cache schema is not an HTTP API behavior change;
- durable-cloud unsupported route groups and non-server fail-closed behavior remain stable;
- `stratum-mount` remains snapshot-only.

**Step 2: Verify and commit**

Run:

```bash
rg -n "Sparse VFS|sparse cache|snapshot-only|hydration" docs/project-status.md docs/http-api-guide.md
git diff --check
```

Commit:

```bash
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: record sparse cache schema foundation"
```

## Task 6: Reviews And Full Verification

**Files:**

- No planned source changes beyond review fixes.

**Step 1: Run focused slice tests**

Run:

```bash
cargo test --locked sparse_cache --lib -- --nocapture
```

**Step 2: Run required gates**

Run the full Slice 10 verification list from the handoff:

```bash
cargo fmt --all -- --check
git diff --check
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

**Step 3: Commit review fixes if needed**

Keep fixes scoped and rerun any failed or affected gates before merging.
