# FUSE Adapter And macOS Fallback Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative mount-adapter foundation for future sparse Stratum mounts, with Linux FUSE and macOS NFS-over-localhost operation mapping covered in provider-free tests while preserving the existing local snapshot FUSE mount.

**Architecture:** Introduce a protocol-neutral, inode-oriented mount adapter layer in the `stratum` runtime crate. Add a read-only sparse-cache adapter over existing SQLite cache/hydration rows, model unknown file size explicitly at the adapter boundary, and keep any numeric sentinel confined to FUSE/NFS wire mapping helpers. Do not wire sparse cache into `stratum-mount`, HTTP, durable-cloud runtime, MCP, REPL, or `.vfs/state.bin`; `stratum-mount` continues mounting `db.snapshot_fs()` and direct durable-cloud non-server surfaces remain fail-closed.

**Tech Stack:** Rust 2024, Cargo workspace, existing `rusqlite` sparse cache, existing `fuser` optional feature for current snapshot FUSE compile checks, provider-free unit tests, no privileged mounts, no Postgres/R2/network requirements, and no macFUSE/kext/notarization work.

---

## Required Skills And Agent Discipline

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare against this plan, Slice 12 acceptance criteria, `docs/project-status.md`, `docs/http-api-guide.md`, the Slice 10 sparse-cache plan, and the Slice 11 hydration plan.

The main session owns integration, local review, final verification, commits, merge to `main`, and pushes. Subagents may implement or review scoped work, but the main session must inspect diffs and rerun gates before accepting work.

## Required Context Read

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-21-sparse-vfs-cache-schema.md`
- `docs/plans/2026-05-21-hydration-scheduler.md`
- `src/sparse_cache/mod.rs`
- `src/sparse_cache/schema.sql`
- `src/sparse_cache/hydration.rs`
- `src/fuse_mount.rs`
- `src/bin/stratum_mount.rs`
- `src/backend/committed_read.rs`
- `src/backend/runtime.rs`
- `src/fs/inode.rs`
- `src/posix.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/vfs/traits.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/vfs/types.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/mount/fuse.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/mount/nfs.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/daemon/`
- `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/node/src/fuse/fs.ts`
- `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/node/src/fuse/fs.test.ts`
- `/Users/rajattiwari/virtualfilesystem/mirage/docs/home/design/fuse.mdx`

## Current Inventory

Current Stratum mount behavior:

- `stratum-mount` is gated by the optional `fuser` feature and calls `ensure_local_state_runtime_for_non_server_surface(NonServerRuntimeSurface::StratumMount)` before opening local state.
- It opens `StratumDb`, clones the current filesystem with `db.snapshot_fs()`, wraps that `VirtualFs` in `Arc<Mutex<_>>`, and calls `fuse_mount::mount`.
- The current FUSE implementation is snapshot/path-centric. It resolves inode numbers back to paths by walking `VirtualFs`, then delegates callbacks to `PosixFs`.
- The existing FUSE callback surface includes lookup, getattr, readlink, mkdir, create, open, read, write, unlink, rmdir, rename, symlink, hardlink, setattr, xattrs, and readdir. File handles are not tracked beyond returning `FileHandle(0)`.
- Existing read-only behavior relies on kernel mount options; the mutation callbacks still exist for snapshot mode.

Sparse-cache and hydration state:

- `SparseCache` schema version 2 models durable identity-scoped views, cached inodes, dentries, chunks, symlinks, statfs rows, hydration jobs, and bounded job progress.
- Hydration verifies commit/root-tree/ref-version identity and transactionally writes tree dentries, inode metadata, symlink targets, statfs counters, and chunk rows through provider-free `StratumStores` tests.
- Cache inode metadata currently stores `size: u64` only. Slice 12 needs an explicit `size_known` bit so `ls -l` / getattr can avoid remote blob reads without persisting a fake sentinel size as domain truth.
- Hydration is not wired into committed reads, HTTP, local state, MCP, REPL, or `stratum-mount`.

Durable read and runtime boundaries:

- Durable committed reads already resolve visible refs/session refs and read tree/blob objects through durable `RefStore`, `CommitStore`, and `ObjectStore`.
- Direct non-server surfaces currently reject durable-cloud through `NonServerRuntimeSurface`; `stratum-mount` uses this guard.
- Slice 12 must not enable durable-cloud FUSE, direct durable-cloud mount access, or local-state fallback under durable-cloud.

smfs lessons to adapt:

- Use an inode-oriented adapter seam: `lookup`, `getattr`, `readdir`, `readdir_plus`, `read`, `readlink`, and `statfs` are the core read operations.
- Keep FUSE/NFS protocol details in adapter/wire layers; the core mount adapter should not know about kernel reply objects.
- Prefer `readdir_plus` for callback/unit coverage so `ls -l` behavior is represented without N+1 cache or remote blob reads.
- Model Linux FUSE and macOS NFS-over-localhost as backend choices; real NFS daemon lifecycle and IPC are later mount-daemon UX work.
- Keep real kernel mount smoke tests ignored/manual if they are added later. Normal CI must stay provider-free and privilege-free.

Mirage size-unknown lessons:

- The domain model should use nullable/unknown size rather than a magic public size.
- `getattr`, `stat`, `ls`, and `ls -l` must not fetch blob bytes just to learn size.
- If a protocol requires a numeric file size, use a bounded sentinel only in the narrow FUSE/NFS wire mapping layer and never persist it to sparse cache or report it through protocol-neutral adapter APIs.
- `cat`/read may fetch or hydrate bytes. After reading a terminal chunk, cache the real size and let later getattr/stat return the real size.
- Read past actual EOF must return empty bytes, not an error.

## Proposed Model

Add `src/mount_adapter.rs` as a protocol-neutral mount surface:

- `MountBackend { Fuse, Nfs }` with default backend `Fuse` on Linux and `Nfs` elsewhere.
- `MountFileKind { File, Directory, Symlink }`.
- `MountFileSize { Known(u64), Unknown }`.
- `MountAttr` containing inode id, kind, size, mode, uid/gid, nlink, block size, blocks, timestamps, MIME/custom-attr metadata counts where useful, and a `size_known()` helper.
- `MountDirEntry` containing name plus `MountAttr`.
- `MountStatfs` containing inode/file/directory/symlink counts, bytes used, blocks used, and block size.
- `MountErrorCode` and `MountError` with fixed redacted public messages. Do not include paths, repo ids, object ids, SQL, backend errors, or symlink targets in `Display`/`Debug`.
- `MountReadAdapter` trait with provider-free synchronous methods:
  - `lookup(parent_ino, name) -> Result<Option<MountAttr>, MountError>`
  - `getattr(ino) -> Result<Option<MountAttr>, MountError>`
  - `readdir(ino) -> Result<Option<Vec<String>>, MountError>`
  - `readdir_plus(ino) -> Result<Option<Vec<MountDirEntry>>, MountError>`
  - `read(ino, offset, size) -> Result<Vec<u8>, MountError>`
  - `readlink(ino) -> Result<Option<String>, MountError>`
  - `statfs() -> Result<MountStatfs, MountError>`
- `UnknownSizePolicy { Zero, Sentinel(u64) }`.
- Pure FUSE/NFS wire mapping helpers that convert `MountAttr`, `MountError`, and directory/read results into small testable structs/enums. These helpers are not real kernel adapters and require no mount privileges.

Add `src/sparse_cache/mount.rs` as a read-only adapter over one cache view:

- `SparseCacheMount<'a, S>` borrows `SparseCache`, a `view_id`, and an optional local `SparseMountBlobSource`.
- `SparseMountBlobSource` is provider-free and test-only-friendly:
  - `load_chunk(repo_id, object_id, chunk_index, chunk_size) -> Result<Option<Vec<u8>>, VfsError>`
  - Returning `None` means cache miss remains unresolved; returning `Some(bytes)` stores a chunk and, when `bytes.len() < chunk_size`, marks the inode size known to `chunk_index * chunk_size + bytes.len()`.
- `SparseCacheMount` implements only read operations. Mutations are out of scope and should map to `MountErrorCode::NotSupported` if helper methods exist.
- `getattr`, `lookup`, `readdir`, `readdir_plus`, and `statfs` must read only SQLite metadata and must not call `SparseMountBlobSource`.
- `read` assembles cached chunks and calls `SparseMountBlobSource` only for missing chunks. It returns empty bytes at known EOF or terminal unknown-size EOF.
- Symlink `readlink` reads `sparse_cache_symlinks` only and does not follow the target.

Extend sparse-cache schema/model:

- Bump `SCHEMA_VERSION` from 2 to 3.
- Add `size_known INTEGER NOT NULL DEFAULT 1` to `sparse_cache_inodes`.
- Add `CachedInode.size_known: bool`.
- Migrate schema version 1 and 2 caches by applying `schema.sql`; the new column default makes existing rows known-size.
- Existing tests and helpers should be updated to set `size_known: true` unless a test explicitly models unknown size.

Keep existing runtime behavior:

- `stratum-mount` remains snapshot-only over `db.snapshot_fs()`.
- `src/fuse_mount.rs` continues compiling and serving local snapshot `VirtualFs`.
- No HTTP route, capability manifest, committed-read, durable runtime, MCP, REPL, audit, idempotency, recovery, object cleanup, or local `.vfs/state.bin` behavior changes.
- macOS default path is documented as future sparse mount backend selection using NFS-over-localhost; this slice does not add daemon UX or real `/sbin/mount_nfs` execution.

## Safety And Redaction

- Public errors/status/log-adjacent output must not expose DB URLs, R2 endpoints, object keys, raw backend/provider errors, commit messages, request bodies, idempotency keys, lease tokens, SQL, migration SQL, advisory lock ids, local cache paths, repo ids, object ids, raw paths, symlink targets, chunk bytes, or secrets.
- Tests must not require privileged FUSE/NFS mounts, Postgres, R2, durable-cloud env, network, or live credentials.
- Do not import smfs latest-wins/path-primary semantics.
- Do not persist sentinel sizes. A sentinel may exist only in protocol wire mapping helpers and tests.
- Do not enable write-back, mutation persistence, flush, fsync, commit staging, durable ref updates, or direct durable-cloud mount access.

## Rollback Boundary

Rollback is deleting `src/mount_adapter.rs`, deleting `src/sparse_cache/mount.rs`, reverting the sparse-cache schema/model version bump, and reverting docs/status updates. Because no runtime path is wired to sparse mounting, rollback does not alter committed reads, HTTP behavior, durable-cloud startup, local `.vfs/state.bin`, snapshot `stratum-mount`, MCP, REPL, audit, idempotency, recovery, or object cleanup.

## Non-Goals

- Do not replace `stratum-mount` with sparse mount behavior.
- Do not add mount daemon UX, PID/socket/log/status/unmount controls.
- Do not add real NFS server lifecycle or invoke `/sbin/mount_nfs` in normal tests.
- Do not add macFUSE kext/notarization/product packaging.
- Do not enable durable-cloud FUSE/MCP/REPL or remote durable MCP serving.
- Do not route committed reads through SQLite.
- Do not add write-back, mutation persistence, flush/fsync semantics, or commit staging.
- Do not change HTTP route behavior, SDK packages, auth login, workspace management productization, audit/runs durable serving, semantic search, execution runner, Redis, KMS, object cleanup, or crate publishing.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-22-fuse-adapter-macos-fallback.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-22-fuse-adapter-macos-fallback.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-22-fuse-adapter-macos-fallback.md
git commit -m "docs: plan fuse adapter macos fallback"
```

## Task 2: Add Protocol-Neutral Mount Adapter Types

**Files:**

- Modify: `src/lib.rs`
- Create: `src/mount_adapter.rs`

**Step 1: Write failing tests**

Add tests in `src/mount_adapter.rs`:

- `mount_backend_default_matches_platform`
- `unknown_size_stays_domain_unknown_until_wire_mapping`
- `fuse_wire_mapping_uses_configured_unknown_size_policy`
- `nfs_wire_mapping_uses_configured_unknown_size_policy`
- `mount_error_display_and_debug_are_redacted`

Expected RED:

```bash
cargo test --locked mount_adapter::tests::unknown_size_stays_domain_unknown_until_wire_mapping --lib -- --exact --nocapture
```

It should fail before the module exists.

**Step 2: Implement minimal adapter model**

Implement the proposed `MountBackend`, `MountFileKind`, `MountFileSize`, `MountAttr`, `MountDirEntry`, `MountStatfs`, `MountErrorCode`, `MountError`, `MountReadAdapter`, `UnknownSizePolicy`, and pure FUSE/NFS wire helper types.

Keep the wire helper structs independent of `fuser` and `nfsserve` so tests run without optional features or mount privileges.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked mount_adapter::tests --lib -- --nocapture
git diff --check
```

Commit:

```bash
git add src/lib.rs src/mount_adapter.rs
git commit -m "feat: add mount adapter model"
```

## Task 3: Add Sparse-Cache Size-Known Metadata

**Files:**

- Modify: `src/sparse_cache/mod.rs`
- Modify: `src/sparse_cache/schema.sql`

**Step 1: Write failing tests**

Add tests in `src/sparse_cache/mod.rs`:

- `inode_size_known_round_trips_for_known_and_unknown_files`
- `schema_migrates_hydration_cache_to_size_known_schema`
- `unknown_size_inode_debug_does_not_leak_identity_or_paths`

Expected RED:

```bash
cargo test --locked sparse_cache::tests::inode_size_known_round_trips_for_known_and_unknown_files --lib -- --exact --nocapture
```

It should fail before `CachedInode` and schema support `size_known`.

**Step 2: Implement schema/model bump**

Implement:

- `SCHEMA_VERSION = 3`.
- A named previous version constant for schema version 2.
- `size_known INTEGER NOT NULL DEFAULT 1` in `sparse_cache_inodes`.
- `CachedInode { size_known: bool }`.
- SQL read/write updates for `put_inode`, `get_inode`, and helper constructors in tests.
- Version 1 and version 2 migration handling in `initialize_schema`.

Do not change object/chunk identity semantics.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked sparse_cache::tests::inode_size_known_round_trips_for_known_and_unknown_files --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::schema_migrates_hydration_cache_to_size_known_schema --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::unknown_size_inode_debug_does_not_leak_identity_or_paths --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
git diff --check
```

Commit:

```bash
git add src/sparse_cache/mod.rs src/sparse_cache/schema.sql
git commit -m "feat: track sparse cache known file sizes"
```

## Task 4: Add Read-Only Sparse Cache Mount Adapter

**Files:**

- Modify: `src/sparse_cache/mod.rs`
- Create: `src/sparse_cache/mount.rs`
- Modify as needed: `src/mount_adapter.rs`

**Step 1: Write failing adapter tests**

Add tests in `src/sparse_cache/mount.rs`:

- `lookup_getattr_and_readdir_use_cached_metadata`
- `readdir_plus_returns_child_attrs_without_blob_reads`
- `statfs_maps_cached_view_counters`
- `read_assembles_cached_chunks_for_cat_behavior`
- `unknown_size_getattr_does_not_call_blob_source`
- `unknown_size_read_loads_missing_chunk_and_records_real_size_at_eof`
- `read_past_unknown_size_eof_returns_empty`
- `missing_inode_and_invalid_directory_errors_are_redacted`

Expected RED:

```bash
cargo test --locked sparse_cache::mount::tests::unknown_size_getattr_does_not_call_blob_source --lib -- --exact --nocapture
```

It should fail before the mount module exists.

**Step 2: Implement read-only sparse adapter**

Implement:

- `SparseMountBlobSource`.
- `SparseCacheMount`.
- `MountReadAdapter` implementation for lookup, getattr, readdir, readdir_plus, read, readlink, and statfs.
- Metadata-only operations must not call the blob source.
- Missing chunk reads may call the blob source and cache returned chunks with existing sparse-cache chunk helpers.
- If a loaded chunk is shorter than `CHUNK_SIZE`, update the inode to `size_known = true` and `size = chunk_index * CHUNK_SIZE + bytes.len()`.
- Known-size reads must stop at `size`.
- Unknown-size reads must stop when a terminal short chunk or an empty chunk is observed.

Do not add mutation/write-back methods beyond returning `NotSupported` if a helper is necessary for tests.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
git diff --check
```

Commit:

```bash
git add src/sparse_cache/mod.rs src/sparse_cache/mount.rs src/mount_adapter.rs
git commit -m "feat: add sparse cache mount adapter"
```

## Task 5: Cover FUSE And NFS Adapter Mapping

**Files:**

- Modify: `src/mount_adapter.rs`
- Modify if needed: `src/fuse_mount.rs`

**Step 1: Write failing protocol mapping tests**

Add tests in `src/mount_adapter.rs`:

- `fuse_mapping_covers_lookup_getattr_readdir_read_and_statfs`
- `nfs_mapping_covers_lookup_getattr_readdir_read_and_statfs`
- `fuse_and_nfs_mapping_redact_errors`
- `ls_l_unknown_size_mapping_does_not_force_blob_reads`
- `cat_unknown_size_mapping_reads_bytes_and_then_uses_real_size`

If `src/fuse_mount.rs` needs a small helper or assertion to tie current snapshot FUSE attr conversion to the protocol-neutral model, add a focused test without changing snapshot behavior.

Expected RED:

```bash
cargo test --locked mount_adapter::tests::ls_l_unknown_size_mapping_does_not_force_blob_reads --lib -- --exact --nocapture
```

**Step 2: Implement pure mapping helpers**

Implement small adapter-probe helpers that take any `MountReadAdapter` and return FUSE-like and NFS-like response structs:

- lookup maps `Ok(None)` to `ENOENT` / `NFS3ERR_NOENT`.
- getattr maps unknown size through the supplied `UnknownSizePolicy`.
- readdir-plus returns names with attrs and avoids extra blob reads.
- read returns bytes or redacted protocol errors.
- statfs maps cached counters and synthetic free-space values.

Do not add `nfsserve` or real NFS server lifecycle in this slice unless a later review explicitly requires it.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked mount_adapter::tests --lib -- --nocapture
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
git diff --check
```

Commit:

```bash
git add src/mount_adapter.rs src/fuse_mount.rs
git commit -m "test: cover fuse and nfs mount mapping"
```

## Task 6: Preserve Runtime Boundaries And Snapshot Mount

**Files:**

- Modify if needed: `src/backend/runtime.rs`
- Modify if needed: `src/bin/stratum_mount.rs`
- Modify if needed: `src/fuse_mount.rs`

**Step 1: Write failing boundary tests if current coverage is insufficient**

Add or extend tests to prove:

- `STRATUM_CORE_RUNTIME=durable-cloud` still rejects `NonServerRuntimeSurface::StratumMount`.
- `stratum-mount` still opens local state only after the non-server runtime guard.
- Snapshot FUSE compile behavior is unchanged.
- No sparse-cache mount code is wired into `stratum-mount`.

Expected selector:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

**Step 2: Implement only necessary boundary fixes**

Prefer no production change in this task. If tests reveal a gap, keep edits scoped to redacted guard coverage and compile boundaries.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
git diff --check
```

Commit only if files changed:

```bash
git add src/backend/runtime.rs src/bin/stratum_mount.rs src/fuse_mount.rs
git commit -m "test: preserve mount runtime boundaries"
```

## Task 7: Update Docs And Status

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/plans/2026-05-22-fuse-adapter-macos-fallback.md`

**Step 1: Write docs updates**

Update `docs/project-status.md` with a Slice 12 section covering:

- Protocol-neutral mount adapter foundation added.
- Sparse-cache read-only adapter and size-known metadata added.
- Linux FUSE and macOS NFS-over-localhost mapping are tested at callback/helper level without privileged mounts.
- `ls -l` / getattr over unknown-size files does not fetch blob bytes; read/cat hydrates/cache-fills bytes and can record real size at EOF.
- `stratum-mount` remains snapshot-only.
- Durable-cloud FUSE/non-server surfaces remain fail-closed.
- No HTTP route, durable runtime, MCP, REPL, write-back, or mount daemon UX changes.

Update `docs/http-api-guide.md` sparse-cache/mount status with the same boundaries. Do not document any new route behavior.

Append an implementation notes / completion section to this plan after code lands.

**Step 2: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
git diff --check
```

Commit:

```bash
git add docs/project-status.md docs/http-api-guide.md docs/plans/2026-05-22-fuse-adapter-macos-fallback.md
git commit -m "docs: record fuse adapter foundation"
```

## Implementation Notes

Completed on `v2/foundation` as a conservative adapter foundation:

- Added `src/mount_adapter.rs` with protocol-neutral mount attributes, redacted mount errors, explicit unknown-size modeling, dependency-free FUSE/NFS wire helpers, and provider-free operation probes for lookup, getattr/stat, plain readdir, readdir-plus, read/cat, and statfs.
- Added sparse-cache schema version 3 with `CachedInode.size_known`, migration coverage for prior hydration caches, and boolean enforcement for fresh and migrated inode rows.
- Added `src/sparse_cache/mount.rs` with a read-only `SparseCacheMount` over one cache view and an injected `SparseMountBlobSource` used only by `read`.
- Kept `lookup`, `getattr`, `readdir`, `readdir_plus`, `statfs`, and `readlink` metadata-only and blob-source-free.
- Kept unknown file size as domain state. FUSE/NFS numeric zero/sentinel mapping exists only in wire/helper code.
- Hardened reads so unresolved chunk misses, known-size short chunks before EOF, non-blob file inodes, missing symlink rows, and over-requested adapter bytes fail closed with redacted errors rather than silent truncation or payload leakage.
- Preserved existing snapshot `stratum-mount`; no sparse mount runtime, durable-cloud FUSE, HTTP route, MCP, REPL, local `.vfs/state.bin`, mount daemon, NFS lifecycle, macFUSE packaging, or write-back behavior was enabled.

Focused verification during implementation:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked mount_adapter::tests --lib -- --nocapture
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked backend::runtime --lib -- --nocapture
```

## Task 8: Review, Hardening, And Final Verification

**Files:**

- Modify as needed based on review findings.

**Step 1: Spec/correctness review**

Dispatch a review subagent with this plan and the full diff since the plan commit. It must focus on:

- Acceptance criteria coverage.
- Any sparse mount runtime accidentally enabled.
- Snapshot `stratum-mount` behavior preserved.
- Durable-cloud non-server fail-closed behavior preserved.
- `ls -l` / getattr not hydrating unknown-size blobs.
- Sentinel not persisted or exposed in protocol-neutral APIs.
- No write-back or mutation persistence.
- Provider-free/privilege-free tests.
- Redaction.

Fix Important/Critical findings, then rerun focused tests.

**Step 2: Code-quality/security review**

Dispatch a second review subagent with the fixed diff. It must focus on:

- Rust API shape, ownership, borrowing, and unnecessary clones.
- SQLite schema migration safety.
- Error handling and redaction.
- Concurrency hazards around borrowed `SparseCache`.
- Test determinism.
- Feature-gate and cross-platform compile risk.

Fix Important/Critical findings, then rerun focused tests.

**Step 3: Full verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked mount_adapter::tests --lib -- --nocapture
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
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

If provider env is unset, report live Postgres/R2 portions as skipped rather than claiming live coverage.

**Step 4: Final commit if review fixes changed code**

Commit any review hardening separately:

```bash
git add <changed-files>
git commit -m "fix: harden mount adapter foundation"
```

**Step 5: Push and merge**

After all verification gates complete:

```bash
git status --short --branch
git push origin v2/foundation
```

Because the local `main` checkout is intentionally dirty and behind, create a temporary clean main worktree for merge:

```bash
git worktree add /tmp/stratum-main-merge-20260522 origin/main
cd /tmp/stratum-main-merge-20260522
git switch -c main-merge-slice-12
git merge --no-ff origin/v2/foundation -m "merge: fuse adapter macos fallback"
git push origin HEAD:main
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation
git worktree remove /tmp/stratum-main-merge-20260522
```

Do not touch, clean, reset, or overwrite the dirty local main checkout.
