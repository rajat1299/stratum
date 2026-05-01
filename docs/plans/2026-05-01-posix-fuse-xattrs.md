# POSIX/FUSE Xattrs Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expose Stratum MIME metadata and custom attrs through POSIX/FUSE extended attributes.

**Architecture:** Keep xattr semantics in `PosixFs`, backed by existing `VirtualFs::stat` and `VirtualFs::set_metadata`. `fuse_mount.rs` should only translate fuser callback arguments, buffer sizing, and errno responses.

**Tech Stack:** Rust 2024, `VirtualFs`, `PosixFs`, optional `fuser` 0.17.0, existing metadata validators and tests.

---

## Design

Use stable xattr names:

- `user.stratum.mime_type` maps to `StatInfo.mime_type`.
- `user.stratum.custom.<key>` maps to `StatInfo.custom_attrs[key]`.

Values are UTF-8 strings because Stratum custom attrs and MIME metadata are strings. Unsupported xattr names are not backed by this slice. The POSIX API should enforce read permission for `getxattr`/`listxattr` and write permission for `setxattr`/`removexattr`.

FUSE callbacks should use the pinned `fuser` 0.17.0 conventions:

- `getxattr` and `listxattr`: `size == 0` returns `ReplyXattr::size`, nonzero size returns `ReplyXattr::data`, and too-small buffers return `ERANGE`.
- `listxattr` returns NUL-terminated name bytes.
- `setxattr` honors `XATTR_CREATE = 1` and `XATTR_REPLACE = 2`; both flags or unknown bits are invalid.
- Missing xattrs map to `Errno::NO_XATTR`.
- Unsupported nonzero `position` maps to `ENOTSUP`.

Symlink behavior for this slice follows inode semantics through the path FUSE gives us, matching current `getattr`/`readlink` behavior rather than HTTP `set_metadata_as` final-target behavior.

## Task 1: Plan Commit

**Files:**
- Create: `docs/plans/2026-05-01-posix-fuse-xattrs.md`

**Step 1: Commit this plan**

Run:

```bash
git add docs/plans/2026-05-01-posix-fuse-xattrs.md
git commit -m "docs: plan posix fuse xattrs"
```

Expected: a docs-only commit.

## Task 2: Core POSIX Xattr API

**Files:**
- Modify: `src/posix.rs`
- Test: `tests/integration/posix.rs`

**Step 1: Write failing POSIX tests**

Add tests for:

- `setxattr`/`getxattr` round-tripping `user.stratum.mime_type`.
- `setxattr`/`getxattr` round-tripping `user.stratum.custom.owner`.
- `listxattr` returning only currently present Stratum xattrs.
- `removexattr` clearing MIME and custom attrs.
- create-only set returning an existing-error when the attr exists.
- replace-only set returning a missing-attr error when the attr is absent.
- read permission required for get/list and write permission required for set/remove.

Run:

```bash
cargo test --locked --test integration posix_xattr -- --nocapture
```

Expected: fail because the POSIX xattr API does not exist yet.

**Step 2: Implement minimal POSIX xattr API**

In `src/posix.rs`, add:

- `pub const STRATUM_MIME_XATTR: &str = "user.stratum.mime_type";`
- `pub const STRATUM_CUSTOM_XATTR_PREFIX: &str = "user.stratum.custom.";`
- `pub enum PosixXattrSetMode { Upsert, CreateOnly, ReplaceOnly }`
- `pub fn listxattr(&self, path: &str) -> Result<Vec<String>, VfsError>`
- `pub fn getxattr(&self, path: &str, name: &str) -> Result<Vec<u8>, VfsError>`
- `pub fn setxattr(&mut self, path: &str, name: &str, value: &[u8], mode: PosixXattrSetMode) -> Result<(), VfsError>`
- `pub fn removexattr(&mut self, path: &str, name: &str) -> Result<(), VfsError>`

Reuse `MetadataUpdate` and `VirtualFs::set_metadata`. Return `VfsError::NotFound { path: name.to_string() }` for missing backed xattrs so FUSE can map it to `NO_XATTR`.

**Step 3: Verify POSIX tests pass**

Run:

```bash
cargo test --locked --test integration posix_xattr -- --nocapture
```

Expected: pass.

**Step 4: Commit**

```bash
git add src/posix.rs tests/integration/posix.rs
git commit -m "feat: add posix metadata xattrs"
```

## Task 3: FUSE Xattr Callbacks

**Files:**
- Modify: `src/fuse_mount.rs`
- Test: `src/fuse_mount.rs`

**Step 1: Write focused helper tests**

Add unit tests under `#[cfg(all(test, feature = "fuser"))]` for helper behavior that does not require mounting:

- list payload encodes names as `name\0name2\0`.
- `size == 0` path is represented by a size response helper, and too-small buffers detect `ERANGE`.
- FUSE flags convert to `PosixXattrSetMode`.
- unsupported flag combinations fail.

Run:

```bash
cargo test --locked --features fuser fuse_mount::tests::xattr -- --nocapture
```

Expected: fail because helpers/callback support do not exist yet.

**Step 2: Implement callbacks**

In `src/fuse_mount.rs`:

- Import `ReplyXattr`.
- Implement `Filesystem::getxattr`, `listxattr`, `setxattr`, and `removexattr`.
- Resolve `ino` to a path with `path_for_inode`.
- Convert `OsStr` xattr names to UTF-8 or return `EINVAL`.
- Convert xattr values to UTF-8 inside `PosixFs`; map invalid values to `EINVAL`.
- Use `reply.size`, `reply.data`, `reply.ok`, and `reply.error` according to fuser 0.17.0 conventions.
- Map missing xattrs to `Errno::NO_XATTR`, not `ENOENT`.

**Step 3: Verify FUSE tests and compile**

Run:

```bash
cargo test --locked --features fuser fuse_mount::tests::xattr -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
```

Expected: pass.

**Step 4: Commit**

```bash
git add src/fuse_mount.rs
git commit -m "feat: expose metadata xattrs over fuse"
```

## Task 4: Docs And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md` only if naming cross-reference is useful.

**Step 1: Update status**

Update the file metadata/POSIX sections to say FUSE xattr mapping exists for:

- `user.stratum.mime_type`
- `user.stratum.custom.<key>`

Keep residual risks explicit:

- no arbitrary binary xattrs
- no native platform xattr persistence beyond Stratum metadata
- no remote sparse FUSE cache correctness guarantees

**Step 2: Verify docs diff**

Run:

```bash
git diff --check
```

Expected: pass.

**Step 3: Commit**

```bash
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: update posix xattr status"
```

## Task 5: Review And Full Verification

**Files:**
- Review all changed files.

**Step 1: Request reviews**

Dispatch:

- Spec/security/API reviewer: confirm xattr names, permissions, error behavior, and non-secret persistence are correct.
- Code-quality/correctness reviewer: inspect Rust API shape, fuser callback behavior, tests, and edge cases.

**Step 2: Fix findings**

Commit review fixes separately if any material fixes are needed:

```bash
git commit -m "fix: address posix xattr review findings"
```

**Step 3: Full verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Expected: all pass.

**Step 4: Merge and push**

After verification:

```bash
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git merge --no-ff v2/foundation
git push origin main
```

Expected: both `v2/foundation` and `main` are pushed with clean worktrees.
