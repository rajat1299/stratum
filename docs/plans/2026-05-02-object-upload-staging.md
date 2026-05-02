# Object Upload Staging Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative object-upload correctness foundation for durable backends: conditional final object writes, staged upload keys, and orphan cleanup helpers while preserving the current local runtime behavior.

**Architecture:** Extend the byte-store boundary with conditional create, delete, and prefix listing operations. Update `BlobObjectStore` so object bytes are staged first, final immutable keys are created with if-absent semantics, metadata insertion remains idempotent, and staged objects can be cleaned up independently. Keep `stratum-server`, HTTP, MCP, CLI, FUSE, and runtime backend selection unwired.

**Tech Stack:** Rust 2024, Tokio async tests, async-trait, AWS SDK for Rust S3 client, Cloudflare R2 S3-compatible API, existing backend object/metadata contracts.

---

## Product Decision

This slice turns the existing byte-backed object adapter from a happy-path scaffold into a safer production-shaped contract. It does not attempt full cloud runtime semantics.

Official docs checked:

- AWS S3 conditional writes document `If-None-Match: *` for create-if-absent writes, `412 PreconditionFailed` for existing keys, and `409 ConditionalRequestConflict` for concurrent write contention.
- AWS SDK for Rust generated S3 docs expose `PutObjectFluentBuilder::if_none_match`, `DeleteObject`, and `ListObjectsV2`.
- Cloudflare R2’s S3-compatible API supports the same object API shape needed by this slice.

SMFS reference checked:

- SMFS has useful retry/claim/finalize patterns in its SQLite push queue, especially explicit operation state, attempts, and `last_error`.
- Do not copy SMFS document sync semantics, latest-wins coalescing, filepath restrictions, or dirty-file policy. Stratum object keys are content-addressed, immutable, repo scoped, and metadata/ref aware.

In scope:

- conditional byte writes on `RemoteBlobStore`;
- local and R2 implementations of conditional create, delete, and prefix listing;
- staged object-upload keys under a repo-scoped staging prefix;
- `BlobObjectStore::put` promotion from staging to final immutable key;
- reconciliation when the final content-addressed key already contains the same bytes;
- corruption detection when the final key exists with different bytes;
- cleanup helper for old staged upload keys;
- dry-run detection for old final objects that are missing metadata;
- delete mode that fails closed until a durable cleanup claim/lease exists;
- focused unit tests and status/docs updates.

Out of scope:

- server runtime cutover to Postgres/R2;
- distributed transactions across object storage and metadata;
- multipart upload, signed URLs, direct browser uploads, lifecycle policies, or bucket retention policy management;
- background cleanup worker;
- SMFS sparse cache, push queue, NFS mount, SDK, or document API extraction.

## Task 1: Conditional Remote Blob Contract

**Files:**
- Modify: `src/remote/blob.rs`
- Modify: `src/error.rs`

**Requirements:**
- Add `BlobPutCondition::{None, IfAbsent}`.
- Add `BlobPutOutcome::{Written, AlreadyExists}`.
- Add `RemoteBlobListing { key, size, modified_at }`.
- Extend `RemoteBlobStore` with:
  - `put_bytes_with_condition(key, data, condition) -> BlobPutOutcome`;
  - `delete_bytes(key)`;
  - `list_keys(prefix) -> Vec<RemoteBlobListing>`.
- Keep existing `put_bytes` as a compatibility method using unconditional writes.
- Add a retryable/write-contention error variant for conditional-write conflict.
- Local implementation:
  - `IfAbsent` creates without overwriting;
  - existing key returns `AlreadyExists`;
  - delete ignores missing keys;
  - list walks nested local files and returns store-relative slash keys.
- R2 implementation:
  - use `if_none_match("*")` for `IfAbsent`;
  - map `PreconditionFailed` to `AlreadyExists`;
  - map `ConditionalRequestConflict` to the write-contention error;
  - use `DeleteObject` and paginated `ListObjectsV2`.

**Verification:**

```bash
cargo test --locked remote::blob -- --nocapture
```

## Task 2: Staged Object Put Semantics

**Files:**
- Modify: `src/backend/blob_object.rs`

**Requirements:**
- Add repo-scoped helpers:
  - `object_staging_prefix(repo_id)`;
  - `object_staging_key(repo_id, kind, id, upload_id)`.
- Update `BlobObjectStore::put`:
  - validate `id == sha256(bytes)`;
  - preserve existing metadata-first idempotent path;
  - write a unique staging key with `IfAbsent`;
  - write the final object key with `IfAbsent`;
  - when final already exists, read and validate that bytes/hash/size match;
  - insert metadata after final object convergence;
  - delete the staging key after success or after metadata failure;
  - never delete the final content-addressed object inline after metadata failure.
- Keep `contains` surfacing corruption/errors instead of collapsing them to `false`.

**Verification:**

```bash
cargo test --locked backend::blob_object -- --nocapture
```

## Task 3: Orphan Cleanup Foundation

**Files:**
- Modify: `src/backend/blob_object.rs`

**Requirements:**
- Add `ObjectOrphanCleanupMode::{StagedUploadsOnly, FinalObjectsMissingMetadataDryRun, FinalObjectsMissingMetadataDelete}`.
- Add `ObjectOrphanCleanupReport` with explicit counts and per-key cleanup errors.
- Add `BlobObjectStore::cleanup_orphans(repo_id, older_than, mode)`.
- Staged cleanup deletes only staged keys older than the cutoff.
- Final-object dry run reports old final object keys without metadata.
- Final-object delete mode returns `NotSupported` in this slice because unconditional deletion can race metadata recovery and corrupt live objects.
- Final cleanup must require the age cutoff and caller behavior should be dry-run detection only until a durable cleanup claim exists.

**Verification:**

```bash
cargo test --locked backend::blob_object -- --nocapture
```

## Task 4: Docs And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Re-open `docs/project-status.md` immediately before editing because the SDK/DX lane may update it separately.
- Document that conditional object writes, staging keys, and cleanup helpers exist at the backend adapter layer.
- Document that no HTTP/MCP/CLI/FUSE runtime cutover exists yet.
- Preserve SDK-lane status content when merging back to `main`.

## Task 5: Review And Verification

Dispatch independent reviewers after implementation:

- Rust/API reviewer: trait shape, async error handling, R2 error mapping, and compatibility of existing callers.
- Storage correctness reviewer: conditional write semantics, cleanup race behavior, and no unsafe final-object deletion.

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
