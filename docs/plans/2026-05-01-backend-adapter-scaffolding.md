# Backend Adapter Scaffolding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the first concrete adapter scaffold behind the durable backend contracts while preserving current local HTTP/server behavior.

**Architecture:** Bridge the existing remote byte-store abstraction into the new `ObjectStore` contract with typed, repo-scoped object keys and a metadata boundary that models the future Postgres `objects` table. Tighten the Postgres migration contract where the prior slice left known mismatches, but do not add a live Postgres client or route runtime traffic through the new adapter.

**Tech Stack:** Rust 2024, async-trait, Tokio tests, existing `RemoteBlobStore` local/R2 byte-store abstraction, existing backend contracts, SQL migration planning file.

---

## Product Decision

This slice is adapter scaffolding, not the production cloud cutover.

The next useful step is to prove how content-addressed object bytes will sit behind `ObjectStore` without pretending that Postgres/R2 is fully operational. The existing server continues to use `.vfs/state.bin` plus local metadata stores. No HTTP, MCP, CLI, FUSE, or `StratumDb` behavior should change.

In scope:

- typed, repo-scoped object keys for durable byte storage;
- an `ObjectStore` adapter over the existing `RemoteBlobStore` trait;
- an in-memory object metadata adapter that models the future Postgres `objects` table;
- local byte-store tests for nested durable object keys;
- migration contract hardening for commit timestamps, ref version bounds, repo ID validation, object hash semantics, audit global sequence uniqueness, and approval uniqueness;
- docs/status updates that clearly distinguish scaffolded adapters from live production backend runtime.

Out of scope:

- a live Postgres dependency, connection pool, migration runner, or CI database service;
- live R2 integration tests;
- HTTP/server cutover to the new backend traits;
- Redis/distributed locks;
- cross-store transactions;
- object upload staging, orphan cleanup workers, or signed URL support;
- normalizing the full POSIX inode/path table.

## Task 1: Remote Object Adapter Contract

**Files:**
- Create: `src/backend/blob_object.rs`
- Modify: `src/backend/mod.rs`

**Requirements:**
- Expose a `backend::blob_object` module.
- Add an object-key helper that derives namespaced immutable byte keys:
  - `repos/{repo_id}/objects/{kind}/{sha256}`
- Do not reuse the legacy unscoped `blobs/{hash}` helper for backend objects.
- Define an object metadata boundary that records repo ID, object ID, kind, object key, size, and SHA-256 hex.
- Implement an in-memory metadata store for tests and local scaffold composition.
- Implement `BlobObjectStore` or equivalent over `RemoteBlobStore` plus the metadata boundary.
- Keep production `StratumStores::local_memory()` unchanged unless a separate constructor for the byte-backed adapter is useful and test-only/local-only.

**Behavior:**
- `put` recomputes `ObjectId::from_bytes(&write.bytes)` and rejects ID mismatches.
- Same repo/object/kind/bytes writes are idempotent.
- Same repo/object with a different kind or different bytes is corruption.
- Missing metadata means `get` returns `Ok(None)`.
- Metadata kind mismatch means `CorruptStore`.
- Metadata exists but bytes are missing or hash-mismatched means `CorruptStore`.
- `contains` must not silently hide corrupted metadata/byte state.

## Task 2: Remote Byte Store Tests

**Files:**
- Modify: `src/remote/blob.rs`

**Requirements:**
- Add focused tests proving `LocalBlobStore` round-trips nested namespaced keys.
- Keep `RemoteBlobStore::get_bytes` semantics unchanged: byte-store errors are errors, not object-not-found decisions. The object adapter determines missing object state from metadata absence.

## Task 3: Migration Contract Hardening

**Files:**
- Modify: `migrations/postgres/0001_durable_backend_foundation.sql`

**Requirements:**
- Add SQL contract checks for `repos.id` matching the Rust `RepoId` envelope.
- Clarify that `objects.object_id` is the content SHA-256 hex and make `sha256` redundant but constrained to match for now.
- Add explicit commit timestamp storage matching `CommitRecord.timestamp`.
- Bound `refs.version` to the signed `BIGINT` range that Rust adapters must respect for Postgres-backed deployments.
- Fix audit sequence uniqueness for global events where `repo_id IS NULL`.
- Add an approval uniqueness contract for active approvals per change request, head commit, and approver.
- Document that adapters must explicitly update `updated_at` columns until triggers are introduced.
- Do not add a SQL client dependency in this slice.

## Task 4: Adapter Tests

**Files:**
- Modify: `src/backend/blob_object.rs`

**Requirements:**
- Add tests for:
  - idempotent byte-backed object put/get;
  - object ID mismatch rejection;
  - same object ID with wrong expected kind returning corruption;
  - same object ID with different stored kind rejected on put;
  - missing metadata returning `Ok(None)`;
  - missing remote bytes after metadata exists returning corruption;
  - remote byte hash mismatch returning corruption;
  - namespaced object-key formatting.

**Focused verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::blob_object -- --nocapture
cargo test --locked remote::blob -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

## Task 5: Docs And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Record the backend adapter scaffolding as the latest completed slice.
- State that live Postgres and live S3/R2 runtime cutover remain unbuilt.
- Explain that the new byte-backed object adapter is not yet wired into HTTP/server runtime.
- Keep recommended next slices current and conservative.

## Review And Full Verification

Dispatch separate reviewers after implementation:

- correctness/API reviewer focused on object contract shape, namespacing, idempotency, and HTTP non-impact;
- security/reliability reviewer focused on corruption handling, byte-store error semantics, migration constraints, and production cutover risks.

Run the full gate before merging:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
