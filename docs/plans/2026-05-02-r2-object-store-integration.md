# R2 Object Store Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an explicit live S3/R2-compatible object-store integration gate for the existing byte-backed object adapter without changing Stratum server runtime behavior.

**Architecture:** Keep `stratum-server` local-backed. Extend the existing `RemoteBlobStore`/`R2BlobStore` test surface with opt-in live integration tests that compose `R2BlobStore` with the backend object adapter and an in-memory metadata store. Add a small script so developers and CI can run the live object-store gate deliberately instead of relying on ambient credentials.

**Tech Stack:** Rust 2024, Tokio tests, AWS SDK for Rust S3 client, Cloudflare R2 S3-compatible API, existing `BlobObjectStore` backend adapter, shell test harness.

---

## Product Decision

This slice proves that the existing object-byte abstraction can talk to a real S3/R2-compatible bucket. It does not wire HTTP, MCP, CLI, FUSE, or `StratumDb` to object storage.

Primary docs checked:

- AWS SDK for Rust S3 examples show the SDK uploading with `put_object`, downloading with `get_object`, and using `ByteStream` for object bodies.
- AWS S3 conditional request docs describe conditional writes such as `If-None-Match` for preventing accidental overwrites.
- Cloudflare R2 S3 compatibility docs list `PutObject`, `GetObject`, `HeadObject`, and conditional operations as supported.
- Cloudflare R2 error docs document S3-compatible `NoSuchKey` errors and `PreconditionFailed` for failed conditional operations.

Local reference checked:

- SMFS has useful future patterns around SQLite cache tables, push queue coalescing, daemon lifecycle, IPC status/sync/unmount, and NFS-on-macOS mounting. Those are mount/runtime cache concerns, not this object-store integration gate. Do not copy SMFS document sync semantics or last-write-wins policy into Stratum object storage.

In scope:

- opt-in R2/S3 live integration test for `R2BlobStore`;
- opt-in composed test proving `BlobObjectStore + R2BlobStore + InMemoryObjectMetadataStore` round-trips raw bytes;
- missing-key behavior check maps remote `NoSuchKey` to `VfsError::ObjectNotFound`;
- no-secret logging and no password/secret echoing in test errors;
- script to run or required-fail the integration gate;
- docs/status updates.

Out of scope:

- server runtime cutover;
- connection pooling or object-store client lifetime changes;
- conditional write API changes to `RemoteBlobStore`;
- object upload staging/cleanup;
- multipart/chunked uploads;
- signed URLs;
- cross-store transaction with Postgres metadata;
- SMFS-style SQLite sparse cache or push queue;
- NFS/FUSE daemon lifecycle changes.

## Task 1: Scripted R2 Gate

**Files:**
- Create: `scripts/check-r2-object-store.sh`

**Requirements:**
- Follow the style of `scripts/check-postgres-migrations.sh`.
- If `STRATUM_R2_TEST_ENABLED=1` and `STRATUM_R2_TEST_REQUIRED=1` are both unset, print a short skip message and exit 0, including in default no-secret CI.
- If required/enabled, require:
  - `STRATUM_R2_BUCKET`
  - `STRATUM_R2_ENDPOINT`
  - `STRATUM_R2_ACCESS_KEY_ID`
  - `STRATUM_R2_SECRET_ACCESS_KEY`
- Do not print secret values.
- Run the focused test command:

```bash
cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
```

**Verification:**

```bash
bash -n scripts/check-r2-object-store.sh
./scripts/check-r2-object-store.sh
STRATUM_R2_TEST_REQUIRED=1 env -u STRATUM_R2_BUCKET ./scripts/check-r2-object-store.sh
```

Expected: shell syntax passes; local unset run skips; required missing env fails before Cargo.

## Task 2: Live R2 Test Harness

**Files:**
- Modify: `src/remote/blob.rs`

**Requirements:**
- Add test-only helper `r2_tests_enabled()`:
  - true when `STRATUM_R2_TEST_ENABLED=1`;
  - true when `STRATUM_R2_TEST_REQUIRED=1`;
  - otherwise skip cleanly.
- Add test-only helper that loads `R2BlobStoreConfig::from_env()`, fails with a clear missing-env message in required/enabled mode, and appends a unique `tests/<uuid>` suffix to `config.prefix`.
- Add test `r2_blob_store_live_integration` that:
  - skips cleanly when not enabled;
  - creates `R2BlobStore`;
  - writes bytes to a unique key under the test prefix;
  - reads them back and asserts exact byte equality;
  - attempts to read a different missing key and asserts `VfsError::ObjectNotFound`;
  - composes `BlobObjectStore::new(Arc::new(store), Arc::new(InMemoryObjectMetadataStore::new()))`;
  - writes raw binary-ish bytes through `ObjectStore::put`;
  - reads them through `ObjectStore::get` and asserts exact bytes/kind/id.
- Avoid cleanup requirements by using unique test prefixes; document that lifecycle cleanup remains future work.

**Verification:**

```bash
env -u STRATUM_R2_TEST_ENABLED -u STRATUM_R2_TEST_REQUIRED cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
```

Expected: one test passes by skipping cleanly.

If credentials are configured:

```bash
STRATUM_R2_TEST_ENABLED=1 cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
```

Expected: test writes/reads via the configured bucket and passes.

## Task 3: CI And Docs

**Files:**
- Modify: `.github/workflows/rust-ci.yml`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Requirements:**
- Add a lightweight `r2-object-store` CI job that runs `./scripts/check-r2-object-store.sh` in default skip mode so the script is syntax- and path-checked in CI without requiring repository secrets.
- Keep live object-store verification explicitly opt-in; do not add fake credentials to CI.
- Update docs/status:
  - live-compatible object-store integration gate exists;
  - default CI only checks skip/script wiring unless credentials are explicitly provided;
  - no runtime cutover, cleanup/staging, multipart/chunking, signed URLs, or cross-store transactions.
- Because the SDK lane may also edit status docs, check `git status` and re-open `docs/project-status.md` immediately before patching.

**Verification:**

```bash
./scripts/check-r2-object-store.sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
git diff --check
```

## Review

Dispatch independent reviewers after implementation:

- Rust/API reviewer: trait semantics, test gating, no runtime behavior change, no secret leakage.
- Storage correctness reviewer: R2/S3 key isolation, missing-key mapping, object adapter composition, and future staging/cleanup risks.
