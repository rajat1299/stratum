# Durable Cleanup Claims Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a durable cleanup-claim and orphan-repair foundation for object metadata recovery without enabling unsafe final-object deletion.

**Architecture:** Add an object cleanup claim contract with expiring leases, owner/token validation, completion, and failure recording. Back it with local memory and the optional Postgres adapter, then add a claim-backed `BlobObjectStore` repair helper that recreates missing object metadata from converged final bytes after rechecking state under a claim. Keep final-object delete mode fail-closed until metadata writes and cleanup workers share a stronger blocking protocol.

**Tech Stack:** Rust 2024, async-trait, Tokio tests, optional `tokio-postgres`, PostgreSQL `INSERT ... ON CONFLICT DO UPDATE`, existing byte-store/object metadata contracts.

---

## Product Decision

This slice repairs the known object-store gap from the upload-staging slice: a final content-addressed object can exist after metadata insertion fails. The safe first product move is to claim and repair metadata for those final bytes, not delete them.

Official docs checked:

- PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` provides an atomic insert-or-update outcome under concurrency.
- PostgreSQL row locking docs confirm `SKIP LOCKED` is queue-oriented and can expose an inconsistent view, so this slice uses targeted upsert/CAS predicates instead of queue scans.

SMFS reference considered:

- SMFS's push queue and daemon cleanup patterns are useful for operation state, attempts, and `last_error`.
- Do not copy latest-wins sync, document-path restrictions, or local-dirty behavior. Stratum cleanup must stay content-addressed, repo scoped, and metadata/ref aware.

In scope:

- `ObjectCleanupClaimStore` trait for claim, complete, and failure recording;
- in-memory claim store for local contract tests;
- Postgres `object_cleanup_claims` schema and optional adapter implementation;
- claim token and lease-owner validation;
- claim-backed final-object metadata repair helper in `BlobObjectStore`;
- tests for claim acquisition, active lease exclusion, expired lease retry, stale token completion rejection, Postgres adapter coverage, and object metadata repair;
- docs/status updates.

Out of scope:

- background cleanup worker or scheduler;
- runtime Postgres/R2 cutover;
- final-object deletion;
- distributed object-store transaction protocol;
- lifecycle-policy automation, multipart cleanup, or signed upload cleanup;
- SDK/DX lane work.

## Task 1: Cleanup Claim Contract

**Files:**
- Create: `src/backend/object_cleanup.rs`
- Modify: `src/backend/mod.rs`

**Requirements:**
- Add `ObjectCleanupClaimKind::FinalObjectMetadataRepair`.
- Add `ObjectCleanupClaimRequest` with:
  - repo id;
  - claim kind;
  - object kind;
  - object id;
  - object key;
  - lease owner;
  - lease duration.
- Keep claim timing store-owned: in-memory uses the local clock, Postgres uses database `clock_timestamp()`, and callers never supply authoritative timestamps.
- Generate a unique lease token per claim request.
- Reject empty or control-character lease owners.
- Reject lease durations shorter than one millisecond.
- Add `ObjectCleanupClaim` carrying the lease token, attempts, target, owner, and expiry.
- Add `ObjectCleanupClaimStore` with:
  - `claim(request) -> Option<ObjectCleanupClaim>`;
  - `complete(claim)`;
  - `record_failure(claim, message)`.
- Add `InMemoryObjectCleanupClaimStore` with:
  - first claim succeeds;
  - active unexpired claim returns `None`;
  - expired incomplete claim can be retried with a new token and attempts incremented;
  - completed claim returns `None`;
  - stale token completion/failure returns an object-write conflict.

**Verification:**

```bash
cargo test --locked backend::object_cleanup -- --nocapture
```

## Task 2: Postgres Claim Schema And Adapter

**Files:**
- Modify: `migrations/postgres/0001_durable_backend_foundation.sql`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`
- Modify: `src/backend/postgres.rs`

**Requirements:**
- Add `object_cleanup_claims` with repo-scoped rows, claim kind, object kind, object id, canonical object key, lease token, lease owner, lease expiry, attempts, last error, completed timestamp, created timestamp, and updated timestamp.
- Keep claims independent of the `objects` table because repair claims are specifically for objects whose metadata row may be missing.
- Add checks for valid claim kind, object kind, 64-char lowercase object ids, UUID-shaped lease tokens, non-empty owner, positive attempts, and completed rows with a final owner/token.
- Implement `ObjectCleanupClaimStore` for `PostgresMetadataStore`.
- Use atomic upsert predicates so only missing or expired incomplete rows can be acquired.
- Require matching, unexpired lease token for completion/failure updates.
- Extend Postgres adapter tests to cover first claim, active-lease exclusion, expired retry, completion, and stale-token rejection.

**Verification:**

```bash
cargo test --locked --features postgres backend::postgres -- --nocapture
./scripts/check-postgres-migrations.sh
```

## Task 3: Claim-Backed Orphan Repair

**Files:**
- Modify: `src/backend/blob_object.rs`

**Requirements:**
- Add report fields for metadata repair and skipped active claims.
- Add a claim-backed repair helper for old final objects missing matching metadata.
- For every old final key:
  - parse the key;
  - skip when matching metadata exists;
  - count it as a final orphan otherwise;
  - acquire a repair claim;
  - recheck metadata after claim acquisition;
  - read final bytes;
  - verify bytes hash to the key's object id;
  - write idempotent metadata;
  - complete the claim.
- If metadata appears after claim acquisition, complete the claim without rewriting.
- If metadata exists with different attributes, record a per-key error and mark the claim failed.
- Preserve `FinalObjectsMissingMetadataDelete` as `NotSupported`.

**Verification:**

```bash
cargo test --locked backend::blob_object -- --nocapture
```

## Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Requirements:**
- Re-open `docs/project-status.md` immediately before editing because the SDK/DX lane may update it separately.
- Document cleanup claims, claim-backed metadata repair, and Postgres schema coverage.
- State that no cleanup worker or HTTP/MCP/CLI endpoint invokes repair yet.
- State that final-object delete mode still fails closed.

## Task 5: Review And Verification

Dispatch independent reviewers after implementation:

- Rust/API reviewer: trait shape, validation, error handling, and local runtime compatibility.
- Storage correctness reviewer: claim/lease concurrency, metadata repair race behavior, Postgres SQL predicates, and no unsafe delete path.

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked --features postgres backend::postgres -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
