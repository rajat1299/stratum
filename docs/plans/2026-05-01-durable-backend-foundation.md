# Durable Backend Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Establish the first durable backend contract layer for Postgres metadata and S3/R2 object storage without changing Stratum's current local runtime behavior.

**Architecture:** Add narrow object, commit, and ref store traits plus local in-memory adapters that model the production contracts. Compose those contracts with the existing workspace, review, idempotency, and audit store traits, and add a SQL migration foundation documenting the future Postgres tables and atomic ref compare-and-swap requirements.

**Tech Stack:** Rust 2024, async-trait, Tokio tests, existing Stratum VCS/object/idempotency/audit models, SQL migration files.

---

## Product Decision

This slice is a contract foundation, not the cloud backend cutover. The existing local `.vfs/state.bin` persistence, local workspace metadata, local review store, local idempotency store, and local audit store continue to power the server.

The new backend contracts should make the next backend implementation explicit:

- object bytes are content-addressed by `sha256(raw_bytes)` and kind-checked;
- commit metadata is queryable independently from object bytes;
- ref updates are compare-and-swap operations with explicit expectations;
- source-checked ref updates are a single metadata-store transaction;
- idempotency and audit retain their current semantics and are composed into the backend surface rather than rewritten.

Out of scope: live Postgres connections, S3/R2 network writes, Redis locks, cross-store distributed transactions, normalized inode/path tables, chunk manifests, signed URLs, secret-aware idempotency replay, and HTTP behavior changes.

## Task 1: Backend Contract Module

**Files:**
- Create: `src/backend/mod.rs`
- Modify: `src/lib.rs`

**Requirements:**
- Add a public `backend` module with rustdoc explaining that it is a contract layer for durable backends.
- Define `RepoId` as a small validated newtype with a `RepoId::local()` helper for the current single-repo runtime.
- Define `ObjectWrite`, `StoredObject`, and `ObjectStore`.
- Define `CommitRecord` and `CommitStore` using the existing `CommitId`, `ObjectId`, and `ChangedPath` types.
- Define `RefVersion`, `RefExpectation`, `RefRecord`, `RefUpdate`, `SourceCheckedRefUpdate`, and `RefStore`.
- Define `StratumStores` as a composition of `ObjectStore`, `CommitStore`, `RefStore`, `SharedWorkspaceMetadataStore`, `SharedReviewStore`, `SharedIdempotencyStore`, and `SharedAuditStore`.
- Do not route production `StratumDb` through these traits yet.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend:: -- --nocapture
```

## Task 2: Local Contract Adapters And Tests

**Files:**
- Modify: `src/backend/mod.rs`

**Requirements:**
- Add in-memory local adapters for object, commit, and ref contracts.
- Object puts must be idempotent for the same `kind + object id + bytes`.
- Object reads must return `Ok(None)` for missing objects and `CorruptStore` for kind mismatches.
- Commit inserts must be idempotent for identical commit records and reject same-id/different-record conflicts.
- Commit listing must return newest-first by insertion order to mirror the current VCS log contract.
- Ref creation must use `RefExpectation::MustNotExist`.
- Ref CAS must reject stale target or stale version without mutating the ref.
- Source-checked ref CAS must verify the source target and target expectation under one write lock.
- Add tests for object dedup/kind mismatch, commit insert/list behavior, stale ref CAS, and source-checked CAS leaving the target unchanged on source mismatch.

**Verification:**

```bash
cargo test --locked backend:: -- --nocapture
```

## Task 3: Migration Foundation

**Files:**
- Create: `migrations/postgres/0001_durable_backend_foundation.sql`

**Requirements:**
- Add a minimal SQL migration foundation for the future Postgres metadata store.
- Include tables for repos, objects, commits, commit parents, refs, idempotency records, audit events, workspaces, workspace tokens, protected ref rules, protected path rules, change requests, approvals, review comments, and reviewer assignments.
- Add uniqueness/foreign-key constraints that express:
  - object ids are unique per repo and kind;
  - commits reference a root tree object;
  - refs are unique per repo/name and carry a version;
  - idempotency records are unique per scope/key hash;
  - audit sequence is unique per repo when repo-scoped.
- Include SQL comments documenting that ref compare-and-swap must update `refs` with `WHERE commit_id = expected AND version = expected`.
- Do not add a SQL client dependency in this slice.

**Verification:**

```bash
git diff --check -- migrations/postgres/0001_durable_backend_foundation.sql
```

## Task 4: Idempotency And Ref Contract Coverage

**Files:**
- Modify: `src/backend/mod.rs`

**Requirements:**
- Add a composition test that creates `StratumStores::local_memory()` and verifies the existing idempotency store semantics through the composed backend surface:
  - first request executes;
  - completed same-key/same-fingerprint replays;
  - same-key/different-fingerprint conflicts.
- Add a ref contract test that models a change-request merge:
  - create `main` and `review/<id>` refs;
  - source-checked CAS updates `main` when source and target expectations match;
  - a later stale source attempt fails without mutating `main`.

**Verification:**

```bash
cargo test --locked backend:: -- --nocapture
```

## Task 5: Docs And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Update project status to record the durable backend contract foundation and the exact remaining cloud backend gaps.
- Correct stale top-level project status fields so the latest completed slice and baseline merge match the current handoff before adding this slice.
- Document in the HTTP guide that the current HTTP behavior remains local-backed, while the backend contracts now define the future durable metadata/object/idempotency/audit boundaries.
- Keep the docs factual: no claim that Postgres/S3 runtime is operational.

## Review And Full Verification

Dispatch separate reviewers after implementation:

- correctness/API reviewer focused on trait shape, ref CAS semantics, and preserving local behavior;
- security/reliability reviewer focused on idempotency replay semantics, audit metadata boundaries, and migration risks.

Run focused checks before the implementation commit:

```bash
cargo fmt --all -- --check
cargo test --locked backend:: -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Run the full gate before merging:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
