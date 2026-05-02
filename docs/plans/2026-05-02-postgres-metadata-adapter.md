# Postgres Metadata Adapter Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a real Postgres-backed metadata adapter for durable object metadata, commit metadata, and ref compare-and-swap contracts without changing the local runtime.

**Architecture:** Keep `stratum-server` and `StratumDb` local-backed. Add an optional `postgres` Cargo feature exposing a backend module that implements existing metadata-facing contracts over the current `0001` Postgres schema. Tests create an isolated schema, apply the migration through `tokio-postgres`, exercise the adapter, and drop the schema afterward.

**Tech Stack:** Rust 2024, `async-trait`, optional `tokio-postgres` with `serde_json` support, PostgreSQL row locks with `SELECT ... FOR UPDATE`, GitHub Actions `postgres:16` service container.

---

## Product Decision

This slice creates the first real Postgres metadata adapter, not a hosted backend cutover.

The useful step after the migration smoke harness is to make the durable schema executable from Rust through the backend contracts. The adapter should prove that object metadata, commit metadata, and ref CAS semantics can round-trip through Postgres. It must not replace `.vfs/state.bin`, wire server config to a database URL, or claim production cloud transaction coverage.

Primary docs checked:

- `tokio-postgres` docs: the client is async and `Send + Sync`; the connection object must be driven by a spawned task; `Client::transaction` rolls back by default unless committed; `Error::code` / `DbError::constraint` expose SQLSTATE and constraint names.
- PostgreSQL docs: `SELECT ... FOR UPDATE` locks selected rows against concurrent updates; in Read Committed, plain reads can see a statement snapshot while subsequent commands may see different committed state, so source-checked ref updates need row locks in one transaction.

In scope:

- optional `postgres` feature and dependency;
- `src/backend/postgres.rs`;
- Postgres-backed implementation of `ObjectMetadataStore`;
- Postgres-backed implementation of `CommitStore`;
- Postgres-backed implementation of `RefStore`;
- isolated Postgres feature tests using the existing migration;
- CI job for `cargo clippy --features postgres` and `cargo test --features postgres backend::postgres`;
- docs/status updates.

Out of scope:

- HTTP, MCP, CLI, FUSE, or `StratumDb` runtime cutover;
- connection pooling;
- TLS/KMS/secrets posture beyond non-URL test passwords;
- production migration runner;
- idempotency, audit, workspace, or review Postgres adapters;
- object byte storage cutover to S3/R2;
- cross-store transaction spanning object bytes plus metadata;
- concurrent Postgres stress tests beyond focused CAS/source-lock behavior.

## Task 1: Feature And Module Boundary

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/backend/mod.rs`
- Create: `src/backend/postgres.rs`

**Requirements:**
- Add an optional `tokio-postgres` dependency:

```toml
tokio-postgres = { version = "0.7", optional = true, default-features = false, features = ["runtime", "with-serde_json-1"] }
```

- Add a feature:

```toml
postgres = ["dep:tokio-postgres"]
```

- Expose the module only when enabled:

```rust
#[cfg(feature = "postgres")]
pub mod postgres;
```

- Do not change default features or default runtime behavior.

**Verification:**

```bash
cargo check --locked
cargo check --locked --features postgres
```

Expected: both compile.

## Task 2: Postgres Connection And Test Schema Harness

**Files:**
- Modify: `src/backend/postgres.rs`

**Requirements:**
- Define `PostgresMetadataStore` holding a cloned `tokio_postgres::Config` plus a validated schema name.
- Provide constructors:
  - `PostgresMetadataStore::new(config: tokio_postgres::Config)`, pinned to the validated `public` schema;
  - `PostgresMetadataStore::with_schema(config, schema)`.
- Connect per operation for this foundation slice:
  - call `config.connect(NoTls).await`;
  - spawn the returned connection future;
  - run `SET search_path TO "<schema>"` after validating the schema identifier.
- Map connection/query errors into `VfsError` without leaking connection strings or passwords.
- Add test-only helpers that:
  - read `STRATUM_POSTGRES_TEST_URL`;
  - if missing, skip locally but panic when `STRATUM_POSTGRES_TEST_REQUIRED=1` or `GITHUB_ACTIONS=true`;
  - apply `STRATUM_POSTGRES_TEST_PASSWORD` to the parsed config when present;
  - create a unique lowercase schema;
  - apply `migrations/postgres/0001_durable_backend_foundation.sql` inside that schema;
  - clean up with `DROP SCHEMA ... CASCADE` at test end.

**Verification:**

```bash
cargo test --locked --features postgres backend::postgres -- --nocapture
```

Expected locally without env: tests skip cleanly. With Postgres env: tests run against an isolated schema.

## Task 3: Object Metadata Adapter

**Files:**
- Modify: `src/backend/postgres.rs`

**Requirements:**
- Implement `crate::backend::blob_object::ObjectMetadataStore` for `PostgresMetadataStore`.
- On `put`:
  - upsert or ensure the `repos` row exists for `record.repo_id`;
  - if an object metadata row exists, return it when identical and return `CorruptStore` when attributes differ;
  - otherwise insert into `objects`.
- On `get`:
  - load by `(repo_id, object_id)`;
  - map `kind` from `blob`, `tree`, `commit`;
  - reject unknown database values as `CorruptStore`.
- Keep object bytes out of Postgres.

**Tests:**
- Metadata put/get round trips.
- Duplicate identical put is idempotent.
- Concurrent duplicate identical puts are idempotent.
- Duplicate same object ID with different kind/key/size is rejected.
- `BlobObjectStore` can compose `PostgresMetadataStore` with `LocalBlobStore` and round-trip bytes.

## Task 4: Commit Metadata Adapter

**Files:**
- Modify: `src/backend/postgres.rs`

**Requirements:**
- Implement `CommitStore` for `PostgresMetadataStore`.
- On `insert`:
  - ensure the repo row exists;
  - require the root tree object metadata to exist through the schema FK;
  - if the commit exists and matches the incoming record including parents and `changed_paths`, return the existing record;
  - if the commit exists with different metadata, return `AlreadyExists`;
  - insert `commits` and ordered `commit_parents` in one transaction.
- On `get`:
  - return `Ok(None)` for missing commits;
  - load parents ordered by `parent_order`;
  - decode `changed_paths_json` as `Vec<ChangedPath>`.
- On `list`:
  - return newest-first deterministic ordering, using `created_at DESC`, `commit_timestamp_seconds DESC`, and `id DESC`.
- Reject timestamps above Postgres `BIGINT` range before SQL.

**Tests:**
- Insert/get/list newest-first round trip with a seeded tree object.
- Duplicate identical commit insert is idempotent.
- Concurrent duplicate identical commit inserts are idempotent.
- Duplicate conflicting commit metadata returns `AlreadyExists`.
- Parent FK is enforced and parent order round-trips.

## Task 5: Ref Metadata Adapter

**Files:**
- Modify: `src/backend/postgres.rs`

**Requirements:**
- Implement `RefStore` for `PostgresMetadataStore`.
- `update`:
  - ensure repo row exists;
  - `MustNotExist` inserts version `1` and returns conflict if the ref exists;
  - `Matches` updates only when current target and version match, increments version, and returns conflict on zero rows;
  - unknown target commits are rejected by the database FK and mapped to an `InvalidArgs`-style error.
- `update_source_checked`:
  - reject mismatched source/target repo IDs before SQL;
  - use one transaction;
  - lock source and target rows with `SELECT ... FOR UPDATE`;
  - check the source expectation after acquiring the source lock;
  - apply the target CAS under the same transaction;
  - commit only after both checks pass.
- Reject `RefVersion` values above Postgres `BIGINT` before SQL.

**Tests:**
- `MustNotExist` creates a ref at version 1 and duplicate create conflicts without mutation.
- Matching CAS increments version.
- Stale target and stale version return conflict without mutation.
- Unknown target commit returns an error without mutation.
- Source-checked CAS updates target when source and target match.
- Source mismatch leaves target unchanged.
- Mismatched source/target repo IDs are rejected before SQL.
- A stale expectation at Postgres `BIGINT` max returns CAS mismatch, while a matching ref at that max version reports version overflow without mutation.

## Task 6: CI

**Files:**
- Modify: `.github/workflows/rust-ci.yml`

**Requirements:**
- Add a separate `postgres-backend` job.
- Use a `postgres:16` service container, matching the existing migration harness pattern.
- Install Rust with the pinned `dtolnay/rust-toolchain` action.
- Run:

```bash
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked --features postgres backend::postgres -- --nocapture
```

- Use a credential-free URL and separate test password:

```yaml
env:
  STRATUM_POSTGRES_TEST_REQUIRED: "1"
  STRATUM_POSTGRES_TEST_URL: postgresql://stratum@localhost:5432/stratum_test
  STRATUM_POSTGRES_TEST_PASSWORD: stratum
```

## Task 7: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Requirements:**
- Record that a Postgres metadata adapter exists for object metadata, commits, and refs behind the optional feature.
- Keep local runtime boundary explicit.
- Keep no connection pooling, migration runner, runtime cutover, S3/R2 byte cutover, or cross-store transactions explicit.
- Update recommended next slices.

## Focused Verification

Without local Postgres:

```bash
cargo test --locked --features postgres backend::postgres -- --nocapture
```

Expected: tests skip cleanly.

With local Postgres:

```bash
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres \
cargo test --locked --features postgres backend::postgres -- --nocapture
```

Expected: adapter tests create an isolated schema, apply the migration, pass, and clean up.

Normal feature checks:

```bash
cargo fmt --all -- --check
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked --features postgres backend::postgres -- --nocapture
git diff --check
```

Full gate before merge:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo test --locked --features postgres backend::postgres -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

## Review

Dispatch independent reviewers after implementation:

- Rust/API reviewer: feature-gating, trait implementations, conversion/error handling, no default-runtime behavior change.
- Postgres correctness reviewer: transactions, row locks, FK handling, idempotent inserts, isolated test schema, credential handling.

Review fixes folded into this slice:

- Use conflict-aware object metadata and commit inserts so concurrent identical duplicate writes preserve idempotency.
- Pin the default adapter schema to `public` and keep isolated tests on explicit validated schemas.
- Reject password-bearing Postgres test configs after parsing so URL query and keyword DSN passwords are not accepted.
- Harden the migration smoke script's password guard for URL, query, and keyword DSN password forms.
- Preserve stale CAS mismatch behavior for high ref-version expectations and only report max-version overflow when the stored ref actually matches that max version.
