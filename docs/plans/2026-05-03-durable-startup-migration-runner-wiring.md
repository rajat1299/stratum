# Durable Startup Migration Runner Wiring Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Wire the existing Postgres migration runner into durable `stratum-server` startup preflight without cutting the server runtime over to Postgres/R2.

**Architecture:** Keep `STRATUM_BACKEND=local` unchanged. For `STRATUM_BACKEND=durable`, validate durable env first, then run a feature-gated Postgres migration preflight when the binary is built with `--features postgres`; after preflight, keep `stratum-server` fail-closed until the runtime cutover lands. Support a status-only default mode and an explicit apply mode so operators can prepare schema state without accidentally enabling the durable runtime.

**Tech Stack:** Rust 2024, Tokio, `tokio-postgres` behind the existing `postgres` feature, existing `PostgresMigrationRunner`, existing `BackendRuntimeConfig`, process startup tests.

---

## Scope Boundaries

In scope:

- Add durable migration startup configuration.
- Wire migration status/apply preflight into `stratum-server` startup for `STRATUM_BACKEND=durable`.
- Preserve default non-`postgres` builds and local runtime behavior.
- Keep durable runtime fail-closed after migration preflight.
- Add focused unit and process tests.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

Out of scope:

- No server runtime cutover to `PostgresMetadataStore`.
- No R2/S3 object-byte runtime cutover.
- No connection pooling, TLS/KMS/secrets expansion, or hosted operations.
- No migration rollback/down migration support.
- No HTTP/admin migration endpoint.

## Design Decisions

- Add `STRATUM_DURABLE_MIGRATION_MODE` with values:
  - `status` by default: connect to Postgres and inspect migration state without applying pending migrations.
  - `apply`: apply pending migrations with the existing schema-scoped advisory lock, then inspect final state.
- Add optional `STRATUM_POSTGRES_SCHEMA`, defaulting to `public`, so tests and operators can target a specific schema. Validation should reuse the existing `PostgresMigrationRunner::with_schema` path.
- If the binary is not built with `postgres`, durable startup should preserve the current fail-closed path and must not try to connect.
- If built with `postgres`, pending migrations in `status` mode should fail with an operator-facing message that says to set `STRATUM_DURABLE_MIGRATION_MODE=apply`; dirty, unknown, or mismatched migration state must fail before the local store is opened.
- `STRATUM_POSTGRES_URL` must still reject embedded passwords. Startup migration connection may consume `PGPASSWORD` if set, but must not store or log it.
- Error messages must not include connection strings, R2 access keys, R2 secret keys, or Postgres passwords.

## Task 1: Runtime Config And Preflight API

**Files:**

- Modify: `src/backend/runtime.rs`

**Steps:**

1. Add constants for `STRATUM_DURABLE_MIGRATION_MODE` and `STRATUM_POSTGRES_SCHEMA`.
2. Add a `DurableMigrationMode` enum with `Status` and `Apply`, parsing empty/missing as `Status`.
3. Extend `DurableBackendRuntimeConfig` with private `postgres_url`, private `postgres_schema`, and private `migration_mode` fields. Keep `Debug` redacted.
4. Add accessors for migration mode and schema if useful for tests.
5. Add an async `prepare_server_startup(&self) -> Result<DurableStartupPreflight, VfsError>` on `BackendRuntimeConfig`.
6. For local mode, return a no-op preflight result.
7. For durable mode without the `postgres` feature, return a preflight result indicating migrations were not checked; let `ensure_supported_for_server` keep the existing fail-closed behavior.
8. For durable mode with the `postgres` feature:
   - Parse `postgres_url` into `tokio_postgres::Config`.
   - Defensively reject parsed configs containing a password.
   - Apply `PGPASSWORD` to the config only if it is non-empty.
   - Create `PostgresMigrationRunner::with_schema`.
   - In `status` mode, call `status()`.
   - In `apply` mode, call `apply_pending()`.
   - Reject dirty, checksum-mismatched, unknown-applied, and pending-in-status-mode reports with secret-free errors.
9. Add runtime unit tests for default migration mode, apply mode parsing, invalid migration mode, schema default/override, and debug redaction.

**Commit target:** `feat: add durable startup migration preflight`

## Task 2: Server Startup Wiring And Process Tests

**Files:**

- Modify: `src/bin/stratum_server.rs`
- Modify: `tests/server_startup.rs`

**Steps:**

1. Call `backend_runtime.prepare_server_startup().await` after logging startup mode and before `ensure_supported_for_server()`.
2. Keep `StratumDb::open(config)` after both durable preflight and durable fail-closed checks so durable failures never create `.vfs`.
3. Preserve the current default-build process tests.
4. Add feature-gated process tests for `postgres` builds:
   - missing durable env still fails before local store creation.
   - complete durable env with `STRATUM_DURABLE_MIGRATION_MODE=status` and an empty isolated schema fails because migrations are pending.
   - complete durable env with `STRATUM_DURABLE_MIGRATION_MODE=apply` and an isolated schema applies migrations, then fails closed because runtime cutover is not wired.
   - dirty migration control state fails before local store creation.
5. Live process tests must skip unless `STRATUM_POSTGRES_TEST_URL` is set or required by `STRATUM_POSTGRES_TEST_REQUIRED=1` / GitHub Actions.
6. Process tests must assert output does not include R2 credentials or Postgres password material.

**Commit target:** `feat: wire durable startup migrations`

## Task 3: Documentation And Status

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Steps:**

1. Document `STRATUM_DURABLE_MIGRATION_MODE=status|apply`.
2. Document `STRATUM_POSTGRES_SCHEMA`.
3. State that durable startup can now inspect/apply migrations when built with `postgres`, but still fails closed before opening local state.
4. Update the latest backend slice and recommended-next-slices status.
5. Preserve SDK/DX lane content.

**Commit target:** `docs: document durable startup migration wiring`

## Verification

Run from `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation`:

```bash
cargo fmt --all -- --check
cargo check --locked --features postgres
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Post-merge main verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
git diff --check
```
