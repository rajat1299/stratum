# Postgres Migration Runner Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a feature-gated Postgres migration runner foundation that tracks ordered migrations, reports status, refuses dirty or mismatched state, and serializes startup migration work without cutting `stratum-server` over to the durable runtime.

**Architecture:** Add a `postgres_migrations` backend module behind the existing `postgres` feature. The runner owns a `stratum_schema_migrations` control table, uses a schema-scoped advisory lock, applies the existing `0001_durable_backend_foundation.sql` exactly once, and records clean/dirty migration state without logging connection strings or secrets.

**Tech Stack:** Rust 2024, `tokio-postgres`, existing `VfsError`, existing Postgres schema fixture, live Postgres tests gated by `STRATUM_POSTGRES_TEST_URL`.

---

### Task 1: Add the Migration Runner Module

**Files:**
- Create: `src/backend/postgres_migrations.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/backend/postgres.rs`
- Test: `src/backend/postgres_migrations.rs`

**Step 1: Write failing tests**

Add feature-gated tests that create an isolated schema, instantiate a runner, and assert:

- fresh schemas report migration `1` as pending;
- applying pending migrations creates `stratum_schema_migrations`, applies `0001`, records `applied`, and a second apply is a no-op;
- `Debug` output includes schema/catalog details but not a Postgres URL.

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres_migrations -- --nocapture
```

Expected: fail because `backend::postgres_migrations` does not exist.

**Step 2: Implement minimal runner**

Add:

- `PostgresMigrationRunner`;
- `PostgresMigration`;
- `PostgresMigrationReport`;
- `PostgresMigrationStatus`;
- static catalog for `0001_durable_backend_foundation.sql`;
- `status()` and `apply_pending()` methods;
- `stratum_schema_migrations` control-table creation;
- migration checksum calculation with SHA-256;
- schema validation/reuse of Postgres helpers without exposing secrets.

Run the focused test again and make it pass.

### Task 2: Add Dirty-State, Checksum, And Lock Refusals

**Files:**
- Modify: `src/backend/postgres_migrations.rs`

**Step 1: Write failing tests**

Add live Postgres tests that assert:

- a `started` or `failed` row causes `apply_pending()` to return `VfsError::CorruptStore`;
- an applied row with the wrong checksum causes `apply_pending()` to return `VfsError::CorruptStore`;
- a held schema advisory lock causes another runner to return a clear `VfsError::ObjectWriteConflict`.

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres_migrations -- --nocapture
```

Expected: fail until the refusal paths are implemented.

**Step 2: Implement refusal paths**

Add:

- `pg_try_advisory_lock` around `apply_pending()`;
- best-effort unlock after apply/status planning;
- dirty-state detection before applying any migration;
- checksum mismatch detection for applied migrations;
- unknown applied-version refusal.

Run the focused test again and make it pass.

### Task 3: Update Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update docs**

Document that:

- a feature-gated Rust migration runner foundation exists;
- `stratum-server` still does not run migrations on startup;
- the runner records applied migration state, reports pending/dirty/mismatched state, and serializes runners with a schema-scoped advisory lock;
- existing shell smoke harness remains rollback-only and explicit.

**Step 2: Update verification status**

Add the focused and full verification commands for this slice to `docs/project-status.md`.

### Task 4: Verify And Integrate

**Files:**
- All changed files

**Step 1: Focused verification**

Run:

```bash
cargo fmt --all -- --check
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres_migrations -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
```

**Step 2: Full verification**

Run:

```bash
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

**Step 3: Commit and merge**

Commit on `v2/foundation`, push it, merge to `main`, run the post-merge gates, and push `main`.
