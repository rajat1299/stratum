# Postgres Migration Harness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an executable Postgres migration smoke harness so the durable backend schema contract is tested in CI before a live Postgres adapter exists.

**Architecture:** Keep Stratum's runtime local-backed. Add a SQL smoke script that applies the existing Postgres migration inside a rollback-only transaction and validates key constraints plus ref compare-and-swap semantics. Add a dedicated GitHub Actions job with a Postgres service container to run the harness, leaving the existing Rust test jobs unchanged.

**Tech Stack:** PostgreSQL SQL/PLpgSQL, `psql -v ON_ERROR_STOP=1`, GitHub Actions service containers, existing Rust CI workflow.

---

## Product Decision

This slice is a database verification harness, not a cloud backend cutover.

The last slice made the durable schema more concrete, but the migration is still only a file unless CI proves it can execute and enforce the intended constraints. The useful next step is a real Postgres smoke gate that catches schema drift before adding a `tokio-postgres` or `sqlx` runtime adapter.

In scope:

- a rollback-only Postgres smoke script for `migrations/postgres/0001_durable_backend_foundation.sql`;
- a small local runner script that skips cleanly when `STRATUM_POSTGRES_TEST_URL` is unset outside required mode;
- a dedicated CI job using a Postgres service container and health check;
- docs/status updates recording that the migration is executable in CI;
- migration fixes only if the harness exposes a real contract defect.

Out of scope:

- a live Postgres metadata adapter;
- a Rust Postgres dependency in `[dependencies]`;
- a Rust dev-dependency unless the shell/SQL harness proves insufficient;
- a migration runner crate;
- runtime reads of `DATABASE_URL`;
- HTTP, MCP, CLI, FUSE, or `StratumDb` behavior changes;
- distributed locking or cross-store transactions.

## Task 1: SQL Smoke Harness

**Files:**
- Create: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Requirements:**
- Start a transaction and roll it back at the end.
- Include `migrations/postgres/0001_durable_backend_foundation.sql`.
- Define local helper functions:
  - `assert_true(boolean, text)`
  - `assert_raises(text, text, text, text)` with expected SQLSTATE and optional expected constraint name
- Insert minimal repo/object/commit/ref/change-request/review/audit rows.
- Assert the schema enforces:
  - repo ID envelope;
  - lowercase 64-character object IDs;
  - `objects.sha256 = objects.object_id`;
  - commit root tree FK scoped by repo;
  - commit parent FK scoped by repo;
  - ref commit FK scoped by repo;
  - ref version bounds;
  - idempotency pending/completed state shape;
  - per-repo audit sequence uniqueness and global `repo_id IS NULL` sequence uniqueness;
  - change-request base/head commit FKs scoped by repo;
  - active approval uniqueness for `(change_request_id, head_commit, approved_by)`.
- Assert the documented ref CAS SQL shape:
  - matching target/version updates one row and increments version;
  - stale target updates zero rows and leaves the ref unchanged;
  - stale version updates zero rows and leaves the ref unchanged;
  - source-checked CAS uses a source-row-locking SQL shape and only updates when the source ref still matches.

## Task 2: Local Runner

**Files:**
- Create: `scripts/check-postgres-migrations.sh`

**Requirements:**
- Use `set -euo pipefail`.
- Resolve the repository root without assuming the current working directory.
- If `STRATUM_POSTGRES_TEST_URL` is unset, print a short skip message and exit 0.
- If `STRATUM_POSTGRES_MIGRATIONS_REQUIRED=1` or `GITHUB_ACTIONS=true`, fail when `STRATUM_POSTGRES_TEST_URL` is unset.
- Reject password-bearing `STRATUM_POSTGRES_TEST_URL` values; use `PGPASSWORD`, `PGPASSFILE`, or `PGSERVICE` for credentials instead.
- If set, run:

```bash
psql "$STRATUM_POSTGRES_TEST_URL" \
  -v ON_ERROR_STOP=1 \
  -f "$repo_root/tests/postgres/0001_durable_backend_foundation_smoke.sql"
```

- Do not print credentials or connection URLs.

## Task 3: CI Job

**Files:**
- Modify: `.github/workflows/rust-ci.yml`

**Requirements:**
- Add a separate `postgres-migrations` job.
- Use `ubuntu-latest`.
- Keep permissions at `contents: read`.
- Use a `postgres:16` service container.
- Configure health checks with `pg_isready`, matching the GitHub Actions service-container pattern.
- Map `5432:5432` because the job runs directly on the runner.
- Keep checkout pinned to the same SHA used by existing jobs.
- Install the PostgreSQL client explicitly before invoking `psql`.
- Run `./scripts/check-postgres-migrations.sh` with required mode and a credential-free connection URL:

```yaml
env:
  STRATUM_POSTGRES_MIGRATIONS_REQUIRED: "1"
  STRATUM_POSTGRES_TEST_URL: postgresql://stratum@localhost:5432/stratum_test
  PGPASSWORD: stratum
```

## Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Requirements:**
- Record that the Postgres migration is now executable through a smoke harness and CI service-container job.
- Keep the local-backed runtime boundary explicit.
- Keep "no live Postgres metadata adapter" explicit.
- Update recommended next slices so the next Postgres step is a real adapter only after this harness is in place.

## Focused Verification

Run without a local Postgres URL:

```bash
./scripts/check-postgres-migrations.sh
```

Expected: exits 0 with a skip message.

Run with a local Postgres URL when available:

```bash
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
```

Expected: exits 0 after applying and rolling back the smoke script.

Run normal checks:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

## Review And Full Verification

Dispatch separate reviewers after implementation:

- CI/reliability reviewer focused on GitHub Actions service-container behavior, credential handling, local skip behavior, and whether the job is isolated from runtime behavior.
- schema/correctness reviewer focused on SQL assertions, transaction rollback, ref CAS semantics, and whether the smoke script meaningfully protects the migration contract.

Run the full gate before merging:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
