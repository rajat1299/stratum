# Real Postgres Pool, Secret Seam, And Migration Adoption Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace Stratum's per-operation Postgres connection churn with a real reusable pool, make Postgres password lookup a first-class deployment-secret seam, add safe adoption for already-migrated legacy schemas, and expose a durable review policy flag for frontend V1 review UX.

**Architecture:** Keep existing fail-closed hosted posture as the boundary: password-bearing URLs remain rejected, remote Postgres still requires `sslmode=require`, all acquire/connect/operation paths remain bounded, and every error exposed to operators or HTTP clients stays redacted. Postgres metadata stores and migration runner share one pooled connector abstraction; the secret provider is env-backed at first but injected through runtime seams. Migration adoption is explicit and lock-protected: it records only schemas whose known migration artifacts are fully verifiable and refuses dirty, unknown, mismatched, partial, or unverifiable states.

**Tech Stack:** Rust 2024, Tokio, `tokio-postgres`, `deadpool-postgres`, `postgres-native-tls`, native TLS, Axum, existing durable backend store traits, Postgres migration SQL, TypeScript/Python SDK contract fixtures.

---

## Context Read

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/plans/2026-05-14-hosted-storage-operations-hardening.md`
- `docs/plans/2026-05-16-durable-cloud-vcs-review-mutations.md`
- `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

## Current Baseline

- `v2/foundation` is clean at slice start.
- Main has unrelated local edits/untracked files; do not touch or revert them.
- Hosted Postgres hardening already includes TLS target validation, timeout knobs, `statement_timeout`, redacted errors, and semaphore gating.
- `PostgresConnector` still opens a fresh `tokio-postgres` connection per store operation; the semaphore is only a cap, not a reusable pool.
- `DurableBackendRuntimeConfig::postgres_config_with_env_password()` reads `PGPASSWORD` directly; this must become an injected secret seam.
- `PostgresMigrationRunner` supports `status` and `apply`; legacy schemas manually migrated before `stratum_schema_migrations` currently look pending and can fail dirty if `apply` replays DDL.
- User-provided live credentials are now provisioned in GitHub environment `live-gates`; wording remains: credentials provisioned; awaiting first scheduled provider-verified green.

## Acceptance Criteria

- Hosted durable startup uses pooled Postgres clients with bounded acquire, connect, and operation timeouts.
- Tests prove connection reuse; a semaphore-only cap is not sufficient.
- Pool acquisition failures, TLS failures, statement timeouts, migration errors, and secret resolution failures return fixed redacted errors.
- Password-bearing URLs remain rejected; no password, DB URL, host, endpoint, SQL text, migration SQL, or secret value leaks.
- Existing local TCP and Unix-socket Postgres development paths still work.
- Migration adoption records already-applied known migrations into `stratum_schema_migrations` without replaying DDL.
- Adoption refuses dirty, unknown, checksum-mismatched, partially applied, or unverifiable schemas.
- Durable-cloud startup fails closed when pool or secret config is invalid.
- Existing optional Postgres tests and local-state runtime behavior remain unchanged.
- Add `require_all_files_viewed: bool` to both protected ref/path rules, defaulting true, persisted in local and Postgres stores, returned by APIs, covered by tests, represented in SDK types/fixtures, and advertised in capability manifest under both rule groups.
- Do not implement file-view tracking or backend enforcement in this slice.

## Task 1: Plan Document

**Files:**
- Create: `docs/plans/2026-05-16-real-postgres-pool-secret-seam-migration-adoption.md`

**Step 1: Save this plan**

Write the plan before implementation so subagents have a stable contract.

**Step 2: Verify plan diff**

Run:

```bash
git diff -- docs/plans/2026-05-16-real-postgres-pool-secret-seam-migration-adoption.md
git diff --check
```

Expected: only the new plan doc; no whitespace errors.

**Step 3: Commit**

```bash
git add docs/plans/2026-05-16-real-postgres-pool-secret-seam-migration-adoption.md
git commit -m "docs: plan real postgres pool adoption slice"
```

## Task 2: Real Postgres Pool And Secret Seam

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/backend/runtime.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/bin/stratum_server.rs` if default secret-provider construction belongs at the binary boundary
- Modify: `tests/server_startup.rs`

**Step 1: Write failing pool and secret tests**

Add feature-gated tests before implementation:

- `PostgresConnector` reuses a checked-out connection after it is returned to the pool. Use a session-local marker such as backend PID, temporary setting, or a test-only connection create counter so the test fails under connect-per-operation behavior.
- Pool acquire timeout maps to `postgres operation timed out` and does not leak config.
- Hosted TLS build still succeeds without opening a socket.
- Localhost, loopback hostaddr, and Unix socket configs still use no-TLS.
- Remote no-TLS still fails closed without leaking the host.
- Secret provider returns a password through the seam, not direct runtime env reads.
- Secret provider failure maps to a fixed `postgres secret resolution failed` style error without leaking the env var value or provider details.
- Durable-cloud startup rejects invalid secret config before creating local state.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected before implementation: new reuse/secret tests fail; existing tests still indicate current semaphore-only behavior.

**Step 2: Add pool dependency**

Add `deadpool-postgres = { version = "0.14.1", optional = true }` behind the `postgres` feature. Preserve current TLS dependencies.

Run once without `--locked` to update the lockfile:

```bash
cargo check --features postgres
```

Expected: `Cargo.lock` includes `deadpool-postgres` and its transitive dependencies; no unrelated dependency changes.

**Step 3: Introduce the secret seam**

In `src/backend/runtime.rs`, add a narrow Postgres credential abstraction:

```rust
pub trait PostgresSecretProvider: Send + Sync {
    fn postgres_password(&self) -> Result<Option<String>, VfsError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EnvPostgresSecretProvider;
```

Implementation rules:

- The env-backed provider reads `PGPASSWORD`.
- Empty passwords are treated as absent.
- non-Unicode or provider failures map to a fixed redacted error.
- `DurableBackendRuntimeConfig` exposes a `postgres_config_with_secret_provider(...)` method.
- Existing `postgres_config_with_env_password()` becomes a small wrapper using `EnvPostgresSecretProvider`, or is removed only if all call sites are updated cleanly.
- Password-bearing URL rejection remains before secret resolution and keeps the existing message shape.

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: secret seam and URL rejection tests pass.

**Step 4: Replace semaphore gating with a real pool**

In `src/backend/postgres.rs`, refactor `PostgresConnector` so it owns a reusable pool instead of a semaphore.

Implementation rules:

- Build a `deadpool_postgres::Pool` with max size from `DurablePostgresRuntimePosture::pool_max_size()`.
- Construct the manager with either `NoTls` or `postgres_native_tls::MakeTlsConnector` after current target validation.
- Preserve `Config::connect_timeout`.
- Bound `pool.get()` with `pool_acquire_timeout()`.
- Bound per-operation queries and `batch_execute` with `operation_timeout()`.
- Ensure every checked-out client has the configured `search_path` and `statement_timeout`; run these on checkout if the pool crate has no creation hook.
- Keep all pool/connect/TLS/acquire failures mapped to fixed redacted errors.
- Keep `Debug` output limited to posture values and no DB URL, host, user, password, SQL, or secret env names.
- Tests must prove actual reuse, not only max-size blocking.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: pool tests pass and optional live portions still skip cleanly when `STRATUM_POSTGRES_TEST_URL` is unset.

**Step 5: Share pooled behavior with migrations and server startup**

Update `PostgresMigrationRunner` and durable server store opening to use the same connector semantics.

Implementation rules:

- Migration status/apply/adopt paths use pooled clients and the same TLS/timeout/redaction behavior.
- `ensure_control_plane_ready()` uses pooled clients.
- Durable-cloud startup still performs migration preflight before opening stores and fails closed on invalid pool or secret config.
- Local-state and optional Postgres tests remain unchanged.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected: migration and startup behavior remains redacted and bounded.

**Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/backend/runtime.rs src/backend/postgres.rs src/backend/postgres_migrations.rs src/server/mod.rs src/bin/stratum_server.rs tests/server_startup.rs
git commit -m "feat: pool postgres connections and isolate secrets"
```

## Task 3: Durable Protected Rule File-Viewed Flag

**Files:**
- Modify: `src/review.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/routes_capabilities.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `migrations/postgres/0001_durable_backend_foundation.sql`
- Create: `migrations/postgres/0013_protected_rules_require_all_files_viewed.sql`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`
- Modify: `docs/http-api-guide.md`
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/python/tests/test_client.py`
- Modify: `sdk/contracts/capabilities.v1.json`
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json`

**Step 1: Write failing Rust model, route, Postgres, and capability tests**

Add tests proving:

- `ProtectedRefRule` and `ProtectedPathRule` default `require_all_files_viewed` to `true`.
- Explicit `false` survives in-memory and local persisted review stores.
- Old local persisted records without the new field decode as `true`.
- Protected ref/path create APIs accept omitted/default `true` and explicit `false`.
- Protected rule list APIs return the field.
- Idempotency fingerprints include the normalized boolean, so same key with different boolean conflicts.
- Postgres insert/list/get and review contract tests persist the field.
- Capability manifest advertises the policy flag under both `protection.ref_rules` and `protection.path_rules`.

Run:

```bash
cargo test --locked review::tests --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected before implementation: new field tests fail.

**Step 2: Add the model field and local persistence compatibility**

In `src/review.rs`:

- Add `require_all_files_viewed: bool` to both rule structs.
- Use `#[serde(default = "default_require_all_files_viewed")]` so existing local persisted records decode to true.
- Add constructor variants or parameters so create paths can set explicit false.
- Keep enforcement behavior unchanged; do not read this field in policy blockers.
- Keep local store migration compatible with existing review store versions.

Run:

```bash
cargo test --locked review::tests --lib -- --nocapture
```

Expected: local model/store tests pass.

**Step 3: Persist the field in Postgres**

Add migration `0013_protected_rules_require_all_files_viewed.sql`:

```sql
ALTER TABLE protected_ref_rules
    ADD COLUMN require_all_files_viewed BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE protected_path_rules
    ADD COLUMN require_all_files_viewed BOOLEAN NOT NULL DEFAULT true;
```

Also update `0001_durable_backend_foundation.sql` for new installs and append migration 13 to the catalog.

Update Postgres review adapter SQL:

- Insert/returning for protected ref rules.
- List/get for protected ref rules.
- Insert/returning for protected path rules.
- List/get for protected path rules.
- Any approval-decision protected rule loading that selects rule columns.
- Control-plane readiness query.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: Postgres adapter and migration catalog tests pass or skip live portions cleanly.

**Step 4: Wire route requests, idempotency, audit, and capability manifest**

In `src/server/routes_review.rs`:

- Add optional request fields `require_all_files_viewed: Option<bool>`.
- Normalize omitted to true before idempotency fingerprinting.
- Pass the normalized value into the review store.
- Include the value in audit details only as a boolean, not as a new enforcement decision.

In `src/server/routes_capabilities.rs`:

- Add a manifest field such as `require_all_files_viewed_default: bool`.
- Set it to true for both ref and path rules.
- Bump `CAPABILITIES_REVISION`.
- Regenerate fixtures with the existing fixture update test.

Run:

```bash
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

Expected: route and manifest tests pass, and both SDK contract fixtures include the new manifest field.

**Step 5: Update SDK types and tests**

Update TypeScript and Python protected-rule request/response types and capability manifest types.

Add focused SDK tests proving:

- Capability fixtures include `require_all_files_viewed_default: true` under both rule groups.
- Protected ref/path create methods forward explicit `require_all_files_viewed: false`.
- Existing calls omitting the field remain type-valid.

Run:

```bash
cd sdk
bun run typecheck
bun run test:run
cd python
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
```

Expected: SDK tests and type checks pass.

**Step 6: Commit**

```bash
git add src/review.rs src/server/routes_review.rs src/server/routes_capabilities.rs src/backend/postgres.rs src/backend/postgres_migrations.rs migrations/postgres tests/postgres/0001_durable_backend_foundation_smoke.sql docs/http-api-guide.md sdk/typescript/src/types.ts sdk/typescript/tests/client.test.ts sdk/python/src/stratum_sdk/types.py sdk/python/tests/test_client.py sdk/contracts
git commit -m "feat: expose review file-view policy flag"
```

## Task 4: Safe Migration Adoption

**Files:**
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `src/backend/runtime.rs`
- Modify: `src/bin/stratum_server.rs`
- Modify: `scripts/check-postgres-migrations.sh`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing adoption tests**

Add tests proving:

- A fully migrated legacy schema with no `stratum_schema_migrations` rows can be adopted into applied control rows without replaying DDL.
- Adoption is explicit; `status` does not silently adopt.
- Adoption dry-run/status reports whether the schema is adoptable without inserting rows.
- Dirty `started` or `failed` control rows refuse adoption.
- Unknown applied versions refuse adoption.
- Name/checksum mismatches refuse adoption.
- Partial schemas refuse adoption and leave no partial metadata.
- Unverifiable schemas refuse adoption and leave no partial metadata.
- Migration SQL and database-controlled migration names are not leaked in errors.
- Startup can run explicit adoption mode and then passes only when all known migrations are verified as already applied.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected before implementation: adoption tests fail because no adoption flow exists.

**Step 2: Add explicit adoption mode**

Add `DurableMigrationMode::Adopt` with env value `adopt`.

Behavior:

- `status`: report current migration state, no adoption.
- `apply`: apply pending migrations only when control table state is safe; do not auto-adopt legacy schemas.
- `adopt`: under the same schema advisory lock, verify a fully migrated legacy schema and insert applied rows with current catalog names/checksums. Refuse if any migration is pending after adoption.

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: migration mode parsing tests pass.

**Step 3: Implement adoption verification**

In `src/backend/postgres_migrations.rs`, add a verifier that checks known durable schema artifacts rather than replaying SQL or parsing SQL strings.

Rules:

- Verify every catalog migration with concrete table, column, constraint, and index probes in `pg_catalog`/`information_schema`.
- Make adoption all-or-nothing in one transaction after taking the advisory lock.
- If `stratum_schema_migrations` contains dirty, unknown, or mismatched rows, return the current fixed refusal errors.
- If a known migration's artifacts are partial or unverifiable, return a fixed redacted `CorruptStore` error.
- Insert applied rows only for the exact current catalog version/name/checksum.
- Do not include migration SQL, raw table data, DB URL, schema, host, password, or secret values in errors.

Suggested minimum artifact probes:

- 0001: core durable tables, protected rules, change requests, approvals, refs CAS columns, idempotency constraints.
- 0002: review local commit id columns or constraints.
- 0003 through 0008: recovery/cleanup tables, enum-like state checks, key indexes.
- 0009: durable auth/session columns including `token_version`.
- 0010: object deletion fence table and key columns.
- 0011: idempotency retention/quota columns.
- 0012: object cleanup deletion-state columns.
- 0013: protected-rule `require_all_files_viewed` columns.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: adoption tests pass or skip cleanly without live Postgres URL.

**Step 4: Wire script and startup docs**

Update `scripts/check-postgres-migrations.sh` so unset URL still skips cleanly. Add documented env knobs for adoption smoke/dry-run without printing connection strings.

Acceptable operator shape:

```bash
STRATUM_POSTGRES_MIGRATIONS_ADOPT_DRY_RUN=1 ./scripts/check-postgres-migrations.sh
STRATUM_DURABLE_MIGRATION_MODE=adopt stratum-server
```

Docs must cover:

- When to use status, apply, and adopt.
- Adoption prerequisites.
- Failure modes for dirty, unknown, checksum-mismatched, partial, and unverifiable schemas.
- Live-gate wording: credentials provisioned; awaiting first scheduled provider-verified green.

Run:

```bash
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
bash -n scripts/check-postgres-migrations.sh
```

Expected: unset URL skips cleanly; shell syntax passes.

**Step 5: Commit**

```bash
git add src/backend/postgres_migrations.rs src/backend/runtime.rs src/bin/stratum_server.rs scripts/check-postgres-migrations.sh docs/http-api-guide.md docs/project-status.md tests/server_startup.rs
git commit -m "feat: adopt verified postgres migrations"
```

## Task 5: Reviews, Fixes, And Verification

**Files:**
- Any files touched by Tasks 2-4.

**Step 1: Run spec/correctness review**

Ask a fresh reviewer to compare the full branch against this plan and the user acceptance criteria. Require special attention to:

- Real pool reuse proof.
- Secret seam usage.
- Redaction of URL, host, password, endpoint, SQL, migration SQL, and secret values.
- Adoption refusal modes and all-or-nothing metadata.
- Protected rule flag visibility without enforcement.
- Live-gate wording.

Fix all Critical/Important findings and re-review if fixes are non-trivial.

**Step 2: Run code-quality/security review**

Ask a fresh reviewer to inspect the full diff for maintainability, async/pool correctness, race conditions, schema adoption safety, SQL/catalog probe robustness, route compatibility, SDK fixture drift, and secret handling.

Fix all Critical/Important findings and re-review if fixes are non-trivial.

**Step 3: Run required verification gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
bash -n scripts/check-postgres-migrations.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Run SDK gates after the protected-rule flag:

```bash
cd sdk
bun run typecheck
bun run test:run
cd python
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
```

Live Postgres/R2 gates:

- Do not block this slice on manual live verification.
- Record local status as: credentials provisioned; awaiting first scheduled provider-verified green.
- If local credentials happen to be present, run the live gates and record the result; otherwise leave them to scheduled CI.

**Step 4: Final commit if review fixes changed files**

```bash
git add <review-fix-files>
git commit -m "fix: tighten postgres pool adoption slice"
```

**Step 5: Push and merge**

After all verification gates pass:

```bash
git status --short --branch
git push origin v2/foundation
git -C /Users/rajattiwari/virtualfilesystem/lattice status --short --branch
```

Then merge `v2/foundation` to `main` without touching unrelated main worktree changes. If main remains dirty with unrelated files, use a non-destructive merge workflow that preserves them. Push `main` after the merge.
