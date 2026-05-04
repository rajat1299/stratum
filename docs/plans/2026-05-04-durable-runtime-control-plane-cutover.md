# Durable Runtime Control-Plane Cutover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Let `STRATUM_BACKEND=durable` start `stratum-server` with Postgres-backed workspace, idempotency, audit, and review stores while keeping the core filesystem/VCS state explicitly local for this phase.

**Architecture:** This is a guarded control-plane cutover, not the full durable filesystem/VCS cutover. The server will keep `StratumDb` on the existing local snapshot backend for file contents, inode state, VCS object bytes, commit vectors, and refs, but route workspace metadata, idempotency records, audit events, and review state through `PostgresMetadataStore` in durable mode after migration preflight. Default `STRATUM_BACKEND=local` behavior remains unchanged.

**Tech Stack:** Rust 2024, Tokio, Axum, optional `postgres` feature with `tokio-postgres`, existing `PostgresMetadataStore`, existing local control-plane stores, process-level `stratum-server` tests, live Postgres tests via `STRATUM_POSTGRES_TEST_URL`.

---

## Scope Boundaries

In scope:

- Add a `ServerStores` bundle for workspace, idempotency, audit, and review stores.
- Preserve `server::build_router(db)` as the local/default compatibility path.
- Add a runtime-aware server store factory that opens local stores in local mode and a shared `PostgresMetadataStore` in durable mode.
- Change durable `stratum-server` support from fail-closed to supported only when built with `--features postgres`.
- Keep migration status/apply preflight before serving durable mode.
- Keep all local filesystem/VCS state in `StratumDb` and `.vfs/state.bin` for this phase.
- Add process tests proving durable mode starts, writes control-plane rows to Postgres, and does not create local control-plane store files.
- Update docs/project-status so nobody mistakes this for full durable FS/VCS cutover.

Out of scope:

- No full `StratumDb` cutover to Postgres metadata or R2 object bytes.
- No durable inode/worktree persistence model.
- No VCS core ref/object/commit routing through `ObjectStore`, `CommitStore`, or `RefStore`.
- No R2 object-byte runtime routing in HTTP/MCP/CLI/FUSE request paths.
- No connection pool, TLS/KMS/secrets manager integration, distributed locks, or cross-store transaction boundary.
- No background cleanup/repair worker or final-object deletion mode.

## Critical Safety Rules

- Never log `STRATUM_POSTGRES_URL`, `PGPASSWORD`, `STRATUM_R2_ACCESS_KEY_ID`, or `STRATUM_R2_SECRET_ACCESS_KEY`.
- Do not construct durable stores before migration preflight succeeds.
- Durable mode without the `postgres` feature must still fail before local store files are opened.
- Durable startup failures must not create `.vfs/workspaces.bin`, `.vfs/idempotency.bin`, `.vfs/audit.bin`, or `.vfs/review.bin`.
- `StratumDb::open(config)` remains the local core-state boundary in this slice; docs and logs must say that plainly.

## Task 1: Server Store Bundle

**Files:**

- Modify: `src/server/mod.rs`

**Steps:**

1. Add a `ServerStores` struct beside `ServerState`:

   ```rust
   #[derive(Clone)]
   pub struct ServerStores {
       pub workspaces: SharedWorkspaceMetadataStore,
       pub idempotency: SharedIdempotencyStore,
       pub audit: SharedAuditStore,
       pub review: SharedReviewStore,
   }
   ```

2. Add a local constructor:

   ```rust
   impl ServerStores {
       pub fn open_local(config: &crate::config::Config) -> Result<Self, VfsError> {
           let workspace_store = LocalWorkspaceMetadataStore::open(config.workspace_metadata_path())?;
           let idempotency_store = LocalIdempotencyStore::open(config.idempotency_path())?;
           let audit_store = LocalAuditStore::open(config.audit_path())?;
           let review_store = LocalReviewStore::open(config.review_path())?;

           Ok(Self {
               workspaces: Arc::new(workspace_store),
               idempotency: Arc::new(idempotency_store),
               audit: Arc::new(audit_store),
               review: Arc::new(review_store),
           })
       }
   }
   ```

3. Keep `build_router(db: StratumDb) -> Result<Router, VfsError>` as the unchanged local default by delegating to `ServerStores::open_local(db.config())`.

4. Add:

   ```rust
   pub fn build_router_with_server_stores(db: StratumDb, stores: ServerStores) -> Router {
       build_router_with_stores(
           db,
           stores.workspaces,
           stores.idempotency,
           stores.audit,
           stores.review,
       )
   }
   ```

5. Do not remove `build_router_with_workspace_store` or `build_router_with_stores`; existing unit tests rely on them.

6. Run:

   ```bash
   cargo fmt --all -- --check
   cargo test --locked server:: --lib -- --nocapture
   cargo test --locked --test server_startup -- --nocapture
   git diff --check
   ```

7. Commit:

   ```bash
   git add src/server/mod.rs
   git commit -m "refactor: add server store bundle"
   ```

## Task 2: Durable Store Factory

**Files:**

- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`

**Steps:**

1. In `src/backend/runtime.rs`, refactor duplicate Postgres URL parsing into a single feature-gated helper on `DurableBackendRuntimeConfig`:

   ```rust
   #[cfg(feature = "postgres")]
   pub(crate) fn postgres_config_with_env_password(
       &self,
   ) -> Result<tokio_postgres::Config, VfsError> {
       let mut config = self
           .postgres_url
           .parse::<tokio_postgres::Config>()
           .map_err(|_| VfsError::InvalidArgs {
               message: format!(
                   "invalid {POSTGRES_URL_ENV}; expected a Postgres connection string without an embedded password"
               ),
           })?;

       if config.get_password().is_some() {
           return Err(VfsError::InvalidArgs {
               message: format!(
                   "{POSTGRES_URL_ENV} must not include a password; use PGPASSWORD or the deployment secret manager"
               ),
           });
       }

       if let Ok(password) = std::env::var("PGPASSWORD")
           && !password.is_empty()
       {
           config.password(password);
       }

       Ok(config)
   }
   ```

2. Update durable migration preflight to call the helper instead of parsing `self.postgres_url` inline.

3. Change `BackendRuntimeConfig::ensure_supported_for_server()`:

   - `Local` returns `Ok(())`.
   - `Durable` with `postgres` feature returns `Ok(())`.
   - `Durable` without `postgres` returns `NotSupported` with a secret-free message such as:

     ```text
     durable backend runtime requires stratum-server built with the postgres feature
     ```

4. In `src/server/mod.rs`, add an async store factory:

   ```rust
   pub async fn open_server_stores_for_runtime(
       runtime: &crate::backend::runtime::BackendRuntimeConfig,
       config: &crate::config::Config,
   ) -> Result<ServerStores, VfsError> {
       match runtime.mode() {
           crate::backend::runtime::BackendRuntimeMode::Local => ServerStores::open_local(config),
           crate::backend::runtime::BackendRuntimeMode::Durable => open_durable_server_stores(runtime).await,
       }
   }
   ```

5. Implement `open_durable_server_stores` with feature gates:

   - Without `postgres`, return `VfsError::NotSupported`.
   - With `postgres`, get `runtime.durable()` or return `InvalidArgs`.
   - Build `PostgresMetadataStore::with_schema(durable.postgres_config_with_env_password()?, durable.postgres_schema().to_string())?`.
   - Wrap the same cloned `PostgresMetadataStore` into all four trait object fields:

     ```rust
     let store = Arc::new(PostgresMetadataStore::with_schema(config, durable.postgres_schema().to_string())?);
     Ok(ServerStores {
         workspaces: store.clone(),
         idempotency: store.clone(),
         audit: store.clone(),
         review: store,
     })
     ```

6. Do not construct `R2BlobStore` in this task. Durable object bytes remain future work, and requiring an unused live R2 connection would make local Postgres process tests brittle.

7. Add unit tests in `src/backend/runtime.rs`:

   - Existing non-`postgres` durable preflight still returns `NotCheckedPostgresFeatureDisabled`.
   - `ensure_supported_for_server()` for durable without `postgres` returns `NotSupported`.
   - With `postgres`, `postgres_config_with_env_password()` rejects parsed password configs and does not include raw invalid URL/password material in error text.

8. Add focused server unit tests if practical:

   - `ServerStores::open_local` creates the four expected local files only when invoked.
   - `build_router(db)` remains local and compiles without a runtime config.

9. Run:

   ```bash
   cargo fmt --all -- --check
   cargo test --locked backend::runtime --lib -- --nocapture
   cargo test --locked --features postgres backend::runtime --lib -- --nocapture
   cargo check --locked --features postgres
   cargo test --locked --test server_startup -- --nocapture
   git diff --check
   ```

10. Commit:

   ```bash
   git add src/backend/runtime.rs src/server/mod.rs
   git commit -m "feat: add durable server store factory"
   ```

## Task 3: Server Startup Cutover And Process Tests

**Files:**

- Modify: `src/bin/stratum_server.rs`
- Modify: `tests/server_startup.rs`

**Steps:**

1. Update `src/bin/stratum_server.rs` startup order:

   ```rust
   backend_runtime.prepare_server_startup().await?;
   backend_runtime.ensure_supported_for_server()?;
   let db = StratumDb::open(config)?;
   let server_stores = server::open_server_stores_for_runtime(&backend_runtime, db.config()).await?;
   let app = server::build_router_with_server_stores(db.clone(), server_stores);
   ```

   Keep actual code in explicit `match` blocks so startup errors log and exit cleanly instead of panicking.

2. Log only:

   - `backend_mode`
   - `data_dir`
   - a non-secret store mode label such as `control_plane_store = "local"` or `"postgres"`

3. Preserve local default process tests.

4. Update existing feature-gated durable startup tests:

   - `status` mode with pending migrations still fails before local store creation.
   - dirty migration control state still fails before local store creation.
   - `apply` mode no longer fails closed; it starts the server and responds to `/health`.

5. Add a process helper in `tests/server_startup.rs`:

   - Reserve a random `127.0.0.1:0` port.
   - Spawn `env!("CARGO_BIN_EXE_stratum-server")` with `STRATUM_LISTEN`.
   - Poll `/health` with `reqwest::Client` until it succeeds or a timeout expires.
   - Kill and wait for the child in `Drop`.

6. Add a live Postgres process test:

   - Skip unless `STRATUM_POSTGRES_TEST_URL` is set, unless required mode or GitHub Actions is set.
   - Create an isolated schema.
   - Spawn durable server with:

     ```text
     STRATUM_BACKEND=durable
     STRATUM_DURABLE_MIGRATION_MODE=apply
     STRATUM_POSTGRES_SCHEMA=<isolated schema>
     STRATUM_POSTGRES_URL=<test url>
     STRATUM_R2_BUCKET=stratum
     STRATUM_R2_ENDPOINT=https://example.invalid
     STRATUM_R2_ACCESS_KEY_ID=raw-r2-access-key
     STRATUM_R2_SECRET_ACCESS_KEY=raw-r2-secret-key
     ```

   - `POST /workspaces` as `Authorization: User root` with an `Idempotency-Key`.
   - `POST /protected/refs` as `Authorization: User root` with an `Idempotency-Key`.
   - Query Postgres directly and assert:

     - `workspaces` has the new workspace row.
     - `idempotency_records` has records for both idempotent requests.
     - `audit_events` has workspace/review mutation audit events.
     - `protected_ref_rules` has the new protected ref rule.

   - Assert these local files do not exist:

     ```text
     <STRATUM_DATA_DIR>/.vfs/workspaces.bin
     <STRATUM_DATA_DIR>/.vfs/idempotency.bin
     <STRATUM_DATA_DIR>/.vfs/audit.bin
     <STRATUM_DATA_DIR>/.vfs/review.bin
     ```

   - Do not assert `.vfs/state.bin` absence because core FS/VCS remains local and may save during server lifetime or shutdown.

7. Preserve and extend secret leakage assertions:

   - Child output must not contain raw R2 access key.
   - Child output must not contain raw R2 secret key.
   - Child output must not contain `PGPASSWORD` or `STRATUM_POSTGRES_TEST_PASSWORD` values.
   - Child output must not contain password-bearing URL test material.

8. Run:

   ```bash
   cargo fmt --all -- --check
   cargo test --locked --test server_startup -- --nocapture
   STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
   cargo check --locked --features postgres
   git diff --check
   ```

9. Commit:

   ```bash
   git add src/bin/stratum_server.rs tests/server_startup.rs
   git commit -m "feat: wire durable control-plane stores"
   ```

## Task 4: Documentation And Status

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Steps:**

1. In `docs/http-api-guide.md`, update Backend Durability Status:

   - `STRATUM_BACKEND=durable` with `postgres` feature now starts `stratum-server` after migration preflight.
   - Workspace metadata, idempotency, audit, and review state are Postgres-backed in durable mode.
   - Core filesystem/VCS state remains local `.vfs/state.bin`.
   - R2 credentials are still required by durable config but object-byte request handling is not cut over in this slice.
   - Durable mode without `postgres` feature fails before serving.

2. In `docs/project-status.md`:

   - Update latest backend slice.
   - Add `Durable Runtime Control-Plane Cutover` section.
   - Move control-plane Postgres runtime wiring from “not built” to “built”.
   - Keep full FS/VCS durable cutover, R2 object-byte runtime, connection pooling, distributed locks, and cross-store semantics in residual risks/not built.
   - Preserve SDK/DX lane content.

3. Run:

   ```bash
   git diff --check
   ```

4. Commit:

   ```bash
   git add docs/http-api-guide.md docs/project-status.md
   git commit -m "docs: document durable control-plane runtime"
   ```

## Task 5: Subagent Reviews And Main-Session Fixes

**Files:**

- No assigned write set initially; review only.

**Steps:**

1. Dispatch a spec-compliance reviewer for the whole branch. Ask them to verify:

   - Durable mode starts only with `postgres` feature.
   - Durable migration preflight still blocks pending/dirty states.
   - Durable mode uses Postgres for workspace/idempotency/audit/review.
   - Local default behavior remains unchanged.
   - Docs do not overstate full durability.

2. Dispatch a code-quality/security reviewer for the whole branch. Ask them to focus on:

   - Secret leakage.
   - Startup ordering.
   - Feature-gate correctness.
   - Process test reliability.
   - Trait-object clone/lifetime correctness.
   - Accidental local control-plane file creation.

3. Main session must inspect each finding locally, fix accepted findings, rerun focused tests, and commit fixes with a small message such as:

   ```bash
   git commit -m "fix: harden durable control-plane runtime"
   ```

4. Do not trust subagent output without local review.

## Full Verification

Run from `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation`:

```bash
cargo fmt --all -- --check
cargo check --locked --features postgres
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
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

## Expected End State

- `STRATUM_BACKEND=local`: same local runtime behavior and local control-plane files as before.
- `STRATUM_BACKEND=durable`, no `postgres` feature: fails before serving and before local control-plane stores are opened.
- `STRATUM_BACKEND=durable`, `postgres` feature, `STRATUM_DURABLE_MIGRATION_MODE=status`: starts only if migrations are already applied.
- `STRATUM_BACKEND=durable`, `postgres` feature, `STRATUM_DURABLE_MIGRATION_MODE=apply`: applies migrations, starts the HTTP server, and stores workspace/idempotency/audit/review rows in Postgres.
- Core filesystem/VCS remains local and documented as the next major durable runtime boundary.
