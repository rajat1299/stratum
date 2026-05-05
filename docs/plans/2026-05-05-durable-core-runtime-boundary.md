# Durable Core Filesystem/VCS Runtime Boundary

Date: 2026-05-05
Branch: v2/foundation
Execution: Codex-driven with local subagents

## Goal

Make the core filesystem/VCS runtime boundary explicit before routing live HTTP filesystem or VCS writes to Postgres object/commit/ref metadata and S3/R2 object bytes.

The previous durable runtime cutover moved workspace metadata, idempotency, audit, and review state into Postgres for `STRATUM_BACKEND=durable`. It did not move core filesystem state, VCS objects, commit vectors, refs, or object bytes off the local `StratumDb` snapshot at `.vfs/state.bin`.

This slice establishes the seam and fail-closed behavior needed for the later core cutover.

## Current Boundary

- `stratum-server` opens `StratumDb::open(config)` in every supported runtime mode.
- `ServerState` owns `db: Arc<StratumDb>` plus control-plane stores.
- HTTP filesystem routes call `state.db.*` directly for reads and writes.
- HTTP VCS routes call `state.db.*` directly for commit, revert, refs, log, status, and diff.
- `StratumDb` owns one `RwLock` around `VirtualFs` and `Vcs`.
- `Vcs` owns an in-memory `BlobStore`, local commit vector, local head, and local refs.
- `PersistManager` serializes full `VirtualFs` plus `Vcs` into `<STRATUM_DATA_DIR>/.vfs/state.bin`.

## Durable Foundations Already Present

- Backend contracts exist for repo-scoped object, commit, and ref stores.
- Postgres adapters exist for object metadata, commit metadata, ref CAS, cleanup claims, idempotency, audit, workspace metadata, and review state.
- `BlobObjectStore` can bridge object metadata plus a byte store with staged uploads and conditional final writes.
- `R2BlobStore` exists behind the remote byte-store abstraction.
- Migration smoke and live Postgres contracts cover the current schema and adapters.

## Non-Goals

- Do not route HTTP filesystem writes to R2 in this slice.
- Do not route `Vcs::commit`, ref updates, or revert to Postgres in this slice.
- Do not add distributed locking, a connection pool, hosted Postgres TLS, or cross-store transactions in this slice.
- Do not delete final immutable objects.
- Do not rewrite `StratumDb` or `Vcs` internals yet.

## Required Runtime Semantics

1. The server must expose which core runtime it is using.
2. The current supported core runtime is `local-state`: `StratumDb` backed by `.vfs/state.bin`.
3. A requested durable core runtime must fail closed before opening local state or serving requests.
4. The fail-closed path must not require Postgres migrations or R2 calls to run first.
5. Error messages must not echo Postgres URLs, R2 credentials, or raw secret-bearing env values.
6. The current durable control-plane mode may continue to use local core state, but that split must be explicit in config, logs, docs, and tests.

## First Implementation Slice

Add a non-invasive core runtime selection seam:

- Add `STRATUM_CORE_RUNTIME`.
- Parse `local-state` as the only supported core runtime.
- Parse durable core spellings such as `durable`, `durable-cloud`, or `postgres-r2` as a known but unsupported mode.
- Reject unknown values without echoing the raw value.
- Include core runtime mode in `BackendRuntimeConfig::Debug`.
- Add `BackendRuntimeConfig::core_runtime_mode()` and keep it independent from `STRATUM_BACKEND`.
- Move server startup to call a runtime-aware core open helper instead of calling `StratumDb::open(config)` directly.
- Ensure unsupported durable core mode fails before migration preflight and before local `.vfs` state is created.
- Log `core_runtime_store=local-state` on startup.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

This does not change normal request behavior.

## Follow-Up Slices

1. Add a route-facing `CoreRuntime` or `CoreDb` boundary around the subset of `StratumDb` used by HTTP filesystem/VCS routes.
2. Move filesystem/VCS route calls through the boundary without behavior change.
3. Design the durable commit transaction: object writes, object metadata writes, commit metadata insert, source-checked ref CAS, idempotency completion, audit append, and workspace head update.
4. Add an object writer fencing model before final-object deletion.
5. Add hosted Postgres connector posture: pooling, TLS, secret manager/KMS integration, and safe diagnostics.
6. Only then cut over one narrow core request path behind an explicit opt-in.

## Verification

Run after every diff in this slice:

```bash
cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Run before each code commit:

```bash
cargo fmt --all -- --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Run before final integration:

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
cargo test --locked --release --test perf -- --test-threads=1 --nocapture
cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
git diff --check
```
