# Hosted Storage Operations Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Stratum's durable storage startup and object operations hosted-ready by requiring bounded Postgres/R2 posture, TLS for remote Postgres, redacted diagnostics, and dependency health output while preserving local and Unix-socket development.

**Architecture:** Keep broad durable-cloud gated/dev-test only, but harden the storage seams it depends on. Runtime parsing owns fail-closed hosted configuration, Postgres owns TLS-capable bounded connections and sanitized SQL diagnostics, the R2 byte store owns bounded timeout/retry behavior and sanitized object-store failures, and `/health` reports only redacted dependency readiness. Durable objects remain immutable and content-addressed; Mirage S3/R2 and SMFS retry patterns are references only.

**Tech Stack:** Rust 2024, Tokio, Axum, `tokio-postgres` behind the `postgres` feature, a TLS connector for hosted Postgres, AWS SDK S3 for R2/S3-compatible bytes, existing durable backend store traits, existing server startup process tests.

---

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- `src/backend/runtime.rs`
- `src/backend/postgres.rs`
- `src/backend/postgres_migrations.rs`
- `src/remote/blob.rs`
- `src/server/mod.rs`
- `src/server/routes_auth.rs`
- `tests/server_startup.rs`
- `scripts/check-postgres-migrations.sh`
- `scripts/check-r2-object-store.sh`
- `docs/http-api-guide.md`

## Current Baseline

- `STRATUM_BACKEND=durable` opens Postgres-backed control-plane stores when the binary is built with `--features postgres`.
- `STRATUM_CORE_RUNTIME=durable-cloud` is dev-gated and remains broad-runtime limited; it already refuses local fallback and local singleton repo identity.
- Postgres URLs with embedded passwords are rejected, and `PGPASSWORD` is used as the secret source.
- Remote Postgres is currently rejected because TLS is not wired. Localhost, loopback hostaddr, and Unix socket targets are accepted with `NoTls`.
- Postgres metadata operations connect per operation through `tokio-postgres` without an explicit pool, acquisition timeout, operation timeout, or TLS connector.
- R2 config redacts credentials and endpoint query/userinfo in `Debug`, but object operations use AWS SDK defaults without explicit Stratum timeout/retry policy.
- R2 operation errors currently convert raw SDK strings into `IoError`; future routes could leak provider/object details if not centralized now.
- `/health` is safe for durable-cloud without a local DB, but it does not yet expose dependency readiness for DB, object store, and recovery stores.

## Extraction Guidance Applied

- Use Mirage's S3/R2 config shape as reference for explicit endpoint/region/prefix/timeout config and redacted operator diagnostics.
- Use SMFS's retry/backoff vocabulary only for bounded operation posture: max attempts, base/max delay, timeout, remaining work, and redacted outcomes.
- Do not copy Mirage mutable resource semantics into Stratum. Durable object bytes stay content-addressed, immutable, and verified by existing object adapter logic.
- Do not copy SMFS latest-wins queue semantics or `.smfs-error.txt` files. Stratum recovery remains keyed by repo/ref/operation/idempotency/commit/object identity, and diagnostics stay redacted.

## Acceptance Checklist

- Hosted-like `sslmode=require` Postgres config parses in feature-gated tests and uses a TLS-capable connector.
- Local TCP loopback and Unix-socket Postgres still work without TLS.
- Remote Postgres without `sslmode=require` fails closed before local state/control-plane files are created.
- Durable-cloud startup fails closed when explicit Postgres pool/timeout and R2 timeout/retry posture is missing.
- R2 config supports bounded timeout and retry knobs, validates them, and redacts secrets/endpoints/object keys in errors.
- `/health` includes redacted dependency readiness for metadata DB, object store, and recovery stores without endpoints, passwords, access keys, object keys, or raw backend errors.
- Default CI still skips live Postgres/R2 checks when URLs/secrets are unset.

## Task 1: Runtime Storage Posture Gates

**Files:**
- Modify: `src/backend/runtime.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Add failing runtime tests**

Add tests under `backend::runtime` for these behaviors:

- Durable backend local-state accepts local Postgres defaults for dev when no hosted-only knobs are set.
- Durable-cloud requires:
  - `STRATUM_POSTGRES_POOL_MAX_SIZE`
  - `STRATUM_POSTGRES_CONNECT_TIMEOUT_MS`
  - `STRATUM_POSTGRES_OPERATION_TIMEOUT_MS`
  - `STRATUM_POSTGRES_POOL_ACQUIRE_TIMEOUT_MS`
  - `STRATUM_R2_REQUEST_TIMEOUT_MS`
  - `STRATUM_R2_CONNECT_TIMEOUT_MS`
  - `STRATUM_R2_MAX_ATTEMPTS`
  - `STRATUM_R2_RETRY_BASE_DELAY_MS`
  - `STRATUM_R2_RETRY_MAX_DELAY_MS`
- Each knob must be positive, bounded, and error by env var name only.
- Remote `postgresql://db.internal/stratum` without `sslmode=require` is rejected without printing `db.internal`.
- Remote `postgresql://db.internal/stratum?sslmode=require` is accepted at config level when the `postgres` feature is enabled.
- Localhost, loopback `hostaddr`, and Unix socket targets remain accepted without TLS.

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::runtime --lib -- --nocapture
```

Expected before implementation: tests fail on missing fields and remote TLS acceptance.

**Step 2: Implement runtime config**

Add bounded config structs:

- `DurablePostgresRuntimePosture`
  - `pool_max_size: usize`
  - `connect_timeout: Duration`
  - `operation_timeout: Duration`
  - `pool_acquire_timeout: Duration`
  - `tls_mode: LocalNoTls | HostedTlsRequired`
- `DurableObjectStoreOperationPosture`
  - `request_timeout: Duration`
  - `connect_timeout: Duration`
  - `max_attempts: u32`
  - `retry_base_delay: Duration`
  - `retry_max_delay: Duration`

Use existing integer parsing style. Suggested upper bounds:

- Postgres pool max: `1..=256`
- Postgres/R2 timeout ms: `1..=300000`
- R2 attempts: `1..=10`

Durable-cloud must require explicit values. Guarded/local durable backend may use conservative defaults so local development remains easy.

**Step 3: Add process startup tests**

Add `tests/server_startup.rs` cases proving durable-cloud complete gates fail before local files when pool/timeout posture is missing or invalid. Reuse `assert_no_secret_leaks`.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Commit:

```bash
git add src/backend/runtime.rs tests/server_startup.rs
git commit -m "feat: gate durable storage posture"
```

## Task 2: Postgres TLS, Pool, And Timeout Posture

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Add failing Postgres tests**

Add feature-gated tests for:

- `sslmode=require` remote config can build a TLS-capable connector without opening a network connection.
- Remote no-TLS configs still reject.
- Localhost and Unix socket configs use the local/no-TLS path.
- Connection debug output and startup errors do not include `STRATUM_POSTGRES_URL`, `PGPASSWORD`, raw host/user/password, or raw SQL text.
- Durable-cloud startup refuses missing pool/timeouts before local files.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected before implementation: TLS connector/pool posture is absent.

**Step 2: Add TLS connector dependency**

Add a TLS connector compatible with `tokio-postgres` behind the `postgres` feature. Keep it feature-scoped.

Expected dependency shape:

```toml
postgres-native-tls = { version = "0.5", optional = true }
native-tls = { version = "0.2", optional = true }

[features]
postgres = ["dep:tokio-postgres", "dep:postgres-native-tls", "dep:native-tls"]
```

Run a non-locked Cargo command once to update `Cargo.lock`, then return to locked commands:

```bash
cargo check --features postgres
```

**Step 3: Implement bounded connector posture**

Add a small Postgres connection layer in `src/backend/postgres.rs`:

- Chooses `NoTls` for local TCP loopback and Unix-socket targets.
- Chooses `postgres_native_tls::MakeTlsConnector` for hosted remote `sslmode=require`.
- Applies `Config::connect_timeout`.
- Applies operation timeout with `SET statement_timeout` after connect and schema selection.
- Maps connection/TLS/timeout failures to fixed redacted messages such as `postgres connect failed` or `postgres operation timed out`, preserving SQLSTATE/constraint-only behavior for DB errors.

If a full reusable connection pool is too invasive for this slice, implement the minimum safe pool limit first: bound concurrent Postgres connections with a semaphore held for the operation, expose the limit in readiness, and leave reusable pooling as a follow-up only if tests prove the API churn is too large. Do not weaken the fail-closed startup gates.

**Step 4: Wire server store opening**

Pass the parsed Postgres posture from `DurableBackendRuntimeConfig` into `PostgresMetadataStore` and `PostgresMigrationRunner`.

Ensure:

- Migration preflight uses the same TLS and connect timeout posture.
- `ensure_control_plane_ready()` is bounded by operation timeout.
- Local/Unix-socket development continues to use the current local path.

**Step 5: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Commit:

```bash
git add Cargo.toml Cargo.lock src/backend/postgres.rs src/backend/postgres_migrations.rs src/backend/runtime.rs src/server/mod.rs tests/server_startup.rs
git commit -m "feat: add hosted postgres connector posture"
```

## Task 3: R2 Timeout, Retry, Readiness, And Redacted Errors

**Files:**
- Modify: `src/remote/blob.rs`
- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `scripts/check-r2-object-store.sh`

**Step 1: Add failing R2 tests**

Add `remote::blob` tests for:

- `R2BlobStoreConfig::Debug` prints timeout/retry numbers but never credentials, endpoint query tokens, or raw object keys.
- Invalid zero/oversized timeout/retry values fail by env var name only.
- Sanitized R2 operation errors do not include bucket, access key, secret key, endpoint query token, or canonical object key.
- Existing local blob store key validation stays unchanged.

Run:

```bash
cargo test --locked remote::blob --lib -- --nocapture
```

Expected before implementation: timeout/retry config and sanitized operation errors are missing.

**Step 2: Implement R2 operation posture**

Extend `R2BlobStoreConfig` with:

- `request_timeout`
- `connect_timeout`
- `max_attempts`
- `retry_base_delay`
- `retry_max_delay`

Construct AWS SDK config with explicit timeout and retry behavior where supported by SDK config APIs. If the SDK does not expose a direct knob for one field, preserve the validated field and enforce request timeout around each operation with `tokio::time::timeout`.

Add a small helper:

```rust
fn sanitized_object_store_error(action: &'static str, error: impl fmt::Display) -> VfsError
```

The returned message must identify only the action (`put`, `get`, `delete`, `list`, `readiness`) and a fixed redacted failure marker. Do not include bucket, endpoint, prefix, canonical key, raw SDK message, or credentials.

**Step 3: Add readiness check**

Add an R2 readiness method that performs a bounded metadata/list probe against the configured prefix without exposing keys. Wire this to durable store startup only when object routing is required by guarded durable route or durable-cloud.

Do not introduce destructive checks or mutable resource semantics.

**Step 4: Update script**

Update `scripts/check-r2-object-store.sh` so default CI still skips cleanly, required mode validates new optional knobs when present, and live mode runs with bounded default timeout/retry env values if unset.

**Step 5: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked remote::blob --lib -- --nocapture
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
```

Commit:

```bash
git add src/remote/blob.rs src/backend/runtime.rs src/server/mod.rs scripts/check-r2-object-store.sh
git commit -m "feat: bound hosted object store operations"
```

## Task 4: Redacted Health Output, Docs, And Verification Notes

**Files:**
- Modify: `src/server/routes_auth.rs`
- Modify: `src/server/mod.rs`
- Modify: `tests/server_startup.rs`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Add failing health tests**

Add tests proving `/health` includes a redacted dependency block:

```json
{
  "dependencies": {
    "metadata_store": {"status": "ready", "kind": "postgres"},
    "object_store": {"status": "ready", "kind": "r2-compatible"},
    "recovery_store": {"status": "ready", "kind": "postgres"}
  }
}
```

For local runtime, report local dependency kinds without exposing local filesystem paths.

Assert no field contains:

- Postgres URL
- `PGPASSWORD`
- R2 access key
- R2 secret key
- endpoint query token
- canonical object key
- raw SQL/backend error

Run:

```bash
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected before implementation: dependency block absent.

**Step 2: Implement health dependency shape**

Keep `/health` unauthenticated and non-invasive. It must report startup-opened dependency availability, not execute unbounded live dependency calls on every request.

Allowed fields:

- static status: `ready`, `not_required`, `unavailable`
- static kind: `local`, `postgres`, `r2-compatible`
- booleans/counts for configured pool/timeouts if needed

Forbidden fields:

- endpoint, URL, host, socket path, username
- credentials/secrets
- object keys/prefixes
- raw DB/S3 error text

**Step 3: Update docs**

Update docs to describe:

- Remote Postgres requires TLS.
- Local TCP loopback and Unix sockets remain supported.
- Passwords stay out of URLs.
- Durable-cloud requires explicit pool/timeouts/R2 retry posture.
- `/health` dependency readiness is redacted.
- R2 live check remains skipped by default without secrets.
- Destructive final-object deletion and production broad durable rollout remain out of scope.

Append a project-status entry for this slice with verification commands and residual risks.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Commit:

```bash
git add src/server/routes_auth.rs src/server/mod.rs tests/server_startup.rs docs/http-api-guide.md docs/project-status.md
git commit -m "docs: record hosted storage hardening"
```

## Task 5: Reviews, Full Gates, Merge, And Push

**Files:**
- Inspect all changed files.

**Step 1: Run spec/correctness review**

Ask a fresh reviewer to compare implementation against this plan and the slice acceptance criteria.

Fix all Critical/Important findings locally. Re-review if fixes are non-trivial.

**Step 2: Run code-quality/security review**

Ask a fresh reviewer to review the full diff for maintainability, race conditions, redaction gaps, TLS/pool/timeout correctness, and object-store semantics.

Fix all Critical/Important findings locally. Re-review if fixes are non-trivial.

**Step 3: Required verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked remote::blob --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 4: Push and merge**

After all gates pass:

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git fetch origin
git checkout main
git merge --ff-only v2/foundation
git push origin main
```

Do not remove or revert unrelated untracked files in the main worktree.
