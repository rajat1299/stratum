# Backend Runtime Selection Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Define and test the server backend runtime-selection contract before any Postgres metadata or S3/R2 object-store cutover.

**Architecture:** Add a small backend runtime config module that validates `local` versus planned `durable` startup prerequisites, rejects unsupported durable server startup explicitly, and keeps the default server runtime local-backed. Keep migration runner work as a documented boundary: migrations are still run through the existing smoke harness, not automatically by `stratum-server`.

**Tech Stack:** Rust 2024, Tokio server startup, existing backend contracts, Postgres libpq connection-string conventions, existing R2/S3-compatible byte-store env names.

---

## Product Decision

This slice creates a controlled runtime-selection contract, not a runtime cutover. Operators and future code should have one place to understand which environment variables are expected for local versus durable mode, and `stratum-server` should fail closed if someone tries to start a durable backend before the code path exists.

Primary docs checked:

- PostgreSQL's official libpq connection documentation defines URI and keyword/value connection strings, including URI userinfo and keyword parameters. Stratum should reject runtime Postgres URLs that embed passwords and require secret material through separate environment/file/service mechanisms instead.

In scope:

- introduce `STRATUM_BACKEND=local|durable` parsing, defaulting to `local`;
- validate planned durable prerequisites without storing or logging raw R2 secrets;
- reject password-bearing `STRATUM_POSTGRES_URL` values;
- reject `STRATUM_R2_ENDPOINT` values with userinfo or secret-bearing query parameters;
- make `stratum-server` log the selected backend mode and reject durable mode with a clear unsupported-runtime error;
- add unit tests for parsing, required durable env vars, password detection, and debug redaction;
- update docs/status.

Out of scope:

- wiring `stratum-server`, MCP, CLI, or FUSE to `PostgresMetadataStore`;
- creating a connection pool;
- running migrations automatically on server startup;
- wiring `BlobObjectStore`/`R2BlobStore` into live request handling;
- storing raw database or object-store secrets in config structs;
- adding distributed locking, object upload staging/cleanup, or cross-store transactions.

## Task 1: Runtime Selection Config

**Files:**
- Create: `src/backend/runtime.rs`
- Modify: `src/backend/mod.rs`

**Requirements:**
- Add `BackendRuntimeMode` with accepted env values:
  - unset or `local` -> local mode;
  - `durable` -> planned durable mode;
  - anything else -> `VfsError::InvalidArgs`.
- Add `BackendRuntimeConfig::from_env()` and testable `from_lookup(...)`.
- Durable mode must require:
  - `STRATUM_POSTGRES_URL`
  - `STRATUM_R2_BUCKET`
  - `STRATUM_R2_ENDPOINT`
  - `STRATUM_R2_ACCESS_KEY_ID`
  - `STRATUM_R2_SECRET_ACCESS_KEY`
- Optional durable fields:
  - `STRATUM_R2_REGION`, default `auto`;
  - `STRATUM_R2_PREFIX`, default `stratum`.
- Reject empty required variables after trimming.
- Reject Postgres runtime URLs that contain passwords in:
  - URI userinfo, for example `postgresql://user:secret@host/db`;
  - query parameters, for example `?password=secret`;
  - keyword/value strings, for example `host=localhost password=secret`.
- Reject `STRATUM_R2_ENDPOINT` values that contain URI userinfo or secret-bearing query parameters, including percent-encoded query keys.
- Do not store raw R2 access key or secret key values in the runtime config; keep only whether each required secret-like variable was configured.
- Implement `Debug` for runtime config so rendered output never includes raw secret values.
- Add `ensure_supported_for_server()`:
  - local mode returns `Ok(())`;
  - durable mode returns `VfsError::NotSupported` explaining that durable server runtime is not wired yet.

**Verification:**

```bash
cargo test --locked backend::runtime -- --nocapture
```

Expected: tests pass and cover defaults, durable validation, password rejection, and redaction.

## Task 2: Server Startup Gate

**Files:**
- Modify: `src/bin/stratum_server.rs`

**Requirements:**
- Parse `BackendRuntimeConfig::from_env()` before opening `StratumDb`.
- On invalid config, log the error and exit non-zero without opening stores.
- Log `backend_mode` for valid config.
- Call `ensure_supported_for_server()` before `StratumDb::open`.
- Default no-env startup must remain unchanged and local-backed.
- Durable mode must fail closed with the unsupported-runtime message until a future slice wires real runtime stores.

**Verification:**

```bash
cargo check --locked --bin stratum-server
cargo test --locked backend::runtime -- --nocapture
```

Expected: server binary compiles, default tests pass.

## Task 3: R2 Config Debug Redaction

**Files:**
- Modify: `src/remote/blob.rs`

**Requirements:**
- Replace derived `Debug` on `R2BlobStoreConfig` with a manual implementation.
- Include non-secret operational fields: bucket, endpoint, region, prefix.
- Redact `access_key_id` and `secret_access_key`.
- Add a focused unit test that proves `format!("{config:?}")` does not contain raw access key or secret key test values.

**Verification:**

```bash
cargo test --locked remote::blob::tests::r2_config_debug_redacts_credentials -- --nocapture
```

Expected: test passes.

## Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Requirements:**
- Re-check `git status` and re-open `docs/project-status.md` immediately before editing because the SDK/DX lane may also update status docs.
- Document `STRATUM_BACKEND=local` as the default.
- Document `STRATUM_BACKEND=durable` as a validated but currently unsupported server mode.
- Document durable prerequisite env vars without showing secrets.
- State that migration execution remains the explicit migration smoke harness and is not automatic on server startup.
- Update status to list backend runtime selection as the latest completed slice after implementation.

**Verification:**

```bash
git diff --check -- docs/http-api-guide.md docs/project-status.md docs/plans/2026-05-02-backend-runtime-selection.md
```

Expected: no whitespace errors.

## Review

Dispatch independent reviewers after implementation:

- Runtime/API reviewer: startup gate semantics, feature/default behavior, and clear unsupported durable cutover boundary.
- Security reviewer: password detection, no secret storage/logging/debug leakage, and docs that do not imply production cloud runtime is enabled.
