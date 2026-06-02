# Execution Phase 2 Runner Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a disabled-by-default, process-local execution runner that creates durable `/runs/<run-id>/` artifacts and supports workspace-scoped run, list, wait, and cancel behavior without command/output leakage in idempotency or audit metadata.

**Architecture:** Extend the existing Phase 1 run-record model with a provider-free in-memory job table inspired by Mirage `JobTable`, then expose narrow `/execute` routes that require mounted workspace bearer auth. Runtime config stays fail-closed by default; enabling process-local execution requires an explicit dev gate, bounded timeout/output caps, and a local temporary working directory rather than a production sandbox or workspace hydrator.

**Tech Stack:** Rust, Tokio, Axum, serde, chrono, uuid, existing `Session`, `StratumDb`, workspace bearer auth, audit store, and idempotency helpers.

---

## Current Inventory

- Run layout and status already live in `src/runs.rs`: `/runs`, `prompt.md`, `command.md`, `stdout.md`, `stderr.md`, `result.md`, `metadata.md`, `artifacts/`, `RunStatus`, `RunRecordInput`, `RunRecordContext`, `RunRecordLayout`, and `RunRecord::new`.
- Existing run routes in `src/server/routes_runs.rs` already implement mounted workspace auth, mounted path projection, run write/read scope checks, idempotent `POST /runs`, redacted run-create audit details, stdout/stderr read APIs, and no backing path leakage in success or replay responses.
- Local full router merges `/runs` in `src/server/mod.rs`; durable-cloud intentionally returns unsupported for `/runs` and must also return unsupported for `/execute`.
- Mirage `JobTable` uses process-local job entries, cooperative cancellation through an abort signal, terminal wait notification, per-workspace list filtering, and foreground/background result handling. Stratum should adapt the lifecycle and workspace isolation, not Mirage's command echoing in public job summaries.
- Existing capabilities currently report `runs.execution = false` with "no execution scheduler yet" in `src/server/routes_capabilities.rs`.

## Route Shape

- `POST /execute`: submit a command for the mounted workspace.
- `GET /execute/jobs`: list jobs for the mounted workspace only.
- `GET /execute/jobs/{job_id}`: read one workspace-owned job summary.
- `POST /execute/jobs/{job_id}/wait`: wait for one workspace-owned job to reach a terminal state or for an optional wait timeout.
- `POST /execute/jobs/{job_id}/cancel`: request cooperative cancellation for one workspace-owned job.

Public job summaries include only `job_id`, `workspace_id`, `run_id`, `status`, projected run paths, timestamps, exit code, and output truncation booleans. They never include raw command, stdout, stderr, prompt, environment, backing workspace paths, temp directory paths, provider errors, tokens, or idempotency keys.

`Idempotency-Key` is not supported on execution routes in this slice. Requests carrying it must fail before reserving a key or starting a job.

## Runtime Gate

Add execution runtime parsing to `src/backend/runtime.rs`.

Environment:
- `STRATUM_EXECUTION_RUNNER`: `disabled` by default; `process-local` enables the provider-free runner mode.
- `STRATUM_EXECUTION_ENABLE_DEV`: must be exactly `1` when `STRATUM_EXECUTION_RUNNER=process-local`.
- `STRATUM_EXECUTION_TIMEOUT_MS`: optional, default `30000`, max `300000`.
- `STRATUM_EXECUTION_OUTPUT_MAX_BYTES`: optional, default `65536`, max `1048576`.
- `STRATUM_EXECUTION_MAX_JOBS`: optional, default `256`, max `10000`.

Partial config, unknown provider values, invalid dev gates, and invalid numeric values fail closed with env-name-only error messages. No local `.vfs` control-plane or core files should be opened when execution config parsing fails during `stratum-server` startup.

## Task 1: Runtime Config And Router Wiring

**Files:**
- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_capabilities.rs`
- Modify: `src/bin/stratum_server.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing runtime config tests**

Add tests in `src/backend/runtime.rs`:
- default config has execution disabled.
- `STRATUM_EXECUTION_RUNNER=process-local` without `STRATUM_EXECUTION_ENABLE_DEV=1` fails and mentions only the required env name.
- process-local mode parses timeout/output/max-jobs.
- partial and invalid config values fail without leaking raw values.
- `Debug` output shows only mode/enabled/limits, not raw rejected values.

Run:

```bash
cargo test --locked backend::runtime::tests::execution_runner --lib -- --nocapture
```

Expected: FAIL because execution runtime config does not exist.

**Step 2: Implement runtime config**

Add `ExecutionRunnerRuntimeConfig`, `ExecutionRunnerMode`, constants for the five env names, parsing helpers, accessors, and `Debug`.

Thread the config into `BackendRuntimeConfig::from_lookup`, `fmt::Debug`, and an accessor:

```rust
pub fn execution_runner(&self) -> &ExecutionRunnerRuntimeConfig
```

**Step 3: Verify runtime config passes**

Run:

```bash
cargo test --locked backend::runtime::tests::execution_runner --lib -- --nocapture
```

Expected: PASS.

**Step 4: Write failing startup/router/capability tests**

Add tests that:
- default local router reports execution disabled in capabilities.
- local router can be built with enabled process-local execution config.
- durable-cloud unsupported route set includes `/execute` and `/execute/{*path}`.
- `stratum-server` exits on invalid execution env before creating local `.vfs` files.

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --test server_startup execution -- --nocapture
```

Expected: FAIL because router state/capabilities/startup do not know execution runtime yet.

**Step 5: Wire router state**

Add a default-disabled execution field to `ServerState`, keep existing test helper router builders default-disabled, and add one explicit router builder path used by `stratum-server` to pass `backend_runtime.execution_runner().clone()`.

Capabilities should advertise `/execute` as unavailable by default, with `requires: ["STRATUM_EXECUTION_RUNNER=process-local", "STRATUM_EXECUTION_ENABLE_DEV=1"]` or equivalent concise strings. When enabled locally, capabilities may report execution available. Durable-cloud capabilities remain unavailable and unsupported.

**Step 6: Verify wiring passes**

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --test server_startup execution -- --nocapture
```

Expected: PASS.

**Step 7: Commit**

```bash
git add src/backend/runtime.rs src/server/mod.rs src/server/routes_capabilities.rs src/bin/stratum_server.rs tests/server_startup.rs
git commit -m "feat: gate process-local execution runner"
```

## Task 2: Process-Local Job Table

**Files:**
- Add: `src/execution.rs`
- Modify: `src/lib.rs`

**Step 1: Write failing job-table tests**

Add focused tests in `src/execution.rs` for:
- `submit` creates a queued/running job and eventually reaches `succeeded` with bounded stdout/stderr.
- failed commands reach `failed` and preserve bounded output.
- output above the cap is truncated while the child process can still exit.
- timeout kills the child and reaches `timed_out`.
- cancellation is cooperative, terminal, and idempotent.
- `list`, `get`, `wait`, and `cancel` enforce workspace isolation.
- debug output and public summaries omit raw command, output, environment, temp paths, and tokens.

Run:

```bash
cargo test --locked execution::tests --lib -- --nocapture
```

Expected: FAIL because `src/execution.rs` does not exist.

**Step 2: Implement job table**

Implement:
- `ExecutionJobTable`
- `ExecutionJobSummary`
- `ExecutionJobStatus`
- `ExecutionSubmitRequest`
- `ExecutionWaitRequest`
- cancellation token based on `AtomicBool` plus `tokio::sync::Notify`
- capped stdout/stderr readers that keep draining after the public cap
- temporary local working directory creation and best-effort cleanup
- command execution through `/bin/sh -c` on Unix

The runner writes no logs containing raw command or output. Public errors use status/failure kind only.

Use `RunStatus` values for durable artifact status mapping:
- submitted: `queued`
- process start: `running`
- zero exit: `succeeded`
- nonzero exit or spawn/read failure: `failed`
- cancel: `cancelled`
- timeout: `timed_out`

**Step 3: Verify job table passes**

Run:

```bash
cargo test --locked execution::tests --lib -- --nocapture
```

Expected: PASS.

**Step 4: Commit**

```bash
git add src/execution.rs src/lib.rs
git commit -m "feat: add process-local execution job table"
```

## Task 3: Execute Routes And Durable Run Artifacts

**Files:**
- Add: `src/server/routes_execute.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/audit.rs`
- Modify: `src/server/routes_capabilities.rs` if Task 1 left final capability fields incomplete

**Step 1: Write failing route tests**

Add tests in `src/server/routes_execute.rs` for:
- default-disabled `/execute` rejects without creating `/runs`.
- enabled `POST /execute` requires mounted workspace bearer auth and write scope for `/runs`.
- enabled submit returns metadata-only job summary and creates a durable run record.
- successful jobs write `command.md`, `stdout.md`, `stderr.md`, `result.md`, and final `metadata.md`.
- failed jobs preserve bounded stdout/stderr/result and final status.
- wait returns terminal status or still-running status after a wait timeout.
- cancel is workspace-scoped and writes `cancelled` final metadata.
- list/get/wait/cancel cannot see or mutate jobs from another workspace.
- `Idempotency-Key` is rejected before job creation.
- audit create/start/finish/cancel/failure details omit command, stdout, stderr, env, tokens, local backing paths, temp paths, and secret-like material.
- public route errors omit command, output, temp paths, and backing workspace paths.

Run:

```bash
cargo test --locked server::routes_execute --lib -- --nocapture
```

Expected: FAIL because routes do not exist.

**Step 2: Implement route module**

Route handlers should:
- use the same mounted session pattern as `routes_runs.rs`.
- reject unmounted/global auth and malformed workspace headers.
- reject `Idempotency-Key`.
- preflight write scope to `/runs` and full generated run layout before spawning a job.
- create the run record with `queued` metadata before spawning.
- update metadata to `running` on process start.
- write bounded stdout/stderr/result/final metadata on terminal state.
- use projected workspace paths in responses.
- avoid returning raw command/output in job summaries or errors.

The route implementation may reuse helpers from `routes_runs.rs` by moving small path projection/error helpers to shared module functions if that reduces duplication without a broad refactor.

**Step 3: Add audit actions**

Add `AuditAction` variants for:
- `RunExecuteCreate`
- `RunExecuteStart`
- `RunExecuteFinish`
- `RunExecuteCancel`
- `RunExecuteFailure`

Map them to `AuditExportClass::Run`. Add serde round-trip tests and export redaction coverage.

Audit details should include only:
- `workspace_id`
- `job_id`
- `run_id`
- `status`
- `root`
- `artifacts`
- `exit_code` when present
- `timeout_ms` when relevant
- `stdout_truncated` / `stderr_truncated`

**Step 4: Verify route tests pass**

Run:

```bash
cargo test --locked server::routes_execute --lib -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/server/routes_execute.rs src/server/mod.rs src/audit.rs src/server/routes_capabilities.rs
git commit -m "feat: execute workspace jobs over http"
```

## Task 4: Documentation And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/execution-roadmap.md`
- Modify: `docs/project-status.md`

**Step 1: Write docs update**

Document:
- runtime gate env names and disabled-by-default behavior.
- route request/response shapes.
- no idempotency support on execution routes.
- command/output redaction boundary.
- temp-dir provider-free sandbox limitation.
- timeout/output caps.
- per-workspace isolation.
- durable-cloud unsupported boundary.
- rollback boundary.
- out-of-scope production sandbox, distributed scheduling, broad network policy, package installation, SDK releases, hosted UI, semantic search, and real event-bus broker adapters.

**Step 2: Verify docs references**

Run:

```bash
rg -n "Execution Phase 2|/execute|STRATUM_EXECUTION|Idempotency-Key|durable-cloud" docs/http-api-guide.md docs/execution-roadmap.md docs/project-status.md
git diff --check docs/http-api-guide.md docs/execution-roadmap.md docs/project-status.md
```

Expected: docs mention the new route and no whitespace errors.

**Step 3: Commit**

```bash
git add docs/http-api-guide.md docs/execution-roadmap.md docs/project-status.md
git commit -m "docs: record execution runner foundation"
```

## Task 5: Review Fixes And Verification

**Files:**
- Modify only files required by reviewer findings.

**Step 1: Spec/correctness review**

Dispatch a reviewer to check implementation against this plan and the acceptance criteria:
- disabled-by-default route gate.
- workspace-scoped run/list/wait/cancel.
- durable run artifact writes.
- timeout/cancel/failure semantics.
- audit/idempotency/public-error redaction.
- durable-cloud unsupported boundary.
- existing `/runs` behavior preserved.

Fix findings and re-run focused tests.

**Step 2: Code-quality/security review**

Dispatch a reviewer for:
- process handling and cancellation races.
- output cap drain behavior.
- temp directory cleanup.
- raw command/output leakage in `Debug`, public errors, audit, logs, idempotency.
- path projection and scope checks.
- lock ordering and async deadlocks.

Fix findings and re-run focused tests.

**Step 3: Required verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked execution::tests --lib -- --nocapture
cargo test --locked server::routes_execute --lib -- --nocapture
cargo test --locked runs --lib -- --nocapture
cargo test --locked server::routes_runs --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

**Step 4: Commit review fixes**

```bash
git add <changed-files>
git commit -m "fix: harden execution runner foundation"
```

## Rollback Boundary

Set `STRATUM_EXECUTION_RUNNER=disabled` or unset all `STRATUM_EXECUTION_*` env vars. Existing `/runs` create/read APIs remain durable read/write artifacts only, and durable-cloud continues to reject execution routes.
