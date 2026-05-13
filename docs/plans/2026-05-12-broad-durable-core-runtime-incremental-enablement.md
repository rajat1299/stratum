# Broad Durable Core Runtime Incremental Enablement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable a narrow `STRATUM_CORE_RUNTIME=durable-cloud` HTTP server path only behind explicit dev/test readiness gates, without opening local `.vfs/state.bin` and without enabling unsupported durable mutations.

**Architecture:** Keep local runtime behavior unchanged while adding a separate durable server open path that constructs `DurableCoreRuntime` directly from durable `StratumStores`. Durable-cloud startup requires explicit backend, auth/session, policy, repo-routing, recovery, and repo-id gates before Postgres/R2 store construction and before router serving. The first durable router exposes only committed FS reads/search/tree and VCS read/status/diff/log/ref-list surfaces; all mutable or unsupported surfaces fail closed with stable redacted `NotSupported` responses.

**Tech Stack:** Rust, Axum, Tokio, existing `CoreDb` seam, durable backend store traits, Postgres metadata adapter behind the `postgres` feature, R2/S3-compatible object byte store, existing workspace-token/auth/session, policy, repo-context, audit/idempotency/recovery stores.

---

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-12-tenant-repo-routing-foundation.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- Reconnaissance on `src/backend/runtime.rs`, `src/server/mod.rs`, `src/server/core.rs`, `src/server/middleware.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/server/routes_review.rs`, `src/server/routes_workspace.rs`, `src/bin/stratum_server.rs`, and `tests/server_startup.rs`.

## SMFS And Mirage Pattern Decisions

Useful patterns:

- Mirage's capability-gated surface vocabulary is useful for this slice: durable-cloud should expose a small live capability set and make disabled surfaces explicit instead of accidentally serving broad route methods.
- SMFS/Mirage recovery-status vocabulary is useful only as a status/readiness pattern: pending/active/backing-off/poisoned/completed states map well to Stratum's existing recovery ledgers and scheduler visibility.
- SMFS/Mirage test inventories are useful for route/capability conformance tests: verify omitted or unsupported methods fail predictably, not just that happy-path reads work.

Not applicable here:

- Do not copy SMFS latest-wins sync semantics into Stratum commit/ref paths. Stratum must remain commit/ref/CAS/recovery-identity based.
- Do not use SMFS dirty cache or Mirage flush-on-close semantics for authoritative durable writes. This slice does not enable broad durable writes.
- Do not copy Mirage process-local job tables for recovery. Stratum recovery visibility must remain durable and multi-process safe.
- Do not copy Mirage's full VFP method list. The initial durable-cloud router should be smaller than the local API.
- FUSE/NFS mount adapter and sparse cache patterns are future work; they are not needed for this HTTP startup slice.

## Current Baseline

- `CoreRuntimeMode::DurableCloud` is parsed in `src/backend/runtime.rs`, but `BackendRuntimeConfig::from_lookup` currently returns early and `ensure_core_runtime_supported_for_server()` rejects it before backend env validation, migration preflight, local state open, or serving.
- Durable backend store open already exists through `open_durable_server_stores()`: Postgres metadata store, `ensure_control_plane_ready()`, R2 byte store, `BlobObjectStore`, and full `StratumStores`.
- `DurableCoreRuntime` can already read committed FS state and VCS metadata from durable refs/commits/objects, while auth/session and most mutations return `NotSupported` or require guarded policy-token paths.
- `ServerState` and router construction currently require a local `StratumDb`; `stratum_server` opens local core state before opening server stores.
- Repo context routing is in place. Hosted/durable requests must not silently fall back to `RepoId::local()`.

## Non-Goals

- No production hosted rollout.
- No MCP/FUSE/embedded durable parity.
- No broad durable FS mutation, commit, revert, ref update, review merge, run-record, execution runner, semantic search, final-object deletion, or GC enablement.
- No local singleton `RepoId::local()` fallback for durable-cloud.
- No raw DB URLs, object keys, token material, idempotency keys, request bodies, file contents, approval comments, or review-comment bodies in errors, logs, audit details, or replay records.

## Task 1: Durable-Cloud Startup Gate Contract

**Files:**
- Modify: `src/backend/runtime.rs`
- Test: `src/backend/runtime.rs`

**Step 1: Write failing runtime gate tests**

Add tests covering:

- `STRATUM_CORE_RUNTIME=durable-cloud` without `STRATUM_BACKEND=durable` fails with a redacted durable-core gate error and does not parse durable Postgres/R2 secrets.
- Durable-cloud with `STRATUM_BACKEND=durable` but missing dev/readiness gates fails before durable env validation.
- Durable-cloud with all readiness gates and explicit repo id parses durable backend config.
- Durable-cloud rejects missing, invalid, or local singleton repo id.
- Local-state and guarded durable route parsing are unchanged.

Use these gate env names:

```text
STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV=1
STRATUM_DURABLE_AUTH_SESSION_READY=1
STRATUM_DURABLE_POLICY_READY=1
STRATUM_DURABLE_REPO_ROUTING_READY=1
STRATUM_DURABLE_RECOVERY_READY=1
STRATUM_DURABLE_CORE_REPO_ID=<non-local RepoId>
```

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: new tests fail because durable-cloud still returns early and is always unsupported.

**Step 2: Implement minimal gate parsing**

Implement a small durable-core readiness config in `BackendRuntimeConfig`:

- Parse `STRATUM_CORE_RUNTIME` first.
- For durable-cloud, parse `STRATUM_BACKEND`; require durable backend.
- Before parsing durable Postgres/R2 config, check every readiness gate above.
- Require `STRATUM_DURABLE_CORE_REPO_ID` to parse as `RepoId` and reject `RepoId::local()`.
- Only after all gates pass, parse `DurableBackendRuntimeConfig`.
- Keep `STRATUM_DURABLE_COMMIT_ROUTE=1` accepted only for local-state plus durable backend.

Keep missing-gate errors stable and redacted. They may name env var names, but must not echo env values or secret-bearing durable config.

**Step 3: Add accessors**

Add accessors needed by server startup:

- `durable_core_repo_id(&self) -> Option<&RepoId>`
- `durable_core_runtime_ready(&self) -> bool`

**Step 4: Verify**

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: PASS.

## Task 2: Server State Without Local StratumDb

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_auth.rs`
- Modify if needed: route tests that construct `ServerState`
- Test: `src/server/mod.rs`
- Test: `src/server/routes_auth.rs`

**Step 1: Write failing server-state tests**

Add tests proving:

- A durable-cloud state can be constructed without a local `StratumDb`.
- Durable-cloud `ServerState::requires_explicit_workspace_repo()` is true.
- `/health` on durable-cloud does not touch local state and returns a stable durable runtime health body.

Run:

```bash
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked server::routes_auth --lib -- --nocapture
```

Expected: fail because `ServerState` requires `Arc<StratumDb>` and health reads local counts.

**Step 2: Introduce a local-db handle**

Replace `ServerState.db: Arc<StratumDb>` with a small wrapper such as `ServerLocalDb`:

- `ServerLocalDb::available(Arc<StratumDb>)`
- `ServerLocalDb::unavailable()`
- `is_available()`
- `get() -> Result<&StratumDb, VfsError>`
- `Deref<Target = StratumDb>` may be used to keep local tests compiling, but durable-cloud health and durable router paths must use explicit availability checks where reachable.

Update all `ServerState` struct literals in tests and helpers to wrap the local DB.

**Step 3: Make health durable-safe**

In `routes_auth::health`, return local counts when a local DB is available. When unavailable, return:

```json
{
  "status": "ok",
  "version": "...",
  "core_runtime": "durable-cloud",
  "commits": null,
  "inodes": null,
  "objects": null
}
```

Do not open local state to compute health counts.

**Step 4: Verify**

Run:

```bash
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked server::routes_auth --lib -- --nocapture
```

Expected: PASS.

## Task 3: Durable Core Router Construction

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_auth.rs`
- Test: `src/server/mod.rs`
- Test: `tests/server_startup.rs`

**Step 1: Write failing router/startup tests**

Add tests proving:

- `open_server_stores_for_runtime()` returns durable core stores when durable-cloud gates pass.
- `build_durable_core_router()` constructs `DurableCoreRuntime` directly from `StratumStores` and does not construct `LocalCoreRuntime`.
- Durable-cloud startup with complete gates and complete durable env reaches durable store preflight/opening without creating `.vfs/state.bin`.
- Durable-cloud startup does not log `using local workspace metadata store`.

For process-level startup tests that require live Postgres, gate with existing `STRATUM_POSTGRES_TEST_URL` helpers. Non-Postgres builds should still fail closed without local files.

Run:

```bash
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Expected: fail because durable-cloud still has no router/open path.

**Step 2: Extend `ServerStores`**

Add a separate durable core store field:

- `durable_core_stores: Option<StratumStores>`
- `guarded_durable_commit_stores: Option<StratumStores>` remains the local-state guarded route capability.

When `runtime.core_runtime_mode() == DurableCloud`, open the full durable stores regardless of `STRATUM_DURABLE_COMMIT_ROUTE`. When local-state plus `STRATUM_DURABLE_COMMIT_ROUTE=1`, preserve current guarded behavior.

**Step 3: Add durable router builder**

Add:

- `build_durable_core_router(stores: ServerStores, repo_id: RepoId) -> Router`
- internal durable state construction with `core: Arc::new(DurableCoreRuntime::new(repo_id, stores.durable_core_stores.unwrap()))`
- `db: ServerLocalDb::unavailable()`
- durable workspace/idempotency/audit/review stores from `ServerStores`
- durable recovery scheduler started from the durable core stores.

**Step 4: Restrict route surface**

Durable-cloud router should mount:

- `/health`
- `GET /fs`, `GET /fs/{*path}`
- `GET /search/grep`, `GET /search/find`
- `GET /tree`, `GET /tree/{*path}`
- `GET /vcs/log`
- `GET /vcs/status`
- `GET /vcs/diff`
- `GET /vcs/refs`

Unsupported route groups should return a stable JSON `501`:

```json
{"error":"stratum: operation not supported: durable-cloud route is not supported yet"}
```

At minimum cover `/auth/login`, FS mutation methods, VCS mutation methods, `/workspaces`, `/runs`, `/audit`, `/protected/*`, and `/change-requests*`.

**Step 5: Update `stratum_server` startup**

Branch before local state open:

- Local-state: existing `open_core_db_for_runtime()`, `open_server_stores_for_runtime()`, local router, auto-save, save-on-shutdown.
- Durable-cloud: `prepare_server_startup()`, `open_server_stores_for_runtime()`, `build_durable_core_router()`, no local `StratumDb::open`, no auto-save, no final `db.save()`.

**Step 6: Verify**

Run:

```bash
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Expected: PASS.

## Task 4: Durable Route Behavior And Fail-Closed Mutations

**Files:**
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/middleware.rs`
- Test: `src/server/core.rs`
- Test: `src/server/routes_fs.rs`
- Test: `src/server/routes_vcs.rs`
- Test: `src/server/middleware.rs`

**Step 1: Write failing route tests**

Add tests with in-memory durable stores and durable-cloud state proving:

- Committed durable FS `cat/list/stat/tree/find/grep` work through the durable core route without local DB state.
- Durable VCS `log/status/diff/list refs` work without local DB state.
- `PUT/PATCH/DELETE/POST /fs`, `POST /vcs/commit`, `POST /vcs/revert`, `POST /vcs/refs`, and `PATCH /vcs/refs/{name}` return stable `NotSupported` or durable-cloud unsupported-route JSON and do not mutate durable stores.
- Workspace bearer auth requires explicit repo identity; missing repo, repo mismatch, and nonlocal token without durable principal fail closed.
- No durable-cloud request falls back to `RepoId::local()`.

Run:

```bash
cargo test --locked server::core --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked server::routes_fs::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::middleware::tests --lib -- --nocapture
```

Expected: new tests fail before route restrictions and no-local durable state are implemented.

**Step 2: Keep broad durable mutations closed**

Route-level durable-cloud mutation handlers should fail before policy/idempotency side effects when the method is not in the enabled surface. Do not rely on local state fallback. Keep guarded route capability behavior unchanged for `STRATUM_DURABLE_COMMIT_ROUTE=1` under local-state.

**Step 3: Preserve stable redaction**

Use `VfsError::NotSupported` display shape or the shared durable-cloud unsupported-route body. Do not include raw paths beyond existing bounded mounted-path formatting, file bytes, commit messages, request bodies, token material, object keys, or backend URLs.

**Step 4: Verify**

Run:

```bash
cargo test --locked server::core --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked server::routes_fs::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::middleware::tests --lib -- --nocapture
```

Expected: PASS.

## Task 5: Documentation And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/plans/2026-05-12-broad-durable-core-runtime-incremental-enablement.md`

**Step 1: Update docs**

Record:

- Latest backend slice: Broad Durable Core Runtime Incremental Enablement.
- Durable-cloud startup is available only with explicit dev/test gates and nonlocal repo id.
- Durable-cloud does not create local `.vfs/state.bin`.
- Enabled durable-cloud route surface is read-only committed FS/search/tree and VCS log/status/diff/ref-list.
- Mutations and unsupported control-plane routes remain fail-closed.
- Guarded `STRATUM_DURABLE_COMMIT_ROUTE=1` local-state behavior is preserved.

**Step 2: Verify docs diff**

Run:

```bash
git diff --check
```

Expected: PASS.

## Required Reviews

Spec/correctness review focus:

- Durable-cloud cannot start unless all gates pass.
- Durable-cloud requires `STRATUM_BACKEND=durable` and a nonlocal explicit repo id.
- Durable-cloud startup does not open or create `.vfs/state.bin` or local control-plane files.
- Durable-cloud core is `DurableCoreRuntime`, not `LocalCoreRuntime` with a guarded capability.
- Route surface is restricted to committed FS reads/search/tree and VCS read/status/diff/log/ref-list.
- Unsupported routes and mutations fail closed before durable mutation or local fallback.
- Existing `STRATUM_DURABLE_COMMIT_ROUTE=1` guarded behavior still passes.

Code-quality/security review focus:

- Startup gates are centralized and hard to bypass.
- No accidental `RepoId::local()` fallback in durable-cloud.
- No raw secrets, DB URLs, object keys, token material, request bodies, file content, approval/comment body text, or idempotency keys leak in errors/logs.
- The no-local-DB state cannot be reached by mounted durable routes that still require local state.
- Router restriction is explicit and test-backed.

## Final Verification

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked server::core --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```
