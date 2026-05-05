# Durable CoreDb Implementation Path Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a narrow durable `CoreDb` implementation path that composes the backend store contracts and durable transaction semantics while keeping Postgres/R2 filesystem and VCS routing fail-closed.

**Architecture:** Introduce a server-local durable core runtime type alongside `LocalCoreRuntime`. The durable runtime owns the future object, commit, ref, workspace, idempotency, audit, and review store bundle plus the transaction contract, but every route-facing `CoreDb` method returns a stable `NotSupported` error until a real executor is implemented. Startup must continue rejecting `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.

**Tech Stack:** Rust, Axum server core seam, existing backend store traits, existing durable transaction contract, Tokio tests, cargo release perf gates.

---

## Scope

- Add a `DurableCoreRuntime` implementation path behind the existing `CoreDb` trait.
- Compose existing backend store contracts through `StratumStores`; do not add new storage traits unless tests prove a real gap.
- Bind the runtime shape to `DurableCoreStepSemantics::ordered_write_path()`.
- Keep all HTTP filesystem/search/tree/VCS routes fail-closed for durable core runtime.
- Keep startup behavior unchanged: `STRATUM_CORE_RUNTIME=durable-cloud` must still fail before opening `.vfs/state.bin`, durable control-plane stores, or routes.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.
- Preserve current local-state route behavior and performance.

## Non-Goals

- Do not route live HTTP filesystem or VCS requests to Postgres/R2.
- Do not implement a production filesystem executor over object/commit/ref stores.
- Do not build sparse tree/path reconstruction, distributed locks, connection pooling, TLS/KMS/secrets work, or background repair workers.
- Do not create a new route API or change response shapes.
- Do not touch MCP, CLI, FUSE, SDK, review, run-record, or workspace-management call paths beyond docs/status.

## Performance Rules For This Slice

- After each meaningful code or docs diff, run the release perf gate and capture real timing plus process footprint:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

- Record wall/user/sys time and maximum resident set size from `/usr/bin/time -l` when summarizing meaningful changes.
- For any slice that touches `CoreDb` dispatch on the local route path, add or run a focused release measurement for direct `StratumDb` versus `LocalCoreRuntime` binding overhead.
- GPU efficiency is not applicable to this Rust server path unless a later slice adds GPU-backed indexing/search. Do not claim GPU coverage for this slice.
- Keep the durable runtime idle-path footprint small: one shared store bundle and repo id, no local `StratumDb`, no eager object/cache allocation, no string formatting with user request values in the fail-closed path.
- Avoid per-method heap work in fail-closed route methods beyond the existing `async_trait` boxing already required by `CoreDb`.
- Do not clone large byte buffers or collections in tests unless the test is explicitly verifying ownership behavior.

## Task 1: Add Durable CoreDb Red Tests

**Files:**
- Modify: `src/server/core.rs`

**Step 1: Write failing tests**

Add focused tests that require a durable core runtime type which does not exist yet:

- `durable_core_runtime_reports_contract_without_local_state`
  - constructs the durable runtime from `RepoId::local()` and `StratumStores::local_memory()`.
  - asserts the repo id is preserved.
  - asserts the transaction write path equals `DurableCoreStepSemantics::ordered_write_path()`.
  - asserts route execution is disabled.
- `durable_core_runtime_route_methods_fail_closed`
  - calls representative route methods such as `list_refs()` and `stat_as(...)`.
  - asserts `VfsError::NotSupported`.
  - asserts the message names durable core runtime support without echoing request path or raw env values.

**Step 2: Run tests and verify RED**

Run:

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

Expected: compile failure because `DurableCoreRuntime` does not exist yet.

## Task 2: Implement DurableCoreRuntime Fail-Closed Path

**Files:**
- Modify: `src/server/core.rs`

**Step 1: Add the runtime type**

Add a `DurableCoreRuntime` that owns:

- `repo_id: RepoId`
- `stores: StratumStores`

Add narrow inherent methods:

- `new(repo_id: RepoId, stores: StratumStores) -> Self`
- `repo_id(&self) -> &RepoId`
- `transaction_write_path(&self) -> &'static [DurableCoreTransactionStep]`
- `route_execution_enabled(&self) -> bool`

Use the existing `DurableCoreStepSemantics` contract as the only source of write-path ordering.

**Step 2: Implement `CoreDb` for the durable runtime**

Implement every `CoreDb` method for `DurableCoreRuntime` as fail-closed:

- Return `VfsError::NotSupported`.
- Use one shared helper so messages stay stable.
- Do not include usernames, tokens, paths, ref names, request bodies, or raw env values in the error message.
- Do not allocate or touch local `StratumDb`.

**Step 3: Run focused tests and perf**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: durable core tests pass and release perf remains within existing gates.

## Task 3: Preserve Startup Fail-Closed Contract

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `tests/server_startup.rs` only if existing coverage does not prove the contract

**Step 1: Add or tighten tests only if needed**

Verify existing tests still prove:

- `open_core_db_for_runtime()` rejects durable core runtime before creating `.vfs/state.bin`.
- `open_server_stores_for_runtime()` rejects durable core runtime before local control-plane files or durable backend migration preflight.
- process startup with `STRATUM_CORE_RUNTIME=durable-cloud` still fails before local files.

If the existing assertions already cover this, do not add duplicate tests. If a gap exists, add one narrow assertion.

**Step 2: Run focused startup gates**

Run:

```bash
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: startup remains fail-closed and release perf passes.

## Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update backend durability docs**

Document:

- A durable `CoreDb` implementation path now exists as an internal fail-closed runtime shape.
- It composes the backend store contracts and transaction contract.
- It is not a live route executor.
- `STRATUM_CORE_RUNTIME=durable-cloud` still fails closed before serving.

**Step 2: Update living status**

Add a completed slice section for Durable CoreDb Implementation Path:

- What is built: durable runtime type, store bundle ownership, transaction write-path binding, stable fail-closed route method behavior, startup fail-closed preservation.
- What is not built: live Postgres/R2 executor, object-byte routing, sparse tree/path reconstruction, distributed transaction/lock, repair worker, connection pool, hosted TLS/KMS/secrets posture.
- Recommended next slice: executor design for a single durable commit route or repair/fencing prerequisite, depending on review findings.

**Step 3: Run docs perf gate**

Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

Expected: release perf and whitespace checks pass.

## Task 5: Review, Verification, And Integration

**Files:**
- All changed files

**Step 1: Subagent reviews**

Run two-stage review after implementation:

- Spec reviewer: verify scope, fail-closed behavior, transaction contract binding, startup behavior, and docs.
- Code-quality reviewer: verify Rust API shape, object-safety, error hygiene, memory/performance footprint, and tests.

Fix review findings in separate small commits where practical.

**Step 2: Focused gates before implementation commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 3: Final integration gates**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
git diff --check
```

**Step 4: Small commits**

Use small, purposeful commits:

```bash
git add docs/plans/2026-05-05-durable-core-db-implementation-path.md
git commit -m "docs: plan durable core db implementation path"

git add src/server/core.rs src/server/mod.rs tests/server_startup.rs
git commit -m "feat: add durable core db implementation path"

git add src/server/core.rs docs/http-api-guide.md docs/project-status.md
git commit -m "fix: address durable core db review findings"
```

Only create the review-fix commit if review produces code/docs changes. Keep final status/docs changes in the implementation or review-fix commit when they are small and directly tied to the slice.

After `v2/foundation` is verified and pushed, merge to `main`, rerun the main post-merge gate subset, and push `main`.
