# Route-Facing Core Runtime Seam Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move HTTP filesystem, search/tree, and VCS route access through an explicit core runtime boundary while preserving the existing local `StratumDb` behavior.

**Architecture:** Add a server-local `CoreDb`/`CoreRuntime` seam that wraps the current local-state `StratumDb` and exposes only the core filesystem, search/tree, VCS, auth, and health methods route handlers need. `ServerState` will carry `core` for route-facing core operations while the durable Postgres/R2 core mode remains recognized but fail-closed at startup.

**Tech Stack:** Rust, Axum, Tokio, existing `StratumDb`, existing server route tests, cargo perf gates.

---

## Scope

- Keep `STRATUM_CORE_RUNTIME=local-state` as the only supported server core runtime.
- Keep `STRATUM_CORE_RUNTIME=durable-cloud` and aliases fail-closed before local state opens or server routes are built.
- Move HTTP filesystem, search, tree, and VCS route calls away from direct `state.db.*` usage and through the core runtime seam.
- Preserve current HTTP behavior, response shapes, idempotency fingerprints, audit behavior, workspace-head updates, and protected ref/path enforcement.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

## Non-Goals

- Do not route object bytes to R2.
- Do not route commit/ref metadata to Postgres.
- Do not add distributed locks, connection pooling, or cross-store transaction semantics.
- Do not rewrite `StratumDb`, `VirtualFs`, or `Vcs`.
- Do not cut over MCP, CLI, FUSE, or SDK code paths.

## Task 1: Add Route Seam Tests

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/mod.rs`

**Step 1: Write failing route seam tests**

Add focused tests that assert the route-facing state exposes a core handle and that filesystem/VCS routes can exercise local-state core behavior through it:

- In `routes_fs` tests, create a state helper that stores `CoreRuntime::local_state(db)` and assert a PUT followed by GET still works through the route state.
- In `routes_vcs` tests, create a state helper that stores `CoreRuntime::local_state(db)` and assert ref list/create/update plus commit/log still work through the route state.
- In `server::tests`, add a construction test proving `build_router_with_stores` installs a local-state core seam rather than exposing only raw `StratumDb`.

**Step 2: Run tests and verify RED**

Run:

```bash
cargo test --locked server::routes_fs::tests::put_fs_routes_through_local_core_runtime --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_routes_use_local_core_runtime --lib -- --nocapture
```

Expected: fail to compile because `CoreRuntime`/`core` does not exist yet.

## Task 2: Implement Local-State Core Runtime Wrapper

**Files:**
- Create: `src/server/core.rs`
- Modify: `src/server/mod.rs`

**Step 1: Add the seam**

Create:

```rust
pub type SharedCoreRuntime = Arc<dyn CoreDb>;

#[async_trait]
pub trait CoreDb: Send + Sync {
    async fn login(&self, username: &str) -> Result<Session, VfsError>;
    async fn authenticate_token(&self, raw_token: &str) -> Result<Session, VfsError>;
    async fn session_for_uid(&self, uid: Uid) -> Result<Session, VfsError>;
    async fn commit_count(&self) -> usize;
    async fn inode_count(&self) -> usize;
    async fn object_count(&self) -> usize;
    async fn config(&self) -> &Config;
    // filesystem/search/tree route methods
    // VCS route methods
}
```

Use the exact methods consumed by current filesystem/search/tree/VCS route handlers, delegating to an internal `Arc<StratumDb>` for `LocalCoreRuntime`.

If returning `&Config` from an async trait is awkward or creates lifetime friction, expose narrow values needed by routes instead, such as `max_file_size()`.

**Step 2: Wire server state**

Change `ServerState` to carry:

```rust
pub core: SharedCoreRuntime,
pub db: Arc<StratumDb>,
```

Keep `db` temporarily for auth/workspace/review/run routes that are outside this slice, but route filesystem/search/tree/VCS through `core`. Build helpers should create `LocalCoreRuntime::new(db.clone())` from the same underlying `StratumDb`.

**Step 3: Run tests and verify GREEN for seam construction**

Run:

```bash
cargo test --locked server::tests:: --lib -- --nocapture
```

Expected: server tests compile and pass.

## Task 3: Move Filesystem/Search/Tree Routes Through Core

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/core.rs`

**Step 1: Replace direct route calls**

Change filesystem, search, and tree route handlers and helper preflights from `state.db.*` to `state.core.*` for:

- `final_existing_write_path_as`
- `check_mkdir_p_as`
- `check_write_file_as`
- `check_set_metadata_as`
- `check_rm_as`
- `check_cp_replay_as`
- `check_mv_replay_as`
- `check_cp_as`
- `check_mv_as`
- `ls_as`
- `stat_as`
- `cat_with_stat_as`
- `mkdir_p_as`
- `write_file_as`
- `set_metadata_as`
- `rm_as`
- `cp_as`
- `mv_as`
- `grep_as`
- `find_as`
- `tree_as`

**Step 2: Run focused filesystem/search tests**

Run:

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
```

Expected: all filesystem route tests pass without behavior changes.

## Task 4: Move VCS Routes Through Core

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/core.rs`

**Step 1: Replace direct route calls**

Change VCS route handlers and helpers from `state.db.*` to `state.core.*` for:

- `resolve_commit_hash`
- `changed_paths_for_revert`
- `list_refs`
- `create_ref`
- `update_ref`
- `commit_as`
- `vcs_log_as`
- `revert_as_with_path_check`
- `vcs_status_as`
- `vcs_diff_as`

Do not change review route internals in this slice unless compilation requires helper movement.

**Step 2: Run focused VCS tests**

Run:

```bash
cargo test --locked server::routes_vcs --lib -- --nocapture
```

Expected: all VCS route tests pass without behavior changes.

## Task 5: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update runtime docs**

Document that HTTP filesystem/search/tree/VCS routes now traverse the route-facing core runtime seam, still backed by local `StratumDb` for `local-state`.

**Step 2: Update living status**

Move the latest backend slice marker to route-facing core runtime seam and note residual risks:

- no live Postgres object/commit/ref routing
- no R2 object-byte routing
- no distributed transaction semantics
- review/control-plane helpers still rely on local core data until the durable core cutover

## Task 6: Verification, Commit, Merge

**Files:**
- All changed files

**Step 1: Run hard perf gate after each diff**

After each docs or code diff, run:

```bash
cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 2: Run focused gates before code commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::core --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_vcs --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

**Step 3: Run final integration gates**

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
cargo test --locked --release --test perf -- --test-threads=1 --nocapture
cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
git diff --check
```

**Step 4: Commit and integrate**

Use small commits:

```bash
git add docs/plans/2026-05-05-route-facing-core-runtime-seam.md
git commit -m "docs: plan route-facing core runtime seam"
git add src/server docs/http-api-guide.md docs/project-status.md
git commit -m "feat: add route-facing core runtime seam"
```

Then merge `v2/foundation` to `main`, rerun the main verification subset, and push both branches.
