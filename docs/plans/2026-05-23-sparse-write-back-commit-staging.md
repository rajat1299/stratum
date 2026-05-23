# Sparse Write-Back And Commit Staging Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative, disabled-by-default sparse write-back and commit-staging foundation that records local dirty cache writes and models durable session-ref flush/promotion semantics without production sparse mount cutover.

**Architecture:** Keep sparse cache as local cache/staging state, not source of truth. Local dirty file content is represented in separate dirty tables and read through an explicit overlay; flush planning maps dirty entries into existing durable session-ref mutation semantics, and commit staging binds flushed session roots to the existing durable ref-CAS commit path. The default mode remains read-only/disabled so sparse mounts cannot mutate durable refs unless tests explicitly opt into the write-back path.

**Tech Stack:** Rust, SQLite via `rusqlite`, existing Stratum durable stores/transactions, provider-free in-memory stores for tests.

---

## Current State

- Sparse cache schema version is `3` and stores durable-identity views, inodes, dentries, immutable chunks, symlinks, statfs, and hydration jobs.
- `SparseCacheMount` implements only `MountReadAdapter`. It may hydrate missing blob chunks through an injected source during `read`, but metadata calls remain provider-free.
- `stratumctl mount status|logs|unmount` are provider-free control-plane commands over local PID/socket/log metadata; there is no real sparse daemon lifecycle.
- `stratum-mount` still mounts `db.snapshot_fs()` and remains the rollback path.
- Durable mounted-session FS mutations already apply policy first, materialize or advance `session_ref`, write durable objects/commits, and CAS-update the session ref. Post-visible audit/idempotency side effects are recovery-backed.
- Guarded durable VCS commit promotes mounted durable session refs through durable commit/ref CAS, with pre-visibility and post-CAS recovery.
- smfs is useful only for write/flush/fsync adapter shape and daemon drain/status patterns. Do not copy its path latest-wins queue or timestamp conflict semantics.

## Non-Goals

- Do not wire production sparse FUSE/NFS writes.
- Do not make sparse mount the default.
- Do not enable durable-cloud non-server FUSE/MCP/REPL access.
- Do not add HTTP route behavior changes.
- Do not require live Postgres, R2, durable-cloud env, network, or privileged mounts in normal tests.
- Do not add broad unreachable object/commit deletion.

## Task 1: Plan Document Commit

**Files:**
- Create: `docs/plans/2026-05-23-sparse-write-back-commit-staging.md`

**Step 1: Verify doc exists**

Run:

```bash
test -f docs/plans/2026-05-23-sparse-write-back-commit-staging.md
```

Expected: exit 0.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-23-sparse-write-back-commit-staging.md
git commit -m "docs: plan sparse write-back staging"
```

Expected: commit succeeds on `v2/foundation`.

## Task 2: Sparse Dirty Cache Schema And Local Write Model

**Files:**
- Modify: `src/sparse_cache/schema.sql`
- Modify: `src/sparse_cache/mod.rs`

**Design:**
- Bump sparse cache schema version from `3` to `4`.
- Add dirty local state tables separate from immutable durable chunks:
  - `sparse_cache_dirty_entries`: one dirty file entry per `(view_id, inode_id)` with normalized path, operation (`write_file` initially), state (`dirty`, `queued`, `flushed`, `failed`), base object id/kind, base commit/ref identity from the view, content length, content hash/object id, timestamps, and fixed optional error code.
  - `sparse_cache_dirty_chunks`: dirty content chunks keyed by `dirty_id` and `chunk_index`.
  - `sparse_cache_writeback_queue`: queue rows keyed by dirty entry with operation id, source identity, state (`pending`, `running`, `flushed`, `failed`, `disabled`), attempts, timestamps, and fixed optional error code.
- Keep all debug output redacted: no file bytes, raw paths, object ids, repo ids, tokens, SQL, provider errors, or local runtime paths.
- Add local APIs with narrow names:
  - `SparseCache::write_dirty_file(...)`
  - `SparseCache::dirty_entry_for_inode(...)`
  - `SparseCache::dirty_file_bytes(...)`
  - `SparseCache::enqueue_writeback(...)`
  - `SparseCache::writeback_progress(...)`

**Step 1: Write failing schema/model tests**

Add focused tests under `sparse_cache::tests`:

```rust
#[test]
fn dirty_file_write_records_local_content_without_overwriting_immutable_chunks() { /* red */ }

#[test]
fn dirty_entry_debug_redacts_path_repo_object_and_bytes() { /* red */ }

#[test]
fn writeback_queue_dedupes_dirty_entry_and_tracks_disabled_state() { /* red */ }
```

Run:

```bash
cargo test --locked sparse_cache::tests::dirty --lib -- --nocapture
```

Expected: fails because dirty schema/API does not exist.

**Step 2: Implement the minimum model**

Implement schema tables, version migration, structs/enums, validation helpers, row mapping, and APIs needed by the tests. Preserve current hydration/read APIs and existing tests.

**Step 3: Verify**

Run:

```bash
cargo test --locked sparse_cache::tests --lib -- --nocapture
```

Expected: sparse cache tests pass.

**Step 4: Commit**

Run:

```bash
git add src/sparse_cache/schema.sql src/sparse_cache/mod.rs
git commit -m "feat: record sparse dirty cache writes"
```

## Task 3: Dirty Read Overlay And Disabled Mount Write Surface

**Files:**
- Modify: `src/mount_adapter.rs`
- Modify: `src/sparse_cache/mount.rs`

**Design:**
- Preserve `MountReadAdapter` as read-only.
- Add a separate provider-free write-shaped contract for future FUSE/NFS wiring, not used by production mounts:
  - `MountWriteAdapter`
  - `MountWriteErrorCode`
  - `MountWriteError`
  - minimal write/flush/fsync outcome structs with redacted debug output.
- Default/rollback write adapter must return a fixed read-only/disabled error and must not mutate cache or durable refs.
- `SparseCacheMount::read` may prefer dirty local bytes only when a dirty entry exists for the inode in the same view. Existing clean read behavior must remain unchanged.
- Do not wire this trait to `stratum-mount`, daemon lifecycle, HTTP routes, or real FUSE/NFS callbacks.

**Step 1: Write failing tests**

Add focused tests:

```rust
#[test]
fn sparse_mount_read_prefers_dirty_overlay_without_mutating_immutable_chunks() { /* red */ }

#[test]
fn default_mount_write_surface_is_read_only_and_redacted() { /* red */ }
```

Run:

```bash
cargo test --locked sparse_cache::mount::tests::dirty --lib -- --nocapture
cargo test --locked mount_adapter::tests::write --lib -- --nocapture
```

Expected: fails because overlay/write surface does not exist.

**Step 2: Implement the minimum overlay and disabled write surface**

Read dirty content from sparse cache before immutable object chunks. Keep missing dirty rows a no-op. Add write trait/types and disabled helper without changing existing read adapter semantics.

**Step 3: Verify**

Run:

```bash
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked mount_adapter::tests --lib -- --nocapture
```

Expected: mount and adapter tests pass.

**Step 4: Commit**

Run:

```bash
git add src/mount_adapter.rs src/sparse_cache/mount.rs
git commit -m "feat: model disabled sparse mount writes"
```

## Task 4: Write-Back Flush Planning Model

**Files:**
- Create: `src/sparse_cache/write_back.rs`
- Modify: `src/sparse_cache/mod.rs`

**Design:**
- Add a disabled-by-default planner that converts queued dirty entries into durable-session mutation intents without executing them by default.
- Add explicit mode:
  - `SparseWriteBackMode::Disabled`
  - `SparseWriteBackMode::EnabledForTests`
- Disabled mode must produce a redacted blocked plan and must not claim jobs, call durable stores, or mutate refs.
- Enabled-for-tests mode may build provider-free durable mutation inputs from queued dirty entries:
  - base ref and session ref from explicit input, not from local paths.
  - source commit/ref/version from sparse view and queue metadata.
  - operation id/fingerprint derived from stable redacted-safe inputs and content hash, not raw bytes.
- Planner output must expose counts, changed paths redacted in debug, and whether mutation execution is allowed.

**Step 1: Write failing tests**

Add tests under `sparse_cache::write_back::tests`:

```rust
#[test]
fn disabled_writeback_mode_blocks_planning_without_claiming_queue() { /* red */ }

#[test]
fn enabled_test_mode_builds_durable_write_file_intent_from_dirty_entry() { /* red */ }

#[test]
fn flush_plan_debug_redacts_paths_repo_ids_object_ids_and_content() { /* red */ }
```

Run:

```bash
cargo test --locked sparse_cache::write_back::tests --lib -- --nocapture
```

Expected: fails because module/model does not exist.

**Step 2: Implement the minimum planner**

Add the module, mode enum, plan structs, validation, stable operation id/fingerprint helpers, and conversion into durable mutation intent structs. Keep execution separate.

**Step 3: Verify**

Run:

```bash
cargo test --locked sparse_cache::write_back::tests --lib -- --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
```

Expected: write-back and sparse cache tests pass.

**Step 4: Commit**

Run:

```bash
git add src/sparse_cache/write_back.rs src/sparse_cache/mod.rs
git commit -m "feat: plan sparse write-back flushes"
```

## Task 5: Provider-Free Durable Session Flush Execution For Tests

**Files:**
- Modify: `src/sparse_cache/write_back.rs`
- Modify only if needed: `src/backend/durable_mutation.rs`
- Modify only if needed: `src/backend/core_transaction.rs`

**Design:**
- Add an explicit test-only execution path that takes an enabled flush plan and applies dirty entries through the existing `DurableMutationEngine`.
- The execution path must:
  - refuse `SparseWriteBackMode::Disabled`;
  - use durable session refs as source of truth;
  - preserve source-checked CAS and stale-session failure behavior;
  - keep queue rows pending/failed if durable mutation fails;
  - mark dirty/queue rows flushed only after durable mutation output confirms `session_ref`, previous commit, new commit, root tree, and changed path count;
  - build recovery-compatible target/envelope metadata from `DurableMutationOutput` without raw body/path leakage.
- Do not call HTTP route handlers from this module. Do not require Postgres/R2/durable-cloud/network.

**Step 1: Write failing tests**

Add provider-free tests using `StratumStores::local_memory`:

```rust
#[tokio::test]
async fn enabled_test_flush_advances_session_ref_through_durable_mutation_engine() { /* red */ }

#[tokio::test]
async fn stale_session_ref_cas_leaves_dirty_entry_unflushed_and_redacted() { /* red */ }

#[tokio::test]
async fn disabled_flush_executor_never_calls_ref_store() { /* red */ }
```

Run:

```bash
cargo test --locked sparse_cache::write_back::tests --lib -- --nocapture
```

Expected: fails until executor exists.

**Step 2: Implement executor**

Use the existing durable mutation engine and in-memory store traits. Keep any helper exports `pub(crate)` and narrow.

**Step 3: Verify**

Run:

```bash
cargo test --locked sparse_cache::write_back::tests --lib -- --nocapture
cargo test --locked backend::durable_mutation --lib -- --nocapture
```

Expected: write-back and durable mutation tests pass.

**Step 4: Commit**

Run:

```bash
git add src/sparse_cache/write_back.rs src/backend/durable_mutation.rs src/backend/core_transaction.rs
git commit -m "feat: flush sparse writes to durable sessions in tests"
```

Only add backend files if they changed.

## Task 6: Commit Staging Model

**Files:**
- Modify: `src/sparse_cache/write_back.rs`
- Modify only if needed: `src/backend/core_transaction.rs`

**Design:**
- Add a provider-free commit-staging model that proves a flushed sparse session can be promoted with existing durable commit semantics.
- Inputs:
  - repo id;
  - target/base ref name, commit id, and version;
  - session ref name, commit id, version, and root tree id;
  - dirty/queue state summary.
- Requirements:
  - reject pending dirty or pending queue entries;
  - reject disabled write-back mode;
  - require session head to descend from the source commit where that proof is available;
  - expose existing durable ordered write path where ref CAS is the visibility point;
  - model pre-visibility and post-CAS recovery compatibility through existing durable commit types; do not add a new commit transaction path.
- This is a staging/validation model only. Do not alter `POST /vcs/commit` behavior.

**Step 1: Write failing tests**

Add tests:

```rust
#[test]
fn commit_staging_rejects_pending_dirty_entries() { /* red */ }

#[tokio::test]
async fn commit_staging_uses_durable_ref_cas_ordered_write_path() { /* red */ }

#[test]
fn commit_staging_debug_redacts_paths_and_messages() { /* red */ }
```

Run:

```bash
cargo test --locked sparse_cache::write_back::tests::commit_staging --lib -- --nocapture
```

Expected: fails until staging model exists.

**Step 2: Implement staging model**

Reuse existing durable commit source snapshot/write-plan/recovery types. Add no new route behavior.

**Step 3: Verify**

Run:

```bash
cargo test --locked sparse_cache::write_back::tests --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

Expected: staging, durable commit write-plan, and VCS recovery tests pass.

**Step 4: Commit**

Run:

```bash
git add src/sparse_cache/write_back.rs src/backend/core_transaction.rs
git commit -m "feat: model sparse commit staging"
```

Only add backend file if it changed.

## Task 7: Docs And Status Updates

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/plans/2026-05-23-sparse-write-back-commit-staging.md`

**Design:**
- Document what now exists:
  - local dirty sparse-cache writes;
  - dirty read overlay;
  - disabled default write-back mode;
  - provider-free enabled-for-tests flush path;
  - commit staging model using durable ref CAS.
- Document what remains out of scope:
  - production sparse FUSE/NFS writes;
  - daemon lifecycle cutover;
  - durable-cloud non-server access;
  - HTTP route changes;
  - live provider verification.
- Mark task completion in this plan as implementation lands.

**Step 1: Write doc updates**

Update the status/API docs with redaction and rollback boundaries.

**Step 2: Verify docs mention no production cutover**

Run:

```bash
rg -n "write-back|dirty|commit staging|production sparse|HTTP API behavior|read-only by default" docs/project-status.md docs/http-api-guide.md docs/plans/2026-05-23-sparse-write-back-commit-staging.md
```

Expected: output includes the new boundaries.

**Step 3: Commit**

Run:

```bash
git add docs/project-status.md docs/http-api-guide.md docs/plans/2026-05-23-sparse-write-back-commit-staging.md
git commit -m "docs: record sparse write-back boundaries"
```

## Implementation Result

Completed on 2026-05-23 from the `v2/foundation` worktree.

- Local dirty sparse-cache writes now live in separate dirty/write-back tables and do not overwrite immutable hydrated chunks.
- Sparse mounts remain read-only by default, with a disabled write adapter and a dirty read overlay for provider-free tests.
- Write-back planning is disabled by default; enabled-for-tests mode builds durable session-ref mutation intents and a test-only executor applies them through the existing `DurableMutationEngine`.
- Commit staging is a provider-free test model only. It validates flushed durable session refs against source-checked `main` ref versions, internal durable mutation ancestry, previous-promotion compatibility, durable ref CAS visibility, and existing pre-visibility/post-CAS recovery steps.
- HTTP API behavior is unchanged. There is no production sparse FUSE/NFS write cutover, daemon lifecycle cutover, durable-cloud non-server/FUSE enablement, or live provider requirement.

Focused verification completed during implementation:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked sparse_cache::write_back::tests --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
```

The full verification gate list also passed locally on 2026-05-23. Live Postgres/R2 provider portions skipped cleanly where `STRATUM_POSTGRES_TEST_URL`, complete `STRATUM_R2_*`, or `STRATUM_R2_TEST_ENABLED=1` were not set.

## Review Plan

- After each implementation task, run spec-compliance review with `gpt-5.5` `xhigh`.
- After spec compliance passes, run code-quality/security review with `gpt-5.5` `xhigh`.
- Main session must inspect diffs locally before accepting any subagent result.
- Main session fixes small integration issues only when needed; delegate substantial code changes back to workers.

## Verification Gates

Run the focused Slice 14 tests introduced by this plan, then:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked mount_daemon::tests --lib -- --nocapture
cargo test --locked --bin stratumctl -- --nocapture
cargo test --locked mount_adapter::tests --lib -- --nocapture
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Live provider portions are expected to skip when their env vars are unset. Do not claim protected-provider CI unless inspected.
