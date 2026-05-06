# Durable Commit Transaction Metadata Preflight Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the first metadata-only durable commit transaction preflight behind the internal durable `CoreDb` runtime while keeping durable startup, auth, and live HTTP serving fail-closed.

**Architecture:** Extend the durable commit skeleton with a small preflight snapshot that describes the target ref and current durable parent state. Wire `DurableCoreRuntime` to read only durable ref/commit metadata, validate that an existing `main` ref points at reachable commit metadata, and return redacted errors without mutating durable stores.

**Tech Stack:** Rust 2024, Tokio async tests, existing `DurableCoreStepSemantics`, `StratumStores`, `RefStore`, `CommitStore`, `RefVersion`, `CommitId`, and `RefName`.

---

## Scope

This slice is a metadata preflight only. It must stop before object-byte writes, tree construction, commit metadata insertion, ref compare-and-swap, workspace-head update, audit append, or idempotency completion.

## Current State

- `DurableCoreCommitExecutorSkeleton` exposes write-ordering, live execution disabled state, unresolved prerequisites, and redacted unsupported-execution errors.
- `DurableCoreRuntime::commit_as` references the skeleton but still returns the existing route-level fail-closed `NotSupported`.
- Durable `create_ref` and `update_ref` can execute over durable commit/ref stores.
- `DurableCoreRuntime` owns `repo_id` and `StratumStores`, so it can safely perform internal metadata reads in tests without route cutover.

## Non-Goals

- Do not enable `STRATUM_CORE_RUNTIME=durable-cloud` serving.
- Do not route HTTP `/vcs/commit` through durable execution.
- Do not write object bytes, object metadata, commit metadata, refs, workspace metadata, audit events, or idempotency records.
- Do not add durable auth/session execution.
- Do not add distributed locks, final-object deletion, background repair workers, R2 live routing, hosted TLS/KMS/secrets posture, or production transaction completion.

## Task 1: Add Preflight Snapshot Types

**Files:**
- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add tests under `backend::core_transaction::tests` for:

- `DurableCoreCommitParentState::Unborn` representing no current `main` ref.
- `DurableCoreCommitParentState::Existing { target, version }` representing a known durable parent.
- `DurableCoreCommitMetadataPreflight` returning:
  - `target_ref() == MAIN_REF`;
  - the parent state;
  - `ordered_write_path()` from `DurableCoreStepSemantics`;
  - `live_execution_enabled() == false`;
  - unresolved prerequisites inherited from `DurableCoreCommitExecutorSkeleton`.

**Step 2: Verify RED**

Run:

```sh
cargo test --locked backend::core_transaction::tests::durable_core_commit_metadata_preflight --lib -- --nocapture
```

Expected: fails because the preflight types do not exist.

**Step 3: Implement minimal types**

Add:

- `DurableCoreCommitParentState`.
- `DurableCoreCommitMetadataPreflight`.
- Constructor/helper methods that avoid raw commit messages, sessions, tokens, or dynamic error strings.

Use `Copy` where practical and borrowed/static slices for transaction steps and prerequisites.

**Step 4: Verify GREEN**

Run the same focused test command and expect pass.

## Task 2: Wire Durable Runtime Metadata Reads

**Files:**
- Modify: `src/server/core.rs`

**Step 1: Write failing tests**

Add tests under `server::core::tests::durable_core_runtime` for:

- No `main` ref returns an unborn preflight and leaves commit/ref stores unchanged.
- Existing `main` ref whose target exists in durable commit metadata returns existing parent target/version.
- Existing `main` ref whose target commit metadata is missing returns a redacted `CorruptStore` error and does not mutate refs/commits.
- A race where `main` changes while checking missing parent metadata is rechecked before returning the missing-parent error; if the ref changed, return a sanitized compare-and-swap/preflight-stale error instead of reporting the old target as corrupt.
- `commit_as` remains fail-closed and redacted after the preflight seam exists.

**Step 2: Verify RED**

Run:

```sh
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

Expected: new preflight tests fail because `DurableCoreRuntime` has no metadata preflight method.

**Step 3: Implement minimal runtime method**

Add an internal async method, for example:

```rust
pub(crate) async fn commit_metadata_preflight(
    &self,
) -> Result<DurableCoreCommitMetadataPreflight, VfsError>
```

Behavior:

- Build/obtain the existing skeleton.
- Validate/construct the durable target ref as `MAIN_REF`.
- Read `main` from `stores.refs`.
- If absent, return unborn preflight.
- If present, check `stores.commits.contains(repo_id, target)`.
- If target metadata exists, return existing parent preflight.
- If target metadata is missing, re-read `main`; if target/version changed, return a sanitized stale-preflight/CAS mismatch error; otherwise return a generic redacted corrupt-store error.

Do not write through any store.

**Step 4: Verify GREEN**

Run:

```sh
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Expected: all pass.

## Task 3: Review And Full Verification

**Files:**
- Modify if needed after review: `src/backend/core_transaction.rs`, `src/server/core.rs`
- Modify status docs after implementation: `docs/project-status.md`

**Step 1: Request reviews**

Run one spec/architecture review and one Rust/code-quality/redaction review. Required review focus:

- no live durable commit execution;
- no writes to durable stores;
- stale-ref race behavior is correct;
- no raw commit message/session/token/workspace values retained or leaked;
- no unnecessary cloning/allocation;
- tests are semantic rather than storage-layout brittle.

**Step 2: Fix findings**

Patch locally after inspection. Do not blindly accept reviewer output.

**Step 3: Run verification**

Focused:

```sh
cargo fmt --check
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Full branch gates:

```sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
git diff --check
```

Measured perf after every meaningful code/docs diff:

```sh
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record real/user/sys time, max RSS, and peak memory footprint. GPU efficiency is not applicable.

## Acceptance Criteria

- Durable commit metadata preflight exists and is covered by RED/GREEN tests.
- Existing `main` parent metadata is read and validated without mutation.
- Missing parent metadata is detected with a redacted durable error.
- A stale race during missing-parent detection does not misreport an old ref as corrupt.
- `commit_as` remains fail-closed with the existing route-level redacted `NotSupported`.
- Durable startup/auth/live HTTP serving remain fail-closed.
- Required tests, reviews, perf, and full gates pass.

