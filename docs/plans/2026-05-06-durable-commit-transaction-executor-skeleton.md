# Durable Commit Transaction Executor Skeleton Implementation Plan

This plan uses the planning, test-driven development, pragmatic Rust, Rust best practices, and Rust async skills. The slice is intentionally narrow: define the first internal durable commit transaction executor skeleton and keep durable HTTP/CoreDb serving fail-closed.

## Goal

Add an internal durable `POST /vcs/commit` execution skeleton that binds the existing transaction semantics contract to the durable `CoreDb` runtime seam. The skeleton should make the commit boundary executable and reviewable without claiming production durable commit support.

## Current State

- `DurableCoreRuntime::create_ref` and `DurableCoreRuntime::update_ref` can execute over durable commit/ref stores.
- `DurableCoreRuntime::commit_as` still returns redacted `NotSupported`.
- `DurableCoreStepSemantics::ordered_write_path()` already defines the durable commit write order:
  `IdempotencyReservation -> AuthPolicyPreflight -> StagedObjectUpload -> FinalObjectPromotion -> ObjectMetadataInsert -> CommitMetadataInsert -> RefCompareAndSwap -> WorkspaceHeadUpdate -> AuditAppend -> IdempotencyCompletion`.
- Route-level `/vcs/commit` currently handles idempotency, auth, workspace head update, audit, and local `CoreDb::commit_as` orchestration outside a durable cross-store transaction.
- Durable object bytes, live tree construction, source filesystem snapshots, workspace-head coupling, audit/idempotency completion, locking/fencing, and repair workers are not ready for live durable commit execution.

## Non-Goals

- Do not enable `STRATUM_CORE_RUNTIME=durable-cloud` serving.
- Do not route HTTP `/vcs/commit` through durable object/commit/ref execution.
- Do not write durable object bytes or commit metadata from the skeleton.
- Do not change durable auth/session behavior.
- Do not add distributed locks, object cleanup workers, final object deletion, hosted secrets/TLS/KMS posture, or R2 live routing.

## Implementation Shape

1. Add a small internal commit executor skeleton type.
   - Prefer `src/backend/core_transaction.rs` for transaction-policy data and ordering.
   - Keep server/runtime-specific wiring in `src/server/core.rs`.
   - The skeleton should expose:
     - the ordered write path from `DurableCoreStepSemantics`;
     - the current live execution state, initially disabled;
     - unresolved prerequisites for live commit execution;
     - a redacted unsupported-execution error helper.

2. Add a durable runtime seam for the skeleton.
   - Add an internal `DurableCoreRuntime` method that returns the skeleton/plan.
   - `CoreDb::commit_as` for `DurableCoreRuntime` may construct or reference the skeleton, but must still return the existing route-level fail-closed `NotSupported` error without leaking commit message, username, token, workspace, or runtime config values.

3. Keep error redaction explicit.
   - The skeleton must not store raw commit messages or session secrets.
   - Any unsupported/preflight error must use stable generic text.
   - Tests should include sensitive input strings to prevent leaks.

4. Add RED tests before implementation.
   - Test that the durable runtime exposes a commit transaction skeleton matching the durable write path.
   - Test that the skeleton marks live execution disabled and reports concrete missing prerequisites.
   - Test that `commit_as` remains fail-closed and redacted for sensitive commit/session input.
   - Test that list/create/update ref behavior remains unchanged where relevant.

5. Keep the implementation allocation-light.
   - Use static slices for ordered steps and prerequisites.
   - Avoid cloning stores, messages, or session data into skeleton state.
   - Keep async work out of pure transaction-policy helpers.

## Expected Tests

Run at minimum:

```sh
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Then run the normal verification gates for the branch, including:

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record real/user/sys time, max RSS, and peak memory footprint from the release perf run.

## Acceptance Criteria

- Internal durable commit executor skeleton exists and is covered by tests.
- The skeleton reuses the existing durable transaction step ordering instead of duplicating a second ordering source.
- Missing live-execution prerequisites are explicit in code and tests.
- Durable `commit_as` remains fail-closed and redacted.
- Durable startup/auth/HTTP serving remain fail-closed.
- No durable stores are mutated by the skeleton.
- Required tests and measured release perf pass.

