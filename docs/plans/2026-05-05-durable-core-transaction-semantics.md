# Durable Core Transaction Semantics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task in this session.

**Goal:** Define and test the durable core filesystem/VCS transaction semantics that must hold before live Postgres/R2 `CoreDb` routing is allowed.

**Architecture:** Add a backend-local transaction semantics module that models the ordered durable core write path, failure classifications, retry/cleanup policy, and commit/ref/workspace/audit/idempotency post-commit behavior. This is a contract and reference-policy slice only: it does not route HTTP filesystem/VCS requests to Postgres/R2 and keeps durable core runtime modes fail-closed.

**Tech Stack:** Rust, Tokio tests, existing backend object/commit/ref/idempotency/audit/workspace contracts, cargo release perf gate.

---

## Scope

- Add a narrow backend module for durable core transaction semantics.
- Encode the durable core write sequence from object bytes through idempotency completion.
- Classify failure points into pre-commit, orphan-repair, committed-partial, and terminal replay states.
- Define final-object cleanup policy so deletion is never allowed without metadata fencing.
- Add focused unit tests that make the ordering and recovery policy executable.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

## Non-Goals

- Do not implement a Postgres/R2-backed `CoreDb`.
- Do not route HTTP filesystem/VCS requests to Postgres/R2.
- Do not add a distributed lock service.
- Do not change current local-state route response behavior.
- Do not change the existing Postgres schema unless a test proves the contract cannot be expressed without it.
- Do not cut over MCP, CLI, FUSE, run-records, review, or workspace management to the core seam.

## Execution Model

- Main session owns this plan, integration, local review, verification, commits, merge, and push.
- Implementation work should be delegated to one bounded Rust worker subagent.
- Review should be delegated to separate spec-compliance and code-quality subagents.
- Rust subagents must use:
  - `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
  - `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
  - `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`
- After every code or docs diff, run:

```bash
cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Use a sleep before long tests and long wait windows while cargo runs.

## Task 1: Add Failing Transaction Semantics Tests

**Files:**
- Create: `src/backend/core_transaction.rs`
- Modify: `src/backend/mod.rs`

**Step 1: Write the tests first**

Add tests that require a transaction semantics API which does not exist yet:

- `durable_core_transaction_steps_are_ordered_for_commit_visibility`
- `failure_before_ref_cas_is_not_committed_and_aborts_idempotency`
- `failure_after_final_object_before_metadata_requires_repair_not_delete`
- `failure_after_ref_cas_is_committed_partial_and_completes_idempotency`
- `final_object_deletion_requires_metadata_fencing`

The tests should assert concrete step ordering and recovery decisions rather than only checking enum equality.

**Step 2: Run RED verification**

Run:

```bash
cargo test --locked backend::core_transaction --lib -- --nocapture
```

Expected: fail to compile because the module/types are missing.

## Task 2: Implement The Semantics Contract Module

**Files:**
- Create: `src/backend/core_transaction.rs`
- Modify: `src/backend/mod.rs`

**Step 1: Add public contract types inside the backend module**

Define crate-public or public backend types as appropriate for existing backend contracts:

- `DurableCoreTransactionStep`
- `DurableCoreCommitPoint`
- `DurableCoreFailureClass`
- `DurableCoreRecoveryAction`
- `FinalObjectCleanupDecision`
- `DurableCoreStepSemantics`

The ordered write path must cover:

1. idempotency reservation
2. auth/policy/protected-rule preflight
3. staged object-byte upload
4. final object-byte promotion
5. object metadata insert
6. commit metadata insert
7. ref compare-and-swap
8. workspace head update
9. audit append
10. idempotency completion

**Step 2: Encode failure policy**

Required policy:

- Failures before ref CAS are not user-visible committed mutations.
- Failures after staged upload can leave staged bytes; staged cleanup is allowed.
- Failures after final object promotion but before metadata insert require metadata repair or retry; final object deletion is forbidden without metadata fencing.
- Failures after commit metadata insert but before ref CAS can leave unreachable commits; retry is allowed, but the mutation is not visible through the target ref.
- Failures after ref CAS are committed partial failures; rollback is not a default recovery action.
- Workspace-head, audit, and idempotency-completion failures after ref CAS must be completed or replayed as committed/partial results, preserving current HTTP semantics.

**Step 3: Run GREEN verification**

Run:

```bash
cargo test --locked backend::core_transaction --lib -- --nocapture
```

Expected: new transaction semantics tests pass.

## Task 3: Add Reference Transaction Contract Tests Against Existing Stores

**Files:**
- Modify: `src/backend/core_transaction.rs`
- Optionally modify: `src/backend/mod.rs`

**Step 1: Add focused in-memory reference tests**

Use existing `LocalMemoryObjectStore`, `LocalMemoryCommitStore`, `LocalMemoryRefStore`, `InMemoryIdempotencyStore`, `InMemoryAuditStore`, and `InMemoryWorkspaceMetadataStore` only for tests.

Add tests proving:

- commit metadata can be inserted before ref CAS without moving the ref on a stale expectation.
- ref CAS is the visibility point for a durable commit transaction.
- idempotency completion happens after committed response construction, not before ref visibility.
- workspace-head update remains a post-ref step and cannot define commit visibility.

Keep these tests narrow. Do not build a production transaction executor in this slice unless the tests cannot express the contract without one.

**Step 2: Run focused verification**

Run:

```bash
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked backend::tests::ref_cas_rejects_stale_target_or_version_without_mutation --lib -- --nocapture
cargo test --locked backend::tests::source_checked_ref_cas_models_change_request_merge --lib -- --nocapture
```

Expected: all pass.

## Task 4: Docs And Living Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP/runtime docs**

Document that durable core transaction semantics are now specified as a backend contract, but HTTP filesystem/VCS routes still use local-state `StratumDb`.

**Step 2: Update status**

Add a new completed slice section for Durable Core Transaction Semantics:

- What is built: ordered durable write path, failure/recovery classifications, final-object cleanup fencing policy, focused tests.
- What is not built: live Postgres/R2 `CoreDb`, distributed locks, background repair worker, full cross-store executor, route cutover.
- Recommended next slice: narrow durable `CoreDb` implementation path behind fail-closed startup, or object repair worker/fencing if the transaction contract exposes a prerequisite.

## Task 5: Verification, Commit, Merge

**Files:**
- All changed files

**Step 1: Run hard release perf gate after every diff**

Run after plan docs, code, docs, formatting, and review-fix diffs:

```bash
cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 2: Run focused gates**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked backend::tests::ref_cas_rejects_stale_target_or_version_without_mutation --lib -- --nocapture
cargo test --locked backend::tests::source_checked_ref_cas_models_change_request_merge --lib -- --nocapture
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
git add docs/plans/2026-05-05-durable-core-transaction-semantics.md
git commit -m "docs: plan durable core transaction semantics"
git add src/backend docs/http-api-guide.md docs/project-status.md
git commit -m "feat: add durable core transaction semantics"
```

Then push `v2/foundation`, merge to `main`, rerun main verification, and push `main`.
