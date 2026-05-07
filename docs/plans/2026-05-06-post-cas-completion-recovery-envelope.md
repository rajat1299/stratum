# Post-CAS Completion And Recovery Envelope Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the internal durable post-CAS completion and recovery envelope for visible durable commits without enabling live durable `POST /vcs/commit` routing.

**Architecture:** Keep the slice behind the existing durable transaction seam in `src/backend/core_transaction.rs`. Add a Lattice-native post-CAS envelope that runs workspace-head update, audit append, and idempotency completion after `DurableCoreCommitRefCasVisibility`, plus a durable recovery claim/backoff/poison state machine keyed by commit/ref identity. SMFS is pattern input only for claim/finalize/backoff/poison; do not copy SMFS latest-wins path coalescing, SQLite inode/chunk cache, or sidecar error-file UX.

**Tech Stack:** Rust, Tokio async tests, existing `DurableCoreCommitObjectTreeWritePlan`, `DurableCoreCommitMetadataInsert`, `DurableCoreCommitRefCasVisibility`, `WorkspaceMetadataStore`, `AuditStore`, `IdempotencyStore`, local in-memory stores, focused fake stores, cargo gates, and `/usr/bin/time -l` release perf.

---

## CTO Plan And Status Read

The durable commit path has already landed internal object/tree write planning, object convergence, commit metadata insert, and ref CAS visibility. The project status now recommends post-CAS completion/recovery as the next slice. This is the first slice after the durable commit becomes visible through `main`; failures here must never roll back the ref or delete final objects.

Relevant current state:

- `DurableCoreTransactionStep` already orders `WorkspaceHeadUpdate`, `AuditAppend`, and `IdempotencyCompletion` after `RefCompareAndSwap`.
- Failure semantics already classify failures after `RefCompareAndSwap` as `PostRefCompareAndSwap` with visible mutation and recovery action `CompleteIdempotencyWithCommittedResponse`.
- `WorkspaceMetadataStore::update_head_commit` exists, but is unconditional and advisory. It is not the visibility point.
- `AuditStore::append` is append-only and not idempotent. Do not add automatic audit replay loops in this slice.
- `IdempotencyStore::complete` requires the original `IdempotencyReservation`. Restart-safe idempotency replay still needs a future durable recovery executor or a recovery-specific completion API.
- Existing durable `commit_as` and durable startup remain fail-closed.

SMFS extraction boundary:

- Use SMFS only for durable claim/finalize/failure/backoff/poison concepts.
- Do not import SMFS's latest-wins `push_queue` behavior. Lattice commits are immutable and CAS-visible.
- Do not use SMFS's SQLite inode/chunk cache as a tree/object model. Lattice already uses content-addressed `Blob`, `Tree`, and `CommitObject` primitives.
- Do not copy `.smfs-error.txt` sidecar poison UX. Lattice poison state should be durable metadata with redacted diagnostics.

## Scope

In scope:

- Add a redacted committed-response wrapper for post-CAS idempotency completion.
- Add a post-CAS envelope bound to the same write plan, metadata insert, and ref visibility summary.
- Complete post-CAS steps in this order:
  1. optional workspace-head update;
  2. audit append;
  3. optional idempotency completion.
- Return explicit complete vs partial outcomes instead of treating post-CAS failures as rollbackable errors.
- Complete idempotency with a fixed redacted partial response when workspace-head or audit append fails after ref CAS.
- Add a recovery claim store contract and in-memory implementation for post-CAS completion work:
  - active claim blocks duplicate workers;
  - failure transitions to backing-off state;
  - retry after backoff gets a new token and incremented attempts;
  - stale tokens cannot complete or poison a retry;
  - completed and poisoned targets are terminal for automatic claims;
  - poison keeps redacted diagnosis and does not imply ref rollback.
- Add focused tests for ordering, partial outcomes, idempotency replay, claim/backoff/finalize/poison, redaction, and fail-closed durable route boundaries.

Out of scope:

- No live durable `POST /vcs/commit` route execution.
- No durable auth/session path or route-level request parsing.
- No Postgres schema changes or Postgres recovery-claim adapter in this slice.
- No background worker, bounded worker pool, `Notify`, or `JoinSet` implementation yet.
- No audit replay worker that can duplicate audit events.
- No workspace-head CAS/version expectation yet.
- No final-object deletion or metadata cleanup policy changes.
- No local route behavior changes.

## Performance Rules

After every meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record real/user/sys time, max RSS, and peak memory footprint. This metadata-only slice should not affect hot local VFS paths; investigate unexpected movement before committing.

## Task 1: Add RED Tests For Post-CAS Recovery Claims

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add a sibling test module such as `backend::core_transaction::tests::durable_core_commit_post_cas_recovery`.

Cover:

- `post_cas_recovery_claim_blocks_duplicate_active_worker`
  - claim `(repo_id, main, commit_id, WorkspaceHeadUpdate)`;
  - assert a second claim returns `None` while the lease is active.
- `post_cas_recovery_failure_backs_off_and_retry_gets_new_token`
  - claim once;
  - record a redacted transient failure with a backoff duration;
  - assert claims before backoff return `None`;
  - advance test clock after backoff;
  - assert retry has attempts `2` and a new token.
- `post_cas_recovery_stale_token_cannot_complete_retry`
  - create an expired/backed-off first claim;
  - reacquire retry;
  - assert completing the first token returns stale-claim error;
  - assert completing the retry succeeds.
- `post_cas_recovery_completed_claim_is_terminal`
  - complete a claim;
  - advance clock;
  - assert the target is not reclaimed.
- `post_cas_recovery_poison_blocks_reclaim_and_keeps_redacted_error`
  - poison a claim with a message containing path/message/token-like data;
  - assert automatic claim returns `None`;
  - assert debug/snapshot output uses a fixed redacted message and does not contain raw inputs.
- `post_cas_failure_requires_completion_not_rollback`
  - assert `DurableCoreStepSemantics::failure_semantics(RefCompareAndSwap, AfterStep)` remains visible, not rollbackable, and has recovery action `CompleteIdempotencyWithCommittedResponse`.

**Step 2: Run RED**

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
```

Expected: FAIL because post-CAS recovery claim types and helpers do not exist.

## Task 2: Implement Post-CAS Recovery Claim Store

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Add minimal types**

Add crate-private types near the existing durable commit summaries:

- `DurableCorePostCasStep` with `WorkspaceHeadUpdate`, `AuditAppend`, `IdempotencyCompletion`.
- `DurableCorePostCasRecoveryClaimRequest`.
- `DurableCorePostCasRecoveryClaim`.
- `DurableCorePostCasRecoveryTarget`.
- `DurableCorePostCasRecoveryClaimStore` trait with `claim`, `complete`, `record_failure`, and `poison`.
- `InMemoryDurableCorePostCasRecoveryClaimStore` for tests and future adapter contract.

State machine requirements:

- Claims are keyed by `repo_id`, `ref_name`, `commit_id`, and post-CAS step.
- `claim` validates lease owner, positive lease duration, and canonical `main` ref name for this slice.
- `claim` returns `None` for active, backing-off, completed, or poisoned entries.
- Expired active claims and elapsed backing-off entries can be claimed with attempts incremented.
- `record_failure` requires a current active token, clears active work, stores a fixed redacted error, and sets retry eligibility to `now + backoff`.
- `complete` requires a current active token and marks the target terminal complete.
- `poison` requires a current active token and marks the target terminal poisoned with fixed redacted diagnosis.

**Step 2: Run GREEN**

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
```

Expected: recovery tests pass.

**Step 3: Run required perf**

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record timing and memory.

## Task 3: Add RED Tests For Post-CAS Completion Envelope

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add a sibling test module such as `backend::core_transaction::tests::durable_core_commit_post_cas_completion`.

Build a helper that:

1. creates a write plan with private file path/content;
2. converges objects;
3. inserts commit metadata;
4. applies ref CAS visibility;
5. reserves an idempotency key when needed.

Cover:

- `post_cas_completion_updates_workspace_head_appends_audit_and_completes_idempotency`
  - create a workspace;
  - build envelope with workspace ID, redacted audit event, idempotency reservation, and committed success response;
  - run completion;
  - assert workspace head is commit ID, audit has exactly one commit event, idempotency replay returns the committed response.
- `post_cas_completion_without_workspace_or_idempotency_still_appends_audit`
  - no workspace ID and no idempotency reservation;
  - assert audit append happens and outcome is complete.
- `post_cas_workspace_head_failure_returns_partial_and_completes_idempotency`
  - pass missing workspace ID;
  - assert outcome failed step is `WorkspaceHeadUpdate`;
  - assert idempotency replay returns a fixed redacted committed-partial response;
  - assert no audit append happened.
- `post_cas_audit_failure_returns_partial_and_completes_idempotency`
  - use an audit store that fails append with a leaky message;
  - assert workspace head remains updated;
  - assert idempotency replay returns a fixed redacted committed-partial response;
  - assert raw audit error is not exposed.
- `post_cas_idempotency_completion_failure_is_partial_after_audit`
  - use an idempotency store that fails completion;
  - assert workspace and audit steps happened once;
  - assert outcome failed step is `IdempotencyCompletion`.
- `post_cas_envelope_rejects_unbound_visibility_before_side_effects`
  - use visibility from a different commit/ref version;
  - assert envelope construction fails before workspace, audit, or idempotency mutation.
- `post_cas_envelope_debug_redacts_message_author_paths_response_body_and_tokens`
  - include private commit message, author, paths, response body, and token-like strings;
  - assert Debug output does not contain them.

**Step 2: Run RED**

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_completion --lib -- --nocapture
```

Expected: FAIL because post-CAS envelope types and methods do not exist.

## Task 4: Implement Post-CAS Completion Envelope

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Add envelope types**

Add crate-private types:

- `DurableCoreCommittedResponse`.
- `DurableCoreCommitPostCasInput`.
- `DurableCoreCommitPostCasEnvelope`.
- `DurableCoreCommitPostCasCompletion`.
- `DurableCorePostCasOutcome`.
- `DurableCorePostCasPartial`.

Use custom `Debug` implementations that expose only repo ID, ref name, commit ID, version, optional workspace ID, response status code, and step names. Do not print response bodies, audit details, commit messages, authors, paths, idempotency reservation tokens, or raw downstream errors.

**Step 2: Add construction method**

On `DurableCoreCommitObjectTreeWritePlan`, add:

```rust
pub(crate) fn post_cas_envelope(
    &self,
    metadata: &DurableCoreCommitMetadataInsert,
    visibility: &DurableCoreCommitRefCasVisibility,
    input: DurableCoreCommitPostCasInput,
) -> Result<DurableCoreCommitPostCasEnvelope, VfsError>
```

Validate before returning:

- metadata is still bound to this write plan via root tree, changed-path count, parent state, and private plan fingerprint;
- visibility repo/commit/ref/version matches metadata and this plan's source parent state;
- committed response status code is in `100..=599`;
- all mismatch errors are fixed redacted `CorruptStore` messages.

**Step 3: Add completion method**

On `DurableCoreCommitPostCasEnvelope`, add:

```rust
pub(crate) async fn complete(
    &self,
    workspaces: &dyn WorkspaceMetadataStore,
    audit: &dyn AuditStore,
    idempotency: &dyn IdempotencyStore,
) -> DurableCorePostCasOutcome
```

Completion order:

1. If `workspace_id` is present, update workspace head to `commit_id.to_hex()`.
2. Append the supplied audit event.
3. If an idempotency reservation is present, complete it with the committed response.

Failure policy:

- Workspace-head failure returns partial `WorkspaceHeadUpdate`, does not append audit, and attempts idempotency completion with a fixed redacted committed-partial response.
- Audit failure returns partial `AuditAppend`, leaves workspace head updated, and attempts idempotency completion with a fixed redacted committed-partial response.
- Idempotency completion failure returns partial `IdempotencyCompletion` after workspace/audit are complete.
- No post-CAS failure attempts to roll back refs, workspace head, audit, object metadata, or commit metadata.

**Step 4: Run GREEN**

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_completion --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_ref_cas_visibility --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
git diff --check
```

Expected: all pass.

**Step 5: Run required perf**

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record timing and memory.

## Task 5: Review And Hardening

**Files:**

- Modify if needed: `src/backend/core_transaction.rs`

Required review checks:

- Spec review: no live route cutover, no SMFS latest-wins/cache import, post-CAS failures never rollback visible refs, recovery claims are per commit/ref/step.
- Code-quality/security review: redaction, stale-token handling, lease/backoff boundary behavior, duplicate audit risk, clone/alloc behavior, lock scope.

Likely hardening tests if reviewers find gaps:

- exact backoff boundary claim behavior;
- stale token cannot record failure or poison retry;
- poisoned target cannot be completed later;
- partial responses do not include raw workspace IDs unless intentionally included, raw audit errors, commit messages, paths, or token hashes.

Run focused tests and required perf after any review-fix diff.

## Task 6: Status, Full Gates, Push, Merge

**Files:**

- Modify: `docs/project-status.md`

Update project status:

- Latest backend slice: durable post-CAS completion and recovery envelope.
- Completed section: what was built, what is still not built, verification numbers, and SMFS extraction boundary.
- Recommended next slice: guarded live durable `POST /vcs/commit` route remains next only after review confirms the envelope; otherwise take any required recovery adapter/schema hardening first.

After docs diff:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

Full v2 gates:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

Then push `v2/foundation`, merge to `main`, run main gates, and push `main`.
