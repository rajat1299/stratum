# Policy Review Audit Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make guarded durable FS, VCS, and review mutations use one route-facing policy/audit contract so protected refs/paths, review decisions, and durable audit identity behave consistently with the existing local route behavior.

**Architecture:** Add a small server-side policy module that evaluates the existing review-store protected ref/path rules and returns a typed allow/deny decision with redacted details. Keep it route-facing rather than a full service boundary, reuse the existing review/audit/idempotency stores, and extend durable audit identity so normal route audit events and recovery audit events dedupe on the same content-free mutation identity.

**Tech Stack:** Rust, Tokio, Axum, existing `ReviewStore`, `AuditStore`, `IdempotencyStore`, guarded durable commit/mutation route capability, Postgres-backed control-plane stores behind the existing `postgres` feature.

---

## Context

Current local route behavior already enforces protected refs and paths through helpers in `src/server/routes_fs.rs` and `src/server/routes_vcs.rs`. Review routes compute approval decisions in `src/review.rs` and audit mutation success paths in `src/server/routes_review.rs`. Guarded durable mounted-session FS mutations now write durable session refs and enqueue post-visible recovery, but the normal route audit event does not yet carry the same durable operation identity that recovery uses. Policy denials and allows are not first-class audit events, so durable/local parity is still inferred from scattered route checks.

Do not add a distributed policy service in this slice. The target seam is a local module that can later become the service boundary.

Preserve default local runtime behavior when `STRATUM_DURABLE_COMMIT_ROUTE` is absent. Keep all audit details content-free: no request bodies, file content, commit messages, approval/comment/dismissal text, raw tokens, idempotency keys, database URLs, or backend error text.

## Task 1: Policy Decision Seam

**Files:**
- Create: `src/server/policy.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Test: `src/server/policy.rs`
- Test: existing route tests in `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, and `src/server/routes_review.rs`

**Step 1: Write failing policy unit tests**

Add unit tests for a small route-facing policy API:

```rust
#[tokio::test]
async fn protected_ref_policy_denies_matching_ref_with_redacted_reason() {
    // Build an InMemoryReviewStore, add a protected main rule, evaluate
    // PolicyAction::VcsCommit against target_ref "main", and assert deny.
    // Assert the reason is a stable code such as "protected_ref", not raw input.
}

#[tokio::test]
async fn protected_path_policy_matches_target_ref_and_boundary_prefix() {
    // Protect /legal for main, evaluate changed paths /legal/a.txt and
    // /legalese/a.txt. Assert only the boundary match is denied.
}

#[tokio::test]
async fn policy_decision_details_are_bounded_and_content_free() {
    // Evaluate with more paths than the detail cap and assert details contain
    // counts/codes, not every path or body-like values.
}
```

Run:

```bash
cargo test --locked server::policy --lib -- --nocapture
```

Expected: fail because `src/server/policy.rs` does not exist.

**Step 2: Add policy types**

Create `src/server/policy.rs` with:

- `RoutePolicyAction` for `FsWrite`, `FsMkdir`, `FsDelete`, `FsCopy`, `FsMove`, `FsMetadataUpdate`, `VcsCommit`, `VcsRevert`, `VcsRefCreate`, `VcsRefUpdate`, `ReviewMerge`, and `ReviewReject`.
- `RoutePolicyRequest` carrying action, actor UID/username, workspace id/root/session ref when present, workspace scope, target ref, changed paths, and optional request correlation.
- `RoutePolicyDecision` as `Allow` or `Deny { reason: RoutePolicyDenyReason }`.
- Redacted detail helpers: action, decision, reason code, target ref, changed path count, matched ref-rule count, matched path-rule count, workspace id if present, and correlation/idempotency presence only.

Use the existing `ReviewStore` rules:

- Protected ref rules match exact target ref and deny direct `VcsCommit`, `VcsRevert`, `VcsRefUpdate`, and `ReviewReject` if applicable.
- Protected path rules match boundary-aware changed paths for mutating FS routes, revert changed paths, and review merge path decisions.
- `ReviewMerge` should allow protected refs/paths when the review approval decision is approved; it should still emit an allow policy event.
- `VcsRefCreate` is audited as a policy decision but should only deny if future rules make it relevant; do not invent new protected-ref semantics for creating unrelated refs.

Do not include path lists in audit details. Keep path counts and matched rule counts.

**Step 3: Wire module**

Add `pub(crate) mod policy;` in `src/server/mod.rs`.

Replace local protected-rule helpers in routes with calls to the new seam while keeping route-visible errors unchanged:

- `routes_fs.rs`: replace `require_unprotected_paths*` internals with `policy::evaluate_route_policy`.
- `routes_vcs.rs`: replace `require_unprotected_ref` and `require_unprotected_revert_paths` internals with the policy seam.
- `routes_review.rs`: call the policy seam around merge/reject decisions after loading the change request and before mutation.

Keep helper wrappers if that minimizes churn; the important change is one decision implementation.

**Step 4: Verify focused tests**

Run:

```bash
cargo test --locked server::policy --lib -- --nocapture
cargo test --locked server::routes_fs::tests::protected_path_rules_block_direct_http_writes --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::protected_ref_rules_block_direct_vcs_mutations --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::protected_path_revert_is_blocked_before_idempotency_replay_without_mutation_or_audit --lib -- --nocapture
cargo test --locked server::routes_review::tests::approval_protected_ref_rule_blocks_merge_until_approved --lib -- --nocapture
cargo test --locked server::routes_review::tests::approval_protected_path_rule_blocks_merge_until_approved --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/server/policy.rs src/server/mod.rs src/server/routes_fs.rs src/server/routes_vcs.rs src/server/routes_review.rs
git commit -m "feat: add route policy decision seam"
```

## Task 2: Policy Decision Audit

**Files:**
- Modify: `src/audit.rs`
- Modify: `src/server/policy.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/backend/postgres.rs` if new audit actions need mapping coverage
- Test: `src/server/routes_fs.rs`
- Test: `src/server/routes_vcs.rs`
- Test: `src/server/routes_review.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Write failing audit tests**

Add route tests proving:

- A protected durable `PUT /fs/...` denial emits one policy-deny audit event and no mutation audit event.
- An allowed guarded durable `PUT /fs/...` emits a policy-allow audit event before the mutation audit event.
- A protected `POST /vcs/commit` denial emits one policy-deny audit event and no VCS commit audit event.
- A review merge blocked for missing approvals emits a policy-deny audit event with `change_request_id`, target ref, and counts, but no review merge audit event.
- Idempotency replay of an allowed mutation does not append a second policy decision event.

Run focused tests and confirm failure because no policy audit exists yet.

**Step 2: Add audit actions and helper**

In `src/audit.rs`, add:

- `AuditAction::PolicyDecisionAllow`
- `AuditAction::PolicyDecisionDeny`
- `AuditResourceKind::PolicyDecision`

In `src/server/policy.rs`, add a helper that converts `RoutePolicyDecision` into a `NewAuditEvent` with:

- actor/workspace from the current session
- action `policy_decision_allow` or `policy_decision_deny`
- resource kind `PolicyDecision`
- details: route action, decision, reason code when denied, target ref, changed path count, matched counts, change request id when applicable, operation correlation when present, and `idempotency_present=true` only

The helper must not include raw idempotency keys, request payloads, commit messages, review comments, approval comments, dismissal reasons, or file contents.

**Step 3: Append policy audit in routes**

For mutating routes that evaluate policy:

- On deny: append policy-deny audit before returning `403`; if audit append fails, return the same mutation-not-committed audit-failure shape currently used by routes.
- On allow: append policy-allow before idempotency reservation for routes where a replay should not duplicate policy audit, or include policy audit in the idempotency-protected execution section and ensure replay bypasses append. Keep existing replay behavior: stored responses are only returned after current auth/policy checks pass.

Use the safer ordering per route:

- FS/VCS direct denials happen before idempotency reservation today; policy-deny audit can happen there.
- FS/VCS allow audit should happen after current auth and protected checks but before the durable/local mutation.
- Review merge/reject allow/deny should happen after loading the change request and approval state, before transition/ref update.

If this creates duplicate allow audit on idempotent replay, move allow append after `begin_*_idempotency` returns `Execute` and before mutation.

**Step 4: Add Postgres mapping coverage**

Because audit actions are stored as text, add/update Postgres adapter tests so new actions round-trip through `audit_events` and `audit_action_from_db`.

Run:

```bash
cargo test --locked audit --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::protected_ref_rules_block_direct_vcs_mutations --lib -- --nocapture
cargo test --locked server::routes_review::tests::approval_protected_ref_rule_blocks_merge_until_approved --lib -- --nocapture
cargo test --locked --features postgres backend::postgres::tests::audit --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/audit.rs src/server/policy.rs src/server/routes_fs.rs src/server/routes_vcs.rs src/server/routes_review.rs src/backend/postgres.rs
git commit -m "feat: audit route policy decisions"
```

## Task 3: Durable FS Audit Identity And Recovery Dedupe

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/audit.rs`
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/postgres.rs`
- Test: `src/server/routes_fs.rs`
- Test: `src/audit.rs`
- Test: `src/backend/core_transaction.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Write failing tests**

Add tests:

```rust
#[tokio::test]
async fn guarded_durable_fs_normal_audit_carries_recovery_identity() {
    // Perform a successful guarded durable PUT and assert the normal FS audit
    // event contains operation_id, target_ref, previous_commit, new_commit,
    // and changed_path_count, with no body content.
}

#[tokio::test]
async fn guarded_durable_fs_recovery_dedupes_against_normal_route_audit_after_idempotency_failure() {
    // Force idempotency completion failure after normal audit append, run FS
    // mutation recovery, and assert audit event count remains one.
}
```

Run:

```bash
cargo test --locked server::routes_fs::tests::guarded_durable_fs_normal_audit_carries_recovery_identity --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable_fs_recovery_dedupes_against_normal_route_audit_after_idempotency_failure --lib -- --nocapture
```

Expected: first fails because normal route audit lacks full durable identity; second may duplicate or only pass through recovery-specific setup.

**Step 2: Extend the normal route audit seed**

Extend `DurableFsAuditRecoverySeed` in `src/server/routes_fs.rs` or replace it with a small helper that can enrich normal audit events when `DurableFsMutationRecoveryObservation` exists.

Add details to normal FS mutation audit events for durable guarded mutations:

- `operation_id`: the audit recovery target operation id, not the raw idempotency key
- `target_ref`
- `previous_commit`
- `new_commit`
- `changed_path_count`
- `changed_paths_truncated` only when applicable

Use the same operation id that `DurableFsMutationAuditRecoveryContext` stores for the audit recovery claim. Do not include changed path names beyond the event resource path already used by local route audit.

**Step 3: Align dedupe matching**

Keep `AuditStore::contains_fs_mutation_recovery_event` matching on action, operation id, target ref, and new commit. Confirm normal route audit now satisfies that contract.

If needed, adjust `durable_fs_mutation_audit_event` in `src/backend/core_transaction.rs` so recovery and normal route audit use the same details and resource kind consistently.

**Step 4: Verify**

Run:

```bash
cargo test --locked audit --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/server/routes_fs.rs src/audit.rs src/backend/core_transaction.rs src/backend/postgres.rs
git commit -m "fix: bind durable fs audit identity"
```

## Task 4: Review Mutation Audit Parity

**Files:**
- Modify: `src/server/routes_review.rs`
- Modify: `src/audit.rs` if helper/action details need extension
- Modify: `src/backend/postgres.rs` if durable audit round-trip coverage needs extension
- Test: `src/server/routes_review.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Write failing tests**

Add or extend route tests to assert durable/local audit details for:

- approval creation
- duplicate approval replay
- approval dismissal
- reviewer assignment create/update
- review comment creation
- merge success
- reject success

Required assertions:

- one mutation audit event per successful mutation, no second event on same-key replay
- stable request correlation details: `change_request_id`, mutation id (`approval_id`, `comment_id`, `assignment_id`, or `target_ref_version`), `head_commit` where relevant, `idempotency_present=true` when supplied
- no approval comments, review comment bodies, dismissal reasons, change-request descriptions, request body text, or raw idempotency keys in serialized audit events

Run:

```bash
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

Expected: some correlation fields are missing.

**Step 2: Add review audit helper**

In `routes_review.rs`, add a local helper or use a helper from `src/server/policy.rs` if cleaner:

- `review_audit_event(session, action, resource)`
- `.with_detail("change_request_id", ...)`
- `.with_detail("head_commit", ...)` for approval/comment/merge where available
- `.with_detail("idempotency_present", "true")` only if a reservation exists
- `.with_detail("idempotency_scope", route constant)` is allowed; raw key hash is not necessary for public audit

Do not add request text fields.

**Step 3: Apply to review routes**

Update:

- `create_change_request_approval`
- `dismiss_change_request_approval`
- `assign_change_request_reviewer`
- `create_change_request_comment`
- `merge_change_request`
- `reject_change_request`

Preserve terminal-state replay ordering: a same-key replay after merge/reject must return the stored response; a new mutation after terminal state must be rejected without new audit.

**Step 4: Verify focused tests**

Run:

```bash
cargo test --locked server::routes_review::tests::approval_create_and_list_records_with_audit_redaction --lib -- --nocapture
cargo test --locked server::routes_review::tests::review_feedback_comment_create_and_list_with_audit_redaction --lib -- --nocapture
cargo test --locked server::routes_review::tests::review_feedback_dismiss_approval_recomputes_state_and_redacts_audit_reason --lib -- --nocapture
cargo test --locked server::routes_review::tests::approval_workflow_merge_idempotency_replays_after_already_merged --lib -- --nocapture
cargo test --locked server::routes_review::tests::reject_change_request_idempotency_replays_after_status_changes --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/server/routes_review.rs src/audit.rs src/backend/postgres.rs
git commit -m "feat: align review mutation audit"
```

## Task 5: Durable Review And Protected-Change Parity

**Files:**
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/core.rs`
- Modify: `src/review.rs` if approval decision needs commit-source abstraction
- Test: `src/server/routes_review.rs`
- Test: `src/server/routes_vcs.rs`
- Test: `src/server/routes_fs.rs`

**Step 1: Write failing parity tests**

Add tests for these acceptance cases:

- A protected path cannot be bypassed by guarded durable mounted-session mutation followed by guarded durable commit promotion.
- A guarded durable `PATCH /vcs/refs/{name}` to a protected ref is denied through the shared policy seam and audited.
- A guarded durable review merge can use durable commit IDs/session refs without requiring the local `.vfs/state.bin` to contain the source/head commits.
- A review reject against a protected target emits policy audit and review audit, but does not alter refs.

Run:

```bash
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

Expected: the durable review merge test should fail if `merge_change_request` still depends only on `state.db` refs/commit ancestry.

**Step 2: Add durable-aware review merge lookup**

Keep full durable status/diff/revert out of scope. For `merge_change_request` only, add a small helper that:

- Uses guarded durable `RefStore`/`CommitStore` when the guarded capability is active and both source/target refs exist there.
- Falls back to existing `state.db` behavior for local runtime.
- Checks source ref equals recorded `head_commit`.
- Checks target ref equals recorded `base_commit`.
- Checks recorded head descends from recorded base using durable commit parent metadata in guarded durable mode, or existing local DB ancestry in local mode.
- Performs target ref CAS with source freshness using the durable `update_ref_if_source_matches` equivalent if already available; otherwise use the existing guarded durable route ref update helper.

Do not require local `.vfs/state.bin` to know durable commit IDs for this merge path.

**Step 3: Ensure protected-change parity**

Use the shared policy seam for:

- durable FS mutations before mutation
- guarded durable commit promotion before durable write plan/CAS
- guarded durable ref update before CAS
- local revert before mutation
- review merge before ref CAS
- review reject before status transition if target ref/path protection applies

Review reject does not change refs, so protected ref rules should be audited as a review decision but should not be blocked solely because target ref is protected unless the product rule explicitly says rejecting a change is blocked. Keep the current semantics and document the chosen behavior in tests.

**Step 4: Verify focused tests**

Run:

```bash
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/server/routes_review.rs src/server/routes_vcs.rs src/server/routes_fs.rs src/server/core.rs src/review.rs
git commit -m "feat: align durable review protected changes"
```

## Task 6: Route Tests, Docs, And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: route test modules touched by this slice

**Step 1: Add final route test coverage**

Add one explicit regression test per acceptance criterion if not already covered:

- Durable FS mutation audit recovery does not duplicate audit after idempotency failure.
- Protected ref/path rules apply to durable FS mutation, durable commit promotion, ref update, revert, and review merge.
- Policy allow/deny decisions are audited and redacted.
- Review route mutations emit durable audit with idempotent replay behavior.
- Existing local runtime protected-change tests still pass.

**Step 2: Update docs**

Update `docs/http-api-guide.md`:

- Mention policy decision audit events in the audit section.
- Mention durable FS audit identity details: operation id, target ref, previous commit, new commit, changed path count.
- Clarify that audit details remain content-free and do not contain raw idempotency keys.
- Clarify guarded durable review merge behavior with durable refs/commit IDs if implemented.

Update `docs/project-status.md`:

- Add a "Policy Review Audit Parity" completed slice section.
- Move the previous "Recommended Next Slices" forward as historical context only, and update residuals honestly.
- Record focused and full verification results after gates complete.

**Step 3: Verification gates**

Run in this order:

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md src/server/routes_fs.rs src/server/routes_vcs.rs src/server/routes_review.rs src/server/policy.rs src/audit.rs src/backend/postgres.rs
git commit -m "docs: record policy review audit parity"
```

## Task 7: Reviews, Publish, And Merge

**Files:**
- No planned code edits unless review findings require fixes.

**Step 1: Spec/correctness review**

Dispatch a `gpt-5.5` high reasoning subagent for:

- protected ref/path parity across FS, VCS, review, durable guarded routes
- durable audit identity and recovery dedupe correctness
- idempotency replay ordering
- no local `.vfs/state.bin` dependency for durable review merge if implemented

Fix findings locally, then rerun focused tests for the touched area.

**Step 2: Code-quality/security review**

Dispatch a `gpt-5.5` high reasoning subagent for:

- audit/log/error redaction
- raw secret/body/idempotency-key leakage
- race windows introduced by policy audit ordering
- Postgres adapter compatibility
- unnecessary abstractions or clone-heavy Rust

Fix findings locally, then rerun focused tests and clippy as needed.

**Step 3: Final verification**

Rerun the full required verification gates from Task 6 after all review fixes.

**Step 4: Push and merge**

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git fetch origin
git merge --ff-only origin/v2/foundation
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --tests
git push origin main
```

Preserve unrelated main worktree changes (`site/index.html` and `.claude/`). If `main` cannot fast-forward because of unrelated local state, stop and resolve without reverting user files.
