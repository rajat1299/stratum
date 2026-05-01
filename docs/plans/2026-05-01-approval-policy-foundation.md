# Approval Policy Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add first-class approval records and enforce approval counts for protected change-request merges.

**Architecture:** Keep direct protected-path/ref enforcement unchanged: direct HTTP filesystem and VCS mutations still return "requires change request merge." Add approval records to the local review store, compute approval requirements from active protected ref rules and protected path rules matching the change request's changed paths, and gate `POST /change-requests/{id}/merge` before the target ref compare-and-swap. Persist approval records locally with a review-store v2 migration; policy decisions are computed on read/merge and are not persisted as an audit pipeline yet.

**Tech Stack:** Rust 2024, Axum route handlers, existing `ReviewStore`, local `wincode` codec persistence, `StratumDb` VCS/ref helpers, metadata-only local audit events, existing HTTP idempotency helpers.

---

## Product Scope

Build this slice:

- Approval records with optional comments, bound to a change request and its recorded `head_commit`.
- Duplicate approvals by the same approver for the same change request/head are idempotent at the store layer and do not double count.
- Approval policy decisions computed from:
  - active protected ref rules matching the change request target ref,
  - active protected path rules whose optional `target_ref` is absent or matches the change request target ref, and whose path prefix matches a changed path between `base_commit` and `head_commit`.
- Effective required approvals is the maximum `required_approvals` across matched rules.
- `POST /change-requests/{id}/merge` returns `403 Forbidden` when approvals are insufficient.
- Change-request JSON includes computed approval state so clients can explain why a merge is blocked.
- Approval mutations are admin-gated, idempotent, and audited without persisting approval comments in audit details.

Do not build in this slice:

- approval groups, reviewer assignment, approval dismissal, comments API beyond approval comments, merge queue, non-fast-forward merge, path-level approve/reject, web UI, or distributed policy transactions.
- persisted policy-decision/audit pipeline for every policy evaluation.
- enforcement in MCP, CLI, POSIX/FUSE, or embedded `StratumDb` direct callers.

## Task 1: Plan Commit

**Files:**
- Create: `docs/plans/2026-05-01-approval-policy-foundation.md`

**Step 1: Commit this plan**

Run:

```bash
git add docs/plans/2026-05-01-approval-policy-foundation.md
git commit -m "docs: plan approval policy foundation"
```

Expected: docs-only commit.

## Task 2: Review Store Approval Model

**Files:**
- Modify: `src/review.rs`

**Step 1: Write failing store tests**

Add tests for:

- in-memory store creates and lists approval records for a change request.
- duplicate approval for the same `(change_request_id, head_commit, approved_by)` returns the existing record and does not double count.
- approval for an unknown change request fails.
- approval with a stale/nonmatching head commit fails.
- local store reloads approvals.
- local store loads existing v1 review stores with empty approvals.
- corrupt v2 store rejects approvals pointing at unknown change requests or invalid commit hex.

Run:

```bash
cargo test --locked review::tests::approval -- --nocapture
```

Expected: fail because approval records do not exist yet.

**Step 2: Implement approval structs and store APIs**

In `src/review.rs`, add:

- `pub struct ApprovalRecord { id: Uuid, change_request_id: Uuid, head_commit: String, approved_by: Uid, comment: Option<String>, active: bool, version: u64 }`
- `pub struct NewApprovalRecord { change_request_id: Uuid, head_commit: String, approved_by: Uid, comment: Option<String> }`
- `pub struct ApprovalRecordMutation { pub record: ApprovalRecord, pub created: bool }`
- `pub struct ApprovalPolicyDecision { pub change_request_id: Uuid, pub required_approvals: u32, pub approval_count: u32, pub approved_by: Vec<Uid>, pub approved: bool, pub matched_ref_rules: Vec<Uuid>, pub matched_path_rules: Vec<Uuid> }`

Extend `ReviewStore` with:

- `create_approval(&self, input: NewApprovalRecord) -> Result<ApprovalRecordMutation, VfsError>`
- `list_approvals(&self, change_request_id: Uuid) -> Result<Vec<ApprovalRecord>, VfsError>`
- `approval_decision(&self, change_request_id: Uuid, changed_paths: &[String]) -> Result<Option<ApprovalPolicyDecision>, VfsError>`

Validation rules:

- change request must exist.
- `head_commit` must equal the current change request's recorded `head_commit`.
- `approved_by` must not equal `change.created_by`.
- approval comment is optional, trimmed, and capped at a conservative byte length.
- duplicate active approval by same approver for same change/head returns `created: false`.
- only active approvals for the current `head_commit` count.

Persistence:

- Add `approvals: BTreeMap<Uuid, ApprovalRecord>` to `ReviewState`.
- Bump the persisted review store to version 2.
- Decode v2 normally.
- Decode v1 stores by using a v1 persisted struct and migrating approvals to empty.
- Validate approvals after loading; reject duplicate ids and approvals referencing missing change requests.

**Step 3: Verify store tests pass**

Run:

```bash
cargo test --locked review::tests::approval -- --nocapture
```

Expected: pass.

**Step 4: Commit**

```bash
git add src/review.rs
git commit -m "feat: add approval records to review store"
```

## Task 3: VCS Changed Paths Helper

**Files:**
- Modify: `src/vcs/mod.rs`
- Modify: `src/db.rs`
- Test: existing unit tests in `src/vcs/mod.rs` or `src/db.rs`

**Step 1: Write failing tests**

Add tests for:

- changed paths between a base commit and direct descendant head include paths changed in the descendant.
- changed paths between a base commit and later descendant include unique paths across the linear parent chain.
- unknown or non-descendant commits return an error.

Run:

```bash
cargo test --locked changed_paths_between -- --nocapture
```

Expected: fail because helper does not exist.

**Step 2: Implement helper**

Add a narrow helper that walks from `head_commit` back through `parent` until `base_commit`, collecting unique changed paths from each commit's `changed_paths`.

Expose it through `StratumDb` as:

```rust
pub async fn changed_paths_between(
    &self,
    base_commit: &str,
    head_commit: &str,
) -> Result<Vec<String>, VfsError>
```

Use full 64-character hex commit ids. Return paths as normalized strings for review-policy matching.

**Step 3: Verify tests pass**

Run:

```bash
cargo test --locked changed_paths_between -- --nocapture
```

Expected: pass.

**Step 4: Commit**

```bash
git add src/vcs/mod.rs src/db.rs
git commit -m "feat: expose changed paths between commits"
```

## Task 4: HTTP Approval Endpoints And Merge Gate

**Files:**
- Modify: `src/audit.rs`
- Modify: `src/server/routes_review.rs`
- Test: `src/server/routes_review.rs`

**Step 1: Write failing route tests**

Add tests for:

- `POST /change-requests/{id}/approvals` creates an approval and emits one audit event.
- `GET /change-requests/{id}/approvals` lists approval records.
- approval idempotency replays without appending a second audit event.
- duplicate approval with a different idempotency key returns existing approval with `created: false` and does not double count.
- self-approval is rejected.
- change-request create/get/list responses include `approval_state`.
- protected ref rule blocks merge until enough approvals exist.
- protected path rule matching changed paths blocks merge until enough approvals exist.
- stale source/target checks still run and conflict without consuming approvals.

Run:

```bash
cargo test --locked server::routes_review::tests::approval -- --nocapture
```

Expected: fail because endpoints/merge gate do not exist yet.

**Step 2: Add audit enum variants**

In `src/audit.rs`, add:

- `AuditAction::ChangeRequestApprove`
- `AuditResourceKind::ApprovalRecord`

Audit details should include only metadata:

- `approval_id`
- `change_request_id`
- `source_ref`
- `target_ref`
- `head_commit`
- `approved_by`
- `created`

Do not include approval comment text.

**Step 3: Add routes and response shaping**

In `src/server/routes_review.rs`:

- Add `GET/POST /change-requests/{id}/approvals`.
- Add an approval response body with `{ "approval": ..., "created": bool, "approval_state": ... }`.
- Replace raw `change_json` responses with change-response JSON that includes:
  - `change_request`
  - `approval_state`
- For list responses, return each change request with its computed approval state.
- Compute changed paths with `state.db.changed_paths_between(&change.base_commit, &change.head_commit).await`.
- If changed-path computation fails for an existing CR, return `409 Conflict` on merge and include a clear error in read responses.

**Step 4: Gate merge**

In `merge_change_request`, after the source/target stale checks and before `db.update_ref`, compute `approval_state`.

If `approval_state.approved` is false, abort idempotency and return `403 Forbidden` with JSON:

```json
{
  "error": "change request <id> requires <n> approval(s)",
  "approval_state": { ... }
}
```

Unprotected change requests with `required_approvals: 0` continue to merge as before.

**Step 5: Verify route tests pass**

Run:

```bash
cargo test --locked server::routes_review::tests::approval -- --nocapture
cargo test --locked server::routes_review::tests -- --nocapture
```

Expected: pass.

**Step 6: Commit**

```bash
git add src/audit.rs src/server/routes_review.rs
git commit -m "feat: enforce change request approvals"
```

## Task 5: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP docs**

Document:

- `POST /change-requests/{id}/approvals`
- `GET /change-requests/{id}/approvals`
- change-request response shape includes `approval_state`
- merge requires approvals when protected rules require them
- approval comments are stored on review records but omitted from audit details
- idempotency support for approval creation

**Step 2: Update project status**

Move approval-record foundation into built status and keep residual risks explicit:

- no groups/reviewer assignment
- no approval dismissal
- no comments thread
- no web review UI
- no distributed policy engine or durable production policy-decision audit pipeline
- protected-change enforcement outside HTTP routes still incomplete

**Step 3: Verify docs diff**

Run:

```bash
git diff --check -- docs/http-api-guide.md docs/project-status.md
```

Expected: pass.

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: update approval policy status"
```

## Task 6: Review And Full Verification

**Files:**
- Review all changed files.

**Step 1: Request reviews**

Dispatch:

- Spec/security/API reviewer: approval semantics, self-approval, idempotency, audit redaction, merge enforcement.
- Code-quality/correctness reviewer: Rust store migration, changed-path helper, route response shape, tests.

**Step 2: Fix findings**

Commit fixes separately if needed:

```bash
git commit -m "fix: address approval policy review findings"
```

**Step 3: Full verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Expected: all pass.

**Step 4: Merge and push**

After verification:

```bash
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git merge --no-ff v2/foundation
git push origin main
```

Expected: both worktrees clean; `v2/foundation` and `main` pushed.
