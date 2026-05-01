# Reviewer Assignment Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add durable reviewer assignments so change requests can require approval from specific reviewers before protected merge.

**Architecture:** Extend the existing local/file-backed `ReviewStore` with first-class reviewer assignment records and fold required reviewer approvals into `ApprovalPolicyDecision`. Expose a small admin-gated HTTP contract for assigning and listing reviewers, using the existing review idempotency and audit patterns. Keep the scope to named users by UID; do not build reviewer groups, code owners, notifications, threaded comments, or UI in this slice.

**Tech Stack:** Rust 2024, async-trait review store, Axum route handlers, existing local `wincode` codec persistence, existing HTTP idempotency helpers, local audit event scaffolding.

---

## Product Scope

Build this slice:

- Durable reviewer assignments with:
  - change request ID
  - reviewer UID
  - assigned-by UID
  - `required` flag
  - active flag
  - version
- Assignment upsert behavior:
  - creating a new active assignment returns `created: true`, `updated: false`
  - reassigning the same reviewer with the same `required` flag returns `created: false`, `updated: false`
  - reassigning the same reviewer with a different `required` flag updates the existing active assignment, increments version, and returns `created: false`, `updated: true`
- Approval policy integration:
  - required active reviewer assignments must be approved by those exact reviewer UIDs for the current captured `head_commit`
  - `ApprovalPolicyDecision` includes `required_reviewers`, `approved_required_reviewers`, and `missing_required_reviewers`
  - `approved` is true only when both the existing approval-count policy and all required reviewer assignments are satisfied
- HTTP endpoints:
  - `GET /change-requests/{id}/reviewers`
  - `POST /change-requests/{id}/reviewers`
- Metadata-only audit event for assignment creation/update. Do not store user-provided free-form text in audit details.
- Local review-store v4 persistence with v1/v2/v3 migrations.

Do not build in this slice:

- reviewer groups, code-owner matching, assignment notifications, threaded/resolved comments, required named reviewers on protected rules, UI, merge queues, delete/unassign endpoints, or distributed policy transactions.

## API Contract

Assign or update a reviewer:

```http
POST /change-requests/{id}/reviewers
Authorization: User root
Idempotency-Key: <retry-key>
Content-Type: application/json

{
  "reviewer_uid": 1,
  "required": true
}
```

`required` defaults to `true` when omitted. The route must validate the reviewer UID resolves to a known user by using `StratumDb::session_for_uid`. Unknown reviewer UIDs return `404 Not Found` without mutating review state or reserving a successful idempotency response.

Response:

```json
{
  "assignment": {
    "id": "<assignment-id>",
    "change_request_id": "<change-request-id>",
    "reviewer": 1,
    "assigned_by": 0,
    "required": true,
    "active": true,
    "version": 1
  },
  "created": true,
  "updated": false,
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [1],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [1],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": []
  }
}
```

List reviewers:

```http
GET /change-requests/{id}/reviewers
Authorization: User root
```

Response:

```json
{
  "assignments": [
    {
      "id": "<assignment-id>",
      "change_request_id": "<change-request-id>",
      "reviewer": 1,
      "assigned_by": 0,
      "required": true,
      "active": true,
      "version": 1
    }
  ],
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [1],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [1],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": []
  }
}
```

## Task 1: Plan Commit

**Files:**
- Create: `docs/plans/2026-05-01-reviewer-assignment-foundation.md`

**Step 1: Commit this plan**

Run:

```bash
git add docs/plans/2026-05-01-reviewer-assignment-foundation.md
git commit -m "docs: plan reviewer assignment foundation"
```

Expected: docs-only commit.

## Task 2: Review Store Assignment Model And Policy

**Files:**
- Modify: `src/review.rs`

**Step 1: Write failing store tests**

Add tests named with `review_assignment` for:

- in-memory store creates and lists reviewer assignments.
- duplicate assignment with the same required flag returns the existing active record with `created: false`, `updated: false`.
- duplicate assignment with a different required flag updates the same active record, increments version, and returns `created: false`, `updated: true`.
- assignment for an unknown change request fails.
- local store reloads assignments.
- local store migrates v1/v2/v3 review stores to v4 with empty assignments.
- approval decision reports required, approved, and missing reviewer UIDs.
- required reviewer assignment blocks merge approval state until that reviewer approves the current `head_commit`.
- optional reviewer assignments do not block approval state.
- corrupt v4 stores reject assignments pointing at unknown change requests, zero-version assignments, inactive assignments, and duplicate active reviewer assignments for the same change request.

Run:

```bash
cargo test --locked review::tests::review_assignment -- --nocapture
```

Expected: fail before implementation.

**Step 2: Implement store models**

In `src/review.rs`, add:

- `pub struct ReviewAssignment { id, change_request_id, reviewer, assigned_by, required, active, version }`
- `pub struct NewReviewAssignment { change_request_id, reviewer, assigned_by, required }`
- `pub struct ReviewAssignmentMutation { pub assignment: ReviewAssignment, pub created: bool, pub updated: bool }`

Extend `ApprovalPolicyDecision` with:

- `required_reviewers: Vec<Uid>`
- `approved_required_reviewers: Vec<Uid>`
- `missing_required_reviewers: Vec<Uid>`

Validation rules:

- assignment requires an existing change request.
- `reviewer` must not equal `change.created_by`.
- `version` must be non-zero.
- persisted assignments must be active in this first slice.
- duplicate active assignment identity is `(change_request_id, reviewer)`.
- required reviewer approval counts only if an active approval exists for the same change request, current `head_commit`, and reviewer UID.

Extend `ReviewStore` with:

- `assign_reviewer(&self, input: NewReviewAssignment) -> Result<ReviewAssignmentMutation, VfsError>`
- `list_reviewer_assignments(&self, change_request_id: Uuid) -> Result<Vec<ReviewAssignment>, VfsError>`

Persistence:

- Add `assignments: BTreeMap<Uuid, ReviewAssignment>` to `ReviewState`.
- Bump `REVIEW_STORE_VERSION` to `4`.
- Decode v4 normally.
- Decode v3 through a v3 persisted struct and migrate assignments to empty.
- Keep v2 and v1 migrations.

**Step 3: Verify store tests pass**

Run:

```bash
cargo test --locked review::tests::review_assignment -- --nocapture
```

Expected: pass.

**Step 4: Commit**

```bash
git add src/review.rs
git commit -m "feat: add reviewer assignments to review store"
```

## Task 3: HTTP Reviewer Assignment Endpoints

**Files:**
- Modify: `src/audit.rs`
- Modify: `src/server/routes_review.rs`

**Step 1: Write failing route tests**

Add tests named with `review_assignment` for:

- `POST /change-requests/{id}/reviewers` creates a required reviewer assignment and emits one metadata-only audit event.
- `GET /change-requests/{id}/reviewers` lists assignments and includes `approval_state`.
- assignment idempotency replays without a second audit event.
- duplicate assignment with a different idempotency key and same required flag returns `created: false`, `updated: false`.
- reassigning same reviewer with a different required flag returns `created: false`, `updated: true`.
- unknown reviewer UID returns `404` without mutation or audit.
- required assigned reviewer blocks merge until that reviewer approves.
- approval by another user satisfies numeric approval count but does not satisfy the required reviewer list.
- optional reviewer assignment does not block merge.

Run:

```bash
cargo test --locked server::routes_review::tests::review_assignment -- --nocapture
```

Expected: fail before route implementation.

**Step 2: Add audit variants**

In `src/audit.rs`, add:

- `AuditAction::ChangeRequestReviewerAssign`
- `AuditResourceKind::ReviewAssignment`

Audit details should include assignment ID, change request ID, reviewer UID, assigned-by UID, required flag, created, updated, active, and version. They should not include any free-form request body text.

**Step 3: Implement routes**

In `src/server/routes_review.rs`:

- Add request type:
  - `AssignReviewerRequest { reviewer_uid: Uid, required: Option<bool> }`
- Add routes:
  - `GET /change-requests/{id}/reviewers`
  - `POST /change-requests/{id}/reviewers`
- Use `require_admin`, `get_change_or_404`, `begin_review_idempotency`, `complete_review_idempotency`, and `abort_review_idempotency`.
- Validate reviewer existence with `state.db.session_for_uid(req.reviewer_uid).await`.
- Assignment response shape:

```json
{
  "assignment": { ... },
  "created": true,
  "updated": false,
  "approval_state": { ... }
}
```

- Assignment list response shape:

```json
{
  "assignments": [ ... ],
  "approval_state": { ... }
}
```

**Step 4: Verify route tests pass**

Run:

```bash
cargo test --locked server::routes_review::tests::review_assignment -- --nocapture
```

Expected: pass.

**Step 5: Commit**

```bash
git add src/audit.rs src/server/routes_review.rs
git commit -m "feat: expose reviewer assignment endpoints"
```

## Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP API docs**

Document:

- `POST /change-requests/{id}/reviewers`
- `GET /change-requests/{id}/reviewers`
- request/response JSON
- required reviewer impact on `approval_state`
- idempotency and audit behavior

**Step 2: Update project status**

Update:

- latest completed slice
- change-request/review capability section
- residual risks/not-built list
- relevant commits
- verification notes after final verification passes

**Step 3: Commit docs/status**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: update reviewer assignment status"
```

## Task 5: Review, Verification, Merge, And Push

**Step 1: Run review agents**

Request two independent read-only reviews:

- spec/security/API review
- Rust correctness/code-quality review

Review focus:

- required-reviewer semantics cannot be bypassed by numeric approval count
- merge uses current `approval_state`
- idempotency and audit behavior are metadata-only
- migrations preserve v1/v2/v3 review stores
- no reviewer assignment UI/group/code-owner scope leaked into this slice

**Step 2: Fix findings separately**

If review finds real issues:

```bash
git add <files>
git commit -m "fix: address reviewer assignment review findings"
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

**Step 4: Push and merge**

Run:

```bash
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git fetch origin
git merge --no-ff v2/foundation -m "Merge branch 'v2/foundation' into main"
git push origin main
```

Expected:

- `v2/foundation` is pushed.
- `main` receives a merge commit.
- both worktrees are clean and synced with origin.
