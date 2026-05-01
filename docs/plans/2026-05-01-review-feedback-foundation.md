# Review Feedback Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add durable change-request comments and approval dismissal so protected review flows can express feedback and revoke approvals without building the full review UI or reviewer assignment system.

**Architecture:** Extend the existing local/file-backed `ReviewStore` with first-class review comments and approval dismissal metadata. Keep merge enforcement based on active approvals only, so dismissing an approval immediately affects computed `approval_state`. Expose small admin-gated HTTP endpoints with existing idempotency and audit patterns; store review text on review records but keep audit details metadata-only.

**Tech Stack:** Rust 2024, async-trait review store, Axum route handlers, existing local `wincode` codec persistence, existing HTTP idempotency helpers, local audit event scaffolding.

---

## Product Scope

Build this slice:

- Durable change-request comments with:
  - `kind`: `general` or `changes_requested`
  - author UID
  - optional normalized path
  - trimmed bounded body text
  - active flag and version
- Approval dismissal:
  - marks an active approval inactive
  - records `dismissed_by` and optional trimmed bounded dismissal reason on the approval record
  - is idempotent when the approval is already inactive
  - immediately removes that approval from computed approval counts
- HTTP endpoints:
  - `GET /change-requests/{id}/comments`
  - `POST /change-requests/{id}/comments`
  - `POST /change-requests/{id}/approvals/{approval_id}/dismiss`
- Metadata-only audit events for comment creation and approval dismissal. Do not put comment bodies or dismissal reasons in audit details.
- Local review-store v3 persistence with v1/v2 migrations.

Do not build in this slice:

- reviewer groups, reviewer assignment, required named approvers, code owners, approval dismissal permissions beyond current admin-gated review routes, submit/resubmit state, comment threading/replies, comment resolution, hunk/file-level decisions, notifications, web UI, or distributed policy transactions.

## Task 1: Plan Commit

**Files:**
- Create: `docs/plans/2026-05-01-review-feedback-foundation.md`

**Step 1: Commit this plan**

Run:

```bash
git add docs/plans/2026-05-01-review-feedback-foundation.md
git commit -m "docs: plan review feedback foundation"
```

Expected: docs-only commit.

## Task 2: Review Store Comments And Approval Dismissal

**Files:**
- Modify: `src/review.rs`

**Step 1: Write failing store tests**

Add tests named with `review_feedback` for:

- in-memory store creates and lists comments for a change request.
- comment body is trimmed; empty bodies are rejected.
- comment path is optional and normalized with the existing path-prefix validator.
- comment for an unknown change request fails.
- local store reloads comments.
- local store migrates v1 and v2 persisted review stores to v3 with empty comments and current approval fields.
- dismissing an active approval marks it inactive, records `dismissed_by`, and removes it from approval counts.
- duplicate dismissal returns the existing inactive approval with `dismissed: false`.
- dismissal for an unknown approval or wrong change request fails.
- corrupt v3 stores reject comments pointing at unknown change requests, empty comment bodies, invalid comment paths, approvals with invalid dismissal metadata, and duplicate active approval identities.

Run:

```bash
cargo test --locked review::tests::review_feedback -- --nocapture
```

Expected: fail before implementation.

**Step 2: Implement store models**

In `src/review.rs`, add:

- `pub enum ReviewCommentKind { General, ChangesRequested }`
- `pub struct ReviewComment { id, change_request_id, author, body, path, kind, active, version }`
- `pub struct NewReviewComment { change_request_id, author, body, path, kind }`
- `pub struct ReviewCommentMutation { pub comment: ReviewComment, pub created: bool }`
- `pub struct DismissApprovalInput { pub change_request_id: Uuid, pub approval_id: Uuid, pub dismissed_by: Uid, pub reason: Option<String> }`
- `pub struct ApprovalDismissalMutation { pub record: ApprovalRecord, pub dismissed: bool }`

Extend `ApprovalRecord` with:

- `dismissed_by: Option<Uid>`
- `dismissal_reason: Option<String>`

Validation rules:

- comments require an existing change request.
- comment body is trimmed, non-empty, and capped at a conservative byte length.
- optional comment path uses `normalize_path_prefix`.
- dismissal requires the approval exists and belongs to the supplied change request.
- dismissal reason is optional, trimmed, capped, and empty becomes `None`.
- active approval dismissal sets `active = false`, records dismissal metadata, and increments approval version.
- already inactive approval dismissal returns `dismissed: false` without mutating.
- corrupt persisted approvals must not have dismissal metadata while active and must have `dismissed_by` when inactive.

Extend `ReviewStore` with:

- `create_comment(&self, input: NewReviewComment) -> Result<ReviewCommentMutation, VfsError>`
- `list_comments(&self, change_request_id: Uuid) -> Result<Vec<ReviewComment>, VfsError>`
- `dismiss_approval(&self, input: DismissApprovalInput) -> Result<ApprovalDismissalMutation, VfsError>`

Persistence:

- Add `comments: BTreeMap<Uuid, ReviewComment>` to `ReviewState`.
- Bump `REVIEW_STORE_VERSION` to `3`.
- Decode v3 normally.
- Decode v2 stores through a v2 persisted struct with old approval shape, migrating comments to empty and dismissal fields to `None`.
- Keep v1 migration to empty approvals and comments.

**Step 3: Verify store tests pass**

Run:

```bash
cargo test --locked review::tests::review_feedback -- --nocapture
```

Expected: pass.

**Step 4: Commit**

```bash
git add src/review.rs
git commit -m "feat: add review comments and approval dismissal"
```

## Task 3: HTTP Review Feedback Endpoints

**Files:**
- Modify: `src/audit.rs`
- Modify: `src/server/routes_review.rs`

**Step 1: Write failing route tests**

Add tests named with `review_feedback` for:

- `POST /change-requests/{id}/comments` creates a comment and emits one audit event without body text.
- `GET /change-requests/{id}/comments` lists comments.
- comment creation idempotency replays without a second audit event.
- empty comment bodies are rejected without audit.
- `POST /change-requests/{id}/approvals/{approval_id}/dismiss` marks the approval inactive, emits one audit event without reason text, and recomputes `approval_state`.
- duplicate dismissal with a different idempotency key returns `dismissed: false`.
- merge is blocked after dismissing the only required approval.
- wrong change-request/approval pairing returns `404` or `400` without mutation.

Run:

```bash
cargo test --locked server::routes_review::tests::review_feedback -- --nocapture
```

Expected: fail before route implementation.

**Step 2: Add audit variants**

In `src/audit.rs`, add:

- `AuditAction::ChangeRequestCommentCreate`
- `AuditAction::ChangeRequestApprovalDismiss`
- `AuditResourceKind::ReviewComment`

Do not add comment body or dismissal reason to audit details.

**Step 3: Implement routes**

In `src/server/routes_review.rs`:

- Add request types:
  - `CreateReviewCommentRequest { body, path, kind }`
  - `DismissApprovalRequest { reason }`
- Add routes:
  - `GET /change-requests/{id}/comments`
  - `POST /change-requests/{id}/comments`
  - `POST /change-requests/{id}/approvals/{approval_id}/dismiss`
- Use `require_admin`, `begin_review_idempotency`, `complete_review_idempotency`, and `abort_review_idempotency`.
- Comment response shape:

```json
{
  "comment": { ... },
  "created": true,
  "approval_state": { ... }
}
```

- Comment list response shape:

```json
{
  "comments": [ ... ],
  "approval_state": { ... }
}
```

- Dismissal response shape:

```json
{
  "approval": { ... },
  "dismissed": true,
  "approval_state": { ... }
}
```

**Step 4: Verify route tests pass**

Run:

```bash
cargo test --locked server::routes_review::tests::review_feedback -- --nocapture
cargo test --locked server::routes_review::tests -- --nocapture
```

Expected: pass.

**Step 5: Commit**

```bash
git add src/audit.rs src/server/routes_review.rs
git commit -m "feat: expose review feedback endpoints"
```

## Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP docs**

Document:

- `GET/POST /change-requests/{id}/comments`
- `POST /change-requests/{id}/approvals/{approval_id}/dismiss`
- comment/dismissal response shapes
- audit redaction for comment bodies and dismissal reasons
- merge behavior after dismissal

**Step 2: Update project status**

Move durable CR comments and approval dismissal into built status. Keep residual risks explicit:

- no reviewer groups/assignment
- no submit/resubmit state
- no comment threading/replies/resolution
- no web review UI
- no distributed policy engine
- no protected-change enforcement outside HTTP route-level gates

**Step 3: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: update review feedback status"
```

## Task 5: Review And Full Verification

**Files:**
- Review all changed files.

**Step 1: Request reviews**

Dispatch:

- Spec/security/API reviewer: comment/dismissal semantics, idempotency, audit redaction, merge enforcement after dismissal.
- Code-quality/correctness reviewer: review-store v3 migration, validation, route response shape, tests.

**Step 2: Fix findings**

Commit fixes separately if needed:

```bash
git commit -m "fix: address review feedback findings"
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
