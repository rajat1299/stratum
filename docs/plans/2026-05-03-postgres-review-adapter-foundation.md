# Postgres Review Adapter Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a feature-gated Postgres-backed `ReviewStore` adapter that proves the durable protected-change and review schema can preserve the current review contract without wiring `stratum-server` to Postgres.

**Architecture:** Reuse the existing protected-rule, change-request, approval, review-comment, and reviewer-assignment tables from `migrations/postgres/0001_durable_backend_foundation.sql` and implement `ReviewStore` for `PostgresMetadataStore`. The current `ReviewStore` trait has no repo parameter, while `change_requests` are repo-scoped in Postgres, so this foundation stores review rows under `RepoId::local()` and documents that future hosted runtime work must add a repo-aware review domain. Keep all work behind the existing `postgres` feature and preserve local runtime behavior.

**Tech Stack:** Rust 2024, `async-trait`, `tokio-postgres`, existing `ReviewStore` trait, existing `PostgresMetadataStore`, live Postgres tests gated by `STRATUM_POSTGRES_TEST_URL`.

---

## Execution Notes

This plan is intended for rate-limit execution by a smaller model. Make small commits after each task. Do not push. Return the commit list, verification commands, verification results, and any design notes or blockers.

Before implementing Rust changes, read:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Keep `docs/project-status.md` current, but preserve unrelated SDK/DX lane content if it changed before or during this slice.

## Scope Boundaries

In scope:

- Expose minimal crate-private review-domain constructors and validators needed by durable adapters.
- Implement `ReviewStore` for `PostgresMetadataStore`.
- Store all Postgres review rows under `RepoId::local()` until the review domain is repo-aware.
- Add focused live Postgres tests for protected ref/path rules, change requests, approval decisions, duplicate approvals, reviewer assignment updates, review comments, approval dismissal, terminal transition behavior, and corrupt-row rejection.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

Out of scope:

- No `stratum-server`, HTTP, MCP, CLI, FUSE, or `StratumDb` runtime cutover to Postgres.
- No schema migration unless a failing test proves the current schema cannot satisfy the current `ReviewStore` contract.
- No reviewer groups, threaded comments, resolved comments, merge queue, review UI, notification system, or code-owner model.
- No distributed policy engine or cross-store transaction boundary.
- No repo-aware public API shape; use `RepoId::local()` only in this adapter foundation.
- No changes to HTTP response shape or route behavior.

Security and correctness posture:

- Do not log approval comments, review comment bodies, dismissal reasons, connection strings, request bodies, file contents, raw tokens, or idempotency keys.
- Postgres error contexts must stay generic and must not include sensitive review text.
- Row decoders must reject corrupt domain rows with `VfsError::CorruptStore`, not silently create invalid domain records.
- Mutations that depend on an open change request must lock the change-request row in the same transaction as the insert/update.
- Duplicate active approval behavior must match the local store: same approver plus same change request plus same head returns the existing active approval with `created: false`.
- Dismissed approvals must stop counting immediately, and a new active approval by the same approver for the same head is allowed after dismissal.

## Task 1: Expose Minimal Review Domain Helpers

**Files:**

- Modify: `src/review.rs`

**Step 1: Make existing methods crate-private**

Change only visibility for these existing methods. Keep their bodies unchanged:

```rust
impl ProtectedRefRule {
    pub(crate) fn validate(&self) -> Result<(), VfsError> {
        // existing body unchanged
    }
}

impl ProtectedPathRule {
    pub(crate) fn validate(&self) -> Result<(), VfsError> {
        // existing body unchanged
    }
}

impl ChangeRequest {
    pub(crate) fn transition(&self, status: ChangeRequestStatus) -> Result<Self, VfsError> {
        // existing body unchanged
    }

    pub(crate) fn validate(&self) -> Result<(), VfsError> {
        // existing body unchanged
    }
}

impl ApprovalRecord {
    pub(crate) fn new(input: NewApprovalRecord, change: &ChangeRequest) -> Result<Self, VfsError> {
        // existing body unchanged
    }

    pub(crate) fn validate(&self, change: &ChangeRequest) -> Result<(), VfsError> {
        // existing body unchanged
    }
}

impl ReviewAssignment {
    pub(crate) fn new(input: NewReviewAssignment, change: &ChangeRequest) -> Result<Self, VfsError> {
        // existing body unchanged
    }

    pub(crate) fn validate(&self, change: &ChangeRequest) -> Result<(), VfsError> {
        // existing body unchanged
    }
}

impl ReviewComment {
    pub(crate) fn new(input: NewReviewComment, change: &ChangeRequest) -> Result<Self, VfsError> {
        // existing body unchanged
    }

    pub(crate) fn validate(&self, change: &ChangeRequest) -> Result<(), VfsError> {
        // existing body unchanged
    }
}
```

**Step 2: Make normalization/open-state helpers crate-private**

Change only visibility for these existing free functions. Keep bodies unchanged:

```rust
pub(crate) fn validate_change_request_open(change: &ChangeRequest) -> Result<(), VfsError> {
    // existing body unchanged
}

pub(crate) fn normalize_approval_comment(comment: Option<String>) -> Result<Option<String>, VfsError> {
    // existing body unchanged
}

pub(crate) fn normalize_dismissal_reason(reason: Option<String>) -> Result<Option<String>, VfsError> {
    // existing body unchanged
}
```

Do not expose `ReviewState`, `reject_duplicate_id`, persisted local-store structs, or local-file migration helpers.

**Step 3: Run focused review tests**

Run:

```bash
cargo test --locked review::tests::in_memory_store_creates_lists_gets_and_transitions_change_requests -- --nocapture
cargo test --locked review::tests::terminal_change_requests_reject_approval_comment_and_dismissal_mutations -- --nocapture
```

Expected: both pass.

**Step 4: Commit**

```bash
git add src/review.rs
git commit -m "refactor: expose review domain helpers"
```

## Task 2: Add Failing Postgres Review Contract Tests

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Add test imports**

Inside `#[cfg(test)] mod tests`, extend imports:

```rust
use crate::review::{
    ApprovalRecordMutation, ChangeRequestStatus, DismissApprovalInput, NewApprovalRecord,
    NewChangeRequest, NewReviewAssignment, NewReviewComment, ReviewCommentKind, ReviewStore,
};
```

Keep imports sorted by local style after `cargo fmt`.

**Step 2: Add review test helpers**

Near the existing Postgres test helpers, add:

```rust
fn review_repo() -> RepoId {
    RepoId::local()
}

async fn seed_review_commits(
    store: &PostgresMetadataStore,
) -> Result<(CommitRecord, CommitRecord), VfsError> {
    let repo_id = review_repo();
    let base_tree = object_id(b"review-base-tree");
    let head_tree = object_id(b"review-head-tree");
    ObjectMetadataStore::put(
        store,
        object_record(&repo_id, base_tree, ObjectKind::Tree, b"review-base-tree"),
    )
    .await?;
    ObjectMetadataStore::put(
        store,
        object_record(&repo_id, head_tree, ObjectKind::Tree, b"review-head-tree"),
    )
    .await?;

    let base = commit_record(
        &repo_id,
        commit_id("review-base"),
        base_tree,
        Vec::new(),
        10,
        "review base",
    );
    let head = commit_record(
        &repo_id,
        commit_id("review-head"),
        head_tree,
        vec![base.id],
        11,
        "review head",
    );
    CommitStore::insert(store, base.clone()).await?;
    CommitStore::insert(store, head.clone()).await?;
    Ok((base, head))
}
```

The `commit_record` helper already uses changed path `/docs/readme.md`. For approval-policy path-rule tests, pass explicit changed paths such as `"/legal/contract.txt"` to `approval_decision`.

**Step 3: Add direct SQL corruption assertion helper**

Add:

```rust
async fn assert_review_corrupt_active_approval_is_rejected(
    store: &PostgresMetadataStore,
    approval_id: Uuid,
    change_request_id: Uuid,
) -> Result<(), VfsError> {
    let client = store.connect_client().await?;
    client
        .execute(
            "UPDATE approvals
             SET active = true, dismissed_by = 99, dismissal_reason = NULL
             WHERE id = $1",
            &[&approval_id],
        )
        .await
        .map_err(|error| postgres_error("corrupt review approval", error))?;

    let err = ReviewStore::list_approvals(store, change_request_id)
        .await
        .expect_err("corrupt active approval should be rejected");
    assert!(matches!(err, VfsError::CorruptStore { .. }));

    client
        .execute(
            "DELETE FROM approvals WHERE id = $1",
            &[&approval_id],
        )
        .await
        .map_err(|error| postgres_error("delete corrupt review approval", error))?;
    Ok(())
}
```

If the implementation chooses a stricter schema-level check later, adapt the test to corrupt another schema-allowed invariant. The important requirement is that Postgres row decoding rejects invalid domain records.

**Step 4: Add `run_review_contracts`**

Add this function and call it from `run_backend_contracts(store).await?` after `run_workspace_contracts(store).await?`:

```rust
async fn run_review_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
    let (base, head) = seed_review_commits(store).await?;

    assert!(ReviewStore::list_protected_ref_rules(store).await?.is_empty());
    assert!(ReviewStore::list_protected_path_rules(store).await?.is_empty());
    assert!(ReviewStore::list_change_requests(store).await?.is_empty());

    let ref_rule = ReviewStore::create_protected_ref_rule(store, "main", 2, 10).await?;
    assert_eq!(ref_rule.ref_name, "main");
    assert_eq!(ref_rule.required_approvals, 2);
    assert!(ref_rule.active);
    assert_eq!(
        ReviewStore::get_protected_ref_rule(store, ref_rule.id).await?,
        Some(ref_rule.clone())
    );

    let path_rule =
        ReviewStore::create_protected_path_rule(store, "/legal", Some("main"), 3, 10).await?;
    assert_eq!(path_rule.path_prefix, "/legal");
    assert_eq!(path_rule.target_ref.as_deref(), Some("main"));
    assert!(path_rule.matches_path("/legal/contract.txt"));
    assert_eq!(
        ReviewStore::get_protected_path_rule(store, path_rule.id).await?,
        Some(path_rule.clone())
    );

    let change = ReviewStore::create_change_request(
        store,
        NewChangeRequest {
            title: " Legal update ".to_string(),
            description: Some("Needs review".to_string()),
            source_ref: "review/legal-update".to_string(),
            target_ref: "main".to_string(),
            base_commit: base.id.to_hex(),
            head_commit: head.id.to_hex(),
            created_by: 10,
        },
    )
    .await?;
    assert_eq!(change.title, "Legal update");
    assert_eq!(change.status, ChangeRequestStatus::Open);
    assert_eq!(change.version, 1);
    assert_eq!(
        ReviewStore::get_change_request(store, change.id).await?,
        Some(change.clone())
    );

    let decision = ReviewStore::approval_decision(
        store,
        change.id,
        &["/legal/contract.txt".to_string()],
    )
    .await?
    .expect("approval decision should exist");
    assert_eq!(decision.required_approvals, 3);
    assert_eq!(decision.approval_count, 0);
    assert!(!decision.approved);
    assert_eq!(decision.matched_ref_rules, vec![ref_rule.id]);
    assert_eq!(decision.matched_path_rules, vec![path_rule.id]);

    let first_approval = ReviewStore::create_approval(
        store,
        NewApprovalRecord {
            change_request_id: change.id,
            head_commit: change.head_commit.clone(),
            approved_by: 20,
            comment: Some(" Looks good ".to_string()),
        },
    )
    .await?;
    assert!(first_approval.created);
    assert_eq!(first_approval.record.comment.as_deref(), Some("Looks good"));

    let duplicate_approval = ReviewStore::create_approval(
        store,
        NewApprovalRecord {
            change_request_id: change.id,
            head_commit: change.head_commit.clone(),
            approved_by: 20,
            comment: Some("different comment ignored on duplicate".to_string()),
        },
    )
    .await?;
    assert_eq!(
        duplicate_approval,
        ApprovalRecordMutation {
            record: first_approval.record.clone(),
            created: false,
        }
    );

    let assignment = ReviewStore::assign_reviewer(
        store,
        NewReviewAssignment {
            change_request_id: change.id,
            reviewer: 30,
            assigned_by: 10,
            required: true,
        },
    )
    .await?;
    assert!(assignment.created);
    assert!(assignment.assignment.required);

    let same_assignment = ReviewStore::assign_reviewer(
        store,
        NewReviewAssignment {
            change_request_id: change.id,
            reviewer: 30,
            assigned_by: 10,
            required: true,
        },
    )
    .await?;
    assert!(!same_assignment.created);
    assert!(!same_assignment.updated);

    let optional_assignment = ReviewStore::assign_reviewer(
        store,
        NewReviewAssignment {
            change_request_id: change.id,
            reviewer: 30,
            assigned_by: 11,
            required: false,
        },
    )
    .await?;
    assert!(!optional_assignment.created);
    assert!(optional_assignment.updated);
    assert!(!optional_assignment.assignment.required);
    assert_eq!(optional_assignment.assignment.version, 2);

    let comment = ReviewStore::create_comment(
        store,
        NewReviewComment {
            change_request_id: change.id,
            author: 20,
            body: " Please update the summary ".to_string(),
            path: Some(" /legal/contract.txt ".to_string()),
            kind: ReviewCommentKind::ChangesRequested,
        },
    )
    .await?;
    assert!(comment.created);
    assert_eq!(comment.comment.body, "Please update the summary");
    assert_eq!(comment.comment.path.as_deref(), Some("/legal/contract.txt"));

    let dismissed = ReviewStore::dismiss_approval(
        store,
        DismissApprovalInput {
            change_request_id: change.id,
            approval_id: first_approval.record.id,
            dismissed_by: 10,
            reason: Some(" stale approval ".to_string()),
        },
    )
    .await?;
    assert!(dismissed.dismissed);
    assert!(!dismissed.record.active);
    assert_eq!(dismissed.record.dismissal_reason.as_deref(), Some("stale approval"));
    assert_eq!(dismissed.record.version, 2);

    let after_dismissal = ReviewStore::approval_decision(
        store,
        change.id,
        &["/legal/contract.txt".to_string()],
    )
    .await?
    .expect("approval decision should exist");
    assert_eq!(after_dismissal.approval_count, 0);
    assert!(!after_dismissal.approved);

    let replacement_approval = ReviewStore::create_approval(
        store,
        NewApprovalRecord {
            change_request_id: change.id,
            head_commit: change.head_commit.clone(),
            approved_by: 20,
            comment: None,
        },
    )
    .await?;
    assert!(replacement_approval.created);
    assert_ne!(replacement_approval.record.id, first_approval.record.id);

    assert_review_corrupt_active_approval_is_rejected(
        store,
        replacement_approval.record.id,
        change.id,
    )
    .await?;

    let rejected = ReviewStore::transition_change_request(
        store,
        change.id,
        ChangeRequestStatus::Rejected,
    )
    .await?
    .expect("change request should transition");
    assert_eq!(rejected.status, ChangeRequestStatus::Rejected);
    assert_eq!(rejected.version, 2);
    assert!(
        ReviewStore::create_comment(
            store,
            NewReviewComment {
                change_request_id: change.id,
                author: 20,
                body: "late comment".to_string(),
                path: None,
                kind: ReviewCommentKind::General,
            },
        )
        .await
        .is_err()
    );

    Ok(())
}
```

If the exact equality assertion for `ApprovalRecordMutation` is too brittle after implementation, keep the behavioral assertions: same ID, `created: false`, same comment as the first active approval.

**Step 5: Run the failing test**

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: FAIL to compile because `PostgresMetadataStore` does not implement `ReviewStore`.

If local Postgres is unavailable, stop and report it. Do not mark this task complete with skipped Postgres tests.

**Step 6: Commit**

```bash
git add src/backend/postgres.rs
git commit -m "test: cover postgres review store contracts"
```

Commit the failing test only if this repository's working practice for the slice accepts red commits. If not, keep this change uncommitted until Task 3 passes, then commit Task 2 and Task 3 together with `feat: add postgres review adapter`.

## Task 3: Implement `ReviewStore for PostgresMetadataStore`

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Add production imports**

At the top of `src/backend/postgres.rs`, extend imports:

```rust
use std::collections::{BTreeMap, BTreeSet};
```

Add review imports:

```rust
use crate::review::{
    ApprovalDismissalMutation, ApprovalPolicyDecision, ApprovalRecord, ApprovalRecordMutation,
    ChangeRequest, ChangeRequestStatus, DismissApprovalInput, NewApprovalRecord, NewChangeRequest,
    NewReviewAssignment, NewReviewComment, ProtectedPathRule, ProtectedRefRule, ReviewAssignment,
    ReviewAssignmentMutation, ReviewComment, ReviewCommentKind, ReviewCommentMutation, ReviewStore,
    normalize_approval_comment, normalize_dismissal_reason, normalize_path_prefix,
    validate_change_request_open,
};
```

Adjust the import list to match actual compiler needs.

**Step 2: Add review repo helper and enum mapping helpers**

Place near other Postgres adapter helpers:

```rust
fn review_repo_id() -> RepoId {
    RepoId::local()
}

fn change_request_status_to_db(status: ChangeRequestStatus) -> &'static str {
    match status {
        ChangeRequestStatus::Open => "open",
        ChangeRequestStatus::Merged => "merged",
        ChangeRequestStatus::Rejected => "rejected",
    }
}

fn change_request_status_from_db(value: &str) -> Result<ChangeRequestStatus, VfsError> {
    match value {
        "open" => Ok(ChangeRequestStatus::Open),
        "merged" => Ok(ChangeRequestStatus::Merged),
        "rejected" => Ok(ChangeRequestStatus::Rejected),
        other => Err(VfsError::CorruptStore {
            message: format!("unknown change request status in Postgres metadata: {other}"),
        }),
    }
}

fn review_comment_kind_to_db(kind: ReviewCommentKind) -> &'static str {
    match kind {
        ReviewCommentKind::General => "general",
        ReviewCommentKind::ChangesRequested => "changes_requested",
    }
}

fn review_comment_kind_from_db(value: &str) -> Result<ReviewCommentKind, VfsError> {
    match value {
        "general" => Ok(ReviewCommentKind::General),
        "changes_requested" => Ok(ReviewCommentKind::ChangesRequested),
        other => Err(VfsError::CorruptStore {
            message: format!("unknown review comment kind in Postgres metadata: {other}"),
        }),
    }
}
```

**Step 3: Add version and UID conversion helpers if needed**

Reuse existing `uid_to_i32`, `i32_to_uid`, and `u64_to_i64` helpers. If row decoding needs a shared positive-version converter, add:

```rust
fn positive_i64_to_u64(value: i64, label: &str) -> Result<u64, VfsError> {
    if value <= 0 {
        return Err(VfsError::CorruptStore {
            message: format!("{label} has invalid version {value}"),
        });
    }
    Ok(value as u64)
}
```

Do not use unchecked casts for database versions or UIDs.

**Step 4: Add row decoders**

Add row decoder helpers for:

- `row_to_protected_ref_rule(row: Row) -> Result<ProtectedRefRule, VfsError>`
- `row_to_protected_path_rule(row: Row) -> Result<ProtectedPathRule, VfsError>`
- `row_to_change_request(row: Row) -> Result<ChangeRequest, VfsError>`
- `row_to_approval_record(row: Row, change: &ChangeRequest) -> Result<ApprovalRecord, VfsError>`
- `row_to_review_assignment(row: Row, change: &ChangeRequest) -> Result<ReviewAssignment, VfsError>`
- `row_to_review_comment(row: Row, change: &ChangeRequest) -> Result<ReviewComment, VfsError>`

Each decoder must construct the domain record, call the crate-private `validate` method, and map validation failures into `VfsError::CorruptStore`:

```rust
let record = ChangeRequest {
    id: row.get("id"),
    title: row.get("title"),
    description: row.get("description"),
    source_ref: row.get("source_ref"),
    target_ref: row.get("target_ref"),
    base_commit: row.get("base_commit"),
    head_commit: row.get("head_commit"),
    status: change_request_status_from_db(&row.get::<_, String>("status"))?,
    created_by: i32_to_uid(row.get("created_by"))?,
    version: positive_i64_to_u64(row.get("version"), "change request")?,
};
record.validate().map_err(corrupt_from_invalid)?;
Ok(record)
```

Use the same pattern for approvals, assignments, comments, and protected rules.

**Step 5: Add load helpers**

Add:

```rust
async fn load_review_change_request<C>(
    client: &C,
    id: Uuid,
) -> Result<Option<ChangeRequest>, VfsError>
where
    C: GenericClient + Sync,
{
    let repo_id = review_repo_id();
    let row = client
        .query_opt(
            "SELECT id, title, description, source_ref, target_ref, base_commit,
                    head_commit, status, created_by, version
             FROM change_requests
             WHERE repo_id = $1 AND id = $2",
            &[&repo_id.as_str(), &id],
        )
        .await
        .map_err(|error| postgres_error("review change request get", error))?;
    row.map(row_to_change_request).transpose()
}
```

Add a `load_review_change_request_for_update` variant for mutation transactions using `FOR UPDATE`.

**Step 6: Implement protected rule methods**

Add `#[async_trait] impl ReviewStore for PostgresMetadataStore`.

For `create_protected_ref_rule`:

- Construct with `ProtectedRefRule::new`.
- `ensure_repo(&client, &review_repo_id()).await?`.
- Insert with `repo_id = review_repo_id().as_str()`.
- Return the inserted row through `row_to_protected_ref_rule`.

Use:

```sql
INSERT INTO protected_ref_rules (id, repo_id, ref_name, required_approvals, created_by, active)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, ref_name, required_approvals, created_by, active
```

For list/get, filter by `repo_id = $1` and order by `created_at ASC, id ASC`.

For path rules, use the same pattern with `ProtectedPathRule::new` and:

```sql
INSERT INTO protected_path_rules (
    id, repo_id, path_prefix, target_ref, required_approvals, created_by, active
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, path_prefix, target_ref, required_approvals, created_by, active
```

**Step 7: Implement change-request create/list/get/transition**

For `create_change_request`:

- Construct with `ChangeRequest::new(input)?`.
- `ensure_repo` for `review_repo_id()`.
- Insert into `change_requests` using `repo_id = review_repo_id()`.
- Let Postgres FK errors reject unknown `base_commit` or `head_commit` for the local repo.
- Return through `row_to_change_request`.

For list/get:

- Filter by `repo_id = $1`.
- Order list by `created_at ASC, id ASC`.

For `transition_change_request`:

- Open a transaction.
- Lock the change-request row with `FOR UPDATE`.
- Return `Ok(None)` if no row exists.
- Call `current.transition(status)?`.
- Update `status`, `version`, and `updated_at = now()`.
- Return the updated row after commit.

Do not allow transitions to `Open`; the existing domain method rejects that.

**Step 8: Implement approval methods**

For `create_approval`:

- Open a transaction.
- Lock the change request with `FOR UPDATE`; unknown change requests return `VfsError::InvalidArgs` with the same style as local store.
- Call `ApprovalRecord::new(input, &change)?`.
- Try to insert an active approval.
- Use partial-index conflict handling:

```sql
INSERT INTO approvals (
    id, change_request_id, head_commit, approved_by, comment, active,
    dismissed_by, dismissal_reason, version
)
VALUES ($1, $2, $3, $4, $5, true, NULL, NULL, 1)
ON CONFLICT (change_request_id, head_commit, approved_by) WHERE active DO NOTHING
RETURNING id, change_request_id, head_commit, approved_by, comment, active,
          dismissed_by, dismissal_reason, version
```

- If insert returns no row, load the active existing approval for the same change/head/approver and return `ApprovalRecordMutation { created: false }`.
- If no inserted or existing row is visible after conflict, return `CorruptStore`.

For `list_approvals`:

- If the change request does not exist, return an empty vector to match the local store's list behavior for unknown IDs.
- Load the change request once.
- Query approvals by `change_request_id`, order by `created_at ASC, id ASC`, and decode each row against the change.

**Step 9: Implement reviewer assignment methods**

For `assign_reviewer`:

- Open a transaction.
- Lock the change request with `FOR UPDATE`; unknown change requests return `InvalidArgs`.
- Validate by constructing `ReviewAssignment::new(input.clone(), &change)?`.
- Query the existing assignment by `(change_request_id, reviewer) FOR UPDATE`.
- If an active assignment exists with the same `required`, return `created: false, updated: false`.
- If an active assignment exists with a different `required`, update `required`, `assigned_by`, `version = version + 1`, and `updated_at = now()`, then return `created: false, updated: true`.
- If no assignment exists, insert a new active assignment with version 1 and return `created: true, updated: false`.
- If an inactive assignment row exists, return `CorruptStore`; the current local contract never creates inactive reviewer assignments.

For `list_reviewer_assignments`:

- If the change request does not exist, return an empty vector.
- Query by `change_request_id`, order by `created_at ASC, id ASC`, and decode each row against the change.

**Step 10: Implement comment methods**

For `create_comment`:

- Open a transaction.
- Lock the change request with `FOR UPDATE`; unknown change requests return `InvalidArgs`.
- Construct with `ReviewComment::new(input, &change)?`.
- Insert with normalized `body`, normalized optional `path`, `kind`, active true, version 1.
- Return `ReviewCommentMutation { created: true }`.

For `list_comments`:

- If the change request does not exist, return an empty vector.
- Query by `change_request_id`, order by `created_at ASC, id ASC`, and decode each row against the change.

**Step 11: Implement approval dismissal**

For `dismiss_approval`:

- Open a transaction.
- Lock the approval row with `FOR UPDATE`.
- If the approval is missing, return `InvalidArgs` with `unknown approval <id>`.
- If `approval.change_request_id != input.change_request_id`, return `InvalidArgs`.
- Lock the corresponding change request with `FOR UPDATE`.
- Call `validate_change_request_open(&change)?` before returning an inactive replay, matching local store behavior.
- Normalize the reason with `normalize_dismissal_reason(input.reason)?`.
- If the approval is already inactive, return `ApprovalDismissalMutation { dismissed: false }`.
- If active, update `active = false`, `dismissed_by`, `dismissal_reason`, `version = version + 1`, `updated_at = now()`.
- Decode and return `dismissed: true`.

Check version overflow in Rust before issuing the update. Do not rely only on Postgres numeric overflow.

**Step 12: Implement approval decision**

For `approval_decision`:

- Load the change request; if absent, return `Ok(None)`.
- Load active protected ref rules for `review_repo_id()`.
- Load active protected path rules for `review_repo_id()`.
- Load active approvals for the change request where `head_commit = change.head_commit`.
- Load active required reviewer assignments for the change request.
- Reproduce the existing `ReviewState::approval_decision` logic:
  - Required approvals are the maximum required count from matching active ref/path rules.
  - Path rules match only when `target_ref` is absent or equals the change target ref and any supplied changed path matches `ProtectedPathRule::matches_path`.
  - `approved_by` is a sorted/deduped list.
  - `required_reviewers`, `approved_required_reviewers`, and `missing_required_reviewers` are sorted/deduped.
  - `approved` is true only when numeric approval count is enough and all required reviewers are satisfied.

Use `BTreeSet<Uid>` like `ReviewState` to preserve deterministic ordering.

**Step 13: Run focused Postgres tests**

Run:

```bash
cargo fmt --all -- --check
cargo check --locked --features postgres
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: all pass.

**Step 14: Commit**

```bash
git add src/backend/postgres.rs src/review.rs
git commit -m "feat: add postgres review adapter"
```

If Task 2 was not committed separately because the repo does not keep red commits, include its test changes in this commit.

## Task 4: Update Docs

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update `docs/http-api-guide.md` durable backend section**

Near the existing Postgres adapter paragraphs, add:

```markdown
The optional `postgres` feature also includes a Postgres-backed `ReviewStore` over protected ref rules, protected path rules, change requests, approvals, reviewer assignments, and review comments. It stores review rows under `RepoId::local()` because the current review trait is not repo-aware, preserves duplicate-approval, dismissal, reviewer-assignment, terminal-state, and approval-policy semantics, and remains unhooked from `stratum-server`.
```

Also keep the existing statement that review-control HTTP behavior remains local/file-backed unless runtime cutover lands.

**Step 2: Update `docs/project-status.md` top matter**

Change:

```markdown
- Baseline on `v2/foundation` before the latest backend slice: `<current HEAD before implementation>` (`docs: plan postgres review adapter foundation`)
- Latest completed backend slice: Postgres review adapter foundation (crate-only; `postgres` feature)
```

Use the actual baseline commit after the plan commit lands.

**Step 3: Add a completed slice section**

Add a section after the Postgres workspace metadata section:

```markdown
## Postgres Review Adapter Foundation

The Postgres review adapter foundation proves the durable protected-change and review tables can satisfy the existing Rust `ReviewStore` contract without changing server runtime behavior.

What is built:

- Feature-gated `impl ReviewStore for PostgresMetadataStore`, storing review rows under `RepoId::local()` until the review domain becomes repo-aware.
- Protected ref/path rule create/list/get, change-request create/list/get/transition, approval create/list/dismissal, reviewer assignment create/update/list, review comment create/list, and approval-policy decision computation over Postgres rows.
- Duplicate active approvals return the existing approval, dismissed approvals stop counting, required reviewer assignments participate in approval decisions, and terminal change requests reject new review mutations.
- Live adapter tests cover rule storage, repo-scoped change-request commit FKs, duplicate approvals, dismissal/re-approval, reviewer assignment updates, comment normalization, terminal-state rejection, approval-policy computation, and corrupt-row rejection.

What is not built:

- No `stratum-server` Postgres review runtime cutover.
- No repo-aware review trait or hosted multi-repo review routing.
- No reviewer groups, threaded/resolved comments, merge queue, web review UI, distributed policy engine, or cross-store transaction boundary.

Residual risk:

- Production review state remains local/file-backed until runtime wiring and the repo-aware review domain are designed.
```

**Step 4: Add factual verification results**

Add the actual command list and observed result after running final verification. Do not claim commands passed unless they did.

**Step 5: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document postgres review adapter"
```

## Task 5: Full Verification Before Handoff

**Files:**

- No direct edits unless verification finds issues.

**Step 1: Run focused and full gates**

Run:

```bash
cargo fmt --all -- --check
cargo check --locked --features postgres
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Expected: all pass.

If Postgres is unavailable locally, report that explicitly and include every command that did run.

**Step 2: Inspect final status**

Run:

```bash
git status --short --branch
git log --oneline -5
```

Expected: clean worktree on `v2/foundation`, ahead of origin by the small commits from this slice.

**Step 3: Return handoff notes**

Return:

- Commit list.
- Verification commands and pass/fail results.
- Any design notes, especially around `RepoId::local()` and repo-aware review-domain follow-up.
- Any places where implementation intentionally differed from this plan.

