# Reviewer Console And File-Viewed Merge Gate Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish private-beta reviewer console polish and add an authoritative, persisted file-viewed merge gate so the backend, SDKs, and UI agree on when a protected change request can merge.

**Architecture:** Implement this as two sequential slices. Task 9 is a web-console production pass over the existing review list/detail components: better actor labels, approval/comment/reviewer rendering, scoped empty/loading/error states, and explicit merge/reject confirmations. Task 10 adds a backend-owned viewed-file model keyed to the authenticated actor and current change-request head, exposes it through review HTTP routes and SDKs, and wires the web diff UI to that state so merge is blocked only when the effective policy requires all changed files to be viewed and some required paths are still unviewed.

**Tech Stack:** Rust/Axum review routes and `ReviewStore`, local review-store persistence, Postgres migrations/adapter, TypeScript SDK, Python SDK, React 19, TanStack Query, Vitest/Testing Library, Bun, pnpm, pytest/mypy/ruff.

---

## Read First

- `/Users/rajattiwari/virtualfilesystem/AGENTS.md`
- `/var/folders/hp/gn9_bs093l31rdpkq98yyj2h0000gn/T/stratum-task7-agent-adapter-handoff.md`
- `docs/private-beta-contract.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-06-10-sdk-cli-change-request-path.md`
- `docs/plans/2026-05-17-pre-slice45-review-contract-coordination.md`
- `web/src/spike/diff-as-reviewed.tsx`
- `markdownfs_v2_cto_architecture_plan.md`

## Preflight

Work in:

```bash
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/private-beta-closeout
git fetch origin main
git status --short --branch
git rev-parse HEAD origin/main
```

Expected:

- Branch is `private-beta-closeout`.
- Only unrelated untracked scratch state is acceptable, especially `?? .codex-scratch/`.
- Do not delete or stage `.codex-scratch/`.
- Do not leave `target/` in the worktree. Use `CARGO_TARGET_DIR=/tmp/stratum-target-task9-10`.
- Do not use destructive git commands.

## Current Findings

- The real reviewer app is in `web/`, not `site/`.
- Current surfaces:
  - `web/src/components/ReviewsScreen.tsx`
  - `web/src/components/ChangeRequestDetail.tsx`
  - `web/src/components/DiffViewer.tsx`
  - `web/src/lib/api/reviews.ts`
- Current tests already cover many list/detail states:
  - `web/src/components/ReviewsScreen.test.tsx`
  - `web/src/components/ChangeRequestDetail.test.tsx`
  - `web/src/lib/api/reviews.test.tsx`
- `DiffViewer.tsx` already exports `DiffFragmentBody`, which should be reused for per-file reviewed cards instead of duplicating fragment renderers.
- `web/src/spike/diff-as-reviewed.tsx` sketches the intended UX: per-file viewed checkbox, progress counter, mark all/unmark all, and merge disabled until all files are viewed.
- `src/review.rs` has no viewed-file model yet. `REVIEW_STORE_VERSION` is currently `4`.
- `src/server/routes_review.rs` computes changed paths and approval state during merge but does not enforce `require_all_files_viewed`.
- `docs/http-api-guide.md` currently says `require_all_files_viewed` is not enforced yet. Task 10 must make that statement false and update the docs.
- TypeScript and Python SDKs both mirror review endpoints, so task 10 must update both.

## Deliberate Scope Boundaries

- Do not add a user-directory endpoint in task 9. The current API returns numeric UIDs but not a full actor directory. Use deterministic frontend labels for now.
- Do not add inline hunk comments.
- Do not add a CR-scoped diff route. Continue using `client.vcs.diff({ base, head })`.
- Do not redesign approval semantics.
- Do not make file-viewed state global across all users. The gate is scoped to the authenticated actor because the UI can only truthfully represent the current actor's viewed state and the merge route can enforce that exact same state.
- Do not store diff contents or comment bodies in audit/idempotency details.

## Chosen Viewed-File API Contract

Add these routes:

```text
GET /change-requests/{id}/viewed-files
PUT /change-requests/{id}/viewed-files
```

`GET` returns the current actor's viewed-file state for the CR's recorded `head_commit`.

`PUT` takes:

```json
{
  "path": "/contracts/loi-acme-q2.docx.md",
  "viewed": true
}
```

`PUT` response:

```json
{
  "viewed_file": {
    "change_request_id": "<uuid>",
    "head_commit": "<64 hex>",
    "path": "/contracts/loi-acme-q2.docx.md",
    "viewed_by": 42,
    "viewed": true,
    "version": 1
  },
  "viewed_files": [
    {
      "change_request_id": "<uuid>",
      "head_commit": "<64 hex>",
      "path": "/contracts/loi-acme-q2.docx.md",
      "viewed_by": 42,
      "viewed": true,
      "version": 1
    }
  ],
  "required_paths": ["/contracts/loi-acme-q2.docx.md"],
  "unviewed_paths": [],
  "all_required_files_viewed": true,
  "require_all_files_viewed": true,
  "approval_state": {
    "approved": true,
    "required_approvals": 1,
    "approval_count": 1,
    "approved_by": [42],
    "matched_ref_rules": [],
    "matched_path_rules": [],
    "require_all_files_viewed": true,
    "required_reviewers": [],
    "approved_required_reviewers": [],
    "missing_required_reviewers": []
  },
  "updated": true
}
```

`GET` response is the same minus `viewed_file` and `updated`.

Merge behavior:

- If `approval_state.approved` is false, keep the existing approval failure response.
- If `approval_state.approved` is true and `approval_state.require_all_files_viewed` is false, do not block merge on viewed-file state.
- If `approval_state.approved` is true and `approval_state.require_all_files_viewed` is true, block merge with `403` when the current authenticated actor has any required changed path not marked `viewed: true` for the CR's `head_commit`.
- Empty changed-path sets count as all viewed.
- Marking a path outside the current changed paths returns `400` and does not mutate.
- Marking viewed/unviewed is idempotent for the same actor, CR, head, path, and boolean value.
- `PUT` supports `Idempotency-Key` and uses secret-free replay classification.

---

## Task 0: Refresh And Baseline

**Files:**

- Read-only: repository status and relevant files.

**Step 1: Refresh from origin**

Run:

```bash
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/private-beta-closeout
git fetch origin main
git status --short --branch
git rev-parse HEAD origin/main
```

Expected: clean except `.codex-scratch/`.

**Step 2: Run focused baseline tests**

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx ReviewsScreen.test.tsx reviews.test.tsx
bun run --cwd sdk/typescript test:run -- client.test.ts
cd sdk/python && python -m pytest tests/test_client.py
cd ../..
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib server::routes_review::tests -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib review::tests -- --nocapture
```

Expected: all baseline tests pass. If a command is missing local dependencies, record that and continue only after confirming it is unrelated to task changes.

---

## Task 9.1: Frontend Actor Labels

**Files:**

- Create: `web/src/lib/review-actors.ts`
- Create: `web/src/lib/review-actors.test.ts`
- Modify: `web/src/components/ReviewsScreen.tsx`
- Modify: `web/src/components/ChangeRequestDetail.tsx`
- Modify: `web/src/components/ReviewsScreen.test.tsx`
- Modify: `web/src/components/ChangeRequestDetail.test.tsx`

**Step 1: Write failing actor-helper tests**

Create `web/src/lib/review-actors.test.ts` with tests for:

- `formatReviewActor(0)` returns `root`.
- `formatReviewActor(42)` returns `Reviewer 42`.
- `formatReviewActor(100)` returns `Agent 100` only when `{ kind: "agent" }` is supplied.
- `formatReviewActorList([42, 7])` returns `Reviewer 42, Reviewer 7`.
- `formatReviewActorList([])` returns `-`.

Use ASCII `-` for the empty placeholder.

Run:

```bash
pnpm --dir web test:run -- review-actors.test.ts
```

Expected: FAIL because `web/src/lib/review-actors.ts` does not exist.

**Step 2: Implement the helper**

Create `web/src/lib/review-actors.ts`:

```ts
export type ReviewActorKind = "reviewer" | "agent" | "system";

export function formatReviewActor(
  uid: number | null | undefined,
  options: { readonly kind?: ReviewActorKind } = {},
): string {
  if (uid === null || uid === undefined) return "Unknown actor";
  if (uid === 0) return "root";
  if (options.kind === "agent") return `Agent ${uid}`;
  if (options.kind === "system") return `System ${uid}`;
  return `Reviewer ${uid}`;
}

export function formatReviewActorList(values: readonly number[]): string {
  if (values.length === 0) return "-";
  return values.map((uid) => formatReviewActor(uid)).join(", ");
}
```

Run:

```bash
pnpm --dir web test:run -- review-actors.test.ts
```

Expected: PASS.

**Step 3: Write failing component tests for no raw UID labels**

Update `web/src/components/ChangeRequestDetail.test.tsx`:

- Replace assertions that expect `uid:42` with `Reviewer 42`.
- Replace `dismissed by uid:0` with `dismissed by root`.
- Assert approval comments render as explicit reasons:
  - active approval with comment shows `Reason: looks good` or equivalent plain label.
  - approval without comment shows `No reason recorded`.
- Assert required/missing reviewers render `Reviewer 42`, not `uid:42`.
- Assert comments render `Reviewer 42`, not raw `uid:42`.

Update `web/src/components/ReviewsScreen.test.tsx`:

- Assert cards render `Reviewer 1` or `Agent 100` using the helper policy chosen in the component.
- Assert no visible `uid:` text appears on the list screen.

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx ReviewsScreen.test.tsx
```

Expected: FAIL on old `uid:` text.

**Step 4: Replace raw UID formatting in components**

In `web/src/components/ChangeRequestDetail.tsx`:

- Import `formatReviewActor` and `formatReviewActorList`.
- Update `ApprovalDetail`:
  - `Approved by`: `formatReviewActorList(a.approved_by)`.
  - `Required reviewers`: map through `formatReviewActor(uid)`, preserving the existing approved marker text.
  - `Missing reviewers`: map through `formatReviewActor(uid)`.
- Update `ApprovalRow`:
  - Active row actor: `formatReviewActor(approval.approved_by)`.
  - If comment exists, render it under an explicit `Reason:` label.
  - If no comment exists, render `No reason recorded` in muted text.
  - Dismissed row: `dismissed by ${formatReviewActor(approval.dismissed_by)}` and render dismissal reason under `Dismissal reason:` when present.
- Update `ReviewerRow`:
  - `formatReviewActor(assignment.reviewer)`.
  - `assigned by ${formatReviewActor(assignment.assigned_by)}`.
- Update `CommentRow`:
  - `formatReviewActor(comment.author)`.
- Update `mergeBlockReason` missing reviewer names to use the helper.

In `web/src/components/ReviewsScreen.tsx`:

- Use `formatReviewActor(cr.created_by)` instead of raw UID text.
- If the screen currently uses an agent heuristic for high UIDs, keep the visual mark only where it already exists, but do not label unknown UIDs as agents unless the component has explicit evidence.

Run:

```bash
pnpm --dir web test:run -- review-actors.test.ts ChangeRequestDetail.test.tsx ReviewsScreen.test.tsx
```

Expected: PASS.

**Step 5: Commit task 9 actor-label slice**

Run:

```bash
git add web/src/lib/review-actors.ts web/src/lib/review-actors.test.ts web/src/components/ReviewsScreen.tsx web/src/components/ReviewsScreen.test.tsx web/src/components/ChangeRequestDetail.tsx web/src/components/ChangeRequestDetail.test.tsx
git commit -m "feat: polish reviewer console actor labels"
```

---

## Task 9.2: Review Detail Flow Polish

**Files:**

- Modify: `web/src/components/ChangeRequestDetail.tsx`
- Modify: `web/src/components/ChangeRequestDetail.test.tsx`
- Modify if needed: `web/src/components/ReviewsScreen.tsx`
- Modify if needed: `web/src/components/ReviewsScreen.test.tsx`

**Step 1: Add failing tests for merge/reject confirmation**

In `web/src/components/ChangeRequestDetail.test.tsx`, add tests:

- Clicking `Reject` opens an inline confirmation region.
- Reject mutation is not called until `Confirm reject` is clicked.
- `Cancel` closes the reject confirmation and resets reject errors.
- Clicking `Merge` on an approved, ready CR opens an inline confirmation region.
- Merge mutation is not called until `Confirm merge` is clicked.
- `Cancel` closes the merge confirmation and resets merge errors.
- Terminal CRs do not show active merge/reject confirmations.

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx
```

Expected: FAIL because merge and reject currently fire directly.

**Step 2: Implement explicit confirmations**

In `ActionRow`:

- Add `showMergeConfirm` and `showRejectConfirm` state.
- Change the primary `Merge` button to open confirmation when `canMerge` is true.
- Change the `Reject` button to open confirmation when open.
- Add an inline merge confirmation block:
  - Text includes target ref, base short hash, and head short hash.
  - `Confirm merge` calls `merge.mutate({ id }, { onSuccess: () => setShowMergeConfirm(false) })`.
  - `Cancel` calls `merge.reset()` and closes the block.
- Add an inline reject confirmation block:
  - Text says the CR will be closed without advancing the target ref.
  - `Confirm reject` calls `reject.mutate({ id }, { onSuccess: () => setShowRejectConfirm(false) })`.
  - `Cancel` calls `reject.reset()` and closes the block.
- Hide the other confirmation when opening one confirmation.
- Keep the existing revert confirmation behavior.

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx
```

Expected: PASS.

**Step 3: Add scoped empty/loading/error regression tests**

Add or tighten tests so the detail screen proves these states independently:

- Approvals load error renders only the approvals alert and keeps overview, reviewers, comments, and diff visible.
- Reviewers load error renders only the reviewers alert and keeps other sections visible.
- Comments load error renders only the comments alert and keeps other sections visible.
- Diff load error renders only the diff alert and keeps approvals/reviewers/comments visible.
- Empty reviewers shows `No reviewers assigned.`
- Empty comments shows `No comments yet.`
- Read-only terminal CRs hide reviewer assignment and comment composer controls.

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx
```

Expected: PASS after implementation or PASS if the existing code already satisfies the added assertions.

**Step 4: Run task 9 web checks**

Run:

```bash
pnpm --dir web test:run -- review-actors.test.ts ChangeRequestDetail.test.tsx ReviewsScreen.test.tsx reviews.test.tsx
pnpm --dir web typecheck
```

Expected: PASS.

**Step 5: Commit task 9 flow polish**

Run:

```bash
git add web/src/components/ChangeRequestDetail.tsx web/src/components/ChangeRequestDetail.test.tsx web/src/components/ReviewsScreen.tsx web/src/components/ReviewsScreen.test.tsx web/src/lib/review-actors.ts web/src/lib/review-actors.test.ts
git commit -m "feat: harden reviewer console flows"
```

---

## Task 10.1: Local ReviewStore Viewed-File Model

**Files:**

- Modify: `src/review.rs`

**Step 1: Write failing local store tests**

Add tests under `#[cfg(test)] mod tests` in `src/review.rs`:

- `viewed_files_are_scoped_by_repo_change_head_and_actor`
- `set_viewed_file_is_idempotent_for_same_value`
- `set_viewed_file_unview_updates_existing_record`
- `set_viewed_file_rejects_terminal_change_request`
- `review_store_v4_decodes_with_empty_viewed_files`

The tests should create a change request using existing helpers, then call new methods that do not exist yet:

```rust
store
    .set_viewed_file_for_repo(
        &repo_id,
        SetViewedFileInput {
            change_request_id: change.id,
            path: "/contracts/a.md".to_string(),
            viewed_by: 42,
            viewed: true,
        },
    )
    .await
    .unwrap();
```

Expected behavior:

- A first set creates a record with `version == 1`.
- Repeating the same `viewed` value returns `updated == false` and does not increment `version`.
- Toggling `viewed` increments `version`.
- `list_viewed_files_for_repo(repo, change.id, 42)` returns only the actor's records for the CR's current `head_commit`.
- A different repo or different actor does not see the records.
- A rejected or merged CR rejects mutation with `VfsError::InvalidArgs` or the same terminal-state error style used by approvals/reviewers/comments.
- Decoding a v4 store produces empty viewed-file state.

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib review::tests::viewed_files -- --nocapture
```

Expected: FAIL because the model and methods do not exist.

**Step 2: Add domain structs and trait methods**

In `src/review.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewedFileRecord {
    pub change_request_id: Uuid,
    pub head_commit: String,
    pub path: String,
    pub viewed_by: Uid,
    pub viewed: bool,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetViewedFileInput {
    pub change_request_id: Uuid,
    pub path: String,
    pub viewed_by: Uid,
    pub viewed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewedFileMutation {
    pub record: ViewedFileRecord,
    pub created: bool,
    pub updated: bool,
}
```

Add trait methods:

```rust
async fn set_viewed_file_for_repo(
    &self,
    repo_id: &RepoId,
    input: SetViewedFileInput,
) -> Result<ViewedFileMutation, VfsError>;

async fn list_viewed_files_for_repo(
    &self,
    repo_id: &RepoId,
    change_request_id: Uuid,
    viewed_by: Uid,
) -> Result<Vec<ViewedFileRecord>, VfsError>;
```

Add local-repo convenience methods like the rest of `ReviewStore`.

**Step 3: Add `ReviewState` storage**

In `ReviewState`, add:

```rust
viewed_files: BTreeMap<(Uuid, String, String, Uid), ViewedFileRecord>,
```

Key order is `(change_request_id, head_commit, path, viewed_by)`.

Add methods:

- `set_viewed_file(&mut self, repo_id: &RepoId, input: SetViewedFileInput) -> Result<ViewedFileMutation, VfsError>`
- `list_viewed_files(&self, repo_id: &RepoId, change_request_id: Uuid, viewed_by: Uid) -> Vec<ViewedFileRecord>`

Validation:

- Change request must exist in the same repo.
- Change request must be open.
- Normalize path as an absolute file path. Reject empty, relative paths, and root-only `/`.
- Use the change request's existing `head_commit`; do not accept a client-supplied head.
- Version must be positive.

**Step 4: Bump local persistence to v5**

In `src/review.rs`:

- Change `REVIEW_STORE_VERSION` from `4` to `5`.
- Add `viewed_files: Vec<ViewedFileRecord>` to `PersistedReviewStore`.
- Add a `PersistedReviewStoreV4` or rename the current v4 struct so old v4 files decode with `viewed_files: Vec::new()`.
- Keep existing `WithoutFileViewed` structs for protected-rule migration compatibility, but do not confuse those old rule flags with new viewed-file records.
- Update `encode` to persist `viewed_files`.
- Update decode validation to insert viewed records and reject duplicates/corrupt versions.

**Step 5: Implement InMemory and LocalReviewStore trait methods**

Implement methods for both:

- `InMemoryReviewStore`
- `LocalReviewStore`

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib review::tests::viewed_files -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib review::tests -- --nocapture
```

Expected: PASS.

**Step 6: Commit local model**

Run:

```bash
git add src/review.rs
git commit -m "feat: persist change request viewed files"
```

---

## Task 10.2: HTTP Routes And Merge Enforcement

**Files:**

- Modify: `src/server/routes_review.rs`
- Modify: `src/audit.rs`
- Modify if needed: `docs/http-api-guide.md` later, but do not update docs until tests are green.

**Step 1: Write failing route tests**

In `src/server/routes_review.rs`, add tests:

- `list_change_request_viewed_files_reports_required_and_unviewed_paths`
- `set_change_request_viewed_file_marks_and_unmarks_path`
- `set_change_request_viewed_file_rejects_path_outside_changed_paths`
- `set_change_request_viewed_file_replays_idempotently`
- `merge_change_request_blocks_when_required_files_unviewed`
- `merge_change_request_allows_when_required_files_viewed`
- `merge_change_request_does_not_block_when_policy_does_not_require_viewed_files`
- `merge_change_request_treats_empty_changed_paths_as_viewed`

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib server::routes_review::tests::viewed_files -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib server::routes_review::tests::merge_change_request_blocks_when_required_files_unviewed -- --nocapture
```

Expected: FAIL because routes do not exist and merge does not enforce the gate.

**Step 2: Add route constants and router entries**

In `src/server/routes_review.rs`, add:

```rust
const SET_CHANGE_REQUEST_VIEWED_FILE_ROUTE: &str = "PUT /change-requests/{id}/viewed-files";
```

Add routes:

```rust
.route(
    "/change-requests/{id}/viewed-files",
    get(list_change_request_viewed_files).put(set_change_request_viewed_file),
)
```

Use the same auth and repo-resolution pattern as comments/reviewers:

- Require an authenticated session.
- Resolve review repo context from headers and session.
- Use current `session.uid` as `viewed_by`.

**Step 3: Add request/response helpers**

Add request struct:

```rust
#[derive(Deserialize)]
struct SetViewedFileRequest {
    path: String,
    viewed: bool,
}
```

Add helper:

```rust
async fn viewed_files_json(
    state: &AppState,
    change: &ChangeRequest,
    viewed_by: Uid,
) -> Result<serde_json::Value, VfsError>
```

The helper must:

- Compute `changed_paths_for_change(state, change)`.
- Compute `approval_decision_for_paths(state, change, &changed_paths)`.
- Load `state.review.list_viewed_files_for_repo(change.repo_id, change.id, viewed_by)`.
- Keep only records where `record.head_commit == change.head_commit` and `record.viewed == true`.
- Compute `required_paths = changed_paths` sorted deterministically.
- Compute `unviewed_paths = required_paths - viewed_true_paths`.
- Return the JSON contract described above.

**Step 4: Implement `GET /viewed-files`**

Behavior:

- `404` when CR does not exist.
- `409` if changed paths or approval state cannot be computed.
- Otherwise return `200`.

**Step 5: Implement `PUT /viewed-files`**

Behavior:

- Begin idempotency with route `PUT /change-requests/{id}/viewed-files`.
- Fingerprint includes route, actor fingerprint, repo id, change request id, path, and viewed boolean.
- Load CR and changed paths.
- Reject path not in changed paths with `400` and abort idempotency.
- Call `state.review.set_viewed_file_for_repo(...)`.
- Append a metadata-only audit event with action `ChangeRequestFileView`.
- Complete idempotency as secret-free.
- Return `200` with full viewed-file state plus `viewed_file` and `updated`.

Audit details may include:

- `change_request_id`
- `path`
- `viewed`
- `viewed_by`
- `head_commit`
- `version`

Audit details must not include file contents or raw tokens.

**Step 6: Add audit action**

In `src/audit.rs`, add `AuditAction::ChangeRequestFileView`.

Classify it under the same change-request category as other review actions.

Run focused audit tests if the enum has serialization tests.

**Step 7: Enforce gate in merge**

In `merge_change_request`:

- Keep existing source/target stale checks before policy checks.
- Keep existing approval failure response when approvals are missing.
- After approval passes and before target ref update, if `approval_state.require_all_files_viewed` is true:
  - Load current actor viewed records for the CR.
  - Compute unviewed paths from the same `changed_paths` used for approval.
  - If unviewed paths is non-empty, abort idempotency and return `403`.

Use response shape:

```json
{
  "error": "change request <id> requires all changed files to be viewed before merge",
  "required_paths": ["/a.md"],
  "unviewed_paths": ["/a.md"],
  "all_required_files_viewed": false,
  "require_all_files_viewed": true,
  "approval_state": { "...": "..." }
}
```

**Step 8: Run route tests**

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib server::routes_review::tests::viewed_files -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib server::routes_review::tests::merge_change_request -- --nocapture
```

Expected: PASS.

**Step 9: Commit routes and merge gate**

Run:

```bash
git add src/server/routes_review.rs src/audit.rs
git commit -m "feat: enforce viewed-file merge gate"
```

---

## Task 10.3: Postgres Migration And Adapter

**Files:**

- Create: `migrations/postgres/0023_review_viewed_files.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `src/backend/postgres.rs`

**Step 1: Write failing migration catalog tests**

Add/update tests in `src/backend/postgres_migrations.rs` so schema verification expects a `change_request_file_views` table and can `SELECT` these columns:

- `change_request_id`
- `head_commit`
- `path`
- `viewed_by`
- `viewed`
- `version`
- `created_at`
- `updated_at`

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --features postgres --lib backend::postgres_migrations -- --nocapture
```

Expected: FAIL because migration 23 does not exist.

**Step 2: Add migration 23**

Create `migrations/postgres/0023_review_viewed_files.sql`:

```sql
CREATE TABLE change_request_file_views (
    change_request_id UUID NOT NULL REFERENCES change_requests(id) ON DELETE CASCADE,
    head_commit TEXT NOT NULL CHECK (head_commit ~ '^[0-9a-f]{64}$'),
    path TEXT NOT NULL CHECK (path LIKE '/%' AND path <> '/'),
    viewed_by INTEGER NOT NULL,
    viewed BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (change_request_id, head_commit, path, viewed_by)
);

CREATE INDEX change_request_file_views_change_head_idx
    ON change_request_file_views(change_request_id, head_commit);
```

**Step 3: Register migration 23**

In `src/backend/postgres_migrations.rs`:

- Add include constant for migration 23.
- Bump `POSTGRES_MIGRATIONS` array length from 22 to 23.
- Add catalog item:

```rust
PostgresMigration {
    version: 23,
    name: "review_viewed_files",
    sql: POSTGRES_MIGRATION_0023_REVIEW_VIEWED_FILES,
},
```

- Add `change_request_file_views` to `verify_known_schema_catalog`.
- Add a `SELECT ... LIMIT 0` verification statement for the new columns.

**Step 4: Write failing Postgres adapter contract tests**

In `src/backend/postgres.rs`, add Postgres review-store tests for:

- Creating/listing viewed files.
- Idempotent same-value update.
- Toggle update increments version.
- Repo scoping.

Use the existing Postgres review contract test setup and migration application pattern.

Run if a local Postgres test URL is configured:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --features postgres --lib postgres_review_viewed_files -- --nocapture
```

Expected: FAIL until adapter methods are implemented. If no Postgres test URL is configured and existing tests skip, record the skip.

**Step 5: Implement Postgres `ReviewStore` methods**

In `src/backend/postgres.rs`:

- Add row conversion helper `row_to_viewed_file_record`.
- Implement `set_viewed_file_for_repo`:
  - Lock/load the change request.
  - Validate repo and open status using existing domain constructor/helper.
  - Insert with `ON CONFLICT (change_request_id, head_commit, path, viewed_by) DO UPDATE`.
  - Only increment `version` when `viewed` changes.
  - Return `created`/`updated` flags consistent with local store.
- Implement `list_viewed_files_for_repo`:
  - Load change request for repo.
  - Select only matching `change_request_id`, `head_commit`, `viewed_by`.
  - Order by `path ASC`.

**Step 6: Run Postgres and migration checks**

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --features postgres --lib backend::postgres_migrations -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --features postgres --lib postgres_review_viewed_files -- --nocapture
```

Expected: PASS or documented skip for live Postgres-only tests.

**Step 7: Commit Postgres slice**

Run:

```bash
git add migrations/postgres/0023_review_viewed_files.sql src/backend/postgres_migrations.rs src/backend/postgres.rs
git commit -m "feat: add postgres viewed-file review state"
```

---

## Task 10.4: SDK Surface

**Files:**

- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/src/client.ts`
- Modify: `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/python/src/stratum_sdk/client.py`
- Modify: `sdk/python/tests/test_client.py`

**Step 1: Add failing TypeScript SDK tests**

In `sdk/typescript/tests/client.test.ts`, add tests:

- `client.reviews.listViewedFiles("cr1")` sends `GET /change-requests/cr1/viewed-files`.
- `client.reviews.setViewedFile("cr1", { path: "/a.md", viewed: true }, { idempotencyKey: "idem" })` sends `PUT /change-requests/cr1/viewed-files` with body and idempotency key.
- Route segments are encoded.

Run:

```bash
bun run --cwd sdk/typescript test:run -- client.test.ts
```

Expected: FAIL because methods/types do not exist.

**Step 2: Add TypeScript types and client methods**

In `sdk/typescript/src/types.ts`, add:

```ts
export interface ViewedFileRecord {
  readonly change_request_id: string;
  readonly head_commit: string;
  readonly path: string;
  readonly viewed_by: number;
  readonly viewed: boolean;
  readonly version: number;
}

export interface ViewedFilesResponse {
  readonly viewed_files: readonly ViewedFileRecord[];
  readonly required_paths: readonly string[];
  readonly unviewed_paths: readonly string[];
  readonly all_required_files_viewed: boolean;
  readonly require_all_files_viewed: boolean;
  readonly approval_state: ApprovalState;
}

export interface UpdateViewedFileRequest {
  readonly path: string;
  readonly viewed: boolean;
}

export interface UpdateViewedFileResponse extends ViewedFilesResponse {
  readonly viewed_file: ViewedFileRecord;
  readonly updated: boolean;
}
```

In `sdk/typescript/src/client.ts`, add methods to `ReviewsClient`:

```ts
listViewedFiles(id: string): Promise<ViewedFilesResponse> {
  return this.http.json(`change-requests/${encodeRouteSegment(id)}/viewed-files`, { method: "GET" });
}

setViewedFile(
  id: string,
  request: UpdateViewedFileRequest,
  options: StratumMutationOptions = {},
): Promise<UpdateViewedFileResponse> {
  return this.http.json(`change-requests/${encodeRouteSegment(id)}/viewed-files`, {
    method: "PUT",
    body: request,
    idempotencyKey: options.idempotencyKey,
    autoIdempotency: true,
  });
}
```

Run:

```bash
bun run --cwd sdk/typescript test:run -- client.test.ts
bun run --cwd sdk/typescript typecheck
```

Expected: PASS.

**Step 3: Add failing Python SDK tests**

In `sdk/python/tests/test_client.py`, add tests mirroring TypeScript:

- `client.reviews.list_viewed_files("cr1")`.
- `client.reviews.set_viewed_file("cr1", {"path": "/a.md", "viewed": True}, idempotency_key="idem")`.

Run:

```bash
cd sdk/python && python -m pytest tests/test_client.py
```

Expected: FAIL because methods/types do not exist.

**Step 4: Add Python types and client methods**

In `sdk/python/src/stratum_sdk/types.py`, add `TypedDict`s equivalent to the TypeScript types.

In `sdk/python/src/stratum_sdk/client.py`, add methods to `ReviewsClient`:

```python
def list_viewed_files(self, change_request_id: str) -> ViewedFilesResponse:
    return cast(
        ViewedFilesResponse,
        self._http.request_json(
            f"change-requests/{encode_route_segment(change_request_id)}/viewed-files",
            "GET",
        ),
    )

def set_viewed_file(
    self,
    change_request_id: str,
    request: UpdateViewedFileRequest,
    *,
    idempotency_key: str | None = None,
) -> UpdateViewedFileResponse:
    return cast(
        UpdateViewedFileResponse,
        self._http.request_json(
            f"change-requests/{encode_route_segment(change_request_id)}/viewed-files",
            "PUT",
            body=request,
            idempotency_key=idempotency_key,
            auto_idempotency=True,
        ),
    )
```

Run:

```bash
cd sdk/python
python -m pytest tests/test_client.py
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
cd ../..
```

Expected: PASS.

**Step 5: Commit SDK slice**

Run:

```bash
git add sdk/typescript/src/types.ts sdk/typescript/src/client.ts sdk/typescript/tests/client.test.ts sdk/python/src/stratum_sdk/types.py sdk/python/src/stratum_sdk/client.py sdk/python/tests/test_client.py
git commit -m "feat: expose viewed files in sdks"
```

---

## Task 10.5: Web Hooks And Viewed Diff UI

**Files:**

- Modify: `web/src/lib/api/reviews.ts`
- Modify: `web/src/lib/api/reviews.test.tsx`
- Modify: `web/src/components/ChangeRequestDetail.tsx`
- Modify: `web/src/components/ChangeRequestDetail.test.tsx`
- Modify if useful: `web/src/components/DiffViewer.tsx`

**Step 1: Add failing API hook tests**

In `web/src/lib/api/reviews.test.tsx`, add tests:

- `useViewedFiles(id)` calls `client.reviews.listViewedFiles(id)` and uses key `reviewKeys.viewedFiles(id)`.
- `useSetViewedFile()` calls `client.reviews.setViewedFile` with a generated idempotency key.
- On success, it invalidates `reviewKeys.viewedFiles(id)`, `reviewKeys.detail(id)`, and `reviewKeys.list()`.

Run:

```bash
pnpm --dir web test:run -- reviews.test.tsx
```

Expected: FAIL.

**Step 2: Implement hooks**

In `web/src/lib/api/reviews.ts`:

- Import `ViewedFilesResponse`, `UpdateViewedFileResponse`, and `UpdateViewedFileRequest`.
- Add query key:

```ts
viewedFiles: (id: string) => [...reviewKeys.all, "viewed-files", id] as const,
```

- Add:

```ts
export function useViewedFiles(id: string): UseQueryResult<ViewedFilesResponse, Error> { ... }
export function useSetViewedFile(): UseMutationResult<
  UpdateViewedFileResponse,
  Error,
  { readonly id: string; readonly path: string; readonly viewed: boolean }
> { ... }
```

Use `newIdempotencyKey()` like existing review mutations.

Run:

```bash
pnpm --dir web test:run -- reviews.test.tsx
```

Expected: PASS.

**Step 3: Add failing detail UI tests for viewed files**

In `web/src/components/ChangeRequestDetail.test.tsx`, replace the old test that expects merge to stay disabled whenever `require_all_files_viewed` is true.

Add tests:

- When `require_all_files_viewed` is true and viewed-files query is loading, merge is disabled with `Loading file review state.`
- When viewed-files query errors, merge is disabled with `Couldn't load file review state.`
- When one required path is unviewed, merge is disabled and copy says `Review 1 remaining file before merging.`
- Checking a file calls `setViewedFile` for that path.
- After all required paths are viewed, merge button can open the confirmation flow.
- When `require_all_files_viewed` is false, merge is not blocked by unviewed paths.
- The diff section shows `0 of N files viewed` and updates based on backend response.
- Empty diff with `required_paths: []` shows all viewed and does not block merge.

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx
```

Expected: FAIL.

**Step 4: Wire viewed-file state into detail**

In `PopulatedDetail`:

- Call `const viewedFiles = useViewedFiles(cr.id);`.
- Pass `viewedFiles` into `ActionRow` and `DiffSection`.

Add helper:

```ts
function fileReviewGate(
  item: ChangeRequestResponse,
  viewedFiles: UseQueryResult<ViewedFilesResponse, Error>,
): {
  readonly blocksMerge: boolean;
  readonly reason?: string;
  readonly allViewed: boolean;
  readonly unviewedCount: number;
}
```

Rules:

- If CR is not open, merge is disabled by terminal state as today.
- If approval state unavailable, keep approval-state block reason.
- If not approved, keep approval block reason.
- If `item.require_all_files_viewed` and/or `approval_state.require_all_files_viewed` is false, do not block on viewed files.
- If viewed query is loading, block with `Loading file review state.`
- If viewed query errored, block with `Couldn't load file review state.`
- If `unviewed_paths.length > 0`, block with `Review N remaining file(s) before merging.`
- Otherwise allow merge.

Use `approval_state.require_all_files_viewed` when approval state is available. Fall back to `item.require_all_files_viewed` only when approval state is unavailable.

**Step 5: Replace plain DiffViewer usage with reviewed cards**

Keep `DiffViewer` unchanged if possible. In `ChangeRequestDetail.tsx`, use `DiffFragmentBody` plus `fragmentTotals` and `summariseFragmentKind` to render each file with:

- Collapsible file header.
- Path.
- Fragment type label.
- Added/removed totals when text diff.
- Checkbox labeled `Viewed`.
- Disabled checkbox when CR is terminal, viewed-files query is loading/error, or mutation is pending for that path.
- Progress row: `X of Y files viewed`.
- `Mark all viewed` and `Unmark all` buttons if the CR is open and the viewed-files query succeeded.

Use backend `required_paths` as the source of truth for the denominator. For fragments not present in `required_paths`, render them but do not require them for merge. In normal responses, `required_paths` should match diff fragments.

Do not use `sessionStorage` or local-only state for persisted viewed status.

**Step 6: Run web tests**

Run:

```bash
pnpm --dir web test:run -- ChangeRequestDetail.test.tsx reviews.test.tsx review-actors.test.ts ReviewsScreen.test.tsx
pnpm --dir web typecheck
```

Expected: PASS.

**Step 7: Commit web viewed UI**

Run:

```bash
git add web/src/lib/api/reviews.ts web/src/lib/api/reviews.test.tsx web/src/components/ChangeRequestDetail.tsx web/src/components/ChangeRequestDetail.test.tsx web/src/components/DiffViewer.tsx
git commit -m "feat: gate reviewer merge on viewed files"
```

---

## Task 10.6: Docs And Status

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/private-beta-contract.md`
- Modify if useful: `sdk/typescript/README.md`
- Modify if useful: `sdk/python/README.md`
- Modify if present/relevant: `docs/project-status.md`
- Modify if required by progress tracking: `markdownfs_v2_cto_architecture_plan.md`

**Step 1: Update HTTP API docs**

In `docs/http-api-guide.md`:

- Document `GET /change-requests/{id}/viewed-files`.
- Document `PUT /change-requests/{id}/viewed-files`.
- Update the old note that `require_all_files_viewed` is not enforced.
- Document merge `403` response for unviewed required files.
- State that viewed-file state is scoped to the authenticated actor and CR head commit.

**Step 2: Update private-beta contract**

In `docs/private-beta-contract.md`:

- Add the reviewer console expectation: changed files can be marked viewed.
- Add that merge is blocked only when the matched protection policy requires viewed files and the current actor still has unviewed required paths.

**Step 3: Update SDK docs only if they already document review methods**

If `sdk/typescript/README.md` has a review/change-request method section, add a small example:

```ts
await client.reviews.setViewedFile(changeRequestId, { path: "/runbooks/a.md", viewed: true });
const viewed = await client.reviews.listViewedFiles(changeRequestId);
```

Do the Python equivalent only if `sdk/python/README.md` has matching review docs.

**Step 4: Run docs diff check**

Run:

```bash
git diff --check -- docs/http-api-guide.md docs/private-beta-contract.md sdk/typescript/README.md sdk/python/README.md docs/project-status.md markdownfs_v2_cto_architecture_plan.md
```

Expected: no whitespace errors.

**Step 5: Commit docs**

Run:

```bash
git add docs/http-api-guide.md docs/private-beta-contract.md sdk/typescript/README.md sdk/python/README.md docs/project-status.md markdownfs_v2_cto_architecture_plan.md
git commit -m "docs: document viewed-file review gate"
```

Only stage files that actually changed.

---

## Task 10.7: Full Verification And Review

**Files:**

- No planned edits unless verification finds issues.

**Step 1: Run Rust checks**

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib review::tests -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --lib server::routes_review::tests -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --features postgres --lib backend::postgres_migrations -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --bin stratumctl -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task9-10 cargo test --locked --test cli_truth -- --nocapture
cargo fmt --all -- --check
```

Expected: PASS. If feature-gated Postgres live adapter tests require a DB URL and skip locally, state that in the final implementation handoff.

**Step 2: Run TypeScript SDK checks**

Run:

```bash
bun run --cwd sdk/typescript test:run -- client.test.ts
bun run --cwd sdk/typescript test:run
bun run --cwd sdk/typescript typecheck
```

Expected: PASS.

**Step 3: Run Python SDK checks**

Run:

```bash
cd sdk/python
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
cd ../..
```

Expected: PASS.

**Step 4: Run web checks**

Run:

```bash
pnpm --dir web test:run
pnpm --dir web typecheck
pnpm --dir web build
```

Expected: PASS. If `web/dist/` changes and it is not intentionally tracked for this repo, do not stage it.

**Step 5: Run global hygiene checks**

Run:

```bash
git diff --check
git status --short
find . -maxdepth 2 -type d -name target -print
```

Expected:

- `git diff --check` has no output.
- Status contains only intended tracked changes plus `.codex-scratch/`.
- No `./target` directory in the worktree.

**Step 6: Secret/content audit**

Run targeted searches:

```bash
rg -n "STRATUM_WORKSPACE_TOKEN|workspace-secret|Bearer |Idempotency-Key|postgres://|OPENAI_API_KEY|sk-[A-Za-z0-9]" docs sdk web src tests migrations
```

Expected:

- No real secrets.
- Matches are docs examples, test fixtures, redaction tests, or typed header names only.
- Viewed-file audit/idempotency details include paths and metadata only, not file content.

**Step 7: Fresh reviews**

Spawn fresh review agents or do fresh passes:

- Security review: viewed-file route auth, idempotency, path validation, audit redaction, merge gate order.
- Code-quality review: state model, persistence migration, route helper duplication, frontend state flow.
- UI review: loading/error/empty states, merge/reject confirmations, viewed-file progress, mobile wrapping.

Fix any findings before final commit/push handoff.

**Step 8: Final status**

Run:

```bash
git log --oneline --decorate -5
git status --short --branch
```

Expected: clean except `.codex-scratch/`.

Do not push from the smaller model session unless explicitly asked. The main session owns integration review, final verification, commit cleanup if needed, and push.

---

## Acceptance Criteria

- Review list/detail render polished loading, error, empty, and populated states.
- Reviewer console no longer exposes raw `uid:` labels in normal review copy.
- Approval comments and dismissal reasons render clearly.
- Comments and reviewer assignments render clear actor labels and empty states.
- Merge and reject flows require explicit confirmation.
- Backend persists viewed-file state per repo, CR, head commit, path, and actor.
- Backend blocks merge only when the effective approval policy requires all files viewed and the current actor has unviewed required paths.
- Backend does not block merge on viewed-file state when `require_all_files_viewed` is false.
- TypeScript and Python SDKs expose viewed-file list/update methods.
- Web diff UI reads and writes backend viewed-file state, never `sessionStorage`.
- Docs no longer claim `require_all_files_viewed` is unenforced.
- All verification commands pass or documented environment-only skips are clear.
