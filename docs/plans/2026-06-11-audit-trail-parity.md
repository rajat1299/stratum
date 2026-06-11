# Audit Trail Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the private-beta audit trail parity gaps so golden-path actions are represented in typed clients, the web audit screen renders them safely, and durable-cloud audit listing remains explicitly unsupported until tenant/repo-scoped listing exists.

**Architecture:** Keep local `/audit` as the only listable audit route for private beta. Durable-cloud continues to persist audit events through the durable audit store, but the HTTP listing route stays fail-closed because the current `AuditStore::list_recent` contract is global and the local `/audit` auth model is not repo-scoped. The implementation is a surgical parity pass: add missing SDK action coverage, make the audit UI use capability limits and bounded details, add backend characterization tests for the already-emitted private-beta audit events, and update posture docs.

**Tech Stack:** Rust/Axum audit and route tests, Postgres audit adapter tests, TypeScript SDK, React 19, TanStack Query, Vitest/Testing Library, Bun, pnpm.

---

## Read First

- `/Users/rajattiwari/virtualfilesystem/AGENTS.md`
- `/var/folders/hp/gn9_bs093l31rdpkq98yyj2h0000gn/T/stratum-task11-audit-trail-parity-handoff.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `docs/private-beta-contract.md`
- `docs/http-api-guide.md`
- `docs/audit-posture.md`
- `docs/plans/2026-05-01-audit-event-scaffolding.md`
- `docs/plans/2026-05-03-postgres-audit-adapter-foundation.md`
- `docs/plans/2026-05-09-policy-review-audit-parity.md`
- `docs/plans/2026-06-02-event-bus-audit-pipeline.md`
- `docs/plans/2026-05-15-frontend-roadmap.md`
- `docs/plans/2026-06-04-conformance-test-scaffolding.md`
- `docs/plans/2026-06-10-reviewer-console-file-viewed-gate.md`

## Preflight

Work in:

```bash
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/private-beta-closeout
export CARGO_TARGET_DIR=/tmp/stratum-target-task11
git fetch origin main
git status --short --branch
git rev-parse HEAD origin/main
```

Expected:

- Branch is `private-beta-closeout`.
- `HEAD` and `origin/main` are `fb10697d3ae7e4be4e01237333819ee997bdc077` or intentionally advanced together.
- Only unrelated untracked scratch state is acceptable, especially `?? .codex-scratch/`.
- Do not delete, stage, or commit `.codex-scratch/`.
- Do not leave `target/` in the worktree.
- Do not enable durable-cloud `/audit` in this slice.

## Current Findings

- Rust audit emission already covers workspace create/token issue/token revoke, FS write/mkdir/delete/copy/move/metadata update, VCS commit/ref create/ref update/revert, protected rules, policy allow/deny, idempotency quota, and change-request create/approve/reviewer assignment/file-view/comment/dismiss/reject/merge.
- Local `/audit` is admin-user-session only and explicitly rejects bearer tokens.
- Durable-cloud `/audit` is currently mounted under `durable_unsupported_routes()` and returns the stable `501` unsupported JSON.
- Durable capabilities and conformance fixtures already advertise durable audit listing as unavailable.
- Postgres audit persistence exists, but append currently stores audit rows with `repo_id = NULL`; some repo identity lives in details for dedupe, which is not a safe listing boundary.
- `sdk/typescript/src/types.ts` is missing the `change_request_file_view` audit action even though Rust emits it.
- `web/src/components/AuditPlaceholder.tsx` is a live audit screen, but it hardcodes limit `50`, does not use capability audit limits, labels `change_request_file_view` through fallback text, and renders every audit detail inline.

## Durable-Cloud Stance

Keep durable-cloud `/audit` explicitly unsupported for private beta.

Do not mount `routes_audit::routes()` into the durable router. A safe hosted audit route needs a separate contract:

- durable-specific auth requiring workspace bearer, active durable root/wheel principal, repo-bound principal, and explicit org/repo context;
- tenant/repo resolver validation with no local fallback;
- an `AuditStore` API that lists by repo/org boundary rather than global recent events;
- Postgres writes that persist authoritative `repo_id`/tenant identity for every event, not only detail metadata.

Until that exists, the beta-compatible parity is: golden-path durable mutations persist audit events in Postgres, capabilities/conformance/docs state that hosted audit listing is unavailable, and the web screen shows an unavailable hosted-preview state without calling `/audit`.

## Task 1: TypeScript Audit Action Parity

**Files:**

- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/tests/client.test.ts`

**Step 1: Write the failing type coverage**

In `sdk/typescript/tests/client.test.ts`, add a type-checked assertion near the audit list test:

```ts
it("types private-beta audit actions emitted by review routes", () => {
  const action: AuditAction = "change_request_file_view";

  expect(action).toBe("change_request_file_view");
});
```

Import `AuditAction` as a type if it is not already imported.

**Step 2: Run test/typecheck to verify red**

Run:

```bash
bun run --cwd sdk/typescript typecheck
```

Expected: FAIL because `"change_request_file_view"` is not assignable to `AuditAction`.

**Step 3: Implement the minimal fix**

Add `"change_request_file_view"` to the `AuditAction` union in `sdk/typescript/src/types.ts`, next to the other change-request actions.

**Step 4: Verify green**

Run:

```bash
bun run --cwd sdk/typescript typecheck
bun run --cwd sdk/typescript test:run -- client.test.ts
```

Expected: PASS.

## Task 2: Web Audit Screen Production Parity

**Files:**

- Modify: `web/src/components/AuditPlaceholder.tsx`
- Modify: `web/src/components/AuditPlaceholder.test.tsx`

Keep the filename/component name for this slice to avoid rename churn. It can be renamed to `AuditScreen` in a separate cosmetic cleanup.

**Step 1: Write failing tests**

Add focused tests in `AuditPlaceholder.test.tsx`:

1. Local capabilities use the manifest audit default limit:

```ts
await waitFor(() =>
  expect(fetchSpy.mock.calls.some(([input]) => String(input).includes("/audit?limit=100"))).toBe(
    true,
  ),
);
```

Update the existing live rendering test from limit `50` to `100`.

2. A `change_request_file_view` event renders as a clear production label such as `Viewed file`, not the fallback `Change request file view`.

3. Long or risky detail maps are bounded in the row. Use an event with more than six details and keys such as `token`, `request_body`, `sql`, `provider_error`, `safe_ref`, and `viewed`. Assert safe metadata renders and raw-risk keys do not render.

**Step 2: Run tests to verify red**

Run:

```bash
bun run --cwd web test:run -- AuditPlaceholder.test.tsx
```

Expected: FAIL on default limit, missing file-view label, and unbounded/risky details.

**Step 3: Implement the minimal UI fix**

In `AuditPlaceholder.tsx`:

- Initialize the selected limit from `capabilities.data.limits.audit_default_limit` once capabilities are known; fall back to `100` only before capabilities are loaded.
- Clamp selectable limits to `capabilities.data.limits.audit_max_limit`.
- Add `change_request_file_view: "Viewed file"` to `actionLabel`.
- Render only a bounded safe detail preview. Keep useful keys like `route`, `change_request_id`, `source_ref`, `target_ref`, `base_commit`, `head_commit`, `path`, `viewed`, `viewed_by`, `version`, `ref`, `quota_kind`, and `route_family`. Suppress keys matching raw-risk terms such as token, hash, request body, SQL, provider error, DB URL, R2 endpoint/object key, file content, commit message, and idempotency key.

Do not add new server query filters in this slice because the backend `/audit` route only supports `limit`.

**Step 4: Verify green**

Run:

```bash
bun run --cwd web test:run -- AuditPlaceholder.test.tsx
bun run --cwd web typecheck
```

Expected: PASS.

## Task 3: Backend Audit Coverage Characterization

**Files:**

- Modify: `src/audit.rs`
- Modify: `src/server/routes_review.rs`
- Modify if needed: `src/backend/postgres.rs`

This is mostly coverage for behavior already present. If the new tests pass immediately, do not force production changes.

**Step 1: Add private-beta action/export-class coverage**

In `src/audit.rs`, add a test that serializes/deserializes representative private-beta actions and checks `AuditExportClass::from_action`:

- workspace: `WorkspaceCreate`, `WorkspaceTokenIssue`, `WorkspaceTokenRevoke`
- filesystem: `FsWriteFile`, `FsMkdir`, `FsDelete`, `FsCopy`, `FsMove`, `FsMetadataUpdate`
- version control: `VcsCommit`, `VcsRevert`, `VcsRefCreate`, `VcsRefUpdate`
- policy/idempotency: `PolicyDecisionAllow`, `PolicyDecisionDeny`, `IdempotencyQuotaExceeded`
- protected/review: `ProtectedRefRuleCreate`, `ProtectedPathRuleCreate`, `ChangeRequestCreate`, `ChangeRequestApprove`, `ChangeRequestFileView`, `ChangeRequestReviewerAssign`, `ChangeRequestCommentCreate`, `ChangeRequestApprovalDismiss`, `ChangeRequestReject`, `ChangeRequestMerge`

**Step 2: Add route-owned redaction assertions**

In `src/server/routes_review.rs`, extend existing file-view and create/reviewer tests so rendered audit events do not include:

- raw idempotency key values;
- request body field names like `title`, `description`, or comment body;
- raw token/hash/provider/backend error strings;
- file content or commit messages.

Keep intentional metadata such as changed paths, refs, commit ids, route, version, and actor ids.

**Step 3: Run focused backend tests**

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked audit::tests --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked server::routes_review::tests --lib -- --nocapture
```

Expected: PASS. If a test fails for a real leak, fix the specific route audit detail construction and rerun.

**Step 4: Postgres adapter check**

Only add a Postgres audit roundtrip test if enum/action parity fails under the adapter. Prefer adding representative `ChangeRequestFileView`, protected-rule, and workspace-token events to the existing `run_audit_contracts` coverage rather than creating a new broad fixture.

Run if touched:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked backend::postgres --lib -- --nocapture
```

Expected: PASS with live Postgres portions skipped when `STRATUM_POSTGRES_TEST_URL` is unset.

## Task 4: Durable-Cloud Stance And Docs

**Files:**

- Modify: `docs/audit-posture.md`
- Modify: `docs/private-beta-contract.md`
- Modify if needed: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update audit posture**

Clarify that:

- local/Postgres audit persistence is the source of record;
- durable-cloud mutations persist audit events in Postgres where supported;
- hosted durable `/audit` listing remains unsupported until repo/tenant-scoped listing exists;
- clients should rely on capabilities and must not fall back to local `.vfs` state.

**Step 2: Update private-beta contract**

In the golden path section, make explicit that local web `/audit` shows golden-path audit events, including file-view and merge-gate decisions, while hosted durable preview advertises audit listing unavailable.

**Step 3: Update project status**

Add a short Task 11 note describing the parity stance, tests, and remaining future work for repo-scoped hosted audit listing.

**Step 4: Verify docs-only diff**

Run:

```bash
git diff -- docs/audit-posture.md docs/private-beta-contract.md docs/http-api-guide.md docs/project-status.md
```

Expected: docs reflect the stance without claiming durable `/audit` support.

## Task 5: Final Verification And Review

Run the focused matrix:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked audit::tests --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked server::routes_audit --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked server::routes_capabilities --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked server::conformance --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked server::routes_review::tests --lib -- --nocapture
bun run --cwd web test:run -- AuditPlaceholder.test.tsx capabilities.test.ts capabilities-sync.test.ts
bun run --cwd web typecheck
bun run --cwd web build
bun run --cwd sdk/typescript test:run -- client.test.ts
bun run --cwd sdk/typescript typecheck
cargo fmt --all -- --check
git diff --check
find . -maxdepth 2 -name target -type d -print
```

If `src/backend/postgres.rs` changes, also run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task11 cargo test --locked backend::postgres --lib -- --nocapture
```

Expected:

- All touched focused tests pass.
- `find` prints nothing.
- No checked-in capability/conformance fixture changes unless behavior intentionally changed.

## Done Criteria

- TypeScript SDK includes every Rust private-beta audit action emitted by the golden path.
- Web `/audit` uses capability limits, labels file-view audit events, keeps durable-cloud unavailable behavior fail-closed, and does not dump risky detail keys inline.
- Backend tests lock private-beta action/export-class coverage and review-route audit redaction.
- Docs clearly distinguish durable audit persistence from unsupported hosted audit listing.
- Verification matrix passes or any skipped live-provider portions are explicitly reported.
