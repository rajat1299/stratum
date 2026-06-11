# Revert Rollback Closure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let a reviewer/operator revert a merged change request and immediately see bounded audit/commit evidence, with stale or conflict responses called out clearly.

**Architecture:** Keep the existing `POST /vcs/revert` backend contract and reviewer-console flow. Widen SDK revert result types to reflect the richer durable response while preserving local `{ reverted_to }`, invalidate audit queries after a revert, render a compact success/error panel in the merged-CR action row, and let the local audit list preview bounded revert metadata.

**Tech Stack:** Rust backend contracts already exist; this slice touches TypeScript/Python SDK types, React/TanStack Query console hooks/components, Vitest, TypeScript typecheck, and focused Rust route tests only if behavior drift is found.

**Source Docs:** `/Users/rajattiwari/virtualfilesystem/stratum_current_state_cto_review.md` recommends the narrow private-beta wedge: hosted/scoped agent edit, review, merge, audit trail, and revert. `docs/private-beta-contract.md`, `docs/audit-posture.md`, and `docs/plans/2026-05-15-frontend-roadmap.md` keep durable-cloud audit listing out of scope while making local audit/revert part of the golden path.

---

### Task 1: Revert Result Type Parity

**Files:**
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Test: `sdk/typescript/tests/client.test.ts`

**Step 1: Write the failing test**

Add a TypeScript SDK test that stubs `POST /vcs/revert` with:

```json
{
  "reverted_to": "1111111111111111111111111111111111111111111111111111111111111111",
  "revert_commit": "2222222222222222222222222222222222222222222222222222222222222222",
  "target_ref": "main",
  "target_commit": "1111111111111111111111111111111111111111111111111111111111111111",
  "expected_head": "3333333333333333333333333333333333333333333333333333333333333333"
}
```

Assert `client.vcs.revert()` returns all optional evidence fields.

**Step 2: Run test to verify it fails**

Run:

```bash
bun run --cwd sdk/typescript typecheck
```

Expected: FAIL because `StratumRevertResult` exposes only `reverted_to`.

**Step 3: Write minimal implementation**

Add optional fields to both SDK result types:

```ts
export interface StratumRevertResult {
  readonly reverted_to: string;
  readonly revert_commit?: string;
  readonly target_ref?: string;
  readonly target_commit?: string;
  readonly expected_head?: string;
}
```

Python `TypedDict` should keep `reverted_to` required and mark the new fields `NotRequired[str]`.

**Step 4: Run test to verify it passes**

Run:

```bash
bun run --cwd sdk/typescript typecheck
bun run --cwd sdk/typescript test:run -- client.test.ts
```

### Task 2: Revert Mutation Refreshes Audit Evidence

**Files:**
- Modify: `web/src/lib/api/reviews.ts`
- Test: `web/src/lib/api/reviews.test.tsx`

**Step 1: Write the failing test**

Extend `useRevertChangeRequest` invalidation coverage to assert it invalidates the audit query prefix:

```ts
expect(calledKeys).toContainEqual(["audit"]);
```

**Step 2: Run test to verify it fails**

Run:

```bash
bun run --cwd web test:run -- src/lib/api/reviews.test.tsx
```

Expected: FAIL because only review query keys are invalidated.

**Step 3: Write minimal implementation**

In `onSuccess`, add:

```ts
void queryClient.invalidateQueries({ queryKey: ["audit"] });
```

This matches the audit screen's `["audit", "list", limit]` key prefix without exporting a component-local key.

**Step 4: Run test to verify it passes**

Run:

```bash
bun run --cwd web test:run -- src/lib/api/reviews.test.tsx
```

### Task 3: Merged-CR Revert Success And Conflict UX

**Files:**
- Modify: `web/src/components/ChangeRequestDetail.tsx`
- Test: `web/src/components/ChangeRequestDetail.test.tsx`

**Step 1: Write failing success-evidence test**

Add a test for a merged CR where `/vcs/revert` returns the durable evidence fields. After confirm, assert the detail screen shows:
- restored commit short hash from `reverted_to`
- new revert commit short hash from `revert_commit`
- target ref from `target_ref`
- an audit wording tied to the VCS revert event

**Step 2: Run test to verify it fails**

Run:

```bash
bun run --cwd web test:run -- src/components/ChangeRequestDetail.test.tsx
```

Expected: FAIL because success data is not rendered.

**Step 3: Write minimal success implementation**

Render a compact success panel when `revert.data` is present. Keep it metadata-only:

- "Reverted main to `<short>`"
- "Revert commit `<short>`" only when present
- "Audit records this as a VCS revert event."

Do not render raw backend details, request bodies, idempotency keys, commit messages, or file content.

**Step 4: Write failing stale/conflict test**

Add a test where `/vcs/revert` returns `409` with a bounded error such as `{"error":"ref compare-and-swap mismatch"}`. Assert the alert includes a clear stale/conflict heading and the bounded backend message.

**Step 5: Run test to verify it fails**

Run:

```bash
bun run --cwd web test:run -- src/components/ChangeRequestDetail.test.tsx
```

Expected: FAIL because `ActionError` prints only the raw error message.

**Step 6: Write minimal conflict implementation**

Teach `ActionError` to identify `status === 409` and render:

- "Revert conflict or stale head."
- the bounded message on the next line

Keep other mutation errors unchanged.

**Step 7: Run test to verify it passes**

Run:

```bash
bun run --cwd web test:run -- src/components/ChangeRequestDetail.test.tsx
```

### Task 4: Docs And Status Closure

**Files:**
- Modify: `web/src/components/AuditPlaceholder.tsx`
- Test: `web/src/components/AuditPlaceholder.test.tsx`
- Modify: `docs/project-status.md`
- Modify if needed: `docs/private-beta-contract.md`
- Modify if needed: `docs/audit-posture.md`

**Step 1: Write failing audit evidence test**

Add an audit-screen test for a `vcs_revert` event whose details include `target_ref`, `reverted_to`, `target_commit`, `expected_head`, and a risky `request_body`. Assert the bounded fields render and the risky body does not.

**Step 2: Run test to verify it fails**

Run:

```bash
bun run --cwd web test:run -- src/components/AuditPlaceholder.test.tsx
```

Expected: FAIL because only `target_ref` is whitelisted.

**Step 3: Write minimal audit preview implementation**

Add `reverted_to`, `target_commit`, and `expected_head` to the audit detail preview safe key set. Do not add generic `hash`, request body, token, provider, SQL, object-key, or commit-message fields.

**Step 4: Run test to verify it passes**

Run:

```bash
bun run --cwd web test:run -- src/components/AuditPlaceholder.test.tsx
```

**Step 5: Update status**

Add `Task 12 / Revert Rollback Closure` above Task 11 with concise completed scope and focused verification commands.

**Step 6: Update contract only if wording is stale**

If implementation changes public behavior, update the private-beta contract. If the existing contract already covers revert and audit listing correctly, leave it unchanged.

**Step 7: Verify docs**

Run:

```bash
git diff --check
```

### Task 5: Final Verification And Commit

**Files:**
- All touched files

**Step 1: Run focused verification**

Run:

```bash
bun run --cwd sdk/typescript typecheck
bun run --cwd sdk/typescript test:run -- client.test.ts
bun run --cwd web typecheck
bun run --cwd web test:run -- src/lib/api/reviews.test.tsx src/components/ChangeRequestDetail.test.tsx src/components/AuditPlaceholder.test.tsx
CARGO_TARGET_DIR=/tmp/stratum-target-task12 cargo test --locked server::routes_vcs::tests::revert --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task12 cargo test --locked server::routes_vcs::tests::guarded_durable_revert --lib -- --nocapture
git diff --check
```

**Step 2: Review diff**

Confirm:
- no raw tokens, idempotency keys, DB URLs, provider errors, commit messages, request bodies, file content, or object keys are surfaced
- durable-cloud `/audit` listing remains unsupported
- local and durable revert response compatibility is preserved
- `.codex-scratch/` remains untracked and unstaged

**Step 3: Commit**

```bash
git add docs/plans/2026-06-11-revert-rollback-closure.md docs/project-status.md sdk/typescript/src/types.ts sdk/typescript/tests/client.test.ts sdk/python/src/stratum_sdk/types.py web/src/lib/api/reviews.ts web/src/lib/api/reviews.test.tsx web/src/components/ChangeRequestDetail.tsx web/src/components/ChangeRequestDetail.test.tsx web/src/components/AuditPlaceholder.tsx web/src/components/AuditPlaceholder.test.tsx
git commit -m "fix: close revert rollback evidence gaps"
```
