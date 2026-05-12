# Tenant/Repo Routing Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace hosted durable `RepoId::local()` assumptions with explicit request repo routing foundations while preserving local singleton compatibility.

**Architecture:** Add a small request repo context at HTTP ingress, then thread that context into workspace bearer sessions, guarded durable route selection, review/protected-rule evaluation, and idempotency scopes. `RepoId::local()` remains a deliberate local-compatibility value; hosted/durable paths must resolve repo identity from a workspace session or an admin repo selector and fail closed when missing or mismatched. Broad `STRATUM_CORE_RUNTIME=durable-cloud` stays disabled.

**Tech Stack:** Rust, Axum, Tokio, existing `RepoId`, `CoreDb`/`GuardedDurableCommitRoute`, workspace metadata stores, review stores, HTTP idempotency helpers, Postgres metadata adapter and migrations.

---

## Reference Material

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-12-policy-enforcement-below-route-layer.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- Session/workspace reconnaissance, review reconnaissance, and durable/idempotency reconnaissance subagent findings from 2026-05-12.

## Current Baseline

- Durable object, commit, ref, recovery, workspace, audit, idempotency, and review tables already have repo-aware foundations in Postgres, but several Rust traits and route seams still behave as local singleton APIs.
- `WorkspaceRecord.repo_id` and mounted session `repo_id` exist as optional strings. Local metadata and legacy migrations still use `None`.
- Workspace bearer middleware copies `repo_id` into the session mount, but hosted/durable mode does not yet require it.
- `GuardedDurableCommitRoute` wraps a single `DurableCoreRuntime` with one repo id. Server construction currently uses `RepoId::local()` for the guarded route.
- Policy decisions and tokens carry `repo_id`, but `evaluate_route_policy()` still reads global review rules.
- `ReviewStore` is repo-unscoped. Postgres review tables are repo-aware, but `PostgresMetadataStore` hard-codes `RepoId::local()` through `review_repo_id()`.
- HTTP idempotency records are keyed by `(scope, key_hash)`. Current FS scopes are workspace-only and VCS/review/workspace scopes are route-only.

## Non-Goals

- Do not enable broad `STRATUM_CORE_RUNTIME=durable-cloud`.
- Do not build org membership, OIDC/SAML, hosted console, MCP/FUSE durable routing parity, final-object deletion/GC, hosted Postgres TLS/pooling/KMS, or idempotency retention/quota.
- Do not make local metadata files require repo ids.
- Do not store raw tokens, raw idempotency keys, request bodies with secrets, DB URLs, object-store errors, file content, approval/comment body text, or commit messages in audit/idempotency/recovery details.

## Task 1: Request Repo Context Seam

**Files:**
- Create: `src/server/repo_context.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/core.rs`
- Modify: `src/auth/session.rs`
- Test: `src/server/repo_context.rs`
- Test: `src/server/middleware.rs`

**Steps:**

1. Add `RequestRepoContext` with:
   - `repo_id: RepoId`
   - source enum: `LocalSingleton`, `WorkspaceMount`, `AdminHeader`
   - `local_singleton()` returning `RepoId::local()`
   - `from_workspace_mount(session)` parsing `mount.repo_id()`
   - `from_admin_header(headers)` parsing `X-Stratum-Repo`
   - `for_durable_request(session, headers, allow_local_singleton)` that prefers workspace mount repo, then admin header, then local singleton only when explicitly allowed.
2. Add small helpers to `SessionMount`:
   - `repo_id_result()` or equivalent to parse optional mount repo into `RepoId`
   - keep `repo_id() -> Option<&str>` unchanged for compatibility.
3. Add `GuardedDurableCommitRoute::for_repo(repo_id: RepoId)` so routes can reuse the same store bundle with a request-selected repo.
4. Keep server construction local-compatible: guarded route can still be seeded with `RepoId::local()`, but routes must use request repo context before durable store work.
5. Add tests proving:
   - missing repo resolves to local only in local singleton mode
   - invalid `X-Stratum-Repo` fails closed
   - mounted workspace repo overrides header ambiguity by rejecting mismatches
   - hosted/durable mode without workspace repo or admin header fails closed.

Run:

```bash
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked server::middleware::tests --lib -- --nocapture
```

## Task 2: Workspace Session Repo Enforcement

**Files:**
- Modify: `src/workspace/mod.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_workspace.rs`
- Test: `src/workspace/mod.rs`
- Test: `src/server/routes_workspace.rs`

**Steps:**

1. Preserve existing local APIs as wrappers, but add repo-aware workspace creation helpers such as `create_workspace_in_repo(repo_id, ...)`.
2. For local/in-memory/local-file stores, keep legacy `create_workspace*` producing `repo_id: None`; repo-aware helpers produce `Some(repo_id)`.
3. For Postgres workspace creation, support inserting `repo_id = Some(repo)` and call `ensure_repo`.
4. In middleware, reject hosted/durable workspace bearer validation when the workspace/token repo is missing or mismatched. Keep local `repo_id: None` fallback through `state.core.session_for_uid()` unchanged.
5. In workspace routes, use `RequestRepoContext` for admin creation/list/get/token operations when durable routing is involved. Local singleton routes keep current behavior.
6. Add route tests proving a workspace token for repo A cannot be used with a repo B selector and cannot produce a repo B mounted session.

Run:

```bash
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
```

## Task 3: Repo-Aware Review Store And Policy Evaluation

**Files:**
- Modify: `src/review.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/server/policy.rs`
- Modify: `src/server/routes_review.rs`
- Modify if needed: `migrations/postgres/*.sql`
- Test: `src/review.rs`
- Test: `src/server/policy.rs`
- Test: `src/server/routes_review.rs`

**Steps:**

1. Add `repo_id` to `ProtectedRefRule`, `ProtectedPathRule`, and `ChangeRequest`. Use `RepoId::local()` as the default for older local persisted records.
2. Add repo-aware `ReviewStore` methods for create/list/get protected rules, create/list/get/transition change requests, child records, and `approval_decision`.
3. Keep old trait ergonomics as wrappers only where needed, delegating to `RepoId::local()` for local singleton callers.
4. Update in-memory/local review state so records are filtered by repo. Avoid cross-repo UUID lookups for change requests and child records.
5. Update Postgres review adapter to read/write `repo_id` from existing columns instead of calling `review_repo_id()`.
6. Update `evaluate_route_policy()` to require and use `request.repo_id` for repo-scoped rule lookup. Local singleton callers should pass `RepoId::local()`.
7. Update review routes to resolve repo context before list/create/read/approve/comment/reject/merge and reject a routed repo that does not match the stored change request repo.
8. Add tests proving:
   - repo A and repo B can both protect `main` and `/legal` independently
   - repo A rules do not block repo B requests
   - approval decisions count only rules and approvals in the same repo
   - review merge cannot advance a target ref in a different repo.

Run:

```bash
cargo test --locked review::tests --lib -- --nocapture
cargo test --locked server::policy --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

## Task 4: Guarded Durable Route Plumbing

**Files:**
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Test: `src/server/core.rs`
- Test: `src/server/routes_fs.rs`
- Test: `src/server/routes_vcs.rs`
- Test: `src/server/routes_review.rs`

**Steps:**

1. Resolve `RequestRepoContext` before guarded durable FS reads/mutations, VCS refs/log/status/diff/commit/revert, and review merge.
2. Replace route-level uses of singleton `capability.repo_id()` with a request-scoped capability from `capability.for_repo(repo_context.repo_id().clone())`.
3. Ensure workspace-mounted durable mutations require the mounted workspace repo to match the request repo and the session ref belongs to that repo.
4. Keep local singleton compatibility for existing local tests and non-durable routes.
5. Keep broad durable runtime startup and `DurableCoreRuntime::route_execution_enabled()` fail-closed.
6. Add focused tests proving two repos can use the same ref names and session refs without durable metadata collision.

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

## Task 5: Repo-Aware Idempotency Scopes

**Files:**
- Modify: `src/server/idempotency.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify if needed: `src/backend/postgres.rs`
- Modify if needed: `migrations/postgres/*.sql`
- Test: route idempotency tests in FS/VCS/review/workspace

**Steps:**

1. Add a helper that returns legacy local scopes for `RequestRepoContext::LocalSingleton` and repo-qualified scopes otherwise.
2. Use repo-qualified scopes for FS, VCS, review, and workspace admin mutations when repo context is explicit.
3. Include the resolved repo id in idempotency request fingerprints for explicit repo contexts.
4. Keep local idempotency replay behavior unchanged for existing local singleton requests.
5. Add tests proving the same `Idempotency-Key` can be used independently in repo A and repo B for protected rules, workspace creation, VCS ref update, and mounted FS mutation.

Run:

```bash
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

## Task 6: Documentation And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify if needed: `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- Modify: `docs/plans/2026-05-12-tenant-repo-routing-foundation.md`

**Steps:**

1. Update status to record tenant/repo routing foundations as the latest backend slice.
2. Fix `Recommended Next Slices` so policy enforcement is no longer listed as upcoming.
3. Record what remains fail-closed:
   - broad `STRATUM_CORE_RUNTIME=durable-cloud`
   - MCP/FUSE durable repo routing parity
   - hosted Postgres TLS/pooling/KMS
   - org membership and hosted admin model
   - idempotency retention/quota
   - final-object deletion/GC.
4. Keep the status factual and short.

Run:

```bash
git diff --check
```

## Required Reviews

1. Spec/correctness review focus:
   - Hosted/durable paths cannot silently fall back to `RepoId::local()`.
   - Workspace token for repo A cannot access repo B.
   - Review/protected rules and approval decisions are scoped by repo.
   - Review merge cannot advance a target ref in a different repo.
   - Same idempotency key in two repos does not collide.
   - Local singleton behavior remains compatible.
   - Broad durable runtime remains fail-closed.

2. Code-quality/security review focus:
   - Repo-context API is small and hard to bypass.
   - No raw tokens, raw idempotency keys, file content, DB URLs, object-store errors, or review body text leak into replay/audit/recovery details.
   - Local compatibility is explicit, not accidental.
   - Postgres adapter queries consistently filter by repo where repo-scoped tables are involved.

## Final Verification

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked review::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```
