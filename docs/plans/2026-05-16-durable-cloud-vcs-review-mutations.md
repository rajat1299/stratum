# Durable-Cloud VCS/Review Mutations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable `STRATUM_CORE_RUNTIME=durable-cloud` to serve VCS ref/commit/revert and review/protected-change mutations from durable stores only.

**Architecture:** Add narrow durable VCS and review mutation capability seams, parallel to the existing durable FS mutation seam, so durable-cloud can reuse the guarded durable commit/ref/revert/review engines without making `DurableCoreRuntime::guarded_durable_commit_route()` return `Some`. Durable-cloud admin mutations use a minimal durable admin-principal seam: a repo-scoped workspace bearer token whose active durable principal is root or in wheel can act as admin for that repo; local `Authorization: User root` remains unsupported. Unsupported auth/login, workspace issuance, runs, audit listing, semantic search, execution, and recovery operator routes stay fail-closed.

**Tech Stack:** Rust, Axum, Tokio, `StratumStores`, durable object/commit/ref/review/idempotency/audit stores, Postgres feature-gated adapters, TypeScript/Python capability fixtures.

---

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `docs/plans/2026-05-16-durable-cloud-mounted-session-fs-mutations.md`
- `docs/plans/2026-05-16-pre-slice3-backend-followups.md`
- `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `docs/plans/2026-05-09-durable-status-diff-revert-parity.md`
- `docs/plans/2026-05-09-policy-review-audit-parity.md`
- `docs/plans/2026-05-12-policy-enforcement-below-route-layer.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

## Current Baseline

- Durable-cloud router supports FS/search/tree reads, mounted-session FS mutations, and VCS log/status/diff/refs list.
- Durable-cloud still hard-codes `501` for `POST /vcs/commit`, `POST /vcs/revert`, `POST /vcs/refs`, `PATCH /vcs/refs/{name}`, `/protected/*`, and `/change-requests/*`.
- Guarded local durable VCS commit/revert/ref routes already use durable stores, policy tokens, idempotency, audit, and recovery.
- Guarded local durable review creation and merge can resolve durable refs/commits and update durable target refs with source-checked CAS.
- Durable review/protected stores exist in Postgres and in-memory tests, but review routes currently assume local admin auth and reviewer lookup through `state.db.session_for_uid`.
- Durable-cloud cannot and must not rely on `Authorization: User root`; `DurableCoreRuntime::login` and bare bearer auth fail closed.

## Auth Pushback And Minimal Seam

Current durable auth is insufficient for safe VCS/review admin sessions if we mount existing handlers unchanged. The smallest seam for this slice is:

- Keep `Authorization: User <name>` unsupported under durable-cloud.
- Reuse repo-scoped workspace bearer validation because it already proves token hash, repo id, workspace id, active durable principal, expiry/revocation, token id, and token version.
- Permit scoped sessions to call durable-cloud VCS/review admin routes only when all are true:
  - server runtime is durable-cloud,
  - session has a mounted workspace with required repo id matching router/header repo,
  - mount has principal uid and token identity,
  - session principal is root or in wheel,
  - the route does not use local `.vfs/state.bin` or local user registry.
- Preserve existing local behavior: workspace bearer sessions remain rejected from local admin VCS/review routes.

This is not OIDC, SAML, SCIM, token issuance, or a complete capability system. It is a repo-bound durable admin-principal gate for this mutation slice only.

## Reference-Material Boundary

Use SMFS/Mirage only for vocabulary and operator/status thinking. Do not extract SMFS latest-wins push queues, SQLite inode/chunk caches, mutable timestamps, Mirage mutable resource storage, or process-local job storage into commit/ref/review paths. VCS/review state must remain keyed by repo, ref, commit/root tree/object identity, source CAS/version, policy token, idempotency identity, and audit/recovery records.

## Task 1: Plan And Baseline Commit

**Files:**
- Create: `docs/plans/2026-05-16-durable-cloud-vcs-review-mutations.md`

**Steps:**
1. Save this plan.
2. Run `git diff --check`.
3. Commit:

```bash
git add docs/plans/2026-05-16-durable-cloud-vcs-review-mutations.md
git commit -m "docs: plan durable-cloud vcs review mutations"
```

## Task 2: Durable Admin-Principal Seam

**Files:**
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Modify if needed: `src/server/routes_capabilities.rs`

**Steps:**
1. Write failing tests proving durable-cloud rejects `Authorization: User root` for newly enabled VCS/review mutations.
2. Add tests proving a repo-scoped workspace bearer backed by an active durable root/wheel principal can pass durable-cloud admin checks.
3. Add tests proving non-wheel durable principals, missing repo headers, cross-repo headers, missing workspace token identity, and normal local workspace bearers are rejected.
4. Implement a helper such as `require_admin_or_durable_admin_principal(state, headers, surface)` that preserves local `require_admin` behavior and allows only the durable-cloud admin-principal seam above.
5. Apply it to VCS mutation and review/protected route handlers.
6. Keep `require_durable_core_repo_context` in the path so repo mismatch fails before mutation.

Run:

```bash
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_cloud_admin --lib -- --nocapture
cargo test --locked server::routes_review::tests::durable_cloud_admin --lib -- --nocapture
```

Commit:

```bash
git add src/server/middleware.rs src/server/routes_vcs.rs src/server/routes_review.rs src/server/routes_capabilities.rs
git commit -m "feat: add durable-cloud admin principal seam"
```

## Task 3: Durable VCS Mutation Capability Seam

**Files:**
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_vcs.rs`

**Steps:**
1. Add `DurableVcsMutationRoute` wrapping `DurableCoreRuntime`, exposing only VCS mutation/read primitives needed by route handlers: `repo_id`, `stores`, `for_repo`, `list_refs`, `create_ref`, `update_ref_with_policy_token`, `commit_metadata_preflight`, `revert_plan`, `vcs_log_as`, `vcs_status_as`, and `vcs_diff_as`.
2. Add `CoreDb::durable_vcs_mutation_route() -> Option<DurableVcsMutationRoute>` defaulting to `None`.
3. Implement it for `LocalCoreRuntime` by converting the guarded route when present.
4. Implement it for `DurableCoreRuntime` by returning a first-class durable VCS route.
5. Do not implement `DurableCoreRuntime::guarded_durable_commit_route()`.
6. Change VCS route helpers to resolve this seam for durable VCS mutations while preserving guarded-local fallback behavior.
7. In durable-cloud `POST /vcs/commit`, require a workspace id whose durable workspace has a `session_ref`; do not fall back to `state.db.snapshot_fs_async()`.
8. Keep recovery operator routes unavailable in durable-cloud.

Run:

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_cloud --lib -- --nocapture
```

Commit:

```bash
git add src/server/core.rs src/server/routes_vcs.rs
git commit -m "feat: add durable vcs mutation route seam"
```

## Task 4: Mount Durable-Cloud VCS Mutations

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/mod.rs`

**Steps:**
1. Add durable-cloud tests for:
   - `POST /vcs/refs` create success, duplicate conflict, missing target, idempotency replay/conflict, and restart/rebuild persistence.
   - `PATCH /vcs/refs/{name}` success, stale CAS `409`, protected ref `403`, unknown target `400`, idempotency replay/conflict, and restart/rebuild persistence.
   - `POST /vcs/commit` promoting a durable mounted session ref to `main`, preserving idempotency replay and audit redaction, and returning a redacted partial/recovery-safe response for post-visible audit/idempotency failures.
   - `POST /vcs/revert` source-checked durable revert with recovery conflict, protected ref/path policy, idempotency replay, CAS `409`, invalid hash `400`, and redacted backend failure `500`.
2. Add explicit tests that local guarded durable behavior remains unchanged and `DurableCoreRuntime::guarded_durable_commit_route()` stays `None`.
3. Replace durable-cloud hard-coded `501` handlers for the four VCS mutation routes with handlers using the new seam.
4. Keep `/vcs/recovery` and `/vcs/recovery/run` stable `501` in durable-cloud.
5. Ensure policy allow/deny audit is appended before mutation; pre-mutation audit failure fails closed.
6. Ensure commit messages, idempotency keys, raw tokens, repo mismatch details, backend errors, object keys, and DB/R2 endpoints do not appear in HTTP errors, replay records, audit details, or logs.

Run:

```bash
cargo test --locked server::routes_vcs::tests::durable_cloud --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
```

Commit:

```bash
git add src/server/routes_vcs.rs src/server/mod.rs
git commit -m "feat: route durable-cloud vcs mutations"
```

## Task 5: Durable Review Mutation Capability Seam

**Files:**
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/workspace/mod.rs`
- Modify: `src/backend/postgres.rs`

**Steps:**
1. Add `DurableReviewMutationRoute` or extend the VCS seam with review-safe methods: `repo_id`, `stores`, `for_repo`, ref lookup, commit lookup, changed-path collection, and source-checked target-ref update.
2. Add `CoreDb::durable_review_mutation_route() -> Option<...>` defaulting to `None`.
3. Implement it for guarded local and durable-cloud without changing `guarded_durable_commit_route()`.
4. Replace review helpers currently checking `state.core.guarded_durable_commit_route()` with the review seam.
5. In durable-cloud, if durable refs are missing, return `404`/`409` as appropriate; never fall back to `state.db`.
6. Add a narrow principal lookup to the durable workspace/principal store so reviewer assignment can validate active durable reviewer principals without `state.db.session_for_uid`.
7. Implement the Postgres principal lookup against `durable_principals` with repo scoping and redacted errors.
8. Add in-memory test support for durable principal lookup.

Run:

```bash
cargo test --locked server::routes_review::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests::durable_cloud --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Commit:

```bash
git add src/server/core.rs src/server/routes_review.rs src/workspace/mod.rs src/backend/postgres.rs
git commit -m "feat: add durable review mutation route seam"
```

## Task 6: Mount Durable-Cloud Review And Protected Routes

**Files:**
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/mod.rs`

**Steps:**
1. Add durable-cloud route tests under `routes_review::tests::durable_cloud` for:
   - `POST /protected/refs` and `POST /protected/paths` create/list with durable persistence and idempotency replay.
   - `POST /change-requests` capturing durable source/target refs, base/head commits, no local VCS state, and secret-safe replay.
   - approvals, reviewers, comments, dismissal, reject, and merge over durable stores.
   - merge fast-forwards durable target ref through source-checked CAS only after approval state is approved.
   - protected refs/paths block direct VCS mutations but allow approved review merge.
   - terminal-state and same-key replay ordering remains unchanged.
   - restart/rebuild from same stores preserves review state and idempotency replay.
2. Mount review/protected routes in `build_durable_core_router()` before the durable unsupported catch-all or remove those catch-all entries once coverage proves exact routes are mounted.
3. Preserve local route behavior and existing guarded durable review merge tests.
4. Ensure review audit and replay bodies omit descriptions, comments, dismissal reasons, raw idempotency keys, raw tokens, and raw backend errors.

Run:

```bash
cargo test --locked server::routes_review::tests::durable_cloud --lib -- --nocapture
cargo test --locked server::routes_review --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_cloud --lib -- --nocapture
```

Commit:

```bash
git add src/server/routes_review.rs src/server/mod.rs
git commit -m "feat: route durable-cloud review mutations"
```

## Task 7: Capability Manifest, SDK Fixtures, And Docs

**Files:**
- Modify: `src/server/routes_capabilities.rs`
- Modify: `sdk/contracts/capabilities.v1.json`
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json`
- Modify if needed: `sdk/typescript/src/types.ts`
- Modify if needed: `sdk/python/src/stratum_sdk/types.py`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Steps:**
1. Bump `CAPABILITIES_REVISION`.
2. For durable-cloud, advertise only newly supported VCS/review/protected surfaces:
   - `vcs.commit`, `vcs.revert`, `vcs.refs.create`, `vcs.refs.update`
   - protected ref/path rules
   - change requests, approvals, reviewers, comments, dismiss, reject, merge
3. Keep durable-cloud auth login, workspace issuance/listing, runs, audit listing, semantic search, execution, and recovery routes unavailable with the stable unsupported reason.
4. Add manifest notes/requirements such as `workspace-bearer`, `durable-admin-principal`, `repo-bound-principal`, `durable-session-ref` where accurate.
5. Update idempotency endpoints so durable-cloud lists the newly supported mutation routes.
6. Regenerate capability fixtures with:

```bash
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
```

7. Update HTTP guide and project status with landed behavior, auth seam, unsupported boundaries, live-gate status, and residual risks.

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
cd sdk && bun run typecheck && bun run test:run
cd sdk/python && python -m pytest
git diff --check
```

Commit:

```bash
git add src/server/routes_capabilities.rs sdk/contracts/capabilities.v1*.json sdk/typescript/src/types.ts sdk/python/src/stratum_sdk/types.py docs/http-api-guide.md docs/project-status.md
git commit -m "docs: advertise durable-cloud vcs review mutations"
```

## Task 8: Startup, No-Local-State, And Provider Gates

**Files:**
- Modify: `tests/server_startup.rs`
- Modify only if needed: `src/server/mod.rs`

**Steps:**
1. Extend startup/integration coverage to prove durable-cloud VCS/review mutations run without `.vfs/state.bin`, `.vfs/review.bin`, local audit/idempotency/workspace files, or local `StratumDb`.
2. Add a positive durable-cloud route smoke using durable stores and admin-principal bearer auth:
   - create protected ref/path rule,
   - mutate mounted session FS,
   - commit to main or create review ref,
   - create CR,
   - approve and merge,
   - verify Postgres row counts when live provider is present.
3. Keep all existing fail-closed startup gates for missing Postgres/R2/migrations/secrets.
4. Do not add broad local fallback or recovery operator support in durable-cloud.

Run:

```bash
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
```

Commit:

```bash
git add tests/server_startup.rs src/server/mod.rs
git commit -m "test: cover durable-cloud vcs review startup boundaries"
```

## Task 9: Reviews And Fixes

**Files:**
- All changed files.

**Spec/correctness review focus:**
- Durable-cloud VCS/review mutations use durable stores only.
- `DurableCoreRuntime::guarded_durable_commit_route()` remains `None`.
- Admin access is durable-principal based, repo-bound, and does not accept local `User root`.
- No local `.vfs/state.bin`/`.vfs/review.bin` fallback exists.
- CAS conflicts map to `409`; invalid inputs to `400`; auth/policy to `401/403`; backend failures to redacted `500`.
- Policy allow/deny audit happens before mutation, and post-mutation audit/idempotency failures return recovery-safe partials.
- Capability manifest matches mounted router.

**Code-quality/security review focus:**
- Redaction of commit messages, review bodies/comments/reasons, raw tokens, idempotency keys, DB URLs, R2 endpoints, object keys, repo mismatch details, and raw backend errors.
- Route mount ordering and exact/catch-all conflicts.
- Token/principal API misuse resistance.
- Test reliability and no hidden latest-wins/path-cache semantics.

Fix confirmed Critical/Important findings locally; rerun focused tests for each touched area and commit fixes in small pieces.

## Final Verification

Run from `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation`:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::routes_vcs::tests::durable_cloud --lib -- --nocapture
cargo test --locked server::routes_review::tests::durable_cloud --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cd sdk && bun run typecheck && bun run test:run
cd sdk/python && python -m pytest
cargo audit --deny warnings
```

If live Postgres/R2 credentials are present, also run the required live gates. If credentials are absent, record `wired, not provider-verified`.

## Publish And Merge

After reviews and verification:

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git fetch origin
git merge --ff-only origin/v2/foundation
cargo fmt --all -- --check
git diff --check
cargo test --locked --lib --tests
git push origin main
```

Preserve unrelated untracked main-worktree files.

