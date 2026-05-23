# Org/Tenant Model Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative org/tenant model foundation so hosted durable requests resolve organization -> repo -> workspace identity without falling back to `RepoId::local()`.

**Architecture:** Introduce a small `OrgId` type, request tenant context, and tenant-aware repo context that bind hosted requests to `X-Stratum-Org` before `X-Stratum-Repo`. Extend workspace bearer identity and provider-free metadata stores to carry org identity consistently, while preserving local singleton behavior and keeping hosted multi-tenant rollout disabled by default. Add a non-destructive Postgres migration for organizations, memberships, service accounts, and `repos.org_id` backfill without changing existing repo primary keys.

**Tech Stack:** Rust, Axum, Tokio, existing `RepoId`/`RequestRepoContext`, workspace metadata stores, review/policy/idempotency route helpers, Postgres migrations, provider-free in-memory tests.

---

## Reference Material

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/http-api-guide.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-durable-auth-session-routing-foundation.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-12-tenant-repo-routing-foundation.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-23-sparse-write-back-commit-staging.md`
- Inventory subagents from 2026-05-23:
  - auth/session/workspace identity inventory
  - server routing/policy/review inventory
  - Postgres/schema migration inventory

## Current Baseline

- `RepoId` is a global ASCII identifier in `src/backend/mod.rs`. `RepoId::local()` is intentionally the local singleton compatibility value.
- `RequestRepoContext` in `src/server/repo_context.rs` resolves only repo identity from a workspace mount, `X-Stratum-Repo`, or an explicitly allowed local singleton fallback.
- `SessionMountIdentity`, `SessionMount`, `WorkspaceRecord`, `WorkspaceTokenRecord`, `ValidWorkspaceToken`, and `WorkspacePrincipalRecord` carry workspace/repo/principal/token identity but no org identity.
- `session_from_headers()` validates workspace bearers with `Authorization: Bearer` plus `X-Stratum-Workspace`; hosted/durable paths already reject missing repo metadata, workspace/token repo mismatches, and repo-scoped tokens without active durable principals.
- `ServerState::requires_explicit_workspace_repo()` is true when local DB is unavailable or a guarded durable route is enabled. FS/VCS mounted durable route helpers fail closed when repo identity is missing or mismatched.
- FS/VCS/review/workspace idempotency scopes are repo-qualified only for explicit non-local repo contexts.
- `RoutePolicyRequest` and `PolicyDecisionToken` are repo/workspace scoped, not org scoped.
- `ReviewStore` is repo-scoped through `*_for_repo` methods. In-memory/local implementations filter by repo, and Postgres review queries use `repo_id`.
- Postgres `repos` currently has no `org_id`; durable principals are repo-scoped through nullable `repo_id`, and `WorkspacePrincipalKind` already includes `ServiceAccount`.
- Durable-cloud remains readiness-gated and disabled unless explicit durable env gates pass; local singleton behavior remains the default runtime.

## Slice 15 Design

### Tenant Identity Model

- Add `OrgId` beside `RepoId`, with the same conservative identifier grammar: non-empty ASCII alphanumeric, `_`, `-`, max 128.
- Reserve a constant local/default org id for compatibility and migration backfill, such as `OrgId::default_org()` returning `default_org`.
- Add provider-free org records:
  - `OrgRecord { id, name, archived }`
  - `OrgMembershipRecord { org_id, uid, role, active }`
  - `OrgServiceAccountRecord { id, org_id, principal_uid, name, active }`
- Keep membership/service-account modeling minimal. This slice is not an auth productization slice, so no OIDC/SAML/SCIM, invitations, admin console, or provisioning routes.

### Request Ordering

- Add `X-Stratum-Org` as the hosted tenant selector.
- Add `RequestTenantContext` in or near `src/server/repo_context.rs`:
  - local mode may resolve to a local singleton tenant only when local singleton repo fallback is allowed;
  - hosted/durable mode requires an explicit org from `X-Stratum-Org` or a workspace mount;
  - duplicate/invalid org headers fail closed with fixed, redacted errors.
- Extend repo resolution so hosted/durable calls resolve tenant first, then repo:
  1. Resolve tenant from workspace mount or `X-Stratum-Org`.
  2. Resolve repo from workspace mount or `X-Stratum-Repo`.
  3. Reject mount/header org mismatch.
  4. Reject mount/header repo mismatch.
  5. Validate the `(org_id, repo_id)` pair against tenant metadata when required.
  6. Only local singleton mode may return `RepoId::local()` without org/repo headers.

### Workspace Bearer Binding

- Extend `SessionMountIdentity` and `SessionMount` with optional `org_id`.
- Extend `WorkspaceRecord`, `WorkspaceTokenRecord`, `ValidWorkspaceToken`, and `WorkspacePrincipalRecord` with optional `org_id`.
- Hosted/durable workspace bearer validation must require matching workspace/token/principal org and matching workspace/token repo before constructing a `Session`.
- Local compatibility remains unchanged:
  - existing persisted local metadata with no org and no repo still decodes;
  - local unmounted sessions can still use local singleton fallback;
  - local workspace token validation may keep `org_id: None` when `repo_id: None`.

### Tenant Metadata Store

- Add a small provider-free trait, likely `TenantMetadataStore`, with only the methods needed by request resolution and tests:
  - `resolve_repo_org(&self, repo_id: &RepoId) -> Result<Option<OrgId>, VfsError>`
  - `repo_belongs_to_org(&self, org_id: &OrgId, repo_id: &RepoId) -> Result<bool, VfsError>`
  - test/setup helpers to insert organizations, memberships, service accounts, and repo bindings.
- Add `InMemoryTenantMetadataStore` for normal tests.
- Add the store to `ServerStores`/`ServerState` so route helpers can validate hosted `(org, repo)` pairs before durable store use.
- Do not require local file persistence for tenant metadata in this slice unless needed for local singleton compatibility. Local singleton must remain unchanged.

### Postgres Migration

- Add `migrations/postgres/0015_org_tenant_foundation.sql`; do not rewrite `0001`.
- Create tables:
  - `organizations`
  - `org_memberships`
  - `org_service_accounts`
- Add `repos.org_id` non-destructively:
  - if `repos.org_id` already exists, preserve existing non-null values;
  - insert organization rows for existing non-null `repos.org_id` values;
  - insert `default_org`;
  - backfill only null `repos.org_id` to `default_org`;
  - set a default for future `ensure_repo` compatibility;
  - add/validate a foreign key to `organizations(id)`;
  - set `repos.org_id` not null after backfill;
  - add `repos_org_id_idx`.
- Do not change `repos.id` primary key or downstream `repo_id` foreign keys in this slice. Repo ids remain globally unique until a later org-local repo slug migration.
- Register migration 15 in `src/backend/postgres_migrations.rs`, extend adoption verification, and update any startup/migration count assertions.

### Policy, Audit, Idempotency, Review

- Thread org id through route policy request/details where route helpers already construct policy context.
- Add org id to hosted/durable idempotency scopes/fingerprints when a non-local tenant context is present. Keep legacy local singleton scopes unchanged.
- Review and policy data can remain repo-keyed for storage in this slice, but route-level request context must prevent cross-org repo selection before review/policy queries run.
- Public errors must remain bounded and redacted: no tokens, DB URLs, provider URLs, raw object keys, or unnecessary tenant internals.

### Rollback Boundary

- Hosted multi-tenant mode remains disabled by default.
- Local singleton mode remains unchanged.
- Durable-cloud readiness and non-server fail-closed behavior remain unchanged except for the explicit tenant-context requirement on hosted/durable request paths.
- No production tenant provisioning, OIDC/SAML/SCIM, hosted admin console, production KMS/secrets provider, Redis, SDK release, or crates.io publishing.

## Task 1: Plan And Inventory Commit

**Files:**
- Create: `docs/plans/2026-05-23-org-tenant-model.md`

**Step 1: Write the plan**

Record current repo-routing/auth/session/workspace state, tenant model, migration approach, route ordering, rollback boundary, and out-of-scope work.

**Step 2: Verify formatting of the doc**

Run:

```bash
git diff --check
```

Expected: PASS.

**Step 3: Commit**

```bash
git add docs/plans/2026-05-23-org-tenant-model.md
git commit -m "docs: plan org tenant model"
```

## Task 2: Tenant Identity And Request Context

**Files:**
- Modify: `src/backend/mod.rs`
- Modify: `src/server/repo_context.rs`
- Modify: `src/server/mod.rs`
- Test: `src/server/repo_context.rs`

**Step 1: Write failing tests**

Add tests proving:

```rust
#[test]
fn hosted_request_requires_org_before_repo_lookup() { /* red */ }

#[test]
fn local_singleton_allows_missing_org_and_repo_when_enabled() { /* red */ }

#[test]
fn invalid_and_duplicate_org_headers_are_rejected_with_fixed_message() { /* red */ }

#[test]
fn workspace_mount_org_and_header_org_mismatch_is_rejected() { /* red */ }

#[test]
fn hosted_repo_resolution_rejects_repo_outside_resolved_org() { /* red */ }
```

**Step 2: Run red tests**

Run:

```bash
cargo test --locked server::repo_context --lib -- --nocapture
```

Expected: new tests fail because `OrgId`, `X-Stratum-Org`, and tenant-aware repo resolution do not exist.

**Step 3: Implement minimal context types**

Add:

- `OrgId` validation and display helpers.
- `STRATUM_ORG_HEADER`.
- `RequestTenantContext`.
- `RequestRepoContext` org binding or a new `RequestTenantRepoContext` wrapper.
- A small in-memory tenant metadata test helper if route state integration is not needed yet.

Keep existing `RequestRepoContext::resolve()` behavior available for local callers until route call sites are updated.

**Step 4: Run green tests**

Run:

```bash
cargo test --locked server::repo_context --lib -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/backend/mod.rs src/server/repo_context.rs src/server/mod.rs
git commit -m "feat: add tenant request context"
```

## Task 3: Workspace Bearer Org Binding

**Files:**
- Modify: `src/auth/session.rs`
- Modify: `src/workspace/mod.rs`
- Modify: `src/server/middleware.rs`
- Modify: tests in the same modules

**Step 1: Write failing tests**

Add tests proving:

```rust
#[tokio::test]
async fn hosted_workspace_bearer_requires_matching_workspace_token_and_principal_org() { /* red */ }

#[tokio::test]
async fn workspace_bearer_org_header_mismatch_is_rejected() { /* red */ }

#[tokio::test]
async fn local_workspace_bearer_without_org_still_uses_local_singleton_compatibility() { /* red */ }
```

Also add session tests for `SessionMountIdentity` org storage and redacted debug output.

**Step 2: Run red tests**

Run:

```bash
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
```

Expected: new tests fail because workspace/session identity has no org field.

**Step 3: Implement minimal model changes**

Add optional `org_id` to:

- `SessionMountIdentity`
- `SessionMount`
- `WorkspaceRecord`
- `WorkspaceTokenRecord`
- `ValidWorkspaceToken`
- `WorkspacePrincipalRecord`

Update in-memory/local decode paths with `#[serde(default)]` compatibility. Require hosted/durable org consistency in `session_from_headers()` before building the mounted session.

**Step 4: Run green tests**

Run the same three commands from Step 2.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/auth/session.rs src/workspace/mod.rs src/server/middleware.rs
git commit -m "feat: bind workspace bearers to org identity"
```

## Task 4: Route Tenant Ordering And Cross-Org Denial

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/policy.rs`
- Test: route tests in the same modules

**Step 1: Write failing tests**

Add provider-free tests proving:

```rust
#[tokio::test]
async fn hosted_fs_rejects_missing_org_before_repo_lookup() { /* red */ }

#[tokio::test]
async fn hosted_vcs_rejects_cross_org_repo_selector() { /* red */ }

#[tokio::test]
async fn review_admin_selector_cannot_cross_org_boundary() { /* red */ }

#[tokio::test]
async fn same_repo_slug_in_different_orgs_uses_distinct_idempotency_scope() { /* red */ }

#[tokio::test]
async fn local_singleton_route_behavior_is_unchanged_without_org() { /* red */ }
```

**Step 2: Run red tests**

Run the focused route suites:

```bash
cargo test --locked server::routes_fs::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked server::policy --lib -- --nocapture
```

Expected: new tests fail because route helpers do not resolve tenant before repo and idempotency/policy context is repo-only.

**Step 3: Implement minimal route integration**

Update shared route helpers to use tenant-aware context where hosted/durable explicit repo context is required. Add org id to hosted/durable idempotency scope/fingerprint and policy details. Preserve local singleton short-circuits for unmounted local sessions.

**Step 4: Run green tests**

Run the same five commands from Step 2.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/server/routes_fs.rs src/server/routes_vcs.rs src/server/routes_workspace.rs src/server/routes_review.rs src/server/policy.rs
git commit -m "feat: resolve hosted tenant before repo routes"
```

## Task 5: Postgres Org/Tenant Migration

**Files:**
- Create: `migrations/postgres/0015_org_tenant_foundation.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `src/backend/postgres.rs`
- Modify if needed: `tests/server_startup.rs`
- Test: `src/backend/postgres_migrations.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Write failing tests**

Add provider-free migration string/catalog tests for:

```rust
#[test]
fn org_tenant_migration_is_non_destructive_and_backfills_repos_org_id() { /* red */ }

#[test]
fn migration_catalog_requires_org_tables_and_repos_org_id() { /* red */ }
```

Add live-Postgres tests behind the existing `STRATUM_POSTGRES_TEST_URL` skip gate for applying migration 15 and verifying FK/check/index behavior.

**Step 2: Run red tests**

Run:

```bash
cargo test --locked backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: new tests fail because migration 15 is absent.

**Step 3: Add migration and catalog support**

Create the migration with:

- `organizations`
- `org_memberships`
- `org_service_accounts`
- safe `repos.org_id` add/backfill/default/FK/not-null/index

Register migration 15, extend `verify_known_schema_catalog()`, and update readiness probes to include `repos.org_id` and tenant tables. Preserve existing `ensure_repo` insert compatibility through the `repos.org_id` default.

**Step 4: Run green tests**

Run the same commands from Step 2.

Expected: provider-free tests pass; live Postgres portions skip cleanly when env is unset.

**Step 5: Commit**

```bash
git add migrations/postgres/0015_org_tenant_foundation.sql src/backend/postgres_migrations.rs src/backend/postgres.rs tests/server_startup.rs
git commit -m "feat: add org tenant postgres schema"
```

## Task 6: Docs, Status, And Review Fixes

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/plans/2026-05-23-org-tenant-model.md`
- Modify code files only for review fixes

**Step 1: Update docs**

Document:

- `X-Stratum-Org` is required for hosted/durable tenant-aware paths.
- Hosted request order is org -> repo -> workspace.
- Local singleton mode is unchanged.
- Hosted multi-tenant provisioning/admin UI remains disabled.
- Migration 15 backfills `repos.org_id` to `default_org` without changing repo ids.
- Public outputs remain redacted and bounded.

**Step 2: Run spec review**

Dispatch a review subagent with `gpt-5.5` xhigh reasoning. Ask it to compare the diff against this plan and acceptance criteria. Fix Critical/Important findings only after verifying locally.

**Step 3: Run code-quality/security review**

Dispatch a second review subagent with `gpt-5.5` xhigh reasoning. Ask it to focus on Rust API quality, fail-closed tenant authorization, redaction, migration safety, and local compatibility. Fix Critical/Important findings only after verifying locally.

Review results recorded for the implemented slice:

- Spec/correctness review found that VCS workspace-header validation and head updates were still repo-only, allowing a workspace from a different org to match the same repo slug. The fix uses org/repo workspace validation and head updates for commit/revert paths.
- Spec/correctness review found that local-compatible route helpers skipped tenant/repo resolver validation for explicit `X-Stratum-Org` or `X-Stratum-Repo` selectors. The fix preserves no-header local singleton fallback while validating explicit local-mode selectors, except the compatibility `default_org` plus `local` pair.
- Spec/correctness review found that org-qualified idempotency scopes still counted per-repo quotas without org identity. The fix adds org to quota identity, keeps legacy no-org local scopes separate, and applies the same org-aware matching to Postgres quota checks.
- Code-quality/security review found that durable Postgres server startup used an empty in-memory tenant resolver after readiness checks. The fix loads `(org_id, repo_id)` bindings from Postgres `repos` into the startup resolver.
- Code-quality/security review noted that `repos.id` remains globally keyed. That is an explicit Slice 15 boundary: repo ids remain globally unique and downstream `repo_id` foreign keys are not migrated to org-local slugs until a later slice.

**Step 4: Run final gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_fs::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Record skipped provider-backed portions only when the command output proves the skip reason.

**Step 5: Commit final docs/fixes**

```bash
git add docs/project-status.md docs/http-api-guide.md docs/plans/2026-05-23-org-tenant-model.md <review-fix-files>
git commit -m "docs: record org tenant model boundaries"
```

## Final Acceptance Checklist

- Plan documents current routing/auth/session/workspace state, org model, migration approach, ordering, rollback boundary, and out-of-scope auth/productization.
- Local singleton mode is unchanged and covered by tests.
- Hosted/durable paths cannot fall back to `RepoId::local()` when org/repo/workspace identity is missing or mismatched.
- Tenant resolution happens before repo lookup and produces bounded/redacted request context.
- Organizations, memberships, and service accounts are represented provider-free and in Postgres migration/model where feasible.
- Durable Postgres startup loads tenant/repo bindings from persisted `repos.org_id` before serving hosted requests.
- `repos.id` remains globally unique in this slice; org-local repo slugs and downstream FK changes are intentionally deferred.
- Workspace bearer sessions bind org/repo/workspace/token/principal identity consistently.
- Cross-org repo access is denied before route handlers use repo-scoped stores.
- Idempotency, policy, audit, and review semantics remain repo/org scoped where touched.
- Existing durable-cloud startup gates remain fail-closed.
- Hosted multi-tenant mode remains disabled by default.
- Normal tests require no Postgres, R2, network, durable-cloud env, or live credentials.
