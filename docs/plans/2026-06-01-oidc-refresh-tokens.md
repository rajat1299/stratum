# OIDC And Refresh Tokens Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative provider-free OIDC and durable refresh-token foundation while preserving existing local user, agent bearer, and workspace bearer authentication.

**Architecture:** Add a separate hosted-auth model instead of changing existing `Authorization: Bearer` semantics. OIDC login uses injected provider-free verification and tenant-bound principal lookup, access sessions use an explicit `Authorization: Stratum-Session <token>` scheme, and refresh tokens are stored hash-only with transactional issue, rotate, revoke, expire, and stale-reuse denial. Postgres schema changes are additive and hosted providers remain disabled by default behind explicit runtime/config gates.

**Tech Stack:** Rust 2024, Axum, Tokio, serde, uuid, SHA-256 token hashing, existing `Session`, `ServerState`, audit, workspace principal, Postgres migration runner, and provider-free tests.

---

## Implementation Status (2026-06-01)

- Tasks 1-8 are implemented and committed through the Slice 16a documentation update.
- The provider-free foundation is in place: hosted auth token domain, explicit `Stratum-Session` authentication, OIDC/refresh routes, redacted audit lifecycle events, Postgres migration 0016, runtime gates, and focused regression gates.
- Hosted auth remains disabled by default. Production IdP/JWKS network verification, production KMS/secrets-manager integration, hosted org provisioning, hosted admin UI, SAML, SCIM, and broad tenant provisioning UI remain out of scope.
- Focused verification completed with hosted auth, session, middleware, route, audit, workspace, runtime, Postgres migration, Postgres backend, and durable startup tests. Live Postgres/R2 portions skipped when local provider env was unset.
- Final review and full verification remain the next plan task.

---

## Current State

- `POST /auth/login` in `src/server/routes_auth.rs` is local-user-only and calls `state.core.login(&req.username)`.
- `session_from_headers()` in `src/server/middleware.rs` supports local `Authorization: User ...`, legacy agent `Authorization: Bearer ...`, and workspace bearer tokens when `X-Stratum-Workspace` is present.
- Workspace bearer validation already checks workspace/token/principal org and repo identity, rejects missing hosted repo identity for durable contexts, and avoids local fallback for hosted durable workspace records.
- `SessionMount` carries optional org/repo/workspace/token/principal identity, but unmounted `Session` has no tenant identity.
- Postgres migration 15 added `organizations`, `org_memberships`, `org_service_accounts`, and org-bound workspace/token/principal columns.
- No durable hosted OIDC provider, external identity, hosted access session, refresh-token family, refresh-token record, or auth lifecycle audit model exists.

## Boundaries

- Do not change existing `/auth/login` response or semantics.
- Do not change existing local `Authorization: User ...`, agent `Authorization: Bearer ...`, or workspace bearer behavior.
- Do not treat bare `Authorization: Bearer ...` as hosted OIDC access-token auth.
- Use `Authorization: Stratum-Session <access-token>` for hosted access sessions.
- Normal tests must not call a real IdP, JWKS URL, Postgres, R2, network, durable-cloud credentials, or live provider.
- Hosted providers remain disabled by default.
- Public errors and audit details must not contain raw authorization codes, ID tokens, access tokens, refresh tokens, client secrets, provider error bodies, DB URLs, issuer URLs when configured as sensitive, request bodies, or token hashes.

## Task 1: Model Hosted Auth Sessions And Refresh Tokens

**Files:**
- Create: `src/auth/hosted.rs`
- Modify: `src/auth/mod.rs`

**Step 1: Write failing tests**

Add tests in `src/auth/hosted.rs` for:

- `access_token_debug_redacts_raw_secret`
- `refresh_token_debug_redacts_raw_secret`
- `refresh_store_issues_hash_only_token`
- `refresh_store_rotates_and_invalidates_previous_token`
- `refresh_store_rejects_revoked_expired_and_stale_tokens`
- `refresh_store_reuse_marks_family_compromised_without_issuing_successor`

Run:

```bash
cargo test --locked auth::hosted --lib -- --nocapture
```

Expected: fails because `auth::hosted` does not exist.

**Step 2: Implement minimal hosted auth domain**

Create:

- `HostedSessionIdentity` with `session_id`, `org_id`, `repo_id`, `uid`, `username`, `gid`, `groups`, and `external_identity_id`.
- `IssuedAccessToken` and `IssuedRefreshToken` with custom `Debug` that redacts raw secrets.
- `RefreshTokenRecord` with `id`, `family_id`, `session_id`, `org_id`, `repo_id`, `principal_uid`, `token_hash`, `version`, `issued_at_unix`, `expires_at_unix`, `rotated_at_unix`, `rotated_to_token_id`, `revoked_at_unix`, and `reuse_denied_at_unix`.
- `RefreshTokenFamilyRecord` with `id`, `session_id`, `org_id`, `repo_id`, `principal_uid`, `revoked_at_unix`, and `reuse_detected_at_unix`.
- `InMemoryHostedAuthStore` with hash-only issue, rotate, revoke, expire, and stale-reuse behavior.

Keep token generation local and provider-free. Use constant-time hash comparison helper style already used in `src/workspace/mod.rs`.

**Step 3: Run test**

```bash
cargo test --locked auth::hosted --lib -- --nocapture
```

Expected: new hosted auth tests pass.

**Step 4: Commit**

```bash
git add src/auth/mod.rs src/auth/hosted.rs
git commit -m "feat: add hosted auth token domain"
```

## Task 2: Add Tenant Identity To Sessions And Middleware

**Files:**
- Modify: `src/auth/session.rs`
- Modify: `src/server/middleware.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/repo_context.rs`

**Step 1: Write failing tests**

Add tests for:

- `auth::session::tenant_identity_is_available_without_workspace_mount`
- `server::middleware::stratum_session_authenticates_without_local_user`
- `server::middleware::stratum_session_org_header_mismatch_is_rejected`
- `server::middleware::stratum_session_does_not_change_agent_bearer`
- `server::repo_context::session_tenant_supplies_hosted_org`
- `server::repo_context::session_tenant_and_header_org_mismatch_is_rejected`

Run:

```bash
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
```

Expected: fails because hosted session identity is not wired.

**Step 2: Implement session and middleware support**

- Add optional hosted tenant/session identity to `Session`.
- Add a `with_hosted_identity()` builder and accessor.
- Add `hosted_auth` store to `ServerState` and `ServerStores`, defaulting to a disabled/in-memory provider-free store for local tests.
- In `session_from_headers()`, add an explicit `Authorization: Stratum-Session <token>` branch before unsupported-scheme handling.
- Do not change `Bearer` handling.
- Build `Session` from hosted identity without calling `state.core.login()` or `state.core.session_for_uid()`.
- Extend repo/tenant resolution only where route code needs hosted session org identity.

**Step 3: Run tests**

```bash
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
```

Expected: new and existing tests pass.

**Step 4: Commit**

```bash
git add src/auth/session.rs src/server/middleware.rs src/server/mod.rs src/server/repo_context.rs
git commit -m "feat: authenticate hosted tenant sessions"
```

## Task 3: Add Provider-Free OIDC Login And Refresh Routes

**Files:**
- Modify: `src/server/routes_auth.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/auth/hosted.rs`

**Step 1: Write failing route tests**

Add tests in `src/server/routes_auth.rs`:

- `oidc_login_denies_disabled_provider_without_session_or_secret_leak`
- `oidc_login_rejects_tenant_mismatch_without_local_fallback`
- `oidc_login_success_issues_tenant_scoped_session_and_refresh`
- `refresh_rotates_token_and_invalidates_previous_refresh`
- `refresh_revoke_blocks_future_refresh`
- `refresh_expiry_denies_without_rotation`
- `stale_refresh_reuse_fails_closed`
- `auth_public_errors_redact_provider_and_token_material`
- existing `login_routes_through_core_runtime` still passes unchanged

Run:

```bash
cargo test --locked server::routes_auth --lib -- --nocapture
```

Expected: fails because OIDC and refresh routes are not implemented.

**Step 2: Implement provider-free route layer**

- Add route handlers:
  - `POST /auth/oidc/login`
  - `POST /auth/refresh`
  - `POST /auth/refresh/revoke`
- Add request/response structs with bounded fields.
- Add a provider-free verifier trait and disabled default implementation.
- Tests inject a fake verifier that returns bounded verified claims; production default denies hosted provider auth.
- Require requested `org_id`/`repo_id` to match verified claims and tenant repo resolver binding.
- Issue access and refresh tokens through the hosted auth store.
- Rotate refresh tokens atomically in the store.
- Revoke refresh token records without returning raw token material.
- Emit bounded, redacted audit events for login issue, refresh issue, rotate, revoke, expiry denial, and reuse denial.

**Step 3: Run test**

```bash
cargo test --locked server::routes_auth --lib -- --nocapture
```

Expected: route auth tests pass.

**Step 4: Commit**

```bash
git add src/server/routes_auth.rs src/server/mod.rs src/auth/hosted.rs
git commit -m "feat: add provider-free oidc refresh routes"
```

## Task 4: Add Auth Lifecycle Audit Types

**Files:**
- Modify: `src/audit.rs`
- Modify if needed: `src/backend/postgres.rs`
- Modify if needed: `src/server/routes_audit.rs`

**Step 1: Write failing tests**

Add tests for snake-case round trip and redaction-safe detail shape:

- `auth_lifecycle_audit_enums_round_trip_as_snake_case`
- `auth_lifecycle_audit_details_are_bounded`

Run:

```bash
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked server::routes_audit --lib -- --nocapture
```

Expected: fails until enum variants exist.

**Step 2: Implement audit variants**

Add actions:

- `AuthOidcLoginDenied`
- `AuthOidcLoginSuccess`
- `AuthRefreshTokenIssue`
- `AuthRefreshTokenRotate`
- `AuthRefreshTokenRevoke`
- `AuthRefreshTokenExpireDenied`
- `AuthRefreshTokenReuseDenied`

Add resource kinds:

- `AuthProvider`
- `ExternalIdentity`
- `HostedSession`
- `RefreshToken`

Use only provider key/id, org id, repo id, principal uid, token id, token family id, and reason enum in details.

**Step 3: Run tests**

```bash
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked server::routes_audit --lib -- --nocapture
```

Expected: audit tests pass.

**Step 4: Commit**

```bash
git add src/audit.rs src/backend/postgres.rs src/server/routes_audit.rs
git commit -m "feat: record hosted auth audit events"
```

## Task 5: Add Postgres Migration 0016

**Files:**
- Create: `migrations/postgres/0016_oidc_refresh_token_foundation.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Modify if needed: `src/backend/postgres.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing migration tests**

Add or update tests for:

- migration catalog count/name includes version 16
- known schema verifier requires new OIDC/refresh tables
- weakened/missing refresh token constraints fail adoption
- startup migration apply/status expectations use current catalog length instead of stale constants where possible

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
```

Expected: fails until 0016 is registered and verifier updated.

**Step 2: Add additive SQL**

Add tables:

- `oidc_providers`
- `external_identities`
- `refresh_token_families`
- `refresh_tokens`

Use `CREATE TABLE IF NOT EXISTS`, guarded constraints, finite timestamp checks, hash shape checks, FK constraints to `organizations`, `repos`, `durable_principals`, and unique indexes for provider key, subject hash, and active refresh token family shape. Do not store raw provider secrets or raw subjects.

**Step 3: Register and verify**

- Add `include_str!` constant.
- Change `POSTGRES_MIGRATIONS` length to 16.
- Append version 16 with name `oidc_refresh_token_foundation`.
- Extend schema verification table/column/index/constraint/FK checks.
- Keep migration adoption non-destructive.

**Step 4: Run tests**

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: Postgres feature tests pass or live portions skip cleanly when env is unset.

**Step 5: Commit**

```bash
git add migrations/postgres/0016_oidc_refresh_token_foundation.sql src/backend/postgres_migrations.rs src/backend/postgres.rs tests/server_startup.rs
git commit -m "feat: add oidc refresh postgres schema"
```

## Task 6: Add Hosted Auth Runtime Gates

**Files:**
- Modify: `src/backend/runtime.rs`
- Modify: `tests/server_startup.rs`
- Modify: `src/bin/stratum_server.rs` if startup ordering needs explicit provider preflight

**Step 1: Write failing tests**

Add tests for:

- OIDC hosted auth disabled by default
- partial `STRATUM_OIDC_*` env fails closed with redacted fixed message
- invalid provider mode fails closed
- explicit complete provider-free test config parses without leaking secrets in `Debug`
- durable-cloud still checks existing readiness gates before hosted provider validation
- server startup with partial OIDC config creates no local `.vfs` files

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected: fails until runtime config exists.

**Step 2: Implement runtime config**

- Add `HostedAuthRuntimeConfig` with disabled default.
- Add explicit env names for hosted auth provider-free foundation, for example:
  - `STRATUM_HOSTED_AUTH_PROVIDER`
  - `STRATUM_HOSTED_AUTH_ENABLE_DEV`
  - bounded non-secret provider fields for tests only
- Any partial hosted auth config fails closed with env-name-only errors.
- `Debug` must show only booleans such as `configured: true`, never raw secrets.
- Do not enable real IdP network validation.

**Step 3: Run tests**

```bash
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected: runtime and startup tests pass or live portions skip cleanly.

**Step 4: Commit**

```bash
git add src/backend/runtime.rs tests/server_startup.rs src/bin/stratum_server.rs
git commit -m "feat: gate hosted auth runtime config"
```

## Task 7: Regression Gates For Existing Auth

**Files:**
- Modify tests only if regressions require targeted updates.

**Step 1: Run focused existing auth gates**

```bash
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_audit --lib -- --nocapture
```

Expected: all pass.

**Step 2: Fix only regressions caused by this slice**

Do not broaden behavior or rewrite unrelated route surfaces.

**Step 3: Commit if fixes were required**

```bash
git add <changed files>
git commit -m "fix: preserve existing auth behavior"
```

## Task 8: Documentation And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: `docs/plans/2026-06-01-oidc-refresh-tokens.md`

**Step 1: Update docs**

Document:

- New provider-free OIDC/refresh foundation.
- Hosted auth disabled by default.
- `Authorization: Stratum-Session <access-token>` as the hosted session scheme.
- Existing `Bearer` semantics unchanged.
- Hash-only refresh storage and one-time raw refresh return.
- Rotation/revocation/expiry/reuse-denial behavior.
- Redacted audit events.
- Postgres migration 0016.
- Out-of-scope SAML, SCIM, hosted admin console, production IdP network integration, production KMS/secrets-manager, and broad tenant provisioning UI.

**Step 2: Run docs-adjacent checks**

```bash
git diff --check
```

Expected: no whitespace errors.

**Step 3: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md docs/plans/2026-06-01-oidc-refresh-tokens.md
git commit -m "docs: record oidc refresh token foundation"
```

## Task 9: Final Review And Verification

**Files:**
- No planned edits except review fixes.

**Step 1: Run spec review subagent**

Ask a reviewer to compare implementation against this plan and the Slice 16a acceptance criteria.

**Step 2: Run code-quality/security review subagent**

Ask a reviewer to focus on auth bypass, tenant fallback, token storage, redaction, migration safety, and preserving legacy auth behavior.

**Step 3: Fix findings with failing tests first**

Use focused TDD for every behavioral fix.

**Step 4: Run final verification**

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Expected: all provider-free gates pass; live Postgres/R2 portions skip when env is unset.

**Step 5: Push and merge**

- Push `v2/foundation`.
- Create a temporary clean `main` worktree.
- Merge `v2/foundation` into `main`.
- Run required post-merge smoke gates.
- Push `main`.
