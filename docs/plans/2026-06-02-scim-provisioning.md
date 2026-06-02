# SCIM Provisioning Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative provider-free SCIM 2.0 user and group provisioning foundation behind the hosted tenant model, while preserving OIDC, SAML, refresh-token, local user, agent bearer, workspace bearer, and hosted `Stratum-Session` behavior.

**Architecture:** Reuse the Slice 15 org/tenant model, the Slice 16a hosted access-session and refresh-token machinery, and the Slice 16b external-identity binding shape. SCIM gets its own provider-free authentication seam (a tenant-scoped SCIM client identified by a token hash), a typed user/group/membership provisioning domain in `src/auth/hosted.rs`, a disabled-by-default `/scim/v2/*` route group in a new `src/server/routes_scim.rs`, redacted audit lifecycle events, and a new additive deprovisioning revocation store method that invalidates a principal's hosted access and refresh-token families. SCIM is orthogonal to the login-provider mode and stays disabled by default behind its own runtime gate. Durable schema changes are additive through migration 0018.

**Tech Stack:** Rust 2024, Axum, Tokio, serde, uuid, SHA-256 token hashing, HMAC-SHA256 SCIM external-id hashing, existing `auth::hosted`, `Session`, `ServerState`, `TenantRepoResolver`, audit store, idempotency store, Postgres migration runner, and provider-free tests.

---

## Implementation Status

- Planned on 2026-06-02 for branch `v2/foundation` (base `32169cf`, Slice 16b SAML SSO Foundation complete).
- Slice 16c provider-free SCIM provisioning foundation is implemented on `v2/foundation`.
- Task 7 docs/status records the implemented foundation: disabled-by-default `/scim/v2/*` routes, `Scim-Bearer` auth, tenant-scoped SCIM clients/users/groups/memberships, idempotent retry behavior, hosted access/refresh revocation on deprovisioning, independent SCIM runtime gates, migration 0018, unchanged existing auth flows, redaction rules, and out-of-scope productization boundaries.

Completed foundation scope:

- Added a disabled-by-default `/scim/v2/*` route group with `POST /scim/v2/Users`, `PATCH /scim/v2/Users/{user_id}`, `DELETE /scim/v2/Users/{user_id}`, `POST /scim/v2/Groups`, and `PATCH /scim/v2/Groups/{group_id}`.
- Added the distinct `Authorization: Scim-Bearer <token>` scheme for SCIM routes only. SCIM requests also require `X-Stratum-Org` and `X-Stratum-Repo`; local/root/session fallback is not allowed.
- Added tenant-scoped SCIM clients, external-user bindings to existing tenant principals, external-group mappings to local gids, and active group memberships. User/group creates and membership changes are idempotent on the represented tenant/client identity.
- Added deprovisioning revocation for represented hosted state: deactivate/delete revokes current hosted access tokens plus refresh-token families/tokens for the bound principal.
- Hardened external-id storage with HMAC-SHA256 keyed by the presented SCIM bearer token, so predictable SCIM external ids are not persisted as plain unsalted hashes.
- Added independent SCIM runtime config: `STRATUM_HOSTED_SCIM_PROVIDER=scim-dev`, `STRATUM_HOSTED_SCIM_ENABLE_DEV=1`, `STRATUM_SCIM_PROVIDER_KEY`, and `STRATUM_SCIM_CLIENT_TOKEN_HASH`. SCIM composes with `oidc-dev` or `saml-dev` and does not alter the login-provider mutual-exclusion model.
- Added Postgres migration 0018 (`scim_provisioning_foundation`) with `scim_clients`, `scim_users`, `scim_groups`, and `scim_group_members`, storing hashes/bounded references only and enforcing strict adoption checks.
- Preserved existing OIDC, SAML, refresh-token, local `User`, agent `Bearer`, workspace bearer, tenant resolution, and hosted `Stratum-Session` behavior.
- Kept hosted admin UI, broad provisioning UI, principal auto-provisioning, production SCIM provider/network integration, production secrets-manager/KMS integration, and SDK releases out of scope.
- Kept SCIM public errors, responses, audit details, debug output, and logs redacted: no raw request/response bodies, SCIM bearer tokens, token hashes, raw external ids, hosted access/refresh tokens, DB URLs, sensitive provider URLs, or provider error bodies.

## Current State

- `src/auth/hosted.rs` owns provider-free hosted access sessions (`HostedSessionIdentity`), access-token issuance (`issue_access_token` / `validate_access_token_at`), refresh-token families (`RefreshTokenFamilyRecord` / `RefreshTokenRecord`), rotation/revoke/expiry/stale-reuse handling, the disabled default OIDC verifier (`DisabledHostedOidcVerifier`), the disabled default SAML verifier (`DisabledHostedSamlVerifier`), SAML metadata/assertion validation, assertion replay tracking, group mapping (`SamlGroupMapping`), the SAML external-identity binding model (`SamlExternalIdentityBinding`, `saml_external_identity_is_bound`), and `InMemoryHostedAuthStore`. The store has no by-principal bulk revocation method today: revocation is `revoke_refresh_token` (single token by id) and `revoke_refresh_token_secret` (by raw secret, marks family revoked). Access-token records (`StoredAccessTokenRecord`) already carry `revoked_at_unix`, but nothing revokes access tokens by principal.
- `src/auth/session.rs` `Session` carries optional `hosted_identity: Option<HostedSessionIdentity>` via `with_hosted_identity` / `hosted_identity`, independent of workspace mount. There is no SCIM concept.
- `src/server/routes_auth.rs` exposes `POST /auth/oidc/login`, `POST /auth/saml/login`, `POST /auth/refresh`, and `POST /auth/refresh/revoke`. It owns the shared `HostedTokenResponse`, `hosted_token_response`, bounded-field helpers (`bounded_auth_field`, `bounded_provider_key`, `MAX_AUTH_FIELD_BYTES`, `MAX_PROVIDER_KEY_BYTES`), the tenant-validation pattern (verify → check `claims.org/repo == requested`, then `repo_belongs_to_org`, then binding), and audit emission helpers (`append_auth_audit`, `*_event` builders, `hosted_audit_actor`). There is no SCIM route, request struct, or audit helper.
- `src/server/middleware.rs` authenticates requests through `Authorization: Bearer <token>` (agent/workspace), `Authorization: Stratum-Session <access-token>` (hosted session), and `Authorization: User <username>` (local). There is no SCIM scheme.
- `src/server/repo_context.rs` resolves tenant before repo via `TenantRepoResolver` / `InMemoryTenantRepoResolver` and rejects org/repo header mismatch. `ServerState::repo_belongs_to_org` wraps the resolver.
- `src/server/mod.rs` builds the router; hosted routes are mounted via a `hosted_routes()` group. `ServerState` exposes `hosted_auth`, `tenant_repos`, `audit`, and `idempotency`.
- `src/audit.rs` `AuditAction` has `AuthOidcLoginDenied`, `AuthOidcLoginSuccess`, `AuthSamlLoginDenied`, `AuthSamlLoginSuccess`, `AuthRefreshTokenIssue`, `AuthRefreshTokenRotate`, `AuthRefreshTokenRevoke`, `AuthRefreshTokenExpireDenied`, `AuthRefreshTokenReuseDenied`. `AuditResourceKind` has `AuthProvider`, `ExternalIdentity`, `HostedSession`, `RefreshToken`. There is no SCIM action or resource kind.
- `src/backend/runtime.rs` defaults hosted auth to disabled. `HostedAuthProviderMode { Disabled, OidcDev, SamlDev }` is a single mutually-exclusive *login-provider* mode parsed from `STRATUM_HOSTED_AUTH_PROVIDER`; OIDC and SAML env are mutually exclusive (`mixed_hosted_auth_config`). Helpers `validate_provider_key`, `validate_lower_hex_sha256`, `incomplete_hosted_auth_config`, `required_config_value`, `optional_value` exist. There is no SCIM runtime config.
- `src/backend/postgres_migrations.rs` registers `POSTGRES_MIGRATIONS: [PostgresMigration; 17]`; migration 17 is `saml_sso_foundation`. Adoption verification checks exact table/column/constraint/index/FK/default shape. There is no migration 18 or SCIM schema.
- `migrations/postgres/0015_org_tenant_foundation.sql` provides `organizations(id)`, `org_memberships(org_id, principal_uid, role, active, …)` PK `(org_id, principal_uid)`, `org_service_accounts`, `repos(org_id, id)` UNIQUE `(org_id, id)`, and `durable_principals(org_id, repo_id, uid, …)` with composite key `(org_id, repo_id, uid)`. SAML tables (0017) FK to `repos(org_id, id)` and `durable_principals(org_id, repo_id, uid)`. These are the membership/principal tables SCIM users, groups, and memberships bind to.
- Normal tests are provider-free: no real IdP/SCIM provider, no Postgres/R2/network/durable-cloud env, no live credentials.

## Boundaries

- Do not implement a hosted admin console, broad tenant/user provisioning UI, production SCIM provider/network integration, production IdP metadata fetching, production XML signature validation, production KMS/secrets-manager, password/passkey/MFA productization, Redis, remote durable MCP serving, semantic search, execution runner, SDK releases, or crates.io publishing.
- Do not allocate brand-new durable principals/uids in this slice. SCIM "create user" binds an external SCIM user to an **already-provisioned** tenant principal (mirroring the SAML external-identity model). Principal auto-provisioning from SCIM payloads is explicit follow-on; manual/admin membership management stays available (the rollback boundary).
- Do not weaken or alter existing OIDC, SAML, refresh-token, local `User`, agent `Bearer`, workspace bearer, tenant resolution, or hosted `Stratum-Session` behavior, the `HostedAuthProviderMode` login-provider enum, or its OIDC/SAML mutual-exclusion invariant. SCIM config is a separate, independent gate.
- Normal tests must not call a real IdP/SCIM provider, fetch provider networks, require Postgres/R2/live credentials, or depend on durable-cloud env.
- Public errors, `Debug`, logs, audit details, and docs examples must not expose raw SCIM request/response bodies, SCIM bearer tokens, token hashes, raw external ids (only bounded references/hashes), provider error bodies, access/refresh tokens, DB URLs, or sensitive provider URLs.
- SCIM must fail closed when disabled, partially configured, unauthenticated, tenant-mismatched, bound to an unknown SCIM client/principal/group, or malformed. Missing/invalid tenant identity must never fall back to local/root/session identity.

## Design

### SCIM Authentication And Client Model

SCIM is orthogonal to login. A tenant-scoped **SCIM client** holds a long-lived bearer credential the provisioning caller presents on every request. In this provider-free foundation we store only a SHA-256 hash of that credential.

Add to `src/auth/hosted.rs`:

- `ScimClientConfig` (or `ScimProviderConfig`): bounded `provider_key`, `org_id: OrgId`, `repo_id: RepoId`, `token_hash: String` (SHA-256 lower-hex), `enabled: bool`. `Debug` redacts `token_hash`.
- A SCIM client registry on `InMemoryHostedAuthStore` keyed by `(org_id, repo_id, provider_key)` and resolvable by token hash, plus a test-only seeding method (e.g. `bind_scim_client_for_test`) beside `bind_saml_external_identity_for_test`.
- `authenticate_scim_client(token_hash: &str, org_id: &OrgId, repo_id: &RepoId) -> Result<ScimClientContext, ScimAuthError>` that returns the bound, enabled tenant-scoped client or fails closed. Disabled/absent client ⇒ `ScimAuthError::Disabled`/`Unauthorized`. Mismatched tenant ⇒ `ScimAuthError::TenantMismatch`. Use constant-time hash comparison (`hosted_token_hash_eq`).
- `ScimAuthError` with redacted variants: `Disabled`, `Unauthorized`, `TenantMismatch`.

Authentication scheme: SCIM routes use a **distinct** `Authorization: Scim-Bearer <token>` scheme parsed only inside the `/scim/v2/*` handlers — never the agent/workspace `Bearer` path and never the `Stratum-Session` middleware. This guarantees zero overlap with existing schemes and keeps existing `Bearer`/`Stratum-Session`/`User` behavior untouched. The handler hashes the presented token and calls `authenticate_scim_client` with the `X-Stratum-Org` / `X-Stratum-Repo` headers (parsed and validated as a bound pair, same as other tenant routes). Production real-IdP `Authorization: Bearer` compatibility is explicit follow-on (production SCIM provider integration is out of scope). Tests must prove a `Scim-Bearer` token cannot authenticate non-SCIM routes and that agent/workspace/`Stratum-Session` credentials cannot authenticate SCIM routes.

### SCIM User Model

Mirror the SAML external-identity shape. SCIM users bind an external SCIM user to an existing tenant principal:

- `ScimUserRecord`: `id: Uuid`, `org_id`, `repo_id`, `client_id` (provider), `principal_uid: Uid`, `external_id_hash: String` (HMAC-SHA256 lower-hex keyed by the presented SCIM bearer token), optional bounded `username_hint`, `active: bool`, lifecycle timestamps, `disabled_at: Option<u64>`. `Debug` redacts `external_id_hash`.
- Provisioning (create) is keyed on `(client_id, external_id_hash)` and is idempotent: a retry returns the existing binding without creating a duplicate, duplicate audit event, or new revocation.
- Update mutates only bounded attributes and the `active` flag; it never re-binds to a different principal or crosses tenants.
- Deactivate (PATCH `active:false`) and deprovision (DELETE) set `active=false` + `disabled_at` and trigger deprovisioning revocation (below). Re-deactivating an already-inactive user is a no-op success (idempotent), emitting no duplicate revocation/audit.
- All operations validate the requested/path resource against the authenticated client's tenant and reject unknown principals (the principal must already exist for the tenant).

### SCIM Group And Membership Model

Mirror the SAML group-mapping shape plus a membership join:

- `ScimGroupRecord`: `id: Uuid`, `org_id`, `repo_id`, `client_id`, `local_gid: Gid`, `external_id_hash: String` (HMAC-SHA256 lower-hex keyed by the presented SCIM bearer token), optional bounded `display_name`, `active: bool`, lifecycle + `disabled_at`. `Debug` redacts `external_id_hash`.
- `ScimGroupMemberRecord`: `id: Uuid`, `org_id`, `repo_id`, `group_id: Uuid`, `principal_uid: Uid`, `active: bool`, lifecycle + `disabled_at`.
- Group provisioning is idempotent on `(client_id, external_id_hash)`; membership add is idempotent on `(group_id, principal_uid)` (re-adding flips an inactive row back to active or is a no-op if already active); membership remove sets `active=false` + `disabled_at` and is idempotent.
- Group membership maps an external SCIM group to a `local_gid` and tracks which existing tenant principals belong; it updates only represented hosted tenant memberships and never mutates another org/repo.

### Idempotency

This foundation uses natural idempotency:

1. **Natural idempotency** via the unique constraints above (`(client_id, external_id_hash)` for users/groups, `(group_id, principal_uid)` for members). Retried create/update/deactivate/member-change converge to the same row set with no duplicate rows, external-identity bindings, revocations, or audit events. This is the primary mechanism and is how real SCIM clients retry (same external id).
2. This slice does not add `Idempotency-Key` replay storage for SCIM routes. Production retry productization can layer that on later without changing the provider-free route contract.

### Deprovisioning Revocation

Add an additive store method to `InMemoryHostedAuthStore`:

- `revoke_principal_hosted_access(org_id: &OrgId, repo_id: &RepoId, principal_uid: Uid, revoked_at_unix: u64) -> ScimRevocationSummary` that:
  - marks every `RefreshTokenFamilyRecord` matching `(org_id, repo_id, principal_uid)` as `revoked_at_unix`,
  - marks every `RefreshTokenRecord` in those families as `revoked_at_unix`,
  - marks every `StoredAccessTokenRecord` whose identity matches `(org_id, repo_id, principal_uid)` as `revoked_at_unix`.
- Returns bounded counts only (no secrets). It is idempotent: a second call after deactivation revokes nothing new and returns zero new revocations.

SCIM user deactivate/deprovision calls this so a deprovisioned user's existing access tokens stop validating (`validate_access_token_at` already rejects `revoked_at_unix`) and refresh families fail closed (rotation already rejects revoked families). This is the only change to revocation semantics and is additive. The `Debug`/audit output reports only bounded counts.

### Routes (disabled by default)

New module `src/server/routes_scim.rs`, mounted via a `scim_routes()` group beside `hosted_routes()` in `src/server/mod.rs`:

- `POST /scim/v2/Users` — provision user (idempotent on external id).
- `PATCH /scim/v2/Users/{user_id}` — update attributes / set `active` (active:false ⇒ deactivate + revoke).
- `DELETE /scim/v2/Users/{user_id}` — deprovision (deactivate + revoke).
- `POST /scim/v2/Groups` — provision group.
- `PATCH /scim/v2/Groups/{group_id}` — group update + member add/remove operations.

Every handler:

- Parses and bounds all input fields (reuse/extend `bounded_auth_field`/`bounded_provider_key`; cap request body shape) before any store mutation.
- Authenticates the SCIM client via `Scim-Bearer` + validated org/repo headers; rejects disabled/unknown/tenant-mismatched clients with bounded errors and a redacted `AuthScimRequestDenied` audit event, never falling back to local/root.
- Validates every targeted user/group/principal belongs to the authenticated client's tenant.
- Emits bounded, redacted audit events for the provisioning decision/mutation (provider key, org/repo, principal uid, bounded external-id reference/hash, reason enum, member counts) — never raw bodies/tokens.
- Returns bounded SCIM-shaped JSON that excludes raw external ids, tokens, and hashes.

The route group does not run the `Stratum-Session` session middleware. SCIM routes are not reachable by `User`/`Bearer`/`Stratum-Session` principals, and SCIM clients are not reachable on any non-SCIM route.

### Audit

Add to `src/audit.rs`:

- `AuditAction`: `AuthScimRequestDenied`, `AuthScimUserProvision`, `AuthScimUserUpdate`, `AuthScimUserDeactivate`, `AuthScimGroupProvision`, `AuthScimGroupUpdate`, `AuthScimGroupMemberAdd`, `AuthScimGroupMemberRemove`.
- `AuditResourceKind`: `ScimClient`, `ScimUser`, `ScimGroup`, `ScimGroupMembership`.
- Keep existing snake_case round-trip coverage; extend the `action_pairs` / resource-kind round-trip tests to include the new variants.

Audit details stay bounded (existing 64-char key / 128-char value caps) and never include raw external ids beyond bounded references/hashes, SCIM tokens, request/response bodies, access/refresh tokens, token hashes, DB URLs, or provider errors.

### Runtime Gates

Add an **independent** SCIM runtime config in `src/backend/runtime.rs` that does not touch `HostedAuthProviderMode` (so OIDC/SAML mutual exclusion and behavior are unchanged):

- `HostedScimMode { Disabled, ScimDev }` parsed from `STRATUM_HOSTED_SCIM_PROVIDER` (default `disabled`).
- `scim-dev` requires `STRATUM_HOSTED_SCIM_ENABLE_DEV=1`, `STRATUM_SCIM_PROVIDER_KEY`, and `STRATUM_SCIM_CLIENT_TOKEN_HASH` (SHA-256 lower-hex). Reuse `validate_provider_key`, `validate_lower_hex_sha256`, `required_config_value`, and `incomplete_hosted_auth_config`.
- Partial/invalid SCIM config fails closed with env-name-only errors before local `.vfs` files are created.
- SCIM may be enabled alongside `oidc-dev` or `saml-dev` (provisioning is orthogonal to login); neither combination is "mixed config." SCIM disabled with no SCIM env is the default and must stay clean.
- Durable-cloud readiness checks must still run before SCIM provider validation so invalid raw SCIM values are never echoed in durable startup errors.

The actual tenant-scoped SCIM client record (token hash + org/repo binding) lives in the store/DB and is seeded in tests; the env gate only enables the mode and provides a bounded key + token hash, mirroring how SAML env enables the mode while bindings live in the store.

### Postgres Migration

Add migration `0018_scim_provisioning_foundation.sql`, register it in `src/backend/postgres_migrations.rs` as version 18 (`POSTGRES_MIGRATIONS: [PostgresMigration; 18]`), and extend adoption/known-schema verification in `src/backend/postgres.rs`.

Additive tables (all `CREATE TABLE IF NOT EXISTS`, hashes/bounded references only, disabled/inactive-safe defaults, finite timestamp + lifecycle checks, org/repo/client/principal shape FKs, non-destructive indexes):

- `scim_clients`: org/repo-scoped client metadata — `provider_key` (bounded), `token_hash` (`^[0-9a-f]{64}$`), `enabled BOOLEAN NOT NULL DEFAULT false`, lifecycle + `disabled_at`, `UNIQUE (org_id, repo_id, provider_key)`, `UNIQUE (id, org_id, repo_id)`, org FK to `organizations(id)`, composite FK `(org_id, repo_id) → repos(org_id, id)`. Mirrors `saml_providers` (token hash instead of cert hashes).
- `scim_users`: `client_id`, `principal_uid`, `external_id_hash` (`^[0-9a-f]{64}$`), optional bounded `username_hint`, `active BOOLEAN NOT NULL DEFAULT true`, lifecycle + `disabled_at`, `UNIQUE (client_id, external_id_hash)`, `UNIQUE (id, org_id, repo_id, principal_uid)`, client shape FK `(client_id, org_id, repo_id) → scim_clients(id, org_id, repo_id)` ON DELETE RESTRICT, principal shape FK `(org_id, repo_id, principal_uid) → durable_principals(org_id, repo_id, uid)` ON DELETE RESTRICT. Mirrors `saml_external_identities`.
- `scim_groups`: `client_id`, `local_gid INTEGER CHECK (>= 0)`, `external_id_hash`, optional bounded `display_name`, `active`, lifecycle + `disabled_at`, `UNIQUE (client_id, external_id_hash)`, `UNIQUE (id, org_id, repo_id)`, client shape FK ON DELETE CASCADE. Mirrors `saml_group_mappings`.
- `scim_group_members`: `group_id`, `principal_uid`, `active`, lifecycle + `disabled_at`, `UNIQUE (group_id, principal_uid)`, group shape FK `(group_id, org_id, repo_id) → scim_groups(id, org_id, repo_id)` ON DELETE CASCADE, principal shape FK `(org_id, repo_id, principal_uid) → durable_principals(org_id, repo_id, uid)` ON DELETE RESTRICT.

Do not alter or weaken OIDC/refresh/SAML constraints. Extend adoption verification so missing/weakened SCIM tables, columns, constraints, FKs, hash checks, exact defaults, indexes, or lifecycle checks fail adoption. Startup/migration-catalog tests use migration count 18.

## Task 1: Plan And Inventory Commit

**Files:**
- Create: `docs/plans/2026-06-02-scim-provisioning.md`

**Step 1: Write the plan** (this document) recording current hosted auth/session/tenant/audit state, the SCIM client/auth model, user/group/membership lifecycle, idempotency approach, deprovisioning/revocation approach, migration approach, runtime gates, rollback boundary, and out-of-scope auth productization.

**Step 2: Verify formatting**

```bash
git diff --check
```

Expected: PASS.

**Step 3: Commit**

```bash
git add docs/plans/2026-06-02-scim-provisioning.md
git commit -m "docs: plan scim provisioning foundation"
```

## Task 2: SCIM Provisioning Domain And Deprovisioning Revocation

**Files:**
- Modify: `src/auth/hosted.rs`

**Step 1: Write failing tests** under `auth::hosted`:

- `scim_client_config_debug_redacts_token_hash`
- `scim_user_and_group_records_redact_external_id_hash`
- `scim_authenticate_rejects_disabled_unknown_and_tenant_mismatch`
- `scim_user_provision_is_idempotent_on_external_id`
- `scim_group_membership_add_remove_is_idempotent`
- `scim_revoke_principal_hosted_access_invalidates_access_and_refresh`
- `scim_revoke_principal_hosted_access_is_idempotent`
- Existing hosted auth OIDC/SAML/refresh/access tests still pass.

```bash
cargo test --locked auth::hosted --lib -- --nocapture
```

Expected: fails because SCIM types and `revoke_principal_hosted_access` do not exist.

**Step 2: Implement minimal provider-free domain**

- Add `ScimClientConfig`, `ScimClientContext`, `ScimAuthError`, `ScimUserRecord`, `ScimGroupRecord`, `ScimGroupMemberRecord`, `ScimRevocationSummary` with redacted `Debug` on secret-bearing fields.
- Add SCIM client registry + user/group/member tracking to `InMemoryHostedAuthStore` (in-memory, mirroring SAML binding storage), with `authenticate_scim_client`, idempotent provisioning helpers, and `revoke_principal_hosted_access`.
- Add test-only seeding (`bind_scim_client_for_test`, principal/group seeding) beside `bind_saml_external_identity_for_test`.
- Keep all secret-bearing debug output redacted; use `hosted_token_hash_eq` for token comparison.

**Step 3: Run green tests**

```bash
cargo test --locked auth::hosted --lib -- --nocapture
```

Expected: SCIM domain tests and existing hosted auth tests pass.

**Step 4: Commit**

```bash
git add src/auth/hosted.rs
git commit -m "feat: add scim provisioning hosted auth domain"
```

## Task 3: SCIM Routes And Redacted Audit

**Files:**
- Create: `src/server/routes_scim.rs`
- Modify: `src/server/mod.rs` (declare module, mount `scim_routes()`)
- Modify: `src/audit.rs`
- Modify if needed: `src/server/routes_auth.rs` (promote shared audit/bounding helpers to `pub(crate)`)

**Step 1: Write failing route/audit tests** under `server::routes_scim` and `audit::tests`:

- `scim_request_denied_when_disabled_without_secret_leak`
- `scim_request_rejects_missing_or_invalid_scim_bearer`
- `scim_request_rejects_tenant_mismatch_without_local_fallback`
- `scim_user_create_update_deactivate_is_tenant_scoped`
- `scim_user_deactivate_revokes_hosted_access_and_refresh`
- `scim_group_membership_add_update_remove_is_tenant_scoped`
- `scim_create_retry_is_idempotent_without_duplicate_audit`
- `scim_public_errors_and_audit_redact_bodies_tokens_and_external_ids`
- `scim_bearer_cannot_authenticate_non_scim_routes`
- `non_scim_credentials_cannot_authenticate_scim_routes`
- `auth_scim_audit_enums_round_trip_as_snake_case` (extend existing enum round-trip tests).

```bash
cargo test --locked server::routes_scim --lib -- --nocapture
cargo test --locked audit::tests --lib -- --nocapture
```

Expected: fails because the routes and audit variants do not exist.

**Step 2: Implement routes and audit variants**

- Add `routes_scim.rs` with the five handlers, bounded request structs, `Scim-Bearer` extraction, tenant validation via `repo_belongs_to_org` + validated org/repo headers, idempotent mutation, deprovisioning revocation on deactivate/delete, and bounded SCIM-shaped responses.
- Add the SCIM `AuditAction` and `AuditResourceKind` variants and bounded redacted audit emission (reuse `append_auth_audit`/`hosted_audit_actor` or a local equivalent).
- Mount `scim_routes()` beside `hosted_routes()` in `mod.rs` without running session middleware.

**Step 3: Run green tests**

```bash
cargo test --locked server::routes_scim --lib -- --nocapture
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked server::routes_auth --lib -- --nocapture
```

Expected: SCIM route/audit tests pass and existing OIDC/SAML/refresh tests remain unchanged.

**Step 4: Commit**

```bash
git add src/server/routes_scim.rs src/server/mod.rs src/audit.rs src/server/routes_auth.rs
git commit -m "feat: add provider-free scim provisioning routes"
```

## Task 4: Hosted SCIM Runtime Gates

**Files:**
- Modify: `src/backend/runtime.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing runtime/startup tests:**

- `hosted_scim_disabled_by_default`
- `hosted_scim_partial_env_fails_closed_without_raw_values`
- `hosted_scim_dev_enabled_alongside_oidc_or_saml`
- `hosted_scim_complete_dev_config_debug_is_redacted`
- `durable_core_runtime_checks_readiness_before_scim_provider_validation`
- `server_startup_with_partial_scim_config_creates_no_local_vfs_files`

```bash
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected: fails until SCIM runtime config exists.

**Step 2: Implement fail-closed config**

- Add SCIM env constants and `HostedScimMode`.
- Keep disabled default; require dev enablement + provider key + token hash for `scim-dev`; return env-name-only errors for partial/invalid config.
- Ensure SCIM enables independently of (and composes with) the login-provider mode; extend startup env scrubbing and redaction assertions.

**Step 3: Run green tests**

```bash
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
```

Expected: runtime and startup tests pass; provider-backed portions skip cleanly when env is unset.

**Step 4: Commit**

```bash
git add src/backend/runtime.rs tests/server_startup.rs
git commit -m "feat: gate scim provisioning config"
```

## Task 5: Postgres Migration 0018

**Files:**
- Create: `migrations/postgres/0018_scim_provisioning_foundation.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Modify if needed: `src/backend/postgres.rs`
- Modify if needed: `tests/server_startup.rs`

**Step 1: Write failing migration tests:**

- `scim_provisioning_foundation_migration_is_registered_and_non_destructive`
- `known_schema_verifier_requires_scim_tables`
- `weakened_scim_hash_lifecycle_or_membership_constraints_fail_adoption`
- startup/migration catalog tests use migration count 18.

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
```

Expected: fails until migration 0018 is registered and verified.

**Step 2: Add additive SQL and catalog checks**

- Add `scim_clients`, `scim_users`, `scim_groups`, `scim_group_members` with the shapes above.
- Use `CREATE TABLE IF NOT EXISTS`, guarded constraints, finite-timestamp + lifecycle checks, lower-hex hash/HMAC shape checks, org/repo/client/principal shape FKs, and non-destructive indexes.
- Bump the migration array to length 18 and register version 18 `scim_provisioning_foundation`.
- Extend known-schema/adoption verification for all new tables, columns, constraints, FKs, defaults, and indexes (mirror the SAML `pg_get_expr` / `pg_attrdef` checks).
- Do not modify existing OIDC/refresh/SAML table semantics except where a shared verifier helper must know the new migration exists.

**Step 3: Run green migration tests**

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: Postgres feature tests pass; live portions skip cleanly when env is unset.

**Step 4: Commit**

```bash
git add migrations/postgres/0018_scim_provisioning_foundation.sql src/backend/postgres_migrations.rs src/backend/postgres.rs tests/server_startup.rs
git commit -m "feat: add scim provisioning postgres schema"
```

## Task 6: Regression Gates For Existing Auth

**Files:**
- Modify tests only if a regression requires targeted updates.

**Step 1: Run focused auth/session gates**

```bash
cargo test --locked auth::hosted --lib -- --nocapture
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
```

Expected: all pass.

**Step 2: Fix only SCIM-caused regressions** using failing tests first. Do not broaden or rewrite unrelated auth/route surfaces.

**Step 3: Commit only if fixes were required**

```bash
git add <changed-files>
git commit -m "fix: preserve existing auth behavior"
```

## Task 7: Documentation And Status

Status: completed in this docs/status update.

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: `docs/plans/2026-06-02-scim-provisioning.md`

**Step 1: Update docs** to document:

- The disabled-by-default `/scim/v2/*` provisioning routes and the `Scim-Bearer` auth scheme.
- SCIM client/user/group/membership model, tenant scoping, and idempotent retries.
- Deprovisioning revocation of hosted access/refresh state.
- SCIM runtime config (independent of login provider; composes with `oidc-dev`/`saml-dev`).
- Migration 0018.
- Unchanged OIDC, SAML, refresh, local `User`, agent `Bearer`, workspace bearer, and `Stratum-Session` behavior.
- Redaction rules (no bodies, SCIM tokens, token hashes, raw external ids, access/refresh tokens, DB/provider URLs).
- Hosted admin UI, broad provisioning UI, principal auto-provisioning, and production SCIM provider integration out of scope.

**Step 2: Check formatting**

```bash
git diff --check
```

Expected: PASS.

**Step 3: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md docs/plans/2026-06-02-scim-provisioning.md
git commit -m "docs: record scim provisioning foundation"
```

## Task 8: Required Reviews And Fixes

**Files:**
- Modify only files needed for verified review findings.

**Step 1: Spec/correctness review** — compare implementation against this plan and the Slice 16c acceptance criteria, focusing on:

- SCIM disabled by default and fails closed.
- SCIM cannot mutate another org/repo or fall back to local/root/session identity.
- Create/update/deactivate user and group-membership flows are tenant-scoped.
- Deactivation/deprovisioning revokes represented hosted access/refresh state.
- Idempotent retries do not duplicate users/groups/memberships/revocations/audit/bindings.
- Existing OIDC, SAML, refresh, local user, agent bearer, workspace bearer, and `Stratum-Session` behavior unchanged.

**Step 2: Code-quality/security review** — focus on:

- Rust API shape and local style.
- Redaction of SCIM bodies, bearer tokens, token hashes, raw external ids, access/refresh tokens, provider errors, and DB/provider URLs.
- Additive migration safety and adoption verification.
- Provider-free tests with no network/live credentials.
- No admin UI / productization / principal-allocation creep.

**Step 3: Fix findings with TDD** — for every behavioral finding, write or update a failing test first, run it red, implement the smallest fix, rerun green.

**Step 4: Commit review fixes** (only if needed)

```bash
git add <changed-files>
git commit -m "fix: harden scim provisioning review findings"
```

## Task 9: Final Verification

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked auth::hosted --lib -- --nocapture
cargo test --locked auth::session --lib -- --nocapture
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked server::routes_scim --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
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

Expected: all provider-free gates pass. Live Postgres/R2 portions may skip only when command output proves the relevant env is unset.

## Final Acceptance Checklist

- Plan documents current hosted auth/session/tenant/audit state, SCIM client model, user/group lifecycle, idempotency approach, deprovisioning/revocation approach, migration approach, rollback boundary, and out-of-scope auth productization.
- SCIM endpoints are disabled by default and fail closed.
- OIDC, SAML, refresh-token, local auth, agent bearer, workspace bearer, and hosted `Stratum-Session` flows remain unchanged and covered by tests.
- SCIM create/update/deactivate user flows are tenant-scoped and cannot mutate another org/repo or fall back to local/root identity.
- SCIM group membership flows are tenant-scoped and update only represented hosted tenant memberships.
- Deactivation/deprovisioning revokes represented hosted access/refresh/session state.
- Idempotent retries do not duplicate users, groups, memberships, revocations, audit events, or external identity bindings.
- Public errors and audit records are bounded and redacted; raw SCIM bodies, bearer tokens, token hashes, sensitive external ids, access/refresh tokens, DB URLs, and provider errors are not logged/debugged/audited/publicly returned.
- Postgres migration/model is backwards-compatible and non-destructive.
- Normal tests require no Postgres, R2, IdP, SCIM provider, network, durable-cloud env, or live credentials.
- Existing durable-cloud startup gates remain fail-closed.
- Hosted admin UI and broad tenant provisioning UI remain out of scope.
- Docs/status updated.
