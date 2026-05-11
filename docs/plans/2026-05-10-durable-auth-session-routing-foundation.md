# Durable Auth Session Routing Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move hosted durable auth/session validation off local-only `StratumDb` and local workspace metadata files by adding a narrow durable principal, workspace-token lifecycle, and mounted-session identity foundation.

**Architecture:** Keep local auth behavior intact while extending the existing workspace/auth/runtime seams so durable hosted paths can validate workspace bearers from durable stores. Broad `STRATUM_CORE_RUNTIME=durable-cloud` remains fail-closed; this slice only adds the durable auth/session foundation, lifecycle checks, and startup readiness gates needed for a future cutover.

**Tech Stack:** Rust, Axum, Tokio, existing `CoreDb` seam, `WorkspaceMetadataStore`, `StratumStores`, Postgres metadata adapter behind the `postgres` feature, existing audit/idempotency/server startup tests.

---

## Reference Material

- `markdownfs_v2_cto_architecture_plan.md`
- `docs/project-status.md`
- `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- `src/auth/session.rs`
- `src/workspace/mod.rs`
- `src/db.rs`
- `src/backend/mod.rs`
- `src/backend/postgres.rs`
- `src/backend/runtime.rs`
- `src/server/middleware.rs`
- `src/server/routes_auth.rs`
- `src/server/routes_workspace.rs`
- `src/server/core.rs`
- `src/server/mod.rs`
- `migrations/postgres/*.sql`
- `tests/server_startup.rs`

## Current Baseline

- `Session` carries Unix uid/gid/groups, optional `SessionScope`, and optional `SessionMount` with workspace id, root path, base ref, and session ref.
- Workspace token records are hash-only and scoped by read/write prefixes, but have no expiry, revocation, issued/updated timestamps, repo id, principal id, token version, or lifecycle status.
- `WorkspaceMetadataStore` has local, in-memory, and Postgres implementations. Postgres already has `workspaces.repo_id`, but the current adapter filters `repo_id IS NULL` for compatibility.
- `session_from_headers()` validates workspace bearers through `state.workspaces`, then calls `state.core.session_for_uid(token.agent_uid)`. This still requires a local user in hosted durable mode.
- `/auth/login` calls `state.db.login()` directly instead of `state.core.login()`.
- Workspace token issuance rejects `Idempotency-Key` before validating the secret-bearing request, which must remain true.
- Workspace token issuance authenticates the backing agent token with `state.db.authenticate_token()` directly, which is local-only.
- `STRATUM_CORE_RUNTIME=durable-cloud` is intentionally rejected before local `.vfs` state opens.
- Durable bearer validation must return enough identity to build a normal `Session`: `uid`, `gid`, `groups`, and `username`. Returning only a uid would keep the local `session_for_uid()` dependency and would weaken existing permission checks.

## Non-Goals

- Do not enable broad `STRATUM_CORE_RUNTIME=durable-cloud`.
- Do not add OIDC/SAML, refresh-token flows, tenant/org membership, web console, MCP/FUSE durable auth cutover, or secret-bearing idempotent replay.
- Do not persist raw workspace tokens, raw bearer tokens, request bodies containing tokens, idempotency keys, Postgres URLs with passwords, R2 credentials, or local backing paths in audit/replay/error records.
- Do not redesign the full identity system. Add only the narrow durable principal/token/session model needed for workspace bearer validation and mounted-session identity.

## Task 1: Extend Workspace Auth Domain Types

**Files:**

- Modify: `src/workspace/mod.rs`
- Modify: `src/auth/session.rs`
- Test: `src/workspace/mod.rs`
- Test: `src/auth/session.rs`

**Step 1: Add narrow principal and lifecycle fields**

Add a narrow durable principal value near the workspace auth model. This is not a full identity system; it is the minimum session identity needed after durable token validation.

```rust
pub enum WorkspacePrincipalKind {
    Human,
    ServiceAccount,
    Agent,
}

pub struct WorkspacePrincipalRecord {
    pub uid: Uid,
    pub username: String,
    pub gid: Gid,
    pub groups: Vec<Gid>,
    pub kind: WorkspacePrincipalKind,
    pub active: bool,
}
```

Extend `WorkspaceTokenRecord` with backward-compatible serde defaults:

```rust
pub struct WorkspaceTokenRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub agent_uid: Uid,
    pub secret_hash: String,
    pub read_prefixes: Vec<String>,
    pub write_prefixes: Vec<String>,
    #[serde(default)]
    pub principal_uid: Option<Uid>,
    #[serde(default)]
    pub token_version: u64,
    #[serde(default)]
    pub issued_at_unix: u64,
    #[serde(default)]
    pub updated_at_unix: u64,
    #[serde(default)]
    pub expires_at_unix: Option<u64>,
    #[serde(default)]
    pub revoked_at_unix: Option<u64>,
}
```

Use a small constructor/helper so new tokens always have `token_version = 1`, nonzero `issued_at_unix`, matching `updated_at_unix`, and `principal_uid = Some(agent_uid)` unless a durable principal id is explicitly supplied. Legacy local tokens should decode with `principal_uid = Some(agent_uid)` during state normalization, not stay as a permanent `None`.

**Step 2: Add repo/session identity fields**

Extend `WorkspaceRecord` and `ValidWorkspaceToken` narrowly:

```rust
pub struct WorkspaceRecord {
    pub id: Uuid,
    pub name: String,
    pub root_path: String,
    pub head_commit: Option<String>,
    pub version: u64,
    pub base_ref: String,
    pub session_ref: Option<String>,
    #[serde(default)]
    pub repo_id: Option<String>,
}

pub struct ValidWorkspaceToken {
    pub workspace: WorkspaceRecord,
    pub token: WorkspaceTokenRecord,
    pub repo_id: Option<String>,
    pub principal: Option<WorkspacePrincipalRecord>,
}
```

Keep local compatibility by defaulting `repo_id` to `None`.

**Step 3: Add validation clock and revoke APIs to the trait**

Add default trait methods that preserve local behavior until implementations override them:

```rust
async fn validate_workspace_token_at(
    &self,
    workspace_id: Uuid,
    raw_secret: &str,
    now_unix: u64,
) -> Result<Option<ValidWorkspaceToken>, VfsError>;

async fn revoke_workspace_token(
    &self,
    workspace_id: Uuid,
    token_id: Uuid,
    now_unix: u64,
) -> Result<Option<WorkspaceTokenRecord>, VfsError>;
```

Make `validate_workspace_token()` delegate to `validate_workspace_token_at()` with current Unix time. Reject tokens when `revoked_at_unix.is_some()` or `expires_at_unix <= now_unix`.

**Step 4: Extend mounted session identity**

Add hash-safe identity to `SessionMount`:

```rust
repo_id: Option<String>,
principal_uid: Option<Uid>,
token_id: Option<Uuid>,
token_version: Option<u64>,
read_prefixes: Vec<String>,
write_prefixes: Vec<String>,
```

Add a new constructor such as `SessionMount::with_workspace_identity(...)` and have existing constructors delegate with empty optional identity so local tests keep passing.

Add a `Session::from_workspace_principal(principal: WorkspacePrincipalRecord)` helper or equivalent local helper in middleware. It must populate `uid`, `gid`, `groups`, and `username` from durable validation output. Do not construct a durable workspace session from only `principal_uid`.

**Step 5: Unit tests**

Run and extend:

```bash
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked auth::session --lib -- --nocapture
```

Expected:

- Legacy local serialized workspace metadata decodes with lifecycle defaults.
- Issued local tokens have hash-only secrets and nonzero lifecycle timestamps.
- `validate_workspace_token_at()` rejects expired and revoked tokens.
- `revoke_workspace_token()` increments token version, sets `revoked_at_unix`, and causes validation failure.
- Mounted sessions expose repo/workspace/base/session/principal/token identity while `project_mounted_error_path()` still returns `<outside workspace>` for backing-path escapes.
- `Session::from_workspace_principal()` creates a normal permission-checkable session with durable `uid`, `gid`, `groups`, and `username`.

**Step 6: Commit**

```bash
git add src/workspace/mod.rs src/auth/session.rs
git commit -m "feat: add workspace token lifecycle model"
```

## Task 2: Add Durable Principal and Token Lifecycle Schema

**Files:**

- Create: `migrations/postgres/0009_durable_auth_session_foundation.sql`
- Modify: `src/backend/postgres.rs`
- Test: `src/backend/postgres.rs`
- Test: `tests/postgres/0001_durable_backend_foundation_smoke.sql` if schema smoke expectations require table additions.

**Step 1: Write migration**

Create narrow durable auth tables and lifecycle columns:

```sql
CREATE TABLE durable_principals (
    uid INTEGER PRIMARY KEY CHECK (uid >= 0),
    repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    username TEXT NOT NULL CHECK (btrim(username) <> '' AND length(username) <= 128),
    primary_gid INTEGER NOT NULL CHECK (primary_gid >= 0),
    groups_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    kind TEXT NOT NULL CHECK (kind IN ('human', 'service_account', 'agent')),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repo_id, username)
);

ALTER TABLE workspace_tokens
    ADD COLUMN repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    ADD COLUMN principal_uid INTEGER,
    ADD COLUMN token_version BIGINT NOT NULL DEFAULT 1 CHECK (token_version > 0),
    ADD COLUMN issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN expires_at TIMESTAMPTZ,
    ADD COLUMN revoked_at TIMESTAMPTZ,
    ADD CONSTRAINT workspace_tokens_principal_fk
        FOREIGN KEY (principal_uid) REFERENCES durable_principals(uid),
    ADD CONSTRAINT workspace_tokens_lifecycle_check
        CHECK (revoked_at IS NULL OR revoked_at >= issued_at),
    ADD CONSTRAINT workspace_tokens_expiry_check
        CHECK (expires_at IS NULL OR expires_at > issued_at);

UPDATE workspace_tokens
SET principal_uid = agent_uid,
    repo_id = (
        SELECT repo_id
        FROM workspaces
        WHERE workspaces.id = workspace_tokens.workspace_id
    )
WHERE principal_uid IS NULL;
```

The migration must not add a foreign key that breaks existing local/global workspace-token rows. If existing tokens have no durable principal row, leave `principal_uid` nullable and let local compatibility continue through `agent_uid`; durable hosted validation requires an active principal row before it can build a session.

Add indexes for validation:

```sql
CREATE INDEX workspace_tokens_workspace_active_idx
    ON workspace_tokens(workspace_id, revoked_at, expires_at);

CREATE INDEX workspace_tokens_repo_principal_idx
    ON workspace_tokens(repo_id, principal_uid);
```

**Step 2: Add Postgres row mapping**

Update `row_to_workspace_record()` and `row_to_workspace_token_record()` to read `repo_id`, lifecycle fields, and token version. Convert timestamps to Unix seconds with bounded helpers.

**Step 3: Add test helpers**

Extend existing Postgres workspace contract tests with SQL assertions:

- `workspace_tokens.secret_hash` is a 64-char hex digest.
- no raw issued secret appears in `workspace_tokens`.
- lifecycle timestamps are present.
- `token_version` starts at `1`.
- `principal_uid` matches the issuing principal.
- validation joins active `durable_principals` when the workspace/token row is repo-scoped.

**Step 4: Run focused migration/schema tests**

```bash
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: migration order applies cleanly; existing Postgres adapter tests pass with new fields.

**Step 5: Commit**

```bash
git add migrations/postgres/0009_durable_auth_session_foundation.sql src/backend/postgres.rs tests/postgres/0001_durable_backend_foundation_smoke.sql
git commit -m "feat: add durable auth session schema"
```

## Task 3: Implement Durable Workspace Token Lifecycle in Stores

**Files:**

- Modify: `src/workspace/mod.rs`
- Modify: `src/backend/postgres.rs`
- Test: `src/workspace/mod.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Update in-memory and local stores**

Implement lifecycle-aware issue, validate, and revoke for `InMemoryWorkspaceMetadataStore` and `LocalWorkspaceMetadataStore`.

Requirements:

- Newly issued tokens are non-idempotent and generate a fresh raw secret every time.
- Raw secrets are never stored.
- Validation compares hashes using the existing constant-time helper.
- Wrong workspace returns `Ok(None)`.
- Expired or revoked matching token returns `Ok(None)` without leaking whether the hash matched.
- Revoke returns `Ok(None)` for unknown token/workspace and never exposes raw secret.
- Local validation may return `principal: None`; middleware will still use `state.core.session_for_uid()` in that compatibility path.

**Step 2: Update Postgres store validation**

Change validation to query the workspace and token in one repo-aware shape:

- Load workspace by id.
- Compute expected hash in memory.
- Query token rows for the workspace.
- Normalize prefixes against the workspace root.
- Enforce wrong workspace, expired, and revoked failures.
- Return `ValidWorkspaceToken` with repo id and lifecycle identity.
- For repo-scoped workspaces, require an active durable principal row and return it in `ValidWorkspaceToken.principal`.
- For `repo_id IS NULL` compatibility workspaces, keep current local semantics and allow `principal: None`.

Keep local compatibility by continuing to support `repo_id IS NULL` workspaces for local/durable-control-plane guarded mode. Do not default hosted durable-cloud request routing to `RepoId::local()` in this task.

**Step 3: Update Postgres issuance**

When issuing a token:

- Lock the workspace row.
- Set `repo_id` from the workspace.
- Set `principal_uid = agent_uid` for this narrow foundation.
- Set `token_version = 1`.
- Set `issued_at = updated_at = now()`.
- Leave expiry optional for route/API compatibility unless the caller supplies it in a later task.
- Retry only on secret-hash collision.

**Step 4: Add tests**

Add or extend tests for:

- local revoked tokens fail validation after persistence rebuild.
- local expired tokens fail validation.
- Postgres wrong-workspace token fails.
- Postgres revoked token fails.
- Postgres expired token fails.
- Postgres corrupted prefixes remain `CorruptStore` without exposing raw token.

**Step 5: Run focused tests**

```bash
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

**Step 6: Commit**

```bash
git add src/workspace/mod.rs src/backend/postgres.rs
git commit -m "feat: enforce workspace token lifecycle"
```

## Task 4: Add Durable Auth Session Resolver Seam

**Files:**

- Modify: `src/server/core.rs`
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_auth.rs`
- Modify: `src/server/routes_workspace.rs`
- Test: `src/server/middleware.rs`
- Test: `src/server/routes_auth.rs`
- Test: `src/server/routes_workspace.rs`

**Step 1: Route login through `CoreDb`**

Change `/auth/login` from:

```rust
state.db.login(&req.username).await
```

to:

```rust
state.core.login(&req.username).await
```

Expected local behavior is unchanged because `LocalCoreRuntime::login()` delegates to `StratumDb`.

**Step 2: Add session construction for durable principals**

Do not add a broad auth method to `CoreDb` for this slice. Build workspace sessions in middleware from validation output:

- if `ValidWorkspaceToken.principal` is `Some(active principal)`, build a `Session` directly from `uid`, `gid`, `groups`, and `username`;
- if `principal` is `None`, preserve local compatibility by calling `state.core.session_for_uid(valid.token.agent_uid)`;
- if a repo-scoped durable token has no active principal, reject it as an invalid workspace bearer.

This keeps global durable login/bearer auth fail-closed while removing the local `StratumDb` dependency for durable workspace bearer validation.

**Step 3: Build workspace sessions from validated token identity**

Update `session_from_headers()` workspace bearer branch so it:

- validates via `state.workspaces.validate_workspace_token_at(workspace_id, token, now)`;
- rejects invalid, expired, revoked, wrong-workspace tokens with the same public `"invalid workspace bearer token"` message;
- creates `SessionScope` from the validated read/write prefixes;
- builds the session from durable principal details when present, without calling `state.core.session_for_uid()`;
- carries workspace id, repo id, base ref, session ref, principal uid, token id, token version, read prefixes, and write prefixes into `SessionMount`;
- does not include backing root path in user-facing auth errors;
- does not fall back to global bearer auth when workspace header is malformed, unknown, expired, revoked, or wrong.

**Step 4: Stop local-only agent token dependency in hosted durable issuance**

Refactor workspace token issuance so backing-agent authentication goes through a seam:

```rust
state.core.authenticate_token(&req.agent_token).await
```

Then keep `DurableCoreRuntime::authenticate_token()` fail-closed until durable non-workspace bearer auth is intentionally added. This means hosted durable token issuance remains disabled unless a later principal provisioning path exists, which is acceptable for this foundation.

**Step 5: Preserve idempotency rejection order**

Keep this invariant:

- `Idempotency-Key` on `POST /workspaces/{id}/tokens` returns `400` before validating the backing agent token, reading the workspace, or writing audit.

**Step 6: Add tests**

Add route/middleware tests for:

- `/auth/login` uses `state.core` by installing a test `CoreDb` that records login calls and does not require `state.db.login()`.
- workspace bearer succeeds when `state.workspaces` is durable-like and `state.db` lacks the agent user, as long as the validated token includes durable principal/session identity.
- revoked and expired workspace bearer tokens return unauthorized and do not fall back to global bearer.
- wrong workspace and wrong prefix still fail.
- mounted session identity includes repo id, workspace id, base ref, session ref, principal uid, token id, token version, read scopes, and write scopes.

**Step 7: Run focused tests**

```bash
cargo test --locked auth --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::middleware::tests --lib -- --nocapture
```

**Step 8: Commit**

```bash
git add src/server/core.rs src/server/middleware.rs src/server/routes_auth.rs src/server/routes_workspace.rs
git commit -m "feat: route auth through session seams"
```

## Task 5: Add Minimal Revoke Route and Safe Audit Details

**Files:**

- Modify: `src/server/routes_workspace.rs`
- Modify: `src/audit.rs` if action enums need a new token revoke action.
- Test: `src/server/routes_workspace.rs`

**Step 1: Add route**

Add an admin-only route:

```text
POST /workspaces/{workspace_id}/tokens/{token_id}/revoke
```

It should:

- require admin session;
- reject scoped workspace sessions;
- reject `Idempotency-Key` for this route unless the existing idempotency layer can replay a secret-free revoke response safely;
- call `state.workspaces.revoke_workspace_token(workspace_id, token_id, now)`;
- return `404` for unknown workspace/token;
- return the token metadata without raw secret or hash.

**Step 2: Audit revoke**

Append a content-free audit event with:

- token id;
- workspace id;
- principal uid if known;
- token version;
- no raw secret;
- no secret hash;
- no request body.

**Step 3: Add tests**

Add tests for:

- admin can revoke token and subsequent bearer validation fails.
- non-admin cannot revoke.
- scoped workspace bearer cannot revoke.
- revoke response and audit do not contain raw token or secret hash.

**Step 4: Run focused tests**

```bash
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/server/routes_workspace.rs src/audit.rs
git commit -m "feat: add workspace token revocation route"
```

## Task 6: Add Fail-Closed Durable Auth Startup Readiness

**Files:**

- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `tests/server_startup.rs`
- Test: `tests/server_startup.rs`

**Step 1: Add readiness naming without enabling durable-cloud**

Keep `STRATUM_CORE_RUNTIME=durable-cloud` rejected, but improve the fail-closed gate to require durable auth/session readiness before any future durable-cloud open path can proceed.

Implementation direction:

- Add a `DurableAuthSessionReadiness` marker or method in `BackendRuntimeConfig`/server startup preflight.
- In durable-cloud mode, return a redacted `NotSupported` that explicitly names missing durable auth/session routing and still exits before durable/local stores open.
- Do not parse durable backend config for durable-cloud in a way that can log secrets.
- Do not create `.vfs/state.bin`, `.vfs/workspaces.bin`, `.vfs/idempotency.bin`, `.vfs/audit.bin`, or `.vfs/review.bin`.

**Step 2: Extend startup tests**

Add/extend tests for:

- `STRATUM_CORE_RUNTIME=durable-cloud` fails before local core state file exists.
- durable-cloud plus durable backend env fails before local control-plane files exist.
- durable-cloud failure output contains no Postgres/R2 secrets and no raw URLs with passwords.
- missing auth/session readiness is mentioned in a stable redacted message.

**Step 3: Run focused startup tests**

```bash
cargo test --locked --test server_startup durable_env -- --nocapture
```

**Step 4: Commit**

```bash
git add src/backend/runtime.rs src/server/mod.rs tests/server_startup.rs
git commit -m "test: guard durable auth startup readiness"
```

## Task 7: Review, Fix, and Final Verification

**Files:**

- Modify as needed from review findings.
- Modify: `docs/project-status.md` only if final implementation materially changes current status.

**Step 1: Spec review**

Run a spec/correctness review focused on:

- no local metadata fallback in durable hosted auth paths;
- no broad durable runtime enablement;
- token revocation/expiry enforced before session creation;
- durable workspace bearer can validate without local workspace metadata files;
- workspace token issuance remains non-idempotent and rejects idempotency keys before secret validation.

**Step 2: Code quality/security review**

Run a code-quality/security review focused on:

- raw token secrets never stored or logged;
- token hashes not returned in HTTP responses;
- no raw Postgres/R2 credentials in errors;
- no backing path leaks in mounted workspace errors;
- migration compatibility and local metadata decode compatibility.

**Step 3: Fix findings**

Fix findings locally after inspecting the diffs. Do not blindly accept reviewer output.

**Step 4: Run required gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked auth --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked --test server_startup durable_env -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

If a gate cannot run because external services are unavailable, record the exact command and reason.

**Step 5: Commit final fixes**

```bash
git add <changed-files>
git commit -m "fix: address durable auth session review findings"
```

Only create this commit if there are review-fix changes.

## Final Acceptance Checklist

- Durable hosted mode can validate a durable workspace bearer without local workspace metadata files.
- Revoked, expired, wrong-workspace, wrong-repo, and wrong-prefix tokens fail.
- Raw token secrets never appear in storage, logs, audit, idempotency replay, or errors.
- Workspace token issuance remains safe around idempotency.
- Local auth/session behavior remains unchanged.
- Broad `STRATUM_CORE_RUNTIME=durable-cloud` remains fail-closed.
- Existing guarded durable routes continue working.
