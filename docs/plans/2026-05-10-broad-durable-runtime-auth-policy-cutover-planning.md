# Broad Durable Runtime Auth Policy Cutover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Define the safe path from guarded durable capability routes to broad `STRATUM_CORE_RUNTIME=durable-cloud` without enabling broad durable runtime in this slice.

**Architecture:** Keep the current guarded durable path as the only live durable filesystem/VCS serving surface while implementation teams move runtime selection, durable auth/session routing, policy enforcement, repo routing, storage operations, and recovery gates into shared lower seams. Broad durable startup remains fail-closed until all launch blockers in this plan are cleared by tests and operator gates.

**Tech Stack:** Rust, Axum, Tokio, existing `CoreDb` seam, Stratum durable backend store traits, Postgres metadata adapter behind the `postgres` feature, R2/S3-compatible object storage adapter, existing audit/idempotency/workspace/review stores.

---

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- `src/backend/runtime.rs`
- `src/backend/mod.rs`
- `src/backend/postgres.rs`
- `src/backend/core_transaction.rs`
- `src/backend/durable_mutation.rs`
- `src/backend/committed_read.rs`
- `src/server/mod.rs`
- `src/server/core.rs`
- `src/server/middleware.rs`
- `src/server/policy.rs`
- `src/server/routes_auth.rs`
- `src/server/routes_workspace.rs`
- `src/server/routes_fs.rs`
- `src/server/routes_vcs.rs`
- `src/server/routes_review.rs`
- `src/bin/stratum_mcp.rs`
- `src/bin/stratumctl.rs`
- `src/fuse_mount.rs`
- `src/auth/session.rs`
- `src/workspace/mod.rs`
- `src/review.rs`
- `migrations/postgres/*.sql`
- `tests/server_startup.rs`

## Current Baseline

The current durable backend has a broad guarded capability path, but not broad durable runtime startup.

Built and guarded:

- Durable backend store contracts exist for objects, commits, refs, workspace metadata, review, idempotency, audit, post-CAS recovery, pre-visibility recovery, durable FS mutation recovery, and object cleanup claims.
- `STRATUM_BACKEND=durable` can open Postgres-backed control-plane stores when built with `postgres`, after migration/readiness preflight.
- `STRATUM_DURABLE_COMMIT_ROUTE=1` wires a guarded durable capability into `LocalCoreRuntime`.
- Guarded durable HTTP routes can serve committed reads, durable VCS metadata, mounted-session FS mutations, commit promotion, status, diff, revert, recovery repair, and operator recovery observability.
- Durable committed reads and mounted-session writes use durable `RefStore`, `CommitStore`, and `ObjectStore` records under `RepoId::local()`.
- Recovery ledgers cover pre-visibility, post-CAS, durable FS mutation side effects, scheduler health, and object cleanup claim visibility.

Still fail-closed or unresolved:

- `STRATUM_CORE_RUNTIME=durable-cloud` is recognized but rejected before durable backend validation, migration preflight, local state open, or serving.
- HTTP route construction still opens local `StratumDb`; guarded durable capability is attached to `LocalCoreRuntime`, not selected as the broad `CoreDb`.
- Durable auth/session routing is not cut over. `/auth/login`, workspace token issuance, MCP, and FUSE still depend on local state or local metadata paths in important places.
- Protected-change enforcement is primarily route-level. `src/server/policy.rs` is a route-facing seam, not a lower runtime policy engine.
- MCP and FUSE do not use the HTTP route policy/idempotency/audit stack. `stratumctl` is HTTP-based and benefits from route gates, but still relies on whichever server runtime is live.
- Postgres adapter connections are per operation through `tokio-postgres` `NoTls`; remote/TLS-required URLs intentionally fail closed.
- Final-object deletion, unreachable commit/object GC, idempotency retention/quota, durable token lifecycle, production secrets posture, distributed locks, tenant/repo routing, and the full audit pipeline are not implemented.

## Non-Goals

- Do not enable broad `STRATUM_CORE_RUNTIME=durable-cloud`.
- Do not implement durable auth/session, distributed locks, final-object deletion/GC, web console, FUSE sparse mount, semantic search, or execution runner in this planning slice.
- Do not weaken guarded durable fail-closed behavior.
- Do not add compatibility shims that let lower surfaces bypass policy while broad runtime is being prepared.

## Runtime Cutover Boundary

### Current guarded durable capability

The live durable path is a capability attached to local serving:

- `server::open_core_db_for_runtime()` opens `StratumDb` only for `CoreRuntimeMode::LocalState`; durable-cloud returns `unsupported_durable_core_runtime()`.
- `server::open_server_stores_for_runtime()` can open durable control-plane stores for `STRATUM_BACKEND=durable`.
- `open_guarded_durable_commit_stores()` composes Postgres metadata stores with the R2-backed object adapter when `STRATUM_DURABLE_COMMIT_ROUTE=1`.
- `build_router_with_stores_and_guarded_durable_commit()` always builds a `LocalCoreRuntime`; the guarded durable stores become a `GuardedDurableCommitRoute` inside that local runtime.
- `LocalCoreRuntime` routes read/search/VCS methods through the guarded durable capability when present. Mutable FS routes use durable mutation only when the session is a mounted workspace session with a session ref.
- The broad `DurableCoreRuntime` has internal implementations for several methods, but `route_execution_enabled()` is still false and startup never selects it as the broad server runtime.

### Move into broad durable `CoreDb`

Before `STRATUM_CORE_RUNTIME=durable-cloud` can serve, these must move out of the guarded capability path and into a first-class durable `CoreDb` open path:

- Durable committed reads: `cat`, `ls`, `stat`, `tree`, `find`, `grep`, `vcs log`, `vcs refs`.
- Durable mounted-session FS mutations: write, mkdir, delete, copy, move, metadata update, and their replay preflights.
- Durable VCS mutations: commit, revert, ref create/update, status, and diff.
- Recovery conflict checks before mutating the same target ref/session.
- Workspace-head, audit, idempotency, and recovery side-effect handling.
- Durable session/auth lookup and scoped workspace mount identity.
- Shared policy evaluation below route handlers.

### Unsupported routes that stay fail-closed

The following must stay explicitly fail-closed during the first broad runtime enablement unless their slice proves parity:

- Auth/session routes that would mint or validate durable credentials without durable token lifecycle and revocation.
- Workspace token issuance with idempotency keys, because raw secret replay storage is intentionally unsupported.
- MCP direct local tools until they use the shared durable runtime/policy seam or an HTTP/client path.
- FUSE mutation persistence until a durable sparse mount/session model exists.
- Execution runner, semantic search, web console, and derived index routes.
- Final-object deletion/GC endpoints or workers until fencing and retention are implemented.
- Any route whose policy decision cannot be audited before mutation.

### Startup gates for `STRATUM_CORE_RUNTIME=durable-cloud`

Startup must remain fail-closed unless all of these are true:

- The binary is built with the required durable features, including `postgres`.
- `STRATUM_BACKEND=durable` and `STRATUM_CORE_RUNTIME=durable-cloud` are both present; durable-cloud without durable backend must fail with no local state files created.
- Migration mode has verified or applied the current catalog, and `ensure_control_plane_ready()` passes.
- Postgres connection config is hosted-ready: no password-bearing URLs, TLS-capable connector for non-local targets, schema selection validated, and no raw URL logging.
- R2/S3 config is valid, credential fields are redacted, object prefix/region settings are explicit, and the object byte store is reachable when live object routing is required.
- Durable auth/session store is available and rejects local-only principals by default in hosted mode.
- Shared policy engine is available for HTTP, MCP, CLI, FUSE, and embedded runtime calls.
- Recovery stores, scheduler/readiness, and bounded admin run controls are available.
- Idempotency retention/quota policy is configured, with no raw secret replay.
- Final-object deletion remains disabled unless the deletion/GC slice has landed; startup should report cleanup claims as observable blockers, not silently delete.
- Tenant/repo routing is configured; fallback to `RepoId::local()` is allowed only in explicit local/dev mode.

Rollback point:

- If any gate fails, startup must exit before opening local `.vfs/state.bin`, local control-plane files, or durable object-byte routing. Existing tests already assert this pattern for the current durable-core fail-closed behavior and should be extended as each gate lands.

## Durable Auth And Session Model

### Current model

Current auth has three important shapes:

- `Authorization: User <name>` logs in through the core runtime, but `/auth/login` still calls `state.db.login()` directly.
- `Authorization: Bearer <token>` without workspace headers calls `state.core.authenticate_token()`.
- `Authorization: Bearer <workspace-token>` with `x-stratum-workspace` validates through `state.workspaces.validate_workspace_token()`, builds a `SessionScope` from read/write prefixes, then calls `state.core.session_for_uid(agent_uid)` and attaches a workspace mount with base/session refs.

Workspace metadata stores include:

- `WorkspaceRecord { id, name, root_path, head_commit, version, base_ref, session_ref }`
- `WorkspaceTokenRecord { id, workspace_id, name, agent_uid, secret_hash, read_prefixes, write_prefixes }`

Token secrets are hash-only at rest, but durable token lifecycle is incomplete:

- No expiry.
- No revocation or rotation.
- No idempotent token issuance because that would require secret-safe replay storage.
- No durable principal table or service-account lifecycle.
- No KMS/secret-manager posture for issued raw secrets.

### Target durable model

Broad durable runtime needs a durable principal/session boundary with these invariants:

- Principals are durable, repo/org-scoped actors: human users, service accounts, and agent identities.
- Tokens are durable records with hash-only secrets, scoped capabilities, issue time, expiry, revocation time, rotation lineage, and audit identity.
- Workspace bearer sessions are short-lived scoped sessions derived from durable workspace metadata and durable token validation.
- Session refs are first-class durable refs, not just route-provided metadata. A mounted workspace identity must carry repo id, workspace id, base ref, optional session ref, principal id, read/write prefixes, token id, and token version.
- Mounted workspace path projection must stay bounded and never expose backing paths outside the mount root in user-facing errors.
- Durable token validation must not rely on local `.vfs/workspaces.bin` or local `StratumDb` users in hosted mode.

### Replay and secret storage constraints

- Never persist raw workspace tokens, raw bearer tokens, idempotency keys, refresh tokens, R2 credentials, Postgres URLs with passwords, or request bodies containing secrets in replay records.
- Workspace token issuance remains non-idempotent until the replay body can be represented as a one-time delivery receipt or protected by a KMS-backed encrypted secret envelope with explicit lifecycle semantics.
- Token validation audit should record token id/hash prefix surrogate, workspace id, repo id, principal id, outcome, reason code, request correlation, and expiry/revocation state. It must not record raw token material.

### Revocation requirements

Before broad runtime:

- Add token expiry and revocation checks to validation.
- Add admin revoke/rotate APIs or keep token issuance disabled in hosted durable mode.
- Add tests proving revoked, expired, wrong-workspace, wrong-repo, and wrong-prefix tokens fail across HTTP and at least one non-HTTP caller seam.
- Add audit events for token issue, validate allow/deny, revoke, rotate, and session mount creation.

Rollback point:

- If durable auth cannot prove revocation and scoped mount identity, keep `STRATUM_CORE_RUNTIME=durable-cloud` fail-closed and keep workspace token issuance limited to guarded/local workflows.

## Policy Enforcement Parity

### Current enforcement

HTTP routes currently do most protected-change enforcement:

- `src/server/policy.rs` evaluates protected ref/path rules from `ReviewStore` and returns route policy decisions.
- FS routes call route helpers before mutating protected paths and append policy allow/deny audit events.
- VCS routes guard commit, revert, ref update, and durable revert path changes.
- Review merge can use approval-state information to allow a protected merge.
- Mutating route audit is content-free and includes route/correlation/idempotency presence where available.

Gaps:

- `RoutePolicyAction` is route-facing; it is not enforced inside `CoreDb`, backend transaction execution, MCP, FUSE, or embedded local callers.
- `DurableMutationEngine` and lower backend store traits do not require a policy decision token before mutation.
- `stratum_mcp` calls `StratumDb` directly for write, mkdir, delete, move, commit, log, and revert.
- `fuse_mount.rs` mutates a `VirtualFs` through POSIX helpers using kernel request uid/gid sessions, outside the HTTP policy/review/audit route stack.
- `stratumctl` is HTTP-based, so it inherits route enforcement only when pointed at a server whose runtime is correctly configured.

### Shared policy seam needed

Introduce a lower shared seam before broad durable runtime:

- `PolicyEngine` or equivalent trait owned below route handlers.
- Inputs: repo id, actor/principal, session/workspace identity, target ref, optional source ref, action, changed paths, path access mode, request correlation, idempotency presence, review approval context.
- Output: allow/deny decision, stable reason code, bounded redacted details, and an audit event builder.
- Mutation APIs must require either an allow decision or an approved review merge context for protected refs/paths.
- Store-level or transaction-level code should not perform protected mutations from raw route inputs.

Protected boundaries:

- Protected ref checks apply to direct VCS commit, revert, ref update, and review reject/merge conflict paths.
- Protected path checks apply to FS write, mkdir, delete, copy, move, metadata update, durable revert changed paths, and review merge changed paths.
- Deny must override allow. Review merge can allow only when approval decision matches the current source/target/head and required rules.
- Policy must not depend on route-only path strings after durable object/tree reconstruction; changed paths need to be computed at the durable runtime boundary and passed to policy before mutation.

Audit requirements:

- Emit allow and deny decisions before mutation for protected-aware actions.
- Include action, decision, reason code, repo id, target ref, changed-path count, matched-rule counts, workspace id if present, session ref if present, request correlation presence, and idempotency presence.
- Exclude raw paths beyond bounded denied path display, file content, commit messages, approval/comment text, raw tokens, raw database/object-store errors, and raw idempotency keys.
- Audit append failure before mutation must fail closed. Audit append failure after visibility must enter recovery and return a partial/recovery response.

Rollback point:

- If a caller surface cannot use the shared policy seam, that surface must remain local-only or disabled for durable-cloud.

## Tenant/Repo Routing

### Current assumptions

- `RepoId::local()` is the current hard-coded durable repo identity in server durable wiring.
- Postgres tables are repo-aware for core durable metadata, but many current control-plane rows are global or `repo_id IS NULL`.
- Review Postgres storage uses `RepoId::local()` until review becomes repo-aware.
- Workspace metadata has `repo_id` in the schema, but current route/domain APIs do not expose a durable org/repo/workspace selector.
- Public route shapes mostly omit repo id and target the singleton local workspace/repo.

### Required routing model

Before broad runtime, define:

- Organization id and repo id selection for every hosted request.
- Workspace id to repo id validation, including base ref/session ref ownership.
- Principal membership in org/repo.
- Route path or header strategy for repo selection, with a compatibility mode for local singleton routes.
- Store-bundle construction keyed by repo/org where needed, without per-request raw env parsing.
- Audit sequence and query model: repo-scoped where appropriate, org/global only for administrative events.

Migration path:

- Keep local/default route compatibility mapped to `RepoId::local()` only when `STRATUM_BACKEND=local` or an explicit local compatibility flag is active.
- Add repo id to durable workspace creation/lookup before broad durable serving.
- Make review/protected rules repo-aware in trait methods, not only in Postgres schema.
- Make idempotency scopes include repo id, workspace id where applicable, route, actor, and action.
- Add tests proving two repos with the same ref names, paths, workspace names, and idempotency keys cannot affect each other.

Rollback point:

- If a route cannot resolve repo id unambiguously, it must return a redacted bad-request/not-supported response rather than defaulting to `RepoId::local()` in durable-cloud.

## Storage And Operations

### Postgres posture

Current posture:

- `PostgresMetadataStore` connects per operation with `tokio-postgres` and `NoTls`.
- Runtime config rejects password-bearing Postgres URLs and can use `PGPASSWORD`.
- Remote/TLS-required Postgres URLs fail closed until TLS support is wired.
- Migration preflight and readiness checks exist.

Required before broad runtime:

- Connection pool with bounded max connections, timeouts, cancellation behavior, health metrics, and redacted errors.
- TLS-capable connector for hosted Postgres; no hosted remote `NoTls`.
- Secret manager/KMS story for database credentials, not raw URL replay or logging.
- Query and transaction timeout policy, including advisory lock timeout policy.
- Multi-node tests for ref CAS/source-checked updates, recovery claims, idempotency, and audit sequence allocation.

### R2/S3 posture

Current posture:

- R2/S3-compatible config is validated and redacted.
- Guarded durable object writes route through `BlobObjectStore` when the guarded capability is enabled.
- Final-object delete mode remains fail-closed.

Required before broad runtime:

- Hosted credential handling via secret manager or environment injection policy with redacted diagnostics.
- Bucket/prefix tenant isolation rules.
- Object write/read timeout, retry, and circuit-breaker policy.
- Lifecycle policy for staged upload cleanup.
- Object repair worker for final bytes missing metadata.
- Explicit final-object deletion fence before any destructive cleanup.

### Final-object deletion and GC prerequisites

Do not enable final-object deletion until:

- Metadata writers consult a durable deletion fence or generation token.
- Cleanup workers can prove object unreachable from refs, commits, sessions, recovery claims, and in-flight idempotency/recovery contexts.
- CAS-lost durable mutation objects are either repaired or proven unreachable.
- GC has dry-run, bounded batch, audit, metrics, and rollback/hold windows.

### Idempotency retention/quota

Current posture:

- Idempotency records are durable in Postgres for control-plane/guarded routes.
- Rows retain indefinitely.
- Workspace token issuance rejects idempotency keys because replay would include a raw secret.

Required before broad runtime:

- Retention TTL by scope/action.
- Quotas per repo/org/principal/workspace.
- Stale pending takeover policy and sweep worker.
- Replay body classification: secret-free replay allowed; secret-bearing replay rejected or encrypted with one-time reveal semantics.
- Operator visibility for retained bytes/rows and denied quota decisions.

### Recovery and observability readiness gates

Before broad runtime:

- Recovery status must include pre-visibility, post-CAS, FS mutation, and object cleanup health.
- Scheduler health must be visible and bounded.
- Mutating routes must pre-enqueue recovery before side effects that can become visible.
- Startup readiness must fail when recovery stores are unavailable.
- Operators must have documented drain/run/observe commands for any broad durable mutation path.

Rollback point:

- If recovery cannot prove whether a mutation is visible or repairable, the route must return a redacted recovery-required or conflict response and must not record a normal success replay.

## Implementation Slices

### Slice 1: Durable Auth/Session Routing Foundation

**Goal:** Move auth/session validation for hosted durable mode off local-only state and into durable principal/workspace/session stores.

**Files:**

- Modify: `src/auth/session.rs`
- Modify: `src/workspace/mod.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_auth.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `migrations/postgres/*.sql`
- Test: `tests/server_startup.rs`
- Test: focused auth/workspace route tests

**Scope:**

- Add durable principal/service-account/token concepts or a narrow foundation table if full identity is deferred.
- Make `/auth/login` and workspace token issuance use the runtime/core auth seam rather than `state.db` in durable hosted mode.
- Add token expiry/revocation fields and validation gates.
- Keep workspace token issuance non-idempotent unless secret-safe replay is implemented.
- Include repo id in workspace/session identity where durable-cloud is used.

**What stays fail-closed:**

- Broad `STRATUM_CORE_RUNTIME=durable-cloud`.
- Token issuance with idempotency keys.
- MCP/FUSE durable auth.
- Refresh-token flows and external OIDC/SAML.

**Acceptance criteria:**

- Durable hosted mode can validate a durable workspace bearer without local workspace metadata files.
- Revoked/expired/wrong-workspace tokens fail.
- Raw token secrets never appear in storage, logs, replay, or audit details.
- Local mode behavior remains unchanged.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked auth --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked --test server_startup durable_env -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 2: Policy Enforcement Below Route Layer

**Goal:** Make protected ref/path policy a shared runtime decision required by HTTP and future non-HTTP durable callers.

**Files:**

- Modify: `src/server/policy.rs`
- Modify: `src/server/core.rs`
- Modify: `src/backend/durable_mutation.rs`
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Test: policy unit tests and route parity tests

**Scope:**

- Introduce a policy trait or decision token that can be passed below route handlers.
- Require protected-aware mutations to carry an allow/review-approved decision before durable mutation/ref update.
- Compute durable changed paths before policy for commit/revert/review merge.
- Emit content-free allow/deny audit before mutation.

**Optional reference (pattern-only, do not extract code):**

- `mirage/typescript/packages/core/src/vfp/{capability,types}.ts` has a stable enum vocabulary for posix ops × filetypes × command flag filters. It is not a runtime policy engine, but the shape is a reasonable starting point to study before designing the audit details/reason-code schema for policy allow/deny events. ~30 minutes of skim, optional.

**What stays fail-closed:**

- MCP and FUSE durable mutations until they can request policy decisions.
- Broad durable runtime startup.
- Any mutation lacking a policy decision.

**Acceptance criteria:**

- HTTP behavior is unchanged except for using the shared seam.
- Lower durable mutation APIs cannot be called in tests without a policy decision where protected rules apply.
- A policy audit failure before mutation fails closed.
- Protected path/ref denials are consistent for local and guarded durable routes.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked server::policy --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 3: Tenant/Repo Routing Foundation

**Goal:** Replace implicit hosted `RepoId::local()` assumptions with explicit durable repo routing while preserving local compatibility.

**Files:**

- Modify: `src/backend/mod.rs`
- Modify: `src/workspace/mod.rs`
- Modify: `src/review.rs`
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `migrations/postgres/*.sql`

**Scope:**

- Define request repo resolution for hosted mode.
- Make workspace/review/protected-rule traits repo-aware.
- Make idempotency scopes repo-aware.
- Keep local singleton routes mapped to `RepoId::local()` only in explicit local mode.

**What stays fail-closed:**

- Durable-cloud startup for requests without explicit repo routing.
- Cross-repo review merge.
- Multi-org admin APIs unless principal membership is defined.

**Acceptance criteria:**

- Two repos can hold the same ref/path names without metadata collisions.
- Workspace token for repo A cannot read/write repo B.
- Protected path/ref rules are repo-scoped.
- Existing local tests continue to pass.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend:: --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked review::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
cargo test --locked server::routes_fs::tests --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 4: Broad Durable Core Runtime Incremental Enablement

**Goal:** Add a broad durable `CoreDb` open path behind startup gates without enabling unsupported routes.

**Files:**

- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/core.rs`
- Modify: `src/bin/stratum_server.rs`
- Modify: `tests/server_startup.rs`

**Scope:**

- Add a durable `CoreDb` construction path that does not require local `StratumDb` for durable FS/VCS requests.
- Gate each route method by capability readiness.
- Keep route-by-route fail-closed behavior explicit.
- Preserve startup no-local-file guarantees when durable-cloud fails.

**What stays fail-closed:**

- Routes without durable auth/session and lower policy.
- MCP/FUSE direct durable callers.
- Final-object deletion/GC.
- Semantic search, web console, execution runner.

**Acceptance criteria:**

- `STRATUM_CORE_RUNTIME=durable-cloud` can start only in a test configuration where auth/session, policy, repo routing, storage, and recovery gates pass.
- Unsupported durable methods return stable redacted `NotSupported`.
- No local `.vfs/state.bin` is created in durable-cloud mode.
- Guarded route behavior remains available until replaced slice-by-slice.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 5: Final-Object Deletion And Unreachable Object GC

**Goal:** Safely clean CAS-lost or unreachable durable object bytes only after metadata fencing and recovery visibility are proven.

**Files:**

- Modify: `src/backend/object_cleanup.rs`
- Modify: `src/backend/blob_object.rs`
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/durable_mutation.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `migrations/postgres/*.sql`

**Scope:**

- Implement metadata-writer fencing for final-object deletion.
- Add dry-run reachability analysis from refs, commits, sessions, recovery claims, idempotency records, and cleanup claims.
- Add bounded deletion worker with audit and status.

**Optional reference (pattern-only, do not extract code):**

- `smfs/crates/smfs-core/src/sync/push.rs` has a hardened bounded-worker UX for a queue-driven background process: state vocabulary `pending/inflight/done/failed/poisoned`, exponential backoff schedule (`backoff_ms`: 500ms → 1s → 2s → 5s → 15s → 30s → 60s, then capped), bounded concurrency (`PUSH_CONCURRENCY = 8`), `Notify`-based wakeup with a 200ms fallback poll, drain-on-unmount semantics, and stuck-processing INFO/WARN/STOP tier logging. Stratum already has `src/backend/object_cleanup.rs` and the post-CAS recovery worker that solve most of this internally, so this is mostly a sanity-check reference for the GC worker's status/drain UX surface and backoff tuning. ~30 minutes of skim, optional.

**What stays fail-closed:**

- Automatic deletion until dry-run and fence tests pass.
- Cross-tenant deletion without repo/org routing.
- Deletion of objects referenced by unresolved recovery or retained idempotency.

**Acceptance criteria:**

- Final object deletion cannot race metadata repair into data loss.
- Unreachable objects are reported before deletion.
- Cleanup worker is bounded, auditable, and retry-safe.
- Existing repair tests still prove metadata repair before delete.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
cargo test --locked backend::durable_mutation --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 6: Idempotency Retention/Quota And Secret-Safe Replay

**Goal:** Prevent unbounded idempotency growth and define which responses may be replayed safely.

**Files:**

- Modify: `src/idempotency.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/server/idempotency.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `migrations/postgres/*.sql`

**Scope:**

- Add retention TTL and sweep worker.
- Add quotas by scope/repo/principal/workspace.
- Add response classification for secret-free versus secret-bearing replay.
- Keep workspace token issuance non-idempotent unless a one-time or encrypted replay model lands.

**What stays fail-closed:**

- Secret-bearing replay by default.
- Broad durable startup without configured retention/quota.
- Sweeping records required by unresolved recovery.

**Acceptance criteria:**

- Stale completed records are swept only when safe.
- Pending stale records have takeover or abort semantics.
- Quota failures are redacted and audited.
- No raw secret appears in replay storage.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 7: Hosted Storage Operations Hardening

**Goal:** Make Postgres/R2 durable runtime operations hosted-ready without changing product surface.

**Files:**

- Modify: `src/backend/runtime.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/remote/blob.rs`
- Modify: `src/server/mod.rs`
- Modify: `tests/server_startup.rs`
- Modify: `scripts/check-postgres-migrations.sh`
- Modify: `scripts/check-r2-object-store.sh`

**Scope:**

- Add Postgres pooling and TLS connector.
- Add hosted secret posture documentation/tests.
- Add timeouts/retries/circuit-breaker behavior for object storage.
- Add readiness metrics for DB/object-store/recovery stores.

**Optional reference (pattern-only, do not extract code):**

- `mirage/typescript/packages/node/src/resource/{s3,r2}` has hardened S3/R2-compatible client configuration across many real backends (timeouts, retries, error mapping, region/prefix conventions). Do not copy code — Stratum is Rust on `aws-sdk-s3` — but worth a 30-min skim for the knob choices and timeout/retry/circuit-breaker policy shape called for in this slice's scope. Optional.

**What stays fail-closed:**

- Remote Postgres without TLS.
- Durable-cloud startup without configured pool limits/timeouts.
- Any logs or errors containing raw credential material.

**Acceptance criteria:**

- Hosted-like Postgres URL with TLS can be accepted in feature-gated tests.
- Local/Unix-socket development remains supported.
- Secret leak sentinel tests pass.
- Durable startup reports store readiness without raw endpoints or passwords.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable_env -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
git diff --check
```

### Slice 8: Non-HTTP Caller Policy And Runtime Parity

**Goal:** Ensure MCP, CLI, FUSE, and embedded callers cannot bypass durable policy/session/runtime gates.

**Files:**

- Modify: `src/bin/stratum_mcp.rs`
- Modify: `src/bin/stratumctl.rs` only if CLI auth defaults need adjustment.
- Modify: `src/fuse_mount.rs`
- Modify: `src/server/core.rs`
- Modify: shared policy/runtime modules from earlier slices.

**Scope:**

- Decide whether each non-HTTP surface uses HTTP APIs, shared `CoreDb` plus policy, or stays local-only.
- Move MCP workspace session validation away from local metadata file reads for durable hosted mode.
- Keep FUSE durable mutation unsupported until sparse mount design lands.

**What stays fail-closed:**

- MCP durable writes without shared policy/audit/idempotency.
- FUSE durable writes without session refs and cache/recovery semantics.
- Embedded direct store mutation without policy decision token.

**Acceptance criteria:**

- Tests prove MCP cannot mutate protected paths/refs in durable hosted mode.
- FUSE durable mode returns unsupported unless mounted with a durable session model.
- `stratumctl` remains HTTP-only for hosted durable operations.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked --bin stratum_mcp -- --nocapture
cargo test --locked --bin stratumctl -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

## Blockers Before Broad Runtime Enablement

Do not enable broad `STRATUM_CORE_RUNTIME=durable-cloud` until these blockers are cleared:

- Durable auth/session routing with token expiry/revocation.
- Shared policy seam below routes, used by all durable mutation surfaces.
- Explicit tenant/repo routing with no hosted fallback to `RepoId::local()`.
- Postgres pooling/TLS/secrets posture.
- R2/S3 hosted credential, timeout, and lifecycle posture.
- Idempotency retention/quota and secret-safe replay rules.
- Recovery readiness for all visible mutation side effects.
- Final-object deletion remains disabled or fully fenced; object cleanup claims are visible.
- Non-HTTP caller bypasses are either closed or explicitly local-only.
- Audit pipeline covers auth, policy allow/deny, read where required, write, review, merge, revert, token lifecycle, recovery, and cleanup actions.

## Planning Slice Verification

This slice is docs-only. Required verification:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked --no-run
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

No Rust code should change in this slice. If a future edit adds Rust code, run both clippy gates, focused tests for touched modules, and `cargo test --locked --lib --tests`.
