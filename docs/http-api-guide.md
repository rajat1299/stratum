# HTTP API Guide

The `stratum-server` binary exposes a REST API for programmatic access to stratum. This guide covers every endpoint with request and response examples.

## Starting the Server

```bash
# Default: listen on 127.0.0.1:3000
cargo run --release --bin stratum-server

# Custom address
STRATUM_LISTEN=0.0.0.0:8080 cargo run --release --bin stratum-server

# With custom data directory and logging
STRATUM_DATA_DIR=/var/data/stratum \
RUST_LOG=stratum=debug \
cargo run --release --bin stratum-server
```

## Capabilities

Fetch the server capability manifest:

```bash
curl -i http://localhost:3000/v1/capabilities
```

`GET /v1/capabilities` is unauthenticated and returns `Cache-Control: max-age=60, must-revalidate`. The response body includes revision `2026-05-17-2`, coarse server/runtime identity, auth modes, mounted route surfaces, idempotency support, diff/protection/recovery support, and public limits. It intentionally omits secrets, DB URLs, R2 endpoints, local filesystem paths, object keys, repo ids, request bodies, tokens, commit messages, raw backend errors, and per-user fields.

Durable-cloud manifests advertise the current mounted-session HTTP surface explicitly: committed and mounted-session filesystem/search/tree reads, mounted-session filesystem write/patch/delete/copy/move, VCS read surfaces, VCS ref create/update, VCS commit/revert, protected ref/path rules, and change-request mutation routes are available. Durable-cloud filesystem mutations include `requires: ["workspace-bearer", "durable-session-ref"]`; durable-cloud VCS/review mutations include `workspace-bearer`, `durable-admin-principal`, and `repo-bound-principal`, with `durable-session-ref` added for `POST /vcs/commit`. These admin routes require a repo-scoped workspace bearer whose durable principal is root or wheel-scoped for the matching repo; `Authorization: User root` is local-only and is not accepted by durable-cloud. Auth login, workspace issuance/listing, runs, audit listing, semantic search, execution, and VCS recovery operator routes remain unavailable or fail-closed with the stable durable-cloud unsupported reason. Guarded durable recovery appears available only when the guarded durable commit route actually serves the operator endpoint; `recovery.scheduler_present` can still be true for durable-cloud because the background scheduler is attached even while the route remains unsupported.

`hints.banner` is either `null` or a closed object with exactly `kind` and `text`. `kind` is `"info"` or `"warn"`, and `text` is server-bounded to 280 characters at construction time. The v1 banner contract does not include markdown, action URLs, links, or extra keys; clients should ignore or reject anything outside that shape as a contract violation.

The `/v1/capabilities` wire shape is locked for the life of v1. Adding a new optional field under an existing group or adding a new top-level group is additive and should bump `revision`. Renaming or removing an existing field, changing field semantics, widening an existing enum, or changing an existing field type is breaking and must ship as a new endpoint such as `GET /v2/capabilities` rather than mutating v1. Route availability flips are not breaking because they are the contract's runtime signal. Once a v2 manifest ships, v1 should remain available for at least 60 days.

The checked-in SDK contract fixtures are generated from the Rust manifest shape at `sdk/contracts/capabilities.v1.json` and `sdk/contracts/capabilities.v1.durable-cloud.json` by running:

```bash
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 \
  cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
```

## Authentication

Filesystem, search, VCS, and workspace management requests require an auth header. Three modes are supported:

| Header | Description |
|---|---|
| `Authorization: User <username>` | Authenticate as a named user |
| `Authorization: Bearer <token>` | Authenticate with an agent API token |
| *(no header)* | Rejected, except for `/health` and `/v1/capabilities` |

Hosted workspace requests can also include:

| Header | Description |
|---|---|
| `X-Stratum-Workspace: <uuid>` | Resolve a bearer token as a hosted workspace token issued by the gateway |

Workspace bearer tokens produce a normal agent session plus the persisted token scope. For filesystem, search, and tree routes, the workspace `root_path` is mounted as `/`, so request paths are workspace-relative. A workspace at `/incidents/checkout-latency` exposes `/read/a.txt` as the backing path `/incidents/checkout-latency/read/a.txt`. The stored `read_prefixes` and `write_prefixes` remain backing absolute paths and are still enforced before Unix-style permissions are checked. Under durable-cloud, mounted-session filesystem mutations require a valid bearer workspace token, matching `X-Stratum-Workspace`, durable principal/session validation, and a workspace `session_ref`; they update that session ref and do not fall back to local `.vfs/state.bin`. Workspace bearer tokens cannot call workspace metadata admin endpoints. Global VCS endpoints remain admin-gated.

Examples:

```bash
# As a named user
curl -H "Authorization: User alice" http://localhost:3000/fs/

# As an agent (token from `addagent`)
curl -H "Authorization: Bearer a1b2c3d4..." http://localhost:3000/fs/

# As root
curl -H "Authorization: User root" http://localhost:3000/fs/
```

## Idempotency For Mutating Requests

Most mutating HTTP endpoints accept an optional `Idempotency-Key` header so clients can safely retry after network failures. Supported endpoints are:

- `PUT /fs/{path}`
- `PATCH /fs/{path}`
- `DELETE /fs/{path}`
- `POST /fs/{path}?op=copy|move`
- `POST /runs`
- `POST /vcs/commit`
- `POST /vcs/revert`
- `POST /vcs/refs`
- `PATCH /vcs/refs/{name}`
- `POST /protected/refs`
- `POST /protected/paths`
- `POST /change-requests`
- `POST /change-requests/{id}/approvals`
- `POST /change-requests/{id}/reviewers`
- `POST /change-requests/{id}/comments`
- `POST /change-requests/{id}/reject`
- `POST /change-requests/{id}/merge`
- `POST /change-requests/{id}/approvals/{approval_id}/dismiss`
- `POST /workspaces`
- `POST /workspaces/{id}/tokens` when secret replay KMS is configured

Durable-cloud advertises idempotency for the mounted-session filesystem mutations, VCS commit/revert/ref mutations, protected ref/path rule creation, and change-request/review mutations above. It does not advertise idempotency for unsupported run, workspace, audit, auth/login, semantic-search, execution, or recovery-operator routes.

When present, `Idempotency-Key` must be provided once, non-empty, visible ASCII, and at most 255 bytes. Stratum stores only a SHA-256 hash of the key.

The request fingerprint includes the route semantics, authenticated actor, workspace boundary when mounted, normalized path/ref/workspace inputs, relevant query/header fields, and normalized JSON request body where applicable. File write fingerprints include content length and SHA-256 digest, not raw file content.

A retry with the same key and same fingerprint replays the original JSON response and includes:

```http
X-Stratum-Idempotent-Replay: true
```

Reusing the same key with a different request returns `409 Conflict` without mutation. A duplicate in-progress request also returns `409 Conflict`. Invalid keys return `400 Bad Request` before mutation.

Authorization still runs before reservation and before replay. A stored replay is not returned to a caller that no longer has the required current access. If a mutation committed but audit recording failed, the idempotency record stores the same client-visible failure body, including `mutation_committed: true` and `audit_recorded: false`, so retries do not duplicate the committed side effect. If idempotency completion itself fails after mutation, the immediate response is redacted and includes `mutation_committed: true` plus `idempotency_recorded: false`.

Replay persistence is classified before storage. `secret_free` responses can be replayed as-is; `partial` responses are replayable only after route-specific sanitization such as omitting commit messages, review text, dismissal reasons, backing paths, or raw execution content. Generic `secret_bearing` replay remains rejected. Workspace-token issuance is the only secret-bearing route with idempotent replay: when secret replay KMS is configured, Stratum encrypts the exact success JSON once, stores only an encrypted replay envelope plus redacted replay metadata, and decrypts that envelope only for a same-key/same-fingerprint retry.

Secret replay KMS is configured with `STRATUM_SECRET_REPLAY_KMS_PROVIDER=local-aead`, `STRATUM_SECRET_REPLAY_KMS_KEY_ID=<stable-id>`, and `STRATUM_SECRET_REPLAY_KMS_KEY_B64=<base64-32-byte-key>`. Missing KMS config leaves the provider disabled; non-idempotent token issuance still works, but token issuance with `Idempotency-Key` fails closed before issuing a token. Decrypt failure, unknown key id, key rotation/removal, ciphertext corruption, malformed envelopes, and idempotency completion failure all return fixed redacted errors and do not issue a duplicate token under the same idempotency key. Raw workspace tokens, raw agent tokens, plaintext replay bodies, raw idempotency keys, encryption keys, DB URLs, SQL text, endpoints, and provider error details are not persisted or logged. Encrypted replay records follow the same completed-record retention and sweep behavior as other idempotency records.

Policy-aware idempotency stores support completed-record retention TTLs, stale-pending takeover/abort, and quotas. Configured quotas apply per idempotency scope and, where route context supplies the identity, per repo, workspace, or principal. A pending record younger than the stale threshold still returns the existing in-progress or conflict behavior. A stale pending record with the same fingerprint can be taken over with a fresh reservation token; a stale pending record with a different fingerprint is aborted and returns deterministic conflict without inserting the new request. Quota failures return a redacted `429 Too Many Requests` body:

```json
{"error":"idempotency quota exceeded","quota":"scope","audit_recorded":true}
```

When the audit store cannot record that quota failure, the same response shape uses `"audit_recorded": false` and does not include backend error details, raw scopes, idempotency keys, or request bodies.

## Backend Durability Status

By default, the HTTP server remains backed by local stores: `.vfs/state.bin` for the in-process filesystem and VCS state, plus local files for workspace metadata, review state, idempotency records, and audit events.

### Live CI Gates

Pull-request CI, including fork PRs, skips the live Postgres and R2 gates and relies on the existing local service-container, unit, syntax, and optional-skip gates. Scheduled workflows and protected-ref contexts require live secrets and run the live wrappers in required mode. Manual dispatches run the live jobs only when dispatched against a protected ref; manual runs on unprotected refs skip the live jobs. Live failures block only those scheduled or protected-ref live contexts; existing non-live CI jobs are unchanged. The live jobs select the `live-gates` GitHub environment so repo admins can scope these secrets to that environment.

Required GitHub secrets for live CI:

- `STRATUM_POSTGRES_TEST_URL`
- `STRATUM_R2_BUCKET`
- `STRATUM_R2_ENDPOINT`
- `STRATUM_R2_ACCESS_KEY_ID`
- `STRATUM_R2_SECRET_ACCESS_KEY`

`STRATUM_POSTGRES_TEST_URL` must be a password-free Postgres connection string. `STRATUM_POSTGRES_TEST_PASSWORD` is optional for providers that use password authentication; the workflow maps it to both `STRATUM_POSTGRES_TEST_PASSWORD` and `PGPASSWORD`. Missing `STRATUM_POSTGRES_TEST_URL` fails closed in required live contexts and skips in optional local wrapper checks. Provider authentication failures are treated as live failures with redacted logs.

Optional R2 tuning secrets are `STRATUM_R2_REGION`, `STRATUM_R2_PREFIX`, `STRATUM_R2_REQUEST_TIMEOUT_MS`, `STRATUM_R2_CONNECT_TIMEOUT_MS`, `STRATUM_R2_MAX_ATTEMPTS`, `STRATUM_R2_RETRY_BASE_DELAY_MS`, and `STRATUM_R2_RETRY_MAX_DELAY_MS`.

Local optional skip checks:

```bash
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
```

Required live wrapper checks:

```bash
STRATUM_LIVE_GATE_REQUIRED=1 \
  ./scripts/ci-live-postgres-gate.sh

STRATUM_LIVE_GATE_REQUIRED=1 \
  ./scripts/ci-live-r2-gate.sh

STRATUM_LIVE_GATE_REQUIRED=1 \
  ./scripts/ci-live-durable-cloud-gate.sh
```

The Postgres, R2, and durable-cloud startup live CI wrappers are wired and provider-verified green on protected main run `26179867532` from 2026-05-20. The durable-cloud startup wrapper runs the exact combined Postgres+R2 durable-cloud startup selector without `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`. The wrappers mask configured secret-bearing values in GitHub Actions and suppress raw failure output to avoid leaking database URLs, passwords, endpoints, bucket names, access keys, object keys, or raw backend/provider errors.

### Pre-Cutover Load And Chaos Suite

The bounded pre-cutover suite exercises the durable-cloud and guarded durable paths that must stay stable before broader runtime cutover. The default run is provider-free and uses deterministic in-memory durable stores:

```bash
./scripts/check-pre-cutover-load-chaos.sh
```

The suite runs focused tests for mounted-session filesystem load, VCS commit/recovery chaos, recovery scheduler phase/shutdown limits, non-destructive object cleanup, idempotency retry/retention pressure, R2 adapter error redaction, and durable startup fail-closed behavior with and without the `postgres` feature. It verifies duplicate-side-effect prevention, persisted claim/lease fencing, bounded manual and scheduler recovery behavior, and redaction of status/error surfaces. The default run does not require Postgres or R2 credentials and does not enable destructive object deletion.
The runner scrubs live provider environment variables from local cargo selectors, so ambient developer credentials do not make the default suite talk to Postgres or R2.

To include the existing optional live provider gates, opt in explicitly:

```bash
STRATUM_PRE_CUTOVER_LIVE=1 ./scripts/check-pre-cutover-load-chaos.sh
```

Set `STRATUM_LIVE_GATE_REQUIRED=1` only in protected contexts where live credentials are expected. The suite uses `scripts/ci-live-postgres-gate.sh` and `scripts/ci-live-r2-gate.sh` for live checks so secret-bearing output remains suppressed; direct raw provider scripts remain available for local no-secret skip checks.

Server startup parses `STRATUM_BACKEND`, defaulting to `local`. When `stratum-server` is built without the optional `postgres` feature, `STRATUM_BACKEND=durable` still fails closed before serving. When built with `--features postgres`, `STRATUM_BACKEND=durable` validates the durable prerequisites, runs the Postgres migration preflight, and starts the server with pooled Postgres-backed workspace metadata, idempotency, audit, and review stores. `STRATUM_POSTGRES_URL` must not include a password; the current deployment-secret seam reads `PGPASSWORD`, and future secret-manager providers should plug into that seam rather than embedding credentials in URLs. Remote Postgres targets must set `sslmode=require`; explicit localhost, loopback `hostaddr`, and Unix-socket targets remain accepted without TLS for local development. `STRATUM_R2_ENDPOINT` must use `https` and must not include userinfo or query parameters. Plaintext R2/S3-compatible endpoints are accepted only for loopback local-test endpoints when `STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT=1`. R2 credentials are validated for durable configuration and are used only by explicitly gated durable routes; credentials are not logged by the runtime selector.

Server startup also parses `STRATUM_CORE_RUNTIME`, defaulting to `local-state`. This setting is separate from `STRATUM_BACKEND`. `local`, `local-state`, `state-file`, and `snapshot` select the existing `.vfs/state.bin` core runtime. `durable`, `durable-cloud`, and `postgres-r2` select the Postgres/R2 core runtime family; `durable-cloud` no longer requires `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`.

`STRATUM_CORE_RUNTIME=durable-cloud` is accepted only with all of these explicit gates: `STRATUM_BACKEND=durable`, `STRATUM_DURABLE_AUTH_SESSION_READY=1`, `STRATUM_DURABLE_POLICY_READY=1`, `STRATUM_DURABLE_REPO_ROUTING_READY=1`, `STRATUM_DURABLE_RECOVERY_READY=1`, `STRATUM_DURABLE_CORE_REPO_ID=<non-local RepoId>`, `STRATUM_IDEMPOTENCY_COMPLETED_RETENTION_SECONDS=<positive bounded integer>`, `STRATUM_IDEMPOTENCY_PENDING_STALE_SECONDS=<positive bounded integer>`, and `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_SCOPE=<positive bounded integer>`. It also requires explicit hosted storage posture knobs: `STRATUM_POSTGRES_POOL_MAX_SIZE`, `STRATUM_POSTGRES_CONNECT_TIMEOUT_MS`, `STRATUM_POSTGRES_OPERATION_TIMEOUT_MS`, `STRATUM_POSTGRES_POOL_ACQUIRE_TIMEOUT_MS`, `STRATUM_R2_REQUEST_TIMEOUT_MS`, `STRATUM_R2_CONNECT_TIMEOUT_MS`, `STRATUM_R2_MAX_ATTEMPTS`, `STRATUM_R2_RETRY_BASE_DELAY_MS`, and `STRATUM_R2_RETRY_MAX_DELAY_MS`. Optional bounded quota gates are `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_REPO`, `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_WORKSPACE`, and `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_PRINCIPAL`. The durable-cloud runtime rejects `STRATUM_DURABLE_COMMIT_ROUTE=1`; that guarded capability remains local-state only. Missing or incomplete durable-cloud configuration fails closed before local `.vfs/state.bin` creation, and invalid durable-core repo ids are rejected without echoing raw values.

When durable-cloud gates pass, `stratum-server` opens durable stores, constructs `DurableCoreRuntime` directly from the durable `StratumStores`, checks R2/S3-compatible object-store readiness with a bounded list probe, and does not open or create local `.vfs/state.bin`. `/health` returns `core_runtime: "durable-cloud"` and leaves local core counters such as `commits`, `inodes`, and `objects` as `null`. The `readiness` block reports only startup/configuration booleans for local core DB, control-plane stores, object store, and recovery stores; it does not include connection strings, endpoints, credentials, object keys, or backend error details. Durable-cloud request sessions are expected to come from workspace bearer validation through durable workspace/principal stores; missing repo identity, workspace/repo mismatch, router/repo mismatch, conflicting or duplicate `X-Stratum-Repo`, malformed repo headers, and non-local workspace tokens without a durable principal all fail closed without falling back to `RepoId::local()`.

The durable-cloud router exposes durable-backed committed and mounted-session filesystem reads (`GET /fs`, `GET /fs/{path}`), filesystem mutations for mounted sessions with a `session_ref` (`PUT /fs/{path}`, `PATCH /fs/{path}`, `DELETE /fs/{path}`, and `POST /fs/{path}?op=copy|move`), search/tree reads (`GET /search/grep`, `GET /search/find`, `GET /tree`, `GET /tree/{path}`), VCS reads (`GET /vcs/log`, `GET /vcs/status`, `GET /vcs/diff`, and `GET /vcs/refs`), VCS mutations (`POST /vcs/refs`, `PATCH /vcs/refs/{name}`, `POST /vcs/commit`, and `POST /vcs/revert`), protected-rule routes, and change-request review routes. FS/search/tree reads use durable ref, commit, and object stores. Mounted-session FS mutations materialize or advance the durable workspace session ref through the durable mutation executor, idempotency store, audit store, and recovery ledger; they require the workspace bearer/session context described above and never read or write local `.vfs/state.bin`. Durable-cloud VCS/review/protected mutations use durable commit/ref/review/protection/idempotency/audit stores and require the repo-scoped workspace bearer admin-principal seam, not local `User root`. Unsupported route groups return stable JSON `501`:

```json
{"error":"stratum: operation not supported: durable-cloud route is not supported yet"}
```

Live provider execution remains explicit. Local pre-cutover runs are provider-free unless `STRATUM_PRE_CUTOVER_LIVE=1` is set; protected live contexts should run the redacted wrappers, including `scripts/ci-live-durable-cloud-gate.sh`, with `STRATUM_LIVE_GATE_REQUIRED=1` when credentials are expected. Rollback for the default gate flip is to reintroduce the explicit dev gate in runtime parsing and leave `STRATUM_CORE_RUNTIME` unset or set to `local-state`.

The internal durable `CoreDb` implementation owns the composed backend store bundle and repo id, binds durable write ordering to the executable core transaction semantics contract, and can read committed filesystem state from the durable `main` ref or mounted session refs. The implemented durable methods include `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, capped plain `find_as`, capped plain regex `grep_as`, mounted-session filesystem write/metadata/delete/copy/move operations, `list_refs`, `vcs_log_as`, `vcs_status_as`, and `vcs_diff_as` over visible `main` or mounted durable session refs. Narrow durable VCS and review mutation seams now expose durable ref create/update, commit, revert, protection, and change-request mutations to durable-cloud without enabling the guarded local durable commit route.

`STRATUM_DURABLE_COMMIT_ROUTE=1` is a separate, explicit route gate. It is accepted only with `STRATUM_BACKEND=durable` and a `postgres` build, while `STRATUM_CORE_RUNTIME` remains `local-state`. Under that gate, mounted workspace filesystem mutations for sessions with owned durable refs can write, create directories, delete, copy, move, and update metadata by materializing a durable session ref, writing durable blob/tree objects, inserting an internal durable mutation commit, and CAS-updating the session ref. Guarded `POST /vcs/commit` promotes a durable session-ref tree for mounted durable workspaces; for non-mounted or no-session-ref cases it still uses the local `StratumDb` filesystem snapshot as the source tree until broad durable mutable workspace routing exists. The same guarded capability serves HTTP filesystem reads/listing/stat/tree plus capped `find` and regex `grep` from durable committed or mounted-session trees; makes admin `GET /vcs/log`, `GET /vcs/refs`, `POST /vcs/refs`, and `PATCH /vcs/refs/{name}` use durable commit/ref metadata; and routes guarded `GET /vcs/status`, `GET /vcs/diff`, and `POST /vcs/revert` through durable commit/ref/object/session primitives. Durable revert creates a new restore commit and advances `main` with source-checked ref CAS rather than applying text hunks. Guarded review creation and merge can also use durable source/target refs and durable commit metadata when both refs exist in the guarded stores; merge checks durable changed-path metadata for approval policy and advances the target ref through source-checked durable ref CAS. Scoped workspace bearer tokens are rejected for global VCS mutations, including guarded durable commit and revert. Local auth/session lookup still uses local `StratumDb`; guarded durable FS/VCS content paths avoid local filesystem/VCS state as their durable source of truth but do not remove all local state usage from server startup.

The same guarded gate exposes admin-only durable recovery controls. `GET /vcs/recovery` lists bounded, redacted post-CAS repair rows, pre-visibility recovery rows, durable filesystem mutation recovery rows, object cleanup rows, aggregate counts, and a `gc_dry_run` object-GC reachability summary. Default `GET /vcs/recovery` and default `POST /vcs/recovery/run` remain non-destructive. `POST /vcs/recovery/run` accepts an optional JSON body with `limit`, defaults to a small bounded run, caps the limit at 100, ignores any caller-supplied lease identity, runs due pre-visibility recovery before post-CAS commit repair, durable FS mutation recovery, and non-destructive object cleanup readiness, and returns redacted summaries for each section.

Operators can enable destructive CAS-lost final-object cleanup for one bounded admin `POST /vcs/recovery/run` request by sending `destructive_final_object_deletion: true`. This switch applies only to cleanup-claim-owned `DurableMutationCasLostObjectCleanup` final objects; broad unreachable commit/object GC remains `gc_dry_run` / dry-run/protocol-only with `deletion_enabled: false`, and the background scheduler remains non-destructive. `final_object_deletion_hold_seconds` is optional and is accepted only with the destructive flag; the maximum is `604800` seconds. When omitted, the server uses the default hold window. Tests may use `0`, but operator runs should use a conservative nonzero hold, for example:

```bash
curl -X POST http://localhost:3000/vcs/recovery/run \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{
    "limit": 10,
    "destructive_final_object_deletion": true,
    "final_object_deletion_hold_seconds": 3600
  }'
```

A first eligible destructive run records readiness and hold state without deleting bytes. A later destructive run after the hold expires repeats the eligibility checks, deletes final bytes and fenced metadata, and completes the cleanup claim. Recovery only uses persisted route-bound context; legacy no-context rows are not repaired by inference. When guarded durable stores are configured, server startup also starts one bounded background scheduler per store set to drain the same recovery queues without requiring manual route calls.

`STRATUM_DURABLE_MIGRATION_MODE` defaults to `status`, which reports pending migrations as a startup error and does not apply or adopt them. `STRATUM_DURABLE_MIGRATION_MODE=apply` applies pending migrations with the schema-scoped advisory lock, validates the final migration state, and then opens the durable control-plane stores. `STRATUM_DURABLE_MIGRATION_MODE=adopt` is only for legacy schemas that were manually migrated before `stratum_schema_migrations` existed: it takes the same schema-scoped startup lock, verifies the known catalog structure through Postgres catalog probes, and records known migrations as applied without replaying DDL. Adoption refuses dirty, unknown, checksum-mismatched, partially populated, or unverifiable schemas. `STRATUM_POSTGRES_SCHEMA` optionally selects the migration schema and defaults to `public`. Dirty, checksum-mismatched, unknown, or still-pending migration state blocks startup without echoing connection strings, passwords, R2 credentials, SQL text, migration SQL, operator input, or database-controlled migration names.

The durable multi-node locking posture is intentionally narrow. Ref creation/update, source-checked ref movement, durable session-ref mutation, recovery scheduler ticks, recovery claims, object cleanup claims, and final-object deletion fences keep their local CAS, source-check, lease-owner/token/expiry, metadata-fence, and idempotent-completion contracts. The only current lock-required Postgres sections are migration apply/adopt, object deletion fence key serialization, idempotency quota enforcement, idempotency retention sweep, and audit global sequence allocation. These use transaction-scoped Postgres advisory locks through a shared helper with stable SHA-256 subject hashing where a string subject is needed. Stratum does not require Redis or a public lock service for the current durable-cloud posture.

This is not the full durable filesystem/VCS cutover. The guarded durable route path covers committed reads, mounted-session filesystem mutations, session-ref promotion through commit, durable VCS metadata, durable status/diff/revert, bounded recovery scheduling, and bounded admin destructive cleanup controls for eligible CAS-lost final objects. The broad durable-cloud path currently covers FS/search/tree reads, mounted-session filesystem mutations, VCS read/mutation surfaces, protected-rule routes, and change-request mutation routes behind explicit readiness, repo, idempotency, Postgres posture, and R2 posture gates. `stratumctl` can target this router as an HTTP client with explicit repo context, while direct local MCP/FUSE/REPL callers fail closed under durable-cloud instead of opening local state. Durable-cloud auth login, workspace management, run records, audit reads, semantic search, execution, VCS recovery operator routes under durable-cloud, remote durable MCP/FUSE serving, production hosted rollout, durable FUSE mutation persistence, a general-purpose Redis-backed lock service, broad unreachable commit/object deletion, and live-provider-verified production Postgres/R2 HTTP write execution remain future work.

The durable backend foundation now defines Rust contracts for future object storage, commit metadata, ref compare-and-swap, idempotency, audit, workspace metadata, and review stores. Its Postgres migration catalog is executable through a rollback-only smoke harness and dedicated CI Postgres service-container jobs.

The backend adapter scaffolding adds a byte-backed object adapter over the existing local/R2 byte-store abstraction using repo-scoped, kind-scoped object keys. This adapter is now used by explicitly gated durable HTTP paths, including the guarded durable capability and durable-cloud router.

The object adapter now stages uploads before converging on final immutable object keys, uses conditional create-if-absent semantics for final object bytes, and exposes cleanup helpers for old staged uploads plus dry-run detection for old final object keys that have no metadata record. It also has a claim-backed repair helper that can recreate missing object metadata from verified final bytes. That repair helper now has in-memory coverage plus live Postgres-backed conformance coverage using durable metadata rows and cleanup-claim leases over local byte-store bytes. Store-backed final-object metadata fences now block concurrent metadata repair while cleanup readiness is being evaluated. Destructive deletion exists only behind an explicit worker deletion mode and is limited to cleanup-claim-owned CAS-lost final objects after a persisted hold window, repeated reachability proof, matching metadata snapshot, active cleanup claim, and active metadata fence.

An optional `postgres` feature now exposes a Postgres metadata adapter for object metadata, object cleanup claims, commit metadata, and ref compare-and-swap contract tests. The same adapter test surface now proves final-object metadata repair with Postgres metadata and cleanup claims. The object/commit/ref adapters are wired only through explicit durable gates; full production HTTP write execution remains future work.

The same optional feature also exposes a Postgres-backed `IdempotencyStore` over the `idempotency_records` table. Rows store only hashed idempotency keys (`key_hash`), not raw `Idempotency-Key` header values, and the schema constrains both `key_hash` and `request_fingerprint` to lowercase SHA-256 digest shape. The store persists replay classification, quota identity, timestamps, and response byte counts; rejects generic `secret_bearing` completion; and has a narrow encrypted secret replay completion path used by workspace-token issuance. Secret replay rows must contain encrypted envelope JSON and matching metadata columns, never raw token response JSON. The store supports stale-pending takeover/abort and bounded retention sweeps. `STRATUM_BACKEND=durable` uses this store for supported HTTP idempotency keys when the server is built with the `postgres` feature. A recovery/GC-safe idempotency sweep helper retains records that unresolved recovery, active cleanup claims, reachable refs/workspaces/reviews, or live commit roots still require. The helper is bounded and redacted, but it is not yet scheduled as an automatic background retention worker.

The optional `postgres` feature also includes a Postgres-backed `AuditStore` over `audit_events`. It stores sanitized audit event actor/workspace/resource/details JSON and allocates global sequences with a transaction-scoped advisory lock. Exact VCS commit/revert audit appends are idempotent by action and commit resource id, so a post-CAS recovery claim that is replaced after lease expiry cannot create duplicate audit rows when a stale worker resumes. `STRATUM_BACKEND=durable` uses this store for current mutation audit events and route policy decision allow/deny events. Policy audit details are bounded and content-free: they record action codes, allow/deny state, redacted reason codes, target ref, changed-path and matched-rule counts, change-request ids when applicable, and idempotency/request presence flags rather than raw idempotency keys, request bodies, commit messages, file content, tokens, or database URLs. Read/auth audit coverage is still not expanded.

The optional `postgres` feature also includes a Postgres-backed `WorkspaceMetadataStore` over `workspaces` and `workspace_tokens`. It stores global workspace rows with `repo_id IS NULL`, preserves base/session refs and head-version updates, and persists only workspace-token secret hashes with normalized read/write prefixes. `STRATUM_BACKEND=durable` uses this store for hosted workspace endpoints. Workspace-token issuance can be idempotent only when secret replay KMS is configured; token rotation, expiry, revocation, and hosted secret-management behavior remain future work.

The optional `postgres` feature also includes a Postgres-backed `ReviewStore` over protected ref rules, protected path rules, change requests, approvals, reviewer assignments, and review comments. `STRATUM_BACKEND=durable` uses this store for review/protected-change endpoints. Rows are still under `RepoId::local()` because the current review trait is not repo-aware. Change-request base/head commit IDs remain lowercase SHA-256-shaped strings and are not foreign-keyed to the durable Postgres commit catalog. Under the guarded durable commit route, review creation and merge can still resolve durable refs and durable commit metadata for the selected repo; otherwise review routes preserve the existing local `.vfs/state.bin` behavior.

Workspace-token revocation still rejects idempotency keys. Workspace-token issuance accepts `Idempotency-Key` only through the encrypted secret replay path described above.

An opt-in R2 object-store integration gate now exercises live-compatible byte round trips and backend object adapter composition when credentials are explicitly supplied. Default CI only checks that the gate skips cleanly without secrets.

An optional Rust Postgres migration runner foundation now tracks ordered migrations in `stratum_schema_migrations`, reports pending/applied/dirty/mismatched state, serializes apply and adoption attempts with a schema-scoped advisory lock, and refuses dirty, unknown, mismatched, partial, or unverifiable state. Migration smoke checks remain explicit through `scripts/check-postgres-migrations.sh`; durable `stratum-server` startup can inspect, apply, or explicitly adopt verified legacy migrations with the `postgres` feature before opening Postgres control-plane stores.

These foundations do not yet enable production hosted S3/R2 runtime rollout, a general-purpose Redis-backed lock service, production destructive cleanup automation, multipart upload, signed URLs, lifecycle policy automation, broad unreachable object/commit deletion, cross-store transactions, or a full server runtime cutover for core filesystem/VCS metadata.

## Health Check

```bash
curl http://localhost:3000/health
```

Response:

```json
{
  "status": "ok",
  "version": "1.0.0",
  "commits": 3,
  "inodes": 47,
  "objects": 12,
  "readiness": {
    "db": {
      "local_core_required": true,
      "local_core_opened": true,
      "control_plane_opened": true
    },
    "object_store": {
      "durable_configured": false,
      "startup_checked": false
    },
    "recovery_stores": {
      "configured": false,
      "startup_opened": false
    }
  }
}
```

In durable-cloud mode, `commits`, `inodes`, and `objects` are `null` because the local core database is intentionally not opened. The readiness block switches `local_core_required` and `local_core_opened` to `false`, while durable object-store and recovery-store startup booleans are `true` after startup has opened and checked the hosted stores. These fields are startup/configuration signals, not a fresh live probe on every `/health` request.

## Login

Verify a user exists and get their identity:

```bash
curl -X POST http://localhost:3000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "alice"}'
```

Response:

```json
{
  "username": "alice",
  "uid": 1,
  "gid": 2,
  "groups": ["alice", "wheel"]
}
```

## Hosted Workspaces

Hosted workspace management endpoints require an admin (`root` or `wheel`) auth header. Records and workspace-token hashes are stored in `<STRATUM_DATA_DIR>/.vfs/workspaces.bin` by default, or `STRATUM_WORKSPACE_METADATA_PATH` when set. In `STRATUM_BACKEND=durable` with the `postgres` feature, these records are stored in Postgres instead.

### List Workspaces

```bash
curl http://localhost:3000/workspaces \
  -H "Authorization: User root"
```

### Create A Workspace

```bash
curl -X POST http://localhost:3000/workspaces \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"incident-demo",
    "root_path":"/incidents/checkout-latency",
    "base_ref":"main",
    "session_ref":"agent/legal-bot/session-123"
  }'
```

`base_ref` and `session_ref` are optional. `base_ref` defaults to `main`; `session_ref` defaults to `null`. When supplied, both must use Stratum's VCS ref namespaces such as `main`, `agent/<actor>/<session>`, `review/<id>`, or `archive/<id>`.

`Idempotency-Key` is optional for workspace creation. Same-key retries replay the original `201 Created` workspace JSON and do not create another workspace record.

### Issue A Workspace Token

```bash
curl -X POST http://localhost:3000/workspaces/<workspace-id>/tokens \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"ci-token",
    "agent_token":"<existing-agent-token>",
    "read_prefixes":["/incidents/checkout-latency/read"],
    "write_prefixes":["/incidents/checkout-latency/work"]
  }'
```

The `agent_token` is validated against the Stratum user registry before a workspace token is issued. `read_prefixes` and `write_prefixes` are optional; when omitted, each defaults to the workspace root path. When supplied, every prefix must normalize under the workspace root. An explicit empty array is allowed and denies all paths for that access class.

Response:

```json
{
  "workspace_id": "<workspace-id>",
  "token_id": "<token-id>",
  "name": "ci-token",
  "workspace_token": "<new-workspace-secret>",
  "agent_uid": 7,
  "read_prefixes": ["/incidents/checkout-latency/read"],
  "write_prefixes": ["/incidents/checkout-latency/work"],
  "base_ref": "main",
  "session_ref": "agent/legal-bot/session-123"
}
```

The response includes the new `workspace_token` secret, authenticated `agent_uid`, and workspace ref ownership; it does not echo the raw agent token.

`Idempotency-Key` is optional for workspace-token issuance when secret replay KMS is configured. Stratum authenticates the admin and backing agent token, resolves the repo/workspace, normalizes read/write prefixes, then fingerprints the route, admin actor, repo id, workspace id, token name, authenticated agent UID, and normalized prefixes. The raw `agent_token` and issued `workspace_token` are not part of the fingerprint. Same-key retries with the same fingerprint decrypt the stored encrypted replay envelope and return the same `workspace_token`, `token_id`, and `X-Stratum-Idempotent-Replay: true`. Same-key retries with a different request return `409 Conflict`, and duplicate in-progress requests return `409 Conflict` without issuing another token.

If secret replay KMS is not configured, token issuance without `Idempotency-Key` preserves the normal one-time-token behavior, while token issuance with `Idempotency-Key` fails closed before issuing a token. If encrypted replay persistence fails after a token has been created, the response is a fixed redacted failure body and matching retries do not mint a duplicate token under the same key.

Use the returned secret with:

```bash
curl http://localhost:3000/fs/read \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>"
```

With `X-Stratum-Workspace`, `/fs`, `/tree`, and omitted search paths refer to the workspace root. Paths in filesystem/search/tree responses are projected back to workspace-relative paths such as `/read/runbook.md`.

## Audit Events

`GET /audit` returns a bounded recent list of audit events for successful or partial mutations. The default local backend reads from the local audit store; `STRATUM_BACKEND=durable` with the `postgres` feature reads from Postgres. It requires an admin-equivalent `Authorization: User ...` session (`root` or `wheel`). Bearer tokens are forbidden, including global agent tokens and workspace tokens, even when the underlying agent is privileged.

```bash
curl "http://localhost:3000/audit?limit=50" \
  -H "Authorization: User root"
```

`limit` is optional, defaults to `100`, and is capped at `1000`.

Response:

```json
{
  "events": [
    {
      "id": "2d4a2f2d-2f08-43e7-99aa-1b5aa77d51b9",
      "sequence": 1,
      "timestamp": "2026-05-01T14:20:00Z",
      "actor": {
        "uid": 0,
        "username": "root",
        "delegate": null
      },
      "workspace": null,
      "action": "workspace_create",
      "resource": {
        "kind": "workspace",
        "id": "5a4d6d69-84b2-4ebd-8c06-97c25547e4e5",
        "path": "/incidents/checkout-latency"
      },
      "outcome": "success",
      "details": {
        "name": "incident-demo",
        "root_path": "/incidents/checkout-latency",
        "base_ref": "main"
      }
    }
  ]
}
```

Audit events include server-assigned `id`, `sequence`, and `timestamp`; actor UID/username plus an optional delegate; optional mounted workspace context; `action`; `resource` kind/id/path; `outcome`; and a small string-keyed `details` map. Current audited actions cover successful filesystem write, mkdir, delete, copy, metadata update, and move operations; VCS commit, revert, ref create, and ref update operations; route policy decision allow/deny events; idempotency quota failures; protected-rule, change-request, approval, reviewer-assignment, review-comment, approval-dismissal, reject, and merge operations; workspace creation and workspace-token issuance; and run-record creation.

Audit details are intentionally metadata-only. They must not contain file contents, raw tokens, request bodies, raw idempotency keys, run prompt/command/stdout/stderr/result content, or commit messages. Guarded durable filesystem mutation audit includes content-free recovery identity so normal route audit and recovery audit can deduplicate the same mutation: operation id, target ref, previous commit, new commit, and changed-path count.

## Run Records

Run records are durable execution artifacts written into the mounted workspace under `/runs/<run-id>/`. This foundation endpoint records a prompt, command, captured output, result text, metadata, and an artifacts directory. It does not execute commands or schedule jobs.

`POST /runs` requires a workspace bearer token plus `X-Stratum-Workspace`; global bearer tokens and `Authorization: User ...` sessions are rejected. The workspace token must have write scope for the backing workspace `/runs` path.

```bash
curl -X POST http://localhost:3000/runs \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "run_id": "run_123",
    "prompt": "Summarize the checkout incident",
    "command": "cargo test --locked",
    "stdout": "",
    "stderr": "",
    "result": "created",
    "status": "succeeded",
    "exit_code": 0,
    "source_commit": "abc123",
    "started_at": "2026-04-30T12:01:00Z",
    "ended_at": "2026-04-30T12:02:00Z"
  }'
```

`run_id` is optional; when omitted, Stratum generates a UUID-based ID. Supplied IDs may contain only ASCII letters, digits, `_`, and `-`. Duplicate run IDs are rejected with `409 Conflict` to preserve existing run records. `stdout`, `stderr`, and `result` are optional and default to empty strings. `status` is optional and defaults to `queued`; accepted values are `queued`, `running`, `succeeded`, `failed`, `cancelled`, and `timed_out`. `exit_code`, `source_commit`, `started_at`, and `ended_at` are optional metadata fields.

Response:

```json
{
  "run_id": "run_123",
  "root": "/runs/run_123",
  "artifacts": "/runs/run_123/artifacts/",
  "files": {
    "prompt": "/runs/run_123/prompt.md",
    "command": "/runs/run_123/command.md",
    "stdout": "/runs/run_123/stdout.md",
    "stderr": "/runs/run_123/stderr.md",
    "result": "/runs/run_123/result.md",
    "metadata": "/runs/run_123/metadata.md"
  }
}
```

`Idempotency-Key` is optional. Stratum fingerprints the `POST /runs` namespace, workspace ID, authenticated agent UID, and normalized JSON request body. Same-key retries replay the original completed `201 Created` JSON response and do not create another run directory. Idempotency replay/conflict checks still require the current workspace bearer token to have run write scope. Duplicate `run_id` values without a matching idempotency replay still return `409 Conflict`.

All response paths are workspace-relative. The backing workspace root path is not returned in success, replay, or projected error messages. Phase 1 writes are not transactional across all run files: if a database write fails after the run root is created, the error response includes `"partial": true`, `run_id`, and the workspace-relative run root.

### Read A Run Record

Run read endpoints require the same workspace bearer auth shape as creation. The workspace token must have read scope for the backing workspace `/runs/<run-id>` path.

```bash
curl http://localhost:3000/runs/run_123 \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>"
```

Response:

```json
{
  "run_id": "run_123",
  "root": "/runs/run_123",
  "artifacts": "/runs/run_123/artifacts/",
  "files": {
    "prompt": {
      "path": "/runs/run_123/prompt.md",
      "kind": "file",
      "size": 31,
      "modified": 1777580000,
      "encoding": "utf-8",
      "content_preview": "Summarize the checkout incident",
      "content_truncated": false
    },
    "command": {"path": "/runs/run_123/command.md", "kind": "file", "size": 19, "modified": 1777580000, "encoding": "utf-8", "content_preview": "cargo test --locked", "content_truncated": false},
    "stdout": {"path": "/runs/run_123/stdout.md", "kind": "file", "size": 2, "modified": 1777580000, "encoding": "utf-8", "content_preview": "ok", "content_truncated": false},
    "stderr": {"path": "/runs/run_123/stderr.md", "kind": "file", "size": 0, "modified": 1777580000, "encoding": "utf-8", "content_preview": "", "content_truncated": false},
    "result": {"path": "/runs/run_123/result.md", "kind": "file", "size": 9, "modified": 1777580000, "encoding": "utf-8", "content_preview": "completed", "content_truncated": false},
    "metadata": {"path": "/runs/run_123/metadata.md", "kind": "file", "size": 240, "modified": 1777580000, "encoding": "utf-8", "content_preview": "---\nrun_id: \"run_123\"\nstatus: \"succeeded\"\n---\n", "content_truncated": false}
  }
}
```

`content_preview` is bounded to 4096 bytes. If a run file is not valid UTF-8, `encoding` is `binary`, `content_preview` is `null`, and the raw bytes should be read through the file API or the dedicated stdout/stderr endpoints.

For raw captured output:

```bash
curl http://localhost:3000/runs/run_123/stdout \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>"

curl http://localhost:3000/runs/run_123/stderr \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>"
```

Raw output endpoints also require read scope on the backing workspace `/runs/<run-id>` root, not only the individual output file. Missing run IDs return `404`. Unsafe run IDs return `400`. Read-scope failures return `403`.

## Filesystem Operations

### Read a File

```bash
curl http://localhost:3000/fs/docs/readme.md \
  -H "Authorization: User alice"
```

Response: raw file content. `Content-Type` is the file's stored MIME type when set, otherwise `application/octet-stream`.

```
# My Project

Welcome to the docs.
```

### List a Directory

```bash
curl http://localhost:3000/fs/docs/ \
  -H "Authorization: User alice"
```

Response:

```json
{
  "path": "/docs",
  "entries": [
    {"name": "api.md", "kind": "file"},
    {"name": "readme.md", "kind": "file"},
    {"name": "specs", "kind": "directory"}
  ]
}
```

### Get File Metadata (stat)

```bash
curl "http://localhost:3000/fs/docs/readme.md?stat=true" \
  -H "Authorization: User alice"
```

Response:

```json
{
  "inode_id": 5,
  "kind": "file",
  "size": 42,
  "mode": "0644",
  "uid": 1,
  "gid": 2,
  "created": 1713000600,
  "modified": 1713001275,
  "mime_type": "text/markdown",
  "content_hash": "sha256:3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7",
  "custom_attrs": {
    "owner": "docs"
  }
}
```

`content_hash` is computed from current file bytes at stat time and is `null` for directories and symlinks. `mime_type` is user-provided metadata, not content sniffing.

### Write a File

```bash
curl -X PUT http://localhost:3000/fs/docs/readme.md \
  -H "Authorization: User alice" \
  -H "Idempotency-Key: <retry-key>" \
  -H "X-Stratum-Mime-Type: text/markdown" \
  -d "# Updated Readme

New content here."
```

Response:

```json
{
  "written": "docs/readme.md",
  "size": 33
}
```

The file is created automatically if it doesn't exist (including parent directories for the path).

When `X-Stratum-Mime-Type` is provided, Stratum stores it as file metadata after the content write. Existing file MIME metadata is preserved when the header is omitted.

### Update File Metadata

```bash
curl -X PATCH http://localhost:3000/fs/docs/readme.md \
  -H "Authorization: User alice" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "mime_type": "text/markdown",
    "custom_attrs": {"owner": "docs", "reviewed": "true"},
    "remove_custom_attrs": ["old-key"]
  }'
```

Response:

```json
{
  "metadata_updated": "/docs/readme.md",
  "changed": true,
  "mime_type": "text/markdown",
  "custom_attr_keys": ["owner", "reviewed"],
  "custom_attrs_set": ["owner", "reviewed"],
  "custom_attrs_removed": ["old-key"]
}
```

`PATCH /fs/{path}` requires write access to the existing path and does not create files. `mime_type: null` clears MIME metadata. Custom attribute keys and values are bounded; values are not included in the PATCH response or recorded in audit events. Read current values with `GET /fs/{path}?stat=true`.

Filesystem write, metadata update, directory creation, delete, copy, and move endpoints accept optional `Idempotency-Key`. Same-key retries replay the original JSON response without appending another mutation audit event.

If an active protected path-prefix rule matches a touched backing path, direct HTTP filesystem mutations return `403 Forbidden` before idempotency reservation or replay. The check runs after authentication and workspace mount path resolution, so rules are evaluated against backing paths rather than projected response paths. File writes and metadata patches also check the final symlink target they would mutate. File writes, directory creates, metadata patches, deletes, copy destinations, and both move source and destination paths are protected. Deletes and move sources also block ancestor paths that would remove a protected descendant. Copy source reads are not blocked by protected path rules. Prefix matching is boundary-aware: `/legal` protects `/legal` and `/legal/draft.txt`, not `/legalese`.

### Create a Directory

```bash
curl -X PUT http://localhost:3000/fs/docs/specs/v2 \
  -H "Authorization: User alice" \
  -H "X-Stratum-Type: directory"
```

Response:

```json
{
  "created": "docs/specs/v2",
  "type": "directory"
}
```

Parent directories are created automatically (`mkdir -p` behavior).

### Delete a File

```bash
curl -X DELETE http://localhost:3000/fs/docs/old-notes.md \
  -H "Authorization: User alice"
```

Response:

```json
{
  "deleted": "docs/old-notes.md"
}
```

### Delete a Directory (Recursive)

```bash
curl -X DELETE "http://localhost:3000/fs/docs/old-stuff?recursive=true" \
  -H "Authorization: User alice"
```

Response:

```json
{
  "deleted": "docs/old-stuff"
}
```

### Copy a File

```bash
curl -X POST "http://localhost:3000/fs/docs/readme.md?op=copy&dst=archive/readme.md" \
  -H "Authorization: User alice"
```

Response:

```json
{
  "copied": "docs/readme.md",
  "to": "archive/readme.md"
}
```

### Move / Rename a File

```bash
curl -X POST "http://localhost:3000/fs/docs/draft.md?op=move&dst=docs/final.md" \
  -H "Authorization: User alice"
```

Response:

```json
{
  "moved": "docs/draft.md",
  "to": "docs/final.md"
}
```

## Search

### grep — Search File Contents

```bash
curl "http://localhost:3000/search/grep?pattern=TODO&path=docs&recursive=true" \
  -H "Authorization: User alice"
```

Response:

```json
{
  "results": [
    {"file": "docs/api.md", "line_num": 3, "line": "TODO: document endpoints"},
    {"file": "docs/api.md", "line_num": 7, "line": "TODO: add examples"}
  ],
  "count": 2
}
```

Parameters:
- `pattern` (required) — regex pattern to search for
- `path` (optional) — directory or file to search in
- `recursive` (optional) — `true` to search subdirectories

### find — Find Files by Name

```bash
curl "http://localhost:3000/search/find?path=.&name=*.md" \
  -H "Authorization: User alice"
```

Response:

```json
{
  "results": [
    "docs/api.md",
    "docs/readme.md",
    "notes/todo.md"
  ],
  "count": 3
}
```

### tree — Directory Tree

```bash
curl http://localhost:3000/tree/docs \
  -H "Authorization: User alice"
```

Response: plain text tree view.

```
docs/
├── api.md
├── readme.md
└── specs/
    ├── auth.md
    └── design.md
```

## Version Control

Global VCS endpoints require an admin-equivalent session.

### Commit

```bash
curl -X POST http://localhost:3000/vcs/commit \
  -H "Content-Type: application/json" \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -d '{"message": "add API documentation"}'
```

Response:

```json
{
  "hash": "a1b2c3d4",
  "message": "add API documentation",
  "author": "root"
}
```

Commit, revert, ref-create, and ref-update endpoints accept optional `Idempotency-Key`. This is especially useful for compare-and-swap ref updates: a retry after a successful first request replays the original updated ref instead of failing as a stale CAS attempt.

When `STRATUM_DURABLE_COMMIT_ROUTE=1` is enabled with `STRATUM_BACKEND=durable` in a `postgres` build, guarded commit and revert execute through durable object/commit/ref stores. Guarded commit promotes a durable mounted session tree when one is present; otherwise it still uses the local filesystem snapshot as the source tree until broad durable mutable workspace routing exists. Guarded revert restores `main` to a durable target commit root by creating a new durable revert commit and advancing `main` with source-checked ref CAS. After `main` is visibly updated, workspace-head/audit/idempotency failures return a redacted `202 Accepted` partial response that is safe to replay. Failures before confirmed ref visibility do not record a committed replay response.

Admin operators can inspect and drain guarded durable recovery work with:

```bash
curl http://localhost:3000/vcs/recovery \
  -H "Authorization: User root"

curl -X POST http://localhost:3000/vcs/recovery/run \
  -H "Content-Type: application/json" \
  -H "Authorization: User root" \
  -d '{"limit": 10}'
```

`GET /vcs/recovery` is bounded and redacted. It preserves the legacy `recovery`, `pre_visibility`, and `fs_mutations` arrays, and also returns an operator-ready shape:

```json
{
  "health": {
    "status": "degraded",
    "backend_mode": "durable",
    "guarded_durable_enabled": true,
    "scheduler": {
      "present": true,
      "enabled": true,
      "state": "running",
      "interval_millis": 5000,
      "tick_limit": 10,
      "lease_millis": 30000,
      "shutdown_drain_enabled": false,
      "shutdown_drain_timeout_millis": 2500,
      "started_at_millis": 1778371200000,
      "last_tick_at_millis": 1778371205000,
      "last_tick_started_at_millis": 1778371205000,
      "last_tick_completed_at_millis": 1778371205012,
      "last_tick_duration_millis": 12,
      "last_outcome": "completed",
      "last_error": null,
      "phases": {
        "pre_visibility": { "attempted": 0, "completed": 0 },
        "post_cas": { "attempted": 0, "completed": 0 },
        "fs_mutations": { "attempted": 0, "completed": 0 },
        "object_cleanup": { "attempted": 0, "completed": 0 }
      },
      "shutdown_drain": null
    },
    "stores": {
      "post_cas": { "available": true },
      "pre_visibility": { "available": true },
      "fs_mutations": { "available": true },
      "object_cleanup": { "available": true }
    }
  },
  "phases": {
    "pre_visibility": { "count": 0, "page_count": 0, "rows": [] },
    "post_cas": { "count": 1, "page_count": 1, "rows": [] },
    "fs_mutations": { "count": 0, "page_count": 0, "rows": [] },
    "object_cleanup": {
      "count": 2,
      "page_count": 2,
      "deletion_enabled": false,
      "deletion_ready": 0,
      "deletion_held": 1,
      "deleted_final_objects": 0,
      "deferred": 1,
      "poisoned": 0,
      "remaining": 2,
      "gc_dry_run": {
        "available": true,
        "mode": "dry_run",
        "deletion_enabled": false,
        "unreachable_commit_count": 1,
        "unreachable_object_count": 1,
        "blockers": []
      },
      "rows": []
    }
  },
  "blockers": {
    "refs": [
      {
        "repo_id": "local",
        "ref_name": "main",
        "blocked": true,
        "reason": "poisoned_recovery"
      }
    ],
    "workspaces": []
  },
  "limit": 100
}
```

Rows include age/readiness fields such as `age_millis`, `created_at_millis`, `updated_at_millis`, `stale_active`, `due`, `retryable`, `stuck_tier`, and `next_retry_at_millis` where that phase has the backing timestamps. Phase summaries include `due_count`, `stale_active_count`, and either `poisoned_count` or `failed_count`. Cleanup rows also include `is_stale`, `has_last_failure`, `deletion_ready`, `deletion_held`, and held `delete_after_millis` when applicable; they identify objects by repo, object kind, and short object ID, not by canonical object key. The nested `gc_dry_run` reports bounded unreachable commit/object candidates using short IDs and redacted blockers. `GET /vcs/recovery` and default `POST /vcs/recovery/run` responses report `object_cleanup.deletion_enabled: false`; only a bounded, authorized operator run that explicitly sets `destructive_final_object_deletion: true` can report it as `true`.

The background recovery scheduler is explicitly configurable when guarded durable stores or durable-cloud stores are attached. `STRATUM_RECOVERY_SCHEDULER=enabled|disabled` defaults to `enabled`; disabled mode still attaches a status handle, reports `enabled: false` and `state: "disabled"`, and does not spawn the background loop. `STRATUM_RECOVERY_SCHEDULER_INTERVAL_MS` defaults to `5000`, `STRATUM_RECOVERY_SCHEDULER_TICK_LIMIT` defaults to `10` and caps at `100`, and `STRATUM_RECOVERY_SCHEDULER_LEASE_MS` defaults to `30000`. The scheduler uses persisted claim owner/token/expiry fencing for multi-node safety rather than a process-local or distributed lock, so duplicate workers race through the same durable recovery stores and complete idempotently.

Shutdown drain is opt-in with `STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN=enabled|disabled`, defaulting to `disabled`. When enabled, `stratum-server` asks the scheduler to stop accepting background ticks, marks `state: "draining"`, runs bounded immediate ticks until no due work is attempted or `STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS` expires, then records a redacted `shutdown_drain` result and leaves process shutdown bounded. The timeout defaults to `2500` milliseconds and caps at `30000`; outcomes are fixed status markers such as `completed`, `partial_failure`, `failed`, `timed_out`, `drain_failed`, or `skipped_disabled`, not raw backend errors.

`POST /vcs/recovery/run` is bounded and redacted. It first classifies due pre-visibility rows, proving `main` visibility directly or through a bounded parent walk before enqueueing contextual post-CAS repair, or safely aborting the original idempotency reservation when the commit is not visible. It then drains remaining post-CAS, durable FS mutation, and object cleanup readiness work within the caller-supplied limit, preserving workspace-head fencing, audit append idempotence, explicit full-vs-partial idempotency replay kind, and final-object metadata fences. Durable revert recovery records replay the durable revert response shape rather than the generic commit response. The object cleanup phase remains non-destructive and reports `deleted_final_objects: 0` while deletion is disabled; a bounded admin request can explicitly enable eligible CAS-lost final-object deletion with `destructive_final_object_deletion: true`. The route returns a redacted correlation ID in the body and `X-Stratum-Recovery-Correlation-Id`, plus remaining work by phase:

```json
{
  "correlation_id": "rec_018f8d4c8d754a8f9f3d9b4f5ad1f4c2",
  "requested_limit": 10,
  "attempted": 3,
  "completed": 2,
  "backing_off": 1,
  "poisoned": 0,
  "skipped": 0,
  "remaining": 4,
  "converged": false,
  "message": "bounded recovery run completed with persisted work remaining",
  "phases": {
    "pre_visibility": { "attempted": 1, "completed": 1, "remaining": 0 },
    "post_cas": { "attempted": 1, "completed": 0, "remaining": 1 },
    "fs_mutations": { "attempted": 1, "completed": 1, "remaining": 0 },
    "object_cleanup": {
      "listed": 1,
      "processed": 1,
      "completed": 0,
      "deleted_final_objects": 0,
      "deletion_ready": 1,
      "deletion_held": 0,
      "deletion_enabled": false,
      "retryable_failures": 0,
      "poisoned": 0,
      "deferred": 0,
      "remaining": 3
    }
  }
}
```

Operator guidance: `pending` means queued; watch age and scheduler progress. `backing_off` means retry is delayed until `next_retry_at_millis` unless an operator runs recovery after fixing the dependency. `poisoned` means automatic retry is stopped and manual investigation is required; max-attempt cleanup rows are sorted behind claimable work so they do not consume the bounded worker first. `stale_active` means a previous worker lease expired and the row is retryable by the scheduler or a manual bounded run. `deletion_ready` means the worker proved an unreachable CAS-lost object under a current cleanup claim, matching final-object metadata, and final-object metadata fence, then persisted readiness and a hold deadline. Cleanup claims and `deletion_ready` are not deletion completion. Destructive mode is default-off and exposed only through the explicit bounded admin `POST /vcs/recovery/run` request fields; broad unreachable commit/object deletion remains protocol-visible only.

Manual `POST /vcs/recovery/run` remains available when the background scheduler is disabled, subject to the same guarded durable route availability and admin authorization. Durable-cloud recovery operator routes remain fail-closed unless explicitly mounted; unsupported calls return the stable durable-cloud unsupported `501` body rather than falling back to local state. Status and run responses must stay redacted: no database URLs, R2 endpoints, bucket names, object keys, raw backend/provider errors, SQL text, request bodies, idempotency keys, tokens, commit messages, or local filesystem paths.

Active exact protected ref rules block direct `POST /vcs/commit`, `POST /vcs/revert`, and `PATCH /vcs/refs/{name}` with `403 Forbidden`. Commit and revert target `main`; ref update targets the named ref. Direct revert is also blocked when the rollback would touch an active protected path rule that applies to `main`. Protection is checked after authentication and ref/path resolution but before idempotency reservation or replay, so an older idempotency key cannot bypass a newly added protected rule. Change-request merge is the allowed fast-forward path for updating protected target refs and paths.

### View Commit History

```bash
curl http://localhost:3000/vcs/log \
  -H "Authorization: User root"
```

Response:

```json
{
  "commits": [
    {
      "hash": "a1b2c3d4",
      "message": "add API documentation",
      "author": "alice",
      "timestamp": 1713005100
    },
    {
      "hash": "e5f6a7b8",
      "message": "initial setup",
      "author": "alice",
      "timestamp": 1713000600
    }
  ]
}
```

### Manage Refs

Refs are named pointers to full 64-character commit IDs. Session refs use the `agent/<actor>/<session>` namespace; review and archive refs use `review/<id>` and `archive/<id>`.

List refs:

```bash
curl http://localhost:3000/vcs/refs \
  -H "Authorization: User root"
```

Response:

```json
{
  "refs": [
    {
      "name": "main",
      "target": "<64-char-commit-id>",
      "version": 2
    },
    {
      "name": "agent/legal-bot/session-123",
      "target": "<64-char-commit-id>",
      "version": 1
    }
  ]
}
```

Create a session ref:

```bash
curl -X POST http://localhost:3000/vcs/refs \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "agent/legal-bot/session-123",
    "target": "<64-char-commit-id>"
  }'
```

Response: `201 Created`

```json
{
  "name": "agent/legal-bot/session-123",
  "target": "<64-char-commit-id>",
  "version": 1
}
```

Update a ref with compare-and-swap protection:

```bash
curl -X PATCH http://localhost:3000/vcs/refs/agent/legal-bot/session-123 \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{
    "target": "<new-64-char-commit-id>",
    "expected_target": "<current-64-char-commit-id>",
    "expected_version": 1
  }'
```

Response:

```json
{
  "name": "agent/legal-bot/session-123",
  "target": "<new-64-char-commit-id>",
  "version": 2
}
```

Duplicate ref creation and stale compare-and-swap updates return `409 Conflict` and leave the existing ref unchanged. Unknown target commits return `400 Bad Request` after the compare-and-swap expectation has been satisfied.

### Protected Rules, Change Requests, Approvals, And Feedback

The review-control foundation is admin-gated and uses the local review store by default; `STRATUM_BACKEND=durable` with the `postgres` feature stores review/protected-change rows in Postgres. It defines protected ref rules, protected path-prefix rules, fast-forward-only change requests, reviewer assignments, review comments, approval records, approval dismissal, and computed approval state. Mutating FS, VCS, and review routes evaluate protected ref/path decisions through a shared route policy seam and emit content-free policy allow/deny audit events before mutation. This is still a foundation: it does not include reviewer groups, threaded comments, merge queues, or a web review UI.

Create a protected ref rule:

```bash
curl -X POST http://localhost:3000/protected/refs \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "ref_name": "main",
    "required_approvals": 1,
    "require_all_files_viewed": true
  }'
```

List protected ref rules:

```bash
curl http://localhost:3000/protected/refs \
  -H "Authorization: User root"
```

Create a protected path-prefix rule:

```bash
curl -X POST http://localhost:3000/protected/paths \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{
    "path_prefix": "/legal",
    "target_ref": "main",
    "required_approvals": 2,
    "require_all_files_viewed": false
  }'
```

`target_ref` is optional. `require_all_files_viewed` is optional on protected ref and path rules, defaults to `true`, and is returned by create/list APIs. This slice only persists and advertises the policy flag; it does not add file-view tracking or backend approval enforcement for that flag. Path prefixes are absolute, normalized boundaries. Direct filesystem enforcement evaluates these rules against resolved backing paths after workspace mount resolution; client responses still use projected paths and do not expose backing workspace paths beyond existing route behavior.

Create a change request:

```bash
curl -X POST http://localhost:3000/change-requests \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Promote legal review",
    "description": "Ready for review",
    "source_ref": "review/cr-1",
    "target_ref": "main"
  }'
```

The server validates both refs exist, captures the current target-ref commit as `base_commit`, captures the current source-ref commit as `head_commit`, and creates an `open` change request. When the guarded durable commit route is enabled and both refs exist in durable stores, this snapshot uses durable ref metadata; otherwise it uses the local VCS refs. A request is not partially mixed between local and durable source/target refs. Change-request create/read/list/reject responses use this shape:

```json
{
  "change_request": {
    "id": "<change-request-id>",
    "title": "Promote legal review",
    "source_ref": "review/cr-1",
    "target_ref": "main",
    "base_commit": "<64-char-commit-id>",
    "head_commit": "<64-char-commit-id>",
    "status": "open",
    "created_by": 0,
    "version": 1
  },
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": [],
    "require_all_files_viewed": true
  },
  "require_all_files_viewed": true
}
```

`approval_state` is computed from active protected ref rules matching the target ref, active protected path rules matching changed paths between `base_commit` and `head_commit`, and active required reviewer assignments. In guarded durable mode, a durable change request whose source/target refs exist in durable stores computes those changed paths by walking durable commit parent metadata from `head_commit` back to `base_commit` and collecting recorded changed-path names; local change requests keep using the local VCS ancestry calculation. The effective required approval count is the maximum `required_approvals` from matching rules. `approved` is true only when the numeric approval count is satisfied and every required reviewer has an active approval for the captured `head_commit`. The top-level `require_all_files_viewed` response field is the resolved CR-level policy value for the matched rules, defaulting fail-closed to `true` if approval-state resolution is unavailable; it is not enforced by this approval-state computation yet.

Read and list change requests:

```bash
curl http://localhost:3000/change-requests \
  -H "Authorization: User root"

curl http://localhost:3000/change-requests/<change-request-id> \
  -H "Authorization: User root"
```

Approve a change request:

```bash
curl -X POST http://localhost:3000/change-requests/<change-request-id>/approvals \
  -H "Authorization: User alice" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "comment": "Looks good"
  }'
```

List approvals:

```bash
curl http://localhost:3000/change-requests/<change-request-id>/approvals \
  -H "Authorization: User root"
```

Approval creation returns `201 Created` for a new approval and `200 OK` with `"created": false` when the same approver has already approved the same change request at the same `head_commit`. Approval records are bound to the captured `head_commit`; stale approval heads, self-approval by the change-request author, and new approvals on merged or rejected change requests are rejected. Approval comments are stored on approval records and returned by approval read APIs, but audit details omit comment text.

Assign a reviewer:

```bash
curl -X POST http://localhost:3000/change-requests/<change-request-id>/reviewers \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "reviewer_uid": 1,
    "required": true
  }'
```

`required` defaults to `true`. New reviewer assignments, and updates that make an optional reviewer required, require the reviewer UID to resolve to a known user who can use the current approval API, which means an admin-equivalent user in this foundation. Existing assignments can still be downgraded to optional if that reviewer later loses approval rights. Assigning the change-request author as reviewer is rejected. Reviewer assignments can only be changed while the change request is open. Reassigning the same active reviewer with the same `required` flag returns the existing assignment with `"created": false` and `"updated": false`; changing the `required` flag updates the existing assignment, increments `version`, and returns `"updated": true`.

Reviewer assignment responses use:

```json
{
  "assignment": {
    "id": "<assignment-id>",
    "change_request_id": "<change-request-id>",
    "reviewer": 1,
    "assigned_by": 0,
    "required": true,
    "active": true,
    "version": 1
  },
  "created": true,
  "updated": false,
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [1],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [1],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": [],
    "require_all_files_viewed": true
  },
  "require_all_files_viewed": true
}
```

List reviewer assignments:

```bash
curl http://localhost:3000/change-requests/<change-request-id>/reviewers \
  -H "Authorization: User root"
```

Reviewer list responses use:

```json
{
  "assignments": [
    {
      "id": "<assignment-id>",
      "change_request_id": "<change-request-id>",
      "reviewer": 1,
      "assigned_by": 0,
      "required": true,
      "active": true,
      "version": 1
    }
  ],
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [1],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [1],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": [],
    "require_all_files_viewed": true
  },
  "require_all_files_viewed": true
}
```

Create a review comment:

```bash
curl -X POST http://localhost:3000/change-requests/<change-request-id>/comments \
  -H "Authorization: User alice" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Please update the summary",
    "path": "/legal.txt",
    "kind": "changes_requested"
  }'
```

`kind` is optional and defaults to `general`; accepted values are `general` and `changes_requested`. `path` is optional and must be an absolute normalized path when supplied. Comment bodies are trimmed, bounded, stored on the review comment, and returned by comment APIs. New review comments are rejected after the change request is merged or rejected. Audit details omit comment body text.

List review comments:

```bash
curl http://localhost:3000/change-requests/<change-request-id>/comments \
  -H "Authorization: User root"
```

Comment create responses use:

```json
{
  "comment": {
    "id": "<comment-id>",
    "change_request_id": "<change-request-id>",
    "author": 1,
    "body": "Please update the summary",
    "path": "/legal.txt",
    "kind": "changes_requested",
    "active": true,
    "version": 1
  },
  "created": true,
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": [],
    "require_all_files_viewed": true
  },
  "require_all_files_viewed": true
}
```

Comment list responses use:

```json
{
  "comments": [
    {
      "id": "<comment-id>",
      "change_request_id": "<change-request-id>",
      "author": 1,
      "body": "Please update the summary",
      "path": "/legal.txt",
      "kind": "changes_requested",
      "active": true,
      "version": 1
    }
  ],
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": [],
    "require_all_files_viewed": true
  },
  "require_all_files_viewed": true
}
```

Dismiss an approval:

```bash
curl -X POST http://localhost:3000/change-requests/<change-request-id>/approvals/<approval-id>/dismiss \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "reason": "Approval was for an older review state"
  }'
```

`reason` is optional, trimmed, bounded, stored on the approval record, and returned by approval APIs. Audit details omit dismissal reason text. Dismissing an active approval marks it inactive, records `dismissed_by`, increments the approval version, and immediately removes it from `approval_state.approval_count`. Dismissing an already inactive approval returns `200 OK` with `"dismissed": false` and the existing inactive approval record. New dismissal attempts are rejected after the change request is merged or rejected, except matching idempotency replays return the originally recorded response.

Dismissal responses use:

```json
{
  "approval": {
    "id": "<approval-id>",
    "change_request_id": "<change-request-id>",
    "head_commit": "<64-char-commit-id>",
    "approved_by": 1,
    "comment": null,
    "active": false,
    "dismissed_by": 0,
    "dismissal_reason": "Approval was for an older review state",
    "version": 2
  },
  "dismissed": true,
  "approval_state": {
    "change_request_id": "<change-request-id>",
    "required_approvals": 1,
    "approval_count": 0,
    "approved_by": [],
    "required_reviewers": [],
    "approved_required_reviewers": [],
    "missing_required_reviewers": [],
    "approved": false,
    "matched_ref_rules": ["<rule-id>"],
    "matched_path_rules": [],
    "require_all_files_viewed": true
  },
  "require_all_files_viewed": true
}
```

Reject an open change request:

```bash
curl -X POST http://localhost:3000/change-requests/<change-request-id>/reject \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>"
```

Fast-forward merge an open change request:

```bash
curl -X POST http://localhost:3000/change-requests/<change-request-id>/merge \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>"
```

Merge succeeds only when the source ref still points to `head_commit`, the target ref still points to `base_commit`, the captured head is a descendant of the captured base, and the computed approval state is approved. Dismissed approvals do not count. Required reviewer assignments must be satisfied by approvals from those exact reviewer UIDs for the captured `head_commit`; approval by another user can satisfy the numeric count but not the required reviewer list. Stale source/target refs return `409 Conflict` before approval enforcement. Insufficient approvals return `403 Forbidden` with `approval_state` and do not update the target ref. A successful merge verifies source freshness under the same local DB write lock as the target compare-and-swap update, updates the target ref to `head_commit`, and marks the change request `merged`.

All protected-rule, approval, reviewer-assignment, review-comment, approval-dismissal, and change-request mutations emit metadata-only audit events and support optional idempotency keys. Approval, reviewer-assignment, review-comment, approval-dismissal, reject, and merge mutations are limited to open change requests; matching idempotency replays still return the originally recorded non-secret response after the change request becomes terminal. Audit details include review metadata, but not approval comments, review comment bodies, or dismissal reasons. Workspace bearer sessions are rejected from these admin endpoints.

### Revert to a Commit

```bash
curl -X POST http://localhost:3000/vcs/revert \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{"hash": "e5f6a7b8"}'
```

Response:

```json
{
  "reverted_to": "<64-char-commit-id>"
}
```

Guarded durable revert responses include the target ref, the expected head observed before CAS, and the new restore commit:

```json
{
  "reverted_to": "<64-char-target-commit-id>",
  "revert_commit": "<64-char-new-revert-commit-id>",
  "target_ref": "main",
  "expected_head": "<64-char-previous-head-id>",
  "target_commit": "<64-char-target-commit-id>"
}
```

### Check Status

```bash
curl http://localhost:3000/vcs/status \
  -H "Authorization: User root"
```

Response: plain text.

```
On commit a1b2c3d4
Objects in store: 12
Files: 8, Total size: 2450 bytes
Changes:
M /docs/readme.md
A /docs/changelog.md
```

In guarded durable mode, status is rendered from durable tree/object records and appends source identity lines for the target ref, optional session ref, base/head commit ids, base/head root tree ids, and changed path count.

### View Text Diff

```bash
curl "http://localhost:3000/vcs/diff?path=/docs/readme.md" \
  -H "Authorization: User root"
```

Frontend change-review callers can diff explicit commit pairs without using a change-request-scoped route:

```bash
curl "http://localhost:3000/vcs/diff?base=<base_commit>&head=<head_commit>&path=/docs/readme.md" \
  -H "Authorization: User root"
```

Response: plain text.

```diff
diff -- /docs/readme.md
--- a/docs/readme.md
+++ b/docs/readme.md
@@
-old line
+new line
```

In guarded durable mode, diffs use durable path maps, exact-or-descendant `path` filtering, grouped unified hunks for text changes including added/deleted text files, and stable summary output for binary, non-UTF-8, oversized, metadata-only, non-file, and type-changed paths.

## Error Responses

All errors return a JSON body with an `error` field:

```json
{
  "error": "stratum: no such file or directory: 'missing.md'"
}
```

Common HTTP status codes:

| Status | Meaning |
|---|---|
| `200` | Success |
| `400` | Bad request (missing params, invalid path, etc.) |
| `403` | Permission denied |
| `404` | File or directory not found |
| `409` | Conflict (duplicate ref, stale ref update, duplicate idempotency key, etc.) |
| `500` | Internal server error |

## Complete Workflow Example

Here's a full session using `curl` to set up a project, write files, and manage versions:

```bash
# 1. Check the server is running
curl http://localhost:3000/health

# 2. Create a project directory
curl -X PUT http://localhost:3000/fs/project \
  -H "Authorization: User alice" \
  -H "X-Stratum-Type: directory"

# 3. Create subdirectories
curl -X PUT http://localhost:3000/fs/project/docs \
  -H "Authorization: User alice" \
  -H "X-Stratum-Type: directory"

# 4. Write some files
curl -X PUT http://localhost:3000/fs/project/readme.md \
  -H "Authorization: User alice" \
  -d "# My Project

Version 1.0 — initial release."

curl -X PUT http://localhost:3000/fs/project/docs/api.md \
  -H "Authorization: User alice" \
  -d "# API Reference

## GET /users
Returns a list of users.

TODO: add more endpoints"

# 5. Commit
curl -X POST http://localhost:3000/vcs/commit \
  -H "Content-Type: application/json" \
  -H "Authorization: User root" \
  -d '{"message": "v1.0 initial release"}'

# 6. Search for TODOs
curl "http://localhost:3000/search/grep?pattern=TODO&recursive=true" \
  -H "Authorization: User alice"

# 7. View the tree
curl http://localhost:3000/tree \
  -H "Authorization: User alice"

# 8. View commit history
curl http://localhost:3000/vcs/log \
  -H "Authorization: User root"
```
