# Durable Deployment Runbook

Status: Task 13 / Durable Deployment Runbook, private-beta closeout.

This runbook defines the single supported durable deployment posture for the
private-beta preview: Cloudflare/Postgres/R2 posture with Stratum running
`STRATUM_BACKEND=durable` and `STRATUM_CORE_RUNTIME=durable-cloud`. It is an
operator checklist, not a promise that every hosted product surface is ready.

Authoritative sources:

- `docs/private-beta-contract.md`
- `docs/http-api-guide.md`
- `sdk/contracts/capabilities.v1.durable-cloud.json`
- `scripts/ci-live-durable-cloud-gate.sh`
- `scripts/check-postgres-migrations.sh`
- `scripts/check-r2-object-store.sh`

## Scope

The private-beta durable posture uses:

- a Cloudflare account and deploy target as the hosted boundary
- Cloudflare R2 or an R2-compatible object store for immutable object bytes
- Postgres for durable control-plane stores, migration state, audit,
  idempotency, workspace metadata, review metadata, refs, commits, and recovery
  rows
- `stratum-server` built with `--features postgres`
- workspace bearer sessions with explicit workspace, org, repo, and durable
  session-ref context where route contracts require them

This repository currently contains the Stratum runtime, provider gates, and
operator checks. It does not contain a checked-in Cloudflare deploy manifest.
Keep Cloudflare account/project configuration in operator-controlled
infrastructure until Task 16 release rehearsal records an exact deploy path.

Out of scope for this runbook: hosted durable audit listing, hosted `/runs`,
hosted `/execute`, VCS recovery operator routes under durable-cloud, semantic
search when the durable search index is unavailable, provider mounts, durable
MCP, durable FUSE, direct durable-cloud REPL, hosted auth login, OIDC/SAML/SCIM
readiness, hosted admin UI, production audit export, broad destructive object
or commit cleanup, and a general-purpose Redis lock service.

## Secret Handling

Do not paste, log, screenshot, or commit credential values. Name credential
types and env var names instead.

Required posture:

- `STRATUM_POSTGRES_URL` and `STRATUM_POSTGRES_TEST_URL` must not include a
  password. Use `PGPASSWORD`, `PGPASSFILE`, `PGSERVICE`, or the deployment
  secret provider. The live wrapper also accepts
  `STRATUM_POSTGRES_TEST_PASSWORD` and maps it to `PGPASSWORD` when needed.
- Remote Postgres targets should require TLS, for example `sslmode=require`.
- `STRATUM_R2_ENDPOINT` must use HTTPS and must not contain userinfo or query
  parameters. Plaintext loopback endpoints are local-test only and require
  `STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT=1`.
- `.env.live-gates` is operator-local. In this checkout it is only a local
  source for gate credentials and Cloudflare operator keys by name, including
  `ACCOUNT_ID`, `API_TOKEN`, and `token_value`. Do not print its values, commit
  it, or rely on it as a published deployment artifact.
- Redacted live wrappers must be used in protected contexts. Prefer
  `STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-durable-cloud-gate.sh`
  instead of running raw provider commands in CI logs.

## Environment Matrix

### Cloudflare Operator Env

These are consumed by the operator deploy context, not by Stratum runtime
selection itself:

| Variable | Purpose |
|---|---|
| `ACCOUNT_ID` | Cloudflare account selector for operator/deploy tooling. |
| `API_TOKEN` | Cloudflare API token held in local/CI secret storage. |
| `token_value` | Existing operator-local token material name found in `.env.live-gates`; keep value secret. |

If the final Cloudflare deploy target introduces different variable names, keep
this runbook updated and keep the live gate env below unchanged unless runtime
code changes.

### Runtime Env

Set these for the hosted durable `stratum-server` process:

| Variable | Required value or posture |
|---|---|
| `STRATUM_BACKEND` | `STRATUM_BACKEND=durable` |
| `STRATUM_CORE_RUNTIME` | `STRATUM_CORE_RUNTIME=durable-cloud` |
| `STRATUM_POSTGRES_URL` | Password-free Postgres URL, TLS for remote targets. |
| `STRATUM_POSTGRES_SCHEMA` | Optional schema selector, defaults to `public`. |
| `STRATUM_DURABLE_MIGRATION_MODE` | `status` by default; see migration section. |
| `STRATUM_R2_BUCKET` | Object-store bucket name. |
| `STRATUM_R2_ENDPOINT` | HTTPS R2/S3-compatible endpoint with no userinfo or query. |
| `STRATUM_R2_ACCESS_KEY_ID` | Secret-managed access key id. |
| `STRATUM_R2_SECRET_ACCESS_KEY` | Secret-managed access key secret. |
| `STRATUM_R2_REGION` | Optional region; set when provider tooling requires it. |
| `STRATUM_R2_PREFIX` | Optional repo/environment prefix for object keys. |
| `STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT` | Local loopback tests only; never set for hosted Cloudflare/R2. |

Readiness gates:

| Variable | Required value or posture |
|---|---|
| `STRATUM_DURABLE_AUTH_SESSION_READY` | `STRATUM_DURABLE_AUTH_SESSION_READY=1` |
| `STRATUM_DURABLE_POLICY_READY` | `STRATUM_DURABLE_POLICY_READY=1` |
| `STRATUM_DURABLE_REPO_ROUTING_READY` | `STRATUM_DURABLE_REPO_ROUTING_READY=1` |
| `STRATUM_DURABLE_RECOVERY_READY` | `STRATUM_DURABLE_RECOVERY_READY=1` |
| `STRATUM_DURABLE_CORE_REPO_ID` | Non-local durable repo id. |

Idempotency and quota posture:

| Variable | Required value or posture |
|---|---|
| `STRATUM_IDEMPOTENCY_COMPLETED_RETENTION_SECONDS` | Positive bounded retention value. |
| `STRATUM_IDEMPOTENCY_PENDING_STALE_SECONDS` | Positive bounded stale-pending value. |
| `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_SCOPE` | Positive bounded per-scope quota. |
| `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_REPO` | Optional bounded quota. |
| `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_WORKSPACE` | Optional bounded quota. |
| `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_PRINCIPAL` | Optional bounded quota. |

Hosted storage posture:

| Variable | Required value or posture |
|---|---|
| `STRATUM_POSTGRES_POOL_MAX_SIZE` | Required positive bounded integer. |
| `STRATUM_POSTGRES_CONNECT_TIMEOUT_MS` | Required positive bounded integer. |
| `STRATUM_POSTGRES_OPERATION_TIMEOUT_MS` | Required positive bounded integer. |
| `STRATUM_POSTGRES_POOL_ACQUIRE_TIMEOUT_MS` | Required positive bounded integer. |
| `STRATUM_R2_REQUEST_TIMEOUT_MS` | Required positive bounded integer. |
| `STRATUM_R2_CONNECT_TIMEOUT_MS` | Required positive bounded integer. |
| `STRATUM_R2_MAX_ATTEMPTS` | Required positive bounded integer. |
| `STRATUM_R2_RETRY_BASE_DELAY_MS` | Required positive bounded integer. |
| `STRATUM_R2_RETRY_MAX_DELAY_MS` | Required positive bounded integer. |

Explicitly disabled or not part of this posture:

- `STRATUM_DURABLE_COMMIT_ROUTE=1` is local-state guarded durable only. It must
  not be set with `STRATUM_CORE_RUNTIME=durable-cloud`.
- `STRATUM_AUDIT_EVENT_EXPORT_PROVIDER` remains disabled unless a separate
  audit-export rollout explicitly scopes it.
- OIDC, SAML, SCIM, hosted runners, process-local execution, direct MCP/FUSE,
  and provider mounts stay outside this deployment checklist.

### Live Verification Env

The live gates use test aliases so operators can verify provider readiness
without exposing runtime secrets in logs:

| Variable | Purpose |
|---|---|
| `STRATUM_POSTGRES_TEST_URL` | Password-free URL for migration and Postgres live checks. |
| `STRATUM_POSTGRES_TEST_PASSWORD` | Optional source for `PGPASSWORD`; value must be secret-managed. |
| `PGPASSWORD` | Preferred password seam for `psql` and Postgres tests. |
| `STRATUM_R2_BUCKET` | Bucket for R2 live gate. |
| `STRATUM_R2_ENDPOINT` | Endpoint for R2 live gate. |
| `STRATUM_R2_ACCESS_KEY_ID` | Access key id for R2 live gate. |
| `STRATUM_R2_SECRET_ACCESS_KEY` | Access key secret for R2 live gate. |
| `STRATUM_R2_REGION` | Optional region. |
| `STRATUM_R2_PREFIX` | Optional prefix. |
| `STRATUM_LIVE_GATE_REQUIRED` | Set `STRATUM_LIVE_GATE_REQUIRED=1` in protected contexts where live credentials must exist. |

## Migrations

Default startup uses `STRATUM_DURABLE_MIGRATION_MODE=status`. Status mode
reports pending migrations and fails startup until an operator applies or
adopts them; it does not mutate the schema.

Use `STRATUM_DURABLE_MIGRATION_MODE=apply` during a controlled maintenance
window to apply pending migrations through the schema-scoped advisory lock.
After apply, restart with `status` unless the deploy automation deliberately
keeps apply mode for a short rollout window.

Use `STRATUM_DURABLE_MIGRATION_MODE=adopt` only for verified legacy schemas
that were manually migrated before `stratum_schema_migrations` existed.
Adoption refuses dirty, unknown, checksum-mismatched, partially populated, or
unverifiable schema state.

Migration smoke command:

```bash
STRATUM_POSTGRES_MIGRATIONS_REQUIRED=1 \
STRATUM_POSTGRES_REDACT_ERRORS=1 \
./scripts/check-postgres-migrations.sh
```

The smoke script runs a rollback-only migration catalog check. It rejects
password-bearing `STRATUM_POSTGRES_TEST_URL` values and redacts provider output
when `STRATUM_POSTGRES_REDACT_ERRORS=1`.

## Startup And Live Gates

Build the server:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task13 \
cargo build --locked --release --features postgres --bin stratum-server
```

Source operator secrets without echoing values:

```bash
set -a
. /path/to/secure/runtime.env
set +a
```

Run provider checks before serving traffic:

```bash
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-r2-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-durable-cloud-gate.sh
```

`./scripts/ci-live-durable-cloud-gate.sh` masks Postgres and R2 values,
requires complete live provider config when `STRATUM_LIVE_GATE_REQUIRED=1`, and
runs the durable-cloud startup test that proves the server opens durable stores
without creating local `.vfs` state.

The redacted wrappers call the lower-level helpers
`./scripts/check-postgres-migrations.sh` and
`./scripts/check-r2-object-store.sh`. Use the wrappers for protected live
contexts; use the lower-level helpers only for local smoke checks or when a
wrapper already controls log redaction.

Start `stratum-server` with the runtime env from the matrix. Exact process
manager commands belong to the Cloudflare deployment target. The Stratum
requirements are the env posture above and a `postgres` feature build.

## Post-Deploy Checks

Health:

```bash
curl -fsS "$STRATUM_BASE_URL/health"
```

Expected durable posture:

- `core_runtime` is `durable-cloud`
- local core counters such as commits/inodes/objects are null
- readiness reports configuration/store booleans only
- no DB URLs, R2 endpoints, credentials, object keys, SQL, request bodies, or
  provider errors appear in output

Capabilities:

```bash
curl -fsS "$STRATUM_BASE_URL/v1/capabilities"
```

This is the `GET /v1/capabilities` evidence source. Confirm:

- `server.backend_mode` is `durable`
- `server.core_runtime` is `durable-cloud`
- workspace auth is the durable advertised mode
- supported FS/search/tree, VCS, protected-rule, workspace, and review routes
  match `sdk/contracts/capabilities.v1.durable-cloud.json`
- unavailable routes below remain unavailable

Unsupported durable-cloud route evidence:

| Capability key | Expected reason |
|---|---|
| `routes.search.semantic` | `search index unavailable` until the durable derived index is ready. |
| `routes.vcs.recovery` | `durable-cloud route is not supported yet` |
| `routes.audit` | `durable-cloud route is not supported yet` |
| `routes.runs` | `durable-cloud route is not supported yet` |
| `routes.execute` | `durable-cloud route is not supported yet` |
| `sources.provider_mounts` | Remote/blob/provider mounts are not enabled for this beta. |

Unsupported HTTP route groups must return stable `501` JSON:

```json
{"error":"stratum: operation not supported: durable-cloud route is not supported yet"}
```

## Rollback checklist

Use rollback only to return traffic to a known supported posture. Do not create
an undocumented local fallback for a durable-cloud route; do not fall back to local `.vfs`
state when durable-cloud config, routes, search indexes, or live stores are
unavailable.

App/config rollback:

- remove traffic from the new Cloudflare deployment
- redeploy the previous known-good app image/config, or unset
  `STRATUM_CORE_RUNTIME` / set it to `local-state` for an explicit local-state
  environment
- remove durable-cloud readiness gates from the rolled-back local-state runtime
- keep `STRATUM_DURABLE_COMMIT_ROUTE=1` unset unless intentionally running the
  separate local-state guarded durable path

Migration rollback:

- default status-mode smoke is rollback-only and does not mutate schema
- after `STRATUM_DURABLE_MIGRATION_MODE=apply`, treat migrations as
  forward-only; restore from a Postgres backup/branch or deploy the previous app
  version only if it is compatible with the applied schema
- do not hand-edit `stratum_schema_migrations`
- `adopt` rollback is a database restore/branch operation, not a second
  adoption attempt against unverifiable state

R2/object rollback:

- stop new writes before rolling back app traffic
- do not delete buckets, prefixes, staged uploads, final objects, or metadata as
  part of app rollback
- preserve `STRATUM_R2_PREFIX` and object bytes for audit/recovery
- destructive cleanup remains outside this private-beta deploy posture

Route rollback:

- unsupported durable-cloud route groups must keep returning the stable `501`
  JSON above
- do not route `/audit`, `/runs`, `/execute`, VCS recovery, provider mounts,
  direct MCP, FUSE, or REPL calls to local state as a hidden fallback
- clients should re-read `/v1/capabilities` after rollback

Audit/export rollback:

- durable mutation audit persistence remains the system of record
- hosted durable audit listing stays unsupported until a repo/tenant listing
  contract exists
- audit export remains disabled unless a separate rollout owns it

## Signoff checklist

Record this evidence for Task 13 signoff:

- secure store contains required env names, with no values pasted into docs,
  tickets, screenshots, or logs
- `.env.live-gates` was sourced only from a secure local/CI context and values
  were not printed
- `STRATUM_POSTGRES_URL` and `STRATUM_POSTGRES_TEST_URL` are password-free
- R2 endpoint posture is HTTPS with no userinfo or query parameters
- migration smoke passed with `STRATUM_POSTGRES_REDACT_ERRORS=1`
- R2 live gate passed with bounded timeout/retry settings
- `./scripts/ci-live-durable-cloud-gate.sh` passed with
  `STRATUM_LIVE_GATE_REQUIRED=1`
- `/health` reports `durable-cloud` without local `.vfs` counters or secret
  details
- `GET /v1/capabilities` matches the checked-in durable manifest
- unsupported durable route groups still return the stable `501` JSON
- rollback owner, Postgres backup/branch, Cloudflare previous deploy, and R2
  preservation plan are recorded before traffic moves
- no undocumented manual steps remain before Task 16 release rehearsal
