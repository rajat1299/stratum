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

## Authentication

Filesystem, search, VCS, and workspace management requests require an auth header. Three modes are supported:

| Header | Description |
|---|---|
| `Authorization: User <username>` | Authenticate as a named user |
| `Authorization: Bearer <token>` | Authenticate with an agent API token |
| *(no header)* | Rejected, except for `/health` |

Hosted workspace requests can also include:

| Header | Description |
|---|---|
| `X-Stratum-Workspace: <uuid>` | Resolve a bearer token as a hosted workspace token issued by the gateway |

Workspace bearer tokens produce a normal agent session plus the persisted token scope. For filesystem, search, and tree routes, the workspace `root_path` is mounted as `/`, so request paths are workspace-relative. A workspace at `/incidents/checkout-latency` exposes `/read/a.txt` as the backing path `/incidents/checkout-latency/read/a.txt`. The stored `read_prefixes` and `write_prefixes` remain backing absolute paths and are still enforced before Unix-style permissions are checked. Workspace bearer tokens cannot call workspace metadata admin endpoints. Global VCS endpoints remain admin-gated.

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

When present, `Idempotency-Key` must be provided once, non-empty, visible ASCII, and at most 255 bytes. Stratum stores only a SHA-256 hash of the key.

The request fingerprint includes the route semantics, authenticated actor, workspace boundary when mounted, normalized path/ref/workspace inputs, relevant query/header fields, and normalized JSON request body where applicable. File write fingerprints include content length and SHA-256 digest, not raw file content.

A retry with the same key and same fingerprint replays the original JSON response and includes:

```http
X-Stratum-Idempotent-Replay: true
```

Reusing the same key with a different request returns `409 Conflict` without mutation. A duplicate in-progress request also returns `409 Conflict`. Invalid keys return `400 Bad Request` before mutation.

Authorization still runs before reservation and before replay. A stored replay is not returned to a caller that no longer has the required current access. If a mutation committed but audit recording failed, the idempotency record stores the same client-visible failure body, including `mutation_committed: true` and `audit_recorded: false`, so retries do not duplicate the committed side effect.

## Backend Durability Status

The current HTTP server remains backed by local stores: `.vfs/state.bin` for the in-process filesystem and VCS state, plus local files for workspace metadata, review state, idempotency records, and audit events.

Server startup now parses `STRATUM_BACKEND`, defaulting to `local`. `STRATUM_BACKEND=durable` validates the planned durable prerequisites, including `STRATUM_POSTGRES_URL`, `STRATUM_R2_BUCKET`, `STRATUM_R2_ENDPOINT`, `STRATUM_R2_ACCESS_KEY_ID`, and `STRATUM_R2_SECRET_ACCESS_KEY`, but then fails closed because the server runtime cutover is not wired yet. `STRATUM_POSTGRES_URL` must not include a password; use a deployment secret mechanism such as `PGPASSWORD`, `PGPASSFILE`, or `PGSERVICE` instead. `STRATUM_R2_ENDPOINT` must not include userinfo or secret-bearing query parameters. R2 credentials are validated only for presence and are not logged by the runtime selector.

The durable backend foundation now defines Rust contracts for future object storage, commit metadata, ref compare-and-swap, idempotency, audit, workspace metadata, and review stores. Its first Postgres metadata migration is executable through a rollback-only smoke harness and dedicated CI Postgres service-container jobs.

The backend adapter scaffolding adds a byte-backed object adapter over the existing local/R2 byte-store abstraction using repo-scoped, kind-scoped object keys. This adapter is still scaffolded behind the backend contracts and is not wired into `stratum-server` request handling.

The object adapter now stages uploads before converging on final immutable object keys, uses conditional create-if-absent semantics for final object bytes, and exposes cleanup helpers for old staged uploads plus dry-run detection for old final object keys that have no metadata record. It also has a claim-backed repair helper that can recreate missing object metadata from verified final bytes. Final object delete mode still fails closed; cleanup claims coordinate repair workers but do not yet fence concurrent metadata writers strongly enough to make deletion safe. These helpers are backend foundations only; no HTTP endpoint invokes them yet.

An optional `postgres` feature now exposes a Postgres metadata adapter for object metadata, object cleanup claims, commit metadata, and ref compare-and-swap contract tests. It is not wired into `stratum-server` request handling.

The same optional feature also exposes a Postgres-backed `IdempotencyStore` over the `idempotency_records` table. Rows store only hashed idempotency keys (`key_hash`), not raw `Idempotency-Key` header values, and the schema constrains both `key_hash` and `request_fingerprint` to lowercase SHA-256 digest shape; the adapter remains unhooked from `stratum-server` request handling.

The optional `postgres` feature also includes a Postgres-backed `AuditStore` over `audit_events`, currently exercised only by live adapter tests. It stores sanitized audit event actor/workspace/resource/details JSON and allocates global sequences with a database transaction lock, but it is not wired into `stratum-server` and does not expand read/auth/policy-decision audit coverage yet.

The optional `postgres` feature also includes a Postgres-backed `WorkspaceMetadataStore` over `workspaces` and `workspace_tokens`, currently exercised only by live adapter tests. It stores global workspace rows with `repo_id IS NULL`, preserves base/session refs and head-version updates, and persists only workspace-token secret hashes with normalized read/write prefixes. It is not wired into `stratum-server`, does not make workspace-token issuance idempotent, and does not add token rotation, expiry, revocation, or hosted secret-management behavior.

Workspace-token issuance still rejects idempotency keys because replay persistence for secret-bearing responses is intentionally out of scope for this slice. Records have no expiration or stale-pending takeover policy in the current migration until a retention model exists for durable runtime cutover.

An opt-in R2 object-store integration gate now exercises live-compatible byte round trips and backend object adapter composition when credentials are explicitly supplied. Default CI only checks that the gate skips cleanly without secrets.

An optional Rust Postgres migration runner foundation now tracks ordered migrations in `stratum_schema_migrations`, reports pending/applied/dirty/mismatched state, serializes apply attempts with a schema-scoped advisory lock, and refuses dirty or unknown applied state. It is a backend foundation behind the `postgres` feature. Migration smoke checks remain explicit through `scripts/check-postgres-migrations.sh`, and `stratum-server` still does not run migrations on startup.

These foundations do not yet enable hosted S3/R2 runtime cutover, distributed locking, background cleanup workers, multipart upload, signed URLs, lifecycle policy automation, final-object deletion, cross-store transactions, or a server runtime cutover to Postgres metadata.

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
  "objects": 12
}
```

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

Hosted workspace management endpoints require an admin (`root` or `wheel`) auth header. Records and workspace-token hashes are stored in `<STRATUM_DATA_DIR>/.vfs/workspaces.bin` by default, or `STRATUM_WORKSPACE_METADATA_PATH` when set.

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

`Idempotency-Key` is intentionally rejected on workspace-token issuance for now. The success response contains a raw `workspace_token`, and the current local idempotency store persists JSON responses. Idempotent token issuance needs secret-aware replay storage before it can be enabled safely.

Use the returned secret with:

```bash
curl http://localhost:3000/fs/read \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>"
```

With `X-Stratum-Workspace`, `/fs`, `/tree`, and omitted search paths refer to the workspace root. Paths in filesystem/search/tree responses are projected back to workspace-relative paths such as `/read/runbook.md`.

## Audit Events

`GET /audit` returns a bounded recent list of local audit events for successful or partial mutations. It requires an admin-equivalent `Authorization: User ...` session (`root` or `wheel`). Bearer tokens are forbidden, including global agent tokens and workspace tokens, even when the underlying agent is privileged.

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

Audit events include server-assigned `id`, `sequence`, and `timestamp`; actor UID/username plus an optional delegate; optional mounted workspace context; `action`; `resource` kind/id/path; `outcome`; and a small string-keyed `details` map. Current audited actions cover successful filesystem write, mkdir, delete, copy, and move operations; VCS commit, revert, ref create, and ref update operations; protected-rule, change-request, approval, review-comment, and approval-dismissal operations; workspace creation and workspace-token issuance; and run-record creation.

Audit details are intentionally metadata-only. They must not contain file contents, raw tokens, request bodies, run prompt/command/stdout/stderr/result content, or commit messages.

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

The review-control foundation is local/file-backed and admin-gated. It defines protected ref rules, protected path-prefix rules, fast-forward-only change requests, reviewer assignments, review comments, approval records, approval dismissal, and computed approval state. This is still a foundation: it does not include reviewer groups, threaded comments, merge queues, or a web review UI.

Create a protected ref rule:

```bash
curl -X POST http://localhost:3000/protected/refs \
  -H "Authorization: User root" \
  -H "Idempotency-Key: <retry-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "ref_name": "main",
    "required_approvals": 1
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
    "required_approvals": 2
  }'
```

`target_ref` is optional. Path prefixes are absolute, normalized boundaries. Direct filesystem enforcement evaluates these rules against resolved backing paths after workspace mount resolution; client responses still use projected paths and do not expose backing workspace paths beyond existing route behavior.

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

The server validates both refs exist, captures the current target-ref commit as `base_commit`, captures the current source-ref commit as `head_commit`, and creates an `open` change request. Change-request create/read/list/reject responses use this shape:

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
    "matched_path_rules": []
  }
}
```

`approval_state` is computed from active protected ref rules matching the target ref, active protected path rules matching changed paths between `base_commit` and `head_commit`, and active required reviewer assignments. The effective required approval count is the maximum `required_approvals` from matching rules. `approved` is true only when the numeric approval count is satisfied and every required reviewer has an active approval for the captured `head_commit`.

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
    "matched_path_rules": []
  }
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
    "matched_path_rules": []
  }
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
    "matched_path_rules": []
  }
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
    "matched_path_rules": []
  }
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
    "matched_path_rules": []
  }
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

### View Text Diff

```bash
curl "http://localhost:3000/vcs/diff?path=/docs/readme.md" \
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
