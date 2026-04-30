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
| `X-Stratum-Workspace: <uuid>` | Scope a bearer token to a hosted workspace token issued by the gateway |

Examples:

```bash
# As a named user
curl -H "Authorization: User alice" http://localhost:3000/fs/

# As an agent (token from `addagent`)
curl -H "Authorization: Bearer a1b2c3d4..." http://localhost:3000/fs/

# As root
curl -H "Authorization: User root" http://localhost:3000/fs/
```

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
  -H "Content-Type: application/json" \
  -d '{"name":"incident-demo","root_path":"/incidents/checkout-latency"}'
```

### Issue A Workspace Token

```bash
curl -X POST http://localhost:3000/workspaces/<workspace-id>/tokens \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{"name":"ci-token","agent_token":"<existing-agent-token>"}'
```

The `agent_token` is validated against the Stratum user registry before a workspace token is issued. The response includes the new `workspace_token` secret and authenticated `agent_uid`; it does not echo the raw agent token.

Use the returned secret with:

```bash
curl http://localhost:3000/fs/ \
  -H "Authorization: Bearer <workspace-secret>" \
  -H "X-Stratum-Workspace: <workspace-id>"
```

## Filesystem Operations

### Read a File

```bash
curl http://localhost:3000/fs/docs/readme.md \
  -H "Authorization: User alice"
```

Response: raw file content. MIME metadata is not yet stored in v2 foundation.

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
  "modified": 1713001275
}
```

### Write a File

```bash
curl -X PUT http://localhost:3000/fs/docs/readme.md \
  -H "Authorization: User alice" \
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
  "reverted_to": "e5f6a7b8"
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
