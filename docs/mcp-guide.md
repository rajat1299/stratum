# MCP Guide — AI Agent Integration

stratum includes an MCP (Model Context Protocol) server that lets AI agents — like Cursor, Claude Desktop, or any MCP-compatible client — interact with the filesystem using structured tool calls.

## What is MCP?

MCP is a protocol that lets AI assistants call tools exposed by external servers. Instead of generating shell commands, the AI calls structured functions like `read_file(path: "docs/readme.md")` and gets structured responses back.

## Setup

### 1. Build the MCP Binary

```bash
cargo build --release --bin stratum-mcp
```

The binary is at `target/release/stratum-mcp`.

### 2. Configure Your MCP Client

#### Cursor

Add to your project's `.cursor/mcp.json` or global MCP config:

```json
{
  "mcpServers": {
    "stratum": {
      "command": "/absolute/path/to/target/release/stratum-mcp",
      "env": {
        "STRATUM_DATA_DIR": "/path/to/your/data",
        "STRATUM_MCP_USER": "agent-x"
      }
    }
  }
}
```

#### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "stratum": {
      "command": "/absolute/path/to/target/release/stratum-mcp",
      "env": {
        "STRATUM_DATA_DIR": "/path/to/your/data",
        "STRATUM_MCP_TOKEN": "agent-api-token"
      }
    }
  }
}
```

#### Scoped Workspace Token

For a scoped workspace session, set both workspace variables:

```json
{
  "mcpServers": {
    "stratum": {
      "command": "/absolute/path/to/target/release/stratum-mcp",
      "env": {
        "STRATUM_DATA_DIR": "/path/to/your/data",
        "STRATUM_MCP_WORKSPACE_ID": "workspace-uuid",
        "STRATUM_MCP_WORKSPACE_TOKEN": "workspace-session-token"
      }
    }
  }
}
```

Workspace tokens are validated against the workspace metadata store at `STRATUM_WORKSPACE_METADATA_PATH`, or `.vfs/workspaces.bin` under `STRATUM_DATA_DIR` when that path is not set. The token's stored read and write prefixes become the MCP session scope.

For workspace-token sessions, the workspace root is mounted as `/`. Tool paths are workspace-relative: `docs/readme.md` and `/docs/readme.md` both refer to `<workspace-root>/docs/readme.md`, not the global backing path. Parent traversal is clamped to the mounted root, so paths like `../outside.txt` stay inside the workspace.

### 3. Verify

After restarting your MCP client, the stratum tools should appear in the tool list. The server communicates over stdio (stdin/stdout).

## Available Tools

### `read_file`

Read the content of a file.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | string | Yes | Path to the file |

**Example call:**
```json
{"path": "docs/readme.md"}
```

**Response:** File content as text.

---

### `write_file`

Write content to a file. Creates the file (and parent directories) if it doesn't exist.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | string | Yes | Path to the file |
| `content` | string | Yes | Content to write |

**Example call:**
```json
{"path": "notes/meeting.md", "content": "# Meeting Notes\n\nDiscussed project roadmap."}
```

---

### `list_directory`

List entries in a directory. Directories are suffixed with `/`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | string | No | Directory path (defaults to root) |

**Example call:**
```json
{"path": "docs"}
```

**Response:**
```
api.md
readme.md
specs/
```

---

### `search_files`

Search file contents using a regex pattern.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `pattern` | string | Yes | Regex pattern to search for |
| `path` | string | No | Directory or file to search in |
| `recursive` | boolean | No | Search subdirectories (default: `true`) |

**Example call:**
```json
{"pattern": "TODO", "recursive": true}
```

**Response:** Matching lines with file paths and line numbers.

---

### `find_files`

Find files by glob pattern.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | string | No | Starting directory |
| `name` | string | No | Glob pattern (e.g., `*.md`, `readme*`) |

**Example call:**
```json
{"path": ".", "name": "*.md"}
```

---

### `create_directory`

Create a directory, including any missing parent directories.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | string | Yes | Directory path to create |

**Example call:**
```json
{"path": "project/docs/specs"}
```

---

### `delete_file`

Delete a file or directory.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | string | Yes | Path to delete |
| `recursive` | boolean | No | Delete directory contents recursively (default: `false`) |

**Example call:**
```json
{"path": "old-notes.md"}
```

---

### `move_file`

Move or rename a file or directory.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | string | Yes | Current path |
| `destination` | string | Yes | New path |

**Example call:**
```json
{"source": "draft.md", "destination": "docs/final.md"}
```

---

### `commit`

Snapshot the current filesystem state.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `message` | string | Yes | Commit message |

**Example call:**
```json
{"message": "add API documentation"}
```

**Response:** Commit hash and confirmation.

---

### `get_history`

View the commit log.

**No parameters.**

**Response:** List of commits with hashes, timestamps, authors, and messages.

---

### `revert`

Revert the filesystem to a previous commit.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `hash` | string | Yes | Commit hash (or prefix) to revert to |

**Example call:**
```json
{"hash": "a1b2c3d4"}
```

---

## Resources

The MCP server also exposes read-only resources:

| URI | Description |
|---|---|
| `stratum://tree` | Full directory tree (text/plain). Workspace-token sessions see the workspace root as `/`. |
| `stratum://files/<path>` | Read a specific file's content. Workspace-token sessions resolve `<path>` relative to the mounted workspace root. |

## Important Notes

- **MCP requires an explicit non-root identity.** Set `STRATUM_MCP_USER`, `STRATUM_MCP_TOKEN`, or the workspace env pair; startup fails if the configured auth does not resolve to a non-root session.
- **Workspace MCP auth uses an explicit env pair.** Set both `STRATUM_MCP_WORKSPACE_ID` and `STRATUM_MCP_WORKSPACE_TOKEN` for scoped workspace auth. If either variable is present, both are required and invalid workspace auth is rejected without falling back to `STRATUM_MCP_TOKEN` or `STRATUM_MCP_USER`.
- **Workspace MCP paths are mounted at `/`.** With workspace auth, pass workspace-relative paths such as `src/main.rs` or `/src/main.rs`. Do not include the backing workspace root path such as `/demo/src/main.rs`; the MCP server adds that root before checking database permissions and projects result paths back to workspace-relative paths.
- **`STRATUM_MCP_TOKEN` alone is global agent-token auth.** It does not imply workspace scope. Use the workspace env pair when the MCP server should be limited to a workspace token's stored read and write prefixes.
- **MCP operations use that session's permissions.** Reads, writes, list/search/tree, delete, and move are checked against the configured user. Global VCS operations such as commit, history, and revert require an admin-equivalent session.
- **All file extensions are accepted by default.** Set `STRATUM_COMPAT_TARGET=markdown` to restore v1 `.md`-only filename enforcement.
- **Write creates parent directories.** Calling `write_file` with path `a/b/c/file.txt` automatically creates `a/`, `a/b/`, and `a/b/c/`.
- **Data persists across restarts.** The MCP server auto-saves to `.vfs/state.bin`.

## Example AI Workflow

Here's how an AI agent might use stratum in a typical session:

```
1. list_directory(path: "/")
   → See what exists

2. create_directory(path: "project/docs")
   → Set up structure

3. write_file(path: "project/docs/design.md", content: "# Design\n\n...")
   → Create documentation

4. search_files(pattern: "TODO", recursive: true)
   → Find all TODOs across the project

5. read_file(path: "project/docs/design.md")
   → Review what was written

6. commit(message: "initial documentation draft")
   → Save a snapshot

7. write_file(path: "project/docs/design.md", content: "# Design v2\n\n...")
   → Make changes

8. get_history()
   → See all commits

9. revert(hash: "a1b2c3d4")
   → Go back to the previous version if needed
```

## Sharing Data Between Access Methods

The CLI, HTTP server, and MCP server all use the same `STRATUM_DATA_DIR` for persistence. Files created through one method are visible to all others:

```bash
# Write via CLI
alice@stratum:/ $ write notes.md # Created from CLI

# Read via HTTP
curl http://localhost:3000/fs/notes.md

# Read via MCP
# Agent calls: read_file(path: "notes.md")
```

**Note:** Only one process can safely write to the same `state.bin` at a time. If you need concurrent access from multiple clients, use the HTTP server as the single point of access.
