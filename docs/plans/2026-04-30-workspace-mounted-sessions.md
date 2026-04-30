# Workspace Mounted Sessions Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Make workspace bearer and MCP sessions expose the workspace root as `/`, so remote agents use workspace-relative paths while existing absolute `SessionScope` prefixes remain the backing security boundary.

**Architecture:** Add a mounted workspace context to `Session`. HTTP and MCP path ingress resolves client paths through the session mount before calling `StratumDb`; responses from mounted sessions project backing paths back to workspace-relative paths. The database continues to enforce absolute read/write scopes after projection, so path traversal and prefix sibling mistakes fail closed.

**Tech Stack:** Rust, Tokio, Axum, rmcp, existing `Session`, `WorkspaceMetadataStore`, `StratumDb`, and route tests.

---

### Task 1: Session Mount Model And Path Projection Helpers

**Files:**
- Modify: `src/auth/session.rs`

**Requirements:**
- Add a `SessionMount` or equivalent mounted workspace context carrying `workspace_id: Uuid` and normalized `root_path`.
- Add `Session::with_mount(...)`.
- Add helpers that:
  - normalize a user-facing path under the mounted root,
  - treat both `foo` and `/foo` as workspace-relative when a mount exists,
  - clamp `.` and `..` so `../outside` and `/../outside` resolve to the mounted root, not outside it,
  - leave paths unchanged for unmounted sessions,
  - project an absolute backing path under the root back to workspace-relative output.
- Add unit tests for mounted and unmounted path behavior.

**Verification:**

```bash
cargo test --locked auth::session::tests::mounted -- --nocapture
```

**Commit:**

```bash
git add src/auth/session.rs
git commit -m "feat: add workspace session mount projection"
```

---

### Task 2: HTTP Workspace Bearer Mounted Filesystem View

**Files:**
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_fs.rs`
- Modify docs if behavior changes examples: `docs/http-api-guide.md`, `docs/getting-started.md`, `docs/cli-cloud-bridge.md`

**Requirements:**
- When `session_from_headers` resolves a workspace bearer token, attach both the existing `SessionScope` and the mounted workspace root from `ValidWorkspaceToken.workspace.root_path`.
- Project all HTTP filesystem, search, and tree input paths through the session before DB calls:
  - `/fs` and `/tree` resolve to the workspace root.
  - `/fs/read/a.txt` resolves to `<workspace-root>/read/a.txt`.
  - `dst` query parameters for copy/move are projected too.
- Project mounted-session response paths back to workspace-relative paths for JSON and text where the server controls output:
  - `ls_to_json.path`
  - `created`, `written`, `deleted`, `copied`, `moved`, `to`
  - grep `file` values
  - find result values
- Preserve unmounted/global behavior.
- Add route tests for workspace-relative read/write/list/search/find/tree and traversal attempts.

**Verification:**

```bash
cargo test --locked server::middleware::tests server::routes_fs::tests -- --nocapture
```

**Commit:**

```bash
git add src/server/middleware.rs src/server/routes_fs.rs docs/http-api-guide.md docs/getting-started.md docs/cli-cloud-bridge.md
git commit -m "feat: mount workspace bearer paths over http"
```

---

### Task 3: MCP Workspace Mounted Tool View

**Files:**
- Modify: `src/bin/stratum_mcp.rs`
- Modify: `docs/mcp-guide.md`

**Requirements:**
- When MCP resolves `STRATUM_MCP_WORKSPACE_ID` plus `STRATUM_MCP_WORKSPACE_TOKEN`, attach the same mounted workspace root as HTTP.
- Project all MCP path arguments before DB calls:
  - `read_file`, `write_file`, `list_directory`, `search_files`, `find_files`, `create_directory`, `delete_file`, `move_file`, `stratum://files/<path>`.
- Project tool output paths back to workspace-relative paths for `search_files`, `find_files`, and status messages.
- Preserve global token/user MCP behavior.
- Add tests proving workspace env sessions read/write relative paths and cannot escape the mount.

**Verification:**

```bash
cargo test --locked --bin stratum_mcp mcp_session_workspace -- --nocapture
```

**Commit:**

```bash
git add src/bin/stratum_mcp.rs docs/mcp-guide.md
git commit -m "feat: mount workspace paths in mcp"
```

---

### Task 4: Review Fixes And Full Verification

**Files:**
- Modify only files needed for reviewer findings.

**Requirements:**
- Dispatch a fresh spec reviewer to compare the implementation against this plan.
- Dispatch a fresh code-quality/security reviewer for path traversal, response projection consistency, and authorization fail-closed behavior.
- Fix reviewer findings and commit review fixes separately.

**Verification:**

```bash
cargo test --locked
git diff --check HEAD~4..HEAD
```

**Commit:**

```bash
git add <changed-files>
git commit -m "fix: address workspace mount review findings"
```
