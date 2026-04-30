# Session Token Scopes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add scoped workspace/session tokens with explicit read and write path prefixes, then enforce them consistently across HTTP, MCP, client, CLI, and persisted workspace metadata.

**Architecture:** `Session` carries identity plus an optional least-privilege `SessionScope`. Global user and agent auth remains unrestricted, while workspace token auth attaches normalized read/write prefixes from the persisted token record. DB `*_as` methods enforce scope before existing Unix-style permissions so HTTP and MCP share the same security boundary.

**Tech Stack:** Rust, Tokio, Axum, serde/bincode, reqwest, clap, existing `StratumDb` and `WorkspaceMetadataStore`.

---

### Task 1: Core Session Scope Model And DB Enforcement

**Files:**
- Modify: `src/auth/session.rs`
- Modify: `src/db.rs`

**Step 1: Write failing tests**

Add focused unit tests near the existing `src/db.rs` tests:

```rust
#[tokio::test]
async fn scoped_session_can_read_allowed_prefix_but_not_sibling() {
    let db = StratumDb::open_memory();
    let mut root = Session::root();
    db.execute_command("mkdir /allowed", &mut root).await.unwrap();
    db.execute_command("touch /allowed/a.txt", &mut root).await.unwrap();
    db.execute_command("write /allowed/a.txt ok", &mut root).await.unwrap();
    db.execute_command("mkdir /blocked", &mut root).await.unwrap();
    db.execute_command("touch /blocked/a.txt", &mut root).await.unwrap();
    db.execute_command("write /blocked/a.txt no", &mut root).await.unwrap();
    db.execute_command("adduser bot", &mut root).await.unwrap();

    let scoped = db
        .login("bot")
        .await
        .unwrap()
        .with_scope(SessionScope::new(vec!["/allowed"], Vec::<String>::new()).unwrap());

    assert_eq!(
        String::from_utf8(db.cat_as("/allowed/a.txt", &scoped).await.unwrap()).unwrap(),
        "ok"
    );
    assert!(matches!(
        db.cat_as("/blocked/a.txt", &scoped).await,
        Err(VfsError::PermissionDenied { .. })
    ));
}

#[tokio::test]
async fn scoped_session_requires_write_prefix_for_mutation() {
    let db = StratumDb::open_memory();
    let mut root = Session::root();
    db.execute_command("mkdir /allowed", &mut root).await.unwrap();
    db.execute_command("mkdir /readonly", &mut root).await.unwrap();
    db.execute_command("adduser bot", &mut root).await.unwrap();

    let scoped = db
        .login("bot")
        .await
        .unwrap()
        .with_scope(SessionScope::new(vec!["/readonly"], vec!["/allowed"]).unwrap());

    db.write_file_as("/allowed/a.txt", b"ok".to_vec(), &scoped)
        .await
        .unwrap();
    assert!(matches!(
        db.write_file_as("/readonly/a.txt", b"no".to_vec(), &scoped).await,
        Err(VfsError::PermissionDenied { .. })
    ));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --locked db::tests::scoped_session -- --nocapture`

Expected: compile failure because `SessionScope` and `with_scope` do not exist.

**Step 3: Write minimal implementation**

- Add `SessionScope` to `src/auth/session.rs` with normalized absolute path prefixes.
- Add `Session.scope: Option<SessionScope>`, default `None` for unrestricted sessions.
- Add helpers:
  - `SessionScope::new(read_prefixes, write_prefixes) -> Result<Self, VfsError>`
  - `Session::with_scope(scope) -> Self`
  - `Session::is_path_allowed(path, access) -> bool`
- In `src/db.rs`, add centralized scope helpers and call them before existing path permission checks in:
  - Reads: `cat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, `grep_as`
  - Writes: `write_file_as`, `mkdir_p_as`, `rm_as`, `mv_as`, `cp_as`
- Preserve current admin gates for `commit_as`, `revert_as`, `vcs_log_as`, `vcs_status_as`, and `vcs_diff_as`.

**Step 4: Run focused tests**

Run: `cargo test --locked db::tests::scoped_session -- --nocapture`

Expected: new tests pass.

**Step 5: Commit**

Run:

```bash
git add src/auth/session.rs src/db.rs
git commit -m "feat: enforce scoped session prefixes"
```

---

### Task 2: Workspace Token Scope Persistence

**Files:**
- Modify: `src/workspace/mod.rs`
- Modify: route test support types in `src/server/routes_vcs.rs` if trait signatures change

**Step 1: Write failing tests**

Extend workspace tests to assert issued tokens store and reload read/write prefixes:

```rust
#[tokio::test]
async fn workspace_tokens_persist_read_write_prefixes() {
    let path = temp_metadata_path("token-scopes");
    let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
    let workspace = store.create_workspace("demo", "/demo").await.unwrap();
    let issued = store
        .issue_workspace_token(
            workspace.id,
            "agent-session",
            7,
            vec!["/demo/read".to_string()],
            vec!["/demo/write".to_string()],
        )
        .await
        .unwrap();
    drop(store);

    let reloaded = LocalWorkspaceMetadataStore::open(&path).unwrap();
    let valid = reloaded
        .validate_workspace_token(workspace.id, &issued.raw_secret)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(valid.token.read_prefixes, vec!["/demo/read"]);
    assert_eq!(valid.token.write_prefixes, vec!["/demo/write"]);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --locked workspace::tests::workspace_tokens_persist_read_write_prefixes -- --nocapture`

Expected: compile failure because the trait and record do not include prefixes.

**Step 3: Write minimal implementation**

- Add `read_prefixes: Vec<String>` and `write_prefixes: Vec<String>` to `WorkspaceTokenRecord`.
- Update `WorkspaceMetadataStore::issue_workspace_token` signature to accept both prefix lists.
- Normalize prefixes using the `SessionScope` constructor or shared path normalization.
- Bump `WORKSPACE_METADATA_VERSION` and keep decode errors explicit for unsupported versions.
- Update all in-memory/local store implementations and test fake stores.
- Preserve existing raw-secret non-persistence behavior.

**Step 4: Run focused tests**

Run: `cargo test --locked workspace::tests::workspace_tokens -- --nocapture`

Expected: workspace token tests pass.

**Step 5: Commit**

Run:

```bash
git add src/workspace/mod.rs src/server/routes_vcs.rs
git commit -m "feat: persist workspace token scopes"
```

---

### Task 3: HTTP Middleware, Workspace Routes, Client, And CLI

**Files:**
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/client/mod.rs`
- Modify: `src/bin/stratumctl.rs`
- Modify docs if request/response examples change: `docs/http-api-guide.md`, `docs/getting-started.md`, `docs/cli-cloud-bridge.md`

**Step 1: Write failing tests**

Add route/middleware tests that show a workspace bearer session carries scope and denies out-of-prefix filesystem access. Add request parsing tests for default/read/write prefix issuance.

**Step 2: Run tests to verify failure**

Run: `cargo test --locked server::middleware::tests::workspace_bearer_authenticates_after_file_store_rebuild server::routes_workspace::tests::issue_workspace_token -- --nocapture`

Expected: compile or assertion failure before middleware attaches scope and route issue requests include prefixes.

**Step 3: Write minimal implementation**

- Extend `IssueTokenRequest` with `read_prefixes` and `write_prefixes`.
- Default omitted prefixes to `WorkspaceRecord.root_path` for both read and write unless explicitly supplied.
- Attach validated workspace token prefixes to the resolved agent session in `session_from_headers`.
- Ensure workspace bearer tokens cannot satisfy workspace metadata admin endpoints.
- Update client and CLI `workspace issue-token` command to accept repeated `--read-prefix` and `--write-prefix` flags.
- Preserve existing header names and JSON response names.

**Step 4: Run focused route/client tests**

Run: `cargo test --locked server::middleware::tests server::routes_workspace::tests client::tests -- --nocapture`

Expected: focused tests pass.

**Step 5: Commit**

Run:

```bash
git add src/server/middleware.rs src/server/routes_workspace.rs src/server/routes_fs.rs src/client/mod.rs src/bin/stratumctl.rs docs/http-api-guide.md docs/getting-started.md docs/cli-cloud-bridge.md
git commit -m "feat: issue scoped workspace tokens over http"
```

---

### Task 4: MCP Scoped Token Wiring

**Files:**
- Modify: `src/bin/stratum_mcp.rs`
- Modify: `docs/mcp-guide.md`

**Step 1: Write failing tests**

Add tests for `mcp_session_from_env` or a small helper that resolves `STRATUM_MCP_WORKSPACE_ID` plus `STRATUM_MCP_WORKSPACE_TOKEN` into a scoped non-root session.

**Step 2: Run test to verify it fails**

Run: `cargo test --locked --bin stratum_mcp mcp_session -- --nocapture`

Expected: failure because MCP only supports global token/user auth.

**Step 3: Write minimal implementation**

- Add explicit env support:
  - `STRATUM_MCP_WORKSPACE_ID`
  - `STRATUM_MCP_WORKSPACE_TOKEN`
- Do not infer workspace scope from plain `STRATUM_MCP_TOKEN`.
- Keep root rejection.
- Use the same scoped `Session` enforcement through existing DB calls.
- Document the MCP env pair and read/write behavior.

**Step 4: Run focused MCP tests**

Run: `cargo test --locked --bin stratum_mcp -- --nocapture`

Expected: MCP tests pass.

**Step 5: Commit**

Run:

```bash
git add src/bin/stratum_mcp.rs docs/mcp-guide.md
git commit -m "feat: support scoped workspace tokens in mcp"
```

---

### Task 5: Review Fixes And Full Verification

**Files:**
- Modify only files needed for reviewer findings.

**Step 1: Dispatch spec review**

Use a fresh `gpt-5.5` reviewer to compare implementation against this plan.

**Step 2: Dispatch code quality review**

Use a fresh `gpt-5.5` reviewer to inspect security, persistence compatibility, route semantics, and test coverage.

**Step 3: Fix review findings**

Make a separate commit for review fixes:

```bash
git add <changed-files>
git commit -m "fix: address scoped token review findings"
```

**Step 4: Full verification**

Run:

```bash
cargo test --locked
git diff --check HEAD~5..HEAD
```

Expected: all tests pass and whitespace check is clean.
