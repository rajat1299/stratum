use stratum::auth::session::{Session, SessionScope};
use stratum::config::Config;
use stratum::db::StratumDb;
use stratum::error::VfsError;
use stratum::workspace::LocalWorkspaceMetadataStore;

use rmcp::model::*;
use rmcp::service::RoleServer;
use rmcp::{ErrorData as McpError, ServerHandler, ServiceExt};

use std::env;
use std::sync::Arc;
use uuid::Uuid;

struct McpServer {
    db: StratumDb,
    session: Session,
}

fn tool_schema(props: serde_json::Value) -> Arc<JsonObject> {
    let mut obj = serde_json::Map::new();
    obj.insert("type".into(), "object".into());
    if let Some(p) = props.get("properties") {
        obj.insert("properties".into(), p.clone());
    }
    if let Some(r) = props.get("required") {
        obj.insert("required".into(), r.clone());
    }
    Arc::new(obj.into())
}

fn make_tool(name: &'static str, desc: &'static str, schema: serde_json::Value) -> Tool {
    Tool::new(name, desc, tool_schema(schema))
}

fn vfs_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn non_root_session(session: Session) -> Result<Session, VfsError> {
    if session.is_root() {
        return Err(VfsError::AuthError {
            message: "MCP identity must resolve to a non-root user".to_string(),
        });
    }
    Ok(session)
}

#[derive(Debug, Clone, Default)]
struct McpAuthEnv {
    token: Option<String>,
    user: Option<String>,
    workspace_id: Option<String>,
    workspace_token: Option<String>,
}

impl McpAuthEnv {
    fn from_process_env() -> Self {
        Self {
            token: env::var("STRATUM_MCP_TOKEN").ok(),
            user: env::var("STRATUM_MCP_USER").ok(),
            workspace_id: env::var("STRATUM_MCP_WORKSPACE_ID").ok(),
            workspace_token: env::var("STRATUM_MCP_WORKSPACE_TOKEN").ok(),
        }
    }
}

async fn mcp_session_from_env(db: &StratumDb) -> Result<Session, VfsError> {
    mcp_session_from_auth_env(db, McpAuthEnv::from_process_env()).await
}

async fn mcp_session_from_auth_env(
    db: &StratumDb,
    auth_env: McpAuthEnv,
) -> Result<Session, VfsError> {
    if auth_env.workspace_id.is_some() || auth_env.workspace_token.is_some() {
        return mcp_workspace_session_from_env(db, auth_env.workspace_id, auth_env.workspace_token)
            .await;
    }

    let token = auth_env
        .token
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    let username = auth_env
        .user
        .as_deref()
        .filter(|value| !value.trim().is_empty());

    if let Some(token) = token {
        if let Ok(session) = db.authenticate_token(token).await {
            return non_root_session(session);
        }
    }

    if let Some(username) = username {
        if let Ok(session) = db.login(username).await {
            return non_root_session(session);
        }
    }

    Err(VfsError::AuthError {
        message: "set STRATUM_MCP_TOKEN, STRATUM_MCP_USER, or STRATUM_MCP_WORKSPACE_ID with STRATUM_MCP_WORKSPACE_TOKEN to a non-root identity".to_string(),
    })
}

async fn mcp_workspace_session_from_env(
    db: &StratumDb,
    workspace_id: Option<String>,
    workspace_token: Option<String>,
) -> Result<Session, VfsError> {
    let (Some(workspace_id), Some(workspace_token)) = (workspace_id, workspace_token) else {
        return Err(VfsError::AuthError {
            message: "set both STRATUM_MCP_WORKSPACE_ID and STRATUM_MCP_WORKSPACE_TOKEN"
                .to_string(),
        });
    };

    if workspace_id.trim().is_empty() || workspace_token.trim().is_empty() {
        return Err(VfsError::AuthError {
            message: "set both STRATUM_MCP_WORKSPACE_ID and STRATUM_MCP_WORKSPACE_TOKEN"
                .to_string(),
        });
    }

    let workspace_id = Uuid::parse_str(workspace_id.trim()).map_err(|_| VfsError::AuthError {
        message: format!("invalid STRATUM_MCP_WORKSPACE_ID: {workspace_id}"),
    })?;
    let Some(valid) = LocalWorkspaceMetadataStore::validate_workspace_token_read_only(
        db.config().workspace_metadata_path(),
        workspace_id,
        &workspace_token,
    )?
    else {
        return Err(VfsError::AuthError {
            message: "invalid workspace MCP token".to_string(),
        });
    };

    let scope = SessionScope::new(
        valid.token.read_prefixes.iter().map(String::as_str),
        valid.token.write_prefixes.iter().map(String::as_str),
    )?;
    non_root_session(
        db.session_for_uid(valid.token.agent_uid)
            .await?
            .with_scope(scope),
    )
}

impl McpServer {
    fn tool_defs() -> Vec<Tool> {
        vec![
            make_tool(
                "read_file",
                "Read a file by path",
                serde_json::json!({
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            ),
            make_tool(
                "write_file",
                "Write text content to a file (creates if needed)",
                serde_json::json!({
                    "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
                    "required": ["path", "content"]
                }),
            ),
            make_tool(
                "list_directory",
                "List files in a directory",
                serde_json::json!({
                    "properties": {"path": {"type": "string"}}
                }),
            ),
            make_tool(
                "search_files",
                "Search file contents with a regex pattern",
                serde_json::json!({
                    "properties": {"pattern": {"type": "string"}, "path": {"type": "string"}, "recursive": {"type": "boolean"}},
                    "required": ["pattern"]
                }),
            ),
            make_tool(
                "find_files",
                "Find files by glob pattern",
                serde_json::json!({
                    "properties": {"path": {"type": "string"}, "name": {"type": "string"}}
                }),
            ),
            make_tool(
                "create_directory",
                "Create a directory (with parents)",
                serde_json::json!({
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            ),
            make_tool(
                "delete_file",
                "Delete a file or directory",
                serde_json::json!({
                    "properties": {"path": {"type": "string"}, "recursive": {"type": "boolean"}},
                    "required": ["path"]
                }),
            ),
            make_tool(
                "move_file",
                "Move or rename a file/directory",
                serde_json::json!({
                    "properties": {"source": {"type": "string"}, "destination": {"type": "string"}},
                    "required": ["source", "destination"]
                }),
            ),
            make_tool(
                "commit",
                "Commit current filesystem state",
                serde_json::json!({
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"]
                }),
            ),
            make_tool(
                "get_history",
                "Show commit history",
                serde_json::json!({
                    "properties": {}
                }),
            ),
            make_tool(
                "revert",
                "Revert to a previous commit",
                serde_json::json!({
                    "properties": {"hash": {"type": "string"}},
                    "required": ["hash"]
                }),
            ),
        ]
    }

    async fn handle_tool(&self, name: &str, args: &serde_json::Value) -> Result<String, String> {
        match name {
            "read_file" => {
                let path = args["path"].as_str().ok_or("missing path")?;
                let path = vfs_path(path);
                let content = self
                    .db
                    .cat_as(&path, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(String::from_utf8_lossy(&content).into_owned())
            }
            "write_file" => {
                let path = args["path"].as_str().ok_or("missing path")?;
                let content = args["content"].as_str().ok_or("missing content")?;
                let path = vfs_path(path);
                self.db
                    .write_file_as(&path, content.as_bytes().to_vec(), &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("Written {} bytes to {path}", content.len()))
            }
            "list_directory" => {
                let path = args.get("path").and_then(|v| v.as_str()).map(vfs_path);
                let entries = self
                    .db
                    .ls_as(path.as_deref(), &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut out = String::new();
                for e in &entries {
                    let suffix = if e.is_dir { "/" } else { "" };
                    out.push_str(&format!("{}{suffix}\n", e.name));
                }
                Ok(out)
            }
            "search_files" => {
                let pattern = args["pattern"].as_str().ok_or("missing pattern")?;
                let path = args.get("path").and_then(|v| v.as_str()).map(vfs_path);
                let recursive = args
                    .get("recursive")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                let results = self
                    .db
                    .grep_as(pattern, path.as_deref(), recursive, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut out = String::new();
                for r in &results {
                    out.push_str(&format!("{}:{}: {}\n", r.file, r.line_num, r.line));
                }
                if out.is_empty() {
                    out = "No matches found.".to_string();
                }
                Ok(out)
            }
            "find_files" => {
                let path = args.get("path").and_then(|v| v.as_str()).map(vfs_path);
                let name = args.get("name").and_then(|v| v.as_str());
                let results = self
                    .db
                    .find_as(path.as_deref(), name, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(results.join("\n"))
            }
            "create_directory" => {
                let path = args["path"].as_str().ok_or("missing path")?;
                let path = vfs_path(path);
                self.db
                    .mkdir_p_as(&path, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("Created directory: {path}"))
            }
            "delete_file" => {
                let path = args["path"].as_str().ok_or("missing path")?;
                let path = vfs_path(path);
                let recursive = args
                    .get("recursive")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                self.db
                    .rm_as(&path, recursive, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("Deleted: {path}"))
            }
            "move_file" => {
                let src = args["source"].as_str().ok_or("missing source")?;
                let dst = args["destination"].as_str().ok_or("missing destination")?;
                let src = vfs_path(src);
                let dst = vfs_path(dst);
                self.db
                    .mv_as(&src, &dst, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("Moved {src} -> {dst}"))
            }
            "commit" => {
                let message = args["message"].as_str().ok_or("missing message")?;
                let hash = self
                    .db
                    .commit_as(message, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("[{hash}] {message}"))
            }
            "get_history" => {
                let commits = self
                    .db
                    .vcs_log_as(&self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                if commits.is_empty() {
                    return Ok("No commits yet.".to_string());
                }
                let mut out = String::new();
                for c in &commits {
                    out.push_str(&format!(
                        "{} {} {}\n",
                        c.id.short_hex(),
                        c.author,
                        c.message
                    ));
                }
                Ok(out)
            }
            "revert" => {
                let hash = args["hash"].as_str().ok_or("missing hash")?;
                self.db
                    .revert_as(hash, &self.session)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("Reverted to {hash}"))
            }
            _ => Err(format!("unknown tool: {name}")),
        }
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        let mut caps = ServerCapabilities::default();
        caps.tools = Some(ToolsCapability { list_changed: None });
        caps.resources = Some(ResourcesCapability {
            subscribe: None,
            list_changed: None,
        });
        InitializeResult::new(caps).with_instructions(
            "stratum is a versioned virtual filesystem with Git-like history. \
             Use tools to read, write, search, and manage files. \
             Markdown-only filename enforcement is available only when the server is \
             started with STRATUM_COMPAT_TARGET=markdown.",
        )
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        async {
            let mut result = ListToolsResult::default();
            result.tools = Self::tool_defs();
            Ok(result)
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        async move {
            let args = serde_json::to_value(&request.arguments).unwrap_or_default();
            match self.handle_tool(&request.name, &args).await {
                Ok(text) => {
                    let mut result = CallToolResult::default();
                    result.content = vec![Content::text(text)];
                    result.is_error = Some(false);
                    Ok(result)
                }
                Err(e) => {
                    let mut result = CallToolResult::default();
                    result.content = vec![Content::text(e)];
                    result.is_error = Some(true);
                    Ok(result)
                }
            }
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, McpError>> + Send + '_ {
        async {
            let resource = RawResource::new("stratum://tree", "Directory Tree")
                .with_description("Full directory tree of the filesystem")
                .with_mime_type("text/plain");
            let mut result = ListResourcesResult::default();
            result.resources = vec![Annotated::new(resource, None)];
            Ok(result)
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResult, McpError>> + Send + '_ {
        async move {
            let uri = request.uri.as_str();
            if uri == "stratum://tree" {
                let tree = self
                    .db
                    .tree_as(None, &self.session)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    tree, uri,
                )]))
            } else if let Some(path) = uri.strip_prefix("stratum://files/") {
                let path = vfs_path(path);
                let content = self
                    .db
                    .cat_as(&path, &self.session)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    String::from_utf8_lossy(&content).into_owned(),
                    uri,
                )]))
            } else {
                Err(McpError::invalid_params(
                    format!("unknown resource: {uri}"),
                    None,
                ))
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stratum=info".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = Config::from_env();
    tracing::info!(data_dir = %config.data_dir.display(), "starting stratum MCP server");

    let db = StratumDb::open(config).expect("failed to open database");
    let session = match mcp_session_from_env(&db).await {
        Ok(session) => session,
        Err(e) => {
            tracing::error!("{e}");
            std::process::exit(1);
        }
    };
    let _save_handle = db.spawn_auto_save();

    let server = McpServer { db, session };
    let transport = rmcp::transport::io::stdio();
    let service = server
        .serve(transport)
        .await
        .expect("failed to start MCP server");
    service.waiting().await.expect("MCP server error");
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratum::auth::session::Session;
    use stratum::persist::MemoryPersistenceBackend;
    use stratum::workspace::{LocalWorkspaceMetadataStore, WorkspaceMetadataStore};
    use uuid::Uuid;

    #[tokio::test]
    async fn get_history_uses_session_gate() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch /history.md", &mut root)
            .await
            .unwrap();
        db.commit("history", "root").await.unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let bob = db.login("bob").await.unwrap();

        let server = McpServer { db, session: bob };
        let err = server
            .handle_tool("get_history", &serde_json::json!({}))
            .await
            .expect_err("non-admin MCP session must not read history");

        assert!(err.contains("permission denied"));
    }

    fn temp_data_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir()
            .join("stratum-mcp-tests")
            .join(format!("{name}-{}", Uuid::new_v4()))
    }

    fn test_db(name: &str) -> StratumDb {
        let data_dir = temp_data_dir(name);
        let workspace_metadata_path = data_dir.join(".vfs").join("workspaces.bin");
        let config = Config::default()
            .with_data_dir(&data_dir)
            .with_workspace_metadata_path(workspace_metadata_path);
        StratumDb::open_with_backend(config, Arc::new(MemoryPersistenceBackend)).unwrap()
    }

    fn extract_agent_token(output: &str) -> String {
        output
            .lines()
            .last()
            .expect("agent token line")
            .trim()
            .to_string()
    }

    fn auth_env(
        token: Option<String>,
        user: Option<String>,
        workspace_id: Option<String>,
        workspace_token: Option<String>,
    ) -> McpAuthEnv {
        McpAuthEnv {
            token,
            user,
            workspace_id,
            workspace_token,
        }
    }

    fn assert_auth_error(err: VfsError) {
        assert!(matches!(err, VfsError::AuthError { .. }), "{err}");
    }

    #[tokio::test]
    async fn mcp_session_workspace_env_resolves_scoped_non_root_session() {
        let db = test_db("scoped-session");
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/read", &root).await.unwrap();
        db.mkdir_p_as("/demo/write", &root).await.unwrap();
        db.mkdir_p_as("/demo/outside", &root).await.unwrap();
        db.write_file_as("/demo/read/allowed.txt", b"readable".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/demo/outside/secret.txt", b"secret".to_vec(), &root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/write", &mut root)
            .await
            .unwrap();

        let store =
            LocalWorkspaceMetadataStore::open(db.config().workspace_metadata_path()).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "ci-token",
                agent.uid,
                vec!["/demo/read".to_string()],
                vec!["/demo/write".to_string()],
            )
            .await
            .unwrap();
        drop(store);

        let session = mcp_session_from_auth_env(
            &db,
            auth_env(
                None,
                None,
                Some(workspace.id.to_string()),
                Some(issued.raw_secret),
            ),
        )
        .await
        .unwrap();

        assert_eq!(session.uid, agent.uid);
        assert_eq!(session.username, "ci-agent");
        assert!(!session.is_root());
        assert!(session.scope.is_some());

        let server = McpServer { db, session };
        let read_allowed = server
            .handle_tool(
                "read_file",
                &serde_json::json!({"path": "/demo/read/allowed.txt"}),
            )
            .await
            .unwrap();
        assert_eq!(read_allowed, "readable");

        let read_denied = server
            .handle_tool(
                "read_file",
                &serde_json::json!({"path": "/demo/outside/secret.txt"}),
            )
            .await
            .expect_err("workspace scope must block reads outside read prefixes");
        assert!(read_denied.contains("permission denied"));

        let write_denied = server
            .handle_tool(
                "write_file",
                &serde_json::json!({"path": "/demo/read/new.txt", "content": "blocked"}),
            )
            .await
            .expect_err("workspace scope must block writes outside write prefixes");
        assert!(write_denied.contains("permission denied"));

        server
            .handle_tool(
                "write_file",
                &serde_json::json!({"path": "/demo/write/new.txt", "content": "created"}),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mcp_session_workspace_env_reads_metadata_without_taking_writer_lock() {
        let db = test_db("read-only-workspace-metadata");
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();

        let store =
            LocalWorkspaceMetadataStore::open(db.config().workspace_metadata_path()).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let issued = store
            .issue_workspace_token(workspace.id, "ci-token", agent.uid)
            .await
            .unwrap();

        let session = mcp_session_from_auth_env(
            &db,
            auth_env(
                None,
                None,
                Some(workspace.id.to_string()),
                Some(issued.raw_secret),
            ),
        )
        .await
        .expect("MCP session resolution should not require acquiring the writer lock");

        assert_eq!(session.uid, agent.uid);
        assert!(session.scope.is_some());
        drop(store);
    }

    #[tokio::test]
    async fn mcp_session_workspace_env_wrong_token_does_not_fallback_to_global_token() {
        let db = test_db("wrong-workspace-token");
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();

        let store =
            LocalWorkspaceMetadataStore::open(db.config().workspace_metadata_path()).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        store
            .issue_workspace_token(workspace.id, "ci-token", agent.uid)
            .await
            .unwrap();
        drop(store);

        let err = mcp_session_from_auth_env(
            &db,
            auth_env(
                Some(raw_agent_token),
                None,
                Some(workspace.id.to_string()),
                Some("wrong-token".to_string()),
            ),
        )
        .await
        .expect_err("wrong workspace token must not fall back to global token auth");

        assert_auth_error(err);
    }

    #[tokio::test]
    async fn mcp_session_workspace_env_partial_pair_is_rejected() {
        let db = test_db("partial-workspace-env");

        let id_only = mcp_session_from_auth_env(
            &db,
            auth_env(None, None, Some(Uuid::new_v4().to_string()), None),
        )
        .await
        .expect_err("workspace id without workspace token must be rejected");
        assert_auth_error(id_only);

        let token_only =
            mcp_session_from_auth_env(&db, auth_env(None, None, None, Some("secret".to_string())))
                .await
                .expect_err("workspace token without workspace id must be rejected");
        assert_auth_error(token_only);
    }

    #[tokio::test]
    async fn mcp_session_workspace_env_invalid_workspace_id_is_rejected() {
        let db = test_db("invalid-workspace-id");

        let err = mcp_session_from_auth_env(
            &db,
            auth_env(
                None,
                None,
                Some("not-a-uuid".to_string()),
                Some("secret".to_string()),
            ),
        )
        .await
        .expect_err("invalid workspace id must be rejected");

        assert_auth_error(err);
    }

    #[tokio::test]
    async fn mcp_session_workspace_env_root_backed_token_is_rejected() {
        let db = test_db("root-workspace-token");
        let root = Session::root();

        let store =
            LocalWorkspaceMetadataStore::open(db.config().workspace_metadata_path()).unwrap();
        let workspace = store.create_workspace("root-demo", "/demo").await.unwrap();
        let issued = store
            .issue_workspace_token(workspace.id, "root-token", root.uid)
            .await
            .unwrap();
        drop(store);

        let err = mcp_session_from_auth_env(
            &db,
            auth_env(
                None,
                None,
                Some(workspace.id.to_string()),
                Some(issued.raw_secret),
            ),
        )
        .await
        .expect_err("workspace token resolving to root must be rejected");

        assert_auth_error(err);
    }
}
