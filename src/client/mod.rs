use reqwest::Client;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::backend::RepoId;
use crate::error::VfsError;

#[derive(Clone)]
pub enum ClientAuth {
    Root,
    User(String),
    Bearer(String),
    WorkspaceBearer { workspace_id: Uuid, secret: String },
}

impl fmt::Debug for ClientAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => f.write_str("Root"),
            Self::User(username) => f.debug_tuple("User").field(username).finish(),
            Self::Bearer(_) => f.debug_tuple("Bearer").field(&"<redacted>").finish(),
            Self::WorkspaceBearer { workspace_id, .. } => f
                .debug_struct("WorkspaceBearer")
                .field("workspace_id", workspace_id)
                .field("secret", &"<redacted>")
                .finish(),
        }
    }
}

#[derive(Clone)]
pub struct StratumClient {
    base_url: String,
    client: Client,
    auth: ClientAuth,
    repo: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClientLsResponse {
    pub path: String,
    pub entries: Vec<ClientLsEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ClientLsEntry {
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct ClientGrepResult {
    pub file: String,
    pub line_num: usize,
    pub line: String,
}

#[derive(Debug, Deserialize)]
pub struct ClientGrepResponse {
    pub results: Vec<ClientGrepResult>,
    pub count: usize,
}

#[derive(Debug, Deserialize)]
pub struct ClientFindResponse {
    pub results: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Deserialize)]
pub struct ClientCommit {
    pub hash: String,
    pub message: String,
    pub author: String,
}

#[derive(Debug, Deserialize)]
pub struct ClientLogResponse {
    pub commits: Vec<ClientCommitLog>,
}

#[derive(Debug, Deserialize)]
pub struct ClientCommitLog {
    pub hash: String,
    pub message: String,
    pub author: String,
    pub timestamp: u64,
}

#[derive(Serialize)]
struct IssueWorkspaceTokenRequest<'a> {
    name: &'a str,
    agent_token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_prefixes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_prefixes: Option<Vec<String>>,
}

impl StratumClient {
    pub fn new(base_url: impl Into<String>, auth: ClientAuth) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: Client::new(),
            auth,
            repo: None,
        }
    }

    pub fn with_repo(mut self, repo: Option<String>) -> Self {
        self.repo = repo;
        self
    }

    fn headers(&self) -> Result<HeaderMap, VfsError> {
        let mut headers = HeaderMap::new();
        match &self.auth {
            ClientAuth::Root => {
                let mut value = HeaderValue::from_static("User root");
                value.set_sensitive(true);
                headers.insert(AUTHORIZATION, value);
            }
            ClientAuth::User(username) => {
                headers.insert(
                    AUTHORIZATION,
                    sensitive_header_value("username", &format!("User {username}"))?,
                );
            }
            ClientAuth::Bearer(token) => {
                headers.insert(
                    AUTHORIZATION,
                    sensitive_header_value("bearer", &format!("Bearer {token}"))?,
                );
            }
            ClientAuth::WorkspaceBearer {
                workspace_id,
                secret,
            } => {
                headers.insert(
                    AUTHORIZATION,
                    sensitive_header_value("workspace bearer", &format!("Bearer {secret}"))?,
                );
                headers.insert(
                    "x-stratum-workspace",
                    HeaderValue::from_str(&workspace_id.to_string()).map_err(|e| {
                        VfsError::InvalidArgs {
                            message: format!("invalid workspace header: {e}"),
                        }
                    })?,
                );
            }
        }
        if let Some(repo) = &self.repo {
            RepoId::new(repo.clone()).map_err(|_| VfsError::InvalidArgs {
                message: "invalid repo header".to_string(),
            })?;
            headers.insert(
                "x-stratum-repo",
                HeaderValue::from_str(repo).map_err(|e| VfsError::InvalidArgs {
                    message: format!("invalid repo header: {e}"),
                })?,
            );
        }
        Ok(headers)
    }

    pub async fn health(&self) -> Result<serde_json::Value, VfsError> {
        self.json(self.client.get(format!("{}/health", self.base_url)))
            .await
    }

    pub async fn list_directory(&self, path: &str) -> Result<ClientLsResponse, VfsError> {
        let path = path.trim_start_matches('/');
        let url = if path.is_empty() {
            format!("{}/fs", self.base_url)
        } else {
            format!("{}/fs/{}", self.base_url, path)
        };
        self.json(self.client.get(url)).await
    }

    pub async fn read_file(&self, path: &str) -> Result<String, VfsError> {
        let url = format!("{}/fs/{}", self.base_url, path.trim_start_matches('/'));
        let response = self
            .client
            .get(url)
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Self::ensure_success(response.status(), response.text().await.unwrap_or_default())
    }

    pub async fn write_file(
        &self,
        path: &str,
        content: String,
    ) -> Result<serde_json::Value, VfsError> {
        let url = format!("{}/fs/{}", self.base_url, path.trim_start_matches('/'));
        self.json(self.client.put(url).headers(self.headers()?).body(content))
            .await
    }

    pub async fn grep(
        &self,
        pattern: &str,
        path: Option<&str>,
    ) -> Result<ClientGrepResponse, VfsError> {
        let mut url = format!(
            "{}/search/grep?pattern={}&recursive=true",
            self.base_url,
            urlencoding::encode(pattern)
        );
        if let Some(path) = path {
            url.push_str("&path=");
            url.push_str(&urlencoding::encode(path));
        }
        self.json(self.client.get(url)).await
    }

    pub async fn find(
        &self,
        pattern: &str,
        path: Option<&str>,
    ) -> Result<ClientFindResponse, VfsError> {
        let mut url = format!(
            "{}/search/find?name={}",
            self.base_url,
            urlencoding::encode(pattern)
        );
        if let Some(path) = path {
            url.push_str("&path=");
            url.push_str(&urlencoding::encode(path));
        }
        self.json(self.client.get(url)).await
    }

    pub async fn tree(&self, path: Option<&str>) -> Result<String, VfsError> {
        let url = match path {
            Some(path) if !path.trim_matches('/').is_empty() => {
                format!("{}/tree/{}", self.base_url, path.trim_start_matches('/'))
            }
            _ => format!("{}/tree", self.base_url),
        };
        let response = self
            .client
            .get(url)
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Self::ensure_success(response.status(), response.text().await.unwrap_or_default())
    }

    pub async fn commit(&self, message: &str) -> Result<ClientCommit, VfsError> {
        self.json(
            self.client
                .post(format!("{}/vcs/commit", self.base_url))
                .headers(self.headers()?)
                .json(&serde_json::json!({ "message": message })),
        )
        .await
    }

    pub async fn log(&self) -> Result<ClientLogResponse, VfsError> {
        self.json(self.client.get(format!("{}/vcs/log", self.base_url)))
            .await
    }

    pub async fn revert(&self, hash: &str) -> Result<serde_json::Value, VfsError> {
        self.json(
            self.client
                .post(format!("{}/vcs/revert", self.base_url))
                .headers(self.headers()?)
                .json(&serde_json::json!({ "hash": hash })),
        )
        .await
    }

    pub async fn status(&self) -> Result<String, VfsError> {
        let response = self
            .client
            .get(format!("{}/vcs/status", self.base_url))
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Self::ensure_success(response.status(), response.text().await.unwrap_or_default())
    }

    pub async fn diff(&self, path: Option<&str>) -> Result<String, VfsError> {
        let mut url = format!("{}/vcs/diff", self.base_url);
        if let Some(path) = path {
            url.push_str("?path=");
            url.push_str(&urlencoding::encode(path));
        }
        let response = self
            .client
            .get(url)
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Self::ensure_success(response.status(), response.text().await.unwrap_or_default())
    }

    pub async fn list_workspaces(&self) -> Result<serde_json::Value, VfsError> {
        self.json(self.client.get(format!("{}/workspaces", self.base_url)))
            .await
    }

    pub async fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
    ) -> Result<serde_json::Value, VfsError> {
        self.json(
            self.client
                .post(format!("{}/workspaces", self.base_url))
                .headers(self.headers()?)
                .json(&serde_json::json!({ "name": name, "root_path": root_path })),
        )
        .await
    }

    pub async fn issue_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_token: &str,
    ) -> Result<serde_json::Value, VfsError> {
        self.issue_scoped_workspace_token(workspace_id, name, agent_token, None, None)
            .await
    }

    pub async fn issue_scoped_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_token: &str,
        read_prefixes: Option<Vec<String>>,
        write_prefixes: Option<Vec<String>>,
    ) -> Result<serde_json::Value, VfsError> {
        self.json(
            self.client
                .post(format!(
                    "{}/workspaces/{workspace_id}/tokens",
                    self.base_url
                ))
                .headers(self.headers()?)
                .json(&IssueWorkspaceTokenRequest {
                    name,
                    agent_token,
                    read_prefixes,
                    write_prefixes,
                }),
        )
        .await
    }

    async fn json<T>(&self, builder: reqwest::RequestBuilder) -> Result<T, VfsError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = builder
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        if !status.is_success() {
            return Err(VfsError::InvalidArgs { message: body });
        }
        serde_json::from_str(&body).map_err(|e| VfsError::CorruptStore {
            message: format!("invalid server response: {e}"),
        })
    }

    fn ensure_success(status: reqwest::StatusCode, body: String) -> Result<String, VfsError> {
        if status.is_success() {
            Ok(body)
        } else {
            Err(VfsError::InvalidArgs { message: body })
        }
    }
}

fn sensitive_header_value(label: &str, value: &str) -> Result<HeaderValue, VfsError> {
    let mut value = HeaderValue::from_str(value).map_err(|e| VfsError::InvalidArgs {
        message: format!("invalid {label} header: {e}"),
    })?;
    value.set_sensitive(true);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, State};
    use axum::http::HeaderMap as AxumHeaderMap;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::Value;
    use std::sync::{Arc, Mutex};

    const WORKSPACE_ID: &str = "11111111-1111-1111-1111-111111111111";
    const WORKSPACE_SECRET: &str = "workspace-secret";
    const REPO_ID: &str = "tenant-a";
    type HeaderRecords = Arc<Mutex<Vec<Value>>>;

    async fn spawn_issue_token_echo_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/workspaces/{workspace_id}/tokens",
            post(
                |Path(workspace_id): Path<Uuid>, Json(body): Json<serde_json::Value>| async move {
                    Json(serde_json::json!({
                        "workspace_id": workspace_id,
                        "request": body,
                    }))
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    async fn spawn_header_echo_server() -> (String, tokio::task::JoinHandle<()>, HeaderRecords) {
        async fn ls_echo(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> Json<Value> {
            record_headers(&records, headers);
            Json(serde_json::json!({ "path": "", "entries": [] }))
        }

        async fn grep_echo(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> Json<Value> {
            record_headers(&records, headers);
            Json(serde_json::json!({ "results": [], "count": 0 }))
        }

        async fn find_echo(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> Json<Value> {
            record_headers(&records, headers);
            Json(serde_json::json!({ "results": [], "count": 0 }))
        }

        async fn log_echo(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> Json<Value> {
            record_headers(&records, headers);
            Json(serde_json::json!({ "commits": [] }))
        }

        async fn value_echo(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> Json<Value> {
            Json(record_headers(&records, headers))
        }

        async fn text_echo(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> (AxumHeaderMap, String) {
            let mut response_headers = AxumHeaderMap::new();
            response_headers.insert("content-type", "text/plain".parse().unwrap());
            (
                response_headers,
                record_headers(&records, headers).to_string(),
            )
        }

        async fn stable_501(
            State(records): State<HeaderRecords>,
            headers: AxumHeaderMap,
        ) -> (axum::http::StatusCode, Json<Value>) {
            record_headers(&records, headers);
            (
                axum::http::StatusCode::NOT_IMPLEMENTED,
                Json(serde_json::json!({
                    "error": "stratum: operation not supported: durable-cloud route is not supported yet"
                })),
            )
        }

        let records = HeaderRecords::default();
        let app = Router::new()
            .route("/fs", get(ls_echo))
            .route("/search/grep", get(grep_echo))
            .route("/search/find", get(find_echo))
            .route("/vcs/log", get(log_echo))
            .route("/workspaces", get(value_echo).post(value_echo))
            .route("/workspaces/{workspace_id}/tokens", post(value_echo))
            .route("/fs/{*path}", get(text_echo).put(stable_501))
            .route("/tree", get(text_echo))
            .route("/tree/{*path}", get(text_echo))
            .route("/vcs/status", get(text_echo))
            .route("/vcs/diff", get(text_echo))
            .route("/vcs/commit", post(stable_501))
            .route("/vcs/revert", post(stable_501))
            .with_state(records.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle, records)
    }

    fn workspace_repo_client(base_url: String) -> StratumClient {
        StratumClient::new(
            base_url,
            ClientAuth::WorkspaceBearer {
                workspace_id: Uuid::parse_str(WORKSPACE_ID).unwrap(),
                secret: WORKSPACE_SECRET.to_string(),
            },
        )
        .with_repo(Some(REPO_ID.to_string()))
    }

    fn assert_workspace_repo_headers(value: &Value) {
        assert_eq!(
            value.get("authorization"),
            Some(&Value::String(format!("Bearer {WORKSPACE_SECRET}")))
        );
        assert_eq!(
            value.get("x-stratum-workspace"),
            Some(&Value::String(WORKSPACE_ID.to_string()))
        );
        assert_eq!(
            value.get("x-stratum-repo"),
            Some(&Value::String(REPO_ID.to_string()))
        );
    }

    fn record_headers(records: &HeaderRecords, headers: AxumHeaderMap) -> Value {
        let value = serde_json::json!({
            "authorization": header_value(&headers, "authorization"),
            "x-stratum-workspace": header_value(&headers, "x-stratum-workspace"),
            "x-stratum-repo": header_value(&headers, "x-stratum-repo"),
        });
        records.lock().unwrap().push(value.clone());
        value
    }

    fn recorded_headers(records: &HeaderRecords) -> Vec<Value> {
        records.lock().unwrap().clone()
    }

    fn header_value(headers: &AxumHeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }

    #[test]
    fn root_auth_uses_explicit_root_identity_header() {
        let client = StratumClient::new("http://127.0.0.1:3000", ClientAuth::Root);

        let headers = client.headers().unwrap();
        let auth = headers.get(AUTHORIZATION).unwrap();

        assert_eq!(auth.to_str().ok(), Some("User root"));
        assert!(auth.is_sensitive());
    }

    #[test]
    fn client_auth_debug_redacts_bearer_secrets() {
        let bearer_debug = format!("{:?}", ClientAuth::Bearer("raw-bearer-secret".to_string()));
        assert!(bearer_debug.contains("redacted"));
        assert!(!bearer_debug.contains("raw-bearer-secret"));

        let workspace_id = Uuid::parse_str(WORKSPACE_ID).unwrap();
        let workspace_debug = format!(
            "{:?}",
            ClientAuth::WorkspaceBearer {
                workspace_id,
                secret: "raw-workspace-secret".to_string(),
            }
        );
        assert!(workspace_debug.contains("redacted"));
        assert!(workspace_debug.contains(WORKSPACE_ID));
        assert!(!workspace_debug.contains("raw-workspace-secret"));
    }

    #[test]
    fn bearer_authorization_headers_are_marked_sensitive() {
        for auth in [
            ClientAuth::Bearer("raw-bearer-secret".to_string()),
            ClientAuth::WorkspaceBearer {
                workspace_id: Uuid::parse_str(WORKSPACE_ID).unwrap(),
                secret: "raw-workspace-secret".to_string(),
            },
        ] {
            let client = StratumClient::new("http://127.0.0.1:3000", auth);

            let headers = client.headers().unwrap();
            let auth = headers.get(AUTHORIZATION).unwrap();

            assert!(auth.is_sensitive());
        }
    }

    #[test]
    fn repo_header_rejects_invalid_repo_id_without_echoing_value() {
        for raw_repo in ["", "   ", " tenant-a ", "not valid repo"] {
            let client = StratumClient::new("http://127.0.0.1:3000", ClientAuth::Root)
                .with_repo(Some(raw_repo.to_string()));

            let err = client.headers().expect_err("invalid repo id should fail");

            let VfsError::InvalidArgs { message } = err else {
                panic!("invalid repo id should return InvalidArgs");
            };
            assert_eq!(message, "invalid repo header");
            if !raw_repo.is_empty() {
                assert!(!message.contains(raw_repo));
            }
        }
    }

    #[tokio::test]
    async fn json_methods_send_workspace_bearer_and_repo_headers() {
        let (base_url, server, records) = spawn_header_echo_server().await;
        let client = workspace_repo_client(base_url);

        client.list_directory("/").await.unwrap();
        client.grep("needle", Some("/src")).await.unwrap();
        client.find("*.rs", Some("/src")).await.unwrap();
        client.log().await.unwrap();
        let workspaces = client.list_workspaces().await.unwrap();
        let workspace_create = client.create_workspace("demo", "/tmp/demo").await.unwrap();
        let token = client
            .issue_workspace_token(
                Uuid::parse_str(WORKSPACE_ID).unwrap(),
                "ci-token",
                "agent-secret",
            )
            .await
            .unwrap();
        server.abort();

        let values = recorded_headers(&records);
        assert_eq!(values.len(), 7);
        for value in values {
            assert_workspace_repo_headers(&value);
        }
        assert_workspace_repo_headers(&workspaces);
        assert_workspace_repo_headers(&workspace_create);
        assert_workspace_repo_headers(&token);
    }

    #[tokio::test]
    async fn text_methods_send_workspace_bearer_and_repo_headers() {
        let (base_url, server, _records) = spawn_header_echo_server().await;
        let client = workspace_repo_client(base_url);

        let cat: Value =
            serde_json::from_str(&client.read_file("/README.md").await.unwrap()).unwrap();
        let tree: Value = serde_json::from_str(&client.tree(Some("/src")).await.unwrap()).unwrap();
        let status: Value = serde_json::from_str(&client.status().await.unwrap()).unwrap();
        let diff: Value =
            serde_json::from_str(&client.diff(Some("/src/main.rs")).await.unwrap()).unwrap();
        server.abort();

        for value in [cat, tree, status, diff] {
            assert_workspace_repo_headers(&value);
        }
    }

    #[tokio::test]
    async fn mutations_send_repo_context_and_return_stable_501_body() {
        let (base_url, server, records) = spawn_header_echo_server().await;
        let client = workspace_repo_client(base_url);

        let write_err = client
            .write_file("/README.md", "updated".to_string())
            .await
            .expect_err("server write 501 should surface through the client");
        let commit_err = client
            .commit("durable mutation")
            .await
            .expect_err("server commit 501 should surface through the client");
        let revert_err = client
            .revert("abc123")
            .await
            .expect_err("server revert 501 should surface through the client");
        server.abort();

        for err in [write_err, commit_err, revert_err] {
            let VfsError::InvalidArgs { message } = err else {
                panic!("expected InvalidArgs for non-success response");
            };
            assert_eq!(
                serde_json::from_str::<Value>(&message).unwrap(),
                serde_json::json!({
                    "error": "stratum: operation not supported: durable-cloud route is not supported yet"
                })
            );
        }
        let values = recorded_headers(&records);
        assert_eq!(values.len(), 3);
        for value in values {
            assert_workspace_repo_headers(&value);
        }
    }

    #[tokio::test]
    async fn issue_workspace_token_omits_scope_prefixes_for_compatibility() {
        let (base_url, server) = spawn_issue_token_echo_server().await;
        let client = StratumClient::new(base_url, ClientAuth::Root);
        let workspace_id = Uuid::new_v4();

        let value = client
            .issue_workspace_token(workspace_id, "ci-token", "agent-secret")
            .await
            .unwrap();
        server.abort();

        let request = value.get("request").unwrap();
        assert_eq!(request.get("name"), Some(&serde_json::json!("ci-token")));
        assert_eq!(
            request.get("agent_token"),
            Some(&serde_json::json!("agent-secret"))
        );
        assert!(request.get("read_prefixes").is_none());
        assert!(request.get("write_prefixes").is_none());
    }

    #[tokio::test]
    async fn issue_scoped_workspace_token_sends_custom_prefixes() {
        let (base_url, server) = spawn_issue_token_echo_server().await;
        let client = StratumClient::new(base_url, ClientAuth::Root);
        let workspace_id = Uuid::new_v4();

        let value = client
            .issue_scoped_workspace_token(
                workspace_id,
                "ci-token",
                "agent-secret",
                Some(vec!["/demo/read".to_string(), "/demo/shared".to_string()]),
                Some(vec!["/demo/write".to_string()]),
            )
            .await
            .unwrap();
        server.abort();

        let request = value.get("request").unwrap();
        assert_eq!(
            request.get("read_prefixes"),
            Some(&serde_json::json!(["/demo/read", "/demo/shared"]))
        );
        assert_eq!(
            request.get("write_prefixes"),
            Some(&serde_json::json!(["/demo/write"]))
        );
    }
}
