use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::Serialize;

use super::AppState;
use super::middleware::session_from_headers;
use crate::auth::session::Session;
use crate::error::VfsError;
use crate::runs::{
    RUNS_ROOT, RunRecord, RunRecordContext, RunRecordFileKind, RunRecordInput, RunRecordLayout,
};

#[derive(Debug, Clone)]
struct ResolvedRunRecordLayout {
    runs_root: String,
    root: String,
    prompt: String,
    command: String,
    stdout: String,
    stderr: String,
    result: String,
    metadata: String,
    artifacts: String,
}

impl ResolvedRunRecordLayout {
    fn new(session: &Session, layout: &RunRecordLayout) -> Result<Self, VfsError> {
        Ok(Self {
            runs_root: session.resolve_mounted_path(RUNS_ROOT)?,
            root: session.resolve_mounted_path(&layout.root)?,
            prompt: session.resolve_mounted_path(&layout.prompt)?,
            command: session.resolve_mounted_path(&layout.command)?,
            stdout: session.resolve_mounted_path(&layout.stdout)?,
            stderr: session.resolve_mounted_path(&layout.stderr)?,
            result: session.resolve_mounted_path(&layout.result)?,
            metadata: session.resolve_mounted_path(&layout.metadata)?,
            artifacts: session.resolve_mounted_path(&layout.artifacts)?,
        })
    }

    fn path_for_kind(&self, kind: RunRecordFileKind) -> &str {
        match kind {
            RunRecordFileKind::Prompt => &self.prompt,
            RunRecordFileKind::Command => &self.command,
            RunRecordFileKind::Stdout => &self.stdout,
            RunRecordFileKind::Stderr => &self.stderr,
            RunRecordFileKind::Result => &self.result,
            RunRecordFileKind::Metadata => &self.metadata,
        }
    }
}

#[derive(Debug, Serialize)]
struct CreateRunFilesResponse {
    prompt: String,
    command: String,
    stdout: String,
    stderr: String,
    result: String,
    metadata: String,
}

#[derive(Debug, Serialize)]
struct CreateRunResponse {
    run_id: String,
    root: String,
    artifacts: String,
    files: CreateRunFilesResponse,
}

impl CreateRunResponse {
    fn new(record: &RunRecord, session: &Session, resolved: &ResolvedRunRecordLayout) -> Self {
        Self {
            run_id: record.run_id.clone(),
            root: session.project_mounted_path(&resolved.root),
            artifacts: project_directory_path(session, &resolved.artifacts),
            files: CreateRunFilesResponse {
                prompt: session.project_mounted_path(&resolved.prompt),
                command: session.project_mounted_path(&resolved.command),
                stdout: session.project_mounted_path(&resolved.stdout),
                stderr: session.project_mounted_path(&resolved.stderr),
                result: session.project_mounted_path(&resolved.result),
                metadata: session.project_mounted_path(&resolved.metadata),
            },
        }
    }
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/runs", post(create_run))
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::AlreadyExists { .. } => StatusCode::CONFLICT,
        VfsError::InvalidArgs { .. } | VfsError::InvalidPath { .. } => StatusCode::BAD_REQUEST,
        _ => fallback,
    }
}

fn error_message(session: &Session, error: &VfsError) -> String {
    match error {
        VfsError::InvalidExtension { name } => format!(
            "stratum: markdown compatibility mode only supports .md files: '{}'",
            session.project_mounted_error_path(name)
        ),
        VfsError::NotFound { path } => format!(
            "stratum: no such file or directory: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::IsDirectory { path } => format!(
            "stratum: is a directory: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::NotDirectory { path } => format!(
            "stratum: not a directory: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::AlreadyExists { path } => format!(
            "stratum: already exists: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::NotEmpty { path } => format!(
            "stratum: directory not empty: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::InvalidPath { path } => format!(
            "stratum: invalid path: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::SymlinkLoop { path } => format!(
            "stratum: symlink loop: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::PermissionDenied { path } => format!(
            "stratum: permission denied: '{}'",
            session.project_mounted_error_path(path)
        ),
        _ => error.to_string(),
    }
}

fn err_json_for(
    session: &Session,
    error: &VfsError,
    fallback: StatusCode,
) -> axum::response::Response {
    err_json(error_status(error, fallback), error_message(session, error)).into_response()
}

fn err_json_partial_for(
    session: &Session,
    error: &VfsError,
    fallback: StatusCode,
    record: &RunRecord,
    resolved: &ResolvedRunRecordLayout,
) -> axum::response::Response {
    (
        error_status(error, fallback),
        Json(serde_json::json!({
            "error": error_message(session, error),
            "partial": true,
            "run_id": record.run_id.clone(),
            "root": session.project_mounted_path(&resolved.root),
        })),
    )
        .into_response()
}

fn project_directory_path(session: &Session, path: &str) -> String {
    let mut projected = session.project_mounted_path(path);
    if !projected.ends_with('/') {
        projected.push('/');
    }
    projected
}

fn validate_record_file_sizes(record: &RunRecord, max_file_size: usize) -> Result<(), VfsError> {
    for file in &record.files {
        let size = file.content.len();
        if size > max_file_size {
            return Err(VfsError::InvalidArgs {
                message: format!("{} size {} exceeds max {}", file.path, size, max_file_size),
            });
        }
    }

    Ok(())
}

async fn create_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<RunRecordInput>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let Some(mount) = session.mount() else {
        let error = VfsError::PermissionDenied {
            path: RUNS_ROOT.to_string(),
        };
        return err_json_for(&session, &error, StatusCode::FORBIDDEN);
    };

    let context = RunRecordContext {
        workspace_id: mount.workspace_id(),
        agent_uid: session.uid,
        agent_username: session.username.clone(),
        created_at: Utc::now(),
    };
    let record = match RunRecord::new(input, context) {
        Ok(record) => record,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    let resolved = match ResolvedRunRecordLayout::new(&session, &record.layout) {
        Ok(layout) => layout,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };

    if let Err(e) = validate_record_file_sizes(&record, state.db.config().max_file_size) {
        return err_json_for(&session, &e, StatusCode::BAD_REQUEST);
    }

    if let Err(e) = state.db.mkdir_p_as(&resolved.runs_root, &session).await {
        return err_json_for(&session, &e, StatusCode::BAD_REQUEST);
    }

    if let Err(e) = state.db.mkdir_as(&resolved.root, &session).await {
        return err_json_for(&session, &e, StatusCode::BAD_REQUEST);
    }

    if let Err(e) = state.db.mkdir_as(&resolved.artifacts, &session).await {
        return err_json_partial_for(&session, &e, StatusCode::BAD_REQUEST, &record, &resolved);
    }

    for file in &record.files {
        let path = resolved.path_for_kind(file.kind);
        if let Err(e) = state
            .db
            .write_file_as(path, file.content.as_bytes().to_vec(), &session)
            .await
        {
            return err_json_partial_for(&session, &e, StatusCode::BAD_REQUEST, &record, &resolved);
        }
    }

    (
        StatusCode::CREATED,
        Json(CreateRunResponse::new(&record, &session, &resolved)),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::server::ServerState;
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use axum::body::Bytes;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
        })
    }

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
        headers
    }

    fn bearer_headers(raw_secret: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {raw_secret}").parse().unwrap(),
        );
        headers
    }

    fn malformed_workspace_headers(raw_secret: &str) -> HeaderMap {
        let mut headers = bearer_headers(raw_secret);
        headers.insert("x-stratum-workspace", "not-a-uuid".parse().unwrap());
        headers
    }

    fn workspace_headers(workspace_id: Uuid, raw_secret: &str) -> HeaderMap {
        let mut headers = bearer_headers(raw_secret);
        headers.insert(
            "x-stratum-workspace",
            workspace_id.to_string().parse().unwrap(),
        );
        headers
    }

    fn extract_agent_token(output: &str) -> String {
        output
            .lines()
            .last()
            .expect("agent token line")
            .trim()
            .to_string()
    }

    async fn response_bytes(response: axum::response::Response) -> Bytes {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        serde_json::from_slice(&response_bytes(response).await).unwrap()
    }

    async fn workspace_state_with_token(
        db: StratumDb,
        workspace_root: &str,
        agent_uid: u32,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> (AppState, Uuid, String) {
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store
            .create_workspace("demo", workspace_root)
            .await
            .unwrap();
        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "run-writer",
                agent_uid,
                read_prefixes,
                write_prefixes,
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
        });
        (state, workspace.id, issued.raw_secret)
    }

    async fn prepare_workspace_db() -> (StratumDb, u32, String) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();

        db.mkdir_p_as("/demo/runs", &root).await.unwrap();
        db.execute_command("chmod 777 /demo/runs", &mut root)
            .await
            .unwrap();

        (db, agent.uid, raw_agent_token)
    }

    fn run_input(run_id: &str) -> RunRecordInput {
        let mut input = RunRecordInput::new(
            Some(run_id.to_string()),
            "Summarize the checkout incident",
            "cargo test --locked",
        );
        input.stdout = "ok".to_string();
        input.stderr = "warning".to_string();
        input.result = "completed".to_string();
        input.exit_code = Some(0);
        input.source_commit = Some("abc123".to_string());
        input
    }

    #[tokio::test]
    async fn workspace_bearer_creates_full_run_record_layout() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let response = create_run(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(run_input("run_123")),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response_json(response).await;
        assert_eq!(body.get("run_id"), Some(&serde_json::json!("run_123")));
        assert_eq!(body.get("root"), Some(&serde_json::json!("/runs/run_123")));
        assert_eq!(
            body.get("artifacts"),
            Some(&serde_json::json!("/runs/run_123/artifacts/"))
        );
        assert_eq!(
            body["files"].get("prompt"),
            Some(&serde_json::json!("/runs/run_123/prompt.md"))
        );
        assert_eq!(
            body["files"].get("command"),
            Some(&serde_json::json!("/runs/run_123/command.md"))
        );
        assert_eq!(
            body["files"].get("stdout"),
            Some(&serde_json::json!("/runs/run_123/stdout.md"))
        );
        assert_eq!(
            body["files"].get("stderr"),
            Some(&serde_json::json!("/runs/run_123/stderr.md"))
        );
        assert_eq!(
            body["files"].get("result"),
            Some(&serde_json::json!("/runs/run_123/result.md"))
        );
        assert_eq!(
            body["files"].get("metadata"),
            Some(&serde_json::json!("/runs/run_123/metadata.md"))
        );
        assert!(!body.to_string().contains("/demo/"));

        let artifact_info = state.db.stat("/demo/runs/run_123/artifacts").await.unwrap();
        assert_eq!(artifact_info.kind, "directory");
        assert_eq!(
            state.db.cat("/demo/runs/run_123/prompt.md").await.unwrap(),
            b"Summarize the checkout incident".to_vec()
        );
        assert_eq!(
            state.db.cat("/demo/runs/run_123/command.md").await.unwrap(),
            b"cargo test --locked".to_vec()
        );
        assert_eq!(
            state.db.cat("/demo/runs/run_123/stdout.md").await.unwrap(),
            b"ok".to_vec()
        );
        assert_eq!(
            state.db.cat("/demo/runs/run_123/stderr.md").await.unwrap(),
            b"warning".to_vec()
        );
        assert_eq!(
            state.db.cat("/demo/runs/run_123/result.md").await.unwrap(),
            b"completed".to_vec()
        );
        let metadata = String::from_utf8(
            state
                .db
                .cat("/demo/runs/run_123/metadata.md")
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(metadata.contains("run_id: \"run_123\""));
        assert!(metadata.contains(&format!("workspace_id: \"{workspace_id}\"")));
        assert!(metadata.contains("agent_uid: "));
        assert!(metadata.contains("agent_username: \"ci-agent\""));
        assert!(metadata.contains("exit_code: 0"));
        assert!(metadata.contains("source_commit: \"abc123\""));
        assert!(!metadata.contains("/demo"));
    }

    #[tokio::test]
    async fn unsafe_run_id_is_rejected_before_writes() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let response = create_run(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(run_input("../escape")),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(state.db.stat("/demo/runs/escape").await.is_err());
    }

    #[tokio::test]
    async fn duplicate_run_id_is_rejected_without_overwriting_existing_record() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let response = create_run(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut overwrite = run_input("run_123");
        overwrite.prompt = "replace the audit record".to_string();
        let response = create_run(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(overwrite),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("/runs/run_123"));
        assert_eq!(
            state.db.cat("/demo/runs/run_123/prompt.md").await.unwrap(),
            b"Summarize the checkout incident".to_vec()
        );
    }

    #[tokio::test]
    async fn oversized_run_file_is_rejected_before_creating_run_root() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let mut input = run_input("too_big");
        input.stdout = "x".repeat(state.db.config().max_file_size + 1);
        let response = create_run(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(input),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs/too_big/stdout.md"), "{error}");
        assert!(state.db.stat("/demo/runs/too_big").await.is_err());
    }

    #[tokio::test]
    async fn unmounted_auth_is_rejected() {
        let (db, _agent_uid, raw_agent_token) = prepare_workspace_db().await;

        let root_response = create_run(
            State(test_state(db.clone())),
            user_headers("root"),
            Json(run_input("root_run")),
        )
        .await
        .into_response();
        assert_eq!(root_response.status(), StatusCode::FORBIDDEN);

        let bearer_response = create_run(
            State(test_state(db.clone())),
            bearer_headers(&raw_agent_token),
            Json(run_input("global_bearer_run")),
        )
        .await
        .into_response();
        assert_eq!(bearer_response.status(), StatusCode::FORBIDDEN);

        let malformed_workspace_response = create_run(
            State(test_state(db)),
            malformed_workspace_headers(&raw_agent_token),
            Json(run_input("malformed_workspace_run")),
        )
        .await
        .into_response();
        assert_eq!(
            malformed_workspace_response.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn insufficient_run_write_scope_is_rejected_without_backing_path_leak() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo/work".to_string()],
        )
        .await;

        let response = create_run(
            State(state),
            workspace_headers(workspace_id, &raw_secret),
            Json(run_input("run_123")),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs"), "{error}");
        assert!(!error.contains("/demo"), "{error}");
    }
}
