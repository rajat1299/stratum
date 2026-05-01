use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Serialize;

use super::AppState;
use super::middleware::session_from_headers;
use crate::auth::perms::Access;
use crate::auth::session::Session;
use crate::error::VfsError;
use crate::idempotency::{IdempotencyBegin, IdempotencyKey, request_fingerprint};
use crate::runs::{
    RUNS_ROOT, RunRecord, RunRecordContext, RunRecordFileKind, RunRecordInput, RunRecordLayout,
};

const RUN_FILE_PREVIEW_BYTES: usize = 4096;
const CREATE_RUN_IDEMPOTENCY_ROUTE: &str = "POST /runs";

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

#[derive(Debug, Serialize)]
struct ReadRunFileResponse {
    path: String,
    kind: &'static str,
    size: u64,
    modified: u64,
    encoding: &'static str,
    content_preview: Option<String>,
    content_truncated: bool,
}

#[derive(Debug, Serialize)]
struct ReadRunFilesResponse {
    prompt: ReadRunFileResponse,
    command: ReadRunFileResponse,
    stdout: ReadRunFileResponse,
    stderr: ReadRunFileResponse,
    result: ReadRunFileResponse,
    metadata: ReadRunFileResponse,
}

#[derive(Debug, Serialize)]
struct ReadRunResponse {
    run_id: String,
    root: String,
    artifacts: String,
    files: ReadRunFilesResponse,
}

#[derive(Debug, Serialize)]
struct CreateRunFingerprint<'a> {
    route: &'static str,
    workspace_id: uuid::Uuid,
    agent_uid: u32,
    request: &'a RunRecordInput,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/runs", post(create_run))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/stdout", get(get_run_stdout))
        .route("/runs/{id}/stderr", get(get_run_stderr))
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

async fn mounted_session_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Session, axum::response::Response> {
    let session = session_from_headers(state, headers)
        .await
        .map_err(|e| err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response())?;

    if session.mount().is_none() {
        let error = VfsError::PermissionDenied {
            path: RUNS_ROOT.to_string(),
        };
        return Err(err_json_for(&session, &error, StatusCode::FORBIDDEN));
    }

    Ok(session)
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

fn idempotency_key_from_headers(headers: &HeaderMap) -> Result<Option<IdempotencyKey>, VfsError> {
    let mut values = headers.get_all("idempotency-key").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(VfsError::InvalidArgs {
            message: "Idempotency-Key must be provided at most once".to_string(),
        });
    }

    Ok(Some(IdempotencyKey::parse_header_value(value)?))
}

fn idempotency_error_response(status: StatusCode, msg: &'static str) -> axum::response::Response {
    err_json(status, msg).into_response()
}

fn create_run_idempotency_scope(workspace_id: uuid::Uuid) -> String {
    format!("{CREATE_RUN_IDEMPOTENCY_ROUTE} workspace:{workspace_id}")
}

fn replay_create_run_response(
    record: crate::idempotency::IdempotencyRecord,
) -> axum::response::Response {
    let status =
        StatusCode::from_u16(record.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        [("x-stratum-idempotent-replay", "true")],
        Json(record.response_body),
    )
        .into_response()
}

fn require_run_write_scope(session: &Session, path: &str) -> Result<(), VfsError> {
    if session.is_path_allowed(path, Access::Write) {
        return Ok(());
    }

    Err(VfsError::PermissionDenied {
        path: path.to_string(),
    })
}

fn require_run_layout_write_scope(
    session: &Session,
    resolved: &ResolvedRunRecordLayout,
) -> Result<(), VfsError> {
    for path in [
        resolved.runs_root.as_str(),
        resolved.root.as_str(),
        resolved.artifacts.as_str(),
        resolved.prompt.as_str(),
        resolved.command.as_str(),
        resolved.stdout.as_str(),
        resolved.stderr.as_str(),
        resolved.result.as_str(),
        resolved.metadata.as_str(),
    ] {
        require_run_write_scope(session, path)?;
    }

    Ok(())
}

async fn authorize_run_create_preflight(
    state: &AppState,
    session: &Session,
    input: &RunRecordInput,
) -> Result<(), VfsError> {
    let runs_root = session.resolve_mounted_path(RUNS_ROOT)?;
    require_run_write_scope(session, &runs_root)?;
    state.db.check_mkdir_p_as(&runs_root, session).await?;

    let Some(run_id) = input.run_id.as_deref() else {
        return Ok(());
    };

    let layout = RunRecordLayout::new(run_id)?;
    let resolved = ResolvedRunRecordLayout::new(session, &layout)?;
    require_run_layout_write_scope(session, &resolved)?;
    state.db.check_mkdir_as(&resolved.root, session).await?;
    state
        .db
        .check_mkdir_p_as(&resolved.artifacts, session)
        .await?;

    Ok(())
}

async fn authorize_run_replay(
    state: &AppState,
    session: &Session,
    record: &crate::idempotency::IdempotencyRecord,
) -> Result<(), VfsError> {
    let run_id = record
        .response_body
        .get("run_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| VfsError::CorruptStore {
            message: "idempotency replay response is missing run_id".to_string(),
        })?;
    let layout = RunRecordLayout::new(run_id)?;
    let resolved = ResolvedRunRecordLayout::new(session, &layout)?;

    require_run_layout_write_scope(session, &resolved)?;
    state
        .db
        .check_mkdir_p_as(&resolved.runs_root, session)
        .await?;
    state.db.check_mkdir_as(&resolved.root, session).await?;
    state
        .db
        .check_mkdir_as(&resolved.artifacts, session)
        .await?;
    for path in [
        resolved.prompt.as_str(),
        resolved.command.as_str(),
        resolved.stdout.as_str(),
        resolved.stderr.as_str(),
        resolved.result.as_str(),
        resolved.metadata.as_str(),
    ] {
        state.db.check_write_file_as(path, session).await?;
    }

    Ok(())
}

async fn read_run_file_response(
    state: &AppState,
    session: &Session,
    path: &str,
) -> Result<ReadRunFileResponse, VfsError> {
    let info = state.db.stat_as(path, session).await?;
    let content = state.db.cat_as(path, session).await?;
    let (encoding, content_preview, content_truncated) = preview_run_file_content(&content);

    Ok(ReadRunFileResponse {
        path: session.project_mounted_path(path),
        kind: info.kind,
        size: info.size,
        modified: info.modified,
        encoding,
        content_preview,
        content_truncated,
    })
}

fn preview_run_file_content(content: &[u8]) -> (&'static str, Option<String>, bool) {
    let Ok(text) = std::str::from_utf8(content) else {
        return ("binary", None, !content.is_empty());
    };

    let mut end = RUN_FILE_PREVIEW_BYTES.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }

    ("utf-8", Some(text[..end].to_string()), end < text.len())
}

async fn read_run_stdout_or_stderr(
    state: AppState,
    headers: HeaderMap,
    run_id: String,
    kind: RunRecordFileKind,
) -> axum::response::Response {
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let layout = match RunRecordLayout::new(&run_id) {
        Ok(layout) => layout,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    let resolved = match ResolvedRunRecordLayout::new(&session, &layout) {
        Ok(layout) => layout,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };

    if let Err(e) = state.db.stat_as(&resolved.root, &session).await {
        return err_json_for(&session, &e, StatusCode::NOT_FOUND);
    }
    let path = resolved.path_for_kind(kind);

    match state.db.cat_as(path, &session).await {
        Ok(content) => (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            Body::from(content),
        )
            .into_response(),
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
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
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let mount = session.mount().expect("mounted session checked above");

    let idempotency_key = match idempotency_key_from_headers(&headers) {
        Ok(key) => key,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };

    let reservation = if let Some(key) = idempotency_key {
        if let Err(e) = authorize_run_create_preflight(&state, &session, &input).await {
            return err_json_for(&session, &e, StatusCode::BAD_REQUEST);
        }

        let idempotency_scope = create_run_idempotency_scope(mount.workspace_id());
        let fingerprint = match request_fingerprint(
            &idempotency_scope,
            &CreateRunFingerprint {
                route: CREATE_RUN_IDEMPOTENCY_ROUTE,
                workspace_id: mount.workspace_id(),
                agent_uid: session.uid,
                request: &input,
            },
        ) {
            Ok(fingerprint) => fingerprint,
            Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
        };

        match state
            .idempotency
            .begin(&idempotency_scope, &key, &fingerprint)
            .await
        {
            Ok(IdempotencyBegin::Execute(reservation)) => Some(reservation),
            Ok(IdempotencyBegin::Replay(record)) => {
                if let Err(e) = authorize_run_replay(&state, &session, &record).await {
                    return err_json_for(&session, &e, StatusCode::BAD_REQUEST);
                }
                return replay_create_run_response(record);
            }
            Ok(IdempotencyBegin::Conflict) => {
                return idempotency_error_response(
                    StatusCode::CONFLICT,
                    "Idempotency-Key was reused with a different request",
                );
            }
            Ok(IdempotencyBegin::InProgress) => {
                return idempotency_error_response(
                    StatusCode::CONFLICT,
                    "Idempotency-Key request is already in progress",
                );
            }
            Err(e) => return err_json_for(&session, &e, StatusCode::INTERNAL_SERVER_ERROR),
        }
    } else {
        None
    };

    match create_run_record(&state, &session, mount.workspace_id(), input).await {
        Ok(body) => {
            if let Some(reservation) = reservation
                && let Err(e) = state
                    .idempotency
                    .complete(&reservation, StatusCode::CREATED.as_u16(), body.clone())
                    .await
            {
                return err_json_for(&session, &e, StatusCode::INTERNAL_SERVER_ERROR);
            }
            (StatusCode::CREATED, Json(body)).into_response()
        }
        Err(response) => {
            if let Some(reservation) = reservation {
                state.idempotency.abort(&reservation).await;
            }
            response
        }
    }
}

async fn create_run_record(
    state: &AppState,
    session: &Session,
    workspace_id: uuid::Uuid,
    input: RunRecordInput,
) -> Result<serde_json::Value, axum::response::Response> {
    let context = RunRecordContext {
        workspace_id,
        agent_uid: session.uid,
        agent_username: session.username.clone(),
        created_at: Utc::now(),
    };
    let record = match RunRecord::new(input, context) {
        Ok(record) => record,
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    };
    let resolved = match ResolvedRunRecordLayout::new(session, &record.layout) {
        Ok(layout) => layout,
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    };

    if let Err(e) = validate_record_file_sizes(&record, state.db.config().max_file_size) {
        return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
    }

    if let Err(e) = state.db.mkdir_p_as(&resolved.runs_root, session).await {
        return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
    }

    if let Err(e) = state.db.mkdir_as(&resolved.root, session).await {
        return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
    }

    if let Err(e) = state.db.mkdir_as(&resolved.artifacts, session).await {
        return Err(err_json_partial_for(
            session,
            &e,
            StatusCode::BAD_REQUEST,
            &record,
            &resolved,
        ));
    }

    for file in &record.files {
        let path = resolved.path_for_kind(file.kind);
        if let Err(e) = state
            .db
            .write_file_as(path, file.content.as_bytes().to_vec(), session)
            .await
        {
            return Err(err_json_partial_for(
                session,
                &e,
                StatusCode::BAD_REQUEST,
                &record,
                &resolved,
            ));
        }
    }

    serde_json::to_value(CreateRunResponse::new(&record, session, &resolved)).map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode run response: {e}"),
        )
        .into_response()
    })
}

async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let layout = match RunRecordLayout::new(&run_id) {
        Ok(layout) => layout,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    let resolved = match ResolvedRunRecordLayout::new(&session, &layout) {
        Ok(layout) => layout,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };

    if let Err(e) = state.db.stat_as(&resolved.root, &session).await {
        return err_json_for(&session, &e, StatusCode::NOT_FOUND);
    }
    if let Err(e) = state.db.stat_as(&resolved.artifacts, &session).await {
        return err_json_for(&session, &e, StatusCode::NOT_FOUND);
    }

    let prompt = match read_run_file_response(&state, &session, &resolved.prompt).await {
        Ok(file) => file,
        Err(e) => return err_json_for(&session, &e, StatusCode::NOT_FOUND),
    };
    let command = match read_run_file_response(&state, &session, &resolved.command).await {
        Ok(file) => file,
        Err(e) => return err_json_for(&session, &e, StatusCode::NOT_FOUND),
    };
    let stdout = match read_run_file_response(&state, &session, &resolved.stdout).await {
        Ok(file) => file,
        Err(e) => return err_json_for(&session, &e, StatusCode::NOT_FOUND),
    };
    let stderr = match read_run_file_response(&state, &session, &resolved.stderr).await {
        Ok(file) => file,
        Err(e) => return err_json_for(&session, &e, StatusCode::NOT_FOUND),
    };
    let result = match read_run_file_response(&state, &session, &resolved.result).await {
        Ok(file) => file,
        Err(e) => return err_json_for(&session, &e, StatusCode::NOT_FOUND),
    };
    let metadata = match read_run_file_response(&state, &session, &resolved.metadata).await {
        Ok(file) => file,
        Err(e) => return err_json_for(&session, &e, StatusCode::NOT_FOUND),
    };

    Json(ReadRunResponse {
        run_id,
        root: session.project_mounted_path(&resolved.root),
        artifacts: project_directory_path(&session, &resolved.artifacts),
        files: ReadRunFilesResponse {
            prompt,
            command,
            stdout,
            stderr,
            result,
            metadata,
        },
    })
    .into_response()
}

async fn get_run_stdout(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    read_run_stdout_or_stderr(state, headers, run_id, RunRecordFileKind::Stdout).await
}

async fn get_run_stderr(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    read_run_stdout_or_stderr(state, headers, run_id, RunRecordFileKind::Stderr).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use axum::body::Bytes;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
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

    fn workspace_headers_with_idempotency(
        workspace_id: Uuid,
        raw_secret: &str,
        key: &str,
    ) -> HeaderMap {
        let mut headers = workspace_headers(workspace_id, raw_secret);
        headers.insert("idempotency-key", key.parse().unwrap());
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
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
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

    fn generated_run_input() -> RunRecordInput {
        let mut input = RunRecordInput::new(
            None,
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

    async fn run_directory_names(state: &AppState) -> Vec<String> {
        let mut names = state
            .db
            .ls(Some("/demo/runs"))
            .await
            .unwrap()
            .into_iter()
            .filter(|entry| entry.is_dir)
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[tokio::test]
    async fn idempotency_key_replays_generated_run_id_response_without_creating_another_run() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let headers = workspace_headers_with_idempotency(
            workspace_id,
            &raw_secret,
            "run-create-generated-replay",
        );

        let first = create_run(
            State(state.clone()),
            headers.clone(),
            Json(generated_run_input()),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(first.headers().get("x-stratum-idempotent-replay").is_none());
        let first_body = response_json(first).await;
        let run_id = first_body["run_id"].as_str().expect("run_id").to_string();
        assert_eq!(run_directory_names(&state).await, vec![run_id.clone()]);

        let replay = create_run(State(state.clone()), headers, Json(generated_run_input()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        let replay_body = response_json(replay).await;

        assert_eq!(replay_body, first_body);
        assert_eq!(run_directory_names(&state).await, vec![run_id]);
    }

    #[tokio::test]
    async fn idempotency_key_replays_explicit_run_id_response_instead_of_duplicate_conflict() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let headers =
            workspace_headers_with_idempotency(workspace_id, &raw_secret, "run-create-explicit");

        let first = create_run(
            State(state.clone()),
            headers.clone(),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let replay = create_run(State(state.clone()), headers, Json(run_input("run_123")))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );

        assert_eq!(response_json(replay).await, first_body);
        assert_eq!(
            run_directory_names(&state).await,
            vec!["run_123".to_string()]
        );
    }

    #[tokio::test]
    async fn idempotency_key_with_different_body_returns_conflict_without_mutation() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let headers =
            workspace_headers_with_idempotency(workspace_id, &raw_secret, "run-create-conflict");

        let first = create_run(
            State(state.clone()),
            headers.clone(),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let mut different = run_input("run_456");
        different.prompt = "different prompt".to_string();
        let conflict = create_run(State(state.clone()), headers, Json(different))
            .await
            .into_response();
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict_body = response_json(conflict).await;
        assert!(!conflict_body.to_string().contains("/demo"));
        assert!(state.db.stat("/demo/runs/run_456").await.is_err());
        assert_eq!(
            run_directory_names(&state).await,
            vec!["run_123".to_string()]
        );
    }

    #[tokio::test]
    async fn idempotency_keys_are_scoped_per_workspace() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let mut root = Session::root();
        db.mkdir_p_as("/other/runs", &root).await.unwrap();
        db.execute_command("chmod 777 /other/runs", &mut root)
            .await
            .unwrap();

        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace_a = store.create_workspace("demo-a", "/demo").await.unwrap();
        let workspace_b = store.create_workspace("demo-b", "/other").await.unwrap();
        let token_a = store
            .issue_scoped_workspace_token(
                workspace_a.id,
                "run-writer-a",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let token_b = store
            .issue_scoped_workspace_token(
                workspace_b.id,
                "run-writer-b",
                agent_uid,
                vec!["/other".to_string()],
                vec!["/other".to_string()],
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let response_a = create_run(
            State(state.clone()),
            workspace_headers_with_idempotency(workspace_a.id, &token_a.raw_secret, "shared-key"),
            Json(run_input("run_a")),
        )
        .await
        .into_response();
        assert_eq!(response_a.status(), StatusCode::CREATED);
        assert!(
            response_a
                .headers()
                .get("x-stratum-idempotent-replay")
                .is_none()
        );

        let response_b = create_run(
            State(state.clone()),
            workspace_headers_with_idempotency(workspace_b.id, &token_b.raw_secret, "shared-key"),
            Json(run_input("run_b")),
        )
        .await
        .into_response();
        assert_eq!(response_b.status(), StatusCode::CREATED);
        assert!(
            response_b
                .headers()
                .get("x-stratum-idempotent-replay")
                .is_none()
        );
        assert!(state.db.stat("/demo/runs/run_a").await.is_ok());
        assert!(state.db.stat("/other/runs/run_b").await.is_ok());
    }

    #[tokio::test]
    async fn idempotency_replay_requires_current_run_write_scope() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let full_scope_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "full-run-writer",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let narrow_scope_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "narrow-run-writer",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo/work".to_string()],
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let idempotency_key = "run-create-replay-scope";
        let first = create_run(
            State(state.clone()),
            workspace_headers_with_idempotency(
                workspace.id,
                &full_scope_token.raw_secret,
                idempotency_key,
            ),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let replay = create_run(
            State(state),
            workspace_headers_with_idempotency(
                workspace.id,
                &narrow_scope_token.raw_secret,
                idempotency_key,
            ),
            Json(run_input("run_123")),
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::FORBIDDEN);
        assert!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .is_none()
        );
        let body = response_json(replay).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs"), "{error}");
        assert!(!error.contains("/demo"), "{error}");
    }

    #[tokio::test]
    async fn idempotency_conflict_requires_current_run_write_scope() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let full_scope_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "full-run-writer",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let narrow_scope_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "narrow-run-writer",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo/work".to_string()],
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let idempotency_key = "run-create-conflict-scope";
        let first = create_run(
            State(state.clone()),
            workspace_headers_with_idempotency(
                workspace.id,
                &full_scope_token.raw_secret,
                idempotency_key,
            ),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let mut different = run_input("run_456");
        different.prompt = "different prompt".to_string();
        let conflict_probe = create_run(
            State(state),
            workspace_headers_with_idempotency(
                workspace.id,
                &narrow_scope_token.raw_secret,
                idempotency_key,
            ),
            Json(different),
        )
        .await
        .into_response();

        assert_eq!(conflict_probe.status(), StatusCode::FORBIDDEN);
        let body = response_json(conflict_probe).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs"), "{error}");
        assert!(!error.contains("/demo"), "{error}");
    }

    #[tokio::test]
    async fn unauthorized_idempotency_key_does_not_reserve_key() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let full_scope_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "full-run-writer",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let narrow_scope_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "narrow-run-writer",
                agent_uid,
                vec!["/demo".to_string()],
                vec!["/demo/work".to_string()],
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let idempotency_key = "run-create-unauthorized-no-reserve";
        let unauthorized = create_run(
            State(state.clone()),
            workspace_headers_with_idempotency(
                workspace.id,
                &narrow_scope_token.raw_secret,
                idempotency_key,
            ),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(unauthorized.status(), StatusCode::FORBIDDEN);
        assert!(state.db.stat("/demo/runs/run_123").await.is_err());

        let authorized = create_run(
            State(state.clone()),
            workspace_headers_with_idempotency(
                workspace.id,
                &full_scope_token.raw_secret,
                idempotency_key,
            ),
            Json(run_input("run_123")),
        )
        .await
        .into_response();

        assert_eq!(authorized.status(), StatusCode::CREATED);
        assert!(state.db.stat("/demo/runs/run_123").await.is_ok());
    }

    #[tokio::test]
    async fn invalid_idempotency_key_is_rejected_before_creating_run() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let headers =
            workspace_headers_with_idempotency(workspace_id, &raw_secret, "contains space");

        let response = create_run(State(state.clone()), headers, Json(run_input("run_123")))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("Idempotency-Key"));
        assert!(state.db.stat("/demo/runs/run_123").await.is_err());
    }

    #[tokio::test]
    async fn duplicate_idempotency_key_headers_are_rejected_before_creating_run() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let mut headers = workspace_headers(workspace_id, &raw_secret);
        headers.append("idempotency-key", "first-key".parse().unwrap());
        headers.append("idempotency-key", "second-key".parse().unwrap());

        let response = create_run(State(state.clone()), headers, Json(run_input("run_123")))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("Idempotency-Key"));
        assert!(state.db.stat("/demo/runs/run_123").await.is_err());
    }

    #[tokio::test]
    async fn idempotency_replay_response_does_not_leak_backing_path() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let headers =
            workspace_headers_with_idempotency(workspace_id, &raw_secret, "run-create-no-leak");

        let first = create_run(
            State(state.clone()),
            headers.clone(),
            Json(run_input("run_123")),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let replay = create_run(State(state), headers, Json(run_input("run_123")))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CREATED);
        let body = response_json(replay).await;
        assert!(!body.to_string().contains("/demo"));
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

    async fn create_sample_run(
        state: AppState,
        workspace_id: Uuid,
        raw_secret: &str,
        run_id: &str,
    ) {
        let response = create_run(
            State(state),
            workspace_headers(workspace_id, raw_secret),
            Json(run_input(run_id)),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn workspace_bearer_reads_run_record_summary() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        create_sample_run(state.clone(), workspace_id, &raw_secret, "run_123").await;

        let response = get_run(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body.get("run_id"), Some(&serde_json::json!("run_123")));
        assert_eq!(body.get("root"), Some(&serde_json::json!("/runs/run_123")));
        assert_eq!(
            body.get("artifacts"),
            Some(&serde_json::json!("/runs/run_123/artifacts/"))
        );
        assert_eq!(
            body["files"]["prompt"]["path"],
            serde_json::json!("/runs/run_123/prompt.md")
        );
        assert_eq!(
            body["files"]["prompt"]["content_preview"],
            serde_json::json!("Summarize the checkout incident")
        );
        assert_eq!(
            body["files"]["stdout"]["content_preview"],
            serde_json::json!("ok")
        );
        assert_eq!(
            body["files"]["stderr"]["content_preview"],
            serde_json::json!("warning")
        );
        assert_eq!(
            body["files"]["metadata"]["path"],
            serde_json::json!("/runs/run_123/metadata.md")
        );
        assert!(
            body["files"]["metadata"]["content_preview"]
                .as_str()
                .unwrap()
                .contains("status: \"queued\"")
        );
        assert!(body["files"]["prompt"]["size"].as_u64().unwrap() > 0);
        assert_eq!(body["files"]["prompt"]["kind"], serde_json::json!("file"));
        assert_eq!(
            body["files"]["prompt"]["encoding"],
            serde_json::json!("utf-8")
        );
        assert_eq!(
            body["files"]["prompt"]["content_truncated"],
            serde_json::json!(false)
        );
        assert!(!body.to_string().contains("/demo/"));
    }

    #[tokio::test]
    async fn run_record_summary_returns_bounded_text_previews() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let mut input = run_input("run_123");
        input.stdout = "x".repeat(RUN_FILE_PREVIEW_BYTES + 64);
        let response = create_run(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(input),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = get_run(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let preview = body["files"]["stdout"]["content_preview"]
            .as_str()
            .expect("stdout preview");
        assert_eq!(preview.len(), RUN_FILE_PREVIEW_BYTES);
        assert_eq!(
            body["files"]["stdout"]["content_truncated"],
            serde_json::json!(true)
        );
        assert_eq!(
            body["files"]["stdout"]["encoding"],
            serde_json::json!("utf-8")
        );
    }

    #[tokio::test]
    async fn run_record_summary_reports_binary_files_without_lossy_preview() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        create_sample_run(state.clone(), workspace_id, &raw_secret, "run_123").await;
        state
            .db
            .write_file("/demo/runs/run_123/stdout.md", vec![0xff, 0xfe])
            .await
            .unwrap();

        let response = get_run(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(
            body["files"]["stdout"]["encoding"],
            serde_json::json!("binary")
        );
        assert_eq!(
            body["files"]["stdout"]["content_preview"],
            serde_json::Value::Null
        );
        assert_eq!(
            body["files"]["stdout"]["content_truncated"],
            serde_json::json!(true)
        );
        assert_eq!(body["files"]["stdout"]["size"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn workspace_bearer_reads_run_stdout_and_stderr_content() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        create_sample_run(state.clone(), workspace_id, &raw_secret, "run_123").await;

        let stdout_response = get_run_stdout(
            State(state.clone()),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_eq!(stdout_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(stdout_response).await,
            Bytes::from_static(b"ok")
        );

        let stderr_response = get_run_stderr(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_eq!(stderr_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(stderr_response).await,
            Bytes::from_static(b"warning")
        );
    }

    #[tokio::test]
    async fn missing_run_read_returns_not_found() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let response = get_run(
            State(state),
            Path("missing".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs/missing"), "{error}");
        assert!(!error.contains("/demo"), "{error}");
    }

    #[tokio::test]
    async fn unsafe_run_id_read_is_rejected() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;

        let response = get_run(
            State(state),
            Path("../escape".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unmounted_auth_is_rejected_for_run_reads() {
        let (db, _agent_uid, raw_agent_token) = prepare_workspace_db().await;

        let root_response = get_run(
            State(test_state(db.clone())),
            Path("run_123".to_string()),
            user_headers("root"),
        )
        .await
        .into_response();
        assert_eq!(root_response.status(), StatusCode::FORBIDDEN);

        let bearer_response = get_run_stdout(
            State(test_state(db.clone())),
            Path("run_123".to_string()),
            bearer_headers(&raw_agent_token),
        )
        .await
        .into_response();
        assert_eq!(bearer_response.status(), StatusCode::FORBIDDEN);

        let malformed_workspace_response = get_run_stderr(
            State(test_state(db)),
            Path("run_123".to_string()),
            malformed_workspace_headers(&raw_agent_token),
        )
        .await
        .into_response();
        assert_eq!(
            malformed_workspace_response.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn insufficient_run_read_scope_is_rejected_without_backing_path_leak() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        create_sample_run(state.clone(), workspace_id, &raw_secret, "run_123").await;
        let issued = state
            .workspaces
            .issue_scoped_workspace_token(
                workspace_id,
                "run-reader",
                agent_uid,
                vec!["/demo/work".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();

        let response = get_run(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &issued.raw_secret),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs/run_123"), "{error}");
        assert!(!error.contains("/demo"), "{error}");
    }

    #[tokio::test]
    async fn raw_output_reads_require_run_root_read_scope() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        create_sample_run(state.clone(), workspace_id, &raw_secret, "run_123").await;

        let stdout_only = state
            .workspaces
            .issue_scoped_workspace_token(
                workspace_id,
                "stdout-only",
                agent_uid,
                vec!["/demo/runs/run_123/stdout.md".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let stdout_response = get_run_stdout(
            State(state.clone()),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &stdout_only.raw_secret),
        )
        .await
        .into_response();
        assert_eq!(stdout_response.status(), StatusCode::FORBIDDEN);
        let body = response_json(stdout_response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs/run_123"), "{error}");
        assert!(!error.contains("/demo"), "{error}");

        let stderr_only = state
            .workspaces
            .issue_scoped_workspace_token(
                workspace_id,
                "stderr-only",
                agent_uid,
                vec!["/demo/runs/run_123/stderr.md".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let stderr_response = get_run_stderr(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &stderr_only.raw_secret),
        )
        .await
        .into_response();
        assert_eq!(stderr_response.status(), StatusCode::FORBIDDEN);
        let body = response_json(stderr_response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("/runs/run_123"), "{error}");
        assert!(!error.contains("/demo"), "{error}");
    }

    #[tokio::test]
    async fn run_read_paths_are_projected_from_workspace_root() {
        let (db, agent_uid, _raw_agent_token) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        create_sample_run(state.clone(), workspace_id, &raw_secret, "run_123").await;

        let response = get_run(
            State(state),
            Path("run_123".to_string()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();

        let body = response_json(response).await;
        assert_eq!(body["root"], serde_json::json!("/runs/run_123"));
        assert_eq!(
            body["files"]["result"]["path"],
            serde_json::json!("/runs/run_123/result.md")
        );
        assert!(!body.to_string().contains("/demo"));
    }
}
