use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use super::AppState;
use super::middleware::session_from_headers;
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, WHEEL_GID};
use crate::error::VfsError;

#[derive(Deserialize)]
pub struct CreateWorkspaceRequest {
    pub name: String,
    pub root_path: String,
    #[serde(default)]
    pub base_ref: Option<String>,
    #[serde(default)]
    pub session_ref: Option<String>,
}

#[derive(Deserialize)]
pub struct IssueTokenRequest {
    pub name: String,
    pub agent_token: String,
    #[serde(default)]
    pub read_prefixes: Option<Vec<String>>,
    #[serde(default)]
    pub write_prefixes: Option<Vec<String>>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/workspaces", get(list_workspaces).post(create_workspace))
        .route("/workspaces/{id}", get(get_workspace))
        .route("/workspaces/{id}/tokens", post(issue_workspace_token))
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::InvalidArgs { .. } => StatusCode::BAD_REQUEST,
        VfsError::IoError(_) | VfsError::CorruptStore { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => fallback,
    }
}

fn issue_token_error_status(error: &VfsError) -> StatusCode {
    match error {
        VfsError::PermissionDenied { .. } => StatusCode::BAD_REQUEST,
        _ => error_status(error, StatusCode::BAD_REQUEST),
    }
}

fn require_admin_session(session: &Session) -> Result<(), VfsError> {
    if session.scope.is_some() {
        return Err(VfsError::PermissionDenied {
            path: "workspace metadata".to_string(),
        });
    }

    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "workspace metadata".to_string(),
        });
    }
    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "workspace metadata".to_string(),
            });
        }
    }
    Ok(())
}

async fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<Session, VfsError> {
    let session = session_from_headers(state, headers).await?;
    require_admin_session(&session)?;
    Ok(session)
}

async fn list_workspaces(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.workspaces.list_workspaces().await {
        Ok(workspaces) => Json(serde_json::json!({ "workspaces": workspaces })).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn create_workspace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateWorkspaceRequest>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state
        .workspaces
        .create_workspace_with_refs(
            &req.name,
            &req.root_path,
            req.base_ref.as_deref().unwrap_or(crate::vcs::MAIN_REF),
            req.session_ref.as_deref(),
        )
        .await
    {
        Ok(workspace) => (StatusCode::CREATED, Json(workspace)).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn get_workspace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.workspaces.get_workspace(id).await {
        Ok(Some(workspace)) => Json(workspace).into_response(),
        Ok(None) => {
            err_json(StatusCode::NOT_FOUND, format!("unknown workspace: {id}")).into_response()
        }
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn issue_workspace_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<IssueTokenRequest>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let agent_session = match state.db.authenticate_token(&req.agent_token).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response();
        }
    };

    let workspace = match state.workspaces.get_workspace(id).await {
        Ok(Some(workspace)) => workspace,
        Ok(None) => {
            return err_json(StatusCode::NOT_FOUND, format!("unknown workspace: {id}"))
                .into_response();
        }
        Err(e) => {
            return err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response();
        }
    };
    let read_prefixes = req
        .read_prefixes
        .unwrap_or_else(|| vec![workspace.root_path.clone()]);
    let write_prefixes = req
        .write_prefixes
        .unwrap_or_else(|| vec![workspace.root_path]);

    match state
        .workspaces
        .issue_scoped_workspace_token(
            id,
            &req.name,
            agent_session.uid,
            read_prefixes,
            write_prefixes,
        )
        .await
    {
        Ok(issued) => Json(serde_json::json!({
            "workspace_id": id,
            "token_id": issued.token.id,
            "name": issued.token.name,
            "workspace_token": issued.raw_secret,
            "agent_uid": issued.token.agent_uid,
            "read_prefixes": issued.token.read_prefixes,
            "write_prefixes": issued.token.write_prefixes,
            "base_ref": workspace.base_ref,
            "session_ref": workspace.session_ref,
        }))
        .into_response(),
        Err(e) => err_json(issue_token_error_status(&e), e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, LocalWorkspaceMetadataStore, WorkspaceMetadataStore,
    };
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        })
    }

    fn temp_metadata_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir()
            .join("stratum-routes-workspace-tests")
            .join(format!("{name}-{}.bin", Uuid::new_v4()))
    }

    fn extract_agent_token(output: &str) -> String {
        output
            .lines()
            .last()
            .expect("agent token line")
            .trim()
            .to_string()
    }

    fn root_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "User root".parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn issue_workspace_token_rejects_invalid_backing_agent_token() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "demo-token".to_string(),
                agent_token: "not-valid".to_string(),
                read_prefixes: None,
                write_prefixes: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn issue_workspace_token_does_not_echo_raw_agent_token() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "demo-token".to_string(),
                agent_token: raw_agent_token.clone(),
                read_prefixes: None,
                write_prefixes: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value.get("agent_token").is_none());
        assert_ne!(
            value.get("workspace_token"),
            Some(&serde_json::json!(raw_agent_token))
        );
        assert_eq!(value.get("agent_uid"), Some(&serde_json::json!(1)));
    }

    #[tokio::test]
    async fn issue_workspace_token_does_not_persist_raw_agent_token() {
        let path = temp_metadata_path("no-agent-token");
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "demo-token".to_string(),
                agent_token: raw_agent_token.clone(),
                read_prefixes: None,
                write_prefixes: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = std::fs::read(&path).unwrap();
        let file_text = String::from_utf8_lossy(&bytes);
        assert!(!file_text.contains(&raw_agent_token));
    }

    #[tokio::test]
    async fn issue_workspace_token_defaults_omitted_prefixes_to_workspace_root() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo/root")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "demo-token".to_string(),
                agent_token: raw_agent_token,
                read_prefixes: None,
                write_prefixes: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value.get("read_prefixes"),
            Some(&serde_json::json!(["/demo/root"]))
        );
        assert_eq!(
            value.get("write_prefixes"),
            Some(&serde_json::json!(["/demo/root"]))
        );
        assert!(value.get("agent_token").is_none());
    }

    #[tokio::test]
    async fn issue_workspace_token_returns_custom_prefixes() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "demo-token".to_string(),
                agent_token: raw_agent_token,
                read_prefixes: Some(vec![
                    "/demo/read".to_string(),
                    "/demo/shared/./".to_string(),
                ]),
                write_prefixes: Some(vec!["/demo/write".to_string()]),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value.get("read_prefixes"),
            Some(&serde_json::json!(["/demo/read", "/demo/shared"]))
        );
        assert_eq!(
            value.get("write_prefixes"),
            Some(&serde_json::json!(["/demo/write"]))
        );
    }

    #[tokio::test]
    async fn issue_workspace_token_accepts_explicit_empty_prefixes_as_deny_all() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "deny-all-token".to_string(),
                agent_token: raw_agent_token,
                read_prefixes: Some(Vec::new()),
                write_prefixes: Some(Vec::new()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.get("read_prefixes"), Some(&serde_json::json!([])));
        assert_eq!(value.get("write_prefixes"), Some(&serde_json::json!([])));
    }

    #[tokio::test]
    async fn issue_workspace_token_rejects_out_of_root_prefixes() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "bad-token".to_string(),
                agent_token: raw_agent_token,
                read_prefixes: Some(vec!["/demo/read".to_string()]),
                write_prefixes: Some(vec!["/demo/../outside/write".to_string()]),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn workspace_bearer_token_cannot_call_workspace_admin_routes() {
        let path = temp_metadata_path("scoped-admin-denied");
        let db = StratumDb::open_memory();
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                crate::auth::ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        drop(store);

        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(LocalWorkspaceMetadataStore::open(&path).unwrap()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", issued.raw_secret).parse().unwrap(),
        );
        headers.insert(
            "x-stratum-workspace",
            workspace.id.to_string().parse().unwrap(),
        );

        let response = list_workspaces(State(state), headers).await.into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn workspace_management_requires_admin_auth() {
        let db = StratumDb::open_memory();
        let state = test_state(db);

        let response = create_workspace(
            State(state),
            HeaderMap::new(),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
