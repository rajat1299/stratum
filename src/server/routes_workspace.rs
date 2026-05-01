use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AppState;
use super::idempotency as http_idempotency;
use super::middleware::session_from_headers;
use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, Uid, WHEEL_GID};
use crate::error::VfsError;
use crate::idempotency::{IdempotencyBegin, IdempotencyReservation, request_fingerprint};

const CREATE_WORKSPACE_IDEMPOTENCY_SCOPE: &str = "workspaces:create";
const CREATE_WORKSPACE_IDEMPOTENCY_ROUTE: &str = "POST /workspaces";
const WORKSPACE_TOKEN_IDEMPOTENCY_REJECTION: &str =
    "Idempotent workspace-token issuance requires secret-aware replay storage";

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

#[derive(Serialize)]
struct AdminActorFingerprint<'a> {
    uid: Uid,
    username: &'a str,
    effective_uid: Uid,
    delegate: Option<AdminDelegateFingerprint<'a>>,
}

#[derive(Serialize)]
struct AdminDelegateFingerprint<'a> {
    uid: Uid,
    username: &'a str,
}

#[derive(Serialize)]
struct CreateWorkspaceFingerprint<'a> {
    route: &'static str,
    actor: AdminActorFingerprint<'a>,
    name: &'a str,
    root_path: &'a str,
    base_ref: &'a str,
    session_ref: Option<&'a str>,
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

async fn append_audit(
    state: &AppState,
    event: NewAuditEvent,
) -> Result<(), axum::response::Response> {
    state.audit.append(event).await.map(|_| ()).map_err(|e| {
        let (status, body) = audit_append_failed_after_mutation(e);
        (status, Json(body)).into_response()
    })
}

fn audit_append_failed_after_mutation(error: VfsError) -> (StatusCode, serde_json::Value) {
    (
        error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
        serde_json::json!({
            "error": format!("audit append failed after mutation: {error}"),
            "mutation_committed": true,
            "audit_recorded": false,
        }),
    )
}

fn admin_actor_fingerprint(session: &Session) -> AdminActorFingerprint<'_> {
    AdminActorFingerprint {
        uid: session.uid,
        username: &session.username,
        effective_uid: session.effective_uid(),
        delegate: session
            .delegate
            .as_ref()
            .map(|delegate| AdminDelegateFingerprint {
                uid: delegate.uid,
                username: &delegate.username,
            }),
    }
}

async fn begin_create_workspace_idempotency(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
    req: &CreateWorkspaceRequest,
    base_ref: &str,
) -> Result<Option<IdempotencyReservation>, axum::response::Response> {
    let idempotency_key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(key) => key,
        Err(e) => {
            return Err(
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response(),
            );
        }
    };
    let Some(key) = idempotency_key else {
        return Ok(None);
    };

    let fingerprint = request_fingerprint(
        CREATE_WORKSPACE_IDEMPOTENCY_SCOPE,
        &CreateWorkspaceFingerprint {
            route: CREATE_WORKSPACE_IDEMPOTENCY_ROUTE,
            actor: admin_actor_fingerprint(session),
            name: &req.name,
            root_path: &req.root_path,
            base_ref,
            session_ref: req.session_ref.as_deref(),
        },
    )
    .map_err(|e| {
        err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
    })?;

    match state
        .idempotency
        .begin(CREATE_WORKSPACE_IDEMPOTENCY_SCOPE, &key, &fingerprint)
        .await
    {
        Ok(IdempotencyBegin::Execute(reservation)) => Ok(Some(reservation)),
        Ok(IdempotencyBegin::Replay(record)) => {
            Err(http_idempotency::idempotency_json_replay_response(record))
        }
        Ok(IdempotencyBegin::Conflict) => Err(http_idempotency::idempotency_conflict_response()),
        Ok(IdempotencyBegin::InProgress) => {
            Err(http_idempotency::idempotency_in_progress_response())
        }
        Err(e) => Err(err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response()),
    }
}

async fn complete_idempotency_or_response(
    state: &AppState,
    reservation: Option<IdempotencyReservation>,
    status: StatusCode,
    body: &serde_json::Value,
) -> Result<(), axum::response::Response> {
    if let Some(reservation) = reservation {
        state
            .idempotency
            .complete(&reservation, status.as_u16(), body.clone())
            .await
            .map_err(|e| {
                err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response()
            })?;
    }
    Ok(())
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let base_ref = req.base_ref.as_deref().unwrap_or(crate::vcs::MAIN_REF);
    let reservation = match begin_create_workspace_idempotency(
        &state, &headers, &session, &req, base_ref,
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(response) => return response,
    };

    match state
        .workspaces
        .create_workspace_with_refs(
            &req.name,
            &req.root_path,
            base_ref,
            req.session_ref.as_deref(),
        )
        .await
    {
        Ok(workspace) => {
            let mut event = NewAuditEvent::from_session(
                &session,
                AuditAction::WorkspaceCreate,
                AuditResource::id(AuditResourceKind::Workspace, workspace.id.to_string())
                    .with_path(&workspace.root_path),
            )
            .with_detail("name", &workspace.name)
            .with_detail("root_path", &workspace.root_path)
            .with_detail("base_ref", &workspace.base_ref);
            if let Some(session_ref) = &workspace.session_ref {
                event = event.with_detail("session_ref", session_ref);
            }
            let body = serde_json::to_value(&workspace).expect("workspace record serializes");
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_after_mutation(e);
                if let Err(response) =
                    complete_idempotency_or_response(&state, reservation, status, &body).await
                {
                    return response;
                }
                return (status, Json(body)).into_response();
            }
            if let Err(response) =
                complete_idempotency_or_response(&state, reservation, StatusCode::CREATED, &body)
                    .await
            {
                return response;
            }
            (StatusCode::CREATED, Json(body)).into_response()
        }
        Err(e) => {
            if let Some(reservation) = reservation {
                state.idempotency.abort(&reservation).await;
            }
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    if headers.contains_key("idempotency-key") {
        return err_json(
            StatusCode::BAD_REQUEST,
            WORKSPACE_TOKEN_IDEMPOTENCY_REJECTION,
        )
        .into_response();
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
        Ok(issued) => {
            if let Err(response) = append_audit(
                &state,
                NewAuditEvent::from_session(
                    &session,
                    AuditAction::WorkspaceTokenIssue,
                    AuditResource::id(
                        AuditResourceKind::WorkspaceToken,
                        issued.token.id.to_string(),
                    ),
                )
                .with_detail("workspace_id", id)
                .with_detail("token_name", &issued.token.name)
                .with_detail("agent_uid", issued.token.agent_uid)
                .with_detail("read_prefixes", issued.token.read_prefixes.join(","))
                .with_detail("write_prefixes", issued.token.write_prefixes.join(",")),
            )
            .await
            {
                return response;
            }
            Json(serde_json::json!({
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
            .into_response()
        }
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
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
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

    fn root_headers_with_idempotency(key: &str) -> HeaderMap {
        let mut headers = root_headers();
        headers.insert("idempotency-key", key.parse().unwrap());
        headers
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    struct FailingAuditStore;

    #[async_trait::async_trait]
    impl crate::audit::AuditStore for FailingAuditStore {
        async fn append(
            &self,
            _event: crate::audit::NewAuditEvent,
        ) -> Result<crate::audit::AuditEvent, VfsError> {
            Err(VfsError::IoError(std::io::Error::other(
                "audit write failed",
            )))
        }

        async fn list_recent(
            &self,
            _limit: usize,
        ) -> Result<Vec<crate::audit::AuditEvent>, VfsError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn create_workspace_idempotency_retry_replays_same_workspace_without_extra_audit() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let headers = root_headers_with_idempotency("workspace-create-retry");

        let first = create_workspace(
            State(state.clone()),
            headers.clone(),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: Some("agent/ci/session".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(first.headers().get("x-stratum-idempotent-replay").is_none());
        let first_body = response_json(first).await;
        let first_id = first_body["id"].as_str().expect("workspace id");
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);

        let replay = create_workspace(
            State(state.clone()),
            headers,
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: Some("agent/ci/session".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        let replay_body = response_json(replay).await;
        assert_eq!(replay_body["id"].as_str(), Some(first_id));
        assert_eq!(replay_body, first_body);
        assert_eq!(state.workspaces.list_workspaces().await.unwrap().len(), 1);
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn create_workspace_same_key_different_body_conflicts_without_second_workspace() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let headers = root_headers_with_idempotency("workspace-create-conflict");

        let first = create_workspace(
            State(state.clone()),
            headers.clone(),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(first.status(), StatusCode::CREATED);

        let conflict = create_workspace(
            State(state.clone()),
            headers,
            Json(CreateWorkspaceRequest {
                name: "other".to_string(),
                root_path: "/other".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let body = response_json(conflict).await;
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("different request")
        );
        assert_eq!(state.workspaces.list_workspaces().await.unwrap().len(), 1);
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn create_workspace_audit_failure_completes_idempotency_record_for_replay() {
        let db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingAuditStore),
        });
        let headers = root_headers_with_idempotency("workspace-create-audit-failure");

        let first = create_workspace(
            State(state.clone()),
            headers.clone(),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let first_body = response_json(first).await;
        assert_eq!(first_body["mutation_committed"], serde_json::json!(true));
        assert_eq!(first_body["audit_recorded"], serde_json::json!(false));

        let replay = create_workspace(
            State(state.clone()),
            headers,
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
        assert_eq!(state.workspaces.list_workspaces().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn issue_workspace_token_rejects_idempotency_key_before_agent_token_authentication() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state.clone()),
            root_headers_with_idempotency("workspace-token-secret-replay"),
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

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("secret-aware replay storage")
        );
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
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
    async fn create_workspace_audit_failure_reports_committed_mutation() {
        let db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingAuditStore),
        });

        let response = create_workspace(
            State(state.clone()),
            root_headers(),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["mutation_committed"], serde_json::json!(true));
        assert_eq!(body["audit_recorded"], serde_json::json!(false));
        assert_eq!(state.workspaces.list_workspaces().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn issue_token_audits_token_id_without_raw_agent_or_workspace_token() {
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
            State(state.clone()),
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
        let body = response_json(response).await;
        let workspace_token = body["workspace_token"]
            .as_str()
            .expect("workspace token response");
        let token_id = body["token_id"].as_str().expect("token id");
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::WorkspaceTokenIssue
        );
        assert_eq!(events[0].resource.id.as_deref(), Some(token_id));
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains(&raw_agent_token));
        assert!(!audit_json.contains(workspace_token));
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
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
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
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
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
