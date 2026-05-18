use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use super::AppState;
use super::idempotency as http_idempotency;
use super::middleware::session_from_headers;
use super::repo_context::RequestRepoContext;
use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, Uid, WHEEL_GID};
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyBegin, IdempotencyQuotaIdentity, IdempotencyReplayClassification,
    IdempotencyReservation, IdempotencyRetentionPolicy, SecretReplayMetadata, request_fingerprint,
};
use crate::secret_replay::{SecretReplayAad, SecretReplayEnvelope};
use crate::workspace::normalize_workspace_token_prefixes;

const CREATE_WORKSPACE_IDEMPOTENCY_SCOPE: &str = "workspaces:create";
const CREATE_WORKSPACE_IDEMPOTENCY_ROUTE: &str = "POST /workspaces";
const ISSUE_WORKSPACE_TOKEN_IDEMPOTENCY_ROUTE: &str = "POST /workspaces/{id}/tokens";
const WORKSPACE_TOKEN_IDEMPOTENCY_REJECTION: &str = "secret replay KMS is unavailable";
const WORKSPACE_TOKEN_IDEMPOTENCY_FAILURE: &str = "workspace-token idempotency replay failed";
const WORKSPACE_TOKEN_COMPENSATION_FAILURE: &str =
    "workspace-token compensation failed after mutation";
const WORKSPACE_TOKEN_REVOKE_IDEMPOTENCY_REJECTION: &str =
    "Idempotency-Key is not supported for workspace-token revocation";

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
    repo_id: Option<&'a str>,
    name: &'a str,
    root_path: &'a str,
    base_ref: &'a str,
    session_ref: Option<&'a str>,
}

#[derive(Serialize)]
struct IssueWorkspaceTokenFingerprint<'a> {
    route: &'static str,
    actor: AdminActorFingerprint<'a>,
    repo_id: Option<&'a str>,
    workspace_id: Uuid,
    name: &'a str,
    agent_uid: Uid,
    read_prefixes: &'a [String],
    write_prefixes: &'a [String],
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/workspaces", get(list_workspaces).post(create_workspace))
        .route("/workspaces/{id}", get(get_workspace))
        .route("/workspaces/{id}/tokens", post(issue_workspace_token))
        .route(
            "/workspaces/{workspace_id}/tokens/{token_id}/revoke",
            post(revoke_workspace_token),
        )
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

fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(1)
        .max(1)
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

fn resolve_admin_repo_context(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
) -> Result<RequestRepoContext, VfsError> {
    RequestRepoContext::resolve(
        headers,
        session.mount(),
        !state.requires_explicit_workspace_repo(),
    )
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
            "error": "audit append failed after mutation",
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

fn workspace_token_idempotency_scope(repo: &RequestRepoContext, workspace_id: Uuid) -> String {
    if repo.is_local_singleton() {
        format!("workspace:{workspace_id}:tokens:issue")
    } else {
        format!(
            "repo:{}:workspace:{workspace_id}:tokens:issue",
            repo.repo_id()
        )
    }
}

fn workspace_token_secret_replay_aad(
    scope: &str,
    key_hash: &str,
    request_fingerprint: &str,
    status: StatusCode,
) -> SecretReplayAad {
    SecretReplayAad {
        scope: scope.to_string(),
        key_hash: key_hash.to_string(),
        request_fingerprint: request_fingerprint.to_string(),
        route: ISSUE_WORKSPACE_TOKEN_IDEMPOTENCY_ROUTE.to_string(),
        status_code: status.as_u16(),
        replay_classification: "secret_bearing".to_string(),
    }
}

fn workspace_token_idempotency_failure_response() -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(workspace_token_idempotency_failure_body()),
    )
        .into_response()
}

fn workspace_token_idempotency_failure_body() -> serde_json::Value {
    serde_json::json!({
        "error": WORKSPACE_TOKEN_IDEMPOTENCY_FAILURE,
        "idempotency_recorded": false,
        "replayable": false,
    })
}

fn workspace_token_compensation_failure_body() -> serde_json::Value {
    serde_json::json!({
        "error": WORKSPACE_TOKEN_COMPENSATION_FAILURE,
        "mutation_committed": true,
        "workspace_token_revoked": false,
        "idempotency_recorded": false,
        "replayable": false,
    })
}

async fn revoke_workspace_token_after_failed_secret_replay(
    state: &AppState,
    repo: &RequestRepoContext,
    workspace_id: Uuid,
    token_id: Uuid,
) -> Result<(), VfsError> {
    let revoked = state
        .workspaces
        .revoke_workspace_token_for_repo(
            repo.repo_id(),
            workspace_id,
            token_id,
            current_unix_time(),
        )
        .await?;
    if revoked.is_some() {
        Ok(())
    } else {
        Err(VfsError::CorruptStore {
            message: "workspace token compensation failed".to_string(),
        })
    }
}

async fn append_workspace_token_compensation_audit(
    state: &AppState,
    session: &Session,
    workspace_id: Uuid,
    token_id: Uuid,
) -> bool {
    let event = NewAuditEvent::from_session(
        session,
        AuditAction::WorkspaceTokenRevoke,
        AuditResource::id(AuditResourceKind::WorkspaceToken, token_id.to_string()),
    )
    .with_detail("workspace_id", workspace_id)
    .with_detail("reason", "post_issue_failure");
    state.audit.append(event).await.is_ok()
}

async fn complete_workspace_token_failure_idempotency(
    state: &AppState,
    reservation: Option<&IdempotencyReservation>,
    status: StatusCode,
    mut body: serde_json::Value,
) -> serde_json::Value {
    let Some(reservation) = reservation else {
        return body;
    };
    body["idempotency_recorded"] = serde_json::Value::Bool(true);
    body["replayable"] = serde_json::Value::Bool(true);
    if http_idempotency::persist_with_classification(
        state.idempotency.as_ref(),
        reservation,
        status,
        body.clone(),
        http_idempotency::secret_free(),
    )
    .await
    .is_ok()
    {
        body
    } else {
        body["idempotency_recorded"] = serde_json::Value::Bool(false);
        body["replayable"] = serde_json::Value::Bool(false);
        body
    }
}

struct WorkspaceTokenFailureCompensation<'a> {
    session: &'a Session,
    repo: &'a RequestRepoContext,
    workspace_id: Uuid,
    token_id: Uuid,
    reservation: Option<&'a IdempotencyReservation>,
    status: StatusCode,
    body: serde_json::Value,
    audit_compensation: bool,
}

async fn compensate_issued_workspace_token_failure(
    state: &AppState,
    mut failure: WorkspaceTokenFailureCompensation<'_>,
) -> axum::response::Response {
    match revoke_workspace_token_after_failed_secret_replay(
        state,
        failure.repo,
        failure.workspace_id,
        failure.token_id,
    )
    .await
    {
        Ok(()) => {
            failure.body["workspace_token_revoked"] = serde_json::Value::Bool(true);
            if failure.audit_compensation {
                failure.body["compensation_audit_recorded"] = serde_json::Value::Bool(
                    append_workspace_token_compensation_audit(
                        state,
                        failure.session,
                        failure.workspace_id,
                        failure.token_id,
                    )
                    .await,
                );
            }
        }
        Err(_) => {
            failure.body = workspace_token_compensation_failure_body();
        }
    }

    let body = complete_workspace_token_failure_idempotency(
        state,
        failure.reservation,
        failure.status,
        failure.body,
    )
    .await;
    (failure.status, Json(body)).into_response()
}

struct IssueWorkspaceTokenIdempotencyContext<'a> {
    repo: &'a RequestRepoContext,
    workspace_id: Uuid,
    req: &'a IssueTokenRequest,
    agent_uid: Uid,
    read_prefixes: &'a [String],
    write_prefixes: &'a [String],
}

async fn begin_issue_workspace_token_idempotency(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
    ctx: IssueWorkspaceTokenIdempotencyContext<'_>,
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
    let Some(kms) = state.secret_replay_kms.as_ref() else {
        return Err(err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            WORKSPACE_TOKEN_IDEMPOTENCY_REJECTION,
        )
        .into_response());
    };

    let scope = workspace_token_idempotency_scope(ctx.repo, ctx.workspace_id);
    let fingerprint = request_fingerprint(
        &scope,
        &IssueWorkspaceTokenFingerprint {
            route: ISSUE_WORKSPACE_TOKEN_IDEMPOTENCY_ROUTE,
            actor: admin_actor_fingerprint(session),
            repo_id: (!ctx.repo.is_local_singleton()).then_some(ctx.repo.repo_id().as_str()),
            workspace_id: ctx.workspace_id,
            name: &ctx.req.name,
            agent_uid: ctx.agent_uid,
            read_prefixes: ctx.read_prefixes,
            write_prefixes: ctx.write_prefixes,
        },
    )
    .map_err(|e| {
        err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
    })?;

    let mut quota_identity = IdempotencyQuotaIdentity::for_scope(&scope);
    quota_identity.workspace_id = Some(ctx.workspace_id.to_string());
    quota_identity.principal_uid = Some(u64::from(session.effective_uid()));

    match state
        .idempotency
        .begin_with_policy(
            &scope,
            &key,
            &fingerprint,
            quota_identity,
            &IdempotencyRetentionPolicy::unlimited(),
        )
        .await
    {
        Ok(IdempotencyBegin::Execute(reservation)) => Ok(Some(reservation)),
        Ok(IdempotencyBegin::Replay(record))
            if record.classification == IdempotencyReplayClassification::SecretFree =>
        {
            Err(http_idempotency::idempotency_json_replay_response(record))
        }
        Ok(IdempotencyBegin::Replay(record))
            if record.classification == IdempotencyReplayClassification::SecretBearing =>
        {
            let envelope: SecretReplayEnvelope =
                serde_json::from_value(record.response_body.clone())
                    .map_err(|_| workspace_token_idempotency_failure_response())?;
            let status = StatusCode::from_u16(record.status_code)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let aad =
                workspace_token_secret_replay_aad(&scope, key.key_hash(), &fingerprint, status);
            let body = kms
                .decrypt_json(&aad, &envelope)
                .map_err(|_| workspace_token_idempotency_failure_response())?;
            Err((
                status,
                [(
                    http_idempotency::IDEMPOTENCY_REPLAY_HEADER,
                    http_idempotency::IDEMPOTENCY_REPLAY_HEADER_VALUE,
                )],
                Json(body),
            )
                .into_response())
        }
        Ok(IdempotencyBegin::Replay(_record)) => {
            Err(workspace_token_idempotency_failure_response())
        }
        Ok(IdempotencyBegin::Conflict) => Err(http_idempotency::idempotency_conflict_response()),
        Ok(IdempotencyBegin::InProgress) => {
            Err(http_idempotency::idempotency_in_progress_response())
        }
        Err(e) => Err(
            http_idempotency::idempotency_quota_response_if_quota_error_with_audit(
                state,
                session,
                "workspace",
                &e,
            )
            .await
            .unwrap_or_else(workspace_token_idempotency_failure_response),
        ),
    }
}

async fn begin_create_workspace_idempotency(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
    repo: &RequestRepoContext,
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

    let scope = if repo.is_local_singleton() {
        CREATE_WORKSPACE_IDEMPOTENCY_SCOPE.to_string()
    } else {
        format!(
            "repo:{}:{CREATE_WORKSPACE_IDEMPOTENCY_SCOPE}",
            repo.repo_id()
        )
    };
    let fingerprint = request_fingerprint(
        &scope,
        &CreateWorkspaceFingerprint {
            route: CREATE_WORKSPACE_IDEMPOTENCY_ROUTE,
            actor: admin_actor_fingerprint(session),
            repo_id: (!repo.is_local_singleton()).then_some(repo.repo_id().as_str()),
            name: &req.name,
            root_path: &req.root_path,
            base_ref,
            session_ref: req.session_ref.as_deref(),
        },
    )
    .map_err(|e| {
        err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
    })?;

    match state.idempotency.begin(&scope, &key, &fingerprint).await {
        Ok(IdempotencyBegin::Execute(reservation)) => Ok(Some(reservation)),
        Ok(IdempotencyBegin::Replay(record)) => {
            Err(http_idempotency::idempotency_json_replay_response(record))
        }
        Ok(IdempotencyBegin::Conflict) => Err(http_idempotency::idempotency_conflict_response()),
        Ok(IdempotencyBegin::InProgress) => {
            Err(http_idempotency::idempotency_in_progress_response())
        }
        Err(e) => Err(
            http_idempotency::idempotency_quota_response_if_quota_error_with_audit(
                state,
                session,
                "workspace",
                &e,
            )
            .await
            .unwrap_or_else(|| {
                err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response()
            }),
        ),
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
            .complete_with_classification(
                &reservation,
                status.as_u16(),
                body.clone(),
                http_idempotency::secret_free(),
            )
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let repo = match resolve_admin_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match state
        .workspaces
        .list_workspaces_for_repo(repo.repo_id())
        .await
    {
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
    let repo = match resolve_admin_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let reservation =
        match begin_create_workspace_idempotency(&state, &headers, &session, &repo, &req, base_ref)
            .await
        {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };

    let create_result = if repo.is_local_singleton() {
        state
            .workspaces
            .create_workspace_with_refs(
                &req.name,
                &req.root_path,
                base_ref,
                req.session_ref.as_deref(),
            )
            .await
    } else {
        state
            .workspaces
            .create_workspace_with_refs_for_repo(
                repo.repo_id().clone(),
                &req.name,
                &req.root_path,
                base_ref,
                req.session_ref.as_deref(),
            )
            .await
    };

    match create_result {
        Ok(workspace) => {
            let mut event = NewAuditEvent::from_session(
                &session,
                AuditAction::WorkspaceCreate,
                AuditResource::id(AuditResourceKind::Workspace, workspace.id.to_string()),
            )
            .with_detail("name", &workspace.name)
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let repo = match resolve_admin_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match state
        .workspaces
        .get_workspace_for_repo(repo.repo_id(), id)
        .await
    {
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

    let has_idempotency_key = headers.contains_key("idempotency-key");
    if has_idempotency_key && state.secret_replay_kms.is_none() {
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            WORKSPACE_TOKEN_IDEMPOTENCY_REJECTION,
        )
        .into_response();
    }

    let agent_session = match state.core.authenticate_token(&req.agent_token).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response();
        }
    };

    let repo = match resolve_admin_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let workspace = match state
        .workspaces
        .get_workspace_for_repo(repo.repo_id(), id)
        .await
    {
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
    let requested_read_prefixes = req
        .read_prefixes
        .clone()
        .unwrap_or_else(|| vec![workspace.root_path.clone()]);
    let requested_write_prefixes = req
        .write_prefixes
        .clone()
        .unwrap_or_else(|| vec![workspace.root_path.clone()]);
    let read_prefixes =
        match normalize_workspace_token_prefixes(&workspace.root_path, requested_read_prefixes) {
            Ok(prefixes) => prefixes,
            Err(e) => {
                return err_json(issue_token_error_status(&e), e.to_string()).into_response();
            }
        };
    let write_prefixes =
        match normalize_workspace_token_prefixes(&workspace.root_path, requested_write_prefixes) {
            Ok(prefixes) => prefixes,
            Err(e) => {
                return err_json(issue_token_error_status(&e), e.to_string()).into_response();
            }
        };

    let idempotency_reservation = match begin_issue_workspace_token_idempotency(
        &state,
        &headers,
        &session,
        IssueWorkspaceTokenIdempotencyContext {
            repo: &repo,
            workspace_id: id,
            req: &req,
            agent_uid: agent_session.uid,
            read_prefixes: &read_prefixes,
            write_prefixes: &write_prefixes,
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(response) => return response,
    };

    match state
        .workspaces
        .issue_scoped_workspace_token_for_repo(
            repo.repo_id(),
            id,
            &req.name,
            agent_session.uid,
            read_prefixes,
            write_prefixes,
        )
        .await
    {
        Ok(issued) => {
            let body = serde_json::json!({
                "workspace_id": id,
                "token_id": issued.token.id,
                "name": &issued.token.name,
                "workspace_token": &issued.raw_secret,
                "agent_uid": issued.token.agent_uid,
                "read_prefixes": &issued.token.read_prefixes,
                "write_prefixes": &issued.token.write_prefixes,
                "base_ref": &workspace.base_ref,
                "session_ref": &workspace.session_ref,
            });
            let issue_audit = NewAuditEvent::from_session(
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
            .with_detail("read_prefix_count", issued.token.read_prefixes.len())
            .with_detail("write_prefix_count", issued.token.write_prefixes.len());
            if let Err(error) = state.audit.append(issue_audit).await {
                let (status, body) = audit_append_failed_after_mutation(error);
                return compensate_issued_workspace_token_failure(
                    &state,
                    WorkspaceTokenFailureCompensation {
                        session: &session,
                        repo: &repo,
                        workspace_id: id,
                        token_id: issued.token.id,
                        reservation: idempotency_reservation.as_ref(),
                        status,
                        body,
                        audit_compensation: false,
                    },
                )
                .await;
            }
            if let Some(reservation) = idempotency_reservation {
                let Some(kms) = state.secret_replay_kms.as_ref() else {
                    return compensate_issued_workspace_token_failure(
                        &state,
                        WorkspaceTokenFailureCompensation {
                            session: &session,
                            repo: &repo,
                            workspace_id: id,
                            token_id: issued.token.id,
                            reservation: Some(&reservation),
                            status: StatusCode::INTERNAL_SERVER_ERROR,
                            body: workspace_token_idempotency_failure_body(),
                            audit_compensation: true,
                        },
                    )
                    .await;
                };
                let status = StatusCode::OK;
                let aad = workspace_token_secret_replay_aad(
                    reservation.scope(),
                    reservation.key_hash(),
                    reservation.request_fingerprint(),
                    status,
                );
                let envelope = match kms.encrypt_json(&aad, &body) {
                    Ok(envelope) => envelope,
                    Err(_) => {
                        return compensate_issued_workspace_token_failure(
                            &state,
                            WorkspaceTokenFailureCompensation {
                                session: &session,
                                repo: &repo,
                                workspace_id: id,
                                token_id: issued.token.id,
                                reservation: Some(&reservation),
                                status: StatusCode::INTERNAL_SERVER_ERROR,
                                body: workspace_token_idempotency_failure_body(),
                                audit_compensation: true,
                            },
                        )
                        .await;
                    }
                };
                let metadata = SecretReplayMetadata {
                    envelope_version: envelope.version,
                    key_id: envelope.key_id.clone(),
                    aad_hash: envelope.aad_hash.clone(),
                    encrypted_at_unix_seconds: envelope.encrypted_at_unix_seconds,
                };
                let encrypted_body = match serde_json::to_value(envelope) {
                    Ok(value) => value,
                    Err(_) => {
                        return compensate_issued_workspace_token_failure(
                            &state,
                            WorkspaceTokenFailureCompensation {
                                session: &session,
                                repo: &repo,
                                workspace_id: id,
                                token_id: issued.token.id,
                                reservation: Some(&reservation),
                                status: StatusCode::INTERNAL_SERVER_ERROR,
                                body: workspace_token_idempotency_failure_body(),
                                audit_compensation: true,
                            },
                        )
                        .await;
                    }
                };
                if http_idempotency::persist_encrypted_secret_replay(
                    state.idempotency.as_ref(),
                    &reservation,
                    status,
                    encrypted_body,
                    metadata,
                )
                .await
                .is_err()
                {
                    return compensate_issued_workspace_token_failure(
                        &state,
                        WorkspaceTokenFailureCompensation {
                            session: &session,
                            repo: &repo,
                            workspace_id: id,
                            token_id: issued.token.id,
                            reservation: Some(&reservation),
                            status: StatusCode::INTERNAL_SERVER_ERROR,
                            body: workspace_token_idempotency_failure_body(),
                            audit_compensation: true,
                        },
                    )
                    .await;
                }
            }
            Json(body).into_response()
        }
        Err(e) => {
            if let Some(reservation) = idempotency_reservation {
                state.idempotency.abort(&reservation).await;
            }
            err_json(issue_token_error_status(&e), e.to_string()).into_response()
        }
    }
}

async fn revoke_workspace_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((workspace_id, token_id)): Path<(Uuid, Uuid)>,
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
            WORKSPACE_TOKEN_REVOKE_IDEMPOTENCY_REJECTION,
        )
        .into_response();
    }

    let repo = match resolve_admin_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let token = match state
        .workspaces
        .revoke_workspace_token_for_repo(
            repo.repo_id(),
            workspace_id,
            token_id,
            current_unix_time(),
        )
        .await
    {
        Ok(Some(token)) => token,
        Ok(None) => {
            return err_json(
                StatusCode::NOT_FOUND,
                format!("unknown workspace token: {token_id}"),
            )
            .into_response();
        }
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let mut event = NewAuditEvent::from_session(
        &session,
        AuditAction::WorkspaceTokenRevoke,
        AuditResource::id(AuditResourceKind::WorkspaceToken, token.id.to_string()),
    )
    .with_detail("workspace_id", workspace_id)
    .with_detail("token_version", token.token_version);
    if let Some(principal_uid) = token.principal_uid {
        event = event.with_detail("principal_uid", principal_uid);
    }

    if let Err(response) = append_audit(&state, event).await {
        return response;
    }

    Json(serde_json::json!({
        "workspace_id": workspace_id,
        "token_id": token.id,
        "name": token.name,
        "agent_uid": token.agent_uid,
        "principal_uid": token.principal_uid,
        "token_version": token.token_version,
        "issued_at_unix": token.issued_at_unix,
        "updated_at_unix": token.updated_at_unix,
        "expires_at_unix": token.expires_at_unix,
        "revoked_at_unix": token.revoked_at_unix,
        "read_prefixes": token.read_prefixes,
        "write_prefixes": token.write_prefixes,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyReplayClassification, IdempotencyReservation,
        IdempotencyStore, InMemoryIdempotencyStore,
    };
    use crate::secret_replay::{LocalAeadSecretReplayKms, SecretReplayKms, SharedSecretReplayKms};
    use crate::server::{ServerLocalDb, ServerState};
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, LocalWorkspaceMetadataStore,
        ValidWorkspaceToken, WorkspaceMetadataStore, WorkspaceRecord, WorkspaceTokenRecord,
    };
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[cfg(feature = "postgres")]
    use crate::backend::postgres::{PostgresMetadataStore, connect_with_schema};
    #[cfg(feature = "postgres")]
    use crate::backend::postgres_migrations::PostgresMigrationRunner;
    #[cfg(feature = "postgres")]
    use tokio_postgres::Config;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
        })
    }

    fn test_kms(key_id: &str, byte: u8) -> SharedSecretReplayKms {
        Arc::new(LocalAeadSecretReplayKms::new(key_id, [byte; 32]).unwrap())
    }

    fn test_state_with_kms(db: StratumDb, kms: SharedSecretReplayKms) -> AppState {
        Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: Some(kms),
        })
    }

    async fn add_agent_token(db: &StratumDb, name: &str) -> String {
        let mut root = Session::root();
        extract_agent_token(
            &db.execute_command(&format!("addagent {name}"), &mut root)
                .await
                .unwrap(),
        )
    }

    #[cfg(feature = "postgres")]
    struct PostgresRouteTestDb {
        config: Config,
        schema: String,
        store: Arc<PostgresMetadataStore>,
    }

    #[cfg(feature = "postgres")]
    impl PostgresRouteTestDb {
        async fn new() -> Option<Self> {
            let Some(url) = std::env::var("STRATUM_POSTGRES_TEST_URL").ok() else {
                if postgres_tests_required() {
                    panic!("STRATUM_POSTGRES_TEST_URL is required for Postgres route tests");
                }
                eprintln!("skipping Postgres route tests; STRATUM_POSTGRES_TEST_URL is unset");
                return None;
            };
            let mut config: Config = url
                .parse()
                .expect("STRATUM_POSTGRES_TEST_URL should parse as a Postgres config");
            if config.get_password().is_some() {
                panic!(
                    "STRATUM_POSTGRES_TEST_URL must not include a password; use STRATUM_POSTGRES_TEST_PASSWORD or PGPASSWORD"
                );
            }
            if let Ok(password) = std::env::var("STRATUM_POSTGRES_TEST_PASSWORD")
                .or_else(|_| std::env::var("PGPASSWORD"))
            {
                config.password(password);
            }

            let schema = format!("stratum_pg_routes_{}", Uuid::new_v4().simple());
            let client = connect_with_schema(&config, None)
                .await
                .expect("connect test Postgres");
            client
                .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
                .await
                .expect("create isolated schema");
            PostgresMigrationRunner::with_schema(config.clone(), schema.clone())
                .expect("create migration runner")
                .apply_pending()
                .await
                .expect("apply route test migrations");
            let store = Arc::new(
                PostgresMetadataStore::with_schema(config.clone(), schema.clone())
                    .expect("open Postgres metadata store"),
            );
            Some(Self {
                config,
                schema,
                store,
            })
        }

        async fn idempotency_row(
            &self,
            scope: &str,
        ) -> (String, String, Option<i32>, Option<String>, Option<String>) {
            let client = connect_with_schema(&self.config, Some(&self.schema))
                .await
                .expect("connect route test schema");
            let row = client
                .query_one(
                    "SELECT replay_classification, response_body_json::text,
                            secret_replay_envelope_version, secret_replay_key_id,
                            secret_replay_aad_hash
                     FROM idempotency_records
                     WHERE scope = $1",
                    &[&scope],
                )
                .await
                .expect("load idempotency row");
            (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
        }

        async fn cleanup(self) {
            if let Ok(client) = connect_with_schema(&self.config, None).await {
                let _ = client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                        self.schema
                    ))
                    .await;
            }
        }
    }

    #[cfg(feature = "postgres")]
    fn postgres_tests_required() -> bool {
        std::env::var("STRATUM_POSTGRES_TEST_REQUIRED").as_deref() == Ok("1")
            || std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
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

    fn root_headers_for_repo(repo_id: &str) -> HeaderMap {
        let mut headers = root_headers();
        headers.insert("x-stratum-repo", repo_id.parse().unwrap());
        headers
    }

    fn root_headers_for_repo_with_idempotency(repo_id: &str, key: &str) -> HeaderMap {
        let mut headers = root_headers_with_idempotency(key);
        headers.insert("x-stratum-repo", repo_id.parse().unwrap());
        headers
    }

    fn workspace_bearer_headers(raw_secret: &str, workspace_id: Uuid) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {raw_secret}").parse().unwrap(),
        );
        headers.insert(
            "x-stratum-workspace",
            workspace_id.to_string().parse().unwrap(),
        );
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

        async fn contains_vcs_commit_event(&self, _commit_id: &str) -> Result<bool, VfsError> {
            Ok(false)
        }
    }

    struct FailingBeginIdempotencyStore;

    #[async_trait::async_trait]
    impl IdempotencyStore for FailingBeginIdempotencyStore {
        async fn begin(
            &self,
            _scope: &str,
            _key: &IdempotencyKey,
            _request_fingerprint: &str,
        ) -> Result<IdempotencyBegin, VfsError> {
            Err(VfsError::CorruptStore {
                message: "postgres://secret@metadata.example/raw replay envelope detail"
                    .to_string(),
            })
        }

        async fn complete(
            &self,
            _reservation: &IdempotencyReservation,
            _status_code: u16,
            _response_body: serde_json::Value,
        ) -> Result<(), VfsError> {
            unreachable!("begin fails before completion")
        }

        async fn abort(&self, _reservation: &IdempotencyReservation) {}
    }

    struct FailingEncryptKms {
        inner: LocalAeadSecretReplayKms,
    }

    impl FailingEncryptKms {
        fn new() -> Self {
            Self {
                inner: LocalAeadSecretReplayKms::new("workspace-token-encrypt-fail", [23; 32])
                    .unwrap(),
            }
        }
    }

    impl SecretReplayKms for FailingEncryptKms {
        fn key_id(&self) -> &str {
            self.inner.key_id()
        }

        fn key_hash(&self) -> &str {
            self.inner.key_hash()
        }

        fn encrypt_json(
            &self,
            _aad: &SecretReplayAad,
            _body: &serde_json::Value,
        ) -> Result<SecretReplayEnvelope, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw KMS provider failure detail".to_string(),
            })
        }

        fn decrypt_json(
            &self,
            aad: &SecretReplayAad,
            envelope: &SecretReplayEnvelope,
        ) -> Result<serde_json::Value, VfsError> {
            self.inner.decrypt_json(aad, envelope)
        }
    }

    #[derive(Default)]
    struct FailingRevokeWorkspaceStore {
        inner: InMemoryWorkspaceMetadataStore,
        issued: Mutex<Vec<IssuedWorkspaceToken>>,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for FailingRevokeWorkspaceStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            self.inner.list_workspaces().await
        }

        async fn create_workspace(
            &self,
            name: &str,
            root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            self.inner.create_workspace(name, root_path).await
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            self.inner.get_workspace(id).await
        }

        async fn update_head_commit(
            &self,
            id: Uuid,
            head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            self.inner.update_head_commit(id, head_commit).await
        }

        async fn update_head_commit_if_current(
            &self,
            id: Uuid,
            expected_head_commit: Option<&str>,
            head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            self.inner
                .update_head_commit_if_current(id, expected_head_commit, head_commit)
                .await
        }

        async fn issue_scoped_workspace_token(
            &self,
            workspace_id: Uuid,
            name: &str,
            agent_uid: Uid,
            read_prefixes: Vec<String>,
            write_prefixes: Vec<String>,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            let issued = self
                .inner
                .issue_scoped_workspace_token(
                    workspace_id,
                    name,
                    agent_uid,
                    read_prefixes,
                    write_prefixes,
                )
                .await?;
            self.issued.lock().unwrap().push(issued.clone());
            Ok(issued)
        }

        async fn validate_workspace_token_at(
            &self,
            workspace_id: Uuid,
            raw_secret: &str,
            now_unix: u64,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            self.inner
                .validate_workspace_token_at(workspace_id, raw_secret, now_unix)
                .await
        }

        async fn revoke_workspace_token(
            &self,
            _workspace_id: Uuid,
            _token_id: Uuid,
            _now_unix: u64,
        ) -> Result<Option<WorkspaceTokenRecord>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw revoke provider failure detail".to_string(),
            })
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
        let key = IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap())
            .expect("idempotency key");
        let session = require_admin(&state, &headers).await.expect("root session");
        let repo = resolve_admin_repo_context(&state, &headers, &session).expect("repo context");
        let scope = CREATE_WORKSPACE_IDEMPOTENCY_SCOPE.to_string();
        let fingerprint = request_fingerprint(
            &scope,
            &CreateWorkspaceFingerprint {
                route: CREATE_WORKSPACE_IDEMPOTENCY_ROUTE,
                actor: admin_actor_fingerprint(&session),
                repo_id: None,
                name: "demo",
                root_path: "/demo",
                base_ref: crate::vcs::MAIN_REF,
                session_ref: Some("agent/ci/session"),
            },
        )
        .expect("fingerprint");
        match state
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => {
                assert_eq!(
                    record.classification,
                    IdempotencyReplayClassification::SecretFree
                );
            }
            other => panic!("expected completed workspace replay record, got {other:?}"),
        }
        assert!(repo.is_local_singleton());
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::WorkspaceCreate);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("/demo"));

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
    async fn create_workspace_idempotency_scope_is_repo_qualified_for_explicit_repo_contexts() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let key = "workspace-create-same-key-across-repos";

        let first = create_workspace(
            State(state.clone()),
            root_headers_for_repo_with_idempotency("repo_a", key),
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
        let first_body = response_json(first).await;
        assert_eq!(first_body["repo_id"].as_str(), Some("repo_a"));

        let second = create_workspace(
            State(state.clone()),
            root_headers_for_repo_with_idempotency("repo_b", key),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                base_ref: None,
                session_ref: Some("agent/ci/session".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(second.status(), StatusCode::CREATED);
        let second_body = response_json(second).await;
        assert_eq!(second_body["repo_id"].as_str(), Some("repo_b"));
        assert_ne!(first_body["id"], second_body["id"]);
        assert_eq!(state.workspaces.list_workspaces().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn workspace_admin_list_and_get_are_repo_scoped() {
        let state = test_state(StratumDb::open_memory());
        let repo_a = create_workspace(
            State(state.clone()),
            root_headers_for_repo("repo_a"),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo-a".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(repo_a.status(), StatusCode::CREATED);
        let repo_a_body = response_json(repo_a).await;
        let repo_a_id = Uuid::parse_str(repo_a_body["id"].as_str().unwrap()).unwrap();

        let repo_b = create_workspace(
            State(state.clone()),
            root_headers_for_repo("repo_b"),
            Json(CreateWorkspaceRequest {
                name: "demo".to_string(),
                root_path: "/demo-b".to_string(),
                base_ref: None,
                session_ref: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(repo_b.status(), StatusCode::CREATED);
        let repo_b_body = response_json(repo_b).await;
        let repo_b_id = Uuid::parse_str(repo_b_body["id"].as_str().unwrap()).unwrap();

        let listed = list_workspaces(State(state.clone()), root_headers_for_repo("repo_a"))
            .await
            .into_response();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed_body = response_json(listed).await;
        let workspaces = listed_body["workspaces"].as_array().unwrap();
        assert_eq!(workspaces.len(), 1);
        assert_eq!(workspaces[0]["id"], repo_a_id.to_string());

        let hidden = get_workspace(
            State(state.clone()),
            root_headers_for_repo("repo_a"),
            Path(repo_b_id),
        )
        .await
        .into_response();
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

        let visible = get_workspace(
            State(state),
            root_headers_for_repo("repo_b"),
            Path(repo_b_id),
        )
        .await
        .into_response();
        assert_eq!(visible.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_workspace_audit_failure_completes_idempotency_record_for_replay() {
        let db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingAuditStore),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
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
    async fn issue_workspace_token_idempotency_replays_same_secret_from_encrypted_record() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let state = test_state_with_kms(db, test_kms("workspace-token-test", 7));
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-replay");
        let req = IssueTokenRequest {
            name: "demo-token".to_string(),
            agent_token: raw_agent_token.clone(),
            read_prefixes: Some(vec!["/demo/read/./".to_string()]),
            write_prefixes: Some(vec!["/demo/write".to_string()]),
        };

        let first = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: req.name.clone(),
                agent_token: req.agent_token.clone(),
                read_prefixes: req.read_prefixes.clone(),
                write_prefixes: req.write_prefixes.clone(),
            }),
        )
        .await
        .into_response();

        assert_eq!(first.status(), StatusCode::OK);
        assert!(first.headers().get("x-stratum-idempotent-replay").is_none());
        let first_body = response_json(first).await;
        let workspace_token = first_body["workspace_token"].as_str().unwrap().to_string();
        let token_id = first_body["token_id"].as_str().unwrap().to_string();

        let session = require_admin(&state, &headers).await.unwrap();
        let repo = resolve_admin_repo_context(&state, &headers, &session).unwrap();
        let scope = workspace_token_idempotency_scope(&repo, workspace.id);
        let read_prefixes = normalize_workspace_token_prefixes(
            &workspace.root_path,
            req.read_prefixes.clone().unwrap(),
        )
        .unwrap();
        let write_prefixes = normalize_workspace_token_prefixes(
            &workspace.root_path,
            req.write_prefixes.clone().unwrap(),
        )
        .unwrap();
        let fingerprint = request_fingerprint(
            &scope,
            &IssueWorkspaceTokenFingerprint {
                route: ISSUE_WORKSPACE_TOKEN_IDEMPOTENCY_ROUTE,
                actor: admin_actor_fingerprint(&session),
                repo_id: None,
                workspace_id: workspace.id,
                name: &req.name,
                agent_uid: agent.uid,
                read_prefixes: &read_prefixes,
                write_prefixes: &write_prefixes,
            },
        )
        .unwrap();
        let key =
            IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap()).unwrap();
        let record = match state
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected encrypted replay record, got {other:?}"),
        };
        assert_eq!(
            record.classification,
            IdempotencyReplayClassification::SecretBearing
        );
        assert!(record.secret_replay.is_some());
        let stored = serde_json::to_string(&record.response_body).unwrap();
        assert!(stored.contains("ciphertext_b64"));
        assert!(!stored.contains(&workspace_token));
        assert!(!stored.contains(&raw_agent_token));

        let replay =
            issue_workspace_token(State(state.clone()), headers, Path(workspace.id), Json(req))
                .await
                .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        let replay_body = response_json(replay).await;
        assert_eq!(replay_body, first_body);
        assert_eq!(replay_body["workspace_token"], workspace_token);
        assert_eq!(replay_body["token_id"], token_id);
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn issue_workspace_token_idempotency_conflicts_for_different_request() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let state = test_state_with_kms(db, test_kms("workspace-token-test", 7));
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-conflict");

        let first = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
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
        assert_eq!(first.status(), StatusCode::OK);

        let conflict = issue_workspace_token(
            State(state.clone()),
            headers,
            Path(workspace.id),
            Json(IssueTokenRequest {
                name: "other-token".to_string(),
                agent_token: raw_agent_token,
                read_prefixes: None,
                write_prefixes: None,
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
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn issue_workspace_token_idempotency_in_progress_does_not_issue_token() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let state = test_state_with_kms(db, test_kms("workspace-token-test", 7));
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-pending");
        let session = require_admin(&state, &headers).await.unwrap();
        let repo = resolve_admin_repo_context(&state, &headers, &session).unwrap();
        let scope = workspace_token_idempotency_scope(&repo, workspace.id);
        let req = IssueTokenRequest {
            name: "demo-token".to_string(),
            agent_token: raw_agent_token,
            read_prefixes: None,
            write_prefixes: None,
        };
        let read_prefixes = normalize_workspace_token_prefixes(
            &workspace.root_path,
            vec![workspace.root_path.clone()],
        )
        .unwrap();
        let write_prefixes = normalize_workspace_token_prefixes(
            &workspace.root_path,
            vec![workspace.root_path.clone()],
        )
        .unwrap();
        let fingerprint = request_fingerprint(
            &scope,
            &IssueWorkspaceTokenFingerprint {
                route: ISSUE_WORKSPACE_TOKEN_IDEMPOTENCY_ROUTE,
                actor: admin_actor_fingerprint(&session),
                repo_id: None,
                workspace_id: workspace.id,
                name: &req.name,
                agent_uid: agent.uid,
                read_prefixes: &read_prefixes,
                write_prefixes: &write_prefixes,
            },
        )
        .unwrap();
        let key =
            IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap()).unwrap();
        assert!(matches!(
            state
                .idempotency
                .begin(&scope, &key, &fingerprint)
                .await
                .unwrap(),
            IdempotencyBegin::Execute(_)
        ));

        let response =
            issue_workspace_token(State(state.clone()), headers, Path(workspace.id), Json(req))
                .await
                .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("already in progress")
        );
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn issue_workspace_token_idempotency_missing_kms_fails_closed_without_row() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-missing-kms");

        let response = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
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

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], WORKSPACE_TOKEN_IDEMPOTENCY_REJECTION);
        let session = require_admin(&state, &headers).await.unwrap();
        let repo = resolve_admin_repo_context(&state, &headers, &session).unwrap();
        let scope = workspace_token_idempotency_scope(&repo, workspace.id);
        let key =
            IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap()).unwrap();
        let fingerprint = request_fingerprint(
            &scope,
            &IssueWorkspaceTokenFingerprint {
                route: ISSUE_WORKSPACE_TOKEN_IDEMPOTENCY_ROUTE,
                actor: admin_actor_fingerprint(&session),
                repo_id: None,
                workspace_id: workspace.id,
                name: "demo-token",
                agent_uid: 1,
                read_prefixes: std::slice::from_ref(&workspace.root_path),
                write_prefixes: std::slice::from_ref(&workspace.root_path),
            },
        )
        .unwrap();
        let pending = state
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap();
        let IdempotencyBegin::Execute(reservation) = pending else {
            panic!("missing KMS must not create idempotency row, got {pending:?}");
        };
        state.idempotency.abort(&reservation).await;
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn issue_workspace_token_idempotency_begin_failure_is_redacted() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(FailingBeginIdempotencyStore),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: Some(test_kms("workspace-token-begin-failure", 17)),
        });
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state),
            root_headers_with_idempotency("workspace-token-begin-failure"),
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

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], WORKSPACE_TOKEN_IDEMPOTENCY_FAILURE);
        assert_eq!(body["idempotency_recorded"], false);
        assert_eq!(body["replayable"], false);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("postgres://secret"));
        assert!(!rendered.contains("raw replay envelope detail"));
        assert!(!rendered.contains(&raw_agent_token));
    }

    #[tokio::test]
    async fn issue_workspace_token_idempotency_invalid_backing_token_returns_unauthorized() {
        let db = StratumDb::open_memory();
        let state = test_state_with_kms(db, test_kms("workspace-token-test", 7));
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let response = issue_workspace_token(
            State(state.clone()),
            root_headers_with_idempotency("workspace-token-invalid-agent"),
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
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn issue_workspace_token_idempotency_decrypt_failure_fails_closed() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let state = test_state_with_kms(db, test_kms("workspace-token-test", 7));
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-decrypt-fail");

        let first = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
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
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;
        let raw_workspace_token = first_body["workspace_token"].as_str().unwrap().to_string();

        let replay_state = Arc::new(ServerState {
            core: state.core.clone(),
            db: state.db.clone(),
            workspaces: state.workspaces.clone(),
            idempotency: state.idempotency.clone(),
            audit: state.audit.clone(),
            review: state.review.clone(),
            secret_replay_kms: Some(test_kms("workspace-token-test", 9)),
        });
        let replay = issue_workspace_token(
            State(replay_state),
            headers,
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

        assert_eq!(replay.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(replay).await;
        assert_eq!(body["error"], WORKSPACE_TOKEN_IDEMPOTENCY_FAILURE);
        assert!(
            !serde_json::to_string(&body)
                .unwrap()
                .contains(&raw_workspace_token)
        );
    }

    #[tokio::test]
    async fn issue_workspace_token_audit_failure_revokes_and_replays_terminal_failure() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingAuditStore),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: Some(test_kms("workspace-token-audit-failure", 13)),
        });
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-audit-failure");
        let request = || IssueTokenRequest {
            name: "demo-token".to_string(),
            agent_token: raw_agent_token.clone(),
            read_prefixes: None,
            write_prefixes: None,
        };

        let response = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
            Path(workspace.id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], "audit append failed after mutation");
        assert_eq!(body["mutation_committed"], true);
        assert_eq!(body["audit_recorded"], false);
        assert_eq!(body["workspace_token_revoked"], true);
        assert_eq!(body["idempotency_recorded"], true);
        assert_eq!(body["replayable"], true);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("audit write failed"));
        assert!(!rendered.contains(&raw_agent_token));

        let replay =
            issue_workspace_token(State(state), headers, Path(workspace.id), Json(request()))
                .await
                .into_response();
        assert_eq!(replay.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            replay
                .headers()
                .get(http_idempotency::IDEMPOTENCY_REPLAY_HEADER),
            Some(&axum::http::HeaderValue::from_static(
                http_idempotency::IDEMPOTENCY_REPLAY_HEADER_VALUE
            ))
        );
        assert_eq!(response_json(replay).await, body);
    }

    #[tokio::test]
    async fn issue_workspace_token_encrypt_failure_revokes_and_replays_terminal_failure() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: Some(Arc::new(FailingEncryptKms::new())),
        });
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-encrypt-failure");
        let request = || IssueTokenRequest {
            name: "demo-token".to_string(),
            agent_token: raw_agent_token.clone(),
            read_prefixes: None,
            write_prefixes: None,
        };

        let response = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
            Path(workspace.id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], WORKSPACE_TOKEN_IDEMPOTENCY_FAILURE);
        assert_eq!(body["workspace_token_revoked"], true);
        assert_eq!(body["compensation_audit_recorded"], true);
        assert_eq!(body["idempotency_recorded"], true);
        assert_eq!(body["replayable"], true);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("raw KMS provider failure detail"));
        assert!(!rendered.contains(&raw_agent_token));

        let replay =
            issue_workspace_token(State(state), headers, Path(workspace.id), Json(request()))
                .await
                .into_response();
        assert_eq!(replay.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            replay
                .headers()
                .get(http_idempotency::IDEMPOTENCY_REPLAY_HEADER),
            Some(&axum::http::HeaderValue::from_static(
                http_idempotency::IDEMPOTENCY_REPLAY_HEADER_VALUE
            ))
        );
        assert_eq!(response_json(replay).await, body);
    }

    #[tokio::test]
    async fn issue_workspace_token_revoke_compensation_failure_is_explicit_and_replayed() {
        let db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&db, "ci-agent").await;
        let workspaces = Arc::new(FailingRevokeWorkspaceStore::default());
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: workspaces.clone(),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: Some(Arc::new(FailingEncryptKms::new())),
        });
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-revoke-failure");
        let request = || IssueTokenRequest {
            name: "demo-token".to_string(),
            agent_token: raw_agent_token.clone(),
            read_prefixes: None,
            write_prefixes: None,
        };

        let response = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
            Path(workspace.id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], WORKSPACE_TOKEN_COMPENSATION_FAILURE);
        assert_eq!(body["workspace_token_revoked"], false);
        assert_eq!(body["idempotency_recorded"], true);
        assert_eq!(body["replayable"], true);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("raw revoke provider failure detail"));
        assert!(!rendered.contains("raw KMS provider failure detail"));
        assert!(!rendered.contains(&raw_agent_token));

        let issued = workspaces.issued.lock().unwrap().clone();
        assert_eq!(issued.len(), 1);
        assert!(
            workspaces
                .validate_workspace_token(workspace.id, &issued[0].raw_secret)
                .await
                .unwrap()
                .is_some(),
            "failed compensation leaves the token active and must be explicit"
        );

        let replay = issue_workspace_token(
            State(state.clone()),
            headers,
            Path(workspace.id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            replay
                .headers()
                .get(http_idempotency::IDEMPOTENCY_REPLAY_HEADER),
            Some(&axum::http::HeaderValue::from_static(
                http_idempotency::IDEMPOTENCY_REPLAY_HEADER_VALUE
            ))
        );
        assert_eq!(response_json(replay).await, body);
        assert_eq!(workspaces.issued.lock().unwrap().len(), 1);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn issue_workspace_token_idempotency_replays_with_postgres_stores() {
        let Some(pg) = PostgresRouteTestDb::new().await else {
            return;
        };
        let core_db = StratumDb::open_memory();
        let raw_agent_token = add_agent_token(&core_db, "ci-agent").await;
        let store = pg.store.clone();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(core_db.clone()),
            db: ServerLocalDb::available(Arc::new(core_db)),
            workspaces: store.clone(),
            idempotency: store.clone(),
            audit: store.clone(),
            review: store,
            secret_replay_kms: Some(test_kms("workspace-token-postgres", 11)),
        });
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let headers = root_headers_with_idempotency("workspace-token-postgres-replay");
        let request = || IssueTokenRequest {
            name: "demo-token".to_string(),
            agent_token: raw_agent_token.clone(),
            read_prefixes: Some(vec!["/demo/read/./".to_string()]),
            write_prefixes: Some(vec!["/demo/write".to_string()]),
        };

        let first = issue_workspace_token(
            State(state.clone()),
            headers.clone(),
            Path(workspace.id),
            Json(request()),
        )
        .await
        .into_response();

        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;
        let workspace_token = first_body["workspace_token"].as_str().unwrap().to_string();
        let token_id = first_body["token_id"].as_str().unwrap().to_string();
        let scope = workspace_token_idempotency_scope(
            &resolve_admin_repo_context(
                &state,
                &headers,
                &require_admin(&state, &headers).await.unwrap(),
            )
            .unwrap(),
            workspace.id,
        );
        let (classification, stored_body, envelope_version, key_id, aad_hash) =
            pg.idempotency_row(&scope).await;
        assert_eq!(classification, "secret_bearing");
        assert_eq!(envelope_version, Some(1));
        assert!(key_id.is_some());
        assert!(aad_hash.as_deref().is_some_and(|hash| hash.len() == 64));
        assert!(stored_body.contains("ciphertext_b64"));
        assert!(!stored_body.contains(&workspace_token));
        assert!(!stored_body.contains(&raw_agent_token));

        let replay = issue_workspace_token(
            State(state.clone()),
            headers,
            Path(workspace.id),
            Json(request()),
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        let replay_body = response_json(replay).await;
        assert_eq!(replay_body, first_body);
        assert_eq!(replay_body["token_id"], token_id);
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);

        pg.cleanup().await;
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
    async fn issue_workspace_token_authenticates_backing_agent_through_core_runtime() {
        let core_db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &core_db
                .execute_command("addagent core-agent", &mut root)
                .await
                .unwrap(),
        );
        let local_only_db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(core_db),
            db: ServerLocalDb::available(Arc::new(local_only_db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
        });
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
                read_prefixes: None,
                write_prefixes: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_can_revoke_workspace_token_and_validation_fails() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = state
            .workspaces
            .issue_workspace_token(workspace.id, "demo-token", agent.uid)
            .await
            .unwrap();

        assert!(
            state
                .workspaces
                .validate_workspace_token(workspace.id, &issued.raw_secret)
                .await
                .unwrap()
                .is_some()
        );
        let response = revoke_workspace_token(
            State(state.clone()),
            root_headers(),
            Path((workspace.id, issued.token.id)),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["workspace_id"], serde_json::json!(workspace.id));
        assert_eq!(body["token_id"], serde_json::json!(issued.token.id));
        assert_eq!(body["token_version"], serde_json::json!(2));
        assert!(body["revoked_at_unix"].as_u64().is_some());
        assert!(body.get("workspace_token").is_none());
        assert!(body.get("raw_secret").is_none());
        assert!(body.get("secret_hash").is_none());
        assert!(
            state
                .workspaces
                .validate_workspace_token(workspace.id, &issued.raw_secret)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn non_admin_cannot_revoke_workspace_token() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser alice", &mut root)
            .await
            .unwrap();
        let state = test_state(db);

        let response = revoke_workspace_token(
            State(state),
            {
                let mut headers = HeaderMap::new();
                headers.insert("authorization", "User alice".parse().unwrap());
                headers
            },
            Path((Uuid::new_v4(), Uuid::new_v4())),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn scoped_workspace_bearer_cannot_revoke_workspace_token() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = state
            .workspaces
            .issue_workspace_token(workspace.id, "demo-token", agent.uid)
            .await
            .unwrap();

        let response = revoke_workspace_token(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
            Path((workspace.id, issued.token.id)),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn revoke_rejects_idempotency_key_without_mutation() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = state
            .workspaces
            .issue_workspace_token(workspace.id, "demo-token", agent.uid)
            .await
            .unwrap();

        let response = revoke_workspace_token(
            State(state.clone()),
            root_headers_with_idempotency("workspace-token-revoke"),
            Path((workspace.id, issued.token.id)),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            state
                .workspaces
                .validate_workspace_token(workspace.id, &issued.raw_secret)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn revoke_audit_omits_raw_token_and_secret_hash() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let state = test_state(db);
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = state
            .workspaces
            .issue_workspace_token(workspace.id, "demo-token", agent.uid)
            .await
            .unwrap();
        let secret_hash = issued.token.secret_hash.clone();

        let response = revoke_workspace_token(
            State(state.clone()),
            root_headers(),
            Path((workspace.id, issued.token.id)),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let audits = state.audit.list_recent(10).await.unwrap();
        let event = audits
            .iter()
            .find(|event| event.action == crate::audit::AuditAction::WorkspaceTokenRevoke)
            .expect("revoke audit event");
        let token_id_text = issued.token.id.to_string();
        let workspace_id_text = workspace.id.to_string();
        let principal_uid_text = agent.uid.to_string();
        assert_eq!(event.resource.id.as_deref(), Some(token_id_text.as_str()));
        assert_eq!(
            event.details.get("workspace_id").map(String::as_str),
            Some(workspace_id_text.as_str())
        );
        assert_eq!(
            event.details.get("principal_uid").map(String::as_str),
            Some(principal_uid_text.as_str())
        );
        assert_eq!(
            event.details.get("token_version").map(String::as_str),
            Some("2")
        );
        let audit_text = serde_json::to_string(event).unwrap();
        assert!(!audit_text.contains(&issued.raw_secret));
        assert!(!audit_text.contains(&secret_hash));
        assert!(!event.details.contains_key("request_body"));
    }

    #[tokio::test]
    async fn create_workspace_audit_failure_reports_committed_mutation() {
        let db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingAuditStore),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
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
        assert!(!events[0].details.contains_key("read_prefixes"));
        assert!(!events[0].details.contains_key("write_prefixes"));
        assert_eq!(
            events[0]
                .details
                .get("read_prefix_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            events[0]
                .details
                .get("write_prefix_count")
                .map(String::as_str),
            Some("1")
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains(&raw_agent_token));
        assert!(!audit_json.contains(workspace_token));
        assert!(!audit_json.contains("/demo"));
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
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
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
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(LocalWorkspaceMetadataStore::open(&path).unwrap()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
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
