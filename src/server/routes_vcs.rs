use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::Deserialize;

use uuid::Uuid;

use super::AppState;
use super::idempotency as http_idempotency;
use super::middleware::session_from_headers;
use crate::audit::{AuditAction, AuditOutcome, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, WHEEL_GID};
use crate::error::VfsError;
use crate::idempotency::{IdempotencyBegin, IdempotencyReservation, request_fingerprint};

const VCS_COMMIT_IDEMPOTENCY_ROUTE: &str = "POST /vcs/commit";
const VCS_REVERT_IDEMPOTENCY_ROUTE: &str = "POST /vcs/revert";
const VCS_CREATE_REF_IDEMPOTENCY_ROUTE: &str = "POST /vcs/refs";
const VCS_UPDATE_REF_IDEMPOTENCY_ROUTE: &str = "PATCH /vcs/refs/{name}";

#[derive(Deserialize)]
pub struct CommitRequest {
    pub message: String,
}

#[derive(Deserialize)]
pub struct RevertRequest {
    pub hash: String,
}

#[derive(Deserialize)]
pub struct DiffQuery {
    pub path: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateRefRequest {
    pub name: String,
    pub target: String,
}

#[derive(Deserialize)]
pub struct UpdateRefRequest {
    pub target: String,
    pub expected_target: String,
    pub expected_version: u64,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/vcs/commit", post(vcs_commit))
        .route("/vcs/log", get(vcs_log))
        .route("/vcs/revert", post(vcs_revert))
        .route("/vcs/status", get(vcs_status))
        .route("/vcs/diff", get(vcs_diff))
        .route("/vcs/refs", get(vcs_list_refs).post(vcs_create_ref))
        .route("/vcs/refs/{*name}", patch(vcs_update_ref))
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn json_response(status: StatusCode, body: serde_json::Value) -> axum::response::Response {
    (status, Json(body)).into_response()
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::AlreadyExists { .. } => StatusCode::CONFLICT,
        VfsError::InvalidArgs { message }
            if message.starts_with("ref compare-and-swap mismatch") =>
        {
            StatusCode::CONFLICT
        }
        VfsError::InvalidArgs { .. } => StatusCode::BAD_REQUEST,
        VfsError::IoError(_) | VfsError::CorruptStore { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => fallback,
    }
}

fn require_admin_session(session: &Session) -> Result<(), VfsError> {
    if session.scope.is_some() {
        return Err(VfsError::PermissionDenied {
            path: "vcs refs".to_string(),
        });
    }

    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "vcs refs".to_string(),
        });
    }

    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "vcs refs".to_string(),
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

fn require_vcs_mutation_session(session: &Session) -> Result<(), VfsError> {
    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "admin operation".to_string(),
        });
    }

    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "admin operation".to_string(),
            });
        }
    }

    Ok(())
}

async fn require_unprotected_ref(
    state: &AppState,
    ref_name: &str,
) -> Result<(), axum::response::Response> {
    let rules = state.review.list_protected_ref_rules().await.map_err(|e| {
        err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response()
    })?;

    if rules
        .iter()
        .any(|rule| rule.active && rule.ref_name == ref_name)
    {
        return Err(err_json(
            StatusCode::FORBIDDEN,
            format!("protected ref '{ref_name}' requires change request merge"),
        )
        .into_response());
    }

    Ok(())
}

async fn require_unprotected_revert_paths(
    state: &AppState,
    hash_prefix: &str,
) -> Result<(String, Vec<crate::review::ProtectedPathRule>), axum::response::Response> {
    let target_hash = state
        .core
        .resolve_commit_hash(hash_prefix)
        .await
        .map_err(|e| {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        })?;

    let rules = state
        .review
        .list_protected_path_rules()
        .await
        .map_err(|e| {
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        })?;
    let applicable_rules: Vec<_> = rules
        .into_iter()
        .filter(|rule| {
            rule.active
                && rule
                    .target_ref
                    .as_deref()
                    .is_none_or(|target_ref| target_ref == crate::vcs::MAIN_REF)
        })
        .collect();
    if applicable_rules.is_empty() {
        return Ok((target_hash, applicable_rules));
    }

    let changed_paths = state
        .core
        .changed_paths_for_revert(&target_hash)
        .await
        .map_err(|e| {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        })?;
    if changed_paths.is_empty() {
        return Ok((target_hash, applicable_rules));
    }

    for path in &changed_paths {
        let blocked = applicable_rules.iter().any(|rule| rule.matches_path(path));
        if blocked {
            return Err(err_json(
                StatusCode::FORBIDDEN,
                format!("protected path requires change request merge: '{path}'"),
            )
            .into_response());
        }
    }

    Ok((target_hash, applicable_rules))
}

fn audit_append_failed_response_parts(error: VfsError) -> (StatusCode, serde_json::Value) {
    (
        error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
        serde_json::json!({
            "error": format!("audit append failed after mutation: {error}"),
            "mutation_committed": true,
            "audit_recorded": false,
        }),
    )
}

fn ref_json(vcs_ref: crate::db::DbVcsRef) -> serde_json::Value {
    serde_json::json!({
        "name": vcs_ref.name,
        "target": vcs_ref.target,
        "version": vcs_ref.version,
    })
}

fn workspace_id_from_headers(headers: &HeaderMap) -> Result<Option<Uuid>, VfsError> {
    let Some(value) = headers.get("x-stratum-workspace") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| VfsError::InvalidArgs {
        message: "invalid x-stratum-workspace header".to_string(),
    })?;
    let id = Uuid::parse_str(value).map_err(|_| VfsError::InvalidArgs {
        message: format!("invalid workspace id: {value}"),
    })?;
    Ok(Some(id))
}

enum VcsIdempotency {
    Execute(Option<IdempotencyReservation>),
    Respond(axum::response::Response),
}

fn vcs_idempotency_scope(route: &str) -> String {
    route.to_string()
}

fn actor_fingerprint(session: &Session) -> serde_json::Value {
    serde_json::json!({
        "principal_uid": session.uid,
        "principal_username": session.username,
        "effective_uid": session.effective_uid(),
        "delegate": session.delegate.as_ref().map(|delegate| {
            serde_json::json!({
                "uid": delegate.uid,
                "gid": delegate.gid,
                "groups": delegate.groups,
                "username": delegate.username,
            })
        }),
    })
}

async fn begin_vcs_idempotency(
    state: &AppState,
    headers: &HeaderMap,
    scope: &str,
    fingerprint_body: serde_json::Value,
) -> VcsIdempotency {
    let idempotency_key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(key) => key,
        Err(e) => {
            return VcsIdempotency::Respond(
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response(),
            );
        }
    };

    let Some(key) = idempotency_key else {
        return VcsIdempotency::Execute(None);
    };

    let fingerprint = match request_fingerprint(scope, &fingerprint_body) {
        Ok(fingerprint) => fingerprint,
        Err(e) => {
            return VcsIdempotency::Respond(
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response(),
            );
        }
    };

    match state.idempotency.begin(scope, &key, &fingerprint).await {
        Ok(IdempotencyBegin::Execute(reservation)) => VcsIdempotency::Execute(Some(reservation)),
        Ok(IdempotencyBegin::Replay(record)) => {
            VcsIdempotency::Respond(http_idempotency::idempotency_json_replay_response(record))
        }
        Ok(IdempotencyBegin::Conflict) => {
            VcsIdempotency::Respond(http_idempotency::idempotency_conflict_response())
        }
        Ok(IdempotencyBegin::InProgress) => {
            VcsIdempotency::Respond(http_idempotency::idempotency_in_progress_response())
        }
        Err(e) => VcsIdempotency::Respond(
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response(),
        ),
    }
}

async fn complete_vcs_idempotency(
    state: &AppState,
    reservation: Option<&IdempotencyReservation>,
    status: StatusCode,
    body: &serde_json::Value,
) -> Result<(), axum::response::Response> {
    if let Some(reservation) = reservation {
        state
            .idempotency
            .complete(reservation, status.as_u16(), body.clone())
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

async fn abort_vcs_idempotency(state: &AppState, reservation: Option<&IdempotencyReservation>) {
    if let Some(reservation) = reservation {
        state.idempotency.abort(reservation).await;
    }
}

async fn update_workspace_head_from_headers(
    state: &AppState,
    headers: &HeaderMap,
    head_commit: Option<String>,
) -> Result<(), VfsError> {
    let Some(workspace_id) = workspace_id_from_headers(headers)? else {
        return Ok(());
    };
    match state
        .workspaces
        .update_head_commit(workspace_id, head_commit)
        .await?
    {
        Some(_) => Ok(()),
        None => Err(VfsError::NotFound {
            path: format!("workspace:{workspace_id}"),
        }),
    }
}

async fn append_workspace_head_partial_audit_event(
    state: &AppState,
    session: &Session,
    action: AuditAction,
    resource: AuditResource,
    workspace_id: Uuid,
    error: &VfsError,
) -> Result<(), VfsError> {
    let status = error_status(error, StatusCode::INTERNAL_SERVER_ERROR);
    state
        .audit
        .append(
            NewAuditEvent::from_session(session, action, resource)
                .with_outcome(AuditOutcome::Partial)
                .with_detail("workspace_id", workspace_id)
                .with_detail("failed_step", "workspace_head_update")
                .with_detail("status", status.as_str())
                .with_detail("error", error.to_string()),
        )
        .await
        .map(|_| ())
}

async fn validate_workspace_header(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<Uuid>, VfsError> {
    let Some(workspace_id) = workspace_id_from_headers(headers)? else {
        return Ok(None);
    };
    match state.workspaces.get_workspace(workspace_id).await? {
        Some(_) => Ok(Some(workspace_id)),
        None => Err(VfsError::NotFound {
            path: format!("workspace:{workspace_id}"),
        }),
    }
}

async fn vcs_list_refs(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.core.list_refs().await {
        Ok(refs) => Json(serde_json::json!({
            "refs": refs.into_iter().map(ref_json).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn vcs_create_ref(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateRefRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let scope = vcs_idempotency_scope(VCS_CREATE_REF_IDEMPOTENCY_ROUTE);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        serde_json::json!({
            "route": VCS_CREATE_REF_IDEMPOTENCY_ROUTE,
            "actor": actor_fingerprint(&session),
            "workspace_id": null,
            "name": &req.name,
            "target": &req.target,
            "expected_target": null,
            "expected_version": null,
        }),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };

    match state.core.create_ref(&req.name, &req.target).await {
        Ok(vcs_ref) => {
            let body = ref_json(vcs_ref.clone());
            let audit_event = NewAuditEvent::from_session(
                &session,
                AuditAction::VcsRefCreate,
                AuditResource::id(AuditResourceKind::Ref, &vcs_ref.name),
            )
            .with_detail("target", &vcs_ref.target)
            .with_detail("version", vcs_ref.version);
            if let Err(e) = state.audit.append(audit_event).await {
                let (status, body) = audit_append_failed_response_parts(e);
                if let Err(response) =
                    complete_vcs_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_vcs_idempotency(&state, reservation.as_ref(), StatusCode::CREATED, &body)
                    .await
            {
                return response;
            }
            json_response(StatusCode::CREATED, body)
        }
        Err(e) => {
            abort_vcs_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn vcs_update_ref(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(req): Json<UpdateRefRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    if let Err(response) = require_unprotected_ref(&state, &name).await {
        return response;
    }

    let scope = vcs_idempotency_scope(VCS_UPDATE_REF_IDEMPOTENCY_ROUTE);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        serde_json::json!({
            "route": VCS_UPDATE_REF_IDEMPOTENCY_ROUTE,
            "actor": actor_fingerprint(&session),
            "workspace_id": null,
            "name": &name,
            "target": &req.target,
            "expected_target": &req.expected_target,
            "expected_version": req.expected_version,
        }),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };

    match state
        .core
        .update_ref(
            &name,
            &req.expected_target,
            req.expected_version,
            &req.target,
        )
        .await
    {
        Ok(vcs_ref) => {
            let body = ref_json(vcs_ref.clone());
            let audit_event = NewAuditEvent::from_session(
                &session,
                AuditAction::VcsRefUpdate,
                AuditResource::id(AuditResourceKind::Ref, &vcs_ref.name),
            )
            .with_detail("expected_target", &req.expected_target)
            .with_detail("expected_version", req.expected_version)
            .with_detail("target", &vcs_ref.target)
            .with_detail("version", vcs_ref.version);
            if let Err(e) = state.audit.append(audit_event).await {
                let (status, body) = audit_append_failed_response_parts(e);
                if let Err(response) =
                    complete_vcs_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_vcs_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body).await
            {
                return response;
            }
            json_response(StatusCode::OK, body)
        }
        Err(e) => {
            abort_vcs_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn vcs_commit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let workspace_id = match validate_workspace_header(&state, &headers).await {
        Ok(workspace_id) => workspace_id,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    if let Err(e) = require_vcs_mutation_session(&session) {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }
    if let Err(response) = require_unprotected_ref(&state, crate::vcs::MAIN_REF).await {
        return response;
    }

    let scope = vcs_idempotency_scope(VCS_COMMIT_IDEMPOTENCY_ROUTE);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        serde_json::json!({
            "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
            "actor": actor_fingerprint(&session),
            "workspace_id": workspace_id,
            "message": &req.message,
        }),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };

    match state.core.commit_as(&req.message, &session).await {
        Ok(hash) => {
            let mut event = NewAuditEvent::from_session(
                &session,
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Commit, &hash),
            )
            .with_detail("author", &session.username);
            if let Some(workspace_id) = workspace_id {
                event = event.with_detail("workspace_id", workspace_id);
            }
            if let Err(e) =
                update_workspace_head_from_headers(&state, &headers, Some(hash.clone())).await
            {
                let (status, body) = if let Some(workspace_id) = workspace_id {
                    match append_workspace_head_partial_audit_event(
                        &state,
                        &session,
                        AuditAction::VcsCommit,
                        AuditResource::id(AuditResourceKind::Commit, &hash),
                        workspace_id,
                        &e,
                    )
                    .await
                    {
                        Ok(()) => (
                            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                            serde_json::json!({"error": e.to_string()}),
                        ),
                        Err(audit_error) => audit_append_failed_response_parts(audit_error),
                    }
                } else {
                    (
                        error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                        serde_json::json!({"error": e.to_string()}),
                    )
                };
                if let Err(response) =
                    complete_vcs_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            let body = serde_json::json!({
                "hash": hash,
                "message": &req.message,
                "author": session.username,
            });
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_response_parts(e);
                if let Err(response) =
                    complete_vcs_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_vcs_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body).await
            {
                return response;
            }
            json_response(StatusCode::OK, body)
        }
        Err(e) => {
            abort_vcs_idempotency(&state, reservation.as_ref()).await;
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        }
    }
}

async fn vcs_log(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let commits = match state.core.vcs_log_as(&session).await {
        Ok(commits) => commits,
        Err(e) => {
            return err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response();
        }
    };
    let items: Vec<serde_json::Value> = commits
        .iter()
        .map(|c| {
            serde_json::json!({
                "hash": c.id.short_hex(),
                "message": c.message,
                "author": c.author,
                "timestamp": c.timestamp,
            })
        })
        .collect();
    Json(serde_json::json!({"commits": items})).into_response()
}

async fn vcs_revert(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RevertRequest>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let workspace_id = match validate_workspace_header(&state, &headers).await {
        Ok(workspace_id) => workspace_id,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    if let Err(e) = require_vcs_mutation_session(&session) {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }
    if let Err(response) = require_unprotected_ref(&state, crate::vcs::MAIN_REF).await {
        return response;
    }
    let (revert_target, applicable_path_rules) =
        match require_unprotected_revert_paths(&state, &req.hash).await {
            Ok(target) => target,
            Err(response) => return response,
        };

    let scope = vcs_idempotency_scope(VCS_REVERT_IDEMPOTENCY_ROUTE);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        serde_json::json!({
            "route": VCS_REVERT_IDEMPOTENCY_ROUTE,
            "actor": actor_fingerprint(&session),
            "workspace_id": workspace_id,
            "hash": &req.hash,
        }),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };

    let final_path_rules = applicable_path_rules.clone();
    let is_protected_path: crate::server::core::ProtectedPathPredicate =
        std::sync::Arc::new(move |path| {
            final_path_rules.iter().any(|rule| rule.matches_path(path))
        });
    match state
        .core
        .revert_as_with_path_check(&revert_target, &session, is_protected_path)
        .await
    {
        Ok(reverted_to) => {
            let mut event = NewAuditEvent::from_session(
                &session,
                AuditAction::VcsRevert,
                AuditResource::id(AuditResourceKind::Commit, &reverted_to),
            );
            if let Some(workspace_id) = workspace_id {
                event = event.with_detail("workspace_id", workspace_id);
            }
            if let Err(e) =
                update_workspace_head_from_headers(&state, &headers, Some(reverted_to.clone()))
                    .await
            {
                let (status, body) = if let Some(workspace_id) = workspace_id {
                    match append_workspace_head_partial_audit_event(
                        &state,
                        &session,
                        AuditAction::VcsRevert,
                        AuditResource::id(AuditResourceKind::Commit, &reverted_to),
                        workspace_id,
                        &e,
                    )
                    .await
                    {
                        Ok(()) => (
                            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                            serde_json::json!({"error": e.to_string()}),
                        ),
                        Err(audit_error) => audit_append_failed_response_parts(audit_error),
                    }
                } else {
                    (
                        error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                        serde_json::json!({"error": e.to_string()}),
                    )
                };
                if let Err(response) =
                    complete_vcs_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            let body = serde_json::json!({"reverted_to": &reverted_to});
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_response_parts(e);
                if let Err(response) =
                    complete_vcs_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_vcs_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body).await
            {
                return response;
            }
            json_response(StatusCode::OK, body)
        }
        Err(e) => {
            abort_vcs_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn vcs_status(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    match state.core.vcs_status_as(&session).await {
        Ok(status) => (StatusCode::OK, status).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn vcs_diff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DiffQuery>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    match state
        .core
        .vcs_diff_as(query.path.as_deref(), &session)
        .await
    {
        Ok(diff) => (StatusCode::OK, diff).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ROOT_UID;
    use crate::auth::Uid;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::server::core::LocalCoreRuntime;
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, ValidWorkspaceToken,
        WorkspaceMetadataStore, WorkspaceRecord,
    };
    use axum::extract::Path;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        })
    }

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
        headers
    }

    fn user_headers_with_idempotency(username: &str, key: &str) -> HeaderMap {
        let mut headers = user_headers(username);
        headers.insert("idempotency-key", key.parse().unwrap());
        headers
    }

    fn workspace_headers(username: &str, workspace_id: Uuid) -> HeaderMap {
        let mut headers = user_headers(username);
        headers.insert(
            "x-stratum-workspace",
            workspace_id.to_string().parse().unwrap(),
        );
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

    async fn json_body(response: axum::response::Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn commit_file(
        db: &StratumDb,
        root: &mut Session,
        path: &str,
        contents: &str,
        message: &str,
    ) -> String {
        db.execute_command(&format!("touch {path}"), root)
            .await
            .unwrap();
        db.execute_command(&format!("write {path} {contents}"), root)
            .await
            .unwrap();
        db.commit(message, "root").await.unwrap();
        db.vcs_log().await[0].id.to_hex()
    }

    #[tokio::test]
    async fn vcs_routes_use_local_core_runtime() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        commit_file(&db, &mut root, "/core-vcs.txt", "first", "first").await;
        let state = test_state(db);

        let commit_response = vcs_commit(
            State(state.clone()),
            user_headers("root"),
            Json(CommitRequest {
                message: "route core commit".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(commit_response.status(), StatusCode::OK);

        let log_response = vcs_log(State(state), user_headers("root"))
            .await
            .into_response();
        assert_eq!(log_response.status(), StatusCode::OK);
        let body = json_body(log_response).await;
        assert_eq!(body["commits"][0]["message"], "route core commit");
    }

    #[tokio::test]
    async fn commit_audits_hash_without_commit_message() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.txt", &mut root).await.unwrap();
        db.execute_command("write a.txt content", &mut root)
            .await
            .unwrap();
        let state = test_state(db);
        let sensitive_message = "sensitive-review-context";

        let response = vcs_commit(
            State(state.clone()),
            user_headers("root"),
            Json(CommitRequest {
                message: sensitive_message.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        let hash = body["hash"].as_str().expect("commit hash");
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsCommit);
        assert_eq!(events[0].resource.id.as_deref(), Some(hash));
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains(sensitive_message));
    }

    #[tokio::test]
    async fn admin_can_create_list_and_update_refs_over_http() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        let state = test_state(db);

        let create_response = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: "agent/alice/session-1".to_string(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let created = json_body(create_response).await;
        assert_eq!(
            created.get("name"),
            Some(&serde_json::json!("agent/alice/session-1"))
        );
        assert_eq!(created.get("target"), Some(&serde_json::json!(first)));
        assert_eq!(created.get("version"), Some(&serde_json::json!(1)));

        let list_response = vcs_list_refs(State(state.clone()), user_headers("root"))
            .await
            .into_response();
        assert_eq!(list_response.status(), StatusCode::OK);
        let refs = json_body(list_response).await;
        let refs = refs
            .get("refs")
            .and_then(serde_json::Value::as_array)
            .expect("refs array");
        assert!(
            refs.iter()
                .any(|item| item.get("name") == Some(&serde_json::json!("main")))
        );
        assert!(
            refs.iter()
                .any(|item| item.get("name") == Some(&serde_json::json!("agent/alice/session-1")))
        );

        let update_response = vcs_update_ref(
            State(state),
            user_headers("root"),
            Path("agent/alice/session-1".to_string()),
            Json(UpdateRefRequest {
                target: second.clone(),
                expected_target: first,
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(update_response.status(), StatusCode::OK);
        let updated = json_body(update_response).await;
        assert_eq!(updated.get("target"), Some(&serde_json::json!(second)));
        assert_eq!(updated.get("version"), Some(&serde_json::json!(2)));
    }

    #[tokio::test]
    async fn duplicate_create_and_stale_ref_update_conflict_without_mutation() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        let third = commit_file(&db, &mut root, "/a.txt", "third", "third").await;
        let state = test_state(db);
        let name = "agent/alice/session-1".to_string();

        let created = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: name.clone(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(created.status(), StatusCode::CREATED);

        let duplicate = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: name.clone(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);

        let updated = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(name.clone()),
            Json(UpdateRefRequest {
                target: second.clone(),
                expected_target: first.clone(),
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(updated.status(), StatusCode::OK);

        let stale = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(name.clone()),
            Json(UpdateRefRequest {
                target: third,
                expected_target: first.clone(),
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(stale.status(), StatusCode::CONFLICT);

        let stale_unknown_target = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(name.clone()),
            Json(UpdateRefRequest {
                target: "0".repeat(64),
                expected_target: first,
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(stale_unknown_target.status(), StatusCode::CONFLICT);

        let refs = json_body(
            vcs_list_refs(State(state), user_headers("root"))
                .await
                .into_response(),
        )
        .await;
        let current = refs
            .get("refs")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .find(|item| item.get("name") == Some(&serde_json::json!(name)))
            .expect("session ref exists");
        assert_eq!(current.get("target"), Some(&serde_json::json!(second)));
        assert_eq!(current.get("version"), Some(&serde_json::json!(2)));
    }

    #[tokio::test]
    async fn create_ref_idempotency_key_replays_original_created_response() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let state = test_state(db);
        let headers = user_headers_with_idempotency("root", "vcs-create-ref-replay");
        let request = || CreateRefRequest {
            name: "agent/alice/session-replay".to_string(),
            target: first.clone(),
        };

        let first_response = vcs_create_ref(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first_response.status(), StatusCode::CREATED);
        let first_body = json_body(first_response).await;

        let replay_response = vcs_create_ref(State(state.clone()), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay_response.status(), StatusCode::CREATED);
        assert_eq!(
            replay_response
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        let replay_body = json_body(replay_response).await;
        assert_eq!(replay_body, first_body);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsRefCreate);
    }

    #[tokio::test]
    async fn update_ref_idempotency_key_replays_original_response_despite_stale_cas() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        let state = test_state(db);
        let name = "agent/alice/session-update-replay".to_string();
        let headers = user_headers_with_idempotency("root", "vcs-update-ref-replay");
        let created = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: name.clone(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(created.status(), StatusCode::CREATED);
        let request = || UpdateRefRequest {
            target: second.clone(),
            expected_target: first.clone(),
            expected_version: 1,
        };

        let first_response = vcs_update_ref(
            State(state.clone()),
            headers.clone(),
            Path(name.clone()),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = json_body(first_response).await;

        let replay_response =
            vcs_update_ref(State(state.clone()), headers, Path(name), Json(request()))
                .await
                .into_response();
        assert_eq!(replay_response.status(), StatusCode::OK);
        assert_eq!(
            replay_response
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        let replay_body = json_body(replay_response).await;
        assert_eq!(replay_body, first_body);

        let events = state.audit.list_recent(10).await.unwrap();
        let update_events = events
            .iter()
            .filter(|event| event.action == crate::audit::AuditAction::VcsRefUpdate)
            .count();
        assert_eq!(update_events, 1);
    }

    #[tokio::test]
    async fn protected_ref_rules_block_direct_vcs_mutations() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        db.execute_command("write a.txt third", &mut root)
            .await
            .unwrap();
        db.create_ref("review/cr-1", &first).await.unwrap();
        let state = test_state(db.clone());
        state
            .review
            .create_protected_ref_rule(crate::vcs::MAIN_REF, 1, ROOT_UID)
            .await
            .unwrap();
        state
            .review
            .create_protected_ref_rule("review/cr-1", 1, ROOT_UID)
            .await
            .unwrap();

        let blocked_commit = vcs_commit(
            State(state.clone()),
            user_headers_with_idempotency("root", "protected-main-commit"),
            Json(CommitRequest {
                message: "blocked direct commit".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(blocked_commit.status(), StatusCode::FORBIDDEN);
        assert!(
            blocked_commit
                .headers()
                .get("x-stratum-idempotent-replay")
                .is_none()
        );
        let body = json_body(blocked_commit).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("protected ref"));
        assert!(error.contains(crate::vcs::MAIN_REF));
        assert_eq!(db.vcs_log().await.len(), 2);

        let blocked_revert = vcs_revert(
            State(state.clone()),
            user_headers("root"),
            Json(RevertRequest {
                hash: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(blocked_revert.status(), StatusCode::FORBIDDEN);
        let body = json_body(blocked_revert).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("protected ref"));
        assert!(error.contains(crate::vcs::MAIN_REF));

        let blocked_update = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path("review/cr-1".to_string()),
            Json(UpdateRefRequest {
                target: second,
                expected_target: first.clone(),
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(blocked_update.status(), StatusCode::FORBIDDEN);
        let body = json_body(blocked_update).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("protected ref"));
        assert!(error.contains("review/cr-1"));

        let refs = json_body(
            vcs_list_refs(State(state), user_headers("root"))
                .await
                .into_response(),
        )
        .await;
        let review_ref = refs["refs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item.get("name") == Some(&serde_json::json!("review/cr-1")))
            .expect("review ref exists");
        assert_eq!(review_ref.get("target"), Some(&serde_json::json!(first)));
        assert_eq!(review_ref.get("version"), Some(&serde_json::json!(1)));
    }

    #[tokio::test]
    async fn commit_idempotency_key_retries_without_second_commit_or_audit_event() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();
        let state = test_state(db.clone());
        let headers = user_headers_with_idempotency("root", "vcs-commit-replay");
        let request = || CommitRequest {
            message: "first commit".to_string(),
        };

        let first_response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = json_body(first_response).await;

        let replay_response = vcs_commit(State(state.clone()), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay_response.status(), StatusCode::OK);
        assert_eq!(
            replay_response
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        let replay_body = json_body(replay_response).await;
        assert_eq!(replay_body, first_body);
        assert_eq!(db.vcs_log().await.len(), 1);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsCommit);
    }

    #[tokio::test]
    async fn revert_idempotency_key_replays_original_response() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md version1", &mut root)
            .await
            .unwrap();
        let original = db.commit("v1", "root").await.unwrap();
        db.execute_command("write a.md version2", &mut root)
            .await
            .unwrap();
        db.commit("v2", "root").await.unwrap();
        let state = test_state(db);
        let headers = user_headers_with_idempotency("root", "vcs-revert-replay");
        let request = || RevertRequest {
            hash: original.clone(),
        };

        let first_response = vcs_revert(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = json_body(first_response).await;

        let replay_response = vcs_revert(State(state.clone()), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay_response.status(), StatusCode::OK);
        assert_eq!(
            replay_response
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        let replay_body = json_body(replay_response).await;
        assert_eq!(replay_body, first_body);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsRevert);
    }

    #[tokio::test]
    async fn revert_response_and_audit_use_resolved_commit_hash() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let original = commit_file(&db, &mut root, "/a.md", "version1", "v1").await;
        commit_file(&db, &mut root, "/a.md", "version2", "v2").await;
        let state = test_state(db);
        let prefix = &original[..8];

        let response = vcs_revert(
            State(state.clone()),
            user_headers("root"),
            Json(RevertRequest {
                hash: prefix.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["reverted_to"], original);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].resource.id.as_deref(), Some(original.as_str()));
    }

    #[tokio::test]
    async fn protected_path_revert_is_blocked_before_idempotency_replay_without_mutation_or_audit()
    {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/legal.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/legal.txt", "second", "second").await;
        let state = test_state(db.clone());
        state
            .review
            .create_protected_path_rule("/legal.txt", Some(crate::vcs::MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();
        let headers = user_headers_with_idempotency("root", "protected-path-revert-replay");
        let session = session_from_headers(&state, &headers).await.unwrap();
        let key = crate::idempotency::IdempotencyKey::parse_header_value(
            headers.get("idempotency-key").unwrap(),
        )
        .unwrap();
        let scope = vcs_idempotency_scope(VCS_REVERT_IDEMPOTENCY_ROUTE);
        let fingerprint = request_fingerprint(
            &scope,
            &serde_json::json!({
                "route": VCS_REVERT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": Option::<Uuid>::None,
                "hash": &first,
            }),
        )
        .unwrap();
        let reservation = match state
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected idempotency reservation, got {other:?}"),
        };
        state
            .idempotency
            .complete(
                &reservation,
                StatusCode::OK.as_u16(),
                serde_json::json!({"reverted_to": &first}),
            )
            .await
            .unwrap();
        let before_ref = db.get_ref(crate::vcs::MAIN_REF).await.unwrap().unwrap();
        assert_eq!(before_ref.target, second);

        let response = vcs_revert(
            State(state.clone()),
            headers,
            Json(RevertRequest {
                hash: first.clone(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(
            response
                .headers()
                .get("x-stratum-idempotent-replay")
                .is_none()
        );
        let body = json_body(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("protected path"));
        assert!(error.contains("/legal.txt"));
        assert_eq!(db.cat("/legal.txt").await.unwrap(), b"second");
        assert_eq!(
            db.get_ref(crate::vcs::MAIN_REF).await.unwrap().unwrap(),
            before_ref
        );
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn protected_path_nonmatching_rule_preserves_normal_revert_behavior() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/open.txt", "first", "first").await;
        commit_file(&db, &mut root, "/open.txt", "second", "second").await;
        let state = test_state(db.clone());
        state
            .review
            .create_protected_path_rule("/legal.txt", None, 1, ROOT_UID)
            .await
            .unwrap();

        let response = vcs_revert(
            State(state.clone()),
            user_headers("root"),
            Json(RevertRequest {
                hash: first.clone(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(db.cat("/open.txt").await.unwrap(), b"first");
        assert_eq!(
            db.get_ref(crate::vcs::MAIN_REF)
                .await
                .unwrap()
                .unwrap()
                .target,
            first
        );
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsRevert);
    }

    #[tokio::test]
    async fn same_idempotency_key_with_different_ref_request_conflicts_without_mutation() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        let state = test_state(db);
        let headers = user_headers_with_idempotency("root", "vcs-create-ref-conflict");
        let name = "agent/alice/session-conflict".to_string();

        let first_response = vcs_create_ref(
            State(state.clone()),
            headers.clone(),
            Json(CreateRefRequest {
                name: name.clone(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(first_response.status(), StatusCode::CREATED);

        let conflict_response = vcs_create_ref(
            State(state.clone()),
            headers,
            Json(CreateRefRequest {
                name: name.clone(),
                target: second,
            }),
        )
        .await
        .into_response();
        assert_eq!(conflict_response.status(), StatusCode::CONFLICT);
        let conflict_body = json_body(conflict_response).await;
        assert_eq!(
            conflict_body.get("error"),
            Some(&serde_json::json!(
                "Idempotency-Key was reused with a different request"
            ))
        );

        let refs = json_body(
            vcs_list_refs(State(state.clone()), user_headers("root"))
                .await
                .into_response(),
        )
        .await;
        let current = refs
            .get("refs")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .find(|item| item.get("name") == Some(&serde_json::json!(name)))
            .expect("session ref exists");
        assert_eq!(current.get("target"), Some(&serde_json::json!(first)));
        assert_eq!(current.get("version"), Some(&serde_json::json!(1)));

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsRefCreate);
    }

    #[tokio::test]
    async fn non_admin_and_workspace_bearer_cannot_manage_refs() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let commit = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let state = test_state(db.clone());

        let missing_auth = vcs_list_refs(State(state.clone()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

        let list_response = vcs_list_refs(State(state.clone()), user_headers("bob"))
            .await
            .into_response();
        assert_eq!(list_response.status(), StatusCode::FORBIDDEN);

        let create_response = vcs_create_ref(
            State(state.clone()),
            user_headers("bob"),
            Json(CreateRefRequest {
                name: "agent/bob/session-1".to_string(),
                target: commit,
            }),
        )
        .await
        .into_response();
        assert_eq!(create_response.status(), StatusCode::FORBIDDEN);

        let workspace_store = Arc::new(InMemoryWorkspaceMetadataStore::new());
        let workspace = workspace_store
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = workspace_store
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let scoped_state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: workspace_store,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let workspace_bearer = vcs_list_refs(
            State(scoped_state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();
        assert_eq!(workspace_bearer.status(), StatusCode::FORBIDDEN);
    }

    struct FailingHeadStore;

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for FailingHeadStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(Vec::new())
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, _id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            Ok(None)
        }

        async fn update_head_commit(
            &self,
            _id: Uuid,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            Err(VfsError::IoError(std::io::Error::other(
                "metadata write failed",
            )))
        }

        async fn issue_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    struct ExistingFailingHeadStore {
        workspace_id: Uuid,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for ExistingFailingHeadStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(Vec::new())
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id == self.workspace_id {
                Ok(Some(WorkspaceRecord {
                    id,
                    name: "demo".to_string(),
                    root_path: "/demo".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: crate::vcs::MAIN_REF.to_string(),
                    session_ref: None,
                }))
            } else {
                Ok(None)
            }
        }

        async fn update_head_commit(
            &self,
            _id: Uuid,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            Err(VfsError::IoError(std::io::Error::other(
                "metadata write failed",
            )))
        }

        async fn issue_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    struct RecordingWorkspaceStore {
        workspace_id: Uuid,
        updated: RwLock<Option<String>>,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for RecordingWorkspaceStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(Vec::new())
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id == self.workspace_id {
                Ok(Some(WorkspaceRecord {
                    id,
                    name: "demo".to_string(),
                    root_path: "/demo".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: crate::vcs::MAIN_REF.to_string(),
                    session_ref: None,
                }))
            } else {
                Ok(None)
            }
        }

        async fn update_head_commit(
            &self,
            id: Uuid,
            head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id != self.workspace_id {
                return Ok(None);
            }
            *self.updated.write().await = head_commit.clone();
            Ok(Some(WorkspaceRecord {
                id,
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                head_commit,
                version: 1,
                base_ref: crate::vcs::MAIN_REF.to_string(),
                session_ref: None,
            }))
        }

        async fn issue_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn non_root_user_cannot_revert_global_state() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md version1", &mut root)
            .await
            .unwrap();
        let original = db.commit("v1", "root").await.unwrap();
        db.execute_command("write a.md version2", &mut root)
            .await
            .unwrap();
        db.commit("v2", "root").await.unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let response = vcs_revert(
            State(test_state(db.clone())),
            user_headers("bob"),
            Json(RevertRequest { hash: original }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            String::from_utf8_lossy(&db.cat("/a.md").await.unwrap()),
            "version2"
        );
    }

    #[tokio::test]
    async fn non_root_user_cannot_read_global_vcs_log() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        db.commit("init", "root").await.unwrap();

        let response = vcs_log(State(test_state(db)), user_headers("bob"))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn non_root_user_cannot_read_global_vcs_status_or_diff() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md before", &mut root)
            .await
            .unwrap();
        db.commit("init", "root").await.unwrap();
        db.execute_command("write a.md after", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let status_response = vcs_status(State(test_state(db.clone())), user_headers("bob"))
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::FORBIDDEN);

        let diff_response = vcs_diff(
            State(test_state(db)),
            user_headers("bob"),
            Query(DiffQuery { path: None }),
        )
        .await
        .into_response();
        assert_eq!(diff_response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn non_root_user_cannot_commit_global_vcs_state() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let response = vcs_commit(
            State(test_state(db.clone())),
            user_headers("bob"),
            Json(CommitRequest {
                message: "blocked".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(db.vcs_log().await.len(), 0);
    }

    #[tokio::test]
    async fn workspace_head_update_failure_is_not_reported_as_success() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();
        let workspace_id = Uuid::new_v4();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(ExistingFailingHeadStore { workspace_id }),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let response = vcs_commit(
            State(state),
            workspace_headers("root", workspace_id),
            Json(CommitRequest {
                message: "with workspace".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn workspace_head_update_failure_still_audits_created_commit() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();
        let workspace_id = Uuid::new_v4();
        let sensitive_message = "sensitive workspace commit";
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(ExistingFailingHeadStore { workspace_id }),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let response = vcs_commit(
            State(state.clone()),
            workspace_headers("root", workspace_id),
            Json(CommitRequest {
                message: sensitive_message.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsCommit);
        assert_eq!(events[0].outcome, crate::audit::AuditOutcome::Partial);
        let expected_workspace_id = workspace_id.to_string();
        assert_eq!(
            events[0].details.get("workspace_id").map(String::as_str),
            Some(expected_workspace_id.as_str())
        );
        assert_eq!(
            events[0].details.get("failed_step").map(String::as_str),
            Some("workspace_head_update")
        );
        assert_eq!(
            events[0].details.get("status").map(String::as_str),
            Some(StatusCode::INTERNAL_SERVER_ERROR.as_str())
        );
        assert!(
            events[0]
                .details
                .get("error")
                .is_some_and(|error| error.contains("metadata write failed"))
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains(sensitive_message));
    }

    #[tokio::test]
    async fn revert_workspace_head_update_failure_audits_partial_outcome() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md version1", &mut root)
            .await
            .unwrap();
        let original = db.commit("v1", "root").await.unwrap();
        let original_full = db.vcs_log().await[0].id.to_hex();
        db.execute_command("write a.md version2", &mut root)
            .await
            .unwrap();
        db.commit("v2", "root").await.unwrap();
        let workspace_id = Uuid::new_v4();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(ExistingFailingHeadStore { workspace_id }),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let response = vcs_revert(
            State(state.clone()),
            workspace_headers("root", workspace_id),
            Json(RevertRequest {
                hash: original.clone(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::VcsRevert);
        assert_eq!(
            events[0].resource.id.as_deref(),
            Some(original_full.as_str())
        );
        assert_eq!(events[0].outcome, crate::audit::AuditOutcome::Partial);
        let expected_workspace_id = workspace_id.to_string();
        assert_eq!(
            events[0].details.get("workspace_id").map(String::as_str),
            Some(expected_workspace_id.as_str())
        );
        assert_eq!(
            events[0].details.get("failed_step").map(String::as_str),
            Some("workspace_head_update")
        );
        assert_eq!(
            events[0].details.get("status").map(String::as_str),
            Some(StatusCode::INTERNAL_SERVER_ERROR.as_str())
        );
        assert!(
            events[0]
                .details
                .get("error")
                .is_some_and(|error| error.contains("metadata write failed"))
        );
    }

    #[tokio::test]
    async fn unknown_workspace_header_is_rejected_before_commit() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();

        let response = vcs_commit(
            State(Arc::new(ServerState {
                core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
                db: Arc::new(db.clone()),
                workspaces: Arc::new(FailingHeadStore),
                idempotency: Arc::new(InMemoryIdempotencyStore::new()),
                audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
                review: Arc::new(crate::review::InMemoryReviewStore::new()),
            })),
            workspace_headers("root", Uuid::new_v4()),
            Json(CommitRequest {
                message: "blocked".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(db.vcs_log().await.len(), 0);
    }

    #[tokio::test]
    async fn known_workspace_header_updates_head_after_commit() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();
        let workspace_id = Uuid::new_v4();
        let store = Arc::new(RecordingWorkspaceStore {
            workspace_id,
            updated: RwLock::new(None),
        });
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: store.clone(),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let response = vcs_commit(
            State(state),
            workspace_headers("root", workspace_id),
            Json(CommitRequest {
                message: "with workspace".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(store.updated.read().await.is_some());
    }

    #[tokio::test]
    async fn root_can_read_diff_plain_text() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md before", &mut root)
            .await
            .unwrap();
        db.commit("init", "root").await.unwrap();
        db.execute_command("write a.md after", &mut root)
            .await
            .unwrap();

        let response = vcs_diff(
            State(test_state(db)),
            user_headers("root"),
            Query(DiffQuery {
                path: Some("/a.md".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("diff -- /a.md"));
        assert!(body.contains("-before"));
        assert!(body.contains("+after"));
    }
}
