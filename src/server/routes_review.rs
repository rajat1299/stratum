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
use crate::review::{
    ApprovalPolicyDecision, ApprovalRecord, ChangeRequest, ChangeRequestStatus,
    DismissApprovalInput, NewApprovalRecord, NewChangeRequest, NewReviewComment, ReviewComment,
    ReviewCommentKind,
};
use crate::vcs::RefName;

const CREATE_PROTECTED_REF_ROUTE: &str = "POST /protected/refs";
const CREATE_PROTECTED_PATH_ROUTE: &str = "POST /protected/paths";
const CREATE_CHANGE_REQUEST_ROUTE: &str = "POST /change-requests";
const CREATE_CHANGE_REQUEST_APPROVAL_ROUTE: &str = "POST /change-requests/{id}/approvals";
const CREATE_CHANGE_REQUEST_COMMENT_ROUTE: &str = "POST /change-requests/{id}/comments";
const DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE: &str =
    "POST /change-requests/{id}/approvals/{approval_id}/dismiss";
const REJECT_CHANGE_REQUEST_ROUTE: &str = "POST /change-requests/{id}/reject";
const MERGE_CHANGE_REQUEST_ROUTE: &str = "POST /change-requests/{id}/merge";

static REVIEW_TRANSITION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Deserialize)]
struct CreateProtectedRefRequest {
    ref_name: String,
    required_approvals: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateProtectedPathRequest {
    path_prefix: String,
    target_ref: Option<String>,
    required_approvals: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateChangeRequestRequest {
    title: String,
    description: Option<String>,
    source_ref: String,
    target_ref: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateApprovalRequest {
    comment: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateReviewCommentRequest {
    body: String,
    path: Option<String>,
    kind: Option<ReviewCommentKind>,
}

#[derive(Debug, Clone, Deserialize)]
struct DismissApprovalRequest {
    reason: Option<String>,
}

#[derive(Serialize)]
struct ReviewActorFingerprint<'a> {
    uid: Uid,
    username: &'a str,
    effective_uid: Uid,
    delegate: Option<ReviewDelegateFingerprint<'a>>,
}

#[derive(Serialize)]
struct ReviewDelegateFingerprint<'a> {
    uid: Uid,
    username: &'a str,
}

enum ReviewIdempotency {
    Execute(Option<IdempotencyReservation>),
    Respond(axum::response::Response),
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/protected/refs",
            get(list_protected_refs).post(create_protected_ref),
        )
        .route(
            "/protected/paths",
            get(list_protected_paths).post(create_protected_path),
        )
        .route(
            "/change-requests",
            get(list_change_requests).post(create_change_request),
        )
        .route("/change-requests/{id}", get(get_change_request))
        .route(
            "/change-requests/{id}/approvals",
            get(list_change_request_approvals).post(create_change_request_approval),
        )
        .route(
            "/change-requests/{id}/comments",
            get(list_change_request_comments).post(create_change_request_comment),
        )
        .route(
            "/change-requests/{id}/approvals/{approval_id}/dismiss",
            post(dismiss_change_request_approval),
        )
        .route("/change-requests/{id}/reject", post(reject_change_request))
        .route("/change-requests/{id}/merge", post(merge_change_request))
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
        VfsError::NotFound { .. } | VfsError::ObjectNotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::AlreadyExists { .. } => StatusCode::CONFLICT,
        VfsError::InvalidArgs { message }
            if message.starts_with("ref compare-and-swap mismatch")
                || message.starts_with("invalid change request transition") =>
        {
            StatusCode::CONFLICT
        }
        VfsError::InvalidArgs { .. } | VfsError::InvalidPath { .. } => StatusCode::BAD_REQUEST,
        VfsError::IoError(_) | VfsError::CorruptStore { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => fallback,
    }
}

fn require_admin_session(session: &Session) -> Result<(), VfsError> {
    if session.scope.is_some() {
        return Err(VfsError::PermissionDenied {
            path: "review".to_string(),
        });
    }

    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "review".to_string(),
        });
    }

    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "review".to_string(),
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

fn actor_fingerprint(session: &Session) -> ReviewActorFingerprint<'_> {
    ReviewActorFingerprint {
        uid: session.uid,
        username: &session.username,
        effective_uid: session.effective_uid(),
        delegate: session
            .delegate
            .as_ref()
            .map(|delegate| ReviewDelegateFingerprint {
                uid: delegate.uid,
                username: &delegate.username,
            }),
    }
}

fn ref_json(vcs_ref: crate::db::DbVcsRef) -> serde_json::Value {
    serde_json::json!({
        "name": vcs_ref.name,
        "target": vcs_ref.target,
        "version": vcs_ref.version,
    })
}

async fn approval_decision(
    state: &AppState,
    change: &ChangeRequest,
) -> Result<ApprovalPolicyDecision, VfsError> {
    let changed_paths = state
        .db
        .changed_paths_between(&change.base_commit, &change.head_commit)
        .await?;
    state
        .review
        .approval_decision(change.id, &changed_paths)
        .await?
        .ok_or_else(|| VfsError::NotFound {
            path: format!("change request {}", change.id),
        })
}

fn approval_state_value(decision: &ApprovalPolicyDecision) -> serde_json::Value {
    serde_json::to_value(decision).expect("approval policy decision serializes")
}

async fn approval_state_json(state: &AppState, change: &ChangeRequest) -> serde_json::Value {
    match approval_decision(state, change).await {
        Ok(decision) => approval_state_value(&decision),
        Err(e) => serde_json::json!({
            "available": false,
            "error": e.to_string(),
        }),
    }
}

async fn change_json(state: &AppState, change: &ChangeRequest) -> serde_json::Value {
    serde_json::json!({
        "change_request": change,
        "approval_state": approval_state_json(state, change).await,
    })
}

async fn approval_list_json(
    state: &AppState,
    change: &ChangeRequest,
    approvals: Vec<ApprovalRecord>,
) -> serde_json::Value {
    serde_json::json!({
        "approvals": approvals,
        "approval_state": approval_state_json(state, change).await,
    })
}

async fn approval_mutation_json(
    state: &AppState,
    change: &ChangeRequest,
    approval: ApprovalRecord,
    created: bool,
) -> serde_json::Value {
    serde_json::json!({
        "approval": approval,
        "created": created,
        "approval_state": approval_state_json(state, change).await,
    })
}

async fn comment_list_json(
    state: &AppState,
    change: &ChangeRequest,
    comments: Vec<ReviewComment>,
) -> serde_json::Value {
    serde_json::json!({
        "comments": comments,
        "approval_state": approval_state_json(state, change).await,
    })
}

async fn comment_mutation_json(
    state: &AppState,
    change: &ChangeRequest,
    comment: ReviewComment,
    created: bool,
) -> serde_json::Value {
    serde_json::json!({
        "comment": comment,
        "created": created,
        "approval_state": approval_state_json(state, change).await,
    })
}

async fn approval_dismissal_json(
    state: &AppState,
    change: &ChangeRequest,
    approval: ApprovalRecord,
    dismissed: bool,
) -> serde_json::Value {
    serde_json::json!({
        "approval": approval,
        "dismissed": dismissed,
        "approval_state": approval_state_json(state, change).await,
    })
}

fn mutation_committed_failure_body(
    message: impl Into<String>,
    extra_key: &str,
) -> serde_json::Value {
    serde_json::json!({
        "error": message.into(),
        "mutation_committed": true,
        extra_key: false,
    })
}

fn audit_append_failed_body(error: VfsError) -> (StatusCode, serde_json::Value) {
    (
        error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
        serde_json::json!({
            "error": format!("audit append failed after mutation: {error}"),
            "mutation_committed": true,
            "audit_recorded": false,
        }),
    )
}

async fn begin_review_idempotency(
    state: &AppState,
    headers: &HeaderMap,
    scope: &str,
    fingerprint_body: serde_json::Value,
) -> ReviewIdempotency {
    let key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(key) => key,
        Err(e) => {
            return ReviewIdempotency::Respond(
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response(),
            );
        }
    };
    let Some(key) = key else {
        return ReviewIdempotency::Execute(None);
    };

    let fingerprint = match request_fingerprint(scope, &fingerprint_body) {
        Ok(fingerprint) => fingerprint,
        Err(e) => {
            return ReviewIdempotency::Respond(
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response(),
            );
        }
    };

    match state.idempotency.begin(scope, &key, &fingerprint).await {
        Ok(IdempotencyBegin::Execute(reservation)) => ReviewIdempotency::Execute(Some(reservation)),
        Ok(IdempotencyBegin::Replay(record)) => {
            ReviewIdempotency::Respond(http_idempotency::idempotency_json_replay_response(record))
        }
        Ok(IdempotencyBegin::Conflict) => {
            ReviewIdempotency::Respond(http_idempotency::idempotency_conflict_response())
        }
        Ok(IdempotencyBegin::InProgress) => {
            ReviewIdempotency::Respond(http_idempotency::idempotency_in_progress_response())
        }
        Err(e) => ReviewIdempotency::Respond(
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response(),
        ),
    }
}

async fn complete_review_idempotency(
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

async fn abort_review_idempotency(state: &AppState, reservation: Option<&IdempotencyReservation>) {
    if let Some(reservation) = reservation {
        state.idempotency.abort(reservation).await;
    }
}

fn not_found_body(kind: &str, id: impl std::fmt::Display) -> serde_json::Value {
    serde_json::json!({"error": format!("unknown {kind}: {id}")})
}

async fn get_change_or_404(
    state: &AppState,
    id: Uuid,
) -> Result<ChangeRequest, axum::response::Response> {
    match state.review.get_change_request(id).await {
        Ok(Some(change)) => Ok(change),
        Ok(None) => Err(json_response(
            StatusCode::NOT_FOUND,
            not_found_body("change request", id),
        )),
        Err(e) => Err(err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response()),
    }
}

async fn list_protected_refs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.review.list_protected_ref_rules().await {
        Ok(rules) => Json(serde_json::json!({ "rules": rules })).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn create_protected_ref(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateProtectedRefRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let ref_name = match RefName::new(&req.ref_name) {
        Ok(ref_name) => ref_name.into_string(),
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        CREATE_PROTECTED_REF_ROUTE,
        serde_json::json!({
            "route": CREATE_PROTECTED_REF_ROUTE,
            "actor": actor_fingerprint(&session),
            "ref_name": &ref_name,
            "required_approvals": req.required_approvals,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    match state
        .review
        .create_protected_ref_rule(&ref_name, req.required_approvals, session.effective_uid())
        .await
    {
        Ok(rule) => {
            let body = serde_json::to_value(&rule).expect("protected ref rule serializes");
            let event = NewAuditEvent::from_session(
                &session,
                AuditAction::ProtectedRefRuleCreate,
                AuditResource::id(AuditResourceKind::ProtectedRefRule, rule.id.to_string()),
            )
            .with_detail("rule_id", rule.id)
            .with_detail("ref_name", &rule.ref_name)
            .with_detail("required_approvals", rule.required_approvals)
            .with_detail("active", rule.active);
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) = complete_review_idempotency(
                &state,
                reservation.as_ref(),
                StatusCode::CREATED,
                &body,
            )
            .await
            {
                return response;
            }
            json_response(StatusCode::CREATED, body)
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn list_protected_paths(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.review.list_protected_path_rules().await {
        Ok(rules) => Json(serde_json::json!({ "rules": rules })).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn create_protected_path(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateProtectedPathRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let path_prefix = match crate::review::normalize_path_prefix(&req.path_prefix) {
        Ok(path_prefix) => path_prefix,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let target_ref = match req
        .target_ref
        .as_ref()
        .map(|target_ref| RefName::new(target_ref).map(RefName::into_string))
        .transpose()
    {
        Ok(target_ref) => target_ref,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        CREATE_PROTECTED_PATH_ROUTE,
        serde_json::json!({
            "route": CREATE_PROTECTED_PATH_ROUTE,
            "actor": actor_fingerprint(&session),
            "path_prefix": &path_prefix,
            "target_ref": target_ref.as_deref(),
            "required_approvals": req.required_approvals,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    match state
        .review
        .create_protected_path_rule(
            &path_prefix,
            target_ref.as_deref(),
            req.required_approvals,
            session.effective_uid(),
        )
        .await
    {
        Ok(rule) => {
            let body = serde_json::to_value(&rule).expect("protected path rule serializes");
            let mut event = NewAuditEvent::from_session(
                &session,
                AuditAction::ProtectedPathRuleCreate,
                AuditResource::id(AuditResourceKind::ProtectedPathRule, rule.id.to_string())
                    .with_path(&rule.path_prefix),
            )
            .with_detail("rule_id", rule.id)
            .with_detail("path_prefix", &rule.path_prefix)
            .with_detail("required_approvals", rule.required_approvals)
            .with_detail("active", rule.active);
            if let Some(target_ref) = &rule.target_ref {
                event = event.with_detail("target_ref", target_ref);
            }
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) = complete_review_idempotency(
                &state,
                reservation.as_ref(),
                StatusCode::CREATED,
                &body,
            )
            .await
            {
                return response;
            }
            json_response(StatusCode::CREATED, body)
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn list_change_requests(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.review.list_change_requests().await {
        Ok(change_requests) => {
            let mut items = Vec::with_capacity(change_requests.len());
            for change in &change_requests {
                items.push(change_json(&state, change).await);
            }
            Json(serde_json::json!({ "change_requests": items })).into_response()
        }
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn create_change_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateChangeRequestRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let source_ref = match RefName::new(&req.source_ref) {
        Ok(source_ref) => source_ref.into_string(),
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let target_ref = match RefName::new(&req.target_ref) {
        Ok(target_ref) => target_ref.into_string(),
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let normalized_title = req.title.trim().to_string();

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        CREATE_CHANGE_REQUEST_ROUTE,
        serde_json::json!({
            "route": CREATE_CHANGE_REQUEST_ROUTE,
            "actor": actor_fingerprint(&session),
            "title": &normalized_title,
            "description": &req.description,
            "source_ref": &source_ref,
            "target_ref": &target_ref,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let source = match state.db.get_ref(&source_ref).await {
        Ok(Some(vcs_ref)) => vcs_ref,
        Ok(None) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return json_response(StatusCode::NOT_FOUND, not_found_body("ref", &source_ref));
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let target = match state.db.get_ref(&target_ref).await {
        Ok(Some(vcs_ref)) => vcs_ref,
        Ok(None) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return json_response(StatusCode::NOT_FOUND, not_found_body("ref", &target_ref));
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match state
        .review
        .create_change_request(NewChangeRequest {
            title: normalized_title,
            description: req.description,
            source_ref: source_ref.clone(),
            target_ref: target_ref.clone(),
            base_commit: target.target.clone(),
            head_commit: source.target.clone(),
            created_by: session.effective_uid(),
        })
        .await
    {
        Ok(change) => {
            let body = change_json(&state, &change).await;
            let event = NewAuditEvent::from_session(
                &session,
                AuditAction::ChangeRequestCreate,
                AuditResource::id(AuditResourceKind::ChangeRequest, change.id.to_string()),
            )
            .with_detail("change_request_id", change.id)
            .with_detail("source_ref", &change.source_ref)
            .with_detail("target_ref", &change.target_ref)
            .with_detail("base_commit", &change.base_commit)
            .with_detail("head_commit", &change.head_commit)
            .with_detail("version", change.version);
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) = complete_review_idempotency(
                &state,
                reservation.as_ref(),
                StatusCode::CREATED,
                &body,
            )
            .await
            {
                return response;
            }
            json_response(StatusCode::CREATED, body)
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn get_change_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match get_change_or_404(&state, id).await {
        Ok(change) => Json(change_json(&state, &change).await).into_response(),
        Err(response) => response,
    }
}

async fn list_change_request_approvals(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    match state.review.list_approvals(id).await {
        Ok(approvals) => Json(approval_list_json(&state, &change, approvals).await).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn create_change_request_approval(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<CreateApprovalRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        CREATE_CHANGE_REQUEST_APPROVAL_ROUTE,
        serde_json::json!({
            "route": CREATE_CHANGE_REQUEST_APPROVAL_ROUTE,
            "actor": actor_fingerprint(&session),
            "change_request_id": id,
            "head_commit": &change.head_commit,
            "comment": &req.comment,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    match state
        .review
        .create_approval(NewApprovalRecord {
            change_request_id: id,
            head_commit: change.head_commit.clone(),
            approved_by: session.effective_uid(),
            comment: req.comment,
        })
        .await
    {
        Ok(mutation) => {
            let body =
                approval_mutation_json(&state, &change, mutation.record.clone(), mutation.created)
                    .await;
            let event = NewAuditEvent::from_session(
                &session,
                AuditAction::ChangeRequestApprove,
                AuditResource::id(
                    AuditResourceKind::ApprovalRecord,
                    mutation.record.id.to_string(),
                ),
            )
            .with_detail("approval_id", mutation.record.id)
            .with_detail("change_request_id", change.id)
            .with_detail("source_ref", &change.source_ref)
            .with_detail("target_ref", &change.target_ref)
            .with_detail("head_commit", &change.head_commit)
            .with_detail("approved_by", mutation.record.approved_by)
            .with_detail("created", mutation.created);
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            let status = if mutation.created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            if let Err(response) =
                complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
            {
                return response;
            }
            json_response(status, body)
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn list_change_request_comments(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    match state.review.list_comments(id).await {
        Ok(comments) => Json(comment_list_json(&state, &change, comments).await).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn create_change_request_comment(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<CreateReviewCommentRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => return response,
    };
    let kind = req.kind.unwrap_or(ReviewCommentKind::General);

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
        serde_json::json!({
            "route": CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
            "actor": actor_fingerprint(&session),
            "change_request_id": id,
            "body": &req.body,
            "path": &req.path,
            "kind": kind,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    match state
        .review
        .create_comment(NewReviewComment {
            change_request_id: id,
            author: session.effective_uid(),
            body: req.body,
            path: req.path,
            kind,
        })
        .await
    {
        Ok(mutation) => {
            let body =
                comment_mutation_json(&state, &change, mutation.comment.clone(), mutation.created)
                    .await;
            let resource = match &mutation.comment.path {
                Some(path) => AuditResource::id(
                    AuditResourceKind::ReviewComment,
                    mutation.comment.id.to_string(),
                )
                .with_path(path),
                None => AuditResource::id(
                    AuditResourceKind::ReviewComment,
                    mutation.comment.id.to_string(),
                ),
            };
            let kind = match mutation.comment.kind {
                ReviewCommentKind::General => "general",
                ReviewCommentKind::ChangesRequested => "changes_requested",
            };
            let event = NewAuditEvent::from_session(
                &session,
                AuditAction::ChangeRequestCommentCreate,
                resource,
            )
            .with_detail("comment_id", mutation.comment.id)
            .with_detail("change_request_id", change.id)
            .with_detail("source_ref", &change.source_ref)
            .with_detail("target_ref", &change.target_ref)
            .with_detail("kind", kind)
            .with_detail("author", mutation.comment.author)
            .with_detail("active", mutation.comment.active)
            .with_detail("version", mutation.comment.version);
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) = complete_review_idempotency(
                &state,
                reservation.as_ref(),
                StatusCode::CREATED,
                &body,
            )
            .await
            {
                return response;
            }
            json_response(StatusCode::CREATED, body)
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn dismiss_change_request_approval(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, approval_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<DismissApprovalRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
        serde_json::json!({
            "route": DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
            "actor": actor_fingerprint(&session),
            "change_request_id": id,
            "approval_id": approval_id,
            "reason": &req.reason,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let _transition_guard = REVIEW_TRANSITION_LOCK.lock().await;

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
    };

    match state
        .review
        .dismiss_approval(DismissApprovalInput {
            change_request_id: id,
            approval_id,
            dismissed_by: session.effective_uid(),
            reason: req.reason,
        })
        .await
    {
        Ok(mutation) => {
            let body = approval_dismissal_json(
                &state,
                &change,
                mutation.record.clone(),
                mutation.dismissed,
            )
            .await;
            let event = NewAuditEvent::from_session(
                &session,
                AuditAction::ChangeRequestApprovalDismiss,
                AuditResource::id(
                    AuditResourceKind::ApprovalRecord,
                    mutation.record.id.to_string(),
                ),
            )
            .with_detail("approval_id", mutation.record.id)
            .with_detail("change_request_id", change.id)
            .with_detail("source_ref", &change.source_ref)
            .with_detail("target_ref", &change.target_ref)
            .with_detail("head_commit", &change.head_commit)
            .with_detail("dismissed_by", session.effective_uid())
            .with_detail("dismissed", mutation.dismissed)
            .with_detail("version", mutation.record.version);
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_review_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body)
                    .await
            {
                return response;
            }
            json_response(StatusCode::OK, body)
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn reject_change_request(
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

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        REJECT_CHANGE_REQUEST_ROUTE,
        serde_json::json!({
            "route": REJECT_CHANGE_REQUEST_ROUTE,
            "actor": actor_fingerprint(&session),
            "change_request_id": id,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let _transition_guard = REVIEW_TRANSITION_LOCK.lock().await;

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
    };
    if change.status != ChangeRequestStatus::Open {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({"error": format!("change request {id} is not open")}),
        );
    }

    match state
        .review
        .transition_change_request(id, ChangeRequestStatus::Rejected)
        .await
    {
        Ok(Some(change)) => {
            let body = change_json(&state, &change).await;
            let event = NewAuditEvent::from_session(
                &session,
                AuditAction::ChangeRequestReject,
                AuditResource::id(AuditResourceKind::ChangeRequest, change.id.to_string()),
            )
            .with_detail("change_request_id", change.id)
            .with_detail("source_ref", &change.source_ref)
            .with_detail("target_ref", &change.target_ref)
            .with_detail("base_commit", &change.base_commit)
            .with_detail("head_commit", &change.head_commit)
            .with_detail("status", "rejected")
            .with_detail("version", change.version);
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_body(e);
                if let Err(response) =
                    complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_review_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body)
                    .await
            {
                return response;
            }
            json_response(StatusCode::OK, body)
        }
        Ok(None) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            json_response(StatusCode::NOT_FOUND, not_found_body("change request", id))
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            err_json(error_status(&e, StatusCode::CONFLICT), e.to_string()).into_response()
        }
    }
}

async fn merge_change_request(
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

    let reservation = match begin_review_idempotency(
        &state,
        &headers,
        MERGE_CHANGE_REQUEST_ROUTE,
        serde_json::json!({
            "route": MERGE_CHANGE_REQUEST_ROUTE,
            "actor": actor_fingerprint(&session),
            "change_request_id": id,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let _transition_guard = REVIEW_TRANSITION_LOCK.lock().await;

    let change = match get_change_or_404(&state, id).await {
        Ok(change) => change,
        Err(response) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
    };
    if change.status != ChangeRequestStatus::Open {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({"error": format!("change request {id} is not open")}),
        );
    }

    let source = match state.db.get_ref(&change.source_ref).await {
        Ok(Some(vcs_ref)) => vcs_ref,
        Ok(None) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return json_response(
                StatusCode::NOT_FOUND,
                not_found_body("ref", &change.source_ref),
            );
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let target = match state.db.get_ref(&change.target_ref).await {
        Ok(Some(vcs_ref)) => vcs_ref,
        Ok(None) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return json_response(
                StatusCode::NOT_FOUND,
                not_found_body("ref", &change.target_ref),
            );
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    if source.target != change.head_commit {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": format!("change request {id} source ref is stale")
            }),
        );
    }
    if target.target != change.base_commit {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": format!("change request {id} target ref is stale")
            }),
        );
    }

    let approval_state = match approval_decision(&state, &change).await {
        Ok(decision) => decision,
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(StatusCode::CONFLICT, e.to_string()).into_response();
        }
    };
    if !approval_state.approved {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::FORBIDDEN,
            serde_json::json!({
                "error": format!(
                    "change request {id} requires {} approval(s)",
                    approval_state.required_approvals
                ),
                "approval_state": approval_state,
            }),
        );
    }

    let updated_ref = match state
        .db
        .update_ref(
            &change.target_ref,
            &target.target,
            target.version,
            &change.head_commit,
        )
        .await
    {
        Ok(vcs_ref) => vcs_ref,
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(error_status(&e, StatusCode::CONFLICT), e.to_string()).into_response();
        }
    };

    let merged = match state
        .review
        .transition_change_request(id, ChangeRequestStatus::Merged)
        .await
    {
        Ok(Some(change)) => change,
        Ok(None) => {
            let status = StatusCode::INTERNAL_SERVER_ERROR;
            let body = mutation_committed_failure_body(
                format!("change request {id} disappeared after target ref update"),
                "change_request_recorded",
            );
            if let Err(response) =
                complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
            {
                return response;
            }
            return json_response(status, body);
        }
        Err(e) => {
            let status = error_status(&e, StatusCode::INTERNAL_SERVER_ERROR);
            let body = mutation_committed_failure_body(
                format!("change request update failed after target ref update: {e}"),
                "change_request_recorded",
            );
            if let Err(response) =
                complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
            {
                return response;
            }
            return json_response(status, body);
        }
    };

    let body = serde_json::json!({
        "change_request": merged,
        "approval_state": approval_state_value(&approval_state),
        "target_ref": ref_json(updated_ref.clone()),
    });
    let event = NewAuditEvent::from_session(
        &session,
        AuditAction::ChangeRequestMerge,
        AuditResource::id(AuditResourceKind::ChangeRequest, merged.id.to_string()),
    )
    .with_detail("change_request_id", merged.id)
    .with_detail("source_ref", &merged.source_ref)
    .with_detail("target_ref", &merged.target_ref)
    .with_detail("base_commit", &merged.base_commit)
    .with_detail("head_commit", &merged.head_commit)
    .with_detail("status", "merged")
    .with_detail("change_request_version", merged.version)
    .with_detail("target_ref_version", updated_ref.version);
    if let Err(e) = state.audit.append(event).await {
        let (status, body) = audit_append_failed_body(e);
        if let Err(response) =
            complete_review_idempotency(&state, reservation.as_ref(), status, &body).await
        {
            return response;
        }
        return json_response(status, body);
    }
    if let Err(response) =
        complete_review_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body).await
    {
        return response;
    }
    json_response(StatusCode::OK, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditAction, AuditResourceKind};
    use crate::auth::ROOT_UID;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::review::{ChangeRequestStatus, InMemoryReviewStore, NewChangeRequest};
    use crate::server::ServerState;
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use axum::extract::Path as AxumPath;
    use std::sync::Arc;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        })
    }

    fn test_state_with_workspaces(
        db: StratumDb,
        workspaces: Arc<InMemoryWorkspaceMetadataStore>,
    ) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
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

    async fn review_fixture() -> (AppState, String, String, Uuid) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let head = commit_file(&db, &mut root, "/legal.txt", "head", "head").await;
        db.create_ref("review/cr-1", &head).await.unwrap();
        let main = db.get_ref("main").await.unwrap().unwrap();
        db.update_ref("main", &main.target, main.version, &base)
            .await
            .unwrap();
        let state = test_state(db);
        let change = state
            .review
            .create_change_request(NewChangeRequest {
                title: "Legal update".to_string(),
                description: Some("metadata only".to_string()),
                source_ref: "review/cr-1".to_string(),
                target_ref: "main".to_string(),
                base_commit: base.clone(),
                head_commit: head.clone(),
                created_by: ROOT_UID,
            })
            .await
            .unwrap();
        (state, base, head, change.id)
    }

    async fn add_admin_user(state: &AppState, username: &str) {
        let mut root = Session::root();
        state
            .db
            .execute_command(&format!("adduser {username}"), &mut root)
            .await
            .unwrap();
        state
            .db
            .execute_command(&format!("usermod -aG wheel {username}"), &mut root)
            .await
            .unwrap();
    }

    async fn approve_change_request_for(
        state: &AppState,
        change_request_id: Uuid,
        username: &str,
    ) -> serde_json::Value {
        let response = create_change_request_approval(
            State(state.clone()),
            user_headers(username),
            AxumPath(change_request_id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        response_json(response).await
    }

    #[tokio::test]
    async fn admin_can_create_and_list_protected_rules() {
        let state = test_state(StratumDb::open_memory());

        let created_ref = create_protected_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(created_ref.status(), StatusCode::CREATED);
        let created_ref = response_json(created_ref).await;
        assert_eq!(created_ref["ref_name"], "main");
        assert_eq!(created_ref["required_approvals"], 1);
        assert_eq!(created_ref["created_by"], ROOT_UID);
        assert_eq!(created_ref["active"], true);
        let ref_rule_id = created_ref["id"].as_str().expect("ref rule id");

        let listed_refs = list_protected_refs(State(state.clone()), user_headers("root"))
            .await
            .into_response();
        assert_eq!(listed_refs.status(), StatusCode::OK);
        let listed_refs = response_json(listed_refs).await;
        assert!(
            listed_refs["rules"]
                .as_array()
                .unwrap()
                .iter()
                .any(|rule| rule["id"] == ref_rule_id)
        );

        let created_path = create_protected_path(
            State(state.clone()),
            user_headers("root"),
            Json(CreateProtectedPathRequest {
                path_prefix: "/legal".to_string(),
                target_ref: Some("main".to_string()),
                required_approvals: 2,
            }),
        )
        .await
        .into_response();
        assert_eq!(created_path.status(), StatusCode::CREATED);
        let created_path = response_json(created_path).await;
        assert_eq!(created_path["path_prefix"], "/legal");
        assert_eq!(created_path["target_ref"], "main");
        assert_eq!(created_path["required_approvals"], 2);
        let path_rule_id = created_path["id"].as_str().expect("path rule id");

        let listed_paths = list_protected_paths(State(state.clone()), user_headers("root"))
            .await
            .into_response();
        assert_eq!(listed_paths.status(), StatusCode::OK);
        let listed_paths = response_json(listed_paths).await;
        assert!(
            listed_paths["rules"]
                .as_array()
                .unwrap()
                .iter()
                .any(|rule| rule["id"] == path_rule_id)
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, AuditAction::ProtectedRefRuleCreate);
        assert_eq!(events[0].resource.kind, AuditResourceKind::ProtectedRefRule);
        assert_eq!(events[1].action, AuditAction::ProtectedPathRuleCreate);
        assert_eq!(
            events[1].resource.kind,
            AuditResourceKind::ProtectedPathRule
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("metadata only"));
    }

    #[tokio::test]
    async fn non_admin_and_workspace_bearer_cannot_use_review_admin_routes() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let state = test_state(db.clone());

        let non_admin_list = list_protected_refs(State(state.clone()), user_headers("bob"))
            .await
            .into_response();
        assert_eq!(non_admin_list.status(), StatusCode::FORBIDDEN);

        let non_admin_create = create_protected_ref(
            State(state.clone()),
            user_headers("bob"),
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(non_admin_create.status(), StatusCode::FORBIDDEN);

        let workspaces = Arc::new(InMemoryWorkspaceMetadataStore::new());
        let workspace = workspaces.create_workspace("demo", "/demo").await.unwrap();
        let issued = workspaces
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let scoped_state = test_state_with_workspaces(db, workspaces);
        let scoped = list_protected_refs(
            State(scoped_state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();
        assert_eq!(scoped.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn create_change_request_captures_base_and_head_refs() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let head = commit_file(&db, &mut root, "/legal.txt", "head", "head").await;
        db.create_ref("review/cr-1", &head).await.unwrap();
        let main = db.get_ref("main").await.unwrap().unwrap();
        db.update_ref("main", &main.target, main.version, &base)
            .await
            .unwrap();
        let state = test_state(db);

        let response = create_change_request(
            State(state.clone()),
            user_headers("root"),
            Json(CreateChangeRequestRequest {
                title: "Legal update".to_string(),
                description: Some("body must stay out of audit".to_string()),
                source_ref: "review/cr-1".to_string(),
                target_ref: "main".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response_json(response).await;
        assert_eq!(body["change_request"]["title"], "Legal update");
        assert_eq!(
            body["change_request"]["description"],
            "body must stay out of audit"
        );
        assert_eq!(body["change_request"]["source_ref"], "review/cr-1");
        assert_eq!(body["change_request"]["target_ref"], "main");
        assert_eq!(body["change_request"]["base_commit"], base);
        assert_eq!(body["change_request"]["head_commit"], head);
        assert_eq!(body["change_request"]["status"], "open");
        assert_eq!(body["change_request"]["created_by"], ROOT_UID);
        assert_eq!(body["change_request"]["version"], 1);
        assert_eq!(body["approval_state"]["required_approvals"], 0);
        assert_eq!(body["approval_state"]["approved"], true);

        let missing_ref = create_change_request(
            State(state.clone()),
            user_headers("root"),
            Json(CreateChangeRequestRequest {
                title: "Missing".to_string(),
                description: None,
                source_ref: "review/missing".to_string(),
                target_ref: "main".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(missing_ref.status(), StatusCode::NOT_FOUND);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestCreate);
        assert_eq!(events[0].resource.kind, AuditResourceKind::ChangeRequest);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("body must stay out of audit"));
    }

    #[tokio::test]
    async fn reject_change_request_only_allows_open_requests() {
        let (state, _base, _head, id) = review_fixture().await;

        let rejected =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);
        let body = response_json(rejected).await;
        assert_eq!(body["change_request"]["status"], "rejected");
        assert_eq!(body["change_request"]["version"], 2);
        assert_eq!(body["approval_state"]["approved"], true);

        let rejected_again =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected_again.status(), StatusCode::CONFLICT);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestReject);
    }

    #[tokio::test]
    async fn merge_change_request_fast_forwards_target_ref() {
        let (state, _base, head, id) = review_fixture().await;

        let merged = merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        let body = response_json(merged).await;
        assert_eq!(body["change_request"]["status"], "merged");
        assert_eq!(body["change_request"]["version"], 2);
        assert_eq!(body["target_ref"]["name"], "main");
        assert_eq!(body["target_ref"]["target"], head);

        let main = state.db.get_ref("main").await.unwrap().unwrap();
        assert_eq!(main.target, head);
        let change = state.review.get_change_request(id).await.unwrap().unwrap();
        assert_eq!(change.status, ChangeRequestStatus::Merged);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestMerge);
    }

    #[tokio::test]
    async fn merge_change_request_conflicts_when_source_or_target_is_stale() {
        let (source_state, _base, _head, source_stale_id) = review_fixture().await;
        let source_ref = source_state
            .db
            .get_ref("review/cr-1")
            .await
            .unwrap()
            .unwrap();
        source_state
            .db
            .update_ref(
                "review/cr-1",
                &source_ref.target,
                source_ref.version,
                source_state
                    .review
                    .get_change_request(source_stale_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .base_commit
                    .as_str(),
            )
            .await
            .unwrap();

        let source_stale = merge_change_request(
            State(source_state.clone()),
            user_headers("root"),
            AxumPath(source_stale_id),
        )
        .await
        .into_response();
        assert_eq!(source_stale.status(), StatusCode::CONFLICT);

        let (target_state, _base, head, target_stale_id) = review_fixture().await;
        let main = target_state.db.get_ref("main").await.unwrap().unwrap();
        target_state
            .db
            .update_ref("main", &main.target, main.version, &head)
            .await
            .unwrap();

        let target_stale = merge_change_request(
            State(target_state.clone()),
            user_headers("root"),
            AxumPath(target_stale_id),
        )
        .await
        .into_response();
        assert_eq!(target_stale.status(), StatusCode::CONFLICT);
        let change = target_state
            .review
            .get_change_request(target_stale_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(change.status, ChangeRequestStatus::Open);
    }

    #[tokio::test]
    async fn protected_ref_create_idempotency_replays_and_conflicts() {
        let state = test_state(StratumDb::open_memory());
        let headers = user_headers_with_idempotency("root", "protected-ref-create");
        let request = || CreateProtectedRefRequest {
            ref_name: "main".to_string(),
            required_approvals: 1,
        };

        let first = create_protected_ref(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(first.headers().get("x-stratum-idempotent-replay").is_none());
        let first_body = response_json(first).await;

        let replay = create_protected_ref(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(response_json(replay).await, first_body);

        let conflict = create_protected_ref(
            State(state.clone()),
            headers,
            Json(CreateProtectedRefRequest {
                ref_name: "review/cr-1".to_string(),
                required_approvals: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(conflict.status(), StatusCode::CONFLICT);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ProtectedRefRuleCreate);
    }

    #[tokio::test]
    async fn approval_create_and_list_records_with_audit_redaction() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;

        let created = create_change_request_approval(
            State(state.clone()),
            user_headers("alice"),
            AxumPath(id),
            Json(CreateApprovalRequest {
                comment: Some("private approval note".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created_body = response_json(created).await;
        assert_eq!(created_body["created"], true);
        assert_eq!(
            created_body["approval"]["change_request_id"],
            id.to_string()
        );
        assert_eq!(created_body["approval"]["approved_by"], 1);
        assert_eq!(created_body["approval"]["comment"], "private approval note");
        assert_eq!(created_body["approval_state"]["approval_count"], 1);
        assert_eq!(
            created_body["approval_state"]["approved_by"],
            serde_json::json!([1])
        );

        let listed =
            list_change_request_approvals(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed_body = response_json(listed).await;
        assert_eq!(listed_body["approvals"].as_array().unwrap().len(), 1);
        assert_eq!(listed_body["approval_state"]["approval_count"], 1);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestApprove);
        assert_eq!(events[0].resource.kind, AuditResourceKind::ApprovalRecord);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("private approval note"));
    }

    #[tokio::test]
    async fn approval_idempotency_replays_without_second_audit_event() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let headers = user_headers_with_idempotency("alice", "approve-replay");
        let request = || CreateApprovalRequest {
            comment: Some("approved".to_string()),
        };

        let first = create_change_request_approval(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let replay = create_change_request_approval(
            State(state.clone()),
            headers,
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(response_json(replay).await, first_body);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestApprove);
    }

    #[tokio::test]
    async fn review_feedback_comment_create_and_list_with_audit_redaction() {
        let (state, _base, _head, id) = review_fixture().await;

        let created = create_change_request_comment(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(CreateReviewCommentRequest {
                body: "  body must stay out of audit  ".to_string(),
                path: Some("/legal.txt".to_string()),
                kind: Some(crate::review::ReviewCommentKind::ChangesRequested),
            }),
        )
        .await
        .into_response();

        assert_eq!(created.status(), StatusCode::CREATED);
        let created_body = response_json(created).await;
        assert_eq!(created_body["created"], true);
        assert_eq!(created_body["comment"]["change_request_id"], id.to_string());
        assert_eq!(created_body["comment"]["author"], ROOT_UID);
        assert_eq!(
            created_body["comment"]["body"],
            "body must stay out of audit"
        );
        assert_eq!(created_body["comment"]["path"], "/legal.txt");
        assert_eq!(created_body["comment"]["kind"], "changes_requested");
        assert_eq!(created_body["approval_state"]["approved"], true);

        let listed =
            list_change_request_comments(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed_body = response_json(listed).await;
        assert_eq!(listed_body["comments"].as_array().unwrap().len(), 1);
        assert_eq!(listed_body["comments"][0], created_body["comment"]);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestCommentCreate);
        assert_eq!(events[0].resource.kind, AuditResourceKind::ReviewComment);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("body must stay out of audit"));
    }

    #[tokio::test]
    async fn review_feedback_comment_idempotency_replays_without_second_audit_event() {
        let (state, _base, _head, id) = review_fixture().await;
        let headers = user_headers_with_idempotency("root", "comment-replay");
        let request = || CreateReviewCommentRequest {
            body: "Please update the summary.".to_string(),
            path: None,
            kind: None,
        };

        let first = create_change_request_comment(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let replay = create_change_request_comment(
            State(state.clone()),
            headers,
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::CREATED);
        assert_eq!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(response_json(replay).await, first_body);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestCommentCreate);
    }

    #[tokio::test]
    async fn review_feedback_empty_comment_body_is_rejected_without_audit() {
        let (state, _base, _head, id) = review_fixture().await;

        let response = create_change_request_comment(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(CreateReviewCommentRequest {
                body: " \n\t ".to_string(),
                path: None,
                kind: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn review_feedback_dismiss_approval_recomputes_state_and_redacts_audit_reason() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_ref_rule("main", 1, ROOT_UID)
            .await
            .unwrap();
        let approval = approve_change_request_for(&state, id, "alice").await;
        let approval_id = Uuid::parse_str(approval["approval"]["id"].as_str().unwrap()).unwrap();

        let dismissed = dismiss_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("root", "dismiss-approval"),
            AxumPath((id, approval_id)),
            Json(DismissApprovalRequest {
                reason: Some("reason must stay out of audit".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(dismissed.status(), StatusCode::OK);
        let dismissed_body = response_json(dismissed).await;
        assert_eq!(dismissed_body["dismissed"], true);
        assert_eq!(dismissed_body["approval"]["active"], false);
        assert_eq!(dismissed_body["approval"]["dismissed_by"], ROOT_UID);
        assert_eq!(
            dismissed_body["approval"]["dismissal_reason"],
            "reason must stay out of audit"
        );
        assert_eq!(dismissed_body["approval_state"]["approval_count"], 0);
        assert_eq!(dismissed_body["approval_state"]["approved"], false);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].action, AuditAction::ChangeRequestApprovalDismiss);
        assert_eq!(events[1].resource.kind, AuditResourceKind::ApprovalRecord);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("reason must stay out of audit"));
    }

    #[tokio::test]
    async fn review_feedback_duplicate_dismissal_with_different_key_returns_noop() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let approval = approve_change_request_for(&state, id, "alice").await;
        let approval_id = Uuid::parse_str(approval["approval"]["id"].as_str().unwrap()).unwrap();

        let first = dismiss_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("root", "dismiss-first"),
            AxumPath((id, approval_id)),
            Json(DismissApprovalRequest {
                reason: Some("first".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let duplicate = dismiss_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("root", "dismiss-second"),
            AxumPath((id, approval_id)),
            Json(DismissApprovalRequest {
                reason: Some("second".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate_body = response_json(duplicate).await;
        assert_eq!(duplicate_body["dismissed"], false);
        assert_eq!(duplicate_body["approval"], first_body["approval"]);
    }

    #[tokio::test]
    async fn review_feedback_merge_is_blocked_after_only_required_approval_is_dismissed() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_ref_rule("main", 1, ROOT_UID)
            .await
            .unwrap();
        let approval = approve_change_request_for(&state, id, "alice").await;
        let approval_id = Uuid::parse_str(approval["approval"]["id"].as_str().unwrap()).unwrap();
        let dismissed = dismiss_change_request_approval(
            State(state.clone()),
            user_headers("root"),
            AxumPath((id, approval_id)),
            Json(DismissApprovalRequest { reason: None }),
        )
        .await
        .into_response();
        assert_eq!(dismissed.status(), StatusCode::OK);

        let merge = merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(merge.status(), StatusCode::FORBIDDEN);
        let merge_body = response_json(merge).await;
        assert_eq!(merge_body["approval_state"]["approval_count"], 0);
        assert_eq!(merge_body["approval_state"]["approved"], false);
    }

    #[tokio::test]
    async fn review_feedback_wrong_change_request_approval_pairing_does_not_mutate() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let approval = approve_change_request_for(&state, id, "alice").await;
        let approval_id = Uuid::parse_str(approval["approval"]["id"].as_str().unwrap()).unwrap();
        let change = state.review.get_change_request(id).await.unwrap().unwrap();
        let other_change = state
            .review
            .create_change_request(NewChangeRequest {
                title: "Other".to_string(),
                description: None,
                source_ref: change.source_ref,
                target_ref: change.target_ref,
                base_commit: change.base_commit,
                head_commit: change.head_commit,
                created_by: ROOT_UID,
            })
            .await
            .unwrap();

        let response = dismiss_change_request_approval(
            State(state.clone()),
            user_headers("root"),
            AxumPath((other_change.id, approval_id)),
            Json(DismissApprovalRequest { reason: None }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let approvals = state.review.list_approvals(id).await.unwrap();
        assert_eq!(approvals.len(), 1);
        assert!(approvals[0].active);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestApprove);
    }

    #[tokio::test]
    async fn approval_duplicate_with_different_key_returns_existing_without_double_counting() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;

        let first = create_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("alice", "approve-first"),
            AxumPath(id),
            Json(CreateApprovalRequest {
                comment: Some("first".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let duplicate = create_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("alice", "approve-duplicate"),
            AxumPath(id),
            Json(CreateApprovalRequest {
                comment: Some("second".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate_body = response_json(duplicate).await;
        assert_eq!(duplicate_body["created"], false);
        assert_eq!(
            duplicate_body["approval"]["id"],
            first_body["approval"]["id"]
        );
        assert_eq!(duplicate_body["approval_state"]["approval_count"], 1);
    }

    #[tokio::test]
    async fn approval_self_approval_is_rejected() {
        let (state, _base, _head, id) = review_fixture().await;

        let response = create_change_request_approval(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn approval_state_is_included_in_change_request_read_and_list_responses() {
        let (state, _base, _head, id) = review_fixture().await;

        let read = get_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        let read_body = response_json(read).await;
        assert_eq!(read_body["change_request"]["id"], id.to_string());
        assert_eq!(read_body["approval_state"]["required_approvals"], 0);

        let listed = list_change_requests(State(state.clone()), user_headers("root"))
            .await
            .into_response();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed_body = response_json(listed).await;
        let first = &listed_body["change_requests"].as_array().unwrap()[0];
        assert_eq!(first["change_request"]["id"], id.to_string());
        assert_eq!(first["approval_state"]["approved"], true);
    }

    #[tokio::test]
    async fn approval_protected_ref_rule_blocks_merge_until_approved() {
        let (state, _base, head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_ref_rule("main", 1, ROOT_UID)
            .await
            .unwrap();

        let blocked =
            merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(blocked.status(), StatusCode::FORBIDDEN);
        let blocked_body = response_json(blocked).await;
        assert_eq!(blocked_body["approval_state"]["required_approvals"], 1);
        assert_eq!(blocked_body["approval_state"]["approved"], false);

        let approval = create_change_request_approval(
            State(state.clone()),
            user_headers("alice"),
            AxumPath(id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(approval.status(), StatusCode::CREATED);

        let merged = merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        let merged_body = response_json(merged).await;
        assert_eq!(merged_body["approval_state"]["approved"], true);
        assert_eq!(merged_body["target_ref"]["target"], head);
    }

    #[tokio::test]
    async fn approval_protected_path_rule_blocks_merge_until_approved() {
        let (state, _base, head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_path_rule("/legal.txt", Some("main"), 1, ROOT_UID)
            .await
            .unwrap();

        let blocked =
            merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(blocked.status(), StatusCode::FORBIDDEN);
        let blocked_body = response_json(blocked).await;
        assert_eq!(
            blocked_body["approval_state"]["matched_path_rules"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let approval = create_change_request_approval(
            State(state.clone()),
            user_headers("alice"),
            AxumPath(id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(approval.status(), StatusCode::CREATED);

        let merged = merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        assert_eq!(response_json(merged).await["target_ref"]["target"], head);
    }

    #[tokio::test]
    async fn approval_required_merge_still_conflicts_when_source_or_target_is_stale() {
        let (source_state, _base, _head, source_stale_id) = review_fixture().await;
        source_state
            .review
            .create_protected_ref_rule("main", 1, ROOT_UID)
            .await
            .unwrap();
        let source_ref = source_state
            .db
            .get_ref("review/cr-1")
            .await
            .unwrap()
            .unwrap();
        let source_change = source_state
            .review
            .get_change_request(source_stale_id)
            .await
            .unwrap()
            .unwrap();
        source_state
            .db
            .update_ref(
                "review/cr-1",
                &source_ref.target,
                source_ref.version,
                &source_change.base_commit,
            )
            .await
            .unwrap();

        let source_stale = merge_change_request(
            State(source_state.clone()),
            user_headers("root"),
            AxumPath(source_stale_id),
        )
        .await
        .into_response();
        assert_eq!(source_stale.status(), StatusCode::CONFLICT);

        let (target_state, _base, head, target_stale_id) = review_fixture().await;
        target_state
            .review
            .create_protected_ref_rule("main", 1, ROOT_UID)
            .await
            .unwrap();
        let main = target_state.db.get_ref("main").await.unwrap().unwrap();
        target_state
            .db
            .update_ref("main", &main.target, main.version, &head)
            .await
            .unwrap();

        let target_stale = merge_change_request(
            State(target_state.clone()),
            user_headers("root"),
            AxumPath(target_stale_id),
        )
        .await
        .into_response();
        assert_eq!(target_stale.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn approval_merge_returns_conflict_when_recorded_commits_are_not_descendants() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let first_head = commit_file(&db, &mut root, "/first.txt", "first", "first").await;
        db.revert(&base).await.unwrap();
        let other_head = commit_file(&db, &mut root, "/other.txt", "other", "other").await;
        db.create_ref("review/cr-1", &first_head).await.unwrap();
        let main = db.get_ref("main").await.unwrap().unwrap();
        db.update_ref("main", &main.target, main.version, &other_head)
            .await
            .unwrap();
        let state = test_state(db);
        let change = state
            .review
            .create_change_request(NewChangeRequest {
                title: "Diverged update".to_string(),
                description: None,
                source_ref: "review/cr-1".to_string(),
                target_ref: "main".to_string(),
                base_commit: other_head,
                head_commit: first_head,
                created_by: ROOT_UID,
            })
            .await
            .unwrap();

        let response = merge_change_request(
            State(state.clone()),
            user_headers("root"),
            AxumPath(change.id),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("is not a descendant")
        );
        let main = state.db.get_ref("main").await.unwrap().unwrap();
        assert_eq!(main.target, change.base_commit);
    }

    #[tokio::test]
    async fn reject_change_request_idempotency_replays_after_status_changes() {
        let (state, _base, _head, id) = review_fixture().await;
        let headers = user_headers_with_idempotency("root", "reject-cr-replay");

        let first = reject_change_request(State(state.clone()), headers.clone(), AxumPath(id))
            .await
            .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = reject_change_request(State(state.clone()), headers, AxumPath(id))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(response_json(replay).await, first_body);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestReject);
    }
}
