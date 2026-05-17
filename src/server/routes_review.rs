use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AppState;
use super::core::{DurableReviewMutationRoute, DurableReviewTargetRefUpdate};
use super::idempotency as http_idempotency;
use super::middleware::require_admin_or_durable_admin_principal;
use super::policy::{
    self, PolicyDecisionToken, RoutePolicyAction, RoutePolicyCorrelation, RoutePolicyEvaluation,
    RoutePolicyRequest, RoutePolicyReviewApproval,
};
use super::repo_context::RequestRepoContext;
use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, Uid, WHEEL_GID};
use crate::backend::{RefRecord, RepoId};
use crate::db::DbVcsRef;
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyBegin, IdempotencyReplayClassification, IdempotencyReservation, request_fingerprint,
};
use crate::review::{
    ApprovalPolicyDecision, ApprovalRecord, ChangeRequest, ChangeRequestStatus,
    DismissApprovalInput, NewApprovalRecord, NewChangeRequest, NewReviewAssignment,
    NewReviewComment, ReviewAssignment, ReviewComment, ReviewCommentKind,
};
use crate::vcs::RefName;

const CREATE_PROTECTED_REF_ROUTE: &str = "POST /protected/refs";
const CREATE_PROTECTED_PATH_ROUTE: &str = "POST /protected/paths";
const CREATE_CHANGE_REQUEST_ROUTE: &str = "POST /change-requests";
const CREATE_CHANGE_REQUEST_APPROVAL_ROUTE: &str = "POST /change-requests/{id}/approvals";
const ASSIGN_CHANGE_REQUEST_REVIEWER_ROUTE: &str = "POST /change-requests/{id}/reviewers";
const CREATE_CHANGE_REQUEST_COMMENT_ROUTE: &str = "POST /change-requests/{id}/comments";
const DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE: &str =
    "POST /change-requests/{id}/approvals/{approval_id}/dismiss";
const REJECT_CHANGE_REQUEST_ROUTE: &str = "POST /change-requests/{id}/reject";
const MERGE_CHANGE_REQUEST_ROUTE: &str = "POST /change-requests/{id}/merge";
const APPROVAL_STATE_UNAVAILABLE_ERROR: &str = "approval state unavailable";
static REVIEW_TRANSITION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Deserialize)]
struct CreateProtectedRefRequest {
    ref_name: String,
    required_approvals: u32,
    require_all_files_viewed: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateProtectedPathRequest {
    path_prefix: String,
    target_ref: Option<String>,
    required_approvals: u32,
    require_all_files_viewed: Option<bool>,
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
struct AssignReviewerRequest {
    reviewer_uid: Uid,
    required: Option<bool>,
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
            "/change-requests/{id}/reviewers",
            get(list_change_request_reviewers).post(assign_change_request_reviewer),
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
    let msg = msg.into();
    let msg = if status == StatusCode::INTERNAL_SERVER_ERROR
        && (msg.starts_with("stratum: corrupt store:") || msg.starts_with("stratum: I/O error:"))
    {
        "internal server error".to_string()
    } else {
        msg
    };
    (status, Json(serde_json::json!({"error": msg})))
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
                || message.starts_with("source ref compare-and-swap mismatch")
                || message.starts_with("invalid change request transition")
                || (message.starts_with("change request ")
                    && message.ends_with(" is not open")) =>
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
    require_admin_or_durable_admin_principal(state, headers, "review").await
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

enum ReviewRefPair {
    Durable {
        source: RefRecord,
        target: RefRecord,
    },
    Local {
        source: DbVcsRef,
        target: DbVcsRef,
    },
}

impl ReviewRefPair {
    fn source_target(&self) -> String {
        match self {
            Self::Durable { source, .. } => source.target.to_hex(),
            Self::Local { source, .. } => source.target.clone(),
        }
    }

    fn target_target(&self) -> String {
        match self {
            Self::Durable { target, .. } => target.target.to_hex(),
            Self::Local { target, .. } => target.target.clone(),
        }
    }
}

async fn review_ref_pair_for_names(
    state: &AppState,
    repo_id: &RepoId,
    source_ref: &str,
    target_ref: &str,
) -> Result<ReviewRefPair, VfsError> {
    if let Some(refs) =
        durable_review_ref_pair_for_names(state, repo_id, source_ref, target_ref).await?
    {
        return Ok(refs);
    }
    if state.core.durable_review_mutation_route().is_some() {
        return Err(VfsError::NotFound {
            path: format!("{source_ref}..{target_ref}"),
        });
    }

    let source = state
        .db
        .get_ref(source_ref)
        .await?
        .ok_or_else(|| VfsError::NotFound {
            path: source_ref.to_string(),
        })?;
    let target = state
        .db
        .get_ref(target_ref)
        .await?
        .ok_or_else(|| VfsError::NotFound {
            path: target_ref.to_string(),
        })?;

    Ok(ReviewRefPair::Local { source, target })
}

async fn durable_review_ref_pair_for_names(
    state: &AppState,
    repo_id: &RepoId,
    source_ref: &str,
    target_ref: &str,
) -> Result<Option<ReviewRefPair>, VfsError> {
    if let Some(capability) = state.core.durable_review_mutation_route() {
        let capability = capability.for_repo(repo_id.clone());
        if let Some((source, target)) = capability
            .ref_pair_for_names(source_ref, target_ref)
            .await?
        {
            return Ok(Some(ReviewRefPair::Durable { source, target }));
        }
        return Ok(None);
    }

    Ok(None)
}

async fn complete_durable_review_ref_pair_for_names(
    state: &AppState,
    repo_id: &RepoId,
    source_ref: &str,
    target_ref: &str,
) -> Result<Option<ReviewRefPair>, VfsError> {
    let Some(capability) = state.core.durable_review_mutation_route() else {
        return Ok(None);
    };
    let capability = capability.for_repo(repo_id.clone());
    Ok(capability
        .complete_ref_pair_for_names(source_ref, target_ref)
        .await?
        .map(|(source, target)| ReviewRefPair::Durable { source, target }))
}

async fn review_ref_pair_for_change(
    state: &AppState,
    change: &ChangeRequest,
) -> Result<ReviewRefPair, VfsError> {
    review_ref_pair_for_names(
        state,
        &change.repo_id,
        &change.source_ref,
        &change.target_ref,
    )
    .await
}

fn durable_ref_to_db_ref(record: RefRecord) -> DbVcsRef {
    DbVcsRef {
        name: record.name.into_string(),
        target: record.target.to_hex(),
        version: record.version.value(),
    }
}

async fn durable_review_changed_paths(
    capability: &DurableReviewMutationRoute,
    base_commit: &str,
    head_commit: &str,
) -> Result<Vec<String>, VfsError> {
    capability
        .changed_paths_between(base_commit, head_commit)
        .await
}

async fn update_review_target_ref(
    state: &AppState,
    change: &ChangeRequest,
    refs: ReviewRefPair,
    changed_paths: &[String],
    review_policy_token: Option<&PolicyDecisionToken>,
) -> Result<DbVcsRef, VfsError> {
    match refs {
        ReviewRefPair::Local { target, .. } => {
            if change.repo_id != RepoId::local() {
                return Err(VfsError::InvalidArgs {
                    message: "local review merge requires local repo context".to_string(),
                });
            }
            state
                .db
                .update_ref_if_source_matches(
                    &change.source_ref,
                    &change.head_commit,
                    &change.target_ref,
                    &target.target,
                    target.version,
                    &change.head_commit,
                )
                .await
        }
        ReviewRefPair::Durable { source, target } => {
            let Some(capability) = state.core.durable_review_mutation_route() else {
                return Err(VfsError::InvalidArgs {
                    message: "durable review capability is unavailable".to_string(),
                });
            };
            let capability = capability.for_repo(change.repo_id.clone());
            let policy_token = review_policy_token.ok_or_else(|| VfsError::PermissionDenied {
                path: "policy decision token".to_string(),
            })?;
            match capability
                .update_target_ref_with_review_token(DurableReviewTargetRefUpdate {
                    change_id: change.id,
                    expected_source_ref: &change.source_ref,
                    expected_target_ref: &change.target_ref,
                    source: &source,
                    target: &target,
                    changed_paths,
                    review_policy_token: policy_token,
                })
                .await
            {
                Ok(record) => Ok(durable_ref_to_db_ref(record)),
                Err(error) => {
                    durable_review_ref_update_error(state, change, &source, &target, error).await
                }
            }
        }
    }
}

async fn durable_review_ref_update_error(
    state: &AppState,
    change: &ChangeRequest,
    expected_source: &RefRecord,
    expected_target: &RefRecord,
    error: VfsError,
) -> Result<DbVcsRef, VfsError> {
    if matches!(error, VfsError::PermissionDenied { .. }) {
        return Err(error);
    }
    let VfsError::InvalidArgs { message } = &error else {
        return Err(VfsError::CorruptStore {
            message: "durable review ref update failed".to_string(),
        });
    };
    if !message.starts_with("ref compare-and-swap mismatch") {
        return Err(VfsError::CorruptStore {
            message: "durable review ref update failed".to_string(),
        });
    }
    match durable_review_ref_staleness(state, change, expected_source, expected_target).await {
        Ok(DurableReviewRefStaleness::Source) => Err(VfsError::InvalidArgs {
            message: "durable source ref compare-and-swap mismatch".to_string(),
        }),
        Ok(DurableReviewRefStaleness::Target) => Err(VfsError::InvalidArgs {
            message: "durable target ref compare-and-swap mismatch".to_string(),
        }),
        Err(_) => Err(error),
    }
}

enum DurableReviewRefStaleness {
    Source,
    Target,
}

async fn durable_review_ref_staleness(
    state: &AppState,
    change: &ChangeRequest,
    expected_source: &RefRecord,
    expected_target: &RefRecord,
) -> Result<DurableReviewRefStaleness, VfsError> {
    let refs = review_ref_pair_for_change(state, change).await?;
    let ReviewRefPair::Durable { source, target } = refs else {
        return Ok(DurableReviewRefStaleness::Target);
    };
    if source.target.to_hex() != change.head_commit || source.version != expected_source.version {
        return Ok(DurableReviewRefStaleness::Source);
    }
    if target.target.to_hex() != change.base_commit || target.version != expected_target.version {
        return Ok(DurableReviewRefStaleness::Target);
    }
    Ok(DurableReviewRefStaleness::Target)
}

async fn approval_decision(
    state: &AppState,
    change: &ChangeRequest,
) -> Result<ApprovalPolicyDecision, VfsError> {
    let changed_paths = changed_paths_for_change(state, change).await?;
    approval_decision_for_paths(state, change, &changed_paths).await
}

async fn changed_paths_for_change(
    state: &AppState,
    change: &ChangeRequest,
) -> Result<Vec<String>, VfsError> {
    if let Some(capability) = state.core.durable_review_mutation_route() {
        if complete_durable_review_ref_pair_for_names(
            state,
            &change.repo_id,
            &change.source_ref,
            &change.target_ref,
        )
        .await?
        .is_some()
        {
            let capability = capability.for_repo(change.repo_id.clone());
            return durable_review_changed_paths(
                &capability,
                &change.base_commit,
                &change.head_commit,
            )
            .await;
        }
        return Err(VfsError::NotFound {
            path: format!("{}..{}", change.source_ref, change.target_ref),
        });
    }

    state
        .db
        .changed_paths_between(&change.base_commit, &change.head_commit)
        .await
}

async fn approval_decision_for_paths(
    state: &AppState,
    change: &ChangeRequest,
    changed_paths: &[String],
) -> Result<ApprovalPolicyDecision, VfsError> {
    state
        .review
        .approval_decision_for_repo(&change.repo_id, change.id, changed_paths)
        .await?
        .ok_or_else(|| VfsError::NotFound {
            path: format!("change request {}", change.id),
        })
}

struct ChangeApprovalSummary {
    approval_state: serde_json::Value,
    require_all_files_viewed: bool,
}

fn approval_state_value(decision: &ApprovalPolicyDecision) -> serde_json::Value {
    serde_json::to_value(decision).expect("approval policy decision serializes")
}

async fn change_approval_summary(
    state: &AppState,
    change: &ChangeRequest,
) -> ChangeApprovalSummary {
    match approval_decision(state, change).await {
        Ok(decision) => ChangeApprovalSummary {
            require_all_files_viewed: decision.require_all_files_viewed,
            approval_state: approval_state_value(&decision),
        },
        Err(_) => ChangeApprovalSummary {
            approval_state: serde_json::json!({
                "available": false,
                "error": APPROVAL_STATE_UNAVAILABLE_ERROR,
            }),
            require_all_files_viewed: true,
        },
    }
}

async fn change_json(state: &AppState, change: &ChangeRequest) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "change_request": change,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn approval_list_json(
    state: &AppState,
    change: &ChangeRequest,
    approvals: Vec<ApprovalRecord>,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "approvals": approvals,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn approval_mutation_json(
    state: &AppState,
    change: &ChangeRequest,
    approval: ApprovalRecord,
    created: bool,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "approval": approval,
        "created": created,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn assignment_list_json(
    state: &AppState,
    change: &ChangeRequest,
    assignments: Vec<ReviewAssignment>,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "assignments": assignments,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn assignment_mutation_json(
    state: &AppState,
    change: &ChangeRequest,
    assignment: ReviewAssignment,
    created: bool,
    updated: bool,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "assignment": assignment,
        "created": created,
        "updated": updated,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn comment_list_json(
    state: &AppState,
    change: &ChangeRequest,
    comments: Vec<ReviewComment>,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "comments": comments,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn comment_mutation_json(
    state: &AppState,
    change: &ChangeRequest,
    comment: ReviewComment,
    created: bool,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "comment": comment,
        "created": created,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
    })
}

async fn approval_dismissal_json(
    state: &AppState,
    change: &ChangeRequest,
    approval: ApprovalRecord,
    dismissed: bool,
) -> serde_json::Value {
    let summary = change_approval_summary(state, change).await;
    serde_json::json!({
        "approval": approval,
        "dismissed": dismissed,
        "approval_state": summary.approval_state,
        "require_all_files_viewed": summary.require_all_files_viewed,
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
            "error": "audit append failed after mutation",
            "mutation_committed": true,
            "audit_recorded": false,
        }),
    )
}

fn policy_audit_append_failed_body(error: VfsError) -> (StatusCode, serde_json::Value) {
    (
        error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
        serde_json::json!({
            "error": "audit append failed before mutation",
            "mutation_committed": false,
            "audit_recorded": false,
        }),
    )
}

async fn append_policy_audit(
    state: &AppState,
    session: &Session,
    evaluation: &RoutePolicyEvaluation,
) -> Result<(), axum::response::Response> {
    state
        .audit
        .append(policy::audit_event_from_policy_evaluation(
            session, evaluation,
        ))
        .await
        .map(|_| ())
        .map_err(|error| {
            let (status, body) = policy_audit_append_failed_body(error);
            json_response(status, body)
        })
}

fn policy_correlation_from_headers(headers: &HeaderMap) -> RoutePolicyCorrelation {
    RoutePolicyCorrelation {
        request_present: headers.contains_key("x-request-id")
            || headers.contains_key("x-correlation-id"),
        idempotency_present: headers.contains_key("idempotency-key"),
    }
}

fn review_mutation_audit_event(
    session: &Session,
    action: AuditAction,
    resource: AuditResource,
    route: &'static str,
    change: &ChangeRequest,
    reservation: Option<&IdempotencyReservation>,
) -> NewAuditEvent {
    let event = NewAuditEvent::from_session(session, action, resource)
        .with_detail("route", route)
        .with_detail("change_request_id", change.id)
        .with_detail("source_ref", &change.source_ref)
        .with_detail("target_ref", &change.target_ref)
        .with_detail("base_commit", &change.base_commit)
        .with_detail("head_commit", &change.head_commit);

    if reservation.is_some() {
        event.with_detail("idempotency_present", true)
    } else {
        event
    }
}

fn review_fingerprint_body(
    mut body: serde_json::Value,
    repo: &RequestRepoContext,
) -> serde_json::Value {
    if let Some(object) = body.as_object_mut() {
        if repo.is_local_singleton() {
            object.remove("repo_id");
        } else {
            object.insert(
                "repo_id".to_string(),
                serde_json::Value::String(repo.repo_id().as_str().to_string()),
            );
        }
    }
    body
}

fn include_non_default_file_view_flag(
    body: &mut serde_json::Value,
    require_all_files_viewed: Option<bool>,
) {
    if require_all_files_viewed != Some(false) {
        return;
    }
    if let Some(object) = body.as_object_mut() {
        object.insert(
            "require_all_files_viewed".to_string(),
            serde_json::Value::Bool(false),
        );
    }
}

fn sanitized_review_idempotency_body(body: &serde_json::Value) -> serde_json::Value {
    match body {
        serde_json::Value::Object(object) => serde_json::Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    if review_idempotency_sensitive_field(key, value) {
                        (key.clone(), serde_json::Value::Null)
                    } else {
                        (key.clone(), sanitized_review_idempotency_body(value))
                    }
                })
                .collect(),
        ),
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(sanitized_review_idempotency_body)
                .collect(),
        ),
        _ => body.clone(),
    }
}

fn review_idempotency_sensitive_field(key: &str, value: &serde_json::Value) -> bool {
    matches!(
        key,
        "title" | "description" | "body" | "reason" | "dismissal_reason"
    ) || (key == "comment" && !value.is_object() && !value.is_array())
}

async fn begin_review_idempotency(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    scope: &str,
    repo: &RequestRepoContext,
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

    let scope = if repo.is_local_singleton() {
        scope.to_string()
    } else {
        format!("repo:{}:{scope}", repo.repo_id())
    };

    let fingerprint_body = review_fingerprint_body(fingerprint_body, repo);
    let fingerprint = match request_fingerprint(&scope, &fingerprint_body) {
        Ok(fingerprint) => fingerprint,
        Err(e) => {
            return ReviewIdempotency::Respond(
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response(),
            );
        }
    };

    match state.idempotency.begin(&scope, &key, &fingerprint).await {
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
            http_idempotency::idempotency_quota_response_if_quota_error_with_audit(
                state, session, "review", &e,
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

async fn complete_review_idempotency(
    state: &AppState,
    reservation: Option<&IdempotencyReservation>,
    status: StatusCode,
    body: &serde_json::Value,
) -> Result<(), axum::response::Response> {
    if let Some(reservation) = reservation {
        let sanitized_body = sanitized_review_idempotency_body(body);
        let classification = if &sanitized_body == body {
            IdempotencyReplayClassification::SecretFree
        } else {
            IdempotencyReplayClassification::Partial
        };
        state
            .idempotency
            .complete_with_classification(
                reservation,
                status.as_u16(),
                sanitized_body,
                classification,
            )
            .await
            .map_err(|e| {
                (
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    Json(serde_json::json!({
                        "error": "idempotency completion failed after mutation",
                        "mutation_committed": true,
                        "idempotency_recorded": false,
                    })),
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
    repo_id: &RepoId,
    id: Uuid,
) -> Result<ChangeRequest, axum::response::Response> {
    match state.review.get_change_request_for_repo(repo_id, id).await {
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

fn resolve_review_repo_context(
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

async fn list_protected_refs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match state
        .review
        .list_protected_ref_rules_for_repo(repo.repo_id())
        .await
    {
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let require_all_files_viewed = req.require_all_files_viewed.unwrap_or(true);

    let mut fingerprint_body = serde_json::json!({
        "route": CREATE_PROTECTED_REF_ROUTE,
        "actor": actor_fingerprint(&session),
        "repo_id": repo.repo_id().as_str(),
        "ref_name": &ref_name,
        "required_approvals": req.required_approvals,
    });
    include_non_default_file_view_flag(&mut fingerprint_body, req.require_all_files_viewed);

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        CREATE_PROTECTED_REF_ROUTE,
        &repo,
        fingerprint_body,
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    match state
        .review
        .create_protected_ref_rule_for_repo(
            repo.repo_id(),
            &ref_name,
            req.required_approvals,
            session.effective_uid(),
            require_all_files_viewed,
        )
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
            .with_detail("require_all_files_viewed", rule.require_all_files_viewed)
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match state
        .review
        .list_protected_path_rules_for_repo(repo.repo_id())
        .await
    {
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let require_all_files_viewed = req.require_all_files_viewed.unwrap_or(true);

    let mut fingerprint_body = serde_json::json!({
        "route": CREATE_PROTECTED_PATH_ROUTE,
        "actor": actor_fingerprint(&session),
        "repo_id": repo.repo_id().as_str(),
        "path_prefix": &path_prefix,
        "target_ref": target_ref.as_deref(),
        "required_approvals": req.required_approvals,
    });
    include_non_default_file_view_flag(&mut fingerprint_body, req.require_all_files_viewed);

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        CREATE_PROTECTED_PATH_ROUTE,
        &repo,
        fingerprint_body,
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    match state
        .review
        .create_protected_path_rule_for_repo(
            repo.repo_id(),
            &path_prefix,
            target_ref.as_deref(),
            req.required_approvals,
            session.effective_uid(),
            require_all_files_viewed,
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
            .with_detail("require_all_files_viewed", rule.require_all_files_viewed)
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match state
        .review
        .list_change_requests_for_repo(repo.repo_id())
        .await
    {
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        CREATE_CHANGE_REQUEST_ROUTE,
        &repo,
        serde_json::json!({
            "route": CREATE_CHANGE_REQUEST_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
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

    let refs =
        match review_ref_pair_for_names(&state, repo.repo_id(), &source_ref, &target_ref).await {
            Ok(refs) => refs,
            Err(VfsError::NotFound { path }) => {
                abort_review_idempotency(&state, reservation.as_ref()).await;
                return json_response(StatusCode::NOT_FOUND, not_found_body("ref", path));
            }
            Err(e) => {
                abort_review_idempotency(&state, reservation.as_ref()).await;
                return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                    .into_response();
            }
        };

    match state
        .review
        .create_change_request_for_repo(
            repo.repo_id(),
            NewChangeRequest {
                title: normalized_title,
                description: req.description,
                source_ref: source_ref.clone(),
                target_ref: target_ref.clone(),
                base_commit: refs.target_target(),
                head_commit: refs.source_target(),
                created_by: session.effective_uid(),
            },
        )
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
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => Json(change_json(&state, &change).await).into_response(),
        Err(response) => response,
    }
}

async fn list_change_request_approvals(
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    match state
        .review
        .list_approvals_for_repo(repo.repo_id(), id)
        .await
    {
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        CREATE_CHANGE_REQUEST_APPROVAL_ROUTE,
        &repo,
        serde_json::json!({
            "route": CREATE_CHANGE_REQUEST_APPROVAL_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
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
        .create_approval_for_repo(
            repo.repo_id(),
            NewApprovalRecord {
                change_request_id: id,
                head_commit: change.head_commit.clone(),
                approved_by: session.effective_uid(),
                comment: req.comment,
            },
        )
        .await
    {
        Ok(mutation) => {
            let body =
                approval_mutation_json(&state, &change, mutation.record.clone(), mutation.created)
                    .await;
            if mutation.created {
                let event = review_mutation_audit_event(
                    &session,
                    AuditAction::ChangeRequestApprove,
                    AuditResource::id(
                        AuditResourceKind::ApprovalRecord,
                        mutation.record.id.to_string(),
                    ),
                    CREATE_CHANGE_REQUEST_APPROVAL_ROUTE,
                    &change,
                    reservation.as_ref(),
                )
                .with_detail("approval_id", mutation.record.id)
                .with_detail("approved_by", mutation.record.approved_by)
                .with_detail("version", mutation.record.version)
                .with_detail("created", mutation.created);
                if let Err(e) = state.audit.append(event).await {
                    let (status, body) = audit_append_failed_body(e);
                    if let Err(response) =
                        complete_review_idempotency(&state, reservation.as_ref(), status, &body)
                            .await
                    {
                        return response;
                    }
                    return json_response(status, body);
                }
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

async fn list_change_request_reviewers(
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    match state
        .review
        .list_reviewer_assignments_for_repo(repo.repo_id(), id)
        .await
    {
        Ok(assignments) => {
            Json(assignment_list_json(&state, &change, assignments).await).into_response()
        }
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn assign_change_request_reviewer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(req): Json<AssignReviewerRequest>,
) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let reviewer_uid = req.reviewer_uid;
    let required = req.required.unwrap_or(true);
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    if let Err(response) = get_change_or_404(&state, repo.repo_id(), id).await {
        return response;
    }

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        ASSIGN_CHANGE_REQUEST_REVIEWER_ROUTE,
        &repo,
        serde_json::json!({
            "route": ASSIGN_CHANGE_REQUEST_REVIEWER_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
            "change_request_id": id,
            "reviewer_uid": reviewer_uid,
            "required": required,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let _transition_guard = REVIEW_TRANSITION_LOCK.lock().await;

    let change = match state
        .review
        .get_change_request_for_repo(repo.repo_id(), id)
        .await
    {
        Ok(Some(change)) => change,
        Ok(None) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return json_response(StatusCode::NOT_FOUND, not_found_body("change request", id));
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response();
        }
    };
    if change.status != ChangeRequestStatus::Open {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({"error": format!("change request {id} is not open")}),
        );
    }

    let assignments = match state
        .review
        .list_reviewer_assignments_for_repo(repo.repo_id(), id)
        .await
    {
        Ok(assignments) => assignments,
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response();
        }
    };
    let existing_assignment = assignments
        .iter()
        .find(|assignment| assignment.active && assignment.reviewer == reviewer_uid);
    let requires_current_approval_rights = existing_assignment
        .map(|assignment| required && !assignment.required)
        .unwrap_or(true);
    if requires_current_approval_rights
        && let Err(e) = validate_reviewer_can_approve(&state, &session, reviewer_uid).await
    {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return reviewer_validation_error_response(reviewer_uid, e);
    }

    match state
        .review
        .assign_reviewer_for_repo(
            repo.repo_id(),
            NewReviewAssignment {
                change_request_id: id,
                reviewer: reviewer_uid,
                assigned_by: session.effective_uid(),
                required,
            },
        )
        .await
    {
        Ok(mutation) => {
            let body = assignment_mutation_json(
                &state,
                &change,
                mutation.assignment.clone(),
                mutation.created,
                mutation.updated,
            )
            .await;
            if mutation.created || mutation.updated {
                let event = review_mutation_audit_event(
                    &session,
                    AuditAction::ChangeRequestReviewerAssign,
                    AuditResource::id(
                        AuditResourceKind::ReviewAssignment,
                        mutation.assignment.id.to_string(),
                    ),
                    ASSIGN_CHANGE_REQUEST_REVIEWER_ROUTE,
                    &change,
                    reservation.as_ref(),
                )
                .with_detail("assignment_id", mutation.assignment.id)
                .with_detail("reviewer", mutation.assignment.reviewer)
                .with_detail("assigned_by", mutation.assignment.assigned_by)
                .with_detail("required", mutation.assignment.required)
                .with_detail("active", mutation.assignment.active)
                .with_detail("version", mutation.assignment.version)
                .with_detail("created", mutation.created)
                .with_detail("updated", mutation.updated);
                if let Err(e) = state.audit.append(event).await {
                    let (status, body) = audit_append_failed_body(e);
                    if let Err(response) =
                        complete_review_idempotency(&state, reservation.as_ref(), status, &body)
                            .await
                    {
                        return response;
                    }
                    return json_response(status, body);
                }
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

async fn validate_reviewer_can_approve(
    state: &AppState,
    actor_session: &Session,
    reviewer_uid: Uid,
) -> Result<(), VfsError> {
    if state.core.durable_core_repo_id().is_some() {
        if actor_session.uid != reviewer_uid {
            return Err(VfsError::PermissionDenied {
                path: "reviewer principal".to_string(),
            });
        }
        if actor_session.uid == ROOT_UID || actor_session.groups.contains(&WHEEL_GID) {
            return Ok(());
        }
        return Err(VfsError::PermissionDenied {
            path: "reviewer principal".to_string(),
        });
    }

    let reviewer_session = state.db.session_for_uid(reviewer_uid).await?;
    require_admin_session(&reviewer_session)
}

fn reviewer_validation_error_response(
    reviewer_uid: Uid,
    error: VfsError,
) -> axum::response::Response {
    match error {
        VfsError::AuthError { .. } | VfsError::NotFound { .. } | VfsError::NotSupported { .. } => {
            json_response(
                StatusCode::NOT_FOUND,
                serde_json::json!({
                    "error": format!("unknown reviewer uid {reviewer_uid}")
                }),
            )
        }
        error => err_json(
            StatusCode::BAD_REQUEST,
            format!("reviewer uid {reviewer_uid} cannot approve change requests: {error}"),
        )
        .into_response(),
    }
}

async fn list_change_request_comments(
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => change,
        Err(response) => return response,
    };

    match state
        .review
        .list_comments_for_repo(repo.repo_id(), id)
        .await
    {
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => change,
        Err(response) => return response,
    };
    let kind = req.kind.unwrap_or(ReviewCommentKind::General);

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
        &repo,
        serde_json::json!({
            "route": CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
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
        .create_comment_for_repo(
            repo.repo_id(),
            NewReviewComment {
                change_request_id: id,
                author: session.effective_uid(),
                body: req.body,
                path: req.path,
                kind,
            },
        )
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
            let event = review_mutation_audit_event(
                &session,
                AuditAction::ChangeRequestCommentCreate,
                resource,
                CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
                &change,
                reservation.as_ref(),
            )
            .with_detail("comment_id", mutation.comment.id)
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
        &repo,
        serde_json::json!({
            "route": DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
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

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
        Ok(change) => change,
        Err(response) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
    };

    match state
        .review
        .dismiss_approval_for_repo(
            repo.repo_id(),
            DismissApprovalInput {
                change_request_id: id,
                approval_id,
                dismissed_by: session.effective_uid(),
                reason: req.reason,
            },
        )
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
            if mutation.dismissed {
                let event = review_mutation_audit_event(
                    &session,
                    AuditAction::ChangeRequestApprovalDismiss,
                    AuditResource::id(
                        AuditResourceKind::ApprovalRecord,
                        mutation.record.id.to_string(),
                    ),
                    DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
                    &change,
                    reservation.as_ref(),
                )
                .with_detail("approval_id", mutation.record.id)
                .with_detail("dismissed_by", session.effective_uid())
                .with_detail("dismissed", mutation.dismissed)
                .with_detail("version", mutation.record.version);
                if let Err(e) = state.audit.append(event).await {
                    let (status, body) = audit_append_failed_body(e);
                    if let Err(response) =
                        complete_review_idempotency(&state, reservation.as_ref(), status, &body)
                            .await
                    {
                        return response;
                    }
                    return json_response(status, body);
                }
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        REJECT_CHANGE_REQUEST_ROUTE,
        &repo,
        serde_json::json!({
            "route": REJECT_CHANGE_REQUEST_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
            "change_request_id": id,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let _transition_guard = REVIEW_TRANSITION_LOCK.lock().await;

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
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

    let policy_request =
        RoutePolicyRequest::from_session(RoutePolicyAction::ReviewReject, &session)
            .with_repo_id(repo.repo_id().clone())
            .with_target_ref(&change.target_ref)
            .with_correlation(policy_correlation_from_headers(&headers));
    let policy_evaluation =
        match policy::evaluate_route_policy(state.review.as_ref(), policy_request).await {
            Ok(evaluation) => evaluation,
            Err(e) => {
                abort_review_idempotency(&state, reservation.as_ref()).await;
                return err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response();
            }
        };
    if !policy_evaluation.decision.is_allowed() {
        if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return err_json(
            StatusCode::FORBIDDEN,
            format!(
                "protected ref '{}' requires change request merge",
                change.target_ref
            ),
        )
        .into_response();
    }
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    match state
        .review
        .transition_change_request_for_repo(repo.repo_id(), id, ChangeRequestStatus::Rejected)
        .await
    {
        Ok(Some(change)) => {
            let body = change_json(&state, &change).await;
            let event = review_mutation_audit_event(
                &session,
                AuditAction::ChangeRequestReject,
                AuditResource::id(AuditResourceKind::ChangeRequest, change.id.to_string()),
                REJECT_CHANGE_REQUEST_ROUTE,
                &change,
                reservation.as_ref(),
            )
            .with_detail("status", "rejected")
            .with_detail("change_request_version", change.version);
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
    let repo = match resolve_review_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    let reservation = match begin_review_idempotency(
        &state,
        &session,
        &headers,
        MERGE_CHANGE_REQUEST_ROUTE,
        &repo,
        serde_json::json!({
            "route": MERGE_CHANGE_REQUEST_ROUTE,
            "actor": actor_fingerprint(&session),
            "repo_id": repo.repo_id().as_str(),
            "change_request_id": id,
        }),
    )
    .await
    {
        ReviewIdempotency::Execute(reservation) => reservation,
        ReviewIdempotency::Respond(response) => return response,
    };

    let _transition_guard = REVIEW_TRANSITION_LOCK.lock().await;

    let change = match get_change_or_404(&state, repo.repo_id(), id).await {
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

    let refs = match review_ref_pair_for_change(&state, &change).await {
        Ok(refs) => refs,
        Err(VfsError::NotFound { path }) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return json_response(StatusCode::NOT_FOUND, not_found_body("ref", path));
        }
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };

    if refs.source_target() != change.head_commit {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": format!("change request {id} source ref is stale")
            }),
        );
    }
    if refs.target_target() != change.base_commit {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return json_response(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": format!("change request {id} target ref is stale")
            }),
        );
    }

    let changed_paths = match changed_paths_for_change(&state, &change).await {
        Ok(paths) => paths,
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(StatusCode::CONFLICT, e.to_string()).into_response();
        }
    };
    let approval_state = match approval_decision_for_paths(&state, &change, &changed_paths).await {
        Ok(decision) => decision,
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return err_json(StatusCode::CONFLICT, e.to_string()).into_response();
        }
    };
    let mut policy_request =
        RoutePolicyRequest::from_session(RoutePolicyAction::ReviewMerge, &session)
            .with_repo_id(repo.repo_id().clone())
            .with_target_ref(&change.target_ref)
            .with_changed_paths(changed_paths.clone())
            .with_correlation(policy_correlation_from_headers(&headers));
    policy_request.review_approval = Some(RoutePolicyReviewApproval {
        approved: approval_state.approved,
        change_request_id: change.id,
        matched_ref_rule_count: approval_state.matched_ref_rules.len(),
        matched_path_rule_count: approval_state.matched_path_rules.len(),
    });
    let policy_evaluation =
        match policy::evaluate_route_policy(state.review.as_ref(), policy_request).await {
            Ok(evaluation) => evaluation,
            Err(e) => {
                abort_review_idempotency(&state, reservation.as_ref()).await;
                return err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response();
            }
        };
    if !approval_state.approved {
        if !policy_evaluation.decision.is_allowed()
            && let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await
        {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
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
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_review_idempotency(&state, reservation.as_ref()).await;
        return response;
    }
    let review_policy_token =
        match PolicyDecisionToken::from_review_approved_evaluation(&policy_evaluation) {
            Ok(token) => token,
            Err(e) => {
                abort_review_idempotency(&state, reservation.as_ref()).await;
                return err_json(error_status(&e, StatusCode::FORBIDDEN), e.to_string())
                    .into_response();
            }
        };

    let updated_ref = match update_review_target_ref(
        &state,
        &change,
        refs,
        &changed_paths,
        Some(&review_policy_token),
    )
    .await
    {
        Ok(vcs_ref) => vcs_ref,
        Err(e) => {
            abort_review_idempotency(&state, reservation.as_ref()).await;
            if let VfsError::InvalidArgs { message } = &e {
                if message.starts_with("source ref compare-and-swap mismatch")
                    || message == "durable source ref compare-and-swap mismatch"
                {
                    return json_response(
                        StatusCode::CONFLICT,
                        serde_json::json!({
                            "error": format!("change request {id} source ref is stale")
                        }),
                    );
                }
                if message.starts_with("ref compare-and-swap mismatch")
                    || message == "durable target ref compare-and-swap mismatch"
                {
                    return json_response(
                        StatusCode::CONFLICT,
                        serde_json::json!({
                            "error": format!("change request {id} target ref is stale")
                        }),
                    );
                }
            }
            return err_json(error_status(&e, StatusCode::CONFLICT), e.to_string()).into_response();
        }
    };

    let merged = match state
        .review
        .transition_change_request_for_repo(repo.repo_id(), id, ChangeRequestStatus::Merged)
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
                "change request update failed after target ref update",
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
        "require_all_files_viewed": approval_state.require_all_files_viewed,
        "target_ref": ref_json(updated_ref.clone()),
    });
    let event = review_mutation_audit_event(
        &session,
        AuditAction::ChangeRequestMerge,
        AuditResource::id(AuditResourceKind::ChangeRequest, merged.id.to_string()),
        MERGE_CHANGE_REQUEST_ROUTE,
        &merged,
        reservation.as_ref(),
    )
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
    use crate::audit::{
        AuditAction, AuditEvent, AuditResourceKind, AuditStore, InMemoryAuditStore,
    };
    use crate::auth::ROOT_UID;
    use crate::auth::session::Session;
    use crate::backend::runtime::BackendRuntimeMode;
    use crate::backend::{CommitRecord, RefExpectation, RefUpdate, RepoId, StratumStores};
    use crate::db::StratumDb;
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyReplayClassification, IdempotencyReservation,
        IdempotencyStore, InMemoryIdempotencyStore,
    };
    use crate::review::{
        ApprovalDismissalMutation, ApprovalPolicyDecision, ApprovalRecord, ApprovalRecordMutation,
        ChangeRequest, ChangeRequestStatus, DismissApprovalInput, InMemoryReviewStore,
        NewApprovalRecord, NewChangeRequest, NewReviewAssignment, NewReviewComment,
        ProtectedPathRule, ProtectedRefRule, ReviewAssignment, ReviewAssignmentMutation,
        ReviewComment, ReviewCommentMutation, ReviewStore,
    };
    use crate::server::{ServerLocalDb, ServerState, ServerStores};
    use crate::store::ObjectId;
    use crate::vcs::{ChangeKind, ChangedPath, CommitId};
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, ValidWorkspaceToken,
        WorkspaceMetadataStore, WorkspacePrincipalKind, WorkspacePrincipalRecord, WorkspaceRecord,
        WorkspaceTokenRecord,
    };
    use axum::extract::Path as AxumPath;
    use std::sync::Arc;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        })
    }

    fn test_state_with_durable_review(
        db: StratumDb,
        repo_id: RepoId,
        stores: StratumStores,
    ) -> AppState {
        Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared_with_guarded_durable_commit_route(
                db.clone(),
                repo_id,
                stores,
            ),
            db: ServerLocalDb::available(Arc::new(db)),
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
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        })
    }

    fn durable_cloud_admin_state(
        repo_id: RepoId,
        workspaces: Arc<dyn WorkspaceMetadataStore>,
    ) -> AppState {
        let stores = StratumStores::local_memory();
        Arc::new(ServerState {
            core: Arc::new(crate::server::core::DurableCoreRuntime::new(
                repo_id, stores,
            )),
            db: ServerLocalDb::unavailable(),
            workspaces,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        })
    }

    struct DurableWorkspaceBearerStore {
        workspace: WorkspaceRecord,
        token: WorkspaceTokenRecord,
        principal: WorkspacePrincipalRecord,
        raw_secret: String,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for DurableWorkspaceBearerStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(vec![self.workspace.clone()])
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            Ok((id == self.workspace.id).then(|| self.workspace.clone()))
        }

        async fn update_head_commit(
            &self,
            _id: Uuid,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            unreachable!("not used")
        }

        async fn update_head_commit_if_current(
            &self,
            _id: Uuid,
            _expected_head_commit: Option<&str>,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            unreachable!("not used")
        }

        async fn issue_scoped_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: crate::auth::Uid,
            _read_prefixes: Vec<String>,
            _write_prefixes: Vec<String>,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token_at(
            &self,
            workspace_id: Uuid,
            raw_secret: &str,
            _now_unix: u64,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            if workspace_id != self.workspace.id || raw_secret != self.raw_secret {
                return Ok(None);
            }
            Ok(Some(ValidWorkspaceToken {
                workspace: self.workspace.clone(),
                token: self.token.clone(),
                repo_id: self.workspace.repo_id.clone(),
                principal: Some(self.principal.clone()),
            }))
        }
    }

    fn durable_workspace_bearer_store(
        repo_id: &RepoId,
        uid: crate::auth::Uid,
        groups: Vec<crate::auth::Gid>,
    ) -> (Arc<dyn WorkspaceMetadataStore>, Uuid, String) {
        let workspace_id = Uuid::new_v4();
        let raw_secret = format!("durable-review-token-{workspace_id}");
        let workspace = WorkspaceRecord {
            id: workspace_id,
            name: "durable-review".to_string(),
            root_path: "/".to_string(),
            head_commit: None,
            version: 1,
            base_ref: "main".to_string(),
            session_ref: Some("agent/durable/review".to_string()),
            repo_id: Some(repo_id.as_str().to_string()),
        };
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: "durable-review-token".to_string(),
            agent_uid: uid,
            secret_hash: "redacted-hash".to_string(),
            read_prefixes: vec!["/".to_string()],
            write_prefixes: vec!["/".to_string()],
            principal_uid: Some(uid),
            token_version: 1,
            issued_at_unix: 1,
            updated_at_unix: 1,
            expires_at_unix: None,
            revoked_at_unix: None,
        };
        let principal = WorkspacePrincipalRecord {
            uid,
            username: format!("durable-review-principal-{uid}"),
            gid: groups.first().copied().unwrap_or(uid),
            groups,
            kind: WorkspacePrincipalKind::Agent,
            active: true,
        };
        (
            Arc::new(DurableWorkspaceBearerStore {
                workspace,
                token,
                principal,
                raw_secret: raw_secret.clone(),
            }),
            workspace_id,
            raw_secret,
        )
    }

    mod durable_cloud_admin {
        use super::*;

        #[tokio::test]
        async fn rejects_user_root_for_review_admin_gate() {
            let repo_id = RepoId::new("repo_review_admin_user_root").unwrap();
            let state = durable_cloud_admin_state(
                repo_id.clone(),
                Arc::new(InMemoryWorkspaceMetadataStore::new()),
            );
            let headers = user_headers_for_repo("root", &repo_id);

            let err = require_admin(&state, &headers)
                .await
                .expect_err("durable-cloud review admin gate must reject User root");

            assert!(matches!(
                err,
                VfsError::AuthError { .. } | VfsError::NotSupported { .. }
            ));
        }

        #[tokio::test]
        async fn accepts_repo_scoped_wheel_workspace_bearer_for_review_admin_gate() {
            let repo_id = RepoId::new("repo_review_admin_wheel").unwrap();
            let (workspaces, workspace_id, raw_secret) =
                durable_workspace_bearer_store(&repo_id, 501, vec![crate::auth::WHEEL_GID]);
            let state = durable_cloud_admin_state(repo_id.clone(), workspaces);
            let mut headers = workspace_bearer_headers(&raw_secret, workspace_id);
            headers.insert("x-stratum-repo", repo_id.as_str().parse().unwrap());

            let session = require_admin(&state, &headers)
                .await
                .expect("wheel durable workspace bearer passes review admin gate");

            assert_eq!(session.uid, 501);
            assert!(session.scope.is_some());
            assert_eq!(
                session
                    .mount()
                    .and_then(crate::auth::session::SessionMount::repo_id),
                Some(repo_id.as_str())
            );
        }
    }

    mod durable_cloud {
        use super::*;

        fn durable_review_router(
            stores: StratumStores,
            workspaces: Arc<dyn WorkspaceMetadataStore>,
            repo_id: RepoId,
        ) -> Router {
            crate::server::build_durable_core_router(
                ServerStores {
                    backend_mode: BackendRuntimeMode::Durable,
                    workspaces,
                    idempotency: stores.idempotency.clone(),
                    audit: stores.audit.clone(),
                    review: stores.review.clone(),
                    guarded_durable_commit_stores: None,
                    durable_core_stores: Some(stores),
                },
                repo_id,
            )
        }

        fn durable_headers(raw_secret: &str, workspace_id: Uuid, repo_id: &RepoId) -> HeaderMap {
            let mut headers = workspace_bearer_headers(raw_secret, workspace_id);
            headers.insert("x-stratum-repo", repo_id.as_str().parse().unwrap());
            headers
        }

        async fn seed_ref(stores: &StratumStores, repo_id: &RepoId, name: &str, target: CommitId) {
            stores
                .refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: RefName::new(name).unwrap(),
                    target,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
        }

        async fn seed_commit(
            stores: &StratumStores,
            repo_id: &RepoId,
            id: CommitId,
            parents: Vec<CommitId>,
            path: &str,
        ) {
            stores
                .commits
                .insert(durable_commit_record(
                    repo_id,
                    id,
                    parents,
                    vec![ChangedPath {
                        path: path.to_string(),
                        kind: ChangeKind::Modified,
                        before: None,
                        after: None,
                    }],
                ))
                .await
                .unwrap();
        }

        fn id_from_change_response(body: &serde_json::Value) -> Uuid {
            Uuid::parse_str(
                body["change_request"]["id"]
                    .as_str()
                    .expect("change request id"),
            )
            .unwrap()
        }

        async fn create_change(
            client: &reqwest::Client,
            base_url: &str,
            headers: HeaderMap,
            source_ref: &str,
            target_ref: &str,
        ) -> serde_json::Value {
            let response = client
                .post(format!("{base_url}/change-requests"))
                .headers(headers)
                .json(&serde_json::json!({
                    "title": "Durable cloud review",
                    "description": "metadata only",
                    "source_ref": source_ref,
                    "target_ref": target_ref,
                }))
                .send()
                .await
                .expect("change request create completes");
            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("change body is json");
            assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
            body
        }

        #[tokio::test]
        async fn review_and_protected_routes_use_durable_stores_over_http() {
            let repo_id = RepoId::new("repo_durable_cloud_review").unwrap();
            let stores = StratumStores::local_memory();
            let base = durable_commit_id("durable-cloud-review-base");
            let merge_head = durable_commit_id("durable-cloud-review-merge-head");
            let reject_head = durable_commit_id("durable-cloud-review-reject-head");
            let dev_base = durable_commit_id("durable-cloud-review-dev-base");
            seed_commit(&stores, &repo_id, base, Vec::new(), "/legal.txt").await;
            seed_commit(&stores, &repo_id, merge_head, vec![base], "/legal.txt").await;
            seed_commit(&stores, &repo_id, reject_head, vec![dev_base], "/notes.txt").await;
            seed_commit(&stores, &repo_id, dev_base, Vec::new(), "/notes.txt").await;
            seed_ref(&stores, &repo_id, "main", base).await;
            seed_ref(&stores, &repo_id, "archive/dev", dev_base).await;
            seed_ref(&stores, &repo_id, "review/merge", merge_head).await;
            seed_ref(&stores, &repo_id, "review/merge2", merge_head).await;
            seed_ref(&stores, &repo_id, "review/reject", reject_head).await;

            let (workspaces, workspace_id, raw_secret) =
                durable_workspace_bearer_store(&repo_id, ROOT_UID, vec![crate::auth::WHEEL_GID]);
            let (base_url, server) = spawn_test_router(durable_review_router(
                stores.clone(),
                workspaces.clone(),
                repo_id.clone(),
            ))
            .await;
            let client = reqwest::Client::new();
            let headers = durable_headers(&raw_secret, workspace_id, &repo_id);

            let user_root_rejected = client
                .get(format!("{base_url}/protected/refs"))
                .header("authorization", "User root")
                .header("x-stratum-repo", repo_id.as_str())
                .send()
                .await
                .expect("local user root request completes");
            assert_eq!(
                user_root_rejected.status(),
                reqwest::StatusCode::UNAUTHORIZED
            );

            let created_ref = client
                .post(format!("{base_url}/protected/refs"))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "ref_name": "main",
                    "required_approvals": 1,
                }))
                .send()
                .await
                .expect("protected ref create completes");
            assert_eq!(created_ref.status(), reqwest::StatusCode::CREATED);
            let listed_refs = client
                .get(format!("{base_url}/protected/refs"))
                .headers(headers.clone())
                .send()
                .await
                .expect("protected ref list completes");
            assert_eq!(listed_refs.status(), reqwest::StatusCode::OK);
            let listed_refs: serde_json::Value = listed_refs.json().await.unwrap();
            assert_eq!(listed_refs["rules"].as_array().unwrap().len(), 1);

            let created_path = client
                .post(format!("{base_url}/protected/paths"))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "path_prefix": "/legal.txt",
                    "target_ref": "main",
                    "required_approvals": 1,
                }))
                .send()
                .await
                .expect("protected path create completes");
            assert_eq!(created_path.status(), reqwest::StatusCode::CREATED);
            let listed_paths = client
                .get(format!("{base_url}/protected/paths"))
                .headers(headers.clone())
                .send()
                .await
                .expect("protected path list completes");
            assert_eq!(listed_paths.status(), reqwest::StatusCode::OK);
            let listed_paths: serde_json::Value = listed_paths.json().await.unwrap();
            assert_eq!(listed_paths["rules"].as_array().unwrap().len(), 1);

            let reject_change = create_change(
                &client,
                &base_url,
                headers.clone(),
                "review/reject",
                "archive/dev",
            )
            .await;
            let reject_change_id = id_from_change_response(&reject_change);
            let assignment_change_id = stores
                .review
                .create_change_request_for_repo(
                    &repo_id,
                    NewChangeRequest {
                        title: "Durable reviewer assignment".to_string(),
                        description: None,
                        source_ref: "review/reject".to_string(),
                        target_ref: "archive/dev".to_string(),
                        base_commit: dev_base.to_hex(),
                        head_commit: reject_head.to_hex(),
                        created_by: 12345,
                    },
                )
                .await
                .unwrap()
                .id;
            let assigned = client
                .post(format!(
                    "{base_url}/change-requests/{assignment_change_id}/reviewers"
                ))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "reviewer_uid": ROOT_UID,
                    "required": true,
                }))
                .send()
                .await
                .expect("reviewer assign completes");
            let assigned_status = assigned.status();
            let assigned_body = assigned.text().await.unwrap();
            assert_eq!(
                assigned_status,
                reqwest::StatusCode::CREATED,
                "{assigned_body}"
            );

            let commented = client
                .post(format!(
                    "{base_url}/change-requests/{reject_change_id}/comments"
                ))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "body": "looks good to reject",
                    "path": "/notes.txt",
                    "kind": "general",
                }))
                .send()
                .await
                .expect("comment create completes");
            assert_eq!(commented.status(), reqwest::StatusCode::CREATED);

            let approved = client
                .post(format!(
                    "{base_url}/change-requests/{assignment_change_id}/approvals"
                ))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "comment": "approved for dismissal path",
                }))
                .send()
                .await
                .expect("approval create completes");
            assert_eq!(approved.status(), reqwest::StatusCode::CREATED);
            let approved: serde_json::Value = approved.json().await.unwrap();
            let approval_id = approved["approval"]["id"].as_str().unwrap();

            let dismissed = client
                .post(format!(
                    "{base_url}/change-requests/{assignment_change_id}/approvals/{approval_id}/dismiss"
                ))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "reason": "stale review",
                }))
                .send()
                .await
                .expect("approval dismiss completes");
            assert_eq!(dismissed.status(), reqwest::StatusCode::OK);

            let rejected = client
                .post(format!(
                    "{base_url}/change-requests/{reject_change_id}/reject"
                ))
                .headers(headers.clone())
                .send()
                .await
                .expect("change reject completes");
            assert_eq!(rejected.status(), reqwest::StatusCode::OK);
            let rejected: serde_json::Value = rejected.json().await.unwrap();
            assert_eq!(rejected["change_request"]["status"], "rejected");

            let mut create_merge_headers = headers.clone();
            create_merge_headers
                .insert("idempotency-key", "durable-review-create".parse().unwrap());
            let _merge_change = create_change(
                &client,
                &base_url,
                create_merge_headers.clone(),
                "review/merge",
                "main",
            )
            .await;
            let replay = client
                .post(format!("{base_url}/change-requests"))
                .headers(create_merge_headers)
                .json(&serde_json::json!({
                    "title": "Durable cloud review",
                    "description": "metadata only",
                    "source_ref": "review/merge",
                    "target_ref": "main",
                }))
                .send()
                .await
                .expect("change request replay completes");
            assert_eq!(replay.status(), reqwest::StatusCode::CREATED);
            assert_eq!(
                replay
                    .headers()
                    .get("x-stratum-idempotent-replay")
                    .and_then(|value| value.to_str().ok()),
                Some("true")
            );
            let replay_body: serde_json::Value = replay.json().await.unwrap();
            assert_eq!(
                replay_body["change_request"]["title"],
                serde_json::Value::Null
            );
            assert_eq!(
                replay_body["change_request"]["description"],
                serde_json::Value::Null
            );
            let rendered_replay = serde_json::to_string(&replay_body).unwrap();
            assert!(!rendered_replay.contains("Durable cloud review"));
            assert!(!rendered_replay.contains("metadata only"));
            let merge_change_id = stores
                .review
                .create_change_request_for_repo(
                    &repo_id,
                    NewChangeRequest {
                        title: "Durable protected merge".to_string(),
                        description: None,
                        source_ref: "review/merge2".to_string(),
                        target_ref: "main".to_string(),
                        base_commit: base.to_hex(),
                        head_commit: merge_head.to_hex(),
                        created_by: 12345,
                    },
                )
                .await
                .unwrap()
                .id;

            let blocked_merge = client
                .post(format!(
                    "{base_url}/change-requests/{merge_change_id}/merge"
                ))
                .headers(headers.clone())
                .send()
                .await
                .expect("blocked merge completes");
            assert_eq!(blocked_merge.status(), reqwest::StatusCode::FORBIDDEN);

            let approved_merge = client
                .post(format!(
                    "{base_url}/change-requests/{merge_change_id}/approvals"
                ))
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "comment": null,
                }))
                .send()
                .await
                .expect("merge approval completes");
            assert_eq!(approved_merge.status(), reqwest::StatusCode::CREATED);

            let merged = client
                .post(format!(
                    "{base_url}/change-requests/{merge_change_id}/merge"
                ))
                .headers(headers.clone())
                .send()
                .await
                .expect("merge completes");
            assert_eq!(merged.status(), reqwest::StatusCode::OK);
            let merged: serde_json::Value = merged.json().await.unwrap();
            assert_eq!(merged["change_request"]["status"], "merged");
            assert_eq!(merged["target_ref"]["target"], merge_head.to_hex());
            assert!(
                stores
                    .refs
                    .get(&repo_id, &RefName::new("main").unwrap())
                    .await
                    .unwrap()
                    .is_some_and(|main| main.target == merge_head)
            );

            server.abort();
        }

        #[tokio::test]
        async fn review_routes_persist_after_rebuild_and_report_stale_merge_conflict() {
            let repo_id = RepoId::new("repo_durable_cloud_review_rebuild").unwrap();
            let stores = StratumStores::local_memory();
            let base = durable_commit_id("durable-cloud-rebuild-base");
            let head = durable_commit_id("durable-cloud-rebuild-head");
            let stale = durable_commit_id("durable-cloud-rebuild-stale");
            seed_commit(&stores, &repo_id, base, Vec::new(), "/legal.txt").await;
            seed_commit(&stores, &repo_id, head, vec![base], "/legal.txt").await;
            seed_commit(&stores, &repo_id, stale, vec![base], "/legal.txt").await;
            seed_ref(&stores, &repo_id, "main", base).await;
            seed_ref(&stores, &repo_id, "review/rebuild", head).await;

            let (workspaces, workspace_id, raw_secret) =
                durable_workspace_bearer_store(&repo_id, ROOT_UID, vec![crate::auth::WHEEL_GID]);
            let (base_url, server) = spawn_test_router(durable_review_router(
                stores.clone(),
                workspaces.clone(),
                repo_id.clone(),
            ))
            .await;
            let client = reqwest::Client::new();
            let headers = durable_headers(&raw_secret, workspace_id, &repo_id);
            let change = create_change(
                &client,
                &base_url,
                headers.clone(),
                "review/rebuild",
                "main",
            )
            .await;
            let change_id = id_from_change_response(&change);
            server.abort();

            let (rebuilt_url, rebuilt_server) = spawn_test_router(durable_review_router(
                stores.clone(),
                workspaces,
                repo_id.clone(),
            ))
            .await;
            let listed = client
                .get(format!("{rebuilt_url}/change-requests"))
                .headers(headers.clone())
                .send()
                .await
                .expect("rebuilt list completes");
            assert_eq!(listed.status(), reqwest::StatusCode::OK);
            let listed: serde_json::Value = listed.json().await.unwrap();
            assert_eq!(
                listed["change_requests"][0]["change_request"]["id"],
                serde_json::json!(change_id.to_string())
            );

            let current_main = stores
                .refs
                .get(&repo_id, &RefName::new("main").unwrap())
                .await
                .unwrap()
                .unwrap();
            stores
                .refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: RefName::new("main").unwrap(),
                    target: stale,
                    expectation: RefExpectation::Matches {
                        target: current_main.target,
                        version: current_main.version,
                    },
                })
                .await
                .unwrap();
            let stale_merge = client
                .post(format!("{rebuilt_url}/change-requests/{change_id}/merge"))
                .headers(headers)
                .send()
                .await
                .expect("stale merge completes");
            assert_eq!(stale_merge.status(), reqwest::StatusCode::CONFLICT);
            let stale_merge: serde_json::Value = stale_merge.json().await.unwrap();
            assert_eq!(
                stale_merge["error"],
                format!("change request {change_id} target ref is stale")
            );
            let rendered_stale = serde_json::to_string(&stale_merge).unwrap();
            assert!(!rendered_stale.contains(repo_id.as_str()));
            assert!(!rendered_stale.contains(&stale.to_hex()));

            rebuilt_server.abort();
        }
    }

    #[derive(Default)]
    struct FailingMutationAuditStore {
        inner: InMemoryAuditStore,
    }

    #[async_trait::async_trait]
    impl AuditStore for FailingMutationAuditStore {
        async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
            if matches!(
                event.action,
                AuditAction::PolicyDecisionAllow | AuditAction::PolicyDecisionDeny
            ) {
                return self.inner.append(event).await;
            }
            Err(VfsError::CorruptStore {
                message: "audit append failed with private-store-detail".to_string(),
            })
        }

        async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
            self.inner.list_recent(limit).await
        }

        async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError> {
            self.inner.contains_vcs_commit_event(commit_id).await
        }

        async fn contains_fs_mutation_recovery_event(
            &self,
            action: AuditAction,
            operation_id: &str,
            target_ref: &str,
            new_commit: &str,
        ) -> Result<bool, VfsError> {
            self.inner
                .contains_fs_mutation_recovery_event(action, operation_id, target_ref, new_commit)
                .await
        }
    }

    #[derive(Default)]
    struct FailingCompleteIdempotencyStore {
        inner: InMemoryIdempotencyStore,
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
                message:
                    "idempotency begin failed with postgres://secret@metadata.example/private-key"
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

    #[async_trait::async_trait]
    impl IdempotencyStore for FailingCompleteIdempotencyStore {
        async fn begin(
            &self,
            scope: &str,
            key: &IdempotencyKey,
            request_fingerprint: &str,
        ) -> Result<IdempotencyBegin, VfsError> {
            self.inner.begin(scope, key, request_fingerprint).await
        }

        async fn complete(
            &self,
            _reservation: &IdempotencyReservation,
            _status_code: u16,
            _response_body: serde_json::Value,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "idempotency completion failed with private-store-detail".to_string(),
            })
        }

        async fn complete_with_classification(
            &self,
            reservation: &IdempotencyReservation,
            status_code: u16,
            response_body: serde_json::Value,
            _classification: IdempotencyReplayClassification,
        ) -> Result<(), VfsError> {
            self.complete(reservation, status_code, response_body).await
        }

        async fn abort(&self, reservation: &IdempotencyReservation) {
            self.inner.abort(reservation).await;
        }
    }

    struct FailingApprovalDecisionReviewStore {
        inner: InMemoryReviewStore,
    }

    #[async_trait::async_trait]
    impl ReviewStore for FailingApprovalDecisionReviewStore {
        async fn create_protected_ref_rule_for_repo(
            &self,
            repo_id: &RepoId,
            ref_name: &str,
            required_approvals: u32,
            created_by: Uid,
            require_all_files_viewed: bool,
        ) -> Result<ProtectedRefRule, VfsError> {
            self.inner
                .create_protected_ref_rule_for_repo(
                    repo_id,
                    ref_name,
                    required_approvals,
                    created_by,
                    require_all_files_viewed,
                )
                .await
        }

        async fn list_protected_ref_rules_for_repo(
            &self,
            repo_id: &RepoId,
        ) -> Result<Vec<ProtectedRefRule>, VfsError> {
            self.inner.list_protected_ref_rules_for_repo(repo_id).await
        }

        async fn get_protected_ref_rule_for_repo(
            &self,
            repo_id: &RepoId,
            id: Uuid,
        ) -> Result<Option<ProtectedRefRule>, VfsError> {
            self.inner
                .get_protected_ref_rule_for_repo(repo_id, id)
                .await
        }

        async fn create_protected_path_rule_for_repo(
            &self,
            repo_id: &RepoId,
            path_prefix: &str,
            target_ref: Option<&str>,
            required_approvals: u32,
            created_by: Uid,
            require_all_files_viewed: bool,
        ) -> Result<ProtectedPathRule, VfsError> {
            self.inner
                .create_protected_path_rule_for_repo(
                    repo_id,
                    path_prefix,
                    target_ref,
                    required_approvals,
                    created_by,
                    require_all_files_viewed,
                )
                .await
        }

        async fn list_protected_path_rules_for_repo(
            &self,
            repo_id: &RepoId,
        ) -> Result<Vec<ProtectedPathRule>, VfsError> {
            self.inner.list_protected_path_rules_for_repo(repo_id).await
        }

        async fn get_protected_path_rule_for_repo(
            &self,
            repo_id: &RepoId,
            id: Uuid,
        ) -> Result<Option<ProtectedPathRule>, VfsError> {
            self.inner
                .get_protected_path_rule_for_repo(repo_id, id)
                .await
        }

        async fn create_change_request_for_repo(
            &self,
            repo_id: &RepoId,
            input: NewChangeRequest,
        ) -> Result<ChangeRequest, VfsError> {
            self.inner
                .create_change_request_for_repo(repo_id, input)
                .await
        }

        async fn list_change_requests_for_repo(
            &self,
            repo_id: &RepoId,
        ) -> Result<Vec<ChangeRequest>, VfsError> {
            self.inner.list_change_requests_for_repo(repo_id).await
        }

        async fn get_change_request_for_repo(
            &self,
            repo_id: &RepoId,
            id: Uuid,
        ) -> Result<Option<ChangeRequest>, VfsError> {
            self.inner.get_change_request_for_repo(repo_id, id).await
        }

        async fn transition_change_request_for_repo(
            &self,
            repo_id: &RepoId,
            id: Uuid,
            status: ChangeRequestStatus,
        ) -> Result<Option<ChangeRequest>, VfsError> {
            self.inner
                .transition_change_request_for_repo(repo_id, id, status)
                .await
        }

        async fn create_approval_for_repo(
            &self,
            repo_id: &RepoId,
            input: NewApprovalRecord,
        ) -> Result<ApprovalRecordMutation, VfsError> {
            self.inner.create_approval_for_repo(repo_id, input).await
        }

        async fn list_approvals_for_repo(
            &self,
            repo_id: &RepoId,
            change_request_id: Uuid,
        ) -> Result<Vec<ApprovalRecord>, VfsError> {
            self.inner
                .list_approvals_for_repo(repo_id, change_request_id)
                .await
        }

        async fn assign_reviewer_for_repo(
            &self,
            repo_id: &RepoId,
            input: NewReviewAssignment,
        ) -> Result<ReviewAssignmentMutation, VfsError> {
            self.inner.assign_reviewer_for_repo(repo_id, input).await
        }

        async fn list_reviewer_assignments_for_repo(
            &self,
            repo_id: &RepoId,
            change_request_id: Uuid,
        ) -> Result<Vec<ReviewAssignment>, VfsError> {
            self.inner
                .list_reviewer_assignments_for_repo(repo_id, change_request_id)
                .await
        }

        async fn create_comment_for_repo(
            &self,
            repo_id: &RepoId,
            input: NewReviewComment,
        ) -> Result<ReviewCommentMutation, VfsError> {
            self.inner.create_comment_for_repo(repo_id, input).await
        }

        async fn list_comments_for_repo(
            &self,
            repo_id: &RepoId,
            change_request_id: Uuid,
        ) -> Result<Vec<ReviewComment>, VfsError> {
            self.inner
                .list_comments_for_repo(repo_id, change_request_id)
                .await
        }

        async fn dismiss_approval_for_repo(
            &self,
            repo_id: &RepoId,
            input: DismissApprovalInput,
        ) -> Result<ApprovalDismissalMutation, VfsError> {
            self.inner.dismiss_approval_for_repo(repo_id, input).await
        }

        async fn approval_decision_for_repo(
            &self,
            _repo_id: &RepoId,
            _change_request_id: Uuid,
            _changed_paths: &[String],
        ) -> Result<Option<ApprovalPolicyDecision>, VfsError> {
            Err(VfsError::CorruptStore {
                message:
                    "approval failed with postgres://secret@metadata.example/private-store-detail"
                        .to_string(),
            })
        }
    }

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
        headers
    }

    fn user_headers_for_repo(username: &str, repo_id: &RepoId) -> HeaderMap {
        let mut headers = user_headers(username);
        headers.insert("x-stratum-repo", repo_id.as_str().parse().unwrap());
        headers
    }

    fn user_headers_with_idempotency(username: &str, key: &str) -> HeaderMap {
        let mut headers = user_headers(username);
        headers.insert("idempotency-key", key.parse().unwrap());
        headers
    }

    fn user_headers_for_repo_with_idempotency(
        username: &str,
        repo_id: &RepoId,
        key: &str,
    ) -> HeaderMap {
        let mut headers = user_headers_for_repo(username, repo_id);
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

    async fn spawn_test_router(router: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test router");
        let addr = listener.local_addr().expect("test listener has address");
        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve test router");
        });
        (format!("http://{addr}"), handle)
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

    fn durable_commit_id(label: &str) -> CommitId {
        CommitId::from(ObjectId::from_bytes(label.as_bytes()))
    }

    fn durable_commit_record(
        repo_id: &RepoId,
        id: CommitId,
        parents: Vec<CommitId>,
        changed_paths: Vec<ChangedPath>,
    ) -> CommitRecord {
        CommitRecord {
            repo_id: repo_id.clone(),
            id,
            root_tree: ObjectId::from_bytes(format!("tree-{id}").as_bytes()),
            parents,
            timestamp: 1,
            message: "metadata only".to_string(),
            author: "root".to_string(),
            changed_paths,
        }
    }

    async fn durable_review_fixture() -> (AppState, StratumStores, String, String) {
        let db = StratumDb::open_memory();
        let repo_id = RepoId::new("review-test").unwrap();
        let stores = StratumStores::local_memory();
        let base = durable_commit_id("durable-review-base");
        let head = durable_commit_id("durable-review-head");
        stores
            .commits
            .insert(durable_commit_record(
                &repo_id,
                base,
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        stores
            .commits
            .insert(durable_commit_record(
                &repo_id,
                head,
                vec![base],
                vec![ChangedPath {
                    path: "/legal.txt".to_string(),
                    kind: ChangeKind::Modified,
                    before: None,
                    after: None,
                }],
            ))
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("main").unwrap(),
                target: base,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("review/cr-1").unwrap(),
                target: head,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

        let state = test_state_with_durable_review(db, repo_id, stores.clone());
        (state, stores, base.to_hex(), head.to_hex())
    }

    async fn create_durable_change(state: &AppState, base: &str, head: &str) -> Uuid {
        let repo_id = RepoId::new("review-test").unwrap();
        state
            .review
            .create_change_request_for_repo(
                &repo_id,
                NewChangeRequest {
                    title: "Durable update".to_string(),
                    description: Some("metadata only".to_string()),
                    source_ref: "review/cr-1".to_string(),
                    target_ref: "main".to_string(),
                    base_commit: base.to_string(),
                    head_commit: head.to_string(),
                    created_by: ROOT_UID,
                },
            )
            .await
            .unwrap()
            .id
    }

    async fn review_fixture_with_services(
        audit: crate::audit::SharedAuditStore,
        idempotency: crate::idempotency::SharedIdempotencyStore,
    ) -> (AppState, String, String, Uuid) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let head = commit_file(&db, &mut root, "/legal.txt", "head", "head").await;
        db.create_ref("review/cr-1", &head).await.unwrap();
        let main = db.get_ref("main").await.unwrap().unwrap();
        db.update_ref("main", &main.target, main.version, &base)
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency,
            audit,
            review: Arc::new(InMemoryReviewStore::new()),
        });
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

    fn assert_review_mutation_audit_context(
        event: &AuditEvent,
        route: &str,
        change_request_id: Uuid,
        base_commit: &str,
        head_commit: &str,
        idempotency_present: bool,
    ) {
        let expected_change_request_id = change_request_id.to_string();
        assert_eq!(event.details.get("route").map(String::as_str), Some(route));
        assert_eq!(
            event.details.get("change_request_id").map(String::as_str),
            Some(expected_change_request_id.as_str())
        );
        assert_eq!(
            event.details.get("source_ref").map(String::as_str),
            Some("review/cr-1")
        );
        assert_eq!(
            event.details.get("target_ref").map(String::as_str),
            Some("main")
        );
        assert_eq!(
            event.details.get("base_commit").map(String::as_str),
            Some(base_commit)
        );
        assert_eq!(
            event.details.get("head_commit").map(String::as_str),
            Some(head_commit)
        );

        if idempotency_present {
            assert_eq!(
                event.details.get("idempotency_present").map(String::as_str),
                Some("true")
            );
        } else {
            assert!(!event.details.contains_key("idempotency_present"));
        }
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

    async fn add_regular_user(state: &AppState, username: &str) {
        let mut root = Session::root();
        state
            .db
            .execute_command(&format!("adduser {username}"), &mut root)
            .await
            .unwrap();
    }

    async fn remove_admin_group(state: &AppState, username: &str) {
        let mut root = Session::root();
        state
            .db
            .execute_command(&format!("usermod -rG wheel {username}"), &mut root)
            .await
            .unwrap();
    }

    async fn delete_user(state: &AppState, username: &str) {
        let mut root = Session::root();
        state
            .db
            .execute_command(&format!("deluser {username}"), &mut root)
            .await
            .unwrap();
    }

    async fn uid_for_user(state: &AppState, username: &str) -> Uid {
        state.db.login(username).await.unwrap().uid
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
                require_all_files_viewed: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(created_ref.status(), StatusCode::CREATED);
        let created_ref = response_json(created_ref).await;
        assert_eq!(created_ref["ref_name"], "main");
        assert_eq!(created_ref["required_approvals"], 1);
        assert_eq!(created_ref["require_all_files_viewed"], true);
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
                .any(|rule| rule["id"] == ref_rule_id && rule["require_all_files_viewed"] == true)
        );

        let created_path = create_protected_path(
            State(state.clone()),
            user_headers("root"),
            Json(CreateProtectedPathRequest {
                path_prefix: "/legal".to_string(),
                target_ref: Some("main".to_string()),
                required_approvals: 2,
                require_all_files_viewed: Some(false),
            }),
        )
        .await
        .into_response();
        assert_eq!(created_path.status(), StatusCode::CREATED);
        let created_path = response_json(created_path).await;
        assert_eq!(created_path["path_prefix"], "/legal");
        assert_eq!(created_path["target_ref"], "main");
        assert_eq!(created_path["required_approvals"], 2);
        assert_eq!(created_path["require_all_files_viewed"], false);
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
                .any(|rule| rule["id"] == path_rule_id
                    && rule["require_all_files_viewed"] == false)
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
    async fn protected_rule_file_view_flag_defaults_true_and_round_trips() {
        let state = test_state(StratumDb::open_memory());

        let created_ref = create_protected_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
                require_all_files_viewed: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(created_ref.status(), StatusCode::CREATED);
        let created_ref = response_json(created_ref).await;
        assert_eq!(created_ref["require_all_files_viewed"], true);

        let created_path = create_protected_path(
            State(state.clone()),
            user_headers("root"),
            Json(CreateProtectedPathRequest {
                path_prefix: "/legal".to_string(),
                target_ref: Some("main".to_string()),
                required_approvals: 1,
                require_all_files_viewed: Some(false),
            }),
        )
        .await
        .into_response();
        assert_eq!(created_path.status(), StatusCode::CREATED);
        let created_path = response_json(created_path).await;
        assert_eq!(created_path["require_all_files_viewed"], false);

        let listed_paths = list_protected_paths(State(state), user_headers("root"))
            .await
            .into_response();
        assert_eq!(listed_paths.status(), StatusCode::OK);
        let listed_paths = response_json(listed_paths).await;
        assert!(
            listed_paths["rules"]
                .as_array()
                .unwrap()
                .iter()
                .any(|rule| rule["path_prefix"] == "/legal"
                    && rule["require_all_files_viewed"] == false)
        );
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
                require_all_files_viewed: None,
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
    async fn approval_state_backend_error_response_is_redacted() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let head = commit_file(&db, &mut root, "/legal.txt", "head", "head").await;
        let review = FailingApprovalDecisionReviewStore {
            inner: InMemoryReviewStore::new(),
        };
        let change = review
            .inner
            .create_change_request(NewChangeRequest {
                title: "Backend detail check".to_string(),
                description: None,
                source_ref: "review/cr-1".to_string(),
                target_ref: "main".to_string(),
                base_commit: base,
                head_commit: head,
                created_by: ROOT_UID,
            })
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(review),
        });

        let response = get_change_request(State(state), user_headers("root"), AxumPath(change.id))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["approval_state"]["available"], false);
        assert_eq!(
            body["approval_state"]["error"],
            APPROVAL_STATE_UNAVAILABLE_ERROR
        );
        assert_eq!(body["require_all_files_viewed"], true);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("postgres://secret"));
        assert!(!rendered.contains("metadata.example"));
        assert!(!rendered.contains("private-store-detail"));
    }

    #[tokio::test]
    async fn review_idempotency_begin_backend_failure_response_is_redacted() {
        let db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(FailingBeginIdempotencyStore),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        });

        let response = create_protected_ref(
            State(state),
            user_headers_with_idempotency("root", "review-begin-private"),
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
                require_all_files_viewed: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], "internal server error");
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("postgres://secret"));
        assert!(!rendered.contains("metadata.example"));
        assert!(!rendered.contains("private-key"));
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
        let headers = user_headers_with_idempotency("root", "change-request-create-redaction");

        let response = create_change_request(
            State(state.clone()),
            headers.clone(),
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

        let key = IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap())
            .expect("idempotency key");
        let session = require_admin(&state, &headers).await.expect("session");
        let repo = resolve_review_repo_context(&state, &headers, &session).expect("repo");
        let scope = CREATE_CHANGE_REQUEST_ROUTE.to_string();
        let fingerprint = request_fingerprint(
            &scope,
            &review_fingerprint_body(
                serde_json::json!({
                    "route": CREATE_CHANGE_REQUEST_ROUTE,
                    "actor": actor_fingerprint(&session),
                    "repo_id": repo.repo_id().as_str(),
                    "title": "Legal update",
                    "description": "body must stay out of audit",
                    "source_ref": "review/cr-1",
                    "target_ref": "main",
                }),
                &repo,
            ),
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
                    IdempotencyReplayClassification::Partial
                );
                assert_eq!(
                    record.response_body["change_request"]["title"],
                    serde_json::Value::Null
                );
                assert_eq!(
                    record.response_body["change_request"]["description"],
                    serde_json::Value::Null
                );
                let rendered = serde_json::to_string(&record.response_body).unwrap();
                assert!(!rendered.contains("Legal update"));
                assert!(!rendered.contains("body must stay out of audit"));
            }
            other => panic!("expected change request replay record, got {other:?}"),
        }

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
    async fn create_change_request_uses_durable_refs_without_local_vcs_state() {
        let (state, _stores, base, head) = durable_review_fixture().await;
        let repo_id = RepoId::new("review-test").unwrap();

        let response = create_change_request(
            State(state.clone()),
            user_headers_for_repo("root", &repo_id),
            Json(CreateChangeRequestRequest {
                title: " Durable update ".to_string(),
                description: Some("body must stay out of audit".to_string()),
                source_ref: "review/cr-1".to_string(),
                target_ref: "main".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response_json(response).await;
        assert_eq!(body["change_request"]["title"], "Durable update");
        assert_eq!(body["change_request"]["base_commit"], base);
        assert_eq!(body["change_request"]["head_commit"], head);
        assert_eq!(body["approval_state"]["approved"], true);
        assert!(state.db.get_ref("main").await.unwrap().is_none());
        assert!(state.db.get_ref("review/cr-1").await.unwrap().is_none());
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("body must stay out of audit"));
    }

    #[tokio::test]
    async fn approval_state_for_local_change_in_durable_mode_fails_closed_without_durable_refs() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let head = commit_file(&db, &mut root, "/legal.txt", "head", "head").await;
        let state = test_state_with_durable_review(
            db,
            RepoId::new("empty-review-stores").unwrap(),
            StratumStores::local_memory(),
        );
        let change = state
            .review
            .create_change_request(NewChangeRequest {
                title: "Local update".to_string(),
                description: None,
                source_ref: "review/missing-local-ref".to_string(),
                target_ref: "archive/missing-target-ref".to_string(),
                base_commit: base,
                head_commit: head,
                created_by: ROOT_UID,
            })
            .await
            .unwrap();

        let response = get_change_request(
            State(state.clone()),
            user_headers_for_repo("root", &RepoId::local()),
            AxumPath(change.id),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["approval_state"]["available"], false);
        assert!(body["approval_state"]["error"].as_str().is_some());
    }

    #[tokio::test]
    async fn approval_state_for_local_change_in_durable_mode_fails_closed_on_partial_durable_refs()
    {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let base = commit_file(&db, &mut root, "/legal.txt", "base", "base").await;
        let head = commit_file(&db, &mut root, "/legal.txt", "head", "head").await;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::new("partial-review-stores").unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("review/missing-local-ref").unwrap(),
                target: durable_commit_id("unrelated-durable-source"),
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        let state = test_state_with_durable_review(db, repo_id, stores);
        state
            .review
            .create_protected_path_rule("/legal.txt", None, 1, ROOT_UID)
            .await
            .unwrap();
        let change = state
            .review
            .create_change_request(NewChangeRequest {
                title: "Local update".to_string(),
                description: None,
                source_ref: "review/missing-local-ref".to_string(),
                target_ref: "archive/missing-target-ref".to_string(),
                base_commit: base,
                head_commit: head,
                created_by: ROOT_UID,
            })
            .await
            .unwrap();

        let response = get_change_request(
            State(state.clone()),
            user_headers_for_repo("root", &RepoId::local()),
            AxumPath(change.id),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["approval_state"]["available"], false);
        assert!(body["approval_state"]["error"].as_str().is_some());
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
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::ChangeRequestReject)
                .count(),
            1
        );
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
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::ChangeRequestMerge)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn merge_change_request_uses_durable_refs_and_commits_without_local_vcs_state() {
        let (state, stores, base, head) = durable_review_fixture().await;
        let id = create_durable_change(&state, &base, &head).await;
        let repo_id = RepoId::new("review-test").unwrap();
        let headers =
            user_headers_for_repo_with_idempotency("root", &repo_id, "durable-merge-replay");

        let merged = merge_change_request(State(state.clone()), headers.clone(), AxumPath(id))
            .await
            .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        let body = response_json(merged).await;
        assert_eq!(body["change_request"]["status"], "merged");
        assert_eq!(body["target_ref"]["name"], "main");
        assert_eq!(body["target_ref"]["target"], head);
        assert!(state.db.get_ref("main").await.unwrap().is_none());
        let durable_main = stores
            .refs
            .get(
                &RepoId::new("review-test").unwrap(),
                &RefName::new("main").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable_main.target.to_hex(), head);

        let replay = merge_change_request(State(state.clone()), headers, AxumPath(id))
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

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::ChangeRequestMerge)
                .count(),
            1
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("durable-merge-replay"));
        assert!(!audit_json.contains("metadata only"));
    }

    #[tokio::test]
    async fn durable_review_target_ref_update_requires_review_merge_policy_token() {
        let (state, stores, base, head) = durable_review_fixture().await;
        let repo_id = RepoId::new("review-test").unwrap();
        let id = create_durable_change(&state, &base, &head).await;
        let change = state
            .review
            .get_change_request_for_repo(&repo_id, id)
            .await
            .unwrap()
            .unwrap();
        let refs = review_ref_pair_for_change(&state, &change).await.unwrap();

        let error = update_review_target_ref(&state, &change, refs, &[], None)
            .await
            .expect_err("durable review target update must fail without policy token");

        let VfsError::PermissionDenied { path } = error else {
            panic!("missing review merge policy token should return PermissionDenied");
        };
        assert_eq!(path, "policy decision token");
        let durable_main = stores
            .refs
            .get(&repo_id, &RefName::new("main").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable_main.target.to_hex(), base);
        assert_eq!(
            state
                .review
                .get_change_request_for_repo(&repo_id, id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ChangeRequestStatus::Open
        );
    }

    #[tokio::test]
    async fn durable_review_target_ref_update_rejects_policy_token_for_different_change() {
        let (state, stores, base, head) = durable_review_fixture().await;
        let repo_id = RepoId::new("review-test").unwrap();
        let id = create_durable_change(&state, &base, &head).await;
        let change = state
            .review
            .get_change_request_for_repo(&repo_id, id)
            .await
            .unwrap()
            .unwrap();
        let refs = review_ref_pair_for_change(&state, &change).await.unwrap();
        let wrong_change_id = Uuid::new_v4();
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::ReviewMerge, &Session::root())
                .with_repo_id(repo_id.clone())
                .with_target_ref(&change.target_ref)
                .with_changed_paths(["/reviewed.txt"]);
        let request = RoutePolicyRequest {
            review_approval: Some(RoutePolicyReviewApproval {
                approved: true,
                change_request_id: wrong_change_id,
                matched_ref_rule_count: 0,
                matched_path_rule_count: 0,
            }),
            ..request
        };
        let evaluation = policy::evaluate_route_policy(state.review.as_ref(), request)
            .await
            .unwrap();
        let token = PolicyDecisionToken::from_review_approved_evaluation(&evaluation).unwrap();
        let error = update_review_target_ref(
            &state,
            &change,
            refs,
            &["/reviewed.txt".to_string()],
            Some(&token),
        )
        .await
        .expect_err("durable review target update must bind token to the change request");

        assert!(matches!(error, VfsError::PermissionDenied { .. }));
        let durable_main = stores
            .refs
            .get(&repo_id, &RefName::new("main").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable_main.target.to_hex(), base);
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
        let change = source_state
            .review
            .get_change_request(source_stale_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(change.status, ChangeRequestStatus::Open);

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
    async fn merge_change_request_conflicts_when_durable_source_ref_is_stale() {
        let (state, stores, base, head) = durable_review_fixture().await;
        let repo_id = RepoId::new("review-test").unwrap();
        let id = create_durable_change(&state, &base, &head).await;
        let source = stores
            .refs
            .get(&repo_id, &RefName::new("review/cr-1").unwrap())
            .await
            .unwrap()
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("review/cr-1").unwrap(),
                target: durable_commit_id("durable-review-base"),
                expectation: RefExpectation::Matches {
                    target: source.target,
                    version: source.version,
                },
            })
            .await
            .unwrap();

        let stale = merge_change_request(
            State(state.clone()),
            user_headers_for_repo("root", &repo_id),
            AxumPath(id),
        )
        .await
        .into_response();

        assert_eq!(stale.status(), StatusCode::CONFLICT);
        let body = response_json(stale).await;
        assert_eq!(
            body["error"],
            format!("change request {id} source ref is stale")
        );
        let target = stores
            .refs
            .get(&repo_id, &RefName::new("main").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.target.to_hex(), base);
        let change = state
            .review
            .get_change_request_for_repo(&repo_id, id)
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
            require_all_files_viewed: None,
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

        let conflict = create_protected_ref(
            State(state.clone()),
            headers,
            Json(CreateProtectedRefRequest {
                ref_name: "review/cr-1".to_string(),
                required_approvals: 1,
                require_all_files_viewed: None,
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
    async fn protected_ref_create_idempotency_conflicts_when_file_view_flag_changes() {
        let state = test_state(StratumDb::open_memory());
        let headers = user_headers_with_idempotency("root", "protected-ref-file-view-flag");

        let first = create_protected_ref(
            State(state.clone()),
            headers.clone(),
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
                require_all_files_viewed: Some(true),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let conflict = create_protected_ref(
            State(state.clone()),
            headers,
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
                require_all_files_viewed: Some(false),
            }),
        )
        .await
        .into_response();
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn protected_ref_create_idempotency_treats_explicit_true_as_default() {
        let state = test_state(StratumDb::open_memory());
        let headers =
            user_headers_with_idempotency("root", "protected-ref-file-view-default-compat");

        let first = create_protected_ref(
            State(state.clone()),
            headers.clone(),
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
                require_all_files_viewed: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let replay = create_protected_ref(
            State(state.clone()),
            headers,
            Json(CreateProtectedRefRequest {
                ref_name: "main".to_string(),
                required_approvals: 1,
                require_all_files_viewed: Some(true),
            }),
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );
    }

    #[tokio::test]
    async fn approval_create_and_list_records_with_audit_redaction() {
        let (state, base, head, id) = review_fixture().await;
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
        assert_review_mutation_audit_context(
            &events[0],
            CREATE_CHANGE_REQUEST_APPROVAL_ROUTE,
            id,
            &base,
            &head,
            false,
        );
        assert_eq!(
            events[0].details.get("approval_id").map(String::as_str),
            created_body["approval"]["id"].as_str()
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("private approval note"));
        assert!(!audit_json.contains("metadata only"));
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestApprove);
    }

    #[tokio::test]
    async fn approval_audit_failure_response_and_replay_are_redacted() {
        let (state, _base, _head, id) = review_fixture_with_services(
            Arc::new(FailingMutationAuditStore::default()),
            Arc::new(InMemoryIdempotencyStore::new()),
        )
        .await;
        add_admin_user(&state, "alice").await;
        let headers = user_headers_with_idempotency("alice", "approval-audit-redaction");
        let request = || CreateApprovalRequest {
            comment: Some("approval comment must not leak".to_string()),
        };

        let response = create_change_request_approval(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(body["error"], "audit append failed after mutation");
        assert_eq!(body["mutation_committed"], true);
        assert_eq!(body["audit_recorded"], false);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-store-detail"));
        assert!(!rendered.contains("approval comment must not leak"));

        let replay = create_change_request_approval(
            State(state.clone()),
            headers,
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        let replay_body = response_json(replay).await;
        assert_eq!(replay_body, body);
        assert!(
            !serde_json::to_string(&replay_body)
                .unwrap()
                .contains("private-store-detail")
        );
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn approval_idempotency_completion_failure_response_is_redacted() {
        let (state, _base, _head, id) = review_fixture_with_services(
            Arc::new(InMemoryAuditStore::new()),
            Arc::new(FailingCompleteIdempotencyStore::default()),
        )
        .await;
        add_admin_user(&state, "alice").await;

        let response = create_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("alice", "approval-idempotency-redaction"),
            AxumPath(id),
            Json(CreateApprovalRequest {
                comment: Some("approval comment must not leak".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(response).await;
        assert_eq!(
            body["error"],
            "idempotency completion failed after mutation"
        );
        assert_eq!(body["mutation_committed"], true);
        assert_eq!(body["idempotency_recorded"], false);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-store-detail"));
        assert!(!rendered.contains("approval comment must not leak"));

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestApprove);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("approval comment must not leak"));
    }

    #[tokio::test]
    async fn review_assignment_create_and_list_with_audit() {
        let (state, base, head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;

        let created = assign_change_request_reviewer(
            State(state.clone()),
            user_headers_with_idempotency("root", "assign-alice"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(created.status(), StatusCode::CREATED);
        let created_body = response_json(created).await;
        assert_eq!(created_body["created"], true);
        assert_eq!(created_body["updated"], false);
        assert_eq!(
            created_body["assignment"]["change_request_id"],
            id.to_string()
        );
        assert_eq!(created_body["assignment"]["reviewer"], alice_uid);
        assert_eq!(created_body["assignment"]["assigned_by"], ROOT_UID);
        assert_eq!(created_body["assignment"]["required"], true);
        assert_eq!(
            created_body["approval_state"]["required_reviewers"],
            serde_json::json!([alice_uid])
        );
        assert_eq!(
            created_body["approval_state"]["missing_required_reviewers"],
            serde_json::json!([alice_uid])
        );
        assert_eq!(created_body["approval_state"]["approved"], false);

        let listed =
            list_change_request_reviewers(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed_body = response_json(listed).await;
        assert_eq!(listed_body["assignments"].as_array().unwrap().len(), 1);
        assert_eq!(listed_body["assignments"][0], created_body["assignment"]);
        assert_eq!(
            listed_body["approval_state"],
            created_body["approval_state"]
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestReviewerAssign);
        assert_eq!(events[0].resource.kind, AuditResourceKind::ReviewAssignment);
        assert_review_mutation_audit_context(
            &events[0],
            ASSIGN_CHANGE_REQUEST_REVIEWER_ROUTE,
            id,
            &base,
            &head,
            true,
        );
        assert_eq!(
            events[0].details.get("assignment_id").map(String::as_str),
            created_body["assignment"]["id"].as_str()
        );
        assert_eq!(
            events[0].details.get("reviewer").map(String::as_str),
            Some("1")
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("assign-alice"));
        assert!(!audit_json.contains("metadata only"));
    }

    #[tokio::test]
    async fn review_assignment_idempotency_replays_without_second_audit_event() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;
        let headers = user_headers_with_idempotency("root", "assign-replay");
        let request = || AssignReviewerRequest {
            reviewer_uid: alice_uid,
            required: Some(true),
        };

        let first = assign_change_request_reviewer(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let replay = assign_change_request_reviewer(
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::ChangeRequestReviewerAssign);
    }

    #[tokio::test]
    async fn review_assignment_duplicate_and_update_semantics_are_reported() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;

        let first = assign_change_request_reviewer(
            State(state.clone()),
            user_headers_with_idempotency("root", "assign-first"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let duplicate = assign_change_request_reviewer(
            State(state.clone()),
            user_headers_with_idempotency("root", "assign-duplicate"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate_body = response_json(duplicate).await;
        assert_eq!(duplicate_body["created"], false);
        assert_eq!(duplicate_body["updated"], false);
        assert_eq!(
            duplicate_body["assignment"]["id"],
            first_body["assignment"]["id"]
        );
        assert_eq!(
            duplicate_body["assignment"]["version"],
            first_body["assignment"]["version"]
        );

        let optional = assign_change_request_reviewer(
            State(state.clone()),
            user_headers_with_idempotency("root", "assign-optional"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(false),
            }),
        )
        .await
        .into_response();
        assert_eq!(optional.status(), StatusCode::OK);
        let optional_body = response_json(optional).await;
        assert_eq!(optional_body["created"], false);
        assert_eq!(optional_body["updated"], true);
        assert_eq!(
            optional_body["assignment"]["id"],
            first_body["assignment"]["id"]
        );
        assert_eq!(optional_body["assignment"]["required"], false);
        assert_eq!(
            optional_body["assignment"]["version"].as_u64().unwrap(),
            first_body["assignment"]["version"].as_u64().unwrap() + 1
        );
        assert_eq!(
            optional_body["approval_state"]["required_reviewers"],
            serde_json::json!([])
        );
        assert_eq!(optional_body["approval_state"]["approved"], true);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, AuditAction::ChangeRequestReviewerAssign);
        assert_eq!(events[1].action, AuditAction::ChangeRequestReviewerAssign);
        assert_eq!(
            events[0].details.get("created").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            events[1].details.get("updated").map(String::as_str),
            Some("true")
        );
    }

    #[tokio::test]
    async fn review_assignment_unknown_reviewer_does_not_mutate_or_reserve_idempotency() {
        let (state, _base, _head, id) = review_fixture().await;
        let headers = user_headers_with_idempotency("root", "assign-missing-reviewer");
        let request = || AssignReviewerRequest {
            reviewer_uid: 1,
            required: Some(true),
        };

        let missing = assign_change_request_reviewer(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert!(
            state
                .review
                .list_reviewer_assignments(id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());

        add_admin_user(&state, "alice").await;
        assert_eq!(uid_for_user(&state, "alice").await, 1);
        let created = assign_change_request_reviewer(
            State(state.clone()),
            headers,
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(created.status(), StatusCode::CREATED);
        assert!(
            created
                .headers()
                .get("x-stratum-idempotent-replay")
                .is_none()
        );
        assert_eq!(
            state
                .review
                .list_reviewer_assignments(id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn review_assignment_reviewer_must_be_able_to_approve() {
        let (state, _base, _head, id) = review_fixture().await;
        add_regular_user(&state, "viewer").await;
        let viewer_uid = uid_for_user(&state, "viewer").await;

        let response = assign_change_request_reviewer(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: viewer_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            state
                .review
                .list_reviewer_assignments(id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(state.audit.list_recent(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn durable_review_assignment_validates_workspace_principal_without_local_user_db() {
        let repo_id = RepoId::new("review-test").unwrap();
        let stores = StratumStores::local_memory();
        let (workspaces, workspace_id, raw_secret) =
            durable_workspace_bearer_store(&repo_id, 501, vec![crate::auth::WHEEL_GID]);
        let state = Arc::new(ServerState {
            core: Arc::new(crate::server::core::DurableCoreRuntime::new(
                repo_id.clone(),
                stores,
            )),
            db: ServerLocalDb::unavailable(),
            workspaces,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        });
        let change = state
            .review
            .create_change_request_for_repo(
                &repo_id,
                NewChangeRequest {
                    title: "Durable review".to_string(),
                    description: Some("metadata only".to_string()),
                    source_ref: "review/cr-1".to_string(),
                    target_ref: "main".to_string(),
                    base_commit: durable_commit_id("durable-assignment-base").to_hex(),
                    head_commit: durable_commit_id("durable-assignment-head").to_hex(),
                    created_by: ROOT_UID,
                },
            )
            .await
            .unwrap();
        let mut headers = workspace_bearer_headers(&raw_secret, workspace_id);
        headers.insert("x-stratum-repo", repo_id.as_str().parse().unwrap());

        let assigned = assign_change_request_reviewer(
            State(state.clone()),
            headers.clone(),
            AxumPath(change.id),
            Json(AssignReviewerRequest {
                reviewer_uid: 501,
                required: Some(true),
            }),
        )
        .await
        .into_response();

        assert_eq!(assigned.status(), StatusCode::CREATED);
        assert_eq!(
            response_json(assigned).await["assignment"]["reviewer"],
            serde_json::json!(501)
        );

        let rejected = assign_change_request_reviewer(
            State(state.clone()),
            headers,
            AxumPath(change.id),
            Json(AssignReviewerRequest {
                reviewer_uid: 777,
                required: Some(true),
            }),
        )
        .await
        .into_response();

        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            state
                .review
                .list_reviewer_assignments_for_repo(&repo_id, change.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn local_guarded_durable_review_assignment_still_uses_local_user_db() {
        let (state, _stores, base, head) = durable_review_fixture().await;
        let change_id = create_durable_change(&state, &base, &head).await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;

        let assigned = assign_change_request_reviewer(
            State(state.clone()),
            user_headers_for_repo("root", &RepoId::new("review-test").unwrap()),
            AxumPath(change_id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();

        assert_eq!(assigned.status(), StatusCode::CREATED);
        assert_eq!(
            response_json(assigned).await["assignment"]["reviewer"],
            serde_json::json!(alice_uid)
        );
    }

    #[tokio::test]
    async fn review_assignment_downgrades_after_reviewer_loses_rights() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;

        let created = assign_change_request_reviewer(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();
        assert_eq!(created.status(), StatusCode::CREATED);

        remove_admin_group(&state, "alice").await;
        let cleared = assign_change_request_reviewer(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(false),
            }),
        )
        .await
        .into_response();

        assert_eq!(cleared.status(), StatusCode::OK);
        let cleared_body = response_json(cleared).await;
        assert_eq!(cleared_body["created"], false);
        assert_eq!(cleared_body["updated"], true);
        assert_eq!(cleared_body["assignment"]["required"], false);
        assert_eq!(
            cleared_body["approval_state"]["required_reviewers"],
            serde_json::json!([])
        );
    }

    #[tokio::test]
    async fn review_assignment_terminal_change_request_is_rejected() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;
        let rejected =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);

        let response = assign_change_request_reviewer(
            State(state.clone()),
            user_headers_with_idempotency("root", "assign-after-reject"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            state
                .review
                .list_reviewer_assignments(id)
                .await
                .unwrap()
                .is_empty()
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::ChangeRequestReject)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn review_assignment_idempotency_replays_after_reviewer_account_is_deleted() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;
        let headers = user_headers_with_idempotency("root", "assign-before-user-delete");
        let request = || AssignReviewerRequest {
            reviewer_uid: alice_uid,
            required: Some(true),
        };

        let first = assign_change_request_reviewer(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        delete_user(&state, "alice").await;
        let replay = assign_change_request_reviewer(
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );
    }

    #[tokio::test]
    async fn review_assignment_idempotency_replays_after_change_request_becomes_terminal() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let alice_uid = uid_for_user(&state, "alice").await;
        let headers = user_headers_with_idempotency("root", "assign-before-terminal");
        let request = || AssignReviewerRequest {
            reviewer_uid: alice_uid,
            required: Some(true),
        };

        let first = assign_change_request_reviewer(
            State(state.clone()),
            headers.clone(),
            AxumPath(id),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = response_json(first).await;

        let rejected =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);

        let replay = assign_change_request_reviewer(
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::ChangeRequestReviewerAssign)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::ChangeRequestReject)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn review_assignment_required_reviewer_must_approve_for_merge() {
        let (state, _base, head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        add_admin_user(&state, "bob").await;
        let alice_uid = uid_for_user(&state, "alice").await;
        let bob_uid = uid_for_user(&state, "bob").await;
        state
            .review
            .create_protected_ref_rule("main", 1, ROOT_UID)
            .await
            .unwrap();

        let assigned = assign_change_request_reviewer(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: alice_uid,
                required: Some(true),
            }),
        )
        .await
        .into_response();
        assert_eq!(assigned.status(), StatusCode::CREATED);

        let bob_approval = approve_change_request_for(&state, id, "bob").await;
        assert_eq!(bob_approval["approval_state"]["approval_count"], 1);
        assert_eq!(
            bob_approval["approval_state"]["approved_by"],
            serde_json::json!([bob_uid])
        );
        assert_eq!(
            bob_approval["approval_state"]["missing_required_reviewers"],
            serde_json::json!([alice_uid])
        );
        assert_eq!(bob_approval["approval_state"]["approved"], false);

        let blocked =
            merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(blocked.status(), StatusCode::FORBIDDEN);
        let blocked_body = response_json(blocked).await;
        assert_eq!(
            blocked_body["approval_state"]["missing_required_reviewers"],
            serde_json::json!([alice_uid])
        );

        let alice_approval = approve_change_request_for(&state, id, "alice").await;
        assert_eq!(
            alice_approval["approval_state"]["approved_required_reviewers"],
            serde_json::json!([alice_uid])
        );
        assert_eq!(
            alice_approval["approval_state"]["missing_required_reviewers"],
            serde_json::json!([])
        );
        assert_eq!(alice_approval["approval_state"]["approved"], true);

        let merged = merge_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        assert_eq!(response_json(merged).await["target_ref"]["target"], head);
    }

    #[tokio::test]
    async fn review_feedback_comment_create_and_list_with_audit_redaction() {
        let (state, base, head, id) = review_fixture().await;

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
        assert_review_mutation_audit_context(
            &events[0],
            CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
            id,
            &base,
            &head,
            false,
        );
        assert_eq!(
            events[0].details.get("comment_id").map(String::as_str),
            created_body["comment"]["id"].as_str()
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("body must stay out of audit"));
        assert!(!audit_json.contains("metadata only"));
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

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
        let (state, base, head, id) = review_fixture().await;
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
        assert_review_mutation_audit_context(
            &events[1],
            DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
            id,
            &base,
            &head,
            true,
        );
        let approval_id = approval_id.to_string();
        assert_eq!(
            events[1].details.get("approval_id").map(String::as_str),
            Some(approval_id.as_str())
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("reason must stay out of audit"));
        assert!(!audit_json.contains("dismiss-approval"));
        assert!(!audit_json.contains("metadata only"));
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
    async fn change_request_responses_include_resolved_file_view_policy() {
        let (state, _base, _head, id) = review_fixture().await;

        let read = get_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        let body = response_json(read).await;
        assert_eq!(body["require_all_files_viewed"], false);

        state
            .review
            .create_protected_ref_rule_for_repo(&RepoId::local(), "main", 1, ROOT_UID, true)
            .await
            .unwrap();
        let read = get_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        let body = response_json(read).await;
        assert_eq!(body["require_all_files_viewed"], true);

        let (state, _base, _head, id) = review_fixture().await;
        state
            .review
            .create_protected_ref_rule_for_repo(&RepoId::local(), "main", 1, ROOT_UID, false)
            .await
            .unwrap();
        let read = get_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        let body = response_json(read).await;
        assert_eq!(body["require_all_files_viewed"], false);

        state
            .review
            .create_protected_path_rule_for_repo(
                &RepoId::local(),
                "/legal.txt",
                Some("main"),
                1,
                ROOT_UID,
                true,
            )
            .await
            .unwrap();
        let read = get_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        let body = response_json(read).await;
        assert_eq!(body["require_all_files_viewed"], true);

        let (state, _base, _head, id) = review_fixture().await;
        state
            .review
            .create_protected_ref_rule_for_repo(&RepoId::local(), "main", 1, ROOT_UID, false)
            .await
            .unwrap();
        state
            .review
            .create_protected_path_rule_for_repo(
                &RepoId::local(),
                "/legal.txt",
                Some("main"),
                1,
                ROOT_UID,
                false,
            )
            .await
            .unwrap();
        let read = get_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
            .await
            .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        let body = response_json(read).await;
        assert_eq!(body["require_all_files_viewed"], false);
    }

    #[tokio::test]
    async fn review_mutation_responses_include_resolved_file_view_policy() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_path_rule_for_repo(
                &RepoId::local(),
                "/legal.txt",
                Some("main"),
                1,
                ROOT_UID,
                true,
            )
            .await
            .unwrap();

        let approval = create_change_request_approval(
            State(state.clone()),
            user_headers("alice"),
            AxumPath(id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(approval.status(), StatusCode::CREATED);
        let approval_body = response_json(approval).await;
        assert_eq!(approval_body["require_all_files_viewed"], true);
        let approval_id = Uuid::parse_str(approval_body["approval"]["id"].as_str().unwrap())
            .expect("approval id");

        let reviewer = assign_change_request_reviewer(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(AssignReviewerRequest {
                reviewer_uid: state.db.login("alice").await.unwrap().uid,
                required: Some(false),
            }),
        )
        .await
        .into_response();
        assert_eq!(reviewer.status(), StatusCode::CREATED);
        let reviewer_body = response_json(reviewer).await;
        assert_eq!(reviewer_body["require_all_files_viewed"], true);

        let comment = create_change_request_comment(
            State(state.clone()),
            user_headers("root"),
            AxumPath(id),
            Json(CreateReviewCommentRequest {
                body: "looks fine".to_string(),
                path: Some("/legal.txt".to_string()),
                kind: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(comment.status(), StatusCode::CREATED);
        let comment_body = response_json(comment).await;
        assert_eq!(comment_body["require_all_files_viewed"], true);

        let dismiss = dismiss_change_request_approval(
            State(state.clone()),
            user_headers("root"),
            AxumPath((id, approval_id)),
            Json(DismissApprovalRequest {
                reason: Some("reset".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(dismiss.status(), StatusCode::OK);
        let dismiss_body = response_json(dismiss).await;
        assert_eq!(dismiss_body["require_all_files_viewed"], true);

        let (state, _base, _head, id) = review_fixture().await;
        state
            .review
            .create_protected_path_rule_for_repo(
                &RepoId::local(),
                "/legal.txt",
                Some("main"),
                1,
                ROOT_UID,
                true,
            )
            .await
            .unwrap();
        let rejected =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);
        let rejected_body = response_json(rejected).await;
        assert_eq!(rejected_body["require_all_files_viewed"], true);

        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_path_rule_for_repo(
                &RepoId::local(),
                "/legal.txt",
                Some("main"),
                1,
                ROOT_UID,
                true,
            )
            .await
            .unwrap();
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
        assert_eq!(merged_body["require_all_files_viewed"], true);
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
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::PolicyDecisionDeny);
        assert_eq!(events[0].resource.kind, AuditResourceKind::PolicyDecision);
        let change_request_id = id.to_string();
        assert_eq!(
            events[0]
                .details
                .get("change_request_id")
                .map(String::as_str),
            Some(change_request_id.as_str())
        );
        assert_eq!(
            events[0].details.get("target_ref").map(String::as_str),
            Some("main")
        );
        assert_eq!(
            events[0]
                .details
                .get("matched_ref_rule_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            events[0]
                .details
                .get("matched_path_rule_count")
                .map(String::as_str),
            Some("0")
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
        let merged_body = response_json(merged).await;
        assert_eq!(merged_body["approval_state"]["approved"], true);
        assert_eq!(merged_body["target_ref"]["target"], head);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].action, AuditAction::PolicyDecisionDeny);
        assert_eq!(events[1].action, AuditAction::ChangeRequestApprove);
        assert_eq!(events[2].action, AuditAction::PolicyDecisionAllow);
        assert_eq!(events[3].action, AuditAction::ChangeRequestMerge);
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
    async fn approval_protected_path_rule_uses_durable_changed_paths() {
        let (state, stores, base, head) = durable_review_fixture().await;
        let repo_id = RepoId::new("review-test").unwrap();
        let id = create_durable_change(&state, &base, &head).await;
        add_admin_user(&state, "alice").await;
        state
            .review
            .create_protected_path_rule_for_repo(
                &repo_id,
                "/legal.txt",
                Some("main"),
                1,
                ROOT_UID,
                true,
            )
            .await
            .unwrap();

        let blocked = merge_change_request(
            State(state.clone()),
            user_headers_for_repo("root", &repo_id),
            AxumPath(id),
        )
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
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::PolicyDecisionDeny);
        assert_eq!(
            events[0]
                .details
                .get("changed_path_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            events[0]
                .details
                .get("matched_path_rule_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            events[0].details.get("target_ref").map(String::as_str),
            Some("main")
        );
        let target = stores
            .refs
            .get(&repo_id, &RefName::new("main").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.target.to_hex(), base);

        let approval = create_change_request_approval(
            State(state.clone()),
            user_headers_for_repo("alice", &repo_id),
            AxumPath(id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(approval.status(), StatusCode::CREATED);

        let merged = merge_change_request(
            State(state.clone()),
            user_headers_for_repo("root", &repo_id),
            AxumPath(id),
        )
        .await
        .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        assert_eq!(response_json(merged).await["target_ref"]["target"], head);
        let target = stores
            .refs
            .get(&repo_id, &RefName::new("main").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.target.to_hex(), head);
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
    async fn approval_workflow_approval_creation_after_reject_or_merge_is_rejected() {
        let (rejected_state, _base, _head, rejected_id) = review_fixture().await;
        add_admin_user(&rejected_state, "alice").await;
        let rejected = reject_change_request(
            State(rejected_state.clone()),
            user_headers("root"),
            AxumPath(rejected_id),
        )
        .await
        .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);
        let audit_count = rejected_state.audit.list_recent(10).await.unwrap().len();

        let approval_after_reject = create_change_request_approval(
            State(rejected_state.clone()),
            user_headers_with_idempotency("alice", "approve-after-reject"),
            AxumPath(rejected_id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(approval_after_reject.status(), StatusCode::CONFLICT);
        assert!(
            rejected_state
                .review
                .list_approvals(rejected_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            rejected_state.audit.list_recent(10).await.unwrap().len(),
            audit_count
        );

        let (merged_state, _base, _head, merged_id) = review_fixture().await;
        add_admin_user(&merged_state, "alice").await;
        let merged = merge_change_request(
            State(merged_state.clone()),
            user_headers("root"),
            AxumPath(merged_id),
        )
        .await
        .into_response();
        assert_eq!(merged.status(), StatusCode::OK);
        let audit_count = merged_state.audit.list_recent(10).await.unwrap().len();

        let approval_after_merge = create_change_request_approval(
            State(merged_state.clone()),
            user_headers_with_idempotency("alice", "approve-after-merge"),
            AxumPath(merged_id),
            Json(CreateApprovalRequest { comment: None }),
        )
        .await
        .into_response();
        assert_eq!(approval_after_merge.status(), StatusCode::CONFLICT);
        assert!(
            merged_state
                .review
                .list_approvals(merged_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            merged_state.audit.list_recent(10).await.unwrap().len(),
            audit_count
        );
    }

    #[tokio::test]
    async fn approval_workflow_dismissal_after_terminal_is_rejected() {
        let (state, _base, _head, id) = review_fixture().await;
        add_admin_user(&state, "alice").await;
        let approval = approve_change_request_for(&state, id, "alice").await;
        let approval_id = Uuid::parse_str(approval["approval"]["id"].as_str().unwrap()).unwrap();
        let rejected =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);
        let audit_count = state.audit.list_recent(10).await.unwrap().len();

        let dismissed = dismiss_change_request_approval(
            State(state.clone()),
            user_headers_with_idempotency("root", "dismiss-after-terminal"),
            AxumPath((id, approval_id)),
            Json(DismissApprovalRequest {
                reason: Some("stale".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(dismissed.status(), StatusCode::CONFLICT);

        let approvals = state.review.list_approvals(id).await.unwrap();
        assert_eq!(approvals.len(), 1);
        assert!(approvals[0].active);
        assert_eq!(
            state.audit.list_recent(10).await.unwrap().len(),
            audit_count
        );
    }

    #[tokio::test]
    async fn approval_workflow_comment_after_terminal_is_rejected() {
        let (state, _base, _head, id) = review_fixture().await;
        let rejected =
            reject_change_request(State(state.clone()), user_headers("root"), AxumPath(id))
                .await
                .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);
        let audit_count = state.audit.list_recent(10).await.unwrap().len();

        let commented = create_change_request_comment(
            State(state.clone()),
            user_headers_with_idempotency("root", "comment-after-terminal"),
            AxumPath(id),
            Json(CreateReviewCommentRequest {
                body: "Too late.".to_string(),
                path: None,
                kind: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(commented.status(), StatusCode::CONFLICT);
        assert!(state.review.list_comments(id).await.unwrap().is_empty());
        assert_eq!(
            state.audit.list_recent(10).await.unwrap().len(),
            audit_count
        );
    }

    #[tokio::test]
    async fn approval_workflow_idempotency_replays_approval_comment_and_dismiss_after_terminal() {
        let (approval_state, _base, _head, approval_id) = review_fixture().await;
        add_admin_user(&approval_state, "alice").await;
        let approval_headers = user_headers_with_idempotency("alice", "approve-before-terminal");
        let approval_request = || CreateApprovalRequest {
            comment: Some("approved".to_string()),
        };
        let first_approval = create_change_request_approval(
            State(approval_state.clone()),
            approval_headers.clone(),
            AxumPath(approval_id),
            Json(approval_request()),
        )
        .await
        .into_response();
        assert_eq!(first_approval.status(), StatusCode::CREATED);
        let first_approval_body = response_json(first_approval).await;
        let rejected = reject_change_request(
            State(approval_state.clone()),
            user_headers("root"),
            AxumPath(approval_id),
        )
        .await
        .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);

        let approval_replay = create_change_request_approval(
            State(approval_state.clone()),
            approval_headers,
            AxumPath(approval_id),
            Json(approval_request()),
        )
        .await
        .into_response();
        assert_eq!(approval_replay.status(), StatusCode::CREATED);
        assert_eq!(
            approval_replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            response_json(approval_replay).await,
            sanitized_review_idempotency_body(&first_approval_body)
        );

        let (comment_state, _base, _head, comment_id) = review_fixture().await;
        let comment_headers = user_headers_with_idempotency("root", "comment-before-terminal");
        let comment_request = || CreateReviewCommentRequest {
            body: "Please update the summary.".to_string(),
            path: None,
            kind: None,
        };
        let first_comment = create_change_request_comment(
            State(comment_state.clone()),
            comment_headers.clone(),
            AxumPath(comment_id),
            Json(comment_request()),
        )
        .await
        .into_response();
        assert_eq!(first_comment.status(), StatusCode::CREATED);
        let first_comment_body = response_json(first_comment).await;
        let comment_key =
            IdempotencyKey::parse_header_value(comment_headers.get("idempotency-key").unwrap())
                .expect("comment idempotency key");
        let comment_session = require_admin(&comment_state, &comment_headers)
            .await
            .expect("comment session");
        let comment_repo =
            resolve_review_repo_context(&comment_state, &comment_headers, &comment_session)
                .expect("comment repo");
        let comment_scope = CREATE_CHANGE_REQUEST_COMMENT_ROUTE.to_string();
        let comment_fingerprint = request_fingerprint(
            &comment_scope,
            &review_fingerprint_body(
                serde_json::json!({
                    "route": CREATE_CHANGE_REQUEST_COMMENT_ROUTE,
                    "actor": actor_fingerprint(&comment_session),
                    "repo_id": comment_repo.repo_id().as_str(),
                    "change_request_id": comment_id,
                    "body": "Please update the summary.",
                    "path": serde_json::Value::Null,
                    "kind": ReviewCommentKind::General,
                }),
                &comment_repo,
            ),
        )
        .expect("comment fingerprint");
        match comment_state
            .idempotency
            .begin(&comment_scope, &comment_key, &comment_fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => {
                assert_eq!(
                    record.classification,
                    IdempotencyReplayClassification::Partial
                );
            }
            other => panic!("expected comment replay record, got {other:?}"),
        }
        let rejected = reject_change_request(
            State(comment_state.clone()),
            user_headers("root"),
            AxumPath(comment_id),
        )
        .await
        .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);

        let comment_replay = create_change_request_comment(
            State(comment_state.clone()),
            comment_headers,
            AxumPath(comment_id),
            Json(comment_request()),
        )
        .await
        .into_response();
        assert_eq!(comment_replay.status(), StatusCode::CREATED);
        assert_eq!(
            comment_replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            response_json(comment_replay).await,
            sanitized_review_idempotency_body(&first_comment_body)
        );

        let (dismiss_state, _base, _head, dismiss_id) = review_fixture().await;
        add_admin_user(&dismiss_state, "alice").await;
        let approval = approve_change_request_for(&dismiss_state, dismiss_id, "alice").await;
        let approval_record_id =
            Uuid::parse_str(approval["approval"]["id"].as_str().unwrap()).unwrap();
        let dismiss_headers = user_headers_with_idempotency("root", "dismiss-before-terminal");
        let dismiss_request = || DismissApprovalRequest {
            reason: Some("stale".to_string()),
        };
        let first_dismiss = dismiss_change_request_approval(
            State(dismiss_state.clone()),
            dismiss_headers.clone(),
            AxumPath((dismiss_id, approval_record_id)),
            Json(dismiss_request()),
        )
        .await
        .into_response();
        assert_eq!(first_dismiss.status(), StatusCode::OK);
        let first_dismiss_body = response_json(first_dismiss).await;
        let dismiss_key =
            IdempotencyKey::parse_header_value(dismiss_headers.get("idempotency-key").unwrap())
                .expect("dismiss idempotency key");
        let dismiss_session = require_admin(&dismiss_state, &dismiss_headers)
            .await
            .expect("dismiss session");
        let dismiss_repo =
            resolve_review_repo_context(&dismiss_state, &dismiss_headers, &dismiss_session)
                .expect("dismiss repo");
        let dismiss_scope = DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE.to_string();
        let dismiss_fingerprint = request_fingerprint(
            &dismiss_scope,
            &review_fingerprint_body(
                serde_json::json!({
                    "route": DISMISS_CHANGE_REQUEST_APPROVAL_ROUTE,
                    "actor": actor_fingerprint(&dismiss_session),
                    "repo_id": dismiss_repo.repo_id().as_str(),
                    "change_request_id": dismiss_id,
                    "approval_id": approval_record_id,
                    "reason": "stale",
                }),
                &dismiss_repo,
            ),
        )
        .expect("dismiss fingerprint");
        match dismiss_state
            .idempotency
            .begin(&dismiss_scope, &dismiss_key, &dismiss_fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => {
                assert_eq!(
                    record.classification,
                    IdempotencyReplayClassification::Partial
                );
                let rendered = serde_json::to_string(&record.response_body).unwrap();
                assert!(!rendered.contains("stale"));
                assert_eq!(
                    record.response_body["approval"]["dismissal_reason"],
                    serde_json::Value::Null
                );
            }
            other => panic!("expected dismiss replay record, got {other:?}"),
        }
        let rejected = reject_change_request(
            State(dismiss_state.clone()),
            user_headers("root"),
            AxumPath(dismiss_id),
        )
        .await
        .into_response();
        assert_eq!(rejected.status(), StatusCode::OK);

        let dismiss_replay = dismiss_change_request_approval(
            State(dismiss_state.clone()),
            dismiss_headers,
            AxumPath((dismiss_id, approval_record_id)),
            Json(dismiss_request()),
        )
        .await
        .into_response();
        assert_eq!(dismiss_replay.status(), StatusCode::OK);
        assert_eq!(
            dismiss_replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            response_json(dismiss_replay).await,
            sanitized_review_idempotency_body(&first_dismiss_body)
        );
    }

    #[tokio::test]
    async fn approval_workflow_merge_idempotency_replays_after_already_merged() {
        let (state, base, head, id) = review_fixture().await;
        let headers = user_headers_with_idempotency("root", "merge-cr-replay");

        let first = merge_change_request(State(state.clone()), headers.clone(), AxumPath(id))
            .await
            .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = merge_change_request(State(state.clone()), headers, AxumPath(id))
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

        let events = state.audit.list_recent(10).await.unwrap();
        let mutation_events = events
            .iter()
            .filter(|event| event.action == AuditAction::ChangeRequestMerge)
            .collect::<Vec<_>>();
        assert_eq!(mutation_events.len(), 1);
        assert_review_mutation_audit_context(
            mutation_events[0],
            MERGE_CHANGE_REQUEST_ROUTE,
            id,
            &base,
            &head,
            true,
        );
        let expected_target_ref_version = first_body["target_ref"]["version"]
            .as_u64()
            .unwrap()
            .to_string();
        assert_eq!(
            mutation_events[0]
                .details
                .get("target_ref_version")
                .map(String::as_str),
            Some(expected_target_ref_version.as_str())
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("merge-cr-replay"));
        assert!(!audit_json.contains("metadata only"));
    }

    #[tokio::test]
    async fn reject_change_request_idempotency_replays_after_status_changes() {
        let (state, base, head, id) = review_fixture().await;
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
        assert_eq!(
            response_json(replay).await,
            sanitized_review_idempotency_body(&first_body)
        );

        let events = state.audit.list_recent(10).await.unwrap();
        let mutation_events = events
            .iter()
            .filter(|event| event.action == AuditAction::ChangeRequestReject)
            .collect::<Vec<_>>();
        assert_eq!(mutation_events.len(), 1);
        assert_review_mutation_audit_context(
            mutation_events[0],
            REJECT_CHANGE_REQUEST_ROUTE,
            id,
            &base,
            &head,
            true,
        );
        let expected_change_request_version = first_body["change_request"]["version"]
            .as_u64()
            .unwrap()
            .to_string();
        assert_eq!(
            mutation_events[0]
                .details
                .get("change_request_version")
                .map(String::as_str),
            Some(expected_change_request_version.as_str())
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("reject-cr-replay"));
        assert!(!audit_json.contains("metadata only"));
    }
}
