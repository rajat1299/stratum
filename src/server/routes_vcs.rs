use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, patch, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;

use uuid::Uuid;

use super::idempotency as http_idempotency;
use super::middleware::{require_durable_core_repo_context, session_from_headers};
use super::policy::{
    self, PolicyDecisionToken, RoutePolicyAction, RoutePolicyCorrelation, RoutePolicyEvaluation,
    RoutePolicyRequest,
};
use super::repo_context::RequestRepoContext;
use super::{AppState, DurableRecoverySchedulerHandle};
use crate::audit::{AuditAction, AuditOutcome, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, WHEEL_GID};
use crate::backend::core_transaction::{
    DurableCoreCommitMetadataInsert, DurableCoreCommitObjectTreeWritePlan,
    DurableCoreCommitParentState, DurableCoreCommitPostCasEnvelope, DurableCoreCommitPostCasInput,
    DurableCoreCommitRefCasVisibility, DurableCoreCommitSourceSnapshot,
    DurableCoreCommittedResponse, DurableCorePostCasIdempotencyRecoveryContext,
    DurableCorePostCasIdempotencyResponseKind, DurableCorePostCasOutcome,
    DurableCorePostCasRecoveryClaim, DurableCorePostCasRecoveryClaimStore,
    DurableCorePostCasRecoveryContext, DurableCorePostCasRecoveryCounts,
    DurableCorePostCasRepairWorker, DurableCorePostCasRepairWorkerStores, DurableCorePostCasStep,
    DurableCorePreVisibilityRecoveryCounts, DurableCorePreVisibilityRecoveryRecord,
    DurableCorePreVisibilityRecoveryRun, DurableCorePreVisibilityRecoveryRunStores,
    DurableFsMutationRecoveryCounts, DurableFsMutationRecoveryWorker,
};
use crate::backend::durable_mutation::DURABLE_MUTATION_COMMIT_MESSAGE;
use crate::backend::object_cleanup::{
    ObjectCleanupClaimCounts, ObjectCleanupClaimKind, ObjectCleanupClaimState, ObjectCleanupWorker,
    ObjectCleanupWorkerSummary, ObjectGcDryRun,
};
use crate::backend::{CommitRecord, RepoId, StratumStores};
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyBegin, IdempotencyReplayClassification, IdempotencyReservation, request_fingerprint,
};
use crate::server::core::{DurableCoreRevertPlan, GuardedDurableCommitRoute};
use crate::vcs::{CommitId, MAIN_REF, RefName};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const VCS_COMMIT_IDEMPOTENCY_ROUTE: &str = "POST /vcs/commit";
const VCS_REVERT_IDEMPOTENCY_ROUTE: &str = "POST /vcs/revert";
const VCS_CREATE_REF_IDEMPOTENCY_ROUTE: &str = "POST /vcs/refs";
const VCS_UPDATE_REF_IDEMPOTENCY_ROUTE: &str = "PATCH /vcs/refs/{name}";
const VCS_RECOVERY_RUN_DEFAULT_LIMIT: usize = 10;
const VCS_RECOVERY_RUN_MAX_LIMIT: usize = 100;
const VCS_RECOVERY_RUN_LEASE_OWNER: &str = "guarded-durable-commit-recovery";
const VCS_FS_MUTATION_RECOVERY_RUN_LEASE_OWNER: &str = "durable-fs-mutation-recovery";
const VCS_RECOVERY_CORRELATION_ID_HEADER: &str = "X-Stratum-Recovery-Correlation-Id";

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

#[derive(Deserialize, Default)]
pub struct RecoveryRunRequest {
    pub limit: Option<usize>,
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
        .route("/vcs/recovery", get(vcs_recovery_status))
        .route("/vcs/recovery/run", post(vcs_recovery_run))
        .route("/vcs/refs", get(vcs_list_refs).post(vcs_create_ref))
        .route("/vcs/refs/{*name}", patch(vcs_update_ref))
}

pub fn durable_read_routes() -> Router<AppState> {
    Router::new()
        .route("/vcs/log", get(durable_vcs_log))
        .route("/vcs/status", get(vcs_status))
        .route("/vcs/diff", get(vcs_diff))
        .route(
            "/vcs/refs",
            get(durable_vcs_list_refs).post(durable_cloud_route_not_supported),
        )
        .route("/vcs/commit", post(durable_cloud_route_not_supported))
        .route("/vcs/revert", post(durable_cloud_route_not_supported))
        .route("/vcs/recovery", get(durable_cloud_route_not_supported))
        .route("/vcs/recovery/run", post(durable_cloud_route_not_supported))
        .route(
            "/vcs/refs/{*name}",
            patch(durable_cloud_route_not_supported),
        )
}

async fn durable_cloud_route_not_supported() -> impl IntoResponse {
    err_json(
        StatusCode::NOT_IMPLEMENTED,
        "stratum: operation not supported: durable-cloud route is not supported yet",
    )
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

fn require_admin_equivalent_session(session: &Session) -> Result<(), VfsError> {
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

async fn require_durable_read_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Session, VfsError> {
    let session = session_from_headers(state, headers).await?;
    require_durable_core_repo_context(state, headers, &session)?;
    require_admin_equivalent_session(&session)?;
    Ok(session)
}

fn require_vcs_mutation_session(session: &Session) -> Result<(), VfsError> {
    if session.scope.is_some() {
        return Err(VfsError::PermissionDenied {
            path: "admin operation".to_string(),
        });
    }

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
    session: &Session,
    headers: &HeaderMap,
    action: RoutePolicyAction,
    ref_name: &str,
) -> Result<RoutePolicyEvaluation, axum::response::Response> {
    let request = route_policy_request_from_session(state, headers, action, session)
        .map_err(|e| {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        })?
        .with_target_ref(ref_name)
        .with_correlation(policy_correlation_from_headers(headers));
    let evaluation = policy::evaluate_route_policy(state.review.as_ref(), request)
        .await
        .map_err(|e| {
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        })?;

    if !evaluation.decision.is_allowed() {
        append_policy_audit(state, session, &evaluation).await?;
        return Err(err_json(
            StatusCode::FORBIDDEN,
            format!("protected ref '{ref_name}' requires change request merge"),
        )
        .into_response());
    }

    Ok(evaluation)
}

fn route_policy_request_from_session(
    state: &AppState,
    headers: &HeaderMap,
    action: RoutePolicyAction,
    session: &Session,
) -> Result<RoutePolicyRequest, VfsError> {
    let repo = if !state.requires_explicit_workspace_repo()
        && session
            .mount()
            .and_then(crate::auth::session::SessionMount::repo_id)
            .is_none()
    {
        RequestRepoContext::local_singleton()
    } else {
        RequestRepoContext::resolve(
            headers,
            session.mount(),
            !state.requires_explicit_workspace_repo(),
        )?
    };
    Ok(RoutePolicyRequest::from_session(action, session).with_repo_id(repo.repo_id().clone()))
}

async fn require_unprotected_revert_paths(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    hash_prefix: &str,
) -> Result<
    (
        String,
        Vec<crate::review::ProtectedPathRule>,
        RoutePolicyEvaluation,
    ),
    axum::response::Response,
> {
    let target_hash = state
        .core
        .resolve_commit_hash(hash_prefix)
        .await
        .map_err(|e| {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        })?;

    let preflight =
        route_policy_request_from_session(state, headers, RoutePolicyAction::VcsRevert, session)
            .map_err(|e| {
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
            })?
            .with_target_ref(crate::vcs::MAIN_REF)
            .with_correlation(policy_correlation_from_headers(headers));
    let preflight = policy::evaluate_route_policy(state.review.as_ref(), preflight)
        .await
        .map_err(|e| {
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        })?;
    if preflight.applicable_path_rules.is_empty() {
        return Ok((
            target_hash,
            preflight.applicable_path_rules.clone(),
            preflight,
        ));
    }

    let changed_paths = state
        .core
        .changed_paths_for_revert(&target_hash)
        .await
        .map_err(|e| {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        })?;
    if changed_paths.is_empty() {
        return Ok((
            target_hash,
            preflight.applicable_path_rules.clone(),
            preflight,
        ));
    }

    let request =
        route_policy_request_from_session(state, headers, RoutePolicyAction::VcsRevert, session)
            .map_err(|e| {
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
            })?
            .with_target_ref(crate::vcs::MAIN_REF)
            .with_changed_paths(changed_paths)
            .with_correlation(policy_correlation_from_headers(headers));
    let evaluation = policy::evaluate_route_policy(state.review.as_ref(), request)
        .await
        .map_err(|e| {
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        })?;
    if !evaluation.decision.is_allowed() {
        append_policy_audit(state, session, &evaluation).await?;
        let path = evaluation
            .denied_path
            .as_deref()
            .unwrap_or(crate::vcs::MAIN_REF);
        return Err(err_json(
            StatusCode::FORBIDDEN,
            format!("protected path requires change request merge: '{path}'"),
        )
        .into_response());
    }

    Ok((
        target_hash,
        evaluation.applicable_path_rules.clone(),
        evaluation,
    ))
}

async fn require_unprotected_durable_revert_paths(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    changed_paths: Vec<String>,
) -> Result<(Vec<crate::review::ProtectedPathRule>, RoutePolicyEvaluation), axum::response::Response>
{
    let preflight =
        route_policy_request_from_session(state, headers, RoutePolicyAction::VcsRevert, session)
            .map_err(|e| {
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
            })?
            .with_target_ref(crate::vcs::MAIN_REF)
            .with_correlation(policy_correlation_from_headers(headers));
    let preflight = policy::evaluate_route_policy(state.review.as_ref(), preflight)
        .await
        .map_err(|e| {
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        })?;
    if changed_paths.is_empty() {
        return Ok((preflight.applicable_path_rules.clone(), preflight));
    }

    let request =
        route_policy_request_from_session(state, headers, RoutePolicyAction::VcsRevert, session)
            .map_err(|e| {
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
            })?
            .with_target_ref(crate::vcs::MAIN_REF)
            .with_changed_paths(changed_paths)
            .with_correlation(policy_correlation_from_headers(headers));
    let evaluation = policy::evaluate_route_policy(state.review.as_ref(), request)
        .await
        .map_err(|e| {
            err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response()
        })?;
    if !evaluation.decision.is_allowed() {
        append_policy_audit(state, session, &evaluation).await?;
        let path = evaluation
            .denied_path
            .as_deref()
            .unwrap_or(crate::vcs::MAIN_REF);
        return Err(err_json(
            StatusCode::FORBIDDEN,
            format!("protected path requires change request merge: '{path}'"),
        )
        .into_response());
    }

    Ok((evaluation.applicable_path_rules.clone(), evaluation))
}

fn audit_append_failed_response_parts(error: VfsError) -> (StatusCode, serde_json::Value) {
    (
        error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
        serde_json::json!({
            "error": "audit append failed after mutation",
            "mutation_committed": true,
            "audit_recorded": false,
        }),
    )
}

fn policy_audit_append_failed_response_parts(error: VfsError) -> (StatusCode, serde_json::Value) {
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
            let (status, body) = policy_audit_append_failed_response_parts(error);
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
        message: "invalid x-stratum-workspace header".to_string(),
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

fn vcs_idempotency_scope_for_repo(route: &str, repo: &RequestRepoContext) -> String {
    if repo.is_local_singleton() {
        vcs_idempotency_scope(route)
    } else {
        format!("repo:{}:{route}", repo.repo_id())
    }
}

fn explicit_repo_fingerprint(repo: &RequestRepoContext) -> Option<&str> {
    (!repo.is_local_singleton()).then_some(repo.repo_id().as_str())
}

fn with_explicit_repo_fingerprint(
    mut body: serde_json::Value,
    repo: &RequestRepoContext,
) -> serde_json::Value {
    if let Some(repo_id) = explicit_repo_fingerprint(repo)
        && let Some(object) = body.as_object_mut()
    {
        object.insert(
            "repo_id".to_string(),
            serde_json::Value::String(repo_id.to_string()),
        );
    }
    body
}

#[expect(
    clippy::result_large_err,
    reason = "route helpers return concrete axum responses for early exits"
)]
fn resolve_vcs_repo_context(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
) -> Result<RequestRepoContext, axum::response::Response> {
    if !state.requires_explicit_workspace_repo()
        && session
            .mount()
            .and_then(crate::auth::session::SessionMount::repo_id)
            .is_none()
    {
        return Ok(RequestRepoContext::local_singleton());
    }

    RequestRepoContext::resolve(
        headers,
        session.mount(),
        !state.requires_explicit_workspace_repo(),
    )
    .map_err(|e| err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response())
}

#[expect(
    clippy::result_large_err,
    reason = "route helpers return concrete axum responses for early exits"
)]
fn resolve_guarded_durable_vcs_capability(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
) -> Result<Option<(GuardedDurableCommitRoute, RequestRepoContext)>, axum::response::Response> {
    require_durable_core_repo_context(state, headers, session).map_err(|e| {
        err_json(error_status(&e, StatusCode::FORBIDDEN), e.to_string()).into_response()
    })?;
    let Some(capability) = state.core.guarded_durable_commit_route() else {
        return Ok(None);
    };
    let repo = resolve_vcs_repo_context(state, headers, session)?;
    Ok(Some((capability.for_repo(repo.repo_id().clone()), repo)))
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
            .complete_with_classification(
                reservation,
                status.as_u16(),
                body.clone(),
                http_idempotency::secret_free(),
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

async fn complete_vcs_partial_idempotency(
    state: &AppState,
    reservation: Option<&IdempotencyReservation>,
    status: StatusCode,
    body: &serde_json::Value,
) -> Result<(), axum::response::Response> {
    if let Some(reservation) = reservation {
        state
            .idempotency
            .complete_with_classification(
                reservation,
                status.as_u16(),
                body.clone(),
                http_idempotency::partial(),
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

fn vcs_commit_idempotency_body(body: &JsonValue) -> JsonValue {
    let mut replay_body = body.clone();
    if let JsonValue::Object(fields) = &mut replay_body
        && fields.contains_key("message")
    {
        fields.insert("message".to_string(), JsonValue::Null);
    }
    replay_body
}

async fn complete_vcs_commit_idempotency(
    state: &AppState,
    reservation: Option<&IdempotencyReservation>,
    status: StatusCode,
    body: &JsonValue,
) -> Result<(), axum::response::Response> {
    let replay_body = vcs_commit_idempotency_body(body);
    let classification = if &replay_body == body {
        IdempotencyReplayClassification::SecretFree
    } else {
        IdempotencyReplayClassification::Partial
    };
    if let Some(reservation) = reservation {
        state
            .idempotency
            .complete_with_classification(reservation, status.as_u16(), replay_body, classification)
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

async fn abort_vcs_idempotency(state: &AppState, reservation: Option<&IdempotencyReservation>) {
    if let Some(reservation) = reservation {
        state.idempotency.abort(reservation).await;
    }
}

async fn update_workspace_head_from_headers(
    state: &AppState,
    headers: &HeaderMap,
    repo_id: &crate::backend::RepoId,
    head_commit: Option<String>,
) -> Result<(), VfsError> {
    let Some(workspace_id) = workspace_id_from_headers(headers)? else {
        return Ok(());
    };
    match state
        .workspaces
        .update_head_commit_for_repo(repo_id, workspace_id, head_commit)
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
                .with_detail("error", "workspace head update failed"),
        )
        .await
        .map(|_| ())
}

async fn validate_workspace_header(
    state: &AppState,
    headers: &HeaderMap,
    repo_id: &crate::backend::RepoId,
) -> Result<Option<Uuid>, VfsError> {
    let Some(workspace_id) = workspace_id_from_headers(headers)? else {
        return Ok(None);
    };
    match state
        .workspaces
        .get_workspace_for_repo(repo_id, workspace_id)
        .await?
    {
        Some(_) => Ok(Some(workspace_id)),
        None => Err(VfsError::NotFound {
            path: format!("workspace:{workspace_id}"),
        }),
    }
}

fn current_unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn current_unix_timestamp_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

async fn guarded_durable_commit_pre_cas_error_response(
    state: &AppState,
    reservation: Option<&IdempotencyReservation>,
    error: VfsError,
) -> axum::response::Response {
    abort_vcs_idempotency(state, reservation).await;
    err_json(
        error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
        error.to_string(),
    )
    .into_response()
}

fn guarded_durable_commit_visibility_unconfirmed_response() -> axum::response::Response {
    err_json(
        StatusCode::INTERNAL_SERVER_ERROR,
        "durable commit visibility recovery is required",
    )
    .into_response()
}

async fn guarded_durable_commit_pre_visibility_unconfirmed_response(
    stores: &StratumStores,
    record: Result<DurableCorePreVisibilityRecoveryRecord, VfsError>,
    context: Option<DurableCorePostCasRecoveryContext>,
) -> axum::response::Response {
    let Ok(record) = record else {
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "durable commit pre-visibility recovery status unavailable",
        )
        .into_response();
    };
    let record = match context {
        Some(context) => record.with_post_cas_context(context),
        None => record,
    };
    if stores.pre_visibility_recovery.record(record).await.is_err() {
        return err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "durable commit pre-visibility recovery status unavailable",
        )
        .into_response();
    }
    guarded_durable_commit_visibility_unconfirmed_response()
}

fn guarded_durable_commit_pre_visibility_context(
    record: &DurableCorePreVisibilityRecoveryRecord,
    session: &Session,
    message: &str,
    workspace_id: Option<Uuid>,
    reservation: Option<&IdempotencyReservation>,
) -> DurableCorePostCasRecoveryContext {
    let commit_hash = record.target().commit_id().to_hex();
    let mut audit_event = NewAuditEvent::from_session(
        session,
        AuditAction::VcsCommit,
        AuditResource::id(AuditResourceKind::Commit, &commit_hash),
    )
    .with_detail("author", &session.username);
    if let Some(workspace_id) = workspace_id {
        audit_event = audit_event.with_detail("workspace_id", workspace_id);
    }
    let _ = message;
    DurableCorePostCasRecoveryContext::new(
        workspace_id,
        record
            .parent_commit_id()
            .map(|commit_id| commit_id.to_hex()),
        Some(audit_event),
        reservation.map(|reservation| {
            DurableCorePostCasIdempotencyRecoveryContext::from_reservation(
                reservation,
                DurableCorePostCasIdempotencyResponseKind::FullCommit,
            )
        }),
    )
}

fn guarded_durable_revert_pre_visibility_context(
    record: &DurableCorePreVisibilityRecoveryRecord,
    session: &Session,
    workspace_id: Option<Uuid>,
    reservation: Option<&IdempotencyReservation>,
    target_commit: CommitId,
    expected_head: CommitId,
) -> DurableCorePostCasRecoveryContext {
    let commit_hash = record.target().commit_id().to_hex();
    let mut audit_event = NewAuditEvent::from_session(
        session,
        AuditAction::VcsRevert,
        AuditResource::id(AuditResourceKind::Commit, &commit_hash),
    )
    .with_detail("reverted_to", target_commit.to_hex())
    .with_detail("target_commit", target_commit.to_hex())
    .with_detail("target_ref", MAIN_REF)
    .with_detail("expected_head", expected_head.to_hex());
    if let Some(workspace_id) = workspace_id {
        audit_event = audit_event.with_detail("workspace_id", workspace_id);
    }
    DurableCorePostCasRecoveryContext::new(
        workspace_id,
        record
            .parent_commit_id()
            .map(|commit_id| commit_id.to_hex()),
        Some(audit_event),
        reservation.map(|reservation| {
            DurableCorePostCasIdempotencyRecoveryContext::from_reservation(
                reservation,
                DurableCorePostCasIdempotencyResponseKind::FullCommit,
            )
        }),
    )
}

fn is_ref_cas_mismatch_error(error: &VfsError) -> bool {
    matches!(
        error,
        VfsError::InvalidArgs { message }
            if message.starts_with("ref compare-and-swap mismatch")
    )
}

async fn guarded_durable_revert_has_unresolved_recovery(
    capability: &GuardedDurableCommitRoute,
) -> Result<bool, axum::response::Response> {
    let repo_id = capability.repo_id();
    let stores = capability.stores();
    let recovery_error = || {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "durable VCS recovery status unavailable",
        )
        .into_response()
    };

    if stores
        .pre_visibility_recovery
        .has_unresolved_for_ref(repo_id, MAIN_REF)
        .await
        .map_err(|_| recovery_error())?
    {
        return Ok(true);
    }

    if stores
        .post_cas_recovery
        .has_unresolved_for_ref(repo_id, MAIN_REF)
        .await
        .map_err(|_| recovery_error())?
    {
        return Ok(true);
    }

    if stores
        .fs_mutation_recovery
        .has_unresolved_for_ref(repo_id, MAIN_REF)
        .await
        .map_err(|_| recovery_error())?
    {
        return Ok(true);
    }

    Ok(false)
}

fn guarded_durable_revert_recovery_conflict_response() -> axum::response::Response {
    err_json(
        StatusCode::CONFLICT,
        "durable VCS recovery is pending for target ref",
    )
    .into_response()
}

async fn guarded_durable_vcs_commit(
    state: &AppState,
    capability: GuardedDurableCommitRoute,
    session: &Session,
    message: &str,
    workspace_id: Option<Uuid>,
    reservation: Option<IdempotencyReservation>,
    policy_token: PolicyDecisionToken,
) -> axum::response::Response {
    if let Err(error) = policy_token.require_allowed_for(
        capability.repo_id(),
        RoutePolicyAction::VcsCommit,
        MAIN_REF,
    ) {
        return guarded_durable_commit_pre_cas_error_response(state, reservation.as_ref(), error)
            .await;
    }

    let preflight = match capability.commit_metadata_preflight().await {
        Ok(preflight) => preflight,
        Err(error) => {
            return guarded_durable_commit_pre_cas_error_response(
                state,
                reservation.as_ref(),
                error,
            )
            .await;
        }
    };

    let source = match DurableCoreCommitSourceSnapshot::from_durable_parent_state(
        capability.repo_id(),
        preflight.parent_state(),
        capability.stores().commits.as_ref(),
        capability.stores().objects.as_ref(),
    )
    .await
    {
        Ok(source) => source,
        Err(error) => {
            return guarded_durable_commit_pre_cas_error_response(
                state,
                reservation.as_ref(),
                error,
            )
            .await;
        }
    };

    let plan =
        match guarded_durable_commit_write_plan(state, &capability, source, workspace_id).await {
            Ok(plan) => plan,
            Err(error) => {
                return guarded_durable_commit_pre_cas_error_response(
                    state,
                    reservation.as_ref(),
                    error,
                )
                .await;
            }
        };
    let convergence = match plan
        .converge_objects(capability.repo_id(), capability.stores().objects.as_ref())
        .await
    {
        Ok(convergence) => convergence,
        Err(error) => {
            return guarded_durable_commit_pre_cas_error_response(
                state,
                reservation.as_ref(),
                error,
            )
            .await;
        }
    };

    let timestamp = current_unix_timestamp_secs();
    let metadata = match plan
        .insert_commit_metadata(
            &convergence,
            capability.stores().commits.as_ref(),
            timestamp,
            &session.username,
            message,
        )
        .await
    {
        Ok(metadata) => metadata,
        Err(_) => match plan
            .recover_commit_metadata_insert(
                &convergence,
                capability.stores().commits.as_ref(),
                timestamp,
                &session.username,
                message,
            )
            .await
        {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return guarded_durable_commit_pre_cas_error_response(
                    state,
                    reservation.as_ref(),
                    VfsError::CorruptStore {
                        message: "durable commit metadata insert failed".to_string(),
                    },
                )
                .await;
            }
            Err(_) => {
                let record = plan.pre_visibility_recovery_record_for_metadata_insert(
                    &convergence,
                    timestamp,
                    &session.username,
                    message,
                    reservation.is_some(),
                    current_unix_timestamp_millis(),
                );
                let context = record.as_ref().ok().map(|record| {
                    guarded_durable_commit_pre_visibility_context(
                        record,
                        session,
                        message,
                        workspace_id,
                        reservation.as_ref(),
                    )
                });
                return guarded_durable_commit_pre_visibility_unconfirmed_response(
                    capability.stores(),
                    record,
                    context,
                )
                .await;
            }
        },
    };

    let visibility = match plan
        .apply_ref_cas_visibility(&metadata, capability.stores().refs.as_ref())
        .await
    {
        Ok(visibility) => visibility,
        Err(error) => {
            if is_ref_cas_mismatch_error(&error) {
                return guarded_durable_commit_pre_cas_error_response(
                    state,
                    reservation.as_ref(),
                    error,
                )
                .await;
            }
            match plan
                .recover_ref_cas_visibility(&metadata, capability.stores().refs.as_ref())
                .await
            {
                Ok(Some(visibility)) => visibility,
                Ok(None) => {
                    return guarded_durable_commit_pre_cas_error_response(
                        state,
                        reservation.as_ref(),
                        VfsError::CorruptStore {
                            message: "durable commit ref visibility update failed".to_string(),
                        },
                    )
                    .await;
                }
                Err(_) => {
                    let record = plan.pre_visibility_recovery_record_for_ref_visibility(
                        &metadata,
                        reservation.is_some(),
                        current_unix_timestamp_millis(),
                    );
                    let context = record.as_ref().ok().map(|record| {
                        guarded_durable_commit_pre_visibility_context(
                            record,
                            session,
                            message,
                            workspace_id,
                            reservation.as_ref(),
                        )
                    });
                    return guarded_durable_commit_pre_visibility_unconfirmed_response(
                        capability.stores(),
                        record,
                        context,
                    )
                    .await;
                }
            }
        }
    };

    guarded_durable_commit_complete_post_cas(GuardedDurableCommitPostCasRouteInput {
        plan: &plan,
        metadata: &metadata,
        visibility: &visibility,
        post_cas_stores: capability.stores(),
        session,
        message,
        workspace_id,
        reservation,
    })
    .await
}

async fn guarded_durable_vcs_revert(
    state: &AppState,
    capability: GuardedDurableCommitRoute,
    session: &Session,
    revert_plan: DurableCoreRevertPlan,
    workspace_id: Option<Uuid>,
    reservation: Option<IdempotencyReservation>,
    policy_token: PolicyDecisionToken,
) -> axum::response::Response {
    let changed_paths = revert_plan.changed_path_strings();
    if let Err(error) = policy_token.require_allowed_for_changed_paths(
        capability.repo_id(),
        RoutePolicyAction::VcsRevert,
        MAIN_REF,
        changed_paths.iter().map(String::as_str),
    ) {
        return guarded_durable_commit_pre_cas_error_response(state, reservation.as_ref(), error)
            .await;
    }

    let target_commit_id = revert_plan.target_commit().id;
    let expected_head = revert_plan.expected_head().target;
    let message = format!("revert to {}", target_commit_id.short_hex());
    let plan = revert_plan.plan();
    let convergence = match plan
        .converge_objects(capability.repo_id(), capability.stores().objects.as_ref())
        .await
    {
        Ok(convergence) => convergence,
        Err(error) => {
            return guarded_durable_commit_pre_cas_error_response(
                state,
                reservation.as_ref(),
                error,
            )
            .await;
        }
    };

    let timestamp = current_unix_timestamp_secs();
    let metadata = match plan
        .insert_commit_metadata(
            &convergence,
            capability.stores().commits.as_ref(),
            timestamp,
            &session.username,
            &message,
        )
        .await
    {
        Ok(metadata) => metadata,
        Err(_) => match plan
            .recover_commit_metadata_insert(
                &convergence,
                capability.stores().commits.as_ref(),
                timestamp,
                &session.username,
                &message,
            )
            .await
        {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return guarded_durable_commit_pre_cas_error_response(
                    state,
                    reservation.as_ref(),
                    VfsError::CorruptStore {
                        message: "durable commit metadata insert failed".to_string(),
                    },
                )
                .await;
            }
            Err(_) => {
                let record = plan.pre_visibility_recovery_record_for_metadata_insert(
                    &convergence,
                    timestamp,
                    &session.username,
                    &message,
                    reservation.is_some(),
                    current_unix_timestamp_millis(),
                );
                let context = record.as_ref().ok().map(|record| {
                    guarded_durable_revert_pre_visibility_context(
                        record,
                        session,
                        workspace_id,
                        reservation.as_ref(),
                        target_commit_id,
                        expected_head,
                    )
                });
                return guarded_durable_commit_pre_visibility_unconfirmed_response(
                    capability.stores(),
                    record,
                    context,
                )
                .await;
            }
        },
    };

    let visibility = match plan
        .apply_ref_cas_visibility(&metadata, capability.stores().refs.as_ref())
        .await
    {
        Ok(visibility) => visibility,
        Err(error) => {
            if is_ref_cas_mismatch_error(&error) {
                return guarded_durable_commit_pre_cas_error_response(
                    state,
                    reservation.as_ref(),
                    error,
                )
                .await;
            }
            match plan
                .recover_ref_cas_visibility(&metadata, capability.stores().refs.as_ref())
                .await
            {
                Ok(Some(visibility)) => visibility,
                Ok(None) => {
                    return guarded_durable_commit_pre_cas_error_response(
                        state,
                        reservation.as_ref(),
                        VfsError::CorruptStore {
                            message: "durable commit ref visibility update failed".to_string(),
                        },
                    )
                    .await;
                }
                Err(_) => {
                    let record = plan.pre_visibility_recovery_record_for_ref_visibility(
                        &metadata,
                        reservation.is_some(),
                        current_unix_timestamp_millis(),
                    );
                    let context = record.as_ref().ok().map(|record| {
                        guarded_durable_revert_pre_visibility_context(
                            record,
                            session,
                            workspace_id,
                            reservation.as_ref(),
                            target_commit_id,
                            expected_head,
                        )
                    });
                    return guarded_durable_commit_pre_visibility_unconfirmed_response(
                        capability.stores(),
                        record,
                        context,
                    )
                    .await;
                }
            }
        }
    };

    let committed_response = match DurableCoreCommittedResponse::vcs_revert_success(
        metadata.commit_id(),
        target_commit_id,
        MAIN_REF,
        expected_head,
    ) {
        Ok(response) => response,
        Err(_) => return guarded_durable_commit_visibility_unconfirmed_response(),
    };
    let mut audit_event = NewAuditEvent::from_session(
        session,
        AuditAction::VcsRevert,
        AuditResource::id(AuditResourceKind::Commit, metadata.commit_id().to_hex()),
    )
    .with_detail("reverted_to", target_commit_id.to_hex())
    .with_detail("target_commit", target_commit_id.to_hex())
    .with_detail("target_ref", MAIN_REF)
    .with_detail("expected_head", expected_head.to_hex());
    if let Some(workspace_id) = workspace_id {
        audit_event = audit_event.with_detail("workspace_id", workspace_id);
    }

    guarded_durable_complete_post_cas(GuardedDurablePostCasRouteInput {
        plan,
        metadata: &metadata,
        visibility: &visibility,
        post_cas_stores: capability.stores(),
        workspace_id,
        reservation,
        committed_response,
        audit_event,
    })
    .await
}

async fn guarded_durable_commit_write_plan(
    state: &AppState,
    capability: &GuardedDurableCommitRoute,
    source: DurableCoreCommitSourceSnapshot,
    workspace_id: Option<Uuid>,
) -> Result<DurableCoreCommitObjectTreeWritePlan, VfsError> {
    if let Some(workspace_id) = workspace_id {
        let workspace = state
            .workspaces
            .get_workspace_for_repo(capability.repo_id(), workspace_id)
            .await?
            .ok_or_else(|| VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            })?;
        if let Some(session_ref) = workspace.session_ref.as_deref() {
            if workspace.base_ref != MAIN_REF {
                return Err(VfsError::NotSupported {
                    message: "durable session commit currently requires main as base ref"
                        .to_string(),
                });
            }
            let session_ref = RefName::new(session_ref).map_err(|_| VfsError::InvalidArgs {
                message: "invalid workspace session ref".to_string(),
            })?;
            let session = capability
                .stores()
                .refs
                .get(capability.repo_id(), &session_ref)
                .await
                .map_err(|_| VfsError::CorruptStore {
                    message: "durable session ref lookup failed".to_string(),
                })?
                .ok_or_else(|| VfsError::NotFound {
                    path: "session ref".to_string(),
                })?;
            let session_commit = capability
                .stores()
                .commits
                .get(capability.repo_id(), session.target)
                .await
                .map_err(|_| VfsError::CorruptStore {
                    message: "durable session commit lookup failed".to_string(),
                })?
                .ok_or_else(|| VfsError::CorruptStore {
                    message: "durable session commit metadata is missing".to_string(),
                })?;
            guarded_validate_session_ref_matches_source(&source, capability, session.target)
                .await?;
            return DurableCoreCommitObjectTreeWritePlan::build_from_durable_root_tree(
                capability.repo_id(),
                source,
                session_commit.root_tree,
                capability.stores().objects.as_ref(),
            )
            .await;
        }
    }

    let fs = state.db.snapshot_fs_async().await;
    match tokio::task::spawn_blocking(move || {
        DurableCoreCommitObjectTreeWritePlan::build(source, &fs)
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(VfsError::CorruptStore {
            message: "durable commit write plan failed".to_string(),
        }),
    }
}

async fn guarded_validate_session_ref_matches_source(
    source: &DurableCoreCommitSourceSnapshot,
    capability: &GuardedDurableCommitRoute,
    session_target: CommitId,
) -> Result<(), VfsError> {
    let DurableCoreCommitParentState::Existing { target, .. } = source.parent_state() else {
        return Err(guarded_durable_ref_cas_mismatch());
    };

    if guarded_session_ref_descends_from(capability, session_target, target).await? {
        Ok(())
    } else {
        Err(guarded_durable_ref_cas_mismatch())
    }
}

async fn guarded_session_ref_descends_from(
    capability: &GuardedDurableCommitRoute,
    session_target: CommitId,
    expected_base: CommitId,
) -> Result<bool, VfsError> {
    let expected_base_commit = capability
        .stores()
        .commits
        .get(capability.repo_id(), expected_base)
        .await
        .map_err(|_| VfsError::CorruptStore {
            message: "durable session commit lookup failed".to_string(),
        })?
        .ok_or_else(|| VfsError::CorruptStore {
            message: "durable session commit metadata is missing".to_string(),
        })?;
    if expected_base_commit.repo_id != *capability.repo_id()
        || expected_base_commit.id != expected_base
    {
        return Err(VfsError::CorruptStore {
            message: "durable session commit metadata is invalid".to_string(),
        });
    }
    let mut current = session_target;
    for _ in 0..1024 {
        let commit = capability
            .stores()
            .commits
            .get(capability.repo_id(), current)
            .await
            .map_err(|_| VfsError::CorruptStore {
                message: "durable session commit lookup failed".to_string(),
            })?
            .ok_or_else(|| VfsError::CorruptStore {
                message: "durable session commit metadata is missing".to_string(),
            })?;
        if commit.repo_id != *capability.repo_id() || commit.id != current {
            return Err(VfsError::CorruptStore {
                message: "durable session commit metadata is invalid".to_string(),
            });
        }
        if current == expected_base {
            return Ok(true);
        }
        if commit.message != DURABLE_MUTATION_COMMIT_MESSAGE {
            return Ok(false);
        }
        if guarded_session_ref_matches_previous_promotion(&commit, &expected_base_commit) {
            return Ok(true);
        }
        let [parent] = commit.parents.as_slice() else {
            return Err(VfsError::CorruptStore {
                message: "durable session commit metadata is invalid".to_string(),
            });
        };
        current = *parent;
    }

    Err(VfsError::CorruptStore {
        message: "durable session commit chain is too deep".to_string(),
    })
}

fn guarded_session_ref_matches_previous_promotion(
    session_commit: &CommitRecord,
    expected_base_commit: &CommitRecord,
) -> bool {
    !expected_base_commit.parents.is_empty()
        && session_commit.root_tree == expected_base_commit.root_tree
        && session_commit.parents == expected_base_commit.parents
}

fn guarded_durable_ref_cas_mismatch() -> VfsError {
    VfsError::InvalidArgs {
        message: "ref compare-and-swap mismatch".to_string(),
    }
}

struct GuardedDurableCommitPostCasRouteInput<'a> {
    plan: &'a DurableCoreCommitObjectTreeWritePlan,
    metadata: &'a DurableCoreCommitMetadataInsert,
    visibility: &'a DurableCoreCommitRefCasVisibility,
    post_cas_stores: &'a StratumStores,
    session: &'a Session,
    message: &'a str,
    workspace_id: Option<Uuid>,
    reservation: Option<IdempotencyReservation>,
}

struct GuardedDurablePostCasRouteInput<'a> {
    plan: &'a DurableCoreCommitObjectTreeWritePlan,
    metadata: &'a DurableCoreCommitMetadataInsert,
    visibility: &'a DurableCoreCommitRefCasVisibility,
    post_cas_stores: &'a StratumStores,
    workspace_id: Option<Uuid>,
    reservation: Option<IdempotencyReservation>,
    committed_response: DurableCoreCommittedResponse,
    audit_event: NewAuditEvent,
}

struct GuardedDurablePostCasRouteRecoveryClaims {
    workspace: Option<DurableCorePostCasRecoveryClaim>,
    audit: DurableCorePostCasRecoveryClaim,
    idempotency: Option<DurableCorePostCasRecoveryClaim>,
}

async fn guarded_durable_commit_complete_post_cas(
    input: GuardedDurableCommitPostCasRouteInput<'_>,
) -> axum::response::Response {
    let metadata = input.metadata;
    let session = input.session;
    let message = input.message;
    let workspace_id = input.workspace_id;
    let reservation = input.reservation;
    let committed_response = match DurableCoreCommittedResponse::vcs_commit_success(
        metadata.commit_id(),
        message,
        &session.username,
    ) {
        Ok(response) => response,
        Err(_) => {
            return guarded_durable_commit_visibility_unconfirmed_response();
        }
    };
    let commit_hash = metadata.commit_id().to_hex();
    let mut audit_event = NewAuditEvent::from_session(
        session,
        AuditAction::VcsCommit,
        AuditResource::id(AuditResourceKind::Commit, &commit_hash),
    )
    .with_detail("author", &session.username);
    if let Some(workspace_id) = workspace_id {
        audit_event = audit_event.with_detail("workspace_id", workspace_id);
    }

    guarded_durable_complete_post_cas(GuardedDurablePostCasRouteInput {
        plan: input.plan,
        metadata,
        visibility: input.visibility,
        post_cas_stores: input.post_cas_stores,
        workspace_id,
        reservation,
        committed_response,
        audit_event,
    })
    .await
}

async fn guarded_durable_complete_post_cas(
    input: GuardedDurablePostCasRouteInput<'_>,
) -> axum::response::Response {
    let metadata = input.metadata;
    let workspace_id = input.workspace_id;
    let reservation = input.reservation;
    let committed_response = input.committed_response;
    let response_status =
        StatusCode::from_u16(committed_response.status_code()).unwrap_or(StatusCode::OK);
    let body = committed_response.response_body().clone();

    let mut post_cas_input =
        DurableCoreCommitPostCasInput::new(input.audit_event, committed_response);
    if let Some(workspace_id) = workspace_id {
        post_cas_input = post_cas_input.with_workspace_id(workspace_id);
    }
    if let Some(reservation) = reservation.clone() {
        post_cas_input = post_cas_input.with_idempotency_reservation(reservation);
    }

    let envelope = match input
        .plan
        .post_cas_envelope(metadata, input.visibility, post_cas_input)
    {
        Ok(envelope) => envelope,
        Err(_) => {
            return guarded_durable_commit_visibility_unconfirmed_response();
        }
    };
    let post_visible_intent_millis = current_unix_timestamp_millis();
    let recovery_claims = match guarded_durable_commit_enqueue_post_visible_recovery_intents(
        &envelope,
        input.post_cas_stores.post_cas_recovery.as_ref(),
        post_visible_intent_millis,
    )
    .await
    {
        Ok(claims) => claims,
        Err(_) => return guarded_durable_commit_visibility_unconfirmed_response(),
    };

    match envelope
        .complete(
            input.post_cas_stores.workspace_metadata.as_ref(),
            input.post_cas_stores.audit.as_ref(),
            input.post_cas_stores.idempotency.as_ref(),
        )
        .await
    {
        DurableCorePostCasOutcome::Complete { .. } => {
            let _ = guarded_durable_commit_complete_post_visible_recovery_intents(
                input.post_cas_stores.post_cas_recovery.as_ref(),
                &recovery_claims,
            )
            .await;
            json_response(response_status, body)
        }
        DurableCorePostCasOutcome::Partial(partial) => {
            let now_millis = current_unix_timestamp_millis();
            if partial.failed_step() == DurableCorePostCasStep::IdempotencyCompletion {
                let _ = guarded_durable_commit_complete_post_visible_recovery_intent(
                    input.post_cas_stores.post_cas_recovery.as_ref(),
                    recovery_claims.workspace.as_ref(),
                )
                .await;
                let _ = guarded_durable_commit_complete_post_visible_recovery_intent(
                    input.post_cas_stores.post_cas_recovery.as_ref(),
                    Some(&recovery_claims.audit),
                )
                .await;
            } else {
                if partial.failed_step() == DurableCorePostCasStep::WorkspaceHeadUpdate {
                    let _ = guarded_durable_commit_record_post_visible_recovery_failure(
                        input.post_cas_stores.post_cas_recovery.as_ref(),
                        recovery_claims.workspace.as_ref(),
                        now_millis,
                    )
                    .await;
                } else {
                    let _ = guarded_durable_commit_complete_post_visible_recovery_intent(
                        input.post_cas_stores.post_cas_recovery.as_ref(),
                        recovery_claims.workspace.as_ref(),
                    )
                    .await;
                }
                if guarded_durable_commit_record_post_visible_recovery_failure(
                    input.post_cas_stores.post_cas_recovery.as_ref(),
                    Some(&recovery_claims.audit),
                    now_millis,
                )
                .await
                .is_err()
                {
                    return guarded_durable_commit_visibility_unconfirmed_response();
                }
            }

            if !partial.idempotency_completed() && envelope.has_idempotency_completion() {
                if guarded_durable_commit_replace_post_visible_idempotency_recovery(
                    &envelope,
                    input.post_cas_stores.post_cas_recovery.as_ref(),
                    recovery_claims.idempotency.as_ref(),
                    now_millis,
                )
                .await
                .is_err()
                {
                    return guarded_durable_commit_visibility_unconfirmed_response();
                }
                if partial.failed_step() != DurableCorePostCasStep::IdempotencyCompletion {
                    if envelope
                        .complete_partial_idempotency_replay(
                            input.post_cas_stores.idempotency.as_ref(),
                        )
                        .await
                        .is_ok()
                    {
                        let _ = guarded_durable_commit_complete_post_visible_recovery_intent(
                            input.post_cas_stores.post_cas_recovery.as_ref(),
                            recovery_claims.idempotency.as_ref(),
                        )
                        .await;
                    } else {
                        let _ = guarded_durable_commit_record_post_visible_recovery_failure(
                            input.post_cas_stores.post_cas_recovery.as_ref(),
                            recovery_claims.idempotency.as_ref(),
                            now_millis,
                        )
                        .await;
                    }
                } else {
                    let _ = guarded_durable_commit_record_post_visible_recovery_failure(
                        input.post_cas_stores.post_cas_recovery.as_ref(),
                        recovery_claims.idempotency.as_ref(),
                        now_millis,
                    )
                    .await;
                }
            }
            json_response(
                StatusCode::ACCEPTED,
                DurableCoreCommittedResponse::partial_body(),
            )
        }
    }
}

async fn guarded_durable_commit_enqueue_post_visible_recovery_intents(
    envelope: &DurableCoreCommitPostCasEnvelope,
    post_cas_recovery: &dyn DurableCorePostCasRecoveryClaimStore,
    now_millis: u64,
) -> Result<GuardedDurablePostCasRouteRecoveryClaims, VfsError> {
    let workspace = if envelope.has_workspace_head_update() {
        Some(
            guarded_durable_commit_enqueue_post_cas_recovery_claim(
                envelope,
                post_cas_recovery,
                DurableCorePostCasStep::WorkspaceHeadUpdate,
                None,
                now_millis,
            )
            .await?,
        )
    } else {
        None
    };
    let audit = guarded_durable_commit_enqueue_post_cas_recovery_claim(
        envelope,
        post_cas_recovery,
        DurableCorePostCasStep::AuditAppend,
        None,
        now_millis,
    )
    .await?;
    let idempotency = if envelope.has_idempotency_completion() {
        Some(
            guarded_durable_commit_enqueue_post_cas_recovery_claim(
                envelope,
                post_cas_recovery,
                DurableCorePostCasStep::IdempotencyCompletion,
                Some(DurableCorePostCasIdempotencyResponseKind::FullCommit),
                now_millis,
            )
            .await?,
        )
    } else {
        None
    };
    Ok(GuardedDurablePostCasRouteRecoveryClaims {
        workspace,
        audit,
        idempotency,
    })
}

async fn guarded_durable_commit_complete_post_visible_recovery_intents(
    post_cas_recovery: &dyn DurableCorePostCasRecoveryClaimStore,
    claims: &GuardedDurablePostCasRouteRecoveryClaims,
) -> Result<(), VfsError> {
    guarded_durable_commit_complete_post_visible_recovery_intent(
        post_cas_recovery,
        claims.workspace.as_ref(),
    )
    .await?;
    guarded_durable_commit_complete_post_visible_recovery_intent(
        post_cas_recovery,
        Some(&claims.audit),
    )
    .await?;
    guarded_durable_commit_complete_post_visible_recovery_intent(
        post_cas_recovery,
        claims.idempotency.as_ref(),
    )
    .await?;
    Ok(())
}

async fn guarded_durable_commit_complete_post_visible_recovery_intent(
    post_cas_recovery: &dyn DurableCorePostCasRecoveryClaimStore,
    claim: Option<&DurableCorePostCasRecoveryClaim>,
) -> Result<(), VfsError> {
    let Some(claim) = claim else {
        return Ok(());
    };
    post_cas_recovery
        .complete(claim, current_unix_timestamp_millis())
        .await
}

async fn guarded_durable_commit_record_post_visible_recovery_failure(
    post_cas_recovery: &dyn DurableCorePostCasRecoveryClaimStore,
    claim: Option<&DurableCorePostCasRecoveryClaim>,
    now_millis: u64,
) -> Result<(), VfsError> {
    let Some(claim) = claim else {
        return Ok(());
    };
    post_cas_recovery
        .record_failure(
            claim,
            "durable commit route post-CAS side effect failed",
            Duration::from_millis(1),
            now_millis.saturating_sub(1),
        )
        .await
}

async fn guarded_durable_commit_replace_post_visible_idempotency_recovery(
    envelope: &DurableCoreCommitPostCasEnvelope,
    post_cas_recovery: &dyn DurableCorePostCasRecoveryClaimStore,
    claim: Option<&DurableCorePostCasRecoveryClaim>,
    now_millis: u64,
) -> Result<(), VfsError> {
    let Some(claim) = claim else {
        return Ok(());
    };
    let context =
        envelope.recovery_context(Some(DurableCorePostCasIdempotencyResponseKind::Partial));
    post_cas_recovery
        .replace_claim_context(claim, context, now_millis)
        .await
}

async fn guarded_durable_commit_enqueue_post_cas_recovery_claim(
    envelope: &DurableCoreCommitPostCasEnvelope,
    post_cas_recovery: &dyn DurableCorePostCasRecoveryClaimStore,
    step: DurableCorePostCasStep,
    idempotency_response_kind: Option<DurableCorePostCasIdempotencyResponseKind>,
    now_millis: u64,
) -> Result<DurableCorePostCasRecoveryClaim, VfsError> {
    let target = envelope.recovery_target(step)?;
    let context = envelope.recovery_context(idempotency_response_kind);
    post_cas_recovery
        .enqueue_with_context_and_claim(
            target,
            context,
            VCS_RECOVERY_RUN_LEASE_OWNER,
            Duration::from_secs(30),
            now_millis,
        )
        .await?
        .ok_or_else(guarded_durable_commit_recovery_claim_unavailable)
}

fn guarded_durable_commit_recovery_claim_unavailable() -> VfsError {
    VfsError::CorruptStore {
        message: "durable commit post-CAS recovery claim is unavailable".to_string(),
    }
}

const VCS_RECOVERY_STUCK_INFO_AFTER_MILLIS: u64 = 5 * 60 * 1000;
const VCS_RECOVERY_STUCK_WARN_AFTER_MILLIS: u64 = 30 * 60 * 1000;
const VCS_RECOVERY_STUCK_AFTER_MILLIS: u64 = 2 * 60 * 60 * 1000;

fn recovery_stuck_tier(age_millis: u64) -> &'static str {
    if age_millis >= VCS_RECOVERY_STUCK_AFTER_MILLIS {
        "stuck"
    } else if age_millis >= VCS_RECOVERY_STUCK_WARN_AFTER_MILLIS {
        "warn"
    } else if age_millis >= VCS_RECOVERY_STUCK_INFO_AFTER_MILLIS {
        "info"
    } else {
        "ok"
    }
}

fn recovery_age_millis(now_millis: u64, timestamp_millis: Option<u64>) -> u64 {
    timestamp_millis.map_or(0, |timestamp| now_millis.saturating_sub(timestamp))
}

fn recovery_status_fields(
    state: &str,
    now_millis: u64,
    age_anchor_millis: Option<u64>,
    lease_expires_at_millis: Option<u64>,
    retry_after_millis: Option<u64>,
) -> JsonMap<String, JsonValue> {
    let stale_active = state == "stale_active"
        || (state == "active"
            && lease_expires_at_millis.is_some_and(|lease_expires| lease_expires <= now_millis));
    let due = match state {
        "pending" => true,
        "active" | "stale_active" => stale_active,
        "backing_off" => retry_after_millis.is_some_and(|retry_after| retry_after <= now_millis),
        _ => false,
    };
    let retryable = matches!(state, "pending" | "active" | "stale_active" | "backing_off") && due;
    let age_millis = recovery_age_millis(now_millis, age_anchor_millis);
    let mut fields = JsonMap::new();
    fields.insert("age_millis".to_string(), serde_json::json!(age_millis));
    fields.insert("stale_active".to_string(), serde_json::json!(stale_active));
    fields.insert("due".to_string(), serde_json::json!(due));
    fields.insert("retryable".to_string(), serde_json::json!(retryable));
    fields.insert(
        "stuck_tier".to_string(),
        serde_json::json!(recovery_stuck_tier(age_millis)),
    );
    fields.insert(
        "next_retry_at_millis".to_string(),
        serde_json::json!(retry_after_millis),
    );
    fields
}

fn recovery_row_with_classification(
    mut row: JsonMap<String, JsonValue>,
    state: &str,
    now_millis: u64,
    age_anchor_millis: Option<u64>,
    lease_expires_at_millis: Option<u64>,
    retry_after_millis: Option<u64>,
) -> JsonValue {
    row.extend(recovery_status_fields(
        state,
        now_millis,
        age_anchor_millis,
        lease_expires_at_millis,
        retry_after_millis,
    ));
    JsonValue::Object(row)
}

fn scheduler_health_json(status: Option<&super::DurableRecoverySchedulerStatus>) -> JsonValue {
    match status {
        Some(status) => serde_json::json!({
            "present": true,
            "started_at_millis": status.started_at_millis,
            "last_tick_at_millis": status.last_tick_at_millis,
            "last_outcome": status.last_outcome,
            "last_error": status.last_error,
            "phases": {
                "pre_visibility": scheduler_phase_json(&status.phases.pre_visibility),
                "post_cas": scheduler_phase_json(&status.phases.post_cas),
                "fs_mutations": scheduler_phase_json(&status.phases.fs_mutations),
                "object_cleanup": scheduler_phase_json(&status.phases.object_cleanup),
            },
        }),
        None => serde_json::json!({
            "present": false,
            "started_at_millis": null,
            "last_tick_at_millis": null,
            "last_outcome": null,
            "last_error": null,
            "phases": {
                "pre_visibility": scheduler_phase_json(&Default::default()),
                "post_cas": scheduler_phase_json(&Default::default()),
                "fs_mutations": scheduler_phase_json(&Default::default()),
                "object_cleanup": scheduler_phase_json(&Default::default()),
            },
        }),
    }
}

fn scheduler_phase_json(phase: &super::DurableRecoverySchedulerPhaseStatus) -> JsonValue {
    serde_json::json!({
        "attempted": phase.attempted,
        "completed": phase.completed,
        "backing_off": phase.backing_off,
        "poisoned": phase.poisoned,
        "skipped": phase.skipped,
        "deletion_ready": phase.deletion_ready,
        "deleted_final_objects": phase.deleted_final_objects,
        "deferred": phase.deferred,
    })
}

fn phase_summary(
    available: bool,
    rows: Vec<JsonValue>,
    counts: JsonValue,
    count: usize,
    unavailable_error: Option<&'static str>,
    terminal_count_key: &'static str,
) -> JsonValue {
    let due_count = rows
        .iter()
        .filter(|row| row["due"].as_bool().unwrap_or(false))
        .count();
    let stale_active_count = rows
        .iter()
        .filter(|row| row["stale_active"].as_bool().unwrap_or(false))
        .count();
    let terminal_count = counts[terminal_count_key].as_u64().unwrap_or(0);
    let oldest_age_millis = rows
        .iter()
        .filter_map(|row| row["age_millis"].as_u64())
        .max();
    let mut phase = serde_json::json!({
        "available": available,
        "counts": counts,
        "count": count,
        "page_count": rows.len(),
        "oldest_age_millis": oldest_age_millis,
        "due_count": due_count,
        "stale_active_count": stale_active_count,
        terminal_count_key: terminal_count,
        "rows": rows,
    });
    if let Some(error) = unavailable_error {
        phase["error"] = serde_json::json!(error);
    }
    phase[format!("{terminal_count_key}_count")] = serde_json::json!(terminal_count);
    phase
}

fn object_cleanup_claim_kind_as_str(kind: ObjectCleanupClaimKind) -> &'static str {
    match kind {
        ObjectCleanupClaimKind::FinalObjectMetadataRepair => "final_object_metadata_repair",
        ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup => {
            "durable_mutation_cas_lost_object_cleanup"
        }
    }
}

fn object_cleanup_state_as_str(state: ObjectCleanupClaimState) -> &'static str {
    match state {
        ObjectCleanupClaimState::Active => "active",
        ObjectCleanupClaimState::StaleActive => "stale_active",
        ObjectCleanupClaimState::Completed => "completed",
        ObjectCleanupClaimState::Failed => "failed",
    }
}

fn object_kind_as_str(kind: crate::store::ObjectKind) -> &'static str {
    match kind {
        crate::store::ObjectKind::Blob => "blob",
        crate::store::ObjectKind::Tree => "tree",
        crate::store::ObjectKind::Commit => "commit",
    }
}

async fn object_gc_dry_run_status(
    stores: &StratumStores,
    repo_id: &RepoId,
    limit: usize,
) -> JsonValue {
    let dry_run = ObjectGcDryRun::new(
        stores.objects.as_ref(),
        stores.commits.as_ref(),
        stores.refs.as_ref(),
        stores.workspace_metadata.as_ref(),
        stores.review.as_ref(),
        stores.idempotency.as_ref(),
        stores.post_cas_recovery.as_ref(),
        stores.pre_visibility_recovery.as_ref(),
        stores.fs_mutation_recovery.as_ref(),
        stores.object_cleanup.as_ref(),
    );
    match dry_run.run(repo_id, limit, None).await {
        Ok(report) => {
            let blockers = report
                .blockers
                .iter()
                .map(|blocker| {
                    serde_json::json!({
                        "source": blocker.source,
                        "reason": blocker.reason,
                    })
                })
                .collect::<Vec<_>>();
            let unreachable_commits = report
                .unreachable_commits
                .iter()
                .map(|candidate| {
                    serde_json::json!({
                        "commit_id": candidate.commit_id_prefix,
                        "root_tree": candidate.root_tree_prefix,
                        "parent_count": candidate.parent_count,
                        "changed_path_count": candidate.changed_path_count,
                    })
                })
                .collect::<Vec<_>>();
            let unreachable_objects = report
                .unreachable_objects
                .iter()
                .map(|candidate| {
                    serde_json::json!({
                        "object_kind": object_kind_as_str(candidate.object_kind),
                        "object_id": candidate.object_id_prefix,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "available": true,
                "mode": "dry_run",
                "repo_id": repo_id.as_str(),
                "limit": limit,
                "deletion_enabled": false,
                "deletion_reason": "dry_run_only",
                "deletion_ready": 0,
                "deletion_ready_reason": "requires_fenced_cleanup_worker",
                "blocked": !blockers.is_empty(),
                "retained_commit_count": report.roots.commit_root_count(),
                "retained_object_count": report.roots.object_root_count(),
                "cleanup_candidate_count": report.roots.cleanup_candidate_count(),
                "unreachable_cleanup_candidate_count": if blockers.is_empty() {
                    unreachable_objects.len()
                } else {
                    0
                },
                "unreachable_commit_count": unreachable_commits.len(),
                "unreachable_object_count": unreachable_objects.len(),
                "unreachable_commits": unreachable_commits,
                "unreachable_objects": unreachable_objects,
                "blockers": blockers,
            })
        }
        Err(_) => serde_json::json!({
            "available": false,
            "mode": "dry_run",
            "repo_id": repo_id.as_str(),
            "limit": limit,
            "deletion_enabled": false,
            "deletion_reason": "dry_run_unavailable",
            "deletion_ready": 0,
            "deletion_ready_reason": "requires_fenced_cleanup_worker",
            "blocked": true,
            "unreachable_cleanup_candidate_count": 0,
            "unreachable_commit_count": 0,
            "unreachable_object_count": 0,
            "unreachable_commits": [],
            "unreachable_objects": [],
            "blockers": [{
                "source": "object_gc",
                "reason": "dry_run_failed",
            }],
        }),
    }
}

#[derive(Default)]
struct RecoveryRefBlocker {
    repo_id: String,
    ref_name: String,
    phases: Vec<&'static str>,
    pending: u64,
    active: u64,
    stale_active: u64,
    backing_off: u64,
    poisoned: u64,
    retryable: u64,
    next_retry_at_millis: Option<u64>,
}

impl RecoveryRefBlocker {
    fn add_row(&mut self, phase: &'static str, row: &JsonValue) {
        if !self.phases.contains(&phase) {
            self.phases.push(phase);
        }
        match row["state"].as_str().unwrap_or_default() {
            "pending" => self.pending += 1,
            "active" => self.active += 1,
            "backing_off" => self.backing_off += 1,
            "poisoned" => self.poisoned += 1,
            _ => {}
        }
        if row["stale_active"].as_bool().unwrap_or(false) {
            self.stale_active += 1;
        }
        if row["retryable"].as_bool().unwrap_or(false) {
            self.retryable += 1;
        }
        if let Some(next_retry) = row["next_retry_at_millis"].as_u64() {
            self.next_retry_at_millis = Some(
                self.next_retry_at_millis
                    .map_or(next_retry, |existing| existing.min(next_retry)),
            );
        }
    }

    fn reason(&self) -> &'static str {
        if self.poisoned > 0 {
            "poisoned_recovery"
        } else if self.stale_active > 0 {
            "stale_active_recovery"
        } else {
            "unresolved_recovery"
        }
    }

    fn into_json(self) -> JsonValue {
        serde_json::json!({
            "repo_id": self.repo_id,
            "ref_name": self.ref_name,
            "blocked": true,
            "reason": self.reason(),
            "phases": self.phases,
            "pending": self.pending,
            "active": self.active,
            "stale_active": self.stale_active,
            "backing_off": self.backing_off,
            "poisoned": self.poisoned,
            "retryable": self.retryable,
            "next_retry_at_millis": self.next_retry_at_millis,
        })
    }
}

#[derive(Default)]
struct RecoveryWorkspaceBlocker {
    workspace_scope: String,
    target_ref: String,
    phases: Vec<&'static str>,
    operation_count: u64,
    poisoned: u64,
    stale_active: u64,
    retryable: u64,
    next_retry_at_millis: Option<u64>,
}

impl RecoveryWorkspaceBlocker {
    fn add_row(&mut self, phase: &'static str, row: &JsonValue) {
        if !self.phases.contains(&phase) {
            self.phases.push(phase);
        }
        self.operation_count += 1;
        if row["state"].as_str() == Some("poisoned") {
            self.poisoned += 1;
        }
        if row["stale_active"].as_bool().unwrap_or(false) {
            self.stale_active += 1;
        }
        if row["retryable"].as_bool().unwrap_or(false) {
            self.retryable += 1;
        }
        if let Some(next_retry) = row["next_retry_at_millis"].as_u64() {
            self.next_retry_at_millis = Some(
                self.next_retry_at_millis
                    .map_or(next_retry, |existing| existing.min(next_retry)),
            );
        }
    }

    fn into_json(self) -> JsonValue {
        serde_json::json!({
            "workspace_scope": self.workspace_scope,
            "target_ref": self.target_ref,
            "blocked": true,
            "phases": self.phases,
            "operation_count": self.operation_count,
            "poisoned": self.poisoned,
            "stale_active": self.stale_active,
            "retryable": self.retryable,
            "next_retry_at_millis": self.next_retry_at_millis,
        })
    }
}

fn build_ref_blockers(
    pre_visibility_rows: &[JsonValue],
    post_cas_rows: &[JsonValue],
) -> Vec<JsonValue> {
    let mut blockers = std::collections::BTreeMap::<(String, String), RecoveryRefBlocker>::new();
    for (phase, rows) in [
        ("pre_visibility", pre_visibility_rows),
        ("post_cas", post_cas_rows),
    ] {
        for row in rows {
            let state = row["state"].as_str().unwrap_or_default();
            if matches!(state, "completed" | "resolved")
                || row["ref_name"].as_str() != Some(MAIN_REF)
            {
                continue;
            }
            let repo_id = row["repo_id"].as_str().unwrap_or_default().to_string();
            let ref_name = row["ref_name"].as_str().unwrap_or_default().to_string();
            let blocker = blockers
                .entry((repo_id.clone(), ref_name.clone()))
                .or_insert_with(|| RecoveryRefBlocker {
                    repo_id,
                    ref_name,
                    ..RecoveryRefBlocker::default()
                });
            blocker.add_row(phase, row);
        }
    }
    blockers
        .into_values()
        .map(RecoveryRefBlocker::into_json)
        .collect()
}

fn build_workspace_blockers(fs_mutation_rows: &[JsonValue]) -> Vec<JsonValue> {
    let mut blockers =
        std::collections::BTreeMap::<(String, String), RecoveryWorkspaceBlocker>::new();
    for row in fs_mutation_rows {
        if matches!(
            row["state"].as_str().unwrap_or_default(),
            "completed" | "resolved"
        ) {
            continue;
        }
        let Some(workspace_scope) = row["workspace_scope"].as_str() else {
            continue;
        };
        let target_ref = row["target_ref"].as_str().unwrap_or_default().to_string();
        let blocker = blockers
            .entry((workspace_scope.to_string(), target_ref.clone()))
            .or_insert_with(|| RecoveryWorkspaceBlocker {
                workspace_scope: workspace_scope.to_string(),
                target_ref,
                ..RecoveryWorkspaceBlocker::default()
            });
        blocker.add_row("fs_mutations", row);
    }
    blockers
        .into_values()
        .map(RecoveryWorkspaceBlocker::into_json)
        .collect()
}

fn recovery_run_correlation_id() -> String {
    format!("rec_{}", Uuid::new_v4().simple())
}

fn pre_visibility_remaining(counts: &DurableCorePreVisibilityRecoveryCounts) -> usize {
    counts.pending() + counts.active() + counts.backing_off() + counts.poisoned()
}

fn post_cas_remaining(counts: &DurableCorePostCasRecoveryCounts) -> usize {
    counts.pending() + counts.active() + counts.backing_off() + counts.poisoned()
}

fn fs_mutation_remaining(counts: &DurableFsMutationRecoveryCounts) -> usize {
    counts.pending() + counts.active() + counts.backing_off() + counts.poisoned()
}

fn object_cleanup_remaining(counts: &ObjectCleanupClaimCounts) -> usize {
    counts.active() + counts.stale_active() + counts.failed()
}

fn object_cleanup_skipped(summary: &ObjectCleanupWorkerSummary) -> usize {
    summary.skipped_non_cas_lost
        + summary.skipped_reachable
        + summary.skipped_blocked
        + summary.skipped_claim_unavailable
}

fn object_cleanup_deferred(summary: &ObjectCleanupWorkerSummary) -> usize {
    summary.skipped_blocked + summary.skipped_claim_unavailable
}

async fn vcs_recovery_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    scheduler: Option<Extension<Arc<DurableRecoverySchedulerHandle>>>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let Some(capability) = state.core.guarded_durable_commit_route() else {
        return err_json(
            StatusCode::NOT_IMPLEMENTED,
            "guarded durable commit recovery is not enabled",
        )
        .into_response();
    };

    let now_millis = current_unix_timestamp_millis();
    let stores = capability.stores();
    let recovery_store = stores.post_cas_recovery.as_ref();
    let (statuses, aggregate_counts) = match (
        recovery_store.list(100).await,
        recovery_store.counts().await,
    ) {
        (Ok(statuses), Ok(aggregate_counts)) => (statuses, aggregate_counts),
        (Err(_), _) | (_, Err(_)) => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "durable commit recovery status unavailable",
            )
            .into_response();
        }
    };

    let counts = serde_json::json!({
        "pending": aggregate_counts.pending(),
        "active": aggregate_counts.active(),
        "backing_off": aggregate_counts.backing_off(),
        "completed": aggregate_counts.completed(),
        "poisoned": aggregate_counts.poisoned(),
    });
    let rows = statuses
        .iter()
        .map(|status| {
            let state = status.state().as_str();
            let mut row = JsonMap::new();
            row.insert(
                "repo_id".to_string(),
                serde_json::json!(status.target().repo_id().as_str()),
            );
            row.insert(
                "ref_name".to_string(),
                serde_json::json!(status.target().ref_name()),
            );
            row.insert(
                "commit_id".to_string(),
                serde_json::json!(status.target().commit_id().to_hex()),
            );
            row.insert(
                "step".to_string(),
                serde_json::json!(status.target().step().as_str()),
            );
            row.insert("state".to_string(), serde_json::json!(state));
            row.insert("attempts".to_string(), serde_json::json!(status.attempts()));
            row.insert(
                "created_at_millis".to_string(),
                serde_json::json!(status.created_at_millis()),
            );
            row.insert(
                "updated_at_millis".to_string(),
                serde_json::json!(status.updated_at_millis()),
            );
            row.insert(
                "lease_expires_at_millis".to_string(),
                serde_json::json!(status.lease_expires_at_millis()),
            );
            row.insert(
                "retry_after_millis".to_string(),
                serde_json::json!(status.retry_after_millis()),
            );
            row.insert(
                "terminal_at_millis".to_string(),
                serde_json::json!(status.terminal_at_millis()),
            );
            row.insert(
                "diagnosis".to_string(),
                serde_json::json!(status.redacted_diagnosis()),
            );
            let age_anchor = match state {
                "pending" => status.created_at_millis(),
                "active" | "backing_off" => status.updated_at_millis(),
                "completed" | "poisoned" => {
                    status.terminal_at_millis().or(status.updated_at_millis())
                }
                _ => status.updated_at_millis().or(status.created_at_millis()),
            };
            recovery_row_with_classification(
                row,
                state,
                now_millis,
                age_anchor,
                status.lease_expires_at_millis(),
                status.retry_after_millis(),
            )
        })
        .collect::<Vec<_>>();
    let post_cas_phase = phase_summary(
        true,
        rows.clone(),
        counts.clone(),
        aggregate_counts.total(),
        None,
        "poisoned",
    );

    let pre_visibility_store = stores.pre_visibility_recovery.as_ref();
    let pre_visibility = match (
        pre_visibility_store.list(100).await,
        pre_visibility_store.counts().await,
    ) {
        (Ok(pre_visibility_statuses), Ok(pre_visibility_aggregate_counts)) => {
            let pre_visibility_counts = serde_json::json!({
                "pending": pre_visibility_aggregate_counts.pending(),
                "active": pre_visibility_aggregate_counts.active(),
                "backing_off": pre_visibility_aggregate_counts.backing_off(),
                "resolved": pre_visibility_aggregate_counts.resolved(),
                "poisoned": pre_visibility_aggregate_counts.poisoned(),
            });
            let pre_visibility_rows = pre_visibility_statuses
                .iter()
                .map(|status| {
                    let state = status.state().as_str();
                    let mut row = JsonMap::new();
                    row.insert(
                        "repo_id".to_string(),
                        serde_json::json!(status.target().repo_id().as_str()),
                    );
                    row.insert(
                        "ref_name".to_string(),
                        serde_json::json!(status.target().ref_name()),
                    );
                    row.insert(
                        "commit_id".to_string(),
                        serde_json::json!(status.target().commit_id().to_hex()),
                    );
                    row.insert(
                        "stage".to_string(),
                        serde_json::json!(status.target().stage().as_str()),
                    );
                    row.insert("state".to_string(), serde_json::json!(state));
                    row.insert(
                        "root_tree_id".to_string(),
                        serde_json::json!(status.root_tree_id().to_hex()),
                    );
                    row.insert(
                        "parent_commit_id".to_string(),
                        serde_json::json!(
                            status
                                .parent_commit_id()
                                .map(|commit_id| commit_id.to_hex())
                        ),
                    );
                    row.insert(
                        "expected_ref_version".to_string(),
                        serde_json::json!(status.expected_ref_version().value()),
                    );
                    row.insert(
                        "object_count".to_string(),
                        serde_json::json!(status.object_count()),
                    );
                    row.insert(
                        "changed_path_count".to_string(),
                        serde_json::json!(status.changed_path_count()),
                    );
                    row.insert(
                        "has_idempotency_reservation".to_string(),
                        serde_json::json!(status.has_idempotency_reservation()),
                    );
                    row.insert(
                        "first_seen_at_millis".to_string(),
                        serde_json::json!(status.first_seen_at_millis()),
                    );
                    row.insert(
                        "last_seen_at_millis".to_string(),
                        serde_json::json!(status.last_seen_at_millis()),
                    );
                    row.insert(
                        "occurrence_count".to_string(),
                        serde_json::json!(status.occurrence_count()),
                    );
                    row.insert("attempts".to_string(), serde_json::json!(status.attempts()));
                    row.insert(
                        "lease_expires_at_millis".to_string(),
                        serde_json::json!(status.lease_expires_at_millis()),
                    );
                    row.insert(
                        "retry_after_millis".to_string(),
                        serde_json::json!(status.retry_after_millis()),
                    );
                    row.insert(
                        "terminal_at_millis".to_string(),
                        serde_json::json!(status.terminal_at_millis()),
                    );
                    row.insert(
                        "diagnosis".to_string(),
                        serde_json::json!(status.redacted_diagnosis()),
                    );
                    row.insert(
                        "has_recovery_context".to_string(),
                        serde_json::json!(status.has_post_cas_context()),
                    );
                    let age_anchor = Some(status.first_seen_at_millis())
                        .or(status.terminal_at_millis())
                        .or(status.retry_after_millis())
                        .or(status.lease_expires_at_millis());
                    recovery_row_with_classification(
                        row,
                        state,
                        now_millis,
                        age_anchor,
                        status.lease_expires_at_millis(),
                        status.retry_after_millis(),
                    )
                })
                .collect::<Vec<_>>();
            phase_summary(
                true,
                pre_visibility_rows,
                pre_visibility_counts,
                pre_visibility_aggregate_counts.total(),
                None,
                "poisoned",
            )
        }
        (Err(_), _) | (_, Err(_)) => phase_summary(
            false,
            Vec::new(),
            serde_json::json!({
                "pending": 0,
                "active": 0,
                "backing_off": 0,
                "resolved": 0,
                "poisoned": 0,
            }),
            0,
            Some("pre-visibility recovery status unavailable"),
            "poisoned",
        ),
    };
    let pre_visibility_rows = pre_visibility["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let fs_mutation_store = stores.fs_mutation_recovery.as_ref();
    let fs_mutations = match (
        fs_mutation_store.list(100).await,
        fs_mutation_store.counts().await,
    ) {
        (Ok(fs_mutation_statuses), Ok(fs_mutation_aggregate_counts)) => {
            let fs_mutation_counts = serde_json::json!({
                "pending": fs_mutation_aggregate_counts.pending(),
                "active": fs_mutation_aggregate_counts.active(),
                "backing_off": fs_mutation_aggregate_counts.backing_off(),
                "completed": fs_mutation_aggregate_counts.completed(),
                "poisoned": fs_mutation_aggregate_counts.poisoned(),
            });
            let fs_mutation_rows = fs_mutation_statuses
                .iter()
                .map(|status| {
                    let state = status.state().as_str();
                    let mut row = JsonMap::new();
                    row.insert(
                        "repo_id".to_string(),
                        serde_json::json!(status.target().repo_id().as_str()),
                    );
                    row.insert(
                        "workspace_scope".to_string(),
                        serde_json::json!(status.target().workspace_scope()),
                    );
                    row.insert(
                        "operation_id".to_string(),
                        serde_json::json!(status.target().operation_id()),
                    );
                    row.insert(
                        "target_ref".to_string(),
                        serde_json::json!(status.target().target_ref()),
                    );
                    row.insert(
                        "previous_commit".to_string(),
                        serde_json::json!(status.target().previous_commit().to_hex()),
                    );
                    row.insert(
                        "new_commit".to_string(),
                        serde_json::json!(status.target().new_commit().to_hex()),
                    );
                    row.insert(
                        "failed_step".to_string(),
                        serde_json::json!(status.target().failed_step().as_str()),
                    );
                    row.insert("state".to_string(), serde_json::json!(state));
                    row.insert("attempts".to_string(), serde_json::json!(status.attempts()));
                    row.insert(
                        "lease_expires_at_millis".to_string(),
                        serde_json::json!(status.lease_expires_at_millis()),
                    );
                    row.insert(
                        "retry_after_millis".to_string(),
                        serde_json::json!(status.retry_after_millis()),
                    );
                    row.insert(
                        "terminal_at_millis".to_string(),
                        serde_json::json!(status.terminal_at_millis()),
                    );
                    row.insert(
                        "diagnosis".to_string(),
                        serde_json::json!(status.redacted_diagnosis()),
                    );
                    let age_anchor = match state {
                        "pending" => status.created_at_millis(),
                        "active" | "backing_off" => status.updated_at_millis(),
                        "completed" | "poisoned" => {
                            status.terminal_at_millis().or(status.updated_at_millis())
                        }
                        _ => status.updated_at_millis().or(status.created_at_millis()),
                    };
                    recovery_row_with_classification(
                        row,
                        state,
                        now_millis,
                        age_anchor,
                        status.lease_expires_at_millis(),
                        status.retry_after_millis(),
                    )
                })
                .collect::<Vec<_>>();
            phase_summary(
                true,
                fs_mutation_rows,
                fs_mutation_counts,
                fs_mutation_aggregate_counts.total(),
                None,
                "poisoned",
            )
        }
        (Err(_), _) | (_, Err(_)) => phase_summary(
            false,
            Vec::new(),
            serde_json::json!({
                "pending": 0,
                "active": 0,
                "backing_off": 0,
                "completed": 0,
                "poisoned": 0,
            }),
            0,
            Some("durable FS mutation recovery status unavailable"),
            "poisoned",
        ),
    };
    let fs_mutation_rows = fs_mutations["rows"].as_array().cloned().unwrap_or_default();
    let mut object_cleanup = match (
        stores
            .object_cleanup
            .list_for_repo(capability.repo_id(), 100)
            .await,
        stores
            .object_cleanup
            .counts_for_repo(capability.repo_id())
            .await,
    ) {
        (Ok(cleanup_statuses), Ok(cleanup_counts)) => {
            let counts = serde_json::json!({
                "active": cleanup_counts.active(),
                "stale_active": cleanup_counts.stale_active(),
                "completed": cleanup_counts.completed(),
                "failed": cleanup_counts.failed(),
            });
            let rows = cleanup_statuses
                .iter()
                .map(|status| {
                    let state = object_cleanup_state_as_str(status.state());
                    let lease_expires_at_millis = status
                        .lease_expires_at()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
                    let created_at_millis = status
                        .created_at()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
                    let completed_at_millis = status
                        .completed_at()
                        .and_then(|completed_at| completed_at.duration_since(UNIX_EPOCH).ok())
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
                    let updated_at_millis = status
                        .updated_at()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
                    let mut row = JsonMap::new();
                    row.insert(
                        "repo_id".to_string(),
                        serde_json::json!(status.repo_id().as_str()),
                    );
                    row.insert(
                        "claim_kind".to_string(),
                        serde_json::json!(object_cleanup_claim_kind_as_str(status.claim_kind())),
                    );
                    row.insert(
                        "object_kind".to_string(),
                        serde_json::json!(object_kind_as_str(status.object_kind())),
                    );
                    row.insert(
                        "object_id".to_string(),
                        serde_json::json!(status.object_id().short_hex()),
                    );
                    row.insert("state".to_string(), serde_json::json!(state));
                    row.insert("attempts".to_string(), serde_json::json!(status.attempts()));
                    row.insert(
                        "lease_expires_at_millis".to_string(),
                        serde_json::json!(lease_expires_at_millis),
                    );
                    row.insert(
                        "completed_at_millis".to_string(),
                        serde_json::json!(completed_at_millis),
                    );
                    row.insert(
                        "created_at_millis".to_string(),
                        serde_json::json!(created_at_millis),
                    );
                    row.insert(
                        "updated_at_millis".to_string(),
                        serde_json::json!(updated_at_millis),
                    );
                    row.insert(
                        "has_last_failure".to_string(),
                        serde_json::json!(status.has_last_failure()),
                    );
                    row.insert("is_stale".to_string(), serde_json::json!(status.is_stale()));
                    let age_anchor = match state {
                        "completed" => completed_at_millis.or(updated_at_millis),
                        "failed" => updated_at_millis.or(created_at_millis),
                        "active" | "stale_active" => updated_at_millis.or(created_at_millis),
                        _ => updated_at_millis.or(created_at_millis),
                    };
                    let mut row = recovery_row_with_classification(
                        row,
                        state,
                        now_millis,
                        age_anchor,
                        lease_expires_at_millis,
                        None,
                    );
                    if state == "failed"
                        && status.is_stale()
                        && let JsonValue::Object(fields) = &mut row
                    {
                        fields.insert("due".to_string(), serde_json::json!(true));
                        fields.insert("retryable".to_string(), serde_json::json!(true));
                    }
                    row
                })
                .collect::<Vec<_>>();
            phase_summary(true, rows, counts, cleanup_counts.total(), None, "failed")
        }
        (Err(_), _) | (_, Err(_)) => phase_summary(
            false,
            Vec::new(),
            serde_json::json!({
                "active": 0,
                "stale_active": 0,
                "completed": 0,
                "failed": 0,
            }),
            0,
            Some("object cleanup recovery status unavailable"),
            "failed",
        ),
    };
    let object_gc_dry_run = object_gc_dry_run_status(stores, capability.repo_id(), 100).await;
    if let Some(object) = object_cleanup.as_object_mut() {
        object.insert("deletion_enabled".to_string(), serde_json::json!(false));
        object.insert("deletion_ready".to_string(), serde_json::json!(0));
        object.insert(
            "deletion_ready_reason".to_string(),
            serde_json::json!("requires_fenced_cleanup_worker"),
        );
        object.insert("gc_dry_run".to_string(), object_gc_dry_run.clone());
    }
    let object_cleanup_rows = object_cleanup["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let ref_blockers = build_ref_blockers(&pre_visibility_rows, &rows);
    let workspace_blockers = build_workspace_blockers(&fs_mutation_rows);
    let has_unavailable_store = !pre_visibility["available"].as_bool().unwrap_or(false)
        || !fs_mutations["available"].as_bool().unwrap_or(false)
        || !object_cleanup["available"].as_bool().unwrap_or(false);
    let object_cleanup_unhealthy = object_cleanup["failed_count"].as_u64().unwrap_or(0) > 0
        || object_cleanup["stale_active_count"].as_u64().unwrap_or(0) > 0;
    let object_gc_unhealthy = object_gc_dry_run["available"].as_bool() != Some(true)
        || object_gc_dry_run["blocked"].as_bool().unwrap_or(true);
    let health_status = if has_unavailable_store
        || !ref_blockers.is_empty()
        || !workspace_blockers.is_empty()
        || object_cleanup_unhealthy
        || object_gc_unhealthy
    {
        "degraded"
    } else {
        "ok"
    };
    let scheduler_status = scheduler.as_ref().map(|Extension(handle)| handle.status());
    Json(serde_json::json!({
        "recovery": rows,
        "counts": counts,
        "count": aggregate_counts.total(),
        "page_count": rows.len(),
        "pre_visibility": pre_visibility_rows,
        "pre_visibility_counts": pre_visibility["counts"].clone(),
        "pre_visibility_count": pre_visibility["count"].clone(),
        "pre_visibility_page_count": pre_visibility["page_count"].clone(),
        "pre_visibility_available": pre_visibility["available"].clone(),
        "pre_visibility_error": pre_visibility.get("error").cloned(),
        "fs_mutations": fs_mutation_rows,
        "fs_mutation_counts": fs_mutations["counts"].clone(),
        "fs_mutation_count": fs_mutations["count"].clone(),
        "fs_mutation_page_count": fs_mutations["page_count"].clone(),
        "fs_mutation_available": fs_mutations["available"].clone(),
        "fs_mutation_error": fs_mutations.get("error").cloned(),
        "health": {
            "status": health_status,
            "backend_mode": "durable",
            "guarded_durable_enabled": true,
            "scheduler": scheduler_health_json(scheduler_status.as_ref()),
            "stores": {
                "post_cas": { "available": true },
                "pre_visibility": {
                    "available": pre_visibility["available"].clone(),
                    "error": pre_visibility.get("error").cloned(),
                },
                "fs_mutations": {
                    "available": fs_mutations["available"].clone(),
                    "error": fs_mutations.get("error").cloned(),
                },
                "object_cleanup": {
                    "available": object_cleanup["available"].clone(),
                    "error": object_cleanup.get("error").cloned(),
                },
            },
        },
        "phases": {
            "pre_visibility": pre_visibility,
            "post_cas": post_cas_phase,
            "fs_mutations": fs_mutations,
            "object_cleanup": object_cleanup,
        },
        "blockers": {
            "refs": ref_blockers,
            "workspaces": workspace_blockers,
        },
        "object_cleanup": object_cleanup_rows,
        "limit": 100,
    }))
    .into_response()
}

async fn vcs_recovery_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let Some(capability) = state.core.guarded_durable_commit_route() else {
        return err_json(
            StatusCode::NOT_IMPLEMENTED,
            "guarded durable commit recovery is not enabled",
        )
        .into_response();
    };

    let limit = match recovery_run_limit_from_body(&body) {
        Ok(limit) => limit,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                .into_response();
        }
    };
    let correlation_id = recovery_run_correlation_id();
    let stores = capability.stores();
    let pre_visibility_runner = DurableCorePreVisibilityRecoveryRun::new(
        DurableCorePreVisibilityRecoveryRunStores::new(
            stores.pre_visibility_recovery.as_ref(),
            stores.post_cas_recovery.as_ref(),
            stores.commits.as_ref(),
            stores.refs.as_ref(),
            stores.idempotency.as_ref(),
        ),
        VCS_RECOVERY_RUN_LEASE_OWNER,
        std::time::Duration::from_secs(30),
        limit,
    );
    let pre_visibility_summary = match pre_visibility_runner.run().await {
        Ok(summary) => summary,
        Err(_) => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "durable commit recovery run failed",
            )
            .into_response();
        }
    };
    let post_cas_limit = limit.saturating_sub(pre_visibility_summary.attempted());
    let worker = DurableCorePostCasRepairWorker::new(
        DurableCorePostCasRepairWorkerStores::new(
            stores.post_cas_recovery.as_ref(),
            stores.commits.as_ref(),
            stores.workspace_metadata.as_ref(),
            stores.audit.as_ref(),
            stores.idempotency.as_ref(),
        ),
        VCS_RECOVERY_RUN_LEASE_OWNER,
        std::time::Duration::from_secs(30),
        post_cas_limit,
    );

    let post_cas_summary = match worker.run().await {
        Ok(summary) => summary,
        Err(_) => {
            return err_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "durable commit recovery run failed",
            )
            .into_response();
        }
    };

    let fs_mutation_limit = post_cas_limit.saturating_sub(post_cas_summary.attempted());
    let fs_mutation_worker = DurableFsMutationRecoveryWorker::new(
        stores.fs_mutation_recovery.as_ref(),
        stores.audit.as_ref(),
        stores.idempotency.as_ref(),
        Some(stores.workspace_metadata.as_ref()),
        VCS_FS_MUTATION_RECOVERY_RUN_LEASE_OWNER,
        std::time::Duration::from_secs(30),
        fs_mutation_limit,
    );
    match fs_mutation_worker.run().await {
        Ok(fs_mutation_summary) => {
            let object_cleanup_limit =
                fs_mutation_limit.saturating_sub(fs_mutation_summary.attempted());
            let object_cleanup_worker = ObjectCleanupWorker::new(
                capability.repo_id(),
                stores.objects.as_ref(),
                stores.object_metadata.as_ref(),
                stores.commits.as_ref(),
                stores.refs.as_ref(),
                stores.workspace_metadata.as_ref(),
                stores.review.as_ref(),
                stores.idempotency.as_ref(),
                stores.post_cas_recovery.as_ref(),
                stores.pre_visibility_recovery.as_ref(),
                stores.fs_mutation_recovery.as_ref(),
                stores.object_cleanup.as_ref(),
            );
            let object_cleanup_summary =
                match object_cleanup_worker.run_once(object_cleanup_limit).await {
                    Ok(summary) => summary,
                    Err(_) => {
                        return err_json(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "object cleanup recovery run failed",
                        )
                        .into_response();
                    }
                };
            let (pre_visibility_counts, post_cas_counts, fs_mutation_counts, object_cleanup_counts) =
                match (
                    stores
                        .pre_visibility_recovery
                        .counts_for_repo(capability.repo_id())
                        .await,
                    stores
                        .post_cas_recovery
                        .counts_for_repo(capability.repo_id())
                        .await,
                    stores
                        .fs_mutation_recovery
                        .counts_for_repo(capability.repo_id())
                        .await,
                    stores
                        .object_cleanup
                        .counts_for_repo(capability.repo_id())
                        .await,
                ) {
                    (Ok(pre_visibility), Ok(post_cas), Ok(fs_mutation), Ok(object_cleanup)) => {
                        (pre_visibility, post_cas, fs_mutation, object_cleanup)
                    }
                    (Err(_), _, _, _)
                    | (_, Err(_), _, _)
                    | (_, _, Err(_), _)
                    | (_, _, _, Err(_)) => {
                        return err_json(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "durable recovery remaining status unavailable",
                        )
                        .into_response();
                    }
                };
            let pre_visibility_remaining = pre_visibility_remaining(&pre_visibility_counts);
            let post_cas_remaining = post_cas_remaining(&post_cas_counts);
            let fs_mutation_remaining = fs_mutation_remaining(&fs_mutation_counts);
            let object_cleanup_remaining = object_cleanup_remaining(&object_cleanup_counts);
            let remaining = pre_visibility_remaining
                + post_cas_remaining
                + fs_mutation_remaining
                + object_cleanup_remaining;
            let attempted = pre_visibility_summary.attempted()
                + post_cas_summary.attempted()
                + fs_mutation_summary.attempted()
                + object_cleanup_summary.processed;
            let completed = pre_visibility_summary.resolved()
                + post_cas_summary.completed()
                + fs_mutation_summary.completed()
                + object_cleanup_summary.deleted_final_objects;
            let backing_off = pre_visibility_summary.backing_off()
                + post_cas_summary.backing_off()
                + fs_mutation_summary.backing_off()
                + object_cleanup_summary.retryable_failures;
            let poisoned = pre_visibility_summary.poisoned()
                + post_cas_summary.poisoned()
                + fs_mutation_summary.poisoned()
                + object_cleanup_summary.poisoned;
            let object_cleanup_skipped = object_cleanup_skipped(&object_cleanup_summary);
            let object_cleanup_deferred = object_cleanup_deferred(&object_cleanup_summary);
            let skipped = pre_visibility_summary.skipped()
                + post_cas_summary.skipped()
                + fs_mutation_summary.skipped()
                + object_cleanup_skipped;
            let message = if remaining == 0 {
                "bounded recovery run completed with no persisted work remaining"
            } else {
                "bounded recovery run completed with persisted work remaining"
            };
            let body = serde_json::json!({
                "correlation_id": correlation_id,
                "requested_limit": limit,
                "limit": post_cas_summary.limit(),
                "scanned": pre_visibility_summary.scanned()
                    + post_cas_summary.scanned()
                    + fs_mutation_summary.scanned()
                    + object_cleanup_summary.candidates_listed,
                "attempted": attempted,
                "completed": completed,
                "backing_off": backing_off,
                "poisoned": poisoned,
                "skipped": skipped,
                "remaining": remaining,
                "converged": remaining == 0,
                "message": message,
                "phases": {
                    "pre_visibility": {
                        "limit": pre_visibility_summary.limit(),
                        "scanned": pre_visibility_summary.scanned(),
                        "attempted": pre_visibility_summary.attempted(),
                        "completed": pre_visibility_summary.resolved(),
                        "backing_off": pre_visibility_summary.backing_off(),
                        "poisoned": pre_visibility_summary.poisoned(),
                        "skipped": pre_visibility_summary.skipped(),
                        "remaining": pre_visibility_remaining,
                    },
                    "post_cas": {
                        "limit": post_cas_summary.limit(),
                        "scanned": post_cas_summary.scanned(),
                        "attempted": post_cas_summary.attempted(),
                        "completed": post_cas_summary.completed(),
                        "backing_off": post_cas_summary.backing_off(),
                        "poisoned": post_cas_summary.poisoned(),
                        "skipped": post_cas_summary.skipped(),
                        "remaining": post_cas_remaining,
                    },
                    "fs_mutations": {
                        "limit": fs_mutation_summary.limit(),
                        "scanned": fs_mutation_summary.scanned(),
                        "attempted": fs_mutation_summary.attempted(),
                        "completed": fs_mutation_summary.completed(),
                        "backing_off": fs_mutation_summary.backing_off(),
                        "poisoned": fs_mutation_summary.poisoned(),
                        "skipped": fs_mutation_summary.skipped(),
                        "remaining": fs_mutation_remaining,
                    },
                    "object_cleanup": {
                        "limit": object_cleanup_limit,
                        "scanned": object_cleanup_summary.candidates_listed,
                        "listed": object_cleanup_summary.candidates_listed,
                        "attempted": object_cleanup_summary.processed,
                        "processed": object_cleanup_summary.processed,
                        "completed": object_cleanup_summary.deleted_final_objects,
                        "deleted_final_objects": object_cleanup_summary.deleted_final_objects,
                        "deletion_ready": object_cleanup_summary.deletion_ready,
                        "backing_off": object_cleanup_summary.retryable_failures,
                        "retryable_failures": object_cleanup_summary.retryable_failures,
                        "poisoned": object_cleanup_summary.poisoned,
                        "skipped": object_cleanup_skipped,
                        "deferred": object_cleanup_deferred,
                        "skipped_non_cas_lost": object_cleanup_summary.skipped_non_cas_lost,
                        "skipped_reachable": object_cleanup_summary.skipped_reachable,
                        "skipped_blocked": object_cleanup_summary.skipped_blocked,
                        "skipped_claim_unavailable": object_cleanup_summary.skipped_claim_unavailable,
                        "deletion_enabled": false,
                        "remaining": object_cleanup_remaining,
                    },
                },
                "pre_visibility": {
                    "limit": pre_visibility_summary.limit(),
                    "scanned": pre_visibility_summary.scanned(),
                    "attempted": pre_visibility_summary.attempted(),
                    "resolved": pre_visibility_summary.resolved(),
                    "backing_off": pre_visibility_summary.backing_off(),
                    "poisoned": pre_visibility_summary.poisoned(),
                    "skipped": pre_visibility_summary.skipped(),
                    "post_cas_enqueued": pre_visibility_summary.post_cas_enqueued(),
                },
                "post_cas": {
                    "limit": post_cas_summary.limit(),
                    "scanned": post_cas_summary.scanned(),
                    "attempted": post_cas_summary.attempted(),
                    "completed": post_cas_summary.completed(),
                    "backing_off": post_cas_summary.backing_off(),
                    "poisoned": post_cas_summary.poisoned(),
                    "skipped": post_cas_summary.skipped(),
                },
                "fs_mutations": {
                    "limit": fs_mutation_summary.limit(),
                    "scanned": fs_mutation_summary.scanned(),
                    "attempted": fs_mutation_summary.attempted(),
                    "completed": fs_mutation_summary.completed(),
                    "backing_off": fs_mutation_summary.backing_off(),
                    "poisoned": fs_mutation_summary.poisoned(),
                    "skipped": fs_mutation_summary.skipped(),
                },
                "object_cleanup": {
                    "limit": object_cleanup_limit,
                    "scanned": object_cleanup_summary.candidates_listed,
                    "listed": object_cleanup_summary.candidates_listed,
                    "attempted": object_cleanup_summary.processed,
                    "processed": object_cleanup_summary.processed,
                    "completed": object_cleanup_summary.deleted_final_objects,
                    "deleted_final_objects": object_cleanup_summary.deleted_final_objects,
                    "deletion_ready": object_cleanup_summary.deletion_ready,
                    "backing_off": object_cleanup_summary.retryable_failures,
                    "retryable_failures": object_cleanup_summary.retryable_failures,
                    "poisoned": object_cleanup_summary.poisoned,
                    "skipped": object_cleanup_skipped,
                    "deferred": object_cleanup_deferred,
                    "deletion_enabled": false,
                },
            });
            let mut response = Json(body).into_response();
            if let Ok(value) = HeaderValue::from_str(&correlation_id) {
                response
                    .headers_mut()
                    .insert(VCS_RECOVERY_CORRELATION_ID_HEADER, value);
            }
            response
        }
        Err(_) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "durable FS mutation recovery run failed",
        )
        .into_response(),
    }
}

fn recovery_run_limit_from_body(body: &[u8]) -> Result<usize, VfsError> {
    if body.is_empty() {
        return Ok(VCS_RECOVERY_RUN_DEFAULT_LIMIT);
    }
    let request: RecoveryRunRequest =
        serde_json::from_slice(body).map_err(|_| VfsError::InvalidArgs {
            message: "invalid recovery run request".to_string(),
        })?;
    Ok(request
        .limit
        .unwrap_or(VCS_RECOVERY_RUN_DEFAULT_LIMIT)
        .min(VCS_RECOVERY_RUN_MAX_LIMIT))
}

async fn vcs_list_refs(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match require_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

    let refs_result = match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
        Ok(Some((capability, _repo))) => capability.list_refs().await,
        Ok(None) => state.core.list_refs().await,
        Err(response) => return response,
    };

    match refs_result {
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

async fn durable_vcs_list_refs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let _session = match require_durable_read_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
    };

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
    let policy_evaluation = match require_unprotected_ref(
        &state,
        &session,
        &headers,
        RoutePolicyAction::VcsRefCreate,
        &req.name,
    )
    .await
    {
        Ok(evaluation) => evaluation,
        Err(response) => return response,
    };
    let repo_context = match resolve_vcs_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(response) => return response,
    };

    let scope = vcs_idempotency_scope_for_repo(VCS_CREATE_REF_IDEMPOTENCY_ROUTE, &repo_context);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        with_explicit_repo_fingerprint(
            serde_json::json!({
                "route": VCS_CREATE_REF_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": null,
                "name": &req.name,
                "target": &req.target,
                "expected_target": null,
                "expected_version": null,
            }),
            &repo_context,
        ),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    let create_result = match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
        Ok(Some((capability, _repo))) => capability.create_ref(&req.name, &req.target).await,
        Ok(None) => state.core.create_ref(&req.name, &req.target).await,
        Err(response) => {
            abort_vcs_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
    };

    match create_result {
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
    let policy_evaluation = match require_unprotected_ref(
        &state,
        &session,
        &headers,
        RoutePolicyAction::VcsRefUpdate,
        &name,
    )
    .await
    {
        Ok(evaluation) => evaluation,
        Err(response) => return response,
    };
    let repo_context = match resolve_vcs_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(response) => return response,
    };

    let scope = vcs_idempotency_scope_for_repo(VCS_UPDATE_REF_IDEMPOTENCY_ROUTE, &repo_context);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        with_explicit_repo_fingerprint(
            serde_json::json!({
                "route": VCS_UPDATE_REF_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": null,
                "name": &name,
                "target": &req.target,
                "expected_target": &req.expected_target,
                "expected_version": req.expected_version,
            }),
            &repo_context,
        ),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    let update_result = match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
        Ok(Some((capability, _repo))) => {
            let policy_token =
                match PolicyDecisionToken::from_allowed_evaluation(&policy_evaluation) {
                    Ok(token) => token,
                    Err(error) => {
                        abort_vcs_idempotency(&state, reservation.as_ref()).await;
                        return err_json(
                            error_status(&error, StatusCode::FORBIDDEN),
                            error.to_string(),
                        )
                        .into_response();
                    }
                };
            capability
                .update_ref_with_policy_token(
                    &name,
                    &req.expected_target,
                    req.expected_version,
                    &req.target,
                    &policy_token,
                )
                .await
        }
        Ok(None) => {
            state
                .core
                .update_ref(
                    &name,
                    &req.expected_target,
                    req.expected_version,
                    &req.target,
                )
                .await
        }
        Err(response) => {
            abort_vcs_idempotency(&state, reservation.as_ref()).await;
            return response;
        }
    };

    match update_result {
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

    let repo_context = match resolve_vcs_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(response) => return response,
    };
    let workspace_id =
        match validate_workspace_header(&state, &headers, repo_context.repo_id()).await {
            Ok(workspace_id) => workspace_id,
            Err(e) => {
                return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                    .into_response();
            }
        };
    if let Err(e) = require_vcs_mutation_session(&session) {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }
    let policy_evaluation = match require_unprotected_ref(
        &state,
        &session,
        &headers,
        RoutePolicyAction::VcsCommit,
        crate::vcs::MAIN_REF,
    )
    .await
    {
        Ok(evaluation) => evaluation,
        Err(response) => return response,
    };
    let scope = vcs_idempotency_scope_for_repo(VCS_COMMIT_IDEMPOTENCY_ROUTE, &repo_context);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        with_explicit_repo_fingerprint(
            serde_json::json!({
                "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": workspace_id,
                "message": &req.message,
            }),
            &repo_context,
        ),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    if let Some((capability, _repo)) =
        match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
            Ok(capability) => capability,
            Err(response) => {
                abort_vcs_idempotency(&state, reservation.as_ref()).await;
                return response;
            }
        }
    {
        let policy_token = match PolicyDecisionToken::from_allowed_evaluation(&policy_evaluation) {
            Ok(token) => token,
            Err(error) => {
                abort_vcs_idempotency(&state, reservation.as_ref()).await;
                return err_json(
                    error_status(&error, StatusCode::FORBIDDEN),
                    error.to_string(),
                )
                .into_response();
            }
        };
        return guarded_durable_vcs_commit(
            &state,
            capability,
            &session,
            &req.message,
            workspace_id,
            reservation,
            policy_token,
        )
        .await;
    }

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
            if let Err(e) = update_workspace_head_from_headers(
                &state,
                &headers,
                repo_context.repo_id(),
                Some(hash.clone()),
            )
            .await
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
                            serde_json::json!({"error": "workspace head update failed after mutation"}),
                        ),
                        Err(audit_error) => audit_append_failed_response_parts(audit_error),
                    }
                } else {
                    (
                        error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                        serde_json::json!({"error": "workspace head update failed after mutation"}),
                    )
                };
                if let Err(response) =
                    complete_vcs_partial_idempotency(&state, reservation.as_ref(), status, &body)
                        .await
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
                    complete_vcs_partial_idempotency(&state, reservation.as_ref(), status, &body)
                        .await
                {
                    return response;
                }
                return json_response(status, body);
            }
            if let Err(response) =
                complete_vcs_commit_idempotency(&state, reservation.as_ref(), StatusCode::OK, &body)
                    .await
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
    let session = if state.core.guarded_durable_commit_route().is_some() {
        match require_admin(&state, &headers).await {
            Ok(session) => session,
            Err(e) => {
                return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                    .into_response();
            }
        }
    } else {
        match session_from_headers(&state, &headers).await {
            Ok(session) => session,
            Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
        }
    };

    let commits_result = match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
        Ok(Some((capability, _repo))) => capability.vcs_log_as(&session).await,
        Ok(None) => state.core.vcs_log_as(&session).await,
        Err(response) => return response,
    };

    let commits = match commits_result {
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

async fn durable_vcs_log(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match require_durable_read_admin(&state, &headers).await {
        Ok(session) => session,
        Err(e) => {
            return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                .into_response();
        }
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

fn durable_revert_idempotency_fingerprint_body(
    session: &Session,
    repo: &RequestRepoContext,
    workspace_id: Option<Uuid>,
    request_hash: &str,
    target_commit: CommitId,
    expected_head: CommitId,
    expected_ref_version: u64,
) -> serde_json::Value {
    with_explicit_repo_fingerprint(
        serde_json::json!({
            "route": VCS_REVERT_IDEMPOTENCY_ROUTE,
            "actor": actor_fingerprint(session),
            "workspace_id": workspace_id,
            "hash": request_hash,
            "target_commit": target_commit.to_hex(),
            "expected_head": expected_head.to_hex(),
            "expected_ref_version": expected_ref_version,
        }),
        repo,
    )
}

async fn begin_durable_revert_idempotency(
    state: &AppState,
    headers: &HeaderMap,
    session: &Session,
    repo: &RequestRepoContext,
    workspace_id: Option<Uuid>,
    request_hash: &str,
    revert_plan: &DurableCoreRevertPlan,
) -> VcsIdempotency {
    let scope = vcs_idempotency_scope_for_repo(VCS_REVERT_IDEMPOTENCY_ROUTE, repo);
    if let Some((replay_head, replay_version)) = revert_plan.replay_fingerprint_head_and_version() {
        match begin_vcs_idempotency(
            state,
            headers,
            &scope,
            durable_revert_idempotency_fingerprint_body(
                session,
                repo,
                workspace_id,
                request_hash,
                revert_plan.target_commit().id,
                replay_head,
                replay_version,
            ),
        )
        .await
        {
            VcsIdempotency::Respond(response) => return VcsIdempotency::Respond(response),
            VcsIdempotency::Execute(Some(reservation)) => {
                abort_vcs_idempotency(state, Some(&reservation)).await;
            }
            VcsIdempotency::Execute(None) => return VcsIdempotency::Execute(None),
        }
    }

    begin_vcs_idempotency(
        state,
        headers,
        &scope,
        durable_revert_idempotency_fingerprint_body(
            session,
            repo,
            workspace_id,
            request_hash,
            revert_plan.target_commit().id,
            revert_plan.expected_head().target,
            revert_plan.expected_head().version.value(),
        ),
    )
    .await
}

async fn guarded_durable_vcs_revert_route(
    state: &AppState,
    capability: GuardedDurableCommitRoute,
    repo: &RequestRepoContext,
    session: &Session,
    headers: &HeaderMap,
    req: &RevertRequest,
    workspace_id: Option<Uuid>,
) -> axum::response::Response {
    if let Err(response) = require_unprotected_ref(
        state,
        session,
        headers,
        RoutePolicyAction::VcsRevert,
        MAIN_REF,
    )
    .await
    {
        return response;
    }

    let revert_plan = match capability.revert_plan(&req.hash).await {
        Ok(revert_plan) => revert_plan,
        Err(error) => {
            return err_json(
                error_status(&error, StatusCode::BAD_REQUEST),
                error.to_string(),
            )
            .into_response();
        }
    };
    let changed_paths = revert_plan.changed_path_strings();
    let (_applicable_path_rules, policy_evaluation) =
        match require_unprotected_durable_revert_paths(state, session, headers, changed_paths).await
        {
            Ok(policy) => policy,
            Err(response) => return response,
        };

    let recovery_pending = match guarded_durable_revert_has_unresolved_recovery(&capability).await {
        Ok(pending) => pending,
        Err(response) => return response,
    };
    if recovery_pending {
        match begin_durable_revert_idempotency(
            state,
            headers,
            session,
            repo,
            workspace_id,
            &req.hash,
            &revert_plan,
        )
        .await
        {
            VcsIdempotency::Respond(response)
                if response
                    .headers()
                    .get("x-stratum-idempotent-replay")
                    .and_then(|value| value.to_str().ok())
                    == Some("true") =>
            {
                return response;
            }
            VcsIdempotency::Execute(Some(reservation)) => {
                abort_vcs_idempotency(state, Some(&reservation)).await;
            }
            VcsIdempotency::Execute(None) | VcsIdempotency::Respond(_) => {}
        }
        return guarded_durable_revert_recovery_conflict_response();
    }

    let reservation = match begin_durable_revert_idempotency(
        state,
        headers,
        session,
        repo,
        workspace_id,
        &req.hash,
        &revert_plan,
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };
    if let Err(response) = append_policy_audit(state, session, &policy_evaluation).await {
        abort_vcs_idempotency(state, reservation.as_ref()).await;
        return response;
    }
    let policy_token = match PolicyDecisionToken::from_allowed_evaluation(&policy_evaluation) {
        Ok(token) => token,
        Err(error) => {
            abort_vcs_idempotency(state, reservation.as_ref()).await;
            return err_json(
                error_status(&error, StatusCode::FORBIDDEN),
                error.to_string(),
            )
            .into_response();
        }
    };

    guarded_durable_vcs_revert(
        state,
        capability,
        session,
        revert_plan,
        workspace_id,
        reservation,
        policy_token,
    )
    .await
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

    let repo_context = match resolve_vcs_repo_context(&state, &headers, &session) {
        Ok(repo) => repo,
        Err(response) => return response,
    };
    let workspace_id =
        match validate_workspace_header(&state, &headers, repo_context.repo_id()).await {
            Ok(workspace_id) => workspace_id,
            Err(e) => {
                return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                    .into_response();
            }
        };
    if let Err(e) = require_vcs_mutation_session(&session) {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }
    if let Some((capability, repo_context)) =
        match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
            Ok(capability) => capability,
            Err(response) => return response,
        }
    {
        return guarded_durable_vcs_revert_route(
            &state,
            capability,
            &repo_context,
            &session,
            &headers,
            &req,
            workspace_id,
        )
        .await;
    }
    if let Err(response) = require_unprotected_ref(
        &state,
        &session,
        &headers,
        RoutePolicyAction::VcsRevert,
        crate::vcs::MAIN_REF,
    )
    .await
    {
        return response;
    }
    let (revert_target, applicable_path_rules, policy_evaluation) =
        match require_unprotected_revert_paths(&state, &session, &headers, &req.hash).await {
            Ok(target) => target,
            Err(response) => return response,
        };

    let scope = vcs_idempotency_scope_for_repo(VCS_REVERT_IDEMPOTENCY_ROUTE, &repo_context);
    let reservation = match begin_vcs_idempotency(
        &state,
        &headers,
        &scope,
        with_explicit_repo_fingerprint(
            serde_json::json!({
                "route": VCS_REVERT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": workspace_id,
                "hash": &req.hash,
            }),
            &repo_context,
        ),
    )
    .await
    {
        VcsIdempotency::Execute(reservation) => reservation,
        VcsIdempotency::Respond(response) => return response,
    };
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

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
            if let Err(e) = update_workspace_head_from_headers(
                &state,
                &headers,
                repo_context.repo_id(),
                Some(reverted_to.clone()),
            )
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
                            serde_json::json!({"error": "workspace head update failed after mutation"}),
                        ),
                        Err(audit_error) => audit_append_failed_response_parts(audit_error),
                    }
                } else {
                    (
                        error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                        serde_json::json!({"error": "workspace head update failed after mutation"}),
                    )
                };
                if let Err(response) =
                    complete_vcs_partial_idempotency(&state, reservation.as_ref(), status, &body)
                        .await
                {
                    return response;
                }
                return json_response(status, body);
            }
            let body = serde_json::json!({"reverted_to": &reverted_to});
            if let Err(e) = state.audit.append(event).await {
                let (status, body) = audit_append_failed_response_parts(e);
                if let Err(response) =
                    complete_vcs_partial_idempotency(&state, reservation.as_ref(), status, &body)
                        .await
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

    let status_result = match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
        Ok(Some((capability, _repo))) => capability.vcs_status_as(&session).await,
        Ok(None) => state.core.vcs_status_as(&session).await,
        Err(response) => return response,
    };

    match status_result {
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

    let diff_result = match resolve_guarded_durable_vcs_capability(&state, &headers, &session) {
        Ok(Some((capability, _repo))) => {
            capability
                .vcs_diff_as(query.path.as_deref(), &session)
                .await
        }
        Ok(None) => {
            state
                .core
                .vcs_diff_as(query.path.as_deref(), &session)
                .await
        }
        Err(response) => return response,
    };

    match diff_result {
        Ok(diff) => (StatusCode::OK, diff).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditEvent, AuditStore, InMemoryAuditStore};
    use crate::auth::ROOT_UID;
    use crate::auth::Uid;
    use crate::auth::session::Session;
    use crate::backend::committed_read::DurableCommittedFsReader;
    use crate::backend::core_transaction::{
        DurableCoreCommittedResponse, DurableCorePostCasRecoveryClaim,
        DurableCorePostCasRecoveryClaimRequest, DurableCorePostCasRecoveryContext,
        DurableCorePostCasRecoveryCounts, DurableCorePostCasRecoveryState,
        DurableCorePostCasRecoveryStatus, DurableCorePostCasRecoveryTarget,
        DurableCorePreVisibilityRecoveryClaim, DurableCorePreVisibilityRecoveryClaimRequest,
        DurableCorePreVisibilityRecoveryCounts, DurableCorePreVisibilityRecoveryRecord,
        DurableCorePreVisibilityRecoveryStage, DurableCorePreVisibilityRecoveryState,
        DurableCorePreVisibilityRecoveryStatus, DurableCorePreVisibilityRecoveryStore,
        DurableCorePreVisibilityRecoveryTarget, DurableFsMutationAuditRecoveryContext,
        DurableFsMutationRecoveryEnvelope, DurableFsMutationRecoveryState,
        DurableFsMutationRecoveryStep, DurableFsMutationRecoveryTarget,
        InMemoryDurableCorePostCasRecoveryClaimStore,
    };
    use crate::backend::durable_mutation::{
        DurableMutationEngine, DurableMutationInput, DurableMutationOperation,
    };
    use crate::backend::object_cleanup::{ObjectCleanupClaimKind, ObjectCleanupClaimRequest};
    use crate::backend::{
        CommitRecord, CommitStore, LocalMemoryObjectStore, ObjectStore, ObjectWrite,
        RefExpectation, RefRecord, RefStore, RefUpdate, RefVersion, RepoId, StoredObject,
        StratumStores,
    };
    use crate::db::StratumDb;
    use crate::fs::MetadataUpdate;
    use crate::idempotency::{IdempotencyKey, IdempotencyStore, InMemoryIdempotencyStore};
    use crate::server::core::LocalCoreRuntime;
    use crate::server::policy::{PolicyAction, PolicyDecisionToken};
    use crate::server::{ServerLocalDb, ServerState, ServerStores, build_durable_core_router};
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, MAIN_REF, RefName};
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, ValidWorkspaceToken,
        WorkspaceMetadataStore, WorkspacePrincipalKind, WorkspacePrincipalRecord, WorkspaceRecord,
        WorkspaceTokenRecord,
    };
    use axum::extract::Path;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        })
    }

    fn assert_audit_action_count(events: &[AuditEvent], action: AuditAction, expected: usize) {
        assert_eq!(
            events.iter().filter(|event| event.action == action).count(),
            expected,
            "unexpected count for {action:?} in {events:?}"
        );
    }

    #[derive(Default)]
    struct FailingMutationAuditStore {
        inner: InMemoryAuditStore,
    }

    #[derive(Default)]
    struct StatusNoBlobGetObjectStore {
        inner: LocalMemoryObjectStore,
        lengths: RwLock<BTreeMap<(RepoId, ObjectId), u64>>,
    }

    #[derive(Default)]
    struct DiffBlobAccessObjectStore {
        inner: LocalMemoryObjectStore,
        lengths: RwLock<BTreeMap<(RepoId, ObjectId), u64>>,
        denied_blob_gets: RwLock<BTreeSet<ObjectId>>,
    }

    impl DiffBlobAccessObjectStore {
        async fn deny_blob_gets(&self, ids: impl IntoIterator<Item = ObjectId>) {
            self.denied_blob_gets.write().await.extend(ids);
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for StatusNoBlobGetObjectStore {
        async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
            let key = (write.repo_id.clone(), write.id);
            let len = write.bytes.len() as u64;
            let stored = self.inner.put(write).await?;
            self.lengths.write().await.insert(key, len);
            Ok(stored)
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<Option<StoredObject>, VfsError> {
            if expected_kind == ObjectKind::Blob {
                return Err(VfsError::CorruptStore {
                    message: "durable status fetched blob bytes".to_string(),
                });
            }
            self.inner.get(repo_id, id, expected_kind).await
        }

        async fn contains(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<bool, VfsError> {
            self.inner.contains(repo_id, id, expected_kind).await
        }

        async fn object_len(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<Option<u64>, VfsError> {
            if expected_kind != ObjectKind::Blob {
                return ObjectStore::object_len(&self.inner, repo_id, id, expected_kind).await;
            }
            Ok(self
                .lengths
                .read()
                .await
                .get(&(repo_id.clone(), id))
                .copied())
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DiffBlobAccessObjectStore {
        async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
            let key = (write.repo_id.clone(), write.id);
            let len = write.bytes.len() as u64;
            let stored = self.inner.put(write).await?;
            self.lengths.write().await.insert(key, len);
            Ok(stored)
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<Option<StoredObject>, VfsError> {
            if expected_kind == ObjectKind::Blob && self.denied_blob_gets.read().await.contains(&id)
            {
                return Err(VfsError::CorruptStore {
                    message: "durable diff fetched filtered blob bytes with private path detail"
                        .to_string(),
                });
            }
            self.inner.get(repo_id, id, expected_kind).await
        }

        async fn contains(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<bool, VfsError> {
            self.inner.contains(repo_id, id, expected_kind).await
        }

        async fn object_len(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<Option<u64>, VfsError> {
            if expected_kind != ObjectKind::Blob {
                return ObjectStore::object_len(&self.inner, repo_id, id, expected_kind).await;
            }
            Ok(self
                .lengths
                .read()
                .await
                .get(&(repo_id.clone(), id))
                .copied())
        }
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

    fn guarded_durable_commit_state_for_repo(
        db: StratumDb,
        repo_id: RepoId,
        stores: StratumStores,
    ) -> AppState {
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared_with_guarded_durable_commit_route(
                db.clone(),
                repo_id,
                stores.clone(),
            ),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: stores.workspace_metadata.clone(),
            idempotency: stores.idempotency.clone(),
            audit: stores.audit.clone(),
            review: stores.review.clone(),
        })
    }

    fn guarded_durable_commit_state(db: StratumDb, stores: StratumStores) -> AppState {
        guarded_durable_commit_state_for_repo(db, RepoId::local(), stores)
    }

    #[tokio::test]
    async fn vcs_idempotency_scope_is_repo_qualified_for_explicit_repo_contexts() {
        let store = InMemoryIdempotencyStore::new();
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("same-key-across-repos"))
                .unwrap();
        let mut repo_a_headers = HeaderMap::new();
        repo_a_headers.insert("x-stratum-repo", "repo_a".parse().unwrap());
        let repo_a =
            RequestRepoContext::resolve(&repo_a_headers, None, true).expect("repo a context");
        let mut repo_b_headers = HeaderMap::new();
        repo_b_headers.insert("x-stratum-repo", "repo_b".parse().unwrap());
        let repo_b =
            RequestRepoContext::resolve(&repo_b_headers, None, true).expect("repo b context");

        let scope_a = vcs_idempotency_scope_for_repo(VCS_COMMIT_IDEMPOTENCY_ROUTE, &repo_a);
        let scope_b = vcs_idempotency_scope_for_repo(VCS_COMMIT_IDEMPOTENCY_ROUTE, &repo_b);
        assert_ne!(scope_a, scope_b);

        let fingerprint_a = request_fingerprint(
            &scope_a,
            &with_explicit_repo_fingerprint(
                serde_json::json!({"route": VCS_COMMIT_IDEMPOTENCY_ROUTE}),
                &repo_a,
            ),
        )
        .unwrap();
        let fingerprint_b = request_fingerprint(
            &scope_b,
            &with_explicit_repo_fingerprint(
                serde_json::json!({"route": VCS_COMMIT_IDEMPOTENCY_ROUTE}),
                &repo_b,
            ),
        )
        .unwrap();

        assert!(matches!(
            store.begin(&scope_a, &key, &fingerprint_a).await.unwrap(),
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store.begin(&scope_b, &key, &fingerprint_b).await.unwrap(),
            IdempotencyBegin::Execute(_)
        ));
    }

    #[tokio::test]
    async fn nonlocal_guarded_durable_admin_reads_require_repo_context() {
        let state = guarded_durable_commit_state_for_repo(
            StratumDb::open_memory(),
            RepoId::new("repo_durable").unwrap(),
            StratumStores::local_memory(),
        );

        let missing_repo = vcs_list_refs(State(state.clone()), user_headers_without_repo("root"))
            .await
            .into_response();
        assert_eq!(missing_repo.status(), StatusCode::BAD_REQUEST);

        let mut headers = user_headers("root");
        headers.insert("x-stratum-repo", "repo_durable".parse().unwrap());
        let explicit_repo = vcs_list_refs(State(state), headers).await.into_response();
        assert_eq!(explicit_repo.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn malformed_guarded_durable_repo_header_response_is_redacted() {
        let state = guarded_durable_commit_state_for_repo(
            StratumDb::open_memory(),
            RepoId::new("repo_durable").unwrap(),
            StratumStores::local_memory(),
        );
        let raw_header = "private-token/header";
        let mut headers = user_headers_without_repo("root");
        headers.insert("x-stratum-repo", raw_header.parse().unwrap());

        let response = vcs_list_refs(State(state), headers).await.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        let error = body["error"].as_str().expect("error string");
        assert_eq!(error, "stratum: invalid x-stratum-repo header");
        assert!(!error.contains(raw_header), "{error}");
    }

    fn user_headers_without_repo(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
        headers
    }

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = user_headers_without_repo(username);
        headers.insert("x-stratum-repo", RepoId::local().as_str().parse().unwrap());
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

    async fn create_local_repo_workspace_with_refs(
        stores: &StratumStores,
        name: &str,
        root_path: &str,
        base_ref: &str,
        session_ref: Option<&str>,
    ) -> WorkspaceRecord {
        stores
            .workspace_metadata
            .create_workspace_with_refs_for_repo(
                RepoId::local(),
                name,
                root_path,
                base_ref,
                session_ref,
            )
            .await
            .unwrap()
    }

    async fn create_local_repo_workspace(
        stores: &StratumStores,
        name: &str,
        root_path: &str,
    ) -> WorkspaceRecord {
        stores
            .workspace_metadata
            .create_workspace_for_repo(RepoId::local(), name, root_path)
            .await
            .unwrap()
    }

    async fn json_body(response: axum::response::Response) -> serde_json::Value {
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
        let raw_secret = format!("durable-read-token-{workspace_id}");
        let workspace = WorkspaceRecord {
            id: workspace_id,
            name: "durable-read".to_string(),
            root_path: "/".to_string(),
            head_commit: None,
            version: 1,
            base_ref: MAIN_REF.to_string(),
            session_ref: Some("agent/durable/read".to_string()),
            repo_id: Some(repo_id.as_str().to_string()),
        };
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: "durable-read-token".to_string(),
            agent_uid: uid,
            secret_hash: "redacted-hash".to_string(),
            read_prefixes: vec!["/".to_string()],
            write_prefixes: Vec::new(),
            principal_uid: Some(uid),
            token_version: 1,
            issued_at_unix: 1,
            updated_at_unix: 1,
            expires_at_unix: None,
            revoked_at_unix: None,
        };
        let principal = WorkspacePrincipalRecord {
            uid,
            username: format!("durable-principal-{uid}"),
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

    fn durable_workspace_bearer_headers(raw_secret: &str, workspace_id: Uuid) -> HeaderMap {
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

    async fn seed_durable_core_router_vcs_metadata(
        stores: &StratumStores,
        repo_id: &RepoId,
    ) -> (CommitId, CommitId) {
        let base = CommitId::from(ObjectId::from_bytes(b"durable-router-base"));
        let head = CommitId::from(ObjectId::from_bytes(b"durable-router-head"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: base,
                root_tree: ObjectId::from_bytes(b"durable-router-base-tree"),
                parents: Vec::new(),
                timestamp: 10,
                message: "durable router base".to_string(),
                author: "admin".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: head,
                root_tree: ObjectId::from_bytes(b"durable-router-head-tree"),
                parents: vec![base],
                timestamp: 11,
                message: "durable router head".to_string(),
                author: "admin".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new(MAIN_REF).unwrap(),
                target: head,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("archive/base").unwrap(),
                target: base,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        (base, head)
    }

    fn durable_core_router_with_workspace_store(
        stores: StratumStores,
        workspaces: Arc<dyn WorkspaceMetadataStore>,
        repo_id: RepoId,
    ) -> Router {
        build_durable_core_router(
            ServerStores {
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

    #[tokio::test]
    async fn durable_core_runtime_refs_allow_admin_workspace_bearer() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::new("repo_durable_router_refs").unwrap();
        let (_base, head) = seed_durable_core_router_vcs_metadata(&stores, &repo_id).await;
        let (workspaces, workspace_id, raw_secret) =
            durable_workspace_bearer_store(&repo_id, 501, vec![WHEEL_GID]);
        let router = durable_core_router_with_workspace_store(stores, workspaces, repo_id);
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/vcs/refs"))
            .headers(durable_workspace_bearer_headers(&raw_secret, workspace_id))
            .send()
            .await
            .expect("refs request completes");
        let status = response.status();
        let body: serde_json::Value = response.json().await.expect("refs response is json");
        server.abort();

        assert_eq!(status, reqwest::StatusCode::OK);
        let refs = body["refs"].as_array().expect("refs array");
        assert!(refs.iter().any(|item| {
            item["name"] == MAIN_REF && item["target"] == serde_json::json!(head.to_hex())
        }));
        assert!(refs.iter().any(|item| item["name"] == "archive/base"));
    }

    #[tokio::test]
    async fn durable_core_runtime_log_allows_admin_workspace_bearer() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::new("repo_durable_router_log").unwrap();
        seed_durable_core_router_vcs_metadata(&stores, &repo_id).await;
        let (workspaces, workspace_id, raw_secret) =
            durable_workspace_bearer_store(&repo_id, ROOT_UID, vec![WHEEL_GID]);
        let router = durable_core_router_with_workspace_store(stores, workspaces, repo_id);
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/vcs/log"))
            .headers(durable_workspace_bearer_headers(&raw_secret, workspace_id))
            .send()
            .await
            .expect("log request completes");
        let status = response.status();
        let body: serde_json::Value = response.json().await.expect("log response is json");
        server.abort();

        assert_eq!(status, reqwest::StatusCode::OK);
        let messages = body["commits"]
            .as_array()
            .expect("commits array")
            .iter()
            .map(|item| item["message"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(messages, vec!["durable router head", "durable router base"]);
    }

    #[tokio::test]
    async fn durable_core_router_rejects_cross_repo_workspace_bearer_before_vcs_metadata_read() {
        let stores = StratumStores::local_memory();
        let repo_a = RepoId::new("repo_durable_vcs_a").unwrap();
        let repo_b = RepoId::new("repo_durable_vcs_b").unwrap();
        seed_durable_core_router_vcs_metadata(&stores, &repo_a).await;
        let (workspaces, workspace_id, raw_secret) =
            durable_workspace_bearer_store(&repo_b, ROOT_UID, vec![WHEEL_GID]);
        let router = durable_core_router_with_workspace_store(stores, workspaces, repo_a.clone());
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/vcs/log"))
            .headers(durable_workspace_bearer_headers(&raw_secret, workspace_id))
            .send()
            .await
            .expect("log request completes");
        let status = response.status();
        let body = response.text().await.expect("error body");
        server.abort();

        assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
        assert!(!body.contains("durable router head"), "{body}");
        assert!(!body.contains(repo_a.as_str()), "{body}");
        assert!(!body.contains(repo_b.as_str()), "{body}");
    }

    #[tokio::test]
    async fn durable_core_router_rejects_conflicting_repo_header_before_vcs_metadata_read() {
        let stores = StratumStores::local_memory();
        let repo_a = RepoId::new("repo_durable_vcs_header_a").unwrap();
        let repo_b = RepoId::new("repo_durable_vcs_header_b").unwrap();
        seed_durable_core_router_vcs_metadata(&stores, &repo_a).await;
        let (workspaces, workspace_id, raw_secret) =
            durable_workspace_bearer_store(&repo_a, ROOT_UID, vec![WHEEL_GID]);
        let router = durable_core_router_with_workspace_store(stores, workspaces, repo_a.clone());
        let (base_url, server) = spawn_test_router(router).await;
        let mut headers = durable_workspace_bearer_headers(&raw_secret, workspace_id);
        headers.insert("x-stratum-repo", repo_b.as_str().parse().unwrap());

        let response = reqwest::Client::new()
            .get(format!("{base_url}/vcs/refs"))
            .headers(headers)
            .send()
            .await
            .expect("refs request completes");
        let status = response.status();
        let body = response.text().await.expect("error body");
        server.abort();

        assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
        assert!(!body.contains("durable router head"), "{body}");
        assert!(!body.contains(repo_a.as_str()), "{body}");
        assert!(!body.contains(repo_b.as_str()), "{body}");
    }

    #[tokio::test]
    async fn durable_core_runtime_metadata_reads_reject_non_admin_workspace_bearer() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::new("repo_durable_router_non_admin").unwrap();
        seed_durable_core_router_vcs_metadata(&stores, &repo_id).await;
        let (workspaces, workspace_id, raw_secret) =
            durable_workspace_bearer_store(&repo_id, 501, vec![501]);
        let router = durable_core_router_with_workspace_store(stores, workspaces, repo_id);
        let (base_url, server) = spawn_test_router(router).await;
        let client = reqwest::Client::new();

        for path in ["/vcs/refs", "/vcs/log"] {
            let response = client
                .get(format!("{base_url}{path}"))
                .headers(durable_workspace_bearer_headers(&raw_secret, workspace_id))
                .send()
                .await
                .expect("metadata request completes");
            assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN, "{path}");
            let body: serde_json::Value = response.json().await.expect("error response is json");
            assert_eq!(
                body["error"],
                "stratum: permission denied: 'admin operation'"
            );
        }
        server.abort();
    }

    async fn text_body(response: axum::response::Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(body.to_vec()).unwrap()
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

    fn synthetic_commit_id(label: &str) -> CommitId {
        CommitId::from(ObjectId::from_bytes(label.as_bytes()))
    }

    fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            kind,
            id,
            mode,
            uid: crate::auth::ROOT_UID,
            gid: crate::auth::ROOT_GID,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        }
    }

    fn tree_entry_with_metadata(
        name: &str,
        kind: TreeEntryKind,
        id: ObjectId,
        mode: u16,
        mime_type: Option<&str>,
        custom_attrs: BTreeMap<String, String>,
    ) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            kind,
            id,
            mode,
            uid: crate::auth::ROOT_UID,
            gid: crate::auth::ROOT_GID,
            mime_type: mime_type.map(str::to_string),
            custom_attrs,
        }
    }

    async fn put_durable_object(
        stores: &StratumStores,
        repo_id: &RepoId,
        kind: ObjectKind,
        bytes: Vec<u8>,
    ) -> ObjectId {
        let id = ObjectId::from_bytes(&bytes);
        let size = bytes.len() as u64;
        stores
            .objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id,
                kind,
                bytes,
            })
            .await
            .unwrap();
        stores
            .object_metadata
            .put(crate::backend::blob_object::ObjectMetadataRecord::new(
                repo_id.clone(),
                id,
                kind,
                size,
            ))
            .await
            .unwrap();
        id
    }

    async fn seed_durable_workspace_base(stores: &StratumStores) -> CommitId {
        let repo_id = RepoId::local();
        let demo_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("demo", TreeEntryKind::Tree, demo_tree_id, 0o755)],
            }
            .serialize(),
        )
        .await;
        let commit_id = CommitId::from(ObjectId::from_bytes(b"durable vcs workspace base"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1_725_000_004,
                message: "durable vcs workspace base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id,
                name: RefName::new(MAIN_REF).unwrap(),
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        commit_id
    }

    struct DurableRevertFixture {
        target_commit: CommitId,
        head_commit: CommitId,
        target_root: ObjectId,
    }

    async fn seed_durable_revert_history(stores: &StratumStores) -> DurableRevertFixture {
        let repo_id = RepoId::local();
        let target_blob =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"before\n".to_vec()).await;
        let head_blob =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"after\n".to_vec()).await;
        let target_demo_tree = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "revert.txt",
                    TreeEntryKind::Blob,
                    target_blob,
                    0o644,
                )],
            }
            .serialize(),
        )
        .await;
        let head_demo_tree = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "revert.txt",
                    TreeEntryKind::Blob,
                    head_blob,
                    0o644,
                )],
            }
            .serialize(),
        )
        .await;
        let target_root = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "demo",
                    TreeEntryKind::Tree,
                    target_demo_tree,
                    0o755,
                )],
            }
            .serialize(),
        )
        .await;
        let head_root = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "demo",
                    TreeEntryKind::Tree,
                    head_demo_tree,
                    0o755,
                )],
            }
            .serialize(),
        )
        .await;
        let target_commit = CommitId::from(ObjectId::from_bytes(b"durable revert target"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: target_commit,
                root_tree: target_root,
                parents: Vec::new(),
                timestamp: 1_725_001_000,
                message: "durable revert target".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        let head_commit = CommitId::from(ObjectId::from_bytes(b"durable revert head"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: head_commit,
                root_tree: head_root,
                parents: vec![target_commit],
                timestamp: 1_725_001_001,
                message: "durable revert head".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id,
                name: RefName::new(MAIN_REF).unwrap(),
                target: head_commit,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

        DurableRevertFixture {
            target_commit,
            head_commit,
            target_root,
        }
    }

    async fn seed_durable_status_base(stores: &StratumStores) -> CommitId {
        let repo_id = RepoId::local();
        let modified_id =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"before".to_vec()).await;
        let deleted_id =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"deleted".to_vec()).await;
        let metadata_id =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"metadata".to_vec()).await;
        let type_changed_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let demo_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("deleted.txt", TreeEntryKind::Blob, deleted_id, 0o644),
                    tree_entry("meta.txt", TreeEntryKind::Blob, metadata_id, 0o644),
                    tree_entry("modified.txt", TreeEntryKind::Blob, modified_id, 0o644),
                    tree_entry(
                        "type-change",
                        TreeEntryKind::Tree,
                        type_changed_tree_id,
                        0o755,
                    ),
                ],
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("demo", TreeEntryKind::Tree, demo_tree_id, 0o755)],
            }
            .serialize(),
        )
        .await;
        let commit_id = CommitId::from(ObjectId::from_bytes(b"durable vcs status base"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1_725_000_104,
                message: "durable vcs status base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id,
                name: RefName::new(MAIN_REF).unwrap(),
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        commit_id
    }

    async fn seed_durable_scoped_status_base(stores: &StratumStores) -> CommitId {
        let repo_id = RepoId::local();
        let public_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let private_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let demo_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("private", TreeEntryKind::Tree, private_tree_id, 0o755),
                    tree_entry("public", TreeEntryKind::Tree, public_tree_id, 0o755),
                ],
            }
            .serialize(),
        )
        .await;
        let secret_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("demo", TreeEntryKind::Tree, demo_tree_id, 0o755),
                    tree_entry("secret", TreeEntryKind::Tree, secret_tree_id, 0o755),
                ],
            }
            .serialize(),
        )
        .await;
        let commit_id = CommitId::from(ObjectId::from_bytes(b"durable scoped status base"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1_725_000_204,
                message: "durable scoped status base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id,
                name: RefName::new(MAIN_REF).unwrap(),
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        commit_id
    }

    const DURABLE_DIFF_OVERSIZED_TEXT_LEN: usize = 512 * 1024 + 1;

    struct DurableDiffFixture {
        added_after: ObjectId,
        deleted_before: ObjectId,
        meta_content: ObjectId,
        binary_before: ObjectId,
        binary_after: ObjectId,
        non_utf8_before: ObjectId,
        non_utf8_after: ObjectId,
        oversized_before: ObjectId,
        oversized_after: ObjectId,
        type_changed_after: ObjectId,
        filtered_before: ObjectId,
        filtered_after: ObjectId,
        nested_before: ObjectId,
        nested_after: ObjectId,
        sibling_before: ObjectId,
        sibling_after: ObjectId,
    }

    impl DurableDiffFixture {
        fn non_modified_blob_ids(&self) -> Vec<ObjectId> {
            vec![
                self.added_after,
                self.deleted_before,
                self.meta_content,
                self.binary_before,
                self.binary_after,
                self.non_utf8_before,
                self.non_utf8_after,
                self.oversized_before,
                self.oversized_after,
                self.type_changed_after,
                self.filtered_before,
                self.filtered_after,
                self.nested_before,
                self.nested_after,
                self.sibling_before,
                self.sibling_after,
            ]
        }
    }

    fn durable_diff_modified_text(replacement: &str) -> Vec<u8> {
        (1..=24)
            .map(|line| {
                if line == 12 {
                    format!("{replacement}\n")
                } else {
                    format!("shared line {line:02}\n")
                }
            })
            .collect::<String>()
            .into_bytes()
    }

    async fn seed_durable_diff_base(stores: &StratumStores) -> (CommitId, DurableDiffFixture) {
        let repo_id = RepoId::local();
        let modified_before_bytes = durable_diff_modified_text("before durable change");
        let modified_before =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, modified_before_bytes).await;
        let deleted_before =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"delete me\n".to_vec()).await;
        let meta_content =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"metadata\n".to_vec()).await;
        let binary_before =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"\0old".to_vec()).await;
        let non_utf8_before =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, vec![0xff, 0xfe, b'a']).await;
        let oversized_before =
            put_durable_object(stores, &repo_id, ObjectKind::Blob, b"small\n".to_vec()).await;
        let filtered_before = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Blob,
            b"filtered before\n".to_vec(),
        )
        .await;
        let nested_before = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Blob,
            b"nested before\n".to_vec(),
        )
        .await;
        let sibling_before = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Blob,
            b"sibling before\n".to_vec(),
        )
        .await;
        let type_changed_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let nested_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "child.txt",
                    TreeEntryKind::Blob,
                    nested_before,
                    0o644,
                )],
            }
            .serialize(),
        )
        .await;
        let demo_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry_with_metadata(
                        "binary.bin",
                        TreeEntryKind::Blob,
                        binary_before,
                        0o644,
                        Some("application/octet-stream"),
                        BTreeMap::new(),
                    ),
                    tree_entry("deleted.txt", TreeEntryKind::Blob, deleted_before, 0o644),
                    tree_entry("filtered.txt", TreeEntryKind::Blob, filtered_before, 0o644),
                    tree_entry("meta.txt", TreeEntryKind::Blob, meta_content, 0o644),
                    tree_entry("modified.txt", TreeEntryKind::Blob, modified_before, 0o644),
                    tree_entry("nested", TreeEntryKind::Tree, nested_tree_id, 0o755),
                    tree_entry("non-utf8.bin", TreeEntryKind::Blob, non_utf8_before, 0o644),
                    tree_entry(
                        "oversized.txt",
                        TreeEntryKind::Blob,
                        oversized_before,
                        0o644,
                    ),
                    tree_entry("sibling.txt", TreeEntryKind::Blob, sibling_before, 0o644),
                    tree_entry(
                        "type-change",
                        TreeEntryKind::Tree,
                        type_changed_tree_id,
                        0o755,
                    ),
                ],
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_durable_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("demo", TreeEntryKind::Tree, demo_tree_id, 0o755)],
            }
            .serialize(),
        )
        .await;
        let commit_id = CommitId::from(ObjectId::from_bytes(b"durable vcs diff base"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1_725_000_304,
                message: "durable vcs diff base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id,
                name: RefName::new(MAIN_REF).unwrap(),
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

        let added_after = ObjectId::from_bytes(b"added durable\n");
        let binary_after = ObjectId::from_bytes(b"\0new");
        let non_utf8_after = ObjectId::from_bytes(&[0xff, 0xfe, b'b']);
        let oversized_after = ObjectId::from_bytes(&vec![b'x'; DURABLE_DIFF_OVERSIZED_TEXT_LEN]);
        let type_changed_after = ObjectId::from_bytes(b"type changed\n");
        let filtered_after = ObjectId::from_bytes(b"filtered after\n");
        let nested_after = ObjectId::from_bytes(b"nested after\n");
        let sibling_after = ObjectId::from_bytes(b"sibling after\n");

        (
            commit_id,
            DurableDiffFixture {
                added_after,
                deleted_before,
                meta_content,
                binary_before,
                binary_after,
                non_utf8_before,
                non_utf8_after,
                oversized_before,
                oversized_after,
                type_changed_after,
                filtered_before,
                filtered_after,
                nested_before,
                nested_after,
                sibling_before,
                sibling_after,
            },
        )
    }

    async fn apply_durable_diff_session_changes(stores: &StratumStores, session_ref: &str) {
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/modified.txt".to_string(),
                content: durable_diff_modified_text("after durable change"),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_305,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/added.txt".to_string(),
                content: b"added durable\n".to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_306,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::Delete {
                path: "/demo/deleted.txt".to_string(),
                recursive: false,
            },
            1_725_000_307,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::SetMetadata {
                path: "/demo/meta.txt".to_string(),
                update: MetadataUpdate {
                    mime_type: Some(Some("text/plain".to_string())),
                    custom_attrs: BTreeMap::from([("reviewed".to_string(), "true".to_string())]),
                    remove_custom_attrs: Vec::new(),
                },
            },
            1_725_000_308,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/binary.bin".to_string(),
                content: b"\0new".to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: Some("application/octet-stream".to_string()),
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_309,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/non-utf8.bin".to_string(),
                content: vec![0xff, 0xfe, b'b'],
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_310,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/oversized.txt".to_string(),
                content: vec![b'x'; DURABLE_DIFF_OVERSIZED_TEXT_LEN],
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: Some("text/plain".to_string()),
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_311,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::Delete {
                path: "/demo/type-change".to_string(),
                recursive: true,
            },
            1_725_000_312,
        )
        .await;
        apply_durable_session_operation(
            stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/type-change".to_string(),
                content: b"type changed\n".to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: Some("text/plain".to_string()),
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_313,
        )
        .await;
        apply_durable_session_write(
            stores,
            session_ref,
            "/demo/filtered.txt",
            b"filtered after\n",
            1_725_000_314,
        )
        .await;
        apply_durable_session_write(
            stores,
            session_ref,
            "/demo/nested/child.txt",
            b"nested after\n",
            1_725_000_315,
        )
        .await;
        apply_durable_session_write(
            stores,
            session_ref,
            "/demo/sibling.txt",
            b"sibling after\n",
            1_725_000_316,
        )
        .await;
    }

    async fn apply_durable_session_operation(
        stores: &StratumStores,
        session_ref: &str,
        operation: DurableMutationOperation,
        timestamp: u64,
    ) {
        let repo_id = RepoId::local();
        let token = durable_mutation_test_policy_token(&operation);
        DurableMutationEngine::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        )
        .with_policy_token(&token)
        .apply(DurableMutationInput {
            base_ref: RefName::new(MAIN_REF).unwrap(),
            session_ref: RefName::new(session_ref).unwrap(),
            operation,
            author: "agent".to_string(),
            timestamp,
            preflight_session: None,
        })
        .await
        .unwrap();
    }

    async fn apply_durable_session_write(
        stores: &StratumStores,
        session_ref: &str,
        path: &str,
        content: &[u8],
        timestamp: u64,
    ) {
        let repo_id = RepoId::local();
        let operation = DurableMutationOperation::WriteFile {
            path: path.to_string(),
            content: content.to_vec(),
            mode: 0o644,
            uid: ROOT_UID,
            gid: crate::auth::ROOT_GID,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        };
        let token = durable_mutation_test_policy_token(&operation);
        DurableMutationEngine::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        )
        .with_policy_token(&token)
        .apply(DurableMutationInput {
            base_ref: RefName::new(MAIN_REF).unwrap(),
            session_ref: RefName::new(session_ref).unwrap(),
            operation,
            author: "agent".to_string(),
            timestamp,
            preflight_session: None,
        })
        .await
        .unwrap();
    }

    fn durable_mutation_test_policy_token(
        operation: &DurableMutationOperation,
    ) -> PolicyDecisionToken {
        match operation {
            DurableMutationOperation::WriteFile { path, .. } => {
                PolicyDecisionToken::allow_for_test_with_paths(
                    PolicyAction::FsWrite,
                    MAIN_REF,
                    [path.as_str()],
                )
            }
            DurableMutationOperation::Mkdir { path, .. } => {
                PolicyDecisionToken::allow_for_test_with_paths(
                    PolicyAction::FsMkdir,
                    MAIN_REF,
                    [path.as_str()],
                )
            }
            DurableMutationOperation::Delete { path, recursive } => {
                if *recursive {
                    PolicyDecisionToken::allow_for_test_with_paths_and_descendants(
                        PolicyAction::FsDelete,
                        MAIN_REF,
                        [path.as_str()],
                        [path.as_str()],
                    )
                } else {
                    PolicyDecisionToken::allow_for_test_with_paths(
                        PolicyAction::FsDelete,
                        MAIN_REF,
                        [path.as_str()],
                    )
                }
            }
            DurableMutationOperation::Copy { destination, .. } => {
                PolicyDecisionToken::allow_for_test_with_paths(
                    PolicyAction::FsCopy,
                    MAIN_REF,
                    [destination.as_str()],
                )
            }
            DurableMutationOperation::Move {
                source,
                destination,
            } => PolicyDecisionToken::allow_for_test_with_paths_and_descendants(
                PolicyAction::FsMove,
                MAIN_REF,
                [source.as_str(), destination.as_str()],
                [source.as_str(), destination.as_str()],
            ),
            DurableMutationOperation::SetMetadata { path, .. } => {
                PolicyDecisionToken::allow_for_test_with_paths(
                    PolicyAction::FsMetadataUpdate,
                    MAIN_REF,
                    [path.as_str()],
                )
            }
        }
    }

    #[tokio::test]
    async fn guarded_durable_status_renders_mounted_session_changes_without_local_vcs_state() {
        let mut stores = StratumStores::local_memory();
        stores.objects = Arc::new(StatusNoBlobGetObjectStore::default());
        let base_commit = seed_durable_status_base(&stores).await;
        let session_ref = "agent/durable-vcs/status-001";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable status",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-status-root",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();

        apply_durable_session_operation(
            &stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/modified.txt".to_string(),
                content: b"after".to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_105,
        )
        .await;
        apply_durable_session_operation(
            &stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/added.txt".to_string(),
                content: b"added".to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_106,
        )
        .await;
        apply_durable_session_operation(
            &stores,
            session_ref,
            DurableMutationOperation::Delete {
                path: "/demo/deleted.txt".to_string(),
                recursive: false,
            },
            1_725_000_107,
        )
        .await;
        apply_durable_session_operation(
            &stores,
            session_ref,
            DurableMutationOperation::Delete {
                path: "/demo/type-change".to_string(),
                recursive: true,
            },
            1_725_000_108,
        )
        .await;
        apply_durable_session_operation(
            &stores,
            session_ref,
            DurableMutationOperation::WriteFile {
                path: "/demo/type-change".to_string(),
                content: b"type changed".to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            1_725_000_109,
        )
        .await;
        apply_durable_session_operation(
            &stores,
            session_ref,
            DurableMutationOperation::SetMetadata {
                path: "/demo/meta.txt".to_string(),
                update: MetadataUpdate {
                    mime_type: None,
                    custom_attrs: BTreeMap::from([("reviewed".to_string(), "true".to_string())]),
                    remove_custom_attrs: Vec::new(),
                },
            },
            1_725_000_110,
        )
        .await;

        let base = stores
            .commits
            .get(&RepoId::local(), base_commit)
            .await
            .unwrap()
            .expect("base commit");
        let head_ref = stores
            .refs
            .get(&RepoId::local(), &RefName::new(session_ref).unwrap())
            .await
            .unwrap()
            .expect("session ref");
        let head = stores
            .commits
            .get(&RepoId::local(), head_ref.target)
            .await
            .unwrap()
            .expect("head commit");
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_status(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        for expected in [
            "Changes:",
            "A /demo/added.txt",
            "M /demo/modified.txt",
            "D /demo/deleted.txt",
            "T /demo/type-change",
            "m /demo/meta.txt",
            "target ref: main",
            "session ref: agent/durable-vcs/status-001",
            &format!("base commit: {}", base_commit.to_hex()),
            &format!("head commit: {}", head_ref.target.to_hex()),
            &format!("base root tree: {}", base.root_tree.to_hex()),
            &format!("head root tree: {}", head.root_tree.to_hex()),
            "changed path count: 5",
        ] {
            assert!(body.contains(expected), "missing {expected:?} in:\n{body}");
        }
    }

    #[tokio::test]
    async fn guarded_durable_status_keeps_admin_gate_for_non_root_global_read() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let stores = StratumStores::local_memory();
        seed_durable_status_base(&stores).await;
        let state = guarded_durable_commit_state(db, stores);

        let response = vcs_status(State(state), user_headers("bob"))
            .await
            .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    }

    #[tokio::test]
    async fn guarded_durable_status_filters_scoped_workspace_to_mounted_readable_paths() {
        let stores = StratumStores::local_memory();
        seed_durable_scoped_status_base(&stores).await;
        let session_ref = "agent/durable-vcs/status-scope";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable scoped status",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-status-scope-root",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/visible.txt",
            b"visible",
            1_725_000_205,
        )
        .await;
        apply_durable_session_write(
            &stores,
            session_ref,
            "/secret/private-token.txt",
            b"secret",
            1_725_000_206,
        )
        .await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_status(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        assert!(body.contains("A /demo/visible.txt"), "body:\n{body}");
        assert!(
            body.contains("Files: 1, Total size: 7 bytes"),
            "body:\n{body}"
        );
        assert!(body.contains("changed path count: 1"), "body:\n{body}");
        assert!(!body.contains("/secret"), "body:\n{body}");
        assert!(!body.contains("private-token"), "body:\n{body}");
    }

    #[tokio::test]
    async fn guarded_durable_status_descends_to_nested_scoped_read_prefixes() {
        let stores = StratumStores::local_memory();
        seed_durable_scoped_status_base(&stores).await;
        let session_ref = "agent/durable-vcs/status-nested-scope";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable nested scoped status",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-status-nested-scope-root",
                ROOT_UID,
                vec!["/demo/public".to_string()],
                vec!["/demo/public".to_string()],
            )
            .await
            .unwrap();
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/public/visible.txt",
            b"visible",
            1_725_000_207,
        )
        .await;
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/private/hidden.txt",
            b"hidden",
            1_725_000_208,
        )
        .await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_status(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        assert!(body.contains("A /demo/public/visible.txt"), "body:\n{body}");
        assert!(
            body.contains("Files: 1, Total size: 7 bytes"),
            "body:\n{body}"
        );
        assert!(body.contains("changed path count: 1"), "body:\n{body}");
        assert!(!body.contains("/demo/private"), "body:\n{body}");
        assert!(!body.contains("hidden"), "body:\n{body}");
    }

    #[tokio::test]
    async fn guarded_durable_diff_renders_mounted_session_changes_without_local_vcs_state() {
        let object_store = Arc::new(DiffBlobAccessObjectStore::default());
        let mut stores = StratumStores::local_memory();
        stores.objects = object_store.clone();
        let (base_commit, fixture) = seed_durable_diff_base(&stores).await;
        let session_ref = "agent/durable-vcs/diff-001";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable diff",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-diff-root",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        apply_durable_diff_session_changes(&stores, session_ref).await;
        let repo_id = RepoId::local();
        let base_commit_record = stores
            .commits
            .get(&repo_id, base_commit)
            .await
            .unwrap()
            .unwrap();
        let session_head = stores
            .refs
            .get(&repo_id, &RefName::new(session_ref).unwrap())
            .await
            .unwrap()
            .unwrap()
            .target;
        let session_commit_record = stores
            .commits
            .get(&repo_id, session_head)
            .await
            .unwrap()
            .unwrap();
        object_store
            .deny_blob_gets([
                fixture.meta_content,
                fixture.binary_before,
                fixture.binary_after,
                fixture.oversized_before,
                fixture.oversized_after,
                fixture.type_changed_after,
            ])
            .await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_diff(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
            Query(DiffQuery { path: None }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        for expected in [
            "diff -- /demo/modified.txt",
            "@@ -",
            "-before durable change\n",
            "+after durable change\n",
            "diff -- /demo/added.txt",
            "+added durable\n",
            "diff -- /demo/deleted.txt",
            "-delete me\n",
            "diff -- /demo/meta.txt",
            "metadata:\n",
            "- mime_type: <unset>\n",
            "+ mime_type: text/plain\n",
            "- custom_attrs.reviewed: <unset>\n",
            "+ custom_attrs.reviewed: true\n",
            "diff -- /demo/binary.bin",
            "reason: binary or non-UTF-8 content is not supported by text diff\n",
            "mime=application/octet-stream",
            "diff -- /demo/non-utf8.bin",
            "type=file mime=<unset>",
            "diff -- /demo/oversized.txt",
            "reason: text diff is too large to render\n",
            "diff -- /demo/type-change",
            "reason: path kind changed; text diff is not available\n",
            "before: object=<none> size=0 type=directory mime=<unset>\n",
            "after: object=",
            "type=file mime=text/plain",
            "target ref: main\n",
            &format!("session ref: {session_ref}\n"),
            &format!("base commit: {}\n", base_commit.to_hex()),
            &format!("head commit: {}\n", session_head.to_hex()),
            &format!(
                "base root tree: {}\n",
                base_commit_record.root_tree.to_hex()
            ),
            &format!(
                "head root tree: {}\n",
                session_commit_record.root_tree.to_hex()
            ),
            "changed path count: 11\n",
        ] {
            assert!(body.contains(expected), "missing {expected:?} in:\n{body}");
        }
        assert!(
            body.contains(&format!(
                "before: object={} size=4",
                fixture.binary_before.to_hex()
            )),
            "body:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "after: object={} size=4",
                fixture.binary_after.to_hex()
            )),
            "body:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "before: object={} size=3",
                fixture.non_utf8_before.to_hex()
            )),
            "body:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "after: object={} size=3",
                fixture.non_utf8_after.to_hex()
            )),
            "body:\n{body}"
        );
        assert!(
            body.contains(&format!(
                "after: object={} size={}",
                fixture.oversized_after.to_hex(),
                DURABLE_DIFF_OVERSIZED_TEXT_LEN
            )),
            "body:\n{body}"
        );
        assert!(
            !body.contains(" shared line 01\n"),
            "grouped hunks should not render distant equal lines:\n{body}"
        );
    }

    #[tokio::test]
    async fn guarded_durable_diff_exact_path_filter_does_not_fetch_filtered_blob_bytes() {
        let object_store = Arc::new(DiffBlobAccessObjectStore::default());
        let mut stores = StratumStores::local_memory();
        stores.objects = object_store.clone();
        let (_base_commit, fixture) = seed_durable_diff_base(&stores).await;
        let session_ref = "agent/durable-vcs/diff-exact-filter";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable diff exact",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-diff-exact-root",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        apply_durable_diff_session_changes(&stores, session_ref).await;
        object_store
            .deny_blob_gets(fixture.non_modified_blob_ids())
            .await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_diff(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
            Query(DiffQuery {
                path: Some("/demo/modified.txt".to_string()),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        assert!(body.contains("diff -- /demo/modified.txt"), "body:\n{body}");
        assert!(body.contains("-before durable change\n"), "body:\n{body}");
        assert!(body.contains("+after durable change\n"), "body:\n{body}");
        for excluded in [
            "/demo/added.txt",
            "/demo/binary.bin",
            "/demo/deleted.txt",
            "/demo/filtered.txt",
            "/demo/meta.txt",
            "/demo/nested/child.txt",
            "/demo/non-utf8.bin",
            "/demo/oversized.txt",
            "/demo/sibling.txt",
            "/demo/type-change",
        ] {
            assert!(!body.contains(excluded), "leaked {excluded:?} in:\n{body}");
        }
    }

    #[tokio::test]
    async fn guarded_durable_diff_descendant_path_filter_includes_only_children() {
        let stores = StratumStores::local_memory();
        seed_durable_diff_base(&stores).await;
        let session_ref = "agent/durable-vcs/diff-prefix-filter";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable diff prefix",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-diff-prefix-root",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        apply_durable_diff_session_changes(&stores, session_ref).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_diff(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
            Query(DiffQuery {
                path: Some("/demo/nested".to_string()),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        assert!(
            body.contains("diff -- /demo/nested/child.txt"),
            "body:\n{body}"
        );
        assert!(body.contains("-nested before\n"), "body:\n{body}");
        assert!(body.contains("+nested after\n"), "body:\n{body}");
        assert!(!body.contains("/demo/sibling.txt"), "body:\n{body}");
        assert!(!body.contains("/demo/modified.txt"), "body:\n{body}");
    }

    #[tokio::test]
    async fn guarded_durable_diff_filters_scoped_workspace_to_mounted_readable_paths() {
        let stores = StratumStores::local_memory();
        seed_durable_scoped_status_base(&stores).await;
        let session_ref = "agent/durable-vcs/diff-scope";
        let workspace = create_local_repo_workspace_with_refs(
            &stores,
            "durable scoped diff",
            "/demo",
            MAIN_REF,
            Some(session_ref),
        )
        .await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-diff-scope-root",
                ROOT_UID,
                vec!["/demo/public".to_string()],
                vec!["/demo/public".to_string()],
            )
            .await
            .unwrap();
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/public/visible.txt",
            b"visible",
            1_725_000_316,
        )
        .await;
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/private/hidden-token.txt",
            b"hidden",
            1_725_000_317,
        )
        .await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_diff(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
            Query(DiffQuery { path: None }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = text_body(response).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");
        assert!(
            body.contains("diff -- /demo/public/visible.txt"),
            "body:\n{body}"
        );
        assert!(body.contains("+visible\n"), "body:\n{body}");
        for forbidden in ["/demo/private", "hidden-token", "/secret"] {
            assert!(
                !body.contains(forbidden),
                "scoped durable diff leaked {forbidden:?} in:\n{body}"
            );
        }
    }

    #[tokio::test]
    async fn guarded_durable_diff_redacts_internal_store_failures_without_request_path_leaks() {
        let stores = StratumStores::local_memory();
        stores
            .refs
            .update(RefUpdate {
                repo_id: RepoId::local(),
                name: RefName::new(MAIN_REF).unwrap(),
                target: synthetic_commit_id("missing durable diff commit"),
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let request_path = "/tenant/alice/private-token";

        let response = vcs_diff(
            State(state),
            user_headers("root"),
            Query(DiffQuery {
                path: Some(request_path.to_string()),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "body: {body}");
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("durable committed read failed"), "{error}");
        for forbidden in [request_path, "alice", "private-token"] {
            assert!(
                !error.contains(forbidden),
                "guarded durable diff leaked {forbidden:?}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn guarded_durable_commit_creates_durable_state_replays_and_skips_local_vcs_commit() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch durable.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write durable.txt durable-content", &mut root)
            .await
            .unwrap();
        let stores = StratumStores::local_memory();
        let workspace = stores
            .workspace_metadata
            .create_workspace("durable route", "/")
            .await
            .unwrap();
        let state = guarded_durable_commit_state(db.clone(), stores.clone());
        let mut headers = workspace_headers("root", workspace.id);
        headers.insert("idempotency-key", "durable-commit-replay".parse().unwrap());
        let request = || CommitRequest {
            message: "durable route commit".to_string(),
        };

        let first_response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = json_body(first_response).await;
        let commit_hash = first_body["hash"].as_str().expect("commit hash");
        assert_eq!(commit_hash.len(), 64);
        assert_eq!(first_body["message"], "durable route commit");
        assert_eq!(first_body["author"], "root");

        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target.to_hex(), commit_hash);
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            1
        );
        assert!(
            stores
                .objects
                .contains(
                    &RepoId::local(),
                    stores
                        .commits
                        .get(&RepoId::local(), main.target)
                        .await
                        .unwrap()
                        .unwrap()
                        .root_tree,
                    ObjectKind::Tree,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            stores
                .workspace_metadata
                .get_workspace(workspace.id)
                .await
                .unwrap()
                .unwrap()
                .head_commit
                .as_deref(),
            Some(commit_hash)
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsCommit);
        assert_eq!(events[1].resource.id.as_deref(), Some(commit_hash));
        let expected_workspace_id = workspace.id.to_string();
        assert_eq!(
            events[1].details.get("workspace_id").map(String::as_str),
            Some(expected_workspace_id.as_str())
        );
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("durable route commit")
        );
        assert_eq!(db.vcs_log().await.len(), 0);

        let replay_response = vcs_commit(State(state), headers, Json(request()))
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
        let expected_replay_body = vcs_commit_idempotency_body(&first_body);
        assert_eq!(replay_body, expected_replay_body);
        assert_eq!(replay_body["message"], JsonValue::Null);
        assert!(
            !serde_json::to_string(&replay_body)
                .unwrap()
                .contains("durable route commit")
        );
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            1
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 1);
    }

    #[tokio::test]
    async fn guarded_durable_commit_uses_session_ref_snapshot_without_local_state() {
        let stores = StratumStores::local_memory();
        let base_commit = seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-vcs/session-001";
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable commit", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/session.txt",
            b"session durable content",
            1_725_000_005,
        )
        .await;

        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());
        let response = vcs_commit(
            State(state.clone()),
            workspace_headers("root", workspace.id),
            Json(CommitRequest {
                message: "promote session".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        let commit_hash = body["hash"].as_str().expect("commit hash");
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target.to_hex(), commit_hash);
        assert_ne!(main.target, base_commit);
        let commit = stores
            .commits
            .get(&RepoId::local(), main.target)
            .await
            .unwrap()
            .expect("user commit");
        assert_eq!(commit.parents, vec![base_commit]);
        assert_eq!(commit.message, "promote session");
        assert!(
            commit
                .changed_paths
                .iter()
                .any(|change| change.path == "/demo/session.txt")
        );
        let read_repo = RepoId::local();
        let reader = DurableCommittedFsReader::new(
            &read_repo,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        let (content, _) = reader
            .cat_with_stat_as("/demo/session.txt", &Session::root())
            .await
            .unwrap();
        assert_eq!(content, b"session durable content");
        assert_eq!(state.db.vcs_log().await.len(), 0);
    }

    #[tokio::test]
    async fn guarded_durable_commit_accepts_session_ref_after_previous_promotion() {
        let stores = StratumStores::local_memory();
        seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-vcs/session-after-promotion";
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs(
                "durable commit sequence",
                "/demo",
                MAIN_REF,
                Some(session_ref),
            )
            .await
            .unwrap();
        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/first.txt",
            b"first durable content",
            1_725_000_008,
        )
        .await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let first_response = vcs_commit(
            State(state.clone()),
            workspace_headers("root", workspace.id),
            Json(CommitRequest {
                message: "promote first session".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_hash = json_body(first_response).await["hash"]
            .as_str()
            .expect("first hash")
            .to_string();

        apply_durable_session_write(
            &stores,
            session_ref,
            "/demo/second.txt",
            b"second durable content",
            1_725_000_009,
        )
        .await;
        let second_response = vcs_commit(
            State(state.clone()),
            workspace_headers("root", workspace.id),
            Json(CommitRequest {
                message: "promote second session".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(second_response.status(), StatusCode::OK);
        let second_hash = json_body(second_response).await["hash"]
            .as_str()
            .expect("second hash")
            .to_string();
        let second_commit = stores
            .commits
            .get(
                &RepoId::local(),
                CommitId::from(ObjectId::from_hex(&second_hash).unwrap()),
            )
            .await
            .unwrap()
            .expect("second commit");
        assert_eq!(second_commit.parents[0].to_hex(), first_hash);

        let read_repo = RepoId::local();
        let reader = DurableCommittedFsReader::new(
            &read_repo,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        let (first, _) = reader
            .cat_with_stat_as("/demo/first.txt", &Session::root())
            .await
            .unwrap();
        let (second, _) = reader
            .cat_with_stat_as("/demo/second.txt", &Session::root())
            .await
            .unwrap();
        assert_eq!(first, b"first durable content");
        assert_eq!(second, b"second durable content");
    }

    #[tokio::test]
    async fn guarded_durable_commit_rejects_session_ref_based_on_stale_main() {
        let stores = StratumStores::local_memory();
        let base_commit = seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-vcs/session-stale-main";
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable commit", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
        let repo_id = RepoId::local();
        let operation = DurableMutationOperation::WriteFile {
            path: "/demo/session.txt".to_string(),
            content: b"stale session content".to_vec(),
            mode: 0o644,
            uid: ROOT_UID,
            gid: crate::auth::ROOT_GID,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        };
        let token = durable_mutation_test_policy_token(&operation);
        let engine = DurableMutationEngine::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        engine
            .with_policy_token(&token)
            .apply(DurableMutationInput {
                base_ref: RefName::new(MAIN_REF).unwrap(),
                session_ref: RefName::new(session_ref).unwrap(),
                operation,
                author: "agent".to_string(),
                timestamp: 1_725_000_006,
                preflight_session: None,
            })
            .await
            .unwrap();

        let main_ref = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        let base = stores
            .commits
            .get(&RepoId::local(), base_commit)
            .await
            .unwrap()
            .expect("base commit");
        let advanced_commit = synthetic_commit_id("durable-vcs-concurrent-main");
        stores
            .commits
            .insert(CommitRecord {
                repo_id: RepoId::local(),
                id: advanced_commit,
                root_tree: base.root_tree,
                parents: vec![base_commit],
                timestamp: 1_725_000_007,
                message: "concurrent main".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: RepoId::local(),
                name: RefName::new(MAIN_REF).unwrap(),
                target: advanced_commit,
                expectation: RefExpectation::Matches {
                    target: base_commit,
                    version: main_ref.version,
                },
            })
            .await
            .unwrap();

        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());
        let response = vcs_commit(
            State(state),
            workspace_headers("root", workspace.id),
            Json(CommitRequest {
                message: "promote stale session".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = json_body(response).await;
        assert!(
            body["error"]
                .as_str()
                .expect("error")
                .contains("ref compare-and-swap mismatch")
        );
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, advanced_commit);
    }

    #[tokio::test]
    async fn guarded_durable_commit_rejects_same_root_internal_session_ref_non_descendant() {
        let stores = StratumStores::local_memory();
        let base_commit = seed_durable_workspace_base(&stores).await;
        let base = stores
            .commits
            .get(&RepoId::local(), base_commit)
            .await
            .unwrap()
            .expect("base commit");
        let session_ref = "agent/durable-vcs/session-forged-same-root";
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable commit", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
        let unrelated_parent = synthetic_commit_id("durable-vcs-unrelated-parent");
        stores
            .commits
            .insert(CommitRecord {
                repo_id: RepoId::local(),
                id: unrelated_parent,
                root_tree: base.root_tree,
                parents: Vec::new(),
                timestamp: 1_725_000_010,
                message: "unrelated user commit".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        let forged_session_commit = synthetic_commit_id("durable-vcs-forged-same-root");
        stores
            .commits
            .insert(CommitRecord {
                repo_id: RepoId::local(),
                id: forged_session_commit,
                root_tree: base.root_tree,
                parents: vec![unrelated_parent],
                timestamp: 1_725_000_011,
                message: DURABLE_MUTATION_COMMIT_MESSAGE.to_string(),
                author: "agent".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: RepoId::local(),
                name: RefName::new(session_ref).unwrap(),
                target: forged_session_commit,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_commit(
            State(state),
            workspace_headers("root", workspace.id),
            Json(CommitRequest {
                message: "promote forged same-root session".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = json_body(response).await;
        assert!(
            body["error"]
                .as_str()
                .expect("error")
                .contains("ref compare-and-swap mismatch")
        );
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, base_commit);
    }

    struct RefRacingObjectStore {
        inner: crate::backend::SharedObjectStore,
        refs: crate::backend::SharedRefStore,
        racing_target: CommitId,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl ObjectStore for RefRacingObjectStore {
        async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                self.refs
                    .update(RefUpdate {
                        repo_id: RepoId::local(),
                        name: RefName::new(MAIN_REF).unwrap(),
                        target: self.racing_target,
                        expectation: RefExpectation::MustNotExist,
                    })
                    .await?;
            }
            self.inner.put(write).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<Option<StoredObject>, VfsError> {
            self.inner.get(repo_id, id, expected_kind).await
        }

        async fn contains(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<bool, VfsError> {
            self.inner.contains(repo_id, id, expected_kind).await
        }
    }

    struct AckLostCommitStore {
        inner: crate::backend::SharedCommitStore,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CommitStore for AckLostCommitStore {
        async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
            let inserted = self.inner.insert(record).await?;
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "commit metadata ack lost with private-store-detail".to_string(),
                });
            }
            Ok(inserted)
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            id: CommitId,
        ) -> Result<Option<CommitRecord>, VfsError> {
            self.inner.get(repo_id, id).await
        }

        async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
            self.inner.contains(repo_id, id).await
        }

        async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
            self.inner.list(repo_id).await
        }
    }

    struct AckLostUnreadableCommitStore {
        inner: crate::backend::SharedCommitStore,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CommitStore for AckLostUnreadableCommitStore {
        async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
            let inserted = self.inner.insert(record).await?;
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "commit metadata ack lost with private-store-detail".to_string(),
                });
            }
            Ok(inserted)
        }

        async fn get(
            &self,
            _repo_id: &RepoId,
            _id: CommitId,
        ) -> Result<Option<CommitRecord>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "commit metadata recovery failed with private-store-detail".to_string(),
            })
        }

        async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
            self.inner.contains(repo_id, id).await
        }

        async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
            self.inner.list(repo_id).await
        }
    }

    struct AckLostRefStore {
        inner: crate::backend::SharedRefStore,
        fired: AtomicBool,
    }

    struct CasMismatchRefStore {
        inner: crate::backend::SharedRefStore,
        fail_updates: AtomicBool,
    }

    #[async_trait::async_trait]
    impl RefStore for CasMismatchRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            self.inner.get(repo_id, name).await
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            if update.name.as_str() == MAIN_REF && self.fail_updates.load(Ordering::SeqCst) {
                return Err(VfsError::InvalidArgs {
                    message: "ref compare-and-swap mismatch".to_string(),
                });
            }
            self.inner.update(update).await
        }

        async fn update_source_checked(
            &self,
            update: crate::backend::SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.inner.update_source_checked(update).await
        }
    }

    #[derive(Debug, Default)]
    struct FailingPostCasRecoveryStore {
        inner: InMemoryDurableCorePostCasRecoveryClaimStore,
    }

    #[async_trait::async_trait]
    impl DurableCorePostCasRecoveryClaimStore for FailingPostCasRecoveryStore {
        async fn enqueue(
            &self,
            _target: DurableCorePostCasRecoveryTarget,
            _now_millis: u64,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "post-CAS recovery enqueue failed with private-store-detail".to_string(),
            })
        }

        async fn enqueue_with_context(
            &self,
            _target: DurableCorePostCasRecoveryTarget,
            _context: DurableCorePostCasRecoveryContext,
            _now_millis: u64,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "post-CAS recovery enqueue failed with private-store-detail".to_string(),
            })
        }

        async fn claim(
            &self,
            request: DurableCorePostCasRecoveryClaimRequest,
        ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError> {
            self.inner.claim(request).await
        }

        async fn complete(
            &self,
            claim: &DurableCorePostCasRecoveryClaim,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            self.inner.complete(claim, now_millis).await
        }

        async fn record_failure(
            &self,
            claim: &DurableCorePostCasRecoveryClaim,
            diagnosis: &str,
            backoff: Duration,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            self.inner
                .record_failure(claim, diagnosis, backoff, now_millis)
                .await
        }

        async fn poison(
            &self,
            claim: &DurableCorePostCasRecoveryClaim,
            diagnosis: &str,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            self.inner.poison(claim, diagnosis, now_millis).await
        }

        async fn list(
            &self,
            limit: usize,
        ) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
            self.inner.list(limit).await
        }

        async fn has_unresolved_for_ref(
            &self,
            repo_id: &RepoId,
            ref_name: &str,
        ) -> Result<bool, VfsError> {
            self.inner.has_unresolved_for_ref(repo_id, ref_name).await
        }

        async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
            self.inner.counts().await
        }

        async fn counts_for_repo(
            &self,
            repo_id: &RepoId,
        ) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
            self.inner.counts_for_repo(repo_id).await
        }
    }

    #[derive(Debug, Default)]
    struct FailingPreVisibilityRecoveryStore;

    #[async_trait::async_trait]
    impl DurableCorePreVisibilityRecoveryStore for FailingPreVisibilityRecoveryStore {
        async fn record(
            &self,
            _record: DurableCorePreVisibilityRecoveryRecord,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery record failed with private-store-detail"
                    .to_string(),
            })
        }

        async fn claim(
            &self,
            _request: DurableCorePreVisibilityRecoveryClaimRequest,
        ) -> Result<Option<DurableCorePreVisibilityRecoveryClaim>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery claim failed with private-store-detail"
                    .to_string(),
            })
        }

        async fn resolve(
            &self,
            _claim: &DurableCorePreVisibilityRecoveryClaim,
            _now_millis: u64,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery resolve failed with private-store-detail"
                    .to_string(),
            })
        }

        async fn record_failure(
            &self,
            _claim: &DurableCorePreVisibilityRecoveryClaim,
            _diagnosis: &str,
            _backoff: Duration,
            _now_millis: u64,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery failure failed with private-store-detail"
                    .to_string(),
            })
        }

        async fn poison(
            &self,
            _claim: &DurableCorePreVisibilityRecoveryClaim,
            _diagnosis: &str,
            _now_millis: u64,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery poison failed with private-store-detail"
                    .to_string(),
            })
        }

        async fn list(
            &self,
            _limit: usize,
        ) -> Result<Vec<DurableCorePreVisibilityRecoveryStatus>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery list failed with private-store-detail"
                    .to_string(),
            })
        }

        async fn has_unresolved_for_ref(
            &self,
            _repo_id: &RepoId,
            _ref_name: &str,
        ) -> Result<bool, VfsError> {
            Err(VfsError::CorruptStore {
                message:
                    "pre-visibility recovery unresolved check failed with private-store-detail"
                        .to_string(),
            })
        }

        async fn counts(&self) -> Result<DurableCorePreVisibilityRecoveryCounts, VfsError> {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery counts failed with private-store-detail"
                    .to_string(),
            })
        }
    }

    #[derive(Debug, Default)]
    struct FailingIdempotencyRecoveryStore {
        inner: InMemoryDurableCorePostCasRecoveryClaimStore,
    }

    #[async_trait::async_trait]
    impl DurableCorePostCasRecoveryClaimStore for FailingIdempotencyRecoveryStore {
        async fn enqueue(
            &self,
            target: DurableCorePostCasRecoveryTarget,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            if target.step() == DurableCorePostCasStep::IdempotencyCompletion {
                return Err(VfsError::CorruptStore {
                    message: "idempotency recovery enqueue failed with private-store-detail"
                        .to_string(),
                });
            }
            self.inner.enqueue(target, now_millis).await
        }

        async fn enqueue_with_context(
            &self,
            target: DurableCorePostCasRecoveryTarget,
            context: DurableCorePostCasRecoveryContext,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            if target.step() == DurableCorePostCasStep::IdempotencyCompletion {
                return Err(VfsError::CorruptStore {
                    message: "idempotency recovery enqueue failed with private-store-detail"
                        .to_string(),
                });
            }
            self.inner
                .enqueue_with_context(target, context, now_millis)
                .await
        }

        async fn claim(
            &self,
            request: DurableCorePostCasRecoveryClaimRequest,
        ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError> {
            self.inner.claim(request).await
        }

        async fn complete(
            &self,
            claim: &DurableCorePostCasRecoveryClaim,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            self.inner.complete(claim, now_millis).await
        }

        async fn record_failure(
            &self,
            claim: &DurableCorePostCasRecoveryClaim,
            diagnosis: &str,
            backoff: Duration,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            self.inner
                .record_failure(claim, diagnosis, backoff, now_millis)
                .await
        }

        async fn poison(
            &self,
            claim: &DurableCorePostCasRecoveryClaim,
            diagnosis: &str,
            now_millis: u64,
        ) -> Result<(), VfsError> {
            self.inner.poison(claim, diagnosis, now_millis).await
        }

        async fn list(
            &self,
            limit: usize,
        ) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
            self.inner.list(limit).await
        }

        async fn has_unresolved_for_ref(
            &self,
            repo_id: &RepoId,
            ref_name: &str,
        ) -> Result<bool, VfsError> {
            self.inner.has_unresolved_for_ref(repo_id, ref_name).await
        }

        async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
            self.inner.counts().await
        }

        async fn counts_for_repo(
            &self,
            repo_id: &RepoId,
        ) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
            self.inner.counts_for_repo(repo_id).await
        }
    }

    #[derive(Debug, Default)]
    struct FailingCompleteIdempotencyStore {
        inner: InMemoryIdempotencyStore,
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
                message: "idempotency completion failed with private-token".to_string(),
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

    #[derive(Debug, Default)]
    struct FailingOnceCompleteIdempotencyStore {
        inner: InMemoryIdempotencyStore,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl IdempotencyStore for FailingOnceCompleteIdempotencyStore {
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
            reservation: &IdempotencyReservation,
            status_code: u16,
            response_body: serde_json::Value,
        ) -> Result<(), VfsError> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "idempotency completion failed with private-token".to_string(),
                });
            }
            self.inner
                .complete(reservation, status_code, response_body)
                .await
        }

        async fn complete_with_classification(
            &self,
            reservation: &IdempotencyReservation,
            status_code: u16,
            response_body: serde_json::Value,
            classification: IdempotencyReplayClassification,
        ) -> Result<(), VfsError> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "idempotency completion failed with private-token".to_string(),
                });
            }
            self.inner
                .complete_with_classification(
                    reservation,
                    status_code,
                    response_body,
                    classification,
                )
                .await
        }

        async fn complete_or_match(
            &self,
            reservation: &IdempotencyReservation,
            status_code: u16,
            response_body: serde_json::Value,
        ) -> Result<(), VfsError> {
            self.inner
                .complete_or_match(reservation, status_code, response_body)
                .await
        }

        async fn complete_or_match_with_classification(
            &self,
            reservation: &IdempotencyReservation,
            status_code: u16,
            response_body: serde_json::Value,
            classification: IdempotencyReplayClassification,
        ) -> Result<(), VfsError> {
            self.inner
                .complete_or_match_with_classification(
                    reservation,
                    status_code,
                    response_body,
                    classification,
                )
                .await
        }

        async fn abort(&self, reservation: &IdempotencyReservation) {
            self.inner.abort(reservation).await;
        }
    }

    #[async_trait::async_trait]
    impl RefStore for AckLostRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            self.inner.get(repo_id, name).await
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            let updated = self.inner.update(update).await?;
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "ref update ack lost with private-store-detail".to_string(),
                });
            }
            Ok(updated)
        }

        async fn update_source_checked(
            &self,
            update: crate::backend::SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.inner.update_source_checked(update).await
        }
    }

    struct FailingRefVisibilityStore {
        inner: crate::backend::SharedRefStore,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl RefStore for FailingRefVisibilityStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            self.inner.get(repo_id, name).await
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "ref visibility failed with private-store-detail".to_string(),
                });
            }
            self.inner.update(update).await
        }

        async fn update_source_checked(
            &self,
            update: crate::backend::SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.inner.update_source_checked(update).await
        }
    }

    struct AckLostUnreadableRefStore {
        inner: crate::backend::SharedRefStore,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl RefStore for AckLostUnreadableRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            if !self.fired.load(Ordering::SeqCst) {
                return self.inner.get(repo_id, name).await;
            }
            Err(VfsError::CorruptStore {
                message: "ref visibility recovery failed with private-store-detail".to_string(),
            })
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            let updated = self.inner.update(update).await?;
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "ref update ack lost with private-store-detail".to_string(),
                });
            }
            Ok(updated)
        }

        async fn update_source_checked(
            &self,
            update: crate::backend::SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.inner.update_source_checked(update).await
        }
    }

    struct AckLostTemporarilyUnreadableRefStore {
        inner: crate::backend::SharedRefStore,
        fired: AtomicBool,
        get_failures_remaining: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl RefStore for AckLostTemporarilyUnreadableRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            if self.fired.load(Ordering::SeqCst)
                && self
                    .get_failures_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                        if value > 0 { Some(value - 1) } else { None }
                    })
                    .is_ok()
            {
                return Err(VfsError::CorruptStore {
                    message: "temporary ref visibility recovery failure with private-store-detail"
                        .to_string(),
                });
            }
            self.inner.get(repo_id, name).await
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            let updated = self.inner.update(update).await?;
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::CorruptStore {
                    message: "ref update ack lost with private-store-detail".to_string(),
                });
            }
            Ok(updated)
        }

        async fn update_source_checked(
            &self,
            update: crate::backend::SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.inner.update_source_checked(update).await
        }
    }

    #[tokio::test]
    async fn guarded_durable_commit_stale_main_cas_conflicts_and_aborts_idempotency() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch race.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write race.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        let racing_target = CommitId::from(ObjectId::from_bytes(b"durable-racer"));
        stores.objects = Arc::new(RefRacingObjectStore {
            inner: stores.objects.clone(),
            refs: stores.refs.clone(),
            racing_target,
            fired: AtomicBool::new(false),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-cas-race");

        let response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: "loses CAS".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
                .await
                .unwrap()
                .unwrap()
                .target,
            racing_target
        );
        assert_eq!(
            stores
                .workspace_metadata
                .list_workspaces()
                .await
                .unwrap()
                .len(),
            0
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);

        let session = session_from_headers(&state, &headers).await.unwrap();
        let key = crate::idempotency::IdempotencyKey::parse_header_value(
            headers.get("idempotency-key").unwrap(),
        )
        .unwrap();
        let scope = vcs_idempotency_scope(VCS_COMMIT_IDEMPOTENCY_ROUTE);
        let fingerprint = request_fingerprint(
            &scope,
            &serde_json::json!({
                "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": Option::<Uuid>::None,
                "message": "loses CAS",
            }),
        )
        .unwrap();
        assert!(matches!(
            stores
                .idempotency
                .begin(&scope, &key, &fingerprint)
                .await
                .unwrap(),
            IdempotencyBegin::Execute(_)
        ));
    }

    #[tokio::test]
    async fn guarded_durable_commit_recovers_metadata_insert_ack_loss_for_idempotency() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch metadata-ack.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write metadata-ack.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.commits = Arc::new(AckLostCommitStore {
            inner: stores.commits.clone(),
            fired: AtomicBool::new(false),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-metadata-ack-lost");
        let request = || CommitRequest {
            message: "metadata ack lost".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        let commit_hash = body["hash"].as_str().expect("commit hash");
        assert_eq!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
                .await
                .unwrap()
                .unwrap()
                .target
                .to_hex(),
            commit_hash
        );

        let replay = vcs_commit(State(state), headers, Json(request()))
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
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            1
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 1);
    }

    #[tokio::test]
    async fn guarded_durable_commit_recovers_ref_visibility_ack_loss_for_idempotency() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch ref-ack.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write ref-ack.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.refs = Arc::new(AckLostRefStore {
            inner: stores.refs.clone(),
            fired: AtomicBool::new(false),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-ref-ack-lost");
        let request = || CommitRequest {
            message: "ref ack lost".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        let commit_hash = body["hash"].as_str().expect("commit hash");
        assert_eq!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
                .await
                .unwrap()
                .unwrap()
                .target
                .to_hex(),
            commit_hash
        );

        let replay = vcs_commit(State(state), headers, Json(request()))
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
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            1
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 1);
    }

    #[tokio::test]
    async fn guarded_durable_commit_metadata_recovery_failure_does_not_replay_partial() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch metadata-unknown.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write metadata-unknown.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.commits = Arc::new(AckLostUnreadableCommitStore {
            inner: stores.commits.clone(),
            fired: AtomicBool::new(false),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-metadata-unknown");
        let request = || CommitRequest {
            message: "metadata unknown".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "durable commit visibility recovery is required"
        );
        assert!(
            !serde_json::to_string(&body)
                .unwrap()
                .contains("private-store-detail")
        );
        assert!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            1
        );
        assert!(stores.post_cas_recovery.list(10).await.unwrap().is_empty());
        let pre_visibility = stores.pre_visibility_recovery.list(10).await.unwrap();
        assert_eq!(pre_visibility.len(), 1);
        assert_eq!(
            pre_visibility[0].target().stage(),
            DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert
        );
        assert_eq!(
            pre_visibility[0].state(),
            DurableCorePreVisibilityRecoveryState::Pending
        );
        assert!(pre_visibility[0].has_idempotency_reservation());
        let persisted_commit = stores.commits.list(&RepoId::local()).await.unwrap();
        assert_eq!(
            pre_visibility[0].target().commit_id(),
            persisted_commit[0].id
        );

        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"), None)
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["pre_visibility_count"], 1);
        assert_eq!(status_body["pre_visibility_page_count"], 1);
        assert_eq!(status_body["pre_visibility_counts"]["pending"], 1);
        assert_eq!(
            status_body["pre_visibility"][0]["stage"],
            "commit_metadata_insert"
        );
        assert_eq!(
            status_body["pre_visibility"][0]["commit_id"],
            persisted_commit[0].id.to_hex()
        );
        let status_rendered = serde_json::to_string(&status_body).unwrap();
        assert!(!status_rendered.contains("metadata unknown"));
        assert!(!status_rendered.contains("durable-metadata-unknown"));
        assert!(!status_rendered.contains("private-store-detail"));

        let replay = vcs_commit(State(state), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(replay).await["error"],
            http_idempotency::IDEMPOTENCY_IN_PROGRESS_MESSAGE
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
    }

    #[tokio::test]
    async fn guarded_durable_commit_metadata_recovery_status_persistence_failure_is_redacted() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch metadata-status-fails.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write metadata-status-fails.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.commits = Arc::new(AckLostUnreadableCommitStore {
            inner: stores.commits.clone(),
            fired: AtomicBool::new(false),
        });
        stores.pre_visibility_recovery = Arc::new(FailingPreVisibilityRecoveryStore);
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-metadata-status-fails");
        let request = || CommitRequest {
            message: "metadata status fails".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "durable commit pre-visibility recovery status unavailable"
        );
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-store-detail"));
        assert!(!rendered.contains("metadata status fails"));
        assert!(!rendered.contains("durable-metadata-status-fails"));
        assert!(stores.post_cas_recovery.list(10).await.unwrap().is_empty());

        let replay = vcs_commit(State(state), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(replay).await["error"],
            http_idempotency::IDEMPOTENCY_IN_PROGRESS_MESSAGE
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
    }

    #[tokio::test]
    async fn guarded_durable_commit_ref_visibility_recovery_failure_records_pre_visibility_status()
    {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch ref-unconfirmed.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write ref-unconfirmed.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        let readable_refs = stores.refs.clone();
        stores.refs = Arc::new(AckLostUnreadableRefStore {
            inner: readable_refs.clone(),
            fired: AtomicBool::new(false),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-ref-unconfirmed");
        let request = || CommitRequest {
            message: "ref unconfirmed".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "durable commit visibility recovery is required"
        );
        assert!(
            !serde_json::to_string(&body)
                .unwrap()
                .contains("private-store-detail")
        );

        let visible_ref = readable_refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("underlying ref became visible despite ack loss");
        assert!(
            stores
                .commits
                .contains(&RepoId::local(), visible_ref.target)
                .await
                .unwrap()
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
        assert!(stores.post_cas_recovery.list(10).await.unwrap().is_empty());

        let pre_visibility = stores.pre_visibility_recovery.list(10).await.unwrap();
        assert_eq!(pre_visibility.len(), 1);
        assert_eq!(
            pre_visibility[0].target().stage(),
            DurableCorePreVisibilityRecoveryStage::RefVisibilityCas
        );
        assert_eq!(
            pre_visibility[0].state(),
            DurableCorePreVisibilityRecoveryState::Pending
        );
        assert_eq!(pre_visibility[0].target().commit_id(), visible_ref.target);
        assert!(pre_visibility[0].has_idempotency_reservation());

        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"), None)
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["pre_visibility_count"], 1);
        assert_eq!(status_body["pre_visibility_page_count"], 1);
        assert_eq!(status_body["pre_visibility_counts"]["pending"], 1);
        assert_eq!(
            status_body["pre_visibility"][0]["stage"],
            "ref_visibility_cas"
        );
        assert_eq!(
            status_body["pre_visibility"][0]["commit_id"],
            visible_ref.target.to_hex()
        );
        let status_rendered = serde_json::to_string(&status_body).unwrap();
        assert!(!status_rendered.contains("ref unconfirmed"));
        assert!(!status_rendered.contains("durable-ref-unconfirmed"));
        assert!(!status_rendered.contains("private-store-detail"));

        let replay = vcs_commit(State(state), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(replay).await["error"],
            http_idempotency::IDEMPOTENCY_IN_PROGRESS_MESSAGE
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
    }

    #[tokio::test]
    async fn recovery_run_resolves_visible_pre_visibility_row_and_enqueues_post_cas() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch ref-run.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write ref-run.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.refs = Arc::new(AckLostTemporarilyUnreadableRefStore {
            inner: stores.refs.clone(),
            fired: AtomicBool::new(false),
            get_failures_remaining: AtomicUsize::new(1),
        });
        let state = guarded_durable_commit_state(db.clone(), stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-ref-run-control");
        let request = || CommitRequest {
            message: "ref run control".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            json_body(response).await["error"],
            "durable commit visibility recovery is required"
        );
        assert_eq!(
            stores
                .pre_visibility_recovery
                .counts()
                .await
                .unwrap()
                .pending(),
            1
        );

        db.execute_command("write ref-run.txt child", &mut root)
            .await
            .unwrap();
        let child_response = vcs_commit(
            State(state.clone()),
            user_headers_with_idempotency("root", "durable-ref-run-control-child"),
            Json(request()),
        )
        .await
        .into_response();
        assert_eq!(child_response.status(), StatusCode::OK);

        let run_response = vcs_recovery_run(
            State(state.clone()),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":1}"#),
        )
        .await
        .into_response();
        assert_eq!(run_response.status(), StatusCode::OK);
        let run_body = json_body(run_response).await;
        assert_eq!(run_body["pre_visibility"]["attempted"], 1);
        assert_eq!(run_body["pre_visibility"]["resolved"], 1);
        assert_eq!(
            stores
                .pre_visibility_recovery
                .counts()
                .await
                .unwrap()
                .resolved(),
            1
        );

        let post_cas = stores.post_cas_recovery.list(10).await.unwrap();
        let post_cas = post_cas
            .iter()
            .filter(|status| status.state() != DurableCorePostCasRecoveryState::Completed)
            .collect::<Vec<_>>();
        assert_eq!(post_cas.len(), 1);
        assert_eq!(
            post_cas[0].target().step(),
            DurableCorePostCasStep::AuditAppend
        );

        let rendered = serde_json::to_string(&run_body).unwrap();
        assert!(!rendered.contains("ref run control"));
        assert!(!rendered.contains("durable-ref-run-control"));
        assert!(!rendered.contains("private-store-detail"));
    }

    #[tokio::test]
    async fn guarded_durable_commit_confirmed_ref_visibility_failure_aborts_idempotency() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch ref-unknown.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write ref-unknown.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.refs = Arc::new(FailingRefVisibilityStore {
            inner: stores.refs.clone(),
            fired: AtomicBool::new(false),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-ref-unknown");
        let request = || CommitRequest {
            message: "ref unknown".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "stratum: corrupt store: durable commit ref visibility update failed"
        );
        assert!(
            !serde_json::to_string(&body)
                .unwrap()
                .contains("private-store-detail")
        );
        assert!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            1
        );
        assert!(
            stores
                .pre_visibility_recovery
                .list(10)
                .await
                .unwrap()
                .is_empty()
        );

        let session = session_from_headers(&state, &headers).await.unwrap();
        let key = crate::idempotency::IdempotencyKey::parse_header_value(
            headers.get("idempotency-key").unwrap(),
        )
        .unwrap();
        let scope = vcs_idempotency_scope(VCS_COMMIT_IDEMPOTENCY_ROUTE);
        let fingerprint = request_fingerprint(
            &scope,
            &serde_json::json!({
                "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": Option::<Uuid>::None,
                "message": "ref unknown",
            }),
        )
        .unwrap();
        assert!(matches!(
            stores
                .idempotency
                .begin(&scope, &key, &fingerprint)
                .await
                .unwrap(),
            IdempotencyBegin::Execute(_)
        ));
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
    }

    #[tokio::test]
    async fn guarded_durable_commit_workspace_failure_returns_partial_and_leaves_ref_visible() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch partial.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write partial.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        let workspace_id = Uuid::new_v4();
        stores.workspace_metadata = Arc::new(ExistingFailingHeadStore { workspace_id });
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace_id);
        headers.insert(
            "idempotency-key",
            "durable-partial-workspace".parse().unwrap(),
        );

        let response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: "partial durable".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            json_body(response).await,
            DurableCoreCommittedResponse::partial_body()
        );
        let visible = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("visible durable ref");
        assert!(
            stores
                .commits
                .contains(&RepoId::local(), visible.target)
                .await
                .unwrap()
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
        let recovery = stores.post_cas_recovery.list(10).await.unwrap();
        assert_eq!(recovery.len(), 3);
        assert!(recovery.iter().all(|status| {
            status.target().commit_id() == visible.target
                && status.state() != DurableCorePostCasRecoveryState::Poisoned
        }));
        let mut recovery_steps = recovery
            .iter()
            .map(|status| status.target().step())
            .collect::<Vec<_>>();
        recovery_steps.sort_unstable();
        assert_eq!(
            recovery_steps,
            vec![
                DurableCorePostCasStep::WorkspaceHeadUpdate,
                DurableCorePostCasStep::AuditAppend,
                DurableCorePostCasStep::IdempotencyCompletion,
            ]
        );
        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"), None)
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["count"], 3);
        assert_eq!(status_body["page_count"], 3);
        assert_eq!(status_body["limit"], 100);
        assert_eq!(status_body["counts"]["pending"], 0);
        assert_eq!(status_body["counts"]["active"], 0);
        assert_eq!(status_body["counts"]["backing_off"], 2);
        assert_eq!(status_body["counts"]["completed"], 1);
        assert_eq!(status_body["counts"]["poisoned"], 0);
        assert_eq!(status_body["pre_visibility_available"], true);
        assert_eq!(
            status_body["recovery"][0]["commit_id"],
            visible.target.to_hex()
        );
        let status_rendered = serde_json::to_string(&status_body).unwrap();
        assert!(!status_rendered.contains("partial durable"));
        assert!(!status_rendered.contains("durable-partial-workspace"));

        let replay = vcs_commit(
            State(state),
            headers,
            Json(CommitRequest {
                message: "partial durable".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        assert_eq!(
            replay
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
    }

    #[tokio::test]
    async fn guarded_durable_commit_records_recovery_intent_before_workspace_side_effect() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch pre-side-effect.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write pre-side-effect.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        let recovery = Arc::new(InMemoryDurableCorePostCasRecoveryClaimStore::new());
        let workspace_id = Uuid::new_v4();
        let observed_recovery = Arc::new(AtomicBool::new(false));
        stores.post_cas_recovery = recovery.clone();
        stores.workspace_metadata = Arc::new(RecoveryObservingFailingHeadStore {
            workspace_id,
            recovery,
            observed_recovery: observed_recovery.clone(),
        });
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace_id);
        headers.insert(
            "idempotency-key",
            "pre-side-effect-recovery".parse().unwrap(),
        );

        let response = vcs_commit(
            State(state),
            headers,
            Json(CommitRequest {
                message: "pre side effect recovery".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(
            observed_recovery.load(Ordering::SeqCst),
            "workspace side effect ran before its recovery intent was visible"
        );
    }

    #[tokio::test]
    async fn vcs_recovery_status_preserves_post_cas_when_pre_visibility_store_fails() {
        let mut stores = StratumStores::local_memory();
        let commit_id = CommitId::from(ObjectId::from_bytes(b"status-post-cas-visible"));
        let target = DurableCorePostCasRecoveryTarget::new(
            RepoId::local(),
            MAIN_REF,
            commit_id,
            DurableCorePostCasStep::WorkspaceHeadUpdate,
        )
        .unwrap();
        stores.post_cas_recovery.enqueue(target, 100).await.unwrap();
        stores.pre_visibility_recovery = Arc::new(FailingPreVisibilityRecoveryStore);
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let status_response = vcs_recovery_status(State(state), user_headers("root"), None)
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["count"], 1);
        assert_eq!(status_body["page_count"], 1);
        assert_eq!(status_body["recovery"][0]["commit_id"], commit_id.to_hex());
        assert_eq!(status_body["pre_visibility_available"], false);
        assert_eq!(status_body["pre_visibility_count"], 0);
        assert_eq!(status_body["pre_visibility_page_count"], 0);
        assert_eq!(
            status_body["pre_visibility_error"],
            "pre-visibility recovery status unavailable"
        );
        let rendered = serde_json::to_string(&status_body).unwrap();
        assert!(!rendered.contains("private-store-detail"));
    }

    #[tokio::test]
    async fn vcs_recovery_status_and_run_include_fs_mutation_recovery() {
        let stores = StratumStores::local_memory();
        let target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "fs:operator-status",
            "fs-operator-status",
            "agent/operator/session",
            synthetic_commit_id("fs-operator-before"),
            synthetic_commit_id("fs-operator-after"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .unwrap();
        stores
            .fs_mutation_recovery
            .enqueue(
                target.clone(),
                DurableFsMutationRecoveryEnvelope::new(
                    None,
                    Some(
                        DurableFsMutationAuditRecoveryContext::new(
                            AuditAction::FsWriteFile,
                            &["/operator/status.txt"],
                        )
                        .unwrap(),
                    ),
                    None,
                ),
                100,
            )
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"), None)
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["fs_mutation_available"], true);
        assert_eq!(status_body["fs_mutation_count"], 1);
        assert_eq!(status_body["fs_mutation_page_count"], 1);
        assert_eq!(
            status_body["fs_mutations"][0]["failed_step"],
            "audit_append"
        );
        assert_eq!(status_body["fs_mutations"][0]["state"], "pending");
        assert_eq!(
            status_body["fs_mutations"][0]["target_ref"],
            target.target_ref()
        );

        let run_response = vcs_recovery_run(State(state), user_headers("root"), Bytes::new())
            .await
            .into_response();
        assert_eq!(run_response.status(), StatusCode::OK);
        let run_body = json_body(run_response).await;
        assert_eq!(run_body["fs_mutations"]["completed"], 1);
        assert_eq!(
            stores
                .fs_mutation_recovery
                .counts()
                .await
                .unwrap()
                .completed(),
            1
        );
        assert_eq!(stores.audit.list_recent(10).await.unwrap().len(), 1);

        let statuses = stores.fs_mutation_recovery.list(10).await.unwrap();
        assert_eq!(
            statuses[0].state(),
            DurableFsMutationRecoveryState::Completed
        );
    }

    #[tokio::test]
    async fn vcs_recovery_run_includes_redacted_correlation_and_remaining_summaries() {
        let stores = StratumStores::local_memory();
        for index in 0..2 {
            let target = DurableFsMutationRecoveryTarget::new(
                RepoId::local(),
                format!("fs:run-summary-{index}"),
                format!("fs-run-summary-{index}"),
                "agent/run-summary/session",
                synthetic_commit_id(&format!("run-summary-before-{index}")),
                synthetic_commit_id(&format!("run-summary-after-{index}")),
                DurableFsMutationRecoveryStep::AuditAppend,
            )
            .unwrap();
            stores
                .fs_mutation_recovery
                .enqueue(
                    target,
                    DurableFsMutationRecoveryEnvelope::new(
                        None,
                        Some(
                            DurableFsMutationAuditRecoveryContext::new(
                                AuditAction::FsWriteFile,
                                &[format!("/run-summary-{index}.txt").as_str()],
                            )
                            .unwrap(),
                        ),
                        None,
                    ),
                    100,
                )
                .await
                .unwrap();
        }
        let cleanup_id = ObjectId::from_bytes(b"run-summary-cleanup");
        stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: RepoId::local(),
                claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
                object_kind: ObjectKind::Blob,
                object_id: cleanup_id,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &RepoId::local(),
                    ObjectKind::Blob,
                    &cleanup_id,
                ),
                lease_owner: "test-recovery-run-summary".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .unwrap()
            .expect("object cleanup claim");
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let run_response = vcs_recovery_run(
            State(state),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":1,"lease_owner":"attacker-supplied"}"#),
        )
        .await
        .into_response();

        assert_eq!(run_response.status(), StatusCode::OK);
        let correlation_header = run_response
            .headers()
            .get("X-Stratum-Recovery-Correlation-Id")
            .and_then(|value| value.to_str().ok())
            .expect("correlation header")
            .to_string();
        let run_body = json_body(run_response).await;
        let correlation_id = run_body["correlation_id"].as_str().expect("correlation_id");
        assert_eq!(correlation_id, correlation_header);
        assert!(correlation_id.starts_with("rec_"));
        assert!(correlation_id.is_ascii());
        assert!(correlation_id.len() <= 40);
        assert_eq!(run_body["requested_limit"], 1);
        assert_eq!(run_body["attempted"], 1);
        assert_eq!(run_body["completed"], 1);
        assert_eq!(run_body["backing_off"], 0);
        assert_eq!(run_body["poisoned"], 0);
        assert_eq!(run_body["skipped"], 0);
        assert_eq!(run_body["phases"]["pre_visibility"]["remaining"], 0);
        assert_eq!(run_body["phases"]["post_cas"]["remaining"], 0);
        assert_eq!(run_body["phases"]["fs_mutations"]["attempted"], 1);
        assert_eq!(run_body["phases"]["fs_mutations"]["completed"], 1);
        assert_eq!(run_body["phases"]["fs_mutations"]["remaining"], 1);
        assert_eq!(run_body["phases"]["object_cleanup"]["attempted"], 0);
        assert_eq!(run_body["phases"]["object_cleanup"]["completed"], 0);
        assert_eq!(run_body["phases"]["object_cleanup"]["remaining"], 1);
        assert_eq!(run_body["remaining"], 2);
        assert_eq!(run_body["converged"], false);
        assert_eq!(
            run_body["message"],
            "bounded recovery run completed with persisted work remaining"
        );
        let rendered = serde_json::to_string(&run_body).unwrap();
        assert!(!rendered.contains("attacker-supplied"));
        assert!(!rendered.contains(VCS_RECOVERY_RUN_LEASE_OWNER));
    }

    #[tokio::test]
    async fn vcs_recovery_run_processes_bounded_object_cleanup_when_fenced() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let lost_object = put_durable_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"operator cleanup ready".to_vec(),
        )
        .await;
        let cleanup_claim = stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: repo_id.clone(),
                claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                object_kind: ObjectKind::Blob,
                object_id: lost_object,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &repo_id,
                    ObjectKind::Blob,
                    &lost_object,
                ),
                lease_owner: "operator-cleanup-test".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .unwrap()
            .expect("object cleanup claim");
        stores
            .object_cleanup
            .record_failure(&cleanup_claim, "raw failure should not be surfaced")
            .await
            .unwrap();
        stores.object_cleanup.release(&cleanup_claim).await.unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let run_response = vcs_recovery_run(
            State(state),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":1}"#),
        )
        .await
        .into_response();

        assert_eq!(run_response.status(), StatusCode::OK);
        let run_body = json_body(run_response).await;
        let object_cleanup = &run_body["phases"]["object_cleanup"];
        assert_eq!(object_cleanup["limit"], 1);
        assert_eq!(object_cleanup["scanned"], 1);
        assert_eq!(object_cleanup["listed"], 1);
        assert_eq!(object_cleanup["attempted"], 1);
        assert_eq!(object_cleanup["processed"], 1);
        assert_eq!(object_cleanup["completed"], 0);
        assert_eq!(object_cleanup["deleted_final_objects"], 0);
        assert_eq!(object_cleanup["deletion_ready"], 1);
        assert_eq!(object_cleanup["backing_off"], 0);
        assert_eq!(object_cleanup["retryable_failures"], 0);
        assert_eq!(object_cleanup["poisoned"], 0);
        assert_eq!(object_cleanup["skipped"], 0);
        assert_eq!(object_cleanup["deferred"], 0);
        assert_eq!(object_cleanup["remaining"], 1);
        assert_eq!(run_body["attempted"], 1);
        assert_eq!(run_body["completed"], 0);
        assert_eq!(run_body["remaining"], 1);
        assert_eq!(
            stores
                .objects
                .get(&repo_id, lost_object, ObjectKind::Blob)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"operator cleanup ready"
        );
        assert_eq!(stores.object_cleanup.counts().await.unwrap().completed(), 0);
    }

    #[tokio::test]
    async fn vcs_recovery_run_keeps_deletion_dry_run_when_blockers_fail() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let lost_object = put_durable_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"operator cleanup blocked".to_vec(),
        )
        .await;
        let cleanup_claim = stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: repo_id.clone(),
                claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                object_kind: ObjectKind::Blob,
                object_id: lost_object,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &repo_id,
                    ObjectKind::Blob,
                    &lost_object,
                ),
                lease_owner: "operator-cleanup-blocked-test".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .unwrap()
            .expect("object cleanup claim");
        stores
            .object_cleanup
            .record_failure(&cleanup_claim, "raw blocked failure should not be surfaced")
            .await
            .unwrap();
        stores.object_cleanup.release(&cleanup_claim).await.unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new(MAIN_REF).unwrap(),
                target: CommitId::from(ObjectId::from_bytes(b"missing cleanup blocker")),
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let run_response = vcs_recovery_run(
            State(state),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":1}"#),
        )
        .await
        .into_response();

        assert_eq!(run_response.status(), StatusCode::OK);
        let run_body = json_body(run_response).await;
        let object_cleanup = &run_body["phases"]["object_cleanup"];
        assert_eq!(object_cleanup["attempted"], 1);
        assert_eq!(object_cleanup["completed"], 0);
        assert_eq!(object_cleanup["deleted_final_objects"], 0);
        assert_eq!(object_cleanup["deletion_ready"], 0);
        assert_eq!(object_cleanup["backing_off"], 1);
        assert_eq!(object_cleanup["retryable_failures"], 1);
        assert_eq!(object_cleanup["skipped"], 1);
        assert_eq!(object_cleanup["deferred"], 1);
        assert_eq!(
            stores
                .objects
                .get(&repo_id, lost_object, ObjectKind::Blob)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"operator cleanup blocked"
        );
        let rendered = serde_json::to_string(&run_body).unwrap();
        assert!(!rendered.contains("raw blocked failure"));
        assert!(!rendered.contains(&lost_object.to_hex()));
    }

    #[tokio::test]
    async fn vcs_recovery_run_uses_repo_scoped_remaining() {
        let stores = StratumStores::local_memory();
        let other_repo = RepoId::new("repo_other_recovery_run").unwrap();
        let other_commit = synthetic_commit_id("other-repo-post-cas");
        stores
            .post_cas_recovery
            .enqueue(
                DurableCorePostCasRecoveryTarget::new(
                    other_repo.clone(),
                    MAIN_REF,
                    other_commit,
                    DurableCorePostCasStep::AuditAppend,
                )
                .unwrap(),
                100,
            )
            .await
            .unwrap();
        stores
            .pre_visibility_recovery
            .record(DurableCorePreVisibilityRecoveryRecord::new(
                DurableCorePreVisibilityRecoveryTarget::new(
                    other_repo.clone(),
                    MAIN_REF,
                    synthetic_commit_id("other-repo-pre-visibility"),
                    DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
                )
                .unwrap(),
                ObjectId::from_bytes(b"other repo pre visibility tree"),
                None,
                RefVersion::new(1).unwrap(),
                1,
                1,
                false,
                100,
            ))
            .await
            .unwrap();
        stores
            .fs_mutation_recovery
            .enqueue(
                DurableFsMutationRecoveryTarget::new(
                    other_repo.clone(),
                    "fs:other-repo",
                    "other-repo-op",
                    "agent/other/session",
                    synthetic_commit_id("other-repo-fs-before"),
                    synthetic_commit_id("other-repo-fs-after"),
                    DurableFsMutationRecoveryStep::AuditAppend,
                )
                .unwrap(),
                DurableFsMutationRecoveryEnvelope::new(None, None, None),
                100,
            )
            .await
            .unwrap();
        let lost_object = put_durable_object(
            &stores,
            &other_repo,
            ObjectKind::Blob,
            b"other repo cleanup should not leak".to_vec(),
        )
        .await;
        stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: other_repo.clone(),
                claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                object_kind: ObjectKind::Blob,
                object_id: lost_object,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &other_repo,
                    ObjectKind::Blob,
                    &lost_object,
                ),
                lease_owner: "other-repo-cleanup-test".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .unwrap()
            .expect("other repo cleanup claim");
        assert_eq!(
            stores
                .pre_visibility_recovery
                .counts()
                .await
                .unwrap()
                .total(),
            1
        );
        assert_eq!(stores.post_cas_recovery.counts().await.unwrap().total(), 1);
        assert_eq!(
            stores.fs_mutation_recovery.counts().await.unwrap().total(),
            1
        );
        assert_eq!(stores.object_cleanup.counts().await.unwrap().total(), 1);
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let run_response = vcs_recovery_run(
            State(state),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":0}"#),
        )
        .await
        .into_response();

        assert_eq!(run_response.status(), StatusCode::OK);
        let run_body = json_body(run_response).await;
        assert_eq!(run_body["attempted"], 0);
        assert_eq!(run_body["remaining"], 0);
        assert_eq!(run_body["converged"], true);
        assert_eq!(run_body["phases"]["pre_visibility"]["remaining"], 0);
        assert_eq!(run_body["phases"]["post_cas"]["remaining"], 0);
        assert_eq!(run_body["phases"]["fs_mutations"]["remaining"], 0);
        assert_eq!(run_body["phases"]["object_cleanup"]["scanned"], 0);
        assert_eq!(run_body["phases"]["object_cleanup"]["remaining"], 0);
    }

    #[tokio::test]
    async fn vcs_recovery_run_keeps_invalid_json_and_limit_behavior() {
        let state =
            guarded_durable_commit_state(StratumDb::open_memory(), StratumStores::local_memory());

        let invalid_response = vcs_recovery_run(
            State(state.clone()),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":"not-a-number"}"#),
        )
        .await
        .into_response();

        assert_eq!(invalid_response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            json_body(invalid_response).await["error"],
            "stratum: invalid recovery run request"
        );

        let capped_response = vcs_recovery_run(
            State(state),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":999}"#),
        )
        .await
        .into_response();

        assert_eq!(capped_response.status(), StatusCode::OK);
        let capped_body = json_body(capped_response).await;
        assert_eq!(capped_body["requested_limit"], VCS_RECOVERY_RUN_MAX_LIMIT);
        assert_eq!(capped_body["limit"], VCS_RECOVERY_RUN_MAX_LIMIT);
    }

    #[tokio::test]
    async fn vcs_recovery_status_rows_include_age_due_and_stale_classification() {
        let stores = StratumStores::local_memory();
        let commit_id = CommitId::from(ObjectId::from_bytes(b"status-classified-post-cas"));
        let post_cas_target = DurableCorePostCasRecoveryTarget::new(
            RepoId::local(),
            MAIN_REF,
            commit_id,
            DurableCorePostCasStep::WorkspaceHeadUpdate,
        )
        .unwrap();
        stores
            .post_cas_recovery
            .enqueue(post_cas_target.clone(), 100)
            .await
            .unwrap();
        let claim = stores
            .post_cas_recovery
            .claim(
                DurableCorePostCasRecoveryClaimRequest::new(
                    post_cas_target,
                    "status-test",
                    Duration::from_millis(10),
                    101,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .expect("post-CAS recovery claim");
        stores
            .post_cas_recovery
            .record_failure(
                &claim,
                "private failure body",
                Duration::from_millis(1),
                102,
            )
            .await
            .unwrap();

        let pre_visibility_target = DurableCorePreVisibilityRecoveryTarget::new(
            RepoId::local(),
            MAIN_REF,
            CommitId::from(ObjectId::from_bytes(b"status-classified-pre")),
            DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
        )
        .unwrap();
        stores
            .pre_visibility_recovery
            .record(DurableCorePreVisibilityRecoveryRecord::new(
                pre_visibility_target,
                ObjectId::from_bytes(b"status-classified-tree"),
                None,
                RefVersion::new(1).unwrap(),
                1,
                1,
                false,
                120,
            ))
            .await
            .unwrap();

        let fs_target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "fs:status-classified",
            "status-classified-op",
            "agent/status/classified",
            synthetic_commit_id("status-classified-before"),
            synthetic_commit_id("status-classified-after"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .unwrap();
        stores
            .fs_mutation_recovery
            .enqueue(
                fs_target,
                DurableFsMutationRecoveryEnvelope::new(None, None, None),
                130,
            )
            .await
            .unwrap();

        let cleanup_id = ObjectId::from_bytes(b"status-classified-cleanup");
        let cleanup_claim = stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: RepoId::local(),
                claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
                object_kind: ObjectKind::Blob,
                object_id: cleanup_id,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &RepoId::local(),
                    ObjectKind::Blob,
                    &cleanup_id,
                ),
                lease_owner: "status-test".to_string(),
                lease_duration: Duration::from_millis(50),
            })
            .await
            .unwrap()
            .expect("object cleanup claim");
        stores
            .object_cleanup
            .record_failure(&cleanup_claim, "raw cleanup storage error")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;

        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let status_response = vcs_recovery_status(State(state), user_headers("root"), None)
            .await
            .into_response();

        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        for row in [
            &status_body["recovery"][0],
            &status_body["pre_visibility"][0],
            &status_body["fs_mutations"][0],
            &status_body["phases"]["object_cleanup"]["rows"][0],
        ] {
            assert!(row["age_millis"].is_u64(), "{row:?}");
            assert!(row["stale_active"].is_boolean(), "{row:?}");
            assert!(row["due"].is_boolean(), "{row:?}");
            assert!(row["retryable"].is_boolean(), "{row:?}");
            assert!(row["stuck_tier"].is_string(), "{row:?}");
            assert!(row.get("next_retry_at_millis").is_some(), "{row:?}");
        }
        let cleanup_row = &status_body["phases"]["object_cleanup"]["rows"][0];
        assert_eq!(cleanup_row["state"], "failed");
        assert_eq!(cleanup_row["is_stale"], true);
        assert_eq!(cleanup_row["due"], true);
        assert_eq!(cleanup_row["retryable"], true);
    }

    #[tokio::test]
    async fn vcs_recovery_status_includes_object_gc_dry_run_summary() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let blob_id = put_durable_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"gc status blob".to_vec(),
        )
        .await;
        let root_tree_id = put_durable_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "gc-status.txt",
                    TreeEntryKind::Blob,
                    blob_id,
                    0o100644,
                )],
            }
            .serialize(),
        )
        .await;
        let commit_id = synthetic_commit_id("gc-status-unreachable");
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1,
                message: "private gc status message".to_string(),
                author: "private gc status author".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        let cleanup_claim = stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: repo_id.clone(),
                claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                object_kind: ObjectKind::Tree,
                object_id: root_tree_id,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &repo_id,
                    ObjectKind::Tree,
                    &root_tree_id,
                ),
                lease_owner: "gc-status-test".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .unwrap()
            .expect("object cleanup claim");
        stores
            .object_cleanup
            .record_failure(&cleanup_claim, "private object-store failure")
            .await
            .unwrap();

        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let status_response = vcs_recovery_status(State(state), user_headers("root"), None)
            .await
            .into_response();

        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        let dry_run = &status_body["phases"]["object_cleanup"]["gc_dry_run"];
        assert_eq!(dry_run["available"], true);
        assert_eq!(dry_run["mode"], "dry_run");
        assert_eq!(dry_run["repo_id"], RepoId::local().as_str());
        assert_eq!(dry_run["deletion_enabled"], false);
        assert_eq!(dry_run["deletion_ready"], 0);
        assert_eq!(
            dry_run["deletion_ready_reason"],
            "requires_fenced_cleanup_worker"
        );
        assert_eq!(dry_run["blocked"], false);
        assert_eq!(dry_run["unreachable_commit_count"], 1);
        assert_eq!(dry_run["unreachable_object_count"], 1);
        assert_eq!(dry_run["unreachable_cleanup_candidate_count"], 1);
        assert_eq!(status_body["phases"]["object_cleanup"]["deletion_ready"], 0);
        assert!(
            dry_run["unreachable_commits"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate["commit_id"] == commit_id.short_hex())
        );
        assert!(
            dry_run["unreachable_objects"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate["object_kind"] == "tree"
                    && candidate["object_id"] == root_tree_id.short_hex())
        );

        let rendered = serde_json::to_string(dry_run).unwrap();
        assert!(!rendered.contains(&commit_id.to_hex()));
        assert!(!rendered.contains(&root_tree_id.to_hex()));
        assert!(!rendered.contains("private gc status message"));
        assert!(!rendered.contains("private object-store failure"));
    }

    #[tokio::test]
    async fn vcs_recovery_status_includes_operator_health_phases_and_blockers() {
        let stores = StratumStores::local_memory();
        let poisoned_commit_id = CommitId::from(ObjectId::from_bytes(b"status-poisoned-post-cas"));
        let post_cas_target = DurableCorePostCasRecoveryTarget::new(
            RepoId::local(),
            MAIN_REF,
            poisoned_commit_id,
            DurableCorePostCasStep::AuditAppend,
        )
        .unwrap();
        stores
            .post_cas_recovery
            .enqueue(post_cas_target.clone(), 100)
            .await
            .unwrap();
        let post_cas_claim = stores
            .post_cas_recovery
            .claim(
                DurableCorePostCasRecoveryClaimRequest::new(
                    post_cas_target,
                    "status-test",
                    Duration::from_secs(1),
                    101,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .expect("post-CAS recovery claim");
        stores
            .post_cas_recovery
            .poison(&post_cas_claim, "private poison detail", 102)
            .await
            .unwrap();

        let fs_target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "fs:workspace-blocker",
            "workspace-blocker-op",
            "agent/status/workspace-blocker",
            synthetic_commit_id("workspace-blocker-before"),
            synthetic_commit_id("workspace-blocker-after"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .unwrap();
        stores
            .fs_mutation_recovery
            .enqueue(
                fs_target,
                DurableFsMutationRecoveryEnvelope::new(None, None, None),
                103,
            )
            .await
            .unwrap();

        for index in 0..105 {
            let cleanup_id = ObjectId::from_bytes(format!("bounded-cleanup-{index}").as_bytes());
            let _ = stores
                .object_cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: RepoId::local(),
                    claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    object_kind: ObjectKind::Blob,
                    object_id: cleanup_id,
                    object_key: crate::backend::object_cleanup::canonical_final_object_key(
                        &RepoId::local(),
                        ObjectKind::Blob,
                        &cleanup_id,
                    ),
                    lease_owner: "status-test".to_string(),
                    lease_duration: Duration::from_secs(30),
                })
                .await
                .unwrap();
        }

        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let status_response = vcs_recovery_status(State(state), user_headers("root"), None)
            .await
            .into_response();

        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["health"]["backend_mode"], "durable");
        assert_eq!(status_body["health"]["guarded_durable_enabled"], true);
        assert_eq!(status_body["health"]["status"], "degraded");
        assert_eq!(
            status_body["health"]["stores"]["object_cleanup"]["available"],
            true
        );
        assert_eq!(status_body["health"]["scheduler"]["present"], false);
        assert!(status_body["recovery"].is_array());
        assert!(status_body["counts"].is_object());
        for phase in [
            "pre_visibility",
            "post_cas",
            "fs_mutations",
            "object_cleanup",
        ] {
            assert_eq!(status_body["phases"][phase]["available"], true, "{phase}");
            assert!(
                status_body["phases"][phase]["counts"].is_object(),
                "{phase}"
            );
            assert!(status_body["phases"][phase]["count"].is_u64(), "{phase}");
            assert!(
                status_body["phases"][phase]["page_count"].is_u64(),
                "{phase}"
            );
            assert!(
                status_body["phases"][phase]["oldest_age_millis"].is_u64()
                    || status_body["phases"][phase]["oldest_age_millis"].is_null(),
                "{phase}"
            );
            assert!(
                status_body["phases"][phase]["due_count"].is_u64(),
                "{phase}"
            );
            assert!(
                status_body["phases"][phase]["stale_active_count"].is_u64(),
                "{phase}"
            );
            let terminal_count_key = if phase == "object_cleanup" {
                "failed_count"
            } else {
                "poisoned_count"
            };
            assert!(
                status_body["phases"][phase][terminal_count_key].is_u64(),
                "{phase}"
            );
            assert!(status_body["phases"][phase]["rows"].is_array(), "{phase}");
        }
        assert_eq!(
            status_body["phases"]["object_cleanup"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            100
        );
        assert_eq!(status_body["phases"]["object_cleanup"]["page_count"], 100);
        let cleanup_row = &status_body["phases"]["object_cleanup"]["rows"][0];
        assert!(cleanup_row.get("object_id").is_some());
        assert!(cleanup_row.get("object_key").is_none());
        assert!(
            status_body["blockers"]["refs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|blocker| blocker["repo_id"] == RepoId::local().as_str()
                    && blocker["ref_name"] == MAIN_REF
                    && blocker["blocked"] == true
                    && blocker["phases"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|phase| phase == "post_cas"))
        );
        assert!(
            status_body["blockers"]["workspaces"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |blocker| blocker["workspace_scope"] == "fs:workspace-blocker"
                        && blocker["target_ref"] == "agent/status/workspace-blocker"
                )
        );

        let rendered = serde_json::to_string(&status_body).unwrap();
        assert!(!rendered.contains("private poison detail"));
        assert!(!rendered.contains("raw cleanup storage error"));
    }

    #[tokio::test]
    async fn vcs_recovery_status_reports_attached_scheduler_status() {
        let stores = StratumStores::local_memory();
        let scheduler = crate::server::start_durable_recovery_scheduler(stores.clone())
            .expect("scheduler should start");

        for _ in 0..40 {
            if scheduler.status().last_tick_at_millis.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let status_response = vcs_recovery_status(
            State(state),
            user_headers("root"),
            Some(Extension(scheduler)),
        )
        .await
        .into_response();

        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = json_body(status_response).await;
        assert_eq!(status_body["health"]["scheduler"]["present"], true);
        assert!(status_body["health"]["scheduler"]["started_at_millis"].is_u64());
        assert!(status_body["health"]["scheduler"]["last_tick_at_millis"].is_u64());
        assert_eq!(
            status_body["health"]["scheduler"]["last_outcome"],
            "completed"
        );
        assert_eq!(
            status_body["health"]["scheduler"]["last_error"],
            JsonValue::Null
        );
        assert!(status_body["health"]["scheduler"]["phases"].is_object());
    }

    #[tokio::test]
    async fn guarded_durable_commit_post_cas_enqueue_failure_does_not_return_normal_partial() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch recovery-enqueue.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write recovery-enqueue.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.post_cas_recovery = Arc::new(FailingPostCasRecoveryStore::default());
        let workspace_id = Uuid::new_v4();
        stores.workspace_metadata = Arc::new(ExistingFailingHeadStore { workspace_id });
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace_id);
        headers.insert(
            "idempotency-key",
            "durable-partial-recovery-enqueue".parse().unwrap(),
        );
        let request = || CommitRequest {
            message: "partial recovery enqueue".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "durable commit visibility recovery is required"
        );
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-store-detail"));
        assert!(!rendered.contains("partial recovery enqueue"));
        assert!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
                .await
                .unwrap()
                .is_some()
        );

        let replay = vcs_commit(State(state), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(replay).await["error"],
            http_idempotency::IDEMPOTENCY_IN_PROGRESS_MESSAGE
        );
    }

    #[tokio::test]
    async fn vcs_recovery_run_non_admin_is_denied() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let state = guarded_durable_commit_state(db, StratumStores::local_memory());

        let response = vcs_recovery_run(
            State(state),
            user_headers("bob"),
            Bytes::from_static(br#"{"limit":1}"#),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn vcs_recovery_run_disabled_guarded_durable_capability_returns_501() {
        let state = test_state(StratumDb::open_memory());

        let response = vcs_recovery_run(State(state), user_headers("root"), Bytes::new())
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            json_body(response).await["error"],
            "guarded durable commit recovery is not enabled"
        );
    }

    #[tokio::test]
    async fn guarded_durable_commit_workspace_partial_recovery_run_repairs_contextual_rows() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch run-repair.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write run-repair.txt content", &mut root)
            .await
            .unwrap();
        let workspace_store = Arc::new(FailingOnceWorkspaceHeadStore::default());
        let workspace = workspace_store
            .create_workspace("run repair", "/run-repair")
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.workspace_metadata = workspace_store;
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace.id);
        headers.insert("idempotency-key", "run-repair-idempotency".parse().unwrap());

        let commit_response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: "secret run repair message".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(commit_response.status(), StatusCode::ACCEPTED);
        let visible = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("visible durable ref");
        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"), None)
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_rendered = serde_json::to_string(&json_body(status_response).await).unwrap();
        for secret in [
            "secret run repair message",
            "root",
            "run-repair-idempotency",
            VCS_RECOVERY_RUN_LEASE_OWNER,
            "metadata write failed",
        ] {
            assert!(
                !status_rendered.contains(secret),
                "status response leaked {secret}"
            );
        }

        let first_run = vcs_recovery_run(
            State(state.clone()),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":1,"lease_owner":"attacker-supplied"}"#),
        )
        .await
        .into_response();
        assert_eq!(first_run.status(), StatusCode::OK);
        let first_body = json_body(first_run).await;
        assert_eq!(first_body["limit"], 1);
        assert_eq!(first_body["attempted"], 1);
        assert_eq!(first_body["completed"], 1);

        let second_run = vcs_recovery_run(
            State(state.clone()),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":10}"#),
        )
        .await
        .into_response();
        assert_eq!(second_run.status(), StatusCode::OK);
        let second_body = json_body(second_run).await;
        assert_eq!(second_body["completed"], 1);

        assert_eq!(
            stores
                .workspace_metadata
                .get_workspace(workspace.id)
                .await
                .unwrap()
                .unwrap()
                .head_commit
                .as_deref(),
            Some(visible.target.to_hex().as_str())
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 1);
        let session = session_from_headers(&state, &headers).await.unwrap();
        let key =
            IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap()).unwrap();
        let scope = vcs_idempotency_scope(VCS_COMMIT_IDEMPOTENCY_ROUTE);
        let fingerprint = request_fingerprint(
            &scope,
            &serde_json::json!({
                "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": Some(workspace.id),
                "message": "secret run repair message",
            }),
        )
        .unwrap();
        let replay = stores
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap();
        match replay {
            IdempotencyBegin::Replay(record) => {
                assert_eq!(record.status_code, 202);
                assert_eq!(
                    record.response_body,
                    DurableCoreCommittedResponse::partial_body()
                );
            }
            other => panic!("expected partial replay, got {other:?}"),
        }

        let rendered = serde_json::to_string(&second_body).unwrap();
        for secret in [
            "secret run repair message",
            "root",
            "run-repair-idempotency",
            "attacker-supplied",
            VCS_RECOVERY_RUN_LEASE_OWNER,
            "metadata write failed",
        ] {
            assert!(!rendered.contains(secret), "run response leaked {secret}");
        }
    }

    #[tokio::test]
    async fn guarded_durable_commit_partial_replay_recovery_run_completes_partial_idempotency() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch run-idempotency.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write run-idempotency.txt content", &mut root)
            .await
            .unwrap();
        let workspace_store = Arc::new(FailingOnceWorkspaceHeadStore::default());
        let workspace = workspace_store
            .create_workspace("run idempotency", "/run-idempotency")
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.workspace_metadata = workspace_store;
        stores.idempotency = Arc::new(FailingOnceCompleteIdempotencyStore::default());
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace.id);
        headers.insert(
            "idempotency-key",
            "run-partial-idempotency-key".parse().unwrap(),
        );

        let commit_response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: "secret partial idempotency message".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(commit_response.status(), StatusCode::ACCEPTED);
        let recovery = stores.post_cas_recovery.list(10).await.unwrap();
        let mut steps = recovery
            .iter()
            .map(|status| status.target().step())
            .collect::<Vec<_>>();
        steps.sort_unstable();
        assert_eq!(
            steps,
            vec![
                DurableCorePostCasStep::WorkspaceHeadUpdate,
                DurableCorePostCasStep::AuditAppend,
                DurableCorePostCasStep::IdempotencyCompletion,
            ]
        );

        for expected_completed in [1, 1, 1] {
            let response = vcs_recovery_run(
                State(state.clone()),
                user_headers("root"),
                Bytes::from_static(br#"{"limit":1}"#),
            )
            .await
            .into_response();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(json_body(response).await["completed"], expected_completed);
        }
        let final_run = vcs_recovery_run(
            State(state.clone()),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":100}"#),
        )
        .await
        .into_response();
        assert_eq!(final_run.status(), StatusCode::OK);
        assert_eq!(json_body(final_run).await["attempted"], 0);

        let session = session_from_headers(&state, &headers).await.unwrap();
        let key =
            IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap()).unwrap();
        let scope = vcs_idempotency_scope(VCS_COMMIT_IDEMPOTENCY_ROUTE);
        let fingerprint = request_fingerprint(
            &scope,
            &serde_json::json!({
                "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": Some(workspace.id),
                "message": "secret partial idempotency message",
            }),
        )
        .unwrap();
        match stores
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => {
                assert_eq!(record.status_code, 202);
                assert_eq!(
                    record.response_body,
                    DurableCoreCommittedResponse::partial_body()
                );
            }
            other => panic!("expected partial replay, got {other:?}"),
        }
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 1);
    }

    #[tokio::test]
    async fn guarded_durable_commit_idempotency_failure_recovery_run_replays_partial_response() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch direct-idempotency.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write direct-idempotency.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.idempotency = Arc::new(FailingOnceCompleteIdempotencyStore::default());
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = user_headers("root");
        headers.insert(
            "idempotency-key",
            "run-direct-idempotency-key".parse().unwrap(),
        );

        let commit_response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: "secret direct idempotency message".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(commit_response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            json_body(commit_response).await,
            DurableCoreCommittedResponse::partial_body()
        );

        let recovery = stores.post_cas_recovery.list(10).await.unwrap();
        let mut steps = recovery
            .iter()
            .map(|status| status.target().step())
            .collect::<Vec<_>>();
        steps.sort_unstable();
        assert_eq!(
            steps,
            vec![
                DurableCorePostCasStep::AuditAppend,
                DurableCorePostCasStep::IdempotencyCompletion,
            ]
        );

        let recovery_run = vcs_recovery_run(
            State(state.clone()),
            user_headers("root"),
            Bytes::from_static(br#"{"limit":10}"#),
        )
        .await
        .into_response();
        assert_eq!(recovery_run.status(), StatusCode::OK);
        assert_eq!(json_body(recovery_run).await["completed"], 1);

        let session = session_from_headers(&state, &headers).await.unwrap();
        let key =
            IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap()).unwrap();
        let scope = vcs_idempotency_scope(VCS_COMMIT_IDEMPOTENCY_ROUTE);
        let fingerprint = request_fingerprint(
            &scope,
            &serde_json::json!({
                "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                "actor": actor_fingerprint(&session),
                "workspace_id": Option::<Uuid>::None,
                "message": "secret direct idempotency message",
            }),
        )
        .unwrap();
        match stores
            .idempotency
            .begin(&scope, &key, &fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => {
                assert_eq!(record.status_code, 202);
                assert_eq!(
                    record.response_body,
                    DurableCoreCommittedResponse::partial_body()
                );
            }
            other => panic!("expected partial replay, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn guarded_durable_commit_workspace_partial_enqueues_idempotency_when_partial_replay_fails()
     {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch recovery-idempotency.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write recovery-idempotency.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.idempotency = Arc::new(FailingCompleteIdempotencyStore::default());
        let workspace_id = Uuid::new_v4();
        stores.workspace_metadata = Arc::new(ExistingFailingHeadStore { workspace_id });
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace_id);
        headers.insert(
            "idempotency-key",
            "durable-partial-idempotency-recovery".parse().unwrap(),
        );

        let response = vcs_commit(
            State(state),
            headers,
            Json(CommitRequest {
                message: "partial idempotency recovery".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let recovery = stores.post_cas_recovery.list(10).await.unwrap();
        let mut steps = recovery
            .iter()
            .map(|status| status.target().step().as_str())
            .collect::<Vec<_>>();
        steps.sort_unstable();
        assert_eq!(
            steps,
            vec![
                "audit_append",
                "idempotency_completion",
                "workspace_head_update",
            ]
        );
        assert!(recovery.iter().all(|status| {
            status.state() != DurableCorePostCasRecoveryState::Poisoned && status.attempts() <= 1
        }));
        let rendered = format!("{recovery:?}");
        assert!(!rendered.contains("private-token"));
        assert!(!rendered.contains("durable-partial-idempotency-recovery"));
    }

    #[tokio::test]
    async fn guarded_durable_commit_partial_replay_failure_requires_idempotency_recovery_claim() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch recovery-idempotency-enqueue.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write recovery-idempotency-enqueue.txt content", &mut root)
            .await
            .unwrap();
        let mut stores = StratumStores::local_memory();
        stores.idempotency = Arc::new(FailingCompleteIdempotencyStore::default());
        stores.post_cas_recovery = Arc::new(FailingIdempotencyRecoveryStore::default());
        let workspace_id = Uuid::new_v4();
        stores.workspace_metadata = Arc::new(ExistingFailingHeadStore { workspace_id });
        let state = guarded_durable_commit_state(db, stores.clone());
        let mut headers = workspace_headers("root", workspace_id);
        headers.insert(
            "idempotency-key",
            "durable-partial-idempotency-enqueue".parse().unwrap(),
        );
        let request = || CommitRequest {
            message: "partial idempotency enqueue".to_string(),
        };

        let response = vcs_commit(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "durable commit visibility recovery is required"
        );
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-token"));
        assert!(!rendered.contains("partial idempotency enqueue"));
        let recovery = stores.post_cas_recovery.list(10).await.unwrap();
        let mut steps = recovery
            .iter()
            .map(|status| status.target().step())
            .collect::<Vec<_>>();
        steps.sort_unstable();
        assert_eq!(
            steps,
            vec![
                DurableCorePostCasStep::WorkspaceHeadUpdate,
                DurableCorePostCasStep::AuditAppend,
            ]
        );

        let replay = vcs_commit(State(state), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(replay).await["error"],
            http_idempotency::IDEMPOTENCY_IN_PROGRESS_MESSAGE
        );
    }

    #[tokio::test]
    async fn guarded_durable_commit_existing_parent_uses_durable_parent_tree_snapshot() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch parent.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write parent.txt first", &mut root)
            .await
            .unwrap();
        let stores = StratumStores::local_memory();
        let state = guarded_durable_commit_state(db.clone(), stores.clone());

        let first = vcs_commit(
            State(state.clone()),
            user_headers("root"),
            Json(CommitRequest {
                message: "first durable".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_hash = json_body(first).await["hash"].as_str().unwrap().to_string();

        db.execute_command("write parent.txt second", &mut root)
            .await
            .unwrap();
        let second = vcs_commit(
            State(state),
            user_headers("root"),
            Json(CommitRequest {
                message: "second durable".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(second.status(), StatusCode::OK);
        let second_hash = json_body(second).await["hash"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(second_hash, first_hash);
        let commits = stores.commits.list(&RepoId::local()).await.unwrap();
        assert_eq!(commits.len(), 2);
        let second_record = commits
            .iter()
            .find(|record| record.id.to_hex() == second_hash)
            .unwrap();
        assert_eq!(
            second_record.parents,
            vec![CommitId::from(ObjectId::from_hex(&first_hash).unwrap())]
        );
        assert!(
            second_record
                .changed_paths
                .iter()
                .any(|change| change.path == "/parent.txt"
                    && change.kind == crate::vcs::ChangeKind::Modified)
        );
        assert_eq!(db.vcs_log().await.len(), 0);
    }

    #[tokio::test]
    async fn guarded_durable_commit_log_and_refs_read_durable_metadata() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch durable-log.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write durable-log.txt first", &mut root)
            .await
            .unwrap();
        let stores = StratumStores::local_memory();
        let state = guarded_durable_commit_state(db.clone(), stores.clone());

        let commit_response = vcs_commit(
            State(state.clone()),
            user_headers("root"),
            Json(CommitRequest {
                message: "durable metadata log".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(commit_response.status(), StatusCode::OK);
        let commit_hash = json_body(commit_response).await["hash"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(db.vcs_log().await.len(), 0);

        let log_response = vcs_log(State(state.clone()), user_headers("root"))
            .await
            .into_response();
        assert_eq!(log_response.status(), StatusCode::OK);
        let log_body = json_body(log_response).await;
        let commits = log_body["commits"].as_array().expect("commits array");
        assert_eq!(commits.len(), 1);
        assert!(
            commit_hash.starts_with(commits[0]["hash"].as_str().expect("short hash")),
            "durable log hash should be a prefix of the returned commit hash"
        );
        assert_eq!(commits[0]["message"], "durable metadata log");
        assert_eq!(commits[0]["author"], "root");
        assert!(commits[0]["timestamp"].as_u64().unwrap() > 0);

        let refs_response = vcs_list_refs(State(state), user_headers("root"))
            .await
            .into_response();
        assert_eq!(refs_response.status(), StatusCode::OK);
        let refs_body = json_body(refs_response).await;
        let refs = refs_body["refs"].as_array().expect("refs array");
        let main = refs
            .iter()
            .find(|item| item["name"] == serde_json::json!(MAIN_REF))
            .expect("durable main ref");
        assert_eq!(main["target"], commit_hash);
        assert_eq!(main["version"], 1);
    }

    #[tokio::test]
    async fn guarded_durable_ref_create_and_update_routes_use_durable_stores() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch durable-ref-route.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write durable-ref-route.txt first", &mut root)
            .await
            .unwrap();
        let stores = StratumStores::local_memory();
        let state = guarded_durable_commit_state(db.clone(), stores.clone());

        let first = vcs_commit(
            State(state.clone()),
            user_headers("root"),
            Json(CommitRequest {
                message: "first durable ref target".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_hash = json_body(first).await["hash"].as_str().unwrap().to_string();

        db.execute_command("write durable-ref-route.txt second", &mut root)
            .await
            .unwrap();
        let second = vcs_commit(
            State(state.clone()),
            user_headers("root"),
            Json(CommitRequest {
                message: "second durable ref target".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(second.status(), StatusCode::OK);
        let second_hash = json_body(second).await["hash"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(db.vcs_log().await.len(), 0);

        let ref_name = "agent/root/session-1";
        let create_response = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: ref_name.to_string(),
                target: first_hash.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let created = json_body(create_response).await;
        assert_eq!(created["name"], ref_name);
        assert_eq!(created["target"], first_hash);
        assert_eq!(created["version"], 1);

        let update_response = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(ref_name.to_string()),
            Json(UpdateRefRequest {
                target: second_hash.clone(),
                expected_target: first_hash.clone(),
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(update_response.status(), StatusCode::OK);
        let updated = json_body(update_response).await;
        assert_eq!(updated["name"], ref_name);
        assert_eq!(updated["target"], second_hash);
        assert_eq!(updated["version"], 2);

        let durable_ref = stores
            .refs
            .get(&RepoId::local(), &RefName::new(ref_name).unwrap())
            .await
            .unwrap()
            .expect("durable session ref");
        assert_eq!(durable_ref.target.to_hex(), second_hash);
        assert_eq!(durable_ref.version.value(), 2);

        let local_refs = db.list_refs().await.unwrap();
        assert!(!local_refs.iter().any(|vcs_ref| vcs_ref.name == ref_name));
    }

    #[tokio::test]
    async fn guarded_durable_vcs_log_keeps_admin_gate() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let stores = StratumStores::local_memory();
        let workspace = create_local_repo_workspace(&stores, "demo", "/demo").await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let state = guarded_durable_commit_state(db, stores);

        let response = vcs_log(State(state.clone()), user_headers("bob"))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let workspace_bearer = vcs_log(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();

        assert_eq!(workspace_bearer.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn guarded_durable_revert_creates_restore_commit_and_advances_main_without_local_state() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["reverted_to"], fixture.target_commit.to_hex());
        assert_eq!(body["target_commit"], fixture.target_commit.to_hex());
        assert_eq!(body["target_ref"], MAIN_REF);
        assert_eq!(body["expected_head"], fixture.head_commit.to_hex());
        let revert_commit = CommitId::from(
            ObjectId::from_hex(
                body["revert_commit"]
                    .as_str()
                    .expect("durable revert commit"),
            )
            .unwrap(),
        );
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, revert_commit);
        assert_eq!(main.version.value(), 2);
        let record = stores
            .commits
            .get(&RepoId::local(), revert_commit)
            .await
            .unwrap()
            .expect("revert commit metadata");
        assert_eq!(record.root_tree, fixture.target_root);
        assert_eq!(record.parents, vec![fixture.head_commit]);
        assert_eq!(
            record
                .changed_paths
                .iter()
                .map(|change| change.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/demo/revert.txt"]
        );
    }

    #[tokio::test]
    async fn guarded_durable_revert_idempotency_replays_without_second_commit_or_audit() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-revert-replay");
        let request = || RevertRequest {
            hash: fixture.target_commit.to_hex(),
        };

        let first_response = vcs_revert(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = json_body(first_response).await;
        let main_after_first = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");

        let replay_response = vcs_revert(State(state), headers, Json(request()))
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
        assert_eq!(json_body(replay_response).await, first_body);
        let main_after_replay = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main_after_replay, main_after_first);
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            3
        );
        let events = stores.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsRevert, 1);
    }

    #[tokio::test]
    async fn guarded_durable_revert_protected_ref_blocks_before_mutation() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        stores
            .review
            .create_protected_ref_rule(MAIN_REF, 1, ROOT_UID)
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
        assert!(body["error"].as_str().unwrap().contains("protected ref"));
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn guarded_durable_revert_protected_path_uses_durable_changed_paths_before_mutation() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        stores
            .review
            .create_protected_path_rule("/demo/revert.txt", Some(MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("protected path"));
        assert!(error.contains("/demo/revert.txt"));
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn guarded_durable_revert_requires_policy_token_for_changed_paths() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());
        let capability = state
            .core
            .guarded_durable_commit_route()
            .expect("guarded durable capability");
        let revert_plan = capability
            .revert_plan(&fixture.target_commit.to_hex())
            .await
            .unwrap();
        let token = PolicyDecisionToken::allow_for_test_with_paths(
            PolicyAction::VcsRevert,
            MAIN_REF,
            ["/other.txt"],
        );

        let response = guarded_durable_vcs_revert(
            &state,
            capability,
            &Session::root(),
            revert_plan,
            None,
            None,
            token,
        )
        .await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn guarded_durable_revert_stale_ref_version_conflicts_without_advancing_main() {
        let mut stores = StratumStores::local_memory();
        let racing_refs = Arc::new(CasMismatchRefStore {
            inner: stores.refs.clone(),
            fail_updates: AtomicBool::new(false),
        });
        stores.refs = racing_refs.clone();
        let fixture = seed_durable_revert_history(&stores).await;
        racing_refs.fail_updates.store(true, Ordering::SeqCst);
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("ref compare-and-swap mismatch")
        );
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
        assert_eq!(main.version.value(), 1);
    }

    #[tokio::test]
    async fn guarded_durable_revert_unresolved_recovery_state_conflicts_with_redacted_error() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        stores
            .post_cas_recovery
            .enqueue(
                DurableCorePostCasRecoveryTarget::new(
                    RepoId::local(),
                    MAIN_REF,
                    fixture.head_commit,
                    DurableCorePostCasStep::AuditAppend,
                )
                .unwrap(),
                current_unix_timestamp_millis(),
            )
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("durable VCS recovery is pending"), "{error}");
        assert!(!error.contains("private-store-detail"), "{error}");
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
    }

    #[tokio::test]
    async fn guarded_durable_revert_poisoned_post_cas_recovery_conflicts() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        let now = current_unix_timestamp_millis();
        let target = DurableCorePostCasRecoveryTarget::new(
            RepoId::local(),
            MAIN_REF,
            fixture.head_commit,
            DurableCorePostCasStep::AuditAppend,
        )
        .unwrap();
        stores
            .post_cas_recovery
            .enqueue(target.clone(), now)
            .await
            .unwrap();
        let claim = stores
            .post_cas_recovery
            .claim(
                DurableCorePostCasRecoveryClaimRequest::new(
                    target,
                    VCS_RECOVERY_RUN_LEASE_OWNER,
                    Duration::from_secs(30),
                    now,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .expect("post-CAS recovery claim");
        stores
            .post_cas_recovery
            .poison(&claim, "private-store-detail /demo/secret.txt", now + 1)
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("durable VCS recovery is pending"), "{error}");
        assert!(!error.contains("private-store-detail"), "{error}");
        assert!(!error.contains("/demo/secret.txt"), "{error}");
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
    }

    #[tokio::test]
    async fn guarded_durable_revert_poisoned_pre_visibility_recovery_conflicts() {
        let stores = StratumStores::local_memory();
        let fixture = seed_durable_revert_history(&stores).await;
        let now = current_unix_timestamp_millis();
        let target = DurableCorePreVisibilityRecoveryTarget::new(
            RepoId::local(),
            MAIN_REF,
            fixture.head_commit,
            DurableCorePreVisibilityRecoveryStage::RefVisibilityCas,
        )
        .unwrap();
        stores
            .pre_visibility_recovery
            .record(DurableCorePreVisibilityRecoveryRecord::new(
                target.clone(),
                fixture.target_root,
                Some(fixture.target_commit),
                RefVersion::new(1).unwrap(),
                0,
                1,
                false,
                now,
            ))
            .await
            .unwrap();
        let claim = stores
            .pre_visibility_recovery
            .claim(
                DurableCorePreVisibilityRecoveryClaimRequest::new(
                    target,
                    VCS_RECOVERY_RUN_LEASE_OWNER,
                    Duration::from_secs(30),
                    now,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .expect("pre-visibility recovery claim");
        stores
            .pre_visibility_recovery
            .poison(&claim, "private-store-detail /demo/secret.txt", now + 1)
            .await
            .unwrap();
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: fixture.target_commit.to_hex(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("durable VCS recovery is pending"), "{error}");
        assert!(!error.contains("private-store-detail"), "{error}");
        assert!(!error.contains("/demo/secret.txt"), "{error}");
        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main.target, fixture.head_commit);
    }

    #[tokio::test]
    async fn guarded_durable_revert_invalid_hash_redacts_request_detail() {
        let stores = StratumStores::local_memory();
        seed_durable_revert_history(&stores).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);

        let response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: "private-token-abc123".to_string(),
            }),
        )
        .await
        .into_response();

        let status = response.status();
        let body = json_body(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
        let error = body["error"].as_str().unwrap();
        assert!(error.contains("invalid commit hash"), "{error}");
        assert!(!error.contains("private-token"), "{error}");
        assert!(!error.contains("abc123"), "{error}");
    }

    #[tokio::test]
    async fn guarded_durable_revert_audit_failure_after_visible_mutation_returns_partial_replay() {
        let mut stores = StratumStores::local_memory();
        stores.audit = Arc::new(FailingMutationAuditStore::default());
        let fixture = seed_durable_revert_history(&stores).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores.clone());
        let headers = user_headers_with_idempotency("root", "durable-revert-audit-partial");
        let request = || RevertRequest {
            hash: fixture.target_commit.to_hex(),
        };

        let first_response = vcs_revert(State(state.clone()), headers.clone(), Json(request()))
            .await
            .into_response();
        assert_eq!(first_response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            json_body(first_response).await,
            DurableCoreCommittedResponse::partial_body()
        );
        let main_after_first = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_ne!(main_after_first.target, fixture.head_commit);

        let replay_response = vcs_revert(State(state), headers, Json(request()))
            .await
            .into_response();
        assert_eq!(replay_response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            replay_response
                .headers()
                .get("x-stratum-idempotent-replay")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            json_body(replay_response).await,
            DurableCoreCommittedResponse::partial_body()
        );
        let main_after_replay = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(main_after_replay, main_after_first);
        assert_eq!(
            stores.commits.list(&RepoId::local()).await.unwrap().len(),
            3
        );
    }

    #[tokio::test]
    async fn scoped_workspace_bearer_cannot_run_global_vcs_mutations() {
        let state = test_state(StratumDb::open_memory());
        let workspace = state
            .workspaces
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = state
            .workspaces
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let headers = workspace_bearer_headers(&issued.raw_secret, workspace.id);

        let commit_response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: "scoped root bearer should not commit".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(commit_response.status(), StatusCode::FORBIDDEN);

        let revert_response = vcs_revert(
            State(state),
            headers,
            Json(RevertRequest {
                hash: "abcdef".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(revert_response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn scoped_workspace_bearer_cannot_run_guarded_durable_commit() {
        let db = StratumDb::open_memory();
        let stores = StratumStores::local_memory();
        let workspace = create_local_repo_workspace(&stores, "demo", "/demo").await;
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let state = guarded_durable_commit_state(db, stores);

        let response = vcs_commit(
            State(state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
            Json(CommitRequest {
                message: "scoped guarded durable commit".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = json_body(response).await;
        assert!(
            body["error"]
                .as_str()
                .expect("error string")
                .contains("permission denied")
        );
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsCommit);
        assert_eq!(events[1].resource.id.as_deref(), Some(hash));
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains(sensitive_message));
    }

    #[tokio::test]
    async fn commit_audit_failure_response_and_replay_are_redacted() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch audit-redacted.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write audit-redacted.txt content", &mut root)
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingMutationAuditStore::default()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let headers = user_headers_with_idempotency("root", "vcs-audit-redaction");
        let sensitive_message = "commit message must not leak";

        let response = vcs_commit(
            State(state.clone()),
            headers.clone(),
            Json(CommitRequest {
                message: sensitive_message.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(body["error"], "audit append failed after mutation");
        assert_eq!(body["mutation_committed"], true);
        assert_eq!(body["audit_recorded"], false);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-store-detail"));
        assert!(!rendered.contains(sensitive_message));

        let replay = vcs_commit(
            State(state.clone()),
            headers,
            Json(CommitRequest {
                message: sensitive_message.to_string(),
            }),
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
        let replay_body = json_body(replay).await;
        assert_eq!(replay_body, body);
        assert!(
            !serde_json::to_string(&replay_body)
                .unwrap()
                .contains("private-store-detail")
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 0);
    }

    #[tokio::test]
    async fn commit_idempotency_completion_failure_response_is_redacted() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch idempotency-redacted.txt", &mut root)
            .await
            .unwrap();
        db.execute_command("write idempotency-redacted.txt content", &mut root)
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(FailingCompleteIdempotencyStore::default()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let sensitive_message = "idempotency commit message must not leak";

        let response = vcs_commit(
            State(state.clone()),
            user_headers_with_idempotency("root", "vcs-idempotency-redaction"),
            Json(CommitRequest {
                message: sensitive_message.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(
            body["error"],
            "idempotency completion failed after mutation"
        );
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("private-token"));
        assert!(!rendered.contains(sensitive_message));
        assert_eq!(body["mutation_committed"], true);
        assert_eq!(body["idempotency_recorded"], false);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_audit_action_count(&events, AuditAction::PolicyDecisionAllow, 1);
        assert_audit_action_count(&events, AuditAction::VcsCommit, 1);
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains("private-token"));
        assert!(!audit_json.contains("vcs-idempotency-redaction"));
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsRefCreate);
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
            vcs_list_refs(State(state.clone()), user_headers("root"))
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

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 3);
        assert!(
            events
                .iter()
                .all(|event| event.action == crate::audit::AuditAction::PolicyDecisionDeny)
        );
        assert!(
            events
                .iter()
                .all(|event| event.resource.kind == crate::audit::AuditResourceKind::PolicyDecision)
        );
        assert_eq!(
            events[0].details.get("reason").map(String::as_str),
            Some("protected_ref")
        );
        assert_eq!(
            events[0].details.get("target_ref").map(String::as_str),
            Some(crate::vcs::MAIN_REF)
        );
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("blocked direct commit")
        );
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
        let key = IdempotencyKey::parse_header_value(headers.get("idempotency-key").unwrap())
            .expect("idempotency key");
        let session = session_from_headers(&state, &headers)
            .await
            .expect("session");
        let repo_context =
            resolve_vcs_repo_context(&state, &headers, &session).expect("repo context");
        let scope = vcs_idempotency_scope_for_repo(VCS_COMMIT_IDEMPOTENCY_ROUTE, &repo_context);
        let fingerprint = request_fingerprint(
            &scope,
            &with_explicit_repo_fingerprint(
                serde_json::json!({
                    "route": VCS_COMMIT_IDEMPOTENCY_ROUTE,
                    "actor": actor_fingerprint(&session),
                    "workspace_id": serde_json::Value::Null,
                    "message": "first commit",
                }),
                &repo_context,
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
            }
            other => panic!("expected commit replay record, got {other:?}"),
        }

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
        let expected_replay_body = vcs_commit_idempotency_body(&first_body);
        assert_eq!(replay_body, expected_replay_body);
        assert_eq!(replay_body["message"], JsonValue::Null);
        assert_eq!(db.vcs_log().await.len(), 1);

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsCommit);
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsRevert);
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
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].resource.id.as_deref(), Some(original.as_str()));
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
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionDeny
        );
        assert_eq!(
            events[0].details.get("reason").map(String::as_str),
            Some("protected_path")
        );
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsRevert);
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsRefCreate);
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
            db: ServerLocalDb::available(Arc::new(db)),
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

        async fn update_head_commit_if_current(
            &self,
            _id: Uuid,
            _expected_head_commit: Option<&str>,
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

        async fn validate_workspace_token_at(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
            _now_unix: u64,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    struct ExistingFailingHeadStore {
        workspace_id: Uuid,
    }

    struct RecoveryObservingFailingHeadStore {
        workspace_id: Uuid,
        recovery: Arc<dyn DurableCorePostCasRecoveryClaimStore>,
        observed_recovery: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for RecoveryObservingFailingHeadStore {
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
                    repo_id: None,
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

        async fn update_head_commit_if_current(
            &self,
            _id: Uuid,
            _expected_head_commit: Option<&str>,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            if let Ok(statuses) = self.recovery.list(10).await {
                self.observed_recovery.store(
                    statuses.iter().any(|status| {
                        status.target().step() == DurableCorePostCasStep::WorkspaceHeadUpdate
                    }),
                    Ordering::SeqCst,
                );
            }
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

        async fn validate_workspace_token_at(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
            _now_unix: u64,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
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
                    repo_id: None,
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

        async fn update_head_commit_if_current(
            &self,
            _id: Uuid,
            _expected_head_commit: Option<&str>,
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

        async fn validate_workspace_token_at(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
            _now_unix: u64,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct FailingOnceWorkspaceHeadStore {
        inner: InMemoryWorkspaceMetadataStore,
        fired: AtomicBool,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for FailingOnceWorkspaceHeadStore {
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
            if !self.fired.swap(true, Ordering::SeqCst) {
                return Err(VfsError::IoError(std::io::Error::other(
                    "metadata write failed with private backend detail",
                )));
            }
            self.inner
                .update_head_commit_if_current(id, expected_head_commit, head_commit)
                .await
        }

        async fn issue_workspace_token(
            &self,
            workspace_id: Uuid,
            name: &str,
            agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            self.inner
                .issue_workspace_token(workspace_id, name, agent_uid)
                .await
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
                    repo_id: None,
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
                repo_id: None,
            }))
        }

        async fn update_head_commit_if_current(
            &self,
            id: Uuid,
            expected_head_commit: Option<&str>,
            head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id != self.workspace_id {
                return Ok(None);
            }
            let mut guard = self.updated.write().await;
            if guard.as_deref() != expected_head_commit {
                return Ok(None);
            }
            *guard = head_commit.clone();
            Ok(Some(WorkspaceRecord {
                id,
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                head_commit,
                version: 1,
                base_ref: crate::vcs::MAIN_REF.to_string(),
                session_ref: None,
                repo_id: None,
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

        async fn validate_workspace_token_at(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
            _now_unix: u64,
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
            db: ServerLocalDb::available(Arc::new(db)),
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
            db: ServerLocalDb::available(Arc::new(db)),
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsCommit);
        assert_eq!(events[1].outcome, crate::audit::AuditOutcome::Partial);
        let expected_workspace_id = workspace_id.to_string();
        assert_eq!(
            events[1].details.get("workspace_id").map(String::as_str),
            Some(expected_workspace_id.as_str())
        );
        assert_eq!(
            events[1].details.get("failed_step").map(String::as_str),
            Some("workspace_head_update")
        );
        assert_eq!(
            events[1].details.get("status").map(String::as_str),
            Some(StatusCode::INTERNAL_SERVER_ERROR.as_str())
        );
        assert!(
            events[1]
                .details
                .get("error")
                .is_some_and(|error| error == "workspace head update failed")
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
            db: ServerLocalDb::available(Arc::new(db)),
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
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::PolicyDecisionAllow
        );
        assert_eq!(events[1].action, crate::audit::AuditAction::VcsRevert);
        assert_eq!(
            events[1].resource.id.as_deref(),
            Some(original_full.as_str())
        );
        assert_eq!(events[1].outcome, crate::audit::AuditOutcome::Partial);
        let expected_workspace_id = workspace_id.to_string();
        assert_eq!(
            events[1].details.get("workspace_id").map(String::as_str),
            Some(expected_workspace_id.as_str())
        );
        assert_eq!(
            events[1].details.get("failed_step").map(String::as_str),
            Some("workspace_head_update")
        );
        assert_eq!(
            events[1].details.get("status").map(String::as_str),
            Some(StatusCode::INTERNAL_SERVER_ERROR.as_str())
        );
        assert!(
            events[1]
                .details
                .get("error")
                .is_some_and(|error| error == "workspace head update failed")
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
                db: ServerLocalDb::available(Arc::new(db.clone())),
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
            db: ServerLocalDb::available(Arc::new(db)),
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
