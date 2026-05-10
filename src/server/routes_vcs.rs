use axum::body::Bytes;
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
use super::policy::{
    self, RoutePolicyAction, RoutePolicyCorrelation, RoutePolicyEvaluation, RoutePolicyRequest,
};
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
    DurableCorePostCasRecoveryContext, DurableCorePostCasRepairWorker,
    DurableCorePostCasRepairWorkerStores, DurableCorePostCasStep,
    DurableCorePreVisibilityRecoveryRecord, DurableCorePreVisibilityRecoveryRun,
    DurableCorePreVisibilityRecoveryRunStores, DurableFsMutationRecoveryWorker,
};
use crate::backend::durable_mutation::DURABLE_MUTATION_COMMIT_MESSAGE;
use crate::backend::{CommitRecord, StratumStores};
use crate::error::VfsError;
use crate::idempotency::{IdempotencyBegin, IdempotencyReservation, request_fingerprint};
use crate::server::core::GuardedDurableCommitRoute;
use crate::vcs::{CommitId, MAIN_REF, RefName};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const VCS_COMMIT_IDEMPOTENCY_ROUTE: &str = "POST /vcs/commit";
const VCS_REVERT_IDEMPOTENCY_ROUTE: &str = "POST /vcs/revert";
const VCS_CREATE_REF_IDEMPOTENCY_ROUTE: &str = "POST /vcs/refs";
const VCS_UPDATE_REF_IDEMPOTENCY_ROUTE: &str = "PATCH /vcs/refs/{name}";
const VCS_RECOVERY_RUN_DEFAULT_LIMIT: usize = 10;
const VCS_RECOVERY_RUN_MAX_LIMIT: usize = 100;
const VCS_RECOVERY_RUN_LEASE_OWNER: &str = "guarded-durable-commit-recovery";
const VCS_FS_MUTATION_RECOVERY_RUN_LEASE_OWNER: &str = "durable-fs-mutation-recovery";

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
    let request = RoutePolicyRequest::from_session(action, session)
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

    let preflight = RoutePolicyRequest::from_session(RoutePolicyAction::VcsRevert, session)
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

    let request = RoutePolicyRequest::from_session(RoutePolicyAction::VcsRevert, session)
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
                .with_detail("error", "workspace head update failed"),
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

fn is_ref_cas_mismatch_error(error: &VfsError) -> bool {
    matches!(
        error,
        VfsError::InvalidArgs { message }
            if message.starts_with("ref compare-and-swap mismatch")
    )
}

async fn guarded_durable_vcs_commit(
    state: &AppState,
    capability: GuardedDurableCommitRoute,
    session: &Session,
    message: &str,
    workspace_id: Option<Uuid>,
    reservation: Option<IdempotencyReservation>,
) -> axum::response::Response {
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

    guarded_durable_commit_complete_post_cas(GuardedDurablePostCasRouteInput {
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

async fn guarded_durable_commit_write_plan(
    state: &AppState,
    capability: &GuardedDurableCommitRoute,
    source: DurableCoreCommitSourceSnapshot,
    workspace_id: Option<Uuid>,
) -> Result<DurableCoreCommitObjectTreeWritePlan, VfsError> {
    if let Some(workspace_id) = workspace_id {
        let workspace = state
            .workspaces
            .get_workspace(workspace_id)
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

struct GuardedDurablePostCasRouteInput<'a> {
    plan: &'a DurableCoreCommitObjectTreeWritePlan,
    metadata: &'a DurableCoreCommitMetadataInsert,
    visibility: &'a DurableCoreCommitRefCasVisibility,
    post_cas_stores: &'a StratumStores,
    session: &'a Session,
    message: &'a str,
    workspace_id: Option<Uuid>,
    reservation: Option<IdempotencyReservation>,
}

struct GuardedDurablePostCasRouteRecoveryClaims {
    workspace: Option<DurableCorePostCasRecoveryClaim>,
    audit: DurableCorePostCasRecoveryClaim,
    idempotency: Option<DurableCorePostCasRecoveryClaim>,
}

async fn guarded_durable_commit_complete_post_cas(
    input: GuardedDurablePostCasRouteInput<'_>,
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
    let response_status =
        StatusCode::from_u16(committed_response.status_code()).unwrap_or(StatusCode::OK);
    let body = committed_response.response_body().clone();
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

    let mut post_cas_input = DurableCoreCommitPostCasInput::new(audit_event, committed_response);
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

async fn vcs_recovery_status(
    State(state): State<AppState>,
    headers: HeaderMap,
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
            serde_json::json!({
                "repo_id": status.target().repo_id().as_str(),
                "ref_name": status.target().ref_name(),
                "commit_id": status.target().commit_id().to_hex(),
                "step": status.target().step().as_str(),
                "state": status.state().as_str(),
                "attempts": status.attempts(),
                "lease_expires_at_millis": status.lease_expires_at_millis(),
                "retry_after_millis": status.retry_after_millis(),
                "terminal_at_millis": status.terminal_at_millis(),
                "diagnosis": status.redacted_diagnosis(),
            })
        })
        .collect::<Vec<_>>();

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
                    serde_json::json!({
                        "repo_id": status.target().repo_id().as_str(),
                        "ref_name": status.target().ref_name(),
                        "commit_id": status.target().commit_id().to_hex(),
                        "stage": status.target().stage().as_str(),
                        "state": status.state().as_str(),
                        "root_tree_id": status.root_tree_id().to_hex(),
                        "parent_commit_id": status
                            .parent_commit_id()
                            .map(|commit_id| commit_id.to_hex()),
                        "expected_ref_version": status.expected_ref_version().value(),
                        "object_count": status.object_count(),
                        "changed_path_count": status.changed_path_count(),
                        "has_idempotency_reservation": status.has_idempotency_reservation(),
                        "first_seen_at_millis": status.first_seen_at_millis(),
                        "last_seen_at_millis": status.last_seen_at_millis(),
                        "occurrence_count": status.occurrence_count(),
                        "attempts": status.attempts(),
                        "lease_expires_at_millis": status.lease_expires_at_millis(),
                        "retry_after_millis": status.retry_after_millis(),
                        "terminal_at_millis": status.terminal_at_millis(),
                        "diagnosis": status.redacted_diagnosis(),
                        "has_recovery_context": status.has_post_cas_context(),
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "available": true,
                "rows": pre_visibility_rows,
                "counts": pre_visibility_counts,
                "count": pre_visibility_aggregate_counts.total(),
            })
        }
        (Err(_), _) | (_, Err(_)) => serde_json::json!({
            "available": false,
            "rows": [],
            "counts": {
                "pending": 0,
                "active": 0,
                "backing_off": 0,
                "resolved": 0,
                "poisoned": 0,
            },
            "count": 0,
            "error": "pre-visibility recovery status unavailable",
        }),
    };
    let pre_visibility_rows = pre_visibility["rows"].clone();
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
                    serde_json::json!({
                        "repo_id": status.target().repo_id().as_str(),
                        "workspace_scope": status.target().workspace_scope(),
                        "operation_id": status.target().operation_id(),
                        "target_ref": status.target().target_ref(),
                        "previous_commit": status.target().previous_commit().to_hex(),
                        "new_commit": status.target().new_commit().to_hex(),
                        "failed_step": status.target().failed_step().as_str(),
                        "state": status.state().as_str(),
                        "attempts": status.attempts(),
                        "lease_expires_at_millis": status.lease_expires_at_millis(),
                        "retry_after_millis": status.retry_after_millis(),
                        "terminal_at_millis": status.terminal_at_millis(),
                        "diagnosis": status.redacted_diagnosis(),
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "available": true,
                "rows": fs_mutation_rows,
                "counts": fs_mutation_counts,
                "count": fs_mutation_aggregate_counts.total(),
            })
        }
        (Err(_), _) | (_, Err(_)) => serde_json::json!({
            "available": false,
            "rows": [],
            "counts": {
                "pending": 0,
                "active": 0,
                "backing_off": 0,
                "completed": 0,
                "poisoned": 0,
            },
            "count": 0,
            "error": "durable FS mutation recovery status unavailable",
        }),
    };
    let fs_mutation_rows = fs_mutations["rows"].clone();
    Json(serde_json::json!({
        "recovery": rows,
        "counts": counts,
        "count": aggregate_counts.total(),
        "page_count": rows.len(),
        "pre_visibility": pre_visibility_rows,
        "pre_visibility_counts": pre_visibility["counts"].clone(),
        "pre_visibility_count": pre_visibility["count"].clone(),
        "pre_visibility_page_count": pre_visibility["rows"].as_array().map_or(0, Vec::len),
        "pre_visibility_available": pre_visibility["available"].clone(),
        "pre_visibility_error": pre_visibility.get("error").cloned(),
        "fs_mutations": fs_mutation_rows,
        "fs_mutation_counts": fs_mutations["counts"].clone(),
        "fs_mutation_count": fs_mutations["count"].clone(),
        "fs_mutation_page_count": fs_mutations["rows"].as_array().map_or(0, Vec::len),
        "fs_mutation_available": fs_mutations["available"].clone(),
        "fs_mutation_error": fs_mutations.get("error").cloned(),
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
        Ok(fs_mutation_summary) => Json(serde_json::json!({
            "limit": post_cas_summary.limit(),
            "scanned": post_cas_summary.scanned(),
            "attempted": post_cas_summary.attempted(),
            "completed": post_cas_summary.completed(),
            "backing_off": post_cas_summary.backing_off(),
            "poisoned": post_cas_summary.poisoned(),
            "skipped": post_cas_summary.skipped(),
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
            "fs_mutations": {
                "limit": fs_mutation_summary.limit(),
                "scanned": fs_mutation_summary.scanned(),
                "attempted": fs_mutation_summary.attempted(),
                "completed": fs_mutation_summary.completed(),
                "backing_off": fs_mutation_summary.backing_off(),
                "poisoned": fs_mutation_summary.poisoned(),
                "skipped": fs_mutation_summary.skipped(),
            },
        }))
        .into_response(),
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
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let refs_result = match state.core.guarded_durable_commit_route() {
        Some(capability) => capability.list_refs().await,
        None => state.core.list_refs().await,
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
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    let create_result = match state.core.guarded_durable_commit_route() {
        Some(capability) => capability.create_ref(&req.name, &req.target).await,
        None => state.core.create_ref(&req.name, &req.target).await,
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
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    let update_result = match state.core.guarded_durable_commit_route() {
        Some(capability) => {
            capability
                .update_ref(
                    &name,
                    &req.expected_target,
                    req.expected_version,
                    &req.target,
                )
                .await
        }
        None => {
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
    if let Err(response) = append_policy_audit(&state, &session, &policy_evaluation).await {
        abort_vcs_idempotency(&state, reservation.as_ref()).await;
        return response;
    }

    if let Some(capability) = state.core.guarded_durable_commit_route() {
        return guarded_durable_vcs_commit(
            &state,
            capability,
            &session,
            &req.message,
            workspace_id,
            reservation,
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
    let commits_result = match state.core.guarded_durable_commit_route() {
        Some(capability) => match require_admin(&state, &headers).await {
            Ok(session) => capability.vcs_log_as(&session).await,
            Err(e) => {
                return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string())
                    .into_response();
            }
        },
        None => match session_from_headers(&state, &headers).await {
            Ok(session) => state.core.vcs_log_as(&session).await,
            Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
        },
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
    if let Some(capability) = state.core.guarded_durable_commit_route() {
        let e = capability.mutable_workspace_not_supported();
        return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response();
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
        DurableFsMutationAuditRecoveryContext, DurableFsMutationRecoveryEnvelope,
        DurableFsMutationRecoveryState, DurableFsMutationRecoveryStep,
        DurableFsMutationRecoveryTarget, InMemoryDurableCorePostCasRecoveryClaimStore,
    };
    use crate::backend::durable_mutation::{
        DurableMutationEngine, DurableMutationInput, DurableMutationOperation,
    };
    use crate::backend::{
        CommitRecord, CommitStore, LocalMemoryObjectStore, ObjectStore, ObjectWrite,
        RefExpectation, RefRecord, RefStore, RefUpdate, RepoId, StoredObject, StratumStores,
    };
    use crate::db::StratumDb;
    use crate::fs::MetadataUpdate;
    use crate::idempotency::{IdempotencyKey, IdempotencyStore, InMemoryIdempotencyStore};
    use crate::server::ServerState;
    use crate::server::core::LocalCoreRuntime;
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, MAIN_REF, RefName};
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, ValidWorkspaceToken,
        WorkspaceMetadataStore, WorkspaceRecord,
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
            db: Arc::new(db),
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

    fn guarded_durable_commit_state(db: StratumDb, stores: StratumStores) -> AppState {
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared_with_guarded_durable_commit_route(
                db.clone(),
                RepoId::local(),
                stores.clone(),
            ),
            db: Arc::new(db),
            workspaces: stores.workspace_metadata.clone(),
            idempotency: stores.idempotency.clone(),
            audit: stores.audit.clone(),
            review: stores.review.clone(),
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
        DurableMutationEngine::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        )
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
        DurableMutationEngine::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        )
        .apply(DurableMutationInput {
            base_ref: RefName::new(MAIN_REF).unwrap(),
            session_ref: RefName::new(session_ref).unwrap(),
            operation: DurableMutationOperation::WriteFile {
                path: path.to_string(),
                content: content.to_vec(),
                mode: 0o644,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            },
            author: "agent".to_string(),
            timestamp,
            preflight_session: None,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn guarded_durable_status_renders_mounted_session_changes_without_local_vcs_state() {
        let mut stores = StratumStores::local_memory();
        stores.objects = Arc::new(StatusNoBlobGetObjectStore::default());
        let base_commit = seed_durable_status_base(&stores).await;
        let session_ref = "agent/durable-vcs/status-001";
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable status", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
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
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs(
                "durable scoped status",
                "/demo",
                MAIN_REF,
                Some(session_ref),
            )
            .await
            .unwrap();
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
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs(
                "durable nested scoped status",
                "/demo",
                MAIN_REF,
                Some(session_ref),
            )
            .await
            .unwrap();
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
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable diff", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
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
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable diff exact", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
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
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable diff prefix", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
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
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("durable scoped diff", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
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
        assert_eq!(json_body(replay_response).await, first_body);
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
        let engine = DurableMutationEngine::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        engine
            .apply(DurableMutationInput {
                base_ref: RefName::new(MAIN_REF).unwrap(),
                session_ref: RefName::new(session_ref).unwrap(),
                operation: DurableMutationOperation::WriteFile {
                    path: "/demo/session.txt".to_string(),
                    content: b"stale session content".to_vec(),
                    mode: 0o644,
                    uid: ROOT_UID,
                    gid: crate::auth::ROOT_GID,
                    mime_type: None,
                    custom_attrs: BTreeMap::new(),
                },
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

        async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
            self.inner.counts().await
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

        async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
            self.inner.counts().await
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

        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"))
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

        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"))
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
        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"))
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

        let status_response = vcs_recovery_status(State(state), user_headers("root"))
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

        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"))
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
        let status_response = vcs_recovery_status(State(state.clone()), user_headers("root"))
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
        let workspace = stores
            .workspace_metadata
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
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
    async fn guarded_durable_revert_route_fails_closed_without_request_leaks() {
        let state =
            guarded_durable_commit_state(StratumDb::open_memory(), StratumStores::local_memory());
        let expected_detail = "durable mutable workspace route execution is not supported yet";

        let revert_response = vcs_revert(
            State(state),
            user_headers("root"),
            Json(RevertRequest {
                hash: "abc123private".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(revert_response.status(), StatusCode::BAD_REQUEST);
        let revert_body = json_body(revert_response).await;
        let error = revert_body["error"].as_str().expect("error string");
        assert!(error.contains(expected_detail));
        assert!(!error.contains("abc123private"));
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
        let workspace = stores
            .workspace_metadata
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
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
            db: Arc::new(db),
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
            db: Arc::new(db),
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

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
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

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
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

        async fn validate_workspace_token(
            &self,
            workspace_id: Uuid,
            raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            self.inner
                .validate_workspace_token(workspace_id, raw_secret)
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
