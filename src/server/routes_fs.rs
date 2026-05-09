use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::AppState;
use super::core::GuardedDurableCommitRoute;
use super::idempotency as http_idempotency;
use super::middleware::session_from_headers;
use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::backend::RepoId;
use crate::backend::core_transaction::{
    DurableFsMutationAuditRecoveryContext, DurableFsMutationIdempotencyRecoveryContext,
    DurableFsMutationRecoveryEnvelope, DurableFsMutationRecoveryStep,
    DurableFsMutationRecoveryTarget,
};
use crate::backend::durable_mutation::DurableMutationOutput;
use crate::error::VfsError;
use crate::fs::{MetadataUpdate, validate_mime_type};
use crate::idempotency::{
    IdempotencyBegin, IdempotencyKey, IdempotencyReservation, request_fingerprint,
};
use crate::vcs::{CommitId, RefName};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize, Default)]
pub struct FsQuery {
    pub stat: Option<bool>,
    pub op: Option<String>,
    pub dst: Option<String>,
    pub recursive: Option<bool>,
}

#[derive(Deserialize, Default)]
pub struct SearchQuery {
    pub pattern: Option<String>,
    pub path: Option<String>,
    pub name: Option<String>,
    pub recursive: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct MetadataPatchRequest {
    #[serde(default, deserialize_with = "deserialize_mime_type_patch")]
    mime_type: Option<Option<String>>,
    #[serde(default)]
    custom_attrs: BTreeMap<String, String>,
    #[serde(default)]
    remove_custom_attrs: Vec<String>,
}

fn deserialize_mime_type_patch<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Ok(Some(None)),
        serde_json::Value::String(value) => Ok(Some(Some(value))),
        _ => Err(serde::de::Error::custom(
            "mime_type must be a string or null",
        )),
    }
}

impl From<MetadataPatchRequest> for MetadataUpdate {
    fn from(request: MetadataPatchRequest) -> Self {
        Self {
            mime_type: request.mime_type,
            custom_attrs: request.custom_attrs,
            remove_custom_attrs: request.remove_custom_attrs,
        }
    }
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
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
        VfsError::IsDirectory { path } => {
            format!(
                "stratum: is a directory: '{}'",
                session.project_mounted_error_path(path)
            )
        }
        VfsError::NotDirectory { path } => format!(
            "stratum: not a directory: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::AlreadyExists { path } => {
            format!(
                "stratum: already exists: '{}'",
                session.project_mounted_error_path(path)
            )
        }
        VfsError::NotEmpty { path } => format!(
            "stratum: directory not empty: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::InvalidPath { path } => format!(
            "stratum: invalid path: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::SymlinkLoop { path } => {
            format!(
                "stratum: symlink loop: '{}'",
                session.project_mounted_error_path(path)
            )
        }
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

fn api_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

async fn append_audit(
    state: &AppState,
    event: NewAuditEvent,
) -> Result<(), (StatusCode, serde_json::Value)> {
    state
        .audit
        .append(event)
        .await
        .map(|_| ())
        .map_err(audit_append_failed_value)
}

fn audit_append_failed_value(e: VfsError) -> (StatusCode, serde_json::Value) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({
            "error": format!("audit append failed after mutation: {e}"),
            "mutation_committed": true,
            "audit_recorded": false,
        }),
    )
}

fn resolve_api_path(session: &Session, path: &str) -> Result<String, VfsError> {
    session.resolve_mounted_path(&api_path(path))
}

fn resolve_root_path(session: &Session) -> Result<String, VfsError> {
    session.resolve_mounted_path("/")
}

fn resolve_optional_query_path(session: &Session, path: Option<&str>) -> Result<String, VfsError> {
    match path {
        Some(path) => resolve_api_path(session, path),
        None => resolve_root_path(session),
    }
}

#[derive(Serialize)]
struct FsActorFingerprint<'a> {
    uid: u32,
    gid: u32,
    username: &'a str,
    effective_uid: u32,
    effective_gid: u32,
    delegate: Option<FsDelegateFingerprint<'a>>,
}

#[derive(Serialize)]
struct FsDelegateFingerprint<'a> {
    uid: u32,
    gid: u32,
    username: &'a str,
}

fn actor_fingerprint(session: &Session) -> FsActorFingerprint<'_> {
    FsActorFingerprint {
        uid: session.uid,
        gid: session.gid,
        username: &session.username,
        effective_uid: session.effective_uid(),
        effective_gid: session.effective_gid(),
        delegate: session
            .delegate
            .as_ref()
            .map(|delegate| FsDelegateFingerprint {
                uid: delegate.uid,
                gid: delegate.gid,
                username: &delegate.username,
            }),
    }
}

fn fs_idempotency_scope(session: &Session) -> String {
    match session.mount() {
        Some(mount) => format!("fs:{}", mount.workspace_id()),
        None => "fs:unmounted".to_string(),
    }
}

fn mounted_workspace_id(session: &Session) -> Option<uuid::Uuid> {
    session.mount().map(|mount| mount.workspace_id())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn mime_type_from_headers(headers: &HeaderMap) -> Result<Option<String>, VfsError> {
    let Some(value) = headers.get("x-stratum-mime-type") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| VfsError::InvalidArgs {
        message: "x-stratum-mime-type must be valid ASCII".to_string(),
    })?;
    validate_mime_type(value)?;
    Ok(Some(value.to_string()))
}

fn stat_to_json(info: &crate::fs::StatInfo) -> serde_json::Value {
    serde_json::json!({
        "inode_id": info.inode_id,
        "kind": info.kind,
        "size": info.size,
        "mode": format!("0{:o}", info.mode),
        "uid": info.uid,
        "gid": info.gid,
        "created": info.created,
        "modified": info.modified,
        "mime_type": info.mime_type,
        "content_hash": info.content_hash,
        "custom_attrs": info.custom_attrs,
    })
}

fn metadata_request_fingerprint_json(request: &MetadataPatchRequest) -> serde_json::Value {
    let mime_type = match &request.mime_type {
        None => serde_json::json!({"op": "absent"}),
        Some(None) => serde_json::json!({"op": "clear"}),
        Some(Some(value)) => serde_json::json!({"op": "set", "value": value}),
    };
    serde_json::json!({
        "mime_type": mime_type,
        "custom_attrs": request.custom_attrs,
        "remove_custom_attrs": request.remove_custom_attrs,
    })
}

async fn begin_idempotent_json_response(
    state: &AppState,
    session: &Session,
    scope: &str,
    fingerprint: &str,
    key: &IdempotencyKey,
) -> Result<Option<IdempotencyReservation>, axum::response::Response> {
    match state.idempotency.begin(scope, key, fingerprint).await {
        Ok(IdempotencyBegin::Execute(reservation)) => Ok(Some(reservation)),
        Ok(IdempotencyBegin::Replay(record)) => {
            Err(http_idempotency::idempotency_json_replay_response(record))
        }
        Ok(IdempotencyBegin::Conflict) => Err(http_idempotency::idempotency_conflict_response()),
        Ok(IdempotencyBegin::InProgress) => {
            Err(http_idempotency::idempotency_in_progress_response())
        }
        Err(e) => Err(err_json_for(session, &e, StatusCode::INTERNAL_SERVER_ERROR)),
    }
}

async fn abort_idempotency(state: &AppState, reservation: Option<IdempotencyReservation>) {
    if let Some(reservation) = reservation {
        state.idempotency.abort(&reservation).await;
    }
}

#[derive(Clone)]
struct DurableFsMutationRecoveryObservation {
    repo_id: RepoId,
    workspace_scope: String,
    target_ref: RefName,
    previous_commit: CommitId,
    new_commit: CommitId,
}

#[derive(Clone)]
struct DurableFsAuditRecoverySeed {
    action: AuditAction,
    changed_paths: Vec<String>,
}

impl DurableFsAuditRecoverySeed {
    fn new(action: AuditAction, changed_paths: impl IntoIterator<Item = String>) -> Self {
        Self {
            action,
            changed_paths: changed_paths.into_iter().collect(),
        }
    }
}

fn durable_fs_mutation_capability(
    state: &AppState,
    session: &Session,
) -> Option<GuardedDurableCommitRoute> {
    session.mount()?.session_ref()?;
    state.core.guarded_durable_commit_route()
}

fn durable_fs_mutation_recovery_from_output(
    state: &AppState,
    session: &Session,
    output: Option<&DurableMutationOutput>,
) -> Option<DurableFsMutationRecoveryObservation> {
    let output = output?;
    let capability = durable_fs_mutation_capability(state, session)?;
    Some(DurableFsMutationRecoveryObservation {
        repo_id: capability.repo_id().clone(),
        workspace_scope: fs_idempotency_scope(session),
        target_ref: output.response_metadata.session_ref.clone(),
        previous_commit: output.previous_commit,
        new_commit: output.new_commit,
    })
}

async fn enqueue_durable_fs_mutation_recovery(
    state: &AppState,
    observation: Option<&DurableFsMutationRecoveryObservation>,
    failed_step: DurableFsMutationRecoveryStep,
    audit: Option<&DurableFsAuditRecoverySeed>,
    reservation: Option<&IdempotencyReservation>,
    status: StatusCode,
    body: &serde_json::Value,
) -> Result<(), VfsError> {
    let Some(observation) = observation else {
        return Ok(());
    };
    let Some(capability) = state.core.guarded_durable_commit_route() else {
        return Ok(());
    };
    let idempotency_context = reservation
        .map(|reservation| {
            DurableFsMutationIdempotencyRecoveryContext::from_reservation(
                reservation,
                status.as_u16(),
                body.clone(),
            )
        })
        .transpose()?;
    let audit_context = audit
        .map(|audit| {
            let changed_paths = audit
                .changed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            DurableFsMutationAuditRecoveryContext::new(audit.action, &changed_paths)
        })
        .transpose()?;
    if idempotency_context.is_none() && audit_context.is_none() {
        return Ok(());
    }

    let operation_id = reservation
        .map(|reservation| reservation.key_hash().to_string())
        .unwrap_or_else(|| observation.new_commit.to_hex());
    let target = DurableFsMutationRecoveryTarget::new(
        observation.repo_id.clone(),
        observation.workspace_scope.clone(),
        operation_id,
        observation.target_ref.as_str(),
        observation.previous_commit,
        observation.new_commit,
        failed_step,
    )?;
    let envelope = DurableFsMutationRecoveryEnvelope::new(idempotency_context, audit_context, None);
    capability
        .stores()
        .fs_mutation_recovery
        .enqueue(target, envelope, current_unix_timestamp_millis())
        .await
}

async fn complete_idempotent_json_response_with_recovery(
    state: &AppState,
    session: &Session,
    reservation: Option<IdempotencyReservation>,
    recovery: Option<&DurableFsMutationRecoveryObservation>,
    status: StatusCode,
    body: serde_json::Value,
) -> axum::response::Response {
    if let Some(reservation) = reservation
        && let Err(e) = state
            .idempotency
            .complete(&reservation, status.as_u16(), body.clone())
            .await
    {
        if enqueue_durable_fs_mutation_recovery(
            state,
            recovery,
            DurableFsMutationRecoveryStep::IdempotencyCompletion,
            None,
            Some(&reservation),
            status,
            &body,
        )
        .await
        .is_ok()
            && recovery.is_some()
        {
            return (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "mutation_committed": true,
                    "idempotency_recorded": false,
                    "recovery_enqueued": true,
                })),
            )
                .into_response();
        }
        return err_json_for(session, &e, StatusCode::INTERNAL_SERVER_ERROR);
    }
    (status, Json(body)).into_response()
}

async fn complete_audit_failure_with_recovery(
    state: &AppState,
    session: &Session,
    reservation: Option<IdempotencyReservation>,
    recovery: Option<&DurableFsMutationRecoveryObservation>,
    audit: &DurableFsAuditRecoverySeed,
    response: (StatusCode, serde_json::Value),
) -> axum::response::Response {
    let (status, body) = response;
    let body = if recovery.is_some() {
        durable_visible_mutation_side_effect_failed_body(&body)
    } else {
        body
    };
    if recovery.is_some()
        && enqueue_durable_fs_mutation_recovery(
            state,
            recovery,
            DurableFsMutationRecoveryStep::AuditAppend,
            Some(audit),
            reservation.as_ref(),
            status,
            &body,
        )
        .await
        .is_err()
    {
        let recovery_body = serde_json::json!({
            "error": "durable FS mutation recovery is required",
            "mutation_committed": true,
            "audit_recorded": false,
            "recovery_enqueued": false,
        });
        return complete_idempotent_json_response_with_recovery(
            state,
            session,
            reservation,
            recovery,
            StatusCode::INTERNAL_SERVER_ERROR,
            recovery_body,
        )
        .await;
    }
    complete_idempotent_json_response_with_recovery(
        state,
        session,
        reservation,
        recovery,
        status,
        body,
    )
    .await
}

fn durable_visible_mutation_side_effect_failed_body(
    original_body: &serde_json::Value,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "error": "durable FS mutation side effect failed after mutation",
        "mutation_committed": true,
    });
    if let Some(audit_recorded) = original_body.get("audit_recorded") {
        body["audit_recorded"] = audit_recorded.clone();
    }
    body
}

fn current_unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

async fn require_unprotected_paths(
    state: &AppState,
    session: &Session,
    paths: &[&str],
) -> Result<(), axum::response::Response> {
    require_unprotected_paths_with_descendants(state, session, paths, false).await
}

async fn require_unprotected_paths_with_descendants(
    state: &AppState,
    session: &Session,
    paths: &[&str],
    include_protected_descendants: bool,
) -> Result<(), axum::response::Response> {
    let rules = state
        .review
        .list_protected_path_rules()
        .await
        .map_err(|e| err_json_for(session, &e, StatusCode::INTERNAL_SERVER_ERROR))?;

    for path in paths {
        let blocked = rules.iter().any(|rule| {
            rule.matches_path(path)
                || (include_protected_descendants && protected_rule_is_descendant(rule, path))
        });
        if blocked {
            let projected = session.project_mounted_error_path(path);
            return Err(err_json(
                StatusCode::FORBIDDEN,
                format!("protected path requires change request merge: '{projected}'"),
            )
            .into_response());
        }
    }

    Ok(())
}

fn protected_rule_is_descendant(rule: &crate::review::ProtectedPathRule, path: &str) -> bool {
    if !rule.active {
        return false;
    }
    let Ok(path) = crate::review::normalize_path_prefix(path) else {
        return false;
    };
    if path == "/" {
        return true;
    }
    rule.path_prefix
        .strip_prefix(&path)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn existing_write_targets(
    state: &AppState,
    session: &Session,
    path: &str,
) -> Result<Vec<String>, axum::response::Response> {
    let mut paths = vec![path.to_string()];
    match state.core.final_existing_write_path_as(path, session).await {
        Ok(Some(final_path)) if final_path != path => paths.push(final_path),
        Ok(_) => {}
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    }
    Ok(paths)
}

fn path_refs(paths: &[String]) -> Vec<&str> {
    paths.iter().map(String::as_str).collect()
}

async fn begin_put_idempotency(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    path: &str,
    is_dir: bool,
    mime_type: Option<&str>,
    body: &[u8],
) -> Result<Option<IdempotencyReservation>, axum::response::Response> {
    let key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(Some(key)) => key,
        Ok(None) => return Ok(None),
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    };

    let x_stratum_type = headers
        .get("x-stratum-type")
        .and_then(|value| value.to_str().ok());

    let preflight = if is_dir {
        state.core.check_mkdir_p_as(path, session).await
    } else {
        state.core.check_write_file_as(path, session).await
    };
    if let Err(e) = preflight {
        return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
    }

    let scope = fs_idempotency_scope(session);
    let fingerprint = request_fingerprint(
        &scope,
        &serde_json::json!({
            "route": "PUT /fs/{path}",
            "actor": actor_fingerprint(session),
            "workspace_id": mounted_workspace_id(session),
            "backing_path": path,
            "projected_path": session.project_mounted_path(path),
            "operation": if is_dir { "mkdir_p" } else { "write_file" },
            "x_stratum_type": x_stratum_type,
            "x_stratum_mime_type": mime_type,
            "is_directory": is_dir,
            "body": if is_dir {
                serde_json::Value::Null
            } else {
                serde_json::json!({
                    "sha256": sha256_hex(body),
                    "byte_length": body.len(),
                })
            },
        }),
    )
    .map_err(|e| err_json_for(session, &e, StatusCode::BAD_REQUEST))?;

    begin_idempotent_json_response(state, session, &scope, &fingerprint, &key).await
}

async fn begin_metadata_idempotency(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    path: &str,
    request: &MetadataPatchRequest,
) -> Result<Option<IdempotencyReservation>, axum::response::Response> {
    let key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(Some(key)) => key,
        Ok(None) => return Ok(None),
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    };

    if let Err(e) = state.core.check_set_metadata_as(path, session).await {
        return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
    }

    let scope = fs_idempotency_scope(session);
    let fingerprint = request_fingerprint(
        &scope,
        &serde_json::json!({
            "route": "PATCH /fs/{path}",
            "actor": actor_fingerprint(session),
            "workspace_id": mounted_workspace_id(session),
            "backing_path": path,
            "projected_path": session.project_mounted_path(path),
            "metadata": metadata_request_fingerprint_json(request),
        }),
    )
    .map_err(|e| err_json_for(session, &e, StatusCode::BAD_REQUEST))?;

    begin_idempotent_json_response(state, session, &scope, &fingerprint, &key).await
}

async fn begin_delete_idempotency(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    path: &str,
    recursive: bool,
) -> Result<Option<IdempotencyReservation>, axum::response::Response> {
    let key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(Some(key)) => key,
        Ok(None) => return Ok(None),
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    };

    if let Err(e) = state.core.check_rm_as(path, recursive, session).await {
        match e {
            VfsError::NotFound { .. } => {
                if let Err(e) = state.core.check_write_file_as(path, session).await {
                    return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
                }
            }
            e => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
        }
    }

    let scope = fs_idempotency_scope(session);
    let fingerprint = request_fingerprint(
        &scope,
        &serde_json::json!({
            "route": "DELETE /fs/{path}",
            "actor": actor_fingerprint(session),
            "workspace_id": mounted_workspace_id(session),
            "backing_path": path,
            "projected_path": session.project_mounted_path(path),
            "operation": "delete",
            "recursive": recursive,
        }),
    )
    .map_err(|e| err_json_for(session, &e, StatusCode::BAD_REQUEST))?;

    begin_idempotent_json_response(state, session, &scope, &fingerprint, &key).await
}

async fn begin_copy_move_idempotency(
    state: &AppState,
    session: &Session,
    headers: &HeaderMap,
    src: &str,
    dst: &str,
    op: &str,
) -> Result<Option<IdempotencyReservation>, axum::response::Response> {
    let key = match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(Some(key)) => key,
        Ok(None) => return Ok(None),
        Err(e) => return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    };

    let replay_preflight = if op == "copy" {
        state.core.check_cp_replay_as(src, dst, session).await
    } else {
        state.core.check_mv_replay_as(src, dst, session).await
    };
    if let Err(e) = replay_preflight {
        return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
    }

    let scope = fs_idempotency_scope(session);
    let fingerprint = request_fingerprint(
        &scope,
        &serde_json::json!({
            "route": "POST /fs/{path}",
            "actor": actor_fingerprint(session),
            "workspace_id": mounted_workspace_id(session),
            "backing_path": src,
            "backing_dst_query_path": dst,
            "projected_path": session.project_mounted_path(src),
            "projected_response_to": session.project_mounted_path(dst),
            "operation": op,
            "query": {
                "op": op,
                "dst": session.project_mounted_path(dst),
            },
        }),
    )
    .map_err(|e| err_json_for(session, &e, StatusCode::BAD_REQUEST))?;

    let reservation =
        begin_idempotent_json_response(state, session, &scope, &fingerprint, &key).await?;

    if let Some(reservation) = reservation.as_ref() {
        let mutation_preflight = if op == "copy" {
            state.core.check_cp_as(src, dst, session).await
        } else {
            state.core.check_mv_as(src, dst, session).await
        };
        if let Err(e) = mutation_preflight {
            state.idempotency.abort(reservation).await;
            return Err(err_json_for(session, &e, StatusCode::BAD_REQUEST));
        }
    }

    Ok(reservation)
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/fs", get(get_fs_root))
        .route(
            "/fs/{*path}",
            get(get_fs)
                .put(put_fs)
                .patch(patch_fs)
                .delete(delete_fs)
                .post(post_fs),
        )
        .route("/search/grep", get(search_grep))
        .route("/search/find", get(search_find))
        .route("/tree", get(get_tree_root))
        .route("/tree/{*path}", get(get_tree))
}

async fn get_fs_root(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_root_path(&session) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    match state.core.ls_as(Some(&path), &session).await {
        Ok(entries) => {
            Json(ls_to_json(&entries, &session.project_mounted_path(&path))).into_response()
        }
        Err(e) => err_json_for(&session, &e, StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<FsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    if query.stat.unwrap_or(false) {
        return match state.core.stat_as(&path, &session).await {
            Ok(info) => Json(stat_to_json(&info)).into_response(),
            Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
        };
    }

    match state.core.cat_with_stat_as(&path, &session).await {
        Ok((content, stat)) => {
            let content_type = stat
                .mime_type
                .unwrap_or_else(|| "application/octet-stream".to_string());
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Body::from(content),
            )
                .into_response()
        }
        Err(crate::error::VfsError::IsDirectory { .. }) => {
            match state.core.ls_as(Some(&path), &session).await {
                Ok(entries) => {
                    Json(ls_to_json(&entries, &session.project_mounted_path(&path))).into_response()
                }
                Err(e) => err_json_for(&session, &e, StatusCode::INTERNAL_SERVER_ERROR),
            }
        }
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
}

async fn put_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let is_dir = headers
        .get("x-stratum-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "directory")
        .unwrap_or(false);
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let mime_type = match mime_type_from_headers(&headers) {
        Ok(mime_type) => mime_type,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    let protected_paths = match existing_write_targets(&state, &session, &path).await {
        Ok(paths) => paths,
        Err(response) => return response,
    };
    if let Err(response) =
        require_unprotected_paths(&state, &session, &path_refs(&protected_paths)).await
    {
        return response;
    }

    let reservation = match begin_put_idempotency(
        &state,
        &session,
        &headers,
        &path,
        is_dir,
        mime_type.as_deref(),
        &body,
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(response) => return response,
    };
    let durable_capability = durable_fs_mutation_capability(&state, &session);

    if is_dir {
        let mutation = match &durable_capability {
            Some(capability) => capability
                .mkdir_p_output_as(&path, &session)
                .await
                .map(Some),
            None => state.core.mkdir_p_as(&path, &session).await.map(|()| None),
        };
        match mutation {
            Ok(output) => {
                let recovery =
                    durable_fs_mutation_recovery_from_output(&state, &session, output.as_ref());
                let project_path = session.project_mounted_path(&path);
                let audit_seed =
                    DurableFsAuditRecoverySeed::new(AuditAction::FsMkdir, [path.clone()]);
                if let Err(response) = append_audit(
                    &state,
                    NewAuditEvent::from_session(
                        &session,
                        AuditAction::FsMkdir,
                        AuditResource::path(AuditResourceKind::Directory, &path),
                    )
                    .with_detail("project_path", &project_path),
                )
                .await
                {
                    return complete_audit_failure_with_recovery(
                        &state,
                        &session,
                        reservation,
                        recovery.as_ref(),
                        &audit_seed,
                        response,
                    )
                    .await;
                }
                let body = serde_json::json!({
                    "created": project_path,
                    "type": "directory"
                });
                complete_idempotent_json_response_with_recovery(
                    &state,
                    &session,
                    reservation,
                    recovery.as_ref(),
                    StatusCode::OK,
                    body,
                )
                .await
            }
            Err(e) => {
                abort_idempotency(&state, reservation).await;
                err_json_for(&session, &e, StatusCode::BAD_REQUEST)
            }
        }
    } else {
        let size = body.len();
        let mutation = match &durable_capability {
            Some(capability) => capability
                .write_file_with_metadata_output_as(
                    &path,
                    body.to_vec(),
                    mime_type.clone(),
                    &session,
                )
                .await
                .map(Some),
            None => match state
                .core
                .write_file_as(&path, body.to_vec(), &session)
                .await
            {
                Ok(()) => {
                    if let Some(mime_type) = mime_type {
                        let update = MetadataUpdate {
                            mime_type: Some(Some(mime_type)),
                            ..MetadataUpdate::default()
                        };
                        if let Err(e) = state.core.set_metadata_as(&path, update, &session).await {
                            let body = serde_json::json!({
                                "error": format!("metadata update failed after write: {e}"),
                                "mutation_committed": true,
                            });
                            let audit_seed = DurableFsAuditRecoverySeed::new(
                                AuditAction::FsWriteFile,
                                [path.clone()],
                            );
                            return complete_audit_failure_with_recovery(
                                &state,
                                &session,
                                reservation,
                                None,
                                &audit_seed,
                                (StatusCode::INTERNAL_SERVER_ERROR, body),
                            )
                            .await;
                        }
                    }
                    Ok(None)
                }
                Err(e) => Err(e),
            },
        };
        match mutation {
            Ok(output) => {
                let recovery =
                    durable_fs_mutation_recovery_from_output(&state, &session, output.as_ref());
                let project_path = session.project_mounted_path(&path);
                let audit_seed =
                    DurableFsAuditRecoverySeed::new(AuditAction::FsWriteFile, [path.clone()]);
                if let Err(response) = append_audit(
                    &state,
                    NewAuditEvent::from_session(
                        &session,
                        AuditAction::FsWriteFile,
                        AuditResource::path(AuditResourceKind::File, &path),
                    )
                    .with_detail("project_path", &project_path)
                    .with_detail("size", size),
                )
                .await
                {
                    return complete_audit_failure_with_recovery(
                        &state,
                        &session,
                        reservation,
                        recovery.as_ref(),
                        &audit_seed,
                        response,
                    )
                    .await;
                }
                let body = serde_json::json!({
                    "written": project_path,
                    "size": size
                });
                complete_idempotent_json_response_with_recovery(
                    &state,
                    &session,
                    reservation,
                    recovery.as_ref(),
                    StatusCode::OK,
                    body,
                )
                .await
            }
            Err(e) => {
                abort_idempotency(&state, reservation).await;
                err_json_for(&session, &e, StatusCode::BAD_REQUEST)
            }
        }
    }
}

async fn patch_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Json(request): Json<MetadataPatchRequest>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let protected_paths = match existing_write_targets(&state, &session, &path).await {
        Ok(paths) => paths,
        Err(response) => return response,
    };
    if let Err(response) =
        require_unprotected_paths(&state, &session, &path_refs(&protected_paths)).await
    {
        return response;
    }

    let reservation =
        match begin_metadata_idempotency(&state, &session, &headers, &path, &request).await {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };
    let update = MetadataUpdate::from(request);
    let durable_capability = durable_fs_mutation_capability(&state, &session);
    let mutation = match &durable_capability {
        Some(capability) => capability
            .set_metadata_output_as(&path, update, &session)
            .await
            .map(|(output, result)| (Some(output), result)),
        None => state
            .core
            .set_metadata_as(&path, update, &session)
            .await
            .map(|result| (None, result)),
    };
    match mutation {
        Ok((output, result)) => {
            let recovery =
                durable_fs_mutation_recovery_from_output(&state, &session, output.as_ref());
            let project_path = session.project_mounted_path(&path);
            let audit_seed =
                DurableFsAuditRecoverySeed::new(AuditAction::FsMetadataUpdate, [path.clone()]);
            if let Err(response) = append_audit(
                &state,
                NewAuditEvent::from_session(
                    &session,
                    AuditAction::FsMetadataUpdate,
                    AuditResource::path(AuditResourceKind::Path, &path),
                )
                .with_detail("project_path", &project_path)
                .with_detail("mime_type_changed", result.mime_type_changed)
                .with_detail("custom_attrs_set", result.custom_attrs_set.join(","))
                .with_detail(
                    "custom_attrs_removed",
                    result.custom_attrs_removed.join(","),
                ),
            )
            .await
            {
                return complete_audit_failure_with_recovery(
                    &state,
                    &session,
                    reservation,
                    recovery.as_ref(),
                    &audit_seed,
                    response,
                )
                .await;
            }

            let custom_attr_keys = result.custom_attrs.keys().cloned().collect::<Vec<_>>();
            let body = serde_json::json!({
                "metadata_updated": project_path,
                "changed": result.changed,
                "mime_type": result.mime_type,
                "custom_attr_keys": custom_attr_keys,
                "custom_attrs_set": result.custom_attrs_set,
                "custom_attrs_removed": result.custom_attrs_removed,
            });
            complete_idempotent_json_response_with_recovery(
                &state,
                &session,
                reservation,
                recovery.as_ref(),
                StatusCode::OK,
                body,
            )
            .await
        }
        Err(e) => {
            abort_idempotency(&state, reservation).await;
            err_json_for(&session, &e, StatusCode::BAD_REQUEST)
        }
    }
}

async fn delete_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<FsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let recursive = query.recursive.unwrap_or(false);
    if let Err(response) =
        require_unprotected_paths_with_descendants(&state, &session, &[&path], true).await
    {
        return response;
    }
    let reservation =
        match begin_delete_idempotency(&state, &session, &headers, &path, recursive).await {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };
    let durable_capability = durable_fs_mutation_capability(&state, &session);
    let result = match &durable_capability {
        Some(capability) => capability
            .rm_output_as(&path, recursive, &session)
            .await
            .map(Some),
        None => state
            .core
            .rm_as(&path, recursive, &session)
            .await
            .map(|()| None),
    };

    match result {
        Ok(output) => {
            let recovery =
                durable_fs_mutation_recovery_from_output(&state, &session, output.as_ref());
            let project_path = session.project_mounted_path(&path);
            let audit_seed = DurableFsAuditRecoverySeed::new(AuditAction::FsDelete, [path.clone()]);
            if let Err(response) = append_audit(
                &state,
                NewAuditEvent::from_session(
                    &session,
                    AuditAction::FsDelete,
                    AuditResource::path(AuditResourceKind::Path, &path),
                )
                .with_detail("project_path", &project_path)
                .with_detail("recursive", recursive),
            )
            .await
            {
                return complete_audit_failure_with_recovery(
                    &state,
                    &session,
                    reservation,
                    recovery.as_ref(),
                    &audit_seed,
                    response,
                )
                .await;
            }
            let body = serde_json::json!({
                "deleted": project_path
            });
            complete_idempotent_json_response_with_recovery(
                &state,
                &session,
                reservation,
                recovery.as_ref(),
                StatusCode::OK,
                body,
            )
            .await
        }
        Err(e) => {
            abort_idempotency(&state, reservation).await;
            err_json_for(&session, &e, StatusCode::BAD_REQUEST)
        }
    }
}

async fn post_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<FsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    match query.op.as_deref() {
        Some("copy") => {
            let dst = match &query.dst {
                Some(d) => d.as_str(),
                None => {
                    return err_json(StatusCode::BAD_REQUEST, "missing dst parameter")
                        .into_response();
                }
            };
            let dst = match resolve_api_path(&session, dst) {
                Ok(dst) => dst,
                Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            };
            if let Err(response) = require_unprotected_paths(&state, &session, &[&dst]).await {
                return response;
            }
            let reservation =
                match begin_copy_move_idempotency(&state, &session, &headers, &path, &dst, "copy")
                    .await
                {
                    Ok(reservation) => reservation,
                    Err(response) => return response,
                };
            let durable_capability = durable_fs_mutation_capability(&state, &session);
            let mutation = match &durable_capability {
                Some(capability) => capability
                    .cp_output_as(&path, &dst, &session)
                    .await
                    .map(Some),
                None => state.core.cp_as(&path, &dst, &session).await.map(|()| None),
            };
            match mutation {
                Ok(output) => {
                    let recovery =
                        durable_fs_mutation_recovery_from_output(&state, &session, output.as_ref());
                    let project_path = session.project_mounted_path(&path);
                    let dst_project_path = session.project_mounted_path(&dst);
                    let audit_seed = DurableFsAuditRecoverySeed::new(
                        AuditAction::FsCopy,
                        [path.clone(), dst.clone()],
                    );
                    if let Err(response) = append_audit(
                        &state,
                        NewAuditEvent::from_session(
                            &session,
                            AuditAction::FsCopy,
                            AuditResource::path(AuditResourceKind::Path, &path),
                        )
                        .with_detail("project_path", &project_path)
                        .with_detail("dst_path", &dst)
                        .with_detail("dst_project_path", &dst_project_path),
                    )
                    .await
                    {
                        return complete_audit_failure_with_recovery(
                            &state,
                            &session,
                            reservation,
                            recovery.as_ref(),
                            &audit_seed,
                            response,
                        )
                        .await;
                    }
                    let body = serde_json::json!({
                        "copied": project_path,
                        "to": dst_project_path
                    });
                    complete_idempotent_json_response_with_recovery(
                        &state,
                        &session,
                        reservation,
                        recovery.as_ref(),
                        StatusCode::OK,
                        body,
                    )
                    .await
                }
                Err(e) => {
                    abort_idempotency(&state, reservation).await;
                    err_json_for(&session, &e, StatusCode::BAD_REQUEST)
                }
            }
        }
        Some("move") => {
            let dst = match &query.dst {
                Some(d) => d.as_str(),
                None => {
                    return err_json(StatusCode::BAD_REQUEST, "missing dst parameter")
                        .into_response();
                }
            };
            let dst = match resolve_api_path(&session, dst) {
                Ok(dst) => dst,
                Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            };
            if let Err(response) =
                require_unprotected_paths_with_descendants(&state, &session, &[&path], true).await
            {
                return response;
            }
            if let Err(response) = require_unprotected_paths(&state, &session, &[&dst]).await {
                return response;
            }
            let reservation =
                match begin_copy_move_idempotency(&state, &session, &headers, &path, &dst, "move")
                    .await
                {
                    Ok(reservation) => reservation,
                    Err(response) => return response,
                };
            let durable_capability = durable_fs_mutation_capability(&state, &session);
            let mutation = match &durable_capability {
                Some(capability) => capability
                    .mv_output_as(&path, &dst, &session)
                    .await
                    .map(Some),
                None => state.core.mv_as(&path, &dst, &session).await.map(|()| None),
            };
            match mutation {
                Ok(output) => {
                    let recovery =
                        durable_fs_mutation_recovery_from_output(&state, &session, output.as_ref());
                    let project_path = session.project_mounted_path(&path);
                    let dst_project_path = session.project_mounted_path(&dst);
                    let audit_seed = DurableFsAuditRecoverySeed::new(
                        AuditAction::FsMove,
                        [path.clone(), dst.clone()],
                    );
                    if let Err(response) = append_audit(
                        &state,
                        NewAuditEvent::from_session(
                            &session,
                            AuditAction::FsMove,
                            AuditResource::path(AuditResourceKind::Path, &path),
                        )
                        .with_detail("project_path", &project_path)
                        .with_detail("dst_path", &dst)
                        .with_detail("dst_project_path", &dst_project_path),
                    )
                    .await
                    {
                        return complete_audit_failure_with_recovery(
                            &state,
                            &session,
                            reservation,
                            recovery.as_ref(),
                            &audit_seed,
                            response,
                        )
                        .await;
                    }
                    let body = serde_json::json!({
                        "moved": project_path,
                        "to": dst_project_path
                    });
                    complete_idempotent_json_response_with_recovery(
                        &state,
                        &session,
                        reservation,
                        recovery.as_ref(),
                        StatusCode::OK,
                        body,
                    )
                    .await
                }
                Err(e) => {
                    abort_idempotency(&state, reservation).await;
                    err_json_for(&session, &e, StatusCode::BAD_REQUEST)
                }
            }
        }
        Some(op) => err_json(StatusCode::BAD_REQUEST, format!("unknown op: {op}")).into_response(),
        None => err_json(StatusCode::BAD_REQUEST, "missing op parameter").into_response(),
    }
}

async fn search_grep(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let pattern = match &query.pattern {
        Some(p) => p.clone(),
        None => {
            return err_json(StatusCode::BAD_REQUEST, "missing pattern parameter").into_response();
        }
    };

    let path = match resolve_optional_query_path(&session, query.path.as_deref()) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let recursive = query.recursive.unwrap_or(true);

    match state
        .core
        .grep_as(&pattern, Some(&path), recursive, &session)
        .await
    {
        Ok(results) => {
            let items: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "file": session.project_mounted_path(&r.file),
                        "line_num": r.line_num,
                        "line": r.line
                    })
                })
                .collect();
            Json(serde_json::json!({"results": items, "count": items.len()})).into_response()
        }
        Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    }
}

async fn search_find(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let path = match resolve_optional_query_path(&session, query.path.as_deref()) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let name = query.name.as_deref();

    match state.core.find_as(Some(&path), name, &session).await {
        Ok(results) => {
            let results: Vec<String> = results
                .iter()
                .map(|path| session.project_mounted_path(path))
                .collect();
            Json(serde_json::json!({"results": results, "count": results.len()})).into_response()
        }
        Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    }
}

async fn get_tree_root(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_root_path(&session) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    match state.core.tree_as(Some(&path), &session).await {
        Ok(tree) => (StatusCode::OK, tree).into_response(),
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
}

async fn get_tree(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    match state.core.tree_as(Some(&path), &session).await {
        Ok(tree) => (StatusCode::OK, tree).into_response(),
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
}

fn ls_to_json(entries: &[crate::fs::LsEntry], path: &str) -> serde_json::Value {
    let items: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            serde_json::json!({
                "name": e.name,
                "is_dir": e.is_dir,
                "is_symlink": e.is_symlink,
                "size": e.size,
                "mode": format!("0{:o}", e.mode),
                "uid": e.uid,
                "gid": e.gid,
                "modified": e.modified,
            })
        })
        .collect();
    serde_json::json!({"entries": items, "path": path})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditEvent, AuditStore};
    use crate::auth::session::Session;
    use crate::auth::{ROOT_GID, ROOT_UID};
    use crate::backend::committed_read::DurableCommittedFsReader;
    use crate::backend::core_transaction::DurableFsMutationRecoveryState;
    use crate::backend::{
        CommitRecord, ObjectWrite, RefExpectation, RefUpdate, RepoId, StratumStores,
    };
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::server::core::LocalCoreRuntime;
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, MAIN_REF, RefName};
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use std::sync::Arc;
    use uuid::Uuid;

    struct FailingAuditStore;

    #[async_trait::async_trait]
    impl AuditStore for FailingAuditStore {
        async fn append(&self, _event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
            Err(VfsError::CorruptStore {
                message: "audit append failed with private-store-detail".to_string(),
            })
        }

        async fn list_recent(&self, _limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
            Ok(Vec::new())
        }

        async fn contains_vcs_commit_event(&self, _commit_id: &str) -> Result<bool, VfsError> {
            Ok(false)
        }
    }

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

    fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            kind,
            id,
            mode,
            uid: ROOT_UID,
            gid: ROOT_GID,
            mime_type: None,
            custom_attrs: Default::default(),
        }
    }

    async fn put_object(
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

    async fn seed_durable_read_fixture(stores: &StratumStores) -> ObjectId {
        let repo_id = RepoId::local();
        let note_id = put_object(
            stores,
            &repo_id,
            ObjectKind::Blob,
            b"durable route\nTODO served from committed object\n".to_vec(),
        )
        .await;
        let nested_id = put_object(
            stores,
            &repo_id,
            ObjectKind::Blob,
            b"nested durable route".to_vec(),
        )
        .await;
        let nested_tree_id = put_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "nested.txt",
                    TreeEntryKind::Blob,
                    nested_id,
                    0o644,
                )],
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("docs", TreeEntryKind::Tree, nested_tree_id, 0o755),
                    tree_entry("notes.txt", TreeEntryKind::Blob, note_id, 0o644),
                ],
            }
            .serialize(),
        )
        .await;
        let commit_id = CommitId::from(ObjectId::from_bytes(b"durable fs route"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1_725_000_002,
                message: "durable fs route".to_string(),
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
        note_id
    }

    async fn seed_durable_workspace_base(stores: &StratumStores) -> CommitId {
        seed_durable_workspace_base_with_demo_mode(stores, 0o755).await
    }

    async fn seed_durable_workspace_base_with_demo_mode(
        stores: &StratumStores,
        demo_mode: u16,
    ) -> CommitId {
        seed_durable_workspace_base_with_demo_entries(stores, demo_mode, Vec::new()).await
    }

    async fn seed_durable_workspace_base_with_demo_entries(
        stores: &StratumStores,
        demo_mode: u16,
        demo_entries: Vec<TreeEntry>,
    ) -> CommitId {
        let repo_id = RepoId::local();
        let demo_tree_id = put_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: demo_entries,
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_object(
            stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "demo",
                    TreeEntryKind::Tree,
                    demo_tree_id,
                    demo_mode,
                )],
            }
            .serialize(),
        )
        .await;
        let commit_id = CommitId::from(ObjectId::from_bytes(b"durable workspace base"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree: root_tree_id,
                parents: Vec::new(),
                timestamp: 1_725_000_003,
                message: "durable workspace base".to_string(),
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

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
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

    fn workspace_headers(workspace_id: Uuid, raw_secret: &str) -> HeaderMap {
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

    fn with_idempotency_key(mut headers: HeaderMap, key: &str) -> HeaderMap {
        headers.insert("idempotency-key", key.parse().unwrap());
        headers
    }

    async fn response_bytes(response: axum::response::Response) -> Bytes {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        serde_json::from_slice(&response_bytes(response).await).unwrap()
    }

    async fn assert_projected_error(
        response: axum::response::Response,
        status: StatusCode,
        expected_path: &str,
    ) {
        assert_eq!(response.status(), status);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains(expected_path), "{error}");
        assert!(!error.contains("/demo/"), "{error}");
    }

    async fn assert_redacted_external_error(response: axum::response::Response) {
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains("<outside workspace>"), "{error}");
        assert!(!error.contains("/demo/"), "{error}");
        assert!(!error.contains("/outside/"), "{error}");
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
                "ci-token",
                agent_uid,
                read_prefixes,
                write_prefixes,
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        (state, workspace.id, issued.raw_secret)
    }

    async fn durable_workspace_state_with_token(
        stores: StratumStores,
        session_ref: &str,
    ) -> (AppState, Uuid, String) {
        durable_workspace_state_with_token_for_uid(stores, session_ref, ROOT_UID).await
    }

    async fn durable_workspace_state_with_token_for_uid(
        stores: StratumStores,
        session_ref: &str,
        agent_uid: u32,
    ) -> (AppState, Uuid, String) {
        durable_workspace_state_with_scoped_token(
            stores,
            session_ref,
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await
    }

    async fn durable_workspace_state_with_scoped_token(
        stores: StratumStores,
        session_ref: &str,
        agent_uid: u32,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> (AppState, Uuid, String) {
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("demo", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-ci-token",
                agent_uid,
                read_prefixes,
                write_prefixes,
            )
            .await
            .unwrap();
        (
            guarded_durable_commit_state(StratumDb::open_memory(), stores),
            workspace.id,
            issued.raw_secret,
        )
    }

    #[tokio::test]
    async fn guarded_durable_fs_routes_read_committed_tree_without_local_state() {
        let stores = StratumStores::local_memory();
        let note_id = seed_durable_read_fixture(&stores).await;
        let state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let headers = user_headers("root");

        let read_response = get_fs(
            State(state.clone()),
            Path("notes.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(read_response).await,
            Bytes::from_static(b"durable route\nTODO served from committed object\n")
        );

        let stat_response = get_fs(
            State(state.clone()),
            Path("notes.txt".to_string()),
            Query(FsQuery {
                stat: Some(true),
                ..Default::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(stat_response.status(), StatusCode::OK);
        let stat = response_json(stat_response).await;
        assert_eq!(stat["kind"], "file");
        assert_eq!(stat["content_hash"], format!("sha256:{}", note_id.to_hex()));

        let ls_response = get_fs_root(State(state.clone()), headers.clone())
            .await
            .into_response();
        assert_eq!(ls_response.status(), StatusCode::OK);
        let listing = response_json(ls_response).await;
        assert_eq!(listing["entries"][0]["name"], "docs");
        assert_eq!(listing["entries"][1]["name"], "notes.txt");

        let tree_response = get_tree_root(State(state.clone()), headers.clone())
            .await
            .into_response();
        assert_eq!(tree_response.status(), StatusCode::OK);
        assert_eq!(
            String::from_utf8(response_bytes(tree_response).await.to_vec()).unwrap(),
            ".\n\u{251c}\u{2500}\u{2500} docs/\n\u{2502}   \u{2514}\u{2500}\u{2500} nested.txt\n\u{2514}\u{2500}\u{2500} notes.txt\n"
        );

        let find_response = search_find(
            State(state.clone()),
            Query(SearchQuery {
                name: Some("*.txt".to_string()),
                path: Some("/".to_string()),
                ..Default::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(find_response.status(), StatusCode::OK);
        let find = response_json(find_response).await;
        assert_eq!(
            find["results"],
            serde_json::json!(["/docs/nested.txt", "/notes.txt"])
        );

        let grep_response = search_grep(
            State(state),
            Query(SearchQuery {
                pattern: Some("TODO".to_string()),
                path: Some("/".to_string()),
                recursive: Some(true),
                ..Default::default()
            }),
            headers,
        )
        .await
        .into_response();
        assert_eq!(grep_response.status(), StatusCode::OK);
        let grep = response_json(grep_response).await;
        assert_eq!(grep["count"], 1);
        assert_eq!(grep["results"][0]["file"], "/notes.txt");
        assert_eq!(
            grep["results"][0]["line"],
            "TODO served from committed object"
        );
    }

    #[tokio::test]
    async fn guarded_durable_unmounted_fs_write_fails_closed_without_local_state() {
        let db = StratumDb::open_memory();
        let state = guarded_durable_commit_state(db.clone(), StratumStores::local_memory());

        let put_response = put_fs(
            State(state),
            Path("/local-only.txt".to_string()),
            user_headers("root"),
            Bytes::from_static(b"local-only durable miss"),
        )
        .await
        .into_response();

        assert_eq!(put_response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(put_response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(
            error.contains("durable mutable workspace route execution is not supported yet"),
            "{error}"
        );
        assert!(matches!(
            db.stat_as("/local-only.txt", &Session::root()).await,
            Err(VfsError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn guarded_durable_write_read_survives_fresh_local_db() {
        let stores = StratumStores::local_memory();
        let base_commit = seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-writer/session-001";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores.clone(), session_ref).await;
        let headers = with_idempotency_key(
            workspace_headers(workspace_id, &raw_secret),
            "durable-fs-write-restart",
        );

        let put_response = put_fs(
            State(state.clone()),
            Path("notes.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"durable session content"),
        )
        .await
        .into_response();
        assert_eq!(put_response.status(), StatusCode::OK);

        assert!(
            state
                .db
                .cat_as("/demo/notes.txt", &Session::root())
                .await
                .is_err(),
            "guarded durable FS write should not require local state"
        );

        let read_response = get_fs(
            State(state.clone()),
            Path("notes.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(read_response).await,
            Bytes::from_static(b"durable session content")
        );

        let main = stores
            .refs
            .get(&RepoId::local(), &RefName::new(MAIN_REF).unwrap())
            .await
            .unwrap()
            .expect("main ref");
        assert_eq!(
            main.target, base_commit,
            "durable FS mutation should advance the session ref, not main"
        );
        let session = stores
            .refs
            .get(&RepoId::local(), &RefName::new(session_ref).unwrap())
            .await
            .unwrap()
            .expect("session ref");
        assert_ne!(session.target, base_commit);

        let fresh_state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let fresh_read = get_fs(
            State(fresh_state),
            Path("notes.txt".to_string()),
            Query(FsQuery::default()),
            headers,
        )
        .await
        .into_response();
        assert_eq!(fresh_read.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(fresh_read).await,
            Bytes::from_static(b"durable session content")
        );
    }

    #[tokio::test]
    async fn guarded_durable_write_only_token_can_mutate_without_read_scope() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let existing_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"metadata target".to_vec(),
        )
        .await;
        let delete_id =
            put_object(&stores, &repo_id, ObjectKind::Blob, b"delete me".to_vec()).await;
        let write_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("existing.txt", TreeEntryKind::Blob, existing_id, 0o666),
                    tree_entry("delete.txt", TreeEntryKind::Blob, delete_id, 0o666),
                ],
            }
            .serialize(),
        )
        .await;
        seed_durable_workspace_base_with_demo_entries(
            &stores,
            0o755,
            vec![tree_entry(
                "write",
                TreeEntryKind::Tree,
                write_tree_id,
                0o777,
            )],
        )
        .await;
        let session_ref = "agent/durable-write-only/session-001";
        let (state, workspace_id, raw_secret) = durable_workspace_state_with_scoped_token(
            stores.clone(),
            session_ref,
            ROOT_UID,
            Vec::new(),
            vec!["/demo/write".to_string()],
        )
        .await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let put_response = put_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"write-only content"),
        )
        .await
        .into_response();
        assert_eq!(put_response.status(), StatusCode::OK);

        let mut mkdir_headers = headers.clone();
        mkdir_headers.insert("x-stratum-type", "directory".parse().unwrap());
        let mkdir_response = put_fs(
            State(state.clone()),
            Path("/write/new-dir".to_string()),
            mkdir_headers,
            Bytes::new(),
        )
        .await
        .into_response();
        assert_eq!(mkdir_response.status(), StatusCode::OK);

        let patch_response = patch_fs(
            State(state.clone()),
            Path("/write/existing.txt".to_string()),
            headers.clone(),
            Json(MetadataPatchRequest {
                mime_type: Some(Some("text/plain".to_string())),
                custom_attrs: BTreeMap::new(),
                remove_custom_attrs: Vec::new(),
            }),
        )
        .await
        .into_response();
        assert_eq!(patch_response.status(), StatusCode::OK);

        let delete_response = delete_fs(
            State(state.clone()),
            Path("/write/delete.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(delete_response.status(), StatusCode::OK);

        let mounted_root = Session::root()
            .with_workspace_mount(workspace_id, "/demo", MAIN_REF, Some(session_ref))
            .unwrap();
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        let (content, _) = reader
            .cat_with_stat_as("/demo/write/new.txt", &mounted_root)
            .await
            .unwrap();
        assert_eq!(content, b"write-only content");
        assert_eq!(
            reader
                .stat_as("/demo/write/existing.txt", &mounted_root)
                .await
                .unwrap()
                .mime_type
                .as_deref(),
            Some("text/plain")
        );
        assert_eq!(
            reader
                .stat_as("/demo/write/new-dir", &mounted_root)
                .await
                .unwrap()
                .kind,
            "directory"
        );
        assert!(matches!(
            reader
                .stat_as("/demo/write/delete.txt", &mounted_root)
                .await,
            Err(VfsError::NotFound { .. })
        ));

        let denied_get = get_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(denied_get.status(), StatusCode::FORBIDDEN);

        let denied_list = get_fs(
            State(state.clone()),
            Path("/write".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(denied_list.status(), StatusCode::FORBIDDEN);

        let denied_stat = get_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            Query(FsQuery {
                stat: Some(true),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(denied_stat.status(), StatusCode::FORBIDDEN);

        let denied_tree = get_tree(State(state), Path("/write".to_string()), headers)
            .await
            .into_response();
        assert_eq!(denied_tree.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn guarded_durable_write_can_overwrite_existing_session_file() {
        let stores = StratumStores::local_memory();
        seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-overwrite/session-001";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores, session_ref).await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let first = put_fs(
            State(state.clone()),
            Path("notes.txt".to_string()),
            with_idempotency_key(headers.clone(), "durable-overwrite-first"),
            Bytes::from_static(b"first"),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);

        let second = put_fs(
            State(state.clone()),
            Path("notes.txt".to_string()),
            with_idempotency_key(headers.clone(), "durable-overwrite-second"),
            Bytes::from_static(b"second"),
        )
        .await
        .into_response();
        assert_eq!(second.status(), StatusCode::OK);

        let read = get_fs(
            State(state),
            Path("notes.txt".to_string()),
            Query(FsQuery::default()),
            headers,
        )
        .await
        .into_response();
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(response_bytes(read).await, Bytes::from_static(b"second"));
    }

    #[tokio::test]
    async fn guarded_durable_write_rejects_symlink_target_without_session_ref_mutation() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let target_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"original target".to_vec(),
        )
        .await;
        let link_id = put_object(&stores, &repo_id, ObjectKind::Blob, b"target.txt".to_vec()).await;
        seed_durable_workspace_base_with_demo_entries(
            &stores,
            0o755,
            vec![
                tree_entry("target.txt", TreeEntryKind::Blob, target_id, 0o644),
                tree_entry("link.txt", TreeEntryKind::Symlink, link_id, 0o777),
            ],
        )
        .await;
        let session_ref = "agent/durable-symlink/session-001";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores.clone(), session_ref).await;

        let response = put_fs(
            State(state),
            Path("link.txt".to_string()),
            workspace_headers(workspace_id, &raw_secret),
            Bytes::from_static(b"replacement"),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .expect("error")
                .contains("durable symlink mutation targets are not supported yet")
        );
        assert!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(session_ref).unwrap())
                .await
                .unwrap()
                .is_none(),
            "rejected symlink write must not materialize the session ref"
        );
    }

    #[tokio::test]
    async fn guarded_durable_copy_idempotency_replay_does_not_require_destination_write() {
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let source_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"copy replay content".to_vec(),
        )
        .await;
        seed_durable_workspace_base_with_demo_entries(
            &stores,
            0o777,
            vec![tree_entry(
                "source.txt",
                TreeEntryKind::Blob,
                source_id,
                0o444,
            )],
        )
        .await;
        let session_ref = "agent/durable-copy-replay/session-001";
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent durable-copy-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("demo", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-copy-replay-token",
                agent.uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let state = guarded_durable_commit_state(db, stores);
        let headers = with_idempotency_key(
            workspace_headers(workspace.id, &issued.raw_secret),
            "durable-copy-readonly-destination-replay",
        );
        let first = post_fs(
            State(state.clone()),
            Path("source.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/copied.txt".to_string()),
                ..Default::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);

        let replay = post_fs(
            State(state),
            Path("source.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/copied.txt".to_string()),
                ..Default::default()
            }),
            headers,
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn guarded_durable_write_respects_committed_parent_mode_bits() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent durable-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let stores = StratumStores::local_memory();
        seed_durable_workspace_base_with_demo_mode(&stores, 0o555).await;
        let session_ref = "agent/durable-permission/session-001";
        let workspace = stores
            .workspace_metadata
            .create_workspace_with_refs("demo", "/demo", MAIN_REF, Some(session_ref))
            .await
            .unwrap();
        let issued = stores
            .workspace_metadata
            .issue_scoped_workspace_token(
                workspace.id,
                "durable-permission-token",
                agent.uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let state = guarded_durable_commit_state(db, stores.clone());
        let response = put_fs(
            State(state),
            Path("blocked.txt".to_string()),
            workspace_headers(workspace.id, &issued.raw_secret),
            Bytes::from_static(b"blocked"),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(
            stores
                .refs
                .get(&RepoId::local(), &RefName::new(session_ref).unwrap())
                .await
                .unwrap()
                .is_none(),
            "permission-denied durable write must not materialize the session ref"
        );
    }

    #[tokio::test]
    async fn guarded_durable_fs_audit_failure_enqueues_recovery_without_body_content() {
        let mut stores = StratumStores::local_memory();
        stores.audit = Arc::new(FailingAuditStore);
        let base_commit = seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-recovery/session-001";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores.clone(), session_ref).await;
        let secret_body = b"durable body must not enter recovery context";
        let headers = with_idempotency_key(
            workspace_headers(workspace_id, &raw_secret),
            "durable-fs-audit-recovery",
        );

        let put_response = put_fs(
            State(state.clone()),
            Path("recover.txt".to_string()),
            headers,
            Bytes::from_static(secret_body),
        )
        .await
        .into_response();

        assert_eq!(put_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(put_response).await;
        assert_eq!(body["mutation_committed"], true);
        assert_eq!(body["audit_recorded"], false);
        assert_eq!(
            body["error"],
            "durable FS mutation side effect failed after mutation"
        );
        assert!(
            !serde_json::to_string(&body)
                .unwrap()
                .contains("private-store-detail")
        );

        let recovery = stores.fs_mutation_recovery.list(10).await.unwrap();
        assert_eq!(recovery.len(), 1);
        assert_eq!(recovery[0].state(), DurableFsMutationRecoveryState::Pending);
        assert_eq!(
            recovery[0].target().workspace_scope(),
            format!("fs:{workspace_id}")
        );
        assert_eq!(recovery[0].target().target_ref(), session_ref);
        assert_eq!(recovery[0].target().previous_commit(), base_commit);
        assert_eq!(recovery[0].target().failed_step().as_str(), "audit_append");

        let session = stores
            .refs
            .get(&RepoId::local(), &RefName::new(session_ref).unwrap())
            .await
            .unwrap()
            .expect("session ref");
        assert_eq!(recovery[0].target().new_commit(), session.target);
        assert_ne!(session.target, base_commit);

        let rendered = format!("{recovery:?}");
        assert!(!rendered.contains("durable body must not enter recovery context"));
        assert!(!rendered.contains("private-store-detail"));
    }

    #[tokio::test]
    async fn guarded_durable_put_mime_recovery_target_uses_single_mutation_output() {
        let mut stores = StratumStores::local_memory();
        stores.audit = Arc::new(FailingAuditStore);
        let base_commit = seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-mime/session-001";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores.clone(), session_ref).await;
        let mut headers = with_idempotency_key(
            workspace_headers(workspace_id, &raw_secret),
            "durable-fs-mime-recovery",
        );
        headers.insert("x-stratum-mime-type", "text/plain".parse().unwrap());

        let put_response = put_fs(
            State(state),
            Path("mime.txt".to_string()),
            headers,
            Bytes::from_static(b"durable mime body"),
        )
        .await
        .into_response();

        assert_eq!(put_response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let recovery = stores.fs_mutation_recovery.list(10).await.unwrap();
        assert_eq!(recovery.len(), 1);
        assert_eq!(recovery[0].target().previous_commit(), base_commit);
        let new_commit = recovery[0].target().new_commit();
        let commit = stores
            .commits
            .get(&RepoId::local(), new_commit)
            .await
            .unwrap()
            .expect("durable MIME mutation commit");
        assert_eq!(commit.parents, vec![base_commit]);
        assert_eq!(commit.changed_paths.len(), 1);
        assert_eq!(commit.changed_paths[0].path, "/demo/mime.txt");
        assert_eq!(
            commit.changed_paths[0]
                .after
                .as_ref()
                .and_then(|record| record.mime_type.as_deref()),
            Some("text/plain")
        );
    }

    #[tokio::test]
    async fn guarded_durable_move_idempotency_replays_after_source_moved() {
        let stores = StratumStores::local_memory();
        seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-move/session-001";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores, session_ref).await;
        let headers = workspace_headers(workspace_id, &raw_secret);
        let write = put_fs(
            State(state.clone()),
            Path("source.txt".to_string()),
            with_idempotency_key(headers.clone(), "durable-move-source"),
            Bytes::from_static(b"durable move"),
        )
        .await
        .into_response();
        assert_eq!(write.status(), StatusCode::OK);

        let move_headers = with_idempotency_key(headers.clone(), "durable-move-replay");
        let first = post_fs(
            State(state.clone()),
            Path("source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            move_headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = post_fs(
            State(state),
            Path("source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            move_headers,
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
    }

    #[tokio::test]
    async fn guarded_durable_mkdir_delete_copy_move_metadata_survive_restart() {
        let stores = StratumStores::local_memory();
        seed_durable_workspace_base(&stores).await;
        let session_ref = "agent/durable-writer/session-ops";
        let (state, workspace_id, raw_secret) =
            durable_workspace_state_with_token(stores.clone(), session_ref).await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let mut mkdir_headers = with_idempotency_key(headers.clone(), "durable-fs-mkdir");
        mkdir_headers.insert("x-stratum-type", "directory".parse().unwrap());
        let mkdir = put_fs(
            State(state.clone()),
            Path("scratch".to_string()),
            mkdir_headers,
            Bytes::new(),
        )
        .await
        .into_response();
        assert_eq!(mkdir.status(), StatusCode::OK);

        let write = put_fs(
            State(state.clone()),
            Path("scratch/source.txt".to_string()),
            with_idempotency_key(headers.clone(), "durable-fs-op-write"),
            Bytes::from_static(b"copy me"),
        )
        .await
        .into_response();
        assert_eq!(write.status(), StatusCode::OK);

        let copy = post_fs(
            State(state.clone()),
            Path("scratch/source.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/scratch/copied.txt".to_string()),
                ..Default::default()
            }),
            with_idempotency_key(headers.clone(), "durable-fs-copy"),
        )
        .await
        .into_response();
        assert_eq!(copy.status(), StatusCode::OK);

        let mv = post_fs(
            State(state.clone()),
            Path("scratch/copied.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/scratch/final.txt".to_string()),
                ..Default::default()
            }),
            with_idempotency_key(headers.clone(), "durable-fs-move"),
        )
        .await
        .into_response();
        assert_eq!(mv.status(), StatusCode::OK);

        let metadata = patch_fs(
            State(state.clone()),
            Path("scratch/final.txt".to_string()),
            with_idempotency_key(headers.clone(), "durable-fs-metadata"),
            Json(MetadataPatchRequest {
                mime_type: Some(Some("text/plain".to_string())),
                custom_attrs: BTreeMap::from([("reviewed".to_string(), "true".to_string())]),
                remove_custom_attrs: Vec::new(),
            }),
        )
        .await
        .into_response();
        assert_eq!(metadata.status(), StatusCode::OK);

        let delete = delete_fs(
            State(state.clone()),
            Path("scratch/source.txt".to_string()),
            Query(FsQuery::default()),
            with_idempotency_key(headers.clone(), "durable-fs-delete"),
        )
        .await
        .into_response();
        assert_eq!(delete.status(), StatusCode::OK);

        let fresh_state = guarded_durable_commit_state(StratumDb::open_memory(), stores);
        let final_read = get_fs(
            State(fresh_state.clone()),
            Path("scratch/final.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(final_read.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(final_read).await,
            Bytes::from_static(b"copy me")
        );

        let stat = get_fs(
            State(fresh_state.clone()),
            Path("scratch/final.txt".to_string()),
            Query(FsQuery {
                stat: Some(true),
                ..Default::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(stat.status(), StatusCode::OK);
        let stat = response_json(stat).await;
        assert_eq!(stat["mime_type"], "text/plain");
        assert_eq!(stat["custom_attrs"]["reviewed"], "true");

        let deleted = get_fs(
            State(fresh_state),
            Path("scratch/source.txt".to_string()),
            Query(FsQuery::default()),
            headers,
        )
        .await
        .into_response();
        assert_eq!(deleted.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_fs_routes_through_local_core_runtime() {
        let state = test_state(StratumDb::open_memory());

        let put_response = put_fs(
            State(state.clone()),
            Path("/core-route.txt".to_string()),
            user_headers("root"),
            Bytes::from_static(b"through-core"),
        )
        .await
        .into_response();
        assert_eq!(put_response.status(), StatusCode::OK);

        let get_response = get_fs(
            State(state),
            Path("/core-route.txt".to_string()),
            Query(FsQuery::default()),
            user_headers("root"),
        )
        .await
        .into_response();
        assert_eq!(get_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(get_response).await,
            Bytes::from_static(b"through-core")
        );
    }

    #[tokio::test]
    async fn put_fs_emits_audit_event_without_body_content() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let secret_body = "body-content-must-not-enter-audit";

        let response = put_fs(
            State(state.clone()),
            Path("/audit.txt".to_string()),
            user_headers("root"),
            Bytes::from(secret_body),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::FsWriteFile);
        assert_eq!(events[0].resource.path.as_deref(), Some("/audit.txt"));
        assert_eq!(
            events[0].details.get("project_path").map(String::as_str),
            Some("/audit.txt")
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(!audit_json.contains(secret_body));
    }

    #[tokio::test]
    async fn put_fs_idempotency_replays_without_second_audit_event() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let headers = with_idempotency_key(user_headers("root"), "fs-put-replay");

        let first = put_fs(
            State(state.clone()),
            Path("/replay.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"same"),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        assert!(first.headers().get("x-stratum-idempotent-replay").is_none());
        let first_body = response_json(first).await;

        let replay = put_fs(
            State(state.clone()),
            Path("/replay.txt".to_string()),
            headers,
            Bytes::from_static(b"same"),
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, crate::audit::AuditAction::FsWriteFile);
    }

    #[tokio::test]
    async fn put_fs_same_idempotency_key_with_different_body_conflicts_without_overwrite() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let headers = with_idempotency_key(user_headers("root"), "fs-put-conflict");

        let first = put_fs(
            State(state.clone()),
            Path("/conflict.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"first"),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);

        let conflict = put_fs(
            State(state.clone()),
            Path("/conflict.txt".to_string()),
            headers,
            Bytes::from_static(b"second"),
        )
        .await
        .into_response();

        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert_eq!(
            state
                .db
                .cat_as("/conflict.txt", &Session::root())
                .await
                .unwrap(),
            b"first".to_vec()
        );
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn put_fs_mime_header_updates_stat_and_raw_content_type() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        let mut headers = user_headers("root");
        headers.insert("x-stratum-mime-type", "text/plain".parse().unwrap());

        let put = put_fs(
            State(state.clone()),
            Path("/mime.txt".to_string()),
            headers,
            Bytes::from_static(b"hello"),
        )
        .await
        .into_response();
        assert_eq!(put.status(), StatusCode::OK);

        let stat = get_fs(
            State(state.clone()),
            Path("/mime.txt".to_string()),
            Query(FsQuery {
                stat: Some(true),
                ..FsQuery::default()
            }),
            user_headers("root"),
        )
        .await
        .into_response();
        assert_eq!(stat.status(), StatusCode::OK);
        let stat = response_json(stat).await;
        assert_eq!(
            stat.get("mime_type"),
            Some(&serde_json::json!("text/plain"))
        );
        assert_eq!(
            stat.get("content_hash"),
            Some(&serde_json::json!(format!(
                "sha256:{}",
                sha256_hex(b"hello")
            )))
        );
        assert_eq!(stat.get("custom_attrs"), Some(&serde_json::json!({})));

        let raw = get_fs(
            State(state.clone()),
            Path("/mime.txt".to_string()),
            Query(FsQuery::default()),
            user_headers("root"),
        )
        .await
        .into_response();
        assert_eq!(raw.status(), StatusCode::OK);
        assert_eq!(raw.headers().get("content-type").unwrap(), "text/plain");
        assert_eq!(response_bytes(raw).await, Bytes::from_static(b"hello"));
    }

    #[test]
    fn metadata_patch_request_distinguishes_missing_and_null_mime_type() {
        let missing: MetadataPatchRequest = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(missing.mime_type, None);

        let clear: MetadataPatchRequest =
            serde_json::from_value(serde_json::json!({"mime_type": null})).unwrap();
        assert_eq!(clear.mime_type, Some(None));

        let set: MetadataPatchRequest =
            serde_json::from_value(serde_json::json!({"mime_type": "text/plain"})).unwrap();
        assert_eq!(set.mime_type, Some(Some("text/plain".to_string())));
    }

    #[tokio::test]
    async fn raw_get_uses_symlink_target_mime_type() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        state
            .db
            .write_file_as("/target.txt", b"target".to_vec(), &Session::root())
            .await
            .unwrap();
        state
            .db
            .ln_s("/target.txt", "/link.txt", 0, 0)
            .await
            .unwrap();
        state
            .db
            .set_metadata_as(
                "/link.txt",
                MetadataUpdate {
                    mime_type: Some(Some("text/plain".to_string())),
                    ..MetadataUpdate::default()
                },
                &Session::root(),
            )
            .await
            .unwrap();

        let raw = get_fs(
            State(state.clone()),
            Path("/link.txt".to_string()),
            Query(FsQuery::default()),
            user_headers("root"),
        )
        .await
        .into_response();

        assert_eq!(raw.status(), StatusCode::OK);
        assert_eq!(raw.headers().get("content-type").unwrap(), "text/plain");
        assert_eq!(response_bytes(raw).await, Bytes::from_static(b"target"));
    }

    #[tokio::test]
    async fn patch_fs_metadata_is_idempotent_and_audited_without_attr_values() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        state
            .db
            .write_file_as("/metadata.txt", b"hello".to_vec(), &Session::root())
            .await
            .unwrap();
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert("owner".to_string(), "docs".to_string());
        let headers = with_idempotency_key(user_headers("root"), "fs-metadata-replay");

        let first = patch_fs(
            State(state.clone()),
            Path("/metadata.txt".to_string()),
            headers.clone(),
            Json(MetadataPatchRequest {
                mime_type: Some(Some("text/plain".to_string())),
                custom_attrs: attrs.clone(),
                remove_custom_attrs: Vec::new(),
            }),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;
        assert_eq!(
            first_body.get("custom_attr_keys"),
            Some(&serde_json::json!(["owner"]))
        );
        assert!(!serde_json::to_string(&first_body).unwrap().contains("docs"));

        let replay = patch_fs(
            State(state.clone()),
            Path("/metadata.txt".to_string()),
            headers,
            Json(MetadataPatchRequest {
                mime_type: Some(Some("text/plain".to_string())),
                custom_attrs: attrs,
                remove_custom_attrs: Vec::new(),
            }),
        )
        .await
        .into_response();
        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);

        let stat = state
            .db
            .stat_as("/metadata.txt", &Session::root())
            .await
            .unwrap();
        assert_eq!(stat.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(
            stat.custom_attrs.get("owner").map(String::as_str),
            Some("docs")
        );

        let events = state.audit.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].action,
            crate::audit::AuditAction::FsMetadataUpdate
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        assert!(audit_json.contains("owner"));
        assert!(!audit_json.contains("docs"));
    }

    #[tokio::test]
    async fn protected_path_rules_block_direct_http_writes() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/legal", &root).await.unwrap();
        db.mkdir_p_as("/demo/legalese", &root).await.unwrap();
        db.mkdir_p_as("/demo/open", &root).await.unwrap();
        db.mkdir_p_as("/demo/parent/legal", &root).await.unwrap();
        db.execute_command("chmod 777 /demo", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/legal", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/legalese", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/open", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/parent", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/parent/legal", &mut root)
            .await
            .unwrap();
        db.write_file_as("/demo/legal/existing.txt", b"legal".to_vec(), &root)
            .await
            .unwrap();
        db.ln_s(
            "/demo/legal/existing.txt",
            "/demo/open/legal-link.txt",
            ROOT_UID,
            ROOT_GID,
        )
        .await
        .unwrap();
        db.write_file_as(
            "/demo/parent/legal/child.txt",
            b"protected child".to_vec(),
            &root,
        )
        .await
        .unwrap();
        db.write_file_as("/demo/open/source.txt", b"source".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/demo/open/move-source.txt", b"move".to_vec(), &root)
            .await
            .unwrap();
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent.uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        state
            .review
            .create_protected_path_rule("/demo/legal", Some(crate::vcs::MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();
        state
            .review
            .create_protected_path_rule(
                "/demo/parent/legal",
                Some(crate::vcs::MAIN_REF),
                1,
                ROOT_UID,
            )
            .await
            .unwrap();

        let blocked_write = put_fs(
            State(state.clone()),
            Path("/legal/new.txt".to_string()),
            with_idempotency_key(
                workspace_headers(workspace_id, &raw_secret),
                "protected-path-write",
            ),
            Bytes::from_static(b"blocked"),
        )
        .await
        .into_response();
        assert_projected_error(blocked_write, StatusCode::FORBIDDEN, "/legal/new.txt").await;
        assert!(
            state
                .db
                .cat_as("/demo/legal/new.txt", &Session::root())
                .await
                .is_err()
        );

        let mut mkdir_headers = workspace_headers(workspace_id, &raw_secret);
        mkdir_headers.insert("x-stratum-type", "directory".parse().unwrap());
        let blocked_mkdir = put_fs(
            State(state.clone()),
            Path("/legal/new-dir".to_string()),
            mkdir_headers,
            Bytes::new(),
        )
        .await
        .into_response();
        assert_projected_error(blocked_mkdir, StatusCode::FORBIDDEN, "/legal/new-dir").await;

        let blocked_metadata = patch_fs(
            State(state.clone()),
            Path("/legal/existing.txt".to_string()),
            workspace_headers(workspace_id, &raw_secret),
            Json(MetadataPatchRequest {
                mime_type: Some(Some("text/plain".to_string())),
                custom_attrs: BTreeMap::new(),
                remove_custom_attrs: Vec::new(),
            }),
        )
        .await
        .into_response();
        assert_projected_error(
            blocked_metadata,
            StatusCode::FORBIDDEN,
            "/legal/existing.txt",
        )
        .await;

        let blocked_delete = delete_fs(
            State(state.clone()),
            Path("/legal/existing.txt".to_string()),
            Query(FsQuery::default()),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_projected_error(blocked_delete, StatusCode::FORBIDDEN, "/legal/existing.txt").await;
        assert_eq!(
            state
                .db
                .cat_as("/demo/legal/existing.txt", &Session::root())
                .await
                .unwrap(),
            b"legal".to_vec()
        );

        let blocked_symlink_write = put_fs(
            State(state.clone()),
            Path("/open/legal-link.txt".to_string()),
            workspace_headers(workspace_id, &raw_secret),
            Bytes::from_static(b"bypass"),
        )
        .await
        .into_response();
        assert_projected_error(
            blocked_symlink_write,
            StatusCode::FORBIDDEN,
            "/legal/existing.txt",
        )
        .await;
        assert_eq!(
            state
                .db
                .cat_as("/demo/legal/existing.txt", &Session::root())
                .await
                .unwrap(),
            b"legal".to_vec()
        );

        let blocked_symlink_metadata = patch_fs(
            State(state.clone()),
            Path("/open/legal-link.txt".to_string()),
            workspace_headers(workspace_id, &raw_secret),
            Json(MetadataPatchRequest {
                mime_type: Some(Some("text/plain".to_string())),
                custom_attrs: BTreeMap::new(),
                remove_custom_attrs: Vec::new(),
            }),
        )
        .await
        .into_response();
        assert_projected_error(
            blocked_symlink_metadata,
            StatusCode::FORBIDDEN,
            "/legal/existing.txt",
        )
        .await;

        let blocked_copy_destination = post_fs(
            State(state.clone()),
            Path("/open/source.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/legal/copied.txt".to_string()),
                ..FsQuery::default()
            }),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_projected_error(
            blocked_copy_destination,
            StatusCode::FORBIDDEN,
            "/legal/copied.txt",
        )
        .await;

        let allowed_protected_copy_source = post_fs(
            State(state.clone()),
            Path("/legal/existing.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/open/copied-from-legal.txt".to_string()),
                ..FsQuery::default()
            }),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_eq!(allowed_protected_copy_source.status(), StatusCode::OK);
        assert_eq!(
            state
                .db
                .cat_as("/demo/open/copied-from-legal.txt", &Session::root())
                .await
                .unwrap(),
            b"legal".to_vec()
        );

        let blocked_move_source = post_fs(
            State(state.clone()),
            Path("/legal/existing.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/open/moved-from-legal.txt".to_string()),
                ..FsQuery::default()
            }),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_projected_error(
            blocked_move_source,
            StatusCode::FORBIDDEN,
            "/legal/existing.txt",
        )
        .await;

        let blocked_move_destination = post_fs(
            State(state.clone()),
            Path("/open/move-source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/legal/moved.txt".to_string()),
                ..FsQuery::default()
            }),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_projected_error(
            blocked_move_destination,
            StatusCode::FORBIDDEN,
            "/legal/moved.txt",
        )
        .await;
        assert_eq!(
            state
                .db
                .cat_as("/demo/open/move-source.txt", &Session::root())
                .await
                .unwrap(),
            b"move".to_vec()
        );

        let blocked_parent_delete = delete_fs(
            State(state.clone()),
            Path("/parent".to_string()),
            Query(FsQuery {
                recursive: Some(true),
                ..FsQuery::default()
            }),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_projected_error(blocked_parent_delete, StatusCode::FORBIDDEN, "/parent").await;
        assert_eq!(
            state
                .db
                .cat_as("/demo/parent/legal/child.txt", &Session::root())
                .await
                .unwrap(),
            b"protected child".to_vec()
        );

        let blocked_parent_move = post_fs(
            State(state.clone()),
            Path("/parent".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/open/parent-moved".to_string()),
                ..FsQuery::default()
            }),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_projected_error(blocked_parent_move, StatusCode::FORBIDDEN, "/parent").await;
        assert_eq!(
            state
                .db
                .cat_as("/demo/parent/legal/child.txt", &Session::root())
                .await
                .unwrap(),
            b"protected child".to_vec()
        );

        let legalese_write = put_fs(
            State(state.clone()),
            Path("/legalese/allowed.txt".to_string()),
            workspace_headers(workspace_id, &raw_secret),
            Bytes::from_static(b"allowed"),
        )
        .await
        .into_response();
        assert_eq!(legalese_write.status(), StatusCode::OK);
        assert_eq!(
            state
                .db
                .cat_as("/demo/legalese/allowed.txt", &Session::root())
                .await
                .unwrap(),
            b"allowed".to_vec()
        );
    }

    #[tokio::test]
    async fn delete_fs_idempotency_replays_deleted_response() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        state
            .db
            .write_file_as("/delete.txt", b"gone".to_vec(), &Session::root())
            .await
            .unwrap();
        let headers = with_idempotency_key(user_headers("root"), "fs-delete-replay");

        let first = delete_fs(
            State(state.clone()),
            Path("/delete.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = delete_fs(
            State(state.clone()),
            Path("/delete.txt".to_string()),
            Query(FsQuery::default()),
            headers,
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn move_fs_idempotency_replays_moved_response() {
        let db = StratumDb::open_memory();
        let state = test_state(db);
        state
            .db
            .write_file_as("/source.txt", b"moved".to_vec(), &Session::root())
            .await
            .unwrap();
        let headers = with_idempotency_key(user_headers("root"), "fs-move-replay");
        let first = post_fs(
            State(state.clone()),
            Path("/source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = post_fs(
            State(state.clone()),
            Path("/source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            headers,
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn copy_fs_idempotency_replays_when_destination_file_is_not_writable() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser alice", &mut root)
            .await
            .unwrap();
        db.mkdir_p_as("/shared", &root).await.unwrap();
        db.execute_command("chmod 777 /shared", &mut root)
            .await
            .unwrap();
        let alice = db.login("alice").await.unwrap();
        db.write_file_as("/shared/source.txt", b"copied".to_vec(), &alice)
            .await
            .unwrap();
        db.execute_command("chmod 444 /shared/source.txt", &mut root)
            .await
            .unwrap();
        let state = test_state(db);
        let headers = with_idempotency_key(user_headers("alice"), "fs-copy-replay-readonly-dst");

        let first = post_fs(
            State(state.clone()),
            Path("/shared/source.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/shared/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = post_fs(
            State(state.clone()),
            Path("/shared/source.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/shared/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            headers,
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
        assert_eq!(
            state
                .db
                .cat_as("/shared/dest.txt", &Session::root())
                .await
                .unwrap(),
            b"copied".to_vec()
        );
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn move_fs_idempotency_replays_when_moved_file_is_not_writable() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser alice", &mut root)
            .await
            .unwrap();
        db.mkdir_p_as("/shared", &root).await.unwrap();
        db.execute_command("chmod 777 /shared", &mut root)
            .await
            .unwrap();
        let alice = db.login("alice").await.unwrap();
        db.write_file_as("/shared/source.txt", b"moved".to_vec(), &alice)
            .await
            .unwrap();
        db.execute_command("chmod 444 /shared/source.txt", &mut root)
            .await
            .unwrap();
        let state = test_state(db);
        let headers = with_idempotency_key(user_headers("alice"), "fs-move-replay-readonly-dst");

        let first = post_fs(
            State(state.clone()),
            Path("/shared/source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/shared/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = response_json(first).await;

        let replay = post_fs(
            State(state.clone()),
            Path("/shared/source.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/shared/dest.txt".to_string()),
                ..FsQuery::default()
            }),
            headers,
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay.headers().get("x-stratum-idempotent-replay"),
            Some(&"true".parse().unwrap())
        );
        assert_eq!(response_json(replay).await, first_body);
        assert_eq!(
            state
                .db
                .cat_as("/shared/dest.txt", &Session::root())
                .await
                .unwrap(),
            b"moved".to_vec()
        );
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn put_fs_idempotency_replay_requires_current_write_scope() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/read", &root).await.unwrap();
        db.mkdir_p_as("/demo/write", &root).await.unwrap();
        db.execute_command("chmod 777 /demo/write", &mut root)
            .await
            .unwrap();

        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let write_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "writer",
                agent.uid,
                vec!["/demo/write".to_string()],
                vec!["/demo/write".to_string()],
            )
            .await
            .unwrap();
        let read_only_token = store
            .issue_scoped_workspace_token(
                workspace.id,
                "reader",
                agent.uid,
                vec!["/demo/write".to_string()],
                Vec::new(),
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let key = "fs-put-replay-scope";

        let first = put_fs(
            State(state.clone()),
            Path("/write/scoped.txt".to_string()),
            with_idempotency_key(
                workspace_headers(workspace.id, &write_token.raw_secret),
                key,
            ),
            Bytes::from_static(b"scoped"),
        )
        .await
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);

        let replay = put_fs(
            State(state.clone()),
            Path("/write/scoped.txt".to_string()),
            with_idempotency_key(
                workspace_headers(workspace.id, &read_only_token.raw_secret),
                key,
            ),
            Bytes::from_static(b"scoped"),
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
        assert_eq!(state.audit.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_file_denies_authenticated_user_without_read_permission() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch secret.md", &mut root)
            .await
            .unwrap();
        db.execute_command("write secret.md classified", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 600 secret.md", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let response = get_fs(
            State(test_state(db)),
            Path("/secret.md".to_string()),
            Query(FsQuery::default()),
            user_headers("bob"),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn workspace_bearer_uses_workspace_relative_paths_for_fs_search_find_and_tree() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/read", &root).await.unwrap();
        db.mkdir_p_as("/demo/search", &root).await.unwrap();
        db.mkdir_p_as("/demo/write", &root).await.unwrap();
        db.write_file_as("/demo/read/allowed.txt", b"readable needle".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/demo/search/hit.txt", b"needle\nsecond".to_vec(), &root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/write", &mut root)
            .await
            .unwrap();

        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent.uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let read_response = get_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(read_response).await,
            Bytes::from_static(b"readable needle")
        );

        let root_list = get_fs_root(State(state.clone()), headers.clone())
            .await
            .into_response();
        assert_eq!(root_list.status(), StatusCode::OK);
        let root_list = response_json(root_list).await;
        assert_eq!(root_list.get("path"), Some(&serde_json::json!("/")));
        assert!(
            root_list["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry.get("name") == Some(&serde_json::json!("read")))
        );

        let read_list = get_fs(
            State(state.clone()),
            Path("/read".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_list.status(), StatusCode::OK);
        let read_list = response_json(read_list).await;
        assert_eq!(read_list.get("path"), Some(&serde_json::json!("/read")));

        let write_response = put_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"written"),
        )
        .await
        .into_response();
        assert_eq!(write_response.status(), StatusCode::OK);
        let write_response = response_json(write_response).await;
        assert_eq!(
            write_response.get("written"),
            Some(&serde_json::json!("/write/new.txt"))
        );

        let mkdir_response = put_fs(
            State(state.clone()),
            Path("/write/nested".to_string()),
            {
                let mut headers = headers.clone();
                headers.insert("x-stratum-type", "directory".parse().unwrap());
                headers
            },
            Bytes::new(),
        )
        .await
        .into_response();
        assert_eq!(mkdir_response.status(), StatusCode::OK);
        let mkdir_response = response_json(mkdir_response).await;
        assert_eq!(
            mkdir_response.get("created"),
            Some(&serde_json::json!("/write/nested"))
        );

        let copy_response = post_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/write/copied.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(copy_response.status(), StatusCode::OK);
        let copy_response = response_json(copy_response).await;
        assert_eq!(
            copy_response.get("copied"),
            Some(&serde_json::json!("/read/allowed.txt"))
        );
        assert_eq!(
            copy_response.get("to"),
            Some(&serde_json::json!("/write/copied.txt"))
        );

        let move_response = post_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/write/moved.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(move_response.status(), StatusCode::OK);
        let move_response = response_json(move_response).await;
        assert_eq!(
            move_response.get("moved"),
            Some(&serde_json::json!("/write/new.txt"))
        );
        assert_eq!(
            move_response.get("to"),
            Some(&serde_json::json!("/write/moved.txt"))
        );

        let delete_response = delete_fs(
            State(state.clone()),
            Path("/write/copied.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(delete_response.status(), StatusCode::OK);
        let delete_response = response_json(delete_response).await;
        assert_eq!(
            delete_response.get("deleted"),
            Some(&serde_json::json!("/write/copied.txt"))
        );

        let grep_response = search_grep(
            State(state.clone()),
            Query(SearchQuery {
                pattern: Some("needle".to_string()),
                path: None,
                name: None,
                recursive: None,
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(grep_response.status(), StatusCode::OK);
        let grep_response = response_json(grep_response).await;
        let grep_files: Vec<_> = grep_response["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result["file"].as_str().unwrap())
            .collect();
        assert!(grep_files.contains(&"/read/allowed.txt"));
        assert!(grep_files.contains(&"/search/hit.txt"));
        assert!(!grep_files.iter().any(|file| file.starts_with("/demo/")));

        let find_response = search_find(
            State(state.clone()),
            Query(SearchQuery {
                pattern: None,
                path: None,
                name: Some("*.txt".to_string()),
                recursive: None,
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(find_response.status(), StatusCode::OK);
        let find_response = response_json(find_response).await;
        let find_results: Vec<_> = find_response["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result.as_str().unwrap())
            .collect();
        assert!(find_results.contains(&"/read/allowed.txt"));
        assert!(find_results.contains(&"/search/hit.txt"));
        assert!(!find_results.iter().any(|path| path.starts_with("/demo/")));

        let tree_response = get_tree_root(State(state), headers).await.into_response();
        assert_eq!(tree_response.status(), StatusCode::OK);
        let tree_response =
            String::from_utf8(response_bytes(tree_response).await.to_vec()).unwrap();
        assert!(tree_response.starts_with(".\n"));
        assert!(tree_response.contains("read/"));
        assert!(tree_response.contains("allowed.txt"));
        assert!(!tree_response.contains("demo/"));
    }

    #[tokio::test]
    async fn workspace_bearer_reads_and_writes_only_inside_token_prefixes() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/read", &root).await.unwrap();
        db.mkdir_p_as("/demo/write", &root).await.unwrap();
        db.mkdir_p_as("/demo/outside", &root).await.unwrap();
        db.mkdir_p_as("/outside", &root).await.unwrap();
        db.write_file_as("/demo/read/allowed.txt", b"readable".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/demo/outside/secret.txt", b"secret".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/outside/secret.txt", b"escaped".to_vec(), &root)
            .await
            .unwrap();
        db.ln_s(
            "/outside/secret.txt",
            "/demo/read/outside-link.txt",
            root.uid,
            root.gid,
        )
        .await
        .unwrap();
        db.execute_command("chmod 777 /demo/write", &mut root)
            .await
            .unwrap();

        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent.uid,
            vec!["/demo/read".to_string()],
            vec!["/demo/write".to_string()],
        )
        .await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let read_allowed = get_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_allowed.status(), StatusCode::OK);

        let external_symlink_denied = get_fs(
            State(state.clone()),
            Path("/read/outside-link.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_redacted_external_error(external_symlink_denied).await;

        let traversal_clamped_inside_mount = get_fs(
            State(state.clone()),
            Path("/../read/allowed.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(traversal_clamped_inside_mount.status(), StatusCode::OK);

        let read_denied = get_fs(
            State(state.clone()),
            Path("/outside/secret.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(read_denied, StatusCode::FORBIDDEN, "/outside/secret.txt").await;

        let traversal_denied = get_fs(
            State(state.clone()),
            Path("/../outside/secret.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(
            traversal_denied,
            StatusCode::FORBIDDEN,
            "/outside/secret.txt",
        )
        .await;

        let write_allowed = put_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"written"),
        )
        .await
        .into_response();
        assert_eq!(write_allowed.status(), StatusCode::OK);

        let copy_denied = post_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/outside/copied.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(copy_denied, StatusCode::FORBIDDEN, "/outside/copied.txt").await;

        let move_denied = post_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/outside/moved.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(move_denied, StatusCode::FORBIDDEN, "/outside/moved.txt").await;

        let search_denied = search_grep(
            State(state.clone()),
            Query(SearchQuery {
                pattern: Some("readable".to_string()),
                path: None,
                name: None,
                recursive: None,
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(search_denied, StatusCode::FORBIDDEN, "/").await;

        let write_denied = put_fs(
            State(state),
            Path("/outside/new.txt".to_string()),
            headers,
            Bytes::from_static(b"blocked"),
        )
        .await
        .into_response();
        assert_projected_error(write_denied, StatusCode::FORBIDDEN, "/outside/new.txt").await;
    }
}
