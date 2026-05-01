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
use super::idempotency as http_idempotency;
use super::middleware::session_from_headers;
use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::error::VfsError;
use crate::fs::{MetadataUpdate, validate_mime_type};
use crate::idempotency::{
    IdempotencyBegin, IdempotencyKey, IdempotencyReservation, request_fingerprint,
};

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

async fn complete_idempotent_json_response(
    state: &AppState,
    session: &Session,
    reservation: Option<IdempotencyReservation>,
    status: StatusCode,
    body: serde_json::Value,
) -> axum::response::Response {
    if let Some(reservation) = reservation
        && let Err(e) = state
            .idempotency
            .complete(&reservation, status.as_u16(), body.clone())
            .await
    {
        return err_json_for(session, &e, StatusCode::INTERNAL_SERVER_ERROR);
    }
    (status, Json(body)).into_response()
}

async fn abort_idempotency(state: &AppState, reservation: Option<IdempotencyReservation>) {
    if let Some(reservation) = reservation {
        state.idempotency.abort(&reservation).await;
    }
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
        state.db.check_mkdir_p_as(path, session).await
    } else {
        state.db.check_write_file_as(path, session).await
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

    if let Err(e) = state.db.check_set_metadata_as(path, session).await {
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

    if let Err(e) = state.db.check_rm_as(path, recursive, session).await {
        match e {
            VfsError::NotFound { .. } => {
                if let Err(e) = state.db.check_write_file_as(path, session).await {
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
        state.db.check_cp_replay_as(src, dst, session).await
    } else {
        state.db.check_mv_replay_as(src, dst, session).await
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
            state.db.check_cp_as(src, dst, session).await
        } else {
            state.db.check_mv_as(src, dst, session).await
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

    match state.db.ls_as(Some(&path), &session).await {
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
        return match state.db.stat_as(&path, &session).await {
            Ok(info) => Json(stat_to_json(&info)).into_response(),
            Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
        };
    }

    match state.db.cat_with_stat_as(&path, &session).await {
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
            match state.db.ls_as(Some(&path), &session).await {
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

    if is_dir {
        match state.db.mkdir_p_as(&path, &session).await {
            Ok(()) => {
                let project_path = session.project_mounted_path(&path);
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
                    let (status, body) = response;
                    return complete_idempotent_json_response(
                        &state,
                        &session,
                        reservation,
                        status,
                        body,
                    )
                    .await;
                }
                let body = serde_json::json!({
                    "created": project_path,
                    "type": "directory"
                });
                complete_idempotent_json_response(
                    &state,
                    &session,
                    reservation,
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
        match state.db.write_file_as(&path, body.to_vec(), &session).await {
            Ok(()) => {
                if let Some(mime_type) = mime_type {
                    let update = MetadataUpdate {
                        mime_type: Some(Some(mime_type)),
                        ..MetadataUpdate::default()
                    };
                    if let Err(e) = state.db.set_metadata_as(&path, update, &session).await {
                        let body = serde_json::json!({
                            "error": format!("metadata update failed after write: {e}"),
                            "mutation_committed": true,
                        });
                        return complete_idempotent_json_response(
                            &state,
                            &session,
                            reservation,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            body,
                        )
                        .await;
                    }
                }
                let project_path = session.project_mounted_path(&path);
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
                    let (status, body) = response;
                    return complete_idempotent_json_response(
                        &state,
                        &session,
                        reservation,
                        status,
                        body,
                    )
                    .await;
                }
                let body = serde_json::json!({
                    "written": project_path,
                    "size": size
                });
                complete_idempotent_json_response(
                    &state,
                    &session,
                    reservation,
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

    let reservation =
        match begin_metadata_idempotency(&state, &session, &headers, &path, &request).await {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };

    let update = MetadataUpdate::from(request);
    match state.db.set_metadata_as(&path, update, &session).await {
        Ok(result) => {
            let project_path = session.project_mounted_path(&path);
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
                let (status, body) = response;
                return complete_idempotent_json_response(
                    &state,
                    &session,
                    reservation,
                    status,
                    body,
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
            complete_idempotent_json_response(&state, &session, reservation, StatusCode::OK, body)
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
    let reservation =
        match begin_delete_idempotency(&state, &session, &headers, &path, recursive).await {
            Ok(reservation) => reservation,
            Err(response) => return response,
        };
    let result = state.db.rm_as(&path, recursive, &session).await;

    match result {
        Ok(()) => {
            let project_path = session.project_mounted_path(&path);
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
                let (status, body) = response;
                return complete_idempotent_json_response(
                    &state,
                    &session,
                    reservation,
                    status,
                    body,
                )
                .await;
            }
            let body = serde_json::json!({
                "deleted": project_path
            });
            complete_idempotent_json_response(&state, &session, reservation, StatusCode::OK, body)
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
            let reservation =
                match begin_copy_move_idempotency(&state, &session, &headers, &path, &dst, "copy")
                    .await
                {
                    Ok(reservation) => reservation,
                    Err(response) => return response,
                };
            match state.db.cp_as(&path, &dst, &session).await {
                Ok(()) => {
                    let project_path = session.project_mounted_path(&path);
                    let dst_project_path = session.project_mounted_path(&dst);
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
                        let (status, body) = response;
                        return complete_idempotent_json_response(
                            &state,
                            &session,
                            reservation,
                            status,
                            body,
                        )
                        .await;
                    }
                    let body = serde_json::json!({
                        "copied": project_path,
                        "to": dst_project_path
                    });
                    complete_idempotent_json_response(
                        &state,
                        &session,
                        reservation,
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
            let reservation =
                match begin_copy_move_idempotency(&state, &session, &headers, &path, &dst, "move")
                    .await
                {
                    Ok(reservation) => reservation,
                    Err(response) => return response,
                };
            match state.db.mv_as(&path, &dst, &session).await {
                Ok(()) => {
                    let project_path = session.project_mounted_path(&path);
                    let dst_project_path = session.project_mounted_path(&dst);
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
                        let (status, body) = response;
                        return complete_idempotent_json_response(
                            &state,
                            &session,
                            reservation,
                            status,
                            body,
                        )
                        .await;
                    }
                    let body = serde_json::json!({
                        "moved": project_path,
                        "to": dst_project_path
                    });
                    complete_idempotent_json_response(
                        &state,
                        &session,
                        reservation,
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
        .db
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

    match state.db.find_as(Some(&path), name, &session).await {
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
    match state.db.tree_as(Some(&path), &session).await {
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
    match state.db.tree_as(Some(&path), &session).await {
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
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
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
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
        });
        (state, workspace.id, issued.raw_secret)
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
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
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
