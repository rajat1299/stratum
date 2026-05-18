use axum::Json;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::audit::{AuditAction, AuditOutcome, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::session::Session;
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyKey, IdempotencyRecord, IdempotencyReplayClassification, IdempotencyReservation,
    IdempotencyStore, SecretReplayMetadata,
};
use crate::server::AppState;

pub const IDEMPOTENCY_REPLAY_HEADER: &str = "x-stratum-idempotent-replay";
pub const IDEMPOTENCY_REPLAY_HEADER_VALUE: &str = "true";
pub const IDEMPOTENCY_CONFLICT_MESSAGE: &str =
    "Idempotency-Key was reused with a different request";
pub const IDEMPOTENCY_IN_PROGRESS_MESSAGE: &str = "Idempotency-Key request is already in progress";
pub const IDEMPOTENCY_SECRET_BEARING_MESSAGE: &str = "idempotency response is not replayable";
pub const IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE: &str = "idempotency quota exceeded";

pub fn idempotency_key_from_headers(
    headers: &HeaderMap,
) -> Result<Option<IdempotencyKey>, VfsError> {
    let mut values = headers.get_all("idempotency-key").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(VfsError::InvalidArgs {
            message: "Idempotency-Key must be provided at most once".to_string(),
        });
    }

    Ok(Some(IdempotencyKey::parse_header_value(value)?))
}

pub fn idempotency_conflict_response() -> axum::response::Response {
    idempotency_error_response(StatusCode::CONFLICT, IDEMPOTENCY_CONFLICT_MESSAGE)
}

pub fn idempotency_in_progress_response() -> axum::response::Response {
    idempotency_error_response(StatusCode::CONFLICT, IDEMPOTENCY_IN_PROGRESS_MESSAGE)
}

pub fn idempotency_secret_bearing_response() -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "error": IDEMPOTENCY_SECRET_BEARING_MESSAGE,
            "idempotency_recorded": false,
            "replayable": false,
        })),
    )
        .into_response()
}

pub fn idempotency_quota_response() -> axum::response::Response {
    idempotency_quota_response_with_audit_status(None)
}

fn idempotency_quota_response_with_audit_status(
    audit_recorded: Option<bool>,
) -> axum::response::Response {
    let mut body = serde_json::json!({
        "error": IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE,
        "quota": "scope",
    });
    if let Some(audit_recorded) = audit_recorded {
        body["audit_recorded"] = serde_json::Value::Bool(audit_recorded);
    }
    (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response()
}

pub fn idempotency_quota_response_if_quota_error(
    error: &VfsError,
) -> Option<axum::response::Response> {
    match error {
        VfsError::InvalidArgs { message } if message == IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE => {
            Some(idempotency_quota_response())
        }
        _ => None,
    }
}

pub async fn idempotency_quota_response_if_quota_error_with_audit(
    state: &AppState,
    session: &Session,
    route_family: &'static str,
    error: &VfsError,
) -> Option<axum::response::Response> {
    idempotency_quota_response_if_quota_error(error)?;
    let audit_recorded = append_idempotency_quota_audit(state, session, route_family).await;
    Some(idempotency_quota_response_with_audit_status(Some(
        audit_recorded,
    )))
}

async fn append_idempotency_quota_audit(
    state: &AppState,
    session: &Session,
    route_family: &'static str,
) -> bool {
    let event = NewAuditEvent::from_session(
        session,
        AuditAction::IdempotencyQuotaExceeded,
        AuditResource::id(AuditResourceKind::Idempotency, "quota"),
    )
    .with_outcome(AuditOutcome::Partial)
    .with_detail("route_family", route_family)
    .with_detail("quota_kind", "scope")
    .with_detail("has_workspace", session.mount().is_some())
    .with_detail("has_delegate", session.delegate.is_some());
    state.audit.append(event).await.is_ok()
}

pub fn idempotency_json_replay_response(record: IdempotencyRecord) -> axum::response::Response {
    if record.classification == IdempotencyReplayClassification::SecretBearing {
        return idempotency_secret_bearing_response();
    }
    let status =
        StatusCode::from_u16(record.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        [(IDEMPOTENCY_REPLAY_HEADER, IDEMPOTENCY_REPLAY_HEADER_VALUE)],
        Json(record.response_body),
    )
        .into_response()
}

pub async fn persist_encrypted_secret_replay(
    store: &dyn IdempotencyStore,
    reservation: &IdempotencyReservation,
    status: StatusCode,
    encrypted_envelope_body: serde_json::Value,
    metadata: SecretReplayMetadata,
) -> Result<(), VfsError> {
    store
        .complete_with_encrypted_secret_replay(
            reservation,
            status.as_u16(),
            encrypted_envelope_body,
            metadata,
        )
        .await
}

pub fn secret_free() -> IdempotencyReplayClassification {
    IdempotencyReplayClassification::SecretFree
}

pub fn partial() -> IdempotencyReplayClassification {
    IdempotencyReplayClassification::Partial
}

pub fn secret_bearing() -> IdempotencyReplayClassification {
    IdempotencyReplayClassification::SecretBearing
}

pub async fn complete_with_classification(
    store: &dyn IdempotencyStore,
    reservation: &IdempotencyReservation,
    status: StatusCode,
    body: serde_json::Value,
    classification: IdempotencyReplayClassification,
) -> Result<(), axum::response::Response> {
    if classification == IdempotencyReplayClassification::SecretBearing {
        return Err(idempotency_secret_bearing_response());
    }

    persist_with_classification(store, reservation, status, body, classification)
        .await
        .map_err(|error| {
            idempotency_quota_response_if_quota_error(&error).unwrap_or_else(|| {
                err_idempotency_completion_response(
                    error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
                    error.to_string(),
                )
            })
        })
}

pub async fn complete_or_match_with_classification(
    store: &dyn IdempotencyStore,
    reservation: &IdempotencyReservation,
    status: StatusCode,
    body: serde_json::Value,
    classification: IdempotencyReplayClassification,
) -> Result<(), axum::response::Response> {
    if classification == IdempotencyReplayClassification::SecretBearing {
        return Err(idempotency_secret_bearing_response());
    }

    persist_or_match_with_classification(store, reservation, status, body, classification)
        .await
        .map_err(|error| {
            idempotency_quota_response_if_quota_error(&error).unwrap_or_else(|| {
                err_idempotency_completion_response(
                    error_status(&error, StatusCode::INTERNAL_SERVER_ERROR),
                    error.to_string(),
                )
            })
        })
}

pub async fn persist_with_classification(
    store: &dyn IdempotencyStore,
    reservation: &IdempotencyReservation,
    status: StatusCode,
    body: serde_json::Value,
    classification: IdempotencyReplayClassification,
) -> Result<(), VfsError> {
    if classification == IdempotencyReplayClassification::SecretBearing {
        return Err(secret_bearing_replay_error());
    }

    store
        .complete_with_classification(reservation, status.as_u16(), body, classification)
        .await
}

pub async fn persist_or_match_with_classification(
    store: &dyn IdempotencyStore,
    reservation: &IdempotencyReservation,
    status: StatusCode,
    body: serde_json::Value,
    classification: IdempotencyReplayClassification,
) -> Result<(), VfsError> {
    if classification == IdempotencyReplayClassification::SecretBearing {
        return Err(secret_bearing_replay_error());
    }

    store
        .complete_or_match_with_classification(reservation, status.as_u16(), body, classification)
        .await
}

fn idempotency_error_response(status: StatusCode, msg: &'static str) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}

fn secret_bearing_replay_error() -> VfsError {
    VfsError::InvalidArgs {
        message: IDEMPOTENCY_SECRET_BEARING_MESSAGE.to_string(),
    }
}

fn err_idempotency_completion_response(
    status: StatusCode,
    message: String,
) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": message}))).into_response()
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::InvalidArgs { .. }
        | VfsError::InvalidExtension { .. }
        | VfsError::InvalidHandle { .. }
        | VfsError::InvalidPath { .. }
        | VfsError::UnknownCommand { .. }
        | VfsError::NoCommits
        | VfsError::DirtyWorkingTree
        | VfsError::NotSupported { .. } => StatusCode::BAD_REQUEST,
        VfsError::NotFound { .. } | VfsError::ObjectNotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::AlreadyExists { .. } | VfsError::ObjectWriteConflict { .. } => {
            StatusCode::CONFLICT
        }
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::IoError(_)
        | VfsError::CorruptStore { .. }
        | VfsError::IsDirectory { .. }
        | VfsError::NotDirectory { .. }
        | VfsError::NotEmpty { .. }
        | VfsError::SymlinkLoop { .. } => fallback,
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderValue, StatusCode};
    use std::sync::Arc;

    use super::*;
    use crate::audit::{
        AuditEvent, AuditResourceKind, AuditStore, InMemoryAuditStore, NewAuditEvent,
    };
    use crate::db::StratumDb;
    use crate::idempotency::{IdempotencyBegin, InMemoryIdempotencyStore, request_fingerprint};
    use crate::review::InMemoryReviewStore;
    use crate::server::{ServerLocalDb, ServerState};
    use crate::workspace::InMemoryWorkspaceMetadataStore;

    #[tokio::test]
    async fn secret_bearing_completion_is_rejected_before_persistence_with_redacted_body() {
        let store = InMemoryIdempotencyStore::new();
        let scope = "server:idempotency:secret-bearing";
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("secret-bearing-key"))
                .unwrap();
        let fingerprint =
            request_fingerprint(scope, &serde_json::json!({"request": "a"})).expect("fingerprint");
        let reservation = match store.begin(scope, &key, &fingerprint).await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };

        let response = complete_with_classification(
            &store,
            &reservation,
            StatusCode::OK,
            serde_json::json!({
                "workspace_token": "raw-secret-token",
                "body": "private-body",
            }),
            secret_bearing(),
        )
        .await
        .expect_err("secret-bearing completion must be rejected");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
        assert_eq!(body["error"], IDEMPOTENCY_SECRET_BEARING_MESSAGE);
        assert_eq!(body["idempotency_recorded"], false);
        assert_eq!(body["replayable"], false);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("raw-secret-token"));
        assert!(!rendered.contains("private-body"));

        match store.begin(scope, &key, &fingerprint).await.unwrap() {
            IdempotencyBegin::InProgress => {}
            other => panic!("secret-bearing response must not be persisted, got {other:?}"),
        }
    }

    #[test]
    fn quota_error_response_is_redacted_and_deterministic() {
        let error = VfsError::InvalidArgs {
            message: IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE.to_string(),
        };

        let response =
            idempotency_quota_response_if_quota_error(&error).expect("quota error response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn quota_error_audit_is_metadata_only() {
        let db = StratumDb::open_memory();
        let audit = Arc::new(InMemoryAuditStore::new());
        let state: AppState = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: audit.clone(),
            review: Arc::new(InMemoryReviewStore::new()),
            secret_replay_kms: None,
        });
        let error = VfsError::InvalidArgs {
            message: IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE.to_string(),
        };

        let response = idempotency_quota_response_if_quota_error_with_audit(
            &state,
            &Session::root(),
            "fs",
            &error,
        )
        .await
        .expect("quota response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
        assert_eq!(body["audit_recorded"], true);
        let events = audit.list_recent(1).await.expect("audit events");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.action, AuditAction::IdempotencyQuotaExceeded);
        assert_eq!(event.resource.kind, AuditResourceKind::Idempotency);
        assert_eq!(event.resource.id.as_deref(), Some("quota"));
        assert_eq!(
            event.details.get("route_family").map(String::as_str),
            Some("fs")
        );
        assert_eq!(
            event.details.get("quota_kind").map(String::as_str),
            Some("scope")
        );
        assert_eq!(
            event.details.get("has_workspace").map(String::as_str),
            Some("false")
        );
        let rendered = format!("{event:?}");
        assert!(!rendered.contains("Idempotency-Key"));
        assert!(!rendered.contains("raw"));
    }

    #[tokio::test]
    async fn quota_error_audit_failure_is_reported_without_backend_leak() {
        let db = StratumDb::open_memory();
        let state: AppState = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available(Arc::new(db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(FailingQuotaAuditStore),
            review: Arc::new(InMemoryReviewStore::new()),
            secret_replay_kms: None,
        });
        let error = VfsError::InvalidArgs {
            message: IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE.to_string(),
        };

        let response = idempotency_quota_response_if_quota_error_with_audit(
            &state,
            &Session::root(),
            "review",
            &error,
        )
        .await
        .expect("quota response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
        assert_eq!(body["error"], IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE);
        assert_eq!(body["quota"], "scope");
        assert_eq!(body["audit_recorded"], false);
        let rendered = serde_json::to_string(&body).unwrap();
        assert!(!rendered.contains("raw audit backend failure"));
        assert!(!rendered.contains("review text"));
        assert!(!rendered.contains("Idempotency-Key"));
    }

    struct FailingQuotaAuditStore;

    #[async_trait::async_trait]
    impl AuditStore for FailingQuotaAuditStore {
        async fn append(&self, _event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw audit backend failure with review text".to_string(),
            })
        }

        async fn list_recent(&self, _limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
            Ok(Vec::new())
        }

        async fn contains_vcs_commit_event(&self, _commit_id: &str) -> Result<bool, VfsError> {
            Ok(false)
        }
    }
}
