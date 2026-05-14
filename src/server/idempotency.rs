use axum::Json;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyKey, IdempotencyRecord, IdempotencyReplayClassification, IdempotencyReservation,
    IdempotencyStore,
};

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
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": IDEMPOTENCY_QUOTA_EXCEEDED_MESSAGE,
            "quota": "scope",
        })),
    )
        .into_response()
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

pub fn idempotency_json_replay_response(record: IdempotencyRecord) -> axum::response::Response {
    let status =
        StatusCode::from_u16(record.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        [(IDEMPOTENCY_REPLAY_HEADER, IDEMPOTENCY_REPLAY_HEADER_VALUE)],
        Json(record.response_body),
    )
        .into_response()
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

    use super::*;
    use crate::idempotency::{IdempotencyBegin, InMemoryIdempotencyStore, request_fingerprint};

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
}
