use axum::Json;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::error::VfsError;
use crate::idempotency::{IdempotencyKey, IdempotencyRecord};

pub const IDEMPOTENCY_REPLAY_HEADER: &str = "x-stratum-idempotent-replay";
pub const IDEMPOTENCY_REPLAY_HEADER_VALUE: &str = "true";
pub const IDEMPOTENCY_CONFLICT_MESSAGE: &str =
    "Idempotency-Key was reused with a different request";
pub const IDEMPOTENCY_IN_PROGRESS_MESSAGE: &str = "Idempotency-Key request is already in progress";

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

fn idempotency_error_response(status: StatusCode, msg: &'static str) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}
