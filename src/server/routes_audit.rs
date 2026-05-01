use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use super::AppState;
use super::middleware::session_from_headers;
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, WHEEL_GID};
use crate::error::VfsError;

const DEFAULT_AUDIT_LIMIT: usize = 100;
const MAX_AUDIT_LIMIT: usize = 1000;

#[derive(Deserialize, Default)]
struct AuditQuery {
    limit: Option<usize>,
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/audit", get(list_audit))
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        _ => fallback,
    }
}

fn require_admin_session(session: &Session) -> Result<(), VfsError> {
    if session.scope.is_some() {
        return Err(VfsError::PermissionDenied {
            path: "audit".to_string(),
        });
    }

    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "audit".to_string(),
        });
    }
    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "audit".to_string(),
            });
        }
    }

    Ok(())
}

async fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<Session, VfsError> {
    if let Some(auth_header) = headers.get("authorization") {
        let header_str = auth_header.to_str().map_err(|_| VfsError::AuthError {
            message: "invalid authorization header".to_string(),
        })?;
        if header_str.starts_with("Bearer ") {
            return Err(VfsError::PermissionDenied {
                path: "audit".to_string(),
            });
        }
    }

    let session = session_from_headers(state, headers).await?;
    require_admin_session(&session)?;
    Ok(session)
}

async fn list_audit(
    State(state): State<AppState>,
    Query(query): Query<AuditQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    let limit = query
        .limit
        .unwrap_or(DEFAULT_AUDIT_LIMIT)
        .min(MAX_AUDIT_LIMIT);
    match state.audit.list_recent(limit).await {
        Ok(events) => Json(serde_json::json!({ "events": events })).into_response(),
        Err(e) => err_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{
        AuditAction, AuditActor, AuditResource, AuditResourceKind, AuditStore, InMemoryAuditStore,
        NewAuditEvent,
    };
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use axum::extract::Query;
    use std::sync::Arc;

    fn root_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "User root".parse().unwrap());
        headers
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn admin_can_list_recent_events_but_workspace_bearer_cannot() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = db
            .execute_command("addagent ci-agent", &mut root)
            .await
            .unwrap()
            .lines()
            .last()
            .unwrap()
            .trim()
            .to_string();
        db.execute_command("usermod -aG wheel ci-agent", &mut root)
            .await
            .unwrap();
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        let workspaces = InMemoryWorkspaceMetadataStore::new();
        let workspace = workspaces.create_workspace("demo", "/demo").await.unwrap();
        let issued = workspaces
            .issue_scoped_workspace_token(
                workspace.id,
                "ci-token",
                agent.uid,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let audit = Arc::new(InMemoryAuditStore::new());
        audit
            .append(NewAuditEvent::new(
                AuditActor::new(0, "root"),
                AuditAction::WorkspaceCreate,
                AuditResource::id(AuditResourceKind::Workspace, workspace.id.to_string()),
            ))
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(workspaces),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit,
        });

        let admin = list_audit(
            State(state.clone()),
            Query(AuditQuery { limit: Some(10) }),
            root_headers(),
        )
        .await
        .into_response();
        assert_eq!(admin.status(), StatusCode::OK);
        let body = response_json(admin).await;
        assert_eq!(body["events"].as_array().unwrap().len(), 1);
        assert_eq!(body["events"][0]["action"], "workspace_create");

        let mut agent_bearer_headers = HeaderMap::new();
        agent_bearer_headers.insert(
            "authorization",
            format!("Bearer {raw_agent_token}").parse().unwrap(),
        );
        let agent_bearer = list_audit(
            State(state.clone()),
            Query(AuditQuery { limit: Some(10) }),
            agent_bearer_headers,
        )
        .await
        .into_response();
        assert_eq!(agent_bearer.status(), StatusCode::FORBIDDEN);

        let mut bearer_headers = HeaderMap::new();
        bearer_headers.insert(
            "authorization",
            format!("Bearer {}", issued.raw_secret).parse().unwrap(),
        );
        bearer_headers.insert(
            "x-stratum-workspace",
            workspace.id.to_string().parse().unwrap(),
        );
        let scoped = list_audit(
            State(state),
            Query(AuditQuery { limit: Some(10) }),
            bearer_headers,
        )
        .await
        .into_response();
        assert_eq!(scoped.status(), StatusCode::FORBIDDEN);
    }
}
