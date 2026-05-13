use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::AppState;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub username: String,
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/auth/login", post(login))
        .route("/health", axum::routing::get(health))
}

async fn login(State(state): State<AppState>, Json(req): Json<LoginRequest>) -> impl IntoResponse {
    match state.core.login(&req.username).await {
        Ok(session) => (
            StatusCode::OK,
            Json(LoginResponse {
                username: session.username.clone(),
                uid: session.uid,
                gid: session.gid,
                groups: session.groups.clone(),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
            .into_response(),
    }
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let Ok(db) = state.db.get() else {
        return Json(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "core_runtime": "durable-cloud",
            "commits": null,
            "inodes": null,
            "objects": null,
        }));
    };

    let commits = db.commit_count().await;
    let inodes = db.inode_count().await;
    let objects = db.object_count().await;

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "commits": commits,
        "inodes": inodes,
        "objects": objects,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditStore;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::review::InMemoryReviewStore;
    use crate::server::core::LocalCoreRuntime;
    use crate::server::{ServerLocalDb, ServerState};
    use crate::workspace::InMemoryWorkspaceMetadataStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn login_routes_through_core_runtime() {
        let core_db = StratumDb::open_memory();
        let mut root = Session::root();
        core_db
            .execute_command("adduser durable-user", &mut root)
            .await
            .expect("create user in core db");

        let local_only_db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared(core_db),
            db: ServerLocalDb::available(Arc::new(local_only_db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        });

        let response = login(
            State(state),
            Json(LoginRequest {
                username: "durable-user".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_returns_durable_runtime_body_without_local_db() {
        let stores = crate::backend::StratumStores::local_memory();
        let state = Arc::new(ServerState {
            core: Arc::new(crate::server::core::DurableCoreRuntime::new(
                crate::backend::RepoId::new("repo_durable_health").expect("valid repo id"),
                stores.clone(),
            )),
            db: ServerLocalDb::unavailable(),
            workspaces: stores.workspace_metadata,
            idempotency: stores.idempotency,
            audit: stores.audit,
            review: stores.review,
        });

        let response = health(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read health response body");
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("health response is json");

        assert_eq!(
            body,
            serde_json::json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION"),
                "core_runtime": "durable-cloud",
                "commits": null,
                "inodes": null,
                "objects": null,
            })
        );
    }

    #[tokio::test]
    async fn health_returns_local_db_counts_when_available() {
        let db = Arc::new(StratumDb::open_memory());
        let expected_commits = db.commit_count().await;
        let expected_inodes = db.inode_count().await;
        let expected_objects = db.object_count().await;
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared_from_arc(db.clone()),
            db: ServerLocalDb::available(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        });

        let response = health(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read health response body");
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("health response is json");

        assert_eq!(body["status"], "ok");
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["commits"], expected_commits);
        assert_eq!(body["inodes"], expected_inodes);
        assert_eq!(body["objects"], expected_objects);
        assert!(body.get("core_runtime").is_none());
    }
}
