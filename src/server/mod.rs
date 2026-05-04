pub mod idempotency;
pub mod middleware;
pub mod routes_audit;
pub mod routes_auth;
pub mod routes_fs;
pub mod routes_review;
pub mod routes_runs;
pub mod routes_vcs;
pub mod routes_workspace;

use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::audit::{InMemoryAuditStore, LocalAuditStore, SharedAuditStore};
use crate::db::StratumDb;
use crate::error::VfsError;
use crate::idempotency::{InMemoryIdempotencyStore, LocalIdempotencyStore, SharedIdempotencyStore};
use crate::review::{InMemoryReviewStore, LocalReviewStore, SharedReviewStore};
use crate::workspace::{LocalWorkspaceMetadataStore, SharedWorkspaceMetadataStore};

#[derive(Clone)]
pub struct ServerState {
    pub db: Arc<StratumDb>,
    pub workspaces: SharedWorkspaceMetadataStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
    pub review: SharedReviewStore,
}

#[derive(Clone)]
pub struct ServerStores {
    pub workspaces: SharedWorkspaceMetadataStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
    pub review: SharedReviewStore,
}

impl ServerStores {
    pub fn open_local(config: &crate::config::Config) -> Result<Self, VfsError> {
        let workspace_store = LocalWorkspaceMetadataStore::open(config.workspace_metadata_path())?;
        let idempotency_store = LocalIdempotencyStore::open(config.idempotency_path())?;
        let audit_store = LocalAuditStore::open(config.audit_path())?;
        let review_store = LocalReviewStore::open(config.review_path())?;

        Ok(Self {
            workspaces: Arc::new(workspace_store),
            idempotency: Arc::new(idempotency_store),
            audit: Arc::new(audit_store),
            review: Arc::new(review_store),
        })
    }
}

pub type AppState = Arc<ServerState>;

pub fn build_router(db: StratumDb) -> Result<Router, VfsError> {
    let stores = ServerStores::open_local(db.config())?;
    Ok(build_router_with_server_stores(db, stores))
}

pub fn build_router_with_server_stores(db: StratumDb, stores: ServerStores) -> Router {
    build_router_with_stores(
        db,
        stores.workspaces,
        stores.idempotency,
        stores.audit,
        stores.review,
    )
}

pub fn build_router_with_workspace_store(
    db: StratumDb,
    workspaces: SharedWorkspaceMetadataStore,
) -> Router {
    let idempotency = Arc::new(InMemoryIdempotencyStore::new());
    let audit = Arc::new(InMemoryAuditStore::new());
    let review = Arc::new(InMemoryReviewStore::new());
    build_router_with_stores(db, workspaces, idempotency, audit, review)
}

pub fn build_router_with_stores(
    db: StratumDb,
    workspaces: SharedWorkspaceMetadataStore,
    idempotency: SharedIdempotencyStore,
    audit: SharedAuditStore,
    review: SharedReviewStore,
) -> Router {
    let state: AppState = Arc::new(ServerState {
        db: Arc::new(db),
        workspaces,
        idempotency,
        audit,
        review,
    });

    Router::new()
        .merge(routes_audit::routes())
        .merge(routes_auth::routes())
        .merge(routes_fs::routes())
        .merge(routes_review::routes())
        .merge(routes_runs::routes())
        .merge(routes_workspace::routes())
        .merge(routes_vcs::routes())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
