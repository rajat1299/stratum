pub mod idempotency;
pub mod middleware;
pub mod routes_audit;
pub mod routes_auth;
pub mod routes_fs;
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
use crate::workspace::{LocalWorkspaceMetadataStore, SharedWorkspaceMetadataStore};

#[derive(Clone)]
pub struct ServerState {
    pub db: Arc<StratumDb>,
    pub workspaces: SharedWorkspaceMetadataStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
}

pub type AppState = Arc<ServerState>;

pub fn build_router(db: StratumDb) -> Result<Router, VfsError> {
    let workspace_store = LocalWorkspaceMetadataStore::open(db.config().workspace_metadata_path())?;
    let idempotency_store = LocalIdempotencyStore::open(db.config().idempotency_path())?;
    let audit_store = LocalAuditStore::open(db.config().audit_path())?;
    Ok(build_router_with_stores(
        db,
        Arc::new(workspace_store),
        Arc::new(idempotency_store),
        Arc::new(audit_store),
    ))
}

pub fn build_router_with_workspace_store(
    db: StratumDb,
    workspaces: SharedWorkspaceMetadataStore,
) -> Router {
    let idempotency = Arc::new(InMemoryIdempotencyStore::new());
    let audit = Arc::new(InMemoryAuditStore::new());
    build_router_with_stores(db, workspaces, idempotency, audit)
}

pub fn build_router_with_stores(
    db: StratumDb,
    workspaces: SharedWorkspaceMetadataStore,
    idempotency: SharedIdempotencyStore,
    audit: SharedAuditStore,
) -> Router {
    let state: AppState = Arc::new(ServerState {
        db: Arc::new(db),
        workspaces,
        idempotency,
        audit,
    });

    Router::new()
        .merge(routes_audit::routes())
        .merge(routes_auth::routes())
        .merge(routes_fs::routes())
        .merge(routes_runs::routes())
        .merge(routes_workspace::routes())
        .merge(routes_vcs::routes())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
