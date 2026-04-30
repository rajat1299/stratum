pub mod middleware;
pub mod routes_auth;
pub mod routes_fs;
pub mod routes_runs;
pub mod routes_vcs;
pub mod routes_workspace;

use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::db::StratumDb;
use crate::error::VfsError;
use crate::workspace::{LocalWorkspaceMetadataStore, SharedWorkspaceMetadataStore};

#[derive(Clone)]
pub struct ServerState {
    pub db: Arc<StratumDb>,
    pub workspaces: SharedWorkspaceMetadataStore,
}

pub type AppState = Arc<ServerState>;

pub fn build_router(db: StratumDb) -> Result<Router, VfsError> {
    let workspace_store = LocalWorkspaceMetadataStore::open(db.config().workspace_metadata_path())?;
    Ok(build_router_with_workspace_store(
        db,
        Arc::new(workspace_store),
    ))
}

pub fn build_router_with_workspace_store(
    db: StratumDb,
    workspaces: SharedWorkspaceMetadataStore,
) -> Router {
    let state: AppState = Arc::new(ServerState {
        db: Arc::new(db),
        workspaces,
    });

    Router::new()
        .merge(routes_auth::routes())
        .merge(routes_fs::routes())
        .merge(routes_runs::routes())
        .merge(routes_workspace::routes())
        .merge(routes_vcs::routes())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}
