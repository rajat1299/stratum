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
#[cfg(feature = "postgres")]
use crate::backend::postgres::PostgresMetadataStore;
use crate::backend::runtime::{
    BackendRuntimeConfig, BackendRuntimeMode, CoreRuntimeMode, unsupported_durable_core_runtime,
};
use crate::config::Config;
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
    pub fn open_local(config: &Config) -> Result<Self, VfsError> {
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

pub fn open_core_db_for_runtime(
    runtime: &BackendRuntimeConfig,
    config: Config,
) -> Result<StratumDb, VfsError> {
    runtime.ensure_supported_for_server()?;

    match runtime.core_runtime_mode() {
        CoreRuntimeMode::LocalState => StratumDb::open(config),
        CoreRuntimeMode::DurableCloud => Err(unsupported_durable_core_runtime()),
    }
}

pub async fn open_server_stores_for_runtime(
    runtime: &BackendRuntimeConfig,
    config: &Config,
) -> Result<ServerStores, VfsError> {
    runtime.ensure_supported_for_server()?;

    match runtime.mode() {
        BackendRuntimeMode::Local => ServerStores::open_local(config),
        BackendRuntimeMode::Durable => open_durable_server_stores(runtime).await,
    }
}

#[cfg(not(feature = "postgres"))]
async fn open_durable_server_stores(
    _runtime: &BackendRuntimeConfig,
) -> Result<ServerStores, VfsError> {
    Err(VfsError::NotSupported {
        message: "durable backend runtime requires stratum-server built with the postgres feature"
            .to_string(),
    })
}

#[cfg(feature = "postgres")]
async fn open_durable_server_stores(
    runtime: &BackendRuntimeConfig,
) -> Result<ServerStores, VfsError> {
    let durable = runtime.durable().ok_or_else(|| VfsError::InvalidArgs {
        message: "durable backend runtime config is missing".to_string(),
    })?;
    let store = Arc::new(PostgresMetadataStore::with_schema(
        durable.postgres_config_with_env_password()?,
        durable.postgres_schema().to_string(),
    )?);
    store.ensure_control_plane_ready().await?;

    Ok(ServerStores {
        workspaces: store.clone(),
        idempotency: store.clone(),
        audit: store.clone(),
        review: store,
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::runtime::CORE_RUNTIME_ENV;
    use uuid::Uuid;

    #[tokio::test]
    async fn open_server_stores_rejects_unsupported_core_before_local_files() {
        let data_dir =
            std::env::temp_dir().join(format!("stratum-open-server-stores-{}", Uuid::new_v4()));
        let config = Config::from_env()
            .with_data_dir(&data_dir)
            .with_workspace_metadata_path(data_dir.join(".vfs").join("workspaces.bin"))
            .with_idempotency_path(data_dir.join(".vfs").join("idempotency.bin"))
            .with_audit_path(data_dir.join(".vfs").join("audit.bin"))
            .with_review_path(data_dir.join(".vfs").join("review.bin"));
        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            CORE_RUNTIME_ENV => Some("durable-cloud".to_string()),
            _ => None,
        })
        .expect("core runtime config should parse");

        let err = match open_server_stores_for_runtime(&runtime, &config).await {
            Ok(_) => panic!("unsupported durable core should reject store opening"),
            Err(err) => err,
        };

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(
            err.to_string()
                .contains("durable core runtime is not supported")
        );
        assert!(!data_dir.join(".vfs").exists());
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn open_core_db_rejects_unsupported_core_before_local_state_file() {
        let data_dir =
            std::env::temp_dir().join(format!("stratum-open-core-db-{}", Uuid::new_v4()));
        let config = Config::from_env().with_data_dir(&data_dir);
        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            CORE_RUNTIME_ENV => Some("durable-cloud".to_string()),
            _ => None,
        })
        .expect("core runtime config should parse");

        let err = match open_core_db_for_runtime(&runtime, config) {
            Ok(_) => panic!("unsupported durable core should reject core db opening"),
            Err(err) => err,
        };

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(
            err.to_string()
                .contains("durable core runtime is not supported")
        );
        assert!(!data_dir.join(".vfs").join("state.bin").exists());
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn open_core_db_rejects_unsupported_backend_before_local_state_file() {
        use crate::backend::runtime::{
            BACKEND_ENV, POSTGRES_URL_ENV, R2_ACCESS_KEY_ID_ENV, R2_BUCKET_ENV, R2_ENDPOINT_ENV,
            R2_SECRET_ACCESS_KEY_ENV,
        };

        let data_dir =
            std::env::temp_dir().join(format!("stratum-open-core-db-backend-{}", Uuid::new_v4()));
        let config = Config::from_env().with_data_dir(&data_dir);
        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            BACKEND_ENV => Some("durable".to_string()),
            POSTGRES_URL_ENV => Some("postgres://127.0.0.1/postgres".to_string()),
            R2_BUCKET_ENV => Some("bucket".to_string()),
            R2_ENDPOINT_ENV => Some("http://127.0.0.1:9000".to_string()),
            R2_ACCESS_KEY_ID_ENV => Some("access-key".to_string()),
            R2_SECRET_ACCESS_KEY_ENV => Some("secret-key".to_string()),
            _ => None,
        })
        .expect("runtime config should parse");

        let err = match open_core_db_for_runtime(&runtime, config) {
            Ok(_) => panic!("unsupported backend should reject core db opening"),
            Err(err) => err,
        };

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(!data_dir.join(".vfs").join("state.bin").exists());
        let _ = std::fs::remove_dir_all(data_dir);
    }
}
