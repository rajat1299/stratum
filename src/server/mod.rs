pub(crate) mod core;
pub mod idempotency;
pub mod middleware;
pub(crate) mod policy;
pub(crate) mod repo_context;
pub mod routes_audit;
pub mod routes_auth;
pub mod routes_capabilities;
pub mod routes_fs;
pub mod routes_review;
pub mod routes_runs;
pub mod routes_vcs;
pub mod routes_workspace;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::routing::any;
use axum::{Extension, Json, Router};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex as AsyncMutex, oneshot, watch};
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::audit::{InMemoryAuditStore, LocalAuditStore, SharedAuditStore};
#[cfg(feature = "postgres")]
use crate::backend::blob_object::BlobObjectStore;
use crate::backend::core_transaction::{
    DurableCorePostCasRepairWorker, DurableCorePostCasRepairWorkerStores,
    DurableCorePreVisibilityRecoveryRun, DurableCorePreVisibilityRecoveryRunStores,
    DurableFsMutationRecoveryWorker,
};
use crate::backend::object_cleanup::ObjectCleanupWorker;
#[cfg(feature = "postgres")]
use crate::backend::postgres::PostgresMetadataStore;
#[cfg(feature = "postgres")]
use crate::backend::runtime::DurableObjectStoreRuntimeConfig;
use crate::backend::runtime::{
    BackendRuntimeConfig, BackendRuntimeMode, CoreRuntimeMode, RecoverySchedulerMode,
    RecoverySchedulerRuntimeConfig, unsupported_durable_core_runtime,
};
#[cfg(feature = "postgres")]
use crate::backend::runtime::{EnvPostgresSecretProvider, PostgresSecretProvider};
use crate::backend::{RepoId, StratumStores};
use crate::config::Config;
use crate::db::StratumDb;
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyBegin, IdempotencyKey, IdempotencyQuotaIdentity, IdempotencyReplayClassification,
    IdempotencyReservation, IdempotencyRetentionPolicy, IdempotencyStore, IdempotencySweepRequest,
    IdempotencySweepSummary, InMemoryIdempotencyStore, LocalIdempotencyStore,
    RetainedIdempotencyRecord, SharedIdempotencyStore,
};
#[cfg(feature = "postgres")]
use crate::remote::blob::{R2BlobStore, R2BlobStoreConfig};
use crate::review::{InMemoryReviewStore, LocalReviewStore, SharedReviewStore};
use crate::secret_replay::SharedSecretReplayKms;
use crate::server::core::{DurableCoreRuntime, LocalCoreRuntime, SharedCoreRuntime};
use crate::workspace::{LocalWorkspaceMetadataStore, SharedWorkspaceMetadataStore};

const DURABLE_RECOVERY_SCHEDULER_COMMIT_LEASE_OWNER: &str =
    "guarded-durable-commit-recovery-scheduler";
const DURABLE_RECOVERY_SCHEDULER_FS_LEASE_OWNER: &str = "durable-fs-mutation-recovery-scheduler";
static DURABLE_RECOVERY_SCHEDULERS: OnceLock<
    Mutex<HashMap<DurableRecoverySchedulerKey, Weak<DurableRecoverySchedulerHandle>>>,
> = OnceLock::new();

#[derive(Clone)]
pub struct ServerState {
    pub(crate) core: SharedCoreRuntime,
    pub db: ServerLocalDb,
    pub workspaces: SharedWorkspaceMetadataStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
    pub review: SharedReviewStore,
    pub secret_replay_kms: Option<SharedSecretReplayKms>,
}

#[derive(Clone)]
pub struct ServerLocalDb {
    db: Option<Arc<StratumDb>>,
    runtime_kind: ServerRuntimeKind,
    backend_mode: BackendRuntimeMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerRuntimeKind {
    LocalState,
    DurableCloud,
}

impl ServerLocalDb {
    pub fn available(db: Arc<StratumDb>) -> Self {
        Self {
            db: Some(db),
            runtime_kind: ServerRuntimeKind::LocalState,
            backend_mode: BackendRuntimeMode::Local,
        }
    }

    pub fn available_with_backend(db: Arc<StratumDb>, backend_mode: BackendRuntimeMode) -> Self {
        Self {
            db: Some(db),
            runtime_kind: ServerRuntimeKind::LocalState,
            backend_mode,
        }
    }

    pub fn unavailable() -> Self {
        Self {
            db: None,
            runtime_kind: ServerRuntimeKind::DurableCloud,
            backend_mode: BackendRuntimeMode::Durable,
        }
    }

    pub fn is_available(&self) -> bool {
        self.db.is_some()
    }

    pub fn get(&self) -> Result<&StratumDb, VfsError> {
        self.db.as_deref().ok_or_else(|| VfsError::NotSupported {
            message: "local StratumDb is not available for this server runtime".to_string(),
        })
    }

    pub(crate) fn runtime_kind(&self) -> ServerRuntimeKind {
        self.runtime_kind
    }

    pub(crate) fn backend_mode(&self) -> BackendRuntimeMode {
        self.backend_mode
    }
}

impl Deref for ServerLocalDb {
    type Target = StratumDb;

    fn deref(&self) -> &Self::Target {
        self.get()
            .expect("local StratumDb is not available for this server runtime")
    }
}

#[derive(Clone)]
pub struct ServerStores {
    pub backend_mode: BackendRuntimeMode,
    pub workspaces: SharedWorkspaceMetadataStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
    pub review: SharedReviewStore,
    pub secret_replay_kms: Option<SharedSecretReplayKms>,
    pub guarded_durable_commit_stores: Option<StratumStores>,
    pub durable_core_stores: Option<StratumStores>,
}

impl ServerStores {
    pub fn open_local(config: &Config) -> Result<Self, VfsError> {
        let workspace_store = LocalWorkspaceMetadataStore::open(config.workspace_metadata_path())?;
        let idempotency_store = LocalIdempotencyStore::open(config.idempotency_path())?;
        let audit_store = LocalAuditStore::open(config.audit_path())?;
        let review_store = LocalReviewStore::open(config.review_path())?;

        Ok(Self {
            backend_mode: BackendRuntimeMode::Local,
            workspaces: Arc::new(workspace_store),
            idempotency: Arc::new(idempotency_store),
            audit: Arc::new(audit_store),
            review: Arc::new(review_store),
            secret_replay_kms: None,
            guarded_durable_commit_stores: None,
            durable_core_stores: None,
        })
    }
}

pub type AppState = Arc<ServerState>;

#[derive(Clone, Default)]
pub struct ServerRecoverySchedulerShutdownHandle {
    durable: Option<Arc<DurableRecoverySchedulerHandle>>,
}

impl ServerRecoverySchedulerShutdownHandle {
    fn from_durable(durable: Option<Arc<DurableRecoverySchedulerHandle>>) -> Self {
        Self { durable }
    }

    pub fn shutdown_drain_enabled(&self) -> bool {
        self.durable
            .as_ref()
            .is_some_and(|handle| handle.status().shutdown_drain_enabled)
    }

    pub async fn request_shutdown_drain_if_enabled(&self) {
        let Some(handle) = &self.durable else {
            return;
        };
        if handle.status().shutdown_drain_enabled {
            let drain = handle.request_shutdown_drain().await;
            tracing::info!(
                timed_out = drain.timed_out,
                outcome = drain.outcome.as_deref().unwrap_or("unknown"),
                "durable recovery scheduler shutdown drain finished"
            );
        }
    }
}

impl ServerState {
    pub(crate) fn requires_explicit_workspace_repo(&self) -> bool {
        !self.db.is_available() || self.core.guarded_durable_commit_route().is_some()
    }
}

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
    #[cfg(feature = "postgres")]
    {
        return open_server_stores_for_runtime_with_secret_provider(
            runtime,
            config,
            &EnvPostgresSecretProvider,
        )
        .await;
    }

    #[cfg(not(feature = "postgres"))]
    {
        runtime.ensure_supported_for_server()?;

        match runtime.mode() {
            BackendRuntimeMode::Local => {
                let mut stores = ServerStores::open_local(config)?;
                stores.secret_replay_kms = runtime.secret_replay_kms()?;
                Ok(stores)
            }
            BackendRuntimeMode::Durable => open_durable_server_stores(runtime).await,
        }
    }
}

#[cfg(feature = "postgres")]
pub(crate) async fn open_server_stores_for_runtime_with_secret_provider(
    runtime: &BackendRuntimeConfig,
    config: &Config,
    secret_provider: &impl PostgresSecretProvider,
) -> Result<ServerStores, VfsError> {
    runtime.ensure_supported_for_server()?;

    match runtime.mode() {
        BackendRuntimeMode::Local => {
            let mut stores = ServerStores::open_local(config)?;
            stores.secret_replay_kms = runtime.secret_replay_kms()?;
            Ok(stores)
        }
        BackendRuntimeMode::Durable => open_durable_server_stores(runtime, secret_provider).await,
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
    secret_provider: &impl PostgresSecretProvider,
) -> Result<ServerStores, VfsError> {
    let durable = runtime.durable().ok_or_else(|| VfsError::InvalidArgs {
        message: "durable backend runtime config is missing".to_string(),
    })?;
    let store = Arc::new(PostgresMetadataStore::with_schema_and_posture(
        durable.postgres_config_with_secret_provider(secret_provider)?,
        durable.postgres_schema().to_string(),
        durable.postgres_posture().clone(),
    )?);
    store.ensure_control_plane_ready().await?;
    let idempotency = runtime
        .idempotency_retention_policy()
        .map(|policy| {
            Arc::new(PolicyIdempotencyStore::new(store.clone(), policy.clone()))
                as SharedIdempotencyStore
        })
        .unwrap_or_else(|| store.clone());
    let durable_core_stores = if runtime.core_runtime_mode() == CoreRuntimeMode::DurableCloud {
        Some(
            open_stratum_stores_for_durable_core(
                store.clone(),
                idempotency.clone(),
                durable.object_store().clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let guarded_durable_commit_stores = if runtime.core_runtime_mode()
        == CoreRuntimeMode::LocalState
        && runtime.guarded_durable_commit_route_enabled()
    {
        Some(
            open_guarded_durable_commit_stores(
                store.clone(),
                idempotency.clone(),
                durable.object_store().clone(),
            )
            .await?,
        )
    } else {
        None
    };

    Ok(ServerStores {
        backend_mode: BackendRuntimeMode::Durable,
        workspaces: store.clone(),
        idempotency,
        audit: store.clone(),
        review: store,
        secret_replay_kms: runtime.secret_replay_kms()?,
        guarded_durable_commit_stores,
        durable_core_stores,
    })
}

#[cfg(feature = "postgres")]
async fn open_guarded_durable_commit_stores(
    store: Arc<PostgresMetadataStore>,
    idempotency: SharedIdempotencyStore,
    object_store: DurableObjectStoreRuntimeConfig,
) -> Result<StratumStores, VfsError> {
    open_stratum_stores_for_durable_core(store, idempotency, object_store).await
}

#[cfg(feature = "postgres")]
async fn open_stratum_stores_for_durable_core(
    store: Arc<PostgresMetadataStore>,
    idempotency: SharedIdempotencyStore,
    object_store: DurableObjectStoreRuntimeConfig,
) -> Result<StratumStores, VfsError> {
    let r2_config = R2BlobStoreConfig::from_runtime_config_with_env_credentials(&object_store)?;
    let blobs = Arc::new(R2BlobStore::new(r2_config).await?);
    blobs.ensure_ready().await?;
    let objects = Arc::new(BlobObjectStore::new(blobs, store.clone()));

    Ok(StratumStores {
        objects,
        object_metadata: store.clone(),
        commits: store.clone(),
        refs: store.clone(),
        workspace_metadata: store.clone(),
        review: store.clone(),
        idempotency,
        audit: store.clone(),
        post_cas_recovery: store.clone(),
        pre_visibility_recovery: store.clone(),
        fs_mutation_recovery: store.clone(),
        object_cleanup: store,
    })
}

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
struct PolicyIdempotencyStore {
    inner: SharedIdempotencyStore,
    policy: IdempotencyRetentionPolicy,
}

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
impl PolicyIdempotencyStore {
    fn new(inner: SharedIdempotencyStore, policy: IdempotencyRetentionPolicy) -> Self {
        Self { inner, policy }
    }
}

#[async_trait]
impl IdempotencyStore for PolicyIdempotencyStore {
    async fn begin(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Result<IdempotencyBegin, VfsError> {
        self.inner
            .begin_with_policy(
                scope,
                key,
                request_fingerprint,
                IdempotencyQuotaIdentity::for_scope(scope),
                &self.policy,
            )
            .await
    }

    async fn begin_with_policy(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
        quota_identity: IdempotencyQuotaIdentity,
        _policy: &IdempotencyRetentionPolicy,
    ) -> Result<IdempotencyBegin, VfsError> {
        self.inner
            .begin_with_policy(
                scope,
                key,
                request_fingerprint,
                quota_identity,
                &self.policy,
            )
            .await
    }

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.inner
            .complete(reservation, status_code, response_body)
            .await
    }

    async fn complete_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        self.inner
            .complete_with_classification(reservation, status_code, response_body, classification)
            .await
    }

    async fn complete_with_encrypted_secret_replay(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        encrypted_envelope_body: serde_json::Value,
        metadata: crate::idempotency::SecretReplayMetadata,
    ) -> Result<(), VfsError> {
        self.inner
            .complete_with_encrypted_secret_replay(
                reservation,
                status_code,
                encrypted_envelope_body,
                metadata,
            )
            .await
    }

    async fn complete_or_match(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.inner
            .complete_or_match(reservation, status_code, response_body)
            .await
    }

    async fn complete_or_match_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        self.inner
            .complete_or_match_with_classification(
                reservation,
                status_code,
                response_body,
                classification,
            )
            .await
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        self.inner.abort(reservation).await;
    }

    async fn sweep_retention(
        &self,
        request: IdempotencySweepRequest,
    ) -> Result<IdempotencySweepSummary, VfsError> {
        self.inner.sweep_retention(request).await
    }

    async fn list_retained_for_repo(
        &self,
        repo_id: &RepoId,
        limit: usize,
    ) -> Result<Vec<RetainedIdempotencyRecord>, VfsError> {
        self.inner.list_retained_for_repo(repo_id, limit).await
    }
}

pub fn build_router(db: StratumDb) -> Result<Router, VfsError> {
    let stores = ServerStores::open_local(db.config())?;
    Ok(build_router_with_server_stores(db, stores))
}

pub fn build_router_with_server_stores(db: StratumDb, stores: ServerStores) -> Router {
    build_router_with_server_stores_and_recovery_scheduler(
        db,
        stores,
        RecoverySchedulerRuntimeConfig::default(),
    )
}

pub fn build_router_with_server_stores_and_recovery_scheduler(
    db: StratumDb,
    stores: ServerStores,
    recovery_scheduler: RecoverySchedulerRuntimeConfig,
) -> Router {
    build_router_with_server_stores_and_recovery_scheduler_shutdown_handle(
        db,
        stores,
        recovery_scheduler,
    )
    .0
}

pub fn build_router_with_server_stores_and_recovery_scheduler_shutdown_handle(
    db: StratumDb,
    stores: ServerStores,
    recovery_scheduler: RecoverySchedulerRuntimeConfig,
) -> (Router, ServerRecoverySchedulerShutdownHandle) {
    build_router_with_config(ServerRouterConfig {
        db,
        backend_mode: stores.backend_mode,
        workspaces: stores.workspaces,
        idempotency: stores.idempotency,
        audit: stores.audit,
        review: stores.review,
        secret_replay_kms: stores.secret_replay_kms,
        recovery_scheduler,
        guarded_durable_commit_stores: stores.guarded_durable_commit_stores,
    })
}

pub fn build_durable_core_router(stores: ServerStores, repo_id: RepoId) -> Router {
    build_durable_core_router_with_recovery_scheduler(
        stores,
        repo_id,
        RecoverySchedulerRuntimeConfig::default(),
    )
}

pub fn build_durable_core_router_with_recovery_scheduler(
    stores: ServerStores,
    repo_id: RepoId,
    recovery_scheduler: RecoverySchedulerRuntimeConfig,
) -> Router {
    build_durable_core_router_with_recovery_scheduler_shutdown_handle(
        stores,
        repo_id,
        recovery_scheduler,
    )
    .0
}

pub fn build_durable_core_router_with_recovery_scheduler_shutdown_handle(
    stores: ServerStores,
    repo_id: RepoId,
    recovery_scheduler: RecoverySchedulerRuntimeConfig,
) -> (Router, ServerRecoverySchedulerShutdownHandle) {
    let durable_core_stores = stores
        .durable_core_stores
        .expect("durable core router requires durable core stores");
    let durable_recovery_scheduler = start_durable_recovery_scheduler_for_repo(
        durable_core_stores.clone(),
        repo_id.clone(),
        recovery_scheduler,
    );
    let state: AppState = Arc::new(ServerState {
        core: Arc::new(DurableCoreRuntime::new(repo_id, durable_core_stores)),
        db: ServerLocalDb::unavailable(),
        workspaces: stores.workspaces,
        idempotency: stores.idempotency,
        audit: stores.audit,
        review: stores.review,
        secret_replay_kms: stores.secret_replay_kms,
    });

    let router = Router::new()
        .merge(routes_capabilities::routes())
        .merge(routes_auth::health_routes())
        .merge(routes_fs::durable_read_routes())
        .merge(routes_review::routes())
        .merge(routes_vcs::durable_read_routes())
        .merge(durable_unsupported_routes())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());
    if let Some(handle) = durable_recovery_scheduler {
        let shutdown_handle =
            ServerRecoverySchedulerShutdownHandle::from_durable(Some(handle.clone()));
        (router.layer(Extension(handle)), shutdown_handle)
    } else {
        (router, ServerRecoverySchedulerShutdownHandle::default())
    }
}

fn durable_unsupported_routes() -> Router<AppState> {
    Router::new()
        .route("/auth/login", any(durable_cloud_route_not_supported))
        .route("/runs", any(durable_cloud_route_not_supported))
        .route("/runs/{*path}", any(durable_cloud_route_not_supported))
        .route("/audit", any(durable_cloud_route_not_supported))
        .route("/audit/{*path}", any(durable_cloud_route_not_supported))
        .route("/workspaces", any(durable_cloud_route_not_supported))
        .route(
            "/workspaces/{*path}",
            any(durable_cloud_route_not_supported),
        )
}

async fn durable_cloud_route_not_supported() -> impl axum::response::IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": "stratum: operation not supported: durable-cloud route is not supported yet"
        })),
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
    build_router_with_config(ServerRouterConfig {
        db,
        backend_mode: BackendRuntimeMode::Local,
        workspaces,
        idempotency,
        audit,
        review,
        secret_replay_kms: None,
        recovery_scheduler: RecoverySchedulerRuntimeConfig::default(),
        guarded_durable_commit_stores: None,
    })
    .0
}

struct ServerRouterConfig {
    db: StratumDb,
    backend_mode: BackendRuntimeMode,
    workspaces: SharedWorkspaceMetadataStore,
    idempotency: SharedIdempotencyStore,
    audit: SharedAuditStore,
    review: SharedReviewStore,
    secret_replay_kms: Option<SharedSecretReplayKms>,
    recovery_scheduler: RecoverySchedulerRuntimeConfig,
    guarded_durable_commit_stores: Option<StratumStores>,
}

fn build_router_with_config(
    config: ServerRouterConfig,
) -> (Router, ServerRecoverySchedulerShutdownHandle) {
    let ServerRouterConfig {
        db,
        backend_mode,
        workspaces,
        idempotency,
        audit,
        review,
        secret_replay_kms,
        recovery_scheduler,
        guarded_durable_commit_stores,
    } = config;
    let db = Arc::new(db);
    let recovery_scheduler_stores = guarded_durable_commit_stores.clone();
    let core = match guarded_durable_commit_stores {
        Some(stores) => LocalCoreRuntime::shared_with_guarded_durable_commit_route(
            db.as_ref().clone(),
            RepoId::local(),
            stores,
        ),
        None => LocalCoreRuntime::shared_from_arc(db.clone()),
    };
    let durable_recovery_scheduler = recovery_scheduler_stores
        .and_then(|stores| start_durable_recovery_scheduler(stores, recovery_scheduler));
    let state: AppState = Arc::new(ServerState {
        core,
        db: ServerLocalDb::available_with_backend(db, backend_mode),
        workspaces,
        idempotency,
        audit,
        review,
        secret_replay_kms,
    });

    let router = Router::new()
        .merge(routes_capabilities::routes())
        .merge(routes_audit::routes())
        .merge(routes_auth::routes())
        .merge(routes_fs::routes())
        .merge(routes_review::routes())
        .merge(routes_runs::routes())
        .merge(routes_workspace::routes())
        .merge(routes_vcs::routes())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());
    if let Some(handle) = durable_recovery_scheduler {
        let shutdown_handle =
            ServerRecoverySchedulerShutdownHandle::from_durable(Some(handle.clone()));
        (router.layer(Extension(handle)), shutdown_handle)
    } else {
        (router, ServerRecoverySchedulerShutdownHandle::default())
    }
}

pub(crate) struct DurableRecoverySchedulerHandle {
    key: DurableRecoverySchedulerKey,
    task: Mutex<Option<JoinHandle<()>>>,
    status: Arc<Mutex<DurableRecoverySchedulerStatus>>,
    repo_id: RepoId,
    stores: StratumStores,
    config: Mutex<DurableRecoverySchedulerConfig>,
    shutdown_tx: watch::Sender<bool>,
    tick_mutex: Arc<AsyncMutex<()>>,
}

impl DurableRecoverySchedulerHandle {
    pub(crate) fn status(&self) -> DurableRecoverySchedulerStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn has_background_task(&self) -> bool {
        self.task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    fn abort_background_task(&self) {
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            task.abort();
        }
    }

    pub(crate) async fn request_shutdown_drain(&self) -> DurableRecoverySchedulerDrainStatus {
        let config = *self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let started_at_millis = current_unix_timestamp_millis();
        {
            let mut status = self
                .status
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            status.state = DurableRecoverySchedulerState::Draining;
            status.shutdown_drain = Some(DurableRecoverySchedulerDrainStatus {
                started_at_millis,
                completed_at_millis: None,
                timeout_millis: config.shutdown_drain_timeout_millis(),
                timed_out: false,
                outcome: None,
            });
        }

        if !config.enabled {
            return self.finish_shutdown_drain(
                started_at_millis,
                config,
                false,
                "skipped_disabled",
                DurableRecoverySchedulerState::Stopped,
            );
        }

        self.shutdown_tx.send_replace(true);
        let (drain_tx, drain_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let timed_out = Arc::new(AtomicBool::new(false));
        let drain_deadline = tokio::time::Instant::now() + config.shutdown_drain_timeout;
        let drain_status = self.status.clone();
        let drain_repo_id = self.repo_id.clone();
        let drain_stores = self.stores.clone();
        let drain_tick_mutex = self.tick_mutex.clone();
        let drain_timed_out = timed_out.clone();
        tokio::spawn(async move {
            let outcome =
                durable_recovery_scheduler_drain_until_idle(DurableRecoverySchedulerDrainContext {
                    repo_id: drain_repo_id,
                    stores: drain_stores,
                    config,
                    status: drain_status.clone(),
                    tick_mutex: drain_tick_mutex,
                    stop_rx,
                    timed_out: drain_timed_out.clone(),
                    deadline: drain_deadline,
                })
                .await;
            let drain = finish_shutdown_drain_after_worker(
                &drain_status,
                started_at_millis,
                config,
                &outcome,
                drain_timed_out.load(Ordering::SeqCst),
            );
            let _ = drain_tx.send(drain);
        });
        let drain_result = tokio::time::timeout_at(drain_deadline, drain_rx).await;

        match drain_result {
            Ok(Ok(drain)) => {
                self.abort_background_task();
                drain
            }
            Ok(Err(_)) => self.finish_shutdown_drain(
                started_at_millis,
                config,
                true,
                "drain_failed",
                DurableRecoverySchedulerState::Stopped,
            ),
            Err(_) => {
                timed_out.store(true, Ordering::SeqCst);
                stop_tx.send_replace(true);
                self.finish_shutdown_drain_timeout(started_at_millis, config)
            }
        }
    }

    fn finish_shutdown_drain_timeout(
        &self,
        started_at_millis: u64,
        config: DurableRecoverySchedulerConfig,
    ) -> DurableRecoverySchedulerDrainStatus {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let final_state = if status.state == DurableRecoverySchedulerState::Stopped
            && status
                .shutdown_drain
                .as_ref()
                .is_some_and(|drain| drain.started_at_millis == started_at_millis)
        {
            DurableRecoverySchedulerState::Stopped
        } else {
            DurableRecoverySchedulerState::Draining
        };
        let drain = DurableRecoverySchedulerDrainStatus {
            started_at_millis,
            completed_at_millis: Some(current_unix_timestamp_millis()),
            timeout_millis: config.shutdown_drain_timeout_millis(),
            timed_out: true,
            outcome: Some("timed_out".to_string()),
        };
        status.enabled = config.enabled;
        status.state = final_state;
        status.shutdown_drain = Some(drain.clone());
        drain
    }

    fn finish_shutdown_drain(
        &self,
        started_at_millis: u64,
        config: DurableRecoverySchedulerConfig,
        timed_out: bool,
        outcome: &str,
        final_state: DurableRecoverySchedulerState,
    ) -> DurableRecoverySchedulerDrainStatus {
        finish_shutdown_drain_status(
            &self.status,
            started_at_millis,
            config,
            timed_out,
            outcome,
            final_state,
        )
    }
}

fn finish_shutdown_drain_status(
    status: &Arc<Mutex<DurableRecoverySchedulerStatus>>,
    started_at_millis: u64,
    config: DurableRecoverySchedulerConfig,
    timed_out: bool,
    outcome: &str,
    final_state: DurableRecoverySchedulerState,
) -> DurableRecoverySchedulerDrainStatus {
    let drain = DurableRecoverySchedulerDrainStatus {
        started_at_millis,
        completed_at_millis: Some(current_unix_timestamp_millis()),
        timeout_millis: config.shutdown_drain_timeout_millis(),
        timed_out,
        outcome: Some(outcome.to_string()),
    };
    let mut status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.enabled = config.enabled;
    status.state = final_state;
    status.shutdown_drain = Some(drain.clone());
    drain
}

fn finish_shutdown_drain_after_worker(
    status: &Arc<Mutex<DurableRecoverySchedulerStatus>>,
    started_at_millis: u64,
    config: DurableRecoverySchedulerConfig,
    outcome: &str,
    timed_out: bool,
) -> DurableRecoverySchedulerDrainStatus {
    let mut status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if timed_out {
        return finish_shutdown_drain_worker_timed_out_status(
            &mut status,
            started_at_millis,
            config,
        );
    }
    let timed_out_status_should_remain = status
        .shutdown_drain
        .as_ref()
        .is_some_and(|drain| drain.started_at_millis == started_at_millis && drain.timed_out);
    if timed_out_status_should_remain {
        status.enabled = config.enabled;
        status.state = DurableRecoverySchedulerState::Stopped;
        return status
            .shutdown_drain
            .clone()
            .expect("checked shutdown drain status");
    }

    let drain = DurableRecoverySchedulerDrainStatus {
        started_at_millis,
        completed_at_millis: Some(current_unix_timestamp_millis()),
        timeout_millis: config.shutdown_drain_timeout_millis(),
        timed_out: false,
        outcome: Some(outcome.to_string()),
    };
    status.enabled = config.enabled;
    status.state = DurableRecoverySchedulerState::Stopped;
    status.shutdown_drain = Some(drain.clone());
    drain
}

impl Drop for DurableRecoverySchedulerHandle {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            task.abort();
        }
        let registry = DURABLE_RECOVERY_SCHEDULERS.get_or_init(|| Mutex::new(HashMap::new()));
        registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
    }
}

fn start_durable_recovery_scheduler(
    stores: StratumStores,
    runtime_config: RecoverySchedulerRuntimeConfig,
) -> Option<Arc<DurableRecoverySchedulerHandle>> {
    start_durable_recovery_scheduler_for_repo(stores, RepoId::local(), runtime_config)
}

fn start_durable_recovery_scheduler_for_repo(
    stores: StratumStores,
    repo_id: RepoId,
    runtime_config: RecoverySchedulerRuntimeConfig,
) -> Option<Arc<DurableRecoverySchedulerHandle>> {
    let key = durable_recovery_scheduler_key(&stores, &repo_id);
    let scheduler_config = DurableRecoverySchedulerConfig::from_runtime(&runtime_config);
    let registry = DURABLE_RECOVERY_SCHEDULERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut started = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = started.get(&key).and_then(Weak::upgrade) {
        let existing_status = existing.status();
        if existing_status.enabled && existing.has_background_task() {
            return Some(existing);
        }
        if existing_status.state == DurableRecoverySchedulerState::Draining {
            return Some(existing);
        }
        if !scheduler_config.enabled {
            existing.replace_status_config(scheduler_config);
            return Some(existing);
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("durable recovery scheduler skipped without a Tokio runtime");
            return None;
        };
        existing.start_background_task(repo_id, stores, scheduler_config, handle);
        return Some(existing);
    }
    started.remove(&key);
    let status = Arc::new(Mutex::new(DurableRecoverySchedulerStatus::new(
        current_unix_timestamp_millis(),
        scheduler_config,
    )));
    let tick_mutex = Arc::new(AsyncMutex::new(()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = if scheduler_config.enabled {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("durable recovery scheduler skipped without a Tokio runtime");
            return None;
        };
        let tick_status = status.clone();
        let tick_mutex = tick_mutex.clone();
        let task_repo_id = repo_id.clone();
        let task_stores = stores.clone();
        Some(handle.spawn(async move {
            durable_recovery_scheduler_loop(
                task_repo_id,
                task_stores,
                scheduler_config,
                tick_status,
                tick_mutex,
                shutdown_rx,
            )
            .await;
        }))
    } else {
        None
    };
    let scheduler = Arc::new(DurableRecoverySchedulerHandle {
        key: key.clone(),
        task: Mutex::new(task),
        status,
        repo_id,
        stores,
        config: Mutex::new(scheduler_config),
        shutdown_tx,
        tick_mutex,
    });
    started.insert(key, Arc::downgrade(&scheduler));
    Some(scheduler)
}

impl DurableRecoverySchedulerHandle {
    fn replace_status_config(&self, config: DurableRecoverySchedulerConfig) {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let started_at_millis = status.started_at_millis;
        *status = DurableRecoverySchedulerStatus::new(started_at_millis, config);
        *self
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
    }

    fn start_background_task(
        &self,
        repo_id: RepoId,
        stores: StratumStores,
        config: DurableRecoverySchedulerConfig,
        handle: tokio::runtime::Handle,
    ) {
        let mut task = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        task.take();
        self.replace_status_config(config);
        self.shutdown_tx.send_replace(false);
        let tick_status = self.status.clone();
        let tick_mutex = self.tick_mutex.clone();
        let shutdown_rx = self.shutdown_tx.subscribe();
        *task = Some(handle.spawn(async move {
            durable_recovery_scheduler_loop(
                repo_id,
                stores,
                config,
                tick_status,
                tick_mutex,
                shutdown_rx,
            )
            .await;
        }));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DurableRecoverySchedulerConfig {
    enabled: bool,
    state: DurableRecoverySchedulerState,
    interval: Duration,
    tick_limit: usize,
    lease_duration: Duration,
    shutdown_drain_enabled: bool,
    shutdown_drain_timeout: Duration,
}

impl DurableRecoverySchedulerConfig {
    fn from_runtime(config: &RecoverySchedulerRuntimeConfig) -> Self {
        let enabled = config.mode() == RecoverySchedulerMode::Enabled;
        Self {
            enabled,
            state: if enabled {
                DurableRecoverySchedulerState::Running
            } else {
                DurableRecoverySchedulerState::Disabled
            },
            interval: config.interval(),
            tick_limit: config.tick_limit(),
            lease_duration: config.lease_duration(),
            shutdown_drain_enabled: config.shutdown_drain_enabled(),
            shutdown_drain_timeout: config.shutdown_drain_timeout(),
        }
    }

    fn interval_millis(self) -> u64 {
        duration_millis(self.interval)
    }

    fn lease_millis(self) -> u64 {
        duration_millis(self.lease_duration)
    }

    fn shutdown_drain_timeout_millis(self) -> u64 {
        duration_millis(self.shutdown_drain_timeout)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurableRecoverySchedulerState {
    Disabled,
    Running,
    Draining,
    Stopped,
}

impl DurableRecoverySchedulerState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DurableRecoverySchedulerStatus {
    pub(crate) enabled: bool,
    pub(crate) state: DurableRecoverySchedulerState,
    pub(crate) interval_millis: u64,
    pub(crate) tick_limit: usize,
    pub(crate) lease_millis: u64,
    pub(crate) shutdown_drain_enabled: bool,
    pub(crate) shutdown_drain_timeout_millis: u64,
    pub(crate) started_at_millis: u64,
    pub(crate) last_tick_at_millis: Option<u64>,
    pub(crate) last_tick_started_at_millis: Option<u64>,
    pub(crate) last_tick_completed_at_millis: Option<u64>,
    pub(crate) last_tick_duration_millis: Option<u64>,
    pub(crate) last_outcome: Option<String>,
    pub(crate) phases: DurableRecoverySchedulerPhaseStatuses,
    pub(crate) last_error: Option<String>,
    pub(crate) shutdown_drain: Option<DurableRecoverySchedulerDrainStatus>,
}

impl DurableRecoverySchedulerStatus {
    fn new(started_at_millis: u64, config: DurableRecoverySchedulerConfig) -> Self {
        Self {
            enabled: config.enabled,
            state: config.state,
            interval_millis: config.interval_millis(),
            tick_limit: config.tick_limit,
            lease_millis: config.lease_millis(),
            shutdown_drain_enabled: config.shutdown_drain_enabled,
            shutdown_drain_timeout_millis: config.shutdown_drain_timeout_millis(),
            started_at_millis,
            last_tick_at_millis: None,
            last_tick_started_at_millis: None,
            last_tick_completed_at_millis: None,
            last_tick_duration_millis: None,
            last_outcome: None,
            phases: DurableRecoverySchedulerPhaseStatuses::default(),
            last_error: None,
            shutdown_drain: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DurableRecoverySchedulerDrainStatus {
    pub(crate) started_at_millis: u64,
    pub(crate) completed_at_millis: Option<u64>,
    pub(crate) timeout_millis: u64,
    pub(crate) timed_out: bool,
    pub(crate) outcome: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DurableRecoverySchedulerPhaseStatuses {
    pub(crate) pre_visibility: DurableRecoverySchedulerPhaseStatus,
    pub(crate) post_cas: DurableRecoverySchedulerPhaseStatus,
    pub(crate) fs_mutations: DurableRecoverySchedulerPhaseStatus,
    pub(crate) object_cleanup: DurableRecoverySchedulerPhaseStatus,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DurableRecoverySchedulerPhaseStatus {
    pub(crate) attempted: Option<usize>,
    pub(crate) completed: Option<usize>,
    pub(crate) backing_off: Option<usize>,
    pub(crate) poisoned: Option<usize>,
    pub(crate) skipped: Option<usize>,
    pub(crate) deletion_ready: Option<usize>,
    pub(crate) deletion_held: Option<usize>,
    pub(crate) deleted_final_objects: Option<usize>,
    pub(crate) deferred: Option<usize>,
}

fn current_unix_timestamp_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DurableRecoverySchedulerKey {
    objects: usize,
    object_metadata: usize,
    pre_visibility_recovery: usize,
    post_cas_recovery: usize,
    commits: usize,
    refs: usize,
    idempotency: usize,
    workspace_metadata: usize,
    review: usize,
    audit: usize,
    fs_mutation_recovery: usize,
    object_cleanup: usize,
    repo_id: String,
}

fn durable_recovery_scheduler_key(
    stores: &StratumStores,
    repo_id: &RepoId,
) -> DurableRecoverySchedulerKey {
    DurableRecoverySchedulerKey {
        objects: arc_trait_object_key(&stores.objects),
        object_metadata: arc_trait_object_key(&stores.object_metadata),
        pre_visibility_recovery: arc_trait_object_key(&stores.pre_visibility_recovery),
        post_cas_recovery: arc_trait_object_key(&stores.post_cas_recovery),
        commits: arc_trait_object_key(&stores.commits),
        refs: arc_trait_object_key(&stores.refs),
        idempotency: arc_trait_object_key(&stores.idempotency),
        workspace_metadata: arc_trait_object_key(&stores.workspace_metadata),
        review: arc_trait_object_key(&stores.review),
        audit: arc_trait_object_key(&stores.audit),
        fs_mutation_recovery: arc_trait_object_key(&stores.fs_mutation_recovery),
        object_cleanup: arc_trait_object_key(&stores.object_cleanup),
        repo_id: repo_id.as_str().to_string(),
    }
}

fn arc_trait_object_key<T: ?Sized>(value: &Arc<T>) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    Arc::as_ptr(value).hash(&mut hasher);
    hasher.finish() as usize
}

async fn durable_recovery_scheduler_loop(
    repo_id: RepoId,
    stores: StratumStores,
    config: DurableRecoverySchedulerConfig,
    status: Arc<Mutex<DurableRecoverySchedulerStatus>>,
    tick_mutex: Arc<AsyncMutex<()>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        {
            let _tick_guard = tick_mutex.lock().await;
            if *shutdown_rx.borrow() {
                break;
            }
            let _ = durable_recovery_scheduler_tick(&repo_id, &stores, config, &status).await;
        }
        tokio::select! {
            _ = tokio::time::sleep(config.interval) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }
}

struct DurableRecoverySchedulerDrainContext {
    repo_id: RepoId,
    stores: StratumStores,
    config: DurableRecoverySchedulerConfig,
    status: Arc<Mutex<DurableRecoverySchedulerStatus>>,
    tick_mutex: Arc<AsyncMutex<()>>,
    stop_rx: watch::Receiver<bool>,
    timed_out: Arc<AtomicBool>,
    deadline: tokio::time::Instant,
}

async fn durable_recovery_scheduler_drain_until_idle(
    context: DurableRecoverySchedulerDrainContext,
) -> String {
    let DurableRecoverySchedulerDrainContext {
        repo_id,
        stores,
        config,
        status,
        tick_mutex,
        mut stop_rx,
        timed_out,
        deadline,
    } = context;
    loop {
        if timed_out.load(Ordering::SeqCst) || *stop_rx.borrow() {
            return "stopped".to_string();
        }
        if tokio::time::Instant::now() >= deadline {
            timed_out.store(true, Ordering::SeqCst);
            return "timed_out".to_string();
        }
        let result = tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                timed_out.store(true, Ordering::SeqCst);
                return "timed_out".to_string();
            }
            tick_guard = tick_mutex.lock() => {
                let _tick_guard = tick_guard;
                if timed_out.load(Ordering::SeqCst) || *stop_rx.borrow() {
                    return "stopped".to_string();
                }
                if tokio::time::Instant::now() >= deadline {
                    timed_out.store(true, Ordering::SeqCst);
                    return "timed_out".to_string();
                }
                durable_recovery_scheduler_tick(&repo_id, &stores, config, &status).await
            }
            changed = stop_rx.changed() => {
                if changed.is_err() || timed_out.load(Ordering::SeqCst) || *stop_rx.borrow() {
                    return "stopped".to_string();
                }
                continue;
            }
        };
        if tokio::time::Instant::now() >= deadline {
            timed_out.store(true, Ordering::SeqCst);
            return result.outcome;
        }
        if result.attempted == 0 {
            return result.outcome;
        }
        if timed_out.load(Ordering::SeqCst) || *stop_rx.borrow() {
            return result.outcome;
        }
        tokio::task::yield_now().await;
    }
}

fn finish_shutdown_drain_worker_timed_out_status(
    status: &mut DurableRecoverySchedulerStatus,
    started_at_millis: u64,
    config: DurableRecoverySchedulerConfig,
) -> DurableRecoverySchedulerDrainStatus {
    let existing_timed_out_status = status
        .shutdown_drain
        .as_ref()
        .is_some_and(|drain| drain.started_at_millis == started_at_millis && drain.timed_out);
    status.enabled = config.enabled;
    status.state = DurableRecoverySchedulerState::Stopped;
    if existing_timed_out_status {
        return status
            .shutdown_drain
            .clone()
            .expect("checked shutdown drain status");
    }

    let drain = DurableRecoverySchedulerDrainStatus {
        started_at_millis,
        completed_at_millis: Some(current_unix_timestamp_millis()),
        timeout_millis: config.shutdown_drain_timeout_millis(),
        timed_out: true,
        outcome: Some("timed_out".to_string()),
    };
    status.shutdown_drain = Some(drain.clone());
    drain
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DurableRecoverySchedulerTickResult {
    attempted: usize,
    outcome: String,
}

async fn durable_recovery_scheduler_tick(
    repo_id: &RepoId,
    stores: &StratumStores,
    config: DurableRecoverySchedulerConfig,
    status: &Arc<Mutex<DurableRecoverySchedulerStatus>>,
) -> DurableRecoverySchedulerTickResult {
    let started_at_millis = current_unix_timestamp_millis();
    let current_status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let mut tick_status = DurableRecoverySchedulerStatus {
        started_at_millis: current_status.started_at_millis,
        enabled: config.enabled,
        state: if current_status.state == DurableRecoverySchedulerState::Draining {
            DurableRecoverySchedulerState::Draining
        } else {
            config.state
        },
        interval_millis: config.interval_millis(),
        tick_limit: config.tick_limit,
        lease_millis: config.lease_millis(),
        shutdown_drain_enabled: config.shutdown_drain_enabled,
        shutdown_drain_timeout_millis: config.shutdown_drain_timeout_millis(),
        last_tick_at_millis: None,
        last_tick_started_at_millis: Some(started_at_millis),
        last_tick_completed_at_millis: None,
        last_tick_duration_millis: None,
        last_outcome: None,
        phases: DurableRecoverySchedulerPhaseStatuses::default(),
        last_error: None,
        shutdown_drain: current_status.shutdown_drain,
    };
    let mut phase_failures = 0usize;
    let mut last_error = None;
    let pre_visibility_runner = DurableCorePreVisibilityRecoveryRun::new(
        DurableCorePreVisibilityRecoveryRunStores::new(
            stores.pre_visibility_recovery.as_ref(),
            stores.post_cas_recovery.as_ref(),
            stores.commits.as_ref(),
            stores.refs.as_ref(),
            stores.idempotency.as_ref(),
        ),
        DURABLE_RECOVERY_SCHEDULER_COMMIT_LEASE_OWNER,
        config.lease_duration,
        config.tick_limit,
    );
    let pre_visibility_attempted = match pre_visibility_runner.run().await {
        Ok(summary) => {
            tick_status.phases.pre_visibility =
                DurableRecoverySchedulerPhaseStatus::from_pre_visibility_summary(&summary);
            summary.attempted()
        }
        Err(_) => {
            tracing::debug!("durable recovery scheduler pre-visibility phase failed");
            phase_failures += 1;
            last_error = Some("pre_visibility_failed".to_string());
            0
        }
    };

    let post_cas_limit = config.tick_limit.saturating_sub(pre_visibility_attempted);
    let post_cas_worker = DurableCorePostCasRepairWorker::new(
        DurableCorePostCasRepairWorkerStores::new(
            stores.post_cas_recovery.as_ref(),
            stores.commits.as_ref(),
            stores.workspace_metadata.as_ref(),
            stores.audit.as_ref(),
            stores.idempotency.as_ref(),
        ),
        DURABLE_RECOVERY_SCHEDULER_COMMIT_LEASE_OWNER,
        config.lease_duration,
        post_cas_limit,
    );
    let post_cas_attempted = match post_cas_worker.run().await {
        Ok(summary) => {
            tick_status.phases.post_cas =
                DurableRecoverySchedulerPhaseStatus::from_post_cas_summary(&summary);
            summary.attempted()
        }
        Err(_) => {
            tracing::debug!("durable recovery scheduler post-CAS phase failed");
            phase_failures += 1;
            last_error = Some("post_cas_failed".to_string());
            0
        }
    };

    let fs_mutation_limit = post_cas_limit.saturating_sub(post_cas_attempted);
    let fs_mutation_worker = DurableFsMutationRecoveryWorker::new(
        stores.fs_mutation_recovery.as_ref(),
        stores.audit.as_ref(),
        stores.idempotency.as_ref(),
        Some(stores.workspace_metadata.as_ref()),
        DURABLE_RECOVERY_SCHEDULER_FS_LEASE_OWNER,
        config.lease_duration,
        fs_mutation_limit,
    );
    let fs_mutation_attempted = match fs_mutation_worker.run().await {
        Ok(summary) => {
            tick_status.phases.fs_mutations =
                DurableRecoverySchedulerPhaseStatus::from_fs_mutation_summary(&summary);
            summary.attempted()
        }
        Err(_) => {
            tracing::debug!("durable recovery scheduler FS mutation phase failed");
            phase_failures += 1;
            last_error = Some("fs_mutations_failed".to_string());
            0
        }
    };

    let object_cleanup_limit = fs_mutation_limit.saturating_sub(fs_mutation_attempted);
    // Idempotency retention sweeping is intentionally not scheduled here until
    // the scheduler has an explicit runtime policy source for this store set.
    let object_cleanup_worker = ObjectCleanupWorker::new(
        repo_id,
        stores.objects.as_ref(),
        stores.object_metadata.as_ref(),
        stores.commits.as_ref(),
        stores.refs.as_ref(),
        stores.workspace_metadata.as_ref(),
        stores.review.as_ref(),
        stores.idempotency.as_ref(),
        stores.post_cas_recovery.as_ref(),
        stores.pre_visibility_recovery.as_ref(),
        stores.fs_mutation_recovery.as_ref(),
        stores.object_cleanup.as_ref(),
    );
    let object_cleanup_attempted = match object_cleanup_worker.run_once(object_cleanup_limit).await
    {
        Ok(summary) => {
            let attempted = summary
                .processed
                .saturating_sub(summary.deletion_held)
                .saturating_sub(summary.poisoned);
            tick_status.phases.object_cleanup =
                DurableRecoverySchedulerPhaseStatus::from_object_cleanup_summary(&summary);
            attempted
        }
        Err(_) => {
            tracing::debug!("durable recovery scheduler object cleanup phase failed");
            phase_failures += 1;
            last_error = Some("object_cleanup_failed".to_string());
            0
        }
    };
    tick_status.last_error = last_error;
    let outcome = match phase_failures {
        0 => "completed",
        4 => "failed",
        _ => "partial_failure",
    }
    .to_string();
    tick_status.last_outcome = Some(outcome.clone());
    let completed_at_millis = current_unix_timestamp_millis();
    tick_status.last_tick_at_millis = Some(completed_at_millis);
    tick_status.last_tick_completed_at_millis = Some(completed_at_millis);
    tick_status.last_tick_duration_millis =
        Some(completed_at_millis.saturating_sub(started_at_millis));
    let mut current_status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if current_status.shutdown_drain.is_some() {
        tick_status.shutdown_drain = current_status.shutdown_drain.clone();
    }
    if matches!(
        current_status.state,
        DurableRecoverySchedulerState::Draining | DurableRecoverySchedulerState::Stopped
    ) {
        tick_status.state = current_status.state;
    }
    *current_status = tick_status;
    DurableRecoverySchedulerTickResult {
        attempted: pre_visibility_attempted
            .saturating_add(post_cas_attempted)
            .saturating_add(fs_mutation_attempted)
            .saturating_add(object_cleanup_attempted),
        outcome,
    }
}

impl DurableRecoverySchedulerPhaseStatus {
    fn from_pre_visibility_summary(
        summary: &crate::backend::core_transaction::DurableCorePreVisibilityRecoveryRunSummary,
    ) -> Self {
        Self {
            attempted: Some(summary.attempted()),
            completed: Some(summary.resolved()),
            backing_off: Some(summary.backing_off()),
            poisoned: Some(summary.poisoned()),
            skipped: Some(summary.skipped()),
            deletion_ready: None,
            deletion_held: None,
            deleted_final_objects: None,
            deferred: None,
        }
    }

    fn from_post_cas_summary(
        summary: &crate::backend::core_transaction::DurableCorePostCasRepairWorkerSummary,
    ) -> Self {
        Self {
            attempted: Some(summary.attempted()),
            completed: Some(summary.completed()),
            backing_off: Some(summary.backing_off()),
            poisoned: Some(summary.poisoned()),
            skipped: Some(summary.skipped()),
            deletion_ready: None,
            deletion_held: None,
            deleted_final_objects: None,
            deferred: None,
        }
    }

    fn from_fs_mutation_summary(
        summary: &crate::backend::core_transaction::DurableFsMutationRecoveryWorkerSummary,
    ) -> Self {
        Self {
            attempted: Some(summary.attempted()),
            completed: Some(summary.completed()),
            backing_off: Some(summary.backing_off()),
            poisoned: Some(summary.poisoned()),
            skipped: Some(summary.skipped()),
            deletion_ready: None,
            deletion_held: None,
            deleted_final_objects: None,
            deferred: None,
        }
    }

    fn from_object_cleanup_summary(
        summary: &crate::backend::object_cleanup::ObjectCleanupWorkerSummary,
    ) -> Self {
        let skipped = summary.skipped_non_cas_lost
            + summary.skipped_reachable
            + summary.skipped_blocked
            + summary.skipped_claim_unavailable;
        Self {
            attempted: Some(summary.processed),
            completed: Some(summary.deleted_final_objects),
            backing_off: Some(summary.retryable_failures),
            poisoned: Some(summary.poisoned),
            skipped: Some(skipped),
            deletion_ready: Some(summary.deletion_ready),
            deletion_held: Some(summary.deletion_held),
            deleted_final_objects: Some(summary.deleted_final_objects),
            deferred: Some(summary.deferred),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditAction, AuditEvent, AuditStore, NewAuditEvent};
    use crate::backend::ObjectWrite;
    use crate::backend::blob_object::ObjectMetadataRecord;
    use crate::backend::core_transaction::{
        DurableFsMutationAuditRecoveryContext, DurableFsMutationRecoveryEnvelope,
        DurableFsMutationRecoveryStep, DurableFsMutationRecoveryTarget,
    };
    use crate::backend::object_cleanup::{ObjectCleanupClaimKind, ObjectCleanupClaimRequest};
    #[cfg(feature = "postgres")]
    use crate::backend::runtime::PostgresSecretProvider;
    use crate::backend::runtime::{
        BACKEND_ENV, CORE_RUNTIME_ENV, DURABLE_AUTH_SESSION_READY_ENV, DURABLE_CORE_REPO_ID_ENV,
        DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV, DURABLE_POLICY_READY_ENV, DURABLE_RECOVERY_READY_ENV,
        DURABLE_REPO_ROUTING_READY_ENV, IDEMPOTENCY_COMPLETED_RETENTION_SECONDS_ENV,
        IDEMPOTENCY_MAX_RECORDS_PER_SCOPE_ENV, IDEMPOTENCY_PENDING_STALE_SECONDS_ENV,
        POSTGRES_CONNECT_TIMEOUT_MS_ENV, POSTGRES_OPERATION_TIMEOUT_MS_ENV,
        POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV, POSTGRES_POOL_MAX_SIZE_ENV, POSTGRES_URL_ENV,
        R2_ACCESS_KEY_ID_ENV, R2_BUCKET_ENV, R2_CONNECT_TIMEOUT_MS_ENV, R2_ENDPOINT_ENV,
        R2_MAX_ATTEMPTS_ENV, R2_REQUEST_TIMEOUT_MS_ENV, R2_RETRY_BASE_DELAY_MS_ENV,
        R2_RETRY_MAX_DELAY_MS_ENV, R2_SECRET_ACCESS_KEY_ENV, RECOVERY_SCHEDULER_ENV,
        RECOVERY_SCHEDULER_INTERVAL_MS_ENV, RECOVERY_SCHEDULER_LEASE_MS_ENV,
        RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV,
        RECOVERY_SCHEDULER_TICK_LIMIT_ENV,
    };
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::CommitId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Barrier, Notify};
    use uuid::Uuid;

    fn commit_id(label: &str) -> CommitId {
        CommitId::from(ObjectId::from_bytes(label.as_bytes()))
    }

    fn recovery_scheduler_config_from_pairs(
        pairs: &[(&str, &str)],
    ) -> RecoverySchedulerRuntimeConfig {
        RecoverySchedulerRuntimeConfig::from_lookup(|name| {
            pairs
                .iter()
                .find_map(|(key, value)| (*key == name).then(|| (*value).to_string()))
        })
        .expect("valid scheduler config")
    }

    async fn spawn_test_router(router: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test router");
        let addr = listener.local_addr().expect("test listener has address");
        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve test router");
        });
        (format!("http://{addr}"), handle)
    }

    struct BlockingAuditStore {
        inner: InMemoryAuditStore,
        append_started: Barrier,
        append_release: Notify,
        append_attempts: AtomicUsize,
    }

    impl BlockingAuditStore {
        fn new() -> Self {
            Self {
                inner: InMemoryAuditStore::new(),
                append_started: Barrier::new(2),
                append_release: Notify::new(),
                append_attempts: AtomicUsize::new(0),
            }
        }

        async fn wait_for_append_attempt(&self) {
            self.append_started.wait().await;
        }

        fn release_append(&self) {
            self.append_release.notify_one();
        }

        fn append_attempts(&self) -> usize {
            self.append_attempts.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AuditStore for BlockingAuditStore {
        async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
            self.append_attempts.fetch_add(1, Ordering::SeqCst);
            self.append_started.wait().await;
            self.append_release.notified().await;
            self.inner.append(event).await
        }

        async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
            self.inner.list_recent(limit).await
        }

        async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError> {
            self.inner.contains_vcs_commit_event(commit_id).await
        }

        async fn contains_fs_mutation_recovery_event(
            &self,
            action: AuditAction,
            operation_id: &str,
            target_ref: &str,
            new_commit: &str,
        ) -> Result<bool, VfsError> {
            self.inner
                .contains_fs_mutation_recovery_event(action, operation_id, target_ref, new_commit)
                .await
        }
    }

    #[test]
    fn durable_cloud_state_can_be_constructed_without_local_db_and_requires_repo() {
        let stores = StratumStores::local_memory();
        let state = ServerState {
            core: Arc::new(crate::server::core::DurableCoreRuntime::new(
                RepoId::new("repo_durable_state").expect("valid repo id"),
                stores.clone(),
            )),
            db: ServerLocalDb::unavailable(),
            workspaces: stores.workspace_metadata,
            idempotency: stores.idempotency,
            audit: stores.audit,
            review: stores.review,
            secret_replay_kms: None,
        };

        assert!(!state.db.is_available());
        assert!(state.requires_explicit_workspace_repo());
    }

    #[tokio::test]
    async fn open_server_stores_rejects_unsupported_core_before_local_files() {
        let data_dir =
            std::env::temp_dir().join(format!("stratum-open-server-stores-{}", Uuid::new_v4()));
        let err = BackendRuntimeConfig::from_lookup(|name| match name {
            CORE_RUNTIME_ENV => Some("durable-cloud".to_string()),
            _ => None,
        })
        .expect_err("durable-cloud should fail before server store opening without gates");

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(err.to_string().contains(CORE_RUNTIME_ENV));
        assert!(err.to_string().contains(BACKEND_ENV));
        assert!(!data_dir.join(".vfs").exists());
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn open_server_stores_uses_injected_secret_provider() {
        struct FailingSecretProvider;

        impl PostgresSecretProvider for FailingSecretProvider {
            fn postgres_password(&self) -> Result<Option<String>, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "raw-store-secret-123".to_string(),
                })
            }
        }

        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            BACKEND_ENV => Some("durable".to_string()),
            POSTGRES_URL_ENV => Some("postgresql://localhost/stratum".to_string()),
            R2_BUCKET_ENV => Some("stratum".to_string()),
            R2_ENDPOINT_ENV => Some("https://example.invalid".to_string()),
            R2_ACCESS_KEY_ID_ENV => Some("test-access-key".to_string()),
            R2_SECRET_ACCESS_KEY_ENV => Some("test-secret-key".to_string()),
            _ => None,
        })
        .expect("durable runtime should parse");
        let config = Config::default().with_data_dir(
            std::env::temp_dir().join(format!("stratum-secret-provider-{}", Uuid::new_v4())),
        );

        let result = open_server_stores_for_runtime_with_secret_provider(
            &runtime,
            &config,
            &FailingSecretProvider,
        )
        .await;
        let err = match result {
            Ok(_) => panic!("store opening should use injected provider"),
            Err(error) => error,
        };
        let message = err.to_string();

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains("postgres secret resolution failed"));
        assert!(!message.contains("raw-store-secret-123"));
    }

    #[test]
    fn open_server_stores_preserves_local_guarded_durable_commit_behavior() {
        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            BACKEND_ENV => Some("local".to_string()),
            _ => None,
        })
        .expect("local runtime should parse");

        let stores = ServerStores {
            backend_mode: BackendRuntimeMode::Local,
            workspaces: Arc::new(crate::workspace::InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(crate::idempotency::InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            secret_replay_kms: None,
            guarded_durable_commit_stores: None,
            durable_core_stores: None,
        };

        assert_eq!(runtime.core_runtime_mode(), CoreRuntimeMode::LocalState);
        assert!(stores.guarded_durable_commit_stores.is_none());
        assert!(stores.durable_core_stores.is_none());
    }

    #[tokio::test]
    async fn build_durable_core_router_uses_durable_runtime_without_local_db() {
        let stores = StratumStores::local_memory();
        let server_stores = ServerStores {
            backend_mode: BackendRuntimeMode::Durable,
            workspaces: stores.workspace_metadata.clone(),
            idempotency: stores.idempotency.clone(),
            audit: stores.audit.clone(),
            review: stores.review.clone(),
            secret_replay_kms: None,
            guarded_durable_commit_stores: None,
            durable_core_stores: Some(stores),
        };
        let router = build_durable_core_router(
            server_stores,
            RepoId::new("repo_durable_router").expect("valid repo id"),
        );

        let (base_url, server) = spawn_test_router(router).await;
        let response = reqwest::Client::new()
            .get(format!("{base_url}/health"))
            .send()
            .await
            .expect("health request should complete");
        let status = response.status();
        let body: serde_json::Value = response.json().await.expect("health body is json");
        server.abort();

        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(body["core_runtime"], "durable-cloud");
        assert!(body["commits"].is_null());
    }

    #[tokio::test]
    async fn durable_core_router_returns_stable_501_for_unsupported_groups() {
        let stores = StratumStores::local_memory();
        let router = build_durable_core_router(
            ServerStores {
                backend_mode: BackendRuntimeMode::Durable,
                workspaces: stores.workspace_metadata.clone(),
                idempotency: stores.idempotency.clone(),
                audit: stores.audit.clone(),
                review: stores.review.clone(),
                secret_replay_kms: None,
                guarded_durable_commit_stores: None,
                durable_core_stores: Some(stores),
            },
            RepoId::new("repo_durable_unsupported").expect("valid repo id"),
        );
        let unsupported = [
            (reqwest::Method::POST, "/auth/login"),
            (reqwest::Method::POST, "/runs"),
            (reqwest::Method::GET, "/audit"),
            (reqwest::Method::GET, "/workspaces"),
            (reqwest::Method::GET, "/vcs/recovery"),
            (reqwest::Method::POST, "/vcs/recovery/run"),
        ];
        let (base_url, server) = spawn_test_router(router).await;
        let client = reqwest::Client::new();

        for (method, path) in unsupported {
            let response = client
                .request(method, format!("{base_url}{path}"))
                .send()
                .await
                .expect("request should complete");
            let status = response.status();
            let body: serde_json::Value = response.json().await.expect("unsupported body is json");

            assert_eq!(status, reqwest::StatusCode::NOT_IMPLEMENTED, "{path}");
            assert_eq!(
                body,
                serde_json::json!({
                    "error": "stratum: operation not supported: durable-cloud route is not supported yet"
                }),
                "{path}"
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_drains_fs_mutation_work_without_manual_run() {
        let stores = StratumStores::local_memory();
        let target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "workspace:background-demo",
            "op-background-demo",
            "agent/demo/session",
            commit_id("background-previous"),
            commit_id("background-new"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .expect("valid FS mutation recovery target");
        stores
            .fs_mutation_recovery
            .enqueue(
                target,
                DurableFsMutationRecoveryEnvelope::new(
                    None,
                    Some(
                        DurableFsMutationAuditRecoveryContext::new(
                            AuditAction::FsWriteFile,
                            &["/docs/background.md"],
                        )
                        .expect("valid recovery audit context"),
                    ),
                    None,
                ),
                1,
            )
            .await
            .expect("enqueue FS mutation recovery");

        let _router = build_router_with_config(ServerRouterConfig {
            db: StratumDb::open_memory(),
            backend_mode: BackendRuntimeMode::Durable,
            workspaces: stores.workspace_metadata.clone(),
            idempotency: stores.idempotency.clone(),
            audit: stores.audit.clone(),
            review: stores.review.clone(),
            secret_replay_kms: None,
            recovery_scheduler: RecoverySchedulerRuntimeConfig::default(),
            guarded_durable_commit_stores: Some(stores.clone()),
        })
        .0;

        for _ in 0..40 {
            if stores
                .fs_mutation_recovery
                .counts()
                .await
                .expect("load FS mutation recovery counts")
                .completed()
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let counts = stores
            .fs_mutation_recovery
            .counts()
            .await
            .expect("load final FS mutation recovery counts");
        assert_eq!(counts.completed(), 1);
        assert_eq!(
            stores
                .audit
                .list_recent(10)
                .await
                .expect("list audit events")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_concurrent_ticks_fence_fs_mutation_side_effects() {
        let mut stores = StratumStores::local_memory();
        let audit = Arc::new(BlockingAuditStore::new());
        stores.audit = audit.clone();
        let target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "workspace:concurrent-demo",
            "op-concurrent-demo",
            "agent/concurrent/session",
            commit_id("concurrent-previous"),
            commit_id("concurrent-new"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .expect("valid FS mutation recovery target");
        stores
            .fs_mutation_recovery
            .enqueue(
                target,
                DurableFsMutationRecoveryEnvelope::new(
                    None,
                    Some(
                        DurableFsMutationAuditRecoveryContext::new(
                            AuditAction::FsWriteFile,
                            &["/docs/concurrent.md"],
                        )
                        .expect("valid recovery audit context"),
                    ),
                    None,
                ),
                1,
            )
            .await
            .expect("enqueue FS mutation recovery");
        let config = DurableRecoverySchedulerConfig::from_runtime(
            &RecoverySchedulerRuntimeConfig::default(),
        );
        let first_status = Arc::new(Mutex::new(DurableRecoverySchedulerStatus::new(
            current_unix_timestamp_millis(),
            config,
        )));
        let second_status = Arc::new(Mutex::new(DurableRecoverySchedulerStatus::new(
            current_unix_timestamp_millis(),
            config,
        )));

        let first_stores = stores.clone();
        let first_status_for_tick = first_status.clone();
        let first_tick = tokio::spawn(async move {
            durable_recovery_scheduler_tick(
                &RepoId::local(),
                &first_stores,
                config,
                &first_status_for_tick,
            )
            .await
        });
        tokio::time::timeout(Duration::from_millis(250), audit.wait_for_append_attempt())
            .await
            .expect("first scheduler tick should reach audit append");

        let second_stores = stores.clone();
        let second_status_for_tick = second_status.clone();
        let second_tick = tokio::spawn(async move {
            durable_recovery_scheduler_tick(
                &RepoId::local(),
                &second_stores,
                config,
                &second_status_for_tick,
            )
            .await
        });
        let second_result = tokio::time::timeout(Duration::from_millis(250), second_tick)
            .await
            .expect("second scheduler tick should not block behind active lease")
            .expect("second scheduler tick should join");
        audit.release_append();
        let first_result = first_tick.await.expect("first scheduler tick should join");

        assert_eq!(first_result.attempted, 1);
        assert_eq!(second_result.attempted, 0);
        assert_eq!(audit.append_attempts(), 1);
        assert_eq!(
            stores
                .audit
                .list_recent(10)
                .await
                .expect("list audit events")
                .len(),
            1
        );
        assert_eq!(
            stores
                .fs_mutation_recovery
                .counts()
                .await
                .expect("load FS mutation recovery counts")
                .completed(),
            1
        );
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_drains_object_cleanup_phase_bounded() {
        let stores = StratumStores::local_memory();
        for index in 0..2 {
            let bytes = format!("background cleanup object {index}").into_bytes();
            let size = bytes.len() as u64;
            let object_id = ObjectId::from_bytes(&bytes);
            stores
                .objects
                .put(ObjectWrite {
                    repo_id: RepoId::local(),
                    id: object_id,
                    kind: ObjectKind::Blob,
                    bytes,
                })
                .await
                .expect("write cleanup object");
            stores
                .object_metadata
                .put(ObjectMetadataRecord::new(
                    RepoId::local(),
                    object_id,
                    ObjectKind::Blob,
                    size,
                ))
                .await
                .expect("write cleanup object metadata");
            let cleanup_claim = stores
                .object_cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: RepoId::local(),
                    claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    object_kind: ObjectKind::Blob,
                    object_id,
                    object_key: crate::backend::object_cleanup::canonical_final_object_key(
                        &RepoId::local(),
                        ObjectKind::Blob,
                        &object_id,
                    ),
                    lease_owner: "scheduler-object-cleanup-test".to_string(),
                    lease_duration: Duration::from_secs(60),
                })
                .await
                .expect("claim cleanup object")
                .expect("cleanup object claim");
            stores
                .object_cleanup
                .record_failure(&cleanup_claim, "make claim retryable")
                .await
                .expect("mark cleanup claim retryable");
            stores
                .object_cleanup
                .release(&cleanup_claim)
                .await
                .expect("release cleanup claim for deterministic retry");
        }

        let scheduler = start_durable_recovery_scheduler(
            stores.clone(),
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("scheduler should start");

        for _ in 0..40 {
            let status = scheduler.status();
            if status.phases.object_cleanup.deletion_ready == Some(2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let status = scheduler.status();
        assert_eq!(status.phases.object_cleanup.attempted, Some(2));
        assert_eq!(status.phases.object_cleanup.completed, Some(0));
        assert_eq!(status.phases.object_cleanup.deleted_final_objects, Some(0));
        assert_eq!(status.phases.object_cleanup.deletion_ready, Some(2));
        assert_eq!(status.phases.object_cleanup.deferred, Some(0));
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_starts_once_per_store_set() {
        let stores = StratumStores::local_memory();

        let handle = start_durable_recovery_scheduler(
            stores.clone(),
            RecoverySchedulerRuntimeConfig::default(),
        );
        let handle = handle.expect("scheduler should start");
        let duplicate = start_durable_recovery_scheduler(
            stores.clone(),
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("duplicate store set should reuse scheduler handle");
        assert!(Arc::ptr_eq(&handle, &duplicate));
        assert_eq!(
            handle.status().started_at_millis,
            duplicate.status().started_at_millis
        );
        drop(handle);
        assert!(
            start_durable_recovery_scheduler(
                stores.clone(),
                RecoverySchedulerRuntimeConfig::default()
            )
            .is_some()
        );
        drop(duplicate);
        assert!(
            start_durable_recovery_scheduler(stores, RecoverySchedulerRuntimeConfig::default())
                .is_some()
        );
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_disabled_status_has_no_background_tick() {
        let stores = StratumStores::local_memory();
        let config = recovery_scheduler_config_from_pairs(&[(RECOVERY_SCHEDULER_ENV, "disabled")]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), config).expect("scheduler attached");

        tokio::time::sleep(Duration::from_millis(75)).await;

        let status = handle.status();
        assert!(!status.enabled);
        assert_eq!(status.state, DurableRecoverySchedulerState::Disabled);
        assert_eq!(status.interval_millis, 5_000);
        assert_eq!(status.tick_limit, 10);
        assert_eq!(status.lease_millis, 30_000);
        assert!(!status.shutdown_drain_enabled);
        assert_eq!(status.shutdown_drain_timeout_millis, 2_500);
        assert_eq!(status.last_tick_at_millis, None);
        assert_eq!(status.last_tick_started_at_millis, None);
        assert_eq!(status.last_tick_completed_at_millis, None);
        assert_eq!(status.last_tick_duration_millis, None);
        assert!(
            handle
                .task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        );
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_disabled_shutdown_drain_records_skipped() {
        let stores = StratumStores::local_memory();
        let config = recovery_scheduler_config_from_pairs(&[
            (RECOVERY_SCHEDULER_ENV, "disabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
        ]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), config).expect("scheduler attached");

        let drain =
            tokio::time::timeout(Duration::from_millis(100), handle.request_shutdown_drain())
                .await
                .expect("disabled drain should return quickly");

        assert!(!drain.timed_out);
        assert_eq!(drain.outcome.as_deref(), Some("skipped_disabled"));
        assert!(drain.completed_at_millis.is_some());
        let status = handle.status();
        assert_eq!(status.state, DurableRecoverySchedulerState::Stopped);
        assert_eq!(status.last_tick_at_millis, None);
        assert_eq!(status.shutdown_drain, Some(drain));
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_disabled_handle_can_be_enabled_later() {
        let stores = StratumStores::local_memory();
        let disabled =
            recovery_scheduler_config_from_pairs(&[(RECOVERY_SCHEDULER_ENV, "disabled")]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), disabled).expect("scheduler attached");
        assert!(!handle.status().enabled);
        assert!(
            handle
                .task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        );

        let enabled = start_durable_recovery_scheduler(
            stores.clone(),
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("scheduler should start after disabled attach");
        assert!(Arc::ptr_eq(&handle, &enabled));

        for _ in 0..40 {
            if enabled.status().last_tick_at_millis.is_some() {
                assert!(enabled.status().enabled);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        panic!("scheduler did not start after disabled attach");
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_enabled_config_status() {
        let stores = StratumStores::local_memory();
        let config = recovery_scheduler_config_from_pairs(&[
            (RECOVERY_SCHEDULER_INTERVAL_MS_ENV, "1234"),
            (RECOVERY_SCHEDULER_TICK_LIMIT_ENV, "3"),
            (RECOVERY_SCHEDULER_LEASE_MS_ENV, "4567"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "2345"),
        ]);
        let handle = start_durable_recovery_scheduler(stores.clone(), config)
            .expect("scheduler should start");

        for _ in 0..40 {
            let status = handle.status();
            if status.last_tick_at_millis.is_some() {
                assert!(status.enabled);
                assert_eq!(status.state, DurableRecoverySchedulerState::Running);
                assert_eq!(status.interval_millis, 1_234);
                assert_eq!(status.tick_limit, 3);
                assert_eq!(status.lease_millis, 4_567);
                assert!(status.shutdown_drain_enabled);
                assert_eq!(status.shutdown_drain_timeout_millis, 2_345);
                assert!(status.started_at_millis > 0);
                assert!(status.last_tick_started_at_millis.is_some());
                assert!(status.last_tick_completed_at_millis.is_some());
                assert!(status.last_tick_duration_millis.is_some());
                assert_eq!(
                    status.last_tick_at_millis,
                    status.last_tick_completed_at_millis
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        panic!("scheduler did not publish configured status");
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_shutdown_drain_completes_queued_fs_mutation_work() {
        let stores = StratumStores::local_memory();
        let config = recovery_scheduler_config_from_pairs(&[
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "1000"),
        ]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), config).expect("scheduler attached");
        for _ in 0..40 {
            if handle.status().last_tick_at_millis.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(handle.status().last_tick_at_millis.is_some());

        let target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "workspace:shutdown-drain-demo",
            "op-shutdown-drain-demo",
            "agent/drain/session",
            commit_id("shutdown-drain-previous"),
            commit_id("shutdown-drain-new"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .expect("valid FS mutation recovery target");
        stores
            .fs_mutation_recovery
            .enqueue(
                target,
                DurableFsMutationRecoveryEnvelope::new(
                    None,
                    Some(
                        DurableFsMutationAuditRecoveryContext::new(
                            AuditAction::FsWriteFile,
                            &["/docs/shutdown-drain.md"],
                        )
                        .expect("valid recovery audit context"),
                    ),
                    None,
                ),
                1,
            )
            .await
            .expect("enqueue FS mutation recovery");

        let drain = handle.request_shutdown_drain().await;

        let counts = stores
            .fs_mutation_recovery
            .counts()
            .await
            .expect("load final FS mutation recovery counts");
        assert_eq!(counts.completed(), 1);
        assert!(!drain.timed_out);
        assert_eq!(drain.outcome.as_deref(), Some("completed"));
        assert_eq!(
            handle.status().state,
            DurableRecoverySchedulerState::Stopped
        );
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_shutdown_drain_does_not_timeout_on_object_cleanup_hold_window()
     {
        let stores = StratumStores::local_memory();
        let bytes = b"shutdown drain held cleanup object".to_vec();
        let size = bytes.len() as u64;
        let object_id = ObjectId::from_bytes(&bytes);
        stores
            .objects
            .put(ObjectWrite {
                repo_id: RepoId::local(),
                id: object_id,
                kind: ObjectKind::Blob,
                bytes,
            })
            .await
            .expect("write cleanup object");
        stores
            .object_metadata
            .put(ObjectMetadataRecord::new(
                RepoId::local(),
                object_id,
                ObjectKind::Blob,
                size,
            ))
            .await
            .expect("write cleanup object metadata");
        let cleanup_claim = stores
            .object_cleanup
            .claim(ObjectCleanupClaimRequest {
                repo_id: RepoId::local(),
                claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                object_kind: ObjectKind::Blob,
                object_id,
                object_key: crate::backend::object_cleanup::canonical_final_object_key(
                    &RepoId::local(),
                    ObjectKind::Blob,
                    &object_id,
                ),
                lease_owner: "scheduler-object-cleanup-drain-test".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .expect("claim cleanup object")
            .expect("cleanup object claim");
        stores
            .object_cleanup
            .record_failure(&cleanup_claim, "make claim retryable")
            .await
            .expect("mark cleanup claim retryable");
        stores
            .object_cleanup
            .release(&cleanup_claim)
            .await
            .expect("release cleanup claim for deterministic retry");
        let config = recovery_scheduler_config_from_pairs(&[
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "1000"),
        ]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), config).expect("scheduler attached");

        for _ in 0..40 {
            if handle.status().phases.object_cleanup.deletion_ready == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            handle.status().phases.object_cleanup.deletion_ready,
            Some(1)
        );

        let drain = handle.request_shutdown_drain().await;

        assert!(!drain.timed_out);
        assert_eq!(drain.outcome.as_deref(), Some("completed"));
        let status = handle.status();
        assert_eq!(status.state, DurableRecoverySchedulerState::Stopped);
        assert_eq!(status.phases.object_cleanup.attempted, Some(1));
        assert_eq!(status.phases.object_cleanup.deletion_held, Some(1));
        assert_eq!(status.phases.object_cleanup.deferred, Some(1));
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_enabled_handle_restarts_after_shutdown_drain() {
        let stores = StratumStores::local_memory();
        let config = recovery_scheduler_config_from_pairs(&[
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "1000"),
        ]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), config).expect("scheduler attached");
        for _ in 0..40 {
            if handle.status().last_tick_at_millis.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(handle.status().last_tick_at_millis.is_some());

        let drain = handle.request_shutdown_drain().await;
        assert!(!drain.timed_out);
        assert_eq!(
            handle.status().state,
            DurableRecoverySchedulerState::Stopped
        );
        assert!(!handle.has_background_task());

        let restarted = start_durable_recovery_scheduler(
            stores.clone(),
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("scheduler should restart after drain");
        assert!(Arc::ptr_eq(&handle, &restarted));
        assert!(restarted.has_background_task());
        for _ in 0..40 {
            if restarted.status().last_tick_at_millis.is_some() {
                assert_eq!(
                    restarted.status().state,
                    DurableRecoverySchedulerState::Running
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        panic!("scheduler did not restart after drain");
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_shutdown_drain_timeout_is_bounded() {
        let stores = StratumStores::local_memory();
        let config = recovery_scheduler_config_from_pairs(&[
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "1"),
        ]);
        let handle =
            start_durable_recovery_scheduler(stores.clone(), config).expect("scheduler attached");
        for _ in 0..40 {
            if handle.status().last_tick_at_millis.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(handle.status().last_tick_at_millis.is_some());

        let target = DurableFsMutationRecoveryTarget::new(
            RepoId::local(),
            "workspace:shutdown-timeout-demo",
            "op-shutdown-timeout-demo",
            "agent/timeout/session",
            commit_id("shutdown-timeout-previous"),
            commit_id("shutdown-timeout-new"),
            DurableFsMutationRecoveryStep::AuditAppend,
        )
        .expect("valid FS mutation recovery target");
        stores
            .fs_mutation_recovery
            .enqueue(
                target,
                DurableFsMutationRecoveryEnvelope::new(
                    None,
                    Some(
                        DurableFsMutationAuditRecoveryContext::new(
                            AuditAction::FsWriteFile,
                            &["/docs/shutdown-timeout.md"],
                        )
                        .expect("valid recovery audit context"),
                    ),
                    None,
                ),
                1,
            )
            .await
            .expect("enqueue FS mutation recovery");
        let _tick_guard = handle.tick_mutex.lock().await;
        let drain = handle.request_shutdown_drain().await;

        assert!(drain.timed_out);
        assert_eq!(drain.outcome.as_deref(), Some("timed_out"));
        assert!(drain.completed_at_millis.is_some());
        assert_eq!(handle.status().shutdown_drain, Some(drain));
        assert!(
            matches!(
                handle.status().state,
                DurableRecoverySchedulerState::Draining | DurableRecoverySchedulerState::Stopped
            ),
            "timeout status may stop immediately when no tick is in flight"
        );
        drop(_tick_guard);
        for _ in 0..40 {
            if handle.status().state == DurableRecoverySchedulerState::Stopped {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let final_status = handle.status();
        assert_eq!(final_status.state, DurableRecoverySchedulerState::Stopped);
        assert!(
            final_status
                .shutdown_drain
                .as_ref()
                .expect("shutdown drain status")
                .timed_out
        );
        let counts = stores
            .fs_mutation_recovery
            .counts()
            .await
            .expect("load final FS mutation recovery counts");
        assert_eq!(counts.completed(), 0);
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_same_store_different_repo_has_distinct_handle() {
        let stores = StratumStores::local_memory();
        let repo_a = RepoId::new("repo_scheduler_a").expect("valid repo id");
        let repo_b = RepoId::new("repo_scheduler_b").expect("valid repo id");

        let handle_a = start_durable_recovery_scheduler_for_repo(
            stores.clone(),
            repo_a,
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("repo a scheduler should start");
        let handle_b = start_durable_recovery_scheduler_for_repo(
            stores,
            repo_b,
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("repo b scheduler should start");

        assert!(!Arc::ptr_eq(&handle_a, &handle_b));
        assert_ne!(handle_a.key.repo_id, handle_b.key.repo_id);
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_status_records_last_tick_outcome() {
        let stores = StratumStores::local_memory();
        let handle = start_durable_recovery_scheduler(
            stores.clone(),
            RecoverySchedulerRuntimeConfig::default(),
        )
        .expect("scheduler should start");

        for _ in 0..40 {
            let status = handle.status();
            if status.last_tick_at_millis.is_some() {
                assert_eq!(status.last_outcome.as_deref(), Some("completed"));
                assert_eq!(status.last_error, None);
                assert!(status.phases.pre_visibility.attempted.is_some());
                assert!(status.phases.post_cas.attempted.is_some());
                assert!(status.phases.fs_mutations.attempted.is_some());
                assert!(status.phases.object_cleanup.attempted.is_some());
                assert_eq!(status.phases.object_cleanup.completed, Some(0));
                assert_eq!(status.phases.object_cleanup.deleted_final_objects, Some(0));
                assert!(status.phases.object_cleanup.deletion_ready.is_some());
                assert!(status.phases.object_cleanup.deferred.is_some());
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        panic!("scheduler did not publish a status snapshot after ticking");
    }

    #[test]
    fn open_core_db_rejects_unsupported_core_before_local_state_file() {
        let data_dir =
            std::env::temp_dir().join(format!("stratum-open-core-db-{}", Uuid::new_v4()));
        let config = Config::from_env().with_data_dir(&data_dir);
        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            BACKEND_ENV => Some("durable".to_string()),
            CORE_RUNTIME_ENV => Some("durable-cloud".to_string()),
            DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV => Some("1".to_string()),
            DURABLE_AUTH_SESSION_READY_ENV => Some("1".to_string()),
            DURABLE_POLICY_READY_ENV => Some("1".to_string()),
            DURABLE_REPO_ROUTING_READY_ENV => Some("1".to_string()),
            DURABLE_RECOVERY_READY_ENV => Some("1".to_string()),
            DURABLE_CORE_REPO_ID_ENV => Some("repo_open_core_db".to_string()),
            IDEMPOTENCY_COMPLETED_RETENTION_SECONDS_ENV => Some("86400".to_string()),
            IDEMPOTENCY_PENDING_STALE_SECONDS_ENV => Some("3600".to_string()),
            IDEMPOTENCY_MAX_RECORDS_PER_SCOPE_ENV => Some("10000".to_string()),
            POSTGRES_POOL_MAX_SIZE_ENV => Some("16".to_string()),
            POSTGRES_CONNECT_TIMEOUT_MS_ENV => Some("5000".to_string()),
            POSTGRES_OPERATION_TIMEOUT_MS_ENV => Some("30000".to_string()),
            POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV => Some("5000".to_string()),
            POSTGRES_URL_ENV => Some("postgresql://127.0.0.1/stratum".to_string()),
            R2_BUCKET_ENV => Some("stratum-test".to_string()),
            R2_ENDPOINT_ENV => Some("https://account.r2.cloudflarestorage.com".to_string()),
            R2_ACCESS_KEY_ID_ENV => Some("test-access-key-id".to_string()),
            R2_SECRET_ACCESS_KEY_ENV => Some("test-secret-access-key".to_string()),
            R2_REQUEST_TIMEOUT_MS_ENV => Some("30000".to_string()),
            R2_CONNECT_TIMEOUT_MS_ENV => Some("5000".to_string()),
            R2_MAX_ATTEMPTS_ENV => Some("3".to_string()),
            R2_RETRY_BASE_DELAY_MS_ENV => Some("100".to_string()),
            R2_RETRY_MAX_DELAY_MS_ENV => Some("5000".to_string()),
            _ => None,
        })
        .expect("core runtime config should parse");

        let err = match open_core_db_for_runtime(&runtime, config) {
            Ok(_) => panic!("unsupported durable core should reject core db opening"),
            Err(err) => err,
        };

        assert!(matches!(err, VfsError::NotSupported { .. }));
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
            R2_ENDPOINT_ENV => Some("https://example.invalid".to_string()),
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
