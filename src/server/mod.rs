pub(crate) mod core;
pub mod idempotency;
pub mod middleware;
pub(crate) mod policy;
pub(crate) mod repo_context;
pub mod routes_audit;
pub mod routes_auth;
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
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
use crate::backend::runtime::{
    BackendRuntimeConfig, BackendRuntimeMode, CoreRuntimeMode, unsupported_durable_core_runtime,
};
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
use crate::server::core::{DurableCoreRuntime, LocalCoreRuntime, SharedCoreRuntime};
use crate::workspace::{LocalWorkspaceMetadataStore, SharedWorkspaceMetadataStore};

const DURABLE_RECOVERY_SCHEDULER_INTERVAL: Duration = Duration::from_secs(5);
const DURABLE_RECOVERY_SCHEDULER_LIMIT: usize = 10;
const DURABLE_RECOVERY_SCHEDULER_LEASE_DURATION: Duration = Duration::from_secs(30);
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
}

#[derive(Clone)]
pub struct ServerLocalDb {
    db: Option<Arc<StratumDb>>,
    runtime_kind: ServerRuntimeKind,
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
        }
    }

    pub fn unavailable() -> Self {
        Self {
            db: None,
            runtime_kind: ServerRuntimeKind::DurableCloud,
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
    pub workspaces: SharedWorkspaceMetadataStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
    pub review: SharedReviewStore,
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
            workspaces: Arc::new(workspace_store),
            idempotency: Arc::new(idempotency_store),
            audit: Arc::new(audit_store),
            review: Arc::new(review_store),
            guarded_durable_commit_stores: None,
            durable_core_stores: None,
        })
    }
}

pub type AppState = Arc<ServerState>;

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
    let idempotency = runtime
        .idempotency_retention_policy()
        .map(|policy| {
            Arc::new(PolicyIdempotencyStore::new(store.clone(), policy.clone()))
                as SharedIdempotencyStore
        })
        .unwrap_or_else(|| store.clone());
    let durable_core_stores = if runtime.core_runtime_mode() == CoreRuntimeMode::DurableCloud {
        Some(open_stratum_stores_for_durable_core(store.clone(), idempotency.clone()).await?)
    } else {
        None
    };
    let guarded_durable_commit_stores = if runtime.core_runtime_mode()
        == CoreRuntimeMode::LocalState
        && runtime.guarded_durable_commit_route_enabled()
    {
        Some(open_guarded_durable_commit_stores(store.clone(), idempotency.clone()).await?)
    } else {
        None
    };

    Ok(ServerStores {
        workspaces: store.clone(),
        idempotency,
        audit: store.clone(),
        review: store,
        guarded_durable_commit_stores,
        durable_core_stores,
    })
}

#[cfg(feature = "postgres")]
async fn open_guarded_durable_commit_stores(
    store: Arc<PostgresMetadataStore>,
    idempotency: SharedIdempotencyStore,
) -> Result<StratumStores, VfsError> {
    open_stratum_stores_for_durable_core(store, idempotency).await
}

#[cfg(feature = "postgres")]
async fn open_stratum_stores_for_durable_core(
    store: Arc<PostgresMetadataStore>,
    idempotency: SharedIdempotencyStore,
) -> Result<StratumStores, VfsError> {
    let r2_config = R2BlobStoreConfig::from_env().ok_or_else(|| VfsError::InvalidArgs {
        message: "missing required R2 object-store environment variables".to_string(),
    })?;
    let blobs = Arc::new(R2BlobStore::new(r2_config).await?);
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
    build_router_with_stores_and_guarded_durable_commit(
        db,
        stores.workspaces,
        stores.idempotency,
        stores.audit,
        stores.review,
        stores.guarded_durable_commit_stores,
    )
}

pub fn build_durable_core_router(stores: ServerStores, repo_id: RepoId) -> Router {
    let durable_core_stores = stores
        .durable_core_stores
        .expect("durable core router requires durable core stores");
    let durable_recovery_scheduler =
        start_durable_recovery_scheduler_for_repo(durable_core_stores.clone(), repo_id.clone());
    let state: AppState = Arc::new(ServerState {
        core: Arc::new(DurableCoreRuntime::new(repo_id, durable_core_stores)),
        db: ServerLocalDb::unavailable(),
        workspaces: stores.workspaces,
        idempotency: stores.idempotency,
        audit: stores.audit,
        review: stores.review,
    });

    let router = Router::new()
        .merge(routes_auth::health_routes())
        .merge(routes_fs::durable_read_routes())
        .merge(routes_vcs::durable_read_routes())
        .merge(durable_unsupported_routes())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());
    if let Some(handle) = durable_recovery_scheduler {
        router.layer(Extension(handle))
    } else {
        router
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
        .route("/protected/{*path}", any(durable_cloud_route_not_supported))
        .route("/change-requests", any(durable_cloud_route_not_supported))
        .route(
            "/change-requests/{*path}",
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
    build_router_with_stores_and_guarded_durable_commit(
        db,
        workspaces,
        idempotency,
        audit,
        review,
        None,
    )
}

fn build_router_with_stores_and_guarded_durable_commit(
    db: StratumDb,
    workspaces: SharedWorkspaceMetadataStore,
    idempotency: SharedIdempotencyStore,
    audit: SharedAuditStore,
    review: SharedReviewStore,
    guarded_durable_commit_stores: Option<StratumStores>,
) -> Router {
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
    let durable_recovery_scheduler =
        recovery_scheduler_stores.and_then(start_durable_recovery_scheduler);
    let state: AppState = Arc::new(ServerState {
        core,
        db: ServerLocalDb::available(db),
        workspaces,
        idempotency,
        audit,
        review,
    });

    let router = Router::new()
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
        router.layer(Extension(handle))
    } else {
        router
    }
}

pub(crate) struct DurableRecoverySchedulerHandle {
    key: DurableRecoverySchedulerKey,
    task: Mutex<Option<JoinHandle<()>>>,
    status: Arc<Mutex<DurableRecoverySchedulerStatus>>,
}

impl DurableRecoverySchedulerHandle {
    pub(crate) fn status(&self) -> DurableRecoverySchedulerStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
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
) -> Option<Arc<DurableRecoverySchedulerHandle>> {
    start_durable_recovery_scheduler_for_repo(stores, RepoId::local())
}

fn start_durable_recovery_scheduler_for_repo(
    stores: StratumStores,
    repo_id: RepoId,
) -> Option<Arc<DurableRecoverySchedulerHandle>> {
    let key = durable_recovery_scheduler_key(&stores, &repo_id);
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("durable recovery scheduler skipped without a Tokio runtime");
        return None;
    };
    let registry = DURABLE_RECOVERY_SCHEDULERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut started = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = started.get(&key).and_then(Weak::upgrade) {
        return Some(existing);
    }
    started.remove(&key);
    let status = Arc::new(Mutex::new(DurableRecoverySchedulerStatus::new(
        current_unix_timestamp_millis(),
    )));
    let tick_status = status.clone();
    let task = handle.spawn(async move {
        loop {
            durable_recovery_scheduler_tick(&repo_id, &stores, &tick_status).await;
            tokio::time::sleep(DURABLE_RECOVERY_SCHEDULER_INTERVAL).await;
        }
    });
    let scheduler = Arc::new(DurableRecoverySchedulerHandle {
        key: key.clone(),
        task: Mutex::new(Some(task)),
        status,
    });
    started.insert(key, Arc::downgrade(&scheduler));
    Some(scheduler)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DurableRecoverySchedulerStatus {
    pub(crate) started_at_millis: u64,
    pub(crate) last_tick_at_millis: Option<u64>,
    pub(crate) last_outcome: Option<String>,
    pub(crate) phases: DurableRecoverySchedulerPhaseStatuses,
    pub(crate) last_error: Option<String>,
}

impl DurableRecoverySchedulerStatus {
    fn new(started_at_millis: u64) -> Self {
        Self {
            started_at_millis,
            last_tick_at_millis: None,
            last_outcome: None,
            phases: DurableRecoverySchedulerPhaseStatuses::default(),
            last_error: None,
        }
    }
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

async fn durable_recovery_scheduler_tick(
    repo_id: &RepoId,
    stores: &StratumStores,
    status: &Arc<Mutex<DurableRecoverySchedulerStatus>>,
) {
    let mut tick_status = DurableRecoverySchedulerStatus {
        started_at_millis: status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .started_at_millis,
        last_tick_at_millis: Some(current_unix_timestamp_millis()),
        last_outcome: None,
        phases: DurableRecoverySchedulerPhaseStatuses::default(),
        last_error: None,
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
        DURABLE_RECOVERY_SCHEDULER_LEASE_DURATION,
        DURABLE_RECOVERY_SCHEDULER_LIMIT,
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

    let post_cas_limit = DURABLE_RECOVERY_SCHEDULER_LIMIT.saturating_sub(pre_visibility_attempted);
    let post_cas_worker = DurableCorePostCasRepairWorker::new(
        DurableCorePostCasRepairWorkerStores::new(
            stores.post_cas_recovery.as_ref(),
            stores.commits.as_ref(),
            stores.workspace_metadata.as_ref(),
            stores.audit.as_ref(),
            stores.idempotency.as_ref(),
        ),
        DURABLE_RECOVERY_SCHEDULER_COMMIT_LEASE_OWNER,
        DURABLE_RECOVERY_SCHEDULER_LEASE_DURATION,
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
        DURABLE_RECOVERY_SCHEDULER_LEASE_DURATION,
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
    match object_cleanup_worker.run_once(object_cleanup_limit).await {
        Ok(summary) => {
            tick_status.phases.object_cleanup =
                DurableRecoverySchedulerPhaseStatus::from_object_cleanup_summary(&summary);
        }
        Err(_) => {
            tracing::debug!("durable recovery scheduler object cleanup phase failed");
            phase_failures += 1;
            last_error = Some("object_cleanup_failed".to_string());
        }
    }
    tick_status.last_error = last_error;
    tick_status.last_outcome = Some(
        match phase_failures {
            0 => "completed",
            4 => "failed",
            _ => "partial_failure",
        }
        .to_string(),
    );
    *status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = tick_status;
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
            deleted_final_objects: Some(summary.deleted_final_objects),
            deferred: Some(summary.skipped_blocked + summary.skipped_claim_unavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::AuditAction;
    use crate::backend::ObjectWrite;
    use crate::backend::blob_object::ObjectMetadataRecord;
    use crate::backend::core_transaction::{
        DurableFsMutationAuditRecoveryContext, DurableFsMutationRecoveryEnvelope,
        DurableFsMutationRecoveryStep, DurableFsMutationRecoveryTarget,
    };
    use crate::backend::object_cleanup::{ObjectCleanupClaimKind, ObjectCleanupClaimRequest};
    use crate::backend::runtime::{
        BACKEND_ENV, CORE_RUNTIME_ENV, DURABLE_AUTH_SESSION_READY_ENV, DURABLE_CORE_REPO_ID_ENV,
        DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV, DURABLE_POLICY_READY_ENV, DURABLE_RECOVERY_READY_ENV,
        DURABLE_REPO_ROUTING_READY_ENV, IDEMPOTENCY_COMPLETED_RETENTION_SECONDS_ENV,
        IDEMPOTENCY_MAX_RECORDS_PER_SCOPE_ENV, IDEMPOTENCY_PENDING_STALE_SECONDS_ENV,
        POSTGRES_URL_ENV, R2_ACCESS_KEY_ID_ENV, R2_BUCKET_ENV, R2_ENDPOINT_ENV,
        R2_SECRET_ACCESS_KEY_ENV,
    };
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::CommitId;
    use uuid::Uuid;

    fn commit_id(label: &str) -> CommitId {
        CommitId::from(ObjectId::from_bytes(label.as_bytes()))
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

    #[test]
    fn open_server_stores_preserves_local_guarded_durable_commit_behavior() {
        let runtime = BackendRuntimeConfig::from_lookup(|name| match name {
            BACKEND_ENV => Some("local".to_string()),
            _ => None,
        })
        .expect("local runtime should parse");

        let stores = ServerStores {
            workspaces: Arc::new(crate::workspace::InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(crate::idempotency::InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
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
            workspaces: stores.workspace_metadata.clone(),
            idempotency: stores.idempotency.clone(),
            audit: stores.audit.clone(),
            review: stores.review.clone(),
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
                workspaces: stores.workspace_metadata.clone(),
                idempotency: stores.idempotency.clone(),
                audit: stores.audit.clone(),
                review: stores.review.clone(),
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
            (reqwest::Method::POST, "/change-requests"),
            (reqwest::Method::GET, "/protected/refs"),
            (reqwest::Method::PUT, "/fs/file.md"),
            (reqwest::Method::POST, "/vcs/commit"),
            (reqwest::Method::PATCH, "/vcs/refs/main"),
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

        let _router = build_router_with_stores_and_guarded_durable_commit(
            StratumDb::open_memory(),
            stores.workspace_metadata.clone(),
            stores.idempotency.clone(),
            stores.audit.clone(),
            stores.review.clone(),
            Some(stores.clone()),
        );

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

        let scheduler =
            start_durable_recovery_scheduler(stores.clone()).expect("scheduler should start");

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

        let handle = start_durable_recovery_scheduler(stores.clone());
        let handle = handle.expect("scheduler should start");
        let duplicate = start_durable_recovery_scheduler(stores.clone())
            .expect("duplicate store set should reuse scheduler handle");
        assert!(Arc::ptr_eq(&handle, &duplicate));
        assert_eq!(
            handle.status().started_at_millis,
            duplicate.status().started_at_millis
        );
        drop(handle);
        assert!(start_durable_recovery_scheduler(stores.clone()).is_some());
        drop(duplicate);
        assert!(start_durable_recovery_scheduler(stores).is_some());
    }

    #[tokio::test]
    async fn durable_recovery_scheduler_status_records_last_tick_outcome() {
        let stores = StratumStores::local_memory();
        let handle =
            start_durable_recovery_scheduler(stores.clone()).expect("scheduler should start");

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
            POSTGRES_URL_ENV => Some("postgresql://127.0.0.1/stratum".to_string()),
            R2_BUCKET_ENV => Some("stratum-test".to_string()),
            R2_ENDPOINT_ENV => Some("https://account.r2.cloudflarestorage.com".to_string()),
            R2_ACCESS_KEY_ID_ENV => Some("test-access-key-id".to_string()),
            R2_SECRET_ACCESS_KEY_ENV => Some("test-secret-access-key".to_string()),
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
