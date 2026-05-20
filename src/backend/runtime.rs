//! Runtime backend selection contract.
//!
//! This module validates the operator-facing backend mode and the planned
//! durable backend prerequisites. The HTTP server can use the Postgres-backed
//! control-plane stores in durable mode when built with the `postgres` feature,
//! while core filesystem/VCS and S3/R2 object-byte routing remain separate
//! cutover boundaries.

use regex::Regex;
use std::env::VarError;
use std::fmt;
#[cfg(feature = "postgres")]
use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::Duration;

use crate::backend::RepoId;
use crate::error::VfsError;
use crate::idempotency::IdempotencyRetentionPolicy;
use crate::secret_replay::{
    LocalAeadSecretReplayKms, SharedSecretReplayKms, local_aead_key_from_b64,
    normalize_secret_replay_key_id,
};

#[cfg(feature = "postgres")]
use tokio_postgres::config::{Host, SslMode};

pub const BACKEND_ENV: &str = "STRATUM_BACKEND";
pub const CORE_RUNTIME_ENV: &str = "STRATUM_CORE_RUNTIME";
pub const POSTGRES_URL_ENV: &str = "STRATUM_POSTGRES_URL";
pub const POSTGRES_SCHEMA_ENV: &str = "STRATUM_POSTGRES_SCHEMA";
pub const POSTGRES_POOL_MAX_SIZE_ENV: &str = "STRATUM_POSTGRES_POOL_MAX_SIZE";
pub const POSTGRES_CONNECT_TIMEOUT_MS_ENV: &str = "STRATUM_POSTGRES_CONNECT_TIMEOUT_MS";
pub const POSTGRES_OPERATION_TIMEOUT_MS_ENV: &str = "STRATUM_POSTGRES_OPERATION_TIMEOUT_MS";
pub const POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV: &str = "STRATUM_POSTGRES_POOL_ACQUIRE_TIMEOUT_MS";
pub const DURABLE_MIGRATION_MODE_ENV: &str = "STRATUM_DURABLE_MIGRATION_MODE";
pub const R2_BUCKET_ENV: &str = "STRATUM_R2_BUCKET";
pub const R2_ENDPOINT_ENV: &str = "STRATUM_R2_ENDPOINT";
pub const R2_ACCESS_KEY_ID_ENV: &str = "STRATUM_R2_ACCESS_KEY_ID";
pub const R2_SECRET_ACCESS_KEY_ENV: &str = "STRATUM_R2_SECRET_ACCESS_KEY";
pub const R2_REGION_ENV: &str = "STRATUM_R2_REGION";
pub const R2_PREFIX_ENV: &str = "STRATUM_R2_PREFIX";
pub const R2_ALLOW_INSECURE_LOCAL_ENDPOINT_ENV: &str = "STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT";
pub const R2_REQUEST_TIMEOUT_MS_ENV: &str = "STRATUM_R2_REQUEST_TIMEOUT_MS";
pub const R2_CONNECT_TIMEOUT_MS_ENV: &str = "STRATUM_R2_CONNECT_TIMEOUT_MS";
pub const R2_MAX_ATTEMPTS_ENV: &str = "STRATUM_R2_MAX_ATTEMPTS";
pub const R2_RETRY_BASE_DELAY_MS_ENV: &str = "STRATUM_R2_RETRY_BASE_DELAY_MS";
pub const R2_RETRY_MAX_DELAY_MS_ENV: &str = "STRATUM_R2_RETRY_MAX_DELAY_MS";
pub const DURABLE_COMMIT_ROUTE_ENV: &str = "STRATUM_DURABLE_COMMIT_ROUTE";
pub const DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV: &str = "STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV";
pub const DURABLE_AUTH_SESSION_READY_ENV: &str = "STRATUM_DURABLE_AUTH_SESSION_READY";
pub const DURABLE_POLICY_READY_ENV: &str = "STRATUM_DURABLE_POLICY_READY";
pub const DURABLE_REPO_ROUTING_READY_ENV: &str = "STRATUM_DURABLE_REPO_ROUTING_READY";
pub const DURABLE_RECOVERY_READY_ENV: &str = "STRATUM_DURABLE_RECOVERY_READY";
pub const DURABLE_CORE_REPO_ID_ENV: &str = "STRATUM_DURABLE_CORE_REPO_ID";
pub const IDEMPOTENCY_COMPLETED_RETENTION_SECONDS_ENV: &str =
    "STRATUM_IDEMPOTENCY_COMPLETED_RETENTION_SECONDS";
pub const IDEMPOTENCY_PENDING_STALE_SECONDS_ENV: &str = "STRATUM_IDEMPOTENCY_PENDING_STALE_SECONDS";
pub const IDEMPOTENCY_MAX_RECORDS_PER_SCOPE_ENV: &str = "STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_SCOPE";
pub const IDEMPOTENCY_MAX_RECORDS_PER_REPO_ENV: &str = "STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_REPO";
pub const IDEMPOTENCY_MAX_RECORDS_PER_WORKSPACE_ENV: &str =
    "STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_WORKSPACE";
pub const IDEMPOTENCY_MAX_RECORDS_PER_PRINCIPAL_ENV: &str =
    "STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_PRINCIPAL";
pub const SECRET_REPLAY_KMS_PROVIDER_ENV: &str = "STRATUM_SECRET_REPLAY_KMS_PROVIDER";
pub const SECRET_REPLAY_KMS_KEY_ID_ENV: &str = "STRATUM_SECRET_REPLAY_KMS_KEY_ID";
pub const SECRET_REPLAY_KMS_KEY_B64_ENV: &str = "STRATUM_SECRET_REPLAY_KMS_KEY_B64";
pub const RECOVERY_SCHEDULER_ENV: &str = "STRATUM_RECOVERY_SCHEDULER";
pub const RECOVERY_SCHEDULER_INTERVAL_MS_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_INTERVAL_MS";
pub const RECOVERY_SCHEDULER_TICK_LIMIT_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_TICK_LIMIT";
pub const RECOVERY_SCHEDULER_LEASE_MS_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_LEASE_MS";
pub const RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV: &str = "STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN";
pub const RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV: &str =
    "STRATUM_RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS";
pub const DURABLE_AUTH_SESSION_READINESS_MISSING: &str =
    "durable auth/session routing readiness is missing";
const IDEMPOTENCY_RETENTION_MAX_SECONDS: u64 = 10 * 365 * 24 * 60 * 60;
const IDEMPOTENCY_QUOTA_MAX_RECORDS: usize = 10_000_000;
const POSTGRES_POOL_MAX_SIZE_MAX: usize = 256;
const STORAGE_TIMEOUT_MAX_MS: u64 = 300_000;
const R2_MAX_ATTEMPTS_MAX: u32 = 10;
const RECOVERY_SCHEDULER_DEFAULT_INTERVAL_MS: u64 = 5_000;
const RECOVERY_SCHEDULER_MAX_INTERVAL_MS: u64 = 300_000;
const RECOVERY_SCHEDULER_DEFAULT_TICK_LIMIT: usize = 10;
const RECOVERY_SCHEDULER_MAX_TICK_LIMIT: usize = 100;
const RECOVERY_SCHEDULER_DEFAULT_LEASE_MS: u64 = 30_000;
const RECOVERY_SCHEDULER_MAX_LEASE_MS: u64 = 300_000;
const RECOVERY_SCHEDULER_DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 2_500;
const RECOVERY_SCHEDULER_MAX_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 30_000;

#[cfg(feature = "postgres")]
pub trait PostgresSecretProvider: Send + Sync {
    fn postgres_password(&self) -> Result<Option<String>, VfsError>;
}

#[cfg(feature = "postgres")]
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvPostgresSecretProvider;

#[cfg(feature = "postgres")]
impl PostgresSecretProvider for EnvPostgresSecretProvider {
    fn postgres_password(&self) -> Result<Option<String>, VfsError> {
        match std::env::var("PGPASSWORD") {
            Ok(password) if password.is_empty() => Ok(None),
            Ok(password) => Ok(Some(password)),
            Err(VarError::NotPresent) => Ok(None),
            Err(VarError::NotUnicode(_)) => Err(postgres_secret_resolution_error()),
        }
    }
}

#[cfg(feature = "postgres")]
fn postgres_secret_resolution_error() -> VfsError {
    VfsError::InvalidArgs {
        message: "postgres secret resolution failed".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonServerRuntimeSurface {
    StratumMcp,
    StratumMount,
    StratumRepl,
}

impl NonServerRuntimeSurface {
    fn as_str(self) -> &'static str {
        match self {
            Self::StratumMcp => "stratum-mcp",
            Self::StratumMount => "stratum-mount",
            Self::StratumRepl => "stratum-repl",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendRuntimeMode {
    Local,
    Durable,
}

impl BackendRuntimeMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "local" => Ok(Self::Local),
            "durable" => Ok(Self::Durable),
            _ => Err(VfsError::InvalidArgs {
                message: format!("invalid {BACKEND_ENV}; expected `local` or `durable`"),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Durable => "durable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreRuntimeMode {
    LocalState,
    DurableCloud,
}

impl CoreRuntimeMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "local" | "local-state" | "state-file" | "snapshot" => Ok(Self::LocalState),
            "durable" | "durable-cloud" | "postgres-r2" => Ok(Self::DurableCloud),
            _ => Err(VfsError::InvalidArgs {
                message: format!(
                    "invalid {CORE_RUNTIME_ENV}; expected `local-state` or `durable-cloud`"
                ),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalState => "local-state",
            Self::DurableCloud => "durable-cloud",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableAuthSessionReadiness {
    NotRequiredForLocalState,
    MissingForDurableCoreRuntime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableMigrationMode {
    Status,
    Apply,
    Adopt,
}

impl DurableMigrationMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "status" => Ok(Self::Status),
            "apply" => Ok(Self::Apply),
            "adopt" => Ok(Self::Adopt),
            _ => Err(VfsError::InvalidArgs {
                message: format!(
                    "invalid {DURABLE_MIGRATION_MODE_ENV}; expected `status`, `apply`, or `adopt`"
                ),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Apply => "apply",
            Self::Adopt => "adopt",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardedDurableCommitRouteMode {
    Disabled,
    Enabled,
}

impl GuardedDurableCommitRouteMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "false" | "off" | "disabled" => Ok(Self::Disabled),
            "1" | "true" | "on" | "enabled" => Ok(Self::Enabled),
            _ => Err(VfsError::InvalidArgs {
                message: format!("invalid {DURABLE_COMMIT_ROUTE_ENV}; expected `0` or `1`"),
            }),
        }
    }

    pub fn enabled(self) -> bool {
        self == Self::Enabled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySchedulerMode {
    Enabled,
    Disabled,
}

impl RecoverySchedulerMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            _ => Err(VfsError::InvalidArgs {
                message: format!("invalid {RECOVERY_SCHEDULER_ENV}; expected enabled or disabled"),
            }),
        }
    }

    pub fn enabled(self) -> bool {
        self == Self::Enabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySchedulerRuntimeConfig {
    mode: RecoverySchedulerMode,
    interval: Duration,
    tick_limit: usize,
    lease_duration: Duration,
    shutdown_drain_enabled: bool,
    shutdown_drain_timeout: Duration,
}

impl RecoverySchedulerRuntimeConfig {
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
        let mode = RecoverySchedulerMode::from_env_value(
            lookup(RECOVERY_SCHEDULER_ENV)
                .as_deref()
                .unwrap_or_default(),
        )?;
        let shutdown_drain_enabled = recovery_scheduler_enabled_flag_from_value(
            RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV,
            lookup(RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV)
                .as_deref()
                .unwrap_or_default(),
            false,
        )?;

        Ok(Self {
            mode,
            interval: optional_duration_ms(
                &mut lookup,
                RECOVERY_SCHEDULER_INTERVAL_MS_ENV,
                RECOVERY_SCHEDULER_MAX_INTERVAL_MS,
            )?
            .unwrap_or_else(|| Duration::from_millis(RECOVERY_SCHEDULER_DEFAULT_INTERVAL_MS)),
            tick_limit: optional_positive_usize(
                &mut lookup,
                RECOVERY_SCHEDULER_TICK_LIMIT_ENV,
                RECOVERY_SCHEDULER_MAX_TICK_LIMIT,
            )?
            .unwrap_or(RECOVERY_SCHEDULER_DEFAULT_TICK_LIMIT),
            lease_duration: optional_duration_ms(
                &mut lookup,
                RECOVERY_SCHEDULER_LEASE_MS_ENV,
                RECOVERY_SCHEDULER_MAX_LEASE_MS,
            )?
            .unwrap_or_else(|| Duration::from_millis(RECOVERY_SCHEDULER_DEFAULT_LEASE_MS)),
            shutdown_drain_enabled,
            shutdown_drain_timeout: optional_duration_ms(
                &mut lookup,
                RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV,
                RECOVERY_SCHEDULER_MAX_SHUTDOWN_DRAIN_TIMEOUT_MS,
            )?
            .unwrap_or_else(|| {
                Duration::from_millis(RECOVERY_SCHEDULER_DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS)
            }),
        })
    }

    pub fn mode(&self) -> RecoverySchedulerMode {
        self.mode
    }

    pub fn enabled(&self) -> bool {
        self.mode.enabled()
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn tick_limit(&self) -> usize {
        self.tick_limit
    }

    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub fn shutdown_drain_enabled(&self) -> bool {
        self.shutdown_drain_enabled
    }

    pub fn shutdown_drain_timeout(&self) -> Duration {
        self.shutdown_drain_timeout
    }
}

impl Default for RecoverySchedulerRuntimeConfig {
    fn default() -> Self {
        Self {
            mode: RecoverySchedulerMode::Enabled,
            interval: Duration::from_millis(RECOVERY_SCHEDULER_DEFAULT_INTERVAL_MS),
            tick_limit: RECOVERY_SCHEDULER_DEFAULT_TICK_LIMIT,
            lease_duration: Duration::from_millis(RECOVERY_SCHEDULER_DEFAULT_LEASE_MS),
            shutdown_drain_enabled: false,
            shutdown_drain_timeout: Duration::from_millis(
                RECOVERY_SCHEDULER_DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
            ),
        }
    }
}

fn recovery_scheduler_enabled_flag_from_value(
    name: &'static str,
    value: &str,
    default: bool,
) -> Result<bool, VfsError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(default),
        "enabled" => Ok(true),
        "disabled" => Ok(false),
        _ => Err(VfsError::InvalidArgs {
            message: format!("invalid {name}; expected enabled or disabled"),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableStartupPreflight {
    migration_status: DurableMigrationPreflightStatus,
}

impl DurableStartupPreflight {
    fn no_op() -> Self {
        Self {
            migration_status: DurableMigrationPreflightStatus::NotRequired,
        }
    }

    #[cfg(not(feature = "postgres"))]
    fn postgres_feature_disabled() -> Self {
        Self {
            migration_status: DurableMigrationPreflightStatus::NotCheckedPostgresFeatureDisabled,
        }
    }

    #[cfg(feature = "postgres")]
    fn checked(migration_status: DurableMigrationPreflightStatus) -> Self {
        Self { migration_status }
    }

    pub fn migration_status(&self) -> DurableMigrationPreflightStatus {
        self.migration_status
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableMigrationPreflightStatus {
    NotRequired,
    NotCheckedPostgresFeatureDisabled,
    Checked,
    Applied,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BackendRuntimeConfig {
    mode: BackendRuntimeMode,
    core_runtime_mode: CoreRuntimeMode,
    guarded_durable_commit_route: GuardedDurableCommitRouteMode,
    secret_replay_kms: SecretReplayKmsRuntimeConfig,
    recovery_scheduler: RecoverySchedulerRuntimeConfig,
    durable_core_runtime: Option<DurableCoreRuntimeReadinessConfig>,
    durable: Option<DurableBackendRuntimeConfig>,
}

impl BackendRuntimeConfig {
    pub fn from_env() -> Result<Self, VfsError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
        let core_runtime_mode = CoreRuntimeMode::from_env_value(
            lookup(CORE_RUNTIME_ENV).as_deref().unwrap_or_default(),
        )?;
        let mode =
            BackendRuntimeMode::from_env_value(lookup(BACKEND_ENV).as_deref().unwrap_or("local"))?;
        let secret_replay_kms = SecretReplayKmsRuntimeConfig::from_lookup(&mut lookup)?;
        let recovery_scheduler = RecoverySchedulerRuntimeConfig::from_lookup(&mut lookup)?;

        if core_runtime_mode == CoreRuntimeMode::DurableCloud {
            if mode != BackendRuntimeMode::Durable {
                return Err(VfsError::NotSupported {
                    message: format!(
                        "{CORE_RUNTIME_ENV}=durable-cloud requires {BACKEND_ENV}=durable"
                    ),
                });
            }

            let durable_core_runtime =
                Some(DurableCoreRuntimeReadinessConfig::from_lookup(&mut lookup)?);
            let guarded_durable_commit_route = GuardedDurableCommitRouteMode::from_env_value(
                lookup(DURABLE_COMMIT_ROUTE_ENV)
                    .as_deref()
                    .unwrap_or_default(),
            )?;
            if guarded_durable_commit_route.enabled() {
                return Err(VfsError::NotSupported {
                    message: format!(
                        "{DURABLE_COMMIT_ROUTE_ENV}=1 is only supported with {CORE_RUNTIME_ENV}=local-state"
                    ),
                });
            }

            return Ok(Self {
                mode,
                core_runtime_mode,
                guarded_durable_commit_route,
                secret_replay_kms,
                recovery_scheduler,
                durable_core_runtime,
                durable: Some(DurableBackendRuntimeConfig::from_lookup(lookup, true)?),
            });
        }

        let guarded_durable_commit_route = GuardedDurableCommitRouteMode::from_env_value(
            lookup(DURABLE_COMMIT_ROUTE_ENV)
                .as_deref()
                .unwrap_or_default(),
        )?;
        match mode {
            BackendRuntimeMode::Local => Ok(Self {
                mode,
                core_runtime_mode,
                guarded_durable_commit_route,
                secret_replay_kms,
                recovery_scheduler,
                durable_core_runtime: None,
                durable: None,
            }),
            BackendRuntimeMode::Durable => Ok(Self {
                mode,
                core_runtime_mode,
                guarded_durable_commit_route,
                secret_replay_kms,
                recovery_scheduler,
                durable_core_runtime: None,
                durable: Some(DurableBackendRuntimeConfig::from_lookup(lookup, false)?),
            }),
        }
    }

    pub fn mode(&self) -> BackendRuntimeMode {
        self.mode
    }

    pub fn core_runtime_mode(&self) -> CoreRuntimeMode {
        self.core_runtime_mode
    }

    pub fn durable(&self) -> Option<&DurableBackendRuntimeConfig> {
        self.durable.as_ref()
    }

    pub fn guarded_durable_commit_route_enabled(&self) -> bool {
        self.guarded_durable_commit_route.enabled()
    }

    pub fn durable_core_repo_id(&self) -> Option<&RepoId> {
        self.durable_core_runtime
            .as_ref()
            .map(DurableCoreRuntimeReadinessConfig::repo_id)
    }

    pub fn idempotency_retention_policy(&self) -> Option<&IdempotencyRetentionPolicy> {
        self.durable_core_runtime
            .as_ref()
            .map(DurableCoreRuntimeReadinessConfig::idempotency_retention_policy)
    }

    pub fn durable_core_runtime_ready(&self) -> bool {
        self.durable_core_runtime.is_some()
    }

    pub fn secret_replay_kms(&self) -> Result<Option<SharedSecretReplayKms>, VfsError> {
        self.secret_replay_kms.provider()
    }

    pub fn recovery_scheduler(&self) -> &RecoverySchedulerRuntimeConfig {
        &self.recovery_scheduler
    }

    pub fn durable_auth_session_readiness(&self) -> DurableAuthSessionReadiness {
        match self.core_runtime_mode {
            CoreRuntimeMode::LocalState => DurableAuthSessionReadiness::NotRequiredForLocalState,
            CoreRuntimeMode::DurableCloud if self.durable_core_runtime.is_none() => {
                DurableAuthSessionReadiness::MissingForDurableCoreRuntime
            }
            CoreRuntimeMode::DurableCloud => DurableAuthSessionReadiness::NotRequiredForLocalState,
        }
    }

    fn ensure_core_runtime_supported_for_server(&self) -> Result<(), VfsError> {
        match self.durable_auth_session_readiness() {
            DurableAuthSessionReadiness::NotRequiredForLocalState => Ok(()),
            DurableAuthSessionReadiness::MissingForDurableCoreRuntime => {
                Err(unsupported_durable_core_runtime())
            }
        }
    }

    pub fn ensure_supported_for_server(&self) -> Result<(), VfsError> {
        self.ensure_core_runtime_supported_for_server()?;
        if self.guarded_durable_commit_route.enabled() && self.mode != BackendRuntimeMode::Durable {
            return Err(VfsError::NotSupported {
                message: format!("{DURABLE_COMMIT_ROUTE_ENV}=1 requires {BACKEND_ENV}=durable"),
            });
        }

        match self.mode {
            BackendRuntimeMode::Local => Ok(()),
            #[cfg(feature = "postgres")]
            BackendRuntimeMode::Durable => Ok(()),
            #[cfg(not(feature = "postgres"))]
            BackendRuntimeMode::Durable => Err(VfsError::NotSupported {
                message:
                    "durable backend runtime requires stratum-server built with the postgres feature"
                        .to_string(),
            }),
        }
    }

    pub async fn prepare_server_startup(&self) -> Result<DurableStartupPreflight, VfsError> {
        self.ensure_core_runtime_supported_for_server()?;

        match (self.mode, self.durable.as_ref()) {
            (BackendRuntimeMode::Local, _) => Ok(DurableStartupPreflight::no_op()),
            (BackendRuntimeMode::Durable, Some(durable)) => durable.prepare_server_startup().await,
            (BackendRuntimeMode::Durable, None) => Err(VfsError::InvalidArgs {
                message: "durable backend runtime config is missing".to_string(),
            }),
        }
    }

    #[cfg(feature = "postgres")]
    #[allow(dead_code)]
    pub(crate) async fn prepare_server_startup_with_secret_provider(
        &self,
        secret_provider: &impl PostgresSecretProvider,
    ) -> Result<DurableStartupPreflight, VfsError> {
        self.ensure_core_runtime_supported_for_server()?;

        match (self.mode, self.durable.as_ref()) {
            (BackendRuntimeMode::Local, _) => Ok(DurableStartupPreflight::no_op()),
            (BackendRuntimeMode::Durable, Some(durable)) => {
                durable
                    .prepare_server_startup_with_secret_provider(secret_provider)
                    .await
            }
            (BackendRuntimeMode::Durable, None) => Err(VfsError::InvalidArgs {
                message: "durable backend runtime config is missing".to_string(),
            }),
        }
    }
}

impl fmt::Debug for BackendRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendRuntimeConfig")
            .field("mode", &self.mode)
            .field("core_runtime_mode", &self.core_runtime_mode)
            .field(
                "guarded_durable_commit_route",
                &self.guarded_durable_commit_route,
            )
            .field("secret_replay_kms", &self.secret_replay_kms)
            .field("recovery_scheduler", &self.recovery_scheduler)
            .field("durable_core_runtime", &self.durable_core_runtime)
            .field("durable", &self.durable)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SecretReplayKmsRuntimeConfig {
    Disabled,
    LocalAead {
        key_id: String,
        key_material: [u8; 32],
    },
}

impl SecretReplayKmsRuntimeConfig {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
        let provider = optional_value(&mut lookup, SECRET_REPLAY_KMS_PROVIDER_ENV)
            .unwrap_or_else(|| "disabled".to_string());
        match provider.trim().to_ascii_lowercase().as_str() {
            "" | "disabled" => Ok(Self::Disabled),
            "local-aead" => {
                let key_id = optional_value(&mut lookup, SECRET_REPLAY_KMS_KEY_ID_ENV)
                    .ok_or_else(|| VfsError::InvalidArgs {
                        message: format!(
                            "missing required secret replay KMS environment variable: {SECRET_REPLAY_KMS_KEY_ID_ENV}"
                        ),
                    })
                    .and_then(|value| {
                        normalize_secret_replay_key_id(value).map_err(|_| VfsError::InvalidArgs {
                            message: format!(
                                "invalid {SECRET_REPLAY_KMS_KEY_ID_ENV}; expected 1-255 bytes"
                            ),
                        })
                    })?;
                let key_b64 = optional_value(&mut lookup, SECRET_REPLAY_KMS_KEY_B64_ENV)
                    .ok_or_else(|| VfsError::InvalidArgs {
                        message: format!(
                            "missing required secret replay KMS environment variable: {SECRET_REPLAY_KMS_KEY_B64_ENV}"
                        ),
                    })?;
                let key_material = local_aead_key_from_b64(&key_b64).map_err(|_| {
                    VfsError::InvalidArgs {
                        message: format!(
                            "invalid {SECRET_REPLAY_KMS_KEY_B64_ENV}; expected base64-encoded 32-byte key"
                        ),
                    }
                })?;
                Ok(Self::LocalAead {
                    key_id,
                    key_material,
                })
            }
            _ => Err(VfsError::InvalidArgs {
                message: format!(
                    "invalid {SECRET_REPLAY_KMS_PROVIDER_ENV}; expected `disabled` or `local-aead`"
                ),
            }),
        }
    }

    fn provider(&self) -> Result<Option<SharedSecretReplayKms>, VfsError> {
        match self {
            Self::Disabled => Ok(None),
            Self::LocalAead {
                key_id,
                key_material,
            } => Ok(Some(std::sync::Arc::new(LocalAeadSecretReplayKms::new(
                key_id.clone(),
                *key_material,
            )?))),
        }
    }
}

impl fmt::Debug for SecretReplayKmsRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("SecretReplayKmsRuntimeConfig::Disabled"),
            Self::LocalAead { key_id, .. } => f
                .debug_struct("SecretReplayKmsRuntimeConfig::LocalAead")
                .field("key_id_configured", &!key_id.is_empty())
                .field("key_material_configured", &true)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DurableCoreRuntimeReadinessConfig {
    repo_id: RepoId,
    idempotency_retention_policy: IdempotencyRetentionPolicy,
}

impl DurableCoreRuntimeReadinessConfig {
    fn from_lookup(lookup: &mut impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
        require_gate(lookup, DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV)?;
        require_gate(lookup, DURABLE_AUTH_SESSION_READY_ENV)?;
        require_gate(lookup, DURABLE_POLICY_READY_ENV)?;
        require_gate(lookup, DURABLE_REPO_ROUTING_READY_ENV)?;
        require_gate(lookup, DURABLE_RECOVERY_READY_ENV)?;

        let Some(repo_id) = optional_value(lookup, DURABLE_CORE_REPO_ID_ENV) else {
            return Err(VfsError::NotSupported {
                message: format!(
                    "durable core runtime readiness gate missing: {DURABLE_CORE_REPO_ID_ENV}"
                ),
            });
        };
        let repo_id = RepoId::new(repo_id).map_err(|_| VfsError::InvalidArgs {
            message: format!("invalid {DURABLE_CORE_REPO_ID_ENV}; expected non-local RepoId"),
        })?;
        if repo_id == RepoId::local() {
            return Err(VfsError::NotSupported {
                message: format!(
                    "{DURABLE_CORE_REPO_ID_ENV} must not use the local singleton repo id"
                ),
            });
        }
        let idempotency_retention_policy = IdempotencyRetentionPolicy::from_required_env(lookup)?;

        Ok(Self {
            repo_id,
            idempotency_retention_policy,
        })
    }

    fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    fn idempotency_retention_policy(&self) -> &IdempotencyRetentionPolicy {
        &self.idempotency_retention_policy
    }
}

impl IdempotencyRetentionPolicy {
    fn from_required_env(
        lookup: &mut impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, VfsError> {
        Ok(Self {
            completed_ttl_seconds: required_positive_u64(
                lookup,
                IDEMPOTENCY_COMPLETED_RETENTION_SECONDS_ENV,
                IDEMPOTENCY_RETENTION_MAX_SECONDS,
            )?,
            pending_stale_after_seconds: required_positive_u64(
                lookup,
                IDEMPOTENCY_PENDING_STALE_SECONDS_ENV,
                IDEMPOTENCY_RETENTION_MAX_SECONDS,
            )?,
            max_records_per_scope: Some(required_positive_usize(
                lookup,
                IDEMPOTENCY_MAX_RECORDS_PER_SCOPE_ENV,
                IDEMPOTENCY_QUOTA_MAX_RECORDS,
            )?),
            max_records_per_repo: optional_positive_usize(
                lookup,
                IDEMPOTENCY_MAX_RECORDS_PER_REPO_ENV,
                IDEMPOTENCY_QUOTA_MAX_RECORDS,
            )?,
            max_records_per_workspace: optional_positive_usize(
                lookup,
                IDEMPOTENCY_MAX_RECORDS_PER_WORKSPACE_ENV,
                IDEMPOTENCY_QUOTA_MAX_RECORDS,
            )?,
            max_records_per_principal: optional_positive_usize(
                lookup,
                IDEMPOTENCY_MAX_RECORDS_PER_PRINCIPAL_ENV,
                IDEMPOTENCY_QUOTA_MAX_RECORDS,
            )?,
        })
    }
}

fn required_positive_u64(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: u64,
) -> Result<u64, VfsError> {
    let Some(value) = optional_value(lookup, name) else {
        return Err(VfsError::NotSupported {
            message: format!("durable core runtime readiness gate missing: {name}"),
        });
    };
    parse_positive_u64(name, &value, max)
}

fn optional_positive_usize(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: usize,
) -> Result<Option<usize>, VfsError> {
    optional_value(lookup, name)
        .map(|value| parse_positive_usize(name, &value, max))
        .transpose()
}

fn optional_duration_ms(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: u64,
) -> Result<Option<Duration>, VfsError> {
    optional_value(lookup, name)
        .map(|value| parse_positive_u64(name, &value, max).map(Duration::from_millis))
        .transpose()
}

fn required_positive_usize(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: usize,
) -> Result<usize, VfsError> {
    let Some(value) = optional_value(lookup, name) else {
        return Err(VfsError::NotSupported {
            message: format!("durable core runtime readiness gate missing: {name}"),
        });
    };
    parse_positive_usize(name, &value, max)
}

fn required_storage_positive_usize(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: usize,
) -> Result<usize, VfsError> {
    let Some(value) = optional_value(lookup, name) else {
        return Err(VfsError::NotSupported {
            message: format!("durable storage posture gate missing: {name}"),
        });
    };
    parse_positive_usize(name, &value, max)
}

fn required_storage_positive_u32(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: u32,
) -> Result<u32, VfsError> {
    let Some(value) = optional_value(lookup, name) else {
        return Err(VfsError::NotSupported {
            message: format!("durable storage posture gate missing: {name}"),
        });
    };
    parse_positive_u32(name, &value, max)
}

fn required_storage_positive_duration_ms(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: u64,
) -> Result<Duration, VfsError> {
    let Some(value) = optional_value(lookup, name) else {
        return Err(VfsError::NotSupported {
            message: format!("durable storage posture gate missing: {name}"),
        });
    };
    parse_positive_u64(name, &value, max).map(Duration::from_millis)
}

fn parse_positive_u64(name: &'static str, value: &str, max: u64) -> Result<u64, VfsError> {
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| invalid_positive_integer_env(name, max))?;
    if parsed == 0 || parsed > max {
        return Err(invalid_positive_integer_env(name, max));
    }
    Ok(parsed)
}

fn parse_positive_u32(name: &'static str, value: &str, max: u32) -> Result<u32, VfsError> {
    let parsed = value
        .trim()
        .parse::<u32>()
        .map_err(|_| invalid_positive_integer_env(name, max))?;
    if parsed == 0 || parsed > max {
        return Err(invalid_positive_integer_env(name, max));
    }
    Ok(parsed)
}

fn parse_positive_usize(name: &'static str, value: &str, max: usize) -> Result<usize, VfsError> {
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| invalid_positive_integer_env(name, max))?;
    if parsed == 0 || parsed > max {
        return Err(invalid_positive_integer_env(name, max));
    }
    Ok(parsed)
}

fn invalid_positive_integer_env(name: &'static str, _max: impl fmt::Display) -> VfsError {
    VfsError::InvalidArgs {
        message: format!("invalid {name}; expected a positive bounded integer"),
    }
}

fn require_gate(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
) -> Result<(), VfsError> {
    match optional_value(lookup, name).as_deref() {
        Some("1") => Ok(()),
        _ => Err(VfsError::NotSupported {
            message: format!("durable core runtime readiness gate missing: {name}"),
        }),
    }
}

pub(crate) fn unsupported_durable_core_runtime() -> VfsError {
    VfsError::NotSupported {
        message: format!(
            "durable core runtime is not supported yet: {DURABLE_AUTH_SESSION_READINESS_MISSING}; set {CORE_RUNTIME_ENV}=local-state"
        ),
    }
}

pub fn ensure_local_state_runtime_for_non_server_surface(
    surface: NonServerRuntimeSurface,
) -> Result<(), VfsError> {
    let value = core_runtime_env_value_from_process()?;
    ensure_local_state_runtime_for_non_server_surface_from_lookup(surface, |_| value.clone())
}

fn core_runtime_env_value_from_process() -> Result<Option<String>, VfsError> {
    core_runtime_env_value_from_result(std::env::var(CORE_RUNTIME_ENV))
}

fn core_runtime_env_value_from_result(
    result: Result<String, VarError>,
) -> Result<Option<String>, VfsError> {
    Ok(match result {
        Ok(value) => Some(value),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => {
            return Err(invalid_core_runtime_env());
        }
    })
}

pub fn ensure_local_state_runtime_for_non_server_surface_from_lookup(
    surface: NonServerRuntimeSurface,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<(), VfsError> {
    match CoreRuntimeMode::from_env_value(lookup(CORE_RUNTIME_ENV).as_deref().unwrap_or_default())?
    {
        CoreRuntimeMode::LocalState => Ok(()),
        CoreRuntimeMode::DurableCloud => Err(VfsError::NotSupported {
            message: format!(
                "{} is local-state only; set {CORE_RUNTIME_ENV}=local-state or use the HTTP server boundary",
                surface.as_str()
            ),
        }),
    }
}

fn invalid_core_runtime_env() -> VfsError {
    VfsError::InvalidArgs {
        message: format!("invalid {CORE_RUNTIME_ENV}; expected `local-state` or `durable-cloud`"),
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DurableBackendRuntimeConfig {
    postgres_url_configured: bool,
    postgres_url: String,
    postgres_schema: String,
    postgres_posture: DurablePostgresRuntimePosture,
    migration_mode: DurableMigrationMode,
    object_store: DurableObjectStoreRuntimeConfig,
}

impl DurableBackendRuntimeConfig {
    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<String>,
        require_hosted_posture: bool,
    ) -> Result<Self, VfsError> {
        let mut missing = Vec::new();
        let postgres_url = required_value(&mut lookup, POSTGRES_URL_ENV, &mut missing);
        let bucket = required_value(&mut lookup, R2_BUCKET_ENV, &mut missing);
        let endpoint = required_value(&mut lookup, R2_ENDPOINT_ENV, &mut missing);
        let access_key_id = required_value(&mut lookup, R2_ACCESS_KEY_ID_ENV, &mut missing);
        let secret_access_key = required_value(&mut lookup, R2_SECRET_ACCESS_KEY_ENV, &mut missing);

        if !missing.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "missing required durable backend environment variables: {}",
                    missing.join(", ")
                ),
            });
        }

        let postgres_url = postgres_url.expect("missing durable env should return earlier");
        if postgres_url_contains_password(&postgres_url) {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "{POSTGRES_URL_ENV} must not include a password; use PGPASSWORD or the deployment secret manager"
                ),
            });
        }
        let postgres_posture =
            DurablePostgresRuntimePosture::from_lookup(&mut lookup, require_hosted_posture)?;
        let postgres_posture = postgres_posture.with_tls_mode(postgres_tls_runtime_mode(
            &postgres_url,
            require_hosted_posture,
        )?);
        let endpoint = endpoint.expect("missing durable env should return earlier");
        let region =
            optional_value(&mut lookup, R2_REGION_ENV).unwrap_or_else(|| "auto".to_string());
        let prefix =
            optional_value(&mut lookup, R2_PREFIX_ENV).unwrap_or_else(|| "stratum".to_string());
        let allow_insecure_local_endpoint =
            r2_allow_insecure_local_endpoint_from_lookup(&mut lookup);
        validate_r2_endpoint(&endpoint, allow_insecure_local_endpoint)?;
        let postgres_schema = optional_value(&mut lookup, POSTGRES_SCHEMA_ENV)
            .unwrap_or_else(|| "public".to_string());
        let migration_mode = DurableMigrationMode::from_env_value(
            lookup(DURABLE_MIGRATION_MODE_ENV)
                .as_deref()
                .unwrap_or_default(),
        )?;

        Ok(Self {
            postgres_url_configured: true,
            postgres_url,
            postgres_schema,
            postgres_posture,
            migration_mode,
            object_store: DurableObjectStoreRuntimeConfig {
                bucket: bucket.expect("missing durable env should return earlier"),
                endpoint,
                access_key_id_configured: access_key_id.is_some(),
                secret_access_key_configured: secret_access_key.is_some(),
                region,
                prefix,
                operation_posture: DurableObjectStoreOperationPosture::from_lookup(
                    &mut lookup,
                    require_hosted_posture,
                )?,
            },
        })
    }

    pub fn postgres_url_configured(&self) -> bool {
        self.postgres_url_configured
    }

    pub fn object_store(&self) -> &DurableObjectStoreRuntimeConfig {
        &self.object_store
    }

    pub fn postgres_schema(&self) -> &str {
        &self.postgres_schema
    }

    pub fn postgres_posture(&self) -> &DurablePostgresRuntimePosture {
        &self.postgres_posture
    }

    pub fn migration_mode(&self) -> DurableMigrationMode {
        self.migration_mode
    }

    #[cfg(all(feature = "postgres", test))]
    pub(crate) fn postgres_config_with_env_password(
        &self,
    ) -> Result<tokio_postgres::Config, VfsError> {
        self.postgres_config_with_secret_provider(&EnvPostgresSecretProvider)
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_config_with_secret_provider(
        &self,
        secret_provider: &impl PostgresSecretProvider,
    ) -> Result<tokio_postgres::Config, VfsError> {
        let mut config = parse_postgres_config(&self.postgres_url)?;

        if config.get_password().is_some() {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "{POSTGRES_URL_ENV} must not include a password; use PGPASSWORD or the deployment secret manager"
                ),
            });
        }
        validate_postgres_runtime_target(&config)?;

        if let Some(password) = secret_provider
            .postgres_password()
            .map_err(|_| postgres_secret_resolution_error())?
            .filter(|password| !password.is_empty())
        {
            config.password(password);
        }

        Ok(config)
    }

    #[cfg(not(feature = "postgres"))]
    async fn prepare_server_startup(&self) -> Result<DurableStartupPreflight, VfsError> {
        Ok(DurableStartupPreflight::postgres_feature_disabled())
    }

    #[cfg(feature = "postgres")]
    async fn prepare_server_startup(&self) -> Result<DurableStartupPreflight, VfsError> {
        self.prepare_server_startup_with_secret_provider(&EnvPostgresSecretProvider)
            .await
    }

    #[cfg(feature = "postgres")]
    async fn prepare_server_startup_with_secret_provider(
        &self,
        secret_provider: &impl PostgresSecretProvider,
    ) -> Result<DurableStartupPreflight, VfsError> {
        use crate::backend::postgres_migrations::PostgresMigrationRunner;

        let config = self.postgres_config_with_secret_provider(secret_provider)?;
        let runner = PostgresMigrationRunner::with_schema_and_posture(
            config,
            self.postgres_schema.clone(),
            self.postgres_posture.clone(),
        )?;
        let report = match self.migration_mode {
            DurableMigrationMode::Status => runner.status().await?,
            DurableMigrationMode::Apply => runner.apply_pending().await?,
            DurableMigrationMode::Adopt => runner.adopt_applied().await?,
        };

        let migration_status = validate_startup_migration_report(&report, self.migration_mode)?;
        Ok(DurableStartupPreflight::checked(migration_status))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresTlsRuntimeMode {
    LocalNoTls,
    HostedTlsRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurablePostgresRuntimePosture {
    pool_max_size: usize,
    connect_timeout: Duration,
    operation_timeout: Duration,
    pool_acquire_timeout: Duration,
    tls_mode: PostgresTlsRuntimeMode,
}

impl DurablePostgresRuntimePosture {
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
        require_hosted_posture: bool,
    ) -> Result<Self, VfsError> {
        if require_hosted_posture {
            return Ok(Self {
                pool_max_size: required_storage_positive_usize(
                    lookup,
                    POSTGRES_POOL_MAX_SIZE_ENV,
                    POSTGRES_POOL_MAX_SIZE_MAX,
                )?,
                connect_timeout: required_storage_positive_duration_ms(
                    lookup,
                    POSTGRES_CONNECT_TIMEOUT_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
                operation_timeout: required_storage_positive_duration_ms(
                    lookup,
                    POSTGRES_OPERATION_TIMEOUT_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
                pool_acquire_timeout: required_storage_positive_duration_ms(
                    lookup,
                    POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
                tls_mode: PostgresTlsRuntimeMode::LocalNoTls,
            });
        }

        Ok(Self::local_defaults())
    }

    pub(crate) fn local_defaults() -> Self {
        Self {
            pool_max_size: 8,
            connect_timeout: Duration::from_millis(5000),
            operation_timeout: Duration::from_millis(30000),
            pool_acquire_timeout: Duration::from_millis(5000),
            tls_mode: PostgresTlsRuntimeMode::LocalNoTls,
        }
    }

    pub(crate) fn with_tls_mode(mut self, tls_mode: PostgresTlsRuntimeMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    pub fn pool_max_size(&self) -> usize {
        self.pool_max_size
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub fn pool_acquire_timeout(&self) -> Duration {
        self.pool_acquire_timeout
    }

    pub fn tls_mode(&self) -> PostgresTlsRuntimeMode {
        self.tls_mode
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn for_test(
        pool_max_size: usize,
        connect_timeout: Duration,
        operation_timeout: Duration,
        pool_acquire_timeout: Duration,
        tls_mode: PostgresTlsRuntimeMode,
    ) -> Self {
        Self {
            pool_max_size,
            connect_timeout,
            operation_timeout,
            pool_acquire_timeout,
            tls_mode,
        }
    }
}

#[cfg(not(feature = "postgres"))]
fn postgres_tls_runtime_mode(
    _postgres_url: &str,
    _require_hosted_posture: bool,
) -> Result<PostgresTlsRuntimeMode, VfsError> {
    Ok(PostgresTlsRuntimeMode::LocalNoTls)
}

#[cfg(feature = "postgres")]
fn postgres_tls_runtime_mode(
    postgres_url: &str,
    _require_hosted_posture: bool,
) -> Result<PostgresTlsRuntimeMode, VfsError> {
    let config = parse_postgres_config(postgres_url)?;
    validate_postgres_runtime_target(&config)
}

#[cfg(feature = "postgres")]
fn parse_postgres_config(postgres_url: &str) -> Result<tokio_postgres::Config, VfsError> {
    postgres_url
        .parse::<tokio_postgres::Config>()
        .map_err(|_| VfsError::InvalidArgs {
            message: format!(
                "invalid {POSTGRES_URL_ENV}; expected a Postgres connection string without an embedded password"
            ),
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableObjectStoreOperationPosture {
    request_timeout: Duration,
    connect_timeout: Duration,
    max_attempts: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
}

impl DurableObjectStoreOperationPosture {
    pub(crate) fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
        require_hosted_posture: bool,
    ) -> Result<Self, VfsError> {
        if require_hosted_posture {
            return Ok(Self {
                request_timeout: required_storage_positive_duration_ms(
                    lookup,
                    R2_REQUEST_TIMEOUT_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
                connect_timeout: required_storage_positive_duration_ms(
                    lookup,
                    R2_CONNECT_TIMEOUT_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
                max_attempts: required_storage_positive_u32(
                    lookup,
                    R2_MAX_ATTEMPTS_ENV,
                    R2_MAX_ATTEMPTS_MAX,
                )?,
                retry_base_delay: required_storage_positive_duration_ms(
                    lookup,
                    R2_RETRY_BASE_DELAY_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
                retry_max_delay: required_storage_positive_duration_ms(
                    lookup,
                    R2_RETRY_MAX_DELAY_MS_ENV,
                    STORAGE_TIMEOUT_MAX_MS,
                )?,
            });
        }

        Ok(Self::local_defaults())
    }

    pub(crate) fn from_optional_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, VfsError> {
        let defaults = Self::local_defaults();
        Ok(Self {
            request_timeout: optional_storage_positive_duration_ms(
                lookup,
                R2_REQUEST_TIMEOUT_MS_ENV,
                STORAGE_TIMEOUT_MAX_MS,
            )?
            .unwrap_or_else(|| defaults.request_timeout()),
            connect_timeout: optional_storage_positive_duration_ms(
                lookup,
                R2_CONNECT_TIMEOUT_MS_ENV,
                STORAGE_TIMEOUT_MAX_MS,
            )?
            .unwrap_or_else(|| defaults.connect_timeout()),
            max_attempts: optional_storage_positive_u32(
                lookup,
                R2_MAX_ATTEMPTS_ENV,
                R2_MAX_ATTEMPTS_MAX,
            )?
            .unwrap_or_else(|| defaults.max_attempts()),
            retry_base_delay: optional_storage_positive_duration_ms(
                lookup,
                R2_RETRY_BASE_DELAY_MS_ENV,
                STORAGE_TIMEOUT_MAX_MS,
            )?
            .unwrap_or_else(|| defaults.retry_base_delay()),
            retry_max_delay: optional_storage_positive_duration_ms(
                lookup,
                R2_RETRY_MAX_DELAY_MS_ENV,
                STORAGE_TIMEOUT_MAX_MS,
            )?
            .unwrap_or_else(|| defaults.retry_max_delay()),
        })
    }

    pub(crate) fn local_defaults() -> Self {
        Self {
            request_timeout: Duration::from_millis(30000),
            connect_timeout: Duration::from_millis(5000),
            max_attempts: 3,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_millis(5000),
        }
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn retry_base_delay(&self) -> Duration {
        self.retry_base_delay
    }

    pub fn retry_max_delay(&self) -> Duration {
        self.retry_max_delay
    }
}

fn optional_storage_positive_duration_ms(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: u64,
) -> Result<Option<Duration>, VfsError> {
    optional_value(lookup, name)
        .map(|value| parse_positive_u64(name, &value, max).map(Duration::from_millis))
        .transpose()
}

fn optional_storage_positive_u32(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    max: u32,
) -> Result<Option<u32>, VfsError> {
    optional_value(lookup, name)
        .map(|value| parse_positive_u32(name, &value, max))
        .transpose()
}

#[cfg(feature = "postgres")]
fn validate_postgres_runtime_target(
    config: &tokio_postgres::Config,
) -> Result<PostgresTlsRuntimeMode, VfsError> {
    if config.get_ssl_mode() == SslMode::Require {
        return Ok(PostgresTlsRuntimeMode::HostedTlsRequired);
    }

    let hosts = config.get_hosts();
    let hostaddrs = config.get_hostaddrs();
    let has_runtime_target = !hosts.is_empty() || !hostaddrs.is_empty();
    if !has_runtime_target
        || !hosts.iter().all(is_no_tls_runtime_host_allowed)
        || !hostaddrs.iter().all(is_no_tls_runtime_hostaddr_allowed)
    {
        return Err(VfsError::NotSupported {
            message: format!(
                "{POSTGRES_URL_ENV} remote Postgres targets must set sslmode=require; local targets may use localhost, 127.0.0.1, ::1, loopback hostaddr, or a Unix socket path without TLS"
            ),
        });
    }

    Ok(PostgresTlsRuntimeMode::LocalNoTls)
}

#[cfg(feature = "postgres")]
fn is_no_tls_runtime_hostaddr_allowed(hostaddr: &IpAddr) -> bool {
    hostaddr.is_loopback()
}

#[cfg(feature = "postgres")]
fn is_no_tls_runtime_host_allowed(host: &Host) -> bool {
    match host {
        Host::Tcp(host) => matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1"),
        #[cfg(unix)]
        Host::Unix(_) => true,
    }
}

#[cfg(feature = "postgres")]
fn validate_startup_migration_report(
    report: &crate::backend::postgres_migrations::PostgresMigrationReport,
    migration_mode: DurableMigrationMode,
) -> Result<DurableMigrationPreflightStatus, VfsError> {
    use crate::backend::postgres_migrations::PostgresMigrationStatus;

    for status in &report.statuses {
        match status {
            PostgresMigrationStatus::Applied { .. } => {}
            PostgresMigrationStatus::Pending { .. }
                if migration_mode == DurableMigrationMode::Status =>
            {
                return Err(VfsError::InvalidArgs {
                    message: format!(
                        "Postgres migrations are pending; set {DURABLE_MIGRATION_MODE_ENV}=apply to apply them, or `adopt` only for verified legacy schemas"
                    ),
                });
            }
            PostgresMigrationStatus::Pending { version, .. } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} is still pending after migration preflight; refusing durable startup"
                    ),
                });
            }
            PostgresMigrationStatus::Dirty {
                version,
                name: _,
                state: _,
            } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} is dirty; refusing durable startup"
                    ),
                });
            }
            PostgresMigrationStatus::ChecksumMismatch { version, name: _ } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} has a checksum or name mismatch; refusing durable startup"
                    ),
                });
            }
            PostgresMigrationStatus::UnknownApplied { version, name: _ } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration table contains unknown applied version {version}; refusing durable startup"
                    ),
                });
            }
        }
    }

    Ok(match migration_mode {
        DurableMigrationMode::Status => DurableMigrationPreflightStatus::Checked,
        DurableMigrationMode::Apply | DurableMigrationMode::Adopt => {
            DurableMigrationPreflightStatus::Applied
        }
    })
}

impl fmt::Debug for DurableBackendRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableBackendRuntimeConfig")
            .field("postgres_url_configured", &self.postgres_url_configured)
            .field("postgres_schema", &self.postgres_schema)
            .field("postgres_posture", &self.postgres_posture)
            .field("migration_mode", &self.migration_mode)
            .field("object_store", &self.object_store)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DurableObjectStoreRuntimeConfig {
    pub bucket: String,
    pub endpoint: String,
    pub access_key_id_configured: bool,
    pub secret_access_key_configured: bool,
    pub region: String,
    pub prefix: String,
    operation_posture: DurableObjectStoreOperationPosture,
}

impl DurableObjectStoreRuntimeConfig {
    pub fn operation_posture(&self) -> &DurableObjectStoreOperationPosture {
        &self.operation_posture
    }
}

impl fmt::Debug for DurableObjectStoreRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableObjectStoreRuntimeConfig")
            .field("bucket", &"<redacted>")
            .field("endpoint_configured", &!self.endpoint.is_empty())
            .field("access_key_id_configured", &self.access_key_id_configured)
            .field(
                "secret_access_key_configured",
                &self.secret_access_key_configured,
            )
            .field("region", &self.region)
            .field("prefix", &"<redacted>")
            .field("operation_posture", &self.operation_posture)
            .finish()
    }
}

fn required_value(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    missing: &mut Vec<&'static str>,
) -> Option<String> {
    match lookup(name).map(|value| value.trim().to_string()) {
        Some(value) if !value.is_empty() => Some(value),
        _ => {
            missing.push(name);
            None
        }
    }
}

fn optional_value(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn postgres_url_contains_password(value: &str) -> bool {
    uri_userinfo_contains_password(value)
        || password_param_regex().is_match(value)
        || query_contains_sensitive_key(value, &["password"])
}

fn uri_userinfo_contains_password(value: &str) -> bool {
    let Some(scheme_index) = value.find("://") else {
        return false;
    };
    let after_scheme = &value[(scheme_index + 3)..];
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let Some(at_index) = authority.rfind('@') else {
        return false;
    };
    authority[..at_index].contains(':')
}

fn endpoint_has_rejected_parts(value: &str) -> bool {
    uri_contains_userinfo(value) || endpoint_contains_query(value)
}

fn endpoint_contains_query(value: &str) -> bool {
    value
        .split_once('?')
        .is_some_and(|(_, query)| !query.split('#').next().unwrap_or_default().is_empty())
}

pub(crate) fn validate_r2_endpoint(
    endpoint: &str,
    allow_insecure_local_endpoint: bool,
) -> Result<(), VfsError> {
    if endpoint_has_rejected_parts(endpoint) {
        return Err(VfsError::InvalidArgs {
            message: format!("{R2_ENDPOINT_ENV} must not include userinfo or query parameters"),
        });
    }

    let Some((scheme, _)) = endpoint.split_once("://") else {
        return Err(invalid_r2_endpoint_scheme());
    };
    if r2_endpoint_host(endpoint).is_none() {
        return Err(invalid_r2_endpoint_scheme());
    }

    match scheme.to_ascii_lowercase().as_str() {
        "https" => Ok(()),
        "http" if allow_insecure_local_endpoint && r2_endpoint_host_is_loopback(endpoint) => Ok(()),
        _ => Err(invalid_r2_endpoint_scheme()),
    }
}

pub(crate) fn r2_allow_insecure_local_endpoint_from_lookup(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> bool {
    optional_value(lookup, R2_ALLOW_INSECURE_LOCAL_ENDPOINT_ENV).as_deref() == Some("1")
}

fn invalid_r2_endpoint_scheme() -> VfsError {
    VfsError::InvalidArgs {
        message: format!(
            "{R2_ENDPOINT_ENV} must use https; http is allowed only for loopback endpoints when {R2_ALLOW_INSECURE_LOCAL_ENDPOINT_ENV}=1"
        ),
    }
}

fn r2_endpoint_host_is_loopback(endpoint: &str) -> bool {
    let Some(host) = r2_endpoint_host(endpoint) else {
        return false;
    };

    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|addr| addr.is_loopback())
}

fn r2_endpoint_host(endpoint: &str) -> Option<&str> {
    let (_, after_scheme) = endpoint.split_once("://")?;
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.is_empty() {
        return None;
    }
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = if let Some(rest) = authority.strip_prefix('[') {
        let (host, _) = rest.split_once(']')?;
        host
    } else if authority.matches(':').count() == 1 {
        authority
            .split_once(':')
            .map_or(authority, |(host, _)| host)
    } else {
        authority
    };
    (!host.is_empty()).then_some(host)
}

fn uri_contains_userinfo(value: &str) -> bool {
    let Some(scheme_index) = value.find("://") else {
        return false;
    };
    let after_scheme = &value[(scheme_index + 3)..];
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    authority.contains('@')
}

fn query_contains_sensitive_key(value: &str, sensitive_keys: &[&str]) -> bool {
    let Some(query) = value.split_once('?').map(|(_, query)| query) else {
        return false;
    };
    let query = query.split('#').next().unwrap_or_default();
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .any(|part| {
            let key = part.split_once('=').map_or(part, |(key, _)| key);
            let decoded = urlencoding::decode(key)
                .map(|key| key.into_owned())
                .unwrap_or_else(|_| key.to_string());
            let normalized = decoded.trim().to_ascii_lowercase();
            sensitive_keys
                .iter()
                .any(|candidate| normalized == *candidate)
        })
}

fn password_param_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(^|[\s?&])password\s*=").expect("password detection regex should compile")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::RepoId;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use std::collections::BTreeMap;

    fn lookup(entries: &[(&str, &str)]) -> impl FnMut(&str) -> Option<String> {
        let values: BTreeMap<String, String> = entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |name| values.get(name).cloned()
    }

    #[test]
    fn secret_replay_kms_missing_config_is_disabled() {
        let runtime = BackendRuntimeConfig::from_lookup(lookup(&[])).unwrap();

        assert!(runtime.secret_replay_kms().unwrap().is_none());
        assert_eq!(runtime.mode(), BackendRuntimeMode::Local);
    }

    #[test]
    fn secret_replay_kms_local_aead_configures_provider() {
        let key = BASE64.encode([9u8; 32]);
        let runtime = BackendRuntimeConfig::from_lookup(lookup(&[
            (SECRET_REPLAY_KMS_PROVIDER_ENV, "local-aead"),
            (SECRET_REPLAY_KMS_KEY_ID_ENV, "test-key"),
            (SECRET_REPLAY_KMS_KEY_B64_ENV, &key),
        ]))
        .unwrap();

        let provider = runtime.secret_replay_kms().unwrap().unwrap();
        assert_eq!(provider.key_id(), "test-key");
        assert!(!provider.key_hash().is_empty());
    }

    #[test]
    fn secret_replay_kms_rejects_malformed_local_key_without_raw_value() {
        let raw_secret = "not-base64-raw-secret";
        let err = BackendRuntimeConfig::from_lookup(lookup(&[
            (SECRET_REPLAY_KMS_PROVIDER_ENV, "local-aead"),
            (SECRET_REPLAY_KMS_KEY_ID_ENV, "test-key"),
            (SECRET_REPLAY_KMS_KEY_B64_ENV, raw_secret),
        ]))
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains(SECRET_REPLAY_KMS_KEY_B64_ENV));
        assert!(!message.contains(raw_secret));
    }

    #[test]
    fn secret_replay_kms_rejects_oversized_key_id() {
        let key = BASE64.encode([11u8; 32]);
        let raw_key_id = "x".repeat(256);
        let err = BackendRuntimeConfig::from_lookup(lookup(&[
            (SECRET_REPLAY_KMS_PROVIDER_ENV, "local-aead"),
            (SECRET_REPLAY_KMS_KEY_ID_ENV, &raw_key_id),
            (SECRET_REPLAY_KMS_KEY_B64_ENV, &key),
        ]))
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains(SECRET_REPLAY_KMS_KEY_ID_ENV));
        assert!(!message.contains(&raw_key_id));
        assert!(!message.contains(&key));
    }

    #[test]
    fn secret_replay_kms_debug_redacts_key_material() {
        let key = BASE64.encode([11u8; 32]);
        let runtime = BackendRuntimeConfig::from_lookup(lookup(&[
            (SECRET_REPLAY_KMS_PROVIDER_ENV, "local-aead"),
            (SECRET_REPLAY_KMS_KEY_ID_ENV, "test-key"),
            (SECRET_REPLAY_KMS_KEY_B64_ENV, &key),
        ]))
        .unwrap();

        let rendered = format!("{runtime:?}");
        assert!(!rendered.contains(&key));
        assert!(!rendered.contains("CwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCws="));
        assert!(rendered.contains("key_material_configured"));
    }

    fn durable_entries() -> Vec<(&'static str, &'static str)> {
        vec![
            (BACKEND_ENV, "durable"),
            (
                POSTGRES_URL_ENV,
                "postgresql://stratum-db.internal/stratum?sslmode=require",
            ),
            (R2_BUCKET_ENV, "stratum-prod"),
            (R2_ENDPOINT_ENV, "https://account.r2.cloudflarestorage.com"),
            (R2_ACCESS_KEY_ID_ENV, "test-access-key-id"),
            (R2_SECRET_ACCESS_KEY_ENV, "test-secret-access-key"),
        ]
    }

    fn durable_core_entries() -> Vec<(&'static str, &'static str)> {
        let mut entries = durable_entries();
        entries.push((CORE_RUNTIME_ENV, "durable-cloud"));
        entries.push((DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV, "1"));
        entries.push((DURABLE_AUTH_SESSION_READY_ENV, "1"));
        entries.push((DURABLE_POLICY_READY_ENV, "1"));
        entries.push((DURABLE_REPO_ROUTING_READY_ENV, "1"));
        entries.push((DURABLE_RECOVERY_READY_ENV, "1"));
        entries.push((DURABLE_CORE_REPO_ID_ENV, "repo_durable_core"));
        entries.push((IDEMPOTENCY_COMPLETED_RETENTION_SECONDS_ENV, "86400"));
        entries.push((IDEMPOTENCY_PENDING_STALE_SECONDS_ENV, "3600"));
        entries.push((IDEMPOTENCY_MAX_RECORDS_PER_SCOPE_ENV, "10000"));
        entries
    }

    fn durable_core_storage_posture_entries() -> Vec<(&'static str, &'static str)> {
        vec![
            (POSTGRES_POOL_MAX_SIZE_ENV, "16"),
            (POSTGRES_CONNECT_TIMEOUT_MS_ENV, "5000"),
            (POSTGRES_OPERATION_TIMEOUT_MS_ENV, "30000"),
            (POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV, "5000"),
            (R2_REQUEST_TIMEOUT_MS_ENV, "30000"),
            (R2_CONNECT_TIMEOUT_MS_ENV, "5000"),
            (R2_MAX_ATTEMPTS_ENV, "3"),
            (R2_RETRY_BASE_DELAY_MS_ENV, "100"),
            (R2_RETRY_MAX_DELAY_MS_ENV, "5000"),
        ]
    }

    fn complete_durable_core_entries() -> Vec<(&'static str, &'static str)> {
        let mut entries = durable_core_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://stratum-db.internal/stratum?sslmode=require",
        ));
        entries.extend(durable_core_storage_posture_entries());
        entries
    }

    fn core_runtime_config(value: &str) -> BackendRuntimeConfig {
        BackendRuntimeConfig::from_lookup(lookup(&[(CORE_RUNTIME_ENV, value)])).unwrap()
    }

    #[cfg(feature = "postgres")]
    fn durable_config_with_postgres_url(postgres_url: &str) -> DurableBackendRuntimeConfig {
        DurableBackendRuntimeConfig {
            postgres_url_configured: true,
            postgres_url: postgres_url.to_string(),
            postgres_schema: "public".to_string(),
            postgres_posture: DurablePostgresRuntimePosture::local_defaults(),
            migration_mode: DurableMigrationMode::Status,
            object_store: DurableObjectStoreRuntimeConfig {
                bucket: "stratum-prod".to_string(),
                endpoint: "https://account.r2.cloudflarestorage.com".to_string(),
                access_key_id_configured: true,
                secret_access_key_configured: true,
                region: "auto".to_string(),
                prefix: "stratum".to_string(),
                operation_posture: DurableObjectStoreOperationPosture::local_defaults(),
            },
        }
    }

    #[cfg(feature = "postgres")]
    static PGPASSWORD_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(feature = "postgres")]
    struct PgPasswordEnvGuard {
        original: Option<std::ffi::OsString>,
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    #[cfg(feature = "postgres")]
    impl PgPasswordEnvGuard {
        fn set(value: Option<&str>) -> Self {
            let guard = PGPASSWORD_TEST_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original = std::env::var_os("PGPASSWORD");
            match value {
                Some(value) => {
                    // SAFETY: these tests serialize PGPASSWORD mutation with a
                    // process-wide mutex and restore the original value on drop.
                    unsafe { std::env::set_var("PGPASSWORD", value) };
                }
                None => {
                    // SAFETY: these tests serialize PGPASSWORD mutation with a
                    // process-wide mutex and restore the original value on drop.
                    unsafe { std::env::remove_var("PGPASSWORD") };
                }
            }

            Self {
                original,
                _guard: guard,
            }
        }

        #[cfg(unix)]
        fn set_invalid(value: std::ffi::OsString) -> Self {
            let guard = PGPASSWORD_TEST_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original = std::env::var_os("PGPASSWORD");
            // SAFETY: these tests serialize PGPASSWORD mutation with a
            // process-wide mutex and restore the original value on drop.
            unsafe { std::env::set_var("PGPASSWORD", value) };

            Self {
                original,
                _guard: guard,
            }
        }
    }

    #[cfg(feature = "postgres")]
    impl Drop for PgPasswordEnvGuard {
        fn drop(&mut self) {
            match self.original.as_ref() {
                Some(value) => {
                    // SAFETY: the guard still holds PGPASSWORD_TEST_LOCK while
                    // restoring the process environment for this test.
                    unsafe { std::env::set_var("PGPASSWORD", value) };
                }
                None => {
                    // SAFETY: the guard still holds PGPASSWORD_TEST_LOCK while
                    // restoring the process environment for this test.
                    unsafe { std::env::remove_var("PGPASSWORD") };
                }
            }
        }
    }

    #[test]
    fn defaults_to_local_backend() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[])).unwrap();

        assert_eq!(config.mode(), BackendRuntimeMode::Local);
        assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::LocalState);
        assert_eq!(
            config.durable_auth_session_readiness(),
            DurableAuthSessionReadiness::NotRequiredForLocalState
        );
        assert!(!config.guarded_durable_commit_route_enabled());
        assert!(config.durable().is_none());
        config.ensure_supported_for_server().unwrap();
    }

    #[test]
    fn recovery_scheduler_defaults_to_enabled_runtime_config() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[])).unwrap();
        let scheduler = config.recovery_scheduler();

        assert_eq!(scheduler.mode(), RecoverySchedulerMode::Enabled);
        assert!(scheduler.enabled());
        assert_eq!(scheduler.interval(), Duration::from_millis(5000));
        assert_eq!(scheduler.tick_limit(), 10);
        assert_eq!(scheduler.lease_duration(), Duration::from_millis(30000));
        assert!(!scheduler.shutdown_drain_enabled());
        assert_eq!(
            scheduler.shutdown_drain_timeout(),
            Duration::from_millis(2500)
        );
    }

    #[test]
    fn recovery_scheduler_parses_explicit_disabled_runtime_config() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[
            (RECOVERY_SCHEDULER_ENV, " disabled "),
            (RECOVERY_SCHEDULER_INTERVAL_MS_ENV, "250"),
            (RECOVERY_SCHEDULER_TICK_LIMIT_ENV, "100"),
            (RECOVERY_SCHEDULER_LEASE_MS_ENV, "1000"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV, "enabled"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "30000"),
        ]))
        .unwrap();
        let scheduler = config.recovery_scheduler();

        assert_eq!(scheduler.mode(), RecoverySchedulerMode::Disabled);
        assert!(!scheduler.enabled());
        assert_eq!(scheduler.interval(), Duration::from_millis(250));
        assert_eq!(scheduler.tick_limit(), 100);
        assert_eq!(scheduler.lease_duration(), Duration::from_millis(1000));
        assert!(scheduler.shutdown_drain_enabled());
        assert_eq!(
            scheduler.shutdown_drain_timeout(),
            Duration::from_millis(30000)
        );
    }

    #[test]
    fn recovery_scheduler_rejects_invalid_modes_without_leaking_raw_value() {
        for (env_name, raw_value) in [
            (RECOVERY_SCHEDULER_ENV, "raw-secret-scheduler-mode"),
            (
                RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_ENV,
                "raw-secret-drain-mode",
            ),
        ] {
            let err = BackendRuntimeConfig::from_lookup(lookup(&[(env_name, raw_value)]))
                .expect_err("invalid recovery scheduler mode should fail");
            let message = err.to_string();

            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(message.contains(env_name), "{message}");
            assert!(message.contains("expected enabled or disabled"));
            assert!(!message.contains(raw_value));
        }
    }

    #[test]
    fn recovery_scheduler_rejects_invalid_numeric_knobs_without_leaking_values() {
        for (env_name, raw_value) in [
            (RECOVERY_SCHEDULER_INTERVAL_MS_ENV, "0"),
            (RECOVERY_SCHEDULER_INTERVAL_MS_ENV, "300001"),
            (RECOVERY_SCHEDULER_TICK_LIMIT_ENV, "0"),
            (RECOVERY_SCHEDULER_TICK_LIMIT_ENV, "101"),
            (RECOVERY_SCHEDULER_LEASE_MS_ENV, "0"),
            (RECOVERY_SCHEDULER_LEASE_MS_ENV, "300001"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "0"),
            (RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV, "30001"),
            (
                RECOVERY_SCHEDULER_SHUTDOWN_DRAIN_TIMEOUT_MS_ENV,
                "raw-secret-timeout",
            ),
        ] {
            let err = BackendRuntimeConfig::from_lookup(lookup(&[(env_name, raw_value)]))
                .expect_err("invalid recovery scheduler numeric knob should fail");
            let message = err.to_string();

            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(message.contains(env_name), "{message}");
            assert!(message.contains("positive bounded integer"));
            assert!(!message.contains(raw_value), "{message}");
        }
    }

    #[test]
    fn recovery_scheduler_debug_redacts_adjacent_sensitive_runtime_values() {
        let key = BASE64.encode([7u8; 32]);
        let config = BackendRuntimeConfig::from_lookup(lookup(&[
            (BACKEND_ENV, "durable"),
            (
                POSTGRES_URL_ENV,
                "postgresql://stratum-db.internal/stratum?sslmode=require",
            ),
            (R2_BUCKET_ENV, "stratum-prod"),
            (R2_ENDPOINT_ENV, "https://account.r2.cloudflarestorage.com"),
            (R2_ACCESS_KEY_ID_ENV, "test-access-key-id"),
            (R2_SECRET_ACCESS_KEY_ENV, "test-secret-access-key"),
            (RECOVERY_SCHEDULER_ENV, "disabled"),
            (RECOVERY_SCHEDULER_INTERVAL_MS_ENV, "250"),
            (SECRET_REPLAY_KMS_PROVIDER_ENV, "local-aead"),
            (SECRET_REPLAY_KMS_KEY_ID_ENV, "debug-key"),
            (SECRET_REPLAY_KMS_KEY_B64_ENV, &key),
        ]))
        .unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("recovery_scheduler"));
        assert!(debug.contains("Disabled"));
        assert!(!debug.contains(&key));
        assert!(!debug.contains("test-access-key-id"));
        assert!(!debug.contains("test-secret-access-key"));
        assert!(!debug.contains("postgresql://stratum-db.internal/stratum"));
        assert!(!debug.contains("account.r2.cloudflarestorage.com"));
    }

    #[test]
    fn guarded_durable_commit_route_requires_durable_backend() {
        let local = BackendRuntimeConfig::from_lookup(lookup(&[
            (BACKEND_ENV, "local"),
            (DURABLE_COMMIT_ROUTE_ENV, "1"),
        ]))
        .unwrap();

        let err = local
            .ensure_supported_for_server()
            .expect_err("guarded durable commit route requires durable backend stores");
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(err.to_string().contains(DURABLE_COMMIT_ROUTE_ENV));

        let mut entries = durable_entries();
        entries.push((DURABLE_COMMIT_ROUTE_ENV, "1"));
        let durable = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();
        assert!(durable.guarded_durable_commit_route_enabled());
        assert_durable_server_support(&durable);
    }

    #[test]
    fn guarded_durable_commit_route_rejects_unknown_values_without_leaking_raw_value() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[(
            DURABLE_COMMIT_ROUTE_ENV,
            "raw-secret-route-flag",
        )]))
        .expect_err("unknown durable commit route mode should fail");

        let message = err.to_string();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(DURABLE_COMMIT_ROUTE_ENV));
        assert!(message.contains("expected"));
        assert!(!message.contains("raw-secret-route-flag"));
    }

    #[test]
    fn core_runtime_defaults_empty_values_to_local_state() {
        for value in ["", "   "] {
            let config = core_runtime_config(value);

            assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::LocalState);
        }
    }

    #[test]
    fn core_runtime_accepts_local_state_aliases() {
        for value in ["local", "local-state", "state-file", "snapshot"] {
            let config = core_runtime_config(value);

            assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::LocalState);
        }
    }

    #[test]
    fn core_runtime_accepts_durable_cloud_aliases() {
        for value in ["durable", "durable-cloud", "postgres-r2"] {
            let mut entries = complete_durable_core_entries();
            entries.retain(|(key, _)| *key != CORE_RUNTIME_ENV);
            entries.push((CORE_RUNTIME_ENV, value));
            let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();

            assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::DurableCloud);
            assert!(config.durable_core_runtime_ready());
            assert_eq!(
                config.durable_core_repo_id().map(RepoId::as_str),
                Some("repo_durable_core")
            );
        }
    }

    #[test]
    fn non_server_local_state_runtime_guard_accepts_local_state_values() {
        for value in ["", "   ", "local", "local-state", "state-file", "snapshot"] {
            ensure_local_state_runtime_for_non_server_surface_from_lookup(
                NonServerRuntimeSurface::StratumMcp,
                lookup(&[(CORE_RUNTIME_ENV, value)]),
            )
            .unwrap();
        }
    }

    #[test]
    fn non_server_local_state_runtime_guard_rejects_durable_cloud_aliases() {
        for value in ["durable", "durable-cloud", "postgres-r2"] {
            let err = ensure_local_state_runtime_for_non_server_surface_from_lookup(
                NonServerRuntimeSurface::StratumMcp,
                lookup(&[(CORE_RUNTIME_ENV, value)]),
            )
            .expect_err("non-server local-state guard must reject durable-cloud mode");

            let message = err.to_string();
            assert!(matches!(err, VfsError::NotSupported { .. }));
            assert!(message.contains("stratum-mcp"));
            assert!(message.contains(CORE_RUNTIME_ENV));
            assert!(!message.contains(value));
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_server_local_state_runtime_guard_rejects_non_unicode_env_value() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let err = core_runtime_env_value_from_result(Err(VarError::NotUnicode(
            OsString::from_vec(vec![0xff]),
        )))
        .expect_err("non-unicode core runtime must fail closed");

        let message = err.to_string();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(CORE_RUNTIME_ENV));
        assert!(message.contains("expected"));
        assert!(!message.contains("0xff"));
    }

    #[test]
    fn non_server_local_state_runtime_guard_preserves_unknown_runtime_error() {
        let err = ensure_local_state_runtime_for_non_server_surface_from_lookup(
            NonServerRuntimeSurface::StratumMcp,
            lookup(&[(CORE_RUNTIME_ENV, "raw-secret-runtime")]),
        )
        .expect_err("unknown core runtime should fail");

        let message = err.to_string();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(CORE_RUNTIME_ENV));
        assert!(message.contains("expected"));
        assert!(!message.contains("raw-secret-runtime"));
    }

    #[test]
    fn non_server_local_state_runtime_guard_reads_only_core_runtime_env() {
        let mut read_names = Vec::new();
        let result = ensure_local_state_runtime_for_non_server_surface_from_lookup(
            NonServerRuntimeSurface::StratumMcp,
            |name| {
                read_names.push(name.to_string());
                match name {
                    CORE_RUNTIME_ENV => Some("durable-cloud".to_string()),
                    POSTGRES_URL_ENV => {
                        Some("postgresql://user:raw-db-password-123@localhost/stratum".to_string())
                    }
                    R2_SECRET_ACCESS_KEY_ENV => Some("raw-secret-access-key".to_string()),
                    _ => None,
                }
            },
        );

        let message = result
            .expect_err("durable-cloud should be rejected")
            .to_string();
        assert_eq!(read_names, vec![CORE_RUNTIME_ENV]);
        assert!(!message.contains("raw-db-password-123"));
        assert!(!message.contains("raw-secret-access-key"));
    }

    #[test]
    fn core_runtime_rejects_unknown_values_without_leaking_raw_value() {
        let err =
            BackendRuntimeConfig::from_lookup(lookup(&[(CORE_RUNTIME_ENV, "raw-secret-runtime")]))
                .expect_err("unknown core runtime should fail");

        let message = err.to_string();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(CORE_RUNTIME_ENV));
        assert!(message.contains("expected"));
        assert!(!message.contains("raw-secret-runtime"));
    }

    #[test]
    fn debug_output_includes_core_runtime_mode_without_durable_values() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[
            (BACKEND_ENV, "local"),
            (CORE_RUNTIME_ENV, "local-state"),
            (
                POSTGRES_URL_ENV,
                "postgresql://user:raw-db-password-123@localhost/stratum",
            ),
        ]))
        .unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("core_runtime_mode: LocalState"));
        assert!(!debug.contains("raw-db-password-123"));
    }

    #[test]
    fn durable_control_plane_still_defaults_to_local_state_core_runtime() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        assert_eq!(config.mode(), BackendRuntimeMode::Durable);
        assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::LocalState);
    }

    #[test]
    fn durable_core_runtime_requires_durable_backend_before_parsing_durable_secrets() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[
            (CORE_RUNTIME_ENV, "durable-cloud"),
            (
                POSTGRES_URL_ENV,
                "postgresql://user:raw-db-password-123@localhost/stratum",
            ),
            (
                R2_ENDPOINT_ENV,
                "https://example.invalid?token=raw-r2-token",
            ),
            (R2_ACCESS_KEY_ID_ENV, "raw-access-key-id"),
            (R2_SECRET_ACCESS_KEY_ENV, "raw-secret-access-key"),
        ]))
        .expect_err("durable-cloud core runtime should require durable backend");

        let message = err.to_string();
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(CORE_RUNTIME_ENV));
        assert!(message.contains(BACKEND_ENV));
        assert!(!message.contains("raw-db-password-123"));
        assert!(!message.contains("raw-r2-token"));
        assert!(!message.contains("raw-access-key-id"));
        assert!(!message.contains("raw-secret-access-key"));
    }

    #[test]
    fn durable_core_runtime_missing_readiness_gate_fails_before_durable_env_validation() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[
            (BACKEND_ENV, "durable"),
            (CORE_RUNTIME_ENV, "durable-cloud"),
        ]))
        .expect_err("durable-cloud should require readiness gates before durable env");

        let message = err.to_string();
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV));
        assert!(!message.contains(POSTGRES_URL_ENV));
        assert!(!message.contains(R2_SECRET_ACCESS_KEY_ENV));
    }

    #[test]
    fn durable_core_runtime_with_all_readiness_gates_parses_durable_backend_config() {
        let config =
            BackendRuntimeConfig::from_lookup(lookup(&complete_durable_core_entries())).unwrap();

        let durable = config
            .durable()
            .expect("durable backend config should parse");
        assert_eq!(config.mode(), BackendRuntimeMode::Durable);
        assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::DurableCloud);
        assert!(config.durable_core_runtime_ready());
        assert_eq!(
            config.durable_core_repo_id().map(RepoId::as_str),
            Some("repo_durable_core")
        );
        assert_eq!(durable.object_store().bucket, "stratum-prod");
    }

    #[test]
    fn durable_backend_local_state_uses_conservative_storage_posture_defaults() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();
        let durable = config.durable().expect("durable config should parse");
        let postgres = durable.postgres_posture();
        let object_store = durable.object_store().operation_posture();

        assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::LocalState);
        assert_eq!(postgres.pool_max_size(), 8);
        assert_eq!(postgres.connect_timeout(), Duration::from_millis(5000));
        assert_eq!(postgres.operation_timeout(), Duration::from_millis(30000));
        assert_eq!(postgres.pool_acquire_timeout(), Duration::from_millis(5000));
        #[cfg(feature = "postgres")]
        assert_eq!(
            postgres.tls_mode(),
            PostgresTlsRuntimeMode::HostedTlsRequired
        );
        #[cfg(not(feature = "postgres"))]
        assert_eq!(postgres.tls_mode(), PostgresTlsRuntimeMode::LocalNoTls);
        assert_eq!(object_store.request_timeout(), Duration::from_millis(30000));
        assert_eq!(object_store.connect_timeout(), Duration::from_millis(5000));
        assert_eq!(object_store.max_attempts(), 3);
        assert_eq!(object_store.retry_base_delay(), Duration::from_millis(100));
        assert_eq!(object_store.retry_max_delay(), Duration::from_millis(5000));
    }

    #[test]
    fn durable_core_runtime_requires_explicit_storage_posture_knobs() {
        for env_name in [
            POSTGRES_POOL_MAX_SIZE_ENV,
            POSTGRES_CONNECT_TIMEOUT_MS_ENV,
            POSTGRES_OPERATION_TIMEOUT_MS_ENV,
            POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV,
            R2_REQUEST_TIMEOUT_MS_ENV,
            R2_CONNECT_TIMEOUT_MS_ENV,
            R2_MAX_ATTEMPTS_ENV,
            R2_RETRY_BASE_DELAY_MS_ENV,
            R2_RETRY_MAX_DELAY_MS_ENV,
        ] {
            let mut entries = complete_durable_core_entries();
            entries.retain(|(key, _)| *key != env_name);

            let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
                .expect_err("durable-cloud should require every hosted posture knob");
            let message = err.to_string();

            assert!(matches!(err, VfsError::NotSupported { .. }));
            assert!(message.contains(env_name), "{message}");
            assert!(!message.contains("16"));
            assert!(!message.contains("5000"));
            assert!(!message.contains("30000"));
        }
    }

    #[test]
    fn durable_core_runtime_rejects_invalid_storage_posture_knobs_by_name_only() {
        for (env_name, invalid_value) in [
            (POSTGRES_POOL_MAX_SIZE_ENV, "0"),
            (POSTGRES_POOL_MAX_SIZE_ENV, "257"),
            (POSTGRES_CONNECT_TIMEOUT_MS_ENV, "0"),
            (POSTGRES_CONNECT_TIMEOUT_MS_ENV, "300001"),
            (POSTGRES_OPERATION_TIMEOUT_MS_ENV, "0"),
            (POSTGRES_OPERATION_TIMEOUT_MS_ENV, "300001"),
            (POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV, "0"),
            (POSTGRES_POOL_ACQUIRE_TIMEOUT_MS_ENV, "300001"),
            (R2_REQUEST_TIMEOUT_MS_ENV, "0"),
            (R2_REQUEST_TIMEOUT_MS_ENV, "300001"),
            (R2_CONNECT_TIMEOUT_MS_ENV, "0"),
            (R2_CONNECT_TIMEOUT_MS_ENV, "300001"),
            (R2_MAX_ATTEMPTS_ENV, "0"),
            (R2_MAX_ATTEMPTS_ENV, "11"),
            (R2_RETRY_BASE_DELAY_MS_ENV, "0"),
            (R2_RETRY_BASE_DELAY_MS_ENV, "300001"),
            (R2_RETRY_MAX_DELAY_MS_ENV, "0"),
            (R2_RETRY_MAX_DELAY_MS_ENV, "300001"),
        ] {
            let mut entries = complete_durable_core_entries();
            entries.retain(|(key, _)| *key != env_name);
            entries.push((env_name, invalid_value));

            let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
                .expect_err("invalid durable-cloud posture knob should fail");
            let message = err.to_string();

            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(message.contains(env_name), "{message}");
            assert!(!message.contains(invalid_value));
        }
    }

    #[test]
    fn durable_core_runtime_parses_explicit_storage_posture() {
        let config =
            BackendRuntimeConfig::from_lookup(lookup(&complete_durable_core_entries())).unwrap();
        let durable = config.durable().expect("durable config should parse");
        let postgres = durable.postgres_posture();
        let object_store = durable.object_store().operation_posture();

        assert_eq!(postgres.pool_max_size(), 16);
        assert_eq!(postgres.connect_timeout(), Duration::from_millis(5000));
        assert_eq!(postgres.operation_timeout(), Duration::from_millis(30000));
        assert_eq!(postgres.pool_acquire_timeout(), Duration::from_millis(5000));
        assert_eq!(object_store.request_timeout(), Duration::from_millis(30000));
        assert_eq!(object_store.connect_timeout(), Duration::from_millis(5000));
        assert_eq!(object_store.max_attempts(), 3);
        assert_eq!(object_store.retry_base_delay(), Duration::from_millis(100));
        assert_eq!(object_store.retry_max_delay(), Duration::from_millis(5000));
    }

    #[test]
    fn durable_core_runtime_does_not_accept_guarded_durable_commit_route() {
        let mut entries = complete_durable_core_entries();
        entries.push((DURABLE_COMMIT_ROUTE_ENV, "1"));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("guarded durable commit route is only accepted for local-state core");

        let message = err.to_string();
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(DURABLE_COMMIT_ROUTE_ENV));
        assert!(message.contains(CORE_RUNTIME_ENV));
    }

    #[test]
    fn durable_core_runtime_rejects_missing_repo_id_before_durable_env_validation() {
        let mut entries = durable_core_entries();
        entries.retain(|(key, _)| {
            !matches!(
                *key,
                DURABLE_CORE_REPO_ID_ENV
                    | POSTGRES_URL_ENV
                    | R2_BUCKET_ENV
                    | R2_ENDPOINT_ENV
                    | R2_ACCESS_KEY_ID_ENV
                    | R2_SECRET_ACCESS_KEY_ENV
            )
        });

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("durable-cloud should require explicit durable core repo id");

        let message = err.to_string();
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(DURABLE_CORE_REPO_ID_ENV));
        assert!(!message.contains(POSTGRES_URL_ENV));
    }

    #[test]
    fn durable_core_runtime_rejects_invalid_repo_id_without_leaking_raw_value() {
        let mut entries = durable_core_entries();
        entries.retain(|(key, _)| *key != DURABLE_CORE_REPO_ID_ENV);
        entries.push((DURABLE_CORE_REPO_ID_ENV, "raw invalid repo id"));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("durable-cloud should reject invalid durable core repo id");

        let message = err.to_string();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(DURABLE_CORE_REPO_ID_ENV));
        assert!(!message.contains("raw invalid repo id"));
    }

    #[test]
    fn durable_core_runtime_rejects_local_singleton_repo_id() {
        let mut entries = durable_core_entries();
        entries.retain(|(key, _)| *key != DURABLE_CORE_REPO_ID_ENV);
        entries.push((DURABLE_CORE_REPO_ID_ENV, "local"));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("durable-cloud should reject local singleton repo id");

        let message = err.to_string();
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(DURABLE_CORE_REPO_ID_ENV));
        assert!(message.contains("local singleton"));
    }

    #[test]
    fn accepts_durable_backend_when_required_env_is_present() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let durable = config.durable().expect("durable config should be present");
        assert_eq!(config.mode(), BackendRuntimeMode::Durable);
        assert!(durable.postgres_url_configured());
        assert_eq!(durable.postgres_schema(), "public");
        assert_eq!(durable.migration_mode(), DurableMigrationMode::Status);
        assert_eq!(durable.object_store().bucket, "stratum-prod");
        assert_eq!(durable.object_store().region, "auto");
        assert_eq!(durable.object_store().prefix, "stratum");
        assert_durable_server_support(&config);
    }

    #[test]
    fn durable_backend_accepts_optional_r2_region_and_prefix() {
        let mut entries = durable_entries();
        entries.push((R2_REGION_ENV, "us-east-1"));
        entries.push((R2_PREFIX_ENV, "hosted"));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();
        let object_store = config.durable().unwrap().object_store();

        assert_eq!(object_store.region, "us-east-1");
        assert_eq!(object_store.prefix, "hosted");
    }

    #[test]
    fn durable_backend_defaults_empty_migration_mode_to_status() {
        let mut entries = durable_entries();
        entries.push((DURABLE_MIGRATION_MODE_ENV, "   "));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();

        assert_eq!(
            config.durable().unwrap().migration_mode(),
            DurableMigrationMode::Status
        );
    }

    #[test]
    fn durable_backend_accepts_apply_migration_mode() {
        let mut entries = durable_entries();
        entries.push((DURABLE_MIGRATION_MODE_ENV, " Apply "));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();

        assert_eq!(
            config.durable().unwrap().migration_mode(),
            DurableMigrationMode::Apply
        );
    }

    #[test]
    fn durable_backend_accepts_adopt_migration_mode() {
        let mut entries = durable_entries();
        entries.push((DURABLE_MIGRATION_MODE_ENV, " Adopt "));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();

        assert_eq!(
            config.durable().unwrap().migration_mode(),
            DurableMigrationMode::Adopt
        );
    }

    #[test]
    fn durable_backend_rejects_invalid_migration_mode_without_leaking_values() {
        let mut entries = durable_entries();
        entries.push((DURABLE_MIGRATION_MODE_ENV, "raw-secret-mode"));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("invalid migration mode should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(err.to_string().contains(DURABLE_MIGRATION_MODE_ENV));
        assert!(!err.to_string().contains("raw-secret-mode"));
    }

    #[test]
    fn durable_backend_accepts_postgres_schema_override() {
        let mut entries = durable_entries();
        entries.push((POSTGRES_SCHEMA_ENV, "tenant_schema_01"));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();

        assert_eq!(
            config.durable().unwrap().postgres_schema(),
            "tenant_schema_01"
        );
    }

    #[test]
    fn rejects_unknown_backend() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[(BACKEND_ENV, "postgres")]))
            .expect_err("unknown backend should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(err.to_string().contains("expected `local` or `durable`"));
        assert!(!err.to_string().contains("postgres"));
    }

    #[test]
    fn local_backend_does_not_capture_durable_env_values() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[
            (BACKEND_ENV, "local"),
            (
                POSTGRES_URL_ENV,
                "postgresql://user:raw-db-password-123@localhost/stratum",
            ),
            (R2_ACCESS_KEY_ID_ENV, "test-access-key-id"),
            (R2_SECRET_ACCESS_KEY_ENV, "test-secret-access-key"),
        ]))
        .unwrap();

        let debug = format!("{config:?}");
        assert_eq!(config.mode(), BackendRuntimeMode::Local);
        assert!(config.durable().is_none());
        assert!(!debug.contains("raw-db-password-123"));
        assert!(!debug.contains("test-access-key-id"));
        assert!(!debug.contains("test-secret-access-key"));
    }

    #[test]
    fn durable_backend_reports_missing_required_env_by_name_only() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[(BACKEND_ENV, "durable")]))
            .expect_err("missing durable env should fail");

        let message = err.to_string();
        assert!(message.contains(POSTGRES_URL_ENV));
        assert!(message.contains(R2_SECRET_ACCESS_KEY_ENV));
        assert!(!message.contains("test-secret-access-key"));
    }

    #[test]
    fn durable_backend_treats_empty_required_env_as_missing() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_BUCKET_ENV);
        entries.push((R2_BUCKET_ENV, "   "));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("empty bucket should fail");

        assert!(err.to_string().contains(R2_BUCKET_ENV));
    }

    #[test]
    fn durable_backend_rejects_postgres_uri_password() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://user:raw-db-password-123@localhost/stratum",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("password-bearing URL should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_postgres_query_password() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://localhost/stratum?password=raw-db-password-123",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("query password should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_percent_encoded_postgres_query_password_key() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://localhost/stratum?pass%77ord=raw-db-password-123",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("encoded query password should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_postgres_keyword_password() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "host=localhost password = raw-db-password-123 dbname=stratum",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("keyword password should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_r2_endpoint_query() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((
            R2_ENDPOINT_ENV,
            "https://example.invalid?tok%65n=raw-endpoint-token",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("query-bearing endpoint should fail");

        assert!(
            err.to_string()
                .contains("must not include userinfo or query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-token"));
    }

    #[test]
    fn durable_backend_rejects_remote_plaintext_r2_endpoint() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((R2_ENDPOINT_ENV, "http://account.r2.cloudflarestorage.com"));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("remote plaintext R2 endpoint should fail");
        let message = err.to_string();

        assert!(message.contains(R2_ENDPOINT_ENV));
        assert!(message.contains("https"));
        assert!(!message.contains("account.r2.cloudflarestorage.com"));
    }

    #[test]
    fn durable_backend_accepts_plaintext_loopback_r2_endpoint_with_explicit_opt_in() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((R2_ENDPOINT_ENV, "http://127.0.0.1:9000"));
        entries.push((R2_ALLOW_INSECURE_LOCAL_ENDPOINT_ENV, "1"));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect("loopback plaintext R2 endpoint should be explicit local-test only");

        assert_eq!(
            config.durable().unwrap().object_store().endpoint,
            "http://127.0.0.1:9000"
        );
    }

    #[test]
    fn durable_backend_rejects_r2_endpoint_userinfo() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((
            R2_ENDPOINT_ENV,
            "https://user:raw-endpoint-password@example.invalid",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("userinfo endpoint should fail");

        assert!(
            err.to_string()
                .contains("must not include userinfo or query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-password"));
    }

    #[test]
    fn durable_backend_rejects_r2_endpoint_query_parameters() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((
            R2_ENDPOINT_ENV,
            "https://example.invalid?api_key=raw-endpoint-query-secret",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("query-bearing endpoint should fail");

        assert!(
            err.to_string()
                .contains("must not include userinfo or query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-query-secret"));
    }

    #[test]
    fn debug_output_redacts_durable_secret_values() {
        let mut entries = durable_entries();
        entries.push((R2_PREFIX_ENV, "objects/blob/abcdef0123456789"));
        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("access_key_id_configured: true"));
        assert!(debug.contains("secret_access_key_configured: true"));
        assert!(!debug.contains("test-access-key-id"));
        assert!(!debug.contains("test-secret-access-key"));
        assert!(!debug.contains("postgresql://stratum-db.internal/stratum"));
        assert!(!debug.contains("objects/blob/abcdef0123456789"));
    }

    #[tokio::test]
    async fn local_preflight_is_noop() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[])).unwrap();

        let preflight = config.prepare_server_startup().await.unwrap();

        assert_eq!(
            preflight.migration_status(),
            DurableMigrationPreflightStatus::NotRequired
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn durable_preflight_without_postgres_feature_skips_migration_check() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let preflight = config.prepare_server_startup().await.unwrap();

        assert_eq!(
            preflight.migration_status(),
            DurableMigrationPreflightStatus::NotCheckedPostgresFeatureDisabled
        );
        assert!(matches!(
            config.ensure_supported_for_server(),
            Err(VfsError::NotSupported { .. })
        ));
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn durable_runtime_remains_fail_closed_until_server_wiring_lands() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let err = config
            .ensure_supported_for_server()
            .expect_err("durable runtime should remain fail closed");

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(err.to_string().contains(
            "durable backend runtime requires stratum-server built with the postgres feature"
        ));
    }

    fn assert_durable_server_support(config: &BackendRuntimeConfig) {
        #[cfg(feature = "postgres")]
        config.ensure_supported_for_server().unwrap();

        #[cfg(not(feature = "postgres"))]
        assert!(matches!(
            config.ensure_supported_for_server(),
            Err(VfsError::NotSupported { .. })
        ));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_rejects_parsed_password_without_leaking_password() {
        let durable = durable_config_with_postgres_url(
            "postgresql://user:raw-db-password-123@localhost/stratum",
        );

        let err = durable
            .postgres_config_with_env_password()
            .expect_err("password-bearing parsed config should fail");
        let message = err.to_string();

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(POSTGRES_URL_ENV));
        assert!(!message.contains("raw-db-password-123"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_secret_provider_rejects_provider_failures_without_leaking_secret() {
        struct FailingSecretProvider;

        impl PostgresSecretProvider for FailingSecretProvider {
            fn postgres_password(&self) -> Result<Option<String>, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "raw-provider-secret-123".to_string(),
                })
            }
        }

        let durable = durable_config_with_postgres_url("postgresql://localhost/stratum");

        let err = durable
            .postgres_config_with_secret_provider(&FailingSecretProvider)
            .expect_err("secret provider failures should fail closed");
        let message = err.to_string();

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains("postgres secret resolution failed"));
        assert!(!message.contains("raw-provider-secret-123"));
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn durable_preflight_uses_injected_secret_provider() {
        struct FailingSecretProvider;

        impl PostgresSecretProvider for FailingSecretProvider {
            fn postgres_password(&self) -> Result<Option<String>, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "raw-preflight-secret-123".to_string(),
                })
            }
        }

        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let err = config
            .prepare_server_startup_with_secret_provider(&FailingSecretProvider)
            .await
            .expect_err("preflight should use the injected provider");
        let message = err.to_string();

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains("postgres secret resolution failed"));
        assert!(!message.contains("raw-preflight-secret-123"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_secret_provider_treats_empty_password_as_absent() {
        struct EmptySecretProvider;

        impl PostgresSecretProvider for EmptySecretProvider {
            fn postgres_password(&self) -> Result<Option<String>, VfsError> {
                Ok(Some(String::new()))
            }
        }

        let durable = durable_config_with_postgres_url("postgresql://localhost/stratum");

        let config = durable
            .postgres_config_with_secret_provider(&EmptySecretProvider)
            .expect("empty provider secret should be accepted as absent");

        assert!(config.get_password().is_none());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_rejects_invalid_url_without_leaking_url_material() {
        let durable =
            durable_config_with_postgres_url("postgresql://raw-invalid-url-secret::::/stratum");

        let err = durable
            .postgres_config_with_env_password()
            .expect_err("invalid postgres URL should fail");
        let message = err.to_string();

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains(POSTGRES_URL_ENV));
        assert!(!message.contains("raw-invalid-url-secret"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_rejects_remote_notls_runtime_hosts() {
        let durable = durable_config_with_postgres_url("postgresql://db.internal/stratum");

        let err = durable
            .postgres_config_with_env_password()
            .expect_err("remote NoTLS runtime host should fail closed");
        let message = err.to_string();

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(POSTGRES_URL_ENV));
        assert!(!message.contains("db.internal"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_rejects_remote_notls_hostaddr() {
        let durable =
            durable_config_with_postgres_url("host=localhost hostaddr=10.0.0.1 dbname=stratum");

        let err = durable
            .postgres_config_with_env_password()
            .expect_err("remote NoTLS hostaddr should fail closed");

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(!err.to_string().contains("10.0.0.1"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_accepts_loopback_hostaddr_only_runtime_targets() {
        let durable = durable_config_with_postgres_url("hostaddr=127.0.0.1 dbname=stratum");

        let config = durable.postgres_config_with_env_password().unwrap();

        assert!(config.get_hosts().is_empty());
        assert!(
            config
                .get_hostaddrs()
                .iter()
                .all(std::net::IpAddr::is_loopback)
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn durable_core_runtime_rejects_remote_postgres_without_tls_without_leaking_host() {
        let mut entries = complete_durable_core_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((POSTGRES_URL_ENV, "postgresql://db.internal/stratum"));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("durable-cloud remote Postgres must require TLS");
        let message = err.to_string();

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains(POSTGRES_URL_ENV));
        assert!(message.contains("sslmode=require"));
        assert!(!message.contains("db.internal"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn durable_core_runtime_accepts_remote_postgres_with_tls_at_config_level() {
        let mut entries = complete_durable_core_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://db.internal/stratum?sslmode=require",
        ));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();
        let durable = config.durable().expect("durable config should parse");

        assert_eq!(
            durable.postgres_posture().tls_mode(),
            PostgresTlsRuntimeMode::HostedTlsRequired
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_accepts_tls_required_runtime_targets() {
        let durable =
            durable_config_with_postgres_url("postgresql://db.internal/stratum?sslmode=require");

        let config = durable
            .postgres_config_with_env_password()
            .expect("TLS-required Postgres should be accepted for connector wiring");

        assert_eq!(config.get_ssl_mode(), SslMode::Require);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_accepts_localhost_no_tls_runtime_targets() {
        for url in [
            "postgresql://localhost/stratum",
            "postgresql://127.0.0.1/stratum",
            "postgresql://[::1]/stratum",
        ] {
            let durable = durable_config_with_postgres_url(url);

            let config = durable.postgres_config_with_env_password().unwrap();

            assert_eq!(config.get_ssl_mode(), SslMode::Prefer);
        }
    }

    #[cfg(all(feature = "postgres", unix))]
    #[test]
    fn postgres_config_with_env_password_accepts_unix_socket_runtime_hosts() {
        let durable = durable_config_with_postgres_url("host=/var/run/postgresql dbname=stratum");

        let config = durable.postgres_config_with_env_password().unwrap();

        assert!(matches!(config.get_hosts(), [Host::Unix(_)]));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_accepts_passwordless_url_without_pgpassword() {
        let _guard = PgPasswordEnvGuard::set(None);
        let durable = durable_config_with_postgres_url("postgresql://localhost/stratum");

        let config = durable.postgres_config_with_env_password().unwrap();

        assert!(config.get_password().is_none());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_applies_non_empty_pgpassword() {
        let _guard = PgPasswordEnvGuard::set(Some("raw-env-password-123"));
        let durable = durable_config_with_postgres_url("postgresql://localhost/stratum");

        let config = durable.postgres_config_with_env_password().unwrap();

        assert_eq!(
            config.get_password(),
            Some("raw-env-password-123".as_bytes())
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_config_with_env_password_ignores_empty_pgpassword() {
        let _guard = PgPasswordEnvGuard::set(Some(""));
        let durable = durable_config_with_postgres_url("postgresql://localhost/stratum");

        let config = durable.postgres_config_with_env_password().unwrap();

        assert!(config.get_password().is_none());
    }

    #[cfg(all(feature = "postgres", unix))]
    #[test]
    fn postgres_config_with_env_password_rejects_non_unicode_pgpassword_without_leaking_secret() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let _guard = PgPasswordEnvGuard::set_invalid(OsString::from_vec(vec![
            b'r', b'a', b'w', 0xff, b's', b'e', b'c', b'r', b'e', b't',
        ]));
        let durable = durable_config_with_postgres_url("postgresql://localhost/stratum");

        let err = durable
            .postgres_config_with_env_password()
            .expect_err("non-Unicode PGPASSWORD should fail closed");
        let message = err.to_string();

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(message.contains("postgres secret resolution failed"));
        assert!(!message.contains("raw"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn startup_migration_report_errors_do_not_echo_db_sourced_fields() {
        use crate::backend::postgres_migrations::{
            PostgresMigrationReport, PostgresMigrationStatus,
        };

        let cases = vec![
            (
                DurableMigrationMode::Apply,
                PostgresMigrationStatus::Pending {
                    version: 11,
                    name: "raw-pending-secret",
                },
            ),
            (
                DurableMigrationMode::Status,
                PostgresMigrationStatus::Dirty {
                    version: 12,
                    name: "raw-dirty-secret".to_string(),
                    state: "raw-state-secret".to_string(),
                },
            ),
            (
                DurableMigrationMode::Status,
                PostgresMigrationStatus::ChecksumMismatch {
                    version: 13,
                    name: "raw-checksum-secret".to_string(),
                },
            ),
            (
                DurableMigrationMode::Status,
                PostgresMigrationStatus::UnknownApplied {
                    version: 14,
                    name: "raw-unknown-secret".to_string(),
                },
            ),
        ];

        for (mode, status) in cases {
            let report = PostgresMigrationReport {
                statuses: vec![status],
            };

            let err = validate_startup_migration_report(&report, mode)
                .expect_err("invalid migration status should fail");
            let message = err.to_string();

            assert!(!message.contains("raw-"));
        }
    }
}
