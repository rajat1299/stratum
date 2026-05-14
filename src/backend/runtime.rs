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

use crate::backend::RepoId;
use crate::error::VfsError;
use crate::idempotency::IdempotencyRetentionPolicy;

#[cfg(feature = "postgres")]
use tokio_postgres::config::{Host, SslMode};

pub const BACKEND_ENV: &str = "STRATUM_BACKEND";
pub const CORE_RUNTIME_ENV: &str = "STRATUM_CORE_RUNTIME";
pub const POSTGRES_URL_ENV: &str = "STRATUM_POSTGRES_URL";
pub const POSTGRES_SCHEMA_ENV: &str = "STRATUM_POSTGRES_SCHEMA";
pub const DURABLE_MIGRATION_MODE_ENV: &str = "STRATUM_DURABLE_MIGRATION_MODE";
pub const R2_BUCKET_ENV: &str = "STRATUM_R2_BUCKET";
pub const R2_ENDPOINT_ENV: &str = "STRATUM_R2_ENDPOINT";
pub const R2_ACCESS_KEY_ID_ENV: &str = "STRATUM_R2_ACCESS_KEY_ID";
pub const R2_SECRET_ACCESS_KEY_ENV: &str = "STRATUM_R2_SECRET_ACCESS_KEY";
pub const R2_REGION_ENV: &str = "STRATUM_R2_REGION";
pub const R2_PREFIX_ENV: &str = "STRATUM_R2_PREFIX";
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
pub const DURABLE_AUTH_SESSION_READINESS_MISSING: &str =
    "durable auth/session routing readiness is missing";
const IDEMPOTENCY_RETENTION_MAX_SECONDS: u64 = 10 * 365 * 24 * 60 * 60;
const IDEMPOTENCY_QUOTA_MAX_RECORDS: usize = 10_000_000;

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
}

impl DurableMigrationMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "status" => Ok(Self::Status),
            "apply" => Ok(Self::Apply),
            _ => Err(VfsError::InvalidArgs {
                message: format!(
                    "invalid {DURABLE_MIGRATION_MODE_ENV}; expected `status` or `apply`"
                ),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Apply => "apply",
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
                durable_core_runtime,
                durable: Some(DurableBackendRuntimeConfig::from_lookup(lookup)?),
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
                durable_core_runtime: None,
                durable: None,
            }),
            BackendRuntimeMode::Durable => Ok(Self {
                mode,
                core_runtime_mode,
                guarded_durable_commit_route,
                durable_core_runtime: None,
                durable: Some(DurableBackendRuntimeConfig::from_lookup(lookup)?),
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
            .field("durable_core_runtime", &self.durable_core_runtime)
            .field("durable", &self.durable)
            .finish()
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

fn invalid_positive_integer_env(name: &'static str, max: impl fmt::Display) -> VfsError {
    VfsError::InvalidArgs {
        message: format!("invalid {name}; expected a positive integer no greater than {max}"),
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
    migration_mode: DurableMigrationMode,
    object_store: DurableObjectStoreRuntimeConfig,
}

impl DurableBackendRuntimeConfig {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
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
        let endpoint = endpoint.expect("missing durable env should return earlier");
        if endpoint_has_sensitive_parts(&endpoint) {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "{R2_ENDPOINT_ENV} must not include userinfo or secret-bearing query parameters"
                ),
            });
        }

        let region =
            optional_value(&mut lookup, R2_REGION_ENV).unwrap_or_else(|| "auto".to_string());
        let prefix =
            optional_value(&mut lookup, R2_PREFIX_ENV).unwrap_or_else(|| "stratum".to_string());
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
            migration_mode,
            object_store: DurableObjectStoreRuntimeConfig {
                bucket: bucket.expect("missing durable env should return earlier"),
                endpoint,
                access_key_id_configured: access_key_id.is_some(),
                secret_access_key_configured: secret_access_key.is_some(),
                region,
                prefix,
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

    pub fn migration_mode(&self) -> DurableMigrationMode {
        self.migration_mode
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_config_with_env_password(
        &self,
    ) -> Result<tokio_postgres::Config, VfsError> {
        let mut config =
            self.postgres_url
                .parse::<tokio_postgres::Config>()
                .map_err(|_| VfsError::InvalidArgs {
                    message: format!(
                        "invalid {POSTGRES_URL_ENV}; expected a Postgres connection string without an embedded password"
                    ),
                })?;

        if config.get_password().is_some() {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "{POSTGRES_URL_ENV} must not include a password; use PGPASSWORD or the deployment secret manager"
                ),
            });
        }
        validate_no_tls_postgres_runtime_target(&config)?;

        if let Ok(password) = std::env::var("PGPASSWORD")
            && !password.is_empty()
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
        use crate::backend::postgres_migrations::PostgresMigrationRunner;

        let config = self.postgres_config_with_env_password()?;
        let runner = PostgresMigrationRunner::with_schema(config, self.postgres_schema.clone())?;
        let report = match self.migration_mode {
            DurableMigrationMode::Status => runner.status().await?,
            DurableMigrationMode::Apply => runner.apply_pending().await?,
        };

        let migration_status = validate_startup_migration_report(&report, self.migration_mode)?;
        Ok(DurableStartupPreflight::checked(migration_status))
    }
}

#[cfg(feature = "postgres")]
fn validate_no_tls_postgres_runtime_target(
    config: &tokio_postgres::Config,
) -> Result<(), VfsError> {
    if config.get_ssl_mode() == SslMode::Require {
        return Err(VfsError::NotSupported {
            message: format!(
                "{POSTGRES_URL_ENV} requires TLS, but durable Postgres runtime TLS is not wired yet"
            ),
        });
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
                "{POSTGRES_URL_ENV} must use localhost, 127.0.0.1, ::1, or a Unix socket path until durable Postgres TLS support is wired"
            ),
        });
    }

    Ok(())
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
                        "Postgres migrations are pending; set {DURABLE_MIGRATION_MODE_ENV}=apply to apply them before durable startup"
                    ),
                });
            }
            PostgresMigrationStatus::Pending { version, .. } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} is still pending after apply; refusing durable startup"
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
        DurableMigrationMode::Apply => DurableMigrationPreflightStatus::Applied,
    })
}

impl fmt::Debug for DurableBackendRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableBackendRuntimeConfig")
            .field("postgres_url_configured", &self.postgres_url_configured)
            .field("postgres_schema", &self.postgres_schema)
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
}

impl fmt::Debug for DurableObjectStoreRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableObjectStoreRuntimeConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &sanitize_endpoint_for_debug(&self.endpoint))
            .field("access_key_id_configured", &self.access_key_id_configured)
            .field(
                "secret_access_key_configured",
                &self.secret_access_key_configured,
            )
            .field("region", &self.region)
            .field("prefix", &self.prefix)
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

fn endpoint_has_sensitive_parts(value: &str) -> bool {
    uri_contains_userinfo(value)
        || query_contains_sensitive_key(
            value,
            &[
                "access_key",
                "access_key_id",
                "accesskey",
                "authorization",
                "awsaccesskeyid",
                "password",
                "secret",
                "secret_access_key",
                "signature",
                "token",
                "x-amz-credential",
                "x-amz-security-token",
                "x-amz-signature",
            ],
        )
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

fn sanitize_endpoint_for_debug(endpoint: &str) -> String {
    let without_query = endpoint.split(['?', '#']).next().unwrap_or(endpoint);
    let Some(scheme_index) = without_query.find("://") else {
        return without_query.to_string();
    };
    let scheme_end = scheme_index + 3;
    let after_scheme = &without_query[scheme_end..];
    let slash_index = after_scheme.find('/');
    let (authority, path) = match slash_index {
        Some(index) => (&after_scheme[..index], &after_scheme[index..]),
        None => (after_scheme, ""),
    };
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{}{}{}", &without_query[..scheme_end], authority, path)
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
    use std::collections::BTreeMap;

    fn lookup(entries: &[(&str, &str)]) -> impl FnMut(&str) -> Option<String> {
        let values: BTreeMap<String, String> = entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |name| values.get(name).cloned()
    }

    fn durable_entries() -> Vec<(&'static str, &'static str)> {
        vec![
            (BACKEND_ENV, "durable"),
            (POSTGRES_URL_ENV, "postgresql://stratum-db.internal/stratum"),
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

    fn core_runtime_config(value: &str) -> BackendRuntimeConfig {
        BackendRuntimeConfig::from_lookup(lookup(&[(CORE_RUNTIME_ENV, value)])).unwrap()
    }

    #[cfg(feature = "postgres")]
    fn durable_config_with_postgres_url(postgres_url: &str) -> DurableBackendRuntimeConfig {
        DurableBackendRuntimeConfig {
            postgres_url_configured: true,
            postgres_url: postgres_url.to_string(),
            postgres_schema: "public".to_string(),
            migration_mode: DurableMigrationMode::Status,
            object_store: DurableObjectStoreRuntimeConfig {
                bucket: "stratum-prod".to_string(),
                endpoint: "https://account.r2.cloudflarestorage.com".to_string(),
                access_key_id_configured: true,
                secret_access_key_configured: true,
                region: "auto".to_string(),
                prefix: "stratum".to_string(),
            },
        }
    }

    #[cfg(feature = "postgres")]
    static PGPASSWORD_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(feature = "postgres")]
    struct PgPasswordEnvGuard {
        original: Option<String>,
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    #[cfg(feature = "postgres")]
    impl PgPasswordEnvGuard {
        fn set(value: Option<&str>) -> Self {
            let guard = PGPASSWORD_TEST_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original = std::env::var("PGPASSWORD").ok();
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
    }

    #[cfg(feature = "postgres")]
    impl Drop for PgPasswordEnvGuard {
        fn drop(&mut self) {
            match self.original.as_deref() {
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
            let mut entries = durable_core_entries();
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
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_core_entries())).unwrap();

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
    fn durable_core_runtime_does_not_accept_guarded_durable_commit_route() {
        let mut entries = durable_core_entries();
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
    fn durable_backend_rejects_secret_bearing_r2_endpoint() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((
            R2_ENDPOINT_ENV,
            "https://example.invalid?tok%65n=raw-endpoint-token",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("secret-bearing endpoint should fail");

        assert!(
            err.to_string()
                .contains("must not include userinfo or secret-bearing query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-token"));
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
                .contains("must not include userinfo or secret-bearing query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-password"));
    }

    #[test]
    fn debug_output_redacts_durable_secret_values() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("access_key_id_configured: true"));
        assert!(debug.contains("secret_access_key_configured: true"));
        assert!(!debug.contains("test-access-key-id"));
        assert!(!debug.contains("test-secret-access-key"));
        assert!(!debug.contains("postgresql://stratum-db.internal/stratum"));
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
    fn postgres_config_with_env_password_rejects_tls_required_without_tls_support() {
        let durable =
            durable_config_with_postgres_url("postgresql://localhost/stratum?sslmode=require");

        let err = durable
            .postgres_config_with_env_password()
            .expect_err("TLS-required runtime URL should fail closed");

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(err.to_string().contains("TLS"));
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
