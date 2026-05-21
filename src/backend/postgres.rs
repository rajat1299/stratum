//! Postgres-backed metadata adapters for durable backend contracts.
//!
//! This module is gated behind the `postgres` feature and is not wired into
//! the server runtime. It proves the current Postgres schema can satisfy the
//! object metadata, cleanup claim, commit metadata, ref compare-and-swap,
//! idempotency, audit, workspace metadata, and review-store contracts.

use async_trait::async_trait;
use deadpool_postgres::{GenericClient, Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, Config, IsolationLevel, NoTls, Row};
use uuid::Uuid;

use crate::audit::{
    AuditAction, AuditActor, AuditEvent, AuditOutcome, AuditResource, AuditResourceKind,
    AuditStore, AuditWorkspaceContext, NewAuditEvent,
};
use crate::auth::Uid;
use crate::backend::blob_object::{
    FinalObjectMetadataFenceRequest, ObjectMetadataRecord, ObjectMetadataStore,
};
use crate::backend::core_transaction::{
    DurableCorePostCasRecoveryClaim, DurableCorePostCasRecoveryClaimRequest,
    DurableCorePostCasRecoveryClaimStore, DurableCorePostCasRecoveryContext,
    DurableCorePostCasRecoveryCounts, DurableCorePostCasRecoveryState,
    DurableCorePostCasRecoveryStatus, DurableCorePostCasRecoveryStatusInput,
    DurableCorePostCasRecoveryTarget, DurableCorePostCasStep,
    DurableCorePreVisibilityRecoveryClaim, DurableCorePreVisibilityRecoveryClaimRequest,
    DurableCorePreVisibilityRecoveryCounts, DurableCorePreVisibilityRecoveryRecord,
    DurableCorePreVisibilityRecoveryStage, DurableCorePreVisibilityRecoveryState,
    DurableCorePreVisibilityRecoveryStatus, DurableCorePreVisibilityRecoveryStatusInput,
    DurableCorePreVisibilityRecoveryStore, DurableCorePreVisibilityRecoveryTarget,
    DurableFsMutationRecoveryClaim, DurableFsMutationRecoveryClaimRequest,
    DurableFsMutationRecoveryCounts, DurableFsMutationRecoveryEnvelope,
    DurableFsMutationRecoveryState, DurableFsMutationRecoveryStatus,
    DurableFsMutationRecoveryStatusInput, DurableFsMutationRecoveryStep,
    DurableFsMutationRecoveryStore, DurableFsMutationRecoveryTarget, FinalObjectMetadataFence,
    FinalObjectMetadataIdentity, contextual_post_cas_recovery_enqueue_conflict,
    validate_durable_fs_mutation_recovery_backoff, validate_post_cas_recovery_backoff,
    validate_pre_visibility_recovery_backoff,
};
use crate::backend::object_cleanup::{
    FinalObjectDeletionReadiness, FinalObjectDeletionSnapshot, ObjectCleanupClaim,
    ObjectCleanupClaimCounts, ObjectCleanupClaimKind, ObjectCleanupClaimRequest,
    ObjectCleanupClaimState, ObjectCleanupClaimStatus, ObjectCleanupClaimStatusInput,
    ObjectCleanupClaimStore, ObjectCleanupWorker, classify_cleanup_claim, cleanup_claim_is_stale,
    stale_cleanup_claim, validate_lease_owner, validate_object_key,
};
use crate::backend::runtime::{DurablePostgresRuntimePosture, PostgresTlsRuntimeMode};
use crate::backend::{
    CommitRecord, CommitStore, RefExpectation, RefRecord, RefStore, RefUpdate, RefVersion, RepoId,
    SourceCheckedRefUpdate,
};
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyBegin, IdempotencyKey, IdempotencyQuotaIdentity, IdempotencyRecord,
    IdempotencyReplayClassification, IdempotencyReservation, IdempotencyRetentionPolicy,
    IdempotencyStore, IdempotencySweepRequest, IdempotencySweepSummary, RetainedIdempotencyRecord,
    SecretReplayMetadata,
};
use crate::review::{
    ApprovalDismissalMutation, ApprovalPolicyDecision, ApprovalRecord, ApprovalRecordMutation,
    ChangeRequest, ChangeRequestStatus, DismissApprovalInput, NewApprovalRecord, NewChangeRequest,
    NewReviewAssignment, NewReviewComment, ProtectedPathRule, ProtectedRefRule, ReviewAssignment,
    ReviewAssignmentMutation, ReviewComment, ReviewCommentKind, ReviewCommentMutation, ReviewStore,
    normalize_dismissal_reason, validate_change_request_open,
};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{ChangedPath, CommitId, MAIN_REF, RefName};
use crate::workspace::{
    IssuedWorkspaceToken, ValidWorkspaceToken, WorkspaceMetadataStore, WorkspacePrincipalKind,
    WorkspacePrincipalRecord, WorkspaceRecord, WorkspaceTokenRecord,
    generate_workspace_token_secret, hash_workspace_token_secret,
    normalize_optional_workspace_session_ref, normalize_workspace_ref,
    normalize_workspace_token_prefixes, token_is_valid_at, workspace_record,
    workspace_record_for_repo, workspace_token_hash_eq,
};

#[derive(Clone)]
pub struct PostgresMetadataStore {
    connector: PostgresConnector,
    schema: String,
}

impl fmt::Debug for PostgresMetadataStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresMetadataStore")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl PostgresMetadataStore {
    pub fn new(config: Config) -> Self {
        Self {
            connector: PostgresConnector::local(config),
            schema: "public".to_string(),
        }
    }

    pub fn with_schema(config: Config, schema: impl Into<String>) -> Result<Self, VfsError> {
        let posture =
            DurablePostgresRuntimePosture::local_defaults().with_tls_mode(infer_tls_mode(&config));
        Self::with_schema_and_posture(config, schema, posture)
    }

    pub fn with_schema_and_posture(
        config: Config,
        schema: impl Into<String>,
        posture: DurablePostgresRuntimePosture,
    ) -> Result<Self, VfsError> {
        Ok(Self {
            connector: PostgresConnector::new(config, posture)?,
            schema: validate_schema_name(schema.into())?,
        })
    }

    async fn connect_client(&self) -> Result<deadpool_postgres::Client, VfsError> {
        self.connector.connect_with_schema(Some(&self.schema)).await
    }

    pub(crate) async fn ensure_control_plane_ready(&self) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        self.connector
            .batch_execute_operation(
                &client,
                "SELECT id, name, created_at
                 FROM repos
                 LIMIT 0;
                 SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref, created_at
                 FROM workspaces
                 LIMIT 0;
                 SELECT id, workspace_id, repo_id, name, agent_uid, secret_hash,
                        read_prefixes_json, write_prefixes_json, principal_uid,
                        token_version, issued_at, updated_at, expires_at,
                        revoked_at, created_at
                 FROM workspace_tokens
                 LIMIT 0;
                 SELECT scope, key_hash, request_fingerprint, state, status_code, response_body_json, reserved_at, created_at, completed_at,
                        replay_classification, quota_repo_id, quota_workspace_id, quota_principal_uid, retention_deferred_at,
                        secret_replay_envelope_version, secret_replay_key_id, secret_replay_aad_hash, secret_replay_encrypted_at
                 FROM idempotency_records
                 LIMIT 0;
                 SELECT id, repo_id, sequence, created_at, actor_json, workspace_json, action, resource_json, outcome, details_json
                 FROM audit_events
                 LIMIT 0;
                 SELECT repo_id, object_kind, object_id, canonical_final_key, lease_owner,
                        fence_token, fence_expires_at, metadata_object_key, metadata_size_bytes,
                        metadata_sha256, created_at, updated_at
                 FROM object_deletion_fences
                 LIMIT 0;
                 SELECT repo_id, ref_name, commit_id, step, state, lease_owner, lease_token, lease_expires_at, attempts, retry_after, last_error, completed_at, poisoned_at, context_json, created_at, updated_at
                 FROM durable_post_cas_recovery_claims
                 LIMIT 0;
                 SELECT repo_id, ref_name, commit_id, stage, state, root_tree_id, parent_commit_id, expected_ref_version, object_count, changed_path_count, has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count, lease_owner, lease_token, lease_expires_at, attempts, retry_after, last_error, resolved_at, poisoned_at, context_json, updated_at
                 FROM durable_pre_visibility_recovery_ledger
                 LIMIT 0;
                 SELECT id, repo_id, ref_name, required_approvals, require_all_files_viewed, created_by, active, created_at
                 FROM protected_ref_rules
                 LIMIT 0;
                 SELECT id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by, active, created_at
                 FROM protected_path_rules
                 LIMIT 0;
                 SELECT id, repo_id, title, description, source_ref, target_ref, base_commit, head_commit, status, created_by, version, created_at, updated_at
                 FROM change_requests
                 LIMIT 0;
                 SELECT id, change_request_id, head_commit, approved_by, comment, active, dismissed_by, dismissal_reason, version, created_at, updated_at
                 FROM approvals
                 LIMIT 0;
                 SELECT id, change_request_id, reviewer, assigned_by, required, active, version, created_at, updated_at
                 FROM reviewer_assignments
                 LIMIT 0;
                 SELECT id, change_request_id, author, body, path, kind, active, version, created_at
                 FROM review_comments
                 LIMIT 0;",
                "durable control-plane readiness",
            )
            .await?;
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn connect_with_schema(
    config: &Config,
    schema: Option<&str>,
) -> Result<deadpool_postgres::Client, VfsError> {
    PostgresConnector::local(config.clone())
        .connect_with_schema(schema)
        .await
}

#[derive(Clone)]
pub(crate) struct PostgresConnector {
    config: Config,
    posture: DurablePostgresRuntimePosture,
    pool: Pool,
}

impl fmt::Debug for PostgresConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnector")
            .field("tls_mode", &self.posture.tls_mode())
            .field("pool_max_size", &self.posture.pool_max_size())
            .field("connect_timeout", &self.posture.connect_timeout())
            .field("operation_timeout", &self.posture.operation_timeout())
            .field("pool_acquire_timeout", &self.posture.pool_acquire_timeout())
            .finish_non_exhaustive()
    }
}

impl PostgresConnector {
    pub(crate) fn local(config: Config) -> Self {
        let posture =
            DurablePostgresRuntimePosture::local_defaults().with_tls_mode(infer_tls_mode(&config));
        let config = config_with_connect_timeout(config, posture.connect_timeout());
        let pool = build_pool(config.clone(), posture.clone())
            .expect("local Postgres pool configuration should build");
        Self {
            config,
            posture,
            pool,
        }
    }

    pub(crate) fn new(
        config: Config,
        posture: DurablePostgresRuntimePosture,
    ) -> Result<Self, VfsError> {
        validate_connector_target(&config, posture.tls_mode())?;
        if posture.tls_mode() == PostgresTlsRuntimeMode::HostedTlsRequired {
            let _ = make_tls_connector()?;
        }

        let config = config_with_connect_timeout(config, posture.connect_timeout());
        let pool = build_pool(config.clone(), posture.clone())?;
        Ok(Self {
            config,
            posture,
            pool,
        })
    }

    #[cfg(test)]
    fn tls_mode(&self) -> PostgresTlsRuntimeMode {
        self.posture.tls_mode()
    }

    #[cfg(test)]
    fn connect_timeout(&self) -> Duration {
        self.config
            .get_connect_timeout()
            .copied()
            .unwrap_or_else(|| self.posture.connect_timeout())
    }

    #[cfg(test)]
    fn uses_tls(&self) -> bool {
        self.posture.tls_mode() == PostgresTlsRuntimeMode::HostedTlsRequired
    }

    pub(crate) async fn connect_with_schema(
        &self,
        schema: Option<&str>,
    ) -> Result<deadpool_postgres::Client, VfsError> {
        validate_connector_target(&self.config, self.posture.tls_mode())?;
        let mut timeouts = deadpool_postgres::Timeouts::new();
        timeouts.wait = Some(self.posture.pool_acquire_timeout());
        timeouts.create = Some(self.posture.connect_timeout());
        timeouts.recycle = Some(self.posture.operation_timeout());
        let client = self.pool.timeout_get(&timeouts).await.map_err(pool_error)?;

        let search_path = if let Some(schema) = schema {
            let schema = validate_schema_name(schema.to_string())?;
            format!("SET search_path TO {}", quote_identifier(&schema))
        } else {
            "RESET search_path".to_string()
        };
        self.batch_execute_with_timeout(&client, &search_path, "startup query")
            .await?;
        self.configure_statement_timeout(&client).await?;

        Ok(client)
    }

    async fn batch_execute_with_timeout(
        &self,
        client: &Client,
        sql: &str,
        context: &str,
    ) -> Result<(), VfsError> {
        tokio::time::timeout(self.posture.operation_timeout(), client.batch_execute(sql))
            .await
            .map_err(|_| postgres_timeout_error())?
            .map_err(|error| postgres_error(context, error))
    }

    pub(crate) async fn batch_execute_operation(
        &self,
        client: &Client,
        sql: &str,
        context: &str,
    ) -> Result<(), VfsError> {
        self.batch_execute_with_timeout(client, sql, context).await
    }

    async fn configure_statement_timeout(&self, client: &Client) -> Result<(), VfsError> {
        // Central operation bound for regular store queries issued through
        // pooled clients. PostgreSQL reports this as QUERY_CANCELED, which
        // postgres_error maps to the fixed timeout error.
        let millis = self
            .posture
            .operation_timeout()
            .as_millis()
            .min(i32::MAX as u128);
        let sql = format!("SET statement_timeout = {millis}");
        self.batch_execute_with_timeout(client, &sql, "startup query")
            .await
    }
}

pub(crate) fn infer_tls_mode(config: &Config) -> PostgresTlsRuntimeMode {
    if config.get_ssl_mode() == SslMode::Require {
        PostgresTlsRuntimeMode::HostedTlsRequired
    } else {
        PostgresTlsRuntimeMode::LocalNoTls
    }
}

fn config_with_connect_timeout(mut config: Config, connect_timeout: Duration) -> Config {
    config.connect_timeout(connect_timeout);
    config
}

fn build_pool(config: Config, posture: DurablePostgresRuntimePosture) -> Result<Pool, VfsError> {
    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let builder = match posture.tls_mode() {
        PostgresTlsRuntimeMode::LocalNoTls => {
            Pool::builder(Manager::from_config(config, NoTls, manager_config))
        }
        PostgresTlsRuntimeMode::HostedTlsRequired => Pool::builder(Manager::from_config(
            config,
            make_tls_connector()?,
            manager_config,
        )),
    };

    builder
        .runtime(Runtime::Tokio1)
        .max_size(posture.pool_max_size())
        .wait_timeout(Some(posture.pool_acquire_timeout()))
        .create_timeout(Some(posture.connect_timeout()))
        .recycle_timeout(Some(posture.operation_timeout()))
        .build()
        .map_err(|_| postgres_connect_failed())
}

fn pool_error(error: deadpool_postgres::PoolError) -> VfsError {
    match error {
        deadpool_postgres::PoolError::Timeout(_) => postgres_timeout_error(),
        deadpool_postgres::PoolError::Backend(_)
        | deadpool_postgres::PoolError::Closed
        | deadpool_postgres::PoolError::NoRuntimeSpecified
        | deadpool_postgres::PoolError::PostCreateHook(_) => postgres_connect_failed(),
    }
}

fn make_tls_connector() -> Result<MakeTlsConnector, VfsError> {
    TlsConnector::builder()
        .build()
        .map(MakeTlsConnector::new)
        .map_err(|_| postgres_connect_failed())
}

fn validate_connector_target(
    config: &Config,
    tls_mode: PostgresTlsRuntimeMode,
) -> Result<(), VfsError> {
    match tls_mode {
        PostgresTlsRuntimeMode::HostedTlsRequired if config.get_ssl_mode() == SslMode::Require => {
            Ok(())
        }
        PostgresTlsRuntimeMode::HostedTlsRequired => Err(remote_tls_required_error()),
        PostgresTlsRuntimeMode::LocalNoTls if local_no_tls_target_allowed(config) => Ok(()),
        PostgresTlsRuntimeMode::LocalNoTls => Err(remote_tls_required_error()),
    }
}

fn local_no_tls_target_allowed(config: &Config) -> bool {
    if config.get_ssl_mode() == SslMode::Require {
        return false;
    }

    let hosts = config.get_hosts();
    let hostaddrs = config.get_hostaddrs();
    let has_runtime_target = !hosts.is_empty() || !hostaddrs.is_empty();
    has_runtime_target
        && hosts.iter().all(is_no_tls_runtime_host_allowed)
        && hostaddrs.iter().all(is_no_tls_runtime_hostaddr_allowed)
}

fn is_no_tls_runtime_hostaddr_allowed(hostaddr: &IpAddr) -> bool {
    hostaddr.is_loopback()
}

fn is_no_tls_runtime_host_allowed(host: &Host) -> bool {
    match host {
        Host::Tcp(host) => matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1"),
        #[cfg(unix)]
        Host::Unix(_) => true,
    }
}

fn remote_tls_required_error() -> VfsError {
    VfsError::NotSupported {
        message: "STRATUM_POSTGRES_URL remote Postgres targets must set sslmode=require; local targets may use localhost, 127.0.0.1, ::1, loopback hostaddr, or a Unix socket path without TLS".to_string(),
    }
}

fn postgres_connect_failed() -> VfsError {
    VfsError::IoError(std::io::Error::other("postgres connect failed"))
}

fn postgres_timeout_error() -> VfsError {
    VfsError::IoError(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "postgres operation timed out",
    ))
}

pub(crate) fn validate_schema_name(schema: String) -> Result<String, VfsError> {
    let valid = !schema.is_empty()
        && schema.len() <= 63
        && schema.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'_' => true,
            b'0'..=b'9' => index > 0,
            _ => false,
        });
    if !valid {
        return Err(VfsError::InvalidArgs {
            message: format!("invalid Postgres schema name: {schema}"),
        });
    }
    Ok(schema)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

async fn ensure_repo<C>(client: &C, repo_id: &RepoId) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    client
        .execute(
            "INSERT INTO repos (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING",
            &[&repo_id.as_str()],
        )
        .await
        .map_err(|error| postgres_error("ensure repo", error))?;
    Ok(())
}

#[async_trait]
impl ObjectMetadataStore for PostgresMetadataStore {
    async fn put(&self, record: ObjectMetadataRecord) -> Result<ObjectMetadataRecord, VfsError> {
        let mut client = self.connect_client().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin object metadata put transaction", error))?;
        ensure_repo(&transaction, &record.repo_id).await?;
        lock_object_deletion_fence_key(&transaction, &record.repo_id, record.kind, record.id)
            .await?;
        reject_active_object_deletion_fence(&transaction, &record.repo_id, record.kind, record.id)
            .await?;

        let size = u64_to_i64(record.size, "object size")?;
        let inserted = transaction
            .query_opt(
                "INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (repo_id, object_id) DO NOTHING
                 RETURNING repo_id, kind, object_id, object_key, size_bytes, sha256",
                &[
                    &record.repo_id.as_str(),
                    &object_kind_to_db(record.kind),
                    &record.id.to_hex(),
                    &record.object_key,
                    &size,
                    &record.sha256,
                ],
            )
            .await
            .map_err(|error| postgres_error("insert object metadata", error))?;
        let result = match inserted.map(row_to_object_metadata).transpose()? {
            Some(inserted) => Ok(inserted),
            None => match load_object_metadata(&transaction, &record.repo_id, record.id).await? {
                Some(existing) if existing == record => Ok(existing),
                Some(_) => Err(VfsError::CorruptStore {
                    message: format!(
                        "object metadata for {} already exists with different attributes",
                        record.id.short_hex()
                    ),
                }),
                None => Err(VfsError::CorruptStore {
                    message: format!(
                        "object metadata insert for {} conflicted without a visible row",
                        record.id.short_hex()
                    ),
                }),
            },
        }?;
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit object metadata put transaction", error))?;
        Ok(result)
    }

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
    ) -> Result<Option<ObjectMetadataRecord>, VfsError> {
        let client = self.connect_client().await?;
        load_object_metadata(&client, repo_id, id).await
    }

    async fn acquire_final_object_metadata_fence(
        &self,
        request: FinalObjectMetadataFenceRequest,
    ) -> Result<Option<FinalObjectMetadataFence>, VfsError> {
        request.validate()?;
        let mut client = self.connect_client().await?;
        let transaction = client.transaction().await.map_err(|error| {
            postgres_error("begin final object metadata fence transaction", error)
        })?;
        ensure_repo(&transaction, &request.repo_id).await?;
        lock_object_deletion_fence_key(
            &transaction,
            &request.repo_id,
            request.object_kind,
            request.object_id,
        )
        .await?;
        let lease_duration_millis = duration_to_i64_millis(
            request.lease_duration,
            "final object metadata fence lease duration",
        )?;
        let token = Uuid::new_v4();
        let row = transaction
            .query_opt(
                "WITH fence_clock AS (
                    SELECT clock_timestamp() AS now
                 ),
                 current_metadata AS (
                    SELECT object_key, size_bytes, sha256
                    FROM objects
                    WHERE repo_id = $1 AND kind = $2 AND object_id = $3
                 )
                 INSERT INTO object_deletion_fences (
                    repo_id,
                    object_kind,
                    object_id,
                    canonical_final_key,
                    lease_owner,
                    fence_token,
                    fence_expires_at,
                    metadata_object_key,
                    metadata_size_bytes,
                    metadata_sha256,
                    updated_at
                 )
                 SELECT
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    fence_clock.now + ($7::bigint * interval '1 millisecond'),
                    current_metadata.object_key,
                    current_metadata.size_bytes,
                    current_metadata.sha256,
                    fence_clock.now
                 FROM fence_clock
                 LEFT JOIN current_metadata ON true
                 ON CONFLICT (repo_id, object_kind, object_id) DO UPDATE
                 SET canonical_final_key = EXCLUDED.canonical_final_key,
                     lease_owner = EXCLUDED.lease_owner,
                     fence_token = EXCLUDED.fence_token,
                     fence_expires_at = EXCLUDED.fence_expires_at,
                     metadata_object_key = EXCLUDED.metadata_object_key,
                     metadata_size_bytes = EXCLUDED.metadata_size_bytes,
                     metadata_sha256 = EXCLUDED.metadata_sha256,
                     updated_at = EXCLUDED.updated_at
                 WHERE object_deletion_fences.fence_expires_at <= EXCLUDED.updated_at
                 RETURNING repo_id, object_kind, object_id, canonical_final_key, lease_owner,
                     fence_token, fence_expires_at, created_at, updated_at,
                     metadata_object_key, metadata_size_bytes, metadata_sha256",
                &[
                    &request.repo_id.as_str(),
                    &object_kind_to_db(request.object_kind),
                    &request.object_id.to_hex(),
                    &request.canonical_final_key,
                    &request.lease_owner,
                    &token.to_string(),
                    &lease_duration_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("acquire final object metadata fence", error))?;
        let fence = row.map(row_to_final_object_metadata_fence).transpose()?;
        transaction.commit().await.map_err(|error| {
            postgres_error("commit final object metadata fence transaction", error)
        })?;
        Ok(fence)
    }

    async fn delete_with_final_object_metadata_fence(
        &self,
        fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        let mut client = self.connect_client().await?;
        let transaction = client.transaction().await.map_err(|error| {
            postgres_error("begin fenced object metadata delete transaction", error)
        })?;
        lock_object_deletion_fence_key(
            &transaction,
            fence.repo_id(),
            fence.object_kind(),
            fence.object_id(),
        )
        .await?;
        require_active_object_deletion_fence(&transaction, fence).await?;
        if let Some(record) =
            load_object_metadata(&transaction, fence.repo_id(), fence.object_id()).await?
        {
            validate_metadata_snapshot_for_fence(&record, fence)?;
            transaction
                .execute(
                    "DELETE FROM objects
                     WHERE repo_id = $1 AND kind = $2 AND object_id = $3",
                    &[
                        &fence.repo_id().as_str(),
                        &object_kind_to_db(fence.object_kind()),
                        &fence.object_id().to_hex(),
                    ],
                )
                .await
                .map_err(|error| postgres_error("delete object metadata with fence", error))?;
        } else if fence.metadata_identity().is_some() {
            return Err(VfsError::ObjectWriteConflict {
                message: format!(
                    "object {} metadata disappeared while final object deletion was fenced; retry",
                    fence.object_id().short_hex()
                ),
            });
        }
        transaction.commit().await.map_err(|error| {
            postgres_error("commit fenced object metadata delete transaction", error)
        })?;
        Ok(())
    }

    async fn validate_final_object_metadata_fence(
        &self,
        fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        let mut client = self.connect_client().await?;
        let transaction = client.transaction().await.map_err(|error| {
            postgres_error(
                "begin final object metadata fence validation transaction",
                error,
            )
        })?;
        lock_object_deletion_fence_key(
            &transaction,
            fence.repo_id(),
            fence.object_kind(),
            fence.object_id(),
        )
        .await?;
        require_active_object_deletion_fence(&transaction, fence).await?;
        if let Some(record) =
            load_object_metadata(&transaction, fence.repo_id(), fence.object_id()).await?
        {
            validate_metadata_snapshot_for_fence(&record, fence)?;
        } else if fence.metadata_identity().is_some() {
            return Err(VfsError::ObjectWriteConflict {
                message: format!(
                    "object {} metadata disappeared while final object deletion was fenced; retry",
                    fence.object_id().short_hex()
                ),
            });
        }
        transaction.commit().await.map_err(|error| {
            postgres_error(
                "commit final object metadata fence validation transaction",
                error,
            )
        })?;
        Ok(())
    }

    async fn release_final_object_metadata_fence(
        &self,
        fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        client
            .execute(
                "DELETE FROM object_deletion_fences
                 WHERE repo_id = $1 AND object_kind = $2 AND object_id = $3 AND fence_token = $4",
                &[
                    &fence.repo_id().as_str(),
                    &object_kind_to_db(fence.object_kind()),
                    &fence.object_id().to_hex(),
                    &fence.token().to_string(),
                ],
            )
            .await
            .map_err(|error| postgres_error("release final object metadata fence", error))?;
        Ok(())
    }
}

#[async_trait]
impl ObjectCleanupClaimStore for PostgresMetadataStore {
    async fn claim(
        &self,
        request: ObjectCleanupClaimRequest,
    ) -> Result<Option<ObjectCleanupClaim>, VfsError> {
        request.validate()?;

        let client = self.connect_client().await?;
        ensure_repo(&client, &request.repo_id).await?;
        let lease_token = Uuid::new_v4().to_string();
        let lease_duration_millis =
            duration_to_i64_millis(request.lease_duration, "cleanup claim lease duration")?;
        let row = client
            .query_opt(
                "WITH claim_clock AS (
                    SELECT clock_timestamp() AS now
                 )
                 INSERT INTO object_cleanup_claims (
                    repo_id,
                    claim_kind,
                    object_kind,
                    object_id,
                    object_key,
                    lease_owner,
                    lease_token,
                    lease_expires_at,
                    attempts,
                    last_error,
                    updated_at
                 )
                 SELECT
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    claim_clock.now + ($8::bigint * interval '1 millisecond'),
                    1,
                    NULL,
                    claim_clock.now
                 FROM claim_clock
                 ON CONFLICT (repo_id, claim_kind, object_key) DO UPDATE
                 SET lease_owner = EXCLUDED.lease_owner,
                     lease_token = EXCLUDED.lease_token,
                     lease_expires_at = EXCLUDED.lease_expires_at,
                     attempts = CASE
                         WHEN object_cleanup_claims.deletion_ready_at IS NOT NULL
                             AND object_cleanup_claims.last_error IS NULL
                             THEN object_cleanup_claims.attempts
                         ELSE object_cleanup_claims.attempts + 1
                     END,
                     last_error = NULL,
                     updated_at = EXCLUDED.updated_at
                 WHERE object_cleanup_claims.completed_at IS NULL
                     AND object_cleanup_claims.lease_expires_at <= EXCLUDED.updated_at
                     AND (
                         object_cleanup_claims.attempts < 9223372036854775807
                         OR (
                             object_cleanup_claims.deletion_ready_at IS NOT NULL
                             AND object_cleanup_claims.last_error IS NULL
                         )
                     )
                     AND object_cleanup_claims.object_kind = EXCLUDED.object_kind
                     AND object_cleanup_claims.object_id = EXCLUDED.object_id
                 RETURNING repo_id, claim_kind, object_kind, object_id, object_key,
                     lease_owner, lease_token, lease_expires_at, attempts",
                &[
                    &request.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(request.claim_kind),
                    &object_kind_to_db(request.object_kind),
                    &request.object_id.to_hex(),
                    &request.object_key,
                    &request.lease_owner,
                    &lease_token,
                    &lease_duration_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("claim object cleanup", error))?;

        match row {
            Some(row) => row_to_cleanup_claim(row).map(Some),
            None => {
                reject_cleanup_claim_target_mismatch(&client, &request).await?;
                Ok(None)
            }
        }
    }

    async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let updated = client
            .execute(
                "UPDATE object_cleanup_claims
                 SET completed_at = clock_timestamp(),
                     last_error = NULL,
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()
                     AND (
                         claim_kind <> 'durable_mutation_cas_lost_object_cleanup'
                         OR (
                             final_object_bytes_deleted_at IS NOT NULL
                             AND final_object_metadata_deleted_at IS NOT NULL
                         )
                     )",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("complete object cleanup claim", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn record_failure(
        &self,
        claim: &ObjectCleanupClaim,
        message: &str,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let updated = client
            .execute(
                "UPDATE object_cleanup_claims
                 SET last_error = $9,
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                    &message,
                ],
            )
            .await
            .map_err(|error| postgres_error("record object cleanup claim failure", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn release(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let updated = client
            .execute(
                "UPDATE object_cleanup_claims
                 SET lease_expires_at = clock_timestamp(),
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("release object cleanup claim", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn validate(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                "SELECT 1
                 FROM object_cleanup_claims
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("validate object cleanup claim", error))?;
        if row.is_some() {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn current_time(&self) -> Result<SystemTime, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_one("SELECT clock_timestamp() AS now", &[])
            .await
            .map_err(|error| postgres_error("read object cleanup claim clock", error))?;
        Ok(row.get("now"))
    }

    async fn mark_deletion_ready(
        &self,
        claim: &ObjectCleanupClaim,
        readiness: FinalObjectDeletionReadiness,
    ) -> Result<(), VfsError> {
        let snapshot_size = u64_to_i64(
            readiness.snapshot.size,
            "final object deletion snapshot size",
        )?;
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin object cleanup deletion readiness", error))?;
        let row = tx
            .query_opt(
                "SELECT deletion_ready_at, delete_after, deletion_snapshot_object_key,
                    deletion_snapshot_size_bytes, deletion_snapshot_sha256,
                    final_object_bytes_deleted_at
                 FROM object_cleanup_claims
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()
                 FOR UPDATE",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("load object cleanup deletion readiness", error))?;
        let Some(row) = row else {
            return Err(stale_cleanup_claim());
        };

        let existing_snapshot = deletion_snapshot_from_claim_row(&row)?;
        if let Some(existing_snapshot) = existing_snapshot {
            if existing_snapshot != readiness.snapshot {
                let updated = tx
                    .execute(
                        "UPDATE object_cleanup_claims
                     SET deletion_ready_at = NULL,
                         delete_after = NULL,
                         deletion_snapshot_object_key = NULL,
                         deletion_snapshot_size_bytes = NULL,
                         deletion_snapshot_sha256 = NULL,
                         final_object_bytes_deleted_at = NULL,
                         final_object_metadata_deleted_at = NULL,
                         updated_at = clock_timestamp()
                     WHERE repo_id = $1
                         AND claim_kind = $2
                         AND object_kind = $3
                         AND object_id = $4
                         AND object_key = $5
                         AND lease_owner = $6
                         AND lease_token = $7
                         AND completed_at IS NULL
                         AND lease_expires_at = $8
                         AND lease_expires_at > clock_timestamp()",
                        &[
                            &claim.repo_id.as_str(),
                            &cleanup_claim_kind_to_db(claim.claim_kind),
                            &object_kind_to_db(claim.object_kind),
                            &claim.object_id.to_hex(),
                            &claim.object_key,
                            &claim.lease_owner,
                            &claim.lease_token.to_string(),
                            &claim.lease_expires_at,
                        ],
                    )
                    .await
                    .map_err(|error| {
                        postgres_error("clear mismatched object cleanup deletion readiness", error)
                    })?;
                if updated != 1 {
                    return Err(stale_cleanup_claim());
                }
                tx.commit().await.map_err(|error| {
                    postgres_error("commit mismatched object cleanup deletion readiness", error)
                })?;
                return Err(VfsError::ObjectWriteConflict {
                    message: "final object deletion readiness metadata snapshot changed; retry"
                        .to_string(),
                });
            }

            let existing_delete_after: SystemTime = row
                .get::<_, Option<SystemTime>>("delete_after")
                .ok_or_else(|| VfsError::CorruptStore {
                    message: "cleanup claim deletion readiness is missing delete_after".to_string(),
                })?;
            let existing_bytes_deleted_at: Option<SystemTime> =
                row.get("final_object_bytes_deleted_at");
            let delete_after = if existing_bytes_deleted_at.is_some() {
                existing_delete_after
            } else {
                existing_delete_after.max(readiness.delete_after)
            };
            let updated = tx
                .execute(
                    "UPDATE object_cleanup_claims
                 SET delete_after = $8,
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $9
                     AND lease_expires_at > clock_timestamp()",
                    &[
                        &claim.repo_id.as_str(),
                        &cleanup_claim_kind_to_db(claim.claim_kind),
                        &object_kind_to_db(claim.object_kind),
                        &claim.object_id.to_hex(),
                        &claim.object_key,
                        &claim.lease_owner,
                        &claim.lease_token.to_string(),
                        &delete_after,
                        &claim.lease_expires_at,
                    ],
                )
                .await
                .map_err(|error| {
                    postgres_error("extend object cleanup deletion readiness", error)
                })?;
            if updated != 1 {
                return Err(stale_cleanup_claim());
            }
            tx.commit().await.map_err(|error| {
                postgres_error("commit object cleanup deletion readiness", error)
            })?;
            return Ok(());
        }

        let updated = tx
            .execute(
                "UPDATE object_cleanup_claims
             SET deletion_ready_at = $8,
                 delete_after = $9,
                 deletion_snapshot_object_key = $10,
                 deletion_snapshot_size_bytes = $11,
                 deletion_snapshot_sha256 = $12,
                 final_object_bytes_deleted_at = NULL,
                 final_object_metadata_deleted_at = NULL,
                 updated_at = clock_timestamp()
             WHERE repo_id = $1
                 AND claim_kind = $2
                 AND object_kind = $3
                 AND object_id = $4
                 AND object_key = $5
                 AND lease_owner = $6
                 AND lease_token = $7
                 AND completed_at IS NULL
                 AND lease_expires_at = $13
                 AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &readiness.deletion_ready_at,
                    &readiness.delete_after,
                    &readiness.snapshot.object_key,
                    &snapshot_size,
                    &readiness.snapshot.sha256,
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("mark object cleanup deletion readiness", error))?;
        if updated != 1 {
            return Err(stale_cleanup_claim());
        }
        tx.commit()
            .await
            .map_err(|error| postgres_error("commit object cleanup deletion readiness", error))?;
        Ok(())
    }

    async fn clear_deletion_ready(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let updated = client
            .execute(
                "UPDATE object_cleanup_claims
                 SET deletion_ready_at = NULL,
                     delete_after = NULL,
                     deletion_snapshot_object_key = NULL,
                     deletion_snapshot_size_bytes = NULL,
                     deletion_snapshot_sha256 = NULL,
                     final_object_bytes_deleted_at = NULL,
                     final_object_metadata_deleted_at = NULL,
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("clear object cleanup deletion readiness", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn validate_final_object_deletion_ready(
        &self,
        claim: &ObjectCleanupClaim,
        readiness: &FinalObjectDeletionReadiness,
    ) -> Result<(), VfsError> {
        let snapshot_size = u64_to_i64(
            readiness.snapshot.size,
            "final object deletion snapshot size",
        )?;
        let client = self.connect_client().await?;
        let current = client
            .query_opt(
                "SELECT 1
                 FROM object_cleanup_claims
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND claim_kind = 'durable_mutation_cas_lost_object_cleanup'
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()
                     AND deletion_ready_at = $9
                     AND delete_after = $10
                     AND delete_after <= clock_timestamp()
                     AND deletion_snapshot_object_key = $11
                     AND deletion_snapshot_size_bytes = $12
                     AND deletion_snapshot_sha256 = $13",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                    &readiness.deletion_ready_at,
                    &readiness.delete_after,
                    &readiness.snapshot.object_key,
                    &snapshot_size,
                    &readiness.snapshot.sha256,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error(
                    "validate object cleanup final object deletion readiness",
                    error,
                )
            })?;
        if current.is_some() {
            Ok(())
        } else {
            Err(VfsError::ObjectWriteConflict {
                message: "object cleanup final object deletion readiness is not current"
                    .to_string(),
            })
        }
    }

    async fn validate_final_object_deletion_readiness_snapshot(
        &self,
        claim: &ObjectCleanupClaim,
        readiness: &FinalObjectDeletionReadiness,
    ) -> Result<(), VfsError> {
        let snapshot_size = u64_to_i64(
            readiness.snapshot.size,
            "final object deletion snapshot size",
        )?;
        let client = self.connect_client().await?;
        let current = client
            .query_opt(
                "SELECT 1
                 FROM object_cleanup_claims
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND claim_kind = 'durable_mutation_cas_lost_object_cleanup'
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()
                     AND deletion_ready_at = $9
                     AND delete_after = $10
                     AND deletion_snapshot_object_key = $11
                     AND deletion_snapshot_size_bytes = $12
                     AND deletion_snapshot_sha256 = $13",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                    &readiness.deletion_ready_at,
                    &readiness.delete_after,
                    &readiness.snapshot.object_key,
                    &snapshot_size,
                    &readiness.snapshot.sha256,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error(
                    "validate object cleanup final object deletion readiness snapshot",
                    error,
                )
            })?;
        if current.is_some() {
            Ok(())
        } else {
            Err(VfsError::ObjectWriteConflict {
                message: "object cleanup final object deletion readiness is not current"
                    .to_string(),
            })
        }
    }

    async fn mark_final_object_bytes_deleted(
        &self,
        claim: &ObjectCleanupClaim,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let updated = client
            .execute(
                "UPDATE object_cleanup_claims
                 SET final_object_bytes_deleted_at = COALESCE(
                         final_object_bytes_deleted_at,
                         clock_timestamp()
                     ),
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()
                     AND claim_kind = 'durable_mutation_cas_lost_object_cleanup'
                     AND deletion_ready_at IS NOT NULL
                     AND delete_after IS NOT NULL
                     AND delete_after <= clock_timestamp()
                     AND deletion_snapshot_object_key IS NOT NULL
                     AND deletion_snapshot_size_bytes IS NOT NULL
                     AND deletion_snapshot_sha256 IS NOT NULL",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("mark object cleanup final object bytes deleted", error)
            })?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn mark_final_object_metadata_deleted(
        &self,
        claim: &ObjectCleanupClaim,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let updated = client
            .execute(
                "UPDATE object_cleanup_claims
                 SET final_object_metadata_deleted_at = COALESCE(
                         final_object_metadata_deleted_at,
                         clock_timestamp()
                     ),
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_kind = $3
                     AND object_id = $4
                     AND object_key = $5
                     AND lease_owner = $6
                     AND lease_token = $7
                     AND completed_at IS NULL
                     AND lease_expires_at = $8
                     AND lease_expires_at > clock_timestamp()
                     AND claim_kind = 'durable_mutation_cas_lost_object_cleanup'
                     AND deletion_ready_at IS NOT NULL
                     AND delete_after IS NOT NULL
                     AND deletion_snapshot_object_key IS NOT NULL
                     AND deletion_snapshot_size_bytes IS NOT NULL
                     AND deletion_snapshot_sha256 IS NOT NULL
                     AND final_object_bytes_deleted_at IS NOT NULL",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &object_kind_to_db(claim.object_kind),
                    &claim.object_id.to_hex(),
                    &claim.object_key,
                    &claim.lease_owner,
                    &claim.lease_token.to_string(),
                    &claim.lease_expires_at,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("mark object cleanup final object metadata deleted", error)
            })?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_cleanup_claim())
        }
    }

    async fn list(&self, limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = usize_to_i64(limit, "object cleanup claim list limit")?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, claim_kind, object_kind, object_id, object_key,
                    lease_expires_at, attempts, completed_at, created_at, updated_at,
                    deletion_ready_at, delete_after, deletion_snapshot_object_key,
                    deletion_snapshot_size_bytes, deletion_snapshot_sha256,
                    final_object_bytes_deleted_at,
                    final_object_metadata_deleted_at,
                    last_error IS NOT NULL AS has_last_failure,
                    clock_timestamp() AS read_at
                 FROM object_cleanup_claims
                 ORDER BY
                    CASE WHEN completed_at IS NULL THEN 0 ELSE 1 END,
                    updated_at ASC,
                    repo_id ASC,
                    object_key ASC
                 LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|error| postgres_error("list object cleanup claims", error))?;
        rows.into_iter().map(row_to_cleanup_claim_status).collect()
    }

    async fn list_for_repo(
        &self,
        repo_id: &RepoId,
        limit: usize,
    ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = usize_to_i64(limit, "repo object cleanup claim list limit")?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, claim_kind, object_kind, object_id, object_key,
                    lease_expires_at, attempts, completed_at, created_at, updated_at,
                    deletion_ready_at, delete_after, deletion_snapshot_object_key,
                    deletion_snapshot_size_bytes, deletion_snapshot_sha256,
                    final_object_bytes_deleted_at,
                    final_object_metadata_deleted_at,
                    last_error IS NOT NULL AS has_last_failure,
                    clock_timestamp() AS read_at
                 FROM object_cleanup_claims
                 WHERE repo_id = $1
                 ORDER BY
                    CASE WHEN completed_at IS NULL THEN 0 ELSE 1 END,
                    updated_at ASC,
                    repo_id ASC,
                    object_key ASC
                 LIMIT $2",
                &[&repo_id.as_str(), &limit],
            )
            .await
            .map_err(|error| postgres_error("list repo object cleanup claims", error))?;
        rows.into_iter().map(row_to_cleanup_claim_status).collect()
    }

    async fn list_claimable_for_repo_and_kind(
        &self,
        repo_id: &RepoId,
        claim_kind: ObjectCleanupClaimKind,
        limit: usize,
    ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = usize_to_i64(limit, "claimable object cleanup claim list limit")?;
        let max_attempts = u64_to_i64(
            ObjectCleanupWorker::MAX_ATTEMPTS,
            "object cleanup max attempts",
        )?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "WITH claim_clock AS (
                    SELECT clock_timestamp() AS now
                 )
                 SELECT repo_id, claim_kind, object_kind, object_id, object_key,
                    lease_expires_at, attempts, completed_at, created_at, updated_at,
                    deletion_ready_at, delete_after, deletion_snapshot_object_key,
                    deletion_snapshot_size_bytes, deletion_snapshot_sha256,
                    final_object_bytes_deleted_at,
                    final_object_metadata_deleted_at,
                    last_error IS NOT NULL AS has_last_failure,
                    claim_clock.now AS read_at
                 FROM object_cleanup_claims, claim_clock
                 WHERE repo_id = $1
                    AND claim_kind = $2
                    AND completed_at IS NULL
                    AND lease_expires_at <= claim_clock.now
                 ORDER BY
                    CASE
                        WHEN delete_after IS NOT NULL AND delete_after > claim_clock.now THEN 1
                        ELSE 0
                    END,
                    CASE
                        WHEN attempts >= $3
                            AND NOT (deletion_ready_at IS NOT NULL AND last_error IS NULL) THEN 1
                        ELSE 0
                    END,
                    updated_at ASC,
                    object_key ASC
                 LIMIT $4",
                &[
                    &repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim_kind),
                    &max_attempts,
                    &limit,
                ],
            )
            .await
            .map_err(|error| postgres_error("list claimable object cleanup claims", error))?;
        rows.into_iter().map(row_to_cleanup_claim_status).collect()
    }

    async fn counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "WITH claim_clock AS (
                    SELECT clock_timestamp() AS now
                 )
                 SELECT
                    CASE
                        WHEN completed_at IS NOT NULL THEN 'completed'
                        WHEN last_error IS NOT NULL THEN 'failed'
                        WHEN lease_expires_at <= claim_clock.now THEN 'stale_active'
                        ELSE 'active'
                    END AS state,
                    count(*)::bigint AS count
                 FROM object_cleanup_claims, claim_clock
                 GROUP BY state",
                &[],
            )
            .await
            .map_err(|error| postgres_error("count object cleanup claims", error))?;
        let mut counts = ObjectCleanupClaimCounts::default();
        for row in rows {
            let state = cleanup_claim_state_from_db(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "object cleanup claim count")?;
            counts.add(state, count);
        }
        let max_attempts = u64_to_i64(
            ObjectCleanupWorker::MAX_ATTEMPTS,
            "object cleanup max attempts",
        )?;
        let row = client
            .query_one(
                "WITH claim_clock AS (
                    SELECT clock_timestamp() AS now
                 )
                 SELECT
                    count(*) FILTER (
                        WHERE completed_at IS NULL
                            AND deletion_ready_at IS NOT NULL
                    )::bigint AS deletion_ready,
                    count(*) FILTER (
                        WHERE completed_at IS NULL
                            AND delete_after > claim_clock.now
                    )::bigint AS deletion_held,
                    count(*) FILTER (
                        WHERE final_object_bytes_deleted_at IS NOT NULL
                            AND final_object_metadata_deleted_at IS NOT NULL
                    )::bigint AS deleted_final_objects,
                    count(*) FILTER (
                        WHERE completed_at IS NULL
                            AND attempts >= $1
                            AND NOT (deletion_ready_at IS NOT NULL AND last_error IS NULL)
                    )::bigint AS poisoned
                 FROM object_cleanup_claims, claim_clock",
                &[&max_attempts],
            )
            .await
            .map_err(|error| postgres_error("count object cleanup deletion status", error))?;
        counts.add_deletion_summary(
            i64_to_usize(
                row.get("deletion_ready"),
                "object cleanup deletion ready count",
            )?,
            i64_to_usize(
                row.get("deletion_held"),
                "object cleanup deletion held count",
            )?,
            i64_to_usize(
                row.get("deleted_final_objects"),
                "object cleanup deleted final object count",
            )?,
            i64_to_usize(row.get("poisoned"), "object cleanup poisoned count")?,
        );
        Ok(counts)
    }

    async fn counts_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<ObjectCleanupClaimCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "WITH claim_clock AS (
                    SELECT clock_timestamp() AS now
                 )
                 SELECT
                    CASE
                        WHEN completed_at IS NOT NULL THEN 'completed'
                        WHEN last_error IS NOT NULL THEN 'failed'
                        WHEN lease_expires_at <= claim_clock.now THEN 'stale_active'
                        ELSE 'active'
                    END AS state,
                    count(*)::bigint AS count
                 FROM object_cleanup_claims, claim_clock
                 WHERE repo_id = $1
                 GROUP BY state",
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("count repo object cleanup claims", error))?;
        let mut counts = ObjectCleanupClaimCounts::default();
        for row in rows {
            let state = cleanup_claim_state_from_db(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "repo object cleanup claim count")?;
            counts.add(state, count);
        }
        let max_attempts = u64_to_i64(
            ObjectCleanupWorker::MAX_ATTEMPTS,
            "object cleanup max attempts",
        )?;
        let row = client
            .query_one(
                "WITH claim_clock AS (
                    SELECT clock_timestamp() AS now
                 )
                 SELECT
                    count(*) FILTER (
                        WHERE completed_at IS NULL
                            AND deletion_ready_at IS NOT NULL
                    )::bigint AS deletion_ready,
                    count(*) FILTER (
                        WHERE completed_at IS NULL
                            AND delete_after > claim_clock.now
                    )::bigint AS deletion_held,
                    count(*) FILTER (
                        WHERE final_object_bytes_deleted_at IS NOT NULL
                            AND final_object_metadata_deleted_at IS NOT NULL
                    )::bigint AS deleted_final_objects,
                    count(*) FILTER (
                        WHERE completed_at IS NULL
                            AND attempts >= $1
                            AND NOT (deletion_ready_at IS NOT NULL AND last_error IS NULL)
                    )::bigint AS poisoned
                 FROM object_cleanup_claims, claim_clock
                 WHERE repo_id = $2",
                &[&max_attempts, &repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("count repo object cleanup deletion status", error))?;
        counts.add_deletion_summary(
            i64_to_usize(
                row.get("deletion_ready"),
                "repo object cleanup deletion ready count",
            )?,
            i64_to_usize(
                row.get("deletion_held"),
                "repo object cleanup deletion held count",
            )?,
            i64_to_usize(
                row.get("deleted_final_objects"),
                "repo object cleanup deleted final object count",
            )?,
            i64_to_usize(row.get("poisoned"), "repo object cleanup poisoned count")?,
        );
        Ok(counts)
    }
}

#[async_trait]
impl DurableCorePostCasRecoveryClaimStore for PostgresMetadataStore {
    async fn enqueue(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        ensure_repo(&client, target.repo_id()).await?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery enqueue time")?;
        client
            .execute(
                "INSERT INTO durable_post_cas_recovery_claims (
                    repo_id, ref_name, commit_id, step, state, attempts, created_at, updated_at
                 )
                 VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    'pending',
                    0,
                    to_timestamp($5::bigint::double precision / 1000.0),
                    to_timestamp($5::bigint::double precision / 1000.0)
                 )
                 ON CONFLICT (repo_id, ref_name, commit_id, step) DO NOTHING",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("enqueue post-CAS recovery claim", error))?;
        Ok(())
    }

    async fn enqueue_with_context(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        context: DurableCorePostCasRecoveryContext,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let mut client = self.connect_client().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin contextual post-CAS recovery enqueue", error))?;
        ensure_repo(&transaction, target.repo_id()).await?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery enqueue time")?;
        let context_json = post_cas_recovery_context_to_json(&context)?;

        let inserted = transaction
            .query_opt(
                "INSERT INTO durable_post_cas_recovery_claims (
                    repo_id, ref_name, commit_id, step, state, attempts, context_json,
                    created_at, updated_at
                 )
                 VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    'pending',
                    0,
                    $5,
                    to_timestamp($6::bigint::double precision / 1000.0),
                    to_timestamp($6::bigint::double precision / 1000.0)
                 )
                 ON CONFLICT (repo_id, ref_name, commit_id, step) DO NOTHING
                 RETURNING 1",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                    &Json(&context_json),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("enqueue contextual post-CAS recovery claim", error))?;
        if inserted.is_some() {
            transaction.commit().await.map_err(|error| {
                postgres_error("commit contextual post-CAS recovery enqueue", error)
            })?;
            return Ok(());
        }

        let row = transaction
            .query_opt(
                "SELECT state, context_json
                 FROM durable_post_cas_recovery_claims
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                 FOR UPDATE",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                ],
            )
            .await
            .map_err(|error| postgres_error("lock contextual post-CAS recovery claim", error))?
            .ok_or_else(contextual_post_cas_recovery_enqueue_conflict)?;
        let state = DurableCorePostCasRecoveryState::from_str(row.get("state"))?;
        let existing_context: Option<Json<serde_json::Value>> = row
            .try_get("context_json")
            .map_err(|_| post_cas_recovery_context_corrupt())?;
        let existing_context = existing_context
            .map(|Json(value)| post_cas_recovery_context_from_json(value))
            .transpose()?;

        match (state, existing_context.as_ref()) {
            (DurableCorePostCasRecoveryState::Poisoned, _) => {
                Err(contextual_post_cas_recovery_enqueue_conflict())
            }
            (
                DurableCorePostCasRecoveryState::Pending
                | DurableCorePostCasRecoveryState::BackingOff,
                Some(existing),
            ) if existing.can_replace_with_partial_idempotency_response(&target, &context) => {
                transaction
                    .execute(
                        "UPDATE durable_post_cas_recovery_claims
                         SET context_json = $5,
                             updated_at = to_timestamp($6::bigint::double precision / 1000.0)
                         WHERE repo_id = $1
                             AND ref_name = $2
                             AND commit_id = $3
                             AND step = $4",
                        &[
                            &target.repo_id().as_str(),
                            &target.ref_name(),
                            &target.commit_id().to_hex(),
                            &target.step().as_str(),
                            &Json(&context_json),
                            &now_millis,
                        ],
                    )
                    .await
                    .map_err(|error| {
                        postgres_error("update contextual post-CAS recovery claim", error)
                    })?;
                transaction.commit().await.map_err(|error| {
                    postgres_error("commit contextual post-CAS recovery enqueue", error)
                })?;
                Ok(())
            }
            (_, Some(_)) => {
                transaction.commit().await.map_err(|error| {
                    postgres_error("commit contextual post-CAS recovery enqueue", error)
                })?;
                Ok(())
            }
            (
                DurableCorePostCasRecoveryState::Pending
                | DurableCorePostCasRecoveryState::BackingOff,
                None,
            ) => {
                transaction
                    .execute(
                        "UPDATE durable_post_cas_recovery_claims
                         SET context_json = $5,
                             updated_at = to_timestamp($6::bigint::double precision / 1000.0)
                         WHERE repo_id = $1
                             AND ref_name = $2
                             AND commit_id = $3
                             AND step = $4",
                        &[
                            &target.repo_id().as_str(),
                            &target.ref_name(),
                            &target.commit_id().to_hex(),
                            &target.step().as_str(),
                            &Json(&context_json),
                            &now_millis,
                        ],
                    )
                    .await
                    .map_err(|error| {
                        postgres_error("upgrade contextual post-CAS recovery claim", error)
                    })?;
                transaction.commit().await.map_err(|error| {
                    postgres_error("commit contextual post-CAS recovery enqueue", error)
                })?;
                Ok(())
            }
            (
                DurableCorePostCasRecoveryState::Active
                | DurableCorePostCasRecoveryState::Completed,
                None,
            ) => Err(contextual_post_cas_recovery_enqueue_conflict()),
        }
    }

    async fn enqueue_with_context_and_claim(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        context: DurableCorePostCasRecoveryContext,
        lease_owner: &str,
        lease_duration: Duration,
        now_millis: u64,
    ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError> {
        validate_lease_owner(lease_owner)?;
        let mut client = self.connect_client().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin claimed post-CAS recovery enqueue", error))?;
        ensure_repo(&transaction, target.repo_id()).await?;
        let context_json = post_cas_recovery_context_to_json(&context)?;
        let lease_token = Uuid::new_v4().to_string();
        let lease_duration_millis =
            duration_to_i64_millis(lease_duration, "post-CAS recovery lease duration")?;
        let now_millis_i64 = u64_to_i64(now_millis, "post-CAS recovery enqueue claim time")?;
        let inserted = transaction
            .query_opt(
                "INSERT INTO durable_post_cas_recovery_claims (
                    repo_id, ref_name, commit_id, step, state, attempts, lease_owner,
                    lease_token, lease_expires_at, context_json, created_at, updated_at
                 )
                 VALUES (
                    $1, $2, $3, $4, 'active', 1, $5, $6,
                    to_timestamp($8::bigint::double precision / 1000.0)
                        + ($7::bigint * interval '1 millisecond'),
                    $9,
                    to_timestamp($8::bigint::double precision / 1000.0),
                    to_timestamp($8::bigint::double precision / 1000.0)
                 )
                 ON CONFLICT (repo_id, ref_name, commit_id, step) DO NOTHING
                 RETURNING repo_id, ref_name, commit_id, step, lease_owner, lease_token,
                     lease_expires_at, attempts, context_json",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                    &lease_owner,
                    &lease_token,
                    &lease_duration_millis,
                    &now_millis_i64,
                    &Json(&context_json),
                ],
            )
            .await
            .map_err(|error| postgres_error("enqueue claimed post-CAS recovery", error))?;
        if let Some(row) = inserted {
            let claim = row_to_post_cas_recovery_claim(row)?;
            transaction.commit().await.map_err(|error| {
                postgres_error("commit claimed post-CAS recovery enqueue", error)
            })?;
            return Ok(Some(claim));
        }

        let existing = transaction
            .query_opt(
                "SELECT state, context_json
                 FROM durable_post_cas_recovery_claims
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                 FOR UPDATE",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                ],
            )
            .await
            .map_err(|error| postgres_error("lock claimed post-CAS recovery", error))?
            .ok_or_else(contextual_post_cas_recovery_enqueue_conflict)?;
        let existing_context = row_to_post_cas_recovery_context(&existing)?;
        if let Some(existing_context) = existing_context.as_ref()
            && !existing_context.can_replace_with_partial_idempotency_response(&target, &context)
            && existing_context != &context
        {
            return Err(contextual_post_cas_recovery_enqueue_conflict());
        }
        let row = transaction
            .query_opt(
                "UPDATE durable_post_cas_recovery_claims
                 SET state = 'active',
                     lease_owner = $5,
                     lease_token = $6,
                     lease_expires_at = to_timestamp($8::bigint::double precision / 1000.0)
                        + ($7::bigint * interval '1 millisecond'),
                     attempts = attempts + 1,
                     retry_after = NULL,
                     last_error = NULL,
                     completed_at = NULL,
                     poisoned_at = NULL,
                     context_json = $9,
                     updated_at = to_timestamp($8::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($8::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($8::bigint::double precision / 1000.0)
                        )
                     )
                 RETURNING repo_id, ref_name, commit_id, step, lease_owner, lease_token,
                     lease_expires_at, attempts, context_json",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                    &lease_owner,
                    &lease_token,
                    &lease_duration_millis,
                    &now_millis_i64,
                    &Json(&context_json),
                ],
            )
            .await
            .map_err(|error| postgres_error("claim existing post-CAS recovery", error))?;
        let claim = row.map(row_to_post_cas_recovery_claim).transpose()?;
        transaction.commit().await.map_err(|error| {
            postgres_error("commit claimed post-CAS recovery enqueue fallback", error)
        })?;
        Ok(claim)
    }

    async fn replace_claim_context(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        context: DurableCorePostCasRecoveryContext,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        if let Some(existing) = claim.context()
            && !existing.can_replace_with_partial_idempotency_response(claim.target(), &context)
            && existing != &context
        {
            return Err(contextual_post_cas_recovery_enqueue_conflict());
        }
        let client = self.connect_client().await?;
        let context_json = post_cas_recovery_context_to_json(&context)?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery context replacement time")?;
        let target = claim.target();
        let updated = client
            .execute(
                "UPDATE durable_post_cas_recovery_claims
                 SET context_json = $7,
                     updated_at = to_timestamp($8::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($8::bigint::double precision / 1000.0)",
                &[
                    &target.repo_id().as_str(),
                    &target.ref_name(),
                    &target.commit_id().to_hex(),
                    &target.step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &Json(&context_json),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("replace post-CAS recovery claim context", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_post_cas_recovery_claim())
        }
    }

    async fn claim(
        &self,
        request: DurableCorePostCasRecoveryClaimRequest,
    ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError> {
        validate_lease_owner(request.lease_owner())?;
        let mut client = self.connect_client().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin post-CAS recovery claim", error))?;
        ensure_repo(&transaction, request.target().repo_id()).await?;
        let lease_token = Uuid::new_v4().to_string();
        let lease_duration_millis =
            duration_to_i64_millis(request.lease_duration(), "post-CAS recovery lease duration")?;
        let now_millis = u64_to_i64(request.now_millis(), "post-CAS recovery claim time")?;

        let candidate = transaction
            .query_opt(
                "SELECT context_json
                 FROM durable_post_cas_recovery_claims
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($5::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($5::bigint::double precision / 1000.0)
                        )
                     )
                 FOR UPDATE",
                &[
                    &request.target().repo_id().as_str(),
                    &request.target().ref_name(),
                    &request.target().commit_id().to_hex(),
                    &request.target().step().as_str(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("lock post-CAS recovery claim", error))?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let _ = row_to_post_cas_recovery_context(&candidate)?;

        let row = transaction
            .query_opt(
                "UPDATE durable_post_cas_recovery_claims
                 SET state = 'active',
                     lease_owner = $5,
                     lease_token = $6,
                     lease_expires_at = to_timestamp($8::bigint::double precision / 1000.0)
                        + ($7::bigint * interval '1 millisecond'),
                     attempts = attempts + 1,
                     retry_after = NULL,
                     last_error = NULL,
                     completed_at = NULL,
                     poisoned_at = NULL,
                     updated_at = to_timestamp($8::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($8::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($8::bigint::double precision / 1000.0)
                        )
                     )
                 RETURNING repo_id, ref_name, commit_id, step, lease_owner, lease_token,
                     lease_expires_at, attempts, context_json",
                &[
                    &request.target().repo_id().as_str(),
                    &request.target().ref_name(),
                    &request.target().commit_id().to_hex(),
                    &request.target().step().as_str(),
                    &request.lease_owner(),
                    &lease_token,
                    &lease_duration_millis,
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("claim post-CAS recovery", error))?;

        let Some(row) = row else {
            return Ok(None);
        };
        let claim = row_to_post_cas_recovery_claim(row)?;
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit post-CAS recovery claim", error))?;
        Ok(Some(claim))
    }

    async fn complete(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery completion time")?;
        let updated = client
            .execute(
                "UPDATE durable_post_cas_recovery_claims
                 SET state = 'completed',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = NULL,
                     last_error = NULL,
                     completed_at = to_timestamp($7::bigint::double precision / 1000.0),
                     poisoned_at = NULL,
                     updated_at = to_timestamp($7::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($7::bigint::double precision / 1000.0)",
                &[
                    &claim.target().repo_id().as_str(),
                    &claim.target().ref_name(),
                    &claim.target().commit_id().to_hex(),
                    &claim.target().step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("complete post-CAS recovery claim", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_post_cas_recovery_claim())
        }
    }

    async fn record_failure(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        _diagnosis: &str,
        backoff: std::time::Duration,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        validate_post_cas_recovery_backoff(backoff)?;
        let backoff_millis = duration_to_i64_millis(backoff, "post-CAS recovery backoff duration")?;
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery failure time")?;
        let updated = client
            .execute(
                "UPDATE durable_post_cas_recovery_claims
                 SET state = 'backing_off',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = to_timestamp($8::bigint::double precision / 1000.0)
                        + ($7::bigint * interval '1 millisecond'),
                     last_error = 'redacted post-CAS recovery failure',
                     completed_at = NULL,
                     poisoned_at = NULL,
                     updated_at = to_timestamp($8::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($8::bigint::double precision / 1000.0)",
                &[
                    &claim.target().repo_id().as_str(),
                    &claim.target().ref_name(),
                    &claim.target().commit_id().to_hex(),
                    &claim.target().step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &backoff_millis,
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("record post-CAS recovery failure", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_post_cas_recovery_claim())
        }
    }

    async fn poison(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        _diagnosis: &str,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery poison time")?;
        let updated = client
            .execute(
                "UPDATE durable_post_cas_recovery_claims
                 SET state = 'poisoned',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = NULL,
                     last_error = 'redacted post-CAS recovery failure',
                     completed_at = NULL,
                     poisoned_at = to_timestamp($7::bigint::double precision / 1000.0),
                     updated_at = to_timestamp($7::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($7::bigint::double precision / 1000.0)",
                &[
                    &claim.target().repo_id().as_str(),
                    &claim.target().ref_name(),
                    &claim.target().commit_id().to_hex(),
                    &claim.target().step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("poison post-CAS recovery claim", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_post_cas_recovery_claim())
        }
    }

    async fn list(&self, limit: usize) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
        let limit = usize_to_i64(limit, "post-CAS recovery list limit")?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, ref_name, commit_id, step, state, attempts,
                    lease_expires_at, retry_after, completed_at, poisoned_at, last_error,
                    created_at, updated_at
                 FROM durable_post_cas_recovery_claims
                 ORDER BY
                    CASE state
                        WHEN 'pending' THEN 0
                        WHEN 'backing_off' THEN 1
                        WHEN 'active' THEN 2
                        WHEN 'poisoned' THEN 3
                        ELSE 4
                    END,
                    commit_id ASC,
                    CASE step
                        WHEN 'workspace_head_update' THEN 0
                        WHEN 'audit_append' THEN 1
                        WHEN 'idempotency_completion' THEN 2
                        ELSE 3
                    END,
                    updated_at DESC
                 LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|error| postgres_error("list post-CAS recovery claims", error))?;
        rows.into_iter()
            .map(row_to_post_cas_recovery_status)
            .collect()
    }

    async fn has_unresolved_for_ref(
        &self,
        repo_id: &RepoId,
        ref_name: &str,
    ) -> Result<bool, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM durable_post_cas_recovery_claims
                    WHERE repo_id = $1
                        AND ref_name = $2
                        AND state <> 'completed'
                 ) AS present",
                &[&repo_id.as_str(), &ref_name],
            )
            .await
            .map_err(|error| postgres_error("check post-CAS recovery unresolved", error))?;
        Ok(row.get("present"))
    }

    async fn list_repair_candidates(
        &self,
        now_millis: u64,
        limit: usize,
    ) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = usize_to_i64(limit, "post-CAS recovery candidate list limit")?;
        let now_millis = u64_to_i64(now_millis, "post-CAS recovery candidate list time")?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, ref_name, commit_id, step, state, attempts,
                    lease_expires_at, retry_after, completed_at, poisoned_at, last_error,
                    created_at, updated_at
                 FROM durable_post_cas_recovery_claims
                 WHERE attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($1::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($1::bigint::double precision / 1000.0)
                        )
                     )
                 ORDER BY
                    CASE state
                        WHEN 'pending' THEN 0
                        WHEN 'backing_off' THEN 1
                        WHEN 'active' THEN 2
                        ELSE 3
                    END,
                    commit_id ASC,
                    CASE step
                        WHEN 'workspace_head_update' THEN 0
                        WHEN 'audit_append' THEN 1
                        WHEN 'idempotency_completion' THEN 2
                        ELSE 3
                    END,
                    updated_at DESC
                 LIMIT $2",
                &[&now_millis, &limit],
            )
            .await
            .map_err(|error| postgres_error("list post-CAS recovery candidates", error))?;
        rows.into_iter()
            .map(row_to_post_cas_recovery_status)
            .collect()
    }

    async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT state, count(*)::bigint AS count
                 FROM durable_post_cas_recovery_claims
                 GROUP BY state",
                &[],
            )
            .await
            .map_err(|error| postgres_error("count post-CAS recovery claims", error))?;
        let mut counts = DurableCorePostCasRecoveryCounts::default();
        for row in rows {
            let state = DurableCorePostCasRecoveryState::from_str(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "post-CAS recovery count")?;
            counts.add(state, count);
        }
        Ok(counts)
    }

    async fn counts_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT state, count(*)::bigint AS count
                 FROM durable_post_cas_recovery_claims
                 WHERE repo_id = $1
                 GROUP BY state",
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("count repo post-CAS recovery claims", error))?;
        let mut counts = DurableCorePostCasRecoveryCounts::default();
        for row in rows {
            let state = DurableCorePostCasRecoveryState::from_str(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "repo post-CAS recovery count")?;
            counts.add(state, count);
        }
        Ok(counts)
    }
}

#[async_trait]
impl DurableFsMutationRecoveryStore for PostgresMetadataStore {
    async fn enqueue(
        &self,
        target: DurableFsMutationRecoveryTarget,
        envelope: DurableFsMutationRecoveryEnvelope,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let mut client = self.connect_client().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin durable FS mutation recovery enqueue", error))?;
        ensure_repo(&transaction, target.repo_id()).await?;
        let now_millis = u64_to_i64(now_millis, "durable FS mutation recovery enqueue time")?;
        let envelope_json = fs_mutation_recovery_envelope_to_json(&envelope)?;
        let inserted = transaction
            .query_opt(
                "INSERT INTO durable_fs_mutation_recovery_ledger (
                    repo_id, workspace_scope, operation_id, target_ref, previous_commit_id,
                    new_commit_id, failed_step, state, attempts, envelope_json, created_at, updated_at
                 )
                 VALUES (
                    $1, $2, $3, $4, $5, $6, $7, 'pending', 0, $8,
                    to_timestamp($9::bigint::double precision / 1000.0),
                    to_timestamp($9::bigint::double precision / 1000.0)
                 )
                 ON CONFLICT (
                    repo_id, workspace_scope, operation_id, target_ref, previous_commit_id,
                    new_commit_id, failed_step
                 ) DO NOTHING
                 RETURNING 1",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &Json(&envelope_json),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("enqueue durable FS mutation recovery", error))?;
        if inserted.is_none() {
            let row = transaction
                .query_opt(
                    "SELECT state, envelope_json
                     FROM durable_fs_mutation_recovery_ledger
                     WHERE repo_id = $1
                         AND workspace_scope = $2
                         AND operation_id = $3
                         AND target_ref = $4
                         AND previous_commit_id = $5
                         AND new_commit_id = $6
                         AND failed_step = $7
                     FOR UPDATE",
                    &[
                        &target.repo_id().as_str(),
                        &target.workspace_scope(),
                        &target.operation_id(),
                        &target.target_ref(),
                        &target.previous_commit().to_hex(),
                        &target.new_commit().to_hex(),
                        &target.failed_step().as_str(),
                    ],
                )
                .await
                .map_err(|error| postgres_error("lock durable FS mutation recovery", error))?
                .ok_or_else(fs_mutation_recovery_enqueue_conflict)?;
            let state = DurableFsMutationRecoveryState::from_str(row.get("state"))?;
            let Json(existing_json): Json<serde_json::Value> = row.get("envelope_json");
            let existing = fs_mutation_recovery_envelope_from_json(existing_json)?;
            if existing != envelope {
                if matches!(
                    state,
                    DurableFsMutationRecoveryState::Pending
                        | DurableFsMutationRecoveryState::BackingOff
                ) && existing.can_replace_idempotency_response_with(&envelope)
                {
                    transaction
                        .execute(
                            "UPDATE durable_fs_mutation_recovery_ledger
                             SET envelope_json = $8,
                                 updated_at = to_timestamp($9::bigint::double precision / 1000.0)
                             WHERE repo_id = $1
                                 AND workspace_scope = $2
                                 AND operation_id = $3
                                 AND target_ref = $4
                                 AND previous_commit_id = $5
                                 AND new_commit_id = $6
                                 AND failed_step = $7",
                            &[
                                &target.repo_id().as_str(),
                                &target.workspace_scope(),
                                &target.operation_id(),
                                &target.target_ref(),
                                &target.previous_commit().to_hex(),
                                &target.new_commit().to_hex(),
                                &target.failed_step().as_str(),
                                &Json(&envelope_json),
                                &now_millis,
                            ],
                        )
                        .await
                        .map_err(|error| {
                            postgres_error("update durable FS mutation recovery envelope", error)
                        })?;
                } else {
                    return Err(fs_mutation_recovery_enqueue_conflict());
                }
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit durable FS mutation recovery enqueue", error))
    }

    async fn enqueue_and_claim(
        &self,
        target: DurableFsMutationRecoveryTarget,
        envelope: DurableFsMutationRecoveryEnvelope,
        lease_owner: &str,
        lease_duration: Duration,
        now_millis: u64,
    ) -> Result<Option<DurableFsMutationRecoveryClaim>, VfsError> {
        validate_lease_owner(lease_owner)?;
        let mut client = self.connect_client().await?;
        let transaction = client.transaction().await.map_err(|error| {
            postgres_error("begin claimed durable FS mutation recovery enqueue", error)
        })?;
        ensure_repo(&transaction, target.repo_id()).await?;
        let envelope_json = fs_mutation_recovery_envelope_to_json(&envelope)?;
        let lease_token = Uuid::new_v4().to_string();
        let lease_duration_millis = duration_to_i64_millis(
            lease_duration,
            "durable FS mutation recovery lease duration",
        )?;
        let now_millis_i64 = u64_to_i64(
            now_millis,
            "durable FS mutation recovery enqueue claim time",
        )?;
        let inserted = transaction
            .query_opt(
                "INSERT INTO durable_fs_mutation_recovery_ledger (
                    repo_id, workspace_scope, operation_id, target_ref, previous_commit_id,
                    new_commit_id, failed_step, state, attempts, lease_owner, lease_token,
                    lease_expires_at, envelope_json, created_at, updated_at
                 )
                 VALUES (
                    $1, $2, $3, $4, $5, $6, $7, 'active', 1, $8, $9,
                    to_timestamp($11::bigint::double precision / 1000.0)
                        + ($10::bigint * interval '1 millisecond'),
                    $12,
                    to_timestamp($11::bigint::double precision / 1000.0),
                    to_timestamp($11::bigint::double precision / 1000.0)
                 )
                 ON CONFLICT (
                    repo_id, workspace_scope, operation_id, target_ref, previous_commit_id,
                    new_commit_id, failed_step
                 ) DO NOTHING
                 RETURNING repo_id, workspace_scope, operation_id, target_ref,
                     previous_commit_id, new_commit_id, failed_step, lease_owner, lease_token,
                     lease_expires_at, attempts, envelope_json",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &lease_owner,
                    &lease_token,
                    &lease_duration_millis,
                    &now_millis_i64,
                    &Json(&envelope_json),
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("enqueue claimed durable FS mutation recovery", error)
            })?;
        if let Some(row) = inserted {
            let claim = row_to_fs_mutation_recovery_claim(row)?;
            transaction.commit().await.map_err(|error| {
                postgres_error("commit claimed durable FS mutation recovery enqueue", error)
            })?;
            return Ok(Some(claim));
        }

        let existing = transaction
            .query_opt(
                "SELECT state, envelope_json
                 FROM durable_fs_mutation_recovery_ledger
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                 FOR UPDATE",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                ],
            )
            .await
            .map_err(|error| postgres_error("lock claimed durable FS mutation recovery", error))?
            .ok_or_else(fs_mutation_recovery_enqueue_conflict)?;
        let Json(existing_json): Json<serde_json::Value> = existing.get("envelope_json");
        let existing_envelope = fs_mutation_recovery_envelope_from_json(existing_json)?;
        if existing_envelope != envelope
            && !existing_envelope.can_replace_idempotency_response_with(&envelope)
        {
            return Err(fs_mutation_recovery_enqueue_conflict());
        }
        let row = transaction
            .query_opt(
                "UPDATE durable_fs_mutation_recovery_ledger
                 SET state = 'active',
                     lease_owner = $8,
                     lease_token = $9,
                     lease_expires_at = to_timestamp($11::bigint::double precision / 1000.0)
                        + ($10::bigint * interval '1 millisecond'),
                     attempts = attempts + 1,
                     retry_after = NULL,
                     last_error = NULL,
                     completed_at = NULL,
                     poisoned_at = NULL,
                     envelope_json = $12,
                     updated_at = to_timestamp($11::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($11::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($11::bigint::double precision / 1000.0)
                        )
                     )
                 RETURNING repo_id, workspace_scope, operation_id, target_ref,
                     previous_commit_id, new_commit_id, failed_step, lease_owner, lease_token,
                     lease_expires_at, attempts, envelope_json",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &lease_owner,
                    &lease_token,
                    &lease_duration_millis,
                    &now_millis_i64,
                    &Json(&envelope_json),
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("claim existing durable FS mutation recovery", error)
            })?;
        let claim = row.map(row_to_fs_mutation_recovery_claim).transpose()?;
        transaction.commit().await.map_err(|error| {
            postgres_error(
                "commit claimed durable FS mutation recovery enqueue fallback",
                error,
            )
        })?;
        Ok(claim)
    }

    async fn replace_claim_envelope(
        &self,
        claim: &DurableFsMutationRecoveryClaim,
        envelope: DurableFsMutationRecoveryEnvelope,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        if !claim
            .envelope()
            .can_replace_idempotency_response_with(&envelope)
            && claim.envelope() != &envelope
        {
            return Err(fs_mutation_recovery_enqueue_conflict());
        }
        let client = self.connect_client().await?;
        let envelope_json = fs_mutation_recovery_envelope_to_json(&envelope)?;
        let now_millis = u64_to_i64(
            now_millis,
            "durable FS mutation recovery envelope replacement time",
        )?;
        let target = claim.target();
        let updated = client
            .execute(
                "UPDATE durable_fs_mutation_recovery_ledger
                 SET envelope_json = $10,
                     updated_at = to_timestamp($11::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                     AND state = 'active'
                     AND lease_owner = $8
                     AND lease_token = $9
                     AND lease_expires_at > to_timestamp($11::bigint::double precision / 1000.0)",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &Json(&envelope_json),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("replace durable FS mutation recovery claim envelope", error)
            })?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_fs_mutation_recovery_claim())
        }
    }

    async fn claim(
        &self,
        request: DurableFsMutationRecoveryClaimRequest,
    ) -> Result<Option<DurableFsMutationRecoveryClaim>, VfsError> {
        validate_lease_owner(request.lease_owner())?;
        let client = self.connect_client().await?;
        let lease_token = Uuid::new_v4().to_string();
        let lease_duration_millis = duration_to_i64_millis(
            request.lease_duration(),
            "durable FS mutation recovery lease duration",
        )?;
        let now_millis = u64_to_i64(
            request.now_millis(),
            "durable FS mutation recovery claim time",
        )?;
        let target = request.target();
        let row = client
            .query_opt(
                "UPDATE durable_fs_mutation_recovery_ledger
                 SET state = 'active',
                     lease_owner = $8,
                     lease_token = $9,
                     lease_expires_at = to_timestamp($10::bigint::double precision / 1000.0)
                        + ($11::bigint * interval '1 millisecond'),
                     attempts = attempts + 1,
                     retry_after = NULL,
                     last_error = NULL,
                     completed_at = NULL,
                     poisoned_at = NULL,
                     updated_at = to_timestamp($10::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($10::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($10::bigint::double precision / 1000.0)
                        )
                     )
                 RETURNING repo_id, workspace_scope, operation_id, target_ref,
                     previous_commit_id, new_commit_id, failed_step, lease_owner, lease_token,
                     lease_expires_at, attempts, envelope_json",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &request.lease_owner(),
                    &lease_token,
                    &now_millis,
                    &lease_duration_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("claim durable FS mutation recovery", error))?;
        row.map(row_to_fs_mutation_recovery_claim).transpose()
    }

    async fn complete(
        &self,
        claim: &DurableFsMutationRecoveryClaim,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "durable FS mutation recovery completion time")?;
        let target = claim.target();
        let updated = client
            .execute(
                "UPDATE durable_fs_mutation_recovery_ledger
                 SET state = 'completed',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = NULL,
                     last_error = NULL,
                     completed_at = to_timestamp($10::bigint::double precision / 1000.0),
                     poisoned_at = NULL,
                     updated_at = to_timestamp($10::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                     AND state = 'active'
                     AND lease_owner = $8
                     AND lease_token = $9
                     AND lease_expires_at > to_timestamp($10::bigint::double precision / 1000.0)",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("complete durable FS mutation recovery", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_fs_mutation_recovery_claim())
        }
    }

    async fn record_failure(
        &self,
        claim: &DurableFsMutationRecoveryClaim,
        _diagnosis: &str,
        backoff: Duration,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        validate_durable_fs_mutation_recovery_backoff(backoff)?;
        let backoff_millis =
            duration_to_i64_millis(backoff, "durable FS mutation recovery backoff duration")?;
        let now_millis = u64_to_i64(now_millis, "durable FS mutation recovery failure time")?;
        let client = self.connect_client().await?;
        let target = claim.target();
        let updated = client
            .execute(
                "UPDATE durable_fs_mutation_recovery_ledger
                 SET state = 'backing_off',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = to_timestamp($11::bigint::double precision / 1000.0)
                        + ($10::bigint * interval '1 millisecond'),
                     last_error = 'redacted durable FS mutation recovery failure',
                     completed_at = NULL,
                     poisoned_at = NULL,
                     updated_at = to_timestamp($11::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                     AND state = 'active'
                     AND lease_owner = $8
                     AND lease_token = $9
                     AND lease_expires_at > to_timestamp($11::bigint::double precision / 1000.0)",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &backoff_millis,
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("record durable FS mutation recovery failure", error)
            })?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_fs_mutation_recovery_claim())
        }
    }

    async fn poison(
        &self,
        claim: &DurableFsMutationRecoveryClaim,
        _diagnosis: &str,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "durable FS mutation recovery poison time")?;
        let target = claim.target();
        let updated = client
            .execute(
                "UPDATE durable_fs_mutation_recovery_ledger
                 SET state = 'poisoned',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = NULL,
                     last_error = 'redacted durable FS mutation recovery failure',
                     completed_at = NULL,
                     poisoned_at = to_timestamp($10::bigint::double precision / 1000.0),
                     updated_at = to_timestamp($10::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND workspace_scope = $2
                     AND operation_id = $3
                     AND target_ref = $4
                     AND previous_commit_id = $5
                     AND new_commit_id = $6
                     AND failed_step = $7
                     AND state = 'active'
                     AND lease_owner = $8
                     AND lease_token = $9
                     AND lease_expires_at > to_timestamp($10::bigint::double precision / 1000.0)",
                &[
                    &target.repo_id().as_str(),
                    &target.workspace_scope(),
                    &target.operation_id(),
                    &target.target_ref(),
                    &target.previous_commit().to_hex(),
                    &target.new_commit().to_hex(),
                    &target.failed_step().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("poison durable FS mutation recovery", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_fs_mutation_recovery_claim())
        }
    }

    async fn list(&self, limit: usize) -> Result<Vec<DurableFsMutationRecoveryStatus>, VfsError> {
        let limit = usize_to_i64(limit, "durable FS mutation recovery list limit")?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, workspace_scope, operation_id, target_ref,
                    previous_commit_id, new_commit_id, failed_step, state, attempts,
                    lease_expires_at, retry_after, completed_at, poisoned_at, last_error,
                    created_at, updated_at
                 FROM durable_fs_mutation_recovery_ledger
                 ORDER BY
                    CASE state
                        WHEN 'pending' THEN 0
                        WHEN 'backing_off' THEN 1
                        WHEN 'active' THEN 2
                        WHEN 'poisoned' THEN 3
                        ELSE 4
                    END,
                    new_commit_id ASC,
                    CASE failed_step
                        WHEN 'workspace_completion' THEN 0
                        WHEN 'audit_append' THEN 1
                        WHEN 'idempotency_completion' THEN 2
                        ELSE 3
                    END,
                    updated_at DESC,
                    operation_id ASC
                 LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|error| postgres_error("list durable FS mutation recovery", error))?;
        rows.into_iter()
            .map(row_to_fs_mutation_recovery_status)
            .collect()
    }

    async fn has_unresolved_for_ref(
        &self,
        repo_id: &RepoId,
        target_ref: &str,
    ) -> Result<bool, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM durable_fs_mutation_recovery_ledger
                    WHERE repo_id = $1
                        AND target_ref = $2
                        AND state <> 'completed'
                 ) AS present",
                &[&repo_id.as_str(), &target_ref],
            )
            .await
            .map_err(|error| {
                postgres_error("check durable FS mutation recovery unresolved", error)
            })?;
        Ok(row.get("present"))
    }

    async fn list_repair_candidates(
        &self,
        now_millis: u64,
        limit: usize,
    ) -> Result<Vec<DurableFsMutationRecoveryStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = usize_to_i64(limit, "durable FS mutation recovery candidate list limit")?;
        let now_millis = u64_to_i64(
            now_millis,
            "durable FS mutation recovery candidate list time",
        )?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, workspace_scope, operation_id, target_ref,
                    previous_commit_id, new_commit_id, failed_step, state, attempts,
                    lease_expires_at, retry_after, completed_at, poisoned_at, last_error,
                    created_at, updated_at
                 FROM durable_fs_mutation_recovery_ledger
                 WHERE attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($1::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($1::bigint::double precision / 1000.0)
                        )
                     )
                 ORDER BY
                    CASE state
                        WHEN 'pending' THEN 0
                        WHEN 'backing_off' THEN 1
                        WHEN 'active' THEN 2
                        ELSE 3
                    END,
                    new_commit_id ASC,
                    CASE failed_step
                        WHEN 'workspace_completion' THEN 0
                        WHEN 'audit_append' THEN 1
                        WHEN 'idempotency_completion' THEN 2
                        ELSE 3
                    END,
                    updated_at DESC,
                    operation_id ASC
                 LIMIT $2",
                &[&now_millis, &limit],
            )
            .await
            .map_err(|error| {
                postgres_error("list durable FS mutation recovery candidates", error)
            })?;
        rows.into_iter()
            .map(row_to_fs_mutation_recovery_status)
            .collect()
    }

    async fn counts(&self) -> Result<DurableFsMutationRecoveryCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT state, count(*)::bigint AS count
                 FROM durable_fs_mutation_recovery_ledger
                 GROUP BY state",
                &[],
            )
            .await
            .map_err(|error| postgres_error("count durable FS mutation recovery", error))?;
        let mut counts = DurableFsMutationRecoveryCounts::default();
        for row in rows {
            let state = DurableFsMutationRecoveryState::from_str(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "durable FS mutation recovery count")?;
            counts.add(state, count);
        }
        Ok(counts)
    }

    async fn counts_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<DurableFsMutationRecoveryCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT state, count(*)::bigint AS count
                 FROM durable_fs_mutation_recovery_ledger
                 WHERE repo_id = $1
                 GROUP BY state",
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("count repo durable FS mutation recovery", error))?;
        let mut counts = DurableFsMutationRecoveryCounts::default();
        for row in rows {
            let state = DurableFsMutationRecoveryState::from_str(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "repo durable FS mutation recovery count")?;
            counts.add(state, count);
        }
        Ok(counts)
    }
}

#[async_trait]
impl DurableCorePreVisibilityRecoveryStore for PostgresMetadataStore {
    async fn record(&self, record: DurableCorePreVisibilityRecoveryRecord) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        ensure_repo(&client, record.target().repo_id()).await?;
        let context_json = record
            .post_cas_context()
            .map(post_cas_recovery_context_to_json)
            .transpose()?
            .map(Json);
        let occurred_at_millis = u64_to_i64(
            record.occurred_at_millis(),
            "pre-visibility recovery occurrence time",
        )?;
        let expected_ref_version = u64_to_i64(
            record.expected_ref_version().value(),
            "pre-visibility recovery expected ref version",
        )?;
        let object_count = usize_to_i64(
            record.object_count(),
            "pre-visibility recovery object count",
        )?;
        let changed_path_count = usize_to_i64(
            record.changed_path_count(),
            "pre-visibility recovery changed path count",
        )?;
        let updated = client
            .execute(
                "INSERT INTO durable_pre_visibility_recovery_ledger (
                    repo_id, ref_name, commit_id, stage, state, root_tree_id, parent_commit_id,
                    expected_ref_version, object_count, changed_path_count,
                    has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count,
                    context_json, updated_at
                 )
                 VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    'pending',
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    to_timestamp($11::bigint::double precision / 1000.0),
                    to_timestamp($11::bigint::double precision / 1000.0),
                    1,
                    $12,
                    to_timestamp($11::bigint::double precision / 1000.0)
                 )
                 ON CONFLICT (repo_id, ref_name, commit_id, stage) DO UPDATE
                 SET last_seen_at = EXCLUDED.last_seen_at,
                     has_idempotency_reservation =
                        durable_pre_visibility_recovery_ledger.has_idempotency_reservation
                        OR EXCLUDED.has_idempotency_reservation,
                     occurrence_count =
                        durable_pre_visibility_recovery_ledger.occurrence_count + 1,
                     context_json = COALESCE(
                        durable_pre_visibility_recovery_ledger.context_json,
                        EXCLUDED.context_json
                     ),
                     updated_at = EXCLUDED.updated_at
                 WHERE durable_pre_visibility_recovery_ledger.state
                        IN ('pending', 'active', 'backing_off')
                     AND durable_pre_visibility_recovery_ledger.root_tree_id =
                        EXCLUDED.root_tree_id
                     AND durable_pre_visibility_recovery_ledger.parent_commit_id
                        IS NOT DISTINCT FROM EXCLUDED.parent_commit_id
                     AND durable_pre_visibility_recovery_ledger.expected_ref_version =
                        EXCLUDED.expected_ref_version
                     AND durable_pre_visibility_recovery_ledger.object_count =
                        EXCLUDED.object_count
                     AND durable_pre_visibility_recovery_ledger.changed_path_count =
                        EXCLUDED.changed_path_count",
                &[
                    &record.target().repo_id().as_str(),
                    &record.target().ref_name(),
                    &record.target().commit_id().to_hex(),
                    &record.target().stage().as_str(),
                    &record.root_tree_id().to_hex(),
                    &record
                        .parent_commit_id()
                        .map(|commit_id| commit_id.to_hex()),
                    &expected_ref_version,
                    &object_count,
                    &changed_path_count,
                    &record.has_idempotency_reservation(),
                    &occurred_at_millis,
                    &context_json,
                ],
            )
            .await
            .map_err(|error| postgres_error("record pre-visibility recovery", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(VfsError::CorruptStore {
                message: "pre-visibility recovery target has conflicting diagnostics".to_string(),
            })
        }
    }

    async fn claim(
        &self,
        request: DurableCorePreVisibilityRecoveryClaimRequest,
    ) -> Result<Option<DurableCorePreVisibilityRecoveryClaim>, VfsError> {
        let mut client = self.connect_client().await?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin pre-visibility recovery claim", error))?;
        ensure_repo(&transaction, request.target().repo_id()).await?;
        let lease_token = Uuid::new_v4().to_string();
        let lease_duration_millis = duration_to_i64_millis(
            request.lease_duration(),
            "pre-visibility recovery lease duration",
        )?;
        let now_millis = u64_to_i64(request.now_millis(), "pre-visibility recovery claim time")?;

        let candidate = transaction
            .query_opt(
                "SELECT context_json
                 FROM durable_pre_visibility_recovery_ledger
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND stage = $4
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($5::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($5::bigint::double precision / 1000.0)
                        )
                     )
                 FOR UPDATE",
                &[
                    &request.target().repo_id().as_str(),
                    &request.target().ref_name(),
                    &request.target().commit_id().to_hex(),
                    &request.target().stage().as_str(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("lock pre-visibility recovery claim", error))?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let _ = row_to_pre_visibility_recovery_context(&candidate)?;

        let row = transaction
            .query_opt(
                "UPDATE durable_pre_visibility_recovery_ledger
                 SET state = 'active',
                     lease_owner = $5,
                     lease_token = $6,
                     lease_expires_at = to_timestamp($8::bigint::double precision / 1000.0)
                        + ($7::bigint * interval '1 millisecond'),
                     attempts = attempts + 1,
                     retry_after = NULL,
                     last_error = NULL,
                     resolved_at = NULL,
                     poisoned_at = NULL,
                     updated_at = to_timestamp($8::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND stage = $4
                     AND attempts < 4294967295
                     AND (
                        state = 'pending'
                        OR (
                            state = 'active'
                            AND lease_expires_at <= to_timestamp($8::bigint::double precision / 1000.0)
                        )
                        OR (
                            state = 'backing_off'
                            AND retry_after <= to_timestamp($8::bigint::double precision / 1000.0)
                        )
                     )
                 RETURNING repo_id, ref_name, commit_id, stage, state, root_tree_id,
                     parent_commit_id, expected_ref_version, object_count, changed_path_count,
                     has_idempotency_reservation, first_seen_at, last_seen_at,
                     occurrence_count, attempts, lease_owner, lease_token, lease_expires_at,
                     retry_after, last_error, resolved_at, poisoned_at, context_json",
                &[
                    &request.target().repo_id().as_str(),
                    &request.target().ref_name(),
                    &request.target().commit_id().to_hex(),
                    &request.target().stage().as_str(),
                    &request.lease_owner(),
                    &lease_token,
                    &lease_duration_millis,
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("claim pre-visibility recovery", error))?;

        let Some(row) = row else {
            return Ok(None);
        };
        let claim = row_to_pre_visibility_recovery_claim(row)?;
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit pre-visibility recovery claim", error))?;
        Ok(Some(claim))
    }

    async fn resolve(
        &self,
        claim: &DurableCorePreVisibilityRecoveryClaim,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "pre-visibility recovery resolution time")?;
        let updated = client
            .execute(
                "UPDATE durable_pre_visibility_recovery_ledger
                 SET state = 'resolved',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = NULL,
                     last_error = NULL,
                     resolved_at = to_timestamp($7::bigint::double precision / 1000.0),
                     poisoned_at = NULL,
                     updated_at = to_timestamp($7::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND stage = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($7::bigint::double precision / 1000.0)",
                &[
                    &claim.target().repo_id().as_str(),
                    &claim.target().ref_name(),
                    &claim.target().commit_id().to_hex(),
                    &claim.target().stage().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("resolve pre-visibility recovery claim", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_pre_visibility_recovery_claim())
        }
    }

    async fn record_failure(
        &self,
        claim: &DurableCorePreVisibilityRecoveryClaim,
        _diagnosis: &str,
        backoff: std::time::Duration,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        validate_pre_visibility_recovery_backoff(backoff)?;
        let backoff_millis =
            duration_to_i64_millis(backoff, "pre-visibility recovery backoff duration")?;
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "pre-visibility recovery failure time")?;
        let updated = client
            .execute(
                "UPDATE durable_pre_visibility_recovery_ledger
                 SET state = 'backing_off',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = to_timestamp($8::bigint::double precision / 1000.0)
                        + ($7::bigint * interval '1 millisecond'),
                     last_error = 'redacted pre-visibility recovery failure',
                     resolved_at = NULL,
                     poisoned_at = NULL,
                     updated_at = to_timestamp($8::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND stage = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($8::bigint::double precision / 1000.0)",
                &[
                    &claim.target().repo_id().as_str(),
                    &claim.target().ref_name(),
                    &claim.target().commit_id().to_hex(),
                    &claim.target().stage().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &backoff_millis,
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("record pre-visibility recovery failure", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_pre_visibility_recovery_claim())
        }
    }

    async fn poison(
        &self,
        claim: &DurableCorePreVisibilityRecoveryClaim,
        _diagnosis: &str,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        let now_millis = u64_to_i64(now_millis, "pre-visibility recovery poison time")?;
        let updated = client
            .execute(
                "UPDATE durable_pre_visibility_recovery_ledger
                 SET state = 'poisoned',
                     lease_owner = NULL,
                     lease_token = NULL,
                     lease_expires_at = NULL,
                     retry_after = NULL,
                     last_error = 'redacted pre-visibility recovery failure',
                     resolved_at = NULL,
                     poisoned_at = to_timestamp($7::bigint::double precision / 1000.0),
                     updated_at = to_timestamp($7::bigint::double precision / 1000.0)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND stage = $4
                     AND state = 'active'
                     AND lease_owner = $5
                     AND lease_token = $6
                     AND lease_expires_at > to_timestamp($7::bigint::double precision / 1000.0)",
                &[
                    &claim.target().repo_id().as_str(),
                    &claim.target().ref_name(),
                    &claim.target().commit_id().to_hex(),
                    &claim.target().stage().as_str(),
                    &claim.lease_owner(),
                    &claim.token(),
                    &now_millis,
                ],
            )
            .await
            .map_err(|error| postgres_error("poison pre-visibility recovery claim", error))?;
        if updated == 1 {
            Ok(())
        } else {
            Err(stale_pre_visibility_recovery_claim())
        }
    }

    async fn list(
        &self,
        limit: usize,
    ) -> Result<Vec<DurableCorePreVisibilityRecoveryStatus>, VfsError> {
        let limit = usize_to_i64(limit, "pre-visibility recovery list limit")?;
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, ref_name, commit_id, stage, state, root_tree_id,
                    parent_commit_id, expected_ref_version, object_count, changed_path_count,
                    has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count,
                    attempts, lease_expires_at, retry_after, last_error, resolved_at,
                    poisoned_at, context_json
                 FROM durable_pre_visibility_recovery_ledger
                 ORDER BY
                    CASE state
                        WHEN 'pending' THEN 0
                        WHEN 'backing_off' THEN 1
                        WHEN 'active' THEN 2
                        WHEN 'poisoned' THEN 3
                        ELSE 4
                    END,
                    updated_at DESC,
                    commit_id ASC,
                    stage ASC
                 LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|error| postgres_error("list pre-visibility recovery", error))?;
        rows.into_iter()
            .map(|row| row_to_pre_visibility_recovery_status(&row))
            .collect()
    }

    async fn has_unresolved_for_ref(
        &self,
        repo_id: &RepoId,
        ref_name: &str,
    ) -> Result<bool, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM durable_pre_visibility_recovery_ledger
                    WHERE repo_id = $1
                        AND ref_name = $2
                        AND state <> 'resolved'
                 ) AS present",
                &[&repo_id.as_str(), &ref_name],
            )
            .await
            .map_err(|error| postgres_error("check pre-visibility recovery unresolved", error))?;
        Ok(row.get("present"))
    }

    async fn counts(&self) -> Result<DurableCorePreVisibilityRecoveryCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT state, count(*)::bigint AS count
                 FROM durable_pre_visibility_recovery_ledger
                 GROUP BY state",
                &[],
            )
            .await
            .map_err(|error| postgres_error("count pre-visibility recovery", error))?;
        let mut counts = DurableCorePreVisibilityRecoveryCounts::default();
        for row in rows {
            let state = DurableCorePreVisibilityRecoveryState::from_str(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "pre-visibility recovery count")?;
            counts.add(state, count);
        }
        Ok(counts)
    }

    async fn counts_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<DurableCorePreVisibilityRecoveryCounts, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT state, count(*)::bigint AS count
                 FROM durable_pre_visibility_recovery_ledger
                 WHERE repo_id = $1
                 GROUP BY state",
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("count repo pre-visibility recovery", error))?;
        let mut counts = DurableCorePreVisibilityRecoveryCounts::default();
        for row in rows {
            let state = DurableCorePreVisibilityRecoveryState::from_str(row.get("state"))?;
            let count = i64_to_usize(row.get("count"), "repo pre-visibility recovery count")?;
            counts.add(state, count);
        }
        Ok(counts)
    }
}

#[async_trait]
impl CommitStore for PostgresMetadataStore {
    async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
        let timestamp = u64_to_i64(record.timestamp, "commit timestamp")?;
        let client = self.connect_client().await?;
        let mut client = client;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin commit transaction", error))?;
        ensure_repo(&transaction, &record.repo_id).await?;

        let changed_paths = Json(&record.changed_paths);
        let inserted = transaction
            .query_opt(
                "INSERT INTO commits (
                    repo_id,
                    id,
                    root_tree_id,
                    author,
                    message,
                    commit_timestamp_seconds,
                    changed_paths_json
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (repo_id, id) DO NOTHING
                 RETURNING id",
                &[
                    &record.repo_id.as_str(),
                    &record.id.to_hex(),
                    &record.root_tree.to_hex(),
                    &record.author,
                    &record.message,
                    &timestamp,
                    &changed_paths,
                ],
            )
            .await
            .map_err(|error| postgres_error("insert commit", error))?;
        if inserted.is_none() {
            let existing = load_commit(&transaction, &record.repo_id, record.id).await?;
            transaction
                .commit()
                .await
                .map_err(|error| postgres_error("commit duplicate commit transaction", error))?;
            return match existing {
                Some(existing) if existing == record => Ok(existing),
                Some(_) => Err(VfsError::AlreadyExists {
                    path: format!("commit:{}", record.id),
                }),
                None => Err(VfsError::CorruptStore {
                    message: format!(
                        "commit insert for {} conflicted without a visible row",
                        record.id
                    ),
                }),
            };
        }

        for (index, parent) in record.parents.iter().enumerate() {
            let parent_order = usize_to_i32(index, "commit parent order")?;
            transaction
                .execute(
                    "INSERT INTO commit_parents (
                        repo_id,
                        commit_id,
                        parent_commit_id,
                        parent_order
                     )
                     VALUES ($1, $2, $3, $4)",
                    &[
                        &record.repo_id.as_str(),
                        &record.id.to_hex(),
                        &parent.to_hex(),
                        &parent_order,
                    ],
                )
                .await
                .map_err(|error| postgres_error("insert commit parent", error))?;
        }

        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit commit transaction", error))?;
        Ok(record)
    }

    async fn get(&self, repo_id: &RepoId, id: CommitId) -> Result<Option<CommitRecord>, VfsError> {
        let client = self.connect_client().await?;
        load_commit(&client, repo_id, id).await
    }

    async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
        let client = self.connect_client().await?;
        commit_exists(&client, repo_id, id).await
    }

    async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT id
                 FROM commits
                 WHERE repo_id = $1
                 ORDER BY created_at DESC, commit_timestamp_seconds DESC, id DESC",
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("list commits", error))?;

        let mut commits = Vec::with_capacity(rows.len());
        for row in rows {
            let id_hex: String = row.get("id");
            let id = parse_commit_id(&id_hex, "commit id")?;
            if let Some(commit) = load_commit(&client, repo_id, id).await? {
                commits.push(commit);
            }
        }
        Ok(commits)
    }

    async fn list_bounded(
        &self,
        repo_id: &RepoId,
        limit: usize,
    ) -> Result<Vec<CommitRecord>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let client = self.connect_client().await?;
        let limit = usize_to_i64(limit, "commit list limit")?;
        let rows = client
            .query(
                "SELECT id
                 FROM commits
                 WHERE repo_id = $1
                 ORDER BY created_at DESC, commit_timestamp_seconds DESC, id DESC
                 LIMIT $2",
                &[&repo_id.as_str(), &limit],
            )
            .await
            .map_err(|error| postgres_error("list bounded commits", error))?;

        let mut commits = Vec::with_capacity(rows.len());
        for row in rows {
            let id_hex: String = row.get("id");
            let id = parse_commit_id(&id_hex, "commit id")?;
            if let Some(commit) = load_commit(&client, repo_id, id).await? {
                commits.push(commit);
            }
        }
        Ok(commits)
    }
}

#[async_trait]
impl RefStore for PostgresMetadataStore {
    async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                "SELECT repo_id, name, commit_id, version
                 FROM refs
                 WHERE repo_id = $1
                 ORDER BY name ASC",
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("list refs", error))?;

        rows.into_iter().map(row_to_ref_record).collect()
    }

    async fn get(&self, repo_id: &RepoId, name: &RefName) -> Result<Option<RefRecord>, VfsError> {
        let client = self.connect_client().await?;
        load_ref(&client, repo_id, name).await
    }

    async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
        let client = self.connect_client().await?;
        ensure_repo(&client, &update.repo_id).await?;
        apply_ref_update(&client, update).await
    }

    async fn update_source_checked(
        &self,
        update: SourceCheckedRefUpdate,
    ) -> Result<RefRecord, VfsError> {
        if update.target_update.repo_id != update.repo_id {
            return Err(VfsError::InvalidArgs {
                message: "source and target ref updates must use the same repo".to_string(),
            });
        }
        if matches!(update.source_expectation, RefExpectation::MustNotExist) {
            return Err(VfsError::NotSupported {
                message: "Postgres source-checked ref updates require an existing source ref"
                    .to_string(),
            });
        }

        let client = self.connect_client().await?;
        let mut client = client;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin source-checked ref transaction", error))?;
        ensure_repo(&transaction, &update.repo_id).await?;

        let mut names = vec![
            update.source_name.clone(),
            update.target_update.name.clone(),
        ];
        names.sort();
        names.dedup();
        for name in &names {
            transaction
                .query_opt(
                    "SELECT name, commit_id, version
                     FROM refs
                     WHERE repo_id = $1 AND name = $2
                     FOR UPDATE",
                    &[&update.repo_id.as_str(), &name.as_str()],
                )
                .await
                .map_err(|error| postgres_error("lock ref row", error))?;
        }

        check_source_expectation(
            &transaction,
            &update.repo_id,
            &update.source_name,
            update.source_expectation,
        )
        .await?;
        let record = apply_ref_update(&transaction, update.target_update).await?;
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit source-checked ref transaction", error))?;
        Ok(record)
    }
}

async fn load_object_metadata<C>(
    client: &C,
    repo_id: &RepoId,
    id: ObjectId,
) -> Result<Option<ObjectMetadataRecord>, VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT repo_id, kind, object_id, object_key, size_bytes, sha256
             FROM objects
             WHERE repo_id = $1 AND object_id = $2",
            &[&repo_id.as_str(), &id.to_hex()],
        )
        .await
        .map_err(|error| postgres_error("load object metadata", error))?;
    row.map(row_to_object_metadata).transpose()
}

fn row_to_object_metadata(row: Row) -> Result<ObjectMetadataRecord, VfsError> {
    let repo_id: String = row.get("repo_id");
    let object_id: String = row.get("object_id");
    let kind: String = row.get("kind");
    let size: i64 = row.get("size_bytes");
    if size < 0 {
        return Err(VfsError::CorruptStore {
            message: "object metadata has negative size".to_string(),
        });
    }

    Ok(ObjectMetadataRecord {
        repo_id: RepoId::new(repo_id).map_err(corrupt_from_invalid)?,
        id: parse_object_id(&object_id, "object id")?,
        kind: object_kind_from_db(&kind)?,
        object_key: row.get("object_key"),
        size: size as u64,
        sha256: row.get("sha256"),
    })
}

const OBJECT_DELETION_FENCE_LOCK_NAMESPACE: i32 = 0x4f44_464e; // "ODFN"

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PostgresAdvisoryXactLockKey {
    namespace: i32,
    key: i32,
}

impl PostgresAdvisoryXactLockKey {
    pub(crate) const fn new(namespace: i32, key: i32) -> Self {
        Self { namespace, key }
    }

    pub(crate) fn from_subject(namespace: i32, subject: &str) -> Self {
        let digest = Sha256::digest(subject.as_bytes());
        let key = i32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
        Self::new(namespace, key)
    }
}

pub(crate) async fn postgres_try_advisory_xact_lock<C>(
    client: &C,
    key: PostgresAdvisoryXactLockKey,
    context: &'static str,
) -> Result<bool, VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_one(
            "SELECT pg_try_advisory_xact_lock($1, $2)",
            &[&key.namespace, &key.key],
        )
        .await
        .map_err(|error| postgres_error(context, error))?;
    Ok(row.get(0))
}

pub(crate) async fn postgres_advisory_xact_lock<C>(
    client: &C,
    key: PostgresAdvisoryXactLockKey,
    context: &'static str,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    client
        .execute(
            "SELECT pg_advisory_xact_lock($1, $2)",
            &[&key.namespace, &key.key],
        )
        .await
        .map_err(|error| postgres_error(context, error))?;
    Ok(())
}

async fn lock_object_deletion_fence_key<C>(
    client: &C,
    repo_id: &RepoId,
    kind: ObjectKind,
    id: ObjectId,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    let lock_key = format!(
        "{}:{}:{}",
        repo_id.as_str(),
        object_kind_to_db(kind),
        id.to_hex()
    );
    client
        .execute(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            &[&OBJECT_DELETION_FENCE_LOCK_NAMESPACE, &lock_key],
        )
        .await
        .map_err(|error| postgres_error("lock object deletion fence key", error))?;
    Ok(())
}

async fn reject_active_object_deletion_fence<C>(
    client: &C,
    repo_id: &RepoId,
    kind: ObjectKind,
    id: ObjectId,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT 1
             FROM object_deletion_fences
             WHERE repo_id = $1
               AND object_kind = $2
               AND object_id = $3
               AND fence_expires_at > clock_timestamp()",
            &[&repo_id.as_str(), &object_kind_to_db(kind), &id.to_hex()],
        )
        .await
        .map_err(|error| postgres_error("check active object deletion fence", error))?;
    if row.is_some() {
        return Err(VfsError::ObjectWriteConflict {
            message: "active final object metadata deletion fence exists; retry".to_string(),
        });
    }
    Ok(())
}

async fn require_active_object_deletion_fence<C>(
    client: &C,
    fence: &FinalObjectMetadataFence,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT 1
             FROM object_deletion_fences
             WHERE repo_id = $1
               AND object_kind = $2
               AND object_id = $3
               AND canonical_final_key = $4
               AND fence_token = $5
               AND fence_expires_at > clock_timestamp()",
            &[
                &fence.repo_id().as_str(),
                &object_kind_to_db(fence.object_kind()),
                &fence.object_id().to_hex(),
                &fence.canonical_final_key(),
                &fence.token().to_string(),
            ],
        )
        .await
        .map_err(|error| postgres_error("load active object deletion fence", error))?;
    if row.is_none() {
        return Err(VfsError::ObjectWriteConflict {
            message: "final object metadata deletion fence token is stale".to_string(),
        });
    }
    Ok(())
}

fn row_to_final_object_metadata_fence(row: Row) -> Result<FinalObjectMetadataFence, VfsError> {
    let repo_id: String = row.get("repo_id");
    let object_kind: String = row.get("object_kind");
    let object_id: String = row.get("object_id");
    let token: String = row.get("fence_token");
    let metadata_object_key: Option<String> = row.get("metadata_object_key");
    let metadata_size_bytes: Option<i64> = row.get("metadata_size_bytes");
    let metadata_sha256: Option<String> = row.get("metadata_sha256");
    let metadata_identity = match (metadata_object_key, metadata_size_bytes, metadata_sha256) {
        (Some(object_key), Some(size), Some(sha256)) if size >= 0 => Some(
            FinalObjectMetadataIdentity::new(object_key, size as u64, sha256),
        ),
        (None, None, None) => None,
        _ => {
            return Err(VfsError::CorruptStore {
                message: "final object metadata fence has invalid metadata snapshot".to_string(),
            });
        }
    };
    Ok(FinalObjectMetadataFence::for_store(
        RepoId::new(repo_id).map_err(corrupt_from_invalid)?,
        object_kind_from_db(&object_kind)?,
        parse_object_id(&object_id, "object id")?,
        row.get("canonical_final_key"),
        row.get("lease_owner"),
        Uuid::parse_str(&token).map_err(|error| VfsError::CorruptStore {
            message: format!("final object metadata fence has invalid token: {error}"),
        })?,
        row.get::<_, DateTime<Utc>>("fence_expires_at").into(),
        row.get::<_, DateTime<Utc>>("created_at").into(),
        row.get::<_, DateTime<Utc>>("updated_at").into(),
        metadata_identity,
    ))
}

fn validate_metadata_snapshot_for_fence(
    record: &ObjectMetadataRecord,
    fence: &FinalObjectMetadataFence,
) -> Result<(), VfsError> {
    if &record.repo_id != fence.repo_id()
        || record.id != fence.object_id()
        || record.kind != fence.object_kind()
        || record.object_key != fence.canonical_final_key()
    {
        return Err(VfsError::ObjectWriteConflict {
            message: format!(
                "object {} metadata changed while final object deletion was fenced; retry",
                fence.object_id().short_hex()
            ),
        });
    }
    if let Some(identity) = fence.metadata_identity()
        && (record.object_key != identity.object_key()
            || record.size != identity.size()
            || record.sha256 != identity.sha256())
    {
        return Err(VfsError::ObjectWriteConflict {
            message: format!(
                "object {} metadata identity changed while final object deletion was fenced; retry",
                fence.object_id().short_hex()
            ),
        });
    }
    Ok(())
}

async fn reject_cleanup_claim_target_mismatch<C>(
    client: &C,
    request: &ObjectCleanupClaimRequest,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    let Some(row) = client
        .query_opt(
            "SELECT object_kind, object_id
             FROM object_cleanup_claims
             WHERE repo_id = $1 AND claim_kind = $2 AND object_key = $3",
            &[
                &request.repo_id.as_str(),
                &cleanup_claim_kind_to_db(request.claim_kind),
                &request.object_key,
            ],
        )
        .await
        .map_err(|error| postgres_error("load object cleanup claim", error))?
    else {
        return Ok(());
    };
    let object_kind: String = row.get("object_kind");
    let object_id: String = row.get("object_id");
    let existing_kind = object_kind_from_db(&object_kind)?;
    let existing_id = parse_object_id(&object_id, "cleanup claim object id")?;
    if existing_kind != request.object_kind || existing_id != request.object_id {
        return Err(VfsError::CorruptStore {
            message: "cleanup claim target key already exists with different object identity"
                .to_string(),
        });
    }
    Ok(())
}

fn row_to_cleanup_claim(row: Row) -> Result<ObjectCleanupClaim, VfsError> {
    let repo_id: String = row.get("repo_id");
    let claim_kind: String = row.get("claim_kind");
    let object_kind: String = row.get("object_kind");
    let object_id: String = row.get("object_id");
    let object_key: String = row.get("object_key");
    let lease_owner: String = row.get("lease_owner");
    let lease_token: String = row.get("lease_token");
    let attempts: i64 = row.get("attempts");
    if attempts <= 0 {
        return Err(VfsError::CorruptStore {
            message: "cleanup claim has non-positive attempts".to_string(),
        });
    }
    validate_object_key(&object_key).map_err(corrupt_from_invalid)?;
    validate_lease_owner(&lease_owner).map_err(corrupt_from_invalid)?;

    Ok(ObjectCleanupClaim {
        repo_id: RepoId::new(repo_id).map_err(corrupt_from_invalid)?,
        claim_kind: cleanup_claim_kind_from_db(&claim_kind)?,
        object_kind: object_kind_from_db(&object_kind)?,
        object_id: parse_object_id(&object_id, "cleanup claim object id")?,
        object_key,
        lease_owner,
        lease_token: Uuid::parse_str(&lease_token).map_err(|_| VfsError::CorruptStore {
            message: format!("invalid cleanup claim lease token: {lease_token}"),
        })?,
        lease_expires_at: row.get("lease_expires_at"),
        attempts: attempts as u64,
    })
}

fn row_to_cleanup_claim_status(row: Row) -> Result<ObjectCleanupClaimStatus, VfsError> {
    let repo_id: String = row.get("repo_id");
    let claim_kind: String = row.get("claim_kind");
    let object_kind: String = row.get("object_kind");
    let object_id: String = row.get("object_id");
    let object_key: String = row.get("object_key");
    let attempts: i64 = row.get("attempts");
    if attempts <= 0 {
        return Err(VfsError::CorruptStore {
            message: "cleanup claim status has non-positive attempts".to_string(),
        });
    }
    validate_object_key(&object_key).map_err(corrupt_from_invalid)?;

    let completed_at: Option<SystemTime> = row.get("completed_at");
    let lease_expires_at: SystemTime = row.get("lease_expires_at");
    let read_at: SystemTime = row.get("read_at");
    let has_last_failure: bool = row.get("has_last_failure");

    Ok(ObjectCleanupClaimStatus::for_store(
        ObjectCleanupClaimStatusInput {
            repo_id: RepoId::new(repo_id).map_err(corrupt_from_invalid)?,
            claim_kind: cleanup_claim_kind_from_db(&claim_kind)?,
            object_kind: object_kind_from_db(&object_kind)?,
            object_id: parse_object_id(&object_id, "cleanup claim status object id")?,
            object_key,
            state: classify_cleanup_claim(
                completed_at,
                has_last_failure,
                lease_expires_at,
                read_at,
            ),
            is_stale: cleanup_claim_is_stale(completed_at, lease_expires_at, read_at),
            attempts: attempts as u64,
            lease_expires_at,
            completed_at,
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            has_last_failure,
            deletion_snapshot: deletion_snapshot_from_claim_row(&row)?,
            deletion_ready_at: row.get("deletion_ready_at"),
            delete_after: row.get("delete_after"),
            final_object_bytes_deleted_at: row.get("final_object_bytes_deleted_at"),
            final_object_metadata_deleted_at: row.get("final_object_metadata_deleted_at"),
            observed_at: read_at,
        },
    ))
}

fn deletion_snapshot_from_claim_row(
    row: &Row,
) -> Result<Option<FinalObjectDeletionSnapshot>, VfsError> {
    let object_key: Option<String> = row.get("deletion_snapshot_object_key");
    let size: Option<i64> = row.get("deletion_snapshot_size_bytes");
    let sha256: Option<String> = row.get("deletion_snapshot_sha256");
    match (object_key, size, sha256) {
        (None, None, None) => Ok(None),
        (Some(object_key), Some(size), Some(sha256)) => {
            validate_object_key(&object_key).map_err(corrupt_from_invalid)?;
            Ok(Some(FinalObjectDeletionSnapshot {
                object_key,
                size: i64_to_u64(size, "cleanup claim deletion snapshot size")?,
                sha256,
            }))
        }
        _ => Err(VfsError::CorruptStore {
            message: "cleanup claim deletion snapshot is partially populated".to_string(),
        }),
    }
}

fn row_to_post_cas_recovery_claim(row: Row) -> Result<DurableCorePostCasRecoveryClaim, VfsError> {
    let target = row_to_post_cas_recovery_target(&row)?;
    let attempts = i64_to_u32(row.get("attempts"), "post-CAS recovery attempts")?;
    let lease_expires_at: DateTime<Utc> = row.get("lease_expires_at");
    let expires_at_millis =
        datetime_to_millis(lease_expires_at, "post-CAS recovery lease expiration")?;
    let context = row_to_post_cas_recovery_context(&row)?;
    Ok(DurableCorePostCasRecoveryClaim::for_store_with_context(
        target,
        row.get::<_, String>("lease_owner"),
        row.get::<_, String>("lease_token"),
        attempts,
        expires_at_millis,
        context,
    ))
}

fn row_to_post_cas_recovery_context(
    row: &Row,
) -> Result<Option<DurableCorePostCasRecoveryContext>, VfsError> {
    let context_json: Option<Json<serde_json::Value>> = row
        .try_get("context_json")
        .map_err(|_| post_cas_recovery_context_corrupt())?;
    context_json
        .map(|Json(value)| post_cas_recovery_context_from_json(value))
        .transpose()
}

fn row_to_post_cas_recovery_status(row: Row) -> Result<DurableCorePostCasRecoveryStatus, VfsError> {
    let target = row_to_post_cas_recovery_target(&row)?;
    let state = DurableCorePostCasRecoveryState::from_str(row.get("state"))?;
    let attempts = i64_to_u32(row.get("attempts"), "post-CAS recovery attempts")?;
    let lease_expires_at = optional_datetime_to_millis(
        row.get("lease_expires_at"),
        "post-CAS recovery lease expiration",
    )?;
    let retry_after =
        optional_datetime_to_millis(row.get("retry_after"), "post-CAS recovery retry time")?;
    let completed_at =
        optional_datetime_to_millis(row.get("completed_at"), "post-CAS recovery completion time")?;
    let poisoned_at =
        optional_datetime_to_millis(row.get("poisoned_at"), "post-CAS recovery poison time")?;
    let created_at = datetime_to_millis(row.get("created_at"), "post-CAS recovery creation time")?;
    let updated_at = datetime_to_millis(row.get("updated_at"), "post-CAS recovery update time")?;
    let last_error: Option<String> = row.get("last_error");

    Ok(DurableCorePostCasRecoveryStatus::for_store(
        DurableCorePostCasRecoveryStatusInput {
            target,
            state,
            attempts,
            created_at_millis: Some(created_at),
            updated_at_millis: Some(updated_at),
            lease_expires_at_millis: lease_expires_at,
            retry_after_millis: retry_after,
            terminal_at_millis: completed_at.or(poisoned_at),
            has_redacted_diagnosis: last_error.is_some(),
        },
    ))
}

fn row_to_post_cas_recovery_target(
    row: &Row,
) -> Result<DurableCorePostCasRecoveryTarget, VfsError> {
    let repo_id = RepoId::new(row.get::<_, String>("repo_id")).map_err(corrupt_from_invalid)?;
    let commit_id = parse_commit_id(row.get("commit_id"), "post-CAS recovery commit id")?;
    let step = DurableCorePostCasStep::from_str(row.get("step"))?;
    DurableCorePostCasRecoveryTarget::new(repo_id, row.get("ref_name"), commit_id, step)
        .map_err(corrupt_from_invalid)
}

fn row_to_fs_mutation_recovery_claim(row: Row) -> Result<DurableFsMutationRecoveryClaim, VfsError> {
    let target = row_to_fs_mutation_recovery_target(&row)?;
    let attempts = i64_to_u32(row.get("attempts"), "durable FS mutation recovery attempts")?;
    let lease_expires_at: DateTime<Utc> = row.get("lease_expires_at");
    let expires_at_millis = datetime_to_millis(
        lease_expires_at,
        "durable FS mutation recovery lease expiration",
    )?;
    let Json(envelope_json): Json<serde_json::Value> = row.get("envelope_json");
    let envelope = fs_mutation_recovery_envelope_from_json(envelope_json)?;
    Ok(DurableFsMutationRecoveryClaim::for_store(
        target,
        row.get::<_, String>("lease_owner"),
        row.get::<_, String>("lease_token"),
        attempts,
        expires_at_millis,
        envelope,
    ))
}

fn row_to_fs_mutation_recovery_status(
    row: Row,
) -> Result<DurableFsMutationRecoveryStatus, VfsError> {
    let target = row_to_fs_mutation_recovery_target(&row)?;
    let state = DurableFsMutationRecoveryState::from_str(row.get("state"))?;
    let attempts = i64_to_u32(row.get("attempts"), "durable FS mutation recovery attempts")?;
    let lease_expires_at = optional_datetime_to_millis(
        row.get("lease_expires_at"),
        "durable FS mutation recovery lease expiration",
    )?;
    let retry_after = optional_datetime_to_millis(
        row.get("retry_after"),
        "durable FS mutation recovery retry time",
    )?;
    let completed_at = optional_datetime_to_millis(
        row.get("completed_at"),
        "durable FS mutation recovery completion time",
    )?;
    let poisoned_at = optional_datetime_to_millis(
        row.get("poisoned_at"),
        "durable FS mutation recovery poison time",
    )?;
    let created_at = datetime_to_millis(
        row.get("created_at"),
        "durable FS mutation recovery creation time",
    )?;
    let updated_at = datetime_to_millis(
        row.get("updated_at"),
        "durable FS mutation recovery update time",
    )?;
    let last_error: Option<String> = row.get("last_error");

    Ok(DurableFsMutationRecoveryStatus::for_store(
        DurableFsMutationRecoveryStatusInput {
            target,
            state,
            attempts,
            created_at_millis: Some(created_at),
            updated_at_millis: Some(updated_at),
            lease_expires_at_millis: lease_expires_at,
            retry_after_millis: retry_after,
            terminal_at_millis: completed_at.or(poisoned_at),
            has_redacted_diagnosis: last_error.is_some(),
        },
    ))
}

fn row_to_fs_mutation_recovery_target(
    row: &Row,
) -> Result<DurableFsMutationRecoveryTarget, VfsError> {
    let repo_id = RepoId::new(row.get::<_, String>("repo_id")).map_err(corrupt_from_invalid)?;
    let previous_commit = parse_commit_id(
        row.get("previous_commit_id"),
        "durable FS mutation recovery previous commit id",
    )?;
    let new_commit = parse_commit_id(
        row.get("new_commit_id"),
        "durable FS mutation recovery new commit id",
    )?;
    let failed_step = DurableFsMutationRecoveryStep::from_str(row.get("failed_step"))?;
    DurableFsMutationRecoveryTarget::new(
        repo_id,
        row.get::<_, String>("workspace_scope"),
        row.get::<_, String>("operation_id"),
        row.get("target_ref"),
        previous_commit,
        new_commit,
        failed_step,
    )
    .map_err(corrupt_from_invalid)
}

fn row_to_pre_visibility_recovery_claim(
    row: Row,
) -> Result<DurableCorePreVisibilityRecoveryClaim, VfsError> {
    let status = row_to_pre_visibility_recovery_status(&row)?;
    let target = status.target().clone();
    let attempts = i64_to_u32(row.get("attempts"), "pre-visibility recovery attempts")?;
    let lease_expires_at: DateTime<Utc> = row.get("lease_expires_at");
    let expires_at_millis =
        datetime_to_millis(lease_expires_at, "pre-visibility recovery lease expiration")?;
    Ok(DurableCorePreVisibilityRecoveryClaim::for_store(
        target,
        row.get::<_, String>("lease_owner"),
        row.get::<_, String>("lease_token"),
        attempts,
        expires_at_millis,
        status,
    ))
}

fn row_to_pre_visibility_recovery_context(
    row: &Row,
) -> Result<Option<DurableCorePostCasRecoveryContext>, VfsError> {
    let context_json: Option<Json<serde_json::Value>> = row
        .try_get("context_json")
        .map_err(|_| pre_visibility_recovery_context_corrupt())?;
    context_json
        .map(|Json(value)| {
            serde_json::from_value(value).map_err(|_| pre_visibility_recovery_context_corrupt())
        })
        .transpose()
}

fn row_to_pre_visibility_recovery_status(
    row: &Row,
) -> Result<DurableCorePreVisibilityRecoveryStatus, VfsError> {
    let target = row_to_pre_visibility_recovery_target(row)?;
    let state = DurableCorePreVisibilityRecoveryState::from_str(row.get("state"))?;
    let root_tree_id = parse_object_id(
        row.get("root_tree_id"),
        "pre-visibility recovery root tree id",
    )?;
    let parent_commit_id = row
        .get::<_, Option<String>>("parent_commit_id")
        .map(|value| parse_commit_id(&value, "pre-visibility recovery parent commit id"))
        .transpose()?;
    let expected_ref_version = RefVersion::new(i64_to_u64(
        row.get("expected_ref_version"),
        "pre-visibility recovery expected ref version",
    )?)
    .map_err(corrupt_from_invalid)?;
    let object_count = i64_to_usize(
        row.get("object_count"),
        "pre-visibility recovery object count",
    )?;
    let changed_path_count = i64_to_usize(
        row.get("changed_path_count"),
        "pre-visibility recovery changed path count",
    )?;
    let first_seen_at = datetime_to_millis(
        row.get("first_seen_at"),
        "pre-visibility recovery first seen time",
    )?;
    let last_seen_at = datetime_to_millis(
        row.get("last_seen_at"),
        "pre-visibility recovery last seen time",
    )?;
    let occurrence_count = i64_to_u64(
        row.get("occurrence_count"),
        "pre-visibility recovery occurrence count",
    )?;
    let attempts = i64_to_u32(row.get("attempts"), "pre-visibility recovery attempts")?;
    let lease_expires_at = optional_datetime_to_millis(
        row.get("lease_expires_at"),
        "pre-visibility recovery lease expiration",
    )?;
    let retry_after =
        optional_datetime_to_millis(row.get("retry_after"), "pre-visibility recovery retry time")?;
    let resolved_at = optional_datetime_to_millis(
        row.get("resolved_at"),
        "pre-visibility recovery resolution time",
    )?;
    let poisoned_at = optional_datetime_to_millis(
        row.get("poisoned_at"),
        "pre-visibility recovery poison time",
    )?;
    let last_error: Option<String> = row.get("last_error");
    let post_cas_context = row_to_pre_visibility_recovery_context(row)?;

    Ok(DurableCorePreVisibilityRecoveryStatus::for_store(
        DurableCorePreVisibilityRecoveryStatusInput {
            target,
            state,
            root_tree_id,
            parent_commit_id,
            expected_ref_version,
            object_count,
            changed_path_count,
            has_idempotency_reservation: row.get("has_idempotency_reservation"),
            first_seen_at_millis: first_seen_at,
            last_seen_at_millis: last_seen_at,
            occurrence_count,
            attempts,
            lease_expires_at_millis: lease_expires_at,
            retry_after_millis: retry_after,
            terminal_at_millis: resolved_at.or(poisoned_at),
            has_redacted_diagnosis: last_error.is_some(),
            post_cas_context,
        },
    ))
}

fn row_to_pre_visibility_recovery_target(
    row: &Row,
) -> Result<DurableCorePreVisibilityRecoveryTarget, VfsError> {
    let repo_id = RepoId::new(row.get::<_, String>("repo_id")).map_err(corrupt_from_invalid)?;
    let commit_id = parse_commit_id(row.get("commit_id"), "pre-visibility recovery commit id")?;
    let stage = DurableCorePreVisibilityRecoveryStage::from_str(row.get("stage"))?;
    DurableCorePreVisibilityRecoveryTarget::new(repo_id, row.get("ref_name"), commit_id, stage)
        .map_err(corrupt_from_invalid)
}

async fn load_commit<C>(
    client: &C,
    repo_id: &RepoId,
    id: CommitId,
) -> Result<Option<CommitRecord>, VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT repo_id, id, root_tree_id, author, message, commit_timestamp_seconds, changed_paths_json
             FROM commits
             WHERE repo_id = $1 AND id = $2",
            &[&repo_id.as_str(), &id.to_hex()],
        )
        .await
        .map_err(|error| postgres_error("load commit", error))?;
    let Some(row) = row else {
        return Ok(None);
    };

    let repo_id_text: String = row.get("repo_id");
    let id_text: String = row.get("id");
    let root_tree_text: String = row.get("root_tree_id");
    let timestamp: i64 = row.get("commit_timestamp_seconds");
    if timestamp < 0 {
        return Err(VfsError::CorruptStore {
            message: "commit has negative timestamp".to_string(),
        });
    }
    let Json(changed_paths): Json<Vec<ChangedPath>> = row.get("changed_paths_json");

    let parent_rows = client
        .query(
            "SELECT parent_commit_id
             FROM commit_parents
             WHERE repo_id = $1 AND commit_id = $2
             ORDER BY parent_order ASC",
            &[&repo_id.as_str(), &id.to_hex()],
        )
        .await
        .map_err(|error| postgres_error("load commit parents", error))?;
    let parents = parent_rows
        .into_iter()
        .map(|row| {
            let parent_id: String = row.get("parent_commit_id");
            parse_commit_id(&parent_id, "parent commit id")
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(CommitRecord {
        repo_id: RepoId::new(repo_id_text).map_err(corrupt_from_invalid)?,
        id: parse_commit_id(&id_text, "commit id")?,
        root_tree: parse_object_id(&root_tree_text, "root tree id")?,
        parents,
        timestamp: timestamp as u64,
        message: row.get("message"),
        author: row.get("author"),
        changed_paths,
    }))
}

async fn commit_exists<C>(client: &C, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError>
where
    C: GenericClient + Sync,
{
    client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM commits
                WHERE repo_id = $1 AND id = $2
             )",
            &[&repo_id.as_str(), &id.to_hex()],
        )
        .await
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("check commit exists", error))
}

async fn load_ref<C>(
    client: &C,
    repo_id: &RepoId,
    name: &RefName,
) -> Result<Option<RefRecord>, VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT repo_id, name, commit_id, version
             FROM refs
             WHERE repo_id = $1 AND name = $2",
            &[&repo_id.as_str(), &name.as_str()],
        )
        .await
        .map_err(|error| postgres_error("load ref", error))?;
    row.map(row_to_ref_record).transpose()
}

async fn apply_ref_update<C>(client: &C, update: RefUpdate) -> Result<RefRecord, VfsError>
where
    C: GenericClient + Sync,
{
    match update.expectation {
        RefExpectation::MustNotExist => {
            let row = client
                .query_opt(
                    "INSERT INTO refs (repo_id, name, commit_id, version, updated_at)
                     VALUES ($1, $2, $3, 1, now())
                     ON CONFLICT (repo_id, name) DO NOTHING
                     RETURNING repo_id, name, commit_id, version",
                    &[
                        &update.repo_id.as_str(),
                        &update.name.as_str(),
                        &update.target.to_hex(),
                    ],
                )
                .await
                .map_err(|error| postgres_error("create ref", error))?;
            row.map(row_to_ref_record)
                .transpose()?
                .ok_or_else(|| ref_cas_mismatch(&update.name))
        }
        RefExpectation::Matches { target, version } => {
            let version = version_to_i64(version)?;
            let max_version = i64::MAX;
            let row = client
                .query_opt(
                    "UPDATE refs
                     SET commit_id = $4,
                         version = version + 1,
                         updated_at = now()
                     WHERE repo_id = $1
                         AND name = $2
                         AND commit_id = $3
                         AND version = $5
                         AND version < $6
                     RETURNING repo_id, name, commit_id, version",
                    &[
                        &update.repo_id.as_str(),
                        &update.name.as_str(),
                        &target.to_hex(),
                        &update.target.to_hex(),
                        &version,
                        &max_version,
                    ],
                )
                .await
                .map_err(|error| postgres_error("update ref", error))?;
            if let Some(row) = row {
                return row_to_ref_record(row);
            }

            let max_ref_version = RefVersion::new(i64::MAX as u64)?;
            if version == i64::MAX
                && matches!(
                    load_ref(client, &update.repo_id, &update.name).await?,
                    Some(record) if record.target == target && record.version == max_ref_version
                )
            {
                return Err(ref_version_overflow());
            }

            Err(ref_cas_mismatch(&update.name))
        }
    }
}

#[async_trait]
impl IdempotencyStore for PostgresMetadataStore {
    async fn begin(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Result<IdempotencyBegin, VfsError> {
        self.begin_with_policy(
            scope,
            key,
            request_fingerprint,
            IdempotencyQuotaIdentity::for_scope(scope),
            &IdempotencyRetentionPolicy::unlimited(),
        )
        .await
    }

    async fn begin_with_policy(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
        mut quota_identity: IdempotencyQuotaIdentity,
        policy: &IdempotencyRetentionPolicy,
    ) -> Result<IdempotencyBegin, VfsError> {
        normalize_postgres_quota_identity(&mut quota_identity, scope);
        let mut client = self.connect_client().await?;
        let key_hash = key.key_hash();
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("idempotency begin transaction", error))?;

        tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&IDEMPOTENCY_QUOTA_LOCK],
        )
        .await
        .map_err(|error| postgres_error("idempotency begin lock", error))?;

        let existing = tx
            .query_opt(
                r#"SELECT scope, key_hash, request_fingerprint, state, status_code,
                          response_body_json, reserved_at, completed_at,
                          replay_classification, quota_repo_id, quota_workspace_id,
                          quota_principal_uid, secret_replay_envelope_version,
                          secret_replay_key_id, secret_replay_aad_hash,
                          secret_replay_encrypted_at, xmin::text AS reservation_token
                   FROM idempotency_records
                   WHERE scope = $1 AND key_hash = $2
                   FOR UPDATE"#,
                &[&scope, &key_hash],
            )
            .await
            .map_err(|error| postgres_error("idempotency load row", error))?;

        if let Some(row) = existing {
            let begin = classify_existing_idempotency_row(
                &tx,
                &row,
                ExistingIdempotencyBeginContext {
                    scope,
                    key_hash,
                    key,
                    request_fingerprint,
                    quota_identity: &quota_identity,
                    policy,
                },
            )
            .await?;
            tx.commit()
                .await
                .map_err(|error| postgres_error("idempotency begin commit", error))?;
            return Ok(begin);
        }

        enforce_postgres_idempotency_quota(&tx, scope, key_hash, &quota_identity, policy).await?;
        let inserted = tx
            .query_opt(
                r#"INSERT INTO idempotency_records (
                       scope, key_hash, request_fingerprint, state, reserved_at, created_at,
                       quota_repo_id, quota_workspace_id, quota_principal_uid
                   )
                   VALUES ($1, $2, $3, 'pending', clock_timestamp(), clock_timestamp(), $4, $5, $6)
                   ON CONFLICT (scope, key_hash) DO NOTHING
                   RETURNING xmin::text AS reservation_token"#,
                &[
                    &scope,
                    &key_hash,
                    &request_fingerprint,
                    &quota_identity.repo_id,
                    &quota_identity.workspace_id,
                    &principal_uid_to_i64(quota_identity.principal_uid)?,
                ],
            )
            .await
            .map_err(|error| postgres_error("idempotency insert pending", error))?;
        if let Some(row) = inserted {
            let reservation_token: String =
                row.try_get("reservation_token")
                    .map_err(|_| VfsError::CorruptStore {
                        message: "idempotency inserted row missing reservation token".to_string(),
                    })?;
            tx.commit()
                .await
                .map_err(|error| postgres_error("idempotency begin commit", error))?;
            return Ok(IdempotencyBegin::Execute(
                IdempotencyReservation::for_store_with_token(
                    scope,
                    key,
                    request_fingerprint,
                    reservation_token,
                ),
            ));
        }

        let row = tx
            .query_one(
                r#"SELECT scope, key_hash, request_fingerprint, state, status_code,
                          response_body_json, reserved_at, completed_at,
                          replay_classification, quota_repo_id, quota_workspace_id,
                          quota_principal_uid, secret_replay_envelope_version,
                          secret_replay_key_id, secret_replay_aad_hash,
                          secret_replay_encrypted_at, xmin::text AS reservation_token
                   FROM idempotency_records
                   WHERE scope = $1 AND key_hash = $2
                   FOR UPDATE"#,
                &[&scope, &key_hash],
            )
            .await
            .map_err(|error| postgres_error("idempotency load conflict row", error))?;
        let begin = classify_existing_idempotency_row(
            &tx,
            &row,
            ExistingIdempotencyBeginContext {
                scope,
                key_hash,
                key,
                request_fingerprint,
                quota_identity: &quota_identity,
                policy,
            },
        )
        .await?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("idempotency begin commit", error))?;
        Ok(begin)
    }

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete_with_classification(
            reservation,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
        )
        .await
    }

    async fn complete_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        if classification == IdempotencyReplayClassification::SecretBearing {
            return Err(VfsError::InvalidArgs {
                message: "idempotency replay is not persistable".to_string(),
            });
        }
        let classification = replay_classification_to_db(&classification)?;
        let client = self.connect_client().await?;
        let status_i32 = i32::from(status_code);

        let n = client
            .execute(
                r#"UPDATE idempotency_records
                   SET state = 'completed',
                       status_code = $5,
                       response_body_json = $6,
                       replay_classification = $7,
                       completed_at = clock_timestamp()
                   WHERE scope = $1
                     AND key_hash = $2
                     AND request_fingerprint = $3
                     AND xmin::text = $4
                     AND state = 'pending'"#,
                &[
                    &reservation.scope(),
                    &reservation.key_hash(),
                    &reservation.request_fingerprint(),
                    &reservation.reservation_token(),
                    &status_i32,
                    &Json(&response_body),
                    &classification,
                ],
            )
            .await
            .map_err(|error| postgres_error("idempotency complete update", error))?;

        if n == 1 {
            return Ok(());
        }
        Err(idempotency_reservation_not_pending())
    }

    async fn complete_with_encrypted_secret_replay(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        encrypted_envelope_body: serde_json::Value,
        metadata: SecretReplayMetadata,
    ) -> Result<(), VfsError> {
        validate_postgres_secret_replay_metadata_and_body(&metadata, &encrypted_envelope_body)?;
        let classification =
            replay_classification_to_db(&IdempotencyReplayClassification::SecretBearing)?;
        let client = self.connect_client().await?;
        let status_i32 = i32::from(status_code);
        let envelope_version = i32::from(metadata.envelope_version);
        let encrypted_at = u64_to_i64(
            metadata.encrypted_at_unix_seconds,
            "idempotency secret replay encrypted_at",
        )?;

        let n = client
            .execute(
                r#"UPDATE idempotency_records
                   SET state = 'completed',
                       status_code = $5,
                       response_body_json = $6,
                       replay_classification = $7,
                       completed_at = clock_timestamp(),
                       secret_replay_envelope_version = $8,
                       secret_replay_key_id = $9,
                       secret_replay_aad_hash = $10,
                       secret_replay_encrypted_at = to_timestamp($11::bigint::double precision)
                   WHERE scope = $1
                     AND key_hash = $2
                     AND request_fingerprint = $3
                     AND xmin::text = $4
                     AND state = 'pending'"#,
                &[
                    &reservation.scope(),
                    &reservation.key_hash(),
                    &reservation.request_fingerprint(),
                    &reservation.reservation_token(),
                    &status_i32,
                    &Json(&encrypted_envelope_body),
                    &classification,
                    &envelope_version,
                    &metadata.key_id,
                    &metadata.aad_hash,
                    &encrypted_at,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("idempotency complete encrypted secret update", error)
            })?;

        if n == 1 {
            return Ok(());
        }
        Err(idempotency_reservation_not_pending())
    }

    async fn complete_or_match(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete_or_match_with_classification(
            reservation,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
        )
        .await
    }

    async fn complete_or_match_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        if classification == IdempotencyReplayClassification::SecretBearing {
            return Err(VfsError::InvalidArgs {
                message: "idempotency replay is not persistable".to_string(),
            });
        }
        let classification = replay_classification_to_db(&classification)?;
        let client = self.connect_client().await?;
        let status_i32 = i32::from(status_code);

        let n = client
            .execute(
                r#"UPDATE idempotency_records
                   SET state = 'completed',
                       status_code = $5,
                       response_body_json = $6,
                       replay_classification = $7,
                       completed_at = clock_timestamp()
                   WHERE scope = $1
                     AND key_hash = $2
                     AND request_fingerprint = $3
                     AND xmin::text = $4
                     AND state = 'pending'"#,
                &[
                    &reservation.scope(),
                    &reservation.key_hash(),
                    &reservation.request_fingerprint(),
                    &reservation.reservation_token(),
                    &status_i32,
                    &Json(&response_body),
                    &classification,
                ],
            )
            .await
            .map_err(|error| postgres_error("idempotency complete-or-match update", error))?;

        if n == 1 {
            return Ok(());
        }

        let row = client
            .query_opt(
                r#"SELECT state, request_fingerprint, status_code, response_body_json,
                          replay_classification, secret_replay_envelope_version,
                          secret_replay_key_id, secret_replay_aad_hash,
                          secret_replay_encrypted_at
                   FROM idempotency_records
                   WHERE scope = $1 AND key_hash = $2"#,
                &[&reservation.scope(), &reservation.key_hash()],
            )
            .await
            .map_err(|error| postgres_error("idempotency complete-or-match load", error))?;

        let Some(row) = row else {
            return Err(idempotency_reservation_not_pending());
        };
        let state: String = row.try_get("state").map_err(|_| VfsError::CorruptStore {
            message: "idempotency row missing state".to_string(),
        })?;
        if state != "completed" {
            return Err(idempotency_reservation_not_pending());
        }

        let stored_fingerprint: String =
            row.try_get("request_fingerprint")
                .map_err(|_| VfsError::CorruptStore {
                    message: "idempotency row missing fingerprint".to_string(),
                })?;
        let stored_status: Option<i32> =
            row.try_get("status_code")
                .map_err(|_| VfsError::CorruptStore {
                    message: "idempotency completed row corrupt".to_string(),
                })?;
        let stored_body: Option<Json<serde_json::Value>> = row
            .try_get("response_body_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;
        let stored_classification: Option<String> =
            row.try_get("replay_classification")
                .map_err(|_| VfsError::CorruptStore {
                    message: "idempotency completed row corrupt".to_string(),
                })?;
        if stored_fingerprint == reservation.request_fingerprint()
            && stored_status == Some(status_i32)
            && stored_body.is_some_and(|Json(body)| body == response_body)
            && stored_classification.as_deref() == Some(classification.as_str())
        {
            return Ok(());
        }

        Err(idempotency_completed_replay_mismatch())
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        match self.abort_idempotency_reservation_inner(reservation).await {
            Ok(()) => {}
            Err(_) => tracing::debug!("postgres idempotency abort skipped"),
        }
    }

    async fn sweep_retention(
        &self,
        request: IdempotencySweepRequest,
    ) -> Result<IdempotencySweepSummary, VfsError> {
        if request.limit == 0 {
            return Ok(IdempotencySweepSummary::default());
        }
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("idempotency sweep transaction", error))?;
        tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&IDEMPOTENCY_QUOTA_LOCK],
        )
        .await
        .map_err(|error| postgres_error("idempotency sweep lock", error))?;

        let mut summary = IdempotencySweepSummary::default();
        let stale_cutoff = request
            .now_unix_seconds
            .saturating_sub(request.policy.pending_stale_after_seconds)
            as f64;
        let completed_cutoff = request
            .now_unix_seconds
            .saturating_sub(request.policy.completed_ttl_seconds)
            as f64;
        let retain_scopes = request
            .retain_keys
            .iter()
            .map(|(scope, _)| scope.clone())
            .collect::<Vec<_>>();
        let retain_key_hashes = request
            .retain_keys
            .iter()
            .map(|(_, key_hash)| key_hash.clone())
            .collect::<Vec<_>>();
        let _ = u64_to_i64(
            request
                .now_unix_seconds
                .saturating_sub(request.policy.pending_stale_after_seconds),
            "idempotency stale cutoff",
        )?;
        let _ = u64_to_i64(
            request
                .now_unix_seconds
                .saturating_sub(request.policy.completed_ttl_seconds),
            "idempotency completed cutoff",
        )?;
        let retain_commit_ids_set = request
            .retain_commit_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<String>>();
        let local_repo = request.repo_id.as_ref() == Some(&RepoId::local());
        let repo_scope_prefix = request
            .repo_id
            .as_ref()
            .filter(|repo_id| repo_id != &&RepoId::local())
            .map(idempotency_repo_scope_prefix);

        let pending_limit = if request.block_completed_when_pending {
            request.limit.min(1_000)
        } else {
            request.limit
        };
        let pending_limit = usize_to_i64(pending_limit, "idempotency sweep pending limit")?;
        let pending_rows = tx
            .query(
                r#"SELECT scope, key_hash, reserved_at
                   FROM idempotency_records
                   WHERE (($2::text IS NULL AND NOT $3::boolean)
                          OR ($2::text IS NOT NULL AND left(scope, length($2)) = $2)
                          OR ($3::boolean AND left(scope, 5) <> 'repo:'))
                     AND state = 'pending'
                     AND reserved_at < to_timestamp($4::double precision)
                     AND NOT EXISTS (
                         SELECT 1
                         FROM unnest($5::text[], $6::text[]) AS retained(scope, key_hash)
                         WHERE retained.scope = idempotency_records.scope
                           AND retained.key_hash = idempotency_records.key_hash
                     )
                   ORDER BY reserved_at ASC, scope ASC, key_hash ASC
                   LIMIT $1
                   FOR UPDATE"#,
                &[
                    &pending_limit,
                    &repo_scope_prefix,
                    &local_repo,
                    &stale_cutoff,
                    &retain_scopes,
                    &retain_key_hashes,
                ],
            )
            .await
            .map_err(|error| postgres_error("idempotency sweep pending candidates", error))?;

        let pending_scan_limit_reached =
            request.block_completed_when_pending && pending_rows.len() >= pending_limit as usize;
        for row in pending_rows {
            summary.scanned += 1;
            let scope: String = row.get("scope");
            let key_hash: String = row.get("key_hash");

            summary.stale_pending += 1;
            if request.abort_stale_pending {
                let n = tx
                    .execute(
                        "DELETE FROM idempotency_records WHERE scope = $1 AND key_hash = $2 AND state = 'pending'",
                        &[&scope, &key_hash],
                    )
                    .await
                    .map_err(|error| postgres_error("idempotency sweep pending delete", error))?;
                if n == 1 {
                    summary.aborted_pending += 1;
                }
            }
        }

        if request.block_completed_when_pending {
            let row = tx
                .query_one(
                    r#"SELECT EXISTS (
                           SELECT 1
                           FROM idempotency_records
                           WHERE (($1::text IS NULL AND NOT $2::boolean)
                                  OR ($1::text IS NOT NULL AND left(scope, length($1)) = $1)
                                  OR ($2::boolean AND left(scope, 5) <> 'repo:'))
                             AND state = 'pending'
                       ) AS pending_exists"#,
                    &[&repo_scope_prefix, &local_repo],
                )
                .await
                .map_err(|error| postgres_error("idempotency sweep pending blocker", error))?;
            let pending_exists: bool = row.get("pending_exists");
            if pending_exists || pending_scan_limit_reached {
                summary.retained_for_roots += 1;
                increment_sweep_reason(&mut summary, "pending_repo_record");
                let remaining = tx
                    .query_one("SELECT count(*) AS count FROM idempotency_records", &[])
                    .await
                    .map_err(|error| postgres_error("idempotency sweep remaining", error))?;
                summary.remaining =
                    i64_to_usize(remaining.get("count"), "idempotency remaining count")?;
                tx.commit()
                    .await
                    .map_err(|error| postgres_error("idempotency sweep commit", error))?;
                return Ok(summary);
            }
        }

        let remaining_limit = request.limit.saturating_sub(summary.scanned);
        if remaining_limit > 0 {
            let remaining_limit =
                usize_to_i64(remaining_limit, "idempotency sweep remaining limit")?;
            let completed_rows = tx
                .query(
                    r#"SELECT scope, key_hash, response_body_json, completed_at
                       FROM idempotency_records
                       WHERE (($2::text IS NULL AND NOT $3::boolean)
                              OR ($2::text IS NOT NULL AND left(scope, length($2)) = $2)
                              OR ($3::boolean AND left(scope, 5) <> 'repo:'))
                         AND state = 'completed'
                         AND completed_at < to_timestamp($4::double precision)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM unnest($5::text[], $6::text[]) AS retained(scope, key_hash)
                             WHERE retained.scope = idempotency_records.scope
                               AND retained.key_hash = idempotency_records.key_hash
                         )
                       ORDER BY COALESCE(retention_deferred_at, completed_at) ASC,
                                scope ASC,
                                key_hash ASC
                       LIMIT $1
                       FOR UPDATE"#,
                    &[
                        &remaining_limit,
                        &repo_scope_prefix,
                        &local_repo,
                        &completed_cutoff,
                        &retain_scopes,
                        &retain_key_hashes,
                    ],
                )
                .await
                .map_err(|error| postgres_error("idempotency sweep completed candidates", error))?;

            for row in completed_rows {
                summary.scanned += 1;
                let scope: String = row.get("scope");
                let key_hash: String = row.get("key_hash");

                if !retain_commit_ids_set.is_empty() {
                    let body: Option<Json<serde_json::Value>> =
                        row.try_get("response_body_json")
                            .map_err(|_| VfsError::CorruptStore {
                                message: "idempotency completed row corrupt".to_string(),
                            })?;
                    let Some(Json(body)) = body else {
                        return Err(VfsError::CorruptStore {
                            message: "idempotency completed row missing replay fields".to_string(),
                        });
                    };
                    let retained =
                        RetainedIdempotencyRecord::completed_response(scope.clone(), 200, body);
                    if retained.commit_roots_truncated {
                        summary.retained_for_roots += 1;
                        increment_sweep_reason(&mut summary, "scan_limit_reached");
                        defer_retained_idempotency_completed_row(
                            &tx,
                            &scope,
                            &key_hash,
                            completed_cutoff,
                        )
                        .await?;
                        continue;
                    }
                    if retained
                        .commit_roots
                        .iter()
                        .any(|root| retain_commit_ids_set.contains(root))
                    {
                        summary.retained_for_roots += 1;
                        increment_sweep_reason(&mut summary, "commit_root");
                        defer_retained_idempotency_completed_row(
                            &tx,
                            &scope,
                            &key_hash,
                            completed_cutoff,
                        )
                        .await?;
                        continue;
                    }
                }

                let n = tx
                    .execute(
                        "DELETE FROM idempotency_records WHERE scope = $1 AND key_hash = $2 AND state = 'completed'",
                        &[&scope, &key_hash],
                    )
                    .await
                    .map_err(|error| postgres_error("idempotency sweep completed delete", error))?;
                if n == 1 {
                    summary.swept_completed += 1;
                }
            }
        }

        let remaining = tx
            .query_one("SELECT count(*) AS count FROM idempotency_records", &[])
            .await
            .map_err(|error| postgres_error("idempotency sweep remaining", error))?;
        summary.remaining = i64_to_usize(remaining.get("count"), "idempotency remaining count")?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("idempotency sweep commit", error))?;
        Ok(summary)
    }

    async fn list_retained_for_repo(
        &self,
        repo_id: &RepoId,
        limit: usize,
    ) -> Result<Vec<RetainedIdempotencyRecord>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let client = self.connect_client().await?;
        let limit = usize_to_i64(limit, "idempotency retained record list limit")?;
        let rows = if repo_id == &RepoId::local() {
            client
                .query(
                    r#"SELECT scope, state, status_code, response_body_json,
                              replay_classification, reserved_at, completed_at,
                              request_fingerprint, quota_repo_id, quota_workspace_id,
                              quota_principal_uid, secret_replay_envelope_version,
                              secret_replay_key_id, secret_replay_aad_hash,
                              secret_replay_encrypted_at
                       FROM idempotency_records
                       WHERE left(scope, 5) <> 'repo:'
                       ORDER BY CASE WHEN state = 'pending' THEN 0 ELSE 1 END,
                                created_at ASC,
                                scope ASC,
                                key_hash ASC
                       LIMIT $1"#,
                    &[&limit],
                )
                .await
                .map_err(|error| postgres_error("idempotency retained local list", error))?
        } else {
            let repo_scope_prefix = idempotency_repo_scope_prefix(repo_id);
            client
                .query(
                    r#"SELECT scope, state, status_code, response_body_json,
                              replay_classification, reserved_at, completed_at,
                              request_fingerprint, quota_repo_id, quota_workspace_id,
                              quota_principal_uid, secret_replay_envelope_version,
                              secret_replay_key_id, secret_replay_aad_hash,
                              secret_replay_encrypted_at
                       FROM idempotency_records
                       WHERE quota_repo_id = $1
                          OR left(scope, length($2)) = $2
                       ORDER BY CASE WHEN state = 'pending' THEN 0 ELSE 1 END,
                                created_at ASC,
                                scope ASC,
                                key_hash ASC
                       LIMIT $3"#,
                    &[&repo_id.as_str(), &repo_scope_prefix, &limit],
                )
                .await
                .map_err(|error| postgres_error("idempotency retained repo list", error))?
        };

        rows.into_iter()
            .map(retained_idempotency_record_from_row)
            .collect()
    }
}

impl PostgresMetadataStore {
    async fn abort_idempotency_reservation_inner(
        &self,
        reservation: &IdempotencyReservation,
    ) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        client
            .execute(
                r#"DELETE FROM idempotency_records
                   WHERE scope = $1
                     AND key_hash = $2
                     AND request_fingerprint = $3
                     AND xmin::text = $4
                     AND state = 'pending'"#,
                &[
                    &reservation.scope(),
                    &reservation.key_hash(),
                    &reservation.request_fingerprint(),
                    &reservation.reservation_token(),
                ],
            )
            .await
            .map_err(|error| postgres_error("idempotency abort delete", error))?;
        Ok(())
    }
}

fn retained_idempotency_record_from_row(row: Row) -> Result<RetainedIdempotencyRecord, VfsError> {
    let scope: String = row.try_get("scope").map_err(|_| VfsError::CorruptStore {
        message: "idempotency retained row missing scope".to_string(),
    })?;
    let state: String = row.try_get("state").map_err(|_| VfsError::CorruptStore {
        message: "idempotency retained row missing state".to_string(),
    })?;
    match state.as_str() {
        "pending" => {
            let reserved_at: DateTime<Utc> =
                row.try_get("reserved_at")
                    .map_err(|_| VfsError::CorruptStore {
                        message: "idempotency retained pending row corrupt".to_string(),
                    })?;
            Ok(RetainedIdempotencyRecord::pending_with_reserved_at(
                scope,
                datetime_to_unix_seconds(reserved_at, "idempotency reserved_at")?,
            ))
        }
        "completed" => {
            let request_fingerprint: String =
                row.try_get("request_fingerprint")
                    .map_err(|_| VfsError::CorruptStore {
                        message: "idempotency retained completed row corrupt".to_string(),
                    })?;
            Ok(RetainedIdempotencyRecord::completed_record(
                scope,
                &idempotency_record_from_row(&row, request_fingerprint)?,
            ))
        }
        other => Err(VfsError::CorruptStore {
            message: format!("unknown idempotency retained row state {other:?}"),
        }),
    }
}

const IDEMPOTENCY_QUOTA_LOCK: i64 = 0x5354_524d_4944_454d; // "STRMIDEM"

fn idempotency_repo_scope_prefix(repo_id: &RepoId) -> String {
    format!("repo:{}:", repo_id.as_str())
}

async fn defer_retained_idempotency_completed_row<C>(
    client: &C,
    scope: &str,
    key_hash: &str,
    defer_until_unix_seconds: f64,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    client
        .execute(
            r#"UPDATE idempotency_records
               SET retention_deferred_at = to_timestamp($3::double precision)
               WHERE scope = $1
                 AND key_hash = $2
                 AND state = 'completed'"#,
            &[&scope, &key_hash, &defer_until_unix_seconds],
        )
        .await
        .map_err(|error| postgres_error("idempotency sweep retained defer", error))?;
    Ok(())
}

struct ExistingIdempotencyBeginContext<'a> {
    scope: &'a str,
    key_hash: &'a str,
    key: &'a IdempotencyKey,
    request_fingerprint: &'a str,
    quota_identity: &'a IdempotencyQuotaIdentity,
    policy: &'a IdempotencyRetentionPolicy,
}

async fn classify_existing_idempotency_row<C>(
    client: &C,
    row: &Row,
    context: ExistingIdempotencyBeginContext<'_>,
) -> Result<IdempotencyBegin, VfsError>
where
    C: GenericClient + Sync,
{
    let state: String = row.try_get("state").map_err(|_| VfsError::CorruptStore {
        message: "idempotency row missing state".to_string(),
    })?;
    let stored_fp: String =
        row.try_get("request_fingerprint")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency row missing fingerprint".to_string(),
            })?;

    match state.as_str() {
        "completed" => {
            if stored_fp == context.request_fingerprint {
                Ok(IdempotencyBegin::Replay(idempotency_record_from_row(
                    row, stored_fp,
                )?))
            } else {
                Ok(IdempotencyBegin::Conflict)
            }
        }
        "pending" => {
            let reserved_at: DateTime<Utc> =
                row.try_get("reserved_at")
                    .map_err(|_| VfsError::CorruptStore {
                        message: "idempotency pending row corrupt".to_string(),
                    })?;
            let now = client
                .query_one(
                    "SELECT floor(extract(epoch from clock_timestamp()))::bigint AS now",
                    &[],
                )
                .await
                .map_err(|error| postgres_error("idempotency begin clock", error))?;
            let now_unix = i64_to_u64(now.get("now"), "idempotency begin clock")?;
            let reserved_at_unix =
                datetime_to_unix_seconds(reserved_at, "idempotency reserved_at")?;
            let stale = now_unix.saturating_sub(reserved_at_unix)
                > context.policy.pending_stale_after_seconds;
            if !stale {
                return if stored_fp == context.request_fingerprint {
                    Ok(IdempotencyBegin::InProgress)
                } else {
                    Ok(IdempotencyBegin::Conflict)
                };
            }
            if stored_fp != context.request_fingerprint {
                client
                    .execute(
                        "DELETE FROM idempotency_records WHERE scope = $1 AND key_hash = $2 AND state = 'pending'",
                        &[&context.scope, &context.key_hash],
                    )
                    .await
                    .map_err(|error| postgres_error("idempotency stale pending delete", error))?;
                return Ok(IdempotencyBegin::Conflict);
            }
            enforce_postgres_idempotency_quota(
                client,
                context.scope,
                context.key_hash,
                context.quota_identity,
                context.policy,
            )
            .await?;
            let row = client
                .query_one(
                    r#"UPDATE idempotency_records
                       SET reserved_at = clock_timestamp(),
                           quota_repo_id = $3,
                           quota_workspace_id = $4,
                           quota_principal_uid = $5
                       WHERE scope = $1
                         AND key_hash = $2
                         AND state = 'pending'
	                       RETURNING xmin::text AS reservation_token"#,
                    &[
                        &context.scope,
                        &context.key_hash,
                        &context.quota_identity.repo_id,
                        &context.quota_identity.workspace_id,
                        &principal_uid_to_i64(context.quota_identity.principal_uid)?,
                    ],
                )
                .await
                .map_err(|error| postgres_error("idempotency stale pending takeover", error))?;
            let reservation_token: String = row.get("reservation_token");
            Ok(IdempotencyBegin::Execute(
                IdempotencyReservation::for_store_with_token(
                    context.scope,
                    context.key,
                    context.request_fingerprint,
                    reservation_token,
                ),
            ))
        }
        other => Err(VfsError::CorruptStore {
            message: format!("unknown idempotency state {other:?}"),
        }),
    }
}

async fn enforce_postgres_idempotency_quota<C>(
    client: &C,
    scope: &str,
    key_hash: &str,
    identity: &IdempotencyQuotaIdentity,
    policy: &IdempotencyRetentionPolicy,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    if quota_exceeded_db(
        client,
        policy.max_records_per_scope,
        "scope = $3",
        scope,
        key_hash,
        &identity.scope,
    )
    .await?
    {
        return Err(idempotency_quota_exceeded());
    }
    if let Some(repo_id) = &identity.repo_id
        && quota_exceeded_db(
            client,
            policy.max_records_per_repo,
            "quota_repo_id = $3",
            scope,
            key_hash,
            repo_id,
        )
        .await?
    {
        return Err(idempotency_quota_exceeded());
    }
    if let Some(workspace_id) = &identity.workspace_id
        && quota_exceeded_db(
            client,
            policy.max_records_per_workspace,
            "quota_workspace_id = $3",
            scope,
            key_hash,
            workspace_id,
        )
        .await?
    {
        return Err(idempotency_quota_exceeded());
    }
    if let Some(principal_uid) = identity.principal_uid
        && policy.max_records_per_principal.is_some()
    {
        let principal_uid = principal_uid_to_i64(Some(principal_uid))?;
        let count = client
            .query_one(
                r#"SELECT count(*) AS count
                   FROM idempotency_records
                   WHERE NOT (scope = $1 AND key_hash = $2)
                     AND quota_principal_uid = $3"#,
                &[&scope, &key_hash, &principal_uid],
            )
            .await
            .map_err(|error| postgres_error("idempotency quota count", error))?;
        let count = i64_to_usize(count.get("count"), "idempotency quota count")?;
        if policy
            .max_records_per_principal
            .is_some_and(|limit| count >= limit)
        {
            return Err(idempotency_quota_exceeded());
        }
    }
    Ok(())
}

async fn quota_exceeded_db<C>(
    client: &C,
    limit: Option<usize>,
    predicate_sql: &str,
    scope: &str,
    key_hash: &str,
    value: &str,
) -> Result<bool, VfsError>
where
    C: GenericClient + Sync,
{
    let Some(limit) = limit else {
        return Ok(false);
    };
    let sql = format!(
        "SELECT count(*) AS count FROM idempotency_records WHERE NOT (scope = $1 AND key_hash = $2) AND {predicate_sql}"
    );
    let row = client
        .query_one(&sql, &[&scope, &key_hash, &value])
        .await
        .map_err(|error| postgres_error("idempotency quota count", error))?;
    let count = i64_to_usize(row.get("count"), "idempotency quota count")?;
    Ok(count >= limit)
}

fn normalize_postgres_quota_identity(identity: &mut IdempotencyQuotaIdentity, scope: &str) {
    identity.scope = scope.to_string();
    let parsed = IdempotencyQuotaIdentity::for_scope(scope);
    identity.repo_id = parsed.repo_id.or_else(|| identity.repo_id.take());
    identity.workspace_id = parsed.workspace_id.or_else(|| identity.workspace_id.take());
}

fn principal_uid_to_i64(value: Option<u64>) -> Result<Option<i64>, VfsError> {
    value
        .map(|value| u64_to_i64(value, "idempotency principal uid"))
        .transpose()
}

fn replay_classification_to_db(
    classification: &IdempotencyReplayClassification,
) -> Result<String, VfsError> {
    match classification {
        IdempotencyReplayClassification::SecretFree => Ok("secret_free".to_string()),
        IdempotencyReplayClassification::Partial => Ok("partial".to_string()),
        IdempotencyReplayClassification::SecretBearing => Ok("secret_bearing".to_string()),
    }
}

fn replay_classification_from_db(
    classification: Option<String>,
) -> Result<IdempotencyReplayClassification, VfsError> {
    match classification.as_deref() {
        Some("secret_free") => Ok(IdempotencyReplayClassification::SecretFree),
        Some("partial") => Ok(IdempotencyReplayClassification::Partial),
        Some("secret_bearing") => Ok(IdempotencyReplayClassification::SecretBearing),
        Some(_) => Err(VfsError::CorruptStore {
            message: "idempotency replay classification is invalid".to_string(),
        }),
        None => Err(VfsError::CorruptStore {
            message: "idempotency completed row missing replay fields".to_string(),
        }),
    }
}

fn validate_postgres_secret_replay_metadata_and_body(
    metadata: &SecretReplayMetadata,
    body: &serde_json::Value,
) -> Result<(), VfsError> {
    if metadata.envelope_version == 0
        || metadata.key_id.trim().is_empty()
        || metadata.key_id.len() > 255
        || !is_lower_hex_sha256(&metadata.aad_hash)
    {
        return Err(VfsError::InvalidArgs {
            message: "idempotency secret replay metadata is invalid".to_string(),
        });
    }
    u64_to_i64(
        metadata.encrypted_at_unix_seconds,
        "idempotency secret replay encrypted_at",
    )?;

    let Some(object) = body.as_object() else {
        return Err(VfsError::InvalidArgs {
            message: "idempotency secret replay envelope is invalid".to_string(),
        });
    };
    let version = object
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u16::try_from(value).ok());
    let key_id = object.get("key_id").and_then(serde_json::Value::as_str);
    let nonce = object.get("nonce_b64").and_then(serde_json::Value::as_str);
    let ciphertext = object
        .get("ciphertext_b64")
        .and_then(serde_json::Value::as_str);
    let aad_hash = object.get("aad_hash").and_then(serde_json::Value::as_str);
    let encrypted_at = object
        .get("encrypted_at_unix_seconds")
        .and_then(serde_json::Value::as_u64);

    if object.len() != 6
        || version != Some(metadata.envelope_version)
        || key_id != Some(metadata.key_id.as_str())
        || nonce.is_none_or(|value| value.is_empty())
        || ciphertext.is_none_or(|value| value.is_empty())
        || aad_hash != Some(metadata.aad_hash.as_str())
        || encrypted_at != Some(metadata.encrypted_at_unix_seconds)
    {
        return Err(VfsError::InvalidArgs {
            message: "idempotency secret replay envelope is invalid".to_string(),
        });
    }
    Ok(())
}

fn idempotency_record_from_row(
    row: &Row,
    request_fingerprint: String,
) -> Result<IdempotencyRecord, VfsError> {
    let status: Option<i32> = row
        .try_get("status_code")
        .map_err(|_| VfsError::CorruptStore {
            message: "idempotency completed row corrupt".to_string(),
        })?;
    let body: Option<Json<serde_json::Value>> =
        row.try_get("response_body_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;
    let completed_at: Option<DateTime<Utc>> =
        row.try_get("completed_at")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;
    let (Some(status), Some(Json(body)), Some(completed_at)) = (status, body, completed_at) else {
        return Err(VfsError::CorruptStore {
            message: "idempotency completed row missing replay fields".to_string(),
        });
    };
    let status_code = u16::try_from(status).map_err(|_| VfsError::CorruptStore {
        message: "idempotency status code is outside supported range".to_string(),
    })?;
    let classification = replay_classification_from_db(row.get("replay_classification"))?;
    let secret_replay = secret_replay_metadata_from_row(row, &classification, &body)?;
    Ok(IdempotencyRecord::for_store_with_policy_and_secret_replay(
        request_fingerprint,
        status_code,
        body,
        classification,
        datetime_to_unix_seconds(completed_at, "idempotency completed_at")?,
        quota_identity_from_row(row)?,
        secret_replay,
    ))
}

fn secret_replay_metadata_from_row(
    row: &Row,
    classification: &IdempotencyReplayClassification,
    body: &serde_json::Value,
) -> Result<Option<SecretReplayMetadata>, VfsError> {
    let envelope_version: Option<i32> =
        row.try_get("secret_replay_envelope_version")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;
    let key_id: Option<String> =
        row.try_get("secret_replay_key_id")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;
    let aad_hash: Option<String> =
        row.try_get("secret_replay_aad_hash")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;
    let encrypted_at: Option<DateTime<Utc>> =
        row.try_get("secret_replay_encrypted_at")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency completed row corrupt".to_string(),
            })?;

    match classification {
        IdempotencyReplayClassification::SecretBearing => {
            let (Some(envelope_version), Some(key_id), Some(aad_hash), Some(encrypted_at)) =
                (envelope_version, key_id, aad_hash, encrypted_at)
            else {
                return Err(VfsError::CorruptStore {
                    message: "idempotency completed row missing replay fields".to_string(),
                });
            };
            let metadata = SecretReplayMetadata {
                envelope_version: u16::try_from(envelope_version).map_err(|_| {
                    VfsError::CorruptStore {
                        message: "idempotency completed row corrupt".to_string(),
                    }
                })?,
                key_id,
                aad_hash,
                encrypted_at_unix_seconds: datetime_to_unix_seconds(
                    encrypted_at,
                    "idempotency secret replay encrypted_at",
                )?,
            };
            validate_postgres_secret_replay_metadata_and_body(&metadata, body).map_err(|_| {
                VfsError::CorruptStore {
                    message: "idempotency completed row corrupt".to_string(),
                }
            })?;
            Ok(Some(metadata))
        }
        _ => {
            if envelope_version.is_some()
                || key_id.is_some()
                || aad_hash.is_some()
                || encrypted_at.is_some()
            {
                return Err(VfsError::CorruptStore {
                    message: "idempotency completed row corrupt".to_string(),
                });
            }
            Ok(None)
        }
    }
}

fn quota_identity_from_row(row: &Row) -> Result<IdempotencyQuotaIdentity, VfsError> {
    let scope: String = row.try_get("scope").map_err(|_| VfsError::CorruptStore {
        message: "idempotency row missing scope".to_string(),
    })?;
    let principal_uid: Option<i64> =
        row.try_get("quota_principal_uid")
            .map_err(|_| VfsError::CorruptStore {
                message: "idempotency quota identity corrupt".to_string(),
            })?;
    let mut identity = IdempotencyQuotaIdentity::for_scope(&scope);
    if let Some(repo_id) = row.try_get("quota_repo_id").ok().flatten() {
        identity.repo_id = Some(repo_id);
    }
    if let Some(workspace_id) = row.try_get("quota_workspace_id").ok().flatten() {
        identity.workspace_id = Some(workspace_id);
    }
    identity.principal_uid = principal_uid
        .map(|value| i64_to_u64(value, "idempotency principal uid"))
        .transpose()?;
    Ok(identity)
}

fn idempotency_reservation_not_pending() -> VfsError {
    VfsError::InvalidArgs {
        message: "idempotency reservation is not pending".to_string(),
    }
}

fn idempotency_completed_replay_mismatch() -> VfsError {
    VfsError::InvalidArgs {
        message: "idempotency completed replay does not match reservation".to_string(),
    }
}

fn idempotency_quota_exceeded() -> VfsError {
    VfsError::InvalidArgs {
        message: "idempotency quota exceeded".to_string(),
    }
}

fn increment_sweep_reason(summary: &mut IdempotencySweepSummary, reason: &str) {
    *summary
        .redacted_reasons
        .entry(reason.to_string())
        .or_insert(0) += 1;
}

const AUDIT_LOCK_NAMESPACE: i32 = 0x5354_524d; // "STRM"
const AUDIT_GLOBAL_SEQUENCE_LOCK: i32 = 0x4155_4454; // "AUDT"

fn audit_enum_to_db<T>(value: T, label: &str) -> Result<String, VfsError>
where
    T: serde::Serialize,
{
    match serde_json::to_value(value).map_err(|error| VfsError::CorruptStore {
        message: format!("audit {label} encode failed: {error}"),
    })? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(VfsError::CorruptStore {
            message: format!("audit {label} did not serialize as a string"),
        }),
    }
}

fn audit_action_from_db(value: &str) -> Result<AuditAction, VfsError> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(|error| {
        VfsError::CorruptStore {
            message: format!("unknown audit action in Postgres metadata: {value}: {error}"),
        }
    })
}

fn audit_outcome_from_db(value: &str) -> Result<AuditOutcome, VfsError> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(|error| {
        VfsError::CorruptStore {
            message: format!("unknown audit outcome in Postgres metadata: {value}: {error}"),
        }
    })
}

fn audit_json<T>(value: &T, label: &str) -> Result<serde_json::Value, VfsError>
where
    T: serde::Serialize,
{
    serde_json::to_value(value).map_err(|error| VfsError::CorruptStore {
        message: format!("audit {label} JSON encode failed: {error}"),
    })
}

fn row_to_audit_event(row: Row) -> Result<AuditEvent, VfsError> {
    let id: Uuid = row.get("id");
    let sequence: i64 = row.get("sequence");
    if sequence <= 0 {
        return Err(VfsError::CorruptStore {
            message: format!("audit event has invalid sequence {sequence}"),
        });
    }
    let timestamp: DateTime<Utc> = row.get("created_at");
    let Json(actor): Json<AuditActor> =
        row.try_get("actor_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "audit event actor JSON corrupt".to_string(),
            })?;
    let workspace: Option<Json<AuditWorkspaceContext>> =
        row.try_get("workspace_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "audit event workspace JSON corrupt".to_string(),
            })?;
    let action_text: String = row.get("action");
    let Json(resource): Json<AuditResource> =
        row.try_get("resource_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "audit event resource JSON corrupt".to_string(),
            })?;
    let outcome_text: String = row.get("outcome");
    let Json(details): Json<BTreeMap<String, String>> =
        row.try_get("details_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "audit event details JSON corrupt".to_string(),
            })?;

    Ok(AuditEvent {
        id,
        sequence: sequence as u64,
        timestamp,
        actor,
        workspace: workspace.map(|Json(workspace)| workspace),
        action: audit_action_from_db(&action_text)?,
        resource,
        outcome: audit_outcome_from_db(&outcome_text)?,
        details,
    })
}

#[async_trait]
impl AuditStore for PostgresMetadataStore {
    async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("audit append transaction", error))?;

        tx.execute(
            "SELECT pg_advisory_xact_lock($1, $2)",
            &[&AUDIT_LOCK_NAMESPACE, &AUDIT_GLOBAL_SEQUENCE_LOCK],
        )
        .await
        .map_err(|error| postgres_error("audit sequence lock", error))?;

        let sequence_row = tx
            .query_one(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
                 FROM audit_events
                 WHERE repo_id IS NULL",
                &[],
            )
            .await
            .map_err(|error| postgres_error("audit next sequence", error))?;
        let sequence: i64 = sequence_row.get("next_sequence");

        let id = Uuid::new_v4();
        let actor_json = Json(audit_json(&event.actor, "actor")?);
        let workspace_json: Option<Json<serde_json::Value>> = match &event.workspace {
            None => None,
            Some(workspace) => Some(Json(audit_json(workspace, "workspace")?)),
        };
        let action = audit_enum_to_db(event.action, "action")?;
        let resource_json = Json(audit_json(&event.resource, "resource")?);
        let outcome = audit_enum_to_db(event.outcome, "outcome")?;
        let details_json = Json(audit_json(&event.details, "details")?);

        let row = tx
            .query_one(
                r#"INSERT INTO audit_events (
                       id,
                       repo_id,
                       sequence,
                       created_at,
                       actor_json,
                       workspace_json,
                       action,
                       resource_json,
                       outcome,
                       details_json
                   )
                   VALUES ($1, NULL, $2, clock_timestamp(), $3, $4, $5, $6, $7, $8)
                   RETURNING id, sequence, created_at, actor_json, workspace_json,
                             action, resource_json, outcome, details_json"#,
                &[
                    &id,
                    &sequence,
                    &actor_json,
                    &workspace_json,
                    &action,
                    &resource_json,
                    &outcome,
                    &details_json,
                ],
            )
            .await
            .map_err(|error| postgres_error("audit insert event", error))?;

        let event = row_to_audit_event(row)?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("audit append commit", error))?;
        Ok(event)
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        let client = self.connect_client().await?;
        let limit = i64::try_from(limit).map_err(|_| VfsError::InvalidArgs {
            message: "audit list limit exceeds Postgres BIGINT range".to_string(),
        })?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = client
            .query(
                r#"SELECT id, sequence, created_at, actor_json, workspace_json,
                          action, resource_json, outcome, details_json
                   FROM audit_events
                   WHERE repo_id IS NULL
                   ORDER BY sequence DESC
                   LIMIT $1"#,
                &[&limit],
            )
            .await
            .map_err(|error| postgres_error("audit list recent", error))?;

        let mut events = rows
            .into_iter()
            .map(row_to_audit_event)
            .collect::<Result<Vec<_>, VfsError>>()?;
        events.reverse();
        Ok(events)
    }

    async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError> {
        let client = self.connect_client().await?;
        let commit_action = audit_enum_to_db(AuditAction::VcsCommit, "action")?;
        let revert_action = audit_enum_to_db(AuditAction::VcsRevert, "action")?;
        let resource_kind = audit_enum_to_db(AuditResourceKind::Commit, "resource kind")?;
        let row = client
            .query_one(
                r#"SELECT EXISTS(
                       SELECT 1
                       FROM audit_events
                       WHERE action IN ($1, $2)
                         AND repo_id IS NULL
                         AND resource_json->>'kind' = $3
                         AND resource_json->>'id' = $4
                         AND resource_json->>'path' IS NULL
                   ) AS present"#,
                &[&commit_action, &revert_action, &resource_kind, &commit_id],
            )
            .await
            .map_err(|error| postgres_error("audit contains VCS commit event", error))?;
        Ok(row.get("present"))
    }

    async fn contains_fs_mutation_recovery_event(
        &self,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let client = self.connect_client().await?;
        let action = audit_enum_to_db(action, "action")?;
        let resource_kind = audit_enum_to_db(AuditResourceKind::Path, "resource kind")?;
        let row = client
            .query_one(
                r#"SELECT EXISTS(
                       SELECT 1
                       FROM audit_events
                       WHERE action = $1
                         AND repo_id IS NULL
                         AND resource_json->>'kind' = $2
                         AND resource_json->>'id' IS NULL
                         AND details_json->>'operation_id' = $3
                         AND details_json->>'target_ref' = $4
                         AND details_json->>'new_commit' = $5
                   ) AS present"#,
                &[
                    &action,
                    &resource_kind,
                    &operation_id,
                    &target_ref,
                    &new_commit,
                ],
            )
            .await
            .map_err(|error| {
                postgres_error("audit contains durable FS mutation recovery event", error)
            })?;
        Ok(row.get("present"))
    }
}

fn uid_to_i32(uid: crate::auth::Uid) -> Result<i32, VfsError> {
    i32::try_from(uid).map_err(|_| VfsError::InvalidArgs {
        message: "uid exceeds Postgres INTEGER range".to_string(),
    })
}

fn i32_to_uid(uid: i32) -> Result<crate::auth::Uid, VfsError> {
    crate::auth::Uid::try_from(uid).map_err(|_| VfsError::CorruptStore {
        message: "Postgres metadata row has invalid uid".to_string(),
    })
}

fn row_to_workspace_record(row: Row) -> Result<WorkspaceRecord, VfsError> {
    let version: i64 = row.get("version");
    if version < 0 {
        return Err(VfsError::CorruptStore {
            message: format!("workspace has invalid negative version {version}"),
        });
    }
    let base_ref: String = row.get("base_ref");
    let base_ref = normalize_workspace_ref(&base_ref).map_err(|error| VfsError::CorruptStore {
        message: format!("workspace has invalid base ref: {error}"),
    })?;
    let session_ref: Option<String> = row.get("session_ref");
    let session_ref =
        normalize_optional_workspace_session_ref(session_ref.as_deref()).map_err(|error| {
            VfsError::CorruptStore {
                message: format!("workspace has invalid session ref: {error}"),
            }
        })?;

    Ok(WorkspaceRecord {
        id: row.get("id"),
        name: row.get("name"),
        root_path: row.get("root_path"),
        head_commit: row.get("head_commit"),
        version: version as u64,
        base_ref,
        session_ref,
        repo_id: row.get("repo_id"),
    })
}

fn row_to_workspace_token_record(row: Row) -> Result<WorkspaceTokenRecord, VfsError> {
    let Json(read_prefixes): Json<Vec<String>> =
        row.try_get("read_prefixes_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "workspace token read prefixes JSON corrupt".to_string(),
            })?;
    let Json(write_prefixes): Json<Vec<String>> =
        row.try_get("write_prefixes_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "workspace token write prefixes JSON corrupt".to_string(),
            })?;
    let agent_uid: i32 = row.get("agent_uid");
    let agent_uid = i32_to_uid(agent_uid)?;
    let _repo_id: Option<String> = row.get("repo_id");
    let principal_uid = row
        .get::<_, Option<i32>>("principal_uid")
        .map(i32_to_uid)
        .transpose()?;
    let token_version = positive_i64_to_u64(row.get("token_version"), "workspace token version")?;
    let secret_hash: String = row.get("secret_hash");
    if !is_lower_hex_sha256(&secret_hash) {
        return Err(VfsError::CorruptStore {
            message: "workspace token secret hash has invalid shape".to_string(),
        });
    }
    let issued_at_unix = datetime_to_unix_seconds(
        workspace_token_datetime(&row, "issued_at", "workspace token issuance time")?,
        "workspace token issuance time",
    )?;
    let updated_at_unix = datetime_to_unix_seconds(
        workspace_token_datetime(&row, "updated_at", "workspace token update time")?,
        "workspace token update time",
    )?;
    let expires_at_unix = optional_datetime_to_unix_seconds(
        optional_workspace_token_datetime(&row, "expires_at", "workspace token expiration time")?,
        "workspace token expiration time",
    )?;
    let revoked_at_unix = optional_datetime_to_unix_seconds(
        optional_workspace_token_datetime(&row, "revoked_at", "workspace token revocation time")?,
        "workspace token revocation time",
    )?;

    Ok(WorkspaceTokenRecord {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        agent_uid,
        secret_hash,
        read_prefixes,
        write_prefixes,
        principal_uid,
        token_version,
        issued_at_unix,
        updated_at_unix,
        expires_at_unix,
        revoked_at_unix,
    })
}

fn workspace_token_datetime(
    row: &Row,
    column: &str,
    label: &str,
) -> Result<DateTime<Utc>, VfsError> {
    row.try_get(column).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn optional_workspace_token_datetime(
    row: &Row,
    column: &str,
    label: &str,
) -> Result<Option<DateTime<Utc>>, VfsError> {
    row.try_get(column).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn workspace_principal_kind_from_db(value: &str) -> Result<WorkspacePrincipalKind, VfsError> {
    match value {
        "human" => Ok(WorkspacePrincipalKind::Human),
        "service_account" => Ok(WorkspacePrincipalKind::ServiceAccount),
        "agent" => Ok(WorkspacePrincipalKind::Agent),
        other => Err(VfsError::CorruptStore {
            message: format!("unknown durable principal kind in Postgres metadata: {other}"),
        }),
    }
}

fn row_to_workspace_principal_record(row: Row) -> Result<WorkspacePrincipalRecord, VfsError> {
    let uid: i32 = row.get("uid");
    let primary_gid: i32 = row.get("primary_gid");
    let Json(groups): Json<Vec<i32>> =
        row.try_get("groups_json")
            .map_err(|_| VfsError::CorruptStore {
                message: "durable principal groups JSON corrupt".to_string(),
            })?;
    let groups = groups
        .into_iter()
        .map(i32_to_uid)
        .collect::<Result<Vec<_>, _>>()?;
    let kind: String = row.get("kind");

    Ok(WorkspacePrincipalRecord {
        uid: i32_to_uid(uid)?,
        username: row.get("username"),
        gid: i32_to_uid(primary_gid)?,
        groups,
        kind: workspace_principal_kind_from_db(&kind)?,
        active: row.get("active"),
    })
}

async fn load_active_workspace_principal(
    client: &Client,
    repo_id: &str,
    principal_uid: crate::auth::Uid,
) -> Result<Option<WorkspacePrincipalRecord>, VfsError> {
    let principal_uid = uid_to_i32(principal_uid)?;
    let row = client
        .query_opt(
            r#"SELECT uid, username, primary_gid, groups_json, kind, active
               FROM durable_principals
               WHERE repo_id = $1 AND uid = $2 AND active = true"#,
            &[&repo_id, &principal_uid],
        )
        .await
        .map_err(|error| postgres_error("durable principal load", error))?;
    row.map(row_to_workspace_principal_record).transpose()
}

#[async_trait]
impl WorkspaceMetadataStore for PostgresMetadataStore {
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL
                   ORDER BY name ASC, id ASC"#,
                &[],
            )
            .await
            .map_err(|error| postgres_error("workspace list", error))?;
        rows.into_iter()
            .map(row_to_workspace_record)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn list_workspaces_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id = $1
                      OR ($1 = 'local' AND repo_id IS NULL)
                   ORDER BY name ASC, id ASC"#,
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("workspace list for repo", error))?;
        rows.into_iter()
            .map(row_to_workspace_record)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
    ) -> Result<WorkspaceRecord, VfsError> {
        self.create_workspace_with_refs(name, root_path, MAIN_REF, None)
            .await
    }

    async fn create_workspace_with_refs(
        &self,
        name: &str,
        root_path: &str,
        base_ref: &str,
        session_ref: Option<&str>,
    ) -> Result<WorkspaceRecord, VfsError> {
        let record = workspace_record(name, root_path, base_ref, session_ref)?;
        let client = self.connect_client().await?;
        let version = u64_to_i64(record.version, "workspace version")?;
        let row = client
            .query_one(
                r#"INSERT INTO workspaces (
                       id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   )
                   VALUES ($1, NULL, $2, $3, $4, $5, $6, $7)
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[
                    &record.id,
                    &record.name,
                    &record.root_path,
                    &record.head_commit,
                    &version,
                    &record.base_ref,
                    &record.session_ref,
                ],
            )
            .await
            .map_err(|error| postgres_error("workspace create", error))?;
        row_to_workspace_record(row)
    }

    async fn create_workspace_with_refs_for_repo(
        &self,
        repo_id: RepoId,
        name: &str,
        root_path: &str,
        base_ref: &str,
        session_ref: Option<&str>,
    ) -> Result<WorkspaceRecord, VfsError> {
        let record =
            workspace_record_for_repo(repo_id.clone(), name, root_path, base_ref, session_ref)?;
        let client = self.connect_client().await?;
        ensure_repo(&client, &repo_id).await?;
        let version = u64_to_i64(record.version, "workspace version")?;
        let row = client
            .query_one(
                r#"INSERT INTO workspaces (
                       id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[
                    &record.id,
                    &repo_id.as_str(),
                    &record.name,
                    &record.root_path,
                    &record.head_commit,
                    &version,
                    &record.base_ref,
                    &record.session_ref,
                ],
            )
            .await
            .map_err(|error| postgres_error("workspace create for repo", error))?;
        row_to_workspace_record(row)
    }

    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1"#,
                &[&id],
            )
            .await
            .map_err(|error| postgres_error("workspace get", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn get_workspace_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE id = $1
                     AND (
                         repo_id = $2
                         OR ($2 = 'local' AND repo_id IS NULL)
                     )"#,
                &[&id, &repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("workspace get for repo", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn update_head_commit(
        &self,
        id: Uuid,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"UPDATE workspaces
                   SET head_commit = $2,
                       version = version + 1
                   WHERE repo_id IS NULL AND id = $1
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&id, &head_commit],
            )
            .await
            .map_err(|error| postgres_error("workspace update head", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn update_head_commit_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"UPDATE workspaces
                   SET head_commit = $3,
                       version = version + 1
                   WHERE id = $1
                     AND (
                         repo_id = $2
                         OR ($2 = 'local' AND repo_id IS NULL)
                     )
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&id, &repo_id.as_str(), &head_commit],
            )
            .await
            .map_err(|error| postgres_error("workspace update head for repo", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn update_head_commit_if_current(
        &self,
        id: Uuid,
        expected_head_commit: Option<&str>,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"UPDATE workspaces
                   SET head_commit = $3,
                       version = version + 1
                   WHERE repo_id IS NULL
                     AND id = $1
                     AND head_commit IS NOT DISTINCT FROM $2
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&id, &expected_head_commit, &head_commit],
            )
            .await
            .map_err(|error| postgres_error("workspace update head if current", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn update_head_commit_if_current_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
        expected_head_commit: Option<&str>,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"UPDATE workspaces
                   SET head_commit = $4,
                       version = version + 1
                   WHERE id = $1
                     AND (
                         repo_id = $2
                         OR ($2 = 'local' AND repo_id IS NULL)
                     )
                     AND head_commit IS NOT DISTINCT FROM $3
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&id, &repo_id.as_str(), &expected_head_commit, &head_commit],
            )
            .await
            .map_err(|error| postgres_error("workspace update head if current for repo", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn issue_scoped_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: crate::auth::Uid,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("workspace token transaction", error))?;

        let workspace_row = tx
            .query_opt(
                r#"SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE id = $1
                   FOR UPDATE"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token load workspace", error))?;
        let Some(workspace_row) = workspace_row else {
            return Err(VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            });
        };
        let workspace = row_to_workspace_record(workspace_row)?;
        let read_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, read_prefixes)?;
        let write_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, write_prefixes)?;
        let read_json = Json(&read_prefixes);
        let write_json = Json(&write_prefixes);
        let agent_uid = uid_to_i32(agent_uid)?;

        for _ in 0..3 {
            let raw_secret = generate_workspace_token_secret();
            let secret_hash = hash_workspace_token_secret(&raw_secret);
            let token_id = Uuid::new_v4();
            let row = tx
                .query_opt(
                    r#"INSERT INTO workspace_tokens (
                           id, workspace_id, repo_id, name, agent_uid, secret_hash,
                           read_prefixes_json, write_prefixes_json,
                           principal_uid, token_version, issued_at, updated_at,
                           expires_at, revoked_at
                       )
                       VALUES (
                           $1, $2, $3, $4, $5, $6, $7, $8,
                           $5, 1, now(), now(), NULL, NULL
                       )
                       ON CONFLICT (workspace_id, secret_hash) DO NOTHING
                       RETURNING id, workspace_id, repo_id, name, agent_uid, secret_hash,
                                 read_prefixes_json, write_prefixes_json,
                                 principal_uid, token_version, issued_at, updated_at,
                                 expires_at, revoked_at, created_at"#,
                    &[
                        &token_id,
                        &workspace_id,
                        &workspace.repo_id,
                        &name,
                        &agent_uid,
                        &secret_hash,
                        &read_json,
                        &write_json,
                    ],
                )
                .await
                .map_err(|error| postgres_error("workspace token insert", error))?;

            if let Some(row) = row {
                let token = row_to_workspace_token_record(row)?;
                tx.commit()
                    .await
                    .map_err(|error| postgres_error("workspace token commit", error))?;
                return Ok(IssuedWorkspaceToken { token, raw_secret });
            }
        }

        Err(VfsError::ObjectWriteConflict {
            message: "workspace token secret collision after retries".to_string(),
        })
    }

    async fn validate_workspace_token_at(
        &self,
        workspace_id: Uuid,
        raw_secret: &str,
        now_unix: u64,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
        let client = self.connect_client().await?;
        let workspace_row = client
            .query_opt(
                r#"SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE id = $1"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token validate workspace", error))?;
        let Some(workspace_row) = workspace_row else {
            return Ok(None);
        };
        let workspace = row_to_workspace_record(workspace_row)?;
        let expected_hash = hash_workspace_token_secret(raw_secret);

        let rows = client
            .query(
                r#"SELECT id, workspace_id, repo_id, name, agent_uid, secret_hash,
                          read_prefixes_json, write_prefixes_json,
                          principal_uid, token_version, issued_at, updated_at,
                          expires_at, revoked_at, created_at
                   FROM workspace_tokens
                   WHERE workspace_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token validate token", error))?;

        let mut matched_token = None;
        let mut matched_expired_token = false;
        for row in rows {
            let token_repo_id: Option<String> = row.get("repo_id");
            if token_repo_id.as_deref() != workspace.repo_id.as_deref() {
                return Err(VfsError::CorruptStore {
                    message: "workspace token repo does not match workspace repo".to_string(),
                });
            }
            let token = row_to_workspace_token_record(row)?;
            let normalized_read = normalize_workspace_token_prefixes(
                &workspace.root_path,
                token.read_prefixes.clone(),
            )
            .map_err(|_| VfsError::CorruptStore {
                message: "workspace token read prefixes are outside workspace root".to_string(),
            })?;
            if normalized_read != token.read_prefixes {
                return Err(VfsError::CorruptStore {
                    message: "workspace token read prefixes are outside workspace root".to_string(),
                });
            }
            let normalized_write = normalize_workspace_token_prefixes(
                &workspace.root_path,
                token.write_prefixes.clone(),
            )
            .map_err(|_| VfsError::CorruptStore {
                message: "workspace token write prefixes are outside workspace root".to_string(),
            })?;
            if normalized_write != token.write_prefixes {
                return Err(VfsError::CorruptStore {
                    message: "workspace token write prefixes are outside workspace root"
                        .to_string(),
                });
            }
            if workspace_token_hash_eq(&token.secret_hash, &expected_hash) {
                if token_is_valid_at(&token, now_unix) {
                    matched_token = Some(token);
                } else {
                    matched_expired_token = true;
                }
            }
        }

        if let Some(token) = matched_token {
            let principal = match workspace.repo_id.as_deref() {
                Some(repo_id) => {
                    let Some(principal_uid) = token.principal_uid else {
                        return Ok(None);
                    };
                    let Some(principal) =
                        load_active_workspace_principal(&client, repo_id, principal_uid).await?
                    else {
                        return Ok(None);
                    };
                    Some(principal)
                }
                None => None,
            };
            return Ok(Some(ValidWorkspaceToken {
                repo_id: workspace.repo_id.clone(),
                workspace,
                token,
                principal,
            }));
        }
        if matched_expired_token {
            return Ok(None);
        }

        Ok(None)
    }

    async fn revoke_workspace_token(
        &self,
        workspace_id: Uuid,
        token_id: Uuid,
        now_unix: u64,
    ) -> Result<Option<WorkspaceTokenRecord>, VfsError> {
        let client = self.connect_client().await?;
        let now_unix = u64_to_i64(now_unix, "workspace token revocation time")?;
        let row = client
            .query_opt(
                r#"UPDATE workspace_tokens
                   SET revoked_at = to_timestamp($3::bigint::double precision),
                       updated_at = to_timestamp($3::bigint::double precision),
                       token_version = token_version + 1
                   WHERE workspace_id = $1
                     AND id = $2
                     AND token_version < 9223372036854775807
                   RETURNING id, workspace_id, repo_id, name, agent_uid, secret_hash,
                             read_prefixes_json, write_prefixes_json,
                             principal_uid, token_version, issued_at, updated_at,
                             expires_at, revoked_at, created_at"#,
                &[&workspace_id, &token_id, &now_unix],
            )
            .await
            .map_err(|error| postgres_error("workspace token revoke", error))?;
        if let Some(row) = row {
            return row_to_workspace_token_record(row).map(Some);
        }

        let overflow_row = client
            .query_opt(
                r#"SELECT 1
                   FROM workspace_tokens
                   WHERE workspace_id = $1
                     AND id = $2
                     AND token_version >= 9223372036854775807"#,
                &[&workspace_id, &token_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token revoke overflow check", error))?;
        if overflow_row.is_some() {
            return Err(VfsError::InvalidArgs {
                message: "workspace token version overflow".to_string(),
            });
        }

        Ok(None)
    }
}

fn change_request_status_to_db(status: ChangeRequestStatus) -> &'static str {
    match status {
        ChangeRequestStatus::Open => "open",
        ChangeRequestStatus::Merged => "merged",
        ChangeRequestStatus::Rejected => "rejected",
    }
}

fn change_request_status_from_db(value: &str) -> Result<ChangeRequestStatus, VfsError> {
    match value {
        "open" => Ok(ChangeRequestStatus::Open),
        "merged" => Ok(ChangeRequestStatus::Merged),
        "rejected" => Ok(ChangeRequestStatus::Rejected),
        other => Err(VfsError::CorruptStore {
            message: format!("unknown change request status in Postgres metadata: {other}"),
        }),
    }
}

fn review_comment_kind_to_db(kind: ReviewCommentKind) -> &'static str {
    match kind {
        ReviewCommentKind::General => "general",
        ReviewCommentKind::ChangesRequested => "changes_requested",
    }
}

fn review_comment_kind_from_db(value: &str) -> Result<ReviewCommentKind, VfsError> {
    match value {
        "general" => Ok(ReviewCommentKind::General),
        "changes_requested" => Ok(ReviewCommentKind::ChangesRequested),
        other => Err(VfsError::CorruptStore {
            message: format!("unknown review comment kind in Postgres metadata: {other}"),
        }),
    }
}

fn positive_i64_to_u64(value: i64, label: &str) -> Result<u64, VfsError> {
    if value <= 0 {
        return Err(VfsError::CorruptStore {
            message: format!("{label} has invalid version {value}"),
        });
    }
    Ok(value as u64)
}

fn required_approvals_from_i32(value: i32, label: &str) -> Result<u32, VfsError> {
    u32::try_from(value).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} has invalid required approvals"),
    })
}

fn row_to_protected_ref_rule(row: Row) -> Result<ProtectedRefRule, VfsError> {
    let required_raw: i32 = row.get("required_approvals");
    let required_approvals = required_approvals_from_i32(required_raw, "protected ref rule")?;
    let record = ProtectedRefRule {
        id: row.get("id"),
        repo_id: RepoId::new(row.get::<_, String>("repo_id"))?,
        ref_name: row.get("ref_name"),
        required_approvals,
        require_all_files_viewed: row.get("require_all_files_viewed"),
        created_by: i32_to_uid(row.get("created_by"))?,
        active: row.get("active"),
    };
    record.validate().map_err(corrupt_from_invalid)?;
    Ok(record)
}

fn row_to_protected_path_rule(row: Row) -> Result<ProtectedPathRule, VfsError> {
    let required_raw: i32 = row.get("required_approvals");
    let required_approvals = required_approvals_from_i32(required_raw, "protected path rule")?;
    let record = ProtectedPathRule {
        id: row.get("id"),
        repo_id: RepoId::new(row.get::<_, String>("repo_id"))?,
        path_prefix: row.get("path_prefix"),
        target_ref: row.get("target_ref"),
        required_approvals,
        require_all_files_viewed: row.get("require_all_files_viewed"),
        created_by: i32_to_uid(row.get("created_by"))?,
        active: row.get("active"),
    };
    record.validate().map_err(corrupt_from_invalid)?;
    Ok(record)
}

fn row_to_change_request(row: Row) -> Result<ChangeRequest, VfsError> {
    let status: String = row.get("status");
    let record = ChangeRequest {
        id: row.get("id"),
        repo_id: RepoId::new(row.get::<_, String>("repo_id"))?,
        title: row.get("title"),
        description: row.get("description"),
        source_ref: row.get("source_ref"),
        target_ref: row.get("target_ref"),
        base_commit: row.get("base_commit"),
        head_commit: row.get("head_commit"),
        status: change_request_status_from_db(&status)?,
        created_by: i32_to_uid(row.get("created_by"))?,
        version: positive_i64_to_u64(row.get("version"), "change request")?,
    };
    record.validate().map_err(corrupt_from_invalid)?;
    Ok(record)
}

fn row_to_approval_record(row: Row, change: &ChangeRequest) -> Result<ApprovalRecord, VfsError> {
    let record = ApprovalRecord {
        id: row.get("id"),
        change_request_id: row.get("change_request_id"),
        head_commit: row.get("head_commit"),
        approved_by: i32_to_uid(row.get("approved_by"))?,
        comment: row.get("comment"),
        active: row.get("active"),
        dismissed_by: row
            .get::<_, Option<i32>>("dismissed_by")
            .map(i32_to_uid)
            .transpose()?,
        dismissal_reason: row.get("dismissal_reason"),
        version: positive_i64_to_u64(row.get("version"), "approval")?,
    };
    record.validate(change).map_err(corrupt_from_invalid)?;
    Ok(record)
}

fn row_to_review_assignment(
    row: Row,
    change: &ChangeRequest,
) -> Result<ReviewAssignment, VfsError> {
    let record = ReviewAssignment {
        id: row.get("id"),
        change_request_id: row.get("change_request_id"),
        reviewer: i32_to_uid(row.get("reviewer"))?,
        assigned_by: i32_to_uid(row.get("assigned_by"))?,
        required: row.get("required"),
        active: row.get("active"),
        version: positive_i64_to_u64(row.get("version"), "review assignment")?,
    };
    record.validate(change).map_err(corrupt_from_invalid)?;
    Ok(record)
}

fn row_to_review_comment(row: Row, change: &ChangeRequest) -> Result<ReviewComment, VfsError> {
    let kind: String = row.get("kind");
    let record = ReviewComment {
        id: row.get("id"),
        change_request_id: row.get("change_request_id"),
        author: i32_to_uid(row.get("author"))?,
        body: row.get("body"),
        path: row.get("path"),
        kind: review_comment_kind_from_db(&kind)?,
        active: row.get("active"),
        version: positive_i64_to_u64(row.get("version"), "review comment")?,
    };
    record.validate(change).map_err(corrupt_from_invalid)?;
    Ok(record)
}

async fn load_review_change_request<C>(
    client: &C,
    repo_id: &RepoId,
    id: Uuid,
) -> Result<Option<ChangeRequest>, VfsError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                      head_commit, status, created_by, version
               FROM change_requests
               WHERE repo_id = $1 AND id = $2"#,
            &[&repo_id.as_str(), &id],
        )
        .await
        .map_err(|error| postgres_error("review change request get", error))?;
    row.map(row_to_change_request).transpose()
}

#[async_trait]
impl ReviewStore for PostgresMetadataStore {
    async fn create_protected_ref_rule_for_repo(
        &self,
        repo_id: &RepoId,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
        require_all_files_viewed: bool,
    ) -> Result<ProtectedRefRule, VfsError> {
        let rule = ProtectedRefRule::new_for_repo(
            repo_id.clone(),
            ref_name,
            required_approvals,
            created_by,
            require_all_files_viewed,
        )?;
        let client = self.connect_client().await?;
        ensure_repo(&client, repo_id).await?;
        let created_by = uid_to_i32(rule.created_by)?;
        let required =
            i32::try_from(rule.required_approvals).map_err(|_| VfsError::InvalidArgs {
                message: "required approvals exceeds Postgres INTEGER range".to_string(),
            })?;
        let row = client
            .query_one(
                r#"INSERT INTO protected_ref_rules (
                       id, repo_id, ref_name, required_approvals, require_all_files_viewed, created_by, active
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   RETURNING id, repo_id, ref_name, required_approvals, require_all_files_viewed, created_by, active"#,
                &[
                    &rule.id,
                    &rule.repo_id.as_str(),
                    &rule.ref_name,
                    &required,
                    &rule.require_all_files_viewed,
                    &created_by,
                    &rule.active,
                ],
            )
            .await
            .map_err(|error| postgres_error("review protected ref insert", error))?;
        row_to_protected_ref_rule(row)
    }

    async fn list_protected_ref_rules_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<ProtectedRefRule>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, repo_id, ref_name, required_approvals, require_all_files_viewed, created_by, active
                   FROM protected_ref_rules
                   WHERE repo_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("review protected ref list", error))?;
        rows.into_iter().map(row_to_protected_ref_rule).collect()
    }

    async fn get_protected_ref_rule_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
    ) -> Result<Option<ProtectedRefRule>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, repo_id, ref_name, required_approvals, require_all_files_viewed, created_by, active
                   FROM protected_ref_rules
                   WHERE repo_id = $1 AND id = $2"#,
                &[&repo_id.as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review protected ref get", error))?;
        row.map(row_to_protected_ref_rule).transpose()
    }

    async fn create_protected_path_rule_for_repo(
        &self,
        repo_id: &RepoId,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
        require_all_files_viewed: bool,
    ) -> Result<ProtectedPathRule, VfsError> {
        let rule = ProtectedPathRule::new_for_repo(
            repo_id.clone(),
            path_prefix,
            target_ref,
            required_approvals,
            created_by,
            require_all_files_viewed,
        )?;
        let client = self.connect_client().await?;
        ensure_repo(&client, repo_id).await?;
        let created_by = uid_to_i32(rule.created_by)?;
        let required =
            i32::try_from(rule.required_approvals).map_err(|_| VfsError::InvalidArgs {
                message: "required approvals exceeds Postgres INTEGER range".to_string(),
            })?;
        let row = client
            .query_one(
                r#"INSERT INTO protected_path_rules (
                       id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by, active
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                   RETURNING id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by, active"#,
                &[
                    &rule.id,
                    &rule.repo_id.as_str(),
                    &rule.path_prefix,
                    &rule.target_ref,
                    &required,
                    &rule.require_all_files_viewed,
                    &created_by,
                    &rule.active,
                ],
            )
            .await
            .map_err(|error| postgres_error("review protected path insert", error))?;
        row_to_protected_path_rule(row)
    }

    async fn list_protected_path_rules_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<ProtectedPathRule>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by, active
                   FROM protected_path_rules
                   WHERE repo_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("review protected path list", error))?;
        rows.into_iter().map(row_to_protected_path_rule).collect()
    }

    async fn get_protected_path_rule_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by, active
                   FROM protected_path_rules
                   WHERE repo_id = $1 AND id = $2"#,
                &[&repo_id.as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review protected path get", error))?;
        row.map(row_to_protected_path_rule).transpose()
    }

    async fn create_change_request_for_repo(
        &self,
        repo_id: &RepoId,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError> {
        let change = ChangeRequest::new_for_repo(repo_id.clone(), input)?;
        let client = self.connect_client().await?;
        ensure_repo(&client, repo_id).await?;
        let created_by = uid_to_i32(change.created_by)?;
        let version = u64_to_i64(change.version, "change request version")?;
        let status = change_request_status_to_db(change.status);
        let row = client
            .query_one(
                r#"INSERT INTO change_requests (
                       id, repo_id, title, description, source_ref, target_ref,
                       base_commit, head_commit, status, created_by, version
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                   RETURNING id, repo_id, title, description, source_ref, target_ref, base_commit,
                             head_commit, status, created_by, version"#,
                &[
                    &change.id,
                    &change.repo_id.as_str(),
                    &change.title,
                    &change.description,
                    &change.source_ref,
                    &change.target_ref,
                    &change.base_commit,
                    &change.head_commit,
                    &status,
                    &created_by,
                    &version,
                ],
            )
            .await
            .map_err(|error| postgres_error("review change request insert", error))?;
        row_to_change_request(row)
    }

    async fn list_change_requests_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<ChangeRequest>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("review change request list", error))?;
        rows.into_iter().map(row_to_change_request).collect()
    }

    async fn get_change_request_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
    ) -> Result<Option<ChangeRequest>, VfsError> {
        load_review_change_request(&self.connect_client().await?, repo_id, id).await
    }

    async fn transition_change_request_for_repo(
        &self,
        repo_id: &RepoId,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review change transition transaction", error))?;
        let current_row = tx
            .query_opt(
                r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&repo_id.as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review change transition lock", error))?;
        let Some(current_row) = current_row else {
            tx.commit()
                .await
                .map_err(|error| postgres_error("review change transition commit", error))?;
            return Ok(None);
        };
        let current = row_to_change_request(current_row)?;
        let next = current.transition(status)?;
        let version = u64_to_i64(next.version, "change request version")?;
        let status_db = change_request_status_to_db(next.status);
        let row = tx
            .query_opt(
                r#"UPDATE change_requests
                   SET status = $1, version = $2, updated_at = now()
                   WHERE repo_id = $3 AND id = $4
                   RETURNING id, repo_id, title, description, source_ref, target_ref, base_commit,
                             head_commit, status, created_by, version"#,
                &[&status_db, &version, &repo_id.as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review change transition update", error))?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("review change transition commit", error))?;
        row.map(row_to_change_request).transpose()
    }

    async fn create_approval_for_repo(
        &self,
        repo_id: &RepoId,
        input: NewApprovalRecord,
    ) -> Result<ApprovalRecordMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review approval transaction", error))?;
        let change_row = tx
            .query_opt(
                r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&repo_id.as_str(), &input.change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review approval lock change request", error))?;
        let Some(change_row) = change_row else {
            tx.rollback()
                .await
                .map_err(|error| postgres_error("review approval rollback", error))?;
            return Err(VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            });
        };
        let change = row_to_change_request(change_row)?;
        let record = ApprovalRecord::new(input.clone(), &change)?;
        let approved_by = uid_to_i32(record.approved_by)?;
        let inserted = tx
            .query_opt(
                r#"INSERT INTO approvals (
                       id, change_request_id, head_commit, approved_by, comment, active,
                       dismissed_by, dismissal_reason, version
                   )
                   VALUES ($1, $2, $3, $4, $5, true, NULL, NULL, 1)
                   ON CONFLICT (change_request_id, head_commit, approved_by) WHERE active DO NOTHING
                   RETURNING id, change_request_id, head_commit, approved_by, comment, active,
                             dismissed_by, dismissal_reason, version"#,
                &[
                    &record.id,
                    &record.change_request_id,
                    &record.head_commit,
                    &approved_by,
                    &record.comment,
                ],
            )
            .await
            .map_err(|error| postgres_error("review approval insert", error))?;
        if let Some(row) = inserted {
            let record = row_to_approval_record(row, &change)?;
            tx.commit()
                .await
                .map_err(|error| postgres_error("review approval commit", error))?;
            return Ok(ApprovalRecordMutation {
                record,
                created: true,
            });
        }

        let existing_row = tx
            .query_opt(
                r#"SELECT id, change_request_id, head_commit, approved_by, comment, active,
                             dismissed_by, dismissal_reason, version
                   FROM approvals
                   WHERE change_request_id = $1 AND head_commit = $2 AND approved_by = $3 AND active = true"#,
                &[&input.change_request_id, &input.head_commit, &approved_by],
            )
            .await
            .map_err(|error| postgres_error("review approval load duplicate", error))?;
        let Some(existing_row) = existing_row else {
            tx.rollback()
                .await
                .map_err(|error| postgres_error("review approval rollback", error))?;
            return Err(VfsError::CorruptStore {
                message: "review approval insert conflicted without a visible active row"
                    .to_string(),
            });
        };
        let existing = row_to_approval_record(existing_row, &change)?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("review approval commit", error))?;
        Ok(ApprovalRecordMutation {
            record: existing,
            created: false,
        })
    }

    async fn list_approvals_for_repo(
        &self,
        repo_id: &RepoId,
        change_request_id: Uuid,
    ) -> Result<Vec<ApprovalRecord>, VfsError> {
        let client = self.connect_client().await?;
        let Some(change) = load_review_change_request(&client, repo_id, change_request_id).await?
        else {
            return Ok(vec![]);
        };
        let rows = client
            .query(
                r#"SELECT id, change_request_id, head_commit, approved_by, comment, active,
                          dismissed_by, dismissal_reason, version
                   FROM approvals
                   WHERE change_request_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review approval list", error))?;
        rows.into_iter()
            .map(|row| row_to_approval_record(row, &change))
            .collect()
    }

    async fn assign_reviewer_for_repo(
        &self,
        repo_id: &RepoId,
        input: NewReviewAssignment,
    ) -> Result<ReviewAssignmentMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review assignment transaction", error))?;
        let change_row = tx
            .query_opt(
                r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&repo_id.as_str(), &input.change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review assignment lock change", error))?;
        let Some(change_row) = change_row else {
            tx.rollback()
                .await
                .map_err(|error| postgres_error("review assignment rollback", error))?;
            return Err(VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            });
        };
        let change = row_to_change_request(change_row)?;
        let assignment = ReviewAssignment::new(input.clone(), &change)?;

        let reviewer_db = uid_to_i32(input.reviewer)?;
        let assignment_row = tx
            .query_opt(
                r#"SELECT id, change_request_id, reviewer, assigned_by, required, active, version
                   FROM reviewer_assignments
                   WHERE change_request_id = $1 AND reviewer = $2
                   FOR UPDATE"#,
                &[&input.change_request_id, &reviewer_db],
            )
            .await
            .map_err(|error| postgres_error("review assignment lock row", error))?;

        if let Some(row) = assignment_row {
            if !row.get::<_, bool>("active") {
                tx.rollback()
                    .await
                    .map_err(|error| postgres_error("review assignment rollback", error))?;
                return Err(VfsError::CorruptStore {
                    message: "review assignment row is inactive".to_string(),
                });
            }
            let required: bool = row.get("required");
            if required == input.required {
                let stored = row_to_review_assignment(row, &change)?;
                tx.commit()
                    .await
                    .map_err(|error| postgres_error("review assignment commit", error))?;
                return Ok(ReviewAssignmentMutation {
                    assignment: stored,
                    created: false,
                    updated: false,
                });
            }
            let id: Uuid = row.get("id");
            let version: i64 = row.get("version");
            let next_version = positive_i64_to_u64(version, "review assignment")?
                .checked_add(1)
                .ok_or_else(|| VfsError::InvalidArgs {
                    message: "review assignment version overflow".to_string(),
                })?;
            let next_version_i64 = u64_to_i64(next_version, "review assignment version")?;
            let assigned_by_db = uid_to_i32(input.assigned_by)?;
            let updated_row = tx
                .query_opt(
                    r#"UPDATE reviewer_assignments
                       SET required = $1, assigned_by = $2, version = $3, updated_at = now()
                       WHERE id = $4
                       RETURNING id, change_request_id, reviewer, assigned_by, required, active, version"#,
                    &[&input.required, &assigned_by_db, &next_version_i64, &id],
                )
                .await
                .map_err(|error| postgres_error("review assignment update", error))?;
            let Some(updated_row) = updated_row else {
                tx.rollback()
                    .await
                    .map_err(|error| postgres_error("review assignment rollback", error))?;
                return Err(VfsError::CorruptStore {
                    message: "review assignment update returned no row".to_string(),
                });
            };
            let stored = row_to_review_assignment(updated_row, &change)?;
            tx.commit()
                .await
                .map_err(|error| postgres_error("review assignment commit", error))?;
            return Ok(ReviewAssignmentMutation {
                assignment: stored,
                created: false,
                updated: true,
            });
        }

        let assigned_by_db = uid_to_i32(assignment.assigned_by)?;
        let row = tx
            .query_one(
                r#"INSERT INTO reviewer_assignments (
                       id, change_request_id, reviewer, assigned_by, required, active, version
                   )
                   VALUES ($1, $2, $3, $4, $5, true, 1)
                   RETURNING id, change_request_id, reviewer, assigned_by, required, active, version"#,
                &[
                    &assignment.id,
                    &assignment.change_request_id,
                    &reviewer_db,
                    &assigned_by_db,
                    &assignment.required,
                ],
            )
            .await
            .map_err(|error| postgres_error("review assignment insert", error))?;
        let stored = row_to_review_assignment(row, &change)?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("review assignment commit", error))?;
        Ok(ReviewAssignmentMutation {
            assignment: stored,
            created: true,
            updated: false,
        })
    }

    async fn list_reviewer_assignments_for_repo(
        &self,
        repo_id: &RepoId,
        change_request_id: Uuid,
    ) -> Result<Vec<ReviewAssignment>, VfsError> {
        let client = self.connect_client().await?;
        let Some(change) = load_review_change_request(&client, repo_id, change_request_id).await?
        else {
            return Ok(vec![]);
        };
        let rows = client
            .query(
                r#"SELECT id, change_request_id, reviewer, assigned_by, required, active, version
                   FROM reviewer_assignments
                   WHERE change_request_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review assignment list", error))?;
        rows.into_iter()
            .map(|row| row_to_review_assignment(row, &change))
            .collect()
    }

    async fn create_comment_for_repo(
        &self,
        repo_id: &RepoId,
        input: NewReviewComment,
    ) -> Result<ReviewCommentMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review comment transaction", error))?;
        let change_row = tx
            .query_opt(
                r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&repo_id.as_str(), &input.change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review comment lock change", error))?;
        let Some(change_row) = change_row else {
            tx.rollback()
                .await
                .map_err(|error| postgres_error("review comment rollback", error))?;
            return Err(VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            });
        };
        let change = row_to_change_request(change_row)?;
        let comment = ReviewComment::new(input, &change)?;
        let author_db = uid_to_i32(comment.author)?;
        let kind = review_comment_kind_to_db(comment.kind);
        let row = tx
            .query_one(
                r#"INSERT INTO review_comments (
                       id, change_request_id, author, body, path, kind, active, version
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, true, 1)
                   RETURNING id, change_request_id, author, body, path, kind, active, version"#,
                &[
                    &comment.id,
                    &comment.change_request_id,
                    &author_db,
                    &comment.body,
                    &comment.path,
                    &kind,
                ],
            )
            .await
            .map_err(|error| postgres_error("review comment insert", error))?;
        let stored = row_to_review_comment(row, &change)?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("review comment commit", error))?;
        Ok(ReviewCommentMutation {
            comment: stored,
            created: true,
        })
    }

    async fn list_comments_for_repo(
        &self,
        repo_id: &RepoId,
        change_request_id: Uuid,
    ) -> Result<Vec<ReviewComment>, VfsError> {
        let client = self.connect_client().await?;
        let Some(change) = load_review_change_request(&client, repo_id, change_request_id).await?
        else {
            return Ok(vec![]);
        };
        let rows = client
            .query(
                r#"SELECT id, change_request_id, author, body, path, kind, active, version
                   FROM review_comments
                   WHERE change_request_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review comment list", error))?;
        rows.into_iter()
            .map(|row| row_to_review_comment(row, &change))
            .collect()
    }

    async fn dismiss_approval_for_repo(
        &self,
        repo_id: &RepoId,
        input: DismissApprovalInput,
    ) -> Result<ApprovalDismissalMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review dismiss transaction", error))?;

        let approval_identity = tx
            .query_opt(
                r#"SELECT change_request_id
                   FROM approvals
                   WHERE id = $1"#,
                &[&input.approval_id],
            )
            .await
            .map_err(|error| postgres_error("review dismiss inspect approval", error))?;
        let Some(approval_identity) = approval_identity else {
            return Err(VfsError::InvalidArgs {
                message: format!("unknown approval {}", input.approval_id),
            });
        };
        let change_request_id: Uuid = approval_identity.get("change_request_id");
        if change_request_id != input.change_request_id {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "approval {} does not belong to change request {}",
                    input.approval_id, input.change_request_id
                ),
            });
        }

        // Keep the lock order consistent with approval creation: change request
        // first, then approval row. Reversing it can deadlock duplicate approval
        // creation racing with dismissal of the active approval.
        let change_row = tx
            .query_opt(
                r#"SELECT id, repo_id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&repo_id.as_str(), &input.change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review dismiss lock change", error))?;
        let Some(change_row) = change_row else {
            return Err(VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            });
        };
        let change = row_to_change_request(change_row)?;
        let reason = normalize_dismissal_reason(input.reason)?;
        validate_change_request_open(&change)?;

        let approval_row = tx
            .query_opt(
                r#"SELECT id, change_request_id, head_commit, approved_by, comment, active,
                             dismissed_by, dismissal_reason, version
                   FROM approvals
                   WHERE id = $1
                   FOR UPDATE"#,
                &[&input.approval_id],
            )
            .await
            .map_err(|error| postgres_error("review dismiss lock approval", error))?;
        let Some(approval_row) = approval_row else {
            return Err(VfsError::CorruptStore {
                message: "review approval disappeared before dismissal lock".to_string(),
            });
        };
        let record = row_to_approval_record(approval_row, &change)?;
        if !record.active {
            tx.commit()
                .await
                .map_err(|error| postgres_error("review dismiss commit", error))?;
            return Ok(ApprovalDismissalMutation {
                record,
                dismissed: false,
            });
        }

        let next_version = record
            .version
            .checked_add(1)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: "approval version overflow".to_string(),
            })?;
        let version_i64 = u64_to_i64(next_version, "approval version")?;
        let dismissed_by_db = uid_to_i32(input.dismissed_by)?;
        let row = tx
            .query_opt(
                r#"UPDATE approvals
                   SET active = false,
                       dismissed_by = $1,
                       dismissal_reason = $2,
                       version = $3,
                       updated_at = now()
                   WHERE id = $4
                   RETURNING id, change_request_id, head_commit, approved_by, comment, active,
                             dismissed_by, dismissal_reason, version"#,
                &[&dismissed_by_db, &reason, &version_i64, &input.approval_id],
            )
            .await
            .map_err(|error| postgres_error("review dismiss update", error))?;
        let Some(row) = row else {
            tx.rollback()
                .await
                .map_err(|error| postgres_error("review dismiss rollback", error))?;
            return Err(VfsError::CorruptStore {
                message: "review dismiss update returned no row".to_string(),
            });
        };
        let updated = row_to_approval_record(row, &change)?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("review dismiss commit", error))?;
        Ok(ApprovalDismissalMutation {
            record: updated,
            dismissed: true,
        })
    }

    async fn approval_decision_for_repo(
        &self,
        repo_id: &RepoId,
        change_request_id: Uuid,
        changed_paths: &[String],
    ) -> Result<Option<ApprovalPolicyDecision>, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(|error| postgres_error("review decision transaction", error))?;
        let Some(change) = load_review_change_request(&tx, repo_id, change_request_id).await?
        else {
            tx.commit()
                .await
                .map_err(|error| postgres_error("review decision commit", error))?;
            return Ok(None);
        };

        let ref_rows = tx
            .query(
                r#"SELECT id, repo_id, ref_name, required_approvals, require_all_files_viewed, created_by, active
                   FROM protected_ref_rules
                   WHERE repo_id = $1
                   ORDER BY id ASC"#,
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("review decision ref rules", error))?;
        let protected_refs: Vec<ProtectedRefRule> = ref_rows
            .into_iter()
            .map(row_to_protected_ref_rule)
            .collect::<Result<Vec<_>, _>>()?;

        let path_rows = tx
            .query(
                r#"SELECT id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by, active
                   FROM protected_path_rules
                   WHERE repo_id = $1
                   ORDER BY id ASC"#,
                &[&repo_id.as_str()],
            )
            .await
            .map_err(|error| postgres_error("review decision path rules", error))?;
        let protected_paths: Vec<ProtectedPathRule> = path_rows
            .into_iter()
            .map(row_to_protected_path_rule)
            .collect::<Result<Vec<_>, _>>()?;

        let mut required_approvals = 0u32;
        let mut matched_ref_rules = Vec::new();
        let mut require_all_files_viewed = false;
        for rule in &protected_refs {
            if rule.active && rule.ref_name == change.target_ref {
                required_approvals = required_approvals.max(rule.required_approvals);
                matched_ref_rules.push(rule.id);
                require_all_files_viewed |= rule.require_all_files_viewed;
            }
        }

        let mut matched_path_rules = Vec::new();
        for rule in &protected_paths {
            let target_matches = rule
                .target_ref
                .as_ref()
                .is_none_or(|target_ref| target_ref == &change.target_ref);
            if rule.active
                && target_matches
                && changed_paths.iter().any(|path| rule.matches_path(path))
            {
                required_approvals = required_approvals.max(rule.required_approvals);
                matched_path_rules.push(rule.id);
                require_all_files_viewed |= rule.require_all_files_viewed;
            }
        }

        let approval_rows = tx
            .query(
                r#"SELECT id, change_request_id, head_commit, approved_by, comment, active,
                             dismissed_by, dismissal_reason, version
                   FROM approvals
                   WHERE change_request_id = $1 AND head_commit = $2 AND active = true
                   ORDER BY created_at ASC, id ASC"#,
                &[&change_request_id, &change.head_commit],
            )
            .await
            .map_err(|error| postgres_error("review decision approvals", error))?;

        let approved_by: BTreeSet<Uid> = approval_rows
            .into_iter()
            .map(|row| row_to_approval_record(row, &change))
            .collect::<Result<Vec<_>, VfsError>>()?
            .into_iter()
            .map(|record| record.approved_by)
            .collect();
        let approved_by: Vec<Uid> = approved_by.into_iter().collect();
        let approval_count = approved_by.len().try_into().unwrap_or(u32::MAX);
        let approved_by_set: BTreeSet<Uid> = approved_by.iter().copied().collect();

        let assignment_rows = tx
            .query(
                r#"SELECT id, change_request_id, reviewer, assigned_by, required, active, version
                   FROM reviewer_assignments
                   WHERE change_request_id = $1 AND active = true
                   ORDER BY created_at ASC, id ASC"#,
                &[&change_request_id],
            )
            .await
            .map_err(|error| postgres_error("review decision assignments", error))?;
        let required_reviewers: Vec<Uid> = assignment_rows
            .into_iter()
            .map(|row| row_to_review_assignment(row, &change))
            .collect::<Result<Vec<_>, VfsError>>()?
            .into_iter()
            .filter(|assignment| assignment.required)
            .map(|assignment| assignment.reviewer)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let (approved_required_reviewers, missing_required_reviewers): (Vec<_>, Vec<_>) =
            required_reviewers
                .iter()
                .copied()
                .partition(|reviewer| approved_by_set.contains(reviewer));
        let required_reviewers_satisfied = missing_required_reviewers.is_empty();

        let decision = ApprovalPolicyDecision {
            change_request_id,
            required_approvals,
            approval_count,
            approved_by,
            required_reviewers,
            approved_required_reviewers,
            missing_required_reviewers,
            approved: approval_count >= required_approvals && required_reviewers_satisfied,
            matched_ref_rules,
            matched_path_rules,
            require_all_files_viewed,
        };
        tx.commit()
            .await
            .map_err(|error| postgres_error("review decision commit", error))?;
        Ok(Some(decision))
    }
}

async fn check_source_expectation<C>(
    client: &C,
    repo_id: &RepoId,
    name: &RefName,
    expectation: RefExpectation,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    match expectation {
        RefExpectation::MustNotExist => Err(VfsError::NotSupported {
            message: "Postgres source-checked ref updates require an existing source ref"
                .to_string(),
        }),
        RefExpectation::Matches { target, version } => {
            let current = load_ref(client, repo_id, name).await?;
            match current {
                Some(record) if record.target == target && record.version == version => Ok(()),
                Some(_) | None => Err(ref_cas_mismatch(name)),
            }
        }
    }
}

fn row_to_ref_record(row: Row) -> Result<RefRecord, VfsError> {
    let repo_id: String = row.get("repo_id");
    let name: String = row.get("name");
    let commit_id: String = row.get("commit_id");
    let version: i64 = row.get("version");
    if version <= 0 {
        return Err(VfsError::CorruptStore {
            message: format!("ref {name} has invalid version {version}"),
        });
    }

    Ok(RefRecord {
        repo_id: RepoId::new(repo_id).map_err(corrupt_from_invalid)?,
        name: RefName::new(name).map_err(corrupt_from_invalid)?,
        target: parse_commit_id(&commit_id, "ref commit id")?,
        version: RefVersion::new(version as u64).map_err(corrupt_from_invalid)?,
    })
}

fn object_kind_to_db(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
    }
}

fn object_kind_from_db(kind: &str) -> Result<ObjectKind, VfsError> {
    match kind {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        _ => Err(VfsError::CorruptStore {
            message: format!("unknown object kind in Postgres metadata: {kind}"),
        }),
    }
}

fn cleanup_claim_kind_to_db(kind: ObjectCleanupClaimKind) -> &'static str {
    match kind {
        ObjectCleanupClaimKind::FinalObjectMetadataRepair => "final_object_metadata_repair",
        ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup => {
            "durable_mutation_cas_lost_object_cleanup"
        }
    }
}

fn cleanup_claim_kind_from_db(kind: &str) -> Result<ObjectCleanupClaimKind, VfsError> {
    match kind {
        "final_object_metadata_repair" => Ok(ObjectCleanupClaimKind::FinalObjectMetadataRepair),
        "durable_mutation_cas_lost_object_cleanup" => {
            Ok(ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup)
        }
        _ => Err(VfsError::CorruptStore {
            message: format!("unknown cleanup claim kind in Postgres metadata: {kind}"),
        }),
    }
}

fn cleanup_claim_state_from_db(state: &str) -> Result<ObjectCleanupClaimState, VfsError> {
    match state {
        "active" => Ok(ObjectCleanupClaimState::Active),
        "stale_active" => Ok(ObjectCleanupClaimState::StaleActive),
        "completed" => Ok(ObjectCleanupClaimState::Completed),
        "failed" => Ok(ObjectCleanupClaimState::Failed),
        _ => Err(VfsError::CorruptStore {
            message: format!("unknown cleanup claim state in Postgres metadata: {state}"),
        }),
    }
}

fn parse_object_id(hex: &str, label: &str) -> Result<ObjectId, VfsError> {
    ObjectId::from_hex(hex).map_err(|_| VfsError::CorruptStore {
        message: format!("invalid {label} in Postgres metadata: {hex}"),
    })
}

fn parse_commit_id(hex: &str, label: &str) -> Result<CommitId, VfsError> {
    parse_object_id(hex, label).map(CommitId::from)
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64, VfsError> {
    i64::try_from(value).map_err(|_| VfsError::InvalidArgs {
        message: format!("{label} exceeds Postgres BIGINT range"),
    })
}

fn i64_to_u32(value: i64, label: &str) -> Result<u32, VfsError> {
    u32::try_from(value).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn i64_to_usize(value: i64, label: &str) -> Result<usize, VfsError> {
    usize::try_from(value).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn i64_to_u64(value: i64, label: &str) -> Result<u64, VfsError> {
    u64::try_from(value).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64, VfsError> {
    i64::try_from(value).map_err(|_| VfsError::InvalidArgs {
        message: format!("{label} exceeds Postgres BIGINT range"),
    })
}

fn usize_to_i32(value: usize, label: &str) -> Result<i32, VfsError> {
    i32::try_from(value).map_err(|_| VfsError::InvalidArgs {
        message: format!("{label} exceeds Postgres INTEGER range"),
    })
}

fn duration_to_i64_millis(value: std::time::Duration, label: &str) -> Result<i64, VfsError> {
    let millis = value.as_millis();
    if millis == 0 {
        return Err(VfsError::InvalidArgs {
            message: format!("{label} must be at least 1 millisecond"),
        });
    }
    i64::try_from(millis).map_err(|_| VfsError::InvalidArgs {
        message: format!("{label} exceeds Postgres BIGINT millisecond range"),
    })
}

fn version_to_i64(version: RefVersion) -> Result<i64, VfsError> {
    u64_to_i64(version.value(), "ref version")
}

fn datetime_to_millis(value: DateTime<Utc>, label: &str) -> Result<u64, VfsError> {
    u64::try_from(value.timestamp_millis()).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn datetime_to_unix_seconds(value: DateTime<Utc>, label: &str) -> Result<u64, VfsError> {
    u64::try_from(value.timestamp()).map_err(|_| VfsError::CorruptStore {
        message: format!("{label} is outside supported range"),
    })
}

fn optional_datetime_to_millis(
    value: Option<DateTime<Utc>>,
    label: &str,
) -> Result<Option<u64>, VfsError> {
    value
        .map(|value| datetime_to_millis(value, label))
        .transpose()
}

fn optional_datetime_to_unix_seconds(
    value: Option<DateTime<Utc>>,
    label: &str,
) -> Result<Option<u64>, VfsError> {
    value
        .map(|value| datetime_to_unix_seconds(value, label))
        .transpose()
}

fn ref_cas_mismatch(name: &RefName) -> VfsError {
    VfsError::InvalidArgs {
        message: format!("ref compare-and-swap mismatch: {name}"),
    }
}

fn stale_post_cas_recovery_claim() -> VfsError {
    VfsError::InvalidArgs {
        message: "post-CAS recovery claim is stale".to_string(),
    }
}

fn stale_pre_visibility_recovery_claim() -> VfsError {
    VfsError::InvalidArgs {
        message: "pre-visibility recovery claim is stale".to_string(),
    }
}

fn stale_fs_mutation_recovery_claim() -> VfsError {
    VfsError::InvalidArgs {
        message: "durable FS mutation recovery claim is stale".to_string(),
    }
}

fn post_cas_recovery_context_to_json(
    context: &DurableCorePostCasRecoveryContext,
) -> Result<serde_json::Value, VfsError> {
    serde_json::to_value(context).map_err(|_| post_cas_recovery_context_corrupt())
}

fn post_cas_recovery_context_from_json(
    value: serde_json::Value,
) -> Result<DurableCorePostCasRecoveryContext, VfsError> {
    serde_json::from_value(value).map_err(|_| post_cas_recovery_context_corrupt())
}

fn post_cas_recovery_context_corrupt() -> VfsError {
    VfsError::CorruptStore {
        message: "post-CAS recovery context is corrupt".to_string(),
    }
}

fn pre_visibility_recovery_context_corrupt() -> VfsError {
    VfsError::CorruptStore {
        message: "pre-visibility recovery context is corrupt".to_string(),
    }
}

fn fs_mutation_recovery_envelope_to_json(
    envelope: &DurableFsMutationRecoveryEnvelope,
) -> Result<serde_json::Value, VfsError> {
    serde_json::to_value(envelope).map_err(|_| fs_mutation_recovery_envelope_corrupt())
}

fn fs_mutation_recovery_envelope_from_json(
    value: serde_json::Value,
) -> Result<DurableFsMutationRecoveryEnvelope, VfsError> {
    serde_json::from_value(value).map_err(|_| fs_mutation_recovery_envelope_corrupt())
}

fn fs_mutation_recovery_envelope_corrupt() -> VfsError {
    VfsError::CorruptStore {
        message: "durable FS mutation recovery envelope is corrupt".to_string(),
    }
}

fn fs_mutation_recovery_enqueue_conflict() -> VfsError {
    VfsError::CorruptStore {
        message: "durable FS mutation recovery target has conflicting envelope".to_string(),
    }
}

fn ref_version_overflow() -> VfsError {
    VfsError::CorruptStore {
        message: "ref version overflow".to_string(),
    }
}

fn corrupt_from_invalid(error: VfsError) -> VfsError {
    VfsError::CorruptStore {
        message: error.to_string(),
    }
}

pub(crate) fn postgres_error(context: &str, error: tokio_postgres::Error) -> VfsError {
    if let Some(db_error) = error.as_db_error() {
        if db_error.code() == &SqlState::QUERY_CANCELED {
            return postgres_timeout_error();
        }
        let constraint = db_error
            .constraint()
            .map(|constraint| format!(" constraint {constraint}"))
            .unwrap_or_default();
        let message = format!(
            "postgres {context} failed with SQLSTATE {}{}",
            db_error.code().code(),
            constraint
        );
        if db_error.code() == &SqlState::UNIQUE_VIOLATION {
            return VfsError::AlreadyExists { path: message };
        }
        if matches!(
            db_error.code(),
            &SqlState::FOREIGN_KEY_VIOLATION
                | &SqlState::CHECK_VIOLATION
                | &SqlState::NOT_NULL_VIOLATION
                | &SqlState::INVALID_TEXT_REPRESENTATION
                | &SqlState::NUMERIC_VALUE_OUT_OF_RANGE
        ) {
            return VfsError::InvalidArgs { message };
        }
        return VfsError::CorruptStore { message };
    }

    VfsError::IoError(std::io::Error::other(format!("postgres {context} failed")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use crate::audit::{
        AuditAction, AuditActor, AuditOutcome, AuditResource, AuditResourceKind, AuditStore,
        AuditWorkspaceContext, NewAuditEvent,
    };
    use crate::auth::ROOT_UID;
    use crate::backend::blob_object::{
        BlobObjectStore, ObjectMetadataRecord, ObjectMetadataStore, ObjectOrphanCleanupMode,
        object_key,
    };
    use crate::backend::core_transaction::{
        DurableCorePostCasIdempotencyRecoveryContext, DurableCorePostCasIdempotencyResponseKind,
        DurableFsMutationAuditRecoveryContext, DurableFsMutationIdempotencyRecoveryContext,
    };
    use crate::backend::object_cleanup::{
        FinalObjectDeletionReadiness, FinalObjectDeletionSnapshot, ObjectCleanupClaim,
        ObjectCleanupClaimKind, ObjectCleanupClaimRequest, ObjectCleanupClaimState,
        ObjectCleanupClaimStore,
    };
    use crate::backend::runtime::{DurablePostgresRuntimePosture, PostgresTlsRuntimeMode};
    use crate::backend::{CommitRecord, CommitStore, ObjectStore, ObjectWrite, RepoId};
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyStore, request_fingerprint,
    };
    use crate::remote::blob::{LocalBlobStore, RemoteBlobStore};
    use crate::review::{
        ApprovalRecordMutation, ChangeRequestStatus, DismissApprovalInput, NewApprovalRecord,
        NewChangeRequest, NewReviewAssignment, NewReviewComment, ReviewCommentKind, ReviewStore,
    };
    use crate::vcs::{ChangeKind, MAIN_REF, PathKind, PathRecord};
    use crate::workspace::WorkspaceMetadataStore;
    use axum::http::HeaderValue;
    use serde_json::json;
    use tokio::sync::Barrier;
    use uuid::Uuid;

    struct TestDb {
        config: Config,
        schema: String,
        store: PostgresMetadataStore,
    }

    #[test]
    fn hosted_sslmode_require_builds_tls_connector_without_connecting() {
        let config: Config = "postgresql://db.internal/stratum?sslmode=require"
            .parse()
            .expect("parse hosted postgres config");
        let posture = DurablePostgresRuntimePosture::local_defaults()
            .with_tls_mode(PostgresTlsRuntimeMode::HostedTlsRequired);

        let connector = PostgresConnector::new(config, posture)
            .expect("hosted TLS connector should build without opening a socket");

        assert_eq!(
            connector.tls_mode(),
            PostgresTlsRuntimeMode::HostedTlsRequired
        );
        assert_eq!(
            connector.connect_timeout(),
            DurablePostgresRuntimePosture::local_defaults().connect_timeout()
        );
        assert!(connector.uses_tls());
    }

    #[test]
    fn remote_no_tls_connector_configs_reject_without_leaking_target() {
        let config: Config = "postgresql://raw-db-host.internal/stratum"
            .parse()
            .expect("parse hosted postgres config");
        let posture = DurablePostgresRuntimePosture::local_defaults();

        let err = PostgresConnector::new(config, posture)
            .expect_err("remote Postgres without sslmode=require should fail closed");
        let message = err.to_string();

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains("sslmode=require"));
        assert!(!message.contains("raw-db-host.internal"));
    }

    #[tokio::test]
    async fn direct_connect_with_schema_rejects_remote_no_tls_without_leaking_target() {
        let config: Config = "postgresql://raw-direct-host.internal/stratum"
            .parse()
            .expect("parse hosted postgres config");

        let err = connect_with_schema(&config, None)
            .await
            .expect_err("direct helper should reject remote no-TLS before connect");
        let message = err.to_string();

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains("sslmode=require"));
        assert!(!message.contains("raw-direct-host.internal"));
    }

    #[tokio::test]
    async fn direct_metadata_store_rejects_remote_no_tls_without_leaking_target() {
        let config: Config = "postgresql://raw-store-host.internal/stratum"
            .parse()
            .expect("parse hosted postgres config");
        let store = PostgresMetadataStore::new(config);

        let err = store
            .ensure_control_plane_ready()
            .await
            .expect_err("direct metadata store should reject remote no-TLS before connect");
        let message = err.to_string();

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains("sslmode=require"));
        assert!(!message.contains("raw-store-host.internal"));
    }

    #[tokio::test]
    async fn pooled_connector_reuses_checked_in_connection() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let posture = DurablePostgresRuntimePosture::for_test(
            1,
            Duration::from_millis(5000),
            Duration::from_millis(5000),
            Duration::from_millis(5000),
            infer_tls_mode(&db.config),
        );
        let connector = PostgresConnector::new(db.config.clone(), posture).expect("build pool");

        let first = connector
            .connect_with_schema(Some(&db.schema))
            .await
            .expect("first pooled checkout");
        let first_pid: i32 = first
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("read first backend pid")
            .get(0);
        drop(first);

        let second = connector
            .connect_with_schema(Some(&db.schema))
            .await
            .expect("second pooled checkout");
        let second_pid: i32 = second
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("read second backend pid")
            .get(0);

        assert_eq!(
            first_pid, second_pid,
            "max-size-one pool should reuse the checked-in Postgres connection"
        );

        db.cleanup().await;
    }

    #[tokio::test]
    async fn pooled_connector_acquire_timeout_is_bounded_with_checked_out_client() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let posture = DurablePostgresRuntimePosture::for_test(
            1,
            Duration::from_millis(5000),
            Duration::from_millis(5000),
            Duration::from_millis(1),
            infer_tls_mode(&db.config),
        );
        let connector = PostgresConnector::new(db.config.clone(), posture).expect("build pool");
        let _held = connector
            .connect_with_schema(Some(&db.schema))
            .await
            .expect("hold only pooled client");

        let err = connector
            .connect_with_schema(Some(&db.schema))
            .await
            .expect_err("second checkout should time out");

        assert!(matches!(err, VfsError::IoError(_)));
        assert!(err.to_string().contains("postgres operation timed out"));

        db.cleanup().await;
    }

    #[tokio::test]
    async fn statement_timeout_query_maps_to_fixed_timeout_error() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let posture = DurablePostgresRuntimePosture::for_test(
            1,
            Duration::from_millis(5000),
            Duration::from_millis(1000),
            Duration::from_millis(5000),
            infer_tls_mode(&db.config),
        );
        let connector = PostgresConnector::new(db.config.clone(), posture).expect("build pool");
        let client = connector
            .connect_with_schema(Some(&db.schema))
            .await
            .expect("checkout pooled client");

        let err = client
            .query_one("SELECT pg_sleep(2)", &[])
            .await
            .map_err(|error| postgres_error("timed test query", error))
            .expect_err("statement timeout should cancel the query");

        assert!(matches!(err, VfsError::IoError(_)));
        assert!(err.to_string().contains("postgres operation timed out"));
        assert!(!err.to_string().contains("pg_sleep"));

        db.cleanup().await;
    }

    #[tokio::test]
    async fn pooled_connector_resets_search_path_for_schema_none() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let posture = DurablePostgresRuntimePosture::for_test(
            1,
            Duration::from_millis(5000),
            Duration::from_millis(5000),
            Duration::from_millis(5000),
            infer_tls_mode(&db.config),
        );
        let connector = PostgresConnector::new(db.config.clone(), posture).expect("build pool");

        let schema_client = connector
            .connect_with_schema(Some(&db.schema))
            .await
            .expect("checkout schema-scoped client");
        let schema_path: String = schema_client
            .query_one("SHOW search_path", &[])
            .await
            .expect("read schema search_path")
            .get(0);
        assert!(schema_path.contains(&db.schema));
        drop(schema_client);

        let default_client = connector
            .connect_with_schema(None)
            .await
            .expect("checkout default-search-path client");
        let default_path: String = default_client
            .query_one("SHOW search_path", &[])
            .await
            .expect("read reset search_path")
            .get(0);

        assert!(
            !default_path.contains(&db.schema),
            "pooled schema=None checkout inherited search_path {default_path}"
        );

        db.cleanup().await;
    }

    #[test]
    fn localhost_connector_configs_use_no_tls_path() {
        for url in [
            "postgresql://localhost/stratum",
            "postgresql://127.0.0.1/stratum",
            "postgresql://[::1]/stratum",
        ] {
            let config: Config = url.parse().expect("parse local postgres config");
            let connector =
                PostgresConnector::new(config, DurablePostgresRuntimePosture::local_defaults())
                    .expect("localhost config should use local connector path");

            assert_eq!(connector.tls_mode(), PostgresTlsRuntimeMode::LocalNoTls);
            assert!(!connector.uses_tls());
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_connector_configs_use_no_tls_path() {
        let config: Config = "host=/var/run/postgresql dbname=stratum"
            .parse()
            .expect("parse unix socket postgres config");

        let connector =
            PostgresConnector::new(config, DurablePostgresRuntimePosture::local_defaults())
                .expect("Unix socket config should use local connector path");

        assert_eq!(connector.tls_mode(), PostgresTlsRuntimeMode::LocalNoTls);
        assert!(!connector.uses_tls());
    }

    #[test]
    fn postgres_connection_debug_output_redacts_sensitive_config_material() {
        let mut config: Config =
            "postgresql://raw-db-user@raw-db-host.internal/stratum?sslmode=require"
                .parse()
                .expect("parse hosted postgres config");
        config.password("raw-db-password-123");
        let posture = DurablePostgresRuntimePosture::local_defaults()
            .with_tls_mode(PostgresTlsRuntimeMode::HostedTlsRequired);

        let connector = PostgresConnector::new(config, posture)
            .expect("hosted TLS connector should build without opening a socket");
        let debug = format!("{connector:?}");

        for secret in [
            "STRATUM_POSTGRES_URL",
            "PGPASSWORD",
            "raw-db-host.internal",
            "raw-db-user",
            "raw-db-password-123",
            "SELECT raw_sql_secret",
        ] {
            assert!(
                !debug.contains(secret),
                "debug output leaked sensitive material: {secret}"
            );
        }
    }

    struct TempBlobDir {
        path: PathBuf,
    }

    impl TempBlobDir {
        fn new(label: &str) -> Self {
            Self {
                path: std::env::temp_dir()
                    .join(format!("stratum-postgres-{label}-{}", Uuid::new_v4())),
            }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempBlobDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct CleanupClaimRow {
        claim: ObjectCleanupClaim,
        completed_at: Option<SystemTime>,
        last_error: Option<String>,
    }

    impl TestDb {
        async fn new() -> Option<Self> {
            let Some(url) = std::env::var("STRATUM_POSTGRES_TEST_URL").ok() else {
                if postgres_tests_required() {
                    panic!("STRATUM_POSTGRES_TEST_URL is required for Postgres backend tests");
                }
                eprintln!("skipping Postgres backend tests; STRATUM_POSTGRES_TEST_URL is unset");
                return None;
            };

            let mut config: Config = url
                .parse()
                .expect("STRATUM_POSTGRES_TEST_URL should parse as a Postgres config");
            if config.get_password().is_some() {
                panic!(
                    "STRATUM_POSTGRES_TEST_URL must not include a password; use STRATUM_POSTGRES_TEST_PASSWORD or PGPASSWORD"
                );
            }
            if let Ok(password) = std::env::var("STRATUM_POSTGRES_TEST_PASSWORD")
                .or_else(|_| std::env::var("PGPASSWORD"))
            {
                config.password(password);
            }

            let schema =
                validate_schema_name(format!("stratum_pg_{}", Uuid::new_v4().simple())).unwrap();
            let client = connect_with_schema(&config, None)
                .await
                .expect("connect test Postgres");
            client
                .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(&schema)))
                .await
                .expect("create isolated schema");
            client
                .batch_execute(&format!("SET search_path TO {}", quote_identifier(&schema)))
                .await
                .expect("set isolated schema search_path");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0001_durable_backend_foundation.sql"
                ))
                .await
                .expect("apply durable backend migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0002_review_local_commit_ids.sql"
                ))
                .await
                .expect("apply review local commit-id migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0003_guarded_commit_recovery_claims.sql"
                ))
                .await
                .expect("apply guarded commit recovery claims migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0004_guarded_commit_recovery_context.sql"
                ))
                .await
                .expect("apply guarded commit recovery context migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0005_guarded_commit_pre_visibility_recovery.sql"
                ))
                .await
                .expect("apply guarded commit pre-visibility recovery migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0006_pre_visibility_recovery_run_control.sql"
                ))
                .await
                .expect("apply pre-visibility recovery run-control migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0007_durable_fs_mutation_recovery.sql"
                ))
                .await
                .expect("apply durable FS mutation recovery migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0008_durable_mutation_cleanup_claim_kind.sql"
                ))
                .await
                .expect("apply durable mutation cleanup claim kind migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0009_durable_auth_session_foundation.sql"
                ))
                .await
                .expect("apply durable auth session foundation migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0010_object_deletion_fences.sql"
                ))
                .await
                .expect("apply object deletion fences migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0011_idempotency_retention_quota.sql"
                ))
                .await
                .expect("apply idempotency retention quota migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0012_object_cleanup_deletion_state.sql"
                ))
                .await
                .expect("apply object cleanup deletion state migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0013_protected_rules_require_all_files_viewed.sql"
                ))
                .await
                .expect("apply protected rules require-all-files-viewed migration");
            client
                .batch_execute(include_str!(
                    "../../migrations/postgres/0014_secret_bearing_idempotency_replay.sql"
                ))
                .await
                .expect("apply secret-bearing idempotency replay migration");

            let posture = DurablePostgresRuntimePosture::for_test(
                32,
                Duration::from_millis(10_000),
                Duration::from_millis(120_000),
                Duration::from_millis(120_000),
                infer_tls_mode(&config),
            );
            let store = PostgresMetadataStore::with_schema_and_posture(
                config.clone(),
                schema.clone(),
                posture,
            )
            .unwrap();
            Some(Self {
                config,
                schema,
                store,
            })
        }

        fn independent_store(&self) -> PostgresMetadataStore {
            let posture = DurablePostgresRuntimePosture::for_test(
                32,
                Duration::from_millis(10_000),
                Duration::from_millis(120_000),
                Duration::from_millis(120_000),
                infer_tls_mode(&self.config),
            );
            PostgresMetadataStore::with_schema_and_posture(
                self.config.clone(),
                self.schema.clone(),
                posture,
            )
            .expect("build independent test Postgres store")
        }

        async fn cleanup(self) {
            if let Ok(client) = connect_with_schema(&self.config, None).await {
                let _ = client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS {} CASCADE",
                        quote_identifier(&self.schema)
                    ))
                    .await;
            }
        }
    }

    fn postgres_tests_required() -> bool {
        std::env::var("STRATUM_POSTGRES_TEST_REQUIRED").as_deref() == Ok("1")
            || std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
    }

    fn repo(name: &str) -> RepoId {
        RepoId::new(name).unwrap()
    }

    fn audit_event(label: &str) -> NewAuditEvent {
        NewAuditEvent::new(
            AuditActor::new(ROOT_UID, "root"),
            AuditAction::FsWriteFile,
            AuditResource::path(AuditResourceKind::File, format!("/docs/{label}.md")),
        )
        .with_detail("content_hash", format!("{label}-hash"))
    }

    fn audit_workspace_context() -> AuditWorkspaceContext {
        AuditWorkspaceContext {
            id: Uuid::from_u128(0x5354_5241_5455_4d00_0000_0000_0000_0001),
            root_path: "/workspaces/demo".to_string(),
            base_ref: "main".to_string(),
            session_ref: Some("agents/demo/session".to_string()),
        }
    }

    fn workspace_audit_event(label: &str) -> NewAuditEvent {
        audit_event(label)
            .with_workspace(audit_workspace_context())
            .with_outcome(AuditOutcome::Partial)
            .with_detail("workspace_id", label)
    }

    fn post_cas_audit_event(commit_id: CommitId) -> NewAuditEvent {
        NewAuditEvent::new(
            AuditActor::new(ROOT_UID, "context-private-user"),
            AuditAction::VcsCommit,
            AuditResource::id(AuditResourceKind::Commit, commit_id.to_hex()),
        )
        .with_detail("context-private-detail", "context-secret")
    }

    fn post_cas_recovery_context(commit_id: CommitId) -> DurableCorePostCasRecoveryContext {
        post_cas_recovery_context_with_response_kind(
            commit_id,
            DurableCorePostCasIdempotencyResponseKind::Partial,
        )
    }

    fn post_cas_recovery_context_with_response_kind(
        commit_id: CommitId,
        response_kind: DurableCorePostCasIdempotencyResponseKind,
    ) -> DurableCorePostCasRecoveryContext {
        DurableCorePostCasRecoveryContext::new(
            Some(Uuid::from_u128(0x5354_5241_5455_4d00_0000_0000_0000_0002)),
            Some(commit_id.to_hex()),
            Some(post_cas_audit_event(commit_id)),
            Some(DurableCorePostCasIdempotencyRecoveryContext::new(
                "vcs:commit",
                "context-key-hash",
                "context-request-fingerprint",
                "context-reservation-token",
                response_kind,
            )),
        )
    }

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    fn workspace_head(label: &str) -> String {
        object_id(label.as_bytes()).to_hex()
    }

    #[tokio::test]
    async fn postgres_advisory_xact_lock_contention_returns_false_across_independent_clients() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let key = PostgresAdvisoryXactLockKey::new(0x5354_0008, 3);
        let mut first_client = db
            .store
            .connect_client()
            .await
            .expect("connect first advisory lock client");
        let second_store = db.independent_store();
        let mut second_client = second_store
            .connect_client()
            .await
            .expect("connect second advisory lock client");
        let first_tx = first_client
            .transaction()
            .await
            .expect("begin first advisory lock transaction");
        let second_tx = second_client
            .transaction()
            .await
            .expect("begin second advisory lock transaction");

        postgres_advisory_xact_lock(&first_tx, key, "advisory lock contention")
            .await
            .expect("first client should acquire advisory lock");
        let acquired = postgres_try_advisory_xact_lock(&second_tx, key, "advisory lock contention")
            .await
            .expect("second client should try advisory lock");

        assert!(!acquired);

        second_tx
            .rollback()
            .await
            .expect("rollback second advisory lock transaction");
        first_tx
            .rollback()
            .await
            .expect("rollback first advisory lock transaction");
        db.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_advisory_xact_lock_releases_on_transaction_end() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let key = PostgresAdvisoryXactLockKey::new(0x5354_0008, 4);
        let mut first_client = db
            .store
            .connect_client()
            .await
            .expect("connect first advisory lock client");
        let second_store = db.independent_store();
        let mut second_client = second_store
            .connect_client()
            .await
            .expect("connect second advisory lock client");
        let first_tx = first_client
            .transaction()
            .await
            .expect("begin first advisory lock transaction");
        let second_tx = second_client
            .transaction()
            .await
            .expect("begin second advisory lock transaction");

        postgres_advisory_xact_lock(&first_tx, key, "advisory lock release")
            .await
            .expect("first client should acquire advisory lock");
        let blocked = postgres_try_advisory_xact_lock(&second_tx, key, "advisory lock release")
            .await
            .expect("second client should be blocked before release");
        first_tx
            .rollback()
            .await
            .expect("rollback first advisory lock transaction");
        let acquired = postgres_try_advisory_xact_lock(&second_tx, key, "advisory lock release")
            .await
            .expect("second client should acquire after release");

        assert!(!blocked);
        assert!(acquired);

        second_tx
            .rollback()
            .await
            .expect("rollback second advisory lock transaction");
        db.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_advisory_xact_lock_distinct_keys_do_not_conflict() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let first_key = PostgresAdvisoryXactLockKey::from_subject(0x5354_0008, "advisory:first");
        let second_key = PostgresAdvisoryXactLockKey::from_subject(0x5354_0008, "advisory:second");
        let mut first_client = db
            .store
            .connect_client()
            .await
            .expect("connect first advisory lock client");
        let second_store = db.independent_store();
        let mut second_client = second_store
            .connect_client()
            .await
            .expect("connect second advisory lock client");
        let first_tx = first_client
            .transaction()
            .await
            .expect("begin first advisory lock transaction");
        let second_tx = second_client
            .transaction()
            .await
            .expect("begin second advisory lock transaction");

        postgres_advisory_xact_lock(&first_tx, first_key, "advisory distinct keys")
            .await
            .expect("first client should acquire first advisory key");
        let acquired =
            postgres_try_advisory_xact_lock(&second_tx, second_key, "advisory distinct keys")
                .await
                .expect("second client should acquire distinct advisory key");

        assert!(acquired);

        second_tx
            .rollback()
            .await
            .expect("rollback second advisory lock transaction");
        first_tx
            .rollback()
            .await
            .expect("rollback first advisory lock transaction");
        db.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_advisory_xact_lock_failure_message_is_redacted() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let key = PostgresAdvisoryXactLockKey::from_subject(
            0x5354_0008,
            "secret advisory subject should not leak",
        );
        let client = db
            .store
            .connect_client()
            .await
            .expect("connect advisory lock client");
        let backend_pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .expect("read backend pid")
            .get(0);
        let killer = db
            .independent_store()
            .connect_client()
            .await
            .expect("connect backend terminator");
        let terminated: bool = killer
            .query_one("SELECT pg_terminate_backend($1)", &[&backend_pid])
            .await
            .expect("terminate advisory lock backend")
            .get(0);
        assert!(terminated);

        let err = postgres_try_advisory_xact_lock(&client, key, "redacted advisory lock failure")
            .await
            .expect_err("closed advisory lock client should fail");
        let message = err.to_string();

        assert!(message.contains("redacted advisory lock failure"));
        for leaked in [
            "pg_try_advisory_xact_lock",
            "5354",
            "secret advisory subject",
            &db.schema,
            "terminating connection",
            "server closed the connection",
        ] {
            assert!(
                !message.contains(leaked),
                "advisory lock error leaked sensitive material: {leaked}"
            );
        }

        db.cleanup().await;
    }

    fn commit_id(name: &str) -> CommitId {
        CommitId::from(object_id(name.as_bytes()))
    }

    fn object_record(
        repo_id: &RepoId,
        id: ObjectId,
        kind: ObjectKind,
        bytes: &[u8],
    ) -> ObjectMetadataRecord {
        ObjectMetadataRecord::new(repo_id.clone(), id, kind, bytes.len() as u64)
    }

    fn cleanup_request(
        repo_id: &RepoId,
        object_id: ObjectId,
        lease_duration: Duration,
    ) -> ObjectCleanupClaimRequest {
        cleanup_claim_request_for_object(repo_id, ObjectKind::Blob, object_id, lease_duration)
    }

    fn durable_cleanup_request(
        repo_id: &RepoId,
        object_id: ObjectId,
        lease_duration: Duration,
    ) -> ObjectCleanupClaimRequest {
        ObjectCleanupClaimRequest {
            claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
            ..cleanup_claim_request_for_object(repo_id, ObjectKind::Blob, object_id, lease_duration)
        }
    }

    fn cleanup_claim_request_for_object(
        repo_id: &RepoId,
        kind: ObjectKind,
        object_id: ObjectId,
        lease_duration: Duration,
    ) -> ObjectCleanupClaimRequest {
        ObjectCleanupClaimRequest {
            repo_id: repo_id.clone(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: kind,
            object_id,
            object_key: object_key(repo_id, kind, &object_id),
            lease_owner: "postgres-worker".to_string(),
            lease_duration,
        }
    }

    fn deletion_readiness_for_claim(
        claim: &ObjectCleanupClaim,
        deletion_ready_at: SystemTime,
        delete_after: SystemTime,
    ) -> FinalObjectDeletionReadiness {
        deletion_readiness_for_claim_with_size(claim, deletion_ready_at, delete_after, 42)
    }

    fn deletion_readiness_for_claim_with_size(
        claim: &ObjectCleanupClaim,
        deletion_ready_at: SystemTime,
        delete_after: SystemTime,
        size: u64,
    ) -> FinalObjectDeletionReadiness {
        FinalObjectDeletionReadiness {
            deletion_ready_at,
            delete_after,
            snapshot: FinalObjectDeletionSnapshot {
                object_key: claim.object_key.clone(),
                size,
                sha256: claim.object_id.to_hex(),
            },
        }
    }

    async fn cleanup_status_for_claim(
        store: &PostgresMetadataStore,
        claim: &ObjectCleanupClaim,
    ) -> Result<ObjectCleanupClaimStatus, VfsError> {
        ObjectCleanupClaimStore::list_for_repo(store, &claim.repo_id, 16)
            .await?
            .into_iter()
            .find(|status| {
                status.claim_kind() == claim.claim_kind && status.object_key() == claim.object_key
            })
            .ok_or_else(|| VfsError::CorruptStore {
                message: "cleanup claim status missing from test list".to_string(),
            })
    }

    fn commit_record(
        repo_id: &RepoId,
        id: CommitId,
        root_tree: ObjectId,
        parents: Vec<CommitId>,
        timestamp: u64,
        message: &str,
    ) -> CommitRecord {
        let path_record = PathRecord {
            path: "/docs/readme.md".to_string(),
            kind: PathKind::File,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            size: 5,
            content_id: Some(object_id(b"content")),
            mime_type: Some("text/markdown".to_string()),
            custom_attrs: Default::default(),
        };
        CommitRecord {
            repo_id: repo_id.clone(),
            id,
            root_tree,
            parents,
            timestamp,
            message: message.to_string(),
            author: "agent".to_string(),
            changed_paths: vec![ChangedPath {
                path: "/docs/readme.md".to_string(),
                kind: ChangeKind::Added,
                before: None,
                after: Some(path_record),
            }],
        }
    }

    async fn assert_workspace_storage_shape(
        store: &PostgresMetadataStore,
        workspace_id: Uuid,
    ) -> Result<(), VfsError> {
        let client = store.connect_client().await?;
        let row = client
            .query_one(
                r#"SELECT repo_id, name, root_path, version, base_ref, session_ref
               FROM workspaces
               WHERE id = $1"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("load workspace storage shape", error))?;

        assert!(row.get::<_, Option<String>>("repo_id").is_none());
        assert_eq!(row.get::<_, String>("name"), "alpha");
        assert_eq!(row.get::<_, String>("root_path"), "/alpha");
        assert_eq!(row.get::<_, i64>("version"), 3);
        assert_eq!(row.get::<_, String>("base_ref"), "main");
        assert_eq!(
            row.get::<_, Option<String>>("session_ref").as_deref(),
            Some("agent/demo/session")
        );
        Ok(())
    }

    async fn assert_workspace_ref_corruption_is_rejected(
        store: &PostgresMetadataStore,
    ) -> Result<(), VfsError> {
        let client = store.connect_client().await?;

        let bad_base_ref_id = Uuid::new_v4();
        client
            .execute(
                r#"INSERT INTO workspaces (
                       id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   )
                   VALUES ($1, NULL, 'bad-base-ref', '/bad-base-ref', NULL, 0, 'bad ref', NULL)"#,
                &[&bad_base_ref_id],
            )
            .await
            .map_err(|error| postgres_error("insert invalid workspace base ref", error))?;
        let err = WorkspaceMetadataStore::get_workspace(store, bad_base_ref_id)
            .await
            .expect_err("invalid base ref should be reported as corrupt");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        client
            .execute("DELETE FROM workspaces WHERE id = $1", &[&bad_base_ref_id])
            .await
            .map_err(|error| postgres_error("delete invalid workspace base ref", error))?;

        let bad_session_ref_id = Uuid::new_v4();
        client
            .execute(
                r#"INSERT INTO workspaces (
                       id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   )
                   VALUES ($1, NULL, 'bad-session-ref', '/bad-session-ref', NULL, 0, 'main', 'main')"#,
                &[&bad_session_ref_id],
            )
            .await
            .map_err(|error| postgres_error("insert invalid workspace session ref", error))?;
        let err = WorkspaceMetadataStore::get_workspace(store, bad_session_ref_id)
            .await
            .expect_err("invalid session ref should be reported as corrupt");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        client
            .execute(
                "DELETE FROM workspaces WHERE id = $1",
                &[&bad_session_ref_id],
            )
            .await
            .map_err(|error| postgres_error("delete invalid workspace session ref", error))?;

        Ok(())
    }

    async fn assert_workspace_token_storage_shape(
        store: &PostgresMetadataStore,
        token_id: Uuid,
        raw_secret: &str,
    ) -> Result<(), VfsError> {
        let client = store.connect_client().await?;
        let row = client
            .query_one(
                r#"SELECT repo_id, principal_uid, token_version, issued_at, updated_at,
                          expires_at, revoked_at, secret_hash,
                          read_prefixes_json, write_prefixes_json
               FROM workspace_tokens
               WHERE id = $1"#,
                &[&token_id],
            )
            .await
            .map_err(|error| postgres_error("load workspace token storage shape", error))?;

        let secret_hash: String = row.get("secret_hash");
        assert!(!secret_hash.eq(raw_secret));
        assert!(is_lower_hex_sha256(&secret_hash));
        assert!(row.get::<_, Option<String>>("repo_id").is_none());
        assert_eq!(row.get::<_, Option<i32>>("principal_uid"), Some(42));
        assert_eq!(row.get::<_, i64>("token_version"), 1);
        let issued_at: DateTime<Utc> = row.get("issued_at");
        let updated_at: DateTime<Utc> = row.get("updated_at");
        assert!(issued_at.timestamp() > 0);
        assert!(updated_at.timestamp() > 0);
        assert!(row.get::<_, Option<DateTime<Utc>>>("expires_at").is_none());
        assert!(row.get::<_, Option<DateTime<Utc>>>("revoked_at").is_none());

        let Json(read_prefixes): Json<Vec<String>> = row.get("read_prefixes_json");
        let Json(write_prefixes): Json<Vec<String>> = row.get("write_prefixes_json");
        assert_eq!(read_prefixes, vec!["/alpha", "/alpha/docs"]);
        assert_eq!(write_prefixes, vec!["/alpha/docs"]);
        Ok(())
    }

    #[tokio::test]
    async fn postgres_metadata_store_round_trips_backend_contracts() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result = run_backend_contracts(&test_db.store).await;
        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_fs_mutation_claim_is_atomic_across_independent_store_handles() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result: Result<(), VfsError> = async {
            let first_store = test_db.independent_store();
            let second_store = test_db.independent_store();
            let repo_id = repo("repo_pg_fs_claim_race");
            let previous_commit = commit_id("postgres-fs-claim-race-previous");
            let new_commit = commit_id("postgres-fs-claim-race-new");
            let previous_tree = object_id(b"postgres-fs-claim-race-previous-tree");
            let new_tree = object_id(b"postgres-fs-claim-race-new-tree");
            ObjectMetadataStore::put(
                &test_db.store,
                object_record(
                    &repo_id,
                    previous_tree,
                    ObjectKind::Tree,
                    b"postgres-fs-claim-race-previous-tree",
                ),
            )
            .await?;
            ObjectMetadataStore::put(
                &test_db.store,
                object_record(
                    &repo_id,
                    new_tree,
                    ObjectKind::Tree,
                    b"postgres-fs-claim-race-new-tree",
                ),
            )
            .await?;
            CommitStore::insert(
                &test_db.store,
                commit_record(
                    &repo_id,
                    previous_commit,
                    previous_tree,
                    Vec::new(),
                    1,
                    "postgres FS claim race previous",
                ),
            )
            .await?;
            CommitStore::insert(
                &test_db.store,
                commit_record(
                    &repo_id,
                    new_commit,
                    new_tree,
                    vec![previous_commit],
                    2,
                    "postgres FS claim race new",
                ),
            )
            .await?;
            let target = DurableFsMutationRecoveryTarget::new(
                repo_id,
                "fs:postgres-claim-race",
                "postgres-fs-claim-race",
                "agent/postgres/race",
                previous_commit,
                new_commit,
                DurableFsMutationRecoveryStep::AuditAppend,
            )?;
            DurableFsMutationRecoveryStore::enqueue(
                &test_db.store,
                target.clone(),
                DurableFsMutationRecoveryEnvelope::new(
                    None,
                    Some(DurableFsMutationAuditRecoveryContext::new(
                        AuditAction::FsWriteFile,
                        &["/postgres/claim-race.txt"],
                    )?),
                    None,
                ),
                100,
            )
            .await?;

            let barrier = Arc::new(Barrier::new(2));
            let first_barrier = barrier.clone();
            let first_target = target.clone();
            let first_claim = async move {
                first_barrier.wait().await;
                DurableFsMutationRecoveryStore::claim(
                    &first_store,
                    DurableFsMutationRecoveryClaimRequest::new(
                        first_target,
                        "postgres-node-a",
                        Duration::from_secs(30),
                        200,
                    )?,
                )
                .await
            };
            let second_barrier = barrier.clone();
            let second_target = target.clone();
            let second_claim = async move {
                second_barrier.wait().await;
                DurableFsMutationRecoveryStore::claim(
                    &second_store,
                    DurableFsMutationRecoveryClaimRequest::new(
                        second_target,
                        "postgres-node-b",
                        Duration::from_secs(30),
                        200,
                    )?,
                )
                .await
            };

            let (first_claim, second_claim) = tokio::join!(first_claim, second_claim);
            let first_claim = first_claim?;
            let second_claim = second_claim?;
            assert_eq!(
                usize::from(first_claim.is_some()) + usize::from(second_claim.is_some()),
                1,
                "exactly one independent Postgres worker should acquire the lease"
            );
            let winning_claim = first_claim
                .or(second_claim)
                .expect("one racing claim should win");
            DurableFsMutationRecoveryStore::complete(&test_db.store, &winning_claim, 201).await?;
            let counts = DurableFsMutationRecoveryStore::counts(&test_db.store).await?;
            assert_eq!(counts.completed(), 1);
            assert_eq!(
                DurableFsMutationRecoveryStore::claim(
                    &test_db.store,
                    DurableFsMutationRecoveryClaimRequest::new(
                        target,
                        "postgres-node-c",
                        Duration::from_secs(30),
                        202,
                    )?,
                )
                .await?,
                None
            );

            Ok(())
        }
        .await;
        test_db.cleanup().await;
        result.unwrap();
    }

    async fn run_cleanup_claim_contracts(
        store: &PostgresMetadataStore,
        repo_id: &RepoId,
    ) -> Result<(), VfsError> {
        let cleanup_object_id = object_id(b"postgres cleanup claim object");
        let first = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(repo_id, cleanup_object_id, Duration::from_secs(60)),
        )
        .await?
        .expect("first cleanup claim should be acquired");
        assert_eq!(first.attempts, 1);

        let duplicate = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(repo_id, cleanup_object_id, Duration::from_secs(60)),
        )
        .await?;
        assert!(duplicate.is_none());

        ObjectCleanupClaimStore::record_failure(store, &first, "transient repair failure").await?;
        expire_cleanup_claim(store, &first).await?;
        assert!(matches!(
            ObjectCleanupClaimStore::complete(store, &first).await,
            Err(VfsError::ObjectWriteConflict { .. })
        ));
        assert!(matches!(
            ObjectCleanupClaimStore::record_failure(store, &first, "too late").await,
            Err(VfsError::ObjectWriteConflict { .. })
        ));
        let retry = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(repo_id, cleanup_object_id, Duration::from_secs(60)),
        )
        .await?
        .expect("expired cleanup claim should be reacquired");
        assert_eq!(retry.attempts, 2);
        assert_ne!(retry.lease_token, first.lease_token);

        assert!(matches!(
            ObjectCleanupClaimStore::complete(store, &first).await,
            Err(VfsError::ObjectWriteConflict { .. })
        ));
        ObjectCleanupClaimStore::complete(store, &retry).await?;
        expire_cleanup_claim(store, &retry).await?;

        let completed_retry = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(repo_id, cleanup_object_id, Duration::from_secs(60)),
        )
        .await?;
        assert!(completed_retry.is_none());

        let invalid = ObjectCleanupClaimRequest {
            lease_owner: "bad\nowner".to_string(),
            ..cleanup_request(
                repo_id,
                object_id(b"postgres bad cleanup claim"),
                Duration::from_secs(60),
            )
        };
        assert!(matches!(
            ObjectCleanupClaimStore::claim(store, invalid).await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let active = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(
                repo_id,
                object_id(b"postgres active cleanup status"),
                Duration::from_secs(60),
            ),
        )
        .await?
        .expect("active status claim should be acquired");
        let stale = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(
                repo_id,
                object_id(b"postgres stale cleanup status"),
                Duration::from_secs(60),
            ),
        )
        .await?
        .expect("stale status claim should be acquired");
        expire_cleanup_claim(store, &stale).await?;
        let failed = ObjectCleanupClaimStore::claim(
            store,
            cleanup_request(
                repo_id,
                object_id(b"postgres failed cleanup status"),
                Duration::from_secs(60),
            ),
        )
        .await?
        .expect("failed status claim should be acquired");
        ObjectCleanupClaimStore::record_failure(store, &failed, "raw postgres failure detail")
            .await?;
        expire_cleanup_claim(store, &failed).await?;

        let statuses = ObjectCleanupClaimStore::list(store, 4).await?;
        assert_eq!(statuses.len(), 4);
        let failed_status = statuses
            .iter()
            .find(|status| status.object_id() == failed.object_id)
            .expect("bounded status list should include failed claim");
        assert_eq!(failed_status.state(), ObjectCleanupClaimState::Failed);
        assert!(failed_status.is_stale());
        assert!(failed_status.has_last_failure());
        assert!(!format!("{failed_status:?}").contains("raw postgres failure detail"));
        assert!(!format!("{failed_status:?}").contains(&failed.lease_token.to_string()));

        let counts = ObjectCleanupClaimStore::counts(store).await?;
        assert_eq!(counts.active(), 1);
        assert_eq!(counts.stale_active(), 1);
        assert_eq!(counts.completed(), 1);
        assert_eq!(counts.failed(), 1);
        assert_eq!(counts.total(), 4);
        assert_eq!(active.attempts, 1);

        Ok(())
    }

    #[tokio::test]
    async fn postgres_cleanup_claim_persists_deletion_ready_hold_state() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result = async {
            let repo_id = repo("repo_pg_cleanup_ready_hold");
            let claim = ObjectCleanupClaimStore::claim(
                &test_db.store,
                cleanup_request(
                    &repo_id,
                    object_id(b"postgres cleanup ready hold"),
                    Duration::from_secs(60),
                ),
            )
            .await?
            .expect("cleanup claim should be acquired");
            let ready_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
            let delete_after = ready_at + Duration::from_secs(300);

            ObjectCleanupClaimStore::mark_deletion_ready(
                &test_db.store,
                &claim,
                deletion_readiness_for_claim(&claim, ready_at, delete_after),
            )
            .await?;

            let status = cleanup_status_for_claim(&test_db.store, &claim).await?;
            assert_eq!(status.deletion_ready_at(), Some(ready_at));
            assert_eq!(status.delete_after(), Some(delete_after));
            assert!(status.final_object_bytes_deleted_at().is_none());
            assert!(status.final_object_metadata_deleted_at().is_none());
            assert!(status.is_deletion_held(ready_at + Duration::from_secs(1)));

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_cleanup_claim_preserves_ready_state_across_release_and_reclaim() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result = async {
            let repo_id = repo("repo_pg_cleanup_ready_reclaim");
            let request = cleanup_request(
                &repo_id,
                object_id(b"postgres cleanup ready reclaim"),
                Duration::from_secs(60),
            );
            let first = ObjectCleanupClaimStore::claim(&test_db.store, request.clone())
                .await?
                .expect("cleanup claim should be acquired");
            let ready_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_100);
            let initial_delete_after = ready_at + Duration::from_secs(60);
            let extended_delete_after = ready_at + Duration::from_secs(120);

            ObjectCleanupClaimStore::mark_deletion_ready(
                &test_db.store,
                &first,
                deletion_readiness_for_claim(&first, ready_at, initial_delete_after),
            )
            .await?;
            ObjectCleanupClaimStore::release(&test_db.store, &first).await?;

            let retry = ObjectCleanupClaimStore::claim(&test_db.store, request)
                .await?
                .expect("released cleanup claim should be reacquired");
            ObjectCleanupClaimStore::mark_deletion_ready(
                &test_db.store,
                &retry,
                deletion_readiness_for_claim(
                    &retry,
                    ready_at + Duration::from_secs(10),
                    extended_delete_after,
                ),
            )
            .await?;

            let status = cleanup_status_for_claim(&test_db.store, &retry).await?;
            assert_eq!(status.deletion_ready_at(), Some(ready_at));
            assert_eq!(status.delete_after(), Some(extended_delete_after));

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_cleanup_claim_clears_ready_state_on_snapshot_mismatch() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result = async {
            let repo_id = repo("repo_pg_cleanup_ready_mismatch");
            let claim = ObjectCleanupClaimStore::claim(
                &test_db.store,
                cleanup_request(
                    &repo_id,
                    object_id(b"postgres cleanup ready mismatch"),
                    Duration::from_secs(60),
                ),
            )
            .await?
            .expect("cleanup claim should be acquired");
            let ready_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_200);
            ObjectCleanupClaimStore::mark_deletion_ready(
                &test_db.store,
                &claim,
                deletion_readiness_for_claim(&claim, ready_at, ready_at + Duration::from_secs(60)),
            )
            .await?;

            let err = ObjectCleanupClaimStore::mark_deletion_ready(
                &test_db.store,
                &claim,
                deletion_readiness_for_claim_with_size(
                    &claim,
                    ready_at + Duration::from_secs(1),
                    ready_at + Duration::from_secs(120),
                    43,
                ),
            )
            .await
            .expect_err("snapshot mismatch should reject deletion readiness");
            assert!(matches!(err, VfsError::ObjectWriteConflict { .. }));
            assert!(!format!("{err:?}").contains(&claim.object_key));
            assert!(!format!("{err:?}").contains(&claim.lease_token.to_string()));

            let status = cleanup_status_for_claim(&test_db.store, &claim).await?;
            assert!(status.deletion_ready_at().is_none());
            assert!(status.delete_after().is_none());
            assert!(status.final_object_bytes_deleted_at().is_none());
            assert!(status.final_object_metadata_deleted_at().is_none());

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_cleanup_claim_persists_deletion_phase_markers_and_requires_them_to_complete()
    {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result = async {
            let repo_id = repo("repo_pg_cleanup_phase_markers");
            let request = durable_cleanup_request(
                &repo_id,
                object_id(b"postgres cleanup phase markers"),
                Duration::from_secs(60),
            );
            let claim = ObjectCleanupClaimStore::claim(&test_db.store, request)
                .await?
                .expect("cleanup claim should be acquired");
            let ready_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_300);

            let incomplete_before_readiness =
                ObjectCleanupClaimStore::complete(&test_db.store, &claim)
                    .await
                    .expect_err(
                        "CAS-lost cleanup claim cannot complete before deletion proof markers",
                    );
            assert!(matches!(
                incomplete_before_readiness,
                VfsError::ObjectWriteConflict { .. }
            ));
            assert!(!format!("{incomplete_before_readiness:?}").contains(&claim.object_key));
            assert!(
                !format!("{incomplete_before_readiness:?}")
                    .contains(&claim.lease_token.to_string())
            );

            ObjectCleanupClaimStore::mark_deletion_ready(
                &test_db.store,
                &claim,
                deletion_readiness_for_claim(&claim, ready_at, ready_at + Duration::from_secs(60)),
            )
            .await?;
            ObjectCleanupClaimStore::validate_final_object_deletion_ready(
                &test_db.store,
                &claim,
                &deletion_readiness_for_claim(&claim, ready_at, ready_at + Duration::from_secs(60)),
            )
            .await?;
            let incomplete = ObjectCleanupClaimStore::complete(&test_db.store, &claim)
                .await
                .expect_err("ready durable cleanup claim cannot complete before phase markers");
            assert!(matches!(incomplete, VfsError::ObjectWriteConflict { .. }));
            assert!(!format!("{incomplete:?}").contains(&claim.object_key));
            assert!(!format!("{incomplete:?}").contains(&claim.lease_token.to_string()));

            ObjectCleanupClaimStore::mark_final_object_bytes_deleted(&test_db.store, &claim)
                .await?;
            let status = cleanup_status_for_claim(&test_db.store, &claim).await?;
            assert!(status.final_object_bytes_deleted_at().is_some());
            assert!(status.final_object_metadata_deleted_at().is_none());
            let still_incomplete = ObjectCleanupClaimStore::complete(&test_db.store, &claim)
                .await
                .expect_err("metadata phase marker is also required before completion");
            assert!(matches!(
                still_incomplete,
                VfsError::ObjectWriteConflict { .. }
            ));

            ObjectCleanupClaimStore::mark_final_object_metadata_deleted(&test_db.store, &claim)
                .await?;
            ObjectCleanupClaimStore::complete(&test_db.store, &claim).await?;
            let completed = cleanup_status_for_claim(&test_db.store, &claim).await?;
            assert_eq!(completed.state(), ObjectCleanupClaimState::Completed);
            assert!(completed.final_object_bytes_deleted_at().is_some());
            assert!(completed.final_object_metadata_deleted_at().is_some());

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_blob_object_repair_should_recreate_missing_metadata_for_final_orphan() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };
        let temp_dir = TempBlobDir::new("repair");

        let result = async {
            let repo_id = repo("repo_pg_blob_repair");
            let orphan_bytes = b"postgres repairable final object bytes";
            let orphan_id = object_id(orphan_bytes);
            let orphan_key = object_key(&repo_id, ObjectKind::Blob, &orphan_id);
            let blobs = Arc::new(LocalBlobStore::new(temp_dir.path()));
            let blob_object_store =
                BlobObjectStore::new(blobs.clone(), Arc::new(test_db.store.clone()));

            RemoteBlobStore::put_bytes(blobs.as_ref(), &orphan_key, orphan_bytes.to_vec()).await?;
            assert!(
                ObjectMetadataStore::get(&test_db.store, &repo_id, orphan_id)
                    .await?
                    .is_none()
            );

            let cutoff = SystemTime::now() + Duration::from_secs(1);
            let err = blob_object_store
                .cleanup_orphans(
                    &repo_id,
                    cutoff,
                    ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDelete,
                )
                .await
                .expect_err("final object delete mode should fail closed");
            assert!(matches!(err, VfsError::NotSupported { .. }));
            assert_eq!(
                RemoteBlobStore::get_bytes(blobs.as_ref(), &orphan_key)
                    .await?
                    .as_slice(),
                orphan_bytes
            );

            let report = blob_object_store
                .repair_final_object_metadata_orphans(
                    &repo_id,
                    cutoff,
                    &test_db.store,
                    "postgres-repair-worker",
                    Duration::from_secs(60),
                )
                .await?;

            assert_eq!(report.final_orphans_found, 1);
            assert_eq!(report.final_orphans_repaired, 1);
            assert_eq!(report.final_orphans_claim_skipped, 0);
            assert_eq!(report.final_orphans_deleted, 0);
            assert!(report.errors.is_empty());
            assert_eq!(
                ObjectMetadataStore::get(&test_db.store, &repo_id, orphan_id).await?,
                Some(ObjectMetadataRecord::new(
                    repo_id.clone(),
                    orphan_id,
                    ObjectKind::Blob,
                    orphan_bytes.len() as u64,
                ))
            );
            let stored = blob_object_store
                .get(&repo_id, orphan_id, ObjectKind::Blob)
                .await?
                .expect("repaired metadata should make final bytes readable");
            assert_eq!(stored.bytes.as_slice(), orphan_bytes);

            let completed_request = cleanup_claim_request_for_object(
                &repo_id,
                ObjectKind::Blob,
                orphan_id,
                Duration::from_secs(60),
            );
            let completed_claim =
                load_cleanup_claim_for_request(&test_db.store, &completed_request)
                    .await?
                    .expect("repair should leave a completed cleanup claim row");
            assert!(completed_claim.completed_at.is_some());
            assert!(completed_claim.last_error.is_none());
            expire_cleanup_claim(&test_db.store, &completed_claim.claim).await?;
            let completed_retry =
                ObjectCleanupClaimStore::claim(&test_db.store, completed_request).await?;
            assert!(completed_retry.is_none());

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_blob_object_repair_should_skip_active_claim() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };
        let temp_dir = TempBlobDir::new("repair");

        let result = async {
            let repo_id = repo("repo_pg_blob_repair_skip");
            let orphan_bytes = b"postgres actively claimed final object";
            let orphan_id = object_id(orphan_bytes);
            let orphan_key = object_key(&repo_id, ObjectKind::Blob, &orphan_id);
            let blobs = Arc::new(LocalBlobStore::new(temp_dir.path()));
            let blob_object_store =
                BlobObjectStore::new(blobs.clone(), Arc::new(test_db.store.clone()));

            RemoteBlobStore::put_bytes(blobs.as_ref(), &orphan_key, orphan_bytes.to_vec()).await?;
            ObjectCleanupClaimStore::claim(
                &test_db.store,
                cleanup_claim_request_for_object(
                    &repo_id,
                    ObjectKind::Blob,
                    orphan_id,
                    Duration::from_secs(120),
                ),
            )
            .await?
            .expect("preclaim should acquire the repair lease");

            let report = blob_object_store
                .repair_final_object_metadata_orphans(
                    &repo_id,
                    SystemTime::now() + Duration::from_secs(1),
                    &test_db.store,
                    "postgres-repair-worker",
                    Duration::from_secs(60),
                )
                .await?;

            assert_eq!(report.final_orphans_found, 1);
            assert_eq!(report.final_orphans_claim_skipped, 1);
            assert_eq!(report.final_orphans_repaired, 0);
            assert!(report.errors.is_empty());
            assert!(
                ObjectMetadataStore::get(&test_db.store, &repo_id, orphan_id)
                    .await?
                    .is_none()
            );
            assert_eq!(
                RemoteBlobStore::get_bytes(blobs.as_ref(), &orphan_key)
                    .await?
                    .as_slice(),
                orphan_bytes
            );

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    #[tokio::test]
    async fn postgres_blob_object_repair_should_record_failure_without_deleting_when_hash_mismatches()
     {
        let Some(test_db) = TestDb::new().await else {
            return;
        };
        let temp_dir = TempBlobDir::new("repair");

        let result = async {
            let repo_id = repo("repo_pg_blob_repair_mismatch");
            let expected_bytes = b"postgres expected final object bytes";
            let wrong_bytes = b"postgres wrong final object bytes";
            let expected_id = object_id(expected_bytes);
            let orphan_key = object_key(&repo_id, ObjectKind::Blob, &expected_id);
            let blobs = Arc::new(LocalBlobStore::new(temp_dir.path()));
            let blob_object_store =
                BlobObjectStore::new(blobs.clone(), Arc::new(test_db.store.clone()));

            RemoteBlobStore::put_bytes(blobs.as_ref(), &orphan_key, wrong_bytes.to_vec()).await?;

            let report = blob_object_store
                .repair_final_object_metadata_orphans(
                    &repo_id,
                    SystemTime::now() + Duration::from_secs(1),
                    &test_db.store,
                    "postgres-repair-worker",
                    Duration::from_secs(60),
                )
                .await?;

            assert_eq!(report.final_orphans_found, 1);
            assert_eq!(report.final_orphans_repaired, 0);
            assert_eq!(report.final_orphans_deleted, 0);
            assert_eq!(report.errors.len(), 1);
            assert!(report.errors[0].message.contains("hashing to"));
            assert!(
                ObjectMetadataStore::get(&test_db.store, &repo_id, expected_id)
                    .await?
                    .is_none()
            );
            assert_eq!(
                RemoteBlobStore::get_bytes(blobs.as_ref(), &orphan_key)
                    .await?
                    .as_slice(),
                wrong_bytes
            );

            let claim_request = cleanup_claim_request_for_object(
                &repo_id,
                ObjectKind::Blob,
                expected_id,
                Duration::from_secs(60),
            );
            let failed_claim = load_cleanup_claim_for_request(&test_db.store, &claim_request)
                .await?
                .expect("repair failure should leave a cleanup claim row");
            assert!(failed_claim.completed_at.is_none());
            assert!(
                failed_claim
                    .last_error
                    .as_deref()
                    .is_some_and(|message| message.contains("hashing to"))
            );
            expire_cleanup_claim(&test_db.store, &failed_claim.claim).await?;
            let retry = ObjectCleanupClaimStore::claim(&test_db.store, claim_request)
                .await?
                .expect("expired failed repair claim should be reacquired");
            assert_eq!(retry.attempts, failed_claim.claim.attempts + 1);

            Ok::<(), VfsError>(())
        }
        .await;

        test_db.cleanup().await;
        result.unwrap();
    }

    async fn idempotency_key_hash_column(
        store: &PostgresMetadataStore,
        scope: &str,
    ) -> Result<Option<String>, VfsError> {
        let client = store.connect_client().await?;
        let row = client
            .query_opt(
                "SELECT key_hash FROM idempotency_records WHERE scope = $1",
                &[&scope],
            )
            .await
            .map_err(|error| postgres_error("fetch idempotency key_hash column", error))?;
        Ok(row.map(|row| row.get::<_, String>("key_hash")))
    }

    fn idempotency_fingerprint(scope: &str, label: &str) -> Result<String, VfsError> {
        request_fingerprint(scope, &json!({ "case": label }))
    }

    async fn run_idempotency_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
        let scope = "runs:create";
        let request_a = idempotency_fingerprint(scope, "request-a")?;
        let request_b = idempotency_fingerprint(scope, "request-b")?;
        let raw_visible_marker = "run-create-postgres-idem-marker";
        let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static(raw_visible_marker))
            .unwrap();
        assert_ne!(raw_visible_marker, key.key_hash());

        let reservation = match store.begin(scope, &key, &request_a).await? {
            IdempotencyBegin::Execute(r) => r,
            other => panic!("expected first begin to execute, got {other:?}"),
        };

        let stored_hash = idempotency_key_hash_column(store, scope)
            .await?
            .expect("pending row present after execute begin");
        assert_eq!(stored_hash, key.key_hash());
        assert!(!stored_hash.contains(raw_visible_marker));

        IdempotencyStore::complete(store, &reservation, 201, json!({"run_id": "run_123"})).await?;
        let replay_reservation = IdempotencyReservation::for_store_parts(
            reservation.scope(),
            reservation.key_hash(),
            reservation.request_fingerprint(),
            reservation.reservation_token(),
        )?;
        IdempotencyStore::complete_or_match(
            store,
            &replay_reservation,
            201,
            json!({"run_id": "run_123"}),
        )
        .await?;
        assert!(matches!(
            IdempotencyStore::complete_or_match(
                store,
                &replay_reservation,
                202,
                json!({"run_id": "run_123"}),
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            IdempotencyStore::complete_or_match(
                store,
                &replay_reservation,
                201,
                json!({"run_id": "different"}),
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        let wrong_fingerprint = IdempotencyReservation::for_store_parts(
            reservation.scope(),
            reservation.key_hash(),
            &request_b,
            reservation.reservation_token(),
        )?;
        assert!(matches!(
            IdempotencyStore::complete_or_match(
                store,
                &wrong_fingerprint,
                201,
                json!({"run_id": "run_123"}),
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let replay = match store.begin(scope, &key, &request_a).await? {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay, got {other:?}"),
        };
        assert_eq!(replay.status_code, 201);
        assert_eq!(replay.response_body, json!({"run_id": "run_123"}));
        assert_eq!(
            replay.classification,
            IdempotencyReplayClassification::SecretFree
        );
        assert!(replay.completed_at_unix_seconds > 0);

        assert!(matches!(
            store.begin(scope, &key, &request_b).await?,
            IdempotencyBegin::Conflict
        ));

        let partial_scope = "runs:create:partial-classification";
        let partial_request = idempotency_fingerprint(partial_scope, "partial")?;
        let partial_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("partial-classification"))
                .unwrap();
        let partial_reservation = match store
            .begin(partial_scope, &partial_key, &partial_request)
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected partial execute, got {other:?}"),
        };
        assert!(matches!(
            IdempotencyStore::complete_with_classification(
                store,
                &partial_reservation,
                200,
                json!({"visible": true}),
                IdempotencyReplayClassification::SecretBearing,
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        IdempotencyStore::complete_with_classification(
            store,
            &partial_reservation,
            200,
            json!({"visible": true}),
            IdempotencyReplayClassification::Partial,
        )
        .await?;
        assert!(matches!(
            IdempotencyStore::complete_or_match_with_classification(
                store,
                &partial_reservation,
                200,
                json!({"visible": true}),
                IdempotencyReplayClassification::SecretFree,
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        IdempotencyStore::complete_or_match_with_classification(
            store,
            &partial_reservation,
            200,
            json!({"visible": true}),
            IdempotencyReplayClassification::Partial,
        )
        .await?;
        let partial_replay = match store
            .begin(partial_scope, &partial_key, &partial_request)
            .await?
        {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected partial replay, got {other:?}"),
        };
        assert_eq!(
            partial_replay.classification,
            IdempotencyReplayClassification::Partial
        );

        let secret_scope = "workspace:secret-replay:tokens";
        let secret_request = idempotency_fingerprint(secret_scope, "secret")?;
        let secret_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("secret-replay")).unwrap();
        let secret_reservation = match store
            .begin(secret_scope, &secret_key, &secret_request)
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected secret replay execute, got {other:?}"),
        };
        let secret_metadata = SecretReplayMetadata {
            envelope_version: 1,
            key_id: "test-key".to_string(),
            aad_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            encrypted_at_unix_seconds: 1_710_000_000,
        };
        let secret_envelope = json!({
            "version": 1,
            "key_id": "test-key",
            "nonce_b64": "bm9uY2U=",
            "ciphertext_b64": "Y2lwaGVydGV4dA==",
            "aad_hash": secret_metadata.aad_hash.clone(),
            "encrypted_at_unix_seconds": 1_710_000_000u64
        });
        IdempotencyStore::complete_with_encrypted_secret_replay(
            store,
            &secret_reservation,
            201,
            secret_envelope.clone(),
            secret_metadata.clone(),
        )
        .await?;
        let secret_replay = match store
            .begin(secret_scope, &secret_key, &secret_request)
            .await?
        {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected encrypted secret replay, got {other:?}"),
        };
        assert_eq!(
            secret_replay.classification,
            IdempotencyReplayClassification::SecretBearing
        );
        assert_eq!(secret_replay.response_body, secret_envelope);
        assert_eq!(secret_replay.secret_replay, Some(secret_metadata.clone()));
        let rendered = format!("{secret_replay:?}");
        assert!(!rendered.contains("Y2lwaGVydGV4dA=="));
        assert!(!rendered.contains("secret-replay"));

        let secret_extra_scope = "workspace:secret-replay-extra:tokens";
        let secret_extra_request = idempotency_fingerprint(secret_extra_scope, "secret-extra")?;
        let secret_extra_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("secret-extra-replay"))
                .unwrap();
        let secret_extra_reservation = match store
            .begin(secret_extra_scope, &secret_extra_key, &secret_extra_request)
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected secret extra replay execute, got {other:?}"),
        };
        let mut secret_extra_envelope = secret_envelope.clone();
        secret_extra_envelope
            .as_object_mut()
            .unwrap()
            .insert("workspace_token".to_string(), json!("secret-token"));
        let err = IdempotencyStore::complete_with_encrypted_secret_replay(
            store,
            &secret_extra_reservation,
            201,
            secret_extra_envelope,
            secret_metadata.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));

        let quota_policy = IdempotencyRetentionPolicy {
            completed_ttl_seconds: u64::MAX,
            pending_stale_after_seconds: u64::MAX,
            max_records_per_scope: Some(1),
            max_records_per_repo: Some(1),
            max_records_per_workspace: Some(1),
            max_records_per_principal: Some(1),
        };
        let quota_scope = "runs:create:quota-scope";
        let quota_key_a =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-scope-a")).unwrap();
        let quota_key_b =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-scope-b")).unwrap();
        let quota_a = match store
            .begin_with_policy(
                quota_scope,
                &quota_key_a,
                &idempotency_fingerprint(quota_scope, "a")?,
                IdempotencyQuotaIdentity::for_scope(quota_scope),
                &quota_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected quota execute, got {other:?}"),
        };
        assert!(matches!(
            store
                .begin_with_policy(
                    quota_scope,
                    &quota_key_b,
                    &idempotency_fingerprint(quota_scope, "b")?,
                    IdempotencyQuotaIdentity::for_scope(quota_scope),
                    &quota_policy,
                )
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        store.abort(&quota_a).await;

        let quota_repo = format!("repo:{}:vcs:commit", Uuid::new_v4().simple());
        let quota_repo_2 = quota_repo.replace("commit", "tag");
        let repo_key_a =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-repo-a")).unwrap();
        let repo_key_b =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-repo-b")).unwrap();
        let repo_reservation = match store
            .begin_with_policy(
                &quota_repo,
                &repo_key_a,
                &idempotency_fingerprint(&quota_repo, "a")?,
                IdempotencyQuotaIdentity::for_scope(&quota_repo),
                &quota_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected repo quota execute, got {other:?}"),
        };
        assert!(matches!(
            store
                .begin_with_policy(
                    &quota_repo_2,
                    &repo_key_b,
                    &idempotency_fingerprint(&quota_repo_2, "b")?,
                    IdempotencyQuotaIdentity::for_scope(&quota_repo_2),
                    &quota_policy,
                )
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        store.abort(&repo_reservation).await;

        let workspace_scope_a = format!("workspace:{}:runs:create", Uuid::new_v4());
        let workspace_scope_b = workspace_scope_a.replace("create", "delete");
        let workspace_key_a =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-workspace-a"))
                .unwrap();
        let workspace_key_b =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-workspace-b"))
                .unwrap();
        let workspace_reservation = match store
            .begin_with_policy(
                &workspace_scope_a,
                &workspace_key_a,
                &idempotency_fingerprint(&workspace_scope_a, "a")?,
                IdempotencyQuotaIdentity::for_scope(&workspace_scope_a),
                &quota_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected workspace quota execute, got {other:?}"),
        };
        assert!(matches!(
            store
                .begin_with_policy(
                    &workspace_scope_b,
                    &workspace_key_b,
                    &idempotency_fingerprint(&workspace_scope_b, "b")?,
                    IdempotencyQuotaIdentity::for_scope(&workspace_scope_b),
                    &quota_policy,
                )
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        store.abort(&workspace_reservation).await;

        let mut principal_identity_a = IdempotencyQuotaIdentity::for_scope("principal:quota:a");
        principal_identity_a.principal_uid = Some(0);
        let mut principal_identity_b = IdempotencyQuotaIdentity::for_scope("principal:quota:b");
        principal_identity_b.principal_uid = Some(0);
        let principal_key_a =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-principal-a"))
                .unwrap();
        let principal_key_b =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-principal-b"))
                .unwrap();
        let principal_reservation = match store
            .begin_with_policy(
                "principal:quota:a",
                &principal_key_a,
                &idempotency_fingerprint("principal:quota:a", "a")?,
                principal_identity_a,
                &quota_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected principal quota execute, got {other:?}"),
        };
        assert!(matches!(
            store
                .begin_with_policy(
                    "principal:quota:b",
                    &principal_key_b,
                    &idempotency_fingerprint("principal:quota:b", "b")?,
                    principal_identity_b,
                    &quota_policy,
                )
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        store.abort(&principal_reservation).await;

        let pending_scope = "runs:create:pending-semantics";
        let pending_request_a = idempotency_fingerprint(pending_scope, "request-a")?;
        let pending_request_b = idempotency_fingerprint(pending_scope, "request-b")?;
        let pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-pending-1")).unwrap();
        let pending_reservation = match store
            .begin(pending_scope, &pending_key, &pending_request_a)
            .await?
        {
            IdempotencyBegin::Execute(r) => r,
            other => panic!("expected execute for pending semantics, got {other:?}"),
        };
        assert!(matches!(
            store
                .begin(pending_scope, &pending_key, &pending_request_a)
                .await?,
            IdempotencyBegin::InProgress
        ));
        assert!(matches!(
            store
                .begin(pending_scope, &pending_key, &pending_request_b)
                .await?,
            IdempotencyBegin::Conflict
        ));

        let stale_policy = IdempotencyRetentionPolicy {
            completed_ttl_seconds: u64::MAX,
            pending_stale_after_seconds: 1,
            max_records_per_scope: None,
            max_records_per_repo: None,
            max_records_per_workspace: None,
            max_records_per_principal: None,
        };
        let stale_same_scope = "runs:create:stale-same";
        let stale_same_request = idempotency_fingerprint(stale_same_scope, "same")?;
        let stale_same_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("stale-same")).unwrap();
        let stale_same_original = match store
            .begin_with_policy(
                stale_same_scope,
                &stale_same_key,
                &stale_same_request,
                IdempotencyQuotaIdentity::for_scope(stale_same_scope),
                &stale_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected stale same execute, got {other:?}"),
        };
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET reserved_at = clock_timestamp() - interval '10 seconds'
                   WHERE scope = $1 AND key_hash = $2"#,
                &[&stale_same_scope, &stale_same_key.key_hash()],
            )
            .await
            .map_err(|error| postgres_error("age stale same pending", error))?;
        drop(client);
        let stale_same_takeover = match store
            .begin_with_policy(
                stale_same_scope,
                &stale_same_key,
                &stale_same_request,
                IdempotencyQuotaIdentity::for_scope(stale_same_scope),
                &stale_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected stale same takeover, got {other:?}"),
        };
        assert_ne!(
            stale_same_original.reservation_token(),
            stale_same_takeover.reservation_token()
        );
        assert!(matches!(
            IdempotencyStore::complete(store, &stale_same_original, 204, serde_json::Value::Null)
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        store.abort(&stale_same_takeover).await;

        let stale_diff_scope = "runs:create:stale-diff";
        let stale_diff_request_a = idempotency_fingerprint(stale_diff_scope, "a")?;
        let stale_diff_request_b = idempotency_fingerprint(stale_diff_scope, "b")?;
        let stale_diff_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("stale-diff")).unwrap();
        let stale_diff_original = match store
            .begin_with_policy(
                stale_diff_scope,
                &stale_diff_key,
                &stale_diff_request_a,
                IdempotencyQuotaIdentity::for_scope(stale_diff_scope),
                &stale_policy,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected stale diff execute, got {other:?}"),
        };
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET reserved_at = clock_timestamp() - interval '10 seconds'
                   WHERE scope = $1 AND key_hash = $2"#,
                &[&stale_diff_scope, &stale_diff_key.key_hash()],
            )
            .await
            .map_err(|error| postgres_error("age stale diff pending", error))?;
        assert!(matches!(
            store
                .begin_with_policy(
                    stale_diff_scope,
                    &stale_diff_key,
                    &stale_diff_request_b,
                    IdempotencyQuotaIdentity::for_scope(stale_diff_scope),
                    &stale_policy,
                )
                .await?,
            IdempotencyBegin::Conflict
        ));
        assert!(matches!(
            IdempotencyStore::complete(store, &stale_diff_original, 204, serde_json::Value::Null)
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let wrong_pending_token = IdempotencyReservation::for_store_parts(
            pending_reservation.scope(),
            pending_reservation.key_hash(),
            pending_reservation.request_fingerprint(),
            "wrong-token",
        )?;
        assert!(matches!(
            IdempotencyStore::complete_or_match(
                store,
                &wrong_pending_token,
                204,
                serde_json::Value::Null
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store
                .begin(pending_scope, &pending_key, &pending_request_a)
                .await?,
            IdempotencyBegin::InProgress
        ));

        store.abort(&pending_reservation).await;

        assert!(matches!(
            IdempotencyStore::complete(store, &pending_reservation, 204, serde_json::Value::Null)
                .await,
            Err(VfsError::InvalidArgs { .. }),
        ));

        match store
            .begin(pending_scope, &pending_key, &pending_request_a)
            .await?
        {
            IdempotencyBegin::Execute(r) => {
                assert!(matches!(
                    IdempotencyStore::complete(
                        store,
                        &pending_reservation,
                        204,
                        serde_json::Value::Null
                    )
                    .await,
                    Err(VfsError::InvalidArgs { .. }),
                ));
                store.abort(&pending_reservation).await;
                assert!(matches!(
                    store
                        .begin(pending_scope, &pending_key, &pending_request_a)
                        .await?,
                    IdempotencyBegin::InProgress
                ));
                store.abort(&r).await;
            }
            other => panic!("expected execute after abort, got {other:?}"),
        }

        let retained_repo = RepoId::new(format!("idem_repo_{}", Uuid::new_v4().simple()))?;
        let retained_scope = format!("repo:{}:vcs:commit", retained_repo.as_str());
        let retained_request = idempotency_fingerprint(&retained_scope, "retained-completed")?;
        let retained_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("retained-completed"))
                .unwrap();
        let retained_reservation = match store
            .begin(&retained_scope, &retained_key, &retained_request)
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected retained execute, got {other:?}"),
        };
        IdempotencyStore::complete_with_classification(
            store,
            &retained_reservation,
            200,
            json!({
                "hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "private_body": "do-not-leak-retained-body"
            }),
            IdempotencyReplayClassification::Partial,
        )
        .await?;
        let retained_pending_scope = format!("repo:{}:vcs:revert", retained_repo.as_str());
        let retained_pending_request =
            idempotency_fingerprint(&retained_pending_scope, "retained-pending")?;
        let retained_pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("retained-pending"))
                .unwrap();
        let retained_pending_reservation = match store
            .begin(
                &retained_pending_scope,
                &retained_pending_key,
                &retained_pending_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected retained pending execute, got {other:?}"),
        };
        let retained = store.list_retained_for_repo(&retained_repo, 10).await?;
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].scope(), retained_pending_scope);
        assert!(retained[0].pending);
        assert!(retained[0].reserved_at_unix_seconds.is_some());
        assert_eq!(retained[1].scope(), retained_scope);
        assert!(!retained[1].pending);
        assert_eq!(
            retained[1].classification,
            Some(IdempotencyReplayClassification::Partial)
        );
        assert!(retained[1].completed_at_unix_seconds.is_some());
        assert!(!format!("{retained:?}").contains("aaaaaaaaaaaaaaaa"));
        assert!(!format!("{retained:?}").contains("do-not-leak-retained-body"));
        store.abort(&retained_pending_reservation).await;

        let sweep_repo = RepoId::new(format!("idem_sweep_{}", Uuid::new_v4().simple()))?;
        let sweep_delete_scope = format!("repo:{}:vcs:delete", sweep_repo.as_str());
        let sweep_secret_scope = format!("repo:{}:workspace:tokens", sweep_repo.as_str());
        let sweep_keep_scope = format!("repo:{}:vcs:keep", sweep_repo.as_str());
        let sweep_pending_scope = format!("repo:{}:vcs:pending", sweep_repo.as_str());
        let sweep_delete_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-delete")).unwrap();
        let sweep_secret_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-secret")).unwrap();
        let sweep_keep_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-keep")).unwrap();
        let sweep_pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-pending")).unwrap();
        let sweep_delete_request = idempotency_fingerprint(&sweep_delete_scope, "delete")?;
        let sweep_secret_request = idempotency_fingerprint(&sweep_secret_scope, "secret")?;
        let sweep_keep_request = idempotency_fingerprint(&sweep_keep_scope, "keep")?;
        let sweep_pending_request = idempotency_fingerprint(&sweep_pending_scope, "pending")?;
        let sweep_delete_reservation = match store
            .begin(
                &sweep_delete_scope,
                &sweep_delete_key,
                &sweep_delete_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected sweep delete execute, got {other:?}"),
        };
        IdempotencyStore::complete(store, &sweep_delete_reservation, 200, json!({"ok": true}))
            .await?;
        let sweep_secret_reservation = match store
            .begin(
                &sweep_secret_scope,
                &sweep_secret_key,
                &sweep_secret_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected sweep secret execute, got {other:?}"),
        };
        IdempotencyStore::complete_with_encrypted_secret_replay(
            store,
            &sweep_secret_reservation,
            201,
            json!({
                "version": 1,
                "key_id": "sweep-key",
                "nonce_b64": "bm9uY2U=",
                "ciphertext_b64": "Y2lwaGVydGV4dA==",
                "aad_hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "encrypted_at_unix_seconds": 1_710_000_001u64
            }),
            SecretReplayMetadata {
                envelope_version: 1,
                key_id: "sweep-key".to_string(),
                aad_hash: "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    .to_string(),
                encrypted_at_unix_seconds: 1_710_000_001,
            },
        )
        .await?;
        let retained_root = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let sweep_keep_reservation = match store
            .begin(&sweep_keep_scope, &sweep_keep_key, &sweep_keep_request)
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected sweep keep execute, got {other:?}"),
        };
        IdempotencyStore::complete(
            store,
            &sweep_keep_reservation,
            200,
            json!({"hash": retained_root}),
        )
        .await?;
        let sweep_pending_reservation = match store
            .begin(
                &sweep_pending_scope,
                &sweep_pending_key,
                &sweep_pending_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected sweep pending execute, got {other:?}"),
        };
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET completed_at = to_timestamp(1)
                   WHERE scope IN ($1, $2, $3)"#,
                &[&sweep_delete_scope, &sweep_keep_scope, &sweep_secret_scope],
            )
            .await
            .map_err(|error| postgres_error("age completed idempotency rows", error))?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET reserved_at = to_timestamp(1)
                   WHERE scope = $1"#,
                &[&sweep_pending_scope],
            )
            .await
            .map_err(|error| postgres_error("age pending idempotency rows", error))?;
        drop(client);
        let sweep_summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 100,
                limit: 10,
                policy: IdempotencyRetentionPolicy {
                    completed_ttl_seconds: 1,
                    pending_stale_after_seconds: 1,
                    max_records_per_scope: None,
                    max_records_per_repo: None,
                    max_records_per_workspace: None,
                    max_records_per_principal: None,
                },
                repo_id: Some(sweep_repo.clone()),
                retain_keys: Vec::new(),
                retain_commit_ids: vec![retained_root.to_string()],
                abort_stale_pending: true,
                block_completed_when_pending: false,
            })
            .await?;
        assert_eq!(sweep_summary.swept_completed, 2);
        assert_eq!(sweep_summary.aborted_pending, 1);
        assert_eq!(sweep_summary.retained_for_roots, 1);
        assert!(matches!(
            IdempotencyStore::complete(
                store,
                &sweep_pending_reservation,
                204,
                serde_json::Value::Null,
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store
                .begin(
                    &sweep_delete_scope,
                    &sweep_delete_key,
                    &sweep_delete_request
                )
                .await?,
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store
                .begin(&sweep_keep_scope, &sweep_keep_key, &sweep_keep_request)
                .await?,
            IdempotencyBegin::Replay(_)
        ));
        assert!(matches!(
            store
                .begin(
                    &sweep_secret_scope,
                    &sweep_secret_key,
                    &sweep_secret_request
                )
                .await?,
            IdempotencyBegin::Execute(_)
        ));

        let sweep_starve_repo =
            RepoId::new(format!("idem_sweep_starve_{}", Uuid::new_v4().simple()))?;
        let active_pending_scope =
            format!("repo:{}:vcs:active-pending", sweep_starve_repo.as_str());
        let stale_pending_scope = format!("repo:{}:vcs:stale-pending", sweep_starve_repo.as_str());
        let active_pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-active-pending"))
                .unwrap();
        let stale_pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-stale-pending"))
                .unwrap();
        let active_pending_request = idempotency_fingerprint(&active_pending_scope, "active")?;
        let stale_pending_request = idempotency_fingerprint(&stale_pending_scope, "stale")?;
        let active_pending_reservation = match store
            .begin(
                &active_pending_scope,
                &active_pending_key,
                &active_pending_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected active pending execute, got {other:?}"),
        };
        let stale_pending_reservation = match store
            .begin(
                &stale_pending_scope,
                &stale_pending_key,
                &stale_pending_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected stale pending execute, got {other:?}"),
        };
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET reserved_at = CASE WHEN scope = $1 THEN to_timestamp(100) ELSE to_timestamp(1) END
                   WHERE scope IN ($1, $2)"#,
                &[&active_pending_scope, &stale_pending_scope],
            )
            .await
            .map_err(|error| postgres_error("age starvation pending rows", error))?;
        drop(client);
        let starve_pending_summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 100,
                limit: 1,
                policy: IdempotencyRetentionPolicy {
                    completed_ttl_seconds: 1,
                    pending_stale_after_seconds: 1,
                    max_records_per_scope: None,
                    max_records_per_repo: None,
                    max_records_per_workspace: None,
                    max_records_per_principal: None,
                },
                repo_id: Some(sweep_starve_repo.clone()),
                retain_keys: Vec::new(),
                retain_commit_ids: Vec::new(),
                abort_stale_pending: true,
                block_completed_when_pending: false,
            })
            .await?;
        assert_eq!(starve_pending_summary.scanned, 1);
        assert_eq!(starve_pending_summary.aborted_pending, 1);
        assert!(matches!(
            IdempotencyStore::complete(
                store,
                &stale_pending_reservation,
                204,
                serde_json::Value::Null
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store
                .begin(
                    &active_pending_scope,
                    &active_pending_key,
                    &active_pending_request
                )
                .await?,
            IdempotencyBegin::InProgress
        ));
        store.abort(&active_pending_reservation).await;

        let unexpired_completed_scope = format!(
            "repo:{}:vcs:unexpired-completed",
            sweep_starve_repo.as_str()
        );
        let expired_completed_scope =
            format!("repo:{}:vcs:expired-completed", sweep_starve_repo.as_str());
        let unexpired_completed_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-unexpired"))
                .unwrap();
        let expired_completed_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-expired")).unwrap();
        let unexpired_completed_request =
            idempotency_fingerprint(&unexpired_completed_scope, "unexpired")?;
        let expired_completed_request =
            idempotency_fingerprint(&expired_completed_scope, "expired")?;
        let unexpired_completed_reservation = match store
            .begin(
                &unexpired_completed_scope,
                &unexpired_completed_key,
                &unexpired_completed_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected unexpired completed execute, got {other:?}"),
        };
        IdempotencyStore::complete(
            store,
            &unexpired_completed_reservation,
            200,
            json!({"ok": "unexpired"}),
        )
        .await?;
        let expired_completed_reservation = match store
            .begin(
                &expired_completed_scope,
                &expired_completed_key,
                &expired_completed_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected expired completed execute, got {other:?}"),
        };
        IdempotencyStore::complete(
            store,
            &expired_completed_reservation,
            200,
            json!({"ok": "expired"}),
        )
        .await?;
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET completed_at = CASE WHEN scope = $1 THEN to_timestamp(100) ELSE to_timestamp(1) END
                   WHERE scope IN ($1, $2)"#,
                &[&unexpired_completed_scope, &expired_completed_scope],
            )
            .await
            .map_err(|error| postgres_error("age starvation completed rows", error))?;
        drop(client);
        let starve_completed_summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 100,
                limit: 1,
                policy: IdempotencyRetentionPolicy {
                    completed_ttl_seconds: 1,
                    pending_stale_after_seconds: 1,
                    max_records_per_scope: None,
                    max_records_per_repo: None,
                    max_records_per_workspace: None,
                    max_records_per_principal: None,
                },
                repo_id: Some(sweep_starve_repo.clone()),
                retain_keys: Vec::new(),
                retain_commit_ids: Vec::new(),
                abort_stale_pending: true,
                block_completed_when_pending: false,
            })
            .await?;
        assert_eq!(starve_completed_summary.scanned, 1);
        assert_eq!(starve_completed_summary.swept_completed, 1);
        assert!(matches!(
            store
                .begin(
                    &expired_completed_scope,
                    &expired_completed_key,
                    &expired_completed_request
                )
                .await?,
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store
                .begin(
                    &unexpired_completed_scope,
                    &unexpired_completed_key,
                    &unexpired_completed_request
                )
                .await?,
            IdempotencyBegin::Replay(_)
        ));

        let explicit_retained_pending_scope =
            format!("repo:{}:vcs:explicit-retained", sweep_starve_repo.as_str());
        let explicit_sweep_pending_scope =
            format!("repo:{}:vcs:explicit-sweep", sweep_starve_repo.as_str());
        let explicit_retained_pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("explicit-retained"))
                .unwrap();
        let explicit_sweep_pending_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("explicit-sweep"))
                .unwrap();
        let explicit_retained_pending_request =
            idempotency_fingerprint(&explicit_retained_pending_scope, "retained")?;
        let explicit_sweep_pending_request =
            idempotency_fingerprint(&explicit_sweep_pending_scope, "sweep")?;
        let explicit_retained_pending_reservation = match store
            .begin(
                &explicit_retained_pending_scope,
                &explicit_retained_pending_key,
                &explicit_retained_pending_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected explicit retained pending execute, got {other:?}"),
        };
        let explicit_sweep_pending_reservation = match store
            .begin(
                &explicit_sweep_pending_scope,
                &explicit_sweep_pending_key,
                &explicit_sweep_pending_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected explicit sweep pending execute, got {other:?}"),
        };
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET reserved_at = CASE WHEN scope = $1 THEN to_timestamp(1) ELSE to_timestamp(2) END
                   WHERE scope IN ($1, $2)"#,
                &[
                    &explicit_retained_pending_scope,
                    &explicit_sweep_pending_scope,
                ],
            )
            .await
            .map_err(|error| postgres_error("age explicit retained pending rows", error))?;
        drop(client);
        let explicit_retained_summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 100,
                limit: 1,
                policy: IdempotencyRetentionPolicy {
                    completed_ttl_seconds: 1,
                    pending_stale_after_seconds: 1,
                    max_records_per_scope: None,
                    max_records_per_repo: None,
                    max_records_per_workspace: None,
                    max_records_per_principal: None,
                },
                repo_id: Some(sweep_starve_repo.clone()),
                retain_keys: vec![(
                    explicit_retained_pending_scope.clone(),
                    explicit_retained_pending_key.key_hash().to_string(),
                )],
                retain_commit_ids: Vec::new(),
                abort_stale_pending: true,
                block_completed_when_pending: false,
            })
            .await?;
        assert_eq!(explicit_retained_summary.scanned, 1);
        assert_eq!(explicit_retained_summary.aborted_pending, 1);
        assert!(matches!(
            IdempotencyStore::complete(
                store,
                &explicit_sweep_pending_reservation,
                204,
                serde_json::Value::Null
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store
                .begin(
                    &explicit_retained_pending_scope,
                    &explicit_retained_pending_key,
                    &explicit_retained_pending_request
                )
                .await?,
            IdempotencyBegin::InProgress
        ));
        store.abort(&explicit_retained_pending_reservation).await;

        const RETAINED_ROOT_BLOCKER_COUNT: usize = 8;
        let retained_root_starve_repo = RepoId::new(format!(
            "idem_sweep_root_starve_{}",
            Uuid::new_v4().simple()
        ))?;
        let retained_root_completed_scopes = (0..RETAINED_ROOT_BLOCKER_COUNT)
            .map(|index| {
                format!(
                    "repo:{}:vcs:retained-root-{index}",
                    retained_root_starve_repo.as_str()
                )
            })
            .collect::<Vec<_>>();
        let sweep_after_root_completed_scope = format!(
            "repo:{}:vcs:sweep-after-root",
            retained_root_starve_repo.as_str()
        );
        let retained_root_completed_keys = (0..RETAINED_ROOT_BLOCKER_COUNT)
            .map(|index| {
                let value = format!("retained-root-{index}");
                IdempotencyKey::parse_header_value(
                    &HeaderValue::from_bytes(value.as_bytes()).unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let sweep_after_root_completed_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("sweep-after-root"))
                .unwrap();
        let retained_root_completed_requests = retained_root_completed_scopes
            .iter()
            .enumerate()
            .map(|(index, scope)| idempotency_fingerprint(scope, &format!("retained-root-{index}")))
            .collect::<Result<Vec<_>, _>>()?;
        let sweep_after_root_completed_request =
            idempotency_fingerprint(&sweep_after_root_completed_scope, "sweep-after-root")?;
        let retained_root_for_starvation =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        for ((scope, key), request) in retained_root_completed_scopes
            .iter()
            .zip(retained_root_completed_keys.iter())
            .zip(retained_root_completed_requests.iter())
        {
            let reservation = match store.begin(scope, key, request).await? {
                IdempotencyBegin::Execute(reservation) => reservation,
                other => panic!("expected retained root completed execute, got {other:?}"),
            };
            IdempotencyStore::complete(
                store,
                &reservation,
                200,
                json!({"hash": retained_root_for_starvation}),
            )
            .await?;
        }
        let sweep_after_root_completed_reservation = match store
            .begin(
                &sweep_after_root_completed_scope,
                &sweep_after_root_completed_key,
                &sweep_after_root_completed_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected sweep after root completed execute, got {other:?}"),
        };
        IdempotencyStore::complete(
            store,
            &sweep_after_root_completed_reservation,
            200,
            json!({"ok": "sweep-after-root"}),
        )
        .await?;
        let client = store.connect_client().await?;
        for (index, scope) in retained_root_completed_scopes.iter().enumerate() {
            let completed_at = (index + 1) as f64;
            client
                .execute(
                    r#"UPDATE idempotency_records
                       SET completed_at = to_timestamp($2::double precision)
                       WHERE scope = $1"#,
                    &[scope, &completed_at],
                )
                .await
                .map_err(|error| postgres_error("age retained root completed row", error))?;
        }
        let sweep_after_root_completed_at = (RETAINED_ROOT_BLOCKER_COUNT + 1) as f64;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET completed_at = to_timestamp($2::double precision)
                   WHERE scope = $1"#,
                &[
                    &sweep_after_root_completed_scope,
                    &sweep_after_root_completed_at,
                ],
            )
            .await
            .map_err(|error| postgres_error("age sweep after root completed row", error))?;
        drop(client);

        for _ in 0..RETAINED_ROOT_BLOCKER_COUNT {
            let retained_root_summary = store
                .sweep_retention(IdempotencySweepRequest {
                    now_unix_seconds: 100,
                    limit: 1,
                    policy: IdempotencyRetentionPolicy {
                        completed_ttl_seconds: 1,
                        pending_stale_after_seconds: 1,
                        max_records_per_scope: None,
                        max_records_per_repo: None,
                        max_records_per_workspace: None,
                        max_records_per_principal: None,
                    },
                    repo_id: Some(retained_root_starve_repo.clone()),
                    retain_keys: Vec::new(),
                    retain_commit_ids: vec![retained_root_for_starvation.to_string()],
                    abort_stale_pending: true,
                    block_completed_when_pending: false,
                })
                .await?;
            assert_eq!(retained_root_summary.scanned, 1);
            assert_eq!(retained_root_summary.swept_completed, 0);
            assert_eq!(retained_root_summary.retained_for_roots, 1);
        }

        let retained_root_starvation_summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 100,
                limit: 1,
                policy: IdempotencyRetentionPolicy {
                    completed_ttl_seconds: 1,
                    pending_stale_after_seconds: 1,
                    max_records_per_scope: None,
                    max_records_per_repo: None,
                    max_records_per_workspace: None,
                    max_records_per_principal: None,
                },
                repo_id: Some(retained_root_starve_repo.clone()),
                retain_keys: Vec::new(),
                retain_commit_ids: vec![retained_root_for_starvation.to_string()],
                abort_stale_pending: true,
                block_completed_when_pending: false,
            })
            .await?;
        assert_eq!(retained_root_starvation_summary.scanned, 1);
        assert_eq!(retained_root_starvation_summary.swept_completed, 1);
        assert!(matches!(
            store
                .begin(
                    &sweep_after_root_completed_scope,
                    &sweep_after_root_completed_key,
                    &sweep_after_root_completed_request
                )
                .await?,
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store
                .begin(
                    &retained_root_completed_scopes[0],
                    &retained_root_completed_keys[0],
                    &retained_root_completed_requests[0]
                )
                .await?,
            IdempotencyBegin::Replay(_)
        ));

        let unrelated_hex_completed_scope = format!(
            "repo:{}:vcs:unrelated-hex",
            retained_root_starve_repo.as_str()
        );
        let unrelated_hex_completed_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("unrelated-hex")).unwrap();
        let unrelated_hex_completed_request =
            idempotency_fingerprint(&unrelated_hex_completed_scope, "unrelated-hex")?;
        let unrelated_hex_completed_reservation = match store
            .begin(
                &unrelated_hex_completed_scope,
                &unrelated_hex_completed_key,
                &unrelated_hex_completed_request,
            )
            .await?
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected unrelated hex completed execute, got {other:?}"),
        };
        IdempotencyStore::complete(
            store,
            &unrelated_hex_completed_reservation,
            200,
            json!({"note": retained_root_for_starvation}),
        )
        .await?;
        let client = store.connect_client().await?;
        client
            .execute(
                r#"UPDATE idempotency_records
                   SET completed_at = to_timestamp(1)
                   WHERE scope = $1"#,
                &[&unrelated_hex_completed_scope],
            )
            .await
            .map_err(|error| postgres_error("age unrelated hex completed row", error))?;
        drop(client);
        let unrelated_hex_summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 100,
                limit: 1,
                policy: IdempotencyRetentionPolicy {
                    completed_ttl_seconds: 1,
                    pending_stale_after_seconds: 1,
                    max_records_per_scope: None,
                    max_records_per_repo: None,
                    max_records_per_workspace: None,
                    max_records_per_principal: None,
                },
                repo_id: Some(retained_root_starve_repo.clone()),
                retain_keys: Vec::new(),
                retain_commit_ids: vec![retained_root_for_starvation.to_string()],
                abort_stale_pending: true,
                block_completed_when_pending: false,
            })
            .await?;
        assert_eq!(unrelated_hex_summary.scanned, 1);
        assert_eq!(unrelated_hex_summary.swept_completed, 1);
        assert!(matches!(
            store
                .begin(
                    &unrelated_hex_completed_scope,
                    &unrelated_hex_completed_key,
                    &unrelated_hex_completed_request
                )
                .await?,
            IdempotencyBegin::Execute(_)
        ));

        let store_arc = Arc::new(store.clone());
        let barrier = Arc::new(Barrier::new(2));
        const SCOPE_CONC: &str = "runs:create:concurrent";
        let request_conc_a = idempotency_fingerprint(SCOPE_CONC, "request-conc-a")?;
        let key_conc =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-concurrent"))
                .unwrap();
        let key_conc_a = key_conc.clone();
        let key_conc_b = key_conc.clone();
        let request_conc_a_1 = request_conc_a.clone();
        let request_conc_a_2 = request_conc_a;
        let s1 = store_arc.clone();
        let b1 = barrier.clone();
        let s2 = store_arc.clone();
        let b2 = barrier.clone();
        let concurrent_a = tokio::spawn(async move {
            b1.wait().await;
            s1.begin(SCOPE_CONC, &key_conc_a, &request_conc_a_1).await
        });
        let concurrent_b = tokio::spawn(async move {
            b2.wait().await;
            s2.begin(SCOPE_CONC, &key_conc_b, &request_conc_a_2).await
        });
        let out_a = concurrent_a.await.expect("task a join")?;
        let out_b = concurrent_b.await.expect("task b join")?;
        let mut executes = 0u8;
        let mut in_progress = 0u8;
        match out_a {
            IdempotencyBegin::Execute(_) => executes += 1,
            IdempotencyBegin::InProgress => in_progress += 1,
            other => panic!("unexpected concurrent result a: {other:?}"),
        }
        match out_b {
            IdempotencyBegin::Execute(_) => executes += 1,
            IdempotencyBegin::InProgress => in_progress += 1,
            other => panic!("unexpected concurrent result b: {other:?}"),
        }
        assert_eq!(executes, 1);
        assert_eq!(in_progress, 1);

        run_blocked_idempotency_begin_contracts(store).await?;

        Ok(())
    }

    async fn assert_audit_storage_shape(
        store: &PostgresMetadataStore,
        sequence: u64,
    ) -> Result<(), VfsError> {
        let client = store.connect_client().await?;
        let sequence = u64_to_i64(sequence, "audit sequence")?;
        let row = client
            .query_one(
                r#"SELECT action,
                          outcome,
                          jsonb_typeof(actor_json) AS actor_kind,
                          jsonb_typeof(workspace_json) AS workspace_kind,
                          jsonb_typeof(resource_json) AS resource_kind,
                          resource_json->>'path' AS resource_path,
                          jsonb_typeof(details_json) AS details_kind,
                          details_json->>'workspace_id' AS workspace_id
                   FROM audit_events
                   WHERE repo_id IS NULL AND sequence = $1"#,
                &[&sequence],
            )
            .await
            .map_err(|error| postgres_error("load audit storage shape", error))?;

        assert_eq!(row.get::<_, String>("action"), "fs_write_file");
        assert_eq!(row.get::<_, String>("outcome"), "partial");
        assert_eq!(row.get::<_, String>("actor_kind"), "object");
        assert_eq!(
            row.get::<_, Option<String>>("workspace_kind").as_deref(),
            Some("object")
        );
        assert_eq!(row.get::<_, String>("resource_kind"), "object");
        assert_eq!(
            row.get::<_, Option<String>>("resource_path").as_deref(),
            Some("/docs/second.md")
        );
        assert_eq!(row.get::<_, String>("details_kind"), "object");
        assert_eq!(
            row.get::<_, Option<String>>("workspace_id").as_deref(),
            Some("second")
        );
        Ok(())
    }

    async fn run_audit_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
        let first = AuditStore::append(store, audit_event("first")).await?;
        assert_eq!(first.sequence, 1);
        assert_eq!(first.actor.username, "root");
        assert!(first.workspace.is_none());
        assert_eq!(first.action, AuditAction::FsWriteFile);
        assert_eq!(first.resource.path.as_deref(), Some("/docs/first.md"));
        assert_eq!(
            first.details.get("content_hash").map(String::as_str),
            Some("first-hash")
        );

        let second = AuditStore::append(store, workspace_audit_event("second")).await?;
        assert_eq!(second.sequence, 2);
        assert_eq!(second.outcome, AuditOutcome::Partial);
        assert_eq!(second.workspace.as_ref(), Some(&audit_workspace_context()));
        assert_eq!(
            second.details.get("workspace_id").map(String::as_str),
            Some("second")
        );
        assert_audit_storage_shape(store, second.sequence).await?;

        let recent_one = AuditStore::list_recent(store, 1).await?;
        assert_eq!(recent_one.len(), 1);
        assert_eq!(recent_one[0].sequence, second.sequence);

        let recent_all = AuditStore::list_recent(store, 10).await?;
        assert_eq!(
            recent_all
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(recent_all[1], second);

        assert!(AuditStore::list_recent(store, 0).await?.is_empty());

        let policy_event = AuditStore::append(
            store,
            NewAuditEvent::new(
                AuditActor::new(ROOT_UID, "root"),
                AuditAction::PolicyDecisionDeny,
                AuditResource::id(AuditResourceKind::PolicyDecision, "vcs_commit"),
            )
            .with_detail("action", "vcs_commit")
            .with_detail("decision", "deny")
            .with_detail("reason", "protected_ref"),
        )
        .await?;
        assert_eq!(policy_event.sequence, 3);
        assert_eq!(policy_event.action, AuditAction::PolicyDecisionDeny);
        assert_eq!(
            policy_event.resource.kind,
            AuditResourceKind::PolicyDecision
        );
        let recent_policy = AuditStore::list_recent(store, 1).await?;
        assert_eq!(recent_policy[0], policy_event);

        let commit_id = CommitId::from(object_id(b"postgres-audit-vcs-commit"));
        assert!(
            !AuditStore::contains_vcs_commit_event(store, &commit_id.to_hex()).await?,
            "missing VCS commit event should return false"
        );
        let commit_event = AuditStore::append(store, post_cas_audit_event(commit_id)).await?;
        assert!(AuditStore::contains_vcs_commit_event(store, &commit_id.to_hex()).await?);
        let revert_commit_id = CommitId::from(object_id(b"postgres-audit-vcs-revert"));
        let revert_event = AuditStore::append(
            store,
            NewAuditEvent::new(
                AuditActor::new(ROOT_UID, "context-private-user"),
                AuditAction::VcsRevert,
                AuditResource::id(AuditResourceKind::Commit, revert_commit_id.to_hex()),
            ),
        )
        .await?;
        assert!(
            AuditStore::contains_vcs_commit_event(store, &revert_commit_id.to_hex()).await?,
            "VCS revert commit resource should count as visible commit audit"
        );
        let path_commit_id = CommitId::from(object_id(b"postgres-audit-vcs-commit-path"));
        let path_event = AuditStore::append(
            store,
            NewAuditEvent::new(
                AuditActor::new(ROOT_UID, "context-private-user"),
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Commit, path_commit_id.to_hex())
                    .with_path("/private/path"),
            ),
        )
        .await?;
        assert!(
            !AuditStore::contains_vcs_commit_event(store, &path_commit_id.to_hex()).await?,
            "commit resource with path should not count as exact VCS commit audit"
        );
        assert!(
            !AuditStore::contains_vcs_commit_event(store, "context-secret").await?,
            "private audit detail must not be used for matching"
        );
        let fs_recovery_event = AuditStore::append(
            store,
            NewAuditEvent::new(
                AuditActor::new(ROOT_UID, "context-private-user"),
                AuditAction::FsWriteFile,
                AuditResource::path(AuditResourceKind::Path, "/postgres/recovered.md"),
            )
            .with_detail("operation_id", "postgres-op-a")
            .with_detail("target_ref", "agent/postgres/session")
            .with_detail("new_commit", commit_id.to_hex()),
        )
        .await?;
        assert!(
            AuditStore::contains_fs_mutation_recovery_event(
                store,
                AuditAction::FsWriteFile,
                "postgres-op-a",
                "agent/postgres/session",
                &commit_id.to_hex(),
            )
            .await?
        );
        assert!(
            !AuditStore::contains_fs_mutation_recovery_event(
                store,
                AuditAction::FsDelete,
                "postgres-op-a",
                "agent/postgres/session",
                &commit_id.to_hex(),
            )
            .await?,
            "different FS recovery action should not match"
        );

        let store_arc = Arc::new(store.clone());
        let barrier = Arc::new(Barrier::new(2));
        let first_store = store_arc.clone();
        let first_barrier = barrier.clone();
        let concurrent_first = tokio::spawn(async move {
            first_barrier.wait().await;
            AuditStore::append(&*first_store, audit_event("concurrent-a")).await
        });
        let second_store = store_arc.clone();
        let second_barrier = barrier.clone();
        let concurrent_second = tokio::spawn(async move {
            second_barrier.wait().await;
            AuditStore::append(&*second_store, audit_event("concurrent-b")).await
        });

        let first_out = concurrent_first.await.expect("audit append task a")?;
        let second_out = concurrent_second.await.expect("audit append task b")?;
        let mut sequences = vec![first_out.sequence, second_out.sequence];
        sequences.sort_unstable();
        assert_eq!(
            sequences,
            vec![
                fs_recovery_event.sequence + 1,
                fs_recovery_event.sequence + 2
            ]
        );

        let final_recent = AuditStore::list_recent(store, 10).await?;
        let mut expected_sequences = recent_all
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>();
        expected_sequences.push(policy_event.sequence);
        expected_sequences.push(commit_event.sequence);
        expected_sequences.push(revert_event.sequence);
        expected_sequences.push(path_event.sequence);
        expected_sequences.push(fs_recovery_event.sequence);
        expected_sequences.extend(sequences.iter().copied());
        assert_eq!(
            final_recent
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            expected_sequences
        );

        Ok(())
    }

    async fn run_workspace_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
        assert!(
            WorkspaceMetadataStore::list_workspaces(store)
                .await?
                .is_empty()
        );

        let beta = WorkspaceMetadataStore::create_workspace(store, "beta", "/beta").await?;
        assert_eq!(beta.name, "beta");
        assert_eq!(beta.root_path, "/beta");
        assert_eq!(beta.version, 0);
        assert_eq!(beta.base_ref, "main");
        assert!(beta.session_ref.is_none());

        let alpha = WorkspaceMetadataStore::create_workspace_with_refs(
            store,
            "alpha",
            "/alpha",
            "main",
            Some("agent/demo/session"),
        )
        .await?;
        assert_eq!(alpha.name, "alpha");
        assert_eq!(alpha.base_ref, "main");
        assert_eq!(alpha.session_ref.as_deref(), Some("agent/demo/session"));

        let listed = WorkspaceMetadataStore::list_workspaces(store).await?;
        assert_eq!(
            listed
                .iter()
                .map(|workspace| workspace.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );

        let loaded = WorkspaceMetadataStore::get_workspace(store, alpha.id)
            .await?
            .expect("workspace should load");
        assert_eq!(loaded.id, alpha.id);
        assert_eq!(loaded.version, 0);

        let head = workspace_head("workspace alpha head");
        let updated =
            WorkspaceMetadataStore::update_head_commit(store, alpha.id, Some(head.clone()))
                .await?
                .expect("workspace update should return row");
        assert_eq!(updated.head_commit.as_deref(), Some(head.as_str()));
        assert_eq!(updated.version, 1);

        let cleared = WorkspaceMetadataStore::update_head_commit(store, alpha.id, None)
            .await?
            .expect("workspace clear should return row");
        assert!(cleared.head_commit.is_none());
        assert_eq!(cleared.version, 2);
        let fenced = WorkspaceMetadataStore::update_head_commit_if_current(
            store,
            alpha.id,
            None,
            Some(head.clone()),
        )
        .await?
        .expect("workspace compare-and-swap should return row");
        assert_eq!(fenced.head_commit.as_deref(), Some(head.as_str()));
        assert_eq!(fenced.version, 3);
        assert!(
            WorkspaceMetadataStore::update_head_commit_if_current(
                store,
                alpha.id,
                Some("stale-head"),
                Some("rollback".to_string()),
            )
            .await?
            .is_none()
        );
        assert_eq!(
            WorkspaceMetadataStore::get_workspace(store, alpha.id)
                .await?
                .expect("workspace should still exist")
                .head_commit
                .as_deref(),
            Some(head.as_str())
        );
        assert!(
            WorkspaceMetadataStore::update_head_commit(store, Uuid::new_v4(), None)
                .await?
                .is_none()
        );

        assert_workspace_storage_shape(store, alpha.id).await?;
        assert_workspace_ref_corruption_is_rejected(store).await?;

        assert!(matches!(
            WorkspaceMetadataStore::issue_scoped_workspace_token(
                store,
                alpha.id,
                "bad-scope",
                42,
                vec!["/outside".to_string()],
                vec!["/alpha/docs".to_string()],
            )
            .await,
            Err(VfsError::PermissionDenied { .. })
        ));

        let issued = WorkspaceMetadataStore::issue_scoped_workspace_token(
            store,
            alpha.id,
            "alpha-token",
            42,
            vec![
                "/alpha/docs".to_string(),
                "/alpha/docs/../docs".to_string(),
                "/alpha".to_string(),
            ],
            vec!["/alpha/docs".to_string()],
        )
        .await?;
        assert_eq!(issued.token.workspace_id, alpha.id);
        assert_eq!(issued.token.agent_uid, 42);
        assert_eq!(issued.token.principal_uid, Some(42));
        assert_eq!(issued.token.token_version, 1);
        assert_ne!(issued.token.issued_at_unix, 0);
        assert_ne!(issued.token.updated_at_unix, 0);
        assert_eq!(issued.token.expires_at_unix, None);
        assert_eq!(issued.token.revoked_at_unix, None);
        assert_eq!(issued.token.read_prefixes, vec!["/alpha", "/alpha/docs"]);
        assert_eq!(issued.token.write_prefixes, vec!["/alpha/docs"]);
        assert!(!issued.token.secret_hash.eq(&issued.raw_secret));
        assert!(is_lower_hex_sha256(&issued.token.secret_hash));
        assert_workspace_token_storage_shape(store, issued.token.id, &issued.raw_secret).await?;

        let valid =
            WorkspaceMetadataStore::validate_workspace_token(store, alpha.id, &issued.raw_secret)
                .await?
                .expect("issued token should validate");
        assert_eq!(valid.workspace.id, alpha.id);
        assert_eq!(valid.token.id, issued.token.id);
        assert_eq!(valid.token.read_prefixes, vec!["/alpha", "/alpha/docs"]);

        assert!(
            WorkspaceMetadataStore::validate_workspace_token(store, alpha.id, "wrong-secret")
                .await?
                .is_none()
        );
        assert!(
            WorkspaceMetadataStore::validate_workspace_token(store, beta.id, &issued.raw_secret)
                .await?
                .is_none()
        );

        let client = store.connect_client().await?;
        client
            .execute(
                "INSERT INTO repos (id, name) VALUES ($1, $2)",
                &[&"mismatch_repo", &"Mismatch Repo"],
            )
            .await
            .map_err(|error| postgres_error("insert workspace token mismatch repo", error))?;
        client
            .execute(
                "UPDATE workspace_tokens SET repo_id = $2 WHERE id = $1",
                &[&issued.token.id, &"mismatch_repo"],
            )
            .await
            .map_err(|error| postgres_error("corrupt global workspace token repo", error))?;
        drop(client);
        let err =
            WorkspaceMetadataStore::validate_workspace_token(store, alpha.id, &issued.raw_secret)
                .await
                .expect_err("global workspace token with repo id should be corrupt");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        let client = store.connect_client().await?;
        client
            .execute(
                "UPDATE workspace_tokens SET repo_id = NULL WHERE id = $1",
                &[&issued.token.id],
            )
            .await
            .map_err(|error| postgres_error("repair global workspace token repo", error))?;
        drop(client);
        let unrelated_issued = WorkspaceMetadataStore::issue_scoped_workspace_token(
            store,
            alpha.id,
            "alpha-unrelated-token",
            42,
            vec!["/alpha".to_string()],
            vec!["/alpha".to_string()],
        )
        .await?;
        let client = store.connect_client().await?;
        client
            .execute(
                "UPDATE workspace_tokens SET repo_id = $2 WHERE id = $1",
                &[&unrelated_issued.token.id, &"mismatch_repo"],
            )
            .await
            .map_err(|error| {
                postgres_error("corrupt unrelated global workspace token repo", error)
            })?;
        let err =
            WorkspaceMetadataStore::validate_workspace_token(store, alpha.id, &issued.raw_secret)
                .await
                .expect_err("any repo-mismatched workspace token row should be corrupt");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        client
            .execute(
                "DELETE FROM workspace_tokens WHERE id = $1",
                &[&unrelated_issued.token.id],
            )
            .await
            .map_err(|error| {
                postgres_error("delete corrupt unrelated global workspace token", error)
            })?;

        client
            .execute(
                "INSERT INTO repos (id, name) VALUES ($1, $2)",
                &[&"workspace_repo", &"Workspace Repo"],
            )
            .await
            .map_err(|error| postgres_error("insert workspace contract repo", error))?;
        client
            .execute(
                r#"INSERT INTO durable_principals (
                       uid, repo_id, username, primary_gid, groups_json, kind, active
                   )
                   VALUES ($1, $2, $3, $4, $5, 'agent', true)"#,
                &[
                    &501_i32,
                    &"workspace_repo",
                    &"repo-agent",
                    &601_i32,
                    &Json(&vec![601_i32, 602_i32]),
                ],
            )
            .await
            .map_err(|error| postgres_error("insert workspace contract principal", error))?;
        let repo_workspace_id = Uuid::new_v4();
        let repo_workspace_row = client
            .query_one(
                r#"INSERT INTO workspaces (
                       id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   )
                   VALUES ($1, 'workspace_repo', 'repo-alpha', '/repo-alpha', NULL, 0, 'main', NULL)
                   RETURNING id, repo_id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&repo_workspace_id],
            )
            .await
            .map_err(|error| postgres_error("insert repo-scoped workspace", error))?;
        let repo_workspace = row_to_workspace_record(repo_workspace_row)?;

        let repo_issued = WorkspaceMetadataStore::issue_scoped_workspace_token(
            store,
            repo_workspace.id,
            "repo-token",
            501,
            vec!["/repo-alpha".to_string()],
            vec!["/repo-alpha".to_string()],
        )
        .await?;
        let repo_valid = WorkspaceMetadataStore::validate_workspace_token(
            store,
            repo_workspace.id,
            &repo_issued.raw_secret,
        )
        .await?
        .expect("repo-scoped token should validate with active principal");
        let principal = repo_valid
            .principal
            .expect("repo-scoped token should return principal");
        assert_eq!(repo_valid.repo_id.as_deref(), Some("workspace_repo"));
        assert_eq!(principal.uid, 501);
        assert_eq!(principal.username, "repo-agent");
        assert_eq!(principal.gid, 601);
        assert_eq!(principal.groups, vec![601, 602]);
        assert_eq!(principal.kind, WorkspacePrincipalKind::Agent);
        assert!(principal.active);

        let repo_expires_at = u64_to_i64(
            repo_issued.token.issued_at_unix + 1,
            "workspace contract token expiration time",
        )?;
        client
            .execute(
                "UPDATE workspace_tokens SET expires_at = to_timestamp($2::bigint::double precision) WHERE id = $1",
                &[&repo_issued.token.id, &repo_expires_at],
            )
            .await
            .map_err(|error| postgres_error("expire workspace contract token", error))?;
        assert!(
            WorkspaceMetadataStore::validate_workspace_token_at(
                store,
                repo_workspace.id,
                &repo_issued.raw_secret,
                repo_issued.token.issued_at_unix + 1,
            )
            .await?
            .is_none()
        );
        client
            .execute(
                "UPDATE workspace_tokens SET expires_at = NULL WHERE id = $1",
                &[&repo_issued.token.id],
            )
            .await
            .map_err(|error| postgres_error("clear workspace contract token expiration", error))?;

        client
            .execute(
                "UPDATE durable_principals SET active = false WHERE uid = $1",
                &[&501_i32],
            )
            .await
            .map_err(|error| postgres_error("deactivate workspace contract principal", error))?;
        assert!(
            WorkspaceMetadataStore::validate_workspace_token(
                store,
                repo_workspace.id,
                &repo_issued.raw_secret,
            )
            .await?
            .is_none()
        );

        client
            .execute(
                "UPDATE durable_principals SET active = true WHERE uid = $1",
                &[&501_i32],
            )
            .await
            .map_err(|error| postgres_error("reactivate workspace contract principal", error))?;
        client
            .execute(
                "UPDATE workspace_tokens SET principal_uid = $2 WHERE id = $1",
                &[&repo_issued.token.id, &777_i32],
            )
            .await
            .map_err(|error| postgres_error("detach workspace contract token principal", error))?;
        assert!(
            WorkspaceMetadataStore::validate_workspace_token(
                store,
                repo_workspace.id,
                &repo_issued.raw_secret,
            )
            .await?
            .is_none()
        );

        client
            .execute(
                "UPDATE workspace_tokens SET principal_uid = $2 WHERE id = $1",
                &[&repo_issued.token.id, &501_i32],
            )
            .await
            .map_err(|error| {
                postgres_error("reattach workspace contract token principal", error)
            })?;
        client
            .execute(
                "UPDATE workspace_tokens SET read_prefixes_json = $2 WHERE id = $1",
                &[
                    &repo_issued.token.id,
                    &Json(&vec!["/outside-repo-alpha".to_string()]),
                ],
            )
            .await
            .map_err(|error| postgres_error("corrupt workspace contract token prefixes", error))?;
        let err = WorkspaceMetadataStore::validate_workspace_token(
            store,
            repo_workspace.id,
            &repo_issued.raw_secret,
        )
        .await
        .expect_err("corrupt workspace token prefixes should be rejected");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        client
            .execute(
                "UPDATE workspace_tokens SET read_prefixes_json = $2 WHERE id = $1",
                &[
                    &repo_issued.token.id,
                    &Json(&vec!["/repo-alpha".to_string()]),
                ],
            )
            .await
            .map_err(|error| postgres_error("repair workspace contract token prefixes", error))?;

        let revoked = WorkspaceMetadataStore::revoke_workspace_token(
            store,
            repo_workspace.id,
            repo_issued.token.id,
            repo_issued.token.issued_at_unix + 1,
        )
        .await?
        .expect("repo-scoped token should revoke");
        assert_eq!(revoked.token_version, repo_issued.token.token_version + 1);
        assert!(revoked.revoked_at_unix.is_some());
        assert!(
            WorkspaceMetadataStore::validate_workspace_token(
                store,
                repo_workspace.id,
                &repo_issued.raw_secret,
            )
            .await?
            .is_none()
        );

        let default_issued =
            WorkspaceMetadataStore::issue_workspace_token(store, beta.id, "beta-token", 43).await?;
        assert_eq!(default_issued.token.read_prefixes, vec!["/beta"]);
        assert_eq!(default_issued.token.write_prefixes, vec!["/beta"]);
        assert!(
            WorkspaceMetadataStore::validate_workspace_token(
                store,
                beta.id,
                &default_issued.raw_secret
            )
            .await?
            .is_some()
        );

        Ok(())
    }

    fn review_repo() -> RepoId {
        RepoId::local()
    }

    async fn seed_review_commits(
        store: &PostgresMetadataStore,
    ) -> Result<(CommitRecord, CommitRecord), VfsError> {
        let repo_id = review_repo();
        let base_tree = object_id(b"review-base-tree");
        let head_tree = object_id(b"review-head-tree");
        ObjectMetadataStore::put(
            store,
            object_record(&repo_id, base_tree, ObjectKind::Tree, b"review-base-tree"),
        )
        .await?;
        ObjectMetadataStore::put(
            store,
            object_record(&repo_id, head_tree, ObjectKind::Tree, b"review-head-tree"),
        )
        .await?;

        let base = commit_record(
            &repo_id,
            commit_id("review-base"),
            base_tree,
            Vec::new(),
            10,
            "review base",
        );
        let head = commit_record(
            &repo_id,
            commit_id("review-head"),
            head_tree,
            vec![base.id],
            11,
            "review head",
        );
        CommitStore::insert(store, base.clone()).await?;
        CommitStore::insert(store, head.clone()).await?;
        Ok((base, head))
    }

    async fn assert_review_corrupt_active_approval_is_rejected(
        store: &PostgresMetadataStore,
        approval_id: Uuid,
        change_request_id: Uuid,
    ) -> Result<(), VfsError> {
        let client = store.connect_client().await?;
        client
            .execute(
                "UPDATE approvals
                 SET active = true, dismissed_by = 99, dismissal_reason = NULL
                 WHERE id = $1",
                &[&approval_id],
            )
            .await
            .map_err(|error| postgres_error("corrupt review approval", error))?;
        drop(client);

        let err = ReviewStore::list_approvals(store, change_request_id)
            .await
            .expect_err("corrupt active approval should be rejected");
        assert!(matches!(err, VfsError::CorruptStore { .. }));

        let client = store.connect_client().await?;
        client
            .execute("DELETE FROM approvals WHERE id = $1", &[&approval_id])
            .await
            .map_err(|error| postgres_error("delete corrupt review approval", error))?;
        Ok(())
    }

    async fn assert_review_accepts_unseeded_local_commit_ids(
        store: &PostgresMetadataStore,
    ) -> Result<(), VfsError> {
        let change = ReviewStore::create_change_request(
            store,
            NewChangeRequest {
                title: "Local opaque commits".to_string(),
                description: None,
                source_ref: "review/local-opaque".to_string(),
                target_ref: "main".to_string(),
                base_commit: "e".repeat(64),
                head_commit: "f".repeat(64),
                created_by: 10,
            },
        )
        .await?;
        assert_eq!(change.base_commit, "e".repeat(64));
        assert_eq!(change.head_commit, "f".repeat(64));
        Ok(())
    }

    async fn run_review_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
        let (base, head) = seed_review_commits(store).await?;

        assert!(
            ReviewStore::list_protected_ref_rules(store)
                .await?
                .is_empty()
        );
        assert!(
            ReviewStore::list_protected_path_rules(store)
                .await?
                .is_empty()
        );
        assert!(ReviewStore::list_change_requests(store).await?.is_empty());
        assert_review_accepts_unseeded_local_commit_ids(store).await?;

        let ref_rule = ReviewStore::create_protected_ref_rule_for_repo(
            store,
            &RepoId::local(),
            "main",
            2,
            10,
            false,
        )
        .await?;
        assert_eq!(ref_rule.ref_name, "main");
        assert_eq!(ref_rule.required_approvals, 2);
        assert!(!ref_rule.require_all_files_viewed);
        assert!(ref_rule.active);
        assert_eq!(
            ReviewStore::get_protected_ref_rule(store, ref_rule.id).await?,
            Some(ref_rule.clone())
        );

        let path_rule = ReviewStore::create_protected_path_rule_for_repo(
            store,
            &RepoId::local(),
            "/legal",
            Some("main"),
            3,
            10,
            false,
        )
        .await?;
        assert_eq!(path_rule.path_prefix, "/legal");
        assert_eq!(path_rule.target_ref.as_deref(), Some("main"));
        assert!(!path_rule.require_all_files_viewed);
        assert!(path_rule.matches_path("/legal/contract.txt"));
        assert_eq!(
            ReviewStore::get_protected_path_rule(store, path_rule.id).await?,
            Some(path_rule.clone())
        );

        let change = ReviewStore::create_change_request(
            store,
            NewChangeRequest {
                title: " Legal update ".to_string(),
                description: Some("Needs review".to_string()),
                source_ref: "review/legal-update".to_string(),
                target_ref: "main".to_string(),
                base_commit: base.id.to_hex(),
                head_commit: head.id.to_hex(),
                created_by: 10,
            },
        )
        .await?;
        assert_eq!(change.title, "Legal update");
        assert_eq!(change.status, ChangeRequestStatus::Open);
        assert_eq!(change.version, 1);
        assert_eq!(
            ReviewStore::get_change_request(store, change.id).await?,
            Some(change.clone())
        );

        let decision =
            ReviewStore::approval_decision(store, change.id, &["/legal/contract.txt".to_string()])
                .await?
                .expect("approval decision should exist");
        assert_eq!(decision.required_approvals, 3);
        assert_eq!(decision.approval_count, 0);
        assert!(!decision.approved);
        assert_eq!(decision.matched_ref_rules, vec![ref_rule.id]);
        assert_eq!(decision.matched_path_rules, vec![path_rule.id]);

        let first_approval = ReviewStore::create_approval(
            store,
            NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 20,
                comment: Some(" Looks good ".to_string()),
            },
        )
        .await?;
        assert!(first_approval.created);
        assert_eq!(first_approval.record.comment.as_deref(), Some("Looks good"));

        let duplicate_approval = ReviewStore::create_approval(
            store,
            NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 20,
                comment: Some("different comment ignored on duplicate".to_string()),
            },
        )
        .await?;
        assert_eq!(
            duplicate_approval,
            ApprovalRecordMutation {
                record: first_approval.record.clone(),
                created: false,
            }
        );

        let assignment = ReviewStore::assign_reviewer(
            store,
            NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 30,
                assigned_by: 10,
                required: true,
            },
        )
        .await?;
        assert!(assignment.created);
        assert!(assignment.assignment.required);

        let same_assignment = ReviewStore::assign_reviewer(
            store,
            NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 30,
                assigned_by: 10,
                required: true,
            },
        )
        .await?;
        assert!(!same_assignment.created);
        assert!(!same_assignment.updated);

        let missing_required_reviewer =
            ReviewStore::approval_decision(store, change.id, &["/legal/contract.txt".to_string()])
                .await?
                .expect("approval decision should exist");
        assert_eq!(missing_required_reviewer.approval_count, 1);
        assert_eq!(missing_required_reviewer.required_reviewers, vec![30]);
        assert!(
            missing_required_reviewer
                .approved_required_reviewers
                .is_empty()
        );
        assert_eq!(
            missing_required_reviewer.missing_required_reviewers,
            vec![30]
        );
        assert!(!missing_required_reviewer.approved);

        let required_reviewer_approval = ReviewStore::create_approval(
            store,
            NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 30,
                comment: None,
            },
        )
        .await?;
        assert!(required_reviewer_approval.created);

        let satisfied_required_reviewer =
            ReviewStore::approval_decision(store, change.id, &["/legal/contract.txt".to_string()])
                .await?
                .expect("approval decision should exist");
        assert_eq!(satisfied_required_reviewer.approval_count, 2);
        assert_eq!(satisfied_required_reviewer.required_reviewers, vec![30]);
        assert_eq!(
            satisfied_required_reviewer.approved_required_reviewers,
            vec![30]
        );
        assert!(
            satisfied_required_reviewer
                .missing_required_reviewers
                .is_empty()
        );
        assert!(!satisfied_required_reviewer.approved);

        let optional_assignment = ReviewStore::assign_reviewer(
            store,
            NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 30,
                assigned_by: 11,
                required: false,
            },
        )
        .await?;
        assert!(!optional_assignment.created);
        assert!(optional_assignment.updated);
        assert!(!optional_assignment.assignment.required);
        assert_eq!(optional_assignment.assignment.version, 2);

        let optional_required_reviewer =
            ReviewStore::approval_decision(store, change.id, &["/legal/contract.txt".to_string()])
                .await?
                .expect("approval decision should exist");
        assert_eq!(optional_required_reviewer.approval_count, 2);
        assert!(optional_required_reviewer.required_reviewers.is_empty());
        assert!(
            optional_required_reviewer
                .approved_required_reviewers
                .is_empty()
        );
        assert!(
            optional_required_reviewer
                .missing_required_reviewers
                .is_empty()
        );
        assert!(!optional_required_reviewer.approved);

        let comment = ReviewStore::create_comment(
            store,
            NewReviewComment {
                change_request_id: change.id,
                author: 20,
                body: " Please update the summary ".to_string(),
                path: Some(" /legal/contract.txt ".to_string()),
                kind: ReviewCommentKind::ChangesRequested,
            },
        )
        .await?;
        assert!(comment.created);
        assert_eq!(comment.comment.body, "Please update the summary");
        assert_eq!(comment.comment.path.as_deref(), Some("/legal/contract.txt"));

        let dismissed = ReviewStore::dismiss_approval(
            store,
            DismissApprovalInput {
                change_request_id: change.id,
                approval_id: first_approval.record.id,
                dismissed_by: 10,
                reason: Some(" stale approval ".to_string()),
            },
        )
        .await?;
        assert!(dismissed.dismissed);
        assert!(!dismissed.record.active);
        assert_eq!(
            dismissed.record.dismissal_reason.as_deref(),
            Some("stale approval")
        );
        assert_eq!(dismissed.record.version, 2);

        let after_dismissal =
            ReviewStore::approval_decision(store, change.id, &["/legal/contract.txt".to_string()])
                .await?
                .expect("approval decision should exist");
        assert_eq!(after_dismissal.approval_count, 1);
        assert_eq!(after_dismissal.approved_by, vec![30]);
        assert!(!after_dismissal.approved);

        let replacement_approval = ReviewStore::create_approval(
            store,
            NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 20,
                comment: None,
            },
        )
        .await?;
        assert!(replacement_approval.created);
        assert_ne!(replacement_approval.record.id, first_approval.record.id);

        assert_review_corrupt_active_approval_is_rejected(
            store,
            replacement_approval.record.id,
            change.id,
        )
        .await?;

        let rejected =
            ReviewStore::transition_change_request(store, change.id, ChangeRequestStatus::Rejected)
                .await?
                .expect("change request should transition");
        assert_eq!(rejected.status, ChangeRequestStatus::Rejected);
        assert_eq!(rejected.version, 2);
        assert!(
            ReviewStore::create_comment(
                store,
                NewReviewComment {
                    change_request_id: change.id,
                    author: 20,
                    body: "late comment".to_string(),
                    path: None,
                    kind: ReviewCommentKind::General,
                },
            )
            .await
            .is_err()
        );

        Ok(())
    }

    async fn seed_pending_idempotency_row(
        tx: &tokio_postgres::Transaction<'_>,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Result<(), VfsError> {
        tx.execute(
            r#"INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state)
               VALUES ($1, $2, $3, 'pending')"#,
            &[&scope, &key.key_hash(), &request_fingerprint],
        )
        .await
        .map_err(|error| postgres_error("seed pending idempotency row", error))?;
        Ok(())
    }

    async fn run_blocked_idempotency_begin_contracts(
        store: &PostgresMetadataStore,
    ) -> Result<(), VfsError> {
        const COMMIT_SCOPE: &str = "runs:create:blocked-commit";
        let commit_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("blocked-commit"))
                .unwrap();
        let commit_request = idempotency_fingerprint(COMMIT_SCOPE, "request-a")?;
        let mut blocker = store.connect_client().await?;
        let tx = blocker
            .transaction()
            .await
            .map_err(|error| postgres_error("blocked idempotency begin transaction", error))?;
        seed_pending_idempotency_row(&tx, COMMIT_SCOPE, &commit_key, &commit_request).await?;

        let store_for_commit = store.clone();
        let commit_key_for_begin = commit_key.clone();
        let commit_request_for_begin = commit_request.clone();
        let blocked_commit = tokio::spawn(async move {
            store_for_commit
                .begin(
                    COMMIT_SCOPE,
                    &commit_key_for_begin,
                    &commit_request_for_begin,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !blocked_commit.is_finished(),
            "begin should wait for an uncommitted idempotency conflict"
        );
        tx.commit()
            .await
            .map_err(|error| postgres_error("commit blocked idempotency row", error))?;
        assert!(matches!(
            blocked_commit.await.expect("blocked commit join")?,
            IdempotencyBegin::InProgress
        ));

        const ROLLBACK_SCOPE: &str = "runs:create:blocked-rollback";
        let rollback_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("blocked-rollback"))
                .unwrap();
        let rollback_request = idempotency_fingerprint(ROLLBACK_SCOPE, "request-a")?;
        let mut blocker = store.connect_client().await?;
        let tx = blocker
            .transaction()
            .await
            .map_err(|error| postgres_error("blocked idempotency rollback transaction", error))?;
        seed_pending_idempotency_row(&tx, ROLLBACK_SCOPE, &rollback_key, &rollback_request).await?;

        let store_for_rollback = store.clone();
        let rollback_key_for_begin = rollback_key.clone();
        let rollback_request_for_begin = rollback_request.clone();
        let blocked_rollback = tokio::spawn(async move {
            store_for_rollback
                .begin(
                    ROLLBACK_SCOPE,
                    &rollback_key_for_begin,
                    &rollback_request_for_begin,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !blocked_rollback.is_finished(),
            "begin should wait for an uncommitted idempotency conflict"
        );
        tx.rollback()
            .await
            .map_err(|error| postgres_error("rollback blocked idempotency row", error))?;
        let reservation = match blocked_rollback.await.expect("blocked rollback join")? {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute after conflicting insert rollback, got {other:?}"),
        };
        store.abort(&reservation).await;

        Ok(())
    }

    async fn expire_cleanup_claim(
        store: &PostgresMetadataStore,
        claim: &ObjectCleanupClaim,
    ) -> Result<(), VfsError> {
        let client = store.connect_client().await?;
        client
            .execute(
                "UPDATE object_cleanup_claims
                 SET lease_expires_at = clock_timestamp() - interval '1 second',
                     updated_at = clock_timestamp()
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_key = $3
                     AND lease_token = $4",
                &[
                    &claim.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(claim.claim_kind),
                    &claim.object_key,
                    &claim.lease_token.to_string(),
                ],
            )
            .await
            .map_err(|error| postgres_error("expire cleanup claim", error))?;
        Ok(())
    }

    async fn load_cleanup_claim_for_request(
        store: &PostgresMetadataStore,
        request: &ObjectCleanupClaimRequest,
    ) -> Result<Option<CleanupClaimRow>, VfsError> {
        let client = store.connect_client().await?;
        let row = client
            .query_opt(
                "SELECT repo_id, claim_kind, object_kind, object_id, object_key,
                     lease_owner, lease_token, lease_expires_at, attempts,
                     completed_at, last_error
                 FROM object_cleanup_claims
                 WHERE repo_id = $1
                     AND claim_kind = $2
                     AND object_key = $3",
                &[
                    &request.repo_id.as_str(),
                    &cleanup_claim_kind_to_db(request.claim_kind),
                    &request.object_key,
                ],
            )
            .await
            .map_err(|error| postgres_error("load cleanup claim for request", error))?;
        row.map(|row| {
            let completed_at = row.get("completed_at");
            let last_error = row.get("last_error");
            Ok(CleanupClaimRow {
                claim: row_to_cleanup_claim(row)?,
                completed_at,
                last_error,
            })
        })
        .transpose()
    }

    async fn run_post_cas_recovery_claim_contracts(
        store: &PostgresMetadataStore,
        repo_id: &RepoId,
        visible_commit_id: CommitId,
        context_commit_id: CommitId,
        active_no_context_commit_id: CommitId,
    ) -> Result<(), VfsError> {
        let target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            visible_commit_id,
            DurableCorePostCasStep::WorkspaceHeadUpdate,
        )?;
        DurableCorePostCasRecoveryClaimStore::enqueue(store, target.clone(), 100).await?;
        DurableCorePostCasRecoveryClaimStore::enqueue(store, target.clone(), 101).await?;

        let statuses = DurableCorePostCasRecoveryClaimStore::list(store, 10).await?;
        let status = statuses
            .iter()
            .find(|status| status.target() == &target)
            .expect("pending recovery status");
        assert_eq!(status.state(), DurableCorePostCasRecoveryState::Pending);
        assert_eq!(status.attempts(), 0);
        let counts = DurableCorePostCasRecoveryClaimStore::counts(store).await?;
        assert_eq!(counts.pending(), 1);
        assert_eq!(counts.total(), 1);

        let missing_target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            visible_commit_id,
            DurableCorePostCasStep::AuditAppend,
        )?;
        assert!(
            DurableCorePostCasRecoveryClaimStore::claim(
                store,
                DurableCorePostCasRecoveryClaimRequest::new(
                    missing_target.clone(),
                    "postgres-worker",
                    Duration::from_secs(1),
                    199,
                )?,
            )
            .await?
            .is_none()
        );
        assert!(
            DurableCorePostCasRecoveryClaimStore::list(store, 10)
                .await?
                .iter()
                .all(|status| status.target() != &missing_target)
        );

        let first = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                target.clone(),
                "postgres-worker",
                Duration::from_millis(1),
                200,
            )?,
        )
        .await?
        .expect("pending target should be claimable");
        assert_eq!(first.attempts(), 1);
        assert!(
            first.context().is_none(),
            "contextless enqueue must claim without repair context"
        );

        let duplicate = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                target.clone(),
                "postgres-worker-2",
                Duration::from_secs(1),
                200,
            )?,
        )
        .await?;
        assert!(duplicate.is_none());

        tokio::time::sleep(Duration::from_millis(5)).await;
        let retry = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                target.clone(),
                "postgres-worker",
                Duration::from_secs(1),
                210,
            )?,
        )
        .await?
        .expect("expired lease should be claimable");
        assert_eq!(retry.attempts(), 2);
        assert_ne!(retry.token(), first.token());

        let stale_complete = DurableCorePostCasRecoveryClaimStore::complete(store, &first, 211)
            .await
            .expect_err("stale token must not complete retry");
        assert!(matches!(stale_complete, VfsError::InvalidArgs { .. }));

        let stale_owner = DurableCorePostCasRecoveryClaim::for_store(
            retry.target().clone(),
            "different-postgres-worker",
            retry.token(),
            retry.attempts(),
            retry.expires_at_millis(),
        );
        let stale_owner_failure = DurableCorePostCasRecoveryClaimStore::record_failure(
            store,
            &stale_owner,
            "raw stale owner failure",
            Duration::from_millis(1),
            212,
        )
        .await
        .expect_err("stale owner must not fence a retry");
        assert!(matches!(stale_owner_failure, VfsError::InvalidArgs { .. }));
        let oversized_backoff = DurableCorePostCasRecoveryClaimStore::record_failure(
            store,
            &retry,
            "raw oversized backoff failure",
            Duration::from_secs(3_601),
            212,
        )
        .await
        .expect_err("oversized backoff must be rejected");
        assert!(matches!(oversized_backoff, VfsError::InvalidArgs { .. }));

        DurableCorePostCasRecoveryClaimStore::record_failure(
            store,
            &retry,
            "raw /private/path idempotency-token postgres detail",
            Duration::from_millis(1),
            212,
        )
        .await?;
        let rendered = format!(
            "{:?}",
            DurableCorePostCasRecoveryClaimStore::list(store, 10).await?
        );
        assert!(rendered.contains("redacted post-CAS recovery failure"));
        assert!(!rendered.contains("/private/path"));
        assert!(!rendered.contains("idempotency-token"));
        assert!(!rendered.contains(retry.token()));

        tokio::time::sleep(Duration::from_millis(5)).await;
        let retry_after_backoff = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                target.clone(),
                "postgres-worker",
                Duration::from_secs(1),
                220,
            )?,
        )
        .await?
        .expect("elapsed backoff should be claimable");
        assert_eq!(retry_after_backoff.attempts(), 3);
        DurableCorePostCasRecoveryClaimStore::complete(store, &retry_after_backoff, 221).await?;
        assert!(
            DurableCorePostCasRecoveryClaimStore::claim(
                store,
                DurableCorePostCasRecoveryClaimRequest::new(
                    target,
                    "postgres-worker",
                    Duration::from_secs(1),
                    222,
                )?,
            )
            .await?
            .is_none()
        );

        let ordered_commit_id = commit_id("postgres-post-cas-ordering");
        let ordered_tree_id = object_id(b"postgres-post-cas-ordering-tree");
        ObjectMetadataStore::put(
            store,
            object_record(
                repo_id,
                ordered_tree_id,
                ObjectKind::Tree,
                b"postgres-post-cas-ordering-tree",
            ),
        )
        .await?;
        CommitStore::insert(
            store,
            commit_record(
                repo_id,
                ordered_commit_id,
                ordered_tree_id,
                vec![visible_commit_id],
                4,
                "postgres post-CAS ordering",
            ),
        )
        .await?;
        for (step, now_millis) in [
            (DurableCorePostCasStep::WorkspaceHeadUpdate, 230),
            (DurableCorePostCasStep::AuditAppend, 231),
            (DurableCorePostCasStep::IdempotencyCompletion, 232),
        ] {
            DurableCorePostCasRecoveryClaimStore::enqueue(
                store,
                DurableCorePostCasRecoveryTarget::new(
                    repo_id.clone(),
                    MAIN_REF,
                    ordered_commit_id,
                    step,
                )?,
                now_millis,
            )
            .await?;
        }
        let ordered_steps =
            DurableCorePostCasRecoveryClaimStore::list_repair_candidates(store, 233, 3)
                .await?
                .into_iter()
                .map(|status| status.target().step())
                .collect::<Vec<_>>();
        assert_eq!(
            ordered_steps,
            vec![
                DurableCorePostCasStep::WorkspaceHeadUpdate,
                DurableCorePostCasStep::AuditAppend,
                DurableCorePostCasStep::IdempotencyCompletion
            ]
        );

        let route_claim_commit_id = commit_id("postgres-post-cas-route-claim");
        let route_claim_tree_id = object_id(b"postgres-post-cas-route-claim-tree");
        ObjectMetadataStore::put(
            store,
            object_record(
                repo_id,
                route_claim_tree_id,
                ObjectKind::Tree,
                b"postgres-post-cas-route-claim-tree",
            ),
        )
        .await?;
        CommitStore::insert(
            store,
            commit_record(
                repo_id,
                route_claim_commit_id,
                route_claim_tree_id,
                vec![visible_commit_id],
                5,
                "postgres post-CAS route claim",
            ),
        )
        .await?;

        let route_claim_target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            route_claim_commit_id,
            DurableCorePostCasStep::AuditAppend,
        )?;
        let route_claim_context = post_cas_recovery_context(route_claim_target.commit_id());
        DurableCorePostCasRecoveryClaimStore::enqueue_with_context(
            store,
            route_claim_target.clone(),
            route_claim_context.clone(),
            240,
        )
        .await?;
        let route_claim = DurableCorePostCasRecoveryClaimStore::enqueue_with_context_and_claim(
            store,
            route_claim_target.clone(),
            route_claim_context.clone(),
            "postgres-route-worker",
            Duration::from_secs(1),
            241,
        )
        .await?
        .expect("route should atomically claim existing pending post-CAS work");
        assert_eq!(route_claim.context(), Some(&route_claim_context));
        assert!(
            DurableCorePostCasRecoveryClaimStore::claim(
                store,
                DurableCorePostCasRecoveryClaimRequest::new(
                    route_claim_target,
                    "postgres-racing-worker",
                    Duration::from_secs(1),
                    242,
                )?,
            )
            .await?
            .is_none(),
            "worker must not claim a route-owned post-CAS row"
        );

        let poison_target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            visible_commit_id,
            DurableCorePostCasStep::AuditAppend,
        )?;
        DurableCorePostCasRecoveryClaimStore::enqueue(store, poison_target.clone(), 299).await?;
        let poison = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                poison_target.clone(),
                "postgres-worker",
                Duration::from_secs(1),
                300,
            )?,
        )
        .await?
        .expect("enqueued target should be claimable");
        DurableCorePostCasRecoveryClaimStore::poison(store, &poison, "raw poison /secret", 301)
            .await?;
        assert!(
            DurableCorePostCasRecoveryClaimStore::claim(
                store,
                DurableCorePostCasRecoveryClaimRequest::new(
                    poison_target,
                    "postgres-worker",
                    Duration::from_secs(1),
                    302,
                )?,
            )
            .await?
            .is_none()
        );

        let contextual_target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            context_commit_id,
            DurableCorePostCasStep::WorkspaceHeadUpdate,
        )?;
        let context = post_cas_recovery_context(context_commit_id);
        DurableCorePostCasRecoveryClaimStore::enqueue(store, contextual_target.clone(), 400)
            .await?;
        DurableCorePostCasRecoveryClaimStore::enqueue_with_context(
            store,
            contextual_target.clone(),
            context.clone(),
            401,
        )
        .await?;
        DurableCorePostCasRecoveryClaimStore::enqueue_with_context(
            store,
            contextual_target.clone(),
            DurableCorePostCasRecoveryContext::new(None, None, None, None),
            402,
        )
        .await?;
        let contextual_claim = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                contextual_target.clone(),
                "postgres-context-worker",
                Duration::from_secs(1),
                403,
            )?,
        )
        .await?
        .expect("context-upgraded row should be claimable");
        assert_eq!(contextual_claim.context(), Some(&context));

        let partial_context_target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            context_commit_id,
            DurableCorePostCasStep::IdempotencyCompletion,
        )?;
        DurableCorePostCasRecoveryClaimStore::enqueue_with_context(
            store,
            partial_context_target.clone(),
            post_cas_recovery_context_with_response_kind(
                context_commit_id,
                DurableCorePostCasIdempotencyResponseKind::FullCommit,
            ),
            450,
        )
        .await?;
        let partial_context = post_cas_recovery_context_with_response_kind(
            context_commit_id,
            DurableCorePostCasIdempotencyResponseKind::Partial,
        );
        DurableCorePostCasRecoveryClaimStore::enqueue_with_context(
            store,
            partial_context_target.clone(),
            partial_context.clone(),
            451,
        )
        .await?;
        let partial_claim = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                partial_context_target,
                "postgres-partial-worker",
                Duration::from_secs(1),
                452,
            )?,
        )
        .await?
        .expect("partial-updated row should be claimable");
        assert_eq!(partial_claim.context(), Some(&partial_context));

        let active_no_context_target = DurableCorePostCasRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            active_no_context_commit_id,
            DurableCorePostCasStep::IdempotencyCompletion,
        )?;
        DurableCorePostCasRecoveryClaimStore::enqueue(store, active_no_context_target.clone(), 500)
            .await?;
        let _active_no_context_claim = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                active_no_context_target.clone(),
                "postgres-active-context-worker",
                Duration::from_secs(1),
                501,
            )?,
        )
        .await?
        .expect("active no-context row should be claimable");
        let active_upgrade_err = DurableCorePostCasRecoveryClaimStore::enqueue_with_context(
            store,
            active_no_context_target.clone(),
            post_cas_recovery_context(active_no_context_commit_id),
            502,
        )
        .await
        .expect_err("active no-context row must not be context-upgraded");
        assert!(matches!(active_upgrade_err, VfsError::CorruptStore { .. }));
        let rendered = active_upgrade_err.to_string();
        assert!(!rendered.contains("context-secret"));
        assert!(!rendered.contains("context-reservation-token"));

        let client = store.connect_client().await?;
        client
            .execute(
                "UPDATE durable_post_cas_recovery_claims
                 SET context_json = jsonb_build_object('workspace_id', 7)
                 WHERE repo_id = $1
                     AND ref_name = $2
                     AND commit_id = $3
                     AND step = $4",
                &[
                    &contextual_target.repo_id().as_str(),
                    &contextual_target.ref_name(),
                    &contextual_target.commit_id().to_hex(),
                    &contextual_target.step().as_str(),
                ],
            )
            .await
            .map_err(|error| postgres_error("corrupt post-CAS recovery context", error))?;
        drop(client);
        let corrupt_context_err = DurableCorePostCasRecoveryClaimStore::claim(
            store,
            DurableCorePostCasRecoveryClaimRequest::new(
                contextual_target,
                "postgres-corrupt-context-worker",
                Duration::from_secs(1),
                2_000,
            )?,
        )
        .await
        .expect_err("corrupt context JSON must be rejected");
        assert!(matches!(corrupt_context_err, VfsError::CorruptStore { .. }));
        assert!(!corrupt_context_err.to_string().contains("workspace_id"));

        Ok(())
    }

    async fn run_pre_visibility_recovery_contracts(
        store: &PostgresMetadataStore,
        repo_id: &RepoId,
    ) -> Result<(), VfsError> {
        let pre_commit_id = commit_id("pre-visibility-unconfirmed");
        let parent_id = commit_id("pre-visibility-parent");
        let target = DurableCorePreVisibilityRecoveryTarget::new(
            repo_id.clone(),
            MAIN_REF,
            pre_commit_id,
            DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
        )?;
        let record = DurableCorePreVisibilityRecoveryRecord::new(
            target.clone(),
            object_id(b"pre-visibility-root"),
            Some(parent_id),
            RefVersion::new(2).unwrap(),
            3,
            1,
            false,
            700,
        );
        DurableCorePreVisibilityRecoveryStore::record(store, record.clone()).await?;
        let context = post_cas_recovery_context(pre_commit_id);
        let later = DurableCorePreVisibilityRecoveryRecord::new(
            target.clone(),
            record.root_tree_id(),
            record.parent_commit_id(),
            record.expected_ref_version(),
            record.object_count(),
            record.changed_path_count(),
            true,
            701,
        )
        .with_post_cas_context(context.clone());
        DurableCorePreVisibilityRecoveryStore::record(store, later).await?;

        let statuses = DurableCorePreVisibilityRecoveryStore::list(store, 10).await?;
        let status = statuses
            .iter()
            .find(|status| status.target() == &target)
            .expect("pre-visibility recovery status");
        assert_eq!(
            status.state(),
            DurableCorePreVisibilityRecoveryState::Pending
        );
        assert_eq!(
            status.target().stage(),
            DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert
        );
        assert_eq!(status.target().commit_id(), pre_commit_id);
        assert_eq!(status.root_tree_id(), record.root_tree_id());
        assert_eq!(status.parent_commit_id(), Some(parent_id));
        assert_eq!(status.expected_ref_version(), RefVersion::new(2).unwrap());
        assert_eq!(status.object_count(), 3);
        assert_eq!(status.changed_path_count(), 1);
        assert!(status.has_idempotency_reservation());
        assert_eq!(status.first_seen_at_millis(), 700);
        assert_eq!(status.last_seen_at_millis(), 701);
        assert_eq!(status.occurrence_count(), 2);
        assert_eq!(status.attempts(), 0);
        assert_eq!(status.post_cas_context(), Some(&context));

        let counts = DurableCorePreVisibilityRecoveryStore::counts(store).await?;
        assert_eq!(counts.pending(), 1);
        assert_eq!(counts.total(), 1);

        let first_claim = DurableCorePreVisibilityRecoveryStore::claim(
            store,
            DurableCorePreVisibilityRecoveryClaimRequest::new(
                target.clone(),
                "postgres-pre-visibility-worker",
                Duration::from_secs(1),
                710,
            )?,
        )
        .await?
        .expect("pending pre-visibility row should be claimable");
        assert_eq!(first_claim.attempts(), 1);
        assert_eq!(first_claim.post_cas_context(), Some(&context));
        assert!(
            DurableCorePreVisibilityRecoveryStore::claim(
                store,
                DurableCorePreVisibilityRecoveryClaimRequest::new(
                    target.clone(),
                    "postgres-pre-visibility-worker",
                    Duration::from_secs(1),
                    711,
                )?,
            )
            .await?
            .is_none()
        );
        DurableCorePreVisibilityRecoveryStore::record_failure(
            store,
            &first_claim,
            "raw postgres pre-visibility failure",
            Duration::from_secs(1),
            712,
        )
        .await?;
        assert!(
            DurableCorePreVisibilityRecoveryStore::list_repair_candidates(store, 1_500, 10)
                .await?
                .is_empty()
        );
        let due =
            DurableCorePreVisibilityRecoveryStore::list_repair_candidates(store, 1_713, 10).await?;
        assert_eq!(due.len(), 1);
        assert_eq!(
            due[0].redacted_diagnosis(),
            Some("redacted pre-visibility recovery failure")
        );

        let retry_claim = DurableCorePreVisibilityRecoveryStore::claim(
            store,
            DurableCorePreVisibilityRecoveryClaimRequest::new(
                target.clone(),
                "postgres-pre-visibility-worker",
                Duration::from_secs(1),
                1_714,
            )?,
        )
        .await?
        .expect("due pre-visibility row should be reclaimable");
        assert_eq!(retry_claim.attempts(), 2);
        DurableCorePreVisibilityRecoveryStore::resolve(store, &first_claim, 1_715)
            .await
            .expect_err("stale pre-visibility claim cannot resolve retry");
        DurableCorePreVisibilityRecoveryStore::resolve(store, &retry_claim, 1_715).await?;

        let counts = DurableCorePreVisibilityRecoveryStore::counts(store).await?;
        assert_eq!(counts.pending(), 0);
        assert_eq!(counts.resolved(), 1);
        assert_eq!(counts.total(), 1);

        let conflicting = DurableCorePreVisibilityRecoveryRecord::new(
            target,
            object_id(b"pre-visibility-different-root"),
            Some(parent_id),
            RefVersion::new(2).unwrap(),
            3,
            1,
            true,
            702,
        );
        let conflict = DurableCorePreVisibilityRecoveryStore::record(store, conflicting)
            .await
            .expect_err("conflicting diagnostics should not overwrite existing rows");
        assert!(matches!(conflict, VfsError::CorruptStore { .. }));

        Ok(())
    }

    async fn run_fs_mutation_recovery_contracts(
        store: &PostgresMetadataStore,
        repo_id: &RepoId,
        previous_commit: CommitId,
        new_commit: CommitId,
    ) -> Result<(), VfsError> {
        let target = DurableFsMutationRecoveryTarget::new(
            repo_id.clone(),
            "fs:postgres-workspace",
            "postgres-fs-recovery",
            "agent/postgres/session",
            previous_commit,
            new_commit,
            DurableFsMutationRecoveryStep::AuditAppend,
        )?;
        let envelope = DurableFsMutationRecoveryEnvelope::new(
            Some(DurableFsMutationIdempotencyRecoveryContext::for_store(
                "fs:postgres-workspace",
                "a".repeat(64),
                "b".repeat(64),
                "postgres-reservation-token",
                500,
                json!({"error": "redacted route response"}),
                IdempotencyReplayClassification::Partial,
            )?),
            Some(DurableFsMutationAuditRecoveryContext::new(
                AuditAction::FsWriteFile,
                &["/postgres/recovery.txt"],
            )?),
            None,
        );

        DurableFsMutationRecoveryStore::enqueue(store, target.clone(), envelope.clone(), 100)
            .await?;
        DurableFsMutationRecoveryStore::enqueue(store, target.clone(), envelope.clone(), 101)
            .await?;
        let statuses = DurableFsMutationRecoveryStore::list(store, 10).await?;
        let status = statuses
            .iter()
            .find(|status| status.target() == &target)
            .expect("durable FS mutation recovery status");
        assert_eq!(status.state(), DurableFsMutationRecoveryState::Pending);
        assert_eq!(status.attempts(), 0);
        let counts = DurableFsMutationRecoveryStore::counts(store).await?;
        assert_eq!(counts.pending(), 1);
        assert_eq!(counts.total(), 1);
        let pending_candidates =
            DurableFsMutationRecoveryStore::list_repair_candidates(store, 199, 10).await?;
        assert_eq!(pending_candidates.len(), 1);
        assert_eq!(pending_candidates[0].target(), &target);

        let first = DurableFsMutationRecoveryStore::claim(
            store,
            DurableFsMutationRecoveryClaimRequest::new(
                target.clone(),
                "postgres-fs-worker",
                Duration::from_millis(1),
                200,
            )?,
        )
        .await?
        .expect("pending durable FS mutation target should be claimable");
        assert_eq!(first.attempts(), 1);
        assert_eq!(first.envelope(), &envelope);

        let duplicate = DurableFsMutationRecoveryStore::claim(
            store,
            DurableFsMutationRecoveryClaimRequest::new(
                target.clone(),
                "postgres-fs-worker-2",
                Duration::from_secs(1),
                200,
            )?,
        )
        .await?;
        assert!(duplicate.is_none());

        let retry = DurableFsMutationRecoveryStore::claim(
            store,
            DurableFsMutationRecoveryClaimRequest::new(
                target.clone(),
                "postgres-fs-worker",
                Duration::from_secs(1),
                210,
            )?,
        )
        .await?
        .expect("expired durable FS mutation claim should be retryable");
        assert_eq!(retry.attempts(), 2);
        assert_ne!(retry.token(), first.token());

        let stale_complete = DurableFsMutationRecoveryStore::complete(store, &first, 211)
            .await
            .expect_err("stale durable FS mutation token must not complete retry");
        assert!(matches!(stale_complete, VfsError::InvalidArgs { .. }));

        DurableFsMutationRecoveryStore::record_failure(
            store,
            &retry,
            "raw /private/path postgres-reservation-token",
            Duration::from_millis(1),
            212,
        )
        .await?;
        let rendered = format!(
            "{:?}",
            DurableFsMutationRecoveryStore::list(store, 10).await?
        );
        assert!(rendered.contains("redacted durable FS mutation recovery failure"));
        assert!(!rendered.contains("/private/path"));
        assert!(!rendered.contains("postgres-reservation-token"));
        assert!(!rendered.contains(retry.token()));
        assert!(
            DurableFsMutationRecoveryStore::list_repair_candidates(store, 212, 10)
                .await?
                .is_empty(),
            "backing-off durable FS mutation row should not be returned before retry time"
        );
        let due_candidates =
            DurableFsMutationRecoveryStore::list_repair_candidates(store, 220, 10).await?;
        assert_eq!(due_candidates.len(), 1);
        assert_eq!(due_candidates[0].target(), &target);

        let retry_after_backoff = DurableFsMutationRecoveryStore::claim(
            store,
            DurableFsMutationRecoveryClaimRequest::new(
                target.clone(),
                "postgres-fs-worker",
                Duration::from_secs(1),
                220,
            )?,
        )
        .await?
        .expect("elapsed durable FS mutation backoff should be claimable");
        assert_eq!(retry_after_backoff.attempts(), 3);
        DurableFsMutationRecoveryStore::complete(store, &retry_after_backoff, 221).await?;
        assert!(
            DurableFsMutationRecoveryStore::claim(
                store,
                DurableFsMutationRecoveryClaimRequest::new(
                    target,
                    "postgres-fs-worker",
                    Duration::from_secs(1),
                    222,
                )?,
            )
            .await?
            .is_none()
        );

        let ordered_new_commit = commit_id("postgres-fs-recovery-ordering");
        let ordered_tree_id = object_id(b"postgres-fs-recovery-ordering-tree");
        ObjectMetadataStore::put(
            store,
            object_record(
                repo_id,
                ordered_tree_id,
                ObjectKind::Tree,
                b"postgres-fs-recovery-ordering-tree",
            ),
        )
        .await?;
        CommitStore::insert(
            store,
            commit_record(
                repo_id,
                ordered_new_commit,
                ordered_tree_id,
                vec![previous_commit],
                6,
                "postgres FS recovery ordering",
            ),
        )
        .await?;
        let ordered_envelope = DurableFsMutationRecoveryEnvelope::new(
            None,
            Some(DurableFsMutationAuditRecoveryContext::new(
                AuditAction::FsWriteFile,
                &["/postgres/order.txt"],
            )?),
            None,
        );
        for (operation_id, failed_step, now_millis) in [
            (
                "postgres-fs-order-workspace",
                DurableFsMutationRecoveryStep::WorkspaceCompletion,
                230,
            ),
            (
                "postgres-fs-order-audit",
                DurableFsMutationRecoveryStep::AuditAppend,
                231,
            ),
            (
                "postgres-fs-order-idempotency",
                DurableFsMutationRecoveryStep::IdempotencyCompletion,
                232,
            ),
        ] {
            DurableFsMutationRecoveryStore::enqueue(
                store,
                DurableFsMutationRecoveryTarget::new(
                    repo_id.clone(),
                    "fs:postgres-ordering",
                    operation_id,
                    "agent/postgres/session",
                    previous_commit,
                    ordered_new_commit,
                    failed_step,
                )?,
                ordered_envelope.clone(),
                now_millis,
            )
            .await?;
        }
        let ordered_steps = DurableFsMutationRecoveryStore::list_repair_candidates(store, 233, 3)
            .await?
            .into_iter()
            .map(|status| status.target().failed_step())
            .collect::<Vec<_>>();
        assert_eq!(
            ordered_steps,
            vec![
                DurableFsMutationRecoveryStep::WorkspaceCompletion,
                DurableFsMutationRecoveryStep::AuditAppend,
                DurableFsMutationRecoveryStep::IdempotencyCompletion
            ]
        );

        let route_claim_commit_id = commit_id("postgres-fs-route-claim");
        let route_claim_tree_id = object_id(b"postgres-fs-route-claim-tree");
        ObjectMetadataStore::put(
            store,
            object_record(
                repo_id,
                route_claim_tree_id,
                ObjectKind::Tree,
                b"postgres-fs-route-claim-tree",
            ),
        )
        .await?;
        CommitStore::insert(
            store,
            commit_record(
                repo_id,
                route_claim_commit_id,
                route_claim_tree_id,
                vec![previous_commit],
                7,
                "postgres FS route claim",
            ),
        )
        .await?;

        let route_claim_target = DurableFsMutationRecoveryTarget::new(
            repo_id.clone(),
            "fs:postgres-route-claim",
            "postgres-fs-route-claim",
            "agent/postgres/session",
            previous_commit,
            route_claim_commit_id,
            DurableFsMutationRecoveryStep::AuditAppend,
        )?;
        DurableFsMutationRecoveryStore::enqueue(
            store,
            route_claim_target.clone(),
            ordered_envelope.clone(),
            240,
        )
        .await?;
        let route_claim = DurableFsMutationRecoveryStore::enqueue_and_claim(
            store,
            route_claim_target.clone(),
            ordered_envelope,
            "postgres-fs-route-worker",
            Duration::from_secs(1),
            241,
        )
        .await?
        .expect("route should atomically claim existing pending FS mutation work");
        assert_eq!(route_claim.envelope().changed_path_count(), 1);
        assert!(
            DurableFsMutationRecoveryStore::claim(
                store,
                DurableFsMutationRecoveryClaimRequest::new(
                    route_claim_target,
                    "postgres-fs-racing-worker",
                    Duration::from_secs(1),
                    242,
                )?,
            )
            .await?
            .is_none(),
            "worker must not claim a route-owned FS mutation row"
        );

        Ok(())
    }

    async fn run_backend_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
        let repo_id = repo("repo_pg");
        let other_repo_id = repo("repo_other");

        let blob_id = object_id(b"blob");
        let blob_record = object_record(&repo_id, blob_id, ObjectKind::Blob, b"blob");
        ObjectMetadataStore::put(store, blob_record.clone()).await?;
        assert_eq!(
            ObjectMetadataStore::get(store, &repo_id, blob_id).await?,
            Some(blob_record.clone())
        );
        assert_eq!(
            ObjectMetadataStore::put(store, blob_record.clone()).await?,
            blob_record.clone()
        );
        let first_blob_put = ObjectMetadataStore::put(store, blob_record.clone());
        let second_blob_put = ObjectMetadataStore::put(store, blob_record.clone());
        let (first_blob_put, second_blob_put) = tokio::join!(first_blob_put, second_blob_put);
        assert_eq!(first_blob_put?, blob_record.clone());
        assert_eq!(second_blob_put?, blob_record.clone());

        let conflicting_blob = ObjectMetadataRecord::new(
            repo_id.clone(),
            blob_id,
            ObjectKind::Tree,
            b"blob".len() as u64,
        );
        assert!(matches!(
            ObjectMetadataStore::put(store, conflicting_blob).await,
            Err(VfsError::CorruptStore { .. })
        ));

        let fenced_id = object_id(b"postgres-fenced-object");
        let fenced_record = object_record(
            &repo_id,
            fenced_id,
            ObjectKind::Blob,
            b"postgres-fenced-object",
        );
        let fenced_key = object_key(&repo_id, ObjectKind::Blob, &fenced_id);
        let fence = ObjectMetadataStore::acquire_final_object_metadata_fence(
            store,
            FinalObjectMetadataFenceRequest::new(
                repo_id.clone(),
                ObjectKind::Blob,
                fenced_id,
                fenced_key,
                "postgres-delete-worker".to_string(),
                Duration::from_secs(60),
            ),
        )
        .await?
        .expect("postgres fence should be acquired");
        assert!(matches!(
            ObjectMetadataStore::put(store, fenced_record.clone()).await,
            Err(VfsError::ObjectWriteConflict { .. })
        ));
        ObjectMetadataStore::release_final_object_metadata_fence(store, &fence).await?;
        ObjectMetadataStore::put(store, fenced_record.clone()).await?;
        let metadata_fence = ObjectMetadataStore::acquire_final_object_metadata_fence(
            store,
            FinalObjectMetadataFenceRequest::new(
                repo_id.clone(),
                ObjectKind::Blob,
                fenced_id,
                object_key(&repo_id, ObjectKind::Blob, &fenced_id),
                "postgres-delete-worker".to_string(),
                Duration::from_secs(60),
            ),
        )
        .await?
        .expect("postgres metadata-present fence should be acquired");
        ObjectMetadataStore::delete_with_final_object_metadata_fence(store, &metadata_fence)
            .await?;
        assert_eq!(
            ObjectMetadataStore::get(store, &repo_id, fenced_id).await?,
            None
        );
        ObjectMetadataStore::release_final_object_metadata_fence(store, &metadata_fence).await?;

        let temp_dir =
            std::env::temp_dir().join(format!("stratum-postgres-blob-object-{}", Uuid::new_v4()));
        let blob_object_store = BlobObjectStore::new(
            Arc::new(LocalBlobStore::new(&temp_dir)),
            Arc::new(store.clone()),
        );
        let object_bytes = b"postgres metadata plus local bytes".to_vec();
        let stored_object = blob_object_store
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: object_id(&object_bytes),
                kind: ObjectKind::Blob,
                bytes: object_bytes.clone(),
            })
            .await?;
        assert_eq!(stored_object.bytes, object_bytes);
        let _ = tokio::fs::remove_dir_all(temp_dir).await;

        let tree_1 = object_id(b"tree-1");
        let tree_2 = object_id(b"tree-2");
        let tree_3 = object_id(b"tree-3");
        ObjectMetadataStore::put(
            store,
            object_record(&repo_id, tree_1, ObjectKind::Tree, b"tree-1"),
        )
        .await?;
        ObjectMetadataStore::put(
            store,
            object_record(&repo_id, tree_2, ObjectKind::Tree, b"tree-2"),
        )
        .await?;
        ObjectMetadataStore::put(
            store,
            object_record(&repo_id, tree_3, ObjectKind::Tree, b"tree-3"),
        )
        .await?;

        let base = commit_record(&repo_id, commit_id("base"), tree_1, Vec::new(), 1, "base");
        let head = commit_record(
            &repo_id,
            commit_id("head"),
            tree_2,
            vec![base.id],
            2,
            "head",
        );
        let newer = commit_record(
            &repo_id,
            commit_id("newer"),
            tree_3,
            vec![head.id],
            3,
            "newer",
        );

        CommitStore::insert(store, base.clone()).await?;
        CommitStore::insert(store, head.clone()).await?;
        CommitStore::insert(store, newer.clone()).await?;
        assert_eq!(
            CommitStore::get(store, &repo_id, head.id).await?,
            Some(head.clone())
        );
        assert_eq!(CommitStore::insert(store, head.clone()).await?, head);

        let conflicting_head = CommitRecord {
            message: "different".to_string(),
            ..head.clone()
        };
        assert!(matches!(
            CommitStore::insert(store, conflicting_head).await,
            Err(VfsError::AlreadyExists { .. })
        ));

        let commits = CommitStore::list(store, &repo_id).await?;
        assert_eq!(
            commits.iter().map(|commit| commit.id).collect::<Vec<_>>(),
            vec![newer.id, head.id, base.id]
        );

        ObjectMetadataStore::put(
            store,
            object_record(&other_repo_id, tree_1, ObjectKind::Tree, b"tree-1"),
        )
        .await?;
        run_cleanup_claim_contracts(store, &repo_id).await?;

        let cross_repo_parent = commit_record(
            &other_repo_id,
            commit_id("cross-repo"),
            tree_1,
            vec![base.id],
            1,
            "cross repo parent",
        );
        assert!(CommitStore::insert(store, cross_repo_parent).await.is_err());

        let concurrent_repo_id = repo("repo_concurrent");
        let concurrent_tree = object_id(b"tree-concurrent");
        ObjectMetadataStore::put(
            store,
            object_record(
                &concurrent_repo_id,
                concurrent_tree,
                ObjectKind::Tree,
                b"tree-concurrent",
            ),
        )
        .await?;
        let concurrent_commit = commit_record(
            &concurrent_repo_id,
            commit_id("concurrent"),
            concurrent_tree,
            Vec::new(),
            1,
            "concurrent",
        );
        let first_commit_insert = CommitStore::insert(store, concurrent_commit.clone());
        let second_commit_insert = CommitStore::insert(store, concurrent_commit.clone());
        let (first_commit_insert, second_commit_insert) =
            tokio::join!(first_commit_insert, second_commit_insert);
        assert_eq!(first_commit_insert?, concurrent_commit.clone());
        assert_eq!(second_commit_insert?, concurrent_commit.clone());

        let main = RefName::new(MAIN_REF).unwrap();
        let review = RefName::new("review/pg").unwrap();
        let created = RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: main.clone(),
                target: base.id,
                expectation: RefExpectation::MustNotExist,
            },
        )
        .await?;
        assert_eq!(created.version, RefVersion::new(1).unwrap());

        assert!(
            RefStore::update(
                store,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: head.id,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .is_err()
        );
        assert_eq!(
            RefStore::get(store, &repo_id, &main).await?,
            Some(created.clone())
        );

        let updated = RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: main.clone(),
                target: head.id,
                expectation: RefExpectation::Matches {
                    target: base.id,
                    version: created.version,
                },
            },
        )
        .await?;
        assert_eq!(updated.version, RefVersion::new(2).unwrap());
        assert_eq!(updated.target, head.id);

        assert!(
            RefStore::update(
                store,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: base.id,
                    expectation: RefExpectation::Matches {
                        target: base.id,
                        version: updated.version,
                    },
                },
            )
            .await
            .is_err()
        );
        assert_eq!(
            RefStore::get(store, &repo_id, &main).await?,
            Some(updated.clone())
        );

        assert!(
            RefStore::update(
                store,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: base.id,
                    expectation: RefExpectation::Matches {
                        target: head.id,
                        version: RefVersion::new(99).unwrap(),
                    },
                },
            )
            .await
            .is_err()
        );
        assert_eq!(
            RefStore::get(store, &repo_id, &main).await?,
            Some(updated.clone())
        );

        let unknown_commit = commit_id("unknown");
        assert!(
            RefStore::update(
                store,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: unknown_commit,
                    expectation: RefExpectation::Matches {
                        target: head.id,
                        version: updated.version,
                    },
                },
            )
            .await
            .is_err()
        );
        assert_eq!(
            RefStore::get(store, &repo_id, &main).await?,
            Some(updated.clone())
        );

        let review_ref = RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: review.clone(),
                target: newer.id,
                expectation: RefExpectation::MustNotExist,
            },
        )
        .await?;

        let merged = RefStore::update_source_checked(
            store,
            SourceCheckedRefUpdate {
                repo_id: repo_id.clone(),
                source_name: review.clone(),
                source_expectation: RefExpectation::Matches {
                    target: newer.id,
                    version: review_ref.version,
                },
                target_update: RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: newer.id,
                    expectation: RefExpectation::Matches {
                        target: head.id,
                        version: updated.version,
                    },
                },
            },
        )
        .await?;
        assert_eq!(merged.target, newer.id);
        assert_eq!(merged.version, RefVersion::new(3).unwrap());

        assert!(
            RefStore::update_source_checked(
                store,
                SourceCheckedRefUpdate {
                    repo_id: repo_id.clone(),
                    source_name: review.clone(),
                    source_expectation: RefExpectation::Matches {
                        target: base.id,
                        version: review_ref.version,
                    },
                    target_update: RefUpdate {
                        repo_id: repo_id.clone(),
                        name: main.clone(),
                        target: base.id,
                        expectation: RefExpectation::Matches {
                            target: newer.id,
                            version: merged.version,
                        },
                    },
                },
            )
            .await
            .is_err()
        );
        assert_eq!(
            RefStore::get(store, &repo_id, &main).await?,
            Some(merged.clone())
        );

        assert!(matches!(
            RefStore::update_source_checked(
                store,
                SourceCheckedRefUpdate {
                    repo_id: repo_id.clone(),
                    source_name: review.clone(),
                    source_expectation: RefExpectation::MustNotExist,
                    target_update: RefUpdate {
                        repo_id: repo_id.clone(),
                        name: main.clone(),
                        target: base.id,
                        expectation: RefExpectation::Matches {
                            target: newer.id,
                            version: merged.version,
                        },
                    },
                },
            )
            .await,
            Err(VfsError::NotSupported { .. })
        ));

        assert!(
            RefStore::update_source_checked(
                store,
                SourceCheckedRefUpdate {
                    repo_id: repo_id.clone(),
                    source_name: review,
                    source_expectation: RefExpectation::Matches {
                        target: newer.id,
                        version: review_ref.version,
                    },
                    target_update: RefUpdate {
                        repo_id: other_repo_id,
                        name: main.clone(),
                        target: base.id,
                        expectation: RefExpectation::MustNotExist,
                    },
                },
            )
            .await
            .is_err()
        );

        let refs = RefStore::list(store, &repo_id).await?;
        assert_eq!(
            refs.iter()
                .map(|record| record.name.as_str())
                .collect::<Vec<_>>(),
            vec!["main", "review/pg"]
        );

        let max_ref = RefName::new("archive/max").unwrap();
        RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: max_ref.clone(),
                target: base.id,
                expectation: RefExpectation::MustNotExist,
            },
        )
        .await?;
        let max_version = RefVersion::new(i64::MAX as u64).unwrap();
        let client = store.connect_client().await?;
        client
            .execute(
                "UPDATE refs SET version = $3 WHERE repo_id = $1 AND name = $2",
                &[&repo_id.as_str(), &max_ref.as_str(), &i64::MAX],
            )
            .await
            .map_err(|error| postgres_error("set test ref version", error))?;
        drop(client);
        assert!(matches!(
            RefStore::update(
                store,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: max_ref.clone(),
                    target: head.id,
                    expectation: RefExpectation::Matches {
                        target: head.id,
                        version: max_version,
                    },
                },
            )
            .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            RefStore::update(
                store,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: max_ref.clone(),
                    target: head.id,
                    expectation: RefExpectation::Matches {
                        target: base.id,
                        version: max_version,
                    },
                },
            )
            .await,
            Err(VfsError::CorruptStore { .. })
        ));
        assert_eq!(
            RefStore::get(store, &repo_id, &max_ref)
                .await?
                .map(|record| (record.target, record.version)),
            Some((base.id, max_version))
        );

        let race = RefName::new("archive/race").unwrap();
        let race_ref = RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: race.clone(),
                target: base.id,
                expectation: RefExpectation::MustNotExist,
            },
        )
        .await?;
        let first = RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: race.clone(),
                target: head.id,
                expectation: RefExpectation::Matches {
                    target: base.id,
                    version: race_ref.version,
                },
            },
        );
        let second = RefStore::update(
            store,
            RefUpdate {
                repo_id: repo_id.clone(),
                name: race,
                target: newer.id,
                expectation: RefExpectation::Matches {
                    target: base.id,
                    version: race_ref.version,
                },
            },
        );
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.is_ok() as u8 + second.is_ok() as u8, 1);

        run_post_cas_recovery_claim_contracts(store, &repo_id, base.id, head.id, newer.id).await?;

        run_pre_visibility_recovery_contracts(store, &repo_id).await?;

        run_fs_mutation_recovery_contracts(store, &repo_id, base.id, head.id).await?;

        run_idempotency_contracts(store).await?;

        run_audit_contracts(store).await?;

        run_workspace_contracts(store).await?;

        run_review_contracts(store).await?;

        Ok(())
    }
}
