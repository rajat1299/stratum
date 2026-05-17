//! Postgres migration runner foundation for durable backend schemas.
//!
//! This module is feature-gated behind `postgres` and backs durable
//! `stratum-server` startup preflight. It provides the ordered migration
//! catalog, schema state reporting, dirty-state refusal, and schema-scoped
//! startup lock used before durable control-plane stores are opened.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use deadpool_postgres::{GenericClient, Transaction};
use tokio_postgres::Config;

#[cfg(test)]
type Client = deadpool_postgres::Client;

use crate::backend::postgres::{
    PostgresConnector, infer_tls_mode, postgres_error, validate_schema_name,
};
use crate::backend::runtime::DurablePostgresRuntimePosture;
use crate::error::VfsError;

const MIGRATION_LOCK_NAMESPACE: i32 = 0x5354_524d; // "STRM"
const DURABLE_BACKEND_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0001_durable_backend_foundation.sql");
const REVIEW_LOCAL_COMMIT_IDS_SQL: &str =
    include_str!("../../migrations/postgres/0002_review_local_commit_ids.sql");
const GUARDED_COMMIT_RECOVERY_CLAIMS_SQL: &str =
    include_str!("../../migrations/postgres/0003_guarded_commit_recovery_claims.sql");
const GUARDED_COMMIT_RECOVERY_CONTEXT_SQL: &str =
    include_str!("../../migrations/postgres/0004_guarded_commit_recovery_context.sql");
const GUARDED_COMMIT_PRE_VISIBILITY_RECOVERY_SQL: &str =
    include_str!("../../migrations/postgres/0005_guarded_commit_pre_visibility_recovery.sql");
const PRE_VISIBILITY_RECOVERY_RUN_CONTROL_SQL: &str =
    include_str!("../../migrations/postgres/0006_pre_visibility_recovery_run_control.sql");
const DURABLE_FS_MUTATION_RECOVERY_SQL: &str =
    include_str!("../../migrations/postgres/0007_durable_fs_mutation_recovery.sql");
const DURABLE_MUTATION_CLEANUP_CLAIM_KIND_SQL: &str =
    include_str!("../../migrations/postgres/0008_durable_mutation_cleanup_claim_kind.sql");
const DURABLE_AUTH_SESSION_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0009_durable_auth_session_foundation.sql");
const OBJECT_DELETION_FENCES_SQL: &str =
    include_str!("../../migrations/postgres/0010_object_deletion_fences.sql");
const IDEMPOTENCY_RETENTION_QUOTA_SQL: &str =
    include_str!("../../migrations/postgres/0011_idempotency_retention_quota.sql");
const OBJECT_CLEANUP_DELETION_STATE_SQL: &str =
    include_str!("../../migrations/postgres/0012_object_cleanup_deletion_state.sql");
const PROTECTED_RULES_REQUIRE_ALL_FILES_VIEWED_SQL: &str =
    include_str!("../../migrations/postgres/0013_protected_rules_require_all_files_viewed.sql");
const SECRET_BEARING_IDEMPOTENCY_REPLAY_SQL: &str =
    include_str!("../../migrations/postgres/0014_secret_bearing_idempotency_replay.sql");
const POSTGRES_MIGRATIONS: [PostgresMigration; 14] = [
    PostgresMigration {
        version: 1,
        name: "durable_backend_foundation",
        sql: DURABLE_BACKEND_FOUNDATION_SQL,
    },
    PostgresMigration {
        version: 2,
        name: "review_local_commit_ids",
        sql: REVIEW_LOCAL_COMMIT_IDS_SQL,
    },
    PostgresMigration {
        version: 3,
        name: "guarded_commit_recovery_claims",
        sql: GUARDED_COMMIT_RECOVERY_CLAIMS_SQL,
    },
    PostgresMigration {
        version: 4,
        name: "guarded_commit_recovery_context",
        sql: GUARDED_COMMIT_RECOVERY_CONTEXT_SQL,
    },
    PostgresMigration {
        version: 5,
        name: "guarded_commit_pre_visibility_recovery",
        sql: GUARDED_COMMIT_PRE_VISIBILITY_RECOVERY_SQL,
    },
    PostgresMigration {
        version: 6,
        name: "pre_visibility_recovery_run_control",
        sql: PRE_VISIBILITY_RECOVERY_RUN_CONTROL_SQL,
    },
    PostgresMigration {
        version: 7,
        name: "durable_fs_mutation_recovery",
        sql: DURABLE_FS_MUTATION_RECOVERY_SQL,
    },
    PostgresMigration {
        version: 8,
        name: "durable_mutation_cleanup_claim_kind",
        sql: DURABLE_MUTATION_CLEANUP_CLAIM_KIND_SQL,
    },
    PostgresMigration {
        version: 9,
        name: "durable_auth_session_foundation",
        sql: DURABLE_AUTH_SESSION_FOUNDATION_SQL,
    },
    PostgresMigration {
        version: 10,
        name: "object_deletion_fences",
        sql: OBJECT_DELETION_FENCES_SQL,
    },
    PostgresMigration {
        version: 11,
        name: "idempotency_retention_quota",
        sql: IDEMPOTENCY_RETENTION_QUOTA_SQL,
    },
    PostgresMigration {
        version: 12,
        name: "object_cleanup_deletion_state",
        sql: OBJECT_CLEANUP_DELETION_STATE_SQL,
    },
    PostgresMigration {
        version: 13,
        name: "protected_rules_require_all_files_viewed",
        sql: PROTECTED_RULES_REQUIRE_ALL_FILES_VIEWED_SQL,
    },
    PostgresMigration {
        version: 14,
        name: "secret_bearing_idempotency_replay",
        sql: SECRET_BEARING_IDEMPOTENCY_REPLAY_SQL,
    },
];

#[derive(Clone)]
pub struct PostgresMigrationRunner {
    connector: PostgresConnector,
    schema: String,
}

impl fmt::Debug for PostgresMigrationRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresMigrationRunner")
            .field("schema", &self.schema)
            .field("migration_count", &POSTGRES_MIGRATIONS.len())
            .finish()
    }
}

impl PostgresMigrationRunner {
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

    pub async fn status(&self) -> Result<PostgresMigrationReport, VfsError> {
        validate_catalog()?;
        let client = self
            .connector
            .connect_with_schema(Some(&self.schema))
            .await?;
        ensure_control_table(&client).await?;
        self.status_with_client(&client).await
    }

    pub async fn apply_pending(&self) -> Result<PostgresMigrationReport, VfsError> {
        validate_catalog()?;
        let mut client = self
            .connector
            .connect_with_schema(Some(&self.schema))
            .await?;
        ensure_control_table(&client).await?;
        let (lock_namespace, lock_key) = migration_lock_ids(&self.schema);
        let mut transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin migration startup transaction", error))?;
        acquire_migration_lock(&transaction, lock_namespace, lock_key).await?;

        let result = self.apply_pending_locked(&mut transaction).await;
        let commit_result = transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit migration startup transaction", error));
        match (result, commit_result) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    pub async fn adopt_applied(&self) -> Result<PostgresMigrationReport, VfsError> {
        validate_catalog()?;
        let mut client = self
            .connector
            .connect_with_schema(Some(&self.schema))
            .await?;
        let (lock_namespace, lock_key) = migration_lock_ids(&self.schema);
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin migration startup transaction", error))?;
        acquire_migration_lock(&transaction, lock_namespace, lock_key).await?;
        ensure_control_table(&transaction).await?;

        let result = self.adopt_applied_locked(&transaction).await;
        match result {
            Ok(report) => transaction
                .commit()
                .await
                .map(|()| report)
                .map_err(|error| postgres_error("commit migration startup transaction", error)),
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn apply_pending_locked(
        &self,
        client: &mut Transaction<'_>,
    ) -> Result<PostgresMigrationReport, VfsError> {
        ensure_control_table(client).await?;
        let initial = self.status_with_client(client).await?;
        validate_report_for_apply(&initial)?;

        for status in initial.statuses {
            let PostgresMigrationStatus::Pending { version, .. } = status else {
                continue;
            };
            let migration =
                migration_by_version(version).ok_or_else(|| VfsError::CorruptStore {
                    message: format!("unknown Postgres migration version: {version}"),
                })?;
            apply_one_migration(client, migration).await?;
        }

        self.status_with_client(client).await
    }

    async fn adopt_applied_locked(
        &self,
        client: &impl GenericClient,
    ) -> Result<PostgresMigrationReport, VfsError> {
        let initial = self.status_with_client(client).await?;
        validate_report_for_adopt(&initial)?;
        verify_known_schema_catalog(client).await?;
        if initial
            .statuses
            .iter()
            .all(|status| matches!(status, PostgresMigrationStatus::Applied { .. }))
        {
            return Ok(initial);
        }

        for migration in &POSTGRES_MIGRATIONS {
            record_migration_adopted(client, migration).await?;
        }

        self.status_with_client(client).await
    }

    async fn status_with_client(
        &self,
        client: &impl GenericClient,
    ) -> Result<PostgresMigrationReport, VfsError> {
        let rows = load_control_rows(client).await?;
        let mut statuses = Vec::new();
        let mut seen_versions = BTreeSet::new();

        for migration in &POSTGRES_MIGRATIONS {
            match rows.get(&migration.version) {
                Some(row) => {
                    seen_versions.insert(row.version);
                    statuses.push(status_for_row(migration, row));
                }
                None => statuses.push(PostgresMigrationStatus::Pending {
                    version: migration.version,
                    name: migration.name,
                }),
            }
        }

        for row in rows.values() {
            if seen_versions.contains(&row.version) {
                continue;
            }
            statuses.push(PostgresMigrationStatus::UnknownApplied {
                version: row.version,
                name: row.name.clone(),
            });
        }

        Ok(PostgresMigrationReport { statuses })
    }

    #[cfg(test)]
    async fn create_control_table_for_test(&self) -> Result<(), VfsError> {
        let client = self
            .connector
            .connect_with_schema(Some(&self.schema))
            .await?;
        ensure_control_table(&client).await
    }

    #[cfg(test)]
    async fn insert_control_row_for_test(
        &self,
        version: i64,
        state: &str,
        checksum: &str,
    ) -> Result<(), VfsError> {
        let client = self
            .connector
            .connect_with_schema(Some(&self.schema))
            .await?;
        let checksum = if checksum == "bogus" {
            "0".repeat(64)
        } else {
            checksum.to_string()
        };
        let failure_message: Option<&str> = if state == "failed" {
            Some("test failure")
        } else {
            None
        };
        client
            .execute(
                "INSERT INTO stratum_schema_migrations (
                    version,
                    name,
                    checksum,
                    state,
                    started_at,
                    finished_at,
                    failure_message
                 )
                 VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    clock_timestamp(),
                    CASE WHEN $4 IN ('applied', 'failed') THEN clock_timestamp() ELSE NULL END,
                    $5
                 )",
                &[
                    &version,
                    &format!("migration_{version}"),
                    &checksum,
                    &state,
                    &failure_message,
                ],
            )
            .await
            .map_err(|error| postgres_error("insert test migration control row", error))?;
        Ok(())
    }

    #[cfg(test)]
    async fn hold_advisory_lock_for_test(&self) -> Result<HeldMigrationLock, VfsError> {
        let client = self
            .connector
            .connect_with_schema(Some(&self.schema))
            .await?;
        let (lock_namespace, lock_key) = migration_lock_ids(&self.schema);
        let locked: bool = client
            .query_one(
                "SELECT pg_try_advisory_lock($1, $2)",
                &[&lock_namespace, &lock_key],
            )
            .await
            .map_err(|error| postgres_error("acquire test migration lock", error))?
            .get(0);
        if !locked {
            return Err(VfsError::ObjectWriteConflict {
                message: "Postgres migration startup lock is already held".to_string(),
            });
        }
        Ok(HeldMigrationLock { _client: client })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresMigrationReport {
    pub statuses: Vec<PostgresMigrationStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresMigrationStatus {
    Pending {
        version: i64,
        name: &'static str,
    },
    Applied {
        version: i64,
        name: &'static str,
    },
    Dirty {
        version: i64,
        name: String,
        state: String,
    },
    ChecksumMismatch {
        version: i64,
        name: String,
    },
    UnknownApplied {
        version: i64,
        name: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct PostgresMigration {
    pub version: i64,
    pub name: &'static str,
    sql: &'static str,
}

impl PostgresMigration {
    fn checksum(self) -> String {
        hex_sha256(self.sql.as_bytes())
    }
}

#[derive(Debug)]
struct ControlRow {
    version: i64,
    name: String,
    checksum: String,
    state: String,
}

#[cfg(test)]
struct HeldMigrationLock {
    _client: Client,
}

async fn ensure_control_table(client: &impl GenericClient) -> Result<(), VfsError> {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS stratum_schema_migrations (
                version BIGINT PRIMARY KEY CHECK (version > 0),
                name TEXT NOT NULL CHECK (name <> '' AND length(name) <= 128),
                checksum TEXT NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
                state TEXT NOT NULL CHECK (state IN ('started', 'applied', 'failed')),
                started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                finished_at TIMESTAMPTZ,
                failure_message TEXT,
                CHECK (
                    (
                        state = 'started'
                        AND finished_at IS NULL
                        AND failure_message IS NULL
                    )
                    OR (
                        state = 'applied'
                        AND finished_at IS NOT NULL
                        AND failure_message IS NULL
                    )
                    OR (
                        state = 'failed'
                        AND finished_at IS NOT NULL
                        AND failure_message IS NOT NULL
                    )
                )
            )",
        )
        .await
        .map_err(|error| postgres_error("create migration control table", error))
}

async fn load_control_rows(
    client: &impl GenericClient,
) -> Result<BTreeMap<i64, ControlRow>, VfsError> {
    let rows = client
        .query(
            "SELECT version, name, checksum, state
             FROM stratum_schema_migrations
             ORDER BY version ASC",
            &[],
        )
        .await
        .map_err(|error| postgres_error("load migration control rows", error))?;

    let mut control_rows = BTreeMap::new();
    for row in rows {
        let version: i64 = row.get("version");
        if version <= 0 {
            return Err(VfsError::CorruptStore {
                message: format!("Postgres migration has invalid version {version}"),
            });
        }
        control_rows.insert(
            version,
            ControlRow {
                version,
                name: row.get("name"),
                checksum: row.get("checksum"),
                state: row.get("state"),
            },
        );
    }
    Ok(control_rows)
}

fn status_for_row(migration: &PostgresMigration, row: &ControlRow) -> PostgresMigrationStatus {
    if row.state != "applied" {
        return PostgresMigrationStatus::Dirty {
            version: row.version,
            name: row.name.clone(),
            state: row.state.clone(),
        };
    }
    if row.name != migration.name || row.checksum != migration.checksum() {
        return PostgresMigrationStatus::ChecksumMismatch {
            version: row.version,
            name: row.name.clone(),
        };
    }
    PostgresMigrationStatus::Applied {
        version: migration.version,
        name: migration.name,
    }
}

fn validate_report_for_apply(report: &PostgresMigrationReport) -> Result<(), VfsError> {
    for status in &report.statuses {
        match status {
            PostgresMigrationStatus::Pending { .. } | PostgresMigrationStatus::Applied { .. } => {}
            PostgresMigrationStatus::Dirty {
                version,
                name: _,
                state: _,
            } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} is dirty; refusing to apply migrations"
                    ),
                });
            }
            PostgresMigrationStatus::ChecksumMismatch { version, name: _ } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} has a checksum or name mismatch; refusing to apply migrations"
                    ),
                });
            }
            PostgresMigrationStatus::UnknownApplied { version, name: _ } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration table contains unknown applied version {version}; refusing to apply migrations"
                    ),
                });
            }
        }
    }
    Ok(())
}

fn validate_report_for_adopt(report: &PostgresMigrationReport) -> Result<(), VfsError> {
    let mut applied = 0usize;
    let mut pending = 0usize;
    for status in &report.statuses {
        match status {
            PostgresMigrationStatus::Pending { .. } => pending += 1,
            PostgresMigrationStatus::Applied { .. } => applied += 1,
            PostgresMigrationStatus::Dirty {
                version,
                name: _,
                state: _,
            } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} is dirty; refusing to adopt migrations"
                    ),
                });
            }
            PostgresMigrationStatus::ChecksumMismatch { version, name: _ } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration version {version} has a checksum or name mismatch; refusing to adopt migrations"
                    ),
                });
            }
            PostgresMigrationStatus::UnknownApplied { version, name: _ } => {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "Postgres migration table contains unknown applied version {version}; refusing to adopt migrations"
                    ),
                });
            }
        }
    }

    if applied > 0 && pending > 0 {
        return Err(VfsError::CorruptStore {
            message:
                "Postgres migration table is partially populated; refusing to adopt migrations"
                    .to_string(),
        });
    }

    Ok(())
}

async fn verify_known_schema_catalog(client: &impl GenericClient) -> Result<(), VfsError> {
    for table in [
        "repos",
        "objects",
        "object_cleanup_claims",
        "commits",
        "commit_parents",
        "refs",
        "idempotency_records",
        "audit_events",
        "workspaces",
        "workspace_tokens",
        "protected_ref_rules",
        "protected_path_rules",
        "change_requests",
        "approvals",
        "review_comments",
        "reviewer_assignments",
        "durable_post_cas_recovery_claims",
        "durable_pre_visibility_recovery_ledger",
        "durable_fs_mutation_recovery_ledger",
        "durable_principals",
        "object_deletion_fences",
    ] {
        require_table(client, table).await?;
    }

    for (table, column) in [
        ("durable_post_cas_recovery_claims", "context_json"),
        ("durable_pre_visibility_recovery_ledger", "repo_id"),
        ("durable_pre_visibility_recovery_ledger", "context_json"),
        ("durable_pre_visibility_recovery_ledger", "lease_owner"),
        ("durable_pre_visibility_recovery_ledger", "lease_token"),
        ("durable_pre_visibility_recovery_ledger", "lease_expires_at"),
        ("durable_pre_visibility_recovery_ledger", "attempts"),
        ("durable_pre_visibility_recovery_ledger", "retry_after"),
        ("durable_pre_visibility_recovery_ledger", "last_error"),
        ("durable_pre_visibility_recovery_ledger", "poisoned_at"),
        ("durable_pre_visibility_recovery_ledger", "updated_at"),
        ("object_cleanup_claims", "claim_kind"),
        ("workspace_tokens", "repo_id"),
        ("workspace_tokens", "principal_uid"),
        ("workspace_tokens", "token_version"),
        ("workspace_tokens", "issued_at"),
        ("workspace_tokens", "updated_at"),
        ("workspace_tokens", "expires_at"),
        ("workspace_tokens", "revoked_at"),
        ("idempotency_records", "replay_classification"),
        ("idempotency_records", "quota_repo_id"),
        ("idempotency_records", "quota_workspace_id"),
        ("idempotency_records", "quota_principal_uid"),
        ("idempotency_records", "retention_deferred_at"),
        ("idempotency_records", "secret_replay_envelope_version"),
        ("idempotency_records", "secret_replay_key_id"),
        ("idempotency_records", "secret_replay_aad_hash"),
        ("idempotency_records", "secret_replay_encrypted_at"),
        ("object_cleanup_claims", "deletion_ready_at"),
        ("object_cleanup_claims", "delete_after"),
        ("object_cleanup_claims", "deletion_snapshot_object_key"),
        ("object_cleanup_claims", "deletion_snapshot_size_bytes"),
        ("object_cleanup_claims", "deletion_snapshot_sha256"),
        ("object_cleanup_claims", "final_object_bytes_deleted_at"),
        ("object_cleanup_claims", "final_object_metadata_deleted_at"),
        ("protected_ref_rules", "require_all_files_viewed"),
        ("protected_path_rules", "require_all_files_viewed"),
    ] {
        require_column(client, table, column).await?;
    }

    for index in [
        "object_cleanup_claims_active_lease_idx",
        "object_cleanup_claims_object_idx",
        "audit_events_global_sequence_idx",
        "approvals_active_head_approver_idx",
        "durable_post_cas_recovery_claims_due_idx",
        "durable_pre_visibility_recovery_status_idx",
        "durable_pre_visibility_recovery_due_idx",
        "durable_fs_mutation_recovery_due_idx",
        "workspace_tokens_workspace_active_idx",
        "workspace_tokens_repo_principal_idx",
        "object_deletion_fences_active_idx",
        "idempotency_records_scope_state_created_idx",
        "idempotency_records_repo_quota_idx",
        "idempotency_records_workspace_quota_idx",
        "idempotency_records_principal_quota_idx",
        "idempotency_records_completed_retention_idx",
        "idempotency_records_pending_retention_idx",
        "object_cleanup_claims_deletion_ready_idx",
    ] {
        require_index(client, index).await?;
    }

    for (table, constraint, required_fragment) in [
        (
            "object_cleanup_claims",
            "object_cleanup_claims_canonical_key_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_completed_error_check",
            None,
        ),
        (
            "durable_post_cas_recovery_claims",
            "durable_post_cas_recovery_claims_pending_check",
            None,
        ),
        (
            "durable_post_cas_recovery_claims",
            "durable_post_cas_recovery_claims_active_check",
            None,
        ),
        (
            "durable_post_cas_recovery_claims",
            "durable_post_cas_recovery_claims_backoff_check",
            Some("redacted post-CAS recovery failure"),
        ),
        (
            "durable_post_cas_recovery_claims",
            "durable_post_cas_recovery_claims_completed_check",
            None,
        ),
        (
            "durable_post_cas_recovery_claims",
            "durable_post_cas_recovery_claims_poisoned_check",
            Some("redacted post-CAS recovery failure"),
        ),
        (
            "durable_post_cas_recovery_claims",
            "durable_post_cas_recovery_claims_context_json_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_state_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_context_json_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_lease_owner_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_lease_token_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_pending_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_active_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_backoff_check",
            Some("redacted pre-visibility recovery failure"),
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_resolved_check",
            None,
        ),
        (
            "durable_pre_visibility_recovery_ledger",
            "durable_pre_visibility_recovery_poisoned_check",
            Some("redacted pre-visibility recovery failure"),
        ),
        (
            "durable_fs_mutation_recovery_ledger",
            "durable_fs_mutation_recovery_pending_check",
            None,
        ),
        (
            "durable_fs_mutation_recovery_ledger",
            "durable_fs_mutation_recovery_active_check",
            None,
        ),
        (
            "durable_fs_mutation_recovery_ledger",
            "durable_fs_mutation_recovery_backoff_check",
            Some("redacted durable FS mutation recovery failure"),
        ),
        (
            "durable_fs_mutation_recovery_ledger",
            "durable_fs_mutation_recovery_completed_check",
            None,
        ),
        (
            "durable_fs_mutation_recovery_ledger",
            "durable_fs_mutation_recovery_poisoned_check",
            Some("redacted durable FS mutation recovery failure"),
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_claim_kind_check",
            Some("durable_mutation_cas_lost_object_cleanup"),
        ),
        (
            "durable_principals",
            "durable_principals_created_at_finite_check",
            None,
        ),
        (
            "durable_principals",
            "durable_principals_updated_at_finite_check",
            None,
        ),
        (
            "workspace_tokens",
            "workspace_tokens_secret_hash_check",
            None,
        ),
        (
            "workspace_tokens",
            "workspace_tokens_issued_at_finite_check",
            None,
        ),
        (
            "workspace_tokens",
            "workspace_tokens_updated_at_finite_check",
            None,
        ),
        (
            "workspace_tokens",
            "workspace_tokens_expires_at_finite_check",
            None,
        ),
        (
            "workspace_tokens",
            "workspace_tokens_revoked_at_finite_check",
            None,
        ),
        ("workspace_tokens", "workspace_tokens_lifecycle_check", None),
        ("workspace_tokens", "workspace_tokens_expiry_check", None),
        (
            "idempotency_records",
            "idempotency_records_replay_classification_check",
            None,
        ),
        (
            "idempotency_records",
            "idempotency_records_quota_principal_uid_check",
            None,
        ),
        (
            "idempotency_records",
            "idempotency_records_completed_replay_classification_check",
            None,
        ),
        (
            "idempotency_records",
            "idempotency_records_secret_replay_metadata_check",
            Some("secret_bearing"),
        ),
        (
            "idempotency_records",
            "idempotency_records_secret_replay_metadata_shape_check",
            Some("secret_replay_aad_hash"),
        ),
        (
            "idempotency_records",
            "idempotency_records_secret_replay_envelope_shape_check",
            Some("ciphertext_b64"),
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_readiness_all_or_none_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_phase_markers_ready_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_phase_claim_kind_check",
            Some("durable_mutation_cas_lost_object_cleanup"),
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_phase_order_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_snapshot_size_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_snapshot_sha256_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_deletion_snapshot_canonical_key_check",
            None,
        ),
        (
            "object_cleanup_claims",
            "object_cleanup_claims_completed_ready_deletion_phases_check",
            Some("durable_mutation_cas_lost_object_cleanup"),
        ),
    ] {
        require_constraint(client, table, constraint, required_fragment).await?;
    }

    require_primary_key(
        client,
        "durable_fs_mutation_recovery_ledger",
        &[
            "repo_id",
            "workspace_scope",
            "operation_id",
            "target_ref",
            "previous_commit_id",
            "new_commit_id",
            "failed_step",
        ],
    )
    .await?;
    require_foreign_key(
        client,
        "durable_fs_mutation_recovery_ledger",
        &["repo_id", "previous_commit_id"],
        "commits",
        &["repo_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "durable_fs_mutation_recovery_ledger",
        &["repo_id", "new_commit_id"],
        "commits",
        &["repo_id", "id"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "durable_fs_mutation_recovery_ledger",
        &["jsonb_typeof", "envelope_json", "object"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "idempotency_records",
        &[
            "secret_bearing",
            "jsonb_typeof",
            "response_body_json",
            "ciphertext_b64",
            "nonce_b64",
            "{}",
        ],
    )
    .await?;

    require_primary_key(client, "durable_principals", &["uid"]).await?;
    require_foreign_key(client, "durable_principals", &["repo_id"], "repos", &["id"]).await?;
    require_unique_key(client, "durable_principals", &["repo_id", "username"]).await?;
    require_check_constraint_with_fragments(client, "durable_principals", &["uid", ">= 0"]).await?;
    require_check_constraint_with_fragments(client, "durable_principals", &["primary_gid", ">= 0"])
        .await?;
    require_check_constraint_with_fragments(
        client,
        "durable_principals",
        &["btrim", "username", "128"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "durable_principals",
        &["human", "service_account", "agent"],
    )
    .await?;

    require_not_null_column_with_default(client, "workspace_tokens", "token_version", &["1"])
        .await?;
    require_not_null_column_with_default(client, "workspace_tokens", "issued_at", &["now()"])
        .await?;
    require_not_null_column_with_default(client, "workspace_tokens", "updated_at", &["now()"])
        .await?;
    require_no_rows(client, "workspace_tokens", "principal_uid IS NULL").await?;
    require_workspace_token_backfill_matches_workspaces(client).await?;

    require_no_non_redacted_recovery_errors(
        client,
        "durable_post_cas_recovery_claims",
        "redacted post-CAS recovery failure",
    )
    .await?;
    require_no_non_redacted_recovery_errors(
        client,
        "durable_pre_visibility_recovery_ledger",
        "redacted pre-visibility recovery failure",
    )
    .await?;
    require_no_non_redacted_recovery_errors(
        client,
        "durable_fs_mutation_recovery_ledger",
        "redacted durable FS mutation recovery failure",
    )
    .await?;
    require_fixed_recovery_error_constraints_enforced(client).await?;

    require_no_constraint(
        client,
        "durable_pre_visibility_recovery_ledger",
        "durable_pre_visibility_recovery_ledger_state_check",
    )
    .await?;

    require_control_plane_readiness_shape(client).await?;

    for (table, column) in [
        ("protected_ref_rules", "require_all_files_viewed"),
        ("protected_path_rules", "require_all_files_viewed"),
    ] {
        require_bool_column_not_null_with_default_true(client, table, column).await?;
    }

    require_no_foreign_key_to_table(client, "change_requests", "commits").await?;

    Ok(())
}

async fn require_table(client: &impl GenericClient, table: &str) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class r
                JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = $1
                  AND r.relkind IN ('r', 'p')
            )",
            &[&table],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_column(
    client: &impl GenericClient,
    table: &str,
    column: &str,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = $1
                  AND column_name = $2
            )",
            &[&table, &column],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_index(client: &impl GenericClient, index: &str) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class r
                JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = $1
                  AND r.relkind = 'i'
            )",
            &[&index],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_constraint(
    client: &impl GenericClient,
    table: &str,
    constraint: &str,
    required_fragment: Option<&str>,
) -> Result<(), VfsError> {
    let row = client
        .query_opt(
            "SELECT pg_catalog.pg_get_constraintdef(c.oid), c.convalidated
             FROM pg_catalog.pg_constraint c
             JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
             WHERE n.nspname = current_schema()
               AND r.relname = $1
               AND c.conname = $2",
            &[&table, &constraint],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    let Some(row) = row else {
        return Err(adoption_verification_error());
    };
    let convalidated: bool = row.get(1);
    if !convalidated {
        return Err(adoption_verification_error());
    }
    if let Some(fragment) = required_fragment {
        let definition: String = row.get(0);
        if !definition.contains(fragment) {
            return Err(adoption_verification_error());
        }
    }
    Ok(())
}

async fn require_primary_key(
    client: &impl GenericClient,
    table: &str,
    columns: &[&str],
) -> Result<(), VfsError> {
    require_key_constraint(client, table, 'p', columns).await
}

async fn require_unique_key(
    client: &impl GenericClient,
    table: &str,
    columns: &[&str],
) -> Result<(), VfsError> {
    require_key_constraint(client, table, 'u', columns).await
}

async fn require_key_constraint(
    client: &impl GenericClient,
    table: &str,
    constraint_type: char,
    columns: &[&str],
) -> Result<(), VfsError> {
    let columns = columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    let constraint_type = constraint_type.to_string();
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = $1
                  AND c.contype::text = $2
                  AND c.convalidated
                  AND (
                    SELECT array_agg(a.attname::text ORDER BY key.ordinality)
                    FROM unnest(c.conkey) WITH ORDINALITY AS key(attnum, ordinality)
                    JOIN pg_catalog.pg_attribute a
                      ON a.attrelid = c.conrelid
                     AND a.attnum = key.attnum
                  ) = $3::text[]
            )",
            &[&table, &constraint_type, &columns],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_foreign_key(
    client: &impl GenericClient,
    table: &str,
    columns: &[&str],
    referenced_table: &str,
    referenced_columns: &[&str],
) -> Result<(), VfsError> {
    let columns = columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    let referenced_columns = referenced_columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                JOIN pg_catalog.pg_class rr ON rr.oid = c.confrelid
                JOIN pg_catalog.pg_namespace rn ON rn.oid = rr.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = $1
                  AND c.contype = 'f'
                  AND c.convalidated
                  AND rn.nspname = current_schema()
                  AND rr.relname = $2
                  AND (
                    SELECT array_agg(a.attname::text ORDER BY key.ordinality)
                    FROM unnest(c.conkey) WITH ORDINALITY AS key(attnum, ordinality)
                    JOIN pg_catalog.pg_attribute a
                      ON a.attrelid = c.conrelid
                     AND a.attnum = key.attnum
                  ) = $3::text[]
                  AND (
                    SELECT array_agg(a.attname::text ORDER BY key.ordinality)
                    FROM unnest(c.confkey) WITH ORDINALITY AS key(attnum, ordinality)
                    JOIN pg_catalog.pg_attribute a
                      ON a.attrelid = c.confrelid
                     AND a.attnum = key.attnum
                  ) = $4::text[]
            )",
            &[&table, &referenced_table, &columns, &referenced_columns],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_check_constraint_with_fragments(
    client: &impl GenericClient,
    table: &str,
    required_fragments: &[&str],
) -> Result<(), VfsError> {
    let rows = client
        .query(
            "SELECT pg_catalog.pg_get_constraintdef(c.oid)
             FROM pg_catalog.pg_constraint c
             JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
             WHERE n.nspname = current_schema()
               AND r.relname = $1
               AND c.contype = 'c'
               AND c.convalidated",
            &[&table],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    let matched = rows.iter().any(|row| {
        let definition: String = row.get(0);
        required_fragments
            .iter()
            .all(|fragment| definition.contains(fragment))
    });
    if matched {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_not_null_column_with_default(
    client: &impl GenericClient,
    table: &str,
    column: &str,
    default_fragments: &[&str],
) -> Result<(), VfsError> {
    let row = client
        .query_opt(
            "SELECT column_default
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1
               AND column_name = $2
               AND is_nullable = 'NO'",
            &[&table, &column],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    let Some(row) = row else {
        return Err(adoption_verification_error());
    };
    let column_default: Option<String> = row.get(0);
    let Some(column_default) = column_default else {
        return Err(adoption_verification_error());
    };
    if default_fragments
        .iter()
        .all(|fragment| column_default.contains(fragment))
    {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_no_rows(
    client: &impl GenericClient,
    table: &str,
    predicate: &str,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            &format!("SELECT EXISTS (SELECT 1 FROM {table} WHERE {predicate})"),
            &[],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Err(adoption_verification_error())
    } else {
        Ok(())
    }
}

async fn require_workspace_token_backfill_matches_workspaces(
    client: &impl GenericClient,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM workspace_tokens token
                JOIN workspaces workspace ON workspace.id = token.workspace_id
                WHERE token.repo_id IS DISTINCT FROM workspace.repo_id
                   OR token.principal_uid IS DISTINCT FROM token.agent_uid
            )",
            &[],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Err(adoption_verification_error())
    } else {
        Ok(())
    }
}

async fn require_no_non_redacted_recovery_errors(
    client: &impl GenericClient,
    table: &str,
    fixed_error: &str,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            &format!(
                "SELECT EXISTS (
                    SELECT 1
                    FROM {table}
                    WHERE state IN ('backing_off', 'poisoned')
                      AND last_error IS DISTINCT FROM $1
                )"
            ),
            &[&fixed_error],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Err(adoption_verification_error())
    } else {
        Ok(())
    }
}

async fn require_fixed_recovery_error_constraints_enforced(
    client: &impl GenericClient,
) -> Result<(), VfsError> {
    const SETUP: &str = "
        INSERT INTO repos (id, name)
        VALUES ('adoption_probe_repo', 'adoption probe')
        ON CONFLICT DO NOTHING;
        INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
        VALUES
            (
                'adoption_probe_repo',
                'tree',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                'adoption/probe/tree',
                0,
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
            ),
            (
                'adoption_probe_repo',
                'commit',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'adoption/probe/commit-a',
                0,
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
            ),
            (
                'adoption_probe_repo',
                'commit',
                'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                'adoption/probe/commit-c',
                0,
                'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
            )
        ON CONFLICT DO NOTHING;
        INSERT INTO commits (
            repo_id, id, root_tree_id, author, message, commit_timestamp_seconds
        )
        VALUES
            (
                'adoption_probe_repo',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                'adoption probe',
                'adoption probe',
                0
            ),
            (
                'adoption_probe_repo',
                'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                'adoption probe',
                'adoption probe',
                0
            )
        ON CONFLICT DO NOTHING;
    ";
    require_statement_rejected(
        client,
        SETUP,
        "
        INSERT INTO durable_post_cas_recovery_claims (
            repo_id, ref_name, commit_id, step, state, attempts, retry_after, last_error
        )
        VALUES (
            'adoption_probe_repo',
            'main',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'audit_append',
            'backing_off',
            1,
            now(),
            'raw recovery detail'
        );
        ",
    )
    .await?;
    require_statement_rejected(
        client,
        SETUP,
        "
        INSERT INTO durable_pre_visibility_recovery_ledger (
            repo_id, ref_name, commit_id, stage, state, root_tree_id,
            parent_commit_id, expected_ref_version, object_count,
            changed_path_count, has_idempotency_reservation,
            first_seen_at, last_seen_at, occurrence_count, resolved_at,
            context_json, lease_owner, lease_token, lease_expires_at,
            attempts, retry_after, last_error, poisoned_at, updated_at
        )
        VALUES (
            'adoption_probe_repo',
            'main',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'ref_visibility_cas',
            'backing_off',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            NULL,
            1,
            0,
            0,
            false,
            now(),
            now(),
            1,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            1,
            now(),
            'raw recovery detail',
            NULL,
            now()
        );
        ",
    )
    .await?;
    require_statement_rejected(
        client,
        SETUP,
        "
        INSERT INTO durable_fs_mutation_recovery_ledger (
            repo_id, workspace_scope, operation_id, target_ref, previous_commit_id,
            new_commit_id, failed_step, state, attempts, retry_after, last_error,
            envelope_json
        )
        VALUES (
            'adoption_probe_repo',
            'workspace:adoption-probe',
            'operation-adoption-probe',
            'main',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
            'audit_append',
            'backing_off',
            1,
            now(),
            'raw recovery detail',
            '{}'::jsonb
        );
        ",
    )
    .await
}

async fn require_statement_rejected(
    client: &impl GenericClient,
    setup_sql: &str,
    statement: &str,
) -> Result<(), VfsError> {
    client
        .batch_execute("SAVEPOINT stratum_adoption_probe")
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    if !setup_sql.trim().is_empty() && client.batch_execute(setup_sql).await.is_err() {
        let _ = client
            .batch_execute("ROLLBACK TO SAVEPOINT stratum_adoption_probe")
            .await;
        return Err(adoption_verification_error());
    }
    let rejected = client.batch_execute(statement).await.is_err();
    client
        .batch_execute(
            "ROLLBACK TO SAVEPOINT stratum_adoption_probe;
             RELEASE SAVEPOINT stratum_adoption_probe;",
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    if rejected {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_no_constraint(
    client: &impl GenericClient,
    table: &str,
    constraint: &str,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = $1
                  AND c.conname = $2
            )",
            &[&table, &constraint],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Err(adoption_verification_error())
    } else {
        Ok(())
    }
}

async fn require_control_plane_readiness_shape(
    client: &impl GenericClient,
) -> Result<(), VfsError> {
    client
        .batch_execute(
            "SELECT id, name, created_at
             FROM repos
             LIMIT 0;
             SELECT repo_id, kind, object_id, object_key, size_bytes, sha256, created_at
             FROM objects
             LIMIT 0;
             SELECT repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
                    lease_token, lease_expires_at, attempts, last_error, created_at,
                    updated_at, completed_at, deletion_ready_at, delete_after,
                    deletion_snapshot_object_key, deletion_snapshot_size_bytes,
                    deletion_snapshot_sha256, final_object_bytes_deleted_at,
                    final_object_metadata_deleted_at
             FROM object_cleanup_claims
             LIMIT 0;
             SELECT repo_id, id, root_tree_kind, root_tree_id, author, message,
                    commit_timestamp_seconds, created_at, changed_paths_json
             FROM commits
             LIMIT 0;
             SELECT repo_id, commit_id, parent_commit_id, parent_order
             FROM commit_parents
             LIMIT 0;
             SELECT repo_id, name, commit_id, version, updated_at
             FROM refs
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
             SELECT repo_id, workspace_scope, operation_id, target_ref, previous_commit_id,
                    new_commit_id, failed_step, state, lease_owner, lease_token,
                    lease_expires_at, attempts, retry_after, last_error, completed_at,
                    poisoned_at, envelope_json, created_at, updated_at
             FROM durable_fs_mutation_recovery_ledger
             LIMIT 0;
             SELECT uid, repo_id, username, primary_gid, groups_json, kind, active,
                    created_at, updated_at
             FROM durable_principals
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
        )
        .await
        .map_err(|_| adoption_verification_error())?;
    Ok(())
}

async fn require_bool_column_not_null_with_default_true(
    client: &impl GenericClient,
    table: &str,
    column: &str,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = $1
                  AND column_name = $2
                  AND data_type = 'boolean'
                  AND is_nullable = 'NO'
                  AND column_default IN ('true', 'true::boolean')
            )",
            &[&table, &column],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_no_foreign_key_to_table(
    client: &impl GenericClient,
    table: &str,
    referenced_table: &str,
) -> Result<(), VfsError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                JOIN pg_catalog.pg_class rr ON rr.oid = c.confrelid
                JOIN pg_catalog.pg_namespace rn ON rn.oid = rr.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = $1
                  AND c.contype = 'f'
                  AND rn.nspname = current_schema()
                  AND rr.relname = $2
            )",
            &[&table, &referenced_table],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if exists {
        Err(adoption_verification_error())
    } else {
        Ok(())
    }
}

fn adoption_verification_error() -> VfsError {
    VfsError::CorruptStore {
        message: "Postgres schema cannot be verified for migration adoption".to_string(),
    }
}

async fn apply_one_migration(
    client: &mut Transaction<'_>,
    migration: &PostgresMigration,
) -> Result<(), VfsError> {
    record_migration_started(client, migration).await?;

    let apply_result = {
        let transaction = client
            .transaction()
            .await
            .map_err(|error| postgres_error("begin migration transaction", error))?;
        if let Err(error) = transaction.batch_execute(migration.sql).await {
            let mapped = postgres_error("apply migration", error);
            let _ = transaction.rollback().await;
            return Err(record_failure_after_apply_error(client, migration, mapped).await);
        }
        if let Err(error) = record_migration_applied(&transaction, migration).await {
            let _ = transaction.rollback().await;
            return Err(record_failure_after_apply_error(client, migration, error).await);
        }
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit migration transaction", error))
    };

    match apply_result {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = record_migration_failed(client, migration, &error).await;
            Err(error)
        }
    }
}

async fn record_failure_after_apply_error(
    client: &impl GenericClient,
    migration: &PostgresMigration,
    error: VfsError,
) -> VfsError {
    let _ = record_migration_failed(client, migration, &error).await;
    error
}

async fn record_migration_started(
    client: &impl GenericClient,
    migration: &PostgresMigration,
) -> Result<(), VfsError> {
    client
        .execute(
            "INSERT INTO stratum_schema_migrations (
                version,
                name,
                checksum,
                state,
                started_at,
                finished_at,
                failure_message
             )
             VALUES ($1, $2, $3, 'started', clock_timestamp(), NULL, NULL)",
            &[&migration.version, &migration.name, &migration.checksum()],
        )
        .await
        .map_err(|error| postgres_error("record migration start", error))?;
    Ok(())
}

async fn record_migration_applied<C>(
    client: &C,
    migration: &PostgresMigration,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync,
{
    let updated = client
        .execute(
            "UPDATE stratum_schema_migrations
             SET state = 'applied',
                 finished_at = clock_timestamp(),
                 failure_message = NULL
             WHERE version = $1 AND state = 'started'",
            &[&migration.version],
        )
        .await
        .map_err(|error| postgres_error("record migration applied", error))?;
    if updated == 1 {
        Ok(())
    } else {
        Err(VfsError::CorruptStore {
            message: format!(
                "Postgres migration {} start row disappeared before completion",
                migration.version
            ),
        })
    }
}

async fn record_migration_adopted(
    client: &impl GenericClient,
    migration: &PostgresMigration,
) -> Result<(), VfsError> {
    client
        .execute(
            "INSERT INTO stratum_schema_migrations (
                version,
                name,
                checksum,
                state,
                started_at,
                finished_at,
                failure_message
             )
             VALUES ($1, $2, $3, 'applied', clock_timestamp(), clock_timestamp(), NULL)",
            &[&migration.version, &migration.name, &migration.checksum()],
        )
        .await
        .map_err(|error| postgres_error("record adopted migration", error))?;
    Ok(())
}

async fn record_migration_failed(
    client: &impl GenericClient,
    migration: &PostgresMigration,
    error: &VfsError,
) -> Result<(), VfsError> {
    let failure_message = error.to_string();
    client
        .execute(
            "UPDATE stratum_schema_migrations
             SET state = 'failed',
                 finished_at = clock_timestamp(),
                 failure_message = $2
             WHERE version = $1 AND state = 'started'",
            &[&migration.version, &failure_message],
        )
        .await
        .map_err(|error| postgres_error("record migration failure", error))?;
    Ok(())
}

async fn acquire_migration_lock(
    client: &impl GenericClient,
    namespace: i32,
    key: i32,
) -> Result<(), VfsError> {
    let locked: bool = client
        .query_one(
            "SELECT pg_try_advisory_xact_lock($1, $2)",
            &[&namespace, &key],
        )
        .await
        .map_err(|error| postgres_error("acquire migration startup lock", error))?
        .get(0);
    if locked {
        Ok(())
    } else {
        Err(VfsError::ObjectWriteConflict {
            message: "Postgres migration startup lock is already held".to_string(),
        })
    }
}

fn migration_by_version(version: i64) -> Option<&'static PostgresMigration> {
    POSTGRES_MIGRATIONS
        .iter()
        .find(|migration| migration.version == version)
}

fn validate_catalog() -> Result<(), VfsError> {
    let mut previous = 0;
    for migration in &POSTGRES_MIGRATIONS {
        if migration.version <= previous {
            return Err(VfsError::CorruptStore {
                message: "Postgres migration catalog is not strictly ordered".to_string(),
            });
        }
        if migration.name.is_empty() || migration.name.len() > 128 {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "Postgres migration {} has an invalid name",
                    migration.version
                ),
            });
        }
        previous = migration.version;
    }
    Ok(())
}

fn migration_lock_ids(schema: &str) -> (i32, i32) {
    let digest = Sha256::digest(schema.as_bytes());
    let key = i32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    (MIGRATION_LOCK_NAMESPACE, key)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("writing sha256 hex should not fail");
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::Config;
    use uuid::Uuid;

    struct TestDb {
        config: Config,
        schema: String,
    }

    impl TestDb {
        async fn new() -> Option<Self> {
            let Some(url) = std::env::var("STRATUM_POSTGRES_TEST_URL").ok() else {
                if postgres_tests_required() {
                    panic!("STRATUM_POSTGRES_TEST_URL is required for Postgres migration tests");
                }
                eprintln!("skipping Postgres migration tests; STRATUM_POSTGRES_TEST_URL is unset");
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

            let schema = format!("stratum_pg_migrations_{}", Uuid::new_v4().simple());
            let connector = PostgresConnector::local(config.clone());
            let client = connector
                .connect_with_schema(None)
                .await
                .expect("connect test Postgres");
            client
                .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
                .await
                .expect("create isolated schema");
            Some(Self { config, schema })
        }

        fn runner(&self) -> PostgresMigrationRunner {
            PostgresMigrationRunner::with_schema(self.config.clone(), self.schema.clone())
                .expect("create migration runner")
        }

        async fn client_in_schema(&self) -> Client {
            PostgresConnector::local(self.config.clone())
                .connect_with_schema(Some(&self.schema))
                .await
                .expect("connect test Postgres")
        }

        async fn apply_legacy_catalog(&self) {
            let client = self.client_in_schema().await;
            for migration in &POSTGRES_MIGRATIONS {
                client
                    .batch_execute(migration.sql)
                    .await
                    .expect("apply legacy migration catalog");
            }
        }

        async fn cleanup(self) {
            if let Ok(client) = PostgresConnector::local(self.config.clone())
                .connect_with_schema(None)
                .await
            {
                let _ = client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                        self.schema
                    ))
                    .await;
            }
        }
    }

    fn postgres_tests_required() -> bool {
        std::env::var("STRATUM_POSTGRES_TEST_REQUIRED").as_deref() == Ok("1")
            || std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
    }

    fn assert_all_known_applied(report: &PostgresMigrationReport) {
        assert_eq!(report.statuses.len(), POSTGRES_MIGRATIONS.len());
        for (status, migration) in report.statuses.iter().zip(POSTGRES_MIGRATIONS.iter()) {
            assert_eq!(
                status,
                &PostgresMigrationStatus::Applied {
                    version: migration.version,
                    name: migration.name,
                }
            );
        }
    }

    #[tokio::test]
    async fn direct_runner_rejects_remote_no_tls_without_leaking_target() {
        let config: Config = "postgresql://raw-migration-host.internal/stratum"
            .parse()
            .expect("parse hosted postgres config");
        let runner = PostgresMigrationRunner::new(config);

        let err = runner
            .status()
            .await
            .expect_err("direct migration runner should reject remote no-TLS before connect");
        let message = err.to_string();

        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert!(message.contains("sslmode=require"));
        assert!(!message.contains("raw-migration-host.internal"));
    }

    #[test]
    fn idempotency_retention_quota_migration_backfills_quota_identity_from_scope() {
        assert!(
            IDEMPOTENCY_RETENTION_QUOTA_SQL
                .contains("quota_repo_id = substring(scope FROM '^repo:([^:[:space:]]+)'")
        );
        assert!(
            IDEMPOTENCY_RETENTION_QUOTA_SQL
                .contains("quota_workspace_id = substring(scope FROM 'workspace:([^:[:space:]]+)'")
        );

        let workspace_component = regex::Regex::new(r"workspace:([^:\s]+)").unwrap();
        for (scope, expected) in [
            ("workspace:workspace_a:runs:create", "workspace_a"),
            (
                "repo:repo_a:workspace:workspace_a:runs:create",
                "workspace_a",
            ),
            ("POST /runs workspace:workspace_a", "workspace_a"),
        ] {
            let captured = workspace_component
                .captures(scope)
                .and_then(|captures| captures.get(1))
                .map(|value| value.as_str());
            assert_eq!(captured, Some(expected), "scope: {scope}");
        }
    }

    #[test]
    fn object_cleanup_deletion_state_migration_constrains_phase_markers() {
        for expected in [
            "object_cleanup_claims_deletion_phase_claim_kind_check",
            "object_cleanup_claims_deletion_phase_order_check",
            "final_object_metadata_deleted_at IS NULL\n            OR final_object_bytes_deleted_at IS NOT NULL",
            "final_object_bytes_deleted_at >= delete_after",
            "final_object_metadata_deleted_at >= final_object_bytes_deleted_at",
            "claim_kind = 'durable_mutation_cas_lost_object_cleanup'",
        ] {
            assert!(
                OBJECT_CLEANUP_DELETION_STATE_SQL.contains(expected),
                "missing migration invariant: {expected}"
            );
        }
    }

    #[tokio::test]
    async fn idempotency_retention_quota_migration_backfills_existing_scope_rows() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let client = db.client_in_schema().await;
        client
            .batch_execute(DURABLE_BACKEND_FOUNDATION_SQL)
            .await
            .expect("apply foundation migration");
        client
            .execute(
                r#"INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state)
                   VALUES
                     ('repo:repo_a:workspace:workspace_a:runs:create', repeat('a', 64), repeat('b', 64), 'pending'),
                     ('POST /runs workspace:workspace_b', repeat('c', 64), repeat('d', 64), 'pending'),
                     ('repo:repo_c:vcs:commit', repeat('e', 64), repeat('f', 64), 'pending')"#,
                &[],
            )
            .await
            .expect("seed legacy idempotency rows");
        client
            .batch_execute(IDEMPOTENCY_RETENTION_QUOTA_SQL)
            .await
            .expect("apply idempotency retention quota migration");

        let rows = client
            .query(
                r#"SELECT scope, quota_repo_id, quota_workspace_id
                   FROM idempotency_records
                   ORDER BY scope ASC"#,
                &[],
            )
            .await
            .expect("load migrated idempotency rows");
        let migrated = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>("scope"),
                    row.get::<_, Option<String>>("quota_repo_id"),
                    row.get::<_, Option<String>>("quota_workspace_id"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            migrated,
            vec![
                (
                    "POST /runs workspace:workspace_b".to_string(),
                    None,
                    Some("workspace_b".to_string()),
                ),
                (
                    "repo:repo_a:workspace:workspace_a:runs:create".to_string(),
                    Some("repo_a".to_string()),
                    Some("workspace_a".to_string()),
                ),
                (
                    "repo:repo_c:vcs:commit".to_string(),
                    Some("repo_c".to_string()),
                    None,
                ),
            ]
        );
        db.cleanup().await;
    }

    #[tokio::test]
    async fn status_reports_initial_catalog_migration_as_pending() {
        let Some(db) = TestDb::new().await else {
            return;
        };

        let report = db.runner().status().await.expect("load migration status");

        assert_eq!(
            report.statuses,
            vec![
                PostgresMigrationStatus::Pending {
                    version: 1,
                    name: "durable_backend_foundation",
                },
                PostgresMigrationStatus::Pending {
                    version: 2,
                    name: "review_local_commit_ids",
                },
                PostgresMigrationStatus::Pending {
                    version: 3,
                    name: "guarded_commit_recovery_claims",
                },
                PostgresMigrationStatus::Pending {
                    version: 4,
                    name: "guarded_commit_recovery_context",
                },
                PostgresMigrationStatus::Pending {
                    version: 5,
                    name: "guarded_commit_pre_visibility_recovery",
                },
                PostgresMigrationStatus::Pending {
                    version: 6,
                    name: "pre_visibility_recovery_run_control",
                },
                PostgresMigrationStatus::Pending {
                    version: 7,
                    name: "durable_fs_mutation_recovery",
                },
                PostgresMigrationStatus::Pending {
                    version: 8,
                    name: "durable_mutation_cleanup_claim_kind",
                },
                PostgresMigrationStatus::Pending {
                    version: 9,
                    name: "durable_auth_session_foundation",
                },
                PostgresMigrationStatus::Pending {
                    version: 10,
                    name: "object_deletion_fences",
                },
                PostgresMigrationStatus::Pending {
                    version: 11,
                    name: "idempotency_retention_quota",
                },
                PostgresMigrationStatus::Pending {
                    version: 12,
                    name: "object_cleanup_deletion_state",
                },
                PostgresMigrationStatus::Pending {
                    version: 13,
                    name: "protected_rules_require_all_files_viewed",
                },
                PostgresMigrationStatus::Pending {
                    version: 14,
                    name: "secret_bearing_idempotency_replay",
                },
            ]
        );
        db.cleanup().await;
    }

    #[tokio::test]
    async fn apply_pending_records_migration_and_second_apply_is_noop() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let runner = db.runner();

        let first = runner.apply_pending().await.expect("apply migrations");
        let second = runner.apply_pending().await.expect("reapply migrations");
        let status = runner.status().await.expect("load migration status");

        assert_eq!(
            first.statuses,
            vec![
                PostgresMigrationStatus::Applied {
                    version: 1,
                    name: "durable_backend_foundation",
                },
                PostgresMigrationStatus::Applied {
                    version: 2,
                    name: "review_local_commit_ids",
                },
                PostgresMigrationStatus::Applied {
                    version: 3,
                    name: "guarded_commit_recovery_claims",
                },
                PostgresMigrationStatus::Applied {
                    version: 4,
                    name: "guarded_commit_recovery_context",
                },
                PostgresMigrationStatus::Applied {
                    version: 5,
                    name: "guarded_commit_pre_visibility_recovery",
                },
                PostgresMigrationStatus::Applied {
                    version: 6,
                    name: "pre_visibility_recovery_run_control",
                },
                PostgresMigrationStatus::Applied {
                    version: 7,
                    name: "durable_fs_mutation_recovery",
                },
                PostgresMigrationStatus::Applied {
                    version: 8,
                    name: "durable_mutation_cleanup_claim_kind",
                },
                PostgresMigrationStatus::Applied {
                    version: 9,
                    name: "durable_auth_session_foundation",
                },
                PostgresMigrationStatus::Applied {
                    version: 10,
                    name: "object_deletion_fences",
                },
                PostgresMigrationStatus::Applied {
                    version: 11,
                    name: "idempotency_retention_quota",
                },
                PostgresMigrationStatus::Applied {
                    version: 12,
                    name: "object_cleanup_deletion_state",
                },
                PostgresMigrationStatus::Applied {
                    version: 13,
                    name: "protected_rules_require_all_files_viewed",
                },
                PostgresMigrationStatus::Applied {
                    version: 14,
                    name: "secret_bearing_idempotency_replay",
                },
            ]
        );
        assert_eq!(
            second.statuses,
            vec![
                PostgresMigrationStatus::Applied {
                    version: 1,
                    name: "durable_backend_foundation",
                },
                PostgresMigrationStatus::Applied {
                    version: 2,
                    name: "review_local_commit_ids",
                },
                PostgresMigrationStatus::Applied {
                    version: 3,
                    name: "guarded_commit_recovery_claims",
                },
                PostgresMigrationStatus::Applied {
                    version: 4,
                    name: "guarded_commit_recovery_context",
                },
                PostgresMigrationStatus::Applied {
                    version: 5,
                    name: "guarded_commit_pre_visibility_recovery",
                },
                PostgresMigrationStatus::Applied {
                    version: 6,
                    name: "pre_visibility_recovery_run_control",
                },
                PostgresMigrationStatus::Applied {
                    version: 7,
                    name: "durable_fs_mutation_recovery",
                },
                PostgresMigrationStatus::Applied {
                    version: 8,
                    name: "durable_mutation_cleanup_claim_kind",
                },
                PostgresMigrationStatus::Applied {
                    version: 9,
                    name: "durable_auth_session_foundation",
                },
                PostgresMigrationStatus::Applied {
                    version: 10,
                    name: "object_deletion_fences",
                },
                PostgresMigrationStatus::Applied {
                    version: 11,
                    name: "idempotency_retention_quota",
                },
                PostgresMigrationStatus::Applied {
                    version: 12,
                    name: "object_cleanup_deletion_state",
                },
                PostgresMigrationStatus::Applied {
                    version: 13,
                    name: "protected_rules_require_all_files_viewed",
                },
                PostgresMigrationStatus::Applied {
                    version: 14,
                    name: "secret_bearing_idempotency_replay",
                },
            ]
        );
        assert_eq!(status, second);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn status_reports_legacy_schema_as_pending_without_adopting() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;

        let report = db.runner().status().await.expect("load migration status");

        assert!(
            report
                .statuses
                .iter()
                .all(|status| matches!(status, PostgresMigrationStatus::Pending { .. }))
        );
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_applied_records_legacy_catalog_without_replaying_ddl() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        let runner = db.runner();

        let first = runner.adopt_applied().await.expect("adopt migrations");
        let second = runner
            .adopt_applied()
            .await
            .expect("second adoption should be no-op");
        let status = runner.status().await.expect("load adopted status");

        assert_all_known_applied(&first);
        assert_eq!(second, first);
        assert_eq!(status, first);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn apply_does_not_implicitly_adopt_legacy_schema() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;

        let err = db
            .runner()
            .apply_pending()
            .await
            .expect_err("apply should replay pending migration and fail");

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_dirty_unknown_checksum_mismatch_and_partial_control_rows() {
        for state in ["started", "failed"] {
            let Some(db) = TestDb::new().await else {
                return;
            };
            let runner = db.runner();
            runner
                .create_control_table_for_test()
                .await
                .expect("create control table");
            runner
                .insert_control_row_for_test(1, state, "bogus")
                .await
                .expect("insert dirty migration row");

            let err = runner
                .adopt_applied()
                .await
                .expect_err("dirty row should fail adoption");
            let message = err.to_string();

            assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
            assert!(!message.contains("migration_1"));
            db.cleanup().await;
        }

        let Some(db) = TestDb::new().await else {
            return;
        };
        let runner = db.runner();
        runner
            .create_control_table_for_test()
            .await
            .expect("create control table");
        runner
            .insert_control_row_for_test(1, "applied", "bogus")
            .await
            .expect("insert mismatched migration row");
        let err = runner
            .adopt_applied()
            .await
            .expect_err("checksum mismatch should fail adoption");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        let runner = db.runner();
        runner
            .create_control_table_for_test()
            .await
            .expect("create control table");
        runner
            .insert_control_row_for_test(999, "applied", "bogus")
            .await
            .expect("insert unknown migration row");
        let err = runner
            .adopt_applied()
            .await
            .expect_err("unknown version should fail adoption");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        let runner = db.runner();
        runner
            .create_control_table_for_test()
            .await
            .expect("create control table");
        let migration = &POSTGRES_MIGRATIONS[0];
        db.client_in_schema()
            .await
            .execute(
                "INSERT INTO stratum_schema_migrations (
                    version,
                    name,
                    checksum,
                    state,
                    started_at,
                    finished_at,
                    failure_message
                 )
                 VALUES ($1, $2, $3, 'applied', clock_timestamp(), clock_timestamp(), NULL)",
                &[&migration.version, &migration.name, &migration.checksum()],
            )
            .await
            .expect("insert partial applied control row");
        let err = runner
            .adopt_applied()
            .await
            .expect_err("partial control table should fail adoption");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(err.to_string().contains("partially populated"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_unverifiable_schema() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE durable_fs_mutation_recovery_ledger DROP COLUMN envelope_json",
            )
            .await
            .expect("make legacy schema unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("unverifiable schema should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("durable_fs_mutation_recovery_ledger"));
        assert!(!message.contains("envelope_json"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_schema_missing_fixed_redaction_constraints() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE durable_fs_mutation_recovery_ledger
                 DROP CONSTRAINT durable_fs_mutation_recovery_backoff_check",
            )
            .await
            .expect("make fixed-redaction constraint unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing redaction constraint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("durable_fs_mutation_recovery_ledger"));
        assert!(!message.contains("durable_fs_mutation_recovery_backoff_check"));
        assert!(!message.contains("redacted durable FS mutation recovery failure"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_schema_missing_secret_replay_constraints() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE idempotency_records
                 DROP CONSTRAINT idempotency_records_secret_replay_envelope_shape_check",
            )
            .await
            .expect("make secret replay constraint unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing secret replay constraint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("idempotency_records"));
        assert!(!message.contains("secret_replay"));
        assert!(!message.contains("ciphertext_b64"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_schema_with_raw_recovery_error_rows() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE durable_pre_visibility_recovery_ledger
                    DROP CONSTRAINT durable_pre_visibility_recovery_backoff_check;
                 ALTER TABLE durable_pre_visibility_recovery_ledger
                    ADD CONSTRAINT durable_pre_visibility_recovery_backoff_check CHECK (
                        state <> 'backing_off'
                        OR true
                        OR last_error = 'redacted pre-visibility recovery failure'
                    );
                 INSERT INTO repos (id, name)
                 VALUES ('adoption_probe_repo', 'adoption probe');
                 INSERT INTO durable_pre_visibility_recovery_ledger (
                    repo_id, ref_name, commit_id, stage, state, root_tree_id,
                    parent_commit_id, expected_ref_version, object_count,
                    changed_path_count, has_idempotency_reservation,
                    first_seen_at, last_seen_at, occurrence_count, resolved_at,
                    context_json, lease_owner, lease_token, lease_expires_at,
                    attempts, retry_after, last_error, poisoned_at, updated_at
                 )
                 VALUES (
                    'adoption_probe_repo',
                    'main',
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    'ref_visibility_cas',
                    'backing_off',
                    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                    NULL,
                    1,
                    0,
                    0,
                    false,
                    now(),
                    now(),
                    1,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    1,
                    now(),
                    'raw recovery detail',
                    NULL,
                    now()
                 );",
            )
            .await
            .expect("make recovery error state unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("raw recovery error should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("raw recovery detail"));
        assert!(!message.contains("durable_pre_visibility_recovery_ledger"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_weakened_fixed_redaction_constraints() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE durable_fs_mutation_recovery_ledger
                    DROP CONSTRAINT durable_fs_mutation_recovery_backoff_check;
                 ALTER TABLE durable_fs_mutation_recovery_ledger
                    ADD CONSTRAINT durable_fs_mutation_recovery_backoff_check CHECK (
                        state <> 'backing_off'
                        OR true
                        OR last_error = 'redacted durable FS mutation recovery failure'
                    );",
            )
            .await
            .expect("weaken fixed-redaction constraint");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened redaction constraint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("durable_fs_mutation_recovery_ledger"));
        assert!(!message.contains("redacted durable FS mutation recovery failure"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_schema_missing_key_constraints() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE durable_fs_mutation_recovery_ledger
                 DROP CONSTRAINT durable_fs_mutation_recovery_ledger_pkey",
            )
            .await
            .expect("make durable FS conflict target unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing durable FS primary key should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("durable_fs_mutation_recovery_ledger"));
        assert!(!message.contains("durable_fs_mutation_recovery_ledger_pkey"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE durable_principals
                 DROP CONSTRAINT durable_principals_repo_id_username_key",
            )
            .await
            .expect("make durable principal unique key unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing durable principal unique key should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("durable_principals"));
        assert!(!message.contains("durable_principals_repo_id_username_key"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_schema_missing_workspace_token_backfill_shape() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute("ALTER TABLE workspace_tokens ALTER COLUMN issued_at DROP NOT NULL")
            .await
            .expect("make workspace token lifecycle shape unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing workspace token not-null shape should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("workspace_tokens"));
        assert!(!message.contains("issued_at"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "INSERT INTO repos (id, name)
                 VALUES ('workspace_token_probe_repo', 'workspace token probe');
                 INSERT INTO workspaces (id, repo_id, name, root_path)
                 VALUES (
                    '00000000-0000-0000-0000-000000009001',
                    'workspace_token_probe_repo',
                    'workspace token probe',
                    '/workspace-token-probe'
                 );
                 INSERT INTO workspace_tokens (
                    id, workspace_id, repo_id, name, agent_uid, secret_hash, principal_uid
                 )
                 VALUES (
                    '00000000-0000-0000-0000-000000009002',
                    '00000000-0000-0000-0000-000000009001',
                    NULL,
                    'workspace token probe',
                    42,
                    'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
                    42
                 );",
            )
            .await
            .expect("make workspace token repo backfill unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("workspace token repo mismatch should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("workspace_tokens"));
        assert!(!message.contains("repo_id"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_revalidates_schema_when_control_rows_are_already_applied() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        let runner = db.runner();
        runner.adopt_applied().await.expect("adopt migrations");
        db.client_in_schema()
            .await
            .batch_execute("ALTER TABLE protected_path_rules DROP COLUMN require_all_files_viewed")
            .await
            .expect("make adopted schema unverifiable");

        let err = runner
            .adopt_applied()
            .await
            .expect_err("already adopted but unverifiable schema should fail adoption");

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(err.to_string().contains("cannot be verified"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn debug_redacts_connection_details() {
        let Some(db) = TestDb::new().await else {
            return;
        };

        let debug = format!("{:?}", db.runner());

        assert!(debug.contains("PostgresMigrationRunner"));
        assert!(debug.contains(&db.schema));
        assert!(debug.contains("migration_count"));
        assert!(!debug.contains("postgres://"));
        assert!(!debug.contains("postgresql://"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn dirty_started_or_failed_row_refuses_apply() {
        for state in ["started", "failed"] {
            let Some(db) = TestDb::new().await else {
                return;
            };
            let runner = db.runner();
            runner
                .create_control_table_for_test()
                .await
                .expect("create control table");
            runner
                .insert_control_row_for_test(1, state, "bogus")
                .await
                .expect("insert dirty migration row");

            let err = runner
                .apply_pending()
                .await
                .expect_err("dirty row should fail");

            assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
            db.cleanup().await;
        }
    }

    #[tokio::test]
    async fn checksum_mismatch_refuses_apply() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let runner = db.runner();
        runner
            .create_control_table_for_test()
            .await
            .expect("create control table");
        runner
            .insert_control_row_for_test(1, "applied", "bogus")
            .await
            .expect("insert mismatched migration row");

        let err = runner
            .apply_pending()
            .await
            .expect_err("checksum mismatch should fail");

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn unknown_applied_version_refuses_apply() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let runner = db.runner();
        runner
            .create_control_table_for_test()
            .await
            .expect("create control table");
        runner
            .insert_control_row_for_test(999, "applied", "bogus")
            .await
            .expect("insert unknown migration row");

        let err = runner
            .apply_pending()
            .await
            .expect_err("unknown version should fail");

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        db.cleanup().await;
    }

    #[test]
    fn apply_validation_errors_do_not_echo_db_sourced_fields() {
        let cases = vec![
            PostgresMigrationStatus::Dirty {
                version: 12,
                name: "raw-dirty-secret".to_string(),
                state: "raw-state-secret".to_string(),
            },
            PostgresMigrationStatus::ChecksumMismatch {
                version: 13,
                name: "raw-checksum-secret".to_string(),
            },
            PostgresMigrationStatus::UnknownApplied {
                version: 14,
                name: "raw-unknown-secret".to_string(),
            },
        ];

        for status in cases {
            let report = PostgresMigrationReport {
                statuses: vec![status],
            };

            let err =
                validate_report_for_apply(&report).expect_err("invalid status should fail apply");
            let message = err.to_string();

            assert!(!message.contains("raw-"));
        }
    }

    #[tokio::test]
    async fn held_schema_advisory_lock_refuses_apply() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let runner = db.runner();
        let lock = runner
            .hold_advisory_lock_for_test()
            .await
            .expect("hold migration advisory lock");

        let err = runner
            .apply_pending()
            .await
            .expect_err("held lock should fail");

        assert!(matches!(
            err,
            crate::error::VfsError::ObjectWriteConflict { .. }
        ));
        drop(lock);
        db.cleanup().await;
    }
}
