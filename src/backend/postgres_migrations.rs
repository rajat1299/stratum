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
    PostgresAdvisoryXactLockKey, PostgresConnector, infer_tls_mode, postgres_error,
    postgres_try_advisory_xact_lock, validate_schema_name,
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
const ORG_TENANT_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0015_org_tenant_foundation.sql");
const OIDC_REFRESH_TOKEN_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0016_oidc_refresh_token_foundation.sql");
const SAML_SSO_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0017_saml_sso_foundation.sql");
const SCIM_PROVISIONING_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0018_scim_provisioning_foundation.sql");
const POSTGRES_FTS_SEARCH_MVP_SQL: &str =
    include_str!("../../migrations/postgres/0019_postgres_fts_search_mvp.sql");
const ACL_SNAPSHOT_FILTERING_SQL: &str =
    include_str!("../../migrations/postgres/0020_acl_snapshot_filtering.sql");
const FILE_EXTRACTORS_SQL: &str =
    include_str!("../../migrations/postgres/0021_file_extractors.sql");
const POSTGRES_MIGRATION_0022_PGVECTOR_SEMANTIC_EXPANSION: &str =
    include_str!("../../migrations/postgres/0022_pgvector_semantic_expansion.sql");
const POSTGRES_MIGRATION_0023_REVIEW_VIEWED_FILES: &str =
    include_str!("../../migrations/postgres/0023_review_viewed_files.sql");
const POSTGRES_MIGRATIONS: [PostgresMigration; 23] = [
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
    PostgresMigration {
        version: 15,
        name: "org_tenant_foundation",
        sql: ORG_TENANT_FOUNDATION_SQL,
    },
    PostgresMigration {
        version: 16,
        name: "oidc_refresh_token_foundation",
        sql: OIDC_REFRESH_TOKEN_FOUNDATION_SQL,
    },
    PostgresMigration {
        version: 17,
        name: "saml_sso_foundation",
        sql: SAML_SSO_FOUNDATION_SQL,
    },
    PostgresMigration {
        version: 18,
        name: "scim_provisioning_foundation",
        sql: SCIM_PROVISIONING_FOUNDATION_SQL,
    },
    PostgresMigration {
        version: 19,
        name: "postgres_fts_search_mvp",
        sql: POSTGRES_FTS_SEARCH_MVP_SQL,
    },
    PostgresMigration {
        version: 20,
        name: "acl_snapshot_filtering",
        sql: ACL_SNAPSHOT_FILTERING_SQL,
    },
    PostgresMigration {
        version: 21,
        name: "file_extractors",
        sql: FILE_EXTRACTORS_SQL,
    },
    PostgresMigration {
        version: 22,
        name: "pgvector_semantic_expansion",
        sql: POSTGRES_MIGRATION_0022_PGVECTOR_SEMANTIC_EXPANSION,
    },
    PostgresMigration {
        version: 23,
        name: "review_viewed_files",
        sql: POSTGRES_MIGRATION_0023_REVIEW_VIEWED_FILES,
    },
];

#[must_use]
pub fn postgres_migration_catalog_len() -> usize {
    POSTGRES_MIGRATIONS.len()
}

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

        match self.apply_pending_locked(&mut transaction).await {
            Ok(report) => transaction
                .commit()
                .await
                .map(|()| report)
                .map_err(|error| postgres_error("commit migration startup transaction", error)),
            Err(ApplyPendingError::Commit(error)) => {
                let _ = transaction.commit().await;
                Err(error)
            }
            Err(ApplyPendingError::Rollback(error)) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
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
    ) -> Result<PostgresMigrationReport, ApplyPendingError> {
        ensure_control_table(client)
            .await
            .map_err(ApplyPendingError::Rollback)?;
        let initial = self
            .status_with_client(client)
            .await
            .map_err(ApplyPendingError::Rollback)?;
        validate_report_for_apply(&initial).map_err(ApplyPendingError::Rollback)?;

        for status in initial.statuses {
            let PostgresMigrationStatus::Pending { version, .. } = status else {
                continue;
            };
            let migration = migration_by_version(version)
                .ok_or_else(|| VfsError::CorruptStore {
                    message: format!("unknown Postgres migration version: {version}"),
                })
                .map_err(ApplyPendingError::Rollback)?;
            apply_one_migration(client, migration)
                .await
                .map_err(ApplyPendingError::Commit)?;
        }

        verify_known_schema_catalog(client)
            .await
            .map_err(ApplyPendingError::Rollback)?;
        self.status_with_client(client)
            .await
            .map_err(ApplyPendingError::Rollback)
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

enum ApplyPendingError {
    Commit(VfsError),
    Rollback(VfsError),
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
        "organizations",
        "org_memberships",
        "org_service_accounts",
        "oidc_providers",
        "external_identities",
        "saml_providers",
        "saml_external_identities",
        "saml_group_mappings",
        "saml_assertion_replay",
        "scim_clients",
        "scim_users",
        "scim_groups",
        "scim_group_members",
        "refresh_token_families",
        "refresh_tokens",
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
        "change_request_file_views",
        "durable_post_cas_recovery_claims",
        "durable_pre_visibility_recovery_ledger",
        "durable_fs_mutation_recovery_ledger",
        "durable_principals",
        "object_deletion_fences",
    ] {
        require_table(client, table).await?;
    }

    for (table, column) in [
        ("repos", "org_id"),
        ("oidc_providers", "provider_key"),
        ("oidc_providers", "issuer_hash"),
        ("oidc_providers", "client_id_hash"),
        ("oidc_providers", "client_secret_ref"),
        ("external_identities", "provider_id"),
        ("external_identities", "subject_hash"),
        ("external_identities", "principal_uid"),
        ("saml_providers", "repo_id"),
        ("saml_providers", "provider_key"),
        ("saml_providers", "idp_entity_id_hash"),
        ("saml_providers", "sp_entity_id_hash"),
        ("saml_providers", "acs_url_hash"),
        ("saml_providers", "audience_hash"),
        ("saml_providers", "signing_cert_ref_hash"),
        ("saml_providers", "binding"),
        ("saml_providers", "group_attribute"),
        ("saml_providers", "required_group_hash"),
        ("saml_providers", "enabled"),
        ("saml_external_identities", "provider_id"),
        ("saml_external_identities", "principal_uid"),
        ("saml_external_identities", "name_id_hash"),
        ("saml_external_identities", "username_hint"),
        ("saml_group_mappings", "provider_id"),
        ("saml_group_mappings", "external_group_hash"),
        ("saml_group_mappings", "external_group_name_ref"),
        ("saml_group_mappings", "local_gid"),
        ("saml_group_mappings", "required"),
        ("saml_group_mappings", "active"),
        ("saml_assertion_replay", "provider_id"),
        ("saml_assertion_replay", "principal_uid"),
        ("saml_assertion_replay", "assertion_id_hash"),
        ("saml_assertion_replay", "replay_key"),
        ("saml_assertion_replay", "first_seen_at"),
        ("saml_assertion_replay", "expires_at"),
        ("scim_clients", "repo_id"),
        ("scim_clients", "provider_key"),
        ("scim_clients", "token_hash"),
        ("scim_clients", "enabled"),
        ("scim_users", "client_id"),
        ("scim_users", "principal_uid"),
        ("scim_users", "external_id_hash"),
        ("scim_users", "username_hint"),
        ("scim_users", "active"),
        ("scim_groups", "client_id"),
        ("scim_groups", "local_gid"),
        ("scim_groups", "external_id_hash"),
        ("scim_groups", "display_name"),
        ("scim_groups", "active"),
        ("scim_group_members", "group_id"),
        ("scim_group_members", "principal_uid"),
        ("scim_group_members", "active"),
        ("refresh_token_families", "external_identity_id"),
        ("refresh_token_families", "current_token_version"),
        ("refresh_token_families", "reuse_detected_at"),
        ("refresh_tokens", "family_id"),
        ("refresh_tokens", "token_hash"),
        ("refresh_tokens", "token_version"),
        ("refresh_tokens", "rotated_at"),
        ("refresh_tokens", "rotated_to_token_id"),
        ("refresh_tokens", "revoked_at"),
        ("refresh_tokens", "reuse_denied_at"),
        ("workspaces", "org_id"),
        ("workspace_tokens", "org_id"),
        ("durable_principals", "org_id"),
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

    require_saml_column_shapes(client).await?;
    require_scim_column_shapes(client).await?;
    require_scim_no_raw_material_columns(client).await?;

    for index in [
        "repos_org_id_idx",
        "org_memberships_principal_idx",
        "org_service_accounts_org_active_idx",
        "oidc_providers_org_key_idx",
        "external_identities_provider_subject_idx",
        "external_identities_principal_idx",
        "saml_providers_org_repo_key_idx",
        "saml_providers_enabled_idx",
        "saml_external_identities_provider_name_id_idx",
        "saml_external_identities_principal_idx",
        "saml_group_mappings_provider_group_idx",
        "saml_group_mappings_local_gid_idx",
        "saml_assertion_replay_key_idx",
        "saml_assertion_replay_provider_assertion_idx",
        "saml_assertion_replay_expiry_idx",
        "scim_clients_org_repo_key_idx",
        "scim_clients_enabled_idx",
        "scim_users_client_external_id_idx",
        "scim_users_principal_idx",
        "scim_groups_client_external_id_idx",
        "scim_groups_local_gid_idx",
        "scim_group_members_group_principal_idx",
        "scim_group_members_principal_idx",
        "refresh_token_families_active_principal_idx",
        "refresh_token_families_expiry_idx",
        "refresh_tokens_family_active_idx",
        "refresh_tokens_principal_active_idx",
        "workspaces_org_repo_idx",
        "workspace_tokens_org_repo_idx",
        "durable_principals_org_repo_idx",
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
            "organizations",
            "organizations_archived_at_finite_check",
            None,
        ),
        (
            "org_memberships",
            "org_memberships_created_at_finite_check",
            None,
        ),
        (
            "org_memberships",
            "org_memberships_updated_at_finite_check",
            None,
        ),
        (
            "org_service_accounts",
            "org_service_accounts_created_at_finite_check",
            None,
        ),
        (
            "org_service_accounts",
            "org_service_accounts_updated_at_finite_check",
            None,
        ),
        (
            "oidc_providers",
            "oidc_providers_created_at_finite_check",
            None,
        ),
        (
            "oidc_providers",
            "oidc_providers_updated_at_finite_check",
            None,
        ),
        (
            "oidc_providers",
            "oidc_providers_disabled_at_finite_check",
            None,
        ),
        (
            "oidc_providers",
            "oidc_providers_provider_key_check",
            Some("provider_key"),
        ),
        (
            "oidc_providers",
            "oidc_providers_display_name_check",
            Some("display_name"),
        ),
        (
            "oidc_providers",
            "oidc_providers_issuer_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "oidc_providers",
            "oidc_providers_client_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "oidc_providers",
            "oidc_providers_client_secret_ref_check",
            Some("client_secret_ref"),
        ),
        ("oidc_providers", "oidc_providers_lifecycle_check", None),
        (
            "external_identities",
            "external_identities_created_at_finite_check",
            None,
        ),
        (
            "external_identities",
            "external_identities_updated_at_finite_check",
            None,
        ),
        (
            "external_identities",
            "external_identities_disabled_at_finite_check",
            None,
        ),
        (
            "external_identities",
            "external_identities_subject_hash_check",
            Some("subject_hash"),
        ),
        (
            "external_identities",
            "external_identities_username_hint_check",
            Some("username_hint"),
        ),
        (
            "external_identities",
            "external_identities_lifecycle_check",
            None,
        ),
        (
            "saml_providers",
            "saml_providers_provider_key_check",
            Some("provider_key"),
        ),
        (
            "saml_providers",
            "saml_providers_display_name_check",
            Some("display_name"),
        ),
        (
            "saml_providers",
            "saml_providers_idp_entity_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_providers",
            "saml_providers_sp_entity_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_providers",
            "saml_providers_acs_url_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_providers",
            "saml_providers_audience_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_providers",
            "saml_providers_signing_cert_ref_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_providers",
            "saml_providers_binding_check",
            Some("binding = 'post'"),
        ),
        (
            "saml_providers",
            "saml_providers_group_attribute_check",
            Some("group_attribute"),
        ),
        (
            "saml_providers",
            "saml_providers_required_group_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_providers",
            "saml_providers_created_at_finite_check",
            None,
        ),
        (
            "saml_providers",
            "saml_providers_updated_at_finite_check",
            None,
        ),
        (
            "saml_providers",
            "saml_providers_disabled_at_finite_check",
            None,
        ),
        (
            "saml_providers",
            "saml_providers_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "saml_external_identities",
            "saml_external_identities_name_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_external_identities",
            "saml_external_identities_username_hint_check",
            Some("username_hint"),
        ),
        (
            "saml_external_identities",
            "saml_external_identities_created_at_finite_check",
            None,
        ),
        (
            "saml_external_identities",
            "saml_external_identities_updated_at_finite_check",
            None,
        ),
        (
            "saml_external_identities",
            "saml_external_identities_disabled_at_finite_check",
            None,
        ),
        (
            "saml_external_identities",
            "saml_external_identities_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_external_group_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_external_group_name_ref_check",
            Some("external_group_name_ref"),
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_local_gid_check",
            Some("local_gid"),
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_created_at_finite_check",
            None,
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_updated_at_finite_check",
            None,
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_disabled_at_finite_check",
            None,
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_assertion_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_replay_key_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_first_seen_at_finite_check",
            None,
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_expires_at_finite_check",
            None,
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_lifecycle_check",
            Some("expires_at > first_seen_at"),
        ),
        (
            "scim_clients",
            "scim_clients_provider_key_check",
            Some("provider_key"),
        ),
        (
            "scim_clients",
            "scim_clients_token_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        ("scim_clients", "scim_clients_created_at_finite_check", None),
        ("scim_clients", "scim_clients_updated_at_finite_check", None),
        (
            "scim_clients",
            "scim_clients_disabled_at_finite_check",
            None,
        ),
        (
            "scim_clients",
            "scim_clients_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "scim_users",
            "scim_users_external_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "scim_users",
            "scim_users_username_hint_check",
            Some("username_hint"),
        ),
        ("scim_users", "scim_users_created_at_finite_check", None),
        ("scim_users", "scim_users_updated_at_finite_check", None),
        ("scim_users", "scim_users_disabled_at_finite_check", None),
        (
            "scim_users",
            "scim_users_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "scim_groups",
            "scim_groups_local_gid_check",
            Some("local_gid"),
        ),
        (
            "scim_groups",
            "scim_groups_external_id_hash_check",
            Some("^[0-9a-f]{64}$"),
        ),
        (
            "scim_groups",
            "scim_groups_display_name_check",
            Some("display_name"),
        ),
        ("scim_groups", "scim_groups_created_at_finite_check", None),
        ("scim_groups", "scim_groups_updated_at_finite_check", None),
        ("scim_groups", "scim_groups_disabled_at_finite_check", None),
        (
            "scim_groups",
            "scim_groups_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "scim_group_members",
            "scim_group_members_created_at_finite_check",
            None,
        ),
        (
            "scim_group_members",
            "scim_group_members_updated_at_finite_check",
            None,
        ),
        (
            "scim_group_members",
            "scim_group_members_disabled_at_finite_check",
            None,
        ),
        (
            "scim_group_members",
            "scim_group_members_lifecycle_check",
            Some("updated_at >= created_at"),
        ),
        (
            "refresh_token_families",
            "refresh_token_families_issued_at_finite_check",
            None,
        ),
        (
            "refresh_token_families",
            "refresh_token_families_updated_at_finite_check",
            None,
        ),
        (
            "refresh_token_families",
            "refresh_token_families_expires_at_finite_check",
            None,
        ),
        (
            "refresh_token_families",
            "refresh_token_families_revoked_at_finite_check",
            None,
        ),
        (
            "refresh_token_families",
            "refresh_token_families_reuse_detected_at_finite_check",
            None,
        ),
        (
            "refresh_token_families",
            "refresh_token_families_current_token_version_check",
            Some("current_token_version"),
        ),
        (
            "refresh_token_families",
            "refresh_token_families_lifecycle_check",
            None,
        ),
        (
            "refresh_tokens",
            "refresh_tokens_token_hash_check",
            Some("token_hash"),
        ),
        (
            "refresh_tokens",
            "refresh_tokens_token_version_check",
            Some("token_version"),
        ),
        (
            "refresh_tokens",
            "refresh_tokens_issued_at_finite_check",
            None,
        ),
        (
            "refresh_tokens",
            "refresh_tokens_expires_at_finite_check",
            None,
        ),
        (
            "refresh_tokens",
            "refresh_tokens_rotated_at_finite_check",
            None,
        ),
        (
            "refresh_tokens",
            "refresh_tokens_revoked_at_finite_check",
            None,
        ),
        (
            "refresh_tokens",
            "refresh_tokens_reuse_denied_at_finite_check",
            None,
        ),
        (
            "refresh_tokens",
            "refresh_tokens_rotation_shape_check",
            None,
        ),
        ("refresh_tokens", "refresh_tokens_lifecycle_check", None),
        ("workspaces", "workspaces_org_repo_shape_check", None),
        (
            "workspace_tokens",
            "workspace_tokens_org_repo_shape_check",
            None,
        ),
        (
            "durable_principals",
            "durable_principals_org_repo_shape_check",
            None,
        ),
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
            Some("secret_replay_encrypted_at"),
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

    for (table, constraint, required_fragments) in [
        (
            "oidc_providers",
            "oidc_providers_provider_key_check",
            &["provider_key", "[A-Za-z0-9][A-Za-z0-9_-]"][..],
        ),
        (
            "oidc_providers",
            "oidc_providers_issuer_hash_check",
            &["issuer_hash", "^[0-9a-f]{64}$"][..],
        ),
        (
            "oidc_providers",
            "oidc_providers_client_id_hash_check",
            &["client_id_hash", "^[0-9a-f]{64}$"][..],
        ),
        (
            "oidc_providers",
            "oidc_providers_client_secret_ref_check",
            &["client_secret_ref", "env|vault|secret-manager"][..],
        ),
        (
            "external_identities",
            "external_identities_subject_hash_check",
            &["subject_hash", "^[0-9a-f]{64}$"][..],
        ),
        (
            "refresh_token_families",
            "refresh_token_families_current_token_version_check",
            &["current_token_version", ">= 0"][..],
        ),
        (
            "refresh_token_families",
            "refresh_token_families_lifecycle_check",
            &[
                "updated_at >= issued_at",
                "expires_at > issued_at",
                "revoked_at >= issued_at",
                "reuse_detected_at >= issued_at",
            ][..],
        ),
        (
            "refresh_tokens",
            "refresh_tokens_token_hash_check",
            &["token_hash", "^[0-9a-f]{64}$"][..],
        ),
        (
            "refresh_tokens",
            "refresh_tokens_token_version_check",
            &["token_version", "> 0"][..],
        ),
        (
            "refresh_tokens",
            "refresh_tokens_rotation_shape_check",
            &[
                "rotated_at IS NULL",
                "rotated_to_token_id IS NULL",
                "rotated_at IS NOT NULL",
                "rotated_to_token_id IS NOT NULL",
                "rotated_to_token_id <> id",
            ][..],
        ),
        (
            "refresh_tokens",
            "refresh_tokens_lifecycle_check",
            &[
                "expires_at > issued_at",
                "rotated_at >= issued_at",
                "revoked_at >= issued_at",
                "reuse_denied_at >= issued_at",
            ][..],
        ),
    ] {
        require_constraint_fragments(client, table, constraint, required_fragments).await?;
    }

    require_saml_constraints_are_strict(client).await?;
    require_scim_constraints_are_strict(client).await?;

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

    require_primary_key(client, "organizations", &["id"]).await?;
    require_primary_key(client, "org_memberships", &["org_id", "principal_uid"]).await?;
    require_foreign_key(
        client,
        "org_memberships",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_primary_key(client, "org_service_accounts", &["id"]).await?;
    require_foreign_key(
        client,
        "org_service_accounts",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_unique_key(client, "org_service_accounts", &["org_id", "name"]).await?;
    require_unique_key(client, "org_service_accounts", &["org_id", "principal_uid"]).await?;
    require_unique_key(client, "durable_principals", &["org_id", "repo_id", "uid"]).await?;
    require_primary_key(client, "oidc_providers", &["id"]).await?;
    require_foreign_key(
        client,
        "oidc_providers",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_unique_key(client, "oidc_providers", &["org_id", "provider_key"]).await?;
    require_unique_key(client, "oidc_providers", &["id", "org_id"]).await?;
    require_primary_key(client, "external_identities", &["id"]).await?;
    require_foreign_key(
        client,
        "external_identities",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "external_identities",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "external_identities",
        &["provider_id", "org_id"],
        "oidc_providers",
        &["id", "org_id"],
    )
    .await?;
    require_foreign_key(
        client,
        "external_identities",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
    )
    .await?;
    require_unique_key(
        client,
        "external_identities",
        &["provider_id", "subject_hash"],
    )
    .await?;
    require_unique_key(
        client,
        "external_identities",
        &["id", "org_id", "repo_id", "principal_uid"],
    )
    .await?;
    require_primary_key(client, "saml_providers", &["id"]).await?;
    require_foreign_key(
        client,
        "saml_providers",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_providers",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_unique_key(
        client,
        "saml_providers",
        &["org_id", "repo_id", "provider_key"],
    )
    .await?;
    require_unique_key(client, "saml_providers", &["id", "org_id", "repo_id"]).await?;
    require_index_shape(
        client,
        "saml_providers_org_repo_key_idx",
        "saml_providers",
        true,
        &["org_id", "repo_id", "provider_key"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "saml_providers_enabled_idx",
        "saml_providers",
        false,
        &["org_id", "repo_id", "enabled", "provider_key"],
        &[],
    )
    .await?;
    require_primary_key(client, "saml_external_identities", &["id"]).await?;
    require_foreign_key(
        client,
        "saml_external_identities",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_external_identities",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_external_identities",
        &["provider_id", "org_id", "repo_id"],
        "saml_providers",
        &["id", "org_id", "repo_id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_external_identities",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
    )
    .await?;
    require_unique_key(
        client,
        "saml_external_identities",
        &["provider_id", "name_id_hash"],
    )
    .await?;
    require_unique_key(
        client,
        "saml_external_identities",
        &["id", "org_id", "repo_id", "principal_uid"],
    )
    .await?;
    require_index_shape(
        client,
        "saml_external_identities_provider_name_id_idx",
        "saml_external_identities",
        true,
        &["provider_id", "name_id_hash"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "saml_external_identities_principal_idx",
        "saml_external_identities",
        false,
        &["org_id", "repo_id", "principal_uid"],
        &["disabled_at IS NULL"],
    )
    .await?;
    require_primary_key(client, "saml_group_mappings", &["id"]).await?;
    require_foreign_key(
        client,
        "saml_group_mappings",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_group_mappings",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_group_mappings",
        &["provider_id", "org_id", "repo_id"],
        "saml_providers",
        &["id", "org_id", "repo_id"],
    )
    .await?;
    require_unique_key(
        client,
        "saml_group_mappings",
        &["provider_id", "external_group_hash"],
    )
    .await?;
    require_unique_key(
        client,
        "saml_group_mappings",
        &["provider_id", "local_gid", "external_group_hash"],
    )
    .await?;
    require_index_shape(
        client,
        "saml_group_mappings_provider_group_idx",
        "saml_group_mappings",
        true,
        &["provider_id", "external_group_hash"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "saml_group_mappings_local_gid_idx",
        "saml_group_mappings",
        false,
        &["org_id", "repo_id", "local_gid"],
        &["active", "disabled_at IS NULL"],
    )
    .await?;
    require_primary_key(client, "saml_assertion_replay", &["id"]).await?;
    require_foreign_key(
        client,
        "saml_assertion_replay",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_assertion_replay",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_assertion_replay",
        &["provider_id", "org_id", "repo_id"],
        "saml_providers",
        &["id", "org_id", "repo_id"],
    )
    .await?;
    require_foreign_key(
        client,
        "saml_assertion_replay",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
    )
    .await?;
    require_unique_key(
        client,
        "saml_assertion_replay",
        &["provider_id", "assertion_id_hash"],
    )
    .await?;
    require_unique_key(client, "saml_assertion_replay", &["replay_key"]).await?;
    require_index_shape(
        client,
        "saml_assertion_replay_key_idx",
        "saml_assertion_replay",
        true,
        &["replay_key"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "saml_assertion_replay_provider_assertion_idx",
        "saml_assertion_replay",
        true,
        &["provider_id", "assertion_id_hash"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "saml_assertion_replay_expiry_idx",
        "saml_assertion_replay",
        false,
        &["expires_at"],
        &[],
    )
    .await?;
    require_primary_key(client, "scim_clients", &["id"]).await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_clients",
        &["org_id"],
        "organizations",
        &["id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_clients",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_unique_key(
        client,
        "scim_clients",
        &["org_id", "repo_id", "provider_key"],
    )
    .await?;
    require_unique_key(client, "scim_clients", &["id", "org_id", "repo_id"]).await?;
    require_index_shape(
        client,
        "scim_clients_org_repo_key_idx",
        "scim_clients",
        true,
        &["org_id", "repo_id", "provider_key"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "scim_clients_enabled_idx",
        "scim_clients",
        false,
        &["org_id", "repo_id", "enabled", "provider_key"],
        &[],
    )
    .await?;
    require_primary_key(client, "scim_users", &["id"]).await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_users",
        &["org_id"],
        "organizations",
        &["id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_users",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_users",
        &["client_id", "org_id", "repo_id"],
        "scim_clients",
        &["id", "org_id", "repo_id"],
        ForeignKeyDeleteRule::Restrict,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_users",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
        ForeignKeyDeleteRule::Restrict,
    )
    .await?;
    require_unique_key(client, "scim_users", &["client_id", "external_id_hash"]).await?;
    require_unique_key(
        client,
        "scim_users",
        &["id", "org_id", "repo_id", "principal_uid"],
    )
    .await?;
    require_index_shape(
        client,
        "scim_users_client_external_id_idx",
        "scim_users",
        true,
        &["client_id", "external_id_hash"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "scim_users_principal_idx",
        "scim_users",
        false,
        &["org_id", "repo_id", "principal_uid"],
        &["active", "disabled_at IS NULL"],
    )
    .await?;
    require_primary_key(client, "scim_groups", &["id"]).await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_groups",
        &["org_id"],
        "organizations",
        &["id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_groups",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_groups",
        &["client_id", "org_id", "repo_id"],
        "scim_clients",
        &["id", "org_id", "repo_id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_unique_key(client, "scim_groups", &["client_id", "external_id_hash"]).await?;
    require_unique_key(client, "scim_groups", &["id", "org_id", "repo_id"]).await?;
    require_index_shape(
        client,
        "scim_groups_client_external_id_idx",
        "scim_groups",
        true,
        &["client_id", "external_id_hash"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "scim_groups_local_gid_idx",
        "scim_groups",
        false,
        &["org_id", "repo_id", "local_gid"],
        &["active", "disabled_at IS NULL"],
    )
    .await?;
    require_primary_key(client, "scim_group_members", &["id"]).await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_group_members",
        &["org_id"],
        "organizations",
        &["id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_group_members",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_group_members",
        &["group_id", "org_id", "repo_id"],
        "scim_groups",
        &["id", "org_id", "repo_id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "scim_group_members",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
        ForeignKeyDeleteRule::Restrict,
    )
    .await?;
    require_unique_key(client, "scim_group_members", &["group_id", "principal_uid"]).await?;
    require_index_shape(
        client,
        "scim_group_members_group_principal_idx",
        "scim_group_members",
        true,
        &["group_id", "principal_uid"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "scim_group_members_principal_idx",
        "scim_group_members",
        false,
        &["org_id", "repo_id", "principal_uid"],
        &["active", "disabled_at IS NULL"],
    )
    .await?;
    require_primary_key(client, "refresh_token_families", &["id"]).await?;
    require_foreign_key(
        client,
        "refresh_token_families",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_token_families",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_token_families",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_token_families",
        &["external_identity_id", "org_id", "repo_id", "principal_uid"],
        "external_identities",
        &["id", "org_id", "repo_id", "principal_uid"],
    )
    .await?;
    require_unique_key(
        client,
        "refresh_token_families",
        &["id", "org_id", "repo_id", "principal_uid"],
    )
    .await?;
    require_index_shape(
        client,
        "refresh_token_families_active_principal_idx",
        "refresh_token_families",
        true,
        &["org_id", "repo_id", "principal_uid", "external_identity_id"],
        &["revoked_at IS NULL", "reuse_detected_at IS NULL"],
    )
    .await?;
    require_primary_key(client, "refresh_tokens", &["id"]).await?;
    require_foreign_key(
        client,
        "refresh_tokens",
        &["family_id", "org_id", "repo_id", "principal_uid"],
        "refresh_token_families",
        &["id", "org_id", "repo_id", "principal_uid"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_tokens",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_tokens",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_tokens",
        &["org_id", "repo_id", "principal_uid"],
        "durable_principals",
        &["org_id", "repo_id", "uid"],
    )
    .await?;
    require_foreign_key(
        client,
        "refresh_tokens",
        &["family_id", "rotated_to_token_id"],
        "refresh_tokens",
        &["family_id", "id"],
    )
    .await?;
    require_unique_key(client, "refresh_tokens", &["family_id", "token_version"]).await?;
    require_unique_key(client, "refresh_tokens", &["family_id", "id"]).await?;
    require_unique_key(client, "refresh_tokens", &["token_hash"]).await?;
    require_index_shape(
        client,
        "refresh_tokens_family_active_idx",
        "refresh_tokens",
        true,
        &["family_id"],
        &[
            "rotated_at IS NULL",
            "revoked_at IS NULL",
            "reuse_denied_at IS NULL",
        ],
    )
    .await?;
    require_index_shape(
        client,
        "refresh_tokens_principal_active_idx",
        "refresh_tokens",
        false,
        &["org_id", "repo_id", "principal_uid", "expires_at"],
        &[
            "rotated_at IS NULL",
            "revoked_at IS NULL",
            "reuse_denied_at IS NULL",
        ],
    )
    .await?;
    require_foreign_key(client, "repos", &["org_id"], "organizations", &["id"]).await?;
    require_unique_key(client, "repos", &["org_id", "id"]).await?;
    require_no_rows(client, "repos", "org_id IS NULL").await?;
    require_foreign_key(client, "workspaces", &["org_id"], "organizations", &["id"]).await?;
    require_foreign_key(
        client,
        "workspaces",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
    require_unique_key(client, "workspaces", &["id", "org_id"]).await?;
    require_foreign_key(
        client,
        "workspace_tokens",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "workspace_tokens",
        &["workspace_id", "org_id"],
        "workspaces",
        &["id", "org_id"],
    )
    .await?;
    require_primary_key(client, "durable_principals", &["uid"]).await?;
    require_foreign_key(
        client,
        "durable_principals",
        &["org_id"],
        "organizations",
        &["id"],
    )
    .await?;
    require_foreign_key(
        client,
        "durable_principals",
        &["org_id", "repo_id"],
        "repos",
        &["org_id", "id"],
    )
    .await?;
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
    require_org_tenant_backfill_consistency(client).await?;

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

    for table in ["search_index_state", "search_index_files"] {
        require_table(client, table).await?;
    }

    for (table, column) in [
        ("search_index_state", "repo_id"),
        ("search_index_state", "commit_id"),
        ("search_index_state", "root_tree_id"),
        ("search_index_state", "status"),
        ("search_index_state", "indexed_file_count"),
        ("search_index_state", "indexed_byte_count"),
        ("search_index_state", "failure_code"),
        ("search_index_state", "started_at"),
        ("search_index_state", "completed_at"),
        ("search_index_state", "updated_at"),
        ("search_index_files", "repo_id"),
        ("search_index_files", "commit_id"),
        ("search_index_files", "root_tree_id"),
        ("search_index_files", "path"),
        ("search_index_files", "object_id"),
        ("search_index_files", "byte_len"),
        ("search_index_files", "content_preview"),
        ("search_index_files", "search_vector"),
        ("search_index_files", "updated_at"),
    ] {
        require_column(client, table, column).await?;
    }

    require_primary_key(
        client,
        "search_index_state",
        &["repo_id", "commit_id", "root_tree_id"],
    )
    .await?;
    require_primary_key(
        client,
        "search_index_files",
        &["repo_id", "commit_id", "root_tree_id", "path"],
    )
    .await?;
    require_foreign_key(
        client,
        "search_index_state",
        &["repo_id", "commit_id"],
        "commits",
        &["repo_id", "id"],
    )
    .await?;
    require_foreign_key(
        client,
        "search_index_files",
        &["repo_id", "commit_id", "root_tree_id"],
        "search_index_state",
        &["repo_id", "commit_id", "root_tree_id"],
    )
    .await?;

    require_index_shape(
        client,
        "search_index_files_state_lookup_idx",
        "search_index_files",
        false,
        &["repo_id", "commit_id", "root_tree_id"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "search_index_files_path_lookup_idx",
        "search_index_files",
        false,
        &["repo_id", "commit_id", "root_tree_id", "path"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "search_index_files_vector_idx",
        "search_index_files",
        false,
        &["search_vector"],
        &[],
    )
    .await?;

    require_column_shape(client, "search_index_state", "repo_id", "text", false, &[]).await?;
    require_column_shape(
        client,
        "search_index_state",
        "commit_id",
        "text",
        false,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "root_tree_id",
        "text",
        false,
        &[],
    )
    .await?;
    require_column_shape(client, "search_index_state", "status", "text", false, &[]).await?;
    require_column_shape(
        client,
        "search_index_state",
        "indexed_file_count",
        "integer",
        false,
        &["0"],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "indexed_byte_count",
        "bigint",
        false,
        &["0"],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "failure_code",
        "text",
        true,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "started_at",
        "timestamp with time zone",
        false,
        &["now", "clock_timestamp"],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "completed_at",
        "timestamp with time zone",
        true,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "updated_at",
        "timestamp with time zone",
        false,
        &["now", "clock_timestamp"],
    )
    .await?;

    require_column_shape(client, "search_index_files", "repo_id", "text", false, &[]).await?;
    require_column_shape(
        client,
        "search_index_files",
        "commit_id",
        "text",
        false,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "root_tree_id",
        "text",
        false,
        &[],
    )
    .await?;
    require_column_shape(client, "search_index_files", "path", "text", false, &[]).await?;
    require_column_shape(
        client,
        "search_index_files",
        "object_id",
        "text",
        false,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "byte_len",
        "integer",
        false,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "content_preview",
        "text",
        false,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "search_vector",
        "USER-DEFINED",
        false,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "updated_at",
        "timestamp with time zone",
        false,
        &["now", "clock_timestamp"],
    )
    .await?;

    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["commit_id", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["root_tree_id", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["status", "indexing", "ready", "failed"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["indexed_file_count", ">= 0"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["indexed_byte_count", ">= 0"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["failure_code", "<> ''"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &[
            "status",
            "completed_at",
            "failure_code",
            "indexing",
            "ready",
            "failed",
        ],
    )
    .await?;
    require_check_constraint_with_fragments(client, "search_index_files", &["path", "<> ''", "^/"])
        .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_files",
        &["object_id", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(client, "search_index_files", &["byte_len", ">= 0"])
        .await?;

    for (table, column) in [
        ("search_index_state", "acl_snapshot_version"),
        ("search_index_state", "acl_snapshot_status"),
        ("search_index_state", "acl_snapshot_failure_code"),
        ("search_index_files", "acl_snapshot_version"),
        ("search_index_files", "acl_snapshot_hash"),
        ("search_index_files", "acl_snapshot"),
    ] {
        require_column(client, table, column).await?;
    }

    require_column_shape(
        client,
        "search_index_state",
        "acl_snapshot_status",
        "text",
        false,
        &["missing"],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "acl_snapshot_version",
        "text",
        true,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_state",
        "acl_snapshot_failure_code",
        "text",
        true,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "acl_snapshot_version",
        "text",
        true,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "acl_snapshot_hash",
        "text",
        true,
        &[],
    )
    .await?;
    require_column_shape(
        client,
        "search_index_files",
        "acl_snapshot",
        "jsonb",
        true,
        &[],
    )
    .await?;

    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["acl_snapshot_status", "missing", "ready", "failed"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["acl_snapshot_failure_code", "<> ''"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_files",
        &["acl_snapshot_hash", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_files",
        &["acl_snapshot", "jsonb_typeof", "object"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_files",
        &["acl_snapshot_version", "acl_snapshot_hash", "acl_snapshot"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &[
            "acl_snapshot_status",
            "acl_snapshot_version",
            "acl_snapshot_failure_code",
        ],
    )
    .await?;

    require_table(client, "extracted_text_records").await?;
    require_primary_key(
        client,
        "extracted_text_records",
        &["repo_id", "commit_id", "root_tree_id", "path"],
    )
    .await?;
    require_foreign_key(
        client,
        "extracted_text_records",
        &["repo_id", "commit_id"],
        "commits",
        &["repo_id", "id"],
    )
    .await?;
    require_index_shape(
        client,
        "extracted_text_records_object_lookup_idx",
        "extracted_text_records",
        false,
        &["repo_id", "object_id"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "extracted_text_records_head_lookup_idx",
        "extracted_text_records",
        false,
        &["repo_id", "commit_id", "root_tree_id"],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "extracted_text_records_path_lookup_idx",
        "extracted_text_records",
        false,
        &["repo_id", "commit_id", "root_tree_id", "path"],
        &[],
    )
    .await?;

    for (table, column) in [
        ("extracted_text_records", "repo_id"),
        ("extracted_text_records", "commit_id"),
        ("extracted_text_records", "root_tree_id"),
        ("extracted_text_records", "path"),
        ("extracted_text_records", "object_id"),
        ("extracted_text_records", "source_byte_len"),
        ("extracted_text_records", "source_mime_type"),
        ("extracted_text_records", "extractor"),
        ("extracted_text_records", "status"),
        ("extracted_text_records", "text_hash"),
        ("extracted_text_records", "text_char_count"),
        ("extracted_text_records", "extracted_text"),
        ("extracted_text_records", "failure_code"),
        ("extracted_text_records", "updated_at"),
        ("search_index_state", "extraction_version"),
        ("search_index_state", "extraction_status"),
        ("search_index_state", "extraction_failure_code"),
        ("search_index_files", "extraction_version"),
        ("search_index_files", "extractor"),
        ("search_index_files", "extracted_text_hash"),
    ] {
        require_column(client, table, column).await?;
    }

    require_column_shape(
        client,
        "search_index_state",
        "extraction_status",
        "text",
        false,
        &["missing"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["status", "ready", "unsupported", "too_large", "failed"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["text_hash", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["object_id", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["path", "<> ''", "^/"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["source_byte_len", ">= 0"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["text_char_count", ">= 0"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &["failure_code", "<> ''"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "extracted_text_records",
        &[
            "status",
            "ready",
            "extracted_text",
            "text_hash",
            "failure_code",
        ],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &["extraction_status", "missing", "ready", "failed"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_files",
        &["extracted_text_hash", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_files",
        &["extraction_version", "extractor", "extracted_text_hash"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_state",
        &[
            "extraction_status",
            "extraction_version",
            "extraction_failure_code",
        ],
    )
    .await?;

    verify_pgvector_schema_catalog(client).await?;

    Ok(())
}

async fn verify_pgvector_schema_catalog(client: &impl GenericClient) -> Result<(), VfsError> {
    require_vector_extension(client).await?;

    require_table(client, "search_index_vector_state").await?;
    require_primary_key(
        client,
        "search_index_vector_state",
        &[
            "repo_id",
            "commit_id",
            "root_tree_id",
            "embedding_provider",
            "embedding_model",
            "embedding_dimensions",
            "chunker_version",
        ],
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "search_index_vector_state",
        &["repo_id", "commit_id", "root_tree_id"],
        "search_index_state",
        &["repo_id", "commit_id", "root_tree_id"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    for column in [
        "repo_id",
        "commit_id",
        "root_tree_id",
        "embedding_model",
        "embedding_provider",
        "embedding_dimensions",
        "chunker_version",
        "status",
        "embedded_file_count",
        "embedded_chunk_count",
        "failure_code",
        "started_at",
        "completed_at",
        "updated_at",
    ] {
        require_column(client, "search_index_vector_state", column).await?;
    }
    require_check_constraint_with_fragments(
        client,
        "search_index_vector_state",
        &["status", "indexing", "ready", "failed"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vector_state",
        &["embedding_dimensions", "> 0", "4096"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vector_state",
        &["embedding_model", "<> ''"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vector_state",
        &["embedding_provider", "<> ''"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vector_state",
        &["chunker_version", "semantic-chunk-v1"],
    )
    .await?;
    require_constraint_fragments(
        client,
        "search_index_vector_state",
        "search_index_vector_state_lifecycle_check",
        &[
            "status = 'indexing'",
            "completed_at IS NULL",
            "failure_code IS NULL",
            "status = 'ready'",
            "completed_at IS NOT NULL",
            "status = 'failed'",
            "failure_code IS NOT NULL",
        ],
    )
    .await?;

    require_table(client, "search_index_vectors").await?;
    require_primary_key(
        client,
        "search_index_vectors",
        &[
            "repo_id",
            "commit_id",
            "root_tree_id",
            "path",
            "chunk_ordinal",
            "embedding_provider",
            "embedding_model",
            "embedding_dimensions",
            "chunker_version",
        ],
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "search_index_vectors",
        &[
            "repo_id",
            "commit_id",
            "root_tree_id",
            "embedding_provider",
            "embedding_model",
            "embedding_dimensions",
            "chunker_version",
        ],
        "search_index_vector_state",
        &[
            "repo_id",
            "commit_id",
            "root_tree_id",
            "embedding_provider",
            "embedding_model",
            "embedding_dimensions",
            "chunker_version",
        ],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    require_foreign_key_with_delete_rule(
        client,
        "search_index_vectors",
        &["repo_id", "commit_id", "root_tree_id", "path"],
        "search_index_files",
        &["repo_id", "commit_id", "root_tree_id", "path"],
        ForeignKeyDeleteRule::Cascade,
    )
    .await?;
    for column in [
        "repo_id",
        "commit_id",
        "root_tree_id",
        "path",
        "chunk_ordinal",
        "object_id",
        "extracted_text_hash",
        "acl_snapshot_hash",
        "embedding_model",
        "embedding_provider",
        "embedding_dimensions",
        "chunker_version",
        "chunk_hash",
        "chunk_char_start",
        "chunk_char_count",
        "embedding",
        "updated_at",
    ] {
        require_column(client, "search_index_vectors", column).await?;
    }
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["path", "<> ''", "^/"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["object_id", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["extracted_text_hash", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["acl_snapshot_hash", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["chunk_hash", "0-9a-f", "64"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["embedding_dimensions", "> 0", "4096"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["vector_dims", "embedding", "embedding_dimensions"],
    )
    .await?;
    require_check_constraint_with_fragments(
        client,
        "search_index_vectors",
        &["chunker_version", "semantic-chunk-v1"],
    )
    .await?;
    require_index_shape(
        client,
        "search_index_vectors_head_model_identity_idx",
        "search_index_vectors",
        false,
        &[
            "repo_id",
            "commit_id",
            "root_tree_id",
            "embedding_provider",
            "embedding_model",
            "embedding_dimensions",
            "chunker_version",
        ],
        &[],
    )
    .await?;
    require_index_shape(
        client,
        "search_index_vectors_path_idx",
        "search_index_vectors",
        false,
        &["repo_id", "commit_id", "root_tree_id", "path"],
        &[],
    )
    .await?;

    require_vector_tables_have_no_raw_text(client).await?;

    Ok(())
}

async fn require_vector_extension(client: &impl GenericClient) -> Result<(), VfsError> {
    let present: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_extension e
                JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
                WHERE e.extname = 'vector'
                  AND n.nspname = 'public'
                  AND to_regtype('public.vector') IS NOT NULL
            )",
            &[],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?
        .get(0);
    if present {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_vector_tables_have_no_raw_text(
    client: &impl GenericClient,
) -> Result<(), VfsError> {
    for table in ["search_index_vector_state", "search_index_vectors"] {
        let rows = client
            .query(
                "SELECT lower(column_name)
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = $1",
                &[&table],
            )
            .await
            .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
        for row in rows {
            let column: String = row.get(0);
            if column == "extracted_text"
                || column == "chunk_text"
                || column == "text"
                || column.contains("raw_text")
                || column.contains("preview")
            {
                return Err(adoption_verification_error());
            }
        }
    }
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

async fn require_saml_column_shapes(client: &impl GenericClient) -> Result<(), VfsError> {
    for (table, column, data_type) in [
        ("saml_providers", "id", "uuid"),
        ("saml_providers", "org_id", "text"),
        ("saml_providers", "repo_id", "text"),
        ("saml_providers", "provider_key", "text"),
        ("saml_providers", "idp_entity_id_hash", "text"),
        ("saml_providers", "sp_entity_id_hash", "text"),
        ("saml_providers", "acs_url_hash", "text"),
        ("saml_providers", "audience_hash", "text"),
        ("saml_providers", "signing_cert_ref_hash", "text"),
        ("saml_providers", "display_name", "text"),
        ("saml_providers", "binding", "text"),
        ("saml_providers", "enabled", "boolean"),
        ("saml_providers", "created_at", "timestamp with time zone"),
        ("saml_providers", "updated_at", "timestamp with time zone"),
        ("saml_external_identities", "id", "uuid"),
        ("saml_external_identities", "org_id", "text"),
        ("saml_external_identities", "repo_id", "text"),
        ("saml_external_identities", "provider_id", "uuid"),
        ("saml_external_identities", "principal_uid", "integer"),
        ("saml_external_identities", "name_id_hash", "text"),
        (
            "saml_external_identities",
            "created_at",
            "timestamp with time zone",
        ),
        (
            "saml_external_identities",
            "updated_at",
            "timestamp with time zone",
        ),
        ("saml_group_mappings", "id", "uuid"),
        ("saml_group_mappings", "org_id", "text"),
        ("saml_group_mappings", "repo_id", "text"),
        ("saml_group_mappings", "provider_id", "uuid"),
        ("saml_group_mappings", "external_group_hash", "text"),
        ("saml_group_mappings", "local_gid", "integer"),
        ("saml_group_mappings", "required", "boolean"),
        ("saml_group_mappings", "active", "boolean"),
        (
            "saml_group_mappings",
            "created_at",
            "timestamp with time zone",
        ),
        (
            "saml_group_mappings",
            "updated_at",
            "timestamp with time zone",
        ),
        ("saml_assertion_replay", "id", "uuid"),
        ("saml_assertion_replay", "org_id", "text"),
        ("saml_assertion_replay", "repo_id", "text"),
        ("saml_assertion_replay", "provider_id", "uuid"),
        ("saml_assertion_replay", "principal_uid", "integer"),
        ("saml_assertion_replay", "assertion_id_hash", "text"),
        ("saml_assertion_replay", "replay_key", "text"),
        (
            "saml_assertion_replay",
            "first_seen_at",
            "timestamp with time zone",
        ),
        (
            "saml_assertion_replay",
            "expires_at",
            "timestamp with time zone",
        ),
    ] {
        require_column_shape(client, table, column, data_type, false, &[]).await?;
    }

    for (table, column, data_type) in [
        ("saml_providers", "group_attribute", "text"),
        ("saml_providers", "required_group_hash", "text"),
        ("saml_providers", "disabled_at", "timestamp with time zone"),
        ("saml_external_identities", "username_hint", "text"),
        (
            "saml_external_identities",
            "disabled_at",
            "timestamp with time zone",
        ),
        ("saml_group_mappings", "external_group_name_ref", "text"),
        (
            "saml_group_mappings",
            "disabled_at",
            "timestamp with time zone",
        ),
    ] {
        require_column_shape(client, table, column, data_type, true, &[]).await?;
    }

    require_column_shape(
        client,
        "saml_providers",
        "binding",
        "text",
        false,
        &["post"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_providers",
        "enabled",
        "boolean",
        false,
        &["false"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_group_mappings",
        "required",
        "boolean",
        false,
        &["false"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_group_mappings",
        "active",
        "boolean",
        false,
        &["true"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_providers",
        "created_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_providers",
        "updated_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_external_identities",
        "created_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_external_identities",
        "updated_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_group_mappings",
        "created_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_group_mappings",
        "updated_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;
    require_column_shape(
        client,
        "saml_assertion_replay",
        "first_seen_at",
        "timestamp with time zone",
        false,
        &["now()"],
    )
    .await?;

    require_column_default_expr(
        client,
        "saml_providers",
        "binding",
        &["'post'::text", "'post'"],
    )
    .await?;
    require_column_default_expr(client, "saml_providers", "enabled", &["false"]).await?;
    require_column_default_expr(client, "saml_providers", "created_at", &["now()"]).await?;
    require_column_default_expr(client, "saml_providers", "updated_at", &["now()"]).await?;
    require_column_default_expr(client, "saml_external_identities", "created_at", &["now()"])
        .await?;
    require_column_default_expr(client, "saml_external_identities", "updated_at", &["now()"])
        .await?;
    require_column_default_expr(client, "saml_group_mappings", "required", &["false"]).await?;
    require_column_default_expr(client, "saml_group_mappings", "active", &["true"]).await?;
    require_column_default_expr(client, "saml_group_mappings", "created_at", &["now()"]).await?;
    require_column_default_expr(client, "saml_group_mappings", "updated_at", &["now()"]).await?;
    require_column_default_expr(client, "saml_assertion_replay", "first_seen_at", &["now()"]).await
}

async fn require_scim_column_shapes(client: &impl GenericClient) -> Result<(), VfsError> {
    for (table, column, data_type) in [
        ("scim_clients", "id", "uuid"),
        ("scim_clients", "org_id", "text"),
        ("scim_clients", "repo_id", "text"),
        ("scim_clients", "provider_key", "text"),
        ("scim_clients", "token_hash", "text"),
        ("scim_clients", "enabled", "boolean"),
        ("scim_clients", "created_at", "timestamp with time zone"),
        ("scim_clients", "updated_at", "timestamp with time zone"),
        ("scim_users", "id", "uuid"),
        ("scim_users", "org_id", "text"),
        ("scim_users", "repo_id", "text"),
        ("scim_users", "client_id", "uuid"),
        ("scim_users", "principal_uid", "integer"),
        ("scim_users", "external_id_hash", "text"),
        ("scim_users", "active", "boolean"),
        ("scim_users", "created_at", "timestamp with time zone"),
        ("scim_users", "updated_at", "timestamp with time zone"),
        ("scim_groups", "id", "uuid"),
        ("scim_groups", "org_id", "text"),
        ("scim_groups", "repo_id", "text"),
        ("scim_groups", "client_id", "uuid"),
        ("scim_groups", "local_gid", "integer"),
        ("scim_groups", "external_id_hash", "text"),
        ("scim_groups", "active", "boolean"),
        ("scim_groups", "created_at", "timestamp with time zone"),
        ("scim_groups", "updated_at", "timestamp with time zone"),
        ("scim_group_members", "id", "uuid"),
        ("scim_group_members", "org_id", "text"),
        ("scim_group_members", "repo_id", "text"),
        ("scim_group_members", "group_id", "uuid"),
        ("scim_group_members", "principal_uid", "integer"),
        ("scim_group_members", "active", "boolean"),
        (
            "scim_group_members",
            "created_at",
            "timestamp with time zone",
        ),
        (
            "scim_group_members",
            "updated_at",
            "timestamp with time zone",
        ),
    ] {
        require_column_shape(client, table, column, data_type, false, &[]).await?;
    }

    for (table, column, data_type) in [
        ("scim_clients", "disabled_at", "timestamp with time zone"),
        ("scim_users", "username_hint", "text"),
        ("scim_users", "disabled_at", "timestamp with time zone"),
        ("scim_groups", "display_name", "text"),
        ("scim_groups", "disabled_at", "timestamp with time zone"),
        (
            "scim_group_members",
            "disabled_at",
            "timestamp with time zone",
        ),
    ] {
        require_column_shape(client, table, column, data_type, true, &[]).await?;
    }

    for (table, column, data_type, default_fragments) in [
        ("scim_clients", "id", "uuid", &["gen_random_uuid()"][..]),
        ("scim_clients", "enabled", "boolean", &["false"][..]),
        (
            "scim_clients",
            "created_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        (
            "scim_clients",
            "updated_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        ("scim_users", "id", "uuid", &["gen_random_uuid()"][..]),
        ("scim_users", "active", "boolean", &["true"][..]),
        (
            "scim_users",
            "created_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        (
            "scim_users",
            "updated_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        ("scim_groups", "id", "uuid", &["gen_random_uuid()"][..]),
        ("scim_groups", "active", "boolean", &["true"][..]),
        (
            "scim_groups",
            "created_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        (
            "scim_groups",
            "updated_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        (
            "scim_group_members",
            "id",
            "uuid",
            &["gen_random_uuid()"][..],
        ),
        ("scim_group_members", "active", "boolean", &["true"][..]),
        (
            "scim_group_members",
            "created_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
        (
            "scim_group_members",
            "updated_at",
            "timestamp with time zone",
            &["now()"][..],
        ),
    ] {
        require_column_shape(client, table, column, data_type, false, default_fragments).await?;
        require_column_default_expr(client, table, column, default_fragments).await?;
    }

    Ok(())
}

async fn require_scim_no_raw_material_columns(client: &impl GenericClient) -> Result<(), VfsError> {
    for table in [
        "scim_clients",
        "scim_users",
        "scim_groups",
        "scim_group_members",
    ] {
        let rows = client
            .query(
                "SELECT lower(column_name)
                 FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = $1",
                &[&table],
            )
            .await
            .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
        for row in rows {
            let column: String = row.get(0);
            if is_scim_raw_material_column(&column) {
                return Err(adoption_verification_error());
            }
        }
    }
    Ok(())
}

fn is_scim_raw_material_column(column: &str) -> bool {
    if matches!(column, "token_hash" | "external_id_hash") {
        return false;
    }
    column == "token"
        || column.contains("token")
        || column.contains("secret")
        || column.starts_with("raw_")
        || column.ends_with("_raw")
        || column.contains("_raw_")
        || column.contains("request_body")
        || column.contains("response_body")
        || column.contains("body")
        || column.contains("provider_url")
        || column.contains("url")
        || (column.contains("external") && column.contains("id"))
}

async fn require_column_shape(
    client: &impl GenericClient,
    table: &str,
    column: &str,
    data_type: &str,
    nullable: bool,
    default_fragments: &[&str],
) -> Result<(), VfsError> {
    let row = client
        .query_opt(
            "SELECT data_type, is_nullable, column_default
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1
               AND column_name = $2",
            &[&table, &column],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    let Some(row) = row else {
        return Err(adoption_verification_error());
    };
    let actual_data_type: String = row.get(0);
    let is_nullable: String = row.get(1);
    let column_default: Option<String> = row.get(2);
    if actual_data_type != data_type {
        return Err(adoption_verification_error());
    }
    if (is_nullable == "YES") != nullable {
        return Err(adoption_verification_error());
    }
    if !default_fragments.is_empty() {
        let Some(column_default) = column_default else {
            return Err(adoption_verification_error());
        };
        if !default_fragments
            .iter()
            .all(|fragment| column_default.contains(fragment))
        {
            return Err(adoption_verification_error());
        }
    }
    Ok(())
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

async fn require_index_shape(
    client: &impl GenericClient,
    index: &str,
    table: &str,
    unique: bool,
    columns: &[&str],
    predicate_fragments: &[&str],
) -> Result<(), VfsError> {
    let row = client
        .query_opt(
            "SELECT i.indisunique,
                    COALESCE(pg_catalog.pg_get_expr(i.indpred, i.indrelid), ''),
                    COALESCE((
                        SELECT array_agg(a.attname::text ORDER BY key.ordinality)
                        FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality)
                        JOIN pg_catalog.pg_attribute a
                          ON a.attrelid = i.indrelid
                         AND a.attnum = key.attnum
                    ), ARRAY[]::text[])
             FROM pg_catalog.pg_class idx
             JOIN pg_catalog.pg_index i ON i.indexrelid = idx.oid
             JOIN pg_catalog.pg_class tbl ON tbl.oid = i.indrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = tbl.relnamespace
             WHERE n.nspname = current_schema()
               AND idx.relname = $1
               AND tbl.relname = $2
               AND idx.relkind = 'i'",
            &[&index, &table],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    let Some(row) = row else {
        return Err(adoption_verification_error());
    };
    let actual_unique: bool = row.get(0);
    let predicate: String = row.get(1);
    let actual_columns: Vec<String> = row.get(2);
    let expected_columns = columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    if actual_unique == unique
        && actual_columns == expected_columns
        && predicate_fragments
            .iter()
            .all(|fragment| predicate.contains(fragment))
    {
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
    match required_fragment {
        Some(fragment) => {
            require_constraint_fragments(client, table, constraint, &[fragment]).await
        }
        None => require_constraint_fragments(client, table, constraint, &[]).await,
    }
}

async fn require_constraint_fragments(
    client: &impl GenericClient,
    table: &str,
    constraint: &str,
    required_fragments: &[&str],
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
    let definition: String = row.get(0);
    if required_fragments
        .iter()
        .all(|fragment| definition.contains(fragment))
    {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_saml_constraints_are_strict(client: &impl GenericClient) -> Result<(), VfsError> {
    for (table, constraint, expected_exprs) in [
        (
            "saml_providers",
            "saml_providers_provider_key_check",
            &[
                "(provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'::text)",
                "(provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_display_name_check",
            &[
                "((btrim(display_name) <> ''::text) AND (length(display_name) <= 255))",
                "((btrim(display_name) <> '') AND (length(display_name) <= 255))",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_idp_entity_id_hash_check",
            &[
                "(idp_entity_id_hash ~ '^[0-9a-f]{64}$'::text)",
                "(idp_entity_id_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_sp_entity_id_hash_check",
            &[
                "(sp_entity_id_hash ~ '^[0-9a-f]{64}$'::text)",
                "(sp_entity_id_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_acs_url_hash_check",
            &[
                "(acs_url_hash ~ '^[0-9a-f]{64}$'::text)",
                "(acs_url_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_audience_hash_check",
            &[
                "(audience_hash ~ '^[0-9a-f]{64}$'::text)",
                "(audience_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_signing_cert_ref_hash_check",
            &[
                "(signing_cert_ref_hash ~ '^[0-9a-f]{64}$'::text)",
                "(signing_cert_ref_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_binding_check",
            &["(binding = 'post'::text)", "(binding = 'post')"][..],
        ),
        (
            "saml_providers",
            "saml_providers_required_group_hash_check",
            &[
                "((required_group_hash IS NULL) OR (required_group_hash ~ '^[0-9a-f]{64}$'::text))",
                "((required_group_hash IS NULL) OR (required_group_hash ~ '^[0-9a-f]{64}$'))",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_group_attribute_check",
            &[
                "((group_attribute IS NULL) OR ((btrim(group_attribute) <> ''::text) AND (length(group_attribute) <= 128)))",
                "((group_attribute IS NULL) OR ((btrim(group_attribute) <> '') AND (length(group_attribute) <= 128)))",
            ][..],
        ),
        (
            "saml_providers",
            "saml_providers_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "saml_providers",
            "saml_providers_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "saml_providers",
            "saml_providers_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "saml_providers",
            "saml_providers_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
        (
            "saml_external_identities",
            "saml_external_identities_name_id_hash_check",
            &[
                "(name_id_hash ~ '^[0-9a-f]{64}$'::text)",
                "(name_id_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_external_identities",
            "saml_external_identities_username_hint_check",
            &["((username_hint IS NULL) OR (length(username_hint) <= 128))"][..],
        ),
        (
            "saml_external_identities",
            "saml_external_identities_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "saml_external_identities",
            "saml_external_identities_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "saml_external_identities",
            "saml_external_identities_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "saml_external_identities",
            "saml_external_identities_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_external_group_hash_check",
            &[
                "(external_group_hash ~ '^[0-9a-f]{64}$'::text)",
                "(external_group_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_external_group_name_ref_check",
            &[
                "((external_group_name_ref IS NULL) OR (external_group_name_ref ~ '^(env|vault|secret-manager)://[A-Za-z0-9._~:/@+=,-]{1,480}$'::text))",
                "((external_group_name_ref IS NULL) OR (external_group_name_ref ~ '^(env|vault|secret-manager)://[A-Za-z0-9._~:/@+=,-]{1,480}$'))",
            ][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_local_gid_check",
            &["(local_gid >= 0)"][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "saml_group_mappings",
            "saml_group_mappings_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_assertion_id_hash_check",
            &[
                "(assertion_id_hash ~ '^[0-9a-f]{64}$'::text)",
                "(assertion_id_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_replay_key_check",
            &[
                "(replay_key ~ '^[0-9a-f]{64}$'::text)",
                "(replay_key ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_first_seen_at_finite_check",
            &["isfinite(first_seen_at)"][..],
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_expires_at_finite_check",
            &["isfinite(expires_at)"][..],
        ),
        (
            "saml_assertion_replay",
            "saml_assertion_replay_lifecycle_check",
            &["(expires_at > first_seen_at)"][..],
        ),
    ] {
        require_constraint_expr(client, table, constraint, expected_exprs).await?;
    }
    Ok(())
}

async fn require_scim_constraints_are_strict(client: &impl GenericClient) -> Result<(), VfsError> {
    for (table, constraint, expected_exprs) in [
        (
            "scim_clients",
            "scim_clients_provider_key_check",
            &[
                "(provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'::text)",
                "(provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')",
            ][..],
        ),
        (
            "scim_clients",
            "scim_clients_token_hash_check",
            &[
                "(token_hash ~ '^[0-9a-f]{64}$'::text)",
                "(token_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "scim_clients",
            "scim_clients_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "scim_clients",
            "scim_clients_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "scim_clients",
            "scim_clients_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "scim_clients",
            "scim_clients_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
        (
            "scim_users",
            "scim_users_external_id_hash_check",
            &[
                "(external_id_hash ~ '^[0-9a-f]{64}$'::text)",
                "(external_id_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "scim_users",
            "scim_users_username_hint_check",
            &["((username_hint IS NULL) OR (length(username_hint) <= 128))"][..],
        ),
        (
            "scim_users",
            "scim_users_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "scim_users",
            "scim_users_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "scim_users",
            "scim_users_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "scim_users",
            "scim_users_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
        (
            "scim_groups",
            "scim_groups_local_gid_check",
            &["(local_gid >= 0)"][..],
        ),
        (
            "scim_groups",
            "scim_groups_external_id_hash_check",
            &[
                "(external_id_hash ~ '^[0-9a-f]{64}$'::text)",
                "(external_id_hash ~ '^[0-9a-f]{64}$')",
            ][..],
        ),
        (
            "scim_groups",
            "scim_groups_display_name_check",
            &[
                "((display_name IS NULL) OR ((btrim(display_name) <> ''::text) AND (length(display_name) <= 255)))",
                "((display_name IS NULL) OR ((btrim(display_name) <> '') AND (length(display_name) <= 255)))",
            ][..],
        ),
        (
            "scim_groups",
            "scim_groups_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "scim_groups",
            "scim_groups_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "scim_groups",
            "scim_groups_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "scim_groups",
            "scim_groups_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
        (
            "scim_group_members",
            "scim_group_members_created_at_finite_check",
            &["isfinite(created_at)"][..],
        ),
        (
            "scim_group_members",
            "scim_group_members_updated_at_finite_check",
            &["isfinite(updated_at)"][..],
        ),
        (
            "scim_group_members",
            "scim_group_members_disabled_at_finite_check",
            &["((disabled_at IS NULL) OR isfinite(disabled_at))"][..],
        ),
        (
            "scim_group_members",
            "scim_group_members_lifecycle_check",
            &[
                "((updated_at >= created_at) AND ((disabled_at IS NULL) OR (disabled_at >= created_at)))",
            ][..],
        ),
    ] {
        require_constraint_expr(client, table, constraint, expected_exprs).await?;
    }
    Ok(())
}

async fn require_constraint_expr(
    client: &impl GenericClient,
    table: &str,
    constraint: &str,
    expected_exprs: &[&str],
) -> Result<(), VfsError> {
    let row = client
        .query_opt(
            "SELECT pg_catalog.pg_get_expr(c.conbin, c.conrelid), c.convalidated
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
    let expression: String = row.get(0);
    if catalog_expression_matches(&expression, expected_exprs) {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

async fn require_column_default_expr(
    client: &impl GenericClient,
    table: &str,
    column: &str,
    expected_exprs: &[&str],
) -> Result<(), VfsError> {
    let row = client
        .query_opt(
            "SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
             FROM pg_catalog.pg_attrdef d
             JOIN pg_catalog.pg_class r ON r.oid = d.adrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
             JOIN pg_catalog.pg_attribute a
               ON a.attrelid = r.oid
              AND a.attnum = d.adnum
             WHERE n.nspname = current_schema()
               AND r.relname = $1
               AND a.attname = $2",
            &[&table, &column],
        )
        .await
        .map_err(|error| postgres_error("verify migration adoption catalog", error))?;
    let Some(row) = row else {
        return Err(adoption_verification_error());
    };
    let expression: String = row.get(0);
    if catalog_expression_matches(&expression, expected_exprs) {
        Ok(())
    } else {
        Err(adoption_verification_error())
    }
}

fn catalog_expression_matches(actual: &str, expected_exprs: &[&str]) -> bool {
    let actual = normalize_catalog_expression(actual);
    expected_exprs
        .iter()
        .any(|expected| actual == normalize_catalog_expression(expected))
}

fn normalize_catalog_expression(expression: &str) -> String {
    expression.split_whitespace().collect::<Vec<_>>().join(" ")
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

#[derive(Clone, Copy)]
enum ForeignKeyDeleteRule {
    Restrict,
    Cascade,
}

impl ForeignKeyDeleteRule {
    fn pg_code(self) -> &'static str {
        match self {
            Self::Restrict => "r",
            Self::Cascade => "c",
        }
    }
}

async fn require_foreign_key_with_delete_rule(
    client: &impl GenericClient,
    table: &str,
    columns: &[&str],
    referenced_table: &str,
    referenced_columns: &[&str],
    delete_rule: ForeignKeyDeleteRule,
) -> Result<(), VfsError> {
    let columns = columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    let referenced_columns = referenced_columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    let delete_rule = delete_rule.pg_code();
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
                  AND c.confdeltype::text = $5
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
            &[
                &table,
                &referenced_table,
                &columns,
                &referenced_columns,
                &delete_rule,
            ],
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

async fn require_org_tenant_backfill_consistency(
    client: &impl GenericClient,
) -> Result<(), VfsError> {
    for query in [
        "SELECT EXISTS (
            SELECT 1
            FROM repos
            WHERE org_id IS NULL
        )",
        "SELECT EXISTS (
            SELECT 1
            FROM workspaces workspace
            JOIN repos repo ON repo.id = workspace.repo_id
            WHERE workspace.org_id IS DISTINCT FROM repo.org_id
        )",
        "SELECT EXISTS (
            SELECT 1
            FROM workspace_tokens token
            JOIN workspaces workspace ON workspace.id = token.workspace_id
            WHERE token.org_id IS DISTINCT FROM workspace.org_id
        )",
        "SELECT EXISTS (
            SELECT 1
            FROM durable_principals principal
            JOIN repos repo ON repo.id = principal.repo_id
            WHERE principal.org_id IS DISTINCT FROM repo.org_id
        )",
    ] {
        let exists: bool = client
            .query_one(query, &[])
            .await
            .map_err(|error| postgres_error("verify migration adoption catalog", error))?
            .get(0);
        if exists {
            return Err(adoption_verification_error());
        }
    }
    Ok(())
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
            "SELECT id, display_name, created_at, archived_at
             FROM organizations
             LIMIT 0;
             SELECT org_id, principal_uid, role, active, created_at, updated_at
             FROM org_memberships
             LIMIT 0;
             SELECT id, org_id, name, principal_uid, active, created_at, updated_at
             FROM org_service_accounts
             LIMIT 0;
             SELECT id, org_id, provider_key, display_name, issuer_hash, client_id_hash,
                    client_secret_ref, enabled, created_at, updated_at, disabled_at
             FROM oidc_providers
             LIMIT 0;
             SELECT id, org_id, repo_id, provider_id, principal_uid, subject_hash,
                    username_hint, created_at, updated_at, disabled_at
             FROM external_identities
             LIMIT 0;
             SELECT id, org_id, repo_id, provider_key, display_name,
                    idp_entity_id_hash, sp_entity_id_hash, acs_url_hash,
                    audience_hash, signing_cert_ref_hash, binding,
                    group_attribute, required_group_hash, enabled, created_at,
                    updated_at, disabled_at
             FROM saml_providers
             LIMIT 0;
             SELECT id, org_id, repo_id, provider_id, principal_uid,
                    name_id_hash, username_hint, created_at, updated_at,
                    disabled_at
             FROM saml_external_identities
             LIMIT 0;
             SELECT id, org_id, repo_id, provider_id, external_group_hash,
                    external_group_name_ref, local_gid, required, active,
                    created_at, updated_at, disabled_at
             FROM saml_group_mappings
             LIMIT 0;
             SELECT id, org_id, repo_id, provider_id, principal_uid,
                    assertion_id_hash, replay_key, first_seen_at, expires_at
             FROM saml_assertion_replay
             LIMIT 0;
             SELECT id, org_id, repo_id, provider_key, token_hash, enabled,
                    created_at, updated_at, disabled_at
             FROM scim_clients
             LIMIT 0;
             SELECT id, org_id, repo_id, client_id, principal_uid,
                    external_id_hash, username_hint, active, created_at,
                    updated_at, disabled_at
             FROM scim_users
             LIMIT 0;
             SELECT id, org_id, repo_id, client_id, local_gid,
                    external_id_hash, display_name, active, created_at,
                    updated_at, disabled_at
             FROM scim_groups
             LIMIT 0;
             SELECT id, org_id, repo_id, group_id, principal_uid, active,
                    created_at, updated_at, disabled_at
             FROM scim_group_members
             LIMIT 0;
             SELECT id, org_id, repo_id, principal_uid, external_identity_id,
                    current_token_version, issued_at, updated_at, expires_at,
                    revoked_at, reuse_detected_at
             FROM refresh_token_families
             LIMIT 0;
             SELECT id, family_id, org_id, repo_id, principal_uid, token_hash,
                    token_version, issued_at, expires_at, rotated_at,
                    rotated_to_token_id, revoked_at, reuse_denied_at
             FROM refresh_tokens
             LIMIT 0;
             SELECT id, org_id, name, created_at
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
             SELECT id, org_id, repo_id, name, root_path, head_commit, version, base_ref, session_ref, created_at
             FROM workspaces
             LIMIT 0;
             SELECT id, workspace_id, org_id, repo_id, name, agent_uid, secret_hash,
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
             SELECT uid, org_id, repo_id, username, primary_gid, groups_json, kind, active,
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
             LIMIT 0;
             SELECT change_request_id, head_commit, path, viewed_by, viewed, version, created_at, updated_at
             FROM change_request_file_views
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
    transaction: &Transaction<'_>,
    namespace: i32,
    key: i32,
) -> Result<(), VfsError> {
    let locked = postgres_try_advisory_xact_lock(
        transaction,
        PostgresAdvisoryXactLockKey::new(namespace, key),
        "acquire migration startup lock",
    )
    .await?;
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

    fn assert_all_known_pending(report: &PostgresMigrationReport) {
        assert_eq!(report.statuses.len(), POSTGRES_MIGRATIONS.len());
        for (status, migration) in report.statuses.iter().zip(POSTGRES_MIGRATIONS.iter()) {
            assert_eq!(
                status,
                &PostgresMigrationStatus::Pending {
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
    fn org_tenant_foundation_migration_is_registered_and_non_destructive() {
        let migration = migration_by_version(15).expect("org tenant migration is registered");
        assert_eq!(migration.name, "org_tenant_foundation");

        for expected in [
            "CREATE TABLE IF NOT EXISTS organizations",
            "CREATE TABLE IF NOT EXISTS org_memberships",
            "CREATE TABLE IF NOT EXISTS org_service_accounts",
            "ALTER TABLE repos ADD COLUMN org_id TEXT",
            "UPDATE repos SET org_id = 'default_org' WHERE org_id IS NULL",
            "ALTER COLUMN org_id SET NOT NULL",
            "ALTER TABLE workspaces ADD COLUMN org_id TEXT",
            "UPDATE workspaces",
            "ALTER TABLE workspace_tokens ADD COLUMN org_id TEXT",
            "ALTER TABLE durable_principals ADD COLUMN org_id TEXT",
            "repos_org_id_fk",
            "workspaces_org_repo_shape_check",
            "workspace_tokens_org_repo_shape_check",
            "durable_principals_org_repo_shape_check",
        ] {
            assert!(
                migration.sql.contains(expected),
                "migration 15 missing invariant: {expected}"
            );
        }
    }

    #[test]
    fn oidc_refresh_token_foundation_migration_is_registered_and_non_destructive() {
        let migration =
            migration_by_version(16).expect("oidc refresh token migration is registered");
        assert_eq!(migration.name, "oidc_refresh_token_foundation");
        assert_eq!(POSTGRES_MIGRATIONS.len(), 23);

        for expected in [
            "CREATE TABLE IF NOT EXISTS oidc_providers",
            "CREATE TABLE IF NOT EXISTS external_identities",
            "CREATE TABLE IF NOT EXISTS refresh_token_families",
            "CREATE TABLE IF NOT EXISTS refresh_tokens",
            "oidc_providers_org_key_idx",
            "external_identities_provider_subject_idx",
            "refresh_token_families_active_principal_idx",
            "refresh_tokens_family_active_idx",
            "oidc_providers_client_secret_ref_check",
            "oidc_providers_issuer_hash_check",
            "oidc_providers_client_id_hash_check",
            "external_identities_subject_hash_check",
            "refresh_tokens_token_hash_check",
            "refresh_tokens_token_version_check",
            "refresh_tokens_lifecycle_check",
            "refresh_tokens_family_id_key",
            "refresh_tokens_rotated_to_family_fk",
            "REFERENCES organizations(id)",
            "REFERENCES repos(org_id, id)",
            "durable_principals_org_repo_uid_key",
            "REFERENCES durable_principals(org_id, repo_id, uid)",
        ] {
            assert!(
                migration.sql.contains(expected),
                "migration 16 missing invariant: {expected}"
            );
        }

        for forbidden in [
            "client_secret TEXT",
            "subject TEXT",
            "raw_subject",
            "raw_secret",
        ] {
            assert!(
                !migration.sql.contains(forbidden),
                "migration 16 must not store raw secret material: {forbidden}"
            );
        }
    }

    #[test]
    fn saml_sso_foundation_migration_is_registered_and_non_destructive() {
        let migration = migration_by_version(17).expect("SAML SSO migration is registered");
        assert_eq!(migration.name, "saml_sso_foundation");
        assert_eq!(POSTGRES_MIGRATIONS.len(), 23);

        for expected in [
            "CREATE TABLE IF NOT EXISTS saml_providers",
            "CREATE TABLE IF NOT EXISTS saml_external_identities",
            "CREATE TABLE IF NOT EXISTS saml_group_mappings",
            "CREATE TABLE IF NOT EXISTS saml_assertion_replay",
            "saml_providers_org_repo_key_idx",
            "saml_external_identities_provider_name_id_idx",
            "saml_group_mappings_provider_group_idx",
            "saml_assertion_replay_key_idx",
            "saml_providers_idp_entity_id_hash_check",
            "saml_providers_sp_entity_id_hash_check",
            "saml_providers_acs_url_hash_check",
            "saml_providers_audience_hash_check",
            "saml_providers_signing_cert_ref_hash_check",
            "saml_providers_required_group_hash_check",
            "saml_external_identities_name_id_hash_check",
            "saml_group_mappings_external_group_hash_check",
            "saml_assertion_replay_assertion_id_hash_check",
            "saml_assertion_replay_replay_key_check",
            "saml_assertion_replay_lifecycle_check",
            "binding = 'post'",
            "REFERENCES organizations(id)",
            "REFERENCES repos(org_id, id)",
            "REFERENCES durable_principals(org_id, repo_id, uid)",
            "REFERENCES saml_providers(id, org_id, repo_id)",
        ] {
            assert!(
                migration.sql.contains(expected),
                "migration 17 missing invariant: {expected}"
            );
        }

        for forbidden in [
            "idp_entity_id TEXT",
            "sp_entity_id TEXT",
            "acs_url TEXT",
            "audience TEXT",
            "signing_cert TEXT",
            "name_id TEXT",
            "assertion_id TEXT",
            "raw_saml",
            "raw_assertion",
            "'redirect'",
        ] {
            assert!(
                !migration.sql.contains(forbidden),
                "migration 17 must not store raw SAML material: {forbidden}"
            );
        }
    }

    #[test]
    fn scim_provisioning_foundation_migration_is_registered_and_non_destructive() {
        let migration =
            migration_by_version(18).expect("SCIM provisioning migration is registered");
        assert_eq!(migration.name, "scim_provisioning_foundation");
        assert_eq!(POSTGRES_MIGRATIONS.len(), 23);

        for expected in [
            "CREATE TABLE IF NOT EXISTS scim_clients",
            "CREATE TABLE IF NOT EXISTS scim_users",
            "CREATE TABLE IF NOT EXISTS scim_groups",
            "CREATE TABLE IF NOT EXISTS scim_group_members",
            "scim_clients_org_repo_key_idx",
            "scim_clients_enabled_idx",
            "scim_users_client_external_id_idx",
            "scim_users_principal_idx",
            "scim_groups_client_external_id_idx",
            "scim_groups_local_gid_idx",
            "scim_group_members_group_principal_idx",
            "scim_group_members_principal_idx",
            "scim_clients_token_hash_check",
            "scim_users_external_id_hash_check",
            "scim_groups_external_id_hash_check",
            "scim_group_members_lifecycle_check",
            "enabled BOOLEAN NOT NULL DEFAULT false",
            "active BOOLEAN NOT NULL DEFAULT true",
            "REFERENCES organizations(id)",
            "REFERENCES repos(org_id, id)",
            "REFERENCES durable_principals(org_id, repo_id, uid)",
            "REFERENCES scim_clients(id, org_id, repo_id)",
            "REFERENCES scim_groups(id, org_id, repo_id)",
        ] {
            assert!(
                migration.sql.contains(expected),
                "migration 18 missing invariant: {expected}"
            );
        }

        for forbidden in [
            "token TEXT",
            "external_id TEXT",
            "username TEXT NOT NULL",
            "display_name TEXT NOT NULL",
            "raw_scim",
            "raw_group",
            "raw_user",
        ] {
            assert!(
                !migration.sql.contains(forbidden),
                "migration 18 must not store raw SCIM material: {forbidden}"
            );
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

        assert_all_known_pending(&report);
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

        assert_all_known_applied(&first);
        assert_all_known_applied(&second);
        assert_eq!(status, second);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn apply_rolls_back_oidc_control_row_when_post_apply_verification_fails() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let client = db.client_in_schema().await;
        for migration in POSTGRES_MIGRATIONS.iter().take(15) {
            client
                .batch_execute(migration.sql)
                .await
                .expect("apply pre-OIDC migration");
        }
        client
            .batch_execute(OIDC_REFRESH_TOKEN_FOUNDATION_SQL)
            .await
            .expect("precreate OIDC migration schema");
        client
            .batch_execute(
                "ALTER TABLE oidc_providers
                    DROP CONSTRAINT oidc_providers_issuer_hash_check;
                 ALTER TABLE oidc_providers
                    ADD CONSTRAINT oidc_providers_issuer_hash_check CHECK (
                        issuer_hash <> ''
                    );",
            )
            .await
            .expect("weaken OIDC issuer hash shape");
        let runner = db.runner();
        runner
            .create_control_table_for_test()
            .await
            .expect("create control table");
        for migration in POSTGRES_MIGRATIONS.iter().take(15) {
            record_migration_adopted(&client, migration)
                .await
                .expect("seed applied pre-OIDC migration");
        }

        let err = runner
            .apply_pending()
            .await
            .expect_err("post-apply schema verification should fail");

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        let rows = client
            .query(
                "SELECT version, state FROM stratum_schema_migrations ORDER BY version ASC",
                &[],
            )
            .await
            .expect("load migration control rows");
        assert_eq!(rows.len(), 15);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.get::<_, i64>("version"), (index + 1) as i64);
            assert_eq!(row.get::<_, String>("state"), "applied");
        }
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
    async fn adopt_refuses_schema_missing_refresh_token_constraints() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE refresh_tokens
                 DROP CONSTRAINT refresh_tokens_token_hash_check",
            )
            .await
            .expect("make refresh token hash shape unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing refresh token hash constraint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("refresh_tokens"));
        assert!(!message.contains("token_hash"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_weakened_oidc_provider_hash_constraint() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE oidc_providers
                    DROP CONSTRAINT oidc_providers_issuer_hash_check;
                 ALTER TABLE oidc_providers
                    ADD CONSTRAINT oidc_providers_issuer_hash_check CHECK (
                        issuer_hash IS NOT NULL
                    );",
            )
            .await
            .expect("weaken OIDC issuer hash shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened OIDC issuer hash constraint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("oidc_providers"));
        assert!(!message.contains("issuer_hash"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_weakened_refresh_token_hash_constraint() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE refresh_tokens
                    DROP CONSTRAINT refresh_tokens_token_hash_check;
                 ALTER TABLE refresh_tokens
                    ADD CONSTRAINT refresh_tokens_token_hash_check CHECK (
                        token_hash IS NOT NULL
                    );",
            )
            .await
            .expect("weaken refresh token hash shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened refresh token hash constraint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("refresh_tokens"));
        assert!(!message.contains("token_hash"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_weakened_active_refresh_token_family_shape() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "DROP INDEX refresh_token_families_active_principal_idx;
                 CREATE INDEX refresh_token_families_active_principal_idx
                    ON refresh_token_families(org_id);",
            )
            .await
            .expect("weaken active refresh token family shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened active family index should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("refresh_token_families"));
        assert!(!message.contains("active_principal"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn adopt_refuses_weakened_active_refresh_token_shape() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "DROP INDEX refresh_tokens_family_active_idx;
                 CREATE INDEX refresh_tokens_family_active_idx
                    ON refresh_tokens(family_id, token_version)
                    WHERE rotated_at IS NULL
                      AND revoked_at IS NULL
                      AND reuse_denied_at IS NULL;",
            )
            .await
            .expect("weaken active refresh token shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened active refresh token index should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("refresh_tokens"));
        assert!(!message.contains("family_active"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn known_schema_verifier_requires_saml_tables() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute("DROP TABLE saml_assertion_replay")
            .await
            .expect("make SAML replay table unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing SAML table should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn weakened_saml_hash_lifecycle_or_replay_constraints_fail_adoption() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_assertion_replay
                    DROP CONSTRAINT saml_assertion_replay_assertion_id_hash_check;
                 ALTER TABLE saml_assertion_replay
                    ADD CONSTRAINT saml_assertion_replay_assertion_id_hash_check CHECK (
                        assertion_id_hash IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML assertion hash shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SAML assertion hash should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        assert!(!message.contains("assertion_id_hash"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_assertion_replay
                    DROP CONSTRAINT saml_assertion_replay_lifecycle_check;
                 ALTER TABLE saml_assertion_replay
                    ADD CONSTRAINT saml_assertion_replay_lifecycle_check CHECK (
                        expires_at IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML replay lifecycle");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SAML replay lifecycle should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        assert!(!message.contains("expires_at"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "DROP INDEX saml_assertion_replay_key_idx;
                 CREATE INDEX saml_assertion_replay_key_idx
                    ON saml_assertion_replay(provider_id);",
            )
            .await
            .expect("weaken SAML replay key shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SAML replay index should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        assert!(!message.contains("replay_key"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn known_schema_verifier_requires_scim_tables() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute("DROP TABLE scim_group_members")
            .await
            .expect("make SCIM membership table unverifiable");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("missing SCIM table should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("scim_group_members"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn raw_scim_material_columns_fail_adoption() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute("ALTER TABLE scim_users ADD COLUMN external_id TEXT;")
            .await
            .expect("add raw SCIM external id material");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("raw SCIM material column should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("external_id"));
        assert!(!message.contains("scim_users"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute("ALTER TABLE scim_groups ADD COLUMN external_group_id TEXT;")
            .await
            .expect("add raw SCIM external group id material");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("patterned raw SCIM material column should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("external_group_id"));
        assert!(!message.contains("scim_groups"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn weakened_scim_hash_lifecycle_or_membership_constraints_fail_adoption() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE scim_users
                    DROP CONSTRAINT scim_users_external_id_hash_check;
                 ALTER TABLE scim_users
                    ADD CONSTRAINT scim_users_external_id_hash_check CHECK (
                        external_id_hash IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SCIM user external id hash shape");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SCIM user hash should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("scim_users"));
        assert!(!message.contains("external_id_hash"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE scim_group_members
                    DROP CONSTRAINT scim_group_members_lifecycle_check;
                 ALTER TABLE scim_group_members
                    ADD CONSTRAINT scim_group_members_lifecycle_check CHECK (
                        updated_at IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SCIM membership lifecycle");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SCIM membership lifecycle should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("scim_group_members"));
        assert!(!message.contains("updated_at"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "DROP INDEX scim_group_members_group_principal_idx;
                 CREATE INDEX scim_group_members_group_principal_idx
                    ON scim_group_members(group_id);",
            )
            .await
            .expect("weaken SCIM membership uniqueness index");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SCIM membership index should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("scim_group_members"));
        assert!(!message.contains("principal_uid"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn weakened_saml_column_shape_or_defaults_fail_adoption() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_assertion_replay
                    ALTER COLUMN replay_key DROP NOT NULL;",
            )
            .await
            .expect("weaken SAML replay key nullability");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("nullable SAML replay key should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        assert!(!message.contains("replay_key"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    ALTER COLUMN display_name DROP NOT NULL;",
            )
            .await
            .expect("weaken SAML provider display name nullability");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("nullable SAML provider display name should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("display_name"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    ALTER COLUMN enabled SET DEFAULT (
                        false OR current_setting('server_version_num') IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML provider disabled default with truthy expression");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("truthy SAML provider enabled default should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("enabled"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_assertion_replay
                    ALTER COLUMN first_seen_at SET DEFAULT (now() + interval '1 day');",
            )
            .await
            .expect("weaken SAML replay first-seen default");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("shifted SAML replay first-seen default should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        assert!(!message.contains("first_seen_at"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    ALTER COLUMN enabled SET DEFAULT true;",
            )
            .await
            .expect("weaken SAML provider disabled default");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("enabled-by-default SAML provider should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("enabled"));
        db.cleanup().await;
    }

    #[tokio::test]
    async fn tautological_saml_constraints_fail_adoption() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    DROP CONSTRAINT saml_providers_binding_check;
                 ALTER TABLE saml_providers
                    ADD CONSTRAINT saml_providers_binding_check CHECK (
                        binding = 'post' OR true
                    );",
            )
            .await
            .expect("weaken SAML binding check with tautology");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("tautological SAML binding should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("binding"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_assertion_replay
                    DROP CONSTRAINT saml_assertion_replay_assertion_id_hash_check;
                 ALTER TABLE saml_assertion_replay
                    ADD CONSTRAINT saml_assertion_replay_assertion_id_hash_check CHECK (
                        assertion_id_hash ~ '^[0-9a-f]{64}$' OR true
                    );",
            )
            .await
            .expect("weaken SAML assertion hash check with tautology");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("tautological SAML assertion hash should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_assertion_replay"));
        assert!(!message.contains("assertion_id_hash"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    DROP CONSTRAINT saml_providers_lifecycle_check;
                 ALTER TABLE saml_providers
                    ADD CONSTRAINT saml_providers_lifecycle_check CHECK (
                        updated_at >= created_at OR disabled_at >= created_at
                    );",
            )
            .await
            .expect("weaken SAML provider lifecycle with alternate OR path");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SAML provider lifecycle should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("lifecycle"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    DROP CONSTRAINT saml_providers_required_group_hash_check;
                 ALTER TABLE saml_providers
                    ADD CONSTRAINT saml_providers_required_group_hash_check CHECK (
                        required_group_hash IS NULL
                        OR required_group_hash ~ '^[0-9a-f]{64}$'
                        OR required_group_hash IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML required group hash check with tautology");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("tautological SAML required group hash should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("required_group_hash"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    DROP CONSTRAINT saml_providers_display_name_check;
                 ALTER TABLE saml_providers
                    ADD CONSTRAINT saml_providers_display_name_check CHECK (
                        display_name IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML display name check");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SAML display name should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("display_name"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_providers
                    DROP CONSTRAINT saml_providers_group_attribute_check;
                 ALTER TABLE saml_providers
                    ADD CONSTRAINT saml_providers_group_attribute_check CHECK (
                        group_attribute IS NULL OR group_attribute IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML group attribute check");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("tautological SAML group attribute should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_providers"));
        assert!(!message.contains("group_attribute"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_external_identities
                    DROP CONSTRAINT saml_external_identities_username_hint_check;
                 ALTER TABLE saml_external_identities
                    ADD CONSTRAINT saml_external_identities_username_hint_check CHECK (
                        username_hint IS NULL OR username_hint IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML username hint check");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("tautological SAML username hint should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_external_identities"));
        assert!(!message.contains("username_hint"));
        db.cleanup().await;

        let Some(db) = TestDb::new().await else {
            return;
        };
        db.apply_legacy_catalog().await;
        db.client_in_schema()
            .await
            .batch_execute(
                "ALTER TABLE saml_group_mappings
                    DROP CONSTRAINT saml_group_mappings_local_gid_check;
                 ALTER TABLE saml_group_mappings
                    ADD CONSTRAINT saml_group_mappings_local_gid_check CHECK (
                        local_gid IS NOT NULL
                    );",
            )
            .await
            .expect("weaken SAML local gid check");

        let err = db
            .runner()
            .adopt_applied()
            .await
            .expect_err("weakened SAML local gid should fail adoption");
        let message = err.to_string();

        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        assert!(message.contains("cannot be verified"));
        assert!(!message.contains("saml_group_mappings"));
        assert!(!message.contains("local_gid"));
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
                "INSERT INTO repos (id, name, org_id)
                 VALUES ('workspace_token_probe_repo', 'workspace token probe', 'default_org');
                 INSERT INTO workspaces (id, org_id, repo_id, name, root_path)
                 VALUES (
                    '00000000-0000-0000-0000-000000009001',
                    'default_org',
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

    #[test]
    fn postgres_fts_search_mvp_migration_is_registered_and_non_destructive() {
        assert_eq!(postgres_migration_catalog_len(), 23);
        let m19 = migration_by_version(19).expect("migration 19 registered");
        assert_eq!(m19.name, "postgres_fts_search_mvp");
        let sql = m19.sql.to_uppercase();
        assert!(!sql.contains("DROP TABLE"));
        assert!(!sql.contains("DROP COLUMN"));
        assert!(!sql.contains("PGVECTOR"));
        assert!(!sql.contains("EMBEDDING"));
    }

    #[test]
    fn file_extractors_migration_is_registered_and_non_destructive() {
        assert_eq!(postgres_migration_catalog_len(), 23);
        let m21 = migration_by_version(21).expect("migration 21 registered");
        assert_eq!(m21.name, "file_extractors");
        let m20 = migration_by_version(20).expect("migration 20 registered");
        assert_eq!(m20.name, "acl_snapshot_filtering");
        let sql = m21.sql.to_uppercase();
        assert!(!sql.contains("DROP TABLE"));
        assert!(!sql.contains("DROP COLUMN"));
        assert!(sql.contains("EXTRACTED_TEXT_RECORDS"));
        assert!(sql.contains("EXTRACTION_STATUS"));
    }

    #[test]
    fn acl_snapshot_filtering_migration_is_registered_and_non_destructive() {
        assert_eq!(postgres_migration_catalog_len(), 23);
        let m20 = migration_by_version(20).expect("migration 20 registered");
        assert_eq!(m20.name, "acl_snapshot_filtering");
        let m19 = migration_by_version(19).expect("migration 19 registered");
        assert_eq!(m19.name, "postgres_fts_search_mvp");
        let sql = m20.sql.to_uppercase();
        assert!(!sql.contains("DROP TABLE"));
        assert!(!sql.contains("DROP COLUMN"));
        assert!(sql.contains("ACL_SNAPSHOT"));
        assert!(sql.contains("SEARCH_INDEX_STATE"));
        assert!(sql.contains("SEARCH_INDEX_FILES"));
    }

    #[test]
    fn pgvector_semantic_expansion_migration_is_registered_and_non_destructive() {
        assert_eq!(postgres_migration_catalog_len(), 23);
        let m22 = migration_by_version(22).expect("migration 22 registered");
        assert_eq!(m22.name, "pgvector_semantic_expansion");
        let m21 = migration_by_version(21).expect("migration 21 registered");
        assert_eq!(m21.name, "file_extractors");

        // Migration 22 is registered after file_extractors.
        let idx22 = POSTGRES_MIGRATIONS
            .iter()
            .position(|m| m.version == 22)
            .expect("migration 22 present");
        let idx21 = POSTGRES_MIGRATIONS
            .iter()
            .position(|m| m.version == 21)
            .expect("migration 21 present");
        assert!(idx22 > idx21, "migration 22 must follow file_extractors");

        let sql = m22.sql.to_uppercase();
        assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS VECTOR WITH SCHEMA PUBLIC"));
        assert!(sql.contains("SEARCH_INDEX_VECTOR_STATE"));
        assert!(sql.contains("SEARCH_INDEX_VECTORS"));
        assert!(sql.contains("PUBLIC.VECTOR"));
        assert!(sql.contains("PUBLIC.VECTOR_DIMS"));
        assert!(sql.contains("EMBEDDING_PROVIDER"));
        assert!(sql.contains("EMBEDDING_DIMENSIONS"));
        assert!(sql.contains("CHUNKER_VERSION"));
        assert!(sql.contains("SEARCH_INDEX_VECTORS_HEAD_MODEL_IDENTITY_IDX"));
        assert!(!sql.contains("DROP TABLE"));
        assert!(!sql.contains("DROP COLUMN"));

        // Must not leak provider secrets, raw provider URLs, or sample embeddings.
        let raw = m22.sql;
        for forbidden in [
            "api_key", "api-key", "apikey", "secret", "https://", "http://", "bearer ",
        ] {
            assert!(
                !raw.to_lowercase().contains(forbidden),
                "migration 22 must not embed provider secret or url material: {forbidden}"
            );
        }
        // No sample embedding literals (a bracketed float list).
        assert!(
            !raw.contains("[0."),
            "migration 22 must not embed sample vector literals"
        );
        // No raw extracted text column on vector tables.
        assert!(
            !sql.contains("EXTRACTED_TEXT TEXT"),
            "vector tables must not store raw extracted text"
        );
        assert!(
            !sql.contains("CHUNK_TEXT"),
            "vector tables must not store raw chunk text"
        );
    }

    #[test]
    fn review_viewed_files_migration_is_registered_and_non_destructive() {
        assert_eq!(postgres_migration_catalog_len(), 23);
        let m23 = migration_by_version(23).expect("migration 23 registered");
        assert_eq!(m23.name, "review_viewed_files");
        let m22 = migration_by_version(22).expect("migration 22 registered");
        assert_eq!(m22.name, "pgvector_semantic_expansion");

        let idx23 = POSTGRES_MIGRATIONS
            .iter()
            .position(|m| m.version == 23)
            .expect("migration 23 present");
        let idx22 = POSTGRES_MIGRATIONS
            .iter()
            .position(|m| m.version == 22)
            .expect("migration 22 present");
        assert!(
            idx23 > idx22,
            "migration 23 must follow pgvector_semantic_expansion"
        );

        let sql = m23.sql.to_uppercase();
        assert!(sql.contains("CREATE TABLE CHANGE_REQUEST_FILE_VIEWS"));
        assert!(sql.contains("CHANGE_REQUEST_FILE_VIEWS_CHANGE_HEAD_IDX"));
        assert!(sql.contains("REFERENCES CHANGE_REQUESTS(ID)"));
        assert!(!sql.contains("DROP TABLE"));
        assert!(!sql.contains("DROP COLUMN"));
    }

    #[tokio::test]
    async fn known_schema_verifier_requires_change_request_file_views_table() {
        let Some(db) = TestDb::new().await else {
            return;
        };
        db.runner().adopt_applied().await.expect("adopt schema");
        db.cleanup().await;
    }

    #[tokio::test]
    async fn known_schema_verifier_requires_search_tables_and_constraints() {
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute("DROP TABLE search_index_files; DROP TABLE search_index_state;")
                .await
                .expect("drop search tables");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert!(err.to_string().contains("cannot be verified"));
            assert!(!err.to_string().contains("search_index"));
            db.cleanup().await;
        }

        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute("ALTER TABLE search_index_state DROP CONSTRAINT search_index_state_status_check")
                .await
                .expect("drop constraint");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert!(err.to_string().contains("cannot be verified"));
            assert!(!err.to_string().contains("status_check"));
            db.cleanup().await;
        }

        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute("DROP INDEX search_index_files_vector_idx")
                .await
                .expect("drop index");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert!(err.to_string().contains("cannot be verified"));
            assert!(!err.to_string().contains("vector_idx"));
            db.cleanup().await;
        }
    }

    fn assert_redacted_adoption_error(err: &VfsError) {
        let text = err.to_string();
        assert!(text.contains("cannot be verified"), "unexpected: {text}");
        for leak in [
            "search_index_vectors",
            "search_index_vector_state",
            "embedding_model",
            "acl_snapshot_hash",
            "extracted_text_hash",
            "vector_dims",
            "embedding_provider",
            "SQLSTATE",
            "[0.",
            "DROP",
            "CONSTRAINT",
        ] {
            assert!(
                !text.contains(leak),
                "adoption error leaked internal detail: {leak}"
            );
        }
    }

    #[tokio::test]
    async fn known_schema_verifier_requires_pgvector_tables_and_constraints() {
        // missing pgvector extension
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute(
                    "DROP TABLE search_index_vectors; \
                     DROP TABLE search_index_vector_state; \
                     DROP EXTENSION vector CASCADE;",
                )
                .await
                .expect("drop vector schema and extension");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // missing vector state table
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute(
                    "DROP TABLE search_index_vectors; DROP TABLE search_index_vector_state;",
                )
                .await
                .expect("drop vector state table");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // missing vector rows table
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute("DROP TABLE search_index_vectors;")
                .await
                .expect("drop vector rows table");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // weakened dimension check
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute(
                    "ALTER TABLE search_index_vectors \
                     DROP CONSTRAINT search_index_vectors_embedding_dimensions_check",
                )
                .await
                .expect("drop dimension check");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // missing FK to search_index_files
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            let client = db.client_in_schema().await;
            let fk: String = client
                .query_one(
                    "SELECT c.conname
                     FROM pg_catalog.pg_constraint c
                     JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                     JOIN pg_catalog.pg_class ref ON ref.oid = c.confrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                     WHERE n.nspname = current_schema()
                       AND r.relname = 'search_index_vectors'
                       AND ref.relname = 'search_index_files'
                       AND c.contype = 'f'",
                    &[],
                )
                .await
                .expect("find files fk")
                .get(0);
            client
                .batch_execute(&format!(
                    "ALTER TABLE search_index_vectors DROP CONSTRAINT \"{fk}\""
                ))
                .await
                .expect("drop files fk");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // missing acl_snapshot_hash column
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute(
                    "ALTER TABLE search_index_vectors DROP COLUMN acl_snapshot_hash CASCADE",
                )
                .await
                .expect("drop acl_snapshot_hash");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // missing extracted_text_hash column
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute(
                    "ALTER TABLE search_index_vectors DROP COLUMN extracted_text_hash CASCADE",
                )
                .await
                .expect("drop extracted_text_hash");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // raw extracted text on vector rows
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute("ALTER TABLE search_index_vectors ADD COLUMN extracted_text TEXT")
                .await
                .expect("add raw text column");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // vector state lifecycle no longer binds status to completion/failure state
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            db.client_in_schema()
                .await
                .batch_execute(
                    "ALTER TABLE search_index_vector_state
                        DROP CONSTRAINT search_index_vector_state_lifecycle_check;
                     ALTER TABLE search_index_vector_state
                        ADD CONSTRAINT search_index_vector_state_lifecycle_check CHECK (
                            status IN ('indexing', 'ready', 'failed')
                            AND (completed_at IS NULL OR completed_at IS NOT NULL)
                            AND (failure_code IS NULL OR failure_code IS NOT NULL)
                        );",
                )
                .await
                .expect("weaken vector state lifecycle");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // vector state is not retained with the exact search head
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            let client = db.client_in_schema().await;
            let fk: String = client
                .query_one(
                    "SELECT c.conname
                     FROM pg_catalog.pg_constraint c
                     JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                     JOIN pg_catalog.pg_class ref ON ref.oid = c.confrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                     WHERE n.nspname = current_schema()
                       AND r.relname = 'search_index_vector_state'
                       AND ref.relname = 'search_index_state'
                       AND c.contype = 'f'",
                    &[],
                )
                .await
                .expect("find vector state fk")
                .get(0);
            client
                .batch_execute(&format!(
                    "ALTER TABLE search_index_vector_state DROP CONSTRAINT \"{fk}\"; \
                     ALTER TABLE search_index_vector_state \
                     ADD CONSTRAINT search_index_vector_state_head_no_action_fk \
                     FOREIGN KEY (repo_id, commit_id, root_tree_id) \
                     REFERENCES search_index_state(repo_id, commit_id, root_tree_id)"
                ))
                .await
                .expect("weaken vector state fk");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }

        // vector rows allowed without matching model identity FK to vector state
        {
            let Some(db) = TestDb::new().await else {
                return;
            };
            db.apply_legacy_catalog().await;
            let client = db.client_in_schema().await;
            let fk: String = client
                .query_one(
                    "SELECT c.conname
                     FROM pg_catalog.pg_constraint c
                     JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                     JOIN pg_catalog.pg_class ref ON ref.oid = c.confrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                     WHERE n.nspname = current_schema()
                       AND r.relname = 'search_index_vectors'
                       AND ref.relname = 'search_index_vector_state'
                       AND c.contype = 'f'",
                    &[],
                )
                .await
                .expect("find state fk")
                .get(0);
            client
                .batch_execute(&format!(
                    "ALTER TABLE search_index_vectors DROP CONSTRAINT \"{fk}\""
                ))
                .await
                .expect("drop state fk");
            let err = db.runner().adopt_applied().await.expect_err("should fail");
            assert_redacted_adoption_error(&err);
            db.cleanup().await;
        }
    }
}
