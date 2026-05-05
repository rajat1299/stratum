//! Postgres migration runner foundation for durable backend schemas.
//!
//! This module is feature-gated behind `postgres` and backs durable
//! `stratum-server` startup preflight. It provides the ordered migration
//! catalog, schema state reporting, dirty-state refusal, and schema-scoped
//! startup lock used before durable control-plane stores are opened.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use tokio_postgres::{Client, Config, GenericClient};

use crate::backend::postgres::{connect_with_schema, postgres_error, validate_schema_name};
use crate::error::VfsError;

const MIGRATION_LOCK_NAMESPACE: i32 = 0x5354_524d; // "STRM"
const DURABLE_BACKEND_FOUNDATION_SQL: &str =
    include_str!("../../migrations/postgres/0001_durable_backend_foundation.sql");
const REVIEW_LOCAL_COMMIT_IDS_SQL: &str =
    include_str!("../../migrations/postgres/0002_review_local_commit_ids.sql");
const POSTGRES_MIGRATIONS: [PostgresMigration; 2] = [
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
];

#[derive(Clone)]
pub struct PostgresMigrationRunner {
    config: Config,
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
            config,
            schema: "public".to_string(),
        }
    }

    pub fn with_schema(config: Config, schema: impl Into<String>) -> Result<Self, VfsError> {
        Ok(Self {
            config,
            schema: validate_schema_name(schema.into())?,
        })
    }

    pub async fn status(&self) -> Result<PostgresMigrationReport, VfsError> {
        validate_catalog()?;
        let client = connect_with_schema(&self.config, Some(&self.schema)).await?;
        ensure_control_table(&client).await?;
        self.status_with_client(&client).await
    }

    pub async fn apply_pending(&self) -> Result<PostgresMigrationReport, VfsError> {
        validate_catalog()?;
        let mut client = connect_with_schema(&self.config, Some(&self.schema)).await?;
        let (lock_namespace, lock_key) = migration_lock_ids(&self.schema);
        acquire_migration_lock(&client, lock_namespace, lock_key).await?;

        let result = self.apply_pending_locked(&mut client).await;
        let unlock_result = release_migration_lock(&client, lock_namespace, lock_key).await;
        match (result, unlock_result) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn apply_pending_locked(
        &self,
        client: &mut Client,
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

    async fn status_with_client(
        &self,
        client: &Client,
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
        let client = connect_with_schema(&self.config, Some(&self.schema)).await?;
        ensure_control_table(&client).await
    }

    #[cfg(test)]
    async fn insert_control_row_for_test(
        &self,
        version: i64,
        state: &str,
        checksum: &str,
    ) -> Result<(), VfsError> {
        let client = connect_with_schema(&self.config, Some(&self.schema)).await?;
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
        let client = connect_with_schema(&self.config, Some(&self.schema)).await?;
        let (lock_namespace, lock_key) = migration_lock_ids(&self.schema);
        acquire_migration_lock(&client, lock_namespace, lock_key).await?;
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

async fn ensure_control_table(client: &Client) -> Result<(), VfsError> {
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

async fn load_control_rows(client: &Client) -> Result<BTreeMap<i64, ControlRow>, VfsError> {
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

async fn apply_one_migration(
    client: &mut Client,
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
    client: &Client,
    migration: &PostgresMigration,
    error: VfsError,
) -> VfsError {
    let _ = record_migration_failed(client, migration, &error).await;
    error
}

async fn record_migration_started(
    client: &Client,
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

async fn record_migration_failed(
    client: &Client,
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

async fn acquire_migration_lock(client: &Client, namespace: i32, key: i32) -> Result<(), VfsError> {
    let locked: bool = client
        .query_one("SELECT pg_try_advisory_lock($1, $2)", &[&namespace, &key])
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

async fn release_migration_lock(client: &Client, namespace: i32, key: i32) -> Result<(), VfsError> {
    let unlocked: bool = client
        .query_one("SELECT pg_advisory_unlock($1, $2)", &[&namespace, &key])
        .await
        .map_err(|error| postgres_error("release migration startup lock", error))?
        .get(0);
    if unlocked {
        Ok(())
    } else {
        Err(VfsError::CorruptStore {
            message: "Postgres migration startup lock was not held at release".to_string(),
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
    use tokio_postgres::{Config, NoTls};
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
            let (client, connection) = config.connect(NoTls).await.expect("connect test Postgres");
            tokio::spawn(async move {
                let _ = connection.await;
            });
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

        async fn cleanup(self) {
            if let Ok((client, connection)) = self.config.connect(NoTls).await {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
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
                }
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
                }
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
                }
            ]
        );
        assert_eq!(status, second);
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
