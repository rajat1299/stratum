//! Postgres-backed metadata adapters for durable backend contracts.
//!
//! This module is gated behind the `postgres` feature and is not wired into
//! the server runtime. It proves the current Postgres schema can satisfy the
//! object metadata, cleanup claim, commit metadata, ref compare-and-swap,
//! idempotency, and audit contracts.

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, Config, GenericClient, NoTls, Row};
use uuid::Uuid;

use crate::audit::{
    AuditAction, AuditActor, AuditEvent, AuditOutcome, AuditResource, AuditStore,
    AuditWorkspaceContext, NewAuditEvent,
};
use crate::backend::blob_object::{ObjectMetadataRecord, ObjectMetadataStore};
use crate::backend::object_cleanup::{
    ObjectCleanupClaim, ObjectCleanupClaimKind, ObjectCleanupClaimRequest, ObjectCleanupClaimStore,
    stale_cleanup_claim, validate_lease_owner, validate_object_key,
};
use crate::backend::{
    CommitRecord, CommitStore, RefExpectation, RefRecord, RefStore, RefUpdate, RefVersion, RepoId,
    SourceCheckedRefUpdate,
};
use crate::error::VfsError;
use crate::idempotency::{
    IdempotencyBegin, IdempotencyKey, IdempotencyRecord, IdempotencyReservation, IdempotencyStore,
};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{ChangedPath, CommitId, RefName};

#[derive(Clone)]
pub struct PostgresMetadataStore {
    config: Config,
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

    async fn connect_client(&self) -> Result<Client, VfsError> {
        connect_with_schema(&self.config, Some(&self.schema)).await
    }
}

pub(crate) async fn connect_with_schema(
    config: &Config,
    schema: Option<&str>,
) -> Result<Client, VfsError> {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|error| postgres_error("connect", error))?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::debug!("postgres metadata connection task ended with an error");
        }
    });

    if let Some(schema) = schema {
        let schema = validate_schema_name(schema.to_string())?;
        client
            .batch_execute(&format!("SET search_path TO {}", quote_identifier(&schema)))
            .await
            .map_err(|error| postgres_error("set search_path", error))?;
    }

    Ok(client)
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
        let client = self.connect_client().await?;
        ensure_repo(&client, &record.repo_id).await?;

        let size = u64_to_i64(record.size, "object size")?;
        let inserted = client
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
        match inserted.map(row_to_object_metadata).transpose()? {
            Some(inserted) => Ok(inserted),
            None => match load_object_metadata(&client, &record.repo_id, record.id).await? {
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
        }
    }

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
    ) -> Result<Option<ObjectMetadataRecord>, VfsError> {
        let client = self.connect_client().await?;
        load_object_metadata(&client, repo_id, id).await
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
                     attempts = object_cleanup_claims.attempts + 1,
                     last_error = NULL,
                     updated_at = EXCLUDED.updated_at
                 WHERE object_cleanup_claims.completed_at IS NULL
                     AND object_cleanup_claims.lease_expires_at <= EXCLUDED.updated_at
                     AND object_cleanup_claims.attempts < 9223372036854775807
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
        let mut client = self.connect_client().await?;
        let key_hash = key.key_hash();
        let insert_sql = r#"INSERT INTO idempotency_records (
                scope,
                key_hash,
                request_fingerprint,
                state,
                reserved_at,
                created_at
            )
            VALUES ($1, $2, $3, 'pending', clock_timestamp(), clock_timestamp())
            ON CONFLICT (scope, key_hash) DO NOTHING
            RETURNING xmin::text AS reservation_token"#;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("idempotency begin transaction", error))?;

        fn classify_row(row: Row, request_fingerprint: &str) -> Result<IdempotencyBegin, VfsError> {
            let state: String = row.try_get("state").map_err(|_| VfsError::CorruptStore {
                message: "idempotency row missing state".to_string(),
            })?;
            let stored_fp: String =
                row.try_get("request_fingerprint")
                    .map_err(|_| VfsError::CorruptStore {
                        message: "idempotency row missing fingerprint".to_string(),
                    })?;
            match state.as_str() {
                "pending" => {
                    if stored_fp == request_fingerprint {
                        Ok(IdempotencyBegin::InProgress)
                    } else {
                        Ok(IdempotencyBegin::Conflict)
                    }
                }
                "completed" => {
                    if stored_fp != request_fingerprint {
                        return Ok(IdempotencyBegin::Conflict);
                    }
                    let status_opt: Option<i32> =
                        row.try_get("status_code")
                            .map_err(|_| VfsError::CorruptStore {
                                message: "idempotency completed row corrupt".to_string(),
                            })?;
                    let body_opt: Option<Json<serde_json::Value>> = row
                        .try_get("response_body_json")
                        .map_err(|_| VfsError::CorruptStore {
                            message: "idempotency completed row corrupt".to_string(),
                        })?;
                    match (status_opt, body_opt) {
                        (Some(code), Some(Json(body))) => {
                            let status_code =
                                u16::try_from(code).map_err(|_| VfsError::CorruptStore {
                                    message: format!(
                                        "idempotency status code out of range: {code}"
                                    ),
                                })?;
                            Ok(IdempotencyBegin::Replay(IdempotencyRecord::for_store(
                                stored_fp,
                                status_code,
                                body,
                            )))
                        }
                        _ => Err(VfsError::CorruptStore {
                            message: "idempotency completed row missing replay fields".to_string(),
                        }),
                    }
                }
                other => Err(VfsError::CorruptStore {
                    message: format!("unknown idempotency state {other:?}"),
                }),
            }
        }

        async fn try_insert_then_load<C>(
            client: &C,
            insert_sql: &str,
            scope: &str,
            key_hash: &str,
            key: &IdempotencyKey,
            request_fingerprint: &str,
            retry_miss: bool,
        ) -> Result<Option<IdempotencyBegin>, VfsError>
        where
            C: GenericClient + Sync,
        {
            let inserted = client
                .query_opt(insert_sql, &[&scope, &key_hash, &request_fingerprint])
                .await
                .map_err(|error| postgres_error("idempotency insert pending", error))?;

            if let Some(row) = inserted {
                let reservation_token: String =
                    row.try_get("reservation_token")
                        .map_err(|_| VfsError::CorruptStore {
                            message: "idempotency inserted row missing reservation token"
                                .to_string(),
                        })?;
                return Ok(Some(IdempotencyBegin::Execute(
                    IdempotencyReservation::for_store_with_token(
                        scope,
                        key,
                        request_fingerprint,
                        reservation_token,
                    ),
                )));
            }

            let row = client
                .query_opt(
                    r#"SELECT state, request_fingerprint, status_code, response_body_json
                       FROM idempotency_records WHERE scope = $1 AND key_hash = $2"#,
                    &[&scope, &key_hash],
                )
                .await
                .map_err(|error| postgres_error("idempotency load row", error))?;

            match row {
                Some(r) => Ok(Some(classify_row(r, request_fingerprint)?)),
                None if retry_miss => Err(VfsError::ObjectWriteConflict {
                    message: "idempotency insert conflict without resolvable backend row"
                        .to_string(),
                }),
                None => Ok(None),
            }
        }

        if let Some(begin) = try_insert_then_load(
            &tx,
            insert_sql,
            scope,
            key_hash,
            key,
            request_fingerprint,
            false,
        )
        .await?
        {
            tx.commit()
                .await
                .map_err(|error| postgres_error("idempotency begin commit", error))?;
            return Ok(begin);
        }

        let second = try_insert_then_load(
            &tx,
            insert_sql,
            scope,
            key_hash,
            key,
            request_fingerprint,
            true,
        )
        .await?;

        let begin = second.ok_or_else(|| VfsError::ObjectWriteConflict {
            message: "idempotency reservation failed after retries".to_string(),
        })?;

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
        let client = self.connect_client().await?;
        let status_i32 = i32::from(status_code);

        let n = client
            .execute(
                r#"UPDATE idempotency_records
                   SET state = 'completed',
                       status_code = $5,
                       response_body_json = $6,
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
                ],
            )
            .await
            .map_err(|error| postgres_error("idempotency complete update", error))?;

        if n == 1 {
            return Ok(());
        }
        Err(VfsError::InvalidArgs {
            message: "idempotency reservation is not pending".to_string(),
        })
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        match self.abort_idempotency_reservation_inner(reservation).await {
            Ok(()) => {}
            Err(_) => tracing::debug!("postgres idempotency abort skipped"),
        }
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
    }
}

fn cleanup_claim_kind_from_db(kind: &str) -> Result<ObjectCleanupClaimKind, VfsError> {
    match kind {
        "final_object_metadata_repair" => Ok(ObjectCleanupClaimKind::FinalObjectMetadataRepair),
        _ => Err(VfsError::CorruptStore {
            message: format!("unknown cleanup claim kind in Postgres metadata: {kind}"),
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

fn ref_cas_mismatch(name: &RefName) -> VfsError {
    VfsError::InvalidArgs {
        message: format!("ref compare-and-swap mismatch: {name}"),
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
    use std::sync::Arc;
    use std::time::Duration;

    use crate::audit::{
        AuditAction, AuditActor, AuditOutcome, AuditResource, AuditResourceKind, AuditStore,
        AuditWorkspaceContext, NewAuditEvent,
    };
    use crate::auth::ROOT_UID;
    use crate::backend::blob_object::{BlobObjectStore, ObjectMetadataRecord};
    use crate::backend::object_cleanup::{
        ObjectCleanupClaim, ObjectCleanupClaimKind, ObjectCleanupClaimRequest,
        ObjectCleanupClaimStore,
    };
    use crate::backend::{ObjectStore, ObjectWrite};
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyStore, request_fingerprint,
    };
    use crate::remote::blob::LocalBlobStore;
    use crate::vcs::{ChangeKind, MAIN_REF, PathKind, PathRecord};
    use axum::http::HeaderValue;
    use serde_json::json;
    use tokio::sync::Barrier;
    use uuid::Uuid;

    struct TestDb {
        config: Config,
        schema: String,
        store: PostgresMetadataStore,
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

            let store = PostgresMetadataStore::with_schema(config.clone(), schema.clone()).unwrap();
            Some(Self {
                config,
                schema,
                store,
            })
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

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
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
        ObjectCleanupClaimRequest {
            repo_id: repo_id.clone(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: ObjectKind::Blob,
            object_id,
            object_key: format!(
                "repos/{}/objects/blob/{}",
                repo_id.as_str(),
                object_id.to_hex()
            ),
            lease_owner: "postgres-worker".to_string(),
            lease_duration,
        }
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

    #[tokio::test]
    async fn postgres_metadata_store_round_trips_backend_contracts() {
        let Some(test_db) = TestDb::new().await else {
            return;
        };

        let result = run_backend_contracts(&test_db.store).await;
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

        Ok(())
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

        let replay = match store.begin(scope, &key, &request_a).await? {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay, got {other:?}"),
        };
        assert_eq!(replay.status_code, 201);
        assert_eq!(replay.response_body, json!({"run_id": "run_123"}));

        assert!(matches!(
            store.begin(scope, &key, &request_b).await?,
            IdempotencyBegin::Conflict
        ));

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
        assert_eq!(sequences, vec![3, 4]);

        let final_recent = AuditStore::list_recent(store, 10).await?;
        assert_eq!(
            final_recent
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
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
                repo_id,
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

        run_idempotency_contracts(store).await?;

        run_audit_contracts(store).await?;

        Ok(())
    }
}
