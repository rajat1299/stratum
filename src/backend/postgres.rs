//! Postgres-backed metadata adapters for durable backend contracts.
//!
//! This module is gated behind the `postgres` feature and is not wired into
//! the server runtime. It proves the current Postgres schema can satisfy the
//! object metadata, cleanup claim, commit metadata, ref compare-and-swap,
//! idempotency, audit, workspace metadata, and review-store contracts.

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use chrono::{DateTime, Utc};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, Config, GenericClient, IsolationLevel, NoTls, Row};
use uuid::Uuid;

use crate::audit::{
    AuditAction, AuditActor, AuditEvent, AuditOutcome, AuditResource, AuditStore,
    AuditWorkspaceContext, NewAuditEvent,
};
use crate::auth::Uid;
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
    IssuedWorkspaceToken, ValidWorkspaceToken, WorkspaceMetadataStore, WorkspaceRecord,
    WorkspaceTokenRecord, generate_workspace_token_secret, hash_workspace_token_secret,
    normalize_optional_workspace_session_ref, normalize_workspace_ref,
    normalize_workspace_token_prefixes, workspace_record, workspace_token_hash_eq,
};

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

    pub(crate) async fn ensure_control_plane_ready(&self) -> Result<(), VfsError> {
        let client = self.connect_client().await?;
        client
            .batch_execute(
                "SELECT id, name, created_at
                 FROM repos
                 LIMIT 0;
                 SELECT id, repo_id, name, root_path, head_commit, version, base_ref, session_ref, created_at
                 FROM workspaces
                 LIMIT 0;
                 SELECT id, workspace_id, name, agent_uid, secret_hash, read_prefixes_json, write_prefixes_json, created_at
                 FROM workspace_tokens
                 LIMIT 0;
                 SELECT scope, key_hash, request_fingerprint, state, status_code, response_body_json, reserved_at, created_at, completed_at
                 FROM idempotency_records
                 LIMIT 0;
                 SELECT id, repo_id, sequence, created_at, actor_json, workspace_json, action, resource_json, outcome, details_json
                 FROM audit_events
                 LIMIT 0;
                 SELECT id, repo_id, ref_name, required_approvals, created_by, active, created_at
                 FROM protected_ref_rules
                 LIMIT 0;
                 SELECT id, repo_id, path_prefix, target_ref, required_approvals, created_by, active, created_at
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
            .map_err(|error| postgres_error("durable control-plane readiness", error))?;
        Ok(())
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

    Ok(WorkspaceTokenRecord {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        agent_uid: i32_to_uid(agent_uid)?,
        secret_hash: row.get("secret_hash"),
        read_prefixes,
        write_prefixes,
    })
}

#[async_trait]
impl WorkspaceMetadataStore for PostgresMetadataStore {
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
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
                   RETURNING id, name, root_path, head_commit, version, base_ref, session_ref"#,
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

    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1"#,
                &[&id],
            )
            .await
            .map_err(|error| postgres_error("workspace get", error))?;
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
                   RETURNING id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&id, &head_commit],
            )
            .await
            .map_err(|error| postgres_error("workspace update head", error))?;
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
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1
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
                           id, workspace_id, name, agent_uid, secret_hash,
                           read_prefixes_json, write_prefixes_json
                       )
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       ON CONFLICT (workspace_id, secret_hash) DO NOTHING
                       RETURNING id, workspace_id, name, agent_uid, secret_hash,
                                 read_prefixes_json, write_prefixes_json"#,
                    &[
                        &token_id,
                        &workspace_id,
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

    async fn validate_workspace_token(
        &self,
        workspace_id: Uuid,
        raw_secret: &str,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
        let client = self.connect_client().await?;
        let workspace_row = client
            .query_opt(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1"#,
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
                r#"SELECT id, workspace_id, name, agent_uid, secret_hash,
                          read_prefixes_json, write_prefixes_json
                   FROM workspace_tokens
                   WHERE workspace_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token validate token", error))?;

        for row in rows {
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
                return Ok(Some(ValidWorkspaceToken { workspace, token }));
            }
        }

        Ok(None)
    }
}

fn review_repo_id() -> RepoId {
    RepoId::local()
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
        ref_name: row.get("ref_name"),
        required_approvals,
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
        path_prefix: row.get("path_prefix"),
        target_ref: row.get("target_ref"),
        required_approvals,
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
    id: Uuid,
) -> Result<Option<ChangeRequest>, VfsError>
where
    C: GenericClient + Sync,
{
    let repo_id = review_repo_id();
    let row = client
        .query_opt(
            r#"SELECT id, title, description, source_ref, target_ref, base_commit,
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
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError> {
        let rule = ProtectedRefRule::new(ref_name, required_approvals, created_by)?;
        let client = self.connect_client().await?;
        ensure_repo(&client, &review_repo_id()).await?;
        let created_by = uid_to_i32(rule.created_by)?;
        let required =
            i32::try_from(rule.required_approvals).map_err(|_| VfsError::InvalidArgs {
                message: "required approvals exceeds Postgres INTEGER range".to_string(),
            })?;
        let row = client
            .query_one(
                r#"INSERT INTO protected_ref_rules (id, repo_id, ref_name, required_approvals, created_by, active)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   RETURNING id, ref_name, required_approvals, created_by, active"#,
                &[
                    &rule.id,
                    &review_repo_id().as_str(),
                    &rule.ref_name,
                    &required,
                    &created_by,
                    &rule.active,
                ],
            )
            .await
            .map_err(|error| postgres_error("review protected ref insert", error))?;
        row_to_protected_ref_rule(row)
    }

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, ref_name, required_approvals, created_by, active
                   FROM protected_ref_rules
                   WHERE repo_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&review_repo_id().as_str()],
            )
            .await
            .map_err(|error| postgres_error("review protected ref list", error))?;
        rows.into_iter().map(row_to_protected_ref_rule).collect()
    }

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, ref_name, required_approvals, created_by, active
                   FROM protected_ref_rules
                   WHERE repo_id = $1 AND id = $2"#,
                &[&review_repo_id().as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review protected ref get", error))?;
        row.map(row_to_protected_ref_rule).transpose()
    }

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError> {
        let rule = ProtectedPathRule::new(path_prefix, target_ref, required_approvals, created_by)?;
        let client = self.connect_client().await?;
        ensure_repo(&client, &review_repo_id()).await?;
        let created_by = uid_to_i32(rule.created_by)?;
        let required =
            i32::try_from(rule.required_approvals).map_err(|_| VfsError::InvalidArgs {
                message: "required approvals exceeds Postgres INTEGER range".to_string(),
            })?;
        let row = client
            .query_one(
                r#"INSERT INTO protected_path_rules (
                       id, repo_id, path_prefix, target_ref, required_approvals, created_by, active
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   RETURNING id, path_prefix, target_ref, required_approvals, created_by, active"#,
                &[
                    &rule.id,
                    &review_repo_id().as_str(),
                    &rule.path_prefix,
                    &rule.target_ref,
                    &required,
                    &created_by,
                    &rule.active,
                ],
            )
            .await
            .map_err(|error| postgres_error("review protected path insert", error))?;
        row_to_protected_path_rule(row)
    }

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, path_prefix, target_ref, required_approvals, created_by, active
                   FROM protected_path_rules
                   WHERE repo_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&review_repo_id().as_str()],
            )
            .await
            .map_err(|error| postgres_error("review protected path list", error))?;
        rows.into_iter().map(row_to_protected_path_rule).collect()
    }

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, path_prefix, target_ref, required_approvals, created_by, active
                   FROM protected_path_rules
                   WHERE repo_id = $1 AND id = $2"#,
                &[&review_repo_id().as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review protected path get", error))?;
        row.map(row_to_protected_path_rule).transpose()
    }

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError> {
        let change = ChangeRequest::new(input)?;
        let client = self.connect_client().await?;
        ensure_repo(&client, &review_repo_id()).await?;
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
                   RETURNING id, title, description, source_ref, target_ref, base_commit,
                             head_commit, status, created_by, version"#,
                &[
                    &change.id,
                    &review_repo_id().as_str(),
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

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&review_repo_id().as_str()],
            )
            .await
            .map_err(|error| postgres_error("review change request list", error))?;
        rows.into_iter().map(row_to_change_request).collect()
    }

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError> {
        load_review_change_request(&self.connect_client().await?, id).await
    }

    async fn transition_change_request(
        &self,
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
                r#"SELECT id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&review_repo_id().as_str(), &id],
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
                   RETURNING id, title, description, source_ref, target_ref, base_commit,
                             head_commit, status, created_by, version"#,
                &[&status_db, &version, &review_repo_id().as_str(), &id],
            )
            .await
            .map_err(|error| postgres_error("review change transition update", error))?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("review change transition commit", error))?;
        row.map(row_to_change_request).transpose()
    }

    async fn create_approval(
        &self,
        input: NewApprovalRecord,
    ) -> Result<ApprovalRecordMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review approval transaction", error))?;
        let change_row = tx
            .query_opt(
                r#"SELECT id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&review_repo_id().as_str(), &input.change_request_id],
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

    async fn list_approvals(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ApprovalRecord>, VfsError> {
        let client = self.connect_client().await?;
        let Some(change) = load_review_change_request(&client, change_request_id).await? else {
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

    async fn assign_reviewer(
        &self,
        input: NewReviewAssignment,
    ) -> Result<ReviewAssignmentMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review assignment transaction", error))?;
        let change_row = tx
            .query_opt(
                r#"SELECT id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&review_repo_id().as_str(), &input.change_request_id],
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

    async fn list_reviewer_assignments(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ReviewAssignment>, VfsError> {
        let client = self.connect_client().await?;
        let Some(change) = load_review_change_request(&client, change_request_id).await? else {
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

    async fn create_comment(
        &self,
        input: NewReviewComment,
    ) -> Result<ReviewCommentMutation, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("review comment transaction", error))?;
        let change_row = tx
            .query_opt(
                r#"SELECT id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&review_repo_id().as_str(), &input.change_request_id],
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

    async fn list_comments(&self, change_request_id: Uuid) -> Result<Vec<ReviewComment>, VfsError> {
        let client = self.connect_client().await?;
        let Some(change) = load_review_change_request(&client, change_request_id).await? else {
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

    async fn dismiss_approval(
        &self,
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
                r#"SELECT id, title, description, source_ref, target_ref, base_commit,
                          head_commit, status, created_by, version
                   FROM change_requests
                   WHERE repo_id = $1 AND id = $2
                   FOR UPDATE"#,
                &[&review_repo_id().as_str(), &input.change_request_id],
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

    async fn approval_decision(
        &self,
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
        let Some(change) = load_review_change_request(&tx, change_request_id).await? else {
            tx.commit()
                .await
                .map_err(|error| postgres_error("review decision commit", error))?;
            return Ok(None);
        };

        let ref_rows = tx
            .query(
                r#"SELECT id, ref_name, required_approvals, created_by, active
                   FROM protected_ref_rules
                   WHERE repo_id = $1
                   ORDER BY id ASC"#,
                &[&review_repo_id().as_str()],
            )
            .await
            .map_err(|error| postgres_error("review decision ref rules", error))?;
        let protected_refs: Vec<ProtectedRefRule> = ref_rows
            .into_iter()
            .map(row_to_protected_ref_rule)
            .collect::<Result<Vec<_>, _>>()?;

        let path_rows = tx
            .query(
                r#"SELECT id, path_prefix, target_ref, required_approvals, created_by, active
                   FROM protected_path_rules
                   WHERE repo_id = $1
                   ORDER BY id ASC"#,
                &[&review_repo_id().as_str()],
            )
            .await
            .map_err(|error| postgres_error("review decision path rules", error))?;
        let protected_paths: Vec<ProtectedPathRule> = path_rows
            .into_iter()
            .map(row_to_protected_path_rule)
            .collect::<Result<Vec<_>, _>>()?;

        let mut required_approvals = 0u32;
        let mut matched_ref_rules = Vec::new();
        for rule in &protected_refs {
            if rule.active && rule.ref_name == change.target_ref {
                required_approvals = required_approvals.max(rule.required_approvals);
                matched_ref_rules.push(rule.id);
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
    use crate::backend::blob_object::{BlobObjectStore, ObjectMetadataRecord, ObjectMetadataStore};
    use crate::backend::object_cleanup::{
        ObjectCleanupClaim, ObjectCleanupClaimKind, ObjectCleanupClaimRequest,
        ObjectCleanupClaimStore,
    };
    use crate::backend::{CommitRecord, CommitStore, ObjectStore, ObjectWrite, RepoId};
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyStore, request_fingerprint,
    };
    use crate::remote::blob::LocalBlobStore;
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

    fn workspace_head(label: &str) -> String {
        object_id(label.as_bytes()).to_hex()
    }

    fn is_lower_hex_sha256(value: &str) -> bool {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
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
        assert_eq!(row.get::<_, i64>("version"), 2);
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
                r#"SELECT secret_hash, read_prefixes_json, write_prefixes_json
               FROM workspace_tokens
               WHERE id = $1"#,
                &[&token_id],
            )
            .await
            .map_err(|error| postgres_error("load workspace token storage shape", error))?;

        let secret_hash: String = row.get("secret_hash");
        assert!(!secret_hash.eq(raw_secret));
        assert!(is_lower_hex_sha256(&secret_hash));

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

        let err = ReviewStore::list_approvals(store, change_request_id)
            .await
            .expect_err("corrupt active approval should be rejected");
        assert!(matches!(err, VfsError::CorruptStore { .. }));

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

        let ref_rule = ReviewStore::create_protected_ref_rule(store, "main", 2, 10).await?;
        assert_eq!(ref_rule.ref_name, "main");
        assert_eq!(ref_rule.required_approvals, 2);
        assert!(ref_rule.active);
        assert_eq!(
            ReviewStore::get_protected_ref_rule(store, ref_rule.id).await?,
            Some(ref_rule.clone())
        );

        let path_rule =
            ReviewStore::create_protected_path_rule(store, "/legal", Some("main"), 3, 10).await?;
        assert_eq!(path_rule.path_prefix, "/legal");
        assert_eq!(path_rule.target_ref.as_deref(), Some("main"));
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

        run_workspace_contracts(store).await?;

        run_review_contracts(store).await?;

        Ok(())
    }
}
