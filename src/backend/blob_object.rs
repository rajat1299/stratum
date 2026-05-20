//! Byte-backed object storage adapter for durable backend scaffolding.
//!
//! This module bridges the existing remote byte store abstraction into the
//! backend `ObjectStore` contract. Object bytes are stored under repo-scoped,
//! kind-scoped immutable keys while object metadata remains behind a separate
//! boundary that models a future SQL `objects` table.

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::backend::core_transaction::{FinalObjectMetadataFence, FinalObjectMetadataIdentity};
use crate::backend::object_cleanup::{
    ObjectCleanupClaimKind, ObjectCleanupClaimRequest, ObjectCleanupClaimStore,
    canonical_final_object_key, is_stale_cleanup_claim,
};
use crate::backend::{ObjectStore, RepoId};
use crate::error::VfsError;
use crate::remote::blob::{BlobPutCondition, BlobPutOutcome, RemoteBlobListing, RemoteBlobStore};
use crate::store::{ObjectId, ObjectKind};

use super::{ObjectWrite, StoredObject};

pub type SharedObjectMetadataStore = Arc<dyn ObjectMetadataStore>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectOrphanCleanupMode {
    StagedUploadsOnly,
    FinalObjectsMissingMetadataDryRun,
    FinalObjectsMissingMetadataDelete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectOrphanCleanupError {
    pub key: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectOrphanCleanupReport {
    pub staged_deleted: usize,
    pub final_orphans_found: usize,
    pub final_orphans_repaired: usize,
    pub final_orphans_claim_skipped: usize,
    pub final_orphans_deleted: usize,
    pub errors: Vec<ObjectOrphanCleanupError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadataRecord {
    pub repo_id: RepoId,
    pub id: ObjectId,
    pub kind: ObjectKind,
    pub object_key: String,
    pub size: u64,
    pub sha256: String,
}

impl ObjectMetadataRecord {
    pub fn new(repo_id: RepoId, id: ObjectId, kind: ObjectKind, size: u64) -> Self {
        Self {
            object_key: object_key(&repo_id, kind, &id),
            sha256: id.to_hex(),
            repo_id,
            id,
            kind,
            size,
        }
    }

    fn from_bytes(repo_id: RepoId, id: ObjectId, kind: ObjectKind, bytes: &[u8]) -> Self {
        Self::new(repo_id, id, kind, bytes.len() as u64)
    }
}

#[async_trait]
pub trait ObjectMetadataStore: Send + Sync {
    async fn put(&self, record: ObjectMetadataRecord) -> Result<ObjectMetadataRecord, VfsError>;

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
    ) -> Result<Option<ObjectMetadataRecord>, VfsError>;

    async fn acquire_final_object_metadata_fence(
        &self,
        _request: FinalObjectMetadataFenceRequest,
    ) -> Result<Option<FinalObjectMetadataFence>, VfsError> {
        Err(VfsError::NotSupported {
            message: "final object metadata fences are not supported by this metadata store"
                .to_string(),
        })
    }

    async fn delete_with_final_object_metadata_fence(
        &self,
        _fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "final object metadata fence deletion is not supported by this metadata store"
                .to_string(),
        })
    }

    async fn validate_final_object_metadata_fence(
        &self,
        _fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message:
                "final object metadata fence validation is not supported by this metadata store"
                    .to_string(),
        })
    }

    async fn release_final_object_metadata_fence(
        &self,
        _fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "final object metadata fence release is not supported by this metadata store"
                .to_string(),
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct FinalObjectMetadataFenceRequest {
    pub repo_id: RepoId,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub canonical_final_key: String,
    pub lease_owner: String,
    pub lease_duration: Duration,
}

impl fmt::Debug for FinalObjectMetadataFenceRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinalObjectMetadataFenceRequest")
            .field("repo_id", &self.repo_id)
            .field("object_kind", &self.object_kind)
            .field("object_id", &self.object_id)
            .field("canonical_final_key", &"[redacted]")
            .field("lease_owner", &"[redacted]")
            .field("lease_duration", &self.lease_duration)
            .finish()
    }
}

impl FinalObjectMetadataFenceRequest {
    pub fn new(
        repo_id: RepoId,
        object_kind: ObjectKind,
        object_id: ObjectId,
        canonical_final_key: String,
        lease_owner: String,
        lease_duration: Duration,
    ) -> Self {
        Self {
            repo_id,
            object_kind,
            object_id,
            canonical_final_key,
            lease_owner,
            lease_duration,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), VfsError> {
        crate::backend::object_cleanup::validate_canonical_object_key(
            &self.repo_id,
            self.object_kind,
            &self.object_id,
            &self.canonical_final_key,
        )?;
        crate::backend::object_cleanup::validate_lease_owner(&self.lease_owner)?;
        if self.lease_duration.as_millis() == 0 {
            return Err(VfsError::InvalidArgs {
                message:
                    "final object metadata fence lease duration must be at least 1 millisecond"
                        .to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryObjectMetadataStore {
    inner: RwLock<InMemoryObjectMetadataState>,
    now_for_tests: RwLock<Option<SystemTime>>,
}

#[derive(Debug, Default)]
struct InMemoryObjectMetadataState {
    records: BTreeMap<(RepoId, ObjectId), ObjectMetadataRecord>,
    fences: BTreeMap<(RepoId, &'static str, ObjectId), FinalObjectMetadataFence>,
}

impl InMemoryObjectMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    async fn set_now_for_tests(&self, now: SystemTime) {
        *self.now_for_tests.write().await = Some(now);
    }

    async fn now(&self) -> SystemTime {
        self.now_for_tests
            .read()
            .await
            .unwrap_or_else(SystemTime::now)
    }
}

#[async_trait]
impl ObjectMetadataStore for InMemoryObjectMetadataStore {
    async fn put(&self, record: ObjectMetadataRecord) -> Result<ObjectMetadataRecord, VfsError> {
        let key = (record.repo_id.clone(), record.id);
        let fence_key = (
            record.repo_id.clone(),
            object_kind_segment(record.kind),
            record.id,
        );
        let now = self.now().await;
        let mut guard = self.inner.write().await;
        if guard
            .fences
            .get(&fence_key)
            .is_some_and(|fence| fence.expires_at() > now)
        {
            return Err(active_final_object_metadata_fence());
        }

        if let Some(existing) = guard.records.get(&key) {
            if existing == &record {
                return Ok(existing.clone());
            }
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object metadata for {} already exists with different attributes",
                    record.id.short_hex()
                ),
            });
        }

        guard.records.insert(key, record.clone());
        Ok(record)
    }

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
    ) -> Result<Option<ObjectMetadataRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.records.get(&(repo_id.clone(), id)).cloned())
    }

    async fn acquire_final_object_metadata_fence(
        &self,
        request: FinalObjectMetadataFenceRequest,
    ) -> Result<Option<FinalObjectMetadataFence>, VfsError> {
        request.validate()?;
        let now = self.now().await;
        let expires_at =
            now.checked_add(request.lease_duration)
                .ok_or_else(|| VfsError::InvalidArgs {
                    message: "final object metadata fence lease expiry overflow".to_string(),
                })?;
        let key = (
            request.repo_id.clone(),
            object_kind_segment(request.object_kind),
            request.object_id,
        );
        let mut guard = self.inner.write().await;
        if guard
            .fences
            .get(&key)
            .is_some_and(|fence| fence.expires_at() > now)
        {
            return Ok(None);
        }
        let created_at = guard
            .fences
            .get(&key)
            .map(FinalObjectMetadataFence::created_at)
            .unwrap_or(now);
        let metadata_identity = guard
            .records
            .get(&(request.repo_id.clone(), request.object_id))
            .map(final_object_metadata_identity);
        let fence = FinalObjectMetadataFence::for_store(
            request.repo_id,
            request.object_kind,
            request.object_id,
            request.canonical_final_key,
            request.lease_owner,
            Uuid::new_v4(),
            expires_at,
            created_at,
            now,
            metadata_identity,
        );
        guard.fences.insert(key, fence.clone());
        Ok(Some(fence))
    }

    async fn delete_with_final_object_metadata_fence(
        &self,
        fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        let now = self.now().await;
        let key = (
            fence.repo_id().clone(),
            object_kind_segment(fence.object_kind()),
            fence.object_id(),
        );
        let mut guard = self.inner.write().await;
        let Some(active) = guard.fences.get(&key) else {
            return Err(stale_final_object_metadata_fence());
        };
        if active.token() != fence.token() || active.expires_at() <= now {
            return Err(stale_final_object_metadata_fence());
        }
        let record_key = (fence.repo_id().clone(), fence.object_id());
        if let Some(record) = guard.records.get(&record_key) {
            validate_metadata_snapshot(record, fence)?;
        } else if fence.metadata_identity().is_some() {
            return Err(VfsError::ObjectWriteConflict {
                message: format!(
                    "object {} metadata disappeared while final object deletion was fenced; retry",
                    fence.object_id().short_hex()
                ),
            });
        }
        guard.records.remove(&record_key);
        Ok(())
    }

    async fn validate_final_object_metadata_fence(
        &self,
        fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        let now = self.now().await;
        let key = (
            fence.repo_id().clone(),
            object_kind_segment(fence.object_kind()),
            fence.object_id(),
        );
        let guard = self.inner.read().await;
        let Some(active) = guard.fences.get(&key) else {
            return Err(stale_final_object_metadata_fence());
        };
        if active.token() != fence.token() || active.expires_at() <= now {
            return Err(stale_final_object_metadata_fence());
        }
        if let Some(record) = guard
            .records
            .get(&(fence.repo_id().clone(), fence.object_id()))
        {
            validate_metadata_snapshot(record, fence)?;
        } else if fence.metadata_identity().is_some() {
            return Err(VfsError::ObjectWriteConflict {
                message: format!(
                    "object {} metadata disappeared while final object deletion was fenced; retry",
                    fence.object_id().short_hex()
                ),
            });
        }
        Ok(())
    }

    async fn release_final_object_metadata_fence(
        &self,
        fence: &FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        let key = (
            fence.repo_id().clone(),
            object_kind_segment(fence.object_kind()),
            fence.object_id(),
        );
        let mut guard = self.inner.write().await;
        if guard
            .fences
            .get(&key)
            .is_some_and(|active| active.token() == fence.token())
        {
            guard.fences.remove(&key);
        }
        Ok(())
    }
}

fn final_object_metadata_identity(record: &ObjectMetadataRecord) -> FinalObjectMetadataIdentity {
    FinalObjectMetadataIdentity::new(
        record.object_key.clone(),
        record.size,
        record.sha256.clone(),
    )
}

fn active_final_object_metadata_fence() -> VfsError {
    VfsError::ObjectWriteConflict {
        message: "active final object metadata deletion fence exists; retry".to_string(),
    }
}

fn stale_final_object_metadata_fence() -> VfsError {
    VfsError::ObjectWriteConflict {
        message: "final object metadata deletion fence token is stale".to_string(),
    }
}

fn validate_metadata_snapshot(
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

pub struct BlobObjectStore {
    blobs: Arc<dyn RemoteBlobStore>,
    metadata: SharedObjectMetadataStore,
}

impl fmt::Debug for BlobObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobObjectStore").finish_non_exhaustive()
    }
}

impl BlobObjectStore {
    pub fn new(blobs: Arc<dyn RemoteBlobStore>, metadata: SharedObjectMetadataStore) -> Self {
        Self { blobs, metadata }
    }

    pub async fn cleanup_orphans(
        &self,
        repo_id: &RepoId,
        older_than: SystemTime,
        mode: ObjectOrphanCleanupMode,
    ) -> Result<ObjectOrphanCleanupReport, VfsError> {
        match mode {
            ObjectOrphanCleanupMode::StagedUploadsOnly => {
                self.cleanup_staged_uploads(repo_id, older_than).await
            }
            ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDryRun
            | ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDelete => {
                self.cleanup_final_objects(repo_id, older_than, mode).await
            }
        }
    }

    pub async fn repair_final_object_metadata_orphans(
        &self,
        repo_id: &RepoId,
        older_than: SystemTime,
        claims: &dyn ObjectCleanupClaimStore,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> Result<ObjectOrphanCleanupReport, VfsError> {
        if lease_duration.as_millis() == 0 {
            return Err(VfsError::InvalidArgs {
                message: "cleanup claim lease duration must be at least 1 millisecond".to_string(),
            });
        }

        let mut report = ObjectOrphanCleanupReport::default();
        for listing in self.blobs.list_keys(&object_prefix(repo_id)).await? {
            if !listing_is_older_than(&listing, older_than) {
                continue;
            }
            let Some((kind, id)) = parse_object_key(repo_id, &listing.key) else {
                report.errors.push(ObjectOrphanCleanupError {
                    key: listing.key,
                    message: "invalid object key layout".to_string(),
                });
                continue;
            };
            if self.metadata.get(repo_id, id).await?.is_some_and(|record| {
                metadata_matches_listing(&record, repo_id, id, kind, &listing.key, listing.size)
            }) {
                continue;
            }

            report.final_orphans_found += 1;
            let claim = claims
                .claim(ObjectCleanupClaimRequest {
                    repo_id: repo_id.clone(),
                    claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
                    object_kind: kind,
                    object_id: id,
                    object_key: listing.key.clone(),
                    lease_owner: lease_owner.to_string(),
                    lease_duration,
                })
                .await?;
            let Some(claim) = claim else {
                report.final_orphans_claim_skipped += 1;
                continue;
            };

            match self
                .repair_final_object_metadata(repo_id, kind, id, &listing.key)
                .await
            {
                Ok(FinalObjectRepairOutcome::Repaired) => {
                    report.final_orphans_repaired += 1;
                    complete_cleanup_claim(claims, &claim).await?;
                }
                Ok(FinalObjectRepairOutcome::AlreadyPresent) => {
                    complete_cleanup_claim(claims, &claim).await?;
                }
                Err(error) => {
                    let message = error.to_string();
                    report.errors.push(ObjectOrphanCleanupError {
                        key: listing.key.clone(),
                        message: message.clone(),
                    });
                    record_cleanup_claim_failure(
                        claims,
                        &claim,
                        &listing.key,
                        &message,
                        &mut report,
                    )
                    .await?;
                }
            }
        }
        Ok(report)
    }
}

#[async_trait]
impl ObjectStore for BlobObjectStore {
    async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
        let computed_id = ObjectId::from_bytes(&write.bytes);
        if computed_id != write.id {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "object id {} does not match sha256(raw_bytes) {}",
                    write.id.short_hex(),
                    computed_id.short_hex()
                ),
            });
        }

        if let Some(existing) = self.metadata.get(&write.repo_id, write.id).await? {
            validate_metadata(&existing, &write.repo_id, write.id, write.kind)?;
            let stored = self.load_object(existing).await?;
            if stored.bytes == write.bytes {
                self.metadata
                    .put(ObjectMetadataRecord::from_bytes(
                        write.repo_id.clone(),
                        write.id,
                        write.kind,
                        &write.bytes,
                    ))
                    .await?;
                return Ok(stored);
            }
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object {} already exists with different bytes",
                    write.id.short_hex()
                ),
            });
        }

        let record = ObjectMetadataRecord::from_bytes(
            write.repo_id.clone(),
            write.id,
            write.kind,
            &write.bytes,
        );
        let staging_key = object_staging_key(&write.repo_id, write.kind, &write.id, Uuid::new_v4());
        match self
            .blobs
            .put_bytes_with_condition(
                &staging_key,
                write.bytes.clone(),
                BlobPutCondition::IfAbsent,
            )
            .await?
        {
            BlobPutOutcome::Written => {}
            BlobPutOutcome::AlreadyExists => {
                return Err(VfsError::ObjectWriteConflict {
                    message: format!(
                        "staged object upload key unexpectedly already exists for {}",
                        write.id.short_hex()
                    ),
                });
            }
        }

        let final_put = self
            .blobs
            .put_bytes_with_condition(
                &record.object_key,
                write.bytes.clone(),
                BlobPutCondition::IfAbsent,
            )
            .await;
        match final_put {
            Ok(BlobPutOutcome::Written) => {}
            Ok(BlobPutOutcome::AlreadyExists) => {
                if let Err(error) = self
                    .validate_existing_final_bytes(&record, &write.bytes)
                    .await
                {
                    self.delete_staging_best_effort(&staging_key).await;
                    return Err(error);
                }
            }
            Err(error) => {
                self.delete_staging_best_effort(&staging_key).await;
                return Err(error);
            }
        }

        if let Err(error) = self.metadata.put(record.clone()).await {
            self.delete_staging_best_effort(&staging_key).await;
            return Err(error);
        }
        self.delete_staging_best_effort(&staging_key).await;

        Ok(StoredObject {
            repo_id: write.repo_id,
            id: write.id,
            kind: write.kind,
            bytes: write.bytes,
        })
    }

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
    ) -> Result<Option<StoredObject>, VfsError> {
        let Some(record) = self.metadata.get(repo_id, id).await? else {
            return Ok(None);
        };
        validate_metadata(&record, repo_id, id, expected_kind)?;
        self.load_object(record).await.map(Some)
    }

    async fn contains(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
    ) -> Result<bool, VfsError> {
        self.get(repo_id, id, expected_kind)
            .await
            .map(|object| object.is_some())
    }

    async fn object_len(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
    ) -> Result<Option<u64>, VfsError> {
        let Some(record) = self.metadata.get(repo_id, id).await? else {
            return Ok(None);
        };
        validate_metadata(&record, repo_id, id, expected_kind)?;
        Ok(Some(record.size))
    }

    async fn delete_final_object_bytes(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
        expected_key: &str,
    ) -> Result<(), VfsError> {
        let canonical_key = object_key(repo_id, expected_kind, &id);
        if expected_key != canonical_key {
            return Err(VfsError::InvalidArgs {
                message: "final object delete key must match canonical object key".to_string(),
            });
        }
        self.blobs
            .delete_bytes(expected_key)
            .await
            .map_err(|_| VfsError::ObjectWriteConflict {
                message: "final object byte deletion failed; retry".to_string(),
            })
    }

    async fn final_object_bytes_present(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
        expected_key: &str,
    ) -> Result<bool, VfsError> {
        let canonical_key = object_key(repo_id, expected_kind, &id);
        if expected_key != canonical_key {
            return Err(VfsError::InvalidArgs {
                message: "final object presence key must match canonical object key".to_string(),
            });
        }
        match self.blobs.get_bytes(expected_key).await {
            Ok(_) => Ok(true),
            Err(VfsError::ObjectNotFound { .. }) => Ok(false),
            Err(_) => Err(VfsError::ObjectWriteConflict {
                message: "final object byte presence check failed; retry".to_string(),
            }),
        }
    }
}

pub fn object_key(repo_id: &RepoId, kind: ObjectKind, id: &ObjectId) -> String {
    canonical_final_object_key(repo_id, kind, id)
}

pub fn object_staging_prefix(repo_id: &RepoId) -> String {
    format!("repos/{}/object-upload-staging/", repo_id.as_str())
}

pub fn object_staging_key(
    repo_id: &RepoId,
    kind: ObjectKind,
    id: &ObjectId,
    upload_id: Uuid,
) -> String {
    format!(
        "{}{}/{}/{}",
        object_staging_prefix(repo_id),
        object_kind_segment(kind),
        id.to_hex(),
        upload_id
    )
}

fn object_prefix(repo_id: &RepoId) -> String {
    format!("repos/{}/objects/", repo_id.as_str())
}

fn object_kind_segment(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
    }
}

impl BlobObjectStore {
    async fn validate_existing_final_bytes(
        &self,
        record: &ObjectMetadataRecord,
        expected_bytes: &[u8],
    ) -> Result<(), VfsError> {
        let bytes = self
            .blobs
            .get_bytes(&record.object_key)
            .await
            .map_err(|error| unreadable_converged_object(record, error))?;
        let actual_id = ObjectId::from_bytes(&bytes);
        if bytes != expected_bytes || actual_id != record.id || bytes.len() as u64 != record.size {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object {} final key already exists with different bytes",
                    record.id.short_hex()
                ),
            });
        }
        Ok(())
    }

    async fn cleanup_staged_uploads(
        &self,
        repo_id: &RepoId,
        older_than: SystemTime,
    ) -> Result<ObjectOrphanCleanupReport, VfsError> {
        let mut report = ObjectOrphanCleanupReport::default();
        let prefix = object_staging_prefix(repo_id);
        for listing in self.blobs.list_keys(&prefix).await? {
            if !listing_is_older_than(&listing, older_than) {
                continue;
            }
            match self.blobs.delete_bytes(&listing.key).await {
                Ok(()) => report.staged_deleted += 1,
                Err(error) => report.errors.push(ObjectOrphanCleanupError {
                    key: listing.key,
                    message: error.to_string(),
                }),
            }
        }
        Ok(report)
    }

    async fn cleanup_final_objects(
        &self,
        repo_id: &RepoId,
        older_than: SystemTime,
        mode: ObjectOrphanCleanupMode,
    ) -> Result<ObjectOrphanCleanupReport, VfsError> {
        if mode == ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDelete {
            return Err(VfsError::NotSupported {
                message: "final object deletion requires a durable cleanup claim; use dry-run orphan detection"
                    .to_string(),
            });
        }

        let mut report = ObjectOrphanCleanupReport::default();
        for listing in self.blobs.list_keys(&object_prefix(repo_id)).await? {
            if !listing_is_older_than(&listing, older_than) {
                continue;
            }
            let Some((kind, id)) = parse_object_key(repo_id, &listing.key) else {
                report.errors.push(ObjectOrphanCleanupError {
                    key: listing.key,
                    message: "invalid object key layout".to_string(),
                });
                continue;
            };
            if self
                .metadata
                .get(repo_id, id)
                .await?
                .is_some_and(|record| record.kind == kind && record.object_key == listing.key)
            {
                continue;
            }
            report.final_orphans_found += 1;
        }
        Ok(report)
    }

    async fn repair_final_object_metadata(
        &self,
        repo_id: &RepoId,
        kind: ObjectKind,
        id: ObjectId,
        key: &str,
    ) -> Result<FinalObjectRepairOutcome, VfsError> {
        let bytes = self.blobs.get_bytes(key).await?;
        let actual_id = ObjectId::from_bytes(&bytes);
        if actual_id != id {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "final object key {key} contains bytes hashing to {} instead of {}",
                    actual_id.short_hex(),
                    id.short_hex(),
                ),
            });
        }

        let record = ObjectMetadataRecord::from_bytes(repo_id.clone(), id, kind, &bytes);
        if let Some(existing) = self.metadata.get(repo_id, id).await? {
            validate_metadata(&existing, repo_id, id, kind)?;
            if existing != record {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "object {} metadata differs from final object bytes",
                        id.short_hex()
                    ),
                });
            }
            return Ok(FinalObjectRepairOutcome::AlreadyPresent);
        }

        if record.object_key != key {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "final object {} key does not match repaired metadata layout",
                    id.short_hex()
                ),
            });
        }
        self.metadata.put(record).await?;
        Ok(FinalObjectRepairOutcome::Repaired)
    }

    async fn delete_staging_best_effort(&self, staging_key: &str) {
        let _ = self.blobs.delete_bytes(staging_key).await;
    }

    async fn load_object(&self, record: ObjectMetadataRecord) -> Result<StoredObject, VfsError> {
        let bytes = self
            .blobs
            .get_bytes(&record.object_key)
            .await
            .map_err(|error| unreadable_object_bytes(&record, error))?;
        let actual_size = bytes.len() as u64;
        let actual_id = ObjectId::from_bytes(&bytes);
        if actual_id != record.id || actual_id.to_hex() != record.sha256 {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object {} bytes hash to {}, expected {}",
                    record.id.short_hex(),
                    actual_id.short_hex(),
                    record.id.short_hex()
                ),
            });
        }
        if actual_size != record.size {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object {} bytes have size {}, expected {}",
                    record.id.short_hex(),
                    actual_size,
                    record.size
                ),
            });
        }

        Ok(StoredObject {
            repo_id: record.repo_id,
            id: record.id,
            kind: record.kind,
            bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalObjectRepairOutcome {
    AlreadyPresent,
    Repaired,
}

async fn complete_cleanup_claim(
    claims: &dyn ObjectCleanupClaimStore,
    claim: &crate::backend::object_cleanup::ObjectCleanupClaim,
) -> Result<(), VfsError> {
    match claims.complete(claim).await {
        Ok(()) => Ok(()),
        Err(error) if is_stale_cleanup_claim(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn record_cleanup_claim_failure(
    claims: &dyn ObjectCleanupClaimStore,
    claim: &crate::backend::object_cleanup::ObjectCleanupClaim,
    key: &str,
    original_message: &str,
    report: &mut ObjectOrphanCleanupReport,
) -> Result<(), VfsError> {
    match claims.record_failure(claim, original_message).await {
        Ok(()) => Ok(()),
        Err(error) if is_stale_cleanup_claim(&error) => Ok(()),
        Err(error) => {
            report.errors.push(ObjectOrphanCleanupError {
                key: key.to_string(),
                message: format!("failed to record cleanup claim failure: {error}"),
            });
            Ok(())
        }
    }
}

fn metadata_matches_listing(
    record: &ObjectMetadataRecord,
    repo_id: &RepoId,
    id: ObjectId,
    expected_kind: ObjectKind,
    expected_key: &str,
    listed_size: Option<u64>,
) -> bool {
    if validate_metadata(record, repo_id, id, expected_kind).is_err()
        || record.object_key != expected_key
    {
        return false;
    }
    match listed_size {
        Some(size) => record.size == size,
        None => true,
    }
}

fn listing_is_older_than(listing: &RemoteBlobListing, older_than: SystemTime) -> bool {
    listing
        .modified_at
        .is_some_and(|modified_at| modified_at <= older_than)
}

fn parse_object_key(repo_id: &RepoId, key: &str) -> Option<(ObjectKind, ObjectId)> {
    let relative = key.strip_prefix(&object_prefix(repo_id))?;
    let mut parts = relative.split('/');
    let kind = match parts.next()? {
        "blob" => ObjectKind::Blob,
        "tree" => ObjectKind::Tree,
        "commit" => ObjectKind::Commit,
        _ => return None,
    };
    let id = ObjectId::from_hex(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((kind, id))
}

fn validate_metadata(
    record: &ObjectMetadataRecord,
    repo_id: &RepoId,
    id: ObjectId,
    expected_kind: ObjectKind,
) -> Result<(), VfsError> {
    if &record.repo_id != repo_id || record.id != id {
        return Err(VfsError::CorruptStore {
            message: format!(
                "object metadata key mismatch for repo {} object {}",
                repo_id,
                id.short_hex()
            ),
        });
    }
    if record.kind != expected_kind {
        return Err(VfsError::CorruptStore {
            message: format!(
                "object {} has kind {:?}, expected {:?}",
                id.short_hex(),
                record.kind,
                expected_kind
            ),
        });
    }

    let expected_sha256 = id.to_hex();
    if record.sha256 != expected_sha256 {
        return Err(VfsError::CorruptStore {
            message: format!(
                "object {} metadata sha256 {}, expected {}",
                id.short_hex(),
                record.sha256,
                expected_sha256
            ),
        });
    }

    let expected_key = object_key(repo_id, expected_kind, &id);
    if record.object_key != expected_key {
        return Err(VfsError::CorruptStore {
            message: format!(
                "object {} metadata points to the wrong storage key",
                id.short_hex()
            ),
        });
    }

    Ok(())
}

fn unreadable_object_bytes(record: &ObjectMetadataRecord, error: VfsError) -> VfsError {
    match error {
        VfsError::IoError(error) if error.kind() == std::io::ErrorKind::NotFound => {
            VfsError::CorruptStore {
                message: format!(
                    "object {} metadata points to missing {} bytes",
                    record.id.short_hex(),
                    object_kind_segment(record.kind)
                ),
            }
        }
        VfsError::NotFound { .. } | VfsError::ObjectNotFound { .. } => VfsError::CorruptStore {
            message: format!(
                "object {} metadata points to missing {} bytes",
                record.id.short_hex(),
                object_kind_segment(record.kind)
            ),
        },
        error => error,
    }
}

fn unreadable_converged_object(record: &ObjectMetadataRecord, error: VfsError) -> VfsError {
    match error {
        VfsError::IoError(error) if error.kind() == std::io::ErrorKind::NotFound => {
            VfsError::ObjectWriteConflict {
                message: format!(
                    "object {} final key existed during conditional write but could not be read; retry",
                    record.id.short_hex()
                ),
            }
        }
        VfsError::NotFound { .. } | VfsError::ObjectNotFound { .. } => {
            VfsError::ObjectWriteConflict {
                message: format!(
                    "object {} final key existed during conditional write but could not be read; retry",
                    record.id.short_hex()
                ),
            }
        }
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ObjectWrite;
    use crate::backend::object_cleanup::{
        InMemoryObjectCleanupClaimStore, ObjectCleanupClaim, ObjectCleanupClaimKind,
        ObjectCleanupClaimRequest, ObjectCleanupClaimStore,
    };
    use std::time::{Duration, SystemTime};

    #[derive(Clone)]
    struct MemoryBlobEntry {
        bytes: Vec<u8>,
        modified_at: SystemTime,
    }

    #[derive(Default)]
    struct MemoryBlobStore {
        inner: RwLock<BTreeMap<String, MemoryBlobEntry>>,
    }

    impl MemoryBlobStore {
        async fn insert_at(&self, key: &str, bytes: Vec<u8>, modified_at: SystemTime) {
            self.inner
                .write()
                .await
                .insert(key.to_string(), MemoryBlobEntry { bytes, modified_at });
        }
    }

    #[async_trait]
    impl RemoteBlobStore for MemoryBlobStore {
        async fn put_bytes_with_condition(
            &self,
            key: &str,
            data: Vec<u8>,
            condition: BlobPutCondition,
        ) -> Result<BlobPutOutcome, VfsError> {
            let mut guard = self.inner.write().await;
            if condition == BlobPutCondition::IfAbsent && guard.contains_key(key) {
                return Ok(BlobPutOutcome::AlreadyExists);
            }
            guard.insert(
                key.to_string(),
                MemoryBlobEntry {
                    bytes: data,
                    modified_at: SystemTime::now(),
                },
            );
            Ok(BlobPutOutcome::Written)
        }

        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
            self.inner
                .read()
                .await
                .get(key)
                .map(|entry| entry.bytes.clone())
                .ok_or_else(|| VfsError::IoError(std::io::ErrorKind::NotFound.into()))
        }

        async fn delete_bytes(&self, key: &str) -> Result<(), VfsError> {
            self.inner.write().await.remove(key);
            Ok(())
        }

        async fn list_keys(&self, prefix: &str) -> Result<Vec<RemoteBlobListing>, VfsError> {
            Ok(self
                .inner
                .read()
                .await
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, entry)| RemoteBlobListing {
                    key: key.clone(),
                    size: Some(entry.bytes.len() as u64),
                    modified_at: Some(entry.modified_at),
                })
                .collect())
        }
    }

    struct FailOnceMetadataStore {
        inner: InMemoryObjectMetadataStore,
        remaining_failures: RwLock<u8>,
    }

    impl FailOnceMetadataStore {
        fn new() -> Self {
            Self {
                inner: InMemoryObjectMetadataStore::new(),
                remaining_failures: RwLock::new(1),
            }
        }
    }

    struct MetadataAppearsClaimStore {
        inner: InMemoryObjectCleanupClaimStore,
        metadata: Arc<InMemoryObjectMetadataStore>,
        record: ObjectMetadataRecord,
    }

    #[async_trait]
    impl ObjectCleanupClaimStore for MetadataAppearsClaimStore {
        async fn claim(
            &self,
            request: ObjectCleanupClaimRequest,
        ) -> Result<Option<ObjectCleanupClaim>, VfsError> {
            let claim = self.inner.claim(request).await?;
            if claim.is_some() {
                self.metadata.put(self.record.clone()).await?;
            }
            Ok(claim)
        }

        async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
            self.inner.complete(claim).await
        }

        async fn record_failure(
            &self,
            claim: &ObjectCleanupClaim,
            message: &str,
        ) -> Result<(), VfsError> {
            self.inner.record_failure(claim, message).await
        }
    }

    #[derive(Default)]
    struct FinalConflictBlobStore {
        inner: MemoryBlobStore,
    }

    #[async_trait]
    impl RemoteBlobStore for FinalConflictBlobStore {
        async fn put_bytes_with_condition(
            &self,
            key: &str,
            data: Vec<u8>,
            condition: BlobPutCondition,
        ) -> Result<BlobPutOutcome, VfsError> {
            if key.contains("/objects/") && condition == BlobPutCondition::IfAbsent {
                return Err(VfsError::ObjectWriteConflict {
                    message: "injected final object conflict".to_string(),
                });
            }
            self.inner
                .put_bytes_with_condition(key, data, condition)
                .await
        }

        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
            self.inner.get_bytes(key).await
        }

        async fn delete_bytes(&self, key: &str) -> Result<(), VfsError> {
            self.inner.delete_bytes(key).await
        }

        async fn list_keys(&self, prefix: &str) -> Result<Vec<RemoteBlobListing>, VfsError> {
            self.inner.list_keys(prefix).await
        }
    }

    #[async_trait]
    impl ObjectMetadataStore for FailOnceMetadataStore {
        async fn put(
            &self,
            record: ObjectMetadataRecord,
        ) -> Result<ObjectMetadataRecord, VfsError> {
            let mut failures = self.remaining_failures.write().await;
            if *failures > 0 {
                *failures -= 1;
                return Err(VfsError::IoError(std::io::Error::other(
                    "injected metadata failure",
                )));
            }
            drop(failures);
            self.inner.put(record).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
        ) -> Result<Option<ObjectMetadataRecord>, VfsError> {
            self.inner.get(repo_id, id).await
        }
    }

    fn repo() -> RepoId {
        RepoId::new("repo_test").unwrap()
    }

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    fn fixture() -> (
        BlobObjectStore,
        Arc<InMemoryObjectMetadataStore>,
        Arc<MemoryBlobStore>,
    ) {
        let blobs = Arc::new(MemoryBlobStore::default());
        let metadata = Arc::new(InMemoryObjectMetadataStore::new());
        (
            BlobObjectStore::new(blobs.clone(), metadata.clone()),
            metadata,
            blobs,
        )
    }

    fn write(bytes: &[u8], kind: ObjectKind) -> ObjectWrite {
        ObjectWrite {
            repo_id: repo(),
            id: object_id(bytes),
            kind,
            bytes: bytes.to_vec(),
        }
    }

    fn claim_request(
        kind: ObjectKind,
        id: ObjectId,
        key: &str,
        lease_duration: Duration,
    ) -> ObjectCleanupClaimRequest {
        ObjectCleanupClaimRequest {
            repo_id: repo(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: kind,
            object_id: id,
            object_key: key.to_string(),
            lease_owner: "repair-worker".to_string(),
            lease_duration,
        }
    }

    #[tokio::test]
    async fn put_should_round_trip_idempotently_when_bytes_match() {
        let (store, _, blobs) = fixture();
        let write = write(b"hello byte backed objects", ObjectKind::Blob);

        let first = store.put(write.clone()).await.unwrap();
        let second = store.put(write.clone()).await.unwrap();
        let loaded = store
            .get(&repo(), write.id, ObjectKind::Blob)
            .await
            .unwrap()
            .expect("object should exist");

        assert_eq!(first, second);
        assert_eq!(loaded.bytes, write.bytes);
        assert!(
            store
                .contains(&repo(), write.id, ObjectKind::Blob)
                .await
                .unwrap()
        );
        assert!(
            blobs
                .list_keys(&object_staging_prefix(&repo()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn put_should_reject_object_id_mismatch() {
        let (store, _, _) = fixture();
        let err = store
            .put(ObjectWrite {
                repo_id: repo(),
                id: object_id(b"different bytes"),
                kind: ObjectKind::Blob,
                bytes: b"actual bytes".to_vec(),
            })
            .await
            .expect_err("mismatched id should be rejected");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn get_should_return_corruption_when_expected_kind_differs() {
        let (store, _, _) = fixture();
        let write = write(b"typed object bytes", ObjectKind::Blob);

        store.put(write.clone()).await.unwrap();
        let err = store
            .get(&repo(), write.id, ObjectKind::Tree)
            .await
            .expect_err("wrong expected kind should corrupt the object contract");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[tokio::test]
    async fn put_should_reject_existing_object_with_different_kind() {
        let (store, _, _) = fixture();
        let blob = write(b"same id different kind", ObjectKind::Blob);

        store.put(blob.clone()).await.unwrap();
        let err = store
            .put(ObjectWrite {
                kind: ObjectKind::Tree,
                ..blob
            })
            .await
            .expect_err("same object id with different kind should be corruption");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[tokio::test]
    async fn get_should_return_none_when_metadata_is_missing() {
        let (store, _, _) = fixture();
        let missing = store
            .get(&repo(), object_id(b"missing"), ObjectKind::Blob)
            .await
            .unwrap();

        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn get_should_return_corruption_when_remote_bytes_are_missing() {
        let (store, metadata, _) = fixture();
        let id = object_id(b"metadata-only object");
        metadata
            .put(ObjectMetadataRecord::new(
                repo(),
                id,
                ObjectKind::Blob,
                b"metadata-only object".len() as u64,
            ))
            .await
            .unwrap();

        let err = store
            .get(&repo(), id, ObjectKind::Blob)
            .await
            .expect_err("metadata without bytes should be corruption");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[tokio::test]
    async fn get_should_return_corruption_when_remote_bytes_hash_mismatches() {
        let bytes = b"expected object bytes";
        let wrong_bytes = b"wrong object bytes";
        let id = object_id(bytes);
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let (store, metadata, blobs) = fixture();
        metadata
            .put(ObjectMetadataRecord::new(
                repo(),
                id,
                ObjectKind::Blob,
                bytes.len() as u64,
            ))
            .await
            .unwrap();
        blobs.put_bytes(&key, wrong_bytes.to_vec()).await.unwrap();

        let err = store
            .get(&repo(), id, ObjectKind::Blob)
            .await
            .expect_err("hash mismatch should corrupt the object contract");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[tokio::test]
    async fn put_should_recover_existing_final_bytes_when_metadata_is_missing() {
        let (store, metadata, blobs) = fixture();
        let write = write(b"final bytes already converged", ObjectKind::Blob);
        let key = object_key(&repo(), write.kind, &write.id);

        blobs.put_bytes(&key, write.bytes.clone()).await.unwrap();
        assert!(metadata.get(&repo(), write.id).await.unwrap().is_none());

        let stored = store.put(write.clone()).await.unwrap();

        assert_eq!(stored.bytes, write.bytes);
        assert!(metadata.get(&repo(), write.id).await.unwrap().is_some());
        assert!(
            blobs
                .list_keys(&object_staging_prefix(&repo()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn put_should_reject_existing_final_key_with_different_bytes() {
        let (store, _, blobs) = fixture();
        let write = write(b"expected final object", ObjectKind::Blob);
        let key = object_key(&repo(), write.kind, &write.id);
        blobs
            .put_bytes(&key, b"wrong final object".to_vec())
            .await
            .unwrap();

        let err = store
            .put(write)
            .await
            .expect_err("different bytes under final key should corrupt object storage");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
        assert!(
            blobs
                .list_keys(&object_staging_prefix(&repo()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn put_should_leave_final_object_and_cleanup_staging_after_metadata_failure() {
        let blobs = Arc::new(MemoryBlobStore::default());
        let metadata = Arc::new(FailOnceMetadataStore::new());
        let store = BlobObjectStore::new(blobs.clone(), metadata.clone());
        let write = write(b"metadata failure object", ObjectKind::Blob);
        let key = object_key(&repo(), write.kind, &write.id);

        let err = store
            .put(write.clone())
            .await
            .expect_err("injected metadata failure should fail put");

        assert!(matches!(err, VfsError::IoError(_)));
        assert_eq!(blobs.get_bytes(&key).await.unwrap(), write.bytes);
        assert!(metadata.get(&repo(), write.id).await.unwrap().is_none());
        assert!(
            blobs
                .list_keys(&object_staging_prefix(&repo()))
                .await
                .unwrap()
                .is_empty()
        );

        let stored = store.put(write.clone()).await.unwrap();
        assert_eq!(stored.bytes, write.bytes);
        assert!(metadata.get(&repo(), write.id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn put_should_not_insert_metadata_after_final_write_conflict() {
        let blobs = Arc::new(FinalConflictBlobStore::default());
        let metadata = Arc::new(InMemoryObjectMetadataStore::new());
        let store = BlobObjectStore::new(blobs.clone(), metadata.clone());
        let write = write(b"final conflict object", ObjectKind::Blob);

        let err = store
            .put(write.clone())
            .await
            .expect_err("final conditional write conflict should fail put");

        assert!(matches!(err, VfsError::ObjectWriteConflict { .. }));
        assert!(metadata.get(&repo(), write.id).await.unwrap().is_none());
        assert!(
            blobs
                .list_keys(&object_staging_prefix(&repo()))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn contains_should_return_corruption_when_remote_bytes_are_missing() {
        let (store, metadata, _) = fixture();
        let id = object_id(b"contains metadata only");
        metadata
            .put(ObjectMetadataRecord::new(
                repo(),
                id,
                ObjectKind::Blob,
                b"contains metadata only".len() as u64,
            ))
            .await
            .unwrap();

        let err = store
            .contains(&repo(), id, ObjectKind::Blob)
            .await
            .expect_err("contains should not hide corrupt object state");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn object_key_should_format_repo_kind_and_sha256_namespace() {
        let id = object_id(b"namespaced key");

        assert_eq!(
            object_key(&repo(), ObjectKind::Tree, &id),
            format!("repos/{}/objects/tree/{}", repo(), id.to_hex())
        );
    }

    #[test]
    fn object_staging_key_should_format_repo_kind_hash_and_upload_id() {
        let id = object_id(b"staging namespaced key");
        let upload_id = Uuid::from_u128(0x1234567890abcdef1234567890abcdef);

        assert_eq!(
            object_staging_key(&repo(), ObjectKind::Commit, &id, upload_id),
            format!(
                "repos/{}/object-upload-staging/commit/{}/{}",
                repo(),
                id.to_hex(),
                upload_id
            )
        );
    }

    #[tokio::test]
    async fn cleanup_orphans_should_delete_old_staged_uploads_only() {
        let (store, _, blobs) = fixture();
        let id = object_id(b"staged cleanup");
        let old_key = object_staging_key(&repo(), ObjectKind::Blob, &id, Uuid::new_v4());
        let recent_key = object_staging_key(&repo(), ObjectKind::Blob, &id, Uuid::new_v4());
        let final_key = object_key(&repo(), ObjectKind::Blob, &id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        blobs
            .insert_at(&old_key, b"old staging".to_vec(), SystemTime::UNIX_EPOCH)
            .await;
        blobs
            .insert_at(
                &recent_key,
                b"recent staging".to_vec(),
                cutoff + Duration::from_secs(10),
            )
            .await;
        blobs
            .insert_at(&final_key, b"final object".to_vec(), SystemTime::UNIX_EPOCH)
            .await;

        let report = store
            .cleanup_orphans(&repo(), cutoff, ObjectOrphanCleanupMode::StagedUploadsOnly)
            .await
            .unwrap();

        assert_eq!(report.staged_deleted, 1);
        assert_eq!(report.final_orphans_found, 0);
        assert!(report.errors.is_empty());
        assert!(blobs.get_bytes(&old_key).await.is_err());
        assert_eq!(
            blobs.get_bytes(&recent_key).await.unwrap(),
            b"recent staging"
        );
        assert_eq!(blobs.get_bytes(&final_key).await.unwrap(), b"final object");
    }

    #[tokio::test]
    async fn cleanup_orphans_should_dry_run_old_final_objects_missing_metadata() {
        let (store, metadata, blobs) = fixture();
        let orphan_bytes = b"orphan final bytes";
        let orphan_id = object_id(orphan_bytes);
        let orphan_key = object_key(&repo(), ObjectKind::Blob, &orphan_id);
        let retained_write = write(b"metadata-backed final bytes", ObjectKind::Blob);
        let retained_key = object_key(&repo(), ObjectKind::Blob, &retained_write.id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        blobs
            .insert_at(&orphan_key, orphan_bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;
        blobs
            .insert_at(
                &retained_key,
                retained_write.bytes.clone(),
                SystemTime::UNIX_EPOCH,
            )
            .await;
        metadata
            .put(ObjectMetadataRecord::from_bytes(
                repo(),
                retained_write.id,
                retained_write.kind,
                &retained_write.bytes,
            ))
            .await
            .unwrap();

        let dry_run = store
            .cleanup_orphans(
                &repo(),
                cutoff,
                ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDryRun,
            )
            .await
            .unwrap();
        assert_eq!(dry_run.final_orphans_found, 1);
        assert_eq!(dry_run.final_orphans_deleted, 0);
        assert_eq!(blobs.get_bytes(&orphan_key).await.unwrap(), orphan_bytes);

        let err = store
            .cleanup_orphans(
                &repo(),
                cutoff,
                ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDelete,
            )
            .await
            .expect_err("final object delete mode should fail closed without a durable claim");
        assert!(matches!(err, VfsError::NotSupported { .. }));
        assert_eq!(blobs.get_bytes(&orphan_key).await.unwrap(), orphan_bytes);
        assert_eq!(
            blobs.get_bytes(&retained_key).await.unwrap(),
            retained_write.bytes
        );
    }

    #[tokio::test]
    async fn repair_final_object_metadata_orphans_should_recreate_missing_metadata() {
        let (store, metadata, blobs) = fixture();
        let orphan_bytes = b"repairable final object bytes";
        let orphan_id = object_id(orphan_bytes);
        let orphan_key = object_key(&repo(), ObjectKind::Blob, &orphan_id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let claims = InMemoryObjectCleanupClaimStore::new();
        blobs
            .insert_at(&orphan_key, orphan_bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;

        let report = store
            .repair_final_object_metadata_orphans(
                &repo(),
                cutoff,
                &claims,
                "repair-worker",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert_eq!(report.final_orphans_found, 1);
        assert_eq!(report.final_orphans_repaired, 1);
        assert_eq!(report.final_orphans_claim_skipped, 0);
        assert_eq!(report.final_orphans_deleted, 0);
        assert!(report.errors.is_empty());
        assert_eq!(blobs.get_bytes(&orphan_key).await.unwrap(), orphan_bytes);
        assert_eq!(
            metadata.get(&repo(), orphan_id).await.unwrap(),
            Some(ObjectMetadataRecord::from_bytes(
                repo(),
                orphan_id,
                ObjectKind::Blob,
                orphan_bytes,
            ))
        );

        let completed_retry = claims
            .claim(claim_request(
                ObjectKind::Blob,
                orphan_id,
                &orphan_key,
                Duration::from_secs(60),
            ))
            .await
            .unwrap();
        assert!(completed_retry.is_none());
    }

    #[tokio::test]
    async fn repair_final_object_metadata_orphans_should_skip_active_claims() {
        let (store, metadata, blobs) = fixture();
        let orphan_bytes = b"actively claimed final object";
        let orphan_id = object_id(orphan_bytes);
        let orphan_key = object_key(&repo(), ObjectKind::Blob, &orphan_id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let claims = InMemoryObjectCleanupClaimStore::new();
        blobs
            .insert_at(&orphan_key, orphan_bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;
        claims
            .claim(claim_request(
                ObjectKind::Blob,
                orphan_id,
                &orphan_key,
                Duration::from_secs(120),
            ))
            .await
            .unwrap()
            .expect("preclaim should acquire the lease");

        let report = store
            .repair_final_object_metadata_orphans(
                &repo(),
                cutoff,
                &claims,
                "repair-worker",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert_eq!(report.final_orphans_found, 1);
        assert_eq!(report.final_orphans_repaired, 0);
        assert_eq!(report.final_orphans_claim_skipped, 1);
        assert!(report.errors.is_empty());
        assert!(metadata.get(&repo(), orphan_id).await.unwrap().is_none());
        assert_eq!(blobs.get_bytes(&orphan_key).await.unwrap(), orphan_bytes);
    }

    #[tokio::test]
    async fn repair_final_object_metadata_orphans_should_complete_if_metadata_appears_after_claim()
    {
        let (store, metadata, blobs) = fixture();
        let orphan_bytes = b"metadata appears during claim";
        let orphan_id = object_id(orphan_bytes);
        let orphan_key = object_key(&repo(), ObjectKind::Blob, &orphan_id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let claims = MetadataAppearsClaimStore {
            inner: InMemoryObjectCleanupClaimStore::new(),
            metadata: metadata.clone(),
            record: ObjectMetadataRecord::from_bytes(
                repo(),
                orphan_id,
                ObjectKind::Blob,
                orphan_bytes,
            ),
        };
        blobs
            .insert_at(&orphan_key, orphan_bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;

        let report = store
            .repair_final_object_metadata_orphans(
                &repo(),
                cutoff,
                &claims,
                "repair-worker",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert_eq!(report.final_orphans_found, 1);
        assert_eq!(report.final_orphans_repaired, 0);
        assert_eq!(report.final_orphans_claim_skipped, 0);
        assert!(report.errors.is_empty());
        assert!(metadata.get(&repo(), orphan_id).await.unwrap().is_some());
        let completed_retry = claims
            .inner
            .claim(claim_request(
                ObjectKind::Blob,
                orphan_id,
                &orphan_key,
                Duration::from_secs(60),
            ))
            .await
            .unwrap();
        assert!(completed_retry.is_none());
    }

    #[tokio::test]
    async fn repair_final_object_metadata_orphans_should_report_hash_mismatch_without_deleting() {
        let (store, metadata, blobs) = fixture();
        let expected_bytes = b"expected final object bytes";
        let wrong_bytes = b"wrong final object bytes";
        let expected_id = object_id(expected_bytes);
        let orphan_key = object_key(&repo(), ObjectKind::Blob, &expected_id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let claims = InMemoryObjectCleanupClaimStore::new();
        blobs
            .insert_at(&orphan_key, wrong_bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;

        let report = store
            .repair_final_object_metadata_orphans(
                &repo(),
                cutoff,
                &claims,
                "repair-worker",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert_eq!(report.final_orphans_found, 1);
        assert_eq!(report.final_orphans_repaired, 0);
        assert_eq!(report.final_orphans_deleted, 0);
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].message.contains("hashing to"));
        assert!(metadata.get(&repo(), expected_id).await.unwrap().is_none());
        assert_eq!(blobs.get_bytes(&orphan_key).await.unwrap(), wrong_bytes);
    }

    #[tokio::test]
    async fn repair_final_object_metadata_orphans_should_report_corrupt_existing_metadata() {
        let (store, metadata, blobs) = fixture();
        let bytes = b"final object with corrupt metadata";
        let id = object_id(bytes);
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let claims = InMemoryObjectCleanupClaimStore::new();
        let mut corrupt_record =
            ObjectMetadataRecord::from_bytes(repo(), id, ObjectKind::Blob, bytes);
        corrupt_record.sha256 = object_id(b"different bytes").to_hex();
        blobs
            .insert_at(&key, bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;
        metadata.put(corrupt_record).await.unwrap();

        let report = store
            .repair_final_object_metadata_orphans(
                &repo(),
                cutoff,
                &claims,
                "repair-worker",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert_eq!(report.final_orphans_found, 1);
        assert_eq!(report.final_orphans_repaired, 0);
        assert_eq!(report.final_orphans_deleted, 0);
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].message.contains("metadata sha256"));
        assert_eq!(blobs.get_bytes(&key).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn metadata_put_should_retry_while_final_object_deletion_fence_is_active() {
        let metadata = InMemoryObjectMetadataStore::new();
        let bytes = b"fenced metadata repair";
        let id = object_id(bytes);
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        metadata.set_now_for_tests(now).await;
        let fence = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key,
                "delete-worker".to_string(),
                Duration::from_secs(60),
            ))
            .await
            .unwrap()
            .expect("fence should be acquired");

        let err = metadata
            .put(ObjectMetadataRecord::from_bytes(
                repo(),
                id,
                ObjectKind::Blob,
                bytes,
            ))
            .await
            .expect_err("active deletion fence must block metadata repair");

        assert!(matches!(err, VfsError::ObjectWriteConflict { .. }));
        metadata
            .release_final_object_metadata_fence(&fence)
            .await
            .unwrap();
        assert!(
            metadata
                .put(ObjectMetadataRecord::from_bytes(
                    repo(),
                    id,
                    ObjectKind::Blob,
                    bytes,
                ))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn put_should_retry_existing_object_while_final_object_deletion_fence_is_active() {
        let (store, metadata, blobs) = fixture();
        let bytes = b"existing object with active fence";
        let write = write(bytes, ObjectKind::Blob);
        let id = write.id;
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.put(write.clone()).await.unwrap();
        metadata.set_now_for_tests(now).await;
        let fence = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key.clone(),
                "delete-worker".to_string(),
                Duration::from_secs(60),
            ))
            .await
            .unwrap()
            .expect("metadata-present object can be fenced");

        let err = store
            .put(write.clone())
            .await
            .expect_err("idempotent put must retry while deletion fence is active");

        assert!(matches!(err, VfsError::ObjectWriteConflict { .. }));
        assert_eq!(blobs.get_bytes(&key).await.unwrap(), bytes);
        metadata
            .release_final_object_metadata_fence(&fence)
            .await
            .unwrap();
        assert_eq!(store.put(write).await.unwrap().bytes, bytes);
    }

    #[tokio::test]
    async fn final_object_deletion_fence_can_be_reacquired_after_expiry() {
        let metadata = InMemoryObjectMetadataStore::new();
        let id = object_id(b"reacquire fence");
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        metadata.set_now_for_tests(now).await;
        let first = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key.clone(),
                "delete-worker-a".to_string(),
                Duration::from_secs(5),
            ))
            .await
            .unwrap()
            .expect("first fence should be acquired");
        assert!(
            metadata
                .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                    repo(),
                    ObjectKind::Blob,
                    id,
                    key.clone(),
                    "delete-worker-b".to_string(),
                    Duration::from_secs(5),
                ))
                .await
                .unwrap()
                .is_none()
        );

        metadata
            .set_now_for_tests(now + Duration::from_secs(6))
            .await;
        let second = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key,
                "delete-worker-b".to_string(),
                Duration::from_secs(30),
            ))
            .await
            .unwrap()
            .expect("expired fence should be reacquired");

        assert_ne!(first.token(), second.token());
        assert_eq!(first.created_at(), second.created_at());
    }

    #[tokio::test]
    async fn stale_final_object_deletion_fence_cannot_delete_metadata() {
        let metadata = InMemoryObjectMetadataStore::new();
        let bytes = b"stale fenced delete";
        let id = object_id(bytes);
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        metadata.set_now_for_tests(now).await;
        metadata
            .put(ObjectMetadataRecord::from_bytes(
                repo(),
                id,
                ObjectKind::Blob,
                bytes,
            ))
            .await
            .unwrap();
        let fence = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key.clone(),
                "delete-worker".to_string(),
                Duration::from_secs(5),
            ))
            .await
            .unwrap()
            .expect("fence should be acquired");
        metadata
            .set_now_for_tests(now + Duration::from_secs(6))
            .await;

        let err = metadata
            .delete_with_final_object_metadata_fence(&fence)
            .await
            .expect_err("expired fence must not delete metadata");

        assert!(matches!(err, VfsError::ObjectWriteConflict { .. }));
        assert!(metadata.get(&repo(), id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn metadata_present_fence_requires_metadata_snapshot_when_deleting_metadata() {
        let metadata = InMemoryObjectMetadataStore::new();
        let bytes = b"metadata snapshot delete";
        let id = object_id(bytes);
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        metadata.set_now_for_tests(now).await;
        metadata
            .put(ObjectMetadataRecord::from_bytes(
                repo(),
                id,
                ObjectKind::Blob,
                bytes,
            ))
            .await
            .unwrap();
        let fence = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key,
                "delete-worker".to_string(),
                Duration::from_secs(60),
            ))
            .await
            .unwrap()
            .expect("metadata-present object can be fenced");
        assert!(fence.metadata_identity().is_some());
        metadata
            .delete_with_final_object_metadata_fence(&fence)
            .await
            .unwrap();

        let err = metadata
            .delete_with_final_object_metadata_fence(&fence)
            .await
            .expect_err("metadata-present snapshot cannot be silently absent");

        assert!(matches!(err, VfsError::ObjectWriteConflict { .. }));
    }

    #[tokio::test]
    async fn metadata_missing_final_object_can_delete_metadata_only_with_active_fence() {
        let metadata = InMemoryObjectMetadataStore::new();
        let bytes = b"metadata missing delete";
        let id = object_id(bytes);
        let key = object_key(&repo(), ObjectKind::Blob, &id);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        metadata.set_now_for_tests(now).await;
        let fence = metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                repo(),
                ObjectKind::Blob,
                id,
                key.clone(),
                "delete-worker".to_string(),
                Duration::from_secs(60),
            ))
            .await
            .unwrap()
            .expect("metadata-missing object can be fenced");
        metadata
            .delete_with_final_object_metadata_fence(&fence)
            .await
            .unwrap();

        assert!(metadata.get(&repo(), id).await.unwrap().is_none());
    }

    #[test]
    fn final_object_metadata_fence_debug_redacts_sensitive_details() {
        let id = object_id(b"redacted fence");
        let fence = FinalObjectMetadataFence::for_store(
            repo(),
            ObjectKind::Blob,
            id,
            object_key(&repo(), ObjectKind::Blob, &id),
            "delete-worker".to_string(),
            Uuid::from_u128(0x1234567890abcdef1234567890abcdef),
            SystemTime::UNIX_EPOCH + Duration::from_secs(60),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Some(FinalObjectMetadataIdentity::new(
                "repos/repo_test/objects/blob/secret".to_string(),
                42,
                "secret-sha".to_string(),
            )),
        );

        let debug = format!("{fence:?}");

        assert!(!debug.contains("1234567890abcdef"));
        assert!(!debug.contains("/objects/blob/"));
        assert!(!debug.contains("delete-worker"));
        assert!(!debug.contains("secret-sha"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn final_object_metadata_fence_request_debug_redacts_sensitive_details() {
        let id = object_id(b"redacted fence request");
        let request = FinalObjectMetadataFenceRequest::new(
            repo(),
            ObjectKind::Blob,
            id,
            object_key(&repo(), ObjectKind::Blob, &id),
            "delete-worker".to_string(),
            Duration::from_secs(60),
        );

        let debug = format!("{request:?}");

        assert!(!debug.contains("/objects/blob/"));
        assert!(!debug.contains("delete-worker"));
        assert!(debug.contains("[redacted]"));
    }

    #[tokio::test]
    async fn cleanup_orphans_should_report_final_key_when_metadata_kind_differs() {
        let (store, metadata, blobs) = fixture();
        let bytes = b"same content id different kind";
        let id = object_id(bytes);
        let tree_key = object_key(&repo(), ObjectKind::Tree, &id);
        let cutoff = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        blobs
            .insert_at(&tree_key, bytes.to_vec(), SystemTime::UNIX_EPOCH)
            .await;
        metadata
            .put(ObjectMetadataRecord::from_bytes(
                repo(),
                id,
                ObjectKind::Blob,
                bytes,
            ))
            .await
            .unwrap();

        let report = store
            .cleanup_orphans(
                &repo(),
                cutoff,
                ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDryRun,
            )
            .await
            .unwrap();

        assert_eq!(report.final_orphans_found, 1);
        assert_eq!(report.final_orphans_deleted, 0);
        assert!(report.errors.is_empty());
    }
}
