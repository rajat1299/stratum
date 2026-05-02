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
use std::time::SystemTime;
use tokio::sync::RwLock;
use uuid::Uuid;

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
}

#[derive(Debug, Default)]
pub struct InMemoryObjectMetadataStore {
    inner: RwLock<BTreeMap<(RepoId, ObjectId), ObjectMetadataRecord>>,
}

impl InMemoryObjectMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectMetadataStore for InMemoryObjectMetadataStore {
    async fn put(&self, record: ObjectMetadataRecord) -> Result<ObjectMetadataRecord, VfsError> {
        let key = (record.repo_id.clone(), record.id);
        let mut guard = self.inner.write().await;
        if let Some(existing) = guard.get(&key) {
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

        guard.insert(key, record.clone());
        Ok(record)
    }

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
    ) -> Result<Option<ObjectMetadataRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.get(&(repo_id.clone(), id)).cloned())
    }
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
}

pub fn object_key(repo_id: &RepoId, kind: ObjectKind, id: &ObjectId) -> String {
    format!(
        "repos/{}/objects/{}/{}",
        repo_id.as_str(),
        object_kind_segment(kind),
        id.to_hex()
    )
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
