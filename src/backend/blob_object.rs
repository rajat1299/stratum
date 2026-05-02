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
use tokio::sync::RwLock;

use crate::backend::{ObjectStore, RepoId};
use crate::error::VfsError;
use crate::remote::blob::RemoteBlobStore;
use crate::store::{ObjectId, ObjectKind};

use super::{ObjectWrite, StoredObject};

pub type SharedObjectMetadataStore = Arc<dyn ObjectMetadataStore>;

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
        // Scaffold adapter: production backends need staged uploads or orphan
        // cleanup if metadata persistence fails after this immutable write.
        self.blobs
            .put_bytes(&record.object_key, write.bytes.clone())
            .await?;
        self.metadata.put(record.clone()).await?;

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

fn object_kind_segment(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
    }
}

impl BlobObjectStore {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ObjectWrite;

    #[derive(Default)]
    struct MemoryBlobStore {
        inner: RwLock<BTreeMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl RemoteBlobStore for MemoryBlobStore {
        async fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), VfsError> {
            self.inner.write().await.insert(key.to_string(), data);
            Ok(())
        }

        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
            self.inner
                .read()
                .await
                .get(key)
                .cloned()
                .ok_or_else(|| VfsError::IoError(std::io::ErrorKind::NotFound.into()))
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
        let (store, _, _) = fixture();
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
}
