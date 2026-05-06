//! Durable backend contracts for Stratum metadata and object storage.
//!
//! This module defines the narrow object, commit, ref, idempotency, audit,
//! workspace, and review store surface future durable backends must satisfy.
//! The current server runtime is still local-backed; these contracts provide a
//! testable foundation for later Postgres and S3/R2 implementations.

pub mod blob_object;
pub(crate) mod core_transaction;
pub mod object_cleanup;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub mod postgres_migrations;
pub mod runtime;

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::audit::{InMemoryAuditStore, SharedAuditStore};
use crate::error::VfsError;
use crate::idempotency::{InMemoryIdempotencyStore, SharedIdempotencyStore};
use crate::review::{InMemoryReviewStore, SharedReviewStore};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{ChangedPath, CommitId, RefName};
use crate::workspace::{InMemoryWorkspaceMetadataStore, SharedWorkspaceMetadataStore};

pub type SharedObjectStore = Arc<dyn ObjectStore>;
pub type SharedCommitStore = Arc<dyn CommitStore>;
pub type SharedRefStore = Arc<dyn RefStore>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoId(String);

impl RepoId {
    pub fn new(value: impl Into<String>) -> Result<Self, VfsError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(VfsError::InvalidArgs {
                message: format!("invalid repo id: {value}"),
            });
        }
        Ok(Self(value))
    }

    pub fn local() -> Self {
        Self("local".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectWrite {
    pub repo_id: RepoId,
    pub id: ObjectId,
    pub kind: ObjectKind,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub repo_id: RepoId,
    pub id: ObjectId,
    pub kind: ObjectKind,
    pub bytes: Vec<u8>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError>;

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
    ) -> Result<Option<StoredObject>, VfsError>;

    async fn contains(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
    ) -> Result<bool, VfsError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub repo_id: RepoId,
    pub id: CommitId,
    pub root_tree: ObjectId,
    pub parents: Vec<CommitId>,
    pub timestamp: u64,
    pub message: String,
    pub author: String,
    pub changed_paths: Vec<ChangedPath>,
}

#[async_trait]
pub trait CommitStore: Send + Sync {
    async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError>;
    async fn get(&self, repo_id: &RepoId, id: CommitId) -> Result<Option<CommitRecord>, VfsError>;
    async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
        self.get(repo_id, id).await.map(|record| record.is_some())
    }
    async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RefVersion(u64);

impl RefVersion {
    pub fn new(value: u64) -> Result<Self, VfsError> {
        if value == 0 || value == u64::MAX {
            return Err(VfsError::InvalidArgs {
                message: format!("invalid ref version: {value}"),
            });
        }
        Ok(Self(value))
    }

    fn initial() -> Self {
        Self(1)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    fn next(self) -> Result<Self, VfsError> {
        match self.0.checked_add(1) {
            Some(next) if next < u64::MAX => Ok(Self(next)),
            _ => Err(VfsError::CorruptStore {
                message: "ref version overflow".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefExpectation {
    MustNotExist,
    Matches {
        target: CommitId,
        version: RefVersion,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefRecord {
    pub repo_id: RepoId,
    pub name: RefName,
    pub target: CommitId,
    pub version: RefVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdate {
    pub repo_id: RepoId,
    pub name: RefName,
    pub target: CommitId,
    pub expectation: RefExpectation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckedRefUpdate {
    pub repo_id: RepoId,
    pub source_name: RefName,
    pub source_expectation: RefExpectation,
    pub target_update: RefUpdate,
}

#[async_trait]
pub trait RefStore: Send + Sync {
    async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError>;
    async fn get(&self, repo_id: &RepoId, name: &RefName) -> Result<Option<RefRecord>, VfsError>;

    /// Applies a compare-and-swap ref update.
    ///
    /// Durable metadata implementations must reject targets that are not present
    /// in their commit metadata. The local memory adapter is a CAS conformance
    /// adapter and does not own commit metadata, so its tests seed synthetic IDs.
    async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError>;

    /// Applies a source freshness check and target compare-and-swap atomically.
    ///
    /// Durable metadata implementations must lock or predicate both source and
    /// target rows in one transaction so the source cannot change before commit.
    async fn update_source_checked(
        &self,
        update: SourceCheckedRefUpdate,
    ) -> Result<RefRecord, VfsError>;
}

#[derive(Clone)]
pub struct StratumStores {
    pub objects: SharedObjectStore,
    pub commits: SharedCommitStore,
    pub refs: SharedRefStore,
    pub workspace_metadata: SharedWorkspaceMetadataStore,
    pub review: SharedReviewStore,
    pub idempotency: SharedIdempotencyStore,
    pub audit: SharedAuditStore,
}

impl StratumStores {
    pub fn local_memory() -> Self {
        Self {
            objects: Arc::new(LocalMemoryObjectStore::new()),
            commits: Arc::new(LocalMemoryCommitStore::new()),
            refs: Arc::new(LocalMemoryRefStore::new()),
            workspace_metadata: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
        }
    }
}

#[derive(Debug, Default)]
pub struct LocalMemoryObjectStore {
    inner: RwLock<BTreeMap<(RepoId, ObjectId), StoredObject>>,
}

impl LocalMemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectStore for LocalMemoryObjectStore {
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

        let key = (write.repo_id.clone(), write.id);
        let mut guard = self.inner.write().await;
        if let Some(existing) = guard.get(&key) {
            if existing.kind == write.kind && existing.bytes == write.bytes {
                return Ok(existing.clone());
            }
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object {} already exists with different kind or bytes",
                    write.id.short_hex()
                ),
            });
        }

        let stored = StoredObject {
            repo_id: write.repo_id,
            id: write.id,
            kind: write.kind,
            bytes: write.bytes,
        };
        guard.insert(key, stored.clone());
        Ok(stored)
    }

    async fn get(
        &self,
        repo_id: &RepoId,
        id: ObjectId,
        expected_kind: ObjectKind,
    ) -> Result<Option<StoredObject>, VfsError> {
        let guard = self.inner.read().await;
        let Some(stored) = guard.get(&(repo_id.clone(), id)) else {
            return Ok(None);
        };
        if stored.kind != expected_kind {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "object {} has kind {:?}, expected {:?}",
                    id.short_hex(),
                    stored.kind,
                    expected_kind
                ),
            });
        }
        Ok(Some(stored.clone()))
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

#[derive(Debug, Default)]
pub struct LocalMemoryCommitStore {
    inner: RwLock<CommitState>,
}

#[derive(Debug, Default)]
struct CommitState {
    records: BTreeMap<(RepoId, CommitId), CommitRecord>,
    insertion_order: Vec<(RepoId, CommitId)>,
}

impl LocalMemoryCommitStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CommitStore for LocalMemoryCommitStore {
    async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
        let key = (record.repo_id.clone(), record.id);
        let mut guard = self.inner.write().await;
        if let Some(existing) = guard.records.get(&key) {
            if existing == &record {
                return Ok(existing.clone());
            }
            return Err(VfsError::AlreadyExists {
                path: format!("commit:{}", record.id),
            });
        }

        guard.records.insert(key.clone(), record.clone());
        guard.insertion_order.push(key);
        Ok(record)
    }

    async fn get(&self, repo_id: &RepoId, id: CommitId) -> Result<Option<CommitRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.records.get(&(repo_id.clone(), id)).cloned())
    }

    async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.records.contains_key(&(repo_id.clone(), id)))
    }

    async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard
            .insertion_order
            .iter()
            .rev()
            .filter(|(record_repo_id, _)| record_repo_id == repo_id)
            .filter_map(|key| guard.records.get(key).cloned())
            .collect())
    }
}

#[derive(Debug, Default)]
pub struct LocalMemoryRefStore {
    inner: RwLock<BTreeMap<(RepoId, RefName), RefRecord>>,
}

impl LocalMemoryRefStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RefStore for LocalMemoryRefStore {
    async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard
            .iter()
            .filter(|((record_repo_id, _), _)| record_repo_id == repo_id)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn get(&self, repo_id: &RepoId, name: &RefName) -> Result<Option<RefRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.get(&(repo_id.clone(), name.clone())).cloned())
    }

    async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
        let mut guard = self.inner.write().await;
        apply_ref_update(&mut guard, update)
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

        let mut guard = self.inner.write().await;
        check_ref_expectation(
            &guard,
            &update.repo_id,
            &update.source_name,
            update.source_expectation,
        )?;
        apply_ref_update(&mut guard, update.target_update)
    }
}

fn apply_ref_update(
    refs: &mut BTreeMap<(RepoId, RefName), RefRecord>,
    update: RefUpdate,
) -> Result<RefRecord, VfsError> {
    check_ref_expectation(refs, &update.repo_id, &update.name, update.expectation)?;
    let key = (update.repo_id.clone(), update.name.clone());
    let version = match refs.get(&key) {
        Some(current) => current.version.next()?,
        None => RefVersion::initial(),
    };
    let record = RefRecord {
        repo_id: update.repo_id,
        name: update.name,
        target: update.target,
        version,
    };
    refs.insert(key, record.clone());
    Ok(record)
}

fn check_ref_expectation(
    refs: &BTreeMap<(RepoId, RefName), RefRecord>,
    repo_id: &RepoId,
    name: &RefName,
    expectation: RefExpectation,
) -> Result<(), VfsError> {
    let current = refs.get(&(repo_id.clone(), name.clone()));
    match (current, expectation) {
        (None, RefExpectation::MustNotExist) => Ok(()),
        (Some(_), RefExpectation::MustNotExist) => Err(ref_cas_mismatch(name)),
        (Some(current), RefExpectation::Matches { target, version })
            if current.target == target && current.version == version =>
        {
            Ok(())
        }
        (Some(_) | None, RefExpectation::Matches { .. }) => Err(ref_cas_mismatch(name)),
    }
}

fn ref_cas_mismatch(name: &RefName) -> VfsError {
    VfsError::InvalidArgs {
        message: format!("ref compare-and-swap mismatch: {name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use serde_json::json;

    use crate::idempotency::{IdempotencyBegin, IdempotencyKey};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, MAIN_REF, RefName};

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    fn commit_id(name: &str) -> CommitId {
        CommitId::from(object_id(name.as_bytes()))
    }

    fn repo() -> RepoId {
        RepoId::local()
    }

    fn commit_record(id: CommitId, root_tree: ObjectId, message: &str) -> CommitRecord {
        CommitRecord {
            repo_id: repo(),
            id,
            root_tree,
            parents: Vec::new(),
            timestamp: 1,
            message: message.to_string(),
            author: "agent".to_string(),
            changed_paths: Vec::new(),
        }
    }

    #[tokio::test]
    async fn object_puts_are_idempotent_and_reads_detect_kind_mismatch() {
        let store = LocalMemoryObjectStore::new();
        let bytes = b"hello durable objects";
        let id = object_id(bytes);
        let write = ObjectWrite {
            repo_id: repo(),
            id,
            kind: ObjectKind::Blob,
            bytes: bytes.to_vec(),
        };

        let first = store.put(write.clone()).await.unwrap();
        let second = store.put(write).await.unwrap();
        assert_eq!(first, second);

        let loaded = store
            .get(&repo(), id, ObjectKind::Blob)
            .await
            .unwrap()
            .expect("object should exist");
        assert_eq!(loaded.bytes, bytes);
        assert!(store.contains(&repo(), id, ObjectKind::Blob).await.unwrap());

        let err = store
            .get(&repo(), id, ObjectKind::Tree)
            .await
            .expect_err("wrong kind should corrupt the store contract");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));

        assert!(
            store
                .get(&repo(), object_id(b"missing"), ObjectKind::Blob)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn commit_inserts_are_idempotent_and_list_newest_first() {
        let store = LocalMemoryCommitStore::new();
        let first = commit_record(commit_id("first"), object_id(b"tree-1"), "first");
        let second = commit_record(commit_id("second"), object_id(b"tree-2"), "second");

        store.insert(first.clone()).await.unwrap();
        store.insert(second.clone()).await.unwrap();
        store.insert(first.clone()).await.unwrap();

        assert_eq!(
            store.get(&repo(), first.id).await.unwrap().unwrap(),
            first.clone()
        );
        assert!(store.contains(&repo(), first.id).await.unwrap());
        assert!(!store.contains(&repo(), commit_id("missing")).await.unwrap());

        let commits = store.list(&repo()).await.unwrap();
        assert_eq!(
            commits.iter().map(|commit| commit.id).collect::<Vec<_>>(),
            vec![second.id, first.id]
        );

        let conflicting = CommitRecord {
            message: "different".to_string(),
            ..first
        };
        let err = store
            .insert(conflicting)
            .await
            .expect_err("same commit id with different metadata should conflict");
        assert!(matches!(err, crate::error::VfsError::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn ref_cas_rejects_stale_target_or_version_without_mutation() {
        let store = LocalMemoryRefStore::new();
        let main = RefName::new(MAIN_REF).unwrap();
        let first = commit_id("first");
        let second = commit_id("second");

        let created = store
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: second,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        assert_eq!(created.version, RefVersion::new(1).unwrap());

        let stale_target = store
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: first,
                expectation: RefExpectation::Matches {
                    target: first,
                    version: created.version,
                },
            })
            .await
            .expect_err("stale target should fail");
        assert!(matches!(
            stale_target,
            crate::error::VfsError::InvalidArgs { .. }
        ));
        assert_eq!(store.get(&repo(), &main).await.unwrap().unwrap(), created);

        let stale_version = store
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: first,
                expectation: RefExpectation::Matches {
                    target: second,
                    version: RefVersion::new(99).unwrap(),
                },
            })
            .await
            .expect_err("stale version should fail");
        assert!(matches!(
            stale_version,
            crate::error::VfsError::InvalidArgs { .. }
        ));
        assert_eq!(store.get(&repo(), &main).await.unwrap().unwrap(), created);
    }

    #[tokio::test]
    async fn source_checked_ref_cas_is_atomic_on_source_mismatch() {
        let store = LocalMemoryRefStore::new();
        let main = RefName::new(MAIN_REF).unwrap();
        let review = RefName::new("review/123").unwrap();
        let base = commit_id("base");
        let head = commit_id("head");
        let other = commit_id("other");

        let target = store
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: base,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        let source = store
            .update(RefUpdate {
                repo_id: repo(),
                name: review.clone(),
                target: head,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

        let err = store
            .update_source_checked(SourceCheckedRefUpdate {
                repo_id: repo(),
                source_name: review,
                source_expectation: RefExpectation::Matches {
                    target: other,
                    version: source.version,
                },
                target_update: RefUpdate {
                    repo_id: repo(),
                    name: main.clone(),
                    target: head,
                    expectation: RefExpectation::Matches {
                        target: base,
                        version: target.version,
                    },
                },
            })
            .await
            .expect_err("source mismatch should fail");
        assert!(matches!(err, crate::error::VfsError::InvalidArgs { .. }));
        assert_eq!(store.get(&repo(), &main).await.unwrap().unwrap(), target);
    }

    #[tokio::test]
    async fn stratum_stores_exposes_existing_idempotency_semantics() {
        let stores = StratumStores::local_memory();
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("backend-key")).unwrap();

        let reservation = match stores
            .idempotency
            .begin("backend:test", &key, "fingerprint-a")
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        stores
            .idempotency
            .complete(&reservation, 201, json!({"ok": true}))
            .await
            .unwrap();

        let replay = match stores
            .idempotency
            .begin("backend:test", &key, "fingerprint-a")
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay, got {other:?}"),
        };
        assert_eq!(replay.status_code, 201);
        assert_eq!(replay.response_body, json!({"ok": true}));

        assert!(matches!(
            stores
                .idempotency
                .begin("backend:test", &key, "fingerprint-b")
                .await
                .unwrap(),
            IdempotencyBegin::Conflict
        ));
    }

    #[tokio::test]
    async fn source_checked_ref_cas_models_change_request_merge() {
        let stores = StratumStores::local_memory();
        let main = RefName::new(MAIN_REF).unwrap();
        let review = RefName::new("review/merge-1").unwrap();
        let base = commit_id("base");
        let head = commit_id("head");
        let newer = commit_id("newer");

        let main_ref = stores
            .refs
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: base,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        let review_ref = stores
            .refs
            .update(RefUpdate {
                repo_id: repo(),
                name: review.clone(),
                target: head,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

        let merged = stores
            .refs
            .update_source_checked(SourceCheckedRefUpdate {
                repo_id: repo(),
                source_name: review.clone(),
                source_expectation: RefExpectation::Matches {
                    target: head,
                    version: review_ref.version,
                },
                target_update: RefUpdate {
                    repo_id: repo(),
                    name: main.clone(),
                    target: head,
                    expectation: RefExpectation::Matches {
                        target: base,
                        version: main_ref.version,
                    },
                },
            })
            .await
            .unwrap();
        assert_eq!(merged.target, head);

        let err = stores
            .refs
            .update_source_checked(SourceCheckedRefUpdate {
                repo_id: repo(),
                source_name: review,
                source_expectation: RefExpectation::Matches {
                    target: newer,
                    version: review_ref.version,
                },
                target_update: RefUpdate {
                    repo_id: repo(),
                    name: main.clone(),
                    target: newer,
                    expectation: RefExpectation::Matches {
                        target: head,
                        version: merged.version,
                    },
                },
            })
            .await
            .expect_err("stale source should fail after merge");
        assert!(matches!(err, crate::error::VfsError::InvalidArgs { .. }));
        assert_eq!(
            stores.refs.get(&repo(), &main).await.unwrap().unwrap(),
            merged
        );
    }
}
