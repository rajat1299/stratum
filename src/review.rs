use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::auth::Uid;
use crate::error::VfsError;
use crate::store::ObjectId;
use crate::vcs::RefName;

const REVIEW_STORE_VERSION: u32 = 1;

pub type SharedReviewStore = Arc<dyn ReviewStore>;

#[async_trait]
pub trait ReviewStore: Send + Sync {
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError>;

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError>;

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError>;

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError>;

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError>;

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError>;

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError>;

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError>;

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError>;

    async fn transition_change_request(
        &self,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedRefRule {
    pub id: Uuid,
    pub ref_name: String,
    pub required_approvals: u32,
    pub created_by: Uid,
    pub active: bool,
}

impl ProtectedRefRule {
    pub fn new(ref_name: &str, required_approvals: u32, created_by: Uid) -> Result<Self, VfsError> {
        if required_approvals == 0 {
            return Err(VfsError::InvalidArgs {
                message: "required approvals must be greater than zero".to_string(),
            });
        }

        Ok(Self {
            id: Uuid::new_v4(),
            ref_name: RefName::new(ref_name)?.into_string(),
            required_approvals,
            created_by,
            active: true,
        })
    }

    fn validate(&self) -> Result<(), VfsError> {
        RefName::new(self.ref_name.clone())?;
        validate_required_approvals(self.required_approvals)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedPathRule {
    pub id: Uuid,
    pub path_prefix: String,
    pub target_ref: Option<String>,
    pub required_approvals: u32,
    pub created_by: Uid,
    pub active: bool,
}

impl ProtectedPathRule {
    pub fn new(
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<Self, VfsError> {
        validate_required_approvals(required_approvals)?;
        let path_prefix = normalize_path_prefix(path_prefix)?;
        let target_ref = target_ref
            .map(|name| RefName::new(name).map(RefName::into_string))
            .transpose()?;

        Ok(Self {
            id: Uuid::new_v4(),
            path_prefix,
            target_ref,
            required_approvals,
            created_by,
            active: true,
        })
    }

    pub fn matches_path(&self, path: &str) -> bool {
        if !self.active {
            return false;
        }

        let Ok(path) = normalize_path_prefix(path) else {
            return false;
        };
        if self.path_prefix == "/" {
            return true;
        }
        path == self.path_prefix
            || path
                .strip_prefix(&self.path_prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    fn validate(&self) -> Result<(), VfsError> {
        normalize_path_prefix(&self.path_prefix)?;
        if let Some(target_ref) = &self.target_ref {
            RefName::new(target_ref.clone())?;
        }
        validate_required_approvals(self.required_approvals)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeRequestStatus {
    Open,
    Merged,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRequest {
    pub id: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub source_ref: String,
    pub target_ref: String,
    pub base_commit: String,
    pub head_commit: String,
    pub status: ChangeRequestStatus,
    pub created_by: Uid,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewChangeRequest {
    pub title: String,
    pub description: Option<String>,
    pub source_ref: String,
    pub target_ref: String,
    pub base_commit: String,
    pub head_commit: String,
    pub created_by: Uid,
}

impl ChangeRequest {
    pub fn new(input: NewChangeRequest) -> Result<Self, VfsError> {
        let title = input.title.trim().to_string();
        if title.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: "change request title must not be empty".to_string(),
            });
        }
        validate_commit_hex(&input.base_commit)?;
        validate_commit_hex(&input.head_commit)?;

        Ok(Self {
            id: Uuid::new_v4(),
            title,
            description: input.description,
            source_ref: RefName::new(input.source_ref)?.into_string(),
            target_ref: RefName::new(input.target_ref)?.into_string(),
            base_commit: input.base_commit,
            head_commit: input.head_commit,
            status: ChangeRequestStatus::Open,
            created_by: input.created_by,
            version: 1,
        })
    }

    fn transition(&self, status: ChangeRequestStatus) -> Result<Self, VfsError> {
        if self.status != ChangeRequestStatus::Open || status == ChangeRequestStatus::Open {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "invalid change request transition from {:?} to {:?}",
                    self.status, status
                ),
            });
        }

        let mut next = self.clone();
        next.status = status;
        next.version = next
            .version
            .checked_add(1)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: "change request version overflow".to_string(),
            })?;
        Ok(next)
    }

    fn validate(&self) -> Result<(), VfsError> {
        if self.version == 0 {
            return Err(VfsError::CorruptStore {
                message: format!("change request {} has zero version", self.id),
            });
        }
        if self.title.trim().is_empty() {
            return Err(VfsError::CorruptStore {
                message: format!("change request {} has empty title", self.id),
            });
        }
        RefName::new(self.source_ref.clone())?;
        RefName::new(self.target_ref.clone())?;
        validate_commit_hex(&self.base_commit)?;
        validate_commit_hex(&self.head_commit)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReviewState {
    protected_refs: BTreeMap<Uuid, ProtectedRefRule>,
    protected_paths: BTreeMap<Uuid, ProtectedPathRule>,
    change_requests: BTreeMap<Uuid, ChangeRequest>,
}

impl ReviewState {
    fn list_protected_ref_rules(&self) -> Vec<ProtectedRefRule> {
        self.protected_refs.values().cloned().collect()
    }

    fn list_protected_path_rules(&self) -> Vec<ProtectedPathRule> {
        self.protected_paths.values().cloned().collect()
    }

    fn list_change_requests(&self) -> Vec<ChangeRequest> {
        self.change_requests.values().cloned().collect()
    }
}

#[derive(Debug, Default)]
pub struct InMemoryReviewStore {
    inner: RwLock<ReviewState>,
}

impl InMemoryReviewStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ReviewStore for InMemoryReviewStore {
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError> {
        let rule = ProtectedRefRule::new(ref_name, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        guard.protected_refs.insert(rule.id, rule.clone());
        Ok(rule)
    }

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_ref_rules())
    }

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_refs.get(&id).cloned())
    }

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError> {
        let rule = ProtectedPathRule::new(path_prefix, target_ref, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        guard.protected_paths.insert(rule.id, rule.clone());
        Ok(rule)
    }

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_path_rules())
    }

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_paths.get(&id).cloned())
    }

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError> {
        let change = ChangeRequest::new(input)?;
        let mut guard = self.inner.write().await;
        guard.change_requests.insert(change.id, change.clone());
        Ok(change)
    }

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_change_requests())
    }

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.change_requests.get(&id).cloned())
    }

    async fn transition_change_request(
        &self,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError> {
        let mut guard = self.inner.write().await;
        let Some(current) = guard.change_requests.get(&id) else {
            return Ok(None);
        };
        let next = current.transition(status)?;
        guard.change_requests.insert(id, next.clone());
        Ok(Some(next))
    }
}

#[derive(Debug)]
pub struct LocalReviewStore {
    path: PathBuf,
    _lock: ReviewStoreLock,
    inner: RwLock<ReviewState>,
}

#[derive(Debug)]
struct ReviewStoreLock {
    path: PathBuf,
    owner_id: Uuid,
    file: Option<File>,
}

impl Drop for ReviewStoreLock {
    fn drop(&mut self) {
        let _ = self.file.take();
        let Ok(owner) = std::fs::read_to_string(&self.path) else {
            return;
        };
        if owner.trim() == self.owner_id.to_string() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedReviewStore {
    version: u32,
    protected_refs: Vec<ProtectedRefRule>,
    protected_paths: Vec<ProtectedPathRule>,
    change_requests: Vec<ChangeRequest>,
}

impl LocalReviewStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, VfsError> {
        let path = path.as_ref().to_path_buf();
        let lock = Self::acquire_lock(&path)?;
        let state = match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ReviewState::default(),
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            path,
            _lock: lock,
            inner: RwLock::new(state),
        })
    }

    fn acquire_lock(path: &Path) -> Result<ReviewStoreLock, VfsError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = path.with_extension("lock");
        let owner_id = Uuid::new_v4();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .map_err(|e| {
                VfsError::IoError(std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to acquire review store lock '{}': {e}",
                        lock_path.display()
                    ),
                ))
            })?;
        {
            use std::io::Write;
            file.write_all(owner_id.to_string().as_bytes())?;
            file.sync_all()?;
        }
        Ok(ReviewStoreLock {
            path: lock_path,
            owner_id,
            file: Some(file),
        })
    }

    fn decode(bytes: &[u8]) -> Result<ReviewState, VfsError> {
        let persisted: PersistedReviewStore =
            crate::codec::deserialize(bytes).map_err(|e| VfsError::CorruptStore {
                message: format!("review store decode failed: {e}"),
            })?;
        if persisted.version != REVIEW_STORE_VERSION {
            return Err(VfsError::CorruptStore {
                message: format!("unsupported review store version {}", persisted.version),
            });
        }

        let mut ids = HashSet::new();
        let mut state = ReviewState::default();
        for rule in persisted.protected_refs {
            reject_duplicate_id(&mut ids, rule.id)?;
            rule.validate().map_err(corrupt_record)?;
            state.protected_refs.insert(rule.id, rule);
        }
        for rule in persisted.protected_paths {
            reject_duplicate_id(&mut ids, rule.id)?;
            rule.validate().map_err(corrupt_record)?;
            state.protected_paths.insert(rule.id, rule);
        }
        for change in persisted.change_requests {
            reject_duplicate_id(&mut ids, change.id)?;
            change.validate().map_err(corrupt_record)?;
            state.change_requests.insert(change.id, change);
        }

        Ok(state)
    }

    fn encode(state: &ReviewState) -> Result<Vec<u8>, VfsError> {
        crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: state.list_protected_ref_rules(),
            protected_paths: state.list_protected_path_rules(),
            change_requests: state.list_change_requests(),
        })
        .map_err(|e| VfsError::CorruptStore {
            message: format!("review store encode failed: {e}"),
        })
    }

    fn persist_locked(&self, state: &ReviewState) -> Result<(), VfsError> {
        let bytes = Self::encode(state)?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp = self.path.with_extension(format!("tmp-{}", Uuid::new_v4()));
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(parent) = self.path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

#[async_trait]
impl ReviewStore for LocalReviewStore {
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError> {
        let rule = ProtectedRefRule::new(ref_name, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        next.protected_refs.insert(rule.id, rule.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(rule)
    }

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_ref_rules())
    }

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_refs.get(&id).cloned())
    }

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError> {
        let rule = ProtectedPathRule::new(path_prefix, target_ref, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        next.protected_paths.insert(rule.id, rule.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(rule)
    }

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_path_rules())
    }

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_paths.get(&id).cloned())
    }

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError> {
        let change = ChangeRequest::new(input)?;
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        next.change_requests.insert(change.id, change.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(change)
    }

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_change_requests())
    }

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.change_requests.get(&id).cloned())
    }

    async fn transition_change_request(
        &self,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError> {
        let mut guard = self.inner.write().await;
        let Some(current) = guard.change_requests.get(&id) else {
            return Ok(None);
        };
        let next_change = current.transition(status)?;
        let mut next = guard.clone();
        next.change_requests.insert(id, next_change.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(Some(next_change))
    }
}

fn validate_required_approvals(required_approvals: u32) -> Result<(), VfsError> {
    if required_approvals == 0 {
        return Err(VfsError::InvalidArgs {
            message: "required approvals must be greater than zero".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn normalize_path_prefix(path: &str) -> Result<String, VfsError> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }
    if path == "/" {
        return Ok(path.to_string());
    }
    if path.ends_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }

    for component in path.trim_start_matches('/').split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(VfsError::InvalidPath {
                path: path.to_string(),
            });
        }
    }
    Ok(path.to_string())
}

fn validate_commit_hex(value: &str) -> Result<(), VfsError> {
    ObjectId::from_hex(value).map(|_| ())
}

fn reject_duplicate_id(ids: &mut HashSet<Uuid>, id: Uuid) -> Result<(), VfsError> {
    if ids.insert(id) {
        return Ok(());
    }
    Err(VfsError::CorruptStore {
        message: format!("duplicate review record id {id}"),
    })
}

fn corrupt_record(error: VfsError) -> VfsError {
    VfsError::CorruptStore {
        message: format!("invalid review record: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn temp_review_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stratum_review_{}_{}_{}.bin",
            name,
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    #[test]
    fn protected_path_prefix_matching_respects_path_boundaries() {
        let root = ProtectedPathRule::new("/", None, 1, 0).unwrap();
        assert!(root.matches_path("/"));
        assert!(root.matches_path("/legal"));
        assert!(root.matches_path("/legal/draft.txt"));

        let legal = ProtectedPathRule::new("/legal", None, 2, 7).unwrap();
        assert!(legal.matches_path("/legal"));
        assert!(legal.matches_path("/legal/draft.txt"));
        assert!(!legal.matches_path("/legalese"));
        assert!(!legal.matches_path("/legal/../legal/draft.txt"));
    }

    #[test]
    fn rules_reject_invalid_refs_and_path_prefixes() {
        assert!(ProtectedRefRule::new("refs/heads/main", 1, 0).is_err());
        assert!(ProtectedPathRule::new("relative/path", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("/legal/../secret", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("/legal/", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("/legal", Some("bad/ref/name"), 1, 0).is_err());

        let input = NewChangeRequest {
            title: "Review release".to_string(),
            description: None,
            source_ref: "refs/heads/topic".to_string(),
            target_ref: "main".to_string(),
            base_commit: "a".repeat(64),
            head_commit: "b".repeat(64),
            created_by: 0,
        };
        assert!(ChangeRequest::new(input).is_err());
    }

    #[tokio::test]
    async fn in_memory_store_creates_lists_gets_and_transitions_change_requests() {
        let store = InMemoryReviewStore::new();

        let ref_rule = store
            .create_protected_ref_rule("main", 2, 10)
            .await
            .unwrap();
        let path_rule = store
            .create_protected_path_rule("/legal", Some("main"), 1, 10)
            .await
            .unwrap();
        let change = store
            .create_change_request(NewChangeRequest {
                title: "Legal update".to_string(),
                description: Some("Needs review".to_string()),
                source_ref: "review/legal-update".to_string(),
                target_ref: "main".to_string(),
                base_commit: "a".repeat(64),
                head_commit: "b".repeat(64),
                created_by: 10,
            })
            .await
            .unwrap();

        assert_eq!(ref_rule.ref_name, "main");
        assert!(path_rule.matches_path("/legal/draft.txt"));
        assert_eq!(change.status, ChangeRequestStatus::Open);
        assert_eq!(change.version, 1);

        assert_eq!(store.list_protected_ref_rules().await.unwrap().len(), 1);
        assert_eq!(store.list_protected_path_rules().await.unwrap().len(), 1);
        assert_eq!(store.list_change_requests().await.unwrap().len(), 1);
        assert_eq!(
            store
                .get_change_request(change.id)
                .await
                .unwrap()
                .unwrap()
                .title,
            "Legal update"
        );

        let rejected = store
            .transition_change_request(change.id, ChangeRequestStatus::Rejected)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rejected.status, ChangeRequestStatus::Rejected);
        assert_eq!(rejected.version, 2);

        assert!(
            store
                .transition_change_request(change.id, ChangeRequestStatus::Merged)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_store_reloads_rules_and_change_requests() {
        let path = temp_review_path("reload");
        let store = LocalReviewStore::open(&path).unwrap();

        let ref_rule = store
            .create_protected_ref_rule("main", 2, 11)
            .await
            .unwrap();
        let path_rule = store
            .create_protected_path_rule("/legal", Some("main"), 1, 11)
            .await
            .unwrap();
        let change = store
            .create_change_request(NewChangeRequest {
                title: "Legal update".to_string(),
                description: None,
                source_ref: "review/legal-update".to_string(),
                target_ref: "main".to_string(),
                base_commit: "a".repeat(64),
                head_commit: "b".repeat(64),
                created_by: 11,
            })
            .await
            .unwrap();
        drop(store);

        let reloaded = LocalReviewStore::open(&path).unwrap();
        assert_eq!(
            reloaded.list_protected_ref_rules().await.unwrap()[0].id,
            ref_rule.id
        );
        assert_eq!(
            reloaded.list_protected_path_rules().await.unwrap()[0].id,
            path_rule.id
        );
        assert_eq!(
            reloaded
                .get_change_request(change.id)
                .await
                .unwrap()
                .unwrap()
                .id,
            change.id
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_store_rejects_corrupt_bytes() {
        let path = temp_review_path("corrupt");
        fs::write(&path, b"not-review").unwrap();

        let err = LocalReviewStore::open(&path).expect_err("corrupt store should fail");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_store_enforces_single_writer_lock() {
        let path = temp_review_path("lock");
        let first = LocalReviewStore::open(&path).unwrap();
        let err = LocalReviewStore::open(&path).expect_err("second writer should fail");
        assert!(matches!(err, crate::error::VfsError::IoError(_)));
        drop(first);

        let second = LocalReviewStore::open(&path).unwrap();
        drop(second);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn local_store_rejects_duplicate_record_ids() {
        let path = temp_review_path("duplicate");
        let duplicate_id = Uuid::new_v4();
        let mut ref_rule = ProtectedRefRule::new("main", 1, 0).unwrap();
        ref_rule.id = duplicate_id;
        let mut path_rule = ProtectedPathRule::new("/legal", None, 1, 0).unwrap();
        path_rule.id = duplicate_id;
        let bytes = crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: vec![ref_rule],
            protected_paths: vec![path_rule],
            change_requests: Vec::new(),
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let err = LocalReviewStore::open(&path).expect_err("duplicate IDs should fail");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }
}
