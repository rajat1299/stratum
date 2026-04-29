use async_trait::async_trait;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::auth::Uid;
use crate::error::VfsError;

const WORKSPACE_METADATA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    pub id: Uuid,
    pub name: String,
    pub root_path: String,
    pub head_commit: Option<String>,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceTokenRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub agent_uid: Uid,
    pub secret_hash: String,
}

#[derive(Debug, Clone)]
pub struct IssuedWorkspaceToken {
    pub token: WorkspaceTokenRecord,
    pub raw_secret: String,
}

#[derive(Debug, Clone)]
pub struct ValidWorkspaceToken {
    pub workspace: WorkspaceRecord,
    pub token: WorkspaceTokenRecord,
}

#[async_trait]
pub trait WorkspaceMetadataStore: Send + Sync {
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError>;
    async fn create_workspace(&self, name: &str, root_path: &str) -> Result<WorkspaceRecord, VfsError>;
    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError>;
    async fn update_head_commit(&self, id: Uuid, head_commit: Option<String>) -> Result<Option<WorkspaceRecord>, VfsError>;
    async fn issue_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
    ) -> Result<IssuedWorkspaceToken, VfsError>;
    async fn validate_workspace_token(
        &self,
        workspace_id: Uuid,
        raw_secret: &str,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError>;
}

#[derive(Default)]
pub struct InMemoryWorkspaceMetadataStore {
    inner: RwLock<WorkspaceMetadataState>,
}

#[derive(Clone, Default)]
struct WorkspaceMetadataState {
    workspaces: HashMap<Uuid, WorkspaceRecord>,
    tokens: HashMap<Uuid, Vec<WorkspaceTokenRecord>>,
}

impl InMemoryWorkspaceMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn hash_secret(raw_secret: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(raw_secret.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn generate_secret() -> String {
        let mut bytes = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn sorted_workspaces(state: &WorkspaceMetadataState) -> Vec<WorkspaceRecord> {
        let mut items: Vec<_> = state.workspaces.values().cloned().collect();
        items.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        items
    }
}

#[async_trait]
impl WorkspaceMetadataStore for InMemoryWorkspaceMetadataStore {
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(Self::sorted_workspaces(&guard))
    }

    async fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
    ) -> Result<WorkspaceRecord, VfsError> {
        let mut guard = self.inner.write().await;
        let record = WorkspaceRecord {
            id: Uuid::new_v4(),
            name: name.to_string(),
            root_path: root_path.to_string(),
            head_commit: None,
            version: 0,
        };
        guard.workspaces.insert(record.id, record.clone());
        Ok(record)
    }

    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.workspaces.get(&id).cloned())
    }

    async fn update_head_commit(
        &self,
        id: Uuid,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let mut guard = self.inner.write().await;
        let Some(workspace) = guard.workspaces.get_mut(&id) else {
            return Ok(None);
        };
        workspace.head_commit = head_commit;
        workspace.version += 1;
        Ok(Some(workspace.clone()))
    }

    async fn issue_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let mut guard = self.inner.write().await;
        if !guard.workspaces.contains_key(&workspace_id) {
            return Err(VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            });
        }

        let raw_secret = Self::generate_secret();
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: name.to_string(),
            agent_uid,
            secret_hash: Self::hash_secret(&raw_secret),
        };
        guard
            .tokens
            .entry(workspace_id)
            .or_default()
            .push(token.clone());
        Ok(IssuedWorkspaceToken { token, raw_secret })
    }

    async fn validate_workspace_token(
        &self,
        workspace_id: Uuid,
        raw_secret: &str,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
        let guard = self.inner.read().await;
        let Some(workspace) = guard.workspaces.get(&workspace_id).cloned() else {
            return Ok(None);
        };
        let expected = Self::hash_secret(raw_secret);
        let Some(token) = guard
            .tokens
            .get(&workspace_id)
            .and_then(|tokens| {
                tokens
                    .iter()
                    .find(|token| constant_time_eq(token.secret_hash.as_bytes(), expected.as_bytes()))
            })
            .cloned()
        else {
            return Ok(None);
        };
        Ok(Some(ValidWorkspaceToken { workspace, token }))
    }
}

pub struct LocalWorkspaceMetadataStore {
    path: PathBuf,
    _lock: WorkspaceMetadataLock,
    inner: RwLock<WorkspaceMetadataState>,
}

struct WorkspaceMetadataLock {
    path: PathBuf,
    _file: File,
}

impl Drop for WorkspaceMetadataLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedWorkspaceMetadata {
    version: u32,
    workspaces: Vec<WorkspaceRecord>,
    tokens: Vec<WorkspaceTokenRecord>,
}

impl LocalWorkspaceMetadataStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, VfsError> {
        let path = path.as_ref().to_path_buf();
        let lock = Self::acquire_lock(&path)?;
        let state = match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => WorkspaceMetadataState::default(),
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            path,
            _lock: lock,
            inner: RwLock::new(state),
        })
    }

    fn acquire_lock(path: &Path) -> Result<WorkspaceMetadataLock, VfsError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = path.with_extension("lock");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .map_err(|e| {
                VfsError::IoError(std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to acquire workspace metadata lock '{}': {e}",
                        lock_path.display()
                    ),
                ))
            })?;
        Ok(WorkspaceMetadataLock {
            path: lock_path,
            _file: file,
        })
    }

    fn decode(bytes: &[u8]) -> Result<WorkspaceMetadataState, VfsError> {
        let persisted: PersistedWorkspaceMetadata =
            bincode::deserialize(bytes).map_err(|e| VfsError::CorruptStore {
                message: format!("workspace metadata decode failed: {e}"),
            })?;
        if persisted.version != WORKSPACE_METADATA_VERSION {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "unsupported workspace metadata version {}",
                    persisted.version
                ),
            });
        }

        let mut state = WorkspaceMetadataState::default();
        for workspace in persisted.workspaces {
            state.workspaces.insert(workspace.id, workspace);
        }
        for token in persisted.tokens {
            state.tokens.entry(token.workspace_id).or_default().push(token);
        }
        Ok(state)
    }

    fn encode(state: &WorkspaceMetadataState) -> Result<Vec<u8>, VfsError> {
        let mut tokens: Vec<_> = state
            .tokens
            .values()
            .flat_map(|tokens| tokens.iter().cloned())
            .collect();
        tokens.sort_by(|a, b| {
            a.workspace_id
                .cmp(&b.workspace_id)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.id.cmp(&b.id))
        });

        let persisted = PersistedWorkspaceMetadata {
            version: WORKSPACE_METADATA_VERSION,
            workspaces: InMemoryWorkspaceMetadataStore::sorted_workspaces(state),
            tokens,
        };
        bincode::serialize(&persisted).map_err(|e| VfsError::CorruptStore {
            message: format!("workspace metadata encode failed: {e}"),
        })
    }

    fn persist_locked(&self, state: &WorkspaceMetadataState) -> Result<(), VfsError> {
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
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkspaceMetadataStore for LocalWorkspaceMetadataStore {
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(InMemoryWorkspaceMetadataStore::sorted_workspaces(&guard))
    }

    async fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
    ) -> Result<WorkspaceRecord, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let record = WorkspaceRecord {
            id: Uuid::new_v4(),
            name: name.to_string(),
            root_path: root_path.to_string(),
            head_commit: None,
            version: 0,
        };
        next.workspaces.insert(record.id, record.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(record)
    }

    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.workspaces.get(&id).cloned())
    }

    async fn update_head_commit(
        &self,
        id: Uuid,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let Some(workspace) = next.workspaces.get_mut(&id) else {
            return Ok(None);
        };
        workspace.head_commit = head_commit;
        workspace.version += 1;
        let updated = workspace.clone();
        self.persist_locked(&next)?;
        *guard = next;
        Ok(Some(updated))
    }

    async fn issue_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        if !next.workspaces.contains_key(&workspace_id) {
            return Err(VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            });
        }

        let raw_secret = InMemoryWorkspaceMetadataStore::generate_secret();
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: name.to_string(),
            agent_uid,
            secret_hash: InMemoryWorkspaceMetadataStore::hash_secret(&raw_secret),
        };
        next
            .tokens
            .entry(workspace_id)
            .or_default()
            .push(token.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(IssuedWorkspaceToken { token, raw_secret })
    }

    async fn validate_workspace_token(
        &self,
        workspace_id: Uuid,
        raw_secret: &str,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
        let guard = self.inner.read().await;
        let Some(workspace) = guard.workspaces.get(&workspace_id).cloned() else {
            return Ok(None);
        };
        let expected = InMemoryWorkspaceMetadataStore::hash_secret(raw_secret);
        let Some(token) = guard
            .tokens
            .get(&workspace_id)
            .and_then(|tokens| {
                tokens
                    .iter()
                    .find(|token| constant_time_eq(token.secret_hash.as_bytes(), expected.as_bytes()))
            })
            .cloned()
        else {
            return Ok(None);
        };
        Ok(Some(ValidWorkspaceToken { workspace, token }))
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

pub type SharedWorkspaceMetadataStore = Arc<dyn WorkspaceMetadataStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_metadata_path(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join("stratum-workspace-tests")
            .join(format!("{name}-{}.bin", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn issues_and_validates_workspace_tokens() {
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store
            .create_workspace("demo", "/incidents/demo")
            .await
            .unwrap();
        let issued = store
            .issue_workspace_token(workspace.id, "demo-token", 42)
            .await
            .unwrap();

        let valid = store
            .validate_workspace_token(workspace.id, &issued.raw_secret)
            .await
            .unwrap()
            .expect("token should validate");

        assert_eq!(valid.workspace.id, workspace.id);
        assert_eq!(valid.token.agent_uid, 42);
    }

    #[tokio::test]
    async fn durable_store_reloads_created_workspace() {
        let path = temp_metadata_path("workspace");
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store
            .create_workspace("demo", "/incidents/demo")
            .await
            .unwrap();
        drop(store);

        let reloaded = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let found = reloaded.get_workspace(workspace.id).await.unwrap().unwrap();
        assert_eq!(found.name, "demo");
        assert_eq!(found.root_path, "/incidents/demo");
    }

    #[tokio::test]
    async fn durable_store_reloads_workspace_token_without_raw_secrets() {
        let path = temp_metadata_path("token");
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store
            .create_workspace("demo", "/incidents/demo")
            .await
            .unwrap();
        let issued = store
            .issue_workspace_token(workspace.id, "agent-session", 7)
            .await
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        let file_text = String::from_utf8_lossy(&bytes);
        assert!(!file_text.contains(&issued.raw_secret));
        assert!(!file_text.contains("raw-agent-token"));
        drop(store);

        let reloaded = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let valid = reloaded
            .validate_workspace_token(workspace.id, &issued.raw_secret)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(valid.token.agent_uid, 7);
    }

    #[tokio::test]
    async fn durable_store_reloads_head_commit_and_version() {
        let path = temp_metadata_path("head");
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let updated = store
            .update_head_commit(workspace.id, Some("abc123".to_string()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.version, 1);
        drop(store);

        let reloaded = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let found = reloaded.get_workspace(workspace.id).await.unwrap().unwrap();
        assert_eq!(found.head_commit.as_deref(), Some("abc123"));
        assert_eq!(found.version, 1);
    }

    #[test]
    fn unsupported_metadata_version_returns_corrupt_store() {
        let path = temp_metadata_path("version");
        let persisted = PersistedWorkspaceMetadata {
            version: WORKSPACE_METADATA_VERSION + 1,
            workspaces: Vec::new(),
            tokens: Vec::new(),
        };
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bincode::serialize(&persisted).unwrap()).unwrap();

        let err = match LocalWorkspaceMetadataStore::open(&path) {
            Ok(_) => panic!("unsupported version should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn corrupt_metadata_bytes_return_corrupt_store() {
        let path = temp_metadata_path("corrupt");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not-workspace-metadata").unwrap();

        let err = match LocalWorkspaceMetadataStore::open(&path) {
            Ok(_) => panic!("corrupt metadata should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn durable_store_enforces_single_writer_lock() {
        let path = temp_metadata_path("lock");
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();

        let err = match LocalWorkspaceMetadataStore::open(&path) {
            Ok(_) => panic!("second writer should fail while lock is held"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::IoError(_)));

        drop(store);
        LocalWorkspaceMetadataStore::open(&path).unwrap();
    }

    #[tokio::test]
    async fn failed_persist_does_not_mutate_live_state() {
        let path = temp_metadata_path("failed-persist");
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        fs::create_dir_all(&path).unwrap();

        let err = store
            .create_workspace("demo", "/demo")
            .await
            .expect_err("rename over directory should fail");
        assert!(matches!(err, VfsError::IoError(_)));

        let workspaces = store.list_workspaces().await.unwrap();
        assert!(workspaces.is_empty());
    }
}
