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

const WORKSPACE_METADATA_VERSION: u32 = 2;
const LEGACY_WORKSPACE_METADATA_VERSION: u32 = 1;

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
    pub read_prefixes: Vec<String>,
    pub write_prefixes: Vec<String>,
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
    async fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
    ) -> Result<WorkspaceRecord, VfsError>;
    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError>;
    async fn update_head_commit(
        &self,
        id: Uuid,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError>;
    async fn issue_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let workspace =
            self.get_workspace(workspace_id)
                .await?
                .ok_or_else(|| VfsError::NotFound {
                    path: format!("workspace:{workspace_id}"),
                })?;
        self.issue_scoped_workspace_token(
            workspace_id,
            name,
            agent_uid,
            vec![workspace.root_path.clone()],
            vec![workspace.root_path],
        )
        .await
    }
    async fn issue_scoped_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let _ = (workspace_id, name, agent_uid, read_prefixes, write_prefixes);
        Err(VfsError::NotSupported {
            message: "scoped workspace token issuance is not supported".to_string(),
        })
    }
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

    async fn issue_scoped_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let mut guard = self.inner.write().await;
        let Some(workspace) = guard.workspaces.get(&workspace_id) else {
            return Err(VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            });
        };
        let read_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, read_prefixes)?;
        let write_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, write_prefixes)?;

        let raw_secret = Self::generate_secret();
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: name.to_string(),
            agent_uid,
            secret_hash: Self::hash_secret(&raw_secret),
            read_prefixes,
            write_prefixes,
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
                tokens.iter().find(|token| {
                    constant_time_eq(token.secret_hash.as_bytes(), expected.as_bytes())
                })
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

#[derive(Serialize, Deserialize)]
struct LegacyPersistedWorkspaceMetadata {
    version: u32,
    workspaces: Vec<WorkspaceRecord>,
    tokens: Vec<LegacyWorkspaceTokenRecord>,
}

#[derive(Serialize, Deserialize)]
struct LegacyWorkspaceTokenRecord {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    agent_uid: Uid,
    secret_hash: String,
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

    pub fn validate_workspace_token_read_only(
        path: impl AsRef<Path>,
        workspace_id: Uuid,
        raw_secret: &str,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let state = Self::decode(&bytes)?;
        let Some(workspace) = state.workspaces.get(&workspace_id).cloned() else {
            return Ok(None);
        };
        let expected = InMemoryWorkspaceMetadataStore::hash_secret(raw_secret);
        let Some(token) = state.tokens.get(&workspace_id).and_then(|tokens| {
            tokens
                .iter()
                .find(|token| constant_time_eq(token.secret_hash.as_bytes(), expected.as_bytes()))
        }) else {
            return Ok(None);
        };

        Ok(Some(ValidWorkspaceToken {
            workspace,
            token: token.clone(),
        }))
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
        match crate::codec::deserialize::<PersistedWorkspaceMetadata>(bytes) {
            Ok(persisted) if persisted.version == WORKSPACE_METADATA_VERSION => {
                Self::state_from_persisted(persisted)
            }
            Ok(persisted) if persisted.version == LEGACY_WORKSPACE_METADATA_VERSION => {
                Self::decode_legacy(bytes)
            }
            Ok(persisted) => Err(VfsError::CorruptStore {
                message: format!(
                    "unsupported workspace metadata version {}",
                    persisted.version
                ),
            }),
            Err(current_error) => match Self::decode_legacy(bytes) {
                Ok(state) => Ok(state),
                Err(legacy_error) => Err(VfsError::CorruptStore {
                    message: format!(
                        "workspace metadata decode failed: {current_error}; legacy decode failed: {legacy_error}"
                    ),
                }),
            },
        }
    }

    fn state_from_persisted(
        persisted: PersistedWorkspaceMetadata,
    ) -> Result<WorkspaceMetadataState, VfsError> {
        let mut state = WorkspaceMetadataState::default();
        for workspace in persisted.workspaces {
            state.workspaces.insert(workspace.id, workspace);
        }
        for mut token in persisted.tokens {
            let workspace = state.workspaces.get(&token.workspace_id).ok_or_else(|| {
                VfsError::CorruptStore {
                    message: format!(
                        "workspace token {} references unknown workspace {}",
                        token.id, token.workspace_id
                    ),
                }
            })?;
            token.read_prefixes =
                normalize_workspace_token_prefixes(&workspace.root_path, token.read_prefixes)
                    .map_err(|e| VfsError::CorruptStore {
                        message: format!("invalid persisted read prefixes: {e}"),
                    })?;
            token.write_prefixes =
                normalize_workspace_token_prefixes(&workspace.root_path, token.write_prefixes)
                    .map_err(|e| VfsError::CorruptStore {
                        message: format!("invalid persisted write prefixes: {e}"),
                    })?;
            state
                .tokens
                .entry(token.workspace_id)
                .or_default()
                .push(token);
        }
        Ok(state)
    }

    fn decode_legacy(bytes: &[u8]) -> Result<WorkspaceMetadataState, VfsError> {
        let persisted: LegacyPersistedWorkspaceMetadata = crate::codec::deserialize(bytes)
            .map_err(|e| VfsError::CorruptStore {
                message: format!("workspace metadata v1 decode failed: {e}"),
            })?;
        if persisted.version != LEGACY_WORKSPACE_METADATA_VERSION {
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
            let workspace = state.workspaces.get(&token.workspace_id).ok_or_else(|| {
                VfsError::CorruptStore {
                    message: format!(
                        "workspace token {} references unknown workspace {}",
                        token.id, token.workspace_id
                    ),
                }
            })?;
            let root_path =
                normalize_workspace_token_prefix(&workspace.root_path).map_err(|e| {
                    VfsError::CorruptStore {
                        message: format!("invalid legacy workspace root path: {e}"),
                    }
                })?;
            let token = WorkspaceTokenRecord {
                id: token.id,
                workspace_id: token.workspace_id,
                name: token.name,
                agent_uid: token.agent_uid,
                secret_hash: token.secret_hash,
                read_prefixes: vec![root_path.clone()],
                write_prefixes: vec![root_path],
            };
            state
                .tokens
                .entry(token.workspace_id)
                .or_default()
                .push(token);
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
        crate::codec::serialize(&persisted).map_err(|e| VfsError::CorruptStore {
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
        if let Some(parent) = self.path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
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

    async fn issue_scoped_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: Uid,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let Some(workspace) = next.workspaces.get(&workspace_id) else {
            return Err(VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            });
        };
        let read_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, read_prefixes)?;
        let write_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, write_prefixes)?;

        let raw_secret = InMemoryWorkspaceMetadataStore::generate_secret();
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: name.to_string(),
            agent_uid,
            secret_hash: InMemoryWorkspaceMetadataStore::hash_secret(&raw_secret),
            read_prefixes,
            write_prefixes,
        };
        next.tokens
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
                tokens.iter().find(|token| {
                    constant_time_eq(token.secret_hash.as_bytes(), expected.as_bytes())
                })
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

fn normalize_workspace_token_prefix(prefix: &str) -> Result<String, VfsError> {
    if !prefix.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: prefix.to_string(),
        });
    }

    let mut parts = Vec::new();
    for part in prefix.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn normalize_workspace_token_prefixes(
    workspace_root: &str,
    prefixes: Vec<String>,
) -> Result<Vec<String>, VfsError> {
    let workspace_root = normalize_workspace_token_prefix(workspace_root)?;
    let mut normalized = Vec::new();
    for prefix in prefixes {
        let prefix = normalize_workspace_token_prefix(&prefix)?;
        if !path_matches_prefix(&prefix, &workspace_root) {
            return Err(VfsError::PermissionDenied { path: prefix });
        }
        normalized.push(prefix);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
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

    fn push_legacy_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn push_legacy_u64(out: &mut Vec<u8>, value: u64) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn push_legacy_uuid(out: &mut Vec<u8>, value: Uuid) {
        push_legacy_u64(out, 16);
        out.extend_from_slice(value.as_bytes());
    }

    fn push_legacy_string(out: &mut Vec<u8>, value: &str) {
        push_legacy_u64(out, value.len() as u64);
        out.extend_from_slice(value.as_bytes());
    }

    fn legacy_v1_metadata_bytes(
        workspace: &WorkspaceRecord,
        token_id: Uuid,
        token_name: &str,
        agent_uid: Uid,
        secret_hash: &str,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        push_legacy_u32(&mut out, 1);

        push_legacy_u64(&mut out, 1);
        push_legacy_uuid(&mut out, workspace.id);
        push_legacy_string(&mut out, &workspace.name);
        push_legacy_string(&mut out, &workspace.root_path);
        out.push(0); // head_commit: None
        push_legacy_u64(&mut out, workspace.version);

        push_legacy_u64(&mut out, 1);
        push_legacy_uuid(&mut out, token_id);
        push_legacy_uuid(&mut out, workspace.id);
        push_legacy_string(&mut out, token_name);
        push_legacy_u32(&mut out, agent_uid);
        push_legacy_string(&mut out, secret_hash);
        out
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
    async fn workspace_tokens_default_scope_to_workspace_root_path() {
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store
            .create_workspace("demo", "/incidents/./demo//")
            .await
            .unwrap();

        let issued = store
            .issue_workspace_token(workspace.id, "demo-token", 42)
            .await
            .unwrap();

        assert_eq!(issued.token.read_prefixes, vec!["/incidents/demo"]);
        assert_eq!(issued.token.write_prefixes, vec!["/incidents/demo"]);

        let valid = store
            .validate_workspace_token(workspace.id, &issued.raw_secret)
            .await
            .unwrap()
            .expect("token should validate");

        assert_eq!(valid.token.read_prefixes, vec!["/incidents/demo"]);
        assert_eq!(valid.token.write_prefixes, vec!["/incidents/demo"]);
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
    async fn workspace_tokens_persist_read_write_prefixes() {
        let path = temp_metadata_path("token-prefixes");
        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store
            .create_workspace("demo", "/incidents/demo")
            .await
            .unwrap();

        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "agent-session",
                7,
                vec![
                    "/incidents/./demo/read//".to_string(),
                    "/incidents/demo/shared".to_string(),
                ],
                vec!["/incidents/demo/write/.".to_string()],
            )
            .await
            .unwrap();

        assert_eq!(
            issued.token.read_prefixes,
            vec!["/incidents/demo/read", "/incidents/demo/shared"]
        );
        assert_eq!(issued.token.write_prefixes, vec!["/incidents/demo/write"]);
        drop(store);

        let reloaded = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let valid = reloaded
            .validate_workspace_token(workspace.id, &issued.raw_secret)
            .await
            .unwrap()
            .expect("token should validate after reload");

        assert_eq!(
            valid.token.read_prefixes,
            vec!["/incidents/demo/read", "/incidents/demo/shared"]
        );
        assert_eq!(valid.token.write_prefixes, vec!["/incidents/demo/write"]);
    }

    #[tokio::test]
    async fn workspace_token_scope_rejects_relative_prefixes() {
        let memory_store = InMemoryWorkspaceMetadataStore::new();
        let memory_workspace = memory_store
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let err = memory_store
            .issue_scoped_workspace_token(
                memory_workspace.id,
                "bad-token",
                7,
                vec!["relative/read".to_string()],
                vec!["/demo/write".to_string()],
            )
            .await
            .expect_err("relative read prefix should fail");
        assert!(matches!(err, VfsError::InvalidPath { .. }));

        let path = temp_metadata_path("invalid-prefix");
        let local_store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let local_workspace = local_store.create_workspace("demo", "/demo").await.unwrap();

        let err = local_store
            .issue_scoped_workspace_token(
                local_workspace.id,
                "bad-token",
                7,
                vec!["/demo/read".to_string()],
                vec!["relative/write".to_string()],
            )
            .await
            .expect_err("relative write prefix should fail");
        assert!(matches!(err, VfsError::InvalidPath { .. }));

        let file_text = String::from_utf8_lossy(&fs::read(&path).unwrap()).to_string();
        assert!(!file_text.contains("relative/write"));
    }

    #[tokio::test]
    async fn workspace_token_scope_rejects_prefixes_outside_workspace_root_after_normalization() {
        let memory_store = InMemoryWorkspaceMetadataStore::new();
        let memory_workspace = memory_store
            .create_workspace("demo", "/demo")
            .await
            .unwrap();

        let err = memory_store
            .issue_scoped_workspace_token(
                memory_workspace.id,
                "bad-token",
                7,
                vec!["/demo/../finance/read".to_string()],
                vec!["/demo/write".to_string()],
            )
            .await
            .expect_err("normalized read prefix outside root should fail");
        assert!(matches!(err, VfsError::PermissionDenied { .. }));

        let path = temp_metadata_path("root-escape");
        let local_store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let local_workspace = local_store.create_workspace("demo", "/demo").await.unwrap();

        let err = local_store
            .issue_scoped_workspace_token(
                local_workspace.id,
                "bad-token",
                7,
                vec!["/demo/read".to_string()],
                vec!["/demo/../finance/write".to_string()],
            )
            .await
            .expect_err("normalized write prefix outside root should fail");
        assert!(matches!(err, VfsError::PermissionDenied { .. }));

        let file_text = String::from_utf8_lossy(&fs::read(&path).unwrap()).to_string();
        assert!(!file_text.contains("/finance/write"));
    }

    #[tokio::test]
    async fn v1_metadata_migrates_workspace_token_scopes_to_root_path() {
        let path = temp_metadata_path("v1-migration");
        let workspace = WorkspaceRecord {
            id: Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
            name: "legacy".to_string(),
            root_path: "/legacy/./root//".to_string(),
            head_commit: None,
            version: 0,
        };
        let raw_secret = "legacy-secret";
        let secret_hash = InMemoryWorkspaceMetadataStore::hash_secret(raw_secret);
        let persisted = legacy_v1_metadata_bytes(
            &workspace,
            Uuid::from_u128(0xffeeddcc_bbaa_9988_7766_554433221100),
            "legacy-token",
            9,
            &secret_hash,
        );
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, persisted).unwrap();

        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let valid = store
            .validate_workspace_token(workspace.id, raw_secret)
            .await
            .unwrap()
            .expect("legacy token should validate");

        assert_eq!(valid.token.read_prefixes, vec!["/legacy/root"]);
        assert_eq!(valid.token.write_prefixes, vec!["/legacy/root"]);
    }

    #[test]
    fn v2_metadata_rejects_token_scopes_outside_workspace_root() {
        let path = temp_metadata_path("v2-root-escape");
        let workspace = WorkspaceRecord {
            id: Uuid::new_v4(),
            name: "demo".to_string(),
            root_path: "/demo".to_string(),
            head_commit: None,
            version: 0,
        };
        let token = WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id: workspace.id,
            name: "bad-token".to_string(),
            agent_uid: 7,
            secret_hash: "hash".to_string(),
            read_prefixes: vec!["/finance/read".to_string()],
            write_prefixes: vec!["/demo/write".to_string()],
        };
        let persisted = PersistedWorkspaceMetadata {
            version: WORKSPACE_METADATA_VERSION,
            workspaces: vec![workspace],
            tokens: vec![token],
        };
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, crate::codec::serialize(&persisted).unwrap()).unwrap();

        let err = match LocalWorkspaceMetadataStore::open(&path) {
            Ok(_) => panic!("out-of-root persisted token scope should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
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
        fs::write(&path, crate::codec::serialize(&persisted).unwrap()).unwrap();

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
