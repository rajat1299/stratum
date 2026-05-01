use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::auth::{Uid, session::Session};
use crate::error::VfsError;

const AUDIT_STORE_VERSION: u32 = 1;

pub type SharedAuditStore = Arc<dyn AuditStore>;

#[async_trait]
pub trait AuditStore: Send + Sync {
    async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError>;
    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditActor {
    pub uid: Uid,
    pub username: String,
    pub delegate: Option<AuditDelegate>,
}

impl AuditActor {
    pub fn new(uid: Uid, username: impl Into<String>) -> Self {
        Self {
            uid,
            username: username.into(),
            delegate: None,
        }
    }

    pub fn from_session(session: &Session) -> Self {
        Self {
            uid: session.uid,
            username: session.username.clone(),
            delegate: session.delegate.as_ref().map(|delegate| AuditDelegate {
                uid: delegate.uid,
                username: delegate.username.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditDelegate {
    pub uid: Uid,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditWorkspaceContext {
    pub id: Uuid,
    pub root_path: String,
    pub base_ref: String,
    pub session_ref: Option<String>,
}

impl AuditWorkspaceContext {
    pub fn from_session(session: &Session) -> Option<Self> {
        let mount = session.mount()?;
        Some(Self {
            id: mount.workspace_id(),
            root_path: mount.root_path().to_string(),
            base_ref: mount.base_ref().to_string(),
            session_ref: mount.session_ref().map(str::to_string),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    FsWriteFile,
    FsMkdir,
    FsDelete,
    FsCopy,
    FsMove,
    FsMetadataUpdate,
    VcsCommit,
    VcsRevert,
    VcsRefCreate,
    VcsRefUpdate,
    ProtectedRefRuleCreate,
    ProtectedPathRuleCreate,
    ChangeRequestCreate,
    ChangeRequestApprove,
    ChangeRequestApprovalDismiss,
    ChangeRequestCommentCreate,
    ChangeRequestReviewerAssign,
    ChangeRequestReject,
    ChangeRequestMerge,
    WorkspaceCreate,
    WorkspaceTokenIssue,
    RunCreate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResourceKind {
    File,
    Directory,
    Path,
    Commit,
    Ref,
    ProtectedRefRule,
    ProtectedPathRule,
    ChangeRequest,
    ApprovalRecord,
    ReviewComment,
    ReviewAssignment,
    Workspace,
    WorkspaceToken,
    Run,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditResource {
    pub kind: AuditResourceKind,
    pub id: Option<String>,
    pub path: Option<String>,
}

impl AuditResource {
    pub fn id(kind: AuditResourceKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: Some(id.into()),
            path: None,
        }
    }

    pub fn path(kind: AuditResourceKind, path: impl Into<String>) -> Self {
        Self {
            kind,
            id: None,
            path: Some(path.into()),
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Success,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAuditEvent {
    pub actor: AuditActor,
    pub workspace: Option<AuditWorkspaceContext>,
    pub action: AuditAction,
    pub resource: AuditResource,
    pub outcome: AuditOutcome,
    pub details: BTreeMap<String, String>,
}

impl NewAuditEvent {
    pub fn new(actor: AuditActor, action: AuditAction, resource: AuditResource) -> Self {
        Self {
            actor,
            workspace: None,
            action,
            resource,
            outcome: AuditOutcome::Success,
            details: BTreeMap::new(),
        }
    }

    pub fn from_session(session: &Session, action: AuditAction, resource: AuditResource) -> Self {
        Self::new(AuditActor::from_session(session), action, resource)
            .with_workspace_from_session(session)
    }

    pub fn with_workspace(mut self, workspace: AuditWorkspaceContext) -> Self {
        self.workspace = Some(workspace);
        self
    }

    pub fn with_workspace_from_session(mut self, session: &Session) -> Self {
        self.workspace = AuditWorkspaceContext::from_session(session);
        self
    }

    pub fn with_outcome(mut self, outcome: AuditOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.details.insert(key.into(), value.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: Uuid,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub actor: AuditActor,
    pub workspace: Option<AuditWorkspaceContext>,
    pub action: AuditAction,
    pub resource: AuditResource,
    pub outcome: AuditOutcome,
    pub details: BTreeMap<String, String>,
}

impl AuditEvent {
    fn from_input(sequence: u64, input: NewAuditEvent) -> Self {
        Self {
            id: Uuid::new_v4(),
            sequence,
            timestamp: Utc::now(),
            actor: input.actor,
            workspace: input.workspace,
            action: input.action,
            resource: input.resource,
            outcome: input.outcome,
            details: input.details,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AuditState {
    events: Vec<AuditEvent>,
}

impl AuditState {
    fn next_sequence(&self) -> u64 {
        self.events
            .last()
            .map(|event| event.sequence.saturating_add(1))
            .unwrap_or(1)
    }

    fn recent(&self, limit: usize) -> Vec<AuditEvent> {
        let start = self.events.len().saturating_sub(limit);
        self.events[start..].to_vec()
    }
}

#[derive(Debug, Default)]
pub struct InMemoryAuditStore {
    inner: RwLock<AuditState>,
}

impl InMemoryAuditStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl AuditStore for InMemoryAuditStore {
    async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
        let mut guard = self.inner.write().await;
        let event = AuditEvent::from_input(guard.next_sequence(), event);
        guard.events.push(event.clone());
        Ok(event)
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.recent(limit))
    }
}

#[derive(Debug)]
pub struct LocalAuditStore {
    path: PathBuf,
    _lock: AuditStoreLock,
    inner: RwLock<AuditState>,
}

#[derive(Debug)]
struct AuditStoreLock {
    path: PathBuf,
    owner_id: Uuid,
    file: Option<File>,
}

impl Drop for AuditStoreLock {
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
struct PersistedAuditStore {
    version: u32,
    events: Vec<AuditEvent>,
}

impl LocalAuditStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, VfsError> {
        let path = path.as_ref().to_path_buf();
        let lock = Self::acquire_lock(&path)?;
        let state = match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => AuditState::default(),
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            path,
            _lock: lock,
            inner: RwLock::new(state),
        })
    }

    fn acquire_lock(path: &Path) -> Result<AuditStoreLock, VfsError> {
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
                        "failed to acquire audit store lock '{}': {e}",
                        lock_path.display()
                    ),
                ))
            })?;
        {
            use std::io::Write;
            file.write_all(owner_id.to_string().as_bytes())?;
            file.sync_all()?;
        }
        Ok(AuditStoreLock {
            path: lock_path,
            owner_id,
            file: Some(file),
        })
    }

    fn decode(bytes: &[u8]) -> Result<AuditState, VfsError> {
        let persisted: PersistedAuditStore =
            crate::codec::deserialize(bytes).map_err(|e| VfsError::CorruptStore {
                message: format!("audit store decode failed: {e}"),
            })?;
        if persisted.version != AUDIT_STORE_VERSION {
            return Err(VfsError::CorruptStore {
                message: format!("unsupported audit store version {}", persisted.version),
            });
        }

        for (expected, event) in (1_u64..).zip(persisted.events.iter()) {
            if event.sequence != expected {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "audit event sequence gap: expected {expected}, got {}",
                        event.sequence
                    ),
                });
            }
        }

        Ok(AuditState {
            events: persisted.events,
        })
    }

    fn encode(state: &AuditState) -> Result<Vec<u8>, VfsError> {
        crate::codec::serialize(&PersistedAuditStore {
            version: AUDIT_STORE_VERSION,
            events: state.events.clone(),
        })
        .map_err(|e| VfsError::CorruptStore {
            message: format!("audit store encode failed: {e}"),
        })
    }

    fn persist_locked(&self, state: &AuditState) -> Result<(), VfsError> {
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
impl AuditStore for LocalAuditStore {
    async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let event = AuditEvent::from_input(next.next_sequence(), event);
        next.events.push(event.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(event)
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.recent(limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn temp_audit_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stratum_audit_{}_{}_{}.bin",
            name,
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    #[tokio::test]
    async fn local_store_reloads_appended_events_in_sequence_order() {
        let path = temp_audit_path("reload");
        let store = LocalAuditStore::open(&path).unwrap();

        let first = store
            .append(
                NewAuditEvent::new(
                    AuditActor::new(42, "ci-agent"),
                    AuditAction::FsWriteFile,
                    AuditResource::path(AuditResourceKind::File, "/demo/a.txt"),
                )
                .with_workspace(AuditWorkspaceContext {
                    id: Uuid::new_v4(),
                    root_path: "/demo".to_string(),
                    base_ref: "main".to_string(),
                    session_ref: Some("agent/ci/session-1".to_string()),
                })
                .with_detail("size", "7"),
            )
            .await
            .unwrap();
        let second = store
            .append(NewAuditEvent::new(
                AuditActor::new(0, "root"),
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Commit, "abc123"),
            ))
            .await
            .unwrap();

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        drop(store);

        let reloaded = LocalAuditStore::open(&path).unwrap();
        let events = reloaded.list_recent(10).await.unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[0].actor.username, "ci-agent");
        assert_eq!(events[0].action, AuditAction::FsWriteFile);
        assert_eq!(events[0].resource.path.as_deref(), Some("/demo/a.txt"));
        assert_eq!(events[0].details.get("size").map(String::as_str), Some("7"));
        assert_eq!(events[1].sequence, 2);
        assert_eq!(events[1].action, AuditAction::VcsCommit);
    }

    #[test]
    fn corrupt_store_bytes_return_corrupt_store() {
        let path = temp_audit_path("corrupt");
        fs::write(&path, b"not-audit").unwrap();

        let err = match LocalAuditStore::open(&path) {
            Ok(_) => panic!("corrupt audit store should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
    }

    #[test]
    fn dropping_store_does_not_remove_replaced_lock_file() {
        let path = temp_audit_path("replaced-lock");
        let lock_path = path.with_extension("lock");
        let store = LocalAuditStore::open(&path).unwrap();

        fs::remove_file(&lock_path).unwrap();
        fs::write(&lock_path, "replacement-owner").unwrap();
        drop(store);

        assert_eq!(fs::read_to_string(&lock_path).unwrap(), "replacement-owner");
        fs::remove_file(lock_path).unwrap();
    }

    #[tokio::test]
    async fn failed_persist_does_not_publish_event() {
        let path = temp_audit_path("append-error");
        let store = LocalAuditStore::open(&path).unwrap();
        fs::create_dir_all(&path).unwrap();

        let err = store
            .append(NewAuditEvent::new(
                AuditActor::new(42, "ci-agent"),
                AuditAction::RunCreate,
                AuditResource::id(AuditResourceKind::Run, "run_123"),
            ))
            .await
            .expect_err("rename over directory should fail");
        assert!(matches!(err, crate::error::VfsError::IoError(_)));

        let events = store.list_recent(10).await.unwrap();
        assert!(events.is_empty());
    }
}
