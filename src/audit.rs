use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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
    async fn append_once(
        &self,
        event: NewAuditEvent,
        identity: AuditAppendIdentity,
    ) -> Result<AuditAppendOutcome, VfsError> {
        match identity {
            AuditAppendIdentity::VcsVisibleCommit { repo_id, commit_id } => {
                if self
                    .contains_vcs_commit_event_for_repo(&repo_id, &commit_id)
                    .await?
                {
                    return Ok(AuditAppendOutcome::AlreadyPresent);
                }
            }
            AuditAppendIdentity::FsMutationRecovery {
                repo_id,
                action,
                operation_id,
                target_ref,
                new_commit,
            } => {
                if self
                    .contains_fs_mutation_recovery_event_for_repo(
                        &repo_id,
                        action,
                        &operation_id,
                        &target_ref,
                        &new_commit,
                    )
                    .await?
                {
                    return Ok(AuditAppendOutcome::AlreadyPresent);
                }
            }
        }
        self.append(event).await.map(AuditAppendOutcome::Appended)
    }
    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError>;
    async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError>;
    async fn contains_vcs_commit_event_for_repo(
        &self,
        repo_id: &str,
        commit_id: &str,
    ) -> Result<bool, VfsError> {
        let _ = repo_id;
        self.contains_vcs_commit_event(commit_id).await
    }
    async fn contains_fs_mutation_recovery_event(
        &self,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let _ = (action, operation_id, target_ref, new_commit);
        Ok(false)
    }
    async fn contains_fs_mutation_recovery_event_for_repo(
        &self,
        repo_id: &str,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let _ = repo_id;
        self.contains_fs_mutation_recovery_event(action, operation_id, target_ref, new_commit)
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditAppendIdentity {
    VcsVisibleCommit {
        repo_id: String,
        commit_id: String,
    },
    FsMutationRecovery {
        repo_id: String,
        action: AuditAction,
        operation_id: String,
        target_ref: String,
        new_commit: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditAppendOutcome {
    Appended(AuditEvent),
    AlreadyPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditExportClass {
    Auth,
    ChangeRequest,
    FilesystemMutation,
    Idempotency,
    Policy,
    Run,
    VersionControl,
    Workspace,
}

impl AuditExportClass {
    fn from_action(action: AuditAction) -> Self {
        match action {
            AuditAction::PolicyDecisionAllow | AuditAction::PolicyDecisionDeny => Self::Policy,
            AuditAction::FsWriteFile
            | AuditAction::FsMkdir
            | AuditAction::FsDelete
            | AuditAction::FsCopy
            | AuditAction::FsMove
            | AuditAction::FsMetadataUpdate => Self::FilesystemMutation,
            AuditAction::VcsCommit
            | AuditAction::VcsRevert
            | AuditAction::VcsRefCreate
            | AuditAction::VcsRefUpdate => Self::VersionControl,
            AuditAction::ProtectedRefRuleCreate
            | AuditAction::ProtectedPathRuleCreate
            | AuditAction::WorkspaceCreate
            | AuditAction::WorkspaceTokenIssue
            | AuditAction::WorkspaceTokenRevoke => Self::Workspace,
            AuditAction::ChangeRequestCreate
            | AuditAction::ChangeRequestApprove
            | AuditAction::ChangeRequestApprovalDismiss
            | AuditAction::ChangeRequestCommentCreate
            | AuditAction::ChangeRequestReviewerAssign
            | AuditAction::ChangeRequestReject
            | AuditAction::ChangeRequestMerge => Self::ChangeRequest,
            AuditAction::RunCreate
            | AuditAction::RunExecuteCreate
            | AuditAction::RunExecuteStart
            | AuditAction::RunExecuteFinish
            | AuditAction::RunExecuteCancel
            | AuditAction::RunExecuteFailure => Self::Run,
            AuditAction::IdempotencyQuotaExceeded => Self::Idempotency,
            AuditAction::AuthOidcLoginDenied
            | AuditAction::AuthOidcLoginSuccess
            | AuditAction::AuthSamlLoginDenied
            | AuditAction::AuthSamlLoginSuccess
            | AuditAction::AuthRefreshTokenIssue
            | AuditAction::AuthRefreshTokenRotate
            | AuditAction::AuthRefreshTokenRevoke
            | AuditAction::AuthRefreshTokenExpireDenied
            | AuditAction::AuthRefreshTokenReuseDenied
            | AuditAction::AuthScimRequestDenied
            | AuditAction::AuthScimUserProvision
            | AuditAction::AuthScimUserUpdate
            | AuditAction::AuthScimUserDeactivate
            | AuditAction::AuthScimGroupProvision
            | AuditAction::AuthScimGroupUpdate
            | AuditAction::AuthScimGroupMemberAdd
            | AuditAction::AuthScimGroupMemberRemove => Self::Auth,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditExportPolicy {
    max_attempts: usize,
    mandatory_classes: BTreeSet<AuditExportClass>,
}

impl Default for AuditExportPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            mandatory_classes: BTreeSet::new(),
        }
    }
}

impl AuditExportPolicy {
    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn with_mandatory_class(mut self, class: AuditExportClass) -> Self {
        self.mandatory_classes.insert(class);
        self
    }

    fn is_mandatory(&self, class: AuditExportClass) -> bool {
        self.mandatory_classes.contains(&class)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditExportPayload {
    pub event_id: Uuid,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub class: AuditExportClass,
    pub actor: AuditActor,
    pub workspace: Option<AuditWorkspaceContext>,
    pub action: AuditAction,
    pub resource: AuditResource,
    pub outcome: AuditOutcome,
    pub details: BTreeMap<String, String>,
}

impl AuditExportPayload {
    fn from_event(event: &AuditEvent) -> Self {
        Self {
            event_id: event.id,
            sequence: event.sequence,
            timestamp: event.timestamp,
            class: AuditExportClass::from_action(event.action),
            actor: event.actor.clone(),
            workspace: event.workspace.clone(),
            action: event.action,
            resource: event.resource.clone(),
            outcome: event.outcome,
            details: redacted_export_details(&event.details),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditExportError {
    code: &'static str,
}

impl AuditExportError {
    pub fn new(code: &'static str) -> Self {
        Self { code }
    }

    fn code(&self) -> &'static str {
        self.code
    }
}

#[async_trait]
pub trait AuditEventSink: Send + Sync {
    async fn publish(&self, payload: &AuditExportPayload) -> Result<(), AuditExportError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditExportDeliveryStatus {
    Pending,
    Delivered,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditExportAttempt {
    pub event_id: Uuid,
    pub attempt: usize,
    pub status: AuditExportDeliveryStatus,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditExportMetrics {
    pub delivered: usize,
    pub pending: usize,
    pub failed: usize,
    pub attempts: usize,
    pub last_error_code: Option<String>,
    pub oldest_pending_age_ms: Option<i64>,
    pub sequence_lag: u64,
}

#[derive(Debug, Clone)]
struct AuditExportDelivery {
    sequence: u64,
    status: AuditExportDeliveryStatus,
    attempts: usize,
    first_pending_at: DateTime<Utc>,
    last_error_code: Option<String>,
}

#[derive(Debug, Default)]
struct AuditExportState {
    deliveries: BTreeMap<Uuid, AuditExportDelivery>,
    attempts: Vec<AuditExportAttempt>,
}

pub struct ExportingAuditStore {
    primary: SharedAuditStore,
    sink: Arc<dyn AuditEventSink>,
    policy: AuditExportPolicy,
    state: RwLock<AuditExportState>,
}

impl ExportingAuditStore {
    pub fn new(primary: SharedAuditStore, sink: Arc<dyn AuditEventSink>) -> Self {
        Self {
            primary,
            sink,
            policy: AuditExportPolicy::default(),
            state: RwLock::new(AuditExportState::default()),
        }
    }

    pub fn with_policy(mut self, policy: AuditExportPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub async fn delivery_status(&self, event_id: Uuid) -> Option<AuditExportDeliveryStatus> {
        self.state
            .read()
            .await
            .deliveries
            .get(&event_id)
            .map(|delivery| delivery.status)
    }

    pub async fn export_metrics(&self) -> AuditExportMetrics {
        let guard = self.state.read().await;
        let now = Utc::now();
        let mut metrics = AuditExportMetrics::default();
        let mut oldest_pending: Option<DateTime<Utc>> = None;
        let mut max_pending_sequence = 0_u64;
        let mut min_pending_sequence = u64::MAX;
        let mut pending_event_ids = BTreeSet::new();
        for (event_id, delivery) in &guard.deliveries {
            metrics.attempts = metrics.attempts.saturating_add(delivery.attempts);
            match delivery.status {
                AuditExportDeliveryStatus::Delivered => {
                    metrics.delivered = metrics.delivered.saturating_add(1);
                }
                AuditExportDeliveryStatus::Pending | AuditExportDeliveryStatus::Failed => {
                    pending_event_ids.insert(*event_id);
                    metrics.pending = metrics.pending.saturating_add(1);
                    if delivery.status == AuditExportDeliveryStatus::Failed {
                        metrics.failed = metrics.failed.saturating_add(1);
                    }
                    if delivery.sequence < min_pending_sequence {
                        min_pending_sequence = delivery.sequence;
                    }
                    if delivery.sequence > max_pending_sequence {
                        max_pending_sequence = delivery.sequence;
                    }
                    if let Some(error_code) = &delivery.last_error_code {
                        metrics.last_error_code = Some(error_code.clone());
                    }
                    oldest_pending = Some(match oldest_pending {
                        Some(current) => current.min(delivery.first_pending_at),
                        None => delivery.first_pending_at,
                    });
                }
            }
        }
        metrics.last_error_code = guard
            .attempts
            .iter()
            .rev()
            .find(|attempt| pending_event_ids.contains(&attempt.event_id))
            .and_then(|attempt| attempt.error_code.clone());
        metrics.sequence_lag = if metrics.pending == 0 {
            0
        } else {
            max_pending_sequence.saturating_sub(min_pending_sequence) + 1
        };
        metrics.oldest_pending_age_ms =
            oldest_pending.map(|oldest| now.signed_duration_since(oldest).num_milliseconds());
        metrics
    }

    async fn record_delivery(
        &self,
        event: &AuditEvent,
        status: AuditExportDeliveryStatus,
        attempts: Vec<AuditExportAttempt>,
        last_error_code: Option<String>,
    ) {
        let mut guard = self.state.write().await;
        guard.deliveries.insert(
            event.id,
            AuditExportDelivery {
                sequence: event.sequence,
                status,
                attempts: attempts.len(),
                first_pending_at: Utc::now(),
                last_error_code,
            },
        );
        guard.attempts.extend(attempts);
    }

    async fn export_after_append(&self, event: &AuditEvent) -> Result<(), VfsError> {
        if self.state.read().await.deliveries.contains_key(&event.id) {
            return Ok(());
        }
        let payload = AuditExportPayload::from_event(event);
        let class = payload.class;
        let mut attempts = Vec::with_capacity(self.policy.max_attempts);
        let mut last_error_code = None;
        for attempt in 1..=self.policy.max_attempts {
            match self.sink.publish(&payload).await {
                Ok(()) => {
                    attempts.push(AuditExportAttempt {
                        event_id: event.id,
                        attempt,
                        status: AuditExportDeliveryStatus::Delivered,
                        error_code: None,
                    });
                    self.record_delivery(
                        event,
                        AuditExportDeliveryStatus::Delivered,
                        attempts,
                        None,
                    )
                    .await;
                    return Ok(());
                }
                Err(error) => {
                    let code = error.code().to_string();
                    last_error_code = Some(code.clone());
                    attempts.push(AuditExportAttempt {
                        event_id: event.id,
                        attempt,
                        status: AuditExportDeliveryStatus::Failed,
                        error_code: Some(code),
                    });
                }
            }
        }

        self.record_delivery(
            event,
            AuditExportDeliveryStatus::Failed,
            attempts,
            last_error_code,
        )
        .await;

        if self.policy.is_mandatory(class) {
            return Err(redacted_audit_export_error());
        }
        Ok(())
    }
}

#[async_trait]
impl AuditStore for ExportingAuditStore {
    async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
        let event = self.primary.append(event).await?;
        self.export_after_append(&event).await?;
        Ok(event)
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        self.primary.list_recent(limit).await
    }

    async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError> {
        self.primary.contains_vcs_commit_event(commit_id).await
    }

    async fn contains_vcs_commit_event_for_repo(
        &self,
        repo_id: &str,
        commit_id: &str,
    ) -> Result<bool, VfsError> {
        self.primary
            .contains_vcs_commit_event_for_repo(repo_id, commit_id)
            .await
    }

    async fn contains_fs_mutation_recovery_event(
        &self,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        self.primary
            .contains_fs_mutation_recovery_event(action, operation_id, target_ref, new_commit)
            .await
    }

    async fn contains_fs_mutation_recovery_event_for_repo(
        &self,
        repo_id: &str,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        self.primary
            .contains_fs_mutation_recovery_event_for_repo(
                repo_id,
                action,
                operation_id,
                target_ref,
                new_commit,
            )
            .await
    }
}

fn redacted_audit_export_error() -> VfsError {
    VfsError::CorruptStore {
        message: "audit event export failed: redacted delivery failure".to_string(),
    }
}

fn redacted_export_details(details: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    details
        .iter()
        .filter_map(|(key, value)| {
            if audit_export_detail_is_sensitive(key, value) {
                None
            } else {
                Some((key.clone(), value.clone()))
            }
        })
        .collect()
}

fn audit_export_detail_is_sensitive(key: &str, value: &str) -> bool {
    let normalized_key = key.to_ascii_lowercase();
    let normalized_value = value.to_ascii_lowercase();
    let execution_sensitive_key = [
        "args",
        "argv",
        "backing_path",
        "command",
        "cwd",
        "env",
        "environment",
        "environment_variables",
        "process_env",
        "process_environment",
        "raw_command",
        "raw_stderr",
        "raw_stdout",
        "shell",
        "stderr",
        "stderr_md",
        "stderr_text",
        "stdout",
        "stdout_md",
        "stdout_text",
        "temp_dir",
        "tmp_dir",
        "workspace_backing_path",
        "working_dir",
    ]
    .contains(&normalized_key.as_str());
    [
        "request_body",
        "body",
        "secret",
        "token",
        "token_hash",
        "access_token",
        "refresh_token",
        "scim_bearer",
        "bearer",
        "external_id",
        "db_url",
        "database_url",
        "provider_error",
        "commit_message",
        "file_contents",
        "content",
        "encrypted_replay_plaintext",
        "plaintext",
    ]
    .iter()
    .any(|sensitive| normalized_key.contains(sensitive) || normalized_value.contains(sensitive))
        || execution_sensitive_key
        || normalized_value.contains("postgres://")
        || normalized_value.contains("mysql://")
        || normalized_value.contains("bearer ")
        || normalized_value.contains("authorization:")
        || normalized_value.contains("api_key")
        || normalized_value.contains("apikey")
        || normalized_value.contains("password=")
        || normalized_value.contains("ghp_")
        || normalized_value.contains("sk-")
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
            root_path: session.project_mounted_path(mount.root_path()),
            base_ref: mount.base_ref().to_string(),
            session_ref: mount.session_ref().map(str::to_string),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    PolicyDecisionAllow,
    PolicyDecisionDeny,
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
    WorkspaceTokenRevoke,
    RunCreate,
    RunExecuteCreate,
    RunExecuteStart,
    RunExecuteFinish,
    RunExecuteCancel,
    RunExecuteFailure,
    IdempotencyQuotaExceeded,
    AuthOidcLoginDenied,
    AuthOidcLoginSuccess,
    AuthSamlLoginDenied,
    AuthSamlLoginSuccess,
    AuthRefreshTokenIssue,
    AuthRefreshTokenRotate,
    AuthRefreshTokenRevoke,
    AuthRefreshTokenExpireDenied,
    AuthRefreshTokenReuseDenied,
    AuthScimRequestDenied,
    AuthScimUserProvision,
    AuthScimUserUpdate,
    AuthScimUserDeactivate,
    AuthScimGroupProvision,
    AuthScimGroupUpdate,
    AuthScimGroupMemberAdd,
    AuthScimGroupMemberRemove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResourceKind {
    PolicyDecision,
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
    Idempotency,
    AuthProvider,
    ExternalIdentity,
    HostedSession,
    RefreshToken,
    ScimClient,
    ScimUser,
    ScimGroup,
    ScimGroupMembership,
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

    fn contains_vcs_commit_event(&self, commit_id: &str) -> bool {
        self.events
            .iter()
            .any(|event| audit_event_matches_vcs_commit(event, commit_id))
    }

    fn contains_vcs_commit_event_for_repo(&self, repo_id: &str, commit_id: &str) -> bool {
        self.events
            .iter()
            .any(|event| audit_event_matches_vcs_commit_for_repo(event, repo_id, commit_id))
    }

    fn contains_fs_mutation_recovery_event(
        &self,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> bool {
        self.events.iter().any(|event| {
            audit_event_matches_fs_mutation_recovery(
                event,
                action,
                operation_id,
                target_ref,
                new_commit,
            )
        })
    }

    fn contains_fs_mutation_recovery_event_for_repo(
        &self,
        repo_id: &str,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> bool {
        self.events.iter().any(|event| {
            audit_event_matches_fs_mutation_recovery_for_repo(
                event,
                repo_id,
                action,
                operation_id,
                target_ref,
                new_commit,
            )
        })
    }

    fn contains_identity(&self, identity: &AuditAppendIdentity) -> bool {
        match identity {
            AuditAppendIdentity::VcsVisibleCommit { repo_id, commit_id } => {
                self.contains_vcs_commit_event_for_repo(repo_id, commit_id)
            }
            AuditAppendIdentity::FsMutationRecovery {
                repo_id,
                action,
                operation_id,
                target_ref,
                new_commit,
            } => self.contains_fs_mutation_recovery_event_for_repo(
                repo_id,
                *action,
                operation_id,
                target_ref,
                new_commit,
            ),
        }
    }
}

fn existing_exact_vcs_commit_event(
    events: &[AuditEvent],
    input: &NewAuditEvent,
) -> Option<AuditEvent> {
    let resource_id = input.resource.id.as_deref()?;
    if !matches!(
        input.action,
        AuditAction::VcsCommit | AuditAction::VcsRevert
    ) || input.resource.kind != AuditResourceKind::Commit
        || input.resource.path.is_some()
    {
        return None;
    }
    let input_repo_id = input.details.get("repo_id").map(String::as_str);

    events
        .iter()
        .find(|event| {
            event.action == input.action
                && event.resource.kind == AuditResourceKind::Commit
                && event.resource.id.as_deref() == Some(resource_id)
                && event.resource.path.is_none()
                && input_repo_id.is_none_or(|repo_id| audit_event_repo_id(event) == Some(repo_id))
        })
        .cloned()
}

fn audit_event_matches_vcs_commit(event: &AuditEvent, commit_id: &str) -> bool {
    matches!(
        event.action,
        AuditAction::VcsCommit | AuditAction::VcsRevert
    ) && event.resource.kind == AuditResourceKind::Commit
        && event.resource.id.as_deref() == Some(commit_id)
        && event.resource.path.is_none()
}

fn audit_event_matches_vcs_commit_for_repo(
    event: &AuditEvent,
    repo_id: &str,
    commit_id: &str,
) -> bool {
    audit_event_matches_vcs_commit(event, commit_id) && audit_event_repo_id(event) == Some(repo_id)
}

fn audit_event_matches_fs_mutation_recovery(
    event: &AuditEvent,
    action: AuditAction,
    operation_id: &str,
    target_ref: &str,
    new_commit: &str,
) -> bool {
    event.action == action
        && event.resource.kind == AuditResourceKind::Path
        && event.resource.id.is_none()
        && event.details.get("operation_id").map(String::as_str) == Some(operation_id)
        && event.details.get("target_ref").map(String::as_str) == Some(target_ref)
        && event.details.get("new_commit").map(String::as_str) == Some(new_commit)
}

fn audit_event_matches_fs_mutation_recovery_for_repo(
    event: &AuditEvent,
    repo_id: &str,
    action: AuditAction,
    operation_id: &str,
    target_ref: &str,
    new_commit: &str,
) -> bool {
    audit_event_matches_fs_mutation_recovery(event, action, operation_id, target_ref, new_commit)
        && audit_event_repo_id(event) == Some(repo_id)
}

fn audit_event_repo_id(event: &AuditEvent) -> Option<&str> {
    event.details.get("repo_id").map(String::as_str)
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
        if let Some(existing) = existing_exact_vcs_commit_event(&guard.events, &event) {
            return Ok(existing);
        }
        let event = AuditEvent::from_input(guard.next_sequence(), event);
        guard.events.push(event.clone());
        Ok(event)
    }

    async fn append_once(
        &self,
        event: NewAuditEvent,
        identity: AuditAppendIdentity,
    ) -> Result<AuditAppendOutcome, VfsError> {
        let mut guard = self.inner.write().await;
        if guard.contains_identity(&identity) {
            return Ok(AuditAppendOutcome::AlreadyPresent);
        }
        let event = AuditEvent::from_input(guard.next_sequence(), event);
        guard.events.push(event.clone());
        Ok(AuditAppendOutcome::Appended(event))
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.recent(limit))
    }

    async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_vcs_commit_event(commit_id))
    }

    async fn contains_vcs_commit_event_for_repo(
        &self,
        repo_id: &str,
        commit_id: &str,
    ) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_vcs_commit_event_for_repo(repo_id, commit_id))
    }

    async fn contains_fs_mutation_recovery_event(
        &self,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_fs_mutation_recovery_event(action, operation_id, target_ref, new_commit))
    }

    async fn contains_fs_mutation_recovery_event_for_repo(
        &self,
        repo_id: &str,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_fs_mutation_recovery_event_for_repo(
            repo_id,
            action,
            operation_id,
            target_ref,
            new_commit,
        ))
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
        if let Some(existing) = existing_exact_vcs_commit_event(&guard.events, &event) {
            return Ok(existing);
        }
        let mut next = guard.clone();
        let event = AuditEvent::from_input(next.next_sequence(), event);
        next.events.push(event.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(event)
    }

    async fn append_once(
        &self,
        event: NewAuditEvent,
        identity: AuditAppendIdentity,
    ) -> Result<AuditAppendOutcome, VfsError> {
        let mut guard = self.inner.write().await;
        if guard.contains_identity(&identity) {
            return Ok(AuditAppendOutcome::AlreadyPresent);
        }
        let mut next = guard.clone();
        let event = AuditEvent::from_input(next.next_sequence(), event);
        next.events.push(event.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(AuditAppendOutcome::Appended(event))
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.recent(limit))
    }

    async fn contains_vcs_commit_event(&self, commit_id: &str) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_vcs_commit_event(commit_id))
    }

    async fn contains_vcs_commit_event_for_repo(
        &self,
        repo_id: &str,
        commit_id: &str,
    ) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_vcs_commit_event_for_repo(repo_id, commit_id))
    }

    async fn contains_fs_mutation_recovery_event(
        &self,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_fs_mutation_recovery_event(action, operation_id, target_ref, new_commit))
    }

    async fn contains_fs_mutation_recovery_event_for_repo(
        &self,
        repo_id: &str,
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> Result<bool, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.contains_fs_mutation_recovery_event_for_repo(
            repo_id,
            action,
            operation_id,
            target_ref,
            new_commit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[derive(Debug, Default)]
    struct ControllableAuditEventSink {
        published: RwLock<Vec<AuditExportPayload>>,
        failures_remaining: RwLock<usize>,
    }

    impl ControllableAuditEventSink {
        fn new() -> Self {
            Self::default()
        }

        async fn fail_next(&self, failures: usize) {
            *self.failures_remaining.write().await = failures;
        }

        async fn published(&self) -> Vec<AuditExportPayload> {
            self.published.read().await.clone()
        }
    }

    #[async_trait]
    impl AuditEventSink for ControllableAuditEventSink {
        async fn publish(&self, payload: &AuditExportPayload) -> Result<(), AuditExportError> {
            let mut failures = self.failures_remaining.write().await;
            if *failures > 0 {
                *failures -= 1;
                return Err(AuditExportError::new("provider_free_test_failure"));
            }
            drop(failures);
            self.published.write().await.push(payload.clone());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct SequencedFailureAuditEventSink {
        codes: RwLock<VecDeque<&'static str>>,
    }

    impl SequencedFailureAuditEventSink {
        fn new(codes: impl IntoIterator<Item = &'static str>) -> Self {
            Self {
                codes: RwLock::new(codes.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl AuditEventSink for SequencedFailureAuditEventSink {
        async fn publish(&self, _payload: &AuditExportPayload) -> Result<(), AuditExportError> {
            let code = self
                .codes
                .write()
                .await
                .pop_front()
                .unwrap_or("fallback_failure");
            Err(AuditExportError::new(code))
        }
    }

    #[derive(Debug)]
    struct FixedAuditStore {
        events: RwLock<VecDeque<AuditEvent>>,
        persisted: RwLock<Vec<AuditEvent>>,
    }

    impl FixedAuditStore {
        fn new(events: impl IntoIterator<Item = AuditEvent>) -> Self {
            Self {
                events: RwLock::new(events.into_iter().collect()),
                persisted: RwLock::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl AuditStore for FixedAuditStore {
        async fn append(&self, _event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
            let event = self
                .events
                .write()
                .await
                .pop_front()
                .expect("fixed audit event");
            self.persisted.write().await.push(event.clone());
            Ok(event)
        }

        async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
            let guard = self.persisted.read().await;
            let start = guard.len().saturating_sub(limit);
            Ok(guard[start..].to_vec())
        }

        async fn contains_vcs_commit_event(&self, _commit_id: &str) -> Result<bool, VfsError> {
            Ok(false)
        }
    }

    fn temp_audit_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stratum_audit_{}_{}_{}.bin",
            name,
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    fn vcs_commit_event(commit_id: &str) -> NewAuditEvent {
        NewAuditEvent::new(
            AuditActor::new(0, "root"),
            AuditAction::VcsCommit,
            AuditResource::id(AuditResourceKind::Commit, commit_id),
        )
        .with_detail("private_commit_id", "do-not-match-this-detail")
    }

    fn vcs_commit_event_for_repo(commit_id: &str, repo_id: &str) -> NewAuditEvent {
        vcs_commit_event(commit_id).with_detail("repo_id", repo_id)
    }

    fn fs_mutation_recovery_event(
        action: AuditAction,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> NewAuditEvent {
        NewAuditEvent::new(
            AuditActor::new(0, "root"),
            action,
            AuditResource::path(AuditResourceKind::Path, "/docs/recovered.md"),
        )
        .with_detail("operation_id", operation_id)
        .with_detail("target_ref", target_ref)
        .with_detail("new_commit", new_commit)
    }

    fn fs_mutation_recovery_event_for_repo(
        action: AuditAction,
        repo_id: &str,
        operation_id: &str,
        target_ref: &str,
        new_commit: &str,
    ) -> NewAuditEvent {
        fs_mutation_recovery_event(action, operation_id, target_ref, new_commit)
            .with_detail("repo_id", repo_id)
    }

    fn fs_write_event(path: &str) -> NewAuditEvent {
        NewAuditEvent::new(
            AuditActor::new(42, "ci-agent"),
            AuditAction::FsWriteFile,
            AuditResource::path(AuditResourceKind::File, path),
        )
    }

    fn run_execute_event(action: AuditAction) -> NewAuditEvent {
        NewAuditEvent::new(
            AuditActor::new(42, "ci-agent"),
            action,
            AuditResource::id(AuditResourceKind::Run, "run_exec_123"),
        )
    }

    fn fixed_audit_event(id: Uuid, sequence: u64, path: &str) -> AuditEvent {
        AuditEvent {
            id,
            sequence,
            timestamp: Utc::now(),
            actor: AuditActor::new(42, "ci-agent"),
            workspace: None,
            action: AuditAction::FsWriteFile,
            resource: AuditResource::path(AuditResourceKind::File, path),
            outcome: AuditOutcome::Success,
            details: BTreeMap::new(),
        }
    }

    fn exporting_store(
        sink: Arc<ControllableAuditEventSink>,
        policy: AuditExportPolicy,
    ) -> ExportingAuditStore {
        ExportingAuditStore::new(Arc::new(InMemoryAuditStore::new()), sink).with_policy(policy)
    }

    #[tokio::test]
    async fn exporting_store_delivers_after_primary_append() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        let store = exporting_store(sink.clone(), AuditExportPolicy::default());

        let event = store
            .append(fs_write_event("/demo/exported.md"))
            .await
            .unwrap();

        let persisted = store.list_recent(10).await.unwrap();
        assert_eq!(persisted, vec![event.clone()]);
        let published = sink.published().await;
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].event_id, event.id);
        assert_eq!(published[0].sequence, event.sequence);
        assert_eq!(published[0].action, event.action);
        assert_eq!(
            store.delivery_status(event.id).await.unwrap(),
            AuditExportDeliveryStatus::Delivered
        );
        assert_eq!(store.export_metrics().await.delivered, 1);
    }

    #[tokio::test]
    async fn exporting_store_records_best_effort_failure_without_blocking_append() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        sink.fail_next(3).await;
        let store = exporting_store(
            sink.clone(),
            AuditExportPolicy::default().with_max_attempts(3),
        );

        let event = store
            .append(fs_write_event("/demo/best-effort.md"))
            .await
            .unwrap();

        assert_eq!(store.list_recent(1).await.unwrap(), vec![event.clone()]);
        assert!(sink.published().await.is_empty());
        assert_eq!(
            store.delivery_status(event.id).await.unwrap(),
            AuditExportDeliveryStatus::Failed
        );
        let metrics = store.export_metrics().await;
        assert_eq!(metrics.delivered, 0);
        assert_eq!(metrics.failed, 1);
        assert_eq!(metrics.pending, 1);
        assert_eq!(metrics.attempts, 3);
    }

    #[tokio::test]
    async fn exporting_store_retries_until_bounded_success() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        sink.fail_next(2).await;
        let store = exporting_store(
            sink.clone(),
            AuditExportPolicy::default().with_max_attempts(3),
        );

        let event = store
            .append(fs_write_event("/demo/retry.md"))
            .await
            .unwrap();

        assert_eq!(
            store.delivery_status(event.id).await.unwrap(),
            AuditExportDeliveryStatus::Delivered
        );
        assert_eq!(sink.published().await.len(), 1);
        let metrics = store.export_metrics().await;
        assert_eq!(metrics.delivered, 1);
        assert_eq!(metrics.failed, 0);
        assert_eq!(metrics.pending, 0);
        assert_eq!(metrics.attempts, 3);
        assert_eq!(metrics.last_error_code, None);
    }

    #[tokio::test]
    async fn exporting_store_fails_closed_for_configured_mandatory_class() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        sink.fail_next(2).await;
        let policy = AuditExportPolicy::default()
            .with_max_attempts(2)
            .with_mandatory_class(AuditExportClass::FilesystemMutation);
        let store = exporting_store(sink, policy);

        let err = store
            .append(fs_write_event("/demo/mandatory.md"))
            .await
            .expect_err("mandatory export failure should fail closed after persistence");

        let rendered = err.to_string();
        assert!(rendered.contains("audit event export failed"));
        assert!(!rendered.contains("/demo/mandatory.md"));
        let persisted = store.list_recent(10).await.unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(
            persisted[0].resource.path.as_deref(),
            Some("/demo/mandatory.md")
        );
        assert_eq!(
            store.delivery_status(persisted[0].id).await.unwrap(),
            AuditExportDeliveryStatus::Failed
        );
    }

    #[tokio::test]
    async fn exporting_store_tracks_pending_lag_and_delivery_status() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        sink.fail_next(1).await;
        let store = exporting_store(
            sink.clone(),
            AuditExportPolicy::default().with_max_attempts(1),
        );

        let failed = store
            .append(fs_write_event("/demo/pending.md"))
            .await
            .unwrap();
        let delivered = store
            .append(fs_write_event("/demo/delivered.md"))
            .await
            .unwrap();

        assert_eq!(
            store.delivery_status(failed.id).await.unwrap(),
            AuditExportDeliveryStatus::Failed
        );
        assert_eq!(
            store.delivery_status(delivered.id).await.unwrap(),
            AuditExportDeliveryStatus::Delivered
        );
        let metrics = store.export_metrics().await;
        assert_eq!(metrics.delivered, 1);
        assert_eq!(metrics.failed, 1);
        assert_eq!(metrics.pending, 1);
        assert_eq!(metrics.sequence_lag, 1);
        assert!(metrics.oldest_pending_age_ms.is_some());
    }

    #[tokio::test]
    async fn exporting_store_exports_redacted_payload_without_sensitive_material() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        let store = exporting_store(sink.clone(), AuditExportPolicy::default());
        let event = fs_write_event("/demo/redacted.md")
            .with_detail("request_body", "raw-body-secret")
            .with_detail("secret", "audit-secret")
            .with_detail("token", "plain-token")
            .with_detail("token_hash", "hash-secret")
            .with_detail("access_token", "access-secret")
            .with_detail("refresh_token", "refresh-secret")
            .with_detail("scim_bearer", "scim-secret")
            .with_detail("external_id", "external-secret")
            .with_detail("db_url", "postgres://user:pass@localhost/db")
            .with_detail("provider_error", "upstream secret")
            .with_detail("commit_message", "private commit message")
            .with_detail("file_contents", "private file contents")
            .with_detail("encrypted_replay_plaintext", "plaintext-secret")
            .with_detail("bounded_safe_code", "fs_write");

        let persisted = store.append(event).await.unwrap();

        let published = sink.published().await;
        assert_eq!(published.len(), 1);
        let exported = serde_json::to_string(&published[0]).unwrap();
        for forbidden in [
            "raw-body-secret",
            "audit-secret",
            "plain-token",
            "hash-secret",
            "access-secret",
            "refresh-secret",
            "scim-secret",
            "external-secret",
            "postgres://",
            "upstream secret",
            "private commit message",
            "private file contents",
            "plaintext-secret",
        ] {
            assert!(!exported.contains(forbidden), "export leaked {forbidden}");
        }
        assert!(exported.contains("bounded_safe_code"));
        assert!(exported.contains("fs_write"));

        let persisted_details = store.list_recent(1).await.unwrap()[0].details.clone();
        assert_eq!(
            persisted_details.get("request_body").map(String::as_str),
            Some("raw-body-secret")
        );
        assert_eq!(
            persisted_details.get("token").map(String::as_str),
            Some("plain-token")
        );
        assert_eq!(persisted.details, persisted_details);
    }

    #[tokio::test]
    async fn exporting_store_redacts_execution_payload_details_without_redacting_run_metadata() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        let store = exporting_store(sink.clone(), AuditExportPolicy::default());
        let event = run_execute_event(AuditAction::RunExecuteFailure)
            .with_detail("workspace_id", "workspace-123")
            .with_detail("job_id", "job-123")
            .with_detail("run_id", "run_exec_123")
            .with_detail("status", "failed")
            .with_detail("root", "/runs/run_exec_123")
            .with_detail("artifacts", "/runs/run_exec_123/artifacts")
            .with_detail("exit_code", "2")
            .with_detail("timeout_ms", "30000")
            .with_detail("stdout_truncated", "true")
            .with_detail("stderr_truncated", "false")
            .with_detail("command", "printf secret-token")
            .with_detail("raw_command", "curl -H Authorization: Bearer secret-token")
            .with_detail("args", "--token secret-token")
            .with_detail("stdout", "stdout secret-token")
            .with_detail("raw_stdout", "raw stdout secret-token")
            .with_detail("stderr", "stderr secret-token")
            .with_detail("raw_stderr", "raw stderr secret-token")
            .with_detail("env", "API_KEY=secret-token")
            .with_detail("environment", "STRATUM_TOKEN=secret-token")
            .with_detail("process_environment", "PASSWORD=secret-token")
            .with_detail("cwd", "/tmp/private-workdir")
            .with_detail("temp_dir", "/tmp/private-exec")
            .with_detail("workspace_backing_path", "/srv/private/workspaces/demo")
            .with_detail("api_key_hint", "api_key=secret-token");

        let persisted = store.append(event).await.unwrap();

        let published = sink.published().await;
        assert_eq!(published.len(), 1);
        let payload = &published[0];
        assert_eq!(payload.class, AuditExportClass::Run);
        assert_eq!(payload.action, AuditAction::RunExecuteFailure);
        for (key, expected) in [
            ("workspace_id", "workspace-123"),
            ("job_id", "job-123"),
            ("run_id", "run_exec_123"),
            ("status", "failed"),
            ("root", "/runs/run_exec_123"),
            ("artifacts", "/runs/run_exec_123/artifacts"),
            ("exit_code", "2"),
            ("timeout_ms", "30000"),
            ("stdout_truncated", "true"),
            ("stderr_truncated", "false"),
        ] {
            assert_eq!(payload.details.get(key).map(String::as_str), Some(expected));
        }
        for forbidden_key in [
            "command",
            "raw_command",
            "args",
            "stdout",
            "raw_stdout",
            "stderr",
            "raw_stderr",
            "env",
            "environment",
            "process_environment",
            "cwd",
            "temp_dir",
            "workspace_backing_path",
            "api_key_hint",
        ] {
            assert!(
                !payload.details.contains_key(forbidden_key),
                "export retained sensitive detail {forbidden_key}"
            );
        }
        let exported = serde_json::to_string(payload).unwrap();
        for forbidden in [
            "secret-token",
            "Authorization:",
            "/tmp/private",
            "/srv/private",
            "API_KEY",
            "PASSWORD",
            "STRATUM_TOKEN",
        ] {
            assert!(!exported.contains(forbidden), "export leaked {forbidden}");
        }

        let persisted_details = store.list_recent(1).await.unwrap()[0].details.clone();
        assert_eq!(
            persisted_details.get("command").map(String::as_str),
            Some("printf secret-token")
        );
        assert_eq!(
            persisted_details.get("stdout").map(String::as_str),
            Some("stdout secret-token")
        );
        assert_eq!(
            persisted_details.get("env").map(String::as_str),
            Some("API_KEY=secret-token")
        );
        assert_eq!(persisted.details, persisted_details);
    }

    #[tokio::test]
    async fn exporting_store_reports_last_error_code_from_most_recent_failure() {
        let first_id = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").expect("valid uuid");
        let second_id =
            Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("valid uuid");
        let primary = Arc::new(FixedAuditStore::new([
            fixed_audit_event(first_id, 1, "/demo/first-failure.md"),
            fixed_audit_event(second_id, 2, "/demo/second-failure.md"),
        ]));
        let sink = Arc::new(SequencedFailureAuditEventSink::new([
            "first_export_failure",
            "second_export_failure",
        ]));
        let store = ExportingAuditStore::new(primary, sink)
            .with_policy(AuditExportPolicy::default().with_max_attempts(1));

        store
            .append(fs_write_event("/demo/first-failure.md"))
            .await
            .unwrap();
        store
            .append(fs_write_event("/demo/second-failure.md"))
            .await
            .unwrap();

        let metrics = store.export_metrics().await;
        assert_eq!(
            metrics.last_error_code.as_deref(),
            Some("second_export_failure")
        );
    }

    #[tokio::test]
    async fn exporting_store_does_not_republish_existing_vcs_audit_event() {
        let sink = Arc::new(ControllableAuditEventSink::new());
        let store = exporting_store(sink.clone(), AuditExportPolicy::default());

        let first = store.append(vcs_commit_event("commit-a")).await.unwrap();
        sink.fail_next(1).await;
        let duplicate = store.append(vcs_commit_event("commit-a")).await.unwrap();

        assert_eq!(duplicate, first);
        let published = sink.published().await;
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].event_id, first.id);
        assert_eq!(
            store.delivery_status(first.id).await.unwrap(),
            AuditExportDeliveryStatus::Delivered
        );
    }

    async fn assert_contains_vcs_commit_contract(store: &dyn AuditStore) {
        let first_commit = store.append(vcs_commit_event("commit-a")).await.unwrap();
        let duplicate_commit = store.append(vcs_commit_event("commit-a")).await.unwrap();
        assert_eq!(duplicate_commit, first_commit);
        store
            .append(NewAuditEvent::new(
                AuditActor::new(0, "root"),
                AuditAction::VcsRevert,
                AuditResource::id(AuditResourceKind::Commit, "revert-commit-a"),
            ))
            .await
            .unwrap();
        store
            .append(NewAuditEvent::new(
                AuditActor::new(0, "root"),
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Ref, "commit-a"),
            ))
            .await
            .unwrap();
        store
            .append(
                NewAuditEvent::new(
                    AuditActor::new(0, "root"),
                    AuditAction::FsWriteFile,
                    AuditResource::id(AuditResourceKind::Commit, "commit-b"),
                )
                .with_detail("private_commit_id", "commit-a"),
            )
            .await
            .unwrap();
        store
            .append(NewAuditEvent::new(
                AuditActor::new(0, "root"),
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Commit, "commit-with-path")
                    .with_path("/private/path"),
            ))
            .await
            .unwrap();

        assert!(store.contains_vcs_commit_event("commit-a").await.unwrap());
        assert!(
            store
                .contains_vcs_commit_event("revert-commit-a")
                .await
                .unwrap()
        );
        assert!(!store.contains_vcs_commit_event("commit-b").await.unwrap());
        assert!(
            !store
                .contains_vcs_commit_event("commit-with-path")
                .await
                .unwrap()
        );
        assert!(
            !store
                .contains_vcs_commit_event("do-not-match-this-detail")
                .await
                .unwrap()
        );
        let recent = store.list_recent(10).await.unwrap();
        assert_eq!(
            recent
                .iter()
                .filter(|event| {
                    event.action == AuditAction::VcsCommit
                        && event.resource.kind == AuditResourceKind::Commit
                        && event.resource.id.as_deref() == Some("commit-a")
                        && event.resource.path.is_none()
                })
                .count(),
            1
        );
    }

    async fn assert_contains_fs_mutation_recovery_contract(store: &dyn AuditStore) {
        store
            .append(fs_mutation_recovery_event(
                AuditAction::FsWriteFile,
                "op-a",
                "agent/demo/session",
                "new-commit-a",
            ))
            .await
            .unwrap();
        store
            .append(fs_mutation_recovery_event(
                AuditAction::FsWriteFile,
                "op-b",
                "agent/demo/session",
                "new-commit-b",
            ))
            .await
            .unwrap();
        store
            .append(NewAuditEvent::new(
                AuditActor::new(0, "root"),
                AuditAction::VcsCommit,
                AuditResource::path(AuditResourceKind::Path, "/docs/recovered.md"),
            ))
            .await
            .unwrap();

        assert!(
            store
                .contains_fs_mutation_recovery_event(
                    AuditAction::FsWriteFile,
                    "op-a",
                    "agent/demo/session",
                    "new-commit-a",
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .contains_fs_mutation_recovery_event(
                    AuditAction::FsDelete,
                    "op-a",
                    "agent/demo/session",
                    "new-commit-a",
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .contains_fs_mutation_recovery_event(
                    AuditAction::FsWriteFile,
                    "op-a",
                    "agent/demo/session",
                    "new-commit-b",
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn in_memory_contains_vcs_commit_event_matches_only_action_resource_and_id() {
        let store = InMemoryAuditStore::new();

        assert_contains_vcs_commit_contract(&store).await;
    }

    #[tokio::test]
    async fn local_contains_vcs_commit_event_matches_only_action_resource_and_id() {
        let path = temp_audit_path("contains-vcs-commit");
        let store = LocalAuditStore::open(&path).unwrap();

        assert_contains_vcs_commit_contract(&store).await;
    }

    #[tokio::test]
    async fn in_memory_contains_fs_mutation_recovery_event_matches_recovery_identity() {
        let store = InMemoryAuditStore::new();

        assert_contains_fs_mutation_recovery_contract(&store).await;
    }

    #[tokio::test]
    async fn local_contains_fs_mutation_recovery_event_matches_recovery_identity() {
        let path = temp_audit_path("contains-fs-mutation-recovery");
        let store = LocalAuditStore::open(&path).unwrap();

        assert_contains_fs_mutation_recovery_contract(&store).await;
    }

    #[tokio::test]
    async fn in_memory_append_once_is_atomic_for_concurrent_vcs_commit_identity() {
        let store = Arc::new(InMemoryAuditStore::new());
        let identity = AuditAppendIdentity::VcsVisibleCommit {
            repo_id: "repo-concurrent".to_string(),
            commit_id: "same-commit".to_string(),
        };

        let first_store = store.clone();
        let first_identity = identity.clone();
        let first = tokio::spawn(async move {
            first_store
                .append_once(
                    vcs_commit_event_for_repo("same-commit", "repo-concurrent"),
                    first_identity,
                )
                .await
        });
        let second_store = store.clone();
        let second = tokio::spawn(async move {
            second_store
                .append_once(
                    vcs_commit_event_for_repo("same-commit", "repo-concurrent"),
                    identity,
                )
                .await
        });

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        let appended = [first, second]
            .iter()
            .filter(|outcome| matches!(outcome, AuditAppendOutcome::Appended(_)))
            .count();

        assert_eq!(appended, 1);
        assert_eq!(store.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn local_append_once_persists_only_one_vcs_commit_identity() {
        let path = temp_audit_path("append-once-local-vcs");
        let store = LocalAuditStore::open(&path).unwrap();
        let identity = AuditAppendIdentity::VcsVisibleCommit {
            repo_id: "repo-local".to_string(),
            commit_id: "same-local-commit".to_string(),
        };

        let first = store
            .append_once(
                vcs_commit_event_for_repo("same-local-commit", "repo-local"),
                identity.clone(),
            )
            .await
            .unwrap();
        let second = store
            .append_once(
                vcs_commit_event_for_repo("same-local-commit", "repo-local"),
                identity,
            )
            .await
            .unwrap();

        assert!(matches!(first, AuditAppendOutcome::Appended(_)));
        assert_eq!(second, AuditAppendOutcome::AlreadyPresent);
        drop(store);

        let reloaded = LocalAuditStore::open(&path).unwrap();
        let events = reloaded.list_recent(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[0].resource.id.as_deref(), Some("same-local-commit"));
    }

    #[tokio::test]
    async fn append_once_scopes_vcs_commit_identity_by_repo() {
        let store = InMemoryAuditStore::new();
        let repo_a = "repo-a";
        let repo_b = "repo-b";
        let identity_a = AuditAppendIdentity::VcsVisibleCommit {
            repo_id: repo_a.to_string(),
            commit_id: "same-cross-repo-commit".to_string(),
        };
        let identity_b = AuditAppendIdentity::VcsVisibleCommit {
            repo_id: repo_b.to_string(),
            commit_id: "same-cross-repo-commit".to_string(),
        };

        let first = store
            .append_once(
                vcs_commit_event_for_repo("same-cross-repo-commit", repo_a),
                identity_a.clone(),
            )
            .await
            .unwrap();
        let second = store
            .append_once(
                vcs_commit_event_for_repo("same-cross-repo-commit", repo_b),
                identity_b,
            )
            .await
            .unwrap();
        let third = store
            .append_once(
                vcs_commit_event_for_repo("same-cross-repo-commit", repo_a),
                identity_a,
            )
            .await
            .unwrap();

        assert!(matches!(first, AuditAppendOutcome::Appended(_)));
        assert!(matches!(second, AuditAppendOutcome::Appended(_)));
        assert_eq!(third, AuditAppendOutcome::AlreadyPresent);
        assert_eq!(store.list_recent(10).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn append_once_dedupes_fs_mutation_recovery_identity() {
        let store = InMemoryAuditStore::new();
        let identity = AuditAppendIdentity::FsMutationRecovery {
            repo_id: "repo-fs".to_string(),
            action: AuditAction::FsWriteFile,
            operation_id: "op-append-once".to_string(),
            target_ref: "agent/demo/session".to_string(),
            new_commit: "new-append-once-commit".to_string(),
        };

        let first = store
            .append_once(
                fs_mutation_recovery_event_for_repo(
                    AuditAction::FsWriteFile,
                    "repo-fs",
                    "op-append-once",
                    "agent/demo/session",
                    "new-append-once-commit",
                ),
                identity.clone(),
            )
            .await
            .unwrap();
        let second = store
            .append_once(
                fs_mutation_recovery_event_for_repo(
                    AuditAction::FsWriteFile,
                    "repo-fs",
                    "op-append-once",
                    "agent/demo/session",
                    "new-append-once-commit",
                ),
                identity,
            )
            .await
            .unwrap();

        assert!(matches!(first, AuditAppendOutcome::Appended(_)));
        assert_eq!(second, AuditAppendOutcome::AlreadyPresent);
        assert_eq!(store.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn append_once_scopes_fs_mutation_recovery_identity_by_repo() {
        let store = InMemoryAuditStore::new();
        let repo_a = "repo-a";
        let repo_b = "repo-b";
        let identity_a = AuditAppendIdentity::FsMutationRecovery {
            repo_id: repo_a.to_string(),
            action: AuditAction::FsWriteFile,
            operation_id: "op-cross-repo".to_string(),
            target_ref: "agent/demo/session".to_string(),
            new_commit: "same-fs-new-commit".to_string(),
        };
        let identity_b = AuditAppendIdentity::FsMutationRecovery {
            repo_id: repo_b.to_string(),
            action: AuditAction::FsWriteFile,
            operation_id: "op-cross-repo".to_string(),
            target_ref: "agent/demo/session".to_string(),
            new_commit: "same-fs-new-commit".to_string(),
        };

        let first = store
            .append_once(
                fs_mutation_recovery_event_for_repo(
                    AuditAction::FsWriteFile,
                    repo_a,
                    "op-cross-repo",
                    "agent/demo/session",
                    "same-fs-new-commit",
                ),
                identity_a.clone(),
            )
            .await
            .unwrap();
        let second = store
            .append_once(
                fs_mutation_recovery_event_for_repo(
                    AuditAction::FsWriteFile,
                    repo_b,
                    "op-cross-repo",
                    "agent/demo/session",
                    "same-fs-new-commit",
                ),
                identity_b,
            )
            .await
            .unwrap();
        let third = store
            .append_once(
                fs_mutation_recovery_event_for_repo(
                    AuditAction::FsWriteFile,
                    repo_a,
                    "op-cross-repo",
                    "agent/demo/session",
                    "same-fs-new-commit",
                ),
                identity_a,
            )
            .await
            .unwrap();

        assert!(matches!(first, AuditAppendOutcome::Appended(_)));
        assert!(matches!(second, AuditAppendOutcome::Appended(_)));
        assert_eq!(third, AuditAppendOutcome::AlreadyPresent);
        assert_eq!(store.list_recent(10).await.unwrap().len(), 2);
    }

    #[test]
    fn audit_workspace_context_projects_mounted_root_without_backing_path() {
        let workspace_id = Uuid::new_v4();
        let session = Session::new(42, 42, vec![42], "durable-agent".to_string())
            .with_workspace_mount(
                workspace_id,
                "/srv/private/workspaces/acme",
                "main",
                Some("agent/acme/session"),
            )
            .unwrap();

        let context = AuditWorkspaceContext::from_session(&session).unwrap();

        assert_eq!(context.id, workspace_id);
        assert_eq!(context.root_path, "/");
        assert_eq!(context.base_ref, "main");
        assert_eq!(context.session_ref.as_deref(), Some("agent/acme/session"));
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

    #[test]
    fn policy_decision_audit_enums_round_trip_as_snake_case() {
        assert_eq!(
            serde_json::to_value(AuditAction::PolicyDecisionAllow).unwrap(),
            serde_json::json!("policy_decision_allow")
        );
        assert_eq!(
            serde_json::to_value(AuditAction::PolicyDecisionDeny).unwrap(),
            serde_json::json!("policy_decision_deny")
        );
        assert_eq!(
            serde_json::from_value::<AuditAction>(serde_json::json!("policy_decision_allow"))
                .unwrap(),
            AuditAction::PolicyDecisionAllow
        );
        assert_eq!(
            serde_json::to_value(AuditResourceKind::PolicyDecision).unwrap(),
            serde_json::json!("policy_decision")
        );
        assert_eq!(
            serde_json::from_value::<AuditResourceKind>(serde_json::json!("policy_decision"))
                .unwrap(),
            AuditResourceKind::PolicyDecision
        );
    }

    #[test]
    fn auth_lifecycle_audit_enums_round_trip_as_snake_case() {
        let action_pairs = [
            (AuditAction::AuthOidcLoginDenied, "auth_oidc_login_denied"),
            (AuditAction::AuthOidcLoginSuccess, "auth_oidc_login_success"),
            (AuditAction::AuthSamlLoginDenied, "auth_saml_login_denied"),
            (AuditAction::AuthSamlLoginSuccess, "auth_saml_login_success"),
            (
                AuditAction::AuthRefreshTokenIssue,
                "auth_refresh_token_issue",
            ),
            (
                AuditAction::AuthRefreshTokenRotate,
                "auth_refresh_token_rotate",
            ),
            (
                AuditAction::AuthRefreshTokenRevoke,
                "auth_refresh_token_revoke",
            ),
            (
                AuditAction::AuthRefreshTokenExpireDenied,
                "auth_refresh_token_expire_denied",
            ),
            (
                AuditAction::AuthRefreshTokenReuseDenied,
                "auth_refresh_token_reuse_denied",
            ),
            (
                AuditAction::AuthScimRequestDenied,
                "auth_scim_request_denied",
            ),
            (
                AuditAction::AuthScimUserProvision,
                "auth_scim_user_provision",
            ),
            (AuditAction::AuthScimUserUpdate, "auth_scim_user_update"),
            (
                AuditAction::AuthScimUserDeactivate,
                "auth_scim_user_deactivate",
            ),
            (
                AuditAction::AuthScimGroupProvision,
                "auth_scim_group_provision",
            ),
            (AuditAction::AuthScimGroupUpdate, "auth_scim_group_update"),
            (
                AuditAction::AuthScimGroupMemberAdd,
                "auth_scim_group_member_add",
            ),
            (
                AuditAction::AuthScimGroupMemberRemove,
                "auth_scim_group_member_remove",
            ),
        ];
        for (action, serialized) in action_pairs {
            assert_eq!(
                serde_json::to_value(action).unwrap(),
                serde_json::json!(serialized)
            );
            assert_eq!(
                serde_json::from_value::<AuditAction>(serde_json::json!(serialized)).unwrap(),
                action
            );
        }

        let resource_pairs = [
            (AuditResourceKind::AuthProvider, "auth_provider"),
            (AuditResourceKind::ExternalIdentity, "external_identity"),
            (AuditResourceKind::HostedSession, "hosted_session"),
            (AuditResourceKind::RefreshToken, "refresh_token"),
            (AuditResourceKind::ScimClient, "scim_client"),
            (AuditResourceKind::ScimUser, "scim_user"),
            (AuditResourceKind::ScimGroup, "scim_group"),
            (
                AuditResourceKind::ScimGroupMembership,
                "scim_group_membership",
            ),
        ];
        for (kind, serialized) in resource_pairs {
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::json!(serialized)
            );
            assert_eq!(
                serde_json::from_value::<AuditResourceKind>(serde_json::json!(serialized)).unwrap(),
                kind
            );
        }
    }

    #[test]
    fn run_execution_audit_enums_round_trip_as_snake_case_and_export_as_run_class() {
        let action_pairs = [
            (AuditAction::RunExecuteCreate, "run_execute_create"),
            (AuditAction::RunExecuteStart, "run_execute_start"),
            (AuditAction::RunExecuteFinish, "run_execute_finish"),
            (AuditAction::RunExecuteCancel, "run_execute_cancel"),
            (AuditAction::RunExecuteFailure, "run_execute_failure"),
        ];
        for (action, serialized) in action_pairs {
            assert_eq!(
                serde_json::to_value(action).unwrap(),
                serde_json::json!(serialized)
            );
            assert_eq!(
                serde_json::from_value::<AuditAction>(serde_json::json!(serialized)).unwrap(),
                action
            );

            let event = AuditEvent::from_input(1, run_execute_event(action));
            assert_eq!(
                AuditExportPayload::from_event(&event).class,
                AuditExportClass::Run
            );
        }
    }

    #[test]
    fn auth_lifecycle_audit_details_are_bounded() {
        let event = NewAuditEvent::new(
            AuditActor::new(4242, "oidc-user"),
            AuditAction::AuthRefreshTokenRotate,
            AuditResource::id(AuditResourceKind::RefreshToken, "token-id"),
        )
        .with_detail("org_id", "org_demo")
        .with_detail("repo_id", "repo_demo")
        .with_detail("principal_uid", 4242)
        .with_detail("token_family_id", "family-id")
        .with_detail("successor_token_id", "successor-id")
        .with_detail("reason", "rotated");

        assert_eq!(event.details.len(), 6);
        for (key, value) in &event.details {
            assert!(key.len() <= 64, "{key} is too large");
            assert!(value.len() <= 128, "{key} detail is too large");
        }
        let debug = format!("{event:?}");
        for forbidden in [
            "authorization_code",
            "id_token",
            "access_token",
            "refresh_secret",
            "client_secret",
            "token_hash",
        ] {
            assert!(!debug.contains(forbidden), "leaked {forbidden}");
        }
    }
}
