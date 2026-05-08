//! Durable core transaction semantics contract.
//!
//! This module is intentionally landed before the live durable `CoreDb`
//! implementation so the transaction policy is executable and reviewable first.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::audit::{AuditAction, AuditResourceKind, AuditStore, NewAuditEvent};
use crate::backend::{
    CommitRecord, CommitStore, ObjectStore, ObjectWrite, RefExpectation, RefStore, RefUpdate,
    RefVersion, RepoId,
};
use crate::error::VfsError;
use crate::fs::VirtualFs;
use crate::fs::inode::{InodeId, InodeKind};
use crate::idempotency::{IdempotencyReservation, IdempotencyStore};
use crate::store::commit::CommitObject;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::change::{PathMap, diff_path_maps, worktree_path_records};
use crate::vcs::{ChangedPath, CommitId, MAIN_REF, PathKind, PathRecord, RefName};
use crate::workspace::WorkspaceMetadataStore;

const DURABLE_CORE_COMMIT_EXECUTION_NOT_SUPPORTED: &str =
    "durable core commit execution is not supported until durable prerequisites are complete";
const POST_CAS_RECOVERY_MAX_LEASE_DURATION: Duration = Duration::from_secs(300);
const POST_CAS_RECOVERY_MAX_BACKOFF_DURATION: Duration = Duration::from_secs(3600);

/// Ordered durable write steps for core mutation visibility semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DurableCoreTransactionStep {
    IdempotencyReservation,
    AuthPolicyPreflight,
    StagedObjectUpload,
    FinalObjectPromotion,
    ObjectMetadataInsert,
    CommitMetadataInsert,
    RefCompareAndSwap,
    WorkspaceHeadUpdate,
    AuditAppend,
    IdempotencyCompletion,
}

/// Commit visibility checkpoint for a specific step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCoreCommitPoint {
    Uncommitted,
    CommittedVisibilityPoint,
    CommittedPartial,
    CommittedComplete,
}

/// Failure policy class used for classification and recovery routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCoreFailureClass {
    PreRefCompareAndSwap,
    CommitMetadataUnreachableBeforeRefCompareAndSwap,
    FinalObjectPromotedMetadataMissing,
    ObjectMetadataInsertedBeforeCommitMetadata,
    PostRefCompareAndSwap,
}

/// Recovery action required after a failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCoreRecoveryAction {
    AbortIdempotencyReservation,
    RetryRefCompareAndSwapWithUnreachableCommit,
    RepairMetadataAndRetry,
    RetryCommitMetadataInsertThenRefCompareAndSwap,
    CompleteIdempotencyWithCommittedResponse,
}

/// Timing for a step-local failure classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCoreFailureTiming {
    BeforeOrDuringStep,
    AfterStep,
}

/// Final object cleanup policy for failure handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalObjectCleanupDecision {
    NotApplicable,
    PreserveFinalObject,
    DeleteFinalObjectWithMetadataFence,
}

/// Metadata fence proving final-object cleanup is explicitly authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalObjectMetadataFence;

impl FinalObjectMetadataFence {
    pub(crate) fn new() -> Self {
        Self
    }
}

/// Step-level contract describing visibility checkpoints only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCoreStepSemantics {
    pub step: DurableCoreTransactionStep,
    pub commit_point: DurableCoreCommitPoint,
}

/// Failure policy for a specific step and timing boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCoreFailureSemantics {
    step: DurableCoreTransactionStep,
    timing: DurableCoreFailureTiming,
    commit_point: DurableCoreCommitPoint,
    failure_class: DurableCoreFailureClass,
    recovery_action: DurableCoreRecoveryAction,
    mutation_visible_through_target_ref: bool,
    default_rollback_allowed: bool,
    staged_cleanup_allowed: bool,
    metadata_repair_required: bool,
    unreachable_commit_retry_allowed: bool,
    final_object_cleanup: FinalObjectCleanupDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FailureSemanticsRow {
    commit_point: DurableCoreCommitPoint,
    failure_class: DurableCoreFailureClass,
    recovery_action: DurableCoreRecoveryAction,
    mutation_visible_through_target_ref: bool,
    default_rollback_allowed: bool,
    staged_cleanup_allowed: bool,
    metadata_repair_required: bool,
    unreachable_commit_retry_allowed: bool,
    final_object_cleanup: FinalObjectCleanupDecision,
}

/// Current live durable commit execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableCoreCommitLiveExecution {
    Disabled,
}

/// Durable commit prerequisite that must be resolved before live execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DurableCoreCommitPrerequisite {
    DurableObjectByteWrites,
    LiveTreeConstruction,
    SourceFilesystemSnapshot,
    WorkspaceHeadCoupling,
    AuditAndIdempotencyCompletion,
    CommitLockingAndFencing,
    RepairWorker,
}

/// Durable parent metadata observed before a commit transaction starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableCoreCommitParentState {
    Unborn,
    Existing {
        target: CommitId,
        version: RefVersion,
    },
}

/// Metadata-only durable commit preflight snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitMetadataPreflight {
    target_ref: &'static str,
    parent_state: DurableCoreCommitParentState,
    skeleton: DurableCoreCommitExecutorSkeleton,
}

impl DurableCoreCommitMetadataPreflight {
    pub(crate) const fn for_main(parent_state: DurableCoreCommitParentState) -> Self {
        Self {
            target_ref: MAIN_REF,
            parent_state,
            skeleton: DurableCoreCommitExecutorSkeleton::new(),
        }
    }

    pub(crate) const fn target_ref(&self) -> &'static str {
        self.target_ref
    }

    pub(crate) const fn parent_state(&self) -> DurableCoreCommitParentState {
        self.parent_state
    }

    pub(crate) fn ordered_write_path(&self) -> &'static [DurableCoreTransactionStep] {
        self.skeleton.ordered_write_path()
    }

    pub(crate) const fn live_execution_enabled(&self) -> bool {
        self.skeleton.live_execution_enabled()
    }

    pub(crate) fn unresolved_prerequisites(&self) -> &'static [DurableCoreCommitPrerequisite] {
        self.skeleton.unresolved_prerequisites()
    }
}

/// Source snapshot contract used by durable commit object/tree planning.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitSourceSnapshot {
    parent_state: DurableCoreCommitParentState,
    base_path_records: Vec<PathRecord>,
}

impl DurableCoreCommitSourceSnapshot {
    pub(crate) fn new(
        parent_state: DurableCoreCommitParentState,
        base_path_records: Vec<PathRecord>,
    ) -> Self {
        Self {
            parent_state,
            base_path_records,
        }
    }

    pub(crate) const fn unborn() -> Self {
        Self {
            parent_state: DurableCoreCommitParentState::Unborn,
            base_path_records: Vec::new(),
        }
    }

    pub(crate) const fn parent_state(&self) -> DurableCoreCommitParentState {
        self.parent_state
    }

    pub(crate) fn base_path_records(&self) -> &[PathRecord] {
        &self.base_path_records
    }

    pub(crate) async fn from_durable_parent_state(
        repo_id: &RepoId,
        parent_state: DurableCoreCommitParentState,
        commit_store: &dyn CommitStore,
        object_store: &dyn ObjectStore,
    ) -> Result<Self, VfsError> {
        let DurableCoreCommitParentState::Existing { target, .. } = parent_state else {
            return Ok(Self::unborn());
        };

        let parent = commit_store
            .get(repo_id, target)
            .await
            .map_err(|_| redacted_durable_parent_source_snapshot_error())?
            .ok_or_else(redacted_durable_parent_source_snapshot_error)?;
        if parent.repo_id != *repo_id || parent.id != target {
            return Err(redacted_durable_parent_source_snapshot_error());
        }

        let base_path_records =
            durable_parent_path_records(repo_id, parent.root_tree, object_store).await?;
        Ok(Self::new(parent_state, base_path_records))
    }
}

impl fmt::Debug for DurableCoreCommitSourceSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommitSourceSnapshot")
            .field("parent_state", &self.parent_state)
            .field("base_path_record_count", &self.base_path_records.len())
            .finish()
    }
}

/// Planned durable object bytes for later idempotent object convergence.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCorePlannedObject {
    kind: ObjectKind,
    id: ObjectId,
    bytes: Vec<u8>,
}

impl DurableCorePlannedObject {
    pub(crate) const fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub(crate) const fn id(&self) -> ObjectId {
        self.id
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn object_write_for_repo(&self, repo_id: &RepoId) -> ObjectWrite {
        ObjectWrite {
            repo_id: repo_id.clone(),
            id: self.id,
            kind: self.kind,
            bytes: self.bytes.clone(),
        }
    }
}

impl fmt::Debug for DurableCorePlannedObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePlannedObject")
            .field("kind", &self.kind)
            .field("id", &self.id)
            .field("byte_len", &self.bytes.len())
            .finish()
    }
}

/// Redacted metadata for a planned object that has converged into durable storage.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreConvergedObject {
    kind: ObjectKind,
    id: ObjectId,
    byte_len: usize,
}

impl DurableCoreConvergedObject {
    pub(crate) const fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub(crate) const fn id(&self) -> ObjectId {
        self.id
    }

    pub(crate) const fn byte_len(&self) -> usize {
        self.byte_len
    }
}

impl fmt::Debug for DurableCoreConvergedObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreConvergedObject")
            .field("kind", &self.kind)
            .field("id", &self.id)
            .field("byte_len", &self.byte_len)
            .finish()
    }
}

/// Redacted summary of planned object convergence for a durable commit.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreObjectConvergence {
    repo_id: RepoId,
    root_tree_id: ObjectId,
    objects: Vec<DurableCoreConvergedObject>,
}

impl DurableCoreObjectConvergence {
    pub(crate) fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub(crate) const fn root_tree_id(&self) -> ObjectId {
        self.root_tree_id
    }

    pub(crate) fn objects(&self) -> &[DurableCoreConvergedObject] {
        &self.objects
    }

    pub(crate) fn object_count(&self) -> usize {
        self.objects.len()
    }
}

impl fmt::Debug for DurableCoreObjectConvergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreObjectConvergence")
            .field("repo_id", &self.repo_id)
            .field("root_tree_id", &self.root_tree_id)
            .field("object_count", &self.objects.len())
            .field("objects", &self.objects)
            .finish()
    }
}

/// Redacted summary of inserted commit metadata for an unreachable durable commit.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitMetadataInsert {
    repo_id: RepoId,
    commit_id: CommitId,
    root_tree_id: ObjectId,
    parents: Vec<CommitId>,
    changed_path_count: usize,
    timestamp: u64,
    plan_fingerprint: ObjectId,
}

impl DurableCoreCommitMetadataInsert {
    pub(crate) fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub(crate) const fn commit_id(&self) -> CommitId {
        self.commit_id
    }

    pub(crate) const fn root_tree_id(&self) -> ObjectId {
        self.root_tree_id
    }

    pub(crate) fn parents(&self) -> &[CommitId] {
        &self.parents
    }

    pub(crate) const fn changed_path_count(&self) -> usize {
        self.changed_path_count
    }

    pub(crate) const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl fmt::Debug for DurableCoreCommitMetadataInsert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommitMetadataInsert")
            .field("repo_id", &self.repo_id)
            .field("commit_id", &self.commit_id)
            .field("root_tree_id", &self.root_tree_id)
            .field("parents", &self.parents)
            .field("changed_path_count", &self.changed_path_count)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

/// Redacted summary of the durable commit compare-and-swap visibility step.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitRefCasVisibility {
    repo_id: RepoId,
    ref_name: &'static str,
    commit_id: CommitId,
    version: RefVersion,
}

impl DurableCoreCommitRefCasVisibility {
    pub(crate) fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub(crate) const fn ref_name(&self) -> &'static str {
        self.ref_name
    }

    pub(crate) const fn commit_id(&self) -> CommitId {
        self.commit_id
    }

    pub(crate) const fn version(&self) -> RefVersion {
        self.version
    }
}

impl fmt::Debug for DurableCoreCommitRefCasVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommitRefCasVisibility")
            .field("repo_id", &self.repo_id)
            .field("ref_name", &self.ref_name)
            .field("commit_id", &self.commit_id)
            .field("version", &self.version)
            .finish()
    }
}

/// Post-CAS completion step that can be claimed independently by recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DurableCorePostCasStep {
    WorkspaceHeadUpdate,
    AuditAppend,
    IdempotencyCompletion,
}

impl DurableCorePostCasStep {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceHeadUpdate => "workspace_head_update",
            Self::AuditAppend => "audit_append",
            Self::IdempotencyCompletion => "idempotency_completion",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self, VfsError> {
        match value {
            "workspace_head_update" => Ok(Self::WorkspaceHeadUpdate),
            "audit_append" => Ok(Self::AuditAppend),
            "idempotency_completion" => Ok(Self::IdempotencyCompletion),
            _ => Err(VfsError::CorruptStore {
                message: "post-CAS recovery step is invalid".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurableCorePostCasIdempotencyResponseKind {
    FullCommit,
    Partial,
}

/// Persisted idempotency repair inputs. Debug output never exposes hashes or tokens.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableCorePostCasIdempotencyRecoveryContext {
    scope: String,
    key_hash: String,
    request_fingerprint: String,
    reservation_token: String,
    response_kind: DurableCorePostCasIdempotencyResponseKind,
}

impl DurableCorePostCasIdempotencyRecoveryContext {
    pub(crate) fn new(
        scope: impl Into<String>,
        key_hash: impl Into<String>,
        request_fingerprint: impl Into<String>,
        reservation_token: impl Into<String>,
        response_kind: DurableCorePostCasIdempotencyResponseKind,
    ) -> Self {
        Self {
            scope: scope.into(),
            key_hash: key_hash.into(),
            request_fingerprint: request_fingerprint.into(),
            reservation_token: reservation_token.into(),
            response_kind,
        }
    }

    pub(crate) fn from_reservation(
        reservation: &IdempotencyReservation,
        response_kind: DurableCorePostCasIdempotencyResponseKind,
    ) -> Self {
        Self::new(
            reservation.scope(),
            reservation.key_hash(),
            reservation.request_fingerprint(),
            reservation.reservation_token(),
            response_kind,
        )
    }

    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    pub(crate) fn key_hash(&self) -> &str {
        &self.key_hash
    }

    pub(crate) fn request_fingerprint(&self) -> &str {
        &self.request_fingerprint
    }

    pub(crate) fn reservation_token(&self) -> &str {
        &self.reservation_token
    }

    pub(crate) const fn response_kind(&self) -> DurableCorePostCasIdempotencyResponseKind {
        self.response_kind
    }
}

impl fmt::Debug for DurableCorePostCasIdempotencyRecoveryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasIdempotencyRecoveryContext")
            .field("has_scope", &(!self.scope.is_empty()))
            .field("has_key_hash", &(!self.key_hash.is_empty()))
            .field(
                "has_request_fingerprint",
                &(!self.request_fingerprint.is_empty()),
            )
            .field(
                "has_reservation_token",
                &(!self.reservation_token.is_empty()),
            )
            .field("response_kind", &self.response_kind)
            .field("context", &"<redacted>")
            .finish()
    }
}

/// Persisted post-CAS repair inputs. Debug output intentionally exposes only shape.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableCorePostCasRecoveryContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_workspace_head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    audit_event: Option<NewAuditEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency: Option<DurableCorePostCasIdempotencyRecoveryContext>,
}

impl DurableCorePostCasRecoveryContext {
    pub(crate) fn new(
        workspace_id: Option<Uuid>,
        expected_workspace_head: Option<String>,
        audit_event: Option<NewAuditEvent>,
        idempotency: Option<DurableCorePostCasIdempotencyRecoveryContext>,
    ) -> Self {
        Self {
            workspace_id,
            expected_workspace_head,
            audit_event,
            idempotency,
        }
    }

    pub(crate) const fn workspace_id(&self) -> Option<Uuid> {
        self.workspace_id
    }

    pub(crate) fn expected_workspace_head(&self) -> Option<&str> {
        self.expected_workspace_head.as_deref()
    }

    pub(crate) fn audit_event(&self) -> Option<&NewAuditEvent> {
        self.audit_event.as_ref()
    }

    pub(crate) fn idempotency_response_kind(
        &self,
    ) -> Option<DurableCorePostCasIdempotencyResponseKind> {
        self.idempotency
            .as_ref()
            .map(|context| context.response_kind())
    }

    pub(crate) fn idempotency(&self) -> Option<&DurableCorePostCasIdempotencyRecoveryContext> {
        self.idempotency.as_ref()
    }
}

impl fmt::Debug for DurableCorePostCasRecoveryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoveryContext")
            .field("has_workspace_id", &self.workspace_id.is_some())
            .field(
                "has_expected_workspace_head",
                &self.expected_workspace_head.is_some(),
            )
            .field("has_audit_event", &self.audit_event.is_some())
            .field(
                "idempotency_response_kind",
                &self.idempotency_response_kind(),
            )
            .field("has_idempotency", &self.idempotency.is_some())
            .field("context", &"<redacted>")
            .finish()
    }
}

/// Recovery target identity for one post-CAS completion step.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DurableCorePostCasRecoveryTarget {
    repo_id: RepoId,
    ref_name: String,
    commit_id: CommitId,
    step: DurableCorePostCasStep,
}

impl DurableCorePostCasRecoveryTarget {
    pub(crate) fn new(
        repo_id: RepoId,
        ref_name: &str,
        commit_id: CommitId,
        step: DurableCorePostCasStep,
    ) -> Result<Self, VfsError> {
        if ref_name != MAIN_REF {
            return Err(VfsError::InvalidArgs {
                message: "post-CAS recovery only supports the main ref".to_string(),
            });
        }
        RefName::new(ref_name).map_err(|_| VfsError::InvalidArgs {
            message: "post-CAS recovery target ref is invalid".to_string(),
        })?;

        Ok(Self {
            repo_id,
            ref_name: ref_name.to_string(),
            commit_id,
            step,
        })
    }

    pub(crate) fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub(crate) fn ref_name(&self) -> &str {
        &self.ref_name
    }

    pub(crate) const fn commit_id(&self) -> CommitId {
        self.commit_id
    }

    pub(crate) const fn step(&self) -> DurableCorePostCasStep {
        self.step
    }
}

impl fmt::Debug for DurableCorePostCasRecoveryTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoveryTarget")
            .field("repo_id", &self.repo_id)
            .field("ref_name", &self.ref_name)
            .field("commit_id", &self.commit_id)
            .field("step", &self.step)
            .finish()
    }
}

/// Claim request with caller-supplied testable clock time.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCorePostCasRecoveryClaimRequest {
    target: DurableCorePostCasRecoveryTarget,
    lease_owner: String,
    lease_duration: Duration,
    now_millis: u64,
}

impl DurableCorePostCasRecoveryClaimRequest {
    pub(crate) fn new(
        target: DurableCorePostCasRecoveryTarget,
        lease_owner: &str,
        lease_duration: Duration,
        now_millis: u64,
    ) -> Result<Self, VfsError> {
        if lease_owner.trim().is_empty()
            || lease_owner.len() > 128
            || lease_owner.chars().any(char::is_control)
        {
            return Err(VfsError::InvalidArgs {
                message: "post-CAS recovery lease owner must be 1-128 non-control characters"
                    .to_string(),
            });
        }
        if lease_duration.as_millis() == 0 {
            return Err(VfsError::InvalidArgs {
                message: "post-CAS recovery lease duration must be at least 1 millisecond"
                    .to_string(),
            });
        }
        if lease_duration > POST_CAS_RECOVERY_MAX_LEASE_DURATION {
            return Err(VfsError::InvalidArgs {
                message: "post-CAS recovery lease duration exceeds maximum".to_string(),
            });
        }

        Ok(Self {
            target,
            lease_owner: lease_owner.to_string(),
            lease_duration,
            now_millis,
        })
    }

    pub(crate) fn target(&self) -> &DurableCorePostCasRecoveryTarget {
        &self.target
    }

    pub(crate) fn lease_owner(&self) -> &str {
        &self.lease_owner
    }

    pub(crate) const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) const fn now_millis(&self) -> u64 {
        self.now_millis
    }
}

impl fmt::Debug for DurableCorePostCasRecoveryClaimRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoveryClaimRequest")
            .field("target", &self.target)
            .field("lease_owner", &"<redacted>")
            .field("lease_duration", &self.lease_duration)
            .field("now_millis", &self.now_millis)
            .finish()
    }
}

/// Active recovery claim token.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCorePostCasRecoveryClaim {
    target: DurableCorePostCasRecoveryTarget,
    lease_owner: String,
    token: String,
    attempts: u32,
    expires_at_millis: u64,
    context: Option<DurableCorePostCasRecoveryContext>,
}

impl DurableCorePostCasRecoveryClaim {
    pub(crate) fn for_store(
        target: DurableCorePostCasRecoveryTarget,
        lease_owner: impl Into<String>,
        token: impl Into<String>,
        attempts: u32,
        expires_at_millis: u64,
    ) -> Self {
        Self {
            target,
            lease_owner: lease_owner.into(),
            token: token.into(),
            attempts,
            expires_at_millis,
            context: None,
        }
    }

    pub(crate) fn for_store_with_context(
        target: DurableCorePostCasRecoveryTarget,
        lease_owner: impl Into<String>,
        token: impl Into<String>,
        attempts: u32,
        expires_at_millis: u64,
        context: Option<DurableCorePostCasRecoveryContext>,
    ) -> Self {
        Self {
            target,
            lease_owner: lease_owner.into(),
            token: token.into(),
            attempts,
            expires_at_millis,
            context,
        }
    }

    pub(crate) fn target(&self) -> &DurableCorePostCasRecoveryTarget {
        &self.target
    }

    pub(crate) fn lease_owner(&self) -> &str {
        &self.lease_owner
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub(crate) const fn attempts(&self) -> u32 {
        self.attempts
    }

    pub(crate) const fn expires_at_millis(&self) -> u64 {
        self.expires_at_millis
    }

    pub(crate) fn context(&self) -> Option<&DurableCorePostCasRecoveryContext> {
        self.context.as_ref()
    }
}

impl fmt::Debug for DurableCorePostCasRecoveryClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoveryClaim")
            .field("target", &self.target)
            .field("lease_owner", &"<redacted>")
            .field("token", &"<redacted>")
            .field("attempts", &self.attempts)
            .field("expires_at_millis", &self.expires_at_millis)
            .field("context", &self.context)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableCorePostCasRecoveryState {
    Pending,
    Active,
    BackingOff,
    Completed,
    Poisoned,
}

impl DurableCorePostCasRecoveryState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::BackingOff => "backing_off",
            Self::Completed => "completed",
            Self::Poisoned => "poisoned",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self, VfsError> {
        match value {
            "pending" => Ok(Self::Pending),
            "active" => Ok(Self::Active),
            "backing_off" => Ok(Self::BackingOff),
            "completed" => Ok(Self::Completed),
            "poisoned" => Ok(Self::Poisoned),
            _ => Err(VfsError::CorruptStore {
                message: "post-CAS recovery state is invalid".to_string(),
            }),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCorePostCasRecoveryStatus {
    target: DurableCorePostCasRecoveryTarget,
    state: DurableCorePostCasRecoveryState,
    attempts: u32,
    lease_expires_at_millis: Option<u64>,
    retry_after_millis: Option<u64>,
    terminal_at_millis: Option<u64>,
    diagnosis: Option<DurableCorePostCasRedactedDiagnosis>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableCorePostCasRecoveryCounts {
    pending: usize,
    active: usize,
    backing_off: usize,
    completed: usize,
    poisoned: usize,
}

impl DurableCorePostCasRecoveryCounts {
    pub(crate) const fn pending(&self) -> usize {
        self.pending
    }

    pub(crate) const fn active(&self) -> usize {
        self.active
    }

    pub(crate) const fn backing_off(&self) -> usize {
        self.backing_off
    }

    pub(crate) const fn completed(&self) -> usize {
        self.completed
    }

    pub(crate) const fn poisoned(&self) -> usize {
        self.poisoned
    }

    pub(crate) const fn total(&self) -> usize {
        self.pending + self.active + self.backing_off + self.completed + self.poisoned
    }

    pub(crate) fn add(&mut self, state: DurableCorePostCasRecoveryState, count: usize) {
        match state {
            DurableCorePostCasRecoveryState::Pending => self.pending += count,
            DurableCorePostCasRecoveryState::Active => self.active += count,
            DurableCorePostCasRecoveryState::BackingOff => self.backing_off += count,
            DurableCorePostCasRecoveryState::Completed => self.completed += count,
            DurableCorePostCasRecoveryState::Poisoned => self.poisoned += count,
        }
    }

    fn increment(&mut self, state: DurableCorePostCasRecoveryState) {
        self.add(state, 1);
    }
}

impl DurableCorePostCasRecoveryStatus {
    pub(crate) fn for_store(
        target: DurableCorePostCasRecoveryTarget,
        state: DurableCorePostCasRecoveryState,
        attempts: u32,
        lease_expires_at_millis: Option<u64>,
        retry_after_millis: Option<u64>,
        terminal_at_millis: Option<u64>,
        has_redacted_diagnosis: bool,
    ) -> Self {
        Self {
            target,
            state,
            attempts,
            lease_expires_at_millis,
            retry_after_millis,
            terminal_at_millis,
            diagnosis: has_redacted_diagnosis.then(DurableCorePostCasRedactedDiagnosis::new),
        }
    }

    pub(crate) fn target(&self) -> &DurableCorePostCasRecoveryTarget {
        &self.target
    }

    pub(crate) const fn state(&self) -> DurableCorePostCasRecoveryState {
        self.state
    }

    pub(crate) const fn attempts(&self) -> u32 {
        self.attempts
    }

    pub(crate) const fn lease_expires_at_millis(&self) -> Option<u64> {
        self.lease_expires_at_millis
    }

    pub(crate) const fn retry_after_millis(&self) -> Option<u64> {
        self.retry_after_millis
    }

    pub(crate) const fn terminal_at_millis(&self) -> Option<u64> {
        self.terminal_at_millis
    }

    pub(crate) fn redacted_diagnosis(&self) -> Option<&'static str> {
        self.diagnosis
            .as_ref()
            .map(|_| DurableCorePostCasRedactedDiagnosis::MESSAGE)
    }
}

impl fmt::Debug for DurableCorePostCasRecoveryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoveryStatus")
            .field("target", &self.target)
            .field("state", &self.state)
            .field("attempts", &self.attempts)
            .field("lease_expires_at_millis", &self.lease_expires_at_millis)
            .field("retry_after_millis", &self.retry_after_millis)
            .field("terminal_at_millis", &self.terminal_at_millis)
            .field("diagnosis", &self.diagnosis)
            .finish()
    }
}

#[async_trait::async_trait]
pub(crate) trait DurableCorePostCasRecoveryClaimStore: Send + Sync {
    async fn enqueue(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        now_millis: u64,
    ) -> Result<(), VfsError>;

    async fn enqueue_with_context(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        context: DurableCorePostCasRecoveryContext,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let _ = (target, context, now_millis);
        Err(VfsError::NotSupported {
            message: "post-CAS recovery context persistence is not supported".to_string(),
        })
    }

    async fn claim(
        &self,
        request: DurableCorePostCasRecoveryClaimRequest,
    ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError>;

    async fn complete(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        now_millis: u64,
    ) -> Result<(), VfsError>;

    async fn record_failure(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        diagnosis: &str,
        backoff: Duration,
        now_millis: u64,
    ) -> Result<(), VfsError>;

    async fn poison(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        diagnosis: &str,
        now_millis: u64,
    ) -> Result<(), VfsError>;

    async fn list(&self, limit: usize) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError>;

    async fn list_repair_candidates(
        &self,
        now_millis: u64,
        limit: usize,
    ) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let scan_limit = limit.saturating_mul(32).max(limit).min(limit.max(1_000));
        let mut statuses = self.list(scan_limit).await?;
        statuses.retain(|status| post_cas_recovery_status_is_due(status, now_millis));
        statuses.truncate(limit);
        Ok(statuses)
    }

    async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError>;
}

#[derive(Clone, PartialEq, Eq)]
struct DurableCorePostCasRedactedDiagnosis;

impl DurableCorePostCasRedactedDiagnosis {
    const MESSAGE: &'static str = "redacted post-CAS recovery failure";

    const fn new() -> Self {
        Self
    }
}

impl fmt::Debug for DurableCorePostCasRedactedDiagnosis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::MESSAGE)
    }
}

#[derive(Clone, PartialEq, Eq)]
enum DurableCorePostCasRecoveryEntry {
    Pending {
        attempts: u32,
        enqueued_at_millis: u64,
        context: Option<DurableCorePostCasRecoveryContext>,
    },
    Active {
        lease_owner: String,
        token: String,
        attempts: u32,
        expires_at_millis: u64,
        context: Option<DurableCorePostCasRecoveryContext>,
    },
    BackingOff {
        attempts: u32,
        retry_after_millis: u64,
        diagnosis: DurableCorePostCasRedactedDiagnosis,
        context: Option<DurableCorePostCasRecoveryContext>,
    },
    Completed {
        attempts: u32,
        completed_at_millis: u64,
        context: Option<DurableCorePostCasRecoveryContext>,
    },
    Poisoned {
        attempts: u32,
        poisoned_at_millis: u64,
        diagnosis: DurableCorePostCasRedactedDiagnosis,
        context: Option<DurableCorePostCasRecoveryContext>,
    },
}

impl fmt::Debug for DurableCorePostCasRecoveryEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending {
                attempts,
                enqueued_at_millis,
                context,
            } => f
                .debug_struct("Pending")
                .field("attempts", attempts)
                .field("enqueued_at_millis", enqueued_at_millis)
                .field("context", context)
                .finish(),
            Self::Active {
                attempts,
                expires_at_millis,
                context,
                ..
            } => f
                .debug_struct("Active")
                .field("lease_owner", &"<redacted>")
                .field("token", &"<redacted>")
                .field("attempts", attempts)
                .field("expires_at_millis", expires_at_millis)
                .field("context", context)
                .finish(),
            Self::BackingOff {
                attempts,
                retry_after_millis,
                diagnosis,
                context,
            } => f
                .debug_struct("BackingOff")
                .field("attempts", attempts)
                .field("retry_after_millis", retry_after_millis)
                .field("diagnosis", diagnosis)
                .field("context", context)
                .finish(),
            Self::Completed {
                attempts,
                completed_at_millis,
                context,
            } => f
                .debug_struct("Completed")
                .field("attempts", attempts)
                .field("completed_at_millis", completed_at_millis)
                .field("context", context)
                .finish(),
            Self::Poisoned {
                attempts,
                poisoned_at_millis,
                diagnosis,
                context,
            } => f
                .debug_struct("Poisoned")
                .field("attempts", attempts)
                .field("poisoned_at_millis", poisoned_at_millis)
                .field("diagnosis", diagnosis)
                .field("context", context)
                .finish(),
        }
    }
}

/// In-memory recovery claim store for tests and the future durable adapter contract.
#[derive(Debug, Default)]
pub(crate) struct InMemoryDurableCorePostCasRecoveryClaimStore {
    entries: RwLock<BTreeMap<DurableCorePostCasRecoveryTarget, DurableCorePostCasRecoveryEntry>>,
}

impl InMemoryDurableCorePostCasRecoveryClaimStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn snapshot(&self) -> DurableCorePostCasRecoverySnapshot {
        let guard = self.entries.read().await;
        DurableCorePostCasRecoverySnapshot {
            entries: guard
                .iter()
                .map(|(target, entry)| (target.clone(), entry.clone()))
                .collect(),
        }
    }
}

pub(crate) struct DurableCorePostCasRecoverySnapshot {
    entries: Vec<(
        DurableCorePostCasRecoveryTarget,
        DurableCorePostCasRecoveryEntry,
    )>,
}

impl fmt::Debug for DurableCorePostCasRecoverySnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoverySnapshot")
            .field("entries", &self.entries)
            .finish()
    }
}

#[async_trait::async_trait]
impl DurableCorePostCasRecoveryClaimStore for InMemoryDurableCorePostCasRecoveryClaimStore {
    async fn enqueue(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let mut guard = self.entries.write().await;
        guard
            .entry(target)
            .or_insert(DurableCorePostCasRecoveryEntry::Pending {
                attempts: 0,
                enqueued_at_millis: now_millis,
                context: None,
            });
        Ok(())
    }

    async fn enqueue_with_context(
        &self,
        target: DurableCorePostCasRecoveryTarget,
        context: DurableCorePostCasRecoveryContext,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let mut guard = self.entries.write().await;
        match guard.get_mut(&target) {
            None => {
                guard.insert(
                    target,
                    DurableCorePostCasRecoveryEntry::Pending {
                        attempts: 0,
                        enqueued_at_millis: now_millis,
                        context: Some(context),
                    },
                );
            }
            Some(entry)
                if entry.context().is_some()
                    && entry.state() != DurableCorePostCasRecoveryState::Poisoned => {}
            Some(
                DurableCorePostCasRecoveryEntry::Pending {
                    context: existing_context,
                    ..
                }
                | DurableCorePostCasRecoveryEntry::BackingOff {
                    context: existing_context,
                    ..
                },
            ) if existing_context.is_none() => {
                *existing_context = Some(context);
            }
            Some(_) => return Err(contextual_post_cas_recovery_enqueue_conflict()),
        }
        Ok(())
    }

    async fn list_repair_candidates(
        &self,
        now_millis: u64,
        limit: usize,
    ) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
        let guard = self.entries.read().await;
        Ok(guard
            .iter()
            .map(|(target, entry)| entry.status_for(target.clone()))
            .filter(|status| post_cas_recovery_status_is_due(status, now_millis))
            .take(limit)
            .collect())
    }

    async fn claim(
        &self,
        request: DurableCorePostCasRecoveryClaimRequest,
    ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError> {
        let expires_at_millis = checked_duration_deadline(
            request.now_millis,
            request.lease_duration,
            "post-CAS recovery lease duration overflow",
        )?;
        let mut guard = self.entries.write().await;
        let attempts = match guard.get(&request.target) {
            None => return Ok(None),
            Some(DurableCorePostCasRecoveryEntry::Pending { attempts, .. }) => {
                next_claim_attempt(*attempts)?
            }
            Some(DurableCorePostCasRecoveryEntry::Active {
                attempts,
                expires_at_millis,
                ..
            }) if request.now_millis >= *expires_at_millis => next_claim_attempt(*attempts)?,
            Some(DurableCorePostCasRecoveryEntry::BackingOff {
                attempts,
                retry_after_millis,
                ..
            }) if request.now_millis >= *retry_after_millis => next_claim_attempt(*attempts)?,
            Some(
                DurableCorePostCasRecoveryEntry::Active { .. }
                | DurableCorePostCasRecoveryEntry::BackingOff { .. }
                | DurableCorePostCasRecoveryEntry::Completed { .. }
                | DurableCorePostCasRecoveryEntry::Poisoned { .. },
            ) => return Ok(None),
        };

        let context = guard
            .get(&request.target)
            .and_then(DurableCorePostCasRecoveryEntry::context)
            .cloned();
        let claim = DurableCorePostCasRecoveryClaim {
            target: request.target,
            lease_owner: request.lease_owner,
            token: Uuid::new_v4().to_string(),
            attempts,
            expires_at_millis,
            context,
        };
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::Active {
                lease_owner: claim.lease_owner.clone(),
                token: claim.token.clone(),
                attempts,
                expires_at_millis,
                context: claim.context.clone(),
            },
        );
        Ok(Some(claim))
    }

    async fn complete(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let mut guard = self.entries.write().await;
        let entry = active_entry_for_claim(&guard, claim, now_millis)?;
        let attempts = entry.attempts();
        let context = entry.context().cloned();
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::Completed {
                attempts,
                completed_at_millis: now_millis,
                context,
            },
        );
        Ok(())
    }

    async fn record_failure(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        _diagnosis: &str,
        backoff: Duration,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        validate_post_cas_recovery_backoff(backoff)?;
        let retry_after_millis = checked_duration_deadline(
            now_millis,
            backoff,
            "post-CAS recovery backoff duration overflow",
        )?;
        let mut guard = self.entries.write().await;
        let entry = active_entry_for_claim(&guard, claim, now_millis)?;
        let attempts = entry.attempts();
        let context = entry.context().cloned();
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::BackingOff {
                attempts,
                retry_after_millis,
                diagnosis: DurableCorePostCasRedactedDiagnosis::new(),
                context,
            },
        );
        Ok(())
    }

    async fn poison(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        _diagnosis: &str,
        now_millis: u64,
    ) -> Result<(), VfsError> {
        let mut guard = self.entries.write().await;
        let entry = active_entry_for_claim(&guard, claim, now_millis)?;
        let attempts = entry.attempts();
        let context = entry.context().cloned();
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::Poisoned {
                attempts,
                poisoned_at_millis: now_millis,
                diagnosis: DurableCorePostCasRedactedDiagnosis::new(),
                context,
            },
        );
        Ok(())
    }

    async fn list(&self, limit: usize) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
        let guard = self.entries.read().await;
        Ok(guard
            .iter()
            .take(limit)
            .map(|(target, entry)| entry.status_for(target.clone()))
            .collect())
    }

    async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
        let guard = self.entries.read().await;
        let mut counts = DurableCorePostCasRecoveryCounts::default();
        for entry in guard.values() {
            counts.increment(entry.state());
        }
        Ok(counts)
    }
}

impl DurableCorePostCasRecoveryEntry {
    const fn state(&self) -> DurableCorePostCasRecoveryState {
        match self {
            Self::Pending { .. } => DurableCorePostCasRecoveryState::Pending,
            Self::Active { .. } => DurableCorePostCasRecoveryState::Active,
            Self::BackingOff { .. } => DurableCorePostCasRecoveryState::BackingOff,
            Self::Completed { .. } => DurableCorePostCasRecoveryState::Completed,
            Self::Poisoned { .. } => DurableCorePostCasRecoveryState::Poisoned,
        }
    }

    fn context(&self) -> Option<&DurableCorePostCasRecoveryContext> {
        match self {
            Self::Pending { context, .. }
            | Self::Active { context, .. }
            | Self::BackingOff { context, .. }
            | Self::Completed { context, .. }
            | Self::Poisoned { context, .. } => context.as_ref(),
        }
    }

    const fn attempts(&self) -> u32 {
        match self {
            Self::Pending { attempts, .. }
            | Self::Active { attempts, .. }
            | Self::BackingOff { attempts, .. }
            | Self::Completed { attempts, .. }
            | Self::Poisoned { attempts, .. } => *attempts,
        }
    }

    fn status_for(
        &self,
        target: DurableCorePostCasRecoveryTarget,
    ) -> DurableCorePostCasRecoveryStatus {
        match self {
            Self::Pending { attempts, .. } => DurableCorePostCasRecoveryStatus {
                target,
                state: DurableCorePostCasRecoveryState::Pending,
                attempts: *attempts,
                lease_expires_at_millis: None,
                retry_after_millis: None,
                terminal_at_millis: None,
                diagnosis: None,
            },
            Self::Active {
                attempts,
                expires_at_millis,
                ..
            } => DurableCorePostCasRecoveryStatus {
                target,
                state: DurableCorePostCasRecoveryState::Active,
                attempts: *attempts,
                lease_expires_at_millis: Some(*expires_at_millis),
                retry_after_millis: None,
                terminal_at_millis: None,
                diagnosis: None,
            },
            Self::BackingOff {
                attempts,
                retry_after_millis,
                diagnosis,
                ..
            } => DurableCorePostCasRecoveryStatus {
                target,
                state: DurableCorePostCasRecoveryState::BackingOff,
                attempts: *attempts,
                lease_expires_at_millis: None,
                retry_after_millis: Some(*retry_after_millis),
                terminal_at_millis: None,
                diagnosis: Some(diagnosis.clone()),
            },
            Self::Completed {
                attempts,
                completed_at_millis,
                ..
            } => DurableCorePostCasRecoveryStatus {
                target,
                state: DurableCorePostCasRecoveryState::Completed,
                attempts: *attempts,
                lease_expires_at_millis: None,
                retry_after_millis: None,
                terminal_at_millis: Some(*completed_at_millis),
                diagnosis: None,
            },
            Self::Poisoned {
                attempts,
                poisoned_at_millis,
                diagnosis,
                ..
            } => DurableCorePostCasRecoveryStatus {
                target,
                state: DurableCorePostCasRecoveryState::Poisoned,
                attempts: *attempts,
                lease_expires_at_millis: None,
                retry_after_millis: None,
                terminal_at_millis: Some(*poisoned_at_millis),
                diagnosis: Some(diagnosis.clone()),
            },
        }
    }
}

fn post_cas_recovery_status_is_due(
    status: &DurableCorePostCasRecoveryStatus,
    now_millis: u64,
) -> bool {
    match status.state() {
        DurableCorePostCasRecoveryState::Pending => true,
        DurableCorePostCasRecoveryState::Active => status
            .lease_expires_at_millis()
            .is_some_and(|expires_at| now_millis >= expires_at),
        DurableCorePostCasRecoveryState::BackingOff => status
            .retry_after_millis()
            .is_some_and(|retry_after| now_millis >= retry_after),
        DurableCorePostCasRecoveryState::Completed | DurableCorePostCasRecoveryState::Poisoned => {
            false
        }
    }
}

fn active_entry_for_claim<'a>(
    entries: &'a BTreeMap<DurableCorePostCasRecoveryTarget, DurableCorePostCasRecoveryEntry>,
    claim: &DurableCorePostCasRecoveryClaim,
    now_millis: u64,
) -> Result<&'a DurableCorePostCasRecoveryEntry, VfsError> {
    match entries.get(claim.target()) {
        Some(DurableCorePostCasRecoveryEntry::Active {
            lease_owner,
            token,
            expires_at_millis,
            ..
        }) if lease_owner == claim.lease_owner()
            && token == claim.token()
            && now_millis < *expires_at_millis =>
        {
            entries
                .get(claim.target())
                .ok_or_else(stale_post_cas_recovery_claim)
        }
        _ => Err(VfsError::InvalidArgs {
            message: "post-CAS recovery claim is stale".to_string(),
        }),
    }
}

pub(crate) fn validate_post_cas_recovery_backoff(backoff: Duration) -> Result<(), VfsError> {
    if backoff.as_millis() == 0 {
        return Err(VfsError::InvalidArgs {
            message: "post-CAS recovery backoff duration must be at least 1 millisecond"
                .to_string(),
        });
    }
    if backoff > POST_CAS_RECOVERY_MAX_BACKOFF_DURATION {
        return Err(VfsError::InvalidArgs {
            message: "post-CAS recovery backoff duration exceeds maximum".to_string(),
        });
    }
    Ok(())
}

fn stale_post_cas_recovery_claim() -> VfsError {
    VfsError::InvalidArgs {
        message: "post-CAS recovery claim is stale".to_string(),
    }
}

pub(crate) fn contextual_post_cas_recovery_enqueue_conflict() -> VfsError {
    VfsError::CorruptStore {
        message: "post-CAS recovery target cannot accept contextual repair".to_string(),
    }
}

fn next_claim_attempt(attempts: u32) -> Result<u32, VfsError> {
    attempts
        .checked_add(1)
        .ok_or_else(|| VfsError::CorruptStore {
            message: "post-CAS recovery claim attempts overflow".to_string(),
        })
}

fn checked_duration_deadline(
    now_millis: u64,
    duration: Duration,
    overflow_message: &str,
) -> Result<u64, VfsError> {
    let duration_millis =
        u64::try_from(duration.as_millis()).map_err(|_| VfsError::InvalidArgs {
            message: overflow_message.to_string(),
        })?;
    now_millis
        .checked_add(duration_millis)
        .ok_or_else(|| VfsError::InvalidArgs {
            message: overflow_message.to_string(),
        })
}

fn current_unix_timestamp_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

const POST_CAS_REPAIR_WORKER_DEFAULT_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableCorePostCasRepairWorkerSummary {
    limit: usize,
    scanned: usize,
    attempted: usize,
    completed: usize,
    backing_off: usize,
    poisoned: usize,
    skipped: usize,
}

impl DurableCorePostCasRepairWorkerSummary {
    pub(crate) const fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) const fn scanned(&self) -> usize {
        self.scanned
    }

    pub(crate) const fn attempted(&self) -> usize {
        self.attempted
    }

    pub(crate) const fn completed(&self) -> usize {
        self.completed
    }

    pub(crate) const fn backing_off(&self) -> usize {
        self.backing_off
    }

    pub(crate) const fn poisoned(&self) -> usize {
        self.poisoned
    }

    pub(crate) const fn skipped(&self) -> usize {
        self.skipped
    }
}

impl fmt::Debug for DurableCorePostCasRepairWorkerSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRepairWorkerSummary")
            .field("limit", &self.limit)
            .field("scanned", &self.scanned)
            .field("attempted", &self.attempted)
            .field("completed", &self.completed)
            .field("backing_off", &self.backing_off)
            .field("poisoned", &self.poisoned)
            .field("skipped", &self.skipped)
            .field("diagnostics", &"<redacted>")
            .finish()
    }
}

pub(crate) struct DurableCorePostCasRepairWorkerStores<'a> {
    recovery: &'a dyn DurableCorePostCasRecoveryClaimStore,
    commits: &'a dyn CommitStore,
    workspaces: &'a dyn WorkspaceMetadataStore,
    audit: &'a dyn AuditStore,
    idempotency: &'a dyn IdempotencyStore,
}

impl<'a> DurableCorePostCasRepairWorkerStores<'a> {
    pub(crate) const fn new(
        recovery: &'a dyn DurableCorePostCasRecoveryClaimStore,
        commits: &'a dyn CommitStore,
        workspaces: &'a dyn WorkspaceMetadataStore,
        audit: &'a dyn AuditStore,
        idempotency: &'a dyn IdempotencyStore,
    ) -> Self {
        Self {
            recovery,
            commits,
            workspaces,
            audit,
            idempotency,
        }
    }
}

pub(crate) struct DurableCorePostCasRepairWorker<'a> {
    stores: DurableCorePostCasRepairWorkerStores<'a>,
    lease_owner: &'a str,
    lease_duration: Duration,
    limit: usize,
    failure_backoff: Duration,
}

impl<'a> DurableCorePostCasRepairWorker<'a> {
    pub(crate) fn new(
        stores: DurableCorePostCasRepairWorkerStores<'a>,
        lease_owner: &'a str,
        lease_duration: Duration,
        limit: usize,
    ) -> Self {
        Self {
            stores,
            lease_owner,
            lease_duration,
            limit,
            failure_backoff: POST_CAS_REPAIR_WORKER_DEFAULT_BACKOFF,
        }
    }

    pub(crate) async fn run(&self) -> Result<DurableCorePostCasRepairWorkerSummary, VfsError> {
        let mut summary = DurableCorePostCasRepairWorkerSummary {
            limit: self.limit,
            scanned: 0,
            attempted: 0,
            completed: 0,
            backing_off: 0,
            poisoned: 0,
            skipped: 0,
        };
        if self.limit == 0 {
            return Ok(summary);
        }

        let statuses = self
            .stores
            .recovery
            .list_repair_candidates(current_unix_timestamp_millis(), self.limit)
            .await?;
        summary.scanned = statuses.len();

        for status in statuses {
            if summary.attempted >= self.limit {
                break;
            }
            let request = DurableCorePostCasRecoveryClaimRequest::new(
                status.target().clone(),
                self.lease_owner,
                self.lease_duration,
                current_unix_timestamp_millis(),
            )?;
            let Some(claim) = self.stores.recovery.claim(request).await? else {
                summary.skipped += 1;
                continue;
            };
            summary.attempted += 1;
            self.process_claim(&claim, &mut summary).await?;
        }

        Ok(summary)
    }

    async fn process_claim(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        summary: &mut DurableCorePostCasRepairWorkerSummary,
    ) -> Result<(), VfsError> {
        let Some(context) = claim.context() else {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        };

        match claim.target().step() {
            DurableCorePostCasStep::WorkspaceHeadUpdate => {
                self.repair_workspace_head(claim, context, summary).await
            }
            DurableCorePostCasStep::AuditAppend => {
                self.repair_audit_append(claim, context, summary).await
            }
            DurableCorePostCasStep::IdempotencyCompletion => {
                self.repair_idempotency_completion(claim, context, summary)
                    .await
            }
        }
    }

    async fn repair_workspace_head(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        context: &DurableCorePostCasRecoveryContext,
        summary: &mut DurableCorePostCasRepairWorkerSummary,
    ) -> Result<(), VfsError> {
        let Some(workspace_id) = context.workspace_id() else {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        };
        if !context.audit_event().is_some_and(|event| {
            audit_event_matches_visible_commit(event, claim.target().commit_id())
        }) {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        }

        let desired_head = claim.target().commit_id().to_hex();
        let repaired = match self
            .stores
            .workspaces
            .update_head_commit_if_current(
                workspace_id,
                context.expected_workspace_head(),
                Some(desired_head.clone()),
            )
            .await
        {
            Ok(Some(workspace)) => workspace.head_commit.as_deref() == Some(desired_head.as_str()),
            Ok(None) => match self.stores.workspaces.get_workspace(workspace_id).await {
                Ok(Some(workspace)) => {
                    workspace.head_commit.as_deref() == Some(desired_head.as_str())
                        || workspace.head_commit.as_deref() != context.expected_workspace_head()
                }
                Ok(None) | Err(_) => false,
            },
            Err(_) => false,
        };

        if !repaired {
            self.stores
                .recovery
                .record_failure(
                    claim,
                    "post-CAS workspace repair failed",
                    self.failure_backoff,
                    current_unix_timestamp_millis(),
                )
                .await?;
            summary.backing_off += 1;
            return Ok(());
        }

        let audit_target = DurableCorePostCasRecoveryTarget::new(
            claim.target().repo_id().clone(),
            claim.target().ref_name(),
            claim.target().commit_id(),
            DurableCorePostCasStep::AuditAppend,
        )?;
        if self
            .stores
            .recovery
            .enqueue_with_context(
                audit_target,
                context.clone(),
                current_unix_timestamp_millis(),
            )
            .await
            .is_err()
        {
            self.stores
                .recovery
                .record_failure(
                    claim,
                    "post-CAS audit follow-up enqueue failed",
                    self.failure_backoff,
                    current_unix_timestamp_millis(),
                )
                .await?;
            summary.backing_off += 1;
            return Ok(());
        }

        self.stores
            .recovery
            .complete(claim, current_unix_timestamp_millis())
            .await?;
        summary.completed += 1;
        Ok(())
    }

    async fn repair_audit_append(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        context: &DurableCorePostCasRecoveryContext,
        summary: &mut DurableCorePostCasRepairWorkerSummary,
    ) -> Result<(), VfsError> {
        let Some(audit_event) = context.audit_event() else {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        };
        if !audit_event_matches_visible_commit(audit_event, claim.target().commit_id()) {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        }

        match self
            .stores
            .audit
            .contains_vcs_commit_event(&claim.target().commit_id().to_hex())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                if self.stores.audit.append(audit_event.clone()).await.is_err() {
                    self.record_claim_failure(claim, "post-CAS audit repair failed", summary)
                        .await?;
                    return Ok(());
                }
            }
            Err(_) => {
                self.record_claim_failure(claim, "post-CAS audit repair failed", summary)
                    .await?;
                return Ok(());
            }
        }

        if context.idempotency().is_some() {
            let idempotency_target = DurableCorePostCasRecoveryTarget::new(
                claim.target().repo_id().clone(),
                claim.target().ref_name(),
                claim.target().commit_id(),
                DurableCorePostCasStep::IdempotencyCompletion,
            )?;
            if self
                .stores
                .recovery
                .enqueue_with_context(
                    idempotency_target,
                    context.clone(),
                    current_unix_timestamp_millis(),
                )
                .await
                .is_err()
            {
                self.record_claim_failure(
                    claim,
                    "post-CAS idempotency follow-up enqueue failed",
                    summary,
                )
                .await?;
                return Ok(());
            }
        }

        self.stores
            .recovery
            .complete(claim, current_unix_timestamp_millis())
            .await?;
        summary.completed += 1;
        Ok(())
    }

    async fn repair_idempotency_completion(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        context: &DurableCorePostCasRecoveryContext,
        summary: &mut DurableCorePostCasRepairWorkerSummary,
    ) -> Result<(), VfsError> {
        let Some(idempotency_context) = context.idempotency() else {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        };
        let reservation = match IdempotencyReservation::for_store_parts(
            idempotency_context.scope(),
            idempotency_context.key_hash(),
            idempotency_context.request_fingerprint(),
            idempotency_context.reservation_token(),
        ) {
            Ok(reservation) => reservation,
            Err(_) => {
                self.poison_claim(claim).await?;
                summary.poisoned += 1;
                return Ok(());
            }
        };
        let Some(audit_event) = context.audit_event() else {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        };
        if !audit_event_matches_visible_commit(audit_event, claim.target().commit_id()) {
            self.poison_claim(claim).await?;
            summary.poisoned += 1;
            return Ok(());
        }
        match self
            .stores
            .audit
            .contains_vcs_commit_event(&claim.target().commit_id().to_hex())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.record_claim_failure(
                    claim,
                    "post-CAS audit prerequisite is not complete",
                    summary,
                )
                .await?;
                return Ok(());
            }
            Err(_) => {
                self.record_claim_failure(
                    claim,
                    "post-CAS audit prerequisite check failed",
                    summary,
                )
                .await?;
                return Ok(());
            }
        }
        let response = match self
            .committed_response_for_repair(claim.target(), idempotency_context.response_kind())
            .await
        {
            Ok(response) => response,
            Err(_) => {
                self.record_claim_failure(
                    claim,
                    "post-CAS idempotency response reconstruction failed",
                    summary,
                )
                .await?;
                return Ok(());
            }
        };

        if self
            .stores
            .idempotency
            .complete_or_match(
                &reservation,
                response.status_code(),
                response.response_body().clone(),
            )
            .await
            .is_err()
        {
            self.record_claim_failure(claim, "post-CAS idempotency repair failed", summary)
                .await?;
            return Ok(());
        }

        self.stores
            .recovery
            .complete(claim, current_unix_timestamp_millis())
            .await?;
        summary.completed += 1;
        Ok(())
    }

    async fn committed_response_for_repair(
        &self,
        target: &DurableCorePostCasRecoveryTarget,
        response_kind: DurableCorePostCasIdempotencyResponseKind,
    ) -> Result<DurableCoreCommittedResponse, VfsError> {
        match response_kind {
            DurableCorePostCasIdempotencyResponseKind::Partial => {
                Ok(DurableCoreCommittedResponse::partial())
            }
            DurableCorePostCasIdempotencyResponseKind::FullCommit => {
                let commit = self
                    .stores
                    .commits
                    .get(target.repo_id(), target.commit_id())
                    .await?
                    .ok_or_else(|| VfsError::CorruptStore {
                        message: "post-CAS idempotency commit metadata missing".to_string(),
                    })?;
                if commit.repo_id != *target.repo_id() || commit.id != target.commit_id() {
                    return Err(VfsError::CorruptStore {
                        message: "post-CAS idempotency commit metadata mismatch".to_string(),
                    });
                }
                DurableCoreCommittedResponse::vcs_commit_success(
                    commit.id,
                    &commit.message,
                    &commit.author,
                )
            }
        }
    }

    async fn record_claim_failure(
        &self,
        claim: &DurableCorePostCasRecoveryClaim,
        diagnosis: &str,
        summary: &mut DurableCorePostCasRepairWorkerSummary,
    ) -> Result<(), VfsError> {
        self.stores
            .recovery
            .record_failure(
                claim,
                diagnosis,
                self.failure_backoff,
                current_unix_timestamp_millis(),
            )
            .await?;
        summary.backing_off += 1;
        Ok(())
    }

    async fn poison_claim(&self, claim: &DurableCorePostCasRecoveryClaim) -> Result<(), VfsError> {
        self.stores
            .recovery
            .poison(
                claim,
                "post-CAS recovery context is unsupported",
                current_unix_timestamp_millis(),
            )
            .await
    }
}

/// Redacted response wrapper for idempotency replay after the commit is visible.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreCommittedResponse {
    status_code: u16,
    response_body: Value,
}

impl DurableCoreCommittedResponse {
    const PARTIAL_STATUS_CODE: u16 = 202;
    const VCS_COMMIT_SUCCESS_STATUS_CODE: u16 = 200;

    pub(crate) fn new(status_code: u16, response_body: Value) -> Result<Self, VfsError> {
        if !(100..=599).contains(&status_code) {
            return Err(VfsError::InvalidArgs {
                message: "committed response status code must be an HTTP status".to_string(),
            });
        }

        Ok(Self {
            status_code,
            response_body,
        })
    }

    pub(crate) const fn status_code(&self) -> u16 {
        self.status_code
    }

    pub(crate) fn response_body(&self) -> &Value {
        &self.response_body
    }

    pub(crate) fn vcs_commit_success(
        commit_id: CommitId,
        message: &str,
        author: &str,
    ) -> Result<Self, VfsError> {
        Self::new(
            Self::VCS_COMMIT_SUCCESS_STATUS_CODE,
            serde_json::json!({
                "hash": commit_id.to_hex(),
                "message": message,
                "author": author,
            }),
        )
    }

    pub(crate) fn partial_body() -> Value {
        serde_json::json!({
            "committed": true,
            "post_cas_completion": "partial",
            "message": "redacted post-CAS completion is partial"
        })
    }

    fn partial() -> Self {
        Self {
            status_code: Self::PARTIAL_STATUS_CODE,
            response_body: Self::partial_body(),
        }
    }
}

impl fmt::Debug for DurableCoreCommittedResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommittedResponse")
            .field("status_code", &self.status_code)
            .field("response_body", &"<redacted>")
            .finish()
    }
}

/// Inputs that drive post-CAS completion side effects.
pub(crate) struct DurableCoreCommitPostCasInput {
    workspace_id: Option<Uuid>,
    audit_event: NewAuditEvent,
    idempotency_reservation: Option<IdempotencyReservation>,
    committed_response: DurableCoreCommittedResponse,
}

impl DurableCoreCommitPostCasInput {
    pub(crate) fn new(
        audit_event: NewAuditEvent,
        committed_response: DurableCoreCommittedResponse,
    ) -> Self {
        Self {
            workspace_id: None,
            audit_event,
            idempotency_reservation: None,
            committed_response,
        }
    }

    pub(crate) const fn with_workspace_id(mut self, workspace_id: Uuid) -> Self {
        self.workspace_id = Some(workspace_id);
        self
    }

    pub(crate) fn with_idempotency_reservation(
        mut self,
        reservation: IdempotencyReservation,
    ) -> Self {
        self.idempotency_reservation = Some(reservation);
        self
    }
}

impl fmt::Debug for DurableCoreCommitPostCasInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommitPostCasInput")
            .field("workspace_id", &self.workspace_id)
            .field("audit_event", &"<redacted>")
            .field(
                "has_idempotency_reservation",
                &self.idempotency_reservation.is_some(),
            )
            .field("committed_response", &self.committed_response)
            .finish()
    }
}

/// Bound post-CAS completion envelope for a visible durable commit.
pub(crate) struct DurableCoreCommitPostCasEnvelope {
    repo_id: RepoId,
    ref_name: &'static str,
    commit_id: CommitId,
    version: RefVersion,
    workspace_id: Option<Uuid>,
    expected_workspace_head: Option<String>,
    audit_event: NewAuditEvent,
    idempotency_reservation: Option<IdempotencyReservation>,
    committed_response: DurableCoreCommittedResponse,
}

impl DurableCoreCommitPostCasEnvelope {
    pub(crate) async fn complete(
        &self,
        workspaces: &dyn WorkspaceMetadataStore,
        audit: &dyn AuditStore,
        idempotency: &dyn IdempotencyStore,
    ) -> DurableCorePostCasOutcome {
        self.complete_from(
            DurableCoreCommitPostCasCompletion::default(),
            workspaces,
            audit,
            idempotency,
        )
        .await
    }

    pub(crate) async fn complete_from(
        &self,
        mut completion: DurableCoreCommitPostCasCompletion,
        workspaces: &dyn WorkspaceMetadataStore,
        audit: &dyn AuditStore,
        idempotency: &dyn IdempotencyStore,
    ) -> DurableCorePostCasOutcome {
        if !completion.workspace_head_updated
            && self
                .complete_workspace_head_update(workspaces)
                .await
                .is_err()
        {
            return Self::partial_after_failure(
                DurableCorePostCasStep::WorkspaceHeadUpdate,
                completion,
            );
        }
        completion.workspace_head_updated = true;

        if !completion.audit_appended && audit.append(self.audit_event.clone()).await.is_err() {
            return Self::partial_after_failure(DurableCorePostCasStep::AuditAppend, completion);
        }
        completion.audit_appended = true;

        if !completion.idempotency_completed
            && self
                .complete_idempotency_with_response(
                    idempotency,
                    self.committed_response.status_code(),
                    self.committed_response.response_body().clone(),
                )
                .await
                .is_err()
        {
            return DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
                failed_step: DurableCorePostCasStep::IdempotencyCompletion,
                completion,
                idempotency_completion_attempted: self.idempotency_reservation.is_some(),
                idempotency_completed: false,
            });
        }
        completion.idempotency_completed = true;

        DurableCorePostCasOutcome::Complete { completion }
    }

    pub(crate) fn recovery_target(
        &self,
        step: DurableCorePostCasStep,
    ) -> Result<DurableCorePostCasRecoveryTarget, VfsError> {
        DurableCorePostCasRecoveryTarget::new(
            self.repo_id.clone(),
            self.ref_name,
            self.commit_id,
            step,
        )
    }

    pub(crate) fn recovery_context(
        &self,
        idempotency_response_kind: Option<DurableCorePostCasIdempotencyResponseKind>,
    ) -> DurableCorePostCasRecoveryContext {
        let idempotency = idempotency_response_kind.and_then(|response_kind| {
            self.idempotency_reservation.as_ref().map(|reservation| {
                DurableCorePostCasIdempotencyRecoveryContext::from_reservation(
                    reservation,
                    response_kind,
                )
            })
        });
        DurableCorePostCasRecoveryContext::new(
            self.workspace_id,
            self.expected_workspace_head.clone(),
            Some(self.audit_event.clone()),
            idempotency,
        )
    }

    pub(crate) async fn complete_recovery_step(
        &self,
        step: DurableCorePostCasStep,
        workspaces: &dyn WorkspaceMetadataStore,
        audit: &dyn AuditStore,
        idempotency: &dyn IdempotencyStore,
    ) -> DurableCorePostCasOutcome {
        let mut completion = DurableCoreCommitPostCasCompletion::default();
        match step {
            DurableCorePostCasStep::WorkspaceHeadUpdate => {
                if self
                    .complete_workspace_head_update(workspaces)
                    .await
                    .is_err()
                {
                    return Self::partial_after_failure(
                        DurableCorePostCasStep::WorkspaceHeadUpdate,
                        completion,
                    );
                }
                completion.workspace_head_updated = true;
            }
            DurableCorePostCasStep::AuditAppend => {
                if audit.append(self.audit_event.clone()).await.is_err() {
                    return Self::partial_after_failure(
                        DurableCorePostCasStep::AuditAppend,
                        completion,
                    );
                }
                completion.audit_appended = true;
            }
            DurableCorePostCasStep::IdempotencyCompletion => {
                if self
                    .complete_idempotency_with_response(
                        idempotency,
                        self.committed_response.status_code(),
                        self.committed_response.response_body().clone(),
                    )
                    .await
                    .is_err()
                {
                    return DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
                        failed_step: DurableCorePostCasStep::IdempotencyCompletion,
                        completion,
                        idempotency_completion_attempted: self.idempotency_reservation.is_some(),
                        idempotency_completed: false,
                    });
                }
                completion.idempotency_completed = true;
            }
        }

        DurableCorePostCasOutcome::Complete { completion }
    }

    async fn complete_workspace_head_update(
        &self,
        workspaces: &dyn WorkspaceMetadataStore,
    ) -> Result<(), VfsError> {
        let Some(workspace_id) = self.workspace_id else {
            return Ok(());
        };
        let desired_head = self.commit_id.to_hex();
        match workspaces
            .update_head_commit_if_current(
                workspace_id,
                self.expected_workspace_head.as_deref(),
                Some(desired_head.clone()),
            )
            .await
        {
            Ok(Some(workspace))
                if workspace.head_commit.as_deref() == Some(desired_head.as_str()) =>
            {
                Ok(())
            }
            Ok(Some(_)) => Err(redacted_post_cas_completion_error()),
            Ok(None) => match workspaces.get_workspace(workspace_id).await {
                Ok(Some(workspace))
                    if workspace.head_commit.as_deref() == Some(desired_head.as_str())
                        || workspace.head_commit.as_deref()
                            != self.expected_workspace_head.as_deref() =>
                {
                    Ok(())
                }
                Ok(Some(_)) | Ok(None) | Err(_) => Err(redacted_post_cas_completion_error()),
            },
            Err(_) => Err(redacted_post_cas_completion_error()),
        }
    }

    pub(crate) async fn complete_partial_idempotency_replay(
        &self,
        idempotency: &dyn IdempotencyStore,
    ) -> Result<(), VfsError> {
        let partial_response = DurableCoreCommittedResponse::partial();
        self.complete_idempotency_with_response(
            idempotency,
            partial_response.status_code(),
            partial_response.response_body().clone(),
        )
        .await
    }

    async fn complete_idempotency_with_response(
        &self,
        idempotency: &dyn IdempotencyStore,
        status_code: u16,
        response_body: Value,
    ) -> Result<(), VfsError> {
        if let Some(reservation) = &self.idempotency_reservation {
            idempotency
                .complete(reservation, status_code, response_body)
                .await
                .map_err(|_| redacted_post_cas_completion_error())
        } else {
            Ok(())
        }
    }

    fn partial_after_failure(
        failed_step: DurableCorePostCasStep,
        completion: DurableCoreCommitPostCasCompletion,
    ) -> DurableCorePostCasOutcome {
        DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
            failed_step,
            completion,
            idempotency_completion_attempted: false,
            idempotency_completed: completion.idempotency_completed,
        })
    }
}

fn redacted_post_cas_completion_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable commit post-CAS completion failed".to_string(),
    }
}

fn audit_event_matches_visible_commit(event: &NewAuditEvent, commit_id: CommitId) -> bool {
    let commit_hex = commit_id.to_hex();
    event.action == AuditAction::VcsCommit
        && event.resource.kind == AuditResourceKind::Commit
        && event.resource.id.as_deref() == Some(commit_hex.as_str())
        && event.resource.path.is_none()
}

impl fmt::Debug for DurableCoreCommitPostCasEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommitPostCasEnvelope")
            .field("repo_id", &self.repo_id)
            .field("ref_name", &self.ref_name)
            .field("commit_id", &self.commit_id)
            .field("version", &self.version)
            .field("workspace_id", &self.workspace_id)
            .field("audit_event", &"<redacted>")
            .field(
                "has_idempotency_reservation",
                &self.idempotency_reservation.is_some(),
            )
            .field("committed_response", &self.committed_response)
            .field(
                "steps",
                &[
                    DurableCorePostCasStep::WorkspaceHeadUpdate,
                    DurableCorePostCasStep::AuditAppend,
                    DurableCorePostCasStep::IdempotencyCompletion,
                ],
            )
            .finish()
    }
}

/// Post-CAS side effects known to have completed in the current attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitPostCasCompletion {
    workspace_head_updated: bool,
    audit_appended: bool,
    idempotency_completed: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableCorePostCasPartial {
    failed_step: DurableCorePostCasStep,
    completion: DurableCoreCommitPostCasCompletion,
    idempotency_completion_attempted: bool,
    idempotency_completed: bool,
}

impl fmt::Debug for DurableCorePostCasPartial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasPartial")
            .field("failed_step", &self.failed_step)
            .field("completion", &self.completion)
            .field(
                "idempotency_completion_attempted",
                &self.idempotency_completion_attempted,
            )
            .field("idempotency_completed", &self.idempotency_completed)
            .field("error", &"<redacted>")
            .finish()
    }
}

impl DurableCorePostCasPartial {
    pub(crate) const fn failed_step(&self) -> DurableCorePostCasStep {
        self.failed_step
    }

    pub(crate) const fn completion(&self) -> DurableCoreCommitPostCasCompletion {
        self.completion
    }

    pub(crate) const fn idempotency_completion_attempted(&self) -> bool {
        self.idempotency_completion_attempted
    }

    pub(crate) const fn idempotency_completed(&self) -> bool {
        self.idempotency_completed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableCorePostCasOutcome {
    Complete {
        completion: DurableCoreCommitPostCasCompletion,
    },
    Partial(DurableCorePostCasPartial),
}

/// Read-only object/tree write plan for a future durable commit transaction.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitObjectTreeWritePlan {
    source: DurableCoreCommitSourceSnapshot,
    root_tree_id: ObjectId,
    planned_objects: Vec<DurableCorePlannedObject>,
    current_path_records: Vec<PathRecord>,
    changed_paths: Vec<ChangedPath>,
    skeleton: DurableCoreCommitExecutorSkeleton,
}

impl DurableCoreCommitObjectTreeWritePlan {
    pub(crate) fn build(
        source: DurableCoreCommitSourceSnapshot,
        fs: &VirtualFs,
    ) -> Result<Self, VfsError> {
        if matches!(source.parent_state(), DurableCoreCommitParentState::Unborn)
            && !source.base_path_records().is_empty()
        {
            return Err(VfsError::InvalidArgs {
                message: "unborn source snapshot cannot include base path records".to_string(),
            });
        }

        let base = path_map_from_records(source.base_path_records())?;
        let current = worktree_path_records(fs)?;
        let changed_paths = diff_path_maps(&base, &current);
        let current_path_records = current.values().cloned().collect();

        let mut planner = DurableCoreObjectTreePlanner::default();
        let root_tree_id = planner.plan_dir(fs, fs.root_id())?;

        Ok(Self {
            source,
            root_tree_id,
            planned_objects: planner.into_planned_objects(),
            current_path_records,
            changed_paths,
            skeleton: DurableCoreCommitExecutorSkeleton::new(),
        })
    }

    pub(crate) fn source(&self) -> &DurableCoreCommitSourceSnapshot {
        &self.source
    }

    pub(crate) const fn root_tree_id(&self) -> ObjectId {
        self.root_tree_id
    }

    pub(crate) fn planned_objects(&self) -> &[DurableCorePlannedObject] {
        &self.planned_objects
    }

    pub(crate) fn current_path_records(&self) -> &[PathRecord] {
        &self.current_path_records
    }

    pub(crate) fn changed_paths(&self) -> &[ChangedPath] {
        &self.changed_paths
    }

    pub(crate) fn ordered_write_path(&self) -> &'static [DurableCoreTransactionStep] {
        self.skeleton.ordered_write_path()
    }

    pub(crate) const fn live_execution_enabled(&self) -> bool {
        self.skeleton.live_execution_enabled()
    }

    pub(crate) fn object_writes_for_repo(&self, repo_id: &RepoId) -> Vec<ObjectWrite> {
        self.planned_objects
            .iter()
            .map(|object| object.object_write_for_repo(repo_id))
            .collect()
    }

    pub(crate) fn post_cas_envelope(
        &self,
        metadata: &DurableCoreCommitMetadataInsert,
        visibility: &DurableCoreCommitRefCasVisibility,
        input: DurableCoreCommitPostCasInput,
    ) -> Result<DurableCoreCommitPostCasEnvelope, VfsError> {
        if !self.metadata_insert_is_bound(metadata)
            || visibility.repo_id() != metadata.repo_id()
            || visibility.ref_name() != MAIN_REF
            || visibility.commit_id() != metadata.commit_id()
            || visibility.version() != self.expected_post_cas_ref_version()?
            || !audit_event_matches_visible_commit(&input.audit_event, metadata.commit_id())
            || !(100..=599).contains(&input.committed_response.status_code())
        {
            return Err(VfsError::CorruptStore {
                message: "durable commit post-CAS envelope input does not match visible commit"
                    .to_string(),
            });
        }

        Ok(DurableCoreCommitPostCasEnvelope {
            repo_id: metadata.repo_id().clone(),
            ref_name: MAIN_REF,
            commit_id: metadata.commit_id(),
            version: visibility.version(),
            workspace_id: input.workspace_id,
            expected_workspace_head: self.expected_workspace_head(),
            audit_event: input.audit_event,
            idempotency_reservation: input.idempotency_reservation,
            committed_response: input.committed_response,
        })
    }

    fn metadata_insert_is_bound(&self, metadata: &DurableCoreCommitMetadataInsert) -> bool {
        let parents_match = match self.source().parent_state() {
            DurableCoreCommitParentState::Unborn => metadata.parents().is_empty(),
            DurableCoreCommitParentState::Existing { target, .. } => metadata.parents() == [target],
        };

        metadata.root_tree_id() == self.root_tree_id()
            && metadata.changed_path_count() == self.changed_paths().len()
            && metadata.plan_fingerprint == durable_commit_plan_fingerprint(self)
            && parents_match
    }

    fn expected_post_cas_ref_version(&self) -> Result<RefVersion, VfsError> {
        match self.source().parent_state() {
            DurableCoreCommitParentState::Unborn => {
                RefVersion::new(1).map_err(|_| VfsError::CorruptStore {
                    message: "durable commit post-CAS envelope input does not match visible commit"
                        .to_string(),
                })
            }
            DurableCoreCommitParentState::Existing { version, .. } => {
                let next_value =
                    version
                        .value()
                        .checked_add(1)
                        .ok_or_else(|| {
                            VfsError::CorruptStore {
                        message:
                            "durable commit post-CAS envelope input does not match visible commit"
                                .to_string(),
                    }
                        })?;
                RefVersion::new(next_value).map_err(|_| VfsError::CorruptStore {
                    message: "durable commit post-CAS envelope input does not match visible commit"
                        .to_string(),
                })
            }
        }
    }

    fn expected_workspace_head(&self) -> Option<String> {
        match self.source().parent_state() {
            DurableCoreCommitParentState::Unborn => None,
            DurableCoreCommitParentState::Existing { target, .. } => Some(target.to_hex()),
        }
    }

    pub(crate) async fn converge_objects(
        &self,
        repo_id: &RepoId,
        object_store: &dyn ObjectStore,
    ) -> Result<DurableCoreObjectConvergence, VfsError> {
        let mut objects = Vec::with_capacity(self.planned_objects.len());

        for planned in &self.planned_objects {
            let stored = object_store
                .put(planned.object_write_for_repo(repo_id))
                .await
                .map_err(|_| VfsError::CorruptStore {
                    message: "object convergence failed to persist planned object".to_string(),
                })?;
            if stored.repo_id != *repo_id
                || stored.id != planned.id
                || stored.kind != planned.kind
                || stored.bytes.as_slice() != planned.bytes()
            {
                return Err(VfsError::CorruptStore {
                    message: "object convergence returned mismatched object".to_string(),
                });
            }

            objects.push(DurableCoreConvergedObject {
                kind: planned.kind,
                id: planned.id,
                byte_len: planned.bytes.len(),
            });
        }

        if !object_store
            .contains(repo_id, self.root_tree_id, ObjectKind::Tree)
            .await
            .map_err(|_| VfsError::CorruptStore {
                message: "object convergence failed to verify root tree".to_string(),
            })?
        {
            return Err(VfsError::CorruptStore {
                message: "object convergence did not persist root tree".to_string(),
            });
        }

        Ok(DurableCoreObjectConvergence {
            repo_id: repo_id.clone(),
            root_tree_id: self.root_tree_id,
            objects,
        })
    }

    pub(crate) async fn insert_commit_metadata(
        &self,
        convergence: &DurableCoreObjectConvergence,
        commit_store: &dyn CommitStore,
        timestamp: u64,
        author: &str,
        message: &str,
    ) -> Result<DurableCoreCommitMetadataInsert, VfsError> {
        if convergence.root_tree_id() != self.root_tree_id()
            || convergence.object_count() != self.planned_objects().len()
            || !convergence
                .objects()
                .iter()
                .zip(self.planned_objects())
                .all(|(converged, planned)| {
                    converged.kind() == planned.kind()
                        && converged.id() == planned.id()
                        && converged.byte_len() == planned.bytes().len()
                })
        {
            return Err(VfsError::CorruptStore {
                message: "durable commit object convergence does not match write plan".to_string(),
            });
        }

        let record = durable_commit_record_for_metadata_insert(
            convergence.repo_id().clone(),
            self,
            timestamp,
            author,
            message,
        );

        for parent in &record.parents {
            let exists = commit_store
                .contains(convergence.repo_id(), *parent)
                .await
                .map_err(|_| VfsError::CorruptStore {
                    message: "durable commit parent metadata check failed".to_string(),
                })?;
            if !exists {
                return Err(VfsError::CorruptStore {
                    message: "durable commit parent metadata is missing".to_string(),
                });
            }
        }

        let expected_commit_id = record.id;
        let expected_parent_state = self.source().parent_state();
        let inserted = commit_store
            .insert(record)
            .await
            .map_err(|_| VfsError::CorruptStore {
                message: "durable commit metadata insert failed".to_string(),
            })?;
        let parents_match = match expected_parent_state {
            DurableCoreCommitParentState::Unborn => inserted.parents.is_empty(),
            DurableCoreCommitParentState::Existing { target, .. } => {
                inserted.parents.as_slice() == [target]
            }
        };
        if inserted.repo_id.as_str() != convergence.repo_id().as_str()
            || inserted.id != expected_commit_id
            || inserted.root_tree != convergence.root_tree_id()
            || !parents_match
            || inserted.timestamp != timestamp
            || inserted.author != author
            || inserted.message != message
            || inserted.changed_paths.as_slice() != self.changed_paths()
        {
            return Err(VfsError::CorruptStore {
                message: "durable commit metadata insert returned mismatched record".to_string(),
            });
        }

        Ok(DurableCoreCommitMetadataInsert {
            repo_id: inserted.repo_id,
            commit_id: inserted.id,
            root_tree_id: inserted.root_tree,
            parents: inserted.parents,
            changed_path_count: inserted.changed_paths.len(),
            timestamp: inserted.timestamp,
            plan_fingerprint: durable_commit_plan_fingerprint(self),
        })
    }

    pub(crate) async fn recover_commit_metadata_insert(
        &self,
        convergence: &DurableCoreObjectConvergence,
        commit_store: &dyn CommitStore,
        timestamp: u64,
        author: &str,
        message: &str,
    ) -> Result<Option<DurableCoreCommitMetadataInsert>, VfsError> {
        let expected = durable_commit_record_for_metadata_insert(
            convergence.repo_id().clone(),
            self,
            timestamp,
            author,
            message,
        );
        let Some(stored) = commit_store
            .get(convergence.repo_id(), expected.id)
            .await
            .map_err(|_| VfsError::CorruptStore {
                message: "durable commit metadata insert recovery failed".to_string(),
            })?
        else {
            return Ok(None);
        };
        if stored != expected {
            return Err(VfsError::CorruptStore {
                message: "durable commit metadata insert recovery failed".to_string(),
            });
        }
        Ok(Some(DurableCoreCommitMetadataInsert {
            repo_id: stored.repo_id,
            commit_id: stored.id,
            root_tree_id: stored.root_tree,
            parents: stored.parents,
            changed_path_count: stored.changed_paths.len(),
            timestamp: stored.timestamp,
            plan_fingerprint: durable_commit_plan_fingerprint(self),
        }))
    }

    pub(crate) async fn apply_ref_cas_visibility(
        &self,
        metadata: &DurableCoreCommitMetadataInsert,
        ref_store: &dyn RefStore,
    ) -> Result<DurableCoreCommitRefCasVisibility, VfsError> {
        let parents_match = match self.source().parent_state() {
            DurableCoreCommitParentState::Unborn => metadata.parents().is_empty(),
            DurableCoreCommitParentState::Existing { target, .. } => metadata.parents() == [target],
        };
        if metadata.root_tree_id() != self.root_tree_id()
            || metadata.changed_path_count() != self.changed_paths().len()
            || metadata.plan_fingerprint != durable_commit_plan_fingerprint(self)
            || !parents_match
        {
            return Err(VfsError::CorruptStore {
                message: "durable commit ref visibility input does not match write plan"
                    .to_string(),
            });
        }

        let main = RefName::new(MAIN_REF).map_err(|_| VfsError::CorruptStore {
            message: "durable commit ref visibility update failed".to_string(),
        })?;
        let expectation = match self.source().parent_state() {
            DurableCoreCommitParentState::Unborn => RefExpectation::MustNotExist,
            DurableCoreCommitParentState::Existing { target, version } => {
                RefExpectation::Matches { target, version }
            }
        };

        let updated = ref_store
            .update(RefUpdate {
                repo_id: metadata.repo_id().clone(),
                name: main,
                target: metadata.commit_id(),
                expectation,
            })
            .await
            .map_err(|err| match err {
                VfsError::InvalidArgs { message } if is_ref_cas_mismatch_message(&message) => {
                    VfsError::InvalidArgs {
                        message: "ref compare-and-swap mismatch".to_string(),
                    }
                }
                _ => VfsError::CorruptStore {
                    message: "durable commit ref visibility update failed".to_string(),
                },
            })?;

        let expected_version = match self.source().parent_state() {
            DurableCoreCommitParentState::Unborn => {
                RefVersion::new(1).map_err(|_| VfsError::CorruptStore {
                    message: "durable commit ref visibility returned mismatched record".to_string(),
                })?
            }
            DurableCoreCommitParentState::Existing { version, .. } => {
                let next_value =
                    version
                        .value()
                        .checked_add(1)
                        .ok_or_else(|| VfsError::CorruptStore {
                            message: "durable commit ref visibility returned mismatched record"
                                .to_string(),
                        })?;
                RefVersion::new(next_value).map_err(|_| VfsError::CorruptStore {
                    message: "durable commit ref visibility returned mismatched record".to_string(),
                })?
            }
        };
        if updated.repo_id != *metadata.repo_id()
            || updated.name.as_str() != MAIN_REF
            || updated.target != metadata.commit_id()
            || updated.version != expected_version
        {
            return Err(VfsError::CorruptStore {
                message: "durable commit ref visibility returned mismatched record".to_string(),
            });
        }

        Ok(DurableCoreCommitRefCasVisibility {
            repo_id: updated.repo_id,
            ref_name: MAIN_REF,
            commit_id: updated.target,
            version: updated.version,
        })
    }

    pub(crate) async fn recover_ref_cas_visibility(
        &self,
        metadata: &DurableCoreCommitMetadataInsert,
        ref_store: &dyn RefStore,
    ) -> Result<Option<DurableCoreCommitRefCasVisibility>, VfsError> {
        let main = RefName::new(MAIN_REF).map_err(|_| VfsError::CorruptStore {
            message: "durable commit ref visibility recovery failed".to_string(),
        })?;
        let Some(current) = ref_store
            .get(metadata.repo_id(), &main)
            .await
            .map_err(|_| VfsError::CorruptStore {
                message: "durable commit ref visibility recovery failed".to_string(),
            })?
        else {
            return Ok(None);
        };
        if current.repo_id == *metadata.repo_id()
            && current.name.as_str() == MAIN_REF
            && current.target == metadata.commit_id()
            && current.version == self.expected_post_cas_ref_version()?
        {
            return Ok(Some(DurableCoreCommitRefCasVisibility {
                repo_id: current.repo_id,
                ref_name: MAIN_REF,
                commit_id: current.target,
                version: current.version,
            }));
        }
        Ok(None)
    }
}

impl fmt::Debug for DurableCoreCommitObjectTreeWritePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCoreCommitObjectTreeWritePlan")
            .field("source", &self.source)
            .field("root_tree_id", &self.root_tree_id)
            .field("planned_object_count", &self.planned_objects.len())
            .field(
                "current_path_record_count",
                &self.current_path_records.len(),
            )
            .field("changed_path_count", &self.changed_paths.len())
            .field("live_execution_enabled", &self.live_execution_enabled())
            .finish()
    }
}

#[derive(Default)]
struct DurableCoreObjectTreePlanner {
    planned_objects: Vec<DurableCorePlannedObject>,
    planned_index: BTreeMap<ObjectId, usize>,
}

impl DurableCoreObjectTreePlanner {
    fn plan_dir(&mut self, fs: &VirtualFs, dir_id: InodeId) -> Result<ObjectId, VfsError> {
        let inode = fs.get_inode(dir_id)?;
        let entries = match &inode.kind {
            InodeKind::Directory { entries } => entries,
            _ => {
                return Err(VfsError::NotDirectory {
                    path: format!("<inode {dir_id}>"),
                });
            }
        };

        let mut tree_entries = Vec::with_capacity(entries.len());
        for (name, child_id) in entries {
            let child = fs.get_inode(*child_id)?;
            let (kind, id) = match &child.kind {
                InodeKind::File { content } => {
                    let blob_id = self.plan_object(ObjectKind::Blob, content)?;
                    (TreeEntryKind::Blob, blob_id)
                }
                InodeKind::Directory { .. } => {
                    let tree_id = self.plan_dir(fs, *child_id)?;
                    (TreeEntryKind::Tree, tree_id)
                }
                InodeKind::Symlink { target } => {
                    let blob_id = self.plan_object(ObjectKind::Blob, target.as_bytes())?;
                    (TreeEntryKind::Symlink, blob_id)
                }
            };

            tree_entries.push(TreeEntry {
                name: name.clone(),
                kind,
                id,
                mode: child.mode,
                uid: child.uid,
                gid: child.gid,
                mime_type: child.mime_type.clone(),
                custom_attrs: child.custom_attrs.clone(),
            });
        }

        let tree = TreeObject {
            entries: tree_entries,
        };
        self.plan_object(ObjectKind::Tree, &tree.serialize())
    }

    fn plan_object(&mut self, kind: ObjectKind, bytes: &[u8]) -> Result<ObjectId, VfsError> {
        let id = ObjectId::from_bytes(bytes);
        if let Some(position) = self.planned_index.get(&id).copied() {
            let existing = &self.planned_objects[position];
            if existing.kind != kind || existing.bytes != bytes {
                return Err(VfsError::InvalidArgs {
                    message: "planned object identity collision".to_string(),
                });
            }
            return Ok(id);
        }

        self.planned_index.insert(id, self.planned_objects.len());
        self.planned_objects.push(DurableCorePlannedObject {
            kind,
            id,
            bytes: bytes.to_vec(),
        });
        Ok(id)
    }

    fn into_planned_objects(self) -> Vec<DurableCorePlannedObject> {
        self.planned_objects
    }
}

fn path_map_from_records(records: &[PathRecord]) -> Result<PathMap, VfsError> {
    let mut map = PathMap::new();
    for record in records {
        if map.insert(record.path.clone(), record.clone()).is_some() {
            return Err(VfsError::InvalidArgs {
                message: "duplicate source path record".to_string(),
            });
        }
    }
    Ok(map)
}

async fn durable_parent_path_records(
    repo_id: &RepoId,
    root_tree_id: ObjectId,
    object_store: &dyn ObjectStore,
) -> Result<Vec<PathRecord>, VfsError> {
    let root_tree = load_durable_parent_tree(repo_id, root_tree_id, object_store).await?;
    let mut records = BTreeMap::new();
    let mut pending = vec![("/".to_string(), root_tree)];

    while let Some((dir_path, tree)) = pending.pop() {
        let mut entries = tree.entries;
        entries.sort_by(|left, right| left.name.cmp(&right.name));

        for entry in entries {
            let path = durable_child_path(&dir_path, &entry.name);
            match entry.kind {
                TreeEntryKind::Blob => {
                    let size = durable_parent_blob_len(repo_id, entry.id, object_store).await?;
                    insert_durable_parent_path_record(
                        &mut records,
                        PathRecord {
                            path,
                            kind: PathKind::File,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size,
                            content_id: Some(entry.id),
                            mime_type: entry.mime_type,
                            custom_attrs: entry.custom_attrs,
                        },
                    )?;
                }
                TreeEntryKind::Tree => {
                    let child_tree =
                        load_durable_parent_tree(repo_id, entry.id, object_store).await?;
                    let size = child_tree.entries.len() as u64;
                    insert_durable_parent_path_record(
                        &mut records,
                        PathRecord {
                            path: path.clone(),
                            kind: PathKind::Directory,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size,
                            content_id: None,
                            mime_type: entry.mime_type,
                            custom_attrs: entry.custom_attrs,
                        },
                    )?;
                    pending.push((path, child_tree));
                }
                TreeEntryKind::Symlink => {
                    let size = durable_parent_blob_len(repo_id, entry.id, object_store).await?;
                    insert_durable_parent_path_record(
                        &mut records,
                        PathRecord {
                            path,
                            kind: PathKind::Symlink,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size,
                            content_id: Some(entry.id),
                            mime_type: entry.mime_type,
                            custom_attrs: entry.custom_attrs,
                        },
                    )?;
                }
            }
        }
    }

    Ok(records.into_values().collect())
}

async fn load_durable_parent_tree(
    repo_id: &RepoId,
    tree_id: ObjectId,
    object_store: &dyn ObjectStore,
) -> Result<TreeObject, VfsError> {
    let stored = object_store
        .get(repo_id, tree_id, ObjectKind::Tree)
        .await
        .map_err(|_| redacted_durable_parent_source_snapshot_error())?
        .ok_or_else(redacted_durable_parent_source_snapshot_error)?;
    if stored.repo_id != *repo_id || stored.id != tree_id || stored.kind != ObjectKind::Tree {
        return Err(redacted_durable_parent_source_snapshot_error());
    }
    TreeObject::deserialize(&stored.bytes)
        .map_err(|_| redacted_durable_parent_source_snapshot_error())
}

async fn durable_parent_blob_len(
    repo_id: &RepoId,
    blob_id: ObjectId,
    object_store: &dyn ObjectStore,
) -> Result<u64, VfsError> {
    let stored = object_store
        .object_len(repo_id, blob_id, ObjectKind::Blob)
        .await
        .map_err(|_| redacted_durable_parent_source_snapshot_error())?
        .ok_or_else(redacted_durable_parent_source_snapshot_error)?;
    Ok(stored)
}

fn insert_durable_parent_path_record(
    records: &mut BTreeMap<String, PathRecord>,
    record: PathRecord,
) -> Result<(), VfsError> {
    if records.insert(record.path.clone(), record).is_some() {
        return Err(redacted_durable_parent_source_snapshot_error());
    }
    Ok(())
}

fn durable_child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn redacted_durable_parent_source_snapshot_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable commit parent source snapshot failed".to_string(),
    }
}

fn durable_commit_record_for_metadata_insert(
    repo_id: RepoId,
    plan: &DurableCoreCommitObjectTreeWritePlan,
    timestamp: u64,
    author: &str,
    message: &str,
) -> CommitRecord {
    let parents = match plan.source().parent_state() {
        DurableCoreCommitParentState::Unborn => Vec::new(),
        DurableCoreCommitParentState::Existing { target, .. } => vec![target],
    };
    let parent = parents.first().copied().map(CommitId::object_id);
    let commit = CommitObject {
        id: ObjectId::from_bytes(&[0; 32]),
        tree: plan.root_tree_id(),
        parent,
        timestamp,
        message: message.to_string(),
        author: author.to_string(),
        changed_paths: plan.changed_paths().to_vec(),
    };
    let commit_id = CommitId::from(ObjectId::from_bytes(&commit.serialize()));
    let CommitObject {
        message,
        author,
        changed_paths,
        ..
    } = commit;

    CommitRecord {
        repo_id,
        id: commit_id,
        root_tree: plan.root_tree_id(),
        parents,
        timestamp,
        message,
        author,
        changed_paths,
    }
}

fn durable_commit_plan_fingerprint(plan: &DurableCoreCommitObjectTreeWritePlan) -> ObjectId {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"stratum-durable-commit-plan-v1");
    bytes.extend_from_slice(plan.root_tree_id().as_bytes());
    match plan.source().parent_state() {
        DurableCoreCommitParentState::Unborn => bytes.push(0),
        DurableCoreCommitParentState::Existing { target, version } => {
            bytes.push(1);
            bytes.extend_from_slice(target.object_id().as_bytes());
            bytes.extend_from_slice(&version.value().to_be_bytes());
        }
    }
    let changed_paths = crate::codec::serialize(&plan.changed_paths())
        .expect("changed path serialization should not fail");
    bytes.extend_from_slice(&(changed_paths.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&changed_paths);
    ObjectId::from_bytes(&bytes)
}

fn is_ref_cas_mismatch_message(message: &str) -> bool {
    message == "ref compare-and-swap mismatch"
        || message.strip_prefix("ref compare-and-swap mismatch: ") == Some(MAIN_REF)
}

/// Internal durable commit transaction executor skeleton.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableCoreCommitExecutorSkeleton;

impl DurableCoreCommitExecutorSkeleton {
    const UNRESOLVED_PREREQUISITES: [DurableCoreCommitPrerequisite; 7] = [
        DurableCoreCommitPrerequisite::DurableObjectByteWrites,
        DurableCoreCommitPrerequisite::LiveTreeConstruction,
        DurableCoreCommitPrerequisite::SourceFilesystemSnapshot,
        DurableCoreCommitPrerequisite::WorkspaceHeadCoupling,
        DurableCoreCommitPrerequisite::AuditAndIdempotencyCompletion,
        DurableCoreCommitPrerequisite::CommitLockingAndFencing,
        DurableCoreCommitPrerequisite::RepairWorker,
    ];

    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn ordered_write_path(&self) -> &'static [DurableCoreTransactionStep] {
        DurableCoreStepSemantics::ordered_write_path()
    }

    pub(crate) const fn live_execution(&self) -> DurableCoreCommitLiveExecution {
        DurableCoreCommitLiveExecution::Disabled
    }

    pub(crate) const fn live_execution_enabled(&self) -> bool {
        false
    }

    pub(crate) fn unresolved_prerequisites(&self) -> &'static [DurableCoreCommitPrerequisite] {
        &Self::UNRESOLVED_PREREQUISITES
    }

    pub(crate) fn preflight_live_execution(&self) -> Result<(), VfsError> {
        Err(self.unsupported_live_execution_error())
    }

    pub(crate) fn unsupported_live_execution_error(&self) -> VfsError {
        VfsError::NotSupported {
            message: DURABLE_CORE_COMMIT_EXECUTION_NOT_SUPPORTED.to_string(),
        }
    }
}

impl DurableCoreStepSemantics {
    const ORDERED_WRITE_PATH: [DurableCoreTransactionStep; 10] = [
        DurableCoreTransactionStep::IdempotencyReservation,
        DurableCoreTransactionStep::AuthPolicyPreflight,
        DurableCoreTransactionStep::StagedObjectUpload,
        DurableCoreTransactionStep::FinalObjectPromotion,
        DurableCoreTransactionStep::ObjectMetadataInsert,
        DurableCoreTransactionStep::CommitMetadataInsert,
        DurableCoreTransactionStep::RefCompareAndSwap,
        DurableCoreTransactionStep::WorkspaceHeadUpdate,
        DurableCoreTransactionStep::AuditAppend,
        DurableCoreTransactionStep::IdempotencyCompletion,
    ];

    pub fn ordered_write_path() -> &'static [DurableCoreTransactionStep] {
        &Self::ORDERED_WRITE_PATH
    }

    pub fn for_step(step: DurableCoreTransactionStep) -> Self {
        match step {
            DurableCoreTransactionStep::IdempotencyReservation
            | DurableCoreTransactionStep::AuthPolicyPreflight
            | DurableCoreTransactionStep::StagedObjectUpload
            | DurableCoreTransactionStep::FinalObjectPromotion
            | DurableCoreTransactionStep::ObjectMetadataInsert
            | DurableCoreTransactionStep::CommitMetadataInsert => Self {
                step,
                commit_point: DurableCoreCommitPoint::Uncommitted,
            },
            DurableCoreTransactionStep::RefCompareAndSwap => Self {
                step,
                commit_point: DurableCoreCommitPoint::CommittedVisibilityPoint,
            },
            DurableCoreTransactionStep::WorkspaceHeadUpdate
            | DurableCoreTransactionStep::AuditAppend => Self {
                step,
                commit_point: DurableCoreCommitPoint::CommittedPartial,
            },
            DurableCoreTransactionStep::IdempotencyCompletion => Self {
                step,
                commit_point: DurableCoreCommitPoint::CommittedComplete,
            },
        }
    }

    pub fn failure_semantics(
        step: DurableCoreTransactionStep,
        timing: DurableCoreFailureTiming,
    ) -> DurableCoreFailureSemantics {
        match step {
            DurableCoreTransactionStep::IdempotencyReservation
            | DurableCoreTransactionStep::AuthPolicyPreflight => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep
                | DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::PreRefCompareAndSwap,
                        recovery_action: DurableCoreRecoveryAction::AbortIdempotencyReservation,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
            },
            DurableCoreTransactionStep::StagedObjectUpload => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep
                | DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::PreRefCompareAndSwap,
                        recovery_action: DurableCoreRecoveryAction::AbortIdempotencyReservation,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: true,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
            },
            DurableCoreTransactionStep::FinalObjectPromotion => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::PreRefCompareAndSwap,
                        recovery_action: DurableCoreRecoveryAction::AbortIdempotencyReservation,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: true,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
                DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::FinalObjectPromotedMetadataMissing,
                        recovery_action: DurableCoreRecoveryAction::RepairMetadataAndRetry,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: true,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::PreserveFinalObject,
                    },
                ),
            },
            DurableCoreTransactionStep::ObjectMetadataInsert => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::FinalObjectPromotedMetadataMissing,
                        recovery_action: DurableCoreRecoveryAction::RepairMetadataAndRetry,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: true,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::PreserveFinalObject,
                    },
                ),
                DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class:
                            DurableCoreFailureClass::ObjectMetadataInsertedBeforeCommitMetadata,
                        recovery_action:
                            DurableCoreRecoveryAction::RetryCommitMetadataInsertThenRefCompareAndSwap,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
            },
            DurableCoreTransactionStep::CommitMetadataInsert => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::PreRefCompareAndSwap,
                        recovery_action: DurableCoreRecoveryAction::AbortIdempotencyReservation,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
                DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class:
                            DurableCoreFailureClass::CommitMetadataUnreachableBeforeRefCompareAndSwap,
                        recovery_action:
                            DurableCoreRecoveryAction::RetryRefCompareAndSwapWithUnreachableCommit,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: true,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
            },
            DurableCoreTransactionStep::RefCompareAndSwap => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::Uncommitted,
                        failure_class: DurableCoreFailureClass::PreRefCompareAndSwap,
                        recovery_action: DurableCoreRecoveryAction::AbortIdempotencyReservation,
                        mutation_visible_through_target_ref: false,
                        default_rollback_allowed: true,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
                DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::CommittedPartial,
                        failure_class: DurableCoreFailureClass::PostRefCompareAndSwap,
                        recovery_action:
                            DurableCoreRecoveryAction::CompleteIdempotencyWithCommittedResponse,
                        mutation_visible_through_target_ref: true,
                        default_rollback_allowed: false,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
            },
            DurableCoreTransactionStep::WorkspaceHeadUpdate
            | DurableCoreTransactionStep::AuditAppend
            | DurableCoreTransactionStep::IdempotencyCompletion => match timing {
                DurableCoreFailureTiming::BeforeOrDuringStep
                | DurableCoreFailureTiming::AfterStep => DurableCoreFailureSemantics::from_row(
                    step,
                    timing,
                    FailureSemanticsRow {
                        commit_point: DurableCoreCommitPoint::CommittedPartial,
                        failure_class: DurableCoreFailureClass::PostRefCompareAndSwap,
                        recovery_action:
                            DurableCoreRecoveryAction::CompleteIdempotencyWithCommittedResponse,
                        mutation_visible_through_target_ref: true,
                        default_rollback_allowed: false,
                        staged_cleanup_allowed: false,
                        metadata_repair_required: false,
                        unreachable_commit_retry_allowed: false,
                        final_object_cleanup: FinalObjectCleanupDecision::NotApplicable,
                    },
                ),
            },
        }
    }
}

impl DurableCoreFailureSemantics {
    fn from_row(
        step: DurableCoreTransactionStep,
        timing: DurableCoreFailureTiming,
        row: FailureSemanticsRow,
    ) -> Self {
        Self {
            step,
            timing,
            commit_point: row.commit_point,
            failure_class: row.failure_class,
            recovery_action: row.recovery_action,
            mutation_visible_through_target_ref: row.mutation_visible_through_target_ref,
            default_rollback_allowed: row.default_rollback_allowed,
            staged_cleanup_allowed: row.staged_cleanup_allowed,
            metadata_repair_required: row.metadata_repair_required,
            unreachable_commit_retry_allowed: row.unreachable_commit_retry_allowed,
            final_object_cleanup: row.final_object_cleanup,
        }
    }

    pub fn request_fenced_final_object_cleanup(mut self, _fence: FinalObjectMetadataFence) -> Self {
        if self.final_object_cleanup == FinalObjectCleanupDecision::PreserveFinalObject {
            self.final_object_cleanup =
                FinalObjectCleanupDecision::DeleteFinalObjectWithMetadataFence;
        }
        self
    }

    pub fn step(&self) -> DurableCoreTransactionStep {
        self.step
    }

    pub fn timing(&self) -> DurableCoreFailureTiming {
        self.timing
    }

    pub fn commit_point(&self) -> DurableCoreCommitPoint {
        self.commit_point
    }

    pub fn failure_class(&self) -> DurableCoreFailureClass {
        self.failure_class
    }

    pub fn recovery_action(&self) -> DurableCoreRecoveryAction {
        self.recovery_action
    }

    pub fn mutation_visible_through_target_ref(&self) -> bool {
        self.mutation_visible_through_target_ref
    }

    pub fn default_rollback_allowed(&self) -> bool {
        self.default_rollback_allowed
    }

    pub fn staged_cleanup_allowed(&self) -> bool {
        self.staged_cleanup_allowed
    }

    pub fn metadata_repair_required(&self) -> bool {
        self.metadata_repair_required
    }

    pub fn unreachable_commit_retry_allowed(&self) -> bool {
        self.unreachable_commit_retry_allowed
    }

    pub fn final_object_cleanup(&self) -> FinalObjectCleanupDecision {
        self.final_object_cleanup
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use axum::http::HeaderValue;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::audit::{
        AuditAction, AuditActor, AuditEvent, AuditResource, AuditResourceKind, AuditStore,
        InMemoryAuditStore, NewAuditEvent,
    };
    use crate::backend::{
        LocalMemoryCommitStore, LocalMemoryRefStore, RefExpectation, RefStore, RefUpdate,
        RefVersion, RepoId,
    };
    use crate::fs::VirtualFs;
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyReservation, IdempotencyStore,
        InMemoryIdempotencyStore,
    };
    use crate::store::tree::{TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{ChangeKind, CommitId, MAIN_REF, PathKind, PathRecord, RefName};
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    fn commit_id(name: &str) -> CommitId {
        CommitId::from(object_id(name.as_bytes()))
    }

    fn repo() -> RepoId {
        RepoId::local()
    }

    mod durable_core_commit_post_cas_recovery {
        use super::*;

        fn target_for_commit(
            commit_name: &str,
            step: DurableCorePostCasStep,
        ) -> DurableCorePostCasRecoveryTarget {
            DurableCorePostCasRecoveryTarget::new(repo(), MAIN_REF, commit_id(commit_name), step)
                .unwrap()
        }

        fn target(step: DurableCorePostCasStep) -> DurableCorePostCasRecoveryTarget {
            target_for_commit("post-cas", step)
        }

        fn request_for_target(
            target: DurableCorePostCasRecoveryTarget,
            now_millis: u64,
        ) -> DurableCorePostCasRecoveryClaimRequest {
            DurableCorePostCasRecoveryClaimRequest::new(
                target,
                "worker-secret-token",
                Duration::from_secs(30),
                now_millis,
            )
            .unwrap()
        }

        fn request(
            step: DurableCorePostCasStep,
            now_millis: u64,
        ) -> DurableCorePostCasRecoveryClaimRequest {
            request_for_target(target(step), now_millis)
        }

        async fn claim_enqueued(
            store: &InMemoryDurableCorePostCasRecoveryClaimStore,
            request: DurableCorePostCasRecoveryClaimRequest,
        ) -> DurableCorePostCasRecoveryClaim {
            store
                .enqueue(
                    request.target().clone(),
                    request.now_millis().saturating_sub(1),
                )
                .await
                .unwrap();
            store
                .claim(request)
                .await
                .unwrap()
                .expect("enqueued work should be claimable")
        }

        #[tokio::test]
        async fn post_cas_recovery_claim_blocks_duplicate_active_worker() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let first = claim_enqueued(
                &store,
                request(DurableCorePostCasStep::WorkspaceHeadUpdate, 100),
            )
            .await;

            let duplicate = store
                .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 101))
                .await
                .unwrap();

            assert_eq!(first.attempts(), 1);
            assert!(duplicate.is_none());
        }

        #[tokio::test]
        async fn post_cas_recovery_claim_missing_target_does_not_create_work() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();

            let missing = store
                .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 100))
                .await
                .unwrap();

            assert!(missing.is_none());
            assert!(store.list(10).await.unwrap().is_empty());
            assert_eq!(store.counts().await.unwrap().total(), 0);
        }

        #[tokio::test]
        async fn post_cas_recovery_enqueue_makes_pending_work_claimable() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let target = target(DurableCorePostCasStep::WorkspaceHeadUpdate);

            store.enqueue(target.clone(), 50).await.unwrap();

            let statuses = store.list(10).await.unwrap();
            assert_eq!(statuses.len(), 1);
            assert_eq!(statuses[0].target(), &target);
            assert_eq!(
                statuses[0].state(),
                DurableCorePostCasRecoveryState::Pending
            );
            assert_eq!(statuses[0].attempts(), 0);

            let claim = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target,
                        "worker-secret-token",
                        Duration::from_secs(30),
                        100,
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .expect("pending work should be claimable");
            assert_eq!(claim.attempts(), 1);
        }

        #[tokio::test]
        async fn post_cas_recovery_enqueue_is_idempotent_and_does_not_reset_active_work() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let target = target(DurableCorePostCasStep::AuditAppend);
            store.enqueue(target.clone(), 1_000).await.unwrap();
            let first = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target.clone(),
                        "worker-secret-token",
                        Duration::from_secs(30),
                        1_001,
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .expect("pending work should be claimable");

            store.enqueue(target.clone(), 1_002).await.unwrap();
            let duplicate = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target,
                        "second-worker",
                        Duration::from_secs(30),
                        1_003,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(first.attempts(), 1);
            assert!(duplicate.is_none());
        }

        #[tokio::test]
        async fn post_cas_recovery_list_is_bounded_and_redacted() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            store
                .enqueue(
                    target_for_commit("pending", DurableCorePostCasStep::WorkspaceHeadUpdate),
                    10,
                )
                .await
                .unwrap();
            let claim = claim_enqueued(
                &store,
                request_for_target(
                    target_for_commit("backing-off", DurableCorePostCasStep::AuditAppend),
                    20,
                ),
            )
            .await;
            store
                .record_failure(
                    &claim,
                    "raw failure /private/path idempotency-token",
                    Duration::from_millis(250),
                    21,
                )
                .await
                .unwrap();

            let active_claim = claim_enqueued(
                &store,
                request_for_target(
                    target_for_commit("active", DurableCorePostCasStep::IdempotencyCompletion),
                    30,
                ),
            )
            .await;
            let completed_claim = claim_enqueued(
                &store,
                request_for_target(
                    target_for_commit("completed", DurableCorePostCasStep::WorkspaceHeadUpdate),
                    40,
                ),
            )
            .await;
            store.complete(&completed_claim, 41).await.unwrap();
            let poisoned_claim = claim_enqueued(
                &store,
                request_for_target(
                    target_for_commit("poisoned", DurableCorePostCasStep::AuditAppend),
                    50,
                ),
            )
            .await;
            store
                .poison(&poisoned_claim, "raw poison /private/path", 51)
                .await
                .unwrap();

            let statuses = store.list(1).await.unwrap();
            assert_eq!(statuses.len(), 1);
            let mut states = store
                .list(10)
                .await
                .unwrap()
                .into_iter()
                .map(|status| status.state())
                .collect::<Vec<_>>();
            states.sort_by_key(|state| state.as_str());
            assert_eq!(
                states,
                vec![
                    DurableCorePostCasRecoveryState::Active,
                    DurableCorePostCasRecoveryState::BackingOff,
                    DurableCorePostCasRecoveryState::Completed,
                    DurableCorePostCasRecoveryState::Pending,
                    DurableCorePostCasRecoveryState::Poisoned,
                ]
            );
            let counts = store.counts().await.unwrap();
            assert_eq!(counts.pending(), 1);
            assert_eq!(counts.active(), 1);
            assert_eq!(counts.backing_off(), 1);
            assert_eq!(counts.completed(), 1);
            assert_eq!(counts.poisoned(), 1);
            assert_eq!(counts.total(), 5);

            let rendered = format!("{:?}", store.list(10).await.unwrap());
            assert!(rendered.contains("redacted post-CAS recovery failure"));
            for secret in [
                "/private/path",
                "idempotency-token",
                claim.token(),
                active_claim.token(),
            ] {
                assert!(
                    !rendered.contains(secret),
                    "status list leaked {secret}: {rendered}"
                );
            }
        }

        #[tokio::test]
        async fn post_cas_recovery_failure_backs_off_and_retry_gets_new_token() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let first =
                claim_enqueued(&store, request(DurableCorePostCasStep::AuditAppend, 1_000)).await;
            store
                .record_failure(
                    &first,
                    "raw failure at /private/path with token abc123",
                    Duration::from_millis(250),
                    1_050,
                )
                .await
                .unwrap();

            assert!(
                store
                    .claim(request(DurableCorePostCasStep::AuditAppend, 1_299))
                    .await
                    .unwrap()
                    .is_none()
            );
            let retry = store
                .claim(request(DurableCorePostCasStep::AuditAppend, 1_300))
                .await
                .unwrap()
                .expect("claim should reopen after backoff");

            assert_eq!(retry.attempts(), 2);
            assert_ne!(retry.token(), first.token());
        }

        #[tokio::test]
        async fn post_cas_recovery_stale_token_cannot_complete_retry() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let first = claim_enqueued(
                &store,
                request(DurableCorePostCasStep::IdempotencyCompletion, 2_000),
            )
            .await;
            store
                .record_failure(
                    &first,
                    "raw transient token",
                    Duration::from_millis(1),
                    2_001,
                )
                .await
                .unwrap();
            let retry = store
                .claim(request(
                    DurableCorePostCasStep::IdempotencyCompletion,
                    2_002,
                ))
                .await
                .unwrap()
                .unwrap();

            let err = store
                .complete(&first, 2_003)
                .await
                .expect_err("stale token cannot complete retry");
            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(!err.to_string().contains(first.token()));

            store.complete(&retry, 2_003).await.unwrap();
        }

        #[tokio::test]
        async fn post_cas_recovery_stale_owner_cannot_complete_fail_or_poison() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim =
                claim_enqueued(&store, request(DurableCorePostCasStep::AuditAppend, 2_500)).await;
            let stale_owner_claim = DurableCorePostCasRecoveryClaim::for_store(
                claim.target().clone(),
                "different-worker",
                claim.token(),
                claim.attempts(),
                claim.expires_at_millis(),
            );

            let complete_err = store
                .complete(&stale_owner_claim, 2_501)
                .await
                .expect_err("stale owner cannot complete claim");
            assert!(matches!(complete_err, VfsError::InvalidArgs { .. }));
            let failure_err = store
                .record_failure(
                    &stale_owner_claim,
                    "raw stale owner failure",
                    Duration::from_millis(1),
                    2_501,
                )
                .await
                .expect_err("stale owner cannot record failure");
            assert!(matches!(failure_err, VfsError::InvalidArgs { .. }));
            let poison_err = store
                .poison(&stale_owner_claim, "raw stale owner poison", 2_501)
                .await
                .expect_err("stale owner cannot poison");
            assert!(matches!(poison_err, VfsError::InvalidArgs { .. }));

            store.complete(&claim, 2_501).await.unwrap();
        }

        #[tokio::test]
        async fn post_cas_recovery_completed_claim_is_terminal() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim = claim_enqueued(
                &store,
                request(DurableCorePostCasStep::WorkspaceHeadUpdate, 3_000),
            )
            .await;
            store.complete(&claim, 3_001).await.unwrap();

            assert!(
                store
                    .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 9_000))
                    .await
                    .unwrap()
                    .is_none()
            );
            store
                .enqueue(target(DurableCorePostCasStep::WorkspaceHeadUpdate), 9_001)
                .await
                .unwrap();
            let status = store.list(10).await.unwrap().remove(0);
            assert_eq!(status.state(), DurableCorePostCasRecoveryState::Completed);
            assert_eq!(status.terminal_at_millis(), Some(3_001));
        }

        #[tokio::test]
        async fn post_cas_recovery_expired_claim_cannot_complete_fail_or_poison() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim =
                claim_enqueued(&store, request(DurableCorePostCasStep::AuditAppend, 5_000)).await;
            let expired_at = claim.expires_at_millis();

            let complete_err = store
                .complete(&claim, expired_at)
                .await
                .expect_err("expired claim should not complete");
            assert!(matches!(complete_err, VfsError::InvalidArgs { .. }));
            let failure_err = store
                .record_failure(&claim, "too late", Duration::from_millis(1), expired_at)
                .await
                .expect_err("expired claim should not record failure");
            assert!(matches!(failure_err, VfsError::InvalidArgs { .. }));
            let poison_err = store
                .poison(&claim, "too late", expired_at)
                .await
                .expect_err("expired claim should not poison");
            assert!(matches!(poison_err, VfsError::InvalidArgs { .. }));

            let retry = store
                .claim(request(DurableCorePostCasStep::AuditAppend, expired_at))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retry.attempts(), 2);
            assert_ne!(retry.token(), claim.token());
        }

        #[tokio::test]
        async fn post_cas_recovery_poison_blocks_reclaim_and_keeps_redacted_error() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim =
                claim_enqueued(&store, request(DurableCorePostCasStep::AuditAppend, 4_000)).await;

            store
                .poison(
                    &claim,
                    "poison raw /private/path token abc123 worker-secret-token",
                    4_100,
                )
                .await
                .unwrap();
            let complete_err = store
                .complete(&claim, 4_101)
                .await
                .expect_err("poisoned claim should not complete later");
            assert!(matches!(complete_err, VfsError::InvalidArgs { .. }));

            assert!(
                store
                    .claim(request(DurableCorePostCasStep::AuditAppend, 99_000))
                    .await
                    .unwrap()
                    .is_none()
            );
            store
                .enqueue(target(DurableCorePostCasStep::AuditAppend), 99_001)
                .await
                .unwrap();
            let status = store.list(10).await.unwrap().remove(0);
            assert_eq!(status.state(), DurableCorePostCasRecoveryState::Poisoned);
            let rendered = format!("{:?}", store.snapshot().await);
            assert!(rendered.contains("redacted post-CAS recovery failure"));
            for secret in [
                "/private/path",
                "abc123",
                "worker-secret-token",
                claim.token(),
            ] {
                assert!(
                    !rendered.contains(secret),
                    "snapshot leaked {secret}: {rendered}"
                );
            }
        }

        #[test]
        fn post_cas_recovery_rejects_invalid_claim_inputs() {
            let recovery_target = target(DurableCorePostCasStep::WorkspaceHeadUpdate);
            assert!(matches!(
                DurableCorePostCasRecoveryClaimRequest::new(
                    recovery_target.clone(),
                    "bad\nowner",
                    Duration::from_millis(1),
                    1,
                ),
                Err(VfsError::InvalidArgs { .. })
            ));
            assert!(matches!(
                DurableCorePostCasRecoveryClaimRequest::new(
                    recovery_target.clone(),
                    &"x".repeat(129),
                    Duration::from_millis(1),
                    1,
                ),
                Err(VfsError::InvalidArgs { .. })
            ));
            assert!(matches!(
                DurableCorePostCasRecoveryClaimRequest::new(
                    recovery_target,
                    "worker",
                    Duration::from_nanos(1),
                    1,
                ),
                Err(VfsError::InvalidArgs { .. })
            ));
            assert!(matches!(
                DurableCorePostCasRecoveryClaimRequest::new(
                    target(DurableCorePostCasStep::WorkspaceHeadUpdate),
                    "worker",
                    POST_CAS_RECOVERY_MAX_LEASE_DURATION + Duration::from_millis(1),
                    1,
                ),
                Err(VfsError::InvalidArgs { .. })
            ));
        }

        #[tokio::test]
        async fn post_cas_recovery_rejects_unbounded_backoff() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim =
                claim_enqueued(&store, request(DurableCorePostCasStep::AuditAppend, 6_000)).await;

            let err = store
                .record_failure(
                    &claim,
                    "private diagnostic",
                    POST_CAS_RECOVERY_MAX_BACKOFF_DURATION + Duration::from_millis(1),
                    6_001,
                )
                .await
                .expect_err("oversized backoff must be rejected");
            assert!(matches!(err, VfsError::InvalidArgs { .. }));
        }

        #[test]
        fn post_cas_failure_requires_completion_not_rollback() {
            let semantics = DurableCoreStepSemantics::failure_semantics(
                DurableCoreTransactionStep::RefCompareAndSwap,
                DurableCoreFailureTiming::AfterStep,
            );

            assert!(semantics.mutation_visible_through_target_ref());
            assert!(!semantics.default_rollback_allowed());
            assert_eq!(
                semantics.recovery_action(),
                DurableCoreRecoveryAction::CompleteIdempotencyWithCommittedResponse
            );
        }
    }

    mod durable_core_commit_post_cas_repair_worker {
        use super::*;

        fn target_for_commit(
            commit_name: &str,
            step: DurableCorePostCasStep,
        ) -> DurableCorePostCasRecoveryTarget {
            DurableCorePostCasRecoveryTarget::new(repo(), MAIN_REF, commit_id(commit_name), step)
                .unwrap()
        }

        fn audit_event(commit_id: CommitId) -> NewAuditEvent {
            NewAuditEvent::new(
                AuditActor::new(1000, "private-user"),
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Commit, commit_id.to_hex()),
            )
            .with_detail("private-detail", "audit-secret-token")
        }

        fn repair_context(
            commit_id: CommitId,
            workspace_id: Option<Uuid>,
        ) -> DurableCorePostCasRecoveryContext {
            DurableCorePostCasRecoveryContext::new(
                workspace_id,
                None,
                Some(audit_event(commit_id)),
                None,
            )
        }

        fn repair_context_with_idempotency(
            commit_id: CommitId,
            idempotency: DurableCorePostCasIdempotencyRecoveryContext,
        ) -> DurableCorePostCasRecoveryContext {
            DurableCorePostCasRecoveryContext::new(
                None,
                None,
                Some(audit_event(commit_id)),
                Some(idempotency),
            )
        }

        fn commit_record(commit_id: CommitId, message: &str, author: &str) -> CommitRecord {
            CommitRecord {
                repo_id: repo(),
                id: commit_id,
                root_tree: object_id(b"repair-root-tree"),
                parents: Vec::new(),
                timestamp: 1_700_000_000,
                message: message.to_string(),
                author: author.to_string(),
                changed_paths: Vec::new(),
            }
        }

        async fn reserve_idempotency(
            store: &dyn IdempotencyStore,
            scope: &str,
            request_fingerprint: &str,
        ) -> (IdempotencyKey, IdempotencyReservation) {
            let key =
                IdempotencyKey::parse_header_value(&HeaderValue::from_static("repair-worker-key"))
                    .unwrap();
            let reservation = match store.begin(scope, &key, request_fingerprint).await.unwrap() {
                IdempotencyBegin::Execute(reservation) => reservation,
                other => panic!("expected idempotency reservation, got {other:?}"),
            };
            (key, reservation)
        }

        async fn idempotency_replay(
            store: &dyn IdempotencyStore,
            scope: &str,
            key: &IdempotencyKey,
            request_fingerprint: &str,
        ) -> crate::idempotency::IdempotencyRecord {
            match store.begin(scope, key, request_fingerprint).await.unwrap() {
                IdempotencyBegin::Replay(record) => record,
                other => panic!("expected idempotency replay, got {other:?}"),
            }
        }

        fn repair_idempotency_context(
            reservation: &IdempotencyReservation,
            response_kind: DurableCorePostCasIdempotencyResponseKind,
        ) -> DurableCorePostCasIdempotencyRecoveryContext {
            DurableCorePostCasIdempotencyRecoveryContext::from_reservation(
                reservation,
                response_kind,
            )
        }

        async fn run_worker(
            store: &InMemoryDurableCorePostCasRecoveryClaimStore,
            workspaces: &InMemoryWorkspaceMetadataStore,
            limit: usize,
        ) -> DurableCorePostCasRepairWorkerSummary {
            let commits = LocalMemoryCommitStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            run_worker_with_stores(store, &commits, workspaces, &audit, &idempotency, limit).await
        }

        async fn run_worker_with_stores(
            store: &dyn DurableCorePostCasRecoveryClaimStore,
            commits: &dyn CommitStore,
            workspaces: &dyn WorkspaceMetadataStore,
            audit: &dyn AuditStore,
            idempotency: &dyn IdempotencyStore,
            limit: usize,
        ) -> DurableCorePostCasRepairWorkerSummary {
            DurableCorePostCasRepairWorker::new(
                DurableCorePostCasRepairWorkerStores::new(
                    store,
                    commits,
                    workspaces,
                    audit,
                    idempotency,
                ),
                "worker-secret-owner",
                Duration::from_secs(30),
                limit,
            )
            .run()
            .await
            .unwrap()
        }

        async fn run_worker_with_short_backoff(
            store: &dyn DurableCorePostCasRecoveryClaimStore,
            commits: &dyn CommitStore,
            workspaces: &dyn WorkspaceMetadataStore,
            audit: &dyn AuditStore,
            idempotency: &dyn IdempotencyStore,
            limit: usize,
        ) -> DurableCorePostCasRepairWorkerSummary {
            let mut worker = DurableCorePostCasRepairWorker::new(
                DurableCorePostCasRepairWorkerStores::new(
                    store,
                    commits,
                    workspaces,
                    audit,
                    idempotency,
                ),
                "worker-secret-owner",
                Duration::from_secs(30),
                limit,
            );
            worker.failure_backoff = Duration::from_millis(1);
            worker.run().await.unwrap()
        }

        #[derive(Debug, Default)]
        struct FailingOnceIdempotencyEnqueueRecoveryStore {
            inner: InMemoryDurableCorePostCasRecoveryClaimStore,
            failed: RwLock<bool>,
        }

        #[derive(Debug)]
        struct StaticCommitStore {
            record: CommitRecord,
        }

        #[async_trait::async_trait]
        impl CommitStore for StaticCommitStore {
            async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
                Ok(record)
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                Ok(Some(self.record.clone()))
            }

            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                Ok(vec![self.record.clone()])
            }
        }

        #[async_trait::async_trait]
        impl DurableCorePostCasRecoveryClaimStore for FailingOnceIdempotencyEnqueueRecoveryStore {
            async fn enqueue(
                &self,
                target: DurableCorePostCasRecoveryTarget,
                now_millis: u64,
            ) -> Result<(), VfsError> {
                self.inner.enqueue(target, now_millis).await
            }

            async fn enqueue_with_context(
                &self,
                target: DurableCorePostCasRecoveryTarget,
                context: DurableCorePostCasRecoveryContext,
                now_millis: u64,
            ) -> Result<(), VfsError> {
                if target.step() == DurableCorePostCasStep::IdempotencyCompletion {
                    let mut failed = self.failed.write().await;
                    if !*failed {
                        *failed = true;
                        return Err(VfsError::CorruptStore {
                            message: "redacted one-shot enqueue failure".to_string(),
                        });
                    }
                }
                self.inner
                    .enqueue_with_context(target, context, now_millis)
                    .await
            }

            async fn claim(
                &self,
                request: DurableCorePostCasRecoveryClaimRequest,
            ) -> Result<Option<DurableCorePostCasRecoveryClaim>, VfsError> {
                self.inner.claim(request).await
            }

            async fn complete(
                &self,
                claim: &DurableCorePostCasRecoveryClaim,
                now_millis: u64,
            ) -> Result<(), VfsError> {
                self.inner.complete(claim, now_millis).await
            }

            async fn record_failure(
                &self,
                claim: &DurableCorePostCasRecoveryClaim,
                diagnosis: &str,
                backoff: Duration,
                now_millis: u64,
            ) -> Result<(), VfsError> {
                self.inner
                    .record_failure(claim, diagnosis, backoff, now_millis)
                    .await
            }

            async fn poison(
                &self,
                claim: &DurableCorePostCasRecoveryClaim,
                diagnosis: &str,
                now_millis: u64,
            ) -> Result<(), VfsError> {
                self.inner.poison(claim, diagnosis, now_millis).await
            }

            async fn list(
                &self,
                limit: usize,
            ) -> Result<Vec<DurableCorePostCasRecoveryStatus>, VfsError> {
                self.inner.list(limit).await
            }

            async fn counts(&self) -> Result<DurableCorePostCasRecoveryCounts, VfsError> {
                self.inner.counts().await
            }
        }

        #[tokio::test]
        async fn repair_worker_missing_context_does_not_repair_side_effects() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("repair-workspace", "/tmp/private-root")
                .await
                .unwrap();
            store
                .enqueue(
                    target_for_commit(
                        "missing-context",
                        DurableCorePostCasStep::WorkspaceHeadUpdate,
                    ),
                    1,
                )
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.poisoned(), 1);
            assert_eq!(summary.completed(), 0);
            assert!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .is_none()
            );
            assert_eq!(store.counts().await.unwrap().poisoned(), 1);
        }

        #[tokio::test]
        async fn repair_worker_workspace_step_updates_head_and_enqueues_audit_before_completing_claim()
         {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("repair-workspace", "/tmp/private-root")
                .await
                .unwrap();
            let commit_id = commit_id("workspace-repair");
            let target = target_for_commit(
                "workspace-repair",
                DurableCorePostCasStep::WorkspaceHeadUpdate,
            );
            let context = repair_context(commit_id, Some(workspace.id));
            store
                .enqueue_with_context(target.clone(), context, 1)
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.completed(), 1);
            assert_eq!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .as_deref(),
                Some(commit_id.to_hex().as_str())
            );
            let statuses = store.list(10).await.unwrap();
            assert_eq!(
                statuses
                    .iter()
                    .find(|status| status.target() == &target)
                    .unwrap()
                    .state(),
                DurableCorePostCasRecoveryState::Completed
            );
            assert_eq!(
                statuses
                    .iter()
                    .find(|status| {
                        status.target().commit_id() == commit_id
                            && status.target().step() == DurableCorePostCasStep::AuditAppend
                    })
                    .unwrap()
                    .state(),
                DurableCorePostCasRecoveryState::Pending
            );
        }

        #[tokio::test]
        async fn repair_worker_contextual_enqueue_upgrades_existing_pending_row() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let target = target_for_commit("context-upgrade", DurableCorePostCasStep::AuditAppend);
            store.enqueue(target.clone(), 1).await.unwrap();

            store
                .enqueue_with_context(target.clone(), repair_context(target.commit_id(), None), 2)
                .await
                .unwrap();

            let claim = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target,
                        "context-worker",
                        Duration::from_secs(30),
                        3,
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .expect("context-upgraded row should be claimable");
            assert!(claim.context().is_some());
        }

        #[tokio::test]
        async fn repair_worker_contextual_enqueue_rejects_active_no_context_row() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let target = target_for_commit(
                "active-context-upgrade",
                DurableCorePostCasStep::AuditAppend,
            );
            store.enqueue(target.clone(), 1).await.unwrap();
            let _claim = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target.clone(),
                        "active-worker",
                        Duration::from_secs(30),
                        current_unix_timestamp_millis(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .expect("active row should be claimable");

            let err = store
                .enqueue_with_context(target.clone(), repair_context(target.commit_id(), None), 2)
                .await
                .expect_err("active no-context row cannot be safely upgraded");

            assert!(matches!(err, VfsError::CorruptStore { .. }));
        }

        #[tokio::test]
        async fn repair_worker_does_not_complete_workspace_when_audit_followup_is_active_without_context()
         {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("repair-workspace", "/tmp/private-root")
                .await
                .unwrap();
            let commit_id = commit_id("active-audit-followup");
            let audit_target =
                target_for_commit("active-audit-followup", DurableCorePostCasStep::AuditAppend);
            store.enqueue(audit_target, 1).await.unwrap();
            let _audit_claim = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target_for_commit(
                            "active-audit-followup",
                            DurableCorePostCasStep::AuditAppend,
                        ),
                        "active-audit-worker",
                        Duration::from_secs(30),
                        current_unix_timestamp_millis(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .expect("audit follow-up should be actively leased");
            let workspace_target = target_for_commit(
                "active-audit-followup",
                DurableCorePostCasStep::WorkspaceHeadUpdate,
            );
            store
                .enqueue_with_context(
                    workspace_target.clone(),
                    repair_context(commit_id, Some(workspace.id)),
                    1,
                )
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.completed(), 0);
            assert_eq!(summary.backing_off(), 1);
            let workspace_status = store
                .list(10)
                .await
                .unwrap()
                .into_iter()
                .find(|status| status.target() == &workspace_target)
                .unwrap();
            assert_eq!(
                workspace_status.state(),
                DurableCorePostCasRecoveryState::BackingOff
            );
        }

        #[tokio::test]
        async fn repair_worker_reaches_due_work_behind_terminal_rows() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("repair-workspace", "/tmp/private-root")
                .await
                .unwrap();
            for index in 0..20 {
                let target = target_for_commit(
                    &format!("terminal-before-due-{index}"),
                    DurableCorePostCasStep::AuditAppend,
                );
                store.enqueue(target.clone(), index).await.unwrap();
                let claim = store
                    .claim(
                        DurableCorePostCasRecoveryClaimRequest::new(
                            target,
                            "terminal-worker",
                            Duration::from_secs(30),
                            index + 100,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                store.complete(&claim, index + 101).await.unwrap();
            }
            let commit_id = commit_id("due-behind-terminal");
            store
                .enqueue_with_context(
                    target_for_commit(
                        "due-behind-terminal",
                        DurableCorePostCasStep::WorkspaceHeadUpdate,
                    ),
                    repair_context(commit_id, Some(workspace.id)),
                    1_000,
                )
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 1).await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.completed(), 1);
            assert_eq!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .as_deref(),
                Some(commit_id.to_hex().as_str())
            );
        }

        #[tokio::test]
        async fn repair_worker_rejects_context_with_audit_event_for_different_commit() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("repair-workspace", "/tmp/private-root")
                .await
                .unwrap();
            let target = target_for_commit(
                "audit-binding-target",
                DurableCorePostCasStep::WorkspaceHeadUpdate,
            );
            let mismatched_context = DurableCorePostCasRecoveryContext::new(
                Some(workspace.id),
                None,
                Some(audit_event(commit_id("different-audit-commit"))),
                None,
            );
            store
                .enqueue_with_context(target, mismatched_context, 1)
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.poisoned(), 1);
            assert!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .is_none()
            );
        }

        #[tokio::test]
        async fn repair_worker_audit_step_appends_contextual_event_and_completes_claim() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("audit-repair");
            store
                .enqueue_with_context(
                    target_for_commit("audit-repair", DurableCorePostCasStep::AuditAppend),
                    repair_context(commit_id, None),
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.completed(), 1);
            assert!(
                audit
                    .contains_vcs_commit_event(&commit_id.to_hex())
                    .await
                    .unwrap()
            );
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
            assert_eq!(store.counts().await.unwrap().completed(), 1);
        }

        #[tokio::test]
        async fn repair_worker_audit_step_avoids_duplicate_append_when_event_already_exists() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("audit-duplicate");
            audit.append(audit_event(commit_id)).await.unwrap();
            store
                .enqueue_with_context(
                    target_for_commit("audit-duplicate", DurableCorePostCasStep::AuditAppend),
                    repair_context(commit_id, None),
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;

            assert_eq!(summary.attempted(), 1);
            assert_eq!(summary.completed(), 1);
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn repair_worker_audit_step_enqueues_idempotency_before_completion() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("audit-enqueues-idempotency");
            let (_key, reservation) =
                reserve_idempotency(&idempotency, "vcs:commit:audit-enqueue", &"a".repeat(64))
                    .await;
            let context = repair_context_with_idempotency(
                commit_id,
                repair_idempotency_context(
                    &reservation,
                    DurableCorePostCasIdempotencyResponseKind::Partial,
                ),
            );
            store
                .enqueue_with_context(
                    target_for_commit(
                        "audit-enqueues-idempotency",
                        DurableCorePostCasStep::AuditAppend,
                    ),
                    context,
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;

            assert_eq!(summary.completed(), 1);
            let statuses = store.list(10).await.unwrap();
            assert_eq!(
                statuses
                    .iter()
                    .find(|status| {
                        status.target().commit_id() == commit_id
                            && status.target().step() == DurableCorePostCasStep::AuditAppend
                    })
                    .unwrap()
                    .state(),
                DurableCorePostCasRecoveryState::Completed
            );
            assert_eq!(
                statuses
                    .iter()
                    .find(|status| {
                        status.target().commit_id() == commit_id
                            && status.target().step()
                                == DurableCorePostCasStep::IdempotencyCompletion
                    })
                    .unwrap()
                    .state(),
                DurableCorePostCasRecoveryState::Pending
            );
        }

        #[tokio::test]
        async fn repair_worker_retry_after_audit_enqueue_failure_does_not_duplicate_audit_event() {
            let store = FailingOnceIdempotencyEnqueueRecoveryStore::default();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("audit-retry-no-duplicate");
            let (_key, reservation) =
                reserve_idempotency(&idempotency, "vcs:commit:audit-retry", &"e".repeat(64)).await;
            let context = repair_context_with_idempotency(
                commit_id,
                repair_idempotency_context(
                    &reservation,
                    DurableCorePostCasIdempotencyResponseKind::Partial,
                ),
            );
            store
                .inner
                .enqueue_with_context(
                    target_for_commit(
                        "audit-retry-no-duplicate",
                        DurableCorePostCasStep::AuditAppend,
                    ),
                    context,
                    1,
                )
                .await
                .unwrap();

            let first = run_worker_with_short_backoff(
                &store,
                &commits,
                &workspaces,
                &audit,
                &idempotency,
                10,
            )
            .await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            let second = run_worker_with_short_backoff(
                &store,
                &commits,
                &workspaces,
                &audit,
                &idempotency,
                10,
            )
            .await;

            assert_eq!(first.backing_off(), 1);
            assert_eq!(second.completed(), 1);
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
            assert_eq!(store.counts().await.unwrap().completed(), 1);
            assert_eq!(store.counts().await.unwrap().pending(), 1);
        }

        #[tokio::test]
        async fn repair_worker_idempotency_step_replays_full_commit_response() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("idempotency-full");
            audit.append(audit_event(commit_id)).await.unwrap();
            commits
                .insert(commit_record(
                    commit_id,
                    "private full message",
                    "private-author",
                ))
                .await
                .unwrap();
            let scope = "vcs:commit:idempotency-full";
            let request_fingerprint = "b".repeat(64);
            let (key, reservation) =
                reserve_idempotency(&idempotency, scope, &request_fingerprint).await;
            let context = repair_context_with_idempotency(
                commit_id,
                repair_idempotency_context(
                    &reservation,
                    DurableCorePostCasIdempotencyResponseKind::FullCommit,
                ),
            );
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-full",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    context,
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;
            let replay = idempotency_replay(&idempotency, scope, &key, &request_fingerprint).await;

            assert_eq!(summary.completed(), 1);
            assert_eq!(replay.status_code, 200);
            assert_eq!(
                replay.response_body,
                json!({
                    "hash": commit_id.to_hex(),
                    "message": "private full message",
                    "author": "private-author",
                })
            );
        }

        #[tokio::test]
        async fn repair_worker_idempotency_full_response_rejects_mismatched_commit_store_record() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let target_commit_id = commit_id("idempotency-full-mismatch");
            let commits = StaticCommitStore {
                record: commit_record(commit_id("wrong-idempotency-full"), "wrong", "wrong-author"),
            };
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            audit.append(audit_event(target_commit_id)).await.unwrap();
            let scope = "vcs:commit:idempotency-full-mismatch";
            let request_fingerprint = "a1".repeat(32);
            let (key, reservation) =
                reserve_idempotency(&idempotency, scope, &request_fingerprint).await;
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-full-mismatch",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    repair_context_with_idempotency(
                        target_commit_id,
                        repair_idempotency_context(
                            &reservation,
                            DurableCorePostCasIdempotencyResponseKind::FullCommit,
                        ),
                    ),
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;

            assert_eq!(summary.completed(), 0);
            assert_eq!(summary.backing_off(), 1);
            assert!(matches!(
                idempotency
                    .begin(scope, &key, &request_fingerprint)
                    .await
                    .unwrap(),
                IdempotencyBegin::InProgress
            ));
        }

        #[tokio::test]
        async fn repair_worker_idempotency_step_replays_partial_response() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("idempotency-partial");
            audit.append(audit_event(commit_id)).await.unwrap();
            let scope = "vcs:commit:idempotency-partial";
            let request_fingerprint = "c".repeat(64);
            let (key, reservation) =
                reserve_idempotency(&idempotency, scope, &request_fingerprint).await;
            let context = repair_context_with_idempotency(
                commit_id,
                repair_idempotency_context(
                    &reservation,
                    DurableCorePostCasIdempotencyResponseKind::Partial,
                ),
            );
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-partial",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    context,
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;
            let replay = idempotency_replay(&idempotency, scope, &key, &request_fingerprint).await;

            assert_eq!(summary.completed(), 1);
            assert_eq!(replay.status_code, 202);
            assert_eq!(
                replay.response_body,
                DurableCoreCommittedResponse::partial_body()
            );
        }

        #[tokio::test]
        async fn repair_worker_idempotency_step_waits_for_audit_prerequisite() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("idempotency-waits-for-audit");
            let scope = "vcs:commit:idempotency-waits";
            let request_fingerprint = "f".repeat(64);
            let (key, reservation) =
                reserve_idempotency(&idempotency, scope, &request_fingerprint).await;
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-waits-for-audit",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    repair_context_with_idempotency(
                        commit_id,
                        repair_idempotency_context(
                            &reservation,
                            DurableCorePostCasIdempotencyResponseKind::Partial,
                        ),
                    ),
                    1,
                )
                .await
                .unwrap();

            let first = run_worker_with_short_backoff(
                &store,
                &commits,
                &workspaces,
                &audit,
                &idempotency,
                10,
            )
            .await;
            assert_eq!(first.backing_off(), 1);
            assert!(matches!(
                idempotency
                    .begin(scope, &key, &request_fingerprint)
                    .await
                    .unwrap(),
                IdempotencyBegin::InProgress
            ));

            audit.append(audit_event(commit_id)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
            let second = run_worker_with_short_backoff(
                &store,
                &commits,
                &workspaces,
                &audit,
                &idempotency,
                10,
            )
            .await;
            let replay = idempotency_replay(&idempotency, scope, &key, &request_fingerprint).await;

            assert_eq!(second.completed(), 1);
            assert_eq!(replay.status_code, 202);
            assert_eq!(
                replay.response_body,
                DurableCoreCommittedResponse::partial_body()
            );
        }

        #[tokio::test]
        async fn repair_worker_idempotency_mismatched_completed_replay_backs_off() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let commits = LocalMemoryCommitStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let commit_id = commit_id("idempotency-mismatch");
            audit.append(audit_event(commit_id)).await.unwrap();
            let scope = "vcs:commit:idempotency-mismatch";
            let request_fingerprint = "d".repeat(64);
            let (key, reservation) =
                reserve_idempotency(&idempotency, scope, &request_fingerprint).await;
            idempotency
                .complete_or_match(&reservation, 409, json!({"already": "different"}))
                .await
                .unwrap();
            let context = repair_context_with_idempotency(
                commit_id,
                repair_idempotency_context(
                    &reservation,
                    DurableCorePostCasIdempotencyResponseKind::Partial,
                ),
            );
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-mismatch",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    context,
                    1,
                )
                .await
                .unwrap();

            let summary =
                run_worker_with_stores(&store, &commits, &workspaces, &audit, &idempotency, 10)
                    .await;
            let replay = idempotency_replay(&idempotency, scope, &key, &request_fingerprint).await;

            assert_eq!(summary.completed(), 0);
            assert_eq!(summary.backing_off(), 1);
            assert_eq!(replay.status_code, 409);
            assert_eq!(replay.response_body, json!({"already": "different"}));
            assert_eq!(store.counts().await.unwrap().backing_off(), 1);
        }

        #[tokio::test]
        async fn repair_worker_idempotency_missing_or_invalid_context_poisons_claim() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let missing_commit = commit_id("idempotency-missing-context");
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-missing-context",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    repair_context(missing_commit, None),
                    1,
                )
                .await
                .unwrap();
            let invalid_commit = commit_id("idempotency-invalid-context");
            store
                .enqueue_with_context(
                    target_for_commit(
                        "idempotency-invalid-context",
                        DurableCorePostCasStep::IdempotencyCompletion,
                    ),
                    repair_context_with_idempotency(
                        invalid_commit,
                        DurableCorePostCasIdempotencyRecoveryContext::new(
                            "vcs:commit",
                            "not-a-valid-key-hash",
                            "not-a-valid-request-fingerprint",
                            "reservation-token",
                            DurableCorePostCasIdempotencyResponseKind::Partial,
                        ),
                    ),
                    1,
                )
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;

            assert_eq!(summary.attempted(), 2);
            assert_eq!(summary.poisoned(), 2);
            assert_eq!(store.counts().await.unwrap().poisoned(), 2);
        }

        #[tokio::test]
        async fn repair_worker_stale_claim_token_cannot_finalize_retry() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let target = target_for_commit("stale-worker", DurableCorePostCasStep::AuditAppend);
            store
                .enqueue_with_context(
                    target.clone(),
                    repair_context(target.commit_id(), None),
                    1_000,
                )
                .await
                .unwrap();
            let first = store
                .claim(
                    DurableCorePostCasRecoveryClaimRequest::new(
                        target,
                        "first-worker-secret",
                        Duration::from_secs(30),
                        1_001,
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            store
                .record_failure(&first, "private failure", Duration::from_millis(1), 1_002)
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;

            assert_eq!(summary.completed(), 1);
            let err = store
                .complete(&first, 1_004)
                .await
                .expect_err("stale claim cannot complete retry finalized by worker");
            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(!err.to_string().contains(first.token()));
        }

        #[tokio::test]
        async fn repair_worker_summary_debug_redacts_private_context() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let commit_id = commit_id("redaction");
            store
                .enqueue_with_context(
                    target_for_commit("redaction", DurableCorePostCasStep::AuditAppend),
                    repair_context(commit_id, None),
                    1,
                )
                .await
                .unwrap();

            let summary = run_worker(&store, &workspaces, 10).await;
            let rendered = format!("{summary:?} {:?}", store.snapshot().await);

            assert!(rendered.contains("DurableCorePostCasRepairWorkerSummary"));
            assert!(rendered.contains("DurableCorePostCasRecoveryContext"));
            for secret in [
                "private-user",
                "audit-secret-token",
                "private-detail",
                "worker-secret-owner",
            ] {
                assert!(
                    !rendered.contains(secret),
                    "repair worker debug leaked {secret}: {rendered}"
                );
            }
        }
    }

    mod durable_core_commit_post_cas_completion {
        use async_trait::async_trait;
        use tokio::sync::Mutex;

        use super::*;
        use crate::backend::{LocalMemoryCommitStore, LocalMemoryObjectStore};

        const TIMESTAMP: u64 = 1_999_999_999;
        const AUTHOR: &str = "private author <private@example.com>";
        const MESSAGE: &str = "private post-cas message";
        const PRIVATE_PATH: &str = "/nested/private-token.txt";
        const PRIVATE_BYTES: &[u8] = b"private-post-cas-bytes";
        const SCOPE: &str = "core-transaction:post-cas";
        const REQUEST_FINGERPRINT: &str = "private request fingerprint";

        fn private_plan() -> DurableCoreCommitObjectTreeWritePlan {
            let mut fs = VirtualFs::new();
            fs.mkdir("/nested", 0, 0).unwrap();
            fs.create_file(PRIVATE_PATH, 0, 0, None).unwrap();
            fs.write_file(PRIVATE_PATH, PRIVATE_BYTES.to_vec()).unwrap();
            fs.create_file("/public.txt", 0, 0, None).unwrap();
            fs.write_file("/public.txt", b"public".to_vec()).unwrap();
            DurableCoreCommitObjectTreeWritePlan::build(
                DurableCoreCommitSourceSnapshot::unborn(),
                &fs,
            )
            .unwrap()
        }

        async fn visible_commit() -> (
            RepoId,
            DurableCoreCommitObjectTreeWritePlan,
            DurableCoreCommitMetadataInsert,
            DurableCoreCommitRefCasVisibility,
        ) {
            let repo_id = repo();
            let plan = private_plan();
            let object_store = LocalMemoryObjectStore::new();
            let commit_store = LocalMemoryCommitStore::new();
            let ref_store = LocalMemoryRefStore::new();
            let convergence = plan
                .converge_objects(&repo_id, &object_store)
                .await
                .unwrap();
            let metadata = plan
                .insert_commit_metadata(&convergence, &commit_store, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap();
            let visibility = plan
                .apply_ref_cas_visibility(&metadata, &ref_store)
                .await
                .unwrap();
            (repo_id, plan, metadata, visibility)
        }

        fn audit_event(commit_id: CommitId) -> NewAuditEvent {
            NewAuditEvent::new(
                AuditActor::new(1000, "private-user"),
                AuditAction::VcsCommit,
                AuditResource::id(AuditResourceKind::Commit, commit_id.to_hex()),
            )
            .with_detail("redacted", "true")
        }

        async fn reserve_idempotency(
            store: &dyn IdempotencyStore,
        ) -> (IdempotencyKey, IdempotencyReservation) {
            let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static(
                "post-cas-private-token",
            ))
            .unwrap();
            let reservation = match store.begin(SCOPE, &key, REQUEST_FINGERPRINT).await.unwrap() {
                IdempotencyBegin::Execute(reservation) => reservation,
                other => panic!("expected execution reservation, got {other:?}"),
            };
            (key, reservation)
        }

        async fn replay(
            store: &dyn IdempotencyStore,
            key: &IdempotencyKey,
        ) -> crate::idempotency::IdempotencyRecord {
            match store.begin(SCOPE, key, REQUEST_FINGERPRINT).await.unwrap() {
                IdempotencyBegin::Replay(record) => record,
                other => panic!("expected replay, got {other:?}"),
            }
        }

        fn committed_response() -> DurableCoreCommittedResponse {
            DurableCoreCommittedResponse::new(
                201,
                json!({
                    "committed": true,
                    "private_body_token": "body-secret-token"
                }),
            )
            .unwrap()
        }

        #[tokio::test]
        async fn post_cas_completion_updates_workspace_head_appends_audit_and_completes_idempotency()
         {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let (key, reservation) = reserve_idempotency(&idempotency).await;
            let response = committed_response();
            let expected_body = response.response_body().clone();

            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(audit_event(metadata.commit_id()), response)
                        .with_workspace_id(workspace.id)
                        .with_idempotency_reservation(reservation),
                )
                .unwrap();

            let outcome = envelope.complete(&workspaces, &audit, &idempotency).await;

            assert!(matches!(
                outcome,
                DurableCorePostCasOutcome::Complete { .. }
            ));
            assert_eq!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit,
                Some(metadata.commit_id().to_hex())
            );
            let events = audit.list_recent(10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].action, AuditAction::VcsCommit);
            let replay = replay(&idempotency, &key).await;
            assert_eq!(replay.status_code, 201);
            assert_eq!(replay.response_body, expected_body);
        }

        #[tokio::test]
        async fn post_cas_completion_without_workspace_or_idempotency_still_appends_audit() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    ),
                )
                .unwrap();

            let outcome = envelope.complete(&workspaces, &audit, &idempotency).await;

            assert!(matches!(
                outcome,
                DurableCorePostCasOutcome::Complete { .. }
            ));
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn post_cas_workspace_head_failure_returns_partial_without_completing_idempotency() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let (key, reservation) = reserve_idempotency(&idempotency).await;
            let missing_workspace_id = Uuid::new_v4();
            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    )
                    .with_workspace_id(missing_workspace_id)
                    .with_idempotency_reservation(reservation),
                )
                .unwrap();

            let outcome = envelope.complete(&workspaces, &audit, &idempotency).await;

            assert!(matches!(
                outcome,
                DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
                    failed_step: DurableCorePostCasStep::WorkspaceHeadUpdate,
                    idempotency_completion_attempted: false,
                    idempotency_completed: false,
                    ..
                })
            ));
            assert!(audit.list_recent(10).await.unwrap().is_empty());
            assert!(matches!(
                idempotency
                    .begin(SCOPE, &key, REQUEST_FINGERPRINT)
                    .await
                    .unwrap(),
                crate::idempotency::IdempotencyBegin::InProgress
            ));
            envelope
                .complete_partial_idempotency_replay(&idempotency)
                .await
                .unwrap();
            let replay = replay(&idempotency, &key).await;
            assert_eq!(replay.status_code, 202);
            assert_eq!(
                replay.response_body,
                DurableCoreCommittedResponse::partial_body()
            );
        }

        #[derive(Debug, Default)]
        struct LeakyAuditStore {
            attempts: Mutex<usize>,
        }

        #[async_trait]
        impl AuditStore for LeakyAuditStore {
            async fn append(&self, _event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
                *self.attempts.lock().await += 1;
                Err(VfsError::CorruptStore {
                    message: "audit failed with private-token /nested/private-token.txt"
                        .to_string(),
                })
            }

            async fn list_recent(&self, _limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
                Ok(Vec::new())
            }

            async fn contains_vcs_commit_event(&self, _commit_id: &str) -> Result<bool, VfsError> {
                Ok(false)
            }
        }

        #[tokio::test]
        async fn post_cas_audit_failure_returns_partial_without_completing_idempotency() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let audit = LeakyAuditStore::default();
            let idempotency = InMemoryIdempotencyStore::new();
            let (key, reservation) = reserve_idempotency(&idempotency).await;
            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    )
                    .with_workspace_id(workspace.id)
                    .with_idempotency_reservation(reservation),
                )
                .unwrap();

            let outcome = envelope.complete(&workspaces, &audit, &idempotency).await;

            assert!(matches!(
                outcome,
                DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
                    failed_step: DurableCorePostCasStep::AuditAppend,
                    idempotency_completion_attempted: false,
                    idempotency_completed: false,
                    ..
                })
            ));
            assert_eq!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit,
                Some(metadata.commit_id().to_hex())
            );
            assert_eq!(*audit.attempts.lock().await, 1);
            assert!(matches!(
                idempotency
                    .begin(SCOPE, &key, REQUEST_FINGERPRINT)
                    .await
                    .unwrap(),
                crate::idempotency::IdempotencyBegin::InProgress
            ));
            envelope
                .complete_partial_idempotency_replay(&idempotency)
                .await
                .unwrap();
            let replay = replay(&idempotency, &key).await;
            assert_eq!(replay.status_code, 202);
            let rendered = format!("{outcome:?}");
            assert!(!rendered.contains("private-token"));
            assert!(!rendered.contains(PRIVATE_PATH));
        }

        #[derive(Debug)]
        struct FailingIdempotencyStore {
            inner: InMemoryIdempotencyStore,
            complete_attempts: Mutex<usize>,
        }

        impl Default for FailingIdempotencyStore {
            fn default() -> Self {
                Self {
                    inner: InMemoryIdempotencyStore::new(),
                    complete_attempts: Mutex::new(0),
                }
            }
        }

        #[async_trait]
        impl IdempotencyStore for FailingIdempotencyStore {
            async fn begin(
                &self,
                scope: &str,
                key: &IdempotencyKey,
                request_fingerprint: &str,
            ) -> Result<IdempotencyBegin, VfsError> {
                self.inner.begin(scope, key, request_fingerprint).await
            }

            async fn complete(
                &self,
                _reservation: &IdempotencyReservation,
                _status_code: u16,
                _response_body: serde_json::Value,
            ) -> Result<(), VfsError> {
                *self.complete_attempts.lock().await += 1;
                Err(VfsError::CorruptStore {
                    message: "idempotency failed with reservation-token private-token".to_string(),
                })
            }

            async fn abort(&self, reservation: &IdempotencyReservation) {
                self.inner.abort(reservation).await;
            }
        }

        #[tokio::test]
        async fn post_cas_idempotency_completion_failure_is_partial_after_audit() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let audit = InMemoryAuditStore::new();
            let idempotency = FailingIdempotencyStore::default();
            let (_key, reservation) = reserve_idempotency(&idempotency).await;
            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    )
                    .with_workspace_id(workspace.id)
                    .with_idempotency_reservation(reservation),
                )
                .unwrap();

            let outcome = envelope.complete(&workspaces, &audit, &idempotency).await;

            assert!(matches!(
                outcome,
                DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
                    failed_step: DurableCorePostCasStep::IdempotencyCompletion,
                    ..
                })
            ));
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
            assert_eq!(*idempotency.complete_attempts.lock().await, 1);
        }

        #[derive(Debug)]
        struct FailingOnceIdempotencyStore {
            inner: InMemoryIdempotencyStore,
            complete_attempts: Mutex<usize>,
            failures_remaining: Mutex<usize>,
        }

        impl Default for FailingOnceIdempotencyStore {
            fn default() -> Self {
                Self {
                    inner: InMemoryIdempotencyStore::new(),
                    complete_attempts: Mutex::new(0),
                    failures_remaining: Mutex::new(1),
                }
            }
        }

        #[async_trait]
        impl IdempotencyStore for FailingOnceIdempotencyStore {
            async fn begin(
                &self,
                scope: &str,
                key: &IdempotencyKey,
                request_fingerprint: &str,
            ) -> Result<IdempotencyBegin, VfsError> {
                self.inner.begin(scope, key, request_fingerprint).await
            }

            async fn complete(
                &self,
                reservation: &IdempotencyReservation,
                status_code: u16,
                response_body: serde_json::Value,
            ) -> Result<(), VfsError> {
                *self.complete_attempts.lock().await += 1;
                let should_fail = {
                    let mut guard = self.failures_remaining.lock().await;
                    if *guard == 0 {
                        false
                    } else {
                        *guard -= 1;
                        true
                    }
                };
                if should_fail {
                    return Err(VfsError::CorruptStore {
                        message: "idempotency failed with reservation-token private-token"
                            .to_string(),
                    });
                }
                self.inner
                    .complete(reservation, status_code, response_body)
                    .await
            }

            async fn abort(&self, reservation: &IdempotencyReservation) {
                self.inner.abort(reservation).await;
            }
        }

        #[tokio::test]
        async fn post_cas_recovery_step_completes_idempotency_without_duplicate_audit() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let audit = InMemoryAuditStore::new();
            let idempotency = FailingOnceIdempotencyStore::default();
            let (key, reservation) = reserve_idempotency(&idempotency).await;
            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    )
                    .with_workspace_id(workspace.id)
                    .with_idempotency_reservation(reservation),
                )
                .unwrap();

            let first = envelope.complete(&workspaces, &audit, &idempotency).await;
            assert!(matches!(
                first,
                DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
                    failed_step: DurableCorePostCasStep::IdempotencyCompletion,
                    ..
                })
            ));
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);

            let recovered = envelope
                .complete_recovery_step(
                    DurableCorePostCasStep::IdempotencyCompletion,
                    &workspaces,
                    &audit,
                    &idempotency,
                )
                .await;

            assert!(matches!(
                recovered,
                DurableCorePostCasOutcome::Complete { .. }
            ));
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
            assert_eq!(*idempotency.complete_attempts.lock().await, 2);
            let replay = replay(&idempotency, &key).await;
            assert_eq!(replay.status_code, 201);
        }

        #[tokio::test]
        async fn post_cas_workspace_head_update_is_fenced_against_newer_head() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let newer_head = commit_id("newer-visible-commit").to_hex();
            workspaces
                .update_head_commit(workspace.id, Some(newer_head.clone()))
                .await
                .unwrap()
                .unwrap();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    )
                    .with_workspace_id(workspace.id),
                )
                .unwrap();

            let outcome = envelope.complete(&workspaces, &audit, &idempotency).await;

            assert!(matches!(
                outcome,
                DurableCorePostCasOutcome::Complete { .. }
            ));
            assert_eq!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .as_deref(),
                Some(newer_head.as_str())
            );
            assert_eq!(audit.list_recent(10).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn post_cas_envelope_rejects_unbound_visibility_before_side_effects() {
            let (_repo_id, plan, metadata, mut visibility) = visible_commit().await;
            visibility.commit_id = commit_id("other-visible-commit");
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let (_key, reservation) = reserve_idempotency(&idempotency).await;

            let err = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()),
                        committed_response(),
                    )
                    .with_workspace_id(workspace.id)
                    .with_idempotency_reservation(reservation),
                )
                .expect_err("unbound visibility must reject envelope");

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .is_none()
            );
            assert!(audit.list_recent(10).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn post_cas_envelope_rejects_unbound_audit_event_before_side_effects() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let workspaces = InMemoryWorkspaceMetadataStore::new();
            let workspace = workspaces
                .create_workspace("post-cas-workspace", "/tmp/post-cas")
                .await
                .unwrap();
            let audit = InMemoryAuditStore::new();
            let idempotency = InMemoryIdempotencyStore::new();
            let (_key, reservation) = reserve_idempotency(&idempotency).await;

            let err = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        NewAuditEvent::new(
                            AuditActor::new(1000, "private-user"),
                            AuditAction::VcsRefUpdate,
                            AuditResource::id(AuditResourceKind::Ref, MAIN_REF),
                        ),
                        committed_response(),
                    )
                    .with_workspace_id(workspace.id)
                    .with_idempotency_reservation(reservation),
                )
                .expect_err("unbound audit event must reject envelope");

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(
                workspaces
                    .get_workspace(workspace.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .head_commit
                    .is_none()
            );
            assert!(audit.list_recent(10).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn post_cas_envelope_debug_redacts_message_author_paths_response_body_and_tokens() {
            let (_repo_id, plan, metadata, visibility) = visible_commit().await;
            let idempotency = InMemoryIdempotencyStore::new();
            let (_key, reservation) = reserve_idempotency(&idempotency).await;

            let envelope = plan
                .post_cas_envelope(
                    &metadata,
                    &visibility,
                    DurableCoreCommitPostCasInput::new(
                        audit_event(metadata.commit_id()).with_detail("secret", "audit-secret"),
                        committed_response(),
                    )
                    .with_workspace_id(Uuid::new_v4())
                    .with_idempotency_reservation(reservation),
                )
                .unwrap();

            let rendered = format!("{envelope:?}");
            assert!(rendered.contains("DurableCoreCommitPostCasEnvelope"));
            assert!(rendered.contains(MAIN_REF));
            assert!(rendered.contains(&metadata.commit_id().short_hex()));
            for secret in [
                MESSAGE,
                AUTHOR,
                PRIVATE_PATH,
                "private-token",
                "private-post-cas-bytes",
                "body-secret-token",
                "audit-secret",
                REQUEST_FINGERPRINT,
            ] {
                assert!(
                    !rendered.contains(secret),
                    "debug leaked {secret}: {rendered}"
                );
            }
        }
    }

    mod durable_core_commit_object_convergence {
        use async_trait::async_trait;
        use tokio::sync::Mutex;

        use super::*;
        use crate::backend::{LocalMemoryObjectStore, ObjectStore, StoredObject};

        fn write_plan_with_private_content() -> DurableCoreCommitObjectTreeWritePlan {
            let mut fs = VirtualFs::new();
            fs.create_file("/private-token.txt", 0, 0, None).unwrap();
            fs.write_file("/private-token.txt", b"secret-token".to_vec())
                .unwrap();
            fs.mkdir("/nested", 0, 0).unwrap();
            fs.create_file("/nested/payload.bin", 0, 0, None).unwrap();
            fs.write_file("/nested/payload.bin", b"nested-private-bytes".to_vec())
                .unwrap();

            DurableCoreCommitObjectTreeWritePlan::build(
                DurableCoreCommitSourceSnapshot::unborn(),
                &fs,
            )
            .unwrap()
        }

        async fn assert_planned_objects_round_trip(
            store: &dyn ObjectStore,
            repo_id: &RepoId,
            plan: &DurableCoreCommitObjectTreeWritePlan,
        ) {
            for planned in plan.planned_objects() {
                let stored = store
                    .get(repo_id, planned.id(), planned.kind())
                    .await
                    .unwrap()
                    .expect("planned object should be persisted");
                assert_eq!(stored.repo_id, *repo_id);
                assert_eq!(stored.id, planned.id());
                assert_eq!(stored.kind, planned.kind());
                assert_eq!(stored.bytes, planned.bytes());
            }
        }

        fn assert_redacted(rendered: &str) {
            assert!(
                !rendered.contains("secret-token"),
                "rendered output leaked file bytes: {rendered}"
            );
            assert!(
                !rendered.contains("nested-private-bytes"),
                "rendered output leaked nested file bytes: {rendered}"
            );
            assert!(
                !rendered.contains("private-token"),
                "rendered output leaked a path: {rendered}"
            );
            assert!(
                !rendered.contains("payload.bin"),
                "rendered output leaked a path: {rendered}"
            );
        }

        #[tokio::test]
        async fn convergence_writes_planned_objects_and_confirms_root_tree() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = LocalMemoryObjectStore::new();

            let convergence = plan.converge_objects(&repo_id, &store).await.unwrap();

            assert_eq!(convergence.repo_id(), &repo_id);
            assert_eq!(convergence.root_tree_id(), plan.root_tree_id());
            assert_eq!(convergence.object_count(), plan.planned_objects().len());
            assert_eq!(convergence.objects().len(), plan.planned_objects().len());
            assert!(
                convergence
                    .objects()
                    .iter()
                    .zip(plan.planned_objects())
                    .all(|(converged, planned)| {
                        converged.kind() == planned.kind()
                            && converged.id() == planned.id()
                            && converged.byte_len() == planned.bytes().len()
                    })
            );
            assert_planned_objects_round_trip(&store, &repo_id, &plan).await;
            assert!(
                store
                    .contains(&repo_id, plan.root_tree_id(), ObjectKind::Tree)
                    .await
                    .unwrap()
            );
        }

        #[tokio::test]
        async fn convergence_is_idempotent_for_matching_existing_objects() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = LocalMemoryObjectStore::new();

            let first = plan.converge_objects(&repo_id, &store).await.unwrap();
            let second = plan.converge_objects(&repo_id, &store).await.unwrap();

            assert_eq!(first, second);
            assert_planned_objects_round_trip(&store, &repo_id, &plan).await;
        }

        #[derive(Debug, Default)]
        struct WrongObjectStore;

        #[async_trait]
        impl ObjectStore for WrongObjectStore {
            async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
                Ok(StoredObject {
                    repo_id: write.repo_id,
                    id: object_id(b"wrong-object-id"),
                    kind: ObjectKind::Tree,
                    bytes: write.bytes,
                })
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<Option<StoredObject>, VfsError> {
                Ok(None)
            }

            async fn contains(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<bool, VfsError> {
                Ok(true)
            }
        }

        #[tokio::test]
        async fn convergence_rejects_store_returning_wrong_object_without_leaking_bytes() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = WrongObjectStore;

            let err = plan
                .converge_objects(&repo_id, &store)
                .await
                .expect_err("mismatched store response should fail convergence");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("object convergence returned mismatched object"));
            assert_redacted(&rendered);
        }

        #[derive(Debug, Default)]
        struct MissingRootObjectStore {
            put_order: Mutex<Vec<ObjectId>>,
            root_checks: Mutex<Vec<ObjectId>>,
        }

        #[async_trait]
        impl ObjectStore for MissingRootObjectStore {
            async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
                self.put_order.lock().await.push(write.id);
                Ok(StoredObject {
                    repo_id: write.repo_id,
                    id: write.id,
                    kind: write.kind,
                    bytes: write.bytes,
                })
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<Option<StoredObject>, VfsError> {
                Ok(None)
            }

            async fn contains(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<bool, VfsError> {
                self.root_checks.lock().await.push(_id);
                Ok(false)
            }
        }

        #[tokio::test]
        async fn convergence_rejects_missing_root_after_puts_without_commit_side_effects() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = MissingRootObjectStore::default();

            let err = plan
                .converge_objects(&repo_id, &store)
                .await
                .expect_err("missing root tree should fail convergence");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("object convergence did not persist root tree"));
            assert_redacted(&rendered);

            let put_order = store.put_order.lock().await.clone();
            let expected_order = plan
                .planned_objects()
                .iter()
                .map(DurableCorePlannedObject::id)
                .collect::<Vec<_>>();
            assert_eq!(put_order, expected_order);
            assert_eq!(
                store.root_checks.lock().await.as_slice(),
                &[plan.root_tree_id()]
            );
        }

        #[derive(Debug, Default)]
        struct LeakyPutErrorObjectStore;

        #[async_trait]
        impl ObjectStore for LeakyPutErrorObjectStore {
            async fn put(&self, _write: ObjectWrite) -> Result<StoredObject, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "downstream leaked secret-token at /private-token.txt and payload.bin"
                        .to_string(),
                })
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<Option<StoredObject>, VfsError> {
                Ok(None)
            }

            async fn contains(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<bool, VfsError> {
                Ok(false)
            }
        }

        #[tokio::test]
        async fn convergence_wraps_put_errors_without_leaking_store_message() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = LeakyPutErrorObjectStore;

            let err = plan
                .converge_objects(&repo_id, &store)
                .await
                .expect_err("downstream put error should fail convergence");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("object convergence failed to persist planned object"));
            assert!(!rendered.contains("downstream leaked"));
            assert_redacted(&rendered);
        }

        #[derive(Debug, Default)]
        struct LeakyContainsErrorObjectStore;

        #[async_trait]
        impl ObjectStore for LeakyContainsErrorObjectStore {
            async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
                Ok(StoredObject {
                    repo_id: write.repo_id,
                    id: write.id,
                    kind: write.kind,
                    bytes: write.bytes,
                })
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<Option<StoredObject>, VfsError> {
                Ok(None)
            }

            async fn contains(
                &self,
                _repo_id: &RepoId,
                _id: ObjectId,
                _expected_kind: ObjectKind,
            ) -> Result<bool, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "downstream leaked nested-private-bytes at /nested/payload.bin"
                        .to_string(),
                })
            }
        }

        #[tokio::test]
        async fn convergence_wraps_root_check_errors_without_leaking_store_message() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = LeakyContainsErrorObjectStore;

            let err = plan
                .converge_objects(&repo_id, &store)
                .await
                .expect_err("downstream root check error should fail convergence");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("object convergence failed to verify root tree"));
            assert!(!rendered.contains("downstream leaked"));
            assert_redacted(&rendered);
        }

        #[tokio::test]
        async fn convergence_summary_debug_redacts_object_bytes_and_paths() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let store = LocalMemoryObjectStore::new();

            let convergence = plan.converge_objects(&repo_id, &store).await.unwrap();
            let rendered = format!("{convergence:?}");

            assert!(rendered.contains(repo_id.as_str()));
            assert!(rendered.contains(&plan.root_tree_id().short_hex()));
            assert!(rendered.contains(&plan.planned_objects().len().to_string()));
            for planned in plan.planned_objects() {
                assert!(rendered.contains(&planned.id().short_hex()));
                assert!(rendered.contains(&planned.bytes().len().to_string()));
            }
            assert_redacted(&rendered);
        }
    }

    mod durable_core_commit_metadata_insert {
        use async_trait::async_trait;
        use tokio::sync::Mutex;

        use super::*;
        use crate::backend::{
            CommitRecord, CommitStore, LocalMemoryCommitStore, LocalMemoryObjectStore,
        };

        const TIMESTAMP: u64 = 1_777_777_777;
        const AUTHOR: &str = "private author <private@example.com>";
        const MESSAGE: &str = "private metadata insert message";
        const PRIVATE_PATH: &str = "/nested/private-token.txt";
        const PRIVATE_BYTES: &[u8] = b"metadata-private-bytes";

        fn write_plan_with_private_content() -> DurableCoreCommitObjectTreeWritePlan {
            let mut fs = VirtualFs::new();
            fs.mkdir("/nested", 0, 0).unwrap();
            fs.create_file(PRIVATE_PATH, 0, 0, None).unwrap();
            fs.write_file(PRIVATE_PATH, PRIVATE_BYTES.to_vec()).unwrap();
            fs.create_file("/public.txt", 0, 0, None).unwrap();
            fs.write_file("/public.txt", b"public".to_vec()).unwrap();

            DurableCoreCommitObjectTreeWritePlan::build(
                DurableCoreCommitSourceSnapshot::unborn(),
                &fs,
            )
            .unwrap()
        }

        fn write_plan_with_parent(parent_id: CommitId) -> DurableCoreCommitObjectTreeWritePlan {
            let base = vec![PathRecord {
                path: "/existing.txt".to_string(),
                kind: PathKind::File,
                mode: 0o644,
                uid: 0,
                gid: 0,
                size: b"before".len() as u64,
                content_id: Some(object_id(b"before")),
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            }];
            let source = DurableCoreCommitSourceSnapshot::new(
                DurableCoreCommitParentState::Existing {
                    target: parent_id,
                    version: RefVersion::new(9).unwrap(),
                },
                base,
            );
            let mut fs = VirtualFs::new();
            fs.create_file("/existing.txt", 0, 0, None).unwrap();
            fs.write_file("/existing.txt", b"after".to_vec()).unwrap();

            DurableCoreCommitObjectTreeWritePlan::build(source, &fs).unwrap()
        }

        async fn converge(
            repo_id: &RepoId,
            plan: &DurableCoreCommitObjectTreeWritePlan,
        ) -> DurableCoreObjectConvergence {
            let object_store = LocalMemoryObjectStore::new();
            plan.converge_objects(repo_id, &object_store).await.unwrap()
        }

        fn parent_record(repo_id: RepoId, parent_id: CommitId) -> CommitRecord {
            CommitRecord {
                repo_id,
                id: parent_id,
                root_tree: object_id(b"parent-root-tree"),
                parents: Vec::new(),
                timestamp: 1,
                message: "parent".to_string(),
                author: "parent-author".to_string(),
                changed_paths: Vec::new(),
            }
        }

        fn assert_metadata_insert_error_redacted(rendered: &str) {
            for secret in [
                MESSAGE,
                AUTHOR,
                PRIVATE_PATH,
                "private-token",
                "metadata-private-bytes",
                "leaky sql detail",
                "leaky parent check",
                "raw-bytes",
            ] {
                assert!(
                    !rendered.contains(secret),
                    "metadata insert error leaked {secret}: {rendered}"
                );
            }
        }

        #[tokio::test]
        async fn metadata_insert_records_unborn_commit_after_convergence_without_ref_visibility() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let convergence = converge(&repo_id, &plan).await;
            let commits = LocalMemoryCommitStore::new();
            let refs = LocalMemoryRefStore::new();

            let inserted = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap();

            assert_eq!(inserted.repo_id(), &repo_id);
            assert_eq!(inserted.root_tree_id(), plan.root_tree_id());
            assert!(inserted.parents().is_empty());
            assert_eq!(inserted.changed_path_count(), plan.changed_paths().len());
            assert_eq!(inserted.timestamp(), TIMESTAMP);

            let stored = commits
                .get(&repo_id, inserted.commit_id())
                .await
                .unwrap()
                .expect("commit metadata should be inserted");
            assert_eq!(stored.repo_id, repo_id);
            assert_eq!(stored.id, inserted.commit_id());
            assert_eq!(stored.root_tree, plan.root_tree_id());
            assert!(stored.parents.is_empty());
            assert_eq!(stored.author, AUTHOR);
            assert_eq!(stored.message, MESSAGE);
            assert_eq!(stored.timestamp, TIMESTAMP);
            assert_eq!(stored.changed_paths, plan.changed_paths());

            let main = RefName::new(MAIN_REF).unwrap();
            assert!(refs.get(&repo(), &main).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn metadata_insert_records_existing_parent_after_parent_validation() {
            let repo_id = repo();
            let parent_id = commit_id("validated-parent");
            let plan = write_plan_with_parent(parent_id);
            let convergence = converge(&repo_id, &plan).await;
            let commits = LocalMemoryCommitStore::new();
            commits
                .insert(parent_record(repo_id.clone(), parent_id))
                .await
                .unwrap();

            let inserted = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap();

            assert_eq!(inserted.parents(), &[parent_id]);
            let stored = commits
                .get(&repo_id, inserted.commit_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.parents, vec![parent_id]);
        }

        #[tokio::test]
        async fn metadata_insert_rejects_missing_parent_without_inserting() {
            let repo_id = repo();
            let plan = write_plan_with_parent(commit_id("missing-parent"));
            let convergence = converge(&repo_id, &plan).await;
            let commits = LocalMemoryCommitStore::new();

            let err = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .expect_err("missing parent must reject metadata insert");

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(
                err.to_string()
                    .contains("durable commit parent metadata is missing")
            );
            assert!(commits.list(&repo_id).await.unwrap().is_empty());
        }

        #[derive(Debug, Default)]
        struct LeakyParentCheckCommitStore {
            insert_attempts: Mutex<usize>,
        }

        #[async_trait]
        impl CommitStore for LeakyParentCheckCommitStore {
            async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
                *self.insert_attempts.lock().await += 1;
                Ok(record)
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                Ok(None)
            }

            async fn contains(&self, _repo_id: &RepoId, _id: CommitId) -> Result<bool, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "leaky parent check with private-token and raw-bytes".to_string(),
                })
            }

            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                Ok(Vec::new())
            }
        }

        #[tokio::test]
        async fn metadata_insert_wraps_parent_check_error_without_inserting_or_leaking() {
            let repo_id = repo();
            let plan = write_plan_with_parent(commit_id("parent-check-error"));
            let convergence = converge(&repo_id, &plan).await;
            let commits = LeakyParentCheckCommitStore::default();

            let err = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .expect_err("parent check error must reject metadata insert");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("durable commit parent metadata check failed"));
            assert_metadata_insert_error_redacted(&rendered);
            assert_eq!(*commits.insert_attempts.lock().await, 0);
        }

        #[tokio::test]
        async fn metadata_insert_rejects_mismatched_convergence_without_inserting() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let mut convergence = converge(&repo_id, &plan).await;
            convergence.root_tree_id = object_id(b"wrong-root-tree");
            let commits = LocalMemoryCommitStore::new();

            let err = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .expect_err("mismatched convergence must reject metadata insert");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(
                rendered.contains("durable commit object convergence does not match write plan")
            );
            assert_metadata_insert_error_redacted(&rendered);
            assert!(commits.list(&repo_id).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn metadata_insert_is_idempotent_for_matching_existing_commit() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let convergence = converge(&repo_id, &plan).await;
            let commits = LocalMemoryCommitStore::new();

            let first = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap();
            let second = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap();

            assert_eq!(first, second);
            assert_eq!(commits.list(&repo_id).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn metadata_insert_rejects_conflicting_duplicate_without_leaking_inputs() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let convergence = converge(&repo_id, &plan).await;
            let commits = LocalMemoryCommitStore::new();
            let mut conflicting = durable_commit_record_for_metadata_insert(
                repo_id.clone(),
                &plan,
                TIMESTAMP,
                AUTHOR,
                MESSAGE,
            );
            conflicting.root_tree = object_id(b"conflicting-root-tree");
            conflicting.message = "conflicting private message".to_string();
            conflicting.author = "conflicting private author".to_string();
            commits.insert(conflicting).await.unwrap();

            let err = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .expect_err("conflicting duplicate must reject");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("durable commit metadata insert failed"));
            assert_metadata_insert_error_redacted(&rendered);
        }

        #[derive(Debug, Default)]
        struct LeakyFailingCommitStore;

        #[async_trait]
        impl CommitStore for LeakyFailingCommitStore {
            async fn insert(&self, _record: CommitRecord) -> Result<CommitRecord, VfsError> {
                Err(VfsError::CorruptStore {
                    message:
                        "leaky sql detail: missing root-tree FK for raw-bytes metadata-private-bytes"
                            .to_string(),
                })
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                Ok(None)
            }

            async fn contains(&self, _repo_id: &RepoId, _id: CommitId) -> Result<bool, VfsError> {
                Ok(true)
            }

            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                Ok(Vec::new())
            }
        }

        #[tokio::test]
        async fn metadata_insert_wraps_root_tree_fk_failure_without_leaking_store_message() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let convergence = converge(&repo_id, &plan).await;

            let err = plan
                .insert_commit_metadata(
                    &convergence,
                    &LeakyFailingCommitStore,
                    TIMESTAMP,
                    AUTHOR,
                    MESSAGE,
                )
                .await
                .expect_err("store failure must be redacted");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("durable commit metadata insert failed"));
            assert_metadata_insert_error_redacted(&rendered);
        }

        #[derive(Debug, Default)]
        struct MismatchedCommitStore;

        #[async_trait]
        impl CommitStore for MismatchedCommitStore {
            async fn insert(&self, mut record: CommitRecord) -> Result<CommitRecord, VfsError> {
                record.root_tree = object_id(b"mismatched-root-tree");
                record.message = "mismatched private message".to_string();
                Ok(record)
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                Ok(None)
            }

            async fn contains(&self, _repo_id: &RepoId, _id: CommitId) -> Result<bool, VfsError> {
                Ok(true)
            }

            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                Ok(Vec::new())
            }
        }

        #[tokio::test]
        async fn metadata_insert_rejects_store_returning_mismatched_record() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let convergence = converge(&repo_id, &plan).await;

            let err = plan
                .insert_commit_metadata(
                    &convergence,
                    &MismatchedCommitStore,
                    TIMESTAMP,
                    AUTHOR,
                    MESSAGE,
                )
                .await
                .expect_err("mismatched store return must reject");
            let rendered = err.to_string();

            assert!(matches!(err, VfsError::CorruptStore { .. }));
            assert!(rendered.contains("durable commit metadata insert returned mismatched record"));
            assert_metadata_insert_error_redacted(&rendered);
        }

        #[tokio::test]
        async fn metadata_insert_debug_redacts_message_author_paths_and_bytes() {
            let repo_id = repo();
            let plan = write_plan_with_private_content();
            let convergence = converge(&repo_id, &plan).await;
            let commits = LocalMemoryCommitStore::new();

            let inserted = plan
                .insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap();
            let rendered = format!("{inserted:?}");

            assert!(rendered.contains("DurableCoreCommitMetadataInsert"));
            assert!(rendered.contains("changed_path_count"));
            assert!(rendered.contains(&TIMESTAMP.to_string()));
            assert_metadata_insert_error_redacted(&rendered);
        }
    }

    mod durable_core_commit_ref_cas_visibility {
        use async_trait::async_trait;

        use super::*;
        use crate::backend::{LocalMemoryCommitStore, LocalMemoryObjectStore, RefRecord};

        const TIMESTAMP: u64 = 1_888_888_888;
        const AUTHOR: &str = "private author <private@example.com>";
        const MESSAGE: &str = "private commit message";
        const PRIVATE_PATH: &str = "/nested/private-token.txt";
        const PRIVATE_BYTES: &[u8] = b"private-commit-bytes";

        fn private_unborn_plan() -> DurableCoreCommitObjectTreeWritePlan {
            let mut fs = VirtualFs::new();
            fs.mkdir("/nested", 0, 0).unwrap();
            fs.create_file(PRIVATE_PATH, 0, 0, None).unwrap();
            fs.write_file(PRIVATE_PATH, PRIVATE_BYTES.to_vec()).unwrap();
            fs.create_file("/public.txt", 0, 0, None).unwrap();
            fs.write_file("/public.txt", b"public".to_vec()).unwrap();
            DurableCoreCommitObjectTreeWritePlan::build(
                DurableCoreCommitSourceSnapshot::unborn(),
                &fs,
            )
            .unwrap()
        }

        fn file_record(path: &str, bytes: &[u8]) -> PathRecord {
            PathRecord {
                path: path.to_string(),
                kind: PathKind::File,
                mode: 0o644,
                uid: 0,
                gid: 0,
                size: bytes.len() as u64,
                content_id: Some(object_id(bytes)),
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            }
        }

        fn private_existing_plan(
            parent_id: CommitId,
            version: RefVersion,
        ) -> DurableCoreCommitObjectTreeWritePlan {
            let source = DurableCoreCommitSourceSnapshot::new(
                DurableCoreCommitParentState::Existing {
                    target: parent_id,
                    version,
                },
                vec![file_record("/existing.txt", b"before")],
            );
            let mut fs = VirtualFs::new();
            fs.create_file("/existing.txt", 0, 0, None).unwrap();
            fs.write_file("/existing.txt", b"after".to_vec()).unwrap();
            fs.mkdir("/nested", 0, 0).unwrap();
            fs.create_file(PRIVATE_PATH, 0, 0, None).unwrap();
            fs.write_file(PRIVATE_PATH, PRIVATE_BYTES.to_vec()).unwrap();
            DurableCoreCommitObjectTreeWritePlan::build(source, &fs).unwrap()
        }

        async fn insert_metadata(
            repo_id: &RepoId,
            plan: &DurableCoreCommitObjectTreeWritePlan,
        ) -> DurableCoreCommitMetadataInsert {
            let objects = LocalMemoryObjectStore::new();
            let commits = LocalMemoryCommitStore::new();
            if let DurableCoreCommitParentState::Existing { target, .. } =
                plan.source().parent_state()
            {
                commits
                    .insert(CommitRecord {
                        repo_id: repo_id.clone(),
                        id: target,
                        root_tree: object_id(b"parent-root-tree"),
                        parents: Vec::new(),
                        timestamp: 1,
                        message: "parent".to_string(),
                        author: "parent-author".to_string(),
                        changed_paths: Vec::new(),
                    })
                    .await
                    .unwrap();
            }
            let convergence = plan.converge_objects(repo_id, &objects).await.unwrap();
            plan.insert_commit_metadata(&convergence, &commits, TIMESTAMP, AUTHOR, MESSAGE)
                .await
                .unwrap()
        }

        #[tokio::test]
        async fn ref_cas_visibility_creates_unborn_main_after_metadata_insert() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();

            let visibility = plan
                .apply_ref_cas_visibility(&metadata, &refs)
                .await
                .unwrap();

            assert_eq!(visibility.repo_id(), &repo_id);
            assert_eq!(visibility.ref_name(), MAIN_REF);
            assert_eq!(visibility.commit_id(), metadata.commit_id());
            assert_eq!(visibility.version(), RefVersion::new(1).unwrap());

            let main = RefName::new(MAIN_REF).unwrap();
            let stored = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(stored.repo_id, repo_id);
            assert_eq!(stored.name, main);
            assert_eq!(stored.target, metadata.commit_id());
            assert_eq!(stored.version, RefVersion::new(1).unwrap());
        }

        #[tokio::test]
        async fn ref_cas_visibility_updates_existing_main_using_parent_target_and_version() {
            let repo_id = repo();
            let parent = commit_id("visibility-parent");
            let source_version = RefVersion::new(1).unwrap();
            let plan = private_existing_plan(parent, source_version);
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();
            let main = RefName::new(MAIN_REF).unwrap();
            refs.update(RefUpdate {
                repo_id: repo_id.clone(),
                name: main.clone(),
                target: parent,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

            let visibility = plan
                .apply_ref_cas_visibility(&metadata, &refs)
                .await
                .unwrap();

            assert_eq!(visibility.repo_id(), &repo_id);
            assert_eq!(visibility.ref_name(), MAIN_REF);
            assert_eq!(visibility.commit_id(), metadata.commit_id());
            assert_eq!(visibility.version(), RefVersion::new(2).unwrap());

            let stored = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(stored.target, metadata.commit_id());
            assert_eq!(stored.version, RefVersion::new(2).unwrap());
        }

        #[tokio::test]
        async fn ref_cas_visibility_rejects_stale_unborn_main_without_mutation() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();
            let main = RefName::new(MAIN_REF).unwrap();
            let stale = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: commit_id("racing-target"),
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();

            let err = plan
                .apply_ref_cas_visibility(&metadata, &refs)
                .await
                .expect_err("stale unborn main should fail");
            assert!(matches!(
                err,
                VfsError::InvalidArgs { ref message }
                if message == "ref compare-and-swap mismatch"
            ));

            let current = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(current, stale);
        }

        #[tokio::test]
        async fn ref_cas_visibility_rejects_stale_existing_main_without_mutation() {
            let repo_id = repo();
            let parent = commit_id("stale-parent");
            let source_version = RefVersion::new(1).unwrap();
            let plan = private_existing_plan(parent, source_version);
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();
            let main = RefName::new(MAIN_REF).unwrap();
            let baseline = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: parent,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
            assert_eq!(baseline.version, source_version);
            let racing = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: commit_id("racing-target"),
                    expectation: RefExpectation::Matches {
                        target: parent,
                        version: source_version,
                    },
                })
                .await
                .unwrap();

            let err = plan
                .apply_ref_cas_visibility(&metadata, &refs)
                .await
                .expect_err("stale existing main should fail");
            assert!(matches!(
                err,
                VfsError::InvalidArgs { ref message }
                if message == "ref compare-and-swap mismatch"
            ));

            let current = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(current, racing);
        }

        #[tokio::test]
        async fn ref_cas_visibility_rejects_stale_existing_version_without_mutation() {
            let repo_id = repo();
            let parent = commit_id("stale-version-parent");
            let source_version = RefVersion::new(1).unwrap();
            let plan = private_existing_plan(parent, source_version);
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();
            let main = RefName::new(MAIN_REF).unwrap();
            let baseline = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: parent,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
            assert_eq!(baseline.version, source_version);
            let racing = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: parent,
                    expectation: RefExpectation::Matches {
                        target: parent,
                        version: source_version,
                    },
                })
                .await
                .unwrap();

            let err = plan
                .apply_ref_cas_visibility(&metadata, &refs)
                .await
                .expect_err("stale existing main version should fail");
            assert!(matches!(
                err,
                VfsError::InvalidArgs { ref message }
                if message == "ref compare-and-swap mismatch"
            ));

            let current = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(current, racing);
            assert_eq!(current.target, parent);
            assert_eq!(current.version, RefVersion::new(2).unwrap());
        }

        #[tokio::test]
        async fn ref_cas_visibility_rejects_mismatched_metadata_insert_without_ref_mutation() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();
            let main = RefName::new(MAIN_REF).unwrap();
            let original = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: commit_id("original-target"),
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
            let mut mismatched = metadata.clone();
            mismatched.root_tree_id = object_id(b"mismatched-root");

            let err = plan
                .apply_ref_cas_visibility(&mismatched, &refs)
                .await
                .expect_err("mismatched metadata should fail before ref mutation");
            assert!(matches!(
                err,
                VfsError::CorruptStore { ref message }
                if message == "durable commit ref visibility input does not match write plan"
            ));

            let current = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(current, original);
        }

        #[tokio::test]
        async fn ref_cas_visibility_rejects_unbound_metadata_insert_without_ref_mutation() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();
            let main = RefName::new(MAIN_REF).unwrap();
            let original = refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: commit_id("original-target"),
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
            let mut mismatched = metadata.clone();
            mismatched.plan_fingerprint = object_id(b"different-plan-fingerprint");

            let err = plan
                .apply_ref_cas_visibility(&mismatched, &refs)
                .await
                .expect_err("metadata from another plan should fail before ref mutation");
            assert!(matches!(
                err,
                VfsError::CorruptStore { ref message }
                if message == "durable commit ref visibility input does not match write plan"
            ));

            let current = refs.get(&repo_id, &main).await.unwrap().unwrap();
            assert_eq!(current, original);
        }

        #[derive(Debug, Default)]
        struct LeakyCasErrorRefStore;

        #[async_trait]
        impl RefStore for LeakyCasErrorRefStore {
            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
                Ok(Vec::new())
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _name: &RefName,
            ) -> Result<Option<RefRecord>, VfsError> {
                Ok(None)
            }

            async fn update(&self, _update: RefUpdate) -> Result<RefRecord, VfsError> {
                Err(VfsError::InvalidArgs {
                    message: "ref compare-and-swap mismatch: main".to_string(),
                })
            }

            async fn update_source_checked(
                &self,
                _update: crate::backend::SourceCheckedRefUpdate,
            ) -> Result<RefRecord, VfsError> {
                unreachable!("not used in this test")
            }
        }

        #[tokio::test]
        async fn ref_cas_visibility_sanitizes_leaky_ref_store_cas_errors() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;

            let err = plan
                .apply_ref_cas_visibility(&metadata, &LeakyCasErrorRefStore)
                .await
                .expect_err("cas mismatch should be sanitized");
            let rendered = err.to_string();

            assert!(matches!(
                err,
                VfsError::InvalidArgs { ref message }
                if message == "ref compare-and-swap mismatch"
            ));
            assert!(rendered.contains("ref compare-and-swap mismatch"));
            assert!(!rendered.contains("private-token"));
            assert!(!rendered.contains(MAIN_REF));
        }

        #[derive(Debug, Default)]
        struct LeakyCasPrefixedInvalidArgsRefStore;

        #[async_trait]
        impl RefStore for LeakyCasPrefixedInvalidArgsRefStore {
            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
                Ok(Vec::new())
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _name: &RefName,
            ) -> Result<Option<RefRecord>, VfsError> {
                Ok(None)
            }

            async fn update(&self, _update: RefUpdate) -> Result<RefRecord, VfsError> {
                Err(VfsError::InvalidArgs {
                    message:
                        "ref compare-and-swap mismatch: main private-token /nested/private-token.txt"
                            .to_string(),
                })
            }

            async fn update_source_checked(
                &self,
                _update: crate::backend::SourceCheckedRefUpdate,
            ) -> Result<RefRecord, VfsError> {
                unreachable!("not used in this test")
            }
        }

        #[tokio::test]
        async fn ref_cas_visibility_wraps_cas_prefixed_invalid_args_with_extra_detail() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;

            let err = plan
                .apply_ref_cas_visibility(&metadata, &LeakyCasPrefixedInvalidArgsRefStore)
                .await
                .expect_err("cas-prefixed invalid args with extra details should be wrapped");
            let rendered = err.to_string();

            assert!(matches!(
                err,
                VfsError::CorruptStore { ref message }
                if message == "durable commit ref visibility update failed"
            ));
            assert!(rendered.contains("durable commit ref visibility update failed"));
            assert!(!rendered.contains("private-token"));
            assert!(!rendered.contains(PRIVATE_PATH));
        }

        #[derive(Debug, Default)]
        struct LeakyNonCasErrorRefStore;

        #[async_trait]
        impl RefStore for LeakyNonCasErrorRefStore {
            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
                Ok(Vec::new())
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _name: &RefName,
            ) -> Result<Option<RefRecord>, VfsError> {
                Ok(None)
            }

            async fn update(&self, _update: RefUpdate) -> Result<RefRecord, VfsError> {
                Err(VfsError::CorruptStore {
                    message:
                        "sql failed for /nested/private-token.txt commit deadbeef private-token"
                            .to_string(),
                })
            }

            async fn update_source_checked(
                &self,
                _update: crate::backend::SourceCheckedRefUpdate,
            ) -> Result<RefRecord, VfsError> {
                unreachable!("not used in this test")
            }
        }

        #[tokio::test]
        async fn ref_cas_visibility_wraps_leaky_non_cas_ref_store_errors() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;

            let err = plan
                .apply_ref_cas_visibility(&metadata, &LeakyNonCasErrorRefStore)
                .await
                .expect_err("non-cas store error should be wrapped");
            let rendered = err.to_string();

            assert!(matches!(
                err,
                VfsError::CorruptStore { ref message }
                if message == "durable commit ref visibility update failed"
            ));
            assert!(rendered.contains("durable commit ref visibility update failed"));
            assert!(!rendered.contains("sql failed"));
            assert!(!rendered.contains("private-token"));
        }

        #[derive(Debug, Default)]
        struct MismatchedRecordRefStore;

        #[async_trait]
        impl RefStore for MismatchedRecordRefStore {
            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
                Ok(Vec::new())
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _name: &RefName,
            ) -> Result<Option<RefRecord>, VfsError> {
                Ok(None)
            }

            async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
                Ok(RefRecord {
                    repo_id: update.repo_id,
                    name: RefName::new(MAIN_REF).unwrap(),
                    target: commit_id("wrong-target"),
                    version: RefVersion::new(99).unwrap(),
                })
            }

            async fn update_source_checked(
                &self,
                _update: crate::backend::SourceCheckedRefUpdate,
            ) -> Result<RefRecord, VfsError> {
                unreachable!("not used in this test")
            }
        }

        #[tokio::test]
        async fn ref_cas_visibility_rejects_store_returning_mismatched_record() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;

            let err = plan
                .apply_ref_cas_visibility(&metadata, &MismatchedRecordRefStore)
                .await
                .expect_err("mismatched returned record should fail");
            assert!(matches!(
                err,
                VfsError::CorruptStore { ref message }
                if message == "durable commit ref visibility returned mismatched record"
            ));
        }

        #[tokio::test]
        async fn ref_cas_visibility_debug_redacts_private_commit_context() {
            let repo_id = repo();
            let plan = private_unborn_plan();
            let metadata = insert_metadata(&repo_id, &plan).await;
            let refs = LocalMemoryRefStore::new();

            let visibility = plan
                .apply_ref_cas_visibility(&metadata, &refs)
                .await
                .unwrap();
            let rendered = format!("{visibility:?}");

            assert!(rendered.contains("DurableCoreCommitRefCasVisibility"));
            assert!(rendered.contains(repo_id.as_str()));
            assert!(rendered.contains(MAIN_REF));
            assert!(rendered.contains(&metadata.commit_id().short_hex()));
            assert!(rendered.contains("version"));
            assert!(!rendered.contains(MESSAGE));
            assert!(!rendered.contains(AUTHOR));
            assert!(!rendered.contains(PRIVATE_PATH));
            assert!(!rendered.contains("private-token"));
            assert!(!rendered.contains("private-commit-bytes"));
        }
    }

    mod durable_core_commit_write_plan {
        use super::*;

        fn unborn_source() -> DurableCoreCommitSourceSnapshot {
            DurableCoreCommitSourceSnapshot::unborn()
        }

        fn existing_source(base_records: Vec<PathRecord>) -> DurableCoreCommitSourceSnapshot {
            DurableCoreCommitSourceSnapshot::new(
                DurableCoreCommitParentState::Existing {
                    target: commit_id("source-parent"),
                    version: RefVersion::new(3).unwrap(),
                },
                base_records,
            )
        }

        fn file_record(path: &str, bytes: &[u8]) -> PathRecord {
            PathRecord {
                path: path.to_string(),
                kind: PathKind::File,
                mode: 0o644,
                uid: 0,
                gid: 0,
                size: bytes.len() as u64,
                content_id: Some(object_id(bytes)),
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            }
        }

        fn metadata_file_record(path: &str, bytes: &[u8], mode: u16) -> PathRecord {
            PathRecord {
                mode,
                ..file_record(path, bytes)
            }
        }

        fn object_by_id(
            plan: &DurableCoreCommitObjectTreeWritePlan,
            id: ObjectId,
        ) -> &DurableCorePlannedObject {
            plan.planned_objects()
                .iter()
                .find(|object| object.id == id)
                .expect("planned object should exist")
        }

        fn planned_tree(plan: &DurableCoreCommitObjectTreeWritePlan, id: ObjectId) -> TreeObject {
            let object = object_by_id(plan, id);
            assert_eq!(object.kind, ObjectKind::Tree);
            TreeObject::deserialize(&object.bytes).expect("planned tree should deserialize")
        }

        #[test]
        fn preflight_plans_blobs_trees_and_root_without_store_writes() {
            let mut fs = VirtualFs::new();
            fs.create_file("/alpha.txt", 0, 0, None).unwrap();
            fs.write_file("/alpha.txt", b"alpha".to_vec()).unwrap();
            fs.mkdir("/nested", 0, 0).unwrap();
            fs.create_file("/nested/beta.txt", 0, 0, None).unwrap();
            fs.write_file("/nested/beta.txt", b"beta".to_vec()).unwrap();
            fs.ln_s("/alpha.txt", "/alpha.link", 0, 0).unwrap();

            let plan = DurableCoreCommitObjectTreeWritePlan::build(unborn_source(), &fs).unwrap();

            assert_eq!(
                plan.changed_paths()
                    .iter()
                    .map(|change| (change.path.as_str(), change.kind))
                    .collect::<Vec<_>>(),
                vec![
                    ("/alpha.link", ChangeKind::Added),
                    ("/alpha.txt", ChangeKind::Added),
                    ("/nested", ChangeKind::Added),
                    ("/nested/beta.txt", ChangeKind::Added),
                ]
            );

            assert!(plan.planned_objects().iter().all(|object| {
                object.id == ObjectId::from_bytes(&object.bytes)
                    && matches!(object.kind, ObjectKind::Blob | ObjectKind::Tree)
            }));
            assert_eq!(
                plan.planned_objects().last().map(|object| object.id),
                Some(plan.root_tree_id())
            );

            let root_tree = planned_tree(&plan, plan.root_tree_id());
            assert_eq!(
                root_tree
                    .entries
                    .iter()
                    .map(|entry| (entry.name.as_str(), entry.kind))
                    .collect::<Vec<_>>(),
                vec![
                    ("alpha.link", TreeEntryKind::Symlink),
                    ("alpha.txt", TreeEntryKind::Blob),
                    ("nested", TreeEntryKind::Tree),
                ]
            );
        }

        #[test]
        fn preflight_orders_children_before_parent_trees() {
            let mut fs = VirtualFs::new();
            fs.mkdir_p("/a/b", 0, 0).unwrap();
            fs.create_file("/a/b/c.txt", 0, 0, None).unwrap();
            fs.write_file("/a/b/c.txt", b"child".to_vec()).unwrap();

            let plan = DurableCoreCommitObjectTreeWritePlan::build(unborn_source(), &fs).unwrap();
            let object_positions = plan
                .planned_objects()
                .iter()
                .enumerate()
                .map(|(position, object)| ((object.kind, object.id), position))
                .collect::<Vec<_>>();

            for (tree_position, object) in plan.planned_objects().iter().enumerate() {
                if object.kind != ObjectKind::Tree {
                    continue;
                }

                let tree = TreeObject::deserialize(&object.bytes).unwrap();
                for entry in tree.entries {
                    let child_kind = match entry.kind {
                        TreeEntryKind::Blob | TreeEntryKind::Symlink => ObjectKind::Blob,
                        TreeEntryKind::Tree => ObjectKind::Tree,
                    };
                    let child_position = object_positions
                        .iter()
                        .find_map(|((kind, id), position)| {
                            (*kind == child_kind && *id == entry.id).then_some(*position)
                        })
                        .expect("tree child should be planned");
                    assert!(
                        child_position < tree_position,
                        "child {} should be planned before parent {}",
                        entry.id.short_hex(),
                        object.id.short_hex()
                    );
                }
            }
        }

        #[test]
        fn preflight_deduplicates_identical_planned_objects() {
            let mut fs = VirtualFs::new();
            fs.create_file("/left.txt", 0, 0, None).unwrap();
            fs.write_file("/left.txt", b"same".to_vec()).unwrap();
            fs.create_file("/right.txt", 0, 0, None).unwrap();
            fs.write_file("/right.txt", b"same".to_vec()).unwrap();

            let plan = DurableCoreCommitObjectTreeWritePlan::build(unborn_source(), &fs).unwrap();
            let blob_id = object_id(b"same");

            assert_eq!(
                plan.planned_objects()
                    .iter()
                    .filter(|object| object.kind == ObjectKind::Blob && object.id == blob_id)
                    .count(),
                1
            );

            let root_tree = planned_tree(&plan, plan.root_tree_id());
            assert_eq!(
                root_tree
                    .entries
                    .iter()
                    .map(|entry| (entry.name.as_str(), entry.id))
                    .collect::<Vec<_>>(),
                vec![("left.txt", blob_id), ("right.txt", blob_id)]
            );
        }

        #[test]
        fn preflight_rejects_cross_kind_object_id_collisions_with_redacted_error() {
            let empty_tree_bytes = TreeObject {
                entries: Vec::new(),
            }
            .serialize();
            let mut fs = VirtualFs::new();
            fs.mkdir("/empty", 0, 0).unwrap();
            fs.create_file("/payload.txt", 0, 0, None).unwrap();
            fs.write_file("/payload.txt", empty_tree_bytes).unwrap();

            let err = DurableCoreCommitObjectTreeWritePlan::build(unborn_source(), &fs)
                .expect_err("cross-kind object id collision cannot converge");
            let message = err.to_string();

            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(message.contains("planned object identity collision"));
            assert!(!message.contains("payload"));
            assert!(!message.contains("empty"));
        }

        #[test]
        fn preflight_debug_redacts_planned_bytes_and_paths() {
            let mut fs = VirtualFs::new();
            fs.create_file("/private-token.txt", 0, 0, None).unwrap();
            fs.write_file("/private-token.txt", vec![65, 66, 67])
                .unwrap();

            let plan = DurableCoreCommitObjectTreeWritePlan::build(unborn_source(), &fs).unwrap();
            let plan_debug = format!("{plan:?}");
            let object_debug = format!("{:?}", plan.planned_objects()[0]);

            for rendered in [plan_debug, object_debug] {
                assert!(
                    !rendered.contains("private-token"),
                    "debug output leaked a path: {rendered}"
                );
                assert!(
                    !rendered.contains("65, 66, 67"),
                    "debug output leaked planned bytes: {rendered}"
                );
            }
        }

        #[test]
        fn preflight_normalizes_source_snapshot_changed_paths() {
            let mut fs = VirtualFs::new();
            fs.create_file("/created.txt", 0, 0, None).unwrap();
            fs.write_file("/created.txt", b"created".to_vec()).unwrap();
            fs.create_file("/modified.txt", 0, 0, None).unwrap();
            fs.write_file("/modified.txt", b"new".to_vec()).unwrap();
            fs.create_file("/metadata.txt", 0, 0, Some(0o600)).unwrap();
            fs.write_file("/metadata.txt", b"stable".to_vec()).unwrap();
            fs.create_file("/renamed-new.txt", 0, 0, None).unwrap();
            fs.write_file("/renamed-new.txt", b"rename".to_vec())
                .unwrap();

            let base = vec![
                file_record("/deleted.txt", b"deleted"),
                file_record("/modified.txt", b"old"),
                metadata_file_record("/metadata.txt", b"stable", 0o644),
                file_record("/renamed-old.txt", b"rename"),
            ];
            let source = existing_source(base);
            let parent_state = source.parent_state();
            let plan = DurableCoreCommitObjectTreeWritePlan::build(source, &fs).unwrap();

            assert_eq!(plan.source().parent_state(), parent_state);
            assert_eq!(
                plan.changed_paths()
                    .iter()
                    .map(|change| (change.path.as_str(), change.kind))
                    .collect::<Vec<_>>(),
                vec![
                    ("/created.txt", ChangeKind::Added),
                    ("/deleted.txt", ChangeKind::Deleted),
                    ("/metadata.txt", ChangeKind::MetadataChanged),
                    ("/modified.txt", ChangeKind::Modified),
                    ("/renamed-new.txt", ChangeKind::Added),
                    ("/renamed-old.txt", ChangeKind::Deleted),
                ]
            );
        }

        #[test]
        fn preflight_converts_plan_to_repo_object_writes_without_mutating() {
            let mut fs = VirtualFs::new();
            fs.create_file("/alpha.txt", 0, 0, None).unwrap();
            fs.write_file("/alpha.txt", b"alpha".to_vec()).unwrap();

            let plan = DurableCoreCommitObjectTreeWritePlan::build(unborn_source(), &fs).unwrap();
            let repo_id = RepoId::local();
            let writes = plan.object_writes_for_repo(&repo_id);

            assert_eq!(writes.len(), plan.planned_objects().len());
            for (planned, write) in plan.planned_objects().iter().zip(writes) {
                assert_eq!(write.repo_id, repo_id);
                assert_eq!(write.kind, planned.kind);
                assert_eq!(write.id, planned.id);
                assert_eq!(write.bytes, planned.bytes);
            }
        }

        #[test]
        fn preflight_rejects_unborn_source_with_non_empty_base_records() {
            let source = DurableCoreCommitSourceSnapshot::new(
                DurableCoreCommitParentState::Unborn,
                vec![file_record("/stale.txt", b"stale")],
            );
            let fs = VirtualFs::new();

            let err = DurableCoreCommitObjectTreeWritePlan::build(source, &fs)
                .expect_err("unborn source cannot carry base records");
            assert!(matches!(err, VfsError::InvalidArgs { .. }));
            assert!(!err.to_string().contains("/stale.txt"));
        }
    }

    #[test]
    fn durable_core_transaction_steps_are_ordered_for_commit_visibility() {
        assert_eq!(
            DurableCoreStepSemantics::ordered_write_path(),
            &[
                DurableCoreTransactionStep::IdempotencyReservation,
                DurableCoreTransactionStep::AuthPolicyPreflight,
                DurableCoreTransactionStep::StagedObjectUpload,
                DurableCoreTransactionStep::FinalObjectPromotion,
                DurableCoreTransactionStep::ObjectMetadataInsert,
                DurableCoreTransactionStep::CommitMetadataInsert,
                DurableCoreTransactionStep::RefCompareAndSwap,
                DurableCoreTransactionStep::WorkspaceHeadUpdate,
                DurableCoreTransactionStep::AuditAppend,
                DurableCoreTransactionStep::IdempotencyCompletion,
            ]
        );

        assert_eq!(
            DurableCoreStepSemantics::for_step(DurableCoreTransactionStep::CommitMetadataInsert),
            DurableCoreStepSemantics {
                step: DurableCoreTransactionStep::CommitMetadataInsert,
                commit_point: DurableCoreCommitPoint::Uncommitted,
            }
        );
        assert_eq!(
            DurableCoreStepSemantics::for_step(DurableCoreTransactionStep::RefCompareAndSwap),
            DurableCoreStepSemantics {
                step: DurableCoreTransactionStep::RefCompareAndSwap,
                commit_point: DurableCoreCommitPoint::CommittedVisibilityPoint,
            }
        );
        assert_eq!(
            DurableCoreStepSemantics::for_step(DurableCoreTransactionStep::WorkspaceHeadUpdate),
            DurableCoreStepSemantics {
                step: DurableCoreTransactionStep::WorkspaceHeadUpdate,
                commit_point: DurableCoreCommitPoint::CommittedPartial,
            }
        );
    }

    #[test]
    fn durable_core_commit_skeleton_reuses_ordered_write_path() {
        let skeleton = DurableCoreCommitExecutorSkeleton::new();

        assert_eq!(
            skeleton.ordered_write_path(),
            DurableCoreStepSemantics::ordered_write_path()
        );
    }

    #[test]
    fn durable_core_commit_skeleton_disables_live_execution() {
        let skeleton = DurableCoreCommitExecutorSkeleton::new();

        assert_eq!(
            skeleton.live_execution(),
            DurableCoreCommitLiveExecution::Disabled
        );
        assert!(!skeleton.live_execution_enabled());
    }

    #[test]
    fn durable_core_commit_skeleton_reports_missing_prerequisites() {
        let skeleton = DurableCoreCommitExecutorSkeleton::new();

        assert_eq!(
            skeleton.unresolved_prerequisites(),
            &[
                DurableCoreCommitPrerequisite::DurableObjectByteWrites,
                DurableCoreCommitPrerequisite::LiveTreeConstruction,
                DurableCoreCommitPrerequisite::SourceFilesystemSnapshot,
                DurableCoreCommitPrerequisite::WorkspaceHeadCoupling,
                DurableCoreCommitPrerequisite::AuditAndIdempotencyCompletion,
                DurableCoreCommitPrerequisite::CommitLockingAndFencing,
                DurableCoreCommitPrerequisite::RepairWorker,
            ]
        );
    }

    #[test]
    fn durable_core_commit_metadata_preflight_reports_unborn_main_ref() {
        let preflight =
            DurableCoreCommitMetadataPreflight::for_main(DurableCoreCommitParentState::Unborn);

        assert_eq!(preflight.target_ref(), MAIN_REF);
        assert_eq!(
            preflight.parent_state(),
            DurableCoreCommitParentState::Unborn
        );
    }

    #[test]
    fn durable_core_commit_metadata_preflight_reports_existing_parent() {
        let target = commit_id("parent-target");
        let version = RefVersion::new(7).unwrap();
        let preflight =
            DurableCoreCommitMetadataPreflight::for_main(DurableCoreCommitParentState::Existing {
                target,
                version,
            });

        assert_eq!(
            preflight.parent_state(),
            DurableCoreCommitParentState::Existing { target, version }
        );
    }

    #[test]
    fn durable_core_commit_metadata_preflight_reuses_commit_skeleton_contract() {
        let preflight =
            DurableCoreCommitMetadataPreflight::for_main(DurableCoreCommitParentState::Unborn);
        let skeleton = DurableCoreCommitExecutorSkeleton::new();

        assert_eq!(
            preflight.ordered_write_path(),
            DurableCoreStepSemantics::ordered_write_path()
        );
        assert!(!preflight.live_execution_enabled());
        assert_eq!(
            preflight.unresolved_prerequisites(),
            skeleton.unresolved_prerequisites()
        );
    }

    #[test]
    fn durable_core_commit_skeleton_preflight_error_is_redacted() {
        let skeleton = DurableCoreCommitExecutorSkeleton::new();
        let err = skeleton
            .preflight_live_execution()
            .expect_err("live execution should fail preflight");
        let message = err.to_string();

        assert!(message.contains("durable core commit execution"));
        for forbidden in [
            "private-token",
            "alice",
            "commit message",
            "workspace-secret",
            "STRATUM_CORE_RUNTIME",
            "durable-cloud",
        ] {
            assert!(
                !message.contains(forbidden),
                "durable commit preflight error leaked sensitive input {forbidden:?}: {message}"
            );
        }
    }

    #[test]
    fn ref_cas_failure_timing_distinguishes_pre_commit_from_post_commit_partial() {
        let before_or_during_cas = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::RefCompareAndSwap,
            DurableCoreFailureTiming::BeforeOrDuringStep,
        );
        assert_eq!(
            before_or_during_cas.commit_point(),
            DurableCoreCommitPoint::Uncommitted
        );
        assert_eq!(
            before_or_during_cas.step(),
            DurableCoreTransactionStep::RefCompareAndSwap
        );
        assert_eq!(
            before_or_during_cas.timing(),
            DurableCoreFailureTiming::BeforeOrDuringStep
        );
        assert!(!before_or_during_cas.mutation_visible_through_target_ref());
        assert!(before_or_during_cas.default_rollback_allowed());
        assert_eq!(
            before_or_during_cas.recovery_action(),
            DurableCoreRecoveryAction::AbortIdempotencyReservation
        );

        let after_successful_cas = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::RefCompareAndSwap,
            DurableCoreFailureTiming::AfterStep,
        );
        assert_eq!(
            after_successful_cas.commit_point(),
            DurableCoreCommitPoint::CommittedPartial
        );
        assert_eq!(
            after_successful_cas.step(),
            DurableCoreTransactionStep::RefCompareAndSwap
        );
        assert_eq!(
            after_successful_cas.timing(),
            DurableCoreFailureTiming::AfterStep
        );
        assert!(after_successful_cas.mutation_visible_through_target_ref());
        assert!(!after_successful_cas.default_rollback_allowed());
        assert_eq!(
            after_successful_cas.recovery_action(),
            DurableCoreRecoveryAction::CompleteIdempotencyWithCommittedResponse
        );
    }

    #[test]
    fn commit_metadata_insert_failure_is_unreachable_commit_retry_without_visibility() {
        let failure = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::CommitMetadataInsert,
            DurableCoreFailureTiming::AfterStep,
        );

        assert_eq!(failure.commit_point(), DurableCoreCommitPoint::Uncommitted);
        assert!(!failure.mutation_visible_through_target_ref());
        assert!(failure.unreachable_commit_retry_allowed());
        assert_eq!(
            failure.recovery_action(),
            DurableCoreRecoveryAction::RetryRefCompareAndSwapWithUnreachableCommit
        );
    }

    #[test]
    fn staged_object_upload_failure_allows_staged_cleanup() {
        let failure = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::StagedObjectUpload,
            DurableCoreFailureTiming::AfterStep,
        );

        assert_eq!(failure.commit_point(), DurableCoreCommitPoint::Uncommitted);
        assert!(failure.staged_cleanup_allowed());
        assert!(!failure.mutation_visible_through_target_ref());
        assert_eq!(
            failure.recovery_action(),
            DurableCoreRecoveryAction::AbortIdempotencyReservation
        );
    }

    #[test]
    fn failure_after_final_object_before_metadata_requires_repair_not_delete() {
        let failure = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::ObjectMetadataInsert,
            DurableCoreFailureTiming::BeforeOrDuringStep,
        );

        assert_eq!(failure.commit_point(), DurableCoreCommitPoint::Uncommitted);
        assert!(!failure.mutation_visible_through_target_ref());
        assert!(!failure.staged_cleanup_allowed());
        assert_eq!(
            failure.failure_class(),
            DurableCoreFailureClass::FinalObjectPromotedMetadataMissing
        );
        assert_eq!(
            failure.recovery_action(),
            DurableCoreRecoveryAction::RepairMetadataAndRetry
        );
        assert!(failure.metadata_repair_required());
        assert_eq!(
            failure.final_object_cleanup(),
            FinalObjectCleanupDecision::PreserveFinalObject
        );
    }

    #[test]
    fn final_object_promotion_timing_separates_inflight_from_promoted_missing_metadata() {
        let before_or_during_promotion = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::FinalObjectPromotion,
            DurableCoreFailureTiming::BeforeOrDuringStep,
        );
        assert_eq!(
            before_or_during_promotion.commit_point(),
            DurableCoreCommitPoint::Uncommitted
        );
        assert!(!before_or_during_promotion.mutation_visible_through_target_ref());
        assert!(before_or_during_promotion.staged_cleanup_allowed());
        assert!(!before_or_during_promotion.metadata_repair_required());
        assert_eq!(
            before_or_during_promotion.recovery_action(),
            DurableCoreRecoveryAction::AbortIdempotencyReservation
        );
        assert_eq!(
            before_or_during_promotion.final_object_cleanup(),
            FinalObjectCleanupDecision::NotApplicable
        );

        let after_promotion = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::FinalObjectPromotion,
            DurableCoreFailureTiming::AfterStep,
        );
        assert_eq!(
            after_promotion.commit_point(),
            DurableCoreCommitPoint::Uncommitted
        );
        assert!(!after_promotion.mutation_visible_through_target_ref());
        assert!(!after_promotion.staged_cleanup_allowed());
        assert!(after_promotion.metadata_repair_required());
        assert_eq!(
            after_promotion.recovery_action(),
            DurableCoreRecoveryAction::RepairMetadataAndRetry
        );
        assert_eq!(
            after_promotion.final_object_cleanup(),
            FinalObjectCleanupDecision::PreserveFinalObject
        );

        let metadata_insert_after = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::ObjectMetadataInsert,
            DurableCoreFailureTiming::AfterStep,
        );
        assert!(!metadata_insert_after.metadata_repair_required());
        assert!(!metadata_insert_after.mutation_visible_through_target_ref());
        assert!(metadata_insert_after.default_rollback_allowed());
        assert_eq!(
            metadata_insert_after.failure_class(),
            DurableCoreFailureClass::ObjectMetadataInsertedBeforeCommitMetadata
        );
        assert_eq!(
            metadata_insert_after.recovery_action(),
            DurableCoreRecoveryAction::RetryCommitMetadataInsertThenRefCompareAndSwap
        );
        assert_eq!(
            metadata_insert_after.final_object_cleanup(),
            FinalObjectCleanupDecision::NotApplicable
        );
    }

    #[test]
    fn metadata_fenced_cleanup_only_applies_when_preserve_is_required() {
        let not_applicable = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::FinalObjectPromotion,
            DurableCoreFailureTiming::BeforeOrDuringStep,
        );
        assert_eq!(
            not_applicable.final_object_cleanup(),
            FinalObjectCleanupDecision::NotApplicable
        );
        assert_eq!(
            not_applicable
                .request_fenced_final_object_cleanup(FinalObjectMetadataFence::new())
                .final_object_cleanup(),
            FinalObjectCleanupDecision::NotApplicable
        );

        let preserve_required = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::FinalObjectPromotion,
            DurableCoreFailureTiming::AfterStep,
        );
        assert_eq!(
            preserve_required
                .request_fenced_final_object_cleanup(FinalObjectMetadataFence::new())
                .final_object_cleanup(),
            FinalObjectCleanupDecision::DeleteFinalObjectWithMetadataFence
        );
    }

    #[test]
    fn final_object_deletion_requires_metadata_fencing() {
        let unfenced = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::ObjectMetadataInsert,
            DurableCoreFailureTiming::BeforeOrDuringStep,
        );
        assert_eq!(
            unfenced.final_object_cleanup(),
            FinalObjectCleanupDecision::PreserveFinalObject
        );

        let fenced = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::ObjectMetadataInsert,
            DurableCoreFailureTiming::BeforeOrDuringStep,
        )
        .request_fenced_final_object_cleanup(FinalObjectMetadataFence::new());
        assert_eq!(
            fenced.final_object_cleanup(),
            FinalObjectCleanupDecision::DeleteFinalObjectWithMetadataFence
        );
    }

    #[test]
    fn commit_insert_is_not_visibility_point_and_ref_visibility_requires_cas() {
        let commit_insert =
            DurableCoreStepSemantics::for_step(DurableCoreTransactionStep::CommitMetadataInsert);
        let ref_cas =
            DurableCoreStepSemantics::for_step(DurableCoreTransactionStep::RefCompareAndSwap);

        assert_eq!(
            commit_insert.commit_point,
            DurableCoreCommitPoint::Uncommitted
        );
        assert_eq!(
            ref_cas.commit_point,
            DurableCoreCommitPoint::CommittedVisibilityPoint
        );

        let commit_after = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::CommitMetadataInsert,
            DurableCoreFailureTiming::AfterStep,
        );
        assert!(!commit_after.mutation_visible_through_target_ref());
        assert!(commit_after.unreachable_commit_retry_allowed());
        assert_eq!(
            commit_after.recovery_action(),
            DurableCoreRecoveryAction::RetryRefCompareAndSwapWithUnreachableCommit
        );
    }

    #[tokio::test]
    async fn ref_cas_is_the_visibility_point() {
        let refs = LocalMemoryRefStore::new();
        let main = RefName::new(MAIN_REF).unwrap();
        let base = commit_id("base");
        let head = commit_id("head");

        let initial = refs
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: base,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        assert_eq!(
            refs.get(&repo(), &main).await.unwrap().unwrap().target,
            base
        );

        refs.update(RefUpdate {
            repo_id: repo(),
            name: main.clone(),
            target: head,
            expectation: RefExpectation::Matches {
                target: base,
                version: initial.version,
            },
        })
        .await
        .unwrap();
        assert_eq!(
            refs.get(&repo(), &main).await.unwrap().unwrap().target,
            head
        );
    }

    #[tokio::test]
    async fn idempotency_completion_is_after_committed_response_construction() {
        let post_ref_failure = DurableCoreStepSemantics::failure_semantics(
            DurableCoreTransactionStep::AuditAppend,
            DurableCoreFailureTiming::AfterStep,
        );
        assert_eq!(
            post_ref_failure.recovery_action(),
            DurableCoreRecoveryAction::CompleteIdempotencyWithCommittedResponse
        );
        assert!(post_ref_failure.mutation_visible_through_target_ref());

        let store = InMemoryIdempotencyStore::new();
        let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static("durable-contract"))
            .unwrap();

        let reservation = match store
            .begin("core-transaction:test", &key, "request-a")
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };

        assert!(matches!(
            store
                .begin("core-transaction:test", &key, "request-a")
                .await
                .unwrap(),
            IdempotencyBegin::InProgress
        ));

        let committed_response = json!({"committed": true, "head": "abc123"});
        store
            .complete(&reservation, 201, committed_response.clone())
            .await
            .unwrap();

        let replay = match store
            .begin("core-transaction:test", &key, "request-a")
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay, got {other:?}"),
        };
        assert_eq!(replay.status_code, 201);
        assert_eq!(replay.response_body, committed_response);
    }

    #[tokio::test]
    async fn workspace_head_is_post_ref_and_not_the_visibility_point() {
        let refs = LocalMemoryRefStore::new();
        let workspaces = InMemoryWorkspaceMetadataStore::new();
        let main = RefName::new(MAIN_REF).unwrap();
        let base = commit_id("base");
        let head = commit_id("head");

        let main_ref = refs
            .update(RefUpdate {
                repo_id: repo(),
                name: main.clone(),
                target: base,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();

        let workspace = workspaces
            .create_workspace("tx-workspace", "/tmp/tx-workspace")
            .await
            .unwrap();

        refs.update(RefUpdate {
            repo_id: repo(),
            name: main.clone(),
            target: head,
            expectation: RefExpectation::Matches {
                target: base,
                version: main_ref.version,
            },
        })
        .await
        .unwrap();

        assert_eq!(
            refs.get(&repo(), &main).await.unwrap().unwrap().target,
            head
        );

        let before_head = workspaces
            .get_workspace(workspace.id)
            .await
            .unwrap()
            .unwrap();
        assert!(before_head.head_commit.is_none());

        let after_head = workspaces
            .update_head_commit(workspace.id, Some(head.to_hex()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_head.head_commit, Some(head.to_hex()));
    }
}
