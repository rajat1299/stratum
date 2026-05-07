//! Durable core transaction semantics contract.
//!
//! This module is intentionally landed before the live durable `CoreDb`
//! implementation so the transaction policy is executable and reviewable first.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

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
use crate::vcs::{ChangedPath, CommitId, MAIN_REF, PathRecord, RefName};
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
    token: String,
    attempts: u32,
    expires_at_millis: u64,
}

impl DurableCorePostCasRecoveryClaim {
    pub(crate) fn target(&self) -> &DurableCorePostCasRecoveryTarget {
        &self.target
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
}

impl fmt::Debug for DurableCorePostCasRecoveryClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCorePostCasRecoveryClaim")
            .field("target", &self.target)
            .field("token", &"<redacted>")
            .field("attempts", &self.attempts)
            .field("expires_at_millis", &self.expires_at_millis)
            .finish()
    }
}

#[async_trait::async_trait]
pub(crate) trait DurableCorePostCasRecoveryClaimStore: Send + Sync {
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
    Active {
        token: String,
        attempts: u32,
        expires_at_millis: u64,
    },
    BackingOff {
        attempts: u32,
        retry_after_millis: u64,
        diagnosis: DurableCorePostCasRedactedDiagnosis,
    },
    Completed {
        attempts: u32,
    },
    Poisoned {
        attempts: u32,
        poisoned_at_millis: u64,
        diagnosis: DurableCorePostCasRedactedDiagnosis,
    },
}

impl fmt::Debug for DurableCorePostCasRecoveryEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active {
                attempts,
                expires_at_millis,
                ..
            } => f
                .debug_struct("Active")
                .field("token", &"<redacted>")
                .field("attempts", attempts)
                .field("expires_at_millis", expires_at_millis)
                .finish(),
            Self::BackingOff {
                attempts,
                retry_after_millis,
                diagnosis,
            } => f
                .debug_struct("BackingOff")
                .field("attempts", attempts)
                .field("retry_after_millis", retry_after_millis)
                .field("diagnosis", diagnosis)
                .finish(),
            Self::Completed { attempts } => f
                .debug_struct("Completed")
                .field("attempts", attempts)
                .finish(),
            Self::Poisoned {
                attempts,
                poisoned_at_millis,
                diagnosis,
            } => f
                .debug_struct("Poisoned")
                .field("attempts", attempts)
                .field("poisoned_at_millis", poisoned_at_millis)
                .field("diagnosis", diagnosis)
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
            None => 1,
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

        let claim = DurableCorePostCasRecoveryClaim {
            target: request.target,
            token: Uuid::new_v4().to_string(),
            attempts,
            expires_at_millis,
        };
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::Active {
                token: claim.token.clone(),
                attempts,
                expires_at_millis,
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
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::Completed { attempts },
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
        let retry_after_millis = checked_duration_deadline(
            now_millis,
            backoff,
            "post-CAS recovery backoff duration overflow",
        )?;
        let mut guard = self.entries.write().await;
        let entry = active_entry_for_claim(&guard, claim, now_millis)?;
        let attempts = entry.attempts();
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::BackingOff {
                attempts,
                retry_after_millis,
                diagnosis: DurableCorePostCasRedactedDiagnosis::new(),
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
        guard.insert(
            claim.target.clone(),
            DurableCorePostCasRecoveryEntry::Poisoned {
                attempts,
                poisoned_at_millis: now_millis,
                diagnosis: DurableCorePostCasRedactedDiagnosis::new(),
            },
        );
        Ok(())
    }
}

impl DurableCorePostCasRecoveryEntry {
    const fn attempts(&self) -> u32 {
        match self {
            Self::Active { attempts, .. }
            | Self::BackingOff { attempts, .. }
            | Self::Completed { attempts }
            | Self::Poisoned { attempts, .. } => *attempts,
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
            token,
            expires_at_millis,
            ..
        }) if token == claim.token() && now_millis < *expires_at_millis => entries
            .get(claim.target())
            .ok_or_else(stale_post_cas_recovery_claim),
        _ => Err(VfsError::InvalidArgs {
            message: "post-CAS recovery claim is stale".to_string(),
        }),
    }
}

fn stale_post_cas_recovery_claim() -> VfsError {
    VfsError::InvalidArgs {
        message: "post-CAS recovery claim is stale".to_string(),
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

/// Redacted response wrapper for idempotency replay after the commit is visible.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableCoreCommittedResponse {
    status_code: u16,
    response_body: Value,
}

impl DurableCoreCommittedResponse {
    const PARTIAL_STATUS_CODE: u16 = 202;

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
            return self
                .partial_after_failure(
                    DurableCorePostCasStep::WorkspaceHeadUpdate,
                    completion,
                    idempotency,
                )
                .await;
        }
        completion.workspace_head_updated = true;

        if !completion.audit_appended && audit.append(self.audit_event.clone()).await.is_err() {
            return self
                .partial_after_failure(DurableCorePostCasStep::AuditAppend, completion, idempotency)
                .await;
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
                    return self
                        .partial_after_failure(
                            DurableCorePostCasStep::WorkspaceHeadUpdate,
                            completion,
                            idempotency,
                        )
                        .await;
                }
                completion.workspace_head_updated = true;
            }
            DurableCorePostCasStep::AuditAppend => {
                if audit.append(self.audit_event.clone()).await.is_err() {
                    return self
                        .partial_after_failure(
                            DurableCorePostCasStep::AuditAppend,
                            completion,
                            idempotency,
                        )
                        .await;
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

    async fn partial_after_failure(
        &self,
        failed_step: DurableCorePostCasStep,
        completion: DurableCoreCommitPostCasCompletion,
        idempotency: &dyn IdempotencyStore,
    ) -> DurableCorePostCasOutcome {
        let (idempotency_completion_attempted, idempotency_completed) =
            if completion.idempotency_completed {
                (false, true)
            } else if let Some(reservation) = &self.idempotency_reservation {
                let partial_response = DurableCoreCommittedResponse::partial();
                (
                    true,
                    idempotency
                        .complete(
                            reservation,
                            partial_response.status_code(),
                            partial_response.response_body().clone(),
                        )
                        .await
                        .is_ok(),
                )
            } else {
                (false, false)
            };

        DurableCorePostCasOutcome::Partial(DurableCorePostCasPartial {
            failed_step,
            completion,
            idempotency_completion_attempted,
            idempotency_completed,
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
        LocalMemoryRefStore, RefExpectation, RefStore, RefUpdate, RefVersion, RepoId,
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

        fn target(step: DurableCorePostCasStep) -> DurableCorePostCasRecoveryTarget {
            DurableCorePostCasRecoveryTarget::new(repo(), MAIN_REF, commit_id("post-cas"), step)
                .unwrap()
        }

        fn request(
            step: DurableCorePostCasStep,
            now_millis: u64,
        ) -> DurableCorePostCasRecoveryClaimRequest {
            DurableCorePostCasRecoveryClaimRequest::new(
                target(step),
                "worker-secret-token",
                Duration::from_secs(30),
                now_millis,
            )
            .unwrap()
        }

        #[tokio::test]
        async fn post_cas_recovery_claim_blocks_duplicate_active_worker() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let first = store
                .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 100))
                .await
                .unwrap()
                .expect("first worker should claim target");

            let duplicate = store
                .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 101))
                .await
                .unwrap();

            assert_eq!(first.attempts(), 1);
            assert!(duplicate.is_none());
        }

        #[tokio::test]
        async fn post_cas_recovery_failure_backs_off_and_retry_gets_new_token() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let first = store
                .claim(request(DurableCorePostCasStep::AuditAppend, 1_000))
                .await
                .unwrap()
                .expect("first worker should claim target");
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
            let first = store
                .claim(request(
                    DurableCorePostCasStep::IdempotencyCompletion,
                    2_000,
                ))
                .await
                .unwrap()
                .unwrap();
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
        async fn post_cas_recovery_completed_claim_is_terminal() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim = store
                .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 3_000))
                .await
                .unwrap()
                .unwrap();
            store.complete(&claim, 3_001).await.unwrap();

            assert!(
                store
                    .claim(request(DurableCorePostCasStep::WorkspaceHeadUpdate, 9_000))
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        #[tokio::test]
        async fn post_cas_recovery_expired_claim_cannot_complete_fail_or_poison() {
            let store = InMemoryDurableCorePostCasRecoveryClaimStore::new();
            let claim = store
                .claim(request(DurableCorePostCasStep::AuditAppend, 5_000))
                .await
                .unwrap()
                .unwrap();
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
            let claim = store
                .claim(request(DurableCorePostCasStep::AuditAppend, 4_000))
                .await
                .unwrap()
                .unwrap();

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
            let claim = store
                .claim(request(DurableCorePostCasStep::AuditAppend, 6_000))
                .await
                .unwrap()
                .unwrap();

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
        async fn post_cas_workspace_head_failure_returns_partial_and_completes_idempotency() {
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
                    ..
                })
            ));
            assert!(audit.list_recent(10).await.unwrap().is_empty());
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
        }

        #[tokio::test]
        async fn post_cas_audit_failure_returns_partial_and_completes_idempotency() {
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
