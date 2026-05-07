//! Durable core transaction semantics contract.
//!
//! This module is intentionally landed before the live durable `CoreDb`
//! implementation so the transaction policy is executable and reviewable first.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;

use crate::backend::{CommitRecord, CommitStore, ObjectStore, ObjectWrite, RefVersion, RepoId};
use crate::error::VfsError;
use crate::fs::VirtualFs;
use crate::fs::inode::{InodeId, InodeKind};
use crate::store::commit::CommitObject;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::change::{PathMap, diff_path_maps, worktree_path_records};
use crate::vcs::{ChangedPath, CommitId, MAIN_REF, PathRecord};

const DURABLE_CORE_COMMIT_EXECUTION_NOT_SUPPORTED: &str =
    "durable core commit execution is not supported until durable prerequisites are complete";

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

    use axum::http::HeaderValue;
    use serde_json::json;

    use super::*;
    use crate::backend::{
        LocalMemoryRefStore, RefExpectation, RefStore, RefUpdate, RefVersion, RepoId,
    };
    use crate::fs::VirtualFs;
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyStore, InMemoryIdempotencyStore,
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
