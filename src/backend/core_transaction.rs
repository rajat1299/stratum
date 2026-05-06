//! Durable core transaction semantics contract.
//!
//! This module is intentionally landed before the live durable `CoreDb`
//! implementation so the transaction policy is executable and reviewable first.
#![allow(dead_code)]

use crate::backend::RefVersion;
use crate::error::VfsError;
use crate::vcs::{CommitId, MAIN_REF};

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
    use axum::http::HeaderValue;
    use serde_json::json;

    use super::*;
    use crate::backend::{
        LocalMemoryRefStore, RefExpectation, RefStore, RefUpdate, RefVersion, RepoId,
    };
    use crate::idempotency::{
        IdempotencyBegin, IdempotencyKey, IdempotencyStore, InMemoryIdempotencyStore,
    };
    use crate::store::ObjectId;
    use crate::vcs::{CommitId, MAIN_REF, RefName};
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
