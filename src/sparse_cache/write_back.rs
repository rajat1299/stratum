use crate::error::VfsError;
use crate::sparse_cache::{
    DirtyEntryState, DirtyOperation, SparseCache, WritebackState, normalize_cache_path,
    object_id_from_hex, sparse_cache_error, to_i64, u64_from_i64,
};
use crate::store::ObjectId;
use crate::vcs::{CommitId, RefName};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

#[cfg(test)]
use crate::backend::core_transaction::{
    DurableCoreCommitObjectTreeWritePlan, DurableCoreCommitParentState,
    DurableCoreCommitSourceSnapshot, DurableCorePostCasStep, DurableCorePreVisibilityRecoveryStage,
    DurableCoreTransactionStep,
};
#[cfg(test)]
use crate::backend::durable_mutation::DURABLE_MUTATION_COMMIT_MESSAGE;
#[cfg(test)]
use crate::backend::durable_mutation::{
    DurableMutationEngine, DurableMutationInput, DurableMutationOperation, DurableMutationOutput,
};
#[cfg(test)]
use crate::backend::{CommitRecord, RefVersion};
#[cfg(test)]
use crate::backend::{RepoId, StratumStores};
#[cfg(test)]
use crate::store::ObjectKind;
#[cfg(test)]
use crate::vcs::MAIN_REF;

const WRITE_BACK_DISABLED: &str = "sparse write-back disabled";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SparseWriteBackMode {
    Disabled,
    EnabledForTests,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SparseWriteBackPlannerInput {
    pub view_id: i64,
    pub mode: SparseWriteBackMode,
    pub base_ref: RefName,
    pub session_ref: RefName,
    pub author: String,
    pub timestamp: u64,
}

impl fmt::Debug for SparseWriteBackPlannerInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackPlannerInput")
            .field("view_id", &self.view_id)
            .field("mode", &self.mode)
            .field("base_ref_present", &true)
            .field("session_ref_present", &true)
            .field("author_present", &!self.author.is_empty())
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SparseWriteBackFlushPlan {
    pub mode: SparseWriteBackMode,
    pub execution_allowed: bool,
    pub blocked_reason: Option<String>,
    pub queued_count: u64,
    pub intent_count: u64,
    pub changed_paths: Vec<String>,
    pub intents: Vec<SparseDurableMutationIntent>,
}

impl fmt::Debug for SparseWriteBackFlushPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackFlushPlan")
            .field("mode", &self.mode)
            .field("execution_allowed", &self.execution_allowed)
            .field("blocked_reason", &self.blocked_reason)
            .field("queued_count", &self.queued_count)
            .field("intent_count", &self.intent_count)
            .field("changed_path_count", &self.changed_paths.len())
            .field("changed_paths", &"<redacted>")
            .field("intents", &self.intents)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SparseDurableMutationIntent {
    pub queue_id: i64,
    pub dirty_id: i64,
    pub inode_id: u64,
    pub operation_id: String,
    pub fingerprint: String,
    pub base_ref: RefName,
    pub session_ref: RefName,
    pub operation: SparseDurableMutationOperationIntent,
    pub author: String,
    pub timestamp: u64,
    pub source_commit_id: Option<CommitId>,
    pub source_ref_name: Option<RefName>,
    pub source_ref_version: Option<u64>,
    pub queue_source_identity_present: bool,
    pub(crate) queued_operation_id: String,
    pub(crate) source_identity: String,
}

impl fmt::Debug for SparseDurableMutationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseDurableMutationIntent")
            .field("queue_id_present", &true)
            .field("dirty_id_present", &true)
            .field("inode_id_present", &true)
            .field("operation_id_present", &!self.operation_id.is_empty())
            .field("fingerprint_present", &!self.fingerprint.is_empty())
            .field("base_ref_present", &true)
            .field("session_ref_present", &true)
            .field("operation", &self.operation)
            .field("author_present", &!self.author.is_empty())
            .field("timestamp", &self.timestamp)
            .field("source_commit_id_present", &self.source_commit_id.is_some())
            .field("source_ref_name_present", &self.source_ref_name.is_some())
            .field("source_ref_version", &self.source_ref_version)
            .field(
                "queue_source_identity_present",
                &self.queue_source_identity_present,
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SparseDurableMutationOperationIntent {
    WriteFile {
        path: String,
        content_object_id: ObjectId,
        content_len: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        mime_type: Option<String>,
        custom_attrs: BTreeMap<String, String>,
    },
}

impl fmt::Debug for SparseDurableMutationOperationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteFile {
                path: _,
                content_object_id: _,
                content_len,
                mode,
                uid,
                gid,
                mime_type,
                custom_attrs,
            } => formatter
                .debug_struct("WriteFile")
                .field("path", &"<redacted>")
                .field("content_object_id", &"<redacted>")
                .field("content_len", content_len)
                .field("mode", mode)
                .field("uid", uid)
                .field("gid", gid)
                .field("mime_type_present", &mime_type.is_some())
                .field("custom_attr_count", &custom_attrs.len())
                .finish(),
        }
    }
}

pub fn plan_sparse_writeback_flush(
    cache: &SparseCache,
    input: SparseWriteBackPlannerInput,
) -> Result<SparseWriteBackFlushPlan, VfsError> {
    if input.mode == SparseWriteBackMode::Disabled {
        let summary = pending_writeback_summary(cache, input.view_id)?;
        return Ok(SparseWriteBackFlushPlan {
            mode: input.mode,
            execution_allowed: false,
            blocked_reason: Some(WRITE_BACK_DISABLED.to_string()),
            queued_count: summary.queued_count,
            intent_count: 0,
            changed_paths: summary.changed_paths,
            intents: Vec::new(),
        });
    }

    let rows = pending_writeback_rows(cache, input.view_id)?;
    let changed_paths = rows.iter().map(|row| row.path.clone()).collect::<Vec<_>>();
    let queued_count = rows.len() as u64;

    let mut intents = Vec::with_capacity(rows.len());
    for row in rows {
        intents.push(row.into_intent(&input)?);
    }

    Ok(SparseWriteBackFlushPlan {
        mode: input.mode,
        execution_allowed: true,
        blocked_reason: None,
        queued_count,
        intent_count: intents.len() as u64,
        changed_paths,
        intents,
    })
}

#[cfg(test)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SparseWriteBackFlushExecution {
    pub(crate) attempted_count: u64,
    pub(crate) flushed_count: u64,
    pub(crate) failed_count: u64,
    pub(crate) flushed: Vec<SparseWriteBackFlushedMutation>,
}

#[cfg(test)]
impl fmt::Debug for SparseWriteBackFlushExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackFlushExecution")
            .field("attempted_count", &self.attempted_count)
            .field("flushed_count", &self.flushed_count)
            .field("failed_count", &self.failed_count)
            .field("flushed", &self.flushed)
            .finish()
    }
}

#[cfg(test)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SparseWriteBackFlushedMutation {
    pub(crate) queue_id: i64,
    pub(crate) dirty_id: i64,
    pub(crate) operation_id: String,
    pub(crate) fingerprint: String,
    pub(crate) target: SparseWriteBackRecoveryTarget,
}

#[cfg(test)]
impl fmt::Debug for SparseWriteBackFlushedMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackFlushedMutation")
            .field("queue_id_present", &true)
            .field("dirty_id_present", &true)
            .field("operation_id_present", &!self.operation_id.is_empty())
            .field("fingerprint_present", &!self.fingerprint.is_empty())
            .field("target", &self.target)
            .finish()
    }
}

#[cfg(test)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SparseWriteBackRecoveryTarget {
    pub(crate) session_ref: RefName,
    pub(crate) session_ref_version: u64,
    pub(crate) previous_commit: CommitId,
    pub(crate) new_commit: CommitId,
    pub(crate) root_tree: ObjectId,
    pub(crate) changed_path_count: usize,
}

#[cfg(test)]
impl fmt::Debug for SparseWriteBackRecoveryTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackRecoveryTarget")
            .field("session_ref_present", &true)
            .field("session_ref_version", &self.session_ref_version)
            .field("previous_commit_present", &true)
            .field("new_commit_present", &true)
            .field("root_tree_present", &true)
            .field("changed_path_count", &self.changed_path_count)
            .finish()
    }
}

#[cfg(test)]
pub(crate) async fn execute_sparse_writeback_flush_for_tests(
    cache: &SparseCache,
    repo_id: &RepoId,
    stores: &StratumStores,
    plan: SparseWriteBackFlushPlan,
    now_unix_nanos: u64,
) -> Result<SparseWriteBackFlushExecution, VfsError> {
    if plan.mode == SparseWriteBackMode::Disabled || !plan.execution_allowed {
        return Err(VfsError::InvalidArgs {
            message: WRITE_BACK_DISABLED.to_string(),
        });
    }

    let engine = DurableMutationEngine::new(
        repo_id,
        stores.refs.as_ref(),
        stores.commits.as_ref(),
        stores.objects.as_ref(),
    )
    .with_cleanup_claims(stores.object_cleanup.as_ref());
    let mut flushed = Vec::new();
    for intent in &plan.intents {
        match execute_sparse_writeback_intent(
            cache,
            &engine,
            stores,
            repo_id,
            intent,
            now_unix_nanos,
        )
        .await
        {
            Ok(mutation) => flushed.push(mutation),
            Err(SparseWriteBackIntentExecutionError::PreVisible(error)) => {
                let _ = mark_writeback_failed(cache, intent, now_unix_nanos);
                tracing::debug!(
                    queue_id_present = true,
                    dirty_id_present = true,
                    "sparse write-back durable mutation failed"
                );
                return Err(redacted_writeback_execution_error(error));
            }
            Err(SparseWriteBackIntentExecutionError::PostVisible(error)) => {
                tracing::debug!(
                    queue_id_present = true,
                    dirty_id_present = true,
                    "sparse write-back post-visible bookkeeping failed"
                );
                return Err(redacted_writeback_post_visible_error(error));
            }
        }
    }

    Ok(SparseWriteBackFlushExecution {
        attempted_count: plan.intents.len() as u64,
        flushed_count: flushed.len() as u64,
        failed_count: 0,
        flushed,
    })
}

#[cfg(test)]
async fn execute_sparse_writeback_intent(
    cache: &SparseCache,
    engine: &DurableMutationEngine<'_>,
    stores: &StratumStores,
    repo_id: &RepoId,
    intent: &SparseDurableMutationIntent,
    now_unix_nanos: u64,
) -> Result<SparseWriteBackFlushedMutation, SparseWriteBackIntentExecutionError> {
    claim_writeback_pending(cache, intent, now_unix_nanos)
        .map_err(SparseWriteBackIntentExecutionError::PreVisible)?;
    let input = durable_mutation_input_for_intent(cache, intent)
        .map_err(SparseWriteBackIntentExecutionError::PreVisible)?;
    let output = engine
        .apply_with_test_policy(input)
        .await
        .map_err(SparseWriteBackIntentExecutionError::PreVisible)?;
    let target = recovery_target_from_output(intent, stores, repo_id, &output)
        .await
        .map_err(SparseWriteBackIntentExecutionError::PostVisible)?;
    mark_writeback_flushed(cache, intent, now_unix_nanos)
        .map_err(SparseWriteBackIntentExecutionError::PostVisible)?;
    Ok(SparseWriteBackFlushedMutation {
        queue_id: intent.queue_id,
        dirty_id: intent.dirty_id,
        operation_id: intent.operation_id.clone(),
        fingerprint: intent.fingerprint.clone(),
        target,
    })
}

#[cfg(test)]
enum SparseWriteBackIntentExecutionError {
    PreVisible(VfsError),
    PostVisible(VfsError),
}

#[cfg(test)]
fn claim_writeback_pending(
    cache: &SparseCache,
    intent: &SparseDurableMutationIntent,
    now_unix_nanos: u64,
) -> Result<(), VfsError> {
    let now = to_i64(now_unix_nanos)?;
    let (content_len, content_object_id) = intent_content_identity(intent)?;
    cache.run_immediate_transaction(|| {
        let dirty_matched = cache
            .connection
            .execute(
                "UPDATE sparse_cache_dirty_entries
                SET updated_at_unix_nanos = ?3,
                    last_error_code = NULL
                WHERE dirty_id = ?1
                  AND state = 'queued'
                  AND inode_id = ?2
                  AND content_len = ?4
                  AND content_object_id = ?5",
                rusqlite::params![
                    intent.dirty_id,
                    to_i64(intent.inode_id)?,
                    now,
                    content_len,
                    content_object_id
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        let queue_claimed = cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue
                SET state = 'running',
                    attempts = attempts + 1,
                    updated_at_unix_nanos = ?3,
                    next_run_at_unix_nanos = NULL,
                    last_error_code = NULL
                WHERE queue_id = ?1
                  AND dirty_id = ?2
                  AND state = 'pending'
                  AND operation_id = ?4
                  AND source_identity = ?5",
                rusqlite::params![
                    intent.queue_id,
                    intent.dirty_id,
                    now,
                    intent.queued_operation_id,
                    intent.source_identity
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        if dirty_matched != 1 || queue_claimed != 1 {
            return Err(sparse_cache_error());
        }
        Ok(())
    })
}

#[cfg(test)]
fn durable_mutation_input_for_intent(
    cache: &SparseCache,
    intent: &SparseDurableMutationIntent,
) -> Result<DurableMutationInput, VfsError> {
    let operation = match &intent.operation {
        SparseDurableMutationOperationIntent::WriteFile {
            path,
            content_object_id,
            content_len,
            mode,
            uid,
            gid,
            mime_type,
            custom_attrs,
        } => {
            let content = cache.dirty_file_bytes(intent.dirty_id)?;
            if ObjectId::from_bytes(&content) != *content_object_id
                || content.len() as u64 != *content_len
            {
                return Err(sparse_cache_error());
            }
            DurableMutationOperation::WriteFile {
                path: path.clone(),
                content,
                mode: *mode,
                uid: *uid,
                gid: *gid,
                mime_type: mime_type.clone(),
                custom_attrs: custom_attrs.clone(),
            }
        }
    };
    Ok(DurableMutationInput {
        base_ref: intent.base_ref.clone(),
        session_ref: intent.session_ref.clone(),
        operation,
        author: intent.author.clone(),
        timestamp: intent.timestamp,
        preflight_session: None,
    })
}

#[cfg(test)]
async fn recovery_target_from_output(
    intent: &SparseDurableMutationIntent,
    stores: &StratumStores,
    repo_id: &RepoId,
    output: &DurableMutationOutput,
) -> Result<SparseWriteBackRecoveryTarget, VfsError> {
    if output.response_metadata.session_ref != intent.session_ref
        || output.response_metadata.changed_path_count != output.changed_paths.len()
        || output.new_commit == output.previous_commit
        || !stores
            .objects
            .contains(
                repo_id,
                output.response_metadata.root_tree,
                ObjectKind::Tree,
            )
            .await?
    {
        return Err(sparse_cache_error());
    }
    Ok(SparseWriteBackRecoveryTarget {
        session_ref: output.response_metadata.session_ref.clone(),
        session_ref_version: output.response_metadata.session_ref_version.value(),
        previous_commit: output.previous_commit,
        new_commit: output.new_commit,
        root_tree: output.response_metadata.root_tree,
        changed_path_count: output.response_metadata.changed_path_count,
    })
}

#[cfg(test)]
fn mark_writeback_flushed(
    cache: &SparseCache,
    intent: &SparseDurableMutationIntent,
    now_unix_nanos: u64,
) -> Result<(), VfsError> {
    let now = to_i64(now_unix_nanos)?;
    let (content_len, content_object_id) = intent_content_identity(intent)?;
    cache.run_immediate_transaction(|| {
        let dirty_updated = cache
            .connection
            .execute(
                "UPDATE sparse_cache_dirty_entries
                SET state = 'flushed',
                    updated_at_unix_nanos = ?3,
                    last_error_code = NULL
                WHERE dirty_id = ?1
                  AND state = 'queued'
                  AND inode_id = ?2
                  AND content_len = ?4
                  AND content_object_id = ?5",
                rusqlite::params![
                    intent.dirty_id,
                    to_i64(intent.inode_id)?,
                    now,
                    content_len,
                    content_object_id
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        let queue_updated = cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue
                SET state = 'flushed',
                    updated_at_unix_nanos = ?3,
                    completed_at_unix_nanos = ?3,
                    last_error_code = NULL
                WHERE queue_id = ?1
                  AND dirty_id = ?2
                  AND state = 'running'
                  AND operation_id = ?4
                  AND source_identity = ?5",
                rusqlite::params![
                    intent.queue_id,
                    intent.dirty_id,
                    now,
                    intent.queued_operation_id,
                    intent.source_identity
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        if dirty_updated != 1 || queue_updated != 1 {
            return Err(sparse_cache_error());
        }
        Ok(())
    })
}

#[cfg(test)]
fn mark_writeback_failed(
    cache: &SparseCache,
    intent: &SparseDurableMutationIntent,
    now_unix_nanos: u64,
) -> Result<(), VfsError> {
    let now = to_i64(now_unix_nanos)?;
    let (content_len, content_object_id) = intent_content_identity(intent)?;
    cache.run_immediate_transaction(|| {
        let dirty_updated = cache
            .connection
            .execute(
                "UPDATE sparse_cache_dirty_entries
                SET state = 'failed',
                    updated_at_unix_nanos = ?2,
                    last_error_code = 'writeback_failed'
                WHERE dirty_id = ?1
                  AND state = 'queued'
                  AND inode_id = ?3
                  AND content_len = ?4
                  AND content_object_id = ?5",
                rusqlite::params![
                    intent.dirty_id,
                    now,
                    to_i64(intent.inode_id)?,
                    content_len,
                    content_object_id
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        let queue_updated = cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue
                SET state = 'failed',
                    updated_at_unix_nanos = ?3,
                    completed_at_unix_nanos = ?3,
                    last_error_code = 'writeback_failed'
                WHERE queue_id = ?1
                  AND dirty_id = ?2
                  AND state = 'running'
                  AND operation_id = ?4
                  AND source_identity = ?5",
                rusqlite::params![
                    intent.queue_id,
                    intent.dirty_id,
                    now,
                    intent.queued_operation_id,
                    intent.source_identity
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        if dirty_updated != 1 || queue_updated != 1 {
            return Err(sparse_cache_error());
        }
        Ok(())
    })
}

#[cfg(test)]
fn intent_content_identity(
    intent: &SparseDurableMutationIntent,
) -> Result<(i64, String), VfsError> {
    match &intent.operation {
        SparseDurableMutationOperationIntent::WriteFile {
            content_len,
            content_object_id,
            ..
        } => Ok((to_i64(*content_len)?, content_object_id.to_hex())),
    }
}

#[cfg(test)]
fn redacted_writeback_execution_error(_error: VfsError) -> VfsError {
    VfsError::CorruptStore {
        message: "sparse write-back durable mutation failed".to_string(),
    }
}

#[cfg(test)]
fn redacted_writeback_post_visible_error(_error: VfsError) -> VfsError {
    VfsError::CorruptStore {
        message: "sparse write-back post-visible bookkeeping failed".to_string(),
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SparseCommitDirtyQueueSummary {
    pub(crate) dirty_entries: u64,
    pub(crate) queued_dirty_entries: u64,
    pub(crate) flushed_dirty_entries: u64,
    pub(crate) failed_dirty_entries: u64,
    pub(crate) pending_queue_entries: u64,
    pub(crate) running_queue_entries: u64,
    pub(crate) flushed_queue_entries: u64,
    pub(crate) failed_queue_entries: u64,
    pub(crate) disabled_queue_entries: u64,
}

#[cfg(test)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SparseCommitStagingInput {
    pub(crate) mode: SparseWriteBackMode,
    pub(crate) repo_id: RepoId,
    pub(crate) target_ref: RefName,
    pub(crate) target_commit_id: CommitId,
    pub(crate) target_ref_version: u64,
    pub(crate) session_ref: RefName,
    pub(crate) session_commit_id: CommitId,
    pub(crate) session_ref_version: u64,
    pub(crate) session_root_tree_id: ObjectId,
    pub(crate) dirty_queue_summary: SparseCommitDirtyQueueSummary,
}

#[cfg(test)]
impl fmt::Debug for SparseCommitStagingInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseCommitStagingInput")
            .field("mode", &self.mode)
            .field("repo_id_present", &true)
            .field("target_ref_present", &true)
            .field("target_commit_id_present", &true)
            .field("target_ref_version", &self.target_ref_version)
            .field("session_ref_present", &true)
            .field("session_commit_id_present", &true)
            .field("session_ref_version", &self.session_ref_version)
            .field("session_root_tree_id_present", &true)
            .field("dirty_queue_summary", &self.dirty_queue_summary)
            .finish()
    }
}

#[cfg(test)]
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SparseCommitStagingPlan {
    pub(crate) promotion_allowed: bool,
    pub(crate) repo_id_present: bool,
    pub(crate) target_ref_present: bool,
    pub(crate) session_ref_present: bool,
    pub(crate) target_ref_version: u64,
    pub(crate) session_ref_version: u64,
    pub(crate) expected_visible_ref_version: u64,
    pub(crate) changed_path_count: usize,
    pub(crate) planned_object_count: usize,
    pub(crate) visibility_step: DurableCoreTransactionStep,
    pub(crate) ordered_write_path: Vec<DurableCoreTransactionStep>,
    pub(crate) pre_visibility_recovery_stages: Vec<DurableCorePreVisibilityRecoveryStage>,
    pub(crate) post_cas_recovery_steps: Vec<DurableCorePostCasStep>,
    pub(crate) redacted_state_message: String,
}

#[cfg(test)]
impl fmt::Debug for SparseCommitStagingPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseCommitStagingPlan")
            .field("promotion_allowed", &self.promotion_allowed)
            .field("repo_id_present", &self.repo_id_present)
            .field("target_ref_present", &self.target_ref_present)
            .field("session_ref_present", &self.session_ref_present)
            .field("target_ref_version", &self.target_ref_version)
            .field("session_ref_version", &self.session_ref_version)
            .field(
                "expected_visible_ref_version",
                &self.expected_visible_ref_version,
            )
            .field("changed_path_count", &self.changed_path_count)
            .field("planned_object_count", &self.planned_object_count)
            .field("visibility_step", &self.visibility_step)
            .field("ordered_write_path", &self.ordered_write_path)
            .field(
                "pre_visibility_recovery_stages",
                &self.pre_visibility_recovery_stages,
            )
            .field("post_cas_recovery_steps", &self.post_cas_recovery_steps)
            .field(
                "redacted_state_present",
                &!self.redacted_state_message.is_empty(),
            )
            .finish()
    }
}

#[cfg(test)]
pub(crate) fn sparse_commit_dirty_queue_summary(
    cache: &SparseCache,
    view_id: i64,
) -> Result<SparseCommitDirtyQueueSummary, VfsError> {
    Ok(SparseCommitDirtyQueueSummary {
        dirty_entries: count_dirty_entries(cache, view_id, "dirty")?,
        queued_dirty_entries: count_dirty_entries(cache, view_id, "queued")?,
        flushed_dirty_entries: count_dirty_entries(cache, view_id, "flushed")?,
        failed_dirty_entries: count_dirty_entries(cache, view_id, "failed")?,
        pending_queue_entries: count_writeback_entries(cache, view_id, "pending")?,
        running_queue_entries: count_writeback_entries(cache, view_id, "running")?,
        flushed_queue_entries: count_writeback_entries(cache, view_id, "flushed")?,
        failed_queue_entries: count_writeback_entries(cache, view_id, "failed")?,
        disabled_queue_entries: count_writeback_entries(cache, view_id, "disabled")?,
    })
}

#[cfg(test)]
pub(crate) async fn stage_sparse_commit_for_tests(
    stores: &StratumStores,
    input: SparseCommitStagingInput,
) -> Result<SparseCommitStagingPlan, VfsError> {
    validate_sparse_commit_staging_input(&input)?;
    validate_commit_staging_refs(stores, &input).await?;
    validate_session_descends_from_source(stores, &input).await?;

    let source = DurableCoreCommitSourceSnapshot::from_durable_parent_state(
        &input.repo_id,
        DurableCoreCommitParentState::Existing {
            target: input.target_commit_id,
            version: RefVersion::new(input.target_ref_version)
                .map_err(|_| sparse_commit_staging_error())?,
        },
        stores.commits.as_ref(),
        stores.objects.as_ref(),
    )
    .await
    .map_err(|_| sparse_commit_staging_error())?;
    let write_plan = DurableCoreCommitObjectTreeWritePlan::build_from_durable_root_tree(
        &input.repo_id,
        source,
        input.session_root_tree_id,
        stores.objects.as_ref(),
    )
    .await
    .map_err(|_| sparse_commit_staging_error())?;
    let expected_visible_ref_version = input
        .target_ref_version
        .checked_add(1)
        .and_then(|version| RefVersion::new(version).ok())
        .map(RefVersion::value)
        .ok_or_else(sparse_commit_staging_error)?;

    Ok(SparseCommitStagingPlan {
        promotion_allowed: true,
        repo_id_present: true,
        target_ref_present: true,
        session_ref_present: true,
        target_ref_version: input.target_ref_version,
        session_ref_version: input.session_ref_version,
        expected_visible_ref_version,
        changed_path_count: write_plan.changed_paths().len(),
        planned_object_count: write_plan.planned_objects().len(),
        visibility_step: DurableCoreTransactionStep::RefCompareAndSwap,
        ordered_write_path: write_plan.ordered_write_path().to_vec(),
        pre_visibility_recovery_stages: vec![
            DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
            DurableCorePreVisibilityRecoveryStage::RefVisibilityCas,
        ],
        post_cas_recovery_steps: vec![
            DurableCorePostCasStep::WorkspaceHeadUpdate,
            DurableCorePostCasStep::AuditAppend,
            DurableCorePostCasStep::IdempotencyCompletion,
        ],
        redacted_state_message: "sparse commit staging uses durable ref CAS visibility".to_string(),
    })
}

#[cfg(test)]
fn validate_sparse_commit_staging_input(input: &SparseCommitStagingInput) -> Result<(), VfsError> {
    if input.mode == SparseWriteBackMode::Disabled {
        return Err(sparse_commit_staging_error());
    }
    if input.target_ref.as_str() != MAIN_REF {
        return Err(sparse_commit_staging_error());
    }
    let summary = input.dirty_queue_summary;
    if summary.dirty_entries != 0
        || summary.queued_dirty_entries != 0
        || summary.failed_dirty_entries != 0
        || summary.pending_queue_entries != 0
        || summary.running_queue_entries != 0
        || summary.failed_queue_entries != 0
        || summary.disabled_queue_entries != 0
    {
        return Err(sparse_commit_staging_error());
    }
    Ok(())
}

#[cfg(test)]
async fn validate_commit_staging_refs(
    stores: &StratumStores,
    input: &SparseCommitStagingInput,
) -> Result<(), VfsError> {
    let target = stores
        .refs
        .get(&input.repo_id, &input.target_ref)
        .await
        .map_err(|_| sparse_commit_staging_error())?
        .ok_or_else(sparse_commit_staging_error)?;
    if target.target != input.target_commit_id
        || target.version
            != RefVersion::new(input.target_ref_version)
                .map_err(|_| sparse_commit_staging_error())?
    {
        return Err(sparse_commit_staging_error());
    }
    let session = stores
        .refs
        .get(&input.repo_id, &input.session_ref)
        .await
        .map_err(|_| sparse_commit_staging_error())?
        .ok_or_else(sparse_commit_staging_error)?;
    if session.target != input.session_commit_id
        || session.version
            != RefVersion::new(input.session_ref_version)
                .map_err(|_| sparse_commit_staging_error())?
    {
        return Err(sparse_commit_staging_error());
    }
    Ok(())
}

#[cfg(test)]
async fn validate_session_descends_from_source(
    stores: &StratumStores,
    input: &SparseCommitStagingInput,
) -> Result<(), VfsError> {
    let expected_base_commit =
        commit_for_staging(stores, &input.repo_id, input.target_commit_id).await?;
    let mut current = input.session_commit_id;
    for _ in 0..1024 {
        let commit = commit_for_staging(stores, &input.repo_id, current).await?;
        if commit.root_tree != input.session_root_tree_id && current == input.session_commit_id {
            return Err(sparse_commit_staging_error());
        }
        if current == input.target_commit_id {
            return Ok(());
        }
        if commit.message != DURABLE_MUTATION_COMMIT_MESSAGE {
            return Err(sparse_commit_staging_error());
        }
        if staging_session_matches_previous_promotion(&commit, &expected_base_commit) {
            return Ok(());
        }
        let [parent] = commit.parents.as_slice() else {
            return Err(sparse_commit_staging_error());
        };
        current = *parent;
    }
    Err(sparse_commit_staging_error())
}

#[cfg(test)]
async fn commit_for_staging(
    stores: &StratumStores,
    repo_id: &RepoId,
    commit_id: CommitId,
) -> Result<CommitRecord, VfsError> {
    let commit = stores
        .commits
        .get(repo_id, commit_id)
        .await
        .map_err(|_| sparse_commit_staging_error())?
        .ok_or_else(sparse_commit_staging_error)?;
    if commit.repo_id != *repo_id || commit.id != commit_id {
        return Err(sparse_commit_staging_error());
    }
    Ok(commit)
}

#[cfg(test)]
fn staging_session_matches_previous_promotion(
    session_commit: &CommitRecord,
    expected_base_commit: &CommitRecord,
) -> bool {
    !expected_base_commit.parents.is_empty()
        && session_commit.root_tree == expected_base_commit.root_tree
        && session_commit.parents == expected_base_commit.parents
}

#[cfg(test)]
fn count_dirty_entries(cache: &SparseCache, view_id: i64, state: &str) -> Result<u64, VfsError> {
    let count = cache
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sparse_cache_dirty_entries WHERE view_id = ?1 AND state = ?2",
            rusqlite::params![view_id, state],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|_| sparse_cache_error())?;
    u64_from_i64(count)
}

#[cfg(test)]
fn count_writeback_entries(
    cache: &SparseCache,
    view_id: i64,
    state: &str,
) -> Result<u64, VfsError> {
    let count = cache
        .connection
        .query_row(
            "SELECT COUNT(*)
            FROM sparse_cache_writeback_queue q
            INNER JOIN sparse_cache_dirty_entries d ON d.dirty_id = q.dirty_id
            WHERE d.view_id = ?1 AND q.state = ?2",
            rusqlite::params![view_id, state],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|_| sparse_cache_error())?;
    u64_from_i64(count)
}

#[cfg(test)]
fn sparse_commit_staging_error() -> VfsError {
    VfsError::InvalidArgs {
        message: "sparse commit staging blocked".to_string(),
    }
}

#[derive(Clone)]
struct PendingWritebackRow {
    queue_id: i64,
    dirty_id: i64,
    view_id: i64,
    inode_id: u64,
    path: String,
    operation: DirtyOperation,
    dirty_state: DirtyEntryState,
    queue_state: WritebackState,
    source_commit_id: Option<CommitId>,
    source_ref_name: Option<RefName>,
    source_ref_version: Option<u64>,
    content_len: u64,
    content_object_id: ObjectId,
    mode: u32,
    uid: u32,
    gid: u32,
    mime_type: Option<String>,
    custom_attrs: BTreeMap<String, String>,
    queued_operation_id: String,
    source_identity: String,
    source_identity_present: bool,
}

impl PendingWritebackRow {
    fn into_intent(
        self,
        input: &SparseWriteBackPlannerInput,
    ) -> Result<SparseDurableMutationIntent, VfsError> {
        let normalized_path = normalize_cache_path(&self.path)?;
        let custom_attrs_identity = canonical_custom_attrs(&self.custom_attrs);
        let source_commit =
            source_commit_hex(self.source_commit_id).unwrap_or_else(|| "none".to_string());
        let source_ref_name = self
            .source_ref_name
            .as_ref()
            .map(|name| name.as_str().to_string())
            .unwrap_or_else(|| "none".to_string());
        let source_ref_version = self
            .source_ref_version
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "none".to_string());
        let content_object_id = self.content_object_id.to_hex();
        let operation_id = stable_digest([
            "sparse-writeback-operation".to_string(),
            self.view_id.to_string(),
            self.queue_id.to_string(),
            self.dirty_id.to_string(),
            self.inode_id.to_string(),
            digest_part(input.base_ref.as_str()),
            digest_part(input.session_ref.as_str()),
            digest_part(&input.author),
            input.timestamp.to_string(),
            digest_part(&self.queued_operation_id),
            digest_part(&self.source_identity),
            digest_part(&normalized_path),
            self.content_len.to_string(),
            content_object_id.clone(),
            self.mode.to_string(),
            self.uid.to_string(),
            self.gid.to_string(),
            digest_part(self.mime_type.as_deref().unwrap_or("none")),
            digest_part(&custom_attrs_identity),
            source_commit.clone(),
            digest_part(&source_ref_name),
            source_ref_version.clone(),
        ]);
        let fingerprint = stable_digest([
            "sparse-writeback-fingerprint".to_string(),
            operation_id.clone(),
            digest_part(input.base_ref.as_str()),
            digest_part(input.session_ref.as_str()),
            digest_part(&input.author),
            input.timestamp.to_string(),
            digest_part(&self.queued_operation_id),
            digest_part(&self.source_identity),
            digest_part(&normalized_path),
            content_object_id,
            self.content_len.to_string(),
            self.mode.to_string(),
            self.uid.to_string(),
            self.gid.to_string(),
            digest_part(self.mime_type.as_deref().unwrap_or("none")),
            digest_part(&custom_attrs_identity),
            source_commit,
            digest_part(&source_ref_name),
            source_ref_version,
        ]);

        Ok(SparseDurableMutationIntent {
            queue_id: self.queue_id,
            dirty_id: self.dirty_id,
            inode_id: self.inode_id,
            operation_id,
            fingerprint,
            base_ref: input.base_ref.clone(),
            session_ref: input.session_ref.clone(),
            operation: match self.operation {
                DirtyOperation::WriteFile => SparseDurableMutationOperationIntent::WriteFile {
                    path: normalized_path,
                    content_object_id: self.content_object_id,
                    content_len: self.content_len,
                    mode: u16::try_from(self.mode).map_err(|_| sparse_cache_error())?,
                    uid: self.uid,
                    gid: self.gid,
                    mime_type: self.mime_type,
                    custom_attrs: self.custom_attrs,
                },
            },
            author: input.author.clone(),
            timestamp: input.timestamp,
            source_commit_id: self.source_commit_id,
            source_ref_name: self.source_ref_name,
            source_ref_version: self.source_ref_version,
            queue_source_identity_present: self.source_identity_present,
            queued_operation_id: self.queued_operation_id,
            source_identity: self.source_identity,
        })
    }
}

struct PendingWritebackSummary {
    queued_count: u64,
    changed_paths: Vec<String>,
}

fn pending_writeback_summary(
    cache: &SparseCache,
    view_id: i64,
) -> Result<PendingWritebackSummary, VfsError> {
    let mut statement = cache
        .connection
        .prepare(
            "SELECT d.path
            FROM sparse_cache_writeback_queue q
            INNER JOIN sparse_cache_dirty_entries d ON d.dirty_id = q.dirty_id
            WHERE d.view_id = ?1
              AND d.state = 'queued'
              AND q.state = 'pending'
            ORDER BY q.queue_id",
        )
        .map_err(|_| sparse_cache_error())?;
    let changed_paths = statement
        .query_map([view_id], |row| row.get::<_, String>(0))
        .map_err(|_| sparse_cache_error())?
        .map(|path| {
            path.map_err(|_| sparse_cache_error())
                .and_then(|path| normalize_cache_path(&path))
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    Ok(PendingWritebackSummary {
        queued_count: changed_paths.len() as u64,
        changed_paths,
    })
}

fn pending_writeback_rows(
    cache: &SparseCache,
    view_id: i64,
) -> Result<Vec<PendingWritebackRow>, VfsError> {
    let mut statement = cache
        .connection
        .prepare(
            "SELECT q.queue_id,
                    d.dirty_id,
                    d.view_id,
                    d.inode_id,
                    d.path,
                    d.operation,
                    d.state,
                    q.state,
                    d.base_commit_id,
                    d.base_ref_name,
                    d.base_ref_version,
                    d.content_len,
                    d.content_object_id,
                    i.mode,
                    i.uid,
                    i.gid,
                    i.mime_type,
                    i.custom_attrs_json,
                    q.operation_id,
                    q.source_identity
            FROM sparse_cache_writeback_queue q
            INNER JOIN sparse_cache_dirty_entries d ON d.dirty_id = q.dirty_id
            INNER JOIN sparse_cache_inodes i
                ON i.view_id = d.view_id AND i.inode_id = d.inode_id
            WHERE d.view_id = ?1
              AND d.state = 'queued'
              AND q.state = 'pending'
            ORDER BY q.queue_id",
        )
        .map_err(|_| sparse_cache_error())?;
    let rows = statement
        .query_map([view_id], pending_writeback_row_from_sql)
        .map_err(|_| sparse_cache_error())?
        .map(|row| {
            row.map_err(|_| sparse_cache_error())
                .and_then(|row| row)
                .and_then(validate_pending_row)
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    Ok(rows)
}

fn pending_writeback_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<PendingWritebackRow, VfsError>> {
    let operation: String = row.get(5)?;
    let dirty_state: String = row.get(6)?;
    let queue_state: String = row.get(7)?;
    let commit_id: Option<String> = row.get(8)?;
    let ref_name: Option<String> = row.get(9)?;
    let ref_version: Option<i64> = row.get(10)?;
    let custom_attrs_json: String = row.get(17)?;
    let queued_operation_id: String = row.get(18)?;
    let source_identity: String = row.get(19)?;

    Ok((|| {
        Ok(PendingWritebackRow {
            queue_id: row.get(0).map_err(|_| sparse_cache_error())?,
            dirty_id: row.get(1).map_err(|_| sparse_cache_error())?,
            view_id: row.get(2).map_err(|_| sparse_cache_error())?,
            inode_id: u64_from_i64(row.get(3).map_err(|_| sparse_cache_error())?)?,
            path: row.get(4).map_err(|_| sparse_cache_error())?,
            operation: dirty_operation_from_sql(&operation)?,
            dirty_state: dirty_state_from_sql(&dirty_state)?,
            queue_state: writeback_state_from_sql(&queue_state)?,
            source_commit_id: commit_id
                .as_deref()
                .map(object_id_from_hex)
                .transpose()?
                .map(CommitId::from),
            source_ref_name: ref_name
                .as_deref()
                .map(|value| RefName::new(value).map_err(|_| sparse_cache_error()))
                .transpose()?,
            source_ref_version: ref_version.map(u64_from_i64).transpose()?,
            content_len: u64_from_i64(row.get(11).map_err(|_| sparse_cache_error())?)?,
            content_object_id: object_id_from_hex(
                &row.get::<_, String>(12).map_err(|_| sparse_cache_error())?,
            )?,
            mode: u32::try_from(row.get::<_, i64>(13).map_err(|_| sparse_cache_error())?)
                .map_err(|_| sparse_cache_error())?,
            uid: u32::try_from(row.get::<_, i64>(14).map_err(|_| sparse_cache_error())?)
                .map_err(|_| sparse_cache_error())?,
            gid: u32::try_from(row.get::<_, i64>(15).map_err(|_| sparse_cache_error())?)
                .map_err(|_| sparse_cache_error())?,
            mime_type: row.get(16).map_err(|_| sparse_cache_error())?,
            custom_attrs: serde_json::from_str(&custom_attrs_json)
                .map_err(|_| sparse_cache_error())?,
            queued_operation_id,
            source_identity: source_identity.clone(),
            source_identity_present: !source_identity.is_empty(),
        })
    })())
}

fn validate_pending_row(mut row: PendingWritebackRow) -> Result<PendingWritebackRow, VfsError> {
    if row.dirty_state != DirtyEntryState::Queued || row.queue_state != WritebackState::Pending {
        return Err(sparse_cache_error());
    }
    if !matches!(row.operation, DirtyOperation::WriteFile) {
        return Err(sparse_cache_error());
    }
    row.path = normalize_cache_path(&row.path)?;
    if row.mode > u32::from(u16::MAX) {
        return Err(sparse_cache_error());
    }
    let _ = to_i64(row.content_len)?;
    Ok(row)
}

fn dirty_operation_from_sql(value: &str) -> Result<DirtyOperation, VfsError> {
    match value {
        "write_file" => Ok(DirtyOperation::WriteFile),
        _ => Err(sparse_cache_error()),
    }
}

fn dirty_state_from_sql(value: &str) -> Result<DirtyEntryState, VfsError> {
    match value {
        "dirty" => Ok(DirtyEntryState::Dirty),
        "queued" => Ok(DirtyEntryState::Queued),
        "flushed" => Ok(DirtyEntryState::Flushed),
        "failed" => Ok(DirtyEntryState::Failed),
        _ => Err(sparse_cache_error()),
    }
}

fn writeback_state_from_sql(value: &str) -> Result<WritebackState, VfsError> {
    match value {
        "pending" => Ok(WritebackState::Pending),
        "running" => Ok(WritebackState::Running),
        "flushed" => Ok(WritebackState::Flushed),
        "failed" => Ok(WritebackState::Failed),
        "disabled" => Ok(WritebackState::Disabled),
        _ => Err(sparse_cache_error()),
    }
}

fn source_commit_hex(commit_id: Option<CommitId>) -> Option<String> {
    commit_id.map(CommitId::to_hex)
}

fn digest_part(value: &str) -> String {
    stable_digest(["sparse-writeback-part".to_string(), value.to_string()])
}

fn canonical_custom_attrs(custom_attrs: &BTreeMap<String, String>) -> String {
    custom_attrs
        .iter()
        .map(|(key, value)| format!("{}:{}={}", key.len(), key, digest_part(value)))
        .collect::<Vec<_>>()
        .join(";")
}

fn stable_digest(parts: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        let part = part.as_ref();
        hasher.update(part.len().to_string().as_bytes());
        hasher.update(b":");
        hasher.update(part.as_bytes());
        hasher.update(b";");
    }
    let digest = hasher.finalize();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::backend::{
        CommitRecord, ObjectStore, ObjectWrite, RefExpectation, RefRecord, RefStore, RefUpdate,
        RepoId, SourceCheckedRefUpdate, StoredObject, StratumStores,
    };
    use crate::error::VfsError;
    use crate::sparse_cache::{
        CacheViewIdentity, CachedInode, CachedNodeKind, SparseCache, WritebackState,
    };
    use crate::store::tree::TreeObject;
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, RefName};
    use std::collections::BTreeMap;

    #[test]
    fn disabled_writeback_mode_blocks_planning_without_claiming_queue() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"disabled base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/secret/disabled.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"disabled local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-secret-operation",
            "queued-secret-source",
            WritebackState::Pending,
            110,
        )?;

        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::Disabled,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        assert!(!plan.execution_allowed);
        assert_eq!(
            plan.blocked_reason.as_deref(),
            Some("sparse write-back disabled")
        );
        assert_eq!(plan.queued_count, 1);
        assert_eq!(plan.intent_count, 0);
        assert!(plan.intents.is_empty());
        assert_eq!(plan.changed_paths, vec!["/secret/disabled.txt"]);
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);
        assert_eq!(
            cache.dirty_entry_for_inode(view_id, 2)?.unwrap().state,
            crate::sparse_cache::DirtyEntryState::Queued
        );

        Ok(())
    }

    #[test]
    fn enabled_test_mode_builds_durable_write_file_intent_from_dirty_entry() -> Result<(), VfsError>
    {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"enabled base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "src/../README.md",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"enabled local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-secret-operation",
            "queued-secret-source",
            WritebackState::Pending,
            110,
        )?;

        let input = SparseWriteBackPlannerInput {
            view_id,
            mode: SparseWriteBackMode::EnabledForTests,
            base_ref: RefName::new("main")?,
            session_ref: RefName::new("agent/test/session")?,
            author: "test-author".to_string(),
            timestamp: 120,
        };
        let plan = plan_sparse_writeback_flush(&cache, input.clone())?;
        let repeat = plan_sparse_writeback_flush(&cache, input)?;

        assert!(plan.execution_allowed);
        assert_eq!(plan.queued_count, 1);
        assert_eq!(plan.intent_count, 1);
        assert_eq!(plan.changed_paths, vec!["/README.md"]);
        assert_eq!(plan.intents, repeat.intents);

        let intent = &plan.intents[0];
        assert_eq!(intent.queue_id, 1);
        assert_eq!(intent.dirty_id, dirty.dirty_id);
        assert_eq!(intent.inode_id, 2);
        assert_eq!(intent.base_ref, RefName::new("main")?);
        assert_eq!(intent.session_ref, RefName::new("agent/test/session")?);
        assert_eq!(intent.author, "test-author");
        assert_eq!(intent.timestamp, 120);
        assert_eq!(intent.source_commit_id, cache_view_identity().commit_id);
        assert_eq!(intent.source_ref_name, cache_view_identity().ref_name);
        assert_eq!(intent.source_ref_version, cache_view_identity().ref_version);
        assert_eq!(intent.queue_source_identity_present, true);
        assert_eq!(intent.operation_id.len(), 64);
        assert_eq!(intent.fingerprint.len(), 64);
        assert_eq!(
            intent.operation,
            SparseDurableMutationOperationIntent::WriteFile {
                path: "/README.md".to_string(),
                content_object_id: object_id(b"enabled local bytes"),
                content_len: 19,
                mode: 0o100644,
                uid: 501,
                gid: 20,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            }
        );

        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[test]
    fn intent_identity_changes_with_durable_operation_inputs() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"identity base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/identity.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"identity local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-identity-operation",
            "queued-identity-source",
            WritebackState::Pending,
            110,
        )?;
        let input = SparseWriteBackPlannerInput {
            view_id,
            mode: SparseWriteBackMode::EnabledForTests,
            base_ref: RefName::new("main")?,
            session_ref: RefName::new("agent/test/session")?,
            author: "test-author".to_string(),
            timestamp: 120,
        };
        let baseline = plan_sparse_writeback_flush(&cache, input.clone())?.intents[0].clone();

        let different_session = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                session_ref: RefName::new("agent/test/other-session")?,
                ..input.clone()
            },
        )?
        .intents[0]
            .clone();
        assert_ne!(different_session.operation_id, baseline.operation_id);
        assert_ne!(different_session.fingerprint, baseline.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue SET operation_id = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["queued-identity-operation-2", dirty.dirty_id],
            )
            .expect("change queued operation id");
        let queued_operation =
            plan_sparse_writeback_flush(&cache, input.clone())?.intents[0].clone();
        assert_ne!(queued_operation.operation_id, baseline.operation_id);
        assert_ne!(queued_operation.fingerprint, baseline.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue SET source_identity = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["queued-identity-source-2", dirty.dirty_id],
            )
            .expect("change queued source identity");
        let source = plan_sparse_writeback_flush(&cache, input.clone())?.intents[0].clone();
        assert_ne!(source.operation_id, queued_operation.operation_id);
        assert_ne!(source.fingerprint, queued_operation.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_dirty_entries SET path = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["/nested/../identity-renamed.txt", dirty.dirty_id],
            )
            .expect("change queued path");
        let path_plan = plan_sparse_writeback_flush(&cache, input.clone())?;
        let path = path_plan.intents[0].clone();
        assert_eq!(path_plan.changed_paths, vec!["/identity-renamed.txt"]);
        assert_ne!(path.operation_id, source.operation_id);
        assert_ne!(path.fingerprint, source.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_inodes SET mode = ?1, uid = ?2 WHERE view_id = ?3 AND inode_id = ?4",
                rusqlite::params![0o100600_i64, 502_i64, view_id, 2_i64],
            )
            .expect("change queued metadata");
        let metadata = plan_sparse_writeback_flush(&cache, input)?.intents[0].clone();
        assert_ne!(metadata.operation_id, path.operation_id);
        assert_ne!(metadata.fingerprint, path.fingerprint);

        Ok(())
    }

    #[test]
    fn enabled_test_mode_rejects_corrupt_queued_metadata_without_lossy_defaults()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"corrupt metadata base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/corrupt.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"corrupt local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-corrupt-operation",
            "queued-corrupt-source",
            WritebackState::Pending,
            110,
        )?;
        cache
            .connection
            .execute(
                "UPDATE sparse_cache_inodes SET custom_attrs_json = '{' WHERE view_id = ?1 AND inode_id = ?2",
                rusqlite::params![view_id, 2_i64],
            )
            .expect("corrupt queued inode metadata");

        let error = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )
        .expect_err("corrupt queued metadata should reject planning");

        assert!(matches!(error, VfsError::CorruptStore { .. }));
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[test]
    fn disabled_mode_blocks_even_when_execution_metadata_is_corrupt() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"disabled corrupt metadata base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/disabled-corrupt.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"disabled corrupt local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-disabled-corrupt-operation",
            "queued-disabled-corrupt-source",
            WritebackState::Pending,
            110,
        )?;
        cache
            .connection
            .execute(
                "UPDATE sparse_cache_inodes SET custom_attrs_json = '{' WHERE view_id = ?1 AND inode_id = ?2",
                rusqlite::params![view_id, 2_i64],
            )
            .expect("corrupt queued inode metadata");

        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::Disabled,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        assert!(!plan.execution_allowed);
        assert_eq!(plan.queued_count, 1);
        assert_eq!(plan.intent_count, 0);
        assert!(plan.intents.is_empty());
        assert_eq!(plan.changed_paths, vec!["/disabled-corrupt.txt"]);
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[test]
    fn flush_plan_debug_redacts_paths_repo_ids_object_ids_and_content() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: RepoId::new("secret_repo")?,
            root_tree_id: object_id(b"secret root tree"),
            commit_id: Some(CommitId::from(object_id(b"secret commit"))),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(7),
        })?;
        let base_object_id = object_id(b"secret base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/secret/path.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"secret local content",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "secret-operation-id",
            "secret-source-identity",
            WritebackState::Pending,
            110,
        )?;

        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "secret-author".to_string(),
                timestamp: 120,
            },
        )?;
        let debug = format!("{plan:?}");

        assert!(debug.contains("SparseWriteBackFlushPlan"));
        assert!(debug.contains("changed_path_count"));
        for secret in [
            "secret",
            "/secret/path.txt",
            "secret_repo",
            &base_object_id.to_hex(),
            &object_id(b"secret local content").to_hex(),
            "secret-operation-id",
            "secret-source-identity",
            "secret-author",
        ] {
            assert!(!debug.contains(secret), "debug leaked {secret}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn enabled_test_flush_advances_session_ref_through_durable_mutation_engine()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let base_commit = seed_empty_base(&stores, &repo_id).await?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: base_root_tree(&stores, &repo_id, base_commit).await?,
            commit_id: Some(base_commit),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(1),
        })?;
        let base_object_id = object_id(b"flush base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/notes.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"durable sparse note\n",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-flush-operation",
            "queued-flush-source",
            WritebackState::Pending,
            110,
        )?;
        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        let execution =
            execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &stores, plan, 130).await?;

        assert_eq!(execution.attempted_count, 1);
        assert_eq!(execution.flushed_count, 1);
        assert_eq!(execution.failed_count, 0);
        let target = &execution.flushed[0].target;
        assert_eq!(target.changed_path_count, 1);
        assert_eq!(target.previous_commit, base_commit);
        assert_ne!(target.new_commit, base_commit);
        assert_eq!(target.session_ref, RefName::new("agent/test/session")?);
        let session = stores
            .refs
            .get(&repo_id, &RefName::new("agent/test/session")?)
            .await?
            .expect("session ref should be advanced");
        assert_eq!(session.target, target.new_commit);
        assert_eq!(cache.writeback_progress(view_id)?.flushed, 1);
        assert_eq!(
            cache.dirty_entry_for_inode(view_id, 2)?.unwrap().state,
            crate::sparse_cache::DirtyEntryState::Flushed
        );

        Ok(())
    }

    #[tokio::test]
    async fn stale_session_ref_cas_leaves_dirty_entry_unflushed_and_redacted()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let base_commit = seed_empty_base(&stores, &repo_id).await?;
        let racing_commit =
            insert_empty_child_commit(&stores, &repo_id, base_commit, "racer").await?;
        let racing_refs = Arc::new(RacingSessionRefStore {
            inner: stores.refs.clone(),
            session_ref: RefName::new("agent/test/session")?,
            racing_target: racing_commit,
            races: AtomicUsize::new(0),
        });
        let mut racing_stores = stores.clone();
        racing_stores.refs = racing_refs;
        racing_stores
            .refs
            .update_source_checked(SourceCheckedRefUpdate {
                repo_id: repo_id.clone(),
                source_name: RefName::new("main")?,
                source_expectation: RefExpectation::Matches {
                    target: base_commit,
                    version: crate::backend::RefVersion::new(1)?,
                },
                target_update: RefUpdate {
                    repo_id: repo_id.clone(),
                    name: RefName::new("agent/test/session")?,
                    target: base_commit,
                    expectation: RefExpectation::MustNotExist,
                },
            })
            .await?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: base_root_tree(&racing_stores, &repo_id, base_commit).await?,
            commit_id: Some(base_commit),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(1),
        })?;
        let base_object_id = object_id(b"stale base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/secret/stale.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"stale secret body",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-stale-operation",
            "queued-stale-source",
            WritebackState::Pending,
            110,
        )?;
        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        let execution =
            execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &racing_stores, plan, 130)
                .await
                .expect_err("stale session CAS should fail the flush");
        let rendered = execution.to_string();

        assert!(
            rendered.contains("sparse write-back durable mutation failed"),
            "{rendered}"
        );
        for secret in [
            "secret",
            "stale secret body",
            "/secret/stale.txt",
            "queued-stale-operation",
            "queued-stale-source",
        ] {
            assert!(!rendered.contains(secret), "error leaked {secret}");
        }
        assert_ne!(
            cache.dirty_entry_for_inode(view_id, 2)?.unwrap().state,
            crate::sparse_cache::DirtyEntryState::Flushed
        );
        let progress = cache.writeback_progress(view_id)?;
        assert_eq!(progress.flushed, 0);
        assert_eq!(progress.failed, 1);

        Ok(())
    }

    #[tokio::test]
    async fn post_visible_bookkeeping_failure_does_not_mark_applied_flush_failed()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let base_commit = seed_empty_base(&stores, &repo_id).await?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: base_root_tree(&stores, &repo_id, base_commit).await?,
            commit_id: Some(base_commit),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(1),
        })?;
        let base_object_id = object_id(b"post visible base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/post-visible.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"post-visible body",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-post-visible-operation",
            "queued-post-visible-source",
            WritebackState::Pending,
            110,
        )?;
        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/post-visible")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;
        let mut failing_stores = stores.clone();
        failing_stores.objects = Arc::new(FailingContainsObjectStore {
            inner: stores.objects.clone(),
        });

        let error =
            execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &failing_stores, plan, 130)
                .await
                .expect_err("post-visible local bookkeeping should fail");
        let rendered = error.to_string();

        assert!(rendered.contains("sparse write-back post-visible bookkeeping failed"));
        assert!(!rendered.contains("post-visible body"));
        let session = stores
            .refs
            .get(&repo_id, &RefName::new("agent/test/post-visible")?)
            .await?
            .expect("durable session ref should already be visible");
        assert_ne!(session.target, base_commit);
        let progress = cache.writeback_progress(view_id)?;
        assert_eq!(progress.running, 1);
        assert_eq!(progress.failed, 0);
        assert_eq!(progress.flushed, 0);
        assert_eq!(
            cache.dirty_entry_for_inode(view_id, 2)?.unwrap().state,
            crate::sparse_cache::DirtyEntryState::Queued
        );

        Ok(())
    }

    #[tokio::test]
    async fn stale_plan_cannot_mark_requeued_dirty_entry_failed() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"stale requeue base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/requeued.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"first requeue body",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-requeue-operation-1",
            "queued-requeue-source-1",
            WritebackState::Pending,
            110,
        )?;
        let stale_plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/requeued")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;
        cache.write_dirty_file(
            view_id,
            2,
            "/requeued.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"second requeue body",
            130,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-requeue-operation-2",
            "queued-requeue-source-2",
            WritebackState::Pending,
            140,
        )?;

        let error =
            execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &stores, stale_plan, 150)
                .await
                .expect_err("stale plan content identity should fail before durable mutation");

        assert!(
            error
                .to_string()
                .contains("sparse write-back durable mutation failed")
        );
        let progress = cache.writeback_progress(view_id)?;
        assert_eq!(progress.pending, 1);
        assert_eq!(progress.failed, 0);
        assert_eq!(progress.flushed, 0);
        assert_eq!(
            cache.dirty_entry_for_inode(view_id, 2)?.unwrap().state,
            crate::sparse_cache::DirtyEntryState::Queued
        );

        Ok(())
    }

    #[tokio::test]
    async fn stale_queue_identity_is_rejected_before_durable_mutation() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let base_commit = seed_empty_base(&stores, &repo_id).await?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: base_root_tree(&stores, &repo_id, base_commit).await?,
            commit_id: Some(base_commit),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(1),
        })?;
        let base_object_id = object_id(b"stale queue identity base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/stale-queue.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"stale queue body",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-stale-queue-operation-1",
            "queued-stale-queue-source",
            WritebackState::Pending,
            110,
        )?;
        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/stale-queue")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;
        cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue SET operation_id = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["queued-stale-queue-operation-2", dirty.dirty_id],
            )
            .expect("replace queue identity after planning");

        let error = execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &stores, plan, 130)
            .await
            .expect_err("stale queue identity should be rejected before durable mutation");

        assert!(
            error
                .to_string()
                .contains("sparse write-back durable mutation failed")
        );
        assert!(
            stores
                .refs
                .get(&repo_id, &RefName::new("agent/test/stale-queue")?)
                .await?
                .is_none()
        );
        let progress = cache.writeback_progress(view_id)?;
        assert_eq!(progress.pending, 1);
        assert_eq!(progress.running, 0);
        assert_eq!(progress.failed, 0);
        assert_eq!(progress.flushed, 0);

        Ok(())
    }

    #[tokio::test]
    async fn disabled_flush_executor_never_calls_ref_store() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"disabled executor base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/disabled-executor.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"disabled executor body",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-disabled-executor-operation",
            "queued-disabled-executor-source",
            WritebackState::Pending,
            110,
        )?;
        let counting_refs = Arc::new(CountingRefStore {
            inner: stores.refs.clone(),
            calls: AtomicUsize::new(0),
        });
        let mut counting_stores = stores.clone();
        counting_stores.refs = counting_refs.clone();
        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::Disabled,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        let error =
            execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &counting_stores, plan, 130)
                .await
                .expect_err("disabled executor should refuse the plan");

        assert!(error.to_string().contains("sparse write-back disabled"));
        assert_eq!(counting_refs.calls.load(Ordering::SeqCst), 0);
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[tokio::test]
    async fn commit_staging_rejects_pending_dirty_entries() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: object_id(b"commit staging dirty root"),
            commit_id: Some(CommitId::from(object_id(b"commit staging dirty base"))),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(1),
        })?;
        let base_commit = CommitId::from(object_id(b"commit staging dirty base"));
        let session_commit = CommitId::from(object_id(b"commit staging dirty session"));
        let session_root = object_id(b"commit staging dirty session root");
        let base_object_id = object_id(b"commit staging dirty base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        cache.write_dirty_file(
            view_id,
            2,
            "/secret/pending.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"pending dirty body",
            100,
        )?;

        let summary = sparse_commit_dirty_queue_summary(&cache, view_id)?;
        let error = stage_sparse_commit_for_tests(
            &StratumStores::local_memory(),
            SparseCommitStagingInput {
                mode: SparseWriteBackMode::EnabledForTests,
                repo_id,
                target_ref: RefName::new("main")?,
                target_commit_id: base_commit,
                target_ref_version: 1,
                session_ref: RefName::new("agent/test/session")?,
                session_commit_id: session_commit,
                session_ref_version: 2,
                session_root_tree_id: session_root,
                dirty_queue_summary: summary,
            },
        )
        .await
        .expect_err("pending dirty entries should block sparse commit staging");

        assert!(error.to_string().contains("sparse commit staging blocked"));

        for (label, summary) in [
            (
                "queued dirty entry without a pending queue row",
                SparseCommitDirtyQueueSummary {
                    queued_dirty_entries: 1,
                    ..SparseCommitDirtyQueueSummary::default()
                },
            ),
            (
                "failed dirty entry",
                SparseCommitDirtyQueueSummary {
                    failed_dirty_entries: 1,
                    ..SparseCommitDirtyQueueSummary::default()
                },
            ),
            (
                "running queue entry",
                SparseCommitDirtyQueueSummary {
                    running_queue_entries: 1,
                    ..SparseCommitDirtyQueueSummary::default()
                },
            ),
            (
                "failed queue entry",
                SparseCommitDirtyQueueSummary {
                    failed_queue_entries: 1,
                    ..SparseCommitDirtyQueueSummary::default()
                },
            ),
            (
                "disabled queue entry",
                SparseCommitDirtyQueueSummary {
                    disabled_queue_entries: 1,
                    ..SparseCommitDirtyQueueSummary::default()
                },
            ),
        ] {
            stage_sparse_commit_for_tests(
                &StratumStores::local_memory(),
                SparseCommitStagingInput {
                    mode: SparseWriteBackMode::EnabledForTests,
                    repo_id: RepoId::local(),
                    target_ref: RefName::new("main")?,
                    target_commit_id: base_commit,
                    target_ref_version: 1,
                    session_ref: RefName::new("agent/test/session")?,
                    session_commit_id: session_commit,
                    session_ref_version: 2,
                    session_root_tree_id: session_root,
                    dirty_queue_summary: summary,
                },
            )
            .await
            .expect_err(label);
        }

        Ok(())
    }

    #[tokio::test]
    async fn commit_staging_uses_durable_ref_cas_ordered_write_path() -> Result<(), VfsError> {
        let fixture = flushed_commit_staging_fixture().await?;
        let staging = stage_sparse_commit_for_tests(&fixture.stores, fixture.input()).await?;

        assert!(staging.promotion_allowed);
        assert!(staging.repo_id_present);
        assert!(staging.target_ref_present);
        assert!(staging.session_ref_present);
        assert_eq!(staging.changed_path_count, 1);
        assert_eq!(staging.planned_object_count, 0);
        assert_eq!(
            staging.visibility_step,
            crate::backend::core_transaction::DurableCoreTransactionStep::RefCompareAndSwap
        );
        assert_eq!(
            staging.ordered_write_path,
            crate::backend::core_transaction::DurableCoreStepSemantics::ordered_write_path()
        );
        assert_eq!(
            staging.pre_visibility_recovery_stages,
            vec![
                crate::backend::core_transaction::DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
                crate::backend::core_transaction::DurableCorePreVisibilityRecoveryStage::RefVisibilityCas,
            ]
        );
        assert_eq!(
            staging.post_cas_recovery_steps,
            vec![
                crate::backend::core_transaction::DurableCorePostCasStep::WorkspaceHeadUpdate,
                crate::backend::core_transaction::DurableCorePostCasStep::AuditAppend,
                crate::backend::core_transaction::DurableCorePostCasStep::IdempotencyCompletion,
            ]
        );
        assert_eq!(staging.target_ref_version, 1);
        assert_eq!(staging.expected_visible_ref_version, 2);

        Ok(())
    }

    #[tokio::test]
    async fn commit_staging_rejects_disabled_non_main_stale_refs_and_overflow()
    -> Result<(), VfsError> {
        let fixture = flushed_commit_staging_fixture().await?;

        let mut disabled = fixture.input();
        disabled.mode = SparseWriteBackMode::Disabled;
        stage_sparse_commit_for_tests(&fixture.stores, disabled)
            .await
            .expect_err("disabled write-back mode should block staging");

        let mut non_main = fixture.input();
        non_main.target_ref = RefName::new("agent/test/not-main")?;
        stage_sparse_commit_for_tests(&fixture.stores, non_main)
            .await
            .expect_err("non-main target ref should block sparse commit staging");

        let mut stale_target = fixture.input();
        stale_target.target_ref_version += 1;
        stage_sparse_commit_for_tests(&fixture.stores, stale_target)
            .await
            .expect_err("stale target ref version should block staging");

        let mut stale_session = fixture.input();
        stale_session.session_ref_version += 1;
        stage_sparse_commit_for_tests(&fixture.stores, stale_session)
            .await
            .expect_err("stale session ref version should block staging");

        let mut overflow_stores = fixture.stores.clone();
        overflow_stores.refs = Arc::new(StaticRefStore::new(vec![
            ref_record(
                &fixture.repo_id,
                &fixture.target_ref,
                fixture.target_commit_id,
                u64::MAX - 1,
            )?,
            ref_record(
                &fixture.repo_id,
                &fixture.session_ref,
                fixture.session_commit_id,
                fixture.session_ref_version,
            )?,
        ]));
        let mut overflow = fixture.input();
        overflow.target_ref_version = u64::MAX - 1;
        stage_sparse_commit_for_tests(&overflow_stores, overflow)
            .await
            .expect_err("impossible next ref version should block staging");

        Ok(())
    }

    #[tokio::test]
    async fn commit_staging_accepts_internal_previous_promotion_only() -> Result<(), VfsError> {
        let fixture = flushed_commit_staging_fixture().await?;
        let promoted_commit = insert_test_commit(
            &fixture.stores,
            &fixture.repo_id,
            "sparse commit previous promotion target",
            fixture.session_root_tree_id,
            vec![fixture.target_commit_id],
            "promoted sparse session",
            200,
        )
        .await?;
        let main = fixture
            .stores
            .refs
            .update(RefUpdate {
                repo_id: fixture.repo_id.clone(),
                name: fixture.target_ref.clone(),
                target: promoted_commit,
                expectation: RefExpectation::Matches {
                    target: fixture.target_commit_id,
                    version: RefVersion::new(fixture.target_ref_version)?,
                },
            })
            .await?;

        let mut previous_promotion = fixture.input();
        previous_promotion.target_commit_id = promoted_commit;
        previous_promotion.target_ref_version = main.version.value();
        let staging =
            stage_sparse_commit_for_tests(&fixture.stores, previous_promotion.clone()).await?;
        assert!(staging.promotion_allowed);
        assert_eq!(staging.expected_visible_ref_version, 3);

        let lookalike = insert_test_commit(
            &fixture.stores,
            &fixture.repo_id,
            "sparse commit non internal lookalike",
            fixture.session_root_tree_id,
            vec![fixture.target_commit_id],
            "not an internal durable mutation",
            201,
        )
        .await?;
        let session = fixture
            .stores
            .refs
            .get(&fixture.repo_id, &fixture.session_ref)
            .await?
            .expect("session ref should exist before lookalike update");
        let updated_session = fixture
            .stores
            .refs
            .update(RefUpdate {
                repo_id: fixture.repo_id.clone(),
                name: fixture.session_ref.clone(),
                target: lookalike,
                expectation: RefExpectation::Matches {
                    target: session.target,
                    version: session.version,
                },
            })
            .await?;
        previous_promotion.session_commit_id = lookalike;
        previous_promotion.session_ref_version = updated_session.version.value();
        stage_sparse_commit_for_tests(&fixture.stores, previous_promotion)
            .await
            .expect_err("non-internal previous-promotion lookalike should block staging");

        Ok(())
    }

    #[tokio::test]
    async fn commit_staging_rejects_forged_internal_session_ancestry() -> Result<(), VfsError> {
        let fixture = flushed_commit_staging_fixture().await?;
        let unrelated_parent = insert_test_commit(
            &fixture.stores,
            &fixture.repo_id,
            "sparse commit forged unrelated parent",
            fixture.session_root_tree_id,
            Vec::new(),
            "unrelated user commit",
            210,
        )
        .await?;
        let forged_session = insert_test_commit(
            &fixture.stores,
            &fixture.repo_id,
            "sparse commit forged internal session",
            fixture.session_root_tree_id,
            vec![unrelated_parent],
            DURABLE_MUTATION_COMMIT_MESSAGE,
            211,
        )
        .await?;
        let session = fixture
            .stores
            .refs
            .get(&fixture.repo_id, &fixture.session_ref)
            .await?
            .expect("session ref should exist before forged update");
        let updated_session = fixture
            .stores
            .refs
            .update(RefUpdate {
                repo_id: fixture.repo_id.clone(),
                name: fixture.session_ref.clone(),
                target: forged_session,
                expectation: RefExpectation::Matches {
                    target: session.target,
                    version: session.version,
                },
            })
            .await?;
        let mut input = fixture.input();
        input.session_commit_id = forged_session;
        input.session_ref_version = updated_session.version.value();

        stage_sparse_commit_for_tests(&fixture.stores, input)
            .await
            .expect_err("forged internal session ancestry should block staging");

        Ok(())
    }

    #[test]
    fn commit_staging_debug_redacts_paths_and_messages() -> Result<(), VfsError> {
        let plan = SparseCommitStagingPlan {
            promotion_allowed: true,
            repo_id_present: true,
            target_ref_present: true,
            session_ref_present: true,
            target_ref_version: 41,
            session_ref_version: 42,
            expected_visible_ref_version: 42,
            changed_path_count: 3,
            planned_object_count: 0,
            visibility_step:
                crate::backend::core_transaction::DurableCoreTransactionStep::RefCompareAndSwap,
            ordered_write_path:
                crate::backend::core_transaction::DurableCoreStepSemantics::ordered_write_path()
                    .to_vec(),
            pre_visibility_recovery_stages: vec![
                crate::backend::core_transaction::DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
                crate::backend::core_transaction::DurableCorePreVisibilityRecoveryStage::RefVisibilityCas,
            ],
            post_cas_recovery_steps: vec![
                crate::backend::core_transaction::DurableCorePostCasStep::WorkspaceHeadUpdate,
                crate::backend::core_transaction::DurableCorePostCasStep::AuditAppend,
                crate::backend::core_transaction::DurableCorePostCasStep::IdempotencyCompletion,
            ],
            redacted_state_message: "secret message /private/path token".to_string(),
        };
        let debug = format!("{plan:?}");

        assert!(debug.contains("SparseCommitStagingPlan"));
        assert!(debug.contains("changed_path_count"));
        assert!(debug.contains("redacted_state_present"));
        for secret in ["secret", "/private/path", "token", "message"] {
            assert!(!debug.contains(secret), "debug leaked {secret}");
        }

        Ok(())
    }

    fn cache_view_identity() -> CacheViewIdentity {
        CacheViewIdentity {
            repo_id: RepoId::new("local").unwrap(),
            root_tree_id: object_id(b"root tree"),
            commit_id: Some(CommitId::from(object_id(b"commit"))),
            ref_name: Some(RefName::new("main").unwrap()),
            ref_version: Some(7),
        }
    }

    fn put_dirty_file_inode(
        cache: &SparseCache,
        view_id: i64,
        inode_id: u64,
        base_object_id: ObjectId,
    ) -> Result<(), VfsError> {
        cache.put_inode(&CachedInode {
            view_id,
            inode_id,
            node_kind: CachedNodeKind::File,
            object_id: Some(base_object_id),
            object_kind: Some(ObjectKind::Blob),
            mode: 0o100644,
            uid: 501,
            gid: 20,
            nlink: 1,
            size: 0,
            size_known: true,
            block_size: 4096,
            blocks: 0,
            mtime_secs: 1,
            mtime_nanos: 2,
            ctime_secs: 3,
            ctime_nanos: 4,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
            lookup_count: 0,
        })
    }

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    struct SparseCommitStageFixture {
        stores: StratumStores,
        repo_id: RepoId,
        target_ref: RefName,
        target_commit_id: CommitId,
        target_ref_version: u64,
        session_ref: RefName,
        session_commit_id: CommitId,
        session_ref_version: u64,
        session_root_tree_id: ObjectId,
        dirty_queue_summary: SparseCommitDirtyQueueSummary,
    }

    impl SparseCommitStageFixture {
        fn input(&self) -> SparseCommitStagingInput {
            SparseCommitStagingInput {
                mode: SparseWriteBackMode::EnabledForTests,
                repo_id: self.repo_id.clone(),
                target_ref: self.target_ref.clone(),
                target_commit_id: self.target_commit_id,
                target_ref_version: self.target_ref_version,
                session_ref: self.session_ref.clone(),
                session_commit_id: self.session_commit_id,
                session_ref_version: self.session_ref_version,
                session_root_tree_id: self.session_root_tree_id,
                dirty_queue_summary: self.dirty_queue_summary,
            }
        }
    }

    async fn flushed_commit_staging_fixture() -> Result<SparseCommitStageFixture, VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let stores = StratumStores::local_memory();
        let repo_id = RepoId::local();
        let target_ref = RefName::new("main")?;
        let session_ref = RefName::new("agent/test/session")?;
        let target_commit_id = seed_empty_base(&stores, &repo_id).await?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: base_root_tree(&stores, &repo_id, target_commit_id).await?,
            commit_id: Some(target_commit_id),
            ref_name: Some(target_ref.clone()),
            ref_version: Some(1),
        })?;
        let base_object_id = object_id(b"commit staging flushed base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/staged.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"staged body",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "commit-staging-operation",
            "commit-staging-source",
            WritebackState::Pending,
            110,
        )?;
        let flush_plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: target_ref.clone(),
                session_ref: session_ref.clone(),
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;
        let flush =
            execute_sparse_writeback_flush_for_tests(&cache, &repo_id, &stores, flush_plan, 130)
                .await?;
        let session = stores
            .refs
            .get(&repo_id, &session_ref)
            .await?
            .expect("session ref should exist after flush");

        Ok(SparseCommitStageFixture {
            stores,
            repo_id,
            target_ref,
            target_commit_id,
            target_ref_version: 1,
            session_ref,
            session_commit_id: session.target,
            session_ref_version: session.version.value(),
            session_root_tree_id: flush.flushed[0].target.root_tree,
            dirty_queue_summary: sparse_commit_dirty_queue_summary(&cache, view_id)?,
        })
    }

    async fn seed_empty_base(
        stores: &StratumStores,
        repo_id: &RepoId,
    ) -> Result<CommitId, VfsError> {
        let root_tree = put_test_object(
            stores,
            repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await?;
        let commit_id = CommitId::from(object_id(b"sparse flush base"));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree,
                parents: Vec::new(),
                timestamp: 1,
                message: "base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await?;
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("main")?,
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await?;
        Ok(commit_id)
    }

    async fn insert_test_commit(
        stores: &StratumStores,
        repo_id: &RepoId,
        seed: &str,
        root_tree: ObjectId,
        parents: Vec<CommitId>,
        message: &str,
        timestamp: u64,
    ) -> Result<CommitId, VfsError> {
        let commit_id = CommitId::from(object_id(seed.as_bytes()));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree,
                parents,
                timestamp,
                message: message.to_string(),
                author: "test-author".to_string(),
                changed_paths: Vec::new(),
            })
            .await?;
        Ok(commit_id)
    }

    fn ref_record(
        repo_id: &RepoId,
        name: &RefName,
        target: CommitId,
        version: u64,
    ) -> Result<RefRecord, VfsError> {
        Ok(RefRecord {
            repo_id: repo_id.clone(),
            name: name.clone(),
            target,
            version: RefVersion::new(version)?,
        })
    }

    async fn insert_empty_child_commit(
        stores: &StratumStores,
        repo_id: &RepoId,
        parent: CommitId,
        seed: &str,
    ) -> Result<CommitId, VfsError> {
        let root_tree = base_root_tree(stores, repo_id, parent).await?;
        let commit_id = CommitId::from(object_id(seed.as_bytes()));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree,
                parents: vec![parent],
                timestamp: 2,
                message: "race".to_string(),
                author: "racer".to_string(),
                changed_paths: Vec::new(),
            })
            .await?;
        Ok(commit_id)
    }

    async fn base_root_tree(
        stores: &StratumStores,
        repo_id: &RepoId,
        commit_id: CommitId,
    ) -> Result<ObjectId, VfsError> {
        stores
            .commits
            .get(repo_id, commit_id)
            .await?
            .map(|commit| commit.root_tree)
            .ok_or_else(sparse_cache_error)
    }

    async fn put_test_object(
        stores: &StratumStores,
        repo_id: &RepoId,
        kind: ObjectKind,
        bytes: Vec<u8>,
    ) -> Result<ObjectId, VfsError> {
        let id = ObjectId::from_bytes(&bytes);
        stores
            .objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id,
                kind,
                bytes,
            })
            .await?;
        Ok(id)
    }

    struct CountingRefStore {
        inner: Arc<dyn RefStore>,
        calls: AtomicUsize,
    }

    struct FailingContainsObjectStore {
        inner: Arc<dyn ObjectStore>,
    }

    struct StaticRefStore {
        records: BTreeMap<(RepoId, RefName), RefRecord>,
    }

    impl StaticRefStore {
        fn new(records: Vec<RefRecord>) -> Self {
            Self {
                records: records
                    .into_iter()
                    .map(|record| ((record.repo_id.clone(), record.name.clone()), record))
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for FailingContainsObjectStore {
        async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
            self.inner.put(write).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<Option<StoredObject>, VfsError> {
            self.inner.get(repo_id, id, expected_kind).await
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

    #[async_trait]
    impl RefStore for StaticRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            Ok(self
                .records
                .iter()
                .filter(|((record_repo_id, _), _)| record_repo_id == repo_id)
                .map(|(_, record)| record.clone())
                .collect())
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            Ok(self.records.get(&(repo_id.clone(), name.clone())).cloned())
        }

        async fn update(&self, _update: RefUpdate) -> Result<RefRecord, VfsError> {
            Err(VfsError::NotSupported {
                message: "static ref store is read-only".to_string(),
            })
        }

        async fn update_source_checked(
            &self,
            _update: SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            Err(VfsError::NotSupported {
                message: "static ref store is read-only".to_string(),
            })
        }
    }

    #[async_trait]
    impl RefStore for CountingRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get(repo_id, name).await
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.update(update).await
        }

        async fn update_source_checked(
            &self,
            update: SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.update_source_checked(update).await
        }
    }

    struct RacingSessionRefStore {
        inner: Arc<dyn RefStore>,
        session_ref: RefName,
        racing_target: CommitId,
        races: AtomicUsize,
    }

    #[async_trait]
    impl RefStore for RacingSessionRefStore {
        async fn list(&self, repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            self.inner.list(repo_id).await
        }

        async fn get(
            &self,
            repo_id: &RepoId,
            name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            self.inner.get(repo_id, name).await
        }

        async fn update(&self, update: RefUpdate) -> Result<RefRecord, VfsError> {
            if update.name == self.session_ref && self.races.fetch_add(1, Ordering::SeqCst) == 0 {
                let current = self
                    .inner
                    .get(&update.repo_id, &update.name)
                    .await?
                    .expect("session ref should exist before racing update");
                self.inner
                    .update(RefUpdate {
                        repo_id: update.repo_id.clone(),
                        name: update.name.clone(),
                        target: self.racing_target,
                        expectation: RefExpectation::Matches {
                            target: current.target,
                            version: current.version,
                        },
                    })
                    .await?;
            }
            self.inner.update(update).await
        }

        async fn update_source_checked(
            &self,
            update: SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.inner.update_source_checked(update).await
        }
    }
}
