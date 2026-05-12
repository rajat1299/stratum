//! Durable mutable tree transactions for session refs.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::auth::perms::{Access, has_sticky_bit};
use crate::auth::session::Session;
use crate::auth::{ROOT_GID, ROOT_UID};
use crate::backend::object_cleanup::{
    ObjectCleanupClaimKind, ObjectCleanupClaimRequest, ObjectCleanupClaimStore,
    canonical_final_object_key,
};
use crate::backend::{
    CommitRecord, CommitStore, ObjectStore, ObjectWrite, RefExpectation, RefRecord, RefStore,
    RefUpdate, RefVersion, RepoId, SourceCheckedRefUpdate,
};
use crate::error::VfsError;
use crate::fs::{MetadataUpdate, validate_mime_type};
use crate::server::policy::{
    PolicyAction, PolicyDecisionToken, require_policy_token_allowed_for,
    require_policy_token_allowed_for_paths_with_descendants,
};
use crate::store::commit::CommitObject;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::change::{PathMap, diff_path_maps};
use crate::vcs::{ChangedPath, CommitId, PathKind, PathRecord, RefName};

const DURABLE_MUTATION_AUTHOR: &str = "durable-mutation";
pub(crate) const DURABLE_MUTATION_COMMIT_MESSAGE: &str = "internal durable mutation";
const MAX_CUSTOM_ATTRS: usize = 64;
const MAX_CUSTOM_ATTR_KEY_LEN: usize = 64;
const MAX_CUSTOM_ATTR_VALUE_LEN: usize = 1024;
const MAX_CUSTOM_ATTR_TOTAL_BYTES: usize = 8192;
const DURABLE_MUTATION_CAS_LOST_CLEANUP_LEASE_OWNER: &str = "durable-mutation-cas-lost-cleanup";
const DURABLE_MUTATION_CAS_LOST_CLEANUP_LEASE_DURATION: Duration = Duration::from_secs(300);

pub(crate) struct DurableMutationEngine<'a> {
    repo_id: &'a RepoId,
    refs: &'a dyn RefStore,
    commits: &'a dyn CommitStore,
    objects: &'a dyn ObjectStore,
    cleanup_claims: Option<&'a dyn ObjectCleanupClaimStore>,
    policy_token: Option<&'a PolicyDecisionToken>,
}

impl<'a> DurableMutationEngine<'a> {
    pub(crate) fn new(
        repo_id: &'a RepoId,
        refs: &'a dyn RefStore,
        commits: &'a dyn CommitStore,
        objects: &'a dyn ObjectStore,
    ) -> Self {
        Self {
            repo_id,
            refs,
            commits,
            objects,
            cleanup_claims: None,
            policy_token: None,
        }
    }

    pub(crate) fn with_cleanup_claims(
        mut self,
        cleanup_claims: &'a dyn ObjectCleanupClaimStore,
    ) -> Self {
        self.cleanup_claims = Some(cleanup_claims);
        self
    }

    pub(crate) fn with_policy_token(mut self, policy_token: &'a PolicyDecisionToken) -> Self {
        self.policy_token = Some(policy_token);
        self
    }

    pub(crate) async fn apply(
        &self,
        input: DurableMutationInput,
    ) -> Result<DurableMutationOutput, VfsError> {
        self.apply_authorized(input, self.policy_token).await
    }

    async fn apply_authorized(
        &self,
        input: DurableMutationInput,
        policy_token: Option<&PolicyDecisionToken>,
    ) -> Result<DurableMutationOutput, VfsError> {
        let policy_action = policy_action_for_operation(&input.operation);
        require_policy_token_allowed_for(
            policy_token,
            self.repo_id,
            policy_action,
            input.base_ref.as_str(),
        )?;
        let policy_records = self.policy_path_records_for_input(&input).await?;
        let policy_scope = policy_scope_for_operation(&policy_records, &input.operation)?;
        require_policy_token_allowed_for_paths_with_descendants(
            policy_token,
            self.repo_id,
            policy_action,
            input.base_ref.as_str(),
            policy_scope.paths.iter().map(String::as_str),
            policy_scope.descendant_paths.iter().map(String::as_str),
        )?;
        let observed = self.resolve_or_materialize_session(&input).await?;
        let previous_commit = observed.session.target;
        let before =
            durable_commit_path_records(self.repo_id, previous_commit, self.commits, self.objects)
                .await?;
        let mut after = before.clone();
        let mut writer = DurableMutationObjectWriter::new(self.repo_id, self.objects);

        if let Some(session) = &input.preflight_session {
            preflight_operation(&before, &input.operation, session)?;
            self.ensure_cleanup_claims_for_preflighted_writes()?;
        }

        apply_operation(&mut after, input.operation, &mut writer).await?;
        refresh_directory_sizes(&mut after);
        let changed_paths = diff_path_maps(&before, &after);
        let root_tree = write_tree_from_path_records(&after, &mut writer).await?;
        let commit = mutation_commit_record(
            self.repo_id.clone(),
            previous_commit,
            root_tree,
            input.timestamp,
            &input.author,
            changed_paths.clone(),
        );
        let new_commit = commit.id;
        let inserted = self
            .commits
            .insert(commit)
            .await
            .map_err(|_| redacted_commit_insert_error())?;
        if inserted.repo_id != *self.repo_id
            || inserted.id != new_commit
            || inserted.root_tree != root_tree
            || inserted.parents.as_slice() != [previous_commit]
        {
            return Err(redacted_commit_insert_error());
        }

        let updated = match self
            .refs
            .update(RefUpdate {
                repo_id: self.repo_id.clone(),
                name: input.session_ref.clone(),
                target: new_commit,
                expectation: RefExpectation::Matches {
                    target: observed.session.target,
                    version: observed.session.version,
                },
            })
            .await
        {
            Ok(updated) => updated,
            Err(error) => {
                let redacted = redact_ref_cas_error(error);
                if is_ref_cas_mismatch_error(&redacted) {
                    self.claim_cas_lost_cleanup_candidates(writer.cleanup_candidates())
                        .await?;
                }
                return Err(redacted);
            }
        };
        if updated.repo_id != *self.repo_id
            || updated.name != input.session_ref
            || updated.target != new_commit
        {
            return Err(redacted_ref_update_error());
        }

        Ok(DurableMutationOutput {
            previous_commit,
            new_commit,
            changed_paths,
            response_metadata: DurableMutationResponseMetadata {
                session_ref: updated.name,
                session_ref_version: updated.version,
                root_tree,
                changed_path_count: inserted.changed_paths.len(),
            },
            cleanup_candidates: writer.into_cleanup_candidates(),
        })
    }

    async fn policy_path_records_for_input(
        &self,
        input: &DurableMutationInput,
    ) -> Result<PathMap, VfsError> {
        let base = self
            .refs
            .get(self.repo_id, &input.base_ref)
            .await
            .map_err(|_| redacted_ref_resolution_error())?
            .ok_or(VfsError::NoCommits)?;
        let target = match self
            .refs
            .get(self.repo_id, &input.session_ref)
            .await
            .map_err(|_| redacted_ref_resolution_error())?
        {
            Some(session) => session.target,
            None => base.target,
        };
        durable_commit_path_records(self.repo_id, target, self.commits, self.objects).await
    }

    #[cfg(test)]
    async fn apply_with_test_policy(
        &self,
        input: DurableMutationInput,
    ) -> Result<DurableMutationOutput, VfsError> {
        let records = self.policy_path_records_for_input(&input).await.unwrap();
        let scope = policy_scope_for_operation(&records, &input.operation).unwrap();
        let token = PolicyDecisionToken::allow_for_test_with_paths(
            policy_action_for_operation(&input.operation),
            input.base_ref.as_str(),
            scope.paths.iter().map(String::as_str),
        );
        let token = if scope.descendant_paths.is_empty() {
            token
        } else {
            PolicyDecisionToken::allow_for_test_with_paths_and_descendants(
                policy_action_for_operation(&input.operation),
                input.base_ref.as_str(),
                scope.paths.iter().map(String::as_str),
                scope.descendant_paths.iter().map(String::as_str),
            )
        };
        self.apply_authorized(input, Some(&token)).await
    }

    async fn resolve_or_materialize_session(
        &self,
        input: &DurableMutationInput,
    ) -> Result<ObservedSessionRef, VfsError> {
        let base = self
            .refs
            .get(self.repo_id, &input.base_ref)
            .await
            .map_err(|_| redacted_ref_resolution_error())?
            .ok_or(VfsError::NoCommits)?;

        if let Some(session) = self
            .refs
            .get(self.repo_id, &input.session_ref)
            .await
            .map_err(|_| redacted_ref_resolution_error())?
        {
            return Ok(ObservedSessionRef { session });
        }

        let session = self
            .refs
            .update_source_checked(SourceCheckedRefUpdate {
                repo_id: self.repo_id.clone(),
                source_name: input.base_ref.clone(),
                source_expectation: RefExpectation::Matches {
                    target: base.target,
                    version: base.version,
                },
                target_update: RefUpdate {
                    repo_id: self.repo_id.clone(),
                    name: input.session_ref.clone(),
                    target: base.target,
                    expectation: RefExpectation::MustNotExist,
                },
            })
            .await
            .map_err(redact_ref_cas_error)?;
        if session.repo_id != *self.repo_id
            || session.name != input.session_ref
            || session.target != base.target
        {
            return Err(redacted_ref_update_error());
        }

        Ok(ObservedSessionRef { session })
    }

    async fn claim_cas_lost_cleanup_candidates(
        &self,
        candidates: Vec<DurableMutationCleanupCandidate>,
    ) -> Result<(), VfsError> {
        let Some(cleanup_claims) = self.cleanup_claims else {
            tracing::debug!(
                candidate_count = candidates.len(),
                "durable mutation CAS-lost cleanup candidates are not recoverable"
            );
            return Ok(());
        };

        // Object writes get durable cleanup claims when CAS loses. The synthetic
        // commit record is still only reachable from commit metadata; reclaiming
        // unreachable commit records needs a separate commit GC contract.
        for candidate in candidates {
            cleanup_claims
                .claim(ObjectCleanupClaimRequest {
                    repo_id: self.repo_id.clone(),
                    claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    object_kind: candidate.object_kind,
                    object_id: candidate.object_id,
                    object_key: canonical_final_object_key(
                        self.repo_id,
                        candidate.object_kind,
                        &candidate.object_id,
                    ),
                    lease_owner: DURABLE_MUTATION_CAS_LOST_CLEANUP_LEASE_OWNER.to_string(),
                    lease_duration: DURABLE_MUTATION_CAS_LOST_CLEANUP_LEASE_DURATION,
                })
                .await?;
        }
        Ok(())
    }

    fn ensure_cleanup_claims_for_preflighted_writes(&self) -> Result<(), VfsError> {
        if self.cleanup_claims.is_some() {
            return Ok(());
        }
        Err(VfsError::CorruptStore {
            message: "durable mutation cleanup claim store is required".to_string(),
        })
    }
}

#[derive(Debug, Clone)]
struct ObservedSessionRef {
    session: RefRecord,
}

#[derive(Clone)]
pub(crate) struct DurableMutationInput {
    pub(crate) base_ref: RefName,
    pub(crate) session_ref: RefName,
    pub(crate) operation: DurableMutationOperation,
    pub(crate) author: String,
    pub(crate) timestamp: u64,
    pub(crate) preflight_session: Option<Session>,
}

impl fmt::Debug for DurableMutationInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableMutationInput")
            .field("base_ref", &self.base_ref)
            .field("session_ref", &self.session_ref)
            .field("operation", &self.operation)
            .field("author", &self.author)
            .field("timestamp", &self.timestamp)
            .field(
                "preflight_session",
                &self.preflight_session.as_ref().map(|_| "<session>"),
            )
            .finish()
    }
}

fn policy_action_for_operation(operation: &DurableMutationOperation) -> PolicyAction {
    match operation {
        DurableMutationOperation::WriteFile { .. } => PolicyAction::FsWrite,
        DurableMutationOperation::Mkdir { .. } => PolicyAction::FsMkdir,
        DurableMutationOperation::Delete { .. } => PolicyAction::FsDelete,
        DurableMutationOperation::Copy { .. } => PolicyAction::FsCopy,
        DurableMutationOperation::Move { .. } => PolicyAction::FsMove,
        DurableMutationOperation::SetMetadata { .. } => PolicyAction::FsMetadataUpdate,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyOperationScope {
    paths: Vec<String>,
    descendant_paths: Vec<String>,
}

fn policy_scope_for_operation(
    records: &PathMap,
    operation: &DurableMutationOperation,
) -> Result<PolicyOperationScope, VfsError> {
    match operation {
        DurableMutationOperation::WriteFile { path, .. }
        | DurableMutationOperation::Mkdir { path, .. }
        | DurableMutationOperation::SetMetadata { path, .. } => Ok(PolicyOperationScope {
            paths: vec![normalize_path(path)?],
            descendant_paths: Vec::new(),
        }),
        DurableMutationOperation::Delete { path, recursive } => {
            let path = normalize_path(path)?;
            Ok(PolicyOperationScope {
                paths: vec![path.clone()],
                descendant_paths: if *recursive { vec![path] } else { Vec::new() },
            })
        }
        DurableMutationOperation::Copy {
            source,
            destination,
        } => {
            let source = normalize_path(source)?;
            let destination = normalize_copy_move_destination(records, &source, destination)?;
            Ok(PolicyOperationScope {
                paths: vec![destination],
                descendant_paths: Vec::new(),
            })
        }
        DurableMutationOperation::Move {
            source,
            destination,
        } => {
            let source = normalize_path(source)?;
            let destination = normalize_copy_move_destination(records, &source, destination)?;
            let descendant_paths = match records.get(&source) {
                Some(record) if record.kind == PathKind::Directory => {
                    vec![source.clone(), destination.clone()]
                }
                _ => Vec::new(),
            };
            Ok(PolicyOperationScope {
                paths: vec![source, destination],
                descendant_paths,
            })
        }
    }
}

#[derive(Clone)]
pub(crate) enum DurableMutationOperation {
    WriteFile {
        path: String,
        content: Vec<u8>,
        mode: u16,
        uid: u32,
        gid: u32,
        mime_type: Option<String>,
        custom_attrs: BTreeMap<String, String>,
    },
    Mkdir {
        path: String,
        mode: u16,
        uid: u32,
        gid: u32,
    },
    Delete {
        path: String,
        recursive: bool,
    },
    Copy {
        source: String,
        destination: String,
    },
    Move {
        source: String,
        destination: String,
    },
    SetMetadata {
        path: String,
        update: MetadataUpdate,
    },
}

impl fmt::Debug for DurableMutationOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteFile {
                path,
                content,
                mode,
                uid,
                gid,
                mime_type,
                custom_attrs,
            } => f
                .debug_struct("WriteFile")
                .field("path", path)
                .field(
                    "content",
                    &format_args!("<redacted:{} bytes>", content.len()),
                )
                .field("mode", mode)
                .field("uid", uid)
                .field("gid", gid)
                .field("mime_type", mime_type)
                .field("custom_attrs", &RedactedCustomAttrs(custom_attrs))
                .finish(),
            Self::Mkdir {
                path,
                mode,
                uid,
                gid,
            } => f
                .debug_struct("Mkdir")
                .field("path", path)
                .field("mode", mode)
                .field("uid", uid)
                .field("gid", gid)
                .finish(),
            Self::Delete { path, recursive } => f
                .debug_struct("Delete")
                .field("path", path)
                .field("recursive", recursive)
                .finish(),
            Self::Copy {
                source,
                destination,
            } => f
                .debug_struct("Copy")
                .field("source", source)
                .field("destination", destination)
                .finish(),
            Self::Move {
                source,
                destination,
            } => f
                .debug_struct("Move")
                .field("source", source)
                .field("destination", destination)
                .finish(),
            Self::SetMetadata { path, update } => f
                .debug_struct("SetMetadata")
                .field("path", path)
                .field("update", &RedactedMetadataUpdate(update))
                .finish(),
        }
    }
}

struct RedactedCustomAttrs<'a>(&'a BTreeMap<String, String>);

impl fmt::Debug for RedactedCustomAttrs<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value_bytes = self.0.values().map(String::len).sum::<usize>();
        f.debug_struct("CustomAttrs")
            .field("count", &self.0.len())
            .field(
                "value_bytes",
                &format_args!("<redacted:{value_bytes} bytes>"),
            )
            .finish()
    }
}

struct RedactedMetadataUpdate<'a>(&'a MetadataUpdate);

impl fmt::Debug for RedactedMetadataUpdate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataUpdate")
            .field("mime_type", &self.0.mime_type)
            .field("custom_attrs", &RedactedCustomAttrs(&self.0.custom_attrs))
            .field(
                "remove_custom_attrs",
                &format_args!("<redacted:{} keys>", self.0.remove_custom_attrs.len()),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableMutationOutput {
    pub(crate) previous_commit: CommitId,
    pub(crate) new_commit: CommitId,
    pub(crate) changed_paths: Vec<ChangedPath>,
    pub(crate) response_metadata: DurableMutationResponseMetadata,
    pub(crate) cleanup_candidates: Vec<DurableMutationCleanupCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableMutationResponseMetadata {
    pub(crate) session_ref: RefName,
    pub(crate) session_ref_version: RefVersion,
    pub(crate) root_tree: ObjectId,
    pub(crate) changed_path_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableMutationCleanupCandidate {
    pub(crate) object_id: ObjectId,
    pub(crate) object_kind: ObjectKind,
}

struct DurableMutationObjectWriter<'a> {
    repo_id: &'a RepoId,
    objects: &'a dyn ObjectStore,
    written: BTreeMap<ObjectId, DurableMutationCleanupCandidate>,
}

impl<'a> DurableMutationObjectWriter<'a> {
    fn new(repo_id: &'a RepoId, objects: &'a dyn ObjectStore) -> Self {
        Self {
            repo_id,
            objects,
            written: BTreeMap::new(),
        }
    }

    async fn put(&mut self, kind: ObjectKind, bytes: Vec<u8>) -> Result<ObjectId, VfsError> {
        let id = ObjectId::from_bytes(&bytes);
        let stored = self
            .objects
            .put(ObjectWrite {
                repo_id: self.repo_id.clone(),
                id,
                kind,
                bytes,
            })
            .await
            .map_err(|_| redacted_object_write_error())?;
        if stored.repo_id != *self.repo_id || stored.id != id || stored.kind != kind {
            return Err(redacted_object_write_error());
        }
        self.written
            .entry(id)
            .or_insert(DurableMutationCleanupCandidate {
                object_id: id,
                object_kind: kind,
            });
        Ok(id)
    }

    fn cleanup_candidates(&self) -> Vec<DurableMutationCleanupCandidate> {
        self.written.values().copied().collect()
    }

    fn into_cleanup_candidates(self) -> Vec<DurableMutationCleanupCandidate> {
        self.written.into_values().collect()
    }
}

fn preflight_operation(
    records: &PathMap,
    operation: &DurableMutationOperation,
    session: &Session,
) -> Result<(), VfsError> {
    match operation {
        DurableMutationOperation::WriteFile { path, .. } => {
            let path = normalize_path(path)?;
            require_scoped_path(session, &path, Access::Write)?;
            match records.get(&path) {
                Some(record) => {
                    reject_symlink_record(record)?;
                    require_record_access(record, session, &path, Access::Write)
                }
                None => require_parent_write_execute(records, &path, session),
            }
        }
        DurableMutationOperation::Mkdir { path, .. } => {
            let path = normalize_path(path)?;
            if path == "/" {
                return Ok(());
            }
            let mut current = String::new();
            for component in path_components(&path) {
                current.push('/');
                current.push_str(component);
                match records.get(&current) {
                    Some(record) if record.kind == PathKind::Directory => {}
                    Some(_) => return Err(VfsError::NotDirectory { path: current }),
                    None => {
                        require_scoped_path(session, &current, Access::Write)?;
                        return require_parent_write_execute(records, &current, session);
                    }
                }
            }
            Ok(())
        }
        DurableMutationOperation::Delete { path, recursive } => {
            let path = normalize_path(path)?;
            ensure_not_root(&path)?;
            require_scoped_path(session, &path, Access::Write)?;
            require_scoped_path(session, parent_path(&path)?, Access::Write)?;
            let record = records
                .get(&path)
                .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            reject_symlink_record(record)?;
            require_parent_write_execute(records, &path, session)?;
            require_sticky_delete(records, &path, session)?;
            if record.kind == PathKind::Directory {
                if !recursive && has_children(records, &path) {
                    return Err(VfsError::NotEmpty { path });
                }
                if *recursive {
                    validate_recursive_delete(records, &path, session)?;
                }
            }
            Ok(())
        }
        DurableMutationOperation::Copy {
            source,
            destination,
        } => preflight_copy_move(records, source, destination, session, CopyMoveKind::Copy),
        DurableMutationOperation::Move {
            source,
            destination,
        } => preflight_copy_move(records, source, destination, session, CopyMoveKind::Move),
        DurableMutationOperation::SetMetadata { path, .. } => {
            let path = normalize_path(path)?;
            require_scoped_path(session, &path, Access::Write)?;
            let record = records
                .get(&path)
                .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            reject_symlink_record(record)?;
            require_record_access(record, session, &path, Access::Write)
        }
    }
}

#[derive(Clone, Copy)]
enum CopyMoveKind {
    Copy,
    Move,
}

fn preflight_copy_move(
    records: &PathMap,
    source: &str,
    destination: &str,
    session: &Session,
    kind: CopyMoveKind,
) -> Result<(), VfsError> {
    let source = normalize_path(source)?;
    let destination = normalize_copy_move_destination(records, &source, destination)?;
    ensure_not_root(&source)?;
    ensure_not_root(&destination)?;
    require_scoped_path(
        session,
        &source,
        match kind {
            CopyMoveKind::Copy => Access::Read,
            CopyMoveKind::Move => Access::Write,
        },
    )?;
    require_scoped_path(session, &destination, Access::Write)?;
    let source_record = records.get(&source).ok_or_else(|| VfsError::NotFound {
        path: source.clone(),
    })?;
    reject_symlink_record(source_record)?;
    if matches!(kind, CopyMoveKind::Copy) && source_record.kind == PathKind::Directory {
        return Err(VfsError::NotSupported {
            message: "durable directory copy is not supported yet".to_string(),
        });
    }
    require_record_access(
        source_record,
        session,
        &source,
        match kind {
            CopyMoveKind::Copy => Access::Read,
            CopyMoveKind::Move => Access::Write,
        },
    )?;
    if matches!(kind, CopyMoveKind::Move) {
        require_parent_write_execute(records, &source, session)?;
        require_sticky_delete(records, &source, session)?;
    }
    require_parent_write_execute(records, &destination, session)?;
    if let Some(destination_record) = records.get(&destination) {
        reject_symlink_record(destination_record)?;
        if destination_record.kind == PathKind::Directory {
            return Err(VfsError::NotSupported {
                message: "durable copy/move directory replacement is not supported yet".to_string(),
            });
        }
        if matches!(kind, CopyMoveKind::Copy) {
            require_record_access(destination_record, session, &destination, Access::Write)?;
        }
        require_sticky_delete(records, &destination, session)?;
    }
    Ok(())
}

async fn apply_operation(
    records: &mut PathMap,
    operation: DurableMutationOperation,
    writer: &mut DurableMutationObjectWriter<'_>,
) -> Result<(), VfsError> {
    match operation {
        DurableMutationOperation::WriteFile {
            path,
            content,
            mode,
            uid,
            gid,
            mime_type,
            custom_attrs,
        } => {
            validate_custom_attrs(&custom_attrs)?;
            if let Some(mime_type) = &mime_type {
                validate_mime_type(mime_type)?;
            }
            let path = normalize_path(&path)?;
            ensure_not_root(&path)?;
            ensure_parent_directory(records, &path)?;
            if records
                .get(&path)
                .is_some_and(|record| record.kind == PathKind::Directory)
            {
                return Err(VfsError::IsDirectory { path });
            }
            let id = writer.put(ObjectKind::Blob, content.clone()).await?;
            records.insert(
                path.clone(),
                PathRecord {
                    path,
                    kind: PathKind::File,
                    mode,
                    uid,
                    gid,
                    size: content.len() as u64,
                    content_id: Some(id),
                    mime_type,
                    custom_attrs,
                },
            );
        }
        DurableMutationOperation::Mkdir {
            path,
            mode,
            uid,
            gid,
        } => mkdir_p(records, &path, mode, uid, gid)?,
        DurableMutationOperation::Delete { path, recursive } => {
            let path = normalize_path(&path)?;
            ensure_not_root(&path)?;
            let record = records
                .get(&path)
                .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            if record.kind == PathKind::Directory && !recursive && has_children(records, &path) {
                return Err(VfsError::NotEmpty { path });
            }
            remove_subtree(records, &path);
        }
        DurableMutationOperation::Copy {
            source,
            destination,
        } => copy_path(records, &source, &destination)?,
        DurableMutationOperation::Move {
            source,
            destination,
        } => move_path(records, &source, &destination)?,
        DurableMutationOperation::SetMetadata { path, update } => {
            let path = normalize_path(&path)?;
            let record = records
                .get_mut(&path)
                .ok_or_else(|| VfsError::NotFound { path: path.clone() })?;
            apply_metadata(record, update)?;
        }
    }
    Ok(())
}

fn mkdir_p(
    records: &mut PathMap,
    path: &str,
    mode: u16,
    uid: u32,
    gid: u32,
) -> Result<(), VfsError> {
    let path = normalize_path(path)?;
    if path == "/" {
        return Ok(());
    }

    let mut current = String::new();
    for component in path_components(&path) {
        current.push('/');
        current.push_str(component);
        match records.get(&current) {
            Some(record) if record.kind == PathKind::Directory => {}
            Some(_) => {
                return Err(VfsError::NotDirectory {
                    path: current.clone(),
                });
            }
            None => {
                records.insert(
                    current.clone(),
                    PathRecord {
                        path: current.clone(),
                        kind: PathKind::Directory,
                        mode,
                        uid,
                        gid,
                        size: 0,
                        content_id: None,
                        mime_type: None,
                        custom_attrs: BTreeMap::new(),
                    },
                );
            }
        }
    }
    Ok(())
}

fn copy_path(records: &mut PathMap, source: &str, destination: &str) -> Result<(), VfsError> {
    let source = normalize_path(source)?;
    let destination = normalize_copy_move_destination(records, &source, destination)?;
    ensure_not_root(&source)?;
    ensure_not_root(&destination)?;
    ensure_parent_directory(records, &destination)?;
    let source_record = records
        .get(&source)
        .cloned()
        .ok_or_else(|| VfsError::NotFound {
            path: source.clone(),
        })?;
    if source_record.kind == PathKind::Directory {
        return Err(VfsError::NotSupported {
            message: "durable directory copy is not supported yet".to_string(),
        });
    }
    if records.contains_key(&destination) {
        remove_subtree(records, &destination);
    }

    let prefix = subtree_prefix(&source);
    let copies = records
        .iter()
        .filter(|(path, _)| *path == &source || path.starts_with(&prefix))
        .map(|(path, record)| {
            let suffix = path.strip_prefix(&source).unwrap_or_default();
            let mut next = record.clone();
            next.path = format!("{destination}{suffix}");
            (next.path.clone(), next)
        })
        .collect::<Vec<_>>();
    for (path, record) in copies {
        records.insert(path, record);
    }
    Ok(())
}

fn move_path(records: &mut PathMap, source: &str, destination: &str) -> Result<(), VfsError> {
    let source = normalize_path(source)?;
    let destination = normalize_copy_move_destination(records, &source, destination)?;
    ensure_not_root(&source)?;
    ensure_not_root(&destination)?;
    ensure_parent_directory(records, &destination)?;
    if !records.contains_key(&source) {
        return Err(VfsError::NotFound { path: source });
    }
    if destination.starts_with(&subtree_prefix(&source)) {
        return Err(VfsError::InvalidPath { path: destination });
    }
    if records.contains_key(&destination) {
        remove_subtree(records, &destination);
    }

    let prefix = subtree_prefix(&source);
    let moved = records
        .iter()
        .filter(|(path, _)| *path == &source || path.starts_with(&prefix))
        .map(|(path, record)| {
            let suffix = path.strip_prefix(&source).unwrap_or_default();
            let mut next = record.clone();
            next.path = format!("{destination}{suffix}");
            (path.clone(), next.path.clone(), next)
        })
        .collect::<Vec<_>>();
    for (old_path, _, _) in &moved {
        records.remove(old_path);
    }
    for (_, new_path, record) in moved {
        records.insert(new_path, record);
    }
    Ok(())
}

fn apply_metadata(record: &mut PathRecord, update: MetadataUpdate) -> Result<(), VfsError> {
    if let Some(mime_type) = &update.mime_type {
        if let Some(mime_type) = mime_type {
            validate_mime_type(mime_type)?;
        }
        record.mime_type = mime_type.clone();
    }

    let mut custom_attrs = record.custom_attrs.clone();
    for key in &update.remove_custom_attrs {
        validate_custom_attr_key(key)?;
        custom_attrs.remove(key);
    }
    for (key, value) in update.custom_attrs {
        validate_custom_attr_key(&key)?;
        validate_custom_attr_value(&value)?;
        custom_attrs.insert(key, value);
    }
    validate_custom_attrs(&custom_attrs)?;
    record.custom_attrs = custom_attrs;
    Ok(())
}

async fn write_tree_from_path_records(
    records: &PathMap,
    writer: &mut DurableMutationObjectWriter<'_>,
) -> Result<ObjectId, VfsError> {
    write_tree_at("/", records, writer).await
}

async fn write_tree_at(
    dir_path: &str,
    records: &PathMap,
    writer: &mut DurableMutationObjectWriter<'_>,
) -> Result<ObjectId, VfsError> {
    let mut entries = Vec::new();
    for child in immediate_children(records, dir_path) {
        match child.kind {
            PathKind::File | PathKind::Symlink => {
                let id = child.content_id.ok_or_else(redacted_tree_write_error)?;
                let kind = match child.kind {
                    PathKind::File => TreeEntryKind::Blob,
                    PathKind::Symlink => TreeEntryKind::Symlink,
                    PathKind::Directory => unreachable!("directory handled separately"),
                };
                entries.push(TreeEntry {
                    name: basename(&child.path)?.to_string(),
                    kind,
                    id,
                    mode: child.mode,
                    uid: child.uid,
                    gid: child.gid,
                    mime_type: child.mime_type.clone(),
                    custom_attrs: child.custom_attrs.clone(),
                });
            }
            PathKind::Directory => {
                let child_tree = Box::pin(write_tree_at(&child.path, records, writer)).await?;
                entries.push(TreeEntry {
                    name: basename(&child.path)?.to_string(),
                    kind: TreeEntryKind::Tree,
                    id: child_tree,
                    mode: child.mode,
                    uid: child.uid,
                    gid: child.gid,
                    mime_type: child.mime_type.clone(),
                    custom_attrs: child.custom_attrs.clone(),
                });
            }
        }
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    writer
        .put(ObjectKind::Tree, TreeObject { entries }.serialize())
        .await
        .map_err(|_| redacted_tree_write_error())
}

async fn durable_commit_path_records(
    repo_id: &RepoId,
    commit_id: CommitId,
    commits: &dyn CommitStore,
    objects: &dyn ObjectStore,
) -> Result<PathMap, VfsError> {
    let commit = commits
        .get(repo_id, commit_id)
        .await
        .map_err(|_| redacted_tree_load_error())?
        .ok_or_else(redacted_tree_load_error)?;
    if commit.repo_id != *repo_id || commit.id != commit_id {
        return Err(redacted_tree_load_error());
    }
    let root = load_tree(repo_id, commit.root_tree, objects).await?;
    let mut records = PathMap::new();
    collect_path_records(repo_id, objects, "/", root, &mut records).await?;
    Ok(records)
}

async fn collect_path_records(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    dir_path: &str,
    tree: TreeObject,
    records: &mut PathMap,
) -> Result<(), VfsError> {
    let mut entries = tree.entries;
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    for entry in entries {
        let path = child_path(dir_path, &entry.name);
        match entry.kind {
            TreeEntryKind::Blob => {
                let size = blob_len(repo_id, entry.id, objects).await?;
                insert_path_record(
                    records,
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
                let child_tree = load_tree(repo_id, entry.id, objects).await?;
                let size = child_tree.entries.len() as u64;
                insert_path_record(
                    records,
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
                Box::pin(collect_path_records(
                    repo_id, objects, &path, child_tree, records,
                ))
                .await?;
            }
            TreeEntryKind::Symlink => {
                let size = blob_len(repo_id, entry.id, objects).await?;
                insert_path_record(
                    records,
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
    Ok(())
}

fn mutation_commit_record(
    repo_id: RepoId,
    parent: CommitId,
    root_tree: ObjectId,
    timestamp: u64,
    author: &str,
    changed_paths: Vec<ChangedPath>,
) -> CommitRecord {
    let commit = CommitObject {
        id: ObjectId::from_bytes(&[0; 32]),
        tree: root_tree,
        parent: Some(parent.object_id()),
        timestamp,
        message: DURABLE_MUTATION_COMMIT_MESSAGE.to_string(),
        author: if author.is_empty() {
            DURABLE_MUTATION_AUTHOR.to_string()
        } else {
            author.to_string()
        },
        changed_paths,
    };
    let id = CommitId::from(ObjectId::from_bytes(&commit.serialize()));
    CommitRecord {
        repo_id,
        id,
        root_tree,
        parents: vec![parent],
        timestamp,
        message: commit.message,
        author: commit.author,
        changed_paths: commit.changed_paths,
    }
}

async fn load_tree(
    repo_id: &RepoId,
    tree_id: ObjectId,
    objects: &dyn ObjectStore,
) -> Result<TreeObject, VfsError> {
    let stored = objects
        .get(repo_id, tree_id, ObjectKind::Tree)
        .await
        .map_err(|_| redacted_tree_load_error())?
        .ok_or_else(redacted_tree_load_error)?;
    if stored.repo_id != *repo_id || stored.id != tree_id || stored.kind != ObjectKind::Tree {
        return Err(redacted_tree_load_error());
    }
    TreeObject::deserialize(&stored.bytes).map_err(|_| redacted_tree_load_error())
}

async fn blob_len(
    repo_id: &RepoId,
    blob_id: ObjectId,
    objects: &dyn ObjectStore,
) -> Result<u64, VfsError> {
    objects
        .object_len(repo_id, blob_id, ObjectKind::Blob)
        .await
        .map_err(|_| redacted_tree_load_error())?
        .ok_or_else(redacted_tree_load_error)
}

fn insert_path_record(records: &mut PathMap, record: PathRecord) -> Result<(), VfsError> {
    if records.insert(record.path.clone(), record).is_some() {
        return Err(redacted_tree_load_error());
    }
    Ok(())
}

fn normalize_path(path: &str) -> Result<String, VfsError> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }
    let mut parts = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(VfsError::InvalidPath {
                        path: path.to_string(),
                    });
                }
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn ensure_not_root(path: &str) -> Result<(), VfsError> {
    if path == "/" {
        return Err(VfsError::InvalidPath {
            path: "cannot mutate root".to_string(),
        });
    }
    Ok(())
}

fn ensure_parent_directory(records: &PathMap, path: &str) -> Result<(), VfsError> {
    let parent = parent_path(path)?;
    if parent == "/" {
        return Ok(());
    }
    match records.get(parent) {
        Some(record) if record.kind == PathKind::Directory => Ok(()),
        Some(_) => Err(VfsError::NotDirectory {
            path: parent.to_string(),
        }),
        None => Err(VfsError::NotFound {
            path: parent.to_string(),
        }),
    }
}

fn normalize_copy_move_destination(
    records: &PathMap,
    source: &str,
    destination: &str,
) -> Result<String, VfsError> {
    let destination = normalize_path(destination)?;
    if records
        .get(&destination)
        .is_some_and(|record| record.kind == PathKind::Directory)
    {
        Ok(child_path(&destination, basename(source)?))
    } else {
        Ok(destination)
    }
}

fn parent_path(path: &str) -> Result<&str, VfsError> {
    ensure_not_root(path)?;
    match path.rsplit_once('/') {
        Some(("", _)) => Ok("/"),
        Some((parent, _)) => Ok(parent),
        None => Err(VfsError::InvalidPath {
            path: path.to_string(),
        }),
    }
}

fn basename(path: &str) -> Result<&str, VfsError> {
    ensure_not_root(path)?;
    path.rsplit('/')
        .next()
        .ok_or_else(|| VfsError::InvalidPath {
            path: path.to_string(),
        })
}

fn path_components(path: &str) -> impl Iterator<Item = &str> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
}

fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn subtree_prefix(path: &str) -> String {
    if path == "/" {
        "/".to_string()
    } else {
        format!("{path}/")
    }
}

fn has_children(records: &PathMap, path: &str) -> bool {
    let prefix = subtree_prefix(path);
    records
        .keys()
        .any(|record_path| record_path.starts_with(&prefix))
}

fn require_scoped_path(session: &Session, path: &str, access: Access) -> Result<(), VfsError> {
    if session.is_path_allowed(path, access) {
        Ok(())
    } else {
        Err(VfsError::PermissionDenied {
            path: path.to_string(),
        })
    }
}

fn require_record_access(
    record: &PathRecord,
    session: &Session,
    path: &str,
    access: Access,
) -> Result<(), VfsError> {
    if session.has_permission_bits(record.mode, record.uid, record.gid, access) {
        Ok(())
    } else {
        Err(VfsError::PermissionDenied {
            path: path.to_string(),
        })
    }
}

fn reject_symlink_record(record: &PathRecord) -> Result<(), VfsError> {
    if record.kind == PathKind::Symlink {
        Err(VfsError::NotSupported {
            message: "durable symlink mutation targets are not supported yet".to_string(),
        })
    } else {
        Ok(())
    }
}

fn require_parent_write_execute(
    records: &PathMap,
    path: &str,
    session: &Session,
) -> Result<(), VfsError> {
    let parent = parent_path(path)?;
    require_scoped_path(session, parent, Access::Write)?;
    match records.get(parent) {
        Some(parent_record) if parent_record.kind == PathKind::Directory => {
            require_record_access(parent_record, session, parent, Access::Write)?;
            require_record_access(parent_record, session, parent, Access::Execute)
        }
        Some(_) => Err(VfsError::NotDirectory {
            path: parent.to_string(),
        }),
        None if parent == "/" => {
            if session.has_permission_bits(0o755, ROOT_UID, ROOT_GID, Access::Write)
                && session.has_permission_bits(0o755, ROOT_UID, ROOT_GID, Access::Execute)
            {
                Ok(())
            } else {
                Err(VfsError::PermissionDenied {
                    path: parent.to_string(),
                })
            }
        }
        None => Err(VfsError::NotFound {
            path: parent.to_string(),
        }),
    }
}

fn require_sticky_delete(records: &PathMap, path: &str, session: &Session) -> Result<(), VfsError> {
    let parent = parent_path(path)?;
    let Some(parent_record) = records.get(parent) else {
        return if parent == "/" {
            Ok(())
        } else {
            Err(VfsError::NotFound {
                path: parent.to_string(),
            })
        };
    };
    if has_sticky_bit(parent_record.mode) && !session.is_effectively_root() {
        let child = records
            .get(path)
            .ok_or_else(|| VfsError::NotFound { path: path.into() })?;
        let effective_uid = session.effective_uid();
        if child.uid != effective_uid && parent_record.uid != effective_uid {
            return Err(VfsError::PermissionDenied {
                path: path.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_recursive_delete(
    records: &PathMap,
    path: &str,
    session: &Session,
) -> Result<(), VfsError> {
    let record = records
        .get(path)
        .ok_or_else(|| VfsError::NotFound { path: path.into() })?;
    if record.kind != PathKind::Directory {
        return Ok(());
    }
    require_record_access(record, session, path, Access::Write)?;
    require_record_access(record, session, path, Access::Execute)?;

    let prefix = subtree_prefix(path);
    let children = records
        .keys()
        .filter(|candidate| candidate.starts_with(&prefix))
        .cloned()
        .collect::<Vec<_>>();
    for child in children {
        if parent_path(&child).ok() == Some(path) {
            require_sticky_delete(records, &child, session)?;
            validate_recursive_delete(records, &child, session)?;
        }
    }
    Ok(())
}

fn remove_subtree(records: &mut PathMap, path: &str) {
    let prefix = subtree_prefix(path);
    let removed = records
        .keys()
        .filter(|record_path| *record_path == path || record_path.starts_with(&prefix))
        .cloned()
        .collect::<Vec<_>>();
    for path in removed {
        records.remove(&path);
    }
}

fn immediate_children(records: &PathMap, dir_path: &str) -> Vec<PathRecord> {
    records
        .values()
        .filter(|record| parent_path(&record.path).ok() == Some(dir_path))
        .cloned()
        .collect()
}

fn refresh_directory_sizes(records: &mut PathMap) {
    let counts = records
        .keys()
        .filter_map(|path| parent_path(path).ok().map(str::to_string))
        .fold(BTreeMap::<String, u64>::new(), |mut counts, parent| {
            *counts.entry(parent).or_default() += 1;
            counts
        });
    for record in records.values_mut() {
        if record.kind == PathKind::Directory {
            record.size = counts.get(&record.path).copied().unwrap_or_default();
        }
    }
}

fn validate_custom_attrs(attrs: &BTreeMap<String, String>) -> Result<(), VfsError> {
    if attrs.len() > MAX_CUSTOM_ATTRS {
        return Err(VfsError::InvalidArgs {
            message: format!("custom attributes exceed maximum count {MAX_CUSTOM_ATTRS}"),
        });
    }
    let total =
        attrs
            .iter()
            .try_fold(0usize, |total, (key, value)| -> Result<usize, VfsError> {
                validate_custom_attr_key(key)?;
                validate_custom_attr_value(value)?;
                Ok(total + key.len() + value.len())
            })?;
    if total > MAX_CUSTOM_ATTR_TOTAL_BYTES {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "custom attributes exceed maximum total size {MAX_CUSTOM_ATTR_TOTAL_BYTES} bytes"
            ),
        });
    }
    Ok(())
}

fn validate_custom_attr_key(key: &str) -> Result<(), VfsError> {
    if key.is_empty() || key.len() > MAX_CUSTOM_ATTR_KEY_LEN {
        return Err(VfsError::InvalidArgs {
            message: format!("custom attribute key must be 1-{MAX_CUSTOM_ATTR_KEY_LEN} bytes"),
        });
    }
    if !key
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(VfsError::InvalidArgs {
            message: "custom attribute key contains unsupported characters".to_string(),
        });
    }
    Ok(())
}

fn validate_custom_attr_value(value: &str) -> Result<(), VfsError> {
    if value.len() > MAX_CUSTOM_ATTR_VALUE_LEN {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "custom attribute value must be at most {MAX_CUSTOM_ATTR_VALUE_LEN} bytes"
            ),
        });
    }
    Ok(())
}

fn redact_ref_cas_error(error: VfsError) -> VfsError {
    match error {
        VfsError::InvalidArgs { message } if is_ref_cas_mismatch_message(&message) => {
            VfsError::InvalidArgs {
                message: "ref compare-and-swap mismatch".to_string(),
            }
        }
        _ => redacted_ref_update_error(),
    }
}

fn is_ref_cas_mismatch_message(message: &str) -> bool {
    message == "ref compare-and-swap mismatch"
        || message.starts_with("ref compare-and-swap mismatch:")
}

fn is_ref_cas_mismatch_error(error: &VfsError) -> bool {
    matches!(
        error,
        VfsError::InvalidArgs { message } if is_ref_cas_mismatch_message(message)
    )
}

fn redacted_ref_resolution_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable mutation ref resolution failed".to_string(),
    }
}

fn redacted_ref_update_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable mutation session ref update failed".to_string(),
    }
}

fn redacted_tree_load_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable mutation source tree load failed".to_string(),
    }
}

fn redacted_tree_write_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable mutation tree write failed".to_string(),
    }
}

fn redacted_object_write_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable mutation object write failed".to_string(),
    }
}

fn redacted_commit_insert_error() -> VfsError {
    VfsError::CorruptStore {
        message: "durable mutation commit insert failed".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::auth::{ROOT_GID, ROOT_UID};
    use crate::backend::object_cleanup::{
        InMemoryObjectCleanupClaimStore, ObjectCleanupClaimStore,
    };
    use crate::backend::{
        CommitRecord, LocalMemoryCommitStore, LocalMemoryObjectStore, LocalMemoryRefStore,
        ObjectWrite, RefExpectation, RefRecord, RefUpdate, SourceCheckedRefUpdate, StoredObject,
    };
    use crate::store::ObjectKind;
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::vcs::{MAIN_REF, PathKind};

    fn repo() -> RepoId {
        RepoId::local()
    }

    fn main_ref() -> RefName {
        RefName::new(MAIN_REF).unwrap()
    }

    fn session_ref() -> RefName {
        RefName::session("agent", "session").unwrap()
    }

    fn commit_id(seed: &str) -> CommitId {
        CommitId::from(ObjectId::from_bytes(seed.as_bytes()))
    }

    fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            kind,
            id,
            mode,
            uid: ROOT_UID,
            gid: ROOT_GID,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        }
    }

    async fn put_object(
        objects: &dyn ObjectStore,
        repo_id: &RepoId,
        kind: ObjectKind,
        bytes: Vec<u8>,
    ) -> ObjectId {
        let id = ObjectId::from_bytes(&bytes);
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id,
                kind,
                bytes,
            })
            .await
            .unwrap();
        id
    }

    async fn seed_empty_base(
        refs: &dyn RefStore,
        commits: &dyn CommitStore,
        objects: &dyn ObjectStore,
    ) -> CommitId {
        let repo_id = repo();
        let root_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: Vec::new(),
            }
            .serialize(),
        )
        .await;
        let base_commit = commit_id("base-empty");
        commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: base_commit,
                root_tree,
                parents: Vec::new(),
                timestamp: 1,
                message: "base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        refs.update(RefUpdate {
            repo_id,
            name: main_ref(),
            target: base_commit,
            expectation: RefExpectation::MustNotExist,
        })
        .await
        .unwrap();
        base_commit
    }

    async fn seed_base_with_docs(
        refs: &dyn RefStore,
        commits: &dyn CommitStore,
        objects: &dyn ObjectStore,
    ) -> CommitId {
        let repo_id = repo();
        let readme = put_object(
            objects,
            &repo_id,
            ObjectKind::Blob,
            b"hello docs\n".to_vec(),
        )
        .await;
        let docs_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("readme.txt", TreeEntryKind::Blob, readme, 0o644)],
            }
            .serialize(),
        )
        .await;
        let root_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("docs", TreeEntryKind::Tree, docs_tree, 0o755)],
            }
            .serialize(),
        )
        .await;
        let base_commit = commit_id("base-docs");
        commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: base_commit,
                root_tree,
                parents: Vec::new(),
                timestamp: 1,
                message: "base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        refs.update(RefUpdate {
            repo_id,
            name: main_ref(),
            target: base_commit,
            expectation: RefExpectation::MustNotExist,
        })
        .await
        .unwrap();
        base_commit
    }

    async fn seed_base_with_permission_fixture(
        refs: &dyn RefStore,
        commits: &dyn CommitStore,
        objects: &dyn ObjectStore,
    ) -> CommitId {
        let repo_id = repo();
        let source = put_object(objects, &repo_id, ObjectKind::Blob, b"source\n".to_vec()).await;
        let target = put_object(objects, &repo_id, ObjectKind::Blob, b"target\n".to_vec()).await;
        let other = put_object(objects, &repo_id, ObjectKind::Blob, b"other\n".to_vec()).await;
        let w_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![TreeEntry {
                    name: "target.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: target,
                    mode: 0o666,
                    uid: 2000,
                    gid: ROOT_GID,
                    mime_type: None,
                    custom_attrs: BTreeMap::new(),
                }],
            }
            .serialize(),
        )
        .await;
        let tmp_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![TreeEntry {
                    name: "other.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: other,
                    mode: 0o666,
                    uid: 2000,
                    gid: ROOT_GID,
                    mime_type: None,
                    custom_attrs: BTreeMap::new(),
                }],
            }
            .serialize(),
        )
        .await;
        let root_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("src.txt", TreeEntryKind::Blob, source, 0o644),
                    tree_entry("w", TreeEntryKind::Tree, w_tree, 0o555),
                    tree_entry("tmp", TreeEntryKind::Tree, tmp_tree, 0o1777),
                ],
            }
            .serialize(),
        )
        .await;
        let base_commit = commit_id("base-permissions");
        commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: base_commit,
                root_tree,
                parents: Vec::new(),
                timestamp: 1,
                message: "base".to_string(),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        refs.update(RefUpdate {
            repo_id,
            name: main_ref(),
            target: base_commit,
            expectation: RefExpectation::MustNotExist,
        })
        .await
        .unwrap();
        base_commit
    }

    async fn seed_session_with_symlink_target(
        refs: &dyn RefStore,
        commits: &dyn CommitStore,
        objects: &dyn ObjectStore,
        base_commit: CommitId,
    ) -> CommitId {
        let repo_id = repo();
        let target = put_object(objects, &repo_id, ObjectKind::Blob, b"/safe.txt".to_vec()).await;
        let w_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "target.txt",
                    TreeEntryKind::Symlink,
                    target,
                    0o777,
                )],
            }
            .serialize(),
        )
        .await;
        let root_tree = put_object(
            objects,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("w", TreeEntryKind::Tree, w_tree, 0o777)],
            }
            .serialize(),
        )
        .await;
        let symlink_commit = commit_id("session-symlink");
        commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: symlink_commit,
                root_tree,
                parents: vec![base_commit],
                timestamp: 2,
                message: "symlink".to_string(),
                author: "racer".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        refs.update(RefUpdate {
            repo_id,
            name: session_ref(),
            target: symlink_commit,
            expectation: RefExpectation::MustNotExist,
        })
        .await
        .unwrap();
        symlink_commit
    }

    fn write_file(path: &str, content: &[u8]) -> DurableMutationOperation {
        DurableMutationOperation::WriteFile {
            path: path.to_string(),
            content: content.to_vec(),
            mode: 0o644,
            uid: ROOT_UID,
            gid: ROOT_GID,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        }
    }

    fn mutation_input(operation: DurableMutationOperation) -> DurableMutationInput {
        DurableMutationInput {
            base_ref: main_ref(),
            session_ref: session_ref(),
            operation,
            author: "agent".to_string(),
            timestamp: 2,
            preflight_session: None,
        }
    }

    fn mutation_input_with_session(
        operation: DurableMutationOperation,
        session: Session,
    ) -> DurableMutationInput {
        DurableMutationInput {
            preflight_session: Some(session),
            ..mutation_input(operation)
        }
    }

    fn policy_token(action: PolicyAction, target_ref: &str) -> PolicyDecisionToken {
        PolicyDecisionToken::allow_for_test_with_paths(action, target_ref, ["/notes.txt"])
    }

    struct RecordingRefStore {
        inner: LocalMemoryRefStore,
        source_checked_updates: AtomicUsize,
    }

    impl RecordingRefStore {
        fn new() -> Self {
            Self {
                inner: LocalMemoryRefStore::new(),
                source_checked_updates: AtomicUsize::new(0),
            }
        }

        fn source_checked_updates(&self) -> usize {
            self.source_checked_updates.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RefStore for RecordingRefStore {
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
            self.inner.update(update).await
        }

        async fn update_source_checked(
            &self,
            update: SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            self.source_checked_updates.fetch_add(1, Ordering::SeqCst);
            self.inner.update_source_checked(update).await
        }
    }

    #[tokio::test]
    async fn durable_mutation_requires_policy_token_before_materializing_session_ref() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        let err = engine
            .apply(mutation_input(write_file("/notes.txt", b"durable note\n")))
            .await
            .expect_err("missing policy token must fail before durable mutation");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(refs.source_checked_updates(), 0);
        assert!(
            !objects
                .contains(
                    &repo(),
                    ObjectId::from_bytes(b"durable note\n"),
                    ObjectKind::Blob
                )
                .await
                .unwrap(),
            "missing token should fail before object writes"
        );
    }

    #[tokio::test]
    async fn durable_mutation_rejects_mismatched_policy_token_before_writes() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let token = policy_token(PolicyAction::FsDelete, main_ref().as_str());
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_policy_token(&token);

        let err = engine
            .apply(mutation_input(write_file("/notes.txt", b"durable note\n")))
            .await
            .expect_err("wrong action policy token must fail before durable mutation");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(refs.source_checked_updates(), 0);
        assert!(
            !objects
                .contains(
                    &repo(),
                    ObjectId::from_bytes(b"durable note\n"),
                    ObjectKind::Blob
                )
                .await
                .unwrap(),
            "mismatched token should fail before object writes"
        );
    }

    #[tokio::test]
    async fn durable_mutation_rejects_wrong_repo_policy_token_before_writes() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let token = PolicyDecisionToken::allow_for_test_with_repo(
            RepoId::new("other").unwrap(),
            PolicyAction::FsWrite,
            main_ref().as_str(),
            1,
        );
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_policy_token(&token);

        let err = engine
            .apply(mutation_input(write_file("/notes.txt", b"durable note\n")))
            .await
            .expect_err("wrong repo policy token must fail before durable mutation");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(refs.source_checked_updates(), 0);
        assert!(
            !objects
                .contains(
                    &repo(),
                    ObjectId::from_bytes(b"durable note\n"),
                    ObjectKind::Blob
                )
                .await
                .unwrap(),
            "wrong repo token should fail before object writes"
        );
    }

    #[tokio::test]
    async fn durable_mutation_rejects_wrong_path_policy_token_before_writes() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let token = PolicyDecisionToken::allow_for_test_with_paths(
            PolicyAction::FsWrite,
            main_ref().as_str(),
            ["/other.txt"],
        );
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_policy_token(&token);

        let err = engine
            .apply(mutation_input(write_file("/notes.txt", b"durable note\n")))
            .await
            .expect_err("wrong path policy token must fail before durable mutation");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(refs.source_checked_updates(), 0);
        assert!(
            !objects
                .contains(
                    &repo(),
                    ObjectId::from_bytes(b"durable note\n"),
                    ObjectKind::Blob
                )
                .await
                .unwrap(),
            "wrong path token should fail before object writes"
        );
    }

    #[tokio::test]
    async fn durable_copy_into_directory_requires_effective_child_path_policy() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_base_with_docs(&refs, &commits, &objects).await;
        let repo_id = repo();
        let token = PolicyDecisionToken::allow_for_test_with_paths(
            PolicyAction::FsCopy,
            main_ref().as_str(),
            ["/"],
        );
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_policy_token(&token);

        let err = engine
            .apply(mutation_input(DurableMutationOperation::Copy {
                source: "/docs/readme.txt".to_string(),
                destination: "/docs".to_string(),
            }))
            .await
            .expect_err("directory copy destination must bind the effective child path");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(refs.source_checked_updates(), 0);
        assert!(
            refs.get(&repo(), &session_ref()).await.unwrap().is_none(),
            "policy rejection must not materialize the session ref"
        );
    }

    #[tokio::test]
    async fn durable_directory_move_requires_descendant_policy_token() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_base_with_docs(&refs, &commits, &objects).await;
        let repo_id = repo();
        let token = PolicyDecisionToken::allow_for_test_with_paths(
            PolicyAction::FsMove,
            main_ref().as_str(),
            ["/docs", "/moved-docs"],
        );
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_policy_token(&token);

        let err = engine
            .apply(mutation_input(DurableMutationOperation::Move {
                source: "/docs".to_string(),
                destination: "/moved-docs".to_string(),
            }))
            .await
            .expect_err(
                "directory move must require descendant-aware source and destination policy",
            );

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(refs.source_checked_updates(), 0);
        assert!(
            refs.get(&repo(), &session_ref()).await.unwrap().is_none(),
            "policy rejection must not materialize the session ref"
        );
    }

    #[tokio::test]
    async fn write_file_creates_session_ref_from_base_with_source_check() {
        let refs = RecordingRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        let base_commit = seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        let output = engine
            .apply_with_test_policy(mutation_input(write_file("/notes.txt", b"durable note\n")))
            .await
            .unwrap();

        assert_eq!(output.previous_commit, base_commit);
        assert_ne!(output.new_commit, base_commit);
        assert_eq!(refs.source_checked_updates(), 1);
        let session = refs.get(&repo(), &session_ref()).await.unwrap().unwrap();
        assert_eq!(session.target, output.new_commit);
        let base = refs.get(&repo(), &main_ref()).await.unwrap().unwrap();
        assert_eq!(base.target, base_commit);
        assert_eq!(output.changed_paths.len(), 1);
        assert_eq!(output.changed_paths[0].path, "/notes.txt");
    }

    #[tokio::test]
    async fn mkdir_delete_copy_move_and_metadata_update_session_tree() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_base_with_docs(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        engine
            .apply_with_test_policy(mutation_input(DurableMutationOperation::Mkdir {
                path: "/scratch".to_string(),
                mode: 0o755,
                uid: ROOT_UID,
                gid: ROOT_GID,
            }))
            .await
            .unwrap();
        engine
            .apply_with_test_policy(mutation_input(DurableMutationOperation::Copy {
                source: "/docs/readme.txt".to_string(),
                destination: "/scratch/copy.txt".to_string(),
            }))
            .await
            .unwrap();
        engine
            .apply_with_test_policy(mutation_input(DurableMutationOperation::Move {
                source: "/scratch/copy.txt".to_string(),
                destination: "/docs/moved.txt".to_string(),
            }))
            .await
            .unwrap();
        let metadata = MetadataUpdate {
            mime_type: Some(Some("text/plain".to_string())),
            custom_attrs: BTreeMap::from([("reviewed".to_string(), "true".to_string())]),
            remove_custom_attrs: Vec::new(),
        };
        engine
            .apply_with_test_policy(mutation_input(DurableMutationOperation::SetMetadata {
                path: "/docs/moved.txt".to_string(),
                update: metadata,
            }))
            .await
            .unwrap();
        let output = engine
            .apply_with_test_policy(mutation_input(DurableMutationOperation::Delete {
                path: "/docs/readme.txt".to_string(),
                recursive: false,
            }))
            .await
            .unwrap();

        let records = durable_commit_path_records(&repo(), output.new_commit, &commits, &objects)
            .await
            .unwrap();
        let moved = records.get("/docs/moved.txt").unwrap();
        assert_eq!(moved.kind, PathKind::File);
        assert_eq!(moved.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(
            moved.custom_attrs.get("reviewed").map(String::as_str),
            Some("true")
        );
        assert!(records.contains_key("/scratch"));
        assert!(!records.contains_key("/docs/readme.txt"));
        assert!(!records.contains_key("/scratch/copy.txt"));
    }

    #[tokio::test]
    async fn durable_copy_rejects_directory_source() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_base_with_docs(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        let err = engine
            .apply_with_test_policy(mutation_input(DurableMutationOperation::Copy {
                source: "/docs".to_string(),
                destination: "/docs-copy".to_string(),
            }))
            .await
            .expect_err("durable directory copy should match the local fail-closed API");

        assert!(matches!(err, VfsError::NotSupported { .. }));
    }

    struct RacingRefStore {
        inner: LocalMemoryRefStore,
        raced: AtomicBool,
        racing_target: CommitId,
    }

    #[async_trait]
    impl RefStore for RacingRefStore {
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
            if update.name == session_ref() && !self.raced.swap(true, Ordering::SeqCst) {
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

    #[tokio::test]
    async fn stale_session_ref_update_is_fenced() {
        let refs = RacingRefStore {
            inner: LocalMemoryRefStore::new(),
            raced: AtomicBool::new(false),
            racing_target: commit_id("racer"),
        };
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        let base_commit = seed_empty_base(&refs, &commits, &objects).await;
        let empty_tree = commits
            .get(&repo(), base_commit)
            .await
            .unwrap()
            .unwrap()
            .root_tree;
        commits
            .insert(CommitRecord {
                repo_id: repo(),
                id: refs.racing_target,
                root_tree: empty_tree,
                parents: vec![base_commit],
                timestamp: 2,
                message: "racer".to_string(),
                author: "racer".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        refs.update_source_checked(SourceCheckedRefUpdate {
            repo_id: repo(),
            source_name: main_ref(),
            source_expectation: RefExpectation::Matches {
                target: base_commit,
                version: refs
                    .get(&repo(), &main_ref())
                    .await
                    .unwrap()
                    .unwrap()
                    .version,
            },
            target_update: RefUpdate {
                repo_id: repo(),
                name: session_ref(),
                target: base_commit,
                expectation: RefExpectation::MustNotExist,
            },
        })
        .await
        .unwrap();
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        let err = engine
            .apply_with_test_policy(mutation_input(write_file("/race.txt", b"loses\n")))
            .await
            .expect_err("stale ref update must be fenced");

        assert!(err.to_string().contains("ref compare-and-swap mismatch"));
        let session = refs.get(&repo(), &session_ref()).await.unwrap().unwrap();
        assert_eq!(session.target, refs.racing_target);
    }

    #[tokio::test]
    async fn stale_session_ref_update_claims_object_cleanup_candidates() {
        let refs = RacingRefStore {
            inner: LocalMemoryRefStore::new(),
            raced: AtomicBool::new(false),
            racing_target: commit_id("racer-cleanup"),
        };
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        let cleanup_claims = InMemoryObjectCleanupClaimStore::new();
        let base_commit = seed_empty_base(&refs, &commits, &objects).await;
        let empty_tree = commits
            .get(&repo(), base_commit)
            .await
            .unwrap()
            .unwrap()
            .root_tree;
        commits
            .insert(CommitRecord {
                repo_id: repo(),
                id: refs.racing_target,
                root_tree: empty_tree,
                parents: vec![base_commit],
                timestamp: 2,
                message: "racer".to_string(),
                author: "racer".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        refs.update_source_checked(SourceCheckedRefUpdate {
            repo_id: repo(),
            source_name: main_ref(),
            source_expectation: RefExpectation::Matches {
                target: base_commit,
                version: refs
                    .get(&repo(), &main_ref())
                    .await
                    .unwrap()
                    .unwrap()
                    .version,
            },
            target_update: RefUpdate {
                repo_id: repo(),
                name: session_ref(),
                target: base_commit,
                expectation: RefExpectation::MustNotExist,
            },
        })
        .await
        .unwrap();
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_cleanup_claims(&cleanup_claims);

        let err = engine
            .apply_with_test_policy(mutation_input(write_file("/race-cleanup.txt", b"loses\n")))
            .await
            .expect_err("stale ref update must be fenced");

        assert!(err.to_string().contains("ref compare-and-swap mismatch"));
        let blob_id = ObjectId::from_bytes(b"loses\n");
        let duplicate = cleanup_claims
            .claim(ObjectCleanupClaimRequest {
                repo_id: repo(),
                claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                object_kind: ObjectKind::Blob,
                object_id: blob_id,
                object_key: canonical_final_object_key(&repo(), ObjectKind::Blob, &blob_id),
                lease_owner: "test-cleanup".to_string(),
                lease_duration: Duration::from_secs(60),
            })
            .await
            .unwrap();
        assert!(
            duplicate.is_none(),
            "CAS-lost blob cleanup claim should already be active"
        );
    }

    #[tokio::test]
    async fn durable_preflight_rejects_symlink_in_observed_session_snapshot() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        let base_commit = seed_empty_base(&refs, &commits, &objects).await;
        let symlink_commit =
            seed_session_with_symlink_target(&refs, &commits, &objects, base_commit).await;
        let cleanup_claims = InMemoryObjectCleanupClaimStore::new();
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_cleanup_claims(&cleanup_claims);

        let err = engine
            .apply_with_test_policy(mutation_input_with_session(
                write_file("/w/target.txt", b"must not follow\n"),
                Session::root(),
            ))
            .await
            .expect_err("engine-local preflight must reject symlink target");

        assert!(matches!(err, VfsError::NotSupported { .. }));
        let session = refs.get(&repo(), &session_ref()).await.unwrap().unwrap();
        assert_eq!(session.target, symlink_commit);
    }

    #[tokio::test]
    async fn preflighted_mutation_requires_cleanup_claim_store_before_writes() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        let err = engine
            .apply_with_test_policy(mutation_input_with_session(
                write_file("/requires-cleanup-store.txt", b"content\n"),
                Session::root(),
            ))
            .await
            .expect_err("preflighted durable routes need recoverable cleanup claims");

        assert!(matches!(err, VfsError::CorruptStore { .. }));
        assert!(
            !objects
                .contains(
                    &repo(),
                    ObjectId::from_bytes(b"content\n"),
                    ObjectKind::Blob
                )
                .await
                .unwrap(),
            "missing cleanup claim store should fail before object writes"
        );
    }

    #[tokio::test]
    async fn durable_copy_replacement_requires_destination_parent_write_execute() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        let cleanup_claims = InMemoryObjectCleanupClaimStore::new();
        seed_base_with_permission_fixture(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_cleanup_claims(&cleanup_claims);
        let user = Session::new(1000, ROOT_GID, vec![ROOT_GID], "user".to_string());

        let err = engine
            .apply_with_test_policy(mutation_input_with_session(
                DurableMutationOperation::Copy {
                    source: "/src.txt".to_string(),
                    destination: "/w/target.txt".to_string(),
                },
                user,
            ))
            .await
            .expect_err("destination parent without write must fail closed");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn durable_delete_and_move_enforce_sticky_directory_ownership() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = LocalMemoryObjectStore::new();
        let cleanup_claims = InMemoryObjectCleanupClaimStore::new();
        seed_base_with_permission_fixture(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects)
            .with_cleanup_claims(&cleanup_claims);
        let user = Session::new(1000, ROOT_GID, vec![ROOT_GID], "user".to_string());

        let delete_err = engine
            .apply_with_test_policy(mutation_input_with_session(
                DurableMutationOperation::Delete {
                    path: "/tmp/other.txt".to_string(),
                    recursive: false,
                },
                user.clone(),
            ))
            .await
            .expect_err("sticky directory should block deleting another user's file");
        let move_err = engine
            .apply_with_test_policy(mutation_input_with_session(
                DurableMutationOperation::Move {
                    source: "/tmp/other.txt".to_string(),
                    destination: "/tmp/new.txt".to_string(),
                },
                user,
            ))
            .await
            .expect_err("sticky directory should block moving another user's file");

        assert!(matches!(delete_err, VfsError::PermissionDenied { .. }));
        assert!(matches!(move_err, VfsError::PermissionDenied { .. }));
    }

    struct FailingObjectStore {
        inner: LocalMemoryObjectStore,
    }

    #[async_trait]
    impl ObjectStore for FailingObjectStore {
        async fn put(&self, write: ObjectWrite) -> Result<StoredObject, VfsError> {
            if write.kind == ObjectKind::Blob {
                return Err(VfsError::CorruptStore {
                    message: "raw sql postgres://secret@example object key auth-token".to_string(),
                });
            }
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
            repo_id: &RepoId,
            id: ObjectId,
            expected_kind: ObjectKind,
        ) -> Result<bool, VfsError> {
            self.inner.contains(repo_id, id, expected_kind).await
        }
    }

    #[tokio::test]
    async fn mutation_errors_are_redacted() {
        let refs = LocalMemoryRefStore::new();
        let commits = LocalMemoryCommitStore::new();
        let objects = FailingObjectStore {
            inner: LocalMemoryObjectStore::new(),
        };
        seed_empty_base(&refs, &commits, &objects).await;
        let repo_id = repo();
        let engine = DurableMutationEngine::new(&repo_id, &refs, &commits, &objects);

        let err = engine
            .apply_with_test_policy(mutation_input(write_file("/secret.txt", b"will fail\n")))
            .await
            .expect_err("object failure should be redacted");
        let rendered = err.to_string();

        assert!(rendered.contains("durable mutation"));
        assert!(!rendered.contains("postgres://secret@example"));
        assert!(!rendered.contains("auth-token"));
        assert!(!rendered.contains("raw sql"));
    }

    #[test]
    fn durable_mutation_debug_redacts_request_body_values() {
        let input = mutation_input(DurableMutationOperation::WriteFile {
            path: "/secret.txt".to_string(),
            content: b"super secret token".to_vec(),
            mode: 0o644,
            uid: ROOT_UID,
            gid: ROOT_GID,
            mime_type: None,
            custom_attrs: BTreeMap::from([(
                "token".to_string(),
                "custom secret value".to_string(),
            )]),
        });
        let metadata = DurableMutationOperation::SetMetadata {
            path: "/secret.txt".to_string(),
            update: MetadataUpdate {
                mime_type: None,
                custom_attrs: BTreeMap::from([(
                    "metadata-token".to_string(),
                    "metadata secret value".to_string(),
                )]),
                remove_custom_attrs: Vec::new(),
            },
        };

        let rendered = format!("{input:?}");
        let metadata_rendered = format!("{metadata:?}");

        assert!(rendered.contains("<redacted:18 bytes>"));
        assert!(rendered.contains("<redacted:19 bytes>"));
        assert!(!rendered.contains("super secret token"));
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("custom secret value"));
        assert!(metadata_rendered.contains("<redacted:21 bytes>"));
        assert!(!metadata_rendered.contains("metadata-token"));
        assert!(!metadata_rendered.contains("metadata secret value"));
    }
}
