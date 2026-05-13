//! Cleanup claim contracts for durable object repair workers.
//!
//! Claims are leases around object-cleanup work. They coordinate workers, but
//! they are not a distributed transaction with object storage. Final object
//! deletion must stay behind a stronger metadata fencing contract.

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::backend::blob_object::{FinalObjectMetadataFenceRequest, ObjectMetadataStore};
use crate::backend::core_transaction::{
    DurableCorePostCasRecoveryClaimStore, DurableCorePreVisibilityRecoveryStore,
    DurableFsMutationRecoveryStore,
};
use crate::backend::{CommitStore, ObjectStore, RefStore, RepoId};
use crate::error::VfsError;
use crate::idempotency::IdempotencyStore;
use crate::review::ReviewStore;
use crate::store::tree::{TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{CommitId, RefName};
use crate::workspace::WorkspaceMetadataStore;

const STALE_CLEANUP_CLAIM_MESSAGE: &str = "cleanup claim lease token is stale";
const GC_ROOT_SCAN_LIMIT: usize = 1_000;
const GC_IDEMPOTENCY_JSON_NODE_LIMIT: usize = 10_000;
const GC_IDEMPOTENCY_JSON_DEPTH_LIMIT: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectCleanupClaimKind {
    FinalObjectMetadataRepair,
    DurableMutationCasLostObjectCleanup,
}

pub(crate) struct ObjectGcDryRun<'a> {
    objects: &'a dyn ObjectStore,
    commits: &'a dyn CommitStore,
    refs: &'a dyn RefStore,
    workspaces: &'a dyn WorkspaceMetadataStore,
    reviews: &'a dyn ReviewStore,
    idempotency: &'a dyn IdempotencyStore,
    post_cas_recovery: &'a dyn DurableCorePostCasRecoveryClaimStore,
    pre_visibility_recovery: &'a dyn DurableCorePreVisibilityRecoveryStore,
    fs_mutation_recovery: &'a dyn DurableFsMutationRecoveryStore,
    cleanup_claims: &'a dyn ObjectCleanupClaimStore,
}

impl<'a> ObjectGcDryRun<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        objects: &'a dyn ObjectStore,
        commits: &'a dyn CommitStore,
        refs: &'a dyn RefStore,
        workspaces: &'a dyn WorkspaceMetadataStore,
        reviews: &'a dyn ReviewStore,
        idempotency: &'a dyn IdempotencyStore,
        post_cas_recovery: &'a dyn DurableCorePostCasRecoveryClaimStore,
        pre_visibility_recovery: &'a dyn DurableCorePreVisibilityRecoveryStore,
        fs_mutation_recovery: &'a dyn DurableFsMutationRecoveryStore,
        cleanup_claims: &'a dyn ObjectCleanupClaimStore,
    ) -> Self {
        Self {
            objects,
            commits,
            refs,
            workspaces,
            reviews,
            idempotency,
            post_cas_recovery,
            pre_visibility_recovery,
            fs_mutation_recovery,
            cleanup_claims,
        }
    }

    pub(crate) async fn run(
        &self,
        repo_id: &RepoId,
        limit: usize,
        current_cleanup_claim: Option<&ObjectCleanupClaim>,
    ) -> Result<ObjectGcDryRunReport, VfsError> {
        if limit == 0 {
            return Ok(ObjectGcDryRunReport::default());
        }

        let mut roots = ObjectGcRoots::default();
        let mut blockers = Vec::new();
        self.collect_roots(repo_id, &mut roots, &mut blockers, current_cleanup_claim)
            .await;
        let root_collection_blocked = !blockers.is_empty();

        let mut reachable_commits = BTreeSet::new();
        let mut reachable_objects = BTreeSet::new();
        let commit_walk_complete = self
            .walk_commits_and_trees(
                repo_id,
                &roots.commit_roots,
                &mut reachable_commits,
                &mut reachable_objects,
                &mut blockers,
            )
            .await;
        reachable_objects.extend(
            reachable_commits
                .iter()
                .map(|commit_id| GcObjectRef::new(ObjectKind::Commit, ObjectId::from(*commit_id))),
        );
        reachable_objects.extend(roots.object_roots.iter().copied());

        let all_commits = match self.commits.list_bounded(repo_id, limit).await {
            Ok(commits) => commits,
            Err(_) => {
                blockers.push(ObjectGcBlockerSummary::new("commits", "list_failed"));
                Vec::new()
            }
        };

        let mut unreachable_commits = Vec::new();
        if !root_collection_blocked && commit_walk_complete {
            for commit in all_commits {
                if !reachable_commits.contains(&commit.id) {
                    unreachable_commits.push(UnreachableCommitCandidate::new(&commit));
                    if unreachable_commits.len() == limit {
                        break;
                    }
                }
            }
        }

        let mut unreachable_objects = Vec::new();
        let mut unreachable_object_refs = BTreeSet::new();
        if blockers.is_empty() {
            for object_ref in roots.object_candidates.iter().copied() {
                if !reachable_objects.contains(&object_ref) {
                    unreachable_object_refs.insert(object_ref);
                    unreachable_objects.push(UnreachableObjectCandidate::new(
                        object_ref.kind,
                        object_ref.id,
                    ));
                    if unreachable_objects.len() == limit {
                        break;
                    }
                }
            }
        }

        Ok(ObjectGcDryRunReport {
            roots,
            unreachable_commits,
            unreachable_objects,
            unreachable_object_refs,
            blockers: blockers.into_iter().take(limit).collect(),
        })
    }

    async fn collect_roots(
        &self,
        repo_id: &RepoId,
        roots: &mut ObjectGcRoots,
        blockers: &mut Vec<ObjectGcBlockerSummary>,
        current_cleanup_claim: Option<&ObjectCleanupClaim>,
    ) {
        match self.refs.list(repo_id).await {
            Ok(refs) => {
                if refs.len() >= GC_ROOT_SCAN_LIMIT {
                    push_scan_limit_blocker(blockers, "refs", GC_ROOT_SCAN_LIMIT);
                }
                roots.commit_roots.extend(
                    refs.into_iter()
                        .take(GC_ROOT_SCAN_LIMIT)
                        .map(|record| record.target),
                );
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("refs", "list_failed")),
        }

        match self.workspaces.list_workspaces_for_repo(repo_id).await {
            Ok(workspaces) => {
                if workspaces.len() >= GC_ROOT_SCAN_LIMIT {
                    push_scan_limit_blocker(blockers, "workspaces", GC_ROOT_SCAN_LIMIT);
                }
                for workspace in workspaces.into_iter().take(GC_ROOT_SCAN_LIMIT) {
                    if let Some(head_commit) = workspace.head_commit.as_deref() {
                        insert_commit_root_from_hex(
                            roots,
                            blockers,
                            "workspace_heads",
                            head_commit,
                        );
                    }
                    for ref_name in [
                        Some(workspace.base_ref.as_str()),
                        workspace.session_ref.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        match RefName::new(ref_name) {
                            Ok(name) => match self.refs.get(repo_id, &name).await {
                                Ok(Some(record)) => {
                                    roots.commit_roots.insert(record.target);
                                }
                                Ok(None) => {}
                                Err(_) => blockers.push(ObjectGcBlockerSummary::new(
                                    "workspace_refs",
                                    "read_failed",
                                )),
                            },
                            Err(_) => blockers
                                .push(ObjectGcBlockerSummary::new("workspace_refs", "invalid_ref")),
                        }
                    }
                }
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("workspaces", "list_failed")),
        }

        match self.reviews.list_change_requests_for_repo(repo_id).await {
            Ok(changes) => {
                if changes.len() >= GC_ROOT_SCAN_LIMIT {
                    push_scan_limit_blocker(blockers, "reviews", GC_ROOT_SCAN_LIMIT);
                }
                for change in changes.into_iter().take(GC_ROOT_SCAN_LIMIT) {
                    insert_commit_root_from_hex(roots, blockers, "reviews", &change.base_commit);
                    insert_commit_root_from_hex(roots, blockers, "reviews", &change.head_commit);
                }
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("reviews", "list_failed")),
        }

        match self.post_cas_recovery.list(GC_ROOT_SCAN_LIMIT).await {
            Ok(statuses) => {
                push_scan_limit_blocker(blockers, "post_cas", statuses.len());
                roots.commit_roots.extend(
                    statuses
                        .into_iter()
                        .filter(|status| status.target().repo_id() == repo_id)
                        .map(|status| status.target().commit_id()),
                );
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("post_cas", "list_failed")),
        }

        match self.pre_visibility_recovery.list(GC_ROOT_SCAN_LIMIT).await {
            Ok(statuses) => {
                push_scan_limit_blocker(blockers, "pre_visibility", statuses.len());
                roots.commit_roots.extend(
                    statuses
                        .into_iter()
                        .filter(|status| status.target().repo_id() == repo_id)
                        .map(|status| status.target().commit_id()),
                );
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("pre_visibility", "list_failed")),
        }

        match self.fs_mutation_recovery.list(GC_ROOT_SCAN_LIMIT).await {
            Ok(statuses) => {
                push_scan_limit_blocker(blockers, "fs_mutation", statuses.len());
                for status in statuses
                    .into_iter()
                    .filter(|status| status.target().repo_id() == repo_id)
                {
                    roots.commit_roots.insert(status.target().previous_commit());
                    roots.commit_roots.insert(status.target().new_commit());
                }
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("fs_mutation", "list_failed")),
        }

        match self
            .idempotency
            .list_retained_for_repo(repo_id, GC_ROOT_SCAN_LIMIT)
            .await
        {
            Ok(records) => {
                push_scan_limit_blocker(blockers, "idempotency", records.len());
                for record in records {
                    if record.pending {
                        blockers.push(ObjectGcBlockerSummary::new(
                            "idempotency",
                            "pending_repo_record",
                        ));
                        continue;
                    }
                    if let Some(body) = &record.response_body
                        && !collect_commit_ids_from_json(body, &mut roots.commit_roots)
                    {
                        blockers.push(ObjectGcBlockerSummary::new(
                            "idempotency",
                            "scan_limit_reached",
                        ));
                    }
                }
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("idempotency", "list_failed")),
        }

        match self
            .cleanup_claims
            .list_for_repo(repo_id, GC_ROOT_SCAN_LIMIT)
            .await
        {
            Ok(statuses) => {
                push_scan_limit_blocker(blockers, "cleanup_claims", statuses.len());
                for status in statuses {
                    let target = GcObjectRef::new(status.object_kind(), status.object_id());
                    roots.object_candidates.insert(target);
                    if status.state() == ObjectCleanupClaimState::Active
                        && !cleanup_status_matches_claim(&status, current_cleanup_claim)
                    {
                        roots.object_roots.insert(target);
                    }
                }
            }
            Err(_) => blockers.push(ObjectGcBlockerSummary::new("cleanup_claims", "list_failed")),
        }
    }

    async fn walk_commits_and_trees(
        &self,
        repo_id: &RepoId,
        commit_roots: &BTreeSet<CommitId>,
        reachable_commits: &mut BTreeSet<CommitId>,
        reachable_objects: &mut BTreeSet<GcObjectRef>,
        blockers: &mut Vec<ObjectGcBlockerSummary>,
    ) -> bool {
        let mut complete = true;
        let mut walked_commits = 0usize;
        let mut tree_walk_budget = GC_ROOT_SCAN_LIMIT;
        let mut queue: VecDeque<CommitId> = commit_roots.iter().copied().collect();
        while let Some(commit_id) = queue.pop_front() {
            if reachable_commits.contains(&commit_id) {
                continue;
            }
            if walked_commits == GC_ROOT_SCAN_LIMIT {
                blockers.push(ObjectGcBlockerSummary::new(
                    "commit_walk",
                    "scan_limit_reached",
                ));
                complete = false;
                break;
            }
            reachable_commits.insert(commit_id);
            walked_commits += 1;
            match self.commits.get(repo_id, commit_id).await {
                Ok(Some(commit)) => {
                    queue.extend(commit.parents.iter().copied());
                    self.walk_tree(
                        repo_id,
                        commit.root_tree,
                        reachable_objects,
                        blockers,
                        &mut tree_walk_budget,
                    )
                    .await;
                }
                Ok(None) => {
                    blockers.push(ObjectGcBlockerSummary::new("commit_walk", "missing"));
                    complete = false;
                }
                Err(_) => {
                    blockers.push(ObjectGcBlockerSummary::new("commit_walk", "read_failed"));
                    complete = false;
                }
            }
        }
        complete
    }

    async fn walk_tree(
        &self,
        repo_id: &RepoId,
        root_tree: ObjectId,
        reachable_objects: &mut BTreeSet<GcObjectRef>,
        blockers: &mut Vec<ObjectGcBlockerSummary>,
        tree_walk_budget: &mut usize,
    ) {
        let mut queue = VecDeque::from([root_tree]);
        while let Some(tree_id) = queue.pop_front() {
            if reachable_objects.contains(&GcObjectRef::new(ObjectKind::Tree, tree_id)) {
                continue;
            }
            if *tree_walk_budget == 0 {
                blockers.push(ObjectGcBlockerSummary::new(
                    "tree_walk",
                    "scan_limit_reached",
                ));
                return;
            }
            *tree_walk_budget -= 1;
            reachable_objects.insert(GcObjectRef::new(ObjectKind::Tree, tree_id));
            let Some(stored) = (match self.objects.get(repo_id, tree_id, ObjectKind::Tree).await {
                Ok(stored) => stored,
                Err(_) => {
                    blockers.push(ObjectGcBlockerSummary::new("tree_walk", "read_failed"));
                    continue;
                }
            }) else {
                blockers.push(ObjectGcBlockerSummary::new("tree_walk", "missing"));
                continue;
            };
            let tree = match TreeObject::deserialize(&stored.bytes) {
                Ok(tree) => tree,
                Err(_) => {
                    blockers.push(ObjectGcBlockerSummary::new("tree_walk", "decode_failed"));
                    continue;
                }
            };
            for entry in tree.entries {
                match entry.kind {
                    TreeEntryKind::Tree => queue.push_back(entry.id),
                    TreeEntryKind::Blob | TreeEntryKind::Symlink => {
                        reachable_objects.insert(GcObjectRef::new(ObjectKind::Blob, entry.id));
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GcObjectRef {
    kind: ObjectKind,
    id: ObjectId,
}

impl GcObjectRef {
    const fn new(kind: ObjectKind, id: ObjectId) -> Self {
        Self { kind, id }
    }
}

impl PartialOrd for GcObjectRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GcObjectRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        object_kind_rank(self.kind)
            .cmp(&object_kind_rank(other.kind))
            .then_with(|| self.id.cmp(&other.id))
    }
}

const fn object_kind_rank(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Blob => 0,
        ObjectKind::Tree => 1,
        ObjectKind::Commit => 2,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectGcRoots {
    commit_roots: BTreeSet<CommitId>,
    object_roots: BTreeSet<GcObjectRef>,
    object_candidates: BTreeSet<GcObjectRef>,
}

impl ObjectGcRoots {
    pub fn commit_root_count(&self) -> usize {
        self.commit_roots.len()
    }

    pub fn object_root_count(&self) -> usize {
        self.object_roots.len()
    }

    pub fn cleanup_candidate_count(&self) -> usize {
        self.object_candidates.len()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectGcDryRunReport {
    pub roots: ObjectGcRoots,
    pub unreachable_commits: Vec<UnreachableCommitCandidate>,
    pub unreachable_objects: Vec<UnreachableObjectCandidate>,
    unreachable_object_refs: BTreeSet<GcObjectRef>,
    pub blockers: Vec<ObjectGcBlockerSummary>,
}

impl ObjectGcDryRunReport {
    #[cfg_attr(not(test), allow(dead_code))]
    fn contains_unreachable_object(&self, object_kind: ObjectKind, object_id: ObjectId) -> bool {
        self.unreachable_object_refs
            .contains(&GcObjectRef::new(object_kind, object_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableCommitCandidate {
    pub commit_id_prefix: String,
    pub root_tree_prefix: String,
    pub parent_count: usize,
    pub changed_path_count: usize,
}

impl UnreachableCommitCandidate {
    fn new(commit: &crate::backend::CommitRecord) -> Self {
        Self {
            commit_id_prefix: commit.id.short_hex(),
            root_tree_prefix: commit.root_tree.short_hex(),
            parent_count: commit.parents.len(),
            changed_path_count: commit.changed_paths.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableObjectCandidate {
    pub object_kind: ObjectKind,
    pub object_id_prefix: String,
}

impl UnreachableObjectCandidate {
    fn new(object_kind: ObjectKind, object_id: ObjectId) -> Self {
        Self {
            object_kind,
            object_id_prefix: object_id.short_hex(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectGcBlockerSummary {
    pub source: &'static str,
    pub reason: &'static str,
}

impl ObjectGcBlockerSummary {
    const fn new(source: &'static str, reason: &'static str) -> Self {
        Self { source, reason }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectCleanupWorkerSummary {
    pub candidates_listed: usize,
    pub processed: usize,
    pub skipped_non_cas_lost: usize,
    pub skipped_reachable: usize,
    pub skipped_blocked: usize,
    pub skipped_claim_unavailable: usize,
    pub deletion_ready: usize,
    pub deleted_final_objects: usize,
    pub retryable_failures: usize,
    pub poisoned: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ObjectCleanupWorker<'a> {
    repo_id: &'a RepoId,
    objects: &'a dyn ObjectStore,
    metadata: &'a dyn ObjectMetadataStore,
    commits: &'a dyn CommitStore,
    refs: &'a dyn RefStore,
    workspaces: &'a dyn WorkspaceMetadataStore,
    reviews: &'a dyn ReviewStore,
    idempotency: &'a dyn IdempotencyStore,
    post_cas_recovery: &'a dyn DurableCorePostCasRecoveryClaimStore,
    pre_visibility_recovery: &'a dyn DurableCorePreVisibilityRecoveryStore,
    fs_mutation_recovery: &'a dyn DurableFsMutationRecoveryStore,
    cleanup_claims: &'a dyn ObjectCleanupClaimStore,
    lease_owner: &'static str,
    lease_duration: Duration,
    fence_owner: &'static str,
    fence_duration: Duration,
}

#[cfg_attr(not(test), allow(dead_code))]
impl<'a> ObjectCleanupWorker<'a> {
    pub(crate) const MAX_ATTEMPTS: u64 = 3;

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        repo_id: &'a RepoId,
        objects: &'a dyn ObjectStore,
        metadata: &'a dyn ObjectMetadataStore,
        commits: &'a dyn CommitStore,
        refs: &'a dyn RefStore,
        workspaces: &'a dyn WorkspaceMetadataStore,
        reviews: &'a dyn ReviewStore,
        idempotency: &'a dyn IdempotencyStore,
        post_cas_recovery: &'a dyn DurableCorePostCasRecoveryClaimStore,
        pre_visibility_recovery: &'a dyn DurableCorePreVisibilityRecoveryStore,
        fs_mutation_recovery: &'a dyn DurableFsMutationRecoveryStore,
        cleanup_claims: &'a dyn ObjectCleanupClaimStore,
    ) -> Self {
        Self {
            repo_id,
            objects,
            metadata,
            commits,
            refs,
            workspaces,
            reviews,
            idempotency,
            post_cas_recovery,
            pre_visibility_recovery,
            fs_mutation_recovery,
            cleanup_claims,
            lease_owner: "object-cleanup-worker",
            lease_duration: Duration::from_secs(300),
            fence_owner: "object-cleanup-worker-final-object-delete",
            fence_duration: Duration::from_secs(300),
        }
    }

    pub(crate) async fn run_once(
        &self,
        limit: usize,
    ) -> Result<ObjectCleanupWorkerSummary, VfsError> {
        if limit == 0 {
            return Ok(ObjectCleanupWorkerSummary::default());
        }

        let statuses = self
            .cleanup_claims
            .list_claimable_for_repo_and_kind(
                self.repo_id,
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                limit,
            )
            .await?;
        let mut summary = ObjectCleanupWorkerSummary {
            candidates_listed: statuses.len(),
            ..ObjectCleanupWorkerSummary::default()
        };
        for status in statuses {
            summary.processed += 1;
            self.process_status(status, &mut summary).await?;
        }
        Ok(summary)
    }

    async fn process_status(
        &self,
        status: ObjectCleanupClaimStatus,
        summary: &mut ObjectCleanupWorkerSummary,
    ) -> Result<(), VfsError> {
        if status.attempts() >= Self::MAX_ATTEMPTS {
            summary.poisoned += 1;
            return Ok(());
        }

        let Some(claim) = self.acquire_or_validate_claim(&status).await? else {
            summary.skipped_claim_unavailable += 1;
            return Ok(());
        };

        match self.try_delete_claimed_object(&claim).await {
            Ok(DeleteClaimOutcome::DeletionReady) => {
                self.cleanup_claims.release(&claim).await?;
                summary.deletion_ready += 1;
            }
            Ok(DeleteClaimOutcome::Reachable) => {
                summary.skipped_reachable += 1;
            }
            Ok(DeleteClaimOutcome::Blocked) => {
                self.record_failure_redacted(&claim).await;
                summary.skipped_blocked += 1;
                summary.retryable_failures += 1;
            }
            Err(error) if is_stale_cleanup_claim(&error) => {
                summary.retryable_failures += 1;
            }
            Err(_error) => {
                self.record_failure_redacted(&claim).await;
                summary.retryable_failures += 1;
            }
        }
        Ok(())
    }

    async fn acquire_or_validate_claim(
        &self,
        status: &ObjectCleanupClaimStatus,
    ) -> Result<Option<ObjectCleanupClaim>, VfsError> {
        match status.state() {
            ObjectCleanupClaimState::Active => {
                // Public status rows intentionally do not expose lease tokens,
                // so active leases must be reacquired by waiting for expiry.
                Ok(None)
            }
            ObjectCleanupClaimState::StaleActive | ObjectCleanupClaimState::Failed => {
                self.cleanup_claims.claim(self.claim_request(status)).await
            }
            ObjectCleanupClaimState::Completed => Ok(None),
        }
    }

    fn claim_request(&self, status: &ObjectCleanupClaimStatus) -> ObjectCleanupClaimRequest {
        ObjectCleanupClaimRequest {
            repo_id: status.repo_id().clone(),
            claim_kind: status.claim_kind(),
            object_kind: status.object_kind(),
            object_id: status.object_id(),
            object_key: status.object_key().to_string(),
            lease_owner: self.lease_owner.to_string(),
            lease_duration: self.lease_duration,
        }
    }

    async fn try_delete_claimed_object(
        &self,
        claim: &ObjectCleanupClaim,
    ) -> Result<DeleteClaimOutcome, VfsError> {
        let dry_run = self
            .gc()
            .run(self.repo_id, GC_ROOT_SCAN_LIMIT, Some(claim))
            .await?;
        if !dry_run.blockers.is_empty() {
            return Ok(DeleteClaimOutcome::Blocked);
        }
        if !dry_run.contains_unreachable_object(claim.object_kind, claim.object_id) {
            return Ok(DeleteClaimOutcome::Reachable);
        }

        let fence = self
            .metadata
            .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                claim.repo_id.clone(),
                claim.object_kind,
                claim.object_id,
                canonical_final_object_key(&claim.repo_id, claim.object_kind, &claim.object_id),
                self.fence_owner.to_string(),
                self.fence_duration,
            ))
            .await?
            .ok_or_else(stale_cleanup_claim)?;

        let result = async {
            let second = self
                .gc()
                .run(self.repo_id, GC_ROOT_SCAN_LIMIT, Some(claim))
                .await?;
            if !second.blockers.is_empty() {
                return Ok(DeleteClaimOutcome::Blocked);
            }
            if !second.contains_unreachable_object(claim.object_kind, claim.object_id) {
                return Ok(DeleteClaimOutcome::Reachable);
            }
            self.verify_metadata_for_claim(claim).await?;
            self.verify_delete_preconditions(claim, &fence).await?;
            Ok(DeleteClaimOutcome::DeletionReady)
        }
        .await;

        self.metadata
            .release_final_object_metadata_fence(&fence)
            .await?;
        result
    }

    async fn verify_metadata_for_claim(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let Some(record) = self.metadata.get(&claim.repo_id, claim.object_id).await? else {
            return Err(VfsError::ObjectWriteConflict {
                message: "final object metadata missing during cleanup; repair before deletion"
                    .to_string(),
            });
        };
        if record.repo_id != claim.repo_id
            || record.id != claim.object_id
            || record.kind != claim.object_kind
            || record.object_key != claim.object_key
        {
            return Err(VfsError::ObjectWriteConflict {
                message: "final object metadata changed during cleanup; retry".to_string(),
            });
        }
        Ok(())
    }

    async fn verify_delete_preconditions(
        &self,
        claim: &ObjectCleanupClaim,
        fence: &crate::backend::core_transaction::FinalObjectMetadataFence,
    ) -> Result<(), VfsError> {
        self.cleanup_claims.validate(claim).await?;
        self.metadata
            .validate_final_object_metadata_fence(fence)
            .await?;
        let third = self
            .gc()
            .run(self.repo_id, GC_ROOT_SCAN_LIMIT, Some(claim))
            .await?;
        if !third.blockers.is_empty() {
            return Err(VfsError::ObjectWriteConflict {
                message: "object cleanup deletion preconditions are blocked; retry".to_string(),
            });
        }
        if !third.contains_unreachable_object(claim.object_kind, claim.object_id) {
            return Err(VfsError::ObjectWriteConflict {
                message: "object cleanup candidate became reachable; retry".to_string(),
            });
        }
        Ok(())
    }

    fn gc(&self) -> ObjectGcDryRun<'_> {
        ObjectGcDryRun::new(
            self.objects,
            self.commits,
            self.refs,
            self.workspaces,
            self.reviews,
            self.idempotency,
            self.post_cas_recovery,
            self.pre_visibility_recovery,
            self.fs_mutation_recovery,
            self.cleanup_claims,
        )
    }

    async fn record_failure_redacted(&self, claim: &ObjectCleanupClaim) {
        let message = "object cleanup attempt failed; retry with backoff";
        let _ = self.cleanup_claims.record_failure(claim, message).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
enum DeleteClaimOutcome {
    DeletionReady,
    Reachable,
    Blocked,
}

fn parse_commit_hex(value: &str) -> Option<CommitId> {
    ObjectId::from_hex(value).ok().map(CommitId::from)
}

fn idempotency_commit_key(key: &str) -> bool {
    matches!(
        key,
        "hash"
            | "commit_id"
            | "head_commit"
            | "previous_commit"
            | "new_commit"
            | "revert_commit"
            | "reverted_to"
            | "target_commit"
            | "target"
            | "expected_head"
    )
}

fn insert_commit_root_from_hex(
    roots: &mut ObjectGcRoots,
    blockers: &mut Vec<ObjectGcBlockerSummary>,
    source: &'static str,
    value: &str,
) {
    match parse_commit_hex(value) {
        Some(commit) => {
            roots.commit_roots.insert(commit);
        }
        None => blockers.push(ObjectGcBlockerSummary::new(source, "invalid_commit")),
    }
}

fn collect_commit_ids_from_json(
    value: &serde_json::Value,
    commit_ids: &mut BTreeSet<CommitId>,
) -> bool {
    fn spend_budget(budget: &mut usize) -> bool {
        let Some(next) = budget.checked_sub(1) else {
            return false;
        };
        *budget = next;
        true
    }

    fn collect_from_commit_value(
        value: &serde_json::Value,
        commit_ids: &mut BTreeSet<CommitId>,
        budget: &mut usize,
        depth: usize,
    ) -> bool {
        if depth > GC_IDEMPOTENCY_JSON_DEPTH_LIMIT || !spend_budget(budget) {
            return false;
        }
        match value {
            serde_json::Value::String(text) => {
                if let Some(commit_id) = parse_commit_hex(text) {
                    commit_ids.insert(commit_id);
                }
                true
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    if !collect_from_commit_value(value, commit_ids, budget, depth + 1) {
                        return false;
                    }
                }
                true
            }
            serde_json::Value::Object(values) => {
                for value in values.values() {
                    if !collect_from_commit_value(value, commit_ids, budget, depth + 1) {
                        return false;
                    }
                }
                true
            }
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
                true
            }
        }
    }

    fn collect_any(
        value: &serde_json::Value,
        commit_ids: &mut BTreeSet<CommitId>,
        budget: &mut usize,
        depth: usize,
    ) -> bool {
        if depth > GC_IDEMPOTENCY_JSON_DEPTH_LIMIT || !spend_budget(budget) {
            return false;
        }
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    if !collect_any(value, commit_ids, budget, depth + 1) {
                        return false;
                    }
                }
                true
            }
            serde_json::Value::Object(values) => {
                for (key, value) in values {
                    if idempotency_commit_key(key) {
                        if !collect_from_commit_value(value, commit_ids, budget, depth + 1) {
                            return false;
                        }
                    } else if !collect_any(value, commit_ids, budget, depth + 1) {
                        return false;
                    }
                }
                true
            }
            serde_json::Value::String(_)
            | serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_) => true,
        }
    }

    let mut budget = GC_IDEMPOTENCY_JSON_NODE_LIMIT;
    collect_any(value, commit_ids, &mut budget, 0)
}

fn cleanup_status_matches_claim(
    status: &ObjectCleanupClaimStatus,
    claim: Option<&ObjectCleanupClaim>,
) -> bool {
    claim.is_some_and(|claim| {
        status.repo_id() == &claim.repo_id
            && status.claim_kind() == claim.claim_kind
            && status.object_kind() == claim.object_kind
            && status.object_id() == claim.object_id
            && status.object_key() == claim.object_key
    })
}

fn push_scan_limit_blocker(
    blockers: &mut Vec<ObjectGcBlockerSummary>,
    source: &'static str,
    count: usize,
) {
    if count == GC_ROOT_SCAN_LIMIT {
        blockers.push(ObjectGcBlockerSummary::new(source, "scan_limit_reached"));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaimRequest {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub object_key: String,
    pub lease_owner: String,
    pub lease_duration: Duration,
}

impl ObjectCleanupClaimRequest {
    pub fn validate(&self) -> Result<(), VfsError> {
        validate_canonical_object_key(
            &self.repo_id,
            self.object_kind,
            &self.object_id,
            &self.object_key,
        )?;
        validate_lease_owner(&self.lease_owner)?;
        if self.lease_duration.as_millis() == 0 {
            return Err(VfsError::InvalidArgs {
                message: "cleanup claim lease duration must be at least 1 millisecond".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaim {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub object_key: String,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: SystemTime,
    pub attempts: u64,
}

impl ObjectCleanupClaim {
    pub fn target(&self) -> ObjectCleanupClaimTarget {
        ObjectCleanupClaimTarget {
            repo_id: self.repo_id.clone(),
            claim_kind: self.claim_kind,
            object_key: self.object_key.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectCleanupClaimTarget {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectCleanupClaimState {
    Active,
    StaleActive,
    Completed,
    Failed,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaimStatus {
    repo_id: RepoId,
    claim_kind: ObjectCleanupClaimKind,
    object_kind: ObjectKind,
    object_id: ObjectId,
    object_key: String,
    state: ObjectCleanupClaimState,
    is_stale: bool,
    attempts: u64,
    lease_expires_at: SystemTime,
    completed_at: Option<SystemTime>,
    created_at: SystemTime,
    updated_at: SystemTime,
    has_last_failure: bool,
}

impl fmt::Debug for ObjectCleanupClaimStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectCleanupClaimStatus")
            .field("repo_id", &self.repo_id)
            .field("claim_kind", &self.claim_kind)
            .field("object_kind", &self.object_kind)
            .field("object_id_prefix", &self.object_id.short_hex())
            .field("state", &self.state)
            .field("is_stale", &self.is_stale)
            .field("attempts", &self.attempts)
            .field("lease_expires_at", &self.lease_expires_at)
            .field("completed_at", &self.completed_at)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("has_last_failure", &self.has_last_failure)
            .finish()
    }
}

impl ObjectCleanupClaimStatus {
    pub fn for_store(input: ObjectCleanupClaimStatusInput) -> Self {
        Self {
            repo_id: input.repo_id,
            claim_kind: input.claim_kind,
            object_kind: input.object_kind,
            object_id: input.object_id,
            object_key: input.object_key,
            state: input.state,
            is_stale: input.is_stale,
            attempts: input.attempts,
            lease_expires_at: input.lease_expires_at,
            completed_at: input.completed_at,
            created_at: input.created_at,
            updated_at: input.updated_at,
            has_last_failure: input.has_last_failure,
        }
    }

    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub const fn claim_kind(&self) -> ObjectCleanupClaimKind {
        self.claim_kind
    }

    pub const fn object_kind(&self) -> ObjectKind {
        self.object_kind
    }

    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub const fn state(&self) -> ObjectCleanupClaimState {
        self.state
    }

    pub const fn is_stale(&self) -> bool {
        self.is_stale
    }

    pub const fn attempts(&self) -> u64 {
        self.attempts
    }

    pub const fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }

    pub const fn completed_at(&self) -> Option<SystemTime> {
        self.completed_at
    }

    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    pub const fn has_last_failure(&self) -> bool {
        self.has_last_failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaimStatusInput {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub object_key: String,
    pub state: ObjectCleanupClaimState,
    pub is_stale: bool,
    pub attempts: u64,
    pub lease_expires_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub has_last_failure: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectCleanupClaimCounts {
    active: usize,
    stale_active: usize,
    completed: usize,
    failed: usize,
}

impl ObjectCleanupClaimCounts {
    pub const fn active(&self) -> usize {
        self.active
    }

    pub const fn stale_active(&self) -> usize {
        self.stale_active
    }

    pub const fn completed(&self) -> usize {
        self.completed
    }

    pub const fn failed(&self) -> usize {
        self.failed
    }

    pub const fn total(&self) -> usize {
        self.active + self.stale_active + self.completed + self.failed
    }

    pub fn add(&mut self, state: ObjectCleanupClaimState, count: usize) {
        match state {
            ObjectCleanupClaimState::Active => self.active += count,
            ObjectCleanupClaimState::StaleActive => self.stale_active += count,
            ObjectCleanupClaimState::Completed => self.completed += count,
            ObjectCleanupClaimState::Failed => self.failed += count,
        }
    }

    fn increment(&mut self, state: ObjectCleanupClaimState) {
        self.add(state, 1);
    }
}

#[async_trait]
pub trait ObjectCleanupClaimStore: Send + Sync {
    async fn claim(
        &self,
        request: ObjectCleanupClaimRequest,
    ) -> Result<Option<ObjectCleanupClaim>, VfsError>;

    async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError>;

    async fn record_failure(
        &self,
        claim: &ObjectCleanupClaim,
        message: &str,
    ) -> Result<(), VfsError>;

    async fn release(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let _ = claim;
        Err(VfsError::NotSupported {
            message: "object cleanup claim lease release is not supported by this store"
                .to_string(),
        })
    }

    async fn validate(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let _ = claim;
        Err(VfsError::NotSupported {
            message: "object cleanup claim validation is not supported by this store".to_string(),
        })
    }

    async fn list(&self, _limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        Err(VfsError::NotSupported {
            message: "object cleanup claim status listing is not supported by this store"
                .to_string(),
        })
    }

    async fn list_for_repo(
        &self,
        _repo_id: &RepoId,
        _limit: usize,
    ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        Err(VfsError::NotSupported {
            message:
                "repo-scoped object cleanup claim status listing is not supported by this store"
                    .to_string(),
        })
    }

    async fn list_claimable_for_repo_and_kind(
        &self,
        _repo_id: &RepoId,
        _claim_kind: ObjectCleanupClaimKind,
        _limit: usize,
    ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        Err(VfsError::NotSupported {
            message: "claimable object cleanup claim status listing is not supported by this store"
                .to_string(),
        })
    }

    async fn counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError> {
        Err(VfsError::NotSupported {
            message: "object cleanup claim status counts are not supported by this store"
                .to_string(),
        })
    }

    async fn counts_for_repo(
        &self,
        _repo_id: &RepoId,
    ) -> Result<ObjectCleanupClaimCounts, VfsError> {
        Err(VfsError::NotSupported {
            message:
                "repo-scoped object cleanup claim status counts are not supported by this store"
                    .to_string(),
        })
    }
}

#[derive(Debug, Default)]
pub struct InMemoryObjectCleanupClaimStore {
    inner: RwLock<BTreeMap<ObjectCleanupClaimTarget, InMemoryClaimEntry>>,
    now_for_tests: RwLock<Option<SystemTime>>,
}

impl InMemoryObjectCleanupClaimStore {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    async fn set_now_for_tests(&self, now: SystemTime) {
        *self.now_for_tests.write().await = Some(now);
    }

    async fn now(&self) -> SystemTime {
        self.now_for_tests
            .read()
            .await
            .unwrap_or_else(SystemTime::now)
    }
}

#[async_trait]
impl ObjectCleanupClaimStore for InMemoryObjectCleanupClaimStore {
    async fn claim(
        &self,
        request: ObjectCleanupClaimRequest,
    ) -> Result<Option<ObjectCleanupClaim>, VfsError> {
        request.validate()?;
        let now = self.now().await;
        let lease_expires_at =
            now.checked_add(request.lease_duration)
                .ok_or_else(|| VfsError::InvalidArgs {
                    message: "cleanup claim lease expiry overflow".to_string(),
                })?;

        let mut guard = self.inner.write().await;
        let target = request.target();
        let created_at = guard
            .get(&target)
            .map(|existing| existing.created_at)
            .unwrap_or(now);
        let attempts =
            match guard.get(&target) {
                Some(existing) if existing.completed_at.is_some() => return Ok(None),
                Some(existing) if existing.claim.lease_expires_at > now => return Ok(None),
                Some(existing) => existing.claim.attempts.checked_add(1).ok_or_else(|| {
                    VfsError::CorruptStore {
                        message: "cleanup claim attempt counter overflow".to_string(),
                    }
                })?,
                None => 1,
            };

        let claim = ObjectCleanupClaim {
            repo_id: request.repo_id,
            claim_kind: request.claim_kind,
            object_kind: request.object_kind,
            object_id: request.object_id,
            object_key: request.object_key,
            lease_owner: request.lease_owner,
            lease_token: Uuid::new_v4(),
            lease_expires_at,
            attempts,
        };
        guard.insert(
            target,
            InMemoryClaimEntry {
                claim: claim.clone(),
                completed_at: None,
                last_error: None,
                created_at,
                updated_at: now,
            },
        );

        Ok(Some(claim))
    }

    async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let completed_at = self.now().await;
        let mut guard = self.inner.write().await;
        let entry = active_entry_for_claim(&mut guard, claim, completed_at)?;
        entry.completed_at = Some(completed_at);
        entry.last_error = None;
        entry.updated_at = completed_at;
        Ok(())
    }

    async fn record_failure(
        &self,
        claim: &ObjectCleanupClaim,
        message: &str,
    ) -> Result<(), VfsError> {
        let failed_at = self.now().await;
        let mut guard = self.inner.write().await;
        let entry = active_entry_for_claim(&mut guard, claim, failed_at)?;
        entry.last_error = Some(message.to_string());
        entry.updated_at = failed_at;
        Ok(())
    }

    async fn release(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let released_at = self.now().await;
        let mut guard = self.inner.write().await;
        let entry = active_entry_for_claim(&mut guard, claim, released_at)?;
        entry.claim.lease_expires_at = released_at;
        entry.updated_at = released_at;
        Ok(())
    }

    async fn validate(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let now = self.now().await;
        let mut guard = self.inner.write().await;
        active_entry_for_claim(&mut guard, claim, now).map(|_| ())
    }

    async fn list(&self, limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut entries: Vec<&InMemoryClaimEntry> = guard.values().collect();
        entries.sort_by(|left, right| {
            left.completed_at
                .is_some()
                .cmp(&right.completed_at.is_some())
                .then_with(|| left.updated_at.cmp(&right.updated_at))
                .then_with(|| left.claim.repo_id.cmp(&right.claim.repo_id))
                .then_with(|| left.claim.object_key.cmp(&right.claim.object_key))
        });
        Ok(entries
            .into_iter()
            .take(limit)
            .map(|entry| entry.to_status(now))
            .collect())
    }

    async fn list_for_repo(
        &self,
        repo_id: &RepoId,
        limit: usize,
    ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut entries: Vec<&InMemoryClaimEntry> = guard
            .values()
            .filter(|entry| entry.claim.repo_id == *repo_id)
            .collect();
        entries.sort_by(|left, right| {
            left.completed_at
                .is_some()
                .cmp(&right.completed_at.is_some())
                .then_with(|| left.updated_at.cmp(&right.updated_at))
                .then_with(|| left.claim.repo_id.cmp(&right.claim.repo_id))
                .then_with(|| left.claim.object_key.cmp(&right.claim.object_key))
        });
        Ok(entries
            .into_iter()
            .take(limit)
            .map(|entry| entry.to_status(now))
            .collect())
    }

    async fn list_claimable_for_repo_and_kind(
        &self,
        repo_id: &RepoId,
        claim_kind: ObjectCleanupClaimKind,
        limit: usize,
    ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut entries: Vec<&InMemoryClaimEntry> = guard
            .values()
            .filter(|entry| {
                entry.claim.repo_id == *repo_id
                    && entry.claim.claim_kind == claim_kind
                    && entry.completed_at.is_none()
                    && entry.claim.lease_expires_at <= now
                    && matches!(
                        entry.state(now),
                        ObjectCleanupClaimState::StaleActive | ObjectCleanupClaimState::Failed
                    )
            })
            .collect();
        entries.sort_by(|left, right| {
            let left_poisoned = left.claim.attempts >= ObjectCleanupWorker::MAX_ATTEMPTS;
            let right_poisoned = right.claim.attempts >= ObjectCleanupWorker::MAX_ATTEMPTS;
            left_poisoned.cmp(&right_poisoned).then_with(|| {
                left.updated_at
                    .cmp(&right.updated_at)
                    .then_with(|| left.claim.object_key.cmp(&right.claim.object_key))
            })
        });
        Ok(entries
            .into_iter()
            .take(limit)
            .map(|entry| entry.to_status(now))
            .collect())
    }

    async fn counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError> {
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut counts = ObjectCleanupClaimCounts::default();
        for entry in guard.values() {
            counts.increment(entry.state(now));
        }
        Ok(counts)
    }

    async fn counts_for_repo(
        &self,
        repo_id: &RepoId,
    ) -> Result<ObjectCleanupClaimCounts, VfsError> {
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut counts = ObjectCleanupClaimCounts::default();
        for entry in guard
            .values()
            .filter(|entry| entry.claim.repo_id == *repo_id)
        {
            counts.increment(entry.state(now));
        }
        Ok(counts)
    }
}

#[derive(Debug, Clone)]
struct InMemoryClaimEntry {
    claim: ObjectCleanupClaim,
    completed_at: Option<SystemTime>,
    last_error: Option<String>,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl InMemoryClaimEntry {
    fn state(&self, now: SystemTime) -> ObjectCleanupClaimState {
        classify_cleanup_claim(
            self.completed_at,
            self.last_error.is_some(),
            self.claim.lease_expires_at,
            now,
        )
    }

    fn to_status(&self, now: SystemTime) -> ObjectCleanupClaimStatus {
        ObjectCleanupClaimStatus::for_store(ObjectCleanupClaimStatusInput {
            repo_id: self.claim.repo_id.clone(),
            claim_kind: self.claim.claim_kind,
            object_kind: self.claim.object_kind,
            object_id: self.claim.object_id,
            object_key: self.claim.object_key.clone(),
            state: self.state(now),
            is_stale: cleanup_claim_is_stale(self.completed_at, self.claim.lease_expires_at, now),
            attempts: self.claim.attempts,
            lease_expires_at: self.claim.lease_expires_at,
            completed_at: self.completed_at,
            created_at: self.created_at,
            updated_at: self.updated_at,
            has_last_failure: self.last_error.is_some(),
        })
    }
}

pub fn classify_cleanup_claim(
    completed_at: Option<SystemTime>,
    has_last_failure: bool,
    lease_expires_at: SystemTime,
    now: SystemTime,
) -> ObjectCleanupClaimState {
    if completed_at.is_some() {
        ObjectCleanupClaimState::Completed
    } else if has_last_failure {
        ObjectCleanupClaimState::Failed
    } else if lease_expires_at <= now {
        ObjectCleanupClaimState::StaleActive
    } else {
        ObjectCleanupClaimState::Active
    }
}

pub fn cleanup_claim_is_stale(
    completed_at: Option<SystemTime>,
    lease_expires_at: SystemTime,
    now: SystemTime,
) -> bool {
    completed_at.is_none() && lease_expires_at <= now
}

impl ObjectCleanupClaimRequest {
    fn target(&self) -> ObjectCleanupClaimTarget {
        ObjectCleanupClaimTarget {
            repo_id: self.repo_id.clone(),
            claim_kind: self.claim_kind,
            object_key: self.object_key.clone(),
        }
    }
}

fn active_entry_for_claim<'a>(
    entries: &'a mut BTreeMap<ObjectCleanupClaimTarget, InMemoryClaimEntry>,
    claim: &ObjectCleanupClaim,
    now: SystemTime,
) -> Result<&'a mut InMemoryClaimEntry, VfsError> {
    let Some(entry) = entries.get_mut(&claim.target()) else {
        return Err(stale_cleanup_claim());
    };
    if entry.completed_at.is_some()
        || entry.claim.lease_token != claim.lease_token
        || entry.claim.lease_expires_at <= now
    {
        return Err(stale_cleanup_claim());
    }
    Ok(entry)
}

pub fn validate_lease_owner(owner: &str) -> Result<(), VfsError> {
    if owner.is_empty() || owner.len() > 128 || owner.chars().any(char::is_control) {
        return Err(VfsError::InvalidArgs {
            message: "cleanup claim lease owner must be 1-128 non-control characters".to_string(),
        });
    }
    Ok(())
}

pub fn validate_object_key(key: &str) -> Result<(), VfsError> {
    if key.is_empty() || key.chars().any(char::is_control) {
        return Err(VfsError::InvalidArgs {
            message: "cleanup claim object key must be non-empty and contain no control characters"
                .to_string(),
        });
    }
    Ok(())
}

pub fn validate_canonical_object_key(
    repo_id: &RepoId,
    kind: ObjectKind,
    id: &ObjectId,
    key: &str,
) -> Result<(), VfsError> {
    validate_object_key(key)?;
    let expected = canonical_final_object_key(repo_id, kind, id);
    if key != expected {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "cleanup claim object key must be canonical final object key {expected}"
            ),
        });
    }
    Ok(())
}

pub fn canonical_final_object_key(repo_id: &RepoId, kind: ObjectKind, id: &ObjectId) -> String {
    format!(
        "repos/{}/objects/{}/{}",
        repo_id.as_str(),
        object_kind_segment(kind),
        id.to_hex()
    )
}

fn object_kind_segment(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
    }
}

pub fn stale_cleanup_claim() -> VfsError {
    VfsError::ObjectWriteConflict {
        message: STALE_CLEANUP_CLAIM_MESSAGE.to_string(),
    }
}

pub fn is_stale_cleanup_claim(error: &VfsError) -> bool {
    matches!(
        error,
        VfsError::ObjectWriteConflict { message } if message == STALE_CLEANUP_CLAIM_MESSAGE
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::blob_object::{BlobObjectStore, InMemoryObjectMetadataStore};
    use crate::backend::core_transaction::{
        DurableCorePostCasRecoveryClaimStore, DurableCorePostCasRecoveryTarget,
        DurableCorePostCasStep, DurableCorePreVisibilityRecoveryRecord,
        DurableCorePreVisibilityRecoveryStage, DurableCorePreVisibilityRecoveryStore,
        DurableCorePreVisibilityRecoveryTarget, DurableFsMutationRecoveryEnvelope,
        DurableFsMutationRecoveryStep, DurableFsMutationRecoveryStore,
        DurableFsMutationRecoveryTarget, InMemoryDurableCorePostCasRecoveryClaimStore,
        InMemoryDurableCorePreVisibilityRecoveryStore, InMemoryDurableFsMutationRecoveryStore,
    };
    use crate::backend::{
        CommitRecord, CommitStore, LocalMemoryCommitStore, LocalMemoryObjectStore,
        LocalMemoryRefStore, ObjectStore, ObjectWrite, RefExpectation, RefRecord, RefStore,
        RefUpdate, SourceCheckedRefUpdate, StoredObject,
    };
    use crate::idempotency::{IdempotencyKey, IdempotencyStore};
    use crate::review::{ChangeRequestStatus, InMemoryReviewStore, NewChangeRequest, ReviewStore};
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::vcs::{CommitId, MAIN_REF, RefName};
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use axum::http::HeaderValue;
    use serde_json::json;
    use std::sync::Arc;

    fn repo() -> RepoId {
        RepoId::new("repo_cleanup").unwrap()
    }

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    fn request(lease_duration: Duration) -> ObjectCleanupClaimRequest {
        let id = object_id(b"repair me");
        ObjectCleanupClaimRequest {
            repo_id: repo(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: ObjectKind::Blob,
            object_id: id,
            object_key: canonical_final_object_key(&repo(), ObjectKind::Blob, &id),
            lease_owner: "worker-a".to_string(),
            lease_duration,
        }
    }

    #[tokio::test]
    async fn first_claim_succeeds_and_active_lease_blocks_duplicate() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;

        let first = store
            .claim(request(Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let duplicate = store.claim(request(Duration::from_secs(60))).await.unwrap();

        assert_eq!(first.attempts, 1);
        assert!(duplicate.is_none());
    }

    #[tokio::test]
    async fn expired_incomplete_claim_can_be_retried() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let first = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.record_failure(&first, "transient").await.unwrap();

        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let retry = store
            .claim(request(Duration::from_secs(30)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retry.attempts, 2);
        assert_ne!(retry.lease_token, first.lease_token);
    }

    #[tokio::test]
    async fn completed_claim_is_not_reacquired() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let claim = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.complete(&claim).await.unwrap();

        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let retry = store.claim(request(Duration::from_secs(30))).await.unwrap();

        assert!(retry.is_none());
    }

    #[tokio::test]
    async fn stale_token_completion_is_rejected() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let claim = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let retry = store
            .claim(request(Duration::from_secs(30)))
            .await
            .unwrap()
            .unwrap();

        let err = store
            .complete(&claim)
            .await
            .expect_err("stale claim token should not complete retry lease");
        assert!(is_stale_cleanup_claim(&err));

        store.complete(&retry).await.unwrap();
    }

    #[tokio::test]
    async fn expired_claim_completion_and_failure_are_rejected() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let claim = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();

        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let complete_err = store
            .complete(&claim)
            .await
            .expect_err("expired cleanup claim should not complete");
        assert!(is_stale_cleanup_claim(&complete_err));

        let failure_err = store
            .record_failure(&claim, "too late")
            .await
            .expect_err("expired cleanup claim should not record failure");
        assert!(is_stale_cleanup_claim(&failure_err));

        let retry = store
            .claim(request(Duration::from_secs(30)))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(retry.lease_token, claim.lease_token);
    }

    #[tokio::test]
    async fn invalid_claim_requests_are_rejected() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;

        let mut bad_owner = request(Duration::from_secs(1));
        bad_owner.lease_owner = "bad\nowner".to_string();
        assert!(matches!(
            store.claim(bad_owner).await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let mut bad_key = request(Duration::from_secs(1));
        bad_key.object_key = "repos/repo_cleanup/objects/blob/not-the-id".to_string();
        assert!(matches!(
            store.claim(bad_key).await,
            Err(VfsError::InvalidArgs { .. })
        ));

        assert!(matches!(
            store.claim(request(Duration::ZERO)).await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store.claim(request(Duration::from_nanos(1))).await,
            Err(VfsError::InvalidArgs { .. })
        ));
    }

    #[tokio::test]
    async fn list_returns_bounded_redacted_cleanup_claim_statuses() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let failed = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store
            .record_failure(&failed, "raw storage path /secret/object failed")
            .await
            .unwrap();
        store.set_now_for_tests(now + Duration::from_secs(6)).await;

        let statuses = store.list(1).await.unwrap();

        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];
        assert_eq!(status.repo_id(), &failed.repo_id);
        assert_eq!(status.claim_kind(), failed.claim_kind);
        assert_eq!(status.object_kind(), failed.object_kind);
        assert_eq!(status.object_id(), failed.object_id);
        assert_eq!(status.object_key(), failed.object_key);
        assert_eq!(status.attempts(), 1);
        assert_eq!(
            status.state(),
            ObjectCleanupClaimState::Failed,
            "failed takes precedence over stale_active for expired failed rows"
        );
        assert!(status.is_stale());
        assert!(status.has_last_failure());
        assert!(status.completed_at().is_none());
        assert!(
            !format!("{status:?}").contains("raw storage path /secret/object failed"),
            "status debug output must not expose raw failure text"
        );
        assert!(
            !format!("{status:?}").contains(&failed.lease_token.to_string()),
            "status debug output must not expose lease tokens"
        );
    }

    #[tokio::test]
    async fn counts_classify_active_stale_completed_and_failed_claims() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;

        let active = store
            .claim(request_with_id(b"active", Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let stale = store
            .claim(request_with_id(b"stale", Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let completed = store
            .claim(request_with_id(b"completed", Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        store.complete(&completed).await.unwrap();
        let failed = store
            .claim(request_with_id(b"failed", Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.record_failure(&failed, "disk denied").await.unwrap();
        store.set_now_for_tests(now + Duration::from_secs(6)).await;

        let counts = store.counts().await.unwrap();

        assert_eq!(counts.active(), 1);
        assert_eq!(counts.stale_active(), 1);
        assert_eq!(counts.completed(), 1);
        assert_eq!(counts.failed(), 1);
        assert_eq!(counts.total(), 4);
        assert_eq!(active.attempts, 1);
        assert_eq!(stale.attempts, 1);
    }

    #[tokio::test]
    async fn gc_dry_run_reports_unreachable_commit_and_objects() {
        let harness = GcHarness::new();
        let reachable = harness.seed_commit("reachable", Vec::new()).await;
        harness.update_ref(MAIN_REF, reachable).await;
        let unreachable = harness.seed_commit("unreachable", Vec::new()).await;
        let unreachable_record = harness.commit(unreachable).await;

        let cleanup_claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Tree,
                unreachable_record.root_tree,
                "lost-tree",
            )
            .await;
        harness
            .cleanup
            .record_failure(&cleanup_claim, "redacted")
            .await
            .unwrap();

        let report = harness.gc().run(&harness.repo, 10, None).await.unwrap();

        assert!(
            report
                .unreachable_commits
                .iter()
                .any(|candidate| candidate.commit_id_prefix == unreachable.short_hex())
        );
        assert!(report.unreachable_objects.iter().any(
            |candidate| candidate.object_id_prefix == unreachable_record.root_tree.short_hex()
        ));
        assert!(report.blockers.is_empty());
        assert!(!format!("{report:?}").contains(&unreachable.to_hex()));
        assert!(!format!("{report:?}").contains("repos/repo_cleanup/objects"));
    }

    #[tokio::test]
    async fn gc_dry_run_preserves_ref_workspace_recovery_idempotency_and_review_roots() {
        let harness = GcHarness::new();
        let ref_commit = harness.seed_commit("ref", Vec::new()).await;
        let workspace_head = harness.seed_commit("workspace-head", Vec::new()).await;
        let workspace_session = harness.seed_commit("workspace-session", Vec::new()).await;
        let review_base = harness.seed_commit("review-base", Vec::new()).await;
        let review_head = harness.seed_commit("review-head", Vec::new()).await;
        let post_cas = harness.seed_commit("post-cas", Vec::new()).await;
        let pre_visibility = harness.seed_commit("pre-visibility", Vec::new()).await;
        let fs_previous = harness.seed_commit("fs-previous", Vec::new()).await;
        let fs_new = harness.seed_commit("fs-new", Vec::new()).await;
        let idempotency_commit = harness.seed_commit("idempotency", Vec::new()).await;

        harness.update_ref(MAIN_REF, ref_commit).await;
        harness
            .update_ref("agent/workspace/session", workspace_session)
            .await;
        let workspace = harness
            .workspaces
            .create_workspace_with_refs_for_repo(
                harness.repo.clone(),
                "workspace",
                "/tmp/workspace",
                MAIN_REF,
                Some("agent/workspace/session"),
            )
            .await
            .unwrap();
        harness
            .workspaces
            .update_head_commit_for_repo(&harness.repo, workspace.id, Some(workspace_head.to_hex()))
            .await
            .unwrap();
        let change = harness
            .reviews
            .create_change_request_for_repo(
                &harness.repo,
                NewChangeRequest {
                    title: "review".to_string(),
                    description: None,
                    source_ref: "review/feature".to_string(),
                    target_ref: MAIN_REF.to_string(),
                    base_commit: review_base.to_hex(),
                    head_commit: review_head.to_hex(),
                    created_by: 1,
                },
            )
            .await
            .unwrap();
        harness
            .reviews
            .transition_change_request_for_repo(
                &harness.repo,
                change.id,
                ChangeRequestStatus::Rejected,
            )
            .await
            .unwrap()
            .expect("terminal change request should remain retained");
        harness.enqueue_post_cas(post_cas).await;
        harness.enqueue_pre_visibility(pre_visibility).await;
        harness.enqueue_fs_mutation(fs_previous, fs_new).await;
        harness
            .complete_idempotency_with_commit(idempotency_commit)
            .await;

        let report = harness.gc().run(&harness.repo, 20, None).await.unwrap();
        let candidates: Vec<_> = report
            .unreachable_commits
            .iter()
            .map(|candidate| candidate.commit_id_prefix.as_str())
            .collect();

        for rooted in [
            ref_commit,
            workspace_head,
            workspace_session,
            review_base,
            review_head,
            post_cas,
            pre_visibility,
            fs_previous,
            fs_new,
            idempotency_commit,
        ] {
            assert!(
                !candidates.contains(&rooted.short_hex().as_str()),
                "rooted commit {} was reported unreachable",
                rooted.short_hex()
            );
        }
        assert!(report.blockers.is_empty());
    }

    #[tokio::test]
    async fn gc_dry_run_treats_active_cleanup_claims_as_roots_except_current_claim() {
        let harness = GcHarness::new();
        let active_blob = harness.seed_blob(b"active cleanup blob").await;
        let current_blob = harness.seed_blob(b"current cleanup blob").await;
        let active_claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                active_blob,
                "active-cleanup",
            )
            .await;
        let current_claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                current_blob,
                "current-cleanup",
            )
            .await;

        let without_allowlist = harness.gc().run(&harness.repo, 10, None).await.unwrap();
        assert!(without_allowlist.unreachable_objects.is_empty());

        let with_allowlist = harness
            .gc()
            .run(&harness.repo, 10, Some(&current_claim))
            .await
            .unwrap();

        assert!(
            !with_allowlist
                .unreachable_objects
                .iter()
                .any(|candidate| candidate.object_id_prefix == active_claim.object_id.short_hex())
        );
        assert!(
            with_allowlist
                .unreachable_objects
                .iter()
                .any(|candidate| candidate.object_id_prefix == current_blob.short_hex())
        );
    }

    #[tokio::test]
    async fn gc_dry_run_preserves_reachable_commit_object_cleanup_candidate() {
        let harness = GcHarness::new();
        let reachable = harness
            .seed_commit("reachable-commit-object", Vec::new())
            .await;
        harness.update_ref(MAIN_REF, reachable).await;
        let cleanup_claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Commit,
                ObjectId::from(reachable),
                "reachable-commit",
            )
            .await;
        harness
            .cleanup
            .record_failure(&cleanup_claim, "redacted")
            .await
            .unwrap();

        let report = harness.gc().run(&harness.repo, 10, None).await.unwrap();

        assert!(report.blockers.is_empty());
        assert!(
            !report
                .unreachable_objects
                .iter()
                .any(|candidate| candidate.object_id_prefix == reachable.short_hex()),
            "reachable commit final object must not be reported unreachable"
        );
    }

    #[tokio::test]
    async fn gc_dry_run_invalid_workspace_head_blocks_object_candidates() {
        let harness = GcHarness::new();
        let workspace = harness
            .workspaces
            .create_workspace_with_refs_for_repo(
                harness.repo.clone(),
                "invalid-head",
                "/tmp/invalid-head",
                MAIN_REF,
                None,
            )
            .await
            .unwrap();
        harness
            .workspaces
            .update_head_commit_for_repo(&harness.repo, workspace.id, Some("not-a-commit".into()))
            .await
            .unwrap();
        let lost_blob = harness.seed_blob(b"lost behind invalid head").await;
        let cleanup_claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost_blob,
                "lost-invalid-head",
            )
            .await;
        harness
            .cleanup
            .record_failure(&cleanup_claim, "raw storage detail")
            .await
            .unwrap();

        let report = harness.gc().run(&harness.repo, 10, None).await.unwrap();

        assert!(report.blockers.iter().any(
            |blocker| blocker.source == "workspace_heads" && blocker.reason == "invalid_commit"
        ));
        assert!(report.unreachable_objects.is_empty());
        assert!(!format!("{report:?}").contains("raw storage detail"));
    }

    #[tokio::test]
    async fn gc_dry_run_is_bounded_and_redacted_when_tree_walk_fails() {
        let harness = GcHarness::new();
        let corrupt_root = harness.seed_raw_tree(b"not a tree").await;
        let corrupt_commit = commit_id("corrupt-root-commit");
        harness
            .commits
            .insert(CommitRecord {
                repo_id: harness.repo.clone(),
                id: corrupt_commit,
                root_tree: corrupt_root,
                parents: Vec::new(),
                timestamp: 1,
                message: "sensitive commit message".to_string(),
                author: "sensitive author".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        harness.update_ref(MAIN_REF, corrupt_commit).await;
        for index in 0..5 {
            harness
                .seed_commit(&format!("unreachable-{index}"), Vec::new())
                .await;
        }

        let report = harness.gc().run(&harness.repo, 2, None).await.unwrap();

        assert_eq!(report.unreachable_commits.len(), 2);
        assert_eq!(report.unreachable_objects.len(), 0);
        assert_eq!(report.blockers.len(), 1);
        assert_eq!(report.blockers[0].source, "tree_walk");
        let rendered = format!("{report:?}");
        assert!(!rendered.contains(&corrupt_commit.to_hex()));
        assert!(!rendered.contains(&corrupt_root.to_hex()));
        assert!(!rendered.contains("sensitive commit message"));
        assert!(!rendered.contains("not a tree"));
    }

    #[tokio::test]
    async fn gc_dry_run_root_source_failure_suppresses_deletion_candidates() {
        let harness = GcHarness::new();
        let _unreachable = harness.seed_commit("root-source-failure", Vec::new()).await;
        let lost_blob = harness
            .seed_blob(b"lost object behind failed root source")
            .await;
        let cleanup_claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost_blob,
                "lost-object",
            )
            .await;
        harness
            .cleanup
            .record_failure(&cleanup_claim, "raw object store error")
            .await
            .unwrap();
        let failing_refs = FailingRefStore;
        let gc = ObjectGcDryRun::new(
            &harness.objects,
            &harness.commits,
            &failing_refs,
            &harness.workspaces,
            &harness.reviews,
            &harness.idempotency,
            &harness.post_cas,
            &harness.pre_visibility,
            &harness.fs_mutation,
            &harness.cleanup,
        );

        let report = gc.run(&harness.repo, 10, None).await.unwrap();

        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.source == "refs" && blocker.reason == "list_failed")
        );
        assert!(report.unreachable_commits.is_empty());
        assert!(
            report.unreachable_objects.is_empty(),
            "root-source failures must suppress deletion candidates"
        );
        let rendered = format!("{report:?}");
        assert!(!rendered.contains(&lost_blob.to_hex()));
        assert!(!rendered.contains("raw object store error"));
    }

    #[tokio::test]
    async fn gc_dry_run_cleanup_claim_scan_limit_is_repo_scoped() {
        let harness = GcHarness::new();
        let other_repo = RepoId::new("repo_other_cleanup_claims").unwrap();
        for index in 0..=GC_ROOT_SCAN_LIMIT {
            let id = object_id(format!("other cleanup claim {index}").as_bytes());
            harness
                .cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: other_repo.clone(),
                    claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    object_kind: ObjectKind::Blob,
                    object_id: id,
                    object_key: canonical_final_object_key(&other_repo, ObjectKind::Blob, &id),
                    lease_owner: "other-repo".to_string(),
                    lease_duration: Duration::from_secs(60),
                })
                .await
                .unwrap();
        }
        let local = harness.seed_blob(b"local cleanup claim").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                local,
                "local-repo",
            )
            .await;

        let report = harness.gc().run(&harness.repo, 10, None).await.unwrap();

        assert!(
            !report
                .blockers
                .iter()
                .any(|blocker| blocker.source == "cleanup_claims"
                    && blocker.reason == "scan_limit_reached")
        );
        assert_eq!(report.roots.cleanup_candidate_count(), 1);
    }

    #[tokio::test]
    async fn cleanup_worker_marks_cas_lost_object_deletion_ready_only_when_unreachable_and_fenced()
    {
        let harness = WorkerHarness::new();
        let lost = harness.seed_blob(b"cas lost unreachable object").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost,
                "mutation-lost",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;

        let summary = harness.worker().run_once(10).await.unwrap();

        assert_eq!(summary.processed, 1);
        assert_eq!(summary.deletion_ready, 1);
        assert_eq!(summary.deleted_final_objects, 0);
        assert_eq!(summary.retryable_failures, 0);
        assert_eq!(summary.poisoned, 0);
        assert_eq!(
            harness
                .objects
                .get(&harness.repo, lost, ObjectKind::Blob)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"cas lost unreachable object"
        );
        assert_eq!(harness.cleanup.counts().await.unwrap().completed(), 0);
    }

    #[tokio::test]
    async fn cleanup_worker_blocks_metadata_missing_final_object_until_repair() {
        let harness = WorkerHarness::new();
        let lost = harness
            .seed_blob(b"metadata missing valid final object")
            .await;
        harness.remove_metadata(ObjectKind::Blob, lost).await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost,
                "metadata-missing",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;

        let summary = harness.worker().run_once(10).await.unwrap();

        assert_eq!(summary.processed, 1);
        assert_eq!(summary.deletion_ready, 0);
        assert_eq!(summary.deleted_final_objects, 0);
        assert_eq!(summary.retryable_failures, 1);
        assert_eq!(harness.cleanup.counts().await.unwrap().completed(), 0);
        assert!(
            harness
                .metadata
                .get(&harness.repo, lost)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn cleanup_worker_preserves_object_reachable_from_ref_workspace_recovery_idempotency_or_review()
     {
        let harness = WorkerHarness::new();
        let ref_root = harness.seed_commit_with_blob("worker-ref").await;
        let workspace_root = harness.seed_commit_with_blob("worker-workspace").await;
        let post_cas_root = harness.seed_commit_with_blob("worker-post-cas").await;
        let idempotency_root = harness.seed_commit_with_blob("worker-idempotency").await;
        let review_root = harness.seed_commit_with_blob("worker-review").await;

        harness.update_ref(MAIN_REF, ref_root.commit).await;
        harness
            .update_ref("agent/worker/session", workspace_root.commit)
            .await;
        harness
            .workspaces
            .create_workspace_with_refs_for_repo(
                harness.repo.clone(),
                "worker",
                "/tmp/worker",
                MAIN_REF,
                Some("agent/worker/session"),
            )
            .await
            .unwrap();
        harness.enqueue_post_cas(post_cas_root.commit).await;
        harness
            .complete_idempotency_with_commit(idempotency_root.commit)
            .await;
        let change = harness
            .reviews
            .create_change_request_for_repo(
                &harness.repo,
                NewChangeRequest {
                    title: "worker review".to_string(),
                    description: None,
                    source_ref: "review/worker".to_string(),
                    target_ref: MAIN_REF.to_string(),
                    base_commit: review_root.commit.to_hex(),
                    head_commit: review_root.commit.to_hex(),
                    created_by: 1,
                },
            )
            .await
            .unwrap();
        harness
            .reviews
            .transition_change_request_for_repo(
                &harness.repo,
                change.id,
                ChangeRequestStatus::Rejected,
            )
            .await
            .unwrap()
            .expect("terminal change request should remain retained");

        for (blob, owner) in [
            (ref_root.blob, "ref"),
            (workspace_root.blob, "workspace"),
            (post_cas_root.blob, "post-cas"),
            (idempotency_root.blob, "idempotency"),
            (review_root.blob, "review"),
        ] {
            harness
                .claim_object(
                    ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    ObjectKind::Blob,
                    blob,
                    owner,
                )
                .await;
        }
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;

        let summary = harness.worker().run_once(10).await.unwrap();

        assert_eq!(summary.processed, 5);
        assert_eq!(summary.deletion_ready, 0);
        assert_eq!(summary.deleted_final_objects, 0);
        assert_eq!(summary.skipped_reachable, 5);
        for root in [
            ref_root,
            workspace_root,
            post_cas_root,
            idempotency_root,
            review_root,
        ] {
            assert_eq!(
                harness
                    .objects
                    .get(&harness.repo, root.blob, ObjectKind::Blob)
                    .await
                    .unwrap()
                    .unwrap(),
                StoredObject {
                    repo_id: harness.repo.clone(),
                    id: root.blob,
                    kind: ObjectKind::Blob,
                    bytes: root.blob_bytes.to_vec(),
                }
            );
        }
    }

    #[tokio::test]
    async fn cleanup_worker_revalidates_after_fence_before_marking_deletion_ready() {
        let harness = WorkerHarness::new();
        let lost = harness.seed_blob(b"lease stolen before delete").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost,
                "first",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;
        harness
            .steal_claim_after_fence
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let summary = harness.worker().run_once(10).await.unwrap();

        assert_eq!(summary.deletion_ready, 0);
        assert_eq!(summary.deleted_final_objects, 0);
        assert_eq!(summary.retryable_failures, 1);
        assert_eq!(
            harness
                .objects
                .get(&harness.repo, lost, ObjectKind::Blob)
                .await
                .unwrap()
                .unwrap(),
            StoredObject {
                repo_id: harness.repo.clone(),
                id: lost,
                kind: ObjectKind::Blob,
                bytes: b"lease stolen before delete".to_vec(),
            }
        );
    }

    #[tokio::test]
    async fn cleanup_worker_records_backoff_and_poison_without_raw_errors() {
        let harness = WorkerHarness::new();
        let lost = harness.seed_blob(b"blocked roots redacted").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost,
                "failing",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;
        harness
            .block_idempotency_roots
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let summary = harness.worker().run_once(10).await.unwrap();

        assert_eq!(summary.retryable_failures, 1);
        assert_eq!(summary.poisoned, 0);
        assert!(!format!("{summary:?}").contains("blocked roots redacted"));
        assert_eq!(
            harness
                .objects
                .get(&harness.repo, lost, ObjectKind::Blob)
                .await
                .unwrap()
                .unwrap(),
            StoredObject {
                repo_id: harness.repo.clone(),
                id: lost,
                kind: ObjectKind::Blob,
                bytes: b"blocked roots redacted".to_vec(),
            }
        );
        let statuses = harness.cleanup.list(10).await.unwrap();
        assert_eq!(statuses[0].state(), ObjectCleanupClaimState::Failed);
        assert!(statuses[0].has_last_failure());
        assert!(!format!("{:?}", statuses[0]).contains("blocked roots redacted"));

        for attempt in 0..ObjectCleanupWorker::MAX_ATTEMPTS {
            harness
                .cleanup
                .set_now_for_tests(SystemTime::now() + Duration::from_secs(400 + (attempt * 400)))
                .await;
            let _ = harness.worker().run_once(10).await.unwrap();
        }
        let poison = harness.worker().run_once(10).await.unwrap();
        assert_eq!(poison.poisoned, 1);
    }

    #[tokio::test]
    async fn cleanup_worker_is_bounded_by_limit() {
        let harness = WorkerHarness::new();
        let mut ids = Vec::new();
        for index in 0..3 {
            let id = harness
                .seed_blob(format!("bounded lost object {index}").as_bytes())
                .await;
            ids.push(id);
            harness
                .claim_object(
                    ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    ObjectKind::Blob,
                    id,
                    "bounded",
                )
                .await;
        }
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;

        let summary = harness.worker().run_once(2).await.unwrap();

        assert_eq!(summary.candidates_listed, 2);
        assert_eq!(summary.processed, 2);
        assert_eq!(summary.deletion_ready, 2);
        assert_eq!(summary.deleted_final_objects, 0);
        let mut present = 0;
        for id in ids {
            if harness
                .objects
                .get(&harness.repo, id, ObjectKind::Blob)
                .await
                .unwrap()
                .is_some()
            {
                present += 1;
            }
        }
        assert_eq!(present, 3);
    }

    #[tokio::test]
    async fn cleanup_worker_does_not_let_poisoned_claims_starve_claimable_work() {
        let harness = WorkerHarness::new();
        let base = SystemTime::now();
        let poison = harness.seed_blob(b"poisoned lost object").await;
        let mut claim = harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                poison,
                "poison",
            )
            .await;
        harness
            .cleanup
            .record_failure(&claim, "redacted poison")
            .await
            .unwrap();
        for attempt in 1..ObjectCleanupWorker::MAX_ATTEMPTS {
            harness
                .cleanup
                .set_now_for_tests(base + Duration::from_secs(70 * attempt))
                .await;
            claim = harness
                .claim_object(
                    ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    ObjectKind::Blob,
                    poison,
                    "poison",
                )
                .await;
            harness
                .cleanup
                .record_failure(&claim, "redacted poison")
                .await
                .unwrap();
        }

        let ready = harness.seed_blob(b"ready behind poison").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                ready,
                "ready",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(base + Duration::from_secs(400))
            .await;

        let summary = harness.worker().run_once(1).await.unwrap();

        assert_eq!(summary.processed, 1);
        assert_eq!(summary.deletion_ready, 1);
        assert_eq!(summary.poisoned, 0);
    }

    #[tokio::test]
    async fn cleanup_worker_scans_repo_and_kind_before_applying_limit() {
        let harness = WorkerHarness::new();
        let other_repo = RepoId::new("repo_other_cleanup").unwrap();
        for index in 0..3 {
            let id = object_id(format!("other repo cleanup {index}").as_bytes());
            harness
                .cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: other_repo.clone(),
                    claim_kind: ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                    object_kind: ObjectKind::Blob,
                    object_id: id,
                    object_key: canonical_final_object_key(&other_repo, ObjectKind::Blob, &id),
                    lease_owner: "other-repo".to_string(),
                    lease_duration: Duration::from_secs(60),
                })
                .await
                .unwrap();
        }
        for index in 0..3 {
            let id = object_id(format!("repair cleanup {index}").as_bytes());
            harness
                .cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: harness.repo.clone(),
                    claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
                    object_kind: ObjectKind::Blob,
                    object_id: id,
                    object_key: canonical_final_object_key(&harness.repo, ObjectKind::Blob, &id),
                    lease_owner: "repair".to_string(),
                    lease_duration: Duration::from_secs(60),
                })
                .await
                .unwrap();
        }
        let lost = harness.seed_blob(b"repo scoped worker target").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost,
                "target",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;

        let summary = harness.worker().run_once(1).await.unwrap();

        assert_eq!(summary.candidates_listed, 1);
        assert_eq!(summary.processed, 1);
        assert_eq!(summary.skipped_non_cas_lost, 0);
        assert_eq!(summary.deletion_ready, 1);
    }

    #[tokio::test]
    async fn cleanup_worker_releases_deletion_ready_claim_for_repeatable_dry_run() {
        let harness = WorkerHarness::new();
        let lost = harness.seed_blob(b"repeatable deletion ready").await;
        harness
            .claim_object(
                ObjectCleanupClaimKind::DurableMutationCasLostObjectCleanup,
                ObjectKind::Blob,
                lost,
                "repeatable",
            )
            .await;
        harness
            .cleanup
            .set_now_for_tests(SystemTime::now() + Duration::from_secs(70))
            .await;

        let first = harness.worker().run_once(10).await.unwrap();
        let second = harness.worker().run_once(10).await.unwrap();

        assert_eq!(first.deletion_ready, 1);
        assert_eq!(second.deletion_ready, 1);
        assert_eq!(second.skipped_claim_unavailable, 0);
        assert_eq!(harness.cleanup.counts().await.unwrap().stale_active(), 1);
    }

    fn request_with_id(bytes: &[u8], lease_duration: Duration) -> ObjectCleanupClaimRequest {
        let id = object_id(bytes);
        ObjectCleanupClaimRequest {
            repo_id: repo(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: ObjectKind::Blob,
            object_id: id,
            object_key: canonical_final_object_key(&repo(), ObjectKind::Blob, &id),
            lease_owner: "worker-a".to_string(),
            lease_duration,
        }
    }

    struct GcHarness {
        repo: RepoId,
        objects: LocalMemoryObjectStore,
        commits: LocalMemoryCommitStore,
        refs: LocalMemoryRefStore,
        workspaces: InMemoryWorkspaceMetadataStore,
        reviews: InMemoryReviewStore,
        idempotency: crate::idempotency::InMemoryIdempotencyStore,
        post_cas: InMemoryDurableCorePostCasRecoveryClaimStore,
        pre_visibility: InMemoryDurableCorePreVisibilityRecoveryStore,
        fs_mutation: InMemoryDurableFsMutationRecoveryStore,
        cleanup: InMemoryObjectCleanupClaimStore,
    }

    impl GcHarness {
        fn new() -> Self {
            Self {
                repo: repo(),
                objects: LocalMemoryObjectStore::new(),
                commits: LocalMemoryCommitStore::new(),
                refs: LocalMemoryRefStore::new(),
                workspaces: InMemoryWorkspaceMetadataStore::new(),
                reviews: InMemoryReviewStore::new(),
                idempotency: crate::idempotency::InMemoryIdempotencyStore::new(),
                post_cas: InMemoryDurableCorePostCasRecoveryClaimStore::new(),
                pre_visibility: InMemoryDurableCorePreVisibilityRecoveryStore::new(),
                fs_mutation: InMemoryDurableFsMutationRecoveryStore::new(),
                cleanup: InMemoryObjectCleanupClaimStore::new(),
            }
        }

        fn gc(&self) -> ObjectGcDryRun<'_> {
            ObjectGcDryRun::new(
                &self.objects,
                &self.commits,
                &self.refs,
                &self.workspaces,
                &self.reviews,
                &self.idempotency,
                &self.post_cas,
                &self.pre_visibility,
                &self.fs_mutation,
                &self.cleanup,
            )
        }

        async fn seed_blob(&self, bytes: &[u8]) -> ObjectId {
            let id = object_id(bytes);
            self.objects
                .put(ObjectWrite {
                    repo_id: self.repo.clone(),
                    id,
                    kind: ObjectKind::Blob,
                    bytes: bytes.to_vec(),
                })
                .await
                .unwrap();
            id
        }

        async fn seed_tree(&self, entries: Vec<TreeEntry>) -> ObjectId {
            let bytes = TreeObject { entries }.serialize();
            self.seed_raw_tree(&bytes).await
        }

        async fn seed_raw_tree(&self, bytes: &[u8]) -> ObjectId {
            let id = object_id(bytes);
            self.objects
                .put(ObjectWrite {
                    repo_id: self.repo.clone(),
                    id,
                    kind: ObjectKind::Tree,
                    bytes: bytes.to_vec(),
                })
                .await
                .unwrap();
            id
        }

        async fn seed_commit(&self, name: &str, parents: Vec<CommitId>) -> CommitId {
            let blob = self.seed_blob(format!("{name}-blob").as_bytes()).await;
            let root_tree = self
                .seed_tree(vec![TreeEntry {
                    name: "file.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: blob,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    mime_type: None,
                    custom_attrs: Default::default(),
                }])
                .await;
            let id = commit_id(name);
            self.commits
                .insert(CommitRecord {
                    repo_id: self.repo.clone(),
                    id,
                    root_tree,
                    parents,
                    timestamp: 1,
                    message: format!("{name} message"),
                    author: "agent".to_string(),
                    changed_paths: Vec::new(),
                })
                .await
                .unwrap();
            id
        }

        async fn commit(&self, id: CommitId) -> CommitRecord {
            self.commits
                .get(&self.repo, id)
                .await
                .unwrap()
                .expect("commit should exist")
        }

        async fn update_ref(&self, name: &str, target: CommitId) {
            self.refs
                .update(RefUpdate {
                    repo_id: self.repo.clone(),
                    name: RefName::new(name).unwrap(),
                    target,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
        }

        async fn claim_object(
            &self,
            claim_kind: ObjectCleanupClaimKind,
            object_kind: ObjectKind,
            object_id: ObjectId,
            owner: &str,
        ) -> ObjectCleanupClaim {
            self.cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: self.repo.clone(),
                    claim_kind,
                    object_kind,
                    object_id,
                    object_key: canonical_final_object_key(&self.repo, object_kind, &object_id),
                    lease_owner: owner.to_string(),
                    lease_duration: Duration::from_secs(60),
                })
                .await
                .unwrap()
                .expect("claim should be acquired")
        }

        async fn enqueue_post_cas(&self, commit: CommitId) {
            self.post_cas
                .enqueue(
                    DurableCorePostCasRecoveryTarget::new(
                        self.repo.clone(),
                        MAIN_REF,
                        commit,
                        DurableCorePostCasStep::IdempotencyCompletion,
                    )
                    .unwrap(),
                    1,
                )
                .await
                .unwrap();
        }

        async fn enqueue_pre_visibility(&self, commit: CommitId) {
            let record = self.commit(commit).await;
            self.pre_visibility
                .record(DurableCorePreVisibilityRecoveryRecord::new(
                    DurableCorePreVisibilityRecoveryTarget::new(
                        self.repo.clone(),
                        MAIN_REF,
                        commit,
                        DurableCorePreVisibilityRecoveryStage::CommitMetadataInsert,
                    )
                    .unwrap(),
                    record.root_tree,
                    None,
                    crate::backend::RefVersion::new(1).unwrap(),
                    1,
                    0,
                    false,
                    1,
                ))
                .await
                .unwrap();
        }

        async fn enqueue_fs_mutation(&self, previous: CommitId, new: CommitId) {
            self.fs_mutation
                .enqueue(
                    DurableFsMutationRecoveryTarget::new(
                        self.repo.clone(),
                        "repo:repo_cleanup:workspace",
                        "op-1",
                        MAIN_REF,
                        previous,
                        new,
                        DurableFsMutationRecoveryStep::IdempotencyCompletion,
                    )
                    .unwrap(),
                    DurableFsMutationRecoveryEnvelope::new(None, None, None),
                    1,
                )
                .await
                .unwrap();
        }

        async fn complete_idempotency_with_commit(&self, commit: CommitId) {
            let key =
                IdempotencyKey::parse_header_value(&HeaderValue::from_static("gc-idempotency"))
                    .unwrap();
            let reservation = match self
                .idempotency
                .begin("repo:repo_cleanup:gc", &key, "fingerprint")
                .await
                .unwrap()
            {
                crate::idempotency::IdempotencyBegin::Execute(reservation) => reservation,
                _ => panic!("fresh idempotency key should execute"),
            };
            self.idempotency
                .complete(
                    &reservation,
                    200,
                    json!({
                        "target": commit.to_hex(),
                        "ignored_secret": "do not expose"
                    }),
                )
                .await
                .unwrap();
        }
    }

    #[derive(Clone, Copy)]
    struct CommitBlobRoot {
        commit: CommitId,
        blob: ObjectId,
        blob_bytes: &'static [u8],
    }

    struct WorkerHarness {
        repo: RepoId,
        objects: BlobObjectStore,
        metadata: Arc<InMemoryObjectMetadataStore>,
        commits: LocalMemoryCommitStore,
        refs: LocalMemoryRefStore,
        workspaces: InMemoryWorkspaceMetadataStore,
        reviews: InMemoryReviewStore,
        idempotency: crate::idempotency::InMemoryIdempotencyStore,
        post_cas: InMemoryDurableCorePostCasRecoveryClaimStore,
        pre_visibility: InMemoryDurableCorePreVisibilityRecoveryStore,
        fs_mutation: InMemoryDurableFsMutationRecoveryStore,
        cleanup: HookedCleanupClaimStore,
        block_idempotency_roots: Arc<std::sync::atomic::AtomicBool>,
        steal_claim_after_fence: Arc<std::sync::atomic::AtomicBool>,
        _temp_dir: std::path::PathBuf,
    }

    impl WorkerHarness {
        fn new() -> Self {
            let metadata = Arc::new(InMemoryObjectMetadataStore::new());
            let temp_dir = std::env::temp_dir()
                .join(format!("stratum-object-cleanup-worker-{}", Uuid::new_v4()));
            let blobs = Arc::new(crate::remote::blob::LocalBlobStore::new(&temp_dir));
            let block_idempotency_roots = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let steal_claim_after_fence = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let cleanup = HookedCleanupClaimStore {
                inner: InMemoryObjectCleanupClaimStore::new(),
                steal_on_validate: steal_claim_after_fence.clone(),
            };
            Self {
                repo: repo(),
                objects: BlobObjectStore::new(blobs.clone(), metadata.clone()),
                metadata,
                commits: LocalMemoryCommitStore::new(),
                refs: LocalMemoryRefStore::new(),
                workspaces: InMemoryWorkspaceMetadataStore::new(),
                reviews: InMemoryReviewStore::new(),
                idempotency: crate::idempotency::InMemoryIdempotencyStore::new(),
                post_cas: InMemoryDurableCorePostCasRecoveryClaimStore::new(),
                pre_visibility: InMemoryDurableCorePreVisibilityRecoveryStore::new(),
                fs_mutation: InMemoryDurableFsMutationRecoveryStore::new(),
                cleanup,
                block_idempotency_roots,
                steal_claim_after_fence,
                _temp_dir: temp_dir,
            }
        }

        fn worker(&self) -> ObjectCleanupWorker<'_> {
            ObjectCleanupWorker::new(
                &self.repo,
                &self.objects,
                self.metadata.as_ref(),
                &self.commits,
                &self.refs,
                &self.workspaces,
                &self.reviews,
                self.idempotency_store(),
                &self.post_cas,
                &self.pre_visibility,
                &self.fs_mutation,
                &self.cleanup,
            )
        }

        fn object_key(&self, kind: ObjectKind, id: ObjectId) -> String {
            canonical_final_object_key(&self.repo, kind, &id)
        }

        async fn remove_metadata(&self, kind: ObjectKind, id: ObjectId) {
            let fence = self
                .metadata
                .acquire_final_object_metadata_fence(FinalObjectMetadataFenceRequest::new(
                    self.repo.clone(),
                    kind,
                    id,
                    self.object_key(kind, id),
                    "test-metadata-removal".to_string(),
                    Duration::from_secs(60),
                ))
                .await
                .unwrap()
                .expect("metadata fence should be acquired");
            self.metadata
                .delete_with_final_object_metadata_fence(&fence)
                .await
                .unwrap();
            self.metadata
                .release_final_object_metadata_fence(&fence)
                .await
                .unwrap();
        }

        fn idempotency_store(&self) -> &dyn IdempotencyStore {
            if self
                .block_idempotency_roots
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                &FailingIdempotencyStore
            } else {
                &self.idempotency
            }
        }

        async fn seed_blob(&self, bytes: &[u8]) -> ObjectId {
            let id = object_id(bytes);
            self.objects
                .put(ObjectWrite {
                    repo_id: self.repo.clone(),
                    id,
                    kind: ObjectKind::Blob,
                    bytes: bytes.to_vec(),
                })
                .await
                .unwrap();
            id
        }

        async fn seed_commit_with_blob(&self, name: &'static str) -> CommitBlobRoot {
            let blob_bytes = match name {
                "worker-ref" => b"worker-ref-blob".as_slice(),
                "worker-workspace" => b"worker-workspace-blob".as_slice(),
                "worker-post-cas" => b"worker-post-cas-blob".as_slice(),
                "worker-idempotency" => b"worker-idempotency-blob".as_slice(),
                "worker-review" => b"worker-review-blob".as_slice(),
                _ => b"worker-blob".as_slice(),
            };
            let blob = self.seed_blob(blob_bytes).await;
            let tree_bytes = TreeObject {
                entries: vec![TreeEntry {
                    name: "file.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: blob,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    mime_type: None,
                    custom_attrs: Default::default(),
                }],
            }
            .serialize();
            let tree = object_id(&tree_bytes);
            self.objects
                .put(ObjectWrite {
                    repo_id: self.repo.clone(),
                    id: tree,
                    kind: ObjectKind::Tree,
                    bytes: tree_bytes,
                })
                .await
                .unwrap();
            let commit = commit_id(name);
            self.commits
                .insert(CommitRecord {
                    repo_id: self.repo.clone(),
                    id: commit,
                    root_tree: tree,
                    parents: Vec::new(),
                    timestamp: 1,
                    message: "worker root".to_string(),
                    author: "worker".to_string(),
                    changed_paths: Vec::new(),
                })
                .await
                .unwrap();
            CommitBlobRoot {
                commit,
                blob,
                blob_bytes,
            }
        }

        async fn update_ref(&self, name: &str, target: CommitId) {
            self.refs
                .update(RefUpdate {
                    repo_id: self.repo.clone(),
                    name: RefName::new(name).unwrap(),
                    target,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
        }

        async fn claim_object(
            &self,
            claim_kind: ObjectCleanupClaimKind,
            object_kind: ObjectKind,
            object_id: ObjectId,
            owner: &str,
        ) -> ObjectCleanupClaim {
            self.cleanup
                .claim(ObjectCleanupClaimRequest {
                    repo_id: self.repo.clone(),
                    claim_kind,
                    object_kind,
                    object_id,
                    object_key: self.object_key(object_kind, object_id),
                    lease_owner: owner.to_string(),
                    lease_duration: Duration::from_secs(60),
                })
                .await
                .unwrap()
                .expect("claim should be acquired")
        }

        async fn enqueue_post_cas(&self, commit: CommitId) {
            self.post_cas
                .enqueue(
                    DurableCorePostCasRecoveryTarget::new(
                        self.repo.clone(),
                        MAIN_REF,
                        commit,
                        DurableCorePostCasStep::IdempotencyCompletion,
                    )
                    .unwrap(),
                    1,
                )
                .await
                .unwrap();
        }

        async fn complete_idempotency_with_commit(&self, commit: CommitId) {
            let key =
                IdempotencyKey::parse_header_value(&HeaderValue::from_static("worker-cleanup"))
                    .unwrap();
            let reservation = match self
                .idempotency
                .begin("repo:repo_cleanup:worker", &key, "fingerprint")
                .await
                .unwrap()
            {
                crate::idempotency::IdempotencyBegin::Execute(reservation) => reservation,
                _ => panic!("fresh idempotency key should execute"),
            };
            self.idempotency
                .complete(&reservation, 200, json!({ "target": commit.to_hex() }))
                .await
                .unwrap();
        }
    }

    struct HookedCleanupClaimStore {
        inner: InMemoryObjectCleanupClaimStore,
        steal_on_validate: Arc<std::sync::atomic::AtomicBool>,
    }

    impl HookedCleanupClaimStore {
        async fn set_now_for_tests(&self, now: SystemTime) {
            self.inner.set_now_for_tests(now).await;
        }
    }

    struct FailingIdempotencyStore;

    #[async_trait]
    impl IdempotencyStore for FailingIdempotencyStore {
        async fn begin(
            &self,
            _scope: &str,
            _key: &IdempotencyKey,
            _request_fingerprint: &str,
        ) -> Result<crate::idempotency::IdempotencyBegin, VfsError> {
            Err(VfsError::CorruptStore {
                message: "blocked roots redacted".to_string(),
            })
        }

        async fn complete(
            &self,
            _reservation: &crate::idempotency::IdempotencyReservation,
            _status_code: u16,
            _response_body: serde_json::Value,
        ) -> Result<(), VfsError> {
            Err(VfsError::CorruptStore {
                message: "blocked roots redacted".to_string(),
            })
        }

        async fn abort(&self, _reservation: &crate::idempotency::IdempotencyReservation) {}

        async fn list_retained_for_repo(
            &self,
            _repo_id: &RepoId,
            _limit: usize,
        ) -> Result<Vec<crate::idempotency::RetainedIdempotencyRecord>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "blocked roots redacted".to_string(),
            })
        }
    }

    #[async_trait]
    impl ObjectCleanupClaimStore for HookedCleanupClaimStore {
        async fn claim(
            &self,
            request: ObjectCleanupClaimRequest,
        ) -> Result<Option<ObjectCleanupClaim>, VfsError> {
            self.inner.claim(request).await
        }

        async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
            self.inner.complete(claim).await
        }

        async fn record_failure(
            &self,
            claim: &ObjectCleanupClaim,
            message: &str,
        ) -> Result<(), VfsError> {
            self.inner.record_failure(claim, message).await
        }

        async fn release(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
            self.inner.release(claim).await
        }

        async fn validate(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
            if self
                .steal_on_validate
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.inner
                    .set_now_for_tests(SystemTime::now() + Duration::from_secs(10_000))
                    .await;
                let _ = self
                    .inner
                    .claim(ObjectCleanupClaimRequest {
                        repo_id: claim.repo_id.clone(),
                        claim_kind: claim.claim_kind,
                        object_kind: claim.object_kind,
                        object_id: claim.object_id,
                        object_key: claim.object_key.clone(),
                        lease_owner: "stealer".to_string(),
                        lease_duration: Duration::from_secs(60),
                    })
                    .await?;
            }
            self.inner.validate(claim).await
        }

        async fn list(&self, limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
            self.inner.list(limit).await
        }

        async fn list_for_repo(
            &self,
            repo_id: &RepoId,
            limit: usize,
        ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
            self.inner.list_for_repo(repo_id, limit).await
        }

        async fn list_claimable_for_repo_and_kind(
            &self,
            repo_id: &RepoId,
            claim_kind: ObjectCleanupClaimKind,
            limit: usize,
        ) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
            self.inner
                .list_claimable_for_repo_and_kind(repo_id, claim_kind, limit)
                .await
        }

        async fn counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError> {
            self.inner.counts().await
        }

        async fn counts_for_repo(
            &self,
            repo_id: &RepoId,
        ) -> Result<ObjectCleanupClaimCounts, VfsError> {
            self.inner.counts_for_repo(repo_id).await
        }
    }

    fn commit_id(name: &str) -> CommitId {
        CommitId::from(object_id(name.as_bytes()))
    }

    struct FailingRefStore;

    #[async_trait]
    impl RefStore for FailingRefStore {
        async fn list(&self, _repo_id: &RepoId) -> Result<Vec<RefRecord>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw ref store failure".to_string(),
            })
        }

        async fn get(
            &self,
            _repo_id: &RepoId,
            _name: &RefName,
        ) -> Result<Option<RefRecord>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw ref store failure".to_string(),
            })
        }

        async fn update(&self, _update: RefUpdate) -> Result<RefRecord, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw ref store failure".to_string(),
            })
        }

        async fn update_source_checked(
            &self,
            _update: SourceCheckedRefUpdate,
        ) -> Result<RefRecord, VfsError> {
            Err(VfsError::CorruptStore {
                message: "raw ref store failure".to_string(),
            })
        }
    }
}
