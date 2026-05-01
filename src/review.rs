use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::auth::Uid;
use crate::error::VfsError;
use crate::store::ObjectId;
use crate::vcs::RefName;

const REVIEW_STORE_VERSION: u32 = 4;
const APPROVAL_COMMENT_MAX_BYTES: usize = 4096;
const REVIEW_COMMENT_MAX_BYTES: usize = 8192;

pub type SharedReviewStore = Arc<dyn ReviewStore>;

#[async_trait]
pub trait ReviewStore: Send + Sync {
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError>;

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError>;

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError>;

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError>;

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError>;

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError>;

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError>;

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError>;

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError>;

    async fn transition_change_request(
        &self,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError>;

    async fn create_approval(
        &self,
        input: NewApprovalRecord,
    ) -> Result<ApprovalRecordMutation, VfsError>;

    async fn list_approvals(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ApprovalRecord>, VfsError>;

    async fn assign_reviewer(
        &self,
        input: NewReviewAssignment,
    ) -> Result<ReviewAssignmentMutation, VfsError>;

    async fn list_reviewer_assignments(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ReviewAssignment>, VfsError>;

    async fn create_comment(
        &self,
        input: NewReviewComment,
    ) -> Result<ReviewCommentMutation, VfsError>;

    async fn list_comments(&self, change_request_id: Uuid) -> Result<Vec<ReviewComment>, VfsError>;

    async fn dismiss_approval(
        &self,
        input: DismissApprovalInput,
    ) -> Result<ApprovalDismissalMutation, VfsError>;

    async fn approval_decision(
        &self,
        change_request_id: Uuid,
        changed_paths: &[String],
    ) -> Result<Option<ApprovalPolicyDecision>, VfsError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedRefRule {
    pub id: Uuid,
    pub ref_name: String,
    pub required_approvals: u32,
    pub created_by: Uid,
    pub active: bool,
}

impl ProtectedRefRule {
    pub fn new(ref_name: &str, required_approvals: u32, created_by: Uid) -> Result<Self, VfsError> {
        if required_approvals == 0 {
            return Err(VfsError::InvalidArgs {
                message: "required approvals must be greater than zero".to_string(),
            });
        }

        Ok(Self {
            id: Uuid::new_v4(),
            ref_name: RefName::new(ref_name)?.into_string(),
            required_approvals,
            created_by,
            active: true,
        })
    }

    fn validate(&self) -> Result<(), VfsError> {
        RefName::new(self.ref_name.clone())?;
        validate_required_approvals(self.required_approvals)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedPathRule {
    pub id: Uuid,
    pub path_prefix: String,
    pub target_ref: Option<String>,
    pub required_approvals: u32,
    pub created_by: Uid,
    pub active: bool,
}

impl ProtectedPathRule {
    pub fn new(
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<Self, VfsError> {
        validate_required_approvals(required_approvals)?;
        let path_prefix = normalize_path_prefix(path_prefix)?;
        let target_ref = target_ref
            .map(|name| RefName::new(name).map(RefName::into_string))
            .transpose()?;

        Ok(Self {
            id: Uuid::new_v4(),
            path_prefix,
            target_ref,
            required_approvals,
            created_by,
            active: true,
        })
    }

    pub fn matches_path(&self, path: &str) -> bool {
        if !self.active {
            return false;
        }

        let Ok(path) = normalize_path_prefix(path) else {
            return false;
        };
        if self.path_prefix == "/" {
            return true;
        }
        path == self.path_prefix
            || path
                .strip_prefix(&self.path_prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    fn validate(&self) -> Result<(), VfsError> {
        normalize_path_prefix(&self.path_prefix)?;
        if let Some(target_ref) = &self.target_ref {
            RefName::new(target_ref.clone())?;
        }
        validate_required_approvals(self.required_approvals)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeRequestStatus {
    Open,
    Merged,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRequest {
    pub id: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub source_ref: String,
    pub target_ref: String,
    pub base_commit: String,
    pub head_commit: String,
    pub status: ChangeRequestStatus,
    pub created_by: Uid,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewChangeRequest {
    pub title: String,
    pub description: Option<String>,
    pub source_ref: String,
    pub target_ref: String,
    pub base_commit: String,
    pub head_commit: String,
    pub created_by: Uid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub id: Uuid,
    pub change_request_id: Uuid,
    pub head_commit: String,
    pub approved_by: Uid,
    pub comment: Option<String>,
    pub active: bool,
    pub dismissed_by: Option<Uid>,
    pub dismissal_reason: Option<String>,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewApprovalRecord {
    pub change_request_id: Uuid,
    pub head_commit: String,
    pub approved_by: Uid,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecordMutation {
    pub record: ApprovalRecord,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewAssignment {
    pub id: Uuid,
    pub change_request_id: Uuid,
    pub reviewer: Uid,
    pub assigned_by: Uid,
    pub required: bool,
    pub active: bool,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewReviewAssignment {
    pub change_request_id: Uuid,
    pub reviewer: Uid,
    pub assigned_by: Uid,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewAssignmentMutation {
    pub assignment: ReviewAssignment,
    pub created: bool,
    pub updated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewCommentKind {
    General,
    ChangesRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewComment {
    pub id: Uuid,
    pub change_request_id: Uuid,
    pub author: Uid,
    pub body: String,
    pub path: Option<String>,
    pub kind: ReviewCommentKind,
    pub active: bool,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewReviewComment {
    pub change_request_id: Uuid,
    pub author: Uid,
    pub body: String,
    pub path: Option<String>,
    pub kind: ReviewCommentKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewCommentMutation {
    pub comment: ReviewComment,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DismissApprovalInput {
    pub change_request_id: Uuid,
    pub approval_id: Uuid,
    pub dismissed_by: Uid,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDismissalMutation {
    pub record: ApprovalRecord,
    pub dismissed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalPolicyDecision {
    pub change_request_id: Uuid,
    pub required_approvals: u32,
    pub approval_count: u32,
    pub approved_by: Vec<Uid>,
    pub required_reviewers: Vec<Uid>,
    pub approved_required_reviewers: Vec<Uid>,
    pub missing_required_reviewers: Vec<Uid>,
    pub approved: bool,
    pub matched_ref_rules: Vec<Uuid>,
    pub matched_path_rules: Vec<Uuid>,
}

impl ChangeRequest {
    pub fn new(input: NewChangeRequest) -> Result<Self, VfsError> {
        let title = input.title.trim().to_string();
        if title.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: "change request title must not be empty".to_string(),
            });
        }
        validate_commit_hex(&input.base_commit)?;
        validate_commit_hex(&input.head_commit)?;

        Ok(Self {
            id: Uuid::new_v4(),
            title,
            description: input.description,
            source_ref: RefName::new(input.source_ref)?.into_string(),
            target_ref: RefName::new(input.target_ref)?.into_string(),
            base_commit: input.base_commit,
            head_commit: input.head_commit,
            status: ChangeRequestStatus::Open,
            created_by: input.created_by,
            version: 1,
        })
    }

    fn transition(&self, status: ChangeRequestStatus) -> Result<Self, VfsError> {
        if self.status != ChangeRequestStatus::Open || status == ChangeRequestStatus::Open {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "invalid change request transition from {:?} to {:?}",
                    self.status, status
                ),
            });
        }

        let mut next = self.clone();
        next.status = status;
        next.version = next
            .version
            .checked_add(1)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: "change request version overflow".to_string(),
            })?;
        Ok(next)
    }

    fn validate(&self) -> Result<(), VfsError> {
        if self.version == 0 {
            return Err(VfsError::CorruptStore {
                message: format!("change request {} has zero version", self.id),
            });
        }
        if self.title.trim().is_empty() {
            return Err(VfsError::CorruptStore {
                message: format!("change request {} has empty title", self.id),
            });
        }
        RefName::new(self.source_ref.clone())?;
        RefName::new(self.target_ref.clone())?;
        validate_commit_hex(&self.base_commit)?;
        validate_commit_hex(&self.head_commit)
    }
}

impl ApprovalRecord {
    fn new(input: NewApprovalRecord, change: &ChangeRequest) -> Result<Self, VfsError> {
        validate_new_approval(&input, change)?;

        Ok(Self {
            id: Uuid::new_v4(),
            change_request_id: input.change_request_id,
            head_commit: input.head_commit,
            approved_by: input.approved_by,
            comment: normalize_approval_comment(input.comment)?,
            active: true,
            dismissed_by: None,
            dismissal_reason: None,
            version: 1,
        })
    }

    fn validate(&self, change: &ChangeRequest) -> Result<(), VfsError> {
        if self.version == 0 {
            return Err(VfsError::CorruptStore {
                message: format!("approval {} has zero version", self.id),
            });
        }
        validate_commit_hex(&self.head_commit)?;
        if self.change_request_id != change.id {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "approval {} belongs to unexpected change request {}",
                    self.id, self.change_request_id
                ),
            });
        }
        if self.head_commit != change.head_commit {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "approval {} head does not match change request {} head",
                    self.id, change.id
                ),
            });
        }
        if self.approved_by == change.created_by {
            return Err(VfsError::CorruptStore {
                message: format!("approval {} is a self-approval", self.id),
            });
        }
        if let Some(comment) = &self.comment {
            validate_approval_comment(comment)?;
        }
        match (
            self.active,
            self.dismissed_by,
            self.dismissal_reason.as_deref(),
        ) {
            (true, Some(_), _) | (true, _, Some(_)) => {
                return Err(VfsError::CorruptStore {
                    message: format!("active approval {} has dismissal metadata", self.id),
                });
            }
            (false, None, _) => {
                return Err(VfsError::CorruptStore {
                    message: format!("inactive approval {} has no dismissed_by", self.id),
                });
            }
            _ => {}
        }
        if let Some(reason) = &self.dismissal_reason {
            validate_dismissal_reason(reason)?;
        }
        Ok(())
    }
}

impl ReviewAssignment {
    fn new(input: NewReviewAssignment, change: &ChangeRequest) -> Result<Self, VfsError> {
        validate_new_assignment(&input, change)?;

        Ok(Self {
            id: Uuid::new_v4(),
            change_request_id: input.change_request_id,
            reviewer: input.reviewer,
            assigned_by: input.assigned_by,
            required: input.required,
            active: true,
            version: 1,
        })
    }

    fn validate(&self, change: &ChangeRequest) -> Result<(), VfsError> {
        if self.version == 0 {
            return Err(VfsError::CorruptStore {
                message: format!("review assignment {} has zero version", self.id),
            });
        }
        if !self.active {
            return Err(VfsError::CorruptStore {
                message: format!("review assignment {} is inactive", self.id),
            });
        }
        if self.change_request_id != change.id {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "review assignment {} belongs to unexpected change request {}",
                    self.id, self.change_request_id
                ),
            });
        }
        if self.reviewer == change.created_by {
            return Err(VfsError::CorruptStore {
                message: format!("review assignment {} assigns the author", self.id),
            });
        }
        Ok(())
    }
}

impl ReviewComment {
    fn new(input: NewReviewComment, change: &ChangeRequest) -> Result<Self, VfsError> {
        validate_comment_change(input.change_request_id, change)?;

        Ok(Self {
            id: Uuid::new_v4(),
            change_request_id: input.change_request_id,
            author: input.author,
            body: normalize_review_comment_body(input.body)?,
            path: normalize_optional_path(input.path)?,
            kind: input.kind,
            active: true,
            version: 1,
        })
    }

    fn validate(&self, change: &ChangeRequest) -> Result<(), VfsError> {
        if self.version == 0 {
            return Err(VfsError::CorruptStore {
                message: format!("review comment {} has zero version", self.id),
            });
        }
        validate_comment_change(self.change_request_id, change)?;
        validate_review_comment_body(&self.body)?;
        if let Some(path) = &self.path {
            normalize_path_prefix(path)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReviewState {
    protected_refs: BTreeMap<Uuid, ProtectedRefRule>,
    protected_paths: BTreeMap<Uuid, ProtectedPathRule>,
    change_requests: BTreeMap<Uuid, ChangeRequest>,
    approvals: BTreeMap<Uuid, ApprovalRecord>,
    assignments: BTreeMap<Uuid, ReviewAssignment>,
    comments: BTreeMap<Uuid, ReviewComment>,
}

impl ReviewState {
    fn list_protected_ref_rules(&self) -> Vec<ProtectedRefRule> {
        self.protected_refs.values().cloned().collect()
    }

    fn list_protected_path_rules(&self) -> Vec<ProtectedPathRule> {
        self.protected_paths.values().cloned().collect()
    }

    fn list_change_requests(&self) -> Vec<ChangeRequest> {
        self.change_requests.values().cloned().collect()
    }

    fn create_approval(
        &mut self,
        input: NewApprovalRecord,
    ) -> Result<ApprovalRecordMutation, VfsError> {
        let change = self
            .change_requests
            .get(&input.change_request_id)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            })?;
        validate_new_approval(&input, change)?;

        if let Some(record) = self.approvals.values().find(|record| {
            record.active
                && record.change_request_id == input.change_request_id
                && record.head_commit == input.head_commit
                && record.approved_by == input.approved_by
        }) {
            return Ok(ApprovalRecordMutation {
                record: record.clone(),
                created: false,
            });
        }

        let record = ApprovalRecord::new(input, change)?;
        self.approvals.insert(record.id, record.clone());
        Ok(ApprovalRecordMutation {
            record,
            created: true,
        })
    }

    fn list_approvals(&self, change_request_id: Uuid) -> Vec<ApprovalRecord> {
        self.approvals
            .values()
            .filter(|record| record.change_request_id == change_request_id)
            .cloned()
            .collect()
    }

    fn assign_reviewer(
        &mut self,
        input: NewReviewAssignment,
    ) -> Result<ReviewAssignmentMutation, VfsError> {
        let change = self
            .change_requests
            .get(&input.change_request_id)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            })?;
        validate_new_assignment(&input, change)?;

        let existing_id = self
            .assignments
            .values()
            .find(|assignment| {
                assignment.active
                    && assignment.change_request_id == input.change_request_id
                    && assignment.reviewer == input.reviewer
            })
            .map(|assignment| assignment.id);

        if let Some(existing_id) = existing_id {
            let assignment = self
                .assignments
                .get_mut(&existing_id)
                .expect("assignment ID came from assignments map");
            if assignment.required == input.required {
                return Ok(ReviewAssignmentMutation {
                    assignment: assignment.clone(),
                    created: false,
                    updated: false,
                });
            }
            assignment.required = input.required;
            assignment.assigned_by = input.assigned_by;
            assignment.version =
                assignment
                    .version
                    .checked_add(1)
                    .ok_or_else(|| VfsError::InvalidArgs {
                        message: "review assignment version overflow".to_string(),
                    })?;
            return Ok(ReviewAssignmentMutation {
                assignment: assignment.clone(),
                created: false,
                updated: true,
            });
        }

        let assignment = ReviewAssignment::new(input, change)?;
        self.assignments.insert(assignment.id, assignment.clone());
        Ok(ReviewAssignmentMutation {
            assignment,
            created: true,
            updated: false,
        })
    }

    fn list_reviewer_assignments(&self, change_request_id: Uuid) -> Vec<ReviewAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.change_request_id == change_request_id)
            .cloned()
            .collect()
    }

    fn create_comment(
        &mut self,
        input: NewReviewComment,
    ) -> Result<ReviewCommentMutation, VfsError> {
        let change = self
            .change_requests
            .get(&input.change_request_id)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            })?;

        let comment = ReviewComment::new(input, change)?;
        self.comments.insert(comment.id, comment.clone());
        Ok(ReviewCommentMutation {
            comment,
            created: true,
        })
    }

    fn list_comments(&self, change_request_id: Uuid) -> Vec<ReviewComment> {
        self.comments
            .values()
            .filter(|comment| comment.change_request_id == change_request_id)
            .cloned()
            .collect()
    }

    fn dismiss_approval(
        &mut self,
        input: DismissApprovalInput,
    ) -> Result<ApprovalDismissalMutation, VfsError> {
        let record =
            self.approvals
                .get_mut(&input.approval_id)
                .ok_or_else(|| VfsError::InvalidArgs {
                    message: format!("unknown approval {}", input.approval_id),
                })?;
        if record.change_request_id != input.change_request_id {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "approval {} does not belong to change request {}",
                    input.approval_id, input.change_request_id
                ),
            });
        }
        if !self.change_requests.contains_key(&input.change_request_id) {
            return Err(VfsError::InvalidArgs {
                message: format!("unknown change request {}", input.change_request_id),
            });
        }
        let dismissal_reason = normalize_dismissal_reason(input.reason)?;
        if !record.active {
            return Ok(ApprovalDismissalMutation {
                record: record.clone(),
                dismissed: false,
            });
        }

        let next_version = record
            .version
            .checked_add(1)
            .ok_or_else(|| VfsError::InvalidArgs {
                message: "approval version overflow".to_string(),
            })?;

        record.active = false;
        record.dismissed_by = Some(input.dismissed_by);
        record.dismissal_reason = dismissal_reason;
        record.version = next_version;
        Ok(ApprovalDismissalMutation {
            record: record.clone(),
            dismissed: true,
        })
    }

    fn approval_decision(
        &self,
        change_request_id: Uuid,
        changed_paths: &[String],
    ) -> Option<ApprovalPolicyDecision> {
        let change = self.change_requests.get(&change_request_id)?;
        let mut required_approvals = 0;
        let mut matched_ref_rules = Vec::new();
        let mut matched_path_rules = Vec::new();

        for rule in self.protected_refs.values() {
            if rule.active && rule.ref_name == change.target_ref {
                required_approvals = required_approvals.max(rule.required_approvals);
                matched_ref_rules.push(rule.id);
            }
        }

        for rule in self.protected_paths.values() {
            let target_matches = rule
                .target_ref
                .as_ref()
                .is_none_or(|target_ref| target_ref == &change.target_ref);
            if rule.active
                && target_matches
                && changed_paths.iter().any(|path| rule.matches_path(path))
            {
                required_approvals = required_approvals.max(rule.required_approvals);
                matched_path_rules.push(rule.id);
            }
        }

        let approved_by: BTreeSet<Uid> = self
            .approvals
            .values()
            .filter(|record| {
                record.active
                    && record.change_request_id == change.id
                    && record.head_commit == change.head_commit
            })
            .map(|record| record.approved_by)
            .collect();
        let approved_by: Vec<Uid> = approved_by.into_iter().collect();
        let approval_count = approved_by.len().try_into().unwrap_or(u32::MAX);
        let approved_by_set: BTreeSet<Uid> = approved_by.iter().copied().collect();
        let required_reviewers: Vec<Uid> = self
            .assignments
            .values()
            .filter(|assignment| {
                assignment.active
                    && assignment.change_request_id == change.id
                    && assignment.required
            })
            .map(|assignment| assignment.reviewer)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let (approved_required_reviewers, missing_required_reviewers): (Vec<_>, Vec<_>) =
            required_reviewers
                .iter()
                .copied()
                .partition(|reviewer| approved_by_set.contains(reviewer));
        let required_reviewers_satisfied = missing_required_reviewers.is_empty();

        Some(ApprovalPolicyDecision {
            change_request_id,
            required_approvals,
            approval_count,
            approved_by,
            required_reviewers,
            approved_required_reviewers,
            missing_required_reviewers,
            approved: approval_count >= required_approvals && required_reviewers_satisfied,
            matched_ref_rules,
            matched_path_rules,
        })
    }
}

#[derive(Debug, Default)]
pub struct InMemoryReviewStore {
    inner: RwLock<ReviewState>,
}

impl InMemoryReviewStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ReviewStore for InMemoryReviewStore {
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError> {
        let rule = ProtectedRefRule::new(ref_name, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        guard.protected_refs.insert(rule.id, rule.clone());
        Ok(rule)
    }

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_ref_rules())
    }

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_refs.get(&id).cloned())
    }

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError> {
        let rule = ProtectedPathRule::new(path_prefix, target_ref, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        guard.protected_paths.insert(rule.id, rule.clone());
        Ok(rule)
    }

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_path_rules())
    }

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_paths.get(&id).cloned())
    }

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError> {
        let change = ChangeRequest::new(input)?;
        let mut guard = self.inner.write().await;
        guard.change_requests.insert(change.id, change.clone());
        Ok(change)
    }

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_change_requests())
    }

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.change_requests.get(&id).cloned())
    }

    async fn transition_change_request(
        &self,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError> {
        let mut guard = self.inner.write().await;
        let Some(current) = guard.change_requests.get(&id) else {
            return Ok(None);
        };
        let next = current.transition(status)?;
        guard.change_requests.insert(id, next.clone());
        Ok(Some(next))
    }

    async fn create_approval(
        &self,
        input: NewApprovalRecord,
    ) -> Result<ApprovalRecordMutation, VfsError> {
        let mut guard = self.inner.write().await;
        guard.create_approval(input)
    }

    async fn list_approvals(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ApprovalRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_approvals(change_request_id))
    }

    async fn assign_reviewer(
        &self,
        input: NewReviewAssignment,
    ) -> Result<ReviewAssignmentMutation, VfsError> {
        let mut guard = self.inner.write().await;
        guard.assign_reviewer(input)
    }

    async fn list_reviewer_assignments(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ReviewAssignment>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_reviewer_assignments(change_request_id))
    }

    async fn create_comment(
        &self,
        input: NewReviewComment,
    ) -> Result<ReviewCommentMutation, VfsError> {
        let mut guard = self.inner.write().await;
        guard.create_comment(input)
    }

    async fn list_comments(&self, change_request_id: Uuid) -> Result<Vec<ReviewComment>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_comments(change_request_id))
    }

    async fn dismiss_approval(
        &self,
        input: DismissApprovalInput,
    ) -> Result<ApprovalDismissalMutation, VfsError> {
        let mut guard = self.inner.write().await;
        guard.dismiss_approval(input)
    }

    async fn approval_decision(
        &self,
        change_request_id: Uuid,
        changed_paths: &[String],
    ) -> Result<Option<ApprovalPolicyDecision>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.approval_decision(change_request_id, changed_paths))
    }
}

#[derive(Debug)]
pub struct LocalReviewStore {
    path: PathBuf,
    _lock: ReviewStoreLock,
    inner: RwLock<ReviewState>,
}

#[derive(Debug)]
struct ReviewStoreLock {
    path: PathBuf,
    owner_id: Uuid,
    file: Option<File>,
}

impl Drop for ReviewStoreLock {
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
struct PersistedReviewStore {
    version: u32,
    protected_refs: Vec<ProtectedRefRule>,
    protected_paths: Vec<ProtectedPathRule>,
    change_requests: Vec<ChangeRequest>,
    approvals: Vec<ApprovalRecord>,
    assignments: Vec<ReviewAssignment>,
    comments: Vec<ReviewComment>,
}

#[derive(Serialize, Deserialize)]
struct PersistedReviewStoreV3 {
    version: u32,
    protected_refs: Vec<ProtectedRefRule>,
    protected_paths: Vec<ProtectedPathRule>,
    change_requests: Vec<ChangeRequest>,
    approvals: Vec<ApprovalRecord>,
    comments: Vec<ReviewComment>,
}

#[derive(Serialize, Deserialize)]
struct PersistedReviewStoreV2 {
    version: u32,
    protected_refs: Vec<ProtectedRefRule>,
    protected_paths: Vec<ProtectedPathRule>,
    change_requests: Vec<ChangeRequest>,
    approvals: Vec<ApprovalRecordV2>,
}

#[derive(Serialize, Deserialize)]
struct ApprovalRecordV2 {
    id: Uuid,
    change_request_id: Uuid,
    head_commit: String,
    approved_by: Uid,
    comment: Option<String>,
    active: bool,
    version: u64,
}

#[derive(Serialize, Deserialize)]
struct PersistedReviewStoreV1 {
    version: u32,
    protected_refs: Vec<ProtectedRefRule>,
    protected_paths: Vec<ProtectedPathRule>,
    change_requests: Vec<ChangeRequest>,
}

impl From<ApprovalRecordV2> for ApprovalRecord {
    fn from(record: ApprovalRecordV2) -> Self {
        Self {
            id: record.id,
            change_request_id: record.change_request_id,
            head_commit: record.head_commit,
            approved_by: record.approved_by,
            comment: record.comment,
            active: record.active,
            dismissed_by: None,
            dismissal_reason: None,
            version: record.version,
        }
    }
}

impl LocalReviewStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, VfsError> {
        let path = path.as_ref().to_path_buf();
        let lock = Self::acquire_lock(&path)?;
        let state = match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ReviewState::default(),
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            path,
            _lock: lock,
            inner: RwLock::new(state),
        })
    }

    fn acquire_lock(path: &Path) -> Result<ReviewStoreLock, VfsError> {
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
                        "failed to acquire review store lock '{}': {e}",
                        lock_path.display()
                    ),
                ))
            })?;
        {
            use std::io::Write;
            file.write_all(owner_id.to_string().as_bytes())?;
            file.sync_all()?;
        }
        Ok(ReviewStoreLock {
            path: lock_path,
            owner_id,
            file: Some(file),
        })
    }

    fn decode(bytes: &[u8]) -> Result<ReviewState, VfsError> {
        let persisted = match crate::codec::deserialize::<PersistedReviewStore>(bytes) {
            Ok(persisted) => {
                if persisted.version != REVIEW_STORE_VERSION {
                    return Err(VfsError::CorruptStore {
                        message: format!("unsupported review store version {}", persisted.version),
                    });
                }
                persisted
            }
            Err(v4_error) => match crate::codec::deserialize::<PersistedReviewStoreV3>(bytes) {
                Ok(v3) => {
                    if v3.version != 3 {
                        return Err(VfsError::CorruptStore {
                            message: format!("unsupported review store version {}", v3.version),
                        });
                    }
                    PersistedReviewStore {
                        version: REVIEW_STORE_VERSION,
                        protected_refs: v3.protected_refs,
                        protected_paths: v3.protected_paths,
                        change_requests: v3.change_requests,
                        approvals: v3.approvals,
                        assignments: Vec::new(),
                        comments: v3.comments,
                    }
                }
                Err(_) => match crate::codec::deserialize::<PersistedReviewStoreV2>(bytes) {
                    Ok(v2) => {
                        if v2.version != 2 {
                            return Err(VfsError::CorruptStore {
                                message: format!("unsupported review store version {}", v2.version),
                            });
                        }
                        PersistedReviewStore {
                            version: REVIEW_STORE_VERSION,
                            protected_refs: v2.protected_refs,
                            protected_paths: v2.protected_paths,
                            change_requests: v2.change_requests,
                            approvals: v2.approvals.into_iter().map(ApprovalRecord::from).collect(),
                            assignments: Vec::new(),
                            comments: Vec::new(),
                        }
                    }
                    Err(_) => {
                        let v1 = crate::codec::deserialize::<PersistedReviewStoreV1>(bytes)
                            .map_err(|_| VfsError::CorruptStore {
                                message: format!("review store decode failed: {v4_error}"),
                            })?;
                        if v1.version != 1 {
                            return Err(VfsError::CorruptStore {
                                message: format!("unsupported review store version {}", v1.version),
                            });
                        }
                        PersistedReviewStore {
                            version: REVIEW_STORE_VERSION,
                            protected_refs: v1.protected_refs,
                            protected_paths: v1.protected_paths,
                            change_requests: v1.change_requests,
                            approvals: Vec::new(),
                            assignments: Vec::new(),
                            comments: Vec::new(),
                        }
                    }
                },
            },
        };

        let mut ids = HashSet::new();
        let mut state = ReviewState::default();
        for rule in persisted.protected_refs {
            reject_duplicate_id(&mut ids, rule.id)?;
            rule.validate().map_err(corrupt_record)?;
            state.protected_refs.insert(rule.id, rule);
        }
        for rule in persisted.protected_paths {
            reject_duplicate_id(&mut ids, rule.id)?;
            rule.validate().map_err(corrupt_record)?;
            state.protected_paths.insert(rule.id, rule);
        }
        for change in persisted.change_requests {
            reject_duplicate_id(&mut ids, change.id)?;
            change.validate().map_err(corrupt_record)?;
            state.change_requests.insert(change.id, change);
        }
        let mut active_approvals = HashSet::new();
        for approval in persisted.approvals {
            reject_duplicate_id(&mut ids, approval.id)?;
            let change = state
                .change_requests
                .get(&approval.change_request_id)
                .ok_or_else(|| VfsError::CorruptStore {
                    message: format!(
                        "approval {} references unknown change request {}",
                        approval.id, approval.change_request_id
                    ),
                })?;
            approval.validate(change).map_err(corrupt_record)?;
            if approval.active
                && !active_approvals.insert((
                    approval.change_request_id,
                    approval.head_commit.clone(),
                    approval.approved_by,
                ))
            {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "duplicate active approval by {} for change request {} at {}",
                        approval.approved_by, approval.change_request_id, approval.head_commit
                    ),
                });
            }
            state.approvals.insert(approval.id, approval);
        }
        let mut active_assignments = HashSet::new();
        for assignment in persisted.assignments {
            reject_duplicate_id(&mut ids, assignment.id)?;
            let change = state
                .change_requests
                .get(&assignment.change_request_id)
                .ok_or_else(|| VfsError::CorruptStore {
                    message: format!(
                        "review assignment {} references unknown change request {}",
                        assignment.id, assignment.change_request_id
                    ),
                })?;
            assignment.validate(change).map_err(corrupt_record)?;
            if assignment.active
                && !active_assignments.insert((assignment.change_request_id, assignment.reviewer))
            {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "duplicate active review assignment for reviewer {} on change request {}",
                        assignment.reviewer, assignment.change_request_id
                    ),
                });
            }
            state.assignments.insert(assignment.id, assignment);
        }
        for comment in persisted.comments {
            reject_duplicate_id(&mut ids, comment.id)?;
            let change = state
                .change_requests
                .get(&comment.change_request_id)
                .ok_or_else(|| VfsError::CorruptStore {
                    message: format!(
                        "review comment {} references unknown change request {}",
                        comment.id, comment.change_request_id
                    ),
                })?;
            comment.validate(change).map_err(corrupt_record)?;
            state.comments.insert(comment.id, comment);
        }

        Ok(state)
    }

    fn encode(state: &ReviewState) -> Result<Vec<u8>, VfsError> {
        crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: state.list_protected_ref_rules(),
            protected_paths: state.list_protected_path_rules(),
            change_requests: state.list_change_requests(),
            approvals: state.approvals.values().cloned().collect(),
            assignments: state.assignments.values().cloned().collect(),
            comments: state.comments.values().cloned().collect(),
        })
        .map_err(|e| VfsError::CorruptStore {
            message: format!("review store encode failed: {e}"),
        })
    }

    fn persist_locked(&self, state: &ReviewState) -> Result<(), VfsError> {
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
impl ReviewStore for LocalReviewStore {
    async fn create_protected_ref_rule(
        &self,
        ref_name: &str,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedRefRule, VfsError> {
        let rule = ProtectedRefRule::new(ref_name, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        next.protected_refs.insert(rule.id, rule.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(rule)
    }

    async fn list_protected_ref_rules(&self) -> Result<Vec<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_ref_rules())
    }

    async fn get_protected_ref_rule(&self, id: Uuid) -> Result<Option<ProtectedRefRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_refs.get(&id).cloned())
    }

    async fn create_protected_path_rule(
        &self,
        path_prefix: &str,
        target_ref: Option<&str>,
        required_approvals: u32,
        created_by: Uid,
    ) -> Result<ProtectedPathRule, VfsError> {
        let rule = ProtectedPathRule::new(path_prefix, target_ref, required_approvals, created_by)?;
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        next.protected_paths.insert(rule.id, rule.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(rule)
    }

    async fn list_protected_path_rules(&self) -> Result<Vec<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_protected_path_rules())
    }

    async fn get_protected_path_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<ProtectedPathRule>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.protected_paths.get(&id).cloned())
    }

    async fn create_change_request(
        &self,
        input: NewChangeRequest,
    ) -> Result<ChangeRequest, VfsError> {
        let change = ChangeRequest::new(input)?;
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        next.change_requests.insert(change.id, change.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(change)
    }

    async fn list_change_requests(&self) -> Result<Vec<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_change_requests())
    }

    async fn get_change_request(&self, id: Uuid) -> Result<Option<ChangeRequest>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.change_requests.get(&id).cloned())
    }

    async fn transition_change_request(
        &self,
        id: Uuid,
        status: ChangeRequestStatus,
    ) -> Result<Option<ChangeRequest>, VfsError> {
        let mut guard = self.inner.write().await;
        let Some(current) = guard.change_requests.get(&id) else {
            return Ok(None);
        };
        let next_change = current.transition(status)?;
        let mut next = guard.clone();
        next.change_requests.insert(id, next_change.clone());
        self.persist_locked(&next)?;
        *guard = next;
        Ok(Some(next_change))
    }

    async fn create_approval(
        &self,
        input: NewApprovalRecord,
    ) -> Result<ApprovalRecordMutation, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let mutation = next.create_approval(input)?;
        if mutation.created {
            self.persist_locked(&next)?;
            *guard = next;
        }
        Ok(mutation)
    }

    async fn list_approvals(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ApprovalRecord>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_approvals(change_request_id))
    }

    async fn assign_reviewer(
        &self,
        input: NewReviewAssignment,
    ) -> Result<ReviewAssignmentMutation, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let mutation = next.assign_reviewer(input)?;
        if mutation.created || mutation.updated {
            self.persist_locked(&next)?;
            *guard = next;
        }
        Ok(mutation)
    }

    async fn list_reviewer_assignments(
        &self,
        change_request_id: Uuid,
    ) -> Result<Vec<ReviewAssignment>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_reviewer_assignments(change_request_id))
    }

    async fn create_comment(
        &self,
        input: NewReviewComment,
    ) -> Result<ReviewCommentMutation, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let mutation = next.create_comment(input)?;
        if mutation.created {
            self.persist_locked(&next)?;
            *guard = next;
        }
        Ok(mutation)
    }

    async fn list_comments(&self, change_request_id: Uuid) -> Result<Vec<ReviewComment>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.list_comments(change_request_id))
    }

    async fn dismiss_approval(
        &self,
        input: DismissApprovalInput,
    ) -> Result<ApprovalDismissalMutation, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let mutation = next.dismiss_approval(input)?;
        if mutation.dismissed {
            self.persist_locked(&next)?;
            *guard = next;
        }
        Ok(mutation)
    }

    async fn approval_decision(
        &self,
        change_request_id: Uuid,
        changed_paths: &[String],
    ) -> Result<Option<ApprovalPolicyDecision>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.approval_decision(change_request_id, changed_paths))
    }
}

fn validate_required_approvals(required_approvals: u32) -> Result<(), VfsError> {
    if required_approvals == 0 {
        return Err(VfsError::InvalidArgs {
            message: "required approvals must be greater than zero".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn normalize_path_prefix(path: &str) -> Result<String, VfsError> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }
    if path == "/" {
        return Ok(path.to_string());
    }
    if path.ends_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }

    for component in path.trim_start_matches('/').split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(VfsError::InvalidPath {
                path: path.to_string(),
            });
        }
    }
    Ok(path.to_string())
}

fn validate_commit_hex(value: &str) -> Result<(), VfsError> {
    ObjectId::from_hex(value).map(|_| ())
}

fn validate_new_approval(
    input: &NewApprovalRecord,
    change: &ChangeRequest,
) -> Result<(), VfsError> {
    validate_commit_hex(&input.head_commit)?;
    if input.head_commit != change.head_commit {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "approval head {} does not match change request {} head {}",
                input.head_commit, change.id, change.head_commit
            ),
        });
    }
    if input.approved_by == change.created_by {
        return Err(VfsError::InvalidArgs {
            message: "change request author cannot approve their own change".to_string(),
        });
    }
    Ok(())
}

fn validate_new_assignment(
    input: &NewReviewAssignment,
    change: &ChangeRequest,
) -> Result<(), VfsError> {
    if input.change_request_id != change.id {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "review assignment belongs to unexpected change request {}",
                input.change_request_id
            ),
        });
    }
    if input.reviewer == change.created_by {
        return Err(VfsError::InvalidArgs {
            message: "change request author cannot be assigned as reviewer".to_string(),
        });
    }
    Ok(())
}

fn normalize_approval_comment(comment: Option<String>) -> Result<Option<String>, VfsError> {
    let Some(comment) = comment else {
        return Ok(None);
    };
    let trimmed = comment.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut end = trimmed.len().min(APPROVAL_COMMENT_MAX_BYTES);
    while !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    Ok(Some(trimmed[..end].to_string()))
}

fn validate_approval_comment(comment: &str) -> Result<(), VfsError> {
    if comment.trim() != comment {
        return Err(VfsError::InvalidArgs {
            message: "approval comment must be trimmed".to_string(),
        });
    }
    if comment.len() > APPROVAL_COMMENT_MAX_BYTES {
        return Err(VfsError::InvalidArgs {
            message: format!("approval comment must be at most {APPROVAL_COMMENT_MAX_BYTES} bytes"),
        });
    }
    Ok(())
}

fn validate_comment_change(
    change_request_id: Uuid,
    change: &ChangeRequest,
) -> Result<(), VfsError> {
    if change_request_id == change.id {
        return Ok(());
    }
    Err(VfsError::InvalidArgs {
        message: format!(
            "comment belongs to unexpected change request {}",
            change_request_id
        ),
    })
}

fn normalize_review_comment_body(body: String) -> Result<String, VfsError> {
    let trimmed = body.trim();
    validate_review_comment_body(trimmed)?;
    Ok(trimmed.to_string())
}

fn validate_review_comment_body(body: &str) -> Result<(), VfsError> {
    if body.trim() != body {
        return Err(VfsError::InvalidArgs {
            message: "review comment body must be trimmed".to_string(),
        });
    }
    if body.is_empty() {
        return Err(VfsError::InvalidArgs {
            message: "review comment body must not be empty".to_string(),
        });
    }
    if body.len() > REVIEW_COMMENT_MAX_BYTES {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "review comment body must be at most {REVIEW_COMMENT_MAX_BYTES} bytes"
            ),
        });
    }
    Ok(())
}

fn normalize_optional_path(path: Option<String>) -> Result<Option<String>, VfsError> {
    path.map(|path| normalize_path_prefix(path.trim()))
        .transpose()
}

fn normalize_dismissal_reason(reason: Option<String>) -> Result<Option<String>, VfsError> {
    let Some(reason) = reason else {
        return Ok(None);
    };
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_dismissal_reason(trimmed)?;
    Ok(Some(trimmed.to_string()))
}

fn validate_dismissal_reason(reason: &str) -> Result<(), VfsError> {
    if reason.trim() != reason {
        return Err(VfsError::InvalidArgs {
            message: "approval dismissal reason must be trimmed".to_string(),
        });
    }
    if reason.len() > APPROVAL_COMMENT_MAX_BYTES {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "approval dismissal reason must be at most {APPROVAL_COMMENT_MAX_BYTES} bytes"
            ),
        });
    }
    Ok(())
}

fn reject_duplicate_id(ids: &mut HashSet<Uuid>, id: Uuid) -> Result<(), VfsError> {
    if ids.insert(id) {
        return Ok(());
    }
    Err(VfsError::CorruptStore {
        message: format!("duplicate review record id {id}"),
    })
}

fn corrupt_record(error: VfsError) -> VfsError {
    VfsError::CorruptStore {
        message: format!("invalid review record: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn temp_review_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stratum_review_{}_{}_{}.bin",
            name,
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    fn test_change_request(created_by: Uid) -> NewChangeRequest {
        NewChangeRequest {
            title: "Legal update".to_string(),
            description: Some("Needs review".to_string()),
            source_ref: "review/legal-update".to_string(),
            target_ref: "main".to_string(),
            base_commit: "a".repeat(64),
            head_commit: "b".repeat(64),
            created_by,
        }
    }

    fn approval_record(change: &ChangeRequest, approved_by: Uid) -> ApprovalRecord {
        ApprovalRecord {
            id: Uuid::new_v4(),
            change_request_id: change.id,
            head_commit: change.head_commit.clone(),
            approved_by,
            comment: None,
            active: true,
            dismissed_by: None,
            dismissal_reason: None,
            version: 1,
        }
    }

    #[derive(Serialize, Deserialize)]
    struct PersistedReviewStoreV2ForTest {
        version: u32,
        protected_refs: Vec<ProtectedRefRule>,
        protected_paths: Vec<ProtectedPathRule>,
        change_requests: Vec<ChangeRequest>,
        approvals: Vec<ApprovalRecordV2ForTest>,
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct ApprovalRecordV2ForTest {
        id: Uuid,
        change_request_id: Uuid,
        head_commit: String,
        approved_by: Uid,
        comment: Option<String>,
        active: bool,
        version: u64,
    }

    fn approval_record_v2(change: &ChangeRequest, approved_by: Uid) -> ApprovalRecordV2ForTest {
        ApprovalRecordV2ForTest {
            id: Uuid::new_v4(),
            change_request_id: change.id,
            head_commit: change.head_commit.clone(),
            approved_by,
            comment: None,
            active: true,
            version: 1,
        }
    }

    fn persisted_store(
        change_requests: Vec<ChangeRequest>,
        approvals: Vec<ApprovalRecord>,
        comments: Vec<ReviewComment>,
    ) -> PersistedReviewStore {
        PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests,
            approvals,
            assignments: Vec::new(),
            comments,
        }
    }

    fn review_assignment(
        change: &ChangeRequest,
        reviewer: Uid,
        required: bool,
    ) -> ReviewAssignment {
        ReviewAssignment {
            id: Uuid::new_v4(),
            change_request_id: change.id,
            reviewer,
            assigned_by: 0,
            required,
            active: true,
            version: 1,
        }
    }

    fn persisted_store_with_assignments(
        change_requests: Vec<ChangeRequest>,
        approvals: Vec<ApprovalRecord>,
        comments: Vec<ReviewComment>,
        assignments: Vec<ReviewAssignment>,
    ) -> PersistedReviewStore {
        PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests,
            approvals,
            comments,
            assignments,
        }
    }

    #[derive(Serialize, Deserialize)]
    struct PersistedReviewStoreV3ForTest {
        version: u32,
        protected_refs: Vec<ProtectedRefRule>,
        protected_paths: Vec<ProtectedPathRule>,
        change_requests: Vec<ChangeRequest>,
        approvals: Vec<ApprovalRecord>,
        comments: Vec<ReviewComment>,
    }

    #[tokio::test]
    async fn review_assignment_in_memory_store_creates_and_lists_assignments() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let mutation = store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 0,
                required: true,
            })
            .await
            .unwrap();

        assert!(mutation.created);
        assert!(!mutation.updated);
        assert_eq!(mutation.assignment.change_request_id, change.id);
        assert_eq!(mutation.assignment.reviewer, 11);
        assert_eq!(mutation.assignment.assigned_by, 0);
        assert!(mutation.assignment.required);
        assert!(mutation.assignment.active);
        assert_eq!(mutation.assignment.version, 1);
        assert_eq!(
            store.list_reviewer_assignments(change.id).await.unwrap(),
            vec![mutation.assignment]
        );
    }

    #[tokio::test]
    async fn review_assignment_duplicate_same_required_returns_existing_record() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let input = NewReviewAssignment {
            change_request_id: change.id,
            reviewer: 11,
            assigned_by: 0,
            required: true,
        };

        let created = store.assign_reviewer(input.clone()).await.unwrap();
        let duplicate = store.assign_reviewer(input).await.unwrap();

        assert!(created.created);
        assert!(!created.updated);
        assert!(!duplicate.created);
        assert!(!duplicate.updated);
        assert_eq!(duplicate.assignment, created.assignment);
        assert_eq!(
            store
                .list_reviewer_assignments(change.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn review_assignment_duplicate_different_required_updates_same_record() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let created = store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 0,
                required: true,
            })
            .await
            .unwrap()
            .assignment;

        let updated = store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 12,
                required: false,
            })
            .await
            .unwrap();

        assert!(!updated.created);
        assert!(updated.updated);
        assert_eq!(updated.assignment.id, created.id);
        assert_eq!(updated.assignment.assigned_by, 12);
        assert!(!updated.assignment.required);
        assert_eq!(updated.assignment.version, created.version + 1);
        assert_eq!(
            store.list_reviewer_assignments(change.id).await.unwrap(),
            vec![updated.assignment]
        );
    }

    #[tokio::test]
    async fn review_assignment_unknown_change_request_and_self_assignment_fail() {
        let store = InMemoryReviewStore::new();
        let unknown = store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: Uuid::new_v4(),
                reviewer: 11,
                assigned_by: 0,
                required: true,
            })
            .await
            .expect_err("unknown change request should fail");
        assert!(matches!(unknown, VfsError::InvalidArgs { .. }));

        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let self_assignment = store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 10,
                assigned_by: 0,
                required: true,
            })
            .await
            .expect_err("self assignment should fail");
        assert!(matches!(self_assignment, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn review_assignment_local_store_reloads_assignments() {
        let path = temp_review_path("review_assignment_reload");
        let store = LocalReviewStore::open(&path).unwrap();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let assignment = store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 0,
                required: true,
            })
            .await
            .unwrap()
            .assignment;
        drop(store);

        let reloaded = LocalReviewStore::open(&path).unwrap();
        assert_eq!(
            reloaded.list_reviewer_assignments(change.id).await.unwrap(),
            vec![assignment]
        );

        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn review_assignment_local_store_migrates_v1_v2_and_v3_to_v4_with_empty_assignments() {
        let v1_path = temp_review_path("review_assignment_v1_migration");
        let v1_change = ChangeRequest::new(test_change_request(10)).unwrap();
        let v1_bytes = crate::codec::serialize(&PersistedReviewStoreV1 {
            version: 1,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![v1_change.clone()],
        })
        .unwrap();
        fs::write(&v1_path, v1_bytes).unwrap();
        let v1_store = LocalReviewStore::open(&v1_path).unwrap();
        assert!(
            v1_store
                .list_reviewer_assignments(v1_change.id)
                .await
                .unwrap()
                .is_empty()
        );
        drop(v1_store);
        fs::remove_file(v1_path).unwrap();

        let v2_path = temp_review_path("review_assignment_v2_migration");
        let v2_change = ChangeRequest::new(test_change_request(20)).unwrap();
        let v2_bytes = crate::codec::serialize(&PersistedReviewStoreV2ForTest {
            version: 2,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![v2_change.clone()],
            approvals: vec![approval_record_v2(&v2_change, 21)],
        })
        .unwrap();
        fs::write(&v2_path, v2_bytes).unwrap();
        let v2_store = LocalReviewStore::open(&v2_path).unwrap();
        assert!(
            v2_store
                .list_reviewer_assignments(v2_change.id)
                .await
                .unwrap()
                .is_empty()
        );
        drop(v2_store);
        fs::remove_file(v2_path).unwrap();

        let v3_path = temp_review_path("review_assignment_v3_migration");
        let v3_change = ChangeRequest::new(test_change_request(30)).unwrap();
        let v3_bytes = crate::codec::serialize(&PersistedReviewStoreV3ForTest {
            version: 3,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![v3_change.clone()],
            approvals: vec![approval_record(&v3_change, 31)],
            comments: Vec::new(),
        })
        .unwrap();
        fs::write(&v3_path, v3_bytes).unwrap();
        let v3_store = LocalReviewStore::open(&v3_path).unwrap();
        assert!(
            v3_store
                .list_reviewer_assignments(v3_change.id)
                .await
                .unwrap()
                .is_empty()
        );
        drop(v3_store);
        fs::remove_file(v3_path).unwrap();
    }

    #[tokio::test]
    async fn review_assignment_approval_decision_reports_required_approved_and_missing_reviewers() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 0,
                required: true,
            })
            .await
            .unwrap();
        store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 12,
                assigned_by: 0,
                required: true,
            })
            .await
            .unwrap();
        store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap();

        let decision = store
            .approval_decision(change.id, &[])
            .await
            .unwrap()
            .unwrap();

        assert_eq!(decision.required_reviewers, vec![11, 12]);
        assert_eq!(decision.approved_required_reviewers, vec![11]);
        assert_eq!(decision.missing_required_reviewers, vec![12]);
        assert!(!decision.approved);
    }

    #[tokio::test]
    async fn review_assignment_required_reviewer_blocks_until_that_reviewer_approves_current_head()
    {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        store
            .create_protected_ref_rule("main", 1, 20)
            .await
            .unwrap();
        store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 0,
                required: true,
            })
            .await
            .unwrap();
        store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 12,
                comment: None,
            })
            .await
            .unwrap();

        let blocked = store
            .approval_decision(change.id, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(blocked.approval_count, 1);
        assert_eq!(blocked.missing_required_reviewers, vec![11]);
        assert!(!blocked.approved);

        store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap();

        let approved = store
            .approval_decision(change.id, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(approved.approved_required_reviewers, vec![11]);
        assert!(approved.missing_required_reviewers.is_empty());
        assert!(approved.approved);
    }

    #[tokio::test]
    async fn review_assignment_optional_reviewers_do_not_block_approval_state() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        store
            .create_protected_ref_rule("main", 1, 20)
            .await
            .unwrap();
        store
            .assign_reviewer(NewReviewAssignment {
                change_request_id: change.id,
                reviewer: 11,
                assigned_by: 0,
                required: false,
            })
            .await
            .unwrap();
        store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 12,
                comment: None,
            })
            .await
            .unwrap();

        let decision = store
            .approval_decision(change.id, &[])
            .await
            .unwrap()
            .unwrap();

        assert_eq!(decision.required_reviewers, Vec::<Uid>::new());
        assert!(decision.approved_required_reviewers.is_empty());
        assert!(decision.missing_required_reviewers.is_empty());
        assert!(decision.approved);
    }

    #[test]
    fn review_assignment_corrupt_v4_store_rejects_invalid_assignments() {
        let unknown_path = temp_review_path("review_assignment_unknown_cr");
        let unknown = ReviewAssignment {
            id: Uuid::new_v4(),
            change_request_id: Uuid::new_v4(),
            reviewer: 11,
            assigned_by: 0,
            required: true,
            active: true,
            version: 1,
        };
        let bytes = crate::codec::serialize(&persisted_store_with_assignments(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![unknown],
        ))
        .unwrap();
        fs::write(&unknown_path, bytes).unwrap();
        assert!(matches!(
            LocalReviewStore::open(&unknown_path),
            Err(VfsError::CorruptStore { .. })
        ));
        fs::remove_file(unknown_path).unwrap();

        let change = ChangeRequest::new(test_change_request(10)).unwrap();
        let invalid_assignments = vec![
            ReviewAssignment {
                version: 0,
                ..review_assignment(&change, 11, true)
            },
            ReviewAssignment {
                active: false,
                ..review_assignment(&change, 11, true)
            },
            ReviewAssignment {
                reviewer: change.created_by,
                ..review_assignment(&change, 11, true)
            },
        ];
        for (index, assignment) in invalid_assignments.into_iter().enumerate() {
            let path = temp_review_path(&format!("review_assignment_invalid_{index}"));
            let bytes = crate::codec::serialize(&persisted_store_with_assignments(
                vec![change.clone()],
                Vec::new(),
                Vec::new(),
                vec![assignment],
            ))
            .unwrap();
            fs::write(&path, bytes).unwrap();
            assert!(matches!(
                LocalReviewStore::open(&path),
                Err(VfsError::CorruptStore { .. })
            ));
            fs::remove_file(path).unwrap();
        }

        let duplicate_path = temp_review_path("review_assignment_duplicate_active");
        let duplicate_a = review_assignment(&change, 11, true);
        let duplicate_b = review_assignment(&change, 11, false);
        let bytes = crate::codec::serialize(&persisted_store_with_assignments(
            vec![change],
            Vec::new(),
            Vec::new(),
            vec![duplicate_a, duplicate_b],
        ))
        .unwrap();
        fs::write(&duplicate_path, bytes).unwrap();
        assert!(matches!(
            LocalReviewStore::open(&duplicate_path),
            Err(VfsError::CorruptStore { .. })
        ));
        fs::remove_file(duplicate_path).unwrap();
    }

    #[tokio::test]
    async fn review_feedback_in_memory_store_creates_and_lists_comments() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let mutation = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: "  Please revisit this paragraph.  ".to_string(),
                path: None,
                kind: ReviewCommentKind::ChangesRequested,
            })
            .await
            .unwrap();

        assert!(mutation.created);
        assert_eq!(mutation.comment.change_request_id, change.id);
        assert_eq!(mutation.comment.author, 11);
        assert_eq!(mutation.comment.body, "Please revisit this paragraph.");
        assert_eq!(mutation.comment.path, None);
        assert_eq!(mutation.comment.kind, ReviewCommentKind::ChangesRequested);
        assert!(mutation.comment.active);
        assert_eq!(mutation.comment.version, 1);
        assert_eq!(
            store.list_comments(change.id).await.unwrap(),
            vec![mutation.comment]
        );
    }

    #[tokio::test]
    async fn review_feedback_comment_body_is_trimmed_and_empty_bodies_are_rejected() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let comment = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: "\nLooks good except the summary.\t".to_string(),
                path: None,
                kind: ReviewCommentKind::General,
            })
            .await
            .unwrap()
            .comment;
        assert_eq!(comment.body, "Looks good except the summary.");

        let err = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: " \n\t ".to_string(),
                path: None,
                kind: ReviewCommentKind::General,
            })
            .await
            .expect_err("empty comment should fail");
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn review_feedback_comment_path_is_optional_and_normalized() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let without_path = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: "General note".to_string(),
                path: None,
                kind: ReviewCommentKind::General,
            })
            .await
            .unwrap()
            .comment;
        assert_eq!(without_path.path, None);

        let with_path = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: "File note".to_string(),
                path: Some("/legal/draft.txt".to_string()),
                kind: ReviewCommentKind::General,
            })
            .await
            .unwrap()
            .comment;
        assert_eq!(with_path.path.as_deref(), Some("/legal/draft.txt"));

        let err = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: "Bad path".to_string(),
                path: Some("/legal/../draft.txt".to_string()),
                kind: ReviewCommentKind::General,
            })
            .await
            .expect_err("invalid path should fail");
        assert!(matches!(err, VfsError::InvalidPath { .. }));
    }

    #[tokio::test]
    async fn review_feedback_comment_for_unknown_change_request_fails() {
        let store = InMemoryReviewStore::new();

        let err = store
            .create_comment(NewReviewComment {
                change_request_id: Uuid::new_v4(),
                author: 11,
                body: "Unknown CR".to_string(),
                path: None,
                kind: ReviewCommentKind::General,
            })
            .await
            .expect_err("unknown change request should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn review_feedback_local_store_reloads_comments() {
        let path = temp_review_path("review_feedback_comments_reload");
        let store = LocalReviewStore::open(&path).unwrap();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let comment = store
            .create_comment(NewReviewComment {
                change_request_id: change.id,
                author: 11,
                body: "Persist me".to_string(),
                path: Some("/legal/draft.txt".to_string()),
                kind: ReviewCommentKind::General,
            })
            .await
            .unwrap()
            .comment;
        drop(store);

        let reloaded = LocalReviewStore::open(&path).unwrap();
        assert_eq!(
            reloaded.list_comments(change.id).await.unwrap(),
            vec![comment]
        );

        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn review_feedback_local_store_migrates_v1_and_v2_to_v3() {
        let v1_path = temp_review_path("review_feedback_v1_migration");
        let v1_change = ChangeRequest::new(test_change_request(10)).unwrap();
        let v1_bytes = crate::codec::serialize(&PersistedReviewStoreV1 {
            version: 1,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![v1_change.clone()],
        })
        .unwrap();
        fs::write(&v1_path, v1_bytes).unwrap();

        let v1_store = LocalReviewStore::open(&v1_path).unwrap();
        assert!(
            v1_store
                .list_approvals(v1_change.id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            v1_store
                .list_comments(v1_change.id)
                .await
                .unwrap()
                .is_empty()
        );
        drop(v1_store);
        fs::remove_file(v1_path).unwrap();

        let v2_path = temp_review_path("review_feedback_v2_migration");
        let v2_change = ChangeRequest::new(test_change_request(20)).unwrap();
        let v2_approval = approval_record_v2(&v2_change, 21);
        let v2_bytes = crate::codec::serialize(&PersistedReviewStoreV2ForTest {
            version: 2,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![v2_change.clone()],
            approvals: vec![v2_approval.clone()],
        })
        .unwrap();
        fs::write(&v2_path, v2_bytes).unwrap();

        let v2_store = LocalReviewStore::open(&v2_path).unwrap();
        let approvals = v2_store.list_approvals(v2_change.id).await.unwrap();
        assert_eq!(approvals.len(), 1);
        assert_eq!(approvals[0].id, v2_approval.id);
        assert_eq!(approvals[0].dismissed_by, None);
        assert_eq!(approvals[0].dismissal_reason, None);
        assert!(
            v2_store
                .list_comments(v2_change.id)
                .await
                .unwrap()
                .is_empty()
        );
        drop(v2_store);
        fs::remove_file(v2_path).unwrap();
    }

    #[tokio::test]
    async fn review_feedback_dismissing_active_approval_records_metadata_and_updates_counts() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        store
            .create_protected_ref_rule("main", 1, 20)
            .await
            .unwrap();
        let approval = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap()
            .record;
        assert!(
            store
                .approval_decision(change.id, &[])
                .await
                .unwrap()
                .unwrap()
                .approved
        );

        let mutation = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: approval.id,
                dismissed_by: 12,
                reason: Some("  stale approval  ".to_string()),
            })
            .await
            .unwrap();

        assert!(mutation.dismissed);
        assert!(!mutation.record.active);
        assert_eq!(mutation.record.dismissed_by, Some(12));
        assert_eq!(
            mutation.record.dismissal_reason.as_deref(),
            Some("stale approval")
        );
        assert_eq!(mutation.record.version, approval.version + 1);
        let decision = store
            .approval_decision(change.id, &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.approval_count, 0);
        assert!(!decision.approved);
    }

    #[tokio::test]
    async fn review_feedback_duplicate_dismissal_returns_inactive_record_without_mutation() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let approval = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap()
            .record;

        let first = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: approval.id,
                dismissed_by: 12,
                reason: Some("first".to_string()),
            })
            .await
            .unwrap();
        let duplicate = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: approval.id,
                dismissed_by: 13,
                reason: Some("second".to_string()),
            })
            .await
            .unwrap();

        assert!(first.dismissed);
        assert!(!duplicate.dismissed);
        assert_eq!(duplicate.record, first.record);
    }

    #[tokio::test]
    async fn review_feedback_duplicate_dismissal_still_validates_reason_contract() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let approval = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap()
            .record;
        let dismissed = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: approval.id,
                dismissed_by: 12,
                reason: Some("first".to_string()),
            })
            .await
            .unwrap();

        let err = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: approval.id,
                dismissed_by: 13,
                reason: Some("x".repeat(APPROVAL_COMMENT_MAX_BYTES + 1)),
            })
            .await
            .expect_err("duplicate dismissal should still validate reason");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert_eq!(
            store.list_approvals(change.id).await.unwrap(),
            vec![dismissed.record]
        );
    }

    #[tokio::test]
    async fn review_feedback_dismissal_unknown_or_wrong_change_request_fails() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let other_change = store
            .create_change_request(NewChangeRequest {
                title: "Other".to_string(),
                description: None,
                source_ref: "review/other".to_string(),
                target_ref: "main".to_string(),
                base_commit: "a".repeat(64),
                head_commit: "b".repeat(64),
                created_by: 20,
            })
            .await
            .unwrap();
        let approval = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap()
            .record;

        let unknown = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: Uuid::new_v4(),
                dismissed_by: 12,
                reason: None,
            })
            .await
            .expect_err("unknown approval should fail");
        assert!(matches!(unknown, VfsError::InvalidArgs { .. }));

        let wrong_change = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: other_change.id,
                approval_id: approval.id,
                dismissed_by: 12,
                reason: None,
            })
            .await
            .expect_err("wrong change request should fail");
        assert!(matches!(wrong_change, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn review_feedback_invalid_dismissal_reason_does_not_mutate_approval() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let approval = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap()
            .record;

        let err = store
            .dismiss_approval(DismissApprovalInput {
                change_request_id: change.id,
                approval_id: approval.id,
                dismissed_by: 12,
                reason: Some("x".repeat(APPROVAL_COMMENT_MAX_BYTES + 1)),
            })
            .await
            .expect_err("oversized dismissal reason should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert_eq!(
            store.list_approvals(change.id).await.unwrap(),
            vec![approval]
        );
    }

    #[test]
    fn review_feedback_corrupt_v3_store_rejects_invalid_comments_approvals_and_duplicates() {
        let unknown_comment_path = temp_review_path("review_feedback_unknown_comment_cr");
        let unknown_comment = ReviewComment {
            id: Uuid::new_v4(),
            change_request_id: Uuid::new_v4(),
            author: 11,
            body: "Unknown".to_string(),
            path: None,
            kind: ReviewCommentKind::General,
            active: true,
            version: 1,
        };
        let bytes = crate::codec::serialize(&persisted_store(
            Vec::new(),
            Vec::new(),
            vec![unknown_comment],
        ))
        .unwrap();
        fs::write(&unknown_comment_path, bytes).unwrap();
        assert!(matches!(
            LocalReviewStore::open(&unknown_comment_path),
            Err(VfsError::CorruptStore { .. })
        ));
        fs::remove_file(unknown_comment_path).unwrap();

        let change = ChangeRequest::new(test_change_request(10)).unwrap();
        let invalid_comments = vec![
            ReviewComment {
                id: Uuid::new_v4(),
                change_request_id: change.id,
                author: 11,
                body: String::new(),
                path: None,
                kind: ReviewCommentKind::General,
                active: true,
                version: 1,
            },
            ReviewComment {
                id: Uuid::new_v4(),
                change_request_id: change.id,
                author: 11,
                body: "Bad path".to_string(),
                path: Some("/legal/../draft.txt".to_string()),
                kind: ReviewCommentKind::General,
                active: true,
                version: 1,
            },
        ];
        for (index, comment) in invalid_comments.into_iter().enumerate() {
            let path = temp_review_path(&format!("review_feedback_invalid_comment_{index}"));
            let bytes = crate::codec::serialize(&persisted_store(
                vec![change.clone()],
                Vec::new(),
                vec![comment],
            ))
            .unwrap();
            fs::write(&path, bytes).unwrap();
            assert!(matches!(
                LocalReviewStore::open(&path),
                Err(VfsError::CorruptStore { .. })
            ));
            fs::remove_file(path).unwrap();
        }

        let mut active_with_dismissal = approval_record(&change, 11);
        active_with_dismissal.dismissed_by = Some(12);
        let mut inactive_without_dismissed_by = approval_record(&change, 12);
        inactive_without_dismissed_by.active = false;
        let approval_cases = vec![active_with_dismissal, inactive_without_dismissed_by];
        for (index, approval) in approval_cases.into_iter().enumerate() {
            let path = temp_review_path(&format!("review_feedback_invalid_approval_{index}"));
            let bytes = crate::codec::serialize(&persisted_store(
                vec![change.clone()],
                vec![approval],
                Vec::new(),
            ))
            .unwrap();
            fs::write(&path, bytes).unwrap();
            assert!(matches!(
                LocalReviewStore::open(&path),
                Err(VfsError::CorruptStore { .. })
            ));
            fs::remove_file(path).unwrap();
        }

        let duplicate_path = temp_review_path("review_feedback_duplicate_active_approval");
        let first = approval_record(&change, 11);
        let second = approval_record(&change, 11);
        let bytes = crate::codec::serialize(&persisted_store(
            vec![change],
            vec![first, second],
            Vec::new(),
        ))
        .unwrap();
        fs::write(&duplicate_path, bytes).unwrap();
        assert!(matches!(
            LocalReviewStore::open(&duplicate_path),
            Err(VfsError::CorruptStore { .. })
        ));
        fs::remove_file(duplicate_path).unwrap();
    }

    #[tokio::test]
    async fn approval_in_memory_store_creates_and_lists_records() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let mutation = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: Some("  Looks good.  ".to_string()),
            })
            .await
            .unwrap();

        assert!(mutation.created);
        assert_eq!(mutation.record.comment.as_deref(), Some("Looks good."));
        assert_eq!(
            store.list_approvals(change.id).await.unwrap(),
            vec![mutation.record]
        );
    }

    #[tokio::test]
    async fn approval_duplicate_for_same_change_head_and_approver_returns_existing_record() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let input = NewApprovalRecord {
            change_request_id: change.id,
            head_commit: change.head_commit.clone(),
            approved_by: 11,
            comment: Some("first".to_string()),
        };

        let created = store.create_approval(input.clone()).await.unwrap();
        let duplicate = store.create_approval(input).await.unwrap();

        assert!(created.created);
        assert!(!duplicate.created);
        assert_eq!(duplicate.record, created.record);
        assert_eq!(store.list_approvals(change.id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn approval_unknown_change_request_fails() {
        let store = InMemoryReviewStore::new();

        let err = store
            .create_approval(NewApprovalRecord {
                change_request_id: Uuid::new_v4(),
                head_commit: "b".repeat(64),
                approved_by: 11,
                comment: None,
            })
            .await
            .expect_err("unknown change request should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn approval_stale_head_commit_fails() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let err = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: "c".repeat(64),
                approved_by: 11,
                comment: None,
            })
            .await
            .expect_err("stale approval head should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn approval_self_approval_fails() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let err = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 10,
                comment: None,
            })
            .await
            .expect_err("self approval should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn approval_local_store_reloads_records() {
        let path = temp_review_path("approval_reload");
        let store = LocalReviewStore::open(&path).unwrap();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let approval = store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: Some("approved".to_string()),
            })
            .await
            .unwrap()
            .record;
        drop(store);

        let reloaded = LocalReviewStore::open(&path).unwrap();
        assert_eq!(
            reloaded.list_approvals(change.id).await.unwrap(),
            vec![approval]
        );

        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn approval_local_store_migrates_v1_with_empty_approvals() {
        let path = temp_review_path("approval_v1");
        let change = ChangeRequest::new(test_change_request(10)).unwrap();
        let bytes = crate::codec::serialize(&PersistedReviewStoreV1 {
            version: 1,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![change.clone()],
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let store = LocalReviewStore::open(&path).unwrap();
        assert!(store.list_approvals(change.id).await.unwrap().is_empty());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn approval_corrupt_v2_unknown_change_request_is_rejected() {
        let path = temp_review_path("approval_unknown_cr");
        let approval = ApprovalRecord {
            id: Uuid::new_v4(),
            change_request_id: Uuid::new_v4(),
            head_commit: "b".repeat(64),
            approved_by: 11,
            comment: None,
            active: true,
            dismissed_by: None,
            dismissal_reason: None,
            version: 1,
        };
        let bytes = crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: Vec::new(),
            approvals: vec![approval],
            assignments: Vec::new(),
            comments: Vec::new(),
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let err = LocalReviewStore::open(&path).expect_err("unknown approval CR should fail");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn approval_corrupt_v2_invalid_commit_hex_is_rejected() {
        let path = temp_review_path("approval_invalid_commit");
        let change = ChangeRequest::new(test_change_request(10)).unwrap();
        let mut approval = approval_record(&change, 11);
        approval.head_commit = "not-hex".to_string();
        let bytes = crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![change],
            approvals: vec![approval],
            assignments: Vec::new(),
            comments: Vec::new(),
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let err = LocalReviewStore::open(&path).expect_err("invalid approval head should fail");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn approval_corrupt_v2_duplicate_active_identity_is_rejected() {
        let path = temp_review_path("approval_duplicate_identity");
        let change = ChangeRequest::new(test_change_request(10)).unwrap();
        let first = approval_record(&change, 11);
        let second = approval_record(&change, 11);
        let bytes = crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: Vec::new(),
            protected_paths: Vec::new(),
            change_requests: vec![change],
            approvals: vec![first, second],
            assignments: Vec::new(),
            comments: Vec::new(),
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let err = LocalReviewStore::open(&path).expect_err("duplicate approval should fail");
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn approval_policy_decision_uses_max_required_approvals_from_refs_and_paths() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();
        let ref_rule = store
            .create_protected_ref_rule("main", 2, 20)
            .await
            .unwrap();
        let path_rule = store
            .create_protected_path_rule("/legal", Some("main"), 3, 20)
            .await
            .unwrap();
        store
            .create_protected_path_rule("/docs", Some("review/release"), 4, 20)
            .await
            .unwrap();
        store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 11,
                comment: None,
            })
            .await
            .unwrap();
        store
            .create_approval(NewApprovalRecord {
                change_request_id: change.id,
                head_commit: change.head_commit.clone(),
                approved_by: 12,
                comment: None,
            })
            .await
            .unwrap();
        let changed_paths = vec!["/legal/draft.txt".to_string()];

        let decision = store
            .approval_decision(change.id, &changed_paths)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(decision.required_approvals, 3);
        assert_eq!(decision.approval_count, 2);
        assert!(!decision.approved);
        assert_eq!(decision.approved_by, vec![11, 12]);
        assert_eq!(decision.matched_ref_rules, vec![ref_rule.id]);
        assert_eq!(decision.matched_path_rules, vec![path_rule.id]);
    }

    #[tokio::test]
    async fn approval_policy_decision_approves_unprotected_change_requests() {
        let store = InMemoryReviewStore::new();
        let change = store
            .create_change_request(test_change_request(10))
            .await
            .unwrap();

        let decision = store
            .approval_decision(change.id, &[])
            .await
            .unwrap()
            .unwrap();

        assert_eq!(decision.required_approvals, 0);
        assert!(decision.approved);
    }

    #[test]
    fn protected_path_prefix_matching_respects_path_boundaries() {
        let root = ProtectedPathRule::new("/", None, 1, 0).unwrap();
        assert!(root.matches_path("/"));
        assert!(root.matches_path("/legal"));
        assert!(root.matches_path("/legal/draft.txt"));

        let legal = ProtectedPathRule::new("/legal", None, 2, 7).unwrap();
        assert!(legal.matches_path("/legal"));
        assert!(legal.matches_path("/legal/draft.txt"));
        assert!(!legal.matches_path("/legalese"));
        assert!(!legal.matches_path("/legal/../legal/draft.txt"));
    }

    #[test]
    fn rules_reject_invalid_refs_and_path_prefixes() {
        assert!(ProtectedRefRule::new("refs/heads/main", 1, 0).is_err());
        assert!(ProtectedPathRule::new("relative/path", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("/legal/../secret", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("/legal/", None, 1, 0).is_err());
        assert!(ProtectedPathRule::new("/legal", Some("bad/ref/name"), 1, 0).is_err());

        let input = NewChangeRequest {
            title: "Review release".to_string(),
            description: None,
            source_ref: "refs/heads/topic".to_string(),
            target_ref: "main".to_string(),
            base_commit: "a".repeat(64),
            head_commit: "b".repeat(64),
            created_by: 0,
        };
        assert!(ChangeRequest::new(input).is_err());
    }

    #[tokio::test]
    async fn in_memory_store_creates_lists_gets_and_transitions_change_requests() {
        let store = InMemoryReviewStore::new();

        let ref_rule = store
            .create_protected_ref_rule("main", 2, 10)
            .await
            .unwrap();
        let path_rule = store
            .create_protected_path_rule("/legal", Some("main"), 1, 10)
            .await
            .unwrap();
        let change = store
            .create_change_request(NewChangeRequest {
                title: "Legal update".to_string(),
                description: Some("Needs review".to_string()),
                source_ref: "review/legal-update".to_string(),
                target_ref: "main".to_string(),
                base_commit: "a".repeat(64),
                head_commit: "b".repeat(64),
                created_by: 10,
            })
            .await
            .unwrap();

        assert_eq!(ref_rule.ref_name, "main");
        assert!(path_rule.matches_path("/legal/draft.txt"));
        assert_eq!(change.status, ChangeRequestStatus::Open);
        assert_eq!(change.version, 1);

        assert_eq!(store.list_protected_ref_rules().await.unwrap().len(), 1);
        assert_eq!(store.list_protected_path_rules().await.unwrap().len(), 1);
        assert_eq!(store.list_change_requests().await.unwrap().len(), 1);
        assert_eq!(
            store
                .get_change_request(change.id)
                .await
                .unwrap()
                .unwrap()
                .title,
            "Legal update"
        );

        let rejected = store
            .transition_change_request(change.id, ChangeRequestStatus::Rejected)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rejected.status, ChangeRequestStatus::Rejected);
        assert_eq!(rejected.version, 2);

        assert!(
            store
                .transition_change_request(change.id, ChangeRequestStatus::Merged)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_store_reloads_rules_and_change_requests() {
        let path = temp_review_path("reload");
        let store = LocalReviewStore::open(&path).unwrap();

        let ref_rule = store
            .create_protected_ref_rule("main", 2, 11)
            .await
            .unwrap();
        let path_rule = store
            .create_protected_path_rule("/legal", Some("main"), 1, 11)
            .await
            .unwrap();
        let change = store
            .create_change_request(NewChangeRequest {
                title: "Legal update".to_string(),
                description: None,
                source_ref: "review/legal-update".to_string(),
                target_ref: "main".to_string(),
                base_commit: "a".repeat(64),
                head_commit: "b".repeat(64),
                created_by: 11,
            })
            .await
            .unwrap();
        drop(store);

        let reloaded = LocalReviewStore::open(&path).unwrap();
        assert_eq!(
            reloaded.list_protected_ref_rules().await.unwrap()[0].id,
            ref_rule.id
        );
        assert_eq!(
            reloaded.list_protected_path_rules().await.unwrap()[0].id,
            path_rule.id
        );
        assert_eq!(
            reloaded
                .get_change_request(change.id)
                .await
                .unwrap()
                .unwrap()
                .id,
            change.id
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_store_rejects_corrupt_bytes() {
        let path = temp_review_path("corrupt");
        fs::write(&path, b"not-review").unwrap();

        let err = LocalReviewStore::open(&path).expect_err("corrupt store should fail");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_store_enforces_single_writer_lock() {
        let path = temp_review_path("lock");
        let first = LocalReviewStore::open(&path).unwrap();
        let err = LocalReviewStore::open(&path).expect_err("second writer should fail");
        assert!(matches!(err, crate::error::VfsError::IoError(_)));
        drop(first);

        let second = LocalReviewStore::open(&path).unwrap();
        drop(second);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn local_store_rejects_duplicate_record_ids() {
        let path = temp_review_path("duplicate");
        let duplicate_id = Uuid::new_v4();
        let mut ref_rule = ProtectedRefRule::new("main", 1, 0).unwrap();
        ref_rule.id = duplicate_id;
        let mut path_rule = ProtectedPathRule::new("/legal", None, 1, 0).unwrap();
        path_rule.id = duplicate_id;
        let bytes = crate::codec::serialize(&PersistedReviewStore {
            version: REVIEW_STORE_VERSION,
            protected_refs: vec![ref_rule],
            protected_paths: vec![path_rule],
            change_requests: Vec::new(),
            approvals: Vec::new(),
            assignments: Vec::new(),
            comments: Vec::new(),
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let err = LocalReviewStore::open(&path).expect_err("duplicate IDs should fail");
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
        fs::remove_file(path).unwrap();
    }
}
