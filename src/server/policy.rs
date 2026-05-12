use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use uuid::Uuid;

use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::Uid;
use crate::auth::session::Session;
use crate::backend::RepoId;
use crate::error::VfsError;
use crate::review::{ProtectedPathRule, ReviewStore};
use crate::store::ObjectId;
use crate::vcs::MAIN_REF;

const DECISION_ALLOW: &str = "allow";
const DECISION_DENY: &str = "deny";
const POLICY_DETAIL_TARGET_REF_MAX_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoutePolicyAction {
    FsWrite,
    FsMkdir,
    FsDelete,
    FsCopy,
    FsMove,
    FsMetadataUpdate,
    VcsCommit,
    VcsRevert,
    VcsRefCreate,
    VcsRefUpdate,
    ReviewMerge,
    ReviewReject,
}

pub(crate) type PolicyAction = RoutePolicyAction;

impl PolicyAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::FsWrite => "fs_write",
            Self::FsMkdir => "fs_mkdir",
            Self::FsDelete => "fs_delete",
            Self::FsCopy => "fs_copy",
            Self::FsMove => "fs_move",
            Self::FsMetadataUpdate => "fs_metadata_update",
            Self::VcsCommit => "vcs_commit",
            Self::VcsRevert => "vcs_revert",
            Self::VcsRefCreate => "vcs_ref_create",
            Self::VcsRefUpdate => "vcs_ref_update",
            Self::ReviewMerge => "review_merge",
            Self::ReviewReject => "review_reject",
        }
    }

    fn checks_protected_refs(self) -> bool {
        matches!(self, Self::VcsCommit | Self::VcsRevert | Self::VcsRefUpdate)
    }

    fn checks_protected_paths(self) -> bool {
        matches!(
            self,
            Self::FsWrite
                | Self::FsMkdir
                | Self::FsDelete
                | Self::FsCopy
                | Self::FsMove
                | Self::FsMetadataUpdate
                | Self::VcsRevert
                | Self::ReviewMerge
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyActor {
    pub(crate) uid: Uid,
    pub(crate) username: String,
}

pub(crate) type PolicyActor = RoutePolicyActor;

impl PolicyActor {
    pub(crate) fn from_session(session: &Session) -> Self {
        Self {
            uid: session.effective_uid(),
            username: session.username.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct RoutePolicyCorrelation {
    pub(crate) request_present: bool,
    pub(crate) idempotency_present: bool,
}

#[expect(
    dead_code,
    reason = "compatibility alias for promoted shared policy type"
)]
pub(crate) type PolicyCorrelation = RoutePolicyCorrelation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyRequest {
    pub(crate) action: RoutePolicyAction,
    pub(crate) actor: RoutePolicyActor,
    pub(crate) repo_id: Option<RepoId>,
    pub(crate) workspace_id: Option<Uuid>,
    pub(crate) workspace_root: Option<String>,
    pub(crate) session_ref: Option<String>,
    pub(crate) workspace_scope: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) changed_paths: Vec<String>,
    pub(crate) include_protected_descendants: bool,
    pub(crate) correlation: RoutePolicyCorrelation,
    pub(crate) review_approval: Option<RoutePolicyReviewApproval>,
}

pub(crate) type PolicyRequest = RoutePolicyRequest;

impl PolicyRequest {
    pub(crate) fn from_session(action: RoutePolicyAction, session: &Session) -> Self {
        let mount = session.mount();
        let target_ref = mount
            .map(|mount| mount.base_ref().to_string())
            .unwrap_or_else(|| MAIN_REF.to_string());
        Self {
            action,
            actor: RoutePolicyActor::from_session(session),
            repo_id: None,
            workspace_id: mount.map(|mount| mount.workspace_id()),
            workspace_root: mount.map(|mount| mount.root_path().to_string()),
            session_ref: mount.and_then(|mount| mount.session_ref().map(str::to_string)),
            workspace_scope: session.scope.as_ref().map(|_| "scoped".to_string()),
            target_ref: Some(target_ref),
            changed_paths: Vec::new(),
            include_protected_descendants: false,
            correlation: RoutePolicyCorrelation::default(),
            review_approval: None,
        }
    }

    pub(crate) fn with_target_ref(mut self, target_ref: impl Into<String>) -> Self {
        self.target_ref = Some(target_ref.into());
        self
    }

    pub(crate) fn with_repo_id(mut self, repo_id: RepoId) -> Self {
        self.repo_id = Some(repo_id);
        self
    }

    pub(crate) fn with_changed_paths<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.changed_paths = paths.into_iter().map(Into::into).collect();
        self
    }

    pub(crate) fn include_protected_descendants(mut self) -> Self {
        self.include_protected_descendants = true;
        self
    }

    pub(crate) fn with_correlation(mut self, correlation: RoutePolicyCorrelation) -> Self {
        self.correlation = correlation;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyReviewApproval {
    pub(crate) approved: bool,
    pub(crate) change_request_id: Uuid,
    pub(crate) matched_ref_rule_count: usize,
    pub(crate) matched_path_rule_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoutePolicyDenyReason {
    MissingRepoContext,
    ProtectedRef,
    ProtectedPath,
    ReviewApprovalRequired,
}

pub(crate) type PolicyDenyReason = RoutePolicyDenyReason;

impl PolicyDenyReason {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::MissingRepoContext => "missing_repo_context",
            Self::ProtectedRef => "protected_ref",
            Self::ProtectedPath => "protected_path",
            Self::ReviewApprovalRequired => "review_approval_required",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoutePolicyDecision {
    Allow(RoutePolicyDecisionDetails),
    Deny {
        reason: RoutePolicyDenyReason,
        details: RoutePolicyDecisionDetails,
    },
}

pub(crate) type PolicyDecision = RoutePolicyDecision;

impl PolicyDecision {
    pub(crate) fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow(_))
    }

    pub(crate) fn deny_reason(&self) -> Option<RoutePolicyDenyReason> {
        match self {
            Self::Allow(_) => None,
            Self::Deny { reason, .. } => Some(*reason),
        }
    }

    pub(crate) fn details(&self) -> &RoutePolicyDecisionDetails {
        match self {
            Self::Allow(details) | Self::Deny { details, .. } => details,
        }
    }

    pub(crate) fn redacted_details(&self) -> BTreeMap<String, String> {
        self.details().redacted_details(self.deny_reason())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyDecisionDetails {
    action: RoutePolicyAction,
    decision: &'static str,
    actor_uid: Uid,
    actor_username_present: bool,
    repo_id: Option<RepoId>,
    target_ref: Option<String>,
    changed_path_count: usize,
    path_digests: BTreeSet<ObjectId>,
    descendant_path_digests: BTreeSet<ObjectId>,
    matched_ref_rule_count: usize,
    matched_path_rule_count: usize,
    workspace_id: Option<Uuid>,
    workspace_scope_present: bool,
    session_ref_present: bool,
    correlation_present: bool,
    idempotency_present: bool,
    change_request_id: Option<Uuid>,
}

pub(crate) type PolicyDecisionDetails = RoutePolicyDecisionDetails;

impl fmt::Debug for RoutePolicyDecisionDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutePolicyDecisionDetails")
            .field("action", &self.action)
            .field("decision", &self.decision)
            .field("actor_uid", &self.actor_uid)
            .field("actor_username_present", &self.actor_username_present)
            .field("repo_id", &self.repo_id)
            .field("target_ref_present", &self.target_ref.is_some())
            .field(
                "target_ref_len",
                &self.target_ref.as_ref().map(|target_ref| target_ref.len()),
            )
            .field("changed_path_count", &self.changed_path_count)
            .field("path_digest_count", &self.path_digests.len())
            .field(
                "descendant_path_digest_count",
                &self.descendant_path_digests.len(),
            )
            .field("matched_ref_rule_count", &self.matched_ref_rule_count)
            .field("matched_path_rule_count", &self.matched_path_rule_count)
            .field("workspace_id", &self.workspace_id)
            .field("workspace_scope_present", &self.workspace_scope_present)
            .field("session_ref_present", &self.session_ref_present)
            .field("correlation_present", &self.correlation_present)
            .field("idempotency_present", &self.idempotency_present)
            .field("change_request_id", &self.change_request_id)
            .finish()
    }
}

impl PolicyDecisionDetails {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "policy token tests inspect target ref")
    )]
    pub(crate) fn target_ref(&self) -> Option<&str> {
        self.target_ref.as_deref()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "policy tests and audit review use count accessors"
        )
    )]
    pub(crate) fn changed_path_count(&self) -> usize {
        self.changed_path_count
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "policy tests and audit review use count accessors"
        )
    )]
    pub(crate) fn matched_ref_rule_count(&self) -> usize {
        self.matched_ref_rule_count
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "policy tests and audit review use count accessors"
        )
    )]
    pub(crate) fn matched_path_rule_count(&self) -> usize {
        self.matched_path_rule_count
    }

    fn redacted_details(
        &self,
        deny_reason: Option<RoutePolicyDenyReason>,
    ) -> BTreeMap<String, String> {
        let mut details = BTreeMap::new();
        details.insert("action".to_string(), self.action.as_str().to_string());
        details.insert("decision".to_string(), self.decision.to_string());
        details.insert("actor_uid".to_string(), self.actor_uid.to_string());
        details.insert(
            "actor_username_present".to_string(),
            self.actor_username_present.to_string(),
        );
        if let Some(reason) = deny_reason {
            details.insert("reason".to_string(), reason.code().to_string());
        }
        if let Some(repo_id) = &self.repo_id {
            details.insert("repo_id".to_string(), repo_id.to_string());
        }
        if let Some(target_ref) = &self.target_ref {
            details.insert(
                "target_ref".to_string(),
                bounded_policy_target_ref_detail(target_ref),
            );
        }
        details.insert(
            "changed_path_count".to_string(),
            self.changed_path_count.to_string(),
        );
        details.insert(
            "descendant_path_count".to_string(),
            self.descendant_path_digests.len().to_string(),
        );
        details.insert(
            "matched_ref_rule_count".to_string(),
            self.matched_ref_rule_count.to_string(),
        );
        details.insert(
            "matched_path_rule_count".to_string(),
            self.matched_path_rule_count.to_string(),
        );
        if let Some(workspace_id) = self.workspace_id {
            details.insert("workspace_id".to_string(), workspace_id.to_string());
        }
        if let Some(change_request_id) = self.change_request_id {
            details.insert(
                "change_request_id".to_string(),
                change_request_id.to_string(),
            );
        }
        details.insert(
            "workspace_scope_present".to_string(),
            self.workspace_scope_present.to_string(),
        );
        details.insert(
            "session_ref_present".to_string(),
            self.session_ref_present.to_string(),
        );
        details.insert(
            "correlation_present".to_string(),
            self.correlation_present.to_string(),
        );
        details.insert(
            "idempotency_present".to_string(),
            self.idempotency_present.to_string(),
        );
        details
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyEvaluation {
    pub(crate) decision: RoutePolicyDecision,
    pub(crate) applicable_path_rules: Vec<ProtectedPathRule>,
    pub(crate) denied_path: Option<String>,
}

pub(crate) type PolicyEvaluation = RoutePolicyEvaluation;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PolicyDecisionToken {
    details: PolicyDecisionDetails,
}

impl fmt::Debug for PolicyDecisionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolicyDecisionToken")
            .field("details", &self.details)
            .finish()
    }
}

impl PolicyDecisionToken {
    pub(crate) fn from_allowed_evaluation(evaluation: &PolicyEvaluation) -> Result<Self, VfsError> {
        match &evaluation.decision {
            PolicyDecision::Allow(details) => Ok(Self {
                details: details.clone(),
            }),
            PolicyDecision::Deny { .. } => Err(policy_token_error()),
        }
    }

    pub(crate) fn from_review_approved_evaluation(
        evaluation: &PolicyEvaluation,
    ) -> Result<Self, VfsError> {
        let token = Self::from_allowed_evaluation(evaluation)?;
        if token.details.action == PolicyAction::ReviewMerge
            && token.details.change_request_id.is_some()
        {
            Ok(token)
        } else {
            Err(policy_token_error())
        }
    }

    pub(crate) fn require_allowed_for(
        &self,
        repo_id: &RepoId,
        action: PolicyAction,
        target_ref: &str,
    ) -> Result<(), VfsError> {
        if self.details.decision != DECISION_ALLOW {
            return Err(policy_token_error());
        }
        if self.details.action != action {
            return Err(policy_token_error());
        }
        if self.details.repo_id.as_ref() != Some(repo_id) {
            return Err(policy_token_error());
        }
        if self.details.target_ref.as_deref() != Some(target_ref) {
            return Err(policy_token_error());
        }
        Ok(())
    }

    pub(crate) fn require_allowed_for_paths<I, S>(
        &self,
        repo_id: &RepoId,
        action: PolicyAction,
        target_ref: &str,
        paths: I,
    ) -> Result<(), VfsError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.require_allowed_for(repo_id, action, target_ref)?;
        let required = policy_path_digests(paths);
        if required.is_empty()
            || !required
                .iter()
                .all(|digest| self.details.path_digests.contains(digest))
        {
            return Err(policy_token_error());
        }
        Ok(())
    }

    pub(crate) fn require_allowed_for_changed_paths<I, S>(
        &self,
        repo_id: &RepoId,
        action: PolicyAction,
        target_ref: &str,
        paths: I,
    ) -> Result<(), VfsError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.require_allowed_for(repo_id, action, target_ref)?;
        let required = policy_path_digests(paths);
        if !required
            .iter()
            .all(|digest| self.details.path_digests.contains(digest))
        {
            return Err(policy_token_error());
        }
        Ok(())
    }

    pub(crate) fn require_review_approved_for_changed_paths<I, S>(
        &self,
        repo_id: &RepoId,
        target_ref: &str,
        change_request_id: Uuid,
        paths: I,
    ) -> Result<(), VfsError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.require_allowed_for_changed_paths(
            repo_id,
            PolicyAction::ReviewMerge,
            target_ref,
            paths,
        )?;
        if self.details.change_request_id != Some(change_request_id) {
            return Err(policy_token_error());
        }
        Ok(())
    }

    pub(crate) fn require_allowed_for_paths_with_descendants<I, S, D, P>(
        &self,
        repo_id: &RepoId,
        action: PolicyAction,
        target_ref: &str,
        paths: I,
        descendant_paths: D,
    ) -> Result<(), VfsError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
        D: IntoIterator<Item = P>,
        P: AsRef<str>,
    {
        self.require_allowed_for_paths(repo_id, action, target_ref, paths)?;
        let required_descendants = policy_path_digests(descendant_paths);
        if !required_descendants
            .iter()
            .all(|digest| self.details.descendant_path_digests.contains(digest))
        {
            return Err(policy_token_error());
        }
        Ok(())
    }

    pub(crate) fn combine_allowed_for_same_scope(&self, other: &Self) -> Result<Self, VfsError> {
        if self.details.decision != DECISION_ALLOW
            || other.details.decision != DECISION_ALLOW
            || self.details.action != other.details.action
            || self.details.actor_uid != other.details.actor_uid
            || self.details.repo_id != other.details.repo_id
            || self.details.target_ref != other.details.target_ref
            || self.details.workspace_id != other.details.workspace_id
            || self.details.session_ref_present != other.details.session_ref_present
        {
            return Err(policy_token_error());
        }

        let mut details = self.details.clone();
        details.changed_path_count = details
            .changed_path_count
            .saturating_add(other.details.changed_path_count);
        details.matched_ref_rule_count = details
            .matched_ref_rule_count
            .saturating_add(other.details.matched_ref_rule_count);
        details.matched_path_rule_count = details
            .matched_path_rule_count
            .saturating_add(other.details.matched_path_rule_count);
        details
            .path_digests
            .extend(other.details.path_digests.iter().copied());
        details
            .descendant_path_digests
            .extend(other.details.descendant_path_digests.iter().copied());
        Ok(Self { details })
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "policy token tests inspect private details")
    )]
    pub(crate) fn details(&self) -> &PolicyDecisionDetails {
        &self.details
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "review merge token enforcement lands in the next plan task"
        )
    )]
    pub(crate) fn redacted_details(&self) -> BTreeMap<String, String> {
        self.details.redacted_details(None)
    }

    #[cfg(test)]
    pub(crate) fn allow_for_test(
        action: PolicyAction,
        target_ref: &str,
        changed_path_count: usize,
    ) -> Self {
        Self::allow_for_test_with_repo(RepoId::local(), action, target_ref, changed_path_count)
    }

    #[cfg(test)]
    pub(crate) fn allow_for_test_with_repo(
        repo_id: RepoId,
        action: PolicyAction,
        target_ref: &str,
        changed_path_count: usize,
    ) -> Self {
        Self {
            details: PolicyDecisionDetails {
                action,
                decision: DECISION_ALLOW,
                actor_uid: crate::auth::ROOT_UID,
                actor_username_present: true,
                repo_id: Some(repo_id),
                target_ref: Some(target_ref.to_string()),
                changed_path_count,
                path_digests: BTreeSet::new(),
                descendant_path_digests: BTreeSet::new(),
                matched_ref_rule_count: 0,
                matched_path_rule_count: 0,
                workspace_id: None,
                workspace_scope_present: false,
                session_ref_present: false,
                correlation_present: false,
                idempotency_present: false,
                change_request_id: None,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn allow_for_test_with_paths<I, S>(
        action: PolicyAction,
        target_ref: &str,
        paths: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let path_digests = policy_path_digests(paths);
        Self::allow_for_test_with_path_digests(action, target_ref, path_digests, BTreeSet::new())
    }

    #[cfg(test)]
    pub(crate) fn allow_for_test_with_paths_and_descendants<I, S, D, P>(
        action: PolicyAction,
        target_ref: &str,
        paths: I,
        descendant_paths: D,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
        D: IntoIterator<Item = P>,
        P: AsRef<str>,
    {
        Self::allow_for_test_with_path_digests(
            action,
            target_ref,
            policy_path_digests(paths),
            policy_path_digests(descendant_paths),
        )
    }

    #[cfg(test)]
    fn allow_for_test_with_path_digests(
        action: PolicyAction,
        target_ref: &str,
        path_digests: BTreeSet<ObjectId>,
        descendant_path_digests: BTreeSet<ObjectId>,
    ) -> Self {
        let changed_path_count = path_digests.len();
        Self {
            details: PolicyDecisionDetails {
                action,
                decision: DECISION_ALLOW,
                actor_uid: crate::auth::ROOT_UID,
                actor_username_present: true,
                repo_id: Some(RepoId::local()),
                target_ref: Some(target_ref.to_string()),
                changed_path_count,
                path_digests,
                descendant_path_digests,
                matched_ref_rule_count: 0,
                matched_path_rule_count: 0,
                workspace_id: None,
                workspace_scope_present: false,
                session_ref_present: false,
                correlation_present: false,
                idempotency_present: false,
                change_request_id: None,
            },
        }
    }
}

pub(crate) fn require_policy_token_allowed_for(
    token: Option<&PolicyDecisionToken>,
    repo_id: &RepoId,
    action: PolicyAction,
    target_ref: &str,
) -> Result<(), VfsError> {
    token
        .ok_or_else(policy_token_error)?
        .require_allowed_for(repo_id, action, target_ref)
}

pub(crate) fn require_policy_token_allowed_for_paths_with_descendants<I, S, D, P>(
    token: Option<&PolicyDecisionToken>,
    repo_id: &RepoId,
    action: PolicyAction,
    target_ref: &str,
    paths: I,
    descendant_paths: D,
) -> Result<(), VfsError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
    D: IntoIterator<Item = P>,
    P: AsRef<str>,
{
    token
        .ok_or_else(policy_token_error)?
        .require_allowed_for_paths_with_descendants(
            repo_id,
            action,
            target_ref,
            paths,
            descendant_paths,
        )
}

fn policy_token_error() -> VfsError {
    VfsError::PermissionDenied {
        path: "policy decision token".to_string(),
    }
}

fn policy_path_digests<I, S>(paths: I) -> BTreeSet<ObjectId>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    paths
        .into_iter()
        .map(|path| {
            let path = path.as_ref();
            let mut bytes = Vec::with_capacity("stratum-policy-path-v1\0".len() + path.len());
            bytes.extend_from_slice(b"stratum-policy-path-v1\0");
            bytes.extend_from_slice(path.as_bytes());
            ObjectId::from_bytes(&bytes)
        })
        .collect()
}

fn bounded_policy_target_ref_detail(target_ref: &str) -> String {
    if target_ref.len() <= POLICY_DETAIL_TARGET_REF_MAX_BYTES {
        return target_ref.to_string();
    }

    let mut bytes = Vec::with_capacity("stratum-policy-target-ref-v1\0".len() + target_ref.len());
    bytes.extend_from_slice(b"stratum-policy-target-ref-v1\0");
    bytes.extend_from_slice(target_ref.as_bytes());
    let digest = ObjectId::from_bytes(&bytes).to_hex();
    format!("<redacted:{} bytes:{digest}>", target_ref.len())
}

pub(crate) fn audit_event_from_policy_evaluation(
    session: &Session,
    evaluation: &RoutePolicyEvaluation,
) -> NewAuditEvent {
    let action = if evaluation.decision.is_allowed() {
        AuditAction::PolicyDecisionAllow
    } else {
        AuditAction::PolicyDecisionDeny
    };
    let mut event = NewAuditEvent::from_session(
        session,
        action,
        AuditResource::id(
            AuditResourceKind::PolicyDecision,
            evaluation.decision.details().action.as_str(),
        ),
    );
    event.details = evaluation.decision.redacted_details();
    event
}

pub(crate) async fn evaluate_route_policy(
    review_store: &dyn ReviewStore,
    request: RoutePolicyRequest,
) -> Result<RoutePolicyEvaluation, VfsError> {
    let Some(repo_id) = request.repo_id.as_ref() else {
        if request.action.checks_protected_refs() || request.action.checks_protected_paths() {
            return Ok(RoutePolicyEvaluation {
                decision: RoutePolicyDecision::Deny {
                    reason: RoutePolicyDenyReason::MissingRepoContext,
                    details: details(&request, DECISION_DENY, 0, 0),
                },
                applicable_path_rules: Vec::new(),
                denied_path: None,
            });
        }
        return Ok(RoutePolicyEvaluation {
            decision: RoutePolicyDecision::Allow(details(&request, DECISION_ALLOW, 0, 0)),
            applicable_path_rules: Vec::new(),
            denied_path: None,
        });
    };

    let ref_rules = review_store
        .list_protected_ref_rules_for_repo(repo_id)
        .await?;
    let matched_ref_rule_count = request
        .target_ref
        .as_deref()
        .map(|target_ref| {
            ref_rules
                .iter()
                .filter(|rule| rule.active && rule.ref_name == target_ref)
                .count()
        })
        .unwrap_or(0);

    let path_rules = review_store
        .list_protected_path_rules_for_repo(repo_id)
        .await?;
    let applicable_path_rules = path_rules
        .into_iter()
        .filter(|rule| rule.active && path_rule_target_matches(rule, request.target_ref.as_deref()))
        .collect::<Vec<_>>();
    let matched_path_rule_count = if request.action.checks_protected_paths() {
        applicable_path_rules
            .iter()
            .filter(|rule| {
                request.changed_paths.iter().any(|path| {
                    rule.matches_path(path)
                        || (request.include_protected_descendants
                            && protected_rule_is_descendant(rule, path))
                })
            })
            .count()
    } else {
        0
    };
    let denied_path = if request.action.checks_protected_paths() {
        first_denied_path(&request, &applicable_path_rules)
    } else {
        None
    };

    let decision = if request.action == RoutePolicyAction::ReviewMerge {
        match &request.review_approval {
            Some(approval) if approval.approved => RoutePolicyDecision::Allow(details(
                &request,
                DECISION_ALLOW,
                approval.matched_ref_rule_count,
                approval.matched_path_rule_count,
            )),
            Some(approval) => RoutePolicyDecision::Deny {
                reason: RoutePolicyDenyReason::ReviewApprovalRequired,
                details: details(
                    &request,
                    DECISION_DENY,
                    approval.matched_ref_rule_count,
                    approval.matched_path_rule_count,
                ),
            },
            _ => RoutePolicyDecision::Allow(details(
                &request,
                DECISION_ALLOW,
                matched_ref_rule_count,
                matched_path_rule_count,
            )),
        }
    } else if request.action.checks_protected_refs() && matched_ref_rule_count > 0 {
        RoutePolicyDecision::Deny {
            reason: RoutePolicyDenyReason::ProtectedRef,
            details: details(
                &request,
                DECISION_DENY,
                matched_ref_rule_count,
                matched_path_rule_count,
            ),
        }
    } else if request.action.checks_protected_paths() && matched_path_rule_count > 0 {
        RoutePolicyDecision::Deny {
            reason: RoutePolicyDenyReason::ProtectedPath,
            details: details(
                &request,
                DECISION_DENY,
                matched_ref_rule_count,
                matched_path_rule_count,
            ),
        }
    } else {
        RoutePolicyDecision::Allow(details(
            &request,
            DECISION_ALLOW,
            matched_ref_rule_count,
            matched_path_rule_count,
        ))
    };

    Ok(RoutePolicyEvaluation {
        decision,
        applicable_path_rules,
        denied_path,
    })
}

fn details(
    request: &RoutePolicyRequest,
    decision: &'static str,
    matched_ref_rule_count: usize,
    matched_path_rule_count: usize,
) -> RoutePolicyDecisionDetails {
    RoutePolicyDecisionDetails {
        action: request.action,
        decision,
        actor_uid: request.actor.uid,
        actor_username_present: !request.actor.username.is_empty(),
        repo_id: request.repo_id.clone(),
        target_ref: request.target_ref.clone(),
        changed_path_count: request.changed_paths.len(),
        path_digests: policy_path_digests(&request.changed_paths),
        descendant_path_digests: if request.include_protected_descendants {
            policy_path_digests(&request.changed_paths)
        } else {
            BTreeSet::new()
        },
        matched_ref_rule_count,
        matched_path_rule_count,
        workspace_id: request.workspace_id,
        workspace_scope_present: request.workspace_scope.is_some(),
        session_ref_present: request.session_ref.is_some(),
        correlation_present: request.correlation.request_present,
        idempotency_present: request.correlation.idempotency_present,
        change_request_id: request
            .review_approval
            .as_ref()
            .map(|approval| approval.change_request_id),
    }
}

fn path_rule_target_matches(rule: &ProtectedPathRule, target_ref: Option<&str>) -> bool {
    rule.target_ref
        .as_deref()
        .is_none_or(|rule_target| target_ref == Some(rule_target))
}

fn protected_rule_is_descendant(rule: &ProtectedPathRule, path: &str) -> bool {
    if !rule.active {
        return false;
    }
    let Ok(path) = crate::review::normalize_path_prefix(path) else {
        return false;
    };
    if path == "/" {
        return true;
    }
    rule.path_prefix
        .strip_prefix(&path)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

fn first_denied_path(
    request: &RoutePolicyRequest,
    applicable_path_rules: &[ProtectedPathRule],
) -> Option<String> {
    request.changed_paths.iter().find_map(|path| {
        applicable_path_rules
            .iter()
            .any(|rule| {
                rule.matches_path(path)
                    || (request.include_protected_descendants
                        && protected_rule_is_descendant(rule, path))
            })
            .then(|| path.clone())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::auth::ROOT_UID;
    use crate::auth::session::Session;
    use crate::review::InMemoryReviewStore;
    use crate::vcs::MAIN_REF;

    use super::*;

    #[tokio::test]
    async fn protected_ref_policy_denies_matching_ref_with_redacted_reason() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_ref_rule(MAIN_REF, 1, ROOT_UID)
            .await
            .unwrap();

        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::VcsCommit, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF);
        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();

        assert_eq!(
            evaluation.decision.deny_reason(),
            Some(RoutePolicyDenyReason::ProtectedRef)
        );
        let details = evaluation.decision.redacted_details();
        assert_eq!(
            details.get("reason").map(String::as_str),
            Some("protected_ref")
        );
        assert_eq!(
            details.get("target_ref").map(String::as_str),
            Some(MAIN_REF)
        );
    }

    #[tokio::test]
    async fn protected_aware_policy_fails_closed_without_repo_context() {
        let review = Arc::new(InMemoryReviewStore::new());
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/legal/a.txt"]);

        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();

        assert_eq!(
            evaluation.decision.deny_reason(),
            Some(RoutePolicyDenyReason::MissingRepoContext)
        );
    }

    #[tokio::test]
    async fn protected_path_policy_matches_target_ref_and_boundary_prefix() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_path_rule("/legal", Some(MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();

        let blocked =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/legal/a.txt"]);
        let blocked = evaluate_route_policy(review.as_ref(), blocked)
            .await
            .unwrap();
        assert_eq!(
            blocked.decision.deny_reason(),
            Some(RoutePolicyDenyReason::ProtectedPath)
        );
        assert_eq!(blocked.decision.details().matched_path_rule_count(), 1);

        let allowed =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/legalese/a.txt"]);
        let allowed = evaluate_route_policy(review.as_ref(), allowed)
            .await
            .unwrap();
        assert!(allowed.decision.is_allowed());
        assert_eq!(allowed.decision.details().matched_path_rule_count(), 0);

        let other_ref =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref("feature")
                .with_changed_paths(["/legal/a.txt"]);
        let other_ref = evaluate_route_policy(review.as_ref(), other_ref)
            .await
            .unwrap();
        assert!(other_ref.decision.is_allowed());
    }

    #[tokio::test]
    async fn unmounted_filesystem_policy_defaults_to_main_ref() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_path_rule("/legal", Some(MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();

        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_changed_paths(["/legal/a.txt"]);
        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();

        assert_eq!(
            evaluation.decision.deny_reason(),
            Some(RoutePolicyDenyReason::ProtectedPath)
        );
    }

    #[tokio::test]
    async fn policy_decision_details_are_bounded_and_content_free() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_path_rule("/private", Some(MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();
        let paths = (0..64)
            .map(|index| format!("/private/body-like-value-{index}.txt"))
            .collect::<Vec<_>>();
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(paths.clone());

        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();
        let details = evaluation.decision.redacted_details();

        assert_eq!(
            evaluation.decision.details().changed_path_count(),
            paths.len()
        );
        assert_eq!(
            details.get("changed_path_count").map(String::as_str),
            Some("64")
        );
        assert!(
            !details
                .values()
                .any(|value| value.contains("body-like-value"))
        );
        assert!(!details.keys().any(|key| key.contains("body-like-value")));
        assert!(details.len() <= 16);
    }

    #[tokio::test]
    async fn policy_decision_details_do_not_store_actor_username_values() {
        let review = Arc::new(InMemoryReviewStore::new());
        let mut session = Session::root();
        session.username = "sensitive-actor-name".repeat(64);
        let request = RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &session)
            .with_repo_id(RepoId::local())
            .with_target_ref(MAIN_REF)
            .with_changed_paths(["/public.txt"]);

        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();
        let details = evaluation.decision.redacted_details();
        let rendered = serde_json::to_string(&details).unwrap();

        assert!(!rendered.contains("sensitive-actor-name"));
        assert_eq!(
            details.get("actor_username_present").map(String::as_str),
            Some("true")
        );
        assert!(details.values().all(|value| value.len() <= 64));
    }

    #[tokio::test]
    async fn policy_decision_details_bound_long_target_refs() {
        let review = Arc::new(InMemoryReviewStore::new());
        let long_ref = format!("refs/{}", "sensitive-ref-component".repeat(32));
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::VcsRefUpdate, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(&long_ref);

        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();
        let details = evaluation.decision.redacted_details();
        let target_ref = details.get("target_ref").expect("target ref detail");

        assert!(!target_ref.contains("sensitive-ref-component"));
        assert!(target_ref.len() <= 96, "{target_ref}");
        assert!(target_ref.starts_with("<redacted:"));
    }

    #[tokio::test]
    async fn policy_token_cannot_be_created_from_denied_decision() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_path_rule("/private", Some(MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/private/body-like-value.txt"]);
        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();

        let err = PolicyDecisionToken::from_allowed_evaluation(&evaluation)
            .expect_err("denied policy decisions must not mint tokens");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn policy_token_rejects_action_and_target_mismatch_without_paths() {
        let review = Arc::new(InMemoryReviewStore::new());
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/public.txt"]);
        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();
        let token = PolicyDecisionToken::from_allowed_evaluation(&evaluation).unwrap();

        assert!(
            token
                .require_allowed_for(&RepoId::local(), RoutePolicyAction::FsWrite, MAIN_REF)
                .is_ok()
        );
        assert!(matches!(
            token.require_allowed_for(&RepoId::local(), RoutePolicyAction::FsDelete, MAIN_REF),
            Err(VfsError::PermissionDenied { .. })
        ));
        assert!(matches!(
            token.require_allowed_for(&RepoId::local(), RoutePolicyAction::FsWrite, "feature"),
            Err(VfsError::PermissionDenied { .. })
        ));
        assert!(matches!(
            token.require_allowed_for(
                &RepoId::new("other").unwrap(),
                RoutePolicyAction::FsWrite,
                MAIN_REF
            ),
            Err(VfsError::PermissionDenied { .. })
        ));
        assert_eq!(token.details().changed_path_count(), 1);
        assert_eq!(token.details().target_ref(), Some(MAIN_REF));
    }

    #[tokio::test]
    async fn policy_token_rejects_missing_descendant_policy_for_path() {
        let token = PolicyDecisionToken::allow_for_test_with_paths(
            RoutePolicyAction::FsDelete,
            MAIN_REF,
            ["/parent"],
        );

        assert!(matches!(
            token.require_allowed_for_paths_with_descendants(
                &RepoId::local(),
                RoutePolicyAction::FsDelete,
                MAIN_REF,
                ["/parent"],
                ["/parent"],
            ),
            Err(VfsError::PermissionDenied { .. })
        ));

        let token = PolicyDecisionToken::allow_for_test_with_paths_and_descendants(
            RoutePolicyAction::FsDelete,
            MAIN_REF,
            ["/parent"],
            ["/parent"],
        );
        assert!(
            token
                .require_allowed_for_paths_with_descendants(
                    &RepoId::local(),
                    RoutePolicyAction::FsDelete,
                    MAIN_REF,
                    ["/parent"],
                    ["/parent"],
                )
                .is_ok()
        );
    }

    #[tokio::test]
    async fn review_approved_policy_token_carries_bounded_counts() {
        let review = Arc::new(InMemoryReviewStore::new());
        let change_request_id = Uuid::new_v4();
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::ReviewMerge, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/private/body-like-value.txt"]);
        let request = RoutePolicyRequest {
            review_approval: Some(RoutePolicyReviewApproval {
                approved: true,
                change_request_id,
                matched_ref_rule_count: 2,
                matched_path_rule_count: 3,
            }),
            ..request
        };
        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();

        let token = PolicyDecisionToken::from_review_approved_evaluation(&evaluation).unwrap();
        let details = token.redacted_details();

        assert_eq!(token.details().matched_ref_rule_count(), 2);
        assert_eq!(token.details().matched_path_rule_count(), 3);
        assert_eq!(
            details.get("change_request_id").map(String::as_str),
            Some(change_request_id.to_string().as_str())
        );
        assert!(!details.values().any(|value| value.contains("body-like")));
    }

    #[tokio::test]
    async fn review_policy_token_rejects_wrong_change_request_or_changed_path() {
        let review = Arc::new(InMemoryReviewStore::new());
        let change_request_id = Uuid::new_v4();
        let request =
            RoutePolicyRequest::from_session(RoutePolicyAction::ReviewMerge, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/reviewed.txt"]);
        let request = RoutePolicyRequest {
            review_approval: Some(RoutePolicyReviewApproval {
                approved: true,
                change_request_id,
                matched_ref_rule_count: 0,
                matched_path_rule_count: 0,
            }),
            ..request
        };
        let evaluation = evaluate_route_policy(review.as_ref(), request)
            .await
            .unwrap();
        let token = PolicyDecisionToken::from_review_approved_evaluation(&evaluation).unwrap();

        assert!(
            token
                .require_review_approved_for_changed_paths(
                    &RepoId::local(),
                    MAIN_REF,
                    change_request_id,
                    ["/reviewed.txt"],
                )
                .is_ok()
        );
        assert!(matches!(
            token.require_review_approved_for_changed_paths(
                &RepoId::local(),
                MAIN_REF,
                Uuid::new_v4(),
                ["/reviewed.txt"],
            ),
            Err(VfsError::PermissionDenied { .. })
        ));
        assert!(matches!(
            token.require_review_approved_for_changed_paths(
                &RepoId::local(),
                MAIN_REF,
                change_request_id,
                ["/other.txt"],
            ),
            Err(VfsError::PermissionDenied { .. })
        ));
    }

    #[tokio::test]
    async fn ref_create_and_review_reject_do_not_block_on_protected_ref() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_ref_rule(MAIN_REF, 1, ROOT_UID)
            .await
            .unwrap();

        let create =
            RoutePolicyRequest::from_session(RoutePolicyAction::VcsRefCreate, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF);
        let create = evaluate_route_policy(review.as_ref(), create)
            .await
            .unwrap();
        assert!(create.decision.is_allowed());
        assert_eq!(create.decision.details().matched_ref_rule_count(), 1);

        let reject =
            RoutePolicyRequest::from_session(RoutePolicyAction::ReviewReject, &Session::root())
                .with_repo_id(RepoId::local())
                .with_target_ref(MAIN_REF);
        let reject = evaluate_route_policy(review.as_ref(), reject)
            .await
            .unwrap();
        assert!(reject.decision.is_allowed());
    }
}
