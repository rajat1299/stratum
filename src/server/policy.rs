use std::collections::BTreeMap;

use uuid::Uuid;

use crate::audit::{AuditAction, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::Uid;
use crate::auth::session::Session;
use crate::error::VfsError;
use crate::review::{ProtectedPathRule, ReviewStore};
use crate::vcs::MAIN_REF;

const DECISION_ALLOW: &str = "allow";
const DECISION_DENY: &str = "deny";

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

impl RoutePolicyAction {
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

impl RoutePolicyActor {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyRequest {
    pub(crate) action: RoutePolicyAction,
    pub(crate) actor: RoutePolicyActor,
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

impl RoutePolicyRequest {
    pub(crate) fn from_session(action: RoutePolicyAction, session: &Session) -> Self {
        let mount = session.mount();
        let target_ref = mount
            .map(|mount| mount.base_ref().to_string())
            .unwrap_or_else(|| MAIN_REF.to_string());
        Self {
            action,
            actor: RoutePolicyActor::from_session(session),
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
    ProtectedRef,
    ProtectedPath,
    ReviewApprovalRequired,
}

impl RoutePolicyDenyReason {
    pub(crate) fn code(self) -> &'static str {
        match self {
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

impl RoutePolicyDecision {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutePolicyDecisionDetails {
    action: RoutePolicyAction,
    decision: &'static str,
    target_ref: Option<String>,
    changed_path_count: usize,
    matched_ref_rule_count: usize,
    matched_path_rule_count: usize,
    workspace_id: Option<Uuid>,
    workspace_scope_present: bool,
    session_ref_present: bool,
    correlation_present: bool,
    idempotency_present: bool,
    change_request_id: Option<Uuid>,
}

impl RoutePolicyDecisionDetails {
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
        if let Some(reason) = deny_reason {
            details.insert("reason".to_string(), reason.code().to_string());
        }
        if let Some(target_ref) = &self.target_ref {
            details.insert("target_ref".to_string(), target_ref.clone());
        }
        details.insert(
            "changed_path_count".to_string(),
            self.changed_path_count.to_string(),
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
    let ref_rules = review_store.list_protected_ref_rules().await?;
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

    let path_rules = review_store.list_protected_path_rules().await?;
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
        target_ref: request.target_ref.clone(),
        changed_path_count: request.changed_paths.len(),
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
    async fn protected_path_policy_matches_target_ref_and_boundary_prefix() {
        let review = Arc::new(InMemoryReviewStore::new());
        review
            .create_protected_path_rule("/legal", Some(MAIN_REF), 1, ROOT_UID)
            .await
            .unwrap();

        let blocked =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
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
                .with_target_ref(MAIN_REF)
                .with_changed_paths(["/legalese/a.txt"]);
        let allowed = evaluate_route_policy(review.as_ref(), allowed)
            .await
            .unwrap();
        assert!(allowed.decision.is_allowed());
        assert_eq!(allowed.decision.details().matched_path_rule_count(), 0);

        let other_ref =
            RoutePolicyRequest::from_session(RoutePolicyAction::FsWrite, &Session::root())
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
        assert!(details.len() <= 12);
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
                .with_target_ref(MAIN_REF);
        let create = evaluate_route_policy(review.as_ref(), create)
            .await
            .unwrap();
        assert!(create.decision.is_allowed());
        assert_eq!(create.decision.details().matched_ref_rule_count(), 1);

        let reject =
            RoutePolicyRequest::from_session(RoutePolicyAction::ReviewReject, &Session::root())
                .with_target_ref(MAIN_REF);
        let reject = evaluate_route_policy(review.as_ref(), reject)
            .await
            .unwrap();
        assert!(reject.decision.is_allowed());
    }
}
