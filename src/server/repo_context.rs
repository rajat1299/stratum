use axum::http::HeaderMap;
use std::collections::BTreeSet;
use std::sync::RwLock;

use crate::auth::session::SessionMount;
use crate::backend::{OrgId, RepoId};
use crate::error::VfsError;

pub(crate) const STRATUM_ORG_HEADER: &str = "x-stratum-org";
pub(crate) const STRATUM_REPO_HEADER: &str = "x-stratum-repo";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestRepoContextSource {
    LocalSingleton,
    WorkspaceMount,
    AdminHeader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestRepoContext {
    repo_id: RepoId,
    source: RequestRepoContextSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestTenantContextSource {
    LocalSingleton,
    WorkspaceMount,
    AdminHeader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestTenantContext {
    org_id: OrgId,
    source: RequestTenantContextSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestTenantRepoContext {
    tenant: RequestTenantContext,
    repo: RequestRepoContext,
}

pub(crate) trait TenantRepoResolver: Send + Sync {
    fn repo_belongs_to_org(&self, org_id: &OrgId, repo_id: &RepoId) -> Result<bool, VfsError>;
}

#[derive(Debug, Default)]
pub(crate) struct InMemoryTenantRepoResolver {
    bindings: RwLock<BTreeSet<(OrgId, RepoId)>>,
}

impl InMemoryTenantRepoResolver {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn bind_repo(&self, org_id: OrgId, repo_id: RepoId) {
        self.bindings
            .write()
            .expect("tenant repo resolver lock poisoned")
            .insert((org_id, repo_id));
    }
}

impl TenantRepoResolver for InMemoryTenantRepoResolver {
    fn repo_belongs_to_org(&self, org_id: &OrgId, repo_id: &RepoId) -> Result<bool, VfsError> {
        Ok(self
            .bindings
            .read()
            .expect("tenant repo resolver lock poisoned")
            .contains(&(org_id.clone(), repo_id.clone())))
    }
}

impl RequestRepoContext {
    pub(crate) fn local_singleton() -> Self {
        Self {
            repo_id: RepoId::local(),
            source: RequestRepoContextSource::LocalSingleton,
        }
    }

    pub(crate) fn resolve(
        headers: &HeaderMap,
        mount: Option<&SessionMount>,
        allow_local_singleton: bool,
    ) -> Result<Self, VfsError> {
        let workspace_repo = match mount {
            Some(mount) if mount.repo_id().is_some() => {
                Some(mount.required_repo_id().map_err(|_| VfsError::AuthError {
                    message: "invalid workspace repo id".to_string(),
                })?)
            }
            Some(_) | None => None,
        };
        let header_repo = parse_repo_header(headers)?;

        match (workspace_repo, header_repo) {
            (Some(workspace_repo), Some(header_repo)) if workspace_repo != header_repo => {
                Err(VfsError::PermissionDenied {
                    path: "repo context".to_string(),
                })
            }
            (Some(repo_id), _) => Ok(Self {
                repo_id,
                source: RequestRepoContextSource::WorkspaceMount,
            }),
            (None, Some(repo_id)) => Ok(Self {
                repo_id,
                source: RequestRepoContextSource::AdminHeader,
            }),
            (None, None) if allow_local_singleton => Ok(Self::local_singleton()),
            (None, None) => Err(VfsError::InvalidArgs {
                message: "repo id is required".to_string(),
            }),
        }
    }

    pub(crate) fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> RequestRepoContextSource {
        self.source
    }

    pub(crate) fn is_local_singleton(&self) -> bool {
        self.repo_id == RepoId::local()
    }
}

impl RequestTenantContext {
    pub(crate) fn local_singleton() -> Self {
        Self {
            org_id: OrgId::default_org(),
            source: RequestTenantContextSource::LocalSingleton,
        }
    }

    pub(crate) fn resolve(
        headers: &HeaderMap,
        workspace_org: Option<&OrgId>,
        allow_local_singleton: bool,
    ) -> Result<Self, VfsError> {
        let header_org = parse_org_header(headers)?;

        match (workspace_org, header_org) {
            (Some(workspace_org), Some(header_org)) if workspace_org != &header_org => {
                Err(VfsError::PermissionDenied {
                    path: "tenant context".to_string(),
                })
            }
            (Some(org_id), _) => Ok(Self {
                org_id: org_id.clone(),
                source: RequestTenantContextSource::WorkspaceMount,
            }),
            (None, Some(org_id)) => Ok(Self {
                org_id,
                source: RequestTenantContextSource::AdminHeader,
            }),
            (None, None) if allow_local_singleton => Ok(Self::local_singleton()),
            (None, None) => Err(VfsError::InvalidArgs {
                message: "org id is required".to_string(),
            }),
        }
    }

    pub(crate) fn org_id(&self) -> &OrgId {
        &self.org_id
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> RequestTenantContextSource {
        self.source
    }
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "staged for Slice 15 route integration")
)]
impl RequestTenantRepoContext {
    pub(crate) fn local_singleton() -> Self {
        Self {
            tenant: RequestTenantContext::local_singleton(),
            repo: RequestRepoContext::local_singleton(),
        }
    }

    pub(crate) fn resolve(
        headers: &HeaderMap,
        mount: Option<&SessionMount>,
        workspace_org: Option<&OrgId>,
        allow_local_singleton: bool,
        resolver: Option<&dyn TenantRepoResolver>,
    ) -> Result<Self, VfsError> {
        let tenant = RequestTenantContext::resolve(headers, workspace_org, allow_local_singleton)?;
        let repo = RequestRepoContext::resolve(headers, mount, allow_local_singleton)?;

        if !allow_local_singleton
            && (!tenant.source.is_local_singleton() || !repo.source.is_local_singleton())
        {
            let Some(resolver) = resolver else {
                return Err(VfsError::PermissionDenied {
                    path: "repo context".to_string(),
                });
            };
            if !resolver.repo_belongs_to_org(tenant.org_id(), repo.repo_id())? {
                return Err(VfsError::PermissionDenied {
                    path: "repo context".to_string(),
                });
            }
        }

        Ok(Self { tenant, repo })
    }

    pub(crate) fn tenant(&self) -> &RequestTenantContext {
        &self.tenant
    }

    pub(crate) fn repo(&self) -> &RequestRepoContext {
        &self.repo
    }

    pub(crate) fn org_id(&self) -> &OrgId {
        self.tenant.org_id()
    }

    pub(crate) fn repo_id(&self) -> &RepoId {
        self.repo.repo_id()
    }

    pub(crate) fn is_local_singleton(&self) -> bool {
        self.tenant.org_id() == &OrgId::default_org() && self.repo.is_local_singleton()
    }
}

impl RequestTenantContextSource {
    fn is_local_singleton(self) -> bool {
        matches!(self, Self::LocalSingleton)
    }
}

impl RequestRepoContextSource {
    fn is_local_singleton(self) -> bool {
        matches!(self, Self::LocalSingleton)
    }
}

pub(crate) fn parse_org_header(headers: &HeaderMap) -> Result<Option<OrgId>, VfsError> {
    let mut values = headers.get_all(STRATUM_ORG_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid_org_header());
    }
    let value = value.to_str().map_err(|_| invalid_org_header())?;
    OrgId::new(value)
        .map(Some)
        .map_err(|_| invalid_org_header())
}

pub(crate) fn parse_repo_header(headers: &HeaderMap) -> Result<Option<RepoId>, VfsError> {
    let mut values = headers.get_all(STRATUM_REPO_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid_repo_header());
    }
    let value = value.to_str().map_err(|_| invalid_repo_header())?;
    RepoId::new(value)
        .map(Some)
        .map_err(|_| invalid_repo_header())
}

pub(crate) fn invalid_org_header() -> VfsError {
    VfsError::InvalidArgs {
        message: "invalid x-stratum-org header".to_string(),
    }
}

pub(crate) fn invalid_repo_header() -> VfsError {
    VfsError::InvalidArgs {
        message: "invalid x-stratum-repo header".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::{SessionMount, SessionMountIdentity};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    fn mount_with_repo(repo_id: &str) -> SessionMount {
        SessionMount::with_identity(
            SessionMountIdentity::new(Uuid::new_v4(), "/workspace")
                .with_repo_id(Some(repo_id.to_string())),
        )
        .unwrap()
    }

    #[test]
    fn missing_repo_fails_when_local_fallback_disallowed() {
        let err = RequestRepoContext::resolve(&HeaderMap::new(), None, false)
            .expect_err("hosted requests require explicit repo context");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[test]
    fn local_fallback_returns_local_repo_when_allowed() {
        let context = RequestRepoContext::resolve(&HeaderMap::new(), None, true).unwrap();

        assert_eq!(context.repo_id(), &RepoId::local());
        assert_eq!(context.source(), RequestRepoContextSource::LocalSingleton);
        assert!(context.is_local_singleton());
    }

    #[test]
    fn invalid_header_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_REPO_HEADER, "not valid".parse().unwrap());

        let err = RequestRepoContext::resolve(&headers, None, true)
            .expect_err("invalid repo header must fail closed");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[test]
    fn duplicate_headers_are_rejected_with_fixed_message() {
        let mut headers = HeaderMap::new();
        headers.append(STRATUM_REPO_HEADER, "repo_a".parse().unwrap());
        headers.append(STRATUM_REPO_HEADER, "repo_b".parse().unwrap());

        let err = RequestRepoContext::resolve(&headers, None, true)
            .expect_err("duplicate repo headers must fail closed");

        let VfsError::InvalidArgs { message } = err else {
            panic!("duplicate repo header should return InvalidArgs");
        };
        assert_eq!(message, "invalid x-stratum-repo header");
        assert!(!message.contains("repo_a"));
        assert!(!message.contains("repo_b"));
    }

    #[test]
    fn mount_header_mismatch_is_rejected() {
        let mount = mount_with_repo("repo_a");
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_REPO_HEADER, "repo_b".parse().unwrap());

        let err = RequestRepoContext::resolve(&headers, Some(&mount), false)
            .expect_err("conflicting repo identities must fail closed");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[test]
    fn matching_mount_header_is_accepted() {
        let mount = mount_with_repo("repo_a");
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_REPO_HEADER, "repo_a".parse().unwrap());

        let context = RequestRepoContext::resolve(&headers, Some(&mount), false).unwrap();

        assert_eq!(context.repo_id(), &RepoId::new("repo_a").unwrap());
        assert_eq!(context.source(), RequestRepoContextSource::WorkspaceMount);
    }

    #[test]
    fn hosted_request_requires_org_before_repo_lookup() {
        let resolver = CountingTenantRepoResolver::default();
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_REPO_HEADER, "repo_a".parse().unwrap());

        let err = RequestTenantRepoContext::resolve(&headers, None, None, false, Some(&resolver))
            .expect_err("hosted requests require org before repo resolution");

        let VfsError::InvalidArgs { message } = err else {
            panic!("missing org should return InvalidArgs");
        };
        assert_eq!(message, "org id is required");
        assert_eq!(resolver.lookups(), 0);
    }

    #[test]
    fn local_singleton_allows_missing_org_and_repo_when_enabled() {
        let context = RequestTenantRepoContext::resolve(&HeaderMap::new(), None, None, true, None)
            .expect("local singleton fallback should include default org and local repo");

        assert_eq!(context.tenant().org_id(), &OrgId::default_org());
        assert_eq!(context.repo().repo_id(), &RepoId::local());
        assert_eq!(
            context.tenant().source(),
            RequestTenantContextSource::LocalSingleton
        );
        assert_eq!(
            context.repo().source(),
            RequestRepoContextSource::LocalSingleton
        );
    }

    #[test]
    fn invalid_and_duplicate_org_headers_are_rejected_with_fixed_message() {
        let mut invalid = HeaderMap::new();
        invalid.insert(STRATUM_ORG_HEADER, "not valid".parse().unwrap());

        let err = RequestTenantRepoContext::resolve(&invalid, None, None, true, None)
            .expect_err("invalid org header must fail closed");
        assert_invalid_org_header(err);

        let mut duplicate = HeaderMap::new();
        duplicate.append(STRATUM_ORG_HEADER, "org_a".parse().unwrap());
        duplicate.append(STRATUM_ORG_HEADER, "org_b".parse().unwrap());

        let err = RequestTenantRepoContext::resolve(&duplicate, None, None, true, None)
            .expect_err("duplicate org headers must fail closed");
        assert_invalid_org_header(err);
    }

    #[test]
    fn workspace_mount_org_and_header_org_mismatch_is_rejected() {
        let mount = mount_with_repo("repo_a");
        let workspace_org = OrgId::new("org_a").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_ORG_HEADER, "org_b".parse().unwrap());
        headers.insert(STRATUM_REPO_HEADER, "repo_a".parse().unwrap());

        let err = RequestTenantRepoContext::resolve(
            &headers,
            Some(&mount),
            Some(&workspace_org),
            false,
            None,
        )
        .expect_err("conflicting org identities must fail closed");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[test]
    fn hosted_repo_resolution_rejects_repo_outside_resolved_org() {
        let resolver = InMemoryTenantRepoResolver::new()
            .with_repo("org_a", "repo_a")
            .with_repo("org_b", "repo_b");
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_ORG_HEADER, "org_a".parse().unwrap());
        headers.insert(STRATUM_REPO_HEADER, "repo_b".parse().unwrap());

        let err = RequestTenantRepoContext::resolve(&headers, None, None, false, Some(&resolver))
            .expect_err("hosted repo resolution must bind repo to resolved org");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[test]
    fn hosted_explicit_org_and_repo_without_resolver_fails_closed() {
        let mut headers = HeaderMap::new();
        headers.insert(STRATUM_ORG_HEADER, "org_a".parse().unwrap());
        headers.insert(STRATUM_REPO_HEADER, "repo_a".parse().unwrap());

        let err = RequestTenantRepoContext::resolve(&headers, None, None, false, None)
            .expect_err("hosted explicit org and repo must require membership validation");

        assert!(matches!(
            err,
            VfsError::PermissionDenied { path } if path == "repo context"
        ));
    }

    fn assert_invalid_org_header(err: VfsError) {
        let VfsError::InvalidArgs { message } = err else {
            panic!("invalid org header should return InvalidArgs");
        };
        assert_eq!(message, "invalid x-stratum-org header");
        assert!(!message.contains("org_a"));
        assert!(!message.contains("org_b"));
    }

    #[derive(Default)]
    struct CountingTenantRepoResolver {
        lookups: AtomicUsize,
    }

    impl CountingTenantRepoResolver {
        fn lookups(&self) -> usize {
            self.lookups.load(Ordering::SeqCst)
        }
    }

    impl TenantRepoResolver for CountingTenantRepoResolver {
        fn repo_belongs_to_org(
            &self,
            _org_id: &OrgId,
            _repo_id: &RepoId,
        ) -> Result<bool, VfsError> {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }
    }

    struct InMemoryTenantRepoResolver {
        bindings: BTreeSet<(OrgId, RepoId)>,
    }

    impl InMemoryTenantRepoResolver {
        fn new() -> Self {
            Self {
                bindings: BTreeSet::new(),
            }
        }

        fn with_repo(mut self, org_id: &str, repo_id: &str) -> Self {
            self.bindings
                .insert((OrgId::new(org_id).unwrap(), RepoId::new(repo_id).unwrap()));
            self
        }
    }

    impl TenantRepoResolver for InMemoryTenantRepoResolver {
        fn repo_belongs_to_org(&self, org_id: &OrgId, repo_id: &RepoId) -> Result<bool, VfsError> {
            Ok(self.bindings.contains(&(org_id.clone(), repo_id.clone())))
        }
    }
}
