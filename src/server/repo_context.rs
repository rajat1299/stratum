use axum::http::HeaderMap;

use crate::auth::session::SessionMount;
use crate::backend::RepoId;
use crate::error::VfsError;

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

pub(crate) fn invalid_repo_header() -> VfsError {
    VfsError::InvalidArgs {
        message: "invalid x-stratum-repo header".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::{SessionMount, SessionMountIdentity};
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
}
