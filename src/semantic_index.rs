use uuid::Uuid;

use crate::backend::RepoId;
use crate::error::VfsError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticIndexScopeOwner {
    Repo { repo_id: RepoId },
    Workspace { repo_id: RepoId, workspace_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticIndexScope {
    owner: SemanticIndexScopeOwner,
    include_prefixes: Vec<String>,
    exclude_prefixes: Vec<String>,
}

impl SemanticIndexScope {
    pub fn for_repo(
        repo_id: RepoId,
        include_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
        exclude_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, VfsError> {
        Self::new(
            SemanticIndexScopeOwner::Repo { repo_id },
            include_prefixes,
            exclude_prefixes,
        )
    }

    pub fn for_workspace(
        repo_id: RepoId,
        workspace_id: Uuid,
        include_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
        exclude_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, VfsError> {
        Self::new(
            SemanticIndexScopeOwner::Workspace {
                repo_id,
                workspace_id,
            },
            include_prefixes,
            exclude_prefixes,
        )
    }

    pub fn owner(&self) -> &SemanticIndexScopeOwner {
        &self.owner
    }

    pub fn include_prefixes(&self) -> &[String] {
        &self.include_prefixes
    }

    pub fn exclude_prefixes(&self) -> &[String] {
        &self.exclude_prefixes
    }

    pub fn permits_path(&self, path: &str) -> Result<bool, VfsError> {
        let path = normalize_scope_path(path)?;
        Ok(self
            .include_prefixes
            .iter()
            .any(|prefix| path_matches_prefix(&path, prefix))
            && !self
                .exclude_prefixes
                .iter()
                .any(|prefix| path_matches_prefix(&path, prefix)))
    }

    fn new(
        owner: SemanticIndexScopeOwner,
        include_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
        exclude_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, VfsError> {
        let include_prefixes = normalize_scope_prefixes(include_prefixes)?;
        if include_prefixes.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: "semantic index scope requires at least one include prefix".to_string(),
            });
        }
        let exclude_prefixes = normalize_scope_prefixes(exclude_prefixes)?;
        Ok(Self {
            owner,
            include_prefixes,
            exclude_prefixes,
        })
    }
}

fn normalize_scope_prefixes(
    prefixes: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<String>, VfsError> {
    let mut normalized = Vec::new();
    for prefix in prefixes {
        normalized.push(normalize_scope_path(prefix.as_ref())?);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn normalize_scope_path(path: &str) -> Result<String, VfsError> {
    if path.is_empty() || !path.starts_with('/') || path.contains('\0') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }

    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_index_scopes_are_deny_by_default() {
        let err = SemanticIndexScope::for_repo(RepoId::local(), Vec::<String>::new(), ["/tmp"])
            .expect_err("empty include list should fail closed");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[test]
    fn include_prefixes_are_normalized_sorted_and_deduped() {
        let scope = SemanticIndexScope::for_repo(
            RepoId::local(),
            ["/docs/../docs", "/docs", "/runbooks/"],
            ["/docs/private/./", "/docs/private"],
        )
        .expect("scope");

        assert_eq!(scope.include_prefixes(), ["/docs", "/runbooks"]);
        assert_eq!(scope.exclude_prefixes(), ["/docs/private"]);
    }

    #[test]
    fn excludes_win_and_prefix_matching_is_boundary_aware() {
        let scope =
            SemanticIndexScope::for_repo(RepoId::local(), ["/"], ["/private", "/docs/drafts"])
                .expect("scope");

        assert!(scope.permits_path("/docs/README.md").unwrap());
        assert!(!scope.permits_path("/private/token.txt").unwrap());
        assert!(scope.permits_path("/private-data/readme.md").unwrap());
        assert!(!scope.permits_path("/docs/drafts/plan.md").unwrap());
        assert!(scope.permits_path("/docs/draftsman/guide.md").unwrap());
    }

    #[test]
    fn workspace_scope_carries_repo_and_workspace_identity_without_changing_path_policy() {
        let repo_id = RepoId::new("repo_semantic_scope").expect("repo id");
        let workspace_id = Uuid::new_v4();
        let scope = SemanticIndexScope::for_workspace(
            repo_id.clone(),
            workspace_id,
            ["/runbooks"],
            ["/runbooks/private"],
        )
        .expect("scope");

        assert_eq!(
            scope.owner(),
            &SemanticIndexScopeOwner::Workspace {
                repo_id,
                workspace_id
            }
        );
        assert!(scope.permits_path("/runbooks/incident.md").unwrap());
        assert!(!scope.permits_path("/runbooks/private/token.md").unwrap());
    }

    #[test]
    fn scope_paths_must_be_absolute_and_content_safe() {
        for path in ["relative", "", "/bad\0path"] {
            let err = SemanticIndexScope::for_repo(RepoId::local(), [path], Vec::<String>::new())
                .expect_err("invalid scope path should fail");
            assert!(matches!(err, VfsError::InvalidPath { .. }));
        }

        let scope = SemanticIndexScope::for_repo(RepoId::local(), ["/docs"], Vec::<String>::new())
            .expect("scope");
        let err = scope
            .permits_path("relative")
            .expect_err("candidate path should be absolute");
        assert!(matches!(err, VfsError::InvalidPath { .. }));
    }
}
