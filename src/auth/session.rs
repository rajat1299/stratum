use super::perms::{Access, check_permission};
use super::{Gid, ROOT_UID, Uid};
use crate::backend::RepoId;
use crate::error::VfsError;
use crate::fs::inode::Inode;
use uuid::Uuid;

/// Context for the user an agent is acting on behalf of.
#[derive(Debug, Clone)]
pub struct DelegateContext {
    pub uid: Uid,
    pub gid: Gid,
    pub groups: Vec<Gid>,
    pub username: String,
}

#[derive(Debug, Clone)]
pub struct Session {
    pub uid: Uid,
    pub gid: Gid,
    pub groups: Vec<Gid>,
    pub username: String,
    pub scope: Option<SessionScope>,
    mount: Option<SessionMount>,
    /// When set, the session acts on behalf of this user.
    /// All permission checks require BOTH the principal AND
    /// the delegate to have access (intersection / least-privilege).
    pub delegate: Option<DelegateContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMountIdentity {
    workspace_id: Uuid,
    root_path: String,
    base_ref: String,
    session_ref: Option<String>,
    repo_id: Option<String>,
    principal_uid: Option<Uid>,
    token_id: Option<Uuid>,
    token_version: Option<u64>,
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
}

impl SessionMountIdentity {
    pub fn new(workspace_id: Uuid, root_path: impl Into<String>) -> Self {
        Self {
            workspace_id,
            root_path: root_path.into(),
            base_ref: "main".to_string(),
            session_ref: None,
            repo_id: None,
            principal_uid: None,
            token_id: None,
            token_version: None,
            read_prefixes: Vec::new(),
            write_prefixes: Vec::new(),
        }
    }

    pub fn with_refs(mut self, base_ref: impl Into<String>, session_ref: Option<String>) -> Self {
        self.base_ref = base_ref.into();
        self.session_ref = session_ref;
        self
    }

    pub fn with_repo_id(mut self, repo_id: Option<String>) -> Self {
        self.repo_id = repo_id;
        self
    }

    pub fn with_principal_uid(mut self, principal_uid: Uid) -> Self {
        self.principal_uid = Some(principal_uid);
        self
    }

    pub fn with_token(mut self, token_id: Uuid, token_version: u64) -> Self {
        self.token_id = Some(token_id);
        self.token_version = Some(token_version);
        self
    }

    pub fn with_prefixes(
        mut self,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> Self {
        self.read_prefixes = read_prefixes;
        self.write_prefixes = write_prefixes;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMount {
    workspace_id: Uuid,
    root_path: String,
    base_ref: String,
    session_ref: Option<String>,
    repo_id: Option<String>,
    principal_uid: Option<Uid>,
    token_id: Option<Uuid>,
    token_version: Option<u64>,
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
}

impl SessionMount {
    pub fn new(workspace_id: Uuid, root_path: impl AsRef<str>) -> Result<Self, VfsError> {
        Self::with_refs(workspace_id, root_path, "main", None)
    }

    pub fn with_refs(
        workspace_id: Uuid,
        root_path: impl AsRef<str>,
        base_ref: impl AsRef<str>,
        session_ref: Option<&str>,
    ) -> Result<Self, VfsError> {
        Self::with_identity(
            SessionMountIdentity::new(workspace_id, root_path.as_ref())
                .with_refs(base_ref.as_ref(), session_ref.map(str::to_string)),
        )
    }

    pub fn with_identity(identity: SessionMountIdentity) -> Result<Self, VfsError> {
        let root_path = normalize_absolute_path(&identity.root_path)?;
        let read_prefixes = normalize_prefixes(identity.read_prefixes)?;
        let write_prefixes = normalize_prefixes(identity.write_prefixes)?;
        for prefix in read_prefixes.iter().chain(write_prefixes.iter()) {
            if !path_matches_prefix(prefix, &root_path) {
                return Err(VfsError::PermissionDenied {
                    path: prefix.clone(),
                });
            }
        }
        Ok(Self {
            workspace_id: identity.workspace_id,
            root_path,
            base_ref: identity.base_ref,
            session_ref: identity.session_ref,
            repo_id: identity.repo_id,
            principal_uid: identity.principal_uid,
            token_id: identity.token_id,
            token_version: identity.token_version,
            read_prefixes,
            write_prefixes,
        })
    }

    pub fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    pub fn root_path(&self) -> &str {
        &self.root_path
    }

    pub fn base_ref(&self) -> &str {
        &self.base_ref
    }

    pub fn session_ref(&self) -> Option<&str> {
        self.session_ref.as_deref()
    }

    pub fn repo_id(&self) -> Option<&str> {
        self.repo_id.as_deref()
    }

    pub(crate) fn required_repo_id(&self) -> Result<RepoId, VfsError> {
        let Some(repo_id) = self.repo_id.as_deref() else {
            return Err(VfsError::InvalidArgs {
                message: "workspace repo id is required".to_string(),
            });
        };
        RepoId::new(repo_id)
    }

    pub fn principal_uid(&self) -> Option<Uid> {
        self.principal_uid
    }

    pub fn token_id(&self) -> Option<Uuid> {
        self.token_id
    }

    pub fn token_version(&self) -> Option<u64> {
        self.token_version
    }

    pub fn read_prefixes(&self) -> &[String] {
        &self.read_prefixes
    }

    pub fn write_prefixes(&self) -> &[String] {
        &self.write_prefixes
    }

    fn resolve_backing_path(&self, path: &str) -> Result<String, VfsError> {
        let relative_path = normalize_workspace_relative_path(path);
        Ok(join_mount_path(&self.root_path, &relative_path))
    }

    fn project_backing_path(&self, path: &str) -> Option<String> {
        let normalized = normalize_absolute_path(path).ok()?;
        if normalized == self.root_path {
            return Some("/".to_string());
        }
        let prefix = format!("{}/", self.root_path.trim_end_matches('/'));
        let rest = normalized.strip_prefix(&prefix)?;
        Some(format!("/{rest}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionScope {
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
}

impl SessionScope {
    pub fn new(
        read_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
        write_prefixes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, VfsError> {
        let read_prefixes = normalize_prefixes(read_prefixes)?;
        let write_prefixes = normalize_prefixes(write_prefixes)?;

        Ok(Self {
            read_prefixes,
            write_prefixes,
        })
    }

    fn allows(&self, path: &str, access: Access) -> bool {
        let Ok(path) = normalize_absolute_path(path) else {
            return false;
        };
        let prefixes = match access {
            Access::Read => &self.read_prefixes,
            Access::Write => &self.write_prefixes,
            Access::Execute => {
                return self.allows(&path, Access::Read) || self.allows(&path, Access::Write);
            }
        };

        prefixes
            .iter()
            .any(|prefix| path_matches_prefix(&path, prefix))
    }
}

impl Session {
    pub fn new(uid: Uid, gid: Gid, groups: Vec<Gid>, username: String) -> Self {
        Session {
            uid,
            gid,
            groups,
            username,
            scope: None,
            mount: None,
            delegate: None,
        }
    }

    pub fn root() -> Self {
        Session {
            uid: ROOT_UID,
            gid: 0,
            groups: vec![0, 1],
            username: "root".to_string(),
            scope: None,
            mount: None,
            delegate: None,
        }
    }

    pub fn from_workspace_principal(
        principal: crate::workspace::WorkspacePrincipalRecord,
    ) -> Result<Self, VfsError> {
        if !principal.active {
            return Err(VfsError::PermissionDenied {
                path: format!("principal:{}", principal.uid),
            });
        }
        Ok(Self::new(
            principal.uid,
            principal.gid,
            principal.groups,
            principal.username,
        ))
    }

    pub fn with_scope(mut self, scope: SessionScope) -> Self {
        self.scope = Some(scope);
        self
    }

    pub fn with_mount(
        mut self,
        workspace_id: Uuid,
        root_path: impl AsRef<str>,
    ) -> Result<Self, VfsError> {
        self.mount = Some(SessionMount::new(workspace_id, root_path)?);
        Ok(self)
    }

    pub fn with_workspace_mount(
        mut self,
        workspace_id: Uuid,
        root_path: impl AsRef<str>,
        base_ref: impl AsRef<str>,
        session_ref: Option<&str>,
    ) -> Result<Self, VfsError> {
        self.mount = Some(SessionMount::with_refs(
            workspace_id,
            root_path,
            base_ref,
            session_ref,
        )?);
        Ok(self)
    }

    pub fn with_workspace_mount_identity(
        mut self,
        identity: SessionMountIdentity,
    ) -> Result<Self, VfsError> {
        self.mount = Some(SessionMount::with_identity(identity)?);
        Ok(self)
    }

    pub fn mount(&self) -> Option<&SessionMount> {
        self.mount.as_ref()
    }

    pub fn resolve_mounted_path(&self, path: &str) -> Result<String, VfsError> {
        let Some(mount) = &self.mount else {
            return normalize_absolute_path(path);
        };

        mount.resolve_backing_path(path)
    }

    pub fn project_mounted_path(&self, path: &str) -> String {
        let normalized_path = normalize_absolute_path(path).unwrap_or_else(|_| path.to_string());
        let Some(mount) = &self.mount else {
            return normalized_path;
        };

        mount
            .project_backing_path(&normalized_path)
            .unwrap_or(normalized_path)
    }

    pub fn project_mounted_error_path(&self, path: &str) -> String {
        let Ok(normalized_path) = normalize_absolute_path(path) else {
            return path.to_string();
        };
        let Some(mount) = &self.mount else {
            return normalized_path;
        };

        if let Some(projected) = mount.project_backing_path(&normalized_path) {
            return projected;
        }

        "<outside workspace>".to_string()
    }

    pub fn is_root(&self) -> bool {
        self.uid == ROOT_UID
    }

    pub fn is_path_allowed(&self, path: &str, access: Access) -> bool {
        match &self.scope {
            Some(scope) => scope.allows(path, access),
            None => true,
        }
    }

    /// Check permission with delegation intersection.
    /// Returns true only if both the principal AND the delegate (if any) have access.
    pub fn has_permission(&self, inode: &Inode, access: Access) -> bool {
        if !check_permission(inode, self.uid, &self.groups, access) {
            return false;
        }
        if let Some(ref delegate) = self.delegate
            && !check_permission(inode, delegate.uid, &delegate.groups, access)
        {
            return false;
        }
        true
    }

    /// Check permission using raw mode/uid/gid bits (for LsEntry filtering).
    /// Respects delegation intersection.
    pub fn has_permission_bits(
        &self,
        mode: u16,
        file_uid: Uid,
        file_gid: Gid,
        access: Access,
    ) -> bool {
        if !check_bits(self.uid, &self.groups, mode, file_uid, file_gid, access) {
            return false;
        }
        if let Some(ref delegate) = self.delegate
            && !check_bits(
                delegate.uid,
                &delegate.groups,
                mode,
                file_uid,
                file_gid,
                access,
            )
        {
            return false;
        }
        true
    }

    /// The effective uid for file ownership — if delegating, use the delegate's uid.
    pub fn effective_uid(&self) -> Uid {
        match &self.delegate {
            Some(d) => d.uid,
            None => self.uid,
        }
    }

    /// The effective gid for file ownership — if delegating, use the delegate's gid.
    pub fn effective_gid(&self) -> Gid {
        match &self.delegate {
            Some(d) => d.gid,
            None => self.gid,
        }
    }

    /// Whether either the principal or (if delegating) the delegate is root.
    /// For intersection: both must pass, so root status only helps if the OTHER side passes.
    /// This is used for checks like "is this session effectively root?" — answer: only if
    /// there's no delegate constraining it.
    pub fn is_effectively_root(&self) -> bool {
        if self.uid != ROOT_UID {
            return false;
        }
        match &self.delegate {
            Some(d) => d.uid == ROOT_UID,
            None => true,
        }
    }

    /// Whether the principal (ignoring delegation) is the owner of a file.
    /// When delegating, checks if the delegate is the owner.
    pub fn is_effective_owner(&self, file_uid: Uid) -> bool {
        match &self.delegate {
            Some(d) => {
                // Both must be owner or root
                let principal_ok = self.uid == ROOT_UID || self.uid == file_uid;
                let delegate_ok = d.uid == ROOT_UID || d.uid == file_uid;
                principal_ok && delegate_ok
            }
            None => self.uid == ROOT_UID || self.uid == file_uid,
        }
    }
}

fn normalize_prefixes(
    prefixes: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<String>, VfsError> {
    let mut normalized = Vec::new();
    for prefix in prefixes {
        normalized.push(normalize_absolute_path(prefix.as_ref())?);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn normalize_absolute_path(path: &str) -> Result<String, VfsError> {
    if !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
        });
    }

    let mut components = Vec::new();
    for component in path.split('/').filter(|component| !component.is_empty()) {
        match component {
            "." => {}
            ".." => {
                components.pop();
            }
            name => components.push(name),
        }
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

fn normalize_workspace_relative_path(path: &str) -> String {
    let mut components = Vec::new();
    for component in path.split('/').filter(|component| !component.is_empty()) {
        match component {
            "." => {}
            ".." => {
                components.pop();
            }
            name => components.push(name),
        }
    }

    components.join("/")
}

fn join_mount_path(root_path: &str, relative_path: &str) -> String {
    if relative_path.is_empty() {
        root_path.to_string()
    } else if root_path == "/" {
        format!("/{relative_path}")
    } else {
        format!("{root_path}/{relative_path}")
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Raw bit-level permission check for a single principal.
fn check_bits(
    uid: Uid,
    groups: &[Gid],
    mode: u16,
    file_uid: Uid,
    file_gid: Gid,
    access: Access,
) -> bool {
    if uid == ROOT_UID {
        return true;
    }
    let bit = match access {
        Access::Read => 4,
        Access::Write => 2,
        Access::Execute => 1,
    };
    if uid == file_uid {
        return (mode >> 6) & bit != 0;
    }
    if groups.contains(&file_gid) {
        return (mode >> 3) & bit != 0;
    }
    mode & bit != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn mounted_session() -> Session {
        Session::new(1000, 1000, vec![1000], "agent".to_string())
            .with_mount(Uuid::nil(), "/workspace/root/./")
            .unwrap()
    }

    fn mounted_session_with_refs() -> Session {
        Session::new(1000, 1000, vec![1000], "agent".to_string())
            .with_workspace_mount(
                Uuid::nil(),
                "/workspace/root/./",
                "main",
                Some("agent/legal-bot/session-123"),
            )
            .unwrap()
    }

    #[test]
    fn mounted_sessions_expose_hash_safe_workspace_identity() {
        let workspace_id = Uuid::new_v4();
        let token_id = Uuid::new_v4();
        let identity = SessionMountIdentity::new(workspace_id, "/workspace/root/./")
            .with_refs("main", Some("agent/legal-bot/session-123".to_string()))
            .with_repo_id(Some("repo_demo".to_string()))
            .with_principal_uid(42)
            .with_token(token_id, 3)
            .with_prefixes(
                vec!["/workspace/root/read".to_string()],
                vec!["/workspace/root/write".to_string()],
            );
        let session = Session::new(1000, 1000, vec![1000], "agent".to_string())
            .with_workspace_mount_identity(identity)
            .unwrap();
        let mount = session.mount().unwrap();

        assert_eq!(mount.workspace_id(), workspace_id);
        assert_eq!(mount.root_path(), "/workspace/root");
        assert_eq!(mount.base_ref(), "main");
        assert_eq!(mount.session_ref(), Some("agent/legal-bot/session-123"));
        assert_eq!(mount.repo_id(), Some("repo_demo"));
        assert_eq!(mount.principal_uid(), Some(42));
        assert_eq!(mount.token_id(), Some(token_id));
        assert_eq!(mount.token_version(), Some(3));
        assert_eq!(mount.read_prefixes(), &["/workspace/root/read".to_string()]);
        assert_eq!(
            mount.write_prefixes(),
            &["/workspace/root/write".to_string()]
        );
        assert_eq!(
            session.project_mounted_error_path("/workspace/root/private/a.md"),
            "/private/a.md"
        );
        assert_eq!(
            session.project_mounted_error_path("/srv/backing/private/a.md"),
            "<outside workspace>"
        );
    }

    #[test]
    fn mounted_identity_rejects_prefixes_outside_mount_root() {
        let identity = SessionMountIdentity::new(Uuid::new_v4(), "/workspace/root")
            .with_refs("main", None)
            .with_repo_id(Some("repo_demo".to_string()))
            .with_principal_uid(42)
            .with_token(Uuid::new_v4(), 1)
            .with_prefixes(
                vec!["/workspace/root/read".to_string()],
                vec!["/workspace/root/../other/write".to_string()],
            );
        let err = Session::new(1000, 1000, vec![1000], "agent".to_string())
            .with_workspace_mount_identity(identity)
            .expect_err("out-of-root write prefix should fail");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[test]
    fn workspace_principal_creates_permission_checkable_session() {
        let principal = crate::workspace::WorkspacePrincipalRecord {
            uid: 42,
            username: "durable-agent".to_string(),
            gid: 7,
            groups: vec![7, 8],
            kind: crate::workspace::WorkspacePrincipalKind::Agent,
            active: true,
        };

        let session = Session::from_workspace_principal(principal).unwrap();
        let mut inode = Inode::new_file(1, 42, 7);
        inode.mode = 0o640;

        assert_eq!(session.uid, 42);
        assert_eq!(session.gid, 7);
        assert_eq!(session.groups, vec![7, 8]);
        assert_eq!(session.username, "durable-agent");
        assert!(session.mount().is_none());
        assert!(session.scope.is_none());
        assert!(session.delegate.is_none());
        assert!(session.has_permission(&inode, Access::Read));
        assert!(session.has_permission(&inode, Access::Write));
        assert!(!session.has_permission(&inode, Access::Execute));
    }

    #[test]
    fn workspace_principal_rejects_inactive_principal() {
        let principal = crate::workspace::WorkspacePrincipalRecord {
            uid: 42,
            username: "durable-agent".to_string(),
            gid: 7,
            groups: vec![7, 8],
            kind: crate::workspace::WorkspacePrincipalKind::Agent,
            active: false,
        };

        let err = Session::from_workspace_principal(principal)
            .expect_err("inactive principal should fail closed");

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[test]
    fn mounted_session_exposes_ref_ownership() {
        let session = mounted_session_with_refs();
        let mount = session.mount().unwrap();

        assert_eq!(mount.base_ref(), "main");
        assert_eq!(mount.session_ref(), Some("agent/legal-bot/session-123"));
    }

    #[test]
    fn mounted_resolves_relative_and_absolute_inputs_under_root() {
        let session = mounted_session();

        assert_eq!(
            session.resolve_mounted_path("read/a.md").unwrap(),
            "/workspace/root/read/a.md"
        );
        assert_eq!(
            session.resolve_mounted_path("/read/a.md").unwrap(),
            "/workspace/root/read/a.md"
        );
        assert_eq!(
            session.resolve_mounted_path("./read/./a.md").unwrap(),
            "/workspace/root/read/a.md"
        );
    }

    #[test]
    fn mounted_clamps_parent_traversal_to_root() {
        let session = mounted_session();

        assert_eq!(
            session.resolve_mounted_path("../outside").unwrap(),
            "/workspace/root/outside"
        );
        assert_eq!(
            session.resolve_mounted_path("/../outside").unwrap(),
            "/workspace/root/outside"
        );
        assert_eq!(
            session.resolve_mounted_path("../../").unwrap(),
            "/workspace/root"
        );
    }

    #[test]
    fn mounted_projects_backing_paths_to_workspace_relative_output() {
        let session = mounted_session();

        assert_eq!(session.project_mounted_path("/workspace/root"), "/");
        assert_eq!(
            session.project_mounted_path("/workspace/root/read/a.md"),
            "/read/a.md"
        );
        assert_eq!(
            session.project_mounted_path("/workspace/root/./read/../read/a.md"),
            "/read/a.md"
        );
    }

    #[test]
    fn mounted_projection_leaves_paths_outside_root_normalized() {
        let session = mounted_session();

        assert_eq!(
            session.project_mounted_path("/workspace/rooted/a.md"),
            "/workspace/rooted/a.md"
        );
        assert_eq!(
            session.project_mounted_path("relative/a.md"),
            "relative/a.md"
        );
    }

    #[test]
    fn mounted_error_projection_redacts_paths_outside_root() {
        let session = mounted_session();

        assert_eq!(
            session.project_mounted_error_path("/workspace/root/read/a.md"),
            "/read/a.md"
        );
        assert_eq!(
            session.project_mounted_error_path("/workspace/rooted/a.md"),
            "<outside workspace>"
        );
        assert_eq!(
            session.project_mounted_error_path("/outside/a.md"),
            "<outside workspace>"
        );
        assert_eq!(
            session.project_mounted_error_path("admin operation"),
            "admin operation"
        );
    }

    #[test]
    fn mounted_mount_root_is_normalized() {
        let session = mounted_session();

        assert_eq!(
            session.resolve_mounted_path("a.md").unwrap(),
            "/workspace/root/a.md"
        );
    }

    #[test]
    fn mounted_unmounted_sessions_keep_existing_path_behavior() {
        let session = Session::new(1000, 1000, vec![1000], "agent".to_string());

        assert_eq!(
            session.resolve_mounted_path("/read/../a.md").unwrap(),
            "/a.md"
        );
        assert!(matches!(
            session.resolve_mounted_path("a.md"),
            Err(VfsError::InvalidPath { .. })
        ));
        assert_eq!(session.project_mounted_path("/read/../a.md"), "/a.md");
        assert_eq!(session.project_mounted_path("a.md"), "a.md");
    }
}
