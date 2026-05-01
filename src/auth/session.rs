use super::perms::{Access, check_permission};
use super::{Gid, ROOT_UID, Uid};
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
pub struct SessionMount {
    workspace_id: Uuid,
    root_path: String,
    base_ref: String,
    session_ref: Option<String>,
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
        let root_path = normalize_absolute_path(root_path.as_ref())?;
        Ok(Self {
            workspace_id,
            root_path,
            base_ref: base_ref.as_ref().to_string(),
            session_ref: session_ref.map(str::to_string),
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

    pub fn mount(&self) -> Option<&SessionMount> {
        self.mount.as_ref()
    }

    pub fn resolve_mounted_path(&self, path: &str) -> Result<String, VfsError> {
        let Some(mount) = &self.mount else {
            return normalize_absolute_path(path);
        };

        let relative_path = normalize_workspace_relative_path(path);
        Ok(join_mount_path(&mount.root_path, &relative_path))
    }

    pub fn project_mounted_path(&self, path: &str) -> String {
        let normalized_path = normalize_absolute_path(path).unwrap_or_else(|_| path.to_string());
        let Some(mount) = &self.mount else {
            return normalized_path;
        };

        if normalized_path == mount.root_path {
            return "/".to_string();
        }

        normalized_path
            .strip_prefix(&mount.root_path)
            .and_then(|rest| rest.strip_prefix('/'))
            .map(|rest| format!("/{rest}"))
            .unwrap_or(normalized_path)
    }

    pub fn project_mounted_error_path(&self, path: &str) -> String {
        let Ok(normalized_path) = normalize_absolute_path(path) else {
            return path.to_string();
        };
        let Some(mount) = &self.mount else {
            return normalized_path;
        };

        if path_matches_prefix(&normalized_path, &mount.root_path) {
            return self.project_mounted_path(&normalized_path);
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
