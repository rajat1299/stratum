use super::perms::{Access, check_permission};
use super::{Gid, ROOT_UID, Uid};
use crate::error::VfsError;
use crate::fs::inode::Inode;

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
    /// When set, the session acts on behalf of this user.
    /// All permission checks require BOTH the principal AND
    /// the delegate to have access (intersection / least-privilege).
    pub delegate: Option<DelegateContext>,
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
            delegate: None,
        }
    }

    pub fn with_scope(mut self, scope: SessionScope) -> Self {
        self.scope = Some(scope);
        self
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
        if let Some(ref delegate) = self.delegate {
            if !check_permission(inode, delegate.uid, &delegate.groups, access) {
                return false;
            }
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
        if let Some(ref delegate) = self.delegate {
            if !check_bits(
                delegate.uid,
                &delegate.groups,
                mode,
                file_uid,
                file_gid,
                access,
            ) {
                return false;
            }
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
