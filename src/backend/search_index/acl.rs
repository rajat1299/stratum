use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::perms::{Access, check_mode_permission};
use crate::auth::session::Session;
use crate::auth::{Gid, ROOT_GID, ROOT_UID, Uid};
use crate::error::VfsError;
use crate::store::ObjectId;
use crate::store::tree::TreeEntry;

use super::{SearchIndexHead, path_matches_prefix, search_index_not_ready_error};

pub const ACL_SNAPSHOT_VERSION_POSIX_TREE_V1: &str = "posix-tree-v1";
const DURABLE_ROOT_MODE: u16 = 0o755;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAclPrincipal {
    pub uid: Uid,
    pub gid: Gid,
    pub groups: Vec<Gid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAclFilter {
    pub principal: SearchAclPrincipal,
    pub delegate: Option<SearchAclPrincipal>,
    pub read_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchAclAccess {
    Read,
    Execute,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchAclRequirement {
    pub path: String,
    pub access: SearchAclAccess,
    pub mode: u16,
    pub uid: Uid,
    pub gid: Gid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchAclSnapshotBody {
    pub version: String,
    pub requirements: Vec<SearchAclRequirement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAclSnapshot {
    pub version: String,
    pub requirements: Vec<SearchAclRequirement>,
    pub hash: String,
}

pub fn search_acl_filter_from_session(session: &Session) -> SearchAclFilter {
    SearchAclFilter {
        principal: SearchAclPrincipal {
            uid: session.uid,
            gid: session.gid,
            groups: session.groups.clone(),
        },
        delegate: session
            .delegate
            .as_ref()
            .map(|delegate| SearchAclPrincipal {
                uid: delegate.uid,
                gid: delegate.gid,
                groups: delegate.groups.clone(),
            }),
        read_prefixes: session.effective_read_prefixes(),
    }
}

pub fn posix_root_execute_requirement() -> SearchAclRequirement {
    SearchAclRequirement {
        path: "/".to_string(),
        access: SearchAclAccess::Execute,
        mode: DURABLE_ROOT_MODE,
        uid: ROOT_UID,
        gid: ROOT_GID,
    }
}

pub fn posix_requirement_from_entry(
    path: String,
    entry: &TreeEntry,
    access: SearchAclAccess,
) -> SearchAclRequirement {
    SearchAclRequirement {
        path,
        access,
        mode: entry.mode,
        uid: entry.uid,
        gid: entry.gid,
    }
}

pub fn build_posix_tree_snapshot(
    head: &SearchIndexHead,
    path: &str,
    object_id: ObjectId,
    ancestors: &[SearchAclRequirement],
    file_read: SearchAclRequirement,
) -> Result<SearchAclSnapshot, VfsError> {
    let mut requirements = ancestors.to_vec();
    requirements.push(file_read);
    let hash = snapshot_hash(head, path, object_id, &requirements)?;
    Ok(SearchAclSnapshot {
        version: ACL_SNAPSHOT_VERSION_POSIX_TREE_V1.to_string(),
        requirements,
        hash,
    })
}

fn snapshot_hash(
    head: &SearchIndexHead,
    path: &str,
    object_id: ObjectId,
    requirements: &[SearchAclRequirement],
) -> Result<String, VfsError> {
    #[derive(Serialize)]
    struct SnapshotHashBody<'a> {
        version: &'a str,
        requirements: &'a [SearchAclRequirement],
    }
    #[derive(Serialize)]
    struct SnapshotHashInput<'a> {
        repo_id: &'a str,
        commit_id: String,
        root_tree_id: String,
        path: &'a str,
        object_id: String,
        snapshot: SnapshotHashBody<'a>,
    }
    let payload = SnapshotHashInput {
        repo_id: head.repo_id.as_str(),
        commit_id: head.commit_id.to_hex(),
        root_tree_id: head.root_tree_id.to_hex(),
        path,
        object_id: object_id.to_hex(),
        snapshot: SnapshotHashBody {
            version: ACL_SNAPSHOT_VERSION_POSIX_TREE_V1,
            requirements,
        },
    };
    let encoded = serde_json::to_vec(&payload).map_err(|_| search_index_not_ready_error())?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub fn verify_acl_snapshot(
    head: &SearchIndexHead,
    path: &str,
    object_id: ObjectId,
    snapshot: &SearchAclSnapshot,
) -> Result<(), VfsError> {
    if snapshot.version != ACL_SNAPSHOT_VERSION_POSIX_TREE_V1 {
        return Err(search_index_not_ready_error());
    }
    if snapshot.requirements.is_empty() {
        return Err(search_index_not_ready_error());
    }
    for requirement in &snapshot.requirements {
        if requirement.path.is_empty() || !requirement.path.starts_with('/') {
            return Err(search_index_not_ready_error());
        }
    }
    if !posix_tree_snapshot_has_required_shape(path, &snapshot.requirements) {
        return Err(search_index_not_ready_error());
    }
    let expected = snapshot_hash(head, path, object_id, &snapshot.requirements)?;
    if snapshot.hash != expected {
        return Err(search_index_not_ready_error());
    }
    Ok(())
}

fn posix_tree_snapshot_has_required_shape(
    path: &str,
    requirements: &[SearchAclRequirement],
) -> bool {
    if !path.starts_with('/') || path == "/" {
        return false;
    }
    requirements.iter().any(|requirement| {
        requirement.path == "/"
            && requirement.access == SearchAclAccess::Execute
            && requirement.mode == DURABLE_ROOT_MODE
            && requirement.uid == ROOT_UID
            && requirement.gid == ROOT_GID
    }) && ancestor_paths(path).into_iter().all(|ancestor| {
        requirements.iter().any(|requirement| {
            requirement.path == ancestor && requirement.access == SearchAclAccess::Execute
        })
    }) && requirements
        .iter()
        .any(|requirement| requirement.path == path && requirement.access == SearchAclAccess::Read)
}

fn ancestor_paths(path: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let mut current = String::new();
    for component in path.split('/').filter(|component| !component.is_empty()) {
        let next = if current.is_empty() {
            format!("/{component}")
        } else {
            format!("{current}/{component}")
        };
        if next != path {
            ancestors.push(next.clone());
        }
        current = next;
    }
    ancestors
}

pub fn acl_filter_allows_path(path: &str, filter: &SearchAclFilter) -> bool {
    filter
        .read_prefixes
        .iter()
        .any(|prefix| path_matches_prefix(path, Some(prefix.as_str())))
}

fn principal_passes_requirement(
    principal: &SearchAclPrincipal,
    requirement: &SearchAclRequirement,
) -> bool {
    let access = match requirement.access {
        SearchAclAccess::Read => Access::Read,
        SearchAclAccess::Execute => Access::Execute,
    };
    check_mode_permission(
        principal.uid,
        principal.gid,
        &principal.groups,
        requirement.mode,
        requirement.uid,
        requirement.gid,
        access,
    )
}

pub fn acl_snapshot_allows(
    filter: &SearchAclFilter,
    snapshot: &SearchAclSnapshot,
    path: &str,
) -> bool {
    if !acl_filter_allows_path(path, filter) {
        return false;
    }
    for requirement in &snapshot.requirements {
        if !principal_passes_requirement(&filter.principal, requirement) {
            return false;
        }
        if let Some(delegate) = &filter.delegate
            && !principal_passes_requirement(delegate, requirement)
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::RepoId;
    use crate::store::tree::TreeEntryKind;
    use crate::vcs::CommitId;

    fn test_head() -> SearchIndexHead {
        SearchIndexHead {
            repo_id: RepoId::new("acl-test").unwrap(),
            commit_id: CommitId::from(ObjectId::from_bytes(&[1; 32])),
            root_tree_id: ObjectId::from_bytes(&[2; 32]),
        }
    }

    fn blob_entry(mode: u16, uid: Uid, gid: Gid) -> TreeEntry {
        TreeEntry {
            name: "file.txt".to_string(),
            kind: TreeEntryKind::Blob,
            id: ObjectId::from_bytes(b"file"),
            mode,
            uid,
            gid,
            mime_type: None,
            custom_attrs: Default::default(),
        }
    }

    #[test]
    fn rejects_self_hashed_snapshot_missing_file_requirement() {
        let head = test_head();
        let object_id = ObjectId::from_bytes(b"file");
        let requirements = vec![posix_root_execute_requirement()];
        let snapshot = SearchAclSnapshot {
            version: ACL_SNAPSHOT_VERSION_POSIX_TREE_V1.to_string(),
            hash: snapshot_hash(&head, "/file.txt", object_id, &requirements).unwrap(),
            requirements,
        };

        assert!(verify_acl_snapshot(&head, "/file.txt", object_id, &snapshot).is_err());
    }

    #[test]
    fn primary_gid_satisfies_group_bits_without_duplicate_group() {
        let head = test_head();
        let object_id = ObjectId::from_bytes(b"file");
        let snapshot = build_posix_tree_snapshot(
            &head,
            "/file.txt",
            object_id,
            &[posix_root_execute_requirement()],
            posix_requirement_from_entry(
                "/file.txt".to_string(),
                &blob_entry(0o040, 1000, 2000),
                SearchAclAccess::Read,
            ),
        )
        .unwrap();
        let filter = SearchAclFilter {
            principal: SearchAclPrincipal {
                uid: 3000,
                gid: 2000,
                groups: Vec::new(),
            },
            delegate: None,
            read_prefixes: vec!["/".to_string()],
        };

        assert!(acl_snapshot_allows(&filter, &snapshot, "/file.txt"));
    }
}
