pub mod change;
pub mod diff;
pub mod refs;
pub mod revert;
pub mod snapshot;

use crate::error::VfsError;
use crate::fs::VirtualFs;
use crate::fs::inode::{InodeId, InodeKind};
use crate::store::blob::BlobStore;
use crate::store::commit::CommitObject;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
pub use change::{ChangeKind, ChangedPath, PathKind, PathRecord, StatusSummary};
use change::{PathMap, committed_path_records, diff_path_maps, worktree_path_records};
pub use refs::{CommitId, MAIN_REF, RefName, RefUpdateExpectation, VcsRef};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Vcs {
    pub(crate) store: BlobStore,
    pub(crate) head: Option<ObjectId>,
    pub(crate) commits: Vec<CommitObject>,
    pub(crate) refs: BTreeMap<RefName, VcsRef>,
}

impl Default for Vcs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vcs {
    pub fn new() -> Self {
        Vcs {
            store: BlobStore::new(),
            head: None,
            commits: Vec::new(),
            refs: BTreeMap::new(),
        }
    }

    pub fn commit(
        &mut self,
        fs: &VirtualFs,
        message: &str,
        author: &str,
    ) -> Result<ObjectId, VfsError> {
        let main_ref = RefName::new(MAIN_REF)?;
        self.ensure_ref_version_can_advance(&main_ref)?;

        let before = self.head_path_records()?;
        let root_tree_id = self.snapshot_dir(fs, fs.root_id())?;
        let after = committed_path_records(&self.store, root_tree_id)?;
        let changed_paths = diff_path_maps(&before, &after);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let commit = CommitObject {
            id: ObjectId::from_bytes(&[0; 32]), // placeholder
            tree: root_tree_id,
            parent: self.head,
            timestamp,
            message: message.to_string(),
            author: author.to_string(),
            changed_paths,
        };

        let commit_data = commit.serialize();
        let commit_id = self.store.put(&commit_data, ObjectKind::Commit);

        let final_commit = CommitObject {
            id: commit_id,
            ..commit
        };

        self.commits.push(final_commit);
        self.head = Some(commit_id);
        self.set_ref_target_unchecked(main_ref, CommitId::from(commit_id))?;
        Ok(commit_id)
    }

    fn snapshot_dir(&mut self, fs: &VirtualFs, dir_id: InodeId) -> Result<ObjectId, VfsError> {
        let inode = fs.get_inode(dir_id)?;
        let entries = match &inode.kind {
            InodeKind::Directory { entries } => entries,
            _ => {
                return Err(VfsError::NotDirectory {
                    path: format!("<inode {dir_id}>"),
                });
            }
        };

        let mut tree_entries = Vec::with_capacity(entries.len());
        for (name, &child_id) in entries {
            let child = fs.get_inode(child_id)?;
            let (kind, id) = match &child.kind {
                InodeKind::File { content } => {
                    let blob_id = self.store.put(content, ObjectKind::Blob);
                    (TreeEntryKind::Blob, blob_id)
                }
                InodeKind::Directory { .. } => {
                    let tree_id = self.snapshot_dir(fs, child_id)?;
                    (TreeEntryKind::Tree, tree_id)
                }
                InodeKind::Symlink { target } => {
                    let blob_id = self.store.put(target.as_bytes(), ObjectKind::Blob);
                    (TreeEntryKind::Symlink, blob_id)
                }
            };

            tree_entries.push(TreeEntry {
                name: name.clone(),
                kind,
                id,
                mode: child.mode,
                uid: child.uid,
                gid: child.gid,
                mime_type: child.mime_type.clone(),
                custom_attrs: child.custom_attrs.clone(),
            });
        }

        let tree = TreeObject {
            entries: tree_entries,
        };
        let tree_data = tree.serialize();
        Ok(self.store.put(&tree_data, ObjectKind::Tree))
    }

    pub fn log(&self) -> Vec<&CommitObject> {
        self.commits.iter().rev().collect()
    }

    pub fn head(&self) -> Option<ObjectId> {
        self.head
    }

    pub fn commit_count(&self) -> usize {
        self.commits.len()
    }

    pub fn object_count(&self) -> usize {
        self.store.object_count()
    }

    pub fn revert(&mut self, fs: &mut VirtualFs, hash_prefix: &str) -> Result<(), VfsError> {
        let main_ref = RefName::new(MAIN_REF)?;
        self.ensure_ref_version_can_advance(&main_ref)?;

        let commit = self.find_commit(hash_prefix)?;
        let tree_id = commit.tree;
        let commit_id = commit.id;

        // Reconstruct VFS from tree
        revert::restore_from_tree(fs, &self.store, tree_id)?;

        self.head = Some(commit_id);
        self.set_ref_target_unchecked(main_ref, CommitId::from(commit_id))?;
        Ok(())
    }

    pub fn find_commit(&self, hash_prefix: &str) -> Result<&CommitObject, VfsError> {
        let matches: Vec<&CommitObject> = self
            .commits
            .iter()
            .filter(|c| c.id.to_hex().starts_with(hash_prefix))
            .collect();

        match matches.len() {
            0 => Err(VfsError::ObjectNotFound {
                id: hash_prefix.to_string(),
            }),
            1 => Ok(matches[0]),
            _ => Err(VfsError::InvalidArgs {
                message: format!("ambiguous commit prefix: {hash_prefix}"),
            }),
        }
    }

    pub fn status(&self, fs: &VirtualFs) -> Result<String, VfsError> {
        let summary = self.status_summary(fs)?;
        if summary.head.is_none() {
            return Ok("No commits yet.\n".to_string());
        }

        let mut output = String::new();
        output.push_str(&format!(
            "On commit {}\n",
            summary.head.unwrap().short_hex()
        ));
        output.push_str(&format!("Objects in store: {}\n", summary.object_count));

        output.push_str(&format!(
            "Files: {}, Total size: {} bytes\n",
            summary.file_count, summary.total_size
        ));
        if summary.is_clean() {
            output.push_str("Working tree clean\n");
        } else {
            output.push_str("Changes:\n");
            for change in &summary.changes {
                output.push_str(&format!("{} {}\n", change.kind.status_code(), change.path));
            }
        }
        Ok(output)
    }

    pub fn status_summary(&self, fs: &VirtualFs) -> Result<StatusSummary, VfsError> {
        let before = self.head_path_records()?;
        let after = worktree_path_records(fs)?;
        let changes = diff_path_maps(&before, &after);

        let mut file_count = 0u64;
        let mut total_size = 0u64;
        for inode in fs.all_inodes().values() {
            if let InodeKind::File { content } = &inode.kind {
                file_count += 1;
                total_size += content.len() as u64;
            }
        }

        Ok(StatusSummary {
            head: self.head,
            object_count: self.store.object_count(),
            file_count,
            total_size,
            changes,
        })
    }

    pub fn diff(&self, fs: &VirtualFs, path: Option<&str>) -> Result<String, VfsError> {
        let before = self.head_path_records()?;
        let after = worktree_path_records(fs)?;
        let changes = diff_path_maps(&before, &after);
        diff::render_worktree_diff(&self.store, fs, &before, &after, &changes, path)
    }

    pub fn list_refs(&self) -> Vec<VcsRef> {
        self.refs.values().cloned().collect()
    }

    pub fn get_ref(&self, name: RefName) -> Result<Option<VcsRef>, VfsError> {
        Ok(self.refs.get(&name).cloned())
    }

    pub fn create_ref(&mut self, name: RefName, target: CommitId) -> Result<VcsRef, VfsError> {
        if self.refs.contains_key(&name) {
            return Err(VfsError::AlreadyExists {
                path: name.into_string(),
            });
        }
        self.ensure_commit_exists(target)?;

        let vcs_ref = VcsRef {
            name: name.clone(),
            target,
            version: 1,
        };
        if name.as_str() == MAIN_REF {
            self.head = Some(target.object_id());
        }
        self.refs.insert(name, vcs_ref.clone());
        Ok(vcs_ref)
    }

    pub fn update_ref(
        &mut self,
        name: RefName,
        expected: RefUpdateExpectation,
        target: CommitId,
    ) -> Result<VcsRef, VfsError> {
        self.compare_and_swap_ref(name, Some(expected), target)
    }

    pub fn compare_and_swap_ref(
        &mut self,
        name: RefName,
        expected: Option<RefUpdateExpectation>,
        target: CommitId,
    ) -> Result<VcsRef, VfsError> {
        match (self.refs.get(&name), expected) {
            (Some(current), Some(expected)) => {
                if current.target != expected.target || current.version != expected.version {
                    return Err(VfsError::InvalidArgs {
                        message: format!("ref compare-and-swap mismatch: {name}"),
                    });
                }
                self.ensure_commit_exists(target)?;
                let next_version = next_ref_version(current.version)?;
                let vcs_ref = VcsRef {
                    name: name.clone(),
                    target,
                    version: next_version,
                };
                if name.as_str() == MAIN_REF {
                    self.head = Some(target.object_id());
                }
                self.refs.insert(name, vcs_ref.clone());
                Ok(vcs_ref)
            }
            (None, None) => self.create_ref(name, target),
            (Some(_), None) | (None, Some(_)) => Err(VfsError::InvalidArgs {
                message: format!("ref compare-and-swap mismatch: {name}"),
            }),
        }
    }

    pub(crate) fn set_ref_target_unchecked(
        &mut self,
        name: RefName,
        target: CommitId,
    ) -> Result<(), VfsError> {
        let version = match self
            .refs
            .get(&name)
            .map(|existing| next_ref_version(existing.version))
        {
            Some(version) => version?,
            None => 1,
        };
        let vcs_ref = VcsRef {
            name: name.clone(),
            target,
            version,
        };
        if name.as_str() == MAIN_REF {
            self.head = Some(target.object_id());
        }
        self.refs.insert(name, vcs_ref);
        Ok(())
    }

    fn ensure_commit_exists(&self, id: CommitId) -> Result<(), VfsError> {
        if self
            .commits
            .iter()
            .any(|commit| commit.id == id.object_id())
        {
            Ok(())
        } else {
            Err(VfsError::ObjectNotFound { id: id.to_hex() })
        }
    }

    fn head_path_records(&self) -> Result<PathMap, VfsError> {
        match self.head {
            Some(head) => {
                let commit = self.commit_by_id(head)?;
                committed_path_records(&self.store, commit.tree)
            }
            None => Ok(BTreeMap::new()),
        }
    }

    fn commit_by_id(&self, id: ObjectId) -> Result<&CommitObject, VfsError> {
        self.commits
            .iter()
            .find(|commit| commit.id == id)
            .ok_or_else(|| VfsError::ObjectNotFound { id: id.short_hex() })
    }

    fn ensure_ref_version_can_advance(&self, name: &RefName) -> Result<(), VfsError> {
        if let Some(current) = self.refs.get(name) {
            next_ref_version(current.version)?;
        }
        Ok(())
    }
}

fn next_ref_version(current: u64) -> Result<u64, VfsError> {
    match current.checked_add(1) {
        Some(next) if next < u64::MAX => Ok(next),
        _ => Err(VfsError::CorruptStore {
            message: "ref version overflow".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{ROOT_GID, ROOT_UID};

    #[test]
    fn ref_version_overflow_fails_without_wrapping() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();

        fs.touch("/a.txt", ROOT_UID, ROOT_GID).unwrap();
        let id1 = vcs.commit(&fs, "first", "root").unwrap();
        fs.touch("/b.txt", ROOT_UID, ROOT_GID).unwrap();
        let id2 = vcs.commit(&fs, "second", "root").unwrap();

        let name = RefName::session("alice", "s1").unwrap();
        vcs.create_ref(name.clone(), CommitId::from(id1)).unwrap();
        vcs.refs.get_mut(&name).unwrap().version = u64::MAX;

        let err = vcs
            .update_ref(
                name,
                RefUpdateExpectation::new(CommitId::from(id1), u64::MAX),
                CommitId::from(id2),
            )
            .unwrap_err();

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn ref_version_update_rejects_max_value_result() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();

        fs.touch("/a.txt", ROOT_UID, ROOT_GID).unwrap();
        let id1 = vcs.commit(&fs, "first", "root").unwrap();
        fs.touch("/b.txt", ROOT_UID, ROOT_GID).unwrap();
        let id2 = vcs.commit(&fs, "second", "root").unwrap();

        let name = RefName::session("alice", "s1").unwrap();
        vcs.create_ref(name.clone(), CommitId::from(id1)).unwrap();
        vcs.refs.get_mut(&name).unwrap().version = u64::MAX - 1;

        let err = vcs
            .update_ref(
                name,
                RefUpdateExpectation::new(CommitId::from(id1), u64::MAX - 1),
                CommitId::from(id2),
            )
            .unwrap_err();

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn main_ref_overflow_blocks_commit_without_partial_head_update() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();

        fs.touch("/a.txt", ROOT_UID, ROOT_GID).unwrap();
        let id1 = vcs.commit(&fs, "first", "root").unwrap();
        let main = RefName::new(MAIN_REF).unwrap();
        vcs.refs.get_mut(&main).unwrap().version = u64::MAX;
        let commits_before = vcs.commits.len();

        fs.touch("/b.txt", ROOT_UID, ROOT_GID).unwrap();
        let err = vcs.commit(&fs, "second", "root").unwrap_err();

        assert!(matches!(err, VfsError::CorruptStore { .. }));
        assert_eq!(vcs.head, Some(id1));
        assert_eq!(vcs.commits.len(), commits_before);
    }
}
