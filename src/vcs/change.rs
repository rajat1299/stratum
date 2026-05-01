use crate::error::VfsError;
use crate::fs::VirtualFs;
use crate::fs::inode::{InodeId, InodeKind};
use crate::store::blob::BlobStore;
use crate::store::tree::{TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    TypeChanged,
    MetadataChanged,
}

impl ChangeKind {
    pub(crate) fn status_code(self) -> &'static str {
        match self {
            ChangeKind::Added => "A",
            ChangeKind::Modified => "M",
            ChangeKind::Deleted => "D",
            ChangeKind::TypeChanged => "T",
            ChangeKind::MetadataChanged => "m",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRecord {
    pub path: String,
    pub kind: PathKind,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub content_id: Option<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangedPath {
    pub path: String,
    pub kind: ChangeKind,
    pub before: Option<PathRecord>,
    pub after: Option<PathRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSummary {
    pub head: Option<ObjectId>,
    pub object_count: usize,
    pub file_count: u64,
    pub total_size: u64,
    pub changes: Vec<ChangedPath>,
}

impl StatusSummary {
    pub fn is_clean(&self) -> bool {
        self.changes.is_empty()
    }
}

pub(crate) type PathMap = BTreeMap<String, PathRecord>;

pub(crate) fn diff_path_maps(before: &PathMap, after: &PathMap) -> Vec<ChangedPath> {
    let mut paths = BTreeSet::new();
    paths.extend(before.keys().cloned());
    paths.extend(after.keys().cloned());

    paths
        .into_iter()
        .filter_map(|path| {
            let before_record = before.get(&path);
            let after_record = after.get(&path);
            let kind = match (before_record, after_record) {
                (None, Some(_)) => ChangeKind::Added,
                (Some(_), None) => ChangeKind::Deleted,
                (Some(before), Some(after)) if before.kind != after.kind => ChangeKind::TypeChanged,
                (Some(before), Some(after)) if content_changed(before, after) => {
                    ChangeKind::Modified
                }
                (Some(before), Some(after)) if metadata_changed(before, after) => {
                    ChangeKind::MetadataChanged
                }
                _ => return None,
            };

            Some(ChangedPath {
                path,
                kind,
                before: before_record.cloned(),
                after: after_record.cloned(),
            })
        })
        .collect()
}

pub(crate) fn worktree_path_records(fs: &VirtualFs) -> Result<PathMap, VfsError> {
    let mut records = BTreeMap::new();
    walk_worktree_dir(fs, fs.root_id(), "/", &mut records)?;
    Ok(records)
}

pub(crate) fn committed_path_records(
    store: &BlobStore,
    root_tree_id: ObjectId,
) -> Result<PathMap, VfsError> {
    let tree = load_tree(store, root_tree_id)?;
    let mut records = BTreeMap::new();
    walk_committed_tree(store, &tree, "/", &mut records)?;
    Ok(records)
}

fn walk_worktree_dir(
    fs: &VirtualFs,
    dir_id: InodeId,
    dir_path: &str,
    records: &mut PathMap,
) -> Result<(), VfsError> {
    let inode = fs.get_inode(dir_id)?;
    let entries = match &inode.kind {
        InodeKind::Directory { entries } => entries,
        _ => {
            return Err(VfsError::NotDirectory {
                path: format!("<inode {dir_id}>"),
            });
        }
    };

    for (name, child_id) in entries {
        let child = fs.get_inode(*child_id)?;
        let path = child_path(dir_path, name);
        match &child.kind {
            InodeKind::File { content } => {
                records.insert(
                    path.clone(),
                    PathRecord {
                        path,
                        kind: PathKind::File,
                        mode: child.mode,
                        uid: child.uid,
                        gid: child.gid,
                        size: content.len() as u64,
                        content_id: Some(ObjectId::from_bytes(content)),
                    },
                );
            }
            InodeKind::Directory { entries } => {
                records.insert(
                    path.clone(),
                    PathRecord {
                        path: path.clone(),
                        kind: PathKind::Directory,
                        mode: child.mode,
                        uid: child.uid,
                        gid: child.gid,
                        size: entries.len() as u64,
                        content_id: None,
                    },
                );
                walk_worktree_dir(fs, *child_id, &path, records)?;
            }
            InodeKind::Symlink { target } => {
                records.insert(
                    path.clone(),
                    PathRecord {
                        path,
                        kind: PathKind::Symlink,
                        mode: child.mode,
                        uid: child.uid,
                        gid: child.gid,
                        size: target.len() as u64,
                        content_id: Some(ObjectId::from_bytes(target.as_bytes())),
                    },
                );
            }
        }
    }

    Ok(())
}

fn walk_committed_tree(
    store: &BlobStore,
    tree: &TreeObject,
    dir_path: &str,
    records: &mut PathMap,
) -> Result<(), VfsError> {
    let mut entries = tree.entries.iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.name.cmp(&right.name));

    for entry in entries {
        let path = child_path(dir_path, &entry.name);
        match entry.kind {
            TreeEntryKind::Blob => {
                let content = store.get_typed(&entry.id, ObjectKind::Blob)?;
                records.insert(
                    path.clone(),
                    PathRecord {
                        path,
                        kind: PathKind::File,
                        mode: entry.mode,
                        uid: entry.uid,
                        gid: entry.gid,
                        size: content.len() as u64,
                        content_id: Some(entry.id),
                    },
                );
            }
            TreeEntryKind::Tree => {
                let child_tree = load_tree(store, entry.id)?;
                records.insert(
                    path.clone(),
                    PathRecord {
                        path: path.clone(),
                        kind: PathKind::Directory,
                        mode: entry.mode,
                        uid: entry.uid,
                        gid: entry.gid,
                        size: child_tree.entries.len() as u64,
                        content_id: None,
                    },
                );
                walk_committed_tree(store, &child_tree, &path, records)?;
            }
            TreeEntryKind::Symlink => {
                let target = store.get_typed(&entry.id, ObjectKind::Blob)?;
                records.insert(
                    path.clone(),
                    PathRecord {
                        path,
                        kind: PathKind::Symlink,
                        mode: entry.mode,
                        uid: entry.uid,
                        gid: entry.gid,
                        size: target.len() as u64,
                        content_id: Some(entry.id),
                    },
                );
            }
        }
    }

    Ok(())
}

fn load_tree(store: &BlobStore, tree_id: ObjectId) -> Result<TreeObject, VfsError> {
    let tree_data = store.get_typed(&tree_id, ObjectKind::Tree)?;
    TreeObject::deserialize(tree_data).map_err(|e| VfsError::CorruptStore {
        message: format!("failed to deserialize tree {}: {e}", tree_id.short_hex()),
    })
}

fn content_changed(before: &PathRecord, after: &PathRecord) -> bool {
    before.content_id != after.content_id
}

fn metadata_changed(before: &PathRecord, after: &PathRecord) -> bool {
    before.mode != after.mode || before.uid != after.uid || before.gid != after.gid
}

fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}
