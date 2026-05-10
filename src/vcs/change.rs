use crate::backend::{ObjectStore, RepoId};
use crate::error::VfsError;
use crate::fs::VirtualFs;
use crate::fs::inode::{InodeId, InodeKind};
use crate::store::blob::BlobStore;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const DURABLE_COMMITTED_READ_FAILED: &str = "durable committed read failed";
const DURABLE_PATH_RECORD_ENTRY_LIMIT: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurablePathRecordAction {
    Skip,
    Descend,
    Include,
    IncludeAndDescend,
}

impl DurablePathRecordAction {
    fn includes(self) -> bool {
        matches!(self, Self::Include | Self::IncludeAndDescend)
    }

    fn descends(self) -> bool {
        matches!(self, Self::Descend | Self::IncludeAndDescend)
    }
}

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
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub custom_attrs: BTreeMap<String, String>,
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

pub(crate) async fn durable_committed_path_records(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    root_tree_id: ObjectId,
) -> Result<PathMap, VfsError> {
    durable_committed_path_records_matching(repo_id, objects, root_tree_id, |_, entry| {
        if entry.kind == TreeEntryKind::Tree {
            DurablePathRecordAction::IncludeAndDescend
        } else {
            DurablePathRecordAction::Include
        }
    })
    .await
}

pub(crate) async fn durable_committed_path_records_matching(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    root_tree_id: ObjectId,
    filter: impl Fn(&str, &TreeEntry) -> DurablePathRecordAction,
) -> Result<PathMap, VfsError> {
    let root = durable_load_tree(repo_id, objects, root_tree_id).await?;
    let mut records = BTreeMap::new();
    let mut stack = vec![("/".to_string(), root)];
    let mut visited = 0usize;

    while let Some((dir_path, tree)) = stack.pop() {
        let mut entries = tree.entries;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        for entry in entries {
            visited = visited.saturating_add(1);
            if visited > DURABLE_PATH_RECORD_ENTRY_LIMIT {
                return Err(durable_read_failed());
            }

            let path = child_path(&dir_path, &entry.name);
            let action = filter(&path, &entry);
            if action == DurablePathRecordAction::Skip {
                continue;
            }

            match entry.kind {
                TreeEntryKind::Blob => {
                    if !action.includes() {
                        continue;
                    }
                    let size = durable_blob_len(repo_id, objects, entry.id).await?;
                    insert_durable_path_record(
                        &mut records,
                        PathRecord {
                            path,
                            kind: PathKind::File,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size,
                            content_id: Some(entry.id),
                            mime_type: entry.mime_type,
                            custom_attrs: entry.custom_attrs,
                        },
                    )?;
                }
                TreeEntryKind::Tree => {
                    if !action.includes() && !action.descends() {
                        continue;
                    }
                    let child_tree = durable_load_tree(repo_id, objects, entry.id).await?;
                    if action.includes() {
                        let size = child_tree.entries.len() as u64;
                        insert_durable_path_record(
                            &mut records,
                            PathRecord {
                                path: path.clone(),
                                kind: PathKind::Directory,
                                mode: entry.mode,
                                uid: entry.uid,
                                gid: entry.gid,
                                size,
                                content_id: None,
                                mime_type: entry.mime_type,
                                custom_attrs: entry.custom_attrs,
                            },
                        )?;
                    }
                    if action.descends() {
                        stack.push((path, child_tree));
                    }
                }
                TreeEntryKind::Symlink => {
                    if !action.includes() {
                        continue;
                    }
                    let size = durable_blob_len(repo_id, objects, entry.id).await?;
                    insert_durable_path_record(
                        &mut records,
                        PathRecord {
                            path,
                            kind: PathKind::Symlink,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            size,
                            content_id: Some(entry.id),
                            mime_type: entry.mime_type,
                            custom_attrs: entry.custom_attrs,
                        },
                    )?;
                }
            }
        }
    }

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
                        mime_type: child.mime_type.clone(),
                        custom_attrs: child.custom_attrs.clone(),
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
                        mime_type: child.mime_type.clone(),
                        custom_attrs: child.custom_attrs.clone(),
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
                        mime_type: child.mime_type.clone(),
                        custom_attrs: child.custom_attrs.clone(),
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
                        mime_type: entry.mime_type.clone(),
                        custom_attrs: entry.custom_attrs.clone(),
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
                        mime_type: entry.mime_type.clone(),
                        custom_attrs: entry.custom_attrs.clone(),
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
                        mime_type: entry.mime_type.clone(),
                        custom_attrs: entry.custom_attrs.clone(),
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

async fn durable_load_tree(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    tree_id: ObjectId,
) -> Result<TreeObject, VfsError> {
    let stored = objects
        .get(repo_id, tree_id, ObjectKind::Tree)
        .await
        .map_err(|_| durable_read_failed())?
        .ok_or_else(durable_read_failed)?;
    if stored.repo_id != *repo_id || stored.id != tree_id || stored.kind != ObjectKind::Tree {
        return Err(durable_read_failed());
    }
    let tree = TreeObject::deserialize(&stored.bytes).map_err(|_| durable_read_failed())?;
    validate_durable_tree(&tree)?;
    Ok(tree)
}

async fn durable_blob_len(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    blob_id: ObjectId,
) -> Result<u64, VfsError> {
    objects
        .object_len(repo_id, blob_id, ObjectKind::Blob)
        .await
        .map_err(|_| durable_read_failed())?
        .ok_or_else(durable_read_failed)
}

fn insert_durable_path_record(records: &mut PathMap, record: PathRecord) -> Result<(), VfsError> {
    if records.insert(record.path.clone(), record).is_some() {
        return Err(durable_read_failed());
    }
    Ok(())
}

fn validate_durable_tree(tree: &TreeObject) -> Result<(), VfsError> {
    let mut names = BTreeSet::new();
    for entry in &tree.entries {
        if entry.name.is_empty()
            || entry.name == "."
            || entry.name == ".."
            || entry.name.contains('/')
            || entry.name.contains('\0')
            || !names.insert(entry.name.as_str())
        {
            return Err(durable_read_failed());
        }
    }
    Ok(())
}

fn durable_read_failed() -> VfsError {
    VfsError::CorruptStore {
        message: DURABLE_COMMITTED_READ_FAILED.to_string(),
    }
}

fn content_changed(before: &PathRecord, after: &PathRecord) -> bool {
    before.content_id != after.content_id
}

fn metadata_changed(before: &PathRecord, after: &PathRecord) -> bool {
    before.mode != after.mode
        || before.uid != after.uid
        || before.gid != after.gid
        || before.mime_type != after.mime_type
        || before.custom_attrs != after.custom_attrs
}

fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}
