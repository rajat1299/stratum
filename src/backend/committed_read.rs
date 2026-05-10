use std::collections::{BTreeMap, BTreeSet};

use regex::Regex;

use crate::auth::perms::Access;
use crate::auth::session::Session;
use crate::auth::{ROOT_GID, ROOT_UID};
use crate::backend::{CommitRecord, CommitStore, ObjectStore, RefStore, RepoId};
use crate::error::VfsError;
use crate::fs::inode::InodeId;
use crate::fs::{GrepResult, LsEntry, StatInfo};
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::change::{
    DurablePathRecordAction, PathMap, diff_path_maps, durable_committed_path_records,
    durable_committed_path_records_matching,
};
use crate::vcs::{ChangedPath, CommitId, MAIN_REF, PathKind, RefName};

const DURABLE_COMMITTED_READ_FAILED: &str = "durable committed read failed";
const DURABLE_COMMITTED_PATH: &str = "durable committed path";
const DURABLE_ROOT_MODE: u16 = 0o755;
const DURABLE_BLOCK_SIZE: u64 = 4096;
const MAX_SYMLINK_DEPTH: usize = 40;
const DURABLE_TRAVERSAL_ENTRY_LIMIT: usize = 100_000;
const DURABLE_SEARCH_RESULT_LIMIT: usize = 10_000;

pub(crate) struct DurableCommittedFsReader<'a> {
    repo_id: &'a RepoId,
    refs: &'a dyn RefStore,
    commits: &'a dyn CommitStore,
    objects: &'a dyn ObjectStore,
}

#[derive(Debug, Clone)]
pub(crate) struct DurablePathCompareSource {
    pub(crate) target_ref: String,
    pub(crate) session_ref: Option<String>,
    pub(crate) base_commit: CommitId,
    pub(crate) head_commit: CommitId,
    pub(crate) base_root_tree: ObjectId,
    pub(crate) head_root_tree: ObjectId,
}

#[derive(Debug, Clone)]
pub(crate) struct DurablePathCompareSummary {
    pub(crate) source: DurablePathCompareSource,
    pub(crate) head_reachable_object_count: usize,
    pub(crate) head_file_count: u64,
    pub(crate) head_total_size: u64,
    pub(crate) changes: Vec<ChangedPath>,
}

#[derive(Clone)]
struct DurableCommitRoot {
    commit: CommitRecord,
    tree: TreeObject,
}

#[derive(Clone)]
enum ResolvedDurableNodeKind {
    Root { tree: TreeObject },
    Entry { entry: TreeEntry },
}

#[derive(Clone)]
struct ResolvedDurableNode {
    path: String,
    kind: ResolvedDurableNodeKind,
}

impl<'a> DurableCommittedFsReader<'a> {
    pub(crate) fn new(
        repo_id: &'a RepoId,
        refs: &'a dyn RefStore,
        commits: &'a dyn CommitStore,
        objects: &'a dyn ObjectStore,
    ) -> Self {
        Self {
            repo_id,
            refs,
            commits,
            objects,
        }
    }

    pub(crate) async fn compare_main_and_session_as(
        &self,
        session: &Session,
    ) -> Result<DurablePathCompareSummary, VfsError> {
        let mount = session.mount();
        let target_ref_name = mount.map(|mount| mount.base_ref()).unwrap_or(MAIN_REF);
        let target_ref = RefName::new(target_ref_name).map_err(|_| durable_read_failed())?;
        let base_ref = self
            .refs
            .get(self.repo_id, &target_ref)
            .await
            .map_err(|_| durable_read_failed())?
            .ok_or_else(not_found)?;
        let base_commit = self.load_commit(base_ref.target).await?;

        let session_ref = mount.and_then(|mount| mount.session_ref());
        let head_commit_id = match session_ref {
            Some(session_ref) => {
                let session_ref = RefName::new(session_ref).map_err(|_| durable_read_failed())?;
                self.refs
                    .get(self.repo_id, &session_ref)
                    .await
                    .map_err(|_| durable_read_failed())?
                    .map(|record| record.target)
                    .unwrap_or(base_ref.target)
            }
            None => base_ref.target,
        };
        let head_commit = if head_commit_id == base_commit.id {
            base_commit.clone()
        } else {
            self.load_commit(head_commit_id).await?
        };

        let before = self
            .status_path_records(base_commit.root_tree, session)
            .await?;
        let (head_reachable_object_count, head_file_count, head_total_size, changes) =
            if head_commit.root_tree == base_commit.root_tree {
                let (object_count, file_count, total_size) = durable_status_counts(&before);
                (object_count, file_count, total_size, Vec::new())
            } else {
                let after = self
                    .status_path_records(head_commit.root_tree, session)
                    .await?;
                let changes = diff_path_maps(&before, &after);
                let (object_count, file_count, total_size) = durable_status_counts(&after);
                (object_count, file_count, total_size, changes)
            };

        Ok(DurablePathCompareSummary {
            source: DurablePathCompareSource {
                target_ref: target_ref.as_str().to_string(),
                session_ref: session_ref.map(str::to_string),
                base_commit: base_commit.id,
                head_commit: head_commit.id,
                base_root_tree: base_commit.root_tree,
                head_root_tree: head_commit.root_tree,
            },
            head_reachable_object_count,
            head_file_count,
            head_total_size,
            changes,
        })
    }

    async fn status_path_records(
        &self,
        root_tree_id: ObjectId,
        session: &Session,
    ) -> Result<PathMap, VfsError> {
        if session.mount().is_none() && session.scope.is_none() {
            return durable_committed_path_records(self.repo_id, self.objects, root_tree_id).await;
        }

        let mount_root = session
            .mount()
            .map(|_| session.resolve_mounted_path("/"))
            .transpose()?;
        durable_committed_path_records_matching(
            self.repo_id,
            self.objects,
            root_tree_id,
            |path, entry| durable_status_path_action(session, mount_root.as_deref(), path, entry),
        )
        .await
    }

    pub(crate) async fn cat_with_stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<(Vec<u8>, StatInfo), VfsError> {
        let root = self.current_root(session).await?;
        let mut node = self.resolve_path_in_root(&root, path, session).await?;
        for _ in 0..MAX_SYMLINK_DEPTH {
            let ResolvedDurableNodeKind::Entry { entry } = &node.kind else {
                return Err(is_directory());
            };
            match entry.kind {
                TreeEntryKind::Blob => {
                    require_read(session, &node.path, entry)?;
                    let content = self.load_blob_bytes(entry.id).await?;
                    let stat = self
                        .stat_for_entry(&node.path, entry, root.commit.timestamp)
                        .await?;
                    return Ok((content, stat));
                }
                TreeEntryKind::Tree => return Err(is_directory()),
                TreeEntryKind::Symlink => {
                    require_read(session, &node.path, entry)?;
                    let target = self.load_blob_bytes(entry.id).await?;
                    let target = String::from_utf8(target).map_err(|_| durable_read_failed())?;
                    let next_path = resolve_symlink_target(&node.path, &target)?;
                    node = self
                        .resolve_path_in_root(&root, &next_path, session)
                        .await?;
                }
            }
        }

        Err(VfsError::SymlinkLoop {
            path: DURABLE_COMMITTED_PATH.to_string(),
        })
    }

    pub(crate) async fn ls_as(
        &self,
        path: Option<&str>,
        session: &Session,
    ) -> Result<Vec<LsEntry>, VfsError> {
        let root = self.current_root(session).await?;
        let node = self
            .resolve_path_in_root(&root, path.unwrap_or("/"), session)
            .await?;
        match node.kind {
            ResolvedDurableNodeKind::Root { tree } => {
                require_root_read_execute(session, &node.path)?;
                self.ls_tree_entries(&node.path, tree.entries, root.commit.timestamp, session)
                    .await
            }
            ResolvedDurableNodeKind::Entry { entry } => match entry.kind {
                TreeEntryKind::Tree => {
                    require_read_execute(session, &node.path, &entry)?;
                    let tree = self.load_tree(entry.id).await?;
                    self.ls_tree_entries(&node.path, tree.entries, root.commit.timestamp, session)
                        .await
                }
                TreeEntryKind::Blob | TreeEntryKind::Symlink => {
                    require_read(session, &node.path, &entry)?;
                    Ok(vec![
                        self.ls_entry_for(basename(&node.path), &entry, root.commit.timestamp)
                            .await?,
                    ])
                }
            },
        }
    }

    pub(crate) async fn stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<StatInfo, VfsError> {
        let root = self.current_root(session).await?;
        let node = self.resolve_path_in_root(&root, path, session).await?;
        match node.kind {
            ResolvedDurableNodeKind::Root { tree } => {
                require_root_read_execute(session, &node.path)?;
                Ok(stat_for_root(&tree, root.commit.timestamp))
            }
            ResolvedDurableNodeKind::Entry { entry } => {
                if entry.kind == TreeEntryKind::Tree {
                    require_read_execute(session, &node.path, &entry)?;
                } else {
                    require_read(session, &node.path, &entry)?;
                }
                self.stat_for_entry(&node.path, &entry, root.commit.timestamp)
                    .await
            }
        }
    }

    pub(crate) async fn mutation_stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<StatInfo, VfsError> {
        let root = self.current_root(session).await?;
        let node = self
            .resolve_path_in_root_for_mutation(&root, path, session)
            .await?;
        match node.kind {
            ResolvedDurableNodeKind::Root { tree } => {
                Ok(stat_for_root(&tree, root.commit.timestamp))
            }
            ResolvedDurableNodeKind::Entry { entry } => {
                self.stat_for_entry(&node.path, &entry, root.commit.timestamp)
                    .await
            }
        }
    }

    pub(crate) async fn tree_as(
        &self,
        path: Option<&str>,
        session: &Session,
    ) -> Result<String, VfsError> {
        let root = self.current_root(session).await?;
        let node = self
            .resolve_path_in_root(&root, path.unwrap_or("/"), session)
            .await?;
        let tree = match node.kind {
            ResolvedDurableNodeKind::Root { tree } => {
                require_root_read_execute(session, &node.path)?;
                tree
            }
            ResolvedDurableNodeKind::Entry { entry } => {
                if entry.kind != TreeEntryKind::Tree {
                    return Err(not_directory());
                }
                require_read_execute(session, &node.path, &entry)?;
                self.load_tree(entry.id).await?
            }
        };

        self.render_tree(tree, &node.path, session).await
    }

    pub(crate) async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        let root = self.current_root(session).await?;
        let base_path = path.unwrap_or("/");
        let node = self.resolve_path_in_root(&root, base_path, session).await?;
        let pattern = pattern.map(glob_regex).transpose()?;
        let tree = match node.kind {
            ResolvedDurableNodeKind::Root { tree } => {
                require_root_read_execute(session, &node.path)?;
                tree
            }
            ResolvedDurableNodeKind::Entry { entry } => {
                if entry.kind != TreeEntryKind::Tree {
                    return Err(not_directory());
                }
                require_read_execute(session, &node.path, &entry)?;
                self.load_tree(entry.id).await?
            }
        };

        self.find_in_tree(
            tree,
            &normalize_absolute_path(base_path)?,
            pattern.as_ref(),
            session,
        )
        .await
    }

    pub(crate) async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        let re = Regex::new(pattern).map_err(|_| VfsError::InvalidArgs {
            message: "invalid regex".to_string(),
        })?;
        let root = self.current_root(session).await?;
        let base_path = path.unwrap_or("/");
        let node = self.resolve_path_in_root(&root, base_path, session).await?;

        match node.kind {
            ResolvedDurableNodeKind::Root { tree } => {
                require_root_read_execute(session, &node.path)?;
                if !recursive {
                    return Err(is_directory());
                }
                self.grep_tree(tree, "/", &re, session).await
            }
            ResolvedDurableNodeKind::Entry { entry } => match entry.kind {
                TreeEntryKind::Blob => {
                    require_read(session, &node.path, &entry)?;
                    let content = self.load_blob_bytes(entry.id).await?;
                    Ok(grep_blob(&node.path, &content, &re))
                }
                TreeEntryKind::Symlink => {
                    require_read(session, &node.path, &entry)?;
                    Ok(Vec::new())
                }
                TreeEntryKind::Tree if recursive => {
                    require_read_execute(session, &node.path, &entry)?;
                    let tree = self.load_tree(entry.id).await?;
                    self.grep_tree(tree, &node.path, &re, session).await
                }
                TreeEntryKind::Tree => Err(is_directory()),
            },
        }
    }

    async fn current_root(&self, session: &Session) -> Result<DurableCommitRoot, VfsError> {
        let mount = session.mount();
        let ref_name = mount
            .and_then(|mount| mount.session_ref())
            .unwrap_or(MAIN_REF);
        let requested = RefName::new(ref_name).map_err(|_| durable_read_failed())?;
        let current = self
            .refs
            .get(self.repo_id, &requested)
            .await
            .map_err(|_| durable_read_failed())?;
        let current = match (current, mount.and_then(|mount| mount.session_ref())) {
            (Some(current), _) => current,
            (None, Some(_)) => {
                let base = RefName::new(mount.map(|mount| mount.base_ref()).unwrap_or(MAIN_REF))
                    .map_err(|_| durable_read_failed())?;
                self.refs
                    .get(self.repo_id, &base)
                    .await
                    .map_err(|_| durable_read_failed())?
                    .ok_or_else(not_found)?
            }
            (None, None) => return Err(not_found()),
        };
        let commit = self
            .commits
            .get(self.repo_id, current.target)
            .await
            .map_err(|_| durable_read_failed())?
            .ok_or_else(durable_read_failed)?;
        let tree = self.load_tree(commit.root_tree).await?;
        Ok(DurableCommitRoot { commit, tree })
    }

    async fn load_commit(&self, id: CommitId) -> Result<CommitRecord, VfsError> {
        let commit = self
            .commits
            .get(self.repo_id, id)
            .await
            .map_err(|_| durable_read_failed())?
            .ok_or_else(durable_read_failed)?;
        if commit.repo_id != *self.repo_id || commit.id != id {
            return Err(durable_read_failed());
        }
        Ok(commit)
    }

    async fn resolve_path_in_root(
        &self,
        root: &DurableCommitRoot,
        path: &str,
        session: &Session,
    ) -> Result<ResolvedDurableNode, VfsError> {
        let path = normalize_absolute_path(path)?;
        if !session.is_path_allowed(&path, Access::Read) {
            return Err(permission_denied());
        }
        self.resolve_normalized_path_in_root(root, path, session)
            .await
    }

    async fn resolve_path_in_root_for_mutation(
        &self,
        root: &DurableCommitRoot,
        path: &str,
        session: &Session,
    ) -> Result<ResolvedDurableNode, VfsError> {
        let path = normalize_absolute_path(path)?;
        self.resolve_normalized_path_in_root(root, path, session)
            .await
    }

    async fn resolve_normalized_path_in_root(
        &self,
        root: &DurableCommitRoot,
        path: String,
        session: &Session,
    ) -> Result<ResolvedDurableNode, VfsError> {
        require_root_execute(session, "/")?;

        let components = path_components(&path);
        if components.is_empty() {
            return Ok(ResolvedDurableNode {
                path,
                kind: ResolvedDurableNodeKind::Root {
                    tree: root.tree.clone(),
                },
            });
        }

        let mut tree = root.tree.clone();
        let mut current_path = "/".to_string();
        for (index, component) in components.iter().enumerate() {
            let entry = sorted_entries(&tree)
                .into_iter()
                .find(|entry| entry.name == *component)
                .ok_or_else(not_found)?;
            current_path = child_path(&current_path, &entry.name);
            let is_last = index + 1 == components.len();
            if is_last {
                return Ok(ResolvedDurableNode {
                    path: current_path,
                    kind: ResolvedDurableNodeKind::Entry { entry },
                });
            }
            if entry.kind != TreeEntryKind::Tree {
                return Err(not_directory());
            }
            require_execute(session, &current_path, &entry)?;
            tree = self.load_tree(entry.id).await?;
        }

        Err(not_found())
    }

    async fn load_tree(&self, id: ObjectId) -> Result<TreeObject, VfsError> {
        let stored = self
            .objects
            .get(self.repo_id, id, ObjectKind::Tree)
            .await
            .map_err(|_| durable_read_failed())?
            .ok_or_else(durable_read_failed)?;
        if stored.repo_id != *self.repo_id || stored.id != id || stored.kind != ObjectKind::Tree {
            return Err(durable_read_failed());
        }
        let tree = TreeObject::deserialize(&stored.bytes).map_err(|_| durable_read_failed())?;
        validate_tree(&tree)?;
        Ok(tree)
    }

    async fn load_blob_bytes(&self, id: ObjectId) -> Result<Vec<u8>, VfsError> {
        let stored = self
            .objects
            .get(self.repo_id, id, ObjectKind::Blob)
            .await
            .map_err(|_| durable_read_failed())?
            .ok_or_else(durable_read_failed)?;
        if stored.repo_id != *self.repo_id || stored.id != id || stored.kind != ObjectKind::Blob {
            return Err(durable_read_failed());
        }
        Ok(stored.bytes)
    }

    async fn blob_len(&self, id: ObjectId) -> Result<u64, VfsError> {
        self.objects
            .object_len(self.repo_id, id, ObjectKind::Blob)
            .await
            .map_err(|_| durable_read_failed())?
            .ok_or_else(durable_read_failed)
    }

    async fn stat_for_entry(
        &self,
        path: &str,
        entry: &TreeEntry,
        timestamp: u64,
    ) -> Result<StatInfo, VfsError> {
        let (kind, size, nlink, content_hash) = match entry.kind {
            TreeEntryKind::Blob => (
                "file",
                self.blob_len(entry.id).await?,
                1,
                Some(format!("sha256:{}", entry.id.to_hex())),
            ),
            TreeEntryKind::Tree => {
                let tree = self.load_tree(entry.id).await?;
                ("directory", tree.entries.len() as u64, 2, None)
            }
            TreeEntryKind::Symlink => ("symlink", self.blob_len(entry.id).await?, 1, None),
        };

        Ok(stat_info(
            synthetic_inode_id(path, Some(entry.id)),
            kind,
            size,
            entry.mode,
            entry.uid,
            entry.gid,
            nlink,
            timestamp,
            entry.mime_type.clone(),
            content_hash,
            entry.custom_attrs.clone(),
        ))
    }

    async fn ls_entry_for(
        &self,
        name: &str,
        entry: &TreeEntry,
        timestamp: u64,
    ) -> Result<LsEntry, VfsError> {
        let size = match entry.kind {
            TreeEntryKind::Blob | TreeEntryKind::Symlink => self.blob_len(entry.id).await?,
            TreeEntryKind::Tree => self.load_tree(entry.id).await?.entries.len() as u64,
        };
        Ok(LsEntry {
            name: name.to_string(),
            is_dir: entry.kind == TreeEntryKind::Tree,
            is_symlink: entry.kind == TreeEntryKind::Symlink,
            size,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            modified: timestamp,
        })
    }

    async fn ls_tree_entries(
        &self,
        parent_path: &str,
        entries: Vec<TreeEntry>,
        timestamp: u64,
        session: &Session,
    ) -> Result<Vec<LsEntry>, VfsError> {
        let mut result = Vec::new();
        for entry in sorted_entries_from_vec(entries) {
            let path = child_path(parent_path, &entry.name);
            if !is_visible_entry(session, &path, &entry) {
                continue;
            }
            result.push(self.ls_entry_for(&entry.name, &entry, timestamp).await?);
        }
        Ok(result)
    }

    async fn render_tree(
        &self,
        tree: TreeObject,
        base_path: &str,
        session: &Session,
    ) -> Result<String, VfsError> {
        let mut output = ".\n".to_string();
        let mut stack = vec![TreeRenderFrame::new(
            tree,
            String::new(),
            base_path.to_string(),
            session,
        )];
        while let Some(frame) = stack.last_mut() {
            let Some((entry, is_last, prefix, dir_path)) = frame.next_entry() else {
                stack.pop();
                continue;
            };
            let connector = if is_last {
                "\u{2514}\u{2500}\u{2500} "
            } else {
                "\u{251c}\u{2500}\u{2500} "
            };
            output.push_str(&prefix);
            output.push_str(connector);
            output.push_str(&entry.name);
            if entry.kind == TreeEntryKind::Tree {
                output.push('/');
            }
            output.push('\n');

            if entry.kind == TreeEntryKind::Tree && can_execute(session, &entry) {
                let child_tree = self.load_tree(entry.id).await?;
                let child_prefix = if is_last {
                    format!("{prefix}    ")
                } else {
                    format!("{prefix}\u{2502}   ")
                };
                let child_path = child_path(&dir_path, &entry.name);
                stack.push(TreeRenderFrame::new(
                    child_tree,
                    child_prefix,
                    child_path,
                    session,
                ));
            }
        }
        Ok(output)
    }

    async fn find_in_tree(
        &self,
        tree: TreeObject,
        base_path: &str,
        pattern: Option<&Regex>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        struct FindFrame {
            dir_path: String,
            entries: Vec<TreeEntry>,
            next: usize,
        }

        let mut results = Vec::new();
        let mut visited = 0usize;
        let mut stack = vec![FindFrame {
            dir_path: base_path.to_string(),
            entries: sorted_entries_from_vec(tree.entries),
            next: 0,
        }];
        while let Some(frame) = stack.last_mut() {
            if frame.next >= frame.entries.len() {
                stack.pop();
                continue;
            }

            let entry = frame.entries[frame.next].clone();
            frame.next += 1;
            visited = visited.saturating_add(1);
            if visited > DURABLE_TRAVERSAL_ENTRY_LIMIT {
                return Err(traversal_limit_exceeded());
            }
            let child = child_path(&frame.dir_path, &entry.name);
            if !is_visible_entry(session, &child, &entry) {
                continue;
            }
            if pattern.is_none_or(|pattern| pattern.is_match(&entry.name)) {
                if results.len() >= DURABLE_SEARCH_RESULT_LIMIT {
                    return Err(traversal_limit_exceeded());
                }
                results.push(child.clone());
            }
            if entry.kind == TreeEntryKind::Tree && can_execute(session, &entry) {
                let child_tree = self.load_tree(entry.id).await?;
                stack.push(FindFrame {
                    dir_path: child,
                    entries: sorted_entries_from_vec(child_tree.entries),
                    next: 0,
                });
            }
        }
        Ok(results)
    }

    async fn grep_tree(
        &self,
        tree: TreeObject,
        base_path: &str,
        re: &Regex,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        struct GrepFrame {
            dir_path: String,
            entries: Vec<TreeEntry>,
            next: usize,
        }

        let mut results = Vec::new();
        let mut visited = 0usize;
        let mut stack = vec![GrepFrame {
            dir_path: base_path.to_string(),
            entries: sorted_entries_from_vec(tree.entries),
            next: 0,
        }];
        while let Some(frame) = stack.last_mut() {
            if frame.next >= frame.entries.len() {
                stack.pop();
                continue;
            }

            let entry = frame.entries[frame.next].clone();
            frame.next += 1;
            visited = visited.saturating_add(1);
            if visited > DURABLE_TRAVERSAL_ENTRY_LIMIT {
                return Err(traversal_limit_exceeded());
            }
            let child = child_path(&frame.dir_path, &entry.name);
            if !is_visible_entry(session, &child, &entry) {
                continue;
            }
            match entry.kind {
                TreeEntryKind::Blob => {
                    let content = self.load_blob_bytes(entry.id).await?;
                    results.extend(grep_blob(&child, &content, re));
                    if results.len() > DURABLE_SEARCH_RESULT_LIMIT {
                        return Err(traversal_limit_exceeded());
                    }
                }
                TreeEntryKind::Tree if can_execute(session, &entry) => {
                    let child_tree = self.load_tree(entry.id).await?;
                    stack.push(GrepFrame {
                        dir_path: child,
                        entries: sorted_entries_from_vec(child_tree.entries),
                        next: 0,
                    });
                }
                TreeEntryKind::Tree | TreeEntryKind::Symlink => {}
            }
        }
        Ok(results)
    }
}

struct TreeRenderFrame {
    entries: Vec<TreeEntry>,
    index: usize,
    prefix: String,
    dir_path: String,
}

impl TreeRenderFrame {
    fn new(tree: TreeObject, prefix: String, dir_path: String, session: &Session) -> Self {
        let entries = sorted_entries_from_vec(tree.entries)
            .into_iter()
            .filter(|entry| {
                let path = child_path(&dir_path, &entry.name);
                is_visible_entry(session, &path, entry)
            })
            .collect();
        Self {
            entries,
            index: 0,
            prefix,
            dir_path,
        }
    }

    fn next_entry(&mut self) -> Option<(TreeEntry, bool, String, String)> {
        if self.index >= self.entries.len() {
            return None;
        }
        let entry = self.entries[self.index].clone();
        self.index += 1;
        let is_last = self.index == self.entries.len();
        Some((entry, is_last, self.prefix.clone(), self.dir_path.clone()))
    }
}

fn stat_for_root(tree: &TreeObject, timestamp: u64) -> StatInfo {
    stat_info(
        0,
        "directory",
        tree.entries.len() as u64,
        DURABLE_ROOT_MODE,
        ROOT_UID,
        ROOT_GID,
        2,
        timestamp,
        None,
        None,
        BTreeMap::new(),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "centralizes StatInfo synthesis without scattering partially-filled builders"
)]
fn stat_info(
    inode_id: InodeId,
    kind: &'static str,
    size: u64,
    mode: u16,
    uid: u32,
    gid: u32,
    nlink: u64,
    timestamp: u64,
    mime_type: Option<String>,
    content_hash: Option<String>,
    custom_attrs: BTreeMap<String, String>,
) -> StatInfo {
    StatInfo {
        inode_id,
        kind,
        size,
        mode,
        uid,
        gid,
        nlink,
        block_size: DURABLE_BLOCK_SIZE,
        blocks: durable_blocks(size),
        created: timestamp,
        modified: timestamp,
        accessed: timestamp,
        changed: timestamp,
        created_nanos: 0,
        modified_nanos: 0,
        accessed_nanos: 0,
        changed_nanos: 0,
        mime_type,
        content_hash,
        custom_attrs,
    }
}

fn durable_blocks(size: u64) -> u64 {
    if size == 0 { 0 } else { size.div_ceil(512) }
}

fn synthetic_inode_id(path: &str, id: Option<ObjectId>) -> InodeId {
    let mut bytes = [0u8; 8];
    if let Some(id) = id {
        bytes.copy_from_slice(&id.as_bytes()[..8]);
    }
    for (index, byte) in path.bytes().enumerate() {
        bytes[index % 8] ^= byte;
    }
    let value = u64::from_be_bytes(bytes);
    if value == 0 { 1 } else { value }
}

fn require_root_read_execute(session: &Session, path: &str) -> Result<(), VfsError> {
    require_root_read(session, path)?;
    require_root_execute(session, path)
}

fn require_root_read(session: &Session, path: &str) -> Result<(), VfsError> {
    if !session.is_path_allowed(path, Access::Read)
        || !session.has_permission_bits(DURABLE_ROOT_MODE, ROOT_UID, ROOT_GID, Access::Read)
    {
        return Err(permission_denied());
    }
    Ok(())
}

fn require_root_execute(session: &Session, path: &str) -> Result<(), VfsError> {
    let _ = path;
    if !session.has_permission_bits(DURABLE_ROOT_MODE, ROOT_UID, ROOT_GID, Access::Execute) {
        return Err(permission_denied());
    }
    Ok(())
}

fn require_read_execute(session: &Session, path: &str, entry: &TreeEntry) -> Result<(), VfsError> {
    require_read(session, path, entry)?;
    require_execute(session, path, entry)
}

fn require_read(session: &Session, path: &str, entry: &TreeEntry) -> Result<(), VfsError> {
    if !session.is_path_allowed(path, Access::Read) || !can_read(session, entry) {
        return Err(permission_denied());
    }
    Ok(())
}

fn require_execute(session: &Session, path: &str, entry: &TreeEntry) -> Result<(), VfsError> {
    let _ = path;
    if !can_execute(session, entry) {
        return Err(permission_denied());
    }
    Ok(())
}

fn can_read(session: &Session, entry: &TreeEntry) -> bool {
    session.has_permission_bits(entry.mode, entry.uid, entry.gid, Access::Read)
}

fn is_visible_entry(session: &Session, path: &str, entry: &TreeEntry) -> bool {
    session.is_path_allowed(path, Access::Read) && can_read(session, entry)
}

fn can_execute(session: &Session, entry: &TreeEntry) -> bool {
    entry.kind != TreeEntryKind::Tree
        || session.has_permission_bits(entry.mode, entry.uid, entry.gid, Access::Execute)
}

fn normalize_absolute_path(path: &str) -> Result<String, VfsError> {
    if !path.starts_with('/') {
        return Err(invalid_path());
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

fn path_components(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|component| !component.is_empty())
        .map(str::to_string)
        .collect()
}

fn resolve_symlink_target(link_path: &str, target: &str) -> Result<String, VfsError> {
    if target.starts_with('/') {
        return normalize_absolute_path(target);
    }
    let parent = parent_path(link_path);
    normalize_absolute_path(&child_path(&parent, target))
}

fn parent_path(path: &str) -> String {
    match path.rsplit_once('/') {
        Some(("", _)) | None => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
    }
}

fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn basename(path: &str) -> &str {
    path.rsplit('/')
        .find(|component| !component.is_empty())
        .unwrap_or(path)
}

fn sorted_entries(tree: &TreeObject) -> Vec<TreeEntry> {
    sorted_entries_from_vec(tree.entries.clone())
}

fn sorted_entries_from_vec(mut entries: Vec<TreeEntry>) -> Vec<TreeEntry> {
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

fn durable_status_counts(records: &PathMap) -> (usize, u64, u64) {
    let file_count = records
        .values()
        .filter(|record| record.kind == PathKind::File)
        .count() as u64;
    let total_size = records
        .values()
        .filter(|record| record.kind == PathKind::File)
        .map(|record| record.size)
        .sum();
    let reachable_object_count = 1
        + records
            .values()
            .filter(|record| record.kind == PathKind::Directory)
            .count()
        + records
            .values()
            .filter(|record| record.content_id.is_some())
            .count();
    (reachable_object_count, file_count, total_size)
}

fn durable_status_path_action(
    session: &Session,
    mount_root: Option<&str>,
    path: &str,
    entry: &TreeEntry,
) -> DurablePathRecordAction {
    if let Some(mount_root) = mount_root {
        if path_is_ancestor_of(path, mount_root) {
            return if entry.kind == TreeEntryKind::Tree && can_execute(session, entry) {
                DurablePathRecordAction::Descend
            } else {
                DurablePathRecordAction::Skip
            };
        }
        if !path_is_at_or_under(path, mount_root) {
            return DurablePathRecordAction::Skip;
        }
    }

    if !session.is_path_allowed(path, Access::Read) || !can_read(session, entry) {
        if mount_root.is_some() && entry.kind == TreeEntryKind::Tree && can_execute(session, entry)
        {
            return DurablePathRecordAction::Descend;
        }
        return DurablePathRecordAction::Skip;
    }

    if entry.kind == TreeEntryKind::Tree && can_execute(session, entry) {
        DurablePathRecordAction::IncludeAndDescend
    } else {
        DurablePathRecordAction::Include
    }
}

fn path_is_at_or_under(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn path_is_ancestor_of(path: &str, descendant: &str) -> bool {
    path != descendant
        && descendant
            .strip_prefix(path)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn validate_tree(tree: &TreeObject) -> Result<(), VfsError> {
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

fn grep_blob(path: &str, content: &[u8], re: &Regex) -> Vec<GrepResult> {
    let text = String::from_utf8_lossy(content);
    text.lines()
        .enumerate()
        .filter(|(_, line)| re.is_match(line))
        .map(|(line_num, line)| GrepResult {
            file: path.to_string(),
            line_num: line_num + 1,
            line: line.to_string(),
        })
        .collect()
}

fn glob_regex(pattern: &str) -> Result<Regex, VfsError> {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }
    regex.push('$');
    Regex::new(&regex).map_err(|_| VfsError::InvalidArgs {
        message: "invalid glob".to_string(),
    })
}

fn traversal_limit_exceeded() -> VfsError {
    VfsError::NotSupported {
        message: "durable committed traversal limit exceeded".to_string(),
    }
}

fn durable_read_failed() -> VfsError {
    VfsError::CorruptStore {
        message: DURABLE_COMMITTED_READ_FAILED.to_string(),
    }
}

fn invalid_path() -> VfsError {
    VfsError::InvalidPath {
        path: DURABLE_COMMITTED_PATH.to_string(),
    }
}

fn not_found() -> VfsError {
    VfsError::NotFound {
        path: DURABLE_COMMITTED_PATH.to_string(),
    }
}

fn not_directory() -> VfsError {
    VfsError::NotDirectory {
        path: DURABLE_COMMITTED_PATH.to_string(),
    }
}

fn is_directory() -> VfsError {
    VfsError::IsDirectory {
        path: DURABLE_COMMITTED_PATH.to_string(),
    }
}

fn permission_denied() -> VfsError {
    VfsError::PermissionDenied {
        path: DURABLE_COMMITTED_PATH.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::DurableCommittedFsReader;
    use crate::auth::session::{Session, SessionScope};
    use crate::backend::{
        CommitRecord, ObjectWrite, RefExpectation, RefUpdate, RepoId, StratumStores,
    };
    use crate::error::VfsError;
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, MAIN_REF, RefName};

    fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            kind,
            id,
            mode,
            uid: 0,
            gid: 0,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        }
    }

    async fn put_object(
        stores: &StratumStores,
        repo_id: &RepoId,
        kind: ObjectKind,
        bytes: Vec<u8>,
    ) -> ObjectId {
        let id = ObjectId::from_bytes(&bytes);
        stores
            .objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id,
                kind,
                bytes,
            })
            .await
            .unwrap();
        id
    }

    async fn seed_commit(
        stores: &StratumStores,
        repo_id: &RepoId,
        root_tree: ObjectId,
        label: &str,
    ) -> CommitId {
        let commit_id = CommitId::from(ObjectId::from_bytes(label.as_bytes()));
        stores
            .commits
            .insert(CommitRecord {
                repo_id: repo_id.clone(),
                id: commit_id,
                root_tree,
                parents: Vec::new(),
                timestamp: 1_725_000_000,
                message: format!("commit {label}"),
                author: "root".to_string(),
                changed_paths: Vec::new(),
            })
            .await
            .unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new(MAIN_REF).unwrap(),
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        commit_id
    }

    #[tokio::test]
    async fn reads_file_stat_list_and_tree_from_durable_main() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let note_bytes = b"durable hello\nTODO matched\n".to_vec();
        let note_id = put_object(&stores, &repo_id, ObjectKind::Blob, note_bytes.clone()).await;
        let nested_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"nested durable".to_vec(),
        )
        .await;
        let nested_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "nested.txt",
                    TreeEntryKind::Blob,
                    nested_id,
                    0o644,
                )],
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("docs", TreeEntryKind::Tree, nested_tree_id, 0o755),
                    tree_entry("notes.txt", TreeEntryKind::Blob, note_id, 0o644),
                ],
            }
            .serialize(),
        )
        .await;
        seed_commit(&stores, &repo_id, root_tree_id, "first durable").await;

        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );

        let (content, stat) = reader
            .cat_with_stat_as("/notes.txt", &Session::root())
            .await
            .unwrap();
        assert_eq!(content, note_bytes);
        assert_eq!(stat.kind, "file");
        assert_eq!(stat.size, 27);
        assert_eq!(stat.mode, 0o644);
        assert_eq!(stat.modified, 1_725_000_000);
        assert_eq!(
            stat.content_hash,
            Some(format!("sha256:{}", note_id.to_hex()))
        );

        let docs_stat = reader.stat_as("/docs", &Session::root()).await.unwrap();
        assert_eq!(docs_stat.kind, "directory");
        assert_eq!(docs_stat.size, 1);

        let entries = reader.ls_as(Some("/"), &Session::root()).await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["docs", "notes.txt"]
        );
        assert!(entries[0].is_dir);
        assert_eq!(entries[1].size, 27);

        let tree = reader.tree_as(Some("/"), &Session::root()).await.unwrap();
        assert_eq!(
            tree,
            ".\n\u{251c}\u{2500}\u{2500} docs/\n\u{2502}   \u{2514}\u{2500}\u{2500} nested.txt\n\u{2514}\u{2500}\u{2500} notes.txt\n"
        );
    }

    #[tokio::test]
    async fn find_and_grep_traverse_committed_tree_with_permissions() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let public_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"visible TODO\n".to_vec(),
        )
        .await;
        let secret_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"hidden TODO\n".to_vec(),
        )
        .await;
        let mut secret_entry = tree_entry("secret.txt", TreeEntryKind::Blob, secret_id, 0o600);
        secret_entry.uid = 42;
        secret_entry.gid = 42;
        let root_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("public.txt", TreeEntryKind::Blob, public_id, 0o644),
                    secret_entry,
                ],
            }
            .serialize(),
        )
        .await;
        seed_commit(&stores, &repo_id, root_tree_id, "permission durable").await;
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        let alice = Session::new(1000, 1000, vec![1000], "alice".to_string());

        let found = reader.find_as(Some("/"), None, &alice).await.unwrap();
        assert_eq!(found, vec!["/public.txt"]);

        let grep = reader
            .grep_as("TODO", Some("/"), true, &alice)
            .await
            .unwrap();
        assert_eq!(grep.len(), 1);
        assert_eq!(grep[0].file, "/public.txt");
        assert_eq!(grep[0].line, "visible TODO");
    }

    #[tokio::test]
    async fn find_treats_glob_metacharacters_as_literal_names() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let plus_id = put_object(&stores, &repo_id, ObjectKind::Blob, b"plus match".to_vec()).await;
        let plain_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"plain mismatch".to_vec(),
        )
        .await;
        let root_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("a+b.txt", TreeEntryKind::Blob, plus_id, 0o644),
                    tree_entry("ab.txt", TreeEntryKind::Blob, plain_id, 0o644),
                ],
            }
            .serialize(),
        )
        .await;
        seed_commit(&stores, &repo_id, root_tree_id, "glob durable").await;
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );

        let found = reader
            .find_as(Some("/"), Some("a+b.txt"), &Session::root())
            .await
            .unwrap();
        assert_eq!(found, vec!["/a+b.txt"]);
    }

    #[tokio::test]
    async fn direct_grep_requires_read_permission_for_symlink_entries() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let target_id =
            put_object(&stores, &repo_id, ObjectKind::Blob, b"/public.txt".to_vec()).await;
        let mut link = tree_entry("private-link", TreeEntryKind::Symlink, target_id, 0o000);
        link.uid = 42;
        link.gid = 42;
        let root_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![link],
            }
            .serialize(),
        )
        .await;
        seed_commit(&stores, &repo_id, root_tree_id, "symlink durable").await;
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        let alice = Session::new(1000, 1000, vec![1000], "alice".to_string());

        let error = reader
            .grep_as("TODO", Some("/private-link"), false, &alice)
            .await
            .expect_err("unreadable symlink grep should fail");
        assert!(matches!(error, VfsError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn scoped_traversals_filter_children_by_backing_path_scope() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let allowed_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Blob,
            b"allowed TODO".to_vec(),
        )
        .await;
        let hidden_id =
            put_object(&stores, &repo_id, ObjectKind::Blob, b"hidden TODO".to_vec()).await;
        let allowed_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry(
                    "allowed.txt",
                    TreeEntryKind::Blob,
                    allowed_id,
                    0o644,
                )],
            }
            .serialize(),
        )
        .await;
        let root_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![
                    tree_entry("allowed", TreeEntryKind::Tree, allowed_tree_id, 0o755),
                    tree_entry("hidden.txt", TreeEntryKind::Blob, hidden_id, 0o644),
                ],
            }
            .serialize(),
        )
        .await;
        seed_commit(&stores, &repo_id, root_tree_id, "scope durable").await;
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );
        let scoped = Session::root()
            .with_scope(SessionScope::new(["/allowed"], std::iter::empty::<&str>()).unwrap());

        let found = reader
            .find_as(Some("/allowed"), None, &scoped)
            .await
            .unwrap();
        assert_eq!(found, vec!["/allowed/allowed.txt"]);
        let grep = reader
            .grep_as("TODO", Some("/allowed"), true, &scoped)
            .await
            .unwrap();
        assert_eq!(grep.len(), 1);
        assert_eq!(grep[0].file, "/allowed/allowed.txt");
    }

    #[tokio::test]
    async fn read_errors_are_redacted_for_missing_or_corrupt_objects() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let corrupt_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            b"postgres://secret@example.test/tree-bytes".to_vec(),
        )
        .await;
        seed_commit(
            &stores,
            &repo_id,
            corrupt_tree_id,
            "corrupt durable private-token",
        )
        .await;
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );

        let error = reader
            .ls_as(Some("/"), &Session::root())
            .await
            .expect_err("bad tree bytes should fail");
        let rendered = error.to_string();
        assert!(rendered.contains("durable committed read failed"));
        for forbidden in [
            &corrupt_tree_id.to_hex(),
            "postgres://secret",
            "example.test",
            "private-token",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "durable read error leaked {forbidden:?}: {rendered}"
            );
        }
    }

    #[tokio::test]
    async fn read_errors_are_redacted_for_invalid_tree_entry_names() {
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let blob_id = put_object(&stores, &repo_id, ObjectKind::Blob, b"secret".to_vec()).await;
        let root_tree_id = put_object(
            &stores,
            &repo_id,
            ObjectKind::Tree,
            TreeObject {
                entries: vec![tree_entry("../secret", TreeEntryKind::Blob, blob_id, 0o644)],
            }
            .serialize(),
        )
        .await;
        seed_commit(
            &stores,
            &repo_id,
            root_tree_id,
            "invalid durable private-token",
        )
        .await;
        let reader = DurableCommittedFsReader::new(
            &repo_id,
            stores.refs.as_ref(),
            stores.commits.as_ref(),
            stores.objects.as_ref(),
        );

        let error = reader
            .tree_as(Some("/"), &Session::root())
            .await
            .expect_err("invalid durable tree names should fail closed");
        let rendered = error.to_string();
        assert!(rendered.contains("durable committed read failed"));
        assert!(!rendered.contains("../secret"));
        assert!(!rendered.contains("private-token"));
    }
}
