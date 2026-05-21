use super::{
    CHUNK_HYDRATION_FAILED, CHUNK_SIZE, CacheViewIdentity, CachedChunk, CachedDentry, CachedInode,
    CachedNodeKind, CachedStatfs, CachedSymlink, HydrationJob, HydrationJobScope,
    HydrationJobState, HydrationJobTarget, SparseCache, TREE_HYDRATION_FAILED,
    expected_chunk_offset, normalize_cache_path,
};
use crate::backend::{CommitRecord, StratumStores};
use crate::error::VfsError;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{CommitId, RefName};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const HYDRATION_FAILED: &str = "sparse cache hydration failed";
const ROOT_INODE_ID: u64 = 1;
const ROOT_DIRECTORY_MODE: u32 = 0o40755;

#[derive(Clone, PartialEq, Eq)]
pub struct HydrationViewRequest {
    pub repo_id: crate::backend::RepoId,
    pub root_tree_id: ObjectId,
    pub commit_id: Option<CommitId>,
    pub ref_name: Option<RefName>,
    pub ref_version: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HydrationRunConfig {
    pub max_jobs_per_tick: u64,
    pub now_unix_nanos: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HydrationRunSummary {
    pub view_id: i64,
    pub claimed_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
}

impl fmt::Debug for HydrationViewRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HydrationViewRequest")
            .field("repo_id", &"<redacted>")
            .field("root_tree_id", &"<redacted>")
            .field("commit_id_present", &self.commit_id.is_some())
            .field("ref_name_present", &self.ref_name.is_some())
            .field("ref_version", &self.ref_version)
            .finish()
    }
}

pub async fn hydrate_view_once(
    cache: &SparseCache,
    stores: &StratumStores,
    request: HydrationViewRequest,
    config: HydrationRunConfig,
) -> Result<HydrationRunSummary, VfsError> {
    verify_view_request(stores, &request).await?;

    let view_id = cache
        .insert_view(&CacheViewIdentity {
            repo_id: request.repo_id.clone(),
            root_tree_id: request.root_tree_id,
            commit_id: request.commit_id,
            ref_name: request.ref_name.clone(),
            ref_version: request.ref_version,
        })
        .map_err(|_| hydration_error())?;

    cache
        .enqueue_hydration_job(
            &HydrationJobTarget {
                view_id,
                scope: HydrationJobScope::Tree,
                object_id: request.root_tree_id,
                object_kind: ObjectKind::Tree,
                chunk_index: None,
                path: "/".to_string(),
            },
            config.now_unix_nanos,
        )
        .map_err(|_| hydration_error())?;

    let jobs = cache
        .claim_hydration_jobs_for_view(view_id, config.max_jobs_per_tick, config.now_unix_nanos)
        .map_err(|_| hydration_error())?;
    let mut completed_jobs = 0;
    let mut failed_jobs = 0;

    for job in &jobs {
        let result = match job.target.scope {
            HydrationJobScope::Tree => hydrate_tree_job(cache, stores, &request, job, config).await,
            HydrationJobScope::Chunk => {
                hydrate_chunk_job(cache, stores, &request, job, config).await
            }
        };
        match result {
            Ok(()) => {
                cache
                    .complete_hydration_job(job.job_id, config.now_unix_nanos)
                    .map_err(|_| hydration_error())?;
                completed_jobs += 1;
            }
            Err(code) => {
                cache
                    .fail_hydration_job(
                        job.job_id,
                        HydrationJobState::Failed,
                        code,
                        None,
                        config.now_unix_nanos,
                    )
                    .map_err(|_| hydration_error())?;
                failed_jobs += 1;
            }
        }
    }

    Ok(HydrationRunSummary {
        view_id,
        claimed_jobs: jobs.len() as u64,
        completed_jobs,
        failed_jobs,
    })
}

pub fn stable_hydration_inode_id(object_id: ObjectId, object_kind: ObjectKind) -> u64 {
    let node_kind = match object_kind {
        ObjectKind::Blob | ObjectKind::Commit => CachedNodeKind::File,
        ObjectKind::Tree => CachedNodeKind::Directory,
    };
    stable_hydration_node_inode_id(object_id, node_kind)
}

fn stable_hydration_node_inode_id(object_id: ObjectId, node_kind: CachedNodeKind) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&object_id.as_bytes()[..8]);
    let kind_tag = match node_kind {
        CachedNodeKind::File => 0x11_u64,
        CachedNodeKind::Directory => 0x22_u64,
        CachedNodeKind::Symlink => 0x33_u64,
    };
    let mut inode_id = (u64::from_be_bytes(bytes) ^ (kind_tag << 56)) & (i64::MAX as u64);
    if inode_id == 0 || inode_id == ROOT_INODE_ID {
        inode_id = kind_tag << 48;
    }
    inode_id
}

async fn verify_view_request(
    stores: &StratumStores,
    request: &HydrationViewRequest,
) -> Result<CommitRecord, VfsError> {
    let commit_id = match (
        request.commit_id,
        request.ref_name.as_ref(),
        request.ref_version,
    ) {
        (Some(commit_id), None, None) => commit_id,
        (Some(commit_id), Some(ref_name), Some(ref_version)) => {
            let live_ref = stores
                .refs
                .get(&request.repo_id, ref_name)
                .await
                .map_err(|_| hydration_error())?
                .ok_or_else(hydration_error)?;
            if live_ref.version.value() != ref_version || live_ref.target != commit_id {
                return Err(hydration_error());
            }
            commit_id
        }
        (None, Some(ref_name), Some(ref_version)) => {
            let live_ref = stores
                .refs
                .get(&request.repo_id, ref_name)
                .await
                .map_err(|_| hydration_error())?
                .ok_or_else(hydration_error)?;
            if live_ref.version.value() != ref_version {
                return Err(hydration_error());
            }
            live_ref.target
        }
        _ => return Err(hydration_error()),
    };

    let commit = stores
        .commits
        .get(&request.repo_id, commit_id)
        .await
        .map_err(|_| hydration_error())?
        .ok_or_else(hydration_error)?;
    if commit.repo_id != request.repo_id
        || commit.id != commit_id
        || commit.root_tree != request.root_tree_id
    {
        return Err(hydration_error());
    }
    Ok(commit)
}

async fn hydrate_tree_job(
    cache: &SparseCache,
    stores: &StratumStores,
    request: &HydrationViewRequest,
    job: &HydrationJob,
    config: HydrationRunConfig,
) -> Result<(), &'static str> {
    let tree = load_tree(stores, request, job.target.object_id).await?;
    let path = normalize_cache_path(&job.target.path).map_err(|_| TREE_HYDRATION_FAILED)?;
    let directory_inode_id = if path == "/" {
        ROOT_INODE_ID
    } else {
        stable_hydration_node_inode_id(job.target.object_id, CachedNodeKind::Directory)
    };
    let tree_counts = TreeScopeCounts::from_entries(&tree.entries);

    cache
        .put_inode(&CachedInode {
            view_id: job.target.view_id,
            inode_id: directory_inode_id,
            node_kind: CachedNodeKind::Directory,
            object_id: Some(job.target.object_id),
            object_kind: Some(ObjectKind::Tree),
            mode: if path == "/" {
                ROOT_DIRECTORY_MODE
            } else {
                0o40755
            },
            uid: 0,
            gid: 0,
            nlink: 2,
            size: 0,
            block_size: u64::from(CHUNK_SIZE),
            blocks: 0,
            mtime_secs: 0,
            mtime_nanos: 0,
            ctime_secs: 0,
            ctime_nanos: 0,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
            lookup_count: 0,
        })
        .map_err(|_| TREE_HYDRATION_FAILED)?;

    let mut enqueued_chunks = BTreeSet::new();
    let mut written_inodes = BTreeSet::new();
    let mut bytes_used = 0;
    for entry in &tree.entries {
        hydrate_tree_entry(
            cache,
            stores,
            request,
            job,
            config,
            directory_inode_id,
            &path,
            &tree_counts,
            &mut enqueued_chunks,
            &mut written_inodes,
            &mut bytes_used,
            entry,
        )
        .await?;
    }

    cache
        .put_statfs(&CachedStatfs {
            view_id: job.target.view_id,
            inode_count: tree_counts.inode_count(),
            file_count: tree_counts.file_count(),
            directory_count: tree_counts.directory_count(),
            symlink_count: tree_counts.symlink_count(),
            bytes_used,
            blocks_used: blocks_for_size(bytes_used),
            block_size: u64::from(CHUNK_SIZE),
        })
        .map_err(|_| TREE_HYDRATION_FAILED)?;

    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "keeps one tree-entry write path local to the hydrator"
)]
async fn hydrate_tree_entry(
    cache: &SparseCache,
    stores: &StratumStores,
    request: &HydrationViewRequest,
    job: &HydrationJob,
    config: HydrationRunConfig,
    directory_inode_id: u64,
    parent_path: &str,
    tree_counts: &TreeScopeCounts,
    enqueued_chunks: &mut BTreeSet<ObjectId>,
    written_inodes: &mut BTreeSet<u64>,
    bytes_used: &mut u64,
    entry: &TreeEntry,
) -> Result<(), &'static str> {
    let child_path = child_path(parent_path, &entry.name)?;
    let (node_kind, object_kind) = match entry.kind {
        TreeEntryKind::Blob => (CachedNodeKind::File, ObjectKind::Blob),
        TreeEntryKind::Tree => (CachedNodeKind::Directory, ObjectKind::Tree),
        TreeEntryKind::Symlink => (CachedNodeKind::Symlink, ObjectKind::Blob),
    };
    let inode_id = stable_hydration_node_inode_id(entry.id, node_kind);
    let symlink_target = match entry.kind {
        TreeEntryKind::Symlink => Some(load_symlink_target(stores, request, entry.id).await?),
        TreeEntryKind::Blob | TreeEntryKind::Tree => None,
    };
    let size = match (&entry.kind, symlink_target.as_ref()) {
        (TreeEntryKind::Blob, _) => object_len(stores, request, entry.id).await?,
        (TreeEntryKind::Tree, _) => 0,
        (TreeEntryKind::Symlink, Some(target)) => target.len() as u64,
        (TreeEntryKind::Symlink, None) => return Err(TREE_HYDRATION_FAILED),
    };

    let is_new_inode = written_inodes.insert(inode_id);
    if is_new_inode {
        if entry.kind != TreeEntryKind::Tree {
            *bytes_used = bytes_used.checked_add(size).ok_or(TREE_HYDRATION_FAILED)?;
        }
        cache
            .put_inode(&CachedInode {
                view_id: job.target.view_id,
                inode_id,
                node_kind,
                object_id: Some(entry.id),
                object_kind: Some(object_kind),
                mode: u32::from(entry.mode),
                uid: entry.uid,
                gid: entry.gid,
                nlink: tree_counts.nlink(entry),
                size,
                block_size: u64::from(CHUNK_SIZE),
                blocks: blocks_for_size(size),
                mtime_secs: 0,
                mtime_nanos: 0,
                ctime_secs: 0,
                ctime_nanos: 0,
                mime_type: entry.mime_type.clone(),
                custom_attrs: entry.custom_attrs.clone(),
                lookup_count: 0,
            })
            .map_err(|_| TREE_HYDRATION_FAILED)?;
    }
    if let Some(target) = symlink_target
        && cache
            .get_symlink(job.target.view_id, inode_id)
            .map_err(|_| TREE_HYDRATION_FAILED)?
            .is_none()
    {
        cache
            .put_symlink(&CachedSymlink {
                view_id: job.target.view_id,
                inode_id,
                target,
                target_object_id: Some(entry.id),
            })
            .map_err(|_| TREE_HYDRATION_FAILED)?;
    }
    cache
        .put_dentry(&CachedDentry {
            view_id: job.target.view_id,
            parent_inode_id: directory_inode_id,
            name: entry.name.clone(),
            child_inode_id: inode_id,
            path: child_path.clone(),
        })
        .map_err(|_| TREE_HYDRATION_FAILED)?;

    match entry.kind {
        TreeEntryKind::Tree => {
            cache
                .enqueue_hydration_job(
                    &HydrationJobTarget {
                        view_id: job.target.view_id,
                        scope: HydrationJobScope::Tree,
                        object_id: entry.id,
                        object_kind: ObjectKind::Tree,
                        chunk_index: None,
                        path: child_path,
                    },
                    config.now_unix_nanos,
                )
                .map_err(|_| TREE_HYDRATION_FAILED)?;
        }
        TreeEntryKind::Blob | TreeEntryKind::Symlink if enqueued_chunks.insert(entry.id) => {
            cache
                .enqueue_hydration_job(
                    &HydrationJobTarget {
                        view_id: job.target.view_id,
                        scope: HydrationJobScope::Chunk,
                        object_id: entry.id,
                        object_kind: ObjectKind::Blob,
                        chunk_index: Some(0),
                        path: child_path,
                    },
                    config.now_unix_nanos,
                )
                .map_err(|_| TREE_HYDRATION_FAILED)?;
        }
        TreeEntryKind::Blob | TreeEntryKind::Symlink => {}
    }
    Ok(())
}

async fn load_symlink_target(
    stores: &StratumStores,
    request: &HydrationViewRequest,
    object_id: ObjectId,
) -> Result<String, &'static str> {
    let bytes = load_blob(stores, request, object_id)
        .await
        .map_err(|_| TREE_HYDRATION_FAILED)?;
    String::from_utf8(bytes).map_err(|_| TREE_HYDRATION_FAILED)
}

async fn hydrate_chunk_job(
    cache: &SparseCache,
    stores: &StratumStores,
    request: &HydrationViewRequest,
    job: &HydrationJob,
    _config: HydrationRunConfig,
) -> Result<(), &'static str> {
    let chunk_index = job.target.chunk_index.ok_or(CHUNK_HYDRATION_FAILED)?;
    let bytes = load_blob(stores, request, job.target.object_id).await?;
    let offset = expected_chunk_offset(chunk_index).map_err(|_| CHUNK_HYDRATION_FAILED)?;
    let start = usize::try_from(offset).map_err(|_| CHUNK_HYDRATION_FAILED)?;
    let chunk_len = CHUNK_SIZE as usize;
    let end = start.saturating_add(chunk_len).min(bytes.len());
    let chunk_bytes = if start >= bytes.len() {
        Vec::new()
    } else {
        bytes[start..end].to_vec()
    };
    cache
        .put_chunk(&CachedChunk {
            repo_id: request.repo_id.clone(),
            object_id: job.target.object_id,
            chunk_index,
            offset,
            byte_len: chunk_bytes.len() as u64,
            bytes: chunk_bytes,
        })
        .map_err(|_| CHUNK_HYDRATION_FAILED)?;
    Ok(())
}

async fn load_tree(
    stores: &StratumStores,
    request: &HydrationViewRequest,
    object_id: ObjectId,
) -> Result<TreeObject, &'static str> {
    let object = stores
        .objects
        .get(&request.repo_id, object_id, ObjectKind::Tree)
        .await
        .map_err(|_| TREE_HYDRATION_FAILED)?
        .ok_or(TREE_HYDRATION_FAILED)?;
    if object.repo_id != request.repo_id
        || object.id != object_id
        || object.kind != ObjectKind::Tree
    {
        return Err(TREE_HYDRATION_FAILED);
    }
    TreeObject::deserialize(&object.bytes).map_err(|_| TREE_HYDRATION_FAILED)
}

async fn load_blob(
    stores: &StratumStores,
    request: &HydrationViewRequest,
    object_id: ObjectId,
) -> Result<Vec<u8>, &'static str> {
    let object = stores
        .objects
        .get(&request.repo_id, object_id, ObjectKind::Blob)
        .await
        .map_err(|_| CHUNK_HYDRATION_FAILED)?
        .ok_or(CHUNK_HYDRATION_FAILED)?;
    if object.repo_id != request.repo_id
        || object.id != object_id
        || object.kind != ObjectKind::Blob
    {
        return Err(CHUNK_HYDRATION_FAILED);
    }
    Ok(object.bytes)
}

async fn object_len(
    stores: &StratumStores,
    request: &HydrationViewRequest,
    object_id: ObjectId,
) -> Result<u64, &'static str> {
    stores
        .objects
        .object_len(&request.repo_id, object_id, ObjectKind::Blob)
        .await
        .map_err(|_| TREE_HYDRATION_FAILED)?
        .ok_or(TREE_HYDRATION_FAILED)
}

fn child_path(parent_path: &str, name: &str) -> Result<String, &'static str> {
    let path = if parent_path == "/" {
        format!("/{name}")
    } else {
        format!("{parent_path}/{name}")
    };
    normalize_cache_path(&path).map_err(|_| TREE_HYDRATION_FAILED)
}

fn blocks_for_size(size: u64) -> u64 {
    if size == 0 {
        0
    } else {
        size.div_ceil(u64::from(CHUNK_SIZE))
    }
}

fn hydration_error() -> VfsError {
    VfsError::CorruptStore {
        message: HYDRATION_FAILED.to_string(),
    }
}

struct TreeScopeCounts {
    entry_counts: BTreeMap<(ObjectId, u8), u64>,
    file_objects: BTreeSet<ObjectId>,
    directory_objects: BTreeSet<ObjectId>,
    symlink_objects: BTreeSet<ObjectId>,
}

impl TreeScopeCounts {
    fn from_entries(entries: &[TreeEntry]) -> Self {
        let mut counts = Self {
            entry_counts: BTreeMap::new(),
            file_objects: BTreeSet::new(),
            directory_objects: BTreeSet::new(),
            symlink_objects: BTreeSet::new(),
        };
        for entry in entries {
            *counts
                .entry_counts
                .entry((entry.id, tree_entry_kind_key(entry.kind)))
                .or_default() += 1;
            match entry.kind {
                TreeEntryKind::Blob => {
                    counts.file_objects.insert(entry.id);
                }
                TreeEntryKind::Tree => {
                    counts.directory_objects.insert(entry.id);
                }
                TreeEntryKind::Symlink => {
                    counts.symlink_objects.insert(entry.id);
                }
            }
        }
        counts
    }

    fn nlink(&self, entry: &TreeEntry) -> u64 {
        self.entry_counts
            .get(&(entry.id, tree_entry_kind_key(entry.kind)))
            .copied()
            .unwrap_or(1)
    }

    fn inode_count(&self) -> u64 {
        1 + self.entry_counts.len() as u64
    }

    fn file_count(&self) -> u64 {
        self.file_objects.len() as u64
    }

    fn directory_count(&self) -> u64 {
        1 + self.directory_objects.len() as u64
    }

    fn symlink_count(&self) -> u64 {
        self.symlink_objects.len() as u64
    }
}

fn tree_entry_kind_key(kind: TreeEntryKind) -> u8 {
    match kind {
        TreeEntryKind::Blob => 1,
        TreeEntryKind::Tree => 2,
        TreeEntryKind::Symlink => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HydrationRunConfig, HydrationViewRequest, hydrate_view_once, stable_hydration_inode_id,
        stable_hydration_node_inode_id,
    };
    use crate::backend::{
        CommitRecord, ObjectWrite, RefExpectation, RefUpdate, RepoId, StratumStores,
    };
    use crate::error::VfsError;
    use crate::sparse_cache::{
        CacheViewIdentity, CachedNodeKind, HydrationJobScope, HydrationJobTarget, SparseCache,
    };
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, RefName};
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn hydrates_tree_entries_inodes_symlinks_and_statfs_from_durable_view()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let readme_bytes = b"durable readme\n".to_vec();
        let link_target = b"README.md".to_vec();
        let readme_id = put_object(&stores, &repo_id, ObjectKind::Blob, readme_bytes).await;
        let link_id = put_object(&stores, &repo_id, ObjectKind::Blob, link_target.clone()).await;
        let src_tree_id = put_tree(&stores, &repo_id, TreeObject { entries: vec![] }).await;
        let root_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![
                    tree_entry("README.md", TreeEntryKind::Blob, readme_id, 0o100644),
                    tree_entry("README-again.md", TreeEntryKind::Blob, readme_id, 0o100644),
                    tree_entry("latest", TreeEntryKind::Symlink, link_id, 0o120777),
                    tree_entry("src", TreeEntryKind::Tree, src_tree_id, 0o40755),
                ],
            },
        )
        .await;
        let commit_id = seed_commit(&stores, &repo_id, root_tree_id, "hydration-tree").await;

        let summary = hydrate_view_once(
            &cache,
            &stores,
            HydrationViewRequest {
                repo_id: repo_id.clone(),
                root_tree_id,
                commit_id: Some(commit_id),
                ref_name: None,
                ref_version: None,
            },
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await?;

        assert_eq!(summary.claimed_jobs, 1);
        assert_eq!(summary.completed_jobs, 1);
        let root = cache.get_inode(summary.view_id, 1)?.unwrap();
        assert_eq!(root.node_kind, CachedNodeKind::Directory);
        let entries = cache.list_dentries(summary.view_id, 1)?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["README-again.md", "README.md", "latest", "src"]
        );
        let readme_inode_id = stable_hydration_inode_id(readme_id, ObjectKind::Blob);
        let readme = cache.get_inode(summary.view_id, readme_inode_id)?.unwrap();
        assert_eq!(readme.nlink, 2);
        assert_eq!(readme.mime_type.as_deref(), Some("text/plain"));
        let link_inode_id = stable_hydration_node_inode_id(link_id, CachedNodeKind::Symlink);
        let symlink = cache.get_symlink(summary.view_id, link_inode_id)?.unwrap();
        assert_eq!(symlink.target, String::from_utf8(link_target).unwrap());
        let statfs = cache.get_statfs(summary.view_id)?.unwrap();
        assert_eq!(statfs.inode_count, 4);
        assert_eq!(statfs.file_count, 1);
        assert_eq!(statfs.directory_count, 2);
        assert_eq!(statfs.symlink_count, 1);
        assert_eq!(statfs.bytes_used, 24);
        assert_eq!(statfs.blocks_used, 1);
        assert_eq!(cache.hydration_progress(summary.view_id)?.pending, 3);

        Ok(())
    }

    #[tokio::test]
    async fn hydrates_blob_chunks_by_repo_object_and_chunk_index() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let blob_bytes = [vec![b'a'; 4096], b"tail".to_vec()].concat();
        let blob_id = put_object(&stores, &repo_id, ObjectKind::Blob, blob_bytes.clone()).await;
        let root_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![tree_entry(
                    "big.bin",
                    TreeEntryKind::Blob,
                    blob_id,
                    0o100644,
                )],
            },
        )
        .await;
        let commit_id = seed_commit(&stores, &repo_id, root_tree_id, "hydration-chunk").await;
        let request = HydrationViewRequest {
            repo_id: repo_id.clone(),
            root_tree_id,
            commit_id: Some(commit_id),
            ref_name: None,
            ref_version: None,
        };
        hydrate_view_once(
            &cache,
            &stores,
            request.clone(),
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await?;

        let summary = hydrate_view_once(
            &cache,
            &stores,
            request,
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 200,
            },
        )
        .await?;

        let chunk = cache.get_chunk(&repo_id, blob_id, 0)?.unwrap();
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(chunk.offset, 0);
        assert_eq!(chunk.byte_len, 4096);
        assert_eq!(chunk.bytes, blob_bytes[..4096].to_vec());
        assert_eq!(cache.get_chunk(&repo_id, blob_id, 1)?, None);
        assert_eq!(cache.hydration_progress(summary.view_id)?.completed, 2);

        Ok(())
    }

    #[tokio::test]
    async fn hydration_keeps_file_and_symlink_inodes_distinct_for_same_blob_object()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let shared_id =
            put_object(&stores, &repo_id, ObjectKind::Blob, b"target.txt".to_vec()).await;
        let root_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![
                    tree_entry("as-file", TreeEntryKind::Blob, shared_id, 0o100644),
                    tree_entry("as-link", TreeEntryKind::Symlink, shared_id, 0o120777),
                ],
            },
        )
        .await;
        let commit_id = seed_commit(&stores, &repo_id, root_tree_id, "mixed-node-kinds").await;

        let summary = hydrate_view_once(
            &cache,
            &stores,
            HydrationViewRequest {
                repo_id,
                root_tree_id,
                commit_id: Some(commit_id),
                ref_name: None,
                ref_version: None,
            },
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await?;

        let file_inode_id = stable_hydration_node_inode_id(shared_id, CachedNodeKind::File);
        let symlink_inode_id = stable_hydration_node_inode_id(shared_id, CachedNodeKind::Symlink);
        assert_ne!(file_inode_id, symlink_inode_id);
        assert_eq!(
            cache
                .get_inode(summary.view_id, file_inode_id)?
                .unwrap()
                .node_kind,
            CachedNodeKind::File
        );
        assert_eq!(
            cache
                .get_inode(summary.view_id, symlink_inode_id)?
                .unwrap()
                .node_kind,
            CachedNodeKind::Symlink
        );
        assert_eq!(
            cache
                .get_symlink(summary.view_id, symlink_inode_id)?
                .unwrap()
                .target,
            "target.txt"
        );

        Ok(())
    }

    #[tokio::test]
    async fn hydration_rejects_stale_ref_version_without_following_latest_ref()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let old_tree_id = put_tree(&stores, &repo_id, TreeObject { entries: vec![] }).await;
        let new_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![tree_entry(
                    "new.txt",
                    TreeEntryKind::Blob,
                    put_object(&stores, &repo_id, ObjectKind::Blob, b"new".to_vec()).await,
                    0o100644,
                )],
            },
        )
        .await;
        let old_commit_id = seed_commit(&stores, &repo_id, old_tree_id, "old").await;
        let new_commit_id = seed_commit_record(&stores, &repo_id, new_tree_id, "new").await;
        let main = RefName::new("main")?;
        let old_ref = stores.refs.get(&repo_id, &main).await?.unwrap();
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: main.clone(),
                target: new_commit_id,
                expectation: RefExpectation::Matches {
                    target: old_commit_id,
                    version: old_ref.version,
                },
            })
            .await?;

        let err = hydrate_view_once(
            &cache,
            &stores,
            HydrationViewRequest {
                repo_id: repo_id.clone(),
                root_tree_id: old_tree_id,
                commit_id: Some(old_commit_id),
                ref_name: Some(main),
                ref_version: Some(old_ref.version.value()),
            },
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            format!("{err:?}"),
            "CorruptStore { message: \"sparse cache hydration failed\" }"
        );
        let view_id = cache.insert_view(&crate::sparse_cache::CacheViewIdentity {
            repo_id,
            root_tree_id: old_tree_id,
            commit_id: Some(old_commit_id),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(old_ref.version.value()),
        })?;
        assert!(cache.list_dentries(view_id, 1)?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn hydration_failures_are_redacted_and_mark_jobs_failed() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let invalid_target = vec![0xff, 0xfe, b's', b'e', b'c', b'r', b'e', b't'];
        let link_id = put_object(&stores, &repo_id, ObjectKind::Blob, invalid_target).await;
        let root_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![tree_entry(
                    "private-link",
                    TreeEntryKind::Symlink,
                    link_id,
                    0o120777,
                )],
            },
        )
        .await;
        let commit_id = seed_commit(&stores, &repo_id, root_tree_id, "hydration-fails").await;

        let summary = hydrate_view_once(
            &cache,
            &stores,
            HydrationViewRequest {
                repo_id,
                root_tree_id,
                commit_id: Some(commit_id),
                ref_name: None,
                ref_version: None,
            },
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await?;

        let progress = cache.hydration_progress(summary.view_id)?;
        let failed_job = cache.claim_hydration_jobs(1, 200)?;
        assert_eq!(summary.failed_jobs, 1);
        assert_eq!(progress.failed, 1);
        assert_eq!(progress.total_attempts, 1);
        assert!(failed_job.is_empty());
        let debug = format!("{summary:?}");
        assert!(!debug.contains("private-link"));
        assert!(!debug.contains("secret"));

        Ok(())
    }

    #[tokio::test]
    async fn hydration_claims_only_jobs_for_requested_view() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let requested_tree_id = put_tree(&stores, &repo_id, TreeObject { entries: vec![] }).await;
        let other_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![tree_entry(
                    "other.txt",
                    TreeEntryKind::Blob,
                    put_object(&stores, &repo_id, ObjectKind::Blob, b"other".to_vec()).await,
                    0o100644,
                )],
            },
        )
        .await;
        let requested_commit =
            seed_commit(&stores, &repo_id, requested_tree_id, "requested-view").await;
        let other_commit = seed_commit_record(&stores, &repo_id, other_tree_id, "other-view").await;
        let other_view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: repo_id.clone(),
            root_tree_id: other_tree_id,
            commit_id: Some(other_commit),
            ref_name: None,
            ref_version: None,
        })?;
        let other_job_id = cache.enqueue_hydration_job(
            &HydrationJobTarget {
                view_id: other_view_id,
                scope: HydrationJobScope::Tree,
                object_id: other_tree_id,
                object_kind: ObjectKind::Tree,
                chunk_index: None,
                path: "/".to_string(),
            },
            1,
        )?;

        let summary = hydrate_view_once(
            &cache,
            &stores,
            HydrationViewRequest {
                repo_id,
                root_tree_id: requested_tree_id,
                commit_id: Some(requested_commit),
                ref_name: None,
                ref_version: None,
            },
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await?;

        assert_eq!(summary.claimed_jobs, 1);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(cache.hydration_progress(other_view_id)?.pending, 1);
        assert_eq!(
            cache.claim_hydration_jobs_for_view(other_view_id, 1, 200)?[0].job_id,
            other_job_id
        );

        Ok(())
    }

    fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
        TreeEntry {
            name: name.to_string(),
            kind,
            id,
            mode,
            uid: 501,
            gid: 20,
            mime_type: matches!(kind, TreeEntryKind::Blob).then(|| "text/plain".to_string()),
            custom_attrs: BTreeMap::new(),
        }
    }

    async fn put_tree(stores: &StratumStores, repo_id: &RepoId, tree: TreeObject) -> ObjectId {
        put_object(stores, repo_id, ObjectKind::Tree, tree.serialize()).await
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
        let commit_id = seed_commit_record(stores, repo_id, root_tree, label).await;
        stores
            .refs
            .update(RefUpdate {
                repo_id: repo_id.clone(),
                name: RefName::new("main").unwrap(),
                target: commit_id,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .unwrap();
        commit_id
    }

    async fn seed_commit_record(
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
        commit_id
    }
}
