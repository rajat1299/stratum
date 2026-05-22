use super::{
    CHUNK_HYDRATION_FAILED, CHUNK_SIZE, CacheViewIdentity, CachedChunk, CachedDentry, CachedInode,
    CachedNodeKind, CachedSymlink, HydrationJob, HydrationJobScope, HydrationJobState,
    HydrationJobTarget, SparseCache, TREE_HYDRATION_FAILED, expected_chunk_offset,
    normalize_cache_path,
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
                completed_jobs += 1;
            }
            Err(code) => {
                cache
                    .fail_hydration_job(
                        job.job_id,
                        job.attempts,
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

fn stable_hydration_directory_inode_id(path: &str) -> u64 {
    stable_hydration_node_inode_id(
        ObjectId::from_bytes(format!("sparse-cache-directory:{path}").as_bytes()),
        CachedNodeKind::Directory,
    )
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

struct PlannedTreeEntry {
    entry: TreeEntry,
    child_path: String,
    inode_id: u64,
    node_kind: CachedNodeKind,
    object_kind: ObjectKind,
    size: u64,
    symlink_target: Option<String>,
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
        stable_hydration_directory_inode_id(&path)
    };
    let planned_entries = plan_tree_entries(stores, request, &path, &tree.entries).await?;

    cache
        .run_immediate_transaction(|| {
            cache.put_inode(&CachedInode {
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
                size_known: true,
                block_size: u64::from(CHUNK_SIZE),
                blocks: 0,
                mtime_secs: 0,
                mtime_nanos: 0,
                ctime_secs: 0,
                ctime_nanos: 0,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
                lookup_count: 0,
            })?;

            let mut enqueued_chunks = BTreeSet::new();
            let mut written_inodes = BTreeSet::new();
            for entry in &planned_entries {
                write_planned_tree_entry(
                    cache,
                    job,
                    config,
                    directory_inode_id,
                    &mut enqueued_chunks,
                    &mut written_inodes,
                    entry,
                )?;
            }

            cache.refresh_view_metadata(job.target.view_id)?;
            cache.complete_hydration_job(job.job_id, job.attempts, config.now_unix_nanos)
        })
        .map_err(|_| TREE_HYDRATION_FAILED)?;

    Ok(())
}

async fn plan_tree_entries(
    stores: &StratumStores,
    request: &HydrationViewRequest,
    parent_path: &str,
    entries: &[TreeEntry],
) -> Result<Vec<PlannedTreeEntry>, &'static str> {
    validate_tree_entries(entries)?;
    let mut planned = Vec::with_capacity(entries.len());
    for entry in entries {
        let child_path = child_path(parent_path, &entry.name)?;
        let (node_kind, object_kind) = match entry.kind {
            TreeEntryKind::Blob => (CachedNodeKind::File, ObjectKind::Blob),
            TreeEntryKind::Tree => (CachedNodeKind::Directory, ObjectKind::Tree),
            TreeEntryKind::Symlink => (CachedNodeKind::Symlink, ObjectKind::Blob),
        };
        let inode_id = match entry.kind {
            TreeEntryKind::Tree => stable_hydration_directory_inode_id(&child_path),
            TreeEntryKind::Blob | TreeEntryKind::Symlink => {
                stable_hydration_node_inode_id(entry.id, node_kind)
            }
        };
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
        planned.push(PlannedTreeEntry {
            entry: entry.clone(),
            child_path,
            inode_id,
            node_kind,
            object_kind,
            size,
            symlink_target,
        });
    }
    Ok(planned)
}

fn validate_tree_entries(entries: &[TreeEntry]) -> Result<(), &'static str> {
    let mut names = BTreeSet::new();
    for entry in entries {
        if entry.name.is_empty()
            || entry.name == "."
            || entry.name == ".."
            || entry.name.contains('/')
            || entry.name.contains('\0')
            || !names.insert(entry.name.as_str())
        {
            return Err(TREE_HYDRATION_FAILED);
        }
    }
    Ok(())
}

fn write_planned_tree_entry(
    cache: &SparseCache,
    job: &HydrationJob,
    config: HydrationRunConfig,
    directory_inode_id: u64,
    enqueued_chunks: &mut BTreeSet<ObjectId>,
    written_inodes: &mut BTreeSet<u64>,
    planned: &PlannedTreeEntry,
) -> Result<(), VfsError> {
    let entry = &planned.entry;
    let node_kind = match entry.kind {
        TreeEntryKind::Blob => CachedNodeKind::File,
        TreeEntryKind::Tree => CachedNodeKind::Directory,
        TreeEntryKind::Symlink => CachedNodeKind::Symlink,
    };

    let is_new_inode = written_inodes.insert(planned.inode_id);
    if is_new_inode {
        cache.put_inode(&CachedInode {
            view_id: job.target.view_id,
            inode_id: planned.inode_id,
            node_kind: planned.node_kind,
            object_id: Some(entry.id),
            object_kind: Some(planned.object_kind),
            mode: u32::from(entry.mode),
            uid: entry.uid,
            gid: entry.gid,
            nlink: if node_kind == CachedNodeKind::Directory {
                2
            } else {
                1
            },
            size: planned.size,
            size_known: true,
            block_size: u64::from(CHUNK_SIZE),
            blocks: blocks_for_size(planned.size),
            mtime_secs: 0,
            mtime_nanos: 0,
            ctime_secs: 0,
            ctime_nanos: 0,
            mime_type: entry.mime_type.clone(),
            custom_attrs: entry.custom_attrs.clone(),
            lookup_count: 0,
        })?;
    }
    if let Some(target) = planned.symlink_target.clone()
        && cache
            .get_symlink(job.target.view_id, planned.inode_id)?
            .is_none()
    {
        cache.put_symlink(&CachedSymlink {
            view_id: job.target.view_id,
            inode_id: planned.inode_id,
            target,
            target_object_id: Some(entry.id),
        })?;
    }
    cache.put_dentry(&CachedDentry {
        view_id: job.target.view_id,
        parent_inode_id: directory_inode_id,
        name: entry.name.clone(),
        child_inode_id: planned.inode_id,
        path: planned.child_path.clone(),
    })?;

    match entry.kind {
        TreeEntryKind::Tree => {
            cache.enqueue_hydration_job(
                &HydrationJobTarget {
                    view_id: job.target.view_id,
                    scope: HydrationJobScope::Tree,
                    object_id: entry.id,
                    object_kind: ObjectKind::Tree,
                    chunk_index: None,
                    path: planned.child_path.clone(),
                },
                config.now_unix_nanos,
            )?;
        }
        TreeEntryKind::Blob | TreeEntryKind::Symlink if enqueued_chunks.insert(entry.id) => {
            cache.enqueue_hydration_job(
                &HydrationJobTarget {
                    view_id: job.target.view_id,
                    scope: HydrationJobScope::Chunk,
                    object_id: entry.id,
                    object_kind: ObjectKind::Blob,
                    chunk_index: Some(0),
                    path: planned.child_path.clone(),
                },
                config.now_unix_nanos,
            )?;
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
    config: HydrationRunConfig,
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
        .run_immediate_transaction(|| {
            cache.put_chunk(&CachedChunk {
                repo_id: request.repo_id.clone(),
                object_id: job.target.object_id,
                chunk_index,
                offset,
                byte_len: chunk_bytes.len() as u64,
                bytes: chunk_bytes,
            })?;
            cache.complete_hydration_job(job.job_id, job.attempts, config.now_unix_nanos)
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

#[cfg(test)]
mod tests {
    use super::{
        HydrationRunConfig, HydrationViewRequest, hydrate_view_once,
        stable_hydration_directory_inode_id, stable_hydration_inode_id,
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
        assert_eq!(statfs.blocks_used, 2);
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
    async fn hydration_rejects_duplicate_tree_entry_names_without_partial_rows()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let blob_id = put_object(&stores, &repo_id, ObjectKind::Blob, b"duplicate".to_vec()).await;
        let root_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![
                    tree_entry("same.txt", TreeEntryKind::Blob, blob_id, 0o100644),
                    tree_entry("same.txt", TreeEntryKind::Blob, blob_id, 0o100644),
                ],
            },
        )
        .await;
        let commit_id = seed_commit(&stores, &repo_id, root_tree_id, "duplicate-tree").await;

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

        assert_eq!(summary.failed_jobs, 1);
        assert_eq!(cache.hydration_progress(summary.view_id)?.failed, 1);
        assert_eq!(cache.get_inode(summary.view_id, 1)?, None);
        assert!(cache.list_dentries(summary.view_id, 1)?.is_empty());
        assert_eq!(cache.get_statfs(summary.view_id)?, None);

        Ok(())
    }

    #[tokio::test]
    async fn hydration_uses_path_directory_inodes_and_view_wide_metadata() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::local();
        let stores = StratumStores::local_memory();
        let shared_blob_id =
            put_object(&stores, &repo_id, ObjectKind::Blob, b"shared".to_vec()).await;
        let child_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![tree_entry(
                    "shared.txt",
                    TreeEntryKind::Blob,
                    shared_blob_id,
                    0o100644,
                )],
            },
        )
        .await;
        let root_tree_id = put_tree(
            &stores,
            &repo_id,
            TreeObject {
                entries: vec![
                    tree_entry("a", TreeEntryKind::Tree, child_tree_id, 0o40755),
                    tree_entry("b", TreeEntryKind::Tree, child_tree_id, 0o40755),
                ],
            },
        )
        .await;
        let commit_id = seed_commit(&stores, &repo_id, root_tree_id, "aggregate-metadata").await;
        let request = HydrationViewRequest {
            repo_id,
            root_tree_id,
            commit_id: Some(commit_id),
            ref_name: None,
            ref_version: None,
        };

        let root_summary = hydrate_view_once(
            &cache,
            &stores,
            request.clone(),
            HydrationRunConfig {
                max_jobs_per_tick: 1,
                now_unix_nanos: 100,
            },
        )
        .await?;
        let child_summary = hydrate_view_once(
            &cache,
            &stores,
            request.clone(),
            HydrationRunConfig {
                max_jobs_per_tick: 4,
                now_unix_nanos: 200,
            },
        )
        .await?;
        assert_eq!(child_summary.claimed_jobs, 2);
        assert_eq!(child_summary.completed_jobs, 2);
        assert_eq!(child_summary.failed_jobs, 0);

        let a_inode_id = stable_hydration_directory_inode_id("/a");
        let b_inode_id = stable_hydration_directory_inode_id("/b");
        assert_ne!(a_inode_id, b_inode_id);
        assert_eq!(
            cache.list_dentries(root_summary.view_id, a_inode_id)?[0].path,
            "/a/shared.txt"
        );
        assert_eq!(
            cache.list_dentries(root_summary.view_id, b_inode_id)?[0].path,
            "/b/shared.txt"
        );
        let shared_inode_id = stable_hydration_inode_id(shared_blob_id, ObjectKind::Blob);
        assert_eq!(
            cache
                .get_inode(root_summary.view_id, shared_inode_id)?
                .unwrap()
                .nlink,
            2
        );
        let statfs = cache.get_statfs(root_summary.view_id)?.unwrap();
        assert_eq!(statfs.inode_count, 4);
        assert_eq!(statfs.file_count, 1);
        assert_eq!(statfs.directory_count, 3);
        assert_eq!(statfs.symlink_count, 0);
        assert_eq!(statfs.bytes_used, 6);
        assert_eq!(statfs.blocks_used, 1);

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
