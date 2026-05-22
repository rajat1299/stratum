use crate::backend::RepoId;
use crate::error::VfsError;
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{CommitId, RefName};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::time::Duration;

pub mod hydration;
pub mod mount;

const SCHEMA_VERSION: u32 = 3;
const PRE_SIZE_KNOWN_SCHEMA_VERSION: u32 = 2;
const PRE_HYDRATION_SCHEMA_VERSION: u32 = 1;
const CHUNK_SIZE: u32 = 4096;
const MAX_CACHE_PATH_COMPONENT_LEN: usize = 255;
const SPARSE_CACHE_ERROR: &str = "sparse cache operation failed";
const TREE_HYDRATION_FAILED: &str = "tree_hydration_failed";
const CHUNK_HYDRATION_FAILED: &str = "chunk_hydration_failed";
const HYDRATION_POISONED: &str = "hydration_poisoned";

pub struct SparseCache {
    connection: Connection,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CacheViewIdentity {
    pub repo_id: RepoId,
    pub root_tree_id: ObjectId,
    pub commit_id: Option<CommitId>,
    pub ref_name: Option<RefName>,
    pub ref_version: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedNodeKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CachedInode {
    pub view_id: i64,
    pub inode_id: u64,
    pub node_kind: CachedNodeKind,
    pub object_id: Option<ObjectId>,
    pub object_kind: Option<ObjectKind>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u64,
    pub size: u64,
    pub size_known: bool,
    pub block_size: u64,
    pub blocks: u64,
    pub mtime_secs: u64,
    pub mtime_nanos: u32,
    pub ctime_secs: u64,
    pub ctime_nanos: u32,
    pub mime_type: Option<String>,
    pub custom_attrs: BTreeMap<String, String>,
    pub lookup_count: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CachedDentry {
    pub view_id: i64,
    pub parent_inode_id: u64,
    pub name: String,
    pub child_inode_id: u64,
    pub path: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CachedChunk {
    pub repo_id: RepoId,
    pub object_id: ObjectId,
    pub chunk_index: u64,
    pub offset: u64,
    pub byte_len: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CachedSymlink {
    pub view_id: i64,
    pub inode_id: u64,
    pub target: String,
    pub target_object_id: Option<ObjectId>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CachedStatfs {
    pub view_id: i64,
    pub inode_count: u64,
    pub file_count: u64,
    pub directory_count: u64,
    pub symlink_count: u64,
    pub bytes_used: u64,
    pub blocks_used: u64,
    pub block_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationJobScope {
    Tree,
    Chunk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationJobState {
    Pending,
    Running,
    Completed,
    Failed,
    Backoff,
    Poisoned,
}

#[derive(Clone, PartialEq, Eq)]
pub struct HydrationJobTarget {
    pub view_id: i64,
    pub scope: HydrationJobScope,
    pub object_id: ObjectId,
    pub object_kind: ObjectKind,
    pub chunk_index: Option<u64>,
    pub path: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct HydrationJob {
    pub job_id: i64,
    pub target: HydrationJobTarget,
    pub state: HydrationJobState,
    pub attempts: u64,
    pub created_at_unix_nanos: u64,
    pub updated_at_unix_nanos: u64,
    pub next_run_at_unix_nanos: Option<u64>,
    pub completed_at_unix_nanos: Option<u64>,
    pub last_error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HydrationProgress {
    pub pending: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub backoff: u64,
    pub poisoned: u64,
    pub total_attempts: u64,
}

impl fmt::Debug for CacheViewIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CacheViewIdentity")
            .field("repo_id", &"<redacted>")
            .field("root_tree_id", &"<redacted>")
            .field("commit_id_present", &self.commit_id.is_some())
            .field("ref_name_present", &self.ref_name.is_some())
            .field("ref_version", &self.ref_version)
            .finish()
    }
}

impl fmt::Debug for CachedInode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedInode")
            .field("view_id", &self.view_id)
            .field("inode_id", &self.inode_id)
            .field("node_kind", &self.node_kind)
            .field("object_id_present", &self.object_id.is_some())
            .field("object_kind", &self.object_kind)
            .field("mode", &self.mode)
            .field("uid", &self.uid)
            .field("gid", &self.gid)
            .field("nlink", &self.nlink)
            .field("size", &self.size)
            .field("size_known", &self.size_known)
            .field("block_size", &self.block_size)
            .field("blocks", &self.blocks)
            .field("mtime_secs", &self.mtime_secs)
            .field("mtime_nanos", &self.mtime_nanos)
            .field("ctime_secs", &self.ctime_secs)
            .field("ctime_nanos", &self.ctime_nanos)
            .field("mime_type_present", &self.mime_type.is_some())
            .field("custom_attr_count", &self.custom_attrs.len())
            .field("lookup_count", &self.lookup_count)
            .finish()
    }
}

impl fmt::Debug for CachedDentry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedDentry")
            .field("view_id", &self.view_id)
            .field("parent_inode_id", &self.parent_inode_id)
            .field("name", &"<redacted>")
            .field("child_inode_id", &self.child_inode_id)
            .field("path", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for CachedChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedChunk")
            .field("repo_id", &"<redacted>")
            .field("object_id", &"<redacted>")
            .field("chunk_index", &self.chunk_index)
            .field("offset", &self.offset)
            .field("byte_len", &self.byte_len)
            .field("bytes_len", &self.bytes.len())
            .finish()
    }
}

impl fmt::Debug for CachedSymlink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedSymlink")
            .field("view_id", &self.view_id)
            .field("inode_id", &self.inode_id)
            .field("target", &"<redacted>")
            .field("target_object_id_present", &self.target_object_id.is_some())
            .finish()
    }
}

impl fmt::Debug for CachedStatfs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedStatfs")
            .field("view_id", &self.view_id)
            .field("inode_count", &self.inode_count)
            .field("file_count", &self.file_count)
            .field("directory_count", &self.directory_count)
            .field("symlink_count", &self.symlink_count)
            .field("bytes_used", &self.bytes_used)
            .field("blocks_used", &self.blocks_used)
            .field("block_size", &self.block_size)
            .finish()
    }
}

impl fmt::Debug for HydrationJobTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HydrationJobTarget")
            .field("view_id", &self.view_id)
            .field("scope", &self.scope)
            .field("object_id", &"<redacted>")
            .field("object_kind", &self.object_kind)
            .field("chunk_index", &self.chunk_index)
            .field("path", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for HydrationJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HydrationJob")
            .field("job_id", &self.job_id)
            .field("target", &self.target)
            .field("state", &self.state)
            .field("attempts", &self.attempts)
            .field("created_at_unix_nanos", &self.created_at_unix_nanos)
            .field("updated_at_unix_nanos", &self.updated_at_unix_nanos)
            .field("next_run_at_unix_nanos", &self.next_run_at_unix_nanos)
            .field("completed_at_unix_nanos", &self.completed_at_unix_nanos)
            .field("last_error_code", &self.last_error_code)
            .finish()
    }
}

impl SparseCache {
    pub fn open(path: &Path) -> Result<Self, VfsError> {
        let connection = Connection::open(path).map_err(|_| sparse_cache_error())?;
        configure_connection(&connection, true)?;
        initialize_schema(connection)
    }

    pub fn open_in_memory() -> Result<Self, VfsError> {
        let connection = Connection::open_in_memory().map_err(|_| sparse_cache_error())?;
        configure_connection(&connection, false)?;
        initialize_schema(connection)
    }

    pub fn schema_version(&self) -> Result<u32, VfsError> {
        self.config_u32("schema_version")
    }

    pub fn chunk_size(&self) -> Result<u32, VfsError> {
        self.config_u32("chunk_size")
    }

    pub fn insert_view(&self, identity: &CacheViewIdentity) -> Result<i64, VfsError> {
        validate_cache_ref_identity(identity.ref_name.as_ref(), identity.ref_version)?;

        let commit_id = identity.commit_id.map(CommitId::to_hex);
        let ref_name = identity.ref_name.as_ref().map(|name| name.as_str());
        let ref_version = identity.ref_version.map(to_i64).transpose()?;

        self.connection
            .execute(
                "INSERT OR IGNORE INTO sparse_cache_views
                (repo_id, root_tree_id, commit_id, ref_name, ref_version)
                VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    identity.repo_id.as_str(),
                    identity.root_tree_id.to_hex(),
                    commit_id,
                    ref_name,
                    ref_version,
                ],
            )
            .map_err(|_| sparse_cache_error())?;

        self.connection
            .query_row(
                "SELECT view_id FROM sparse_cache_views
                WHERE repo_id = ?1
                  AND root_tree_id = ?2
                  AND commit_id IS ?3
                  AND ref_name IS ?4
                  AND ref_version IS ?5",
                params![
                    identity.repo_id.as_str(),
                    identity.root_tree_id.to_hex(),
                    commit_id,
                    ref_name,
                    ref_version,
                ],
                |row| row.get(0),
            )
            .map_err(|_| sparse_cache_error())
    }

    pub fn get_view_identity(&self, view_id: i64) -> Result<CacheViewIdentity, VfsError> {
        self.connection
            .query_row(
                "SELECT repo_id, root_tree_id, commit_id, ref_name, ref_version
                FROM sparse_cache_views
                WHERE view_id = ?1",
                [view_id],
                |row| {
                    let repo_id: String = row.get(0)?;
                    let root_tree_id: String = row.get(1)?;
                    let commit_id: Option<String> = row.get(2)?;
                    let ref_name: Option<String> = row.get(3)?;
                    let ref_version: Option<i64> = row.get(4)?;
                    Ok((repo_id, root_tree_id, commit_id, ref_name, ref_version))
                },
            )
            .map_err(|_| sparse_cache_error())
            .and_then(
                |(repo_id, root_tree_id, commit_id, ref_name, ref_version)| {
                    Ok(CacheViewIdentity {
                        repo_id: RepoId::new(repo_id).map_err(|_| sparse_cache_error())?,
                        root_tree_id: ObjectId::from_hex(&root_tree_id)
                            .map_err(|_| sparse_cache_error())?,
                        commit_id: commit_id
                            .as_deref()
                            .map(object_id_from_hex)
                            .transpose()?
                            .map(CommitId::from),
                        ref_name: ref_name
                            .as_deref()
                            .map(|name| RefName::new(name).map_err(|_| sparse_cache_error()))
                            .transpose()?,
                        ref_version: ref_version.map(u64_from_i64).transpose()?,
                    })
                },
            )
    }

    pub fn put_inode(&self, inode: &CachedInode) -> Result<(), VfsError> {
        validate_inode(inode)?;
        let custom_attrs_json =
            serde_json::to_string(&inode.custom_attrs).map_err(|_| sparse_cache_error())?;
        let object_kind = inode.object_kind.map(object_kind_text);
        self.connection
            .execute(
                "INSERT INTO sparse_cache_inodes
                (view_id, inode_id, node_kind, object_id, object_kind, mode, uid, gid, nlink,
                 size, size_known, block_size, blocks, mtime_secs, mtime_nanos, ctime_secs, ctime_nanos,
                 mime_type, custom_attrs_json, lookup_count)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                        ?17, ?18, ?19, ?20)
                ON CONFLICT(view_id, inode_id) DO UPDATE SET
                    node_kind = excluded.node_kind,
                    object_id = excluded.object_id,
                    object_kind = excluded.object_kind,
                    mode = excluded.mode,
                    uid = excluded.uid,
                    gid = excluded.gid,
                    nlink = excluded.nlink,
                    size = excluded.size,
                    size_known = excluded.size_known,
                    block_size = excluded.block_size,
                    blocks = excluded.blocks,
                    mtime_secs = excluded.mtime_secs,
                    mtime_nanos = excluded.mtime_nanos,
                    ctime_secs = excluded.ctime_secs,
                    ctime_nanos = excluded.ctime_nanos,
                    mime_type = excluded.mime_type,
                    custom_attrs_json = excluded.custom_attrs_json,
                    lookup_count = excluded.lookup_count",
                params![
                    inode.view_id,
                    to_i64(inode.inode_id)?,
                    node_kind_text(inode.node_kind),
                    inode.object_id.map(|id| id.to_hex()),
                    object_kind,
                    i64::from(inode.mode),
                    i64::from(inode.uid),
                    i64::from(inode.gid),
                    to_i64(inode.nlink)?,
                    to_i64(inode.size)?,
                    inode.size_known,
                    to_i64(inode.block_size)?,
                    to_i64(inode.blocks)?,
                    to_i64(inode.mtime_secs)?,
                    i64::from(inode.mtime_nanos),
                    to_i64(inode.ctime_secs)?,
                    i64::from(inode.ctime_nanos),
                    inode.mime_type,
                    custom_attrs_json,
                    to_i64(inode.lookup_count)?,
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn get_inode(&self, view_id: i64, inode_id: u64) -> Result<Option<CachedInode>, VfsError> {
        let inode = self
            .connection
            .query_row(
                "SELECT view_id, inode_id, node_kind, object_id, object_kind, mode, uid, gid,
                        nlink, size, size_known, block_size, blocks, mtime_secs, mtime_nanos,
                        ctime_secs, ctime_nanos, mime_type, custom_attrs_json, lookup_count
                FROM sparse_cache_inodes
                WHERE view_id = ?1 AND inode_id = ?2",
                params![view_id, to_i64(inode_id)?],
                inode_from_row,
            )
            .optional()
            .map_err(|_| sparse_cache_error())?;
        inode.transpose()
    }

    pub fn put_dentry(&self, dentry: &CachedDentry) -> Result<(), VfsError> {
        validate_dentry_name(&dentry.name)?;
        let path = normalize_cache_path(&dentry.path)?;
        self.connection
            .execute(
                "INSERT OR REPLACE INTO sparse_cache_dentries
                (view_id, parent_inode_id, name, child_inode_id, path)
                VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    dentry.view_id,
                    to_i64(dentry.parent_inode_id)?,
                    dentry.name,
                    to_i64(dentry.child_inode_id)?,
                    path,
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn list_dentries(
        &self,
        view_id: i64,
        parent_inode_id: u64,
    ) -> Result<Vec<CachedDentry>, VfsError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT view_id, parent_inode_id, name, child_inode_id, path
                FROM sparse_cache_dentries
                WHERE view_id = ?1 AND parent_inode_id = ?2
                ORDER BY name",
            )
            .map_err(|_| sparse_cache_error())?;
        let entries = statement
            .query_map(params![view_id, to_i64(parent_inode_id)?], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(|_| sparse_cache_error())?
            .map(|entry| {
                let (view_id, parent_inode_id, name, child_inode_id, path) =
                    entry.map_err(|_| sparse_cache_error())?;
                Ok(CachedDentry {
                    view_id,
                    parent_inode_id: u64_from_i64(parent_inode_id)?,
                    name,
                    child_inode_id: u64_from_i64(child_inode_id)?,
                    path,
                })
            })
            .collect::<Result<Vec<_>, VfsError>>()?;
        Ok(entries)
    }

    pub fn put_chunk(&self, chunk: &CachedChunk) -> Result<(), VfsError> {
        validate_chunk(chunk)?;
        self.connection
            .execute(
                "INSERT OR REPLACE INTO sparse_cache_chunks
                (repo_id, object_id, chunk_index, offset, byte_len, bytes)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    chunk.repo_id.as_str(),
                    chunk.object_id.to_hex(),
                    to_i64(chunk.chunk_index)?,
                    to_i64(chunk.offset)?,
                    to_i64(chunk.byte_len)?,
                    &chunk.bytes,
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn get_chunk(
        &self,
        repo_id: &RepoId,
        object_id: ObjectId,
        chunk_index: u64,
    ) -> Result<Option<CachedChunk>, VfsError> {
        let chunk = self
            .connection
            .query_row(
                "SELECT repo_id, object_id, chunk_index, offset, byte_len, bytes
                FROM sparse_cache_chunks
                WHERE repo_id = ?1 AND object_id = ?2 AND chunk_index = ?3",
                params![repo_id.as_str(), object_id.to_hex(), to_i64(chunk_index)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| sparse_cache_error())?;
        chunk
            .map(
                |(repo_id, object_id, chunk_index, offset, byte_len, bytes)| {
                    let chunk = CachedChunk {
                        repo_id: RepoId::new(repo_id).map_err(|_| sparse_cache_error())?,
                        object_id: object_id_from_hex(&object_id)?,
                        chunk_index: u64_from_i64(chunk_index)?,
                        offset: u64_from_i64(offset)?,
                        byte_len: u64_from_i64(byte_len)?,
                        bytes,
                    };
                    validate_chunk(&chunk)?;
                    Ok(chunk)
                },
            )
            .transpose()
    }

    pub fn put_symlink(&self, symlink: &CachedSymlink) -> Result<(), VfsError> {
        self.connection
            .execute(
                "INSERT OR REPLACE INTO sparse_cache_symlinks
                (view_id, inode_id, target, target_object_id)
                VALUES (?1, ?2, ?3, ?4)",
                params![
                    symlink.view_id,
                    to_i64(symlink.inode_id)?,
                    symlink.target,
                    symlink.target_object_id.map(|id| id.to_hex()),
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn get_symlink(
        &self,
        view_id: i64,
        inode_id: u64,
    ) -> Result<Option<CachedSymlink>, VfsError> {
        let symlink = self
            .connection
            .query_row(
                "SELECT view_id, inode_id, target, target_object_id
                FROM sparse_cache_symlinks
                WHERE view_id = ?1 AND inode_id = ?2",
                params![view_id, to_i64(inode_id)?],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| sparse_cache_error())?;
        symlink
            .map(|(view_id, inode_id, target, target_object_id)| {
                Ok(CachedSymlink {
                    view_id,
                    inode_id: u64_from_i64(inode_id)?,
                    target,
                    target_object_id: target_object_id
                        .as_deref()
                        .map(object_id_from_hex)
                        .transpose()?,
                })
            })
            .transpose()
    }

    pub fn put_statfs(&self, statfs: &CachedStatfs) -> Result<(), VfsError> {
        if statfs.block_size == 0 {
            return Err(VfsError::InvalidArgs {
                message: "sparse cache statfs block size must be positive".to_string(),
            });
        }
        self.connection
            .execute(
                "INSERT OR REPLACE INTO sparse_cache_statfs
                (view_id, inode_count, file_count, directory_count, symlink_count, bytes_used,
                 blocks_used, block_size)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    statfs.view_id,
                    to_i64(statfs.inode_count)?,
                    to_i64(statfs.file_count)?,
                    to_i64(statfs.directory_count)?,
                    to_i64(statfs.symlink_count)?,
                    to_i64(statfs.bytes_used)?,
                    to_i64(statfs.blocks_used)?,
                    to_i64(statfs.block_size)?,
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn get_statfs(&self, view_id: i64) -> Result<Option<CachedStatfs>, VfsError> {
        let statfs = self
            .connection
            .query_row(
                "SELECT view_id, inode_count, file_count, directory_count, symlink_count,
                        bytes_used, blocks_used, block_size
                FROM sparse_cache_statfs
                WHERE view_id = ?1",
                [view_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| sparse_cache_error())?;
        statfs
            .map(
                |(
                    view_id,
                    inode_count,
                    file_count,
                    directory_count,
                    symlink_count,
                    bytes_used,
                    blocks_used,
                    block_size,
                )| {
                    Ok(CachedStatfs {
                        view_id,
                        inode_count: u64_from_i64(inode_count)?,
                        file_count: u64_from_i64(file_count)?,
                        directory_count: u64_from_i64(directory_count)?,
                        symlink_count: u64_from_i64(symlink_count)?,
                        bytes_used: u64_from_i64(bytes_used)?,
                        blocks_used: u64_from_i64(blocks_used)?,
                        block_size: u64_from_i64(block_size)?,
                    })
                },
            )
            .transpose()
    }

    pub fn record_lookup(&self, view_id: i64, inode_id: u64) -> Result<(), VfsError> {
        self.connection
            .execute(
                "UPDATE sparse_cache_inodes
                SET lookup_count = CASE
                    WHEN lookup_count < ?3 THEN lookup_count + 1
                    ELSE lookup_count
                END
                WHERE view_id = ?1 AND inode_id = ?2",
                params![view_id, to_i64(inode_id)?, i64::MAX],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn forget(&self, view_id: i64, inode_id: u64, count: u64) -> Result<(), VfsError> {
        self.connection
            .execute(
                "UPDATE sparse_cache_inodes
                SET lookup_count = MAX(lookup_count - ?3, 0)
                WHERE view_id = ?1 AND inode_id = ?2",
                params![view_id, to_i64(inode_id)?, to_i64(count)?],
            )
            .map_err(|_| sparse_cache_error())?;
        Ok(())
    }

    pub fn prune_forgotten_unlinked(&self) -> Result<usize, VfsError> {
        self.connection
            .execute(
                "DELETE FROM sparse_cache_inodes
                WHERE lookup_count = 0 AND nlink = 0",
                [],
            )
            .map_err(|_| sparse_cache_error())
    }

    pub fn enqueue_hydration_job(
        &self,
        target: &HydrationJobTarget,
        now_unix_nanos: u64,
    ) -> Result<i64, VfsError> {
        validate_hydration_target(target)?;
        let identity = self.get_view_identity(target.view_id)?;
        let normalized_path = normalize_cache_path(&target.path)?;
        let commit_id = identity.commit_id.map(CommitId::to_hex);
        let ref_name = identity.ref_name.as_ref().map(|name| name.as_str());
        let ref_version = identity.ref_version.map(to_i64).transpose()?;
        let chunk_index = target.chunk_index.map(to_i64).transpose()?;
        let now = to_i64(now_unix_nanos)?;

        self.connection
            .execute(
                "INSERT OR IGNORE INTO sparse_cache_hydration_jobs
                (view_id, repo_id, root_tree_id, commit_id, ref_name, ref_version, scope,
                 object_id, object_kind, chunk_index, path, state, attempts,
                 created_at_unix_nanos, updated_at_unix_nanos, next_run_at_unix_nanos)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'pending', 0, ?12, ?12, ?12)",
                params![
                    target.view_id,
                    identity.repo_id.as_str(),
                    identity.root_tree_id.to_hex(),
                    commit_id,
                    ref_name,
                    ref_version,
                    hydration_scope_text(target.scope),
                    target.object_id.to_hex(),
                    object_kind_text(target.object_kind),
                    chunk_index,
                    normalized_path,
                    now,
                ],
            )
            .map_err(|_| sparse_cache_error())?;

        self.connection
            .query_row(
                "SELECT job_id FROM sparse_cache_hydration_jobs
                WHERE view_id = ?1
                  AND scope = ?2
                  AND object_id = ?3
                  AND object_kind = ?4
                  AND COALESCE(chunk_index, -1) = COALESCE(?5, -1)
                  AND path = ?6",
                params![
                    target.view_id,
                    hydration_scope_text(target.scope),
                    target.object_id.to_hex(),
                    object_kind_text(target.object_kind),
                    chunk_index,
                    normalized_path,
                ],
                |row| row.get(0),
            )
            .map_err(|_| sparse_cache_error())
    }

    pub fn claim_hydration_jobs(
        &self,
        limit: u64,
        now_unix_nanos: u64,
    ) -> Result<Vec<HydrationJob>, VfsError> {
        self.claim_hydration_jobs_where(None, limit, now_unix_nanos)
    }

    pub fn claim_hydration_jobs_for_view(
        &self,
        view_id: i64,
        limit: u64,
        now_unix_nanos: u64,
    ) -> Result<Vec<HydrationJob>, VfsError> {
        self.claim_hydration_jobs_where(Some(view_id), limit, now_unix_nanos)
    }

    fn claim_hydration_jobs_where(
        &self,
        view_id: Option<i64>,
        limit: u64,
        now_unix_nanos: u64,
    ) -> Result<Vec<HydrationJob>, VfsError> {
        if matches!(view_id, Some(value) if value <= 0) {
            return Err(VfsError::InvalidArgs {
                message: "sparse cache hydration view id is invalid".to_string(),
            });
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = to_i64(limit)?;
        let now = to_i64(now_unix_nanos)?;
        let view_filter = view_id.unwrap_or(-1);
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|_| sparse_cache_error())?;
        let claim_result = (|| {
            let job_ids = {
                let mut statement = self
                    .connection
                    .prepare(
                        "SELECT job_id FROM sparse_cache_hydration_jobs
                        WHERE (?3 = -1 OR view_id = ?3)
                          AND (
                              state = 'pending'
                              OR (state = 'backoff' AND next_run_at_unix_nanos IS NOT NULL AND next_run_at_unix_nanos <= ?1)
                          )
                        ORDER BY created_at_unix_nanos, job_id
                        LIMIT ?2",
                    )
                    .map_err(|_| sparse_cache_error())?;
                statement
                    .query_map(params![now, limit, view_filter], |row| row.get::<_, i64>(0))
                    .map_err(|_| sparse_cache_error())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| sparse_cache_error())?
            };
            let mut claimed = Vec::new();
            for job_id in job_ids {
                let updated = self
                    .connection
                    .execute(
                        "UPDATE sparse_cache_hydration_jobs
                        SET state = 'running',
                            attempts = attempts + 1,
                            updated_at_unix_nanos = ?2,
                            next_run_at_unix_nanos = NULL
                        WHERE job_id = ?1
                          AND (?3 = -1 OR view_id = ?3)
                          AND (state = 'pending'
                               OR (state = 'backoff' AND next_run_at_unix_nanos IS NOT NULL AND next_run_at_unix_nanos <= ?2))",
                        params![job_id, now, view_filter],
                    )
                    .map_err(|_| sparse_cache_error())?;
                if updated == 1 {
                    claimed.push(job_id);
                }
            }
            Ok(claimed)
        })();
        let job_ids = match claim_result {
            Ok(job_ids) => {
                self.connection
                    .execute_batch("COMMIT")
                    .map_err(|_| sparse_cache_error())?;
                job_ids
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        let mut jobs = job_ids
            .into_iter()
            .map(|job_id| self.get_hydration_job(job_id))
            .collect::<Result<Vec<_>, VfsError>>()?;
        jobs.sort_by_key(|job| (job.created_at_unix_nanos, job.job_id));
        Ok(jobs)
    }

    pub fn complete_hydration_job(
        &self,
        job_id: i64,
        expected_attempts: u64,
        now_unix_nanos: u64,
    ) -> Result<(), VfsError> {
        let attempts = to_i64(expected_attempts)?;
        let now = to_i64(now_unix_nanos)?;
        let updated = self
            .connection
            .execute(
                "UPDATE sparse_cache_hydration_jobs
                SET state = 'completed',
                    updated_at_unix_nanos = ?2,
                    completed_at_unix_nanos = ?2,
                    next_run_at_unix_nanos = NULL,
                    last_error_code = NULL
                WHERE job_id = ?1 AND state = 'running' AND attempts = ?3",
                params![job_id, now, attempts],
            )
            .map_err(|_| sparse_cache_error())?;
        require_single_hydration_transition(updated)
    }

    pub fn fail_hydration_job(
        &self,
        job_id: i64,
        expected_attempts: u64,
        state: HydrationJobState,
        last_error_code: &str,
        next_run_at_unix_nanos: Option<u64>,
        now_unix_nanos: u64,
    ) -> Result<(), VfsError> {
        validate_hydration_error_code(last_error_code)?;
        let state_text = match state {
            HydrationJobState::Failed => "failed",
            HydrationJobState::Backoff => "backoff",
            HydrationJobState::Poisoned => "poisoned",
            HydrationJobState::Pending
            | HydrationJobState::Running
            | HydrationJobState::Completed => {
                return Err(VfsError::InvalidArgs {
                    message: "sparse cache hydration failure state is invalid".to_string(),
                });
            }
        };
        if state == HydrationJobState::Backoff && next_run_at_unix_nanos.is_none() {
            return Err(VfsError::InvalidArgs {
                message: "sparse cache hydration backoff requires next run".to_string(),
            });
        }
        if state == HydrationJobState::Poisoned && last_error_code != HYDRATION_POISONED {
            return Err(VfsError::InvalidArgs {
                message: "sparse cache hydration poisoned error code is invalid".to_string(),
            });
        }
        let next_run_at = next_run_at_unix_nanos.map(to_i64).transpose()?;
        let attempts = to_i64(expected_attempts)?;
        let now = to_i64(now_unix_nanos)?;

        let updated = self
            .connection
            .execute(
                "UPDATE sparse_cache_hydration_jobs
                SET state = ?2,
                    updated_at_unix_nanos = ?5,
                    next_run_at_unix_nanos = ?4,
                    completed_at_unix_nanos = NULL,
                    last_error_code = ?3
                WHERE job_id = ?1 AND state = 'running' AND attempts = ?6",
                params![
                    job_id,
                    state_text,
                    last_error_code,
                    next_run_at,
                    now,
                    attempts
                ],
            )
            .map_err(|_| sparse_cache_error())?;
        require_single_hydration_transition(updated)
    }

    pub fn hydration_progress(&self, view_id: i64) -> Result<HydrationProgress, VfsError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT state, COUNT(*), COALESCE(SUM(attempts), 0)
                FROM sparse_cache_hydration_jobs
                WHERE view_id = ?1
                GROUP BY state",
            )
            .map_err(|_| sparse_cache_error())?;
        let mut rows = statement
            .query([view_id])
            .map_err(|_| sparse_cache_error())?;
        let mut progress = HydrationProgress::default();
        while let Some(row) = rows.next().map_err(|_| sparse_cache_error())? {
            let state: String = row.get(0).map_err(|_| sparse_cache_error())?;
            let count = u64_from_i64(row.get(1).map_err(|_| sparse_cache_error())?)?;
            let attempts = u64_from_i64(row.get(2).map_err(|_| sparse_cache_error())?)?;
            progress.total_attempts = progress
                .total_attempts
                .checked_add(attempts)
                .ok_or_else(sparse_cache_error)?;
            match hydration_state(&state)? {
                HydrationJobState::Pending => progress.pending = count,
                HydrationJobState::Running => progress.running = count,
                HydrationJobState::Completed => progress.completed = count,
                HydrationJobState::Failed => progress.failed = count,
                HydrationJobState::Backoff => progress.backoff = count,
                HydrationJobState::Poisoned => progress.poisoned = count,
            }
        }
        Ok(progress)
    }

    fn config_u32(&self, key: &str) -> Result<u32, VfsError> {
        let value: String = self
            .connection
            .query_row(
                "SELECT value FROM sparse_cache_config WHERE key = ?1",
                [key],
                |row| row.get(0),
            )
            .map_err(|_| sparse_cache_error())?;
        value.parse::<u32>().map_err(|_| sparse_cache_error())
    }

    fn get_hydration_job(&self, job_id: i64) -> Result<HydrationJob, VfsError> {
        self.connection
            .query_row(
                "SELECT job_id, view_id, scope, object_id, object_kind, chunk_index, path, state,
                        attempts, created_at_unix_nanos, updated_at_unix_nanos,
                        next_run_at_unix_nanos, completed_at_unix_nanos, last_error_code
                FROM sparse_cache_hydration_jobs
                WHERE job_id = ?1",
                [job_id],
                hydration_job_from_row,
            )
            .map_err(|_| sparse_cache_error())?
    }

    fn run_immediate_transaction<F>(&self, operation: F) -> Result<(), VfsError>
    where
        F: FnOnce() -> Result<(), VfsError>,
    {
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|_| sparse_cache_error())?;
        match operation() {
            Ok(()) => self
                .connection
                .execute_batch("COMMIT")
                .map_err(|_| sparse_cache_error()),
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn refresh_view_metadata(&self, view_id: i64) -> Result<(), VfsError> {
        self.connection
            .execute(
                "UPDATE sparse_cache_inodes
                SET nlink = 2
                WHERE view_id = ?1 AND inode_id = 1 AND node_kind = 'directory'",
                [view_id],
            )
            .map_err(|_| sparse_cache_error())?;
        self.connection
            .execute(
                "UPDATE sparse_cache_inodes
                SET nlink = CASE
                    WHEN node_kind = 'directory' THEN MAX(2, (
                        SELECT COUNT(*) FROM sparse_cache_dentries
                        WHERE view_id = sparse_cache_inodes.view_id
                          AND child_inode_id = sparse_cache_inodes.inode_id
                    ))
                    ELSE MAX(1, (
                        SELECT COUNT(*) FROM sparse_cache_dentries
                        WHERE view_id = sparse_cache_inodes.view_id
                          AND child_inode_id = sparse_cache_inodes.inode_id
                    ))
                END
                WHERE view_id = ?1 AND inode_id != 1",
                [view_id],
            )
            .map_err(|_| sparse_cache_error())?;
        let statfs = self
            .connection
            .query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(CASE WHEN node_kind = 'file' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN node_kind = 'directory' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN node_kind = 'symlink' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN node_kind != 'directory' THEN size ELSE 0 END), 0),
                        COALESCE(SUM(blocks), 0)
                FROM sparse_cache_inodes
                WHERE view_id = ?1",
                [view_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .map_err(|_| sparse_cache_error())?;
        self.put_statfs(&CachedStatfs {
            view_id,
            inode_count: u64_from_i64(statfs.0)?,
            file_count: u64_from_i64(statfs.1)?,
            directory_count: u64_from_i64(statfs.2)?,
            symlink_count: u64_from_i64(statfs.3)?,
            bytes_used: u64_from_i64(statfs.4)?,
            blocks_used: u64_from_i64(statfs.5)?,
            block_size: u64::from(CHUNK_SIZE),
        })
    }
}

pub fn normalize_cache_path(path: &str) -> Result<String, VfsError> {
    if path.contains('\0') {
        return Err(invalid_cache_path());
    }

    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if components.pop().is_none() {
                    return Err(invalid_cache_path());
                }
            }
            value if value.len() <= MAX_CACHE_PATH_COMPONENT_LEN => components.push(value),
            _ => return Err(invalid_cache_path()),
        }
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

fn inode_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<CachedInode, VfsError>> {
    let node_kind: String = row.get(2)?;
    let object_id: Option<String> = row.get(3)?;
    let object_kind: Option<String> = row.get(4)?;
    let size_known: i64 = row.get(10)?;
    let custom_attrs_json: String = row.get(18)?;

    Ok((|| {
        Ok(CachedInode {
            view_id: row.get(0).map_err(|_| sparse_cache_error())?,
            inode_id: u64_from_i64(row.get(1).map_err(|_| sparse_cache_error())?)?,
            node_kind: cached_node_kind(&node_kind)?,
            object_id: object_id.as_deref().map(object_id_from_hex).transpose()?,
            object_kind: object_kind.as_deref().map(cached_object_kind).transpose()?,
            mode: u32_from_i64(row.get(5).map_err(|_| sparse_cache_error())?)?,
            uid: u32_from_i64(row.get(6).map_err(|_| sparse_cache_error())?)?,
            gid: u32_from_i64(row.get(7).map_err(|_| sparse_cache_error())?)?,
            nlink: u64_from_i64(row.get(8).map_err(|_| sparse_cache_error())?)?,
            size: u64_from_i64(row.get(9).map_err(|_| sparse_cache_error())?)?,
            size_known: bool_from_i64(size_known)?,
            block_size: u64_from_i64(row.get(11).map_err(|_| sparse_cache_error())?)?,
            blocks: u64_from_i64(row.get(12).map_err(|_| sparse_cache_error())?)?,
            mtime_secs: u64_from_i64(row.get(13).map_err(|_| sparse_cache_error())?)?,
            mtime_nanos: u32_from_i64(row.get(14).map_err(|_| sparse_cache_error())?)?,
            ctime_secs: u64_from_i64(row.get(15).map_err(|_| sparse_cache_error())?)?,
            ctime_nanos: u32_from_i64(row.get(16).map_err(|_| sparse_cache_error())?)?,
            mime_type: row.get(17).map_err(|_| sparse_cache_error())?,
            custom_attrs: serde_json::from_str(&custom_attrs_json)
                .map_err(|_| sparse_cache_error())?,
            lookup_count: u64_from_i64(row.get(19).map_err(|_| sparse_cache_error())?)?,
        })
    })())
}

fn node_kind_text(kind: CachedNodeKind) -> &'static str {
    match kind {
        CachedNodeKind::File => "file",
        CachedNodeKind::Directory => "directory",
        CachedNodeKind::Symlink => "symlink",
    }
}

fn cached_node_kind(value: &str) -> Result<CachedNodeKind, VfsError> {
    match value {
        "file" => Ok(CachedNodeKind::File),
        "directory" => Ok(CachedNodeKind::Directory),
        "symlink" => Ok(CachedNodeKind::Symlink),
        _ => Err(sparse_cache_error()),
    }
}

fn object_kind_text(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
    }
}

fn cached_object_kind(value: &str) -> Result<ObjectKind, VfsError> {
    match value {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        _ => Err(sparse_cache_error()),
    }
}

fn hydration_scope_text(scope: HydrationJobScope) -> &'static str {
    match scope {
        HydrationJobScope::Tree => "tree",
        HydrationJobScope::Chunk => "chunk",
    }
}

fn hydration_scope(value: &str) -> Result<HydrationJobScope, VfsError> {
    match value {
        "tree" => Ok(HydrationJobScope::Tree),
        "chunk" => Ok(HydrationJobScope::Chunk),
        _ => Err(sparse_cache_error()),
    }
}

fn hydration_state(value: &str) -> Result<HydrationJobState, VfsError> {
    match value {
        "pending" => Ok(HydrationJobState::Pending),
        "running" => Ok(HydrationJobState::Running),
        "completed" => Ok(HydrationJobState::Completed),
        "failed" => Ok(HydrationJobState::Failed),
        "backoff" => Ok(HydrationJobState::Backoff),
        "poisoned" => Ok(HydrationJobState::Poisoned),
        _ => Err(sparse_cache_error()),
    }
}

fn validate_cache_ref_identity(
    ref_name: Option<&RefName>,
    ref_version: Option<u64>,
) -> Result<(), VfsError> {
    match (ref_name, ref_version) {
        (Some(_), Some(version)) if version > 0 => Ok(()),
        (None, None) => Ok(()),
        (Some(_), Some(_)) => Err(VfsError::InvalidArgs {
            message: "sparse cache ref version is invalid".to_string(),
        }),
        (Some(_), None) => Err(VfsError::InvalidArgs {
            message: "sparse cache ref view requires ref version".to_string(),
        }),
        (None, Some(_)) => Err(VfsError::InvalidArgs {
            message: "sparse cache ref version requires ref name".to_string(),
        }),
    }
}

fn validate_hydration_target(target: &HydrationJobTarget) -> Result<(), VfsError> {
    match (target.scope, target.object_kind, target.chunk_index) {
        (HydrationJobScope::Tree, ObjectKind::Tree, None)
        | (HydrationJobScope::Chunk, ObjectKind::Blob, Some(_)) => Ok(()),
        _ => Err(VfsError::InvalidArgs {
            message: "sparse cache hydration target is invalid".to_string(),
        }),
    }
}

fn validate_hydration_error_code(value: &str) -> Result<(), VfsError> {
    match value {
        TREE_HYDRATION_FAILED | CHUNK_HYDRATION_FAILED | HYDRATION_POISONED => Ok(()),
        _ => Err(VfsError::InvalidArgs {
            message: "sparse cache hydration error code is invalid".to_string(),
        }),
    }
}

fn require_single_hydration_transition(updated: usize) -> Result<(), VfsError> {
    if updated == 1 {
        Ok(())
    } else {
        Err(VfsError::InvalidArgs {
            message: "sparse cache hydration job is not running".to_string(),
        })
    }
}

fn hydration_job_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<HydrationJob, VfsError>> {
    let scope: String = row.get(2)?;
    let object_id: String = row.get(3)?;
    let object_kind: String = row.get(4)?;
    let chunk_index: Option<i64> = row.get(5)?;
    let state: String = row.get(7)?;
    let next_run_at_unix_nanos: Option<i64> = row.get(11)?;
    let completed_at_unix_nanos: Option<i64> = row.get(12)?;

    Ok((|| {
        Ok(HydrationJob {
            job_id: row.get(0).map_err(|_| sparse_cache_error())?,
            target: HydrationJobTarget {
                view_id: row.get(1).map_err(|_| sparse_cache_error())?,
                scope: hydration_scope(&scope)?,
                object_id: object_id_from_hex(&object_id)?,
                object_kind: cached_object_kind(&object_kind)?,
                chunk_index: chunk_index.map(u64_from_i64).transpose()?,
                path: row.get(6).map_err(|_| sparse_cache_error())?,
            },
            state: hydration_state(&state)?,
            attempts: u64_from_i64(row.get(8).map_err(|_| sparse_cache_error())?)?,
            created_at_unix_nanos: u64_from_i64(row.get(9).map_err(|_| sparse_cache_error())?)?,
            updated_at_unix_nanos: u64_from_i64(row.get(10).map_err(|_| sparse_cache_error())?)?,
            next_run_at_unix_nanos: next_run_at_unix_nanos.map(u64_from_i64).transpose()?,
            completed_at_unix_nanos: completed_at_unix_nanos.map(u64_from_i64).transpose()?,
            last_error_code: row.get(13).map_err(|_| sparse_cache_error())?,
        })
    })())
}

fn validate_inode(inode: &CachedInode) -> Result<(), VfsError> {
    if inode.block_size == 0 {
        return Err(VfsError::InvalidArgs {
            message: "sparse cache inode block size must be positive".to_string(),
        });
    }
    if inode.mtime_nanos > 999_999_999 || inode.ctime_nanos > 999_999_999 {
        return Err(VfsError::InvalidArgs {
            message: "sparse cache inode timestamp nanos out of range".to_string(),
        });
    }
    if inode.object_id.is_some() != inode.object_kind.is_some() {
        return Err(VfsError::InvalidArgs {
            message: "sparse cache inode object identity is incomplete".to_string(),
        });
    }
    Ok(())
}

fn validate_dentry_name(name: &str) -> Result<(), VfsError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\0')
        || name.len() > MAX_CACHE_PATH_COMPONENT_LEN
    {
        return Err(invalid_cache_path());
    }
    Ok(())
}

fn validate_chunk(chunk: &CachedChunk) -> Result<(), VfsError> {
    if chunk.byte_len != chunk.bytes.len() as u64 {
        return Err(VfsError::InvalidArgs {
            message: "sparse cache chunk length does not match bytes".to_string(),
        });
    }
    if chunk.byte_len > u64::from(CHUNK_SIZE) {
        return Err(VfsError::InvalidArgs {
            message: "sparse cache chunk length exceeds chunk size".to_string(),
        });
    }
    if chunk.offset != expected_chunk_offset(chunk.chunk_index)? {
        return Err(VfsError::InvalidArgs {
            message: "sparse cache chunk offset does not match index".to_string(),
        });
    }
    Ok(())
}

fn expected_chunk_offset(chunk_index: u64) -> Result<u64, VfsError> {
    chunk_index
        .checked_mul(u64::from(CHUNK_SIZE))
        .ok_or_else(|| VfsError::InvalidArgs {
            message: "sparse cache chunk offset out of range".to_string(),
        })
}

fn object_id_from_hex(value: &str) -> Result<ObjectId, VfsError> {
    ObjectId::from_hex(value).map_err(|_| sparse_cache_error())
}

fn to_i64(value: u64) -> Result<i64, VfsError> {
    i64::try_from(value).map_err(|_| VfsError::InvalidArgs {
        message: "sparse cache integer out of range".to_string(),
    })
}

fn u64_from_i64(value: i64) -> Result<u64, VfsError> {
    u64::try_from(value).map_err(|_| sparse_cache_error())
}

fn u32_from_i64(value: i64) -> Result<u32, VfsError> {
    u32::try_from(value).map_err(|_| sparse_cache_error())
}

fn bool_from_i64(value: i64) -> Result<bool, VfsError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(sparse_cache_error()),
    }
}

fn invalid_cache_path() -> VfsError {
    VfsError::InvalidPath {
        path: "sparse cache path".to_string(),
    }
}

fn configure_connection(connection: &Connection, file_backed: bool) -> Result<(), VfsError> {
    connection
        .busy_timeout(Duration::from_millis(5000))
        .map_err(|_| sparse_cache_error())?;
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = ON;
            PRAGMA synchronous = NORMAL;
            ",
        )
        .map_err(|_| sparse_cache_error())?;

    if file_backed {
        connection
            .execute_batch("PRAGMA journal_mode = WAL;")
            .map_err(|_| sparse_cache_error())?;
    }

    Ok(())
}

fn initialize_schema(connection: Connection) -> Result<SparseCache, VfsError> {
    let existing_version = existing_schema_version(&connection)?;
    match existing_version {
        None => {
            connection
                .execute_batch(include_str!("schema.sql"))
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "INSERT INTO sparse_cache_config (key, value) VALUES (?1, ?2)",
                    ("schema_version", SCHEMA_VERSION.to_string()),
                )
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "INSERT INTO sparse_cache_config (key, value) VALUES (?1, ?2)",
                    ("chunk_size", CHUNK_SIZE.to_string()),
                )
                .map_err(|_| sparse_cache_error())?;
        }
        Some(PRE_HYDRATION_SCHEMA_VERSION | PRE_SIZE_KNOWN_SCHEMA_VERSION) => {
            ensure_size_known_column(&connection)?;
            connection
                .execute_batch(include_str!("schema.sql"))
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "UPDATE sparse_cache_config SET value = ?1 WHERE key = 'schema_version'",
                    [SCHEMA_VERSION.to_string()],
                )
                .map_err(|_| sparse_cache_error())?;
        }
        Some(SCHEMA_VERSION) => {
            connection
                .execute_batch(include_str!("schema.sql"))
                .map_err(|_| sparse_cache_error())?;
        }
        Some(_) => return Err(sparse_cache_error()),
    }
    let cache = SparseCache { connection };
    cache.validate_config_value("schema_version", SCHEMA_VERSION)?;
    cache.validate_config_value("chunk_size", CHUNK_SIZE)?;
    cache.validate_stored_ref_versions()?;
    Ok(cache)
}

fn ensure_size_known_column(connection: &Connection) -> Result<(), VfsError> {
    let has_inodes = connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sparse_cache_inodes'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|_| sparse_cache_error())?
        .is_some();
    if !has_inodes {
        return Ok(());
    }

    let has_size_known = connection
        .query_row(
            "SELECT 1 FROM pragma_table_info('sparse_cache_inodes') WHERE name = 'size_known'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|_| sparse_cache_error())?
        .is_some();
    if !has_size_known {
        connection
            .execute(
                "ALTER TABLE sparse_cache_inodes
                ADD COLUMN size_known INTEGER NOT NULL DEFAULT 1",
                [],
            )
            .map_err(|_| sparse_cache_error())?;
    }

    Ok(())
}

fn existing_schema_version(connection: &Connection) -> Result<Option<u32>, VfsError> {
    let has_config = connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sparse_cache_config'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|_| sparse_cache_error())?
        .is_some();
    if !has_config {
        return Ok(None);
    }
    let value: String = connection
        .query_row(
            "SELECT value FROM sparse_cache_config WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|_| sparse_cache_error())?;
    value
        .parse::<u32>()
        .map(Some)
        .map_err(|_| sparse_cache_error())
}

impl SparseCache {
    fn validate_config_value(&self, key: &str, expected: u32) -> Result<(), VfsError> {
        if self.config_u32(key)? == expected {
            Ok(())
        } else {
            Err(sparse_cache_error())
        }
    }

    fn validate_stored_ref_versions(&self) -> Result<(), VfsError> {
        let invalid_views: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sparse_cache_views
                WHERE ref_version IS NOT NULL AND ref_version <= 0",
                [],
                |row| row.get(0),
            )
            .map_err(|_| sparse_cache_error())?;
        let invalid_jobs: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sparse_cache_hydration_jobs
                WHERE ref_version IS NOT NULL AND ref_version <= 0",
                [],
                |row| row.get(0),
            )
            .map_err(|_| sparse_cache_error())?;
        if invalid_views == 0 && invalid_jobs == 0 {
            Ok(())
        } else {
            Err(sparse_cache_error())
        }
    }
}

fn sparse_cache_error() -> VfsError {
    VfsError::CorruptStore {
        message: SPARSE_CACHE_ERROR.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CacheViewIdentity, CachedChunk, CachedDentry, CachedInode, CachedNodeKind, CachedStatfs,
        CachedSymlink, HydrationJobScope, HydrationJobState, HydrationJobTarget, SparseCache,
        normalize_cache_path,
    };
    use crate::backend::RepoId;
    use crate::error::VfsError;
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, RefName};
    use rusqlite::Connection;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn creates_schema_and_records_version() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;

        assert_eq!(cache.schema_version()?, 3);
        assert_eq!(cache.chunk_size()?, 4096);

        Ok(())
    }

    #[test]
    fn reopens_existing_cache_without_recreating_identity_rows() -> Result<(), VfsError> {
        let path = unique_cache_path("reopens_existing_cache");

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 3);
            assert_eq!(config_row_count(&path)?, 2);
        }

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.chunk_size()?, 4096);
            assert_eq!(config_row_count(&path)?, 2);
        }

        fs::remove_file(path)?;

        Ok(())
    }

    #[test]
    fn view_identity_includes_repo_root_commit_and_ref_version() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let identity = cache_view_identity();
        let view_id = cache.insert_view(&identity)?;

        assert_eq!(cache.get_view_identity(view_id)?, identity);

        let missing_ref_version = CacheViewIdentity {
            ref_name: Some(RefName::new("main")?),
            ref_version: None,
            ..cache_view_identity()
        };
        assert!(matches!(
            cache.insert_view(&missing_ref_version),
            Err(VfsError::InvalidArgs { .. })
        ));
        let missing_ref_name = CacheViewIdentity {
            ref_name: None,
            ref_version: Some(7),
            ..cache_view_identity()
        };
        assert!(matches!(
            cache.insert_view(&missing_ref_name),
            Err(VfsError::InvalidArgs { .. })
        ));
        let zero_ref_version = CacheViewIdentity {
            ref_version: Some(0),
            ..cache_view_identity()
        };
        assert!(matches!(
            cache.insert_view(&zero_ref_version),
            Err(VfsError::InvalidArgs { .. })
        ));

        Ok(())
    }

    #[test]
    fn normalizes_absolute_paths_and_rejects_root_escape() -> Result<(), VfsError> {
        assert_eq!(normalize_cache_path("///src/./lib.rs")?, "/src/lib.rs");
        assert_eq!(normalize_cache_path("src//./main.rs")?, "/src/main.rs");
        assert_eq!(normalize_cache_path("/src/../README.md")?, "/README.md");
        assert!(matches!(
            normalize_cache_path("/../secret.txt"),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            normalize_cache_path("/contains\0nul"),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            normalize_cache_path(&format!("/{}", "a".repeat(256))),
            Err(VfsError::InvalidPath { .. })
        ));

        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        cache.put_inode(&cached_inode(
            1,
            CachedNodeKind::Directory,
            None,
            None,
            0o755,
            2,
        ))?;
        cache.put_inode(&cached_inode(
            2,
            CachedNodeKind::File,
            Some(object_id(b"readme")),
            Some(ObjectKind::Blob),
            0o644,
            1,
        ))?;

        cache.put_dentry(&CachedDentry {
            view_id,
            parent_inode_id: 1,
            name: "README.md".to_string(),
            child_inode_id: 2,
            path: "docs/../README.md".to_string(),
        })?;
        assert!(matches!(
            cache.put_dentry(&CachedDentry {
                view_id,
                parent_inode_id: 1,
                name: "nested/name".to_string(),
                child_inode_id: 2,
                path: "/nested/name".to_string(),
            }),
            Err(VfsError::InvalidPath { .. })
        ));

        assert_eq!(cache.list_dentries(view_id, 1)?[0].path, "/README.md");

        Ok(())
    }

    #[test]
    fn inode_upserts_preserve_existing_dentries() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let mut parent = cached_inode(1, CachedNodeKind::Directory, None, None, 0o755, 2);
        let mut child = cached_inode(
            2,
            CachedNodeKind::File,
            Some(object_id(b"readme")),
            Some(ObjectKind::Blob),
            0o644,
            1,
        );
        cache.put_inode(&parent)?;
        cache.put_inode(&child)?;
        cache.put_dentry(&cached_dentry(view_id, 1, "README.md", 2, "/README.md"))?;

        parent.nlink = 3;
        child.nlink = 2;
        cache.put_inode(&parent)?;
        cache.put_inode(&child)?;

        let entries = cache.list_dentries(view_id, 1)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "/README.md");
        assert_eq!(cache.get_inode(view_id, 1)?.unwrap().nlink, 3);
        assert_eq!(cache.get_inode(view_id, 2)?.unwrap().nlink, 2);

        Ok(())
    }

    #[test]
    fn metadata_round_trips_for_file_directory_and_symlink() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let mut attrs = BTreeMap::new();
        attrs.insert("language".to_string(), "rust".to_string());

        let file = CachedInode {
            view_id,
            inode_id: 2,
            node_kind: CachedNodeKind::File,
            object_id: Some(object_id(b"file")),
            object_kind: Some(ObjectKind::Blob),
            mode: 0o100644,
            uid: 501,
            gid: 20,
            nlink: 1,
            size: 123,
            size_known: true,
            block_size: 4096,
            blocks: 1,
            mtime_secs: 10,
            mtime_nanos: 20,
            ctime_secs: 30,
            ctime_nanos: 40,
            mime_type: Some("text/x-rust".to_string()),
            custom_attrs: attrs,
            lookup_count: 3,
        };
        let directory = cached_inode(1, CachedNodeKind::Directory, None, None, 0o40755, 2);
        let symlink = CachedInode {
            size: 11,
            object_id: Some(object_id(b"symlink")),
            object_kind: Some(ObjectKind::Blob),
            ..cached_inode(3, CachedNodeKind::Symlink, None, None, 0o120777, 1)
        };

        cache.put_inode(&directory)?;
        cache.put_inode(&file)?;
        cache.put_inode(&symlink)?;

        assert_eq!(cache.get_inode(view_id, 1)?, Some(directory));
        assert_eq!(cache.get_inode(view_id, 2)?, Some(file));
        assert_eq!(cache.get_inode(view_id, 3)?, Some(symlink));

        Ok(())
    }

    #[test]
    fn inode_size_known_round_trips_for_known_and_unknown_files() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let known_file = CachedInode {
            size: 123,
            size_known: true,
            ..cached_inode(
                1,
                CachedNodeKind::File,
                Some(object_id(b"known-size")),
                Some(ObjectKind::Blob),
                0o100644,
                1,
            )
        };
        let unknown_file = CachedInode {
            inode_id: 2,
            size: 0,
            size_known: false,
            object_id: Some(object_id(b"unknown-size")),
            object_kind: Some(ObjectKind::Blob),
            ..cached_inode(2, CachedNodeKind::File, None, None, 0o100644, 1)
        };

        cache.put_inode(&known_file)?;
        cache.put_inode(&unknown_file)?;

        assert_eq!(cache.get_inode(view_id, 1)?, Some(known_file));
        assert_eq!(cache.get_inode(view_id, 2)?, Some(unknown_file));

        Ok(())
    }

    #[test]
    fn tree_entries_can_reference_tree_blob_and_symlink_objects() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        cache.put_inode(&cached_inode(
            1,
            CachedNodeKind::Directory,
            None,
            None,
            0o40755,
            2,
        ))?;
        cache.put_inode(&cached_inode(
            2,
            CachedNodeKind::Directory,
            Some(object_id(b"tree")),
            Some(ObjectKind::Tree),
            0o40755,
            2,
        ))?;
        cache.put_inode(&cached_inode(
            3,
            CachedNodeKind::File,
            Some(object_id(b"blob")),
            Some(ObjectKind::Blob),
            0o100644,
            1,
        ))?;
        cache.put_inode(&cached_inode(
            4,
            CachedNodeKind::Symlink,
            Some(object_id(b"symlink-object")),
            Some(ObjectKind::Blob),
            0o120777,
            1,
        ))?;

        cache.put_dentry(&cached_dentry(view_id, 1, "src", 2, "/src"))?;
        cache.put_dentry(&cached_dentry(view_id, 1, "README.md", 3, "/README.md"))?;
        cache.put_dentry(&cached_dentry(view_id, 1, "latest", 4, "/latest"))?;

        let entries = cache.list_dentries(view_id, 1)?;
        assert_eq!(entries.len(), 3);
        assert_eq!(
            cache
                .get_inode(view_id, entries[0].child_inode_id)?
                .unwrap()
                .object_kind,
            Some(ObjectKind::Blob)
        );
        assert_eq!(
            cache
                .get_inode(view_id, entries[1].child_inode_id)?
                .unwrap()
                .object_kind,
            Some(ObjectKind::Blob)
        );
        assert_eq!(
            cache
                .get_inode(view_id, entries[2].child_inode_id)?
                .unwrap()
                .object_kind,
            Some(ObjectKind::Tree)
        );

        Ok(())
    }

    #[test]
    fn hardlinks_are_multiple_dentries_to_one_inode_with_nlink() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        cache.put_inode(&cached_inode(
            1,
            CachedNodeKind::Directory,
            None,
            None,
            0o40755,
            2,
        ))?;
        cache.put_inode(&cached_inode(
            2,
            CachedNodeKind::File,
            Some(object_id(b"hardlinked")),
            Some(ObjectKind::Blob),
            0o100644,
            2,
        ))?;

        cache.put_dentry(&cached_dentry(view_id, 1, "one.txt", 2, "/one.txt"))?;
        cache.put_dentry(&cached_dentry(view_id, 1, "two.txt", 2, "/two.txt"))?;

        let entries = cache.list_dentries(view_id, 1)?;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.child_inode_id == 2));
        assert_eq!(cache.get_inode(view_id, 2)?.unwrap().nlink, 2);

        Ok(())
    }

    #[test]
    fn chunks_are_keyed_by_repo_object_and_chunk_index() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let repo_id = RepoId::new("local")?;
        let other_repo_id = RepoId::new("fork")?;
        let object_id = object_id(b"shared-content");
        let same_index = 3;

        let local_chunk = CachedChunk {
            repo_id: repo_id.clone(),
            object_id,
            chunk_index: same_index,
            offset: 12_288,
            byte_len: 4,
            bytes: b"main".to_vec(),
        };
        let fork_chunk = CachedChunk {
            repo_id: other_repo_id.clone(),
            object_id,
            chunk_index: same_index,
            offset: 12_288,
            byte_len: 4,
            bytes: b"fork".to_vec(),
        };
        let next_chunk = CachedChunk {
            repo_id: repo_id.clone(),
            object_id,
            chunk_index: same_index + 1,
            offset: 16_384,
            byte_len: 4,
            bytes: b"next".to_vec(),
        };

        cache.put_chunk(&local_chunk)?;
        cache.put_chunk(&fork_chunk)?;
        cache.put_chunk(&next_chunk)?;
        let mismatched_len = CachedChunk {
            byte_len: 99,
            ..local_chunk.clone()
        };
        let oversized_chunk = CachedChunk {
            offset: 16_384,
            chunk_index: 4,
            byte_len: 4097,
            bytes: vec![0; 4097],
            ..local_chunk.clone()
        };
        let mismatched_offset = CachedChunk {
            offset: 1,
            ..local_chunk.clone()
        };
        assert!(matches!(
            cache.put_chunk(&mismatched_len),
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            cache.put_chunk(&oversized_chunk),
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            cache.put_chunk(&mismatched_offset),
            Err(VfsError::InvalidArgs { .. })
        ));

        assert_eq!(
            cache.get_chunk(&repo_id, object_id, same_index)?,
            Some(local_chunk)
        );
        assert_eq!(
            cache.get_chunk(&other_repo_id, object_id, same_index)?,
            Some(fork_chunk)
        );
        assert_eq!(
            cache.get_chunk(&repo_id, object_id, same_index + 1)?,
            Some(next_chunk)
        );

        Ok(())
    }

    #[test]
    fn hydration_jobs_dedupe_by_view_identity_scope_object_chunk_and_path() -> Result<(), VfsError>
    {
        let cache = SparseCache::open_in_memory()?;
        let main_view_id = cache.insert_view(&cache_view_identity())?;
        let fork_view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: RepoId::new("fork")?,
            ..cache_view_identity()
        })?;
        let tree_object_id = object_id(b"tree-to-hydrate");

        let first_job_id = cache.enqueue_hydration_job(
            &HydrationJobTarget {
                view_id: main_view_id,
                scope: HydrationJobScope::Tree,
                object_id: tree_object_id,
                object_kind: ObjectKind::Tree,
                chunk_index: None,
                path: "/src".to_string(),
            },
            10,
        )?;
        let duplicate_job_id = cache.enqueue_hydration_job(
            &HydrationJobTarget {
                view_id: main_view_id,
                scope: HydrationJobScope::Tree,
                object_id: tree_object_id,
                object_kind: ObjectKind::Tree,
                chunk_index: None,
                path: "src".to_string(),
            },
            20,
        )?;
        let different_view_job_id = cache.enqueue_hydration_job(
            &HydrationJobTarget {
                view_id: fork_view_id,
                scope: HydrationJobScope::Tree,
                object_id: tree_object_id,
                object_kind: ObjectKind::Tree,
                chunk_index: None,
                path: "/src".to_string(),
            },
            30,
        )?;
        let chunk_job_id = cache.enqueue_hydration_job(
            &HydrationJobTarget {
                view_id: main_view_id,
                scope: HydrationJobScope::Chunk,
                object_id: object_id(b"blob-to-hydrate"),
                object_kind: ObjectKind::Blob,
                chunk_index: Some(0),
                path: "/src/lib.rs".to_string(),
            },
            40,
        )?;

        assert_eq!(duplicate_job_id, first_job_id);
        assert_ne!(different_view_job_id, first_job_id);
        assert_ne!(chunk_job_id, first_job_id);
        assert!(matches!(
            cache.enqueue_hydration_job(
                &HydrationJobTarget {
                    view_id: main_view_id,
                    scope: HydrationJobScope::Tree,
                    object_id: tree_object_id,
                    object_kind: ObjectKind::Tree,
                    chunk_index: Some(0),
                    path: "/src".to_string(),
                },
                50,
            ),
            Err(VfsError::InvalidArgs { .. })
        ));

        Ok(())
    }

    #[test]
    fn hydration_claim_is_bounded_and_moves_due_jobs_to_running() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let first_job_id = cache.enqueue_hydration_job(&tree_hydration_target(view_id, "a"), 10)?;
        let second_job_id =
            cache.enqueue_hydration_job(&tree_hydration_target(view_id, "b"), 20)?;
        let third_job_id = cache.enqueue_hydration_job(&tree_hydration_target(view_id, "c"), 30)?;

        let initially_claimed = cache.claim_hydration_jobs(2, 35)?;
        assert_eq!(
            initially_claimed
                .iter()
                .map(|job| job.job_id)
                .collect::<Vec<_>>(),
            vec![first_job_id, second_job_id]
        );
        cache.complete_hydration_job(first_job_id, initially_claimed[0].attempts, 40)?;
        cache.fail_hydration_job(
            second_job_id,
            initially_claimed[1].attempts,
            HydrationJobState::Backoff,
            "tree_hydration_failed",
            Some(90),
            50,
        )?;

        let claimed = cache.claim_hydration_jobs(2, 100)?;
        let progress = cache.hydration_progress(view_id)?;

        assert_eq!(
            claimed.iter().map(|job| job.job_id).collect::<Vec<_>>(),
            vec![second_job_id, third_job_id]
        );
        assert_eq!(
            claimed
                .iter()
                .map(|job| (job.state, job.attempts))
                .collect::<Vec<_>>(),
            vec![
                (HydrationJobState::Running, 2),
                (HydrationJobState::Running, 1)
            ]
        );
        assert_eq!(progress.running, 2);
        assert_eq!(progress.pending, 0);
        assert_eq!(progress.completed, 1);
        assert_eq!(progress.total_attempts, 4);
        Ok(())
    }

    #[test]
    fn hydration_view_scoped_claim_rejects_invalid_view_id() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let job_id = cache.enqueue_hydration_job(&tree_hydration_target(view_id, "scoped"), 10)?;

        assert!(matches!(
            cache.claim_hydration_jobs_for_view(-1, 1, 20),
            Err(VfsError::InvalidArgs { .. })
        ));
        assert_eq!(cache.hydration_progress(view_id)?.pending, 1);
        assert_eq!(
            cache.claim_hydration_jobs_for_view(view_id, 1, 20)?[0].job_id,
            job_id
        );

        Ok(())
    }

    #[test]
    fn hydration_failures_record_fixed_redacted_codes_and_backoff() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let job_id = cache.enqueue_hydration_job(&tree_hydration_target(view_id, "secret"), 10)?;

        cache
            .fail_hydration_job(
                job_id,
                1,
                HydrationJobState::Backoff,
                "/raw/path leaked from backend",
                Some(200),
                100,
            )
            .unwrap_err();
        let claimed = cache.claim_hydration_jobs(1, 100)?;
        assert_eq!(claimed[0].job_id, job_id);
        cache.fail_hydration_job(
            job_id,
            claimed[0].attempts,
            HydrationJobState::Backoff,
            "tree_hydration_failed",
            Some(200),
            110,
        )?;
        let claimed_after_backoff = cache.claim_hydration_jobs(1, 200)?;
        cache.fail_hydration_job(
            claimed_after_backoff[0].job_id,
            claimed_after_backoff[0].attempts,
            HydrationJobState::Poisoned,
            "hydration_poisoned",
            None,
            210,
        )?;

        let progress = cache.hydration_progress(view_id)?;
        let job_debug = format!("{:?}", cache.claim_hydration_jobs(1, 300)?);

        assert_eq!(progress.poisoned, 1);
        assert_eq!(progress.total_attempts, 2);
        assert!(!job_debug.contains("secret"));
        assert!(!job_debug.contains("/raw/path"));

        Ok(())
    }

    #[test]
    fn hydration_terminal_transitions_are_fenced_by_claim_attempt() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let job_id = cache.enqueue_hydration_job(&tree_hydration_target(view_id, "fenced"), 10)?;

        let first_claim = cache.claim_hydration_jobs(1, 20)?;
        assert_eq!(first_claim[0].attempts, 1);
        cache.fail_hydration_job(
            job_id,
            first_claim[0].attempts,
            HydrationJobState::Backoff,
            "tree_hydration_failed",
            Some(30),
            25,
        )?;
        let second_claim = cache.claim_hydration_jobs(1, 30)?;
        assert_eq!(second_claim[0].attempts, 2);

        assert!(matches!(
            cache.complete_hydration_job(job_id, first_claim[0].attempts, 35),
            Err(VfsError::InvalidArgs { .. })
        ));
        assert_eq!(cache.hydration_progress(view_id)?.running, 1);

        cache.complete_hydration_job(job_id, second_claim[0].attempts, 40)?;
        assert_eq!(cache.hydration_progress(view_id)?.completed, 1);

        Ok(())
    }

    #[test]
    fn hydration_terminal_transitions_require_running_jobs() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let job_id = cache.enqueue_hydration_job(&tree_hydration_target(view_id, "pending"), 10)?;

        assert!(matches!(
            cache.complete_hydration_job(job_id, 1, 20),
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            cache.fail_hydration_job(
                job_id,
                1,
                HydrationJobState::Failed,
                "tree_hydration_failed",
                None,
                20,
            ),
            Err(VfsError::InvalidArgs { .. })
        ));

        let progress = cache.hydration_progress(view_id)?;
        assert_eq!(progress.pending, 1);
        assert_eq!(progress.completed, 0);
        assert_eq!(progress.failed, 0);

        Ok(())
    }

    #[test]
    fn schema_migrates_slice10_cache_to_hydration_schema() -> Result<(), VfsError> {
        let path = unique_cache_path("schema_migrates_slice10_cache");
        {
            let connection = Connection::open(&path).map_err(|_| sparse_cache_error())?;
            connection
                .execute_batch(
                    "
                    CREATE TABLE sparse_cache_config (
                        key TEXT PRIMARY KEY NOT NULL,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE sparse_cache_views (
                        view_id INTEGER PRIMARY KEY,
                        repo_id TEXT NOT NULL,
                        root_tree_id TEXT NOT NULL,
                        commit_id TEXT,
                        ref_name TEXT,
                        ref_version INTEGER,
                        created_at_unix_nanos INTEGER NOT NULL DEFAULT 0
                    );
                    ",
                )
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "INSERT OR REPLACE INTO sparse_cache_config (key, value)
                    VALUES ('schema_version', '1')",
                    [],
                )
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "INSERT OR REPLACE INTO sparse_cache_config (key, value)
                    VALUES ('chunk_size', '4096')",
                    [],
                )
                .map_err(|_| sparse_cache_error())?;
        }

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 3);
            let view_id = cache.insert_view(&cache_view_identity())?;
            let job_id =
                cache.enqueue_hydration_job(&tree_hydration_target(view_id, "migrated"), 1)?;
            assert_eq!(cache.claim_hydration_jobs(1, 1)?[0].job_id, job_id);
            assert!(
                cache
                    .connection
                    .execute(
                        "INSERT INTO sparse_cache_views
                        (repo_id, root_tree_id, commit_id, ref_name, ref_version)
                        VALUES ('local', ?1, NULL, 'main', 0)",
                        [object_id(b"migrated bad ref version").to_hex()],
                    )
                    .is_err()
            );
        }

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn schema_migrates_hydration_cache_to_size_known_schema() -> Result<(), VfsError> {
        let path = unique_cache_path("schema_migrates_hydration_cache");
        let root_tree_id = object_id(b"v2 root").to_hex();
        let object_id = object_id(b"v2 inode").to_hex();
        {
            let connection = Connection::open(&path).map_err(|_| sparse_cache_error())?;
            connection
                .execute_batch(
                    "
                    CREATE TABLE sparse_cache_config (
                        key TEXT PRIMARY KEY NOT NULL,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE sparse_cache_views (
                        view_id INTEGER PRIMARY KEY,
                        repo_id TEXT NOT NULL,
                        root_tree_id TEXT NOT NULL,
                        commit_id TEXT,
                        ref_name TEXT,
                        ref_version INTEGER,
                        created_at_unix_nanos INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE sparse_cache_inodes (
                        view_id INTEGER NOT NULL,
                        inode_id INTEGER NOT NULL,
                        node_kind TEXT NOT NULL,
                        object_id TEXT,
                        object_kind TEXT,
                        mode INTEGER NOT NULL,
                        uid INTEGER NOT NULL,
                        gid INTEGER NOT NULL,
                        nlink INTEGER NOT NULL,
                        size INTEGER NOT NULL,
                        block_size INTEGER NOT NULL,
                        blocks INTEGER NOT NULL,
                        mtime_secs INTEGER NOT NULL,
                        mtime_nanos INTEGER NOT NULL,
                        ctime_secs INTEGER NOT NULL,
                        ctime_nanos INTEGER NOT NULL,
                        mime_type TEXT,
                        custom_attrs_json TEXT NOT NULL DEFAULT '{}',
                        lookup_count INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (view_id, inode_id)
                    );
                    CREATE TABLE sparse_cache_hydration_jobs (
                        job_id INTEGER PRIMARY KEY,
                        view_id INTEGER NOT NULL,
                        repo_id TEXT NOT NULL,
                        root_tree_id TEXT NOT NULL,
                        commit_id TEXT,
                        ref_name TEXT,
                        ref_version INTEGER,
                        scope TEXT NOT NULL,
                        object_id TEXT NOT NULL,
                        object_kind TEXT NOT NULL,
                        chunk_index INTEGER,
                        path TEXT NOT NULL,
                        state TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        created_at_unix_nanos INTEGER NOT NULL,
                        updated_at_unix_nanos INTEGER NOT NULL,
                        next_run_at_unix_nanos INTEGER,
                        completed_at_unix_nanos INTEGER,
                        last_error_code TEXT
                    );
                    INSERT INTO sparse_cache_config (key, value)
                    VALUES ('schema_version', '2'), ('chunk_size', '4096');
                    ",
                )
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "INSERT INTO sparse_cache_views
                    (view_id, repo_id, root_tree_id, commit_id, ref_name, ref_version)
                    VALUES (1, 'local', ?1, NULL, NULL, NULL)",
                    [&root_tree_id],
                )
                .map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "INSERT INTO sparse_cache_inodes
                    (view_id, inode_id, node_kind, object_id, object_kind, mode, uid, gid,
                     nlink, size, block_size, blocks, mtime_secs, mtime_nanos, ctime_secs,
                     ctime_nanos, custom_attrs_json, lookup_count)
                    VALUES (1, 2, 'file', ?1, 'blob', 33188, 501, 20, 1, 987, 4096, 1,
                            1, 2, 3, 4, '{}', 0)",
                    [&object_id],
                )
                .map_err(|_| sparse_cache_error())?;
        }

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 3);
            let inode = cache.get_inode(1, 2)?.unwrap();
            assert!(inode.size_known);
            assert_eq!(inode.size, 987);

            let invalid = cache.connection.execute(
                "UPDATE sparse_cache_inodes
                SET size_known = 2
                WHERE view_id = 1 AND inode_id = 2",
                [],
            );
            assert!(invalid.is_err());
        }

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 3);
            let inode = cache.get_inode(1, 2)?.unwrap();
            assert!(inode.size_known);

            let invalid = cache.connection.execute(
                "UPDATE sparse_cache_inodes
                SET size_known = 2
                WHERE view_id = 1 AND inode_id = 2",
                [],
            );
            assert!(invalid.is_err());
        }

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn unknown_schema_version_is_rejected_before_current_tables_are_created() -> Result<(), VfsError>
    {
        let path = unique_cache_path("schema_unknown_version");
        {
            let connection = Connection::open(&path).map_err(|_| sparse_cache_error())?;
            connection
                .execute_batch(
                    "
                    CREATE TABLE sparse_cache_config (
                        key TEXT PRIMARY KEY NOT NULL,
                        value TEXT NOT NULL
                    );
                    INSERT INTO sparse_cache_config (key, value)
                    VALUES ('schema_version', '999'), ('chunk_size', '4096');
                    ",
                )
                .map_err(|_| sparse_cache_error())?;
        }

        assert!(matches!(
            SparseCache::open(&path),
            Err(VfsError::CorruptStore { .. })
        ));
        {
            let connection = Connection::open(&path).map_err(|_| sparse_cache_error())?;
            let table_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema
                    WHERE type = 'table' AND name = 'sparse_cache_hydration_jobs'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|_| sparse_cache_error())?;
            assert_eq!(table_count, 0);
        }

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn unknown_size_inode_debug_does_not_leak_identity_or_paths() -> Result<(), VfsError> {
        let inode = CachedInode {
            size_known: false,
            object_id: Some(object_id(b"secret unknown size object")),
            object_kind: Some(ObjectKind::Blob),
            mime_type: Some("/secret/path.txt".to_string()),
            custom_attrs: BTreeMap::from([(
                "source_path".to_string(),
                "/secret/path.txt".to_string(),
            )]),
            ..cached_inode(42, CachedNodeKind::File, None, None, 0o100644, 1)
        };

        let debug = format!("{inode:?}");

        assert!(debug.contains("size_known"));
        assert!(debug.contains("false"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("/secret/path.txt"));
        assert!(!debug.contains(&object_id(b"secret unknown size object").to_hex()));

        Ok(())
    }

    #[test]
    fn schema_version_is_checked_when_reopening_cache() -> Result<(), VfsError> {
        let path = unique_cache_path("schema_version_mismatch");
        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 3);
        }
        {
            let connection = Connection::open(&path).map_err(|_| sparse_cache_error())?;
            connection
                .execute(
                    "UPDATE sparse_cache_config SET value = '999' WHERE key = 'schema_version'",
                    [],
                )
                .map_err(|_| sparse_cache_error())?;
        }

        assert!(matches!(
            SparseCache::open(&path),
            Err(VfsError::CorruptStore { .. })
        ));

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn schema_rejects_invalid_domain_rows() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;

        assert!(
            cache
                .connection
                .execute(
                    "INSERT INTO sparse_cache_views
                    (repo_id, root_tree_id, commit_id, ref_name, ref_version)
                    VALUES ('local', ?1, NULL, 'main', 0)",
                    [object_id(b"bad ref version").to_hex()],
                )
                .is_err()
        );
        assert!(
            cache
                .connection
                .execute(
                    "INSERT INTO sparse_cache_inodes
                    (view_id, inode_id, node_kind, mode, uid, gid, nlink, size, block_size,
                     blocks, mtime_secs, mtime_nanos, ctime_secs, ctime_nanos,
                     custom_attrs_json, lookup_count)
                    VALUES (?1, 1, 'socket', 0, 0, 0, 1, 0, 4096, 0, 0, 0, 0, 0, '{}', 0)",
                    [view_id],
                )
                .is_err()
        );
        assert!(
            cache
                .connection
                .execute(
                    "INSERT INTO sparse_cache_inodes
                    (view_id, inode_id, node_kind, mode, uid, gid, nlink, size, block_size,
                     blocks, mtime_secs, mtime_nanos, ctime_secs, ctime_nanos,
                     custom_attrs_json, lookup_count)
                    VALUES (?1, 2, 'file', 0, 0, 0, 1, 0, 0, 0, 0, 1000000000, 0, 0, '{}', 0)",
                    [view_id],
                )
                .is_err()
        );
        assert!(
            cache
                .connection
                .execute(
                    "INSERT INTO sparse_cache_chunks
                    (repo_id, object_id, chunk_index, offset, byte_len, bytes)
                    VALUES ('local', ?1, 1, 1, 4, x'00010203')",
                    [object_id(b"bad chunk").to_hex()],
                )
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn symlink_targets_round_trip_without_hydrating_target() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        cache.put_inode(&cached_inode(
            1,
            CachedNodeKind::Symlink,
            Some(object_id(b"symlink")),
            Some(ObjectKind::Blob),
            0o120777,
            1,
        ))?;

        let symlink = CachedSymlink {
            view_id,
            inode_id: 1,
            target: "../not-yet-hydrated/target.md".to_string(),
            target_object_id: None,
        };

        cache.put_symlink(&symlink)?;

        assert_eq!(cache.get_symlink(view_id, 1)?, Some(symlink));
        assert_eq!(cache.get_inode(view_id, 99)?, None);

        Ok(())
    }

    #[test]
    fn statfs_counters_round_trip_for_a_view() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let statfs = CachedStatfs {
            view_id,
            inode_count: 9,
            file_count: 5,
            directory_count: 3,
            symlink_count: 1,
            bytes_used: 32_768,
            blocks_used: 8,
            block_size: 4096,
        };

        cache.put_statfs(&statfs)?;

        assert_eq!(cache.get_statfs(view_id)?, Some(statfs));

        Ok(())
    }

    #[test]
    fn forget_decrements_lookup_count_without_touching_durable_identity() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let inode = cached_inode(
            1,
            CachedNodeKind::File,
            Some(object_id(b"durable-file")),
            Some(ObjectKind::Blob),
            0o100644,
            1,
        );
        cache.put_inode(&inode)?;

        cache.record_lookup(view_id, 1)?;
        cache.record_lookup(view_id, 1)?;
        cache.forget(view_id, 1, 1)?;

        let cached = cache.get_inode(view_id, 1)?.unwrap();
        assert_eq!(cached.lookup_count, 1);
        assert_eq!(cached.object_id, inode.object_id);
        assert_eq!(cached.object_kind, inode.object_kind);
        assert_eq!(cached.nlink, inode.nlink);

        Ok(())
    }

    #[test]
    fn forgotten_unlinked_inode_can_be_pruned_after_lookup_count_reaches_zero()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let mut inode = cached_inode(
            1,
            CachedNodeKind::File,
            Some(object_id(b"unlinked-file")),
            Some(ObjectKind::Blob),
            0o100644,
            0,
        );
        inode.lookup_count = 1;
        cache.put_inode(&inode)?;

        assert_eq!(cache.prune_forgotten_unlinked()?, 0);

        cache.forget(view_id, 1, 1)?;

        assert_eq!(cache.prune_forgotten_unlinked()?, 1);
        assert_eq!(cache.get_inode(view_id, 1)?, None);

        Ok(())
    }

    #[test]
    fn lookup_count_saturates_before_sqlite_integer_overflow() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let mut inode = cached_inode(
            1,
            CachedNodeKind::File,
            Some(object_id(b"durable-file")),
            Some(ObjectKind::Blob),
            0o100644,
            1,
        );
        inode.lookup_count = i64::MAX as u64;
        cache.put_inode(&inode)?;

        cache.record_lookup(view_id, 1)?;

        assert_eq!(
            cache.get_inode(view_id, 1)?.unwrap().lookup_count,
            i64::MAX as u64
        );

        Ok(())
    }

    #[test]
    fn debug_output_redacts_payloads_paths_and_cache_identities() -> Result<(), VfsError> {
        let chunk = CachedChunk {
            repo_id: RepoId::new("local")?,
            object_id: object_id(b"secret-object"),
            chunk_index: 0,
            offset: 0,
            byte_len: 11,
            bytes: b"secret-data".to_vec(),
        };
        let dentry = cached_dentry(1, 1, "secret-name.txt", 2, "/secret-name.txt");
        let symlink = CachedSymlink {
            view_id: 1,
            inode_id: 2,
            target: "../secret-target".to_string(),
            target_object_id: Some(object_id(b"secret-target")),
        };

        let chunk_debug = format!("{chunk:?}");
        let dentry_debug = format!("{dentry:?}");
        let symlink_debug = format!("{symlink:?}");

        assert!(!chunk_debug.contains("local"));
        assert!(!chunk_debug.contains("secret-data"));
        assert!(!dentry_debug.contains("secret-name"));
        assert!(!symlink_debug.contains("secret-target"));

        Ok(())
    }

    fn config_row_count(path: &Path) -> Result<u32, VfsError> {
        let connection = Connection::open(path).map_err(|_| sparse_cache_error())?;
        connection
            .query_row("SELECT COUNT(*) FROM sparse_cache_config", [], |row| {
                row.get::<_, u32>(0)
            })
            .map_err(|_| sparse_cache_error())
    }

    fn unique_cache_path(label: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        path.push(format!(
            "stratum-{label}-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        path
    }

    fn cache_view_identity() -> CacheViewIdentity {
        CacheViewIdentity {
            repo_id: RepoId::new("local").unwrap(),
            root_tree_id: object_id(b"root tree"),
            commit_id: Some(CommitId::from(object_id(b"commit"))),
            ref_name: Some(RefName::new("main").unwrap()),
            ref_version: Some(7),
        }
    }

    fn cached_inode(
        inode_id: u64,
        node_kind: CachedNodeKind,
        object_id: Option<ObjectId>,
        object_kind: Option<ObjectKind>,
        mode: u32,
        nlink: u64,
    ) -> CachedInode {
        CachedInode {
            view_id: 1,
            inode_id,
            node_kind,
            object_id,
            object_kind,
            mode,
            uid: 501,
            gid: 20,
            nlink,
            size: 0,
            size_known: true,
            block_size: 4096,
            blocks: 0,
            mtime_secs: 1,
            mtime_nanos: 2,
            ctime_secs: 3,
            ctime_nanos: 4,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
            lookup_count: 0,
        }
    }

    fn cached_dentry(
        view_id: i64,
        parent_inode_id: u64,
        name: &str,
        child_inode_id: u64,
        path: &str,
    ) -> CachedDentry {
        CachedDentry {
            view_id,
            parent_inode_id,
            name: name.to_string(),
            child_inode_id,
            path: path.to_string(),
        }
    }

    fn tree_hydration_target(view_id: i64, path_seed: &str) -> HydrationJobTarget {
        HydrationJobTarget {
            view_id,
            scope: HydrationJobScope::Tree,
            object_id: object_id(path_seed.as_bytes()),
            object_kind: ObjectKind::Tree,
            chunk_index: None,
            path: format!("/{path_seed}"),
        }
    }

    fn object_id(seed: &[u8]) -> ObjectId {
        ObjectId::from_bytes(seed)
    }

    fn sparse_cache_error() -> VfsError {
        VfsError::CorruptStore {
            message: "sparse cache operation failed".to_string(),
        }
    }
}
