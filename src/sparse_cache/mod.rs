use crate::backend::RepoId;
use crate::error::VfsError;
use crate::store::{ObjectId, ObjectKind};
use crate::vcs::{CommitId, RefName};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

const SCHEMA_VERSION: u32 = 1;
const CHUNK_SIZE: u32 = 4096;
const MAX_CACHE_PATH_COMPONENT_LEN: usize = 255;
const SPARSE_CACHE_ERROR: &str = "sparse cache operation failed";

pub struct SparseCache {
    connection: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedDentry {
    pub view_id: i64,
    pub parent_inode_id: u64,
    pub name: String,
    pub child_inode_id: u64,
    pub path: String,
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
        if identity.ref_name.is_some() && identity.ref_version.is_none() {
            return Err(VfsError::InvalidArgs {
                message: "sparse cache ref view requires ref version".to_string(),
            });
        }
        if identity.ref_name.is_none() && identity.ref_version.is_some() {
            return Err(VfsError::InvalidArgs {
                message: "sparse cache ref version requires ref name".to_string(),
            });
        }

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
        let custom_attrs_json =
            serde_json::to_string(&inode.custom_attrs).map_err(|_| sparse_cache_error())?;
        let object_kind = inode.object_kind.map(object_kind_text);
        self.connection
            .execute(
                "INSERT OR REPLACE INTO sparse_cache_inodes
                (view_id, inode_id, node_kind, object_id, object_kind, mode, uid, gid, nlink,
                 size, block_size, blocks, mtime_secs, mtime_nanos, ctime_secs, ctime_nanos,
                 mime_type, custom_attrs_json, lookup_count)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                        ?17, ?18, ?19)",
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
                        nlink, size, block_size, blocks, mtime_secs, mtime_nanos, ctime_secs,
                        ctime_nanos, mime_type, custom_attrs_json, lookup_count
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
    let custom_attrs_json: String = row.get(17)?;

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
            block_size: u64_from_i64(row.get(10).map_err(|_| sparse_cache_error())?)?,
            blocks: u64_from_i64(row.get(11).map_err(|_| sparse_cache_error())?)?,
            mtime_secs: u64_from_i64(row.get(12).map_err(|_| sparse_cache_error())?)?,
            mtime_nanos: u32_from_i64(row.get(13).map_err(|_| sparse_cache_error())?)?,
            ctime_secs: u64_from_i64(row.get(14).map_err(|_| sparse_cache_error())?)?,
            ctime_nanos: u32_from_i64(row.get(15).map_err(|_| sparse_cache_error())?)?,
            mime_type: row.get(16).map_err(|_| sparse_cache_error())?,
            custom_attrs: serde_json::from_str(&custom_attrs_json)
                .map_err(|_| sparse_cache_error())?,
            lookup_count: u64_from_i64(row.get(18).map_err(|_| sparse_cache_error())?)?,
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
    connection
        .execute_batch(include_str!("schema.sql"))
        .map_err(|_| sparse_cache_error())?;
    connection
        .execute(
            "INSERT OR IGNORE INTO sparse_cache_config (key, value) VALUES (?1, ?2)",
            ("schema_version", SCHEMA_VERSION.to_string()),
        )
        .map_err(|_| sparse_cache_error())?;
    connection
        .execute(
            "INSERT OR IGNORE INTO sparse_cache_config (key, value) VALUES (?1, ?2)",
            ("chunk_size", CHUNK_SIZE.to_string()),
        )
        .map_err(|_| sparse_cache_error())?;
    Ok(SparseCache { connection })
}

fn sparse_cache_error() -> VfsError {
    VfsError::CorruptStore {
        message: SPARSE_CACHE_ERROR.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CacheViewIdentity, CachedDentry, CachedInode, CachedNodeKind, SparseCache,
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

        assert_eq!(cache.schema_version()?, 1);
        assert_eq!(cache.chunk_size()?, 4096);

        Ok(())
    }

    #[test]
    fn reopens_existing_cache_without_recreating_identity_rows() -> Result<(), VfsError> {
        let path = unique_cache_path("reopens_existing_cache");

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 1);
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

        assert_eq!(cache.list_dentries(view_id, 1)?[0].path, "/README.md");

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

    fn object_id(seed: &[u8]) -> ObjectId {
        ObjectId::from_bytes(seed)
    }

    fn sparse_cache_error() -> VfsError {
        VfsError::CorruptStore {
            message: "sparse cache operation failed".to_string(),
        }
    }
}
