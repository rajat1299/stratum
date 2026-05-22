//! Read-only mount adapter over sparse-cache metadata and chunks.

use crate::backend::RepoId;
use crate::error::VfsError;
use crate::mount_adapter::{
    MountAttr, MountDirEntry, MountError, MountErrorCode, MountFileKind, MountFileSize,
    MountReadAdapter, MountStatfs, MountTimestamp,
};
use crate::sparse_cache::{
    CachedChunk, CachedInode, CachedNodeKind, SPARSE_CACHE_ERROR, SparseCache,
};
use crate::store::{ObjectId, ObjectKind};

/// Provider-free chunk source for read-through sparse-cache mount reads.
pub trait SparseMountBlobSource {
    /// Loads one blob chunk by durable identity and chunk shape.
    ///
    /// Returning `Ok(None)` leaves the cache miss unresolved.
    fn load_chunk(
        &self,
        repo_id: &RepoId,
        object_id: ObjectId,
        chunk_index: u64,
        chunk_size: u32,
    ) -> Result<Option<Vec<u8>>, VfsError>;
}

impl SparseMountBlobSource for () {
    fn load_chunk(
        &self,
        _repo_id: &RepoId,
        _object_id: ObjectId,
        _chunk_index: u64,
        _chunk_size: u32,
    ) -> Result<Option<Vec<u8>>, VfsError> {
        Ok(None)
    }
}

/// Read-only mount adapter for one sparse-cache view.
#[derive(Clone, Copy)]
pub struct SparseCacheMount<'a, S = ()> {
    cache: &'a SparseCache,
    view_id: i64,
    source: Option<&'a S>,
}

impl<'a> SparseCacheMount<'a> {
    /// Creates an adapter over cached metadata without a blob source.
    #[must_use]
    pub const fn new(cache: &'a SparseCache, view_id: i64) -> Self {
        Self {
            cache,
            view_id,
            source: None,
        }
    }

    /// Adds a provider-free blob source used only by `read`.
    #[must_use]
    pub const fn with_source<T>(self, source: &'a T) -> SparseCacheMount<'a, T> {
        SparseCacheMount {
            cache: self.cache,
            view_id: self.view_id,
            source: Some(source),
        }
    }
}

impl<S> MountReadAdapter for SparseCacheMount<'_, S>
where
    S: SparseMountBlobSource,
{
    fn lookup(&self, parent_ino: u64, name: &str) -> Result<Option<MountAttr>, MountError> {
        let Some(parent) = self.inode(parent_ino)? else {
            return Ok(None);
        };
        if parent.node_kind != CachedNodeKind::Directory {
            return Err(MountError::new(MountErrorCode::NotDirectory));
        }

        let entries = self.dentries(parent_ino)?;
        let Some(entry) = entries.into_iter().find(|entry| entry.name == name) else {
            return Ok(None);
        };
        self.getattr(entry.child_inode_id)
    }

    fn getattr(&self, ino: u64) -> Result<Option<MountAttr>, MountError> {
        self.inode(ino)?
            .map(|inode| attr_from_inode(&inode))
            .transpose()
    }

    fn readdir(&self, ino: u64) -> Result<Option<Vec<String>>, MountError> {
        let Some(inode) = self.inode(ino)? else {
            return Ok(None);
        };
        if inode.node_kind != CachedNodeKind::Directory {
            return Err(MountError::new(MountErrorCode::NotDirectory));
        }
        Ok(Some(
            self.dentries(ino)?
                .into_iter()
                .map(|entry| entry.name)
                .collect(),
        ))
    }

    fn readdir_plus(&self, ino: u64) -> Result<Option<Vec<MountDirEntry>>, MountError> {
        let Some(inode) = self.inode(ino)? else {
            return Ok(None);
        };
        if inode.node_kind != CachedNodeKind::Directory {
            return Err(MountError::new(MountErrorCode::NotDirectory));
        }

        let mut entries = Vec::new();
        for entry in self.dentries(ino)? {
            if let Some(attr) = self.getattr(entry.child_inode_id)? {
                entries.push(MountDirEntry::new(entry.name, attr));
            }
        }
        Ok(Some(entries))
    }

    fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, MountError> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let Some(inode) = self.inode(ino)? else {
            return Err(MountError::new(MountErrorCode::NotFound));
        };
        if inode.node_kind != CachedNodeKind::File {
            return Err(MountError::new(MountErrorCode::IsDirectory));
        }
        let object_id = match (inode.object_id, inode.object_kind) {
            (Some(object_id), Some(ObjectKind::Blob)) => object_id,
            (None, None) => return Err(MountError::new(MountErrorCode::NotFound)),
            _ => return Err(MountError::new(MountErrorCode::Io)),
        };

        if inode.size_known && offset >= inode.size {
            return Ok(Vec::new());
        }

        let chunk_size = u64::from(self.cache.chunk_size().map_err(redact_vfs_error)?);
        let read_end = offset
            .checked_add(u64::from(size))
            .ok_or_else(|| MountError::new(MountErrorCode::InvalidInput))?;
        let target_end = if inode.size_known {
            read_end.min(inode.size)
        } else {
            read_end
        };
        let repo_id = self
            .cache
            .get_view_identity(self.view_id)
            .map_err(redact_vfs_error)?
            .repo_id;
        let mut bytes = Vec::new();
        let mut cursor = offset;

        while cursor < target_end {
            let chunk_index = cursor / chunk_size;
            let chunk_offset = cursor % chunk_size;
            let expected_len = if inode.size_known {
                Some(expected_known_chunk_len(&inode, chunk_index, chunk_size)?)
            } else {
                None
            };
            let Some(chunk) =
                self.load_or_fill_chunk(&repo_id, object_id, chunk_index, expected_len)?
            else {
                return Err(MountError::new(MountErrorCode::Io));
            };
            let chunk_len = chunk.bytes.len() as u64;

            if let Some(expected_len) = expected_len {
                if chunk_len < expected_len {
                    return Err(MountError::new(MountErrorCode::Io));
                }
            }

            if !inode.size_known && chunk_len < chunk_size {
                self.record_known_size(&inode, chunk_index * chunk_size + chunk_len)?;
            }

            if chunk_offset >= chunk_len {
                break;
            }

            let available = chunk_len - chunk_offset;
            let wanted = (target_end - cursor).min(available);
            let start = usize::try_from(chunk_offset)
                .map_err(|_| MountError::new(MountErrorCode::InvalidInput))?;
            let end = usize::try_from(chunk_offset + wanted)
                .map_err(|_| MountError::new(MountErrorCode::InvalidInput))?;
            bytes.extend_from_slice(&chunk.bytes[start..end]);
            cursor += wanted;

            if chunk_len < chunk_size {
                break;
            }
        }

        Ok(bytes)
    }

    fn readlink(&self, ino: u64) -> Result<Option<String>, MountError> {
        let Some(inode) = self.inode(ino)? else {
            return Ok(None);
        };
        if inode.node_kind != CachedNodeKind::Symlink {
            return Ok(None);
        }
        let symlink = self
            .cache
            .get_symlink(self.view_id, ino)
            .map_err(redact_vfs_error)?
            .ok_or_else(|| MountError::new(MountErrorCode::Io))?;
        Ok(Some(symlink.target))
    }

    fn statfs(&self) -> Result<MountStatfs, MountError> {
        let statfs = self
            .cache
            .get_statfs(self.view_id)
            .map_err(redact_vfs_error)?
            .ok_or_else(|| MountError::new(MountErrorCode::NotFound))?;
        let block_size = u32::try_from(statfs.block_size)
            .map_err(|_| MountError::new(MountErrorCode::InvalidInput))?;
        Ok(MountStatfs::new(
            statfs.inode_count,
            statfs.file_count,
            statfs.directory_count,
            statfs.symlink_count,
            statfs.bytes_used,
            statfs.blocks_used,
            block_size,
        ))
    }
}

impl<S> SparseCacheMount<'_, S>
where
    S: SparseMountBlobSource,
{
    fn inode(&self, ino: u64) -> Result<Option<CachedInode>, MountError> {
        self.cache
            .get_inode(self.view_id, ino)
            .map_err(redact_vfs_error)
    }

    fn dentries(&self, ino: u64) -> Result<Vec<crate::sparse_cache::CachedDentry>, MountError> {
        self.cache
            .list_dentries(self.view_id, ino)
            .map_err(redact_vfs_error)
    }

    fn load_or_fill_chunk(
        &self,
        repo_id: &RepoId,
        object_id: ObjectId,
        chunk_index: u64,
        expected_min_len: Option<u64>,
    ) -> Result<Option<CachedChunk>, MountError> {
        if let Some(chunk) = self
            .cache
            .get_chunk(repo_id, object_id, chunk_index)
            .map_err(redact_vfs_error)?
        {
            return Ok(Some(chunk));
        }

        let Some(source) = self.source else {
            return Ok(None);
        };
        let chunk_size = self.cache.chunk_size().map_err(redact_vfs_error)?;
        let Some(bytes) = source
            .load_chunk(repo_id, object_id, chunk_index, chunk_size)
            .map_err(redact_vfs_error)?
        else {
            return Ok(None);
        };
        if let Some(expected_min_len) = expected_min_len {
            if (bytes.len() as u64) < expected_min_len {
                return Err(MountError::new(MountErrorCode::Io));
            }
        }
        let chunk = CachedChunk {
            repo_id: repo_id.clone(),
            object_id,
            chunk_index,
            offset: chunk_index
                .checked_mul(u64::from(chunk_size))
                .ok_or_else(|| MountError::new(MountErrorCode::InvalidInput))?,
            byte_len: bytes.len() as u64,
            bytes,
        };
        self.cache.put_chunk(&chunk).map_err(redact_vfs_error)?;
        Ok(Some(chunk))
    }

    fn record_known_size(&self, inode: &CachedInode, size: u64) -> Result<(), MountError> {
        let mut updated = inode.clone();
        updated.size = size;
        updated.size_known = true;
        self.cache.put_inode(&updated).map_err(redact_vfs_error)
    }
}

fn expected_known_chunk_len(
    inode: &CachedInode,
    chunk_index: u64,
    chunk_size: u64,
) -> Result<u64, MountError> {
    let chunk_start = chunk_index
        .checked_mul(chunk_size)
        .ok_or_else(|| MountError::new(MountErrorCode::InvalidInput))?;
    if chunk_start >= inode.size {
        return Ok(0);
    }
    Ok((inode.size - chunk_start).min(chunk_size))
}

fn attr_from_inode(inode: &CachedInode) -> Result<MountAttr, MountError> {
    let kind = match inode.node_kind {
        CachedNodeKind::File => MountFileKind::File,
        CachedNodeKind::Directory => MountFileKind::Directory,
        CachedNodeKind::Symlink => MountFileKind::Symlink,
    };
    let size = if inode.size_known {
        MountFileSize::Known(inode.size)
    } else {
        MountFileSize::Unknown
    };
    let mut attr = MountAttr::new(inode.inode_id, kind, size);
    attr.mode = inode.mode;
    attr.uid = inode.uid;
    attr.gid = inode.gid;
    attr.nlink =
        u32::try_from(inode.nlink).map_err(|_| MountError::new(MountErrorCode::InvalidInput))?;
    attr.block_size = u32::try_from(inode.block_size)
        .map_err(|_| MountError::new(MountErrorCode::InvalidInput))?;
    attr.blocks = inode.blocks;
    attr.modified = MountTimestamp::new(
        i64::try_from(inode.mtime_secs)
            .map_err(|_| MountError::new(MountErrorCode::InvalidInput))?,
        inode.mtime_nanos,
    );
    attr.changed = MountTimestamp::new(
        i64::try_from(inode.ctime_secs)
            .map_err(|_| MountError::new(MountErrorCode::InvalidInput))?,
        inode.ctime_nanos,
    );
    Ok(attr)
}

fn redact_vfs_error(error: VfsError) -> MountError {
    match error {
        VfsError::InvalidArgs { .. } => MountError::new(MountErrorCode::InvalidInput),
        VfsError::CorruptStore { message } if message == SPARSE_CACHE_ERROR => {
            MountError::new(MountErrorCode::Io)
        }
        _ => MountError::new(MountErrorCode::Io),
    }
}

#[cfg(test)]
mod tests {
    use super::{SparseCacheMount, SparseMountBlobSource};
    use crate::backend::RepoId;
    use crate::error::VfsError;
    use crate::mount_adapter::{MountErrorCode, MountFileKind, MountFileSize, MountReadAdapter};
    use crate::sparse_cache::{
        CacheViewIdentity, CachedChunk, CachedDentry, CachedInode, CachedNodeKind, CachedStatfs,
        CachedSymlink, SparseCache,
    };
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, RefName};
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Default)]
    struct RecordingSource {
        chunks: RefCell<BTreeMap<u64, Option<Vec<u8>>>>,
        calls: RefCell<Vec<u64>>,
    }

    impl RecordingSource {
        fn with_chunk(self, chunk_index: u64, bytes: impl Into<Vec<u8>>) -> Self {
            self.chunks
                .borrow_mut()
                .insert(chunk_index, Some(bytes.into()));
            self
        }

        fn calls(&self) -> Vec<u64> {
            self.calls.borrow().clone()
        }
    }

    impl SparseMountBlobSource for RecordingSource {
        fn load_chunk(
            &self,
            _repo_id: &RepoId,
            _object_id: ObjectId,
            chunk_index: u64,
            _chunk_size: u32,
        ) -> Result<Option<Vec<u8>>, VfsError> {
            self.calls.borrow_mut().push(chunk_index);
            Ok(self.chunks.borrow().get(&chunk_index).cloned().flatten())
        }
    }

    #[test]
    fn lookup_getattr_and_readdir_use_cached_metadata() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let lookup = mount.lookup(1, "alpha").unwrap().unwrap();
        let attr = mount.getattr(2).unwrap().unwrap();
        let names = mount.readdir(1).unwrap().unwrap();
        let link = mount.readlink(3).unwrap().unwrap();

        assert_eq!(lookup.ino, 2);
        assert_eq!(attr.kind, MountFileKind::File);
        assert_eq!(names, vec!["alpha".to_string(), "link".to_string()]);
        assert_eq!(link, "../alpha");
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn readdir_plus_returns_child_attrs_without_blob_reads() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let entries = mount.readdir_plus(1).unwrap().unwrap();
        let attr_kinds = entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.attr.kind))
            .collect::<Vec<_>>();

        assert_eq!(
            attr_kinds,
            vec![
                ("alpha", MountFileKind::File),
                ("link", MountFileKind::Symlink)
            ]
        );
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn readdir_plus_preserves_unknown_child_size_without_blob_reads() -> Result<(), VfsError> {
        let fixture = CacheFixture::new_unknown_size()?;
        let source = RecordingSource::default().with_chunk(0, b"loaded");
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let entries = mount.readdir_plus(1).unwrap().unwrap();
        let file_entry = entries
            .iter()
            .find(|entry| entry.name == "alpha")
            .expect("file dentry should exist");

        assert_eq!(file_entry.attr.size, MountFileSize::Unknown);
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn statfs_maps_cached_view_counters() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let statfs = mount.statfs().unwrap();

        assert_eq!(statfs.inode_count, 3);
        assert_eq!(statfs.file_count, 1);
        assert_eq!(statfs.directory_count, 1);
        assert_eq!(statfs.symlink_count, 1);
        assert_eq!(statfs.bytes_used, 18);
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn read_assembles_cached_chunks_for_cat_behavior() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        fixture.set_file_size(4101)?;
        let mut first_chunk = vec![b'x'; 4096];
        first_chunk[4090..4096].copy_from_slice(b"hello ");
        fixture.put_chunk(0, first_chunk)?;
        fixture.put_chunk(1, b"world".to_vec())?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let bytes = mount.read(2, 4090, 11).unwrap();

        assert_eq!(bytes, b"hello world");
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn unknown_size_getattr_does_not_call_blob_source() -> Result<(), VfsError> {
        let fixture = CacheFixture::new_unknown_size()?;
        let source = RecordingSource::default().with_chunk(0, b"loaded");
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let attr = mount.getattr(2).unwrap().unwrap();

        assert_eq!(attr.size, MountFileSize::Unknown);
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn unknown_size_read_loads_missing_chunk_and_records_real_size_at_eof() -> Result<(), VfsError>
    {
        let fixture = CacheFixture::new_unknown_size()?;
        let source = RecordingSource::default().with_chunk(0, b"terminal");
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let bytes = mount.read(2, 0, 64).unwrap();
        let attr = mount.getattr(2).unwrap().unwrap();

        assert_eq!(bytes, b"terminal");
        assert_eq!(source.calls(), vec![0]);
        assert_eq!(attr.size, MountFileSize::Known(8));
        Ok(())
    }

    #[test]
    fn known_size_missing_chunk_is_redacted_error_not_eof() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let error = mount.read(2, 0, 64).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        assert_eq!(source.calls(), vec![0]);
        Ok(())
    }

    #[test]
    fn known_size_missing_middle_chunk_is_redacted_error_not_truncation() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        fixture.set_file_size(8193)?;
        fixture.put_chunk(0, vec![b'a'; 4096])?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let error = mount.read(2, 0, 8193).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        assert_eq!(source.calls(), vec![1]);
        Ok(())
    }

    #[test]
    fn unknown_size_missing_chunk_is_redacted_error_not_eof() -> Result<(), VfsError> {
        let fixture = CacheFixture::new_unknown_size()?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let error = mount.read(2, 0, 64).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        assert_eq!(source.calls(), vec![0]);
        Ok(())
    }

    #[test]
    fn short_provider_chunk_before_known_eof_is_not_cached() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        fixture.set_file_size(8192)?;
        let source = RecordingSource::default().with_chunk(0, b"short");
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let error = mount.read(2, 0, 64).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        assert!(
            fixture
                .cache
                .get_chunk(&fixture.repo_id, fixture.file_object_id, 0)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn short_cached_chunk_before_known_eof_is_redacted_error() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        fixture.set_file_size(8192)?;
        fixture.put_chunk(0, b"short".to_vec())?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let error = mount.read(2, 0, 64).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn file_inode_with_non_blob_object_kind_is_redacted_error() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        fixture.cache.put_inode(&cached_inode(
            fixture.view_id,
            4,
            CachedNodeKind::File,
            Some(object_id(b"tree object")),
            Some(ObjectKind::Tree),
            0o100644,
            10,
            true,
        ))?;
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id);

        let error = mount.read(4, 0, 1).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        Ok(())
    }

    #[test]
    fn symlink_inode_without_target_row_is_redacted_error() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        fixture.cache.put_inode(&cached_inode(
            fixture.view_id,
            4,
            CachedNodeKind::Symlink,
            None,
            None,
            0o120777,
            8,
            true,
        ))?;
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id);

        let error = mount.readlink(4).unwrap_err();

        assert_eq!(error.code(), MountErrorCode::Io);
        Ok(())
    }

    #[test]
    fn read_past_unknown_size_eof_returns_empty() -> Result<(), VfsError> {
        let fixture = CacheFixture::new_unknown_size()?;
        fixture.put_chunk(0, b"short".to_vec())?;
        let source = RecordingSource::default();
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id).with_source(&source);

        let bytes = mount.read(2, 10, 64).unwrap();

        assert!(bytes.is_empty());
        assert!(source.calls().is_empty());
        Ok(())
    }

    #[test]
    fn missing_inode_and_invalid_directory_errors_are_redacted() -> Result<(), VfsError> {
        let fixture = CacheFixture::new()?;
        let mount = SparseCacheMount::new(&fixture.cache, fixture.view_id);

        let missing = mount.read(99, 0, 1).unwrap_err();
        let not_directory = mount.readdir(2).unwrap_err();
        let debug = format!("{missing:?} {not_directory:?}");
        let forbidden = BTreeSet::from(["alpha", "repo", "object", "hello", "../alpha"]);

        assert_eq!(missing.code(), MountErrorCode::NotFound);
        assert_eq!(not_directory.code(), MountErrorCode::NotDirectory);
        assert!(forbidden.iter().all(|value| !debug.contains(value)));
        Ok(())
    }

    struct CacheFixture {
        cache: SparseCache,
        view_id: i64,
        repo_id: RepoId,
        file_object_id: ObjectId,
    }

    impl CacheFixture {
        fn new() -> Result<Self, VfsError> {
            Self::with_size_known(true)
        }

        fn new_unknown_size() -> Result<Self, VfsError> {
            Self::with_size_known(false)
        }

        fn with_size_known(size_known: bool) -> Result<Self, VfsError> {
            let cache = SparseCache::open_in_memory()?;
            let repo_id = RepoId::new("repo").unwrap();
            let file_object_id = object_id(b"file object");
            let view_id = cache.insert_view(&CacheViewIdentity {
                repo_id: repo_id.clone(),
                root_tree_id: object_id(b"root tree"),
                commit_id: Some(CommitId::from(object_id(b"commit"))),
                ref_name: Some(RefName::new("main").unwrap()),
                ref_version: Some(1),
            })?;
            cache.put_inode(&cached_inode(
                view_id,
                1,
                CachedNodeKind::Directory,
                None,
                None,
                0o040755,
                0,
                true,
            ))?;
            cache.put_inode(&cached_inode(
                view_id,
                2,
                CachedNodeKind::File,
                Some(file_object_id),
                Some(ObjectKind::Blob),
                0o100644,
                18,
                size_known,
            ))?;
            cache.put_inode(&cached_inode(
                view_id,
                3,
                CachedNodeKind::Symlink,
                None,
                None,
                0o120777,
                8,
                true,
            ))?;
            cache.put_dentry(&CachedDentry {
                view_id,
                parent_inode_id: 1,
                name: "alpha".to_string(),
                child_inode_id: 2,
                path: "/alpha".to_string(),
            })?;
            cache.put_dentry(&CachedDentry {
                view_id,
                parent_inode_id: 1,
                name: "link".to_string(),
                child_inode_id: 3,
                path: "/link".to_string(),
            })?;
            cache.put_symlink(&CachedSymlink {
                view_id,
                inode_id: 3,
                target: "../alpha".to_string(),
                target_object_id: None,
            })?;
            cache.put_statfs(&CachedStatfs {
                view_id,
                inode_count: 3,
                file_count: 1,
                directory_count: 1,
                symlink_count: 1,
                bytes_used: 18,
                blocks_used: 2,
                block_size: 4096,
            })?;
            Ok(Self {
                cache,
                view_id,
                repo_id,
                file_object_id,
            })
        }

        fn put_chunk(&self, chunk_index: u64, bytes: Vec<u8>) -> Result<(), VfsError> {
            self.cache.put_chunk(&CachedChunk {
                repo_id: self.repo_id.clone(),
                object_id: self.file_object_id,
                chunk_index,
                offset: chunk_index * 4096,
                byte_len: bytes.len() as u64,
                bytes,
            })
        }

        fn set_file_size(&self, size: u64) -> Result<(), VfsError> {
            self.cache.put_inode(&cached_inode(
                self.view_id,
                2,
                CachedNodeKind::File,
                Some(self.file_object_id),
                Some(ObjectKind::Blob),
                0o100644,
                size,
                true,
            ))
        }
    }

    fn cached_inode(
        view_id: i64,
        inode_id: u64,
        node_kind: CachedNodeKind,
        object_id: Option<ObjectId>,
        object_kind: Option<ObjectKind>,
        mode: u32,
        size: u64,
        size_known: bool,
    ) -> CachedInode {
        CachedInode {
            view_id,
            inode_id,
            node_kind,
            object_id,
            object_kind,
            mode,
            uid: 501,
            gid: 20,
            nlink: 1,
            size,
            size_known,
            block_size: 4096,
            blocks: 1,
            mtime_secs: 10,
            mtime_nanos: 20,
            ctime_secs: 30,
            ctime_nanos: 40,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
            lookup_count: 0,
        }
    }

    fn object_id(seed: &[u8]) -> ObjectId {
        ObjectId::from_bytes(seed)
    }
}
