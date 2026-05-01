#![cfg(feature = "fuser")]

use crate::auth::session::Session;
use crate::fs::VirtualFs;
use crate::fs::inode::InodeKind;
use crate::posix::{PosixFs, PosixSetAttr, PosixXattrSetMode};
use fuser::{
    BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, KernelConfig, MountOption, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(1);
const XATTR_CREATE: i32 = 1;
const XATTR_REPLACE: i32 = 2;

pub struct StratumFuse {
    fs: Arc<Mutex<VirtualFs>>,
}

impl StratumFuse {
    pub fn new(fs: Arc<Mutex<VirtualFs>>) -> Self {
        Self { fs }
    }

    pub fn mount_config(read_only: bool) -> Config {
        let mut mount_options = vec![
            MountOption::FSName("stratum".to_string()),
            MountOption::Subtype("stratum".to_string()),
            MountOption::AutoUnmount,
            MountOption::DefaultPermissions,
        ];
        if read_only {
            mount_options.push(MountOption::RO);
        } else {
            mount_options.push(MountOption::RW);
        }
        let mut config = Config::default();
        config.mount_options = mount_options;
        config
    }

    fn session_for(req: &Request) -> Session {
        Session::new(
            req.uid(),
            req.gid(),
            vec![req.gid()],
            format!("uid-{}", req.uid()),
        )
    }

    fn path_for_inode(fs: &VirtualFs, ino: INodeNo) -> Option<String> {
        if ino.0 == fs.root_id() {
            return Some("/".to_string());
        }
        fn walk(
            fs: &VirtualFs,
            current: u64,
            current_path: &str,
            target: INodeNo,
        ) -> Option<String> {
            let inode = fs.get_inode(current).ok()?;
            let InodeKind::Directory { entries } = &inode.kind else {
                return None;
            };

            for (name, child) in entries {
                let child_path = if current_path == "/" {
                    format!("/{name}")
                } else {
                    format!("{current_path}/{name}")
                };
                if *child == target.0 {
                    return Some(child_path);
                }
                if let Some(found) = walk(fs, *child, &child_path, target) {
                    return Some(found);
                }
            }
            None
        }

        walk(fs, fs.root_id(), "/", ino)
    }

    fn child_path(fs: &VirtualFs, parent: INodeNo, name: &OsStr) -> Option<String> {
        let parent_path = Self::path_for_inode(fs, parent)?;
        let name = name.to_str()?;
        Some(if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        })
    }
}

impl Filesystem for StratumFuse {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> io::Result<()> {
        Ok(())
    }

    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::child_path(&guard, parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let posix = PosixFs::new(&mut guard, &session);
        match posix.lookup(&path).map(|stat| stat_to_attr(&stat)) {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn getattr(&self, req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let posix = PosixFs::new(&mut guard, &session);
        match posix.getattr(&path).map(|stat| stat_to_attr(&stat)) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let posix = PosixFs::new(&mut guard, &session);
        match posix.readlink(&path) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::child_path(&guard, parent, name) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix
            .mkdir(&path, mode as u16)
            .and_then(|_| posix.getattr(&path))
            .map(|stat| stat_to_attr(&stat))
        {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::child_path(&guard, parent, name) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix
            .create(&path, mode as u16)
            .and_then(|_| posix.getattr(&path))
        {
            Ok(stat) => reply.created(
                &TTL,
                &stat_to_attr(&stat),
                Generation(0),
                FileHandle(0),
                FopenFlags::empty(),
            ),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let writable = matches!(
            flags.acc_mode(),
            OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR
        );
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.open(&path, writable) {
            Ok(_fh) => reply.opened(FileHandle(0), FopenFlags::empty()),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn read(
        &self,
        req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.read(&path, offset as usize, size as usize) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.write(&path, offset as usize, data) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::child_path(&guard, parent, name) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.unlink(&path) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::child_path(&guard, parent, name) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.rmdir(&path) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(src) = Self::child_path(&guard, parent, name) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let Some(dst) = Self::child_path(&guard, newparent, newname) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.rename(&src, &dst) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::child_path(&guard, parent, link_name) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let target = target.to_string_lossy().into_owned();
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix
            .symlink(&target, &path)
            .and_then(|_| posix.getattr(&path))
            .map(|stat| stat_to_attr(&stat))
        {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn link(
        &self,
        req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(target) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(path) = Self::child_path(&guard, newparent, newname) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix
            .link(&target, &path)
            .and_then(|_| posix.getattr(&path))
            .map(|stat| stat_to_attr(&stat))
        {
            Ok(attr) => reply.entry(&TTL, &attr, Generation(0)),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix
            .setattr(
                &path,
                PosixSetAttr {
                    mode: mode.map(|mode| mode as u16),
                    uid,
                    gid,
                    size: size.map(|size| size as usize),
                },
            )
            .map(|stat| stat_to_attr(&stat))
        {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => reply.error(map_error(err)),
        }
    }

    fn setxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if position != 0 {
            reply.error(Errno::ENOTSUP);
            return;
        }
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let mode = match xattr_set_mode(flags) {
            Ok(mode) => mode,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.setxattr(&path, name, value, mode) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(map_xattr_error(err)),
        }
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };

        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let posix = PosixFs::new(&mut guard, &session);
        match posix.getxattr(&path, name) {
            Ok(value) => reply_xattr(value, size, reply),
            Err(err) => reply.error(map_xattr_error(err)),
        }
    }

    fn listxattr(&self, req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let posix = PosixFs::new(&mut guard, &session);
        match posix.listxattr(&path) {
            Ok(names) => reply_xattr(xattr_list_payload(&names), size, reply),
            Err(err) => reply.error(map_xattr_error(err)),
        }
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };

        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let mut posix = PosixFs::new(&mut guard, &session);
        match posix.removexattr(&path, name) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(map_xattr_error(err)),
        }
    }

    fn readdir(
        &self,
        req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let mut guard = self.fs.lock().expect("fuse mutex poisoned");
        let Some(path) = Self::path_for_inode(&guard, ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let session = Self::session_for(req);
        let posix = PosixFs::new(&mut guard, &session);
        match posix.readdir(&path) {
            Ok(entries) => {
                if offset == 0 {
                    let _ = reply.add(ino, 1, FileType::Directory, ".");
                    let parent_ino = if path == "/" {
                        ino
                    } else {
                        let parent_path = Path::new(&path)
                            .parent()
                            .map(|p| p.to_string_lossy().into_owned())
                            .unwrap_or_else(|| "/".to_string());
                        let parent_path = if parent_path.is_empty() {
                            "/".to_string()
                        } else {
                            parent_path
                        };
                        guard
                            .stat(&parent_path)
                            .map(|stat| INodeNo(stat.inode_id))
                            .unwrap_or(ino)
                    };
                    let _ = reply.add(parent_ino, 2, FileType::Directory, "..");
                }
                let start = offset.saturating_sub(2) as usize;
                for (entry_index, entry) in entries.into_iter().enumerate().skip(start) {
                    let full = (entry_index + 3) as u64;
                    let full_type = file_type(entry.stat.kind);
                    if reply.add(INodeNo(entry.stat.inode_id), full, full_type, entry.name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(err) => reply.error(map_error(err)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum XattrResponse {
    Size(u32),
    Data(Vec<u8>),
}

fn xattr_set_mode(flags: i32) -> Result<PosixXattrSetMode, Errno> {
    match flags {
        0 => Ok(PosixXattrSetMode::Upsert),
        XATTR_CREATE => Ok(PosixXattrSetMode::CreateOnly),
        XATTR_REPLACE => Ok(PosixXattrSetMode::ReplaceOnly),
        _ => Err(Errno::EINVAL),
    }
}

fn xattr_list_payload(names: &[String]) -> Vec<u8> {
    let mut payload = Vec::new();
    for name in names {
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
    }
    payload
}

fn sized_xattr_response(data: &[u8], size: u32) -> Result<XattrResponse, Errno> {
    if size == 0 {
        let len = u32::try_from(data.len()).map_err(|_| Errno::ERANGE)?;
        return Ok(XattrResponse::Size(len));
    }
    if data.len() > size as usize {
        return Err(Errno::ERANGE);
    }
    Ok(XattrResponse::Data(data.to_vec()))
}

fn reply_xattr(data: Vec<u8>, size: u32, reply: ReplyXattr) {
    match sized_xattr_response(&data, size) {
        Ok(XattrResponse::Size(len)) => reply.size(len),
        Ok(XattrResponse::Data(data)) => reply.data(&data),
        Err(errno) => reply.error(errno),
    }
}

fn stat_to_attr(stat: &crate::fs::StatInfo) -> FileAttr {
    let atime = UNIX_EPOCH + Duration::new(stat.accessed, stat.accessed_nanos);
    let mtime = UNIX_EPOCH + Duration::new(stat.modified, stat.modified_nanos);
    let ctime = UNIX_EPOCH + Duration::new(stat.changed, stat.changed_nanos);
    let crtime = UNIX_EPOCH + Duration::new(stat.created, stat.created_nanos);

    FileAttr {
        ino: INodeNo(stat.inode_id),
        size: stat.size,
        blocks: stat.blocks,
        atime,
        mtime,
        ctime,
        crtime,
        kind: file_type(stat.kind),
        perm: stat.mode,
        nlink: stat.nlink as u32,
        uid: stat.uid,
        gid: stat.gid,
        rdev: 0,
        blksize: stat.block_size as u32,
        flags: 0,
    }
}

fn file_type(kind: &str) -> FileType {
    match kind {
        "directory" => FileType::Directory,
        "symlink" => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

fn map_error(err: crate::error::VfsError) -> Errno {
    match err {
        crate::error::VfsError::NotFound { .. } => Errno::ENOENT,
        crate::error::VfsError::PermissionDenied { .. } => Errno::EACCES,
        crate::error::VfsError::AlreadyExists { .. } => Errno::EEXIST,
        crate::error::VfsError::NotDirectory { .. } => Errno::ENOTDIR,
        crate::error::VfsError::IsDirectory { .. } => Errno::EISDIR,
        crate::error::VfsError::NotEmpty { .. } => Errno::ENOTEMPTY,
        _ => Errno::EINVAL,
    }
}

fn map_xattr_error(err: crate::error::VfsError) -> Errno {
    match err {
        crate::error::VfsError::NotFound { .. } => Errno::NO_XATTR,
        err => map_error(err),
    }
}

pub fn mount(
    fs: Arc<Mutex<VirtualFs>>,
    mountpoint: PathBuf,
    read_only: bool,
) -> Result<(), std::io::Error> {
    let config = StratumFuse::mount_config(read_only);
    fuser::mount2(StratumFuse::new(fs), mountpoint, &config)
}

#[cfg(all(test, feature = "fuser"))]
mod tests {
    use super::*;

    mod xattr {
        use super::*;

        #[test]
        fn list_payload_encodes_names_as_nul_terminated_strings() {
            let names = vec![
                "user.stratum.mime_type".to_string(),
                "user.stratum.custom.owner".to_string(),
            ];

            assert_eq!(
                xattr_list_payload(&names),
                b"user.stratum.mime_type\0user.stratum.custom.owner\0"
            );
        }

        #[test]
        fn sized_reply_reports_size_when_requested_size_is_zero() {
            assert_eq!(
                sized_xattr_response(b"text/plain", 0).unwrap(),
                XattrResponse::Size(10)
            );
        }

        #[test]
        fn sized_reply_returns_data_when_buffer_is_large_enough() {
            assert_eq!(
                sized_xattr_response(b"text/plain", 10).unwrap(),
                XattrResponse::Data(b"text/plain".to_vec())
            );
        }

        #[test]
        fn sized_reply_rejects_too_small_buffers_with_erange() {
            assert_errno(
                sized_xattr_response(b"text/plain", 9).unwrap_err(),
                Errno::ERANGE,
            );
        }

        #[test]
        fn flags_convert_to_posix_set_modes() {
            assert_eq!(
                xattr_set_mode(0).unwrap(),
                crate::posix::PosixXattrSetMode::Upsert
            );
            assert_eq!(
                xattr_set_mode(1).unwrap(),
                crate::posix::PosixXattrSetMode::CreateOnly
            );
            assert_eq!(
                xattr_set_mode(2).unwrap(),
                crate::posix::PosixXattrSetMode::ReplaceOnly
            );
        }

        #[test]
        fn flags_reject_unsupported_combinations() {
            assert_errno(xattr_set_mode(3).unwrap_err(), Errno::EINVAL);
            assert_errno(xattr_set_mode(4).unwrap_err(), Errno::EINVAL);
            assert_errno(xattr_set_mode(-1).unwrap_err(), Errno::EINVAL);
        }

        fn assert_errno(actual: Errno, expected: Errno) {
            assert_eq!(actual.code(), expected.code());
        }
    }
}
