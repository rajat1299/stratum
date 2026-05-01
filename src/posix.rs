use crate::auth::perms::Access;
use crate::auth::session::Session;
use crate::auth::{Gid, Uid};
use crate::error::VfsError;
use crate::fs::{HandleId, LsEntry, MetadataUpdate, StatInfo, VirtualFs};

pub const STRATUM_MIME_XATTR: &str = "user.stratum.mime_type";
pub const STRATUM_CUSTOM_XATTR_PREFIX: &str = "user.stratum.custom.";

#[derive(Debug, Clone)]
pub struct PosixDirEntry {
    pub name: String,
    pub stat: StatInfo,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PosixSetAttr {
    pub mode: Option<u16>,
    pub uid: Option<Uid>,
    pub gid: Option<Gid>,
    pub size: Option<usize>,
}

pub struct PosixFs<'a> {
    fs: &'a mut VirtualFs,
    session: &'a Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PosixXattrSetMode {
    Upsert,
    CreateOnly,
    ReplaceOnly,
}

impl<'a> PosixFs<'a> {
    pub fn new(fs: &'a mut VirtualFs, session: &'a Session) -> Self {
        Self { fs, session }
    }

    pub fn getattr(&self, path: &str) -> Result<StatInfo, VfsError> {
        let _ = self.fs.resolve_path_checked(path, self.session)?;
        self.fs.stat(path)
    }

    pub fn lookup(&self, path: &str) -> Result<StatInfo, VfsError> {
        self.getattr(path)
    }

    pub fn opendir(&mut self, path: &str) -> Result<HandleId, VfsError> {
        let dir_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, dir_id, self.session, Access::Read, path)?;
        require_access(self.fs, dir_id, self.session, Access::Execute, path)?;
        self.fs.opendir(path)
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<PosixDirEntry>, VfsError> {
        let dir_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, dir_id, self.session, Access::Read, path)?;
        require_access(self.fs, dir_id, self.session, Access::Execute, path)?;

        let entries = self.fs.ls(Some(path))?;
        let mut results = Vec::new();
        for entry in entries {
            if !self.can_see(&entry) {
                continue;
            }
            let child_path = join_paths(path, &entry.name);
            results.push(PosixDirEntry {
                name: entry.name,
                stat: self.fs.stat(&child_path)?,
            });
        }
        Ok(results)
    }

    pub fn open(&mut self, path: &str, writable: bool) -> Result<HandleId, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        let access = if writable {
            Access::Write
        } else {
            Access::Read
        };
        require_access(self.fs, inode_id, self.session, access, path)?;
        self.fs.open(path, writable)
    }

    pub fn create(&mut self, path: &str, mode: u16) -> Result<HandleId, VfsError> {
        let (parent_id, _) = self.fs.resolve_parent_checked(path, self.session)?;
        require_access(self.fs, parent_id, self.session, Access::Write, path)?;
        require_access(self.fs, parent_id, self.session, Access::Execute, path)?;
        self.fs.create_file(
            path,
            self.session.effective_uid(),
            self.session.effective_gid(),
            Some(mode),
        )?;
        self.fs.open(path, true)
    }

    pub fn mkdir(&mut self, path: &str, mode: u16) -> Result<(), VfsError> {
        let (parent_id, _) = self.fs.resolve_parent_checked(path, self.session)?;
        require_access(self.fs, parent_id, self.session, Access::Write, path)?;
        require_access(self.fs, parent_id, self.session, Access::Execute, path)?;
        self.fs.mkdir(
            path,
            self.session.effective_uid(),
            self.session.effective_gid(),
        )?;
        self.fs.chmod(path, mode)
    }

    pub fn read(&mut self, path: &str, offset: usize, size: usize) -> Result<Vec<u8>, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Read, path)?;
        self.fs.read_file_at(path, offset, size)
    }

    pub fn write(&mut self, path: &str, offset: usize, data: &[u8]) -> Result<usize, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Write, path)?;
        self.fs.write_file_at(path, offset, data)
    }

    pub fn read_handle(&mut self, handle: HandleId, size: usize) -> Result<Vec<u8>, VfsError> {
        self.fs.read_handle(handle, size)
    }

    pub fn write_handle(&mut self, handle: HandleId, data: &[u8]) -> Result<usize, VfsError> {
        self.fs.write_handle(handle, data)
    }

    pub fn release(&mut self, handle: HandleId) -> Result<(), VfsError> {
        self.fs.release_handle(handle)
    }

    pub fn truncate(&mut self, path: &str, size: usize) -> Result<(), VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Write, path)?;
        self.fs.truncate(path, size)
    }

    pub fn setattr(&mut self, path: &str, attr: PosixSetAttr) -> Result<StatInfo, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        let (current_uid, current_gid) = {
            let inode = self.fs.get_inode(inode_id)?;
            (inode.uid, inode.gid)
        };

        if let Some(mode) = attr.mode {
            if !self.session.is_effective_owner(current_uid) {
                return Err(VfsError::PermissionDenied {
                    path: path.to_string(),
                });
            }
            self.fs.chmod(path, mode)?;
        }

        if let Some(uid) = attr.uid {
            if !self.session.is_effectively_root() {
                return Err(VfsError::PermissionDenied {
                    path: path.to_string(),
                });
            }
            let gid = attr.gid.unwrap_or(current_gid);
            self.fs.chown(path, uid, gid)?;
        } else if let Some(gid) = attr.gid {
            if !self.session.is_effective_owner(current_uid) && !self.session.is_effectively_root()
            {
                return Err(VfsError::PermissionDenied {
                    path: path.to_string(),
                });
            }
            self.fs.chown(path, current_uid, gid)?;
        }

        if let Some(size) = attr.size {
            require_access(self.fs, inode_id, self.session, Access::Write, path)?;
            self.fs.truncate(path, size)?;
        }

        self.fs.stat(path)
    }

    pub fn unlink(&mut self, path: &str) -> Result<(), VfsError> {
        let (parent_id, _) = self.fs.resolve_parent_checked(path, self.session)?;
        require_access(self.fs, parent_id, self.session, Access::Write, path)?;
        require_access(self.fs, parent_id, self.session, Access::Execute, path)?;
        self.fs.rm(path)
    }

    pub fn rmdir(&mut self, path: &str) -> Result<(), VfsError> {
        let (parent_id, _) = self.fs.resolve_parent_checked(path, self.session)?;
        require_access(self.fs, parent_id, self.session, Access::Write, path)?;
        require_access(self.fs, parent_id, self.session, Access::Execute, path)?;
        self.fs.rmdir(path)
    }

    pub fn rename(&mut self, src: &str, dst: &str) -> Result<(), VfsError> {
        let (src_parent, _) = self.fs.resolve_parent_checked(src, self.session)?;
        require_access(self.fs, src_parent, self.session, Access::Write, src)?;
        require_access(self.fs, src_parent, self.session, Access::Execute, src)?;

        if let Ok((dst_parent, _)) = self.fs.resolve_parent_checked(dst, self.session) {
            require_access(self.fs, dst_parent, self.session, Access::Write, dst)?;
            require_access(self.fs, dst_parent, self.session, Access::Execute, dst)?;
        }

        self.fs.rename(src, dst)
    }

    pub fn symlink(&mut self, target: &str, link_path: &str) -> Result<(), VfsError> {
        let (parent_id, _) = self.fs.resolve_parent_checked(link_path, self.session)?;
        require_access(self.fs, parent_id, self.session, Access::Write, link_path)?;
        require_access(self.fs, parent_id, self.session, Access::Execute, link_path)?;
        self.fs.ln_s(
            target,
            link_path,
            self.session.effective_uid(),
            self.session.effective_gid(),
        )
    }

    pub fn link(&mut self, target: &str, link_path: &str) -> Result<(), VfsError> {
        let target_id = self.fs.resolve_path_checked(target, self.session)?;
        require_access(self.fs, target_id, self.session, Access::Read, target)?;
        let (parent_id, _) = self.fs.resolve_parent_checked(link_path, self.session)?;
        require_access(self.fs, parent_id, self.session, Access::Write, link_path)?;
        require_access(self.fs, parent_id, self.session, Access::Execute, link_path)?;
        self.fs.link(target, link_path)
    }

    pub fn readlink(&self, path: &str) -> Result<String, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Read, path)?;
        self.fs.readlink(path)
    }

    pub fn listxattr(&self, path: &str) -> Result<Vec<String>, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Read, path)?;

        let stat = self.fs.stat(path)?;
        let mut names = Vec::new();
        if stat.mime_type.is_some() {
            names.push(STRATUM_MIME_XATTR.to_string());
        }
        names.extend(
            stat.custom_attrs
                .keys()
                .map(|key| format!("{STRATUM_CUSTOM_XATTR_PREFIX}{key}")),
        );
        Ok(names)
    }

    pub fn getxattr(&self, path: &str, name: &str) -> Result<Vec<u8>, VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Read, path)?;

        let stat = self.fs.stat(path)?;
        let value = match parse_stratum_xattr_name(name)? {
            PosixXattrName::MimeType => stat.mime_type.ok_or_else(|| missing_xattr_error(name))?,
            PosixXattrName::Custom(key) => stat
                .custom_attrs
                .get(key)
                .cloned()
                .ok_or_else(|| missing_xattr_error(name))?,
        };
        Ok(value.into_bytes())
    }

    pub fn setxattr(
        &mut self,
        path: &str,
        name: &str,
        value: &[u8],
        mode: PosixXattrSetMode,
    ) -> Result<(), VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Write, path)?;

        let parsed_name = parse_stratum_xattr_name(name)?;
        let value = std::str::from_utf8(value)
            .map_err(|_| VfsError::InvalidArgs {
                message: "xattr values must be UTF-8 strings".to_string(),
            })?
            .to_string();

        let stat = self.fs.stat(path)?;
        let mut update = MetadataUpdate::default();
        let exists = match parsed_name {
            PosixXattrName::MimeType => {
                update.mime_type = Some(Some(value));
                stat.mime_type.is_some()
            }
            PosixXattrName::Custom(key) => {
                update.custom_attrs.insert(key.to_string(), value);
                stat.custom_attrs.contains_key(key)
            }
        };

        match (mode, exists) {
            (PosixXattrSetMode::CreateOnly, true) => Err(VfsError::AlreadyExists {
                path: name.to_string(),
            }),
            (PosixXattrSetMode::ReplaceOnly, false) => Err(missing_xattr_error(name)),
            _ => {
                self.fs.set_metadata(path, update)?;
                Ok(())
            }
        }
    }

    pub fn removexattr(&mut self, path: &str, name: &str) -> Result<(), VfsError> {
        let inode_id = self.fs.resolve_path_checked(path, self.session)?;
        require_access(self.fs, inode_id, self.session, Access::Write, path)?;

        let stat = self.fs.stat(path)?;
        let mut update = MetadataUpdate::default();
        match parse_stratum_xattr_name(name)? {
            PosixXattrName::MimeType => {
                if stat.mime_type.is_none() {
                    return Err(missing_xattr_error(name));
                }
                update.mime_type = Some(None);
            }
            PosixXattrName::Custom(key) => {
                if !stat.custom_attrs.contains_key(key) {
                    return Err(missing_xattr_error(name));
                }
                update.remove_custom_attrs.push(key.to_string());
            }
        }

        self.fs.set_metadata(path, update)?;
        Ok(())
    }

    fn can_see(&self, entry: &LsEntry) -> bool {
        self.session
            .has_permission_bits(entry.mode, entry.uid, entry.gid, Access::Read)
    }
}

enum PosixXattrName<'a> {
    MimeType,
    Custom(&'a str),
}

fn parse_stratum_xattr_name(name: &str) -> Result<PosixXattrName<'_>, VfsError> {
    if name == STRATUM_MIME_XATTR {
        return Ok(PosixXattrName::MimeType);
    }

    if let Some(key) = name.strip_prefix(STRATUM_CUSTOM_XATTR_PREFIX) {
        if key.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: "custom xattr key must not be empty".to_string(),
            });
        }
        return Ok(PosixXattrName::Custom(key));
    }

    Err(VfsError::NotSupported {
        message: format!("unsupported xattr name: {name}"),
    })
}

fn missing_xattr_error(name: &str) -> VfsError {
    VfsError::NotFound {
        path: name.to_string(),
    }
}

fn require_access(
    fs: &VirtualFs,
    inode_id: u64,
    session: &Session,
    access: Access,
    path: &str,
) -> Result<(), VfsError> {
    let inode = fs.get_inode(inode_id)?;
    if !session.has_permission(inode, access) {
        return Err(VfsError::PermissionDenied {
            path: path.to_string(),
        });
    }
    Ok(())
}

fn join_paths(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{child}")
    } else if parent == "." {
        format!("./{child}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), child)
    }
}
