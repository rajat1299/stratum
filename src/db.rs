use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, RwLock};

use crate::auth::perms::{Access, has_sticky_bit};
use crate::auth::session::Session;
use crate::auth::{Gid, ROOT_UID, Uid, WHEEL_GID};
use crate::config::Config;
use crate::error::VfsError;
use crate::fs::inode::InodeKind;
use crate::fs::{FsOptions, GrepResult, HandleId, LsEntry, StatInfo, VirtualFs};
use crate::persist::{
    LocalStateBackend, MemoryPersistenceBackend, PersistenceBackend, PersistenceInfo,
};
use crate::store::ObjectId;
use crate::store::commit::CommitObject;
use crate::vcs::{CommitId, RefName, RefUpdateExpectation, Vcs};

struct DbInner {
    fs: VirtualFs,
    vcs: Vcs,
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

fn normalize_scope_path(path: &str) -> String {
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
        "/".to_string()
    } else {
        format!("/{}", components.join("/"))
    }
}

fn scope_path(fs: &VirtualFs, path: &str) -> String {
    let absolute = if path.starts_with('/') {
        path.to_string()
    } else {
        child_path(&fs.pwd(), path)
    };
    normalize_scope_path(&absolute)
}

fn scope_parent_path(fs: &VirtualFs, path: &str) -> String {
    let path = scope_path(fs, path);
    if path == "/" {
        return "/".to_string();
    }

    match path.rsplit_once('/') {
        Some(("", _)) => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
        None => "/".to_string(),
    }
}

fn require_scope_for_path(
    fs: &VirtualFs,
    session: &Session,
    path: &str,
    access: Access,
) -> Result<(), VfsError> {
    let path = scope_path(fs, path);
    if !session.is_path_allowed(&path, access) {
        return Err(VfsError::PermissionDenied { path });
    }
    Ok(())
}

fn require_scope_for_parent(
    fs: &VirtualFs,
    session: &Session,
    path: &str,
    access: Access,
) -> Result<(), VfsError> {
    let path = scope_parent_path(fs, path);
    if !session.is_path_allowed(&path, access) {
        return Err(VfsError::PermissionDenied { path });
    }
    Ok(())
}

fn require_admin(session: &Session) -> Result<(), VfsError> {
    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "admin operation".to_string(),
        });
    }

    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "admin operation".to_string(),
            });
        }
    }

    Ok(())
}

fn mkdir_components(path: &str) -> (bool, Vec<String>) {
    let absolute = path.starts_with('/');
    let mut components = Vec::new();
    for component in path.split('/').filter(|component| !component.is_empty()) {
        match component {
            "." => {}
            ".." => {
                components.pop();
            }
            name => components.push(name.to_string()),
        }
    }
    (absolute, components)
}

fn child_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else if parent == "/" {
        format!("/{child}")
    } else {
        format!("{parent}/{child}")
    }
}

fn path_basename(path: &str) -> Result<String, VfsError> {
    path.split('/')
        .filter(|component| !component.is_empty())
        .next_back()
        .map(str::to_string)
        .ok_or_else(|| VfsError::InvalidPath {
            path: path.to_string(),
        })
}

fn trim_destination_dir_path(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() && path.starts_with('/') {
        "/"
    } else {
        trimmed
    }
}

fn insert_destination(
    fs: &VirtualFs,
    src: &str,
    dst: &str,
    session: &Session,
) -> Result<(u64, String), VfsError> {
    match fs.resolve_path_checked(dst, session) {
        Ok(dst_id) if fs.get_inode(dst_id)?.is_dir() => {
            let src_name = path_basename(src)?;
            let dst_dir = trim_destination_dir_path(dst);
            Ok((dst_id, child_path(dst_dir, &src_name)))
        }
        Ok(_) | Err(VfsError::NotFound { .. }) => {
            let (dst_parent, _) = fs.resolve_parent_checked(dst, session)?;
            Ok((dst_parent, dst.to_string()))
        }
        Err(e) => Err(e),
    }
}

fn require_destination_replace(
    fs: &VirtualFs,
    parent_id: u64,
    path: &str,
    session: &Session,
    require_destination_write: bool,
) -> Result<(), VfsError> {
    let dst_id = match fs.resolve_path(path) {
        Ok(id) => id,
        Err(VfsError::NotFound { .. }) => return Ok(()),
        Err(e) => return Err(e),
    };

    if require_destination_write {
        require_access(fs, dst_id, session, Access::Write, path)?;
    }
    require_sticky_delete(fs, parent_id, dst_id, session, path)
}

fn clean_root_result_path(path: &mut String) {
    if path.starts_with("//") {
        path.remove(0);
    }
}

fn checked_final_path(
    fs: &VirtualFs,
    path: &str,
    session: &Session,
    final_access: Access,
) -> Result<String, VfsError> {
    let mut current = path.to_string();
    for _ in 0..40 {
        require_scope_for_path(fs, session, &current, final_access)?;
        let id = fs.resolve_path_checked(&current, session)?;
        let inode = fs.get_inode(id)?;

        match &inode.kind {
            InodeKind::Symlink { target } => {
                require_access(fs, id, session, final_access, &current)?;
                current = target.clone();
            }
            _ => {
                require_access(fs, id, session, final_access, &current)?;
                return Ok(current);
            }
        }
    }

    Err(VfsError::SymlinkLoop {
        path: path.to_string(),
    })
}

fn require_sticky_delete(
    fs: &VirtualFs,
    parent_id: u64,
    child_id: u64,
    session: &Session,
    path: &str,
) -> Result<(), VfsError> {
    let parent = fs.get_inode(parent_id)?;
    if has_sticky_bit(parent.mode) && !session.is_effectively_root() {
        let child = fs.get_inode(child_id)?;
        let effective_uid = session.effective_uid();
        if child.uid != effective_uid && parent.uid != effective_uid {
            return Err(VfsError::PermissionDenied {
                path: path.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_recursive_delete(
    fs: &VirtualFs,
    path: &str,
    session: &Session,
) -> Result<(), VfsError> {
    require_scope_for_path(fs, session, path, Access::Write)?;
    let id = fs.resolve_path_checked(path, session)?;
    let inode = fs.get_inode(id)?;
    let entries = match &inode.kind {
        InodeKind::Directory { entries } => entries.clone(),
        _ => return Ok(()),
    };

    require_access(fs, id, session, Access::Write, path)?;
    require_access(fs, id, session, Access::Execute, path)?;

    for (name, child_id) in entries {
        let child_path = child_path(path, &name);
        require_sticky_delete(fs, id, child_id, session, &child_path)?;
        let child = fs.get_inode(child_id)?;
        if child.is_dir() {
            validate_recursive_delete(fs, &child_path, session)?;
        }
    }

    Ok(())
}

/// Thread-safe, concurrent markdown database.
///
/// All methods take `&self` (not `&mut self`). The struct is `Clone`
/// via the inner `Arc`, so it can be shared across threads cheaply.
#[derive(Clone)]
pub struct StratumDb {
    inner: Arc<RwLock<DbInner>>,
    persist: Arc<dyn PersistenceBackend>,
    config: Arc<Config>,
    write_count: Arc<AtomicU64>,
    save_notify: Arc<Notify>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbVcsRef {
    pub name: String,
    pub target: String,
    pub version: u64,
}

impl From<crate::vcs::VcsRef> for DbVcsRef {
    fn from(vcs_ref: crate::vcs::VcsRef) -> Self {
        Self {
            name: vcs_ref.name.into_string(),
            target: vcs_ref.target.to_hex(),
            version: vcs_ref.version,
        }
    }
}

impl StratumDb {
    pub fn open(config: Config) -> Result<Self, VfsError> {
        let persist: Arc<dyn PersistenceBackend> =
            Arc::new(LocalStateBackend::new(&config.data_dir));
        Self::open_with_backend(config, persist)
    }

    pub fn open_with_backend(
        config: Config,
        persist: Arc<dyn PersistenceBackend>,
    ) -> Result<Self, VfsError> {
        let options = FsOptions {
            compatibility_target: config.compatibility_target,
        };
        let (mut fs, vcs) = match persist.load()? {
            Some(state) => (state.fs, state.vcs),
            None => (VirtualFs::new_with_options(options), Vcs::new()),
        };
        fs.set_compatibility_target(config.compatibility_target);

        Ok(StratumDb {
            inner: Arc::new(RwLock::new(DbInner { fs, vcs })),
            persist,
            config: Arc::new(config),
            write_count: Arc::new(AtomicU64::new(0)),
            save_notify: Arc::new(Notify::new()),
        })
    }

    pub fn open_memory() -> Self {
        let config = Config::from_env();
        let options = FsOptions {
            compatibility_target: config.compatibility_target,
        };
        StratumDb {
            inner: Arc::new(RwLock::new(DbInner {
                fs: VirtualFs::new_with_options(options),
                vcs: Vcs::new(),
            })),
            persist: Arc::new(MemoryPersistenceBackend),
            config: Arc::new(config),
            write_count: Arc::new(AtomicU64::new(0)),
            save_notify: Arc::new(Notify::new()),
        }
    }

    fn mark_dirty(&self) {
        let count = self.write_count.fetch_add(1, Ordering::Relaxed);
        if count + 1 >= self.config.auto_save_write_threshold {
            self.save_notify.notify_one();
        }
    }

    /// Spawn a background auto-save task. Returns a handle that can be aborted on shutdown.
    pub fn spawn_auto_save(&self) -> tokio::task::JoinHandle<()> {
        let db = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(
                        db.config.auto_save_interval_secs,
                    )) => {}
                    _ = db.save_notify.notified() => {}
                }

                let prev = db.write_count.swap(0, Ordering::Relaxed);
                if prev > 0 {
                    if let Err(e) = db.save().await {
                        tracing::error!("auto-save failed: {e}");
                    } else {
                        tracing::debug!("auto-saved after {prev} writes");
                    }
                }
            }
        })
    }

    pub async fn save(&self) -> Result<(), VfsError> {
        let guard = self.inner.read().await;
        self.persist.save(&guard.fs, &guard.vcs)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn persist_info(&self) -> PersistenceInfo {
        self.persist.info()
    }

    // ─── Read operations (take read lock) ───

    pub async fn cat(&self, path: &str) -> Result<Vec<u8>, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.cat_owned(path)
    }

    pub async fn ls(&self, path: Option<&str>) -> Result<Vec<LsEntry>, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.ls(path)
    }

    pub async fn stat(&self, path: &str) -> Result<StatInfo, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.stat(path)
    }

    pub async fn pwd(&self) -> String {
        let guard = self.inner.read().await;
        guard.fs.pwd()
    }

    pub async fn tree(
        &self,
        path: Option<&str>,
        session: Option<&Session>,
    ) -> Result<String, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.tree(path, "", session)
    }

    pub async fn find(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: Option<&Session>,
    ) -> Result<Vec<String>, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.find(path, pattern, session)
    }

    pub async fn grep(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: Option<&Session>,
    ) -> Result<Vec<GrepResult>, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.grep(pattern, path, recursive, session)
    }

    pub async fn cat_as(&self, path: &str, session: &Session) -> Result<Vec<u8>, VfsError> {
        let guard = self.inner.read().await;
        let target_path = checked_final_path(&guard.fs, path, session, Access::Read)?;
        guard.fs.cat_owned(&target_path)
    }

    pub async fn ls_as(
        &self,
        path: Option<&str>,
        session: &Session,
    ) -> Result<Vec<LsEntry>, VfsError> {
        let guard = self.inner.read().await;
        let path = path.unwrap_or("/");
        require_scope_for_path(&guard.fs, session, path, Access::Read)?;
        let id = guard.fs.resolve_path_checked(path, session)?;
        let inode = guard.fs.get_inode(id)?;
        require_access(&guard.fs, id, session, Access::Read, path)?;
        if inode.is_dir() {
            require_access(&guard.fs, id, session, Access::Execute, path)?;
        }

        let entries = guard.fs.ls(Some(path))?;
        Ok(entries
            .into_iter()
            .filter(|entry| {
                session.has_permission_bits(entry.mode, entry.uid, entry.gid, Access::Read)
            })
            .collect())
    }

    pub async fn stat_as(&self, path: &str, session: &Session) -> Result<StatInfo, VfsError> {
        let guard = self.inner.read().await;
        require_scope_for_path(&guard.fs, session, path, Access::Read)?;
        let id = guard.fs.resolve_path_checked(path, session)?;
        require_access(&guard.fs, id, session, Access::Read, path)?;
        guard.fs.stat(path)
    }

    pub async fn tree_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError> {
        let guard = self.inner.read().await;
        let path = path.unwrap_or("/");
        require_scope_for_path(&guard.fs, session, path, Access::Read)?;
        let id = guard.fs.resolve_path_checked(path, session)?;
        let inode = guard.fs.get_inode(id)?;
        require_access(&guard.fs, id, session, Access::Read, path)?;
        if inode.is_dir() {
            require_access(&guard.fs, id, session, Access::Execute, path)?;
        }
        guard.fs.tree(Some(path), "", Some(session))
    }

    pub async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        let guard = self.inner.read().await;
        let path = path.unwrap_or("/");
        require_scope_for_path(&guard.fs, session, path, Access::Read)?;
        let id = guard.fs.resolve_path_checked(path, session)?;
        let inode = guard.fs.get_inode(id)?;
        require_access(&guard.fs, id, session, Access::Read, path)?;
        if inode.is_dir() {
            require_access(&guard.fs, id, session, Access::Execute, path)?;
        }

        let mut results = guard.fs.find(Some(path), pattern, Some(session))?;
        if path == "/" {
            for result in &mut results {
                clean_root_result_path(result);
            }
        }
        Ok(results)
    }

    pub async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        let guard = self.inner.read().await;
        let path = path.unwrap_or("/");
        require_scope_for_path(&guard.fs, session, path, Access::Read)?;
        let id = guard.fs.resolve_path_checked(path, session)?;
        let inode = guard.fs.get_inode(id)?;
        require_access(&guard.fs, id, session, Access::Read, path)?;
        if inode.is_dir() {
            require_access(&guard.fs, id, session, Access::Execute, path)?;
        }

        let mut results = guard
            .fs
            .grep(pattern, Some(path), recursive, Some(session))?;
        if path == "/" {
            for result in &mut results {
                clean_root_result_path(&mut result.file);
            }
        }
        Ok(results)
    }

    pub async fn vcs_log(&self) -> Vec<CommitObject> {
        let guard = self.inner.read().await;
        guard.vcs.log().into_iter().cloned().collect()
    }

    pub async fn vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError> {
        require_admin(session)?;
        Ok(self.vcs_log().await)
    }

    pub async fn vcs_status(&self) -> Result<String, VfsError> {
        let guard = self.inner.read().await;
        let inner = &*guard;
        inner.vcs.status(&inner.fs)
    }

    pub async fn vcs_status_as(&self, session: &Session) -> Result<String, VfsError> {
        require_admin(session)?;
        self.vcs_status().await
    }

    pub async fn vcs_diff(&self, path: Option<&str>) -> Result<String, VfsError> {
        let guard = self.inner.read().await;
        let inner = &*guard;
        inner.vcs.diff(&inner.fs, path)
    }

    pub async fn vcs_diff_as(
        &self,
        path: Option<&str>,
        session: &Session,
    ) -> Result<String, VfsError> {
        require_admin(session)?;
        self.vcs_diff(path).await
    }

    pub async fn list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError> {
        let guard = self.inner.read().await;
        Ok(guard.vcs.list_refs().into_iter().map(Into::into).collect())
    }

    pub async fn get_ref(&self, name: &str) -> Result<Option<DbVcsRef>, VfsError> {
        let name = RefName::new(name)?;
        let guard = self.inner.read().await;
        Ok(guard.vcs.get_ref(name)?.map(Into::into))
    }

    pub async fn create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError> {
        let name = RefName::new(name)?;
        let target = CommitId::from(ObjectId::from_hex(target)?);
        let mut guard = self.inner.write().await;
        let vcs_ref = guard.vcs.create_ref(name, target)?;
        drop(guard);
        self.mark_dirty();
        Ok(vcs_ref.into())
    }

    pub async fn update_ref(
        &self,
        name: &str,
        expected_target: &str,
        expected_version: u64,
        target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        let name = RefName::new(name)?;
        let expected = RefUpdateExpectation::new(
            CommitId::from(ObjectId::from_hex(expected_target)?),
            expected_version,
        );
        let target = CommitId::from(ObjectId::from_hex(target)?);
        let mut guard = self.inner.write().await;
        let vcs_ref = guard.vcs.update_ref(name, expected, target)?;
        drop(guard);
        self.mark_dirty();
        Ok(vcs_ref.into())
    }

    pub async fn compare_and_swap_ref(
        &self,
        name: &str,
        expected_target: Option<&str>,
        expected_version: Option<u64>,
        target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        let name = RefName::new(name)?;
        let expected = match (expected_target, expected_version) {
            (Some(target), Some(version)) => Some(RefUpdateExpectation::new(
                CommitId::from(ObjectId::from_hex(target)?),
                version,
            )),
            (None, None) => None,
            _ => {
                return Err(VfsError::InvalidArgs {
                    message: "expected ref target and version must be supplied together"
                        .to_string(),
                });
            }
        };
        let target = CommitId::from(ObjectId::from_hex(target)?);
        let mut guard = self.inner.write().await;
        let vcs_ref = guard.vcs.compare_and_swap_ref(name, expected, target)?;
        drop(guard);
        self.mark_dirty();
        Ok(vcs_ref.into())
    }

    // ─── Write operations (take write lock) ───

    pub async fn touch(&self, path: &str, uid: Uid, gid: Gid) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.touch(path, uid, gid)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn write_file(&self, path: &str, content: Vec<u8>) -> Result<(), VfsError> {
        if content.len() > self.config.max_file_size {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "file size {} exceeds max {}",
                    content.len(),
                    self.config.max_file_size
                ),
            });
        }
        let mut guard = self.inner.write().await;
        guard.fs.write_file(path, content)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn write_file_as(
        &self,
        path: &str,
        content: Vec<u8>,
        session: &Session,
    ) -> Result<(), VfsError> {
        if content.len() > self.config.max_file_size {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "file size {} exceeds max {}",
                    content.len(),
                    self.config.max_file_size
                ),
            });
        }

        let mut guard = self.inner.write().await;
        require_scope_for_path(&guard.fs, session, path, Access::Write)?;
        let mut write_path = path.to_string();
        match guard.fs.resolve_path(path) {
            Ok(id) => {
                let _ = guard.fs.resolve_path_checked(path, session)?;
                require_access(&guard.fs, id, session, Access::Write, path)?;
                write_path = checked_final_path(&guard.fs, path, session, Access::Write)?;
            }
            Err(VfsError::NotFound { .. }) => {
                let (parent_id, _) = guard.fs.resolve_parent_checked(path, session)?;
                require_access(&guard.fs, parent_id, session, Access::Write, path)?;
                require_access(&guard.fs, parent_id, session, Access::Execute, path)?;
                guard
                    .fs
                    .touch(path, session.effective_uid(), session.effective_gid())?;
            }
            Err(e) => return Err(e),
        }

        guard.fs.write_file(&write_path, content)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn mkdir(&self, path: &str, uid: Uid, gid: Gid) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.mkdir(path, uid, gid)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn mkdir_p(&self, path: &str, uid: Uid, gid: Gid) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.mkdir_p(path, uid, gid)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn mkdir_p_as(&self, path: &str, session: &Session) -> Result<(), VfsError> {
        let (absolute, components) = mkdir_components(path);
        if components.is_empty() {
            return Ok(());
        }

        let mut guard = self.inner.write().await;
        require_scope_for_path(&guard.fs, session, path, Access::Write)?;
        let mut current = if absolute {
            "/".to_string()
        } else {
            String::new()
        };
        let mut created = false;

        for component in components {
            let next = child_path(&current, &component);
            match guard.fs.resolve_path(&next) {
                Ok(id) => {
                    let _ = guard.fs.resolve_path_checked(&next, session)?;
                    if !guard.fs.get_inode(id)?.is_dir() {
                        return Err(VfsError::NotDirectory { path: next });
                    }
                }
                Err(VfsError::NotFound { .. }) => {
                    require_scope_for_path(&guard.fs, session, &next, Access::Write)?;
                    let (parent_id, _) = guard.fs.resolve_parent_checked(&next, session)?;
                    require_access(&guard.fs, parent_id, session, Access::Write, &next)?;
                    require_access(&guard.fs, parent_id, session, Access::Execute, &next)?;
                    guard
                        .fs
                        .mkdir(&next, session.effective_uid(), session.effective_gid())?;
                    created = true;
                }
                Err(e) => return Err(e),
            }
            current = next;
        }

        drop(guard);
        if created {
            self.mark_dirty();
        }
        Ok(())
    }

    pub async fn rm(&self, path: &str) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.rm(path)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn rm_rf(&self, path: &str) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.rm_rf(path)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn rm_as(
        &self,
        path: &str,
        recursive: bool,
        session: &Session,
    ) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        require_scope_for_path(&guard.fs, session, path, Access::Write)?;
        require_scope_for_parent(&guard.fs, session, path, Access::Write)?;
        let (parent_id, _) = guard.fs.resolve_parent_checked(path, session)?;
        require_access(&guard.fs, parent_id, session, Access::Write, path)?;
        require_access(&guard.fs, parent_id, session, Access::Execute, path)?;

        let parent = guard.fs.get_inode(parent_id)?;
        if has_sticky_bit(parent.mode) && !session.is_effectively_root() {
            let file_id = guard.fs.resolve_path(path)?;
            let file_inode = guard.fs.get_inode(file_id)?;
            let effective_uid = session.effective_uid();
            if file_inode.uid != effective_uid && parent.uid != effective_uid {
                return Err(VfsError::PermissionDenied {
                    path: path.to_string(),
                });
            }
        }

        if recursive {
            let target_id = guard.fs.resolve_path_checked(path, session)?;
            require_sticky_delete(&guard.fs, parent_id, target_id, session, path)?;
            validate_recursive_delete(&guard.fs, path, session)?;
            guard.fs.rm_rf(path)?;
        } else {
            guard.fs.rm(path)?;
        }
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn mv(&self, src: &str, dst: &str) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.mv(src, dst)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn mv_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        require_scope_for_path(&guard.fs, session, src, Access::Write)?;
        require_scope_for_path(&guard.fs, session, dst, Access::Write)?;
        let (src_parent, _) = guard.fs.resolve_parent_checked(src, session)?;
        require_access(&guard.fs, src_parent, session, Access::Write, src)?;
        require_access(&guard.fs, src_parent, session, Access::Execute, src)?;

        let src_parent_inode = guard.fs.get_inode(src_parent)?;
        if has_sticky_bit(src_parent_inode.mode) && !session.is_effectively_root() {
            let src_id = guard.fs.resolve_path(src)?;
            let src_inode = guard.fs.get_inode(src_id)?;
            let effective_uid = session.effective_uid();
            if src_inode.uid != effective_uid && src_parent_inode.uid != effective_uid {
                return Err(VfsError::PermissionDenied {
                    path: src.to_string(),
                });
            }
        }

        let (dst_parent, dst_path) = insert_destination(&guard.fs, src, dst, session)?;
        require_scope_for_path(&guard.fs, session, &dst_path, Access::Write)?;
        require_access(&guard.fs, dst_parent, session, Access::Write, dst)?;
        require_access(&guard.fs, dst_parent, session, Access::Execute, dst)?;
        require_destination_replace(&guard.fs, dst_parent, &dst_path, session, false)?;

        guard.fs.mv(src, dst)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn cp(&self, src: &str, dst: &str, uid: Uid, gid: Gid) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.cp(src, dst, uid, gid)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn cp_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        require_scope_for_path(&guard.fs, session, src, Access::Read)?;
        require_scope_for_path(&guard.fs, session, dst, Access::Write)?;
        let src_id = guard.fs.resolve_path_checked(src, session)?;
        require_access(&guard.fs, src_id, session, Access::Read, src)?;

        let (dst_parent, dst_path) = insert_destination(&guard.fs, src, dst, session)?;
        require_scope_for_path(&guard.fs, session, &dst_path, Access::Write)?;
        require_access(&guard.fs, dst_parent, session, Access::Write, dst)?;
        require_access(&guard.fs, dst_parent, session, Access::Execute, dst)?;
        require_destination_replace(&guard.fs, dst_parent, &dst_path, session, true)?;

        guard
            .fs
            .cp(src, dst, session.effective_uid(), session.effective_gid())?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn chmod(&self, path: &str, mode: u16) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.chmod(path, mode)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn chown(&self, path: &str, uid: Uid, gid: Gid) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.chown(path, uid, gid)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn ln_s(
        &self,
        target: &str,
        link_path: &str,
        uid: Uid,
        gid: Gid,
    ) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.ln_s(target, link_path, uid, gid)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn link(&self, target: &str, link_path: &str) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.link(target, link_path)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn readlink(&self, path: &str) -> Result<String, VfsError> {
        let guard = self.inner.read().await;
        guard.fs.readlink(path)
    }

    pub async fn truncate(&self, path: &str, size: usize) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.truncate(path, size)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn read_file_at(
        &self,
        path: &str,
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>, VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.read_file_at(path, offset, size)
    }

    pub async fn write_file_at(
        &self,
        path: &str,
        offset: usize,
        data: &[u8],
    ) -> Result<usize, VfsError> {
        let end = offset.saturating_add(data.len());
        if end > self.config.max_file_size {
            return Err(VfsError::InvalidArgs {
                message: format!("write exceeds max file size {}", self.config.max_file_size),
            });
        }
        let mut guard = self.inner.write().await;
        let written = guard.fs.write_file_at(path, offset, data)?;
        drop(guard);
        self.mark_dirty();
        Ok(written)
    }

    pub async fn open_file(&self, path: &str, writable: bool) -> Result<HandleId, VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.open(path, writable)
    }

    pub async fn open_dir(&self, path: &str) -> Result<HandleId, VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.opendir(path)
    }

    pub async fn read_handle(&self, handle: HandleId, size: usize) -> Result<Vec<u8>, VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.read_handle(handle, size)
    }

    pub async fn write_handle(&self, handle: HandleId, data: &[u8]) -> Result<usize, VfsError> {
        let mut guard = self.inner.write().await;
        let written = guard.fs.write_handle(handle, data)?;
        drop(guard);
        self.mark_dirty();
        Ok(written)
    }

    pub async fn release_handle(&self, handle: HandleId) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        guard.fs.release_handle(handle)
    }

    pub async fn commit(&self, message: &str, author: &str) -> Result<String, VfsError> {
        let mut guard = self.inner.write().await;
        let inner = &mut *guard;
        let id = inner.vcs.commit(&inner.fs, message, author)?;
        drop(guard);
        self.mark_dirty();
        Ok(id.short_hex())
    }

    pub async fn commit_as(&self, message: &str, session: &Session) -> Result<String, VfsError> {
        require_admin(session)?;
        self.commit(message, &session.username).await
    }

    pub async fn revert(&self, hash_prefix: &str) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        let inner = &mut *guard;
        inner.vcs.revert(&mut inner.fs, hash_prefix)?;
        drop(guard);
        self.mark_dirty();
        Ok(())
    }

    pub async fn revert_as(&self, hash_prefix: &str, session: &Session) -> Result<(), VfsError> {
        require_admin(session)?;
        self.revert(hash_prefix).await
    }

    // ─── Command execution (write lock — dispatches through cmd module) ───

    pub async fn execute_command(
        &self,
        line: &str,
        session: &mut Session,
    ) -> Result<String, VfsError> {
        use crate::cmd;
        use crate::cmd::parser;

        let pipeline = parser::parse_pipeline(line);
        if pipeline.commands.is_empty() {
            return Ok(String::new());
        }

        if let Some(first) = pipeline.commands.first() {
            match first.program.as_str() {
                "commit" => {
                    let msg = if first.args.is_empty() {
                        "snapshot"
                    } else {
                        &first.args.join(" ")
                    };
                    let hash = self.commit_as(msg, session).await?;
                    return Ok(format!("[{hash}] {msg}\n"));
                }
                "log" => {
                    let commits = self.vcs_log_as(session).await?;
                    if commits.is_empty() {
                        return Ok("No commits yet.\n".to_string());
                    }
                    let mut output = String::new();
                    for c in &commits {
                        let time = chrono::DateTime::from_timestamp(c.timestamp as i64, 0)
                            .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| "???".to_string());
                        output.push_str(&format!(
                            "{} {} {} {}\n",
                            c.id.short_hex(),
                            time,
                            c.author,
                            c.message
                        ));
                    }
                    return Ok(output);
                }
                "revert" => {
                    if first.args.is_empty() {
                        return Err(VfsError::InvalidArgs {
                            message: "revert: need commit hash prefix".to_string(),
                        });
                    }
                    self.revert_as(&first.args[0], session).await?;
                    return Ok(format!("Reverted to {}\n", first.args[0]));
                }
                "status" => {
                    return self.vcs_status_as(session).await;
                }
                "diff" => {
                    return self
                        .vcs_diff_as(first.args.first().map(String::as_str), session)
                        .await;
                }
                _ => {}
            }
        }

        let mut guard = self.inner.write().await;
        let inner = &mut *guard;
        let result = cmd::execute_pipeline(&pipeline, &mut inner.fs, session);
        let is_write = pipeline.commands.iter().any(|c| {
            matches!(
                c.program.as_str(),
                "touch"
                    | "write"
                    | "mkdir"
                    | "rm"
                    | "rmdir"
                    | "mv"
                    | "cp"
                    | "chmod"
                    | "chown"
                    | "ln"
                    | "adduser"
                    | "addagent"
                    | "deluser"
                    | "addgroup"
                    | "delgroup"
                    | "usermod"
            )
        });
        drop(guard);
        if is_write {
            self.mark_dirty();
        }
        result
    }

    // ─── Auth helpers ───

    pub async fn login(&self, username: &str) -> Result<Session, VfsError> {
        let mut guard = self.inner.write().await;
        let uid = guard
            .registry_lookup_uid(username)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("unknown user: {username}"),
            })?;
        let user = guard
            .registry_get_user(uid)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("user uid={uid} not found"),
            })?;
        let session = Session::new(
            user.uid,
            user.groups.first().copied().unwrap_or(0),
            user.groups.clone(),
            user.name.clone(),
        );

        let home_path = format!("/home/{username}");
        if guard.fs.stat(&home_path).is_ok() {
            let _ = guard.fs.cd(&home_path);
        }

        Ok(session)
    }

    pub async fn authenticate_token(&self, raw_token: &str) -> Result<Session, VfsError> {
        let guard = self.inner.read().await;
        let uid = guard
            .fs
            .registry
            .authenticate_token(raw_token)
            .ok_or_else(|| VfsError::AuthError {
                message: "invalid token".to_string(),
            })?;
        let user = guard
            .fs
            .registry
            .get_user(uid)
            .ok_or_else(|| VfsError::AuthError {
                message: "token user not found".to_string(),
            })?;
        Ok(Session::new(
            user.uid,
            user.groups.first().copied().unwrap_or(0),
            user.groups.clone(),
            user.name.clone(),
        ))
    }

    pub async fn session_for_uid(&self, uid: crate::auth::Uid) -> Result<Session, VfsError> {
        let guard = self.inner.read().await;
        let user = guard
            .fs
            .registry
            .get_user(uid)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("user uid={uid} not found"),
            })?;
        Ok(Session::new(
            user.uid,
            user.groups.first().copied().unwrap_or(0),
            user.groups.clone(),
            user.name.clone(),
        ))
    }

    pub async fn has_users(&self) -> bool {
        let guard = self.inner.read().await;
        guard
            .fs
            .registry
            .list_users()
            .iter()
            .any(|u| u.uid != crate::auth::ROOT_UID)
    }

    pub async fn create_admin(&self, name: &str) -> Result<Session, VfsError> {
        let mut guard = self.inner.write().await;
        let (uid, _) = guard.fs.registry.add_user(name, false)?;
        let _ = guard.fs.registry.usermod_add_group(name, "wheel");
        let user = guard.fs.registry.get_user(uid).unwrap();
        let gid = user.groups.first().copied().unwrap_or(0);
        let session = Session::new(uid, gid, user.groups.clone(), user.name.clone());

        let _ = guard
            .fs
            .mkdir_p("/home", crate::auth::ROOT_UID, crate::auth::ROOT_GID);
        let home_path = format!("/home/{name}");
        let _ = guard.fs.mkdir(&home_path, uid, gid);
        let _ = guard.fs.cd(&home_path);

        drop(guard);
        self.mark_dirty();
        Ok(session)
    }

    pub async fn commit_count(&self) -> usize {
        let guard = self.inner.read().await;
        guard.vcs.commits.len()
    }

    pub async fn object_count(&self) -> usize {
        let guard = self.inner.read().await;
        guard.vcs.store.object_count()
    }

    pub async fn inode_count(&self) -> usize {
        let guard = self.inner.read().await;
        guard.fs.all_inodes().len()
    }

    pub fn snapshot_fs(&self) -> VirtualFs {
        self.inner.blocking_read().fs.clone()
    }
}

impl DbInner {
    fn registry_lookup_uid(&self, name: &str) -> Option<Uid> {
        self.fs.registry.lookup_uid(name)
    }

    fn registry_get_user(&self, uid: Uid) -> Option<crate::auth::User> {
        self.fs.registry.get_user(uid).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ROOT_GID;
    use crate::auth::session::SessionScope;
    use crate::config::CompatibilityTarget;
    use crate::persist::{LoadedState, PersistenceBackend, PersistenceInfo};
    use crate::vcs::Vcs;
    use std::sync::{Arc, Mutex};

    struct FixedLoadBackend {
        state: Mutex<Option<LoadedState>>,
    }

    impl FixedLoadBackend {
        fn new(fs: VirtualFs, vcs: Vcs) -> Self {
            Self {
                state: Mutex::new(Some(LoadedState { fs, vcs })),
            }
        }
    }

    impl PersistenceBackend for FixedLoadBackend {
        fn load(&self) -> Result<Option<LoadedState>, VfsError> {
            Ok(self.state.lock().unwrap().take())
        }

        fn save(&self, _vfs: &VirtualFs, _vcs: &Vcs) -> Result<(), VfsError> {
            Ok(())
        }

        fn info(&self) -> PersistenceInfo {
            PersistenceInfo {
                backend: "fixed-test",
                location: None,
            }
        }
    }

    struct FailingLoadBackend;

    impl PersistenceBackend for FailingLoadBackend {
        fn load(&self) -> Result<Option<LoadedState>, VfsError> {
            Err(VfsError::CorruptStore {
                message: "bad state".to_string(),
            })
        }

        fn save(&self, _vfs: &VirtualFs, _vcs: &Vcs) -> Result<(), VfsError> {
            Ok(())
        }

        fn info(&self) -> PersistenceInfo {
            PersistenceInfo {
                backend: "failing",
                location: None,
            }
        }
    }

    async fn db_with_readable_non_traversable_dir() -> (StratumDb, Session) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();

        db.execute_command("mkdir /nox", &mut root).await.unwrap();
        db.execute_command("touch /nox/hidden.md", &mut root)
            .await
            .unwrap();
        db.execute_command("write /nox/hidden.md needle", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 604 /nox", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let bob = db.login("bob").await.unwrap();
        (db, bob)
    }

    async fn db_with_bob() -> (StratumDb, Session) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let bob = db.login("bob").await.unwrap();
        (db, bob)
    }

    async fn db_with_alice_and_bob() -> (StratumDb, Session, Session) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser alice", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let alice = db.login("alice").await.unwrap();
        let bob = db.login("bob").await.unwrap();
        (db, alice, bob)
    }

    fn scoped_root(read_prefixes: &[&str], write_prefixes: &[&str]) -> Session {
        Session::root().with_scope(
            SessionScope::new(
                read_prefixes.iter().copied(),
                write_prefixes.iter().copied(),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn scoped_session_can_read_allowed_prefix_but_not_sibling() {
        let db = StratumDb::open_memory();
        db.mkdir_p("/workspace/app", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.touch("/workspace/app/readme.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.write_file("/workspace/app/readme.md", b"allowed".to_vec())
            .await
            .unwrap();
        db.mkdir_p("/workspace/apple", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.touch("/workspace/apple/readme.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.write_file("/workspace/apple/readme.md", b"sibling".to_vec())
            .await
            .unwrap();
        let scoped = scoped_root(&["/workspace/app"], &[]);

        assert_eq!(
            db.cat_as("/workspace/app/readme.md", &scoped)
                .await
                .unwrap(),
            b"allowed".to_vec()
        );
        let err = db
            .cat_as("/workspace/apple/readme.md", &scoped)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn scoped_session_requires_write_prefix_for_mutation() {
        let db = StratumDb::open_memory();
        db.mkdir_p("/workspace/app", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        let read_only = scoped_root(&["/workspace/app"], &[]);

        let err = db
            .write_file_as("/workspace/app/new.md", b"blocked".to_vec(), &read_only)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));

        let read_write = scoped_root(&["/workspace/app"], &["/workspace/app"]);
        db.write_file_as("/workspace/app/new.md", b"created".to_vec(), &read_write)
            .await
            .unwrap();

        assert_eq!(
            db.cat("/workspace/app/new.md").await.unwrap(),
            b"created".to_vec()
        );
    }

    #[tokio::test]
    async fn scoped_cp_requires_source_read_and_destination_write_prefixes() {
        let db = StratumDb::open_memory();
        db.mkdir_p("/source", ROOT_UID, ROOT_GID).await.unwrap();
        db.touch("/source/file.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.write_file("/source/file.md", b"source".to_vec())
            .await
            .unwrap();
        db.mkdir_p("/destination", ROOT_UID, ROOT_GID)
            .await
            .unwrap();

        let destination_only = scoped_root(&[], &["/destination"]);
        let err = db
            .cp_as("/source/file.md", "/destination/file.md", &destination_only)
            .await
            .unwrap_err();
        assert!(matches!(err, VfsError::PermissionDenied { .. }));

        let source_only = scoped_root(&["/source"], &[]);
        let err = db
            .cp_as("/source/file.md", "/destination/file.md", &source_only)
            .await
            .unwrap_err();
        assert!(matches!(err, VfsError::PermissionDenied { .. }));

        let scoped = scoped_root(&["/source"], &["/destination"]);
        db.cp_as("/source/file.md", "/destination/file.md", &scoped)
            .await
            .unwrap();

        assert_eq!(
            db.cat("/destination/file.md").await.unwrap(),
            b"source".to_vec()
        );
    }

    #[tokio::test]
    async fn scoped_mv_requires_write_scope_on_source_and_destination() {
        let db = StratumDb::open_memory();
        db.mkdir_p("/source", ROOT_UID, ROOT_GID).await.unwrap();
        db.touch("/source/file.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.write_file("/source/file.md", b"source".to_vec())
            .await
            .unwrap();
        db.mkdir_p("/destination", ROOT_UID, ROOT_GID)
            .await
            .unwrap();

        let destination_only = scoped_root(&[], &["/destination"]);
        let err = db
            .mv_as("/source/file.md", "/destination/file.md", &destination_only)
            .await
            .unwrap_err();
        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert!(db.stat("/source/file.md").await.is_ok());

        let source_only = scoped_root(&[], &["/source"]);
        let err = db
            .mv_as("/source/file.md", "/destination/file.md", &source_only)
            .await
            .unwrap_err();
        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert!(db.stat("/source/file.md").await.is_ok());

        let scoped = scoped_root(&[], &["/source", "/destination"]);
        db.mv_as("/source/file.md", "/destination/file.md", &scoped)
            .await
            .unwrap();

        assert!(db.stat("/source/file.md").await.is_err());
        assert_eq!(
            db.cat("/destination/file.md").await.unwrap(),
            b"source".to_vec()
        );
    }

    #[tokio::test]
    async fn vcs_commands_requiring_global_visibility_are_admin_only() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch secret.md", &mut root)
            .await
            .unwrap();
        db.execute_command("write secret.md before", &mut root)
            .await
            .unwrap();
        let commit = db.commit("init", "root").await.unwrap();
        db.execute_command("write secret.md after", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let mut bob = db.login("bob").await.unwrap();
        for command in [
            "commit blocked".to_string(),
            "log".to_string(),
            "status".to_string(),
            "diff /secret.md".to_string(),
            format!("revert {commit}"),
        ] {
            let err = db.execute_command(&command, &mut bob).await.unwrap_err();
            assert!(matches!(err, VfsError::PermissionDenied { .. }));
        }
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }

    #[tokio::test]
    async fn open_with_backend_uses_runtime_compatibility_over_persisted_state() {
        let persisted_fs = VirtualFs::new_with_options(FsOptions {
            compatibility_target: CompatibilityTarget::Markdown,
        });
        let persist: Arc<dyn PersistenceBackend> =
            Arc::new(FixedLoadBackend::new(persisted_fs, Vcs::new()));
        let config = Config::from_env().with_compatibility_target(CompatibilityTarget::Posix);

        let db = StratumDb::open_with_backend(config, persist).unwrap();

        db.touch("/notes.txt", ROOT_UID, ROOT_GID).await.unwrap();
        assert_eq!(db.cat("/notes.txt").await.unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn open_with_backend_propagates_persistence_load_errors() {
        let config = Config::from_env();
        let err = match StratumDb::open_with_backend(config, Arc::new(FailingLoadBackend)) {
            Ok(_) => panic!("expected persistence load error"),
            Err(err) => err,
        };

        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[tokio::test]
    async fn ref_update_requires_matching_commit_and_version() {
        let db = StratumDb::open_memory();
        let name = "agent/alice/session-1";

        db.touch("/a.txt", ROOT_UID, ROOT_GID).await.unwrap();
        db.commit("first", "root").await.unwrap();
        let id1 = db.vcs_log().await[0].id.to_hex();

        db.touch("/b.txt", ROOT_UID, ROOT_GID).await.unwrap();
        db.commit("second", "root").await.unwrap();
        let id2 = db.vcs_log().await[0].id.to_hex();

        db.touch("/c.txt", ROOT_UID, ROOT_GID).await.unwrap();
        db.commit("third", "root").await.unwrap();
        let id3 = db.vcs_log().await[0].id.to_hex();

        let created = db.create_ref(name, &id1).await.unwrap();
        let fetched = db.get_ref(name).await.unwrap().unwrap();
        assert_eq!(fetched, created);

        let updated = db
            .update_ref(name, &id1, created.version, &id2)
            .await
            .unwrap();
        assert_eq!(updated.target, id2);
        assert_eq!(updated.version, created.version + 1);

        let cas_name = "agent/bob/session-1";
        let cas_created = db
            .compare_and_swap_ref(cas_name, None, None, &id1)
            .await
            .unwrap();
        assert_eq!(cas_created.target, id1);
        assert_eq!(cas_created.version, 1);

        let cas_updated = db
            .compare_and_swap_ref(cas_name, Some(&id1), Some(cas_created.version), &id2)
            .await
            .unwrap();
        assert_eq!(cas_updated.target, id2);
        assert_eq!(cas_updated.version, cas_created.version + 1);

        let stale = db
            .update_ref(name, &id2, created.version, &id3)
            .await
            .unwrap_err();
        assert!(matches!(stale, VfsError::InvalidArgs { .. }));

        let current = db
            .list_refs()
            .await
            .unwrap()
            .into_iter()
            .find(|vcs_ref| vcs_ref.name == name)
            .unwrap();
        assert_eq!(current.target, id2);
        assert_eq!(current.version, updated.version);
    }

    #[tokio::test]
    async fn ref_changes_persist_through_db_save_reload() {
        let tmp = std::env::temp_dir().join(format!(
            "stratum_db_refs_{}_{}",
            std::process::id(),
            unique_suffix()
        ));
        let config = Config::from_env().with_data_dir(&tmp);
        let name = "agent/alice/session-1";

        let db = StratumDb::open(config.clone()).unwrap();
        db.touch("/a.txt", ROOT_UID, ROOT_GID).await.unwrap();
        db.commit("first", "root").await.unwrap();
        let id1 = db.vcs_log().await[0].id.to_hex();
        db.touch("/b.txt", ROOT_UID, ROOT_GID).await.unwrap();
        db.commit("second", "root").await.unwrap();
        let id2 = db.vcs_log().await[0].id.to_hex();

        let created = db.create_ref(name, &id1).await.unwrap();
        let updated = db
            .update_ref(name, &id1, created.version, &id2)
            .await
            .unwrap();
        db.save().await.unwrap();

        let reopened = StratumDb::open(config).unwrap();
        let loaded = reopened.get_ref(name).await.unwrap().unwrap();

        assert_eq!(loaded, updated);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn recursive_tree_does_not_descend_without_execute_permission() {
        let (db, bob) = db_with_readable_non_traversable_dir().await;

        let tree = db.tree_as(None, &bob).await.unwrap();

        assert!(tree.contains("nox/"));
        assert!(!tree.contains("hidden.md"));
    }

    #[tokio::test]
    async fn recursive_find_does_not_descend_without_execute_permission() {
        let (db, bob) = db_with_readable_non_traversable_dir().await;

        let results = db.find_as(None, None, &bob).await.unwrap();

        assert!(results.iter().any(|path| path.contains("nox")));
        assert!(!results.iter().any(|path| path.contains("hidden.md")));
    }

    #[tokio::test]
    async fn recursive_grep_does_not_descend_without_execute_permission() {
        let (db, bob) = db_with_readable_non_traversable_dir().await;

        let results = db.grep_as("needle", None, true, &bob).await.unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn cat_as_does_not_read_through_symlink_to_unreadable_target() {
        let (db, bob) = db_with_bob().await;
        db.touch("/secret.md", ROOT_UID, ROOT_GID).await.unwrap();
        db.write_file("/secret.md", b"classified".to_vec())
            .await
            .unwrap();
        db.chmod("/secret.md", 0o600).await.unwrap();
        db.ln_s("/secret.md", "/public-link.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();

        let err = db.cat_as("/public-link.md", &bob).await.unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn cat_as_does_not_read_through_symlink_without_target_traverse() {
        let (db, bob) = db_with_bob().await;
        db.mkdir("/sealed", ROOT_UID, ROOT_GID).await.unwrap();
        db.touch("/sealed/readable.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.write_file("/sealed/readable.md", b"hidden".to_vec())
            .await
            .unwrap();
        db.chmod("/sealed", 0o604).await.unwrap();
        db.ln_s("/sealed/readable.md", "/sealed-link.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();

        let err = db.cat_as("/sealed-link.md", &bob).await.unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn write_file_as_does_not_write_through_symlink_to_unwritable_target() {
        let (db, bob) = db_with_bob().await;
        db.touch("/target.md", ROOT_UID, ROOT_GID).await.unwrap();
        db.write_file("/target.md", b"original".to_vec())
            .await
            .unwrap();
        db.chmod("/target.md", 0o644).await.unwrap();
        db.ln_s("/target.md", "/write-link.md", ROOT_UID, ROOT_GID)
            .await
            .unwrap();

        let err = db
            .write_file_as("/write-link.md", b"changed".to_vec(), &bob)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(db.cat("/target.md").await.unwrap(), b"original".to_vec());
    }

    #[tokio::test]
    async fn recursive_rm_as_does_not_delete_non_traversable_subtree() {
        let (db, bob) = db_with_bob().await;
        db.mkdir_p_as("/home/bob/tree/blocked", &bob).await.unwrap();
        db.write_file_as("/home/bob/tree/blocked/hidden.md", b"hidden".to_vec(), &bob)
            .await
            .unwrap();
        db.chmod("/home/bob/tree/blocked", 0o600).await.unwrap();

        let err = db.rm_as("/home/bob/tree", true, &bob).await.unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert!(db.stat("/home/bob/tree/blocked/hidden.md").await.is_ok());
    }

    #[tokio::test]
    async fn cp_as_denies_overwrite_when_destination_child_is_not_writable() {
        let (db, alice, bob) = db_with_alice_and_bob().await;

        db.mkdir("/shared.md", ROOT_UID, ROOT_GID).await.unwrap();
        db.chmod("/shared.md", 0o777).await.unwrap();
        db.touch("/source.md", ROOT_UID, ROOT_GID).await.unwrap();
        db.write_file("/source.md", b"source".to_vec())
            .await
            .unwrap();
        db.chmod("/source.md", 0o644).await.unwrap();
        db.write_file_as("/shared.md/source.md", b"alice".to_vec(), &alice)
            .await
            .unwrap();
        db.chmod("/shared.md/source.md", 0o600).await.unwrap();

        let err = db
            .cp_as("/source.md", "/shared.md", &bob)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(
            db.cat("/shared.md/source.md").await.unwrap(),
            b"alice".to_vec()
        );
    }

    #[tokio::test]
    async fn cp_as_denies_existing_destination_dir_without_ancestor_execute() {
        let (db, bob) = db_with_bob().await;

        db.write_file_as("/home/bob/source.md", b"source".to_vec(), &bob)
            .await
            .unwrap();
        db.mkdir("/sealed", ROOT_UID, ROOT_GID).await.unwrap();
        db.mkdir("/sealed/target", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.chmod("/sealed", 0o604).await.unwrap();
        db.chmod("/sealed/target", 0o777).await.unwrap();

        let err = db
            .cp_as("/home/bob/source.md", "/sealed/target", &bob)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert!(db.stat("/sealed/target/source.md").await.is_err());
    }

    #[tokio::test]
    async fn mv_as_denies_overwrite_of_sticky_destination_child_owned_by_other() {
        let (db, alice, bob) = db_with_alice_and_bob().await;

        db.mkdir("/tmp.md", ROOT_UID, ROOT_GID).await.unwrap();
        db.chmod("/tmp.md", 0o1777).await.unwrap();
        db.write_file_as("/tmp.md/collide.md", b"alice".to_vec(), &alice)
            .await
            .unwrap();
        db.chmod("/tmp.md/collide.md", 0o644).await.unwrap();
        db.write_file_as("/home/bob/collide.md", b"bob".to_vec(), &bob)
            .await
            .unwrap();

        let err = db
            .mv_as("/home/bob/collide.md", "/tmp.md", &bob)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert_eq!(
            db.cat("/tmp.md/collide.md").await.unwrap(),
            b"alice".to_vec()
        );
        assert_eq!(
            db.cat("/home/bob/collide.md").await.unwrap(),
            b"bob".to_vec()
        );
    }

    #[tokio::test]
    async fn mv_as_denies_existing_destination_dir_without_ancestor_execute() {
        let (db, bob) = db_with_bob().await;

        db.write_file_as("/home/bob/source.md", b"source".to_vec(), &bob)
            .await
            .unwrap();
        db.mkdir("/sealed", ROOT_UID, ROOT_GID).await.unwrap();
        db.mkdir("/sealed/target", ROOT_UID, ROOT_GID)
            .await
            .unwrap();
        db.chmod("/sealed", 0o604).await.unwrap();
        db.chmod("/sealed/target", 0o777).await.unwrap();

        let err = db
            .mv_as("/home/bob/source.md", "/sealed/target", &bob)
            .await
            .unwrap_err();

        assert!(matches!(err, VfsError::PermissionDenied { .. }));
        assert!(db.stat("/sealed/target/source.md").await.is_err());
        assert_eq!(
            db.cat("/home/bob/source.md").await.unwrap(),
            b"source".to_vec()
        );
    }
}
