use crate::auth::registry::UserRegistry;
use crate::config::CompatibilityTarget;
use crate::error::VfsError;
use crate::fs::inode::{Inode, InodeId, InodeKind};
use crate::fs::{FsOptions, VirtualFs};
use crate::store::ObjectId;
use crate::store::blob::BlobStore;
use crate::store::commit::CommitObject;
use crate::vcs::{CommitId, MAIN_REF, RefName, Vcs, VcsRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const VFS_DIR: &str = ".vfs";
const STATE_FILE: &str = "state.bin";
const VERSION: u32 = 6;

/// Complete persisted state of the VFS + VCS.
#[derive(Serialize, Deserialize)]
struct PersistedState {
    version: u32,
    fs_state: FsState,
    vcs_state: VcsState,
}

#[derive(Serialize, Deserialize)]
struct FsState {
    inodes: HashMap<InodeId, Inode>,
    root: InodeId,
    cwd: InodeId,
    next_id: InodeId,
    cwd_path: Vec<(String, InodeId)>,
    compatibility_target: CompatibilityTarget,
    registry: UserRegistry,
}

#[derive(Serialize, Deserialize)]
struct VcsState {
    /// All objects in the blob store: (ObjectId bytes, kind byte, data)
    objects: Vec<(Vec<u8>, u8, Vec<u8>)>,
    head: Option<Vec<u8>>,
    commits: Vec<CommitObject>,
    refs: Vec<PersistedRef>,
}

#[derive(Serialize, Deserialize)]
struct PersistedRef {
    name: String,
    target: Vec<u8>,
    version: u64,
}

#[derive(Deserialize)]
struct CommitObjectV4 {
    id: ObjectId,
    tree: ObjectId,
    parent: Option<ObjectId>,
    timestamp: u64,
    message: String,
    author: String,
}

impl From<CommitObjectV4> for CommitObject {
    fn from(commit: CommitObjectV4) -> Self {
        Self {
            id: commit.id,
            tree: commit.tree,
            parent: commit.parent,
            timestamp: commit.timestamp,
            message: commit.message,
            author: commit.author,
            changed_paths: Vec::new(),
        }
    }
}

#[derive(Deserialize)]
struct PersistedStateV4 {
    version: u32,
    fs_state: FsStateV5,
    vcs_state: VcsStateV4,
}

#[derive(Deserialize)]
struct VcsStateV4 {
    objects: Vec<(Vec<u8>, u8, Vec<u8>)>,
    head: Option<Vec<u8>>,
    commits: Vec<CommitObjectV4>,
    refs: Vec<PersistedRef>,
}

#[derive(Deserialize)]
struct PersistedStateV5 {
    version: u32,
    fs_state: FsStateV5,
    vcs_state: VcsState,
}

#[derive(Deserialize)]
struct FsStateV5 {
    inodes: HashMap<InodeId, InodeV5>,
    root: InodeId,
    cwd: InodeId,
    next_id: InodeId,
    cwd_path: Vec<(String, InodeId)>,
    compatibility_target: CompatibilityTarget,
    registry: UserRegistry,
}

#[derive(Deserialize)]
struct InodeV5 {
    id: InodeId,
    kind: InodeKindV5,
    mode: u16,
    uid: u32,
    gid: u32,
    #[serde(default = "default_legacy_nlink")]
    nlink: u64,
    #[serde(default = "default_legacy_block_size")]
    block_size: u64,
    #[serde(default)]
    created_at: crate::fs::inode::Timestamp,
    #[serde(default)]
    modified_at: crate::fs::inode::Timestamp,
    #[serde(default)]
    accessed_at: crate::fs::inode::Timestamp,
    #[serde(default)]
    changed_at: crate::fs::inode::Timestamp,
    #[serde(default)]
    created: u64,
    #[serde(default)]
    modified: u64,
}

#[derive(Deserialize)]
enum InodeKindV5 {
    File { content: Vec<u8> },
    Directory { entries: BTreeMap<String, InodeId> },
    Symlink { target: String },
}

// ─── V1 legacy types for migration ───

#[derive(Deserialize)]
struct PersistedStateV1 {
    version: u32,
    fs_state: FsStateV1,
    vcs_state: VcsStateV3,
}

#[derive(Deserialize)]
struct FsStateV1 {
    inodes: HashMap<InodeId, InodeV5>,
    root: InodeId,
    cwd: InodeId,
    next_id: InodeId,
    cwd_path: Vec<(String, InodeId)>,
    // No registry in V1
}

#[derive(Deserialize)]
struct PersistedStateV2 {
    version: u32,
    fs_state: FsStateV2,
    vcs_state: VcsStateV3,
}

#[derive(Deserialize)]
struct FsStateV2 {
    inodes: HashMap<InodeId, InodeV5>,
    root: InodeId,
    cwd: InodeId,
    next_id: InodeId,
    cwd_path: Vec<(String, InodeId)>,
    registry: UserRegistry,
}

#[derive(Deserialize)]
struct PersistedStateV3 {
    version: u32,
    fs_state: FsStateV5,
    vcs_state: VcsStateV3,
}

#[derive(Deserialize)]
struct VcsStateV3 {
    objects: Vec<(Vec<u8>, u8, Vec<u8>)>,
    head: Option<Vec<u8>>,
    commits: Vec<CommitObjectV4>,
}

fn default_legacy_nlink() -> u64 {
    1
}

fn default_legacy_block_size() -> u64 {
    4096
}

pub struct PersistManager {
    base_dir: PathBuf,
}

pub struct LoadedState {
    pub fs: VirtualFs,
    pub vcs: Vcs,
}

#[derive(Debug, Clone)]
pub struct PersistenceInfo {
    pub backend: &'static str,
    pub location: Option<PathBuf>,
}

pub trait PersistenceBackend: Send + Sync {
    fn load(&self) -> Result<Option<LoadedState>, VfsError>;
    fn save(&self, vfs: &VirtualFs, vcs: &Vcs) -> Result<(), VfsError>;
    fn info(&self) -> PersistenceInfo;
}

pub struct LocalStateBackend {
    manager: PersistManager,
}

impl LocalStateBackend {
    pub fn new(base_dir: &Path) -> Self {
        Self {
            manager: PersistManager::new(base_dir),
        }
    }
}

pub struct MemoryPersistenceBackend;

impl PersistenceBackend for LocalStateBackend {
    fn load(&self) -> Result<Option<LoadedState>, VfsError> {
        if !self.manager.state_exists() {
            return Ok(None);
        }
        let (fs, vcs) = self.manager.load()?;
        Ok(Some(LoadedState { fs, vcs }))
    }

    fn save(&self, vfs: &VirtualFs, vcs: &Vcs) -> Result<(), VfsError> {
        self.manager.save(vfs, vcs)
    }

    fn info(&self) -> PersistenceInfo {
        PersistenceInfo {
            backend: "local-state-file",
            location: Some(self.manager.data_dir().to_path_buf()),
        }
    }
}

impl PersistenceBackend for MemoryPersistenceBackend {
    fn load(&self) -> Result<Option<LoadedState>, VfsError> {
        Ok(None)
    }

    fn save(&self, _vfs: &VirtualFs, _vcs: &Vcs) -> Result<(), VfsError> {
        Ok(())
    }

    fn info(&self) -> PersistenceInfo {
        PersistenceInfo {
            backend: "memory",
            location: None,
        }
    }
}

impl InodeV5 {
    fn into_current(self) -> Inode {
        Inode {
            id: self.id,
            kind: match self.kind {
                InodeKindV5::File { content } => InodeKind::File { content },
                InodeKindV5::Directory { entries } => InodeKind::Directory { entries },
                InodeKindV5::Symlink { target } => InodeKind::Symlink { target },
            },
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            nlink: self.nlink,
            block_size: self.block_size,
            created_at: self.created_at,
            modified_at: self.modified_at,
            accessed_at: self.accessed_at,
            changed_at: self.changed_at,
            created: self.created,
            modified: self.modified,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
        }
    }
}

impl PersistManager {
    pub fn new(base_dir: &Path) -> Self {
        PersistManager {
            base_dir: base_dir.join(VFS_DIR),
        }
    }

    pub fn state_exists(&self) -> bool {
        self.base_dir.join(STATE_FILE).exists()
    }

    pub fn save(&self, vfs: &VirtualFs, vcs: &Vcs) -> Result<(), VfsError> {
        fs::create_dir_all(&self.base_dir)?;

        let fs_state = FsState {
            inodes: vfs.all_inodes().clone(),
            root: vfs.root_id(),
            cwd: vfs.cwd_id(),
            next_id: vfs.next_inode_id(),
            cwd_path: vfs.cwd_path_clone(),
            compatibility_target: vfs.compatibility_target(),
            registry: vfs.registry.clone(),
        };

        let vcs_state = VcsState {
            objects: vcs.store.export_all(),
            head: vcs.head.map(|id| id.as_bytes().to_vec()),
            commits: vcs.commits.clone(),
            refs: vcs
                .refs
                .values()
                .map(|vcs_ref| PersistedRef {
                    name: vcs_ref.name.as_str().to_string(),
                    target: vcs_ref.target.object_id().as_bytes().to_vec(),
                    version: vcs_ref.version,
                })
                .collect(),
        };

        let state = PersistedState {
            version: VERSION,
            fs_state,
            vcs_state,
        };

        let data = crate::codec::serialize(&state).map_err(|e| VfsError::CorruptStore {
            message: format!("serialization failed: {e}"),
        })?;

        let tmp_path = self.base_dir.join("state.tmp");
        let final_path = self.base_dir.join(STATE_FILE);

        // Atomic write: write to tmp, then rename
        fs::write(&tmp_path, &data)?;
        fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }

    pub fn load(&self) -> Result<(VirtualFs, Vcs), VfsError> {
        let path = self.base_dir.join(STATE_FILE);
        let data = fs::read(&path)?;

        // Try current version first
        if let Ok(state) = crate::codec::deserialize::<PersistedState>(&data)
            && state.version == VERSION
        {
            return Self::load_current(state);
        }

        // Try V5 migration
        if let Ok(state) = crate::codec::deserialize::<PersistedStateV5>(&data)
            && state.version == 5
        {
            return Self::load_v5(state);
        }

        // Try V4 migration
        if let Ok(state) = crate::codec::deserialize::<PersistedStateV4>(&data)
            && state.version == 4
        {
            return Self::load_v4(state);
        }

        // Try V3 migration
        if let Ok(state) = crate::codec::deserialize::<PersistedStateV3>(&data)
            && state.version == 3
        {
            return Self::load_v3(state);
        }

        // Try V2 migration
        if let Ok(state) = crate::codec::deserialize::<PersistedStateV2>(&data)
            && state.version == 2
        {
            return Self::load_v2(state);
        }

        // Try V1 migration
        if let Ok(state) = crate::codec::deserialize::<PersistedStateV1>(&data)
            && state.version == 1
        {
            return Self::load_v1(state);
        }

        Err(VfsError::CorruptStore {
            message: "failed to deserialize state (unknown version or corrupt data)".to_string(),
        })
    }

    fn load_current(state: PersistedState) -> Result<(VirtualFs, Vcs), VfsError> {
        let vfs = VirtualFs::from_persisted(
            state.fs_state.inodes,
            state.fs_state.root,
            state.fs_state.cwd,
            state.fs_state.next_id,
            state.fs_state.cwd_path,
            FsOptions {
                compatibility_target: state.fs_state.compatibility_target,
            },
            state.fs_state.registry,
        );
        let vcs = Self::load_vcs(state.vcs_state)?;
        Ok((vfs, vcs))
    }

    fn load_v5(state: PersistedStateV5) -> Result<(VirtualFs, Vcs), VfsError> {
        let vfs = Self::load_fs_v5(state.fs_state);
        let vcs = Self::load_vcs(state.vcs_state)?;
        Ok((vfs, vcs))
    }

    fn load_fs_v5(fs_state: FsStateV5) -> VirtualFs {
        VirtualFs::from_persisted(
            fs_state
                .inodes
                .into_iter()
                .map(|(id, inode)| (id, inode.into_current()))
                .collect(),
            fs_state.root,
            fs_state.cwd,
            fs_state.next_id,
            fs_state.cwd_path,
            FsOptions {
                compatibility_target: fs_state.compatibility_target,
            },
            fs_state.registry,
        )
    }

    fn load_v4(state: PersistedStateV4) -> Result<(VirtualFs, Vcs), VfsError> {
        let vfs = Self::load_fs_v5(state.fs_state);
        let vcs = Self::load_vcs_v4(state.vcs_state)?;
        Ok((vfs, vcs))
    }

    fn load_v3(state: PersistedStateV3) -> Result<(VirtualFs, Vcs), VfsError> {
        let vfs = Self::load_fs_v5(state.fs_state);
        let vcs = Self::load_vcs_v3(state.vcs_state)?;
        Ok((vfs, vcs))
    }

    fn load_v2(state: PersistedStateV2) -> Result<(VirtualFs, Vcs), VfsError> {
        let vfs = VirtualFs::from_persisted(
            state
                .fs_state
                .inodes
                .into_iter()
                .map(|(id, inode)| (id, inode.into_current()))
                .collect(),
            state.fs_state.root,
            state.fs_state.cwd,
            state.fs_state.next_id,
            state.fs_state.cwd_path,
            FsOptions::default(),
            state.fs_state.registry,
        );

        let vcs = Self::load_vcs_v3(state.vcs_state)?;
        Ok((vfs, vcs))
    }

    fn load_v1(state: PersistedStateV1) -> Result<(VirtualFs, Vcs), VfsError> {
        // V1 inodes already have uid/gid=0 defaults from serde (they were added with defaults)
        // Create a fresh registry (root user only)
        let registry = UserRegistry::new();

        let vfs = VirtualFs::from_persisted(
            state
                .fs_state
                .inodes
                .into_iter()
                .map(|(id, inode)| (id, inode.into_current()))
                .collect(),
            state.fs_state.root,
            state.fs_state.cwd,
            state.fs_state.next_id,
            state.fs_state.cwd_path,
            FsOptions::default(),
            registry,
        );

        let vcs = Self::load_vcs_v3(state.vcs_state)?;
        Ok((vfs, vcs))
    }

    fn load_vcs(vcs_state: VcsState) -> Result<Vcs, VfsError> {
        Self::load_vcs_with_refs(
            vcs_state.objects,
            vcs_state.head,
            vcs_state.commits,
            vcs_state.refs,
        )
    }

    fn load_vcs_v4(vcs_state: VcsStateV4) -> Result<Vcs, VfsError> {
        Self::load_vcs_with_refs(
            vcs_state.objects,
            vcs_state.head,
            vcs_state
                .commits
                .into_iter()
                .map(CommitObject::from)
                .collect(),
            vcs_state.refs,
        )
    }

    fn load_vcs_with_refs(
        objects: Vec<(Vec<u8>, u8, Vec<u8>)>,
        persisted_head: Option<Vec<u8>>,
        commits: Vec<CommitObject>,
        persisted_refs: Vec<PersistedRef>,
    ) -> Result<Vcs, VfsError> {
        let mut store = BlobStore::new();
        store.import_all(objects)?;

        let head = decode_object_id_opt(persisted_head)?;
        if let Some(head) = head {
            ensure_persisted_commit_exists(&commits, CommitId::from(head))?;
        }

        let mut refs = BTreeMap::new();
        for persisted_ref in persisted_refs {
            let target = CommitId::from(decode_object_id(persisted_ref.target)?);
            let name = RefName::new(persisted_ref.name)?;
            if persisted_ref.version == 0 || persisted_ref.version == u64::MAX {
                return Err(VfsError::CorruptStore {
                    message: format!("ref {name} has invalid version {}", persisted_ref.version),
                });
            }
            ensure_persisted_commit_exists(&commits, target)?;

            if refs
                .insert(
                    name.clone(),
                    VcsRef {
                        name: name.clone(),
                        target,
                        version: persisted_ref.version,
                    },
                )
                .is_some()
            {
                return Err(VfsError::CorruptStore {
                    message: format!("duplicate ref: {name}"),
                });
            }
        }

        match head {
            Some(head) => match refs.get(&RefName::new(MAIN_REF)?) {
                Some(main) if main.target.object_id() == head => {}
                Some(_) => {
                    return Err(VfsError::CorruptStore {
                        message: "main ref does not match legacy head".to_string(),
                    });
                }
                None => {
                    return Err(VfsError::CorruptStore {
                        message: "main ref missing for persisted head".to_string(),
                    });
                }
            },
            None if refs.contains_key(&RefName::new(MAIN_REF)?) => {
                return Err(VfsError::CorruptStore {
                    message: "main ref exists without persisted head".to_string(),
                });
            }
            None => {}
        }

        Ok(Vcs {
            store,
            head,
            commits,
            refs,
        })
    }

    fn load_vcs_v3(vcs_state: VcsStateV3) -> Result<Vcs, VfsError> {
        let mut store = BlobStore::new();
        store.import_all(vcs_state.objects)?;

        let head = decode_object_id_opt(vcs_state.head)?;
        let commits = vcs_state
            .commits
            .into_iter()
            .map(CommitObject::from)
            .collect();
        let mut vcs = Vcs {
            store,
            head,
            commits,
            refs: BTreeMap::new(),
        };
        if let Some(head) = head {
            ensure_persisted_commit_exists(&vcs.commits, CommitId::from(head))?;
            vcs.set_ref_target_unchecked(RefName::new(MAIN_REF)?, CommitId::from(head))?;
        }
        Ok(vcs)
    }

    pub fn data_dir(&self) -> &Path {
        &self.base_dir
    }
}

fn decode_object_id(bytes: Vec<u8>) -> Result<ObjectId, VfsError> {
    let arr: [u8; 32] = bytes.try_into().map_err(|_| VfsError::CorruptStore {
        message: "invalid object id length".to_string(),
    })?;
    Ok(ObjectId::from_raw(arr))
}

fn decode_object_id_opt(bytes: Option<Vec<u8>>) -> Result<Option<ObjectId>, VfsError> {
    bytes.map(decode_object_id).transpose()
}

fn ensure_persisted_commit_exists(commits: &[CommitObject], id: CommitId) -> Result<(), VfsError> {
    if commits.iter().any(|commit| commit.id == id.object_id()) {
        Ok(())
    } else {
        Err(VfsError::CorruptStore {
            message: format!("ref points to unknown commit: {}", id.short_hex()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{ROOT_GID, ROOT_UID};
    use crate::fs::inode::Timestamp;
    use crate::store::commit::CommitObject;
    use serde::Serialize;

    #[derive(Serialize)]
    struct PersistedStateV5Fixture {
        version: u32,
        fs_state: FsStateV5Fixture,
        vcs_state: VcsState,
    }

    #[derive(Serialize)]
    struct FsStateV5Fixture {
        inodes: HashMap<InodeId, InodeV5Fixture>,
        root: InodeId,
        cwd: InodeId,
        next_id: InodeId,
        cwd_path: Vec<(String, InodeId)>,
        compatibility_target: CompatibilityTarget,
        registry: UserRegistry,
    }

    #[derive(Serialize)]
    struct InodeV5Fixture {
        id: InodeId,
        kind: InodeKindV5Fixture,
        mode: u16,
        uid: u32,
        gid: u32,
        nlink: u64,
        block_size: u64,
        created_at: Timestamp,
        modified_at: Timestamp,
        accessed_at: Timestamp,
        changed_at: Timestamp,
        created: u64,
        modified: u64,
    }

    #[derive(Serialize)]
    #[allow(dead_code)]
    enum InodeKindV5Fixture {
        File { content: Vec<u8> },
        Directory { entries: BTreeMap<String, InodeId> },
        Symlink { target: String },
    }

    #[derive(Serialize)]
    struct TreeObjectV5Fixture {
        entries: Vec<TreeEntryV5Fixture>,
    }

    #[derive(Serialize)]
    struct TreeEntryV5Fixture {
        name: String,
        kind: TreeEntryKindV5Fixture,
        id: ObjectId,
        mode: u16,
        uid: u32,
        gid: u32,
    }

    #[derive(Serialize)]
    #[allow(dead_code)]
    enum TreeEntryKindV5Fixture {
        Blob,
        Tree,
        Symlink,
    }

    fn fs_state_v5(fs: &VirtualFs) -> FsStateV5 {
        FsStateV5 {
            inodes: fs
                .all_inodes()
                .iter()
                .map(|(id, inode)| (*id, inode_v5(inode)))
                .collect(),
            root: fs.root_id(),
            cwd: fs.cwd_id(),
            next_id: fs.next_inode_id(),
            cwd_path: fs.cwd_path_clone(),
            compatibility_target: fs.compatibility_target(),
            registry: fs.registry.clone(),
        }
    }

    fn inode_v5(inode: &Inode) -> InodeV5 {
        InodeV5 {
            id: inode.id,
            kind: match &inode.kind {
                InodeKind::File { content } => InodeKindV5::File {
                    content: content.clone(),
                },
                InodeKind::Directory { entries } => InodeKindV5::Directory {
                    entries: entries.clone(),
                },
                InodeKind::Symlink { target } => InodeKindV5::Symlink {
                    target: target.clone(),
                },
            },
            mode: inode.mode,
            uid: inode.uid,
            gid: inode.gid,
            nlink: inode.nlink,
            block_size: inode.block_size,
            created_at: inode.created_at,
            modified_at: inode.modified_at,
            accessed_at: inode.accessed_at,
            changed_at: inode.changed_at,
            created: inode.created,
            modified: inode.modified,
        }
    }

    fn legacy_commits(vcs: &Vcs) -> Vec<CommitObjectV4> {
        vcs.commits
            .iter()
            .map(|commit| CommitObjectV4 {
                id: commit.id,
                tree: commit.tree,
                parent: commit.parent,
                timestamp: commit.timestamp,
                message: commit.message.clone(),
                author: commit.author.clone(),
            })
            .collect()
    }

    fn write_state_fixture(base_dir: &Path, data: &[u8]) {
        let state_dir = base_dir.join(VFS_DIR);
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(state_dir.join(STATE_FILE), data).unwrap();
    }

    #[test]
    fn v5_load_defaults_file_and_tree_metadata() {
        let tmp = std::env::temp_dir().join(format!(
            "stratum_v5_metadata_migration_{}_{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));

        let timestamp = Timestamp::now();
        let mut root_entries = BTreeMap::new();
        root_entries.insert("old.txt".to_string(), 1);
        let mut inodes = HashMap::new();
        inodes.insert(
            0,
            InodeV5Fixture {
                id: 0,
                kind: InodeKindV5Fixture::Directory {
                    entries: root_entries,
                },
                mode: 0o755,
                uid: ROOT_UID,
                gid: ROOT_GID,
                nlink: 2,
                block_size: 4096,
                created_at: timestamp,
                modified_at: timestamp,
                accessed_at: timestamp,
                changed_at: timestamp,
                created: timestamp.secs,
                modified: timestamp.secs,
            },
        );
        inodes.insert(
            1,
            InodeV5Fixture {
                id: 1,
                kind: InodeKindV5Fixture::File {
                    content: b"old".to_vec(),
                },
                mode: 0o644,
                uid: ROOT_UID,
                gid: ROOT_GID,
                nlink: 1,
                block_size: 4096,
                created_at: timestamp,
                modified_at: timestamp,
                accessed_at: timestamp,
                changed_at: timestamp,
                created: timestamp.secs,
                modified: timestamp.secs,
            },
        );

        let blob_id = ObjectId::from_bytes(b"old");
        let tree = TreeObjectV5Fixture {
            entries: vec![TreeEntryV5Fixture {
                name: "old.txt".to_string(),
                kind: TreeEntryKindV5Fixture::Blob,
                id: blob_id,
                mode: 0o644,
                uid: ROOT_UID,
                gid: ROOT_GID,
            }],
        };
        let tree_data = crate::codec::serialize(&tree).unwrap();
        let tree_id = ObjectId::from_bytes(&tree_data);
        let commit_id = ObjectId::from_bytes(b"legacy v5 commit");
        let commit = CommitObject {
            id: commit_id,
            tree: tree_id,
            parent: None,
            timestamp: timestamp.secs,
            message: "legacy v5".to_string(),
            author: "root".to_string(),
            changed_paths: Vec::new(),
        };

        let state = PersistedStateV5Fixture {
            version: 5,
            fs_state: FsStateV5Fixture {
                inodes,
                root: 0,
                cwd: 0,
                next_id: 2,
                cwd_path: Vec::new(),
                compatibility_target: CompatibilityTarget::Posix,
                registry: UserRegistry::new(),
            },
            vcs_state: VcsState {
                objects: vec![
                    (blob_id.as_bytes().to_vec(), 0, b"old".to_vec()),
                    (tree_id.as_bytes().to_vec(), 1, tree_data),
                ],
                head: Some(commit_id.as_bytes().to_vec()),
                commits: vec![commit],
                refs: vec![PersistedRef {
                    name: MAIN_REF.to_string(),
                    target: commit_id.as_bytes().to_vec(),
                    version: 1,
                }],
            },
        };

        let data = crate::codec::serialize(&state).unwrap();
        write_state_fixture(&tmp, &data);
        let (fs, vcs) = PersistManager::new(&tmp).load().unwrap();

        let stat = fs.stat("/old.txt").unwrap();
        assert_eq!(stat.mime_type, None);
        assert!(stat.custom_attrs.is_empty());
        assert!(vcs.status_summary(&fs).unwrap().is_clean());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn v3_load_synthesizes_main_ref_from_legacy_head() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "legacy", "root").unwrap();

        let state = PersistedStateV3 {
            version: 3,
            fs_state: fs_state_v5(&fs),
            vcs_state: VcsStateV3 {
                objects: vcs.store.export_all(),
                head: vcs.head.map(|id| id.as_bytes().to_vec()),
                commits: legacy_commits(&vcs),
            },
        };

        let (_, loaded_vcs) = PersistManager::load_v3(state).unwrap();
        let main = loaded_vcs
            .get_ref(RefName::new(MAIN_REF).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(main.target, CommitId::from(commit_id));
        assert_eq!(main.version, 1);
    }

    #[test]
    fn v4_load_migrates_commits_with_empty_changed_paths() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "legacy", "root").unwrap();

        let state = PersistedStateV4 {
            version: 4,
            fs_state: fs_state_v5(&fs),
            vcs_state: VcsStateV4 {
                objects: vcs.store.export_all(),
                head: Some(commit_id.as_bytes().to_vec()),
                commits: legacy_commits(&vcs),
                refs: vec![PersistedRef {
                    name: MAIN_REF.to_string(),
                    target: commit_id.as_bytes().to_vec(),
                    version: 1,
                }],
            },
        };

        let (_, loaded_vcs) = PersistManager::load_v4(state).unwrap();
        assert_eq!(loaded_vcs.commits.len(), 1);
        assert!(loaded_vcs.commits[0].changed_paths.is_empty());
    }

    #[test]
    fn v4_load_rejects_ref_to_unknown_commit() {
        let missing = ObjectId::from_bytes(b"missing commit");
        let state = VcsState {
            objects: Vec::new(),
            head: None,
            commits: Vec::new(),
            refs: vec![PersistedRef {
                name: "agent/alice/session-1".to_string(),
                target: missing.as_bytes().to_vec(),
                version: 1,
            }],
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected unknown ref target to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn v4_load_rejects_max_ref_version() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "current", "root").unwrap();

        let state = VcsState {
            objects: vcs.store.export_all(),
            head: Some(commit_id.as_bytes().to_vec()),
            commits: vcs.commits.clone(),
            refs: vec![PersistedRef {
                name: MAIN_REF.to_string(),
                target: commit_id.as_bytes().to_vec(),
                version: u64::MAX,
            }],
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected max ref version to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn v4_load_rejects_zero_ref_version() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "current", "root").unwrap();

        let state = VcsState {
            objects: vcs.store.export_all(),
            head: Some(commit_id.as_bytes().to_vec()),
            commits: vcs.commits.clone(),
            refs: vec![PersistedRef {
                name: MAIN_REF.to_string(),
                target: commit_id.as_bytes().to_vec(),
                version: 0,
            }],
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected zero ref version to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn v4_load_rejects_duplicate_refs() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "current", "root").unwrap();
        let target = commit_id.as_bytes().to_vec();

        let state = VcsState {
            objects: vcs.store.export_all(),
            head: Some(commit_id.as_bytes().to_vec()),
            commits: vcs.commits.clone(),
            refs: vec![
                PersistedRef {
                    name: MAIN_REF.to_string(),
                    target: target.clone(),
                    version: 1,
                },
                PersistedRef {
                    name: MAIN_REF.to_string(),
                    target,
                    version: 2,
                },
            ],
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected duplicate refs to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn v4_load_rejects_missing_main_ref_when_head_exists() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "current", "root").unwrap();

        let state = VcsState {
            objects: vcs.store.export_all(),
            head: Some(commit_id.as_bytes().to_vec()),
            commits: vcs.commits.clone(),
            refs: Vec::new(),
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected missing main ref to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn v4_load_rejects_main_without_head() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/data.txt", ROOT_UID, ROOT_GID).unwrap();
        let commit_id = vcs.commit(&fs, "current", "root").unwrap();

        let state = VcsState {
            objects: vcs.store.export_all(),
            head: None,
            commits: vcs.commits.clone(),
            refs: vec![PersistedRef {
                name: MAIN_REF.to_string(),
                target: commit_id.as_bytes().to_vec(),
                version: 1,
            }],
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected main without head to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }

    #[test]
    fn v4_load_rejects_main_target_mismatch() {
        let mut fs = VirtualFs::new();
        let mut vcs = Vcs::new();
        fs.touch("/first.txt", ROOT_UID, ROOT_GID).unwrap();
        let id1 = vcs.commit(&fs, "first", "root").unwrap();
        fs.touch("/second.txt", ROOT_UID, ROOT_GID).unwrap();
        let id2 = vcs.commit(&fs, "second", "root").unwrap();

        let state = VcsState {
            objects: vcs.store.export_all(),
            head: Some(id2.as_bytes().to_vec()),
            commits: vcs.commits.clone(),
            refs: vec![PersistedRef {
                name: MAIN_REF.to_string(),
                target: id1.as_bytes().to_vec(),
                version: 2,
            }],
        };

        let err = match PersistManager::load_vcs(state) {
            Ok(_) => panic!("expected main/head mismatch to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::CorruptStore { .. }));
    }
}
