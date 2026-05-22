//! Protocol-neutral read-only mount adapter types.

use std::fmt;
use std::str::FromStr;

/// Mount protocol backend preference.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountBackend {
    /// Linux FUSE mount backend.
    Fuse,
    /// NFS-over-localhost mount backend.
    Nfs,
}

impl Default for MountBackend {
    fn default() -> Self {
        if cfg!(target_os = "linux") {
            Self::Fuse
        } else {
            Self::Nfs
        }
    }
}

impl fmt::Display for MountBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fuse => f.write_str("fuse"),
            Self::Nfs => f.write_str("nfs"),
        }
    }
}

impl FromStr for MountBackend {
    type Err = MountError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fuse" => Ok(Self::Fuse),
            "nfs" => Ok(Self::Nfs),
            _ => Err(MountError::new(MountErrorCode::InvalidInput)),
        }
    }
}

/// Protocol-neutral file kind.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountFileKind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

/// Protocol-neutral file size.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountFileSize {
    /// The exact byte size is known.
    Known(u64),
    /// The exact byte size is unknown at the adapter boundary.
    Unknown,
}

impl MountFileSize {
    /// Returns known bytes, or `None` when the domain size is unknown.
    #[must_use]
    pub const fn known_bytes(self) -> Option<u64> {
        match self {
            Self::Known(bytes) => Some(bytes),
            Self::Unknown => None,
        }
    }

    /// Returns whether the domain size is known.
    #[must_use]
    pub const fn is_known(self) -> bool {
        matches!(self, Self::Known(_))
    }
}

/// Seconds/nanoseconds timestamp used by protocol-neutral mount attributes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MountTimestamp {
    /// Whole seconds since the Unix epoch.
    pub seconds: i64,
    /// Additional nanoseconds within the current second.
    pub nanos: u32,
}

impl MountTimestamp {
    /// Creates a timestamp from epoch seconds and nanoseconds.
    #[must_use]
    pub const fn new(seconds: i64, nanos: u32) -> Self {
        Self { seconds, nanos }
    }
}

/// Protocol-neutral inode attributes.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountAttr {
    /// Stable inode id for this mount view.
    pub ino: u64,
    /// File kind.
    pub kind: MountFileKind,
    /// Domain file size, preserving unknown-size state.
    pub size: MountFileSize,
    /// POSIX mode bits.
    pub mode: u32,
    /// Owning user id.
    pub uid: u32,
    /// Owning group id.
    pub gid: u32,
    /// Link count.
    pub nlink: u32,
    /// Preferred block size.
    pub block_size: u32,
    /// Allocated block count.
    pub blocks: u64,
    /// Last access timestamp.
    pub accessed: MountTimestamp,
    /// Last modification timestamp.
    pub modified: MountTimestamp,
    /// Metadata change timestamp.
    pub changed: MountTimestamp,
    /// Creation timestamp.
    pub created: MountTimestamp,
}

impl MountAttr {
    /// Creates attributes with conservative defaults for unit-testable adapters.
    #[must_use]
    pub const fn new(ino: u64, kind: MountFileKind, size: MountFileSize) -> Self {
        Self {
            ino,
            kind,
            size,
            mode: 0,
            uid: 0,
            gid: 0,
            nlink: 1,
            block_size: 4096,
            blocks: 0,
            accessed: MountTimestamp::new(0, 0),
            modified: MountTimestamp::new(0, 0),
            changed: MountTimestamp::new(0, 0),
            created: MountTimestamp::new(0, 0),
        }
    }

    /// Returns known bytes, or `None` when the domain size is unknown.
    #[must_use]
    pub const fn known_bytes(&self) -> Option<u64> {
        self.size.known_bytes()
    }

    /// Returns whether the domain size is known.
    #[must_use]
    pub const fn size_known(&self) -> bool {
        self.size.is_known()
    }
}

/// Directory entry returned by `readdir_plus`.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountDirEntry {
    /// Entry name relative to its parent directory.
    pub name: String,
    /// Entry attributes.
    pub attr: MountAttr,
}

impl MountDirEntry {
    /// Creates a directory entry.
    #[must_use]
    pub fn new(name: impl Into<String>, attr: MountAttr) -> Self {
        Self {
            name: name.into(),
            attr,
        }
    }
}

/// Protocol-neutral filesystem statistics.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountStatfs {
    /// Total block count.
    pub blocks_total: u64,
    /// Free block count.
    pub blocks_free: u64,
    /// Available block count for unprivileged callers.
    pub blocks_available: u64,
    /// Total inode count.
    pub inode_count: u64,
    /// Free inode count.
    pub inodes_free: u64,
    /// File count.
    pub file_count: u64,
    /// Directory count.
    pub directory_count: u64,
    /// Symlink count.
    pub symlink_count: u64,
    /// Total bytes used.
    pub bytes_used: u64,
    /// Blocks used.
    pub blocks_used: u64,
    /// Filesystem block size.
    pub block_size: u32,
    /// Fundamental fragment size.
    pub fragment_size: u32,
    /// Maximum file name length.
    pub name_max: u32,
}

impl MountStatfs {
    /// Creates filesystem statistics with synthetic free-space defaults.
    #[must_use]
    pub const fn new(
        inode_count: u64,
        file_count: u64,
        directory_count: u64,
        symlink_count: u64,
        bytes_used: u64,
        blocks_used: u64,
        block_size: u32,
    ) -> Self {
        Self {
            blocks_total: u64::MAX / 2,
            blocks_free: u64::MAX / 2,
            blocks_available: u64::MAX / 2,
            inode_count,
            inodes_free: u64::MAX / 2,
            file_count,
            directory_count,
            symlink_count,
            bytes_used,
            blocks_used,
            block_size,
            fragment_size: block_size,
            name_max: 255,
        }
    }
}

/// Redacted mount error category.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountErrorCode {
    /// Entry was not found.
    NotFound,
    /// Entry is not a directory.
    NotDirectory,
    /// Entry is a directory where a non-directory was required.
    IsDirectory,
    /// Operation was denied.
    PermissionDenied,
    /// Entry already exists.
    AlreadyExists,
    /// Directory is not empty.
    NotEmpty,
    /// Input was invalid.
    InvalidInput,
    /// Operation is not supported.
    NotSupported,
    /// I/O failed.
    Io,
}

impl MountErrorCode {
    const fn redacted_message(self) -> &'static str {
        match self {
            Self::NotFound => "not found",
            Self::NotDirectory => "not a directory",
            Self::IsDirectory => "is a directory",
            Self::PermissionDenied => "permission denied",
            Self::AlreadyExists => "already exists",
            Self::NotEmpty => "directory not empty",
            Self::InvalidInput => "invalid input",
            Self::NotSupported => "operation not supported",
            Self::Io => "io error",
        }
    }
}

/// Redacted mount adapter error.
#[derive(Clone, Eq, PartialEq)]
pub struct MountError {
    code: MountErrorCode,
}

impl MountError {
    /// Creates a redacted mount error from a category.
    #[must_use]
    pub const fn new(code: MountErrorCode) -> Self {
        Self { code }
    }

    /// Returns the error category.
    #[must_use]
    pub const fn code(&self) -> MountErrorCode {
        self.code
    }

    fn redacted_message(&self) -> &'static str {
        self.code.redacted_message()
    }
}

impl fmt::Display for MountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mount operation failed: {}", self.redacted_message())
    }
}

impl fmt::Debug for MountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MountError")
            .field("code", &self.code)
            .field("message", &self.to_string())
            .finish()
    }
}

impl std::error::Error for MountError {}

/// Provider-free synchronous read-only mount adapter surface.
pub trait MountReadAdapter {
    /// Looks up a named child in a parent inode.
    fn lookup(&self, parent_ino: u64, name: &str) -> Result<Option<MountAttr>, MountError>;

    /// Gets attributes for an inode.
    fn getattr(&self, ino: u64) -> Result<Option<MountAttr>, MountError>;

    /// Reads directory entry names for an inode.
    fn readdir(&self, ino: u64) -> Result<Option<Vec<String>>, MountError>;

    /// Reads directory entries and attributes for an inode.
    fn readdir_plus(&self, ino: u64) -> Result<Option<Vec<MountDirEntry>>, MountError>;

    /// Reads file bytes from an inode.
    fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, MountError>;

    /// Reads a symlink target from an inode.
    fn readlink(&self, ino: u64) -> Result<Option<String>, MountError>;

    /// Gets filesystem statistics.
    fn statfs(&self) -> Result<MountStatfs, MountError>;
}

/// Policy for mapping unknown domain sizes to protocol-required numeric sizes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownSizePolicy {
    /// Map unknown size to zero.
    Zero,
    /// Map unknown size to a configured sentinel.
    Sentinel(u64),
}

impl UnknownSizePolicy {
    const fn map_size(self, size: MountFileSize) -> u64 {
        match size {
            MountFileSize::Known(bytes) => bytes,
            MountFileSize::Unknown => match self {
                Self::Zero => 0,
                Self::Sentinel(bytes) => bytes,
            },
        }
    }
}

/// Minimal FUSE-shaped wire attributes for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FuseWireAttr {
    /// Inode id.
    pub ino: u64,
    /// File kind.
    pub kind: MountFileKind,
    /// Protocol numeric size.
    pub size: u64,
    /// POSIX mode bits.
    pub mode: u32,
    /// Owning user id.
    pub uid: u32,
    /// Owning group id.
    pub gid: u32,
    /// Link count.
    pub nlink: u32,
    /// Preferred block size.
    pub block_size: u32,
    /// Allocated block count.
    pub blocks: u64,
}

/// Maps protocol-neutral attributes to dependency-free FUSE-shaped attributes.
#[must_use]
pub fn fuse_wire_attr(attr: &MountAttr, unknown_size_policy: UnknownSizePolicy) -> FuseWireAttr {
    FuseWireAttr {
        ino: attr.ino,
        kind: attr.kind,
        size: unknown_size_policy.map_size(attr.size),
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        block_size: attr.block_size,
        blocks: attr.blocks,
    }
}

/// Minimal NFS-shaped wire attributes for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NfsWireAttr {
    /// File id.
    pub fileid: u64,
    /// File kind.
    pub kind: MountFileKind,
    /// Protocol numeric size.
    pub size: u64,
    /// POSIX mode bits.
    pub mode: u32,
    /// Owning user id.
    pub uid: u32,
    /// Owning group id.
    pub gid: u32,
    /// Link count.
    pub nlink: u32,
    /// Preferred block size.
    pub block_size: u32,
    /// Allocated block count.
    pub blocks: u64,
}

/// Maps protocol-neutral attributes to dependency-free NFS-shaped attributes.
#[must_use]
pub fn nfs_wire_attr(attr: &MountAttr, unknown_size_policy: UnknownSizePolicy) -> NfsWireAttr {
    NfsWireAttr {
        fileid: attr.ino,
        kind: attr.kind,
        size: unknown_size_policy.map_size(attr.size),
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        block_size: attr.block_size,
        blocks: attr.blocks,
    }
}

/// Maps a redacted mount error to a Linux FUSE errno code.
#[must_use]
pub const fn fuse_wire_error_code(error: &MountError) -> i32 {
    match error.code() {
        MountErrorCode::NotFound => 2,
        MountErrorCode::NotDirectory => 20,
        MountErrorCode::IsDirectory => 21,
        MountErrorCode::PermissionDenied => 13,
        MountErrorCode::AlreadyExists => 17,
        MountErrorCode::NotEmpty => 39,
        MountErrorCode::InvalidInput => 22,
        MountErrorCode::NotSupported => 95,
        MountErrorCode::Io => 5,
    }
}

/// Maps a redacted mount error to an NFS status code.
#[must_use]
pub const fn nfs_wire_error_code(error: &MountError) -> u32 {
    match error.code() {
        MountErrorCode::NotFound => 2,
        MountErrorCode::NotDirectory => 20,
        MountErrorCode::IsDirectory => 21,
        MountErrorCode::PermissionDenied => 13,
        MountErrorCode::AlreadyExists => 17,
        MountErrorCode::NotEmpty => 66,
        MountErrorCode::InvalidInput => 22,
        MountErrorCode::NotSupported => 10004,
        MountErrorCode::Io => 5,
    }
}

/// Minimal FUSE-shaped directory entry for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FuseWireDirEntry {
    /// Entry name relative to its parent directory.
    pub name: String,
    /// Entry attributes.
    pub attr: FuseWireAttr,
}

/// Maps protocol-neutral directory entries to dependency-free FUSE-shaped entries.
#[must_use]
pub fn fuse_wire_dir_entries(
    entries: &[MountDirEntry],
    unknown_size_policy: UnknownSizePolicy,
) -> Vec<FuseWireDirEntry> {
    entries
        .iter()
        .map(|entry| FuseWireDirEntry {
            name: entry.name.clone(),
            attr: fuse_wire_attr(&entry.attr, unknown_size_policy),
        })
        .collect()
}

/// Minimal NFS-shaped directory entry for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NfsWireDirEntry {
    /// Entry name relative to its parent directory.
    pub name: String,
    /// Entry attributes.
    pub attr: NfsWireAttr,
}

/// Maps protocol-neutral directory entries to dependency-free NFS-shaped entries.
#[must_use]
pub fn nfs_wire_dir_entries(
    entries: &[MountDirEntry],
    unknown_size_policy: UnknownSizePolicy,
) -> Vec<NfsWireDirEntry> {
    entries
        .iter()
        .map(|entry| NfsWireDirEntry {
            name: entry.name.clone(),
            attr: nfs_wire_attr(&entry.attr, unknown_size_policy),
        })
        .collect()
}

/// Minimal FUSE-shaped read result.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub struct FuseWireRead {
    /// Bytes read from the file.
    pub data: Vec<u8>,
}

impl fmt::Debug for FuseWireRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FuseWireRead")
            .field("data_len", &self.data.len())
            .finish()
    }
}

/// Maps protocol-neutral read bytes to a dependency-free FUSE-shaped result.
#[must_use]
pub fn fuse_wire_read(data: &[u8]) -> FuseWireRead {
    FuseWireRead {
        data: data.to_vec(),
    }
}

/// Minimal NFS-shaped read result.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub struct NfsWireRead {
    /// Bytes read from the file.
    pub data: Vec<u8>,
    /// Whether the read reached end-of-file.
    pub eof: bool,
}

impl fmt::Debug for NfsWireRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NfsWireRead")
            .field("data_len", &self.data.len())
            .field("eof", &self.eof)
            .finish()
    }
}

/// Maps protocol-neutral read bytes to a dependency-free NFS-shaped result.
#[must_use]
pub fn nfs_wire_read(data: &[u8], eof: bool) -> NfsWireRead {
    NfsWireRead {
        data: data.to_vec(),
        eof,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn unknown_size_attr() -> MountAttr {
        MountAttr::new(42, MountFileKind::File, MountFileSize::Unknown)
    }

    #[test]
    fn mount_backend_default_matches_platform() {
        let expected = if cfg!(target_os = "linux") {
            MountBackend::Fuse
        } else {
            MountBackend::Nfs
        };

        assert_eq!(MountBackend::default(), expected);
    }

    #[test]
    fn unknown_size_stays_domain_unknown_until_wire_mapping() {
        let attr = unknown_size_attr();

        assert_eq!(attr.size, MountFileSize::Unknown);
    }

    #[test]
    fn fuse_wire_mapping_uses_configured_unknown_size_policy() {
        let wire_attr = fuse_wire_attr(&unknown_size_attr(), UnknownSizePolicy::Sentinel(8192));

        assert_eq!(wire_attr.size, 8192);
    }

    #[test]
    fn nfs_wire_mapping_uses_configured_unknown_size_policy() {
        let wire_attr = nfs_wire_attr(&unknown_size_attr(), UnknownSizePolicy::Zero);

        assert_eq!(wire_attr.size, 0);
    }

    #[test]
    fn mount_error_display_and_debug_are_redacted() {
        let error = MountError::new(MountErrorCode::Io);

        assert_eq!(error.to_string(), "mount operation failed: io error");
        assert_eq!(
            format!("{error:?}"),
            "MountError { code: Io, message: \"mount operation failed: io error\" }"
        );
    }

    #[test]
    fn mount_error_maps_to_fuse_and_nfs_wire_codes() {
        let cases = [
            (MountErrorCode::NotFound, 2, 2),
            (MountErrorCode::NotDirectory, 20, 20),
            (MountErrorCode::IsDirectory, 21, 21),
            (MountErrorCode::PermissionDenied, 13, 13),
            (MountErrorCode::AlreadyExists, 17, 17),
            (MountErrorCode::NotEmpty, 39, 66),
            (MountErrorCode::InvalidInput, 22, 22),
            (MountErrorCode::NotSupported, 95, 10004),
            (MountErrorCode::Io, 5, 5),
        ];

        for (code, expected_fuse, expected_nfs) in cases {
            let error = MountError::new(code);

            assert_eq!(fuse_wire_error_code(&error), expected_fuse);
            assert_eq!(nfs_wire_error_code(&error), expected_nfs);
        }
    }

    #[test]
    fn readdir_plus_entries_map_unknown_sizes_only_at_wire_boundary() {
        let entries = vec![
            MountDirEntry::new(
                "unknown.bin",
                MountAttr::new(7, MountFileKind::File, MountFileSize::Unknown),
            ),
            MountDirEntry::new(
                "known.bin",
                MountAttr::new(8, MountFileKind::File, MountFileSize::Known(128)),
            ),
        ];

        let fuse_entries = fuse_wire_dir_entries(&entries, UnknownSizePolicy::Sentinel(4096));
        let nfs_entries = nfs_wire_dir_entries(&entries, UnknownSizePolicy::Zero);

        assert_eq!(entries[0].attr.size, MountFileSize::Unknown);
        assert_eq!(
            fuse_entries,
            vec![
                FuseWireDirEntry {
                    name: "unknown.bin".to_owned(),
                    attr: FuseWireAttr {
                        ino: 7,
                        kind: MountFileKind::File,
                        size: 4096,
                        mode: 0,
                        uid: 0,
                        gid: 0,
                        nlink: 1,
                        block_size: 4096,
                        blocks: 0,
                    },
                },
                FuseWireDirEntry {
                    name: "known.bin".to_owned(),
                    attr: FuseWireAttr {
                        ino: 8,
                        kind: MountFileKind::File,
                        size: 128,
                        mode: 0,
                        uid: 0,
                        gid: 0,
                        nlink: 1,
                        block_size: 4096,
                        blocks: 0,
                    },
                },
            ]
        );
        assert_eq!(nfs_entries[0].attr.size, 0);
        assert_eq!(nfs_entries[1].attr.size, 128);
    }

    #[test]
    fn read_results_map_to_fuse_and_nfs_wire_shapes() {
        let bytes = vec![1, 2, 3, 4];

        assert_eq!(
            fuse_wire_read(&bytes),
            FuseWireRead {
                data: vec![1, 2, 3, 4],
            }
        );
        assert_eq!(
            nfs_wire_read(&bytes, true),
            NfsWireRead {
                data: vec![1, 2, 3, 4],
                eof: true,
            }
        );
    }

    #[test]
    fn read_wire_debug_redacts_bytes() {
        let bytes = b"secret file bytes".to_vec();

        assert_eq!(
            format!("{:?}", fuse_wire_read(&bytes)),
            "FuseWireRead { data_len: 17 }"
        );
        assert_eq!(
            format!("{:?}", nfs_wire_read(&bytes, false)),
            "NfsWireRead { data_len: 17, eof: false }"
        );
    }

    struct ExternalStyleAdapter;

    impl MountReadAdapter for ExternalStyleAdapter {
        fn lookup(&self, _parent_ino: u64, _name: &str) -> Result<Option<MountAttr>, MountError> {
            Ok(None)
        }

        fn getattr(&self, _ino: u64) -> Result<Option<MountAttr>, MountError> {
            Ok(None)
        }

        fn readdir(&self, _ino: u64) -> Result<Option<Vec<String>>, MountError> {
            Ok(Some(Vec::new()))
        }

        fn readdir_plus(&self, _ino: u64) -> Result<Option<Vec<MountDirEntry>>, MountError> {
            Ok(Some(Vec::new()))
        }

        fn read(&self, _ino: u64, _offset: u64, _size: u32) -> Result<Vec<u8>, MountError> {
            Ok(Vec::new())
        }

        fn readlink(&self, _ino: u64) -> Result<Option<String>, MountError> {
            Ok(None)
        }

        fn statfs(&self) -> Result<MountStatfs, MountError> {
            Ok(MountStatfs::new(3, 1, 1, 1, 4096, 1, 4096))
        }
    }

    #[test]
    fn external_style_adapter_can_construct_statfs() {
        let statfs = ExternalStyleAdapter.statfs().unwrap();

        assert_eq!(statfs.inode_count, 3);
        assert_eq!(statfs.file_count, 1);
        assert_eq!(statfs.directory_count, 1);
        assert_eq!(statfs.symlink_count, 1);
        assert_eq!(statfs.bytes_used, 4096);
        assert_eq!(statfs.blocks_used, 1);
        assert_eq!(statfs.block_size, 4096);
        assert_eq!(statfs.fragment_size, 4096);
        assert_eq!(statfs.name_max, 255);
    }
}
