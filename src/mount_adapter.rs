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
#[derive(Clone, Eq, PartialEq)]
pub struct MountDirEntry {
    /// Entry name relative to its parent directory.
    pub name: String,
    /// Entry attributes.
    pub attr: MountAttr,
}

impl fmt::Debug for MountDirEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDirEntry")
            .field("name_len", &self.name.len())
            .field("attr", &self.attr)
            .finish()
    }
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

/// Redacted mount write error category.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountWriteErrorCode {
    /// Write-shaped mount operations are disabled for this adapter.
    ReadOnlyDisabled,
    /// Input was invalid.
    InvalidInput,
    /// I/O failed.
    Io,
}

impl MountWriteErrorCode {
    const fn redacted_message(self) -> &'static str {
        match self {
            Self::ReadOnlyDisabled => "write disabled",
            Self::InvalidInput => "invalid input",
            Self::Io => "io error",
        }
    }
}

/// Redacted mount write error.
#[derive(Clone, Eq, PartialEq)]
pub struct MountWriteError {
    code: MountWriteErrorCode,
}

impl MountWriteError {
    /// Creates a redacted mount write error from a category.
    #[must_use]
    pub const fn new(code: MountWriteErrorCode) -> Self {
        Self { code }
    }

    /// Returns the error category.
    #[must_use]
    pub const fn code(&self) -> MountWriteErrorCode {
        self.code
    }

    fn redacted_message(&self) -> &'static str {
        self.code.redacted_message()
    }
}

impl fmt::Display for MountWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mount write operation failed: {}",
            self.redacted_message()
        )
    }
}

impl fmt::Debug for MountWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MountWriteError")
            .field("code", &self.code)
            .field("message", &self.to_string())
            .finish()
    }
}

impl std::error::Error for MountWriteError {}

/// Minimal write result for future protocol wiring.
#[non_exhaustive]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MountWriteOutcome {
    /// Number of bytes accepted by the write adapter.
    pub bytes_written: u32,
}

impl fmt::Debug for MountWriteOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountWriteOutcome")
            .field("bytes_written", &"<redacted>")
            .finish()
    }
}

/// Minimal flush result for future protocol wiring.
#[non_exhaustive]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MountFlushOutcome {
    /// Whether local dirty work was queued for write-back.
    pub queued: bool,
}

impl fmt::Debug for MountFlushOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountFlushOutcome")
            .field("queued", &self.queued)
            .finish()
    }
}

/// Minimal fsync result for future protocol wiring.
#[non_exhaustive]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MountFsyncOutcome {
    /// Whether bytes are durable after the fsync request.
    pub durable: bool,
}

impl fmt::Debug for MountFsyncOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountFsyncOutcome")
            .field("durable", &self.durable)
            .finish()
    }
}

/// Provider-free synchronous write-shaped mount adapter surface.
pub trait MountWriteAdapter {
    /// Writes file bytes at an offset.
    fn write(
        &self,
        ino: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<MountWriteOutcome, MountWriteError>;

    /// Flushes local dirty state for an inode.
    fn flush(&self, ino: u64) -> Result<MountFlushOutcome, MountWriteError>;

    /// Synchronizes local dirty state for an inode.
    fn fsync(&self, ino: u64, datasync: bool) -> Result<MountFsyncOutcome, MountWriteError>;
}

/// Disabled write adapter used by read-only rollback/default mounts.
#[derive(Clone, Copy, Debug, Default)]
pub struct DisabledMountWriteAdapter;

impl DisabledMountWriteAdapter {
    const fn disabled_error() -> MountWriteError {
        MountWriteError::new(MountWriteErrorCode::ReadOnlyDisabled)
    }
}

impl MountWriteAdapter for DisabledMountWriteAdapter {
    fn write(
        &self,
        _ino: u64,
        _offset: u64,
        _data: &[u8],
    ) -> Result<MountWriteOutcome, MountWriteError> {
        Err(Self::disabled_error())
    }

    fn flush(&self, _ino: u64) -> Result<MountFlushOutcome, MountWriteError> {
        Err(Self::disabled_error())
    }

    fn fsync(&self, _ino: u64, _datasync: bool) -> Result<MountFsyncOutcome, MountWriteError> {
        Err(Self::disabled_error())
    }
}

/// Returns the disabled write adapter for read-only rollback/default mounts.
#[must_use]
pub const fn disabled_mount_write_adapter() -> DisabledMountWriteAdapter {
    DisabledMountWriteAdapter
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
#[derive(Clone, Eq, PartialEq)]
pub struct FuseWireDirEntry {
    /// Entry name relative to its parent directory.
    pub name: String,
    /// Entry attributes.
    pub attr: FuseWireAttr,
}

impl fmt::Debug for FuseWireDirEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FuseWireDirEntry")
            .field("name_len", &self.name.len())
            .field("attr", &self.attr)
            .finish()
    }
}

/// Minimal FUSE-shaped name-only directory listing for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub struct FuseWireDirNames {
    /// Entry names relative to the listed directory.
    pub names: Vec<String>,
}

impl fmt::Debug for FuseWireDirNames {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name_lengths = self.names.iter().map(String::len).collect::<Vec<_>>();

        formatter
            .debug_struct("FuseWireDirNames")
            .field("count", &self.names.len())
            .field("name_lengths", &name_lengths)
            .finish()
    }
}

/// Maps protocol-neutral directory names to a dependency-free FUSE-shaped listing.
#[must_use]
pub fn fuse_wire_dir_names(names: &[String]) -> FuseWireDirNames {
    FuseWireDirNames {
        names: names.to_vec(),
    }
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
#[derive(Clone, Eq, PartialEq)]
pub struct NfsWireDirEntry {
    /// Entry name relative to its parent directory.
    pub name: String,
    /// Entry attributes.
    pub attr: NfsWireAttr,
}

impl fmt::Debug for NfsWireDirEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NfsWireDirEntry")
            .field("name_len", &self.name.len())
            .field("attr", &self.attr)
            .finish()
    }
}

/// Minimal NFS-shaped name-only directory listing for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub struct NfsWireDirNames {
    /// Entry names relative to the listed directory.
    pub names: Vec<String>,
}

impl fmt::Debug for NfsWireDirNames {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name_lengths = self.names.iter().map(String::len).collect::<Vec<_>>();

        formatter
            .debug_struct("NfsWireDirNames")
            .field("count", &self.names.len())
            .field("name_lengths", &name_lengths)
            .finish()
    }
}

/// Maps protocol-neutral directory names to a dependency-free NFS-shaped listing.
#[must_use]
pub fn nfs_wire_dir_names(names: &[String]) -> NfsWireDirNames {
    NfsWireDirNames {
        names: names.to_vec(),
    }
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

/// Dependency-free result shape for FUSE-like operation probes.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub enum FuseOperationResult<T> {
    /// Operation completed successfully.
    Ok(T),
    /// Operation failed with a redacted errno value.
    Errno(i32),
}

impl<T> FuseOperationResult<T> {
    fn from_adapter_result<U>(result: Result<U, MountError>, map_ok: impl FnOnce(U) -> T) -> Self {
        match result {
            Ok(value) => Self::Ok(map_ok(value)),
            Err(error) => Self::Errno(fuse_wire_error_code(&error)),
        }
    }

    fn from_optional_adapter_result<U>(
        result: Result<Option<U>, MountError>,
        map_ok: impl FnOnce(U) -> T,
    ) -> Self {
        match result {
            Ok(Some(value)) => Self::Ok(map_ok(value)),
            Ok(None) => Self::Errno(fuse_wire_error_code(&MountError::new(
                MountErrorCode::NotFound,
            ))),
            Err(error) => Self::Errno(fuse_wire_error_code(&error)),
        }
    }
}

impl<T> fmt::Debug for FuseOperationResult<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok(_) => formatter.write_str("Ok(<redacted>)"),
            Self::Errno(errno) => formatter.debug_tuple("Errno").field(errno).finish(),
        }
    }
}

/// Dependency-free result shape for NFS-like operation probes.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub enum NfsOperationResult<T> {
    /// Operation completed successfully.
    Ok(T),
    /// Operation failed with a redacted NFS status value.
    Status(u32),
}

impl<T> NfsOperationResult<T> {
    fn from_adapter_result<U>(result: Result<U, MountError>, map_ok: impl FnOnce(U) -> T) -> Self {
        match result {
            Ok(value) => Self::Ok(map_ok(value)),
            Err(error) => Self::Status(nfs_wire_error_code(&error)),
        }
    }

    fn from_optional_adapter_result<U>(
        result: Result<Option<U>, MountError>,
        map_ok: impl FnOnce(U) -> T,
    ) -> Self {
        match result {
            Ok(Some(value)) => Self::Ok(map_ok(value)),
            Ok(None) => Self::Status(nfs_wire_error_code(&MountError::new(
                MountErrorCode::NotFound,
            ))),
            Err(error) => Self::Status(nfs_wire_error_code(&error)),
        }
    }
}

impl<T> fmt::Debug for NfsOperationResult<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok(_) => formatter.write_str("Ok(<redacted>)"),
            Self::Status(status) => formatter.debug_tuple("Status").field(status).finish(),
        }
    }
}

/// Minimal FUSE-shaped statfs result for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FuseWireStatfs {
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

/// Maps protocol-neutral statfs data to dependency-free FUSE-shaped statfs data.
#[must_use]
pub const fn fuse_wire_statfs(statfs: &MountStatfs) -> FuseWireStatfs {
    FuseWireStatfs {
        blocks_total: statfs.blocks_total,
        blocks_free: statfs.blocks_free,
        blocks_available: statfs.blocks_available,
        inode_count: statfs.inode_count,
        inodes_free: statfs.inodes_free,
        file_count: statfs.file_count,
        directory_count: statfs.directory_count,
        symlink_count: statfs.symlink_count,
        bytes_used: statfs.bytes_used,
        blocks_used: statfs.blocks_used,
        block_size: statfs.block_size,
        fragment_size: statfs.fragment_size,
        name_max: statfs.name_max,
    }
}

/// Minimal NFS-shaped statfs result for provider-free tests.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NfsWireStatfs {
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

/// Maps protocol-neutral statfs data to dependency-free NFS-shaped statfs data.
#[must_use]
pub const fn nfs_wire_statfs(statfs: &MountStatfs) -> NfsWireStatfs {
    NfsWireStatfs {
        blocks_total: statfs.blocks_total,
        blocks_free: statfs.blocks_free,
        blocks_available: statfs.blocks_available,
        inode_count: statfs.inode_count,
        inodes_free: statfs.inodes_free,
        file_count: statfs.file_count,
        directory_count: statfs.directory_count,
        symlink_count: statfs.symlink_count,
        bytes_used: statfs.bytes_used,
        blocks_used: statfs.blocks_used,
        block_size: statfs.block_size,
        fragment_size: statfs.fragment_size,
        name_max: statfs.name_max,
    }
}

/// Maps an adapter lookup into a dependency-free FUSE-like response.
#[must_use]
pub fn fuse_lookup<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    parent_ino: u64,
    name: &str,
    unknown_size_policy: UnknownSizePolicy,
) -> FuseOperationResult<FuseWireAttr> {
    FuseOperationResult::from_optional_adapter_result(adapter.lookup(parent_ino, name), |attr| {
        fuse_wire_attr(&attr, unknown_size_policy)
    })
}

/// Maps an adapter getattr into a dependency-free FUSE-like response.
#[must_use]
pub fn fuse_getattr<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
    unknown_size_policy: UnknownSizePolicy,
) -> FuseOperationResult<FuseWireAttr> {
    FuseOperationResult::from_optional_adapter_result(adapter.getattr(ino), |attr| {
        fuse_wire_attr(&attr, unknown_size_policy)
    })
}

/// Maps an adapter readdir into a dependency-free FUSE-like response.
#[must_use]
pub fn fuse_readdir<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
) -> FuseOperationResult<FuseWireDirNames> {
    FuseOperationResult::from_optional_adapter_result(adapter.readdir(ino), |names| {
        fuse_wire_dir_names(&names)
    })
}

/// Maps an adapter readdir-plus into a dependency-free FUSE-like response.
#[must_use]
pub fn fuse_readdir_plus<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
    unknown_size_policy: UnknownSizePolicy,
) -> FuseOperationResult<Vec<FuseWireDirEntry>> {
    FuseOperationResult::from_optional_adapter_result(adapter.readdir_plus(ino), |entries| {
        fuse_wire_dir_entries(&entries, unknown_size_policy)
    })
}

/// Maps an adapter read into a dependency-free FUSE-like response.
#[must_use]
pub fn fuse_read<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
    offset: u64,
    size: u32,
) -> FuseOperationResult<FuseWireRead> {
    match adapter.read(ino, offset, size) {
        Ok(data) if data.len() <= size as usize => FuseOperationResult::Ok(fuse_wire_read(&data)),
        Ok(_) => {
            FuseOperationResult::Errno(fuse_wire_error_code(&MountError::new(MountErrorCode::Io)))
        }
        Err(error) => FuseOperationResult::Errno(fuse_wire_error_code(&error)),
    }
}

/// Maps an adapter statfs into a dependency-free FUSE-like response.
#[must_use]
pub fn fuse_statfs<A: MountReadAdapter + ?Sized>(
    adapter: &A,
) -> FuseOperationResult<FuseWireStatfs> {
    FuseOperationResult::from_adapter_result(adapter.statfs(), |statfs| fuse_wire_statfs(&statfs))
}

/// Maps an adapter lookup into a dependency-free NFS-like response.
#[must_use]
pub fn nfs_lookup<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    parent_ino: u64,
    name: &str,
    unknown_size_policy: UnknownSizePolicy,
) -> NfsOperationResult<NfsWireAttr> {
    NfsOperationResult::from_optional_adapter_result(adapter.lookup(parent_ino, name), |attr| {
        nfs_wire_attr(&attr, unknown_size_policy)
    })
}

/// Maps an adapter getattr into a dependency-free NFS-like response.
#[must_use]
pub fn nfs_getattr<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
    unknown_size_policy: UnknownSizePolicy,
) -> NfsOperationResult<NfsWireAttr> {
    NfsOperationResult::from_optional_adapter_result(adapter.getattr(ino), |attr| {
        nfs_wire_attr(&attr, unknown_size_policy)
    })
}

/// Maps an adapter readdir into a dependency-free NFS-like response.
#[must_use]
pub fn nfs_readdir<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
) -> NfsOperationResult<NfsWireDirNames> {
    NfsOperationResult::from_optional_adapter_result(adapter.readdir(ino), |names| {
        nfs_wire_dir_names(&names)
    })
}

/// Maps an adapter readdir-plus into a dependency-free NFS-like response.
#[must_use]
pub fn nfs_readdir_plus<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
    unknown_size_policy: UnknownSizePolicy,
) -> NfsOperationResult<Vec<NfsWireDirEntry>> {
    NfsOperationResult::from_optional_adapter_result(adapter.readdir_plus(ino), |entries| {
        nfs_wire_dir_entries(&entries, unknown_size_policy)
    })
}

/// Maps an adapter read into a dependency-free NFS-like response.
///
/// EOF is conservative because `MountReadAdapter::read` returns bytes, not an
/// explicit EOF marker: short replies are EOF, full-sized replies are not.
#[must_use]
pub fn nfs_read<A: MountReadAdapter + ?Sized>(
    adapter: &A,
    ino: u64,
    offset: u64,
    size: u32,
) -> NfsOperationResult<NfsWireRead> {
    match adapter.read(ino, offset, size) {
        Ok(data) if data.len() <= size as usize => {
            let eof = data.len() < size as usize;

            NfsOperationResult::Ok(nfs_wire_read(&data, eof))
        }
        Ok(_) => {
            NfsOperationResult::Status(nfs_wire_error_code(&MountError::new(MountErrorCode::Io)))
        }
        Err(error) => NfsOperationResult::Status(nfs_wire_error_code(&error)),
    }
}

/// Maps an adapter statfs into a dependency-free NFS-like response.
#[must_use]
pub fn nfs_statfs<A: MountReadAdapter + ?Sized>(adapter: &A) -> NfsOperationResult<NfsWireStatfs> {
    NfsOperationResult::from_adapter_result(adapter.statfs(), |statfs| nfs_wire_statfs(&statfs))
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

    #[test]
    fn directory_debug_does_not_expose_entry_names() {
        let entry = MountDirEntry::new(
            "secret-child.txt",
            MountAttr::new(7, MountFileKind::File, MountFileSize::Known(12)),
        );
        let fuse_entry =
            fuse_wire_dir_entries(std::slice::from_ref(&entry), UnknownSizePolicy::Zero).remove(0);
        let nfs_entry =
            nfs_wire_dir_entries(std::slice::from_ref(&entry), UnknownSizePolicy::Zero).remove(0);
        let fuse_names = fuse_wire_dir_names(&["secret-child.txt".to_string()]);
        let nfs_names = nfs_wire_dir_names(&["secret-child.txt".to_string()]);

        for debug in [
            format!("{entry:?}"),
            format!("{fuse_entry:?}"),
            format!("{nfs_entry:?}"),
            format!("{fuse_names:?}"),
            format!("{nfs_names:?}"),
        ] {
            assert!(!debug.contains("secret-child.txt"));
            assert!(!debug.contains("secret-child"));
        }
    }

    #[derive(Default)]
    struct ProbeAdapter {
        lookup_result: Option<Result<Option<MountAttr>, MountError>>,
        getattr_result: Option<Result<Option<MountAttr>, MountError>>,
        readdir_result: Option<Result<Option<Vec<String>>, MountError>>,
        readdir_plus_result: Option<Result<Option<Vec<MountDirEntry>>, MountError>>,
        read_result: Option<Result<Vec<u8>, MountError>>,
        statfs_result: Option<Result<MountStatfs, MountError>>,
        read_calls: std::cell::Cell<u32>,
    }

    impl MountReadAdapter for ProbeAdapter {
        fn lookup(&self, _parent_ino: u64, _name: &str) -> Result<Option<MountAttr>, MountError> {
            self.lookup_result.clone().unwrap_or(Ok(Some(MountAttr::new(
                2,
                MountFileKind::File,
                MountFileSize::Known(11),
            ))))
        }

        fn getattr(&self, _ino: u64) -> Result<Option<MountAttr>, MountError> {
            self.getattr_result
                .clone()
                .unwrap_or(Ok(Some(MountAttr::new(
                    2,
                    MountFileKind::File,
                    MountFileSize::Known(11),
                ))))
        }

        fn readdir(&self, _ino: u64) -> Result<Option<Vec<String>>, MountError> {
            self.readdir_result
                .clone()
                .unwrap_or(Ok(Some(vec!["child.txt".to_string()])))
        }

        fn readdir_plus(&self, _ino: u64) -> Result<Option<Vec<MountDirEntry>>, MountError> {
            self.readdir_plus_result
                .clone()
                .unwrap_or(Ok(Some(vec![MountDirEntry::new(
                    "child.txt",
                    MountAttr::new(3, MountFileKind::File, MountFileSize::Known(5)),
                )])))
        }

        fn read(&self, _ino: u64, _offset: u64, _size: u32) -> Result<Vec<u8>, MountError> {
            self.read_calls.set(self.read_calls.get() + 1);
            self.read_result.clone().unwrap_or(Ok(b"hello".to_vec()))
        }

        fn readlink(&self, _ino: u64) -> Result<Option<String>, MountError> {
            Ok(None)
        }

        fn statfs(&self) -> Result<MountStatfs, MountError> {
            self.statfs_result
                .clone()
                .unwrap_or(Ok(MountStatfs::new(4, 2, 1, 1, 8192, 2, 4096)))
        }
    }

    #[test]
    fn fuse_operations_map_lookup_getattr_readdir_readdir_plus_read_and_statfs() {
        let adapter = ProbeAdapter::default();

        assert_eq!(
            fuse_lookup(&adapter, 1, "child.txt", UnknownSizePolicy::Zero),
            FuseOperationResult::Ok(FuseWireAttr {
                ino: 2,
                kind: MountFileKind::File,
                size: 11,
                mode: 0,
                uid: 0,
                gid: 0,
                nlink: 1,
                block_size: 4096,
                blocks: 0,
            })
        );
        assert_eq!(
            fuse_getattr(&adapter, 2, UnknownSizePolicy::Zero),
            FuseOperationResult::Ok(FuseWireAttr {
                ino: 2,
                kind: MountFileKind::File,
                size: 11,
                mode: 0,
                uid: 0,
                gid: 0,
                nlink: 1,
                block_size: 4096,
                blocks: 0,
            })
        );
        assert_eq!(
            fuse_readdir(&adapter, 1),
            FuseOperationResult::Ok(FuseWireDirNames {
                names: vec!["child.txt".to_string()],
            })
        );
        assert_eq!(
            fuse_readdir_plus(&adapter, 1, UnknownSizePolicy::Zero),
            FuseOperationResult::Ok(vec![FuseWireDirEntry {
                name: "child.txt".to_owned(),
                attr: FuseWireAttr {
                    ino: 3,
                    kind: MountFileKind::File,
                    size: 5,
                    mode: 0,
                    uid: 0,
                    gid: 0,
                    nlink: 1,
                    block_size: 4096,
                    blocks: 0,
                },
            }])
        );
        assert_eq!(
            fuse_read(&adapter, 2, 0, 64),
            FuseOperationResult::Ok(FuseWireRead {
                data: b"hello".to_vec(),
            })
        );
        assert_eq!(
            fuse_statfs(&adapter),
            FuseOperationResult::Ok(FuseWireStatfs {
                blocks_total: u64::MAX / 2,
                blocks_free: u64::MAX / 2,
                blocks_available: u64::MAX / 2,
                inode_count: 4,
                inodes_free: u64::MAX / 2,
                file_count: 2,
                directory_count: 1,
                symlink_count: 1,
                bytes_used: 8192,
                blocks_used: 2,
                block_size: 4096,
                fragment_size: 4096,
                name_max: 255,
            })
        );
    }

    #[test]
    fn nfs_operations_map_lookup_getattr_readdir_readdir_plus_read_and_statfs() {
        let adapter = ProbeAdapter::default();

        assert_eq!(
            nfs_lookup(&adapter, 1, "child.txt", UnknownSizePolicy::Zero),
            NfsOperationResult::Ok(NfsWireAttr {
                fileid: 2,
                kind: MountFileKind::File,
                size: 11,
                mode: 0,
                uid: 0,
                gid: 0,
                nlink: 1,
                block_size: 4096,
                blocks: 0,
            })
        );
        assert_eq!(
            nfs_getattr(&adapter, 2, UnknownSizePolicy::Zero),
            NfsOperationResult::Ok(NfsWireAttr {
                fileid: 2,
                kind: MountFileKind::File,
                size: 11,
                mode: 0,
                uid: 0,
                gid: 0,
                nlink: 1,
                block_size: 4096,
                blocks: 0,
            })
        );
        assert_eq!(
            nfs_readdir(&adapter, 1),
            NfsOperationResult::Ok(NfsWireDirNames {
                names: vec!["child.txt".to_string()],
            })
        );
        assert_eq!(
            nfs_readdir_plus(&adapter, 1, UnknownSizePolicy::Zero),
            NfsOperationResult::Ok(vec![NfsWireDirEntry {
                name: "child.txt".to_owned(),
                attr: NfsWireAttr {
                    fileid: 3,
                    kind: MountFileKind::File,
                    size: 5,
                    mode: 0,
                    uid: 0,
                    gid: 0,
                    nlink: 1,
                    block_size: 4096,
                    blocks: 0,
                },
            }])
        );
        assert_eq!(
            nfs_read(&adapter, 2, 0, 64),
            NfsOperationResult::Ok(NfsWireRead {
                data: b"hello".to_vec(),
                eof: true,
            })
        );
        assert_eq!(
            nfs_statfs(&adapter),
            NfsOperationResult::Ok(NfsWireStatfs {
                blocks_total: u64::MAX / 2,
                blocks_free: u64::MAX / 2,
                blocks_available: u64::MAX / 2,
                inode_count: 4,
                inodes_free: u64::MAX / 2,
                file_count: 2,
                directory_count: 1,
                symlink_count: 1,
                bytes_used: 8192,
                blocks_used: 2,
                block_size: 4096,
                fragment_size: 4096,
                name_max: 255,
            })
        );
    }

    #[test]
    fn operation_errors_map_to_redacted_numeric_values() {
        let adapter = ProbeAdapter {
            lookup_result: Some(Err(MountError::new(MountErrorCode::PermissionDenied))),
            getattr_result: Some(Err(MountError::new(MountErrorCode::PermissionDenied))),
            readdir_result: Some(Err(MountError::new(MountErrorCode::PermissionDenied))),
            readdir_plus_result: Some(Err(MountError::new(MountErrorCode::PermissionDenied))),
            read_result: Some(Err(MountError::new(MountErrorCode::PermissionDenied))),
            statfs_result: Some(Err(MountError::new(MountErrorCode::PermissionDenied))),
            read_calls: std::cell::Cell::new(0),
        };

        assert_eq!(
            fuse_lookup(&adapter, 1, "secret", UnknownSizePolicy::Zero),
            FuseOperationResult::Errno(13)
        );
        assert_eq!(
            nfs_lookup(&adapter, 1, "secret", UnknownSizePolicy::Zero),
            NfsOperationResult::Status(13)
        );
        assert_eq!(fuse_readdir(&adapter, 1), FuseOperationResult::Errno(13));
        assert_eq!(nfs_readdir(&adapter, 1), NfsOperationResult::Status(13));
        assert_eq!(format!("{:?}", fuse_read(&adapter, 2, 0, 64)), "Errno(13)");
        assert_eq!(format!("{:?}", nfs_read(&adapter, 2, 0, 64)), "Status(13)");
    }

    #[test]
    fn operation_result_debug_redacts_success_payloads() {
        let adapter = ProbeAdapter::default();

        let debug_readdir = format!(
            "{:?}",
            fuse_readdir_plus(&adapter, 1, UnknownSizePolicy::Zero)
        );
        let debug_read = format!("{:?}", nfs_read(&adapter, 2, 0, 64));

        assert!(!debug_readdir.contains("child.txt"));
        assert!(!debug_read.contains("hello"));
    }

    #[test]
    fn missing_lookup_getattr_readdir_and_readdir_plus_map_to_not_found() {
        let adapter = ProbeAdapter {
            lookup_result: Some(Ok(None)),
            getattr_result: Some(Ok(None)),
            readdir_result: Some(Ok(None)),
            readdir_plus_result: Some(Ok(None)),
            ..ProbeAdapter::default()
        };

        assert_eq!(
            fuse_lookup(&adapter, 1, "missing", UnknownSizePolicy::Zero),
            FuseOperationResult::Errno(2)
        );
        assert_eq!(
            nfs_getattr(&adapter, 99, UnknownSizePolicy::Zero),
            NfsOperationResult::Status(2)
        );
        assert_eq!(fuse_readdir(&adapter, 99), FuseOperationResult::Errno(2));
        assert_eq!(nfs_readdir(&adapter, 99), NfsOperationResult::Status(2));
        assert_eq!(
            fuse_readdir_plus(&adapter, 99, UnknownSizePolicy::Zero),
            FuseOperationResult::Errno(2)
        );
    }

    #[test]
    fn plain_readdir_maps_ls_without_reading() {
        let adapter = ProbeAdapter::default();

        assert_eq!(
            fuse_readdir(&adapter, 1),
            FuseOperationResult::Ok(FuseWireDirNames {
                names: vec!["child.txt".to_string()],
            })
        );
        assert_eq!(
            nfs_readdir(&adapter, 1),
            NfsOperationResult::Ok(NfsWireDirNames {
                names: vec!["child.txt".to_string()],
            })
        );
        assert_eq!(adapter.read_calls.get(), 0);
    }

    #[test]
    fn getattr_unknown_size_uses_policy_without_reading() {
        let adapter = ProbeAdapter {
            getattr_result: Some(Ok(Some(MountAttr::new(
                9,
                MountFileKind::File,
                MountFileSize::Unknown,
            )))),
            ..ProbeAdapter::default()
        };

        assert_eq!(
            fuse_getattr(&adapter, 9, UnknownSizePolicy::Sentinel(16 * 1024)),
            FuseOperationResult::Ok(FuseWireAttr {
                ino: 9,
                kind: MountFileKind::File,
                size: 16 * 1024,
                mode: 0,
                uid: 0,
                gid: 0,
                nlink: 1,
                block_size: 4096,
                blocks: 0,
            })
        );
        assert_eq!(adapter.read_calls.get(), 0);
    }

    #[test]
    fn read_unknown_size_returns_bytes_and_nfs_eof_without_getattr_size() {
        let adapter = ProbeAdapter {
            getattr_result: Some(Err(MountError::new(MountErrorCode::Io))),
            read_result: Some(Ok(b"tail".to_vec())),
            ..ProbeAdapter::default()
        };

        assert_eq!(
            nfs_read(&adapter, 9, 0, 128),
            NfsOperationResult::Ok(NfsWireRead {
                data: b"tail".to_vec(),
                eof: true,
            })
        );
        assert_eq!(adapter.read_calls.get(), 1);
    }

    #[test]
    fn read_helpers_reject_over_requested_adapter_bytes() {
        let adapter = ProbeAdapter {
            read_result: Some(Ok(b"overflow".to_vec())),
            ..ProbeAdapter::default()
        };

        assert_eq!(fuse_read(&adapter, 9, 0, 4), FuseOperationResult::Errno(5));
        assert_eq!(nfs_read(&adapter, 9, 0, 4), NfsOperationResult::Status(5));
    }

    #[test]
    fn nfs_read_full_sized_reply_is_conservatively_not_eof() {
        let adapter = ProbeAdapter {
            read_result: Some(Ok(b"tail".to_vec())),
            ..ProbeAdapter::default()
        };

        assert_eq!(
            nfs_read(&adapter, 9, 0, 4),
            NfsOperationResult::Ok(NfsWireRead {
                data: b"tail".to_vec(),
                eof: false,
            })
        );
    }

    #[test]
    fn write_default_mount_write_surface_is_read_only_and_redacted() {
        let adapter = disabled_mount_write_adapter();

        let write = adapter.write(42, 7, b"secret write bytes").unwrap_err();
        let flush = adapter.flush(42).unwrap_err();
        let fsync = adapter.fsync(42, false).unwrap_err();
        let debug = format!("{write:?} {flush:?} {fsync:?}");

        assert_eq!(write.code(), MountWriteErrorCode::ReadOnlyDisabled);
        assert_eq!(flush.code(), MountWriteErrorCode::ReadOnlyDisabled);
        assert_eq!(fsync.code(), MountWriteErrorCode::ReadOnlyDisabled);
        assert_eq!(
            write.to_string(),
            "mount write operation failed: write disabled"
        );
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("42"));
        assert!(!debug.contains("7"));

        let outcome_debug = format!(
            "{:?} {:?} {:?}",
            MountWriteOutcome { bytes_written: 17 },
            MountFlushOutcome { queued: false },
            MountFsyncOutcome { durable: false }
        );
        assert!(!outcome_debug.contains("17"));
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
