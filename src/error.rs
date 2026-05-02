use std::fmt;

#[derive(Debug)]
pub enum VfsError {
    InvalidExtension { name: String },
    InvalidHandle { handle: u64 },
    NotFound { path: String },
    IsDirectory { path: String },
    NotDirectory { path: String },
    AlreadyExists { path: String },
    NotEmpty { path: String },
    InvalidPath { path: String },
    IoError(std::io::Error),
    UnknownCommand { name: String },
    InvalidArgs { message: String },
    SymlinkLoop { path: String },
    ObjectNotFound { id: String },
    ObjectWriteConflict { message: String },
    CorruptStore { message: String },
    NoCommits,
    DirtyWorkingTree,
    PermissionDenied { path: String },
    AuthError { message: String },
    NotSupported { message: String },
}

impl fmt::Display for VfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VfsError::InvalidExtension { name } => {
                write!(
                    f,
                    "stratum: markdown compatibility mode only supports .md files: '{name}'"
                )
            }
            VfsError::InvalidHandle { handle } => write!(f, "stratum: invalid handle: {handle}"),
            VfsError::NotFound { path } => {
                write!(f, "stratum: no such file or directory: '{path}'")
            }
            VfsError::IsDirectory { path } => write!(f, "stratum: is a directory: '{path}'"),
            VfsError::NotDirectory { path } => write!(f, "stratum: not a directory: '{path}'"),
            VfsError::AlreadyExists { path } => write!(f, "stratum: already exists: '{path}'"),
            VfsError::NotEmpty { path } => write!(f, "stratum: directory not empty: '{path}'"),
            VfsError::InvalidPath { path } => write!(f, "stratum: invalid path: '{path}'"),
            VfsError::IoError(e) => write!(f, "stratum: I/O error: {e}"),
            VfsError::UnknownCommand { name } => write!(f, "stratum: unknown command: '{name}'"),
            VfsError::InvalidArgs { message } => write!(f, "stratum: {message}"),
            VfsError::SymlinkLoop { path } => write!(f, "stratum: symlink loop: '{path}'"),
            VfsError::ObjectNotFound { id } => write!(f, "stratum: object not found: {id}"),
            VfsError::ObjectWriteConflict { message } => {
                write!(f, "stratum: object write conflict: {message}")
            }
            VfsError::CorruptStore { message } => write!(f, "stratum: corrupt store: {message}"),
            VfsError::NoCommits => write!(f, "stratum: no commits yet"),
            VfsError::DirtyWorkingTree => {
                write!(f, "stratum: working tree has uncommitted changes")
            }
            VfsError::PermissionDenied { path } => {
                write!(f, "stratum: permission denied: '{path}'")
            }
            VfsError::AuthError { message } => write!(f, "stratum: {message}"),
            VfsError::NotSupported { message } => {
                write!(f, "stratum: operation not supported: {message}")
            }
        }
    }
}

impl std::error::Error for VfsError {}

impl From<std::io::Error> for VfsError {
    fn from(e: std::io::Error) -> Self {
        VfsError::IoError(e)
    }
}
