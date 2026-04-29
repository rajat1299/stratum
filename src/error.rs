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
                write!(f, "lattice: only .md files are supported: '{name}'")
            }
            VfsError::InvalidHandle { handle } => write!(f, "lattice: invalid handle: {handle}"),
            VfsError::NotFound { path } => write!(f, "lattice: no such file or directory: '{path}'"),
            VfsError::IsDirectory { path } => write!(f, "lattice: is a directory: '{path}'"),
            VfsError::NotDirectory { path } => write!(f, "lattice: not a directory: '{path}'"),
            VfsError::AlreadyExists { path } => write!(f, "lattice: already exists: '{path}'"),
            VfsError::NotEmpty { path } => write!(f, "lattice: directory not empty: '{path}'"),
            VfsError::InvalidPath { path } => write!(f, "lattice: invalid path: '{path}'"),
            VfsError::IoError(e) => write!(f, "lattice: I/O error: {e}"),
            VfsError::UnknownCommand { name } => write!(f, "lattice: unknown command: '{name}'"),
            VfsError::InvalidArgs { message } => write!(f, "lattice: {message}"),
            VfsError::SymlinkLoop { path } => write!(f, "lattice: symlink loop: '{path}'"),
            VfsError::ObjectNotFound { id } => write!(f, "lattice: object not found: {id}"),
            VfsError::CorruptStore { message } => write!(f, "lattice: corrupt store: {message}"),
            VfsError::NoCommits => write!(f, "lattice: no commits yet"),
            VfsError::DirtyWorkingTree => {
                write!(f, "lattice: working tree has uncommitted changes")
            }
            VfsError::PermissionDenied { path } => {
                write!(f, "lattice: permission denied: '{path}'")
            }
            VfsError::AuthError { message } => write!(f, "lattice: {message}"),
            VfsError::NotSupported { message } => write!(f, "lattice: operation not supported: {message}"),
        }
    }
}

impl std::error::Error for VfsError {}

impl From<std::io::Error> for VfsError {
    fn from(e: std::io::Error) -> Self {
        VfsError::IoError(e)
    }
}
