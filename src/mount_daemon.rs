//! Provider-free mount daemon status and path model.

use crate::mount_adapter::MountBackend;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

const MAX_TAG_LEN: usize = 64;
const MAX_SOCKET_PATH_LEN: usize = 100;
const REDACTED_MARKER: &str = "<redacted>";

/// Mount backend preference used by daemon status models.
pub type MountDaemonBackend = MountBackend;

/// Validated local identity for one mount daemon control plane.
#[derive(Clone, Eq, PartialEq)]
pub struct MountDaemonTag {
    value: String,
}

impl MountDaemonTag {
    /// Creates a daemon tag safe for deterministic runtime path derivation.
    ///
    /// # Errors
    ///
    /// Returns [`MountDaemonErrorCode::InvalidTag`] when the tag is empty,
    /// too long, path-like, or contains characters outside the accepted set.
    pub fn new(value: impl Into<String>) -> Result<Self, MountDaemonError> {
        let value = value.into();
        if is_valid_tag(&value) {
            Ok(Self { value })
        } else {
            Err(MountDaemonError::new(MountDaemonErrorCode::InvalidTag))
        }
    }

    /// Returns the validated runtime path component.
    #[must_use]
    pub fn path_component(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for MountDaemonTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDaemonTag")
            .field("value", &"<redacted>")
            .field("len", &self.value.len())
            .finish()
    }
}

impl fmt::Display for MountDaemonTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// Deterministic daemon metadata paths under a caller-provided runtime root.
#[derive(Clone, Eq, PartialEq)]
pub struct MountDaemonPaths {
    runtime_root: PathBuf,
    tag: MountDaemonTag,
    pid_path: PathBuf,
    socket_path: PathBuf,
    log_path: PathBuf,
}

impl MountDaemonPaths {
    /// Derives daemon metadata paths without touching the filesystem.
    ///
    /// # Errors
    ///
    /// Returns [`MountDaemonErrorCode::InvalidInput`] when the runtime root
    /// is relative, contains NUL or parent-directory components, or when the
    /// derived socket path exceeds the conservative daemon model limit.
    pub fn new(
        runtime_root: impl AsRef<Path>,
        tag: MountDaemonTag,
    ) -> Result<Self, MountDaemonError> {
        let runtime_root = runtime_root.as_ref().to_path_buf();
        validate_runtime_root(&runtime_root)?;

        let path_component = tag.path_component();
        let pid_path = runtime_root.join(format!("{path_component}.pid"));
        let socket_path = runtime_root.join(format!("{path_component}.sock"));
        let log_path = runtime_root.join(format!("{path_component}.log"));
        validate_socket_path_len(&socket_path)?;

        Ok(Self {
            runtime_root,
            tag,
            pid_path,
            socket_path,
            log_path,
        })
    }

    /// Returns the derived PID file path.
    #[must_use]
    pub fn pid_path(&self) -> &Path {
        &self.pid_path
    }

    /// Returns the derived local socket path.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Returns the derived log file path.
    #[must_use]
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn tag(&self) -> &MountDaemonTag {
        &self.tag
    }
}

impl fmt::Debug for MountDaemonPaths {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDaemonPaths")
            .field("runtime_root", &"<redacted>")
            .field(
                "runtime_root_component_count",
                &self.runtime_root.components().count(),
            )
            .field("tag", &self.tag)
            .field("pid_file_name_len", &file_name_len(&self.pid_path))
            .field("socket_file_name_len", &file_name_len(&self.socket_path))
            .field("log_file_name_len", &file_name_len(&self.log_path))
            .finish()
    }
}

/// Public daemon lifecycle state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountDaemonState {
    /// No daemon metadata indicates a running daemon.
    Stopped,
    /// Daemon metadata indicates an active daemon.
    Running,
    /// PID metadata exists but no live daemon was confirmed.
    StalePid,
    /// Daemon metadata indicates a crash.
    Crashed,
    /// Daemon metadata indicates an unmount request is in progress.
    Unmounting,
    /// Status could not be resolved through the local control plane.
    Unavailable,
}

impl fmt::Display for MountDaemonState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stopped => "stopped",
            Self::Running => "running",
            Self::StalePid => "stale_pid",
            Self::Crashed => "crashed",
            Self::Unmounting => "unmounting",
            Self::Unavailable => "unavailable",
        })
    }
}

/// Count-only hydration progress included in daemon status.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MountDaemonHydrationProgress {
    /// Hydration jobs waiting to run.
    pub pending: u64,
    /// Hydration jobs currently running.
    pub running: u64,
    /// Hydration jobs completed successfully.
    pub completed: u64,
    /// Hydration jobs failed.
    pub failed: u64,
    /// Hydration jobs waiting for retry backoff.
    pub backoff: u64,
    /// Hydration jobs blocked by poison state.
    pub poisoned: u64,
    /// Total hydration attempts recorded.
    pub total_attempts: u64,
}

impl MountDaemonHydrationProgress {
    /// Creates count-only hydration progress for status reporting.
    #[must_use]
    pub const fn new(
        pending: u64,
        running: u64,
        completed: u64,
        failed: u64,
        backoff: u64,
        poisoned: u64,
        total_attempts: u64,
    ) -> Self {
        Self {
            pending,
            running,
            completed,
            failed,
            backoff,
            poisoned,
            total_attempts,
        }
    }
}

impl fmt::Debug for MountDaemonHydrationProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDaemonHydrationProgress")
            .field("pending", &self.pending)
            .field("running", &self.running)
            .field("completed", &self.completed)
            .field("failed", &self.failed)
            .field("backoff", &self.backoff)
            .field("poisoned", &self.poisoned)
            .field("total_attempts", &self.total_attempts)
            .finish()
    }
}

/// Redacted public daemon status.
#[derive(Clone, Eq, PartialEq)]
pub struct MountDaemonStatus {
    tag: MountDaemonTag,
    backend: MountDaemonBackend,
    state: MountDaemonState,
    paths: Option<MountDaemonPaths>,
    pid_present: bool,
    socket_present: bool,
    log_present: bool,
    uptime: Option<Duration>,
    hydration_progress: Option<MountDaemonHydrationProgress>,
    reason_code: Option<MountDaemonErrorCode>,
}

impl MountDaemonStatus {
    /// Creates redacted daemon status with no filesystem observations.
    #[must_use]
    pub const fn new(
        tag: MountDaemonTag,
        backend: MountDaemonBackend,
        state: MountDaemonState,
    ) -> Self {
        Self {
            tag,
            backend,
            state,
            paths: None,
            pid_present: false,
            socket_present: false,
            log_present: false,
            uptime: None,
            hydration_progress: None,
            reason_code: None,
        }
    }

    /// Attaches deterministic daemon paths to the status.
    ///
    /// # Errors
    ///
    /// Returns [`MountDaemonErrorCode::InvalidInput`] when the paths were
    /// derived for a different daemon tag.
    pub fn with_paths(mut self, paths: MountDaemonPaths) -> Result<Self, MountDaemonError> {
        if &self.tag != paths.tag() {
            return Err(MountDaemonError::new(MountDaemonErrorCode::InvalidInput));
        }

        self.paths = Some(paths);
        Ok(self)
    }

    /// Returns the redacted tag marker.
    #[must_use]
    pub const fn redacted_tag(&self) -> &'static str {
        REDACTED_MARKER
    }

    /// Returns the validated tag length without exposing the tag value.
    #[must_use]
    pub fn tag_len(&self) -> usize {
        self.tag.value.len()
    }

    /// Returns the daemon backend.
    #[must_use]
    pub const fn backend(&self) -> MountDaemonBackend {
        self.backend
    }

    /// Returns the daemon lifecycle state.
    #[must_use]
    pub const fn state(&self) -> MountDaemonState {
        self.state
    }

    /// Returns whether PID metadata is present.
    #[must_use]
    pub const fn pid_present(&self) -> bool {
        self.pid_present
    }

    /// Returns whether socket metadata is present.
    #[must_use]
    pub const fn socket_present(&self) -> bool {
        self.socket_present
    }

    /// Returns whether log metadata is present.
    #[must_use]
    pub const fn log_present(&self) -> bool {
        self.log_present
    }

    /// Returns redacted daemon uptime.
    #[must_use]
    pub const fn uptime(&self) -> Option<Duration> {
        self.uptime
    }

    /// Returns count-only hydration progress.
    #[must_use]
    pub const fn hydration_progress(&self) -> Option<MountDaemonHydrationProgress> {
        self.hydration_progress
    }

    /// Returns the fixed redacted status reason.
    #[must_use]
    pub const fn reason_code(&self) -> Option<MountDaemonErrorCode> {
        self.reason_code
    }

    /// Records whether PID metadata is present.
    #[must_use]
    pub const fn with_pid_present(mut self, pid_present: bool) -> Self {
        self.pid_present = pid_present;
        self
    }

    /// Records whether socket metadata is present.
    #[must_use]
    pub const fn with_socket_present(mut self, socket_present: bool) -> Self {
        self.socket_present = socket_present;
        self
    }

    /// Records whether log metadata is present.
    #[must_use]
    pub const fn with_log_present(mut self, log_present: bool) -> Self {
        self.log_present = log_present;
        self
    }

    /// Records redacted daemon uptime.
    #[must_use]
    pub const fn with_uptime(mut self, uptime: Duration) -> Self {
        self.uptime = Some(uptime);
        self
    }

    /// Records count-only hydration progress.
    #[must_use]
    pub const fn with_hydration_progress(
        mut self,
        hydration_progress: MountDaemonHydrationProgress,
    ) -> Self {
        self.hydration_progress = Some(hydration_progress);
        self
    }

    /// Records a fixed redacted status reason.
    #[must_use]
    pub const fn with_reason_code(mut self, reason_code: MountDaemonErrorCode) -> Self {
        self.reason_code = Some(reason_code);
        self
    }
}

impl fmt::Debug for MountDaemonStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDaemonStatus")
            .field("tag", &"<redacted>")
            .field("tag_len", &self.tag.value.len())
            .field("backend", &self.backend)
            .field("state", &self.state)
            .field("paths_present", &self.paths.is_some())
            .field("pid_present", &self.pid_present)
            .field("socket_present", &self.socket_present)
            .field("log_present", &self.log_present)
            .field(
                "uptime_secs",
                &self.uptime.map(|duration| duration.as_secs()),
            )
            .field("hydration_progress", &self.hydration_progress)
            .field("reason_code", &self.reason_code)
            .finish()
    }
}

impl fmt::Display for MountDaemonStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "mount daemon status: state={}, backend={}, tag=<redacted>",
            self.state, self.backend
        )
    }
}

/// Fixed redacted daemon error category.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountDaemonErrorCode {
    /// The daemon tag is invalid.
    InvalidTag,
    /// Input was invalid.
    InvalidInput,
    /// Local control-plane metadata is unavailable.
    Unavailable,
    /// Local I/O failed.
    Io,
    /// Local daemon IPC failed.
    Ipc,
}

impl MountDaemonErrorCode {
    const fn redacted_message(self) -> &'static str {
        match self {
            Self::InvalidTag => "invalid tag",
            Self::InvalidInput => "invalid input",
            Self::Unavailable => "unavailable",
            Self::Io => "io error",
            Self::Ipc => "ipc error",
        }
    }
}

impl fmt::Display for MountDaemonErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.redacted_message())
    }
}

/// Redacted mount daemon model error.
#[derive(Clone, Eq, PartialEq)]
pub struct MountDaemonError {
    code: MountDaemonErrorCode,
}

impl MountDaemonError {
    /// Creates a redacted daemon error from a fixed category.
    #[must_use]
    pub const fn new(code: MountDaemonErrorCode) -> Self {
        Self { code }
    }

    /// Returns the fixed error category.
    #[must_use]
    pub const fn code(&self) -> MountDaemonErrorCode {
        self.code
    }
}

impl fmt::Display for MountDaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "mount daemon operation failed: {}",
            self.code.redacted_message()
        )
    }
}

impl fmt::Debug for MountDaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDaemonError")
            .field("code", &self.code)
            .field("message", &self.to_string())
            .finish()
    }
}

impl std::error::Error for MountDaemonError {}

fn is_valid_tag(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TAG_LEN
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_runtime_root(runtime_root: &Path) -> Result<(), MountDaemonError> {
    if !runtime_root.is_absolute()
        || runtime_root.to_string_lossy().contains('\0')
        || runtime_root
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(MountDaemonError::new(MountDaemonErrorCode::InvalidInput));
    }

    Ok(())
}

fn validate_socket_path_len(socket_path: &Path) -> Result<(), MountDaemonError> {
    if socket_path.to_string_lossy().len() > MAX_SOCKET_PATH_LEN {
        return Err(MountDaemonError::new(MountDaemonErrorCode::InvalidInput));
    }

    Ok(())
}

fn file_name_len(path: &Path) -> Option<usize> {
    path.file_name()
        .map(|file_name| file_name.to_string_lossy().len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount_adapter::MountBackend;
    use std::path::PathBuf;

    #[test]
    fn mount_daemon_tag_rejects_path_traversal_and_nul() {
        for value in ["..", ".", "../repo", "repo/../other", "repo\0secret"] {
            let error = MountDaemonTag::new(value).unwrap_err();

            assert_eq!(error.code(), MountDaemonErrorCode::InvalidTag);
        }
    }

    #[test]
    fn mount_daemon_paths_are_deterministic_under_runtime_root() {
        let runtime_root = PathBuf::from("/tmp/stratum-runtime");
        let tag = MountDaemonTag::new("repo-main").unwrap();

        let paths = MountDaemonPaths::new(&runtime_root, tag.clone()).unwrap();

        assert_eq!(paths.pid_path(), runtime_root.join("repo-main.pid"));
        assert_eq!(paths.socket_path(), runtime_root.join("repo-main.sock"));
        assert_eq!(paths.log_path(), runtime_root.join("repo-main.log"));
    }

    #[test]
    fn mount_daemon_paths_reject_invalid_runtime_roots_and_long_socket_paths() {
        let tag = MountDaemonTag::new("repo-main").unwrap();
        for runtime_root in [
            PathBuf::from("relative-runtime"),
            PathBuf::from("/tmp/stratum\0runtime"),
            PathBuf::from("/tmp/../stratum-runtime"),
        ] {
            let error = MountDaemonPaths::new(&runtime_root, tag.clone()).unwrap_err();

            assert_eq!(error.code(), MountDaemonErrorCode::InvalidInput);
        }

        let long_root = PathBuf::from(format!("/tmp/{}", "x".repeat(100)));
        let error = MountDaemonPaths::new(&long_root, tag).unwrap_err();

        assert_eq!(error.code(), MountDaemonErrorCode::InvalidInput);
    }

    #[test]
    fn mount_daemon_backend_default_matches_mount_adapter_default() {
        assert_eq!(MountDaemonBackend::default(), MountBackend::default());
    }

    #[test]
    fn mount_daemon_status_debug_redacts_paths_and_identity() {
        let runtime_root = PathBuf::from("/tmp/stratum-runtime/user-secret");
        let tag = MountDaemonTag::new("private-repo").unwrap();
        let paths = MountDaemonPaths::new(&runtime_root, tag.clone()).unwrap();
        let status = MountDaemonStatus::new(
            tag,
            MountDaemonBackend::default(),
            MountDaemonState::Running,
        )
        .with_paths(paths)
        .unwrap()
        .with_pid_present(true)
        .with_socket_present(true)
        .with_log_present(true);

        let debug = format!("{status:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("private-repo"));
        assert!(!debug.contains("user-secret"));
        assert!(!debug.contains("/tmp/stratum-runtime"));
    }

    #[test]
    fn mount_daemon_status_debug_includes_hydration_counts_only() {
        let tag = MountDaemonTag::new("repo-secret").unwrap();
        let status = MountDaemonStatus::new(
            tag,
            MountDaemonBackend::default(),
            MountDaemonState::Running,
        )
        .with_hydration_progress(MountDaemonHydrationProgress::new(3, 2, 11, 5, 7, 13, 19));

        let debug = format!("{status:?}");

        assert!(debug.contains("hydration_progress"));
        assert!(debug.contains("pending"));
        assert!(debug.contains("running"));
        assert!(debug.contains("completed"));
        assert!(debug.contains("failed"));
        assert!(debug.contains("backoff"));
        assert!(debug.contains("poisoned"));
        assert!(debug.contains("total_attempts"));
        assert!(debug.contains("3"));
        assert!(debug.contains("19"));
        assert!(!debug.contains("repo-secret"));
        assert!(!debug.contains("object"));
        assert!(!debug.contains("/tmp"));
        assert!(!debug.contains("/"));
    }

    #[test]
    fn mount_daemon_status_accessors_are_redacted_safe() {
        let tag = MountDaemonTag::new("safe-repo").unwrap();
        let progress = MountDaemonHydrationProgress::new(1, 2, 3, 4, 5, 6, 7);
        let status = MountDaemonStatus::new(
            tag,
            MountDaemonBackend::default(),
            MountDaemonState::Running,
        )
        .with_pid_present(true)
        .with_socket_present(true)
        .with_log_present(true)
        .with_uptime(Duration::from_secs(42))
        .with_hydration_progress(progress)
        .with_reason_code(MountDaemonErrorCode::Unavailable);

        assert_eq!(status.redacted_tag(), "<redacted>");
        assert_eq!(status.tag_len(), "safe-repo".len());
        assert_eq!(status.backend(), MountDaemonBackend::default());
        assert_eq!(status.state(), MountDaemonState::Running);
        assert!(status.pid_present());
        assert!(status.socket_present());
        assert!(status.log_present());
        assert_eq!(status.uptime(), Some(Duration::from_secs(42)));
        assert_eq!(status.hydration_progress(), Some(progress));
        assert_eq!(
            status.reason_code(),
            Some(MountDaemonErrorCode::Unavailable)
        );
    }

    #[test]
    fn mount_daemon_status_rejects_mismatched_path_tags_without_leaking_tags() {
        let runtime_root = PathBuf::from("/tmp/stratum-runtime");
        let status_tag = MountDaemonTag::new("status-secret").unwrap();
        let paths_tag = MountDaemonTag::new("paths-secret").unwrap();
        let paths = MountDaemonPaths::new(&runtime_root, paths_tag).unwrap();

        let error = MountDaemonStatus::new(
            status_tag,
            MountDaemonBackend::default(),
            MountDaemonState::Running,
        )
        .with_paths(paths)
        .unwrap_err();

        let debug = format!("{error:?}");
        assert_eq!(error.code(), MountDaemonErrorCode::InvalidInput);
        assert!(!debug.contains("status-secret"));
        assert!(!debug.contains("paths-secret"));
    }

    #[test]
    fn mount_daemon_public_error_is_fixed_and_redacted() {
        let error = MountDaemonError::new(MountDaemonErrorCode::Unavailable);

        assert_eq!(
            error.to_string(),
            "mount daemon operation failed: unavailable"
        );
        assert_eq!(
            format!("{error:?}"),
            "MountDaemonError { code: Unavailable, message: \"mount daemon operation failed: unavailable\" }"
        );
    }
}
