//! Provider-free mount daemon status and path model.

use crate::mount_adapter::MountBackend;
use std::fmt;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
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

/// PID metadata read from the daemon control-plane PID file.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum MountDaemonPidMetadata {
    /// PID metadata is absent.
    Missing,
    /// PID metadata exists but is not a usable process identifier.
    Invalid,
    /// PID metadata contains a process identifier.
    Present(u32),
}

impl fmt::Debug for MountDaemonPidMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str("Missing"),
            Self::Invalid => formatter.write_str("Invalid"),
            Self::Present(_) => formatter
                .debug_struct("Present")
                .field("pid_present", &true)
                .finish(),
        }
    }
}

/// Redacted daemon status returned by the daemon IPC status request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountDaemonIpcStatus {
    /// Backend currently served by the daemon.
    pub backend: MountDaemonBackend,
    /// Lifecycle state reported by the daemon.
    pub state: MountDaemonState,
    /// Redacted daemon uptime.
    pub uptime: Option<Duration>,
    /// Count-only hydration progress.
    pub hydration_progress: Option<MountDaemonHydrationProgress>,
}

/// Injectable process liveness checks for daemon status resolution.
pub trait MountDaemonProcessProbe {
    /// Returns whether `pid` currently refers to a live process.
    ///
    /// Implementations must collapse platform details into fixed
    /// [`MountDaemonErrorCode`] categories.
    fn is_alive(&self, pid: u32) -> Result<bool, MountDaemonError>;
}

/// Injectable daemon metadata and log file access.
pub trait MountDaemonFileStore {
    /// Reads daemon PID metadata.
    fn read_pid(
        &self,
        paths: &MountDaemonPaths,
    ) -> Result<MountDaemonPidMetadata, MountDaemonError>;

    /// Returns whether daemon socket metadata exists.
    fn socket_exists(&self, paths: &MountDaemonPaths) -> Result<bool, MountDaemonError>;

    /// Returns whether daemon log metadata exists.
    fn log_exists(&self, paths: &MountDaemonPaths) -> Result<bool, MountDaemonError>;

    /// Removes stale daemon metadata files.
    fn remove_stale_files(&self, paths: &MountDaemonPaths) -> Result<(), MountDaemonError>;

    /// Reads the trailing daemon logs, redacted by callers before display.
    fn tail_logs(
        &self,
        paths: &MountDaemonPaths,
        max_bytes: usize,
    ) -> Result<String, MountDaemonError>;
}

/// Injectable daemon control-plane IPC requests.
pub trait MountDaemonIpcClient {
    /// Requests daemon status.
    fn status(&self, paths: &MountDaemonPaths) -> Result<MountDaemonIpcStatus, MountDaemonError>;

    /// Requests daemon logs through IPC.
    fn logs(&self, paths: &MountDaemonPaths, max_bytes: usize) -> Result<String, MountDaemonError>;

    /// Requests daemon unmount.
    fn unmount(&self, paths: &MountDaemonPaths) -> Result<(), MountDaemonError>;
}

/// Production process liveness probe.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemMountDaemonProcessProbe;

impl MountDaemonProcessProbe for SystemMountDaemonProcessProbe {
    fn is_alive(&self, pid: u32) -> Result<bool, MountDaemonError> {
        #[cfg(unix)]
        {
            unix_process_exists(pid)
        }

        #[cfg(not(unix))]
        {
            let _ = pid;
            Err(MountDaemonError::new(MountDaemonErrorCode::Unavailable))
        }
    }
}

/// Production daemon metadata file access.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemMountDaemonFileStore;

impl MountDaemonFileStore for SystemMountDaemonFileStore {
    fn read_pid(
        &self,
        paths: &MountDaemonPaths,
    ) -> Result<MountDaemonPidMetadata, MountDaemonError> {
        match fs::read_to_string(paths.pid_path()) {
            Ok(contents) => Ok(parse_pid_metadata(&contents)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(MountDaemonPidMetadata::Missing)
            }
            Err(_) => Err(MountDaemonError::new(MountDaemonErrorCode::Io)),
        }
    }

    fn socket_exists(&self, paths: &MountDaemonPaths) -> Result<bool, MountDaemonError> {
        path_exists(paths.socket_path())
    }

    fn log_exists(&self, paths: &MountDaemonPaths) -> Result<bool, MountDaemonError> {
        path_exists(paths.log_path())
    }

    fn remove_stale_files(&self, paths: &MountDaemonPaths) -> Result<(), MountDaemonError> {
        remove_file_if_exists(paths.pid_path())?;
        remove_file_if_exists(paths.socket_path())
    }

    fn tail_logs(
        &self,
        paths: &MountDaemonPaths,
        max_bytes: usize,
    ) -> Result<String, MountDaemonError> {
        if max_bytes == 0 {
            return Ok(String::new());
        }

        let mut file = fs::File::open(paths.log_path())
            .map_err(|_| MountDaemonError::new(MountDaemonErrorCode::Io))?;
        let file_len = file
            .metadata()
            .map_err(|_| MountDaemonError::new(MountDaemonErrorCode::Io))?
            .len();
        let max_bytes = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let read_len = file_len.min(max_bytes);
        let start = file_len.saturating_sub(read_len);
        file.seek(SeekFrom::Start(start))
            .map_err(|_| MountDaemonError::new(MountDaemonErrorCode::Io))?;

        let capacity = usize::try_from(read_len)
            .map_err(|_| MountDaemonError::new(MountDaemonErrorCode::Io))?;
        let mut bytes = Vec::with_capacity(capacity);
        file.take(read_len)
            .read_to_end(&mut bytes)
            .map_err(|_| MountDaemonError::new(MountDaemonErrorCode::Io))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

/// Placeholder production IPC client for provider-free lifecycle modeling.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableMountDaemonIpcClient;

impl MountDaemonIpcClient for UnavailableMountDaemonIpcClient {
    fn status(&self, _paths: &MountDaemonPaths) -> Result<MountDaemonIpcStatus, MountDaemonError> {
        Err(MountDaemonError::new(MountDaemonErrorCode::Ipc))
    }

    fn logs(
        &self,
        _paths: &MountDaemonPaths,
        _max_bytes: usize,
    ) -> Result<String, MountDaemonError> {
        Err(MountDaemonError::new(MountDaemonErrorCode::Ipc))
    }

    fn unmount(&self, _paths: &MountDaemonPaths) -> Result<(), MountDaemonError> {
        Err(MountDaemonError::new(MountDaemonErrorCode::Ipc))
    }
}

/// Coordinates daemon lifecycle probes without depending on mount providers.
#[derive(Clone)]
pub struct MountDaemonController<P, F, I> {
    process_probe: P,
    file_store: F,
    ipc_client: I,
}

impl<P, F, I> fmt::Debug for MountDaemonController<P, F, I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MountDaemonController")
            .field("process_probe_present", &true)
            .field("file_store_present", &true)
            .field("ipc_client_present", &true)
            .finish()
    }
}

impl<P, F, I> MountDaemonController<P, F, I> {
    /// Creates a controller from injectable daemon status effects.
    #[must_use]
    pub const fn new(process_probe: P, file_store: F, ipc_client: I) -> Self {
        Self {
            process_probe,
            file_store,
            ipc_client,
        }
    }
}

impl<P, F, I> MountDaemonController<P, F, I>
where
    P: MountDaemonProcessProbe,
    F: MountDaemonFileStore,
    I: MountDaemonIpcClient,
{
    /// Resolves daemon lifecycle status through provider-free local probes.
    pub fn resolve_status(
        &self,
        tag: MountDaemonTag,
        runtime_root: impl AsRef<Path>,
    ) -> Result<MountDaemonStatus, MountDaemonError> {
        resolve_status(tag, runtime_root, self)
    }
}

/// Resolves daemon lifecycle status through provider-free local probes.
pub fn resolve_status<P, F, I>(
    tag: MountDaemonTag,
    runtime_root: impl AsRef<Path>,
    controller: &MountDaemonController<P, F, I>,
) -> Result<MountDaemonStatus, MountDaemonError>
where
    P: MountDaemonProcessProbe,
    F: MountDaemonFileStore,
    I: MountDaemonIpcClient,
{
    let paths = MountDaemonPaths::new(runtime_root, tag.clone())?;
    let pid_metadata = match controller.file_store.read_pid(&paths) {
        Ok(pid_metadata) => pid_metadata,
        Err(error) => {
            return status_with_observations(
                tag,
                paths,
                MountDaemonState::Unavailable,
                false,
                false,
                false,
                MountDaemonBackend::default(),
            )
            .map(|status| status.with_reason_code(error.code()));
        }
    };
    let socket_present = match controller.file_store.socket_exists(&paths) {
        Ok(socket_present) => socket_present,
        Err(error) => {
            return status_with_observations(
                tag,
                paths,
                MountDaemonState::Unavailable,
                matches!(
                    pid_metadata,
                    MountDaemonPidMetadata::Invalid | MountDaemonPidMetadata::Present(_)
                ),
                false,
                false,
                MountDaemonBackend::default(),
            )
            .map(|status| status.with_reason_code(error.code()));
        }
    };
    let log_present = match controller.file_store.log_exists(&paths) {
        Ok(log_present) => log_present,
        Err(error) => {
            return status_with_observations(
                tag,
                paths,
                MountDaemonState::Unavailable,
                matches!(
                    pid_metadata,
                    MountDaemonPidMetadata::Invalid | MountDaemonPidMetadata::Present(_)
                ),
                socket_present,
                false,
                MountDaemonBackend::default(),
            )
            .map(|status| status.with_reason_code(error.code()));
        }
    };

    match pid_metadata {
        MountDaemonPidMetadata::Missing if !socket_present => status_with_observations(
            tag,
            paths,
            MountDaemonState::Stopped,
            false,
            socket_present,
            log_present,
            MountDaemonBackend::default(),
        ),
        MountDaemonPidMetadata::Missing | MountDaemonPidMetadata::Invalid => stale_status(
            tag,
            paths,
            matches!(pid_metadata, MountDaemonPidMetadata::Invalid),
            socket_present,
            log_present,
        ),
        MountDaemonPidMetadata::Present(pid) => {
            let alive = match controller.process_probe.is_alive(pid) {
                Ok(alive) => alive,
                Err(error) => {
                    return status_with_observations(
                        tag,
                        paths,
                        MountDaemonState::Unavailable,
                        true,
                        socket_present,
                        log_present,
                        MountDaemonBackend::default(),
                    )
                    .map(|status| status.with_reason_code(error.code()));
                }
            };

            if !alive {
                return stale_status(tag, paths, true, socket_present, log_present);
            }

            if !socket_present {
                return status_with_observations(
                    tag,
                    paths,
                    MountDaemonState::Crashed,
                    true,
                    false,
                    log_present,
                    MountDaemonBackend::default(),
                )
                .map(|status| status.with_reason_code(MountDaemonErrorCode::Unavailable));
            }

            match controller.ipc_client.status(&paths) {
                Ok(ipc_status) => {
                    status_from_ipc(tag, paths, socket_present, log_present, ipc_status)
                }
                Err(error) => status_with_observations(
                    tag,
                    paths,
                    MountDaemonState::Crashed,
                    true,
                    socket_present,
                    log_present,
                    MountDaemonBackend::default(),
                )
                .map(|status| status.with_reason_code(error.code())),
            }
        }
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

fn status_with_observations(
    tag: MountDaemonTag,
    paths: MountDaemonPaths,
    state: MountDaemonState,
    pid_present: bool,
    socket_present: bool,
    log_present: bool,
    backend: MountDaemonBackend,
) -> Result<MountDaemonStatus, MountDaemonError> {
    let status = MountDaemonStatus::new(tag, backend, state)
        .with_paths(paths)?
        .with_pid_present(pid_present)
        .with_socket_present(socket_present)
        .with_log_present(log_present);

    Ok(status)
}

fn stale_status(
    tag: MountDaemonTag,
    paths: MountDaemonPaths,
    pid_present: bool,
    socket_present: bool,
    log_present: bool,
) -> Result<MountDaemonStatus, MountDaemonError> {
    status_with_observations(
        tag,
        paths,
        MountDaemonState::StalePid,
        pid_present,
        socket_present,
        log_present,
        MountDaemonBackend::default(),
    )
    .map(|status| status.with_reason_code(MountDaemonErrorCode::Unavailable))
}

fn status_from_ipc(
    tag: MountDaemonTag,
    paths: MountDaemonPaths,
    socket_present: bool,
    log_present: bool,
    ipc_status: MountDaemonIpcStatus,
) -> Result<MountDaemonStatus, MountDaemonError> {
    let mut status = status_with_observations(
        tag,
        paths,
        ipc_status.state,
        true,
        socket_present,
        log_present,
        ipc_status.backend,
    )?;

    if let Some(uptime) = ipc_status.uptime {
        status = status.with_uptime(uptime);
    }
    if let Some(hydration_progress) = ipc_status.hydration_progress {
        status = status.with_hydration_progress(hydration_progress);
    }

    Ok(status)
}

fn parse_pid_metadata(contents: &str) -> MountDaemonPidMetadata {
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return MountDaemonPidMetadata::Invalid;
    }

    match trimmed.parse::<u32>() {
        Ok(0) | Err(_) => MountDaemonPidMetadata::Invalid,
        Ok(pid) => MountDaemonPidMetadata::Present(pid),
    }
}

fn path_exists(path: &Path) -> Result<bool, MountDaemonError> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(MountDaemonError::new(MountDaemonErrorCode::Io)),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<(), MountDaemonError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(MountDaemonError::new(MountDaemonErrorCode::Io)),
    }
}

#[cfg(unix)]
fn unix_process_exists(pid: u32) -> Result<bool, MountDaemonError> {
    use std::os::raw::c_int;

    const EPERM: i32 = 1;
    const ESRCH: i32 = 3;

    unsafe extern "C" {
        fn kill(pid: c_int, sig: c_int) -> c_int;
    }

    let pid = c_int::try_from(pid)
        .map_err(|_| MountDaemonError::new(MountDaemonErrorCode::Unavailable))?;

    // SAFETY: `kill(pid, 0)` performs a POSIX liveness/permission probe only;
    // it does not deliver a signal. `pid` is range-checked for `c_int` above.
    let result = unsafe { kill(pid, 0) };
    if result == 0 {
        return Ok(true);
    }

    match io::Error::last_os_error().raw_os_error() {
        Some(EPERM) => Ok(true),
        Some(ESRCH) => Ok(false),
        _ => Err(MountDaemonError::new(MountDaemonErrorCode::Unavailable)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount_adapter::MountBackend;
    use std::cell::Cell;
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

    #[test]
    fn status_reports_stopped_when_pid_and_socket_are_absent() {
        let controller = fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Missing),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::Stopped);
        assert!(!status.pid_present());
        assert!(!status.socket_present());
        assert!(!status.log_present());
        assert_eq!(status.reason_code(), None);
    }

    #[test]
    fn status_reports_stale_pid_when_pid_is_dead_and_socket_exists() {
        let controller = fake_controller(
            FakeProcessProbe {
                alive: Ok(false),
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Present(42)),
                socket_exists: Ok(true),
                log_exists: Ok(true),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::StalePid);
        assert!(status.pid_present());
        assert!(status.socket_present());
        assert!(status.log_present());
        assert_eq!(
            status.reason_code(),
            Some(MountDaemonErrorCode::Unavailable)
        );
    }

    #[test]
    fn status_reports_crashed_when_pid_is_alive_but_socket_is_unreachable() {
        let controller = fake_controller(
            FakeProcessProbe {
                alive: Ok(true),
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Present(42)),
                socket_exists: Ok(true),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient {
                status: Err(MountDaemonErrorCode::Ipc),
                ..FakeIpcClient::default()
            },
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::Crashed);
        assert!(status.pid_present());
        assert!(status.socket_present());
        assert!(!status.log_present());
        assert_eq!(status.reason_code(), Some(MountDaemonErrorCode::Ipc));
    }

    #[test]
    fn status_reports_crashed_when_pid_is_alive_but_socket_is_absent() {
        let controller = fake_controller(
            FakeProcessProbe {
                alive: Ok(true),
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Present(42)),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::Crashed);
        assert!(status.pid_present());
        assert!(!status.socket_present());
        assert_eq!(
            status.reason_code(),
            Some(MountDaemonErrorCode::Unavailable)
        );
    }

    #[test]
    fn status_reports_stale_pid_when_pid_file_is_invalid() {
        let controller = fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Invalid),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::StalePid);
        assert!(status.pid_present());
        assert_eq!(
            status.reason_code(),
            Some(MountDaemonErrorCode::Unavailable)
        );
    }

    #[test]
    fn status_reports_stale_pid_when_pid_is_missing_but_socket_exists() {
        let controller = fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Missing),
                socket_exists: Ok(true),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::StalePid);
        assert!(!status.pid_present());
        assert!(status.socket_present());
        assert_eq!(
            status.reason_code(),
            Some(MountDaemonErrorCode::Unavailable)
        );
    }

    #[test]
    fn status_reports_running_from_ipc_status_response() {
        let progress = MountDaemonHydrationProgress::new(1, 2, 3, 4, 5, 6, 7);
        let controller = fake_controller(
            FakeProcessProbe {
                alive: Ok(true),
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Present(42)),
                socket_exists: Ok(true),
                log_exists: Ok(true),
                ..FakeFileStore::default()
            },
            FakeIpcClient {
                status: Ok(MountDaemonIpcStatus {
                    backend: MountDaemonBackend::Nfs,
                    state: MountDaemonState::Running,
                    uptime: Some(Duration::from_secs(99)),
                    hydration_progress: Some(progress),
                }),
                ..FakeIpcClient::default()
            },
        );

        let status = controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(status.state(), MountDaemonState::Running);
        assert_eq!(status.backend(), MountDaemonBackend::Nfs);
        assert_eq!(status.uptime(), Some(Duration::from_secs(99)));
        assert_eq!(status.hydration_progress(), Some(progress));
        assert_eq!(status.reason_code(), None);
    }

    #[test]
    fn status_collapses_io_and_ipc_failures_to_redacted_reason_codes() {
        let io_controller = fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore {
                pid: Err(MountDaemonErrorCode::Io),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );
        let ipc_controller = fake_controller(
            FakeProcessProbe {
                alive: Ok(true),
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Present(42)),
                socket_exists: Ok(true),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient {
                status: Err(MountDaemonErrorCode::Ipc),
                ..FakeIpcClient::default()
            },
        );
        let process_controller = fake_controller(
            FakeProcessProbe {
                alive: Err(MountDaemonErrorCode::Unavailable),
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Present(42)),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                ..FakeFileStore::default()
            },
            FakeIpcClient::default(),
        );

        let io_status = io_controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();
        let ipc_status = ipc_controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();
        let process_status = process_controller
            .resolve_status(test_tag(), test_runtime_root())
            .unwrap();

        assert_eq!(io_status.state(), MountDaemonState::Unavailable);
        assert_eq!(io_status.reason_code(), Some(MountDaemonErrorCode::Io));
        assert_eq!(ipc_status.state(), MountDaemonState::Crashed);
        assert_eq!(ipc_status.reason_code(), Some(MountDaemonErrorCode::Ipc));
        assert_eq!(process_status.state(), MountDaemonState::Unavailable);
        assert_eq!(
            process_status.reason_code(),
            Some(MountDaemonErrorCode::Unavailable)
        );
    }

    #[test]
    fn mount_daemon_pid_metadata_debug_redacts_raw_pid() {
        let debug = format!("{:?}", MountDaemonPidMetadata::Present(12345));

        assert!(debug.contains("pid_present"));
        assert!(!debug.contains("12345"));
    }

    #[test]
    fn mount_daemon_controller_debug_redacts_injected_effect_fields() {
        let controller = fake_controller(
            FakeProcessProbe {
                _secret: "process-secret",
                ..FakeProcessProbe::default()
            },
            FakeFileStore {
                _secret: "file-secret",
                ..FakeFileStore::default()
            },
            FakeIpcClient {
                _secret: "ipc-secret",
                ..FakeIpcClient::default()
            },
        );

        let debug = format!("{controller:?}");

        assert!(debug.contains("MountDaemonController"));
        assert!(!debug.contains("process-secret"));
        assert!(!debug.contains("file-secret"));
        assert!(!debug.contains("ipc-secret"));
    }

    fn fake_controller(
        process_probe: FakeProcessProbe,
        file_store: FakeFileStore,
        ipc_client: FakeIpcClient,
    ) -> MountDaemonController<FakeProcessProbe, FakeFileStore, FakeIpcClient> {
        MountDaemonController::new(process_probe, file_store, ipc_client)
    }

    fn test_tag() -> MountDaemonTag {
        MountDaemonTag::new("repo-main").unwrap()
    }

    fn test_runtime_root() -> PathBuf {
        PathBuf::from("/tmp/stratum-runtime")
    }

    #[derive(Clone)]
    struct FakeProcessProbe {
        alive: Result<bool, MountDaemonErrorCode>,
        _secret: &'static str,
    }

    impl Default for FakeProcessProbe {
        fn default() -> Self {
            Self {
                alive: Ok(false),
                _secret: "",
            }
        }
    }

    impl MountDaemonProcessProbe for FakeProcessProbe {
        fn is_alive(&self, _pid: u32) -> Result<bool, MountDaemonError> {
            self.alive.map_err(MountDaemonError::new)
        }
    }

    #[derive(Clone)]
    struct FakeFileStore {
        pid: Result<MountDaemonPidMetadata, MountDaemonErrorCode>,
        socket_exists: Result<bool, MountDaemonErrorCode>,
        log_exists: Result<bool, MountDaemonErrorCode>,
        removed_stale_files: Cell<u8>,
        _secret: &'static str,
    }

    impl Default for FakeFileStore {
        fn default() -> Self {
            Self {
                pid: Ok(MountDaemonPidMetadata::Missing),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                removed_stale_files: Cell::new(0),
                _secret: "",
            }
        }
    }

    impl MountDaemonFileStore for FakeFileStore {
        fn read_pid(
            &self,
            _paths: &MountDaemonPaths,
        ) -> Result<MountDaemonPidMetadata, MountDaemonError> {
            self.pid.map_err(MountDaemonError::new)
        }

        fn socket_exists(&self, _paths: &MountDaemonPaths) -> Result<bool, MountDaemonError> {
            self.socket_exists.map_err(MountDaemonError::new)
        }

        fn log_exists(&self, _paths: &MountDaemonPaths) -> Result<bool, MountDaemonError> {
            self.log_exists.map_err(MountDaemonError::new)
        }

        fn remove_stale_files(&self, _paths: &MountDaemonPaths) -> Result<(), MountDaemonError> {
            self.removed_stale_files
                .set(self.removed_stale_files.get() + 1);
            Ok(())
        }

        fn tail_logs(
            &self,
            _paths: &MountDaemonPaths,
            _max_bytes: usize,
        ) -> Result<String, MountDaemonError> {
            Ok(String::new())
        }
    }

    #[derive(Clone)]
    struct FakeIpcClient {
        status: Result<MountDaemonIpcStatus, MountDaemonErrorCode>,
        _secret: &'static str,
    }

    impl Default for FakeIpcClient {
        fn default() -> Self {
            Self {
                status: Err(MountDaemonErrorCode::Ipc),
                _secret: "",
            }
        }
    }

    impl MountDaemonIpcClient for FakeIpcClient {
        fn status(
            &self,
            _paths: &MountDaemonPaths,
        ) -> Result<MountDaemonIpcStatus, MountDaemonError> {
            self.status.clone().map_err(MountDaemonError::new)
        }

        fn logs(
            &self,
            _paths: &MountDaemonPaths,
            _max_bytes: usize,
        ) -> Result<String, MountDaemonError> {
            Ok(String::new())
        }

        fn unmount(&self, _paths: &MountDaemonPaths) -> Result<(), MountDaemonError> {
            Ok(())
        }
    }
}
