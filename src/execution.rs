use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

use crate::runs::{RunRecordLayout, RunStatus};

const PROCESS_TERMINATION_GRACE: Duration = Duration::from_millis(100);
const SAFE_COMMAND_PATH: &str = "/usr/bin:/bin:/usr/sbin:/sbin";

#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(unix)]
const SIGTERM: i32 = 15;

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

#[derive(Clone)]
pub struct ExecutionJobTable {
    jobs: Arc<RwLock<HashMap<Uuid, Arc<ExecutionJobEntry>>>>,
    max_jobs_per_workspace: usize,
}

impl ExecutionJobTable {
    pub fn new() -> Self {
        Self::with_max_jobs(256)
    }

    pub fn with_max_jobs(max_jobs_per_workspace: usize) -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            max_jobs_per_workspace: max_jobs_per_workspace.max(1),
        }
    }

    pub async fn submit(
        &self,
        workspace_id: Uuid,
        request: ExecutionSubmitRequest,
    ) -> Result<ExecutionJobSnapshot, ExecutionJobError> {
        let now = Utc::now();
        let job_id = Uuid::new_v4();
        let run_id = request.run_id.clone();
        let entry = Arc::new(ExecutionJobEntry {
            workspace_id,
            job_id,
            run_id,
            state: RwLock::new(ExecutionJobState {
                status: ExecutionJobStatus::Queued,
                created_at: now,
                started_at: None,
                ended_at: None,
                exit_code: None,
                stdout: String::new(),
                stderr: String::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            }),
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        });

        self.insert_entry_enforcing_workspace_job_limit(workspace_id, Arc::clone(&entry))
            .await?;
        tokio::spawn(run_job(Arc::clone(&entry), request));

        entry.snapshot().await
    }

    pub async fn list(&self, workspace_id: Uuid) -> Vec<ExecutionJobSummary> {
        let entries = self.workspace_entries(workspace_id).await;
        let mut summaries = Vec::with_capacity(entries.len());
        for entry in entries {
            summaries.push(entry.public_summary().await);
        }
        summaries.sort_by_key(|summary| summary.created_at);
        summaries
    }

    pub async fn get(
        &self,
        workspace_id: Uuid,
        job_id: Uuid,
    ) -> Result<Option<ExecutionJobSnapshot>, ExecutionJobError> {
        let Some(entry) = self.get_owned(workspace_id, job_id).await else {
            return Ok(None);
        };
        entry.snapshot().await.map(Some)
    }

    pub async fn wait(
        &self,
        workspace_id: Uuid,
        job_id: Uuid,
        request: ExecutionWaitRequest,
    ) -> Result<Option<ExecutionJobSnapshot>, ExecutionJobError> {
        let Some(entry) = self.get_owned(workspace_id, job_id).await else {
            return Ok(None);
        };

        let deadline = tokio::time::sleep(request.timeout);
        tokio::pin!(deadline);

        loop {
            if entry.is_terminal().await {
                return entry.snapshot().await.map(Some);
            }

            tokio::select! {
                () = entry.notify.notified() => {}
                () = &mut deadline => return entry.snapshot().await.map(Some),
            }
        }
    }

    pub async fn cancel(
        &self,
        workspace_id: Uuid,
        job_id: Uuid,
    ) -> Result<Option<ExecutionJobSnapshot>, ExecutionJobError> {
        let Some(entry) = self.get_owned(workspace_id, job_id).await else {
            return Ok(None);
        };

        entry.cancelled.store(true, Ordering::SeqCst);
        entry.notify.notify_waiters();
        entry.snapshot().await.map(Some)
    }

    async fn get_owned(&self, workspace_id: Uuid, job_id: Uuid) -> Option<Arc<ExecutionJobEntry>> {
        let entry = self.jobs.read().await.get(&job_id).cloned()?;
        (entry.workspace_id == workspace_id).then_some(entry)
    }

    async fn workspace_entries(&self, workspace_id: Uuid) -> Vec<Arc<ExecutionJobEntry>> {
        self.jobs
            .read()
            .await
            .values()
            .filter(|entry| entry.workspace_id == workspace_id)
            .cloned()
            .collect()
    }

    async fn insert_entry_enforcing_workspace_job_limit(
        &self,
        workspace_id: Uuid,
        entry: Arc<ExecutionJobEntry>,
    ) -> Result<(), ExecutionJobError> {
        let mut terminal_jobs = Vec::new();
        let mut remaining = 0_usize;
        let mut jobs = self.jobs.write().await;
        for entry in jobs.values() {
            if entry.workspace_id != workspace_id {
                continue;
            }

            remaining += 1;
            let state = entry.state.read().await;
            if state.status.is_terminal() {
                terminal_jobs.push((state.created_at, entry.job_id));
            }
        }
        terminal_jobs.sort_by_key(|(created_at, _)| *created_at);

        for (_, job_id) in terminal_jobs {
            if remaining < self.max_jobs_per_workspace {
                break;
            }
            if jobs.remove(&job_id).is_some() {
                remaining = remaining.saturating_sub(1);
            }
        }

        if remaining < self.max_jobs_per_workspace {
            jobs.insert(entry.job_id, entry);
            Ok(())
        } else {
            Err(ExecutionJobError::MaxJobsExceeded)
        }
    }
}

impl Default for ExecutionJobTable {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ExecutionJobTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionJobTable")
            .field("scope", &"process-local")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ExecutionSubmitRequest {
    pub run_id: String,
    pub command: String,
    pub timeout: Duration,
    pub output_max_bytes: usize,
}

impl fmt::Debug for ExecutionSubmitRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionSubmitRequest")
            .field("run_id", &self.run_id)
            .field("command", &"<redacted>")
            .field("timeout", &self.timeout)
            .field("output_max_bytes", &self.output_max_bytes)
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ExecutionWaitRequest {
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl ExecutionJobStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

impl From<ExecutionJobStatus> for RunStatus {
    fn from(status: ExecutionJobStatus) -> Self {
        match status {
            ExecutionJobStatus::Queued => Self::Queued,
            ExecutionJobStatus::Running => Self::Running,
            ExecutionJobStatus::Succeeded => Self::Succeeded,
            ExecutionJobStatus::Failed => Self::Failed,
            ExecutionJobStatus::Cancelled => Self::Cancelled,
            ExecutionJobStatus::TimedOut => Self::TimedOut,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ExecutionJobSnapshot {
    pub workspace_id: Uuid,
    pub run_id: String,
    pub job_id: Uuid,
    pub status: ExecutionJobStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl ExecutionJobSnapshot {
    pub fn public_summary(&self) -> ExecutionJobSummary {
        ExecutionJobSummary {
            workspace_id: self.workspace_id,
            run_id: self.run_id.clone(),
            job_id: self.job_id,
            status: self.status,
            run_paths: ExecutionRunPaths::for_run_id(&self.run_id),
            created_at: self.created_at,
            started_at: self.started_at,
            ended_at: self.ended_at,
            exit_code: self.exit_code,
            stdout_truncated: self.stdout_truncated,
            stderr_truncated: self.stderr_truncated,
        }
    }
}

impl fmt::Debug for ExecutionJobSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionJobSnapshot")
            .field("workspace_id", &self.workspace_id)
            .field("run_id", &self.run_id)
            .field("job_id", &self.job_id)
            .field("status", &self.status)
            .field("created_at", &self.created_at)
            .field("started_at", &self.started_at)
            .field("ended_at", &self.ended_at)
            .field("exit_code", &self.exit_code)
            .field("stdout_truncated", &self.stdout_truncated)
            .field("stderr_truncated", &self.stderr_truncated)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionJobSummary {
    pub workspace_id: Uuid,
    pub run_id: String,
    pub job_id: Uuid,
    pub status: ExecutionJobStatus,
    pub run_paths: ExecutionRunPaths,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRunPaths {
    pub root: String,
    pub prompt: String,
    pub command: String,
    pub stdout: String,
    pub stderr: String,
    pub result: String,
    pub metadata: String,
    pub artifacts: String,
}

impl ExecutionRunPaths {
    fn for_run_id(run_id: &str) -> Self {
        let layout = RunRecordLayout::new(run_id)
            .expect("execution run ids are generated as valid UUID strings");
        Self {
            root: layout.root,
            prompt: layout.prompt,
            command: layout.command,
            stdout: layout.stdout,
            stderr: layout.stderr,
            result: layout.result,
            metadata: layout.metadata,
            artifacts: layout.artifacts,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionJobError {
    SpawnFailed,
    OutputReadFailed,
    MaxJobsExceeded,
    Internal,
}

impl fmt::Display for ExecutionJobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::SpawnFailed => "execution job spawn failed",
            Self::OutputReadFailed => "execution job output read failed",
            Self::MaxJobsExceeded => "execution job limit exceeded",
            Self::Internal => "execution job internal failure",
        };
        f.write_str(message)
    }
}

impl std::error::Error for ExecutionJobError {}

struct ExecutionJobEntry {
    workspace_id: Uuid,
    run_id: String,
    job_id: Uuid,
    state: RwLock<ExecutionJobState>,
    cancelled: AtomicBool,
    notify: Notify,
}

impl ExecutionJobEntry {
    async fn snapshot(&self) -> Result<ExecutionJobSnapshot, ExecutionJobError> {
        let state = self.state.read().await;
        Ok(ExecutionJobSnapshot {
            workspace_id: self.workspace_id,
            run_id: self.run_id.clone(),
            job_id: self.job_id,
            status: state.status,
            created_at: state.created_at,
            started_at: state.started_at,
            ended_at: state.ended_at,
            exit_code: state.exit_code,
            stdout: state.stdout.clone(),
            stderr: state.stderr.clone(),
            stdout_truncated: state.stdout_truncated,
            stderr_truncated: state.stderr_truncated,
        })
    }

    async fn public_summary(&self) -> ExecutionJobSummary {
        self.snapshot()
            .await
            .expect("snapshot reads in-memory state")
            .public_summary()
    }

    async fn is_terminal(&self) -> bool {
        self.state.read().await.status.is_terminal()
    }

    async fn mark_started(&self) {
        let mut state = self.state.write().await;
        if !state.status.is_terminal() {
            state.status = ExecutionJobStatus::Running;
            state.started_at = Some(Utc::now());
        }
        self.notify.notify_waiters();
    }

    async fn finish(&self, update: ExecutionJobFinish) {
        let mut state = self.state.write().await;
        state.status = update.status;
        state.ended_at = Some(Utc::now());
        state.exit_code = update.exit_code;
        state.stdout = update.stdout;
        state.stderr = update.stderr;
        state.stdout_truncated = update.stdout_truncated;
        state.stderr_truncated = update.stderr_truncated;
        self.notify.notify_waiters();
    }
}

struct ExecutionJobState {
    status: ExecutionJobStatus,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

struct ExecutionJobFinish {
    status: ExecutionJobStatus,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

async fn run_job(entry: Arc<ExecutionJobEntry>, request: ExecutionSubmitRequest) {
    if entry.cancelled.load(Ordering::SeqCst) {
        entry
            .finish(ExecutionJobFinish::empty(ExecutionJobStatus::Cancelled))
            .await;
        return;
    }

    let temp_dir = match create_temp_working_dir(entry.job_id) {
        Ok(path) => path,
        Err(_) => {
            entry
                .finish(ExecutionJobFinish::empty(ExecutionJobStatus::Failed))
                .await;
            return;
        }
    };

    entry.mark_started().await;

    let mut child = match shell_command(&request.command, &temp_dir).spawn() {
        Ok(child) => child,
        Err(_) => {
            cleanup_temp_dir(temp_dir).await;
            entry
                .finish(ExecutionJobFinish::empty(ExecutionJobStatus::Failed))
                .await;
            return;
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = tokio::spawn(capped_read(stdout, request.output_max_bytes));
    let stderr_task = tokio::spawn(capped_read(stderr, request.output_max_bytes));

    let status = tokio::select! {
        status = child.wait() => status.ok().map(ProcessOutcome::Exit),
        () = tokio::time::sleep(request.timeout) => {
            terminate_child_process_tree(&mut child).await;
            Some(ProcessOutcome::TimedOut)
        }
        () = wait_for_cancel(Arc::clone(&entry)) => {
            terminate_child_process_tree(&mut child).await;
            Some(ProcessOutcome::Cancelled)
        }
    };

    let killed = matches!(
        status,
        Some(ProcessOutcome::Cancelled | ProcessOutcome::TimedOut)
    );
    let (stdout_result, stderr_result) = if killed {
        let (stdout, stderr) = tokio::join!(
            join_capped_reader_after_termination(stdout_task),
            join_capped_reader_after_termination(stderr_task)
        );
        (Ok(stdout), Ok(stderr))
    } else {
        (
            join_capped_reader(stdout_task).await,
            join_capped_reader(stderr_task).await,
        )
    };
    cleanup_temp_dir(temp_dir).await;

    let finish = match (status, stdout_result, stderr_result) {
        (Some(outcome), Ok(stdout), Ok(stderr)) => finish_from_outcome(outcome, stdout, stderr),
        _ => ExecutionJobFinish::empty(ExecutionJobStatus::Failed),
    };

    entry.finish(finish).await;
}

async fn terminate_child_process_tree(child: &mut Child) {
    #[cfg(unix)]
    {
        signal_child_process_group(child, SIGTERM);
        if tokio::time::timeout(PROCESS_TERMINATION_GRACE, child.wait())
            .await
            .is_ok()
        {
            return;
        }

        signal_child_process_group(child, SIGKILL);
        let _ = child.start_kill();
        let _ = tokio::time::timeout(PROCESS_TERMINATION_GRACE, child.wait()).await;
    }

    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(PROCESS_TERMINATION_GRACE, child.wait()).await;
    }
}

#[cfg(unix)]
fn signal_child_process_group(child: &Child, signal: i32) {
    let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        return;
    };

    unsafe {
        let _ = kill(-pid, signal);
    }
}

async fn wait_for_cancel(entry: Arc<ExecutionJobEntry>) {
    loop {
        let notified = entry.notify.notified();
        if entry.cancelled.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

enum ProcessOutcome {
    Exit(std::process::ExitStatus),
    Cancelled,
    TimedOut,
}

fn finish_from_outcome(
    outcome: ProcessOutcome,
    stdout: CappedOutput,
    stderr: CappedOutput,
) -> ExecutionJobFinish {
    let (status, exit_code) = match outcome {
        ProcessOutcome::Exit(exit_status) => {
            let code = exit_status.code();
            if code == Some(0) {
                (ExecutionJobStatus::Succeeded, code)
            } else {
                (ExecutionJobStatus::Failed, code)
            }
        }
        ProcessOutcome::Cancelled => (ExecutionJobStatus::Cancelled, None),
        ProcessOutcome::TimedOut => (ExecutionJobStatus::TimedOut, None),
    };

    ExecutionJobFinish {
        status,
        exit_code,
        stdout: stdout.output,
        stderr: stderr.output,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    }
}

impl ExecutionJobFinish {
    fn empty(status: ExecutionJobStatus) -> Self {
        Self {
            status,
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }
}

struct CappedOutput {
    output: String,
    truncated: bool,
}

async fn capped_read<R>(
    reader: Option<R>,
    max_bytes: usize,
) -> Result<CappedOutput, ExecutionJobError>
where
    R: AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return Ok(CappedOutput {
            output: String::new(),
            truncated: false,
        });
    };

    let mut retained = Vec::with_capacity(max_bytes.min(8192));
    let mut truncated = false;
    let mut buffer = [0_u8; 8192];

    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| ExecutionJobError::OutputReadFailed)?;
        if read == 0 {
            break;
        }

        let available = max_bytes.saturating_sub(retained.len());
        if available > 0 {
            let keep = available.min(read);
            retained.extend_from_slice(&buffer[..keep]);
            truncated |= read > keep;
        } else {
            truncated = true;
        }
    }

    let (output, output_truncated) = string_from_lossy_utf8_with_byte_cap(&retained, max_bytes);
    Ok(CappedOutput {
        output,
        truncated: truncated || output_truncated,
    })
}

fn string_from_lossy_utf8_with_byte_cap(bytes: &[u8], max_bytes: usize) -> (String, bool) {
    let mut output = String::from_utf8_lossy(bytes).into_owned();
    if output.len() <= max_bytes {
        return (output, false);
    }

    let mut end = 0;
    for (index, character) in output.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    output.truncate(end);
    (output, true)
}

async fn join_capped_reader(
    task: tokio::task::JoinHandle<Result<CappedOutput, ExecutionJobError>>,
) -> Result<CappedOutput, ExecutionJobError> {
    task.await
        .map_err(|_| ExecutionJobError::OutputReadFailed)?
}

async fn join_capped_reader_after_termination(
    mut task: tokio::task::JoinHandle<Result<CappedOutput, ExecutionJobError>>,
) -> CappedOutput {
    match tokio::time::timeout(PROCESS_TERMINATION_GRACE, &mut task).await {
        Ok(Ok(Ok(output))) => output,
        Ok(Ok(Err(_))) | Ok(Err(_)) => interrupted_output(),
        Err(_) => {
            task.abort();
            interrupted_output()
        }
    }
}

fn interrupted_output() -> CappedOutput {
    CappedOutput {
        output: String::new(),
        truncated: true,
    }
}

fn create_temp_working_dir(job_id: Uuid) -> io::Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("stratum-execution-{job_id}"));
    std::fs::create_dir(&dir)?;
    Ok(dir)
}

async fn cleanup_temp_dir(path: PathBuf) {
    let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(path)).await;
}

#[cfg(unix)]
fn shell_command(command: &str, working_dir: &PathBuf) -> Command {
    let mut process = Command::new("/bin/sh");
    process
        .arg("-c")
        .arg(command)
        .current_dir(working_dir)
        .env_clear()
        .env("PATH", SAFE_COMMAND_PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    process.process_group(0);
    process
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::sync::Barrier;
    use uuid::Uuid;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn request(command: &str) -> ExecutionSubmitRequest {
        ExecutionSubmitRequest {
            run_id: Uuid::new_v4().to_string(),
            command: command.to_string(),
            timeout: Duration::from_secs(2),
            output_max_bytes: 64,
        }
    }

    #[tokio::test]
    async fn submit_reaches_succeeded_with_bounded_output() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();

        let submitted = table
            .submit(workspace_id, request("printf hello"))
            .await
            .unwrap();
        assert_eq!(submitted.workspace_id, workspace_id);

        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::Succeeded);
        assert_eq!(waited.exit_code, Some(0));
        assert_eq!(waited.stdout, "hello");
        assert_eq!(waited.stderr, "");
        assert!(!waited.stdout_truncated);
        assert!(!waited.stderr_truncated);
    }

    #[tokio::test]
    async fn failed_commands_reach_failed_and_preserve_bounded_output() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();

        let submitted = table
            .submit(workspace_id, request("printf out; printf err >&2; exit 7"))
            .await
            .unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::Failed);
        assert_eq!(waited.exit_code, Some(7));
        assert_eq!(waited.stdout, "out");
        assert_eq!(waited.stderr, "err");
    }

    #[tokio::test]
    async fn output_above_cap_is_truncated_while_readers_keep_draining() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let mut submit = request("yes x | head -c 200000");
        submit.output_max_bytes = 17;

        let submitted = table.submit(workspace_id, submit).await.unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::Succeeded);
        assert_eq!(waited.stdout.len(), 17);
        assert!(waited.stdout_truncated);
    }

    #[tokio::test]
    async fn invalid_utf8_output_does_not_exceed_byte_cap_after_lossy_decoding() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let mut submit = request("printf '\\377'");
        submit.output_max_bytes = 2;

        let submitted = table.submit(workspace_id, submit).await.unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::Succeeded);
        assert!(waited.stdout.len() <= 2, "{:?}", waited.stdout);
        assert!(waited.stdout_truncated);
    }

    #[tokio::test]
    async fn commands_do_not_inherit_parent_secret_environment() {
        let _guard = ENV_LOCK.lock().unwrap();
        let key = "STRATUM_SECRET_EXECUTION_TEST";
        let value = "server-secret-value";
        unsafe {
            std::env::set_var(key, value);
        }

        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let submitted = table
            .submit(
                workspace_id,
                request("printf '%s' \"$STRATUM_SECRET_EXECUTION_TEST\""),
            )
            .await
            .unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        unsafe {
            std::env::remove_var(key);
        }

        assert_eq!(waited.status, ExecutionJobStatus::Succeeded);
        assert_eq!(waited.stdout, "");
        assert!(!format!("{waited:?}").contains(value));
        assert!(
            !serde_json::to_string(&waited.public_summary())
                .unwrap()
                .contains(value)
        );
    }

    #[tokio::test]
    async fn timeout_kills_child_and_reaches_timed_out() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let mut submit = request("sleep 5");
        submit.timeout = Duration::from_millis(100);

        let submitted = table.submit(workspace_id, submit).await.unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::TimedOut);
        assert_eq!(waited.exit_code, None);
        assert!(waited.ended_at.is_some());
    }

    #[tokio::test]
    async fn timeout_kills_descendants_holding_output_pipes_promptly() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let mut submit = request("printf ready; sleep 2 & sleep 2");
        submit.timeout = Duration::from_millis(50);

        let submitted = table.submit(workspace_id, submit).await.unwrap();
        let started = Instant::now();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_millis(750),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::TimedOut);
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "timeout completion took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn cancellation_is_terminal_and_idempotent() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();

        let submitted = table
            .submit(workspace_id, request("sleep 5"))
            .await
            .unwrap();
        let first = table.cancel(workspace_id, submitted.job_id).await.unwrap();
        let second = table.cancel(workspace_id, submitted.job_id).await.unwrap();
        let first = first.unwrap();
        let second = second.unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            first.status,
            ExecutionJobStatus::Queued
                | ExecutionJobStatus::Running
                | ExecutionJobStatus::Cancelled
        ));
        assert!(matches!(
            second.status,
            ExecutionJobStatus::Queued
                | ExecutionJobStatus::Running
                | ExecutionJobStatus::Cancelled
        ));
        assert_eq!(waited.status, ExecutionJobStatus::Cancelled);
    }

    #[tokio::test]
    async fn cancellation_kills_descendants_holding_output_pipes_promptly() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let marker = std::env::temp_dir().join(format!(
            "stratum-execution-cancel-marker-{}",
            Uuid::new_v4()
        ));

        let submitted = table
            .submit(
                workspace_id,
                request(&format!(
                    "printf ready; touch {}; sleep 2 & sleep 2",
                    marker.display()
                )),
            )
            .await
            .unwrap();

        for _ in 0..50 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(marker.exists(), "job did not create marker before cancel");

        table
            .cancel(workspace_id, submitted.job_id)
            .await
            .unwrap()
            .unwrap();

        let started = Instant::now();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_millis(750),
                },
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(waited.status, ExecutionJobStatus::Cancelled);
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "cancel completion took {:?}",
            started.elapsed()
        );
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test]
    async fn list_get_wait_and_cancel_enforce_workspace_isolation() {
        let table = ExecutionJobTable::new();
        let owner = Uuid::new_v4();
        let other = Uuid::new_v4();

        let submitted = table.submit(owner, request("sleep 1")).await.unwrap();

        assert!(table.list(other).await.is_empty());
        assert!(table.get(other, submitted.job_id).await.unwrap().is_none());
        assert!(
            table
                .wait(
                    other,
                    submitted.job_id,
                    ExecutionWaitRequest {
                        timeout: Duration::from_millis(10),
                    },
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .cancel(other, submitted.job_id)
                .await
                .unwrap()
                .is_none()
        );

        assert!(table.get(owner, submitted.job_id).await.unwrap().is_some());
        assert!(!table.list(owner).await.is_empty());
    }

    #[tokio::test]
    async fn max_jobs_prunes_old_terminal_jobs_and_rejects_when_all_jobs_are_active() {
        let table = ExecutionJobTable::with_max_jobs(1);
        let workspace_id = Uuid::new_v4();

        let first = table
            .submit(workspace_id, request("printf first"))
            .await
            .unwrap();
        let _ = table
            .wait(
                workspace_id,
                first.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        let second = table
            .submit(workspace_id, request("sleep 1"))
            .await
            .unwrap();

        assert!(
            table
                .get(workspace_id, first.job_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get(workspace_id, second.job_id)
                .await
                .unwrap()
                .is_some()
        );

        let err = table
            .submit(workspace_id, request("printf third"))
            .await
            .expect_err("active job should hold the workspace job slot");

        assert_eq!(err, ExecutionJobError::MaxJobsExceeded);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_submissions_do_not_exceed_max_jobs_per_workspace() {
        let table = Arc::new(ExecutionJobTable::with_max_jobs(1));
        let workspace_id = Uuid::new_v4();
        let attempts = 32;
        let barrier = Arc::new(Barrier::new(attempts));
        let mut handles = Vec::with_capacity(attempts);

        for _ in 0..attempts {
            let table = Arc::clone(&table);
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                table.submit(workspace_id, request("sleep 1")).await
            }));
        }

        let mut accepted = Vec::new();
        let mut rejected = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(snapshot) => accepted.push(snapshot.job_id),
                Err(ExecutionJobError::MaxJobsExceeded) => rejected += 1,
                Err(error) => panic!("unexpected submit error: {error:?}"),
            }
        }

        assert_eq!(accepted.len(), 1);
        assert_eq!(rejected, attempts - 1);

        for job_id in accepted {
            table.cancel(workspace_id, job_id).await.unwrap().unwrap();
            let waited = table
                .wait(
                    workspace_id,
                    job_id,
                    ExecutionWaitRequest {
                        timeout: Duration::from_secs(5),
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(waited.status, ExecutionJobStatus::Cancelled);
        }
    }

    #[tokio::test]
    async fn debug_and_summary_redact_sensitive_fields() {
        let table = ExecutionJobTable::new();
        let workspace_id = Uuid::new_v4();
        let secret_command = "SECRET_TOKEN=token-123 printf supersecret";

        let submitted = table
            .submit(workspace_id, request(secret_command))
            .await
            .unwrap();
        let waited = table
            .wait(
                workspace_id,
                submitted.job_id,
                ExecutionWaitRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap()
            .unwrap();

        let table_debug = format!("{table:?}");
        let summary_debug = format!("{waited:?}");
        let summary_json = serde_json::to_string(&waited.public_summary()).unwrap();

        for rendered in [table_debug, summary_debug, summary_json] {
            assert!(!rendered.contains(secret_command), "{rendered}");
            assert!(!rendered.contains("token-123"), "{rendered}");
            assert!(!rendered.contains("supersecret"), "{rendered}");
            assert!(!rendered.contains("/tmp"), "{rendered}");
            assert!(!rendered.contains("Idempotency"), "{rendered}");
        }
    }
}
