use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

use super::AppState;
use super::idempotency as http_idempotency;
use super::middleware::session_from_headers;
use crate::audit::{AuditAction, AuditOutcome, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::perms::Access;
use crate::auth::session::Session;
use crate::error::VfsError;
use crate::execution::{
    ExecutionArtifactMetadata, ExecutionJobSnapshot, ExecutionJobStatus, ExecutionRunPaths,
    ExecutionSubmitRequest, ExecutionWaitRequest,
};
use crate::runs::{
    RUNS_ROOT, RunRecord, RunRecordContext, RunRecordFileKind, RunRecordInput, RunRecordLayout,
    RunStatus,
};

const EXECUTE_ROUTE_DISABLED: &str = "execution runner is disabled; set STRATUM_EXECUTION_RUNNER=process-local and STRATUM_EXECUTION_ENABLE_DEV=1";
const EXECUTE_IDEMPOTENCY_UNSUPPORTED: &str =
    "Idempotency-Key is not supported on execution routes";
const RESULT_SUCCEEDED: &str = "Execution completed successfully.";
const RESULT_FAILED: &str = "Execution failed.";
const RESULT_CANCELLED: &str = "Execution was cancelled.";
const RESULT_TIMED_OUT: &str = "Execution timed out.";

#[derive(Debug, Deserialize)]
struct ExecuteRequest {
    command: String,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExecuteWaitBody {
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecuteJobSummaryResponse {
    workspace_id: Uuid,
    job_id: Uuid,
    run_id: String,
    status: ExecutionJobStatus,
    run_paths: ExecutionRunPaths,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    exit_code: Option<i32>,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

#[derive(Debug, Clone)]
struct ResolvedRunRecordLayout {
    runs_root: String,
    root: String,
    prompt: String,
    command: String,
    stdout: String,
    stderr: String,
    result: String,
    metadata: String,
    artifacts: String,
}

#[derive(Debug, Clone)]
struct ExecuteArtifactContext {
    workspace_id: Uuid,
    agent_uid: u32,
    agent_username: String,
    created_at: DateTime<Utc>,
}

impl From<&RunRecord> for ExecuteArtifactContext {
    fn from(record: &RunRecord) -> Self {
        Self {
            workspace_id: record.metadata.workspace_id,
            agent_uid: record.metadata.agent_uid,
            agent_username: record.metadata.agent_username.clone(),
            created_at: record.metadata.created_at,
        }
    }
}

impl From<&RunRecord> for ExecutionArtifactMetadata {
    fn from(record: &RunRecord) -> Self {
        Self {
            agent_uid: record.metadata.agent_uid,
            agent_username: record.metadata.agent_username.clone(),
            created_at: record.metadata.created_at,
        }
    }
}

impl ExecuteArtifactContext {
    fn from_snapshot(snapshot: &ExecutionJobSnapshot) -> Option<Self> {
        let metadata = snapshot.artifact_metadata.as_ref()?;
        Some(Self {
            workspace_id: snapshot.workspace_id,
            agent_uid: metadata.agent_uid,
            agent_username: metadata.agent_username.clone(),
            created_at: metadata.created_at,
        })
    }
}

impl ResolvedRunRecordLayout {
    fn new(session: &Session, layout: &RunRecordLayout) -> Result<Self, VfsError> {
        Ok(Self {
            runs_root: session.resolve_mounted_path(RUNS_ROOT)?,
            root: session.resolve_mounted_path(&layout.root)?,
            prompt: session.resolve_mounted_path(&layout.prompt)?,
            command: session.resolve_mounted_path(&layout.command)?,
            stdout: session.resolve_mounted_path(&layout.stdout)?,
            stderr: session.resolve_mounted_path(&layout.stderr)?,
            result: session.resolve_mounted_path(&layout.result)?,
            metadata: session.resolve_mounted_path(&layout.metadata)?,
            artifacts: session.resolve_mounted_path(&layout.artifacts)?,
        })
    }

    fn path_for_kind(&self, kind: RunRecordFileKind) -> &str {
        match kind {
            RunRecordFileKind::Prompt => &self.prompt,
            RunRecordFileKind::Command => &self.command,
            RunRecordFileKind::Stdout => &self.stdout,
            RunRecordFileKind::Stderr => &self.stderr,
            RunRecordFileKind::Result => &self.result,
            RunRecordFileKind::Metadata => &self.metadata,
        }
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/execute", post(execute))
        .route("/execute/jobs", get(list_jobs))
        .route("/execute/jobs/{job_id}", get(get_job))
        .route("/execute/jobs/{job_id}/wait", post(wait_job))
        .route("/execute/jobs/{job_id}/cancel", post(cancel_job))
}

async fn execute(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ExecuteRequest>,
) -> impl IntoResponse {
    if let Some(response) = ensure_execution_enabled(&state) {
        return response;
    }
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if let Some(response) = reject_idempotency(&session, &headers) {
        return response;
    }

    let mount = session.mount().expect("mounted session checked above");
    let workspace_id = mount.workspace_id();
    if let Err(e) = require_runs_scope(&state, &session, Access::Write).await {
        return err_json_for(&session, &e, StatusCode::FORBIDDEN);
    }

    let input = match execute_request_to_run_input(request, state.db.config().max_file_size) {
        Ok(input) => input,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    let context = RunRecordContext {
        workspace_id,
        agent_uid: session.uid,
        agent_username: session.username.clone(),
        created_at: Utc::now(),
    };
    let record = match RunRecord::new(input, context) {
        Ok(record) => record,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    let resolved = match ResolvedRunRecordLayout::new(&session, &record.layout) {
        Ok(resolved) => resolved,
        Err(e) => return err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    };
    if let Err(e) = require_run_layout_write_scope(&session, &resolved) {
        return err_json_for(&session, &e, StatusCode::FORBIDDEN);
    }
    if let Err(e) = create_run_artifacts(&state, &session, &record, &resolved).await {
        return err_json_for(&session, &e, StatusCode::BAD_REQUEST);
    }

    let runtime = state.db.execution_runner().clone();
    let snapshot = match state
        .db
        .execution_jobs()
        .submit(
            workspace_id,
            ExecutionSubmitRequest {
                run_id: record.run_id.clone(),
                command: record
                    .file(RunRecordFileKind::Command)
                    .map(|file| file.content.clone())
                    .unwrap_or_default(),
                timeout: runtime.timeout(),
                output_max_bytes: runtime.output_max_bytes(),
                artifact_metadata: Some(ExecutionArtifactMetadata::from(&record)),
            },
        )
        .await
    {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let _ = write_submit_failure_artifacts(&state, &session, &record, &resolved).await;
            append_execute_failure_audit(
                &state,
                &session,
                ExecuteFailureAudit {
                    workspace_id,
                    job_id: None,
                    run_id: &record.run_id,
                    resolved: &resolved,
                    exit_code: None,
                    timeout: runtime.timeout(),
                },
            )
            .await;
            return err_json(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("execution job could not be submitted: {e}"),
            );
        }
    };

    append_execute_audit(
        &state,
        &session,
        AuditAction::RunExecuteCreate,
        workspace_id,
        &snapshot,
        &resolved,
        runtime.timeout(),
    )
    .await;
    spawn_artifact_monitor(
        state.clone(),
        session.clone(),
        ExecuteArtifactContext::from(&record),
        workspace_id,
        snapshot.job_id,
        runtime.timeout(),
    );

    (
        StatusCode::CREATED,
        Json(summary_response(&session, &snapshot)),
    )
        .into_response()
}

async fn list_jobs(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(response) = ensure_execution_enabled(&state) {
        return response;
    }
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if let Some(response) = reject_idempotency(&session, &headers) {
        return response;
    }
    let mount = session.mount().expect("mounted session checked above");
    if let Err(e) = require_runs_scope(&state, &session, Access::Read).await {
        return err_json_for(&session, &e, StatusCode::FORBIDDEN);
    }

    let summaries = state
        .db
        .execution_jobs()
        .list(mount.workspace_id())
        .await
        .into_iter()
        .map(|summary| {
            let snapshot = ExecutionJobSnapshot {
                workspace_id: summary.workspace_id,
                run_id: summary.run_id,
                job_id: summary.job_id,
                artifact_metadata: None,
                status: summary.status,
                created_at: summary.created_at,
                started_at: summary.started_at,
                ended_at: summary.ended_at,
                exit_code: summary.exit_code,
                stdout: String::new(),
                stderr: String::new(),
                stdout_truncated: summary.stdout_truncated,
                stderr_truncated: summary.stderr_truncated,
            };
            summary_response(&session, &snapshot)
        })
        .collect::<Vec<_>>();

    Json(serde_json::json!({ "jobs": summaries })).into_response()
}

async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
) -> impl IntoResponse {
    job_lookup_response(state, headers, job_id).await
}

async fn wait_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ExecuteWaitBody>,
) -> impl IntoResponse {
    if let Some(response) = ensure_execution_enabled(&state) {
        return response;
    }
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if let Some(response) = reject_idempotency(&session, &headers) {
        return response;
    }
    let mount = session.mount().expect("mounted session checked above");
    if let Err(e) = require_runs_scope(&state, &session, Access::Read).await {
        return err_json_for(&session, &e, StatusCode::FORBIDDEN);
    }

    let timeout = wait_timeout(body.timeout_ms, state.db.execution_runner().timeout());
    match state
        .db
        .execution_jobs()
        .wait(
            mount.workspace_id(),
            job_id,
            ExecutionWaitRequest { timeout },
        )
        .await
    {
        Ok(Some(snapshot)) => {
            if snapshot.status.is_terminal()
                && let Some(artifact_context) = ExecuteArtifactContext::from_snapshot(&snapshot)
                && let Ok(resolved) = resolved_for_snapshot(&session, &snapshot)
                && require_run_layout_write_scope(&session, &resolved).is_ok()
                && write_terminal_artifacts(&state, &session, &artifact_context, &snapshot)
                    .await
                    .is_ok()
            {
                let _ = state
                    .db
                    .execution_jobs()
                    .mark_terminal_artifacts_finalized(snapshot.workspace_id, snapshot.job_id)
                    .await;
            }
            Json(summary_response(&session, &snapshot)).into_response()
        }
        Ok(None) => err_json(StatusCode::NOT_FOUND, "execution job not found"),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("execution job state unavailable: {e}"),
        ),
    }
}

async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(response) = ensure_execution_enabled(&state) {
        return response;
    }
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if let Some(response) = reject_idempotency(&session, &headers) {
        return response;
    }
    let mount = session.mount().expect("mounted session checked above");
    if let Err(e) = require_runs_scope(&state, &session, Access::Write).await {
        return err_json_for(&session, &e, StatusCode::FORBIDDEN);
    }

    let workspace_id = mount.workspace_id();
    match state.db.execution_jobs().cancel(workspace_id, job_id).await {
        Ok(Some(snapshot)) => {
            append_execute_audit(
                &state,
                &session,
                AuditAction::RunExecuteCancel,
                workspace_id,
                &snapshot,
                &resolved_for_snapshot(&session, &snapshot).unwrap_or_else(|_| {
                    ResolvedRunRecordLayout {
                        runs_root: RUNS_ROOT.to_string(),
                        root: format!("{RUNS_ROOT}/{}", snapshot.run_id),
                        prompt: String::new(),
                        command: String::new(),
                        stdout: String::new(),
                        stderr: String::new(),
                        result: String::new(),
                        metadata: String::new(),
                        artifacts: format!("{RUNS_ROOT}/{}/artifacts/", snapshot.run_id),
                    }
                }),
                state.db.execution_runner().timeout(),
            )
            .await;
            let waited = state
                .db
                .execution_jobs()
                .wait(
                    workspace_id,
                    job_id,
                    ExecutionWaitRequest {
                        timeout: Duration::from_secs(5).min(state.db.execution_runner().timeout()),
                    },
                )
                .await
                .ok()
                .flatten()
                .unwrap_or(snapshot);
            Json(summary_response(&session, &waited)).into_response()
        }
        Ok(None) => err_json(StatusCode::NOT_FOUND, "execution job not found"),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("execution job state unavailable: {e}"),
        ),
    }
}

async fn job_lookup_response(
    state: AppState,
    headers: HeaderMap,
    job_id: Uuid,
) -> axum::response::Response {
    if let Some(response) = ensure_execution_enabled(&state) {
        return response;
    }
    let session = match mounted_session_from_headers(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if let Some(response) = reject_idempotency(&session, &headers) {
        return response;
    }
    let mount = session.mount().expect("mounted session checked above");
    if let Err(e) = require_runs_scope(&state, &session, Access::Read).await {
        return err_json_for(&session, &e, StatusCode::FORBIDDEN);
    }

    match state
        .db
        .execution_jobs()
        .get(mount.workspace_id(), job_id)
        .await
    {
        Ok(Some(snapshot)) => Json(summary_response(&session, &snapshot)).into_response(),
        Ok(None) => err_json(StatusCode::NOT_FOUND, "execution job not found"),
        Err(e) => err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("execution job state unavailable: {e}"),
        ),
    }
}

fn ensure_execution_enabled(state: &AppState) -> Option<axum::response::Response> {
    if state.db.execution_runner().enabled() {
        None
    } else {
        Some(err_json(
            StatusCode::SERVICE_UNAVAILABLE,
            EXECUTE_ROUTE_DISABLED,
        ))
    }
}

fn reject_idempotency(session: &Session, headers: &HeaderMap) -> Option<axum::response::Response> {
    match http_idempotency::idempotency_key_from_headers(headers) {
        Ok(None) => None,
        Ok(Some(_)) => Some(err_json_for(
            session,
            &VfsError::InvalidArgs {
                message: EXECUTE_IDEMPOTENCY_UNSUPPORTED.to_string(),
            },
            StatusCode::BAD_REQUEST,
        )),
        Err(e) => Some(err_json_for(session, &e, StatusCode::BAD_REQUEST)),
    }
}

async fn mounted_session_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Session, axum::response::Response> {
    let session = session_from_headers(state, headers)
        .await
        .map_err(|e| err_json(StatusCode::UNAUTHORIZED, e.to_string()))?;
    if session.mount().is_none() {
        let error = VfsError::PermissionDenied {
            path: RUNS_ROOT.to_string(),
        };
        return Err(err_json_for(&session, &error, StatusCode::FORBIDDEN));
    }
    Ok(session)
}

async fn require_runs_scope(
    state: &AppState,
    session: &Session,
    access: Access,
) -> Result<(), VfsError> {
    let runs_root = session.resolve_mounted_path(RUNS_ROOT)?;
    if !session.is_path_allowed(&runs_root, access) {
        return Err(VfsError::PermissionDenied { path: runs_root });
    }
    match access {
        Access::Read => {
            state.db.stat_as(&runs_root, session).await?;
        }
        Access::Write => {
            state.db.check_mkdir_p_as(&runs_root, session).await?;
        }
        Access::Execute => {}
    }
    Ok(())
}

fn require_run_layout_write_scope(
    session: &Session,
    resolved: &ResolvedRunRecordLayout,
) -> Result<(), VfsError> {
    for path in [
        resolved.runs_root.as_str(),
        resolved.root.as_str(),
        resolved.artifacts.as_str(),
        resolved.prompt.as_str(),
        resolved.command.as_str(),
        resolved.stdout.as_str(),
        resolved.stderr.as_str(),
        resolved.result.as_str(),
        resolved.metadata.as_str(),
    ] {
        if !session.is_path_allowed(path, Access::Write) {
            return Err(VfsError::PermissionDenied {
                path: path.to_string(),
            });
        }
    }
    Ok(())
}

fn execute_request_to_run_input(
    request: ExecuteRequest,
    max_file_size: usize,
) -> Result<RunRecordInput, VfsError> {
    if request.command.trim().is_empty() {
        return Err(VfsError::InvalidArgs {
            message: "command cannot be empty".to_string(),
        });
    }
    if request.command.len() > max_file_size {
        return Err(VfsError::InvalidArgs {
            message: format!("command size exceeds max {max_file_size}"),
        });
    }

    let mut input = RunRecordInput::new(
        request.run_id,
        request.prompt.unwrap_or_default(),
        request.command,
    );
    input.status = Some(RunStatus::Queued);
    Ok(input)
}

async fn create_run_artifacts(
    state: &AppState,
    session: &Session,
    record: &RunRecord,
    resolved: &ResolvedRunRecordLayout,
) -> Result<(), VfsError> {
    state.db.mkdir_p_as(&resolved.runs_root, session).await?;
    state.db.mkdir_as(&resolved.root, session).await?;
    state.db.mkdir_as(&resolved.artifacts, session).await?;
    for file in &record.files {
        state
            .db
            .write_file_as(
                resolved.path_for_kind(file.kind),
                file.content.as_bytes().to_vec(),
                session,
            )
            .await?;
    }
    Ok(())
}

fn spawn_artifact_monitor(
    state: AppState,
    session: Session,
    artifact_context: ExecuteArtifactContext,
    workspace_id: Uuid,
    job_id: Uuid,
    timeout: Duration,
) {
    tokio::spawn(async move {
        let mut start_recorded = false;
        loop {
            let snapshot = match state.db.execution_jobs().get(workspace_id, job_id).await {
                Ok(Some(snapshot)) => snapshot,
                _ => return,
            };
            if !start_recorded && snapshot.started_at.is_some() {
                let _ =
                    write_running_artifacts(&state, &session, &artifact_context, &snapshot).await;
                if let Ok(resolved) = resolved_for_snapshot(&session, &snapshot) {
                    append_execute_audit(
                        &state,
                        &session,
                        AuditAction::RunExecuteStart,
                        workspace_id,
                        &snapshot,
                        &resolved,
                        timeout,
                    )
                    .await;
                }
                start_recorded = true;
            }
            if snapshot.status.is_terminal() {
                if write_terminal_artifacts(&state, &session, &artifact_context, &snapshot)
                    .await
                    .is_ok()
                {
                    let _ = state
                        .db
                        .execution_jobs()
                        .mark_terminal_artifacts_finalized(workspace_id, job_id)
                        .await;
                }
                if let Ok(resolved) = resolved_for_snapshot(&session, &snapshot) {
                    let action = match snapshot.status {
                        ExecutionJobStatus::Succeeded => AuditAction::RunExecuteFinish,
                        ExecutionJobStatus::Cancelled => AuditAction::RunExecuteCancel,
                        ExecutionJobStatus::Failed | ExecutionJobStatus::TimedOut => {
                            AuditAction::RunExecuteFailure
                        }
                        ExecutionJobStatus::Queued | ExecutionJobStatus::Running => {
                            AuditAction::RunExecuteFinish
                        }
                    };
                    append_execute_audit(
                        &state,
                        &session,
                        action,
                        workspace_id,
                        &snapshot,
                        &resolved,
                        timeout,
                    )
                    .await;
                }
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}

async fn write_running_artifacts(
    state: &AppState,
    session: &Session,
    artifact_context: &ExecuteArtifactContext,
    snapshot: &ExecutionJobSnapshot,
) -> Result<(), VfsError> {
    let layout = RunRecordLayout::new(&snapshot.run_id)?;
    let resolved = ResolvedRunRecordLayout::new(session, &layout)?;
    let metadata =
        run_record_for_snapshot(artifact_context, snapshot, String::new(), String::new())?;
    let content = metadata
        .file(RunRecordFileKind::Metadata)
        .expect("metadata file")
        .content
        .as_bytes()
        .to_vec();
    state
        .db
        .write_file_as(&resolved.metadata, content, session)
        .await
}

async fn write_terminal_artifacts(
    state: &AppState,
    session: &Session,
    artifact_context: &ExecuteArtifactContext,
    snapshot: &ExecutionJobSnapshot,
) -> Result<(), VfsError> {
    let layout = RunRecordLayout::new(&snapshot.run_id)?;
    let resolved = ResolvedRunRecordLayout::new(session, &layout)?;
    let result = result_text(snapshot);
    let record =
        run_record_for_snapshot(artifact_context, snapshot, snapshot.stdout.clone(), result)?;
    for kind in [
        RunRecordFileKind::Stdout,
        RunRecordFileKind::Stderr,
        RunRecordFileKind::Result,
        RunRecordFileKind::Metadata,
    ] {
        let Some(file) = record.file(kind) else {
            continue;
        };
        state
            .db
            .write_file_as(
                resolved.path_for_kind(kind),
                file.content.as_bytes().to_vec(),
                session,
            )
            .await?;
    }
    Ok(())
}

async fn write_submit_failure_artifacts(
    state: &AppState,
    session: &Session,
    record: &RunRecord,
    resolved: &ResolvedRunRecordLayout,
) -> Result<(), VfsError> {
    let mut input = RunRecordInput::new(Some(record.run_id.clone()), "", "");
    input.result = RESULT_FAILED.to_string();
    input.status = Some(RunStatus::Failed);
    input.ended_at = Some(Utc::now());
    let failed_record = RunRecord::new(
        input,
        RunRecordContext {
            workspace_id: record.metadata.workspace_id,
            agent_uid: record.metadata.agent_uid,
            agent_username: record.metadata.agent_username.clone(),
            created_at: record.metadata.created_at,
        },
    )?;

    for kind in [
        RunRecordFileKind::Stdout,
        RunRecordFileKind::Stderr,
        RunRecordFileKind::Result,
        RunRecordFileKind::Metadata,
    ] {
        let Some(file) = failed_record.file(kind) else {
            continue;
        };
        state
            .db
            .write_file_as(
                resolved.path_for_kind(kind),
                file.content.as_bytes().to_vec(),
                session,
            )
            .await?;
    }
    Ok(())
}

fn run_record_for_snapshot(
    artifact_context: &ExecuteArtifactContext,
    snapshot: &ExecutionJobSnapshot,
    stdout: String,
    result: String,
) -> Result<RunRecord, VfsError> {
    let mut input = RunRecordInput::new(Some(snapshot.run_id.clone()), "", "");
    input.stdout = stdout;
    input.stderr = snapshot.stderr.clone();
    input.result = result;
    input.status = Some(RunStatus::from(snapshot.status));
    input.exit_code = snapshot.exit_code;
    input.started_at = snapshot.started_at;
    input.ended_at = snapshot.ended_at;
    RunRecord::new(
        input,
        RunRecordContext {
            workspace_id: artifact_context.workspace_id,
            agent_uid: artifact_context.agent_uid,
            agent_username: artifact_context.agent_username.clone(),
            created_at: artifact_context.created_at,
        },
    )
}

fn result_text(snapshot: &ExecutionJobSnapshot) -> String {
    let mut text = match snapshot.status {
        ExecutionJobStatus::Succeeded => RESULT_SUCCEEDED.to_string(),
        ExecutionJobStatus::Failed => RESULT_FAILED.to_string(),
        ExecutionJobStatus::Cancelled => RESULT_CANCELLED.to_string(),
        ExecutionJobStatus::TimedOut => RESULT_TIMED_OUT.to_string(),
        ExecutionJobStatus::Queued => "Execution is queued.".to_string(),
        ExecutionJobStatus::Running => "Execution is running.".to_string(),
    };
    if let Some(exit_code) = snapshot.exit_code {
        text.push_str(&format!("\nexit_code: {exit_code}"));
    }
    text
}

fn resolved_for_snapshot(
    session: &Session,
    snapshot: &ExecutionJobSnapshot,
) -> Result<ResolvedRunRecordLayout, VfsError> {
    ResolvedRunRecordLayout::new(session, &RunRecordLayout::new(&snapshot.run_id)?)
}

fn summary_response(
    session: &Session,
    snapshot: &ExecutionJobSnapshot,
) -> ExecuteJobSummaryResponse {
    let mut summary = snapshot.public_summary();
    summary.run_paths = project_run_paths(session, &summary.run_paths);
    ExecuteJobSummaryResponse {
        workspace_id: summary.workspace_id,
        job_id: summary.job_id,
        run_id: summary.run_id,
        status: summary.status,
        run_paths: summary.run_paths,
        created_at: summary.created_at,
        started_at: summary.started_at,
        ended_at: summary.ended_at,
        exit_code: summary.exit_code,
        stdout_truncated: summary.stdout_truncated,
        stderr_truncated: summary.stderr_truncated,
    }
}

fn project_run_paths(session: &Session, paths: &ExecutionRunPaths) -> ExecutionRunPaths {
    ExecutionRunPaths {
        root: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.root)
                .unwrap_or_else(|_| paths.root.clone()),
        ),
        prompt: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.prompt)
                .unwrap_or_else(|_| paths.prompt.clone()),
        ),
        command: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.command)
                .unwrap_or_else(|_| paths.command.clone()),
        ),
        stdout: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.stdout)
                .unwrap_or_else(|_| paths.stdout.clone()),
        ),
        stderr: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.stderr)
                .unwrap_or_else(|_| paths.stderr.clone()),
        ),
        result: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.result)
                .unwrap_or_else(|_| paths.result.clone()),
        ),
        metadata: session.project_mounted_path(
            &session
                .resolve_mounted_path(&paths.metadata)
                .unwrap_or_else(|_| paths.metadata.clone()),
        ),
        artifacts: project_directory_path(
            session,
            &session
                .resolve_mounted_path(&paths.artifacts)
                .unwrap_or_else(|_| paths.artifacts.clone()),
        ),
    }
}

struct ExecuteFailureAudit<'a> {
    workspace_id: Uuid,
    job_id: Option<Uuid>,
    run_id: &'a str,
    resolved: &'a ResolvedRunRecordLayout,
    exit_code: Option<i32>,
    timeout: Duration,
}

async fn append_execute_failure_audit(
    state: &AppState,
    session: &Session,
    failure: ExecuteFailureAudit<'_>,
) {
    let event = execute_audit_event(
        session,
        AuditAction::RunExecuteFailure,
        failure.workspace_id,
        failure.job_id,
        failure.run_id,
        "failed",
        failure.resolved,
        failure.exit_code,
        failure.timeout,
        false,
        false,
    );
    let _ = state.audit.append(event).await;
}

async fn append_execute_audit(
    state: &AppState,
    session: &Session,
    action: AuditAction,
    workspace_id: Uuid,
    snapshot: &ExecutionJobSnapshot,
    resolved: &ResolvedRunRecordLayout,
    timeout: Duration,
) {
    let event = execute_audit_event(
        session,
        action,
        workspace_id,
        Some(snapshot.job_id),
        &snapshot.run_id,
        status_name(snapshot.status),
        resolved,
        snapshot.exit_code,
        timeout,
        snapshot.stdout_truncated,
        snapshot.stderr_truncated,
    );
    let _ = state.audit.append(event).await;
}

#[allow(clippy::too_many_arguments)]
fn execute_audit_event(
    session: &Session,
    action: AuditAction,
    workspace_id: Uuid,
    job_id: Option<Uuid>,
    run_id: &str,
    status: &str,
    resolved: &ResolvedRunRecordLayout,
    exit_code: Option<i32>,
    timeout: Duration,
    stdout_truncated: bool,
    stderr_truncated: bool,
) -> NewAuditEvent {
    let mut event = NewAuditEvent::from_session(
        session,
        action,
        AuditResource::id(AuditResourceKind::Run, run_id)
            .with_path(session.project_mounted_path(&resolved.root)),
    )
    .with_outcome(AuditOutcome::Success)
    .with_detail("workspace_id", workspace_id)
    .with_detail("run_id", run_id)
    .with_detail("status", status)
    .with_detail("root", session.project_mounted_path(&resolved.root))
    .with_detail(
        "artifacts",
        project_directory_path(session, &resolved.artifacts),
    )
    .with_detail("timeout_ms", duration_millis(timeout))
    .with_detail("stdout_truncated", stdout_truncated)
    .with_detail("stderr_truncated", stderr_truncated);
    if let Some(job_id) = job_id {
        event = event.with_detail("job_id", job_id);
    }
    if let Some(exit_code) = exit_code {
        event = event.with_detail("exit_code", exit_code);
    }
    event
}

fn status_name(status: ExecutionJobStatus) -> &'static str {
    match status {
        ExecutionJobStatus::Queued => "queued",
        ExecutionJobStatus::Running => "running",
        ExecutionJobStatus::Succeeded => "succeeded",
        ExecutionJobStatus::Failed => "failed",
        ExecutionJobStatus::Cancelled => "cancelled",
        ExecutionJobStatus::TimedOut => "timed_out",
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn wait_timeout(requested_timeout_ms: Option<u64>, runtime_timeout: Duration) -> Duration {
    let runtime_timeout_ms = duration_millis(runtime_timeout);
    let timeout_ms = requested_timeout_ms
        .unwrap_or(runtime_timeout_ms)
        .min(runtime_timeout_ms);
    Duration::from_millis(timeout_ms)
}

fn project_directory_path(session: &Session, path: &str) -> String {
    let mut projected = session.project_mounted_path(path);
    if !projected.ends_with('/') {
        projected.push('/');
    }
    projected
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": msg.into()}))).into_response()
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::AlreadyExists { .. } => StatusCode::CONFLICT,
        VfsError::InvalidArgs { .. } | VfsError::InvalidPath { .. } => StatusCode::BAD_REQUEST,
        _ => fallback,
    }
}

fn error_message(session: &Session, error: &VfsError) -> String {
    match error {
        VfsError::NotFound { path } => format!(
            "stratum: no such file or directory: '{}'",
            session.project_mounted_error_path(path)
        ),
        VfsError::AlreadyExists { path } => {
            format!(
                "stratum: already exists: '{}'",
                session.project_mounted_error_path(path)
            )
        }
        VfsError::InvalidPath { path } => {
            format!(
                "stratum: invalid path: '{}'",
                session.project_mounted_error_path(path)
            )
        }
        VfsError::PermissionDenied { path } => format!(
            "stratum: permission denied: '{}'",
            session.project_mounted_error_path(path)
        ),
        _ => error.to_string(),
    }
}

fn err_json_for(
    session: &Session,
    error: &VfsError,
    fallback: StatusCode,
) -> axum::response::Response {
    err_json(error_status(error, fallback), error_message(session, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::runtime::{
        BackendRuntimeConfig, EXECUTION_ENABLE_DEV_ENV, EXECUTION_MAX_JOBS_ENV,
        EXECUTION_OUTPUT_MAX_BYTES_ENV, EXECUTION_RUNNER_ENV, EXECUTION_TIMEOUT_MS_ENV,
    };
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::{ServerLocalDb, ServerState};
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use axum::body::Bytes;
    use std::sync::Arc;

    fn enabled_execution_config() -> crate::backend::runtime::ExecutionRunnerRuntimeConfig {
        enabled_execution_config_with("2000", None)
    }

    fn enabled_execution_config_with(
        timeout_ms: &str,
        max_jobs: Option<&str>,
    ) -> crate::backend::runtime::ExecutionRunnerRuntimeConfig {
        BackendRuntimeConfig::from_lookup(|name| match name {
            EXECUTION_RUNNER_ENV => Some("process-local".to_string()),
            EXECUTION_ENABLE_DEV_ENV => Some("1".to_string()),
            EXECUTION_TIMEOUT_MS_ENV => Some(timeout_ms.to_string()),
            EXECUTION_OUTPUT_MAX_BYTES_ENV => Some("16".to_string()),
            EXECUTION_MAX_JOBS_ENV => max_jobs.map(str::to_string),
            _ => None,
        })
        .unwrap()
        .execution_runner()
        .clone()
    }

    fn extract_agent_token(output: &str) -> String {
        output.lines().last().unwrap().trim().to_string()
    }

    async fn prepare_workspace_db() -> (StratumDb, u32, String) {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/runs", &root).await.unwrap();
        db.execute_command("chmod 777 /demo/runs", &mut root)
            .await
            .unwrap();
        db.mkdir_p_as("/other/runs", &root).await.unwrap();
        db.execute_command("chmod 777 /other/runs", &mut root)
            .await
            .unwrap();
        (db, agent.uid, raw_agent_token)
    }

    async fn workspace_state_with_token(
        db: StratumDb,
        workspace_root: &str,
        agent_uid: u32,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
        enabled: bool,
    ) -> (AppState, Uuid, String) {
        let execution = if enabled {
            enabled_execution_config()
        } else {
            Default::default()
        };
        workspace_state_with_execution(
            db,
            workspace_root,
            agent_uid,
            read_prefixes,
            write_prefixes,
            execution,
        )
        .await
    }

    async fn workspace_state_with_execution(
        db: StratumDb,
        workspace_root: &str,
        agent_uid: u32,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
        execution: crate::backend::runtime::ExecutionRunnerRuntimeConfig,
    ) -> (AppState, Uuid, String) {
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store
            .create_workspace("demo", workspace_root)
            .await
            .unwrap();
        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "run-writer",
                agent_uid,
                read_prefixes,
                write_prefixes,
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: ServerLocalDb::available_with_backend_and_execution(
                Arc::new(db),
                crate::backend::runtime::BackendRuntimeMode::Local,
                execution,
            ),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
            hosted_auth: Arc::new(crate::auth::hosted::InMemoryHostedAuthStore::new()),
            tenant_repos: Arc::new(crate::server::repo_context::InMemoryTenantRepoResolver::new()),
            secret_replay_kms: None,
            search_index: crate::server::unavailable_search_index_store(),
        });
        (state, workspace.id, issued.raw_secret)
    }

    fn bearer_headers(raw_secret: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {raw_secret}").parse().unwrap(),
        );
        headers
    }

    fn workspace_headers(workspace_id: Uuid, raw_secret: &str) -> HeaderMap {
        let mut headers = bearer_headers(raw_secret);
        headers.insert(
            "x-stratum-workspace",
            workspace_id.to_string().parse().unwrap(),
        );
        headers
    }

    fn workspace_headers_with_idempotency(
        workspace_id: Uuid,
        raw_secret: &str,
        key: &str,
    ) -> HeaderMap {
        let mut headers = workspace_headers(workspace_id, raw_secret);
        headers.insert("idempotency-key", key.parse().unwrap());
        headers
    }

    fn malformed_workspace_headers(raw_secret: &str) -> HeaderMap {
        let mut headers = bearer_headers(raw_secret);
        headers.insert("x-stratum-workspace", "not-a-uuid".parse().unwrap());
        headers
    }

    async fn response_bytes(response: axum::response::Response) -> Bytes {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        serde_json::from_slice(&response_bytes(response).await).unwrap()
    }

    async fn submit_command(
        state: AppState,
        workspace_id: Uuid,
        raw_secret: &str,
        command: &str,
        run_id: &str,
    ) -> (StatusCode, serde_json::Value) {
        let response = execute(
            State(state),
            workspace_headers(workspace_id, raw_secret),
            Json(ExecuteRequest {
                command: command.to_string(),
                prompt: Some("prompt-secret".to_string()),
                run_id: Some(run_id.to_string()),
            }),
        )
        .await
        .into_response();
        let status = response.status();
        (status, response_json(response).await)
    }

    #[tokio::test]
    async fn disabled_execute_rejects_without_creating_runs_or_jobs() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            false,
        )
        .await;

        let response = execute(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteRequest {
                command: "printf secret-disabled".to_string(),
                prompt: None,
                run_id: Some("disabled_run".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            state
                .db
                .stat_as("/demo/runs/disabled_run", &Session::root())
                .await
                .is_err()
        );
        assert!(
            state
                .db
                .execution_jobs()
                .list(workspace_id)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn successful_execute_writes_artifacts_and_redacts_public_response() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;

        let (status, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "printf stdout-secret; printf stderr-secret >&2",
            "success_run",
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_no_public_leaks(&body);
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();
        let response = wait_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(2000),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let waited = response_json(response).await;
        assert_eq!(waited["status"], "succeeded");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let root = Session::root();
        assert_eq!(
            String::from_utf8(
                state
                    .db
                    .cat_as("/demo/runs/success_run/command.md", &root)
                    .await
                    .unwrap()
            )
            .unwrap(),
            "printf stdout-secret; printf stderr-secret >&2"
        );
        assert_eq!(
            String::from_utf8(
                state
                    .db
                    .cat_as("/demo/runs/success_run/stdout.md", &root)
                    .await
                    .unwrap()
            )
            .unwrap(),
            "stdout-secret"
        );
        assert_eq!(
            String::from_utf8(
                state
                    .db
                    .cat_as("/demo/runs/success_run/stderr.md", &root)
                    .await
                    .unwrap()
            )
            .unwrap(),
            "stderr-secret"
        );
        let metadata = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/success_run/metadata.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(metadata.contains("status: \"succeeded\""));
        assert!(metadata.contains(&format!("agent_uid: {agent_uid}")));
        assert!(metadata.contains("agent_username: \"ci-agent\""));
        assert!(!metadata.contains("execution-runner"));
    }

    #[tokio::test]
    async fn read_only_wait_preserves_submitter_metadata_attribution() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let mut root = Session::root();
        let reader_raw_token = extract_agent_token(
            &db.execute_command("addagent reader-agent", &mut root)
                .await
                .unwrap(),
        );
        let reader = db.authenticate_token(&reader_raw_token).await.unwrap();
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;
        let reader_token = state
            .workspaces
            .issue_scoped_workspace_token(
                workspace_id,
                "reader",
                reader.uid,
                vec!["/demo".to_string()],
                Vec::new(),
            )
            .await
            .unwrap();

        let (status, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "printf submitter-output",
            "read_only_wait_run",
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();

        let response = wait_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(workspace_id, &reader_token.raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(2000),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let metadata = String::from_utf8(
            state
                .db
                .cat_as(
                    "/demo/runs/read_only_wait_run/metadata.md",
                    &Session::root(),
                )
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(metadata.contains(&format!("agent_uid: {agent_uid}")));
        assert!(metadata.contains("agent_username: \"ci-agent\""));
        assert!(!metadata.contains(&format!("agent_uid: {}", reader.uid)));
        assert!(!metadata.contains("reader-agent"));
    }

    #[tokio::test]
    async fn submit_failure_terminalizes_precreated_run_artifacts() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_execution(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            enabled_execution_config_with("2000", Some("1")),
        )
        .await;

        let (first_status, first_body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "sleep 5",
            "active_run",
        )
        .await;
        assert_eq!(first_status, StatusCode::CREATED);
        let first_job_id: Uuid = serde_json::from_value(first_body["job_id"].clone()).unwrap();

        let (rejected_status, rejected_body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "printf submit-secret",
            "submit_rejected_run",
        )
        .await;
        let root = Session::root();
        let metadata = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/submit_rejected_run/metadata.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        let result = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/submit_rejected_run/result.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        let _ = state
            .db
            .execution_jobs()
            .cancel(workspace_id, first_job_id)
            .await;

        assert_eq!(rejected_status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            rejected_body["error"]
                .as_str()
                .unwrap()
                .contains("execution job could not be submitted")
        );
        assert!(metadata.contains("status: \"failed\""));
        assert!(metadata.contains("ended_at:"));
        assert!(result.contains("Execution failed."));
        assert!(!metadata.contains("submit-secret"));
        assert!(!result.contains("submit-secret"));
        assert_eq!(state.db.execution_jobs().list(workspace_id).await.len(), 1);
    }

    #[tokio::test]
    async fn failure_writes_bounded_output_and_result_without_public_output() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;

        let (status, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "yes failure-secret | head -c 100; printf stderr-secret >&2; exit 9",
            "failure_run",
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();
        let response = wait_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(2000),
            }),
        )
        .await
        .into_response();
        let waited = response_json(response).await;
        assert_eq!(waited["status"], "failed");
        assert_eq!(waited["stdout_truncated"], true);
        assert_no_public_leaks(&waited);

        tokio::time::sleep(Duration::from_millis(50)).await;
        let root = Session::root();
        let stdout = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/failure_run/stdout.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(stdout.len(), 16);
        let result = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/failure_run/result.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(result.contains("Execution failed."));
        assert!(!result.contains("failure-secret"));
    }

    #[tokio::test]
    async fn wait_timeout_returns_running_summary() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;

        let (_, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "sleep 1; printf done",
            "wait_timeout_run",
        )
        .await;
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();
        let response = wait_job(
            State(state),
            Path(job_id),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(10),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(matches!(
            body["status"].as_str(),
            Some("queued" | "running")
        ));
    }

    #[tokio::test]
    async fn huge_wait_timeout_is_clamped_to_execution_runtime_timeout() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_execution(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            enabled_execution_config_with("25", None),
        )
        .await;
        let snapshot = state
            .db
            .execution_jobs()
            .submit(
                workspace_id,
                ExecutionSubmitRequest {
                    run_id: "huge_wait_run".to_string(),
                    command: "sleep 5".to_string(),
                    timeout: Duration::from_secs(5),
                    output_max_bytes: 16,
                    artifact_metadata: None,
                },
            )
            .await
            .unwrap();

        let waited = tokio::time::timeout(
            Duration::from_millis(500),
            wait_job(
                State(state.clone()),
                Path(snapshot.job_id),
                workspace_headers(workspace_id, &raw_secret),
                Json(ExecuteWaitBody {
                    timeout_ms: Some(u64::MAX),
                }),
            ),
        )
        .await;
        let _ = state
            .db
            .execution_jobs()
            .cancel(workspace_id, snapshot.job_id)
            .await;

        let response = waited
            .expect("huge wait timeout should be clamped by the route")
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(matches!(
            body["status"].as_str(),
            Some("queued" | "running")
        ));
    }

    #[tokio::test]
    async fn list_get_and_wait_are_workspace_isolated() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;
        let other = state
            .workspaces
            .create_workspace("other", "/other")
            .await
            .unwrap();
        let other_token = state
            .workspaces
            .issue_scoped_workspace_token(
                other.id,
                "other",
                agent_uid,
                vec!["/other".to_string()],
                vec!["/other".to_string()],
            )
            .await
            .unwrap();

        let (_, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "printf isolated-secret",
            "isolated_run",
        )
        .await;
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();

        let list = list_jobs(
            State(state.clone()),
            workspace_headers(other.id, &other_token.raw_secret),
        )
        .await
        .into_response();
        let list_body = response_json(list).await;
        assert_eq!(list_body["jobs"].as_array().unwrap().len(), 0);

        let get = get_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(other.id, &other_token.raw_secret),
        )
        .await
        .into_response();
        assert_eq!(get.status(), StatusCode::NOT_FOUND);

        let wait = wait_job(
            State(state),
            Path(job_id),
            workspace_headers(other.id, &other_token.raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(10),
            }),
        )
        .await
        .into_response();
        assert_eq!(wait.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancel_is_workspace_scoped_and_writes_cancelled_metadata() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db.clone(),
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;
        let store = state.workspaces.clone();
        let other = store.create_workspace("other", "/other").await.unwrap();
        let other_token = store
            .issue_scoped_workspace_token(
                other.id,
                "other",
                agent_uid,
                vec!["/other".to_string()],
                vec!["/other".to_string()],
            )
            .await
            .unwrap();

        let (_, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "sleep 5",
            "cancel_run",
        )
        .await;
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();
        let denied = cancel_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(other.id, &other_token.raw_secret),
        )
        .await
        .into_response();
        assert_eq!(denied.status(), StatusCode::NOT_FOUND);

        let cancelled = cancel_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(workspace_id, &raw_secret),
        )
        .await
        .into_response();
        assert_eq!(cancelled.status(), StatusCode::OK);
        let body = response_json(cancelled).await;
        assert_eq!(body["status"], "cancelled");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let metadata = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/cancel_run/metadata.md", &Session::root())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(metadata.contains("status: \"cancelled\""));
    }

    #[tokio::test]
    async fn terminal_wait_writes_final_artifacts_before_immediate_prune() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_execution(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            enabled_execution_config_with("2000", Some("1")),
        )
        .await;

        let (first_status, first_body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "printf prune-secret",
            "pruned_terminal_run",
        )
        .await;
        assert_eq!(first_status, StatusCode::CREATED);
        let first_job_id: Uuid = serde_json::from_value(first_body["job_id"].clone()).unwrap();
        let waited = wait_job(
            State(state.clone()),
            Path(first_job_id),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(2000),
            }),
        )
        .await
        .into_response();
        assert_eq!(waited.status(), StatusCode::OK);
        let waited_body = response_json(waited).await;
        assert_eq!(waited_body["status"], "succeeded");

        let (second_status, second_body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "sleep 5",
            "replacement_run",
        )
        .await;
        assert_eq!(second_status, StatusCode::CREATED);
        let second_job_id: Uuid = serde_json::from_value(second_body["job_id"].clone()).unwrap();

        let root = Session::root();
        let stdout = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/pruned_terminal_run/stdout.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        let result = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/pruned_terminal_run/result.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        let metadata = String::from_utf8(
            state
                .db
                .cat_as("/demo/runs/pruned_terminal_run/metadata.md", &root)
                .await
                .unwrap(),
        )
        .unwrap();
        let _ = state
            .db
            .execution_jobs()
            .cancel(workspace_id, second_job_id)
            .await;

        assert_eq!(stdout, "prune-secret");
        assert!(result.contains("Execution completed successfully."));
        assert!(metadata.contains("status: \"succeeded\""));
    }

    #[tokio::test]
    async fn idempotency_key_is_rejected_before_job_creation() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;

        let response = execute(
            State(state.clone()),
            workspace_headers_with_idempotency(workspace_id, &raw_secret, "exec-key"),
            Json(ExecuteRequest {
                command: "printf idempotency-secret".to_string(),
                prompt: None,
                run_id: Some("idempotency_run".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            state
                .db
                .stat_as("/demo/runs/idempotency_run", &Session::root())
                .await
                .is_err()
        );
        assert!(
            state
                .db
                .execution_jobs()
                .list(workspace_id)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn auth_and_scope_rejections_are_enforced() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            Vec::new(),
            true,
        )
        .await;

        let global = execute(
            State(state.clone()),
            bearer_headers(&raw_secret),
            Json(ExecuteRequest {
                command: "printf global-secret".to_string(),
                prompt: None,
                run_id: Some("global_run".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(global.status(), StatusCode::UNAUTHORIZED);

        let malformed = execute(
            State(state.clone()),
            malformed_workspace_headers(&raw_secret),
            Json(ExecuteRequest {
                command: "printf malformed-secret".to_string(),
                prompt: None,
                run_id: Some("malformed_run".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(malformed.status(), StatusCode::UNAUTHORIZED);

        let scoped = execute(
            State(state.clone()),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteRequest {
                command: "printf scope-secret".to_string(),
                prompt: None,
                run_id: Some("scope_run".to_string()),
            }),
        )
        .await
        .into_response();
        assert_eq!(scoped.status(), StatusCode::FORBIDDEN);
        assert!(
            state
                .db
                .execution_jobs()
                .list(workspace_id)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn audit_details_are_metadata_only() {
        let (db, agent_uid, _) = prepare_workspace_db().await;
        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent_uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
            true,
        )
        .await;

        let (_, body) = submit_command(
            state.clone(),
            workspace_id,
            &raw_secret,
            "printf audit-secret; printf audit-stderr-secret >&2",
            "audit_run",
        )
        .await;
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).unwrap();
        let _ = wait_job(
            State(state.clone()),
            Path(job_id),
            workspace_headers(workspace_id, &raw_secret),
            Json(ExecuteWaitBody {
                timeout_ms: Some(2000),
            }),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let events = state.audit.list_recent(10).await.unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.action == AuditAction::RunExecuteCreate)
        );
        assert!(
            events
                .iter()
                .any(|event| event.action == AuditAction::RunExecuteFinish)
        );
        let audit_json = serde_json::to_string(&events).unwrap();
        for forbidden in [
            "printf audit-secret",
            "audit-secret",
            "audit-stderr-secret",
            "prompt-secret",
            "/tmp/stratum-execution",
            "/demo/runs",
        ] {
            assert!(!audit_json.contains(forbidden), "audit leaked {forbidden}");
        }
        assert!(audit_json.contains("/runs/audit_run"));
    }

    fn assert_no_public_leaks(body: &serde_json::Value) {
        let rendered = serde_json::to_string(body).unwrap();
        for forbidden in [
            "stdout-secret",
            "stderr-secret",
            "failure-secret",
            "prompt-secret",
            "printf ",
            "/demo/runs",
            "/tmp/stratum-execution",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "public response leaked {forbidden}: {rendered}"
            );
        }
    }
}
