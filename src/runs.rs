use crate::auth::Uid;
use crate::error::VfsError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const RUNS_ROOT: &str = "/runs";
pub const PROMPT_FILE: &str = "prompt.md";
pub const COMMAND_FILE: &str = "command.md";
pub const STDOUT_FILE: &str = "stdout.md";
pub const STDERR_FILE: &str = "stderr.md";
pub const RESULT_FILE: &str = "result.md";
pub const METADATA_FILE: &str = "metadata.md";
pub const ARTIFACTS_DIR: &str = "artifacts";
pub const MAX_RUN_ID_LEN: usize = 128;

pub fn validate_run_id(run_id: &str) -> Result<&str, VfsError> {
    if run_id.is_empty() {
        return Err(invalid_run_id("run id cannot be empty"));
    }

    if run_id == "." || run_id == ".." {
        return Err(invalid_run_id("run id cannot be . or .."));
    }

    if run_id.len() > MAX_RUN_ID_LEN {
        return Err(invalid_run_id(format!(
            "run id cannot exceed {MAX_RUN_ID_LEN} bytes"
        )));
    }

    if !run_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(invalid_run_id(
            "run id may contain only ASCII letters, digits, _ and -",
        ));
    }

    Ok(run_id)
}

fn invalid_run_id(message: impl Into<String>) -> VfsError {
    VfsError::InvalidArgs {
        message: format!("invalid run id: {}", message.into()),
    }
}

fn yaml_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');

    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0C}' => escaped.push_str("\\f"),
            ch if ch.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(escaped, "\\u{:04X}", ch as u32);
            }
            ch => escaped.push(ch),
        }
    }

    escaped.push('"');
    escaped
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecordInput {
    pub run_id: Option<String>,
    pub prompt: String,
    pub command: String,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub result: String,
    pub exit_code: Option<i32>,
    pub source_commit: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
}

impl RunRecordInput {
    pub fn new(
        run_id: Option<String>,
        prompt: impl Into<String>,
        command: impl Into<String>,
    ) -> Self {
        Self {
            run_id,
            prompt: prompt.into(),
            command: command.into(),
            stdout: String::new(),
            stderr: String::new(),
            result: String::new(),
            exit_code: None,
            source_commit: None,
            started_at: None,
            ended_at: None,
        }
    }

    pub fn resolve_run_id(&self) -> Result<String, VfsError> {
        let run_id = self
            .run_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        validate_run_id(&run_id)?;
        Ok(run_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecordContext {
    pub workspace_id: Uuid,
    pub agent_uid: Uid,
    pub agent_username: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecordLayout {
    pub root: String,
    pub prompt: String,
    pub command: String,
    pub stdout: String,
    pub stderr: String,
    pub result: String,
    pub metadata: String,
    pub artifacts: String,
}

impl RunRecordLayout {
    pub fn new(run_id: &str) -> Result<Self, VfsError> {
        validate_run_id(run_id)?;

        let root = format!("{RUNS_ROOT}/{run_id}");
        Ok(Self {
            prompt: format!("{root}/{PROMPT_FILE}"),
            command: format!("{root}/{COMMAND_FILE}"),
            stdout: format!("{root}/{STDOUT_FILE}"),
            stderr: format!("{root}/{STDERR_FILE}"),
            result: format!("{root}/{RESULT_FILE}"),
            metadata: format!("{root}/{METADATA_FILE}"),
            artifacts: format!("{root}/{ARTIFACTS_DIR}/"),
            root,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunRecordFileKind {
    Prompt,
    Command,
    Stdout,
    Stderr,
    Result,
    Metadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecordFile {
    pub kind: RunRecordFileKind,
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecordMetadata {
    pub run_id: String,
    pub workspace_id: Uuid,
    pub agent_uid: Uid,
    pub agent_username: String,
    pub created_at: DateTime<Utc>,
    pub exit_code: Option<i32>,
    pub source_commit: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
}

impl RunRecordMetadata {
    pub fn to_markdown(&self) -> String {
        let mut lines = vec![
            "---".to_string(),
            format!("run_id: {}", yaml_string(&self.run_id)),
            format!(
                "workspace_id: {}",
                yaml_string(&self.workspace_id.to_string())
            ),
            format!("agent_uid: {}", self.agent_uid),
            format!("agent_username: {}", yaml_string(&self.agent_username)),
            format!("created_at: {}", yaml_string(&self.created_at.to_rfc3339())),
        ];

        if let Some(exit_code) = self.exit_code {
            lines.push(format!("exit_code: {exit_code}"));
        }

        if let Some(source_commit) = &self.source_commit {
            lines.push(format!("source_commit: {}", yaml_string(source_commit)));
        }

        if let Some(started_at) = self.started_at {
            lines.push(format!(
                "started_at: {}",
                yaml_string(&started_at.to_rfc3339())
            ));
        }

        if let Some(ended_at) = self.ended_at {
            lines.push(format!("ended_at: {}", yaml_string(&ended_at.to_rfc3339())));
        }

        lines.push("---".to_string());
        lines.push(String::new());
        lines.join("\n")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub layout: RunRecordLayout,
    pub metadata: RunRecordMetadata,
    pub files: Vec<RunRecordFile>,
}

impl RunRecord {
    pub fn new(input: RunRecordInput, context: RunRecordContext) -> Result<Self, VfsError> {
        let run_id = input.resolve_run_id()?;
        let layout = RunRecordLayout::new(&run_id)?;
        let metadata = RunRecordMetadata {
            run_id: run_id.clone(),
            workspace_id: context.workspace_id,
            agent_uid: context.agent_uid,
            agent_username: context.agent_username,
            created_at: context.created_at,
            exit_code: input.exit_code,
            source_commit: input.source_commit,
            started_at: input.started_at,
            ended_at: input.ended_at,
        };

        let files = vec![
            RunRecordFile {
                kind: RunRecordFileKind::Prompt,
                path: layout.prompt.clone(),
                content: input.prompt,
            },
            RunRecordFile {
                kind: RunRecordFileKind::Command,
                path: layout.command.clone(),
                content: input.command,
            },
            RunRecordFile {
                kind: RunRecordFileKind::Stdout,
                path: layout.stdout.clone(),
                content: input.stdout,
            },
            RunRecordFile {
                kind: RunRecordFileKind::Stderr,
                path: layout.stderr.clone(),
                content: input.stderr,
            },
            RunRecordFile {
                kind: RunRecordFileKind::Result,
                path: layout.result.clone(),
                content: input.result,
            },
            RunRecordFile {
                kind: RunRecordFileKind::Metadata,
                path: layout.metadata.clone(),
                content: metadata.to_markdown(),
            },
        ];

        Ok(Self {
            run_id,
            layout,
            metadata,
            files,
        })
    }

    pub fn file(&self, kind: RunRecordFileKind) -> Option<&RunRecordFile> {
        self.files.iter().find(|file| file.kind == kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn context() -> RunRecordContext {
        RunRecordContext {
            workspace_id: Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
            agent_uid: 42,
            agent_username: "agent-smith".to_string(),
            created_at: Utc.with_ymd_and_hms(2026, 4, 30, 12, 0, 0).unwrap(),
        }
    }

    #[test]
    fn run_id_validation_accepts_safe_ids() {
        for id in ["abc", "ABC", "run_123", "run-123", "a_b-C_9"] {
            assert_eq!(validate_run_id(id).unwrap(), id);
        }
    }

    #[test]
    fn run_id_validation_rejects_unsafe_ids() {
        let too_long = "a".repeat(MAX_RUN_ID_LEN + 1);
        let rejected = [
            "",
            ".",
            "..",
            "has/slash",
            "has\\slash",
            "has space",
            "has.dot",
            "ümlaut",
            "../escape",
            &too_long,
        ];

        for id in rejected {
            assert!(validate_run_id(id).is_err(), "accepted unsafe id {id:?}");
        }
    }

    #[test]
    fn omitted_run_id_generates_uuid_based_id() {
        let id = RunRecordInput::new(None, "prompt", "echo ok")
            .resolve_run_id()
            .unwrap();

        Uuid::parse_str(&id).expect("generated run id should be a UUID");
        validate_run_id(&id).expect("generated run id should pass validation");
    }

    #[test]
    fn layout_paths_are_canonical() {
        let layout = RunRecordLayout::new("run_123").unwrap();

        assert_eq!(RUNS_ROOT, "/runs");
        assert_eq!(layout.root, "/runs/run_123");
        assert_eq!(layout.prompt, "/runs/run_123/prompt.md");
        assert_eq!(layout.command, "/runs/run_123/command.md");
        assert_eq!(layout.stdout, "/runs/run_123/stdout.md");
        assert_eq!(layout.stderr, "/runs/run_123/stderr.md");
        assert_eq!(layout.result, "/runs/run_123/result.md");
        assert_eq!(layout.metadata, "/runs/run_123/metadata.md");
        assert_eq!(layout.artifacts, "/runs/run_123/artifacts/");
    }

    #[test]
    fn default_output_files_are_empty() {
        let record = RunRecord::new(
            RunRecordInput::new(Some("run_123".to_string()), "Prompt", "echo ok"),
            context(),
        )
        .unwrap();

        let stdout = record.file(RunRecordFileKind::Stdout).unwrap();
        let stderr = record.file(RunRecordFileKind::Stderr).unwrap();
        let result = record.file(RunRecordFileKind::Result).unwrap();

        assert_eq!(stdout.path, "/runs/run_123/stdout.md");
        assert_eq!(stdout.content, "");
        assert_eq!(stderr.path, "/runs/run_123/stderr.md");
        assert_eq!(stderr.content, "");
        assert_eq!(result.path, "/runs/run_123/result.md");
        assert_eq!(result.content, "");
    }

    #[test]
    fn metadata_content_contains_required_and_optional_fields() {
        let mut input = RunRecordInput::new(Some("run_123".to_string()), "Prompt", "echo ok");
        input.exit_code = Some(7);
        input.source_commit = Some("abc123".to_string());
        input.started_at = Some(Utc.with_ymd_and_hms(2026, 4, 30, 12, 1, 0).unwrap());
        input.ended_at = Some(Utc.with_ymd_and_hms(2026, 4, 30, 12, 2, 0).unwrap());

        let record = RunRecord::new(input, context()).unwrap();
        let metadata = record.file(RunRecordFileKind::Metadata).unwrap();

        assert_eq!(metadata.path, "/runs/run_123/metadata.md");
        assert!(metadata.content.contains("run_id: \"run_123\""));
        assert!(
            metadata
                .content
                .contains("workspace_id: \"11111111-1111-4111-8111-111111111111\"")
        );
        assert!(metadata.content.contains("agent_uid: 42"));
        assert!(metadata.content.contains("agent_username: \"agent-smith\""));
        assert!(
            metadata
                .content
                .contains("created_at: \"2026-04-30T12:00:00+00:00\"")
        );
        assert!(metadata.content.contains("exit_code: 7"));
        assert!(metadata.content.contains("source_commit: \"abc123\""));
        assert!(
            metadata
                .content
                .contains("started_at: \"2026-04-30T12:01:00+00:00\"")
        );
        assert!(
            metadata
                .content
                .contains("ended_at: \"2026-04-30T12:02:00+00:00\"")
        );
    }

    #[test]
    fn metadata_escapes_frontmatter_controlled_strings() {
        let mut input = RunRecordInput::new(Some("run_123".to_string()), "Prompt", "echo ok");
        input.source_commit = Some("abc\nrun_id: forged\n---".to_string());

        let mut context = context();
        context.agent_username = "agent\n---\nsource_commit: forged".to_string();

        let record = RunRecord::new(input, context).unwrap();
        let metadata = record.file(RunRecordFileKind::Metadata).unwrap();

        assert!(
            metadata
                .content
                .contains("agent_username: \"agent\\n---\\nsource_commit: forged\"")
        );
        assert!(
            metadata
                .content
                .contains("source_commit: \"abc\\nrun_id: forged\\n---\"")
        );
        assert_eq!(
            metadata
                .content
                .lines()
                .filter(|line| *line == "---")
                .count(),
            2
        );
    }
}
