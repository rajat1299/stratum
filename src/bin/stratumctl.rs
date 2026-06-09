use clap::{Parser, Subcommand};
use std::path::PathBuf;
use stratum::client::{ClientAuth, StratumClient};
use stratum::error::VfsError;
use stratum::mount_daemon::{
    MountDaemonController, MountDaemonError, MountDaemonFileStore, MountDaemonIpcClient,
    MountDaemonLogView, MountDaemonProcessProbe, MountDaemonStatus, MountDaemonTag,
    MountDaemonUnmountOutcome, SystemMountDaemonFileStore, SystemMountDaemonProcessProbe,
    UnavailableMountDaemonIpcClient,
};
use uuid::Uuid;

#[path = "stratumctl/seed_demo.rs"]
mod seed_demo;

const DEFAULT_MOUNT_TAG: &str = "default";
const DEFAULT_MOUNT_LOG_LINES: usize = 50;
const MIN_MOUNT_LOG_LINES: usize = 1;
const MAX_MOUNT_LOG_LINES: usize = 200;

#[derive(Parser)]
#[command(name = "stratumctl", version, about = "Remote-first stratum CLI")]
struct Cli {
    #[arg(long, env = "STRATUM_URL", default_value = "http://127.0.0.1:3000")]
    url: String,

    #[arg(long, env = "STRATUM_USER")]
    user: Option<String>,

    #[arg(long, env = "STRATUM_TOKEN")]
    token: Option<String>,

    #[arg(long, env = "STRATUM_WORKSPACE_ID")]
    workspace_id: Option<String>,

    #[arg(long, env = "STRATUM_WORKSPACE_TOKEN")]
    workspace_token: Option<String>,

    #[arg(long, env = "STRATUM_REPO")]
    repo: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Health,
    Ls {
        path: Option<String>,
    },
    Cat {
        path: String,
    },
    Write {
        path: String,
        #[arg(long)]
        stdin: bool,
        content: Vec<String>,
    },
    Grep {
        pattern: String,
        path: Option<String>,
    },
    Find {
        pattern: String,
        path: Option<String>,
    },
    Tree {
        path: Option<String>,
    },
    Commit {
        message: String,
    },
    Log,
    Revert {
        hash: String,
    },
    Status,
    Diff {
        path: Option<String>,
    },
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    Mount {
        #[command(subcommand)]
        command: MountCommand,
    },
}

#[derive(Subcommand)]
enum WorkspaceCommand {
    List,
    Create {
        name: String,
        root_path: String,
    },
    IssueToken {
        workspace_id: Uuid,
        name: String,
        agent_token: String,
        #[arg(long = "read-prefix")]
        read_prefixes: Vec<String>,
        #[arg(long = "write-prefix")]
        write_prefixes: Vec<String>,
    },
    SeedDemo {
        #[arg(long, env = "STRATUM_AGENT_TOKEN")]
        agent_token: Option<String>,
        #[arg(long)]
        fixture: Option<PathBuf>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        root_path: Option<String>,
        #[arg(long)]
        token_name: Option<String>,
        #[arg(long)]
        session_ref: Option<String>,
        #[arg(long = "env-out")]
        env_out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum MountCommand {
    Status {
        #[arg(long, default_value = DEFAULT_MOUNT_TAG)]
        tag: String,
        #[arg(long, env = "STRATUM_MOUNT_RUNTIME_DIR")]
        runtime_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Logs {
        #[arg(long, default_value = DEFAULT_MOUNT_TAG)]
        tag: String,
        #[arg(long, env = "STRATUM_MOUNT_RUNTIME_DIR")]
        runtime_dir: Option<PathBuf>,
        #[arg(long, default_value_t = DEFAULT_MOUNT_LOG_LINES, value_parser = parse_mount_log_lines)]
        lines: usize,
        #[arg(long)]
        json: bool,
    },
    Unmount {
        #[arg(long, default_value = DEFAULT_MOUNT_TAG)]
        tag: String,
        #[arg(long, env = "STRATUM_MOUNT_RUNTIME_DIR")]
        runtime_dir: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Command::Mount { command } = &cli.command {
        match run_mount_command(command) {
            Ok(output) => {
                print!("{output}");
            }
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
        return;
    }

    let auth = match resolve_auth(&cli) {
        Ok(auth) => auth,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };
    let client = StratumClient::new(cli.url.clone(), auth).with_repo(cli.repo.clone());

    let result = match cli.command {
        Command::Health => print_json(client.health().await),
        Command::Ls { path } => match client.list_directory(path.as_deref().unwrap_or("")).await {
            Ok(response) => {
                for entry in response.entries {
                    let suffix = if entry.kind == "directory" { "/" } else { "" };
                    println!("{}{suffix}", entry.name);
                }
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Cat { path } => match client.read_file(&path).await {
            Ok(contents) => {
                print!("{contents}");
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Write {
            path,
            stdin,
            content,
        } => {
            let contents = if stdin {
                read_stdin().await
            } else {
                content.join(" ")
            };
            print_json(client.write_file(&path, contents).await)
        }
        Command::Grep { pattern, path } => match client.grep(&pattern, path.as_deref()).await {
            Ok(response) => {
                for item in response.results {
                    println!("{}:{}: {}", item.file, item.line_num, item.line);
                }
                eprintln!("{} match(es)", response.count);
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Find { pattern, path } => match client.find(&pattern, path.as_deref()).await {
            Ok(response) => {
                for item in response.results {
                    println!("{item}");
                }
                eprintln!("{} match(es)", response.count);
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Tree { path } => match client.tree(path.as_deref()).await {
            Ok(tree) => {
                print!("{tree}");
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Commit { message } => match client.commit(&message).await {
            Ok(commit) => {
                println!("[{}] {}", commit.hash, commit.message);
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Log => match client.log().await {
            Ok(response) => {
                for commit in response.commits {
                    println!(
                        "{} {} {} {}",
                        commit.hash, commit.timestamp, commit.author, commit.message
                    );
                }
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Revert { hash } => print_json(client.revert(&hash).await),
        Command::Status => match client.status().await {
            Ok(status) => {
                print!("{status}");
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Diff { path } => match client.diff(path.as_deref()).await {
            Ok(diff) => {
                print!("{diff}");
                Ok(())
            }
            Err(err) => Err(err),
        },
        Command::Workspace { ref command } => match command {
            WorkspaceCommand::List => print_json(client.list_workspaces().await),
            WorkspaceCommand::Create { name, root_path } => {
                print_json(client.create_workspace(&name, &root_path).await)
            }
            WorkspaceCommand::IssueToken {
                workspace_id,
                name,
                agent_token,
                read_prefixes,
                write_prefixes,
            } => print_json(
                client
                    .issue_scoped_workspace_token(
                        *workspace_id,
                        name,
                        agent_token,
                        (!read_prefixes.is_empty()).then_some(read_prefixes.clone()),
                        (!write_prefixes.is_empty()).then_some(write_prefixes.clone()),
                    )
                    .await,
            ),
            WorkspaceCommand::SeedDemo {
                agent_token,
                fixture,
                name,
                root_path,
                token_name,
                session_ref,
                env_out,
            } => {
                let auth = resolve_seed_demo_admin_auth(&cli);
                match seed_demo::run_workspace_seed_demo(
                    seed_demo::SeedDemoContext {
                        url: cli.url.clone(),
                        auth,
                        repo: cli.repo.clone(),
                        command_name: std::env::args()
                            .next()
                            .unwrap_or_else(|| "stratumctl".to_string()),
                    },
                    seed_demo::SeedDemoOptions {
                        agent_token: agent_token.clone(),
                        fixture: fixture.clone(),
                        name: name.clone(),
                        root_path: root_path.clone(),
                        token_name: token_name.clone(),
                        session_ref: session_ref.clone(),
                        env_out: env_out.clone(),
                    },
                )
                .await
                {
                    Ok(output) => {
                        print!("{output}");
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
        },
        Command::Mount { .. } => unreachable!("mount commands are handled before auth resolution"),
    };

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run_mount_command(command: &MountCommand) -> Result<String, VfsError> {
    let controller = MountDaemonController::new(
        SystemMountDaemonProcessProbe,
        SystemMountDaemonFileStore,
        UnavailableMountDaemonIpcClient,
    );
    render_mount_command(command, &controller)
}

fn render_mount_command<P, F, I>(
    command: &MountCommand,
    controller: &MountDaemonController<P, F, I>,
) -> Result<String, VfsError>
where
    P: MountDaemonProcessProbe,
    F: MountDaemonFileStore,
    I: MountDaemonIpcClient,
{
    match command {
        MountCommand::Status {
            tag,
            runtime_dir,
            json,
        } => {
            let tag = parse_mount_tag(tag)?;
            let runtime_dir = mount_runtime_dir(runtime_dir);
            let status = controller
                .resolve_status(tag, runtime_dir)
                .map_err(mount_error_to_vfs)?;
            Ok(render_mount_status(&status, *json))
        }
        MountCommand::Logs {
            tag,
            runtime_dir,
            lines,
            json,
        } => {
            let tag = parse_mount_tag(tag)?;
            let runtime_dir = mount_runtime_dir(runtime_dir);
            let logs = controller
                .logs(tag.clone(), &runtime_dir, *lines)
                .map_err(mount_error_to_vfs)?;
            Ok(render_mount_logs(
                &logs,
                *json,
                tag.path_component(),
                &runtime_dir,
            ))
        }
        MountCommand::Unmount {
            tag,
            runtime_dir,
            json,
        } => {
            let tag = parse_mount_tag(tag)?;
            let runtime_dir = mount_runtime_dir(runtime_dir);
            let outcome = controller
                .unmount(tag, runtime_dir)
                .map_err(mount_error_to_vfs)?;
            Ok(render_mount_unmount(outcome, *json))
        }
    }
}

fn render_mount_status(status: &MountDaemonStatus, json: bool) -> String {
    if json {
        let value = serde_json::json!({
            "tag": status.redacted_tag(),
            "tag_len": status.tag_len(),
            "backend": status.backend().to_string(),
            "state": status.state().to_string(),
            "pid_present": status.pid_present(),
            "socket_present": status.socket_present(),
            "log_present": status.log_present(),
            "reason": status.reason_code().map(|code| code.to_string()),
            "hydration": status.hydration_progress().map(|progress| serde_json::json!({
                "pending": progress.pending,
                "running": progress.running,
                "completed": progress.completed,
                "failed": progress.failed,
                "backoff": progress.backoff,
                "poisoned": progress.poisoned,
                "total_attempts": progress.total_attempts,
            })),
        });
        return format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
    }

    let reason = status
        .reason_code()
        .map_or_else(|| "none".to_string(), |code| code.to_string());
    format!(
        "mount daemon: state={} backend={} tag=<redacted> pid={} socket={} log={} reason={}\n",
        status.state(),
        status.backend(),
        present_label(status.pid_present()),
        present_label(status.socket_present()),
        present_label(status.log_present()),
        reason
    )
}

fn render_mount_logs(
    logs: &MountDaemonLogView,
    json: bool,
    raw_tag: &str,
    runtime_dir: &std::path::Path,
) -> String {
    let lines = redact_mount_log_lines_for_cli(logs.lines(), raw_tag, runtime_dir);
    if json {
        let value = serde_json::json!({
            "available": logs.available(),
            "returned_count": logs.returned_count(),
            "truncated": logs.truncated(),
            "lines": lines,
        });
        return format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
    }

    if !logs.available() {
        return "mount daemon logs unavailable\n".to_string();
    }

    let mut output = String::new();
    for line in &lines {
        output.push_str(line);
        output.push('\n');
    }
    output
}

fn render_mount_unmount(outcome: MountDaemonUnmountOutcome, json: bool) -> String {
    let outcome_label = match outcome {
        MountDaemonUnmountOutcome::NotRunning => "not_running",
        MountDaemonUnmountOutcome::StaleCleaned => "stale_cleaned",
        MountDaemonUnmountOutcome::Requested => "requested",
        MountDaemonUnmountOutcome::AlreadyUnmounting => "already_unmounting",
        MountDaemonUnmountOutcome::Crashed => "crashed",
        MountDaemonUnmountOutcome::Unavailable => "unavailable",
        _ => "unknown",
    };

    if json {
        let value = serde_json::json!({
            "outcome": outcome_label,
            "tag": "<redacted>",
        });
        return format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
    }

    format!("mount daemon unmount: outcome={outcome_label} tag=<redacted>\n")
}

fn parse_mount_tag(value: &str) -> Result<MountDaemonTag, VfsError> {
    MountDaemonTag::new(value.to_string()).map_err(mount_error_to_vfs)
}

fn mount_runtime_dir(runtime_dir: &Option<PathBuf>) -> PathBuf {
    runtime_dir
        .clone()
        .unwrap_or_else(default_mount_runtime_dir)
}

fn default_mount_runtime_dir() -> PathBuf {
    std::env::temp_dir().join("stratum-mount-runtime")
}

fn parse_mount_log_lines(value: &str) -> Result<usize, String> {
    let lines = value
        .parse::<usize>()
        .map_err(|_| "lines must be an integer in 1..=200".to_string())?;
    if (MIN_MOUNT_LOG_LINES..=MAX_MOUNT_LOG_LINES).contains(&lines) {
        Ok(lines)
    } else {
        Err("lines must be in 1..=200".to_string())
    }
}

fn present_label(value: bool) -> &'static str {
    if value { "present" } else { "absent" }
}

fn redact_mount_log_lines_for_cli(
    lines: &[String],
    raw_tag: &str,
    runtime_dir: &std::path::Path,
) -> Vec<String> {
    let runtime_dir = runtime_dir.to_string_lossy();
    lines
        .iter()
        .map(|line| {
            line.replace(raw_tag, "<redacted>")
                .replace(runtime_dir.as_ref(), "<redacted>")
        })
        .collect()
}

fn mount_error_to_vfs(error: MountDaemonError) -> VfsError {
    VfsError::InvalidArgs {
        message: error.to_string(),
    }
}

#[cfg(test)]
#[test]
fn mount_status_command_parses_default_tag() {
    let cli = Cli::try_parse_from(["stratumctl", "mount", "status"]).unwrap();

    let Command::Mount {
        command: MountCommand::Status {
            tag, runtime_dir, ..
        },
    } = cli.command
    else {
        panic!("expected mount status command");
    };

    assert_eq!(tag, DEFAULT_MOUNT_TAG);
    assert_eq!(runtime_dir, None);
}

#[cfg(test)]
#[test]
fn mount_logs_command_bounds_requested_lines() {
    for lines in ["0", "201"] {
        let result = Cli::try_parse_from(["stratumctl", "mount", "logs", "--lines", lines]);
        assert!(result.is_err(), "lines={lines} should be rejected");
    }

    let cli = Cli::try_parse_from(["stratumctl", "mount", "logs", "--lines", "200"]).unwrap();
    let Command::Mount {
        command: MountCommand::Logs { lines, .. },
    } = cli.command
    else {
        panic!("expected mount logs command");
    };
    assert_eq!(lines, 200);
}

#[cfg(test)]
#[test]
fn mount_unmount_command_parses_runtime_dir() {
    let cli = Cli::try_parse_from([
        "stratumctl",
        "mount",
        "unmount",
        "--runtime-dir",
        "/safe-runtime",
    ])
    .unwrap();

    let Command::Mount {
        command: MountCommand::Unmount { runtime_dir, .. },
    } = cli.command
    else {
        panic!("expected mount unmount command");
    };

    assert_eq!(runtime_dir, Some(PathBuf::from("/safe-runtime")));
}

#[cfg(test)]
#[test]
fn mount_command_rejects_unsafe_tag_before_control_execution() {
    let err = run_mount_command(&MountCommand::Status {
        tag: "../secret-tag".to_string(),
        runtime_dir: Some(PathBuf::from("/safe-runtime")),
        json: false,
    })
    .expect_err("unsafe tag should fail before control execution");

    let VfsError::InvalidArgs { message } = err else {
        panic!("unsafe tag should return InvalidArgs");
    };
    assert_eq!(message, "mount daemon operation failed: invalid tag");
    assert!(!message.contains("../secret-tag"));
}

#[cfg(test)]
#[test]
fn mount_command_text_rendering_redacts_runtime_paths() {
    let view = MountDaemonLogView::from_raw_tail("tag=secret-tag path=/safe-runtime ready", 1);
    let output = render_mount_logs(
        &view,
        false,
        "secret-tag",
        std::path::Path::new("/safe-runtime"),
    );

    assert!(output.contains("tag=<redacted> path=<redacted> ready"));
    assert!(!output.contains("secret-tag"));
    assert!(!output.contains("/safe-runtime"));
}

#[cfg(test)]
#[test]
fn mount_command_json_rendering_reports_status_without_secrets() {
    let tag = MountDaemonTag::new("secret-tag").unwrap();
    let status = MountDaemonStatus::new(
        tag,
        stratum::mount_daemon::MountDaemonBackend::default(),
        stratum::mount_daemon::MountDaemonState::Stopped,
    );

    let output = render_mount_status(&status, true);

    assert!(output.contains("\"tag\": \"<redacted>\""));
    assert!(output.contains("\"state\": \"stopped\""));
    assert!(!output.contains("secret-tag"));
}

fn resolve_auth(cli: &Cli) -> Result<ClientAuth, VfsError> {
    match (cli.workspace_id.as_deref(), cli.workspace_token.clone()) {
        (Some(workspace_id), Some(secret)) => {
            let workspace_id =
                Uuid::parse_str(workspace_id).map_err(|_| VfsError::InvalidArgs {
                    message: "workspace auth requires a valid --workspace-id".to_string(),
                })?;
            return Ok(ClientAuth::WorkspaceBearer {
                workspace_id,
                secret,
            });
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(VfsError::InvalidArgs {
                message: "workspace auth requires both --workspace-id and --workspace-token"
                    .to_string(),
            });
        }
        (None, None) => {}
    }
    if let Some(token) = cli.token.clone() {
        return Ok(ClientAuth::Bearer(token));
    }
    if let Some(user) = cli.user.clone() {
        return Ok(ClientAuth::User(user));
    }
    Ok(ClientAuth::Root)
}

fn resolve_seed_demo_admin_auth(cli: &Cli) -> ClientAuth {
    if let Some(token) = cli.token.clone() {
        return ClientAuth::Bearer(token);
    }
    if let Some(user) = cli.user.clone() {
        return ClientAuth::User(user);
    }
    ClientAuth::Root
}

async fn read_stdin() -> String {
    use tokio::io::AsyncReadExt;

    let mut input = String::new();
    let mut stdin = tokio::io::stdin();
    let _ = stdin.read_to_string(&mut input).await;
    input
}

fn print_json<T>(
    result: Result<T, stratum::error::VfsError>,
) -> Result<(), stratum::error::VfsError>
where
    T: serde::Serialize,
{
    match result {
        Ok(value) => {
            println!("{}", serde_json::to_string_pretty(&value).unwrap());
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};
    use stratum::mount_daemon::{
        MountDaemonBackend, MountDaemonErrorCode, MountDaemonIpcStatus, MountDaemonPaths,
        MountDaemonPidMetadata, MountDaemonState,
    };

    static STRATUM_REPO_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct StratumRepoEnvGuard {
        previous: Option<OsString>,
        _guard: MutexGuard<'static, ()>,
    }

    impl StratumRepoEnvGuard {
        fn set(value: &str) -> Self {
            let guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
            let previous = std::env::var_os("STRATUM_REPO");
            unsafe {
                std::env::set_var("STRATUM_REPO", value);
            }
            Self {
                previous,
                _guard: guard,
            }
        }
    }

    impl Drop for StratumRepoEnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe {
                    std::env::set_var("STRATUM_REPO", value);
                },
                None => unsafe {
                    std::env::remove_var("STRATUM_REPO");
                },
            }
        }
    }

    struct WorkspaceIdEnvGuard {
        previous: Option<OsString>,
        _guard: MutexGuard<'static, ()>,
    }

    impl WorkspaceIdEnvGuard {
        fn set(value: &str) -> Self {
            let guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
            let previous = std::env::var_os("STRATUM_WORKSPACE_ID");
            unsafe {
                std::env::set_var("STRATUM_WORKSPACE_ID", value);
            }
            Self {
                previous,
                _guard: guard,
            }
        }
    }

    impl Drop for WorkspaceIdEnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe {
                    std::env::set_var("STRATUM_WORKSPACE_ID", value);
                },
                None => unsafe {
                    std::env::remove_var("STRATUM_WORKSPACE_ID");
                },
            }
        }
    }

    #[test]
    fn repo_flag_parses_repo_context() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let cli = Cli::try_parse_from(["stratumctl", "--repo", "tenant-a", "ls", "/"]).unwrap();

        assert_eq!(cli.repo.as_deref(), Some("tenant-a"));
    }

    #[test]
    fn repo_env_parses_repo_context() {
        let _repo_env = StratumRepoEnvGuard::set("tenant-env");

        let cli = Cli::try_parse_from(["stratumctl", "ls", "/"]).unwrap();

        assert_eq!(cli.repo.as_deref(), Some("tenant-env"));
    }

    #[test]
    fn partial_workspace_auth_is_rejected_before_broader_auth_fallback() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let workspace_id = Uuid::new_v4();

        let id_only = Cli::try_parse_from([
            "stratumctl",
            "--workspace-id",
            &workspace_id.to_string(),
            "--token",
            "global-token-secret",
            "ls",
            "/",
        ])
        .unwrap();
        let err = resolve_auth(&id_only).expect_err("partial workspace auth must fail closed");
        let VfsError::InvalidArgs { message } = err else {
            panic!("partial workspace auth should return InvalidArgs");
        };
        assert_eq!(
            message,
            "workspace auth requires both --workspace-id and --workspace-token"
        );
        assert!(!message.contains("global-token-secret"));

        let token_only = Cli::try_parse_from([
            "stratumctl",
            "--workspace-token",
            "workspace-secret",
            "--user",
            "root",
            "ls",
            "/",
        ])
        .unwrap();
        let err = resolve_auth(&token_only).expect_err("partial workspace auth must fail closed");
        let VfsError::InvalidArgs { message } = err else {
            panic!("partial workspace auth should return InvalidArgs");
        };
        assert_eq!(
            message,
            "workspace auth requires both --workspace-id and --workspace-token"
        );
        assert!(!message.contains("workspace-secret"));
    }

    #[test]
    fn complete_workspace_auth_resolves_as_workspace_bearer() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let workspace_id = Uuid::new_v4();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "--workspace-id",
            &workspace_id.to_string(),
            "--workspace-token",
            "workspace-secret",
            "ls",
            "/",
        ])
        .unwrap();

        let auth = resolve_auth(&cli).unwrap();

        let ClientAuth::WorkspaceBearer {
            workspace_id: parsed_workspace_id,
            secret,
        } = auth
        else {
            panic!("expected workspace bearer auth");
        };
        assert_eq!(parsed_workspace_id, workspace_id);
        assert_eq!(secret, "workspace-secret");
    }

    #[test]
    fn seed_demo_admin_auth_ignores_stale_workspace_auth() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let workspace_id = Uuid::new_v4();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "--workspace-id",
            &workspace_id.to_string(),
            "--workspace-token",
            "stale-workspace-secret",
            "--user",
            "root",
            "workspace",
            "seed-demo",
            "--agent-token",
            "backing-agent-secret",
        ])
        .unwrap();

        let auth = resolve_seed_demo_admin_auth(&cli);

        assert!(matches!(auth, ClientAuth::User(username) if username == "root"));
    }

    #[test]
    fn invalid_workspace_id_is_rejected_without_leaking_raw_value() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "--workspace-id",
            "not-a-workspace-id-secret",
            "--workspace-token",
            "workspace-secret",
            "ls",
            "/",
        ])
        .unwrap();

        let err = resolve_auth(&cli).expect_err("invalid workspace id must fail closed");
        let VfsError::InvalidArgs { message } = err else {
            panic!("invalid workspace id should return InvalidArgs");
        };
        assert_eq!(message, "workspace auth requires a valid --workspace-id");
        assert!(!message.contains("not-a-workspace-id-secret"));
        assert!(!message.contains("workspace-secret"));
    }

    #[test]
    fn workspace_issue_token_parses_repeated_scope_prefix_flags() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let workspace_id = Uuid::new_v4();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "workspace",
            "issue-token",
            &workspace_id.to_string(),
            "ci-token",
            "agent-secret",
            "--read-prefix",
            "/demo/read",
            "--read-prefix",
            "/demo/shared",
            "--write-prefix",
            "/demo/write",
        ])
        .unwrap();

        let Command::Workspace {
            command:
                WorkspaceCommand::IssueToken {
                    workspace_id: parsed_workspace_id,
                    name,
                    agent_token,
                    read_prefixes,
                    write_prefixes,
                },
        } = cli.command
        else {
            panic!("expected workspace issue-token command");
        };

        assert_eq!(parsed_workspace_id, workspace_id);
        assert_eq!(name, "ci-token");
        assert_eq!(agent_token, "agent-secret");
        assert_eq!(read_prefixes, vec!["/demo/read", "/demo/shared"]);
        assert_eq!(write_prefixes, vec!["/demo/write"]);
    }

    #[test]
    fn workspace_seed_demo_parses_defaults() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "workspace",
            "seed-demo",
            "--agent-token",
            "backing-agent-secret",
        ])
        .unwrap();

        let Command::Workspace {
            command:
                WorkspaceCommand::SeedDemo {
                    agent_token,
                    fixture,
                    name,
                    root_path,
                    token_name,
                    session_ref,
                    env_out,
                },
        } = cli.command
        else {
            panic!("expected workspace seed-demo command");
        };

        assert_eq!(agent_token.as_deref(), Some("backing-agent-secret"));
        assert_eq!(fixture, None);
        assert_eq!(name, None);
        assert_eq!(root_path, None);
        assert_eq!(token_name, None);
        assert_eq!(session_ref, None);
        assert_eq!(env_out, None);
    }

    #[test]
    fn workspace_seed_demo_parses_custom_flags() {
        let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "workspace",
            "seed-demo",
            "--agent-token",
            "backing-agent-secret",
            "--fixture",
            "custom/fixture",
            "--name",
            "custom-demo",
            "--root-path",
            "/demo/custom",
            "--token-name",
            "custom-agent",
            "--session-ref",
            "agent/custom/session",
            "--env-out",
            ".stratum-demo/custom.env",
        ])
        .unwrap();

        let Command::Workspace {
            command:
                WorkspaceCommand::SeedDemo {
                    agent_token,
                    fixture,
                    name,
                    root_path,
                    token_name,
                    session_ref,
                    env_out,
                },
        } = cli.command
        else {
            panic!("expected workspace seed-demo command");
        };

        assert_eq!(agent_token.as_deref(), Some("backing-agent-secret"));
        assert_eq!(fixture, Some(PathBuf::from("custom/fixture")));
        assert_eq!(name.as_deref(), Some("custom-demo"));
        assert_eq!(root_path.as_deref(), Some("/demo/custom"));
        assert_eq!(token_name.as_deref(), Some("custom-agent"));
        assert_eq!(session_ref.as_deref(), Some("agent/custom/session"));
        assert_eq!(env_out, Some(PathBuf::from(".stratum-demo/custom.env")));
    }

    struct AgentTokenEnvGuard {
        previous: Option<OsString>,
        _guard: MutexGuard<'static, ()>,
    }

    impl AgentTokenEnvGuard {
        fn set(value: &str) -> Self {
            let guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
            let previous = std::env::var_os("STRATUM_AGENT_TOKEN");
            unsafe {
                std::env::set_var("STRATUM_AGENT_TOKEN", value);
            }
            Self {
                previous,
                _guard: guard,
            }
        }
    }

    impl Drop for AgentTokenEnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe {
                    std::env::set_var("STRATUM_AGENT_TOKEN", value);
                },
                None => unsafe {
                    std::env::remove_var("STRATUM_AGENT_TOKEN");
                },
            }
        }
    }

    #[test]
    fn workspace_seed_demo_agent_token_env_parses() {
        let _agent_env = AgentTokenEnvGuard::set("env-backing-agent-secret");
        let cli = Cli::try_parse_from(["stratumctl", "workspace", "seed-demo"]).unwrap();

        let Command::Workspace {
            command: WorkspaceCommand::SeedDemo { agent_token, .. },
        } = cli.command
        else {
            panic!("expected workspace seed-demo command");
        };

        assert_eq!(agent_token.as_deref(), Some("env-backing-agent-secret"));
    }

    #[test]
    fn mount_status_command_parses_default_tag() {
        let cli = Cli::try_parse_from(["stratumctl", "mount", "status"]).unwrap();

        let Command::Mount {
            command: MountCommand::Status {
                tag, runtime_dir, ..
            },
        } = cli.command
        else {
            panic!("expected mount status command");
        };

        assert_eq!(tag, DEFAULT_MOUNT_TAG);
        assert_eq!(runtime_dir, None);
    }

    #[test]
    fn mount_status_command_parses_local_flags() {
        let cli = Cli::try_parse_from([
            "stratumctl",
            "mount",
            "status",
            "--tag",
            "repo-main",
            "--runtime-dir",
            "/tmp/stratum-runtime",
            "--json",
        ])
        .unwrap();

        let Command::Mount {
            command:
                MountCommand::Status {
                    tag,
                    runtime_dir,
                    json,
                },
        } = cli.command
        else {
            panic!("expected mount status command");
        };

        assert_eq!(tag, "repo-main");
        assert_eq!(runtime_dir, Some(PathBuf::from("/tmp/stratum-runtime")));
        assert!(json);
    }

    #[test]
    fn mount_logs_command_parses_local_flags() {
        let cli = Cli::try_parse_from([
            "stratumctl",
            "mount",
            "logs",
            "--tag",
            "repo-main",
            "--runtime-dir",
            "/tmp/stratum-runtime",
            "--lines",
            "12",
            "--json",
        ])
        .unwrap();

        let Command::Mount {
            command:
                MountCommand::Logs {
                    tag,
                    runtime_dir,
                    lines,
                    json,
                },
        } = cli.command
        else {
            panic!("expected mount logs command");
        };

        assert_eq!(tag, "repo-main");
        assert_eq!(runtime_dir, Some(PathBuf::from("/tmp/stratum-runtime")));
        assert_eq!(lines, 12);
        assert!(json);
    }

    #[test]
    fn mount_unmount_command_parses_defaults_and_local_flags() {
        let cli = Cli::try_parse_from([
            "stratumctl",
            "mount",
            "unmount",
            "--runtime-dir",
            "/tmp/stratum-runtime",
            "--json",
        ])
        .unwrap();

        let Command::Mount {
            command:
                MountCommand::Unmount {
                    tag,
                    runtime_dir,
                    json,
                },
        } = cli.command
        else {
            panic!("expected mount unmount command");
        };

        assert_eq!(tag, DEFAULT_MOUNT_TAG);
        assert_eq!(runtime_dir, Some(PathBuf::from("/tmp/stratum-runtime")));
        assert!(json);
    }

    #[test]
    fn mount_logs_command_rejects_lines_outside_bounds() {
        for lines in ["0", "201"] {
            let result = Cli::try_parse_from(["stratumctl", "mount", "logs", "--lines", lines]);
            assert!(result.is_err(), "lines={lines} should be rejected");
        }

        let cli = Cli::try_parse_from(["stratumctl", "mount", "logs", "--lines", "200"]).unwrap();
        let Command::Mount {
            command: MountCommand::Logs { lines, .. },
        } = cli.command
        else {
            panic!("expected mount logs command");
        };
        assert_eq!(lines, 200);
    }

    #[test]
    fn mount_status_command_renders_redacted_json() {
        let controller = fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Missing),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                tail_logs: Err(MountDaemonErrorCode::Unavailable),
            },
            FakeIpcClient::default(),
        );

        let status_output = render_mount_command(
            &MountCommand::Status {
                tag: "secret-tag".to_string(),
                runtime_dir: Some(PathBuf::from("/tmp/secret-runtime")),
                json: true,
            },
            &controller,
        )
        .unwrap();

        assert!(status_output.contains("\"tag\": \"<redacted>\""));
        assert!(status_output.contains("\"state\": \"stopped\""));
        assert!(!status_output.contains("secret-tag"));
        assert!(!status_output.contains("/tmp/secret-runtime"));
    }

    #[test]
    fn mount_logs_command_renders_redacted_text_and_json() {
        let controller = fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore {
                pid: Ok(MountDaemonPidMetadata::Missing),
                socket_exists: Ok(false),
                log_exists: Ok(true),
                tail_logs: Ok([
                    "Authorization: Bearer raw-secret-token",
                    "tag=secret-tag path=/safe-runtime ready",
                ]
                .join("\n")),
            },
            FakeIpcClient::default(),
        );

        let log_text = render_mount_command(
            &MountCommand::Logs {
                tag: "secret-tag".to_string(),
                runtime_dir: Some(PathBuf::from("/safe-runtime")),
                lines: 2,
                json: false,
            },
            &controller,
        )
        .unwrap();
        assert!(log_text.contains("tag=<redacted> path=<redacted> ready"));
        assert!(!log_text.contains("raw-secret-token"));
        assert!(!log_text.contains("secret-tag"));
        assert!(!log_text.contains("/safe-runtime"));

        let log_json = render_mount_command(
            &MountCommand::Logs {
                tag: "secret-tag".to_string(),
                runtime_dir: Some(PathBuf::from("/safe-runtime")),
                lines: 2,
                json: true,
            },
            &controller,
        )
        .unwrap();
        assert!(log_json.contains("\"lines\""));
        assert!(log_json.contains("ready"));
        assert!(!log_json.contains("raw-secret-token"));
        assert!(!log_json.contains("secret-tag"));
        assert!(!log_json.contains("/safe-runtime"));
    }

    #[test]
    fn mount_logs_command_renders_unavailable_logs() {
        let output = render_mount_command(
            &MountCommand::Logs {
                tag: DEFAULT_MOUNT_TAG.to_string(),
                runtime_dir: Some(PathBuf::from("/tmp/stratum-runtime")),
                lines: 10,
                json: false,
            },
            &fake_controller_default(),
        )
        .unwrap();

        assert_eq!(output, "mount daemon logs unavailable\n");
    }

    #[test]
    fn mount_unmount_command_renders_redacted_outcomes() {
        for outcome in [
            MountDaemonUnmountOutcome::NotRunning,
            MountDaemonUnmountOutcome::StaleCleaned,
            MountDaemonUnmountOutcome::Requested,
            MountDaemonUnmountOutcome::AlreadyUnmounting,
            MountDaemonUnmountOutcome::Crashed,
            MountDaemonUnmountOutcome::Unavailable,
        ] {
            let output = render_mount_unmount(outcome, false);

            assert!(output.contains("mount daemon unmount: outcome="));
            assert!(output.contains("tag=<redacted>"));
        }
    }

    #[test]
    fn local_mount_commands_do_not_require_http_auth_resolution() {
        let workspace_id = Uuid::new_v4();
        let cli = Cli::try_parse_from([
            "stratumctl",
            "--workspace-id",
            &workspace_id.to_string(),
            "mount",
            "status",
            "--runtime-dir",
            "/tmp/stratum-runtime",
        ])
        .unwrap();

        assert!(resolve_auth(&cli).is_err());
        let Command::Mount { command } = &cli.command else {
            panic!("expected mount command");
        };

        let output = render_mount_command(command, &fake_controller_default()).unwrap();

        assert!(output.contains("mount daemon:"));
        assert!(!output.contains(&workspace_id.to_string()));
    }

    #[test]
    fn local_mount_commands_ignore_malformed_workspace_id_env() {
        let _workspace_id_env = WorkspaceIdEnvGuard::set("not-a-workspace-id-secret");

        let cli = Cli::try_parse_from([
            "stratumctl",
            "mount",
            "status",
            "--runtime-dir",
            "/tmp/stratum-runtime",
        ])
        .unwrap();

        let Command::Mount { command } = &cli.command else {
            panic!("expected mount command");
        };
        let output = render_mount_command(command, &fake_controller_default()).unwrap();

        assert!(output.contains("mount daemon:"));
        assert!(!output.contains("not-a-workspace-id-secret"));
    }

    fn fake_controller_default()
    -> MountDaemonController<FakeProcessProbe, FakeFileStore, FakeIpcClient> {
        fake_controller(
            FakeProcessProbe::default(),
            FakeFileStore::default(),
            FakeIpcClient::default(),
        )
    }

    fn fake_controller(
        process_probe: FakeProcessProbe,
        file_store: FakeFileStore,
        ipc_client: FakeIpcClient,
    ) -> MountDaemonController<FakeProcessProbe, FakeFileStore, FakeIpcClient> {
        MountDaemonController::new(process_probe, file_store, ipc_client)
    }

    #[derive(Default)]
    struct FakeProcessProbe {
        alive: bool,
    }

    impl MountDaemonProcessProbe for FakeProcessProbe {
        fn is_alive(&self, _pid: u32) -> Result<bool, MountDaemonError> {
            Ok(self.alive)
        }
    }

    struct FakeFileStore {
        pid: Result<MountDaemonPidMetadata, MountDaemonErrorCode>,
        socket_exists: Result<bool, MountDaemonErrorCode>,
        log_exists: Result<bool, MountDaemonErrorCode>,
        tail_logs: Result<String, MountDaemonErrorCode>,
    }

    impl Default for FakeFileStore {
        fn default() -> Self {
            Self {
                pid: Ok(MountDaemonPidMetadata::Missing),
                socket_exists: Ok(false),
                log_exists: Ok(false),
                tail_logs: Err(MountDaemonErrorCode::Unavailable),
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
            Ok(())
        }

        fn tail_logs(
            &self,
            _paths: &MountDaemonPaths,
            _max_bytes: usize,
        ) -> Result<String, MountDaemonError> {
            self.tail_logs.clone().map_err(MountDaemonError::new)
        }
    }

    struct FakeIpcClient {
        unmount_requests: Cell<u8>,
    }

    impl Default for FakeIpcClient {
        fn default() -> Self {
            Self {
                unmount_requests: Cell::new(0),
            }
        }
    }

    impl MountDaemonIpcClient for FakeIpcClient {
        fn status(
            &self,
            _paths: &MountDaemonPaths,
        ) -> Result<MountDaemonIpcStatus, MountDaemonError> {
            Ok(MountDaemonIpcStatus {
                backend: MountDaemonBackend::default(),
                state: MountDaemonState::Running,
                uptime: None,
                hydration_progress: None,
            })
        }

        fn logs(
            &self,
            _paths: &MountDaemonPaths,
            _max_bytes: usize,
        ) -> Result<String, MountDaemonError> {
            Err(MountDaemonError::new(MountDaemonErrorCode::Ipc))
        }

        fn unmount(&self, _paths: &MountDaemonPaths) -> Result<(), MountDaemonError> {
            self.unmount_requests.set(self.unmount_requests.get() + 1);
            Ok(())
        }
    }
}
