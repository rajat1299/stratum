use clap::{Parser, Subcommand};
use stratum::client::{ClientAuth, StratumClient};
use stratum::error::VfsError;
use uuid::Uuid;

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
    workspace_id: Option<Uuid>,

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
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let auth = match resolve_auth(&cli) {
        Ok(auth) => auth,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };
    let client = StratumClient::new(cli.url, auth).with_repo(cli.repo);

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
        Command::Workspace { command } => match command {
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
                        workspace_id,
                        &name,
                        &agent_token,
                        (!read_prefixes.is_empty()).then_some(read_prefixes),
                        (!write_prefixes.is_empty()).then_some(write_prefixes),
                    )
                    .await,
            ),
        },
    };

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn resolve_auth(cli: &Cli) -> Result<ClientAuth, VfsError> {
    match (cli.workspace_id, cli.workspace_token.clone()) {
        (Some(workspace_id), Some(secret)) => {
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
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};

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
}
