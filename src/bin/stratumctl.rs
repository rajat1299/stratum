use clap::{Parser, Subcommand};
use stratum::client::{ClientAuth, StratumClient};
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
    let auth = resolve_auth(&cli);
    let client = StratumClient::new(cli.url, auth);

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

fn resolve_auth(cli: &Cli) -> ClientAuth {
    if let (Some(workspace_id), Some(secret)) = (cli.workspace_id, cli.workspace_token.clone()) {
        return ClientAuth::WorkspaceBearer {
            workspace_id,
            secret,
        };
    }
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

    #[test]
    fn workspace_issue_token_parses_repeated_scope_prefix_flags() {
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
