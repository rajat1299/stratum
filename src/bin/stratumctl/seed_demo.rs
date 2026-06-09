use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use stratum::client::{ClientAuth, StratumClient};
use stratum::error::VfsError;
use uuid::Uuid;

const DEFAULT_SEED_FIXTURE: &str = "examples/incident-workspace";
const DEFAULT_SEED_WORKSPACE_NAME: &str = "incident-demo";
const DEFAULT_SEED_ROOT_PATH: &str = "/demo/incident-workspace";
const DEFAULT_SEED_TOKEN_NAME: &str = "incident-demo-agent";
const DEFAULT_SEED_SESSION_REF: &str = "agent/incident-demo/session";
const DEFAULT_SEED_ENV_OUT: &str = ".stratum-demo/incident-workspace.env";

const SEED_DEMO_FILES: &[(&str, &str)] = &[
    (
        "incidents/checkout-latency/timeline.md",
        "/incidents/checkout-latency/timeline.md",
    ),
    (
        "incidents/checkout-latency/evidence.md",
        "/incidents/checkout-latency/evidence.md",
    ),
    (
        "incidents/checkout-latency/hypotheses.md",
        "/incidents/checkout-latency/hypotheses.md",
    ),
    (
        "runbooks/payment-service.md",
        "/runbooks/payment-service.md",
    ),
    (
        "memory/agents/researcher.md",
        "/memory/agents/researcher.md",
    ),
];

pub(crate) struct SeedDemoContext {
    pub(crate) url: String,
    pub(crate) auth: ClientAuth,
    pub(crate) repo: Option<String>,
    pub(crate) command_name: String,
}

pub(crate) struct SeedDemoOptions {
    pub(crate) agent_token: Option<String>,
    pub(crate) fixture: Option<PathBuf>,
    pub(crate) name: Option<String>,
    pub(crate) root_path: Option<String>,
    pub(crate) token_name: Option<String>,
    pub(crate) session_ref: Option<String>,
    pub(crate) env_out: Option<PathBuf>,
}

struct ResolvedSeedDemoOptions {
    agent_token: String,
    fixture: PathBuf,
    workspace_name: String,
    root_path: String,
    token_name: String,
    session_ref: String,
    env_out: PathBuf,
}

struct SeedDemoEnvVars<'a> {
    url: &'a str,
    workspace_id: Uuid,
    workspace_token: &'a str,
    repo: Option<&'a str>,
}

struct SeedDemoEnvFile {
    path: PathBuf,
    file: Option<File>,
    keep: bool,
}

fn resolve_seed_demo_options(
    options: SeedDemoOptions,
) -> Result<ResolvedSeedDemoOptions, VfsError> {
    let agent_token = options
        .agent_token
        .filter(|value| !value.is_empty())
        .ok_or(VfsError::InvalidArgs {
            message: "workspace seed-demo requires --agent-token or STRATUM_AGENT_TOKEN"
                .to_string(),
        })?;

    Ok(ResolvedSeedDemoOptions {
        agent_token,
        fixture: options
            .fixture
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SEED_FIXTURE)),
        workspace_name: options
            .name
            .unwrap_or_else(|| DEFAULT_SEED_WORKSPACE_NAME.to_string()),
        root_path: options
            .root_path
            .unwrap_or_else(|| DEFAULT_SEED_ROOT_PATH.to_string()),
        token_name: options
            .token_name
            .unwrap_or_else(|| DEFAULT_SEED_TOKEN_NAME.to_string()),
        session_ref: options
            .session_ref
            .unwrap_or_else(|| DEFAULT_SEED_SESSION_REF.to_string()),
        env_out: options
            .env_out
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SEED_ENV_OUT)),
    })
}

fn shell_env_value(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if !value.contains('\'') {
        return format!("'{value}'");
    }
    format!(
        "'{}'",
        value.split('\'').collect::<Vec<_>>().join("'\"'\"'")
    )
}

fn seed_demo_env_contents(vars: SeedDemoEnvVars<'_>) -> String {
    let mut contents = String::new();
    contents.push_str(&format!("STRATUM_URL={}\n", shell_env_value(vars.url)));
    contents.push_str(&format!(
        "STRATUM_WORKSPACE_ID={}\n",
        shell_env_value(&vars.workspace_id.to_string())
    ));
    contents.push_str(&format!(
        "STRATUM_WORKSPACE_TOKEN={}\n",
        shell_env_value(vars.workspace_token)
    ));
    if let Some(repo) = vars.repo {
        contents.push_str(&format!("STRATUM_REPO={}\n", shell_env_value(repo)));
    }
    contents
}

fn create_seed_demo_env_file(env_out: &Path) -> Result<File, VfsError> {
    if let Some(parent) = env_out.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(VfsError::IoError)?;
        }
    }

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(env_out).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            VfsError::InvalidArgs {
                message: format!(
                    "refusing to overwrite existing env file at {}",
                    env_out.display()
                ),
            }
        } else {
            VfsError::IoError(error)
        }
    })
}

impl SeedDemoEnvFile {
    fn reserve(path: &Path) -> Result<Self, VfsError> {
        Ok(Self {
            path: path.to_path_buf(),
            file: Some(create_seed_demo_env_file(path)?),
            keep: false,
        })
    }

    fn write(&mut self, vars: SeedDemoEnvVars<'_>) -> Result<(), VfsError> {
        self.file
            .as_mut()
            .expect("reserved env file handle must be present")
            .write_all(seed_demo_env_contents(vars).as_bytes())
            .map_err(VfsError::IoError)?;
        Ok(())
    }

    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for SeedDemoEnvFile {
    fn drop(&mut self) {
        if !self.keep {
            drop(self.file.take());
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
fn write_seed_demo_env_file(env_out: &Path, vars: SeedDemoEnvVars<'_>) -> Result<(), VfsError> {
    let mut env_file = SeedDemoEnvFile::reserve(env_out)?;
    env_file.write(vars)?;
    env_file.keep();
    Ok(())
}

fn render_seed_demo_next_commands(env_out: &Path, command_name: &str) -> String {
    let env_out = shell_env_value(&env_out.display().to_string());
    let command_name = shell_env_value(command_name);
    format!(
        "source {env_out}\n\
         {command_name} tree /\n\
         {command_name} grep timeout /\n\
         {command_name} write /incidents/checkout-latency/root-cause.md --stdin\n"
    )
}

fn collect_seed_demo_files(fixture: &Path) -> Result<Vec<(String, String)>, VfsError> {
    let mut files = Vec::with_capacity(SEED_DEMO_FILES.len());
    for (relative_path, workspace_path) in SEED_DEMO_FILES {
        let source = fixture.join(relative_path);
        let content = std::fs::read_to_string(&source).map_err(|e| VfsError::InvalidArgs {
            message: format!("failed to read seed fixture {}: {e}", source.display()),
        })?;
        files.push(((*workspace_path).to_string(), content));
    }
    Ok(files)
}

fn seed_demo_parent_dirs(files: &[(String, String)]) -> Vec<String> {
    let mut dirs = BTreeSet::new();
    for (path, _) in files {
        if let Some(parent) = Path::new(path).parent() {
            let parent = parent.to_string_lossy();
            if parent != "/" && !parent.is_empty() {
                dirs.insert(parent.to_string());
            }
        }
    }
    dirs.into_iter().collect()
}

async fn ensure_seed_demo_local_state(client: &StratumClient) -> Result<(), VfsError> {
    let capabilities = client.capabilities().await?;
    let core_runtime = capabilities.server.core_runtime;
    if core_runtime == "local-state" {
        return Ok(());
    }

    Err(VfsError::InvalidArgs {
        message: format!(
            "workspace seed-demo is local-state only; target reports core_runtime={core_runtime}"
        ),
    })
}

pub(crate) async fn run_workspace_seed_demo(
    context: SeedDemoContext,
    options: SeedDemoOptions,
) -> Result<String, VfsError> {
    let options = resolve_seed_demo_options(options)?;
    let seed_files = collect_seed_demo_files(&options.fixture)?;
    let mut env_file = SeedDemoEnvFile::reserve(&options.env_out)?;

    let admin_client = StratumClient::new(context.url.clone(), context.auth.clone())
        .with_repo(context.repo.clone());
    ensure_seed_demo_local_state(&admin_client).await?;

    let workspace = admin_client
        .create_workspace_with_refs(
            &options.workspace_name,
            &options.root_path,
            None,
            Some(&options.session_ref),
        )
        .await?;
    admin_client.mkdir_p(&options.root_path).await?;

    let issued = admin_client
        .issue_scoped_workspace_token_parsed(
            workspace.id,
            &options.token_name,
            &options.agent_token,
            None,
            None,
        )
        .await?;

    if let Err(err) = env_file.write(SeedDemoEnvVars {
        url: &context.url,
        workspace_id: workspace.id,
        workspace_token: &issued.workspace_token,
        repo: context.repo.as_deref(),
    }) {
        let _ = admin_client
            .revoke_workspace_token(workspace.id, issued.token_id)
            .await;
        return Err(err);
    }

    let workspace_client = StratumClient::new(
        context.url,
        ClientAuth::WorkspaceBearer {
            workspace_id: workspace.id,
            secret: issued.workspace_token,
        },
    )
    .with_repo(context.repo);

    for dir in seed_demo_parent_dirs(&seed_files) {
        if let Err(err) = workspace_client.mkdir_p(&dir).await {
            let _ = admin_client
                .revoke_workspace_token(workspace.id, issued.token_id)
                .await;
            return Err(err);
        }
    }
    for (path, content) in seed_files {
        if let Err(err) = workspace_client.write_file(&path, content).await {
            let _ = admin_client
                .revoke_workspace_token(workspace.id, issued.token_id)
                .await;
            return Err(err);
        }
    }
    env_file.keep();

    Ok(render_seed_demo_next_commands(
        &options.env_out,
        &context.command_name,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Json;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    fn capabilities_fixture(path: &str) -> serde_json::Value {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
        serde_json::from_str(&std::fs::read_to_string(path).expect("read capabilities fixture"))
            .expect("capabilities fixture is json")
    }

    fn local_capabilities_fixture() -> serde_json::Value {
        capabilities_fixture("sdk/contracts/capabilities.v1.json")
    }

    fn durable_capabilities_fixture() -> serde_json::Value {
        capabilities_fixture("sdk/contracts/capabilities.v1.durable-cloud.json")
    }

    fn seed_context(url: String) -> SeedDemoContext {
        SeedDemoContext {
            url,
            auth: ClientAuth::User("root".to_string()),
            repo: None,
            command_name: "stratumctl".to_string(),
        }
    }

    fn write_fixture_tree() -> PathBuf {
        let fixture_dir =
            std::env::temp_dir().join(format!("stratum-seed-fixture-{}", Uuid::new_v4()));
        for (relative_path, _workspace_path) in SEED_DEMO_FILES {
            let source = fixture_dir.join(relative_path);
            if let Some(parent) = source.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&source, format!("seed content for {relative_path}")).unwrap();
        }
        fixture_dir
    }

    #[test]
    fn resolve_seed_demo_options_applies_defaults() {
        let resolved = resolve_seed_demo_options(SeedDemoOptions {
            agent_token: Some("backing-agent-secret".to_string()),
            fixture: None,
            name: None,
            root_path: None,
            token_name: None,
            session_ref: None,
            env_out: None,
        })
        .unwrap();

        assert_eq!(resolved.fixture, PathBuf::from(DEFAULT_SEED_FIXTURE));
        assert_eq!(resolved.workspace_name, DEFAULT_SEED_WORKSPACE_NAME);
        assert_eq!(resolved.root_path, DEFAULT_SEED_ROOT_PATH);
        assert_eq!(resolved.token_name, DEFAULT_SEED_TOKEN_NAME);
        assert_eq!(resolved.session_ref, DEFAULT_SEED_SESSION_REF);
        assert_eq!(resolved.env_out, PathBuf::from(DEFAULT_SEED_ENV_OUT));
    }

    #[test]
    fn shell_env_value_quotes_special_characters() {
        assert_eq!(shell_env_value("plain"), "'plain'");
        assert_eq!(shell_env_value(""), "''");
        assert_eq!(shell_env_value("it's"), "'it'\"'\"'s'");
    }

    #[test]
    fn seed_demo_env_file_contains_workspace_token_only_in_file() {
        let dir = std::env::temp_dir().join(format!("stratum-seed-demo-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let env_out = dir.join("demo.env");
        let workspace_id = Uuid::new_v4();

        write_seed_demo_env_file(
            &env_out,
            SeedDemoEnvVars {
                url: "http://127.0.0.1:3000",
                workspace_id,
                workspace_token: "issued-workspace-secret",
                repo: Some("tenant-a"),
            },
        )
        .unwrap();

        let contents = std::fs::read_to_string(&env_out).unwrap();
        assert!(contents.contains("STRATUM_URL='http://127.0.0.1:3000'"));
        assert!(contents.contains(&format!("STRATUM_WORKSPACE_ID='{workspace_id}'")));
        assert!(contents.contains("STRATUM_WORKSPACE_TOKEN='issued-workspace-secret'"));
        assert!(contents.contains("STRATUM_REPO='tenant-a'"));
        assert!(!contents.contains("backing-agent-secret"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&env_out).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn seed_demo_next_commands_reference_env_file_not_raw_tokens() {
        let output = render_seed_demo_next_commands(
            Path::new(".stratum-demo/incident-workspace.env"),
            "stratumctl",
        );

        assert!(output.contains("source '.stratum-demo/incident-workspace.env'"));
        assert!(output.contains("'stratumctl' tree /"));
        assert!(output.contains("'stratumctl' grep timeout /"));
        assert!(
            output.contains("'stratumctl' write /incidents/checkout-latency/root-cause.md --stdin")
        );
        assert!(!output.contains("issued-workspace-secret"));
        assert!(!output.contains("backing-agent-secret"));
        assert!(!output.contains("--workspace-token"));
    }

    #[test]
    fn seed_demo_next_commands_shell_quote_env_path() {
        let output = render_seed_demo_next_commands(
            Path::new(".stratum demo/incident env"),
            "/tmp/stratum ctl",
        );

        assert!(output.contains("source '.stratum demo/incident env'"));
        assert!(output.contains("'/tmp/stratum ctl' tree /"));
    }

    #[cfg(unix)]
    #[test]
    fn seed_demo_env_file_refuses_dangling_symlink() {
        let dir = std::env::temp_dir().join(format!("stratum-seed-symlink-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let env_out = dir.join("demo.env");
        let symlink_target = dir.join("target.env");
        std::os::unix::fs::symlink(&symlink_target, &env_out).unwrap();

        let err = write_seed_demo_env_file(
            &env_out,
            SeedDemoEnvVars {
                url: "http://127.0.0.1:3000",
                workspace_id: Uuid::new_v4(),
                workspace_token: "issued-workspace-secret",
                repo: None,
            },
        )
        .expect_err("env writer must not follow symlinks for token material");

        assert!(!err.to_string().contains("issued-workspace-secret"));
        assert!(!symlink_target.exists());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn seed_demo_stdout_redacts_secrets_and_writes_env_file_only() {
        use axum::Router;
        use axum::extract::{Path, State};
        use axum::routing::{post, put};

        let workspace_id = Uuid::new_v4();
        let token_issue_count = Arc::new(Mutex::new(0));
        let token_issue_count_for_handler = token_issue_count.clone();
        let created_dirs = Arc::new(Mutex::new(HashSet::<String>::new()));
        let created_dirs_for_handler = created_dirs.clone();
        let workspace_root_created = Arc::new(Mutex::new(false));
        let workspace_root_created_for_handler = workspace_root_created.clone();
        let app = Router::new()
            .route(
                "/v1/capabilities",
                axum::routing::get(|| async { Json(local_capabilities_fixture()) }),
            )
            .route(
                "/workspaces",
                post({
                    move |Json(body): Json<serde_json::Value>| async move {
                        Json(serde_json::json!({
                            "id": workspace_id,
                            "name": body.get("name").and_then(|value| value.as_str()).unwrap_or_default(),
                            "root_path": body.get("root_path").and_then(|value| value.as_str()).unwrap_or_default(),
                            "base_ref": "main",
                            "session_ref": body.get("session_ref").and_then(|value| value.as_str()),
                        }))
                    }
                }),
            )
            .route(
                "/workspaces/{workspace_id}/tokens",
                post({
                    move |Path(_): Path<Uuid>, Json(_body): Json<serde_json::Value>| {
                        let token_issue_count = token_issue_count_for_handler.clone();
                        async move {
                            *token_issue_count.lock().unwrap() += 1;
                            Json(serde_json::json!({
                                "workspace_id": workspace_id,
                                "token_id": "22222222-2222-2222-2222-222222222222",
                                "workspace_token": "issued-workspace-secret",
                                "name": "incident-demo-agent",
                            }))
                        }
                    }
                }),
            )
            .route(
                "/fs/{*path}",
                put(
                    move |State(created_dirs): State<Arc<Mutex<HashSet<String>>>>,
                     Path(path): Path<String>,
                     headers: axum::http::HeaderMap,
                     body: String| async move {
                        let path = format!("/{}", path.trim_start_matches('/'));
                        if headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            == Some("User root")
                            && headers
                                .get("x-stratum-type")
                                .and_then(|value| value.to_str().ok())
                                == Some("directory")
                            && path == DEFAULT_SEED_ROOT_PATH
                        {
                            *workspace_root_created_for_handler.lock().unwrap() = true;
                            return (
                                axum::http::StatusCode::OK,
                                Json(serde_json::json!({ "created": path, "type": "directory" })),
                            );
                        }
                        if headers.get("authorization").and_then(|value| value.to_str().ok())
                            != Some("Bearer issued-workspace-secret")
                        {
                            return (
                                axum::http::StatusCode::UNAUTHORIZED,
                                Json(serde_json::json!({ "error": "missing workspace bearer" })),
                            );
                        }
                        if headers
                            .get("x-stratum-workspace")
                            .and_then(|value| value.to_str().ok())
                            != Some(&workspace_id.to_string())
                        {
                            return (
                                axum::http::StatusCode::UNAUTHORIZED,
                                Json(serde_json::json!({ "error": "missing workspace id" })),
                            );
                        }
                        if !*workspace_root_created_for_handler.lock().unwrap() {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({ "error": "missing workspace root" })),
                            );
                        }
                        if headers
                            .get("x-stratum-type")
                            .and_then(|value| value.to_str().ok())
                            == Some("directory")
                        {
                            created_dirs.lock().unwrap().insert(path.clone());
                            return (
                                axum::http::StatusCode::OK,
                                Json(serde_json::json!({ "created": path, "type": "directory" })),
                            );
                        }

                        let parent = std::path::Path::new(&path)
                            .parent()
                            .and_then(std::path::Path::to_str)
                            .unwrap_or("/");
                        if parent != "/" && !created_dirs.lock().unwrap().contains(parent) {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(serde_json::json!({ "error": "missing parent" })),
                            );
                        }
                        (
                            axum::http::StatusCode::OK,
                            Json(serde_json::json!({ "path": path, "bytes": body.len() })),
                        )
                    },
                ),
            )
            .with_state(created_dirs_for_handler);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let fixture_dir = write_fixture_tree();
        let env_dir = std::env::temp_dir().join(format!("stratum-seed-env-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&env_dir).unwrap();
        let env_out = env_dir.join("demo.env");

        let stdout = run_workspace_seed_demo(
            seed_context(format!("http://{addr}")),
            SeedDemoOptions {
                agent_token: Some("backing-agent-secret".to_string()),
                fixture: Some(fixture_dir.clone()),
                name: None,
                root_path: None,
                token_name: None,
                session_ref: None,
                env_out: Some(env_out.clone()),
            },
        )
        .await
        .unwrap();
        server.abort();

        assert!(!stdout.contains("backing-agent-secret"));
        assert!(!stdout.contains("issued-workspace-secret"));
        assert!(stdout.contains("source "));
        assert!(stdout.contains("'stratumctl' tree /"));
        assert!(*workspace_root_created.lock().unwrap());
        assert!(
            created_dirs
                .lock()
                .unwrap()
                .contains("/incidents/checkout-latency")
        );
        assert!(created_dirs.lock().unwrap().contains("/runbooks"));
        assert!(created_dirs.lock().unwrap().contains("/memory/agents"));

        let env_contents = std::fs::read_to_string(&env_out).unwrap();
        assert!(env_contents.contains("issued-workspace-secret"));
        assert!(!env_contents.contains("backing-agent-secret"));

        let _ = std::fs::remove_dir_all(fixture_dir);
        let _ = std::fs::remove_dir_all(env_dir);
    }

    #[tokio::test]
    async fn seed_demo_reads_fixtures_before_remote_mutation() {
        use axum::Router;
        use axum::routing::post;

        let workspace_create_count = Arc::new(Mutex::new(0));
        let workspace_create_count_for_handler = workspace_create_count.clone();
        let app = Router::new()
            .route(
                "/v1/capabilities",
                axum::routing::get(|| async { Json(local_capabilities_fixture()) }),
            )
            .route(
                "/workspaces",
                post(move |Json(_body): Json<serde_json::Value>| {
                    let workspace_create_count = workspace_create_count_for_handler.clone();
                    async move {
                        *workspace_create_count.lock().unwrap() += 1;
                        Json(serde_json::json!({
                            "id": Uuid::new_v4(),
                            "name": "incident-demo",
                            "root_path": "/demo/incident-workspace",
                            "base_ref": "main",
                            "session_ref": "agent/incident-demo/session",
                        }))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let env_dir = std::env::temp_dir().join(format!("stratum-seed-env-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&env_dir).unwrap();
        let env_out = env_dir.join("demo.env");

        let err = run_workspace_seed_demo(
            seed_context(format!("http://{addr}")),
            SeedDemoOptions {
                agent_token: Some("backing-agent-secret".to_string()),
                fixture: Some(env_dir.join("missing-fixtures")),
                name: None,
                root_path: None,
                token_name: None,
                session_ref: None,
                env_out: Some(env_out.clone()),
            },
        )
        .await
        .expect_err("missing fixtures must fail before remote mutation");
        server.abort();

        let VfsError::InvalidArgs { message } = err else {
            panic!("missing fixture should return InvalidArgs");
        };
        assert!(message.contains("failed to read seed fixture"));
        assert_eq!(*workspace_create_count.lock().unwrap(), 0);
        assert!(!env_out.exists());

        let _ = std::fs::remove_dir_all(env_dir);
    }

    #[tokio::test]
    async fn seed_demo_rejects_durable_cloud_before_remote_mutation() {
        use axum::Router;
        use axum::routing::post;

        let workspace_create_count = Arc::new(Mutex::new(0));
        let workspace_create_count_for_handler = workspace_create_count.clone();
        let app = Router::new()
            .route(
                "/v1/capabilities",
                axum::routing::get(|| async { Json(durable_capabilities_fixture()) }),
            )
            .route(
                "/workspaces",
                post(move |Json(_body): Json<serde_json::Value>| {
                    let workspace_create_count = workspace_create_count_for_handler.clone();
                    async move {
                        *workspace_create_count.lock().unwrap() += 1;
                        Json(serde_json::json!({
                            "id": Uuid::new_v4(),
                            "name": "incident-demo",
                            "root_path": "/demo/incident-workspace",
                            "base_ref": "main",
                            "session_ref": "agent/incident-demo/session",
                        }))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let fixture_dir = write_fixture_tree();
        let env_dir = std::env::temp_dir().join(format!("stratum-seed-env-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&env_dir).unwrap();
        let mut context = seed_context(format!("http://{addr}"));
        context.repo = Some("tenant-a".to_string());

        let err = run_workspace_seed_demo(
            context,
            SeedDemoOptions {
                agent_token: Some("backing-agent-secret".to_string()),
                fixture: Some(fixture_dir.clone()),
                name: None,
                root_path: None,
                token_name: None,
                session_ref: None,
                env_out: Some(env_dir.join("demo.env")),
            },
        )
        .await
        .expect_err("seed-demo must be local-only");
        server.abort();

        let VfsError::InvalidArgs { message } = err else {
            panic!("durable guard should return InvalidArgs");
        };
        assert!(message.contains("local-state"));
        assert!(!message.contains("backing-agent-secret"));
        assert_eq!(*workspace_create_count.lock().unwrap(), 0);

        let _ = std::fs::remove_dir_all(fixture_dir);
        let _ = std::fs::remove_dir_all(env_dir);
    }

    #[tokio::test]
    async fn seed_demo_refuses_existing_env_file_before_token_issue() {
        use axum::Router;
        use axum::extract::Path;
        use axum::routing::post;

        let workspace_id = Uuid::new_v4();
        let token_issue_count = Arc::new(Mutex::new(0));
        let token_issue_count_for_handler = token_issue_count.clone();
        let app = Router::new()
            .route(
                "/workspaces",
                post({
                    move |Json(_body): Json<serde_json::Value>| async move {
                        Json(serde_json::json!({
                            "id": workspace_id,
                            "name": "incident-demo",
                            "root_path": "/demo/incident-workspace",
                            "base_ref": "main",
                            "session_ref": "agent/incident-demo/session",
                        }))
                    }
                }),
            )
            .route(
                "/workspaces/{workspace_id}/tokens",
                post({
                    move |Path(_): Path<Uuid>, Json(_body): Json<serde_json::Value>| {
                        let token_issue_count = token_issue_count_for_handler.clone();
                        async move {
                            *token_issue_count.lock().unwrap() += 1;
                            Json(serde_json::json!({
                                "workspace_id": workspace_id,
                                "token_id": "22222222-2222-2222-2222-222222222222",
                                "workspace_token": "issued-workspace-secret",
                                "name": "incident-demo-agent",
                            }))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let env_dir =
            std::env::temp_dir().join(format!("stratum-seed-existing-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&env_dir).unwrap();
        let env_out = env_dir.join("demo.env");
        std::fs::write(&env_out, "STRATUM_URL='http://127.0.0.1:3000'\n").unwrap();

        let err = run_workspace_seed_demo(
            seed_context(format!("http://{addr}")),
            SeedDemoOptions {
                agent_token: Some("backing-agent-secret".to_string()),
                fixture: Some(PathBuf::from(DEFAULT_SEED_FIXTURE)),
                name: None,
                root_path: None,
                token_name: None,
                session_ref: None,
                env_out: Some(env_out.clone()),
            },
        )
        .await
        .expect_err("existing env file should fail closed");
        server.abort();

        let VfsError::InvalidArgs { message } = err else {
            panic!("existing env file should return InvalidArgs");
        };
        assert!(message.contains("refusing to overwrite existing env file"));
        assert!(!message.contains("backing-agent-secret"));
        assert_eq!(*token_issue_count.lock().unwrap(), 0);

        let _ = std::fs::remove_dir_all(env_dir);
    }

    #[tokio::test]
    async fn seed_demo_revokes_token_and_removes_env_when_seed_write_fails() {
        use axum::Router;
        use axum::extract::Path;
        use axum::routing::{post, put};

        let workspace_id = Uuid::new_v4();
        let token_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let revoke_count = Arc::new(Mutex::new(0));
        let revoke_count_for_handler = revoke_count.clone();
        let app = Router::new()
            .route(
                "/v1/capabilities",
                axum::routing::get(|| async { Json(local_capabilities_fixture()) }),
            )
            .route(
                "/workspaces",
                post({
                    move |Json(body): Json<serde_json::Value>| async move {
                        Json(serde_json::json!({
                            "id": workspace_id,
                            "name": body.get("name").and_then(|value| value.as_str()).unwrap_or_default(),
                            "root_path": body.get("root_path").and_then(|value| value.as_str()).unwrap_or_default(),
                            "base_ref": "main",
                            "session_ref": body.get("session_ref").and_then(|value| value.as_str()),
                        }))
                    }
                }),
            )
            .route(
                "/workspaces/{workspace_id}/tokens",
                post(move |Path(_): Path<Uuid>, Json(_body): Json<serde_json::Value>| async move {
                    Json(serde_json::json!({
                        "workspace_id": workspace_id,
                        "token_id": token_id,
                        "workspace_token": "issued-workspace-secret",
                        "name": "incident-demo-agent",
                    }))
                }),
            )
            .route(
                "/workspaces/{workspace_id}/tokens/{token_id}/revoke",
                post(move |Path((_workspace_id, _token_id)): Path<(Uuid, Uuid)>| {
                    let revoke_count = revoke_count_for_handler.clone();
                    async move {
                        *revoke_count.lock().unwrap() += 1;
                        Json(serde_json::json!({
                            "workspace_id": workspace_id,
                            "token_id": token_id,
                            "revoked": true,
                        }))
                    }
                }),
            )
            .route(
                "/fs/{*path}",
                put(
                    move |Path(path): Path<String>, headers: axum::http::HeaderMap| async move {
                        if headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            == Some("User root")
                            && path.trim_start_matches('/')
                                == DEFAULT_SEED_ROOT_PATH.trim_start_matches('/')
                        {
                            return (
                                axum::http::StatusCode::OK,
                                Json(serde_json::json!({ "created": DEFAULT_SEED_ROOT_PATH })),
                            );
                        }
                        (
                            axum::http::StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({ "error": "seed write failed" })),
                        )
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let fixture_dir = write_fixture_tree();
        let env_dir = std::env::temp_dir().join(format!("stratum-seed-env-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&env_dir).unwrap();
        let env_out = env_dir.join("demo.env");

        let err = run_workspace_seed_demo(
            seed_context(format!("http://{addr}")),
            SeedDemoOptions {
                agent_token: Some("backing-agent-secret".to_string()),
                fixture: Some(fixture_dir.clone()),
                name: None,
                root_path: None,
                token_name: None,
                session_ref: None,
                env_out: Some(env_out.clone()),
            },
        )
        .await
        .expect_err("seed write failure should fail setup");
        server.abort();

        let VfsError::InvalidArgs { message } = err else {
            panic!("seed write failure should surface as InvalidArgs");
        };
        assert!(message.contains("seed write failed"));
        assert!(!env_out.exists());
        assert_eq!(*revoke_count.lock().unwrap(), 1);

        let _ = std::fs::remove_dir_all(fixture_dir);
        let _ = std::fs::remove_dir_all(env_dir);
    }
}
