use std::ffi::OsStr;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use uuid::Uuid;

const RAW_R2_ACCESS_KEY: &str = "raw-r2-access-key";
const RAW_R2_SECRET_KEY: &str = "raw-r2-secret-key";
const RAW_POSTGRES_PASSWORD: &str = "raw-db-password-123";
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const SERVER_STARTUP_ATTEMPTS: usize = 3;

struct TempDataDir {
    path: PathBuf,
}

impl TempDataDir {
    fn new(name: &str) -> Self {
        Self {
            path: std::env::temp_dir()
                .join(format!("stratum-server-startup-{name}-{}", Uuid::new_v4())),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn server_command(data_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stratum-server"));
    command
        .env_remove("STRATUM_BACKEND")
        .env_remove("STRATUM_POSTGRES_URL")
        .env_remove("STRATUM_POSTGRES_SCHEMA")
        .env_remove("STRATUM_DURABLE_MIGRATION_MODE")
        .env_remove("PGPASSWORD")
        .env_remove("STRATUM_POSTGRES_TEST_PASSWORD")
        .env_remove("STRATUM_WORKSPACE_METADATA_PATH")
        .env_remove("STRATUM_IDEMPOTENCY_PATH")
        .env_remove("STRATUM_AUDIT_PATH")
        .env_remove("STRATUM_REVIEW_PATH")
        .env_remove("STRATUM_R2_BUCKET")
        .env_remove("STRATUM_R2_ENDPOINT")
        .env_remove("STRATUM_R2_ACCESS_KEY_ID")
        .env_remove("STRATUM_R2_SECRET_ACCESS_KEY")
        .env_remove("STRATUM_R2_REGION")
        .env_remove("STRATUM_R2_PREFIX")
        .env("STRATUM_DATA_DIR", data_dir);
    command
}

fn combined_output(output: &Output) -> String {
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

fn assert_no_secret_leaks(text: &str) {
    assert!(!text.contains(RAW_R2_ACCESS_KEY));
    assert!(!text.contains(RAW_R2_SECRET_KEY));
    assert!(!text.contains(RAW_POSTGRES_PASSWORD));
    assert!(!text.contains("postgresql://user:"));
    assert!(!text.contains("postgres://user:"));
    for name in ["PGPASSWORD", "STRATUM_POSTGRES_TEST_PASSWORD"] {
        if let Ok(secret) = std::env::var(name)
            && should_check_parent_env_secret(&secret)
        {
            assert!(!text.contains(&secret));
        }
    }
}

fn should_check_parent_env_secret(secret: &str) -> bool {
    !secret.is_empty() && !matches!(secret, "postgres" | "stratum")
}

fn assert_no_local_control_plane_files(data_dir: &std::path::Path) {
    for path in [
        data_dir.join(".vfs").join("workspaces.bin"),
        data_dir.join(".vfs").join("idempotency.bin"),
        data_dir.join(".vfs").join("audit.bin"),
        data_dir.join(".vfs").join("review.bin"),
    ] {
        assert!(!path.exists(), "local control-plane file exists: {path:?}");
    }
}

fn reserve_localhost_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve localhost port");
    let addr = listener
        .local_addr()
        .expect("reserved listener has address");
    addr.to_string()
}

struct ServerStartupError {
    message: String,
    output: Option<Output>,
}

impl ServerStartupError {
    fn combined_output(&self) -> String {
        self.output
            .as_ref()
            .map(combined_output)
            .unwrap_or_default()
    }

    fn looks_like_bind_conflict(&self) -> bool {
        let text = self.combined_output();
        text.contains("failed to bind") && text.contains("Address already in use")
    }
}

struct RunningServer {
    child: Option<Child>,
    base_url: String,
}

impl RunningServer {
    async fn spawn(mut command: Command) -> Result<Self, ServerStartupError> {
        let listen_addr =
            command_env_value(&command, "STRATUM_LISTEN").unwrap_or_else(reserve_localhost_addr);
        command
            .env("STRATUM_LISTEN", &listen_addr)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().map_err(|error| ServerStartupError {
            message: format!("spawn stratum-server: {error}"),
            output: None,
        })?;
        let base_url = format!("http://{listen_addr}");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .expect("build health check client");
        let deadline = Instant::now() + SERVER_STARTUP_TIMEOUT;

        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let output = child.wait_with_output().ok();
                    return Err(ServerStartupError {
                        message: format!(
                            "stratum-server exited before /health succeeded with {status}"
                        ),
                        output,
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = child.kill();
                    let output = child.wait_with_output().ok();
                    return Err(ServerStartupError {
                        message: format!("poll stratum-server status: {error}"),
                        output,
                    });
                }
            }

            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().ok();
                return Err(ServerStartupError {
                    message: format!("timed out waiting for {base_url}/health"),
                    output,
                });
            }

            match client.get(format!("{base_url}/health")).send().await {
                Ok(response) if response.status().is_success() => {
                    return Ok(Self {
                        child: Some(child),
                        base_url,
                    });
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn shutdown(mut self) -> Output {
        let mut child = self.child.take().expect("server child should be present");
        let _ = child.kill();
        child
            .wait_with_output()
            .expect("collect stratum-server output")
    }
}

fn command_env_value(command: &Command, name: &str) -> Option<String> {
    command
        .get_envs()
        .find(|(key, _)| *key == OsStr::new(name))
        .and_then(|(_, value)| value.and_then(|value| value.to_str().map(str::to_owned)))
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn spawn_healthy_server(mut make_command: impl FnMut() -> Command) -> RunningServer {
    let mut last_bind_conflict = None;

    for attempt in 1..=SERVER_STARTUP_ATTEMPTS {
        match RunningServer::spawn(make_command()).await {
            Ok(server) => return server,
            Err(error) if error.looks_like_bind_conflict() && attempt < SERVER_STARTUP_ATTEMPTS => {
                let text = error.combined_output();
                assert_no_secret_leaks(&text);
                last_bind_conflict = Some((error.message, text));
            }
            Err(error) => {
                let text = error.combined_output();
                assert_no_secret_leaks(&text);
                panic!(
                    "stratum-server failed to become healthy: {}\n{}",
                    error.message, text
                );
            }
        }
    }

    if let Some((message, text)) = last_bind_conflict {
        assert_no_secret_leaks(&text);
        panic!("stratum-server failed to bind after retries: {message}\n{text}");
    }

    unreachable!("server startup attempt count should be nonzero")
}

fn assert_no_secret_leaks_for_output(output: &Output) {
    let text = combined_output(output);
    assert_no_secret_leaks(&text);
}

#[test]
fn temp_data_dir_cleanup_removes_directory_on_drop() {
    let path = {
        let data_dir = TempDataDir::new("drop-cleanup");
        std::fs::create_dir_all(data_dir.path().join(".vfs")).expect("create temp data dir");
        data_dir.path().to_path_buf()
    };

    assert!(!path.exists());
}

#[test]
fn known_benign_parent_postgres_password_values_are_not_leak_sentinels() {
    assert!(!should_check_parent_env_secret(""));
    assert!(!should_check_parent_env_secret("postgres"));
    assert!(!should_check_parent_env_secret("stratum"));
    assert!(should_check_parent_env_secret(RAW_POSTGRES_PASSWORD));
    assert!(should_check_parent_env_secret("ciPassword123"));
}

#[tokio::test]
async fn bind_conflict_retry_eventually_starts_server() {
    let data_dir = TempDataDir::new("bind-conflict-retry");
    let held_listener = TcpListener::bind("127.0.0.1:0").expect("reserve conflicting port");
    let conflicted_addr = held_listener
        .local_addr()
        .expect("conflicting listener has address")
        .to_string();
    let mut attempts = 0;

    let server = spawn_healthy_server(|| {
        attempts += 1;
        let mut command = server_command(data_dir.path());
        if attempts == 1 {
            command.env("STRATUM_LISTEN", &conflicted_addr);
        }
        command
    })
    .await;

    assert!(attempts >= 2);
    let output = server.shutdown();
    assert_no_secret_leaks_for_output(&output);
}

#[tokio::test]
async fn local_backend_default_starts_and_responds_to_health() {
    let data_dir = TempDataDir::new("local-default-health");
    let server = spawn_healthy_server(|| server_command(data_dir.path())).await;

    let response = reqwest::Client::new()
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .expect("health request should succeed");

    assert!(response.status().is_success());
    let output = server.shutdown();
    assert_no_secret_leaks_for_output(&output);
}

#[test]
fn durable_backend_startup_fails_before_creating_local_store_when_env_is_missing() {
    let data_dir = TempDataDir::new("missing-env");
    let output = server_command(data_dir.path())
        .env("STRATUM_BACKEND", "durable")
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("missing required durable backend environment variables"));
    assert_no_secret_leaks(&text);
    assert!(!data_dir.path().join(".vfs").exists());
    assert_no_local_control_plane_files(data_dir.path());
}

#[test]
fn durable_backend_startup_rejects_password_url_without_leaking_password_or_creating_local_store() {
    let data_dir = TempDataDir::new("password-url");
    let output = server_command(data_dir.path())
        .env("STRATUM_BACKEND", "durable")
        .env(
            "STRATUM_POSTGRES_URL",
            format!("postgresql://user:{RAW_POSTGRES_PASSWORD}@localhost/stratum"),
        )
        .env("STRATUM_R2_BUCKET", "stratum")
        .env("STRATUM_R2_ENDPOINT", "https://example.invalid")
        .env("STRATUM_R2_ACCESS_KEY_ID", RAW_R2_ACCESS_KEY)
        .env("STRATUM_R2_SECRET_ACCESS_KEY", RAW_R2_SECRET_KEY)
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("STRATUM_POSTGRES_URL must not include a password"));
    assert_no_secret_leaks(&text);
    assert!(!data_dir.path().join(".vfs").exists());
    assert_no_local_control_plane_files(data_dir.path());
}

#[cfg(not(feature = "postgres"))]
#[test]
fn durable_backend_startup_fails_closed_without_creating_local_store_when_env_is_complete() {
    let data_dir = TempDataDir::new("unsupported");
    let output = server_command(data_dir.path())
        .env("STRATUM_BACKEND", "durable")
        .env("STRATUM_POSTGRES_URL", "postgresql://localhost/stratum")
        .env("STRATUM_R2_BUCKET", "stratum")
        .env("STRATUM_R2_ENDPOINT", "https://example.invalid")
        .env("STRATUM_R2_ACCESS_KEY_ID", RAW_R2_ACCESS_KEY)
        .env("STRATUM_R2_SECRET_ACCESS_KEY", RAW_R2_SECRET_KEY)
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains(
        "durable backend runtime requires stratum-server built with the postgres feature"
    ));
    assert_no_secret_leaks(&text);
    assert!(!data_dir.path().join(".vfs").exists());
    assert_no_local_control_plane_files(data_dir.path());
}

#[cfg(feature = "postgres")]
#[test]
fn durable_backend_rejects_remote_notls_postgres_before_creating_local_control_plane_store() {
    let data_dir = TempDataDir::new("remote-notls-postgres");
    let output = server_command(data_dir.path())
        .env("STRATUM_BACKEND", "durable")
        .env("STRATUM_POSTGRES_URL", "postgresql://db.internal/stratum")
        .env("STRATUM_R2_BUCKET", "stratum")
        .env("STRATUM_R2_ENDPOINT", "https://example.invalid")
        .env("STRATUM_R2_ACCESS_KEY_ID", RAW_R2_ACCESS_KEY)
        .env("STRATUM_R2_SECRET_ACCESS_KEY", RAW_R2_SECRET_KEY)
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("must use localhost"));
    assert!(!text.contains("db.internal"));
    assert_no_secret_leaks(&text);
    assert!(!data_dir.path().join(".vfs").exists());
    assert_no_local_control_plane_files(data_dir.path());
}

#[cfg(feature = "postgres")]
mod postgres_process_tests {
    use super::*;
    use stratum::backend::postgres_migrations::{PostgresMigrationRunner, PostgresMigrationStatus};
    use tokio_postgres::{Config, NoTls};

    struct TestPostgres {
        config: Config,
        url: String,
        schema: String,
        password: Option<String>,
    }

    impl TestPostgres {
        async fn new() -> Option<Self> {
            let Some(url) = std::env::var("STRATUM_POSTGRES_TEST_URL").ok() else {
                if postgres_tests_required() {
                    panic!("STRATUM_POSTGRES_TEST_URL is required for server startup tests");
                }
                eprintln!(
                    "skipping server startup Postgres tests; STRATUM_POSTGRES_TEST_URL is unset"
                );
                return None;
            };

            let mut config: Config = url
                .parse()
                .expect("STRATUM_POSTGRES_TEST_URL should parse as a Postgres config");
            if config.get_password().is_some() {
                panic!(
                    "STRATUM_POSTGRES_TEST_URL must not include a password; use STRATUM_POSTGRES_TEST_PASSWORD or PGPASSWORD"
                );
            }

            let password = std::env::var("STRATUM_POSTGRES_TEST_PASSWORD")
                .or_else(|_| std::env::var("PGPASSWORD"))
                .ok();
            if let Some(password) = password.as_deref() {
                config.password(password);
            }

            let schema = format!("stratum_server_startup_{}", Uuid::new_v4().simple());
            let (client, connection) = config.connect(NoTls).await.expect("connect test Postgres");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
                .await
                .expect("create isolated schema");

            Some(Self {
                config,
                url,
                schema,
                password,
            })
        }

        fn runner(&self) -> PostgresMigrationRunner {
            PostgresMigrationRunner::with_schema(self.config.clone(), self.schema.clone())
                .expect("create migration runner")
        }

        async fn client(&self) -> tokio_postgres::Client {
            let (client, connection) = self
                .config
                .connect(NoTls)
                .await
                .expect("connect test Postgres");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }

        async fn row_count(&self, table: &str) -> i64 {
            assert!(matches!(
                table,
                "workspaces"
                    | "idempotency_records"
                    | "audit_events"
                    | "protected_ref_rules"
                    | "change_requests"
            ));
            let client = self.client().await;
            let row = client
                .query_one(
                    &format!("SELECT COUNT(*) FROM \"{}\".{table}", self.schema),
                    &[],
                )
                .await
                .expect("count table rows");
            row.get(0)
        }

        async fn drop_table(&self, table: &str) {
            assert!(matches!(table, "repos" | "workspaces"));
            let client = self.client().await;
            client
                .batch_execute(&format!("DROP TABLE \"{}\".{table} CASCADE", self.schema))
                .await
                .expect("drop test table");
        }

        fn server_command(&self, data_dir: &Path, migration_mode: &str) -> Command {
            let mut command = super::server_command(data_dir);
            command
                .env("STRATUM_BACKEND", "durable")
                .env("STRATUM_POSTGRES_URL", &self.url)
                .env("STRATUM_POSTGRES_SCHEMA", &self.schema)
                .env("STRATUM_DURABLE_MIGRATION_MODE", migration_mode)
                .env("STRATUM_R2_BUCKET", "stratum")
                .env("STRATUM_R2_ENDPOINT", "https://example.invalid")
                .env("STRATUM_R2_ACCESS_KEY_ID", RAW_R2_ACCESS_KEY)
                .env("STRATUM_R2_SECRET_ACCESS_KEY", RAW_R2_SECRET_KEY);
            if let Some(password) = self.password.as_deref() {
                command.env("PGPASSWORD", password);
            }
            command
        }

        async fn insert_dirty_control_row(&self) {
            self.runner()
                .status()
                .await
                .expect("create migration control table");
            let (client, connection) = self
                .config
                .connect(NoTls)
                .await
                .expect("connect test Postgres");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let checksum = "0".repeat(64);
            client
                .execute(
                    &format!(
                        "INSERT INTO \"{}\".stratum_schema_migrations (
                            version,
                            name,
                            checksum,
                            state,
                            started_at,
                            finished_at,
                            failure_message
                         )
                         VALUES (1, 'dirty_startup_test', $1, 'started', clock_timestamp(), NULL, NULL)",
                        self.schema
                    ),
                    &[&checksum],
                )
                .await
                .expect("insert dirty migration control row");
        }
    }

    impl Drop for TestPostgres {
        fn drop(&mut self) {
            let config = self.config.clone();
            let schema = self.schema.clone();

            let cleanup = std::thread::spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };

                runtime.block_on(async move {
                    if let Ok((client, connection)) = config.connect(NoTls).await {
                        tokio::spawn(async move {
                            let _ = connection.await;
                        });
                        let _ = client
                            .batch_execute(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                            .await;
                    }
                });
            });

            let _ = cleanup.join();
        }
    }

    fn postgres_tests_required() -> bool {
        std::env::var("STRATUM_POSTGRES_TEST_REQUIRED").as_deref() == Ok("1")
            || std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
    }

    async fn current_main_ref_target(client: &reqwest::Client, base_url: &str) -> String {
        let response = client
            .get(format!("{base_url}/vcs/refs"))
            .header("Authorization", "User root")
            .send()
            .await
            .expect("list refs request should complete");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "list refs response: {}",
            response.text().await.unwrap_or_default()
        );
        let body = response
            .json::<serde_json::Value>()
            .await
            .expect("list refs response json");
        body["refs"]
            .as_array()
            .expect("refs array")
            .iter()
            .find(|item| item["name"].as_str() == Some("main"))
            .and_then(|item| item["target"].as_str())
            .expect("main ref target")
            .to_string()
    }

    #[tokio::test]
    async fn test_postgres_drop_removes_isolated_schema() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let config = db.config.clone();
        let schema = db.schema.clone();

        drop(db);

        let (client, connection) = config.connect(NoTls).await.expect("connect test Postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = $1
                )",
                &[&schema],
            )
            .await
            .expect("query schema existence");
        let exists: bool = row.get(0);
        assert!(!exists, "isolated test schema was not dropped: {schema}");
    }

    #[tokio::test]
    async fn complete_durable_env_status_mode_fails_when_migrations_are_pending() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let data_dir = TempDataDir::new("pending-migrations");

        let output = db
            .server_command(data_dir.path(), "status")
            .output()
            .expect("stratum-server should execute");

        assert!(!output.status.success());
        let text = combined_output(&output);
        assert!(text.contains("Postgres migrations are pending"));
        assert_no_secret_leaks(&text);
        assert!(!data_dir.path().join(".vfs").exists());
        assert_no_local_control_plane_files(data_dir.path());
    }

    #[tokio::test]
    async fn durable_env_status_mode_fails_when_control_plane_tables_are_missing() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        db.runner()
            .apply_pending()
            .await
            .expect("apply migrations before drift simulation");
        db.drop_table("workspaces").await;
        let data_dir = TempDataDir::new("missing-control-plane-table");

        let output = db
            .server_command(data_dir.path(), "status")
            .output()
            .expect("stratum-server should execute");

        assert!(!output.status.success());
        let text = combined_output(&output);
        assert!(text.contains("durable control-plane readiness"));
        assert_no_secret_leaks(&text);
        assert_no_local_control_plane_files(data_dir.path());
    }

    #[tokio::test]
    async fn durable_env_status_mode_fails_when_repo_table_is_missing() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        db.runner()
            .apply_pending()
            .await
            .expect("apply migrations before drift simulation");
        db.drop_table("repos").await;
        let data_dir = TempDataDir::new("missing-repo-table");

        let output = db
            .server_command(data_dir.path(), "status")
            .output()
            .expect("stratum-server should execute");

        assert!(!output.status.success());
        let text = combined_output(&output);
        assert!(text.contains("durable control-plane readiness"));
        assert_no_secret_leaks(&text);
        assert_no_local_control_plane_files(data_dir.path());
    }

    #[tokio::test]
    async fn complete_durable_env_apply_mode_applies_migrations_then_serves_health() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let data_dir = TempDataDir::new("apply-migrations");

        let server = spawn_healthy_server(|| db.server_command(data_dir.path(), "apply")).await;
        let output = server.shutdown();
        let text = combined_output(&output);
        assert_no_secret_leaks(&text);
        assert_no_local_control_plane_files(data_dir.path());

        let report = db.runner().status().await.expect("load migration status");
        assert_eq!(
            report.statuses,
            vec![
                PostgresMigrationStatus::Applied {
                    version: 1,
                    name: "durable_backend_foundation",
                },
                PostgresMigrationStatus::Applied {
                    version: 2,
                    name: "review_local_commit_ids",
                }
            ]
        );
    }

    #[tokio::test]
    async fn dirty_migration_control_state_fails_before_creating_local_store() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        db.insert_dirty_control_row().await;
        let data_dir = TempDataDir::new("dirty-migrations");

        let output = db
            .server_command(data_dir.path(), "apply")
            .output()
            .expect("stratum-server should execute");

        assert!(!output.status.success());
        let text = combined_output(&output);
        assert!(text.contains("Postgres migration version 1 is dirty"));
        assert_no_secret_leaks(&text);
        assert!(!data_dir.path().join(".vfs").exists());
        assert_no_local_control_plane_files(data_dir.path());
    }

    #[tokio::test]
    async fn durable_server_persists_control_plane_rows_in_postgres() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let data_dir = TempDataDir::new("durable-control-plane");
        let server = spawn_healthy_server(|| db.server_command(data_dir.path(), "apply")).await;
        let client = reqwest::Client::new();

        let workspace_response = client
            .post(format!("{}/workspaces", server.base_url()))
            .header("Authorization", "User root")
            .header("Idempotency-Key", "runtime-demo-workspace")
            .json(&serde_json::json!({
                "name": "runtime-demo",
                "root_path": "/runtime-demo",
            }))
            .send()
            .await
            .expect("create workspace request should complete");
        assert_eq!(
            workspace_response.status(),
            reqwest::StatusCode::CREATED,
            "create workspace response: {}",
            workspace_response.text().await.unwrap_or_default()
        );

        let protected_ref_response = client
            .post(format!("{}/protected/refs", server.base_url()))
            .header("Authorization", "User root")
            .header("Idempotency-Key", "runtime-demo-protected-ref")
            .json(&serde_json::json!({
                "ref_name": "review/protected-runtime",
                "required_approvals": 1,
            }))
            .send()
            .await
            .expect("create protected ref request should complete");
        assert_eq!(
            protected_ref_response.status(),
            reqwest::StatusCode::CREATED,
            "create protected ref response: {}",
            protected_ref_response.text().await.unwrap_or_default()
        );

        let base_write_response = client
            .put(format!("{}/fs/review.txt", server.base_url()))
            .header("Authorization", "User root")
            .body("base")
            .send()
            .await
            .expect("base write request should complete");
        assert_eq!(
            base_write_response.status(),
            reqwest::StatusCode::OK,
            "base write response: {}",
            base_write_response.text().await.unwrap_or_default()
        );

        let base_commit_response = client
            .post(format!("{}/vcs/commit", server.base_url()))
            .header("Authorization", "User root")
            .json(&serde_json::json!({"message": "runtime base"}))
            .send()
            .await
            .expect("base commit request should complete");
        assert_eq!(
            base_commit_response.status(),
            reqwest::StatusCode::OK,
            "base commit response: {}",
            base_commit_response.text().await.unwrap_or_default()
        );
        let base_commit = current_main_ref_target(&client, server.base_url()).await;

        let base_ref_response = client
            .post(format!("{}/vcs/refs", server.base_url()))
            .header("Authorization", "User root")
            .json(&serde_json::json!({
                "name": "archive/runtime-base",
                "target": base_commit,
            }))
            .send()
            .await
            .expect("base ref request should complete");
        assert_eq!(
            base_ref_response.status(),
            reqwest::StatusCode::CREATED,
            "base ref response: {}",
            base_ref_response.text().await.unwrap_or_default()
        );

        let head_write_response = client
            .put(format!("{}/fs/review.txt", server.base_url()))
            .header("Authorization", "User root")
            .body("head")
            .send()
            .await
            .expect("head write request should complete");
        assert_eq!(
            head_write_response.status(),
            reqwest::StatusCode::OK,
            "head write response: {}",
            head_write_response.text().await.unwrap_or_default()
        );

        let head_commit_response = client
            .post(format!("{}/vcs/commit", server.base_url()))
            .header("Authorization", "User root")
            .json(&serde_json::json!({"message": "runtime head"}))
            .send()
            .await
            .expect("head commit request should complete");
        assert_eq!(
            head_commit_response.status(),
            reqwest::StatusCode::OK,
            "head commit response: {}",
            head_commit_response.text().await.unwrap_or_default()
        );
        let head_commit = current_main_ref_target(&client, server.base_url()).await;

        let head_ref_response = client
            .post(format!("{}/vcs/refs", server.base_url()))
            .header("Authorization", "User root")
            .json(&serde_json::json!({
                "name": "review/runtime-head",
                "target": head_commit,
            }))
            .send()
            .await
            .expect("head ref request should complete");
        assert_eq!(
            head_ref_response.status(),
            reqwest::StatusCode::CREATED,
            "head ref response: {}",
            head_ref_response.text().await.unwrap_or_default()
        );

        let change_request_response = client
            .post(format!("{}/change-requests", server.base_url()))
            .header("Authorization", "User root")
            .header("Idempotency-Key", "runtime-demo-change-request")
            .json(&serde_json::json!({
                "title": "Runtime review",
                "source_ref": "review/runtime-head",
                "target_ref": "archive/runtime-base",
            }))
            .send()
            .await
            .expect("change request should complete");
        assert_eq!(
            change_request_response.status(),
            reqwest::StatusCode::CREATED,
            "change request response: {}",
            change_request_response.text().await.unwrap_or_default()
        );

        assert_eq!(db.row_count("workspaces").await, 1);
        assert_eq!(db.row_count("idempotency_records").await, 3);
        assert_eq!(db.row_count("audit_events").await, 9);
        assert_eq!(db.row_count("protected_ref_rules").await, 1);
        assert_eq!(db.row_count("change_requests").await, 1);
        assert_no_local_control_plane_files(data_dir.path());

        let output = server.shutdown();
        let text = combined_output(&output);
        assert_no_secret_leaks(&text);
    }
}
