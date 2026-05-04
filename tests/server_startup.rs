use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use uuid::Uuid;

const RAW_R2_ACCESS_KEY: &str = "raw-r2-access-key";
const RAW_R2_SECRET_KEY: &str = "raw-r2-secret-key";
const RAW_POSTGRES_PASSWORD: &str = "raw-db-password-123";
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

fn temp_data_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("stratum-server-startup-{name}-{}", Uuid::new_v4()))
}

fn server_command(data_dir: &PathBuf) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stratum-server"));
    command
        .env_remove("STRATUM_BACKEND")
        .env_remove("STRATUM_POSTGRES_URL")
        .env_remove("STRATUM_POSTGRES_SCHEMA")
        .env_remove("STRATUM_DURABLE_MIGRATION_MODE")
        .env_remove("PGPASSWORD")
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
            && !secret.is_empty()
        {
            assert!(!text.contains(&secret));
        }
    }
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
}

struct RunningServer {
    child: Option<Child>,
    base_url: String,
}

impl RunningServer {
    async fn spawn(mut command: Command) -> Result<Self, ServerStartupError> {
        let listen_addr = reserve_localhost_addr();
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

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn spawn_healthy_server(command: Command) -> RunningServer {
    match RunningServer::spawn(command).await {
        Ok(server) => server,
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

#[tokio::test]
async fn local_backend_default_starts_and_responds_to_health() {
    let data_dir = temp_data_dir("local-default-health");
    let server = spawn_healthy_server(server_command(&data_dir)).await;

    let response = reqwest::Client::new()
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .expect("health request should succeed");

    assert!(response.status().is_success());
    let output = server.shutdown();
    let text = combined_output(&output);
    assert_no_secret_leaks(&text);

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn durable_backend_startup_fails_before_creating_local_store_when_env_is_missing() {
    let data_dir = temp_data_dir("missing-env");
    let output = server_command(&data_dir)
        .env("STRATUM_BACKEND", "durable")
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("missing required durable backend environment variables"));
    assert_no_secret_leaks(&text);
    assert!(!data_dir.join(".vfs").exists());
    assert_no_local_control_plane_files(&data_dir);

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn durable_backend_startup_rejects_password_url_without_leaking_password_or_creating_local_store() {
    let data_dir = temp_data_dir("password-url");
    let output = server_command(&data_dir)
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
    assert!(!data_dir.join(".vfs").exists());
    assert_no_local_control_plane_files(&data_dir);

    let _ = std::fs::remove_dir_all(data_dir);
}

#[cfg(not(feature = "postgres"))]
#[test]
fn durable_backend_startup_fails_closed_without_creating_local_store_when_env_is_complete() {
    let data_dir = temp_data_dir("unsupported");
    let output = server_command(&data_dir)
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
    assert!(!data_dir.join(".vfs").exists());
    assert_no_local_control_plane_files(&data_dir);

    let _ = std::fs::remove_dir_all(data_dir);
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
                "workspaces" | "idempotency_records" | "audit_events" | "protected_ref_rules"
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

        fn server_command(&self, data_dir: &PathBuf, migration_mode: &str) -> Command {
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

        async fn cleanup(self) {
            if let Ok((client, connection)) = self.config.connect(NoTls).await {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let _ = client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                        self.schema
                    ))
                    .await;
            }
        }
    }

    fn postgres_tests_required() -> bool {
        std::env::var("STRATUM_POSTGRES_TEST_REQUIRED").as_deref() == Ok("1")
            || std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
    }

    #[tokio::test]
    async fn complete_durable_env_status_mode_fails_when_migrations_are_pending() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let data_dir = temp_data_dir("pending-migrations");

        let output = db
            .server_command(&data_dir, "status")
            .output()
            .expect("stratum-server should execute");

        assert!(!output.status.success());
        let text = combined_output(&output);
        assert!(text.contains("Postgres migrations are pending"));
        assert_no_secret_leaks(&text);
        assert!(!data_dir.join(".vfs").exists());
        assert_no_local_control_plane_files(&data_dir);

        let _ = std::fs::remove_dir_all(data_dir);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn complete_durable_env_apply_mode_applies_migrations_then_serves_health() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let data_dir = temp_data_dir("apply-migrations");

        let server = spawn_healthy_server(db.server_command(&data_dir, "apply")).await;
        let output = server.shutdown();
        let text = combined_output(&output);
        assert_no_secret_leaks(&text);
        assert_no_local_control_plane_files(&data_dir);

        let report = db.runner().status().await.expect("load migration status");
        assert_eq!(
            report.statuses,
            vec![PostgresMigrationStatus::Applied {
                version: 1,
                name: "durable_backend_foundation",
            }]
        );

        let _ = std::fs::remove_dir_all(data_dir);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn dirty_migration_control_state_fails_before_creating_local_store() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        db.insert_dirty_control_row().await;
        let data_dir = temp_data_dir("dirty-migrations");

        let output = db
            .server_command(&data_dir, "apply")
            .output()
            .expect("stratum-server should execute");

        assert!(!output.status.success());
        let text = combined_output(&output);
        assert!(text.contains("Postgres migration version 1 is dirty"));
        assert_no_secret_leaks(&text);
        assert!(!data_dir.join(".vfs").exists());
        assert_no_local_control_plane_files(&data_dir);

        let _ = std::fs::remove_dir_all(data_dir);
        db.cleanup().await;
    }

    #[tokio::test]
    async fn durable_server_persists_control_plane_rows_in_postgres() {
        let Some(db) = TestPostgres::new().await else {
            return;
        };
        let data_dir = temp_data_dir("durable-control-plane");
        let server = spawn_healthy_server(db.server_command(&data_dir, "apply")).await;
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
                "ref_name": "main",
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

        assert_eq!(db.row_count("workspaces").await, 1);
        assert_eq!(db.row_count("idempotency_records").await, 2);
        assert_eq!(db.row_count("audit_events").await, 2);
        assert_eq!(db.row_count("protected_ref_rules").await, 1);
        assert_no_local_control_plane_files(&data_dir);

        let output = server.shutdown();
        let text = combined_output(&output);
        assert_no_secret_leaks(&text);

        let _ = std::fs::remove_dir_all(data_dir);
        db.cleanup().await;
    }
}
