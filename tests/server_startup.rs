use std::path::PathBuf;
use std::process::{Command, Output};

use uuid::Uuid;

fn temp_data_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("stratum-server-startup-{name}-{}", Uuid::new_v4()))
}

fn server_command(data_dir: &PathBuf) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stratum-server"));
    command
        .env_remove("STRATUM_BACKEND")
        .env_remove("STRATUM_POSTGRES_URL")
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
    assert!(!data_dir.join(".vfs").exists());

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn durable_backend_startup_rejects_password_url_without_leaking_password_or_creating_local_store() {
    let data_dir = temp_data_dir("password-url");
    let output = server_command(&data_dir)
        .env("STRATUM_BACKEND", "durable")
        .env(
            "STRATUM_POSTGRES_URL",
            "postgresql://user:raw-db-password-123@localhost/stratum",
        )
        .env("STRATUM_R2_BUCKET", "stratum")
        .env("STRATUM_R2_ENDPOINT", "https://example.invalid")
        .env("STRATUM_R2_ACCESS_KEY_ID", "test-access-key")
        .env("STRATUM_R2_SECRET_ACCESS_KEY", "test-secret-key")
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("STRATUM_POSTGRES_URL must not include a password"));
    assert!(!text.contains("raw-db-password-123"));
    assert!(!text.contains("test-access-key"));
    assert!(!text.contains("test-secret-key"));
    assert!(!data_dir.join(".vfs").exists());

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn durable_backend_startup_fails_closed_without_creating_local_store_when_env_is_complete() {
    let data_dir = temp_data_dir("unsupported");
    let output = server_command(&data_dir)
        .env("STRATUM_BACKEND", "durable")
        .env("STRATUM_POSTGRES_URL", "postgresql://localhost/stratum")
        .env("STRATUM_R2_BUCKET", "stratum")
        .env("STRATUM_R2_ENDPOINT", "https://example.invalid")
        .env("STRATUM_R2_ACCESS_KEY_ID", "test-access-key")
        .env("STRATUM_R2_SECRET_ACCESS_KEY", "test-secret-key")
        .output()
        .expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("durable backend runtime is validated but not wired"));
    assert!(!text.contains("test-access-key"));
    assert!(!text.contains("test-secret-key"));
    assert!(!data_dir.join(".vfs").exists());

    let _ = std::fs::remove_dir_all(data_dir);
}
