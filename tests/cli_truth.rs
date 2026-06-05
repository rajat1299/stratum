use std::path::Path;
use std::process::{Command, Output};

const STRATUM_ENV_VARS: &[&str] = &[
    "STRATUM_URL",
    "STRATUM_USER",
    "STRATUM_TOKEN",
    "STRATUM_WORKSPACE_ID",
    "STRATUM_WORKSPACE_TOKEN",
    "STRATUM_REPO",
];

#[test]
fn stratumctl_help_matches_truth_file() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_stratumctl"));
    command.arg("--help");
    clear_stratum_env(&mut command);

    let output = command.output().expect("run stratumctl --help");

    assert!(
        output.status.success(),
        "stratumctl --help failed: {}",
        combined_output(&output)
    );
    assert_text_matches_fixture(
        &String::from_utf8_lossy(&output.stdout),
        Path::new("tests/fixtures/cli/stratumctl-help.stdout"),
    );
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

fn clear_stratum_env(command: &mut Command) {
    for name in STRATUM_ENV_VARS {
        command.env_remove(name);
    }
}

fn assert_text_matches_fixture(actual: &str, fixture_path: &Path) {
    let expected = std::fs::read_to_string(fixture_path)
        .unwrap_or_else(|error| panic!("read fixture {fixture_path:?}: {error}"));
    assert_eq!(normalize_newlines(actual), normalize_newlines(&expected));
}

fn normalize_newlines(text: &str) -> String {
    text.replace("\r\n", "\n")
}

fn combined_output(output: &Output) -> String {
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}
