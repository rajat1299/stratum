//! Shared conformance fixture model for local-state and durable-cloud route behavior.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

pub const CONFORMANCE_FIXTURE_REVISION: &str = "2026-06-08-1";
pub const CONFORMANCE_FIXTURE_VERSION: u32 = 1;
pub const CONFORMANCE_FIXTURE_FILE: &str = "conformance.routes.v1.json";

const ALLOWED_MODES: &[&str] = &["local-state", "durable-cloud"];
const ALLOWED_AUTH_PROFILES: &[&str] = &["none", "root-user", "workspace-bearer"];
const ALLOWED_TYPESCRIPT_MAPPINGS: &[&str] = &["getCapabilities", "writeFile"];
const ALLOWED_PYTHON_MAPPINGS: &[&str] = &["get_capabilities", "write_file"];
const ALLOWED_BASH_MAPPINGS: &[&str] = &[];
const ALLOWED_RUST_CLIENT_MAPPINGS: &[&str] = &["write_file"];
const ALLOWED_CLI_MAPPINGS: &[&str] = &[];

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ConformanceRoutesFixture {
    version: u32,
    revision: String,
    capability_revision: String,
    modes: Vec<String>,
    auth_profiles: HashMap<String, AuthProfileFixture>,
    forbidden_substrings: Vec<String>,
    cases: Vec<ConformanceCaseFixture>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AuthProfileFixture {
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ConformanceCaseFixture {
    id: String,
    mode: String,
    method: String,
    path: String,
    auth: String,
    expect: CaseExpectFixture,
    sdk: SdkCoverageFixture,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_gate: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CaseExpectFixture {
    status: u16,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    headers: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    json_paths: HashMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replay_header: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SdkCoverageFixture {
    typescript: Option<String>,
    python: Option<String>,
    bash: Option<String>,
    rust_client: Option<String>,
    cli: Option<String>,
}

pub fn conformance_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("sdk/contracts")
        .join(CONFORMANCE_FIXTURE_FILE)
}

fn load_conformance_fixture() -> ConformanceRoutesFixture {
    let path = conformance_fixture_path();
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
}

#[test]
fn fixture_schema_is_valid() {
    let fixture = load_conformance_fixture();

    assert_eq!(fixture.version, CONFORMANCE_FIXTURE_VERSION);
    assert_eq!(fixture.revision, CONFORMANCE_FIXTURE_REVISION);
    assert!(!fixture.capability_revision.is_empty());
    assert!(!fixture.forbidden_substrings.is_empty());

    let modes: HashSet<_> = fixture.modes.iter().map(String::as_str).collect();
    for mode in ALLOWED_MODES {
        assert!(modes.contains(mode), "missing mode {mode}");
    }

    for auth in ALLOWED_AUTH_PROFILES {
        assert!(
            fixture.auth_profiles.contains_key(*auth),
            "missing auth profile {auth}"
        );
    }

    assert!(!fixture.cases.is_empty(), "fixture must include cases");
    let mut case_ids = HashSet::new();
    for case in &fixture.cases {
        assert!(!case.id.is_empty(), "case id must be non-empty");
        assert!(
            case_ids.insert(case.id.clone()),
            "duplicate case id {}",
            case.id
        );
        assert!(
            ALLOWED_AUTH_PROFILES.contains(&case.auth.as_str()),
            "case {} has unknown auth profile {}",
            case.id,
            case.auth
        );
        assert!(
            ALLOWED_MODES.contains(&case.mode.as_str()),
            "case {} has unknown mode {}",
            case.id,
            case.mode
        );
        assert!(
            !case.method.is_empty() && !case.path.is_empty(),
            "case {} must declare method and path",
            case.id
        );
        assert!(
            case.expect.status > 0,
            "case {} must declare expect.status",
            case.id
        );
        assert_sdk_mapping(
            &case.id,
            "typescript",
            case.sdk.typescript.as_deref(),
            ALLOWED_TYPESCRIPT_MAPPINGS,
        );
        assert_sdk_mapping(
            &case.id,
            "python",
            case.sdk.python.as_deref(),
            ALLOWED_PYTHON_MAPPINGS,
        );
        assert_sdk_mapping(
            &case.id,
            "bash",
            case.sdk.bash.as_deref(),
            ALLOWED_BASH_MAPPINGS,
        );
        assert_sdk_mapping(
            &case.id,
            "rust_client",
            case.sdk.rust_client.as_deref(),
            ALLOWED_RUST_CLIENT_MAPPINGS,
        );
        assert_sdk_mapping(
            &case.id,
            "cli",
            case.sdk.cli.as_deref(),
            ALLOWED_CLI_MAPPINGS,
        );
    }
}

fn assert_sdk_mapping(case_id: &str, sdk_name: &str, mapping: Option<&str>, allowed: &[&str]) {
    let Some(mapping) = mapping else {
        return;
    };
    assert!(
        allowed.contains(&mapping),
        "case {case_id} has unsupported {sdk_name} sdk mapping {mapping}"
    );
}

mod fixture {
    use super::*;

    const EXPLICIT_FORBIDDEN_SUBSTRINGS: &[&str] = &[
        "postgres://",
        "postgresql://",
        "Bearer ",
        "STRATUM_",
        "SQLSTATE",
        "/Users/",
        "/tmp/",
    ];

    const FORBIDDEN_FIELD_NAMES: &[&str] = &[
        "duration_ms",
        "elapsed",
        "timestamp",
        "object_key",
        "db_url",
        "raw_secret",
        "provider_error",
    ];

    fn fixture_json_value() -> serde_json::Value {
        let path = conformance_fixture_path();
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        serde_json::from_str(&raw).expect("fixture must be valid json")
    }

    fn collect_object_keys(value: &serde_json::Value, keys: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    keys.push(key.clone());
                    collect_object_keys(child, keys);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    collect_object_keys(item, keys);
                }
            }
            _ => {}
        }
    }

    fn fixture_json_without_forbidden_list() -> serde_json::Value {
        let mut value = fixture_json_value();
        if let serde_json::Value::Object(map) = &mut value {
            map.remove("forbidden_substrings");
        }
        value
    }

    #[test]
    fn has_no_forbidden_substrings() {
        let fixture = load_conformance_fixture();
        let serialized = serde_json::to_string(&fixture_json_without_forbidden_list())
            .expect("serialize fixture for redaction scan");

        let mut needles: Vec<&str> = fixture
            .forbidden_substrings
            .iter()
            .map(String::as_str)
            .collect();
        for needle in EXPLICIT_FORBIDDEN_SUBSTRINGS {
            if !needles.contains(needle) {
                needles.push(needle);
            }
        }

        for needle in needles {
            assert!(
                !serialized.contains(needle),
                "fixture must not contain forbidden substring: {needle}"
            );
        }
    }

    #[test]
    fn has_no_unstable_field_names() {
        let value = fixture_json_value();
        let mut keys = Vec::new();
        collect_object_keys(&value, &mut keys);

        for key in keys {
            assert!(
                !FORBIDDEN_FIELD_NAMES.contains(&key.as_str()),
                "fixture must not contain unstable field name: {key}"
            );
        }
    }
}

mod harness;

mod capabilities {
    use super::harness::*;
    use super::*;
    use crate::backend::{RepoId, StratumStores};
    use crate::server::routes_capabilities::CAPABILITIES_REVISION;

    #[tokio::test]
    async fn fixture_cases_match_live_capabilities_routes() {
        let fixture = load_conformance_fixture();
        let cases = cases_with_prefix(&fixture.cases, "capabilities.");
        for case in cases {
            let router = match case.mode.as_str() {
                "local-state" => local_router(),
                "durable-cloud" => {
                    let stores = StratumStores::local_memory();
                    durable_router(
                        stores.workspace_metadata.clone(),
                        RepoId::new("repo_conformance_capabilities").expect("repo"),
                        stores,
                    )
                }
                other => panic!("unknown mode {other}"),
            };
            let (base_url, server) = spawn_test_router(router).await;
            let response = reqwest::Client::new()
                .get(format!("{base_url}{}", case.path))
                .send()
                .await
                .expect("capabilities request");
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            let body: serde_json::Value = response.json().await.expect("json body");
            server.abort();

            assert_eq!(status, case.expect.status, "{}", case.id);
            for (name, expected) in &case.expect.headers {
                assert_eq!(
                    headers.get(name).and_then(|value| value.to_str().ok()),
                    Some(expected.as_str()),
                    "{} header {}",
                    case.id,
                    name
                );
            }
            for (path, expected) in &case.expect.json_paths {
                assert_json_path(&body, path, expected);
            }
        }
    }

    #[test]
    fn manifest_revision_matches_capability_contracts() {
        let fixture = load_conformance_fixture();
        assert_eq!(fixture.capability_revision, CAPABILITIES_REVISION);
        let local_contract = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/contracts/capabilities.v1.json"),
        )
        .expect("read local capabilities contract");
        let durable_contract = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("sdk/contracts/capabilities.v1.durable-cloud.json"),
        )
        .expect("read durable capabilities contract");
        assert!(local_contract.contains(CAPABILITIES_REVISION));
        assert!(durable_contract.contains(CAPABILITIES_REVISION));
    }
}

mod auth {
    use super::harness::*;
    use super::*;
    use crate::backend::{RepoId, StratumStores};

    #[tokio::test]
    async fn fixture_auth_cases_fail_closed() {
        let fixture = load_conformance_fixture();
        for case in cases_with_prefix(&fixture.cases, "auth.") {
            match case.id.as_str() {
                "auth.local.fs-mutation.missing" => {
                    let router = local_router();
                    let (base_url, server) = spawn_test_router(router).await;
                    let response = reqwest::Client::new()
                        .put(format!("{base_url}{}", case.path))
                        .body("conformance")
                        .send()
                        .await
                        .expect("request");
                    assert_auth_case(&fixture, case, response, &[]).await;
                    server.abort();
                }
                "auth.durable.fs-route.missing" => {
                    let router = durable_read_router().await;
                    let (base_url, server) = spawn_test_router(router).await;
                    let response = reqwest::Client::new()
                        .get(format!("{base_url}{}", case.path))
                        .send()
                        .await
                        .expect("request");
                    assert_auth_case(&fixture, case, response, &[]).await;
                    server.abort();
                }
                "auth.durable.workspaces.missing" => {
                    let stores = StratumStores::local_memory();
                    let router = durable_router(
                        stores.workspace_metadata.clone(),
                        RepoId::new("repo_conformance_workspace_auth").expect("repo"),
                        stores,
                    );
                    let (base_url, server) = spawn_test_router(router).await;
                    let response = reqwest::Client::new()
                        .get(format!("{base_url}{}", case.path))
                        .send()
                        .await
                        .expect("request");
                    assert_auth_case(&fixture, case, response, &[]).await;
                    server.abort();
                }
                "auth.durable.workspace-context.missing" => {
                    let (router, ctx) = durable_mutation_router(None).await;
                    let (base_url, server) = spawn_test_router(router).await;
                    let response = reqwest::Client::new()
                        .put(format!("{base_url}{}", case.path))
                        .headers(workspace_bearer_headers(&ctx.raw_secret, ctx.workspace_id))
                        .body("must not commit")
                        .send()
                        .await
                        .expect("request");
                    let workspace_id = ctx.workspace_id.to_string();
                    assert_auth_case(
                        &fixture,
                        case,
                        response,
                        &[ctx.raw_secret.as_str(), workspace_id.as_str()],
                    )
                    .await;
                    server.abort();
                }
                "auth.durable.workspace-context.mismatch" => {
                    let (router, ctx) = durable_cross_repo_router().await;
                    let (base_url, server) = spawn_test_router(router).await;
                    let response = reqwest::Client::new()
                        .put(format!("{base_url}{}", case.path))
                        .headers(workspace_bearer_headers(&ctx.raw_secret, ctx.workspace_id))
                        .body("cross repo blocked")
                        .send()
                        .await
                        .expect("request");
                    let workspace_id = ctx.workspace_id.to_string();
                    assert_auth_case(
                        &fixture,
                        case,
                        response,
                        &[ctx.raw_secret.as_str(), workspace_id.as_str()],
                    )
                    .await;
                    server.abort();
                }
                other => panic!("unknown auth case {other}"),
            }
        }
    }

    async fn assert_auth_case(
        fixture: &ConformanceRoutesFixture,
        case: &ConformanceCaseFixture,
        response: reqwest::Response,
        dynamic_forbidden: &[&str],
    ) {
        let headers = response.headers().clone();
        assert_case_status(case, response.status().as_u16());
        let body_text = response.text().await.expect("body");
        assert_public_response_headers(&case.id, &headers, fixture, dynamic_forbidden);
        assert_public_text(&case.id, &body_text, fixture, dynamic_forbidden);
        if let Some(fragment) = &case.expect.body_contains {
            assert!(
                body_text.contains(fragment),
                "{} expected public body fragment {fragment}",
                case.id,
            );
        }
        if !case.expect.json_paths.is_empty() {
            let body: serde_json::Value =
                serde_json::from_str(&body_text).expect("json error body");
            for (path, expected) in &case.expect.json_paths {
                assert_json_path(&body, path, expected);
            }
        }
    }
}

mod idempotency_local {
    use super::harness::*;
    use super::*;

    #[tokio::test]
    async fn local_write_replay_and_conflict_match_fixture() {
        let fixture = load_conformance_fixture();
        let router = local_router();
        let (base_url, server) = spawn_test_router(router).await;
        let client = reqwest::Client::new();

        let replay_case = fixture
            .cases
            .iter()
            .find(|case| case.id == "idempotency.local.write.replay")
            .expect("replay case");
        let idempotency_key = "conformance-local-write-1";
        let request_body = "conformance-local-body";
        let headers = with_idempotency_key(user_root_headers(), idempotency_key);
        let first = client
            .put(format!("{base_url}{}", replay_case.path))
            .headers(headers.clone())
            .body(request_body)
            .send()
            .await
            .expect("first write");
        assert_case_status(replay_case, first.status().as_u16());
        assert_public_response_headers(
            &replay_case.id,
            first.headers(),
            &fixture,
            &[idempotency_key, request_body],
        );
        let first_body = first.text().await.expect("first body");
        assert_public_text(
            &replay_case.id,
            &first_body,
            &fixture,
            &[idempotency_key, request_body],
        );

        let replay = client
            .put(format!("{base_url}{}", replay_case.path))
            .headers(headers)
            .body(request_body)
            .send()
            .await
            .expect("replay write");
        assert_case_status(replay_case, replay.status().as_u16());
        assert_expected_replay_header(replay_case, replay.headers());
        assert_public_response_headers(
            &replay_case.id,
            replay.headers(),
            &fixture,
            &[idempotency_key, request_body],
        );
        assert_eq!(replay.text().await.expect("replay body"), first_body);

        let conflict_case = fixture
            .cases
            .iter()
            .find(|case| case.id == "idempotency.local.write.conflict")
            .expect("conflict case");
        let conflict_key = "conformance-local-conflict-1";
        let conflict_headers = with_idempotency_key(user_root_headers(), conflict_key);
        let seed = client
            .put(format!("{base_url}{}", conflict_case.path))
            .headers(conflict_headers.clone())
            .body("first-body")
            .send()
            .await
            .expect("seed conflict");
        assert_public_response_headers(
            &conflict_case.id,
            seed.headers(),
            &fixture,
            &[conflict_key, "first-body"],
        );
        let seed_body = seed.text().await.expect("seed body");
        assert_public_text(
            &conflict_case.id,
            &seed_body,
            &fixture,
            &[conflict_key, "first-body"],
        );
        let conflict = client
            .put(format!("{base_url}{}", conflict_case.path))
            .headers(conflict_headers)
            .body("second-body")
            .send()
            .await
            .expect("conflict write");
        assert_case_status(conflict_case, conflict.status().as_u16());
        assert_public_response_headers(
            &conflict_case.id,
            conflict.headers(),
            &fixture,
            &[conflict_key, "second-body"],
        );
        let conflict_body: serde_json::Value = conflict.json().await.expect("conflict json");
        let rendered = conflict_body.to_string();
        assert_public_text(
            &conflict_case.id,
            &rendered,
            &fixture,
            &[conflict_key, "second-body"],
        );
        for (path, expected) in &conflict_case.expect.json_paths {
            assert_json_path(&conflict_body, path, expected);
        }
        server.abort();
    }
}

mod idempotency {
    use super::harness::*;
    use super::*;

    #[tokio::test]
    async fn durable_write_replay_matches_fixture_when_mounted() {
        let fixture = load_conformance_fixture();
        let case = fixture
            .cases
            .iter()
            .find(|case| case.id == "idempotency.durable.write.replay")
            .expect("durable replay case");
        let (router, ctx) =
            durable_mutation_router(Some("agent/conformance/durable-session")).await;
        let (base_url, server) = spawn_test_router(router).await;
        let client = reqwest::Client::new();
        let idempotency_key = "conformance-durable-write-1";
        let request_body = "conformance-durable-body";
        let workspace_id = ctx.workspace_id.to_string();
        let headers = with_idempotency_key(
            workspace_bearer_headers(&ctx.raw_secret, ctx.workspace_id),
            idempotency_key,
        );
        let first = client
            .put(format!("{base_url}{}", case.path))
            .headers(headers.clone())
            .body(request_body)
            .send()
            .await
            .expect("first durable write");
        assert_case_status(case, first.status().as_u16());
        assert_public_response_headers(
            &case.id,
            first.headers(),
            &fixture,
            &[
                ctx.raw_secret.as_str(),
                workspace_id.as_str(),
                idempotency_key,
                request_body,
            ],
        );
        let first_body = first.text().await.expect("first body");
        assert_public_text(
            &case.id,
            &first_body,
            &fixture,
            &[
                ctx.raw_secret.as_str(),
                workspace_id.as_str(),
                idempotency_key,
                request_body,
            ],
        );

        let replay = client
            .put(format!("{base_url}{}", case.path))
            .headers(headers)
            .body(request_body)
            .send()
            .await
            .expect("replay durable write");
        assert_case_status(case, replay.status().as_u16());
        assert_expected_replay_header(case, replay.headers());
        assert_public_response_headers(
            &case.id,
            replay.headers(),
            &fixture,
            &[
                ctx.raw_secret.as_str(),
                workspace_id.as_str(),
                idempotency_key,
                request_body,
            ],
        );
        assert_eq!(replay.text().await.expect("replay body"), first_body);
        server.abort();
    }
}

mod unsupported_durable {
    use super::harness::*;
    use super::*;
    use crate::backend::{RepoId, StratumStores};
    use axum::http::Method;

    #[tokio::test]
    async fn durable_unsupported_routes_match_fixture() {
        let fixture = load_conformance_fixture();
        let stores = StratumStores::local_memory();
        let router = durable_router(
            stores.workspace_metadata.clone(),
            RepoId::new("repo_conformance_unsupported").expect("repo"),
            stores,
        );
        let (base_url, server) = spawn_test_router(router).await;
        let client = reqwest::Client::new();

        for case in cases_with_prefix(&fixture.cases, "unsupported.durable.") {
            let method = Method::from_bytes(case.method.as_bytes()).expect("method");
            let response = client
                .request(method, format!("{base_url}{}", case.path))
                .send()
                .await
                .expect("unsupported request");
            assert_eq!(
                response.status().as_u16(),
                case.expect.status,
                "{}",
                case.id
            );
            let headers = response.headers().clone();
            let body: serde_json::Value = response.json().await.expect("json");
            for (path, expected) in &case.expect.json_paths {
                assert_json_path(&body, path, expected);
            }
            let rendered = body.to_string();
            assert_public_response_headers(&case.id, &headers, &fixture, &[]);
            assert_public_text(&case.id, &rendered, &fixture, &[]);
        }
        server.abort();
    }
}

#[test]
fn update_checked_in_conformance_fixture_when_requested() {
    checked_in_fixture_matches_canonical_serialization();
}

#[test]
fn checked_in_fixture_matches_canonical_serialization() {
    let path = conformance_fixture_path();
    let fixture = load_conformance_fixture();
    let canonical = serde_json::to_value(&fixture).expect("serialize conformance fixture value");
    if std::env::var("STRATUM_UPDATE_CONFORMANCE_FIXTURES").as_deref() == Ok("1") {
        let pretty = serde_json::to_string_pretty(&canonical).expect("pretty fixture");
        std::fs::write(&path, format!("{pretty}\n")).expect("update conformance fixture");
        return;
    }
    let on_disk = std::fs::read_to_string(&path).expect("read conformance fixture");
    let on_disk_value: serde_json::Value =
        serde_json::from_str(&on_disk).expect("parse conformance fixture");
    assert_eq!(
        on_disk_value, canonical,
        "conformance.routes.v1.json is stale; run with STRATUM_UPDATE_CONFORMANCE_FIXTURES=1"
    );
}
