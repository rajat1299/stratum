//! Shared conformance fixture model for local-state and durable-cloud route behavior.

#[cfg(test)]
pub(crate) mod tests {
    use serde::Deserialize;
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    pub const CONFORMANCE_FIXTURE_REVISION: &str = "2026-06-04-1";
    pub const CONFORMANCE_FIXTURE_VERSION: u32 = 1;
    pub const CONFORMANCE_FIXTURE_FILE: &str = "conformance.routes.v1.json";

    const ALLOWED_MODES: &[&str] = &["local-state", "durable-cloud"];
    const ALLOWED_AUTH_PROFILES: &[&str] = &["none", "root-user", "workspace-bearer"];

    #[derive(Debug, Deserialize)]
    struct ConformanceRoutesFixture {
        version: u32,
        revision: String,
        capability_revision: String,
        modes: Vec<String>,
        auth_profiles: HashMap<String, AuthProfileFixture>,
        forbidden_substrings: Vec<String>,
        cases: Vec<ConformanceCaseFixture>,
    }

    #[derive(Debug, Deserialize)]
    struct AuthProfileFixture {
        kind: String,
        #[serde(default)]
        label: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct ConformanceCaseFixture {
        id: String,
        mode: String,
        method: String,
        path: String,
        auth: String,
        expect: CaseExpectFixture,
        sdk: SdkCoverageFixture,
        #[serde(default)]
        idempotency: Option<String>,
        #[serde(default)]
        default_gate: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct CaseExpectFixture {
        status: u16,
        #[serde(default)]
        headers: HashMap<String, String>,
        #[serde(default)]
        json_paths: HashMap<String, serde_json::Value>,
        #[serde(default)]
        body_contains: Option<String>,
    }

    #[derive(Debug, Deserialize)]
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

    pub fn load_conformance_fixture() -> ConformanceRoutesFixture {
        let path = conformance_fixture_path();
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        serde_json::from_str(&raw)
            .unwrap_or_else(|err| panic!("parse {}: {err}", path.display()))
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
            assert!(case.expect.status > 0, "case {} must declare expect.status", case.id);
            let _ = &case.sdk;
        }
    }
}
