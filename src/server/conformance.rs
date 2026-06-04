//! Shared conformance fixture model for local-state and durable-cloud route behavior.

#[cfg(test)]
pub(crate) mod tests {
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    pub const CONFORMANCE_FIXTURE_REVISION: &str = "2026-06-04-1";
    pub const CONFORMANCE_FIXTURE_VERSION: u32 = 1;
    pub const CONFORMANCE_FIXTURE_FILE: &str = "conformance.routes.v1.json";

    const ALLOWED_MODES: &[&str] = &["local-state", "durable-cloud"];
    const ALLOWED_AUTH_PROFILES: &[&str] = &["none", "root-user", "workspace-bearer"];

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
            let _ = &case.sdk;
        }
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

    mod harness {
        use super::*;
        use axum::Router;
        use axum::http::HeaderMap;
        use std::sync::Arc;
        use uuid::Uuid;

        use crate::audit::InMemoryAuditStore;
        use crate::auth::hosted::InMemoryHostedAuthStore;
        use crate::auth::{ROOT_GID, ROOT_UID};
        use crate::backend::runtime::BackendRuntimeMode;
        use crate::backend::{
            CommitRecord, ObjectWrite, OrgId, RefExpectation, RefUpdate, RepoId, StratumStores,
        };
        use crate::db::StratumDb;
        use crate::idempotency::InMemoryIdempotencyStore;
        use crate::review::InMemoryReviewStore;
        use crate::server::repo_context::InMemoryTenantRepoResolver;
        use crate::server::{ServerStores, build_durable_core_router, build_router_with_stores};
        use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
        use crate::store::{ObjectId, ObjectKind};
        use crate::vcs::CommitId;
        use crate::vcs::{MAIN_REF, RefName};
        use crate::workspace::ValidWorkspaceToken;
        use crate::workspace::{
            InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore, WorkspacePrincipalKind,
            WorkspacePrincipalRecord, WorkspaceRecord, WorkspaceTokenRecord,
        };

        pub struct DurableHarnessContext {
            pub workspace_id: Uuid,
            pub raw_secret: String,
        }

        pub async fn spawn_test_router(router: Router) -> (String, tokio::task::JoinHandle<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind conformance router");
            let addr = listener.local_addr().expect("listener address");
            let handle = tokio::spawn(async move {
                axum::serve(listener, router)
                    .await
                    .expect("serve conformance router");
            });
            (format!("http://{addr}"), handle)
        }

        pub fn local_router() -> Router {
            build_router_with_stores(
                StratumDb::open_memory(),
                Arc::new(InMemoryWorkspaceMetadataStore::new()),
                Arc::new(InMemoryIdempotencyStore::new()),
                Arc::new(InMemoryAuditStore::new()),
                Arc::new(InMemoryReviewStore::new()),
            )
        }

        pub fn durable_router(
            workspaces: Arc<dyn WorkspaceMetadataStore>,
            repo_id: RepoId,
            stores: StratumStores,
        ) -> Router {
            build_durable_core_router(
                ServerStores {
                    backend_mode: BackendRuntimeMode::Durable,
                    workspaces,
                    idempotency: stores.idempotency.clone(),
                    audit: stores.audit.clone(),
                    review: stores.review.clone(),
                    hosted_auth: Arc::new(InMemoryHostedAuthStore::new()),
                    tenant_repos: Arc::new(InMemoryTenantRepoResolver::new()),
                    secret_replay_kms: None,
                    guarded_durable_commit_stores: None,
                    durable_core_stores: Some(stores),
                    search_index: crate::server::unavailable_search_index_store(),
                    text_extraction: crate::server::unavailable_text_extraction_store(),
                    embedding_provider: crate::server::unavailable_embedding_provider(),
                },
                repo_id,
            )
        }

        async fn put_object(
            stores: &StratumStores,
            repo_id: &RepoId,
            kind: ObjectKind,
            bytes: Vec<u8>,
        ) -> ObjectId {
            let id = ObjectId::from_bytes(&bytes);
            stores
                .objects
                .put(ObjectWrite {
                    repo_id: repo_id.clone(),
                    id,
                    kind,
                    bytes,
                })
                .await
                .expect("put object");
            id
        }

        fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
            TreeEntry {
                name: name.to_string(),
                kind,
                id,
                mode,
                uid: ROOT_UID,
                gid: ROOT_GID,
                mime_type: None,
                custom_attrs: Default::default(),
            }
        }

        pub async fn seed_durable_read_fixture(stores: &StratumStores) {
            let repo_id = RepoId::local();
            let note_id = put_object(
                stores,
                &repo_id,
                ObjectKind::Blob,
                b"durable conformance read\n".to_vec(),
            )
            .await;
            let docs_tree = put_object(
                stores,
                &repo_id,
                ObjectKind::Tree,
                TreeObject {
                    entries: vec![tree_entry(
                        "nested.txt",
                        TreeEntryKind::Blob,
                        note_id,
                        0o644,
                    )],
                }
                .serialize(),
            )
            .await;
            let root_tree = put_object(
                stores,
                &repo_id,
                ObjectKind::Tree,
                TreeObject {
                    entries: vec![
                        tree_entry("docs", TreeEntryKind::Tree, docs_tree, 0o755),
                        tree_entry("notes.txt", TreeEntryKind::Blob, note_id, 0o644),
                    ],
                }
                .serialize(),
            )
            .await;
            let commit_id = CommitId::from(ObjectId::from_bytes(b"conformance read head"));
            stores
                .commits
                .insert(CommitRecord {
                    repo_id: repo_id.clone(),
                    id: commit_id,
                    root_tree,
                    parents: Vec::new(),
                    timestamp: 1_725_000_010,
                    message: "conformance read".to_string(),
                    author: "conformance".to_string(),
                    changed_paths: Vec::new(),
                })
                .await
                .expect("insert commit");
            stores
                .refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: RefName::new(MAIN_REF).unwrap(),
                    target: commit_id,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .expect("update main ref");
        }

        pub async fn seed_durable_workspace_base(stores: &StratumStores) -> CommitId {
            let repo_id = RepoId::local();
            let demo_tree = put_object(
                stores,
                &repo_id,
                ObjectKind::Tree,
                TreeObject { entries: vec![] }.serialize(),
            )
            .await;
            let root_tree = put_object(
                stores,
                &repo_id,
                ObjectKind::Tree,
                TreeObject {
                    entries: vec![tree_entry("demo", TreeEntryKind::Tree, demo_tree, 0o755)],
                }
                .serialize(),
            )
            .await;
            let commit_id = CommitId::from(ObjectId::from_bytes(b"conformance workspace base"));
            stores
                .commits
                .insert(CommitRecord {
                    repo_id: repo_id.clone(),
                    id: commit_id,
                    root_tree,
                    parents: Vec::new(),
                    timestamp: 1_725_000_011,
                    message: "conformance workspace base".to_string(),
                    author: "conformance".to_string(),
                    changed_paths: Vec::new(),
                })
                .await
                .expect("insert workspace base");
            stores
                .refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: RefName::new(MAIN_REF).unwrap(),
                    target: commit_id,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .expect("update main");
            commit_id
        }

        struct DurableWorkspaceBearerStore {
            workspace: WorkspaceRecord,
            token: WorkspaceTokenRecord,
            principal: WorkspacePrincipalRecord,
            raw_secret: String,
        }

        #[async_trait::async_trait]
        impl WorkspaceMetadataStore for DurableWorkspaceBearerStore {
            async fn list_workspaces(
                &self,
            ) -> Result<Vec<WorkspaceRecord>, crate::error::VfsError> {
                Ok(vec![self.workspace.clone()])
            }

            async fn create_workspace(
                &self,
                _name: &str,
                _root_path: &str,
            ) -> Result<WorkspaceRecord, crate::error::VfsError> {
                unreachable!("conformance durable bearer store is read-only")
            }

            async fn get_workspace(
                &self,
                id: Uuid,
            ) -> Result<Option<WorkspaceRecord>, crate::error::VfsError> {
                Ok((id == self.workspace.id).then(|| self.workspace.clone()))
            }

            async fn update_head_commit(
                &self,
                _id: Uuid,
                _head_commit: Option<String>,
            ) -> Result<Option<WorkspaceRecord>, crate::error::VfsError> {
                unreachable!("conformance durable bearer store is read-only")
            }

            async fn update_head_commit_if_current(
                &self,
                _id: Uuid,
                _expected_head_commit: Option<&str>,
                _head_commit: Option<String>,
            ) -> Result<Option<WorkspaceRecord>, crate::error::VfsError> {
                unreachable!("conformance durable bearer store is read-only")
            }

            async fn validate_workspace_token_at(
                &self,
                workspace_id: Uuid,
                raw_secret: &str,
                _now_unix: u64,
            ) -> Result<Option<ValidWorkspaceToken>, crate::error::VfsError> {
                if workspace_id != self.workspace.id || raw_secret != self.raw_secret {
                    return Ok(None);
                }
                Ok(Some(ValidWorkspaceToken {
                    workspace: self.workspace.clone(),
                    token: self.token.clone(),
                    org_id: self.workspace.org_id.clone(),
                    repo_id: self.workspace.repo_id.clone(),
                    principal: Some(self.principal.clone()),
                }))
            }
        }

        pub async fn durable_mutation_router(
            session_ref: Option<&str>,
        ) -> (Router, DurableHarnessContext) {
            let stores = StratumStores::local_memory();
            seed_durable_workspace_base(&stores).await;
            let workspace_id = Uuid::new_v4();
            let raw_secret = format!("conformance-durable-token-{workspace_id}");
            let workspace = WorkspaceRecord {
                id: workspace_id,
                name: "conformance-durable".to_string(),
                root_path: "/demo".to_string(),
                head_commit: None,
                version: 1,
                base_ref: MAIN_REF.to_string(),
                session_ref: session_ref.map(str::to_string),
                org_id: None,
                repo_id: Some(RepoId::local().as_str().to_string()),
            };
            let token = WorkspaceTokenRecord {
                id: Uuid::new_v4(),
                workspace_id,
                name: "conformance-token".to_string(),
                agent_uid: ROOT_UID,
                secret_hash: "redacted".to_string(),
                read_prefixes: vec!["/demo".to_string()],
                write_prefixes: vec!["/demo".to_string()],
                principal_uid: Some(ROOT_UID),
                token_version: 1,
                issued_at_unix: 1,
                updated_at_unix: 1,
                expires_at_unix: None,
                revoked_at_unix: None,
                org_id: None,
            };
            let principal = WorkspacePrincipalRecord {
                uid: ROOT_UID,
                username: "conformance-principal".to_string(),
                gid: ROOT_GID,
                groups: vec![ROOT_GID],
                kind: WorkspacePrincipalKind::Agent,
                active: true,
                org_id: None,
            };
            let workspaces: Arc<dyn WorkspaceMetadataStore> =
                Arc::new(DurableWorkspaceBearerStore {
                    workspace,
                    token,
                    principal,
                    raw_secret: raw_secret.clone(),
                });
            let router = durable_router(workspaces, RepoId::local(), stores);
            (
                router,
                DurableHarnessContext {
                    workspace_id,
                    raw_secret,
                },
            )
        }

        pub async fn durable_read_router() -> Router {
            let stores = StratumStores::local_memory();
            seed_durable_read_fixture(&stores).await;
            let (workspaces, _workspace_id, _raw_secret) =
                durable_workspace_bearer_store(&RepoId::local());
            durable_router(workspaces, RepoId::local(), stores)
        }

        fn durable_workspace_bearer_store(
            repo_id: &RepoId,
        ) -> (Arc<dyn WorkspaceMetadataStore>, Uuid, String) {
            let workspace_id = Uuid::new_v4();
            let raw_secret = format!("conformance-read-token-{workspace_id}");
            let workspace = WorkspaceRecord {
                id: workspace_id,
                name: "conformance-read".to_string(),
                root_path: "/".to_string(),
                head_commit: None,
                version: 1,
                base_ref: MAIN_REF.to_string(),
                session_ref: Some("agent/conformance/read".to_string()),
                org_id: None,
                repo_id: Some(repo_id.as_str().to_string()),
            };
            let token = WorkspaceTokenRecord {
                id: Uuid::new_v4(),
                workspace_id,
                name: "conformance-read-token".to_string(),
                agent_uid: ROOT_UID,
                secret_hash: "redacted".to_string(),
                read_prefixes: vec!["/".to_string()],
                write_prefixes: Vec::new(),
                principal_uid: Some(ROOT_UID),
                token_version: 1,
                issued_at_unix: 1,
                updated_at_unix: 1,
                expires_at_unix: None,
                revoked_at_unix: None,
                org_id: None,
            };
            let principal = WorkspacePrincipalRecord {
                uid: ROOT_UID,
                username: "conformance-read-principal".to_string(),
                gid: ROOT_GID,
                groups: vec![ROOT_GID],
                kind: WorkspacePrincipalKind::Agent,
                active: true,
                org_id: None,
            };
            (
                Arc::new(DurableWorkspaceBearerStore {
                    workspace,
                    token,
                    principal,
                    raw_secret: raw_secret.clone(),
                }),
                workspace_id,
                raw_secret,
            )
        }

        pub async fn durable_cross_repo_router() -> (Router, DurableHarnessContext) {
            let stores = StratumStores::local_memory();
            let repo_a = RepoId::new("repo_conformance_a").expect("repo a");
            let repo_b = RepoId::new("repo_conformance_b").expect("repo b");
            seed_durable_workspace_base(&stores).await;
            let workspace_id = Uuid::new_v4();
            let raw_secret = format!("conformance-cross-token-{workspace_id}");
            let workspace = WorkspaceRecord {
                id: workspace_id,
                name: "conformance-cross".to_string(),
                root_path: "/demo".to_string(),
                head_commit: None,
                version: 1,
                base_ref: MAIN_REF.to_string(),
                session_ref: Some("agent/conformance/cross".to_string()),
                org_id: None,
                repo_id: Some(repo_b.as_str().to_string()),
            };
            let token = WorkspaceTokenRecord {
                id: Uuid::new_v4(),
                workspace_id,
                name: "conformance-cross-token".to_string(),
                agent_uid: ROOT_UID,
                secret_hash: "redacted".to_string(),
                read_prefixes: vec!["/demo".to_string()],
                write_prefixes: vec!["/demo".to_string()],
                principal_uid: Some(ROOT_UID),
                token_version: 1,
                issued_at_unix: 1,
                updated_at_unix: 1,
                expires_at_unix: None,
                revoked_at_unix: None,
                org_id: None,
            };
            let principal = WorkspacePrincipalRecord {
                uid: ROOT_UID,
                username: "conformance-cross-principal".to_string(),
                gid: ROOT_GID,
                groups: vec![ROOT_GID],
                kind: WorkspacePrincipalKind::Agent,
                active: true,
                org_id: None,
            };
            let workspaces: Arc<dyn WorkspaceMetadataStore> =
                Arc::new(DurableWorkspaceBearerStore {
                    workspace,
                    token,
                    principal,
                    raw_secret: raw_secret.clone(),
                });
            let router = durable_router(workspaces, repo_a, stores);
            (
                router,
                DurableHarnessContext {
                    workspace_id,
                    raw_secret,
                },
            )
        }

        pub fn workspace_bearer_headers(raw_secret: &str, workspace_id: Uuid) -> HeaderMap {
            let mut headers = HeaderMap::new();
            headers.insert(
                "authorization",
                format!("Bearer {raw_secret}")
                    .parse()
                    .expect("authorization"),
            );
            headers.insert(
                "x-stratum-org",
                OrgId::default_org().as_str().parse().expect("org"),
            );
            headers.insert(
                "x-stratum-workspace",
                workspace_id.to_string().parse().expect("workspace"),
            );
            headers
        }

        pub fn user_root_headers() -> HeaderMap {
            let mut headers = HeaderMap::new();
            headers.insert("authorization", "User root".parse().expect("user auth"));
            headers
        }

        pub fn with_idempotency_key(mut headers: HeaderMap, key: &str) -> HeaderMap {
            headers.insert("idempotency-key", key.parse().expect("idempotency key"));
            headers
        }

        pub fn assert_json_path(
            body: &serde_json::Value,
            path: &str,
            expected: &serde_json::Value,
        ) {
            let actual = json_path_value(body, path);
            assert_eq!(
                actual,
                expected.clone(),
                "json path {path} mismatch: actual={actual} expected={expected}"
            );
        }

        fn json_path_value(body: &serde_json::Value, path: &str) -> serde_json::Value {
            assert!(path.starts_with("$."), "unsupported json path {path}");
            let mut current = body;
            for segment in path.trim_start_matches("$.").split('.') {
                if segment.contains('[') {
                    panic!("unsupported json path segment {segment}");
                }
                current = &current[segment];
            }
            current.clone()
        }

        #[allow(clippy::needless_lifetimes)]
        pub fn cases_with_prefix<'a>(
            cases: &'a [ConformanceCaseFixture],
            prefix: &str,
        ) -> Vec<&'a ConformanceCaseFixture> {
            cases
                .iter()
                .filter(|case| case.id.starts_with(prefix))
                .collect()
        }
    }

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
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("sdk/contracts/capabilities.v1.json"),
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
                        assert_auth_case(case, response).await;
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
                        assert_auth_case(case, response).await;
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
                        assert_auth_case(case, response).await;
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
                        assert_auth_case(case, response).await;
                        server.abort();
                    }
                    other => panic!("unknown auth case {other}"),
                }
            }
        }

        async fn assert_auth_case(case: &ConformanceCaseFixture, response: reqwest::Response) {
            assert_eq!(
                response.status().as_u16(),
                case.expect.status,
                "{}",
                case.id
            );
            let body_text = response.text().await.expect("body");
            if let Some(fragment) = &case.expect.body_contains {
                assert!(
                    body_text.contains(fragment),
                    "{} expected body to contain {fragment}, got {body_text}",
                    case.id
                );
            }
            if !case.expect.json_paths.is_empty() {
                let body: serde_json::Value =
                    serde_json::from_str(&body_text).expect("json error body");
                for (path, expected) in &case.expect.json_paths {
                    assert_json_path(&body, path, expected);
                }
            }
            for forbidden in &load_conformance_fixture().forbidden_substrings {
                if forbidden == "Bearer " {
                    continue;
                }
                assert!(
                    !body_text.contains(forbidden),
                    "{} leaked forbidden substring {forbidden}",
                    case.id
                );
            }
        }
    }

    mod idempotency_local {
        use super::harness::*;
        use super::*;
        use crate::server::idempotency::{IDEMPOTENCY_CONFLICT_MESSAGE, IDEMPOTENCY_REPLAY_HEADER};

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
            let headers = with_idempotency_key(user_root_headers(), "conformance-local-write-1");
            let first = client
                .put(format!("{base_url}{}", replay_case.path))
                .headers(headers.clone())
                .body("conformance-local-body")
                .send()
                .await
                .expect("first write");
            assert_eq!(first.status().as_u16(), 200);
            let first_body = first.text().await.expect("first body");

            let replay = client
                .put(format!("{base_url}{}", replay_case.path))
                .headers(headers)
                .body("conformance-local-body")
                .send()
                .await
                .expect("replay write");
            assert_eq!(replay.status().as_u16(), 200);
            assert_eq!(
                replay
                    .headers()
                    .get(IDEMPOTENCY_REPLAY_HEADER)
                    .and_then(|value| value.to_str().ok()),
                Some("true")
            );
            assert_eq!(replay.text().await.expect("replay body"), first_body);

            let conflict_case = fixture
                .cases
                .iter()
                .find(|case| case.id == "idempotency.local.write.conflict")
                .expect("conflict case");
            let conflict_headers =
                with_idempotency_key(user_root_headers(), "conformance-local-conflict-1");
            let _ = client
                .put(format!("{base_url}{}", conflict_case.path))
                .headers(conflict_headers.clone())
                .body("first-body")
                .send()
                .await
                .expect("seed conflict");
            let conflict = client
                .put(format!("{base_url}{}", conflict_case.path))
                .headers(conflict_headers)
                .body("second-body")
                .send()
                .await
                .expect("conflict write");
            assert_eq!(conflict.status().as_u16(), 409);
            let conflict_body: serde_json::Value = conflict.json().await.expect("conflict json");
            assert_eq!(
                conflict_body["error"].as_str(),
                Some(IDEMPOTENCY_CONFLICT_MESSAGE)
            );
            server.abort();
        }
    }

    mod idempotency {
        use super::harness::*;
        use super::*;
        use crate::server::idempotency::IDEMPOTENCY_REPLAY_HEADER;

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
            let headers = with_idempotency_key(
                workspace_bearer_headers(&ctx.raw_secret, ctx.workspace_id),
                "conformance-durable-write-1",
            );
            let first = client
                .put(format!("{base_url}{}", case.path))
                .headers(headers.clone())
                .body("conformance-durable-body")
                .send()
                .await
                .expect("first durable write");
            assert_eq!(
                first.status().as_u16(),
                200,
                "{}",
                first.text().await.unwrap_or_default()
            );
            let first_body = first.text().await.expect("first body");

            let replay = client
                .put(format!("{base_url}{}", case.path))
                .headers(headers)
                .body("conformance-durable-body")
                .send()
                .await
                .expect("replay durable write");
            assert_eq!(replay.status().as_u16(), 200);
            assert_eq!(
                replay
                    .headers()
                    .get(IDEMPOTENCY_REPLAY_HEADER)
                    .and_then(|value| value.to_str().ok()),
                Some("true")
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
                let body: serde_json::Value = response.json().await.expect("json");
                for (path, expected) in &case.expect.json_paths {
                    assert_json_path(&body, path, expected);
                }
                let rendered = body.to_string();
                for forbidden in &fixture.forbidden_substrings {
                    if forbidden == "Bearer " {
                        continue;
                    }
                    assert!(
                        !rendered.contains(forbidden),
                        "{} leaked {forbidden}",
                        case.id
                    );
                }
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
        let canonical =
            serde_json::to_value(&fixture).expect("serialize conformance fixture value");
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
}
