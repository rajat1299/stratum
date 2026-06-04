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
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, crate::error::VfsError> {
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

pub async fn durable_mutation_router(session_ref: Option<&str>) -> (Router, DurableHarnessContext) {
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
    let workspaces: Arc<dyn WorkspaceMetadataStore> = Arc::new(DurableWorkspaceBearerStore {
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
    let (workspaces, _workspace_id, _raw_secret) = durable_workspace_bearer_store(&RepoId::local());
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
    let workspaces: Arc<dyn WorkspaceMetadataStore> = Arc::new(DurableWorkspaceBearerStore {
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

pub fn assert_json_path(body: &serde_json::Value, path: &str, expected: &serde_json::Value) {
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

pub fn assert_case_status(case: &ConformanceCaseFixture, status: u16) {
    assert_eq!(status, case.expect.status, "{}", case.id);
}

pub fn assert_expected_replay_header(case: &ConformanceCaseFixture, headers: &HeaderMap) {
    let header = case
        .expect
        .replay_header
        .as_deref()
        .expect("idempotency replay case must declare replay_header");
    assert_eq!(
        headers.get(header).and_then(|value| value.to_str().ok()),
        Some("true"),
        "{} replay header {}",
        case.id,
        header
    );
}

pub fn assert_public_response_headers(
    case_id: &str,
    headers: &HeaderMap,
    fixture: &ConformanceRoutesFixture,
    dynamic_forbidden: &[&str],
) {
    let mut rendered = String::new();
    for (name, value) in headers {
        rendered.push_str(name.as_str());
        rendered.push(':');
        if let Ok(value) = value.to_str() {
            rendered.push_str(value);
        }
        rendered.push('\n');
    }
    assert_public_text(case_id, &rendered, fixture, dynamic_forbidden);
}

pub fn assert_public_text(
    case_id: &str,
    text: &str,
    fixture: &ConformanceRoutesFixture,
    dynamic_forbidden: &[&str],
) {
    for forbidden in fixture
        .forbidden_substrings
        .iter()
        .map(String::as_str)
        .chain(dynamic_forbidden.iter().copied())
        .filter(|forbidden| !forbidden.is_empty())
    {
        assert!(
            !text.contains(forbidden),
            "{case_id} leaked forbidden response fragment {forbidden}"
        );
    }
}

pub fn cases_with_prefix<'a>(
    cases: &'a [ConformanceCaseFixture],
    prefix: &'a str,
) -> impl Iterator<Item = &'a ConformanceCaseFixture> {
    cases.iter().filter(move |case| case.id.starts_with(prefix))
}
