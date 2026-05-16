use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::{AppState, ServerRuntimeKind, ServerState};
use crate::backend::runtime::BackendRuntimeMode;

pub const CAPABILITIES_REVISION: &str = "2026-05-15-1";
pub const CAPABILITIES_CACHE_CONTROL: &str = "max-age=60, must-revalidate";

const UNSUPPORTED_DURABLE_CLOUD_REASON: &str = "durable-cloud route is not supported yet";
const SEMANTIC_SEARCH_TRACKING_REF: &str = "execution-roadmap section 3";
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_MAX_INODES: u64 = 1_000_000;
const DEFAULT_MAX_DEPTH: u64 = 256;
const DEFAULT_AUDIT_LIMIT: u64 = 100;
const MAX_AUDIT_LIMIT: u64 = 1000;
const LOG_MAX_LIMIT: u64 = 1000;
const MAX_TEXT_DIFF_BYTES: u64 = 512 * 1024;
const MAX_TEXT_DIFF_CELLS: u64 = 4_000_000;
const DIFF_CONTEXT_LINES: u64 = 3;
const REQUIRED_APPROVALS_MAX: u64 = 16;
const IDEMPOTENCY_MAX_KEY_BYTES: u64 = 255;
const IDEMPOTENCY_STALE_PENDING_SECONDS: u64 = 60;
const IDEMPOTENCY_COMPLETED_RETENTION_SECONDS: u64 = 86_400;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub revision: String,
    pub server: ServerCapabilities,
    pub auth: AuthCapabilities,
    pub routes: RouteCapabilities,
    pub diff: DiffCapabilities,
    pub protection: ProtectionCapabilities,
    pub idempotency: IdempotencyCapabilities,
    pub recovery: RecoveryCapabilities,
    pub limits: LimitCapabilities,
    pub hints: HintCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerCapabilities {
    pub name: String,
    pub version: String,
    pub build: Option<String>,
    pub backend_mode: String,
    pub core_runtime: String,
    pub build_features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthCapabilities {
    pub modes: Vec<String>,
    pub providers: Vec<AuthProviderCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthProviderCapability {
    pub id: String,
    pub label: String,
    #[serde(rename = "default")]
    pub default_provider: bool,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteCapabilities {
    pub filesystem: FilesystemRouteCapabilities,
    pub search: SearchRouteCapabilities,
    pub vcs: VcsRouteCapabilities,
    pub review: ReviewRouteCapabilities,
    pub workspaces: WorkspaceRouteCapabilities,
    pub audit: RouteOperationCapability,
    pub runs: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemRouteCapabilities {
    pub read: RouteOperationCapability,
    pub list: RouteOperationCapability,
    pub stat: RouteOperationCapability,
    pub write: RouteOperationCapability,
    pub delete: RouteOperationCapability,
    pub patch: RouteOperationCapability,
    pub copy: RouteOperationCapability,
    #[serde(rename = "move")]
    pub move_: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchRouteCapabilities {
    pub grep: RouteOperationCapability,
    pub find: RouteOperationCapability,
    pub tree: RouteOperationCapability,
    pub semantic: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRouteCapabilities {
    pub log: RouteOperationCapability,
    pub status: RouteOperationCapability,
    pub diff: RouteOperationCapability,
    pub refs: VcsRefRouteCapabilities,
    pub commit: RouteOperationCapability,
    pub revert: RouteOperationCapability,
    pub recovery: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRefRouteCapabilities {
    pub list: RouteOperationCapability,
    pub create: RouteOperationCapability,
    pub update: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewRouteCapabilities {
    pub change_requests: RouteOperationCapability,
    pub approvals: RouteOperationCapability,
    pub reviewers: RouteOperationCapability,
    pub comments: RouteOperationCapability,
    pub merge: RouteOperationCapability,
    pub reject: RouteOperationCapability,
    pub dismiss: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRouteCapabilities {
    pub list: RouteOperationCapability,
    pub create: RouteOperationCapability,
    pub issue_token: RouteOperationCapability,
    pub revoke_token: RouteOperationCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteOperationCapability {
    pub available: bool,
    pub admin: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracking_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_when: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffCapabilities {
    pub format: String,
    pub max_text_diff_bytes: u64,
    pub max_text_diff_cells: u64,
    pub context_lines: u64,
    pub supported_fragment_kinds: Vec<String>,
    pub json_format_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectionCapabilities {
    pub ref_rules: ProtectionRuleCapabilities,
    pub path_rules: ProtectionRuleCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectionRuleCapabilities {
    pub available: bool,
    pub required_approvals_max: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_ref_optional: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyCapabilities {
    pub header: String,
    pub max_key_bytes: u64,
    pub stale_pending_seconds: u64,
    pub completed_retention_seconds: u64,
    pub endpoints_supported: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCapabilities {
    pub available: bool,
    pub phases: Vec<String>,
    pub destructive_cleanup_enabled: bool,
    pub scheduler_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitCapabilities {
    pub max_file_size_bytes: u64,
    pub max_inodes: u64,
    pub max_depth: u64,
    pub audit_default_limit: u64,
    pub audit_max_limit: u64,
    pub log_max_limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HintCapabilities {
    pub banner: Option<serde_json::Value>,
    pub branding: Option<serde_json::Value>,
    pub support_url: Option<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/capabilities", get(get_capabilities))
}

async fn get_capabilities(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CACHE_CONTROL, CAPABILITIES_CACHE_CONTROL)],
        Json(manifest_for_state(&state)),
    )
}

pub(crate) fn manifest_for_state(state: &ServerState) -> CapabilityManifest {
    let durable_cloud = state.db.runtime_kind() == ServerRuntimeKind::DurableCloud;
    let guarded_durable = state.core.guarded_durable_commit_route().is_some();
    let backend_mode = match state.db.backend_mode() {
        BackendRuntimeMode::Local => "local",
        BackendRuntimeMode::Durable => "durable",
    };
    let core_runtime = match state.db.runtime_kind() {
        ServerRuntimeKind::LocalState => "local-state",
        ServerRuntimeKind::DurableCloud => "durable-cloud",
    };
    let recovery_available = guarded_durable;
    let scheduler_present = guarded_durable || durable_cloud;

    CapabilityManifest {
        revision: CAPABILITIES_REVISION.to_string(),
        server: ServerCapabilities {
            name: "stratum".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            build: None,
            backend_mode: backend_mode.to_string(),
            core_runtime: core_runtime.to_string(),
            build_features: build_features(),
        },
        auth: auth_capabilities(durable_cloud),
        routes: route_capabilities(durable_cloud, recovery_available),
        diff: diff_capabilities(),
        protection: protection_capabilities(!durable_cloud),
        idempotency: idempotency_capabilities(durable_cloud),
        recovery: recovery_capabilities(recovery_available, scheduler_present),
        limits: LimitCapabilities {
            max_file_size_bytes: DEFAULT_MAX_FILE_SIZE_BYTES,
            max_inodes: DEFAULT_MAX_INODES,
            max_depth: DEFAULT_MAX_DEPTH,
            audit_default_limit: DEFAULT_AUDIT_LIMIT,
            audit_max_limit: MAX_AUDIT_LIMIT,
            log_max_limit: LOG_MAX_LIMIT,
        },
        hints: HintCapabilities {
            banner: None,
            branding: None,
            support_url: Some("https://stratum.dev/support".to_string()),
        },
    }
}

fn build_features() -> Vec<String> {
    let mut features = Vec::new();
    if cfg!(feature = "postgres") {
        features.push("postgres".to_string());
    }
    features
}

fn auth_capabilities(durable_cloud: bool) -> AuthCapabilities {
    let modes = if durable_cloud {
        vec!["workspace".to_string()]
    } else {
        vec![
            "user".to_string(),
            "bearer".to_string(),
            "workspace".to_string(),
        ]
    };

    AuthCapabilities {
        modes,
        providers: vec![
            AuthProviderCapability {
                id: "local".to_string(),
                label: "Local users".to_string(),
                default_provider: true,
                available: !durable_cloud,
            },
            AuthProviderCapability {
                id: "oidc".to_string(),
                label: "SSO (OIDC)".to_string(),
                default_provider: false,
                available: false,
            },
        ],
    }
}

fn route_capabilities(durable_cloud: bool, recovery_available: bool) -> RouteCapabilities {
    RouteCapabilities {
        filesystem: filesystem_routes(durable_cloud),
        search: search_routes(),
        vcs: vcs_routes(durable_cloud, recovery_available),
        review: review_routes(!durable_cloud),
        workspaces: workspace_routes(!durable_cloud),
        audit: admin_route(!durable_cloud),
        runs: runs_route(!durable_cloud),
    }
}

fn filesystem_routes(durable_cloud: bool) -> FilesystemRouteCapabilities {
    FilesystemRouteCapabilities {
        read: route(true, false),
        list: route(true, false),
        stat: route(true, false),
        write: filesystem_mutation(durable_cloud),
        delete: filesystem_mutation(durable_cloud),
        patch: filesystem_mutation(durable_cloud),
        copy: filesystem_mutation(durable_cloud),
        move_: filesystem_mutation(durable_cloud),
    }
}

fn filesystem_mutation(durable_cloud: bool) -> RouteOperationCapability {
    let mut capability = mutation(true, false);
    if durable_cloud {
        capability.requires = vec![
            "workspace-bearer".to_string(),
            "durable-session-ref".to_string(),
        ];
    }
    capability
}

fn search_routes() -> SearchRouteCapabilities {
    SearchRouteCapabilities {
        grep: route(true, false),
        find: route(true, false),
        tree: route(true, false),
        semantic: RouteOperationCapability {
            available: false,
            admin: false,
            idempotent: None,
            reason: Some("not implemented".to_string()),
            tracking_ref: Some(SEMANTIC_SEARCH_TRACKING_REF.to_string()),
            blocked_when: Vec::new(),
            requires: Vec::new(),
            execution: None,
            notes: None,
        },
    }
}

fn vcs_routes(durable_cloud: bool, recovery_available: bool) -> VcsRouteCapabilities {
    VcsRouteCapabilities {
        log: route(true, true),
        status: route(true, true),
        diff: route(true, true),
        refs: refs_route(durable_cloud),
        commit: mutation(!durable_cloud, true).with_blocked_when(durable_cloud, "durable-cloud"),
        revert: mutation(!durable_cloud, true).with_blocked_when(durable_cloud, "durable-cloud"),
        recovery: RouteOperationCapability {
            available: recovery_available,
            admin: true,
            idempotent: None,
            reason: (!recovery_available).then(|| {
                if durable_cloud {
                    UNSUPPORTED_DURABLE_CLOUD_REASON.to_string()
                } else {
                    "guarded durable commit recovery is not enabled".to_string()
                }
            }),
            tracking_ref: None,
            blocked_when: Vec::new(),
            requires: vec!["guarded-durable-commit-route".to_string()],
            execution: None,
            notes: None,
        },
    }
}

fn review_routes(available: bool) -> ReviewRouteCapabilities {
    ReviewRouteCapabilities {
        change_requests: mutation(available, true),
        approvals: mutation(available, true),
        reviewers: mutation(available, true),
        comments: mutation(available, true),
        merge: mutation(available, true),
        reject: mutation(available, true),
        dismiss: mutation(available, true),
    }
}

fn workspace_routes(available: bool) -> WorkspaceRouteCapabilities {
    let unsupported_reason = || UNSUPPORTED_DURABLE_CLOUD_REASON.to_string();
    WorkspaceRouteCapabilities {
        list: admin_route(available),
        create: mutation(available, true),
        issue_token: RouteOperationCapability {
            available,
            admin: true,
            idempotent: Some(false),
            reason: Some(if available {
                "secret-bearing response; idempotency replay unsafe".to_string()
            } else {
                unsupported_reason()
            }),
            tracking_ref: None,
            blocked_when: Vec::new(),
            requires: Vec::new(),
            execution: None,
            notes: None,
        },
        revoke_token: RouteOperationCapability {
            available,
            admin: true,
            idempotent: Some(false),
            reason: (!available).then(unsupported_reason),
            tracking_ref: None,
            blocked_when: Vec::new(),
            requires: Vec::new(),
            execution: None,
            notes: available.then(|| {
                "Idempotency-Key is not supported for workspace-token revocation.".to_string()
            }),
        },
    }
}

fn runs_route(available: bool) -> RouteOperationCapability {
    RouteOperationCapability {
        available,
        admin: false,
        idempotent: Some(available),
        reason: (!available).then(|| UNSUPPORTED_DURABLE_CLOUD_REASON.to_string()),
        tracking_ref: None,
        blocked_when: Vec::new(),
        requires: Vec::new(),
        execution: Some(false),
        notes: Some("Phase-1 record only; no execution scheduler yet.".to_string()),
    }
}

fn admin_route(available: bool) -> RouteOperationCapability {
    route(available, true)
}

fn route(available: bool, admin: bool) -> RouteOperationCapability {
    RouteOperationCapability {
        available,
        admin,
        idempotent: None,
        reason: (!available).then(|| UNSUPPORTED_DURABLE_CLOUD_REASON.to_string()),
        tracking_ref: None,
        blocked_when: Vec::new(),
        requires: Vec::new(),
        execution: None,
        notes: None,
    }
}

fn mutation(available: bool, admin: bool) -> RouteOperationCapability {
    RouteOperationCapability {
        idempotent: Some(available),
        ..route(available, admin)
    }
}

fn refs_route(durable_cloud: bool) -> VcsRefRouteCapabilities {
    VcsRefRouteCapabilities {
        list: route(true, true),
        create: mutation(!durable_cloud, true).with_blocked_when(durable_cloud, "durable-cloud"),
        update: mutation(!durable_cloud, true).with_blocked_when(durable_cloud, "durable-cloud"),
    }
}

impl RouteOperationCapability {
    fn with_blocked_when(mut self, blocked: bool, mode: &str) -> Self {
        if blocked {
            self.reason = Some(UNSUPPORTED_DURABLE_CLOUD_REASON.to_string());
            self.blocked_when.push(mode.to_string());
        }
        self
    }
}

fn diff_capabilities() -> DiffCapabilities {
    DiffCapabilities {
        format: "text/v1".to_string(),
        max_text_diff_bytes: MAX_TEXT_DIFF_BYTES,
        max_text_diff_cells: MAX_TEXT_DIFF_CELLS,
        context_lines: DIFF_CONTEXT_LINES,
        supported_fragment_kinds: vec![
            "text-unified".to_string(),
            "metadata-only".to_string(),
            "binary".to_string(),
            "too-large".to_string(),
            "kind-changed".to_string(),
        ],
        json_format_available: false,
    }
}

fn protection_capabilities(available: bool) -> ProtectionCapabilities {
    ProtectionCapabilities {
        ref_rules: ProtectionRuleCapabilities {
            available,
            required_approvals_max: REQUIRED_APPROVALS_MAX,
            target_ref_optional: None,
        },
        path_rules: ProtectionRuleCapabilities {
            available,
            required_approvals_max: REQUIRED_APPROVALS_MAX,
            target_ref_optional: Some(true),
        },
    }
}

fn idempotency_capabilities(durable_cloud: bool) -> IdempotencyCapabilities {
    let endpoints_supported = if durable_cloud {
        vec![
            "PUT /fs/{path}",
            "PATCH /fs/{path}",
            "DELETE /fs/{path}",
            "POST /fs/{path}?op=copy|move",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    } else {
        vec![
            "PUT /fs/{path}",
            "PATCH /fs/{path}",
            "DELETE /fs/{path}",
            "POST /fs/{path}?op=copy|move",
            "POST /runs",
            "POST /vcs/commit",
            "POST /vcs/revert",
            "POST /vcs/refs",
            "PATCH /vcs/refs/{name}",
            "POST /protected/refs",
            "POST /protected/paths",
            "POST /change-requests",
            "POST /change-requests/{id}/approvals",
            "POST /change-requests/{id}/reviewers",
            "POST /change-requests/{id}/comments",
            "POST /change-requests/{id}/reject",
            "POST /change-requests/{id}/merge",
            "POST /change-requests/{id}/approvals/{approval_id}/dismiss",
            "POST /workspaces",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    };

    IdempotencyCapabilities {
        header: "Idempotency-Key".to_string(),
        max_key_bytes: IDEMPOTENCY_MAX_KEY_BYTES,
        stale_pending_seconds: IDEMPOTENCY_STALE_PENDING_SECONDS,
        completed_retention_seconds: IDEMPOTENCY_COMPLETED_RETENTION_SECONDS,
        endpoints_supported,
    }
}

fn recovery_capabilities(available: bool, scheduler_present: bool) -> RecoveryCapabilities {
    RecoveryCapabilities {
        available,
        phases: vec![
            "pre_visibility".to_string(),
            "post_cas".to_string(),
            "fs_mutations".to_string(),
            "object_cleanup".to_string(),
        ],
        destructive_cleanup_enabled: false,
        scheduler_present,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditStore;
    use crate::backend::{RepoId, StratumStores};
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::review::InMemoryReviewStore;
    use crate::server::core::{DurableCoreRuntime, LocalCoreRuntime};
    use crate::server::{ServerLocalDb, ServerState, ServerStores};
    use crate::workspace::InMemoryWorkspaceMetadataStore;
    use axum::Router;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn local_capabilities_route_is_unauthenticated_and_cacheable() {
        let router = Router::new().merge(routes()).with_state(local_state());
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/v1/capabilities"))
            .send()
            .await
            .expect("capabilities request should complete");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("max-age=60, must-revalidate")
        );
        let body: CapabilityManifest = response.json().await.expect("manifest is json");
        assert_eq!(body.revision, "2026-05-15-1");
        assert_eq!(body.server.core_runtime, "local-state");
        assert!(body.routes.filesystem.write.available);
        assert!(!body.routes.vcs.recovery.available);
        server.abort();
    }

    #[tokio::test]
    async fn durable_cloud_capabilities_advertise_mounted_session_fs_mutations() {
        let router = Router::new()
            .merge(routes())
            .with_state(durable_cloud_state());
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/v1/capabilities"))
            .send()
            .await
            .expect("capabilities request should complete");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: CapabilityManifest = response.json().await.expect("manifest is json");
        assert_eq!(body.server.backend_mode, "durable");
        assert_eq!(body.server.core_runtime, "durable-cloud");
        assert_eq!(body.auth.modes, vec!["workspace".to_string()]);
        assert!(body.routes.filesystem.read.available);
        assert!(body.routes.filesystem.write.available);
        assert!(body.routes.filesystem.patch.available);
        assert!(body.routes.filesystem.delete.available);
        assert!(body.routes.filesystem.copy.available);
        assert!(body.routes.filesystem.move_.available);
        assert_eq!(body.routes.filesystem.write.reason, None);
        assert_eq!(
            body.routes.filesystem.write.requires,
            vec![
                "workspace-bearer".to_string(),
                "durable-session-ref".to_string(),
            ]
        );
        assert_eq!(
            body.idempotency.endpoints_supported,
            vec![
                "PUT /fs/{path}".to_string(),
                "PATCH /fs/{path}".to_string(),
                "DELETE /fs/{path}".to_string(),
                "POST /fs/{path}?op=copy|move".to_string(),
            ]
        );
        assert!(body.routes.vcs.log.available);
        assert!(body.routes.vcs.refs.list.available);
        assert!(!body.routes.vcs.refs.create.available);
        assert!(!body.routes.vcs.refs.update.available);
        assert_eq!(
            body.routes.vcs.refs.create.reason.as_deref(),
            Some("durable-cloud route is not supported yet")
        );
        assert!(!body.routes.vcs.commit.available);
        assert!(!body.routes.vcs.recovery.available);
        assert!(!body.routes.audit.available);
        assert!(!body.routes.workspaces.create.available);
        assert_eq!(
            body.routes.workspaces.issue_token.reason.as_deref(),
            Some("durable-cloud route is not supported yet")
        );
        assert_eq!(
            body.routes.workspaces.revoke_token.reason.as_deref(),
            Some("durable-cloud route is not supported yet")
        );
        assert_eq!(body.routes.workspaces.revoke_token.notes, None);
        assert!(!body.routes.review.change_requests.available);
        assert!(!body.routes.runs.available);
        assert!(!body.recovery.available);
        assert!(body.recovery.scheduler_present);
        server.abort();
    }

    #[tokio::test]
    async fn local_full_router_mounts_capabilities_without_auth() {
        let router = crate::server::build_router_with_stores(
            StratumDb::open_memory(),
            Arc::new(InMemoryWorkspaceMetadataStore::new()),
            Arc::new(InMemoryIdempotencyStore::new()),
            Arc::new(InMemoryAuditStore::new()),
            Arc::new(InMemoryReviewStore::new()),
        );
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/v1/capabilities"))
            .send()
            .await
            .expect("capabilities request should complete");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: CapabilityManifest = response.json().await.expect("manifest is json");
        assert_eq!(body.server.core_runtime, "local-state");
        assert!(body.routes.filesystem.write.available);
        let client = reqwest::Client::new();
        for (label, available, method, path) in [
            (
                "filesystem.read",
                body.routes.filesystem.read.available,
                reqwest::Method::GET,
                "/fs/probe",
            ),
            (
                "filesystem.list",
                body.routes.filesystem.list.available,
                reqwest::Method::GET,
                "/fs",
            ),
            (
                "filesystem.stat",
                body.routes.filesystem.stat.available,
                reqwest::Method::GET,
                "/fs/probe?stat=true",
            ),
            (
                "filesystem.write",
                body.routes.filesystem.write.available,
                reqwest::Method::PUT,
                "/fs/probe",
            ),
            (
                "filesystem.patch",
                body.routes.filesystem.patch.available,
                reqwest::Method::PATCH,
                "/fs/probe",
            ),
            (
                "filesystem.delete",
                body.routes.filesystem.delete.available,
                reqwest::Method::DELETE,
                "/fs/probe",
            ),
            (
                "filesystem.copy",
                body.routes.filesystem.copy.available,
                reqwest::Method::POST,
                "/fs/probe?op=copy&to=/copy",
            ),
            (
                "filesystem.move",
                body.routes.filesystem.move_.available,
                reqwest::Method::POST,
                "/fs/probe?op=move&to=/moved",
            ),
            (
                "search.grep",
                body.routes.search.grep.available,
                reqwest::Method::GET,
                "/search/grep",
            ),
            (
                "search.find",
                body.routes.search.find.available,
                reqwest::Method::GET,
                "/search/find",
            ),
            (
                "search.tree",
                body.routes.search.tree.available,
                reqwest::Method::GET,
                "/tree",
            ),
            (
                "vcs.status",
                body.routes.vcs.status.available,
                reqwest::Method::GET,
                "/vcs/status",
            ),
            (
                "vcs.diff",
                body.routes.vcs.diff.available,
                reqwest::Method::GET,
                "/vcs/diff",
            ),
            (
                "vcs.log",
                body.routes.vcs.log.available,
                reqwest::Method::GET,
                "/vcs/log",
            ),
            (
                "vcs.refs.list",
                body.routes.vcs.refs.list.available,
                reqwest::Method::GET,
                "/vcs/refs",
            ),
            (
                "vcs.refs.create",
                body.routes.vcs.refs.create.available,
                reqwest::Method::POST,
                "/vcs/refs",
            ),
            (
                "vcs.refs.update",
                body.routes.vcs.refs.update.available,
                reqwest::Method::PATCH,
                "/vcs/refs/main",
            ),
            (
                "vcs.commit",
                body.routes.vcs.commit.available,
                reqwest::Method::POST,
                "/vcs/commit",
            ),
            (
                "vcs.revert",
                body.routes.vcs.revert.available,
                reqwest::Method::POST,
                "/vcs/revert",
            ),
            (
                "review.change_requests",
                body.routes.review.change_requests.available,
                reqwest::Method::POST,
                "/change-requests",
            ),
            (
                "review.approvals",
                body.routes.review.approvals.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/approvals",
            ),
            (
                "review.reviewers",
                body.routes.review.reviewers.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/reviewers",
            ),
            (
                "review.comments",
                body.routes.review.comments.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/comments",
            ),
            (
                "review.merge",
                body.routes.review.merge.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/merge",
            ),
            (
                "review.reject",
                body.routes.review.reject.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/reject",
            ),
            (
                "review.dismiss",
                body.routes.review.dismiss.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/approvals/00000000-0000-0000-0000-000000000002/dismiss",
            ),
            (
                "workspaces.list",
                body.routes.workspaces.list.available,
                reqwest::Method::GET,
                "/workspaces",
            ),
            (
                "workspaces.create",
                body.routes.workspaces.create.available,
                reqwest::Method::POST,
                "/workspaces",
            ),
            (
                "workspaces.issue_token",
                body.routes.workspaces.issue_token.available,
                reqwest::Method::POST,
                "/workspaces/00000000-0000-0000-0000-000000000001/tokens",
            ),
            (
                "workspaces.revoke_token",
                body.routes.workspaces.revoke_token.available,
                reqwest::Method::POST,
                "/workspaces/00000000-0000-0000-0000-000000000001/tokens/00000000-0000-0000-0000-000000000002/revoke",
            ),
            (
                "protection.ref_rules",
                body.protection.ref_rules.available,
                reqwest::Method::GET,
                "/protected/refs",
            ),
            (
                "protection.path_rules",
                body.protection.path_rules.available,
                reqwest::Method::GET,
                "/protected/paths",
            ),
            (
                "audit",
                body.routes.audit.available,
                reqwest::Method::GET,
                "/audit",
            ),
            (
                "runs",
                body.routes.runs.available,
                reqwest::Method::POST,
                "/runs",
            ),
        ] {
            assert!(available, "{label} should be advertised available locally");
            assert_route_is_mounted(&client, &base_url, method, path, label).await;
        }
        assert!(!body.routes.search.semantic.available);
        assert_route_is_not_mounted(
            &client,
            &base_url,
            reqwest::Method::GET,
            "/search/semantic",
            "search.semantic",
        )
        .await;
        assert!(!body.routes.vcs.recovery.available);
        assert_route_is_mounted(
            &client,
            &base_url,
            reqwest::Method::GET,
            "/vcs/recovery",
            "vcs.recovery",
        )
        .await;
        server.abort();
    }

    #[tokio::test]
    async fn durable_cloud_full_router_mounts_capabilities_without_auth() {
        let stores = StratumStores::local_memory();
        let router = crate::server::build_durable_core_router(
            ServerStores {
                backend_mode: crate::backend::runtime::BackendRuntimeMode::Durable,
                workspaces: stores.workspace_metadata.clone(),
                idempotency: stores.idempotency.clone(),
                audit: stores.audit.clone(),
                review: stores.review.clone(),
                guarded_durable_commit_stores: None,
                durable_core_stores: Some(stores),
            },
            RepoId::new("repo_capabilities_full_router").expect("valid repo id"),
        );
        let (base_url, server) = spawn_test_router(router).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/v1/capabilities"))
            .send()
            .await
            .expect("capabilities request should complete");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: CapabilityManifest = response.json().await.expect("manifest is json");
        assert_eq!(body.server.core_runtime, "durable-cloud");
        assert!(body.routes.filesystem.write.available);
        assert!(body.recovery.scheduler_present);
        let client = reqwest::Client::new();
        for (label, available, method, path) in [
            (
                "filesystem.read",
                body.routes.filesystem.read.available,
                reqwest::Method::GET,
                "/fs/probe",
            ),
            (
                "filesystem.list",
                body.routes.filesystem.list.available,
                reqwest::Method::GET,
                "/fs",
            ),
            (
                "filesystem.stat",
                body.routes.filesystem.stat.available,
                reqwest::Method::GET,
                "/fs/probe?stat=true",
            ),
            (
                "filesystem.write",
                body.routes.filesystem.write.available,
                reqwest::Method::PUT,
                "/fs/probe",
            ),
            (
                "filesystem.patch",
                body.routes.filesystem.patch.available,
                reqwest::Method::PATCH,
                "/fs/probe",
            ),
            (
                "filesystem.delete",
                body.routes.filesystem.delete.available,
                reqwest::Method::DELETE,
                "/fs/probe",
            ),
            (
                "filesystem.copy",
                body.routes.filesystem.copy.available,
                reqwest::Method::POST,
                "/fs/probe?op=copy&to=/copy",
            ),
            (
                "filesystem.move",
                body.routes.filesystem.move_.available,
                reqwest::Method::POST,
                "/fs/probe?op=move&to=/moved",
            ),
            (
                "search.grep",
                body.routes.search.grep.available,
                reqwest::Method::GET,
                "/search/grep",
            ),
            (
                "search.find",
                body.routes.search.find.available,
                reqwest::Method::GET,
                "/search/find",
            ),
            (
                "search.tree",
                body.routes.search.tree.available,
                reqwest::Method::GET,
                "/tree",
            ),
            (
                "vcs.status",
                body.routes.vcs.status.available,
                reqwest::Method::GET,
                "/vcs/status",
            ),
            (
                "vcs.diff",
                body.routes.vcs.diff.available,
                reqwest::Method::GET,
                "/vcs/diff",
            ),
            (
                "vcs.log",
                body.routes.vcs.log.available,
                reqwest::Method::GET,
                "/vcs/log",
            ),
            (
                "vcs.refs.list",
                body.routes.vcs.refs.list.available,
                reqwest::Method::GET,
                "/vcs/refs",
            ),
        ] {
            assert!(
                available,
                "{label} should be advertised available in durable-cloud"
            );
            assert_route_is_mounted(&client, &base_url, method, path, label).await;
        }
        for (label, available, method, path) in [
            (
                "vcs.refs.create",
                body.routes.vcs.refs.create.available,
                reqwest::Method::POST,
                "/vcs/refs",
            ),
            (
                "vcs.refs.update",
                body.routes.vcs.refs.update.available,
                reqwest::Method::PATCH,
                "/vcs/refs/main",
            ),
            (
                "vcs.commit",
                body.routes.vcs.commit.available,
                reqwest::Method::POST,
                "/vcs/commit",
            ),
            (
                "vcs.revert",
                body.routes.vcs.revert.available,
                reqwest::Method::POST,
                "/vcs/revert",
            ),
            (
                "vcs.recovery",
                body.routes.vcs.recovery.available,
                reqwest::Method::GET,
                "/vcs/recovery",
            ),
            (
                "review.approvals",
                body.routes.review.approvals.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/approvals",
            ),
            (
                "review.reviewers",
                body.routes.review.reviewers.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/reviewers",
            ),
            (
                "review.comments",
                body.routes.review.comments.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/comments",
            ),
            (
                "review.merge",
                body.routes.review.merge.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/merge",
            ),
            (
                "review.reject",
                body.routes.review.reject.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/reject",
            ),
            (
                "review.dismiss",
                body.routes.review.dismiss.available,
                reqwest::Method::POST,
                "/change-requests/00000000-0000-0000-0000-000000000001/approvals/00000000-0000-0000-0000-000000000002/dismiss",
            ),
            (
                "workspaces.list",
                body.routes.workspaces.list.available,
                reqwest::Method::GET,
                "/workspaces",
            ),
            (
                "workspaces.create",
                body.routes.workspaces.create.available,
                reqwest::Method::POST,
                "/workspaces",
            ),
            (
                "workspaces.issue_token",
                body.routes.workspaces.issue_token.available,
                reqwest::Method::POST,
                "/workspaces/00000000-0000-0000-0000-000000000001/tokens",
            ),
            (
                "workspaces.revoke_token",
                body.routes.workspaces.revoke_token.available,
                reqwest::Method::POST,
                "/workspaces/00000000-0000-0000-0000-000000000001/tokens/00000000-0000-0000-0000-000000000002/revoke",
            ),
            (
                "review.change_requests",
                body.routes.review.change_requests.available,
                reqwest::Method::POST,
                "/change-requests",
            ),
            (
                "protection.ref_rules",
                body.protection.ref_rules.available,
                reqwest::Method::GET,
                "/protected/refs",
            ),
            (
                "protection.path_rules",
                body.protection.path_rules.available,
                reqwest::Method::GET,
                "/protected/paths",
            ),
            (
                "audit",
                body.routes.audit.available,
                reqwest::Method::GET,
                "/audit",
            ),
            (
                "runs",
                body.routes.runs.available,
                reqwest::Method::POST,
                "/runs",
            ),
        ] {
            assert!(
                !available,
                "{label} should be advertised unavailable in durable-cloud"
            );
            assert_route_returns_not_supported(&client, &base_url, method, path, label).await;
        }
        assert!(!body.routes.search.semantic.available);
        assert_route_is_not_mounted(
            &client,
            &base_url,
            reqwest::Method::GET,
            "/search/semantic",
            "search.semantic",
        )
        .await;
        server.abort();
    }

    #[test]
    fn manifest_schema_round_trips_through_serde() {
        let manifest = manifest_for_state(&local_state());
        let encoded = serde_json::to_string_pretty(&manifest).expect("manifest encodes");
        let decoded: CapabilityManifest = serde_json::from_str(&encoded).expect("manifest decodes");

        assert_eq!(decoded, manifest);
        assert!(!encoded.contains("repo_"));
        assert!(!encoded.contains(".vfs"));
        assert!(!encoded.contains("STRATUM_POSTGRES_URL"));
        assert!(!encoded.contains("workspace_token"));
    }

    #[test]
    fn guarded_durable_local_state_advertises_recovery() {
        let manifest = manifest_for_state(&guarded_durable_state());

        assert_eq!(manifest.server.backend_mode, "durable");
        assert_eq!(manifest.server.core_runtime, "local-state");
        assert!(manifest.routes.vcs.recovery.available);
        assert!(manifest.recovery.available);
        assert!(manifest.recovery.scheduler_present);
    }

    #[test]
    fn durable_control_plane_local_state_advertises_durable_backend_without_recovery() {
        let db = Arc::new(StratumDb::open_memory());
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared_from_arc(db.clone()),
            db: ServerLocalDb::available_with_backend(
                db,
                crate::backend::runtime::BackendRuntimeMode::Durable,
            ),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        });

        let manifest = manifest_for_state(&state);

        assert_eq!(manifest.server.backend_mode, "durable");
        assert_eq!(manifest.server.core_runtime, "local-state");
        assert!(!manifest.routes.vcs.recovery.available);
        assert!(!manifest.recovery.scheduler_present);
    }

    #[test]
    fn update_checked_in_sdk_contract_fixture_when_requested() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let fixture_dir = repo_root.join("sdk/contracts");
        let update_fixtures =
            std::env::var("STRATUM_UPDATE_CAPABILITY_FIXTURES").as_deref() == Ok("1");
        if update_fixtures {
            std::fs::create_dir_all(&fixture_dir).expect("create contract fixture dir");
        }
        for (file_name, state) in [
            ("capabilities.v1.json", local_state()),
            ("capabilities.v1.durable-cloud.json", durable_cloud_state()),
        ] {
            let manifest = manifest_for_state(&state);
            let json = serde_json::to_string_pretty(&manifest).expect("serialize fixture manifest");
            let expected = format!("{json}\n");
            let fixture_path = fixture_dir.join(file_name);
            if update_fixtures {
                std::fs::write(&fixture_path, expected).expect("write contract fixture");
            } else {
                let actual = std::fs::read_to_string(&fixture_path).expect("read contract fixture");
                assert_eq!(
                    actual, expected,
                    "{file_name} is stale; run with STRATUM_UPDATE_CAPABILITY_FIXTURES=1"
                );
            }
        }
    }

    fn local_state() -> Arc<ServerState> {
        let db = Arc::new(StratumDb::open_memory());
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared_from_arc(db.clone()),
            db: ServerLocalDb::available(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
        })
    }

    fn durable_cloud_state() -> Arc<ServerState> {
        let stores = StratumStores::local_memory();
        Arc::new(ServerState {
            core: Arc::new(DurableCoreRuntime::new(
                RepoId::new("repo_capabilities").expect("valid repo id"),
                stores.clone(),
            )),
            db: ServerLocalDb::unavailable(),
            workspaces: stores.workspace_metadata,
            idempotency: stores.idempotency,
            audit: stores.audit,
            review: stores.review,
        })
    }

    fn guarded_durable_state() -> Arc<ServerState> {
        let local_db = StratumDb::open_memory();
        let core_db = Arc::new(local_db.clone());
        let stores = StratumStores::local_memory();
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared_with_guarded_durable_commit_route(
                local_db,
                RepoId::local(),
                stores.clone(),
            ),
            db: ServerLocalDb::available_with_backend(
                core_db,
                crate::backend::runtime::BackendRuntimeMode::Durable,
            ),
            workspaces: stores.workspace_metadata,
            idempotency: stores.idempotency,
            audit: stores.audit,
            review: stores.review,
        })
    }

    async fn spawn_test_router(router: Router) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr: SocketAddr = listener.local_addr().expect("test listener addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test router serves");
        });
        (format!("http://{addr}"), server)
    }

    async fn assert_route_is_mounted(
        client: &reqwest::Client,
        base_url: &str,
        method: reqwest::Method,
        path: &str,
        label: &str,
    ) {
        let status = client
            .request(method, format!("{base_url}{path}"))
            .send()
            .await
            .unwrap_or_else(|err| panic!("{label} probe should complete: {err}"))
            .status();
        assert_ne!(
            status,
            reqwest::StatusCode::NOT_FOUND,
            "{label} should be mounted"
        );
        assert_ne!(
            status,
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
            "{label} should accept the advertised method"
        );
    }

    async fn assert_route_returns_not_supported(
        client: &reqwest::Client,
        base_url: &str,
        method: reqwest::Method,
        path: &str,
        label: &str,
    ) {
        let status = client
            .request(method, format!("{base_url}{path}"))
            .send()
            .await
            .unwrap_or_else(|err| panic!("{label} probe should complete: {err}"))
            .status();
        assert_eq!(
            status,
            reqwest::StatusCode::NOT_IMPLEMENTED,
            "{label} should fail closed as unsupported"
        );
    }

    async fn assert_route_is_not_mounted(
        client: &reqwest::Client,
        base_url: &str,
        method: reqwest::Method,
        path: &str,
        label: &str,
    ) {
        let status = client
            .request(method, format!("{base_url}{path}"))
            .send()
            .await
            .unwrap_or_else(|err| panic!("{label} probe should complete: {err}"))
            .status();
        assert_eq!(
            status,
            reqwest::StatusCode::NOT_FOUND,
            "{label} should not be mounted"
        );
    }
}
