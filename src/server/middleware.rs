use axum::http::HeaderMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::auth::session::{Session, SessionMount, SessionMountIdentity, SessionScope};
use crate::backend::RepoId;
use crate::error::VfsError;
use crate::server::AppState;

const INVALID_WORKSPACE_BEARER_TOKEN: &str = "invalid workspace bearer token";

pub async fn session_from_headers(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Session, VfsError> {
    if let Some(auth_header) = headers.get("authorization") {
        let header_str = auth_header.to_str().map_err(|_| VfsError::AuthError {
            message: "invalid authorization header".to_string(),
        })?;

        if let Some(token) = header_str.strip_prefix("Bearer ") {
            if let Some(workspace_header) = headers.get("x-stratum-workspace") {
                let workspace_value =
                    workspace_header.to_str().map_err(|_| VfsError::AuthError {
                        message: "invalid x-stratum-workspace header".to_string(),
                    })?;
                let workspace_id =
                    Uuid::parse_str(workspace_value).map_err(|_| VfsError::AuthError {
                        message: "invalid x-stratum-workspace header".to_string(),
                    })?;
                let now_unix = current_unix_time();
                let Some(valid) = state
                    .workspaces
                    .validate_workspace_token_at(workspace_id, token, now_unix)
                    .await?
                else {
                    return Err(VfsError::AuthError {
                        message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                    });
                };
                if valid.workspace.repo_id != valid.repo_id {
                    return Err(VfsError::AuthError {
                        message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                    });
                }
                if state.requires_explicit_workspace_repo() && valid.repo_id.is_none() {
                    return Err(VfsError::AuthError {
                        message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                    });
                }
                if let Some(repo_id) = valid.repo_id.as_deref() {
                    RepoId::new(repo_id).map_err(|_| VfsError::AuthError {
                        message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                    })?;
                }

                let principal_uid = valid
                    .principal
                    .as_ref()
                    .map(|principal| principal.uid)
                    .or(valid.token.principal_uid)
                    .unwrap_or(valid.token.agent_uid);
                let identity =
                    SessionMountIdentity::new(valid.workspace.id, valid.workspace.root_path)
                        .with_refs(valid.workspace.base_ref, valid.workspace.session_ref)
                        .with_repo_id(valid.repo_id)
                        .with_principal_uid(principal_uid)
                        .with_token(valid.token.id, valid.token.token_version)
                        .with_prefixes(
                            valid.token.read_prefixes.clone(),
                            valid.token.write_prefixes.clone(),
                        );
                SessionMount::with_identity(identity.clone()).map_err(|_| VfsError::AuthError {
                    message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                })?;
                let scope = SessionScope::new(
                    valid.token.read_prefixes.iter().map(String::as_str),
                    valid.token.write_prefixes.iter().map(String::as_str),
                )
                .map_err(|_| VfsError::AuthError {
                    message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                })?;
                let session = match valid.principal {
                    Some(principal) => {
                        Session::from_workspace_principal(principal).map_err(|_| {
                            VfsError::AuthError {
                                message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                            }
                        })?
                    }
                    None if valid
                        .workspace
                        .repo_id
                        .as_deref()
                        .is_some_and(|repo_id| repo_id != RepoId::local().as_str()) =>
                    {
                        return Err(VfsError::AuthError {
                            message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                        });
                    }
                    None => state.core.session_for_uid(valid.token.agent_uid).await?,
                };

                return session
                    .with_scope(scope)
                    .with_workspace_mount_identity(identity)
                    .map_err(|_| VfsError::AuthError {
                        message: INVALID_WORKSPACE_BEARER_TOKEN.to_string(),
                    });
            }

            return state.core.authenticate_token(token).await;
        }

        if let Some(username) = header_str.strip_prefix("User ") {
            return state.core.login(username).await;
        }

        return Err(VfsError::AuthError {
            message: "unsupported authorization scheme".to_string(),
        });
    }

    Err(VfsError::AuthError {
        message: "missing authorization header".to_string(),
    })
}

fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(1)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::perms::Access;
    use crate::backend::{RepoId, StratumStores};
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, LocalWorkspaceMetadataStore,
        ValidWorkspaceToken, WorkspaceMetadataStore, WorkspacePrincipalKind,
        WorkspacePrincipalRecord, WorkspaceRecord, WorkspaceTokenRecord, token_is_valid_at,
    };
    use async_trait::async_trait;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state() -> AppState {
        let db = StratumDb::open_memory();
        Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        })
    }

    #[tokio::test]
    async fn missing_auth_is_rejected_instead_of_root() {
        let state = test_state();
        let headers = HeaderMap::new();

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("missing auth must not fall back to root");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn unsupported_auth_scheme_is_rejected_instead_of_root() {
        let state = test_state();
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic abc123".parse().unwrap());

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("unsupported auth must not fall back to root");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    fn temp_metadata_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir()
            .join("stratum-middleware-tests")
            .join(format!("{name}-{}.bin", Uuid::new_v4()))
    }

    fn extract_agent_token(output: &str) -> String {
        output
            .lines()
            .last()
            .expect("agent token line")
            .trim()
            .to_string()
    }

    fn workspace_bearer_headers(raw_secret: &str, workspace_id: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {raw_secret}").parse().unwrap(),
        );
        headers.insert("x-stratum-workspace", workspace_id.parse().unwrap());
        headers
    }

    #[derive(Clone)]
    struct DurableLikeWorkspaceStore {
        workspace: WorkspaceRecord,
        token: WorkspaceTokenRecord,
        principal: Option<WorkspacePrincipalRecord>,
        raw_secret: String,
        repo_id_override: Option<String>,
    }

    #[async_trait]
    impl WorkspaceMetadataStore for DurableLikeWorkspaceStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(vec![self.workspace.clone()])
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            Ok((id == self.workspace.id).then(|| self.workspace.clone()))
        }

        async fn update_head_commit(
            &self,
            _id: Uuid,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            unreachable!("not used")
        }

        async fn update_head_commit_if_current(
            &self,
            _id: Uuid,
            _expected_head_commit: Option<&str>,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            unreachable!("not used")
        }

        async fn issue_scoped_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: crate::auth::Uid,
            _read_prefixes: Vec<String>,
            _write_prefixes: Vec<String>,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token_at(
            &self,
            workspace_id: Uuid,
            raw_secret: &str,
            now_unix: u64,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            if workspace_id != self.workspace.id
                || raw_secret != self.raw_secret
                || !token_is_valid_at(&self.token, now_unix)
            {
                return Ok(None);
            }
            Ok(Some(ValidWorkspaceToken {
                repo_id: self
                    .repo_id_override
                    .clone()
                    .or_else(|| self.workspace.repo_id.clone()),
                workspace: self.workspace.clone(),
                token: self.token.clone(),
                principal: self.principal.clone(),
            }))
        }
    }

    fn durable_like_workspace_store(
        raw_secret: String,
        token: WorkspaceTokenRecord,
        principal: WorkspacePrincipalRecord,
    ) -> DurableLikeWorkspaceStore {
        DurableLikeWorkspaceStore {
            workspace: WorkspaceRecord {
                id: token.workspace_id,
                name: "durable".to_string(),
                root_path: "/durable".to_string(),
                head_commit: None,
                version: 0,
                base_ref: "main".to_string(),
                session_ref: Some("agent/durable/session".to_string()),
                repo_id: Some("repo_durable".to_string()),
            },
            token,
            principal: Some(principal),
            raw_secret,
            repo_id_override: None,
        }
    }

    fn guarded_durable_state_for_repo(
        repo_id: RepoId,
        workspaces: Arc<dyn WorkspaceMetadataStore>,
    ) -> AppState {
        let db = StratumDb::open_memory();
        Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared_with_guarded_durable_commit_route(
                db.clone(),
                repo_id,
                StratumStores::local_memory(),
            ),
            db: Arc::new(db),
            workspaces,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        })
    }

    fn guarded_durable_state(workspaces: Arc<dyn WorkspaceMetadataStore>) -> AppState {
        guarded_durable_state_for_repo(RepoId::local(), workspaces)
    }

    fn durable_workspace_token(workspace_id: Uuid) -> WorkspaceTokenRecord {
        WorkspaceTokenRecord {
            id: Uuid::new_v4(),
            workspace_id,
            name: "durable-token".to_string(),
            agent_uid: 501,
            secret_hash: "hash-only".to_string(),
            read_prefixes: vec!["/durable/read".to_string()],
            write_prefixes: vec!["/durable/write".to_string()],
            principal_uid: Some(501),
            token_version: 7,
            issued_at_unix: 1,
            updated_at_unix: 1,
            expires_at_unix: None,
            revoked_at_unix: None,
        }
    }

    fn durable_workspace_principal() -> WorkspacePrincipalRecord {
        WorkspacePrincipalRecord {
            uid: 501,
            username: "durable-agent".to_string(),
            gid: 601,
            groups: vec![601, 602],
            kind: WorkspacePrincipalKind::Agent,
            active: true,
        }
    }

    #[tokio::test]
    async fn workspace_bearer_authenticates_after_file_store_rebuild() {
        let path = temp_metadata_path("workspace-bearer");
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();

        let store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "ci-token",
                agent.uid,
                vec!["/demo/read".to_string()],
                vec!["/demo/write".to_string()],
            )
            .await
            .unwrap();
        drop(store);

        let rebuilt_store = LocalWorkspaceMetadataStore::open(&path).unwrap();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(rebuilt_store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", issued.raw_secret).parse().unwrap(),
        );
        headers.insert(
            "x-stratum-workspace",
            workspace.id.to_string().parse().unwrap(),
        );

        let session = session_from_headers(&state, &headers).await.unwrap();
        assert_eq!(session.uid, agent.uid);
        assert_eq!(session.username, "ci-agent");
        assert!(session.scope.is_some());
        let mount = session.mount().expect("workspace bearer session mount");
        assert_eq!(mount.workspace_id(), workspace.id);
        assert_eq!(mount.root_path(), "/demo");
        assert_eq!(mount.base_ref(), "main");
        assert_eq!(mount.session_ref(), None);
        assert_eq!(
            session.resolve_mounted_path("/read/file.txt").unwrap(),
            "/demo/read/file.txt"
        );
        assert!(session.is_path_allowed("/demo/read/file.txt", Access::Read));
        assert!(!session.is_path_allowed("/demo/outside/file.txt", Access::Read));
        assert!(session.is_path_allowed("/demo/write/file.txt", Access::Write));
        assert!(!session.is_path_allowed("/demo/read/file.txt", Access::Write));
    }

    #[tokio::test]
    async fn workspace_bearer_uses_durable_principal_without_local_user() {
        let workspace_id = Uuid::new_v4();
        let raw_secret = "durable-secret".to_string();
        let token = durable_workspace_token(workspace_id);
        let token_id = token.id;
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(StratumDb::open_memory()),
            db: Arc::new(StratumDb::open_memory()),
            workspaces: Arc::new(durable_like_workspace_store(
                raw_secret.clone(),
                token,
                durable_workspace_principal(),
            )),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let session = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_secret, &workspace_id.to_string()),
        )
        .await
        .unwrap();

        assert_eq!(session.uid, 501);
        assert_eq!(session.gid, 601);
        assert_eq!(session.groups, vec![601, 602]);
        assert_eq!(session.username, "durable-agent");
        let mount = session.mount().expect("durable workspace mount");
        assert_eq!(mount.workspace_id(), workspace_id);
        assert_eq!(mount.repo_id(), Some("repo_durable"));
        assert_eq!(mount.session_ref(), Some("agent/durable/session"));
        assert_eq!(mount.principal_uid(), Some(501));
        assert_eq!(mount.token_id(), Some(token_id));
        assert_eq!(mount.token_version(), Some(7));
        assert_eq!(mount.read_prefixes(), &["/durable/read".to_string()]);
        assert_eq!(mount.write_prefixes(), &["/durable/write".to_string()]);
        assert!(session.is_path_allowed("/durable/read/file.txt", Access::Read));
        assert!(!session.is_path_allowed("/durable/write/file.txt", Access::Read));
        assert!(session.is_path_allowed("/durable/write/file.txt", Access::Write));
    }

    #[tokio::test]
    async fn repo_scoped_workspace_bearer_without_principal_rejects_without_global_fallback() {
        let workspace_id = Uuid::new_v4();
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let token = durable_workspace_token(workspace_id);
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(DurableLikeWorkspaceStore {
                workspace: WorkspaceRecord {
                    id: workspace_id,
                    name: "durable".to_string(),
                    root_path: "/durable".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: "main".to_string(),
                    session_ref: Some("agent/durable/session".to_string()),
                    repo_id: Some("repo_durable".to_string()),
                },
                token,
                principal: None,
                raw_secret: raw_agent_token.clone(),
                repo_id_override: None,
            }),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let err = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_agent_token, &workspace_id.to_string()),
        )
        .await
        .expect_err("repo-scoped workspace bearer without principal must not use global auth");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn guarded_durable_workspace_bearer_rejects_missing_repo() {
        let workspace_id = Uuid::new_v4();
        let raw_secret = "durable-secret".to_string();
        let token = durable_workspace_token(workspace_id);
        let state = guarded_durable_state(Arc::new(DurableLikeWorkspaceStore {
            workspace: WorkspaceRecord {
                id: workspace_id,
                name: "durable".to_string(),
                root_path: "/durable".to_string(),
                head_commit: None,
                version: 0,
                base_ref: "main".to_string(),
                session_ref: Some("agent/durable/session".to_string()),
                repo_id: None,
            },
            token,
            principal: Some(durable_workspace_principal()),
            raw_secret: raw_secret.clone(),
            repo_id_override: None,
        }));

        let err = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_secret, &workspace_id.to_string()),
        )
        .await
        .expect_err("guarded durable workspace bearer must carry explicit repo metadata");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn nonlocal_guarded_durable_workspace_bearer_rejects_missing_repo() {
        let workspace_id = Uuid::new_v4();
        let raw_secret = "durable-secret".to_string();
        let token = durable_workspace_token(workspace_id);
        let state = guarded_durable_state_for_repo(
            RepoId::new("repo_durable").unwrap(),
            Arc::new(DurableLikeWorkspaceStore {
                workspace: WorkspaceRecord {
                    id: workspace_id,
                    name: "durable".to_string(),
                    root_path: "/durable".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: "main".to_string(),
                    session_ref: Some("agent/durable/session".to_string()),
                    repo_id: None,
                },
                token,
                principal: Some(durable_workspace_principal()),
                raw_secret: raw_secret.clone(),
                repo_id_override: None,
            }),
        );

        let err = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_secret, &workspace_id.to_string()),
        )
        .await
        .expect_err("hosted durable workspace bearer must carry repo identity");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn workspace_bearer_rejects_workspace_token_repo_mismatch() {
        let workspace_id = Uuid::new_v4();
        let raw_secret = "durable-secret".to_string();
        let token = durable_workspace_token(workspace_id);
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(StratumDb::open_memory()),
            db: Arc::new(StratumDb::open_memory()),
            workspaces: Arc::new(DurableLikeWorkspaceStore {
                workspace: WorkspaceRecord {
                    id: workspace_id,
                    name: "durable".to_string(),
                    root_path: "/durable".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: "main".to_string(),
                    session_ref: Some("agent/durable/session".to_string()),
                    repo_id: Some("repo_workspace".to_string()),
                },
                token,
                principal: Some(durable_workspace_principal()),
                raw_secret: raw_secret.clone(),
                repo_id_override: Some("repo_token".to_string()),
            }),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let err = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_secret, &workspace_id.to_string()),
        )
        .await
        .expect_err("workspace/token repo mismatch must fail closed");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn expired_workspace_bearer_rejects_without_global_fallback() {
        let workspace_id = Uuid::new_v4();
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let mut token = durable_workspace_token(workspace_id);
        token.expires_at_unix = Some(1);
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(durable_like_workspace_store(
                raw_agent_token.clone(),
                token,
                durable_workspace_principal(),
            )),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let err = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_agent_token, &workspace_id.to_string()),
        )
        .await
        .expect_err("expired workspace token must not fall back to global bearer");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn revoked_workspace_bearer_rejects_without_global_fallback() {
        let workspace_id = Uuid::new_v4();
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let mut token = durable_workspace_token(workspace_id);
        token.revoked_at_unix = Some(2);
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(durable_like_workspace_store(
                raw_agent_token.clone(),
                token,
                durable_workspace_principal(),
            )),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });

        let err = session_from_headers(
            &state,
            &workspace_bearer_headers(&raw_agent_token, &workspace_id.to_string()),
        )
        .await
        .expect_err("revoked workspace token must not fall back to global bearer");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn workspace_bearer_rejects_malformed_workspace_header_without_global_fallback() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let headers = workspace_bearer_headers(&raw_agent_token, "not-a-uuid");

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("malformed workspace bearer header must not fall back to global auth");

        assert!(matches!(err, VfsError::AuthError { .. }));
        let message = err.to_string();
        assert!(message.contains("invalid x-stratum-workspace header"));
        assert!(!message.contains("not-a-uuid"));
    }

    #[tokio::test]
    async fn workspace_bearer_rejects_unknown_workspace_without_global_fallback() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let headers = workspace_bearer_headers(&raw_agent_token, &Uuid::new_v4().to_string());

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("unknown workspace bearer must not fall back to global auth");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }

    #[tokio::test]
    async fn workspace_bearer_rejects_wrong_token_without_global_fallback() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store.create_workspace("demo", "/demo").await.unwrap();
        let state = Arc::new(ServerState {
            core: crate::server::core::LocalCoreRuntime::shared(db.clone()),
            db: Arc::new(db),
            workspaces: Arc::new(store),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(crate::audit::InMemoryAuditStore::new()),
            review: Arc::new(crate::review::InMemoryReviewStore::new()),
        });
        let headers = workspace_bearer_headers(&raw_agent_token, &workspace.id.to_string());

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("wrong workspace bearer token must not fall back to global auth");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }
}
