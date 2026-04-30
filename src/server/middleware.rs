use axum::http::HeaderMap;
use uuid::Uuid;

use crate::auth::session::{Session, SessionScope};
use crate::error::VfsError;
use crate::server::AppState;

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
                    workspace_header
                        .to_str()
                        .map_err(|_| VfsError::AuthError {
                            message: "invalid x-stratum-workspace header".to_string(),
                        })?;
                let workspace_id =
                    Uuid::parse_str(workspace_value).map_err(|_| VfsError::AuthError {
                        message: format!("invalid workspace id: {workspace_value}"),
                    })?;
                let Some(valid) = state
                    .workspaces
                    .validate_workspace_token(workspace_id, token)
                    .await?
                else {
                    return Err(VfsError::AuthError {
                        message: "invalid workspace bearer token".to_string(),
                    });
                };

                let scope = SessionScope::new(
                    valid.token.read_prefixes.iter().map(String::as_str),
                    valid.token.write_prefixes.iter().map(String::as_str),
                )?;
                return Ok(state
                    .db
                    .session_for_uid(valid.token.agent_uid)
                    .await?
                    .with_scope(scope));
            }

            return state.db.authenticate_token(token).await;
        }

        if let Some(username) = header_str.strip_prefix("User ") {
            return state.db.login(username).await;
        }

        return Err(VfsError::AuthError {
            message: "unsupported authorization scheme".to_string(),
        });
    }

    Err(VfsError::AuthError {
        message: "missing authorization header".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::perms::Access;
    use crate::db::StratumDb;
    use crate::server::ServerState;
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, LocalWorkspaceMetadataStore, WorkspaceMetadataStore,
    };
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state() -> AppState {
        Arc::new(ServerState {
            db: Arc::new(StratumDb::open_memory()),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
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
            db: Arc::new(db),
            workspaces: Arc::new(rebuilt_store),
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
        assert!(session.is_path_allowed("/demo/read/file.txt", Access::Read));
        assert!(!session.is_path_allowed("/demo/outside/file.txt", Access::Read));
        assert!(session.is_path_allowed("/demo/write/file.txt", Access::Write));
        assert!(!session.is_path_allowed("/demo/read/file.txt", Access::Write));
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
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
        });
        let headers = workspace_bearer_headers(&raw_agent_token, "not-a-uuid");

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("malformed workspace bearer header must not fall back to global auth");

        assert!(matches!(err, VfsError::AuthError { .. }));
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
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
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
            db: Arc::new(db),
            workspaces: Arc::new(store),
        });
        let headers = workspace_bearer_headers(&raw_agent_token, &workspace.id.to_string());

        let err = session_from_headers(&state, &headers)
            .await
            .expect_err("wrong workspace bearer token must not fall back to global auth");

        assert!(matches!(err, VfsError::AuthError { .. }));
    }
}
