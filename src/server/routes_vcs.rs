use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::Deserialize;

use uuid::Uuid;

use super::AppState;
use super::middleware::session_from_headers;
use crate::auth::session::Session;
use crate::auth::{ROOT_UID, WHEEL_GID};
use crate::error::VfsError;

#[derive(Deserialize)]
pub struct CommitRequest {
    pub message: String,
}

#[derive(Deserialize)]
pub struct RevertRequest {
    pub hash: String,
}

#[derive(Deserialize)]
pub struct DiffQuery {
    pub path: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateRefRequest {
    pub name: String,
    pub target: String,
}

#[derive(Deserialize)]
pub struct UpdateRefRequest {
    pub target: String,
    pub expected_target: String,
    pub expected_version: u64,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/vcs/commit", post(vcs_commit))
        .route("/vcs/log", get(vcs_log))
        .route("/vcs/revert", post(vcs_revert))
        .route("/vcs/status", get(vcs_status))
        .route("/vcs/diff", get(vcs_diff))
        .route("/vcs/refs", get(vcs_list_refs).post(vcs_create_ref))
        .route("/vcs/refs/{*name}", patch(vcs_update_ref))
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
        VfsError::AlreadyExists { .. } => StatusCode::CONFLICT,
        VfsError::InvalidArgs { message }
            if message.starts_with("ref compare-and-swap mismatch") =>
        {
            StatusCode::CONFLICT
        }
        VfsError::InvalidArgs { .. } => StatusCode::BAD_REQUEST,
        VfsError::IoError(_) | VfsError::CorruptStore { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => fallback,
    }
}

fn require_admin_session(session: &Session) -> Result<(), VfsError> {
    if session.scope.is_some() {
        return Err(VfsError::PermissionDenied {
            path: "vcs refs".to_string(),
        });
    }

    let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
    if !principal_admin {
        return Err(VfsError::PermissionDenied {
            path: "vcs refs".to_string(),
        });
    }

    if let Some(delegate) = &session.delegate {
        let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
        if !delegate_admin {
            return Err(VfsError::PermissionDenied {
                path: "vcs refs".to_string(),
            });
        }
    }

    Ok(())
}

async fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<Session, VfsError> {
    let session = session_from_headers(state, headers).await?;
    require_admin_session(&session)?;
    Ok(session)
}

fn ref_json(vcs_ref: crate::db::DbVcsRef) -> serde_json::Value {
    serde_json::json!({
        "name": vcs_ref.name,
        "target": vcs_ref.target,
        "version": vcs_ref.version,
    })
}

fn workspace_id_from_headers(headers: &HeaderMap) -> Result<Option<Uuid>, VfsError> {
    let Some(value) = headers.get("x-stratum-workspace") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| VfsError::InvalidArgs {
        message: "invalid x-stratum-workspace header".to_string(),
    })?;
    let id = Uuid::parse_str(value).map_err(|_| VfsError::InvalidArgs {
        message: format!("invalid workspace id: {value}"),
    })?;
    Ok(Some(id))
}

async fn update_workspace_head_from_headers(
    state: &AppState,
    headers: &HeaderMap,
    head_commit: Option<String>,
) -> Result<(), VfsError> {
    let Some(workspace_id) = workspace_id_from_headers(headers)? else {
        return Ok(());
    };
    match state
        .workspaces
        .update_head_commit(workspace_id, head_commit)
        .await?
    {
        Some(_) => Ok(()),
        None => Err(VfsError::NotFound {
            path: format!("workspace:{workspace_id}"),
        }),
    }
}

async fn validate_workspace_header(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<Uuid>, VfsError> {
    let Some(workspace_id) = workspace_id_from_headers(headers)? else {
        return Ok(None);
    };
    match state.workspaces.get_workspace(workspace_id).await? {
        Some(_) => Ok(Some(workspace_id)),
        None => Err(VfsError::NotFound {
            path: format!("workspace:{workspace_id}"),
        }),
    }
}

async fn vcs_list_refs(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.db.list_refs().await {
        Ok(refs) => Json(serde_json::json!({
            "refs": refs.into_iter().map(ref_json).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn vcs_create_ref(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateRefRequest>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state.db.create_ref(&req.name, &req.target).await {
        Ok(vcs_ref) => (StatusCode::CREATED, Json(ref_json(vcs_ref))).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn vcs_update_ref(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(req): Json<UpdateRefRequest>,
) -> impl IntoResponse {
    if let Err(e) = require_admin(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::UNAUTHORIZED), e.to_string()).into_response();
    }

    match state
        .db
        .update_ref(
            &name,
            &req.expected_target,
            req.expected_version,
            &req.target,
        )
        .await
    {
        Ok(vcs_ref) => Json(ref_json(vcs_ref)).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn vcs_commit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CommitRequest>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    if let Err(e) = validate_workspace_header(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response();
    }

    match state.db.commit_as(&req.message, &session).await {
        Ok(hash) => {
            if let Err(e) =
                update_workspace_head_from_headers(&state, &headers, Some(hash.clone())).await
            {
                return err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response();
            }
            Json(serde_json::json!({
                "hash": hash,
                "message": req.message,
                "author": session.username,
            }))
            .into_response()
        }
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn vcs_log(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let commits = match state.db.vcs_log_as(&session).await {
        Ok(commits) => commits,
        Err(e) => {
            return err_json(
                error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
            .into_response();
        }
    };
    let items: Vec<serde_json::Value> = commits
        .iter()
        .map(|c| {
            serde_json::json!({
                "hash": c.id.short_hex(),
                "message": c.message,
                "author": c.author,
                "timestamp": c.timestamp,
            })
        })
        .collect();
    Json(serde_json::json!({"commits": items})).into_response()
}

async fn vcs_revert(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RevertRequest>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    if let Err(e) = validate_workspace_header(&state, &headers).await {
        return err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response();
    }

    match state.db.revert_as(&req.hash, &session).await {
        Ok(()) => {
            if let Err(e) =
                update_workspace_head_from_headers(&state, &headers, Some(req.hash.clone())).await
            {
                return err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response();
            }
            Json(serde_json::json!({"reverted_to": req.hash})).into_response()
        }
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn vcs_status(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    match state.db.vcs_status_as(&session).await {
        Ok(status) => (StatusCode::OK, status).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
    }
}

async fn vcs_diff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DiffQuery>,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    match state.db.vcs_diff_as(query.path.as_deref(), &session).await {
        Ok(diff) => (StatusCode::OK, diff).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ROOT_UID;
    use crate::auth::Uid;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::server::ServerState;
    use crate::workspace::{
        InMemoryWorkspaceMetadataStore, IssuedWorkspaceToken, ValidWorkspaceToken,
        WorkspaceMetadataStore, WorkspaceRecord,
    };
    use axum::extract::Path;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        })
    }

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
        headers
    }

    fn workspace_headers(username: &str, workspace_id: Uuid) -> HeaderMap {
        let mut headers = user_headers(username);
        headers.insert(
            "x-stratum-workspace",
            workspace_id.to_string().parse().unwrap(),
        );
        headers
    }

    fn workspace_bearer_headers(raw_secret: &str, workspace_id: Uuid) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {raw_secret}").parse().unwrap(),
        );
        headers.insert(
            "x-stratum-workspace",
            workspace_id.to_string().parse().unwrap(),
        );
        headers
    }

    async fn json_body(response: axum::response::Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn commit_file(
        db: &StratumDb,
        root: &mut Session,
        path: &str,
        contents: &str,
        message: &str,
    ) -> String {
        db.execute_command(&format!("touch {path}"), root)
            .await
            .unwrap();
        db.execute_command(&format!("write {path} {contents}"), root)
            .await
            .unwrap();
        db.commit(message, "root").await.unwrap();
        db.vcs_log().await[0].id.to_hex()
    }

    #[tokio::test]
    async fn admin_can_create_list_and_update_refs_over_http() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        let state = test_state(db);

        let create_response = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: "agent/alice/session-1".to_string(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(create_response.status(), StatusCode::CREATED);
        let created = json_body(create_response).await;
        assert_eq!(
            created.get("name"),
            Some(&serde_json::json!("agent/alice/session-1"))
        );
        assert_eq!(created.get("target"), Some(&serde_json::json!(first)));
        assert_eq!(created.get("version"), Some(&serde_json::json!(1)));

        let list_response = vcs_list_refs(State(state.clone()), user_headers("root"))
            .await
            .into_response();
        assert_eq!(list_response.status(), StatusCode::OK);
        let refs = json_body(list_response).await;
        let refs = refs
            .get("refs")
            .and_then(serde_json::Value::as_array)
            .expect("refs array");
        assert!(
            refs.iter()
                .any(|item| item.get("name") == Some(&serde_json::json!("main")))
        );
        assert!(
            refs.iter()
                .any(|item| item.get("name") == Some(&serde_json::json!("agent/alice/session-1")))
        );

        let update_response = vcs_update_ref(
            State(state),
            user_headers("root"),
            Path("agent/alice/session-1".to_string()),
            Json(UpdateRefRequest {
                target: second.clone(),
                expected_target: first,
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(update_response.status(), StatusCode::OK);
        let updated = json_body(update_response).await;
        assert_eq!(updated.get("target"), Some(&serde_json::json!(second)));
        assert_eq!(updated.get("version"), Some(&serde_json::json!(2)));
    }

    #[tokio::test]
    async fn duplicate_create_and_stale_ref_update_conflict_without_mutation() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let first = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let second = commit_file(&db, &mut root, "/a.txt", "second", "second").await;
        let third = commit_file(&db, &mut root, "/a.txt", "third", "third").await;
        let state = test_state(db);
        let name = "agent/alice/session-1".to_string();

        let created = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: name.clone(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(created.status(), StatusCode::CREATED);

        let duplicate = vcs_create_ref(
            State(state.clone()),
            user_headers("root"),
            Json(CreateRefRequest {
                name: name.clone(),
                target: first.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);

        let updated = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(name.clone()),
            Json(UpdateRefRequest {
                target: second.clone(),
                expected_target: first.clone(),
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(updated.status(), StatusCode::OK);

        let stale = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(name.clone()),
            Json(UpdateRefRequest {
                target: third,
                expected_target: first.clone(),
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(stale.status(), StatusCode::CONFLICT);

        let stale_unknown_target = vcs_update_ref(
            State(state.clone()),
            user_headers("root"),
            Path(name.clone()),
            Json(UpdateRefRequest {
                target: "0".repeat(64),
                expected_target: first,
                expected_version: 1,
            }),
        )
        .await
        .into_response();
        assert_eq!(stale_unknown_target.status(), StatusCode::CONFLICT);

        let refs = json_body(
            vcs_list_refs(State(state), user_headers("root"))
                .await
                .into_response(),
        )
        .await;
        let current = refs
            .get("refs")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .find(|item| item.get("name") == Some(&serde_json::json!(name)))
            .expect("session ref exists");
        assert_eq!(current.get("target"), Some(&serde_json::json!(second)));
        assert_eq!(current.get("version"), Some(&serde_json::json!(2)));
    }

    #[tokio::test]
    async fn non_admin_and_workspace_bearer_cannot_manage_refs() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        let commit = commit_file(&db, &mut root, "/a.txt", "first", "first").await;
        let state = test_state(db.clone());

        let missing_auth = vcs_list_refs(State(state.clone()), HeaderMap::new())
            .await
            .into_response();
        assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

        let list_response = vcs_list_refs(State(state.clone()), user_headers("bob"))
            .await
            .into_response();
        assert_eq!(list_response.status(), StatusCode::FORBIDDEN);

        let create_response = vcs_create_ref(
            State(state.clone()),
            user_headers("bob"),
            Json(CreateRefRequest {
                name: "agent/bob/session-1".to_string(),
                target: commit,
            }),
        )
        .await
        .into_response();
        assert_eq!(create_response.status(), StatusCode::FORBIDDEN);

        let workspace_store = Arc::new(InMemoryWorkspaceMetadataStore::new());
        let workspace = workspace_store
            .create_workspace("demo", "/demo")
            .await
            .unwrap();
        let issued = workspace_store
            .issue_scoped_workspace_token(
                workspace.id,
                "root-scoped",
                ROOT_UID,
                vec!["/demo".to_string()],
                vec!["/demo".to_string()],
            )
            .await
            .unwrap();
        let scoped_state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: workspace_store,
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let workspace_bearer = vcs_list_refs(
            State(scoped_state),
            workspace_bearer_headers(&issued.raw_secret, workspace.id),
        )
        .await
        .into_response();
        assert_eq!(workspace_bearer.status(), StatusCode::FORBIDDEN);
    }

    struct FailingHeadStore;

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for FailingHeadStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(Vec::new())
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, _id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            Ok(None)
        }

        async fn update_head_commit(
            &self,
            _id: Uuid,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            Err(VfsError::IoError(std::io::Error::other(
                "metadata write failed",
            )))
        }

        async fn issue_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    struct ExistingFailingHeadStore {
        workspace_id: Uuid,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for ExistingFailingHeadStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(Vec::new())
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id == self.workspace_id {
                Ok(Some(WorkspaceRecord {
                    id,
                    name: "demo".to_string(),
                    root_path: "/demo".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: crate::vcs::MAIN_REF.to_string(),
                    session_ref: None,
                }))
            } else {
                Ok(None)
            }
        }

        async fn update_head_commit(
            &self,
            _id: Uuid,
            _head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            Err(VfsError::IoError(std::io::Error::other(
                "metadata write failed",
            )))
        }

        async fn issue_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    struct RecordingWorkspaceStore {
        workspace_id: Uuid,
        updated: RwLock<Option<String>>,
    }

    #[async_trait::async_trait]
    impl WorkspaceMetadataStore for RecordingWorkspaceStore {
        async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
            Ok(Vec::new())
        }

        async fn create_workspace(
            &self,
            _name: &str,
            _root_path: &str,
        ) -> Result<WorkspaceRecord, VfsError> {
            unreachable!("not used")
        }

        async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id == self.workspace_id {
                Ok(Some(WorkspaceRecord {
                    id,
                    name: "demo".to_string(),
                    root_path: "/demo".to_string(),
                    head_commit: None,
                    version: 0,
                    base_ref: crate::vcs::MAIN_REF.to_string(),
                    session_ref: None,
                }))
            } else {
                Ok(None)
            }
        }

        async fn update_head_commit(
            &self,
            id: Uuid,
            head_commit: Option<String>,
        ) -> Result<Option<WorkspaceRecord>, VfsError> {
            if id != self.workspace_id {
                return Ok(None);
            }
            *self.updated.write().await = head_commit.clone();
            Ok(Some(WorkspaceRecord {
                id,
                name: "demo".to_string(),
                root_path: "/demo".to_string(),
                head_commit,
                version: 1,
                base_ref: crate::vcs::MAIN_REF.to_string(),
                session_ref: None,
            }))
        }

        async fn issue_workspace_token(
            &self,
            _workspace_id: Uuid,
            _name: &str,
            _agent_uid: Uid,
        ) -> Result<IssuedWorkspaceToken, VfsError> {
            unreachable!("not used")
        }

        async fn validate_workspace_token(
            &self,
            _workspace_id: Uuid,
            _raw_secret: &str,
        ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn non_root_user_cannot_revert_global_state() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md version1", &mut root)
            .await
            .unwrap();
        let original = db.commit("v1", "root").await.unwrap();
        db.execute_command("write a.md version2", &mut root)
            .await
            .unwrap();
        db.commit("v2", "root").await.unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let response = vcs_revert(
            State(test_state(db.clone())),
            user_headers("bob"),
            Json(RevertRequest { hash: original }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            String::from_utf8_lossy(&db.cat("/a.md").await.unwrap()),
            "version2"
        );
    }

    #[tokio::test]
    async fn non_root_user_cannot_read_global_vcs_log() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();
        db.commit("init", "root").await.unwrap();

        let response = vcs_log(State(test_state(db)), user_headers("bob"))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn non_root_user_cannot_read_global_vcs_status_or_diff() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md before", &mut root)
            .await
            .unwrap();
        db.commit("init", "root").await.unwrap();
        db.execute_command("write a.md after", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let status_response = vcs_status(State(test_state(db.clone())), user_headers("bob"))
            .await
            .into_response();
        assert_eq!(status_response.status(), StatusCode::FORBIDDEN);

        let diff_response = vcs_diff(
            State(test_state(db)),
            user_headers("bob"),
            Query(DiffQuery { path: None }),
        )
        .await
        .into_response();
        assert_eq!(diff_response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn non_root_user_cannot_commit_global_vcs_state() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let response = vcs_commit(
            State(test_state(db.clone())),
            user_headers("bob"),
            Json(CommitRequest {
                message: "blocked".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(db.vcs_log().await.len(), 0);
    }

    #[tokio::test]
    async fn workspace_head_update_failure_is_not_reported_as_success() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();
        let workspace_id = Uuid::new_v4();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(ExistingFailingHeadStore { workspace_id }),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let response = vcs_commit(
            State(state),
            workspace_headers("root", workspace_id),
            Json(CommitRequest {
                message: "with workspace".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn unknown_workspace_header_is_rejected_before_commit() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();

        let response = vcs_commit(
            State(Arc::new(ServerState {
                db: Arc::new(db.clone()),
                workspaces: Arc::new(FailingHeadStore),
                idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            })),
            workspace_headers("root", Uuid::new_v4()),
            Json(CommitRequest {
                message: "blocked".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(db.vcs_log().await.len(), 0);
    }

    #[tokio::test]
    async fn known_workspace_header_updates_head_after_commit() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md content", &mut root)
            .await
            .unwrap();
        let workspace_id = Uuid::new_v4();
        let store = Arc::new(RecordingWorkspaceStore {
            workspace_id,
            updated: RwLock::new(None),
        });
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: store.clone(),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
        });

        let response = vcs_commit(
            State(state),
            workspace_headers("root", workspace_id),
            Json(CommitRequest {
                message: "with workspace".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(store.updated.read().await.is_some());
    }

    #[tokio::test]
    async fn root_can_read_diff_plain_text() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch a.md", &mut root).await.unwrap();
        db.execute_command("write a.md before", &mut root)
            .await
            .unwrap();
        db.commit("init", "root").await.unwrap();
        db.execute_command("write a.md after", &mut root)
            .await
            .unwrap();

        let response = vcs_diff(
            State(test_state(db)),
            user_headers("root"),
            Query(DiffQuery {
                path: Some("/a.md".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("diff -- /a.md"));
        assert!(body.contains("-before"));
        assert!(body.contains("+after"));
    }
}
