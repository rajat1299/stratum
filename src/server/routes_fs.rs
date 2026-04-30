use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use super::AppState;
use super::middleware::session_from_headers;
use crate::auth::session::Session;
use crate::error::VfsError;

#[derive(Deserialize, Default)]
pub struct FsQuery {
    pub stat: Option<bool>,
    pub op: Option<String>,
    pub dst: Option<String>,
    pub recursive: Option<bool>,
}

#[derive(Deserialize, Default)]
pub struct SearchQuery {
    pub pattern: Option<String>,
    pub path: Option<String>,
    pub name: Option<String>,
    pub recursive: Option<bool>,
}

fn err_json(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(serde_json::json!({"error": msg.into()})))
}

fn error_status(error: &VfsError, fallback: StatusCode) -> StatusCode {
    match error {
        VfsError::AuthError { .. } => StatusCode::UNAUTHORIZED,
        VfsError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        VfsError::NotFound { .. } => StatusCode::NOT_FOUND,
        _ => fallback,
    }
}

fn error_message(session: &Session, error: &VfsError) -> String {
    match error {
        VfsError::InvalidExtension { name } => format!(
            "stratum: markdown compatibility mode only supports .md files: '{}'",
            session.project_mounted_path(name)
        ),
        VfsError::NotFound { path } => format!(
            "stratum: no such file or directory: '{}'",
            session.project_mounted_path(path)
        ),
        VfsError::IsDirectory { path } => {
            format!(
                "stratum: is a directory: '{}'",
                session.project_mounted_path(path)
            )
        }
        VfsError::NotDirectory { path } => format!(
            "stratum: not a directory: '{}'",
            session.project_mounted_path(path)
        ),
        VfsError::AlreadyExists { path } => {
            format!(
                "stratum: already exists: '{}'",
                session.project_mounted_path(path)
            )
        }
        VfsError::NotEmpty { path } => format!(
            "stratum: directory not empty: '{}'",
            session.project_mounted_path(path)
        ),
        VfsError::InvalidPath { path } => format!(
            "stratum: invalid path: '{}'",
            session.project_mounted_path(path)
        ),
        VfsError::SymlinkLoop { path } => {
            format!(
                "stratum: symlink loop: '{}'",
                session.project_mounted_path(path)
            )
        }
        VfsError::PermissionDenied { path } => format!(
            "stratum: permission denied: '{}'",
            session.project_mounted_path(path)
        ),
        _ => error.to_string(),
    }
}

fn err_json_for(
    session: &Session,
    error: &VfsError,
    fallback: StatusCode,
) -> axum::response::Response {
    err_json(error_status(error, fallback), error_message(session, error)).into_response()
}

fn api_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn resolve_api_path(session: &Session, path: &str) -> Result<String, VfsError> {
    session.resolve_mounted_path(&api_path(path))
}

fn resolve_root_path(session: &Session) -> Result<String, VfsError> {
    session.resolve_mounted_path("/")
}

fn resolve_optional_query_path(session: &Session, path: Option<&str>) -> Result<String, VfsError> {
    match path {
        Some(path) => resolve_api_path(session, path),
        None => resolve_root_path(session),
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/fs", get(get_fs_root))
        .route(
            "/fs/{*path}",
            get(get_fs).put(put_fs).delete(delete_fs).post(post_fs),
        )
        .route("/search/grep", get(search_grep))
        .route("/search/find", get(search_find))
        .route("/tree", get(get_tree_root))
        .route("/tree/{*path}", get(get_tree))
}

async fn get_fs_root(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_root_path(&session) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    match state.db.ls_as(Some(&path), &session).await {
        Ok(entries) => {
            Json(ls_to_json(&entries, &session.project_mounted_path(&path))).into_response()
        }
        Err(e) => err_json_for(&session, &e, StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<FsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    if query.stat.unwrap_or(false) {
        return match state.db.stat_as(&path, &session).await {
            Ok(info) => Json(serde_json::json!({
                "inode_id": info.inode_id,
                "kind": info.kind,
                "size": info.size,
                "mode": format!("0{:o}", info.mode),
                "uid": info.uid,
                "gid": info.gid,
                "created": info.created,
                "modified": info.modified,
            }))
            .into_response(),
            Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
        };
    }

    match state.db.cat_as(&path, &session).await {
        Ok(content) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            Body::from(content),
        )
            .into_response(),
        Err(crate::error::VfsError::IsDirectory { .. }) => {
            match state.db.ls_as(Some(&path), &session).await {
                Ok(entries) => {
                    Json(ls_to_json(&entries, &session.project_mounted_path(&path))).into_response()
                }
                Err(e) => err_json_for(&session, &e, StatusCode::INTERNAL_SERVER_ERROR),
            }
        }
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
}

async fn put_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let is_dir = headers
        .get("x-stratum-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "directory")
        .unwrap_or(false);
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    if is_dir {
        match state.db.mkdir_p_as(&path, &session).await {
            Ok(()) => Json(serde_json::json!({
                "created": session.project_mounted_path(&path),
                "type": "directory"
            }))
            .into_response(),
            Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
        }
    } else {
        let size = body.len();
        match state.db.write_file_as(&path, body.to_vec(), &session).await {
            Ok(()) => Json(serde_json::json!({
                "written": session.project_mounted_path(&path),
                "size": size
            }))
            .into_response(),
            Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
        }
    }
}

async fn delete_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<FsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let recursive = query.recursive.unwrap_or(false);
    let result = state.db.rm_as(&path, recursive, &session).await;

    match result {
        Ok(()) => Json(serde_json::json!({
            "deleted": session.project_mounted_path(&path)
        }))
        .into_response(),
        Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    }
}

async fn post_fs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<FsQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    match query.op.as_deref() {
        Some("copy") => {
            let dst = match &query.dst {
                Some(d) => d.as_str(),
                None => {
                    return err_json(StatusCode::BAD_REQUEST, "missing dst parameter")
                        .into_response();
                }
            };
            let dst = match resolve_api_path(&session, dst) {
                Ok(dst) => dst,
                Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            };
            match state.db.cp_as(&path, &dst, &session).await {
                Ok(()) => Json(serde_json::json!({
                    "copied": session.project_mounted_path(&path),
                    "to": session.project_mounted_path(&dst)
                }))
                .into_response(),
                Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
            }
        }
        Some("move") => {
            let dst = match &query.dst {
                Some(d) => d.as_str(),
                None => {
                    return err_json(StatusCode::BAD_REQUEST, "missing dst parameter")
                        .into_response();
                }
            };
            let dst = match resolve_api_path(&session, dst) {
                Ok(dst) => dst,
                Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            };
            match state.db.mv_as(&path, &dst, &session).await {
                Ok(()) => Json(serde_json::json!({
                    "moved": session.project_mounted_path(&path),
                    "to": session.project_mounted_path(&dst)
                }))
                .into_response(),
                Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
            }
        }
        Some(op) => err_json(StatusCode::BAD_REQUEST, format!("unknown op: {op}")).into_response(),
        None => err_json(StatusCode::BAD_REQUEST, "missing op parameter").into_response(),
    }
}

async fn search_grep(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let pattern = match &query.pattern {
        Some(p) => p.clone(),
        None => {
            return err_json(StatusCode::BAD_REQUEST, "missing pattern parameter").into_response();
        }
    };

    let path = match resolve_optional_query_path(&session, query.path.as_deref()) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let recursive = query.recursive.unwrap_or(true);

    match state
        .db
        .grep_as(&pattern, Some(&path), recursive, &session)
        .await
    {
        Ok(results) => {
            let items: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "file": session.project_mounted_path(&r.file),
                        "line_num": r.line_num,
                        "line": r.line
                    })
                })
                .collect();
            Json(serde_json::json!({"results": items, "count": items.len()})).into_response()
        }
        Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    }
}

async fn search_find(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };

    let path = match resolve_optional_query_path(&session, query.path.as_deref()) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let name = query.name.as_deref();

    match state.db.find_as(Some(&path), name, &session).await {
        Ok(results) => {
            let results: Vec<String> = results
                .iter()
                .map(|path| session.project_mounted_path(path))
                .collect();
            Json(serde_json::json!({"results": results, "count": results.len()})).into_response()
        }
        Err(e) => err_json_for(&session, &e, StatusCode::BAD_REQUEST),
    }
}

async fn get_tree_root(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_root_path(&session) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    match state.db.tree_as(Some(&path), &session).await {
        Ok(tree) => (StatusCode::OK, tree).into_response(),
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
}

async fn get_tree(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    let path = match resolve_api_path(&session, &path) {
        Ok(path) => path,
        Err(e) => return err_json(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    match state.db.tree_as(Some(&path), &session).await {
        Ok(tree) => (StatusCode::OK, tree).into_response(),
        Err(e) => err_json_for(&session, &e, StatusCode::NOT_FOUND),
    }
}

fn ls_to_json(entries: &[crate::fs::LsEntry], path: &str) -> serde_json::Value {
    let items: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            serde_json::json!({
                "name": e.name,
                "is_dir": e.is_dir,
                "is_symlink": e.is_symlink,
                "size": e.size,
                "mode": format!("0{:o}", e.mode),
                "uid": e.uid,
                "gid": e.gid,
                "modified": e.modified,
            })
        })
        .collect();
    serde_json::json!({"entries": items, "path": path})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::db::StratumDb;
    use crate::server::ServerState;
    use crate::workspace::{InMemoryWorkspaceMetadataStore, WorkspaceMetadataStore};
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_state(db: StratumDb) -> AppState {
        Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
        })
    }

    fn user_headers(username: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("User {username}").parse().unwrap());
        headers
    }

    fn extract_agent_token(output: &str) -> String {
        output
            .lines()
            .last()
            .expect("agent token line")
            .trim()
            .to_string()
    }

    fn workspace_headers(workspace_id: Uuid, raw_secret: &str) -> HeaderMap {
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

    async fn response_bytes(response: axum::response::Response) -> Bytes {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        serde_json::from_slice(&response_bytes(response).await).unwrap()
    }

    async fn assert_projected_error(
        response: axum::response::Response,
        status: StatusCode,
        expected_path: &str,
    ) {
        assert_eq!(response.status(), status);
        let body = response_json(response).await;
        let error = body["error"].as_str().expect("error string");
        assert!(error.contains(expected_path), "{error}");
        assert!(!error.contains("/demo/"), "{error}");
    }

    async fn workspace_state_with_token(
        db: StratumDb,
        workspace_root: &str,
        agent_uid: u32,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> (AppState, Uuid, String) {
        let store = InMemoryWorkspaceMetadataStore::new();
        let workspace = store
            .create_workspace("demo", workspace_root)
            .await
            .unwrap();
        let issued = store
            .issue_scoped_workspace_token(
                workspace.id,
                "ci-token",
                agent_uid,
                read_prefixes,
                write_prefixes,
            )
            .await
            .unwrap();
        let state = Arc::new(ServerState {
            db: Arc::new(db),
            workspaces: Arc::new(store),
        });
        (state, workspace.id, issued.raw_secret)
    }

    #[tokio::test]
    async fn get_file_denies_authenticated_user_without_read_permission() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        db.execute_command("touch secret.md", &mut root)
            .await
            .unwrap();
        db.execute_command("write secret.md classified", &mut root)
            .await
            .unwrap();
        db.execute_command("chmod 600 secret.md", &mut root)
            .await
            .unwrap();
        db.execute_command("adduser bob", &mut root).await.unwrap();

        let response = get_fs(
            State(test_state(db)),
            Path("/secret.md".to_string()),
            Query(FsQuery::default()),
            user_headers("bob"),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn workspace_bearer_uses_workspace_relative_paths_for_fs_search_find_and_tree() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/read", &root).await.unwrap();
        db.mkdir_p_as("/demo/search", &root).await.unwrap();
        db.mkdir_p_as("/demo/write", &root).await.unwrap();
        db.write_file_as("/demo/read/allowed.txt", b"readable needle".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/demo/search/hit.txt", b"needle\nsecond".to_vec(), &root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/write", &mut root)
            .await
            .unwrap();

        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent.uid,
            vec!["/demo".to_string()],
            vec!["/demo".to_string()],
        )
        .await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let read_response = get_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_response.status(), StatusCode::OK);
        assert_eq!(
            response_bytes(read_response).await,
            Bytes::from_static(b"readable needle")
        );

        let root_list = get_fs_root(State(state.clone()), headers.clone())
            .await
            .into_response();
        assert_eq!(root_list.status(), StatusCode::OK);
        let root_list = response_json(root_list).await;
        assert_eq!(root_list.get("path"), Some(&serde_json::json!("/")));
        assert!(
            root_list["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry.get("name") == Some(&serde_json::json!("read")))
        );

        let read_list = get_fs(
            State(state.clone()),
            Path("/read".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_list.status(), StatusCode::OK);
        let read_list = response_json(read_list).await;
        assert_eq!(read_list.get("path"), Some(&serde_json::json!("/read")));

        let write_response = put_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"written"),
        )
        .await
        .into_response();
        assert_eq!(write_response.status(), StatusCode::OK);
        let write_response = response_json(write_response).await;
        assert_eq!(
            write_response.get("written"),
            Some(&serde_json::json!("/write/new.txt"))
        );

        let mkdir_response = put_fs(
            State(state.clone()),
            Path("/write/nested".to_string()),
            {
                let mut headers = headers.clone();
                headers.insert("x-stratum-type", "directory".parse().unwrap());
                headers
            },
            Bytes::new(),
        )
        .await
        .into_response();
        assert_eq!(mkdir_response.status(), StatusCode::OK);
        let mkdir_response = response_json(mkdir_response).await;
        assert_eq!(
            mkdir_response.get("created"),
            Some(&serde_json::json!("/write/nested"))
        );

        let copy_response = post_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/write/copied.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(copy_response.status(), StatusCode::OK);
        let copy_response = response_json(copy_response).await;
        assert_eq!(
            copy_response.get("copied"),
            Some(&serde_json::json!("/read/allowed.txt"))
        );
        assert_eq!(
            copy_response.get("to"),
            Some(&serde_json::json!("/write/copied.txt"))
        );

        let move_response = post_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/write/moved.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(move_response.status(), StatusCode::OK);
        let move_response = response_json(move_response).await;
        assert_eq!(
            move_response.get("moved"),
            Some(&serde_json::json!("/write/new.txt"))
        );
        assert_eq!(
            move_response.get("to"),
            Some(&serde_json::json!("/write/moved.txt"))
        );

        let delete_response = delete_fs(
            State(state.clone()),
            Path("/write/copied.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(delete_response.status(), StatusCode::OK);
        let delete_response = response_json(delete_response).await;
        assert_eq!(
            delete_response.get("deleted"),
            Some(&serde_json::json!("/write/copied.txt"))
        );

        let grep_response = search_grep(
            State(state.clone()),
            Query(SearchQuery {
                pattern: Some("needle".to_string()),
                path: None,
                name: None,
                recursive: None,
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(grep_response.status(), StatusCode::OK);
        let grep_response = response_json(grep_response).await;
        let grep_files: Vec<_> = grep_response["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result["file"].as_str().unwrap())
            .collect();
        assert!(grep_files.contains(&"/read/allowed.txt"));
        assert!(grep_files.contains(&"/search/hit.txt"));
        assert!(!grep_files.iter().any(|file| file.starts_with("/demo/")));

        let find_response = search_find(
            State(state.clone()),
            Query(SearchQuery {
                pattern: None,
                path: None,
                name: Some("*.txt".to_string()),
                recursive: None,
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(find_response.status(), StatusCode::OK);
        let find_response = response_json(find_response).await;
        let find_results: Vec<_> = find_response["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result.as_str().unwrap())
            .collect();
        assert!(find_results.contains(&"/read/allowed.txt"));
        assert!(find_results.contains(&"/search/hit.txt"));
        assert!(!find_results.iter().any(|path| path.starts_with("/demo/")));

        let tree_response = get_tree_root(State(state), headers).await.into_response();
        assert_eq!(tree_response.status(), StatusCode::OK);
        let tree_response =
            String::from_utf8(response_bytes(tree_response).await.to_vec()).unwrap();
        assert!(tree_response.starts_with(".\n"));
        assert!(tree_response.contains("read/"));
        assert!(tree_response.contains("allowed.txt"));
        assert!(!tree_response.contains("demo/"));
    }

    #[tokio::test]
    async fn workspace_bearer_reads_and_writes_only_inside_token_prefixes() {
        let db = StratumDb::open_memory();
        let mut root = Session::root();
        let raw_agent_token = extract_agent_token(
            &db.execute_command("addagent ci-agent", &mut root)
                .await
                .unwrap(),
        );
        let agent = db.authenticate_token(&raw_agent_token).await.unwrap();
        db.mkdir_p_as("/demo/read", &root).await.unwrap();
        db.mkdir_p_as("/demo/write", &root).await.unwrap();
        db.mkdir_p_as("/demo/outside", &root).await.unwrap();
        db.mkdir_p_as("/outside", &root).await.unwrap();
        db.write_file_as("/demo/read/allowed.txt", b"readable".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/demo/outside/secret.txt", b"secret".to_vec(), &root)
            .await
            .unwrap();
        db.write_file_as("/outside/secret.txt", b"escaped".to_vec(), &root)
            .await
            .unwrap();
        db.execute_command("chmod 777 /demo/write", &mut root)
            .await
            .unwrap();

        let (state, workspace_id, raw_secret) = workspace_state_with_token(
            db,
            "/demo",
            agent.uid,
            vec!["/demo/read".to_string()],
            vec!["/demo/write".to_string()],
        )
        .await;
        let headers = workspace_headers(workspace_id, &raw_secret);

        let read_allowed = get_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(read_allowed.status(), StatusCode::OK);

        let traversal_clamped_inside_mount = get_fs(
            State(state.clone()),
            Path("/../read/allowed.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_eq!(traversal_clamped_inside_mount.status(), StatusCode::OK);

        let read_denied = get_fs(
            State(state.clone()),
            Path("/outside/secret.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(read_denied, StatusCode::FORBIDDEN, "/outside/secret.txt").await;

        let traversal_denied = get_fs(
            State(state.clone()),
            Path("/../outside/secret.txt".to_string()),
            Query(FsQuery::default()),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(
            traversal_denied,
            StatusCode::FORBIDDEN,
            "/outside/secret.txt",
        )
        .await;

        let write_allowed = put_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            headers.clone(),
            Bytes::from_static(b"written"),
        )
        .await
        .into_response();
        assert_eq!(write_allowed.status(), StatusCode::OK);

        let copy_denied = post_fs(
            State(state.clone()),
            Path("/read/allowed.txt".to_string()),
            Query(FsQuery {
                op: Some("copy".to_string()),
                dst: Some("/outside/copied.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(copy_denied, StatusCode::FORBIDDEN, "/outside/copied.txt").await;

        let move_denied = post_fs(
            State(state.clone()),
            Path("/write/new.txt".to_string()),
            Query(FsQuery {
                op: Some("move".to_string()),
                dst: Some("/outside/moved.txt".to_string()),
                ..FsQuery::default()
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(move_denied, StatusCode::FORBIDDEN, "/outside/moved.txt").await;

        let search_denied = search_grep(
            State(state.clone()),
            Query(SearchQuery {
                pattern: Some("readable".to_string()),
                path: None,
                name: None,
                recursive: None,
            }),
            headers.clone(),
        )
        .await
        .into_response();
        assert_projected_error(search_denied, StatusCode::FORBIDDEN, "/").await;

        let write_denied = put_fs(
            State(state),
            Path("/outside/new.txt".to_string()),
            headers,
            Bytes::from_static(b"blocked"),
        )
        .await
        .into_response();
        assert_projected_error(write_denied, StatusCode::FORBIDDEN, "/outside/new.txt").await;
    }
}
