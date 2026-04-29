use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use super::middleware::session_from_headers;
use super::AppState;
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

fn api_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
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

    match state.db.ls_as(None, &session).await {
        Ok(entries) => Json(ls_to_json(&entries, "/")).into_response(),
        Err(e) => err_json(
            error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
        .into_response(),
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
    let path = api_path(&path);

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
            Err(e) => {
                err_json(error_status(&e, StatusCode::NOT_FOUND), e.to_string()).into_response()
            }
        };
    }

    match state.db.cat_as(&path, &session).await {
        Ok(content) => (
            StatusCode::OK,
            [("content-type", "text/markdown")],
            Body::from(content),
        )
            .into_response(),
        Err(crate::error::VfsError::IsDirectory { .. }) => {
            match state.db.ls_as(Some(&path), &session).await {
                Ok(entries) => Json(ls_to_json(&entries, &path)).into_response(),
                Err(e) => err_json(
                    error_status(&e, StatusCode::INTERNAL_SERVER_ERROR),
                    e.to_string(),
                )
                .into_response(),
            }
        }
        Err(e) => err_json(error_status(&e, StatusCode::NOT_FOUND), e.to_string()).into_response(),
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
    let path = api_path(&path);

    if is_dir {
        match state.db.mkdir_p_as(&path, &session).await {
            Ok(()) => {
                Json(serde_json::json!({"created": path, "type": "directory"})).into_response()
            }
            Err(e) => {
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
            }
        }
    } else {
        let size = body.len();
        match state.db.write_file_as(&path, body.to_vec(), &session).await {
            Ok(()) => Json(serde_json::json!({"written": path, "size": size})).into_response(),
            Err(e) => {
                err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
            }
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
    let path = api_path(&path);

    let recursive = query.recursive.unwrap_or(false);
    let result = state.db.rm_as(&path, recursive, &session).await;

    match result {
        Ok(()) => Json(serde_json::json!({"deleted": path})).into_response(),
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
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
    let path = api_path(&path);

    match query.op.as_deref() {
        Some("copy") => {
            let dst = match &query.dst {
                Some(d) => d.as_str(),
                None => {
                    return err_json(StatusCode::BAD_REQUEST, "missing dst parameter")
                        .into_response();
                }
            };
            let dst = api_path(dst);
            match state.db.cp_as(&path, &dst, &session).await {
                Ok(()) => Json(serde_json::json!({"copied": path, "to": dst})).into_response(),
                Err(e) => err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                    .into_response(),
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
            let dst = api_path(dst);
            match state.db.mv_as(&path, &dst, &session).await {
                Ok(()) => Json(serde_json::json!({"moved": path, "to": dst})).into_response(),
                Err(e) => err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string())
                    .into_response(),
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

    let path = query.path.as_deref().map(api_path);
    let path = path.as_deref();
    let recursive = query.recursive.unwrap_or(true);

    match state.db.grep_as(&pattern, path, recursive, &session).await {
        Ok(results) => {
            let items: Vec<serde_json::Value> = results
                .iter()
                .map(
                    |r| serde_json::json!({"file": r.file, "line_num": r.line_num, "line": r.line}),
                )
                .collect();
            Json(serde_json::json!({"results": items, "count": items.len()})).into_response()
        }
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
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

    let path = query.path.as_deref().map(api_path);
    let path = path.as_deref();
    let name = query.name.as_deref();

    match state.db.find_as(path, name, &session).await {
        Ok(results) => {
            Json(serde_json::json!({"results": results, "count": results.len()})).into_response()
        }
        Err(e) => {
            err_json(error_status(&e, StatusCode::BAD_REQUEST), e.to_string()).into_response()
        }
    }
}

async fn get_tree_root(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let session = match session_from_headers(&state, &headers).await {
        Ok(s) => s,
        Err(e) => return err_json(StatusCode::UNAUTHORIZED, e.to_string()).into_response(),
    };
    match state.db.tree_as(None, &session).await {
        Ok(tree) => (StatusCode::OK, tree).into_response(),
        Err(e) => err_json(error_status(&e, StatusCode::NOT_FOUND), e.to_string()).into_response(),
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
    let path = api_path(&path);
    match state.db.tree_as(Some(&path), &session).await {
        Ok(tree) => (StatusCode::OK, tree).into_response(),
        Err(e) => err_json(error_status(&e, StatusCode::NOT_FOUND), e.to_string()).into_response(),
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
    use crate::workspace::InMemoryWorkspaceMetadataStore;
    use std::sync::Arc;

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
}
