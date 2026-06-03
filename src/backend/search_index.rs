use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backend::{RepoId, ObjectStore};
use crate::store::ObjectId;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::store::ObjectKind;
use crate::vcs::CommitId;
use crate::error::VfsError;

mod commit_id_serde {
    use super::CommitId;
    use crate::store::ObjectId;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(commit_id: &CommitId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&commit_id.to_hex())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<CommitId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let obj_id = ObjectId::from_hex(&s).map_err(serde::de::Error::custom)?;
        Ok(CommitId::from(obj_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchIndexHead {
    pub repo_id: RepoId,
    pub commit_id: CommitId,
    pub root_tree_id: ObjectId,
}

#[derive(Debug, Clone)]
pub struct SearchIndexRequest {
    pub repo_id: RepoId,
    pub commit_id: CommitId,
    pub root_tree_id: ObjectId,
    pub query: String,
    pub path_prefix: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchIndexResult {
    pub path: String,
    pub score: f64,
    pub snippet: String,
    #[serde(with = "commit_id_serde")]
    pub commit: CommitId,
    pub root_tree: ObjectId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexStatus {
    Indexing,
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
pub struct SearchIndexState {
    pub status: SearchIndexStatus,
    pub indexed_file_count: i32,
    pub indexed_byte_count: i64,
    pub failure_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexedFileRow {
    pub path: String,
    pub object_id: ObjectId,
    pub byte_len: usize,
    pub content_preview: String,
}

#[async_trait]
pub trait SearchIndexStore: Send + Sync {
    async fn index_commit(
        &self,
        head: SearchIndexHead,
        files: Vec<IndexedFileRow>,
    ) -> Result<(), VfsError>;

    async fn search(
        &self,
        req: SearchIndexRequest,
    ) -> Result<Vec<SearchIndexResult>, VfsError>;

    async fn health_for_head(
        &self,
        head: &SearchIndexHead,
    ) -> Result<Option<SearchIndexState>, VfsError>;

    fn available(&self) -> bool;
}

pub fn validate_query(query: &str) -> Result<(), VfsError> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err(VfsError::InvalidArgs {
            message: "query cannot be empty".to_string(),
        });
    }
    if trimmed.len() > 256 {
        return Err(VfsError::InvalidArgs {
            message: "query cannot exceed 256 characters".to_string(),
        });
    }
    Ok(())
}

pub fn validate_limit(limit: usize) -> Result<(), VfsError> {
    if limit == 0 {
        return Err(VfsError::InvalidArgs {
            message: "limit must be at least 1".to_string(),
        });
    }
    if limit > 1000 {
        return Err(VfsError::InvalidArgs {
            message: "limit cannot exceed 1000".to_string(),
        });
    }
    Ok(())
}

pub async fn index_durable_commit(
    repo_id: &RepoId,
    head: &SearchIndexHead,
    objects: &dyn ObjectStore,
    store: &dyn SearchIndexStore,
) -> Result<(), VfsError> {
    if !store.available() {
        return Err(VfsError::NotSupported {
            message: "search index store is unavailable".to_string(),
        });
    }

    let mut files = Vec::new();
    let mut visited = 0usize;

    struct TraverseFrame {
        dir_path: String,
        entries: Vec<TreeEntry>,
        next: usize,
    }

    let root_stored = objects.get(repo_id, head.root_tree_id, ObjectKind::Tree).await?
        .ok_or_else(|| VfsError::CorruptStore {
            message: "root tree not found".to_string(),
        })?;
    let root_tree = TreeObject::deserialize(&root_stored.bytes).map_err(|_| VfsError::CorruptStore {
        message: "failed to deserialize root tree".to_string(),
    })?;

    let mut stack = vec![TraverseFrame {
        dir_path: "/".to_string(),
        entries: root_tree.entries,
        next: 0,
    }];

    while let Some(frame) = stack.last_mut() {
        if frame.next >= frame.entries.len() {
            stack.pop();
            continue;
        }

        let entry = frame.entries[frame.next].clone();
        frame.next += 1;

        visited += 1;
        if visited > 100_000 {
            return Err(VfsError::CorruptStore {
                message: "traversal limit exceeded".to_string(),
            });
        }

        let path = if frame.dir_path == "/" {
            format!("/{}", entry.name)
        } else {
            format!("{}/{}", frame.dir_path, entry.name)
        };

        match entry.kind {
            TreeEntryKind::Blob => {
                let stored = objects.get(repo_id, entry.id, ObjectKind::Blob).await?
                    .ok_or_else(|| VfsError::CorruptStore {
                        message: format!("blob not found: {}", entry.id),
                    })?;
                
                if let Ok(content) = String::from_utf8(stored.bytes.clone()) {
                    let content_preview = if content.len() > 100_000 {
                        content[..100_000].to_string()
                    } else {
                        content
                    };
                    files.push(IndexedFileRow {
                        path,
                        object_id: entry.id,
                        byte_len: stored.bytes.len(),
                        content_preview,
                    });
                }
            }
            TreeEntryKind::Tree => {
                let stored = objects.get(repo_id, entry.id, ObjectKind::Tree).await?
                    .ok_or_else(|| VfsError::CorruptStore {
                        message: format!("tree not found: {}", entry.id),
                    })?;
                let tree = TreeObject::deserialize(&stored.bytes).map_err(|_| VfsError::CorruptStore {
                    message: "failed to deserialize tree".to_string(),
                })?;
                stack.push(TraverseFrame {
                    dir_path: path,
                    entries: tree.entries,
                    next: 0,
                });
            }
            TreeEntryKind::Symlink => {}
        }
    }

    store.index_commit(head.clone(), files).await?;
    Ok(())
}

pub struct UnavailableSearchIndexStore;

#[async_trait]
impl SearchIndexStore for UnavailableSearchIndexStore {
    async fn index_commit(&self, _head: SearchIndexHead, _files: Vec<IndexedFileRow>) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "semantic search index is unavailable".to_string(),
        })
    }

    async fn search(&self, _req: SearchIndexRequest) -> Result<Vec<SearchIndexResult>, VfsError> {
        Err(VfsError::NotSupported {
            message: "semantic search index is unavailable".to_string(),
        })
    }

    async fn health_for_head(&self, _head: &SearchIndexHead) -> Result<Option<SearchIndexState>, VfsError> {
        Ok(None)
    }

    fn available(&self) -> bool {
        false
    }
}

pub struct InMemorySearchIndexStore {
    state: Arc<RwLock<BTreeMap<SearchIndexHead, (SearchIndexState, Vec<IndexedFileRow>)>>>,
}

impl InMemorySearchIndexStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl SearchIndexStore for InMemorySearchIndexStore {
    async fn index_commit(&self, head: SearchIndexHead, files: Vec<IndexedFileRow>) -> Result<(), VfsError> {
        let mut guard = self.state.write().await;
        let mut byte_count = 0i64;
        for file in &files {
            byte_count += file.byte_len as i64;
        }
        let state = SearchIndexState {
            status: SearchIndexStatus::Ready,
            indexed_file_count: files.len() as i32,
            indexed_byte_count: byte_count,
            failure_code: None,
        };
        guard.insert(head, (state, files));
        Ok(())
    }

    async fn search(&self, req: SearchIndexRequest) -> Result<Vec<SearchIndexResult>, VfsError> {
        let guard = self.state.read().await;
        let key = SearchIndexHead {
            repo_id: req.repo_id,
            commit_id: req.commit_id,
            root_tree_id: req.root_tree_id,
        };
        let Some((state, files)) = guard.get(&key) else {
            return Err(VfsError::NotFound {
                path: "index_not_found".to_string(),
            });
        };
        if state.status != SearchIndexStatus::Ready {
            return Err(VfsError::NotSupported {
                message: "index not ready".to_string(),
            });
        }
        
        let mut results = Vec::new();
        for file in files {
            if file.content_preview.contains(&req.query) {
                results.push(SearchIndexResult {
                    path: file.path.clone(),
                    score: 1.0,
                    snippet: file.content_preview.clone(),
                    commit: key.commit_id,
                    root_tree: key.root_tree_id,
                });
            }
        }
        results.truncate(req.limit);
        Ok(results)
    }

    async fn health_for_head(&self, head: &SearchIndexHead) -> Result<Option<SearchIndexState>, VfsError> {
        let guard = self.state.read().await;
        Ok(guard.get(head).map(|(state, _)| state.clone()))
    }

    fn available(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{RepoId, LocalMemoryObjectStore, ObjectWrite};
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::ObjectId;
    use std::sync::Arc;

    #[tokio::test]
    async fn search_index_domain_validation_and_traversal() {
        assert!(validate_query("").is_err());
        assert!(validate_query("   ").is_err());
        assert!(validate_query(&"a".repeat(300)).is_err());
        
        assert!(validate_limit(0).is_err());
        assert!(validate_limit(10000).is_err());
        
        let objects = Arc::new(LocalMemoryObjectStore::new());
        let repo_id = RepoId::new("test-repo").unwrap();
        
        let utf8_content = b"hello world this is a test document".to_vec();
        let utf8_id = ObjectId::from_bytes(&utf8_content);
        objects.put(ObjectWrite {
            repo_id: repo_id.clone(),
            id: utf8_id,
            kind: crate::store::ObjectKind::Blob,
            bytes: utf8_content,
        }).await.unwrap();

        let binary_content = vec![0u8, 159u8, 146u8, 150u8];
        let binary_id = ObjectId::from_bytes(&binary_content);
        objects.put(ObjectWrite {
            repo_id: repo_id.clone(),
            id: binary_id,
            kind: crate::store::ObjectKind::Blob,
            bytes: binary_content,
        }).await.unwrap();
        
        let tree = TreeObject {
            entries: vec![
                TreeEntry {
                    name: "doc.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: utf8_id,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    mime_type: None,
                    custom_attrs: Default::default(),
                },
                TreeEntry {
                    name: "bin.dat".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: binary_id,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    mime_type: None,
                    custom_attrs: Default::default(),
                }
            ]
        };
        let tree_bytes = tree.serialize();
        let tree_id = ObjectId::from_bytes(&tree_bytes);
        objects.put(ObjectWrite {
            repo_id: repo_id.clone(),
            id: tree_id,
            kind: crate::store::ObjectKind::Tree,
            bytes: tree_bytes,
        }).await.unwrap();

        let store = InMemorySearchIndexStore::new();
        let head = SearchIndexHead {
            repo_id: repo_id.clone(),
            commit_id: crate::vcs::CommitId::from(ObjectId::from_bytes(&[1; 32])),
            root_tree_id: tree_id,
        };
        
        index_durable_commit(&repo_id, &head, &*objects, &store).await.unwrap();
        
        let state = store.health_for_head(&head).await.unwrap().unwrap();
        assert_eq!(state.status, SearchIndexStatus::Ready);
        assert_eq!(state.indexed_file_count, 1);
        
        let req = SearchIndexRequest {
            repo_id: repo_id.clone(),
            commit_id: head.commit_id,
            root_tree_id: tree_id,
            query: "test".to_string(),
            path_prefix: None,
            limit: 10,
        };
        let results = store.search(req).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "/doc.txt");
    }
}
