use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backend::text_extraction::{
    EXTRACTED_TEXT_VERSION_V1, ExtractedTextStatus, TextExtractionStore, extract_text_for_blob,
};
use crate::backend::{ObjectStore, RepoId};
use crate::error::VfsError;
use crate::store::ObjectId;
use crate::store::ObjectKind;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use crate::vcs::CommitId;

const MAX_QUERY_CHARS: usize = 256;
const MAX_RESULT_LIMIT: usize = 1000;
const MAX_INDEXED_CONTENT_CHARS: usize = 100_000;
const MAX_IN_MEMORY_SNIPPET_CHARS: usize = 240;
const MAX_INDEX_TRAVERSAL_ENTRIES: usize = 100_000;

mod acl;
pub use acl::{
    ACL_SNAPSHOT_VERSION_POSIX_TREE_V1, SearchAclAccess, SearchAclFilter, SearchAclPrincipal,
    SearchAclRequirement, SearchAclSnapshot, SearchAclSnapshotBody, acl_filter_allows_path,
    acl_snapshot_allows, build_posix_tree_snapshot, posix_requirement_from_entry,
    posix_root_execute_requirement, search_acl_filter_from_session, verify_acl_snapshot,
};

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
    pub acl_filter: SearchAclFilter,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchIndexResult {
    pub path: String,
    #[serde(skip)]
    pub object_id: Option<ObjectId>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclSnapshotStatus {
    Missing,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractionStatus {
    Missing,
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
pub struct SearchIndexState {
    pub status: SearchIndexStatus,
    pub indexed_file_count: i32,
    pub indexed_byte_count: i64,
    pub failure_code: Option<String>,
    pub acl_snapshot_status: AclSnapshotStatus,
    pub acl_snapshot_version: Option<String>,
    pub extraction_status: ExtractionStatus,
    pub extraction_version: Option<String>,
    pub extraction_failure_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexedFileRow {
    pub path: String,
    pub object_id: ObjectId,
    pub byte_len: usize,
    pub content_preview: String,
    pub acl_snapshot: Option<SearchAclSnapshot>,
    pub extraction_version: String,
    pub extractor: String,
    pub extracted_text_hash: String,
}

#[async_trait]
pub trait SearchIndexStore: Send + Sync {
    async fn ensure_available(&self) -> Result<(), VfsError>;

    async fn index_commit(
        &self,
        head: SearchIndexHead,
        files: Vec<IndexedFileRow>,
    ) -> Result<(), VfsError>;

    async fn search(&self, req: SearchIndexRequest) -> Result<Vec<SearchIndexResult>, VfsError>;

    async fn health_for_head(
        &self,
        head: &SearchIndexHead,
    ) -> Result<Option<SearchIndexState>, VfsError>;

    fn available(&self) -> bool;
}

pub fn search_index_not_ready_error() -> VfsError {
    VfsError::NotSupported {
        message: "search index not ready".to_string(),
    }
}

pub fn search_index_acl_ready(state: &SearchIndexState) -> bool {
    state.status == SearchIndexStatus::Ready
        && state.acl_snapshot_status == AclSnapshotStatus::Ready
        && state
            .acl_snapshot_version
            .as_deref()
            .is_some_and(|version| version == ACL_SNAPSHOT_VERSION_POSIX_TREE_V1)
}

pub fn search_index_extraction_ready(state: &SearchIndexState) -> bool {
    state.status == SearchIndexStatus::Ready
        && state.extraction_status == ExtractionStatus::Ready
        && state
            .extraction_version
            .as_deref()
            .is_some_and(|version| version == EXTRACTED_TEXT_VERSION_V1)
}

pub fn search_index_semantic_ready(state: &SearchIndexState) -> bool {
    search_index_acl_ready(state) && search_index_extraction_ready(state)
}

fn validate_indexed_file_extraction_metadata(file: &IndexedFileRow) -> Result<(), VfsError> {
    if file.extraction_version != EXTRACTED_TEXT_VERSION_V1
        || file.extractor.is_empty()
        || file.extracted_text_hash.len() != 64
        || !file
            .extracted_text_hash
            .chars()
            .all(|ch| ch.is_ascii_hexdigit())
    {
        return Err(search_index_not_ready_error());
    }
    Ok(())
}

pub fn validate_query(query: &str) -> Result<(), VfsError> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err(VfsError::InvalidArgs {
            message: "query cannot be empty".to_string(),
        });
    }
    if trimmed.chars().count() > MAX_QUERY_CHARS {
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
    if limit > MAX_RESULT_LIMIT {
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
    extraction_store: &dyn TextExtractionStore,
    search_store: &dyn SearchIndexStore,
) -> Result<(), VfsError> {
    if repo_id != &head.repo_id {
        return Err(VfsError::InvalidArgs {
            message: "repo id does not match search index head".to_string(),
        });
    }
    extraction_store.ensure_available().await?;
    search_store.ensure_available().await?;

    let mut files = Vec::new();
    let mut extraction_records = Vec::new();
    let mut visited = 0usize;

    struct TraverseFrame {
        dir_path: String,
        entries: Vec<TreeEntry>,
        next: usize,
        ancestors: Vec<SearchAclRequirement>,
    }

    let root_stored = objects
        .get(repo_id, head.root_tree_id, ObjectKind::Tree)
        .await?
        .ok_or_else(|| VfsError::CorruptStore {
            message: "root tree not found".to_string(),
        })?;
    let root_tree =
        TreeObject::deserialize(&root_stored.bytes).map_err(|_| VfsError::CorruptStore {
            message: "failed to deserialize root tree".to_string(),
        })?;
    validate_tree_entries(&root_tree.entries)?;

    let mut stack = vec![TraverseFrame {
        dir_path: "/".to_string(),
        entries: root_tree.entries,
        next: 0,
        ancestors: vec![posix_root_execute_requirement()],
    }];

    while let Some(frame) = stack.last_mut() {
        if frame.next >= frame.entries.len() {
            stack.pop();
            continue;
        }

        let entry = frame.entries[frame.next].clone();
        frame.next += 1;
        let ancestors = frame.ancestors.clone();

        visited += 1;
        if visited > MAX_INDEX_TRAVERSAL_ENTRIES {
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
                let stored = objects
                    .get(repo_id, entry.id, ObjectKind::Blob)
                    .await?
                    .ok_or_else(|| VfsError::CorruptStore {
                        message: "durable search index source blob missing".to_string(),
                    })?;

                let record = extract_text_for_blob(
                    &path,
                    entry.mime_type.as_deref(),
                    entry.id,
                    &stored.bytes,
                );
                extraction_records.push(record.clone());
                if record.status == ExtractedTextStatus::Ready {
                    let Some(text) = record.text.filter(|text| !text.is_empty()) else {
                        continue;
                    };
                    let Some(text_hash) = record.text_hash else {
                        continue;
                    };
                    let file_read =
                        posix_requirement_from_entry(path.clone(), &entry, SearchAclAccess::Read);
                    let acl_snapshot =
                        build_posix_tree_snapshot(head, &path, entry.id, &ancestors, file_read)?;
                    files.push(IndexedFileRow {
                        path,
                        object_id: entry.id,
                        byte_len: stored.bytes.len(),
                        content_preview: truncate_chars(text, MAX_INDEXED_CONTENT_CHARS),
                        acl_snapshot: Some(acl_snapshot),
                        extraction_version: EXTRACTED_TEXT_VERSION_V1.to_string(),
                        extractor: record.extractor,
                        extracted_text_hash: text_hash,
                    });
                }
            }
            TreeEntryKind::Tree => {
                let stored = objects
                    .get(repo_id, entry.id, ObjectKind::Tree)
                    .await?
                    .ok_or_else(|| VfsError::CorruptStore {
                        message: "durable search index source tree missing".to_string(),
                    })?;
                let tree =
                    TreeObject::deserialize(&stored.bytes).map_err(|_| VfsError::CorruptStore {
                        message: "failed to deserialize tree".to_string(),
                    })?;
                validate_tree_entries(&tree.entries)?;
                let mut child_ancestors = ancestors;
                child_ancestors.push(posix_requirement_from_entry(
                    path.clone(),
                    &entry,
                    SearchAclAccess::Execute,
                ));
                stack.push(TraverseFrame {
                    dir_path: path,
                    entries: tree.entries,
                    next: 0,
                    ancestors: child_ancestors,
                });
            }
            TreeEntryKind::Symlink => {}
        }
    }

    extraction_store
        .put_records(head.clone(), extraction_records)
        .await?;
    search_store.index_commit(head.clone(), files).await?;
    Ok(())
}

fn validate_tree_entries(entries: &[TreeEntry]) -> Result<(), VfsError> {
    let mut names = BTreeSet::new();
    for entry in entries {
        if entry.name.is_empty()
            || entry.name == "."
            || entry.name == ".."
            || entry.name.contains('/')
            || entry.name.contains('\0')
            || !names.insert(entry.name.as_str())
        {
            return Err(VfsError::CorruptStore {
                message: "durable search index tree entry is invalid".to_string(),
            });
        }
    }
    Ok(())
}

fn truncate_chars(mut value: String, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value;
    }
    value.truncate(
        value
            .char_indices()
            .nth(max_chars)
            .map(|(index, _)| index)
            .unwrap_or(value.len()),
    );
    value
}

pub(crate) fn path_matches_prefix(path: &str, path_prefix: Option<&str>) -> bool {
    let Some(prefix) = path_prefix else {
        return true;
    };
    if prefix == "/" {
        return true;
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

pub struct UnavailableSearchIndexStore;

#[async_trait]
impl SearchIndexStore for UnavailableSearchIndexStore {
    async fn ensure_available(&self) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "semantic search index is unavailable".to_string(),
        })
    }

    async fn index_commit(
        &self,
        _head: SearchIndexHead,
        _files: Vec<IndexedFileRow>,
    ) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "semantic search index is unavailable".to_string(),
        })
    }

    async fn search(&self, _req: SearchIndexRequest) -> Result<Vec<SearchIndexResult>, VfsError> {
        Err(VfsError::NotSupported {
            message: "semantic search index is unavailable".to_string(),
        })
    }

    async fn health_for_head(
        &self,
        _head: &SearchIndexHead,
    ) -> Result<Option<SearchIndexState>, VfsError> {
        Ok(None)
    }

    fn available(&self) -> bool {
        false
    }
}

type InMemorySearchIndexState = BTreeMap<SearchIndexHead, (SearchIndexState, Vec<IndexedFileRow>)>;

pub struct InMemorySearchIndexStore {
    state: Arc<RwLock<InMemorySearchIndexState>>,
}

impl Default for InMemorySearchIndexStore {
    fn default() -> Self {
        Self::new()
    }
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
    async fn ensure_available(&self) -> Result<(), VfsError> {
        Ok(())
    }

    async fn index_commit(
        &self,
        head: SearchIndexHead,
        files: Vec<IndexedFileRow>,
    ) -> Result<(), VfsError> {
        let mut guard = self.state.write().await;
        let mut byte_count = 0i64;
        for file in &files {
            let Some(snapshot) = &file.acl_snapshot else {
                return Err(search_index_not_ready_error());
            };
            verify_acl_snapshot(&head, &file.path, file.object_id, snapshot)?;
            validate_indexed_file_extraction_metadata(file)?;
            byte_count += file.byte_len as i64;
        }
        let state = SearchIndexState {
            status: SearchIndexStatus::Ready,
            indexed_file_count: files.len() as i32,
            indexed_byte_count: byte_count,
            failure_code: None,
            acl_snapshot_status: AclSnapshotStatus::Ready,
            acl_snapshot_version: Some(ACL_SNAPSHOT_VERSION_POSIX_TREE_V1.to_string()),
            extraction_status: ExtractionStatus::Ready,
            extraction_version: Some(EXTRACTED_TEXT_VERSION_V1.to_string()),
            extraction_failure_code: None,
        };
        guard.insert(head, (state, files));
        Ok(())
    }

    async fn search(&self, req: SearchIndexRequest) -> Result<Vec<SearchIndexResult>, VfsError> {
        validate_query(&req.query)?;
        validate_limit(req.limit)?;
        let guard = self.state.read().await;
        let key = SearchIndexHead {
            repo_id: req.repo_id.clone(),
            commit_id: req.commit_id,
            root_tree_id: req.root_tree_id,
        };
        let Some((state, files)) = guard.get(&key) else {
            return Err(VfsError::NotFound {
                path: "index_not_found".to_string(),
            });
        };
        if !search_index_semantic_ready(state) {
            return Err(search_index_not_ready_error());
        }

        let mut results = Vec::new();
        let query = req.query.to_lowercase();
        for file in files {
            if !path_matches_prefix(&file.path, req.path_prefix.as_deref()) {
                continue;
            }
            let Some(snapshot) = &file.acl_snapshot else {
                return Err(search_index_not_ready_error());
            };
            verify_acl_snapshot(&key, &file.path, file.object_id, snapshot)?;
            if !acl_snapshot_allows(&req.acl_filter, snapshot, &file.path) {
                continue;
            }
            if file.content_preview.to_lowercase().contains(&query) {
                results.push(SearchIndexResult {
                    path: file.path.clone(),
                    object_id: Some(file.object_id),
                    score: 1.0,
                    snippet: truncate_chars(
                        file.content_preview.clone(),
                        MAX_IN_MEMORY_SNIPPET_CHARS,
                    ),
                    commit: key.commit_id,
                    root_tree: key.root_tree_id,
                });
                if results.len() >= req.limit {
                    break;
                }
            }
        }
        Ok(results)
    }

    async fn health_for_head(
        &self,
        head: &SearchIndexHead,
    ) -> Result<Option<SearchIndexState>, VfsError> {
        let guard = self.state.read().await;
        Ok(guard.get(head).map(|(state, _)| state.clone()))
    }

    fn available(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod extra_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::{DelegateContext, Session, SessionScope};
    use crate::auth::{Gid, Uid};
    use crate::backend::text_extraction::InMemoryTextExtractionStore;
    use crate::backend::{LocalMemoryObjectStore, ObjectWrite, RepoId};
    use crate::store::ObjectId;
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use std::sync::Arc;

    fn root_acl_filter() -> SearchAclFilter {
        search_acl_filter_from_session(&Session::root())
    }

    fn user_acl_filter(uid: Uid, gid: Gid, groups: Vec<Gid>) -> SearchAclFilter {
        search_acl_filter_from_session(&Session::new(uid, gid, groups, format!("user-{uid}")))
    }

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
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: utf8_id,
                kind: crate::store::ObjectKind::Blob,
                bytes: utf8_content,
            })
            .await
            .unwrap();

        let binary_content = vec![0u8, 159u8, 146u8, 150u8];
        let binary_id = ObjectId::from_bytes(&binary_content);
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: binary_id,
                kind: crate::store::ObjectKind::Blob,
                bytes: binary_content,
            })
            .await
            .unwrap();

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
                },
            ],
        };
        let tree_bytes = tree.serialize();
        let tree_id = ObjectId::from_bytes(&tree_bytes);
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: tree_id,
                kind: crate::store::ObjectKind::Tree,
                bytes: tree_bytes,
            })
            .await
            .unwrap();

        let store = InMemorySearchIndexStore::new();
        let extraction = InMemoryTextExtractionStore::new();
        let head = SearchIndexHead {
            repo_id: repo_id.clone(),
            commit_id: crate::vcs::CommitId::from(ObjectId::from_bytes(&[1; 32])),
            root_tree_id: tree_id,
        };

        index_durable_commit(&repo_id, &head, &*objects, &extraction, &store)
            .await
            .unwrap();

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
            acl_filter: root_acl_filter(),
        };
        let results = store.search(req).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "/doc.txt");
        let guard = store.state.read().await;
        let snapshot = guard.get(&head).unwrap().1[0]
            .acl_snapshot
            .as_ref()
            .expect("snapshot");
        assert!(
            snapshot
                .requirements
                .iter()
                .any(|req| req.path == "/" && req.access == SearchAclAccess::Execute)
        );

        let scoped_results = store
            .search(SearchIndexRequest {
                repo_id,
                commit_id: head.commit_id,
                root_tree_id: tree_id,
                query: "test".to_string(),
                path_prefix: Some("/doc".to_string()),
                limit: 10,
                acl_filter: root_acl_filter(),
            })
            .await
            .unwrap();
        assert_eq!(scoped_results.len(), 0);
    }

    #[tokio::test]
    async fn acl_snapshot_requires_root_execute_and_file_read() {
        let objects = Arc::new(LocalMemoryObjectStore::new());
        let repo_id = RepoId::new("acl-snapshot-repo").unwrap();
        let content = b"private alpha document".to_vec();
        let blob_id = ObjectId::from_bytes(&content);
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: blob_id,
                kind: crate::store::ObjectKind::Blob,
                bytes: content,
            })
            .await
            .unwrap();
        let docs_tree = TreeObject {
            entries: vec![TreeEntry {
                name: "secret.txt".to_string(),
                kind: TreeEntryKind::Blob,
                id: blob_id,
                mode: 0o600,
                uid: 1000,
                gid: 1000,
                mime_type: None,
                custom_attrs: Default::default(),
            }],
        };
        let docs_tree_bytes = docs_tree.serialize();
        let docs_tree_id = ObjectId::from_bytes(&docs_tree_bytes);
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: docs_tree_id,
                kind: crate::store::ObjectKind::Tree,
                bytes: docs_tree_bytes,
            })
            .await
            .unwrap();
        let root = TreeObject {
            entries: vec![TreeEntry {
                name: "docs".to_string(),
                kind: TreeEntryKind::Tree,
                id: docs_tree_id,
                mode: 0o755,
                uid: 1000,
                gid: 1000,
                mime_type: None,
                custom_attrs: Default::default(),
            }],
        };
        let root_bytes = root.serialize();
        let root_tree_id = ObjectId::from_bytes(&root_bytes);
        objects
            .put(ObjectWrite {
                repo_id: repo_id.clone(),
                id: root_tree_id,
                kind: crate::store::ObjectKind::Tree,
                bytes: root_bytes,
            })
            .await
            .unwrap();
        let store = InMemorySearchIndexStore::new();
        let extraction = InMemoryTextExtractionStore::new();
        let head = SearchIndexHead {
            repo_id: repo_id.clone(),
            commit_id: CommitId::from(ObjectId::from_bytes(&[4; 32])),
            root_tree_id,
        };
        index_durable_commit(&repo_id, &head, &*objects, &extraction, &store)
            .await
            .unwrap();
        let guard = store.state.read().await;
        let snapshot = guard.get(&head).unwrap().1[0]
            .acl_snapshot
            .as_ref()
            .expect("snapshot");
        assert!(snapshot.requirements.iter().any(|req| req.path == "/"));
        assert!(
            snapshot
                .requirements
                .iter()
                .any(|req| req.path == "/docs" && req.access == SearchAclAccess::Execute)
        );
        assert!(
            snapshot
                .requirements
                .iter()
                .any(|req| req.path == "/docs/secret.txt" && req.access == SearchAclAccess::Read)
        );
    }

    #[tokio::test]
    async fn acl_snapshot_hash_changes_with_path_or_mode() {
        let head = SearchIndexHead {
            repo_id: RepoId::new("hash-repo").unwrap(),
            commit_id: CommitId::from(ObjectId::from_bytes(&[5; 32])),
            root_tree_id: ObjectId::from_bytes(&[6; 32]),
        };
        let ancestors = vec![posix_root_execute_requirement()];
        let read_a = posix_requirement_from_entry(
            "/a.txt".to_string(),
            &TreeEntry {
                name: "a.txt".to_string(),
                kind: TreeEntryKind::Blob,
                id: ObjectId::from_bytes(b"a"),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                mime_type: None,
                custom_attrs: Default::default(),
            },
            SearchAclAccess::Read,
        );
        let read_b = posix_requirement_from_entry(
            "/b.txt".to_string(),
            &TreeEntry {
                name: "b.txt".to_string(),
                kind: TreeEntryKind::Blob,
                id: ObjectId::from_bytes(b"b"),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                mime_type: None,
                custom_attrs: Default::default(),
            },
            SearchAclAccess::Read,
        );
        let snap_a = build_posix_tree_snapshot(
            &head,
            "/a.txt",
            ObjectId::from_bytes(b"a"),
            &ancestors,
            read_a,
        )
        .unwrap();
        let snap_b = build_posix_tree_snapshot(
            &head,
            "/b.txt",
            ObjectId::from_bytes(b"b"),
            &ancestors,
            read_b,
        )
        .unwrap();
        assert_ne!(snap_a.hash, snap_b.hash);
    }

    #[tokio::test]
    async fn missing_acl_snapshot_fails_search_closed() {
        let store = InMemorySearchIndexStore::new();
        let head = SearchIndexHead {
            repo_id: RepoId::new("missing-snapshot").unwrap(),
            commit_id: CommitId::from(ObjectId::from_bytes(&[7; 32])),
            root_tree_id: ObjectId::from_bytes(&[8; 32]),
        };
        store
            .index_commit(
                head.clone(),
                vec![IndexedFileRow {
                    path: "/legacy.txt".to_string(),
                    object_id: ObjectId::from_bytes(b"legacy"),
                    byte_len: 3,
                    content_preview: "old".to_string(),
                    acl_snapshot: None,
                    extraction_version: EXTRACTED_TEXT_VERSION_V1.to_string(),
                    extractor: "plain-text-v1".to_string(),
                    extracted_text_hash: "0".repeat(64),
                }],
            )
            .await
            .expect_err("rows without snapshots must fail indexing");
    }

    #[tokio::test]
    async fn acl_filter_denies_user_without_read_bits() {
        let store = InMemorySearchIndexStore::new();
        let head = SearchIndexHead {
            repo_id: RepoId::new("deny-user").unwrap(),
            commit_id: CommitId::from(ObjectId::from_bytes(&[9; 32])),
            root_tree_id: ObjectId::from_bytes(&[10; 32]),
        };
        let snapshot = build_posix_tree_snapshot(
            &head,
            "/owned.txt",
            ObjectId::from_bytes(b"owned"),
            &[posix_root_execute_requirement()],
            posix_requirement_from_entry(
                "/owned.txt".to_string(),
                &TreeEntry {
                    name: "owned.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: ObjectId::from_bytes(b"owned"),
                    mode: 0o600,
                    uid: 1000,
                    gid: 1000,
                    mime_type: None,
                    custom_attrs: Default::default(),
                },
                SearchAclAccess::Read,
            ),
        )
        .unwrap();
        store
            .index_commit(
                head.clone(),
                vec![IndexedFileRow {
                    path: "/owned.txt".to_string(),
                    object_id: ObjectId::from_bytes(b"owned"),
                    byte_len: 5,
                    content_preview: "hello".to_string(),
                    acl_snapshot: Some(snapshot),
                    extraction_version: EXTRACTED_TEXT_VERSION_V1.to_string(),
                    extractor: "plain-text-v1".to_string(),
                    extracted_text_hash: "0".repeat(64),
                }],
            )
            .await
            .unwrap();
        let allowed = store
            .search(SearchIndexRequest {
                repo_id: head.repo_id.clone(),
                commit_id: head.commit_id,
                root_tree_id: head.root_tree_id,
                query: "hello".to_string(),
                path_prefix: None,
                limit: 10,
                acl_filter: user_acl_filter(1000, 1000, vec![1000]),
            })
            .await
            .unwrap();
        assert_eq!(allowed.len(), 1);
        let denied = store
            .search(SearchIndexRequest {
                repo_id: head.repo_id.clone(),
                commit_id: head.commit_id,
                root_tree_id: head.root_tree_id,
                query: "hello".to_string(),
                path_prefix: None,
                limit: 10,
                acl_filter: user_acl_filter(2000, 2000, vec![2000]),
            })
            .await
            .unwrap();
        assert!(denied.is_empty());
    }

    #[tokio::test]
    async fn delegate_intersection_requires_both_principals() {
        let head = SearchIndexHead {
            repo_id: RepoId::new("delegate-repo").unwrap(),
            commit_id: CommitId::from(ObjectId::from_bytes(&[11; 32])),
            root_tree_id: ObjectId::from_bytes(&[12; 32]),
        };
        let snapshot = build_posix_tree_snapshot(
            &head,
            "/shared.txt",
            ObjectId::from_bytes(b"shared"),
            &[posix_root_execute_requirement()],
            posix_requirement_from_entry(
                "/shared.txt".to_string(),
                &TreeEntry {
                    name: "shared.txt".to_string(),
                    kind: TreeEntryKind::Blob,
                    id: ObjectId::from_bytes(b"shared"),
                    mode: 0o640,
                    uid: 1000,
                    gid: 1000,
                    mime_type: None,
                    custom_attrs: Default::default(),
                },
                SearchAclAccess::Read,
            ),
        )
        .unwrap();
        let mut session = Session::new(1000, 1000, vec![1000], "owner".to_string());
        session.delegate = Some(DelegateContext {
            uid: 2000,
            gid: 2000,
            groups: vec![1000, 2000],
            username: "delegate".to_string(),
        });
        let filter = search_acl_filter_from_session(&session);
        assert!(acl_snapshot_allows(&filter, &snapshot, "/shared.txt"));
        session.delegate = Some(DelegateContext {
            uid: 3000,
            gid: 3000,
            groups: vec![3000],
            username: "blocked".to_string(),
        });
        let blocked = search_acl_filter_from_session(&session);
        assert!(!acl_snapshot_allows(&blocked, &snapshot, "/shared.txt"));
    }

    #[tokio::test]
    async fn read_prefixes_are_segment_safe() {
        assert!(acl_filter_allows_path(
            "/docs/file.txt",
            &SearchAclFilter {
                principal: SearchAclPrincipal {
                    uid: 1000,
                    gid: 1000,
                    groups: vec![1000],
                },
                delegate: None,
                read_prefixes: vec!["/docs".to_string()],
            },
        ));
        assert!(!acl_filter_allows_path(
            "/docs-extra/file.txt",
            &SearchAclFilter {
                principal: SearchAclPrincipal {
                    uid: 1000,
                    gid: 1000,
                    groups: vec![1000],
                },
                delegate: None,
                read_prefixes: vec!["/doc".to_string()],
            },
        ));
        let scoped = Session::new(1000, 1000, vec![1000], "scoped".to_string())
            .with_scope(SessionScope::new(["/docs/public"], ["/docs/public"]).unwrap());
        let filter = search_acl_filter_from_session(&scoped);
        assert!(acl_filter_allows_path("/docs/public/allowed.txt", &filter));
        assert!(!acl_filter_allows_path("/docs/private.txt", &filter));
    }
}
