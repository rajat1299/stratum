use super::*;
use crate::auth::session::Session;
use crate::auth::{Gid, Uid};
use crate::backend::embedding::{
    DeterministicEmbeddingProvider, DocumentEmbedding, EmbeddingChunkInput, EmbeddingModelConfig,
    EmbeddingProvider, FailingEmbeddingProvider, QueryEmbedding, UnavailableEmbeddingProvider,
};
use crate::backend::text_extraction::{ExtractedTextStatus, InMemoryTextExtractionStore};
use crate::backend::{LocalMemoryObjectStore, ObjectWrite, RepoId};
use crate::error::VfsError;
use crate::store::ObjectId;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use std::sync::Arc;

const VECTOR_TEST_DIMS: usize = 64;

struct MismatchedEmbeddingProvider {
    inner: DeterministicEmbeddingProvider,
}

#[async_trait::async_trait]
impl EmbeddingProvider for MismatchedEmbeddingProvider {
    fn config(&self) -> Option<EmbeddingModelConfig> {
        self.inner.config()
    }

    fn available(&self) -> bool {
        true
    }

    async fn embed_query(&self, query: &str) -> Result<QueryEmbedding, VfsError> {
        self.inner.embed_query(query).await
    }

    async fn embed_documents(
        &self,
        chunks: Vec<EmbeddingChunkInput>,
    ) -> Result<Vec<DocumentEmbedding>, VfsError> {
        let mut embeddings = self.inner.embed_documents(chunks).await?;
        if let Some(first) = embeddings.first_mut() {
            first.chunk_hash = "0".repeat(64);
        }
        Ok(embeddings)
    }
}

struct FileSpec {
    name: &'static str,
    content: &'static str,
    mode: u16,
    uid: Uid,
    gid: Gid,
}

fn file_spec(name: &'static str, content: &'static str) -> FileSpec {
    FileSpec {
        name,
        content,
        mode: 0o644,
        uid: 0,
        gid: 0,
    }
}

/// Builds a single-level repo tree of text files and returns the object store,
/// repo id, and the durable search head.
async fn build_repo(
    repo_name: &str,
    files: &[FileSpec],
) -> (Arc<LocalMemoryObjectStore>, RepoId, SearchIndexHead) {
    let objects = Arc::new(LocalMemoryObjectStore::new());
    let repo_id = RepoId::new(repo_name).unwrap();
    let mut entries = Vec::new();
    for spec in files {
        let content = spec.content.as_bytes().to_vec();
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
        entries.push(TreeEntry {
            name: spec.name.to_string(),
            kind: TreeEntryKind::Blob,
            id: blob_id,
            mode: spec.mode,
            uid: spec.uid,
            gid: spec.gid,
            mime_type: Some("text/plain".to_string()),
            custom_attrs: Default::default(),
        });
    }
    let tree = TreeObject { entries };
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
    let head = SearchIndexHead {
        repo_id: repo_id.clone(),
        commit_id: crate::vcs::CommitId::from(ObjectId::from_bytes(repo_name.as_bytes())),
        root_tree_id: tree_id,
    };
    (objects, repo_id, head)
}

fn root_filter() -> SearchAclFilter {
    search_acl_filter_from_session(&Session::root())
}

fn user_filter(uid: Uid, gid: Gid) -> SearchAclFilter {
    search_acl_filter_from_session(&Session::new(uid, gid, Vec::new(), format!("user-{uid}")))
}

async fn vector_request(
    head: &SearchIndexHead,
    provider: &DeterministicEmbeddingProvider,
    query: &str,
    limit: usize,
    filter: SearchAclFilter,
    path_prefix: Option<String>,
) -> VectorSearchIndexRequest {
    let query_embedding = provider.embed_query(query).await.expect("embed query");
    VectorSearchIndexRequest {
        repo_id: head.repo_id.clone(),
        commit_id: head.commit_id,
        root_tree_id: head.root_tree_id,
        query: query.to_string(),
        path_prefix,
        limit,
        acl_filter: filter,
        query_embedding,
    }
}

#[tokio::test]
async fn vector_ready_head_returns_vector_ranked_results() {
    let (objects, repo_id, head) = build_repo(
        "vec-ready",
        &[
            file_spec("alpha.txt", "alpha alpha alpha keyword"),
            file_spec("beta.txt", "completely unrelated gamma delta"),
        ],
    )
    .await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    let model = provider.config().unwrap();
    let health = store
        .vector_health_for_head(&head, &model)
        .await
        .unwrap()
        .expect("vector state present");
    assert_eq!(health.status, VectorIndexStatus::Ready);
    assert_eq!(health.embedded_file_count, 2);

    let req = vector_request(
        &head,
        &provider,
        "alpha alpha alpha keyword",
        10,
        root_filter(),
        None,
    )
    .await;
    let results = store.vector_search(req).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].path, "/alpha.txt", "closest vector ranks first");
}

#[tokio::test]
async fn vector_missing_head_falls_back_to_fts() {
    let (objects, repo_id, head) = build_repo(
        "vec-missing",
        &[file_spec("doc.txt", "searchable content here")],
    )
    .await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    // No provider: FTS is indexed, vectors are missing.
    index_durable_commit(&repo_id, &head, &*objects, &extraction, &store)
        .await
        .unwrap();

    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    let req = vector_request(&head, &provider, "searchable", 10, root_filter(), None).await;
    let err = store.vector_search(req).await.unwrap_err();
    assert!(matches!(err, VfsError::NotSupported { .. }));

    // FTS search still works.
    let fts = store
        .search(SearchIndexRequest {
            repo_id: repo_id.clone(),
            commit_id: head.commit_id,
            root_tree_id: head.root_tree_id,
            query: "searchable".to_string(),
            path_prefix: None,
            limit: 10,
            acl_filter: root_filter(),
        })
        .await
        .unwrap();
    assert_eq!(fts.len(), 1);
}

#[tokio::test]
async fn provider_disabled_head_still_returns_fts() {
    let (objects, repo_id, head) =
        build_repo("vec-disabled", &[file_spec("doc.txt", "indexable words")]).await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let disabled = UnavailableEmbeddingProvider;
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&disabled),
    )
    .await
    .unwrap();

    // FTS ready, vector missing.
    let state = store.health_for_head(&head).await.unwrap().unwrap();
    assert_eq!(state.status, SearchIndexStatus::Ready);
    let fts = store
        .search(SearchIndexRequest {
            repo_id,
            commit_id: head.commit_id,
            root_tree_id: head.root_tree_id,
            query: "indexable".to_string(),
            path_prefix: None,
            limit: 10,
            acl_filter: root_filter(),
        })
        .await
        .unwrap();
    assert_eq!(fts.len(), 1);
}

#[tokio::test]
async fn provider_failed_head_still_returns_fts_without_leaks() {
    let (objects, repo_id, head) =
        build_repo("vec-failed", &[file_spec("doc.txt", "indexable words")]).await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let failing = FailingEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    // Embedding failure must not fail the indexing call: FTS stays ready.
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&failing),
    )
    .await
    .unwrap();

    let state = store.health_for_head(&head).await.unwrap().unwrap();
    assert_eq!(state.status, SearchIndexStatus::Ready);

    let model = failing.config().unwrap();
    let health = store
        .vector_health_for_head(&head, &model)
        .await
        .unwrap()
        .expect("failed vector state present");
    assert_eq!(health.status, VectorIndexStatus::Failed);
    assert_eq!(health.failure_code.as_deref(), Some("vector_index_failed"));

    let provider = DeterministicEmbeddingProvider::new(model);
    let req = vector_request(&head, &provider, "indexable", 10, root_filter(), None).await;
    let err = store.vector_search(req).await.unwrap_err();
    let rendered = err.to_string();
    assert!(matches!(err, VfsError::NotSupported { .. }));
    for leak in ["failing-fixture", "endpoint", "api_key", "http"] {
        assert!(!rendered.contains(leak), "leaked provider detail: {leak}");
    }

    let fts = store
        .search(SearchIndexRequest {
            repo_id,
            commit_id: head.commit_id,
            root_tree_id: head.root_tree_id,
            query: "indexable".to_string(),
            path_prefix: None,
            limit: 10,
            acl_filter: root_filter(),
        })
        .await
        .unwrap();
    assert_eq!(fts.len(), 1);
}

#[tokio::test]
async fn malformed_provider_output_marks_vector_failed_and_keeps_fts_ready() {
    let (objects, repo_id, head) =
        build_repo("vec-malformed", &[file_spec("doc.txt", "indexable words")]).await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let model = EmbeddingModelConfig {
        provider: "malformed-fixture".to_string(),
        model: "malformed-fixture-v1".to_string(),
        dimensions: VECTOR_TEST_DIMS,
        retention_policy: "head-scoped".to_string(),
    };
    let provider = MismatchedEmbeddingProvider {
        inner: DeterministicEmbeddingProvider::new(model.clone()),
    };

    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    let health = store
        .vector_health_for_head(&head, &model)
        .await
        .unwrap()
        .expect("failed vector state present");
    assert_eq!(health.status, VectorIndexStatus::Failed);
    assert_eq!(health.failure_code.as_deref(), Some("vector_index_failed"));

    let fts = store
        .search(SearchIndexRequest {
            repo_id,
            commit_id: head.commit_id,
            root_tree_id: head.root_tree_id,
            query: "indexable".to_string(),
            path_prefix: None,
            limit: 10,
            acl_filter: root_filter(),
        })
        .await
        .unwrap();
    assert_eq!(fts.len(), 1);
}

#[tokio::test]
async fn same_model_name_with_different_provider_keeps_separate_vector_state() {
    let (objects, repo_id, head) =
        build_repo("vec-provider-key", &[file_spec("doc.txt", "alpha beta")]).await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider_a = DeterministicEmbeddingProvider::new(EmbeddingModelConfig {
        provider: "provider-a".to_string(),
        model: "shared-model".to_string(),
        dimensions: VECTOR_TEST_DIMS,
        retention_policy: "head-scoped".to_string(),
    });
    let provider_b = DeterministicEmbeddingProvider::new(EmbeddingModelConfig {
        provider: "provider-b".to_string(),
        model: "shared-model".to_string(),
        dimensions: VECTOR_TEST_DIMS,
        retention_policy: "head-scoped".to_string(),
    });

    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider_a),
    )
    .await
    .unwrap();
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider_b),
    )
    .await
    .unwrap();

    let model_a = provider_a.config().unwrap();
    let model_b = provider_b.config().unwrap();
    let health_a = store
        .vector_health_for_head(&head, &model_a)
        .await
        .unwrap()
        .expect("provider a state present");
    let health_b = store
        .vector_health_for_head(&head, &model_b)
        .await
        .unwrap()
        .expect("provider b state present");
    assert_eq!(health_a.embedding_provider.as_deref(), Some("provider-a"));
    assert_eq!(health_b.embedding_provider.as_deref(), Some("provider-b"));

    let req_a = vector_request(&head, &provider_a, "alpha", 10, root_filter(), None).await;
    let req_b = vector_request(&head, &provider_b, "alpha", 10, root_filter(), None).await;
    assert_eq!(store.vector_search(req_a).await.unwrap().len(), 1);
    assert_eq!(store.vector_search(req_b).await.unwrap().len(), 1);
}

#[tokio::test]
async fn empty_ready_vector_index_falls_back_when_fts_has_files() {
    let (objects, repo_id, head) = build_repo(
        "vec-empty-ready",
        &[file_spec("doc.txt", "searchable text")],
    )
    .await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    index_durable_commit(&repo_id, &head, &*objects, &extraction, &store)
        .await
        .unwrap();

    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    let model = provider.config().unwrap();
    store
        .index_vectors(head.clone(), model.clone(), Vec::new())
        .await
        .unwrap();

    let health = store
        .vector_health_for_head(&head, &model)
        .await
        .unwrap()
        .expect("empty vector state present");
    assert_eq!(health.status, VectorIndexStatus::Ready);
    assert_eq!(health.embedded_chunk_count, 0);

    let req = vector_request(&head, &provider, "searchable", 10, root_filter(), None).await;
    assert!(matches!(
        store.vector_search(req).await,
        Err(VfsError::NotSupported { .. })
    ));
    let fts = store
        .search(SearchIndexRequest {
            repo_id,
            commit_id: head.commit_id,
            root_tree_id: head.root_tree_id,
            query: "searchable".to_string(),
            path_prefix: None,
            limit: 10,
            acl_filter: root_filter(),
        })
        .await
        .unwrap();
    assert_eq!(fts.len(), 1);
}

#[tokio::test]
async fn missing_acl_snapshot_on_vector_row_fails_closed() {
    let (objects, repo_id, head) =
        build_repo("vec-no-acl", &[file_spec("doc.txt", "content alpha")]).await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    // Corrupt the FTS row's ACL snapshot to None after indexing.
    {
        let mut guard = store.state.write().await;
        let (_, files) = guard.get_mut(&head).unwrap();
        files[0].acl_snapshot = None;
    }

    let req = vector_request(&head, &provider, "content alpha", 10, root_filter(), None).await;
    let err = store.vector_search(req).await.unwrap_err();
    assert!(matches!(err, VfsError::NotSupported { .. }));
}

#[tokio::test]
async fn unsupported_extraction_produces_no_vector_chunk() {
    let (objects, repo_id, head) = build_repo(
        "vec-unsupported",
        &[file_spec("readme.txt", "real text content")],
    )
    .await;
    // Add a binary blob with an unsupported mime so extraction yields no text.
    let binary = vec![0u8, 159u8, 146u8, 150u8];
    let binary_id = ObjectId::from_bytes(&binary);
    objects
        .put(ObjectWrite {
            repo_id: repo_id.clone(),
            id: binary_id,
            kind: crate::store::ObjectKind::Blob,
            bytes: binary,
        })
        .await
        .unwrap();
    // Rebuild a tree that includes the binary entry next to the text file.
    let text = "real text content".as_bytes().to_vec();
    let text_id = ObjectId::from_bytes(&text);
    let tree = TreeObject {
        entries: vec![
            TreeEntry {
                name: "readme.txt".to_string(),
                kind: TreeEntryKind::Blob,
                id: text_id,
                mode: 0o644,
                uid: 0,
                gid: 0,
                mime_type: Some("text/plain".to_string()),
                custom_attrs: Default::default(),
            },
            TreeEntry {
                name: "blob.bin".to_string(),
                kind: TreeEntryKind::Blob,
                id: binary_id,
                mode: 0o644,
                uid: 0,
                gid: 0,
                mime_type: Some("application/octet-stream".to_string()),
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
    let head = SearchIndexHead {
        repo_id: repo_id.clone(),
        commit_id: head.commit_id,
        root_tree_id: tree_id,
    };

    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    let model = provider.config().unwrap();
    let health = store
        .vector_health_for_head(&head, &model)
        .await
        .unwrap()
        .expect("vector state present");
    // Only the text file produced a chunk; the unsupported binary did not.
    assert_eq!(health.embedded_file_count, 1);
    assert_eq!(health.embedded_chunk_count, 1);
}

#[tokio::test]
async fn vector_path_prefix_is_segment_safe() {
    let (objects, repo_id, head) = build_repo(
        "vec-prefix",
        &[
            file_spec("docs", "alpha content one"),
            file_spec("docs2.txt", "alpha content two"),
        ],
    )
    .await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    // Prefix "/docs" must not match "/docs2.txt" (segment-safe).
    let req = vector_request(
        &head,
        &provider,
        "alpha content",
        10,
        root_filter(),
        Some("/docs".to_string()),
    )
    .await;
    let results = store.vector_search(req).await.unwrap();
    assert!(results.iter().all(|r| r.path == "/docs"));
}

#[tokio::test]
async fn vector_query_and_limit_validation_unchanged() {
    let (objects, repo_id, head) =
        build_repo("vec-validate", &[file_spec("doc.txt", "alpha content")]).await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    let mut empty = vector_request(&head, &provider, "alpha", 10, root_filter(), None).await;
    empty.query = "   ".to_string();
    assert!(matches!(
        store.vector_search(empty).await,
        Err(VfsError::InvalidArgs { .. })
    ));

    let mut bad_limit = vector_request(&head, &provider, "alpha", 10, root_filter(), None).await;
    bad_limit.limit = 0;
    assert!(matches!(
        store.vector_search(bad_limit).await,
        Err(VfsError::InvalidArgs { .. })
    ));
}

#[tokio::test]
async fn vector_acl_filtering_happens_before_ranking_and_limit() {
    // Two high-scoring rows the caller is DENIED, one lower-scoring row ALLOWED.
    let (objects, repo_id, head) = build_repo(
        "vec-acl-limit",
        &[
            FileSpec {
                name: "denied_a.txt",
                content: "alpha alpha alpha",
                mode: 0o600,
                uid: 1000,
                gid: 1000,
            },
            FileSpec {
                name: "denied_b.txt",
                content: "alpha alpha beta",
                mode: 0o600,
                uid: 1000,
                gid: 1000,
            },
            FileSpec {
                name: "allowed.txt",
                content: "gamma delta epsilon",
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        ],
    )
    .await;
    let store = InMemorySearchIndexStore::new();
    let extraction = InMemoryTextExtractionStore::new();
    let provider = DeterministicEmbeddingProvider::with_dimensions(VECTOR_TEST_DIMS);
    index_durable_commit_with_embeddings(
        &repo_id,
        &head,
        &*objects,
        &extraction,
        &store,
        Some(&provider),
    )
    .await
    .unwrap();

    // Caller is uid 2000: denied the 0600 files, allowed the 0644 file.
    // Query is closest to the denied rows; with limit=1 the only authorized
    // (lower-scoring) row must be returned, proving ACL filters before limit.
    let req = vector_request(
        &head,
        &provider,
        "alpha alpha alpha",
        1,
        user_filter(2000, 2000),
        None,
    )
    .await;
    let results = store.vector_search(req).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].path, "/allowed.txt");
}

#[tokio::test]
async fn search_index_traversal_rejects_invalid_tree_names_without_leaking_them() {
    let objects = Arc::new(LocalMemoryObjectStore::new());
    let repo_id = RepoId::new("invalid-tree-repo").unwrap();
    let content = b"hello world".to_vec();
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
    let tree = TreeObject {
        entries: vec![TreeEntry {
            name: "../secret".to_string(),
            kind: TreeEntryKind::Blob,
            id: blob_id,
            mode: 0o644,
            uid: 0,
            gid: 0,
            mime_type: None,
            custom_attrs: Default::default(),
        }],
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
    let head = SearchIndexHead {
        repo_id: repo_id.clone(),
        commit_id: crate::vcs::CommitId::from(ObjectId::from_bytes(&[2; 32])),
        root_tree_id: tree_id,
    };

    let extraction = InMemoryTextExtractionStore::new();
    let error = index_durable_commit(&repo_id, &head, &*objects, &extraction, &store)
        .await
        .expect_err("invalid tree names should fail closed");
    let rendered = error.to_string();
    assert!(matches!(error, VfsError::CorruptStore { .. }));
    assert!(!rendered.contains("../secret"));
}

#[tokio::test]
async fn search_index_truncates_utf8_content_without_panicking() {
    let objects = Arc::new(LocalMemoryObjectStore::new());
    let repo_id = RepoId::new("utf8-truncation-repo").unwrap();
    let content = "é".repeat(MAX_INDEXED_CONTENT_CHARS + 1).into_bytes();
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
    let tree = TreeObject {
        entries: vec![TreeEntry {
            name: "unicode.txt".to_string(),
            kind: TreeEntryKind::Blob,
            id: blob_id,
            mode: 0o644,
            uid: 0,
            gid: 0,
            mime_type: None,
            custom_attrs: Default::default(),
        }],
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
    let head = SearchIndexHead {
        repo_id: repo_id.clone(),
        commit_id: crate::vcs::CommitId::from(ObjectId::from_bytes(&[3; 32])),
        root_tree_id: tree_id,
    };

    let extraction = InMemoryTextExtractionStore::new();
    index_durable_commit(&repo_id, &head, &*objects, &extraction, &store)
        .await
        .unwrap();
    let guard = store.state.read().await;
    let (_, files) = guard.get(&head).expect("indexed head");
    assert!(
        files.is_empty(),
        "oversized extracted text must not be indexed"
    );
    let record = extraction
        .record_for_path(&head, "/unicode.txt")
        .await
        .expect("extraction lookup")
        .expect("extraction record");
    assert_eq!(record.status, ExtractedTextStatus::TooLarge);
}
