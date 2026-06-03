use super::*;
use crate::backend::text_extraction::{ExtractedTextStatus, InMemoryTextExtractionStore};
use crate::backend::{LocalMemoryObjectStore, ObjectWrite, RepoId};
use crate::error::VfsError;
use crate::store::ObjectId;
use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
use std::sync::Arc;

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
