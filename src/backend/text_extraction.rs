use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backend::search_index::SearchIndexHead;
use crate::error::VfsError;
use crate::store::ObjectId;

pub const EXTRACTED_TEXT_VERSION_V1: &str = "extracted-text-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractedTextStatus {
    Ready,
    Unsupported,
    TooLarge,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ExtractedTextRecord {
    pub path: String,
    pub object_id: ObjectId,
    pub source_byte_len: u64,
    pub source_mime_type: Option<String>,
    pub extractor: String,
    pub status: ExtractedTextStatus,
    pub text: Option<String>,
    pub text_hash: Option<String>,
    pub failure_code: Option<String>,
}

#[async_trait]
pub trait TextExtractionStore: Send + Sync {
    async fn ensure_available(&self) -> Result<(), VfsError>;

    async fn put_records(
        &self,
        head: SearchIndexHead,
        records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError>;

    async fn record_for_path(
        &self,
        head: &SearchIndexHead,
        path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError>;

    fn available(&self) -> bool;
}

pub struct UnavailableTextExtractionStore;

#[async_trait]
impl TextExtractionStore for UnavailableTextExtractionStore {
    async fn ensure_available(&self) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "text extraction store is unavailable".to_string(),
        })
    }

    async fn put_records(
        &self,
        _head: SearchIndexHead,
        _records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError> {
        Err(VfsError::NotSupported {
            message: "text extraction store is unavailable".to_string(),
        })
    }

    async fn record_for_path(
        &self,
        _head: &SearchIndexHead,
        _path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError> {
        Ok(None)
    }

    fn available(&self) -> bool {
        false
    }
}

type InMemoryExtractionState =
    BTreeMap<(String, String, String, String), ExtractedTextRecord>;

pub struct InMemoryTextExtractionStore {
    state: Arc<RwLock<InMemoryExtractionState>>,
}

impl Default for InMemoryTextExtractionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTextExtractionStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn key(head: &SearchIndexHead, path: &str) -> (String, String, String, String) {
        (
            head.repo_id.as_str().to_string(),
            head.commit_id.to_hex(),
            head.root_tree_id.to_hex(),
            path.to_string(),
        )
    }
}

#[async_trait]
impl TextExtractionStore for InMemoryTextExtractionStore {
    async fn ensure_available(&self) -> Result<(), VfsError> {
        Ok(())
    }

    async fn put_records(
        &self,
        head: SearchIndexHead,
        records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError> {
        let mut guard = self.state.write().await;
        for record in records {
            guard.insert(Self::key(&head, &record.path), record);
        }
        Ok(())
    }

    async fn record_for_path(
        &self,
        head: &SearchIndexHead,
        path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError> {
        let guard = self.state.read().await;
        Ok(guard.get(&Self::key(head, path)).cloned())
    }

    fn available(&self) -> bool {
        true
    }
}

pub type SharedTextExtractionStore = Arc<dyn TextExtractionStore>;
