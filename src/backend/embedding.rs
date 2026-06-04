//! Embedding provider boundary and semantic chunk domain (Slice 23).
//!
//! This module defines the `embedding-provider-v1` interface used to turn query
//! text and bounded extracted-text previews into vectors for pgvector ranking.
//!
//! Hard rules enforced here:
//! - The default provider is disabled. Only an explicit deterministic,
//!   provider-free fixture is wired for tests and dev. No code path in this
//!   module makes a network call.
//! - Raw embeddings, provider responses, endpoints, secrets, and raw input text
//!   are never placed in `Debug` output or error messages. Provider failures
//!   render a single fixed, redacted error.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

use crate::backend::search_index::IndexedFileRow;
use crate::error::VfsError;

pub const EMBEDDING_PROVIDER_VERSION_V1: &str = "embedding-provider-v1";
pub const SEMANTIC_CHUNK_VERSION_V1: &str = "semantic-chunk-v1";
pub const MAX_EMBEDDING_INPUT_CHARS: usize = 100_000;
pub const MAX_EMBEDDING_DIMENSIONS: usize = 4096;

/// Required configuration for an embedding model. Missing any value must leave
/// vector readiness unavailable rather than weakening the FTS fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModelConfig {
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub retention_policy: String,
}

/// A query embedding. `values` are never logged or rendered in `Debug`.
#[derive(Clone)]
pub struct QueryEmbedding {
    pub config: EmbeddingModelConfig,
    pub values: Vec<f32>,
}

impl fmt::Debug for QueryEmbedding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryEmbedding")
            .field("config", &self.config)
            .field("dimensions", &self.values.len())
            .finish_non_exhaustive()
    }
}

/// A document chunk embedding produced by the provider. `values` are never
/// logged or rendered in `Debug`.
#[derive(Clone)]
pub struct DocumentEmbedding {
    pub chunk_ordinal: i32,
    pub chunk_hash: String,
    pub chunk_char_start: i32,
    pub chunk_char_count: i32,
    pub values: Vec<f32>,
}

impl fmt::Debug for DocumentEmbedding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentEmbedding")
            .field("chunk_ordinal", &self.chunk_ordinal)
            .field("chunk_hash", &self.chunk_hash)
            .field("chunk_char_start", &self.chunk_char_start)
            .field("chunk_char_count", &self.chunk_char_count)
            .field("dimensions", &self.values.len())
            .finish_non_exhaustive()
    }
}

/// One semantic chunk derived from a file's bounded extracted-text preview.
///
/// The raw `text` is the embedding input only; it is never stored in the vector
/// table nor surfaced in `Debug`, errors, or logs.
#[derive(Clone)]
pub struct SemanticChunk {
    pub chunk_ordinal: i32,
    pub chunk_hash: String,
    pub chunk_char_start: i32,
    pub chunk_char_count: i32,
    pub text: String,
}

impl fmt::Debug for SemanticChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemanticChunk")
            .field("chunk_ordinal", &self.chunk_ordinal)
            .field("chunk_hash", &self.chunk_hash)
            .field("chunk_char_start", &self.chunk_char_start)
            .field("chunk_char_count", &self.chunk_char_count)
            .finish_non_exhaustive()
    }
}

/// Provider input is the same shape as a semantic chunk.
pub type EmbeddingChunkInput = SemanticChunk;

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Returns the model configuration when the provider is configured.
    fn config(&self) -> Option<EmbeddingModelConfig>;

    /// Whether the provider can serve embeddings for this runtime posture.
    fn available(&self) -> bool;

    async fn embed_query(&self, query: &str) -> Result<QueryEmbedding, VfsError>;

    async fn embed_documents(
        &self,
        chunks: Vec<EmbeddingChunkInput>,
    ) -> Result<Vec<DocumentEmbedding>, VfsError>;
}

pub type SharedEmbeddingProvider = Arc<dyn EmbeddingProvider>;

/// Fixed redacted error used when the provider is not configured/available.
pub fn embedding_provider_unavailable_error() -> VfsError {
    VfsError::NotSupported {
        message: "embedding provider is unavailable".to_string(),
    }
}

/// Fixed redacted error used when a configured provider request fails. This must
/// never include provider response text, endpoints, secrets, or raw input.
pub fn embedding_provider_failed_error() -> VfsError {
    VfsError::NotSupported {
        message: "embedding provider request failed".to_string(),
    }
}

fn embedding_input_empty_error() -> VfsError {
    VfsError::InvalidArgs {
        message: "embedding input cannot be empty".to_string(),
    }
}

fn embedding_input_too_large_error() -> VfsError {
    VfsError::InvalidArgs {
        message: "embedding input is too large".to_string(),
    }
}

/// Validates input before any provider use. Rejects empty and overlong input.
pub fn validate_embedding_input(text: &str) -> Result<(), VfsError> {
    if text.trim().is_empty() {
        return Err(embedding_input_empty_error());
    }
    if text.chars().count() > MAX_EMBEDDING_INPUT_CHARS {
        return Err(embedding_input_too_large_error());
    }
    Ok(())
}

/// Provider that reports unavailable and never attempts an embedding. This is
/// the default in every runtime posture.
pub struct UnavailableEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for UnavailableEmbeddingProvider {
    fn config(&self) -> Option<EmbeddingModelConfig> {
        None
    }

    fn available(&self) -> bool {
        false
    }

    async fn embed_query(&self, _query: &str) -> Result<QueryEmbedding, VfsError> {
        Err(embedding_provider_unavailable_error())
    }

    async fn embed_documents(
        &self,
        _chunks: Vec<EmbeddingChunkInput>,
    ) -> Result<Vec<DocumentEmbedding>, VfsError> {
        Err(embedding_provider_unavailable_error())
    }
}

/// Deterministic, provider-free embedding used by tests and explicit dev use.
///
/// Produces stable, L2-normalized vectors via feature hashing so that identical
/// text yields identical vectors and shared tokens raise cosine similarity. It
/// makes no network call.
pub struct DeterministicEmbeddingProvider {
    config: EmbeddingModelConfig,
}

impl DeterministicEmbeddingProvider {
    pub fn new(config: EmbeddingModelConfig) -> Self {
        Self { config }
    }

    /// Convenience constructor for tests and dev with a fixed fixture config.
    pub fn with_dimensions(dimensions: usize) -> Self {
        Self {
            config: EmbeddingModelConfig {
                provider: "deterministic-fixture".to_string(),
                model: "deterministic-fixture-v1".to_string(),
                dimensions,
                retention_policy: "head-scoped".to_string(),
            },
        }
    }

    fn embed_text(&self, text: &str) -> Vec<f32> {
        deterministic_embedding(text, self.config.dimensions)
    }
}

#[async_trait]
impl EmbeddingProvider for DeterministicEmbeddingProvider {
    fn config(&self) -> Option<EmbeddingModelConfig> {
        Some(self.config.clone())
    }

    fn available(&self) -> bool {
        true
    }

    async fn embed_query(&self, query: &str) -> Result<QueryEmbedding, VfsError> {
        validate_embedding_input(query)?;
        Ok(QueryEmbedding {
            config: self.config.clone(),
            values: self.embed_text(query),
        })
    }

    async fn embed_documents(
        &self,
        chunks: Vec<EmbeddingChunkInput>,
    ) -> Result<Vec<DocumentEmbedding>, VfsError> {
        let mut out = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            validate_embedding_input(&chunk.text)?;
            let values = self.embed_text(&chunk.text);
            out.push(DocumentEmbedding {
                chunk_ordinal: chunk.chunk_ordinal,
                chunk_hash: chunk.chunk_hash,
                chunk_char_start: chunk.chunk_char_start,
                chunk_char_count: chunk.chunk_char_count,
                values,
            });
        }
        Ok(out)
    }
}

/// Configured provider whose backend requests always fail. Used to exercise the
/// provider-failed fallback path with a fixed redacted error. Makes no network
/// call.
pub struct FailingEmbeddingProvider {
    config: EmbeddingModelConfig,
}

impl FailingEmbeddingProvider {
    pub fn new(config: EmbeddingModelConfig) -> Self {
        Self { config }
    }

    pub fn with_dimensions(dimensions: usize) -> Self {
        Self {
            config: EmbeddingModelConfig {
                provider: "failing-fixture".to_string(),
                model: "failing-fixture-v1".to_string(),
                dimensions,
                retention_policy: "head-scoped".to_string(),
            },
        }
    }
}

#[async_trait]
impl EmbeddingProvider for FailingEmbeddingProvider {
    fn config(&self) -> Option<EmbeddingModelConfig> {
        Some(self.config.clone())
    }

    fn available(&self) -> bool {
        true
    }

    async fn embed_query(&self, query: &str) -> Result<QueryEmbedding, VfsError> {
        validate_embedding_input(query)?;
        Err(embedding_provider_failed_error())
    }

    async fn embed_documents(
        &self,
        chunks: Vec<EmbeddingChunkInput>,
    ) -> Result<Vec<DocumentEmbedding>, VfsError> {
        for chunk in &chunks {
            validate_embedding_input(&chunk.text)?;
        }
        Err(embedding_provider_failed_error())
    }
}

fn deterministic_embedding(text: &str, dimensions: usize) -> Vec<f32> {
    let dims = dimensions.clamp(1, MAX_EMBEDDING_DIMENSIONS);
    let mut values = vec![0.0f32; dims];
    let mut token_count = 0usize;
    for token in text
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
    {
        token_count += 1;
        let lowered = token.to_ascii_lowercase();
        let digest = Sha256::digest(lowered.as_bytes());
        let mut seed = [0u8; 8];
        seed.copy_from_slice(&digest[0..8]);
        let index = u64::from_be_bytes(seed) as usize % dims;
        let sign = if digest[8] & 1 == 0 { 1.0 } else { -1.0 };
        values[index] += sign;
    }
    if token_count == 0 {
        let digest = Sha256::digest(text.as_bytes());
        let mut seed = [0u8; 8];
        seed.copy_from_slice(&digest[0..8]);
        let index = u64::from_be_bytes(seed) as usize % dims;
        values[index] = 1.0;
    }
    normalize_l2(&mut values);
    values
}

fn normalize_l2(values: &mut [f32]) {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in values.iter_mut() {
            *v /= norm;
        }
    }
}

/// Produces semantic chunks for an indexed file from its bounded preview.
///
/// First pass: one chunk per file. No chunk is produced for an empty preview.
pub fn semantic_chunks_for_indexed_file(file: &IndexedFileRow) -> Vec<SemanticChunk> {
    if file.content_preview.is_empty() {
        return Vec::new();
    }
    let text = file.content_preview.clone();
    let char_count = text.chars().count().min(MAX_EMBEDDING_INPUT_CHARS) as i32;
    let chunk_hash = semantic_chunk_hash(
        &file.path,
        &file.object_id.to_hex(),
        &file.extracted_text_hash,
        &text,
    );
    vec![SemanticChunk {
        chunk_ordinal: 0,
        chunk_hash,
        chunk_char_start: 0,
        chunk_char_count: char_count,
        text,
    }]
}

/// SHA-256 over chunker version, path, object id, extracted text hash, and chunk
/// text. The chunk text contributes to the hash but is never stored.
pub fn semantic_chunk_hash(
    path: &str,
    object_id_hex: &str,
    extracted_text_hash: &str,
    text: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(SEMANTIC_CHUNK_VERSION_V1.as_bytes());
    hasher.update([0u8]);
    hasher.update(path.as_bytes());
    hasher.update([0u8]);
    hasher.update(object_id_hex.as_bytes());
    hasher.update([0u8]);
    hasher.update(extracted_text_hash.as_bytes());
    hasher.update([0u8]);
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::search_index::IndexedFileRow;
    use crate::store::ObjectId;

    fn fixture_config(dimensions: usize) -> EmbeddingModelConfig {
        EmbeddingModelConfig {
            provider: "deterministic-fixture".to_string(),
            model: "deterministic-fixture-v1".to_string(),
            dimensions,
            retention_policy: "head-scoped".to_string(),
        }
    }

    fn l2_norm(values: &[f32]) -> f32 {
        values.iter().map(|v| v * v).sum::<f32>().sqrt()
    }

    #[tokio::test]
    async fn disabled_provider_reports_unavailable_and_never_embeds() {
        let provider = UnavailableEmbeddingProvider;
        assert!(!provider.available());
        assert!(provider.config().is_none());
        let err = provider.embed_query("hello world").await.unwrap_err();
        assert!(matches!(err, VfsError::NotSupported { .. }));
        let err = provider
            .embed_documents(vec![SemanticChunk {
                chunk_ordinal: 0,
                chunk_hash: "x".to_string(),
                chunk_char_start: 0,
                chunk_char_count: 1,
                text: "hello".to_string(),
            }])
            .await
            .unwrap_err();
        assert!(matches!(err, VfsError::NotSupported { .. }));
    }

    #[tokio::test]
    async fn deterministic_provider_returns_stable_vectors_with_configured_dimensions() {
        let provider = DeterministicEmbeddingProvider::new(fixture_config(64));
        let a = provider.embed_query("the quick brown fox").await.unwrap();
        let b = provider.embed_query("the quick brown fox").await.unwrap();
        assert_eq!(a.values.len(), 64);
        assert_eq!(a.config.dimensions, 64);
        assert_eq!(a.values, b.values, "vectors must be deterministic");

        let other = provider
            .embed_query("a different sentence entirely")
            .await
            .unwrap();
        assert_ne!(
            a.values, other.values,
            "different text yields different vectors"
        );
    }

    #[tokio::test]
    async fn deterministic_query_and_document_vectors_are_normalized_consistently() {
        let provider = DeterministicEmbeddingProvider::new(fixture_config(128));
        let query = provider.embed_query("alpha beta gamma").await.unwrap();
        assert!((l2_norm(&query.values) - 1.0).abs() < 1e-4);

        let docs = provider
            .embed_documents(vec![
                SemanticChunk {
                    chunk_ordinal: 0,
                    chunk_hash: "h0".to_string(),
                    chunk_char_start: 0,
                    chunk_char_count: 16,
                    text: "alpha beta gamma".to_string(),
                },
                SemanticChunk {
                    chunk_ordinal: 1,
                    chunk_hash: "h1".to_string(),
                    chunk_char_start: 0,
                    chunk_char_count: 5,
                    text: "delta".to_string(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(docs.len(), 2);
        for doc in &docs {
            assert_eq!(doc.values.len(), 128);
            assert!((l2_norm(&doc.values) - 1.0).abs() < 1e-4);
        }
        // Query and an identical-text document embed to the same normalized vector.
        assert_eq!(query.values, docs[0].values);
    }

    #[tokio::test]
    async fn overlong_input_is_rejected_before_provider_use() {
        let provider = DeterministicEmbeddingProvider::new(fixture_config(32));
        let huge = "a".repeat(MAX_EMBEDDING_INPUT_CHARS + 1);
        let err = provider.embed_query(&huge).await.unwrap_err();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        let err = provider
            .embed_documents(vec![SemanticChunk {
                chunk_ordinal: 0,
                chunk_hash: "h".to_string(),
                chunk_char_start: 0,
                chunk_char_count: 0,
                text: huge,
            }])
            .await
            .unwrap_err();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn empty_input_is_rejected() {
        let provider = DeterministicEmbeddingProvider::new(fixture_config(32));
        let err = provider.embed_query("   ").await.unwrap_err();
        assert!(matches!(err, VfsError::InvalidArgs { .. }));
    }

    #[tokio::test]
    async fn provider_failure_renders_fixed_redacted_error() {
        let provider = FailingEmbeddingProvider::new(EmbeddingModelConfig {
            provider: "failing-fixture".to_string(),
            model: "failing-fixture-v1".to_string(),
            dimensions: 16,
            retention_policy: "head-scoped".to_string(),
        });
        assert!(provider.available());
        let secret_input = "https://endpoint.example/secret?api_key=TOPSECRET /etc/object/key";
        let err = provider.embed_query(secret_input).await.unwrap_err();
        let text = err.to_string();
        assert_eq!(
            text,
            "stratum: operation not supported: embedding provider request failed"
        );
        for leak in [
            "https://",
            "api_key",
            "TOPSECRET",
            "/etc/object/key",
            "SELECT",
            "endpoint.example",
        ] {
            assert!(!text.contains(leak), "redacted error leaked: {leak}");
        }
        // Debug of the failed query must not echo embeddings either; nothing to
        // print here, but assert the redacted error path stays fixed.
        let err2 = provider.embed_query(secret_input).await.unwrap_err();
        assert_eq!(err2.to_string(), text);
    }

    fn indexed_file(preview: &str) -> IndexedFileRow {
        IndexedFileRow {
            path: "/docs/readme.md".to_string(),
            object_id: ObjectId::from_bytes(&[7u8; 32]),
            byte_len: preview.len(),
            content_preview: preview.to_string(),
            acl_snapshot: None,
            extraction_version: "extracted-text-v1".to_string(),
            extractor: "markdown-v1".to_string(),
            extracted_text_hash: "a".repeat(64),
        }
    }

    #[test]
    fn semantic_chunks_one_chunk_per_file_and_stable_hash() {
        let file = indexed_file("hello world content");
        let chunks = semantic_chunks_for_indexed_file(&file);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_ordinal, 0);
        assert_eq!(chunks[0].chunk_char_start, 0);
        assert_eq!(
            chunks[0].chunk_char_count,
            "hello world content".chars().count() as i32
        );
        // Hash is stable for the same inputs.
        let again = semantic_chunks_for_indexed_file(&file);
        assert_eq!(chunks[0].chunk_hash, again[0].chunk_hash);
        // Hash changes if the text changes.
        let other = semantic_chunks_for_indexed_file(&indexed_file("different content"));
        assert_ne!(chunks[0].chunk_hash, other[0].chunk_hash);
        // The chunk Debug must not echo the raw text.
        let debug = format!("{:?}", chunks[0]);
        assert!(!debug.contains("hello world content"));
    }

    #[test]
    fn semantic_chunks_empty_preview_yields_no_chunk() {
        let file = indexed_file("");
        assert!(semantic_chunks_for_indexed_file(&file).is_empty());
    }
}
