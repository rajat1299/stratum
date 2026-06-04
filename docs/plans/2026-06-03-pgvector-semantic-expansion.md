# pgvector Semantic Expansion Implementation Plan

**Goal:** Add pgvector-backed vector ranking as an additive derived search index while preserving the current extraction-ready, ACL-filtered Postgres FTS route and its final committed-read recheck.

**Architecture:** Keep `GET /search/semantic` and SDK method shapes stable. Add a separate vector readiness layer beside the existing FTS/extraction/ACL readiness state, store vector rows as derived metadata tied to exact `(repo_id, commit_id, root_tree_id, path, object_id, extracted_text_hash, acl_snapshot_hash)`, and use vector ranking only when pgvector and the embedding provider are ready for the current head. If pgvector or the provider is unavailable, missing, or failed, the route falls back to the existing FTS behavior rather than weakening ACL filtering or disabling durable FTS.

**Tech Stack:** Rust 2024, Axum, Tokio, Postgres with pgvector, existing durable object/commit/ref stores, existing `SearchIndexStore`, `TextExtractionStore`, `posix-tree-v1` ACL snapshots, provider-free deterministic embedding fixtures, TypeScript/Python/Bash SDK contract checks, and optional live Postgres tests behind `STRATUM_POSTGRES_TEST_URL`.

---

## Current Inventory

- `src/backend/search_index.rs` owns `SearchIndexHead`, `SearchIndexRequest`, `SearchIndexResult`, `SearchIndexState`, `IndexedFileRow`, `SearchIndexStore`, in-memory search, query/limit validation, extraction-aware commit indexing, and `path_matches_prefix`.
- `src/backend/search_index/acl.rs` owns `posix-tree-v1` ACL snapshots and caller filters. Vector search must reuse this exact permission model.
- `src/backend/text_extraction.rs` owns `extracted-text-v1`, provider-free text/docx/pdf extraction, `TextExtractionStore`, and extracted-text records.
- `src/backend/postgres.rs` implements `SearchIndexStore` and `TextExtractionStore`. Its current FTS SQL applies repo/commit/root, path prefix, extraction metadata, ACL JSON predicates, and result limiting, then Rust verifies snapshots again.
- `migrations/postgres/0019_postgres_fts_search_mvp.sql` created `search_index_state` and `search_index_files`.
- `migrations/postgres/0020_acl_snapshot_filtering.sql` added ACL snapshot metadata and makes old non-ACL rows fail closed.
- `migrations/postgres/0021_file_extractors.sql` added `extracted_text_records` and extraction readiness.
- `src/server/routes_fs.rs::search_semantic` resolves the durable head, builds the caller ACL filter, calls the search store, then rechecks each candidate with `cat_with_stat_as` and object hash equality before rendering.
- `src/server/routes_capabilities.rs::semantic_search_capability` currently reports semantic search available when durable-cloud has a ready FTS index, ACL readiness, and extraction readiness for the current `main` head.
- TypeScript and Python SDKs already call `GET /search/semantic`; Bash `sgrep` remains unsupported and points callers at server capabilities.

## Design Decisions

- Vector rows are derived data, never source of truth.
- The existing FTS route is the rollback path. Vector failure must not make already-ready FTS unavailable.
- Do not add raw embeddings, raw extracted text, provider requests/responses, provider errors, secrets, DB URLs, object keys, backing paths, or SQL to public responses, logs, docs examples, debug strings, or SDK errors.
- Keep the public route shape stable:

```http
GET /search/semantic?query=<text>&path=<optional>&limit=<optional>
```

- Keep the success response shape stable. `score`, `snippet`, and `match.rank` may be produced by vector ranking when vector is ready; do not add a public `embedding`, `vector`, `provider`, or raw model response field.
- Store no chunk text in the vector table. Reuse bounded `search_index_files.content_preview` for snippets and existing final reads for source truth.
- Use a first chunker version named `semantic-chunk-v1`. Start with one chunk per indexed file from the bounded extracted text preview; add multi-chunk sectioning only if tests prove the one-chunk shape is inadequate.
- Use a first provider interface version named `embedding-provider-v1`.
- Default provider is disabled. Default tests use only a deterministic provider-free fixture provider. No default test may make a network call.
- A production network provider remains disabled unless configured by explicit runtime posture. Provider configuration must include provider id, model id, dimensions, timeout, max batch size, and retention policy. Missing any required value fails vector readiness but leaves FTS available.
- Retention policy for this slice: vector rows are retained only while their exact search head row exists. They cascade with `search_index_state`, are deleted and rewritten on reindex of the same head/model, and are invalid unless `object_id`, `extracted_text_hash`, `acl_snapshot_hash`, model id, dimensions, and chunker version match. Provider requests/responses are never stored.
- Because pgvector ANN indexes can interact poorly with highly selective filters, the first Postgres query should use an ACL-filtered `MATERIALIZED` CTE and exact vector ordering. Add HNSW/IVFFlat only after a review proves that ACL/path/head filters are applied before ranking/limit and exact fallback parity is tested.

## Schema Shape

Add migration `migrations/postgres/0022_pgvector_semantic_expansion.sql`.

The migration should be additive:

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS search_index_vector_state (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL CONSTRAINT search_index_vector_state_commit_id_check CHECK (commit_id ~ '^[0-9a-f]{64}$'),
    root_tree_id TEXT NOT NULL CONSTRAINT search_index_vector_state_root_tree_id_check CHECK (root_tree_id ~ '^[0-9a-f]{64}$'),
    embedding_model TEXT NOT NULL CONSTRAINT search_index_vector_state_model_check CHECK (embedding_model <> ''),
    embedding_provider TEXT NOT NULL CONSTRAINT search_index_vector_state_provider_check CHECK (embedding_provider <> ''),
    embedding_dimensions INTEGER NOT NULL CONSTRAINT search_index_vector_state_dimensions_check CHECK (embedding_dimensions > 0 AND embedding_dimensions <= 4096),
    chunker_version TEXT NOT NULL CONSTRAINT search_index_vector_state_chunker_check CHECK (chunker_version = 'semantic-chunk-v1'),
    status TEXT NOT NULL CONSTRAINT search_index_vector_state_status_check CHECK (status IN ('indexing', 'ready', 'failed')),
    embedded_file_count INTEGER NOT NULL DEFAULT 0 CONSTRAINT search_index_vector_state_file_count_check CHECK (embedded_file_count >= 0),
    embedded_chunk_count INTEGER NOT NULL DEFAULT 0 CONSTRAINT search_index_vector_state_chunk_count_check CHECK (embedded_chunk_count >= 0),
    failure_code TEXT CONSTRAINT search_index_vector_state_failure_code_check CHECK (failure_code IS NULL OR failure_code <> ''),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, commit_id, root_tree_id, embedding_model),
    FOREIGN KEY (repo_id, commit_id, root_tree_id)
        REFERENCES search_index_state(repo_id, commit_id, root_tree_id)
        ON DELETE CASCADE,
    CONSTRAINT search_index_vector_state_lifecycle_check CHECK (
        (status = 'indexing' AND completed_at IS NULL AND failure_code IS NULL)
        OR (status = 'ready' AND completed_at IS NOT NULL AND failure_code IS NULL)
        OR (status = 'failed' AND completed_at IS NOT NULL AND failure_code IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS search_index_vectors (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL,
    root_tree_id TEXT NOT NULL,
    path TEXT NOT NULL CONSTRAINT search_index_vectors_path_check CHECK (path <> '' AND path ~ '^/'),
    chunk_ordinal INTEGER NOT NULL CONSTRAINT search_index_vectors_chunk_ordinal_check CHECK (chunk_ordinal >= 0),
    object_id TEXT NOT NULL CONSTRAINT search_index_vectors_object_id_check CHECK (object_id ~ '^[0-9a-f]{64}$'),
    extracted_text_hash TEXT NOT NULL CONSTRAINT search_index_vectors_extracted_text_hash_check CHECK (extracted_text_hash ~ '^[0-9a-f]{64}$'),
    acl_snapshot_hash TEXT NOT NULL CONSTRAINT search_index_vectors_acl_snapshot_hash_check CHECK (acl_snapshot_hash ~ '^[0-9a-f]{64}$'),
    embedding_model TEXT NOT NULL CONSTRAINT search_index_vectors_model_check CHECK (embedding_model <> ''),
    embedding_provider TEXT NOT NULL CONSTRAINT search_index_vectors_provider_check CHECK (embedding_provider <> ''),
    embedding_dimensions INTEGER NOT NULL CONSTRAINT search_index_vectors_dimensions_check CHECK (embedding_dimensions > 0 AND embedding_dimensions <= 4096),
    chunker_version TEXT NOT NULL CONSTRAINT search_index_vectors_chunker_check CHECK (chunker_version = 'semantic-chunk-v1'),
    chunk_hash TEXT NOT NULL CONSTRAINT search_index_vectors_chunk_hash_check CHECK (chunk_hash ~ '^[0-9a-f]{64}$'),
    chunk_char_start INTEGER NOT NULL CONSTRAINT search_index_vectors_chunk_start_check CHECK (chunk_char_start >= 0),
    chunk_char_count INTEGER NOT NULL CONSTRAINT search_index_vectors_chunk_count_check CHECK (chunk_char_count >= 0),
    embedding vector NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, commit_id, root_tree_id, path, chunk_ordinal, embedding_model),
    FOREIGN KEY (repo_id, commit_id, root_tree_id, embedding_model)
        REFERENCES search_index_vector_state(repo_id, commit_id, root_tree_id, embedding_model)
        ON DELETE CASCADE,
    FOREIGN KEY (repo_id, commit_id, root_tree_id, path)
        REFERENCES search_index_files(repo_id, commit_id, root_tree_id, path)
        ON DELETE CASCADE,
    CONSTRAINT search_index_vectors_embedding_dimensions_check CHECK (vector_dims(embedding) = embedding_dimensions)
);

CREATE INDEX IF NOT EXISTS search_index_vectors_head_model_idx
    ON search_index_vectors(repo_id, commit_id, root_tree_id, embedding_model);

CREATE INDEX IF NOT EXISTS search_index_vectors_path_idx
    ON search_index_vectors(repo_id, commit_id, root_tree_id, path);
```

Do not add an ANN vector index in the first migration unless the implementation also adds exact-query parity tests and adoption checks. If an ANN index is added, it must be treated as an optimization only; correctness must not depend on it.

## Task 1: pgvector Migration And Adoption Verification

**Files:**

- Add: `migrations/postgres/0022_pgvector_semantic_expansion.sql`
- Modify: `src/backend/postgres_migrations.rs`

**Step 1: Write failing migration registration tests**

Add `pgvector_semantic_expansion_migration_is_registered_and_non_destructive`.

Assert:

- catalog length is `22`,
- migration 22 is registered after `file_extractors`,
- SQL contains `CREATE EXTENSION IF NOT EXISTS vector`,
- SQL contains `search_index_vector_state` and `search_index_vectors`,
- SQL does not contain `DROP TABLE`, `DROP COLUMN`, provider secrets, raw provider URLs, or sample embeddings.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations::tests::pgvector_semantic_expansion_migration_is_registered_and_non_destructive --lib -- --nocapture
```

Expected: FAIL before migration 22 is registered.

**Step 2: Add the migration**

Add migration 22 and register it in `POSTGRES_MIGRATIONS` with:

```rust
PostgresMigration {
    version: 22,
    name: "pgvector_semantic_expansion",
    sql: POSTGRES_MIGRATION_0022_PGVECTOR_SEMANTIC_EXPANSION,
}
```

Update `postgres_migration_catalog_len()` expectations from `21` to `22`.

**Step 3: Add schema adoption checks**

Extend `verify_known_schema_catalog` to require:

- `vector` extension exists and `to_regtype('vector')` is not null,
- `search_index_vector_state` table, primary key, FK to `search_index_state`, lifecycle checks, status checks, dimensions check, model/provider non-empty checks, chunker check, and timestamp columns,
- `search_index_vectors` table, primary key, FK to vector state, FK to `search_index_files`, path/object/text-hash/ACL-hash/chunk-hash checks, dimension check using `vector_dims(embedding)`, and lookup indexes,
- no vector table stores raw extracted text.

Add negative adoption tests for:

- missing pgvector extension,
- missing vector state table,
- missing vector rows table,
- weakened dimension check,
- missing FK to `search_index_files`,
- missing `acl_snapshot_hash`,
- missing `extracted_text_hash`,
- vector rows allowed without matching `embedding_model`,
- public adoption errors leaking table names, column names, SQLSTATE, provider ids, or sample vector values.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: PASS after migration/adoption code lands, with live Postgres portions skipped when `STRATUM_POSTGRES_TEST_URL` is unset.

**Step 4: Commit**

```bash
git add migrations/postgres/0022_pgvector_semantic_expansion.sql src/backend/postgres_migrations.rs
git commit -m "migration: add pgvector semantic index"
```

## Task 2: Embedding Provider Boundary And Vector Domain

**Files:**

- Add: `src/backend/embedding.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/backend/search_index.rs`

**Step 1: Write failing provider boundary tests**

Add tests under `backend::embedding` for:

- disabled provider reports unavailable and never attempts embedding,
- deterministic provider returns stable vectors with configured dimensions,
- deterministic query/document vectors are normalized consistently,
- overlong input is rejected before provider use,
- empty input is rejected,
- provider failure renders a fixed redacted error that omits provider response text, endpoint, API key, DB URL, object key, path, SQL, and raw input text.

Run:

```bash
cargo test --locked backend::embedding --lib -- --nocapture
```

Expected: FAIL before `embedding.rs` exists.

**Step 2: Add provider/domain types**

Add these shapes, adapting names only if the surrounding code suggests a clearer local pattern:

```rust
pub const EMBEDDING_PROVIDER_VERSION_V1: &str = "embedding-provider-v1";
pub const SEMANTIC_CHUNK_VERSION_V1: &str = "semantic-chunk-v1";
pub const MAX_EMBEDDING_INPUT_CHARS: usize = 100_000;
pub const MAX_EMBEDDING_DIMENSIONS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModelConfig {
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub retention_policy: String,
}

#[derive(Debug, Clone)]
pub struct QueryEmbedding {
    pub config: EmbeddingModelConfig,
    pub values: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct DocumentEmbedding {
    pub chunk_ordinal: i32,
    pub chunk_hash: String,
    pub chunk_char_start: i32,
    pub chunk_char_count: i32,
    pub values: Vec<f32>,
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    fn config(&self) -> Option<EmbeddingModelConfig>;
    fn available(&self) -> bool;
    async fn embed_query(&self, query: &str) -> Result<QueryEmbedding, VfsError>;
    async fn embed_documents(&self, chunks: Vec<EmbeddingChunkInput>) -> Result<Vec<DocumentEmbedding>, VfsError>;
}
```

Add:

- `UnavailableEmbeddingProvider`,
- `DeterministicEmbeddingProvider` for tests and explicit provider-free dev use,
- `SharedEmbeddingProvider = Arc<dyn EmbeddingProvider>`.

Do not add a network provider in this task. If a network provider is added later in the slice, it must be disabled by default and gated by explicit runtime config with redaction tests.

**Step 3: Add chunk domain**

Add `SemanticChunk` and a `semantic_chunks_for_indexed_file(file: &IndexedFileRow)` helper. First pass should produce one chunk per indexed file using `content_preview`.

Rules:

- no chunk for empty preview,
- chunk hash is SHA-256 over chunker version, path, object id, extracted text hash, and chunk text,
- chunk text is never stored in the vector table or public errors,
- char offsets are non-negative and bounded.

**Step 4: Wire stores**

Add `embedding_provider` to `ServerStores` and `ServerState`. Defaults:

- local/default runtime: `UnavailableEmbeddingProvider`,
- tests can inject `DeterministicEmbeddingProvider`,
- durable-cloud runtime: unavailable unless explicit config is added in a later task.

If runtime config is implemented here, use explicit env names and keep missing config vector-unavailable:

- `STRATUM_EMBEDDING_PROVIDER`
- `STRATUM_EMBEDDING_MODEL`
- `STRATUM_EMBEDDING_DIMENSIONS`
- `STRATUM_EMBEDDING_RETENTION_POLICY`
- `STRATUM_EMBEDDING_TIMEOUT_MS`

Never log raw values for endpoints, keys, model responses, or input text.

**Step 5: Verify and commit**

```bash
cargo test --locked backend::embedding --lib -- --nocapture
cargo check --locked
git add src/backend/embedding.rs src/backend/mod.rs src/server/mod.rs src/backend/search_index.rs
git commit -m "feat: add embedding provider boundary"
```

## Task 3: Vector Indexing Contract And In-Memory Store

**Files:**

- Modify: `src/backend/search_index.rs`
- Modify: `src/backend/search_index/extra_tests.rs`
- Modify: `src/backend/embedding.rs`

**Step 1: Write failing in-memory vector tests**

Add tests under `backend::search_index` for:

- vector-ready head returns vector-ranked results when deterministic provider is available,
- vector-missing head falls back to FTS search results,
- provider-disabled head still returns FTS results,
- provider-failed head still returns FTS results and does not leak provider details,
- missing ACL snapshot on any vector-backed row fails vector search closed and falls back to FTS only if FTS rows are independently ACL-ready,
- missing extracted-text metadata makes vector readiness missing/failed and preserves existing FTS fail-closed behavior for extraction-missing rows,
- unsupported extraction records do not produce vector chunks,
- path-prefix filtering is segment-safe for vector results,
- query validation and limit validation remain unchanged,
- ACL filtering happens before ranking/limit by constructing two high-scoring denied rows plus one lower-scoring allowed row with `limit = 1` and asserting the allowed row is returned,
- final object-hash recheck still suppresses stale indexed object ids at the route layer.

Run:

```bash
cargo test --locked backend::search_index --lib -- --nocapture
```

Expected: FAIL before vector store methods exist.

**Step 2: Add vector state/result types**

Add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexStatus {
    Missing,
    Indexing,
    Ready,
    Failed,
}

#[derive(Debug, Clone)]
pub struct VectorIndexState {
    pub status: VectorIndexStatus,
    pub embedding_model: Option<String>,
    pub embedding_provider: Option<String>,
    pub embedding_dimensions: Option<i32>,
    pub chunker_version: Option<String>,
    pub embedded_file_count: i32,
    pub embedded_chunk_count: i32,
    pub failure_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexedVectorChunk {
    pub path: String,
    pub chunk_ordinal: i32,
    pub object_id: ObjectId,
    pub extracted_text_hash: String,
    pub acl_snapshot_hash: String,
    pub embedding_model: String,
    pub embedding_provider: String,
    pub embedding_dimensions: i32,
    pub chunker_version: String,
    pub chunk_hash: String,
    pub chunk_char_start: i32,
    pub chunk_char_count: i32,
    pub embedding: Vec<f32>,
}
```

Add a `VectorSearchIndexRequest` that carries the existing head, query text, optional path prefix, limit, ACL filter, and `QueryEmbedding`.

**Step 3: Extend `SearchIndexStore` without breaking FTS**

Add methods:

```rust
async fn index_vectors(
    &self,
    head: SearchIndexHead,
    model: EmbeddingModelConfig,
    chunks: Vec<IndexedVectorChunk>,
) -> Result<(), VfsError>;

async fn vector_search(
    &self,
    req: VectorSearchIndexRequest,
) -> Result<Vec<SearchIndexResult>, VfsError>;

async fn vector_health_for_head(
    &self,
    head: &SearchIndexHead,
    model: &EmbeddingModelConfig,
) -> Result<Option<VectorIndexState>, VfsError>;
```

Existing `search(req)` remains FTS-backed. Vector methods returning `NotSupported`, `NotFound`, or vector-not-ready must not alter FTS readiness.

**Step 4: Add vector indexing helper**

Extend `index_durable_commit` carefully. Preferred shape:

```rust
pub async fn index_durable_commit(
    repo_id: &RepoId,
    head: &SearchIndexHead,
    objects: &dyn ObjectStore,
    extraction_store: &dyn TextExtractionStore,
    search_store: &dyn SearchIndexStore,
    embedding_provider: Option<&dyn EmbeddingProvider>,
) -> Result<(), VfsError>
```

If changing all call sites is noisy, add `index_durable_commit_with_embeddings(...)` and keep the old helper delegating with `None`.

Ordering:

1. extract records,
2. write extraction records,
3. write FTS index rows,
4. if provider unavailable, stop successfully with vector missing,
5. if provider available, build chunks only from FTS-ready rows,
6. embed chunks,
7. write vector rows/state,
8. if embedding or vector write fails, mark only vector state failed and leave extraction/FTS rows ready.

**Step 5: Implement in-memory vector store**

Use deterministic cosine or dot-product ranking over `Vec<f32>`. Before ranking:

- find exact head/model/dimensions,
- require vector state ready,
- require FTS state semantic-ready,
- require matching file row for each vector chunk,
- require `extracted_text_hash` and `acl_snapshot_hash` match the FTS row,
- verify ACL snapshot,
- apply caller ACL filter,
- apply path prefix.

Then rank and limit.

**Step 6: Verify and commit**

```bash
cargo test --locked backend::embedding --lib -- --nocapture
cargo test --locked backend::search_index --lib -- --nocapture
cargo check --locked
git add src/backend/search_index.rs src/backend/search_index/extra_tests.rs src/backend/embedding.rs
git commit -m "feat: add vector search domain"
```

## Task 4: Postgres Vector Store And SQL Correctness

**Files:**

- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs` only if adoption gaps appear

**Step 1: Write failing Postgres vector tests**

Add tests under `backend::postgres` for:

- pgvector schema unavailable makes vector methods unavailable without leaking `search_index_vectors`, SQLSTATE, DB URL, or extension details,
- vector index writes are idempotent and scoped by repo/commit/root/model,
- vector-ready search returns vector-ranked results when FTS/ACL/extraction readiness also exists,
- vector-missing state causes the route/store caller to use FTS fallback,
- provider-disabled/failure marks vector missing/failed without changing FTS `search_index_state.status = ready`,
- missing ACL snapshot row fails vector search closed,
- missing extracted text hash or mismatched extracted text hash fails vector search closed,
- unsupported extraction has no vector row,
- stale object id is still removed by route final recheck,
- path-prefix filtering is segment-safe,
- ACL filtering happens before vector ranking and limit,
- vector query/result errors are redacted.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: FAIL before Postgres vector methods exist.

**Step 2: Implement vector state row conversion**

Add helpers beside existing search-index helpers:

- `vector_index_unavailable_error()`,
- `vector_index_not_ready_error()`,
- `vector_index_state_from_row(row)`,
- `prepare_vector_chunks(head, model, chunks)`,
- `format_pgvector(values: &[f32]) -> Result<String, VfsError>`.

`format_pgvector` should:

- reject empty vectors,
- reject non-finite floats,
- reject dimensions above `MAX_EMBEDDING_DIMENSIONS`,
- produce a parameter value such as `[0.1,0.2]`,
- never include vectors in errors or debug logs.

**Step 3: Implement `index_vectors` transaction**

Within one transaction:

1. insert/update `search_index_vector_state` as `indexing`,
2. delete old `search_index_vectors` for the same head/model,
3. insert chunks,
4. update state to `ready`,
5. on vector insert failure, rollback and mark vector state `failed`.

Do not update `search_index_state.status`, `acl_snapshot_status`, or `extraction_status` on vector failures.

**Step 4: Implement vector search SQL**

Use parameterized SQL. The query must filter before ranking/limit.

Recommended shape:

```sql
WITH acl_allowed AS MATERIALIZED (
    SELECT f.path,
           f.object_id,
           f.content_preview,
           f.acl_snapshot_version,
           f.acl_snapshot_hash,
           f.acl_snapshot,
           v.chunk_ordinal,
           v.embedding <=> $1::vector AS distance
    FROM search_index_files f
    JOIN search_index_vectors v
      ON v.repo_id = f.repo_id
     AND v.commit_id = f.commit_id
     AND v.root_tree_id = f.root_tree_id
     AND v.path = f.path
    WHERE f.repo_id = $2
      AND f.commit_id = $3
      AND f.root_tree_id = $4
      AND v.embedding_model = $5
      AND v.embedding_provider = $6
      AND v.embedding_dimensions = $7
      AND v.chunker_version = $8
      AND v.object_id = f.object_id
      AND v.extracted_text_hash = f.extracted_text_hash
      AND v.acl_snapshot_hash = f.acl_snapshot_hash
      AND f.extraction_version = $9
      AND f.extracted_text_hash IS NOT NULL
      AND f.acl_snapshot_version = $10
      AND f.acl_snapshot_hash IS NOT NULL
      AND f.acl_snapshot IS NOT NULL
      -- existing path-prefix and ACL JSON predicates go here before ORDER BY/LIMIT
)
SELECT path,
       object_id,
       acl_snapshot_version,
       acl_snapshot_hash,
       acl_snapshot,
       1.0 / (1.0 + distance) AS score,
       content_preview AS snippet
FROM acl_allowed
ORDER BY distance ASC, path ASC, chunk_ordinal ASC
LIMIT $N
```

Copy the existing ACL SQL predicate rather than weakening it. Keep the Rust-side `verify_acl_snapshot` and `acl_snapshot_allows` checks after row decode.

**Step 5: Add readiness helpers**

`vector_health_for_head` should return `None` when schema is unavailable or no vector state exists. A `ready` vector state is usable only when:

- base `SearchIndexState` is FTS/ACL/extraction ready,
- vector state is `ready`,
- provider id/model/dimensions match the query embedding,
- chunker version is `semantic-chunk-v1`,
- vector row count is positive when indexed file count is positive.

**Step 6: Verify and commit**

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked --features postgres
git add src/backend/postgres.rs src/backend/postgres_migrations.rs
git commit -m "feat: add Postgres vector search store"
```

## Task 5: Route, Capability, And Rollback Behavior

**Files:**

- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_capabilities.rs`
- Modify: `src/server/mod.rs`
- Modify: `sdk/contracts/capabilities.v1.json` only if manifest output intentionally changes
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json` only if manifest output intentionally changes

**Step 1: Write failing route tests**

Add route tests for:

- vector-ready durable-cloud search returns vector-ranked results and stable response shape,
- vector-missing durable-cloud search returns existing FTS results,
- provider-disabled durable-cloud search returns existing FTS results,
- provider-failed durable-cloud search returns existing FTS results and redacted errors/logs,
- missing ACL snapshot never returns a vector result,
- missing extracted text never returns a vector result,
- unsupported extraction never returns a vector result,
- stale object id from vector result is suppressed by final `cat_with_stat_as` hash recheck,
- path-prefix filtering is segment-safe,
- ACL filtering happens before ranking/limit by ensuring denied high-score rows cannot consume `limit`,
- local/default runtime still returns `501` for semantic store unavailable,
- FTS remains available when vector readiness is missing or failed.

Run:

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
```

Expected: FAIL before route vector fallback is wired.

**Step 2: Route logic**

In `search_semantic`:

1. preserve existing durable-cloud/runtime/store/auth/repo/head/query/path/limit validation,
2. build `SearchIndexRequest` exactly as today,
3. try vector only when:
   - `state.embedding_provider.available()` is true,
   - provider config exists,
   - `vector_health_for_head(&head, &config)` is ready,
   - `embed_query(query_text)` succeeds,
4. call `vector_search`,
5. on vector `NotSupported`, `NotFound`, provider unavailable, provider failed, vector missing, or vector not ready, call existing FTS `search`,
6. on vector ACL/hash/malformed-row safety failure, prefer fail-closed for vector and fall back only to FTS if the FTS search independently succeeds with ACL/extraction readiness,
7. run the existing final candidate projection loop unchanged.

Do not surface whether vector or FTS served the result unless docs later add an explicit, compatible metadata field.

**Step 3: Capability logic**

Keep `routes.search.semantic.available` tied to existing durable-cloud FTS/ACL/extraction readiness. Vector readiness should not be required for the route to be available because FTS is the rollback path.

If adding capability notes, update fixtures and revision intentionally. Otherwise, leave fixture shape stable.

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: PASS with no fixture changes unless a manifest change is intentional.

**Step 4: Verify and commit**

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
git add src/server/routes_fs.rs src/server/routes_capabilities.rs src/server/mod.rs sdk/contracts/capabilities.v1.json sdk/contracts/capabilities.v1.durable-cloud.json
git commit -m "feat: add vector-aware semantic search route"
```

## Task 6: SDK Impact And Documentation

**Files:**

- Modify: `docs/semantic-index.md`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify if response/capability shape changes: `sdk/typescript/src/types.ts`
- Modify if response/capability shape changes: `sdk/typescript/tests/client.test.ts`
- Modify if response/capability shape changes: `sdk/python/src/stratum_sdk/types.py`
- Modify if response/capability shape changes: `sdk/python/tests/test_client.py`
- Modify if Bash messaging changes: `sdk/bash/src/commands.ts`
- Modify if Bash messaging changes: `sdk/bash/src/tool-description.ts`

**SDK assessment:**

- TypeScript: no public API change expected. `client.search.semantic(query, options)` continues to call the same route and receive the same response shape.
- Python: no public API change expected. `client.search.semantic(...)` continues to surface server HTTP/capability behavior.
- Bash: `sgrep` may remain unsupported. If wording changes, keep it capability-driven and do not imply host grep/vector fallback.
- Capability fixtures should change only if `routes.search.semantic` notes/revision intentionally change. Vector readiness is internal and should not make semantic route unavailable when FTS is ready.

**Step 1: Write/update docs**

Update docs to cover:

- FTS remains the fallback and rollback path,
- vector rows are derived and tied to exact head/path/object/extracted-text/ACL snapshot hashes,
- pgvector/provider readiness is additive,
- provider calls are disabled by default,
- deterministic provider-free fixtures are used in tests,
- public responses never include embeddings or provider details,
- final committed-read/object hash recheck still applies,
- no raw binary bytes or unsupported extraction records are embedded,
- automatic/background production is still out of scope unless implementation adds it.

**Step 2: Run SDK checks**

If SDK files are unchanged, still run:

```bash
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

If the SDK fixture output changes, update fixtures only with the existing Rust fixture update command:

```bash
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
```

Then rerun SDK checks.

**Step 3: Commit**

```bash
git add docs/semantic-index.md docs/http-api-guide.md docs/project-status.md sdk/typescript/src/types.ts sdk/typescript/tests/client.test.ts sdk/python/src/stratum_sdk/types.py sdk/python/tests/test_client.py sdk/bash/src/commands.ts sdk/bash/src/tool-description.ts
git commit -m "docs: document pgvector semantic expansion"
```

Only add SDK files that actually changed.

## Task 7: Final Review, Hardening, And Full Verification

**Files:**

- Inspect all changed files.
- No new files unless previous tasks required them.

**Step 1: Manual review checklist**

Review for:

- vector rows cannot outlive or outrank missing FTS/ACL/extraction readiness,
- vector failures do not mark the base FTS index failed,
- ACL filtering happens before vector ranking and limit,
- final `cat_with_stat_as` object hash recheck remains unchanged,
- no raw embeddings are returned, logged, debug-printed, or documented,
- no provider errors, secrets, DB URLs, object keys, backing paths, SQL, or raw extracted text leak,
- pgvector extension absence is redacted and fails vector readiness without breaking FTS,
- migrations are additive and adoption failures are redacted,
- provider-disabled/provider-failed behavior is explicit,
- route and SDK shapes remain stable unless intentionally documented.

**Step 2: Required verification**

Run:

```bash
git status --short --branch
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::embedding --lib -- --nocapture
cargo test --locked backend::search_index --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked
cargo check --locked --features postgres
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cd sdk && bun run typecheck
cd sdk && bun run test:run
cargo audit --deny warnings
cargo deny check
```

If `cargo-deny` is not installed or not configured, record that exact fact in the review notes and do not treat it as a silent pass.

**Step 3: Final commit if needed**

If verification causes formatting or hardening changes:

```bash
git add <changed-files>
git commit -m "fix: harden pgvector semantic expansion"
```

## Required Test Matrix

The implementation is not complete until these behaviors are covered:

- vector-ready search ranks with vectors after ACL/path/head/extraction filters,
- vector-missing falls back to current FTS results,
- provider-disabled falls back to current FTS results,
- provider-failed falls back to current FTS results with redacted failure handling,
- stale object id is suppressed by final route recheck,
- missing ACL snapshot never returns a result,
- missing extracted text never returns a vector result,
- unsupported extraction never creates a vector row,
- query validation stays unchanged,
- path-prefix filtering remains segment-safe,
- ACL filtering happens before ranking and limit,
- pgvector extension missing fails vector readiness without breaking FTS,
- vector dimension mismatch fails closed,
- vector model/provider mismatch fails closed,
- migration adoption verifies pgvector tables, extension, dimensions, FKs, indexes, and readiness lifecycle,
- public errors/logs/docs omit raw embeddings, provider errors, secrets, DB URLs, object keys, backing paths, SQL, and raw extracted text.

## Review Prompts

Use these after implementation, before accepting any diff.

**SQL correctness review:**

Review the pgvector migration and Postgres vector search SQL. Focus on extension checks, additive migration behavior, FKs to `search_index_state` and `search_index_files`, vector dimension checks, vector state lifecycle, exact head/model scoping, parameterized vector input, and whether ACL/path/head/extraction filters happen before `ORDER BY` and `LIMIT`. Call out any raw SQL/provider/vector leakage in public errors.

**Fail-closed behavior review:**

Review vector readiness and route fallback. Prove vector failures cannot return unauthorized rows, cannot skip final `cat_with_stat_as` object hash recheck, cannot make old rows look vector-ready, and cannot mark the base FTS index failed. Confirm FTS remains available when vector readiness, pgvector, or provider state is missing/failed.

**Migration adoption review:**

Review `postgres_migrations.rs` adoption checks for migration 22. Confirm the verifier catches missing extension, missing vector tables, weakened dimension checks, missing FKs, missing ACL/extracted-text hashes, and weakened readiness lifecycle. Confirm adoption failure messages are redacted.

**pgvector performance review:**

Review the vector query plan and indexes. Confirm the first implementation is correct under ACL filtering before optimizing. If ANN indexes were added, require exact-search parity tests and proof that filtering semantics cannot return globally-nearest denied rows or starve allowed rows under low limits.

**AI code slop removal review:**

Remove unused abstractions, vague provider config, dead fields, overly broad logging, duplicated ACL SQL that drifted from FTS behavior, fake tests that only assert mocks, and any public response field that exposes internals. Prefer small, explicit helpers that match existing Stratum patterns.
