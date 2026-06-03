-- pgvector Semantic Expansion (Slice 23)
--
-- Adds pgvector-backed vector ranking as an additive derived search index.
-- Vector rows are derived data tied to exact head/path/object/extracted-text/ACL
-- snapshot hashes. They cascade with search_index_state and never become the
-- source of truth. The existing FTS index remains the rollback/fallback path.

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
