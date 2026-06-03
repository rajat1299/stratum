-- Postgres FTS Search MVP
--
-- Adds search_index_state and search_index_files tables to index file contents in commits.

CREATE TABLE IF NOT EXISTS search_index_state (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL CONSTRAINT search_index_state_commit_id_check CHECK (commit_id ~ '^[0-9a-f]{64}$'),
    root_tree_id TEXT NOT NULL CONSTRAINT search_index_state_root_tree_id_check CHECK (root_tree_id ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CONSTRAINT search_index_state_status_check CHECK (status IN ('indexing', 'ready', 'failed')),
    indexed_file_count INTEGER NOT NULL DEFAULT 0 CONSTRAINT search_index_state_file_count_check CHECK (indexed_file_count >= 0),
    indexed_byte_count BIGINT NOT NULL DEFAULT 0 CONSTRAINT search_index_state_byte_count_check CHECK (indexed_byte_count >= 0),
    failure_code TEXT CONSTRAINT search_index_state_failure_code_check CHECK (failure_code <> ''),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, commit_id, root_tree_id),
    FOREIGN KEY (repo_id, commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    CONSTRAINT search_index_state_lifecycle_check CHECK (
        (status = 'indexing' AND completed_at IS NULL AND failure_code IS NULL)
        OR (status = 'ready' AND completed_at IS NOT NULL AND failure_code IS NULL)
        OR (status = 'failed' AND completed_at IS NOT NULL AND failure_code IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS search_index_files (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL,
    root_tree_id TEXT NOT NULL,
    path TEXT NOT NULL CONSTRAINT search_index_files_path_check CHECK (path <> '' AND path ~ '^/'),
    object_id TEXT NOT NULL CONSTRAINT search_index_files_object_id_check CHECK (object_id ~ '^[0-9a-f]{64}$'),
    byte_len INTEGER NOT NULL CONSTRAINT search_index_files_byte_len_check CHECK (byte_len >= 0),
    content_preview TEXT NOT NULL,
    search_vector TSVECTOR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, commit_id, root_tree_id, path),
    FOREIGN KEY (repo_id, commit_id, root_tree_id) REFERENCES search_index_state(repo_id, commit_id, root_tree_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS search_index_files_vector_idx ON search_index_files USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS search_index_files_state_lookup_idx ON search_index_files(repo_id, commit_id, root_tree_id);
CREATE INDEX IF NOT EXISTS search_index_files_path_lookup_idx ON search_index_files(repo_id, commit_id, root_tree_id, path);
