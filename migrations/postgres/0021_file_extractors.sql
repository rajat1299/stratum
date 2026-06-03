-- File extractors: derived text records and search extraction readiness (Slice 22)

CREATE TABLE IF NOT EXISTS extracted_text_records (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL CONSTRAINT extracted_text_records_commit_id_check CHECK (commit_id ~ '^[0-9a-f]{64}$'),
    root_tree_id TEXT NOT NULL CONSTRAINT extracted_text_records_root_tree_id_check CHECK (root_tree_id ~ '^[0-9a-f]{64}$'),
    path TEXT NOT NULL CONSTRAINT extracted_text_records_path_check CHECK (path <> '' AND path ~ '^/'),
    object_id TEXT NOT NULL CONSTRAINT extracted_text_records_object_id_check CHECK (object_id ~ '^[0-9a-f]{64}$'),
    source_byte_len BIGINT NOT NULL CONSTRAINT extracted_text_records_source_byte_len_check CHECK (source_byte_len >= 0),
    source_mime_type TEXT,
    extractor TEXT NOT NULL,
    status TEXT NOT NULL CONSTRAINT extracted_text_records_status_check CHECK (status IN ('ready', 'unsupported', 'too_large', 'failed')),
    text_hash TEXT CONSTRAINT extracted_text_records_text_hash_check CHECK (text_hash IS NULL OR text_hash ~ '^[0-9a-f]{64}$'),
    text_char_count INTEGER NOT NULL DEFAULT 0 CONSTRAINT extracted_text_records_text_char_count_check CHECK (text_char_count >= 0),
    extracted_text TEXT,
    failure_code TEXT CONSTRAINT extracted_text_records_failure_code_check CHECK (failure_code IS NULL OR failure_code <> ''),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, commit_id, root_tree_id, path),
    FOREIGN KEY (repo_id, commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    CONSTRAINT extracted_text_records_ready_shape_check CHECK (
        (status = 'ready' AND extracted_text IS NOT NULL AND text_hash IS NOT NULL AND failure_code IS NULL)
        OR (status <> 'ready' AND extracted_text IS NULL AND text_hash IS NULL AND failure_code IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS extracted_text_records_object_lookup_idx
    ON extracted_text_records(repo_id, object_id);
CREATE INDEX IF NOT EXISTS extracted_text_records_head_lookup_idx
    ON extracted_text_records(repo_id, commit_id, root_tree_id);
CREATE INDEX IF NOT EXISTS extracted_text_records_path_lookup_idx
    ON extracted_text_records(repo_id, commit_id, root_tree_id, path);

ALTER TABLE search_index_state
  ADD COLUMN IF NOT EXISTS extraction_version TEXT,
  ADD COLUMN IF NOT EXISTS extraction_status TEXT NOT NULL DEFAULT 'missing'
    CONSTRAINT search_index_state_extraction_status_check
    CHECK (extraction_status IN ('missing', 'ready', 'failed')),
  ADD COLUMN IF NOT EXISTS extraction_failure_code TEXT
    CONSTRAINT search_index_state_extraction_failure_code_check
    CHECK (extraction_failure_code IS NULL OR extraction_failure_code <> '');

ALTER TABLE search_index_files
  ADD COLUMN IF NOT EXISTS extraction_version TEXT,
  ADD COLUMN IF NOT EXISTS extractor TEXT,
  ADD COLUMN IF NOT EXISTS extracted_text_hash TEXT
    CONSTRAINT search_index_files_extracted_text_hash_check
    CHECK (extracted_text_hash IS NULL OR extracted_text_hash ~ '^[0-9a-f]{64}$');

ALTER TABLE search_index_state
  ADD CONSTRAINT search_index_state_extraction_ready_lifecycle_check
  CHECK (
    extraction_status <> 'ready'
    OR (
      status = 'ready'
      AND extraction_version IS NOT NULL
      AND extraction_failure_code IS NULL
    )
  );

ALTER TABLE search_index_files
  ADD CONSTRAINT search_index_files_extraction_shape_check
  CHECK (
    (
      extraction_version IS NULL
      AND extractor IS NULL
      AND extracted_text_hash IS NULL
    )
    OR (
      extraction_version IS NOT NULL
      AND extractor IS NOT NULL
      AND extracted_text_hash IS NOT NULL
    )
  );
