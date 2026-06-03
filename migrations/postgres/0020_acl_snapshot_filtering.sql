-- ACL snapshot filtering for Postgres FTS semantic search (Slice 21)

ALTER TABLE search_index_state
  ADD COLUMN IF NOT EXISTS acl_snapshot_version TEXT,
  ADD COLUMN IF NOT EXISTS acl_snapshot_status TEXT NOT NULL DEFAULT 'missing'
    CONSTRAINT search_index_state_acl_snapshot_status_check
    CHECK (acl_snapshot_status IN ('missing', 'ready', 'failed')),
  ADD COLUMN IF NOT EXISTS acl_snapshot_failure_code TEXT
    CONSTRAINT search_index_state_acl_snapshot_failure_code_check
    CHECK (acl_snapshot_failure_code IS NULL OR acl_snapshot_failure_code <> '');

ALTER TABLE search_index_files
  ADD COLUMN IF NOT EXISTS acl_snapshot_version TEXT,
  ADD COLUMN IF NOT EXISTS acl_snapshot_hash TEXT
    CONSTRAINT search_index_files_acl_snapshot_hash_check
    CHECK (acl_snapshot_hash IS NULL OR acl_snapshot_hash ~ '^[0-9a-f]{64}$'),
  ADD COLUMN IF NOT EXISTS acl_snapshot JSONB;

ALTER TABLE search_index_files
  ADD CONSTRAINT search_index_files_acl_snapshot_shape_check
  CHECK (
    (
      acl_snapshot_version IS NULL
      AND acl_snapshot_hash IS NULL
      AND acl_snapshot IS NULL
    )
    OR (
      acl_snapshot_version IS NOT NULL
      AND acl_snapshot_hash IS NOT NULL
      AND acl_snapshot IS NOT NULL
      AND jsonb_typeof(acl_snapshot) = 'object'
    )
  );

ALTER TABLE search_index_state
  ADD CONSTRAINT search_index_state_acl_ready_lifecycle_check
  CHECK (
    acl_snapshot_status <> 'ready'
    OR (
      status = 'ready'
      AND acl_snapshot_version IS NOT NULL
      AND acl_snapshot_failure_code IS NULL
    )
  );
