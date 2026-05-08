-- Persisted, operator-visible diagnostics for guarded durable commit attempts
-- whose commit/ref visibility could not be proven.
--
-- This table is intentionally separate from post-CAS repair claims. Rows here
-- are diagnostic and must not be used to replay committed responses unless a
-- later recovery path first proves ref visibility.

CREATE TABLE durable_pre_visibility_recovery_ledger (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    ref_name TEXT NOT NULL CHECK (ref_name = 'main'),
    commit_id TEXT NOT NULL CHECK (commit_id ~ '^[0-9a-f]{64}$'),
    stage TEXT NOT NULL CHECK (
        stage IN ('commit_metadata_insert', 'ref_visibility_cas')
    ),
    state TEXT NOT NULL CHECK (state IN ('pending', 'resolved')),
    root_tree_id TEXT NOT NULL CHECK (root_tree_id ~ '^[0-9a-f]{64}$'),
    parent_commit_id TEXT CHECK (
        parent_commit_id IS NULL OR parent_commit_id ~ '^[0-9a-f]{64}$'
    ),
    expected_ref_version BIGINT NOT NULL CHECK (expected_ref_version > 0),
    object_count BIGINT NOT NULL CHECK (object_count >= 0),
    changed_path_count BIGINT NOT NULL CHECK (changed_path_count >= 0),
    has_idempotency_reservation BOOLEAN NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    occurrence_count BIGINT NOT NULL CHECK (occurrence_count > 0),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (repo_id, ref_name, commit_id, stage),
    CONSTRAINT durable_pre_visibility_recovery_pending_check CHECK (
        state <> 'pending' OR resolved_at IS NULL
    ),
    CONSTRAINT durable_pre_visibility_recovery_resolved_check CHECK (
        state <> 'resolved' OR resolved_at IS NOT NULL
    )
);

CREATE INDEX durable_pre_visibility_recovery_status_idx
    ON durable_pre_visibility_recovery_ledger(repo_id, state, last_seen_at DESC);
