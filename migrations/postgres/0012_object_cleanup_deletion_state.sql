-- Persist deletion readiness and final deletion phase markers for cleanup claims.
--
-- The readiness snapshot is all-or-none so destructive cleanup can detect a
-- changed final object before any future byte-deletion phase proceeds.

ALTER TABLE object_cleanup_claims
    ADD COLUMN deletion_ready_at TIMESTAMPTZ,
    ADD COLUMN delete_after TIMESTAMPTZ,
    ADD COLUMN deletion_snapshot_object_key TEXT,
    ADD COLUMN deletion_snapshot_size_bytes BIGINT,
    ADD COLUMN deletion_snapshot_sha256 TEXT,
    ADD COLUMN final_object_bytes_deleted_at TIMESTAMPTZ,
    ADD COLUMN final_object_metadata_deleted_at TIMESTAMPTZ,
    ADD CONSTRAINT object_cleanup_claims_deletion_readiness_all_or_none_check CHECK (
        (
            deletion_ready_at IS NULL
            AND delete_after IS NULL
            deletion_snapshot_object_key IS NULL
            AND deletion_snapshot_size_bytes IS NULL
            AND deletion_snapshot_sha256 IS NULL
        )
        OR
        (
            deletion_ready_at IS NOT NULL
            AND delete_after IS NOT NULL
            deletion_snapshot_object_key IS NOT NULL
            AND deletion_snapshot_size_bytes IS NOT NULL
            AND deletion_snapshot_sha256 IS NOT NULL
        )
    ),
    ADD CONSTRAINT object_cleanup_claims_deletion_phase_markers_ready_check CHECK (
        (
            final_object_bytes_deleted_at IS NULL
            AND final_object_metadata_deleted_at IS NULL
        )
        OR (
            deletion_ready_at IS NOT NULL
            AND delete_after IS NOT NULL
            AND deletion_snapshot_object_key IS NOT NULL
            AND deletion_snapshot_size_bytes IS NOT NULL
            AND deletion_snapshot_sha256 IS NOT NULL
        )
    ),
    ADD CONSTRAINT object_cleanup_claims_deletion_snapshot_size_check CHECK (
        deletion_snapshot_size_bytes IS NULL OR deletion_snapshot_size_bytes >= 0
    ),
    ADD CONSTRAINT object_cleanup_claims_deletion_snapshot_sha256_check CHECK (
        deletion_snapshot_sha256 IS NULL OR deletion_snapshot_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT object_cleanup_claims_deletion_snapshot_canonical_key_check CHECK (
        deletion_snapshot_object_key IS NULL
        OR deletion_snapshot_object_key = 'repos/' || repo_id || '/objects/' || object_kind || '/' || object_id
    ),
    ADD CONSTRAINT object_cleanup_claims_completed_ready_deletion_phases_check CHECK (
        completed_at IS NULL
        OR claim_kind <> 'durable_mutation_cas_lost_object_cleanup'
        OR deletion_ready_at IS NULL
        OR (
            final_object_bytes_deleted_at IS NOT NULL
            AND final_object_metadata_deleted_at IS NOT NULL
        )
    );

CREATE INDEX object_cleanup_claims_deletion_ready_idx
    ON object_cleanup_claims(delete_after, updated_at)
    WHERE deletion_ready_at IS NOT NULL
      AND completed_at IS NULL;
