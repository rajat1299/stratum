-- Store-backed final object metadata deletion fences.
--
-- Fences are keyed by logical object identity and intentionally do not depend
-- on an objects row existing, so cleanup can fence metadata-missing final
-- objects before deleting bytes.

CREATE TABLE object_deletion_fences (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('blob', 'tree', 'commit')),
    object_id TEXT NOT NULL CHECK (object_id ~ '^[0-9a-f]{64}$'),
    canonical_final_key TEXT NOT NULL CHECK (
        canonical_final_key = 'repos/' || repo_id || '/objects/' || object_kind || '/' || object_id
    ),
    lease_owner TEXT NOT NULL CHECK (
        lease_owner <> ''
        AND length(lease_owner) <= 128
        AND lease_owner !~ '[[:cntrl:]]'
    ),
    fence_token TEXT NOT NULL CHECK (
        fence_token ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ),
    fence_expires_at TIMESTAMPTZ NOT NULL,
    metadata_object_key TEXT,
    metadata_size_bytes BIGINT CHECK (metadata_size_bytes IS NULL OR metadata_size_bytes >= 0),
    metadata_sha256 TEXT CHECK (metadata_sha256 IS NULL OR metadata_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, object_kind, object_id),
    CHECK (
        (metadata_object_key IS NULL AND metadata_size_bytes IS NULL AND metadata_sha256 IS NULL)
        OR
        (metadata_object_key IS NOT NULL AND metadata_size_bytes IS NOT NULL AND metadata_sha256 IS NOT NULL)
    )
);

CREATE INDEX object_deletion_fences_active_idx
    ON object_deletion_fences(fence_expires_at);
