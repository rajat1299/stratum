-- Persisted recovery claims for guarded durable commit post-CAS completion.
--
-- These rows are keyed by immutable commit/ref/step identity. They are not a
-- latest-wins path queue and must not be coalesced by filepath.

CREATE TABLE durable_post_cas_recovery_claims (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    ref_name TEXT NOT NULL CHECK (ref_name = 'main'),
    commit_id TEXT NOT NULL CHECK (commit_id ~ '^[0-9a-f]{64}$'),
    step TEXT NOT NULL CHECK (
        step IN ('workspace_head_update', 'audit_append', 'idempotency_completion')
    ),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'active', 'backing_off', 'completed', 'poisoned')
    ),
    lease_owner TEXT CHECK (
        lease_owner IS NULL OR (
            btrim(lease_owner) <> ''
            AND length(lease_owner) <= 128
            AND lease_owner !~ '[[:cntrl:]]'
        )
    ),
    lease_token TEXT CHECK (
        lease_token IS NULL OR
        lease_token ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ),
    lease_expires_at TIMESTAMPTZ,
    attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 4294967295),
    retry_after TIMESTAMPTZ,
    last_error TEXT,
    completed_at TIMESTAMPTZ,
    poisoned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, ref_name, commit_id, step),
    FOREIGN KEY (repo_id, commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    CONSTRAINT durable_post_cas_recovery_claims_pending_check CHECK (
        state <> 'pending' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NULL
            AND last_error IS NULL
            AND completed_at IS NULL
            AND poisoned_at IS NULL
        )
    ),
    CONSTRAINT durable_post_cas_recovery_claims_active_check CHECK (
        state <> 'active' OR (
            lease_owner IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND retry_after IS NULL
            AND last_error IS NULL
            AND completed_at IS NULL
            AND poisoned_at IS NULL
            AND attempts > 0
        )
    ),
    CONSTRAINT durable_post_cas_recovery_claims_backoff_check CHECK (
        state <> 'backing_off' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NOT NULL
            AND last_error = 'redacted post-CAS recovery failure'
            AND completed_at IS NULL
            AND poisoned_at IS NULL
            AND attempts > 0
        )
    ),
    CONSTRAINT durable_post_cas_recovery_claims_completed_check CHECK (
        state <> 'completed' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NULL
            AND last_error IS NULL
            AND completed_at IS NOT NULL
            AND poisoned_at IS NULL
            AND attempts > 0
        )
    ),
    CONSTRAINT durable_post_cas_recovery_claims_poisoned_check CHECK (
        state <> 'poisoned' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NULL
            AND last_error = 'redacted post-CAS recovery failure'
            AND completed_at IS NULL
            AND poisoned_at IS NOT NULL
            AND attempts > 0
        )
    )
);

CREATE INDEX durable_post_cas_recovery_claims_due_idx
    ON durable_post_cas_recovery_claims(repo_id, state, retry_after, lease_expires_at)
    WHERE state IN ('pending', 'active', 'backing_off');
