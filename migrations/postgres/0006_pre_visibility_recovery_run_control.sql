-- Claim/backoff/poison run control and persisted route context for
-- guarded durable commit pre-visibility recovery rows.

DROP INDEX IF EXISTS durable_pre_visibility_recovery_status_idx;

ALTER TABLE durable_pre_visibility_recovery_ledger
    DROP CONSTRAINT durable_pre_visibility_recovery_pending_check,
    DROP CONSTRAINT durable_pre_visibility_recovery_resolved_check,
    DROP CONSTRAINT durable_pre_visibility_recovery_ledger_state_check;

ALTER TABLE durable_pre_visibility_recovery_ledger
    ADD COLUMN context_json JSONB,
    ADD COLUMN lease_owner TEXT,
    ADD COLUMN lease_token TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 4294967295),
    ADD COLUMN retry_after TIMESTAMPTZ,
    ADD COLUMN last_error TEXT,
    ADD COLUMN poisoned_at TIMESTAMPTZ,
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD CONSTRAINT durable_pre_visibility_recovery_state_check CHECK (
        state IN ('pending', 'active', 'backing_off', 'resolved', 'poisoned')
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_context_json_check CHECK (
        context_json IS NULL OR jsonb_typeof(context_json) = 'object'
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_lease_owner_check CHECK (
        lease_owner IS NULL OR (
            btrim(lease_owner) <> ''
            AND length(lease_owner) <= 128
            AND lease_owner !~ '[[:cntrl:]]'
        )
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_lease_token_check CHECK (
        lease_token IS NULL OR
        lease_token ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_pending_check CHECK (
        state <> 'pending' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND attempts = 0
            AND retry_after IS NULL
            AND last_error IS NULL
            AND resolved_at IS NULL
            AND poisoned_at IS NULL
        )
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_active_check CHECK (
        state <> 'active' OR (
            lease_owner IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND attempts > 0
            AND retry_after IS NULL
            AND last_error IS NULL
            AND resolved_at IS NULL
            AND poisoned_at IS NULL
        )
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_backoff_check CHECK (
        state <> 'backing_off' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND attempts > 0
            AND retry_after IS NOT NULL
            AND last_error = 'redacted pre-visibility recovery failure'
            AND resolved_at IS NULL
            AND poisoned_at IS NULL
        )
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_resolved_check CHECK (
        state <> 'resolved' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NULL
            AND last_error IS NULL
            AND resolved_at IS NOT NULL
            AND poisoned_at IS NULL
        )
    ),
    ADD CONSTRAINT durable_pre_visibility_recovery_poisoned_check CHECK (
        state <> 'poisoned' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND attempts > 0
            AND retry_after IS NULL
            AND last_error = 'redacted pre-visibility recovery failure'
            AND resolved_at IS NULL
            AND poisoned_at IS NOT NULL
        )
    );

CREATE INDEX durable_pre_visibility_recovery_status_idx
    ON durable_pre_visibility_recovery_ledger(repo_id, state, updated_at DESC);

CREATE INDEX durable_pre_visibility_recovery_due_idx
    ON durable_pre_visibility_recovery_ledger(repo_id, state, retry_after, lease_expires_at)
    WHERE state IN ('pending', 'active', 'backing_off');
