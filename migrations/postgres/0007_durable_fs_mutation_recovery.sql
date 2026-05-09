-- Persisted recovery ledger for durable filesystem mutations whose tree/ref
-- update is already visible, but route side effects still need repair.

CREATE TABLE durable_fs_mutation_recovery_ledger (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    workspace_scope TEXT NOT NULL CHECK (
        btrim(workspace_scope) <> ''
        AND length(workspace_scope) <= 256
        AND workspace_scope !~ '[[:cntrl:]]'
    ),
    operation_id TEXT NOT NULL CHECK (
        btrim(operation_id) <> ''
        AND length(operation_id) <= 128
        AND operation_id !~ '[[:cntrl:]]'
    ),
    target_ref TEXT NOT NULL CHECK (
        btrim(target_ref) <> ''
        AND length(target_ref) <= 255
        AND target_ref !~ '[[:cntrl:]]'
    ),
    previous_commit_id TEXT NOT NULL CHECK (previous_commit_id ~ '^[0-9a-f]{64}$'),
    new_commit_id TEXT NOT NULL CHECK (new_commit_id ~ '^[0-9a-f]{64}$'),
    failed_step TEXT NOT NULL CHECK (
        failed_step IN ('workspace_completion', 'audit_append', 'idempotency_completion')
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
    envelope_json JSONB NOT NULL CHECK (jsonb_typeof(envelope_json) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        repo_id,
        workspace_scope,
        operation_id,
        target_ref,
        previous_commit_id,
        new_commit_id,
        failed_step
    ),
    FOREIGN KEY (repo_id, previous_commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    FOREIGN KEY (repo_id, new_commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    CONSTRAINT durable_fs_mutation_recovery_pending_check CHECK (
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
    CONSTRAINT durable_fs_mutation_recovery_active_check CHECK (
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
    CONSTRAINT durable_fs_mutation_recovery_backoff_check CHECK (
        state <> 'backing_off' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NOT NULL
            AND last_error = 'redacted durable FS mutation recovery failure'
            AND completed_at IS NULL
            AND poisoned_at IS NULL
            AND attempts > 0
        )
    ),
    CONSTRAINT durable_fs_mutation_recovery_completed_check CHECK (
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
    CONSTRAINT durable_fs_mutation_recovery_poisoned_check CHECK (
        state <> 'poisoned' OR (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND retry_after IS NULL
            AND last_error = 'redacted durable FS mutation recovery failure'
            AND completed_at IS NULL
            AND poisoned_at IS NOT NULL
            AND attempts > 0
        )
    )
);

CREATE INDEX durable_fs_mutation_recovery_due_idx
    ON durable_fs_mutation_recovery_ledger(repo_id, state, retry_after, lease_expires_at)
    WHERE state IN ('pending', 'active', 'backing_off');
