ALTER TABLE idempotency_records
    ADD COLUMN replay_classification TEXT,
    ADD COLUMN quota_repo_id TEXT,
    ADD COLUMN quota_workspace_id TEXT,
    ADD COLUMN quota_principal_uid BIGINT,
    ADD COLUMN retention_deferred_at TIMESTAMPTZ;

UPDATE idempotency_records
SET replay_classification = 'secret_free'
WHERE state = 'completed'
  AND replay_classification IS NULL;

UPDATE idempotency_records
SET quota_repo_id = substring(scope FROM '^repo:([^:[:space:]]+)')
WHERE quota_repo_id IS NULL
  AND scope ~ '^repo:[^:[:space:]]+';

UPDATE idempotency_records
SET quota_workspace_id = substring(scope FROM 'workspace:([^:[:space:]]+)')
WHERE quota_workspace_id IS NULL
  AND scope ~ 'workspace:[^:[:space:]]+';

ALTER TABLE idempotency_records
    ADD CONSTRAINT idempotency_records_replay_classification_check
        CHECK (
            replay_classification IS NULL
            OR replay_classification IN ('secret_free', 'partial')
        ),
    ADD CONSTRAINT idempotency_records_quota_principal_uid_check
        CHECK (quota_principal_uid IS NULL OR quota_principal_uid >= 0),
    ADD CONSTRAINT idempotency_records_completed_replay_classification_check
        CHECK (state <> 'completed' OR replay_classification IS NOT NULL);

CREATE INDEX idempotency_records_scope_state_created_idx
    ON idempotency_records(scope, state, created_at, reserved_at, completed_at);

CREATE INDEX idempotency_records_repo_quota_idx
    ON idempotency_records(quota_repo_id, state, created_at)
    WHERE quota_repo_id IS NOT NULL;

CREATE INDEX idempotency_records_workspace_quota_idx
    ON idempotency_records(quota_workspace_id, state, created_at)
    WHERE quota_workspace_id IS NOT NULL;

CREATE INDEX idempotency_records_principal_quota_idx
    ON idempotency_records(quota_principal_uid, state, created_at)
    WHERE quota_principal_uid IS NOT NULL;

CREATE INDEX idempotency_records_completed_retention_idx
    ON idempotency_records((COALESCE(retention_deferred_at, completed_at)), scope, key_hash)
    WHERE state = 'completed';

CREATE INDEX idempotency_records_pending_retention_idx
    ON idempotency_records(reserved_at, scope, key_hash)
    WHERE state = 'pending';
