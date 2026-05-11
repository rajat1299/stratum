-- Durable auth/session foundation schema.
--
-- This migration adds durable principal identity and workspace-token lifecycle
-- metadata without requiring existing compatibility tokens to have a durable
-- principal row.

CREATE TABLE durable_principals (
    uid INTEGER PRIMARY KEY CHECK (uid >= 0),
    repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    username TEXT NOT NULL CHECK (btrim(username) <> '' AND length(username) <= 128),
    primary_gid INTEGER NOT NULL CHECK (primary_gid >= 0),
    groups_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    kind TEXT NOT NULL CHECK (kind IN ('human', 'service_account', 'agent')),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repo_id, username)
);

ALTER TABLE workspace_tokens
    ADD COLUMN repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    ADD COLUMN principal_uid INTEGER,
    ADD COLUMN token_version BIGINT NOT NULL DEFAULT 1 CHECK (token_version > 0),
    ADD COLUMN issued_at TIMESTAMPTZ,
    ADD COLUMN updated_at TIMESTAMPTZ,
    ADD COLUMN expires_at TIMESTAMPTZ,
    ADD COLUMN revoked_at TIMESTAMPTZ,
    ADD CONSTRAINT workspace_tokens_issued_at_finite_check
        CHECK (isfinite(issued_at)),
    ADD CONSTRAINT workspace_tokens_updated_at_finite_check
        CHECK (isfinite(updated_at)),
    ADD CONSTRAINT workspace_tokens_expires_at_finite_check
        CHECK (expires_at IS NULL OR isfinite(expires_at)),
    ADD CONSTRAINT workspace_tokens_revoked_at_finite_check
        CHECK (revoked_at IS NULL OR isfinite(revoked_at)),
    ADD CONSTRAINT workspace_tokens_lifecycle_check
        CHECK (revoked_at IS NULL OR revoked_at >= issued_at),
    ADD CONSTRAINT workspace_tokens_expiry_check
        CHECK (expires_at IS NULL OR expires_at > issued_at);

UPDATE workspace_tokens
SET principal_uid = agent_uid,
    repo_id = (
        SELECT workspaces.repo_id
        FROM workspaces
        WHERE workspaces.id = workspace_tokens.workspace_id
    ),
    issued_at = created_at,
    updated_at = created_at
WHERE principal_uid IS NULL;

UPDATE workspace_tokens
SET issued_at = created_at
WHERE issued_at IS NULL;

UPDATE workspace_tokens
SET updated_at = issued_at
WHERE updated_at IS NULL;

ALTER TABLE workspace_tokens
    ALTER COLUMN issued_at SET NOT NULL,
    ALTER COLUMN issued_at SET DEFAULT now(),
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN updated_at SET DEFAULT now();

CREATE INDEX workspace_tokens_workspace_active_idx
    ON workspace_tokens(workspace_id, revoked_at, expires_at);

CREATE INDEX workspace_tokens_repo_principal_idx
    ON workspace_tokens(repo_id, principal_uid);
