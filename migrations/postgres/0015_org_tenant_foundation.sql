-- Conservative hosted tenant metadata foundation.
--
-- Hosted multi-tenant serving remains disabled by default. This migration makes
-- the durable schema able to represent org -> repo -> workspace identity while
-- preserving legacy local singleton rows that intentionally have no org.

CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_-]{1,128}$'),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> '' AND length(display_name) <= 255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ,
    CONSTRAINT organizations_archived_at_finite_check CHECK (
        archived_at IS NULL OR isfinite(archived_at)
    )
);

INSERT INTO organizations (id, display_name)
VALUES ('default_org', 'Default organization')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS org_memberships (
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    principal_uid INTEGER NOT NULL CHECK (principal_uid >= 0),
    role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member')),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, principal_uid),
    CONSTRAINT org_memberships_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT org_memberships_updated_at_finite_check CHECK (isfinite(updated_at))
);

CREATE INDEX IF NOT EXISTS org_memberships_principal_idx
    ON org_memberships(principal_uid, org_id)
    WHERE active;

CREATE TABLE IF NOT EXISTS org_service_accounts (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (btrim(name) <> '' AND length(name) <= 128),
    principal_uid INTEGER CHECK (principal_uid IS NULL OR principal_uid >= 0),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT org_service_accounts_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT org_service_accounts_updated_at_finite_check CHECK (isfinite(updated_at)),
    UNIQUE (org_id, name),
    UNIQUE (org_id, principal_uid)
);

CREATE INDEX IF NOT EXISTS org_service_accounts_org_active_idx
    ON org_service_accounts(org_id, active, name);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'repos'
          AND column_name = 'org_id'
    ) THEN
        ALTER TABLE repos ADD COLUMN org_id TEXT;
    END IF;
END $$;

INSERT INTO organizations (id, display_name)
SELECT DISTINCT org_id, org_id
FROM repos
WHERE org_id IS NOT NULL
ON CONFLICT (id) DO NOTHING;

UPDATE repos SET org_id = 'default_org' WHERE org_id IS NULL;

ALTER TABLE repos
    ALTER COLUMN org_id SET DEFAULT 'default_org',
    ALTER COLUMN org_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'repos'
          AND c.conname = 'repos_org_id_fk'
    ) THEN
        ALTER TABLE repos
            ADD CONSTRAINT repos_org_id_fk
            FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE RESTRICT;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'repos'
          AND c.conname = 'repos_org_id_id_key'
    ) THEN
        ALTER TABLE repos
            ADD CONSTRAINT repos_org_id_id_key UNIQUE (org_id, id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS repos_org_id_idx ON repos(org_id, id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'workspaces'
          AND column_name = 'org_id'
    ) THEN
        ALTER TABLE workspaces ADD COLUMN org_id TEXT;
    END IF;
END $$;

UPDATE workspaces
SET org_id = repos.org_id
FROM repos
WHERE workspaces.repo_id = repos.id
  AND workspaces.org_id IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspaces'
          AND c.conname = 'workspaces_org_id_fk'
    ) THEN
        ALTER TABLE workspaces
            ADD CONSTRAINT workspaces_org_id_fk
            FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspaces'
          AND c.conname = 'workspaces_org_repo_fk'
    ) THEN
        ALTER TABLE workspaces
            ADD CONSTRAINT workspaces_org_repo_fk
            FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspaces'
          AND c.conname = 'workspaces_id_org_id_key'
    ) THEN
        ALTER TABLE workspaces
            ADD CONSTRAINT workspaces_id_org_id_key UNIQUE (id, org_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspaces'
          AND c.conname = 'workspaces_org_repo_shape_check'
    ) THEN
        ALTER TABLE workspaces
            ADD CONSTRAINT workspaces_org_repo_shape_check
            CHECK (
                (repo_id IS NULL AND org_id IS NULL)
                OR (repo_id IS NOT NULL AND org_id IS NOT NULL)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS workspaces_org_repo_idx ON workspaces(org_id, repo_id, id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'workspace_tokens'
          AND column_name = 'org_id'
    ) THEN
        ALTER TABLE workspace_tokens ADD COLUMN org_id TEXT;
    END IF;
END $$;

UPDATE workspace_tokens
SET org_id = workspaces.org_id
FROM workspaces
WHERE workspace_tokens.workspace_id = workspaces.id
  AND workspace_tokens.org_id IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspace_tokens'
          AND c.conname = 'workspace_tokens_org_id_fk'
    ) THEN
        ALTER TABLE workspace_tokens
            ADD CONSTRAINT workspace_tokens_org_id_fk
            FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspace_tokens'
          AND c.conname = 'workspace_tokens_workspace_org_fk'
    ) THEN
        ALTER TABLE workspace_tokens
            ADD CONSTRAINT workspace_tokens_workspace_org_fk
            FOREIGN KEY (workspace_id, org_id) REFERENCES workspaces(id, org_id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'workspace_tokens'
          AND c.conname = 'workspace_tokens_org_repo_shape_check'
    ) THEN
        ALTER TABLE workspace_tokens
            ADD CONSTRAINT workspace_tokens_org_repo_shape_check
            CHECK (
                (repo_id IS NULL AND org_id IS NULL)
                OR (repo_id IS NOT NULL AND org_id IS NOT NULL)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS workspace_tokens_org_repo_idx
    ON workspace_tokens(org_id, repo_id, workspace_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'durable_principals'
          AND column_name = 'org_id'
    ) THEN
        ALTER TABLE durable_principals ADD COLUMN org_id TEXT;
    END IF;
END $$;

UPDATE durable_principals
SET org_id = repos.org_id
FROM repos
WHERE durable_principals.repo_id = repos.id
  AND durable_principals.org_id IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'durable_principals'
          AND c.conname = 'durable_principals_org_id_fk'
    ) THEN
        ALTER TABLE durable_principals
            ADD CONSTRAINT durable_principals_org_id_fk
            FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'durable_principals'
          AND c.conname = 'durable_principals_org_repo_fk'
    ) THEN
        ALTER TABLE durable_principals
            ADD CONSTRAINT durable_principals_org_repo_fk
            FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'durable_principals'
          AND c.conname = 'durable_principals_org_repo_shape_check'
    ) THEN
        ALTER TABLE durable_principals
            ADD CONSTRAINT durable_principals_org_repo_shape_check
            CHECK (
                (repo_id IS NULL AND org_id IS NULL)
                OR (repo_id IS NOT NULL AND org_id IS NOT NULL)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS durable_principals_org_repo_idx
    ON durable_principals(org_id, repo_id, uid);
