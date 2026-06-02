-- SCIM provisioning durable foundation.
--
-- SCIM provisioning is disabled by default. This migration stores only
-- lower-hex SHA-256 hashes and bounded hints/references for clients, users,
-- groups, and memberships.

CREATE TABLE IF NOT EXISTS scim_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    provider_key TEXT NOT NULL CONSTRAINT scim_clients_provider_key_check CHECK (
        provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
    ),
    token_hash TEXT NOT NULL CONSTRAINT scim_clients_token_hash_check CHECK (
        token_hash ~ '^[0-9a-f]{64}$'
    ),
    enabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT scim_clients_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT scim_clients_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT scim_clients_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT scim_clients_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT scim_clients_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (org_id, repo_id, provider_key),
    UNIQUE (id, org_id, repo_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS scim_clients_org_repo_key_idx
    ON scim_clients(org_id, repo_id, provider_key);

CREATE INDEX IF NOT EXISTS scim_clients_enabled_idx
    ON scim_clients(org_id, repo_id, enabled, provider_key);

CREATE TABLE IF NOT EXISTS scim_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    client_id UUID NOT NULL,
    principal_uid INTEGER NOT NULL,
    external_id_hash TEXT NOT NULL CONSTRAINT scim_users_external_id_hash_check CHECK (
        external_id_hash ~ '^[0-9a-f]{64}$'
    ),
    username_hint TEXT CONSTRAINT scim_users_username_hint_check CHECK (
        username_hint IS NULL OR length(username_hint) <= 128
    ),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT scim_users_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT scim_users_client_shape_fk
        FOREIGN KEY (client_id, org_id, repo_id)
        REFERENCES scim_clients(id, org_id, repo_id)
        ON DELETE RESTRICT,
    CONSTRAINT scim_users_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT scim_users_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT scim_users_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT scim_users_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT scim_users_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (client_id, external_id_hash),
    UNIQUE (id, org_id, repo_id, principal_uid)
);

CREATE UNIQUE INDEX IF NOT EXISTS scim_users_client_external_id_idx
    ON scim_users(client_id, external_id_hash);

CREATE INDEX IF NOT EXISTS scim_users_principal_idx
    ON scim_users(org_id, repo_id, principal_uid)
    WHERE active AND disabled_at IS NULL;

CREATE TABLE IF NOT EXISTS scim_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    client_id UUID NOT NULL,
    local_gid INTEGER NOT NULL CONSTRAINT scim_groups_local_gid_check CHECK (
        local_gid >= 0
    ),
    external_id_hash TEXT NOT NULL CONSTRAINT scim_groups_external_id_hash_check CHECK (
        external_id_hash ~ '^[0-9a-f]{64}$'
    ),
    display_name TEXT CONSTRAINT scim_groups_display_name_check CHECK (
        display_name IS NULL OR (btrim(display_name) <> '' AND length(display_name) <= 255)
    ),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT scim_groups_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT scim_groups_client_shape_fk
        FOREIGN KEY (client_id, org_id, repo_id)
        REFERENCES scim_clients(id, org_id, repo_id)
        ON DELETE CASCADE,
    CONSTRAINT scim_groups_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT scim_groups_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT scim_groups_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT scim_groups_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (client_id, external_id_hash),
    UNIQUE (id, org_id, repo_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS scim_groups_client_external_id_idx
    ON scim_groups(client_id, external_id_hash);

CREATE INDEX IF NOT EXISTS scim_groups_local_gid_idx
    ON scim_groups(org_id, repo_id, local_gid)
    WHERE active AND disabled_at IS NULL;

CREATE TABLE IF NOT EXISTS scim_group_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    group_id UUID NOT NULL,
    principal_uid INTEGER NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT scim_group_members_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT scim_group_members_group_shape_fk
        FOREIGN KEY (group_id, org_id, repo_id)
        REFERENCES scim_groups(id, org_id, repo_id)
        ON DELETE CASCADE,
    CONSTRAINT scim_group_members_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT scim_group_members_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT scim_group_members_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT scim_group_members_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT scim_group_members_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (group_id, principal_uid)
);

CREATE UNIQUE INDEX IF NOT EXISTS scim_group_members_group_principal_idx
    ON scim_group_members(group_id, principal_uid);

CREATE INDEX IF NOT EXISTS scim_group_members_principal_idx
    ON scim_group_members(org_id, repo_id, principal_uid)
    WHERE active AND disabled_at IS NULL;
