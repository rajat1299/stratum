-- Provider-free SAML SSO durable foundation.
--
-- Hosted auth remains disabled by default. This migration stores only hashes or
-- bounded references for SAML provider metadata, external identities, group
-- mappings, and assertion replay protection.

CREATE TABLE IF NOT EXISTS saml_providers (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    provider_key TEXT NOT NULL CONSTRAINT saml_providers_provider_key_check CHECK (
        provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
    ),
    display_name TEXT NOT NULL CONSTRAINT saml_providers_display_name_check CHECK (
        btrim(display_name) <> '' AND length(display_name) <= 255
    ),
    idp_entity_id_hash TEXT NOT NULL CONSTRAINT saml_providers_idp_entity_id_hash_check CHECK (
        idp_entity_id_hash ~ '^[0-9a-f]{64}$'
    ),
    sp_entity_id_hash TEXT NOT NULL CONSTRAINT saml_providers_sp_entity_id_hash_check CHECK (
        sp_entity_id_hash ~ '^[0-9a-f]{64}$'
    ),
    acs_url_hash TEXT NOT NULL CONSTRAINT saml_providers_acs_url_hash_check CHECK (
        acs_url_hash ~ '^[0-9a-f]{64}$'
    ),
    audience_hash TEXT NOT NULL CONSTRAINT saml_providers_audience_hash_check CHECK (
        audience_hash ~ '^[0-9a-f]{64}$'
    ),
    signing_cert_ref_hash TEXT NOT NULL
        CONSTRAINT saml_providers_signing_cert_ref_hash_check CHECK (
            signing_cert_ref_hash ~ '^[0-9a-f]{64}$'
        ),
    binding TEXT NOT NULL DEFAULT 'post' CONSTRAINT saml_providers_binding_check CHECK (
        binding = 'post'
    ),
    group_attribute TEXT CONSTRAINT saml_providers_group_attribute_check CHECK (
        group_attribute IS NULL
        OR (btrim(group_attribute) <> '' AND length(group_attribute) <= 128)
    ),
    required_group_hash TEXT CONSTRAINT saml_providers_required_group_hash_check CHECK (
        required_group_hash IS NULL OR required_group_hash ~ '^[0-9a-f]{64}$'
    ),
    enabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT saml_providers_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT saml_providers_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT saml_providers_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT saml_providers_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT saml_providers_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (org_id, repo_id, provider_key),
    UNIQUE (id, org_id, repo_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS saml_providers_org_repo_key_idx
    ON saml_providers(org_id, repo_id, provider_key);

CREATE INDEX IF NOT EXISTS saml_providers_enabled_idx
    ON saml_providers(org_id, repo_id, enabled, provider_key);

CREATE TABLE IF NOT EXISTS saml_external_identities (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    provider_id UUID NOT NULL,
    principal_uid INTEGER NOT NULL,
    name_id_hash TEXT NOT NULL CONSTRAINT saml_external_identities_name_id_hash_check CHECK (
        name_id_hash ~ '^[0-9a-f]{64}$'
    ),
    username_hint TEXT CONSTRAINT saml_external_identities_username_hint_check CHECK (
        username_hint IS NULL OR length(username_hint) <= 128
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT saml_external_identities_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT saml_external_identities_provider_shape_fk
        FOREIGN KEY (provider_id, org_id, repo_id)
        REFERENCES saml_providers(id, org_id, repo_id)
        ON DELETE RESTRICT,
    CONSTRAINT saml_external_identities_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT saml_external_identities_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT saml_external_identities_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT saml_external_identities_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT saml_external_identities_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (provider_id, name_id_hash),
    UNIQUE (id, org_id, repo_id, principal_uid)
);

CREATE UNIQUE INDEX IF NOT EXISTS saml_external_identities_provider_name_id_idx
    ON saml_external_identities(provider_id, name_id_hash);

CREATE INDEX IF NOT EXISTS saml_external_identities_principal_idx
    ON saml_external_identities(org_id, repo_id, principal_uid)
    WHERE disabled_at IS NULL;

CREATE TABLE IF NOT EXISTS saml_group_mappings (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    provider_id UUID NOT NULL,
    external_group_hash TEXT NOT NULL
        CONSTRAINT saml_group_mappings_external_group_hash_check CHECK (
            external_group_hash ~ '^[0-9a-f]{64}$'
        ),
    external_group_name_ref TEXT
        CONSTRAINT saml_group_mappings_external_group_name_ref_check CHECK (
            external_group_name_ref IS NULL
            OR external_group_name_ref ~ '^(env|vault|secret-manager)://[A-Za-z0-9._~:/@+=,-]{1,480}$'
        ),
    local_gid INTEGER NOT NULL CONSTRAINT saml_group_mappings_local_gid_check CHECK (
        local_gid >= 0
    ),
    required BOOLEAN NOT NULL DEFAULT false,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT saml_group_mappings_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT saml_group_mappings_provider_shape_fk
        FOREIGN KEY (provider_id, org_id, repo_id)
        REFERENCES saml_providers(id, org_id, repo_id)
        ON DELETE CASCADE,
    CONSTRAINT saml_group_mappings_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT saml_group_mappings_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT saml_group_mappings_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT saml_group_mappings_lifecycle_check CHECK (
        updated_at >= created_at
        AND (disabled_at IS NULL OR disabled_at >= created_at)
    ),
    UNIQUE (provider_id, external_group_hash),
    UNIQUE (provider_id, local_gid, external_group_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS saml_group_mappings_provider_group_idx
    ON saml_group_mappings(provider_id, external_group_hash);

CREATE INDEX IF NOT EXISTS saml_group_mappings_local_gid_idx
    ON saml_group_mappings(org_id, repo_id, local_gid)
    WHERE active AND disabled_at IS NULL;

CREATE TABLE IF NOT EXISTS saml_assertion_replay (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    provider_id UUID NOT NULL,
    principal_uid INTEGER NOT NULL,
    assertion_id_hash TEXT NOT NULL
        CONSTRAINT saml_assertion_replay_assertion_id_hash_check CHECK (
            assertion_id_hash ~ '^[0-9a-f]{64}$'
        ),
    replay_key TEXT NOT NULL CONSTRAINT saml_assertion_replay_replay_key_check CHECK (
        replay_key ~ '^[0-9a-f]{64}$'
    ),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT saml_assertion_replay_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT saml_assertion_replay_provider_shape_fk
        FOREIGN KEY (provider_id, org_id, repo_id)
        REFERENCES saml_providers(id, org_id, repo_id)
        ON DELETE CASCADE,
    CONSTRAINT saml_assertion_replay_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT saml_assertion_replay_provider_assertion_key UNIQUE (
        provider_id,
        assertion_id_hash
    ),
    CONSTRAINT saml_assertion_replay_replay_key_key UNIQUE (replay_key),
    CONSTRAINT saml_assertion_replay_first_seen_at_finite_check CHECK (
        isfinite(first_seen_at)
    ),
    CONSTRAINT saml_assertion_replay_expires_at_finite_check CHECK (isfinite(expires_at)),
    CONSTRAINT saml_assertion_replay_lifecycle_check CHECK (
        expires_at > first_seen_at
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS saml_assertion_replay_key_idx
    ON saml_assertion_replay(replay_key);

CREATE UNIQUE INDEX IF NOT EXISTS saml_assertion_replay_provider_assertion_idx
    ON saml_assertion_replay(provider_id, assertion_id_hash);

CREATE INDEX IF NOT EXISTS saml_assertion_replay_expiry_idx
    ON saml_assertion_replay(expires_at);
