-- Provider-free OIDC and refresh-token durable foundation.
--
-- Hosted auth remains disabled by default. This migration stores provider and
-- external identity metadata without raw provider secrets, raw subjects, or raw
-- refresh tokens.

CREATE TABLE IF NOT EXISTS oidc_providers (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider_key TEXT NOT NULL CONSTRAINT oidc_providers_provider_key_check CHECK (
        provider_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
    ),
    display_name TEXT NOT NULL CONSTRAINT oidc_providers_display_name_check CHECK (
        btrim(display_name) <> '' AND length(display_name) <= 255
    ),
    issuer_hash TEXT NOT NULL CONSTRAINT oidc_providers_issuer_hash_check CHECK (
        issuer_hash ~ '^[0-9a-f]{64}$'
    ),
    client_id_hash TEXT NOT NULL CONSTRAINT oidc_providers_client_id_hash_check CHECK (
        client_id_hash ~ '^[0-9a-f]{64}$'
    ),
    client_secret_ref TEXT,
    enabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT oidc_providers_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT oidc_providers_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT oidc_providers_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT oidc_providers_client_secret_ref_check CHECK (
        client_secret_ref IS NULL
        OR client_secret_ref ~ '^(env|vault|secret-manager)://[A-Za-z0-9._~:/@+=,-]{1,480}$'
    ),
    CONSTRAINT oidc_providers_lifecycle_check CHECK (
        disabled_at IS NULL OR disabled_at >= created_at
    ),
    UNIQUE (org_id, provider_key),
    UNIQUE (id, org_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS oidc_providers_org_key_idx
    ON oidc_providers(org_id, provider_key);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = current_schema()
          AND r.relname = 'durable_principals'
          AND c.conname = 'durable_principals_org_repo_uid_key'
    ) THEN
        ALTER TABLE durable_principals
            ADD CONSTRAINT durable_principals_org_repo_uid_key
            UNIQUE (org_id, repo_id, uid);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS external_identities (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    provider_id UUID NOT NULL,
    principal_uid INTEGER NOT NULL,
    subject_hash TEXT NOT NULL,
    username_hint TEXT CONSTRAINT external_identities_username_hint_check CHECK (
        username_hint IS NULL OR length(username_hint) <= 128
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,
    CONSTRAINT external_identities_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT external_identities_provider_org_fk
        FOREIGN KEY (provider_id, org_id) REFERENCES oidc_providers(id, org_id) ON DELETE RESTRICT,
    CONSTRAINT external_identities_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT external_identities_created_at_finite_check CHECK (isfinite(created_at)),
    CONSTRAINT external_identities_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT external_identities_disabled_at_finite_check CHECK (
        disabled_at IS NULL OR isfinite(disabled_at)
    ),
    CONSTRAINT external_identities_subject_hash_check CHECK (subject_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT external_identities_lifecycle_check CHECK (
        disabled_at IS NULL OR disabled_at >= created_at
    ),
    UNIQUE (provider_id, subject_hash),
    UNIQUE (id, org_id, repo_id, principal_uid)
);

CREATE UNIQUE INDEX IF NOT EXISTS external_identities_provider_subject_idx
    ON external_identities(provider_id, subject_hash);

CREATE INDEX IF NOT EXISTS external_identities_principal_idx
    ON external_identities(org_id, repo_id, principal_uid)
    WHERE disabled_at IS NULL;

CREATE TABLE IF NOT EXISTS refresh_token_families (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    principal_uid INTEGER NOT NULL,
    external_identity_id UUID NOT NULL REFERENCES external_identities(id) ON DELETE RESTRICT,
    current_token_version BIGINT NOT NULL DEFAULT 0
        CONSTRAINT refresh_token_families_current_token_version_check CHECK (
            current_token_version >= 0
        ),
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    reuse_detected_at TIMESTAMPTZ,
    CONSTRAINT refresh_token_families_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT refresh_token_families_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT refresh_token_families_external_identity_shape_fk
        FOREIGN KEY (external_identity_id, org_id, repo_id, principal_uid)
        REFERENCES external_identities(id, org_id, repo_id, principal_uid)
        ON DELETE RESTRICT,
    CONSTRAINT refresh_token_families_issued_at_finite_check CHECK (isfinite(issued_at)),
    CONSTRAINT refresh_token_families_updated_at_finite_check CHECK (isfinite(updated_at)),
    CONSTRAINT refresh_token_families_expires_at_finite_check CHECK (isfinite(expires_at)),
    CONSTRAINT refresh_token_families_revoked_at_finite_check CHECK (
        revoked_at IS NULL OR isfinite(revoked_at)
    ),
    CONSTRAINT refresh_token_families_reuse_detected_at_finite_check CHECK (
        reuse_detected_at IS NULL OR isfinite(reuse_detected_at)
    ),
    CONSTRAINT refresh_token_families_lifecycle_check CHECK (
        updated_at >= issued_at
        AND expires_at > issued_at
        AND (revoked_at IS NULL OR revoked_at >= issued_at)
        AND (reuse_detected_at IS NULL OR reuse_detected_at >= issued_at)
    ),
    UNIQUE (id, org_id, repo_id, principal_uid)
);

CREATE UNIQUE INDEX IF NOT EXISTS refresh_token_families_active_principal_idx
    ON refresh_token_families(org_id, repo_id, principal_uid, external_identity_id)
    WHERE revoked_at IS NULL AND reuse_detected_at IS NULL;

CREATE INDEX IF NOT EXISTS refresh_token_families_expiry_idx
    ON refresh_token_families(expires_at)
    WHERE revoked_at IS NULL AND reuse_detected_at IS NULL;

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id UUID PRIMARY KEY,
    family_id UUID NOT NULL,
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL,
    principal_uid INTEGER NOT NULL,
    token_hash TEXT NOT NULL,
    token_version BIGINT NOT NULL CONSTRAINT refresh_tokens_token_version_check CHECK (
        token_version > 0
    ),
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    rotated_at TIMESTAMPTZ,
    rotated_to_token_id UUID,
    revoked_at TIMESTAMPTZ,
    reuse_denied_at TIMESTAMPTZ,
    CONSTRAINT refresh_tokens_org_repo_fk
        FOREIGN KEY (org_id, repo_id) REFERENCES repos(org_id, id) ON DELETE CASCADE,
    CONSTRAINT refresh_tokens_principal_shape_fk
        FOREIGN KEY (org_id, repo_id, principal_uid)
        REFERENCES durable_principals(org_id, repo_id, uid)
        ON DELETE RESTRICT,
    CONSTRAINT refresh_tokens_family_shape_fk
        FOREIGN KEY (family_id, org_id, repo_id, principal_uid)
        REFERENCES refresh_token_families(id, org_id, repo_id, principal_uid)
        ON DELETE CASCADE,
    CONSTRAINT refresh_tokens_family_version_key UNIQUE (family_id, token_version),
    CONSTRAINT refresh_tokens_family_id_key UNIQUE (family_id, id),
    CONSTRAINT refresh_tokens_rotated_to_family_fk
        FOREIGN KEY (family_id, rotated_to_token_id)
        REFERENCES refresh_tokens(family_id, id)
        ON DELETE RESTRICT,
    CONSTRAINT refresh_tokens_token_hash_key UNIQUE (token_hash),
    CONSTRAINT refresh_tokens_token_hash_check CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT refresh_tokens_issued_at_finite_check CHECK (isfinite(issued_at)),
    CONSTRAINT refresh_tokens_expires_at_finite_check CHECK (isfinite(expires_at)),
    CONSTRAINT refresh_tokens_rotated_at_finite_check CHECK (
        rotated_at IS NULL OR isfinite(rotated_at)
    ),
    CONSTRAINT refresh_tokens_revoked_at_finite_check CHECK (
        revoked_at IS NULL OR isfinite(revoked_at)
    ),
    CONSTRAINT refresh_tokens_reuse_denied_at_finite_check CHECK (
        reuse_denied_at IS NULL OR isfinite(reuse_denied_at)
    ),
    CONSTRAINT refresh_tokens_rotation_shape_check CHECK (
        (
            (rotated_at IS NULL AND rotated_to_token_id IS NULL)
            OR (rotated_at IS NOT NULL AND rotated_to_token_id IS NOT NULL)
        )
        AND (rotated_to_token_id IS NULL OR rotated_to_token_id <> id)
    ),
    CONSTRAINT refresh_tokens_lifecycle_check CHECK (
        expires_at > issued_at
        AND (rotated_at IS NULL OR rotated_at >= issued_at)
        AND (revoked_at IS NULL OR revoked_at >= issued_at)
        AND (reuse_denied_at IS NULL OR reuse_denied_at >= issued_at)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS refresh_tokens_family_active_idx
    ON refresh_tokens(family_id)
    WHERE rotated_at IS NULL AND revoked_at IS NULL AND reuse_denied_at IS NULL;

CREATE INDEX IF NOT EXISTS refresh_tokens_principal_active_idx
    ON refresh_tokens(org_id, repo_id, principal_uid, expires_at)
    WHERE rotated_at IS NULL AND revoked_at IS NULL AND reuse_denied_at IS NULL;
