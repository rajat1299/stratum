-- Durable backend metadata foundation for future Postgres-backed Stratum deployments.
--
-- This migration is intentionally not wired into the local runtime yet. It records
-- the first production metadata contract while the server continues to use the
-- existing local stores.

CREATE TABLE repos (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    default_ref TEXT NOT NULL DEFAULT 'main',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ
);

CREATE TABLE objects (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('blob', 'tree', 'commit')),
    object_id TEXT NOT NULL,
    object_key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, object_id),
    UNIQUE (repo_id, kind, object_id),
    UNIQUE (repo_id, object_key)
);

CREATE TABLE commits (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    root_tree_kind TEXT NOT NULL DEFAULT 'tree' CHECK (root_tree_kind = 'tree'),
    root_tree_id TEXT NOT NULL,
    author TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_paths_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id, root_tree_kind, root_tree_id) REFERENCES objects(repo_id, kind, object_id)
);

CREATE TABLE commit_parents (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL,
    parent_commit_id TEXT NOT NULL,
    parent_order INTEGER NOT NULL CHECK (parent_order >= 0),
    PRIMARY KEY (repo_id, commit_id, parent_order),
    FOREIGN KEY (repo_id, commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    FOREIGN KEY (repo_id, parent_commit_id) REFERENCES commits(repo_id, id)
);

CREATE TABLE refs (
    repo_id TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    commit_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, name),
    FOREIGN KEY (repo_id, commit_id) REFERENCES commits(repo_id, id)
);

-- Ref compare-and-swap must be one transaction:
-- UPDATE refs
-- SET commit_id = $new_commit, version = version + 1, updated_at = now()
-- WHERE repo_id = $repo_id AND name = $name
--   AND commit_id = $expected_commit
--   AND version = $expected_version;
--
-- Source-checked ref updates must lock the source and target rows in the same
-- transaction, for example with SELECT ... FOR UPDATE before the target update,
-- or include the source target predicate in the update statement. A plain read
-- of the source row is not enough under READ COMMITTED isolation.

CREATE TABLE idempotency_records (
    scope TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'completed')),
    status_code INTEGER CHECK (status_code BETWEEN 100 AND 599),
    response_body_json JSONB,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (scope, key_hash),
    CHECK (
        (state = 'pending' AND status_code IS NULL AND response_body_json IS NULL AND completed_at IS NULL)
        OR
        (state = 'completed' AND status_code IS NOT NULL AND response_body_json IS NOT NULL AND completed_at IS NOT NULL)
    )
);

CREATE TABLE audit_events (
    id UUID PRIMARY KEY,
    repo_id TEXT REFERENCES repos(id) ON DELETE SET NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor_json JSONB NOT NULL,
    workspace_json JSONB,
    action TEXT NOT NULL,
    resource_json JSONB NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('success', 'partial')),
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (repo_id, sequence)
);

CREATE TABLE workspaces (
    id UUID PRIMARY KEY,
    repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL,
    head_commit TEXT,
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    base_ref TEXT NOT NULL DEFAULT 'main',
    session_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workspace_tokens (
    id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    agent_uid INTEGER NOT NULL,
    secret_hash TEXT NOT NULL,
    read_prefixes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    write_prefixes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, secret_hash)
);

CREATE TABLE protected_ref_rules (
    id UUID PRIMARY KEY,
    repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    ref_name TEXT NOT NULL,
    required_approvals INTEGER NOT NULL CHECK (required_approvals > 0),
    created_by INTEGER NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE protected_path_rules (
    id UUID PRIMARY KEY,
    repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    path_prefix TEXT NOT NULL,
    target_ref TEXT,
    required_approvals INTEGER NOT NULL CHECK (required_approvals > 0),
    created_by INTEGER NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE change_requests (
    id UUID PRIMARY KEY,
    repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    description TEXT,
    source_ref TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    head_commit TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'merged', 'rejected')),
    created_by INTEGER NOT NULL,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE approvals (
    id UUID PRIMARY KEY,
    change_request_id UUID NOT NULL REFERENCES change_requests(id) ON DELETE CASCADE,
    head_commit TEXT NOT NULL,
    approved_by INTEGER NOT NULL,
    comment TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    dismissed_by INTEGER,
    dismissal_reason TEXT,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE review_comments (
    id UUID PRIMARY KEY,
    change_request_id UUID NOT NULL REFERENCES change_requests(id) ON DELETE CASCADE,
    author INTEGER NOT NULL,
    body TEXT NOT NULL,
    path TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('general', 'changes_requested')),
    active BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE reviewer_assignments (
    id UUID PRIMARY KEY,
    change_request_id UUID NOT NULL REFERENCES change_requests(id) ON DELETE CASCADE,
    reviewer INTEGER NOT NULL,
    assigned_by INTEGER NOT NULL,
    required BOOLEAN NOT NULL DEFAULT true,
    active BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (change_request_id, reviewer)
);
