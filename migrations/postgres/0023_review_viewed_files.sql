CREATE TABLE change_request_file_views (
    change_request_id UUID NOT NULL REFERENCES change_requests(id) ON DELETE CASCADE,
    head_commit TEXT NOT NULL CHECK (head_commit ~ '^[0-9a-f]{64}$'),
    path TEXT NOT NULL CHECK (path LIKE '/%' AND path <> '/'),
    viewed_by INTEGER NOT NULL,
    viewed BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (change_request_id, head_commit, path, viewed_by)
);

CREATE INDEX change_request_file_views_change_head_idx
    ON change_request_file_views(change_request_id, head_commit);
