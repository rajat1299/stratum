CREATE TABLE IF NOT EXISTS sparse_cache_config (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sparse_cache_views (
    view_id INTEGER PRIMARY KEY,
    repo_id TEXT NOT NULL,
    root_tree_id TEXT NOT NULL,
    commit_id TEXT,
    ref_name TEXT,
    ref_version INTEGER,
    created_at_unix_nanos INTEGER NOT NULL DEFAULT 0,
    CHECK (
        (ref_name IS NULL AND ref_version IS NULL)
        OR (ref_name IS NOT NULL AND ref_version IS NOT NULL)
    ),
    UNIQUE (repo_id, root_tree_id, commit_id, ref_name, ref_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS sparse_cache_views_identity_idx
ON sparse_cache_views (
    repo_id,
    root_tree_id,
    COALESCE(commit_id, ''),
    COALESCE(ref_name, ''),
    COALESCE(ref_version, -1)
);

CREATE TABLE IF NOT EXISTS sparse_cache_inodes (
    view_id INTEGER NOT NULL,
    inode_id INTEGER NOT NULL,
    node_kind TEXT NOT NULL,
    object_id TEXT,
    object_kind TEXT,
    mode INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    nlink INTEGER NOT NULL,
    size INTEGER NOT NULL,
    block_size INTEGER NOT NULL,
    blocks INTEGER NOT NULL,
    mtime_secs INTEGER NOT NULL,
    mtime_nanos INTEGER NOT NULL,
    ctime_secs INTEGER NOT NULL,
    ctime_nanos INTEGER NOT NULL,
    mime_type TEXT,
    custom_attrs_json TEXT NOT NULL DEFAULT '{}',
    lookup_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (view_id, inode_id),
    FOREIGN KEY (view_id) REFERENCES sparse_cache_views (view_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sparse_cache_dentries (
    view_id INTEGER NOT NULL,
    parent_inode_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    child_inode_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (view_id, parent_inode_id, name),
    UNIQUE (view_id, path),
    FOREIGN KEY (view_id, parent_inode_id)
        REFERENCES sparse_cache_inodes (view_id, inode_id) ON DELETE CASCADE,
    FOREIGN KEY (view_id, child_inode_id)
        REFERENCES sparse_cache_inodes (view_id, inode_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sparse_cache_chunks (
    repo_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    byte_len INTEGER NOT NULL,
    bytes BLOB NOT NULL,
    PRIMARY KEY (repo_id, object_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS sparse_cache_symlinks (
    view_id INTEGER NOT NULL,
    inode_id INTEGER NOT NULL,
    target TEXT NOT NULL,
    target_object_id TEXT,
    PRIMARY KEY (view_id, inode_id),
    FOREIGN KEY (view_id, inode_id)
        REFERENCES sparse_cache_inodes (view_id, inode_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sparse_cache_statfs (
    view_id INTEGER PRIMARY KEY,
    inode_count INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    directory_count INTEGER NOT NULL,
    symlink_count INTEGER NOT NULL,
    bytes_used INTEGER NOT NULL,
    blocks_used INTEGER NOT NULL,
    block_size INTEGER NOT NULL,
    FOREIGN KEY (view_id) REFERENCES sparse_cache_views (view_id) ON DELETE CASCADE
);
