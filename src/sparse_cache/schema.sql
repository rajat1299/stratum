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
    CHECK (length(repo_id) > 0),
    CHECK (length(root_tree_id) = 64),
    CHECK (commit_id IS NULL OR length(commit_id) = 64),
    CHECK (ref_version IS NULL OR (typeof(ref_version) = 'integer' AND ref_version > 0)),
    CHECK (typeof(created_at_unix_nanos) = 'integer' AND created_at_unix_nanos >= 0),
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

CREATE TRIGGER IF NOT EXISTS sparse_cache_views_ref_version_insert_check
BEFORE INSERT ON sparse_cache_views
WHEN NEW.ref_version IS NOT NULL AND NEW.ref_version <= 0
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid ref version');
END;

CREATE TRIGGER IF NOT EXISTS sparse_cache_views_ref_version_update_check
BEFORE UPDATE OF ref_version ON sparse_cache_views
WHEN NEW.ref_version IS NOT NULL AND NEW.ref_version <= 0
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid ref version');
END;

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
    size_known INTEGER NOT NULL DEFAULT 1,
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
    CHECK (typeof(inode_id) = 'integer' AND inode_id >= 0),
    CHECK (node_kind IN ('file', 'directory', 'symlink')),
    CHECK (object_id IS NULL OR length(object_id) = 64),
    CHECK (object_kind IS NULL OR object_kind IN ('blob', 'tree', 'commit')),
    CHECK (
        (object_id IS NULL AND object_kind IS NULL)
        OR (object_id IS NOT NULL AND object_kind IS NOT NULL)
    ),
    CHECK (typeof(mode) = 'integer' AND mode >= 0 AND mode <= 4294967295),
    CHECK (typeof(uid) = 'integer' AND uid >= 0 AND uid <= 4294967295),
    CHECK (typeof(gid) = 'integer' AND gid >= 0 AND gid <= 4294967295),
    CHECK (typeof(nlink) = 'integer' AND nlink >= 0),
    CHECK (typeof(size) = 'integer' AND size >= 0),
    CHECK (typeof(size_known) = 'integer' AND size_known IN (0, 1)),
    CHECK (size_known = 1 OR size = 0),
    CHECK (typeof(block_size) = 'integer' AND block_size > 0),
    CHECK (typeof(blocks) = 'integer' AND blocks >= 0),
    CHECK (typeof(mtime_secs) = 'integer' AND mtime_secs >= 0),
    CHECK (typeof(mtime_nanos) = 'integer' AND mtime_nanos BETWEEN 0 AND 999999999),
    CHECK (typeof(ctime_secs) = 'integer' AND ctime_secs >= 0),
    CHECK (typeof(ctime_nanos) = 'integer' AND ctime_nanos BETWEEN 0 AND 999999999),
    CHECK (typeof(lookup_count) = 'integer' AND lookup_count >= 0),
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
    CHECK (typeof(parent_inode_id) = 'integer' AND parent_inode_id >= 0),
    CHECK (typeof(child_inode_id) = 'integer' AND child_inode_id >= 0),
    CHECK (
        length(name) BETWEEN 1 AND 255
        AND name NOT IN ('.', '..')
        AND instr(name, '/') = 0
        AND instr(name, char(0)) = 0
    ),
    CHECK (length(path) > 0 AND substr(path, 1, 1) = '/' AND instr(path, char(0)) = 0),
    FOREIGN KEY (view_id, parent_inode_id)
        REFERENCES sparse_cache_inodes (view_id, inode_id) ON DELETE CASCADE,
    FOREIGN KEY (view_id, child_inode_id)
        REFERENCES sparse_cache_inodes (view_id, inode_id) ON DELETE CASCADE
);

CREATE TRIGGER IF NOT EXISTS sparse_cache_inodes_size_known_insert_check
BEFORE INSERT ON sparse_cache_inodes
WHEN NEW.size_known NOT IN (0, 1)
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid size_known');
END;

CREATE TRIGGER IF NOT EXISTS sparse_cache_inodes_size_known_update_check
BEFORE UPDATE OF size_known ON sparse_cache_inodes
WHEN NEW.size_known NOT IN (0, 1)
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid size_known');
END;

CREATE TRIGGER IF NOT EXISTS sparse_cache_inodes_unknown_size_insert_check
BEFORE INSERT ON sparse_cache_inodes
WHEN NEW.size_known = 0 AND NEW.size != 0
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid size_known');
END;

CREATE TRIGGER IF NOT EXISTS sparse_cache_inodes_unknown_size_update_check
BEFORE UPDATE OF size, size_known ON sparse_cache_inodes
WHEN NEW.size_known = 0 AND NEW.size != 0
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid size_known');
END;

CREATE TABLE IF NOT EXISTS sparse_cache_chunks (
    repo_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    byte_len INTEGER NOT NULL,
    bytes BLOB NOT NULL,
    PRIMARY KEY (repo_id, object_id, chunk_index),
    CHECK (length(repo_id) > 0),
    CHECK (length(object_id) = 64),
    CHECK (typeof(chunk_index) = 'integer' AND chunk_index >= 0),
    CHECK (typeof(offset) = 'integer' AND offset >= 0),
    CHECK (typeof(byte_len) = 'integer' AND byte_len BETWEEN 0 AND 4096),
    CHECK (byte_len = length(bytes)),
    CHECK (offset = chunk_index * 4096)
);

CREATE TABLE IF NOT EXISTS sparse_cache_symlinks (
    view_id INTEGER NOT NULL,
    inode_id INTEGER NOT NULL,
    target TEXT NOT NULL,
    target_object_id TEXT,
    PRIMARY KEY (view_id, inode_id),
    CHECK (typeof(inode_id) = 'integer' AND inode_id >= 0),
    CHECK (length(target) > 0 AND instr(target, char(0)) = 0),
    CHECK (target_object_id IS NULL OR length(target_object_id) = 64),
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
    FOREIGN KEY (view_id) REFERENCES sparse_cache_views (view_id) ON DELETE CASCADE,
    CHECK (typeof(inode_count) = 'integer' AND inode_count >= 0),
    CHECK (typeof(file_count) = 'integer' AND file_count >= 0),
    CHECK (typeof(directory_count) = 'integer' AND directory_count >= 0),
    CHECK (typeof(symlink_count) = 'integer' AND symlink_count >= 0),
    CHECK (typeof(bytes_used) = 'integer' AND bytes_used >= 0),
    CHECK (typeof(blocks_used) = 'integer' AND blocks_used >= 0),
    CHECK (typeof(block_size) = 'integer' AND block_size > 0)
);

CREATE TABLE IF NOT EXISTS sparse_cache_hydration_jobs (
    job_id INTEGER PRIMARY KEY,
    view_id INTEGER NOT NULL,
    repo_id TEXT NOT NULL,
    root_tree_id TEXT NOT NULL,
    commit_id TEXT,
    ref_name TEXT,
    ref_version INTEGER,
    scope TEXT NOT NULL,
    object_id TEXT NOT NULL,
    object_kind TEXT NOT NULL,
    chunk_index INTEGER,
    path TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at_unix_nanos INTEGER NOT NULL,
    updated_at_unix_nanos INTEGER NOT NULL,
    next_run_at_unix_nanos INTEGER,
    completed_at_unix_nanos INTEGER,
    last_error_code TEXT,
    CHECK (length(repo_id) > 0),
    CHECK (length(root_tree_id) = 64),
    CHECK (commit_id IS NULL OR length(commit_id) = 64),
    CHECK (ref_version IS NULL OR (typeof(ref_version) = 'integer' AND ref_version > 0)),
    CHECK (
        (ref_name IS NULL AND ref_version IS NULL)
        OR (ref_name IS NOT NULL AND ref_version IS NOT NULL)
    ),
    CHECK (scope IN ('tree', 'chunk')),
    CHECK (length(object_id) = 64),
    CHECK (object_kind IN ('blob', 'tree')),
    CHECK (
        (scope = 'tree' AND object_kind = 'tree' AND chunk_index IS NULL)
        OR (scope = 'chunk' AND object_kind = 'blob' AND typeof(chunk_index) = 'integer' AND chunk_index >= 0)
    ),
    CHECK (length(path) > 0 AND substr(path, 1, 1) = '/' AND instr(path, char(0)) = 0),
    CHECK (state IN ('pending', 'running', 'completed', 'failed', 'backoff', 'poisoned')),
    CHECK (typeof(attempts) = 'integer' AND attempts >= 0),
    CHECK (typeof(created_at_unix_nanos) = 'integer' AND created_at_unix_nanos >= 0),
    CHECK (typeof(updated_at_unix_nanos) = 'integer' AND updated_at_unix_nanos >= 0),
    CHECK (next_run_at_unix_nanos IS NULL OR (typeof(next_run_at_unix_nanos) = 'integer' AND next_run_at_unix_nanos >= 0)),
    CHECK (completed_at_unix_nanos IS NULL OR (typeof(completed_at_unix_nanos) = 'integer' AND completed_at_unix_nanos >= 0)),
    CHECK (last_error_code IS NULL OR last_error_code IN ('tree_hydration_failed', 'chunk_hydration_failed', 'hydration_poisoned')),
    FOREIGN KEY (view_id) REFERENCES sparse_cache_views (view_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS sparse_cache_hydration_jobs_dedupe_idx
ON sparse_cache_hydration_jobs (
    view_id,
    scope,
    object_id,
    object_kind,
    COALESCE(chunk_index, -1),
    path
);

CREATE TRIGGER IF NOT EXISTS sparse_cache_hydration_jobs_ref_version_insert_check
BEFORE INSERT ON sparse_cache_hydration_jobs
WHEN NEW.ref_version IS NOT NULL AND NEW.ref_version <= 0
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid ref version');
END;

CREATE TRIGGER IF NOT EXISTS sparse_cache_hydration_jobs_ref_version_update_check
BEFORE UPDATE OF ref_version ON sparse_cache_hydration_jobs
WHEN NEW.ref_version IS NOT NULL AND NEW.ref_version <= 0
BEGIN
    SELECT RAISE(ABORT, 'sparse cache invalid ref version');
END;
