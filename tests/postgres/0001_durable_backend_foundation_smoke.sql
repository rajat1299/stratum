BEGIN;

\ir ../../migrations/postgres/0001_durable_backend_foundation.sql
\ir ../../migrations/postgres/0002_review_local_commit_ids.sql
\ir ../../migrations/postgres/0003_guarded_commit_recovery_claims.sql
\ir ../../migrations/postgres/0004_guarded_commit_recovery_context.sql
\ir ../../migrations/postgres/0005_guarded_commit_pre_visibility_recovery.sql
\ir ../../migrations/postgres/0006_pre_visibility_recovery_run_control.sql
\ir ../../migrations/postgres/0007_durable_fs_mutation_recovery.sql
\ir ../../migrations/postgres/0008_durable_mutation_cleanup_claim_kind.sql
\ir ../../migrations/postgres/0011_idempotency_retention_quota.sql
\ir ../../migrations/postgres/0013_protected_rules_require_all_files_viewed.sql
\ir ../../migrations/postgres/0014_secret_bearing_idempotency_replay.sql

CREATE OR REPLACE FUNCTION assert_true(condition boolean, message text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF condition IS NOT TRUE THEN
        RAISE EXCEPTION 'assertion failed: %', message;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION assert_raises(
    statement text,
    expected_sqlstate text,
    expected_constraint text,
    message text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    actual_sqlstate text;
    actual_constraint text;
    actual_message text;
BEGIN
    EXECUTE statement;
    RAISE EXCEPTION 'assertion failed: % did not raise', message;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            actual_sqlstate = RETURNED_SQLSTATE,
            actual_constraint = CONSTRAINT_NAME,
            actual_message = MESSAGE_TEXT;

        IF actual_sqlstate = 'P0001' AND actual_message LIKE 'assertion failed:%' THEN
            RAISE;
        END IF;

        IF actual_sqlstate IS DISTINCT FROM expected_sqlstate THEN
            RAISE EXCEPTION
                'assertion failed: % raised SQLSTATE %, expected %: %',
                message,
                actual_sqlstate,
                expected_sqlstate,
                actual_message;
        END IF;

        IF expected_constraint IS NOT NULL AND actual_constraint IS DISTINCT FROM expected_constraint THEN
            RAISE EXCEPTION
                'assertion failed: % raised constraint %, expected %',
                message,
                actual_constraint,
                expected_constraint;
        END IF;
END;
$$;

SELECT assert_raises(
    $$INSERT INTO repos (id, name) VALUES ('bad repo', 'bad repo')$$,
    '23514',
    'repos_id_check',
    'repo IDs reject spaces'
);

INSERT INTO repos (id, name) VALUES
    ('repo_ok', 'Repository OK'),
    ('other_repo', 'Other Repository');

INSERT INTO workspaces (
    id,
    repo_id,
    name,
    root_path,
    head_commit,
    version,
    base_ref,
    session_ref
) VALUES (
    '00000000-0000-4000-8000-000000000900',
    'repo_ok',
    'pre auth workspace',
    '/pre-auth-workspace',
    NULL,
    0,
    'main',
    NULL
);

INSERT INTO workspace_tokens (
    id,
    workspace_id,
    name,
    agent_uid,
    secret_hash,
    read_prefixes_json,
    write_prefixes_json,
    created_at
) VALUES (
    '00000000-0000-4000-8000-000000000903',
    '00000000-0000-4000-8000-000000000900',
    'pre-auth-token',
    100,
    repeat('b', 64),
    '["/pre-auth-workspace"]'::jsonb,
    '["/pre-auth-workspace"]'::jsonb,
    '2024-01-02 03:04:05+00'::timestamptz
);

\ir ../../migrations/postgres/0009_durable_auth_session_foundation.sql

SELECT assert_true(
    (
        SELECT repo_id = 'repo_ok'
           AND principal_uid = 100
           AND token_version = 1
           AND issued_at = created_at
           AND updated_at = created_at
        FROM workspace_tokens
        WHERE id = '00000000-0000-4000-8000-000000000903'
    ),
    'migration preserves existing workspace token creation time as lifecycle time'
);

INSERT INTO durable_principals (
    uid,
    repo_id,
    username,
    primary_gid,
    groups_json,
    kind
) VALUES (
    100,
    'repo_ok',
    'repo-agent',
    100,
    '[100, 101]'::jsonb,
    'agent'
);

SELECT assert_raises(
    $$INSERT INTO durable_principals (uid, repo_id, username, primary_gid, kind)
      VALUES (101, 'repo_ok', 'bad-kind', 101, 'robot')$$,
    '23514',
    'durable_principals_kind_check',
    'durable principal kind enum is enforced'
);

SELECT assert_raises(
    $$INSERT INTO durable_principals (uid, repo_id, username, primary_gid, kind, created_at)
      VALUES (102, 'repo_ok', 'bad-created-at', 102, 'agent', 'infinity'::timestamptz)$$,
    '23514',
    'durable_principals_created_at_finite_check',
    'durable principal created_at must be finite'
);

SELECT assert_raises(
    $$INSERT INTO durable_principals (uid, repo_id, username, primary_gid, kind, updated_at)
      VALUES (103, 'repo_ok', 'bad-updated-at', 103, 'agent', '-infinity'::timestamptz)$$,
    '23514',
    'durable_principals_updated_at_finite_check',
    'durable principal updated_at must be finite'
);

INSERT INTO workspaces (
    id,
    repo_id,
    name,
    root_path,
    head_commit,
    version,
    base_ref,
    session_ref
) VALUES (
    '00000000-0000-4000-8000-000000000901',
    'repo_ok',
    'auth workspace',
    '/auth-workspace',
    NULL,
    0,
    'main',
    NULL
);

INSERT INTO workspace_tokens (
    id,
    workspace_id,
    repo_id,
    name,
    agent_uid,
    secret_hash,
    read_prefixes_json,
    write_prefixes_json,
    principal_uid
) VALUES (
    '00000000-0000-4000-8000-000000000902',
    '00000000-0000-4000-8000-000000000901',
    'repo_ok',
    'auth-token',
    100,
    repeat('a', 64),
    '["/auth-workspace"]'::jsonb,
    '["/auth-workspace"]'::jsonb,
    100
);

SELECT assert_raises(
    $$INSERT INTO workspace_tokens (
          id, workspace_id, repo_id, name, agent_uid, secret_hash,
          read_prefixes_json, write_prefixes_json
      )
      VALUES (
          '00000000-0000-4000-8000-000000000904',
          '00000000-0000-4000-8000-000000000901',
          'repo_ok',
          'raw-secret-token',
          100,
          'raw-workspace-token',
          '["/workspace"]'::jsonb,
          '["/workspace"]'::jsonb
      )$$,
    '23514',
    'workspace_tokens_secret_hash_check',
    'workspace token secret hash must be lowercase sha256 hex'
);

SELECT assert_true(
    (
        SELECT repo_id = 'repo_ok'
           AND principal_uid = 100
           AND token_version = 1
           AND issued_at IS NOT NULL
           AND updated_at IS NOT NULL
           AND expires_at IS NULL
           AND revoked_at IS NULL
        FROM workspace_tokens
        WHERE id = '00000000-0000-4000-8000-000000000902'
    ),
    'workspace token durable lifecycle columns are present'
);

INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256) VALUES
    ('repo_ok', 'blob', repeat('0', 64), 'objects/blob/0', 1, repeat('0', 64)),
    ('repo_ok', 'tree', repeat('1', 64), 'objects/tree/1', 1, repeat('1', 64)),
    ('repo_ok', 'tree', repeat('2', 64), 'objects/tree/2', 1, repeat('2', 64)),
    ('repo_ok', 'tree', repeat('3', 64), 'objects/tree/3', 1, repeat('3', 64)),
    ('repo_ok', 'tree', repeat('4', 64), 'objects/tree/4', 1, repeat('4', 64)),
    ('repo_ok', 'commit', repeat('a', 64), 'objects/commit/a', 1, repeat('a', 64));

SELECT assert_raises(
    $$INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
      VALUES ('repo_ok', 'note', repeat('5', 64), 'objects/note/5', 1, repeat('5', 64))$$,
    '23514',
    'objects_kind_check',
    'object kind enum is enforced'
);

SELECT assert_raises(
    $$INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
      VALUES ('repo_ok', 'blob', repeat('5', 64), 'objects/blob/negative-size', -1, repeat('5', 64))$$,
    '23514',
    'objects_size_bytes_check',
    'object size cannot be negative'
);

SELECT assert_raises(
    $$INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
      VALUES ('repo_ok', 'blob', repeat('A', 64), 'objects/blob/upper', 1, repeat('A', 64))$$,
    '23514',
    'objects_object_id_check',
    'object IDs must be lowercase hex'
);

SELECT assert_raises(
    $$INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
      VALUES ('repo_ok', 'blob', repeat('b', 63), 'objects/blob/short', 1, repeat('b', 63))$$,
    '23514',
    'objects_object_id_check',
    'object IDs must be 64 characters'
);

SELECT assert_raises(
    $$INSERT INTO objects (repo_id, kind, object_id, object_key, size_bytes, sha256)
      VALUES ('repo_ok', 'blob', repeat('b', 64), 'objects/blob/mismatch', 1, repeat('c', 64))$$,
    '23514',
    'objects_check',
    'object sha256 must match object_id'
);

INSERT INTO object_cleanup_claims (
    repo_id,
    claim_kind,
    object_kind,
    object_id,
    object_key,
    lease_owner,
    lease_token,
    lease_expires_at,
    attempts
)
VALUES (
    'repo_ok',
    'final_object_metadata_repair',
    'blob',
    repeat('0', 64),
    'repos/repo_ok/objects/blob/' || repeat('0', 64),
    'worker-a',
    '00000000-0000-4000-8000-000000000001',
    now() + interval '5 minutes',
    1
);

INSERT INTO object_cleanup_claims (
    repo_id,
    claim_kind,
    object_kind,
    object_id,
    object_key,
    lease_owner,
    lease_token,
    lease_expires_at,
    attempts
)
VALUES (
    'repo_ok',
    'durable_mutation_cas_lost_object_cleanup',
    'tree',
    repeat('1', 64),
    'repos/repo_ok/objects/tree/' || repeat('1', 64),
    'worker-a',
    '00000000-0000-4000-8000-000000000008',
    now() + interval '5 minutes',
    1
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'delete_final_object', 'blob', repeat('1', 64),
        'repos/repo_ok/objects/blob/' || repeat('1', 64), 'worker-a',
        '00000000-0000-4000-8000-000000000002', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'object_cleanup_claims_claim_kind_check',
    'cleanup claim kind is constrained to supported repair work'
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'final_object_metadata_repair', 'blob', repeat('g', 64),
        'repos/repo_ok/objects/blob/' || repeat('g', 64), 'worker-a',
        '00000000-0000-4000-8000-000000000003', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'object_cleanup_claims_object_id_check',
    'cleanup claim object IDs must be lowercase hex'
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'final_object_metadata_repair', 'blob', repeat('1', 64),
        '', 'worker-a',
        '00000000-0000-4000-8000-000000000004', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'object_cleanup_claims_canonical_key_check',
    'cleanup claim object keys cannot be empty'
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'final_object_metadata_repair', 'blob', repeat('1', 64),
        'repos/repo_ok/objects/blob/' || repeat('2', 64), 'worker-a',
        '00000000-0000-4000-8000-000000000007', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'object_cleanup_claims_canonical_key_check',
    'cleanup claim object keys must match repo, kind, and object id'
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'final_object_metadata_repair', 'blob', repeat('1', 64),
        'repos/repo_ok/objects/blob/' || repeat('1', 64), '',
        '00000000-0000-4000-8000-000000000005', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'object_cleanup_claims_lease_owner_check',
    'cleanup claim lease owner cannot be empty'
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'final_object_metadata_repair', 'blob', repeat('2', 64),
        'repos/repo_ok/objects/blob/' || repeat('2', 64), 'worker-a',
        'not-a-uuid', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'object_cleanup_claims_lease_token_check',
    'cleanup claim lease tokens must be UUID-shaped'
);

SELECT assert_raises(
    $$INSERT INTO object_cleanup_claims (
        repo_id, claim_kind, object_kind, object_id, object_key, lease_owner,
        lease_token, lease_expires_at, attempts
      )
      VALUES (
        'repo_ok', 'final_object_metadata_repair', 'blob', repeat('3', 64),
        'repos/repo_ok/objects/blob/' || repeat('3', 64), 'worker-a',
        '00000000-0000-4000-8000-000000000006', now() + interval '5 minutes', 0
      )$$,
    '23514',
    'object_cleanup_claims_attempts_check',
    'cleanup claim attempts must be positive'
);

SELECT assert_raises(
    $$UPDATE object_cleanup_claims
      SET completed_at = now(), last_error = 'should not coexist'
      WHERE repo_id = 'repo_ok'
        AND claim_kind = 'final_object_metadata_repair'
        AND object_key = 'repos/repo_ok/objects/blob/' || repeat('0', 64)$$,
    '23514',
    'object_cleanup_claims_completed_error_check',
    'completed cleanup claims cannot retain last_error'
);

SELECT assert_raises(
    $$INSERT INTO commits (repo_id, id, root_tree_kind, root_tree_id, author, message, commit_timestamp_seconds)
      VALUES ('repo_ok', repeat('e', 64), 'blob', repeat('0', 64), 'agent', 'bad root kind', 1)$$,
    '23514',
    'commits_root_tree_kind_check',
    'commit root tree kind must be tree'
);

SELECT assert_raises(
    $$INSERT INTO commits (repo_id, id, root_tree_id, author, message, commit_timestamp_seconds)
      VALUES ('repo_ok', repeat('e', 64), repeat('1', 64), 'agent', 'negative timestamp', -1)$$,
    '23514',
    'commits_commit_timestamp_seconds_check',
    'commit timestamp cannot be negative'
);

SELECT assert_raises(
    $$INSERT INTO commits (repo_id, id, root_tree_id, author, message, commit_timestamp_seconds)
      VALUES ('other_repo', repeat('a', 64), repeat('1', 64), 'agent', 'bad root tree scope', 1)$$,
    '23503',
    NULL,
    'commit root tree FK is repo scoped'
);

INSERT INTO commits (repo_id, id, root_tree_id, author, message, commit_timestamp_seconds) VALUES
    ('repo_ok', repeat('a', 64), repeat('1', 64), 'agent', 'base commit', 1),
    ('repo_ok', repeat('b', 64), repeat('2', 64), 'agent', 'second commit', 2),
    ('repo_ok', repeat('c', 64), repeat('3', 64), 'agent', 'third commit', 3),
    ('repo_ok', repeat('d', 64), repeat('4', 64), 'agent', 'fourth commit', 4);

INSERT INTO commit_parents (repo_id, commit_id, parent_commit_id, parent_order)
VALUES ('repo_ok', repeat('b', 64), repeat('a', 64), 0);

INSERT INTO durable_fs_mutation_recovery_ledger (
    repo_id,
    workspace_scope,
    operation_id,
    target_ref,
    previous_commit_id,
    new_commit_id,
    failed_step,
    state,
    envelope_json
)
VALUES (
    'repo_ok',
    'workspace:demo',
    'op-demo',
    'agent/demo/session',
    repeat('a', 64),
    repeat('b', 64),
    'audit_append',
    'pending',
    jsonb_build_object('audit', jsonb_build_object('action', 'fs_write_file'))
);

SELECT assert_raises(
    $$INSERT INTO durable_fs_mutation_recovery_ledger (
          repo_id, workspace_scope, operation_id, target_ref,
          previous_commit_id, new_commit_id, failed_step, state,
          retry_after, last_error, attempts, envelope_json
      )
      VALUES (
          'repo_ok', 'workspace:demo', 'op-backoff', 'agent/demo/session',
          repeat('a', 64), repeat('b', 64), 'audit_append', 'backing_off',
          now() + interval '1 minute', 'raw postgres detail', 1,
          jsonb_build_object('audit', jsonb_build_object('action', 'fs_write_file'))
      )$$,
    '23514',
    'durable_fs_mutation_recovery_backoff_check',
    'backing-off FS mutation recovery diagnostics must be redacted'
);

SELECT assert_raises(
    $$INSERT INTO commit_parents (repo_id, commit_id, parent_commit_id, parent_order)
      VALUES ('other_repo', repeat('b', 64), repeat('a', 64), 0)$$,
    '23503',
    NULL,
    'commit parent FK is repo scoped'
);

INSERT INTO durable_post_cas_recovery_claims (repo_id, ref_name, commit_id, step, state)
VALUES (
    'repo_ok',
    'main',
    repeat('b', 64),
    'workspace_head_update',
    'pending'
);

UPDATE durable_post_cas_recovery_claims
SET context_json = jsonb_build_object(
    'workspace_id', '53545241-5455-4d00-0000-000000000001',
    'expected_workspace_head', repeat('a', 64),
    'audit_event', jsonb_build_object(
        'actor', jsonb_build_object('uid', 0, 'username', 'root', 'delegate', NULL),
        'workspace', NULL,
        'action', 'vcs_commit',
        'resource', jsonb_build_object('kind', 'commit', 'id', repeat('b', 64), 'path', NULL),
        'outcome', 'success',
        'details', jsonb_build_object('commit_id', repeat('b', 64))
    )
)
WHERE repo_id = 'repo_ok'
    AND ref_name = 'main'
    AND commit_id = repeat('b', 64)
    AND step = 'workspace_head_update';

SELECT assert_true(
    (
        SELECT context_json IS NOT NULL
        FROM durable_post_cas_recovery_claims
        WHERE repo_id = 'repo_ok'
            AND ref_name = 'main'
            AND commit_id = repeat('b', 64)
            AND step = 'workspace_head_update'
    ),
    'post-CAS recovery context JSON persists when present'
);

SELECT assert_raises(
    $$UPDATE durable_post_cas_recovery_claims
      SET context_json = '[]'::jsonb
      WHERE repo_id = 'repo_ok'
          AND ref_name = 'main'
          AND commit_id = repeat('b', 64)
          AND step = 'workspace_head_update'$$,
    '23514',
    'durable_post_cas_recovery_claims_context_json_check',
    'post-CAS recovery context JSON must be an object'
);

SELECT assert_raises(
    $$INSERT INTO durable_post_cas_recovery_claims (repo_id, ref_name, commit_id, step, state)
      VALUES ('repo_ok', 'agent/session', repeat('b', 64), 'workspace_head_update', 'pending')$$,
    '23514',
    'durable_post_cas_recovery_claims_ref_name_check',
    'post-CAS recovery claims are scoped to main for this guarded route slice'
);

SELECT assert_raises(
    $$INSERT INTO durable_post_cas_recovery_claims (repo_id, ref_name, commit_id, step, state)
      VALUES ('repo_ok', 'main', repeat('b', 64), 'unknown_step', 'pending')$$,
    '23514',
    'durable_post_cas_recovery_claims_step_check',
    'post-CAS recovery step enum is enforced'
);

SELECT assert_raises(
    $$INSERT INTO durable_post_cas_recovery_claims (
          repo_id, ref_name, commit_id, step, state, lease_owner
      )
      VALUES ('repo_ok', 'main', repeat('b', 64), 'audit_append', 'pending', 'worker')$$,
    '23514',
    'durable_post_cas_recovery_claims_pending_check',
    'pending recovery claims cannot carry lease state'
);

SELECT assert_raises(
    $$INSERT INTO durable_post_cas_recovery_claims (
          repo_id, ref_name, commit_id, step, state, lease_owner, lease_token,
          lease_expires_at, attempts
      )
      VALUES (
          'repo_ok', 'main', repeat('b', 64), 'audit_append', 'active',
          'worker', 'not-a-uuid', now() + interval '5 minutes', 1
      )$$,
    '23514',
    'durable_post_cas_recovery_claims_lease_token_check',
    'active recovery claim lease tokens must be UUID shaped'
);

SELECT assert_raises(
    $$INSERT INTO durable_post_cas_recovery_claims (
          repo_id, ref_name, commit_id, step, state, retry_after, last_error, attempts
      )
      VALUES (
          'repo_ok', 'main', repeat('b', 64), 'audit_append', 'backing_off',
          now() + interval '1 minute', 'raw postgres detail', 1
      )$$,
    '23514',
    'durable_post_cas_recovery_claims_backoff_check',
    'backing-off recovery diagnostics must be redacted'
);

SELECT assert_raises(
    $$INSERT INTO durable_post_cas_recovery_claims (repo_id, ref_name, commit_id, step, state)
      VALUES ('other_repo', 'main', repeat('b', 64), 'audit_append', 'pending')$$,
    '23503',
    NULL,
    'post-CAS recovery claim commit FK is repo scoped'
);

INSERT INTO durable_pre_visibility_recovery_ledger (
    repo_id,
    ref_name,
    commit_id,
    stage,
    state,
    root_tree_id,
    parent_commit_id,
    expected_ref_version,
    object_count,
    changed_path_count,
    has_idempotency_reservation,
    first_seen_at,
    last_seen_at,
    occurrence_count
)
VALUES (
    'repo_ok',
    'main',
    repeat('e', 64),
    'commit_metadata_insert',
    'pending',
    repeat('9', 64),
    repeat('a', 64),
    2,
    3,
    1,
    true,
    now(),
    now(),
    1
);

SELECT assert_true(
    EXISTS (
        SELECT 1
        FROM durable_pre_visibility_recovery_ledger
        WHERE repo_id = 'repo_ok'
            AND ref_name = 'main'
            AND commit_id = repeat('e', 64)
            AND stage = 'commit_metadata_insert'
    ),
    'pre-visibility recovery ledger can persist unconfirmed commit ids without a commit FK'
);

SELECT assert_raises(
    $$INSERT INTO durable_pre_visibility_recovery_ledger (
          repo_id, ref_name, commit_id, stage, state, root_tree_id,
          expected_ref_version, object_count, changed_path_count,
          has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count
      )
      VALUES (
          'repo_ok', 'agent/session', repeat('f', 64), 'ref_visibility_cas',
          'pending', repeat('9', 64), 1, 1, 1, true, now(), now(), 1
      )$$,
    '23514',
    'durable_pre_visibility_recovery_ledger_ref_name_check',
    'pre-visibility recovery is scoped to main for this guarded route slice'
);

SELECT assert_raises(
    $$INSERT INTO durable_pre_visibility_recovery_ledger (
          repo_id, ref_name, commit_id, stage, state, root_tree_id,
          expected_ref_version, object_count, changed_path_count,
          has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count
      )
      VALUES (
          'repo_ok', 'main', repeat('f', 64), 'unknown_stage',
          'pending', repeat('9', 64), 1, 1, 1, true, now(), now(), 1
      )$$,
    '23514',
    'durable_pre_visibility_recovery_ledger_stage_check',
    'pre-visibility recovery stage enum is enforced'
);

SELECT assert_raises(
    $$INSERT INTO durable_pre_visibility_recovery_ledger (
          repo_id, ref_name, commit_id, stage, state, root_tree_id,
          expected_ref_version, object_count, changed_path_count,
          has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count,
          resolved_at
      )
      VALUES (
          'repo_ok', 'main', repeat('f', 64), 'ref_visibility_cas',
          'pending', repeat('9', 64), 1, 1, 1, true, now(), now(), 1, now()
      )$$,
    '23514',
    'durable_pre_visibility_recovery_pending_check',
    'pending pre-visibility recovery rows cannot carry terminal state'
);

SELECT assert_raises(
    $$INSERT INTO durable_pre_visibility_recovery_ledger (
          repo_id, ref_name, commit_id, stage, state, root_tree_id,
          expected_ref_version, object_count, changed_path_count,
          has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count,
          lease_owner, lease_token, lease_expires_at, attempts
      )
      VALUES (
          'repo_ok', 'main', repeat('f', 64), 'ref_visibility_cas',
          'active', repeat('9', 64), 1, 1, 1, true, now(), now(), 1,
          'operator', 'not-a-uuid', now() + interval '1 minute', 1
      )$$,
    '23514',
    'durable_pre_visibility_recovery_lease_token_check',
    'active pre-visibility recovery lease tokens must be UUID shaped'
);

SELECT assert_raises(
    $$INSERT INTO durable_pre_visibility_recovery_ledger (
          repo_id, ref_name, commit_id, stage, state, root_tree_id,
          expected_ref_version, object_count, changed_path_count,
          has_idempotency_reservation, first_seen_at, last_seen_at, occurrence_count,
          retry_after, last_error, attempts
      )
      VALUES (
          'repo_ok', 'main', repeat('f', 64), 'ref_visibility_cas',
          'backing_off', repeat('9', 64), 1, 1, 1, true, now(), now(), 1,
          now() + interval '1 minute', 'raw postgres detail', 1
      )$$,
    '23514',
    'durable_pre_visibility_recovery_backoff_check',
    'backing-off pre-visibility recovery diagnostics must be redacted'
);

INSERT INTO refs (repo_id, name, commit_id, version)
VALUES
    ('repo_ok', 'main', repeat('a', 64), 1),
    ('repo_ok', 'staging', repeat('c', 64), 1);

SELECT assert_raises(
    $$INSERT INTO refs (repo_id, name, commit_id, version)
      VALUES ('other_repo', 'main', repeat('a', 64), 1)$$,
    '23503',
    NULL,
    'ref commit FK is repo scoped'
);

SELECT assert_raises(
    $$INSERT INTO refs (repo_id, name, commit_id, version)
      VALUES ('repo_ok', 'zero-version', repeat('a', 64), 0)$$,
    '23514',
    'refs_version_check',
    'ref version lower bound is enforced'
);

INSERT INTO refs (repo_id, name, commit_id, version)
VALUES ('repo_ok', 'max-version', repeat('a', 64), 9223372036854775807);

INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state)
VALUES ('scope', repeat('1', 64), repeat('2', 64), 'pending');

SELECT assert_raises(
    $$INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state)
      VALUES ('scope', 'raw-retry-key', repeat('2', 64), 'pending')$$,
    '23514',
    'idempotency_records_key_hash_check',
    'idempotency key_hash must be a 64 byte lowercase hex digest'
);

SELECT assert_raises(
    $$INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state)
      VALUES ('scope', repeat('1', 64), 'request-a', 'pending')$$,
    '23514',
    'idempotency_records_request_fingerprint_check',
    'idempotency request_fingerprint must be a 64 byte lowercase hex digest'
);

INSERT INTO idempotency_records (
    scope,
    key_hash,
    request_fingerprint,
    state,
    status_code,
    response_body_json,
    replay_classification,
    completed_at
)
VALUES ('scope', repeat('3', 64), repeat('4', 64), 'completed', 200, '{"ok":true}'::jsonb, 'secret_free', now());

SELECT assert_raises(
    $$INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state, status_code)
      VALUES ('scope', repeat('5', 64), repeat('6', 64), 'pending', 200)$$,
    '23514',
    'idempotency_records_check',
    'pending idempotency records cannot carry a response status'
);

SELECT assert_raises(
    $$INSERT INTO idempotency_records (scope, key_hash, request_fingerprint, state)
      VALUES ('scope', repeat('7', 64), repeat('8', 64), 'completed')$$,
    '23514',
    'idempotency_records_check',
    'completed idempotency records must carry response data'
);

SELECT assert_raises(
    $$INSERT INTO idempotency_records (
          scope, key_hash, request_fingerprint, state, replay_classification
      )
      VALUES (
          'scope', repeat('9', 64), repeat('a', 64), 'pending', 'secret_bearing'
      )$$,
    '23514',
    'idempotency_records_secret_replay_metadata_check',
    'secret-bearing idempotency rows require metadata'
);

SELECT assert_raises(
    $$INSERT INTO idempotency_records (
          scope, key_hash, request_fingerprint, state, status_code,
          response_body_json, replay_classification, completed_at,
          secret_replay_envelope_version, secret_replay_key_id,
          secret_replay_aad_hash, secret_replay_encrypted_at
      )
      VALUES (
          'scope', repeat('b', 64), repeat('c', 64), 'completed', 201,
          '{"not_an_envelope":true}'::jsonb, 'secret_bearing', now(),
          1, 'test-key', repeat('d', 64), now()
      )$$,
    '23514',
    'idempotency_records_secret_replay_envelope_shape_check',
    'secret-bearing idempotency response must be an encrypted envelope object'
);

SELECT assert_raises(
    $$INSERT INTO idempotency_records (
          scope, key_hash, request_fingerprint, state, status_code,
          response_body_json, replay_classification, completed_at,
          secret_replay_envelope_version, secret_replay_key_id,
          secret_replay_aad_hash, secret_replay_encrypted_at
      )
      VALUES (
          'scope', repeat('d', 64), repeat('e', 64), 'completed', 200,
          '{"ok":true}'::jsonb, 'secret_free', now(),
          1, 'test-key', repeat('f', 64), now()
      )$$,
    '23514',
    'idempotency_records_secret_replay_metadata_check',
    'non-secret idempotency rows cannot carry secret replay metadata'
    );

SELECT assert_raises(
    $$INSERT INTO idempotency_records (
          scope, key_hash, request_fingerprint, state, status_code,
          response_body_json, replay_classification, completed_at,
          secret_replay_envelope_version, secret_replay_key_id,
          secret_replay_aad_hash, secret_replay_encrypted_at
      )
      VALUES (
          'scope', repeat('1', 64), repeat('2', 64), 'completed', 201,
          jsonb_build_object(
              'version', 1,
              'key_id', 'test-key',
              'nonce_b64', 'bm9uY2U=',
              'ciphertext_b64', 'Y2lwaGVydGV4dA==',
              'aad_hash', repeat('1', 64),
              'encrypted_at_unix_seconds', 1710000001
          ),
          'secret_bearing', now(),
          1, 'test-key', repeat('1', 64), to_timestamp(1710000000)
      )$$,
    '23514',
    'idempotency_records_secret_replay_envelope_shape_check',
    'secret-bearing idempotency encrypted_at metadata must match envelope'
);

INSERT INTO idempotency_records (
    scope,
    key_hash,
    request_fingerprint,
    state,
    status_code,
    response_body_json,
    replay_classification,
    completed_at,
    secret_replay_envelope_version,
    secret_replay_key_id,
    secret_replay_aad_hash,
    secret_replay_encrypted_at
)
VALUES (
    'scope',
    repeat('f', 64),
    repeat('0', 64),
    'completed',
    201,
    jsonb_build_object(
        'version', 1,
        'key_id', 'test-key',
        'nonce_b64', 'bm9uY2U=',
        'ciphertext_b64', 'Y2lwaGVydGV4dA==',
        'aad_hash', repeat('1', 64),
        'encrypted_at_unix_seconds', 1710000000
    ),
    'secret_bearing',
    now(),
    1,
    'test-key',
    repeat('1', 64),
    to_timestamp(1710000000)
);

INSERT INTO audit_events (
    id,
    repo_id,
    sequence,
    actor_json,
    action,
    resource_json,
    outcome
)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'repo_ok', 1, '{"uid":1}'::jsonb, 'write', '{"path":"/"}'::jsonb, 'success'),
    ('00000000-0000-0000-0000-000000000002', NULL, 1, '{"uid":1}'::jsonb, 'system', '{"kind":"global"}'::jsonb, 'success');

SELECT assert_raises(
    $$INSERT INTO audit_events (id, repo_id, sequence, actor_json, action, resource_json, outcome)
      VALUES ('00000000-0000-0000-0000-000000000003', 'repo_ok', 1, '{"uid":2}'::jsonb, 'write', '{"path":"/again"}'::jsonb, 'success')$$,
    '23505',
    'audit_events_repo_id_sequence_key',
    'per-repo audit sequences are unique'
);

SELECT assert_raises(
    $$INSERT INTO audit_events (id, repo_id, sequence, actor_json, action, resource_json, outcome)
      VALUES ('00000000-0000-0000-0000-000000000004', NULL, 1, '{"uid":2}'::jsonb, 'system', '{"kind":"again"}'::jsonb, 'success')$$,
    '23505',
    'audit_events_global_sequence_idx',
    'global audit sequences are unique when repo_id is null'
);

SELECT assert_raises(
    $$INSERT INTO audit_events (id, repo_id, sequence, actor_json, action, resource_json, outcome)
      VALUES ('00000000-0000-0000-0000-000000000005', 'repo_ok', 0, '{"uid":2}'::jsonb, 'write', '{"path":"/bad-seq"}'::jsonb, 'success')$$,
    '23514',
    'audit_events_sequence_check',
    'audit sequence must be positive'
);

SELECT assert_raises(
    $$INSERT INTO audit_events (id, repo_id, sequence, actor_json, action, resource_json, outcome)
      VALUES ('00000000-0000-0000-0000-000000000006', 'repo_ok', 2, '{"uid":2}'::jsonb, 'write', '{"path":"/bad-outcome"}'::jsonb, 'denied')$$,
    '23514',
    'audit_events_outcome_check',
    'audit outcome enum is enforced'
);

SELECT assert_raises(
    $$INSERT INTO protected_ref_rules (id, repo_id, ref_name, required_approvals, created_by)
      VALUES ('00000000-0000-0000-0000-000000000050', 'repo_ok', 'main', 0, 1)$$,
    '23514',
    'protected_ref_rules_required_approvals_check',
    'protected ref rules require positive approval counts'
);

INSERT INTO protected_ref_rules (id, repo_id, ref_name, required_approvals, created_by)
VALUES ('00000000-0000-0000-0000-000000000051', 'repo_ok', 'main', 1, 1);

INSERT INTO protected_path_rules (
    id, repo_id, path_prefix, target_ref, required_approvals, require_all_files_viewed, created_by
)
VALUES ('00000000-0000-0000-0000-000000000052', 'repo_ok', '/legal', 'main', 1, false, 1);

SELECT assert_true(
    (
        SELECT require_all_files_viewed
        FROM protected_ref_rules
        WHERE id = '00000000-0000-0000-0000-000000000051'
    )
    AND NOT (
        SELECT require_all_files_viewed
        FROM protected_path_rules
        WHERE id = '00000000-0000-0000-0000-000000000052'
    ),
    'protected rule file-view flags default and persist'
);

SELECT assert_raises(
    $$INSERT INTO change_requests (
          id, repo_id, title, source_ref, target_ref, base_commit, head_commit, status, created_by
      )
      VALUES (
          '00000000-0000-0000-0000-000000000011',
          'repo_ok',
          'bad commit shape',
          'staging',
          'main',
          repeat('z', 64),
          repeat('b', 64),
          'open',
          1
      )$$,
    '23514',
    NULL,
    'change-request commit IDs must be valid hashes'
);

INSERT INTO change_requests (
    id,
    repo_id,
    title,
    source_ref,
    target_ref,
    base_commit,
    head_commit,
    status,
    created_by
)
VALUES (
    '00000000-0000-0000-0000-000000000010',
    'repo_ok',
    'Review this change',
    'staging',
    'main',
    repeat('e', 64),
    repeat('f', 64),
    'open',
    1
);

INSERT INTO approvals (id, change_request_id, head_commit, approved_by, comment)
VALUES (
    '00000000-0000-0000-0000-000000000020',
    '00000000-0000-0000-0000-000000000010',
    repeat('f', 64),
    2,
    'approved'
);

SELECT assert_raises(
    $$INSERT INTO approvals (id, change_request_id, head_commit, approved_by, comment)
      VALUES (
          '00000000-0000-0000-0000-000000000021',
          '00000000-0000-0000-0000-000000000010',
          repeat('f', 64),
          2,
          'duplicate active approval'
      )$$,
    '23505',
    'approvals_active_head_approver_idx',
    'active approval uniqueness is enforced per head and approver'
);

INSERT INTO approvals (id, change_request_id, head_commit, approved_by, comment, active)
VALUES (
    '00000000-0000-0000-0000-000000000022',
    '00000000-0000-0000-0000-000000000010',
    repeat('f', 64),
    2,
    'dismissed duplicate approval history',
    false
);

INSERT INTO review_comments (id, change_request_id, author, body, kind)
VALUES (
    '00000000-0000-0000-0000-000000000030',
    '00000000-0000-0000-0000-000000000010',
    3,
    'Looks good.',
    'general'
);

SELECT assert_raises(
    $$INSERT INTO review_comments (id, change_request_id, author, body, kind)
      VALUES (
          '00000000-0000-0000-0000-000000000031',
          '00000000-0000-0000-0000-000000000010',
          3,
          'Bad kind.',
          'blocking'
      )$$,
    '23514',
    'review_comments_kind_check',
    'review comment kind enum is enforced'
);

INSERT INTO reviewer_assignments (id, change_request_id, reviewer, assigned_by)
VALUES (
    '00000000-0000-0000-0000-000000000040',
    '00000000-0000-0000-0000-000000000010',
    4,
    1
);

SELECT assert_raises(
    $$INSERT INTO reviewer_assignments (id, change_request_id, reviewer, assigned_by)
      VALUES (
          '00000000-0000-0000-0000-000000000041',
          '00000000-0000-0000-0000-000000000010',
          4,
          1
      )$$,
    '23505',
    'reviewer_assignments_change_request_id_reviewer_key',
    'reviewer assignment uniqueness is enforced'
);

WITH updated AS (
    UPDATE refs
    SET commit_id = repeat('b', 64),
        version = version + 1,
        updated_at = now()
    WHERE repo_id = 'repo_ok'
        AND name = 'main'
        AND commit_id = repeat('a', 64)
        AND version = 1
    RETURNING commit_id, version
)
SELECT assert_true(
    (SELECT count(*) FROM updated) = 1
        AND EXISTS (
            SELECT 1
            FROM updated
            WHERE commit_id = repeat('b', 64) AND version = 2
        ),
    'matching ref CAS updates one row and increments version'
);

WITH updated AS (
    UPDATE refs
    SET commit_id = repeat('c', 64),
        version = version + 1,
        updated_at = now()
    WHERE repo_id = 'repo_ok'
        AND name = 'main'
        AND commit_id = repeat('a', 64)
        AND version = 2
    RETURNING commit_id, version
)
SELECT assert_true(
    (SELECT count(*) FROM updated) = 0
        AND EXISTS (
            SELECT 1
            FROM refs
            WHERE repo_id = 'repo_ok'
                AND name = 'main'
                AND commit_id = repeat('b', 64)
                AND version = 2
        ),
    'stale ref CAS target updates zero rows and leaves the ref unchanged'
);

WITH updated AS (
    UPDATE refs
    SET commit_id = repeat('c', 64),
        version = version + 1,
        updated_at = now()
    WHERE repo_id = 'repo_ok'
        AND name = 'main'
        AND commit_id = repeat('b', 64)
        AND version = 1
    RETURNING commit_id, version
)
SELECT assert_true(
    (SELECT count(*) FROM updated) = 0
        AND EXISTS (
            SELECT 1
            FROM refs
            WHERE repo_id = 'repo_ok'
                AND name = 'main'
                AND commit_id = repeat('b', 64)
                AND version = 2
        ),
    'stale ref CAS version updates zero rows and leaves the ref unchanged'
);

WITH locked_source AS (
    SELECT commit_id, version
    FROM refs
    WHERE repo_id = 'repo_ok'
        AND name = 'staging'
    FOR UPDATE
),
updated AS (
    UPDATE refs AS target
    SET commit_id = repeat('c', 64),
        version = target.version + 1,
        updated_at = now()
    FROM locked_source AS source
    WHERE target.repo_id = 'repo_ok'
        AND target.name = 'main'
        AND target.commit_id = repeat('b', 64)
        AND target.version = 2
        AND source.commit_id = repeat('c', 64)
        AND source.version = 1
    RETURNING target.commit_id, target.version
)
SELECT assert_true(
    (SELECT count(*) FROM updated) = 1
        AND EXISTS (
            SELECT 1
            FROM updated
            WHERE commit_id = repeat('c', 64) AND version = 3
        ),
    'source-checked CAS updates when the source ref still matches'
);

WITH locked_source AS (
    SELECT commit_id, version
    FROM refs
    WHERE repo_id = 'repo_ok'
        AND name = 'staging'
    FOR UPDATE
),
updated AS (
    UPDATE refs AS target
    SET commit_id = repeat('d', 64),
        version = target.version + 1,
        updated_at = now()
    FROM locked_source AS source
    WHERE target.repo_id = 'repo_ok'
        AND target.name = 'main'
        AND target.commit_id = repeat('c', 64)
        AND target.version = 3
        AND source.commit_id = repeat('d', 64)
        AND source.version = 1
    RETURNING target.commit_id, target.version
)
SELECT assert_true(
    (SELECT count(*) FROM updated) = 0
        AND EXISTS (
            SELECT 1
            FROM refs
            WHERE repo_id = 'repo_ok'
                AND name = 'main'
                AND commit_id = repeat('c', 64)
                AND version = 3
        ),
    'source-checked CAS skips when the source ref no longer matches'
);

ROLLBACK;
