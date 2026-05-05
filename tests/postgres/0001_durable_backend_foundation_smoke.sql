BEGIN;

\ir ../../migrations/postgres/0001_durable_backend_foundation.sql
\ir ../../migrations/postgres/0002_review_local_commit_ids.sql

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

SELECT assert_raises(
    $$INSERT INTO commit_parents (repo_id, commit_id, parent_commit_id, parent_order)
      VALUES ('other_repo', repeat('b', 64), repeat('a', 64), 0)$$,
    '23503',
    NULL,
    'commit parent FK is repo scoped'
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
    completed_at
)
VALUES ('scope', repeat('3', 64), repeat('4', 64), 'completed', 200, '{"ok":true}'::jsonb, now());

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
