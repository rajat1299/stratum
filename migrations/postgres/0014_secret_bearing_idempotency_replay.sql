ALTER TABLE idempotency_records
    ADD COLUMN secret_replay_envelope_version INTEGER,
    ADD COLUMN secret_replay_key_id TEXT,
    ADD COLUMN secret_replay_aad_hash TEXT,
    ADD COLUMN secret_replay_encrypted_at TIMESTAMPTZ;

ALTER TABLE idempotency_records
    DROP CONSTRAINT idempotency_records_replay_classification_check,
    ADD CONSTRAINT idempotency_records_replay_classification_check
        CHECK (
            replay_classification IS NULL
            OR replay_classification IN ('secret_free', 'partial', 'secret_bearing')
        ),
    ADD CONSTRAINT idempotency_records_secret_replay_metadata_check
        CHECK (
            (
                replay_classification = 'secret_bearing'
                AND state = 'completed'
                AND secret_replay_envelope_version IS NOT NULL
                AND secret_replay_key_id IS NOT NULL
                AND secret_replay_aad_hash IS NOT NULL
                AND secret_replay_encrypted_at IS NOT NULL
            )
            OR
            (
                replay_classification IS DISTINCT FROM 'secret_bearing'
                AND secret_replay_envelope_version IS NULL
                AND secret_replay_key_id IS NULL
                AND secret_replay_aad_hash IS NULL
                AND secret_replay_encrypted_at IS NULL
            )
        ),
    ADD CONSTRAINT idempotency_records_secret_replay_metadata_shape_check
        CHECK (
            secret_replay_envelope_version IS NULL
            OR (
                secret_replay_envelope_version > 0
                AND length(btrim(secret_replay_key_id)) BETWEEN 1 AND 255
                AND secret_replay_aad_hash ~ '^[0-9a-f]{64}$'
                AND secret_replay_encrypted_at > '-infinity'::timestamptz
                AND secret_replay_encrypted_at < 'infinity'::timestamptz
            )
        ),
    ADD CONSTRAINT idempotency_records_secret_replay_envelope_shape_check
        CHECK (
            replay_classification IS DISTINCT FROM 'secret_bearing'
            OR (
                jsonb_typeof(response_body_json) = 'object'
                AND jsonb_typeof(response_body_json -> 'version') = 'number'
                AND response_body_json -> 'version' = to_jsonb(secret_replay_envelope_version)
                AND jsonb_typeof(response_body_json -> 'key_id') = 'string'
                AND response_body_json ->> 'key_id' = secret_replay_key_id
                AND jsonb_typeof(response_body_json -> 'nonce_b64') = 'string'
                AND length(response_body_json ->> 'nonce_b64') > 0
                AND jsonb_typeof(response_body_json -> 'ciphertext_b64') = 'string'
                AND length(response_body_json ->> 'ciphertext_b64') > 0
                AND jsonb_typeof(response_body_json -> 'aad_hash') = 'string'
                AND response_body_json ->> 'aad_hash' = secret_replay_aad_hash
                AND jsonb_typeof(response_body_json -> 'encrypted_at_unix_seconds') = 'number'
            )
        );
