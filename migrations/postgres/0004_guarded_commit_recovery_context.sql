-- Persisted redacted repair context for guarded durable commit post-CAS recovery.
--
-- The column is nullable so recovery rows created before contextual enqueue
-- remain readable and are explicitly unsupported by repair workers.

ALTER TABLE durable_post_cas_recovery_claims
    ADD COLUMN context_json JSONB,
    ADD CONSTRAINT durable_post_cas_recovery_claims_context_json_check CHECK (
        context_json IS NULL OR jsonb_typeof(context_json) = 'object'
    );
