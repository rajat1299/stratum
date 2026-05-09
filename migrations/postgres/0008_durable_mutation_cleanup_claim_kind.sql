-- Allow CAS-lost durable mutation object cleanup claims to use the shared
-- object cleanup claim ledger without changing the original foundation schema.

ALTER TABLE object_cleanup_claims
    DROP CONSTRAINT object_cleanup_claims_claim_kind_check,
    ADD CONSTRAINT object_cleanup_claims_claim_kind_check CHECK (
        claim_kind IN (
            'final_object_metadata_repair',
            'durable_mutation_cas_lost_object_cleanup'
        )
    );
