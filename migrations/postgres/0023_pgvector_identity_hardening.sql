-- pgvector identity hardening (Slice 23 follow-up)
--
-- Keeps migration 0022 immutable while tightening the derived vector identity
-- from model-only to provider/model/dimensions/chunker. Vector rows are
-- derived data, so constraint/index rewrites do not affect canonical contents.

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

DO $$
DECLARE
    extension_schema TEXT;
BEGIN
    SELECT n.nspname
      INTO extension_schema
      FROM pg_catalog.pg_extension e
      JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
     WHERE e.extname = 'vector';

    IF extension_schema IS DISTINCT FROM 'public' THEN
        EXECUTE 'ALTER EXTENSION vector SET SCHEMA public';
    END IF;
END $$;

DO $$
DECLARE
    state_fk_name TEXT;
BEGIN
    SELECT c.conname
      INTO state_fk_name
      FROM pg_catalog.pg_constraint c
      JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
      JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
      JOIN pg_catalog.pg_class ref ON ref.oid = c.confrelid
      JOIN pg_catalog.pg_namespace rn ON rn.oid = ref.relnamespace
     WHERE n.nspname = current_schema()
       AND rn.nspname = current_schema()
       AND r.relname = 'search_index_vectors'
       AND ref.relname = 'search_index_vector_state'
       AND c.contype = 'f'
     LIMIT 1;

    IF state_fk_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE %I.search_index_vectors DROP CONSTRAINT %I',
            current_schema(),
            state_fk_name
        );
    END IF;
END $$;

ALTER TABLE search_index_vector_state
    DROP CONSTRAINT IF EXISTS search_index_vector_state_pkey;

ALTER TABLE search_index_vector_state
    ADD PRIMARY KEY (
        repo_id, commit_id, root_tree_id,
        embedding_provider, embedding_model, embedding_dimensions, chunker_version
    );

ALTER TABLE search_index_vectors
    DROP CONSTRAINT IF EXISTS search_index_vectors_pkey;

ALTER TABLE search_index_vectors
    ADD PRIMARY KEY (
        repo_id, commit_id, root_tree_id, path, chunk_ordinal,
        embedding_provider, embedding_model, embedding_dimensions, chunker_version
    );

ALTER TABLE search_index_vectors
    ADD FOREIGN KEY (
        repo_id, commit_id, root_tree_id,
        embedding_provider, embedding_model, embedding_dimensions, chunker_version
    )
    REFERENCES search_index_vector_state(
        repo_id, commit_id, root_tree_id,
        embedding_provider, embedding_model, embedding_dimensions, chunker_version
    )
    ON DELETE CASCADE;

DROP INDEX IF EXISTS search_index_vectors_head_model_idx;

CREATE INDEX IF NOT EXISTS search_index_vectors_head_model_identity_idx
    ON search_index_vectors(
        repo_id, commit_id, root_tree_id,
        embedding_provider, embedding_model, embedding_dimensions, chunker_version
    );
