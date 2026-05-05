-- The durable control-plane runtime still creates change requests from the
-- local StratumDb VCS state. Keep review commit IDs shape-checked, but do not
-- require them to exist in the durable Postgres commit catalog until the core
-- filesystem/VCS runtime is cut over.
DO $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'change_requests'::regclass
          AND confrelid = 'commits'::regclass
          AND contype = 'f'
    LOOP
        EXECUTE format('ALTER TABLE change_requests DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;
