-- Add protected-rule policy flag for future file-view tracking enforcement.

ALTER TABLE protected_ref_rules
    ADD COLUMN IF NOT EXISTS require_all_files_viewed BOOLEAN NOT NULL DEFAULT true,
    ALTER COLUMN require_all_files_viewed SET DEFAULT true;

ALTER TABLE protected_path_rules
    ADD COLUMN IF NOT EXISTS require_all_files_viewed BOOLEAN NOT NULL DEFAULT true,
    ALTER COLUMN require_all_files_viewed SET DEFAULT true;

UPDATE protected_ref_rules
SET require_all_files_viewed = true
WHERE require_all_files_viewed IS NULL;

UPDATE protected_path_rules
SET require_all_files_viewed = true
WHERE require_all_files_viewed IS NULL;

ALTER TABLE protected_ref_rules
    ALTER COLUMN require_all_files_viewed SET NOT NULL;

ALTER TABLE protected_path_rules
    ALTER COLUMN require_all_files_viewed SET NOT NULL;
