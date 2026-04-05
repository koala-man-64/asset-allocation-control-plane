BEGIN;

-- Ranking has been removed from this project. Clean up any retired objects from
-- earlier deployments (safe to run even if they do not exist).

DROP SCHEMA IF EXISTS ranking CASCADE;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ranking_writer') THEN
    -- Ensure the role can be dropped even if it previously owned objects or had grants.
    -- These statements are no-ops when nothing is owned/granted.
    EXECUTE format('REASSIGN OWNED BY %I TO %I', 'ranking_writer', current_user);
    EXECUTE format('DROP OWNED BY %I', 'ranking_writer');
    EXECUTE format('DROP ROLE %I', 'ranking_writer');
  END IF;
END $$;

COMMIT;

