BEGIN;

ALTER TABLE core.broker_account_configuration_audit
  DROP CONSTRAINT IF EXISTS broker_account_configuration_audit_category_check;

ALTER TABLE core.broker_account_configuration_audit
  ADD CONSTRAINT broker_account_configuration_audit_category_check
  CHECK (category IN ('trading_policy', 'allocation', 'onboarding'));

COMMIT;
