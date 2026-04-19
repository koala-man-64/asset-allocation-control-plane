BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS core.government_signal_source_state (
  source_name TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  state_type TEXT NOT NULL,
  cursor_value TEXT,
  source_commit TEXT,
  last_effective_at TIMESTAMPTZ,
  last_published_at TIMESTAMPTZ,
  last_source_updated_at TIMESTAMPTZ,
  last_ingested_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (source_name, dataset_name, state_type)
);

CREATE INDEX IF NOT EXISTS idx_core_government_signal_source_state_updated_at
  ON core.government_signal_source_state(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_government_signal_source_state_last_ingested_at
  ON core.government_signal_source_state(last_ingested_at DESC);

CREATE TABLE IF NOT EXISTS core.government_signal_entity_map (
  mapping_id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  raw_key TEXT NOT NULL,
  raw_name TEXT NOT NULL,
  proposed_symbol TEXT,
  confidence DOUBLE PRECISION,
  status TEXT NOT NULL DEFAULT 'pending_review' CHECK (status IN ('pending_review', 'mapped', 'ignored')),
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_core_government_signal_entity_map_confidence CHECK (
    confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)
  ),
  CONSTRAINT uq_core_government_signal_entity_map_source_key UNIQUE (source_name, entity_type, raw_key)
);

CREATE INDEX IF NOT EXISTS idx_core_government_signal_entity_map_status_updated
  ON core.government_signal_entity_map(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_government_signal_entity_map_symbol_status
  ON core.government_signal_entity_map(proposed_symbol, status);

CREATE TABLE IF NOT EXISTS core.government_signal_mapping_overrides (
  override_id TEXT PRIMARY KEY,
  mapping_id TEXT NOT NULL REFERENCES core.government_signal_entity_map(mapping_id) ON DELETE CASCADE,
  action TEXT NOT NULL CHECK (action IN ('map', 'ignore', 'defer')),
  symbol TEXT,
  reason TEXT,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_core_government_signal_mapping_overrides_symbol CHECK (
    (action = 'map' AND symbol IS NOT NULL)
    OR (action <> 'map' AND symbol IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_core_government_signal_mapping_overrides_mapping_created
  ON core.government_signal_mapping_overrides(mapping_id, created_at DESC);

CREATE TABLE IF NOT EXISTS gold.government_signal_congress_events (
  event_id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  member_id TEXT,
  member_name TEXT NOT NULL,
  chamber TEXT NOT NULL DEFAULT 'unknown',
  party TEXT,
  state TEXT,
  district TEXT,
  committee_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  traded_at TIMESTAMPTZ NOT NULL,
  filed_at TIMESTAMPTZ,
  notified_at TIMESTAMPTZ,
  relationship_type TEXT NOT NULL DEFAULT 'unknown',
  transaction_type TEXT NOT NULL,
  filing_status TEXT NOT NULL DEFAULT 'unknown',
  amendment_flag BOOLEAN NOT NULL DEFAULT FALSE,
  late_filing_days INTEGER,
  asset_name TEXT NOT NULL,
  asset_description TEXT,
  asset_type TEXT,
  issuer_name TEXT,
  issuer_ticker TEXT,
  amount_lower_usd DOUBLE PRECISION,
  amount_upper_usd DOUBLE PRECISION,
  amount_bucket_label TEXT,
  comments TEXT,
  excess_return DOUBLE PRECISION,
  confidence DOUBLE PRECISION,
  mapping_status TEXT NOT NULL DEFAULT 'pending_review' CHECK (mapping_status IN ('pending_review', 'mapped', 'ignored')),
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  CONSTRAINT chk_gold_government_signal_congress_events_late_days CHECK (
    late_filing_days IS NULL OR late_filing_days >= 0
  ),
  CONSTRAINT chk_gold_government_signal_congress_events_amount_bounds CHECK (
    amount_lower_usd IS NULL OR amount_upper_usd IS NULL OR amount_upper_usd >= amount_lower_usd
  ),
  CONSTRAINT chk_gold_government_signal_congress_events_confidence CHECK (
    confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)
  )
);

CREATE INDEX IF NOT EXISTS idx_gold_government_signal_congress_events_symbol_traded
  ON gold.government_signal_congress_events(issuer_ticker, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_government_signal_congress_events_member_traded
  ON gold.government_signal_congress_events(member_id, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_government_signal_congress_events_filed
  ON gold.government_signal_congress_events(filed_at DESC);

CREATE TABLE IF NOT EXISTS gold.government_signal_contract_events (
  event_id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_at TIMESTAMPTZ NOT NULL,
  recipient_name TEXT NOT NULL,
  recipient_ticker TEXT,
  awarding_agency TEXT NOT NULL,
  funding_agency TEXT,
  award_id TEXT,
  parent_award_id TEXT,
  opportunity_id TEXT,
  solicitation_id TEXT,
  title TEXT NOT NULL,
  description TEXT,
  award_amount_usd DOUBLE PRECISION,
  obligation_delta_usd DOUBLE PRECISION,
  outlay_delta_usd DOUBLE PRECISION,
  cumulative_obligation_usd DOUBLE PRECISION,
  modification_number TEXT,
  option_exercise_flag BOOLEAN NOT NULL DEFAULT FALSE,
  termination_flag BOOLEAN NOT NULL DEFAULT FALSE,
  cancellation_flag BOOLEAN NOT NULL DEFAULT FALSE,
  protest_flag BOOLEAN NOT NULL DEFAULT FALSE,
  naics_code TEXT,
  psc_code TEXT,
  competition_type TEXT,
  set_aside_type TEXT,
  contract_vehicle TEXT,
  place_of_performance_country TEXT,
  place_of_performance_state TEXT,
  confidence DOUBLE PRECISION,
  mapping_status TEXT NOT NULL DEFAULT 'pending_review' CHECK (mapping_status IN ('pending_review', 'mapped', 'ignored')),
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  CONSTRAINT chk_gold_government_signal_contract_events_confidence CHECK (
    confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)
  )
);

CREATE INDEX IF NOT EXISTS idx_gold_government_signal_contract_events_symbol_event
  ON gold.government_signal_contract_events(recipient_ticker, event_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_government_signal_contract_events_agency_event
  ON gold.government_signal_contract_events(awarding_agency, event_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_government_signal_contract_events_type_event
  ON gold.government_signal_contract_events(event_type, event_at DESC);

CREATE TABLE IF NOT EXISTS gold.government_signal_issuer_daily (
  symbol TEXT NOT NULL,
  as_of_date DATE NOT NULL,
  issuer_name TEXT,
  congress_purchase_count_1d INTEGER NOT NULL DEFAULT 0,
  congress_purchase_count_7d INTEGER NOT NULL DEFAULT 0,
  congress_purchase_count_30d INTEGER NOT NULL DEFAULT 0,
  congress_purchase_count_90d INTEGER NOT NULL DEFAULT 0,
  congress_sale_count_1d INTEGER NOT NULL DEFAULT 0,
  congress_sale_count_7d INTEGER NOT NULL DEFAULT 0,
  congress_sale_count_30d INTEGER NOT NULL DEFAULT 0,
  congress_sale_count_90d INTEGER NOT NULL DEFAULT 0,
  congress_net_amount_proxy_usd_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
  congress_net_amount_proxy_usd_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  congress_amendment_rate_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  congress_late_filing_rate_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  congress_unique_members_90d INTEGER NOT NULL DEFAULT 0,
  congress_unique_committees_90d INTEGER NOT NULL DEFAULT 0,
  contract_award_count_30d INTEGER NOT NULL DEFAULT 0,
  contract_award_count_90d INTEGER NOT NULL DEFAULT 0,
  contract_obligation_delta_usd_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
  contract_obligation_delta_usd_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  contract_outlay_delta_usd_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
  contract_outlay_delta_usd_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  contract_modification_count_90d INTEGER NOT NULL DEFAULT 0,
  contract_option_exercise_count_90d INTEGER NOT NULL DEFAULT 0,
  contract_termination_count_90d INTEGER NOT NULL DEFAULT 0,
  contract_cancellation_count_90d INTEGER NOT NULL DEFAULT 0,
  contract_protest_count_90d INTEGER NOT NULL DEFAULT 0,
  contract_unique_awarding_agencies_90d INTEGER NOT NULL DEFAULT 0,
  contract_unique_naics_90d INTEGER NOT NULL DEFAULT 0,
  contract_unique_psc_90d INTEGER NOT NULL DEFAULT 0,
  last_congress_trade_at TIMESTAMPTZ,
  last_contract_event_at TIMESTAMPTZ,
  mapping_status TEXT NOT NULL DEFAULT 'pending_review' CHECK (mapping_status IN ('pending_review', 'mapped', 'ignored')),
  PRIMARY KEY (symbol, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_gold_government_signal_issuer_daily_as_of_symbol
  ON gold.government_signal_issuer_daily(as_of_date DESC, symbol);
CREATE INDEX IF NOT EXISTS idx_gold_government_signal_issuer_daily_mapping_status
  ON gold.government_signal_issuer_daily(mapping_status, as_of_date DESC);

CREATE TABLE IF NOT EXISTS gold.government_signal_agency_daily (
  as_of_date DATE NOT NULL,
  awarding_agency TEXT NOT NULL,
  award_count_30d INTEGER NOT NULL DEFAULT 0,
  award_count_90d INTEGER NOT NULL DEFAULT 0,
  obligation_delta_usd_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
  obligation_delta_usd_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  outlay_delta_usd_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
  outlay_delta_usd_90d DOUBLE PRECISION NOT NULL DEFAULT 0,
  unique_recipients_90d INTEGER NOT NULL DEFAULT 0,
  modification_count_90d INTEGER NOT NULL DEFAULT 0,
  option_exercise_count_90d INTEGER NOT NULL DEFAULT 0,
  termination_count_90d INTEGER NOT NULL DEFAULT 0,
  cancellation_count_90d INTEGER NOT NULL DEFAULT 0,
  protest_count_90d INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (as_of_date, awarding_agency)
);

CREATE INDEX IF NOT EXISTS idx_gold_government_signal_agency_daily_agency_as_of
  ON gold.government_signal_agency_daily(awarding_agency, as_of_date DESC);

CREATE TABLE IF NOT EXISTS gold.government_signal_alerts (
  alert_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  as_of_date DATE NOT NULL,
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  congress_signal_score DOUBLE PRECISION,
  contract_signal_score DOUBLE PRECISION,
  composite_signal_score DOUBLE PRECISION,
  source_event_ids TEXT[] NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_gold_government_signal_alerts_symbol_as_of
  ON gold.government_signal_alerts(symbol, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_government_signal_alerts_severity_as_of
  ON gold.government_signal_alerts(severity, as_of_date DESC);

CREATE OR REPLACE VIEW gold.government_signal_congress_events_by_date AS
SELECT *
FROM gold.government_signal_congress_events;

CREATE OR REPLACE VIEW gold.government_signal_contract_events_by_date AS
SELECT *
FROM gold.government_signal_contract_events;

CREATE OR REPLACE VIEW gold.government_signal_issuer_daily_by_date AS
SELECT *
FROM gold.government_signal_issuer_daily;

CREATE OR REPLACE VIEW gold.government_signal_agency_daily_by_date AS
SELECT *
FROM gold.government_signal_agency_daily;

CREATE OR REPLACE VIEW gold.government_signal_alerts_by_date AS
SELECT *
FROM gold.government_signal_alerts;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.government_signal_source_state TO backtest_service;
    GRANT SELECT ON TABLE core.government_signal_entity_map TO backtest_service;
    GRANT SELECT ON TABLE core.government_signal_mapping_overrides TO backtest_service;

    GRANT USAGE ON SCHEMA gold TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_congress_events TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_contract_events TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_issuer_daily TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_agency_daily TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_alerts TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_congress_events_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_contract_events_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_issuer_daily_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_agency_daily_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.government_signal_alerts_by_date TO backtest_service;
  END IF;
END $$;

COMMIT;
