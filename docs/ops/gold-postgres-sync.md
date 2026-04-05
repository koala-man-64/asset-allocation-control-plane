# Gold Postgres Sync

Gold Delta remains the source of truth. The gold jobs now replicate successful bucket outputs into Postgres `gold` tables after each Delta write and before bucket watermarks advance.

## Objects

- Migrations:
  - [`deploy/sql/postgres/migrations/0019_gold_postgres_sync.sql`](../../deploy/sql/postgres/migrations/0019_gold_postgres_sync.sql)
  - [`deploy/sql/postgres/migrations/0024_add_gold_earnings_calendar_columns.sql`](../../deploy/sql/postgres/migrations/0024_add_gold_earnings_calendar_columns.sql)
  - [`deploy/sql/postgres/migrations/0027_add_gold_market_structure_features.sql`](../../deploy/sql/postgres/migrations/0027_add_gold_market_structure_features.sql)
- Shared sync helper: [`tasks/common/postgres_gold_sync.py`](../../tasks/common/postgres_gold_sync.py)
- Column metadata catalog:
  - migration: [`deploy/sql/postgres/migrations/0031_gold_column_lookup.sql`](../../deploy/sql/postgres/migrations/0031_gold_column_lookup.sql)
  - seed: [`core/metadata/gold_column_lookup_seed.json`](../../core/metadata/gold_column_lookup_seed.json)
  - sync script: [`scripts/ops/data/sync_gold_column_lookup.py`](../../scripts/ops/data/sync_gold_column_lookup.py)
- Serving tables:
  - `gold.market_data`
  - `gold.finance_data`
  - `gold.earnings_data`
  - `gold.price_target_data`
- Control table: `core.gold_sync_state`
- By-date views:
  - `gold.market_data_by_date`
  - `gold.finance_data_by_date`
  - `gold.earnings_data_by_date`
  - `gold.price_target_data_by_date`

## Runtime Rules

- If `POSTGRES_DSN` is not set, the gold jobs keep their prior Delta-only behavior.
- If `POSTGRES_DSN` is set, a bucket is skipped only when both conditions are true:
  - the Delta watermark is current for the latest silver commit
  - `core.gold_sync_state` shows a successful Postgres sync for that same bucket and source commit
- On a changed bucket, the job:
  - overwrites the Delta bucket
  - creates a session-local temp stage table that mirrors the serving table columns
  - bulk loads the current bucket rows into that temp stage
  - deletes only stale serving-table rows for symbols in the bucket scope that are absent from stage
  - upserts only new or changed staged rows into the matching Postgres table
  - upserts `core.gold_sync_state`
  - advances the bucket watermark only after Postgres sync succeeds
- Gold jobs can write to Postgres concurrently across domains:
  - each domain writes only its own serving table
  - each sync uses a session-local temp stage table
  - same-domain overlap is still prevented by the existing job locks

## Failure Telemetry

- Postgres sync failures now emit `postgres_gold_sync_failure` with:
  - `domain`
  - `bucket`
  - `stage`
  - `category`
  - `error_class`
  - `transient`
- Successful applies emit `postgres_gold_sync_apply_stats` with:
  - `domain`
  - `bucket`
  - `staged_rows`
  - `deleted_rows`
  - `upserted_rows`
  - `unchanged_rows`
  - `scope_symbols`
  - `duration_ms`
- Gold earnings now emits `gold_earnings_failure_counter` whenever it increments a failure counter.
- Final publication logs now use category-accurate blocked reasons:
  - `failed_symbols`
  - `failed_buckets`
  - `failed_finalization`
  - `mixed_failures`
- Use [`docs/ops/gold-earnings-failure-timeline.kql`](gold-earnings-failure-timeline.kql) to reconstruct the incident table for a specific gold earnings execution.
- If Delta writes succeed but Postgres serving sync fails, the bucket data may already exist in storage while watermarks and the shared gold symbol index remain blocked by design.

## Gold Market Notes

- `gold.market_data` is an upstream dependency for `gold-regime-job` and the storage-backed market API endpoints.
- Ordinary symbol failures in `gold-market-job` are partial success:
  - the bucket still writes surviving symbols to Delta and Postgres
  - the bucket emits `status=ok_with_failures`
  - final symbol-index publication and final watermark persistence stay blocked for the run
- Critical market symbols `SPY`, `^VIX`, and `^VIX3M` remain fail-closed:
  - a compute failure on any of those symbols aborts the bucket write
  - a post-sync verification failure in `gold.market_data` blocks final publication
- Operator-facing market logs now distinguish:
  - `layer_handoff_status ... status=ok_with_failures` for ordinary-symbol partial success
  - `layer_handoff_status ... status=failed ... critical_symbol=true symbol=<ticker>` for regime-critical hard failures
  - `postgres_gold_critical_symbol_status` for the final Postgres presence/sync verification step

## Gold Regime Notes

- `gold-regime-job` does not use the shared bucket sync helper because it writes domain-wide regime tables rather than bucketed symbol tables.
- Its Postgres write path now uses the same staged-apply principle:
  - `gold.regime_inputs_daily` is reconciled against the full staged input set
  - `gold.regime_history`, `gold.regime_latest`, and `gold.regime_transitions` are reconciled only for the active model scope
- Successful regime applies emit `gold_regime_postgres_apply_stats` for each target table with staged, deleted, upserted, unchanged, and scope counts.

## Bootstrap

Run Postgres migrations first, clear the Gold Delta layer, then rerun the gold jobs:

```powershell
pwsh ./scripts/ops/data/apply_postgres_migrations.ps1
```

The first successful run seeds `core.gold_sync_state`. After that, unchanged buckets resume normal incremental skipping.

`gold.earnings_data` now includes future-aware calendar columns in addition to the historical surprise metrics:
- `next_earnings_date`
- `days_until_next_earnings`
- `next_earnings_estimate`
- `next_earnings_time_of_day`
- `next_earnings_fiscal_date_ending`
- `has_upcoming_earnings`
- `is_scheduled_earnings_day`

`gold.market_data` now also includes market-structure features derived from daily OHLCV history:
- Donchian channel highs/lows, ATR-normalized distance, and breakout flags for 20-day and 55-day windows
- Confirmed-pivot nearest support/resistance zone scalars (`sr_*`)
- Fibonacci retracement levels and active-swing context (`fib_*`)

## Rebuild / Recovery

- Full rebuild: apply migrations, clear the Gold Delta layer, rerun the gold jobs.
- Single-domain rebuild: clear the matching Gold bucket path, rerun the matching gold job.
- If a job wrote Delta but failed Postgres sync, rerun the same job. Watermarks stay blocked, so the bucket will be retried.
- Rollback for the staged-apply refactor is code-only:
  - revert the shared Postgres sync helper to the prior delete/copy implementation
  - rerun the affected gold job or bucket
  - no serving-table or `core.gold_sync_state` schema rollback is expected

## Verification

Check sync status:

```sql
SELECT domain, bucket, status, source_commit, row_count, symbol_count, synced_at
FROM core.gold_sync_state
ORDER BY domain, bucket;
```

Check serving-table access patterns:

```sql
SELECT *
FROM gold.market_data
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 20;

SELECT *
FROM gold.market_data_by_date
WHERE date = DATE '2026-01-02'
ORDER BY symbol;
```
