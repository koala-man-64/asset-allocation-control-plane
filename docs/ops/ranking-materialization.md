# Ranking Materialization

Ranking materialization turns a saved strategy configuration plus a saved ranking schema into a strategy-specific platinum table in Postgres. Strategies and ranking schemas each resolve their own saved universe configuration, and the materializer ranks only the symbol/date rows that satisfy both universes.

## Control Plane Objects

- `core.strategies`
  - stores `config.universeConfigName`
  - stores `config.rankingSchemaName`
  - stores `output_table_name`
- `core.universe_configs`
  - stores reusable universe definitions referenced by strategies and ranking schemas
- `core.universe_config_revisions`
  - stores immutable saved universe revisions
- `core.ranking_schemas`
  - stores `config.universeConfigName`
  - stores the active schema definition
- `core.ranking_schema_revisions`
  - stores immutable saved revisions
- `core.ranking_runs`
  - stores run status, row counts, and errors
- `core.ranking_watermarks`
  - stores the latest ranked date per strategy

## Platinum Output

For each ranking-enabled strategy, the materializer writes to:

- `platinum.<strategy_output_table>`

Schema:

```sql
date DATE NOT NULL,
symbol TEXT NOT NULL,
rank INTEGER NOT NULL,
score DOUBLE PRECISION NOT NULL,
last_updated_date DATE NOT NULL
```

Primary key:

- `(date, symbol)`

Indexes:

- `(symbol, date DESC)`
- `(date DESC, rank)`

## API Flow

- Save ranking schema: `POST /api/rankings/`
- Save universe config: `POST /api/universes/`
- Attach schema to strategy: `POST /api/strategies/`
- Preview draft rankings for a strategy/date: `POST /api/rankings/preview`
- Materialize platinum rankings: `POST /api/rankings/materialize`

## Job Flow

Worker entrypoint:

- `python -m tasks.ranking.platinum_rankings`

Relevant environment variables:

- `POSTGRES_DSN`
- `RANKING_STRATEGY_NAME` optional
- `RANKING_START_DATE` optional, ISO date
- `RANKING_END_DATE` optional, ISO date

Behavior:

- If `RANKING_STRATEGY_NAME` is set, only that strategy is materialized.
- If it is omitted, the worker scans saved strategies and materializes each strategy that references an existing ranking schema.
- If no date range is provided, the worker derives a best-effort full range from the referenced gold tables.
- Universe configs cannot be deleted while they are still referenced by saved strategies or ranking schemas.

## Verification

List schemas:

```sql
SELECT name, version, updated_at
FROM core.ranking_schemas
ORDER BY name;
```

Check recent runs:

```sql
SELECT run_id, strategy_name, ranking_schema_name, status, row_count, date_count, started_at, finished_at
FROM core.ranking_runs
ORDER BY started_at DESC
LIMIT 20;
```

Inspect a platinum output:

```sql
SELECT date, symbol, rank, score, last_updated_date
FROM platinum.mom_spy_res
ORDER BY date DESC, rank ASC
LIMIT 50;
```

Check watermarks:

```sql
SELECT strategy_name, ranking_schema_name, ranking_schema_version, output_table_name, last_ranked_date, updated_at
FROM core.ranking_watermarks
ORDER BY strategy_name;
```
