# Gold Column Lookup

`gold.column_lookup` is the canonical metadata catalog for strategy exploration.

It stores one row per `(schema_name, table_name, column_name)` and includes:

- structural fields (`data_type`, `is_nullable`)
- curated feature metadata (`description`, `calculation_type`, `calculation_notes`, `calculation_expression`, `calculation_dependencies`)
- ownership/status fields (`source_job`, `status`, `updated_at`, `updated_by`)

## Migration

- `deploy/sql/postgres/migrations/0031_gold_column_lookup.sql`

Apply migrations before syncing:

```powershell
pwsh ./scripts/ops/data/apply_postgres_migrations.ps1
```

## Seed + Sync Workflow

Seed metadata is repo-backed at:

- `core/metadata/gold_column_lookup_seed.json`

Sync script:

- `scripts/ops/data/sync_gold_column_lookup.py`

Example usage:

```powershell
.\.venv\Scripts\python.exe scripts/ops/data/sync_gold_column_lookup.py
```

Dry run:

```powershell
.\.venv\Scripts\python.exe scripts/ops/data/sync_gold_column_lookup.py --dry-run
```

Force seed metadata to overwrite existing curated fields:

```powershell
.\.venv\Scripts\python.exe scripts/ops/data/sync_gold_column_lookup.py --force-metadata
```

## API Endpoints

- `GET /api/system/postgres/gold-column-lookup/tables`
- `GET /api/system/postgres/gold-column-lookup?table=&q=&status=&limit=&offset=`

## CI Drift Gates

The following tests enforce lookup metadata quality:

- `tests/tools/test_gold_column_lookup_seed.py`
  - fails if any expected gold column is missing in seed metadata
  - fails if an `approved` row uses a placeholder description
- `tests/tools/test_sync_gold_column_lookup_script.py`
  - validates non-destructive sync behavior and placeholder safeguards
