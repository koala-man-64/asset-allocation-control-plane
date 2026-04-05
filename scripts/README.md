# Scripts

This directory is organized by audience first, then by intent.

## Layout

- `dev/`
  Local developer convenience scripts. Safe to run on a workstation.
- `automation/`
  CI, release, and repo-quality helpers used by workflows or local validation.
- `repo/`
  Repository maintenance and sync helpers.
- `ops/provision/`
  Azure provisioning and infrastructure reconciliation scripts.
- `ops/validate/`
  Azure environment and permission validation scripts.
- `ops/inspect/`
  Read-only or low-risk diagnostics and inspection scripts.
- `ops/data/`
  Postgres and storage data-management scripts. Some are destructive.

## Destructive Scripts

Review arguments before running anything in `ops/data/`, especially:

- `ops/data/delete_gold_tables.ps1`
- `ops/data/purge_storage_containers.ps1`
- `ops/data/reset_postgres.py`
- `ops/data/reset_postgres_from_scratch.ps1`

Provisioning scripts under `ops/provision/` also mutate shared Azure resources and should be treated as production-impacting.

## Common Entry Points

- Contract export: `python scripts/automation/export_contract_artifacts.py`
- Quality gates: `python scripts/automation/run_quality_gate.py <gate>`
- GitHub sync: `pwsh ./scripts/repo/sync-all-to-github.ps1`
- Shared Azure provision: `pwsh ./scripts/ops/provision/provision_azure.ps1`
- Postgres migrations: `pwsh ./scripts/ops/data/apply_postgres_migrations.ps1`
