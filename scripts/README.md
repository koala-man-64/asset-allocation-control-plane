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
- Contract drift check: `python scripts/automation/run_quality_gate.py contract-artifacts`
- Quality gates: `python scripts/automation/run_quality_gate.py <gate>`
- Env bootstrap: `pwsh ./scripts/setup-env.ps1`
- Git hook installer: `python scripts/dev/install_git_hooks.py`
If you change FastAPI request or response models, including imported shared Pydantic models, treat that as an OpenAPI contract change. Regenerate `api/contracts/*` and keep the regenerated files in the same commit as the route or model change.
- GitHub sync: `pwsh ./scripts/sync-all-to-github.ps1`
- Shared Azure provision: `pwsh ./scripts/ops/provision/provision_azure.ps1`
- Postgres migrations: `pwsh ./scripts/ops/data/apply_postgres_migrations.ps1`

When `AI_RELAY_ENABLED=true`, `scripts/setup-env.ps1` requires `AI_RELAY_REQUIRED_ROLES` and leaves `AI_RELAY_API_KEY` as manual input. `scripts/sync-all-to-github.ps1` fails fast if those AI relay requirements are still missing.
