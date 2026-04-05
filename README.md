# Asset Allocation Control Plane

Runtime-owned control-plane repository for:
- `api/` FastAPI transport and operator endpoints
- `monitoring/` health, status, and Azure diagnostics
- control-plane-side `core/` runtime modules

Local development assumes the shared contracts package is available:

```powershell
python -m pip install -e ../asset-allocation-contracts/python
python -m pytest tests/api tests/monitoring -q
```

Contract artifacts can be regenerated locally with:

```powershell
python scripts/automation/export_contract_artifacts.py
```

## Operations

Canonical workflows live under `.github/workflows/`.

- `ci.yml` is the required validation path for PRs and `main`.
- `security.yml` runs dependency audits and uploads SARIF for runtime dependency findings.
- `release.yml` builds the API image, exports contract artifacts, writes `release-manifest.json`, and dispatches `control_plane_released` to jobs.
- `deploy-prod.yml` is the only runtime deploy path for `asset-allocation-api`.
- `infra-shared-prod.yml` is the only workflow allowed to mutate shared Azure runtime substrate.
- `scripts/dev/setup-env.ps1` builds repo-local `.env.web` using contract defaults and existing values.
- `scripts/repo/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.
