# Asset Allocation Control Plane

Runtime-owned control-plane repository for:
- `api/` FastAPI transport and operator endpoints
- `monitoring/` health, status, and Azure diagnostics
- control-plane-side `core/` runtime modules

Local development installs versioned shared packages rather than sibling repos:

```powershell
python -m pip install asset-allocation-contracts==1.2.0
python -m pip install asset-allocation-runtime-common==2.0.1
python -m pytest tests/api tests/monitoring -q
```

Contract artifacts can be regenerated locally with:

```powershell
python scripts/automation/export_contract_artifacts.py
```

## Operations

Canonical workflows live under `.github/workflows/`.

- `ci.yml` is the required validation path for PRs and `main`.
- `security.yml` runs scheduled or manual dependency audits and uploads SARIF for runtime dependency findings.
- `compat.yml` is the only workflow allowed to validate candidate `asset-allocation-contracts` or `asset-allocation-runtime-common` refs.
- `release.yml` builds the API image, exports contract artifacts, writes `release-manifest.json`, and dispatches `control_plane_released` to jobs.
- `deploy-prod.yml` is the only runtime deploy path for `asset-allocation-api`; manual runs auto-resolve the latest released ACR image, while `deploy_runtime` repository dispatch remains the explicit-digest path for automation and rollback.
- `infra-shared-prod.yml` is the only workflow allowed to mutate shared Azure runtime substrate.
- `scripts/dev/setup-env.ps1` builds repo-local `.env.web` using contract defaults and existing values.
- `scripts/repo/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.

## Backtesting Operations

Backtest lifecycle state is owned here and the shared payloads live in `asset-allocation-contracts`.

- `GET /api/internal/backtests/ready` is the authenticated, non-mutating readiness check for trusted backtest consumers. It validates auth and Postgres connectivity before a job attempts claim or dispatch.
- `POST /api/internal/backtests/runs/reconcile` is the internal recovery endpoint used by the jobs repo scheduled reconcile task.
- Reconcile dispatches old queued runs that never received an execution, re-dispatches queued runs whose recorded ACA execution is missing or terminal, and fails stale running runs when no active execution still exists.
- Backtest read responses expose additive v4 metadata where needed. Summary payloads include cost-drag, exposure, and closed-position quality metrics; timeseries points expose `period_return`; rolling points expose `window_periods`; trades expose `position_id` and `trade_role`; and `GET /api/backtests/{run_id}/positions/closed` returns the flat-to-flat position lifecycle surface.
- `monitoring/system_health.py` continues to be the operator surface. Backtest-specific queue depth, oldest queued age, running count, stale heartbeat count, dispatch-failure proxy count, and completion duration signals are attached to the `backtests-job` resource when that job is included in `SYSTEM_HEALTH_ARM_JOBS`.
