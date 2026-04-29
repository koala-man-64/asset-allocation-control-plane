# Asset Allocation Control Plane

Runtime-owned control-plane repository for:
- `api/` FastAPI transport and operator endpoints
- `monitoring/` health, status, and Azure diagnostics
- control-plane-side `core/` runtime modules

Local development installs versioned shared packages rather than sibling repos:

```powershell
python -m pip install asset-allocation-contracts==3.12.0
python -m pip install asset-allocation-runtime-common==3.5.0 --no-deps
python -m pytest tests/api tests/monitoring -q
```

Refresh the shared package pins with Codex:

```powershell
.\scripts\refresh_shared_dependencies_with_codex.ps1
.\scripts\refresh_shared_dependencies_with_codex.ps1 -ExecutionMode full-auto
```

The wrapper stores the generated prompt, console log, and final Codex summary under `artifacts/codex/shared-dependency-refresh/<timestamp>/`.

Contract artifacts can be regenerated locally with:

```powershell
python scripts/automation/export_contract_artifacts.py
```

Verify that tracked artifacts are current without rewriting files:

```powershell
python scripts/automation/run_quality_gate.py contract-artifacts
```

Install the opt-in local hook guard for this clone:

```powershell
python scripts/dev/install_git_hooks.py
```

OpenAPI-facing route model changes are contract changes. If you change request or response models, including imported shared Pydantic models used directly in FastAPI route signatures, regenerate `api/contracts/*` and commit those files in the same change.

## AI Relay

This is a contracts-repo-first change area. The control-plane now exposes `POST /api/ai/chat/stream`, an authenticated Server-Sent Events relay backed by the OpenAI Responses API for single-turn prompt execution with optional file attachments.

- Auth requires the route-specific role `AssetAllocation.AiRelay.Use`.
- Requests support `application/json` when no files are sent and `multipart/form-data` with a JSON `request` part plus repeated `files` parts when attachments are included.
- Responses stream typed SSE events: `started`, `status`, `reasoning_summary_delta`, `output_text_delta`, `completed`, and `error`.
- Runtime configuration is disabled by default. Enable it with `AI_RELAY_ENABLED=true` plus a real `AI_RELAY_API_KEY`.
- The repo currently ships a compatibility shim for the AI request and stream event models. Publish the shared `asset-allocation-contracts` AI types and then bump the package here to remove the fallback.

## Operations

Canonical workflows live under `.github/workflows/`.

- `ci.yml` is the required validation path for PRs and `main`.
- `security.yml` runs scheduled or manual dependency audits and uploads SARIF for runtime dependency findings.
- `release.yml` builds the API image, exports contract artifacts, writes `release-manifest.json`, and dispatches `control_plane_released` to jobs.
- `deploy-prod.yml` is the only runtime deploy path for `asset-allocation-api`; manual runs auto-resolve the latest released ACR image, while `deploy_runtime` repository dispatch remains the explicit-digest path for automation and rollback.
- `infra-shared-prod.yml` is the only workflow allowed to mutate shared Azure runtime substrate.
- `scripts/setup-env.ps1` builds repo-local `.env.web` using contract defaults and existing values.
- `scripts/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.

## Backtesting Operations

Backtest lifecycle state is owned here and the shared payloads live in `asset-allocation-contracts`.

- `GET /api/internal/backtests/ready` is the authenticated, non-mutating readiness check for trusted backtest consumers. It validates auth and Postgres connectivity before a job attempts claim or dispatch.
- `POST /api/internal/backtests/runs/reconcile` is the internal recovery endpoint used by the jobs repo scheduled reconcile task.
- Reconcile dispatches old queued runs that never received an execution, re-dispatches queued runs whose recorded ACA execution is missing or terminal, and fails stale running runs when no active execution still exists.
- Backtest read responses expose additive v4 metadata where needed. Summary payloads include cost-drag, exposure, and closed-position quality metrics; timeseries points expose `period_return`; rolling points expose `window_periods`; trades expose `position_id` and `trade_role`; and `GET /api/backtests/{run_id}/positions/closed` returns the flat-to-flat position lifecycle surface.
- `monitoring/system_health.py` continues to be the operator surface. Backtest-specific queue depth, oldest queued age, running count, stale heartbeat count, dispatch-failure proxy count, and completion duration signals are attached to the `backtests-job` resource when that job is included in `SYSTEM_HEALTH_ARM_JOBS`.
