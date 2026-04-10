# Control Plane Architecture

## Purpose and scope

This document is the canonical, repo-specific design and ownership contract for `asset-allocation-control-plane`.

- Confirmed: this repository is the runtime-owned control-plane repository for FastAPI transport, operator endpoints, monitoring, and control-plane-side runtime modules.
- Confirmed: this document is current-repo-first. When older lineage or refactor documents conflict with current code, tests, or workflows, current repo state wins.
- Confirmed: subordinate documents remain authoritative for their narrower scope:
  - `docs/architecture/adr-001-runtime-surfaces.md` for boundary policy
  - `docs/architecture/runtime-surface-extraction-manifest.md` for facade and extracted-surface inventory
  - `docs/architecture/runtime-surface-ci-matrix.md` and `docs/architecture/runtime-surface-test-targets.md` for validation command references
  - `docs/architecture/runtime-surface-refactor-ledger.md` for historical execution evidence
  - `DEPLOYMENT_SETUP.md` for deploy, operate, and rollback runbook detail
- Confirmed: no code, API, schema, or workflow behavior changes are defined by this document. It records the currently intended architecture and the tests and workflows that enforce it.

Evidence:
- `README.md`
- `pyproject.toml`
- `docs/architecture/adr-001-runtime-surfaces.md`
- `docs/architecture/runtime-surface-extraction-manifest.md`
- `docs/architecture/runtime-surface-ci-matrix.md`
- `docs/architecture/runtime-surface-test-targets.md`
- `docs/architecture/runtime-surface-refactor-ledger.md`
- `DEPLOYMENT_SETUP.md`

## What this repo owns

- Confirmed: the repo packages and deploys the control-plane API as `asset-allocation-control-plane` on Python `>=3.14,<3.15`.
- Confirmed: the repo owns FastAPI application startup, auth configuration, request routing, health/config/docs endpoints, realtime ticketing, provider gateways, monitoring, and Postgres-backed runtime control state.
- Confirmed: the repo owns generated contract artifacts under `api/contracts/`.
- Confirmed: the repo owns the control-plane release and deploy workflows, plus the shared-infra reconcile workflow for the Azure substrate used by the control plane.
- Inference: sibling runtimes should treat this repo as the operator/control boundary over HTTP and contract artifacts, not as a shared source tree.

### Ownership matrix

| Surface | Responsibility | Current owner modules | Public facade | Compatibility-only seams | Enforcement tests / workflows |
| --- | --- | --- | --- | --- | --- |
| `api/` | HTTP transport, operator endpoints, realtime, health/config/docs surfaces | `api/service/app.py`, `api/service/*`, `api/endpoints/*` | `/healthz`, `/readyz`, `/config.js`, `/docs`, `/openapi.json`, `/api/*` | `api/endpoints/system.py` remains a facade and re-export surface | `tests/architecture/test_system_facade_guard.py`, `tests/api/*`, `.github/workflows/ci.yml` |
| `core/` | Control-plane-side runtime logic, repositories, runtime config, backtest storage, storage helpers, strategy and ranking engines | `core/*.py`, `core/ranking_engine/*`, `core/strategy_engine/*` | Python package import surface used by API and monitoring layers | Historical split overlap exists, but direct `tasks.*` imports are prohibited here | `tests/architecture/test_python_module_boundaries.py`, `tests/core/*` |
| `monitoring/` | System health collection, ARM/metrics/log analytics integration, control-plane resource inspection | `monitoring/system_health_modules/*`, `monitoring/control_plane.py`, `monitoring/*.py` | `monitoring/system_health.py` | `monitoring/system_health.py` remains the facade and patch surface | `tests/monitoring/*`, `docs/architecture/runtime-surface-ci-matrix.md` |
| `alpha_vantage/`, `massive_provider/` | API-side provider clients and helper modules used by gateway endpoints | `alpha_vantage/*.py`, `massive_provider/*.py`, `api/service/*gateway*.py` | `/api/providers/alpha-vantage/*`, `/api/providers/massive/*` | Historical lineage overlaps with jobs-side ingestion ownership | `tests/alpha_vantage/*`, `tests/api/test_alpha_vantage_endpoints.py`, `tests/api/test_massive_endpoints.py` |
| `api/contracts/*` | Generated API and UI runtime contract artifacts | `scripts/automation/export_contract_artifacts.py` | `control-plane.openapi.json`, `ui-runtime-config.schema.json` | None | `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `tests/api/test_config_js_contract.py` |
| `.github/workflows/*.yml` | Validation, release, deploy, compat, infra reconcile, and security automation | `ci.yml`, `compat.yml`, `release.yml`, `deploy-prod.yml`, `infra-shared-prod.yml`, `security.yml` | GitHub Actions workflow entrypoints | Only `compat.yml` may check out sibling repos | `tests/test_multirepo_dependency_contract.py`, `tests/test_workflow_runtime_ownership.py` |
| `deploy/` and repo ops scripts | API deploy manifests, shared infra bootstrap, env sync, local contract/export helpers | `deploy/app_api*.yaml`, `scripts/dev/setup-env.ps1`, `scripts/repo/sync-all-to-github.ps1`, `scripts/ops/*` | `deploy-prod.yml`, `infra-shared-prod.yml` | Legacy job manifests are still present; see observed mismatches below | `DEPLOYMENT_SETUP.md`, `tests/test_workflow_runtime_ownership.py`, `.github/workflows/deploy-prod.yml`, `.github/workflows/infra-shared-prod.yml` |

Evidence:
- `README.md`
- `pyproject.toml`
- `api/service/app.py`
- `api/endpoints/internal.py`
- `api/endpoints/system.py`
- `api/endpoints/system_modules/*`
- `monitoring/system_health.py`
- `monitoring/system_health_modules/*`
- `scripts/automation/export_contract_artifacts.py`
- `.github/workflows/*.yml`

## What this repo does not own

- Confirmed: this repo does not own the standalone jobs runtime. The split-system lineage assigns ETL, batch orchestration, provider ingestion, and job-side monitoring to `asset-allocation-jobs`.
- Confirmed: this repo does not own the standalone UI application or UI deployment. `UI_DIST_DIR` support is a runtime compatibility path, not the primary UI ownership model.
- Confirmed: this repo does not own the source trees for `asset-allocation-contracts` or `asset-allocation-runtime-common`. It consumes versioned packages and uses narrow compatibility workflows when validating candidate refs.
- Confirmed: normal CI and release workflows are not allowed to copy or check out sibling repos except in the dedicated compat workflow.
- Confirmed: shared Azure substrate mutation is not broadly owned by all workflows in this repo. `infra-shared-prod.yml` is the only workflow allowed to mutate the shared runtime substrate.

Evidence:
- `docs/architecture/original-monolith-and-five-repo-map.md`
- `README.md`
- `api/service/app.py`
- `DEPLOYMENT_SETUP.md`
- `tests/test_multirepo_dependency_contract.py`
- `tests/test_workflow_runtime_ownership.py`
- `.github/workflows/compat.yml`

## System overview

- Confirmed: `api/service/app.py` is the application composition root. It constructs the FastAPI app, owns lifespan startup and shutdown, wires auth and provider gateways, initializes realtime/log streaming support, and registers the route surfaces.
- Confirmed: the API surface includes system, data, Postgres, universes, strategies, rankings, regimes, backtests, internal, realtime, and provider routes.
- Confirmed: the repo also owns monitoring and control-plane inspection logic used to build richer system-health views than the simple liveness and readiness endpoints.
- Confirmed: the repo publishes runtime contract artifacts for downstream consumers.
- Inference: this control plane exists to centralize operator-visible runtime state, monitoring, and command surfaces so the UI and jobs runtimes can consume a stable control boundary instead of sharing source code.

Evidence:
- `api/service/app.py`
- `api/API_ENDPOINTS.md`
- `monitoring/control_plane.py`
- `monitoring/system_health.py`
- `scripts/automation/export_contract_artifacts.py`
- `docs/architecture/original-monolith-and-five-repo-map.md`

## Runtime composition and control flow

1. Confirmed: the container image starts `uvicorn api.service.app:app` on port `8000`.
2. Confirmed: `ServiceSettings.from_env()` validates auth-related environment inputs and distinguishes local anonymous mode from deployed OIDC-required mode.
3. Confirmed: FastAPI lifespan startup initializes `AuthManager`, provider gateways, log streaming, websocket ticket storage, and system-health cache state.
4. Confirmed: when `POSTGRES_DSN` is configured, startup applies Postgres-backed runtime config overrides and refreshes debug-symbol state into process memory and application state.
5. Confirmed: the app exposes lightweight `/healthz` and `/readyz` routes, runtime browser configuration via `/config.js`, OpenAPI/docs routes, and the `/api/*` router tree.
6. Confirmed: `api/endpoints/internal.py` exposes Postgres-backed internal control-plane reads and backtest coordination APIs for trusted runtime consumers.
7. Confirmed: `release.yml` builds and publishes one immutable API image, exports contract artifacts, writes a release manifest, and dispatches `control_plane_released` to the jobs repo.
8. Inference: the jobs repo depends on this repo both at runtime, through internal and public HTTP surfaces, and at release time, through `control_plane_released` metadata and pinned shared package versions.

Evidence:
- `Dockerfile.asset_allocation_api`
- `api/service/app.py`
- `api/service/settings.py`
- `core/runtime_config.py`
- `api/endpoints/internal.py`
- `.github/workflows/release.yml`

## Public contract surfaces

### HTTP and browser bootstrap

- Confirmed: `/healthz`
- Confirmed: `/readyz`
- Confirmed: `/config.js`
- Confirmed: docs and OpenAPI routes under `/docs`, `/openapi.json`, and the active API prefix

### Operator and runtime APIs

- Confirmed: `/api/system/*` owns system health, runtime config, debug symbols, purge operations, container app control, and job control surfaces.
- Confirmed: `/api/data/*` owns raw and derived data access surfaces.
- Confirmed: `/api/backtests/*` owns backtest submission, status, summaries, metrics, and artifact retrieval.
- Confirmed: `/api/providers/alpha-vantage/*` and `/api/providers/massive/*` own API-side provider gateway surfaces.
- Confirmed: `/api/internal/*` owns trusted internal control-plane reads and backtest worker coordination.
- Confirmed: `/api/ws/updates` and realtime ticket issuance support browser realtime updates.

### Generated artifacts and release metadata

- Confirmed: `api/contracts/control-plane.openapi.json`
- Confirmed: `api/contracts/ui-runtime-config.schema.json`
- Confirmed: `artifacts/release-manifest.json`
- Confirmed: repository dispatch event `control_plane_released`

### Workflow ownership contract

- Confirmed: `ci.yml` is the required validation path for PRs and `main`.
- Confirmed: `release.yml` is the API image and contract-artifact release path.
- Confirmed: `deploy-prod.yml` is the only runtime deploy path for `asset-allocation-api`.
- Confirmed: `infra-shared-prod.yml` is the only workflow allowed to mutate shared Azure substrate.
- Confirmed: `compat.yml` is the only sibling-repo compatibility validation workflow for candidate `asset-allocation-contracts` and `asset-allocation-runtime-common` refs.
- Confirmed: `security.yml` owns scheduled or manual dependency audit and SARIF publication for runtime dependency findings.

Evidence:
- `api/service/app.py`
- `api/API_ENDPOINTS.md`
- `api/endpoints/internal.py`
- `api/contracts/*`
- `scripts/automation/export_contract_artifacts.py`
- `README.md`
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/deploy-prod.yml`
- `.github/workflows/infra-shared-prod.yml`
- `.github/workflows/compat.yml`
- `.github/workflows/security.yml`

## Compatibility facades and transitional boundaries

- Confirmed: `api/endpoints/system.py` is intentionally a facade and compatibility surface. Extracted route clusters and helper ownership now live under `api/endpoints/system_modules/*`.
- Confirmed: `monitoring/system_health.py` is intentionally a facade and compatibility surface. Extracted helpers and collectors now live under `monitoring/system_health_modules/*`.
- Confirmed: the architecture boundary rules prohibit `api/`, `monitoring/`, and non-shim `core/` modules from importing `tasks.*`, even though older docs still describe the historical refactor path.
- Confirmed: optional `UI_DIST_DIR` static serving exists in the API container, but it should be treated as compatibility behavior rather than as primary UI ownership.
- Confirmed: refactor ledgers and manifests provide provenance and surface maps, but they are not the primary source of current runtime ownership.
- Inference: future cleanup can narrow or remove compatibility seams only when the corresponding guard tests, route consumers, and subordinate docs are updated together.

Evidence:
- `docs/architecture/adr-001-runtime-surfaces.md`
- `docs/architecture/runtime-surface-extraction-manifest.md`
- `api/endpoints/system.py`
- `api/endpoints/system_modules/*`
- `monitoring/system_health.py`
- `monitoring/system_health_modules/*`
- `tests/architecture/test_python_module_boundaries.py`
- `tests/architecture/test_system_facade_guard.py`
- `api/service/app.py`

## Deployment and operational model

- Confirmed: the intended deployment target is one control-plane Azure Container App named `asset-allocation-api`.
- Confirmed: `deploy-prod.yml` resolves a full image digest, renders `deploy/app_api_public.yaml`, applies the manifest, and verifies `/healthz`, `/readyz`, `/config.js`, and an OpenAPI endpoint after rollout.
- Confirmed: `infra-shared-prod.yml` is the shared Azure substrate reconcile path and runs the provisioning PowerShell entrypoint from this repo.
- Confirmed: `release.yml` resolves pinned shared package versions from `pyproject.toml`, exports contract artifacts, builds and pushes the API image, uploads release artifacts, and dispatches the jobs repo.
- Confirmed: the runbook requires bootstrap and sync through `scripts/dev/setup-env.ps1` and `scripts/repo/sync-all-to-github.ps1`.
- Confirmed: the runbook treats this repo as the source of truth for `/config.js` and the OpenAPI contract artifacts.

Evidence:
- `DEPLOYMENT_SETUP.md`
- `.github/workflows/release.yml`
- `.github/workflows/deploy-prod.yml`
- `.github/workflows/infra-shared-prod.yml`
- `deploy/app_api.yaml`
- `deploy/app_api_public.yaml`
- `scripts/dev/setup-env.ps1`
- `scripts/repo/sync-all-to-github.ps1`

## Observability and failure semantics

### Confirmed behaviors

| Concern | Current behavior | Evidence |
| --- | --- | --- |
| Application composition root | `api/service/app.py` owns startup, route wiring, middleware, config/bootstrap, and shutdown orchestration. New domain logic should not grow here without an explicit boundary decision. | `api/service/app.py` |
| Auth configuration | Invalid deployed auth configuration fails during settings resolution rather than silently degrading to anonymous access. | `api/service/settings.py` |
| Runtime config at startup | If `POSTGRES_DSN` is present, the app attempts to apply runtime config and refresh debug symbols. Failures are logged as warnings rather than crashing the process. | `api/service/app.py`, `core/runtime_config.py` |
| Storage auth diagnostics | Delta storage auth diagnostics are logged at startup; diagnostic resolution failure is warning-only. | `api/service/app.py` |
| Liveness | `/healthz` returns a simple `{"status":"ok"}` response. | `api/service/app.py` |
| Readiness | `/readyz` returns a simple `{"status":"ready"}` response. | `api/service/app.py` |
| Rich health view | richer runtime and Azure health aggregation lives under the system-health surfaces, not under `/healthz` or `/readyz`. | `api/endpoints/system.py`, `monitoring/system_health.py`, `monitoring/control_plane.py` |
| Shutdown | provider gateway close and log-stream shutdown are best-effort and guarded against exception leakage. | `api/service/app.py` |

### Operational intent

- Confirmed: lightweight readiness and liveness endpoints are separate from the richer system-health view.
- Confirmed: deploy verification currently relies on `/healthz`, `/readyz`, `/config.js`, and an OpenAPI path.
- Needs confirmation: whether `/readyz` should remain shallow or should eventually reflect deeper dependency readiness, such as Postgres connectivity.

Evidence:
- `api/service/app.py`
- `api/service/settings.py`
- `core/runtime_config.py`
- `api/endpoints/system.py`
- `monitoring/system_health.py`
- `.github/workflows/deploy-prod.yml`

## Validation and enforcement

### Baseline design-contract tests

- Confirmed: `tests/architecture/test_python_module_boundaries.py` enforces that `api/`, `monitoring/`, and `core/` do not import `tasks.*`.
- Confirmed: `tests/architecture/test_system_facade_guard.py` keeps `api/endpoints/system.py` as a facade rather than allowing migrated helper ownership to grow back.
- Confirmed: `tests/test_multirepo_dependency_contract.py` enforces pinned shared-package usage, blocks sibling-repo copying in the API Dockerfile, and constrains sibling checkouts to `compat.yml`.
- Confirmed: `tests/test_workflow_runtime_ownership.py` enforces the expected workflow inventory and local bootstrap path references in `DEPLOYMENT_SETUP.md`.

### Required commands for design-level changes

Run these from the repository root when a change affects architecture, boundaries, workflow ownership, or public contract surfaces:

```powershell
python -m pytest tests/architecture/test_python_module_boundaries.py tests/architecture/test_system_facade_guard.py tests/test_multirepo_dependency_contract.py tests/test_workflow_runtime_ownership.py -q
```

For broader backend or runtime work, run the full suite before marking the change complete:

```powershell
python -m pytest
```

When shared contract artifacts or OpenAPI-facing surfaces change, regenerate artifacts and fail the change on drift:

```powershell
python scripts/automation/export_contract_artifacts.py
```

Use `docs/architecture/runtime-surface-ci-matrix.md` and `docs/architecture/runtime-surface-test-targets.md` as the command catalog for more targeted surface validation.

### Current enforcement gaps

- Needs confirmation: `tests/architecture/*` are not currently wired into `ci.yml`, so the architecture boundary checks are enforced by local or targeted execution rather than by the required CI path.
- Needs confirmation: the runtime-surface CI matrix describes intended commands that are not all implemented as first-class workflow jobs.
- Needs confirmation: workflow ownership tests validate workflow inventory and some path invariants, but not all workflow permissions, trigger details, or step behavior.
- Needs confirmation: there is no dedicated test that ties `release-manifest.json` and the `control_plane_released` payload back to the pinned shared package versions beyond current workflow logic.

Evidence:
- `tests/architecture/test_python_module_boundaries.py`
- `tests/architecture/test_system_facade_guard.py`
- `tests/test_multirepo_dependency_contract.py`
- `tests/test_workflow_runtime_ownership.py`
- `.github/workflows/ci.yml`
- `docs/architecture/runtime-surface-ci-matrix.md`
- `docs/architecture/runtime-surface-test-targets.md`
- `scripts/automation/export_contract_artifacts.py`

## Known ambiguities / needs confirmation

### Observed mismatches

1. `readyz` depth
   - Confirmed: `api/API_ENDPOINTS.md` describes `/readyz` as a readiness check that checks DB connectivity.
   - Confirmed: `api/service/app.py` currently returns a shallow `{"status":"ready"}` response without a DB connectivity check.
   - Needs confirmation: whether the docs should be corrected to match current code, or readiness should be deepened to match the docs.

2. OpenAPI verification path
   - Confirmed: `api/service/app.py` and `api/API_ENDPOINTS.md` describe OpenAPI availability under `/api/openapi.json` plus top-level redirects.
   - Confirmed: `DEPLOYMENT_SETUP.md` and `.github/workflows/deploy-prod.yml` verify `/api/v1/openapi.json`.
   - Needs confirmation: which public path is intended to be the canonical deploy-time OpenAPI verification endpoint.

3. Legacy job manifests under `deploy/`
   - Confirmed: `deploy/` still contains multiple `job_*.yaml` manifests.
   - Confirmed: `DEPLOYMENT_SETUP.md` states that this repo should not deploy jobs and that `deploy-prod.yml` deploys only `asset-allocation-api`.
   - Needs confirmation: whether the job manifests are retained as historical artifacts, temporary shared references, or stale files that should eventually move or be removed.

4. Historical docs versus current tree
   - Confirmed: `docs/architecture/original-monolith-and-five-repo-map.md` and some refactor-era docs describe `tasks/` and `ui/` as part of the runtime-surface story.
   - Confirmed: the current repo tree does not contain top-level `tasks/` or `ui/` directories.
   - Needs confirmation: which older docs should remain as historical lineage only versus which should be refreshed to avoid being read as current-repo structure.

## Update contract for future agents

- Confirmed: if a change affects repo ownership, module boundaries, runtime surfaces, or compatibility seams, update this document in the same PR.
- Confirmed: if a change affects deploy, rollback, workflow ownership, or shared-infra mutation rules, update this document, `README.md`, `DEPLOYMENT_SETUP.md`, and any affected workflow tests in the same PR.
- Confirmed: if a change affects public contract artifacts or bootstrap surfaces, regenerate `api/contracts/*` and update this document and the affected tests in the same PR.
- Confirmed: if a compatibility facade is narrowed or removed, update this document, the extraction manifest, and the guard tests together.
- Confirmed: if a statement cannot be verified, do not guess. Record it as `Needs confirmation`, cite the conflicting evidence, and leave the repo in an honest state.
- Confirmed: do not use the runtime-surface ledger or historical monolith map as the primary ownership source when they conflict with current code, tests, or workflows. Treat them as provenance and lineage only.

Evidence:
- `README.md`
- `DEPLOYMENT_SETUP.md`
- `docs/architecture/adr-001-runtime-surfaces.md`
- `docs/architecture/runtime-surface-extraction-manifest.md`
- `docs/architecture/runtime-surface-refactor-ledger.md`
- `tests/architecture/test_python_module_boundaries.py`
- `tests/architecture/test_system_facade_guard.py`
- `tests/test_multirepo_dependency_contract.py`
- `tests/test_workflow_runtime_ownership.py`

## Evidence index

### Code

- `api/service/app.py`
- `api/service/settings.py`
- `api/endpoints/internal.py`
- `api/endpoints/system.py`
- `api/endpoints/system_modules/*`
- `monitoring/system_health.py`
- `monitoring/system_health_modules/*`
- `monitoring/control_plane.py`
- `core/runtime_config.py`
- `Dockerfile.asset_allocation_api`

### Docs

- `README.md`
- `DEPLOYMENT_SETUP.md`
- `api/API_ENDPOINTS.md`
- `docs/architecture/adr-001-runtime-surfaces.md`
- `docs/architecture/original-monolith-and-five-repo-map.md`
- `docs/architecture/runtime-surface-extraction-manifest.md`
- `docs/architecture/runtime-surface-ci-matrix.md`
- `docs/architecture/runtime-surface-test-targets.md`
- `docs/architecture/runtime-surface-refactor-ledger.md`

### Scripts and artifacts

- `scripts/automation/export_contract_artifacts.py`
- `scripts/dev/setup-env.ps1`
- `scripts/repo/sync-all-to-github.ps1`
- `api/contracts/control-plane.openapi.json`
- `api/contracts/ui-runtime-config.schema.json`

### Workflows

- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/deploy-prod.yml`
- `.github/workflows/infra-shared-prod.yml`
- `.github/workflows/compat.yml`
- `.github/workflows/security.yml`

### Tests

- `tests/architecture/test_python_module_boundaries.py`
- `tests/architecture/test_system_facade_guard.py`
- `tests/test_multirepo_dependency_contract.py`
- `tests/test_workflow_runtime_ownership.py`
- `tests/api/test_config_js_contract.py`
