# Original Monolith and Five-Repo System Map

## Scope

This document describes two states of the same system:

- The original monolith in `asset-allocation` at commit `10c951778e9d61b6acb13335577ddc6cace89df2`
- The current split system across these five sibling repos in `Projects/`:
  - `asset-allocation-contracts`
  - `asset-allocation-runtime-common`
  - `asset-allocation-control-plane`
  - `asset-allocation-jobs`
  - `asset-allocation-ui`

The goal is to explain what the original application did, how it was structured, and where each major capability now lives after the split.

## What the Original Application Did

The original AssetAllocation application was an Azure-oriented market data and operator platform. It combined data ingestion, batch processing, operational controls, and a browser-based operator console in one repository.

At a high level, it did five things:

1. Ingested market, finance, earnings, and price-target data from external providers.
2. Materialized bronze, silver, and gold datasets in Delta tables on Azure Data Lake Storage.
3. Exposed a FastAPI service for data inspection, system health, provider gateway access, runtime control, and strategy management.
4. Served a React/Vite operator UI for health, data exploration, data quality, debug-symbol management, runtime config, Postgres exploration, and strategy workflows.
5. Deployed the API, UI, and scheduled batch jobs onto Azure Container Apps and Azure Container App Jobs with OIDC and Postgres-backed runtime control state.

## How the Original Monolith Was Structured

The monolith grouped all major concerns into one repo:

| Monolith Path | Responsibility | Why It Existed |
| --- | --- | --- |
| `api/` | FastAPI transport, auth, operator/data endpoints, `/config.js`, websocket and realtime tickets | Exposed the control plane and operator-facing backend |
| `tasks/` | Batch ETL, medallion jobs, backtesting worker, job orchestration helpers | Performed scheduled and on-demand data processing |
| `ui/` | React/Vite single-page application | Gave operators a browser-based console |
| `core/` | Shared backend logic such as runtime config, debug symbols, repository classes, Delta helpers, auth/client helpers | Supported both API and job runtime behavior |
| `alpha_vantage/`, `massive_provider/`, `alpaca/` | Provider adapters and ingestion utilities | Connected the platform to external market and finance data sources |
| `monitoring/` | Health, status, diagnostics, observability utilities | Powered runtime health and operational reporting |
| `deploy/` | Azure Container App and job manifests | Defined how the application ran in Azure |
| `scripts/` | Local run, provisioning, validation, automation, deployment helpers | Operational entrypoints for developers and operators |
| `tests/` | API, job, deploy, contract, and runtime tests | Verified behavior across the combined stack |

## How the Original System Worked End to End

1. Scheduled jobs in `tasks/` pulled data from providers and wrote bronze, silver, and gold Delta outputs.
2. Job-side shared logic in `core/` handled concerns such as job locks, watermarks, publication rules, runtime config application, and operator-facing metadata handling.
3. The FastAPI app in `api/service/app.py` exposed data, system, strategy, provider, and websocket surfaces, and read runtime control state such as debug symbols and runtime overrides.
4. The UI in `ui/` booted from `/config.js`, called the API for operator workflows, and listened to realtime updates.
5. Azure deployment packaged the API and UI into a Container App deployment and ran scheduled job manifests from `deploy/job_*.yaml`.

## What Changed in the Split

The split did not create a new product. It reorganized the same application into clearer ownership boundaries:

- Shared schemas and public data contracts were extracted into `asset-allocation-contracts`.
- Transport-neutral shared backend helpers were extracted into `asset-allocation-runtime-common`.
- API, operator state, and monitoring stayed together in `asset-allocation-control-plane`.
- Batch ETL, provider adapters, and job orchestration moved into `asset-allocation-jobs`.
- The operator SPA moved into `asset-allocation-ui`.

The business capability stayed the same. The packaging, release model, and cross-repo boundaries changed.

## New Five-Repo Structure

| Repo | Current Responsibility | Lineage From the Monolith |
| --- | --- | --- |
| `asset-allocation-contracts` | Shared Python and TypeScript contracts, schemas, compatibility fixtures | Extracted from data shapes that were previously embedded across `api/`, `core/`, `tasks/`, and `ui/` |
| `asset-allocation-runtime-common` | Transport-neutral auth helpers, control-plane HTTP transport, and read-only runtime client repositories | Extracted from reusable, side-effect-free parts of the original `core/` |
| `asset-allocation-control-plane` | FastAPI API, operator endpoints, monitoring, Postgres-backed runtime control state, deploy and ops surfaces | Inherits the monolith's `api/`, `monitoring/`, and control-plane-owned `core/` behavior |
| `asset-allocation-jobs` | Batch ETL, provider adapters, backtesting worker, job orchestration, job-side monitoring | Inherits the monolith's `tasks/`, provider modules, and job-side `core/` behavior |
| `asset-allocation-ui` | Standalone operator UI and UI-specific deploy/bootstrap surfaces | Inherits the monolith's `ui/` application |

## Functional Mapping From Monolith to Split Repos

| Original Capability | Monolith Surface | Current Repo Ownership |
| --- | --- | --- |
| Operator/data API | `api/` | `asset-allocation-control-plane` |
| Health, readiness, diagnostics, websocket status | `api/`, `monitoring/` | `asset-allocation-control-plane` |
| Runtime config, debug symbols, strategy, universe, ranking, regime operator state | `core/` plus API routes | `asset-allocation-control-plane` for owned state, `asset-allocation-runtime-common` for read-only client helpers, `asset-allocation-contracts` for shared schemas |
| Market/finance/earnings/price-target ETL | `tasks/` | `asset-allocation-jobs` |
| Backtesting worker | `tasks/backtesting/worker.py` | `asset-allocation-jobs` |
| External provider adapters | `alpha_vantage/`, `massive_provider/`, `alpaca/` | `asset-allocation-jobs` |
| Operator UI pages and browser auth flow | `ui/` | `asset-allocation-ui`, with shared data shapes from `asset-allocation-contracts` |
| Shared UI/runtime contracts | implicit cross-cutting shapes in `api/`, `core/`, `tasks/`, `ui/` | `asset-allocation-contracts` |
| Shared runtime auth and HTTP transport helpers | reusable parts of `core/` | `asset-allocation-runtime-common` |
| Deploy manifests and runtime-specific release logic | `deploy/`, `.github/workflows`, `scripts/` | Mainly `asset-allocation-control-plane`, `asset-allocation-jobs`, and `asset-allocation-ui`, with package publication owned by `asset-allocation-contracts` and `asset-allocation-runtime-common` |

## Current Runtime Interaction Model

The split system now works like this:

1. `asset-allocation-jobs` runs the ETL and batch workloads, including provider integration and medallion processing.
2. `asset-allocation-jobs` reads operator-owned control state from the control plane over HTTP instead of owning that state directly.
3. `asset-allocation-control-plane` remains the API and operator-state owner, including health, monitoring, runtime configuration, and operator-facing management surfaces.
4. `asset-allocation-ui` remains the browser-based operator console and talks to the control plane for runtime data and operator workflows.
5. `asset-allocation-contracts` supplies the shared schema layer used by Python and TypeScript consumers.
6. `asset-allocation-runtime-common` supplies shared backend helpers such as bearer-token acquisition, control-plane transport, and read-only client repositories that both Python runtimes can use without re-owning state.

## What Stayed the Same

- The system is still a market-data and operator platform.
- The core data domains are still market, finance, earnings, and price-target workflows.
- The medallion pattern is still the main batch data model.
- The operator experience is still a web UI backed by a FastAPI control plane.
- Azure Container Apps, scheduled jobs, OIDC, and Postgres-backed runtime control remain part of the operating model.

## What Changed Operationally

- Builds and releases are now repo-local instead of monorepo-wide.
- Shared contracts are versioned artifacts instead of sibling-source imports.
- Shared backend helpers now have an explicit home in `asset-allocation-runtime-common`.
- The UI is packaged and deployed as its own repo and image.
- Cross-runtime dependencies are meant to flow through published packages or HTTP boundaries rather than through shared source trees.

## Important Boundary Note

The intended target state is five repos with clear ownership, but the backend split still carries some temporary compatibility shims. In particular, some helper modules are temporarily mirrored in the Python runtime repos while `asset-allocation-runtime-common` becomes the canonical owner. The ownership intent is documented in `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md`.

## Why This Matters

This lineage matters because the five-repo system is easiest to understand when read as a decomposition of one original application:

- `asset-allocation-control-plane` is the monolith's API and operator backend, separated out.
- `asset-allocation-jobs` is the monolith's ETL and batch runtime, separated out.
- `asset-allocation-ui` is the monolith's browser application, separated out.
- `asset-allocation-contracts` makes the old implicit shared shapes explicit.
- `asset-allocation-runtime-common` makes the old reusable backend helpers explicit.

That means new engineers should think of the split system as one product with five codebases, not five unrelated services.

## Evidence

Original monolith evidence at `10c951778e9d61b6acb13335577ddc6cace89df2`:

- `asset-allocation:README.md`
- `asset-allocation:pyproject.toml`
- `asset-allocation:api/service/app.py`
- `asset-allocation:ui/src/app/App.tsx`
- `asset-allocation:deploy/app_api_public.yaml`

Current split-repo evidence:

- `asset-allocation-contracts:README.md`
- `asset-allocation-contracts:python/asset_allocation_contracts/*.py`
- `asset-allocation-contracts:ts/src/contracts.ts`
- `asset-allocation-runtime-common:README.md`
- `asset-allocation-runtime-common:python/asset_allocation_runtime_common/*.py`
- `asset-allocation-runtime-common:docs/architecture/repo-ownership-map.md`
- `asset-allocation-control-plane:README.md`
- `asset-allocation-control-plane:pyproject.toml`
- `asset-allocation-control-plane:api/service/app.py`
- `asset-allocation-jobs:README.md`
- `asset-allocation-jobs:pyproject.toml`
- `asset-allocation-jobs:tasks/`
- `asset-allocation-ui:README.md`
- `asset-allocation-ui:package.json`
- `asset-allocation-ui:src/app/App.tsx`
