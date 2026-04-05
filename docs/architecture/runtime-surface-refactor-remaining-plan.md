# Runtime Surface Refactor: Remaining Implementation Plan

Completion note:
- As of April 4, 2026, `WI-RSR-001` through `WI-RSR-006` are complete in the current worktree.
- This document now serves as historical reference for the implementation sequence rather than an active future-work plan.
- Current source-of-truth status lives in [runtime-surface-refactor-ledger.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-refactor-ledger.md).
- Current extraction-readiness references live in:
  - [runtime-surface-test-targets.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-test-targets.md)
  - [runtime-surface-extraction-manifest.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-extraction-manifest.md)
  - [runtime-surface-ci-matrix.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-ci-matrix.md)

Historical note:
- The sections below record the plan and sequencing used to complete the refactor.
- They should not be interpreted as open work unless the ledger is updated to reopen a work item explicitly.

## Baseline

- Architectural source of truth: [adr-001-runtime-surfaces.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/adr-001-runtime-surfaces.md)
- Work-item status and evidence: [runtime-surface-refactor-ledger.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-refactor-ledger.md)
- Verified baseline before remaining work:
  - `python -m pytest` -> `898 passed, 3 skipped`
  - `pnpm exec vitest run` -> `34 files passed, 166 tests passed`
  - `tests/architecture/test_python_module_boundaries.py -q` -> passing
- Current `WI-RSR-003` baseline:
  - `api/endpoints/system_modules/status_read.py` extracted
  - `api/endpoints/system_modules/domain_metadata.py` extracted
  - `api/endpoints/system_modules/domain_columns.py` extracted
  - `api/endpoints/system_modules/purge.py` extracted
  - `api/endpoints/system_modules/runtime_ops.py` extracted
  - `api/endpoints/system_modules/container_apps.py` extracted
  - `api/endpoints/system_modules/jobs.py` extracted
  - `api/endpoints/system.py` remains the public facade and monkeypatch surface, and `WI-RSR-003` is complete
- Current `WI-RSR-004` baseline:
  - `monitoring/system_health_modules/env_config.py` extracted
  - `monitoring/system_health_modules/signals.py` extracted
  - `monitoring/system_health_modules/job_queries.py` extracted
  - `monitoring/system_health.py` remains the public facade and compatibility surface

## Global Constraints

- Preserve external contracts:
  - API routes
  - response payload shapes
  - UI routes
  - `python -m tasks...` entrypoints
  - env var names
  - deploy and storage contracts
- Keep compatibility import surfaces stable while files are split:
  - `api.endpoints.system`
  - `monitoring.system_health`
  - `ui/src/app/App.tsx`
- Update [runtime-surface-refactor-ledger.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-refactor-ledger.md) after every agent action.
- Do not start `WI-RSR-005` until `WI-RSR-003` and `WI-RSR-004` are stable.
- Do not start `WI-RSR-006` until the structural refactors are stable.

## Execution Order

Completed in this order:

1. `WI-RSR-004` split [system_health.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/monitoring/system_health.py)
2. `WI-RSR-005` reorganize UI into feature folders while keeping [App.tsx](/mnt/c/Users/rdpro/Projects/AssetAllocation/ui/src/app/App.tsx) as shell
3. `WI-RSR-006` add extraction-readiness packaging and per-surface test targets

## WI-RSR-003: System Endpoint Decomposition

Completed. This section remains as historical reference for the module layout and validation pattern already used successfully in `api.endpoints.system`.

### Objective

Split [system.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/api/endpoints/system.py) by route concern and helper ownership without changing route behavior or breaking tests that import helpers from `api.endpoints.system`.

### Current Evidence

- File size: approximately 6061 lines after the first three route extractions
- Route clusters currently present:
  - purge rules and purge operations
  - runtime config
  - debug symbols
  - container apps
  - jobs
- Already extracted into `api/endpoints/system_modules/`:
  - status and lineage read surface
  - domain metadata and snapshot surface
  - domain columns surface
- Tests currently depend on both routes and internal helpers exposed from `api.endpoints.system`

### Required Compatibility Rules

- Keep [system.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/api/endpoints/system.py) as the public module.
- Do not create `api/endpoints/system/` as a package. Use `api/endpoints/system_modules/` instead.
- Do not introduce `shared.py` at this stage. Shared helper relocation is explicitly deferred because it increases break risk for the test-facing facade.
- `system.py` must continue to expose:
  - `router`
  - request and response models referenced by tests
  - helper functions referenced by tests
  - constants referenced by tests

### Target Structure

Create `api/endpoints/system_modules/` with:
- `__init__.py`
- `status_read.py`
  - `/health`
  - `/status-view`
  - `/symbol-sync-state`
  - `/lineage`
- `domain_metadata.py`
  - domain metadata cache helpers
  - snapshot cache helpers
  - `/domain-metadata`
  - `/domain-metadata/snapshot`
  - snapshot cache endpoints
- `domain_columns.py`
  - domain column cache helpers
  - schema discovery helpers
  - `/domain-columns`
  - `/domain-columns/refresh`
- `purge.py`
  - purge request models
  - purge rule helpers
  - purge candidate helpers
  - purge symbol cleanup helpers
  - domain list and checkpoint reset helpers
  - `/purge*`
  - `/purge-rules*`
  - `/domain-lists*`
  - `/domain-checkpoints/reset`
- `runtime_ops.py`
  - runtime config routes
  - debug symbol routes
- `container_apps.py`
  - container app health helpers
  - container app list/log/start/stop routes
- `jobs.py`
  - job run/suspend/stop/resume routes
  - job log routes
  - job-control helpers

### Extraction Sequence

1. Keep the already-extracted `status_read.py`, `domain_metadata.py`, and `domain_columns.py` slices intact.
2. Extract `purge.py`.
3. Extract `runtime_ops.py`.
4. Extract `container_apps.py`.
5. Extract `jobs.py`.
6. Reduce `system.py` to:
   - root imports
   - `router = APIRouter()`
   - `include_router()` calls
   - compatibility re-exports for tests

### Validation Matrix

- After `purge.py`:
  - `python -m pytest tests/api/test_system_purge_audit_rule_helpers.py tests/api/test_system_purge_candidates_operations.py tests/api/test_system_purge_parallelism.py tests/api/test_system_purge_symbol_cleanup.py -q`
- After `runtime_ops.py`:
  - `python -m pytest tests/api/test_runtime_config_endpoints.py tests/api/test_debug_symbols_endpoints.py -q`
  - `python -m pytest tests/api/test_swagger_docs.py -q`
 - After `container_apps.py`:
  - `python -m pytest tests/api/test_system_container_apps_endpoints.py -q`
 - After `jobs.py`:
  - `python -m pytest tests/api/test_system_job_logs_endpoints.py -q`
- At completion:
  - `python -m pytest tests/api/test_system*.py -q`
  - `python -m pytest`

### Definition of Done

- [system.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/api/endpoints/system.py) acts as a facade and compatibility layer
- no replacement module exceeds roughly 1500 lines
- all route paths and response behavior remain unchanged
- all existing `tests/api/test_system*.py` tests pass

## WI-RSR-004: System Health Decomposition

### Objective

Split [system_health.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/monitoring/system_health.py) into focused modules while preserving `monitoring.system_health` as the public import surface for tests and callers.

### Current Evidence

- File size: 2333 lines
- Tests currently import:
  - `collect_system_health_snapshot`
  - dataclasses such as `MarkerProbeConfig` and `JobScheduleMetadata`
  - private helpers such as `_resolve_freshness_policy`, `_compute_layer_status`, `_default_layer_specs`, and `_resolve_domain_schedule`
- Already extracted into `monitoring/system_health_modules/`:
  - `env_config.py`
  - `signals.py`
  - `job_queries.py`

### Required Compatibility Rules

- Keep [system_health.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/monitoring/system_health.py) as the public module.
- Do not change snapshot payload keys or alert semantics.
- Re-export test-referenced dataclasses and helpers from `monitoring.system_health`.

### Target Structure

Create `monitoring/system_health_modules/` with:
- `__init__.py`
- `env_config.py`
  - env parsing helpers
  - defaults
  - `FreshnessPolicy`
  - `MarkerProbeConfig`
  - `JobScheduleMetadata`
  - `BronzeSymbolJumpThreshold`
- `signals.py`
  - signal normalization
  - percent signal derivation
  - resource status combination helpers
- `job_queries.py`
  - KQL escaping
  - recent job query helpers
  - retry-symbol metadata queries
  - bronze symbol jump queries
  - bronze finance zero-write queries
- `freshness.py`
  - marker probe config
  - marker and blob timestamp resolution
  - schedule resolution
  - layer freshness evaluation
  - `DomainTimestampResolution`
- `alerts.py`
  - alert id helpers
  - job failure alerts
  - bronze symbol jump alerts
  - bronze finance zero-write alerts
- `snapshot.py`
  - top-level orchestration
  - `collect_system_health_snapshot`

### Extraction Sequence

1. Keep the already-extracted `env_config.py`, `signals.py`, and `job_queries.py` slices intact.
2. Move freshness and schedule logic into `freshness.py`.
3. Move alert builders into `alerts.py`.
4. Move orchestration plus presentation helpers into `snapshot.py`.
5. Reduce `system_health.py` to re-exports plus `collect_system_health_snapshot`.

### Validation Matrix

- After env/freshness extraction:
  - `python -m pytest tests/monitoring/test_system_health_staleness.py -q`
- After query, alerts, and snapshot extraction:
  - `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_phase3b_signals.py -q`
- After any change touching `_default_layer_specs` or blob freshness behavior:
  - `python -m pytest tests/tasks/test_blob_freshness.py -q`
- At completion:
  - `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q`
  - `python -m pytest`

### Definition of Done

- [system_health.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/monitoring/system_health.py) acts as a facade and compatibility layer
- no replacement module exceeds roughly 900 lines
- current monitoring health tests pass unchanged

## WI-RSR-005: UI Feature-Surface Reorganization

### Objective

Move page-oriented UI code into feature folders while keeping [App.tsx](/mnt/c/Users/rdpro/Projects/AssetAllocation/ui/src/app/App.tsx) as the shell and provider owner.

### Current Evidence

- `App.tsx` currently owns:
  - providers
  - auth callback handling
  - protected app shell
  - route transition indicator
  - lazy route declarations
- `ui/src/app/components/pages` still contains most page entry components

### Required Compatibility Rules

- Keep route paths unchanged.
- Keep `App.tsx` responsible for providers, auth routes, shell layout, and route rendering.
- Do not move shared layout, `ui`, or common components into feature folders.

### Target Structure

Add `ui/src/app/routes.tsx` and `ui/src/features/` with:
- `system-status/`
- `data-explorer/`
- `postgres-explorer/`
- `symbol-purge/`
- `runtime-config/`
- `debug-symbols/`
- `strategies/`
- `universes/`
- `rankings/`
- `regimes/`
- `stocks/`
- `data-quality/`
- `data-profiling/`

Keep shared assets in:
- `ui/src/app/components/layout`
- `ui/src/app/components/ui`
- `ui/src/app/components/common`

### Extraction Sequence

1. Add `ui/src/app/routes.tsx` and move lazy imports and `<Route>` declarations there.
2. Update `App.tsx` to render `AppRoutes`.
3. Move page entries from `ui/src/app/components/pages` into `ui/src/features/<feature>/`.
4. Move page-local helpers and subcomponents into the same feature folder when they are not shared.
5. Leave shared shell components in place.

### Validation

- Run page-level Vitest files affected by each move.
- At completion:
  - `pnpm exec vitest run`

### Definition of Done

- `App.tsx` contains shell concerns only
- route declarations live in `ui/src/app/routes.tsx`
- feature pages are owned by `ui/src/features/*`
- full Vitest suite passes

### 2026-04-04 status

- Route-level feature ownership is complete and the next UI refactor wave is now closed:
  - `ui/src/features/symbol-purge/SymbolPurgeByCriteriaPage.tsx` is a thin composition entrypoint backed by feature-local `components/`, `hooks/`, and `lib/`.
  - `ui/src/features/strategy-exploration/StrategyDataCatalogPage.tsx` is a thin composition entrypoint backed by feature-local `components/`, `hooks/`, and `lib/`.
  - `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` is now a compatibility wrapper over `ui/src/features/system-status/domain-layer-comparison/DomainLayerComparisonPanel.tsx`.
- Validation closed green for the UI wave:
  - `npm exec vitest -- run src/app/__tests__/StrategyDataCatalogPage.test.tsx src/app/__tests__/SymbolPurgeByCriteriaPage.test.tsx src/app/__tests__/DomainLayerComparisonPanel.test.tsx src/app/__tests__/SystemStatusPage.test.tsx`
  - `npm exec vite -- build`
- Remaining UI work is no longer page-entry ownership. Future UI cleanup should target deeper decomposition of the feature-owned `domain-layer-comparison` implementation if it remains a hotspot.

## WI-RSR-006: Extraction-Readiness Packaging

### Objective

Document extraction-readiness boundaries and per-surface test targets after the structural refactors are stable.

### Required Artifacts

- `docs/architecture/runtime-surface-test-targets.md`
  - per-surface validation commands
  - owners
  - trigger conditions
- `docs/architecture/runtime-surface-extraction-manifest.md`
  - runtime surfaces
  - public entrypoints
  - current blocking shared dependencies
  - extraction order assumptions
- `docs/architecture/runtime-surface-ci-matrix.md`
  - minimal CI matrix keyed to runtime surfaces

### Required Content

- surfaces:
  - `core`
  - `api`
  - `monitoring`
  - `tasks`
  - `ui`
- runnable commands for each surface
- unresolved coupling that still blocks extraction
- recommended extraction sequence

### Validation

- verify each documented command runs in the current repo
- run `python -m pytest`
- run `pnpm exec vitest run` if UI docs or structure changed

### Definition of Done

- extraction docs exist and are runnable against current repo state
- ledger records remaining blockers after the structural refactors

## Ledger Rules For Remaining Work

After every agent action:
- update [runtime-surface-refactor-ledger.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/docs/architecture/runtime-surface-refactor-ledger.md)
- record:
  - timestamp
  - acting agent
  - work item id
  - action taken
  - evidence produced
  - state transition
  - next step
- increment rework loop count only when a validation gate fails after code changes

Before a work item moves to `Done`:
- the work-item-specific validation matrix must pass
- `python -m pytest` must pass
- `pnpm exec vitest run` must pass for any UI-changing item

## Immediate Next Step

Begin `WI-RSR-004` by:
1. creating `monitoring/system_health_modules/freshness.py`
2. moving the marker-probe, schedule, layer-spec, and layer-status helpers into that module
3. keeping [system_health.py](/mnt/c/Users/rdpro/Projects/AssetAllocation/monitoring/system_health.py) as the facade plus compatibility re-export and monkeypatch layer
4. validating with:
   - `python -m pytest tests/monitoring/test_system_health_staleness.py -q`
   - `python -m pytest tests/tasks/test_blob_freshness.py -q`
   - `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_phase3b_signals.py tests/monitoring/test_system_health_staleness.py tests/tasks/test_blob_freshness.py -q`
