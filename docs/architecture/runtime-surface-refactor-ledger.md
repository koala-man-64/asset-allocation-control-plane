# Runtime Surface Refactor Ledger

This file is the single source of truth for runtime-surface refactor status, assignments, decisions, and validation evidence.

Historical note:
- `.codex/gateway/ledger.md` remains historical context only for prior local orchestration activity.
- This ledger is the primary handoff artifact for `WI-RSR-*` work items.

## State Machine

`Intake -> Scoped -> Planned -> In Progress -> Needs Review -> Needs QA -> Done`

Allowed pause or terminal states:
- `Blocked`
- `Deferred`
- `Rest`

Any non-standard transition must include a reason in the action log.

## Status Board

| Work Item ID | Title | Owner | State | Priority | Dependencies | Next Action | Blockers | Rework Loop Count |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| WI-RSR-001 | Runtime boundary cutover | Delivery Engineer Agent | Done | P1 | ADR 001, `core/*` shim modules, architecture boundary test | Hold complete; next work starts at WI-RSR-003 or WI-RSR-004 | None | 0 |
| WI-RSR-002 | Silver finance pilot completion | Delivery Engineer Agent | Done | P1 | WI-RSR-001 | Hold complete; next work starts at WI-RSR-003 or WI-RSR-004 | None | 1 |
| WI-RSR-003 | System endpoint decomposition | Delivery Engineer Agent | Done | P2 | WI-RSR-001 | Hold complete; next structural work shifts to WI-RSR-004 | None | 1 |
| WI-RSR-004 | System health decomposition | Delivery Engineer Agent | Done | P2 | WI-RSR-001, WI-RSR-003 | Hold complete; next implementation work shifts to WI-RSR-005 | None | 2 |
| WI-RSR-005 | UI feature-surface reorganization | Delivery Engineer Agent | Done | P3 | WI-RSR-003, WI-RSR-004 | Hold complete; UI feature layout is now part of the validated baseline | None | 0 |
| WI-RSR-006 | Extraction-readiness packaging | Delivery Orchestrator Agent | Done | P3 | WI-RSR-003, WI-RSR-004, WI-RSR-005 | Hold complete; extraction-readiness docs and verification set are now part of the validated baseline | None | 0 |
| WI-RSR-007 | Shared foundation ownership transfer | Delivery Engineer Agent | Done | P1 | ADR 001, `core/*` owner modules, architecture boundary test | Hold complete; shared foundation ownership now lives in `core/*` and legacy `tasks.common.*` paths are compatibility-only | None | 0 |
| WI-RSR-008 | Finance ETL module package decomposition | Delivery Engineer Agent | Done | P1 | WI-RSR-002, WI-RSR-007 | Hold complete; finance helper packages now exist for silver, bronze, and gold while top-level job modules remain the stable runtime surface | None | 0 |
| WI-RSR-009 | Final `system.py` facade cutover | Delivery Engineer Agent | Done | P1 | WI-RSR-003, WI-RSR-004, architecture/system API regression suites | Hold complete; `system.py` now re-exports owner-module models and non-purge helper seams while the facade guard blocks regrowth of the migrated clusters | None | 0 |

## Work Item Details

### WI-RSR-001
- **Title:** Runtime boundary cutover
- **Objective:** Replace direct `tasks.*` imports in `api/`, `monitoring/`, and non-shim `core/` consumers with `core/*` interfaces.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P1
- **Dependencies:** ADR 001, `core/bronze_bucketing.py`, `core/layer_bucketing.py`, `core/domain_artifacts.py`, `core/domain_metadata_snapshots.py`, `core/finance_contracts.py`, `core/market_symbols.py`, `core/gold_sync_contracts.py`, `tests/architecture/test_python_module_boundaries.py`
- **Files / Surfaces:** `api/data_service.py`, `api/endpoints/data.py`, `api/endpoints/system.py`, `monitoring/domain_metadata.py`, `core/gold_column_lookup_catalog.py`
- **Acceptance Criteria:** `python -m pytest tests/architecture/test_python_module_boundaries.py -q` passes; no direct `tasks.*` imports remain in `api/`, `monitoring/`, or non-shim `core/`
- **Last Action:** Full Python validation completed with the architecture boundary test included
- **Evidence:** `python -m pytest` -> `898 passed, 3 skipped`; architecture and targeted boundary suites passed earlier in the sequence
- **Next Action:** None for this work item; queued work shifts to WI-RSR-003 and WI-RSR-004
- **Blockers:** None
- **Rework Loop Count:** 0

### WI-RSR-002
- **Title:** Silver finance pilot completion
- **Objective:** Make `tasks/finance_data/silver_finance_data.py` an orchestration entrypoint by routing parsing and frame logic through extracted modules.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P1
- **Dependencies:** WI-RSR-001
- **Files / Surfaces:** `tasks/finance_data/silver_finance_data.py`, `tasks/finance_data/silver_parsing.py`, `tasks/finance_data/silver_frames.py`, finance silver tests
- **Acceptance Criteria:** `silver_finance_data.py` is materially smaller and orchestration-focused; finance behavior and tests remain green
- **Last Action:** Imported the remaining extracted frame helper and completed end-of-milestone validation
- **Evidence:** `tests/finance_data/test_silver_finance_data.py -q` -> `22 passed`; `python -m pytest` -> `898 passed, 3 skipped`
- **Next Action:** None for this work item; queued work shifts to WI-RSR-003 and WI-RSR-004
- **Blockers:** None
- **Rework Loop Count:** 1

### WI-RSR-003
- **Title:** System endpoint decomposition
- **Objective:** Split `api/endpoints/system.py` by responsibility without changing route contracts.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P2
- **Dependencies:** WI-RSR-001
- **Files / Surfaces:** `api/endpoints/system.py`, `api/endpoints/system_modules/status_read.py`, related API system tests
- **Acceptance Criteria:** No replacement file becomes another monolith; system endpoint suites remain green
- **Last Action:** Extracted the final `jobs.py` route cluster and closed the work item with a green full Python suite
- **Evidence:** Added `api/endpoints/system_modules/jobs.py`; updated `api/endpoints/system.py` to include `_jobs_router` and preserve `trigger_job_run`, `suspend_job`, `stop_job`, `resume_job`, and `get_job_logs`; `python -m pytest tests/api/test_system_job_logs_endpoints.py -q` -> `2 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`; `python -m pytest` -> `898 passed, 3 skipped`
- **Next Action:** None for this work item; queued structural work shifts to WI-RSR-004
- **Blockers:** None
- **Rework Loop Count:** 1

### WI-RSR-004
- **Title:** System health decomposition
- **Objective:** Split `monitoring/system_health.py` into config, collectors, reducers, and presentation models.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P2
- **Dependencies:** WI-RSR-001, WI-RSR-003
- **Files / Surfaces:** `monitoring/system_health.py`, monitoring health tests
- **Acceptance Criteria:** Behavior unchanged; monitoring health suites remain green; dependency boundaries improve rather than spread
- **Last Action:** Closed the full-suite validation gate by restoring the remaining `api.endpoints.system` facade patch points and rerunning the targeted API regressions plus full Python validation
- **Evidence:** Added `monitoring/system_health_modules/freshness.py`, `monitoring/system_health_modules/alerts.py`, and `monitoring/system_health_modules/snapshot.py`; rewrote `monitoring/system_health.py` to keep constants, Azure patch points, and re-exported helper symbols while delegating collection/orchestration to the extracted modules; `python -m py_compile monitoring/system_health.py monitoring/system_health_modules/freshness.py monitoring/system_health_modules/alerts.py monitoring/system_health_modules/snapshot.py` completed successfully; `python -m pytest tests/monitoring/test_system_health_staleness.py -q` -> `9 passed`; `python -m pytest tests/tasks/test_blob_freshness.py -q` -> `1 passed`; initial combined monitoring run failed because `api.endpoints.system` no longer exposed `validate_auth` to the extracted status router runtime; added `validate_auth` back to the `api.service.dependencies` import list in `api/endpoints/system.py`; reran `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_phase3b_signals.py tests/monitoring/test_system_health_staleness.py tests/tasks/test_blob_freshness.py -q` -> `34 passed`; full `python -m pytest` initially failed because `api.endpoints.system` no longer exposed `DEFAULT_ENV_OVERRIDE_KEYS`, `list_runtime_config`, `upsert_runtime_config`, `delete_runtime_config`, `normalize_env_override`, `read_debug_symbols_state`, `replace_debug_symbols_state`, `delete_debug_symbols_state`, `build_snapshot_miss_payload`, `AzureLogAnalyticsClient`, and `timedelta`; restored those facade imports in `api/endpoints/system.py`; `python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q` -> `38 passed`; final `python -m pytest` -> `898 passed, 3 skipped`
- **Next Action:** None for this work item; next implementation work shifts to WI-RSR-005
- **Blockers:** None
- **Rework Loop Count:** 2

### WI-RSR-005
- **Title:** UI feature-surface reorganization
- **Objective:** Move UI code toward feature folders while keeping `ui/src/app/App.tsx` as the shell.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P3
- **Dependencies:** WI-RSR-001
- **Files / Surfaces:** `ui/src/app`, UI feature tests
- **Acceptance Criteria:** Route behavior unchanged; feature ownership clearer; full Vitest suite remains green
- **Last Action:** Closed the UI milestone by rerunning the documented cross-surface validation set after the extraction-readiness docs were written
- **Evidence:** Moved routed page entries into `ui/src/features/data-explorer`, `regimes`, `system-status`, `data-quality`, `data-profiling`, `debug-symbols`, `runtime-config`, `symbol-purge`, `stocks`, `postgres-explorer`, `strategies`, `universes`, `rankings`, and `strategy-exploration`; added `ui/src/app/routes.tsx`; updated `ui/src/app/App.tsx` to keep providers, auth gating, shell layout, and route-transition indicator while delegating route composition to `AppRoutes`; replaced the old `ui/src/app/components/pages/*.tsx` route-entry files with thin re-export wrappers; updated `ui/src/app/__tests__/App.test.tsx` and `ui/src/app/__tests__/App.auth.test.tsx` to mock the feature entry modules; final documented validation set passed: `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q` -> `38 passed`; `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q` -> `34 passed`; `python -m pytest tests/finance_data/test_silver_finance_data.py -q` -> `22 passed`; `python -m pytest` -> `898 passed, 3 skipped`; `pnpm exec vitest run` from `ui/` -> `34 files passed, 166 tests passed`
- **Next Action:** None for this work item; UI feature layout is now part of the validated refactor baseline
- **Blockers:** None
- **Rework Loop Count:** 0

### WI-RSR-006
- **Title:** Extraction-readiness packaging
- **Objective:** Define per-surface test targets, packaging boundaries, and extraction manifests once the structural refactor is stable.
- **Owner:** Delivery Orchestrator Agent
- **State:** Done
- **Priority:** P3
- **Dependencies:** WI-RSR-001, WI-RSR-002, WI-RSR-003, WI-RSR-004, WI-RSR-005
- **Files / Surfaces:** Packaging/test-target docs and manifests
- **Acceptance Criteria:** Each runtime surface has a clear test target and extraction map; no packaging work starts before boundaries are stable
- **Last Action:** Added the extraction-readiness docs and verified every documented command against the current repository layout
- **Evidence:** Added `docs/architecture/runtime-surface-test-targets.md`, `docs/architecture/runtime-surface-extraction-manifest.md`, and `docs/architecture/runtime-surface-ci-matrix.md`; verified every documented command successfully: `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q` -> `38 passed`; `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q` -> `34 passed`; `python -m pytest tests/finance_data/test_silver_finance_data.py -q` -> `22 passed`; `python -m pytest` -> `898 passed, 3 skipped`; `pnpm exec vitest run` from `ui/` -> `34 files passed, 166 tests passed`
- **Next Action:** None for this work item; extraction-readiness docs are now part of the validated refactor baseline
- **Blockers:** None
- **Rework Loop Count:** 0

### WI-RSR-007
- **Title:** Shared foundation ownership transfer
- **Objective:** Move shared contracts/helpers out of `tasks.common.*` and into `core/*`, leaving `tasks.common.*` as compatibility-only wrappers.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P1
- **Dependencies:** ADR 001, `core/run_manifests.py`, `core/bronze_bucketing.py`, `core/layer_bucketing.py`, `core/domain_artifacts.py`, `core/domain_metadata_snapshots.py`, `core/finance_contracts.py`, `core/market_symbols.py`, `core/gold_sync_contracts.py`, `tests/architecture/test_python_module_boundaries.py`
- **Files / Surfaces:** shared foundation modules, affected task consumers, architecture/core/task wrapper tests, ADR 001
- **Acceptance Criteria:** `core/*` owns the targeted shared implementations; targeted `tasks.common.*` modules are compatibility-only; `python -m pytest tests/architecture/test_python_module_boundaries.py -q` passes without a core shim allowlist; runtime behavior remains unchanged
- **Last Action:** Completed the ownership transfer, rewrote internal imports to `core/*`, converted legacy modules into compatibility wrappers, and closed the work item with a green full Python suite
- **Evidence:** Added `core/run_manifests.py`; replaced shim behavior in `core/finance_contracts.py`, `core/market_symbols.py`, `core/domain_metadata_snapshots.py`, `core/bronze_bucketing.py`, `core/layer_bucketing.py`, `core/domain_artifacts.py`, and `core/gold_sync_contracts.py`; converted the corresponding `tasks.common.*` modules into transitional wrappers; added direct owner tests in `tests/core/`; added wrapper smoke coverage in `tests/tasks/common/` and `tests/tasks/`; `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; targeted shared-foundation and consumer suites -> `37 passed`, `8 passed`, and `125 passed`; full `python -m pytest` -> `909 passed, 3 skipped`
- **Next Action:** None for this work item; follow-on cleanup can remove the legacy wrappers once task call sites no longer need them
- **Blockers:** None
- **Rework Loop Count:** 0

### WI-RSR-008
- **Title:** Finance ETL module package decomposition
- **Objective:** Establish `silver_modules/*`, `bronze_modules/*`, and `gold_modules/*` as the decomposed finance helper namespace without breaking the stable top-level finance job entrypoints.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P1
- **Dependencies:** WI-RSR-002, WI-RSR-007
- **Files / Surfaces:** `tasks/finance_data/silver_modules/*`, `tasks/finance_data/bronze_modules/*`, `tasks/finance_data/gold_modules/*`, `tasks/finance_data/silver_parsing.py`, `tasks/finance_data/silver_frames.py`, finance ETL tests and extraction docs
- **Acceptance Criteria:** `silver_modules/*`, `bronze_modules/*`, and `gold_modules/*` exist and expose the decomposed finance helper surfaces; `silver_parsing.py` and `silver_frames.py` delegate to `silver_modules/*`; the top-level finance job modules remain the runtime entrypoint and monkeypatch surface; finance validation remains green
- **Last Action:** Added the finance helper packages, converted the legacy silver helper files into compatibility wrappers, added package-surface smoke coverage, and closed the work item with green finance and boundary suites
- **Evidence:** Added `tasks/finance_data/silver_modules/`, `tasks/finance_data/bronze_modules/`, and `tasks/finance_data/gold_modules/`; moved the silver parsing and frame implementations under `silver_modules/`; converted `tasks/finance_data/silver_parsing.py` and `tasks/finance_data/silver_frames.py` into explicit compatibility wrappers; added `tests/finance_data/test_finance_module_packages.py`; `python -m pytest tests/finance_data/test_finance_module_packages.py tests/finance_data/test_silver_finance_data.py tests/finance_data/test_bronze_finance_data.py tests/finance_data/test_gold_finance_delta_write.py tests/finance_data/test_feature_generator.py tests/tasks/test_reconciliation_contracts.py tests/tasks/test_job_entrypoint_contracts.py tests/tasks/test_postgres_gold_sync.py -q` -> `102 passed`; `python -m pytest tests/finance_data -q` -> `64 passed`; `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`
- **Next Action:** None for this work item; a later refactor wave can invert full ownership from the top-level job modules into the package modules once the test patch surface is reduced
- **Blockers:** None
- **Rework Loop Count:** 0

### WI-RSR-009
- **Title:** Final `system.py` facade cutover
- **Objective:** Move route-owned request/response models and non-purge helper clusters out of `api/endpoints/system.py` and into `api/endpoints/system_modules/*` while preserving `api.endpoints.system` as the import and monkeypatch surface.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P1
- **Dependencies:** WI-RSR-003, WI-RSR-004, `tests/api/*system*`, `tests/architecture/test_python_module_boundaries.py`
- **Files / Surfaces:** `api/endpoints/system.py`, `api/endpoints/system_modules/status_read.py`, `api/endpoints/system_modules/domain_metadata.py`, `api/endpoints/system_modules/domain_columns.py`, `api/endpoints/system_modules/runtime_ops.py`, `api/endpoints/system_modules/container_apps.py`, `api/endpoints/system_modules/jobs.py`, `api/endpoints/system_modules/purge.py`, `tests/architecture/test_system_facade_guard.py`, `docs/architecture/runtime-surface-extraction-manifest.md`
- **Acceptance Criteria:** `system.py` owns no top-level Pydantic models; migrated helper clusters live in `system_modules/*`; `api.endpoints.system` still exposes the legacy patch surface; architecture, targeted API, monitoring, and full-suite validation stay green
- **Last Action:** Moved the route-owned models plus the status/domain-metadata/domain-columns/container-apps/jobs helper implementations into their owner modules, rewired the facade to re-export those seams, added a facade guard test, and reran the broad API plus full Python validation
- **Evidence:** `python -m py_compile api/endpoints/system.py api/endpoints/system_modules/status_read.py api/endpoints/system_modules/domain_metadata.py api/endpoints/system_modules/domain_columns.py api/endpoints/system_modules/runtime_ops.py api/endpoints/system_modules/container_apps.py api/endpoints/system_modules/jobs.py api/endpoints/system_modules/purge.py` completed successfully; `python -m pytest tests/architecture/test_system_facade_guard.py -q` -> `2 passed`; `python -m pytest tests/monitoring/test_system_health.py -q` -> `22 passed`; `python -m pytest tests/api/test_system_domain_metadata_cache.py -q` -> `18 passed`; `python -m pytest tests/api/test_system_domain_columns_cache.py -q` -> `6 passed`; `python -m pytest tests/api/test_runtime_config_endpoints.py tests/api/test_debug_symbols_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_job_logs_endpoints.py -q` -> `20 passed`; `python -m pytest tests/api/test_system_purge_audit_rule_helpers.py tests/api/test_system_purge_candidates_operations.py tests/api/test_system_purge_parallelism.py tests/api/test_system_purge_symbol_cleanup.py -q` -> `31 passed`; `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `python -m pytest tests/api -q` -> `213 passed`; `python -m pytest -q` -> `915 passed, 3 skipped`
- **Next Action:** None for this work item; any future cleanup can target the remaining purge execution helpers still kept on the facade patch surface
- **Blockers:** None
- **Rework Loop Count:** 0

### WI-RSR-010
- **Title:** Split the largest UI feature pages and panels
- **Objective:** Break the largest routed UI surfaces into feature-local `components`, `hooks`, and `lib` ownership units while preserving the current wrapper imports, routes, and visible behavior.
- **Owner:** Delivery Engineer Agent
- **State:** Done
- **Priority:** P1
- **Dependencies:** WI-RSR-005, UI feature tests, `ui/src/app/routes.tsx`
- **Files / Surfaces:** `ui/src/features/symbol-purge/*`, `ui/src/features/strategy-exploration/*`, `ui/src/features/system-status/domain-layer-comparison/*`, `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`, UI architecture docs
- **Acceptance Criteria:** `SymbolPurgeByCriteriaPage.tsx` and `StrategyDataCatalogPage.tsx` are thin composition files; `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` is a compatibility wrapper; feature behavior, wrapper imports, and lazy routes remain unchanged; targeted Vitest suites and the Vite production build remain green
- **Last Action:** Split `symbol-purge` and `strategy-exploration` into feature-local controller/hooks/components/lib files, moved the domain-layer comparison panel ownership into `ui/src/features/system-status/domain-layer-comparison/`, added feature-local helper tests, and closed the work item with green targeted UI suites plus a production build
- **Evidence:** Added `ui/src/features/symbol-purge/components/`, `ui/src/features/symbol-purge/hooks/useSymbolPurgeController.ts`, and `ui/src/features/symbol-purge/lib/symbolPurge.ts`; added `ui/src/features/strategy-exploration/components/`, `ui/src/features/strategy-exploration/hooks/useStrategyDataCatalog.ts`, and `ui/src/features/strategy-exploration/lib/strategyDataCatalog.ts`; moved the real domain-layer comparison implementation to `ui/src/features/system-status/domain-layer-comparison/DomainLayerComparisonPanel.tsx` and converted `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` into a thin re-export wrapper; added `ui/src/features/symbol-purge/lib/symbolPurge.test.ts` and `ui/src/features/strategy-exploration/lib/strategyDataCatalog.test.ts`; current thin entrypoint lengths are `SymbolPurgeByCriteriaPage.tsx` -> `18`, `StrategyDataCatalogPage.tsx` -> `69`, and wrapper `DomainLayerComparisonPanel.tsx` -> `3`; `npm exec vitest -- run src/app/__tests__/SymbolPurgeByCriteriaPage.test.tsx` -> `11 passed`; `npm exec vitest -- run src/app/__tests__/StrategyDataCatalogPage.test.tsx` -> `2 passed`; `npm exec vitest -- run src/app/__tests__/DomainLayerComparisonPanel.test.tsx` -> `20 passed`; `npm exec vitest -- run src/app/__tests__/SystemStatusPage.test.tsx` -> `8 passed`; `npm exec vitest -- run src/app/__tests__/StrategyDataCatalogPage.test.tsx src/app/__tests__/SymbolPurgeByCriteriaPage.test.tsx src/app/__tests__/DomainLayerComparisonPanel.test.tsx src/app/__tests__/SystemStatusPage.test.tsx src/features/symbol-purge/lib/symbolPurge.test.ts src/features/strategy-exploration/lib/strategyDataCatalog.test.ts` -> `46 passed`; `npm exec vite -- build` completed successfully
- **Next Action:** None for this work item; a later cleanup wave can further decompose the feature-owned domain-layer comparison implementation if the panel remains a maintenance hotspot
- **Blockers:** None
- **Rework Loop Count:** 0

## Action Log

### 2026-04-04T20:25:00-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-010
- **Action Taken:** Split the largest UI feature pages into feature-local `components`, `hooks`, and `lib` seams, moved the domain-layer comparison panel ownership into `ui/src/features/system-status/domain-layer-comparison/`, retained the legacy wrapper imports, added feature-local helper tests, and reran the targeted UI suites plus production build validation.
- **Evidence Produced:** `npm exec vitest -- run src/app/__tests__/SymbolPurgeByCriteriaPage.test.tsx` -> `11 passed`; `npm exec vitest -- run src/app/__tests__/StrategyDataCatalogPage.test.tsx` -> `2 passed`; `npm exec vitest -- run src/app/__tests__/DomainLayerComparisonPanel.test.tsx` -> `20 passed`; `npm exec vitest -- run src/app/__tests__/SystemStatusPage.test.tsx` -> `8 passed`; `npm exec vitest -- run src/app/__tests__/StrategyDataCatalogPage.test.tsx src/app/__tests__/SymbolPurgeByCriteriaPage.test.tsx src/app/__tests__/DomainLayerComparisonPanel.test.tsx src/app/__tests__/SystemStatusPage.test.tsx src/features/symbol-purge/lib/symbolPurge.test.ts src/features/strategy-exploration/lib/strategyDataCatalog.test.ts` -> `46 passed`; `npm exec vite -- build` completed successfully.
- **State Transition:** `WI-RSR-010` moved from `Planned` to `Done`
- **Follow-On Assignment / Next Step:** Treat the new feature-local ownership seams as the baseline for future UI work; only remove wrapper imports in a later intentional cleanup wave

### 2026-04-04T18:40:00-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent, Delivery Orchestrator Agent, QA Release Gate Agent, and Architecture / Security / Maintainability review lanes
- **Work Item ID:** WI-RSR-009
- **Action Taken:** Collected explicit signoff on the compatibility-first cutover plan, moved the route-owned models plus the status/domain-metadata/domain-columns/container-apps/jobs helper ownership into `system_modules/*`, rewired `api/endpoints/system.py` into a thinner facade plus re-export bridge, added the facade guard test, and updated the extraction manifest.
- **Evidence Produced:** Active signoff board approved-with-conditions for architecture, maintainability, testing, security/fail-fast, cleanup, and workflow/governance; `python -m py_compile api/endpoints/system.py api/endpoints/system_modules/status_read.py api/endpoints/system_modules/domain_metadata.py api/endpoints/system_modules/domain_columns.py api/endpoints/system_modules/runtime_ops.py api/endpoints/system_modules/container_apps.py api/endpoints/system_modules/jobs.py api/endpoints/system_modules/purge.py` completed successfully; `python -m pytest tests/architecture/test_system_facade_guard.py -q` -> `2 passed`; `python -m pytest tests/monitoring/test_system_health.py -q` -> `22 passed`; `python -m pytest tests/api/test_system_domain_metadata_cache.py tests/api/test_system_domain_columns_cache.py tests/api/test_runtime_config_endpoints.py tests/api/test_debug_symbols_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_job_logs_endpoints.py tests/api/test_system_purge_audit_rule_helpers.py tests/api/test_system_purge_candidates_operations.py tests/api/test_system_purge_parallelism.py tests/api/test_system_purge_symbol_cleanup.py -q` -> `69 passed`; `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `python -m pytest tests/api -q` -> `213 passed`; `python -m pytest -q` -> `915 passed, 3 skipped`
- **State Transition:** `WI-RSR-009` moved from `Planned` to `Done`
- **Follow-On Assignment / Next Step:** Optional later cleanup can relocate the remaining purge execution helpers once the direct facade monkeypatch surface is intentionally reduced

### 2026-04-02T12:46:55.6247209-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-001, WI-RSR-002, WI-RSR-003, WI-RSR-004, WI-RSR-005, WI-RSR-006
- **Action Taken:** Inspected current repo state against the runtime-surface refactor plan, ADR, shim modules, boundary test, and finance pilot extraction files.
- **Evidence Produced:** Confirmed ADR exists locally; confirmed seven untracked `core/*` compatibility shims; confirmed untracked architecture test; identified five remaining phase-1 import offenders; confirmed `silver_parsing.py` and `silver_frames.py` exist but are not wired through; recorded module baseline sizes for later phases.
- **State Transition:** `WI-RSR-001` remained `In Progress`; `WI-RSR-002` remained `Planned`; `WI-RSR-003` through `WI-RSR-006` remained `Planned` or `Deferred`
- **Follow-On Assignment / Next Step:** Create the persistent refactor ledger, then execute WI-RSR-001 boundary rewires

### 2026-04-02T12:46:55.6247209-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-001, WI-RSR-002, WI-RSR-003, WI-RSR-004, WI-RSR-005, WI-RSR-006
- **Action Taken:** Created `docs/architecture/runtime-surface-refactor-ledger.md` as the primary repo-tracked refactor ledger.
- **Evidence Produced:** This file now records work items, assignments, state, dependencies, acceptance criteria, blockers, and action history.
- **State Transition:** No work item state change; ledger system initialized
- **Follow-On Assignment / Next Step:** Rewire remaining phase-1 import offenders and record the result here immediately after the code change

### 2026-04-02T12:48:12.5000734-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-001
- **Action Taken:** Rewired the remaining phase-1 consumers away from direct `tasks.common.*` imports to `core/*` interfaces and switched `core/gold_column_lookup_catalog.py` to `core.gold_sync_contracts`.
- **Evidence Produced:** Edited `api/data_service.py`, `api/endpoints/data.py`, `api/endpoints/system.py`, `monitoring/domain_metadata.py`, and `core/gold_column_lookup_catalog.py`.
- **State Transition:** `WI-RSR-001` remained `In Progress` pending validation
- **Follow-On Assignment / Next Step:** Run the architecture boundary test and targeted validation for rewired consumers

### 2026-04-02T12:48:50.6109497-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-001
- **Action Taken:** Ran the architecture boundary test and the targeted API, monitoring, and gold-lookup suites that cover the rewired imports.
- **Evidence Produced:** `tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `tests/api/test_data_service_adls_preview.py tests/api/test_system_domain_metadata_cache.py tests/monitoring/test_domain_metadata.py tests/tools/test_gold_column_lookup_seed.py -q` -> `46 passed`
- **State Transition:** `WI-RSR-001` moved from `In Progress` to `Needs QA`
- **Follow-On Assignment / Next Step:** Keep WI-RSR-001 open until end-of-milestone full Python validation completes

### 2026-04-02T12:49:28.3583367-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-002
- **Action Taken:** Reworked `tasks/finance_data/silver_finance_data.py` to import parsing and frame helpers from `silver_parsing.py` and `silver_frames.py` instead of keeping local duplicate implementations.
- **Evidence Produced:** Deleted the duplicated parsing/frame helper block from `silver_finance_data.py` and replaced it with extracted-module imports while preserving the helper names exported from the entrypoint module.
- **State Transition:** `WI-RSR-002` moved from `Planned` to `In Progress`
- **Follow-On Assignment / Next Step:** Run the silver finance suite and record any residual wiring gaps

### 2026-04-02T12:53:30.6114228-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-002
- **Action Taken:** Ran `tests/finance_data/test_silver_finance_data.py -q` against the finance pilot refactor.
- **Evidence Produced:** `16 passed, 6 failed`; all failures were `NameError: _prepare_finance_delta_write_frame is not defined` in `_write_alpha26_finance_silver_buckets`
- **State Transition:** `WI-RSR-002` remained `In Progress`; rework loop count incremented to `1`
- **Follow-On Assignment / Next Step:** Import `_prepare_finance_delta_write_frame` from `tasks.finance_data.silver_frames` and rerun the finance suite

### 2026-04-02T12:54:34.5734423-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-002
- **Action Taken:** Imported `_prepare_finance_delta_write_frame` from `tasks.finance_data.silver_frames` and reran the silver finance suite.
- **Evidence Produced:** `tests/finance_data/test_silver_finance_data.py -q` -> `22 passed`
- **State Transition:** `WI-RSR-002` moved from `In Progress` to `Needs QA`
- **Follow-On Assignment / Next Step:** Run end-of-milestone full Python validation before marking WI-RSR-002 done

### 2026-04-02T12:55:23.7522519-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-001, WI-RSR-002
- **Action Taken:** Ran end-of-milestone full Python validation with the architecture boundary test included.
- **Evidence Produced:** `python -m pytest` -> `898 passed, 3 skipped`
- **State Transition:** `WI-RSR-001` moved from `Needs QA` to `Done`; `WI-RSR-002` moved from `Needs QA` to `Done`
- **Follow-On Assignment / Next Step:** Leave completed work items in place for handoff; next structural work begins with WI-RSR-003 and WI-RSR-004

### 2026-04-02T13:03:09.9670070-04:00
- **Acting Agent:** Codex / Delivery Orchestrator Agent
- **Work Item ID:** WI-RSR-003, WI-RSR-004, WI-RSR-005, WI-RSR-006
- **Action Taken:** Wrote the remaining implementation plan as a repo-tracked handoff document and aligned the queued work items to that plan.
- **Evidence Produced:** Added `docs/architecture/runtime-surface-refactor-remaining-plan.md` with target module layouts, extraction order, compatibility rules, validation commands, and per-work-item definitions of done.
- **State Transition:** `WI-RSR-003` moved from `Planned` to `Scoped`; `WI-RSR-004` moved from `Planned` to `Scoped`; `WI-RSR-005` and `WI-RSR-006` remained `Deferred`
- **Follow-On Assignment / Next Step:** Start WI-RSR-003 by extracting `status_read.py` into `api/endpoints/system_modules/` while keeping `api.endpoints.system` as facade plus re-export layer

### 2026-04-02T13:19:24.0608091-04:00
- **Acting Agent:** Codex / Architecture Review Agent
- **Work Item ID:** WI-RSR-003, WI-RSR-004, WI-RSR-005, WI-RSR-006
- **Action Taken:** Rewrote the remaining-plan document into a more explicit implementation plan using current route, helper, test, and module-boundary evidence.
- **Evidence Produced:** `docs/architecture/runtime-surface-refactor-remaining-plan.md` now defines concrete target module structures, extraction order, compatibility rules, validation matrices, and definitions of done for the remaining work items.
- **State Transition:** No work item state change; `WI-RSR-003` and `WI-RSR-004` remain `Scoped`, `WI-RSR-005` and `WI-RSR-006` remain `Deferred`
- **Follow-On Assignment / Next Step:** Execute `WI-RSR-003` starting with `api/endpoints/system_modules/__init__.py` and `status_read.py`

### 2026-04-02T14:05:47.4091292-04:00
- **Acting Agent:** Codex / Delivery Orchestrator Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Loaded the delivery-orchestrator skill, reviewed the orchestration contract, and mapped the first `system.py` extraction slice.
- **Evidence Produced:** Identified the initial status and lineage cluster (`/health`, `/symbol-sync-state`, `/status-view`, `/lineage`), traced its helper dependencies, and confirmed that system status-view tests patch `api.endpoints.system` symbols directly.
- **State Transition:** `WI-RSR-003` moved from `Scoped` to `In Progress`
- **Follow-On Assignment / Next Step:** Implement `api/endpoints/system_modules/status_read.py` with runtime-bound dependency lookup so extracted routes still honor monkeypatches against `api.endpoints.system`

### 2026-04-02T14:13:01.8432623-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Implemented the first `system.py` extraction slice by moving the read-only status and lineage route assembly into `api/endpoints/system_modules/status_read.py` while keeping the facade module export surface intact.
- **Evidence Produced:** Added `api/endpoints/system_modules/__init__.py` and `api/endpoints/system_modules/status_read.py`; updated `api/endpoints/system.py` to include the extracted router and re-export `system_health`, `get_symbol_sync_state_endpoint`, `system_status_view`, and `system_lineage`; `python -m pytest tests/api/test_lifespan_workers.py tests/api/test_system_domain_metadata_cache.py -q` -> `22 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Run the broader `tests/api/test_system*.py -q` suite to verify there are no indirect system-endpoint regressions from the first router extraction

### 2026-04-02T14:14:15.1105065-04:00
- **Acting Agent:** Codex / QA Release Gate Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Ran the full `tests/api/test_system*.py` surface after expanding the test file list explicitly for PowerShell.
- **Evidence Produced:** `Get-ChildItem tests/api -Filter 'test_system*.py' | ... ; python -m pytest @files -q` -> `63 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Continue WI-RSR-003 with the `domain_metadata.py` extraction slice now that the router-split pattern has held under the broader system test surface

### 2026-04-02T14:17:27.7356743-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Extracted the domain-metadata route cluster into `api/endpoints/system_modules/domain_metadata.py` and rewired `api/endpoints/system.py` to include the new router while preserving facade exports.
- **Evidence Produced:** Added `api/endpoints/system_modules/domain_metadata.py`; re-exported `domain_metadata`, `domain_metadata_snapshot`, `get_domain_metadata_snapshot_cache`, and `put_domain_metadata_snapshot_cache` from `api/endpoints/system.py`; `python -m pytest tests/api/test_system_domain_metadata_cache.py -q` -> `18 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Continue WI-RSR-003 with the `domain_columns.py` extraction slice using the same facade-preserving router pattern

### 2026-04-02T14:25:59.7031589-04:00
- **Acting Agent:** Codex / Delivery Orchestrator Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Mapped the `domain-columns` extraction slice in `api/endpoints/system.py` and confirmed the test-facing compatibility constraints before editing code.
- **Evidence Produced:** Confirmed the route handlers only need router extraction, not helper relocation; verified `tests/api/test_system_domain_columns_cache.py` monkeypatches `_read_domain_columns_from_artifact` on `api.endpoints.system` and directly exercises `_read_cached_domain_columns`, `_write_cached_domain_columns`, `_retrieve_domain_columns`, and `_run_with_timeout`.
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Implement `api/endpoints/system_modules/domain_columns.py` and preserve `get_domain_columns` plus `refresh_domain_columns` exports on the facade module

### 2026-04-02T14:27:57.7117826-04:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Extracted the domain-columns route cluster into `api/endpoints/system_modules/domain_columns.py` and rewired `api/endpoints/system.py` to include the new router while preserving facade exports.
- **Evidence Produced:** Added `api/endpoints/system_modules/domain_columns.py`; re-exported `get_domain_columns` and `refresh_domain_columns` from `api/endpoints/system.py`; `python -m pytest tests/api/test_system_domain_columns_cache.py -q` -> `6 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Continue WI-RSR-003 with the purge/domain-list extraction slice using the same facade-preserving router pattern

### 2026-04-02T14:31:48.2459626-04:00
- **Acting Agent:** Codex / QA Release Gate Agent and Delivery Engineer Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Ran full Python validation, found an OpenAPI generation regression in the extracted `domain_columns.py` route module, removed the postponed annotation causing the unresolved request-model forward reference, and reran validation.
- **Evidence Produced:** Initial `python -m pytest` failed in `tests/api/test_swagger_docs.py` with a `PydanticUserError` for `domain_columns_refresh_request_model`; removed `from __future__ import annotations` from `api/endpoints/system_modules/domain_columns.py`; `python -m pytest tests/api/test_swagger_docs.py -q` -> `2 passed`; `python -m pytest` -> `898 passed, 3 skipped`
- **State Transition:** `WI-RSR-003` remained `In Progress`; rework loop count incremented from `0` to `1`
- **Follow-On Assignment / Next Step:** Continue WI-RSR-003 with the purge/domain-list extraction slice now that the extracted router pattern has a green full-suite baseline

### 2026-04-04T17:25:00-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-007
- **Action Taken:** Transferred shared foundation ownership into `core/*`, added `core/run_manifests.py`, converted legacy `tasks.common.*` modules into compatibility wrappers, rewired task consumers to `core/*`, and updated boundary coverage plus ADR/ledger documentation.
- **Evidence Produced:** `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `python -m pytest tests/core/test_finance_contracts.py tests/core/test_market_symbols.py tests/core/test_run_manifests.py tests/core/test_bronze_bucketing.py tests/core/test_domain_artifacts.py tests/core/test_gold_sync_contracts.py -q` -> `37 passed`; wrapper smoke validation -> `8 passed`; targeted consumer suites -> `125 passed`; full `python -m pytest` -> `909 passed, 3 skipped`
- **State Transition:** `WI-RSR-007` moved from `Planned` to `Done`
- **Follow-On Assignment / Next Step:** Keep the legacy wrappers until a later cleanup wave removes the remaining task-side compatibility imports entirely

### 2026-04-04T09:46:50.0375824-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-003, WI-RSR-004, WI-RSR-005, WI-RSR-006
- **Action Taken:** Refreshed the remaining-plan document to the current implementation baseline before resuming code changes.
- **Evidence Produced:** Updated `docs/architecture/runtime-surface-refactor-remaining-plan.md` to record the current `WI-RSR-003` extraction state, today's green Python and Vitest baselines, and the explicit no-`shared.py` rule for the remaining `system.py` work.
- **State Transition:** No work item state change; `WI-RSR-003` remains `In Progress`, `WI-RSR-004` remains `Scoped`, and `WI-RSR-005` / `WI-RSR-006` remain `Deferred`
- **Follow-On Assignment / Next Step:** Implement `api/endpoints/system_modules/purge.py` and rewire `api/endpoints/system.py` to include it while preserving the legacy facade exports

### 2026-04-04T09:52:36.2647604-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Extracted the purge-related route cluster into `api/endpoints/system_modules/purge.py`, rewired the facade exports in `api/endpoints/system.py`, and validated the slice with purge-specific plus expanded system-endpoint tests.
- **Evidence Produced:** Added `api/endpoints/system_modules/purge.py`; updated `api/endpoints/system.py` to include `_purge_router` and preserve `list_purge_rule_operators`, `list_purge_rules_endpoint`, `create_purge_rule_endpoint`, `update_purge_rule_endpoint`, `delete_purge_rule_endpoint`, `preview_purge_rule`, `run_purge_rule_now`, `purge_data`, `get_domain_lists`, `reset_domain_lists`, `reset_domain_checkpoints`, `get_purge_candidates`, `create_purge_candidates_operation`, `get_blacklist_symbols_for_purge`, `purge_symbols`, `purge_symbol`, and `get_purge_operation`; `python -m pytest tests/api/test_system_purge_audit_rule_helpers.py tests/api/test_system_purge_candidates_operations.py tests/api/test_system_purge_parallelism.py tests/api/test_system_purge_symbol_cleanup.py -q` -> `31 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Implement `api/endpoints/system_modules/runtime_ops.py` and rewire `api/endpoints/system.py` to preserve the runtime-config and debug-symbol exports on the facade

### 2026-04-04T09:55:31.2122245-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Extracted the runtime-config and debug-symbol route cluster into `api/endpoints/system_modules/runtime_ops.py`, rewired the facade exports in `api/endpoints/system.py`, and validated the slice with targeted, OpenAPI, and expanded system-endpoint tests.
- **Evidence Produced:** Added `api/endpoints/system_modules/runtime_ops.py`; updated `api/endpoints/system.py` to include `_runtime_ops_router` and preserve `get_runtime_config_catalog`, `get_runtime_config`, `set_runtime_config`, `remove_runtime_config`, `get_debug_symbols`, `set_debug_symbols`, and `remove_debug_symbols`; `python -m pytest tests/api/test_runtime_config_endpoints.py tests/api/test_debug_symbols_endpoints.py -q` -> `12 passed`; `python -m pytest tests/api/test_swagger_docs.py -q` -> `2 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Implement `api/endpoints/system_modules/container_apps.py` and rewire `api/endpoints/system.py` to preserve the container-app route exports on the facade

### 2026-04-04T09:58:01.0172715-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-003
- **Action Taken:** Extracted the container-app route cluster into `api/endpoints/system_modules/container_apps.py`, rewired the facade exports in `api/endpoints/system.py`, and validated the slice with targeted plus expanded system-endpoint tests.
- **Evidence Produced:** Added `api/endpoints/system_modules/container_apps.py`; updated `api/endpoints/system.py` to include `_container_apps_router` and preserve `list_container_apps`, `get_container_app_logs`, `start_container_app`, and `stop_container_app`; `python -m pytest tests/api/test_system_container_apps_endpoints.py -q` -> `6 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`
- **State Transition:** `WI-RSR-003` remained `In Progress`
- **Follow-On Assignment / Next Step:** Implement `api/endpoints/system_modules/jobs.py` and rewire `api/endpoints/system.py` to preserve the job-control and job-log exports on the facade

### 2026-04-04T10:02:26.5582175-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-003, WI-RSR-004
- **Action Taken:** Extracted the final `jobs.py` route cluster, rewired the facade exports in `api/endpoints/system.py`, ran targeted job-log, expanded system-endpoint, and full Python validation, then advanced the plan to `WI-RSR-004`.
- **Evidence Produced:** Added `api/endpoints/system_modules/jobs.py`; updated `api/endpoints/system.py` to include `_jobs_router` and preserve `trigger_job_run`, `suspend_job`, `stop_job`, `resume_job`, and `get_job_logs`; `python -m pytest tests/api/test_system_job_logs_endpoints.py -q` -> `2 passed`; expanded `tests/api/test_system*.py` run -> `63 passed`; `python -m pytest` -> `898 passed, 3 skipped`
- **State Transition:** `WI-RSR-003` moved from `In Progress` to `Done`; `WI-RSR-004` moved from `Scoped` to `In Progress`
- **Follow-On Assignment / Next Step:** Implement `monitoring/system_health_modules/env_config.py` and keep `monitoring/system_health.py` as the facade and compatibility surface

### 2026-04-04T10:09:24.7924534-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-004
- **Action Taken:** Extracted the first three `system_health` helper groups into `env_config.py`, `signals.py`, and `job_queries.py`, rewired `monitoring/system_health.py` to re-export them, and validated both the targeted monitoring surface and the full Python suite.
- **Evidence Produced:** Added `monitoring/system_health_modules/env_config.py`, `monitoring/system_health_modules/signals.py`, and `monitoring/system_health_modules/job_queries.py`; updated `monitoring/system_health.py` imports to preserve `FreshnessPolicy`, `MarkerProbeConfig`, `JobScheduleMetadata`, `BronzeSymbolJumpThreshold`, `_require_env`, `_env_or_default`, `_require_int`, `_resolve_freshness_policy` dependencies, signal helpers, and job-query helpers on the facade; `python -m pytest tests/monitoring/test_system_health_staleness.py -q` -> `9 passed`; `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q` -> `25 passed`; `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_phase3b_signals.py tests/monitoring/test_system_health_staleness.py tests/tasks/test_blob_freshness.py -q` -> `34 passed`; `python -m pytest` -> `898 passed, 3 skipped`
- **State Transition:** `WI-RSR-004` remained `In Progress`
- **Follow-On Assignment / Next Step:** Implement `monitoring/system_health_modules/freshness.py` and continue the facade-preserving split with the staleness, schedule, and layer-spec helpers

### 2026-04-04T14:06:22.5228546-05:00
- **Acting Agent:** Codex / Delivery Orchestrator Agent
- **Work Item ID:** WI-RSR-004, WI-RSR-005, WI-RSR-006
- **Action Taken:** Reconciled the remaining-plan doc against the current ledger so the active execution queue starts at `WI-RSR-004` instead of the already-complete `WI-RSR-003`.
- **Evidence Produced:** Updated `docs/architecture/runtime-surface-refactor-remaining-plan.md` execution order, marked `WI-RSR-003` as completed-history, and replaced the stale immediate-next-step block with the `freshness.py` extraction and validation commands.
- **State Transition:** No work item state change; `WI-RSR-004` remained `In Progress`, `WI-RSR-005` remained `Deferred`, and `WI-RSR-006` remained `Deferred`
- **Follow-On Assignment / Next Step:** Extract `monitoring/system_health_modules/freshness.py` and re-export the moved helpers from `monitoring.system_health`

### 2026-04-04T14:17:50.4731279-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent
- **Work Item ID:** WI-RSR-004
- **Action Taken:** Completed the structural `system_health` split by adding `freshness.py`, `alerts.py`, and `snapshot.py`, then collapsing `monitoring.system_health` into the compatibility facade and patch surface.
- **Evidence Produced:** Added the three new `monitoring/system_health_modules/*` files, rewrote `monitoring/system_health.py` to re-export the moved helpers and orchestration entrypoint, and ran `python -m py_compile monitoring/system_health.py monitoring/system_health_modules/freshness.py monitoring/system_health_modules/alerts.py monitoring/system_health_modules/snapshot.py` successfully.
- **State Transition:** `WI-RSR-004` remained `In Progress` pending test validation
- **Follow-On Assignment / Next Step:** Run `tests/monitoring/test_system_health_staleness.py`, `tests/tasks/test_blob_freshness.py`, the combined monitoring cluster, and then full `python -m pytest`

### 2026-04-04T14:22:17.6203654-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-004
- **Action Taken:** Ran the `WI-RSR-004` validation sequence, found a cross-surface facade-export regression in `api.endpoints.system`, restored the missing `validate_auth` export path, and reran the combined monitoring cluster successfully.
- **Evidence Produced:** `python -m pytest tests/monitoring/test_system_health_staleness.py -q` -> `9 passed`; `python -m pytest tests/tasks/test_blob_freshness.py -q` -> `1 passed`; initial `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_phase3b_signals.py tests/monitoring/test_system_health_staleness.py tests/tasks/test_blob_freshness.py -q` failed because `api.endpoints.system_modules.status_read` could not resolve `validate_auth` from the facade runtime; added `validate_auth` back to the `api.service.dependencies` import list in `api/endpoints/system.py`; reran the same combined monitoring command -> `34 passed`
- **State Transition:** `WI-RSR-004` remained `In Progress`; rework loop count incremented from `0` to `1`
- **Follow-On Assignment / Next Step:** Run full `python -m pytest` and, if green, close `WI-RSR-004` before starting `WI-RSR-005`

### 2026-04-04T14:27:37.7188681-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-004
- **Action Taken:** Ran the full Python closeout gate, found additional `api.endpoints.system` facade drift affecting extracted runtime-config, debug-symbols, domain-metadata, container-app-log, and job-log routes, restored those facade patch points, reran the failing API slices, and closed the work item with a green full suite.
- **Evidence Produced:** Initial `python -m pytest` failed because `api.endpoints.system` no longer exposed `DEFAULT_ENV_OVERRIDE_KEYS`, `list_runtime_config`, `upsert_runtime_config`, `delete_runtime_config`, `normalize_env_override`, `read_debug_symbols_state`, `replace_debug_symbols_state`, `delete_debug_symbols_state`, `build_snapshot_miss_payload`, `AzureLogAnalyticsClient`, and later `timedelta`; restored those imports in `api/endpoints/system.py`; `python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q` -> `38 passed`; final `python -m pytest` -> `898 passed, 3 skipped`
- **State Transition:** `WI-RSR-004` moved from `In Progress` to `Done`; rework loop count incremented from `1` to `2`
- **Follow-On Assignment / Next Step:** Start `WI-RSR-005` by reading the current `ui/src/app/App.tsx`, page entry modules, and UI tests, then add `ui/src/app/routes.tsx` while keeping `App.tsx` as the shell

### 2026-04-04T14:36:13.7167665-05:00
- **Acting Agent:** Codex / Delivery Engineer Agent and QA Release Gate Agent
- **Work Item ID:** WI-RSR-005
- **Action Taken:** Reorganized the routed UI surface into `ui/src/features/*`, split route composition into `ui/src/app/routes.tsx`, preserved old app-page imports as compatibility wrappers, and validated the full UI suite from the package root.
- **Evidence Produced:** Moved routed page entry files from `ui/src/app/components/pages/` into `ui/src/features/`; added `ui/src/app/routes.tsx`; updated `ui/src/app/App.tsx` to keep the auth/providers shell and render `AppRoutes`; added thin re-export wrappers at the legacy `ui/src/app/components/pages/*.tsx` paths; updated `ui/src/app/__tests__/App.test.tsx` and `ui/src/app/__tests__/App.auth.test.tsx` to mock the new feature entrypoints; first `pnpm exec vitest run` from repo root failed with `ERR_PNPM_RECURSIVE_EXEC_NO_PACKAGE`; reran from `ui/` and `pnpm exec vitest run` -> `34 files passed, 166 tests passed`
- **State Transition:** `WI-RSR-005` moved from `Deferred` to `Needs QA`
- **Follow-On Assignment / Next Step:** Start `WI-RSR-006` by writing the extraction-readiness docs against the new feature layout, then rerun `python -m pytest` plus `pnpm exec vitest run` for final closeout

### 2026-04-04T14:44:10.1784531-05:00
- **Acting Agent:** Codex / Delivery Orchestrator Agent, Delivery Engineer Agent, and QA Release Gate Agent
- **Work Item ID:** WI-RSR-005, WI-RSR-006
- **Action Taken:** Added the extraction-readiness docs, verified every documented command against the current tree, and closed the final two work items.
- **Evidence Produced:** Added `docs/architecture/runtime-surface-test-targets.md`, `docs/architecture/runtime-surface-extraction-manifest.md`, and `docs/architecture/runtime-surface-ci-matrix.md`; `python -m pytest tests/architecture/test_python_module_boundaries.py -q` -> `3 passed`; `python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q` -> `38 passed`; `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q` -> `34 passed`; `python -m pytest tests/finance_data/test_silver_finance_data.py -q` -> `22 passed`; `python -m pytest` -> `898 passed, 3 skipped`; `pnpm exec vitest run` from `ui/` -> `34 files passed, 166 tests passed`
- **State Transition:** `WI-RSR-005` moved from `Needs QA` to `Done`; `WI-RSR-006` moved from `Deferred` to `Done` in a single docs-only batch because the work item scope was documentation plus immediate command verification
- **Follow-On Assignment / Next Step:** None; the runtime-surface refactor plan is complete and the ledger plus extraction docs are the handoff baseline
