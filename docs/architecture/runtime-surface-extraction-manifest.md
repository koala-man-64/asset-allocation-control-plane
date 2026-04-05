# Runtime Surface Extraction Manifest

This manifest records the current extraction-ready surface boundaries after the runtime-surface refactor.

## Surface Inventory

### API System Surface

- Public facade: `api/endpoints/system.py`
- Extracted modules:
  - `api/endpoints/system_modules/status_read.py`
  - `api/endpoints/system_modules/domain_metadata.py`
  - `api/endpoints/system_modules/domain_columns.py`
  - `api/endpoints/system_modules/purge.py`
  - `api/endpoints/system_modules/runtime_ops.py`
  - `api/endpoints/system_modules/container_apps.py`
  - `api/endpoints/system_modules/jobs.py`
- Compatibility rule:
  - keep `api.endpoints.system` as the route import, monkeypatch, and helper re-export surface until downstream tests and callers no longer depend on it directly
  - route-owned request/response models and the non-purge helper clusters now live in `system_modules/*`; the facade keeps shared glue plus the remaining purge execution helpers that still serve as the direct patch surface

### Monitoring Health Surface

- Public facade: `monitoring/system_health.py`
- Extracted modules:
  - `monitoring/system_health_modules/env_config.py`
  - `monitoring/system_health_modules/signals.py`
  - `monitoring/system_health_modules/job_queries.py`
  - `monitoring/system_health_modules/freshness.py`
  - `monitoring/system_health_modules/alerts.py`
  - `monitoring/system_health_modules/snapshot.py`
- Compatibility rule:
  - keep `monitoring.system_health` as the import and patch surface for snapshot orchestration, helper access, constants, and Azure client seams

### Shared Runtime Contracts

- Public shared surface:
  - `core/bronze_bucketing.py`
  - `core/layer_bucketing.py`
  - `core/domain_artifacts.py`
  - `core/domain_metadata_snapshots.py`
  - `core/finance_contracts.py`
  - `core/market_symbols.py`
  - `core/gold_sync_contracts.py`
- Compatibility rule:
  - `api/`, `monitoring/`, and non-shim `core/` consumers import shared contracts through `core/*`, not `tasks.common.*`

### Finance ETL Surface

- Public entrypoints:
  - `tasks/finance_data/bronze_finance_data.py`
  - `tasks/finance_data/silver_finance_data.py`
  - `tasks/finance_data/gold_finance_data.py`
- Extracted modules:
  - `tasks/finance_data/silver_modules/parsing.py`
  - `tasks/finance_data/silver_modules/frames.py`
  - `tasks/finance_data/silver_modules/discovery.py`
  - `tasks/finance_data/silver_modules/indexing.py`
  - `tasks/finance_data/silver_modules/writes.py`
  - `tasks/finance_data/silver_modules/reconciliation.py`
  - `tasks/finance_data/silver_modules/runner.py`
  - `tasks/finance_data/bronze_modules/coverage.py`
  - `tasks/finance_data/bronze_modules/provider.py`
  - `tasks/finance_data/bronze_modules/invalid_symbols.py`
  - `tasks/finance_data/bronze_modules/assembly.py`
  - `tasks/finance_data/bronze_modules/publication.py`
  - `tasks/finance_data/bronze_modules/runner.py`
  - `tasks/finance_data/gold_modules/features.py`
  - `tasks/finance_data/gold_modules/schema.py`
  - `tasks/finance_data/gold_modules/watermarks.py`
  - `tasks/finance_data/gold_modules/sync.py`
  - `tasks/finance_data/gold_modules/reconciliation.py`
  - `tasks/finance_data/gold_modules/runner.py`
- Compatibility rule:
  - keep `bronze_finance_data.py`, `silver_finance_data.py`, and `gold_finance_data.py` as the public entrypoint and monkeypatch surface while the `*_modules/` packages provide the decomposed helper namespace during the transition

### UI Application Surface

- Public shell:
  - `ui/src/app/App.tsx`
  - `ui/src/app/routes.tsx`
- Extracted feature entrypoints:
  - `ui/src/features/data-explorer/DataExplorerPage.tsx`
  - `ui/src/features/regimes/RegimeMonitorPage.tsx`
  - `ui/src/features/system-status/SystemStatusPage.tsx`
  - `ui/src/features/data-quality/DataQualityPage.tsx`
  - `ui/src/features/data-profiling/DataProfilingPage.tsx`
  - `ui/src/features/debug-symbols/DebugSymbolsPage.tsx`
  - `ui/src/features/runtime-config/RuntimeConfigPage.tsx`
  - `ui/src/features/symbol-purge/SymbolPurgeByCriteriaPage.tsx`
  - `ui/src/features/stocks/StockExplorerPage.tsx`
  - `ui/src/features/stocks/StockDetailPage.tsx`
  - `ui/src/features/postgres-explorer/PostgresExplorerPage.tsx`
  - `ui/src/features/strategies/StrategyConfigPage.tsx`
  - `ui/src/features/universes/UniverseConfigPage.tsx`
  - `ui/src/features/rankings/RankingConfigPage.tsx`
  - `ui/src/features/strategy-exploration/StrategyDataCatalogPage.tsx`
- Feature-local internal ownership seams:
  - `ui/src/features/symbol-purge/components/*`
  - `ui/src/features/symbol-purge/hooks/useSymbolPurgeController.ts`
  - `ui/src/features/symbol-purge/lib/symbolPurge.ts`
  - `ui/src/features/strategy-exploration/components/*`
  - `ui/src/features/strategy-exploration/hooks/useStrategyDataCatalog.ts`
  - `ui/src/features/strategy-exploration/lib/strategyDataCatalog.ts`
  - `ui/src/features/system-status/domain-layer-comparison/DomainLayerComparisonPanel.tsx`
- Compatibility wrappers:
  - `ui/src/app/components/pages/*.tsx` for the routed page entry files
- Compatibility rule:
  - keep `App.tsx` as the providers/auth/layout shell
  - keep legacy `ui/src/app/components/pages/*` entry imports available until downstream tests and imports are fully migrated

## Extraction Readiness Notes

- Backend extraction is now facade-first, not package-split. The stable extraction boundaries are the facade modules, not the internal helper files.
- UI extraction is route-entry-first. Feature folders own routed pages, while shared layout, auth, common components, and low-level UI primitives remain under `ui/src/app/components/*`.
- The remaining extraction work after this document is organizational or packaging-oriented, not structural code splitting within the audited runtime surfaces.
