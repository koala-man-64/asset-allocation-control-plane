# Runtime Surface Test Targets

This document lists the canonical, runnable validation commands for the refactored runtime surfaces.

## Conventions

- Run Python commands from the repository root: `C:\Users\rdpro\Projects\asset-allocation-control-plane`
- Use these commands as the handoff-safe validation set for the current refactor baseline

## Canonical Commands

### Full Python Closeout

```powershell
python -m pytest
```

Purpose:
- end-to-end regression gate for `WI-RSR-001` through `WI-RSR-004`
- required before marking backend/runtime work done

### Architecture Boundary Guardrail

```powershell
python -m pytest tests/architecture/test_python_module_boundaries.py tests/architecture/test_system_facade_guard.py tests/architecture/test_monitoring_facade_guard.py tests/test_workflow_runtime_ownership.py tests/test_deploy_manifests.py -q
```

Purpose:
- verifies `api/`, `monitoring/`, and non-shim `core/` modules stay off direct `tasks.*` imports
- keeps `api.endpoints.system` and `monitoring.system_health` as facades instead of helper-ownership modules
- blocks new top-level non-API YAML manifests under `deploy/`

### System Facade Compatibility

```powershell
python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q
```

Purpose:
- validates the `api.endpoints.system` facade after extraction into `api/endpoints/system_modules/*`
- catches missing monkeypatch surfaces and route-module runtime dependencies

### Monitoring Health Surface

```powershell
python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q
```

Purpose:
- validates the `monitoring.system_health` facade after extraction into `monitoring/system_health_modules/*`

### Finance ETL Module Surface

```powershell
python -m pytest tests/finance_data/test_finance_module_packages.py tests/finance_data/test_silver_finance_data.py tests/finance_data/test_bronze_finance_data.py tests/finance_data/test_gold_finance_delta_write.py tests/finance_data/test_feature_generator.py tests/tasks/test_reconciliation_contracts.py tests/tasks/test_job_entrypoint_contracts.py tests/tasks/test_postgres_gold_sync.py -q
```

Purpose:
- validates the finance job compatibility surfaces after establishing `silver_modules/*`, `bronze_modules/*`, and `gold_modules/*`
- confirms the legacy top-level entrypoints and the new module packages resolve to the same helper surfaces where compatibility wrappers are still in place

### UI Closeout

This repository no longer contains a top-level UI workspace. Run UI validation in the sibling `asset-allocation-ui` repository when that repo changes.
