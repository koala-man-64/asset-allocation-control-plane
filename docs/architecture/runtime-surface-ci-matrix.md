# Runtime Surface CI Matrix

This is the recommended validation matrix for the current runtime-surface layout. It documents the checks that should exist logically, even though CI workflow changes are intentionally out of scope for this refactor.

## Matrix

| Check | Working Directory | Command | Covers | Required For |
| --- | --- | --- | --- | --- |
| `python-full` | repo root | `python -m pytest` | full backend/runtime regression | backend milestone closeout and final merge gate |
| `python-architecture` | repo root | `python -m pytest tests/architecture/test_python_module_boundaries.py -q` | runtime dependency boundaries | any change touching `api/`, `monitoring/`, `core/`, or `tests/architecture` |
| `python-system-facade` | repo root | `python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q` | `api.endpoints.system` facade compatibility | changes under `api/endpoints/system.py` or `api/endpoints/system_modules/*` |
| `python-system-health` | repo root | `python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q` | `monitoring.system_health` facade compatibility | changes under `monitoring/system_health.py` or `monitoring/system_health_modules/*` |
| `python-finance-silver` | repo root | `python -m pytest tests/finance_data/test_silver_finance_data.py -q` | silver finance orchestration boundary | changes under `tasks/finance_data/silver_*` |
| `ui-full` | `ui/` | `pnpm exec vitest run` | full routed UI surface and compatibility wrappers | any change under `ui/src/app`, `ui/src/features`, or routed UI tests |

## Execution Order

Recommended order for human or CI execution:

1. surface-specific targeted commands for the files touched
2. `ui-full` when UI changed
3. `python-full` before marking runtime/backend work complete

## Intent

- keep the matrix small enough that every command is actually runnable and maintained
- keep targeted gates aligned to the facade boundaries introduced by the refactor
- avoid documenting speculative future jobs that do not match the current repository layout
