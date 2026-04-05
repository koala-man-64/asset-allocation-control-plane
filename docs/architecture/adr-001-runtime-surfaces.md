# ADR 001: Runtime Surface Boundaries for Modular Monolith Extraction

## Status
- Accepted

## Context
- The repository currently mixes transport, ETL, monitoring, provider, and shared-contract concerns across `api/`, `core/`, `monitoring/`, and `tasks/`.
- Several high-churn modules are oversized and act as cross-surface coordination points, including:
  - `tasks/finance_data/silver_finance_data.py`
  - `api/endpoints/system.py`
  - `monitoring/system_health.py`
- The target architecture remains a single deployable repository for now, but the internal module boundaries must support later extraction into multiple repositories with minimal rewrite.

## Decision
- Organize the codebase around runtime surfaces:
  - `core/`: shared foundation and shared internal contracts
  - `tasks/`: ETL jobs and job orchestration
  - `api/`: FastAPI transport, auth, and read orchestration
  - `monitoring/`: health/status collection and Azure monitoring logic
  - `ui/`: feature-driven frontend and typed API clients
  - provider adapters remain exposed through stable modules consumed by `api/` and `tasks/`
- Preserve current external contracts during the refactor:
  - API routes
  - UI routes
  - `python -m tasks...` entrypoints
  - env var names
  - deploy manifests
  - Postgres and Delta storage contracts

## Boundary Rules
- `api/` must not import from `tasks.*`.
- `monitoring/` must not import from `tasks.*`.
- `core/` must not import from `tasks.*`.
- `tasks/` may depend on `core/`.
- Shared contracts used by multiple runtime surfaces should be reached through `core/`, not `tasks.common.*`.
- `tasks.common.*` modules that mirror `core/*` are compatibility-only wrappers and are not the source of truth.

## Compatibility Strategy
- Shared implementations and contracts now live in `core/*`.
- Legacy `tasks.common.*` modules may re-export `core/*` while downstream callers migrate.
- Those wrappers are transitional and should be removed once remaining task call sites no longer require the legacy import paths.
- New cross-surface code should target the `core/*` interface first.

## Initial Extraction Priorities
1. Replace direct `api/`, `monitoring/`, and shared `core/` imports from `tasks.common.*` with `core/*` interfaces.
2. Break oversized finance ETL modules into focused parsing, normalization, indexing, and orchestration units.
3. Split `api/endpoints/system.py` and `monitoring/system_health.py` by responsibility.
4. Move the UI toward feature folders while keeping `ui/src/app/App.tsx` as the shell.

## Consequences
- Short term: a thin compatibility layer exists in `tasks.common`.
- Medium term: boundary tests can enforce the new dependency rules in CI without a `core -> tasks` allowlist.
- Long term: `ui`, `api-service`, `etl-jobs`, and shared/provider surfaces can be extracted into separate repositories without changing external contracts first.
