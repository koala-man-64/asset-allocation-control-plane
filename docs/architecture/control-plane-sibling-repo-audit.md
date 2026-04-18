# Architecture & Code Audit Report

Decision: This is local-only and does not require contracts repo routing.

## 1. Executive Summary

The sibling-repo audit does not support moving UI, contracts, or general runtime-common transport functionality into `asset-allocation-control-plane`. Those boundaries are mostly aligned with the current five-repo split. The two real ownership drifts are narrower and sharper: backtest result publication is still implemented as direct SQL from outside the control plane, and the jobs repo still carries mirrored control-plane mutation helpers for runtime config and debug symbols. The control plane should remain the canonical owner of operator-state persistence and backtest storage, while jobs should consume those surfaces through read-only helpers or authenticated internal APIs.

## 2. System Map (High-Level)

- `asset-allocation-control-plane` is the operator backend: FastAPI routes, Postgres-backed operator state, monitoring, internal control endpoints, and backtest read models.
- `asset-allocation-jobs` is the worker runtime: ETL, scheduled jobs, and backtest execution. It should consume control-plane state, not own it.
- `asset-allocation-runtime-common` is intended to hold transport-neutral helpers and runtime clients shared by the Python runtimes.
- `asset-allocation-contracts` owns shared schemas and cross-repo payload contracts.
- `asset-allocation-ui` is a standalone browser client that talks to the control plane over HTTP and websocket surfaces.

Dependency direction and boundary notes:

- UI -> control plane over `/api/*`, `/config.js`, and `/ws/updates`.
- Jobs -> control plane over authenticated internal HTTP endpoints for run lifecycle and config/state reads.
- Runtime-common -> should stay below both runtimes as a client/helper package, not as an owner of control-plane storage semantics.
- Current drift: jobs -> runtime-common -> direct SQL into control-plane-owned `core.*` backtest tables.
- Current drift: jobs still contains byte-identical copies of control-plane runtime-config and debug-symbol mutation helpers.

Data flows:

- Operator requests: UI -> control plane -> Postgres/operator state.
- Backtest submission: control plane validates, records queued run state, and exposes read APIs.
- Backtest execution: jobs claims work from the control plane, runs the simulation, and currently writes result rows directly into control-plane tables before the control plane marks the run complete.
- Runtime config/debug symbols: control plane owns the Postgres rows; jobs currently reads them, but also still carries write-capable mirrored modules.

## 3. Findings (Triaged)

### 3.1 Critical (Must Fix)

None.

### 3.2 Major

- **[Backtest result persistence bypasses the control-plane ownership boundary]**
  - **Evidence:** `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md:8` says `asset-allocation-runtime-common` does not own Postgres repositories and `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md:9` says the control plane owns Postgres/operator state. `asset-allocation-control-plane/docs/architecture/control-plane-architecture.md:30` and `asset-allocation-control-plane/docs/architecture/control-plane-architecture.md:40` place Postgres-backed runtime control state and backtest storage in this repo. But `asset-allocation-jobs/core/backtest_runtime.py:16` imports `persist_backtest_results`, `asset-allocation-jobs/core/backtest_runtime.py:1650` calls it, and `asset-allocation-runtime-common/python/asset_allocation_runtime_common/backtest_results.py:359` plus `asset-allocation-runtime-common/python/asset_allocation_runtime_common/backtest_results.py:446` perform direct `INSERT` and `UPDATE` statements against `core.backtest_*` tables and `core.runs`. `asset-allocation-jobs/README.md:36` says jobs only owns worker runtime, while `asset-allocation-jobs/README.md:41` admits it writes Postgres-backed v4 results.
  - **Why it matters:** This makes the worker runtime and runtime-common package de facto owners of the control plane read model. Schema changes, publication invariants, and rollback safety now require coordinated releases across repos even though the data is documented as control-plane-owned. It also bypasses the intended HTTP trust boundary and makes it harder to enforce auth, validation, audit logging, and versioned ingestion rules in one place.
  - **Recommendation:** Move canonical backtest result publication into `asset-allocation-control-plane`. Preferred implementation: add an authenticated internal ingest surface in the control plane, for example `POST /api/internal/backtests/runs/{run_id}/results`, and keep jobs as a producer of typed payloads only. If payload size is too large for a single request, have jobs upload a versioned artifact and ask the control plane to ingest it. Keep runtime-common limited to transport helpers and request models.
  - **Acceptance Criteria:** No code outside `asset-allocation-control-plane` executes SQL against `core.backtest_run_summary`, `core.backtest_timeseries`, `core.backtest_rolling_metrics`, `core.backtest_trades`, `core.backtest_closed_positions`, `core.backtest_selection_trace`, `core.backtest_regime_trace`, or `core.runs` for results publication. Jobs publishes results through a control-plane-owned internal surface. Boundary tests fail if jobs or runtime-common reintroduce direct writes to those tables.
  - **Owner Suggestion:** Delivery Engineer Agent

- **[Jobs retains mirrored mutation helpers for control-plane runtime config and debug symbols]**
  - **Evidence:** `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md:9` gives Postgres/operator state to the control plane and `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md:10` says jobs does not own control-plane Postgres state. Yet `asset-allocation-jobs/core/runtime_config.py:338` inserts into `core.runtime_config` and `asset-allocation-jobs/core/runtime_config.py:377` deletes from it. `asset-allocation-jobs/core/debug_symbols.py:67` and `asset-allocation-jobs/core/debug_symbols.py:95` expose write/delete entrypoints that call those mutation helpers. Production jobs usage is read-only via `asset-allocation-jobs/core/core.py:110` and `asset-allocation-jobs/core/core.py:144`. The jobs and control-plane copies of `runtime_config.py` and `debug_symbols.py` are byte-identical based on `Get-FileHash`.
  - **Why it matters:** Even if the write paths are mostly unused in production, the jobs repo still carries a fully capable control-plane state mutator. That weakens the ownership model, creates duplicate maintenance surfaces, and makes future changes to operator-state validation or audit behavior easy to miss in one repo. The duplication also signals to future contributors that direct worker-side mutation is acceptable when it is not.
  - **Recommendation:** Remove runtime-config and debug-symbol mutation capability from `asset-allocation-jobs`. Keep the authoritative mutation logic only in `asset-allocation-control-plane`. If jobs still needs bootstrap reads, extract only the read/apply subset into `asset-allocation-runtime-common` or convert jobs to use an internal control-plane read client instead of carrying full mirrored modules.
  - **Acceptance Criteria:** `asset-allocation-jobs` no longer contains code paths that insert, update, or delete `core.runtime_config` rows or mutate debug-symbol state directly. Jobs still applies runtime-config overrides and refreshes debug symbols through a read-only surface. Tests in jobs no longer validate direct mutation of control-plane-owned Postgres state.
  - **Owner Suggestion:** Delivery Engineer Agent

### 3.3 Minor

- **[Mirror governance docs are lagging actual code drift]**
  - **Evidence:** The approved temporary mirror list in `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md:17-24` covers runtime-common shims only. It does not mention the mirrored `runtime_config.py` and `debug_symbols.py` copies in jobs, even though those files still exist and are identical to control-plane copies.
  - **Why it matters:** The code and the published mirror retirement plan are out of sync. That weakens the repo boundary contract and makes cleanup work easy to defer indefinitely.
  - **Recommendation:** Either remove the mirrored jobs modules now or add an explicit short-lived retirement entry with an issue and date. Removal is the better option.
  - **Acceptance Criteria:** Every cross-repo mirror is either deleted or explicitly listed in the ownership map with a retirement date and removal issue.
  - **Owner Suggestion:** Delivery Engineer Agent

## 4. Architectural Recommendations

- Keep the current five-repo split. Do not move UI, contracts, or generic control-plane transport into this repo.
- Re-center all control-plane-owned persistence behind control-plane-owned interfaces. For backtests, that means the worker produces results and the control plane commits them.
- Treat jobs as a read-only consumer of operator state. Runtime-config and debug-symbol writes should remain an operator/backend concern, not a worker concern.
- Narrow runtime-common back to its intended role: auth helpers, HTTP transport, read-only or explicitly scoped clients, and pure transforms. If a helper encodes `core.*` table names, run lifecycle transitions, or direct Postgres writes to control-plane schema, it is in the wrong layer.

Tradeoffs and phased migration plan:

- Phase 1: add a control-plane internal backtest-results ingest contract and tests that enforce table-write ownership.
- Phase 2: switch jobs to publish results through that surface; keep the payload version explicit.
- Phase 3: delete `asset-allocation-runtime-common.backtest_results` direct SQL helpers and remove jobs mutation helpers for runtime config and debug symbols.
- Phase 4: add repo-boundary tests in jobs and runtime-common that fail on direct SQL against control-plane-owned tables.

## 5. Operational Readiness & Observability

- Current gap: backtest result publication happens outside the control plane, so ingestion success, validation failures, row counts, and schema-version mismatches are not observed as first-class control-plane events.
- Required signals:
  - Structured event when a worker submits backtest results.
  - Structured event when the control plane accepts, rejects, or partially ingests a result publication.
  - Metrics for ingest duration, payload size, rows written by table, and schema-version mismatch rate.
  - Correlation IDs linking `run_id`, worker execution name, and control-plane ingest request.
- Release-readiness risk: as long as SQL writes live outside the control plane, schema rollouts and rollbacks are coupled across repos without a single choke point for validation.

## 6. Refactoring Examples (Targeted)

- **Before:**
  ```python
  # asset-allocation-jobs/core/backtest_runtime.py
  from asset_allocation_runtime_common import BACKTEST_RESULTS_SCHEMA_VERSION, persist_backtest_results

  persist_backtest_results(
      dsn,
      run_id=run_id,
      summary=summary,
      timeseries_rows=timeseries_rows,
      rolling_metric_rows=rolling_metric_rows,
      trade_rows=trade_rows,
  )
  ```

- **After:**
  ```python
  # jobs worker publishes results through a control-plane-owned internal API
  client.publish_backtest_results(
      run_id=run_id,
      schema_version=4,
      payload=result_payload,
  )
  ```

- **Before:**
  ```python
  # asset-allocation-jobs/core/runtime_config.py
  INSERT INTO core.runtime_config(...)
  DELETE FROM core.runtime_config ...
  ```

- **After:**
  ```python
  # jobs keeps read/apply only; control-plane remains the sole mutator
  applied = runtime_config_client.apply_runtime_overrides()
  debug_symbols = runtime_config_client.refresh_debug_symbols()
  ```

## 7. Evidence & Telemetry

- Files reviewed:
  - `asset-allocation-control-plane/docs/architecture/control-plane-architecture.md:1`
  - `asset-allocation-control-plane/docs/architecture/original-monolith-and-five-repo-map.md:1`
  - `asset-allocation-runtime-common/docs/architecture/repo-ownership-map.md:1`
  - `asset-allocation-control-plane/README.md:1`
  - `asset-allocation-jobs/README.md:1`
  - `asset-allocation-jobs/core/backtest_runtime.py:1`
  - `asset-allocation-runtime-common/python/asset_allocation_runtime_common/backtest_results.py:1`
  - `asset-allocation-jobs/core/runtime_config.py:1`
  - `asset-allocation-jobs/core/debug_symbols.py:1`
  - `asset-allocation-control-plane/api/endpoints/internal.py:1`
  - `asset-allocation-control-plane/api/endpoints/backtests.py:1`
  - `asset-allocation-jobs/core/core.py:1`
- Commands run:
  - `Get-Content` on control-plane, jobs, runtime-common, UI, and contracts docs and modules
  - `Get-FileHash` on mirrored jobs/control-plane modules
  - targeted `Select-String` searches for backtest persistence and runtime-config/debug-symbol mutation paths
- Telemetry and CI references:
  - No runtime logs, traces, or CI run IDs were required for this boundary audit.
