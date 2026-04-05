# Architecture & Code Audit Report

Historical note: this audit predates the structured Postgres-gold universe builder. The current strategy contract lives in [DATA.md](/mnt/c/Users/rdpro/Projects/AssetAllocation/DATA.md).

## 1. Executive Summary

The strategy subsystem has a clear control-plane contract: authenticated FastAPI endpoints validate a closed `StrategyConfig` schema, normalize defaults, and persist named strategy documents in Postgres for the React UI to edit. The strongest implemented runtime behavior is the exit engine, where `intrabarConflictPolicy` and `exits[]` are validated and exercised by unit tests. The biggest architectural gap is that most top-level strategy fields (`universe`, `rebalance`, `longOnly`, `topN`, `lookbackWindow`, `holdingPeriod`, `costModel`) are configurable and persisted but are not consumed by the runtime evaluator or simulator. Near-term priorities are to publish an execution matrix for every field, remove or gate unsupported affordances such as `type=code-based`, and replace the current long modal editor with a full-page workbench that combines authoring, validation, and preview.

## 2. System Map (High-Level)

- Key components and how they interact
  - Postgres stores strategy records in `core.strategies` with `name`, `type`, `description`, `config`, and timestamps.
  - `api/endpoints/strategies.py` exposes authenticated list, detail, config, and save routes.
  - `core/strategy_repository.py` is the persistence layer for reads and upserts.
  - `core/strategy_engine/contracts.py`, `exit_rules.py`, `position_state.py`, and `simulator.py` define the strategy contract and exit evaluation primitives.
  - `ui/src/app/components/pages/StrategyConfigPage.tsx` lists strategies, and `ui/src/app/components/pages/StrategyEditor.tsx` edits them in a sheet.

- Dependency direction and boundary notes
  - UI -> `strategyApi` -> FastAPI strategies router -> `StrategyRepository` -> Postgres.
  - API request bodies are normalized through `StrategyConfig.model_validate(...)` before storage.
  - Runtime logic currently depends on `config.exits` and `config.intrabarConflictPolicy`; the rest of the top-level strategy fields stop at the contract and editor layers.

- Data flows (requests, events, persistence)
  - Create or update: `POST /api/strategies/` -> validate and normalize -> `INSERT ... ON CONFLICT` into `core.strategies`.
  - Read summary list: `GET /api/strategies/` -> metadata only.
  - Read detail: `GET /api/strategies/{name}/detail` -> metadata plus normalized `config`.
  - Read config only: `GET /api/strategies/{name}` -> normalized `StrategyConfig`.
  - Exit evaluation: `StrategySimulator` and `ExitRuleEvaluator` can simulate exit behavior locally in code, but no API or UI path calls them today.

- Configurable surface inventory
  - Metadata
    - `name`
    - `type`
    - `description`
  - Strategy config
    - `universe`
    - `rebalance`
    - `longOnly`
    - `topN`
    - `lookbackWindow`
    - `holdingPeriod`
    - `costModel`
    - `intrabarConflictPolicy`
    - `exits[]`
  - Exit rule config
    - `id`
    - `enabled`
    - `type`
    - `scope`
    - `priceField`
    - `value`
    - `atrColumn`
    - `priority`
    - `action`
    - `minHoldBars`
    - `reference`

- How to configure it today
  - UI path: open `/strategies`, click `New Strategy`, or click an existing row to edit in the sheet.
  - API path: send an authenticated `POST /api/strategies/` payload containing top-level metadata plus a nested `config`.
  - Persistence prerequisite: `POSTGRES_DSN` must be configured. In this audit session it was empty, so no live strategy rows were inspected.

Example payload:

```json
{
  "name": "mom-sp500-exit-stack",
  "type": "configured",
  "description": "Momentum strategy with layered exits",
  "config": {
    "universe": {
      "source": "postgres_gold",
      "root": {
        "kind": "group",
        "operator": "and",
        "clauses": [
          {
            "kind": "condition",
            "table": "market_data",
            "column": "close",
            "operator": "gt",
            "value": 10
          }
        ]
      }
    },
    "rebalance": "monthly",
    "longOnly": true,
    "topN": 20,
    "lookbackWindow": 63,
    "holdingPeriod": 21,
    "costModel": "default",
    "intrabarConflictPolicy": "stop_first",
    "exits": [
      {
        "id": "stop-8",
        "type": "stop_loss_fixed",
        "value": 0.08
      },
      {
        "id": "time-40",
        "type": "time_stop",
        "value": 40
      }
    ]
  }
}
```

Notes:

- The backend rejects unexpected keys because both `StrategyConfig` and `ExitRule` use `extra="forbid"`.
- Omitted exit-rule fields are normalized where supported. For example, `enabled`, `priority`, `reference`, and `priceField` receive type-specific defaults.
- `scope` is currently fixed to `position`, and `action` is fixed to `exit_full`.

## 3. Findings (Triaged)

### 3.1 Critical (Must Fix)

- None found in the reviewed local source for strategy configuration and exit evaluation.

### 3.2 Major

- **[Stored Strategy Fields Exceed Runtime-Executed Strategy Fields]**
  - **Evidence:** `core/strategy_engine/contracts.py:102-110` defines `universe`, `rebalance`, `longOnly`, `topN`, `lookbackWindow`, `holdingPeriod`, `costModel`, `intrabarConflictPolicy`, and `exits`. `core/strategy_engine/exit_rules.py:49-58` and `core/strategy_engine/simulator.py:36-74` only consume `strategy_config.exits` and `strategy_config.intrabarConflictPolicy`. Repository-wide search found no runtime consumers for `universe`, `rebalance`, `longOnly`, `topN`, `lookbackWindow`, `holdingPeriod`, or `costModel` outside contracts, UI, docs, and tests.
  - **Why it matters:** Operators can configure selection and portfolio fields that appear meaningful in the editor but do not currently change simulated or executable behavior. That creates false confidence in backtests, reviews, and future live-trading assumptions.
  - **Recommendation:** Publish a field-level execution matrix immediately, then either wire each field into selection and rebalance logic or mark unsupported fields as metadata-only and disable them in the editor until implemented.
  - **Acceptance Criteria:** Every strategy field is labeled `runtime_enforced`, `metadata_only`, or `planned`; tests prove behavior changes for each `runtime_enforced` field; the UI visually warns when a saved field is not active in runtime.
  - **Owner Suggestion:** Delivery Engineer Agent / QA Release Gate Agent

- **[The UI Exposes `code-based` Strategies Without Any Runtime Contract]**
  - **Evidence:** `ui/src/app/components/pages/StrategyEditor.tsx:286-301` exposes `Configured` and `Code Based` in the editor. `api/endpoints/strategies.py:29-33` accepts any string for `type`. `core/strategy_repository.py:67-89` persists the value, but no runtime path branches on strategy type or resolves a code-based implementation.
  - **Why it matters:** The screen presents an operational mode that is not backed by loader, registry, or execution semantics. Users can save a record that looks valid but has no defined behavior difference from any other stored row.
  - **Recommendation:** Either remove `code-based` from the editor and reject unsupported types at the API boundary, or implement a typed strategy registry with explicit handlers and validation rules per type.
  - **Acceptance Criteria:** Unsupported types are rejected with a clear validation error, or a code-based type resolves to a concrete strategy implementation with tests covering load and execution behavior.
  - **Owner Suggestion:** Delivery Engineer Agent

- **[Strategy Changes Are Last-Write-Wins With No Versioning, Diff, Or Rollback]**
  - **Evidence:** `ui/src/services/strategyApi.ts:6-28` exposes only list, get, detail, and save. `api/endpoints/strategies.py:50-106` exposes the same CRUD subset. `core/strategy_repository.py:77-89` performs `INSERT ... ON CONFLICT (name) DO UPDATE` and overwrites the record, only updating `updated_at`.
  - **Why it matters:** Strategies are control-plane objects. Without revision history, optimistic concurrency, archive semantics, or restore, concurrent edits can silently overwrite each other and break reproducibility.
  - **Recommendation:** Add versioning with a revision table or explicit `version` field plus `If-Match`/ETag semantics, add archive/delete semantics, and surface diff/restore in the UI.
  - **Acceptance Criteria:** Saving a stale copy returns a conflict; prior revisions are queryable and restorable; UI can compare current draft vs last published version.
  - **Owner Suggestion:** Delivery Engineer Agent / QA Release Gate Agent

- **[No User-Facing Validation Or Preview Despite Existing Engine Primitives]**
  - **Evidence:** `core/strategy_engine/simulator.py:32-74` and `core/strategy_engine/exit_rules.py:39-184` provide local evaluation primitives. `ui/src/services/strategyApi.ts:6-28` exposes no validate or preview endpoint, and `ui/src/app/components/pages/StrategyConfigPage.tsx:38-74` only lists strategies and opens the editor sheet.
  - **Why it matters:** Users cannot test rule normalization, confirm priority behavior, detect missing ATR features, or see how intrabar conflicts resolve before saving. That weakens trust in the screen and increases operator error.
  - **Recommendation:** Add `POST /api/strategies/validate` and `POST /api/strategies/preview` endpoints that return normalized config, warnings, and sample exit-evaluation output; expose them in the UI as a live preview rail.
  - **Acceptance Criteria:** A user can preview normalized strategy JSON, see warnings for invalid or incomplete rule dependencies, and review at least one sample-bar exit decision before saving.
  - **Owner Suggestion:** Delivery Engineer Agent / QA Release Gate Agent

### 3.3 Minor

- **[Open String Fields Encourage Config Drift]**
  - **Evidence:** `core/strategy_engine/contracts.py:102-108` models `universe`, `rebalance`, and `costModel` as unconstrained strings beyond length checks. `ui/src/app/components/pages/StrategyEditor.tsx:316-372` uses free-text inputs for `universe` and `costModel`.
  - **Why it matters:** Strategies can accumulate inconsistent labels such as `sp500`, `SP500`, `S&P500`, or ad hoc cost model names, which makes grouping and downstream automation brittle.
  - **Recommendation:** Back these fields with catalogs or enums, and if custom values are required, mark them explicitly as custom rather than silently accepting arbitrary strings.
  - **Acceptance Criteria:** The API validates catalog-backed values or flags custom values explicitly; the UI presents canonical choices with help text.
  - **Owner Suggestion:** Delivery Engineer Agent

- **[Documentation Still Points To `platinum.strategies` While Runtime Uses `core.strategies`]**
  - **Evidence:** `DATA.md:62` identifies the strategy storage path as `platinum.strategies`. `core/strategy_repository.py:9` uses `core.strategies`, and `deploy/sql/postgres/migrations/0015_move_strategies_to_platinum.sql:7-53` migrates data into `core.strategies`.
  - **Why it matters:** This creates onboarding and support confusion when engineers inspect the wrong schema or assume stale storage conventions.
  - **Recommendation:** Update strategy storage documentation to reference `core.strategies` and note the historical migration from `public` and `platinum`.
  - **Acceptance Criteria:** Docs, migrations, and repository code all reference the same canonical table.
  - **Owner Suggestion:** Technical Writer Dev Advocate / Delivery Engineer Agent

- **[Strategy Operations Have Minimal Telemetry And No Audit Trail]**
  - **Evidence:** `api/endpoints/strategies.py` and `core/strategy_repository.py` log only warnings or errors. No counters, traces, request correlation, actor attribution, or change summaries are emitted for strategy operations.
  - **Why it matters:** Failed saves, frequent edits, and configuration churn are hard to diagnose, and there is no reliable record of who changed a strategy and when beyond `updated_at`.
  - **Recommendation:** Add structured logs, counters, latency histograms, and an audit trail for create, update, archive, validate, and publish actions.
  - **Acceptance Criteria:** Strategy operations emit correlated logs and metrics, and every configuration change includes actor, timestamp, old/new version, and change summary.
  - **Owner Suggestion:** Delivery Engineer Agent / DevOps Agent

## 4. Architectural Recommendations

- Structural improvements
  - Split the current mixed `StrategyConfig` object into clear domains: selection, portfolio construction, exit logic, and lifecycle metadata. The current flat model makes stored metadata look executable.
  - Introduce a discriminated strategy mode, for example `configured` vs `code_based`, only if each mode has a concrete loader, schema, and execution contract.
  - Add a service layer between API and repository for strategy normalization, diff generation, validation warnings, and publish/version rules.

- Configuration guide: what is configurable today and how

| Surface | Fields | Configure in UI | Configure via API | Runtime-enforced today |
| --- | --- | --- | --- | --- |
| Metadata | `name`, `type`, `description` | Metadata section in the editor sheet | Top-level JSON fields | No; stored and displayed only |
| Selection and portfolio inputs | `universe`, `rebalance`, `longOnly`, `topN`, `lookbackWindow`, `holdingPeriod`, `costModel` | Configuration section in the editor sheet | `config.*` | No evidence of runtime enforcement in current engine |
| Exit policy | `intrabarConflictPolicy` | Configuration section in the editor sheet | `config.intrabarConflictPolicy` | Yes |
| Exit rules | `exits[]` and nested rule fields | Exit Rules section in the editor sheet | `config.exits[]` | Yes |
| Fixed rule constraints | `scope=position`, `action=exit_full` | Implied by UI copy; not directly editable | Allowed in payload but only one value is valid | Yes, as fixed contract values |

- Recommended screen functionality
  - Replace the current modal sheet with a full-page strategy workbench. The existing editor is already long enough that future validation, preview, and versioning features will become cramped in a drawer.
  - Add searchable strategy catalog controls: search, filter by type, sort by updated date, and status chips for `draft`, `published`, `archived`, and `metadata-only fields present`.
  - Add `Clone`, `Archive`, `Delete`, `Compare`, `Validate`, and `Preview` actions next to `Save`.
  - Show field-level help with defaults and runtime status, for example `Runtime enforced`, `Stored only`, or `Planned`.
  - Add rule templates with inline semantic labels such as `8% stop below entry`, `3 ATR trail from highest high`, and `exit after 40 bars`.
  - Add normalized JSON preview and import/export so advanced users can audit or script configuration changes.
  - Add sample evaluation panels: trigger price calculator, priority ordering preview, intrabar conflict resolution example, and ATR-column dependency checks.
  - Show provenance: last updated time, actor, revision number, linked backtests, and publish status.

- Design Direction
  - Preserve the current paper-and-walnut editorial theme (`Josefin Sans` display, `Montserrat` body, walnut/cream/mustard/teal palette) and turn the strategy editor into a ledger-style operator workbench rather than a generic settings form.
  - The screen should read as a trading desk notebook: left rail for catalog and status, center canvas for authoring, right rail for semantic preview and warnings.

- Signature
  - A persistent left rail lists strategies like ledger entries, with compact status chips and timestamps.
  - The center canvas groups fields by intent: `Identity`, `Selection`, `Execution`, and `Exit Stack`, not one long undifferentiated form.
  - The right rail is live and semantic, showing normalized JSON, rule math, sample outcomes, and dependency warnings instead of raw helper text only.
  - A sticky footer or header action bar keeps `Validate`, `Compare`, `Save Draft`, and `Publish` visible during long edits.

- Recommended layout

```text
+---------------------------------------------------------------------------------------------+
| Strategy Workbench                                             [Validate] [Save Draft] [Publish] |
+-------------------------+--------------------------------------+----------------------------------+
| Strategy Catalog        | Strategy Canvas                      | Preview and Diagnostics          |
|                         |                                      |                                  |
| Search                  | Identity                             | Runtime Status                   |
| Filters                 | - name                               | - runtime-enforced fields        |
|                         | - type                               | - metadata-only fields           |
| [mom-sp500]  Draft      | - description                        | - last validation result         |
| [trend-atr]  Published  |                                      |                                  |
| [rebalance-q] Archived  | Selection                            | Normalized JSON                  |
|                         | - universe                           |                                  |
| + New Strategy          | - rebalance                          | Rule Semantics                   |
| Clone / Archive / Diff  | - longOnly                           | - stop-8 => 92.00 on entry 100   |
|                         | - topN                               | - time-40 => close after 40 bars |
|                         | - lookbackWindow                     |                                  |
|                         | - holdingPeriod                      | Sample Evaluation                |
|                         | - costModel                          | - chosen exit                    |
|                         |                                      | - conflict policy outcome        |
|                         | Execution                            | - missing ATR column warnings    |
|                         | - intrabarConflictPolicy             |                                  |
|                         |                                      | Revision History                 |
|                         | Exit Stack                           | - v12 published by user X        |
|                         | - draggable rule cards               | - compare with current draft     |
|                         | - template add buttons               |                                  |
+-------------------------+--------------------------------------+----------------------------------+
```

- Responsive behavior
  - Desktop: three columns as shown above.
  - Tablet: left rail collapses to a drawer, preview rail docks below the form.
  - Mobile: stacked tabs for `Catalog`, `Edit`, and `Preview`, with sticky action bar at the bottom.

- Tech alignment and phased migration plan
  - Phase 1: update the screen copy to mark metadata-only fields, remove unsupported `code-based` affordance, and fix storage docs.
  - Phase 2: add validate and preview APIs, normalized JSON preview, dependency warnings, and versioning.
  - Phase 3: implement actual selection and rebalance execution paths or reduce the editor surface to only active runtime fields.

## 5. Operational Readiness & Observability

- Gaps in health checks, metrics, logging, traces
  - Platform-level `/healthz` and `/readyz` exist, but strategy-specific endpoints do not emit operational metrics beyond generic error logs.
  - No trace spans or structured logs identify strategy name, actor, version, or change summary for create/update events.
  - No audit table or revision trail exists for strategy changes.
  - No preview-time diagnostics exist for missing data dependencies such as absent ATR features.

- Required signals and correlation strategy
  - Counters: `strategy_list_total`, `strategy_detail_total`, `strategy_save_total`, `strategy_validate_total`, `strategy_publish_total`, `strategy_save_conflict_total`.
  - Histograms: `strategy_save_latency_ms`, `strategy_validate_latency_ms`, `strategy_preview_latency_ms`.
  - Structured logs: `strategy_name`, `strategy_type`, `version`, `actor`, `request_id`, `result`, `warning_count`.
  - Audit trail: old version hash, new version hash, diff summary, actor, and timestamp.
  - Preview diagnostics: missing required feature columns, duplicated IDs, priority collisions, and metadata-only-field warnings.

- Release-readiness risks tied to telemetry evidence
  - A richer strategy workbench should not ship without validation and revision telemetry; otherwise the UI becomes easier to use while remaining hard to trust.
  - If strategy editing becomes a production control plane, auditability and rollback are operational requirements rather than optional nice-to-haves.

## 6. Refactoring Examples (Targeted)

- **Before:**
  ```python
  class StrategyConfig(BaseModel):
      universe: str = Field(default="SP500", min_length=1, max_length=128)
      rebalance: str = Field(default="monthly", min_length=1, max_length=64)
      longOnly: bool = True
      topN: int = Field(default=20, ge=1)
      lookbackWindow: int = Field(default=63, ge=1)
      holdingPeriod: int = Field(default=21, ge=1)
      costModel: str = Field(default="default", min_length=1, max_length=64)
      intrabarConflictPolicy: IntrabarConflictPolicy = "stop_first"
      exits: list[ExitRule] = Field(default_factory=list)
  ```
  Suggested direction: split the flat model into clearly named sub-objects such as `selection`, `execution`, and `exits`, then mark each block as runtime-enforced or metadata-only.

- **Before:**
  ```python
  @router.post("/")
  async def save_strategy(strategy: StrategyUpsertRequest, request: Request) -> dict[str, str]:
      ...
  ```
  Suggested direction: keep save simple, but add companion endpoints for `validate`, `preview`, and `revisions`, and require a version token for update operations.

## 7. Evidence & Telemetry

- Files reviewed
  - `core/strategy_engine/contracts.py`
  - `core/strategy_engine/exit_rules.py`
  - `core/strategy_engine/simulator.py`
  - `core/strategy_engine/position_state.py`
  - `core/strategy_repository.py`
  - `api/endpoints/strategies.py`
  - `ui/src/services/strategyApi.ts`
  - `ui/src/types/strategy.ts`
  - `ui/src/app/components/pages/StrategyConfigPage.tsx`
  - `ui/src/app/components/pages/StrategyEditor.tsx`
  - `ui/src/styles/theme.css`
  - `DATA.md`
  - `README.md`
  - `tests/api/test_strategies.py`
  - `tests/core/test_strategy_repository.py`
  - `tests/core/strategy_engine/test_contracts.py`
  - `tests/core/strategy_engine/test_exit_rules.py`
  - `deploy/sql/postgres/migrations/0007_create_strategies_table.sql`
  - `deploy/sql/postgres/migrations/0015_move_strategies_to_platinum.sql`

- Commands run
  - `rg -n "strategy|strategies|allocation model|rebalance|signal|indicator|configur|parameter" .`
  - `rg --files | rg 'strategy|strategies|allocation|config|yaml|json|toml|md$'`
  - `sed -n` and `nl -ba` against the strategy backend, UI, tests, migrations, and docs
  - `printf '%s\n' "$POSTGRES_DSN"`
  - `git status --short`

- Log/trace IDs or CI run references
  - None available in this local audit.

- Scope note
  - No live strategy rows were inspected because `POSTGRES_DSN` was not configured in this session. Findings are based on local source, tests, migrations, and documentation.
