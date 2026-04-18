# DATA

This document is the current data interface contract for AssetAllocation. It covers the canonical persisted datasets written by the Bronze, Silver, and Gold jobs under `tasks/` for the `market`, `finance`, `earnings`, and `price-target` domains, plus the Postgres-backed strategy and ranking control-plane contracts used by the strategies and rankings APIs and UI.

Scope notes:

- This contract primarily documents the bucketed medallion tables, with control-plane exceptions for `core.strategies`, `core.ranking_schemas`, `core.ranking_runs`, and `core.ranking_watermarks`.
- `finance` Bronze stores provider payloads as opaque JSON strings. Downstream contracts are the extracted Silver and Gold schemas, not the provider's full inner JSON shape.
- The Postgres `gold.*` tables are serving replicas of the canonical Gold Delta buckets. `core.gold_sync_state` tracks whether each alphabet bucket has been fully synchronized for a given source commit.

## Contract Conventions

| Convention | Meaning |
| --- | --- |
| `symbol` | Uppercased ticker/symbol identifier used across all domains. |
| `date` / `obs_date` | Timezone-naive normalized date/datetime field written by the ETL job. |
| `number` | Nullable numeric metric. Writer code typically uses `float64` unless explicitly noted otherwise. |
| `binary flag` | `0` or `1` indicator derived from business logic or pattern detection. |
| `nullable int` | Integer metric persisted with null support. |
| `json string` | Compact serialized provider payload stored as a string; inner keys are provider-defined and not frozen by this repo. |
| `bucket` layout | Canonical persisted datasets are written to alphabet bucket paths such as `.../buckets/A`, `.../buckets/B`, and so on. |

## Layer And Domain Inventory

| Layer | Domain | Canonical path pattern | Row grain | Notes |
| --- | --- | --- | --- | --- |
| Bronze | market | `market-data/buckets/{bucket}` | `symbol` + `date` | Raw market bars normalized to a stable OHLCV-plus-short-interest shape. |
| Silver | market | `market-data/buckets/{bucket}` | `symbol` + `date` | Canonical market history with stable snake_case columns. |
| Gold | market | `market/buckets/{bucket}` | `symbol` + `date` | Technical-feature table built from Silver OHLCV history. |
| Bronze | finance | `finance-data/buckets/{bucket}` | `symbol` + `report_type` | Raw Alpha Vantage report payloads plus coverage metadata. |
| Silver | finance / `balance_sheet` | `finance-data/balance_sheet/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled balance-sheet subset for Piotroski inputs. |
| Silver | finance / `income_statement` | `finance-data/income_statement/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled income-statement subset for Piotroski inputs. |
| Silver | finance / `cash_flow` | `finance-data/cash_flow/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled cash-flow subset for Piotroski inputs. |
| Silver | finance / `valuation` | `finance-data/valuation/buckets/{bucket}` | `symbol` + `date` | Daily forward-filled valuation snapshot built from `overview` plus Silver close prices. |
| Gold | finance | `finance/buckets/{bucket}` | `symbol` + `date` | Piotroski components and F-score plus selected valuation metrics. |
| Bronze | earnings | `earnings-data/buckets/{bucket}` | `symbol` + `date` | Canonical earnings events combining historical actuals and upcoming scheduled report dates. |
| Silver | earnings | `earnings-data/buckets/{bucket}` | `symbol` + `date` | Canonical earnings history plus retained upcoming scheduled events. |
| Gold | earnings | `earnings/buckets/{bucket}` | `symbol` + `date` | Daily earnings-surprise features plus derived upcoming-earnings context. |
| Bronze | price-target | `price-target-data/buckets/{bucket}` | `symbol` + `obs_date` | Raw analyst target snapshots plus ingestion metadata. |
| Silver | price-target | `price-target-data/buckets/{bucket}` | `symbol` + `obs_date` | Daily forward-filled target history. |
| Gold | price-target | `targets/buckets/{bucket}` | `symbol` + `obs_date` | Dispersion and revision features built from Silver targets. |
| Control plane | strategies | `Postgres table core.strategies` | `name` | Strategy metadata plus validated `StrategyConfig` JSON used by `/strategies` API routes and the React strategy editor. |
| Control plane | ranking schemas | `Postgres table core.ranking_schemas` | `name` | Validated `RankingSchemaConfig` documents used by `/rankings` API routes, the React ranking workbench, and ranking materialization. |
| Platinum | strategy rankings | `Postgres table platinum.<strategy_output_table>` | `date` + `symbol` | Materialized cross-sectional symbol ranks per strategy/date generated from a strategy universe plus a ranking schema. |

## Gold Postgres Serving Replica

The Gold jobs can optionally mirror successful Gold bucket writes into Postgres when `POSTGRES_DSN` is configured.

| Object | Role | Grain |
| --- | --- | --- |
| `gold.market_data` | Serving replica of Gold market features with symbol/date and date/symbol indexes. | `symbol` + `date` |
| `gold.finance_data` | Serving replica of Gold finance features with symbol/date and date/symbol indexes. | `symbol` + `date` |
| `gold.earnings_data` | Serving replica of Gold earnings features with symbol/date and date/symbol indexes. | `symbol` + `date` |
| `gold.price_target_data` | Serving replica of Gold price-target features with symbol/obs_date and obs_date/symbol indexes. | `symbol` + `obs_date` |
| `gold.*_by_date` views | By-date accessors over the same physical Gold serving tables. | Same as base table |
| `core.gold_sync_state` | Per-domain, per-bucket sync checkpoint used to bootstrap full population before incremental skip behavior resumes. | `domain` + `bucket` |

## Ranking Control Plane

### Ranking Schema Storage

Path: `Postgres table core.ranking_schemas`

Each row stores one named ranking schema plus a versioned `config` payload. Revisions are appended to `core.ranking_schema_revisions`, materialization runs are recorded in `core.ranking_runs`, and per-strategy freshness is tracked in `core.ranking_watermarks`.

| Column | Type | Description |
| --- | --- | --- |
| `name` | string | Unique ranking schema identifier referenced by strategy config. |
| `description` | string | Optional human-readable description shown in the ranking workbench. |
| `version` | integer | Monotonic version incremented on each save. |
| `config` | JSON object | Validated `RankingSchemaConfig` describing groups, weighted factors, and ordered transforms. |
| `updated_at` | datetime | Server-managed timestamp set to `NOW()` on insert and update. |

### Ranking Schema Config: `config`

| Field | Type | Description |
| --- | --- | --- |
| `universeConfigName` | string | Required name of the saved universe configuration applied to the ranking schema itself. Ranking previews and materialization intersect this universe with the selected strategy universe. |
| `groups` | array of `RankingGroup` | Required array of weighted groups. |
| `overallTransforms` | array of `RankingTransform` | Ordered transforms applied after weighted group aggregation. |

### Ranking Group

| Field | Type | Description |
| --- | --- | --- |
| `name` | string | Unique group name within the schema. |
| `weight` | number | Relative contribution of the group to the final score. |
| `factors` | array of `RankingFactor` | Required array of weighted factor definitions. |
| `transforms` | array of `RankingTransform` | Ordered transforms applied to the group score after factor aggregation. |

### Ranking Factor

| Field | Type | Description |
| --- | --- | --- |
| `name` | string | Unique factor name within the containing group. |
| `table` | string | Gold serving table name, for example `market_data` or `finance_data`. |
| `column` | string | Numeric or boolean scalar column in the selected gold table. |
| `weight` | number | Relative contribution of the factor within the group. |
| `direction` | enum | `desc` means higher values are better; `asc` means lower values are better. |
| `missingValuePolicy` | enum | `exclude` drops rows with missing factor values; `zero` fills missing values with `0`. |
| `transforms` | array of `RankingTransform` | Ordered transform chain applied to the raw factor values before weighted group aggregation. |

### Ranking Transform

Supported transform types:

- `percentile_rank`
- `zscore`
- `minmax`
- `clip`
- `winsorize`
- `coalesce`
- `log1p`
- `negate`
- `abs`

Parameter rules:

- `clip` accepts `lower` and/or `upper`
- `winsorize` accepts `lowerQuantile` and/or `upperQuantile`
- `coalesce` requires `value`
- the remaining transform types reject params

### Strategy-to-Ranking Link

`StrategyConfig` now supports `rankingSchemaName`, which links a saved strategy to a named ranking schema. `core.strategies.output_table_name` stores the sanitized platinum table identifier used during materialization.

### Ranking Materialization Output

Path: `Postgres table platinum.<strategy_output_table>`

| Column | Type | Description |
| --- | --- | --- |
| `date` | date | Ranking as-of date. |
| `symbol` | string | Uppercased ticker/symbol identifier. |
| `rank` | integer | Cross-sectional rank for the strategy/date where `1` is best. |
| `score` | number | Weighted composite score used to order symbols before rank assignment. |
| `last_updated_date` | date | Date the row was last materialized into platinum. |

## Strategies

### Strategy Storage

Path: `Postgres table core.strategies`

Each row stores a single named strategy. The API returns either the full record (`/strategies/{name}/detail`) or just the validated `config` payload (`/strategies/{name}`). On create or update, the API validates and normalizes the incoming `config` against `StrategyConfig` before writing it back to Postgres.

| Column | Type | Description |
| --- | --- | --- |
| `name` | string | Unique strategy identifier. This is the lookup key used by the repository and API routes. |
| `type` | string | Freeform strategy classification string. The API defaults it to `configured`; the current UI offers `configured` and `code-based`. |
| `description` | string | Optional human-readable description shown in the UI and returned by the detail API. |
| `updated_at` | datetime | Server-managed timestamp set to `NOW()` on insert and update. |
| `config` | JSON object | Nested `StrategyConfig` document containing selection settings, conflict policy, and exit-rule definitions. |

### Strategy Config: `config`

Unexpected keys are rejected. The backend model uses `extra="forbid"` for both `StrategyConfig` and `ExitRule`, so the contract below is closed rather than open-ended.

| Field | Type | Description |
| --- | --- | --- |
| `universeConfigName` | string | Required name of the saved universe configuration attached to the strategy. Strategy materialization resolves this config independently from the ranking schema universe. |
| `rebalance` | string | Rebalance cadence label. Default is `monthly`; the current UI offers `daily`, `weekly`, `monthly`, and `quarterly`. |
| `longOnly` | boolean | Long-only flag for the strategy. Default is `true`. |
| `topN` | integer | Target number of selected holdings or symbols. Must be `>= 1`. Default is `20`. |
| `lookbackWindow` | integer | Lookback window used by strategy logic. Must be `>= 1`. Default is `63`. |
| `holdingPeriod` | integer | Planned holding horizon in bars or days. Must be `>= 1`. Default is `21`. |
| `costModel` | string | Cost-model identifier used by strategy or simulation logic. Default is `default`. |
| `intrabarConflictPolicy` | enum | Same-bar tie-break policy when multiple exit rules trigger. Supported values are `stop_first`, `take_profit_first`, and `priority_order`. Default is `stop_first`. |
| `exits` | array of `ExitRule` | Ordered list of exit rules. Duplicate rule IDs are rejected. When `priority` is omitted, the backend normalizes it to the rule's array index. |

### Universe Config Storage

Path: `Postgres table core.universe_configs`

Universe configurations are first-class saved objects reused by both strategies and ranking schemas. Revisions are appended to `core.universe_config_revisions`.

| Column | Type | Description |
| --- | --- | --- |
| `name` | string | Unique universe configuration identifier. |
| `description` | string | Optional UI-facing description. |
| `version` | integer | Monotonic version incremented on each save. |
| `config` | JSON object (`UniverseDefinition`) | Structured rule tree filtered against Postgres `gold.*` serving tables. |
| `updated_at` | datetime | Server-managed timestamp set to `NOW()` on insert and update. |

### Universe Definition: `core.universe_configs.config`

Universe authoring is exposed through a public field catalog. The editor builds a recursive rule tree, and the preview endpoint resolves the current matching symbol set by translating stable field ids to the local gold serving tables used by the control plane.

| Field | Type | Description |
| --- | --- | --- |
| `source` | enum | The only supported value is `postgres_gold`. |
| `root` | object (`UniverseGroup`) | Root boolean group for the universe rule tree. Must contain at least one clause. |

### Universe Groups And Conditions

| Field | Type | Description |
| --- | --- | --- |
| `kind` | enum | Discriminator. Groups use `group`; conditions use `condition`. |
| `operator` | enum | Group operator. Supported values are `and` and `or`. |
| `clauses` | array | Child groups or conditions. Empty groups are rejected by the backend. |
| `field` | string | Stable public universe field id, for example `market.close` or `quality.piotroski_f_score`. The backend resolves the field against its local gold-table map and rejects unknown ids. |
| `operator` | enum | Condition operator. Supported values are `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`, and `is_not_null`. |
| `value` | string, number, boolean, or null | Single-value input used by scalar operators such as `eq` or `gt`. Null operators reject both `value` and `values`. |
| `values` | array of string, number, or boolean | Multi-value input used only by `in` and `not_in`. |

### Universe Preview

- `GET /api/strategies/universe/catalog` returns public field ids, labels, value kinds, and allowed operators.
- `POST /api/strategies/universe/preview` accepts a draft `UniverseDefinition` and returns `symbolCount`, `sampleSymbols`, `fieldsUsed`, and `warnings`.
- Preview is current-state only for this milestone. It evaluates each condition against the latest available row per symbol in the referenced gold table and then combines clause results with `and` intersection or `or` union.

### Exit Rule Components: `config.exits[]`

Milestone 1 keeps exit handling constrained to full position exits. The UI reflects that by treating `scope` and `action` as fixed values.

| Field | Type | Description |
| --- | --- | --- |
| `id` | string | Required rule identifier, unique within the strategy. This ID is emitted in exit decisions and trade metadata. |
| `enabled` | boolean | When `false`, the evaluator skips the rule entirely. Default is `true`. |
| `type` | enum | Rule type. Supported values are `stop_loss_fixed`, `take_profit_fixed`, `trailing_stop_pct`, `trailing_stop_atr`, and `time_stop`. |
| `scope` | enum | Exit scope. The only supported value is `position`. Default is `position`. |
| `priceField` | enum or null | Price observed on the bar to check whether the rule triggered. Supported values are `open`, `high`, `low`, and `close`. Type-specific defaults apply when omitted. |
| `value` | number or null | Rule threshold. For fixed and trailing-percent exits it is a percentage-like decimal, for ATR trailing stops it is an ATR multiple, and for `time_stop` it is a bar count. Must be `> 0`; `time_stop` requires an integer value. |
| `atrColumn` | string or null | Feature column name used only by `trailing_stop_atr`. Required for that rule type and rejected for all others. |
| `priority` | integer or null | Explicit rule order for resolution. Lower numbers win. If omitted, the backend assigns the array index. |
| `action` | enum | Exit action. The only supported value is `exit_full`. Default is `exit_full`. |
| `minHoldBars` | integer | Minimum `bars_held` before the rule is eligible to fire. Must be `>= 0`. Default is `0`. |
| `reference` | enum or null | Trigger anchor for price-based rules. Supported values are `entry_price` and `highest_since_entry`. Type-specific defaults apply; `time_stop` rejects `reference`. |

### Exit Rule Types And Trigger Semantics

| Rule type | Trigger calculation | Required fields | Defaults and constraints | Exit price recorded |
| --- | --- | --- | --- | --- |
| `stop_loss_fixed` | `entry_price * (1 - value)` | `id`, `type`, `value` | Defaults `reference` to `entry_price` and `priceField` to `low`. Rejects `atrColumn`. | Computed trigger price, not the observed bar price. |
| `take_profit_fixed` | `entry_price * (1 + value)` | `id`, `type`, `value` | Defaults `reference` to `entry_price` and `priceField` to `high`. Rejects `atrColumn`. | Computed trigger price. |
| `trailing_stop_pct` | `(highest_since_entry or entry_price) * (1 - value)` | `id`, `type`, `value` | Defaults `reference` to `highest_since_entry` and `priceField` to `low`. Rejects `atrColumn`. | Computed trigger price. |
| `trailing_stop_atr` | `(highest_since_entry or entry_price) - (value * bar.features[atrColumn])` | `id`, `type`, `value`, `atrColumn` | Defaults `reference` to `highest_since_entry` and `priceField` to `low`. If the ATR feature is missing on the bar, the rule does not fire. | Computed trigger price. |
| `time_stop` | Fires once `bars_held >= value` | `id`, `type`, integer `value` | Forces `priceField` to `close`. Rejects `reference` and `atrColumn`. | Current bar `close` price. |

### Intrabar Conflict Policies

The evaluator sorts triggered candidates by `(priority, ordinal)` before applying policy-specific logic. `ordinal` is the rule's array position, so array order is the final tie-breaker when priorities match.

| Policy | Behavior |
| --- | --- |
| `stop_first` | If a stop-like rule and a take-profit rule trigger on the same bar, choose the first stop-like candidate after ordering. If all triggered rules are from the same class, choose the first ordered rule. |
| `take_profit_first` | If both classes trigger on the same bar, choose the first take-profit candidate after ordering. Otherwise choose the first ordered rule. |
| `priority_order` | Ignore stop-versus-profit class and choose the first ordered rule directly. |

### Runtime Evaluation Notes

- The evaluator advances position state before testing exit rules. That means `bars_held` is incremented for the current bar first, and trailing-stop anchors such as `highest_since_entry` include the current bar's prices before trigger evaluation.
- A price-threshold rule checks the selected `priceField` against the trigger, but the emitted `exit_price` is the computed trigger price rather than the observed intrabar print.
- Disabled rules and rules blocked by `minHoldBars` are skipped.
- When more than one rule triggers on a bar, the evaluation records an intrabar conflict and returns a single chosen exit decision according to `intrabarConflictPolicy`.

### Editor-Seeded Defaults

These are UI defaults for newly created strategies and rules, not additional backend requirements:

- New run configurations start with no attached universe config, `rebalance=monthly`, `longOnly=true`, `topN=20`, `lookbackWindow=63`, `holdingPeriod=21`, `costModel=default`, `intrabarConflictPolicy=stop_first`, and an empty `exits` array.
- New universe configurations start with `source=postgres_gold` and a root `and` group seeded with a single blank condition.
- New `stop_loss_fixed` rules are seeded with `value=0.08`, `reference=entry_price`, and `priceField=low`.
- New `take_profit_fixed` rules are seeded with `value=0.15`, `reference=entry_price`, and `priceField=high`.
- New `trailing_stop_pct` rules are seeded with `value=0.07`, `reference=highest_since_entry`, and `priceField=low`.
- New `trailing_stop_atr` rules are seeded with `value=3`, `atrColumn=atr_14d`, `reference=highest_since_entry`, and `priceField=low`.
- New `time_stop` rules are seeded with `value=40` and `priceField=close`.

Evidence:

- `core/strategy_repository.py:9-35`
- `core/strategy_repository.py:40-89`
- `api/endpoints/strategies.py:25-46`
- `api/endpoints/strategies.py:75-106`
- `core/strategy_engine/contracts.py:7-121`
- `core/strategy_engine/exit_rules.py:39-184`
- `core/strategy_engine/position_state.py:42-68`
- `ui/src/app/components/pages/StrategyEditor.tsx:64-150`
- `ui/src/app/components/pages/StrategyEditor.tsx:376-619`
- `tests/core/strategy_engine/test_contracts.py:6-61`
- `tests/core/strategy_engine/test_exit_rules.py:10-166`

## Market

### Bronze Market

Path: `market-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `date` | datetime | Trading session date for the bar. |
| `open` | number | Session opening price. |
| `high` | number | Session high price. |
| `low` | number | Session low price. |
| `close` | number | Session closing price. |
| `volume` | number | Session traded volume. |
| `short_interest` | number | Short-interest value joined during Bronze ingestion when available. |
| `short_volume` | number | Short-volume value joined during Bronze ingestion when available. |
| `ingested_at` | string | UTC ingestion timestamp recorded when the Bronze row is written. |
| `source_hash` | string | Hash of the normalized Bronze payload used for change detection and watermarking. |

### Silver Market

Path: `market-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Canonical trading date. |
| `symbol` | string | Uppercased ticker symbol. |
| `open` | number | Session opening price. |
| `high` | number | Session high price. |
| `low` | number | Session low price. |
| `close` | number | Session closing price. |
| `volume` | number | Session traded volume. |
| `short_interest` | number | Canonical short-interest metric. |
| `short_volume` | number | Canonical short-volume metric. |

### Gold Market

Path: `market/buckets/{bucket}`

Base columns:

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Feature as-of date. |
| `symbol` | string | Uppercased ticker symbol. |
| `open` | number | Input open price carried from Silver. |
| `high` | number | Input high price carried from Silver. |
| `low` | number | Input low price carried from Silver. |
| `close` | number | Input close price carried from Silver. |
| `volume` | number | Input volume carried from Silver. |

Return, volatility, range, and volume-context columns:

| Column | Type | Description |
| --- | --- | --- |
| `return_1d` | number | One-day close-to-close return. |
| `return_5d` | number | Five-day close-to-close return. |
| `return_20d` | number | Twenty-day close-to-close return. |
| `return_60d` | number | Sixty-day close-to-close return. |
| `vol_20d` | number | Rolling 20-day standard deviation of daily return. |
| `vol_60d` | number | Rolling 60-day standard deviation of daily return. |
| `rolling_max_252d` | number | Rolling 252-day maximum close. |
| `drawdown_1y` | number | Current close divided by rolling 252-day max minus 1. |
| `true_range` | number | Max of intraday range and prior-close gap range. |
| `atr_14d` | number | Fourteen-day average true range. |
| `gap_atr` | number | Absolute open-to-prior-close gap normalized by ATR. |
| `bb_width_20d` | number | Normalized 20-day Bollinger-band width. |
| `range_close` | number | Intraday range divided by close. |
| `range_20` | number | Rolling 20-day high-low range divided by close. |
| `compression_score` | number | Percentile rank of `range_20` over a 252-day lookback. |
| `volume_z_20d` | number | Twenty-day z-score of volume. |
| `volume_pct_rank_252d` | number | Percentile rank of volume over a 252-day lookback. |

Trend and moving-average columns:

| Column | Type | Description |
| --- | --- | --- |
| `sma_20d` | number | 20-day simple moving average of close. |
| `sma_50d` | number | 50-day simple moving average of close. |
| `sma_200d` | number | 200-day simple moving average of close. |
| `sma_20_gt_sma_50` | binary flag | `1` when `sma_20d > sma_50d`. |
| `sma_50_gt_sma_200` | binary flag | `1` when `sma_50d > sma_200d`. |
| `trend_50_200` | number | `sma_50d / sma_200d - 1`. |
| `above_sma_50` | binary flag | `1` when close is above `sma_50d`. |
| `sma_20_crosses_above_sma_50` | binary flag | `1` on the row where `sma_20_gt_sma_50` flips from `0` to `1`. |
| `sma_20_crosses_below_sma_50` | binary flag | `1` on the row where `sma_20_gt_sma_50` flips from `1` to `0`. |
| `sma_50_crosses_above_sma_200` | binary flag | `1` on the row where `sma_50_gt_sma_200` flips from `0` to `1`. |
| `sma_50_crosses_below_sma_200` | binary flag | `1` on the row where `sma_50_gt_sma_200` flips from `1` to `0`. |

Market-structure columns:

| Column | Type | Description |
| --- | --- | --- |
| `donchian_high_20d` | number | Prior 20-day rolling high, shifted one row to avoid look-ahead. |
| `donchian_low_20d` | number | Prior 20-day rolling low, shifted one row to avoid look-ahead. |
| `dist_donchian_high_20d_atr` | number | `(donchian_high_20d - close) / atr_14d`. |
| `dist_donchian_low_20d_atr` | number | `(close - donchian_low_20d) / atr_14d`. |
| `above_donchian_high_20d` | binary flag | `1` when close is above the prior 20-day high. |
| `below_donchian_low_20d` | binary flag | `1` when close is below the prior 20-day low. |
| `crosses_above_donchian_high_20d` | binary flag | `1` when close breaks above the prior 20-day high on that row. |
| `crosses_below_donchian_low_20d` | binary flag | `1` when close breaks below the prior 20-day low on that row. |
| `donchian_high_55d` | number | Prior 55-day rolling high, shifted one row to avoid look-ahead. |
| `donchian_low_55d` | number | Prior 55-day rolling low, shifted one row to avoid look-ahead. |
| `dist_donchian_high_55d_atr` | number | `(donchian_high_55d - close) / atr_14d`. |
| `dist_donchian_low_55d_atr` | number | `(close - donchian_low_55d) / atr_14d`. |
| `above_donchian_high_55d` | binary flag | `1` when close is above the prior 55-day high. |
| `below_donchian_low_55d` | binary flag | `1` when close is below the prior 55-day low. |
| `crosses_above_donchian_high_55d` | binary flag | `1` when close breaks above the prior 55-day high on that row. |
| `crosses_below_donchian_low_55d` | binary flag | `1` when close breaks below the prior 55-day low on that row. |
| `sr_support_1_mid` | number | Midpoint of the nearest confirmed-pivot support zone. |
| `sr_support_1_low` | number | Lower edge of the nearest confirmed-pivot support zone. |
| `sr_support_1_high` | number | Upper edge of the nearest confirmed-pivot support zone. |
| `sr_support_1_touches` | integer | Confirmed pivot touches assigned to the nearest support zone. |
| `sr_support_1_strength` | number | Recency-decayed strength score for the nearest support zone. |
| `sr_support_1_dist_atr` | number | `(close - sr_support_1_mid) / atr_14d`. |
| `sr_resistance_1_mid` | number | Midpoint of the nearest confirmed-pivot resistance zone. |
| `sr_resistance_1_low` | number | Lower edge of the nearest confirmed-pivot resistance zone. |
| `sr_resistance_1_high` | number | Upper edge of the nearest confirmed-pivot resistance zone. |
| `sr_resistance_1_touches` | integer | Confirmed pivot touches assigned to the nearest resistance zone. |
| `sr_resistance_1_strength` | number | Recency-decayed strength score for the nearest resistance zone. |
| `sr_resistance_1_dist_atr` | number | `(sr_resistance_1_mid - close) / atr_14d`. |
| `sr_in_support_1_zone` | binary flag | `1` when close is inside the nearest support zone. |
| `sr_in_resistance_1_zone` | binary flag | `1` when close is inside the nearest resistance zone. |
| `sr_breaks_above_resistance_1` | binary flag | `1` when close moves above the nearest resistance-zone high after the prior close was not above it. |
| `sr_breaks_below_support_1` | binary flag | `1` when close moves below the nearest support-zone low after the prior close was not below it. |
| `sr_zone_position` | number | Relative position of close between the nearest support and resistance mids. |
| `fib_swing_direction` | integer | `1` for an up swing, `-1` for a down swing, `0` when no confirmed opposite-pivot pair exists yet. |
| `fib_anchor_low` | number | Confirmed low anchor of the active Fibonacci swing. |
| `fib_anchor_high` | number | Confirmed high anchor of the active Fibonacci swing. |
| `fib_level_236` | number | 23.6% retracement level of the active swing. |
| `fib_level_382` | number | 38.2% retracement level of the active swing. |
| `fib_level_500` | number | 50.0% retracement level of the active swing. |
| `fib_level_618` | number | 61.8% retracement level of the active swing. |
| `fib_level_786` | number | 78.6% retracement level of the active swing. |
| `fib_nearest_level` | number | Closest Fibonacci retracement level to close for the active swing. |
| `fib_nearest_dist_atr` | number | `(close - fib_nearest_level) / atr_14d`. |
| `fib_in_value_zone` | binary flag | `1` when close sits between the 38.2% and 61.8% retracement levels of the active swing. |

Candle-geometry columns:

| Column | Type | Description |
| --- | --- | --- |
| `range` | number | `high - low`, clipped at zero. |
| `body` | number | Absolute candle body size, `abs(close - open)`. |
| `is_bull` | binary flag | `1` when `close > open`. |
| `is_bear` | binary flag | `1` when `close < open`. |
| `upper_shadow` | number | Distance from candle body top to session high. |
| `lower_shadow` | number | Distance from session low to candle body bottom. |
| `body_to_range` | number | Candle body divided by session range. |
| `upper_to_range` | number | Upper shadow divided by session range. |
| `lower_to_range` | number | Lower shadow divided by session range. |

Candlestick pattern flags:

| Column | Type | Description |
| --- | --- | --- |
| `pat_doji` | binary flag | `1` when a doji pattern is detected. |
| `pat_spinning_top` | binary flag | `1` when a spinning-top pattern is detected. |
| `pat_bullish_marubozu` | binary flag | `1` when a bullish marubozu pattern is detected. |
| `pat_bearish_marubozu` | binary flag | `1` when a bearish marubozu pattern is detected. |
| `pat_star_gap_up` | binary flag | `1` when a star-style gap-up setup is detected. |
| `pat_star_gap_down` | binary flag | `1` when a star-style gap-down setup is detected. |
| `pat_star` | binary flag | `1` when a generic star candle is detected. |
| `pat_hammer` | binary flag | `1` when a hammer pattern is detected. |
| `pat_hanging_man` | binary flag | `1` when a hanging-man pattern is detected. |
| `pat_inverted_hammer` | binary flag | `1` when an inverted-hammer pattern is detected. |
| `pat_shooting_star` | binary flag | `1` when a shooting-star pattern is detected. |
| `pat_dragonfly_doji` | binary flag | `1` when a dragonfly-doji pattern is detected. |
| `pat_gravestone_doji` | binary flag | `1` when a gravestone-doji pattern is detected. |
| `pat_bullish_spinning_top` | binary flag | `1` when a bullish spinning-top context is detected. |
| `pat_bearish_spinning_top` | binary flag | `1` when a bearish spinning-top context is detected. |
| `pat_bullish_engulfing` | binary flag | `1` when a bullish engulfing pattern is detected. |
| `pat_bearish_engulfing` | binary flag | `1` when a bearish engulfing pattern is detected. |
| `pat_bullish_harami` | binary flag | `1` when a bullish harami pattern is detected. |
| `pat_bearish_harami` | binary flag | `1` when a bearish harami pattern is detected. |
| `pat_piercing_line` | binary flag | `1` when a piercing-line pattern is detected. |
| `pat_dark_cloud_line` | binary flag | `1` when a dark-cloud-line pattern is detected. |
| `pat_tweezer_bottom` | binary flag | `1` when a tweezer-bottom pattern is detected. |
| `pat_tweezer_top` | binary flag | `1` when a tweezer-top pattern is detected. |
| `pat_bullish_kicker` | binary flag | `1` when a bullish kicker pattern is detected. |
| `pat_bearish_kicker` | binary flag | `1` when a bearish kicker pattern is detected. |
| `pat_morning_star` | binary flag | `1` when a morning-star pattern is detected. |
| `pat_morning_doji_star` | binary flag | `1` when a morning-doji-star pattern is detected. |
| `pat_evening_star` | binary flag | `1` when an evening-star pattern is detected. |
| `pat_evening_doji_star` | binary flag | `1` when an evening-doji-star pattern is detected. |
| `pat_bullish_abandoned_baby` | binary flag | `1` when a bullish abandoned-baby pattern is detected. |
| `pat_bearish_abandoned_baby` | binary flag | `1` when a bearish abandoned-baby pattern is detected. |
| `pat_three_white_soldiers` | binary flag | `1` when a three-white-soldiers pattern is detected. |
| `pat_three_black_crows` | binary flag | `1` when a three-black-crows pattern is detected. |
| `pat_bullish_three_line_strike` | binary flag | `1` when a bullish three-line-strike pattern is detected. |
| `pat_bearish_three_line_strike` | binary flag | `1` when a bearish three-line-strike pattern is detected. |
| `pat_three_inside_up` | binary flag | `1` when a three-inside-up pattern is detected. |
| `pat_three_outside_up` | binary flag | `1` when a three-outside-up pattern is detected. |
| `pat_three_inside_down` | binary flag | `1` when a three-inside-down pattern is detected. |
| `pat_three_outside_down` | binary flag | `1` when a three-outside-down pattern is detected. |

Heikin-Ashi and Ichimoku columns:

| Column | Type | Description |
| --- | --- | --- |
| `ha_open` | number | Heikin-Ashi open value. |
| `ha_high` | number | Heikin-Ashi high value. |
| `ha_low` | number | Heikin-Ashi low value. |
| `ha_close` | number | Heikin-Ashi close value. |
| `ichimoku_tenkan_sen_9` | number | Ichimoku tenkan-sen (9-period conversion line). |
| `ichimoku_kijun_sen_26` | number | Ichimoku kijun-sen (26-period base line). |
| `ichimoku_senkou_span_a` | number | Ichimoku senkou span A at the row's as-of date. |
| `ichimoku_senkou_span_b` | number | Ichimoku senkou span B at the row's as-of date. |
| `ichimoku_senkou_span_a_26` | number | Senkou span A shifted 26 periods for alignment without look-ahead leakage. |
| `ichimoku_senkou_span_b_26` | number | Senkou span B shifted 26 periods for alignment without look-ahead leakage. |
| `ichimoku_chikou_span_26` | number | Close shifted 26 periods to represent the chikou span. |

Evidence:

- `README.md:3-9`
- `core/pipeline.py:29-37`
- `tasks/market_data/bronze_market_data.py:38-49`
- `tasks/market_data/silver_market_data.py:52-87`
- `tasks/market_data/gold_market_data.py:146-249`
- `tasks/technical_analysis/technical_indicators.py:92-182`
- `tasks/technical_analysis/technical_indicators.py:184-490`

## Finance

### Bronze Finance

Path: `finance-data/buckets/{bucket}`

`report_type` values emitted by the Bronze job currently come from the configured report set: `balance_sheet`, `cash_flow`, `income_statement`, and `overview`. Silver materializes the three Piotroski source subdomains plus a slim `valuation` view sourced from `overview`; Gold merges those inputs into the unified finance feature bucket.

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `report_type` | string | Finance report family stored in `payload_json`. |
| `payload_json` | json string | Compact serialized provider response for the report type. |
| `source_min_date` | string | Earliest report date found in the provider payload, if available. |
| `source_max_date` | string | Latest report date found in the provider payload, if available. |
| `ingested_at` | string | UTC ingestion timestamp for the Bronze row. |
| `payload_hash` | string | Hash of `payload_json` used for change detection. |

### Silver Finance: `balance_sheet`

Path: `finance-data/balance_sheet/buckets/{bucket}`

Rows are extracted from Bronze JSON, reduced to the required Piotroski fields, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `long_term_debt` | number | Long-term debt input for leverage checks. |
| `total_assets` | number | Total assets input for ROA and asset-turnover calculations. |
| `current_assets` | number | Current assets input for liquidity calculations. |
| `current_liabilities` | number | Current liabilities input for liquidity calculations. |
| `shares_outstanding` | number | Shares outstanding input for share-dilution checks. |

### Silver Finance: `income_statement`

Path: `finance-data/income_statement/buckets/{bucket}`

Rows are extracted from Bronze JSON, reduced to the required Piotroski fields, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `total_revenue` | number | Total revenue input for growth and margin calculations. |
| `gross_profit` | number | Gross profit input for gross-margin calculations. |
| `net_income` | number | Net income input for ROA and profitability checks. |

### Silver Finance: `cash_flow`

Path: `finance-data/cash_flow/buckets/{bucket}`

Rows are extracted from Bronze JSON, reduced to the required Piotroski fields, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `operating_cash_flow` | number | Operating cash flow input for cash-generation and accrual checks. |

### Silver Finance: `valuation`

Path: `finance-data/valuation/buckets/{bucket}`

Rows are normalized directly from Bronze Massive `ratios` history, then resampled to daily frequency with forward fill.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `market_cap` | number | Daily market capitalization carried from Massive ratios history. |
| `pe_ratio` | number | Daily trailing P/E carried from Massive ratios history. |
| `price_to_book` | number | Daily price-to-book ratio carried from Massive ratios history. |
| `price_to_sales` | number | Daily price-to-sales ratio carried from Massive ratios history. |
| `price_to_cash_flow` | number | Daily price-to-cash-flow ratio carried from Massive ratios history. |
| `price_to_free_cash_flow` | number | Daily price-to-free-cash-flow ratio carried from Massive ratios history. |
| `dividend_yield` | number | Daily dividend yield carried from Massive ratios history. |
| `return_on_assets` | number | Daily return on assets carried from Massive ratios history. |
| `return_on_equity` | number | Daily return on equity carried from Massive ratios history. |
| `debt_to_equity` | number | Daily debt-to-equity ratio carried from Massive ratios history. |
| `current_ratio` | number | Daily current ratio carried from Massive ratios history. |
| `quick_ratio` | number | Daily quick ratio carried from Massive ratios history. |
| `cash_ratio` | number | Daily cash ratio carried from Massive ratios history. |
| `ev_to_sales` | number | Daily enterprise-value-to-sales ratio carried from Massive ratios history. |
| `ev_to_ebitda` | number | Daily enterprise-value-to-EBITDA ratio carried from Massive ratios history. |
| `enterprise_value` | number | Daily enterprise value carried from Massive ratios history. |
| `earnings_per_share` | number | Daily earnings per share carried from Massive ratios history. |
| `free_cash_flow` | number | Daily free cash flow carried from Massive ratios history. |

### Gold Finance

Path: `finance/buckets/{bucket}`

Gold finance computes a larger feature set internally, then persists the Piotroski output together with the valuation metrics carried from Silver.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date from the merged Silver finance inputs. |
| `symbol` | string | Uppercased ticker symbol. |
| `market_cap` | number | Daily market capitalization carried from Silver valuation. |
| `pe_ratio` | number | Daily trailing P/E carried from Silver valuation. |
| `price_to_book` | number | Daily price-to-book ratio carried from Silver valuation. |
| `price_to_sales` | number | Daily price-to-sales ratio carried from Silver valuation. |
| `price_to_cash_flow` | number | Daily price-to-cash-flow ratio carried from Silver valuation. |
| `price_to_free_cash_flow` | number | Daily price-to-free-cash-flow ratio carried from Silver valuation. |
| `dividend_yield` | number | Daily dividend yield carried from Silver valuation. |
| `return_on_assets` | number | Daily return on assets carried from Silver valuation. |
| `return_on_equity` | number | Daily return on equity carried from Silver valuation. |
| `debt_to_equity` | number | Daily debt-to-equity ratio carried from Silver valuation. |
| `current_ratio` | number | Provider current ratio carried from Silver valuation. |
| `quick_ratio` | number | Daily quick ratio carried from Silver valuation. |
| `cash_ratio` | number | Daily cash ratio carried from Silver valuation. |
| `ev_to_sales` | number | Daily enterprise-value-to-sales ratio carried from Silver valuation. |
| `ev_to_ebitda` | number | Daily enterprise-value-to-EBITDA ratio carried from Silver valuation. |
| `enterprise_value` | number | Daily enterprise value carried from Silver valuation. |
| `earnings_per_share` | number | Daily earnings per share carried from Silver valuation. |
| `free_cash_flow` | number | Daily free cash flow carried from Silver valuation. |
| `piotroski_roa_pos` | nullable int | `1` when trailing-twelve-month ROA is positive. |
| `piotroski_cfo_pos` | nullable int | `1` when trailing-twelve-month operating cash flow is positive. |
| `piotroski_delta_roa_pos` | nullable int | `1` when trailing-twelve-month ROA improved versus four periods earlier. |
| `piotroski_accruals_pos` | nullable int | `1` when operating cash flow exceeds net income on a trailing-twelve-month basis. |
| `piotroski_leverage_decrease` | nullable int | `1` when long-term-debt-to-assets improved versus four periods earlier. |
| `piotroski_liquidity_increase` | nullable int | `1` when current ratio improved versus four periods earlier. |
| `piotroski_no_new_shares` | nullable int | `1` when shares outstanding did not increase versus four periods earlier. |
| `piotroski_gross_margin_increase` | nullable int | `1` when trailing-twelve-month gross margin improved versus four periods earlier. |
| `piotroski_asset_turnover_increase` | nullable int | `1` when trailing-twelve-month asset turnover improved versus four periods earlier. |
| `piotroski_f_score` | nullable int | Sum of the nine Piotroski component flags. |

Evidence:

- `core/pipeline.py:50-67`
- `core/pipeline.py:89`
- `tasks/finance_data/bronze_finance_data.py:42-74`
- `tasks/common/finance_contracts.py:3-132`
- `tasks/finance_data/silver_finance_data.py:242-284`
- `tasks/finance_data/silver_finance_data.py:287-312`
- `tasks/finance_data/silver_finance_data.py:696-745`
- `tasks/finance_data/gold_finance_data.py:95-109`
- `tasks/finance_data/gold_finance_data.py:459-651`

## Earnings

### Bronze Earnings

Path: `earnings-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `date` | datetime | Canonical event date. For actual rows this stays aligned to the historical earnings event date; for scheduled rows it equals `report_date`. |
| `report_date` | datetime | Provider report date when available. |
| `fiscal_date_ending` | datetime | Fiscal quarter end for the earnings event. |
| `reported_eps` | number | Reported earnings per share. |
| `eps_estimate` | number | Consensus EPS estimate. |
| `surprise` | number | Surprise metric normalized from the provider payload. |
| `record_type` | string | `actual` for historical earnings rows, `scheduled` for upcoming calendar rows. |
| `is_future_event` | binary flag | `1` when the row represents an upcoming scheduled earnings event. |
| `calendar_time_of_day` | string | Provider time-of-day hint for scheduled events, such as `pre-market` or `post-market`. |
| `calendar_currency` | string | Currency code supplied on scheduled earnings rows. |
| `ingested_at` | string | UTC ingestion timestamp for the Bronze row. |
| `source_hash` | string | Hash of the normalized Bronze earnings payload. |

### Silver Earnings

Path: `earnings-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Canonical earnings date. |
| `symbol` | string | Uppercased ticker symbol. |
| `report_date` | datetime | Provider report date when available. |
| `fiscal_date_ending` | datetime | Fiscal quarter end for the earnings event. |
| `reported_eps` | number | Reported earnings per share. |
| `eps_estimate` | number | Consensus EPS estimate. |
| `surprise` | number | Surprise metric carried from Bronze. |
| `record_type` | string | `actual` for historical earnings rows, `scheduled` for upcoming calendar rows. |
| `is_future_event` | binary flag | `1` when the row represents an upcoming scheduled earnings event. |
| `calendar_time_of_day` | string | Provider time-of-day hint for scheduled events. |
| `calendar_currency` | string | Currency code supplied on scheduled earnings rows. |

### Gold Earnings

Path: `earnings/buckets/{bucket}`

Gold earnings expands sparse quarterly observations into a daily forward-filled feature table. Rolling surprise metrics are computed from actual earnings rows only, while upcoming-earnings fields are derived from scheduled calendar rows. When scheduled rows exist, the daily frame extends through the next scheduled report date.

| Column | Type | Description |
| --- | --- | --- |
| `date` | datetime | Daily as-of date after expansion. |
| `symbol` | string | Uppercased ticker symbol. |
| `reported_eps` | number | Last reported EPS value carried forward from the most recent earnings event. |
| `eps_estimate` | number | Last EPS estimate carried forward from the most recent earnings event. |
| `surprise` | number | Last raw surprise value carried forward from the most recent earnings event. |
| `surprise_pct` | number | `(reported_eps - eps_estimate) / abs(eps_estimate)`. |
| `surprise_mean_4q` | number | Rolling four-quarter mean of `surprise_pct`. |
| `surprise_std_8q` | number | Rolling eight-quarter standard deviation of `surprise_pct`. |
| `beat_rate_8q` | number | Rolling eight-quarter share of positive `surprise_pct` values. |
| `is_earnings_day` | binary flag | `1` on rows representing the actual earnings event date, else `0`. |
| `last_earnings_date` | datetime | Most recent earnings date carried forward to each daily row. |
| `days_since_earnings` | number | Integer day difference between `date` and `last_earnings_date`. |
| `next_earnings_date` | datetime | Next scheduled earnings report date on or after the row’s `date`. |
| `days_until_next_earnings` | number | Integer day difference between `date` and `next_earnings_date`. |
| `next_earnings_estimate` | number | Consensus EPS estimate for the next scheduled earnings event. |
| `next_earnings_time_of_day` | string | Provider time-of-day hint for the next scheduled earnings event. |
| `next_earnings_fiscal_date_ending` | datetime | Fiscal quarter end for the next scheduled earnings event. |
| `has_upcoming_earnings` | binary flag | `1` when a scheduled earnings event exists on or after the row’s `date`. |
| `is_scheduled_earnings_day` | binary flag | `1` on rows representing a scheduled earnings report date, else `0`. |

Evidence:

- `core/pipeline.py:46`
- `core/pipeline.py:83-86`
- `tasks/earnings_data/bronze_earnings_data.py:44-52`
- `tasks/earnings_data/bronze_earnings_data.py:216-230`
- `tasks/earnings_data/silver_earnings_data.py:47-52`
- `tasks/earnings_data/silver_earnings_data.py:195-223`
- `tasks/earnings_data/gold_earnings_data.py:100-149`

## Price Target

### Bronze Price Target

Path: `price-target-data/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `symbol` | string | Uppercased ticker symbol. |
| `obs_date` | datetime | Observation date for the target snapshot. |
| `tp_mean_est` | number | Mean analyst price target. |
| `tp_std_dev_est` | number | Standard deviation of analyst price targets. |
| `tp_high_est` | number | Highest analyst price target. |
| `tp_low_est` | number | Lowest analyst price target. |
| `tp_cnt_est` | number | Count of contributing analyst estimates. |
| `tp_cnt_est_rev_up` | number | Count of upward target revisions. |
| `tp_cnt_est_rev_down` | number | Count of downward target revisions. |
| `ingested_at` | string | UTC ingestion timestamp for the Bronze row. |
| `source_hash` | string | Hash of the normalized Bronze target payload. |

### Silver Price Target

Path: `price-target-data/buckets/{bucket}`

Silver price-target data is reindexed to a daily series and forward-filled so every stored row has the canonical target columns.

| Column | Type | Description |
| --- | --- | --- |
| `obs_date` | datetime | Daily observation date after forward fill. |
| `symbol` | string | Uppercased ticker symbol. |
| `tp_mean_est` | number | Mean analyst price target. |
| `tp_std_dev_est` | number | Standard deviation of analyst price targets. |
| `tp_high_est` | number | Highest analyst price target. |
| `tp_low_est` | number | Lowest analyst price target. |
| `tp_cnt_est` | number | Count of contributing analyst estimates. |
| `tp_cnt_est_rev_up` | number | Count of upward target revisions. |
| `tp_cnt_est_rev_down` | number | Count of downward target revisions. |

### Gold Price Target

Path: `targets/buckets/{bucket}`

| Column | Type | Description |
| --- | --- | --- |
| `obs_date` | datetime | Daily observation date. |
| `symbol` | string | Uppercased ticker symbol. |
| `tp_mean_est` | number | Mean analyst price target carried from Silver. |
| `tp_std_dev_est` | number | Standard deviation of analyst targets carried from Silver. |
| `tp_high_est` | number | Highest analyst target carried from Silver. |
| `tp_low_est` | number | Lowest analyst target carried from Silver. |
| `tp_cnt_est` | number | Count of contributing analyst estimates. |
| `tp_cnt_est_rev_up` | number | Count of upward target revisions. |
| `tp_cnt_est_rev_down` | number | Count of downward target revisions. |
| `disp_abs` | number | Absolute target dispersion, `tp_high_est - tp_low_est`. |
| `disp_norm` | number | Target dispersion normalized by `tp_mean_est`. |
| `disp_std_norm` | number | Target standard deviation normalized by `tp_mean_est`. |
| `rev_net` | number | Net revisions, `tp_cnt_est_rev_up - tp_cnt_est_rev_down`. |
| `rev_ratio` | number | Revision ratio, `(tp_cnt_est_rev_up + 1) / (tp_cnt_est_rev_down + 1)`. |
| `rev_intensity` | number | Net revisions normalized by `tp_cnt_est`. |
| `disp_norm_change_30d` | number | Thirty-day change in normalized dispersion. |
| `tp_mean_change_30d` | number | Thirty-day change in mean target. |
| `disp_z` | number | 252-day z-score of normalized dispersion. |
| `tp_mean_slope_90d` | number | Ninety-day rolling slope of `tp_mean_est`. |

Evidence:

- `core/pipeline.py:75-79`
- `tasks/price_target_data/bronze_price_target_data.py:33-44`
- `tasks/price_target_data/bronze_price_target_data.py:148-169`
- `tasks/price_target_data/silver_price_target_data.py:45-55`
- `tasks/price_target_data/silver_price_target_data.py:111-183`
- `tasks/price_target_data/silver_price_target_data.py:263-306`
- `tasks/price_target_data/gold_price_target_data.py:125-186`
