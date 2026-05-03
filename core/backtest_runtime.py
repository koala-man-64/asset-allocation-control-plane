from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd
from asset_allocation_runtime_common import BACKTEST_RESULTS_SCHEMA_VERSION, persist_backtest_results

from core.backtest_repository import BacktestRepository
from asset_allocation_runtime_common.foundation.postgres import connect
from core.ranking_engine import service as ranking_service
from core.ranking_engine.contracts import RankingSchemaConfig
from asset_allocation_runtime_common.domain.regime import DEFAULT_REGIME_MODEL_NAME, RegimePolicy
from core.regime_repository import RegimeRepository
from core.ranking_repository import RankingRepository
from core.strategy_engine import StrategyConfig, UniverseDefinition
from core.strategy_engine.exit_rules import ExitRuleEvaluator
from core.strategy_engine.position_state import PositionState, PriceBar
from core.strategy_engine import universe as universe_service
from core.strategy_repository import StrategyRepository, normalize_strategy_config_document
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

_PRICE_TABLE = "market_data"
_PRICE_COLUMNS = {"open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class ResolvedBacktestDefinition:
    strategy_name: str | None
    strategy_version: int | None
    strategy_config: StrategyConfig
    strategy_config_raw: dict[str, Any]
    strategy_universe: UniverseDefinition
    ranking_schema_name: str
    ranking_schema_version: int | None
    ranking_schema: RankingSchemaConfig
    ranking_universe_name: str | None
    ranking_universe_version: int | None
    ranking_universe: UniverseDefinition
    regime_model_name: str | None = None
    regime_model_version: int | None = None
    regime_model_config: dict[str, Any] | None = None


@dataclass
class PolicyRuntimeState:
    last_rebalance_signal_index: int | None = None
    last_rebalance_signal_ts: datetime | None = None
    manual_rebalance_skip_emitted: bool = False
    position_cooldown_until: dict[str, int] = field(default_factory=dict)
    position_approval_required: set[str] = field(default_factory=set)
    position_approval_emitted: set[str] = field(default_factory=set)
    strategy_nav_baseline: float | None = None
    strategy_nav_peak: float | None = None
    strategy_cooldown_until_index: int | None = None
    strategy_approval_required: bool = False
    strategy_approval_emitted: bool = False
    strategy_exposure_multiplier: float = 1.0
    strategy_freeze_buys: bool = False


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _session_bounds(ts: datetime) -> tuple[datetime, datetime]:
    start = datetime.combine(ts.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine(ts.date(), time.max, tzinfo=timezone.utc)
    return start, end


def _normalize_timestamp_value(value: Any, *, kind: str) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Unable to normalize timestamp value: {value!r}")
    if kind == "slower":
        parsed = parsed.normalize()
    return parsed.to_pydatetime()


def _bounds_for_spec(spec: universe_service.UniverseTableSpec, start_ts: datetime, end_ts: datetime) -> tuple[Any, Any]:
    if spec.as_of_kind == "intraday":
        return start_ts, end_ts
    return start_ts.date(), end_ts.date()


def _load_run_schedule(
    dsn: str,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> list[datetime]:
    start_bound, end_bound = _bounds_for_spec(table_spec, start_ts, end_ts)
    sql = f"""
        SELECT DISTINCT {universe_service._quote_identifier(table_spec.as_of_column)} AS as_of_value
        FROM "gold".{universe_service._quote_identifier(table_name)}
        WHERE {universe_service._quote_identifier(table_spec.as_of_column)} >= %s
          AND {universe_service._quote_identifier(table_spec.as_of_column)} <= %s
    """
    params: list[Any] = [start_bound, end_bound]
    if bar_size and "bar_size" in table_spec.columns:
        sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
        params.append(bar_size)
    sql += " ORDER BY as_of_value"

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [_normalize_timestamp_value(row[0], kind=table_spec.as_of_kind) for row in rows if row and row[0] is not None]


def _load_exact_coverage(
    dsn: str,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> set[datetime]:
    return set(
        _load_run_schedule(
            dsn,
            table_name=table_name,
            table_spec=table_spec,
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=bar_size,
        )
    )


def _value_series(
    raw: pd.Series,
    *,
    column_spec: universe_service.UniverseColumnSpec,
) -> pd.Series:
    if column_spec.value_kind == "number":
        return pd.to_numeric(raw, errors="coerce")
    if column_spec.value_kind == "boolean":
        return raw.astype("boolean")
    if column_spec.value_kind in {"date", "datetime"}:
        return pd.to_datetime(raw, utc=True, errors="coerce")
    return raw.astype("string")


def _prepare_loaded_frame(
    frame: pd.DataFrame,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    selected_columns: Iterable[str],
) -> pd.DataFrame:
    normalized_columns = list(selected_columns)
    if frame.empty:
        return pd.DataFrame(columns=["as_of", "symbol", *[f"{table_name}__{name}" for name in normalized_columns]])
    out = frame.copy()
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()
    out["as_of"] = pd.to_datetime(out["as_of"], utc=True, errors="coerce")
    for column_name in normalized_columns:
        out[f"{table_name}__{column_name}"] = _value_series(out[column_name], column_spec=table_spec.columns[column_name])
    return out[["as_of", "symbol", *[f"{table_name}__{name}" for name in normalized_columns]]]


def _load_intraday_session_frames(
    dsn: str,
    *,
    table_specs: dict[str, universe_service.UniverseTableSpec],
    required_columns: dict[str, set[str]],
    session_start: datetime,
    session_end: datetime,
    bar_size: str | None,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs[table_name]
            if spec.as_of_kind != "intraday":
                continue
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS as_of",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            sql = f"""
                SELECT {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} >= %s
                  AND {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            params: list[Any] = [session_start, session_end]
            if bar_size and "bar_size" in spec.columns:
                sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
                params.append(bar_size)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            frames[table_name] = _prepare_loaded_frame(
                frame,
                table_name=table_name,
                table_spec=spec,
                selected_columns=selected_columns,
            )
    return frames


def _load_slow_frames(
    dsn: str,
    *,
    table_specs: dict[str, universe_service.UniverseTableSpec],
    required_columns: dict[str, set[str]],
    as_of_ts: datetime,
    bar_size: str | None,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs[table_name]
            if spec.as_of_kind == "intraday":
                continue
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS as_of",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            sql = f"""
                SELECT DISTINCT ON ({universe_service._quote_identifier('symbol')})
                    {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            params: list[Any] = [as_of_ts.date()]
            if bar_size and "bar_size" in spec.columns:
                sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
                params.append(bar_size)
            sql += f"""
                ORDER BY
                    {universe_service._quote_identifier('symbol')},
                    {universe_service._quote_identifier(spec.as_of_column)} DESC NULLS LAST
            """
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            frames[table_name] = _prepare_loaded_frame(
                frame,
                table_name=table_name,
                table_spec=spec,
                selected_columns=selected_columns,
            )
    return frames


def _snapshot_for_timestamp(
    ts: datetime,
    *,
    intraday_frames: dict[str, pd.DataFrame],
    slow_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for frame in intraday_frames.values():
        if frame.empty:
            continue
        exact = frame[frame["as_of"] == pd.Timestamp(ts)]
        if exact.empty:
            continue
        frames.append(exact.drop(columns=["as_of"], errors="ignore"))
    for frame in slow_frames.values():
        if frame.empty:
            continue
        frames.append(frame.drop(columns=["as_of"], errors="ignore"))

    merged: pd.DataFrame | None = None
    for frame in frames:
        frame = frame.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
        if merged is None:
            merged = frame.copy()
        else:
            merged = merged.merge(frame, on="symbol", how="outer")
    if merged is None:
        return pd.DataFrame(columns=["date", "symbol"])
    merged["date"] = pd.Timestamp(ts)
    merged = merged.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    return merged


def _resolve_strategy_universe(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    fallback_universe: UniverseDefinition,
) -> UniverseDefinition:
    if strategy_config.universe is not None:
        return strategy_config.universe
    if strategy_config.universeConfigName:
        record = UniverseRepository(dsn).get_universe_config(strategy_config.universeConfigName)
        if record:
            return UniverseDefinition.model_validate(record.get("config") or {})
    return fallback_universe


def resolve_backtest_definition(
    dsn: str,
    *,
    strategy_name: str,
    strategy_version: int | None = None,
    regime_model_name: str | None = None,
    regime_model_version: int | None = None,
) -> ResolvedBacktestDefinition:
    strategy_repo = StrategyRepository(dsn)

    strategy_revision = strategy_repo.get_strategy_revision(strategy_name, strategy_version)
    if strategy_revision:
        strategy_config_raw = dict(strategy_revision.get("config") or {})
    else:
        strategy_record = strategy_repo.get_strategy(strategy_name)
        if not strategy_record:
            raise ValueError(f"Strategy '{strategy_name}' not found.")
        strategy_config_raw = dict(strategy_record.get("config") or {})

    return resolve_backtest_definition_from_config(
        dsn,
        strategy_name=strategy_name,
        strategy_version=(int(strategy_revision["version"]) if strategy_revision else None),
        strategy_config_raw=strategy_config_raw,
        ranking_schema_name=(strategy_revision or {}).get("ranking_schema_name"),
        ranking_schema_version=(
            int(strategy_revision["ranking_schema_version"])
            if strategy_revision and strategy_revision.get("ranking_schema_version") is not None
            else None
        ),
        universe_name=(strategy_revision or {}).get("universe_name"),
        universe_version=(
            int(strategy_revision["universe_version"])
            if strategy_revision and strategy_revision.get("universe_version") is not None
            else None
        ),
        regime_model_name=regime_model_name,
        regime_model_version=regime_model_version,
    )


def resolve_backtest_definition_from_config(
    dsn: str,
    *,
    strategy_config_raw: dict[str, Any],
    strategy_name: str | None = None,
    strategy_version: int | None = None,
    ranking_schema_name: str | None = None,
    ranking_schema_version: int | None = None,
    universe_name: str | None = None,
    universe_version: int | None = None,
    regime_model_name: str | None = None,
    regime_model_version: int | None = None,
) -> ResolvedBacktestDefinition:
    ranking_repo = RankingRepository(dsn)
    universe_repo = UniverseRepository(dsn)

    normalized_strategy_config = normalize_strategy_config_document(strategy_config_raw)
    strategy_config = StrategyConfig.model_validate(normalized_strategy_config)
    resolved_ranking_schema_name = str(ranking_schema_name or strategy_config.rankingSchemaName or "").strip()
    strategy_subject = f"Strategy '{strategy_name}'" if strategy_name else "Strategy config"
    if not resolved_ranking_schema_name:
        raise ValueError(f"{strategy_subject} does not reference a ranking schema.")

    ranking_record = ranking_repo.get_ranking_schema_revision(resolved_ranking_schema_name, ranking_schema_version)
    if not ranking_record:
        raise ValueError(f"Ranking schema '{resolved_ranking_schema_name}' not found.")
    ranking_schema = RankingSchemaConfig.model_validate(ranking_record.get("config") or {})

    resolved_universe_name = str(
        universe_name
        or ranking_record.get("config", {}).get("universeConfigName")
        or ranking_schema.universeConfigName
        or ""
    ).strip() or None
    if not resolved_universe_name:
        raise ValueError(f"Ranking schema '{resolved_ranking_schema_name}' does not reference a universe config.")

    universe_record = universe_repo.get_universe_config_revision(resolved_universe_name, universe_version)
    if not universe_record:
        raise ValueError(f"Universe config '{resolved_universe_name}' not found.")

    ranking_universe = UniverseDefinition.model_validate(universe_record.get("config") or {})
    strategy_universe = _resolve_strategy_universe(
        dsn,
        strategy_config=strategy_config,
        fallback_universe=ranking_universe,
    )
    resolved_regime_name, resolved_regime_version, resolved_regime_config = _resolve_regime_revision(
        dsn,
        strategy_config=strategy_config,
        regime_model_name=regime_model_name,
        regime_model_version=regime_model_version,
    )
    return ResolvedBacktestDefinition(
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        strategy_config=strategy_config,
        strategy_config_raw=normalized_strategy_config,
        strategy_universe=strategy_universe,
        ranking_schema_name=resolved_ranking_schema_name,
        ranking_schema_version=int(ranking_record["version"]),
        ranking_schema=ranking_schema,
        ranking_universe_name=resolved_universe_name,
        ranking_universe_version=int(universe_record["version"]),
        ranking_universe=ranking_universe,
        regime_model_name=resolved_regime_name,
        regime_model_version=resolved_regime_version,
        regime_model_config=resolved_regime_config,
    )


def _required_columns(definition: ResolvedBacktestDefinition) -> dict[str, set[str]]:
    required = ranking_service._collect_required_columns(
        definition.strategy_universe,
        definition.ranking_universe,
        definition.ranking_schema,
    )
    required.setdefault(_PRICE_TABLE, set()).update(_PRICE_COLUMNS)
    for rule in definition.strategy_config.exits:
        if rule.atrColumn:
            required[_PRICE_TABLE].add(str(rule.atrColumn))
    return required


def validate_backtest_submission(
    dsn: str,
    *,
    definition: ResolvedBacktestDefinition,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> list[datetime]:
    table_specs = universe_service._load_gold_table_specs(dsn)
    required = _required_columns(definition)
    missing_tables = [name for name in required if name not in table_specs]
    if missing_tables:
        raise ValueError(f"Missing required gold tables: {sorted(missing_tables)}")

    price_spec = table_specs[_PRICE_TABLE]
    intraday_tables = sorted(name for name, spec in table_specs.items() if name in required and spec.as_of_kind == "intraday")
    schedule_source_name = _PRICE_TABLE if price_spec.as_of_kind == "intraday" else (intraday_tables[0] if intraday_tables else _PRICE_TABLE)
    schedule_source = table_specs[schedule_source_name]
    schedule = _load_run_schedule(
        dsn,
        table_name=schedule_source_name,
        table_spec=schedule_source,
        start_ts=start_ts,
        end_ts=end_ts,
        bar_size=bar_size,
    )
    if len(schedule) < 2:
        raise ValueError("Backtest window must resolve to at least two bars.")
    if intraday_tables and price_spec.as_of_kind != "intraday":
        raise ValueError(
            "Execution price table 'market_data' is not intraday while intraday feature tables are required."
        )

    schedule_set = set(schedule)
    for table_name in intraday_tables:
        coverage = _load_exact_coverage(
            dsn,
            table_name=table_name,
            table_spec=table_specs[table_name],
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=bar_size,
        )
        missing = sorted(schedule_set.difference(coverage))
        if missing:
            sample = ", ".join(item.isoformat() for item in missing[:5])
            raise ValueError(
                f"Intraday feature coverage gap for gold.{table_name}; missing {len(missing)} rebalance bars, sample={sample}"
            )
    if definition.regime_model_name and definition.regime_model_version is not None:
        _validate_regime_history_coverage(
            dsn,
            model_name=definition.regime_model_name,
            model_version=definition.regime_model_version,
            schedule=schedule,
        )
    return schedule


def _score_snapshot(
    snapshot: pd.DataFrame,
    *,
    definition: ResolvedBacktestDefinition,
    rebalance_ts: datetime,
    target_weight_multiplier: float = 1.0,
) -> pd.DataFrame:
    if snapshot.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])
    filtered = snapshot[
        ranking_service._evaluate_universe_mask(snapshot, definition.strategy_universe.root)
        & ranking_service._evaluate_universe_mask(snapshot, definition.ranking_universe.root)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    group_scores: list[tuple[str, float, pd.Series]] = []
    required_masks: list[pd.Series] = []
    for group in definition.ranking_schema.groups:
        group_series, group_required_masks = ranking_service._score_group(filtered, group)
        group_scores.append((group.name, group.weight, group_series))
        required_masks.extend(group_required_masks)

    if required_masks:
        keep_mask = pd.concat(required_masks, axis=1).all(axis=1)
        filtered = filtered[keep_mask].copy()
        group_scores = [(name, weight, series.loc[filtered.index]) for name, weight, series in group_scores]
        if filtered.empty:
            return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    weighted_total = pd.Series(0.0, index=filtered.index)
    total_weight = 0.0
    for _name, weight, series in group_scores:
        weighted_total = weighted_total.add(series * weight, fill_value=0.0)
        total_weight += weight
    if total_weight <= 0:
        raise ValueError("Ranking schema produced zero total group weight.")
    filtered["score"] = weighted_total / total_weight
    filtered["score"] = ranking_service._apply_transforms(
        filtered["score"],
        filtered["date"],
        definition.ranking_schema.overallTransforms,
    )
    filtered = filtered.dropna(subset=["score"]).copy()
    if filtered.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    filtered = filtered.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    filtered["ordinal"] = np.arange(1, len(filtered) + 1)
    top_n = min(definition.strategy_config.topN, len(filtered))
    filtered["selected"] = filtered["ordinal"] <= top_n
    target_weight = float(target_weight_multiplier) / top_n if top_n > 0 else 0.0
    filtered["target_weight"] = np.where(filtered["selected"], target_weight, 0.0)
    filtered["rebalance_ts"] = pd.Timestamp(rebalance_ts)
    return filtered[["rebalance_ts", "symbol", "score", "ordinal", "selected", "target_weight"]]


def _market_row(snapshot: pd.DataFrame, symbol: str) -> pd.Series | None:
    matches = snapshot[snapshot["symbol"] == symbol]
    if matches.empty:
        return None
    return matches.iloc[0]


def _price_bar(ts: datetime, row: pd.Series) -> PriceBar:
    features = {
        column.removeprefix(f"{_PRICE_TABLE}__"): row[column]
        for column in row.index
        if str(column).startswith(f"{_PRICE_TABLE}__")
    }
    return PriceBar(
        date=ts,
        open=_maybe_float(features.get("open")),
        high=_maybe_float(features.get("high")),
        low=_maybe_float(features.get("low")),
        close=_maybe_float(features.get("close")),
        features=features,
    )


def _maybe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return float(int(value))
    try:
        return float(value)
    except Exception:
        return None


def _record_policy_event(
    policy_event_rows: list[dict[str, Any]],
    *,
    ts: datetime,
    scope: str,
    policy_type: str,
    decision: str,
    reason_code: str,
    symbol: str | None = None,
    position_id: str | None = None,
    policy_id: str | None = None,
    observed_value: float | None = None,
    threshold_value: float | None = None,
    action: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    policy_event_rows.append(
        {
            "event_seq": len(policy_event_rows) + 1,
            "bar_ts": ts.isoformat(),
            "scope": scope,
            "policy_type": policy_type,
            "decision": decision,
            "reason_code": reason_code,
            "symbol": symbol,
            "position_id": position_id,
            "policy_id": policy_id,
            "observed_value": observed_value,
            "threshold_value": threshold_value,
            "action": action,
            "details": details or {},
        }
    )


def _rebalance_policy_allows_signal(
    policy: Any,
    *,
    current_ts: datetime,
    bar_index: int,
    state: PolicyRuntimeState,
    policy_event_rows: list[dict[str, Any]],
) -> bool:
    if policy is None:
        return True

    frequency = str(policy.frequency)
    if frequency == "manual":
        if not state.manual_rebalance_skip_emitted:
            _record_policy_event(
                policy_event_rows,
                ts=current_ts,
                scope="strategy",
                policy_type="rebalance",
                decision="skipped",
                reason_code="manual_mode",
                action="none",
            )
            state.manual_rebalance_skip_emitted = True
        return False

    last_index = state.last_rebalance_signal_index
    last_ts = state.last_rebalance_signal_ts
    if last_index is None or last_ts is None:
        return True
    if frequency == "every_bar":
        return True
    if frequency == "daily":
        return current_ts.date() != last_ts.date()
    if frequency == "weekly":
        return current_ts.isocalendar()[:2] != last_ts.isocalendar()[:2]
    if frequency == "monthly":
        return (current_ts.year, current_ts.month) != (last_ts.year, last_ts.month)
    if frequency == "quarterly":
        current_quarter = (current_ts.month - 1) // 3
        last_quarter = (last_ts.month - 1) // 3
        return (current_ts.year, current_quarter) != (last_ts.year, last_quarter)
    if frequency == "every_n_bars":
        return bar_index - last_index >= int(policy.intervalBars or 1)
    return False


def _position_price(
    snapshot: pd.DataFrame,
    symbol: str,
    position: PositionState,
    previous_close_by_symbol: dict[str, float],
    *,
    prefer_open: bool = False,
) -> float:
    row = _market_row(snapshot, symbol)
    if row is None:
        return float(previous_close_by_symbol.get(symbol, position.entry_price))
    primary_column = "open" if prefer_open else "close"
    fallback_column = "close" if prefer_open else "open"
    price = _maybe_float(row.get(f"{_PRICE_TABLE}__{primary_column}")) or _maybe_float(
        row.get(f"{_PRICE_TABLE}__{fallback_column}")
    )
    if price is None or price <= 0:
        price = previous_close_by_symbol.get(symbol, position.entry_price)
    return float(price)


def _current_position_weights(
    positions: dict[str, PositionState],
    *,
    snapshot: pd.DataFrame,
    previous_close_by_symbol: dict[str, float],
    equity: float,
) -> dict[str, float]:
    if equity <= 0:
        return {}
    return {
        symbol: float(position.quantity * _position_price(snapshot, symbol, position, previous_close_by_symbol) / equity)
        for symbol, position in positions.items()
    }


def _strategy_risk_active(state: PolicyRuntimeState, *, bar_index: int) -> bool:
    if state.strategy_approval_required:
        return True
    return state.strategy_cooldown_until_index is not None and bar_index <= state.strategy_cooldown_until_index


def _active_strategy_exposure_multiplier(
    state: PolicyRuntimeState,
    *,
    policy: Any,
    bar_index: int,
    current_ts: datetime,
    policy_event_rows: list[dict[str, Any]],
) -> float:
    if policy is None or not bool(policy.enabled):
        return 1.0
    if state.strategy_approval_required:
        if (
            state.strategy_cooldown_until_index is not None
            and bar_index > state.strategy_cooldown_until_index
            and not state.strategy_approval_emitted
        ):
            _record_policy_event(
                policy_event_rows,
                ts=current_ts,
                scope="strategy",
                policy_type="reentry",
                decision="blocked",
                reason_code="approval_required",
                action="hold_reduced_exposure",
            )
            state.strategy_approval_emitted = True
        return float(state.strategy_exposure_multiplier)
    if state.strategy_cooldown_until_index is not None and bar_index > state.strategy_cooldown_until_index:
        state.strategy_cooldown_until_index = None
        state.strategy_exposure_multiplier = 1.0
        state.strategy_freeze_buys = False
    return float(state.strategy_exposure_multiplier)


def _filter_targets_for_position_reentry(
    target_weights: dict[str, float],
    *,
    state: PolicyRuntimeState,
    policy: Any,
    current_ts: datetime,
    bar_index: int,
    policy_event_rows: list[dict[str, Any]],
) -> dict[str, float]:
    if policy is None or not bool(policy.enabled):
        return target_weights

    filtered: dict[str, float] = {}
    for symbol, target_weight in target_weights.items():
        approval_required = symbol in state.position_approval_required
        cooldown_until = state.position_cooldown_until.get(symbol)
        cooldown_active = cooldown_until is not None and bar_index <= cooldown_until
        if approval_required and not cooldown_active:
            if symbol not in state.position_approval_emitted:
                _record_policy_event(
                    policy_event_rows,
                    ts=current_ts,
                    scope="position",
                    policy_type="reentry",
                    decision="blocked",
                    reason_code="approval_required",
                    symbol=symbol,
                    action="block_reentry",
                )
                state.position_approval_emitted.add(symbol)
            continue
        if cooldown_active:
            _record_policy_event(
                policy_event_rows,
                ts=current_ts,
                scope="position",
                policy_type="reentry",
                decision="blocked",
                reason_code="cooldown",
                symbol=symbol,
                observed_value=float(max(0, int(cooldown_until) - bar_index)),
                action="block_reentry",
            )
            continue
        if cooldown_until is not None and bar_index > cooldown_until:
            state.position_cooldown_until.pop(symbol, None)
            state.position_approval_emitted.discard(symbol)
        filtered[symbol] = target_weight
    return filtered


def _schedule_rebalance_targets(
    ranking_records: list[dict[str, Any]],
    *,
    policy: Any,
    risk_policy: Any,
    state: PolicyRuntimeState,
    positions: dict[str, PositionState],
    snapshot: pd.DataFrame,
    previous_close_by_symbol: dict[str, float],
    close_equity: float,
    current_ts: datetime,
    bar_index: int,
    policy_event_rows: list[dict[str, Any]],
) -> dict[str, float] | None:
    if not _rebalance_policy_allows_signal(
        policy,
        current_ts=current_ts,
        bar_index=bar_index,
        state=state,
        policy_event_rows=policy_event_rows,
    ):
        return None

    state.last_rebalance_signal_index = bar_index
    state.last_rebalance_signal_ts = current_ts

    target_weights = {
        str(row["symbol"]): float(row["target_weight"])
        for row in ranking_records
        if bool(row.get("selected"))
    }
    current_weights = _current_position_weights(
        positions,
        snapshot=snapshot,
        previous_close_by_symbol=previous_close_by_symbol,
        equity=close_equity,
    )

    if policy is not None:
        cash_buffer_multiplier = max(0.0, 1.0 - float(policy.cashBufferPct or 0.0) / 100.0)
        target_weights = {symbol: weight * cash_buffer_multiplier for symbol, weight in target_weights.items()}

    target_weights = _filter_targets_for_position_reentry(
        target_weights,
        state=state,
        policy=risk_policy,
        current_ts=current_ts,
        bar_index=bar_index,
        policy_event_rows=policy_event_rows,
    )

    if risk_policy is not None and bool(risk_policy.enabled) and state.strategy_freeze_buys:
        target_weights = {
            symbol: min(weight, current_weights.get(symbol, 0.0))
            for symbol, weight in target_weights.items()
            if symbol in current_weights
        }

    all_weight_symbols = set(current_weights) | set(target_weights)
    max_drift_pct = max(
        (abs(target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)) * 100.0 for symbol in all_weight_symbols),
        default=0.0,
    )
    if policy is not None and policy.driftThresholdPct is not None and max_drift_pct < float(policy.driftThresholdPct):
        _record_policy_event(
            policy_event_rows,
            ts=current_ts,
            scope="strategy",
            policy_type="rebalance",
            decision="skipped",
            reason_code="drift_below_threshold",
            observed_value=float(max_drift_pct),
            threshold_value=float(policy.driftThresholdPct),
            action="none",
        )
        return None

    turnover_pct = sum(
        abs(target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)) for symbol in all_weight_symbols
    ) * 100.0
    if policy is not None and policy.maxTurnoverPct is not None and turnover_pct > float(policy.maxTurnoverPct):
        cap_pct = float(policy.maxTurnoverPct)
        if not bool(policy.allowPartialRebalance):
            _record_policy_event(
                policy_event_rows,
                ts=current_ts,
                scope="strategy",
                policy_type="rebalance",
                decision="skipped",
                reason_code="turnover_cap",
                observed_value=float(turnover_pct),
                threshold_value=cap_pct,
                action="none",
            )
            return None
        scale = cap_pct / turnover_pct if turnover_pct > 0 else 1.0
        target_weights = {
            symbol: current_weights.get(symbol, 0.0)
            + (target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)) * scale
            for symbol in all_weight_symbols
        }
        _record_policy_event(
            policy_event_rows,
            ts=current_ts,
            scope="strategy",
            policy_type="rebalance",
            decision="applied",
            reason_code="turnover_cap",
            observed_value=float(turnover_pct),
            threshold_value=cap_pct,
            action="partial_rebalance",
            details={"scale": float(scale)},
        )
    elif policy is not None:
        _record_policy_event(
            policy_event_rows,
            ts=current_ts,
            scope="strategy",
            policy_type="rebalance",
            decision="applied",
            reason_code="scheduled",
            observed_value=float(max_drift_pct),
            action="rebalance",
            details={"frequency": str(policy.frequency)},
        )

    return target_weights


def _apply_strategy_risk_policy(
    policy: Any,
    *,
    state: PolicyRuntimeState,
    close_equity: float,
    current_ts: datetime,
    bar_index: int,
    policy_event_rows: list[dict[str, Any]],
) -> None:
    if policy is None or not bool(policy.enabled) or close_equity <= 0:
        return
    if state.strategy_nav_baseline is None:
        state.strategy_nav_baseline = close_equity
    if state.strategy_nav_peak is None:
        state.strategy_nav_peak = close_equity
    state.strategy_nav_peak = max(float(state.strategy_nav_peak), close_equity)

    if _strategy_risk_active(state, bar_index=bar_index):
        return

    stop_loss = policy.stopLoss
    if stop_loss is not None and bool(stop_loss.enabled) and state.strategy_nav_peak > 0:
        drawdown_pct = (float(state.strategy_nav_peak) - close_equity) / float(state.strategy_nav_peak) * 100.0
        if drawdown_pct >= float(stop_loss.thresholdPct):
            if stop_loss.action == "liquidate":
                state.strategy_exposure_multiplier = 0.0
            elif stop_loss.action == "freeze_buys":
                state.strategy_freeze_buys = True
            else:
                reduction = float(stop_loss.reductionPct or 0.0) / 100.0
                state.strategy_exposure_multiplier = max(0.0, state.strategy_exposure_multiplier * (1.0 - reduction))
            state.strategy_cooldown_until_index = bar_index + int(policy.reentry.cooldownBars or 0)
            state.strategy_approval_required = bool(policy.reentry.requireApproval)
            state.strategy_approval_emitted = False
            state.strategy_nav_baseline = close_equity
            state.strategy_nav_peak = close_equity
            _record_policy_event(
                policy_event_rows,
                ts=current_ts,
                scope=str(policy.scope),
                policy_type="strategy_risk",
                decision="applied",
                reason_code="nav_drawdown_stop_loss",
                policy_id=str(stop_loss.id),
                observed_value=float(drawdown_pct),
                threshold_value=float(stop_loss.thresholdPct),
                action=str(stop_loss.action),
                details={"cooldownBars": int(policy.reentry.cooldownBars or 0)},
            )
            return

    take_profit = policy.takeProfit
    if take_profit is not None and bool(take_profit.enabled) and state.strategy_nav_baseline > 0:
        gain_pct = (close_equity / float(state.strategy_nav_baseline) - 1.0) * 100.0
        if gain_pct >= float(take_profit.thresholdPct):
            if take_profit.action == "reduce_exposure":
                reduction = float(take_profit.reductionPct or 0.0) / 100.0
                state.strategy_exposure_multiplier = max(0.0, state.strategy_exposure_multiplier * (1.0 - reduction))
                state.strategy_cooldown_until_index = bar_index + int(policy.reentry.cooldownBars or 0)
                state.strategy_approval_required = bool(policy.reentry.requireApproval)
                state.strategy_approval_emitted = False
            else:
                state.strategy_exposure_multiplier = 1.0
                state.strategy_freeze_buys = False
                state.strategy_cooldown_until_index = None
                state.strategy_approval_required = False
            state.strategy_nav_baseline = close_equity
            state.strategy_nav_peak = close_equity
            _record_policy_event(
                policy_event_rows,
                ts=current_ts,
                scope=str(policy.scope),
                policy_type="strategy_risk",
                decision="applied",
                reason_code="nav_gain_take_profit",
                policy_id=str(take_profit.id),
                observed_value=float(gain_pct),
                threshold_value=float(take_profit.thresholdPct),
                action=str(take_profit.action),
                details={"cooldownBars": int(policy.reentry.cooldownBars or 0)},
            )


def _costs_from_raw_config(raw: dict[str, Any]) -> tuple[float, float]:
    costs = raw.get("costs") if isinstance(raw, dict) else None
    if not isinstance(costs, dict):
        return 0.0, 0.0
    commission_bps = float(costs.get("commissionBps") or costs.get("commission_bps") or 0.0)
    slippage_bps = float(costs.get("slippageBps") or costs.get("slippage_bps") or 0.0)
    return commission_bps, slippage_bps


def _resolve_regime_revision(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    regime_model_name: str | None = None,
    regime_model_version: int | None = None,
) -> tuple[str | None, int | None, dict[str, Any] | None]:
    policy = strategy_config.regimePolicy
    if policy is None:
        return None, None, None

    resolved_name = str(regime_model_name or policy.modelName or DEFAULT_REGIME_MODEL_NAME).strip()
    if not resolved_name:
        resolved_name = DEFAULT_REGIME_MODEL_NAME
    if resolved_name == DEFAULT_REGIME_MODEL_NAME and getattr(policy, "mode", None) != "observe_only":
        raise ValueError("default-regime requires regimePolicy.mode='observe_only'.")

    repo = RegimeRepository(dsn)
    revision = (
        repo.get_regime_model_revision(resolved_name, version=regime_model_version)
        if regime_model_version is not None
        else repo.get_active_regime_model_revision(resolved_name)
    )
    if not revision:
        if regime_model_version is not None:
            raise ValueError(f"Regime model '{resolved_name}' version '{regime_model_version}' not found.")
        raise ValueError(f"Regime model '{resolved_name}' does not have an active revision.")
    return resolved_name, int(revision["version"]), dict(revision.get("config") or {})


def _load_regime_history_frame(
    dsn: str,
    *,
    model_name: str,
    model_version: int,
    max_effective_from_date: date,
) -> pd.DataFrame:
    sql = """
        SELECT
            as_of_date,
            effective_from_date,
            model_name,
            model_version,
            regime_code,
            display_name,
            signal_state,
            score,
            activation_threshold,
            is_active,
            matched_rule_id,
            halt_flag,
            halt_reason,
            evidence_json,
            computed_at
        FROM gold.regime_history
        WHERE model_name = %s
          AND model_version = %s
          AND effective_from_date <= %s
        ORDER BY effective_from_date ASC, as_of_date ASC, regime_code ASC
    """
    with connect(dsn) as conn:
        frame = pd.read_sql_query(
            sql,
            conn,
            params=(model_name, int(model_version), max_effective_from_date),
        )
    if frame.empty:
        return frame
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce").dt.date
    frame["effective_from_date"] = pd.to_datetime(frame["effective_from_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["as_of_date", "effective_from_date"]).reset_index(drop=True)
    return frame


def _snapshot_records_from_regime_history(regime_history: pd.DataFrame) -> pd.DataFrame:
    if regime_history.empty:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "effective_from_date",
                "model_name",
                "model_version",
                "signals",
                "active_regimes",
                "halt_flag",
                "halt_reason",
                "computed_at",
            ]
        )

    snapshot_rows: list[dict[str, Any]] = []
    group_columns = ["as_of_date", "effective_from_date", "model_name", "model_version"]
    for group_key, group in regime_history.groupby(group_columns, sort=True, dropna=False):
        ordered_group = group.sort_values("regime_code").reset_index(drop=True)
        signals: list[dict[str, Any]] = []
        active_regimes: list[str] = []
        for row in ordered_group.to_dict("records"):
            evidence = row.get("evidence_json")
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except json.JSONDecodeError:
                    evidence = {"raw": evidence}
            signal = {
                "regime_code": row.get("regime_code"),
                "display_name": row.get("display_name"),
                "signal_state": row.get("signal_state"),
                "score": row.get("score"),
                "activation_threshold": row.get("activation_threshold"),
                "is_active": bool(row.get("is_active")),
                "matched_rule_id": row.get("matched_rule_id"),
                "evidence": evidence or {},
            }
            signals.append(signal)
            if bool(row.get("is_active")):
                active_regimes.append(str(row.get("regime_code")))
        first = ordered_group.iloc[0].to_dict()
        as_of_date, effective_from_date, model_name, model_version = group_key
        snapshot_rows.append(
            {
                "as_of_date": as_of_date,
                "effective_from_date": effective_from_date,
                "model_name": model_name,
                "model_version": model_version,
                "signals": signals,
                "active_regimes": active_regimes,
                "halt_flag": bool(first.get("halt_flag")),
                "halt_reason": first.get("halt_reason"),
                "computed_at": first.get("computed_at"),
            }
        )
    return pd.DataFrame(snapshot_rows)


def _materialize_regime_schedule(
    regime_history: pd.DataFrame,
    *,
    session_dates: list[date],
) -> pd.DataFrame:
    schedule_frame = pd.DataFrame({"session_date": sorted(set(session_dates))})
    if schedule_frame.empty:
        return schedule_frame
    schedule_frame["session_date"] = pd.to_datetime(schedule_frame["session_date"], errors="coerce")
    if regime_history.empty:
        schedule_frame["effective_from_date"] = pd.NaT
        return schedule_frame

    history = _snapshot_records_from_regime_history(regime_history)
    history["effective_from_date"] = pd.to_datetime(history["effective_from_date"], errors="coerce")
    history = history.dropna(subset=["effective_from_date"]).sort_values(["effective_from_date", "as_of_date"])
    schedule_frame = schedule_frame.dropna(subset=["session_date"]).sort_values("session_date")
    merged = pd.merge_asof(
        schedule_frame,
        history,
        left_on="session_date",
        right_on="effective_from_date",
        direction="backward",
    )
    merged["session_date"] = pd.to_datetime(merged["session_date"], errors="coerce").dt.date
    return merged


def _validate_regime_history_coverage(
    dsn: str,
    *,
    model_name: str,
    model_version: int,
    schedule: list[datetime],
) -> None:
    session_dates = sorted({ts.date() for ts in schedule})
    if not session_dates:
        return
    history = _load_regime_history_frame(
        dsn,
        model_name=model_name,
        model_version=model_version,
        max_effective_from_date=max(session_dates),
    )
    merged = _materialize_regime_schedule(history, session_dates=session_dates)
    if merged.empty:
        raise ValueError(
            f"Regime history coverage gap for {model_name}@v{model_version}; no rows found for requested backtest window."
        )
    missing = merged[merged["effective_from_date"].isna()]
    if not missing.empty:
        sample = ", ".join(str(value) for value in missing["session_date"].astype(str).tolist()[:5])
        raise ValueError(
            f"Regime history coverage gap for {model_name}@v{model_version}; missing {len(missing)} session dates, sample={sample}"
        )


def _load_regime_schedule_map(
    dsn: str,
    *,
    definition: ResolvedBacktestDefinition,
    schedule: list[datetime],
) -> dict[date, dict[str, Any]]:
    if not definition.regime_model_name or definition.regime_model_version is None:
        return {}
    session_dates = sorted({ts.date() for ts in schedule})
    if not session_dates:
        return {}
    history = _load_regime_history_frame(
        dsn,
        model_name=definition.regime_model_name,
        model_version=definition.regime_model_version,
        max_effective_from_date=max(session_dates),
    )
    merged = _materialize_regime_schedule(history, session_dates=session_dates)
    regime_map: dict[date, dict[str, Any]] = {}
    for row in merged.to_dict("records"):
        session_date = row.get("session_date")
        if isinstance(session_date, date):
            regime_map[session_date] = row
    return regime_map


def _regime_context_for_session(
    policy: RegimePolicy | None,
    regime_row: dict[str, Any] | None,
) -> dict[str, Any]:
    if policy is None or not regime_row:
        return {
            "primary_regime_code": None,
            "halt_flag": False,
            "halt_reason": None,
            "as_of_date": None,
            "effective_from_date": None,
            "active_regimes": [],
            "signals": [],
        }
    active_regimes = [str(value) for value in (regime_row.get("active_regimes") or []) if str(value or "").strip()]
    primary_regime = active_regimes[0] if active_regimes else None
    signals = list(regime_row.get("signals") or [])
    halt_flag = bool(regime_row.get("halt_flag"))
    halt_reason = regime_row.get("halt_reason")

    return {
        "primary_regime_code": primary_regime,
        "halt_flag": halt_flag,
        "halt_reason": halt_reason,
        "as_of_date": regime_row.get("as_of_date"),
        "effective_from_date": regime_row.get("effective_from_date"),
        "active_regimes": active_regimes,
        "signals": signals,
    }


def _execute_trade(
    *,
    trades: list[dict[str, Any]],
    ts: datetime,
    symbol: str,
    quantity_delta: float,
    price: float,
    cash: float,
    commission_bps: float,
    slippage_bps: float,
) -> tuple[float, float, float]:
    if math.isclose(quantity_delta, 0.0, abs_tol=1e-12):
        return cash, 0.0, 0.0
    notional = float(quantity_delta * price)
    abs_notional = abs(notional)
    commission = abs_notional * commission_bps / 10000.0
    slippage = abs_notional * slippage_bps / 10000.0
    cash_after = cash - notional - commission - slippage
    trades.append(
        {
            "execution_date": ts.isoformat(),
            "symbol": symbol,
            "quantity": float(quantity_delta),
            "price": float(price),
            "notional": float(notional),
            "commission": float(commission),
            "slippage_cost": float(slippage),
            "cash_after": float(cash_after),
        }
    )
    return cash_after, commission, slippage


def _compute_summary(timeseries: pd.DataFrame, trades: pd.DataFrame, *, run_id: str, run_name: str | None) -> dict[str, Any]:
    if timeseries.empty:
        return {
            "run_id": run_id,
            "run_name": run_name,
            "total_return": 0.0,
            "annualized_return": 0.0,
            "annualized_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "trades": int(len(trades)),
            "initial_cash": 0.0,
            "final_equity": 0.0,
        }
    initial_cash = float(timeseries["portfolio_value"].iloc[0])
    final_equity = float(timeseries["portfolio_value"].iloc[-1])
    total_return = (final_equity / initial_cash - 1.0) if initial_cash else 0.0
    returns = pd.to_numeric(timeseries["daily_return"], errors="coerce").fillna(0.0)
    periods = max(len(returns), 1)
    annualization = 252.0
    annualized_return = (1.0 + total_return) ** (annualization / periods) - 1.0 if periods > 0 else 0.0
    annualized_volatility = float(returns.std(ddof=0) * math.sqrt(annualization)) if len(returns) > 1 else 0.0
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0.0
    max_drawdown = float(pd.to_numeric(timeseries["drawdown"], errors="coerce").min() or 0.0)
    return {
        "run_id": run_id,
        "run_name": run_name,
        "start_date": str(timeseries["date"].iloc[0]),
        "end_date": str(timeseries["date"].iloc[-1]),
        "total_return": float(total_return),
        "annualized_return": float(annualized_return),
        "annualized_volatility": float(annualized_volatility),
        "sharpe_ratio": float(sharpe_ratio),
        "max_drawdown": float(max_drawdown),
        "trades": int(len(trades)),
        "initial_cash": float(initial_cash),
        "final_equity": float(final_equity),
    }


def _compute_rolling_metrics(timeseries: pd.DataFrame, *, window_bars: int = 63) -> pd.DataFrame:
    if timeseries.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "window_days",
                "rolling_return",
                "rolling_volatility",
                "rolling_sharpe",
                "rolling_max_drawdown",
                "turnover_sum",
                "commission_sum",
                "slippage_cost_sum",
                "n_trades_sum",
                "gross_exposure_avg",
                "net_exposure_avg",
            ]
        )
    frame = timeseries.copy()
    returns = pd.to_numeric(frame["daily_return"], errors="coerce").fillna(0.0)
    frame["rolling_return"] = (1.0 + returns).rolling(window_bars).apply(lambda values: float(np.prod(values) - 1.0), raw=True)
    frame["rolling_volatility"] = returns.rolling(window_bars).std(ddof=0) * math.sqrt(252.0)
    frame["rolling_sharpe"] = np.where(
        frame["rolling_volatility"].fillna(0.0) > 0,
        frame["rolling_return"] * (252.0 / max(window_bars, 1)) / frame["rolling_volatility"],
        0.0,
    )
    frame["rolling_max_drawdown"] = frame["drawdown"].rolling(window_bars).min()
    frame["turnover_sum"] = pd.to_numeric(frame["turnover"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["commission_sum"] = pd.to_numeric(frame["commission"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["slippage_cost_sum"] = pd.to_numeric(frame["slippage_cost"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["n_trades_sum"] = pd.to_numeric(frame["trade_count"], errors="coerce").fillna(0.0).rolling(window_bars).sum()
    frame["gross_exposure_avg"] = pd.to_numeric(frame["gross_exposure"], errors="coerce").fillna(0.0).rolling(window_bars).mean()
    frame["net_exposure_avg"] = pd.to_numeric(frame["net_exposure"], errors="coerce").fillna(0.0).rolling(window_bars).mean()
    frame["window_days"] = window_bars
    return frame[
        [
            "date",
            "window_days",
            "rolling_return",
            "rolling_volatility",
            "rolling_sharpe",
            "rolling_max_drawdown",
            "turnover_sum",
            "commission_sum",
            "slippage_cost_sum",
            "n_trades_sum",
            "gross_exposure_avg",
            "net_exposure_avg",
        ]
    ].copy()


def _normalized_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalized_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def resolve_backtest_definition_for_run(
    dsn: str,
    *,
    run: dict[str, Any],
) -> ResolvedBacktestDefinition:
    effective_config = run.get("effective_config") if isinstance(run.get("effective_config"), dict) else {}
    input_mode = str(effective_config.get("inputMode") or "").strip().lower()
    if input_mode == "inline":
        strategy_payload = effective_config.get("strategy")
        if not isinstance(strategy_payload, dict) or not strategy_payload:
            raise ValueError(f"Run '{run.get('run_id')}' is missing inline strategy configuration.")
        pins = effective_config.get("pins") if isinstance(effective_config.get("pins"), dict) else {}
        return resolve_backtest_definition_from_config(
            dsn,
            strategy_config_raw=strategy_payload,
            ranking_schema_name=_normalized_text(pins.get("rankingSchemaName")),
            ranking_schema_version=_normalized_int(pins.get("rankingSchemaVersion")),
            universe_name=_normalized_text(pins.get("universeName")),
            universe_version=_normalized_int(pins.get("universeVersion")),
            regime_model_name=_normalized_text(pins.get("regimeModelName")),
            regime_model_version=_normalized_int(pins.get("regimeModelVersion")),
        )
    strategy_name = _normalized_text(run.get("strategy_name"))
    if not strategy_name:
        raise ValueError(f"Run '{run.get('run_id')}' does not have a saved strategy reference.")
    return resolve_backtest_definition(
        dsn,
        strategy_name=strategy_name,
        strategy_version=_normalized_int(run.get("strategy_version")),
        regime_model_name=_normalized_text(run.get("regime_model_name")),
        regime_model_version=_normalized_int(run.get("regime_model_version")),
    )


def execute_backtest_run(
    dsn: str,
    *,
    run_id: str,
    execution_name: str | None = None,
) -> dict[str, Any]:
    repo = BacktestRepository(dsn)
    run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found.")
    if run["status"] == "queued":
        repo.start_run(run_id, execution_name=execution_name)
        run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found after start.")

    start_ts = _ensure_utc(run["start_ts"])
    end_ts = _ensure_utc(run["end_ts"])
    definition = resolve_backtest_definition_for_run(dsn, run=run)
    schedule = validate_backtest_submission(
        dsn,
        definition=definition,
        start_ts=start_ts,
        end_ts=end_ts,
        bar_size=str(run.get("bar_size") or "").strip() or None,
    )

    table_specs = universe_service._load_gold_table_specs(dsn)
    required_columns = _required_columns(definition)
    grouped_schedule: dict[date, list[datetime]] = defaultdict(list)
    for ts in schedule:
        grouped_schedule[ts.date()].append(ts)
    regime_schedule_map = _load_regime_schedule_map(dsn, definition=definition, schedule=schedule)

    evaluator = ExitRuleEvaluator()
    commission_bps, slippage_bps = _costs_from_raw_config(definition.strategy_config_raw)
    cash = float(definition.strategy_config_raw.get("initialCash") or 100000.0)
    positions: dict[str, PositionState] = {}
    rebalance_policy = definition.strategy_config.rebalancePolicy
    strategy_risk_policy = definition.strategy_config.strategyRiskPolicy
    structured_policy_enabled = rebalance_policy is not None or strategy_risk_policy is not None
    policy_state = PolicyRuntimeState()
    pending_target_weights: dict[str, float] | None = {}
    selection_trace_rows: list[dict[str, Any]] = []
    regime_trace_rows: list[dict[str, Any]] = []
    policy_event_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []
    previous_equity = cash
    previous_close_by_symbol: dict[str, float] = {}
    first_signal_computed = False
    bar_index = -1

    for session_date, session_schedule in grouped_schedule.items():
        session_start, session_end = _session_bounds(session_schedule[0])
        intraday_frames = _load_intraday_session_frames(
            dsn,
            table_specs=table_specs,
            required_columns=required_columns,
            session_start=session_start,
            session_end=session_end,
            bar_size=str(run.get("bar_size") or "").strip() or None,
        )
        slow_frames = _load_slow_frames(
            dsn,
            table_specs=table_specs,
            required_columns=required_columns,
            as_of_ts=session_schedule[-1],
            bar_size=str(run.get("bar_size") or "").strip() or None,
        )
        for index, current_ts in enumerate(session_schedule):
            bar_index += 1
            snapshot = _snapshot_for_timestamp(current_ts, intraday_frames=intraday_frames, slow_frames=slow_frames)
            repo.update_heartbeat(run_id)
            regime_row = regime_schedule_map.get(session_date)
            regime_context = _regime_context_for_session(definition.strategy_config.regimePolicy, regime_row)
            regime_trace_rows.append(
                {
                    "date": current_ts.isoformat(),
                    "session_date": session_date.isoformat(),
                    "model_name": definition.regime_model_name,
                    "model_version": definition.regime_model_version,
                    "as_of_date": (
                        regime_context["as_of_date"].isoformat()
                        if isinstance(regime_context["as_of_date"], date)
                        else regime_context["as_of_date"]
                    ),
                    "effective_from_date": (
                        regime_context["effective_from_date"].isoformat()
                        if isinstance(regime_context["effective_from_date"], date)
                        else regime_context["effective_from_date"]
                    ),
                    "primary_regime_code": regime_context["primary_regime_code"],
                    "halt_flag": bool(regime_context["halt_flag"]),
                    "halt_reason": regime_context["halt_reason"],
                    "active_regimes": list(regime_context["active_regimes"]),
                    "signals": list(regime_context["signals"]),
                }
            )
            if not first_signal_computed:
                target_multiplier = _active_strategy_exposure_multiplier(
                    policy_state,
                    policy=strategy_risk_policy,
                    bar_index=bar_index,
                    current_ts=current_ts,
                    policy_event_rows=policy_event_rows,
                )
                initial_ranking = _score_snapshot(
                    snapshot,
                    definition=definition,
                    rebalance_ts=current_ts,
                    target_weight_multiplier=target_multiplier,
                )
                initial_ranking_records = initial_ranking.to_dict("records")
                selection_trace_rows.extend(initial_ranking_records)
                if structured_policy_enabled:
                    pending_target_weights = _schedule_rebalance_targets(
                        initial_ranking_records,
                        policy=rebalance_policy,
                        risk_policy=strategy_risk_policy,
                        state=policy_state,
                        positions=positions,
                        snapshot=snapshot,
                        previous_close_by_symbol=previous_close_by_symbol,
                        close_equity=cash,
                        current_ts=current_ts,
                        bar_index=bar_index,
                        policy_event_rows=policy_event_rows,
                    )
                else:
                    pending_target_weights = {
                        str(row["symbol"]): float(row["target_weight"])
                        for row in initial_ranking_records
                        if bool(row["selected"])
                    }
                first_signal_computed = True
                continue

            total_commission = 0.0
            total_slippage = 0.0
            trade_count = 0
            market_equity_open = cash

            for symbol, position in list(positions.items()):
                row = _market_row(snapshot, symbol)
                if row is None:
                    market_equity_open += position.quantity * previous_close_by_symbol.get(symbol, position.entry_price)
                    continue
                open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(row.get(f"{_PRICE_TABLE}__close")) or position.entry_price
                market_equity_open += position.quantity * open_price

            should_apply_pending_rebalance = pending_target_weights is not None if structured_policy_enabled else True
            target_qty_by_symbol: dict[str, float] = {}
            if should_apply_pending_rebalance and pending_target_weights:
                for symbol, target_weight in pending_target_weights.items():
                    row = _market_row(snapshot, symbol)
                    if row is None:
                        continue
                    open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(
                        row.get(f"{_PRICE_TABLE}__close")
                    )
                    if open_price is None or open_price <= 0:
                        continue
                    target_qty_by_symbol[symbol] = (market_equity_open * target_weight) / open_price

            all_symbols = sorted((set(positions.keys()) | set(target_qty_by_symbol.keys())) if should_apply_pending_rebalance else set())
            for symbol in all_symbols:
                row = _market_row(snapshot, symbol)
                if row is None:
                    continue
                open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(
                    row.get(f"{_PRICE_TABLE}__close")
                )
                if open_price is None or open_price <= 0:
                    continue
                current_qty = positions[symbol].quantity if symbol in positions else 0.0
                if (
                    structured_policy_enabled
                    and rebalance_policy is not None
                    and not bool(rebalance_policy.closeRemovedPositions)
                    and symbol not in target_qty_by_symbol
                ):
                    target_qty = current_qty
                else:
                    target_qty = target_qty_by_symbol.get(symbol, 0.0)
                delta_qty = target_qty - current_qty
                if math.isclose(delta_qty, 0.0, abs_tol=1e-9):
                    continue
                trade_notional = abs(delta_qty * open_price)
                if (
                    structured_policy_enabled
                    and rebalance_policy is not None
                    and trade_notional < float(rebalance_policy.minTradeNotional or 0.0)
                ):
                    _record_policy_event(
                        policy_event_rows,
                        ts=current_ts,
                        scope="symbol",
                        policy_type="rebalance",
                        decision="skipped",
                        reason_code="min_notional",
                        symbol=symbol,
                        observed_value=float(trade_notional),
                        threshold_value=float(rebalance_policy.minTradeNotional or 0.0),
                        action="skip_trade",
                    )
                    continue
                cash, commission, slippage = _execute_trade(
                    trades=trade_rows,
                    ts=current_ts,
                    symbol=symbol,
                    quantity_delta=delta_qty,
                    price=open_price,
                    cash=cash,
                    commission_bps=commission_bps,
                    slippage_bps=slippage_bps,
                )
                total_commission += commission
                total_slippage += slippage
                trade_count += 1
                if target_qty <= 1e-9:
                    positions.pop(symbol, None)
                    previous_close_by_symbol.pop(symbol, None)
                else:
                    positions[symbol] = PositionState(
                        symbol=symbol,
                        entry_date=current_ts,
                        entry_price=open_price,
                        quantity=float(target_qty),
                    )

            pending_target_weights = None if structured_policy_enabled else {}

            for symbol, position in list(positions.items()):
                row = _market_row(snapshot, symbol)
                if row is None:
                    continue
                bar = _price_bar(current_ts, row)
                evaluation = evaluator.evaluate_bar(definition.strategy_config, position, bar)
                positions[symbol] = evaluation.position_state
                if evaluation.decision is None:
                    previous_close_by_symbol[symbol] = bar.close or previous_close_by_symbol.get(symbol, position.entry_price)
                    continue
                cash, commission, slippage = _execute_trade(
                    trades=trade_rows,
                    ts=current_ts,
                    symbol=symbol,
                    quantity_delta=-position.quantity,
                    price=float(evaluation.decision.exit_price),
                    cash=cash,
                    commission_bps=commission_bps,
                    slippage_bps=slippage_bps,
                )
                total_commission += commission
                total_slippage += slippage
                trade_count += 1
                positions.pop(symbol, None)
                previous_close_by_symbol.pop(symbol, None)
                if strategy_risk_policy is not None and bool(strategy_risk_policy.enabled):
                    cooldown_bars = int(strategy_risk_policy.reentry.cooldownBars or 0)
                    if cooldown_bars > 0:
                        policy_state.position_cooldown_until[symbol] = bar_index + cooldown_bars
                    if bool(strategy_risk_policy.reentry.requireApproval):
                        policy_state.position_approval_required.add(symbol)
                    _record_policy_event(
                        policy_event_rows,
                        ts=current_ts,
                        scope="position",
                        policy_type="position_exit",
                        decision="applied",
                        reason_code=str(evaluation.decision.exit_reason),
                        symbol=symbol,
                        policy_id=str(evaluation.decision.rule_id),
                        action="exit_position",
                    )

            close_equity = cash
            gross_exposure = 0.0
            for symbol, position in positions.items():
                row = _market_row(snapshot, symbol)
                close_price = None
                if row is not None:
                    close_price = _maybe_float(row.get(f"{_PRICE_TABLE}__close")) or _maybe_float(row.get(f"{_PRICE_TABLE}__open"))
                if close_price is None:
                    close_price = previous_close_by_symbol.get(symbol, position.entry_price)
                previous_close_by_symbol[symbol] = float(close_price)
                position_value = float(position.quantity * close_price)
                close_equity += position_value
                gross_exposure += abs(position_value)

            period_return = (close_equity / previous_equity - 1.0) if previous_equity else 0.0
            running_peak = max([close_equity, *(row["portfolio_value"] for row in timeseries_rows)] if timeseries_rows else [close_equity])
            drawdown = (close_equity / running_peak - 1.0) if running_peak else 0.0
            timeseries_rows.append(
                {
                    "date": current_ts.isoformat(),
                    "portfolio_value": float(close_equity),
                    "drawdown": float(drawdown),
                    "daily_return": float(period_return),
                    "cumulative_return": float(close_equity / timeseries_rows[0]["portfolio_value"] - 1.0) if timeseries_rows else 0.0,
                    "cash": float(cash),
                    "gross_exposure": float(gross_exposure / close_equity) if close_equity else 0.0,
                    "net_exposure": float(gross_exposure / close_equity) if close_equity else 0.0,
                    "turnover": float(
                        sum(abs(trade["notional"]) for trade in trade_rows[-trade_count:]) / previous_equity
                    ) if previous_equity and trade_count else 0.0,
                    "commission": float(total_commission),
                    "slippage_cost": float(total_slippage),
                    "trade_count": int(trade_count),
                }
            )
            previous_equity = close_equity

            _apply_strategy_risk_policy(
                strategy_risk_policy,
                state=policy_state,
                close_equity=close_equity,
                current_ts=current_ts,
                bar_index=bar_index,
                policy_event_rows=policy_event_rows,
            )

            if index < len(session_schedule) - 1:
                target_multiplier = _active_strategy_exposure_multiplier(
                    policy_state,
                    policy=strategy_risk_policy,
                    bar_index=bar_index,
                    current_ts=current_ts,
                    policy_event_rows=policy_event_rows,
                )
                ranking = _score_snapshot(
                    snapshot,
                    definition=definition,
                    rebalance_ts=current_ts,
                    target_weight_multiplier=target_multiplier,
                )
                ranking_records = ranking.to_dict("records")
                selection_trace_rows.extend(ranking_records)
                if structured_policy_enabled:
                    pending_target_weights = _schedule_rebalance_targets(
                        ranking_records,
                        policy=rebalance_policy,
                        risk_policy=strategy_risk_policy,
                        state=policy_state,
                        positions=positions,
                        snapshot=snapshot,
                        previous_close_by_symbol=previous_close_by_symbol,
                        close_equity=close_equity,
                        current_ts=current_ts,
                        bar_index=bar_index,
                        policy_event_rows=policy_event_rows,
                    )
                else:
                    pending_target_weights = {
                        str(row["symbol"]): float(row["target_weight"])
                        for row in ranking_records
                        if bool(row["selected"])
                    }

    timeseries = pd.DataFrame(timeseries_rows)
    trades = pd.DataFrame(trade_rows)
    selection_trace = pd.DataFrame(selection_trace_rows)
    regime_trace = pd.DataFrame(regime_trace_rows)
    rolling_metrics = _compute_rolling_metrics(timeseries)
    summary = _compute_summary(
        timeseries,
        trades,
        run_id=run_id,
        run_name=run.get("run_name"),
    )

    persist_backtest_results(
        dsn,
        run_id=run_id,
        summary=summary,
        timeseries_rows=timeseries.to_dict("records"),
        rolling_metric_rows=rolling_metrics.to_dict("records"),
        trade_rows=trades.to_dict("records"),
        selection_trace_rows=selection_trace.to_dict("records"),
        regime_trace_rows=regime_trace.to_dict("records"),
        policy_event_rows=policy_event_rows,
        results_schema_version=BACKTEST_RESULTS_SCHEMA_VERSION,
    )
    repo.complete_run(run_id, summary=summary)
    return {"summary": summary}
