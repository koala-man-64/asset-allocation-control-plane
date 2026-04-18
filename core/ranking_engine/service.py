from __future__ import annotations

import logging
import math
import uuid
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from asset_allocation_runtime_common.foundation.postgres import connect, copy_rows
from core.ranking_engine.contracts import (
    RankingGroup,
    RankingMaterializationSummary,
    RankingPreviewRow,
    RankingSchemaConfig,
    RankingTransform,
)
from core.ranking_engine.naming import build_scoped_identifier, slugify_strategy_output_table
from core.ranking_repository import RankingRepository
from core.strategy_engine import StrategyConfig, UniverseCondition, UniverseDefinition, UniverseGroup
from core.strategy_engine import universe as universe_service
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)


def preview_strategy_rankings(
    dsn: str,
    *,
    strategy_name: str,
    schema: RankingSchemaConfig,
    as_of_date: date,
    limit: int = 25,
) -> dict[str, Any]:
    strategy = _load_strategy(dsn, strategy_name)
    ranked = _compute_rankings_dataframe(
        dsn,
        strategy_config=strategy["config"],
        ranking_schema=schema,
        start_date=as_of_date,
        end_date=as_of_date,
    )
    preview_rows = [
        RankingPreviewRow(symbol=str(row["symbol"]), rank=int(row["rank"]), score=float(row["score"])).model_dump()
        for _, row in ranked.head(limit).iterrows()
    ]
    return {
        "strategyName": strategy_name,
        "asOfDate": as_of_date,
        "rowCount": int(len(ranked)),
        "rows": preview_rows,
        "warnings": [] if not ranked.empty else ["Preview returned zero ranked symbols."],
    }


def materialize_strategy_rankings(
    dsn: str,
    *,
    strategy_name: str,
    start_date: date | None = None,
    end_date: date | None = None,
    triggered_by: str = "manual",
) -> dict[str, Any]:
    strategy = _load_strategy(dsn, strategy_name)
    strategy_config: StrategyConfig = strategy["config"]
    if not strategy_config.rankingSchemaName:
        raise ValueError(f"Strategy '{strategy_name}' does not reference a ranking schema.")

    ranking_repo = RankingRepository(dsn)
    ranking_schema_record = ranking_repo.get_ranking_schema(strategy_config.rankingSchemaName)
    if not ranking_schema_record:
        raise ValueError(f"Ranking schema '{strategy_config.rankingSchemaName}' not found.")

    ranking_schema = RankingSchemaConfig.model_validate(ranking_schema_record["config"])
    resolved_start, resolved_end = _resolve_date_range(dsn, ranking_schema, strategy_config, start_date, end_date)
    run_id = uuid.uuid4().hex
    output_table_name = str(strategy.get("output_table_name") or slugify_strategy_output_table(strategy_name))
    _insert_ranking_run(
        dsn,
        run_id=run_id,
        strategy_name=strategy_name,
        ranking_schema_name=strategy_config.rankingSchemaName,
        ranking_schema_version=int(ranking_schema_record["version"]),
        output_table_name=output_table_name,
        start_date=resolved_start,
        end_date=resolved_end,
        status="running",
        triggered_by=triggered_by,
    )

    try:
        ranked = _compute_rankings_dataframe(
            dsn,
            strategy_config=strategy_config,
            ranking_schema=ranking_schema,
            start_date=resolved_start,
            end_date=resolved_end,
        )
        rows_written = _write_rankings_to_platinum(
            dsn,
            table_name=output_table_name,
            ranked=ranked,
            start_date=resolved_start,
            end_date=resolved_end,
        )
        _update_ranking_run(
            dsn,
            run_id=run_id,
            status="success",
            row_count=rows_written,
            date_count=int(ranked["date"].nunique()) if not ranked.empty else 0,
            error=None,
        )
        _upsert_ranking_watermark(
            dsn,
            strategy_name=strategy_name,
            ranking_schema_name=strategy_config.rankingSchemaName,
            ranking_schema_version=int(ranking_schema_record["version"]),
            output_table_name=output_table_name,
            last_ranked_date=resolved_end,
        )
        summary = RankingMaterializationSummary(
            runId=run_id,
            strategyName=strategy_name,
            rankingSchemaName=strategy_config.rankingSchemaName,
            rankingSchemaVersion=int(ranking_schema_record["version"]),
            outputTableName=output_table_name,
            startDate=resolved_start,
            endDate=resolved_end,
            rowCount=rows_written,
            dateCount=int(ranked["date"].nunique()) if not ranked.empty else 0,
        )
        return summary.model_dump()
    except Exception as exc:
        logger.exception("Ranking materialization failed for strategy '%s'.", strategy_name)
        _update_ranking_run(dsn, run_id=run_id, status="error", row_count=0, date_count=0, error=str(exc))
        raise


def _load_strategy(dsn: str, strategy_name: str) -> dict[str, Any]:
    repo = StrategyRepository(dsn)
    strategy = repo.get_strategy(strategy_name)
    if not strategy:
        raise ValueError(f"Strategy '{strategy_name}' not found.")
    strategy["config"] = StrategyConfig.model_validate(_normalize_strategy_config_payload(strategy.get("config") or {}))
    return strategy


def _normalize_strategy_config_payload(config: Any) -> dict[str, Any]:
    payload = config.model_dump(exclude_none=True) if hasattr(config, "model_dump") else dict(config or {})
    universe_payload = payload.get("universe")
    if universe_payload is not None:
        normalized_universe = universe_service._normalize_universe_definition(universe_payload)
        payload["universe"] = normalized_universe.model_dump(exclude_none=True)
    return payload


def _resolve_date_range(
    dsn: str,
    ranking_schema: RankingSchemaConfig,
    strategy_config: StrategyConfig,
    start_date: date | None,
    end_date: date | None,
) -> tuple[date, date]:
    if start_date and end_date:
        return start_date, end_date

    table_specs = universe_service._load_gold_table_specs(dsn)
    strategy_universe = _resolve_strategy_universe(dsn, strategy_config)
    ranking_universe = _resolve_ranking_universe(dsn, ranking_schema)
    referenced_tables = _collect_required_columns(strategy_universe, ranking_universe, ranking_schema)
    candidate_dates: list[date] = []
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            for table_name in referenced_tables.keys():
                spec = table_specs[table_name]
                cur.execute(
                    f"""
                    SELECT MIN({universe_service._quote_identifier(spec.as_of_column)}),
                           MAX({universe_service._quote_identifier(spec.as_of_column)})
                    FROM "gold".{universe_service._quote_identifier(table_name)}
                    """
                )
                row = cur.fetchone()
                if not row or not row[1]:
                    continue
                if row[0]:
                    candidate_dates.append(row[0])
                candidate_dates.append(row[1])
    if not candidate_dates:
        today = datetime.now(timezone.utc).date()
        return start_date or today, end_date or today
    resolved_start = start_date or min(candidate_dates)
    resolved_end = end_date or max(candidate_dates)
    return resolved_start, resolved_end


def _compute_rankings_dataframe(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    ranking_schema: RankingSchemaConfig,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    table_specs = universe_service._load_gold_table_specs(dsn)
    strategy_universe = _resolve_strategy_universe(dsn, strategy_config)
    ranking_universe = _resolve_ranking_universe(dsn, ranking_schema)
    required_columns = _collect_required_columns(strategy_universe, ranking_universe, ranking_schema)
    frames = _load_table_frames(dsn, table_specs=table_specs, required_columns=required_columns, start_date=start_date, end_date=end_date)
    merged = _merge_frames(frames)
    if merged.empty:
        return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    filtered = merged[
        _evaluate_universe_mask(merged, strategy_universe.root) & _evaluate_universe_mask(merged, ranking_universe.root)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    group_scores: list[tuple[str, float, pd.Series]] = []
    required_factor_columns: list[pd.Series] = []
    for group in ranking_schema.groups:
        group_series, group_required_masks = _score_group(filtered, group)
        group_scores.append((group.name, group.weight, group_series))
        required_factor_columns.extend(group_required_masks)

    if required_factor_columns:
        required_mask = pd.concat(required_factor_columns, axis=1).all(axis=1)
        filtered = filtered[required_mask].copy()
        group_scores = [(name, weight, series.loc[filtered.index]) for name, weight, series in group_scores]
        if filtered.empty:
            return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    weighted_total = pd.Series(0.0, index=filtered.index)
    total_weight = 0.0
    for _name, weight, series in group_scores:
        weighted_total = weighted_total.add(series * weight, fill_value=0.0)
        total_weight += weight
    if total_weight <= 0:
        raise ValueError("Ranking schema produced zero total group weight.")
    filtered["score"] = weighted_total / total_weight
    filtered["score"] = _apply_transforms(filtered["score"], filtered["date"], ranking_schema.overallTransforms)
    filtered = filtered.dropna(subset=["score"]).copy()
    if filtered.empty:
        return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    filtered = filtered.sort_values(["date", "score", "symbol"], ascending=[True, False, True]).reset_index(drop=True)
    filtered["rank"] = filtered.groupby("date").cumcount() + 1
    return filtered[["date", "symbol", "score", "rank"]]


def _resolve_strategy_universe(dsn: str, strategy_config: StrategyConfig) -> UniverseDefinition:
    if strategy_config.universe is not None:
        return universe_service._normalize_universe_definition(strategy_config.universe)
    if not strategy_config.universeConfigName:
        raise ValueError("Strategy config must reference universeConfigName.")
    repo = UniverseRepository(dsn)
    universe = repo.get_universe_config(strategy_config.universeConfigName)
    if not universe:
        raise ValueError(f"Universe config '{strategy_config.universeConfigName}' not found.")
    return universe_service._normalize_universe_definition(universe.get("config") or {})


def _resolve_ranking_universe(dsn: str, ranking_schema: RankingSchemaConfig) -> UniverseDefinition:
    if not ranking_schema.universeConfigName:
        raise ValueError("Ranking schema config must reference universeConfigName.")
    repo = UniverseRepository(dsn)
    universe = repo.get_universe_config(ranking_schema.universeConfigName)
    if not universe:
        raise ValueError(f"Universe config '{ranking_schema.universeConfigName}' not found.")
    return universe_service._normalize_universe_definition(universe.get("config") or {})


def _collect_required_columns(
    strategy_universe: UniverseDefinition,
    ranking_universe: UniverseDefinition,
    ranking_schema: RankingSchemaConfig,
) -> dict[str, set[str]]:
    required: dict[str, set[str]] = {}
    _collect_universe_columns(strategy_universe.root, required)
    _collect_universe_columns(ranking_universe.root, required)
    for group in ranking_schema.groups:
        for factor in group.factors:
            required.setdefault(factor.table, set()).add(factor.column)
    return required


def _collect_universe_columns(node: UniverseGroup | UniverseCondition, required: dict[str, set[str]]) -> None:
    if isinstance(node, dict):
        normalized = universe_service._normalize_universe_definition({"source": "postgres_gold", "root": node})
        _collect_universe_columns(normalized.root, required)
        return
    if isinstance(node, UniverseCondition):
        table_name, column_name = _resolve_universe_condition_column(node)
        required.setdefault(table_name, set()).add(column_name)
        return
    for clause in node.clauses:
        _collect_universe_columns(clause, required)


def _load_table_frames(
    dsn: str,
    *,
    table_specs: dict[str, Any],
    required_columns: dict[str, set[str]],
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs.get(table_name)
            if spec is None:
                raise ValueError(f"Unknown gold table '{table_name}'.")
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS date",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            query = f"""
                SELECT {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} >= %s
                  AND {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            with conn.cursor() as cur:
                cur.execute(query, (start_date, end_date))
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            if frame.empty:
                frames[table_name] = pd.DataFrame(columns=["date", "symbol", *[f"{table_name}__{column}" for column in selected_columns]])
                continue
            frame["symbol"] = frame["symbol"].astype("string").str.upper()
            frame["date"] = pd.to_datetime(frame["date"]).dt.date
            for column in selected_columns:
                normalized = f"{table_name}__{column}"
                series = frame[column]
                if str(series.dtype) == "bool":
                    frame[normalized] = series.astype(int)
                else:
                    frame[normalized] = pd.to_numeric(series, errors="coerce")
            frames[table_name] = frame[["date", "symbol", *[f"{table_name}__{column}" for column in selected_columns]]]
    return frames


def _merge_frames(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for frame in frames.values():
        if merged is None:
            merged = frame.copy()
            continue
        merged = merged.merge(frame, on=["date", "symbol"], how="outer")
    if merged is None:
        return pd.DataFrame(columns=["date", "symbol"])
    merged = merged.drop_duplicates(subset=["date", "symbol"]).reset_index(drop=True)
    return merged


def _evaluate_universe_mask(df: pd.DataFrame, node: UniverseGroup | UniverseCondition) -> pd.Series:
    if isinstance(node, dict):
        normalized = universe_service._normalize_universe_definition({"source": "postgres_gold", "root": node})
        return _evaluate_universe_mask(df, normalized.root)
    if isinstance(node, UniverseCondition):
        table_name, column_name = _resolve_universe_condition_column(node)
        column_name = f"{table_name}__{column_name}"
        if column_name not in df.columns:
            return pd.Series(False, index=df.index)
        series = df[column_name]
        operator = node.operator
        if operator == "eq":
            return series == node.value
        if operator == "ne":
            return series != node.value
        if operator == "gt":
            return series > node.value
        if operator == "gte":
            return series >= node.value
        if operator == "lt":
            return series < node.value
        if operator == "lte":
            return series <= node.value
        if operator == "in":
            return series.isin(node.values or [])
        if operator == "not_in":
            return ~series.isin(node.values or [])
        if operator == "is_null":
            return series.isna()
        if operator == "is_not_null":
            return series.notna()
        raise ValueError(f"Unsupported universe operator '{operator}'.")

    child_masks = [_evaluate_universe_mask(df, clause) for clause in node.clauses]
    if node.operator == "and":
        result = child_masks[0].copy()
        for mask in child_masks[1:]:
            result &= mask
        return result
    result = child_masks[0].copy()
    for mask in child_masks[1:]:
        result |= mask
    return result


def _resolve_universe_condition_column(node: UniverseCondition) -> tuple[str, str]:
    field_id = getattr(node, "field", None)
    if field_id:
        normalized_field_id = str(field_id).strip().lower()
        field_spec = universe_service.UNIVERSE_FIELD_MAP.get(normalized_field_id)
        if field_spec is None:
            raise ValueError(f"Unknown universe field '{field_id}'.")
        return field_spec.table, field_spec.column

    table_name = str(getattr(node, "table", "") or "").strip().lower()
    column_name = str(getattr(node, "column", "") or "").strip().lower()
    if not table_name or not column_name:
        raise ValueError("Universe condition must declare either field or table/column.")
    return table_name, column_name


def _score_group(df: pd.DataFrame, group: RankingGroup) -> tuple[pd.Series, list[pd.Series]]:
    weighted_total = pd.Series(0.0, index=df.index)
    total_weight = 0.0
    required_masks: list[pd.Series] = []
    for factor in group.factors:
        column_name = f"{factor.table}__{factor.column}"
        if column_name not in df.columns:
            raise ValueError(f"Missing ranking factor column '{column_name}'.")
        values = pd.to_numeric(df[column_name], errors="coerce")
        if factor.direction == "asc":
            values = values * -1
        values = _apply_transforms(values, df["date"], factor.transforms)
        if factor.missingValuePolicy == "zero":
            values = values.fillna(0.0)
        else:
            required_masks.append(values.notna())
        weighted_total = weighted_total.add(values.fillna(0.0) * factor.weight, fill_value=0.0)
        total_weight += factor.weight
    if total_weight <= 0:
        raise ValueError(f"Ranking group '{group.name}' produced zero factor weight.")
    group_score = weighted_total / total_weight
    group_score = _apply_transforms(group_score, df["date"], group.transforms)
    return group_score, required_masks


def _apply_transforms(series: pd.Series, dates: pd.Series, transforms: list[RankingTransform]) -> pd.Series:
    current = pd.to_numeric(series, errors="coerce")
    groups = dates.astype("string")
    for transform in transforms:
        transform_type = transform.type
        params = transform.params
        if transform_type == "coalesce":
            current = current.fillna(params.get("value"))
        elif transform_type == "clip":
            current = current.clip(lower=params.get("lower"), upper=params.get("upper"))
        elif transform_type == "winsorize":
            current = current.groupby(groups, group_keys=False).apply(
                lambda item: _winsorize(
                    item,
                    lower_quantile=_optional_float(params.get("lowerQuantile")),
                    upper_quantile=_optional_float(params.get("upperQuantile")),
                )
            )
        elif transform_type == "log1p":
            current = current.where(current > -1).map(lambda value: math.log1p(value) if pd.notna(value) else np.nan)
        elif transform_type == "negate":
            current = current * -1
        elif transform_type == "abs":
            current = current.abs()
        elif transform_type == "percentile_rank":
            current = current.groupby(groups, group_keys=False).rank(method="average", pct=True)
        elif transform_type == "zscore":
            current = current.groupby(groups, group_keys=False).apply(_zscore)
        elif transform_type == "minmax":
            current = current.groupby(groups, group_keys=False).apply(_minmax)
        else:
            raise ValueError(f"Unsupported transform '{transform_type}'.")
    return current


def _winsorize(series: pd.Series, *, lower_quantile: float | None, upper_quantile: float | None) -> pd.Series:
    lower = series.quantile(lower_quantile) if lower_quantile is not None else None
    upper = series.quantile(upper_quantile) if upper_quantile is not None else None
    return series.clip(lower=lower, upper=upper)


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(0.0, index=series.index)
    return (series - series.mean()) / std


def _minmax(series: pd.Series) -> pd.Series:
    min_value = series.min()
    max_value = series.max()
    if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
        return pd.Series(0.0, index=series.index)
    return (series - min_value) / (max_value - min_value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _write_rankings_to_platinum(
    dsn: str,
    *,
    table_name: str,
    ranked: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> int:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS platinum")
            _ensure_platinum_output_table(cur, table_name)
            cur.execute(
                f"""
                DELETE FROM "platinum".{universe_service._quote_identifier(table_name)}
                WHERE date >= %s AND date <= %s
                """,
                (start_date, end_date),
            )
            if ranked.empty:
                return 0
            last_updated_date = datetime.now(timezone.utc).date()
            rows = (
                (
                    row["date"],
                    str(row["symbol"]),
                    int(row["rank"]),
                    float(row["score"]),
                    last_updated_date,
                )
                for _, row in ranked.iterrows()
            )
            copy_rows(
                cur,
                table=f'"platinum".{universe_service._quote_identifier(table_name)}',
                columns=("date", "symbol", "rank", "score", "last_updated_date"),
                rows=rows,
            )
            return int(len(ranked))


def _ensure_platinum_output_table(cursor: Any, table_name: str) -> None:
    identifier = universe_service._quote_identifier(table_name)
    symbol_date_index = build_scoped_identifier(table_name, "symbol", "date", "idx")
    date_rank_index = build_scoped_identifier(table_name, "date", "rank", "idx")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "platinum".{identifier} (
            date DATE NOT NULL,
            symbol TEXT NOT NULL,
            rank INTEGER NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            last_updated_date DATE NOT NULL DEFAULT CURRENT_DATE,
            PRIMARY KEY (date, symbol)
        )
        """
    )
    cursor.execute(
        f"""
        ALTER TABLE "platinum".{identifier}
        ADD COLUMN IF NOT EXISTS score DOUBLE PRECISION
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {symbol_date_index}
        ON "platinum".{identifier}(symbol, date DESC)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {date_rank_index}
        ON "platinum".{identifier}(date DESC, rank)
        """
    )


def _insert_ranking_run(
    dsn: str,
    *,
    run_id: str,
    strategy_name: str,
    ranking_schema_name: str,
    ranking_schema_version: int,
    output_table_name: str,
    start_date: date,
    end_date: date,
    status: str,
    triggered_by: str,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.ranking_runs (
                    run_id,
                    strategy_name,
                    ranking_schema_name,
                    ranking_schema_version,
                    output_table_name,
                    start_date,
                    end_date,
                    status,
                    triggered_by,
                    started_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    run_id,
                    strategy_name,
                    ranking_schema_name,
                    ranking_schema_version,
                    output_table_name,
                    start_date,
                    end_date,
                    status,
                    triggered_by,
                ),
            )


def _update_ranking_run(
    dsn: str,
    *,
    run_id: str,
    status: str,
    row_count: int,
    date_count: int,
    error: str | None,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE core.ranking_runs
                SET status = %s,
                    row_count = %s,
                    date_count = %s,
                    error = %s,
                    finished_at = NOW()
                WHERE run_id = %s
                """,
                (status, row_count, date_count, error, run_id),
            )


def _upsert_ranking_watermark(
    dsn: str,
    *,
    strategy_name: str,
    ranking_schema_name: str,
    ranking_schema_version: int,
    output_table_name: str,
    last_ranked_date: date,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.ranking_watermarks (
                    strategy_name,
                    ranking_schema_name,
                    ranking_schema_version,
                    output_table_name,
                    last_ranked_date,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (strategy_name)
                DO UPDATE SET
                    ranking_schema_name = EXCLUDED.ranking_schema_name,
                    ranking_schema_version = EXCLUDED.ranking_schema_version,
                    output_table_name = EXCLUDED.output_table_name,
                    last_ranked_date = EXCLUDED.last_ranked_date,
                    updated_at = NOW()
                """,
                (
                    strategy_name,
                    ranking_schema_name,
                    ranking_schema_version,
                    output_table_name,
                    last_ranked_date,
                ),
            )
