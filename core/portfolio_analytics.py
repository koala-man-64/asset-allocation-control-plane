from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from asset_allocation_contracts.portfolio import (
    PortfolioForecastAssumption,
    PortfolioForecastConfidence,
    PortfolioForecastHorizon,
    PortfolioForecastResponse,
    PortfolioForecastSampleMode,
    PortfolioHistoryPoint,
    PortfolioNextRebalanceResponse,
    PortfolioRebalanceCadence,
)

_HORIZON_WINDOWS: dict[PortfolioForecastHorizon, int] = {
    "1M": 21,
    "3M": 63,
    "6M": 126,
}
_WEEKDAY_NAMES = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
]


def _normalize_regime_code(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if normalized == "trending_bull":
        return "trending_up"
    if normalized == "trending_bear":
        return "trending_down"
    if normalized in {"choppy_mean_reversion", "choppy"}:
        return "mean_reverting"
    if normalized == "high_vol":
        return "high_volatility"
    return normalized or "unclassified"


def _coerce_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _coerce_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric


def _average(values: Iterable[float]) -> float | None:
    items = list(values)
    if not items:
        return None
    return sum(items) / len(items)


def _percentile(values: Iterable[float], quantile: float) -> float | None:
    items = sorted(float(value) for value in values)
    if not items:
        return None
    index = (len(items) - 1) * quantile
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(items) - 1)
    lower = items[lower_index]
    upper = items[upper_index]
    if lower_index == upper_index:
        return lower
    weight = index - lower_index
    return lower + (upper - lower) * weight


def _regime_date(row: dict[str, Any]) -> date | None:
    return _coerce_date(row.get("as_of_date") or row.get("effective_from_date"))


def _build_regime_lookup(regime_history_rows: list[dict[str, Any]]):
    sorted_rows = sorted(
        [row for row in regime_history_rows if _regime_date(row) is not None],
        key=lambda row: _regime_date(row) or date.min,
    )

    def resolve(target_date: date) -> str:
        resolved = "unclassified"
        for row in sorted_rows:
            row_date = _regime_date(row)
            if row_date is None:
                continue
            if row_date <= target_date:
                active = row.get("active_regimes") or []
                resolved = _normalize_regime_code(active[0] if active else None)
            else:
                break
        return resolved

    return resolve


def _build_benchmark_series(benchmark_rows: list[dict[str, Any]]) -> list[tuple[date, float]]:
    series: list[tuple[date, float]] = []
    for row in benchmark_rows:
        row_date = _coerce_date(row.get("date"))
        close = _coerce_float(row.get("close"))
        if row_date is None or close is None:
            continue
        series.append((row_date, close))
    series.sort(key=lambda item: item[0])
    return series


def _build_samples(
    *,
    history_points: list[PortfolioHistoryPoint],
    benchmark_rows: list[dict[str, Any]],
    regime_history_rows: list[dict[str, Any]],
    horizon: PortfolioForecastHorizon,
) -> tuple[list[dict[str, Any]], bool]:
    sorted_history = sorted(history_points, key=lambda point: point.asOf)
    requested_window = _HORIZON_WINDOWS[horizon]
    window_length = min(requested_window, max(2, len(sorted_history) - 1))
    truncated_window = window_length < requested_window
    if len(sorted_history) <= window_length:
        return [], truncated_window

    benchmark_series = _build_benchmark_series(benchmark_rows)
    regime_for_date = _build_regime_lookup(regime_history_rows)
    benchmark_index = 0
    matched_close: float | None = None
    benchmark_indexed_by_date: dict[date, float] = {}
    benchmark_baseline: float | None = None

    for point in sorted_history:
        while benchmark_index < len(benchmark_series) and benchmark_series[benchmark_index][0] <= point.asOf:
            matched_close = benchmark_series[benchmark_index][1]
            benchmark_index += 1
            if benchmark_baseline is None and matched_close > 0:
                benchmark_baseline = matched_close
        benchmark_indexed_by_date[point.asOf] = matched_close if matched_close is not None else float("nan")

    samples: list[dict[str, Any]] = []
    for start_index in range(0, len(sorted_history) - window_length):
        start = sorted_history[start_index]
        end = sorted_history[start_index + window_length]
        if not start.nav or not end.nav:
            continue

        portfolio_return_pct = round(((end.nav / start.nav) - 1) * 100, 2)
        start_benchmark = benchmark_indexed_by_date.get(start.asOf)
        end_benchmark = benchmark_indexed_by_date.get(end.asOf)
        benchmark_return_pct: float | None = None
        if start_benchmark is not None and end_benchmark is not None:
            if start_benchmark == start_benchmark and end_benchmark == end_benchmark and start_benchmark > 0:
                benchmark_return_pct = round(((end_benchmark / start_benchmark) - 1) * 100, 2)

        samples.append(
            {
                "date": end.asOf,
                "regimeCode": regime_for_date(end.asOf),
                "portfolioReturnPct": portfolio_return_pct,
                "activeReturnPct": (
                    None
                    if benchmark_return_pct is None
                    else round(portfolio_return_pct - benchmark_return_pct, 2)
                ),
            }
        )

    return samples, truncated_window


def _to_confidence(
    sample_size: int,
    sample_mode: PortfolioForecastSampleMode,
    truncated_window: bool,
) -> tuple[PortfolioForecastConfidence, str]:
    confidence: PortfolioForecastConfidence
    if sample_size >= 18 and sample_mode == "regime-conditioned" and not truncated_window:
        confidence = "high"
    elif sample_size >= 10 and sample_mode == "regime-conditioned":
        confidence = "medium"
    elif sample_size >= 4:
        confidence = "low"
    else:
        confidence = "thin"

    label_map = {
        "high": "High confidence",
        "medium": "Medium confidence",
        "low": "Low confidence",
        "thin": "Thin sample",
    }
    return confidence, label_map[confidence]


def derive_portfolio_forecast(
    *,
    account_id: str,
    history_points: list[PortfolioHistoryPoint],
    benchmark_rows: list[dict[str, Any]],
    regime_history_rows: list[dict[str, Any]],
    current_regime_code: str | None,
    benchmark_symbol: str | None,
    model_name: str,
    model_version: int | None,
    horizon: PortfolioForecastHorizon,
    assumption: PortfolioForecastAssumption,
    cost_drag_override_bps: float = 0.0,
) -> PortfolioForecastResponse:
    applied_regime_code = _normalize_regime_code(
        current_regime_code if assumption == "current" else assumption
    )
    samples, truncated_window = _build_samples(
        history_points=history_points,
        benchmark_rows=benchmark_rows,
        regime_history_rows=regime_history_rows,
        horizon=horizon,
    )
    as_of = history_points[-1].asOf if history_points else None
    notes: list[str] = []

    if not samples:
        return PortfolioForecastResponse(
            accountId=account_id,
            asOf=as_of,
            modelName=model_name,
            modelVersion=model_version,
            benchmarkSymbol=benchmark_symbol,
            horizon=horizon,
            assumption=assumption,
            costDragOverrideBps=cost_drag_override_bps,
            expectedReturnPct=None,
            expectedActiveReturnPct=None,
            downsidePct=None,
            upsidePct=None,
            confidence="thin",
            confidenceLabel="Thin sample",
            sampleSize=0,
            sampleMode="insufficient-history",
            appliedRegimeCode=applied_regime_code,
            notes=["Insufficient portfolio history for this horizon."],
        )

    selected_samples = [sample for sample in samples if sample["regimeCode"] == applied_regime_code]
    sample_mode: PortfolioForecastSampleMode = "regime-conditioned"
    if len(selected_samples) < 4:
        selected_samples = samples
        sample_mode = "fallback-history"
        notes.append("Regime sample is thin; falling back to all available history.")
    if truncated_window:
        notes.append("The requested horizon exceeds available history; using the longest local window.")

    return_samples = [float(sample["portfolioReturnPct"]) for sample in selected_samples]
    active_samples = [
        float(sample["activeReturnPct"])
        for sample in selected_samples
        if sample["activeReturnPct"] is not None
    ]
    drag_adjustment_pct = round((float(cost_drag_override_bps or 0) / 100), 2)

    expected_return = _average(return_samples)
    expected_active = _average(active_samples)
    downside = _percentile(return_samples, 0.2)
    upside = _percentile(return_samples, 0.8)
    confidence, confidence_label = _to_confidence(len(selected_samples), sample_mode, truncated_window)

    return PortfolioForecastResponse(
        accountId=account_id,
        asOf=as_of,
        modelName=model_name,
        modelVersion=model_version,
        benchmarkSymbol=benchmark_symbol,
        horizon=horizon,
        assumption=assumption,
        costDragOverrideBps=float(cost_drag_override_bps or 0),
        expectedReturnPct=(
            None if expected_return is None else round(expected_return - drag_adjustment_pct, 2)
        ),
        expectedActiveReturnPct=(
            None if expected_active is None else round(expected_active - drag_adjustment_pct, 2)
        ),
        downsidePct=None if downside is None else round(downside - drag_adjustment_pct, 2),
        upsidePct=None if upside is None else round(upside - drag_adjustment_pct, 2),
        confidence=confidence,
        confidenceLabel=confidence_label,
        sampleSize=len(selected_samples),
        sampleMode=sample_mode,
        appliedRegimeCode=applied_regime_code,
        notes=notes,
    )


def _parse_weekday(anchor_text: str) -> int | None:
    normalized = anchor_text.strip().lower()
    for index, name in enumerate(_WEEKDAY_NAMES):
        if name in normalized:
            return index
    return None


def _parse_month_day(anchor_text: str) -> int | None:
    iso_match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", anchor_text)
    if iso_match:
        return int(iso_match.group(3))

    ordinal_match = re.search(r"\b([12]?\d|3[01])(st|nd|rd|th)?\b", anchor_text, flags=re.IGNORECASE)
    if ordinal_match is None:
        return None
    parsed = int(ordinal_match.group(1))
    if parsed < 1 or parsed > 31:
        return None
    return parsed


def _add_month(base_date: date) -> date:
    month = base_date.month + 1
    year = base_date.year
    if month > 12:
        month = 1
        year += 1
    return date(year, month, 1)


def _next_month_day(base_date: date, month_day: int) -> date:
    next_month = _add_month(base_date)
    safe_day = min(month_day, 28)
    return date(next_month.year, next_month.month, safe_day)


def _next_weekday(base_date: date, weekday: int) -> date:
    offset = (weekday + 7 - base_date.weekday() - 1) % 7 or 7
    return base_date + timedelta(days=offset)


def derive_next_rebalance(
    *,
    account_id: str,
    rebalance_cadence: PortfolioRebalanceCadence,
    anchor_text: str,
    last_materialized_at: datetime | None,
    snapshot_as_of: date | None,
    effective_from: date | None,
    as_of: date | None,
) -> PortfolioNextRebalanceResponse:
    base_date = (
        last_materialized_at.date()
        if last_materialized_at is not None
        else snapshot_as_of or effective_from
    )
    response_kwargs = {
        "accountId": account_id,
        "asOf": as_of,
        "rebalanceCadence": rebalance_cadence,
        "anchorText": anchor_text,
    }

    if base_date is None:
        return PortfolioNextRebalanceResponse(
            **response_kwargs,
            nextDate=None,
            inferred=True,
            basis="unknown",
            reason="No last build date or effective assignment date is available.",
        )

    if rebalance_cadence == "daily":
        return PortfolioNextRebalanceResponse(
            **response_kwargs,
            nextDate=base_date + timedelta(days=1),
            inferred=False,
            basis="cadence",
            reason="Daily cadence advances one trading window from the last build.",
        )

    if rebalance_cadence == "weekly":
        parsed_weekday = _parse_weekday(anchor_text)
        if parsed_weekday is not None:
            return PortfolioNextRebalanceResponse(
                **response_kwargs,
                nextDate=_next_weekday(base_date, parsed_weekday),
                inferred=False,
                basis="anchor",
                reason="Weekly cadence is anchored to the parsed weekday in the rebalance anchor.",
            )
        return PortfolioNextRebalanceResponse(
            **response_kwargs,
            nextDate=base_date + timedelta(days=7),
            inferred=True,
            basis="cadence",
            reason="Anchor text could not be parsed cleanly; using last build plus one weekly cadence.",
        )

    parsed_month_day = _parse_month_day(anchor_text)
    if parsed_month_day is not None:
        return PortfolioNextRebalanceResponse(
            **response_kwargs,
            nextDate=_next_month_day(base_date, parsed_month_day),
            inferred=False,
            basis="anchor",
            reason="Monthly cadence is anchored to the parsed day in the rebalance anchor.",
        )

    return PortfolioNextRebalanceResponse(
        **response_kwargs,
        nextDate=_next_month_day(base_date, base_date.day),
        inferred=True,
        basis="cadence",
        reason="Anchor text could not be parsed cleanly; using last build plus one monthly cadence.",
    )
