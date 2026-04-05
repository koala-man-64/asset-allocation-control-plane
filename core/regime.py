from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from pandas.tseries.offsets import BDay
from asset_allocation_contracts.regime import (
    DEFAULT_HALT_REASON,
    DEFAULT_REGIME_MODEL_NAME,
    CurveState,
    RegimeBlockedAction,
    RegimeCode,
    RegimeModelConfig,
    RegimePolicy,
    RegimeStatus,
    TargetGrossExposureByRegime,
    TrendState,
    default_regime_model_config,
)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        out = int(value)
    except (TypeError, ValueError):
        return None
    return out


def compute_trend_state(return_20d: Any, *, config: RegimeModelConfig | Mapping[str, Any] | None = None) -> TrendState:
    cfg = config if isinstance(config, RegimeModelConfig) else RegimeModelConfig.model_validate(config or {})
    value = _safe_float(return_20d)
    if value is None:
        return "near_zero"
    if value > cfg.trendPositiveThreshold:
        return "positive"
    if value < cfg.trendNegativeThreshold:
        return "negative"
    return "near_zero"


def compute_curve_state(vix_slope: Any, *, config: RegimeModelConfig | Mapping[str, Any] | None = None) -> CurveState:
    cfg = config if isinstance(config, RegimeModelConfig) else RegimeModelConfig.model_validate(config or {})
    value = _safe_float(vix_slope)
    if value is None:
        return "flat"
    if value >= cfg.curveContangoThreshold:
        return "contango"
    if value <= cfg.curveInvertedThreshold:
        return "inverted"
    return "flat"


def classify_regime_row(
    row: Mapping[str, Any],
    *,
    prev_confirmed_regime: RegimeCode | None = None,
    config: RegimeModelConfig | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config if isinstance(config, RegimeModelConfig) else RegimeModelConfig.model_validate(config or {})
    inputs_complete = bool(row.get("inputs_complete_flag"))
    if not inputs_complete:
        return {
            "regime_code": "unclassified",
            "regime_status": "unclassified",
            "matched_rule_id": None,
            "halt_flag": False,
            "halt_reason": None,
            "trend_state": compute_trend_state(row.get("return_20d"), config=cfg),
            "curve_state": compute_curve_state(row.get("vix_slope"), config=cfg),
        }

    trend_state = compute_trend_state(row.get("return_20d"), config=cfg)
    curve_state = compute_curve_state(row.get("vix_slope"), config=cfg)
    rvol_10d_ann = _safe_float(row.get("rvol_10d_ann"))
    vix_spot_close = _safe_float(row.get("vix_spot_close"))
    vix_gt_32_streak = _safe_int(row.get("vix_gt_32_streak")) or 0

    halt_flag = bool(
        vix_spot_close is not None
        and vix_spot_close > cfg.haltVixThreshold
        and vix_gt_32_streak >= cfg.haltVixStreakDays
    )

    regime_code: RegimeCode
    regime_status: RegimeStatus
    matched_rule_id: str | None

    if rvol_10d_ann is not None and rvol_10d_ann >= cfg.highVolEnterThreshold and curve_state == "inverted":
        regime_code = "high_vol"
        regime_status = "confirmed"
        matched_rule_id = "high_vol"
    elif rvol_10d_ann is not None and cfg.highVolExitThreshold <= rvol_10d_ann < cfg.highVolEnterThreshold:
        regime_code = prev_confirmed_regime or "unclassified"
        regime_status = "transition" if prev_confirmed_regime else "unclassified"
        matched_rule_id = "transition_band"
    elif (
        rvol_10d_ann is not None
        and cfg.bearVolMin <= rvol_10d_ann < cfg.bearVolMaxExclusive
        and trend_state == "negative"
        and curve_state in {"flat", "inverted"}
    ):
        regime_code = "trending_bear"
        regime_status = "confirmed"
        matched_rule_id = "trending_bear"
    elif (
        rvol_10d_ann is not None
        and rvol_10d_ann < cfg.bullVolMaxExclusive
        and trend_state == "positive"
        and curve_state == "contango"
    ):
        regime_code = "trending_bull"
        regime_status = "confirmed"
        matched_rule_id = "trending_bull"
    elif (
        rvol_10d_ann is not None
        and cfg.choppyVolMin <= rvol_10d_ann < cfg.choppyVolMaxExclusive
        and trend_state == "near_zero"
        and curve_state == "contango"
    ):
        regime_code = "choppy_mean_reversion"
        regime_status = "confirmed"
        matched_rule_id = "choppy_mean_reversion"
    else:
        regime_code = "unclassified"
        regime_status = "unclassified"
        matched_rule_id = None

    return {
        "regime_code": regime_code,
        "regime_status": regime_status,
        "matched_rule_id": matched_rule_id,
        "halt_flag": halt_flag,
        "halt_reason": DEFAULT_HALT_REASON if halt_flag else None,
        "trend_state": trend_state,
        "curve_state": curve_state,
    }


def build_regime_outputs(
    inputs: pd.DataFrame,
    *,
    model_name: str,
    model_version: int,
    config: RegimeModelConfig | Mapping[str, Any] | None = None,
    computed_at: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cfg = config if isinstance(config, RegimeModelConfig) else RegimeModelConfig.model_validate(config or {})
    if inputs.empty:
        empty_history = pd.DataFrame(
            columns=[
                "as_of_date",
                "effective_from_date",
                "model_name",
                "model_version",
                "regime_code",
                "regime_status",
                "matched_rule_id",
                "halt_flag",
                "halt_reason",
                "spy_return_20d",
                "rvol_10d_ann",
                "vix_spot_close",
                "vix3m_close",
                "vix_slope",
                "trend_state",
                "curve_state",
                "vix_gt_32_streak",
                "computed_at",
            ]
        )
        empty_latest = pd.DataFrame(columns=empty_history.columns)
        empty_transitions = pd.DataFrame(
            columns=[
                "model_name",
                "model_version",
                "effective_from_date",
                "prior_regime_code",
                "new_regime_code",
                "trigger_rule_id",
                "computed_at",
            ]
        )
        return empty_history, empty_latest, empty_transitions

    computed = pd.Timestamp(computed_at or datetime.now(timezone.utc))
    frame = inputs.copy()
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["as_of_date"]).sort_values("as_of_date").reset_index(drop=True)

    as_of_dates = frame["as_of_date"].tolist()
    effective_dates: list[date] = []
    for index, as_of_date in enumerate(as_of_dates):
        if index < len(as_of_dates) - 1:
            effective_dates.append(as_of_dates[index + 1])
            continue
        next_business = (pd.Timestamp(as_of_date) + BDay(1)).date()
        effective_dates.append(next_business)
    frame["effective_from_date"] = effective_dates

    history_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    prev_confirmed_regime: RegimeCode | None = None
    last_recorded_confirmed: RegimeCode | None = None

    for row in frame.to_dict("records"):
        classification = classify_regime_row(
            row,
            prev_confirmed_regime=prev_confirmed_regime,
            config=cfg,
        )
        regime_code = classification["regime_code"]
        regime_status = classification["regime_status"]
        matched_rule_id = classification["matched_rule_id"]

        history_rows.append(
            {
                "as_of_date": row["as_of_date"],
                "effective_from_date": row["effective_from_date"],
                "model_name": model_name,
                "model_version": int(model_version),
                "regime_code": regime_code,
                "regime_status": regime_status,
                "matched_rule_id": matched_rule_id,
                "halt_flag": bool(classification["halt_flag"]),
                "halt_reason": classification["halt_reason"],
                "spy_return_20d": _safe_float(row.get("return_20d")),
                "rvol_10d_ann": _safe_float(row.get("rvol_10d_ann")),
                "vix_spot_close": _safe_float(row.get("vix_spot_close")),
                "vix3m_close": _safe_float(row.get("vix3m_close")),
                "vix_slope": _safe_float(row.get("vix_slope")),
                "trend_state": classification["trend_state"],
                "curve_state": classification["curve_state"],
                "vix_gt_32_streak": _safe_int(row.get("vix_gt_32_streak")),
                "computed_at": computed,
            }
        )

        if regime_status == "confirmed":
            prev_confirmed_regime = regime_code
            if last_recorded_confirmed != regime_code:
                transition_rows.append(
                    {
                        "model_name": model_name,
                        "model_version": int(model_version),
                        "effective_from_date": row["effective_from_date"],
                        "prior_regime_code": last_recorded_confirmed,
                        "new_regime_code": regime_code,
                        "trigger_rule_id": matched_rule_id,
                        "computed_at": computed,
                    }
                )
                last_recorded_confirmed = regime_code

    history = pd.DataFrame(history_rows)
    latest = history.tail(1).reset_index(drop=True)
    transitions = pd.DataFrame(transition_rows)
    return history, latest, transitions
