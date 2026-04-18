from __future__ import annotations

from typing import Dict, Tuple

from asset_allocation_runtime_common.market_data.gold_sync_contracts import get_sync_config
SUPPORTED_GOLD_LOOKUP_TABLES: Tuple[str, ...] = (
    "market_data",
    "finance_data",
    "earnings_data",
    "price_target_data",
    "regime_inputs_daily",
    "regime_history",
    "regime_latest",
    "regime_transitions",
)

TABLE_SOURCE_JOBS: Dict[str, str] = {
    "market_data": "tasks.market_data.gold_market_data",
    "finance_data": "tasks.finance_data.gold_finance_data",
    "earnings_data": "tasks.earnings_data.gold_earnings_data",
    "price_target_data": "tasks.price_target_data.gold_price_target_data",
    "regime_inputs_daily": "tasks.regime_data.gold_regime_data",
    "regime_history": "tasks.regime_data.gold_regime_data",
    "regime_latest": "tasks.regime_data.gold_regime_data",
    "regime_transitions": "tasks.regime_data.gold_regime_data",
}

REGIME_TABLE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "regime_inputs_daily": (
        "as_of_date",
        "spy_close",
        "return_1d",
        "return_20d",
        "rvol_10d_ann",
        "vix_spot_close",
        "vix3m_close",
        "vix_slope",
        "trend_state",
        "curve_state",
        "vix_gt_32_streak",
        "inputs_complete_flag",
        "computed_at",
    ),
    "regime_history": (
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
    ),
    "regime_latest": (
        "model_name",
        "model_version",
        "as_of_date",
        "effective_from_date",
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
    ),
    "regime_transitions": (
        "model_name",
        "model_version",
        "effective_from_date",
        "prior_regime_code",
        "new_regime_code",
        "trigger_rule_id",
        "computed_at",
    ),
}


def expected_gold_lookup_columns() -> Dict[str, Tuple[str, ...]]:
    return {
        "market_data": tuple(get_sync_config("market").columns),
        "finance_data": tuple(get_sync_config("finance").columns),
        "earnings_data": tuple(get_sync_config("earnings").columns),
        "price_target_data": tuple(get_sync_config("price-target").columns),
        **REGIME_TABLE_COLUMNS,
    }
