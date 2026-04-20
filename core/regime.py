from __future__ import annotations

"""Compatibility facade for shared regime logic."""

from asset_allocation_runtime_common.domain.regime import (
    DEFAULT_HALT_REASON,
    DEFAULT_REGIME_MODEL_NAME,
    CurveState,
    RegimeCode,
    RegimeModelConfig,
    RegimeStatus,
    TargetGrossExposureByRegime,
    TrendState,
    build_regime_outputs,
    classify_regime_row,
    compute_curve_state,
    compute_trend_state,
    default_regime_model_config,
)

__all__ = [
    "CurveState",
    "DEFAULT_HALT_REASON",
    "DEFAULT_REGIME_MODEL_NAME",
    "RegimeCode",
    "RegimeModelConfig",
    "RegimeStatus",
    "TargetGrossExposureByRegime",
    "TrendState",
    "default_regime_model_config",
    "compute_trend_state",
    "compute_curve_state",
    "classify_regime_row",
    "build_regime_outputs",
]
