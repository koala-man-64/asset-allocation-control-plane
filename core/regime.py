from __future__ import annotations

"""Compatibility facade for shared regime logic."""

from asset_allocation_runtime_common.domain.regime import (
    DEFAULT_HALT_REASON,
    DEFAULT_REGIME_MODEL_NAME,
    CurveState,
    RegimeCode,
    RegimeModelConfig,
    RegimeSignal,
    RegimeSignalConfig,
    RegimeSignalState,
    RegimeTransitionType,
    TrendState,
    build_regime_outputs,
    classify_regime_row,
    canonical_default_regime_model_config,
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
    "RegimeSignal",
    "RegimeSignalConfig",
    "RegimeSignalState",
    "RegimeTransitionType",
    "TrendState",
    "canonical_default_regime_model_config",
    "default_regime_model_config",
    "compute_trend_state",
    "compute_curve_state",
    "classify_regime_row",
    "build_regime_outputs",
]
