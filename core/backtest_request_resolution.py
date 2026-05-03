from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from asset_allocation_runtime_common import BACKTEST_RESULTS_SCHEMA_VERSION
from pydantic import BaseModel
from api.service.backtest_contracts_compat import StrategyReferenceInput

from core.backtest_runtime import (
    ResolvedBacktestDefinition,
    resolve_backtest_definition,
    resolve_backtest_definition_from_config,
    validate_backtest_submission,
)
from core.strategy_repository import normalize_strategy_config_document

_JSON_SEPARATORS = (",", ":")
_CONFIG_FINGERPRINT_VERSION = 1
_REQUEST_FINGERPRINT_VERSION = 1
_REPLAY_CONFIG_VERSION = 1


@dataclass(frozen=True)
class ResolvedBacktestRequest:
    input_mode: str
    start_ts: datetime
    end_ts: datetime
    bar_size: str
    schedule: list[datetime]
    definition: ResolvedBacktestDefinition
    strategy_ref: StrategyReferenceInput | None
    request_payload: dict[str, Any]
    effective_config: dict[str, Any]
    config_fingerprint: str
    request_fingerprint: str


def _json_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str, separators=_JSON_SEPARATORS)
    return hashlib.md5(encoded.encode("utf-8")).hexdigest()


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_request_dict(value: BaseModel | dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, dict):
        return json.loads(json.dumps(value))
    raise TypeError(f"Unsupported backtest request payload type: {type(value)!r}")


def _frozen_pins(definition: ResolvedBacktestDefinition) -> dict[str, Any]:
    component_refs = definition.strategy_config.componentRefs
    universe_ref = component_refs.universe if component_refs else None
    ranking_ref = component_refs.ranking if component_refs else None
    rebalance_ref = component_refs.rebalance if component_refs else None
    regime_ref = component_refs.regimePolicy if component_refs else None
    risk_ref = component_refs.riskPolicy if component_refs else None
    exit_ref = component_refs.exitPolicy if component_refs else None
    return {
        "strategyName": definition.strategy_name,
        "strategyVersion": definition.strategy_version,
        "universeName": universe_ref.name if universe_ref else definition.ranking_universe_name,
        "universeVersion": universe_ref.version if universe_ref else definition.ranking_universe_version,
        "rankingSchemaName": ranking_ref.name if ranking_ref else definition.ranking_schema_name,
        "rankingSchemaVersion": ranking_ref.version if ranking_ref else definition.ranking_schema_version,
        "rebalancePolicyName": rebalance_ref.name if rebalance_ref else None,
        "rebalancePolicyVersion": rebalance_ref.version if rebalance_ref else None,
        "regimePolicyName": regime_ref.name if regime_ref else definition.strategy_config.regimePolicyConfigName,
        "regimePolicyVersion": regime_ref.version if regime_ref else definition.strategy_config.regimePolicyConfigVersion,
        "riskPolicyName": risk_ref.name if risk_ref else definition.strategy_config.riskPolicyName,
        "riskPolicyVersion": risk_ref.version if risk_ref else definition.strategy_config.riskPolicyVersion,
        "exitPolicyName": exit_ref.name if exit_ref else definition.strategy_config.exitRuleSetName,
        "exitPolicyVersion": exit_ref.version if exit_ref else definition.strategy_config.exitRuleSetVersion,
        "regimeModelName": definition.regime_model_name,
        "regimeModelVersion": definition.regime_model_version,
    }


def _fingerprint_pins(definition: ResolvedBacktestDefinition) -> dict[str, Any]:
    pins = dict(_frozen_pins(definition))
    pins.pop("strategyName", None)
    pins.pop("strategyVersion", None)
    return pins


def resolve_backtest_request(
    dsn: str,
    *,
    strategy_ref: StrategyReferenceInput | dict[str, Any] | None,
    strategy_config: BaseModel | dict[str, Any] | None,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str,
    assumptions: BaseModel | dict[str, Any] | None = None,
) -> ResolvedBacktestRequest:
    normalized_start_ts = _ensure_utc(start_ts)
    normalized_end_ts = _ensure_utc(end_ts)
    if normalized_end_ts <= normalized_start_ts:
        raise ValueError("endTs must be after startTs.")

    normalized_bar_size = str(bar_size or "").strip()
    if not normalized_bar_size:
        raise ValueError("barSize is required.")

    has_strategy_ref = strategy_ref is not None
    has_strategy_config = strategy_config is not None
    if has_strategy_ref == has_strategy_config:
        raise ValueError("Exactly one of strategyRef or strategyConfig must be provided.")

    resolved_strategy_ref = StrategyReferenceInput.model_validate(strategy_ref) if has_strategy_ref else None
    if resolved_strategy_ref is not None:
        definition = resolve_backtest_definition(
            dsn,
            strategy_name=resolved_strategy_ref.strategyName,
            strategy_version=resolved_strategy_ref.strategyVersion,
        )
        normalized_strategy_config = normalize_strategy_config_document(definition.strategy_config_raw)
        input_mode = "strategy_ref"
    else:
        raw_strategy_config = _as_request_dict(strategy_config)
        if raw_strategy_config is None:
            raise ValueError("strategyConfig is required.")
        normalized_strategy_config = normalize_strategy_config_document(raw_strategy_config)
        definition = resolve_backtest_definition_from_config(
            dsn,
            strategy_config_raw=normalized_strategy_config,
        )
        input_mode = "inline"

    schedule = validate_backtest_submission(
        dsn,
        definition=definition,
        start_ts=normalized_start_ts,
        end_ts=normalized_end_ts,
        bar_size=normalized_bar_size,
    )
    pins = _frozen_pins(definition)
    normalized_assumptions = _as_request_dict(assumptions) or {}
    config_fingerprint = _json_hash(
        {
            "schemaVersion": _CONFIG_FINGERPRINT_VERSION,
            "strategyConfig": normalized_strategy_config,
            "pins": _fingerprint_pins(definition),
        }
    )
    request_fingerprint = _json_hash(
        {
            "schemaVersion": _REQUEST_FINGERPRINT_VERSION,
            "configFingerprint": config_fingerprint,
            "startTs": normalized_start_ts.isoformat(),
            "endTs": normalized_end_ts.isoformat(),
            "barSize": normalized_bar_size,
            "assumptions": normalized_assumptions,
            "resultsSchemaVersion": BACKTEST_RESULTS_SCHEMA_VERSION,
        }
    )
    request_payload = {
        "strategyRef": resolved_strategy_ref.model_dump(mode="json") if resolved_strategy_ref is not None else None,
        "strategyConfig": normalized_strategy_config if resolved_strategy_ref is None else None,
        "startTs": normalized_start_ts.isoformat(),
        "endTs": normalized_end_ts.isoformat(),
        "barSize": normalized_bar_size,
        "assumptions": normalized_assumptions or None,
    }
    effective_config = {
        "schemaVersion": _REPLAY_CONFIG_VERSION,
        "inputMode": input_mode,
        "strategyRef": resolved_strategy_ref.model_dump(mode="json") if resolved_strategy_ref is not None else None,
        "strategy": normalized_strategy_config,
        "pins": pins,
        "execution": {
            "startTs": normalized_start_ts.isoformat(),
            "endTs": normalized_end_ts.isoformat(),
            "barSize": normalized_bar_size,
            "barsResolved": len(schedule),
            "assumptions": normalized_assumptions,
        },
        "fingerprints": {
            "configFingerprint": config_fingerprint,
            "requestFingerprint": request_fingerprint,
            "resultsSchemaVersion": BACKTEST_RESULTS_SCHEMA_VERSION,
        },
    }
    return ResolvedBacktestRequest(
        input_mode=input_mode,
        start_ts=normalized_start_ts,
        end_ts=normalized_end_ts,
        bar_size=normalized_bar_size,
        schedule=schedule,
        definition=definition,
        strategy_ref=resolved_strategy_ref,
        request_payload=request_payload,
        effective_config=effective_config,
        config_fingerprint=config_fingerprint,
        request_fingerprint=request_fingerprint,
    )
