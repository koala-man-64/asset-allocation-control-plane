from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from asset_allocation_runtime_common.market_data import domain_artifacts
from core.backtest_repository import BacktestRepository
from core.backtest_runtime import _required_columns, resolve_backtest_definition, validate_backtest_submission
from asset_allocation_runtime_common.foundation.postgres import connect
from core.ranking_engine import service as ranking_service
from core.ranking_engine.contracts import RankingSchemaConfig
from core.ranking_repository import RankingRepository
from core.strategy_engine import StrategyConfig
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

_TABLE_TO_DOMAIN: dict[str, str] = {
    "market_data": "market",
    "finance_data": "finance",
    "earnings_data": "earnings",
    "price_target_data": "price-target",
}
_TRACKED_GOLD_DOMAINS: tuple[str, ...] = ("market", "finance", "earnings", "price-target", "regime")
_CLAIM_TTL = timedelta(minutes=30)
_JSON_SEPARATORS = (",", ":")


@dataclass(frozen=True)
class DomainFreshnessInput:
    domain: str
    fingerprint: str
    artifact_path: str | None
    source_commit: str | None
    published_at: str | None
    affected_as_of_start: date | None
    affected_as_of_end: date | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "fingerprint": self.fingerprint,
            "artifactPath": self.artifact_path,
            "sourceCommit": self.source_commit,
            "publishedAt": self.published_at,
            "affectedAsOfStart": _iso_date(self.affected_as_of_start),
            "affectedAsOfEnd": _iso_date(self.affected_as_of_end),
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        try:
            normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
        return normalized.date()


def _iso_date(value: date | None) -> str | None:
    return value.isoformat() if isinstance(value, date) else None


def _json_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str, separators=_JSON_SEPARATORS)
    return hashlib.md5(encoded.encode("utf-8")).hexdigest()


def _artifact_fingerprint_payload(payload: dict[str, Any], *, start_date: date | None, end_date: date | None) -> dict[str, Any]:
    source_commit = payload.get("sourceCommit")
    return {
        "sourceCommit": source_commit,
        "publishedAt": payload.get("publishedAt"),
        "artifactPath": payload.get("artifactPath"),
        "dataPath": payload.get("dataPath"),
        "activeDataPrefix": payload.get("activeDataPrefix"),
        "manifestPath": payload.get("manifestPath"),
        "affectedAsOfStart": _iso_date(start_date),
        "affectedAsOfEnd": _iso_date(end_date),
        "dateRange": payload.get("dateRange") if source_commit is None else None,
        "updatedAt": payload.get("updatedAt") if source_commit is None else None,
    }


def _load_domain_input(domain: str) -> DomainFreshnessInput:
    payload = domain_artifacts.load_domain_artifact(layer="gold", domain=domain) or {}
    date_range = payload.get("dateRange") if isinstance(payload.get("dateRange"), dict) else {}
    start_date = _normalize_date(payload.get("affectedAsOfStart") or date_range.get("min"))
    end_date = _normalize_date(payload.get("affectedAsOfEnd") or date_range.get("max"))
    return DomainFreshnessInput(
        domain=domain,
        fingerprint=_json_hash(_artifact_fingerprint_payload(payload, start_date=start_date, end_date=end_date)),
        artifact_path=_normalize_text(payload.get("artifactPath")),
        source_commit=_normalize_text(payload.get("sourceCommit")),
        published_at=_normalize_text(payload.get("publishedAt") or payload.get("updatedAt")),
        affected_as_of_start=start_date,
        affected_as_of_end=end_date,
    )


def _load_domain_inputs() -> dict[str, DomainFreshnessInput]:
    return {domain: _load_domain_input(domain) for domain in _TRACKED_GOLD_DOMAINS}


def _domain_names_for_required_columns(required_columns: dict[str, set[str]]) -> list[str]:
    names = {
        domain
        for table_name, domain in _TABLE_TO_DOMAIN.items()
        if required_columns.get(table_name)
    }
    return sorted(names)


def _merge_date_window(
    current_start: date | None,
    current_end: date | None,
    next_start: date | None,
    next_end: date | None,
) -> tuple[date | None, date | None]:
    if next_start is None or next_end is None:
        return current_start, current_end
    if current_start is None or current_end is None:
        return next_start, next_end
    return min(current_start, next_start), max(current_end, next_end)


def _ranking_row_from_db(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if not row:
        return None
    columns = [
        "strategy_name",
        "dependency_fingerprint",
        "dependency_state",
        "dirty_start_date",
        "dirty_end_date",
        "status",
        "claim_token",
        "claimed_by",
        "claimed_at",
        "claim_expires_at",
        "last_materialized_fingerprint",
        "last_materialized_state",
        "last_materialized_at",
        "last_run_id",
        "last_error",
        "updated_at",
    ]
    payload = dict(zip(columns, row))
    payload["dependency_state"] = payload.get("dependency_state") if isinstance(payload.get("dependency_state"), dict) else {}
    payload["last_materialized_state"] = (
        payload.get("last_materialized_state") if isinstance(payload.get("last_materialized_state"), dict) else {}
    )
    return payload


def _canonical_target_from_db(row: tuple[Any, ...]) -> dict[str, Any]:
    columns = [
        "target_id",
        "strategy_name",
        "start_ts",
        "end_ts",
        "bar_size",
        "enabled",
        "last_applied_fingerprint",
        "last_dependency_state",
        "last_enqueued_fingerprint",
        "last_enqueued_at",
        "last_run_id",
        "last_completed_at",
        "created_at",
        "updated_at",
    ]
    payload = dict(zip(columns, row))
    if not isinstance(payload.get("last_dependency_state"), dict):
        payload["last_dependency_state"] = {}
    return payload


def _get_ranking_refresh_state(dsn: str, strategy_name: str) -> dict[str, Any] | None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    strategy_name,
                    dependency_fingerprint,
                    dependency_state,
                    dirty_start_date,
                    dirty_end_date,
                    status,
                    claim_token,
                    claimed_by,
                    claimed_at,
                    claim_expires_at,
                    last_materialized_fingerprint,
                    last_materialized_state,
                    last_materialized_at,
                    last_run_id,
                    last_error,
                    updated_at
                FROM core.ranking_refresh_state
                WHERE strategy_name = %s
                """,
                (strategy_name,),
            )
            return _ranking_row_from_db(cur.fetchone())


def _upsert_ranking_refresh_state(
    dsn: str,
    *,
    strategy_name: str,
    dependency_fingerprint: str | None,
    dependency_state: dict[str, Any],
    dirty_start_date: date | None,
    dirty_end_date: date | None,
    status: str,
    claim_token: str | None = None,
    claimed_by: str | None = None,
    claimed_at: datetime | None = None,
    claim_expires_at: datetime | None = None,
    last_materialized_fingerprint: str | None = None,
    last_materialized_state: dict[str, Any] | None = None,
    last_materialized_at: datetime | None = None,
    last_run_id: str | None = None,
    last_error: str | None = None,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.ranking_refresh_state (
                    strategy_name,
                    dependency_fingerprint,
                    dependency_state,
                    dirty_start_date,
                    dirty_end_date,
                    status,
                    claim_token,
                    claimed_by,
                    claimed_at,
                    claim_expires_at,
                    last_materialized_fingerprint,
                    last_materialized_state,
                    last_materialized_at,
                    last_run_id,
                    last_error,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (strategy_name)
                DO UPDATE SET
                    dependency_fingerprint = EXCLUDED.dependency_fingerprint,
                    dependency_state = EXCLUDED.dependency_state,
                    dirty_start_date = EXCLUDED.dirty_start_date,
                    dirty_end_date = EXCLUDED.dirty_end_date,
                    status = EXCLUDED.status,
                    claim_token = EXCLUDED.claim_token,
                    claimed_by = EXCLUDED.claimed_by,
                    claimed_at = EXCLUDED.claimed_at,
                    claim_expires_at = EXCLUDED.claim_expires_at,
                    last_materialized_fingerprint = COALESCE(EXCLUDED.last_materialized_fingerprint, core.ranking_refresh_state.last_materialized_fingerprint),
                    last_materialized_state = COALESCE(EXCLUDED.last_materialized_state, core.ranking_refresh_state.last_materialized_state),
                    last_materialized_at = COALESCE(EXCLUDED.last_materialized_at, core.ranking_refresh_state.last_materialized_at),
                    last_run_id = COALESCE(EXCLUDED.last_run_id, core.ranking_refresh_state.last_run_id),
                    last_error = EXCLUDED.last_error,
                    updated_at = NOW()
                """,
                (
                    strategy_name,
                    dependency_fingerprint,
                    json.dumps(dependency_state, sort_keys=True),
                    dirty_start_date,
                    dirty_end_date,
                    status,
                    claim_token,
                    claimed_by,
                    claimed_at,
                    claim_expires_at,
                    last_materialized_fingerprint,
                    json.dumps(last_materialized_state, sort_keys=True) if last_materialized_state is not None else None,
                    last_materialized_at,
                    last_run_id,
                    last_error[:4000] if isinstance(last_error, str) else None,
                ),
            )


def _list_canonical_targets(dsn: str) -> list[dict[str, Any]]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    target_id,
                    strategy_name,
                    start_ts,
                    end_ts,
                    bar_size,
                    enabled,
                    last_applied_fingerprint,
                    last_dependency_state,
                    last_enqueued_fingerprint,
                    last_enqueued_at,
                    last_run_id,
                    last_completed_at,
                    created_at,
                    updated_at
                FROM core.canonical_backtest_targets
                ORDER BY target_id
                """
            )
            return [_canonical_target_from_db(row) for row in cur.fetchall()]


def _update_canonical_target_state(
    dsn: str,
    *,
    target_id: str,
    last_dependency_state: dict[str, Any],
    last_enqueued_fingerprint: str | None = None,
    last_enqueued_at: datetime | None = None,
    last_run_id: str | None = None,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE core.canonical_backtest_targets
                SET
                    last_dependency_state = %s,
                    last_enqueued_fingerprint = COALESCE(%s, last_enqueued_fingerprint),
                    last_enqueued_at = COALESCE(%s, last_enqueued_at),
                    last_run_id = COALESCE(%s, last_run_id),
                    updated_at = NOW()
                WHERE target_id = %s
                """,
                (
                    json.dumps(last_dependency_state, sort_keys=True),
                    last_enqueued_fingerprint,
                    last_enqueued_at,
                    last_run_id,
                    target_id,
                ),
            )


def _clear_ranking_refresh_state(dsn: str, *, strategy_name: str) -> None:
    existing = _get_ranking_refresh_state(dsn, strategy_name)
    if not existing:
        return
    _upsert_ranking_refresh_state(
        dsn,
        strategy_name=strategy_name,
        dependency_fingerprint=existing.get("dependency_fingerprint"),
        dependency_state=existing.get("dependency_state") or {},
        dirty_start_date=None,
        dirty_end_date=None,
        status="idle",
        claim_token=None,
        claimed_by=None,
        claimed_at=None,
        claim_expires_at=None,
        last_materialized_fingerprint=existing.get("last_materialized_fingerprint"),
        last_materialized_state=existing.get("last_materialized_state") or {},
        last_materialized_at=existing.get("last_materialized_at"),
        last_run_id=existing.get("last_run_id"),
        last_error=None,
    )


def _ranking_structural_state(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy": payload.get("strategy"),
        "ranking": payload.get("ranking"),
        "universe": payload.get("universe"),
        "domains": sorted((payload.get("domains") or {}).keys()),
    }


def _build_ranking_dependency_state(
    dsn: str,
    *,
    strategy_name: str,
    domain_inputs: dict[str, DomainFreshnessInput],
) -> dict[str, Any] | None:
    strategy_repo = StrategyRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    universe_repo = UniverseRepository(dsn)

    strategy_record = strategy_repo.get_strategy(strategy_name)
    if not strategy_record:
        return None

    raw_strategy_config = strategy_record.get("config") or {}
    strategy_config = StrategyConfig.model_validate(raw_strategy_config)
    ranking_schema_name = str(strategy_config.rankingSchemaName or "").strip()
    if not ranking_schema_name:
        return None

    strategy_revision = strategy_repo.get_strategy_revision(strategy_name)
    ranking_schema_record = ranking_repo.get_ranking_schema(ranking_schema_name)
    if not strategy_revision or not ranking_schema_record:
        return None

    ranking_schema_version = int(strategy_revision.get("ranking_schema_version") or ranking_schema_record.get("version") or 0)
    ranking_revision = ranking_repo.get_ranking_schema_revision(ranking_schema_name, version=ranking_schema_version)
    if not ranking_revision:
        return None

    ranking_schema = RankingSchemaConfig.model_validate(
        ranking_revision.get("config") or ranking_schema_record.get("config") or {}
    )
    strategy_universe = ranking_service._resolve_strategy_universe(dsn, strategy_config)
    ranking_universe = ranking_service._resolve_ranking_universe(dsn, ranking_schema)
    required_columns = ranking_service._collect_required_columns(strategy_universe, ranking_universe, ranking_schema)
    required_domains = _domain_names_for_required_columns(required_columns)
    universe_name = (
        _normalize_text(strategy_revision.get("universe_name"))
        or _normalize_text(strategy_config.universeConfigName)
        or _normalize_text(ranking_schema.universeConfigName)
    )
    universe_version = int(strategy_revision.get("universe_version") or 0)
    universe_revision = (
        universe_repo.get_universe_config_revision(universe_name, version=universe_version)
        if universe_name and universe_version > 0
        else None
    )
    domain_state = {domain: domain_inputs[domain].to_payload() for domain in required_domains if domain in domain_inputs}
    return {
        "strategy": {
            "name": strategy_name,
            "version": int(strategy_revision.get("version") or 0),
            "configHash": _normalize_text(strategy_revision.get("config_hash")),
        },
        "ranking": {
            "name": ranking_schema_name,
            "version": ranking_schema_version,
            "configHash": _normalize_text(ranking_revision.get("config_hash")),
        },
        "universe": {
            "name": universe_name,
            "version": universe_version if universe_version > 0 else None,
            "configHash": _normalize_text((universe_revision or {}).get("config_hash")),
        },
        "domains": domain_state,
    }


def _ranking_dirty_window(
    previous_state: dict[str, Any] | None,
    current_state: dict[str, Any],
) -> tuple[date | None, date | None]:
    if not previous_state:
        return _ranking_full_window(current_state)

    if _json_hash(_ranking_structural_state(previous_state)) != _json_hash(_ranking_structural_state(current_state)):
        return _ranking_full_window(current_state)

    previous_domains = previous_state.get("domains") or {}
    current_domains = current_state.get("domains") or {}
    dirty_start: date | None = None
    dirty_end: date | None = None
    for domain, payload in current_domains.items():
        previous_payload = previous_domains.get(domain) if isinstance(previous_domains, dict) else None
        if not isinstance(payload, dict):
            continue
        if previous_payload == payload:
            continue
        dirty_start, dirty_end = _merge_date_window(
            dirty_start,
            dirty_end,
            _normalize_date(payload.get("affectedAsOfStart")),
            _normalize_date(payload.get("affectedAsOfEnd")),
        )
    return dirty_start, dirty_end


def _ranking_full_window(state: dict[str, Any]) -> tuple[date | None, date | None]:
    dirty_start: date | None = None
    dirty_end: date | None = None
    for payload in (state.get("domains") or {}).values():
        if not isinstance(payload, dict):
            continue
        dirty_start, dirty_end = _merge_date_window(
            dirty_start,
            dirty_end,
            _normalize_date(payload.get("affectedAsOfStart")),
            _normalize_date(payload.get("affectedAsOfEnd")),
        )
    return dirty_start, dirty_end


def _build_canonical_target_state(
    dsn: str,
    *,
    target: dict[str, Any],
    domain_inputs: dict[str, DomainFreshnessInput],
) -> tuple[dict[str, Any], dict[str, Any]]:
    definition = resolve_backtest_definition(dsn, strategy_name=str(target["strategy_name"]))
    validate_backtest_submission(
        dsn,
        definition=definition,
        start_ts=target["start_ts"],
        end_ts=target["end_ts"],
        bar_size=str(target.get("bar_size") or "").strip() or None,
    )
    required_columns = _required_columns(definition)
    required_domains = _domain_names_for_required_columns(required_columns)
    if definition.regime_model_name:
        required_domains = sorted(set(required_domains).union({"regime"}))
    domain_state = {domain: domain_inputs[domain].to_payload() for domain in required_domains if domain in domain_inputs}
    state = {
        "target": {
            "targetId": str(target["target_id"]),
            "strategyName": definition.strategy_name,
            "startTs": target["start_ts"].isoformat(),
            "endTs": target["end_ts"].isoformat(),
            "barSize": target["bar_size"],
        },
        "strategy": {
            "name": definition.strategy_name,
            "version": definition.strategy_version,
        },
        "ranking": {
            "name": definition.ranking_schema_name,
            "version": definition.ranking_schema_version,
        },
        "universe": {
            "name": definition.ranking_universe_name,
            "version": definition.ranking_universe_version,
        },
        "regime": {
            "name": definition.regime_model_name,
            "version": definition.regime_model_version,
        },
        "domains": domain_state,
    }
    effective_config = {
        "strategy": definition.strategy_config_raw,
        "pins": {
            "strategyName": definition.strategy_name,
            "strategyVersion": definition.strategy_version,
            "rankingSchemaName": definition.ranking_schema_name,
            "rankingSchemaVersion": definition.ranking_schema_version,
            "universeName": definition.ranking_universe_name,
            "universeVersion": definition.ranking_universe_version,
            "regimeModelName": definition.regime_model_name,
            "regimeModelVersion": definition.regime_model_version,
        },
        "execution": {
            "startTs": target["start_ts"].isoformat(),
            "endTs": target["end_ts"].isoformat(),
            "barSize": target["bar_size"],
        },
        "freshness": {
            "canonicalTargetId": target["target_id"],
            "dependencyFingerprint": _json_hash(state),
        },
    }
    return state, effective_config


def reconcile_results_freshness(dsn: str, *, dry_run: bool = False) -> dict[str, Any]:
    domain_inputs = _load_domain_inputs()
    strategy_repo = StrategyRepository(dsn)
    backtest_repo = BacktestRepository(dsn)
    ranking_dirty_count = 0
    ranking_noop_count = 0
    canonical_enqueued_count = 0
    canonical_up_to_date_count = 0
    canonical_skipped_count = 0
    errors: list[str] = []

    for strategy in strategy_repo.list_strategies():
        strategy_name = _normalize_text(strategy.get("name"))
        if not strategy_name:
            continue
        try:
            current_state = _build_ranking_dependency_state(dsn, strategy_name=strategy_name, domain_inputs=domain_inputs)
            if current_state is None:
                if not dry_run:
                    _clear_ranking_refresh_state(dsn, strategy_name=strategy_name)
                ranking_noop_count += 1
                continue
            current_fingerprint = _json_hash(current_state)
            existing = _get_ranking_refresh_state(dsn, strategy_name)
            dirty_start, dirty_end = _ranking_dirty_window(existing.get("dependency_state") if existing else None, current_state)
            merged_start = existing.get("dirty_start_date") if existing else None
            merged_end = existing.get("dirty_end_date") if existing else None
            merged_start, merged_end = _merge_date_window(merged_start, merged_end, dirty_start, dirty_end)
            if merged_start and merged_end:
                ranking_dirty_count += 1
                status = "claimed" if existing and existing.get("status") == "claimed" and existing.get("claim_expires_at") and existing["claim_expires_at"] > _utc_now() else "dirty"
            else:
                ranking_noop_count += 1
                status = "idle"
            if dry_run:
                continue
            _upsert_ranking_refresh_state(
                dsn,
                strategy_name=strategy_name,
                dependency_fingerprint=current_fingerprint,
                dependency_state=current_state,
                dirty_start_date=merged_start if status != "idle" else None,
                dirty_end_date=merged_end if status != "idle" else None,
                status=status,
                claim_token=existing.get("claim_token") if status == "claimed" and existing else None,
                claimed_by=existing.get("claimed_by") if status == "claimed" and existing else None,
                claimed_at=existing.get("claimed_at") if status == "claimed" and existing else None,
                claim_expires_at=existing.get("claim_expires_at") if status == "claimed" and existing else None,
                last_materialized_fingerprint=existing.get("last_materialized_fingerprint") if existing else None,
                last_materialized_state=existing.get("last_materialized_state") if existing else None,
                last_materialized_at=existing.get("last_materialized_at") if existing else None,
                last_run_id=existing.get("last_run_id") if existing else None,
                last_error=None if status in {"idle", "dirty"} else existing.get("last_error"),
            )
        except Exception as exc:
            logger.exception("Ranking freshness reconcile failed for strategy '%s'.", strategy_name)
            errors.append(f"ranking:{strategy_name}:{exc}")

    for target in _list_canonical_targets(dsn):
        if not bool(target.get("enabled")):
            continue
        try:
            state, effective_config = _build_canonical_target_state(dsn, target=target, domain_inputs=domain_inputs)
            fingerprint = _json_hash(state)
            if target.get("last_applied_fingerprint") == fingerprint:
                canonical_up_to_date_count += 1
                if not dry_run:
                    _update_canonical_target_state(
                        dsn,
                        target_id=str(target["target_id"]),
                        last_dependency_state=state,
                    )
                continue

            existing_run = backtest_repo.find_latest_canonical_run(
                target_id=str(target["target_id"]),
                fingerprint=fingerprint,
            )
            if existing_run and str(existing_run.get("status") or "").strip() in {"queued", "running", "completed"}:
                canonical_skipped_count += 1
                if not dry_run:
                    _update_canonical_target_state(
                        dsn,
                        target_id=str(target["target_id"]),
                        last_dependency_state=state,
                        last_enqueued_fingerprint=fingerprint,
                        last_enqueued_at=existing_run.get("submitted_at") or _utc_now(),
                        last_run_id=_normalize_text(existing_run.get("run_id")),
                    )
                continue

            canonical_enqueued_count += 1
            if dry_run:
                continue

            run = backtest_repo.create_run(
                config={
                    "mode": "canonical",
                    "targetId": target["target_id"],
                    "strategyName": target["strategy_name"],
                    "startTs": target["start_ts"].isoformat(),
                    "endTs": target["end_ts"].isoformat(),
                    "barSize": target["bar_size"],
                },
                effective_config=effective_config,
                status="queued",
                run_name=f"canonical-{target['target_id']}",
                start_ts=target["start_ts"],
                end_ts=target["end_ts"],
                bar_size=target["bar_size"],
                strategy_name=effective_config["pins"]["strategyName"],
                strategy_version=effective_config["pins"]["strategyVersion"],
                ranking_schema_name=effective_config["pins"]["rankingSchemaName"],
                ranking_schema_version=effective_config["pins"]["rankingSchemaVersion"],
                universe_name=effective_config["pins"]["universeName"],
                universe_version=effective_config["pins"]["universeVersion"],
                regime_model_name=effective_config["pins"]["regimeModelName"],
                regime_model_version=effective_config["pins"]["regimeModelVersion"],
                canonical_target_id=str(target["target_id"]),
                canonical_fingerprint=fingerprint,
                submitted_by="results-reconcile",
            )
            _update_canonical_target_state(
                dsn,
                target_id=str(target["target_id"]),
                last_dependency_state=state,
                last_enqueued_fingerprint=fingerprint,
                last_enqueued_at=_utc_now(),
                last_run_id=_normalize_text(run.get("run_id")),
            )
        except Exception as exc:
            logger.exception("Canonical backtest freshness reconcile failed for target '%s'.", target.get("target_id"))
            errors.append(f"canonical:{target.get('target_id')}:{exc}")

    return {
        "dryRun": dry_run,
        "rankingDirtyCount": ranking_dirty_count,
        "rankingNoopCount": ranking_noop_count,
        "canonicalEnqueuedCount": canonical_enqueued_count,
        "canonicalUpToDateCount": canonical_up_to_date_count,
        "canonicalSkippedCount": canonical_skipped_count,
        "errorCount": len(errors),
        "errors": errors,
    }


def claim_next_ranking_refresh(dsn: str, *, execution_name: str | None = None) -> dict[str, Any] | None:
    claim_token = uuid.uuid4().hex
    claimed_at = _utc_now()
    claim_expires_at = claimed_at + _CLAIM_TTL
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    strategy_name,
                    dependency_fingerprint,
                    dependency_state,
                    dirty_start_date,
                    dirty_end_date
                FROM core.ranking_refresh_state
                WHERE dirty_start_date IS NOT NULL
                  AND dirty_end_date IS NOT NULL
                  AND (
                    status IN ('dirty', 'failed')
                    OR (
                      status = 'claimed'
                      AND (claim_expires_at IS NULL OR claim_expires_at <= NOW())
                    )
                  )
                ORDER BY dirty_start_date ASC, updated_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            strategy_name = str(row[0])
            dependency_fingerprint = _normalize_text(row[1])
            dependency_state = row[2] if isinstance(row[2], dict) else {}
            dirty_start_date = row[3]
            dirty_end_date = row[4]
            cur.execute(
                """
                UPDATE core.ranking_refresh_state
                SET
                    status = 'claimed',
                    claim_token = %s,
                    claimed_by = %s,
                    claimed_at = %s,
                    claim_expires_at = %s,
                    last_error = NULL,
                    updated_at = NOW()
                WHERE strategy_name = %s
                """,
                (
                    claim_token,
                    _normalize_text(execution_name),
                    claimed_at,
                    claim_expires_at,
                    strategy_name,
                ),
            )
    return {
        "strategyName": strategy_name,
        "startDate": _iso_date(dirty_start_date),
        "endDate": _iso_date(dirty_end_date),
        "claimToken": claim_token,
        "dependencyFingerprint": dependency_fingerprint,
        "dependencyState": dependency_state,
    }


def complete_ranking_refresh(
    dsn: str,
    *,
    strategy_name: str,
    claim_token: str,
    run_id: str | None,
    dependency_fingerprint: str | None,
    dependency_state: dict[str, Any] | None,
) -> dict[str, Any]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dependency_fingerprint, dependency_state, dirty_start_date, dirty_end_date
                FROM core.ranking_refresh_state
                WHERE strategy_name = %s AND claim_token = %s
                FOR UPDATE
                """,
                (strategy_name, claim_token),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError(f"Ranking refresh claim not found for strategy '{strategy_name}'.")
            current_dependency_fingerprint = _normalize_text(row[0])
            current_dependency_state = row[1] if isinstance(row[1], dict) else {}
            current_dirty_start = row[2]
            current_dirty_end = row[3]
            clear_dirty = current_dependency_fingerprint == _normalize_text(dependency_fingerprint)
            cur.execute(
                """
                UPDATE core.ranking_refresh_state
                SET
                    status = %s,
                    dirty_start_date = %s,
                    dirty_end_date = %s,
                    claim_token = NULL,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL,
                    last_materialized_fingerprint = %s,
                    last_materialized_state = %s,
                    last_materialized_at = NOW(),
                    last_run_id = %s,
                    last_error = NULL,
                    updated_at = NOW()
                WHERE strategy_name = %s
                """,
                (
                    "idle" if clear_dirty else "dirty",
                    None if clear_dirty else current_dirty_start,
                    None if clear_dirty else current_dirty_end,
                    _normalize_text(dependency_fingerprint),
                    json.dumps(dependency_state if isinstance(dependency_state, dict) else current_dependency_state, sort_keys=True),
                    _normalize_text(run_id),
                    strategy_name,
                ),
            )
    return {"status": "ok", "strategyName": strategy_name, "currentDependencyFingerprint": current_dependency_fingerprint}


def fail_ranking_refresh(
    dsn: str,
    *,
    strategy_name: str,
    claim_token: str,
    error: str,
) -> dict[str, Any]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE core.ranking_refresh_state
                SET
                    status = 'failed',
                    claim_token = NULL,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL,
                    last_error = %s,
                    updated_at = NOW()
                WHERE strategy_name = %s AND claim_token = %s
                RETURNING strategy_name
                """,
                (str(error)[:4000], strategy_name, claim_token),
            )
            if not cur.fetchone():
                raise LookupError(f"Ranking refresh claim not found for strategy '{strategy_name}'.")
    return {"status": "ok", "strategyName": strategy_name}
