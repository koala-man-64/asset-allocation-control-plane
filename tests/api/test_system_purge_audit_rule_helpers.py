from __future__ import annotations

import pytest
from fastapi import HTTPException

from api.endpoints import system
from asset_allocation_runtime_common.foundation.purge_rules import PurgeRule
def _build_rule_from_kwargs(**kwargs) -> PurgeRule:
    actor = kwargs.get("actor")
    return PurgeRule(
        id=77,
        name=str(kwargs["name"]),
        layer=str(kwargs["layer"]),
        domain=str(kwargs["domain"]),
        column_name=str(kwargs["column_name"]),
        operator=str(kwargs["operator"]),
        threshold=float(kwargs["threshold"]),
        run_interval_minutes=int(kwargs["run_interval_minutes"]),
        next_run_at=None,
        last_run_at=None,
        last_status=None,
        last_error=None,
        last_match_count=None,
        last_purge_count=None,
        created_at=None,
        updated_at=None,
        created_by=actor,
        updated_by=actor,
    )


def test_persist_purge_symbols_audit_rule_creates_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_purge_rule(**kwargs):
        captured.update(kwargs)
        return _build_rule_from_kwargs(**kwargs)

    monkeypatch.setattr(system, "create_purge_rule", fake_create_purge_rule)

    audit_rule = system.PurgeRuleAuditRequest(
        layer="silver",
        domain="market",
        column_name="Close",
        operator="lt",
        threshold=1,
        aggregation="avg",
        recent_rows=5,
        expression="avg(Close) over last 5 rows < 1",
        selected_symbol_count=25,
        matched_symbol_count=40,
    )

    persisted = system._persist_purge_symbols_audit_rule(
        dsn="postgresql://test",
        audit_rule=audit_rule,
        actor="tester",
    )

    assert persisted.id == 77
    assert captured["dsn"] == "postgresql://test"
    assert "enabled" not in captured
    assert captured["run_interval_minutes"] == system._PURGE_RULE_AUDIT_INTERVAL_MINUTES
    assert captured["layer"] == "silver"
    assert captured["domain"] == "market"
    assert captured["column_name"] == "Close"
    assert captured["operator"] == "lt"
    assert captured["threshold"] == 1.0
    assert "matched=40" in str(captured["name"])
    assert "selected=25" in str(captured["name"])


def test_persist_purge_symbols_audit_rule_rejects_invalid_percentile_threshold() -> None:
    audit_rule = system.PurgeRuleAuditRequest(
        layer="silver",
        domain="market",
        column_name="Close",
        operator="top_percent",
        threshold=101,
    )

    with pytest.raises(HTTPException) as exc:
        system._persist_purge_symbols_audit_rule(
            dsn="postgresql://test",
            audit_rule=audit_rule,
            actor="tester",
        )

    assert exc.value.status_code == 400
    assert "between 0 and 100" in str(exc.value.detail)
