from __future__ import annotations

import json
from datetime import date

from asset_allocation_contracts.portfolio import (
    PortfolioAccount,
    PortfolioAssignment,
    PortfolioDefinition,
    PortfolioDefinitionDetailResponse,
    PortfolioRevision,
    PortfolioSnapshot,
    PortfolioUpsertRequest,
    StrategySliceAttribution,
)

from core.portfolio_repository import PortfolioRepository


class _FakeCursor:
    def __init__(self, *, fetchone_result=None, fetchall_result=None) -> None:
        self.fetchone_result = fetchone_result
        self.fetchall_result = fetchall_result or []
        self.execute_calls: list[tuple[str, tuple[object, ...] | list[object] | None]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _notional_payload() -> PortfolioUpsertRequest:
    return PortfolioUpsertRequest.model_validate(
        {
            "name": "core-book",
            "allocationMode": "notional_base_ccy",
            "allocatableCapital": 100000,
            "allocations": [
                {
                    "sleeveId": "core-growth",
                    "strategy": {"strategyName": "growth", "strategyVersion": 1},
                    "allocationMode": "notional_base_ccy",
                    "targetNotionalBaseCcy": 25000,
                }
            ],
        }
    )


def test_get_portfolio_revision_derives_notional_allocation_weights(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_result=(
            "core-book",
            2,
            "",
            None,
            "notional_base_ccy",
            100000.0,
            [
                {
                    "sleeveId": "core-growth",
                    "strategy": {"strategyName": "growth", "strategyVersion": 1},
                    "targetNotionalBaseCcy": 25000.0,
                }
            ],
            "",
            None,
            None,
            None,
        )
    )
    monkeypatch.setattr("core.portfolio_repository.connect", lambda _dsn: _FakeConnection(cursor))

    revision = PortfolioRepository("postgresql://user:pass@localhost/db").get_portfolio_revision(
        "core-book",
        version=2,
    )

    assert revision is not None
    assert revision.allocationMode == "notional_base_ccy"
    assert revision.allocatableCapital == 100000.0
    assert revision.allocations[0].allocationMode == "notional_base_ccy"
    assert revision.allocations[0].derivedWeight == 0.25


def test_save_portfolio_persists_notional_mode_and_derived_weights(monkeypatch) -> None:
    cursor = _FakeCursor(fetchall_result=[])
    monkeypatch.setattr("core.portfolio_repository.connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(PortfolioRepository, "get_portfolio", lambda self, name: None)
    monkeypatch.setattr(
        PortfolioRepository,
        "get_portfolio_detail",
        lambda self, name: PortfolioDefinitionDetailResponse(
            portfolio=PortfolioDefinition(name=name, status="active", latestVersion=1, activeVersion=1),
            activeRevision=None,
            revisions=[],
        ),
    )

    PortfolioRepository("postgresql://user:pass@localhost/db").save_portfolio(
        payload=_notional_payload(),
        created_by="tester",
    )

    revision_insert = next(sql_params for sql_params in cursor.execute_calls if "INSERT INTO core.portfolio_revisions" in sql_params[0])
    _sql, params = revision_insert
    assert params is not None
    assert params[4] == "notional_base_ccy"
    assert params[5] == 100000.0
    allocations = json.loads(str(params[6]))
    assert allocations[0]["allocationMode"] == "notional_base_ccy"
    assert allocations[0]["targetNotionalBaseCcy"] == 25000.0
    assert allocations[0]["derivedWeight"] == 0.25


def test_create_rebalance_preview_uses_notional_targets(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr("core.portfolio_repository.connect", lambda _dsn: _FakeConnection(cursor))
    account = PortfolioAccount(
        accountId="acct-core",
        name="Core",
        status="active",
        mode="internal_model_managed",
        accountingDepth="position_level",
        cadenceMode="strategy_native",
        baseCurrency="USD",
        inceptionDate=date(2026, 1, 2),
        activePortfolioName="core-book",
        activePortfolioVersion=1,
    )
    revision = PortfolioRevision.model_validate(
        {
            "portfolioName": "core-book",
            "version": 1,
            "allocationMode": "notional_base_ccy",
            "allocatableCapital": 100000,
            "allocations": [
                {
                    "sleeveId": "core-growth",
                    "strategy": {"strategyName": "growth", "strategyVersion": 1},
                    "allocationMode": "notional_base_ccy",
                    "targetNotionalBaseCcy": 50000,
                }
            ],
        }
    )
    snapshot = PortfolioSnapshot(
        accountId="acct-core",
        accountName="Core",
        asOf=date(2026, 4, 26),
        nav=200000.0,
        cash=100000.0,
        grossExposure=0.5,
        netExposure=0.5,
        sinceInceptionPnl=0.0,
        sinceInceptionReturn=0.0,
        currentDrawdown=0.0,
        slices=[
            StrategySliceAttribution(
                asOf=date(2026, 4, 26),
                sleeveId="core-growth",
                strategyName="growth",
                strategyVersion=1,
                targetWeight=0.25,
                actualWeight=0.05,
                marketValue=10000.0,
                grossExposure=0.05,
                netExposure=0.05,
            )
        ],
    )
    monkeypatch.setattr(PortfolioRepository, "get_account", lambda self, account_id: account)
    monkeypatch.setattr(
        PortfolioRepository,
        "get_active_assignment",
        lambda self, account_id, as_of=None: PortfolioAssignment(
            assignmentId="assign-1",
            accountId=account_id,
            accountVersion=1,
            portfolioName="core-book",
            portfolioVersion=1,
            effectiveFrom=date(2026, 1, 2),
            status="active",
        ),
    )
    monkeypatch.setattr(PortfolioRepository, "get_portfolio_revision", lambda self, name, *, version=None: revision)
    monkeypatch.setattr(PortfolioRepository, "get_snapshot", lambda self, account_id: snapshot)

    proposal = PortfolioRepository("postgresql://user:pass@localhost/db").create_rebalance_preview(
        "acct-core",
        as_of=date(2026, 4, 26),
    )

    assert proposal.blocked is False
    assert proposal.estimatedCashImpact == -40000.0
    assert proposal.estimatedTurnover == 0.2
    assert proposal.trades[0].estimatedNotional == 40000.0
