from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountSummary,
    TradeCapabilityFlags,
    TradeDataFreshness,
)

from api.service.broker_account_status_refresh_service import BrokerAccountStatusRefreshService
from api.service.settings import BrokerAccountStatusRefreshSettings
from core.trade_desk_repository import TradeAccountRecord


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _account(
    account_id: str = "acct-paper",
    *,
    provider: str = "alpaca",
    environment: str = "paper",
    can_read: bool = False,
) -> TradeAccountSummary:
    return TradeAccountSummary(
        accountId=account_id,
        name=f"{account_id} account",
        provider=provider,
        environment=environment,
        readiness="review",
        readinessReason="Reconnect required.",
        capabilities=TradeCapabilityFlags(
            canReadAccount=can_read,
            canReadPositions=can_read,
            canReadOrders=can_read,
            readOnly=not can_read,
            unsupportedReason=None if can_read else "Reconnect required.",
        ),
        freshness=TradeDataFreshness(),
    )


def _record(account: TradeAccountSummary, *, provider_key: str | None = "provider-key") -> TradeAccountRecord:
    return TradeAccountRecord(
        account=account,
        detail=TradeAccountDetail(account=account),
        providerAccountKey=provider_key,
    )


class FakeRepo:
    def __init__(self, records: list[TradeAccountRecord], *, lock_acquired: bool = True) -> None:
        self.records = {record.account.accountId: record for record in records}
        self.lock_acquired = lock_acquired
        self.saved: list[dict[str, Any]] = []

    def get_account_record(self, account_id: str) -> TradeAccountRecord | None:
        return self.records.get(account_id)

    def list_account_records(self, *, limit: int | None = None) -> list[TradeAccountRecord]:
        records = list(self.records.values())
        return records[:limit] if limit is not None else records

    @contextmanager
    def account_refresh_lock(self, account_id: str):
        yield self.lock_acquired

    def save_account_snapshot(self, **kwargs: Any) -> None:
        self.saved.append(kwargs)
        account = kwargs["account"]
        self.records[account.accountId] = _record(account)


class FakeAlpacaGateway:
    def __init__(self, *, fail_environment: str | None = None) -> None:
        self.fail_environment = fail_environment
        self.account_calls: list[str] = []

    def get_account(self, *, environment: str, subject: str | None) -> dict[str, Any]:
        self.account_calls.append(environment)
        if environment == self.fail_environment:
            raise RuntimeError("provider timeout")
        return {
            "account_number": "PA123456789",
            "currency": "USD",
            "cash": "100000",
            "buying_power": "125000",
            "equity": "150000",
        }

    def list_positions(self, *, environment: str, subject: str | None) -> list[dict[str, Any]]:
        return [
            {
                "symbol": "MSFT",
                "qty": "10",
                "market_value": "4200",
                "avg_entry_price": "390",
            }
        ]

    def list_orders(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "id": "alpaca-order-1",
                "symbol": "MSFT",
                "side": "buy",
                "type": "limit",
                "time_in_force": "day",
                "qty": "2",
                "limit_price": "420",
                "status": "accepted",
            }
        ]


class FakeSchwabGateway:
    def get_session_state(self) -> dict[str, Any]:
        return {"configured": True, "connected": False}


def _service(
    repo: FakeRepo,
    *,
    alpaca_gateway: FakeAlpacaGateway | None = None,
    schwab_gateway: FakeSchwabGateway | None = None,
) -> BrokerAccountStatusRefreshService:
    return BrokerAccountStatusRefreshService(
        repo,  # type: ignore[arg-type]
        BrokerAccountStatusRefreshSettings(batch_size=10),
        alpaca_gateway=alpaca_gateway,  # type: ignore[arg-type]
        schwab_gateway=schwab_gateway,  # type: ignore[arg-type]
    )


def test_alpaca_refresh_persists_connected_snapshot_positions_and_orders() -> None:
    repo = FakeRepo([_record(_account())])
    service = _service(repo, alpaca_gateway=FakeAlpacaGateway())

    outcome = service.refresh_account("acct-paper")

    assert outcome.status == "completed"
    assert len(repo.saved) == 1
    saved = repo.saved[0]
    saved_account = saved["account"]
    assert saved_account.capabilities.canReadAccount is True
    assert saved_account.freshness.balancesState == "fresh"
    assert saved_account.accountNumberMasked == "***6789"
    assert saved_account.cash == 100_000
    assert saved_account.buyingPower == 125_000
    assert saved_account.equity == 150_000
    assert saved_account.positionCount == 1
    assert saved_account.openOrderCount == 1
    assert saved["positions"][0].symbol == "MSFT"
    assert saved["orders"][0].providerOrderId == "alpaca-order-1"


def test_oauth_missing_session_is_saved_as_reconnect_required() -> None:
    repo = FakeRepo([_record(_account("acct-live", provider="schwab", environment="live"))])
    service = _service(repo, schwab_gateway=FakeSchwabGateway())

    response = service.action_response(account_id="acct-live", action="reconnect", trigger="reconnect")

    assert response.status == "completed"
    assert response.resultingConnectionHealth is not None
    assert response.resultingConnectionHealth.connectionState == "reconnect_required"
    assert response.resultingConnectionHealth.authStatus == "reauth_required"
    saved_account = repo.saved[0]["account"]
    assert saved_account.capabilities.canReadAccount is False
    assert "Reconnect required" in str(saved_account.readinessReason)


def test_refresh_lock_reports_in_progress_without_provider_call() -> None:
    gateway = FakeAlpacaGateway()
    repo = FakeRepo([_record(_account())], lock_acquired=False)
    service = _service(repo, alpaca_gateway=gateway)

    outcome = service.refresh_account("acct-paper")

    assert outcome.status == "in_progress"
    assert repo.saved == []
    assert gateway.account_calls == []


def test_batch_refresh_persists_failed_account_and_continues_other_accounts() -> None:
    repo = FakeRepo(
        [
            _record(_account("acct-paper", environment="paper")),
            _record(_account("acct-live", environment="live")),
        ]
    )
    service = _service(repo, alpaca_gateway=FakeAlpacaGateway(fail_environment="live"))

    outcomes = service.refresh_due_accounts(force=True)

    assert [outcome.account.accountId for outcome in outcomes] == ["acct-paper", "acct-live"]
    assert [outcome.status for outcome in outcomes] == ["completed", "failed"]
    assert len(repo.saved) == 2
    failed = repo.saved[1]["account"]
    assert failed.accountId == "acct-live"
    assert failed.capabilities.canReadAccount is False
    assert "provider timeout" in str(failed.readinessReason)
