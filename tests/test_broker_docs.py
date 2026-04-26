from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_broker_close_position_workflow_is_documented() -> None:
    text = (_repo_root() / "docs" / "ops" / "broker-close-position-workflow.md").read_text(encoding="utf-8")

    assert "does not expose a generic `close-position` endpoint" in text
    assert "GET /api/providers/alpaca/positions" in text
    assert "GET /api/providers/etrade/accounts/{account_key}/portfolio" in text
    assert "GET /api/providers/kalshi/positions" in text
    assert "GET /api/providers/schwab/accounts/{account_number}/positions" in text
    assert "Do not resend the request blindly" in text


def test_schwab_control_plane_runbook_documents_trading_gates_and_recovery() -> None:
    text = (_repo_root() / "docs" / "ops" / "schwab-control-plane.md").read_text(encoding="utf-8")

    assert "`SCHWAB_ENABLED=true`" in text
    assert "`SCHWAB_TRADING_ENABLED=true`" in text
    assert "never written back to `.env`, `.env.web`, setup output, or GitHub sync input" in text
    assert "`POST /api/providers/schwab/accounts/{account_number}/orders/preview`" in text
    assert "The callback and complete routes reject missing, expired, or mismatched OAuth state." in text
    assert "Do not resend the request blindly." in text


def test_kalshi_control_plane_runbook_documents_signing_and_recovery() -> None:
    text = (_repo_root() / "docs" / "ops" / "kalshi-control-plane.md").read_text(encoding="utf-8")

    assert "`KALSHI_ENABLED=true`" in text
    assert "`KALSHI_TRADING_ENABLED=true`" in text
    assert "signs each authenticated request with RSA-PSS" in text
    assert "`GET /api/providers/kalshi/orders/queue-positions`" in text
    assert "`POST /api/providers/kalshi/orders/{order_id}/amend`" in text
    assert "Do not resend the request blindly." in text
