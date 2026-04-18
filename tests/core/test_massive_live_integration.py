from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
import requests

from massive_provider import MassiveClient, MassiveConfig


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_repo_env_value(name: str) -> str:
    for filename in (".env", ".env.web"):
        env_path = _repo_root() / filename
        if not env_path.exists():
            continue

        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() != name:
                    continue
                return value.strip().strip('"').strip("'")
        except OSError:
            continue
    return ""


def _live_env(name: str, *, default: str = "") -> str:
    value = str(os.getenv(name, "")).strip()
    if value:
        return value
    repo_value = _read_repo_env_value(name)
    return repo_value or default


def _live_enabled() -> bool:
    run_flag = _live_env("RUN_LIVE_MASSIVE_TESTS").strip().lower()
    api_key = _live_env("MASSIVE_API_KEY")
    return run_flag in {"1", "true", "t", "yes", "y", "on"} and bool(api_key)


def test_live_env_reads_repo_env_web_when_process_env_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tests.core.test_massive_live_integration._repo_root", lambda: tmp_path)
    monkeypatch.delenv("RUN_LIVE_MASSIVE_TESTS", raising=False)
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    (tmp_path / ".env.web").write_text(
        "RUN_LIVE_MASSIVE_TESTS=1\nMASSIVE_API_KEY=test-massive-key\n",
        encoding="utf-8",
    )

    assert _live_env("RUN_LIVE_MASSIVE_TESTS") == "1"
    assert _live_env("MASSIVE_API_KEY") == "test-massive-key"


def test_live_enabled_accepts_repo_env_web_opt_in(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tests.core.test_massive_live_integration._repo_root", lambda: tmp_path)
    monkeypatch.delenv("RUN_LIVE_MASSIVE_TESTS", raising=False)
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    (tmp_path / ".env.web").write_text(
        "RUN_LIVE_MASSIVE_TESTS=true\nMASSIVE_API_KEY=test-massive-key\n",
        encoding="utf-8",
    )

    assert _live_enabled() is True


def _live_config() -> MassiveConfig:
    return MassiveConfig(
        api_key=_live_env("MASSIVE_API_KEY"),
        base_url=_live_env("MASSIVE_BASE_URL", default="https://api.massive.com"),
        timeout_seconds=float(_live_env("MASSIVE_TIMEOUT_SECONDS", default="30.0")),
        prefer_official_sdk=False,
    )


def _request_json(
    session: requests.Session,
    *,
    base_url: str,
    path: str,
    api_key: str,
    timeout_seconds: float,
) -> tuple[int, Any]:
    response = session.get(
        f"{str(base_url).rstrip('/')}{path}",
        params={"apiKey": api_key},
        timeout=float(timeout_seconds),
    )
    try:
        payload: Any = response.json()
    except ValueError:
        payload = response.text
    return response.status_code, payload


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(
    not _live_enabled(),
    reason="Set RUN_LIVE_MASSIVE_TESTS=1 and MASSIVE_API_KEY in the environment or repo .env/.env.web to run live Massive integration tests.",
)
def test_live_snapshot_and_daily_calls_succeed_without_mocking() -> None:
    cfg = _live_config()
    requested = {"AAPL", "MSFT"}

    with MassiveClient(cfg) as client:
        snapshot = client.get_unified_snapshot(tickers=sorted(requested), asset_type="stocks")
        rows = snapshot.get("results") if isinstance(snapshot, dict) else None
        assert isinstance(rows, list) and rows

        returned = {
            str(row.get("ticker") or "").strip().upper()
            for row in rows
            if isinstance(row, dict) and str(row.get("ticker") or "").strip()
        }
        assert returned
        assert returned.issubset(requested)
        assert requested.intersection(returned)

        end_date = datetime.now(timezone.utc).date().isoformat()
        bars = client.list_ohlcv(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="1970-01-01",
            to=end_date,
            adjusted=True,
            sort="asc",
            limit=10,
            pagination=False,
        )
        assert isinstance(bars, list)
        assert len(bars) > 0


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(
    not _live_enabled(),
    reason="Set RUN_LIVE_MASSIVE_TESTS=1 and MASSIVE_API_KEY in the environment or repo .env/.env.web to run live Massive integration tests.",
)
@pytest.mark.parametrize("symbol", ["I:VIX", "I:VIX3M"])
def test_live_index_reference_and_prev_close_calls_return_data(symbol: str) -> None:
    cfg = _live_config()

    with requests.Session() as session:
        ref_status, ref_payload = _request_json(
            session,
            base_url=str(cfg.base_url),
            path=f"/v3/reference/tickers/{quote(symbol, safe=':')}",
            api_key=str(cfg.api_key),
            timeout_seconds=float(cfg.timeout_seconds),
        )
        assert ref_status == 200, (
            f"Massive reference lookup failed for {symbol}. "
            f"status={ref_status} payload={ref_payload}"
        )
        ref_results = ref_payload.get("results") if isinstance(ref_payload, dict) else None
        assert isinstance(ref_results, dict), f"Unexpected Massive reference payload for {symbol}: {ref_payload}"
        assert str(ref_results.get("ticker") or "").strip().upper() == symbol

        prev_status, prev_payload = _request_json(
            session,
            base_url=str(cfg.base_url),
            path=f"/v2/aggs/ticker/{quote(symbol, safe=':')}/prev",
            api_key=str(cfg.api_key),
            timeout_seconds=float(cfg.timeout_seconds),
        )
        assert prev_status == 200, (
            f"Massive previous-close request failed for {symbol}. "
            f"status={prev_status} payload={prev_payload}"
        )
        prev_results = prev_payload.get("results") if isinstance(prev_payload, dict) else None
        assert isinstance(prev_results, list) and prev_results, (
            f"Massive previous-close request returned no data rows for {symbol}. payload={prev_payload}"
        )
        last_row = prev_results[-1]
        for field in ("o", "h", "l", "c"):
            assert last_row.get(field) is not None, (
                f"Massive previous-close row for {symbol} is missing OHLC field {field}. row={last_row}"
            )
