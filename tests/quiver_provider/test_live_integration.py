from __future__ import annotations

import os

import pytest

from quiver_provider import QuiverClient, QuiverConfig


def _live_enabled() -> bool:
    return str(os.environ.get("RUN_LIVE_QUIVER_TESTS") or "").strip().lower() in {"1", "true", "yes", "on"}


pytestmark = pytest.mark.skipif(not _live_enabled(), reason="RUN_LIVE_QUIVER_TESTS is not enabled.")


def test_live_quiver_congress_trading_smoke() -> None:
    if not str(os.environ.get("QUIVER_API_KEY") or "").strip():
        pytest.skip("QUIVER_API_KEY is not configured.")

    client = QuiverClient(QuiverConfig.from_env(require_api_key=True))
    try:
        payload = client.get_json("/beta/live/congresstrading")
    finally:
        client.close()

    assert isinstance(payload, list)
