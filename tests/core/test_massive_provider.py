import json

import pytest
import requests

from core.massive_provider import (
    MassiveProvider,
    MassiveProviderConfig,
    MassiveProviderError,
    get_complete_ticker_list,
    tickers_to_dataframe,
)


class _FakeResponse:
    def __init__(self, payload, *, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []
        self.closed = False

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if not self._responses:
            raise AssertionError("No fake responses left.")
        return self._responses.pop(0)

    def close(self):
        self.closed = True


def _provider(session):
    return MassiveProvider(
        MassiveProviderConfig(api_key="test-key", base_url="https://api.massive.com", timeout_seconds=11.0),
        session=session,
    )


def test_list_tickers_paginates_through_next_url_and_propagates_api_key():
    fake_session = _FakeSession(
        [
            _FakeResponse(
                {
                    "results": [{"ticker": "AAPL"}, {"ticker": "MSFT"}],
                    "next_url": "https://api.massive.com/v3/reference/tickers?cursor=abc123",
                }
            ),
            _FakeResponse({"results": [{"ticker": "NVDA"}], "next_url": None}),
        ]
    )

    provider = _provider(fake_session)
    rows = provider.list_tickers()

    assert [item["ticker"] for item in rows] == ["AAPL", "MSFT", "NVDA"]
    assert len(fake_session.calls) == 2
    assert fake_session.calls[0]["url"] == "https://api.massive.com/v3/reference/tickers"
    assert fake_session.calls[0]["params"]["apiKey"] == "test-key"
    assert fake_session.calls[0]["params"]["limit"] == 1000
    assert fake_session.calls[1]["params"] is None
    assert "cursor=abc123" in fake_session.calls[1]["url"]
    assert "apiKey=test-key" in fake_session.calls[1]["url"]


def test_list_tickers_supports_relative_next_url():
    fake_session = _FakeSession(
        [
            _FakeResponse({"results": [{"ticker": "AAPL"}], "next_url": "/v3/reference/tickers?cursor=next"}),
            _FakeResponse({"results": [{"ticker": "GOOG"}]}),
        ]
    )

    provider = _provider(fake_session)
    rows = provider.list_tickers()

    assert [item["ticker"] for item in rows] == ["AAPL", "GOOG"]
    assert fake_session.calls[1]["url"].startswith("https://api.massive.com/v3/reference/tickers?cursor=next")
    assert "apiKey=test-key" in fake_session.calls[1]["url"]


def test_list_tickers_raises_for_http_errors():
    fake_session = _FakeSession([_FakeResponse({"error": "bad"}, status_code=503)])
    provider = _provider(fake_session)

    with pytest.raises(MassiveProviderError, match="Massive request failed"):
        provider.list_tickers()


def test_list_tickers_raises_for_invalid_results_shape():
    fake_session = _FakeSession([_FakeResponse({"results": {"ticker": "AAPL"}})])
    provider = _provider(fake_session)

    with pytest.raises(MassiveProviderError, match="`results` was not a list"):
        provider.list_tickers()


def test_tickers_to_dataframe_normalizes_symbols_and_deduplicates():
    df = tickers_to_dataframe(
        [
            {"ticker": " aapl ", "name": "Apple", "primary_exchange": "XNAS", "type": "CS", "active": True},
            {"ticker": "AAPL", "name": "Apple Duplicate", "primary_exchange": "XNAS", "type": "CS"},
            {"ticker": " msft ", "name": "Microsoft", "primary_exchange": "XNAS", "type": "CS", "active": "true"},
            {"ticker": ""},
        ]
    )

    assert df["Symbol"].tolist() == ["AAPL", "MSFT"]
    assert df["source_massive"].tolist() == [True, True]
    assert df["Exchange"].tolist() == ["XNAS", "XNAS"]


def test_get_complete_ticker_list_requires_api_key(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    with pytest.raises(MassiveProviderError, match="MASSIVE_API_KEY is required"):
        get_complete_ticker_list(api_key=None)


def test_get_complete_ticker_list_builds_dataframe_from_all_pages():
    fake_session = _FakeSession(
        [
            _FakeResponse(
                {
                    "results": [{"ticker": "AAPL", "name": "Apple", "active": True}],
                    "next_url": "https://api.massive.com/v3/reference/tickers?cursor=page2",
                }
            ),
            _FakeResponse({"results": [{"ticker": "MSFT", "name": "Microsoft", "active": True}]}),
        ]
    )

    df = get_complete_ticker_list(api_key="abc", session=fake_session)

    assert df["Symbol"].tolist() == ["AAPL", "MSFT"]
    assert df["Active"].tolist() == [True, True]
    assert len(fake_session.calls) == 2
