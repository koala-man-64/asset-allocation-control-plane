import httpx

from massive_provider import MassiveClient, MassiveConfig


def _build_client(handler, *, float_endpoint: str = "/stocks/vX/float") -> MassiveClient:
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(
        transport=transport,
        base_url="https://api.massive.com",
        headers={"Authorization": "Bearer test-key"},
    )
    cfg = MassiveConfig(
        api_key="test-key",
        base_url="https://api.massive.com",
        timeout_seconds=10.0,
        float_endpoint=float_endpoint,
    )
    return MassiveClient(cfg, http_client=http_client)


def test_massive_client_paths_align_with_docs() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path.startswith("/v2/aggs/ticker/"):
            return httpx.Response(200, json={"results": []})
        return httpx.Response(200, json={"status": "OK", "results": []})

    client = _build_client(handler)
    try:
        client.get_daily_ticker_summary(ticker="AAPL", date="2026-02-09", adjusted=False)
        client.list_ohlcv(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="2026-02-01",
            to="2026-02-09",
            adjusted=True,
            sort="asc",
            limit=50000,
            pagination=False,
        )
        client.get_short_interest(ticker="AAPL")
        client.get_short_volume(ticker="AAPL")
        client.get_float(ticker="AAPL")
        client.get_income_statement(ticker="AAPL")
        client.get_cash_flow_statement(ticker="AAPL")
        client.get_balance_sheet(ticker="AAPL")
        client.get_ratios(ticker="AAPL")
        client.get_unified_snapshot(tickers=["AAPL", "MSFT"])
    finally:
        client.close()

    assert seen[0][0] == "/v1/open-close/AAPL/2026-02-09"
    assert seen[0][1].get("adjusted") == "false"

    assert seen[1][0] == "/v2/aggs/ticker/AAPL/range/1/day/2026-02-01/2026-02-09"
    assert seen[1][1].get("adjusted") == "true"
    assert seen[1][1].get("sort") == "asc"
    assert seen[1][1].get("limit") == "50000"

    assert seen[2][0] == "/stocks/v1/short-interest"
    assert seen[3][0] == "/stocks/v1/short-volume"
    assert seen[4][0] == "/stocks/vX/float"
    assert seen[5][0] == "/stocks/financials/v1/income-statements"
    assert seen[5][1].get("tickers") == "AAPL"
    assert seen[5][1].get("ticker") is None
    assert seen[6][0] == "/stocks/financials/v1/cash-flow-statements"
    assert seen[6][1].get("tickers") == "AAPL"
    assert seen[6][1].get("ticker") is None
    assert seen[7][0] == "/stocks/financials/v1/balance-sheets"
    assert seen[7][1].get("tickers") == "AAPL"
    assert seen[7][1].get("ticker") is None
    assert seen[8][0] == "/stocks/financials/v1/ratios"
    assert seen[8][1].get("ticker") == "AAPL"
    assert seen[9][0] == "/v3/snapshot"
    assert seen[9][1].get("type") is None
    assert seen[9][1].get("ticker.any_of") == "AAPL,MSFT"
    assert seen[9][1].get("limit") == "2"


def test_massive_client_float_endpoint_can_be_overridden() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"status": "OK", "results": []})

    client = _build_client(handler, float_endpoint="/stocks/v1/float")
    try:
        client.get_float(ticker="AAPL")
    finally:
        client.close()

    assert seen == ["/stocks/v1/float"]


def test_massive_client_financial_statements_follow_next_url_pagination() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if request.url.path == "/stocks/financials/v1/balance-sheets" and request.url.params.get("cursor") != "next":
            return httpx.Response(
                200,
                json={
                    "results": [{"id": 1}],
                    "next_url": "https://api.massive.com/stocks/financials/v1/balance-sheets?cursor=next",
                },
            )
        return httpx.Response(200, json={"results": [{"id": 2}], "next_url": None})

    client = _build_client(handler)
    try:
        payload = client.get_balance_sheet(
            ticker="AAPL",
            params={"timeframe": "quarterly", "sort": "period_end.asc", "limit": 100},
            pagination=True,
        )
    finally:
        client.close()

    assert [row["id"] for row in payload["results"]] == [1, 2]
    assert "tickers=AAPL" in seen[0]
    assert "timeframe=quarterly" in seen[0]
    assert "sort=period_end.asc" in seen[0]
    assert "limit=100" in seen[0]
    assert seen[1].endswith("cursor=next")

def test_fundamentals_pagination_aggregates_all_pages() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, str(request.url)))
        if len(seen) > 5:
            raise AssertionError(f"Pagination loop detected: {seen}")
        page = request.url.params.get("page")
        if page == "2":
            return httpx.Response(200, json={"results": [{"settlement_date": "2025-01-31", "short_interest": 110}]})
        return httpx.Response(
            200,
            json={
                "results": [{"settlement_date": "2025-01-15", "short_interest": 100}],
                "next_url": "https://api.massive.com/stocks/v1/short-interest?page=2",
            },
        )

    client = _build_client(handler)
    try:
        payload = client.get_short_interest(ticker="AAPL", params={"limit": 1}, pagination=True)
    finally:
        client.close()

    assert isinstance(payload, dict)
    assert [row["settlement_date"] for row in payload["results"]] == ["2025-01-15", "2025-01-31"]
    assert payload["next_url"] is None
    assert seen[0][0] == "/stocks/v1/short-interest"
    assert seen[1][0] == "/stocks/v1/short-interest"


def test_unified_snapshot_chunks_over_250_tickers() -> None:
    seen_chunks: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        tickers_raw = str(request.url.params.get("ticker.any_of") or "")
        chunk = [ticker for ticker in tickers_raw.split(",") if ticker]
        seen_chunks.append(chunk)
        return httpx.Response(200, json={"results": [{"ticker": ticker} for ticker in chunk]})

    tickers = [f"T{i:03d}" for i in range(300)]
    client = _build_client(handler)
    try:
        payload = client.get_unified_snapshot(tickers=tickers)
    finally:
        client.close()

    assert len(seen_chunks) == 2
    assert len(seen_chunks[0]) == 250
    assert len(seen_chunks[1]) == 50
    assert isinstance(payload, dict)
    assert len(payload.get("results", [])) == 300


def test_unified_snapshot_strips_type_param_when_filtering_by_ticker() -> None:
    seen_params: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_params.append(dict(request.url.params))
        return httpx.Response(200, json={"results": [{"ticker": "AAPL"}]})

    client = _build_client(handler)
    try:
        payload = client.get_unified_snapshot(
            tickers=["AAPL"],
            asset_type="stocks",
            params={"type": "stocks"},
        )
    finally:
        client.close()

    assert payload["results"][0]["ticker"] == "AAPL"
    assert len(seen_params) == 1
    assert seen_params[0].get("ticker.any_of") == "AAPL"
    assert seen_params[0].get("type") is None
