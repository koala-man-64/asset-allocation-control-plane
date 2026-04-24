import httpx

from schwab.config import SchwabConfig
from schwab.list_accounts import _render_rows, fetch_account_snapshot


def test_fetch_account_snapshot_refreshes_tokens_once_and_formats_rows(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "SCHWAB_CLIENT_ID=client-id",
                "SCHWAB_CLIENT_SECRET=client-secret",
                "SCHWAB_APP_CALLBACK_URL=https://127.0.0.1",
            ]
        ),
        encoding="utf-8",
    )
    config = SchwabConfig(
        client_id="client-id",
        client_secret="client-secret",
        app_callback_url="https://127.0.0.1",
        access_token="expired-access-token",
        refresh_token="refresh-token",
    )

    request_log: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_log.append((request.method, str(request.url)))
        authorization = request.headers.get("Authorization", "")

        if request.url.path == "/trader/v1/accounts/accountNumbers" and authorization == "Bearer expired-access-token":
            return httpx.Response(401, json={"error": "expired"})

        if request.url.path == "/v1/oauth/token":
            return httpx.Response(
                200,
                json={
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                    "refresh_token": "fresh-refresh-token",
                    "access_token": "fresh-access-token",
                    "id_token": "id-token",
                },
            )

        if request.url.path == "/trader/v1/accounts/accountNumbers" and authorization == "Bearer fresh-access-token":
            return httpx.Response(
                200,
                json=[
                    {
                        "accountNumber": "123456789",
                        "hashValue": "encrypted-account-id-1234567890",
                    }
                ],
            )

        if request.url.path == "/trader/v1/accounts" and authorization == "Bearer fresh-access-token":
            return httpx.Response(
                200,
                json=[
                    {
                        "securitiesAccount": {
                            "accountNumber": "123456789",
                            "type": "MARGIN",
                            "currentBalances": {
                                "liquidationValue": 1000.5,
                                "cashBalance": 250.25,
                                "buyingPower": 5000.0,
                            },
                        }
                    }
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url} {authorization}")

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    snapshot = fetch_account_snapshot(env_file=env_path, config=config, http_client=http_client)

    assert snapshot.refreshed_tokens is True
    assert len(snapshot.rows) == 1
    row = snapshot.rows[0]
    assert row.account == "***6789"
    assert row.encrypted_id == "encryp...567890"
    assert row.account_type == "MARGIN"
    assert row.net_liquidation == "$1,000.50"
    assert row.cash == "$250.25"
    assert row.buying_power == "$5,000.00"
    assert request_log == [
        ("GET", "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"),
        ("POST", "https://api.schwabapi.com/v1/oauth/token"),
        ("GET", "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"),
        ("GET", "https://api.schwabapi.com/trader/v1/accounts"),
    ]

    saved = env_path.read_text(encoding="utf-8")
    assert "SCHWAB_ACCESS_TOKEN" not in saved
    assert "SCHWAB_REFRESH_TOKEN" not in saved


def test_render_rows_masks_and_aligns_multiple_accounts():
    rendered = _render_rows(
        [
            type(
                "Row",
                (),
                {
                    "account": "***6789",
                    "encrypted_id": "encryp...567890",
                    "account_type": "MARGIN",
                    "net_liquidation": "$1,000.50",
                    "cash": "$250.25",
                    "buying_power": "$5,000.00",
                },
            )(),
            type(
                "Row",
                (),
                {
                    "account": "***4321",
                    "encrypted_id": "",
                    "account_type": "CASH",
                    "net_liquidation": "$400.00",
                    "cash": "$400.00",
                    "buying_power": "$400.00",
                },
            )(),
        ]
    )

    assert "account" in rendered
    assert "***6789" in rendered
    assert "***4321" in rendered
    assert "encryp...567890" in rendered
