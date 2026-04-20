import base64

import httpx
import pytest

from schwab import SchwabClient, SchwabConfig
from schwab.errors import SchwabAuthError, SchwabNotConfiguredError, SchwabRateLimitError


def _config(**overrides) -> SchwabConfig:
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "app_callback_url": "https://127.0.0.1/callback",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
    }
    values.update(overrides)
    return SchwabConfig(**values)


def test_build_authorization_url_uses_registered_app_settings():
    client = SchwabClient(_config())

    url = client.build_authorization_url(state="opaque-state")

    assert url == (
        "https://api.schwabapi.com/v1/oauth/authorize"
        "?response_type=code&client_id=client-id&redirect_uri=https%3A%2F%2F127.0.0.1%2Fcallback&state=opaque-state"
    )


def test_extract_authorization_code_decodes_redirect_url():
    redirected_url = "https://127.0.0.1/callback?code=abc%40example.com&session=session-1"

    assert SchwabClient.extract_authorization_code(redirected_url) == "abc@example.com"


def test_exchange_authorization_code_uses_basic_auth_and_form_encoded_body():
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers["Authorization"]
        captured["content_type"] = request.headers["Content-Type"]
        captured["body"] = request.content.decode("utf-8")
        return httpx.Response(
            200,
            json={
                "expires_in": 1800,
                "token_type": "Bearer",
                "scope": "api",
                "refresh_token": "new-refresh-token",
                "access_token": "new-access-token",
                "id_token": "new-id-token",
            },
        )

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    tokens = client.exchange_authorization_code("abc%40example.com")

    expected_basic = base64.b64encode(b"client-id:client-secret").decode("ascii")
    assert captured["url"] == "https://api.schwabapi.com/v1/oauth/token"
    assert captured["authorization"] == f"Basic {expected_basic}"
    assert captured["content_type"] == "application/x-www-form-urlencoded"
    assert captured["body"] == "grant_type=authorization_code&code=abc%40example.com&redirect_uri=https%3A%2F%2F127.0.0.1%2Fcallback"
    assert tokens.access_token == "new-access-token"
    assert tokens.refresh_token == "new-refresh-token"


def test_refresh_access_token_uses_refresh_token_flow():
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["Authorization"]
        captured["body"] = request.content.decode("utf-8")
        return httpx.Response(
            200,
            json={
                "expires_in": 1800,
                "token_type": "Bearer",
                "scope": "api",
                "refresh_token": "refreshed-token",
                "access_token": "replacement-access-token",
                "id_token": "jwt",
            },
        )

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    tokens = client.refresh_access_token()

    expected_basic = base64.b64encode(b"client-id:client-secret").decode("ascii")
    assert captured["authorization"] == f"Basic {expected_basic}"
    assert captured["body"] == "grant_type=refresh_token&refresh_token=refresh-token"
    assert tokens.access_token == "replacement-access-token"


def test_get_account_numbers_uses_bearer_token():
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["Authorization"]
        captured["url"] = str(request.url)
        return httpx.Response(200, json=[{"accountNumber": "123456789", "hashValue": "hashed"}])

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    payload = client.get_account_numbers()

    assert captured["authorization"] == "Bearer access-token"
    assert captured["url"] == "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"
    assert payload[0]["hashValue"] == "hashed"


def test_place_order_posts_json_body_and_returns_response_metadata():
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["Authorization"]
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode("utf-8")
        return httpx.Response(201, headers={"Location": "/accounts/123/orders/456"})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    response = client.place_order("123456789", {"orderType": "MARKET", "session": "NORMAL"})

    assert captured["authorization"] == "Bearer access-token"
    assert captured["url"] == "https://api.schwabapi.com/trader/v1/accounts/123456789/orders"
    assert captured["body"] == '{"orderType":"MARKET","session":"NORMAL"}'
    assert response.status_code == 201
    assert response.payload is None
    assert response.headers["Location"] == "/accounts/123/orders/456"


def test_list_orders_for_account_passes_query_params():
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json=[{"orderId": 1}])

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    payload = client.list_orders(
        account_number="123456789",
        params={"fromEnteredTime": "2026-04-19T00:00:00Z", "toEnteredTime": "2026-04-19T23:59:59Z"},
    )

    assert captured["url"] == (
        "https://api.schwabapi.com/trader/v1/accounts/123456789/orders"
        "?fromEnteredTime=2026-04-19T00%3A00%3A00Z&toEnteredTime=2026-04-19T23%3A59%3A59Z"
    )
    assert payload[0]["orderId"] == 1


def test_missing_access_token_raises_not_configured():
    client = SchwabClient(_config(access_token=""))

    with pytest.raises(SchwabNotConfiguredError, match="SCHWAB_ACCESS_TOKEN"):
        client.get_account_numbers()


def test_unauthorized_response_maps_to_auth_error():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "unauthorized"})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    with pytest.raises(SchwabAuthError):
        client.get_user_preference()


def test_rate_limit_response_maps_to_rate_limit_error():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"message": "too many requests"})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = SchwabClient(_config(), http_client=http_client)

    with pytest.raises(SchwabRateLimitError):
        client.get_account_numbers()
