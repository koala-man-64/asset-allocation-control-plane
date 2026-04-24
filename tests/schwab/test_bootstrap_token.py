import base64

import httpx

from schwab.bootstrap_token import bootstrap_tokens_from_callback


def test_bootstrap_tokens_from_callback_keeps_tokens_out_of_env_file(tmp_path):
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
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers["Authorization"]
        captured["body"] = request.content.decode("utf-8")
        return httpx.Response(
            200,
            json={
                "expires_in": 1800,
                "token_type": "Bearer",
                "scope": "api",
                "refresh_token": "refresh-token-from-api",
                "access_token": "access-token-from-api",
                "id_token": "id-token-from-api",
            },
        )

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    result = bootstrap_tokens_from_callback(
        "https://127.0.0.1/?code=abc%40example.com&session=session-1",
        env_file=env_path,
        state="opaque-state",
        http_client=http_client,
    )

    expected_basic = base64.b64encode(b"client-id:client-secret").decode("ascii")
    assert result.authorization_url == (
        "https://api.schwabapi.com/v1/oauth/authorize"
        "?response_type=code&client_id=client-id&redirect_uri=https%3A%2F%2F127.0.0.1&state=opaque-state"
    )
    assert captured["url"] == "https://api.schwabapi.com/v1/oauth/token"
    assert captured["authorization"] == f"Basic {expected_basic}"
    assert captured["body"] == "grant_type=authorization_code&code=abc%40example.com&redirect_uri=https%3A%2F%2F127.0.0.1"
    assert result.tokens.access_token == "access-token-from-api"
    assert result.tokens.refresh_token == "refresh-token-from-api"

    saved = env_path.read_text(encoding="utf-8")
    assert "SCHWAB_ACCESS_TOKEN" not in saved
    assert "SCHWAB_REFRESH_TOKEN" not in saved
