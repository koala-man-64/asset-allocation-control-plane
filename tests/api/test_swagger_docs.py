from __future__ import annotations

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_swagger_routes_available_under_api_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_ROOT_PREFIX", raising=False)

    app = create_app()
    async with get_test_client(app) as client:
        docs = await client.get("/api/docs")
        openapi = await client.get("/api/openapi.json")
        docs_redirect = await client.get("/docs", follow_redirects=False)
        openapi_redirect = await client.get("/openapi.json", follow_redirects=False)

    assert docs.status_code == 200
    assert "text/html" in docs.headers.get("content-type", "")
    assert "Swagger UI" in docs.text

    assert openapi.status_code == 200
    body = openapi.json()
    assert body["info"]["title"] == "Asset Allocation API"

    assert docs_redirect.status_code == 307
    assert docs_redirect.headers.get("location") == "/api/docs"

    assert openapi_redirect.status_code == 307
    assert openapi_redirect.headers.get("location") == "/api/openapi.json"


@pytest.mark.asyncio
async def test_swagger_routes_available_with_root_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")

    app = create_app()
    async with get_test_client(app) as client:
        docs_default = await client.get("/api/docs")
        docs_prefixed = await client.get("/asset-allocation/api/docs")
        openapi_prefixed = await client.get("/asset-allocation/api/openapi.json")
        docs_redirect = await client.get("/docs", follow_redirects=False)

    assert docs_default.status_code == 200
    assert docs_prefixed.status_code == 200
    assert openapi_prefixed.status_code == 200
    assert docs_redirect.status_code == 307
    assert docs_redirect.headers.get("location") == "/asset-allocation/api/docs"
