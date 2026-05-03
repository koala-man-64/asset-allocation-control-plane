from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import urlsplit
from typing import Any, Dict, Optional

import httpx
from azure.identity import DefaultAzureCredential


ARM_SCOPE = "https://management.azure.com/.default"


@dataclass(frozen=True)
class ArmConfig:
    subscription_id: str
    resource_group: str
    api_version: str = "2023-05-01"
    timeout_seconds: float = 5.0


class AzureArmClient:
    def __init__(
        self,
        cfg: ArmConfig,
        *,
        credential: Optional[Any] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self._cfg = cfg
        self._credential = credential or DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(cfg.timeout_seconds))
        self._owns_http = http_client is None
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> "AzureArmClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()

    def _get_bearer(self) -> str:
        now = time.time()
        if self._token and now < (self._token_expires_at - 60):
            return self._token

        token = self._credential.get_token(ARM_SCOPE)
        self._token = token.token
        self._token_expires_at = float(getattr(token, "expires_on", 0) or 0)
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_bearer()}"}

    def _query_params(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        query = dict(params or {})
        parsed = urlsplit(url)
        if "api-version=" not in parsed.query.lower():
            query.setdefault("api-version", self._cfg.api_version)
        return {k: str(v) for k, v in query.items() if v is not None}

    def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        resp = self._http.get(url, headers=self._headers(), params=self._query_params(url, params))
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("ARM response was not a JSON object.")
        return payload

    def post_json(
        self,
        url: str,
        *,
        params: Optional[Dict[str, str]] = None,
        json_body: Any = None,
    ) -> Any:
        resp = self._http.post(url, headers=self._headers(), params=self._query_params(url, params), json=json_body)
        resp.raise_for_status()
        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError:
            return {"raw": resp.text}

    def patch_json(
        self,
        url: str,
        *,
        params: Optional[Dict[str, str]] = None,
        json_body: Any = None,
    ) -> Any:
        resp = self._http.patch(url, headers=self._headers(), params=self._query_params(url, params), json=json_body)
        resp.raise_for_status()
        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError:
            return {"raw": resp.text}

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        sub = self._cfg.subscription_id
        rg = self._cfg.resource_group
        provider = provider.strip().lstrip("/").rstrip("/")
        resource_type = resource_type.strip().lstrip("/").rstrip("/")
        name = name.strip()
        return (
            f"https://management.azure.com/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/{provider}/{resource_type}/{name}"
        )

    def resource_collection_url(self, *, provider: str, resource_type: str) -> str:
        sub = self._cfg.subscription_id
        rg = self._cfg.resource_group
        provider = provider.strip().lstrip("/").rstrip("/")
        resource_type = resource_type.strip().lstrip("/").rstrip("/")
        return (
            f"https://management.azure.com/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/{provider}/{resource_type}"
        )

