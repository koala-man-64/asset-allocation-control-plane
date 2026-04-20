import logging
import time
from typing import Any

import httpx

from alpaca.config import AlpacaConfig

logger = logging.getLogger(__name__)


class AlpacaHttpTransport:
    def __init__(self, config: AlpacaConfig) -> None:
        self._config = config
        self._base_url = config.get_trading_base_url()
        self._headers = {
            "APCA-API-KEY-ID": config.get_api_key(),
            "APCA-API-SECRET-KEY": config.get_api_secret(),
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(
            base_url=self._base_url,
            headers=self._headers,
            timeout=config.http.timeout_s,
        )

    def close(self) -> None:
        self._client.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> Any:
        retries = self._config.http.max_retries
        backoff = self._config.http.backoff_base_s

        for attempt in range(retries + 1):
            try:
                response = self._client.request(method, endpoint, params=params, json=json_data)
                response.raise_for_status()
                if response.status_code == 204:
                    return {}
                return response.json()
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                if status_code == 429:
                    logger.warning("Rate limited on %s %s. Retrying in %ss...", method, endpoint, backoff)
                elif 400 <= status_code < 500:
                    logger.error("Client error %s on %s %s: %s", status_code, method, endpoint, exc.response.text)
                    raise
                else:
                    logger.warning("Server error %s on %s %s. Retrying in %ss...", status_code, method, endpoint, backoff)

                if attempt == retries:
                    raise
            except httpx.RequestError as exc:
                logger.warning("Network error on %s %s: %s. Retrying in %ss...", method, endpoint, exc, backoff)
                if attempt == retries:
                    raise

            time.sleep(backoff)
            backoff *= 2.0

        raise RuntimeError("Unreachable code")

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, json_data: dict[str, Any] | None = None) -> Any:
        return self._request("POST", endpoint, json_data=json_data)

    def delete(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("DELETE", endpoint, params=params)

    def put(self, endpoint: str, json_data: dict[str, Any] | None = None) -> Any:
        return self._request("PUT", endpoint, json_data=json_data)

    def patch(self, endpoint: str, json_data: dict[str, Any] | None = None) -> Any:
        return self._request("PATCH", endpoint, json_data=json_data)
