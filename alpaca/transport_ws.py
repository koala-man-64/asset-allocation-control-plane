import json
import logging
from typing import Any

import websockets

try:
    import msgpack  # type: ignore
except ImportError:
    msgpack = None

from alpaca.config import AlpacaConfig

logger = logging.getLogger(__name__)


class AlpacaWsTransport:
    def __init__(self, config: AlpacaConfig) -> None:
        self._config = config
        self._url = config.get_trading_ws_url()
        self._api_key = config.get_api_key()
        self._api_secret = config.get_api_secret()
        self._ws: Any = None
        self._running = False

    async def connect(self) -> None:
        logger.info("Connecting to Alpaca WS: %s", self._url)
        self._ws = await websockets.connect(self._url)
        await self._authenticate()
        logger.info("Connected and authenticated.")
        self._running = True

    async def _authenticate(self) -> None:
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        auth_payload = {
            "action": "auth",
            "key": self._api_key,
            "secret": self._api_secret,
        }
        await self._ws.send(json.dumps(auth_payload))

        response = await self._ws.recv()
        message = self._parse_message(response)
        if isinstance(message, dict) and message.get("stream") == "authorization":
            if message.get("data", {}).get("status") == "authorized":
                return
            raise RuntimeError(f"Auth failed: {message}")

        if isinstance(message, list):
            for item in message:
                if item.get("stream") == "authorization":
                    if item.get("data", {}).get("status") == "authorized":
                        return
                    raise RuntimeError(f"Auth failed: {item}")

        raise RuntimeError(f"Unexpected auth response: {message}")

    async def subscribe(self, streams: list[str]) -> None:
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        payload = {
            "action": "listen",
            "data": {
                "streams": streams,
            },
        }
        await self._ws.send(json.dumps(payload))

    async def listen(self):
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        while self._running:
            try:
                message = await self._ws.recv()
                data = self._parse_message(message)
                if isinstance(data, list):
                    for item in data:
                        yield item
                else:
                    yield data
            except websockets.ConnectionClosed:
                logger.warning("WebSocket connection closed.")
                break
            except Exception as exc:
                logger.error("Error reading WebSocket message: %s", exc)
                break

    def _parse_message(self, message: str | bytes) -> Any:
        if isinstance(message, bytes):
            if msgpack:
                return msgpack.unpackb(message, raw=False)
            try:
                return json.loads(message.decode("utf-8"))
            except Exception:
                return json.loads(message)
        return json.loads(message)

    async def close(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
