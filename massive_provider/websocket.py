"""WebSocket helpers for Massive.

The project does not expose Massive WebSocket data through the Asset Allocation
API service (yet). Instead, this module provides a small wrapper around the
official Massive SDK so downstream jobs can consume live streams.

If you want to use this, install the official SDK:

    pip install massive
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List

from massive_provider.errors import MassiveNotConfiguredError


try:  # optional dependency
    from massive import WebSocketClient as _SDKWebSocketClient  # type: ignore
    from massive.websocket.models import WebSocketMessage as _SDKWebSocketMessage  # type: ignore
except Exception:  # pragma: no cover
    _SDKWebSocketClient = None
    _SDKWebSocketMessage = None


def stocks_trades(*tickers: str) -> list[str]:
    """Build stock trade subscription topics: ``T.AAPL``."""

    return [f"T.{str(t).strip().upper()}" for t in tickers if str(t).strip()]


def stocks_quotes(*tickers: str) -> list[str]:
    """Build stock quote subscription topics: ``Q.AAPL``."""

    return [f"Q.{str(t).strip().upper()}" for t in tickers if str(t).strip()]


def stocks_minute_aggs(*tickers: str) -> list[str]:
    """Build stock *minute* aggregate subscription topics: ``AM.AAPL``."""

    return [f"AM.{str(t).strip().upper()}" for t in tickers if str(t).strip()]


def stocks_second_aggs(*tickers: str) -> list[str]:
    """Build stock *second* aggregate subscription topics: ``A.AAPL``."""

    return [f"A.{str(t).strip().upper()}" for t in tickers if str(t).strip()]


@dataclass(frozen=True)
class MassiveWebSocketConfig:
    api_key: str
    subscriptions: tuple[str, ...]


class MassiveWebSocketRunner:
    """Thin wrapper over ``massive.WebSocketClient``."""

    def __init__(self, config: MassiveWebSocketConfig) -> None:
        if not config.api_key:
            raise MassiveNotConfiguredError("MASSIVE_API_KEY is required for WebSocket usage.")
        if _SDKWebSocketClient is None:
            raise ImportError(
                "The official Massive SDK is not installed (pip install massive). "
                "This project intentionally keeps it optional."
            )

        self.config = config
        self._client = _SDKWebSocketClient(api_key=config.api_key, subscriptions=list(config.subscriptions))

    def run(self, *, handle_msg: Callable[[List[object]], None]) -> None:
        """Run the WebSocket client.

        Parameters
        ----------
        handle_msg:
            Callback invoked by the SDK with a list of decoded WebSocket messages.
        """

        self._client.run(handle_msg=handle_msg)


__all__ = [
    "MassiveWebSocketConfig",
    "MassiveWebSocketRunner",
    "stocks_trades",
    "stocks_quotes",
    "stocks_minute_aggs",
    "stocks_second_aggs",
]
