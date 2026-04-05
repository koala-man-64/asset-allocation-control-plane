"""
Alpha Vantage configuration support.

This module defines the :class:`AlphaVantageConfig` dataclass which encapsulates
the essential configuration parameters required to interact with the Alpha
Vantage API. These parameters include the API key, base URL, request timeout,
maximum number of concurrent workers, and the allowed number of calls per
minute.

Example
-------

    >>> from alpha_vantage.config import AlphaVantageConfig
    >>> cfg = AlphaVantageConfig(api_key="demo", rate_limit_per_min=60, max_workers=5)
    >>> cfg.get_query_url()
    'https://www.alphavantage.co/query'
"""

from dataclasses import dataclass


@dataclass
class AlphaVantageConfig:
    """Configuration container for the Alpha Vantage client.

    Attributes
    ----------
    api_key:
        The API key issued by Alpha Vantage. This key is required for every request.

    base_url:
        The base URL of the Alpha Vantage service. If you run a proxy or mirror you
        can override this value, otherwise the default ``https://www.alphavantage.co``
        should be used.

    rate_limit_per_min:
        Maximum number of requests permitted per minute. Set this value to match
        your subscription tier to avoid throttling.

    max_workers:
        Number of concurrent workers used when fetching multiple endpoints in
        parallel. Increasing this value can reduce overall runtime when pulling
        many symbols, but it should not exceed your rate limit.

    timeout:
        Timeout in seconds for individual HTTP requests. Requests taking longer
        than this will raise an exception.

    rate_wait_timeout_seconds:
        Optional timeout for waiting on a local rate-limit slot. ``None`` waits
        indefinitely.

    throttle_cooldown_seconds:
        Cooldown window (seconds) enforced after provider throttle signals
        (payload Note/Information or HTTP 429). During this window the client
        suppresses additional outbound calls.
    """

    api_key: str
    base_url: str = "https://www.alphavantage.co"
    rate_limit_per_min: int = 5
    max_workers: int = 1
    timeout: float = 10.0
    max_retries: int = 5
    backoff_base_seconds: float = 0.5
    rate_wait_timeout_seconds: float | None = 120.0
    throttle_cooldown_seconds: float = 60.0

    def get_query_url(self) -> str:
        """Return the full query endpoint for the API."""
        return f"{self.base_url.rstrip('/')}/query"
