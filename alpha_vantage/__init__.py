"""
Alpha Vantage Client Library
===========================

This package provides a simple yet flexible interface for interacting
with the Alpha Vantage REST API.  The core of the library is the
``AlphaVantageClient`` class which encapsulates authentication,
rate‑limiting and request construction for all of the functions
documented by Alpha Vantage.  High‑level convenience methods are
provided for common tasks such as fetching daily, weekly or monthly
time series, foreign exchange rates, cryptocurrency prices, technical
indicators and fundamental statements.

Unlike many wrappers which return only raw JSON, this module offers
helpers to parse time series responses into pandas ``DataFrame``
objects, merge new data with existing datasets and perform
incremental updates without re‑downloading an entire history.  The
library is intentionally light on dependencies and makes use of
``httpx`` for HTTP requests and a simple token bucket for rate
limiting.  Concurrency is supported via Python's thread pool so
multiple symbols can be queried in parallel while still respecting
API call quotas.

To begin using the client, construct a :class:`~alpha_vantage.config.AlphaVantageConfig`
with your API key and pass it to :class:`~alpha_vantage.client.AlphaVantageClient`.
For example::

    from alpha_vantage import AlphaVantageClient, AlphaVantageConfig

    config = AlphaVantageConfig(api_key="YOUR_API_KEY", rate_limit_per_min=60, max_workers=10)
    av_client = AlphaVantageClient(config)
    data = av_client.get_daily_time_series("AAPL", outputsize="full")
    df = av_client.parse_time_series(data)
    print(df.head())

See the individual classes and methods for more details.
"""

from .config import AlphaVantageConfig  # noqa: F401
from .client import AlphaVantageClient  # noqa: F401
from .errors import (  # noqa: F401
    AlphaVantageError,
    AlphaVantageInvalidSymbolError,
    AlphaVantageThrottleError,
)
from .rate_limiter import RateLimiter  # noqa: F401
from .utils import parse_time_series, parse_financial_reports  # noqa: F401

__all__ = [
    "AlphaVantageConfig",
    "AlphaVantageClient",
    "AlphaVantageError",
    "AlphaVantageInvalidSymbolError",
    "AlphaVantageThrottleError",
    "RateLimiter",
    "parse_time_series",
    "parse_financial_reports",
]
