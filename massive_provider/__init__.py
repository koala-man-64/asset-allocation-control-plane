"""Massive provider integration.

This project already depends (or will depend) on the **official** Massive Python
SDK, which is imported as :mod:`massive`.

To avoid accidentally shadowing that dependency, this package is intentionally
named :mod:`massive_provider`.

The core public entry point is :class:`~massive_provider.client.MassiveClient`.
"""

from massive_provider.client import MassiveClient
from massive_provider.config import MassiveConfig
from massive_provider.errors import (
    MassiveAuthError,
    MassiveError,
    MassiveNotConfiguredError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
)

__all__ = [
    "MassiveClient",
    "MassiveConfig",
    "MassiveError",
    "MassiveNotConfiguredError",
    "MassiveAuthError",
    "MassiveRateLimitError",
    "MassiveNotFoundError",
    "MassiveServerError",
]
