"""Error types used by the Schwab Trader API client."""

from __future__ import annotations

from typing import Any, Optional


class SchwabError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        detail: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail
        self.payload = payload


class SchwabNotConfiguredError(SchwabError):
    """Raised when client credentials or tokens are missing."""


class SchwabAuthError(SchwabError):
    """Raised when Schwab returns 401 or 403."""


class SchwabRateLimitError(SchwabError):
    """Raised when Schwab returns 429."""


class SchwabNotFoundError(SchwabError):
    """Raised when Schwab returns 404."""


class SchwabServerError(SchwabError):
    """Raised when Schwab returns a 5xx response."""
