"""Error types used by the Massive provider façade."""

from __future__ import annotations

from typing import Any, Optional


class MassiveError(RuntimeError):
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


class MassiveNotConfiguredError(MassiveError):
    """Raised when MASSIVE_API_KEY or related config is missing."""


class MassiveAuthError(MassiveError):
    """Raised when Massive returns 401/403."""


class MassiveRateLimitError(MassiveError):
    """Raised when Massive returns 429."""


class MassiveNotFoundError(MassiveError):
    """Raised when Massive returns 404."""


class MassiveServerError(MassiveError):
    """Raised for 5xx errors from Massive."""
