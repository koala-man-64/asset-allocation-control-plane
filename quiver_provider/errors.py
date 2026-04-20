from __future__ import annotations

from typing import Any, Optional


class QuiverError(RuntimeError):
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


class QuiverNotConfiguredError(QuiverError):
    pass


class QuiverInvalidRequestError(QuiverError):
    pass


class QuiverAuthError(QuiverError):
    pass


class QuiverEntitlementError(QuiverError):
    pass


class QuiverNotFoundError(QuiverError):
    pass


class QuiverRateLimitError(QuiverError):
    pass


class QuiverProtocolError(QuiverError):
    pass


class QuiverTimeoutError(QuiverError):
    pass


class QuiverUnavailableError(QuiverError):
    pass
