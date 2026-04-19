from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ETradeError(Exception):
    message: str
    code: str = "etrade_error"
    status_code: int | None = None
    payload: Optional[Mapping[str, Any]] = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


class ETradeNotConfiguredError(ETradeError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, code="not_configured", status_code=503)


class ETradeSessionExpiredError(ETradeError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, code="session_expired", status_code=409)


class ETradeInactiveSessionError(ETradeError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, code="session_inactive", status_code=409)


class ETradeApiError(ETradeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "api_error",
        status_code: int | None = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message=message, code=code, status_code=status_code, payload=payload)


class ETradeBrokerAuthError(ETradeApiError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="broker_auth_error", status_code=status_code or 401, payload=payload)


class ETradeRateLimitError(ETradeApiError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="rate_limited", status_code=status_code or 429, payload=payload)


class ETradeValidationError(ETradeApiError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="validation_error", status_code=status_code or 400, payload=payload)


class ETradeAmbiguousWriteError(ETradeApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="unknown_submission_state", status_code=502, payload=payload)
