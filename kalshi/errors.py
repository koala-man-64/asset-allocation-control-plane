from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class KalshiError(Exception):
    message: str
    code: str = "kalshi_error"
    status_code: int | None = None
    payload: Optional[Mapping[str, Any]] = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


class KalshiNotConfiguredError(KalshiError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, code="not_configured", status_code=503)


class KalshiApiError(KalshiError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "api_error",
        status_code: int | None = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message=message, code=code, status_code=status_code, payload=payload)


class KalshiValidationError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="validation_error", status_code=400, payload=payload)


class KalshiAuthError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="auth_error", status_code=401, payload=payload)


class KalshiPermissionError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="permission_error", status_code=403, payload=payload)


class KalshiNotFoundError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="not_found", status_code=404, payload=payload)


class KalshiConflictError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="conflict", status_code=409, payload=payload)


class KalshiRateLimitError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="rate_limited", status_code=429, payload=payload)


class KalshiTimeoutError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="timeout", status_code=502, payload=payload)


class KalshiNetworkError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="network_error", status_code=502, payload=payload)


class KalshiServerError(KalshiApiError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="server_error", status_code=status_code or 502, payload=payload)


class KalshiInvalidResponseError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="invalid_response", status_code=502, payload=payload)


class KalshiAmbiguousWriteError(KalshiApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="ambiguous_write", status_code=409, payload=payload)
