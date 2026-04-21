from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class AlpacaError(Exception):
    message: str
    code: str = "alpaca_error"
    status_code: int | None = None
    payload: Optional[Mapping[str, Any]] = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


class AlpacaNotConfiguredError(AlpacaError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, code="not_configured", status_code=503)


class AlpacaApiError(AlpacaError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "api_error",
        status_code: int | None = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message=message, code=code, status_code=status_code, payload=payload)


class AlpacaValidationError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="validation_error", status_code=400, payload=payload)


class AlpacaAuthError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="auth_error", status_code=401, payload=payload)


class AlpacaPermissionError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="permission_error", status_code=403, payload=payload)


class AlpacaNotFoundError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="not_found", status_code=404, payload=payload)


class AlpacaConflictError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="conflict", status_code=409, payload=payload)


class AlpacaRateLimitError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="rate_limited", status_code=429, payload=payload)


class AlpacaTimeoutError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="timeout", status_code=502, payload=payload)


class AlpacaNetworkError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="network_error", status_code=502, payload=payload)


class AlpacaServerError(AlpacaApiError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="server_error", status_code=status_code or 502, payload=payload)


class AlpacaInvalidResponseError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="invalid_response", status_code=502, payload=payload)


class AlpacaAmbiguousWriteError(AlpacaApiError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message, code="ambiguous_write", status_code=409, payload=payload)
