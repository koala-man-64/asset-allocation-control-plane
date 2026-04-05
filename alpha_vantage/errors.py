from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class AlphaVantageError(Exception):
    """
    Base exception type for Alpha Vantage client failures.

    `payload` should contain a redacted, non-secret-bearing representation of the
    error body when available. Never include API keys in this payload.
    """

    message: str
    code: str = "alpha_vantage_error"
    payload: Optional[Mapping[str, Any]] = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


class AlphaVantageThrottleError(AlphaVantageError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message=message, code="throttle", payload=payload)


class AlphaVantageInvalidSymbolError(AlphaVantageError):
    def __init__(self, message: str, *, payload: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(message=message, code="invalid_symbol", payload=payload)

