from __future__ import annotations

import logging
import re
import traceback
from collections.abc import Mapping
from typing import Any

REDACTED = "[REDACTED]"

_BEARER_TOKEN_RE = re.compile(r"\b(Bearer)\s+([A-Za-z0-9._~+/=-]+)", re.IGNORECASE)
_QUERY_SECRET_RE = re.compile(
    r"(^|[?&;\s])"
    r"("
    r"(?:api[_-]?key|apikey|token|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"client[_-]?secret|password|passwd|secret|sig|signature|code)"
    r"=)"
    r"([^&#\s]+)",
    re.IGNORECASE,
)
_CONNECTION_STRING_SECRET_RE = re.compile(
    r"\b(AccountKey|SharedAccessSignature|SharedAccessKey|ClientSecret)=([^;,\s]+)",
    re.IGNORECASE,
)

_SENSITIVE_EXACT_KEYS = {
    "authorization",
    "proxy_authorization",
    "cookie",
    "set_cookie",
    "x_api_key",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "id_token",
    "client_secret",
    "password",
    "passwd",
    "signature",
    "sig",
}
_SENSITIVE_KEY_FRAGMENTS = (
    "token",
    "secret",
    "password",
    "passwd",
    "apikey",
    "api_key",
    "authorization",
    "cookie",
)
_KEY_LIKE_FRAGMENTS = ("api", "access", "account", "consumer", "private")

_STANDARD_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {
    "asctime",
    "message",
}


def _normalize_key(key: object) -> str:
    return str(key or "").strip().lower().replace("-", "_")


def is_sensitive_key(key: object) -> bool:
    normalized = _normalize_key(key)
    if not normalized:
        return False
    if normalized in _SENSITIVE_EXACT_KEYS:
        return True
    if any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS):
        return True
    return "key" in normalized and any(fragment in normalized for fragment in _KEY_LIKE_FRAGMENTS)


def redact_text(value: str) -> str:
    text = str(value)
    if not text:
        return text
    redacted = _BEARER_TOKEN_RE.sub(r"\1 " + REDACTED, text)
    redacted = _QUERY_SECRET_RE.sub(lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}", redacted)
    redacted = _CONNECTION_STRING_SECRET_RE.sub(lambda match: f"{match.group(1)}={REDACTED}", redacted)
    return redacted


def redact_exception_text(value: str) -> str:
    return redact_text(value)


def redact_value(value: Any, *, key_hint: object | None = None, _depth: int = 0, _seen: set[int] | None = None) -> Any:
    if key_hint is not None and is_sensitive_key(key_hint):
        return REDACTED if value is not None else None

    if value is None or isinstance(value, (bool, int, float)):
        return value

    if _depth >= 12:
        return redact_text(str(value))

    seen = _seen if _seen is not None else set()
    value_id = id(value)
    if value_id in seen:
        return "<cycle>"

    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, bytes):
        try:
            return redact_text(value.decode("utf-8", errors="replace"))
        except Exception:
            return REDACTED
    if isinstance(value, BaseException):
        return redact_text(f"{type(value).__name__}: {value}")

    if isinstance(value, Mapping):
        seen.add(value_id)
        try:
            return {
                key: redact_value(item, key_hint=key, _depth=_depth + 1, _seen=seen)
                for key, item in value.items()
            }
        finally:
            seen.discard(value_id)

    if isinstance(value, tuple):
        seen.add(value_id)
        try:
            return tuple(redact_value(item, _depth=_depth + 1, _seen=seen) for item in value)
        finally:
            seen.discard(value_id)

    if isinstance(value, list):
        seen.add(value_id)
        try:
            return [redact_value(item, _depth=_depth + 1, _seen=seen) for item in value]
        finally:
            seen.discard(value_id)

    if isinstance(value, set):
        seen.add(value_id)
        try:
            return {redact_value(item, _depth=_depth + 1, _seen=seen) for item in value}
        finally:
            seen.discard(value_id)

    return redact_text(str(value))


def _redact_log_record(record: logging.LogRecord) -> logging.LogRecord:
    safe_args = redact_value(record.args) if record.args else record.args
    try:
        message = str(record.msg)
        if safe_args:
            message = message % safe_args
        record.msg = redact_text(message)
        record.args = ()
    except Exception:
        record.msg = redact_text(f"{record.msg} {safe_args or ''}".strip())
        record.args = ()

    for key in list(record.__dict__):
        if key in _STANDARD_RECORD_FIELDS or key.startswith("_"):
            continue
        record.__dict__[key] = redact_value(record.__dict__[key], key_hint=key)

    if record.exc_info:
        try:
            record.exc_text = redact_exception_text("".join(traceback.format_exception(*record.exc_info)))
        except Exception:
            record.exc_text = None

    return record


def install_log_redaction() -> None:
    current_factory = logging.getLogRecordFactory()
    if getattr(current_factory, "_asset_allocation_redacting", False):
        factory_installed = True
    else:
        factory_installed = False

    if not factory_installed:
        def redacting_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
            return _redact_log_record(current_factory(*args, **kwargs))

        redacting_factory._asset_allocation_redacting = True  # type: ignore[attr-defined]
        logging.setLogRecordFactory(redacting_factory)

    current_make_record = logging.Logger.makeRecord
    if getattr(current_make_record, "_asset_allocation_redacting", False):
        make_record_installed = True
    else:
        make_record_installed = False

    if not make_record_installed:
        def redacting_make_record(self: logging.Logger, *args: Any, **kwargs: Any) -> logging.LogRecord:
            return _redact_log_record(current_make_record(self, *args, **kwargs))

        redacting_make_record._asset_allocation_redacting = True  # type: ignore[attr-defined]
        logging.Logger.makeRecord = redacting_make_record  # type: ignore[method-assign]

    current_format_exception = logging.Formatter.formatException
    if getattr(current_format_exception, "_asset_allocation_redacting", False):
        return

    def redacting_format_exception(self: logging.Formatter, ei: Any) -> str:
        return redact_exception_text(current_format_exception(self, ei))

    redacting_format_exception._asset_allocation_redacting = True  # type: ignore[attr-defined]
    logging.Formatter.formatException = redacting_format_exception  # type: ignore[method-assign]
