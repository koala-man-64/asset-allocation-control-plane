"""Small utilities used throughout the Massive integration."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any, Mapping


def to_jsonable(value: Any) -> Any:
    """Best-effort conversion of SDK models to JSON-able values.

    The official Massive SDK returns typed model objects (pydantic-like or
    dataclass-like depending on the release). For this project, we generally want
    plain dicts/lists so we can return JSON via FastAPI and/or serialize to parquet
    without pulling in SDK internals.
    """

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]

    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}

    if dataclasses.is_dataclass(value):
        return to_jsonable(dataclasses.asdict(value))

    # Pydantic v2
    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        try:
            return to_jsonable(value.model_dump())
        except Exception:
            pass

    # Pydantic v1
    if hasattr(value, "dict") and callable(getattr(value, "dict")):
        try:
            return to_jsonable(value.dict())
        except Exception:
            pass

    # Plain object
    if hasattr(value, "__dict__"):
        try:
            raw = {k: v for k, v in vars(value).items() if not str(k).startswith("_")}
            if raw:
                return to_jsonable(raw)
        except Exception:
            pass

    # Fallback
    return str(value)


def ms_to_iso_date(timestamp_ms: Any) -> str:
    """Convert an epoch-milliseconds timestamp to YYYY-MM-DD (UTC)."""
    try:
        ms = int(timestamp_ms)
    except Exception:
        raise ValueError(f"Invalid timestamp_ms={timestamp_ms!r}")
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.date().isoformat()


def filter_none(params: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of ``params`` without any ``None`` values."""
    out: dict[str, Any] = {}
    for k, v in params.items():
        if v is None:
            continue
        out[str(k)] = v
    return out
