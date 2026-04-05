from __future__ import annotations

import hashlib
import re

_NON_IDENTIFIER = re.compile(r"[^a-z0-9]+")
_REPEATED_UNDERSCORES = re.compile(r"_+")


def slugify_strategy_output_table(name: str) -> str:
    raw = str(name or "").strip().lower()
    normalized = _NON_IDENTIFIER.sub("_", raw)
    normalized = _REPEATED_UNDERSCORES.sub("_", normalized).strip("_")
    if not normalized:
        normalized = "strategy_output"
    if normalized[0].isdigit():
        normalized = f"strategy_{normalized}"
    if len(normalized) <= 63:
        return normalized

    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
    prefix = normalized[:54].rstrip("_")
    return f"{prefix}_{digest}"


def build_scoped_identifier(*parts: str, limit: int = 63) -> str:
    normalized_parts = [
        _REPEATED_UNDERSCORES.sub("_", _NON_IDENTIFIER.sub("_", str(part or "").strip().lower())).strip("_")
        for part in parts
        if str(part or "").strip("_")
    ]
    identifier = "_".join(part for part in normalized_parts if part)
    if not identifier:
        identifier = "identifier"
    if identifier[0].isdigit():
        identifier = f"id_{identifier}"
    if len(identifier) <= limit:
        return identifier

    digest = hashlib.sha1(identifier.encode("utf-8")).hexdigest()[:8]
    suffix = normalized_parts[-1] if len(normalized_parts) > 1 else ""
    if suffix:
        prefix_limit = limit - len(digest) - len(suffix) - 2
        prefix = identifier[:prefix_limit].rstrip("_")
        if prefix:
            return f"{prefix}_{digest}_{suffix}"
        return f"{digest}_{suffix}"

    prefix = identifier[: limit - len(digest) - 1].rstrip("_")
    return f"{prefix}_{digest}"
