from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse


_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


class UnsafePathError(ValueError):
    pass


def validate_run_id(value: str) -> str:
    run_id = str(value or "").strip()
    if not run_id:
        raise ValueError("run_id is required.")
    if not _RUN_ID_PATTERN.match(run_id):
        raise ValueError("run_id must match /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/")
    return run_id


def validate_artifact_name(value: str) -> str:
    name = str(value or "").strip()
    if not name:
        raise ValueError("artifact name is required.")
    if "/" in name or "\\" in name or ".." in name:
        raise ValueError("artifact name must not contain path separators or '..'.")
    if len(name) > 256:
        raise ValueError("artifact name is too long.")
    return name


def resolve_under_base(base_dir: Path, candidate: Path) -> Path:
    base = base_dir.resolve(strict=False)
    resolved = candidate.resolve(strict=False)
    try:
        if not resolved.is_relative_to(base):
            raise UnsafePathError(f"Path escapes base dir: {candidate}")
    except AttributeError:
        # Python < 3.9 fallback, kept for completeness.
        if str(base) not in str(resolved):
            raise UnsafePathError(f"Path escapes base dir: {candidate}")
    return resolved


def resolve_local_data_path(path_text: str) -> Path:
    raw = str(path_text or "").strip()
    if not raw:
        raise ValueError("Path is empty.")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve(strict=False)
    else:
        path = path.resolve(strict=False)
    return path


def assert_path_under_allowlist(path_text: str, allowed_base_dirs: Iterable[Path]) -> Path:
    resolved = resolve_local_data_path(path_text)
    allowed = [p.resolve(strict=False) for p in allowed_base_dirs]
    for base in allowed:
        try:
            if resolved.is_relative_to(base):
                return resolved
        except AttributeError:
            if str(base) in str(resolved):
                return resolved
    raise UnsafePathError(f"Path {resolved} is not under allowed dirs: {[str(p) for p in allowed]}")


def parse_container_and_path(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text:
        raise ValueError("ADLS reference is empty.")

    if text.startswith("abfss://"):
        parsed = urlparse(text)
        if not parsed.netloc or "@" not in parsed.netloc:
            raise ValueError(f"Invalid abfss URI: {text!r}")
        container = parsed.netloc.split("@", 1)[0]
        path = parsed.path.lstrip("/")
        if not container or not path:
            raise ValueError(f"Invalid abfss URI: {text!r}")
        return container, path

    if "/" not in text:
        raise ValueError(f"ADLS reference must be 'container/path' or abfss://... (got {text!r}).")

    container, path = text.split("/", 1)
    container = container.strip()
    path = path.strip().lstrip("/")
    if not container or not path:
        raise ValueError(f"Invalid ADLS reference: {text!r}")
    return container, path


def assert_allowed_container(container: str, allowlist: Iterable[str]) -> None:
    allowed = {str(c).strip() for c in allowlist if str(c).strip()}
    if allowed and container not in allowed:
        raise ValueError(f"Container '{container}' is not allowed.")

