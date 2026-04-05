from __future__ import annotations

import json
import logging
import mimetypes
import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from core import core as mdc
from core.datetime_utils import utc_isoformat

logger = logging.getLogger(__name__)

_REMOTE_ROOT = "backtests"


def _utc_iso(value: Any | None = None) -> str | None:
    current = datetime.now(timezone.utc) if value is None else value
    return utc_isoformat(current)


def _common_storage_available() -> bool:
    return getattr(mdc, "common_storage_client", None) is not None


def _local_root() -> Path:
    configured = str(os.environ.get("BACKTEST_OUTPUT_DIR") or "").strip()
    root = Path(configured or "/tmp/backtest_results")
    return root / "common" / _REMOTE_ROOT


def artifact_prefix(run_id: str) -> str:
    return f"{_REMOTE_ROOT}/{run_id}"


def _local_path(run_id: str, artifact_name: str) -> Path:
    return _local_root() / run_id / artifact_name


def _write_local_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def write_json_artifact(run_id: str, artifact_name: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, indent=2).encode("utf-8")
    remote_path = f"{artifact_prefix(run_id)}/{artifact_name}"
    if _common_storage_available():
        mdc.common_storage_client.upload_data(remote_path, encoded, overwrite=True)
    else:
        _write_local_bytes(_local_path(run_id, artifact_name), encoded)
    return remote_path


def write_text_artifact(run_id: str, artifact_name: str, text: str) -> str:
    payload = text.encode("utf-8")
    remote_path = f"{artifact_prefix(run_id)}/{artifact_name}"
    if _common_storage_available():
        mdc.common_storage_client.upload_data(remote_path, payload, overwrite=True)
    else:
        _write_local_bytes(_local_path(run_id, artifact_name), payload)
    return remote_path


def write_parquet_artifact(run_id: str, artifact_name: str, frame: pd.DataFrame) -> str:
    remote_path = f"{artifact_prefix(run_id)}/{artifact_name}"
    if _common_storage_available():
        mdc.common_storage_client.write_parquet(remote_path, frame)
    else:
        path = _local_path(run_id, artifact_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
    return remote_path


def read_json_artifact(run_id: str, artifact_name: str) -> dict[str, Any] | None:
    raw = read_artifact_bytes(run_id, artifact_name)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def read_parquet_artifact(run_id: str, artifact_name: str) -> pd.DataFrame:
    raw = read_artifact_bytes(run_id, artifact_name)
    if not raw:
        return pd.DataFrame()
    try:
        return pd.read_parquet(BytesIO(raw))
    except Exception as exc:
        logger.warning("Failed to read parquet artifact %s/%s: %s", run_id, artifact_name, exc)
        return pd.DataFrame()


def read_artifact_bytes(run_id: str, artifact_name: str) -> bytes | None:
    remote_path = f"{artifact_prefix(run_id)}/{artifact_name}"
    if _common_storage_available():
        return mdc.common_storage_client.download_data(remote_path)
    path = _local_path(run_id, artifact_name)
    if not path.exists() or not path.is_file():
        return None
    return path.read_bytes()


def list_artifacts(run_id: str) -> list[dict[str, Any]]:
    prefix = artifact_prefix(run_id)
    if _common_storage_available():
        infos = mdc.common_storage_client.list_blob_infos(name_starts_with=prefix)
        artifacts: list[dict[str, Any]] = []
        for info in infos:
            name = str(info.get("name") or "")
            if not name.startswith(f"{prefix}/"):
                continue
            relative = name[len(prefix) + 1 :]
            content_type, _ = mimetypes.guess_type(relative)
            artifacts.append(
                {
                    "name": relative,
                    "path": name,
                    "size": info.get("size"),
                    "updatedAt": _utc_iso(info.get("last_modified")) if info.get("last_modified") else None,
                    "contentType": content_type or "application/octet-stream",
                }
            )
        return sorted(artifacts, key=lambda item: str(item["name"]))

    root = _local_root() / run_id
    if not root.exists():
        return []
    artifacts = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        content_type, _ = mimetypes.guess_type(relative)
        artifacts.append(
            {
                "name": relative,
                "path": str(path),
                "size": path.stat().st_size,
                "updatedAt": _utc_iso(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)),
                "contentType": content_type or "application/octet-stream",
            }
        )
    return artifacts


def write_manifest(run_id: str) -> str:
    payload = {
        "runId": run_id,
        "rootPrefix": artifact_prefix(run_id),
        "updatedAt": _utc_iso(),
        "artifacts": list_artifacts(run_id),
    }
    return write_json_artifact(run_id, "manifest.json", payload)
