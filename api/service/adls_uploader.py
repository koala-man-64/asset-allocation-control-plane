from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from core.blob_storage import BlobStorageClient

from api.service.security import parse_container_and_path


@dataclass(frozen=True)
class UploadResult:
    container: str
    prefix: str
    manifest_path: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def upload_run_artifacts(
    *,
    run_id: str,
    run_dir: Path,
    adls_dir: str,
    ensure_container_exists: bool = True,
) -> UploadResult:
    """
    Uploads all files in `run_dir` to Azure Blob Storage under:
      <container>/<path>/<run_id>/<filename>

    Returns the remote location and writes `artifacts_manifest.json` locally.
    """
    container, prefix = parse_container_and_path(adls_dir)
    remote_prefix = f"{prefix.rstrip('/')}/{run_id}".strip("/")

    client = BlobStorageClient(container_name=container, ensure_container_exists=ensure_container_exists)

    files: List[Dict[str, object]] = []
    for item in sorted(run_dir.iterdir()):
        if not item.is_file():
            continue
        name = item.name
        remote_path = f"{remote_prefix}/{name}"
        size_bytes = int(item.stat().st_size)
        sha256 = _sha256_file(item)
        client.upload_file(str(item), remote_path)
        files.append(
            {
                "name": name,
                "remote_path": remote_path,
                "size_bytes": size_bytes,
                "sha256": sha256,
            }
        )

    manifest = {
        "run_id": run_id,
        "uploaded_at": _utc_now_iso(),
        "container": container,
        "prefix": remote_prefix,
        "files": files,
    }

    manifest_path = run_dir / "artifacts_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    client.upload_file(str(manifest_path), f"{remote_prefix}/{manifest_path.name}")

    return UploadResult(container=container, prefix=remote_prefix, manifest_path=f"{remote_prefix}/{manifest_path.name}")
