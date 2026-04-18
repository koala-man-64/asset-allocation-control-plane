from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from asset_allocation_runtime_common.foundation.blob_storage import BlobStorageClient
from asset_allocation_runtime_common.foundation.datetime_utils import utc_isoformat
@dataclass(frozen=True)
class ArtifactInfo:
    name: str
    size_bytes: int
    last_modified: Optional[str]


def list_local_artifacts(run_dir: Path) -> List[ArtifactInfo]:
    if not run_dir.exists() or not run_dir.is_dir():
        return []
    infos: List[ArtifactInfo] = []
    for item in sorted(run_dir.iterdir()):
        if not item.is_file():
            continue
        stat = item.stat()
        infos.append(
            ArtifactInfo(
                name=item.name,
                size_bytes=int(stat.st_size),
                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            )
        )
    return infos


def list_remote_artifacts(*, container: str, prefix: str) -> List[ArtifactInfo]:
    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    infos: List[ArtifactInfo] = []
    for blob in client.list_blob_infos(name_starts_with=f"{prefix.rstrip('/')}/"):
        name = blob.get("name")
        if not name or name.endswith("/"):
            continue
        # return only the basename relative to prefix
        rel = name[len(prefix.rstrip('/') + "/") :] if name.startswith(prefix.rstrip('/') + "/") else name
        infos.append(
            ArtifactInfo(
                name=rel,
                size_bytes=int(blob.get("size") or 0),
                last_modified=utc_isoformat(blob.get("last_modified")),
            )
        )
    infos.sort(key=lambda item: item.last_modified or "", reverse=True)
    return infos


def download_remote_artifact(*, container: str, remote_path: str) -> Optional[bytes]:
    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    return client.download_data(remote_path)

