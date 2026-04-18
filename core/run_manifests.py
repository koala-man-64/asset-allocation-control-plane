from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.datetime_utils import parse_utc_datetime, utc_isoformat
_MANIFEST_VERSION = 1
_ROOT_PREFIX = "system/run-manifests"
_SILVER_FINANCE_PREFIX = f"{_ROOT_PREFIX}/silver_finance"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return utc_isoformat(dt)


def _parse_iso(raw: Any) -> Optional[datetime]:
    return parse_utc_datetime(raw)


def _run_id(prefix: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    token = uuid.uuid4().hex[:8]
    return f"{prefix}-{now}-{token}"


def _require_common_storage(action: str) -> bool:
    if getattr(mdc, "common_storage_client", None) is None:
        mdc.write_warning(f"Skipping {action}: common storage client is not initialized.")
        return False
    return True


def _normalize_domain(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _domain_slug(value: str) -> str:
    return _normalize_domain(value).replace("-", "_")


def _manifest_root_for_domain(domain: str) -> str:
    return f"{_ROOT_PREFIX}/bronze_{_domain_slug(domain)}"


def _load_common_manifest_json(path: str, *, missing_message: str) -> Optional[Dict[str, Any]]:
    raw = mdc.read_raw_bytes(
        path,
        client=mdc.common_storage_client,
        missing_ok=True,
        missing_message=missing_message,
    )
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        mdc.write_warning(f"Failed to decode manifest JSON from {path}: {exc}")
        return None


def _normalize_blob_entry(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name = str(raw.get("name", "") or raw.get("path", "")).strip()
    if not name:
        return None

    entry: Dict[str, Any] = {"name": name}
    bucket = str(raw.get("bucket", "")).strip().upper()
    if bucket:
        entry["bucket"] = bucket
    etag = raw.get("etag")
    if etag is not None:
        entry["etag"] = str(etag)
    lm = _parse_iso(raw.get("last_modified"))
    if lm is not None:
        entry["last_modified"] = _iso(lm)
    size = raw.get("size")
    if isinstance(size, int):
        entry["size"] = int(size)
    return entry


def _normalize_bucket_paths(items: Iterable[Any]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, str):
            parsed = _normalize_blob_entry({"name": item})
        elif isinstance(item, dict):
            parsed = _normalize_blob_entry(item)
        else:
            parsed = None
        if parsed is not None:
            normalized.append(parsed)
    normalized.sort(key=lambda item: str(item.get("name", "")))
    return normalized


def _finance_manifest_payload(
    *,
    producer_job_name: str,
    listed_blobs: Iterable[Any],
    metadata: Optional[Dict[str, Any]],
    run_id: Optional[str] = None,
) -> tuple[Dict[str, Any], Dict[str, Any], str, str]:
    normalized_blobs = _normalize_bucket_paths(listed_blobs)
    manifest_run_id = str(run_id or _run_id("bronze-finance")).strip()
    produced_at = datetime.now(timezone.utc)
    manifest_root = _manifest_root_for_domain("finance")
    manifest_path = f"{manifest_root}/{manifest_run_id}.json"
    latest_path = f"{manifest_root}/latest.json"
    manifest = {
        "version": _MANIFEST_VERSION,
        "manifestType": "bronze-finance",
        "domain": "finance",
        "runId": manifest_run_id,
        "producerJobName": str(producer_job_name or "").strip(),
        "producedAt": _iso(produced_at),
        "blobPrefix": "finance-data/",
        "blobCount": len(normalized_blobs),
        "blobs": normalized_blobs,
        # Keep the alpha26 aliases so newer consumers can read a consistent shape.
        "dataPrefix": "finance-data",
        "bucketCount": len(normalized_blobs),
        "bucketPaths": normalized_blobs,
        "indexPath": None,
        "metadata": dict(metadata or {}),
    }
    latest_payload = {
        "version": _MANIFEST_VERSION,
        "manifestType": "bronze-finance-latest",
        "domain": "finance",
        "runId": manifest_run_id,
        "manifestPath": manifest_path,
        "updatedAt": _iso(produced_at),
        "blobCount": len(normalized_blobs),
        "dataPrefix": manifest["dataPrefix"],
        "bucketCount": len(normalized_blobs),
        "indexPath": None,
    }
    return manifest, latest_payload, manifest_path, latest_path


def create_bronze_alpha26_manifest(
    *,
    domain: str,
    producer_job_name: str,
    data_prefix: str,
    bucket_paths: Iterable[Any],
    index_path: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
    run_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not _require_common_storage("bronze alpha26 manifest write"):
        return None

    normalized_domain = _normalize_domain(domain)
    normalized_bucket_paths = _normalize_bucket_paths(bucket_paths)
    manifest_run_id = str(run_id or _run_id(f"bronze-{_domain_slug(normalized_domain)}")).strip()
    produced_at = datetime.now(timezone.utc)
    manifest_root = _manifest_root_for_domain(normalized_domain)
    manifest_path = f"{manifest_root}/{manifest_run_id}.json"
    latest_path = f"{manifest_root}/latest.json"
    manifest = {
        "version": _MANIFEST_VERSION,
        "manifestType": "bronze-alpha26",
        "domain": normalized_domain,
        "runId": manifest_run_id,
        "producerJobName": str(producer_job_name or "").strip(),
        "producedAt": _iso(produced_at),
        "dataPrefix": str(data_prefix or "").strip().strip("/"),
        "bucketCount": len(normalized_bucket_paths),
        "bucketPaths": normalized_bucket_paths,
        "indexPath": str(index_path or "").strip() or None,
        "metadata": dict(metadata or {}),
    }
    latest_payload = {
        "version": _MANIFEST_VERSION,
        "manifestType": "bronze-alpha26-latest",
        "domain": normalized_domain,
        "runId": manifest_run_id,
        "manifestPath": manifest_path,
        "updatedAt": _iso(produced_at),
        "dataPrefix": manifest["dataPrefix"],
        "bucketCount": len(normalized_bucket_paths),
        "indexPath": manifest["indexPath"],
    }
    try:
        mdc.save_common_json_content(manifest, manifest_path)
        mdc.save_common_json_content(latest_payload, latest_path)
    except Exception as exc:
        mdc.write_warning(f"Failed to persist bronze alpha26 manifest for domain={normalized_domain}: {exc}")
        return None

    return {
        "runId": manifest_run_id,
        "manifestPath": manifest_path,
        "bucketCount": len(normalized_bucket_paths),
        "dataPrefix": manifest["dataPrefix"],
        "indexPath": manifest["indexPath"],
    }


def load_latest_bronze_alpha26_manifest(domain: str) -> Optional[Dict[str, Any]]:
    if not _require_common_storage("bronze alpha26 manifest read"):
        return None

    latest_path = f"{_manifest_root_for_domain(domain)}/latest.json"
    latest = _load_common_manifest_json(
        latest_path,
        missing_message=f"Bronze alpha26 manifest pointer missing for domain={_normalize_domain(domain)}.",
    )
    if not isinstance(latest, dict):
        return None

    manifest_path = str(latest.get("manifestPath") or "").strip()
    if not manifest_path:
        return None

    manifest = _load_common_manifest_json(
        manifest_path,
        missing_message=f"Bronze alpha26 manifest blob missing for domain={_normalize_domain(domain)} path={manifest_path}.",
    )
    if not isinstance(manifest, dict):
        return None
    out = dict(manifest)
    out.setdefault("manifestPath", manifest_path)
    return out


def resolve_active_bronze_alpha26_prefix(domain: str) -> Optional[str]:
    manifest = load_latest_bronze_alpha26_manifest(domain)
    if not isinstance(manifest, dict):
        return None
    data_prefix = str(manifest.get("dataPrefix") or "").strip().strip("/")
    return data_prefix or None


def manifest_blobs(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = manifest.get("bucketPaths")
    if not isinstance(raw, list):
        raw = manifest.get("blobs")
    if not isinstance(raw, list):
        return []
    normalized = _normalize_bucket_paths(raw)
    produced_at = _parse_iso(manifest.get("producedAt") or manifest.get("updatedAt"))
    if produced_at is None:
        return normalized
    produced_at_iso = _iso(produced_at)
    for entry in normalized:
        entry.setdefault("last_modified", produced_at_iso)
    return normalized


def create_bronze_finance_manifest(
    *,
    producer_job_name: str,
    listed_blobs: List[Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not _require_common_storage("bronze finance manifest write"):
        return None

    manifest, latest_payload, manifest_path, latest_path = _finance_manifest_payload(
        producer_job_name=producer_job_name,
        listed_blobs=listed_blobs,
        metadata=metadata,
    )
    try:
        mdc.save_common_json_content(manifest, manifest_path)
        mdc.save_common_json_content(latest_payload, latest_path)
    except Exception as exc:
        mdc.write_warning(f"Failed to persist bronze finance manifest: {exc}")
        return None
    return {
        "runId": manifest["runId"],
        "manifestPath": manifest_path,
        "blobCount": manifest["blobCount"],
        "dataPrefix": manifest["dataPrefix"],
    }


def load_latest_bronze_finance_manifest() -> Optional[Dict[str, Any]]:
    manifest = load_latest_bronze_alpha26_manifest("finance")
    if not isinstance(manifest, dict):
        return None

    out = dict(manifest)
    normalized_blobs = manifest_blobs(out)
    if normalized_blobs:
        out.setdefault("blobs", [dict(item) for item in normalized_blobs])
        out.setdefault("bucketPaths", [dict(item) for item in normalized_blobs])
        out.setdefault("blobCount", len(normalized_blobs))
        out.setdefault("bucketCount", len(normalized_blobs))
    out.setdefault("blobPrefix", "finance-data/")
    out.setdefault("dataPrefix", "finance-data")
    return out


def silver_finance_ack_exists(run_id: str) -> bool:
    if not run_id:
        return False
    if not _require_common_storage("silver finance manifest ack read"):
        return False

    path = f"{_SILVER_FINANCE_PREFIX}/{run_id}.json"
    existing = mdc.get_common_json_content(path)
    return isinstance(existing, dict)


def write_silver_finance_ack(
    *,
    run_id: str,
    manifest_path: str,
    status: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if not run_id:
        return None
    if not _require_common_storage("silver finance manifest ack write"):
        return None

    payload = {
        "version": _MANIFEST_VERSION,
        "manifestType": "silver-finance-ack",
        "runId": str(run_id).strip(),
        "manifestPath": str(manifest_path or "").strip(),
        "status": str(status or "").strip().lower() or "unknown",
        "recordedAt": _iso(datetime.now(timezone.utc)),
        "metadata": dict(metadata or {}),
    }
    ack_path = f"{_SILVER_FINANCE_PREFIX}/{run_id}.json"
    try:
        mdc.save_common_json_content(payload, ack_path)
    except Exception as exc:
        mdc.write_warning(f"Failed to persist silver finance manifest ack for runId={run_id}: {exc}")
        return None
    return ack_path
