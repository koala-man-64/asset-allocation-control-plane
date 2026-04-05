from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple

from azure.core.exceptions import ResourceNotFoundError

from core.blob_storage import BlobStorageClient

from monitoring.delta_log import find_latest_delta_version, parse_last_checkpoint_version


logger = logging.getLogger("asset_allocation.monitoring.azure_blob_store")


@dataclass(frozen=True)
class AzureBlobStoreConfig:
    account_name: Optional[str]
    connection_string: Optional[str]

    @staticmethod
    def from_env() -> "AzureBlobStoreConfig":
        account_name_raw = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        account_name = account_name_raw.strip() if account_name_raw and account_name_raw.strip() else None
        connection_string_raw = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        connection_string = (
            connection_string_raw.strip() if connection_string_raw and connection_string_raw.strip() else None
        )
        return AzureBlobStoreConfig(account_name=account_name, connection_string=connection_string)


@dataclass(frozen=True)
class LastModifiedProbeResult:
    state: str
    last_modified: Optional[datetime] = None
    error: Optional[str] = None


class AzureBlobStore:
    """
    Minimal data-plane helper for Azure Blob Storage used by monitoring probes.

    Uses either:
    - AZURE_STORAGE_CONNECTION_STRING, or
    - AZURE_STORAGE_ACCOUNT_NAME + DefaultAzureCredential (Managed Identity)
    """

    def __init__(self, cfg: AzureBlobStoreConfig):
        self._cfg = cfg
        self._clients: Dict[str, BlobStorageClient] = {}

    def _client(self, container: str) -> BlobStorageClient:
        container_name = container.strip()
        if not container_name:
            raise ValueError("container name is required")
        existing = self._clients.get(container_name)
        if existing is not None:
            return existing
        client = BlobStorageClient(
            account_name=self._cfg.account_name,
            connection_string=self._cfg.connection_string,
            container_name=container_name,
            ensure_container_exists=False,
        )
        self._clients[container_name] = client
        return client

    def get_blob_last_modified(self, *, container: str, blob_name: str) -> Optional[datetime]:
        client = self._client(container)
        blob_client = client.container_client.get_blob_client(blob_name)
        try:
            props = blob_client.get_blob_properties()
        except ResourceNotFoundError:
            return None
        return props.last_modified

    def _delta_log_prefix(self, table_path: str) -> str:
        base = table_path.strip().lstrip("/").rstrip("/")
        return f"{base}/_delta_log/"

    def get_delta_table_last_modified(self, *, container: str, table_path: str) -> Tuple[Optional[int], Optional[datetime]]:
        """
        Returns (version, last-modified) of the latest Delta commit JSON file for a table.

        This is an efficient probe that avoids listing the entire container by:
        - reading _delta_log/_last_checkpoint (when present) to pick a start version
        - probing commit JSON existence via exponential + binary search
        """
        client = self._client(container)
        delta_prefix = self._delta_log_prefix(table_path)
        checkpoint_blob = f"{delta_prefix}_last_checkpoint"

        start_version = 0
        try:
            checkpoint_bytes = client.download_data(checkpoint_blob)
        except ResourceNotFoundError:
            checkpoint_bytes = None

        if checkpoint_bytes:
            parsed = parse_last_checkpoint_version(checkpoint_bytes.decode("utf-8", errors="replace"))
            if parsed is not None:
                start_version = parsed

        def _commit_exists(version: int) -> bool:
            name = f"{delta_prefix}{version:020d}.json"
            return client.file_exists(name)

        latest_version = find_latest_delta_version(_commit_exists, start_version=start_version)
        if latest_version is None:
            return None, None

        latest_blob = f"{delta_prefix}{latest_version:020d}.json"
        return latest_version, self.get_blob_last_modified(container=container, blob_name=latest_blob)

    def probe_container_last_modified(
        self, *, container: str, prefix: Optional[str] = None
    ) -> LastModifiedProbeResult:
        """
        Recursively probes the latest `last_modified` among blobs in the container
        (optionally filtered by prefix).

        Returns a typed state so callers can separate "not found" from real probe errors.
        """
        client = self._client(container)
        try:
            # list_blobs(name_starts_with=prefix) returns a flat listing of all blobs matching the prefix,
            # effectively recursing into all "subfolders" (virtual directories).
            # We iterate directly to find the max last_modified without loading all into memory.
            blobs_iter = client.container_client.list_blobs(name_starts_with=prefix)

            max_lm: Optional[datetime] = None
            for blob in blobs_iter:
                lm = blob.last_modified
                if lm and (max_lm is None or lm > max_lm):
                    max_lm = lm

            if max_lm is None:
                return LastModifiedProbeResult(state="not_found")
            return LastModifiedProbeResult(state="ok", last_modified=max_lm)
        except Exception as exc:
            logger.warning(
                "Container last-modified probe failed: container=%s prefix=%s error=%s",
                container,
                prefix or "",
                exc,
                exc_info=True,
            )
            return LastModifiedProbeResult(state="error", error=str(exc))

    def get_container_last_modified(self, *, container: str, prefix: Optional[str] = None) -> Optional[datetime]:
        """
        Backward-compatible helper that returns only the timestamp.
        """
        result = self.probe_container_last_modified(container=container, prefix=prefix)
        return result.last_modified

