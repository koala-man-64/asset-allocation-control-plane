"""Flat-file (S3-compatible) helpers for Massive.

Massive offers historical "flat files" through an S3-compatible endpoint.

This module purposefully keeps the dependency on ``boto3`` optional. If you
want to use these helpers, install it in your runtime environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from massive_provider.config import MassiveConfig, _strip_or_none

try:  # optional dependency
    import boto3  # type: ignore
    from botocore.config import Config as _BotoConfig  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None
    _BotoConfig = None


class MassiveFlatFilesDependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class MassiveFlatFilesCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None

    @staticmethod
    def from_env() -> "MassiveFlatFilesCredentials":
        ak = _strip_or_none(os.environ.get("MASSIVE_FLATFILES_ACCESS_KEY_ID"))
        sk = _strip_or_none(os.environ.get("MASSIVE_FLATFILES_SECRET_ACCESS_KEY"))
        st = _strip_or_none(os.environ.get("MASSIVE_FLATFILES_SESSION_TOKEN"))

        if not ak or not sk:
            raise ValueError(
                "MASSIVE_FLATFILES_ACCESS_KEY_ID and MASSIVE_FLATFILES_SECRET_ACCESS_KEY are required for flat files."
            )
        return MassiveFlatFilesCredentials(access_key_id=str(ak), secret_access_key=str(sk), session_token=st)


class MassiveFlatFilesClient:
    """Minimal S3 client for Massive flat files."""

    def __init__(
        self,
        config: MassiveConfig,
        *,
        credentials: MassiveFlatFilesCredentials,
        region: str = "us-east-1",
    ) -> None:
        if boto3 is None:
            raise MassiveFlatFilesDependencyError(
                "boto3 is not installed. Install it to use Massive flat file helpers (pip install boto3)."
            )

        self.config = config
        self.credentials = credentials
        self.region = str(region)

        # S3-compatible endpoint (not AWS). Force path-style to avoid DNS / TLS
        # issues with bucket subdomains.
        s3_cfg = _BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"})
        self._s3 = boto3.client(
            "s3",
            endpoint_url=str(config.flatfiles_endpoint_url).rstrip("/"),
            region_name=self.region,
            aws_access_key_id=credentials.access_key_id,
            aws_secret_access_key=credentials.secret_access_key,
            aws_session_token=credentials.session_token,
            config=s3_cfg,
        )

    def list_keys(self, *, prefix: str, max_keys: int = 1000) -> list[str]:
        resp = self._s3.list_objects_v2(Bucket=self.config.flatfiles_bucket, Prefix=str(prefix), MaxKeys=int(max_keys))
        out: list[str] = []
        for obj in (resp.get("Contents") or []):
            key = obj.get("Key")
            if isinstance(key, str):
                out.append(key)
        return out

    def download(self, *, key: str, dest_path: str) -> str:
        """Download an object key from Massive flat files into ``dest_path``."""

        self._s3.download_file(self.config.flatfiles_bucket, str(key), str(dest_path))
        return str(dest_path)
