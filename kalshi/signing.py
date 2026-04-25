from __future__ import annotations

import base64
import time
from typing import Any
from urllib.parse import urlparse

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def load_private_key(private_key_pem: str) -> Any:
    return serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)


def sign_request(private_key: Any, timestamp: str, method: str, path: str) -> str:
    path_without_query = path.split("?", 1)[0]
    message = f"{timestamp}{method.upper()}{path_without_query}".encode("utf-8")
    signed = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signed).decode("utf-8")


def build_auth_headers(
    private_key: Any,
    api_key_id: str,
    *,
    method: str,
    base_url: str,
    endpoint: str,
    content_type: str | None = None,
) -> dict[str, str]:
    timestamp = str(int(time.time() * 1000))
    sign_path = urlparse(base_url.rstrip("/") + endpoint).path
    headers = {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": sign_request(private_key, timestamp, method, sign_path),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }
    if content_type:
        headers["Content-Type"] = content_type
    return headers
