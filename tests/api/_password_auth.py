from __future__ import annotations

import base64
import hashlib


def password_verifier_for(secret: str, *, iterations: int = 600_000, salt: str = "unit-test-salt") -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(secret).encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"pbkdf2_sha256${iterations}${salt}${encoded_digest}"
