from __future__ import annotations

import base64
import hashlib
import hmac
import threading
import time
from collections import deque
from dataclasses import dataclass, field


def _base64url_decode(value: str) -> bytes:
    normalized = str(value or "").strip()
    padding = "=" * (-len(normalized) % 4)
    return base64.urlsafe_b64decode((normalized + padding).encode("ascii"))


def _verify_pbkdf2_sha256(verifier: str, password: str) -> bool:
    algorithm, iteration_text, salt_text, digest_text = verifier.split("$", 3)
    if algorithm != "pbkdf2_sha256":
        return False

    iterations = int(iteration_text)
    if iterations <= 0:
        raise ValueError("PBKDF2 password verifier iterations must be positive.")

    salt = salt_text.encode("utf-8")
    expected_digest = _base64url_decode(digest_text)
    actual_digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt,
        iterations,
        dklen=len(expected_digest),
    )
    return hmac.compare_digest(actual_digest, expected_digest)


def _verify_scrypt(verifier: str, password: str) -> bool:
    algorithm, n_text, r_text, p_text, salt_text, digest_text = verifier.split("$", 5)
    if algorithm != "scrypt":
        return False

    n = int(n_text)
    r = int(r_text)
    p = int(p_text)
    if n <= 1 or r <= 0 or p <= 0:
        raise ValueError("Scrypt password verifier parameters must be positive.")

    salt = _base64url_decode(salt_text)
    expected_digest = _base64url_decode(digest_text)
    actual_digest = hashlib.scrypt(
        str(password or "").encode("utf-8"),
        salt=salt,
        n=n,
        r=r,
        p=p,
        dklen=len(expected_digest),
    )
    return hmac.compare_digest(actual_digest, expected_digest)


def validate_password_hash_format(verifier: str) -> None:
    normalized = str(verifier or "").strip()
    if not normalized:
        raise ValueError("UI_SHARED_PASSWORD_HASH must not be blank.")

    prefix = normalized.split("$", 1)[0]
    if prefix not in {"pbkdf2_sha256", "scrypt"}:
        raise ValueError("UI_SHARED_PASSWORD_HASH must start with 'pbkdf2_sha256$' or 'scrypt$'.")

    try:
        if prefix == "pbkdf2_sha256":
            _verify_pbkdf2_sha256(normalized, "")
            return
        _verify_scrypt(normalized, "")
    except Exception as exc:
        raise ValueError("UI_SHARED_PASSWORD_HASH is not a valid supported password verifier.") from exc


def verify_password_hash(verifier: str, password: str) -> bool:
    normalized = str(verifier or "").strip()
    prefix = normalized.split("$", 1)[0]
    if prefix == "pbkdf2_sha256":
        return _verify_pbkdf2_sha256(normalized, password)
    if prefix == "scrypt":
        return _verify_scrypt(normalized, password)
    raise ValueError("UI_SHARED_PASSWORD_HASH must start with 'pbkdf2_sha256$' or 'scrypt$'.")


class PasswordRateLimitError(Exception):
    def __init__(self, retry_after_seconds: int):
        super().__init__("Too many failed login attempts. Try again later.")
        self.retry_after_seconds = max(1, int(retry_after_seconds))


@dataclass
class _AttemptBucket:
    attempts: deque[float] = field(default_factory=deque)


class PasswordAttemptRateLimiter:
    def __init__(
        self,
        *,
        window_seconds: int,
        max_attempts_per_ip: int,
        max_attempts_global: int,
    ) -> None:
        self._window_seconds = max(1, int(window_seconds))
        self._max_attempts_per_ip = max(1, int(max_attempts_per_ip))
        self._max_attempts_global = max(1, int(max_attempts_global))
        self._lock = threading.RLock()
        self._global_attempts: deque[float] = deque()
        self._attempts_by_ip: dict[str, _AttemptBucket] = {}

    def check(self, client_ip: str) -> None:
        now = time.monotonic()
        normalized_ip = str(client_ip or "unknown").strip() or "unknown"

        with self._lock:
            self._prune(now)
            ip_bucket = self._attempts_by_ip.get(normalized_ip)
            ip_attempts = len(ip_bucket.attempts) if ip_bucket is not None else 0
            global_attempts = len(self._global_attempts)

            if ip_attempts >= self._max_attempts_per_ip:
                oldest = ip_bucket.attempts[0]
                raise PasswordRateLimitError(self._retry_after_seconds(now, oldest))
            if global_attempts >= self._max_attempts_global:
                oldest = self._global_attempts[0]
                raise PasswordRateLimitError(self._retry_after_seconds(now, oldest))

    def record_failure(self, client_ip: str) -> None:
        now = time.monotonic()
        normalized_ip = str(client_ip or "unknown").strip() or "unknown"

        with self._lock:
            self._prune(now)
            self._global_attempts.append(now)
            bucket = self._attempts_by_ip.setdefault(normalized_ip, _AttemptBucket())
            bucket.attempts.append(now)

    def record_success(self, client_ip: str) -> None:
        normalized_ip = str(client_ip or "unknown").strip() or "unknown"
        with self._lock:
            self._attempts_by_ip.pop(normalized_ip, None)

    def _prune(self, now: float) -> None:
        cutoff = now - float(self._window_seconds)
        while self._global_attempts and self._global_attempts[0] <= cutoff:
            self._global_attempts.popleft()

        stale_ips: list[str] = []
        for client_ip, bucket in self._attempts_by_ip.items():
            while bucket.attempts and bucket.attempts[0] <= cutoff:
                bucket.attempts.popleft()
            if not bucket.attempts:
                stale_ips.append(client_ip)

        for client_ip in stale_ips:
            self._attempts_by_ip.pop(client_ip, None)

    def _retry_after_seconds(self, now: float, oldest_attempt: float) -> int:
        elapsed = max(0.0, now - oldest_attempt)
        remaining = int(self._window_seconds - elapsed) + 1
        return max(1, remaining)
