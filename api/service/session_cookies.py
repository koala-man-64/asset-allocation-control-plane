from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any

from cryptography.fernet import Fernet, InvalidToken


logger = logging.getLogger("api.service.session_cookies")

LEGACY_SESSION_PAYLOAD_VERSION = 1
SESSION_PAYLOAD_VERSION = 2
SAFE_SESSION_CLAIM_KEYS = {
    "appid",
    "azp",
    "email",
    "name",
    "oid",
    "preferred_username",
    "roles",
    "scp",
    "sub",
    "tid",
    "upn",
}


@dataclass(frozen=True)
class SessionCookieBundle:
    session_cookie: str
    csrf_token: str
    max_age_seconds: int


@dataclass(frozen=True)
class VerifiedSessionCookie:
    mode: str
    subject: str
    claims: dict[str, Any]
    csrf_token: str
    renewal: SessionCookieBundle | None = None


class SessionCookieError(Exception):
    def __init__(self, detail: str, *, cookie_present: bool = True):
        super().__init__(detail)
        self.detail = detail
        self.cookie_present = cookie_present


class SessionCookieManager:
    def __init__(
        self,
        *,
        enabled: bool,
        secret_keys: list[str],
        idle_ttl_seconds: int,
        absolute_ttl_seconds: int,
        session_version: str,
        cookie_name: str,
        csrf_cookie_name: str,
        secure: bool,
    ) -> None:
        self.enabled = bool(enabled)
        self.idle_ttl_seconds = int(idle_ttl_seconds)
        self.absolute_ttl_seconds = int(absolute_ttl_seconds)
        self.session_version = str(session_version or "1").strip() or "1"
        self.cookie_name = cookie_name
        self.csrf_cookie_name = csrf_cookie_name
        self.secure = bool(secure)
        self.same_site = "lax"
        self.path = "/"
        self._fernets = [_build_fernet(secret) for secret in secret_keys if str(secret or "").strip()]

        if self.enabled and not self._fernets:
            raise ValueError("API_AUTH_SESSION_SECRET_KEYS is required when API_AUTH_SESSION_MODE=cookie.")
        if self.enabled and self.idle_ttl_seconds <= 0:
            raise ValueError("API_AUTH_SESSION_IDLE_TTL_SECONDS must be positive.")
        if self.enabled and self.absolute_ttl_seconds < self.idle_ttl_seconds:
            raise ValueError("API_AUTH_SESSION_ABSOLUTE_TTL_SECONDS must be greater than or equal to idle TTL.")

    def issue(self, *, mode: str, subject: str | None, claims: dict[str, Any]) -> SessionCookieBundle:
        if not self.enabled:
            raise SessionCookieError("Cookie auth session mode is not enabled.")

        now = _now()
        csrf_token = secrets.token_urlsafe(32)
        payload = self._build_payload(
            mode=mode,
            subject=subject,
            claims=claims,
            csrf_token=csrf_token,
            issued_at=now,
            absolute_expires_at=now + self.absolute_ttl_seconds,
            idle_expires_at=now + self.idle_ttl_seconds,
        )
        logger.info(
            "Auth session cookie issued: subject=%s idle_ttl_seconds=%s absolute_ttl_seconds=%s",
            subject or "-",
            self.idle_ttl_seconds,
            self.absolute_ttl_seconds,
        )
        return SessionCookieBundle(
            session_cookie=self._encrypt_payload(payload),
            csrf_token=csrf_token,
            max_age_seconds=self.idle_ttl_seconds,
        )

    def verify(self, cookies: dict[str, str]) -> VerifiedSessionCookie:
        raw_cookie = str(cookies.get(self.cookie_name) or "").strip()
        if not raw_cookie:
            raise SessionCookieError("Missing auth session cookie.", cookie_present=False)

        payload = self._decrypt_payload(raw_cookie)
        now = _now()
        mode = str(payload.get("mode") or "oidc").strip() or "oidc"
        subject = str(payload.get("sub") or "").strip()
        csrf_token = str(payload.get("csrf") or "").strip()
        claims = payload.get("claims")
        absolute_expires_at = _coerce_int(payload.get("absExp"))
        idle_expires_at = _coerce_int(payload.get("idleExp"))
        issued_at = _coerce_int(payload.get("iat"))
        session_version = str(payload.get("sv") or "").strip()
        payload_version = _coerce_int(payload.get("v"))

        if payload_version not in {LEGACY_SESSION_PAYLOAD_VERSION, SESSION_PAYLOAD_VERSION}:
            raise SessionCookieError("Invalid auth session cookie.")
        if not subject or not csrf_token or not isinstance(claims, dict):
            raise SessionCookieError("Invalid auth session cookie.")
        if payload_version == SESSION_PAYLOAD_VERSION:
            if mode not in {"oidc", "password"}:
                raise SessionCookieError("Invalid auth session cookie.")
            if session_version != self.session_version:
                raise SessionCookieError("Auth session expired.")
        else:
            mode = "oidc"
        if absolute_expires_at <= now:
            logger.info("Auth session cookie expired by absolute TTL: subject=%s", subject)
            raise SessionCookieError("Auth session expired.")
        if idle_expires_at <= now:
            logger.info("Auth session cookie expired by idle TTL: subject=%s", subject)
            raise SessionCookieError("Auth session expired.")

        renewal = self._build_renewal(
            mode=mode,
            subject=subject,
            claims=claims,
            csrf_token=csrf_token,
            issued_at=issued_at,
            absolute_expires_at=absolute_expires_at,
            idle_expires_at=idle_expires_at,
            now=now,
        )
        if renewal:
            logger.info("Auth session cookie renewed: subject=%s", subject)

        return VerifiedSessionCookie(
            mode=mode,
            subject=subject,
            claims=dict(claims),
            csrf_token=csrf_token,
            renewal=renewal,
        )

    def set_cookies(self, response: Any, bundle: SessionCookieBundle) -> None:
        response.set_cookie(
            self.cookie_name,
            bundle.session_cookie,
            max_age=bundle.max_age_seconds,
            path=self.path,
            secure=self.secure,
            httponly=True,
            samesite=self.same_site,
        )
        response.set_cookie(
            self.csrf_cookie_name,
            bundle.csrf_token,
            max_age=bundle.max_age_seconds,
            path=self.path,
            secure=self.secure,
            httponly=False,
            samesite=self.same_site,
        )

    def clear_cookies(self, response: Any) -> None:
        response.delete_cookie(
            self.cookie_name,
            path=self.path,
            secure=self.secure,
            httponly=True,
            samesite=self.same_site,
        )
        response.delete_cookie(
            self.csrf_cookie_name,
            path=self.path,
            secure=self.secure,
            httponly=False,
            samesite=self.same_site,
        )

    def _build_renewal(
        self,
        *,
        mode: str,
        subject: str,
        claims: dict[str, Any],
        csrf_token: str,
        issued_at: int,
        absolute_expires_at: int,
        idle_expires_at: int,
        now: int,
    ) -> SessionCookieBundle | None:
        remaining_idle_seconds = idle_expires_at - now
        if remaining_idle_seconds > max(60, self.idle_ttl_seconds // 2):
            return None

        next_idle_expires_at = min(now + self.idle_ttl_seconds, absolute_expires_at)
        if next_idle_expires_at <= idle_expires_at:
            return None

        payload = self._build_payload(
            mode=mode,
            subject=subject,
            claims=claims,
            csrf_token=csrf_token,
            issued_at=issued_at,
            absolute_expires_at=absolute_expires_at,
            idle_expires_at=next_idle_expires_at,
        )
        return SessionCookieBundle(
            session_cookie=self._encrypt_payload(payload),
            csrf_token=csrf_token,
            max_age_seconds=max(1, min(self.idle_ttl_seconds, absolute_expires_at - now)),
        )

    def _build_payload(
        self,
        *,
        mode: str,
        subject: str | None,
        claims: dict[str, Any],
        csrf_token: str,
        issued_at: int,
        absolute_expires_at: int,
        idle_expires_at: int,
    ) -> dict[str, Any]:
        compact_claims = _compact_claims(claims)
        normalized_subject = str(subject or compact_claims.get("sub") or compact_claims.get("oid") or "").strip()
        if not normalized_subject:
            raise SessionCookieError("Cannot issue auth session cookie without a subject.")
        if "sub" not in compact_claims:
            compact_claims["sub"] = normalized_subject

        return {
            "v": SESSION_PAYLOAD_VERSION,
            "sv": self.session_version,
            "mode": str(mode or "oidc").strip() or "oidc",
            "sub": normalized_subject,
            "csrf": csrf_token,
            "iat": issued_at,
            "absExp": absolute_expires_at,
            "idleExp": idle_expires_at,
            "claims": compact_claims,
        }

    def _encrypt_payload(self, payload: dict[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return self._fernets[0].encrypt(encoded).decode("ascii")

    def _decrypt_payload(self, token: str) -> dict[str, Any]:
        for fernet in self._fernets:
            try:
                decoded = fernet.decrypt(token.encode("ascii"))
                payload = json.loads(decoded.decode("utf-8"))
            except (InvalidToken, UnicodeEncodeError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                return payload
        raise SessionCookieError("Invalid auth session cookie.")


def constant_time_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(str(left or ""), str(right or ""))


def _build_fernet(secret: str) -> Fernet:
    normalized = str(secret or "").strip()
    if len(normalized) < 32:
        raise ValueError("API_AUTH_SESSION_SECRET_KEYS entries must be at least 32 characters.")

    try:
        decoded = base64.urlsafe_b64decode(normalized.encode("ascii"))
    except Exception:
        decoded = b""

    if len(decoded) == 32:
        return Fernet(normalized.encode("ascii"))

    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _compact_claims(claims: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in SAFE_SESSION_CLAIM_KEYS:
        value = claims.get(key)
        if isinstance(value, str) and value.strip():
            compact[key] = value.strip()
        elif isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            if items:
                compact[key] = items
    return compact


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _now() -> int:
    return int(time.time())
