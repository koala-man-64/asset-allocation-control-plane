from __future__ import annotations

import json
import logging
import hashlib
import ipaddress
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import jwt
from jwt.exceptions import InvalidTokenError
from jwt.algorithms import RSAAlgorithm
import requests

from api.service.settings import AuthMode, ServiceSettings
from api.service.password_auth import PasswordAttemptRateLimiter, PasswordRateLimitError, verify_password_hash
from api.service.session_cookies import SessionCookieBundle, SessionCookieError, SessionCookieManager


logger = logging.getLogger("api.service.auth")


@dataclass(frozen=True)
class AuthContext:
    mode: AuthMode
    subject: Optional[str]
    claims: Dict[str, Any]
    source: str = "bearer"
    csrf_token: Optional[str] = None
    session_renewal: Optional[SessionCookieBundle] = None
    session_id: Optional[str] = None


class AuthError(Exception):
    def __init__(self, *, status_code: int, detail: str, www_authenticate: Optional[str] = None):
        super().__init__(detail)
        self.status_code = int(status_code)
        self.detail = str(detail)
        self.www_authenticate = www_authenticate


def _fetch_openid_configuration(issuer: str) -> Dict[str, Any]:
    url = f"{issuer.rstrip('/')}/.well-known/openid-configuration"
    resp = requests.get(url, timeout=(2, 10))
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("OIDC discovery document is not a JSON object.")
    return data


def _is_bearer_auth(header_value: str) -> bool:
    return header_value.strip().lower().startswith("bearer ")


def _extract_bearer_token(header_value: str) -> str:
    token = header_value.strip()[len("bearer ") :].strip()
    if not token:
        raise AuthError(status_code=401, detail="Missing bearer token.", www_authenticate="Bearer")
    return token


def _claim_text(claims: Dict[str, Any], *names: str) -> str:
    for name in names:
        value = claims.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "-"


def _claim_text_list(claims: Dict[str, Any], name: str) -> list[str]:
    value = claims.get(name)
    if isinstance(value, str):
        return [item for item in value.split(" ") if item]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _expected_tenant_id_from_issuer(issuer: str) -> str | None:
    parsed = urlparse(str(issuer or "").strip())
    host = (parsed.hostname or "").strip().lower()
    if host not in {"login.microsoftonline.com", "login.windows.net", "sts.windows.net"}:
        return None
    segments = [segment.strip() for segment in parsed.path.split("/") if segment.strip()]
    if not segments:
        return None
    tenant_id = segments[0]
    if tenant_id.lower() in {"common", "organizations", "consumers"}:
        return None
    return tenant_id


def _sanitize_reason_for_logs(value: str | None) -> str:
    reason = " ".join(str(value or "").split())
    if not reason:
        return "-"
    return reason[:200]


def _client_ip_allowed(client_ip: str, allowed_cidrs: list[str]) -> bool:
    try:
        resolved_ip = ipaddress.ip_address(str(client_ip or "").strip())
    except ValueError:
        return False
    return any(resolved_ip in ipaddress.ip_network(cidr, strict=False) for cidr in allowed_cidrs)


def summarize_auth_claims_for_logs(claims: Dict[str, Any] | None) -> Dict[str, Any]:
    normalized = claims if isinstance(claims, dict) else {}
    roles = sorted(set(_claim_text_list(normalized, "roles")))
    scopes = _claim_text_list(normalized, "scp")
    return {
        "iss": normalized.get("iss"),
        "aud": normalized.get("aud"),
        "azp": normalized.get("azp") or normalized.get("appid"),
        "tid": normalized.get("tid"),
        "oid": normalized.get("oid"),
        "sub": normalized.get("sub"),
        "scp": " ".join(scopes) if scopes else None,
        "roles": roles,
        "exp": normalized.get("exp"),
        "nbf": normalized.get("nbf"),
    }


def _summarize_unverified_token_for_logs(token: str) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "header": None,
        "claims": "<not-decoded-before-verification>",
        "length": len(token or ""),
        "sha256_12": hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:12],
    }

    try:
        header = jwt.get_unverified_header(token)
    except Exception as exc:
        summary["header_error"] = f"{type(exc).__name__}: {exc}"
    else:
        summary["header"] = {
            "alg": header.get("alg"),
            "kid": header.get("kid"),
            "typ": header.get("typ"),
        }

    return summary


class AuthManager:
    def __init__(self, settings: ServiceSettings):
        self._settings = settings
        self._session_cookies = SessionCookieManager(
            enabled=settings.cookie_auth_sessions_enabled,
            secret_keys=settings.auth_session_secret_keys,
            idle_ttl_seconds=settings.auth_session_idle_ttl_seconds,
            absolute_ttl_seconds=settings.auth_session_absolute_ttl_seconds,
            session_version=settings.auth_session_version,
            cookie_name=settings.auth_session_cookie_name,
            csrf_cookie_name=settings.auth_session_csrf_cookie_name,
            secure=settings.auth_session_cookie_secure,
        )
        self._password_attempt_limiter = PasswordAttemptRateLimiter(
            window_seconds=settings.password_auth.rate_limit_window_seconds,
            max_attempts_per_ip=settings.password_auth.rate_limit_max_attempts_per_ip,
            max_attempts_global=settings.password_auth.rate_limit_max_attempts_global,
        )
        self._jwks_client_lock = threading.RLock()
        self._jwks_url: Optional[str] = None
        self._jwks_cache: Optional[Dict[str, Any]] = None
        self._jwks_cached_at: float = 0.0

    def _get_jwks_url(self) -> str:
        if self._jwks_url is not None:
            return self._jwks_url

        with self._jwks_client_lock:
            if self._jwks_url is not None:
                return self._jwks_url

            issuer = self._settings.oidc_issuer or ""
            jwks_url = self._settings.oidc_jwks_url
            if not jwks_url:
                discovery = _fetch_openid_configuration(issuer)
                jwks_url = str(discovery.get("jwks_uri") or "").strip() or None
            if not jwks_url:
                raise ValueError("OIDC jwks_url could not be resolved (set API_OIDC_JWKS_URL).")

            self._jwks_url = jwks_url
            return self._jwks_url

    def _fetch_jwks(self) -> Dict[str, Any]:
        url = self._get_jwks_url()
        resp = requests.get(url, timeout=(2, 10))
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or not isinstance(data.get("keys"), list):
            raise ValueError("OIDC JWKS endpoint did not return a JSON object with a 'keys' array.")
        return data

    def _get_jwks(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        ttl_s = 600.0
        now = time.time()
        with self._jwks_client_lock:
            if (
                force_refresh
                or self._jwks_cache is None
                or (now - float(self._jwks_cached_at)) > ttl_s
            ):
                self._jwks_cache = self._fetch_jwks()
                self._jwks_cached_at = now
            return self._jwks_cache

    def _get_public_key_for_token(self, token: str):
        header = jwt.get_unverified_header(token)
        kid = str(header.get("kid") or "").strip()

        def _find_key(jwks: Dict[str, Any]):
            keys = jwks.get("keys") or []
            if kid:
                for item in keys:
                    if str(item.get("kid") or "").strip() == kid:
                        return item
                return None
            if len(keys) == 1:
                return keys[0]
            return None

        jwks = self._get_jwks(force_refresh=False)
        jwk = _find_key(jwks)
        if jwk is None:
            jwks = self._get_jwks(force_refresh=True)
            jwk = _find_key(jwks)
        if jwk is None:
            raise AuthError(status_code=401, detail="Signing key not found for bearer token.", www_authenticate="Bearer")

        return RSAAlgorithm.from_jwk(json.dumps(jwk))

    def _verify_bearer_token(
        self,
        token: str,
        *,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> AuthContext:
        issuer = self._settings.oidc_issuer or ""
        audience = list(self._settings.oidc_audience or [])
        if not issuer or not audience:
            raise AuthError(status_code=500, detail="OIDC is not configured.")

        context = dict(request_context or {})
        unverified_token_summary = _summarize_unverified_token_for_logs(token)
        logger.info(
            "OIDC bearer verification started: request_id=%s method=%s path=%s host=%s token=%s",
            context.get("request_id", "-"),
            context.get("method", "-"),
            context.get("path", "-"),
            context.get("host", "-"),
            unverified_token_summary,
        )

        try:
            signing_key = self._get_public_key_for_token(token)
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                issuer=issuer,
                audience=audience,
            )
        except InvalidTokenError as exc:
            logger.warning(
                "OIDC bearer verification rejected: request_id=%s method=%s path=%s reason=%s token=%s",
                context.get("request_id", "-"),
                context.get("method", "-"),
                context.get("path", "-"),
                type(exc).__name__,
                unverified_token_summary,
            )
            raise AuthError(status_code=401, detail="Invalid bearer token.", www_authenticate="Bearer") from exc
        except Exception as exc:
            logger.exception(
                "OIDC verification failed: request_id=%s method=%s path=%s token=%s",
                context.get("request_id", "-"),
                context.get("method", "-"),
                context.get("path", "-"),
                unverified_token_summary,
            )
            raise AuthError(status_code=502, detail="Failed to verify bearer token.", www_authenticate="Bearer") from exc

        self._enforce_claim_requirements(claims if isinstance(claims, dict) else {}, context)

        subject = str(claims.get("sub") or claims.get("oid") or "") or None
        logger.info(
            "OIDC bearer verification accepted: request_id=%s method=%s path=%s subject=%s claims=%s",
            context.get("request_id", "-"),
            context.get("method", "-"),
            context.get("path", "-"),
            subject or "-",
            summarize_auth_claims_for_logs(claims),
        )
        return AuthContext(mode="oidc", subject=subject, claims=dict(claims), source="bearer")

    def _enforce_claim_requirements(self, claims: Dict[str, Any], context: Dict[str, Any]) -> None:
        expected_tenant_id = _expected_tenant_id_from_issuer(self._settings.oidc_issuer or "")
        if expected_tenant_id:
            actual_tenant_id = _claim_text(claims, "tid")
            if actual_tenant_id != expected_tenant_id:
                logger.warning(
                    "OIDC tenant check failed: request_id=%s path=%s expected_tid=%s actual_tid=%s",
                    context.get("request_id", "-"),
                    context.get("path", "-"),
                    expected_tenant_id,
                    actual_tenant_id or "-",
                )
                raise AuthError(status_code=401, detail="Invalid bearer token tenant.", www_authenticate="Bearer")

        required_scopes = set(self._settings.oidc_required_scopes or [])
        if required_scopes:
            raw_scopes = str(claims.get("scp") or "").strip()
            token_scopes = {s for s in raw_scopes.split(" ") if s}
            missing = sorted(required_scopes - token_scopes)
            logger.info(
                "OIDC scope check: request_id=%s path=%s required=%s token=%s missing=%s",
                context.get("request_id", "-"),
                context.get("path", "-"),
                sorted(required_scopes),
                sorted(token_scopes),
                missing,
            )
            if missing:
                raise AuthError(status_code=403, detail=f"Missing required scopes: {', '.join(missing)}.")

        required_roles = set(self._settings.oidc_required_roles or [])
        if required_roles:
            roles_claim = claims.get("roles") or []
            if not isinstance(roles_claim, list):
                roles_claim = []
            token_roles = {str(r) for r in roles_claim if str(r).strip()}
            missing = sorted(required_roles - token_roles)
            logger.info(
                "OIDC role check: request_id=%s path=%s required=%s token=%s missing=%s",
                context.get("request_id", "-"),
                context.get("path", "-"),
                sorted(required_roles),
                sorted(token_roles),
                missing,
            )
            if missing:
                raise AuthError(status_code=403, detail=f"Missing required roles: {', '.join(missing)}.")

    def authenticate_bearer_headers(
        self,
        headers: Dict[str, str],
        *,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> AuthContext:
        normalized = {str(k).lower(): str(v) for k, v in headers.items()}
        authorization = (normalized.get("authorization") or "").strip()
        if not authorization or not _is_bearer_auth(authorization):
            raise AuthError(status_code=401, detail="Missing bearer token.", www_authenticate="Bearer")
        return self._verify_bearer_token(_extract_bearer_token(authorization), request_context=request_context)

    def issue_session_cookie(self, auth_context: AuthContext) -> SessionCookieBundle:
        return self._session_cookies.issue(
            mode=auth_context.mode,
            subject=auth_context.subject,
            claims=auth_context.claims,
        )

    def set_session_cookies(self, response: Any, bundle: SessionCookieBundle) -> None:
        self._session_cookies.set_cookies(response, bundle)

    def clear_session_cookies(self, response: Any) -> None:
        self._session_cookies.clear_cookies(response)

    def authenticate_password(
        self,
        password: str,
        *,
        client_ip: str,
        break_glass_reason: str | None = None,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> AuthContext:
        context = dict(request_context or {})
        reason_for_logs = _sanitize_reason_for_logs(break_glass_reason)
        logger.info(
            "break_glass_attempted: request_id=%s client_ip=%s path=%s reason=%s",
            context.get("request_id", "-"),
            client_ip or "unknown",
            context.get("path", "-"),
            reason_for_logs,
        )
        if not self._settings.password_auth.enabled or not self._settings.password_auth.verifier:
            logger.warning(
                "break_glass_blocked: request_id=%s client_ip=%s path=%s policy=disabled",
                context.get("request_id", "-"),
                client_ip or "unknown",
                context.get("path", "-"),
            )
            raise AuthError(status_code=503, detail="Password authentication is not configured.")
        if not str(break_glass_reason or "").strip():
            logger.warning(
                "break_glass_blocked: request_id=%s client_ip=%s path=%s policy=missing_reason",
                context.get("request_id", "-"),
                client_ip or "unknown",
                context.get("path", "-"),
            )
            raise AuthError(status_code=403, detail="Break-glass reason is required.")
        if not _client_ip_allowed(client_ip, self._settings.password_auth.allowed_cidrs):
            logger.warning(
                "break_glass_blocked: request_id=%s client_ip=%s path=%s policy=cidr_denied allowed_cidrs=%s",
                context.get("request_id", "-"),
                client_ip or "unknown",
                context.get("path", "-"),
                self._settings.password_auth.allowed_cidrs,
            )
            raise AuthError(status_code=403, detail="Break-glass access is not allowed from this network.")
        expires_at_epoch = self._settings.password_auth.expires_at_epoch or 0
        if expires_at_epoch <= int(time.time()):
            logger.warning(
                "break_glass_blocked: request_id=%s client_ip=%s path=%s policy=expired expires_at_epoch=%s",
                context.get("request_id", "-"),
                client_ip or "unknown",
                context.get("path", "-"),
                expires_at_epoch,
            )
            raise AuthError(status_code=403, detail="Break-glass access has expired.")

        try:
            self._password_attempt_limiter.check(client_ip)
        except PasswordRateLimitError as exc:
            logger.warning(
                "Password auth rate limited: request_id=%s client_ip=%s path=%s retry_after=%s",
                context.get("request_id", "-"),
                client_ip or "unknown",
                context.get("path", "-"),
                exc.retry_after_seconds,
            )
            raise AuthError(status_code=429, detail=str(exc)) from exc

        verified = verify_password_hash(self._settings.password_auth.verifier, password)
        if not verified:
            self._password_attempt_limiter.record_failure(client_ip)
            logger.warning(
                "Password auth rejected: request_id=%s client_ip=%s path=%s",
                context.get("request_id", "-"),
                client_ip or "unknown",
                context.get("path", "-"),
            )
            raise AuthError(status_code=401, detail="Invalid credentials.")

        self._password_attempt_limiter.record_success(client_ip)
        claims = {
            "sub": self._settings.password_auth.session_subject,
            "name": self._settings.password_auth.session_display_name,
            "preferred_username": self._settings.password_auth.session_username,
            "roles": list(self._settings.password_auth.session_roles),
        }
        logger.info(
            "break_glass_granted: request_id=%s client_ip=%s path=%s subject=%s roles=%s reason=%s",
            context.get("request_id", "-"),
            client_ip or "unknown",
            context.get("path", "-"),
            self._settings.password_auth.session_subject,
            list(self._settings.password_auth.session_roles),
            reason_for_logs,
        )
        return AuthContext(
            mode="password",
            subject=self._settings.password_auth.session_subject,
            claims=claims,
            source="password",
        )

    def authenticate_headers(
        self,
        headers: Dict[str, str],
        *,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> AuthContext:
        return self.authenticate_request(headers, {}, request_context=request_context)

    def authenticate_request(
        self,
        headers: Dict[str, str],
        cookies: Dict[str, str],
        *,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> AuthContext:
        normalized = {str(k).lower(): str(v) for k, v in headers.items()}
        context = dict(request_context or {})
        authorization = (normalized.get("authorization") or "").strip()
        if self._session_cookies.enabled and authorization and _is_bearer_auth(authorization):
            logger.warning(
                "POLICY_BLOCKED bearer auth rejected on non-bootstrap endpoint: request_id=%s method=%s path=%s",
                context.get("request_id", "-"),
                context.get("method", "-"),
                context.get("path", "-"),
            )
            raise AuthError(
                status_code=401,
                detail="Bearer auth is only accepted on POST /api/auth/session.",
                www_authenticate="Bearer",
            )
        if not self._session_cookies.enabled and self._settings.oidc_auth_enabled and authorization and _is_bearer_auth(authorization):
            token = _extract_bearer_token(authorization)
            ctx = self._verify_bearer_token(token, request_context=context)
            logger.info(
                "Auth success via oidc: request_id=%s path=%s subject=%s oid=%s tid=%s azp=%s roles=%s scp=%s",
                context.get("request_id", "-"),
                context.get("path", "-"),
                ctx.subject or "-",
                _claim_text(ctx.claims, "oid"),
                _claim_text(ctx.claims, "tid"),
                _claim_text(ctx.claims, "azp", "appid"),
                summarize_auth_claims_for_logs(ctx.claims).get("roles"),
                summarize_auth_claims_for_logs(ctx.claims).get("scp"),
            )
            return ctx
        if self._session_cookies.enabled:
            try:
                verified = self._session_cookies.verify(cookies)
            except SessionCookieError as exc:
                if exc.cookie_present:
                    logger.warning(
                        "Auth session cookie rejected: request_id=%s path=%s detail=%s",
                        context.get("request_id", "-"),
                        context.get("path", "-"),
                        exc.detail,
                    )
                    raise AuthError(status_code=401, detail=exc.detail, www_authenticate="Bearer") from exc
            else:
                if verified.mode == "oidc":
                    self._enforce_claim_requirements(verified.claims, context)
                ctx = AuthContext(
                    mode=verified.mode,  # type: ignore[arg-type]
                    subject=verified.subject,
                    claims=verified.claims,
                    source="session-cookie",
                    csrf_token=verified.csrf_token,
                    session_renewal=verified.renewal,
                    session_id=verified.session_id,
                )
                logger.info(
                    "Auth success via session cookie: request_id=%s path=%s subject=%s mode=%s session_id=%s oid=%s tid=%s roles=%s scp=%s renewed=%s",
                    context.get("request_id", "-"),
                    context.get("path", "-"),
                    ctx.subject or "-",
                    ctx.mode,
                    ctx.session_id or "-",
                    _claim_text(ctx.claims, "oid"),
                    _claim_text(ctx.claims, "tid"),
                    summarize_auth_claims_for_logs(ctx.claims).get("roles"),
                    summarize_auth_claims_for_logs(ctx.claims).get("scp"),
                    bool(verified.renewal),
                )
                return ctx

        if self._settings.anonymous_local_auth_enabled:
            logger.info(
                "Auth success via anonymous mode: request_id=%s path=%s",
                context.get("request_id", "-"),
                context.get("path", "-"),
            )
            return AuthContext(mode="anonymous", subject=None, claims={}, source="anonymous")

        logger.warning(
            "Auth rejected before token verification: request_id=%s method=%s path=%s auth_present=%s anonymous_enabled=%s",
            context.get("request_id", "-"),
            context.get("method", "-"),
            context.get("path", "-"),
            bool((normalized.get("authorization") or "").strip()),
            self._settings.anonymous_local_auth_enabled,
        )
        raise AuthError(status_code=401, detail="Unauthorized.", www_authenticate="Bearer")
