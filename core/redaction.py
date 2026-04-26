from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


_REDACTION = "[REDACTED]"
_TOKEN_REDACTIONS = (
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*"),
    re.compile(r"(?i)\b([A-Z0-9_]*(?:API_KEY|SECRET_KEY|CONSUMER_SECRET|CLIENT_SECRET|PRIVATE_KEY|ACCESS_TOKEN|REFRESH_TOKEN|STORAGE_KEY))=([^;\s&]+)"),
    re.compile(r"(?i)\b(AccountKey|SharedAccessSignature|sig|code|oauth_token|oauth_verifier|access_token|refresh_token|client_secret|api_key|apikey|key|password|pwd|secret)=([^;\s&]+)"),
    re.compile(r"(?i)\b(postgresql|postgres)://[^@\s]+@[^/\s]+(?:/[^\s]*)?"),
    re.compile(r"(?i)\b(DefaultEndpointsProtocol=[^;\s]+;AccountName=[^;\s]+;AccountKey=)[^;\s]+"),
)
_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "client_secret",
    "code",
    "key",
    "oauth_token",
    "oauth_verifier",
    "password",
    "pwd",
    "refresh_token",
    "secret",
    "sig",
    "signature",
    "token",
}


def redact_sensitive_text(value: Any) -> str:
    text = str(value)
    for pattern in _TOKEN_REDACTIONS:
        text = pattern.sub(lambda match: _redact_match(match), text)
    return _redact_url_query_values(text)


def redact_sensitive_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, dict):
        return {key: redact_sensitive_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_sensitive_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_value(item) for item in value)
    return value


def summarize_query_params(query: str) -> dict[str, Any]:
    pairs = parse_qsl(str(query or ""), keep_blank_values=True)
    keys = sorted({key for key, _ in pairs})
    return {
        "count": len(pairs),
        "keys": keys,
        "hasSensitiveKeys": any(key.lower() in _SENSITIVE_QUERY_KEYS for key in keys),
    }


def _redact_match(match: re.Match[str]) -> str:
    if match.lastindex and match.lastindex >= 2:
        return f"{match.group(1)}={_REDACTION}"
    if match.lastindex and match.lastindex >= 1 and match.group(1).lower().startswith("defaultendpointsprotocol"):
        return f"{match.group(1)}{_REDACTION}"
    if match.group(0).lower().startswith("bearer "):
        return f"Bearer {_REDACTION}"
    return _REDACTION


def _redact_url_query_values(text: str) -> str:
    def replace_url(match: re.Match[str]) -> str:
        raw_url = match.group(0)
        try:
            parts = urlsplit(raw_url)
        except ValueError:
            return raw_url
        if not parts.query:
            return raw_url
        query_pairs = parse_qsl(parts.query, keep_blank_values=True)
        redacted_pairs = [
            (key, _REDACTION if key.lower() in _SENSITIVE_QUERY_KEYS else value)
            for key, value in query_pairs
        ]
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(redacted_pairs, doseq=True, quote_via=quote),
                parts.fragment,
            )
        )

    return re.sub(r"https?://[^\s\"'<>]+", replace_url, text)
