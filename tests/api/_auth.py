from __future__ import annotations

from typing import Any

import pytest

from api.service.auth import AuthContext, AuthError


def install_auth_stub(
    monkeypatch: pytest.MonkeyPatch,
    auth: Any,
    *,
    auth_context: AuthContext | None = None,
    auth_error: AuthError | None = None,
) -> None:
    if (auth_context is None) == (auth_error is None):
        raise ValueError("Provide exactly one of auth_context or auth_error.")

    def _authenticate_request(
        _headers: dict[str, str],
        _cookies: dict[str, str] | None = None,
        *,
        request_context: dict[str, str] | None = None,
    ) -> AuthContext:
        del _headers, _cookies, request_context
        if auth_error is not None:
            raise auth_error
        assert auth_context is not None
        return auth_context

    def _authenticate_headers(
        headers: dict[str, str],
        *,
        request_context: dict[str, str] | None = None,
    ) -> AuthContext:
        return _authenticate_request(headers, {}, request_context=request_context)

    monkeypatch.setattr(auth, "authenticate_request", _authenticate_request)
    monkeypatch.setattr(auth, "authenticate_headers", _authenticate_headers)
