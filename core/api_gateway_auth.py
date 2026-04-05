from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from azure.identity import DefaultAzureCredential


logger = logging.getLogger(__name__)
_TOKEN_REFRESH_SKEW_SECONDS = 60.0


def build_access_token_provider(scope: str) -> Callable[[], str]:
    resolved_scope = str(scope or "").strip()
    if not resolved_scope:
        raise ValueError("API gateway bearer auth requires a non-empty scope.")

    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    token_lock = threading.Lock()
    cached_token: str | None = None
    cached_expires_on = 0.0

    def get_token() -> str:
        nonlocal cached_token, cached_expires_on
        now = time.time()
        with token_lock:
            if cached_token and (cached_expires_on - now) > _TOKEN_REFRESH_SKEW_SECONDS:
                return cached_token

            access_token = credential.get_token(resolved_scope)
            token_value = str(getattr(access_token, "token", "") or "").strip()
            if not token_value:
                raise RuntimeError(
                    f"API gateway token acquisition returned an empty token for scope {resolved_scope!r}."
                )

            cached_token = token_value
            cached_expires_on = float(getattr(access_token, "expires_on", 0.0) or 0.0)
            logger.info("Refreshed API gateway bearer token for scope=%s", resolved_scope)
            return cached_token

    return get_token
