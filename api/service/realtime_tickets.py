from __future__ import annotations

import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from api.service.settings import AuthMode


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class WebSocketTicket:
    ticket: str
    subject: str | None
    auth_mode: AuthMode
    issued_at: datetime
    expires_at: datetime


class WebSocketTicketStore:
    def __init__(
        self,
        *,
        ttl_seconds: int = 60,
        now_fn: Callable[[], datetime] = utc_now,
    ) -> None:
        self._ttl_seconds = int(ttl_seconds)
        self._now_fn = now_fn
        self._tickets: dict[str, WebSocketTicket] = {}
        self._lock = threading.RLock()

    def issue(self, *, subject: str | None, auth_mode: AuthMode) -> WebSocketTicket:
        now = self._now_fn()
        ticket = WebSocketTicket(
            ticket=secrets.token_urlsafe(32),
            subject=subject,
            auth_mode=auth_mode,
            issued_at=now,
            expires_at=now + timedelta(seconds=max(0, self._ttl_seconds)),
        )
        with self._lock:
            self._purge_expired_locked(now=now)
            self._tickets[ticket.ticket] = ticket
        return ticket

    def consume(
        self,
        ticket: str,
    ) -> WebSocketTicket | None:
        resolved = str(ticket or "").strip()
        if not resolved:
            return None

        now = self._now_fn()
        with self._lock:
            self._purge_expired_locked(now=now)
            record = self._tickets.pop(resolved, None)
            if record is None:
                return None
            if record.expires_at <= now:
                return None
            return record

    def purge_expired(self) -> int:
        with self._lock:
            return self._purge_expired_locked(now=self._now_fn())

    def _purge_expired_locked(self, *, now: datetime) -> int:
        expired = [
            key
            for key, record in self._tickets.items()
            if record.expires_at <= now
        ]
        for key in expired:
            self._tickets.pop(key, None)
        return len(expired)
