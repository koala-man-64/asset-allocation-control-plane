"""
Simple thread‑safe rate limiter.

Alpha Vantage enforces per‑minute request quotas on API keys. To
ensure that client code does not exceed those quotas, this module
implements a lightweight limiter that serializes outgoing requests
such that no more than ``rate_per_minute`` calls are made within any
sixty second window. The limiter supports fair-share caller rotation
to reduce starvation when multiple workloads contend.

Note that this limiter does not account for second‑level burst limits
(e.g. a maximum number of calls per second) – it simply spaces calls
evenly over a minute.  If your subscription tier specifies more
complex rules you may wish to implement a more sophisticated
mechanism.
"""

from collections import deque
import threading
import time
from typing import Optional


class RateLimiter:
    """A basic token bucket rate limiter.

    Parameters
    ----------
    rate_per_minute : int
        The number of requests permitted per minute.  A value of
        ``1`` means the client will wait one minute between calls,
        while ``60`` allows one call per second.  Values less than
        one are coerced to one.

    """

    def __init__(self, rate_per_minute: Optional[int] = None) -> None:
        rate = rate_per_minute or 1
        self.rate = max(1, rate)
        self.interval = 60.0 / float(self.rate)
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._last_call = 0.0
        self._pending_by_caller: dict[str, int] = {}
        self._rotation: deque[str] = deque()

    @staticmethod
    def _normalize_caller(caller: Optional[str]) -> str:
        text = str(caller or "").strip()
        if not text:
            return "default"
        if len(text) > 128:
            return text[:128]
        return text

    def _enqueue_caller(self, caller_key: str) -> None:
        self._pending_by_caller[caller_key] = self._pending_by_caller.get(caller_key, 0) + 1
        if caller_key not in self._rotation:
            self._rotation.append(caller_key)

    def _dequeue_granted(self, caller_key: str) -> None:
        if self._rotation and self._rotation[0] == caller_key:
            self._rotation.popleft()

        remaining = self._pending_by_caller.get(caller_key, 0) - 1
        if remaining > 0:
            self._pending_by_caller[caller_key] = remaining
            # Requeue caller to the end so other callers get a fair turn.
            if caller_key not in self._rotation:
                self._rotation.append(caller_key)
            return

        self._pending_by_caller.pop(caller_key, None)

    def _remove_waiting_caller(self, caller_key: str) -> None:
        current = self._pending_by_caller.get(caller_key, 0)
        if current <= 0:
            return
        remaining = current - 1
        if remaining > 0:
            self._pending_by_caller[caller_key] = remaining
            return

        self._pending_by_caller.pop(caller_key, None)
        try:
            self._rotation.remove(caller_key)
        except ValueError:
            pass

    def wait(self, *, caller: Optional[str] = None, timeout_seconds: Optional[float] = None) -> None:
        """Block until the next request is permitted.

        This method should be called immediately before making an
        outbound request. The optional caller key is used for fair
        rotation among active callers. ``timeout_seconds`` can be used
        to fail fast when contention is too high.
        """
        caller_key = self._normalize_caller(caller)
        timeout = None
        if timeout_seconds is not None:
            timeout = max(0.0, float(timeout_seconds))

        with self._cond:
            self._enqueue_caller(caller_key)
            started = time.monotonic()

            while True:
                now = time.monotonic()
                elapsed = now - self._last_call
                rate_wait = self.interval - elapsed

                is_turn = bool(self._rotation and self._rotation[0] == caller_key)
                if is_turn and rate_wait <= 0:
                    self._dequeue_granted(caller_key)
                    self._last_call = now
                    self._cond.notify_all()
                    return

                wait_timeout = None
                if is_turn and rate_wait > 0:
                    wait_timeout = rate_wait

                if timeout is not None:
                    remaining = timeout - (now - started)
                    if remaining <= 0:
                        self._remove_waiting_caller(caller_key)
                        self._cond.notify_all()
                        raise TimeoutError(
                            f"Timed out waiting for rate-limit slot (caller={caller_key}, timeout={timeout:.3f}s)."
                        )
                    if wait_timeout is None or remaining < wait_timeout:
                        wait_timeout = remaining

                self._cond.wait(timeout=wait_timeout)
