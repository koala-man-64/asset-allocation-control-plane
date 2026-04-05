import itertools
import threading
from collections import Counter

import pytest

from alpha_vantage.rate_limiter import RateLimiter


def test_rate_limiter_rotates_between_callers() -> None:
    limiter = RateLimiter(rate_per_minute=60_000)
    rounds = 12
    barrier = threading.Barrier(2)
    order: list[str] = []
    order_lock = threading.Lock()

    def worker(name: str) -> None:
        for _ in range(rounds):
            barrier.wait(timeout=5.0)
            limiter.wait(caller=name, timeout_seconds=1.0)
            with order_lock:
                order.append(name)

    t1 = threading.Thread(target=worker, args=("market",), daemon=True)
    t2 = threading.Thread(target=worker, args=("finance",), daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)

    assert not t1.is_alive()
    assert not t2.is_alive()

    counts = Counter(order)
    assert counts["market"] == rounds
    assert counts["finance"] == rounds

    max_streak = max(len(list(group)) for _, group in itertools.groupby(order))
    assert max_streak <= 2


def test_rate_limiter_timeout_fails_fast() -> None:
    limiter = RateLimiter(rate_per_minute=1)
    limiter.wait(caller="holder", timeout_seconds=0.1)

    with pytest.raises(TimeoutError):
        limiter.wait(caller="contender", timeout_seconds=0.01)
