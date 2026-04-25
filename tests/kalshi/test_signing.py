from __future__ import annotations

import base64

from kalshi.signing import sign_request


class _DummyPrivateKey:
    def __init__(self) -> None:
        self.messages: list[bytes] = []

    def sign(self, message, _padding, _algorithm):  # type: ignore[no-untyped-def]
        self.messages.append(message)
        return b"signature"


def test_sign_request_strips_query_parameters() -> None:
    private_key = _DummyPrivateKey()

    signature = sign_request(private_key, "1703123456789", "GET", "/trade-api/v2/portfolio/orders?limit=5")

    assert private_key.messages == [b"1703123456789GET/trade-api/v2/portfolio/orders"]
    assert signature == base64.b64encode(b"signature").decode("utf-8")
