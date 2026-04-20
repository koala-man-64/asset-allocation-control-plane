class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, float] = {}

    def update_price(self, symbol: str, price: float) -> None:
        self._prices[symbol] = price

    def get_price(self, symbol: str) -> float | None:
        return self._prices.get(symbol)

    def snapshot(self) -> dict[str, float]:
        return self._prices.copy()
