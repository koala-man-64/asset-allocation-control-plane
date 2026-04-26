# Broker Close-Position Workflow

This is local-only and does not require contracts repo routing.

The v1 control-plane does not expose a generic `close-position` endpoint. Closing a position is an explicit broker order workflow because position quantity, option side, open orders, preview semantics, and write recovery are broker-specific.

## Workflow

1. Read the current position and metadata.
   - Alpaca: `GET /api/providers/alpaca/positions`
   - E*TRADE: `GET /api/providers/etrade/accounts/{account_key}/portfolio`
   - Kalshi: `GET /api/providers/kalshi/positions`
   - Schwab: `GET /api/providers/schwab/accounts/{account_number}/positions`
   - Kalshi is intentionally excluded from this generic close workflow. Use `GET /api/providers/kalshi/positions` plus an explicit YES/NO order decision instead of a normalized close-position helper.

2. Check open orders for the same symbol or contract.
   - Alpaca: `GET /api/providers/alpaca/orders?status=open`
   - E*TRADE: `GET /api/providers/etrade/orders`
   - Kalshi: `GET /api/providers/kalshi/orders`
   - Schwab: `GET /api/providers/schwab/accounts/{account_number}/orders`

3. Determine the closing instruction.
   - Long equity: sell
   - Short equity: buy to cover
   - Long option: sell to close
   - Short option: buy to close
   - Prediction market: use the same Kalshi market ticker and make the reduce-versus-offset decision explicitly; prefer `reduce_only` when the intent is to flatten existing exposure instead of opening new opposite-side exposure

4. Preview when supported.
   - E*TRADE: `POST /api/providers/etrade/orders/preview`, then `POST /api/providers/etrade/orders/place`
   - Schwab: `POST /api/providers/schwab/accounts/{account_number}/orders/preview`, then `POST /api/providers/schwab/accounts/{account_number}/orders`
   - Alpaca: no preview route; submit with a unique `client_order_id`
   - Kalshi: no preview route; submit with a unique `client_order_id` and `reduce_only` when flattening an existing position

5. Reconcile.
   - Fetch order status.
   - Re-read positions.
   - If a write times out or has a network failure, do not retry blindly. Reconcile through order history first.

## Safety Rules

- Do not close against stale position data.
- Do not submit a close order while an open order could already close the same symbol or option contract.
- Do not resend the request blindly after a timeout or network failure.
- Do not assume a submitted market order is filled until the broker reports final status.
- Do not normalize broker order payloads into a shared contract in v1; provider-shaped payloads stay inside this repo.
- Treat a future cross-broker normalized close-position API as a separate design item and route it through `asset-allocation-contracts` if it introduces shared DTOs.
