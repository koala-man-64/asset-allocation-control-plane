# Kalshi Control Plane Runbook

This is local-only and does not require contracts repo routing.

## Scope

The control-plane Kalshi integration lives under `/api/providers/kalshi` and supports:

- market list, market detail, and market orderbook reads
- balance, positions, order list, order detail, queue positions, and account API limits
- order create, amend, and cancel

Deferred for v2:

- trade-desk adoption through shared contracts
- a normalized cross-broker prediction-market DTO
- WebSocket bridging for user orders or market streams
- RFQ, FIX, and multivariate combo workflows

## Required Configuration

Set the following environment variables before using the routes:

- `KALSHI_ENABLED=true`
- `KALSHI_DEMO_API_KEY_ID` and `KALSHI_DEMO_PRIVATE_KEY_PEM` for demo authenticated routes
- `KALSHI_LIVE_API_KEY_ID` and `KALSHI_LIVE_PRIVATE_KEY_PEM` for live authenticated routes

Optional controls:

- `KALSHI_TIMEOUT_SECONDS`
- `KALSHI_READ_RETRY_ATTEMPTS`
- `KALSHI_READ_RETRY_BASE_DELAY_SECONDS`
- `KALSHI_REQUIRED_ROLES`
- `KALSHI_TRADING_REQUIRED_ROLES`
- `KALSHI_TRADING_ENABLED=true` only when create, amend, and cancel should be allowed
- `KALSHI_DEMO_BASE_URL` and `KALSHI_LIVE_BASE_URL` only for explicit endpoint overrides

Store the private keys as PEM text. Escaped `\n` sequences are accepted and normalized back to real newlines before the key is loaded.

## Auth Model

Kalshi does not use OAuth for these routes. The API signs each authenticated request with RSA-PSS using:

1. the current millisecond timestamp
2. the uppercased HTTP method
3. the `/trade-api/v2/...` path without query parameters

The signed headers are:

- `KALSHI-ACCESS-KEY`
- `KALSHI-ACCESS-SIGNATURE`
- `KALSHI-ACCESS-TIMESTAMP`

Public market data routes still go through normal control-plane auth and the optional `KALSHI_REQUIRED_ROLES`, but they do not require exchange credentials. Authenticated portfolio and order routes require the relevant demo or live keypair for the requested `environment`.

## Market And Portfolio Routes

Read routes:

- `GET /api/providers/kalshi/markets`
- `GET /api/providers/kalshi/markets/{ticker}`
- `GET /api/providers/kalshi/markets/{ticker}/orderbook`
- `GET /api/providers/kalshi/balance`
- `GET /api/providers/kalshi/positions`
- `GET /api/providers/kalshi/orders`
- `GET /api/providers/kalshi/orders/{order_id}`
- `GET /api/providers/kalshi/orders/{order_id}/queue-position`
- `GET /api/providers/kalshi/orders/queue-positions`
- `GET /api/providers/kalshi/account/limits`

Write routes:

- `POST /api/providers/kalshi/orders`
- `POST /api/providers/kalshi/orders/{order_id}/amend`
- `DELETE /api/providers/kalshi/orders/{order_id}`

All routes require `environment=demo|live`. The control-plane keeps Kalshi’s provider-shaped fields intact instead of mapping them into the shared trade-desk contract surface.

## Fixed-Point Handling

Kalshi uses fixed-point dollar strings (`*_dollars`) and fixed-point contract strings (`*_fp`). The control-plane:

- validates price fields at up to 4 decimal places
- validates contract count fields at up to 2 decimal places
- keeps prediction-market order intent explicit with `side=yes|no` and `action=buy|sell`
- preserves Kalshi-native flags such as `post_only`, `reduce_only`, `buy_max_cost`, `self_trade_prevention_type`, `cancel_order_on_pause`, and `order_group_id`

Use `reduce_only` when the intention is to flatten or shrink existing exposure instead of opening fresh exposure on the same market.

## Role Model

- all routes require normal operator auth and any optional `KALSHI_REQUIRED_ROLES`
- create, amend, and cancel additionally require `KALSHI_TRADING_ENABLED=true`
- create, amend, and cancel additionally require `KALSHI_TRADING_REQUIRED_ROLES`

Keep `KALSHI_TRADING_ENABLED=false` until there is a clear owner for live prediction-market order flow, write reconciliation, and rollback.

## Ambiguous Write Recovery

The gateway does not retry Kalshi write calls after timeout or network failure. If a write outcome is unknown:

1. Do not resend the request blindly.
2. Call `GET /api/providers/kalshi/orders` for the relevant market, time window, and subaccount.
3. Call `GET /api/providers/kalshi/orders/{order_id}` or `GET /api/providers/kalshi/orders/queue-positions` when the original order id is known.
4. Re-read `GET /api/providers/kalshi/positions` and `GET /api/providers/kalshi/balance`.
5. Submit a new request only after reconciliation confirms the original action did not take effect.
