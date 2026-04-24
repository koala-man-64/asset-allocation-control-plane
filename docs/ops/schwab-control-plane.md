# Schwab Control Plane Runbook

This is local-only and does not require contracts repo routing.

## Scope

The control-plane Schwab integration lives under `/api/providers/schwab` and supports:

- OAuth connect, callback, session, and disconnect
- account numbers, accounts, account balance, positions, and user preference metadata
- order list, order detail, preview, place, replace, and cancel
- account transaction history and transaction detail

Deferred for v2:

- persistent encrypted broker token storage
- background order polling
- a normalized cross-broker broker DTO
- a generic `close-position` endpoint

## Required Configuration

Set the following environment variables before using the routes:

- `SCHWAB_ENABLED=true`
- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN` for runtime token refresh
- `SCHWAB_APP_CALLBACK_URL` if `API_PUBLIC_BASE_URL` cannot derive the registered callback URL

Optional controls:

- `SCHWAB_ACCESS_TOKEN` for bootstrap-only local sessions
- `SCHWAB_TIMEOUT_SECONDS`
- `SCHWAB_REQUIRED_ROLES`
- `SCHWAB_TRADING_REQUIRED_ROLES`
- `SCHWAB_TRADING_ENABLED=true` only when preview, place, replace, and cancel should be allowed

## Auth Flow

Browser callback flow:

1. `POST /api/providers/schwab/connect/start`
2. Open the returned `authorize_url`
3. Approve the app on Schwab
4. Schwab redirects to `GET /api/providers/schwab/connect/callback?code=...&state=...`
5. Verify with `GET /api/providers/schwab/session`

Manual code fallback:

1. `POST /api/providers/schwab/connect/start`
2. Open the returned `authorize_url`
3. Copy the returned OAuth code from the callback URL
4. `POST /api/providers/schwab/connect/complete` with `{"code":"...","state":"..."}`

The callback and complete routes reject missing, expired, or mismatched OAuth state. Tokens are kept in memory; the API does not write OAuth tokens back to `.env` or `.env.web`.

## Account And Trading Routes

Read routes:

- `GET /api/providers/schwab/account-numbers`
- `GET /api/providers/schwab/accounts`
- `GET /api/providers/schwab/accounts/{account_number}`
- `GET /api/providers/schwab/accounts/{account_number}/balance`
- `GET /api/providers/schwab/accounts/{account_number}/positions`
- `GET /api/providers/schwab/accounts/{account_number}/transactions`
- `GET /api/providers/schwab/accounts/{account_number}/transactions/{transaction_id}`
- `GET /api/providers/schwab/orders`
- `GET /api/providers/schwab/accounts/{account_number}/orders`
- `GET /api/providers/schwab/accounts/{account_number}/orders/{order_id}`
- `GET /api/providers/schwab/user-preference`

Write routes:

- `POST /api/providers/schwab/accounts/{account_number}/orders/preview`
- `POST /api/providers/schwab/accounts/{account_number}/orders`
- `PUT /api/providers/schwab/accounts/{account_number}/orders/{order_id}`
- `DELETE /api/providers/schwab/accounts/{account_number}/orders/{order_id}`

The control-plane forwards provider-shaped Schwab order bodies. It does not normalize orders into a shared broker contract.

## Role Model

- read routes require normal operator auth and any optional `SCHWAB_REQUIRED_ROLES`
- write routes additionally require `SCHWAB_TRADING_ENABLED=true`
- write routes additionally require `SCHWAB_TRADING_REQUIRED_ROLES`

Production broker read roles should be explicit when the integration is enabled. Keep `SCHWAB_TRADING_ENABLED=false` until live broker trading has an operational owner, rollback procedure, and reconciliation workflow.

## Ambiguous Write Recovery

The gateway does not retry Schwab write calls after provider auth failure, timeout, or network failure. If a write outcome is unknown:

1. Do not resend the request blindly.
2. Call `GET /api/providers/schwab/accounts/{account_number}/orders` for the relevant date range.
3. Reconcile whether the order was accepted, replaced, or canceled.
4. Submit a new request only after reconciliation confirms the original action did not take effect.
