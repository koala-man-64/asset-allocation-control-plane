# E*TRADE Control Plane Runbook

This is local-only and does not require contracts repo routing.

## Scope

The control-plane E*TRADE integration lives under `/api/providers/etrade` and supports:

- sandbox and live OAuth 1.0a login
- account list, balances, portfolio, transaction history, quotes, and order list
- order preview, place, and cancel
- equities and single-leg options only

Deferred for v2:

- multi-leg option orders and spreads
- change-order APIs
- persistent encrypted broker session storage
- background order polling

## Required Configuration

Set the following environment variables before using the routes:

- `ETRADE_ENABLED=true`
- `ETRADE_TRADING_ENABLED=true` only when preview, place, and cancel should be allowed
- `ETRADE_SANDBOX_CONSUMER_KEY`
- `ETRADE_SANDBOX_CONSUMER_SECRET`
- `ETRADE_LIVE_CONSUMER_KEY`
- `ETRADE_LIVE_CONSUMER_SECRET`
- `ETRADE_CALLBACK_URL` if you want browser callback completion instead of manual verifier entry

Optional safety controls:

- `ETRADE_TRADING_REQUIRED_ROLES`
- `ETRADE_REQUIRED_ROLES`
- `ETRADE_SESSION_EXPIRY_GUARD_SECONDS`
- `ETRADE_IDLE_RENEW_SECONDS`
- `ETRADE_PREVIEW_TTL_SECONDS`

## Callback Registration

E*TRADE does not self-serve callback registration. Per the official developer guide, you must log in to your E*TRADE account and send a secure message to Customer Service with:

- your consumer key
- the callback URL you want associated with that key

Official references:

- [Developer Guides](https://developer.etrade.com/getting-started/developer-guides)
- [Get Request Token](https://apisb.etrade.com/docs/api/authorization/request_token.html)
- [Authorize Application](https://apisb.etrade.com/docs/api/authorization/authorize.html)

Per the E*TRADE request-token doc, the control-plane always requests the token with `oauth_callback=oob`. Callback registration is handled on the E*TRADE side against the consumer key, not by sending the callback URL in the request-token call.

Use separate sandbox and live keys. The same callback path can be used for both because the control-plane matches callbacks by the pending request token.

## Auth Flow

Browser callback flow:

1. `POST /api/providers/etrade/connect/start` with `{"environment":"sandbox"}` or `{"environment":"live"}`
2. Open the returned `authorize_url`
3. Approve the app on E*TRADE
4. E*TRADE redirects to `GET /api/providers/etrade/connect/callback?oauth_token=...&oauth_verifier=...`
5. Verify with `GET /api/providers/etrade/session`

Manual verifier fallback:

1. `POST /api/providers/etrade/connect/start`
2. Open the returned `authorize_url`
3. Copy the verifier shown by E*TRADE
4. `POST /api/providers/etrade/connect/complete` with `{"environment":"sandbox","verifier":"..."}` or live

## Session Semantics

The gateway keeps broker tokens in memory only.

- Request tokens expire after 5 minutes.
- Access tokens go inactive after 2 hours of no API activity.
- Access tokens expire at midnight US Eastern and must be reacquired after that.
- Read calls renew idle sessions automatically when safe to do so.
- Preview, place, and cancel are blocked when the session is idle or too close to the Eastern-midnight expiry guard window.

Official references:

- [Get Request Token](https://apisb.etrade.com/docs/api/authorization/request_token.html)
- [Get Access Token](https://apisb.etrade.com/docs/api/authorization/get_access_token.html)
- [Renew Access Token](https://apisb.etrade.com/docs/api/authorization/renew_access_token.html)
- [Revoke Access Token](https://apisb.etrade.com/docs/api/authorization/revoke_access_token.html)

## Accounts And Portfolio

The account list and portfolio routes can return HTTP `204 No Content` when E*TRADE returns no data:

- `GET /api/providers/etrade/accounts`
- `GET /api/providers/etrade/accounts/{account_key}/portfolio`

The balance route defaults `real_time_nav=false` unless you explicitly opt in with the query parameter.

Official references:

- [Accounts API](https://apisb.etrade.com/docs/api/account/api-account-v1.html)
- [Balance API](https://apisb.etrade.com/docs/api/account/api-balance-v1.html)

## Transaction History

Use the transaction routes for account activity history:

- `GET /api/providers/etrade/accounts/{account_key}/transactions`
- `GET /api/providers/etrade/accounts/{account_key}/transactions/{transaction_id}`

Supported query parameters on the list route:

- `environment`
- `startDate` and `endDate` together, formatted as `YYYY-MM-DD` for the control-plane route
- `sortOrder`
- `marker`
- `count`
- `transactionGroup`

Use `transactionGroup=TRADES` when you specifically want trade history. Supported local group filters are:

- `TRADES`
- `WITHDRAWALS`
- `CASH`

The control-plane normalizes those filters to the broker paths and returns the raw broker JSON payload.

If E*TRADE returns no matching transactions, the control-plane responds with HTTP `204 No Content` instead of an empty JSON body.

Official reference:

- [Transaction API](https://apisb.etrade.com/docs/api/account/api-transaction-v1.html)

## Order Workflow

Use the control-plane order flow exactly in this sequence:

1. `POST /api/providers/etrade/orders/preview`
2. Read the returned `preview_id`
3. `POST /api/providers/etrade/orders/place` with that `preview_id`

The gateway caches preview payloads for 3 minutes and will only place from the cached preview payload. That guarantees the place request matches the previewed request.

Official reference:

- [Order API](https://apisb.etrade.com/docs/api/order/api-order-v1.html)

## Ambiguous Write Recovery

The client never auto-retries place or cancel. If the connection drops or times out after a write attempt, the response is treated as `unknown_submission_state`.

Recovery procedure:

1. Do not resend the original write blindly.
2. Call `GET /api/providers/etrade/orders` for the same account and expected date range.
3. Reconcile whether the order was placed or canceled.
4. Only submit a new request after reconciliation confirms the original action did not take effect.

Sandbox warning:

- sandbox responses are stored examples and can differ from the exact request payload
- use sandbox to validate wire shape and auth flow only, not live execution semantics

## Role Model

The E*TRADE routes sit behind the existing control-plane auth boundary.

- read routes require normal operator auth and any optional `ETRADE_REQUIRED_ROLES`
- preview, place, and cancel additionally require `ETRADE_TRADING_ENABLED=true`
- preview, place, and cancel additionally require `ETRADE_TRADING_REQUIRED_ROLES`

The callback route is intentionally exempt from the normal bearer-token requirement because E*TRADE redirects a plain browser request there. It is constrained by the pending request-token match instead.
