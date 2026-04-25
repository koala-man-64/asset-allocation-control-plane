# Ops Scripts

## Kalshi NYC Weather Smoke Script

`kalshi_nyc_weather_smoke.py` is an operator-facing Kalshi smoke. It mirrors the env-file loading pattern used by the Schwab and broker balance tooling, then:

1. resolves today's NYC daily high-temperature market, unless you pass an explicit market ticker
2. fetches the orderbook
3. places one post-only YES limit order
4. cancels the order

By default it loads, in order, any existing copies of:

- repo `.env`
- repo `.env.web`
- `~/Projects/asset-allocation-control-plane/.env`
- `~/Projects/asset-allocation-control-plane/.env.web`

Override or extend that list with repeated `--env-file` arguments.

### Supported Credentials

Preferred environment variables:

```powershell
$env:KALSHI_DEMO_API_KEY_ID="your-demo-api-key-id"
$env:KALSHI_DEMO_PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----`n...`n-----END PRIVATE KEY-----"
```

or for live:

```powershell
$env:KALSHI_LIVE_API_KEY_ID="your-live-api-key-id"
$env:KALSHI_LIVE_PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----`n...`n-----END PRIVATE KEY-----"
```

Legacy fallback is still accepted:

```powershell
$env:KALSHI_API_KEY_ID="your-api-key-id"
$env:KALSHI_PRIVATE_KEY_PEM="-----BEGIN PRIVATE KEY-----`n...`n-----END PRIVATE KEY-----"
```

or:

```powershell
$env:KALSHI_API_KEY_ID="your-api-key-id"
$env:KALSHI_PRIVATE_KEY_PATH="C:\path\to\kalshi-private-key.pem"
```

Optional:

```powershell
$env:KALSHI_ENV="demo"
```

`KALSHI_ENV` defaults to `demo`. Supported values are `demo`, `prod`, `production`, and `live`.

### Run

Auto-select today's NYC weather market:

```powershell
python .\scripts\ops\kalshi_nyc_weather_smoke.py
```

Explicit market ticker:

```powershell
python .\scripts\ops\kalshi_nyc_weather_smoke.py --environment demo --market-ticker KXHIGHNY-26APR25-B80
```

Custom price and env file:

```powershell
python .\scripts\ops\kalshi_nyc_weather_smoke.py --env-file .\.env --yes-price-cents 1 --count 1
```

Expected output includes the loaded env files, selected market ticker, orderbook level counts, created order ID, and cancelled order ID.

### Failure Behavior

The script exits nonzero if credentials are missing, no active market is available for the requested series or explicit ticker, order placement fails, or cancellation fails.
