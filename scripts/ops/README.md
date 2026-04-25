# Ops Scripts

## Kalshi NYC Weather Smoke Script

`kalshi_nyc_weather_smoke.py` checks the Kalshi NYC daily high-temperature market, fetches its orderbook, places one 1-contract demo order, and cancels it.

It reads credentials from the process environment only. It does not load `.env` automatically.

### Required Environment

```powershell
$env:KALSHI_API_KEY_ID="your-demo-api-key-id"
$env:KALSHI_PRIVATE_KEY_PATH="C:\path\to\kalshi-demo-private-key.key"
```

Optional:

```powershell
$env:KALSHI_ENV="demo"
```

`KALSHI_ENV` defaults to `demo`. Supported values are `demo`, `prod`, `production`, and `live`.

### Run

```powershell
python .\scripts\ops\kalshi_nyc_weather_smoke.py
```

Expected output includes the selected market ticker, orderbook level counts, created order ID, and cancelled order ID.

### Failure Behavior

The script exits nonzero if credentials are missing, no active `KXHIGHNY` market is available for today, order placement fails, or cancellation fails.
