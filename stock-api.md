# Local Stock API

Self-hosted stock information API backed by SQLite.

## Run

```powershell
python api.py
```

Default bind:

- `http://127.0.0.1:8000`
- API key header: `X-API-Key`
- Default token: `change-me-local-token`

Override with env vars:

```powershell
$env:STOCK_API_TOKEN="replace-this"
$env:STOCK_API_HOST="0.0.0.0"
$env:STOCK_API_PORT="8080"
$env:ALPHA_VANTAGE_API_KEY="FPP4ZN1O556ERK3K"
python api.py
```

## Public Endpoints

- `GET /`
- `GET /health`
- `GET /v1/symbols`
- `GET /v1/search?q=app`
- `GET /v1/quotes/AAPL`
- `GET /v1/history/AAPL?limit=5`
- `GET /v1/export/AAPL`

## Admin Endpoints

Need header:

```text
X-API-Key: your-token
```

### Create or update symbol

```powershell
curl -X POST http://127.0.0.1:8000/v1/admin/symbols `
  -H "Content-Type: application/json" `
  -H "X-API-Key: change-me-local-token" `
  -d "{\"symbol\":\"TSLA\",\"name\":\"Tesla, Inc.\",\"exchange\":\"NASDAQ\",\"currency\":\"USD\",\"sector\":\"Consumer Cyclical\",\"industry\":\"Auto Manufacturers\",\"country\":\"US\"}"
```

### Create or update quote

```powershell
curl -X POST http://127.0.0.1:8000/v1/admin/quotes `
  -H "Content-Type: application/json" `
  -H "X-API-Key: change-me-local-token" `
  -d "{\"symbol\":\"TSLA\",\"price\":177.25,\"change_amount\":2.1,\"change_percent\":1.2,\"open_price\":174.5,\"high_price\":178.3,\"low_price\":173.8,\"previous_close\":175.15,\"volume\":99887766,\"market_cap\":600000000000}"
```

### Import OHLCV CSV

CSV columns:

```text
trade_date,open_price,high_price,low_price,close_price,adjusted_close,volume
```

```powershell
curl -X POST http://127.0.0.1:8000/v1/admin/import/csv `
  -H "X-API-Key: change-me-local-token" `
  -F "symbol=TSLA" `
  -F "file=@tsla_history.csv"
```

### Live sync from Alpha Vantage

```powershell
curl -X POST http://127.0.0.1:8000/v1/admin/sync/alpha-vantage/AAPL `
  -H "X-API-Key: change-me-local-token"
```

## Storage

SQLite file:

`data/root/stock_api/stock_api.db`

App auto-creates schema and demo records for `AAPL`, `MSFT`, `NVDA` on first boot.
