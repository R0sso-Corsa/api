# Project Worklog

## Overview

This file summarizes the work completed for the self-hosted stock API project in:

`C:\Users\paron\Desktop\Dev\unrelated projects\api`

The goal was to turn a one-off script into a usable local stock information service with a browser dashboard, local storage, import/sync features, and documentation.

## Major Deliverables

### 1. Built a self-hosted stock API

Created a Flask-based local API in `api.py` with SQLite persistence.

Implemented:

- `GET /`
- `GET /health`
- `GET /v1/symbols`
- `GET /v1/search?q=...`
- `GET /v1/quotes/<symbol>`
- `GET /v1/history/<symbol>`
- `GET /v1/chart/<symbol>.svg`
- `GET /v1/export/<symbol>`

Admin endpoints:

- `POST /v1/admin/symbols`
- `POST /v1/admin/quotes`
- `POST /v1/admin/bars`
- `POST /v1/admin/import/csv`
- `POST /v1/admin/sync/alpha-vantage/<symbol>`

Compatibility alias kept:

- `POST /v1/admin/sync/stooq/<symbol>`

## 2. Added local SQLite storage

Created and used:

- `data/root/stock_api/stock_api.db`

Schema includes:

- `symbols`
- `quotes`
- `daily_bars`

Bootstrapped demo records for:

- `AAPL`
- `MSFT`
- `NVDA`

## 3. Added dashboard UI

Reworked `new.html` into a browser dashboard that can:

- load ticker quotes
- load price history
- render a chart from the API
- trigger live sync
- display backend error messages cleanly

Dashboard route:

- `GET /dashboard`

## 4. Added chart endpoint

Implemented server-side SVG chart generation in:

- `GET /v1/chart/<symbol>.svg?days=30`

The chart is used by the dashboard and can also be opened directly in a browser.

## 5. Added stock client utility

Created `stock_client.py` as a small CLI helper for API access.

It supports:

- `health`
- `symbols`
- `search`
- `quote`
- `history`

## 6. Added project docs

Created and updated:

- `stock-api.md`
- `tickers.md`
- `PROJECT_WORKLOG.md`

`stock-api.md` explains how to run and use the service.

`tickers.md` lists the symbols currently in the local database.

## Provider / Live Sync History

### Initial approach: Stooq

Live Sync was first implemented against Stooq.

Problems encountered:

- PowerShell `&` broke unquoted URLs
- Stooq API key page opened blank in browser
- Stooq CSV responses changed shape
- Stooq sometimes returned an API key challenge instead of CSV
- parsing had to be hardened multiple times

Fixes made during that phase:

- safe normalization of provider rows
- support for list-valued CSV fields from `csv.DictReader`
- support for mixed-case headers
- support for comma, semicolon, tab, pipe, and space-delimited rows
- support for missing/unrecognized headers
- JSON-only error responses instead of raw HTML `500` pages

Even after parser hardening, Stooq was not reliable for this setup because the provider flow required browser/API-key behavior that was not practical in the app.

### Final approach: Alpha Vantage

Live Sync was switched from Stooq to Alpha Vantage.

Configured key:

- `ALPHA_VANTAGE_API_KEY`

Current default fallback in code:

- `FPP4ZN1O556ERK3K`

Alpha Vantage implementation:

- uses `TIME_SERIES_DAILY`
- parses JSON daily time series
- writes bars into `daily_bars`
- updates current quote in `quotes`

One Alpha Vantage issue occurred:

- `outputsize=full` required a premium plan

Fix:

- changed to `outputsize=compact`

This keeps Live Sync compatible with the free tier.

## Bug Fixes Completed

### Dashboard / API error handling

Fixed cases where the frontend showed:

- JSON parse errors
- blank responses
- generic HTML `500 Internal Server Error` pages

Improvements:

- frontend now reads raw response text first
- backend returns JSON errors for Live Sync failures
- caching disabled to reduce stale frontend behavior

### CSV normalization crash

Fixed:

- `'list' object has no attribute 'strip'`

Cause:

- provider rows from `csv.DictReader` could include list values for overflow columns

Fix:

- added safe normalization for strings, lists, `None`, and other scalar values

### Provider format handling

Improved tolerance for:

- lowercase headers
- alternate headers
- malformed rows
- missing headers
- extra preamble lines

### Missing import

Fixed a runtime bug in the Alpha Vantage path caused by a missing `json` import.

## Files Created or Updated

Primary files:

- `api.py`
- `new.html`
- `stock_client.py`
- `stock-api.md`
- `tickers.md`
- `PROJECT_WORKLOG.md`

## Current Usage

Start server:

```powershell
cd "C:\Users\paron\Desktop\Dev\unrelated projects\api"
python api.py
```

Optional environment variables:

```powershell
$env:STOCK_API_TOKEN="replace-this"
$env:STOCK_API_HOST="127.0.0.1"
$env:STOCK_API_PORT="8000"
$env:ALPHA_VANTAGE_API_KEY="FPP4ZN1O556ERK3K"
python api.py
```

Open dashboard:

- `http://127.0.0.1:8000/dashboard`

Example live sync:

```powershell
curl -X POST http://127.0.0.1:8000/v1/admin/sync/alpha-vantage/AAPL `
  -H "X-API-Key: change-me-local-token"
```

## Current State

The project now has:

- a local Flask API
- SQLite-backed storage
- import and admin write endpoints
- a browser dashboard
- SVG chart generation
- a CLI client
- Alpha Vantage-powered Live Sync
- markdown documentation and ticker inventory

## Notes

- Live Sync now depends on Alpha Vantage rather than Stooq
- Alpha Vantage free tier only supports compact daily history
- the database currently contains demo symbols unless more are added through admin endpoints or imports
