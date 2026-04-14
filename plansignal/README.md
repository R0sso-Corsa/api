# PlanSignal

England-first planning and development intelligence API scaffold.

This package now implements the core product path from the PlanSignal blueprint:

1. Define ICP: planning consultancies and land buyers in England
2. Define schema for `application`, `site`, `actor`, and `planning_event`
3. Set up source ingestion from the Planning Data API
4. Build the first normalizer
5. Add watchlists, usage tracking, and alert simulation
6. Expose premium-style screening and intelligence endpoints
7. Add persistence, API auth, and a lightweight dashboard
8. Add real user login, bearer sessions, and API-key management

## What is included

- FastAPI app with blueprint-aligned endpoints
- ICP profile and stage map
- Canonical schema models
- Planning Data ingestion client with local fixture fallback
- Normalizer that converts raw upstream records into decision-ready application objects
- Sample dataset for three England borough clusters
- SQLite persistence for organizations, API keys, watchlists, and usage events
- High-priority signals, actor lookups, site screening, borough benchmarking, and natural-language query endpoints
- Multi-page website shell served from FastAPI
- Session login and self-serve API key creation

## Run

```bash
uvicorn api:app --reload
```

## Demo auth

Use this demo header for protected endpoints:

```text
X-API-Key: plansignal-demo-key
```

Or sign in with:

```text
email: demo@plansignal.local
password: plansignal-demo-password
```

Website pages:

- `GET /`
- `GET /product`
- `GET /pricing`
- `GET /developers`
- `GET /sources`
- `GET /playground`
- `GET /contact`
- `GET /map`
- `GET /reports`
- `GET /reports/{report_id}`
- `GET /applications/view/{application_id}`
- `GET /dashboard`

## Key endpoints

- `GET /health`
- `GET /`
- `GET /product`
- `GET /pricing`
- `GET /developers`
- `GET /sources`
- `GET /playground`
- `GET /contact`
- `GET /map`
- `GET /reports`
- `GET /dashboard`
- `GET /blueprint/icp`
- `GET /blueprint/stages`
- `POST /site/waitlist`
- `GET /map/data`
- `GET /reports/summary`
- `POST /reports/save`
- `GET /reports/saved`
- `GET /reports/saved/{report_id}`
- `POST /reports/scheduled`
- `GET /reports/scheduled`
- `POST /reports/scheduled/{schedule_id}/run`
- `GET /email/outbox`
- `POST /webhooks/endpoints`
- `GET /webhooks/endpoints`
- `POST /webhooks/endpoints/{webhook_id}/test`
- `GET /webhooks/deliveries`
- `GET /ops/scheduler`
- `POST /ops/scheduler/run`
- `GET /exports/applications.csv`
- `GET /exports/signals.csv`
- `POST /auth/register`
- `POST /auth/login`
- `GET /me`
- `GET /api-keys`
- `POST /api-keys`
- `GET /applications/raw`
- `GET /applications`
- `GET /applications/{application_id}`
- `GET /applications/{application_id}/context`
- `GET /applications/{application_id}/history`
- `GET /applications/{application_id}/documents`
- `GET /areas/{area_id}/activity`
- `GET /actors/{actor_id}/applications`
- `POST /watchlists`
- `GET /watchlists`
- `GET /watchlists/{id}`
- `POST /watchlists/{id}/link-webhook`
- `GET /watchlists/{id}/changes`
- `POST /watchlists/{id}/deliver`
- `POST /alerts/test`
- `GET /usage`
- `GET /signals/high-priority`
- `POST /screen/sites`
- `GET /benchmark/boroughs`
- `POST /natural-language-query`

## Notes

- Remote ingestion is implemented with `urllib` so the service can fetch the Planning Data API later without adding a heavy client.
- Runtime now targets live official Planning Data feeds for applications and context overlays.
- Scheduled reports now run in a background scheduler loop inside the app process.
- Email delivery supports SMTP when `PLANSIGNAL_SMTP_HOST` and related SMTP env vars are configured.
- Without SMTP configured, scheduled emails remain queued in the local outbox for later delivery.
- Official context overlays now include Article 4, brownfield, green belt, and developer agreements.
- Billing and richer org management are still future phases.
