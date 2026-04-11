# PlanSignal Implementation Status

This workspace now contains the first six execution stages from the England-first PlanSignal blueprint.

## Stage 1. Define ICP

Primary ICP is encoded in [`plansignal/app/config.py`](/C:/Users/paron/Desktop/Dev/CS_PROJECT/plansignal/app/config.py).

Key choice:

- start with small and mid-sized planning consultancies and land buyers in England
- keep one ICP only for now
- encode pain and job-to-be-done in code so API metadata and dashboard copy can reuse it

## Stage 2. Define Schema

Core entities live in [`plansignal/app/schemas.py`](/C:/Users/paron/Desktop/Dev/CS_PROJECT/plansignal/app/schemas.py).

Chosen canonical entities:

- `NormalizedApplication`
- `Site`
- `Actor`
- `PlanningEvent`
- supporting objects: `Authority`, `PlanningDocument`, `ApplicationScores`, `AreaActivity`

Why this shape:

- `application` is commercial center of gravity
- `site` supports later UPRN and radius workflows
- `actor` supports applicant and agent normalization
- `planning_event` supports status-change monitoring and history
- scores reserve room for later AI/relevance layer without changing response shape

## Stage 3. Ingestion

Ingestion client lives in [`plansignal/app/services/ingestion.py`](/C:/Users/paron/Desktop/Dev/CS_PROJECT/plansignal/app/services/ingestion.py).

Key choices:

- use `urllib` first, not a larger HTTP client
- shape query around Planning Data dataset access
- fall back to a local sample fixture by default
- keep fetch and fixture loading behind one service boundary

Why sample fallback:

- this workspace may run without network
- blueprint work can still move forward
- same normalizer can later run on live payloads

## Stage 4. First Normalizer

Normalizer lives in [`plansignal/app/services/normalizer.py`](/C:/Users/paron/Desktop/Dev/CS_PROJECT/plansignal/app/services/normalizer.py).

What it does:

- maps source statuses into canonical statuses
- maps proposal text into normalized categories
- normalizes applicant and agent names
- generates change history from received, validated, status, and decision dates
- attaches tags
- computes initial scores
- emits one stable `NormalizedApplication` object

## Stage 5. Watchlists and Alerts

Watchlist and usage storage lives in [`plansignal/app/services/storage.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/storage.py).

Added:

- `POST /watchlists`
- `GET /watchlists`
- `GET /watchlists/{id}`
- `GET /watchlists/{id}/changes`
- `POST /alerts/test`
- `GET /usage`

Why this matters:

- turns API into repeat workflow, not one-off search
- enables pilot delivery modes
- creates daily-use retention hooks

## Stage 6. Premium Intelligence Endpoints

Query service lives in [`plansignal/app/services/query.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/query.py).

Added:

- `GET /applications/{id}/documents`
- `GET /actors/{id}/applications`
- `GET /signals/high-priority`
- `POST /screen/sites`
- `GET /benchmark/boroughs`
- `POST /natural-language-query`

Why this matters:

- premium surface now reflects commercial intelligence workflows
- buyer can move from raw application list to signal-driven triage
- architecture now supports pilot demos for API and dashboard stories

## Stage 7. Phase 2 Foundation

Phase 2 infrastructure lives in:

- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/services/auth.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/auth.py)
- [`plansignal/templates/dashboard.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/dashboard.html)

Added:

- SQLite persistence for organizations, API keys, watchlists, and usage events
- bootstrap demo organization and API key
- `X-API-Key` auth dependency on business endpoints
- lightweight dashboard at `/`

Why this matters:

- pilot users now have state across restarts
- API can model account-level usage
- dashboard creates lower-friction commercial demo path

## Stage 8. Phase 3 Identity Layer

Identity and key management now live in:

- [`plansignal/app/services/auth.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/auth.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)

Added:

- `POST /auth/register`
- `POST /auth/login`
- `GET /me`
- `GET /api-keys`
- `POST /api-keys`
- bearer session auth alongside `X-API-Key`
- in-place SQLite migration for older DB files

Why this matters:

- buyers can sign in like a normal SaaS product
- dashboards no longer depend on pasted hardcoded demo keys
- orgs can mint API keys for internal tools and automations

## Sample Data

Fixture file:

- [`plansignal/data/samples/planning_data_applications.json`](/C:/Users/paron/Desktop/Dev/CS_PROJECT/plansignal/data/samples/planning_data_applications.json)

Coverage:

- Camden cluster
- Southwark cluster
- Hackney cluster

Why these samples:

- one major mixed-use scheme
- one approved corridor reuse scheme
- one active workspace intensification scheme

This gives enough variation to test status mapping, category mapping, event generation, and area summaries.

## API Surface

FastAPI app lives in [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/CS_PROJECT/plansignal/app/main.py).

Current endpoints:

- `GET /blueprint/icp`
- `GET /blueprint/stages`
- `GET /`
- `POST /auth/register`
- `POST /auth/login`
- `GET /me`
- `GET /api-keys`
- `POST /api-keys`
- `GET /applications/raw`
- `GET /applications`
- `GET /applications/{application_id}`
- `GET /applications/{application_id}/history`
- `GET /applications/{application_id}/documents`
- `GET /areas/{area_id}/activity`
- `GET /actors/{actor_id}/applications`
- `POST /watchlists`
- `GET /watchlists`
- `GET /watchlists/{id}`
- `GET /watchlists/{id}/changes`
- `POST /alerts/test`
- `GET /usage`
- `GET /signals/high-priority`
- `POST /screen/sites`
- `GET /benchmark/boroughs`
- `POST /natural-language-query`

## What is not built yet

- auth
- Stripe billing
- real alert delivery
- real document summarization jobs
- relational database / PostGIS
- webhook auth and richer org-level permissions
- historical backfills and scheduled ingestion jobs

Those belong to later roadmap phases, not this implementation pass.
