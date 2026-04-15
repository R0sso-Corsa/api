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

## Stage 9. Signed Webhook Delivery

Webhook delivery now lives in:

- [`plansignal/app/services/webhook_delivery.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/webhook_delivery.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)

Added:

- per-endpoint signing secret storage in SQLite
- auto-generated secrets for existing and new webhook endpoints
- HMAC SHA-256 signature headers on watchlist deliveries and webhook tests
- raw alert test support for optional signing secret
- local receiver verification flow for pilot testing

Why this matters:

- receivers can verify payload origin before trusting alert content
- webhook demos now match real integration expectations better
- foundation exists for later replay protection and secret rotation

## Stage 10. Webhook Reliability And Rotation

Webhook reliability work now lives in:

- [`plansignal/app/services/scheduler.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/scheduler.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)
- [`receiver.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/receiver.py)

Added:

- durable webhook outbox payload storage in SQLite
- queued webhook retry with exponential backoff
- scheduler processing for pending webhook retries
- per-endpoint secret rotation API and reports UI button
- receiver replay-window validation using signed timestamp freshness

Why this matters:

- transient receiver/network failures no longer mean permanent delivery loss
- operators can rotate secrets without manual DB edits
- signed webhook verification now checks both origin and freshness

## Stage 11. Admin Permission Hardening

Admin permission checks now live in:

- [`plansignal/app/services/auth.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/auth.py)
- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)

Added:

- session auth now carries `user_role` through request context
- shared owner/admin guard for sensitive org mutations
- admin-only enforcement on API key minting, webhook endpoint management, webhook-linked watchlist actions, and scheduler runs

Why this matters:

- sensitive org operations are no longer unlocked by any valid session
- webhook/admin controls now align better with real multi-user SaaS expectations
- foundation exists for later staff/member role expansion

## Stage 12. Watchlist And Webhook Management CRUD

Management CRUD now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)

Added:

- update and delete endpoints for watchlists
- update and delete endpoints for webhook endpoints
- reports UI edit/delete actions with form prefill and cancel-edit flow
- safe webhook endpoint deletion that unlinks watchlists and closes queued retries

Why this matters:

- operators can manage alert configuration without direct DB intervention
- broken webhook references are cleaned up automatically on delete
- reports page now behaves more like a usable admin console than a one-way demo form

## Stage 13. Scheduled Report Management CRUD

Scheduled report management now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)

Added:

- update and delete endpoints for scheduled reports
- reports UI edit/delete actions for schedules
- admin-session guard on schedule create, update, run, and delete

Why this matters:

- operators can tune email automation without database access
- schedule management now matches the CRUD shape of watchlists and webhooks
- reports page covers the full delivery-config lifecycle instead of only creation

## Stage 14. Saved Report Management CRUD

Saved report management now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)

Added:

- update and delete endpoints for saved reports
- reports UI edit/delete actions for saved report snapshots
- owner/admin guard on saved report update and delete

Why this matters:

- operators can curate stored report snapshots instead of only accumulating them
- saved reports now match the CRUD shape of the other delivery/admin resources
- reports page covers snapshot lifecycle management, not only creation

## Stage 15. Organization Member Management

Organization member management now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/services/auth.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/auth.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)

Added:

- organization user list endpoint
- owner-only member creation, role update, and user deletion
- admin/owner access to view org users
- safeguards against self-demotion, self-deletion, and removing the last owner
- reports UI for member creation and role management

Why this matters:

- PlanSignal is no longer effectively single-user per organization
- owner/admin split now has practical meaning in the product
- groundwork exists for richer membership and invitation flows later

## Stage 16. Invitation Flows

Organization invitations now live in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/schemas.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/schemas.py)
- [`plansignal/templates/reports.html`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/templates/reports.html)

Added:

- organization invitation list endpoint for owner/admin visibility
- owner-only invitation creation and revoke actions
- one-shot invite acceptance flow that creates the user and returns a session
- pending invite expiry handling with accepted/revoked/expired states
- reports UI for issuing and revoking manual invite tokens

Why this matters:

- owners can onboard teammates without pre-assigning passwords
- membership flow is now closer to a real multi-user product
- outbound email can be layered later without reworking core invite state

## Stage 17. Enterprise Auth Hardening

Auth hardening now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/services/auth.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/auth.py)

Added:

- expiring sessions with `PLANSIGNAL_SESSION_TTL_HOURS`
- session revocation via logout
- role policy visibility endpoint for owner/admin review
- persisted revoked session state

Why this matters:

- bearer sessions are no longer permanent tokens
- logout has server-side effect
- role permissions are visible and ready for deeper product gating

## Stage 18. Billing Foundation

Billing now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/config.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/config.py)

Added:

- persisted billing subscriptions
- checkout-session creation
- billing portal-session creation
- local mock checkout mode when Stripe credentials are absent
- Stripe webhook-shaped endpoint for checkout completion events

Why this matters:

- the product now has a durable billing state model
- local development works without Stripe secrets
- real Stripe session creation can be swapped behind the existing endpoints

## Stage 19. Document Summarization Jobs

Document summarization now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/schemas.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/schemas.py)

Added:

- durable document summary job table
- summary job list/detail endpoints
- per-document summarize endpoint
- deterministic local summarizer based on normalized application context
- cached ready summary reuse unless forced

Why this matters:

- document summarization is now a job-backed product feature
- external LLM/pdf extraction can replace the local summary body later
- API consumers can track summary status instead of relying on ad hoc fields

## Stage 20. Relational Spatial / PostGIS Readiness

Spatial readiness now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)

Added:

- relational spatial index table for normalized application coordinates
- spatial index rebuild endpoint
- spatial index list endpoint
- readiness endpoint explaining current SQLite mode and PostGIS migration path

Why this matters:

- location data is no longer only embedded in normalized application payloads
- the app now has a clear migration seam for PostGIS geometry columns
- spatial search/indexing can evolve without rewriting normalized application reads

## Stage 21. Ingestion Backfills and Operations

Ingestion operations now live in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)
- [`plansignal/app/services/db.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/db.py)
- [`plansignal/app/services/ingestion.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/services/ingestion.py)

Added:

- durable ingestion job table
- admin-only backfill endpoint
- ingestion job list endpoint
- planning application backfill summary
- authority index and overlay refresh job modes
- sample fallback support for local/offline execution

Why this matters:

- ingestion work is now observable instead of only request/response
- operators can run controlled backfills from API/admin flows
- scheduled ingestion can reuse the same job model later

## Stage 22. Role Policy Surface

Role policy work now lives in:

- [`plansignal/app/main.py`](/C:/Users/paron/Desktop/Dev/unrelated%20projects/api/plansignal/app/main.py)

Added:

- owner/admin/member role policy map
- role policy endpoint
- current user permission echo

Why this matters:

- membership permissions are explicit instead of implicit route behavior only
- the UI/API can explain why an action is allowed or blocked
- richer permission gating has a stable policy source

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
- `POST /auth/logout`
- `POST /auth/accept-invite`
- `GET /me`
- `GET /org/roles`
- `GET /org/users`
- `GET /org/invitations`
- `POST /org/invitations`
- `DELETE /org/invitations/{invitation_id}`
- `POST /org/users`
- `PUT /org/users/{user_id}/role`
- `DELETE /org/users/{user_id}`
- `GET /api-keys`
- `POST /api-keys`
- `GET /billing/subscription`
- `POST /billing/checkout-session`
- `POST /billing/checkout-session/{session_id}/complete`
- `POST /billing/portal-session`
- `POST /billing/webhook/stripe`
- `GET /applications/raw`
- `GET /applications`
- `GET /applications/{application_id}`
- `GET /applications/{application_id}/history`
- `GET /applications/{application_id}/documents`
- `POST /applications/{application_id}/documents/{document_id}/summarize`
- `GET /document-summaries/jobs`
- `GET /document-summaries/jobs/{job_id}`
- `GET /areas/{area_id}/activity`
- `GET /actors/{actor_id}/applications`
- `GET /spatial/readiness`
- `POST /spatial/index/rebuild`
- `GET /spatial/index`
- `POST /reports/save`
- `GET /reports/saved`
- `PUT /reports/saved/{report_id}`
- `DELETE /reports/saved/{report_id}`
- `GET /reports/saved/{report_id}`
- `POST /reports/scheduled`
- `PUT /reports/scheduled/{schedule_id}`
- `DELETE /reports/scheduled/{schedule_id}`
- `GET /reports/scheduled`
- `POST /reports/scheduled/{schedule_id}/run`
- `POST /watchlists`
- `PUT /watchlists/{id}`
- `DELETE /watchlists/{id}`
- `GET /watchlists`
- `GET /watchlists/{id}`
- `GET /watchlists/{id}/changes`
- `POST /alerts/test`
- `POST /webhooks/endpoints`
- `PUT /webhooks/endpoints/{webhook_id}`
- `DELETE /webhooks/endpoints/{webhook_id}`
- `GET /webhooks/endpoints`
- `POST /webhooks/endpoints/{webhook_id}/rotate-secret`
- `POST /webhooks/endpoints/{webhook_id}/test`
- `GET /webhooks/deliveries`
- `GET /ops/ingestion/jobs`
- `POST /ops/ingestion/backfill`
- `GET /usage`
- `GET /signals/high-priority`
- `POST /screen/sites`
- `GET /benchmark/boroughs`
- `POST /natural-language-query`

## External Deployment Wiring Still Needed

The local roadmap foundation is now implemented. Production deployment still needs environment-specific provisioning:

- real Stripe account, price IDs, webhook signing verification, and Stripe SDK/session creation
- managed Postgres/PostGIS migration and data migration from local SQLite
- production SSO/MFA provider if enterprise customers require it
- external document extraction/LLM provider for richer summaries
- scheduled worker process for recurring ingestion/backfills outside the API process

Those are deployment/infrastructure tasks rather than missing local product foundations.
