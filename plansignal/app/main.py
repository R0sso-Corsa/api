from __future__ import annotations

import csv
import math
from datetime import datetime
from io import StringIO

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.responses import StreamingResponse

from .blueprint import get_icp_profile, get_stage_map
from .config import TEMPLATES_DIR
from .schemas import (
    AlertTestRequest,
    ApiKeyCreateRequest,
    AuthenticatedContext,
    BillingCheckoutRequest,
    DocumentSummaryRequest,
    IngestionJobCreateRequest,
    InvitationAcceptRequest,
    NaturalLanguageQueryRequest,
    OrganizationInvitationCreateRequest,
    OrganizationUserCreateRequest,
    SavedReportCreateRequest,
    SavedReportUpdateRequest,
    ScheduledReportCreateRequest,
    ScheduledReportUpdateRequest,
    SiteWaitlistRequest,
    SiteScreenRequest,
    UserRoleUpdateRequest,
    UserLoginRequest,
    UserRegistrationRequest,
    WatchlistCreateRequest,
    WatchlistUpdateRequest,
    WatchlistWebhookLinkRequest,
    WebhookEndpointCreateRequest,
    WebhookEndpointSecretRotateRequest,
    WebhookEndpointUpdateRequest,
)
from .services.auth import require_admin_session_context, require_api_key, require_owner_session_context
from .services.db import create_api_key as create_api_key_record
from .services.db import complete_billing_checkout_session
from .services.db import complete_document_summary_job
from .services.db import complete_ingestion_job
from .services.db import create_billing_checkout_session
from .services.db import create_billing_portal_session
from .services.db import create_document_summary_job
from .services.db import create_ingestion_job
from .services.db import create_organization_invitation
from .services.db import create_organization_user
from .services.db import fail_document_summary_job
from .services.db import fail_ingestion_job
from .services.db import delete_saved_report as delete_saved_report_record
from .services.db import delete_organization_user
from .services.db import accept_organization_invitation
from .services.db import create_saved_report
from .services.db import create_scheduled_report
from .services.db import create_site_waitlist_entry
from .services.db import create_watchlist as create_watchlist_record
from .services.db import delete_scheduled_report as delete_scheduled_report_record
from .services.db import get_watchlist as get_watchlist_record
from .services.db import get_saved_report
from .services.db import get_scheduled_report
from .services.db import get_user_profile
from .services.db import init_db
from .services.db import get_billing_subscription
from .services.db import get_document_summary_job
from .services.db import get_latest_document_summary_job
from .services.db import list_api_keys
from .services.db import list_document_summary_jobs
from .services.db import list_email_outbox
from .services.db import list_ingestion_jobs
from .services.db import list_organization_invitations
from .services.db import list_organization_users
from .services.db import list_scheduled_reports
from .services.db import list_saved_reports
from .services.db import list_watchlists as list_watchlist_records
from .services.db import list_webhook_deliveries
from .services.db import list_webhook_endpoints
from .services.db import login_user
from .services.db import create_webhook_endpoint
from .services.db import delete_watchlist as delete_watchlist_record
from .services.db import delete_webhook_endpoint as delete_webhook_endpoint_record
from .services.db import get_webhook_endpoint
from .services.db import link_watchlist_webhook
from .services.db import mark_watchlist_webhook_sent
from .services.db import queue_webhook_delivery
from .services.db import record_usage as record_usage_event
from .services.db import register_user
from .services.db import replace_spatial_index
from .services.db import revoke_organization_invitation
from .services.db import revoke_session
from .services.db import rotate_webhook_endpoint_secret
from .services.db import update_saved_report as update_saved_report_record
from .services.db import update_scheduled_report as update_scheduled_report_record
from .services.db import update_organization_user_role
from .services.db import update_watchlist as update_watchlist_record
from .services.db import update_webhook_endpoint as update_webhook_endpoint_record
from .services.db import usage_snapshot
from .services.db import list_spatial_index_entries
from .services.ingestion import fetch_authorities_live, fetch_overlay_dataset, fetch_planning_data, fetch_sample_planning_data
from .services.normalizer import build_area_activity, get_source_kind, normalize_envelope
from .services.query import (
    actor_applications,
    benchmark_boroughs,
    build_watchlist_alert_payload,
    filter_applications,
    high_priority_signals,
    natural_language_query,
    screen_sites,
    watchlist_changes,
)
from .services.scheduler import (
    deliver_pending_outbox_once,
    deliver_pending_webhooks_once,
    dispatch_webhook_delivery,
    run_due_schedules_once,
    run_schedule_now,
    run_watchlist_webhooks_once,
    scheduler_status,
    start_scheduler,
)


app = FastAPI(
    title="PlanSignal API",
    version="0.1.0",
    description="England-first planning intelligence API scaffold for consultancies and land buyers.",
)


ROLE_POLICIES = {
    "owner": [
        "read",
        "manage_members",
        "manage_billing",
        "manage_integrations",
        "manage_reports",
        "run_operations",
    ],
    "admin": [
        "read",
        "manage_integrations",
        "manage_reports",
        "run_operations",
    ],
    "member": [
        "read",
        "manage_reports",
    ],
}


def _render_template(name: str) -> str:
    return (TEMPLATES_DIR / name).read_text(encoding="utf-8")


def _load_applications(
    *,
    area_id: str | None = None,
    live: bool = True,
    limit: int = 1000,
    use_sample_fallback: bool = False,
):
    envelope = fetch_planning_data(area_id=area_id, limit=limit, use_sample_fallback=use_sample_fallback)
    return normalize_envelope(envelope)


def _official_context_summary(*, article_4_limit: int = 120, brownfield_limit: int = 120) -> dict:
    article_4 = fetch_overlay_dataset("article-4-direction", limit=article_4_limit)
    brownfield = fetch_overlay_dataset("brownfield-land", limit=brownfield_limit)
    green_belt = fetch_overlay_dataset("green-belt", limit=120)
    developer_agreements = fetch_overlay_dataset("developer-agreement", limit=120)

    return {
        "article_4": {
            "dataset": "article-4-direction",
            "loaded_count": article_4["loaded_count"],
            "total_available": article_4["total_available"],
            "recent": [
                {
                    "entity": str(item.get("entity")),
                    "name": item.get("name") or item.get("reference") or "Unnamed Article 4 direction",
                    "reference": item.get("reference"),
                    "start_date": item.get("start-date"),
                    "description": item.get("description"),
                    "documentation_url": item.get("documentation-url"),
                    "organisation_entity": item.get("organisation-entity"),
                    "quality": item.get("quality"),
                }
                for item in article_4["records"][:8]
            ],
        },
        "brownfield": {
            "dataset": "brownfield-land",
            "loaded_count": brownfield["loaded_count"],
            "total_available": brownfield["total_available"],
            "recent": [
                {
                    "entity": str(item.get("entity")),
                    "name": item.get("name") or item.get("reference") or "Unnamed brownfield site",
                    "reference": item.get("reference"),
                    "site_address": item.get("site-address"),
                    "planning_permission_status": item.get("planning-permission-status"),
                    "planning_permission_type": item.get("planning-permission-type"),
                    "minimum_net_dwellings": item.get("minimum-net-dwellings"),
                    "maximum_net_dwellings": item.get("maximum-net-dwellings"),
                    "hectares": item.get("hectares"),
                    "site_plan_url": item.get("site-plan-url"),
                    "organisation_entity": item.get("organisation-entity"),
                    "deliverable": item.get("deliverable"),
                }
                for item in brownfield["records"][:8]
            ],
        },
        "green_belt": {
            "dataset": "green-belt",
            "loaded_count": green_belt["loaded_count"],
            "total_available": green_belt["total_available"],
            "recent": [
                {
                    "entity": str(item.get("entity")),
                    "name": item.get("name") or item.get("reference") or "Unnamed green belt area",
                    "reference": item.get("reference"),
                    "green_belt_core": item.get("green-belt-core"),
                    "local_authority_district": item.get("local-authority-district"),
                    "documentation_url": item.get("documentation-url"),
                    "organisation_entity": item.get("organisation-entity"),
                    "quality": item.get("quality"),
                    "point": item.get("point"),
                }
                for item in green_belt["records"][:8]
            ],
        },
        "developer_agreements": {
            "dataset": "developer-agreement",
            "loaded_count": developer_agreements["loaded_count"],
            "total_available": developer_agreements["total_available"],
            "recent": [
                {
                    "entity": str(item.get("entity")),
                    "reference": item.get("reference"),
                    "name": item.get("name") or item.get("reference") or "Unnamed developer agreement",
                    "planning_application": item.get("planning-application"),
                    "developer_agreement_type": item.get("developer-agreement-type"),
                    "start_date": item.get("start-date"),
                    "entry_date": item.get("entry-date"),
                    "document_url": item.get("document-url"),
                    "organisation_entity": item.get("organisation-entity"),
                    "quality": item.get("quality"),
                    "point": item.get("point"),
                }
                for item in developer_agreements["records"][:8]
            ],
        },
    }


def _parse_point_wkt(value: str | None) -> dict[str, float] | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned.startswith("POINT"):
        return None
    try:
        coords = cleaned[cleaned.find("(") + 1:cleaned.find(")")].split()
        lon = float(coords[0])
        lat = float(coords[1])
        return {"lat": lat, "lon": lon}
    except (ValueError, IndexError):
        return None


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return radius_km * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def _coords_match_filters(
    coords: dict[str, float],
    *,
    center_lat: float | None = None,
    center_lon: float | None = None,
    radius_km: float | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lon: float | None = None,
    max_lon: float | None = None,
) -> bool:
    if center_lat is not None and center_lon is not None and radius_km is not None:
        if _distance_km(center_lat, center_lon, coords["lat"], coords["lon"]) > radius_km:
            return False
    if min_lat is not None and coords["lat"] < min_lat:
        return False
    if max_lat is not None and coords["lat"] > max_lat:
        return False
    if min_lon is not None and coords["lon"] < min_lon:
        return False
    if max_lon is not None and coords["lon"] > max_lon:
        return False
    return True


def _application_context(application) -> dict:
    context = _official_context_summary()
    organisation_entity = str(application.raw_payload.get("organisation-entity") or "").strip()
    raw_entity = str(application.raw_payload.get("entity") or "").strip()
    point = _parse_point_wkt(application.raw_payload.get("point"))

    def same_org(records: list[dict]) -> list[dict]:
        if not organisation_entity:
            return []
        return [item for item in records if str(item.get("organisation_entity") or "").strip() == organisation_entity][:8]

    linked_developer_agreements = []
    for item in context["developer_agreements"]["recent"]:
        planning_application = str(item.get("planning_application") or "").strip()
        if raw_entity and planning_application == raw_entity:
            linked_developer_agreements.append(item)
        elif application.source_reference and planning_application == application.source_reference:
            linked_developer_agreements.append(item)

    nearby_counts = {
        "brownfield_within_5km": 0,
        "green_belt_within_5km": 0,
        "developer_agreements_within_5km": 0,
    }
    if point:
        for item in context["brownfield"]["recent"]:
            coords = _parse_point_wkt(item.get("point"))
            if coords and _distance_km(point["lat"], point["lon"], coords["lat"], coords["lon"]) <= 5:
                nearby_counts["brownfield_within_5km"] += 1
        for item in context["green_belt"]["recent"]:
            coords = _parse_point_wkt(item.get("point"))
            if coords and _distance_km(point["lat"], point["lon"], coords["lat"], coords["lon"]) <= 5:
                nearby_counts["green_belt_within_5km"] += 1
        for item in context["developer_agreements"]["recent"]:
            coords = _parse_point_wkt(item.get("point"))
            if coords and _distance_km(point["lat"], point["lon"], coords["lat"], coords["lon"]) <= 5:
                nearby_counts["developer_agreements_within_5km"] += 1

    return {
        "authority_matches": {
            "article_4": same_org(context["article_4"]["recent"]),
            "brownfield": same_org(context["brownfield"]["recent"]),
            "green_belt": same_org(context["green_belt"]["recent"]),
            "developer_agreements": same_org(context["developer_agreements"]["recent"]),
        },
        "linked_developer_agreements": linked_developer_agreements,
        "nearby_counts": nearby_counts,
    }


def _map_payload(
    *,
    center_lat: float | None = None,
    center_lon: float | None = None,
    radius_km: float | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lon: float | None = None,
    max_lon: float | None = None,
) -> dict:
    envelope = fetch_planning_data(limit=300)
    applications = normalize_envelope(envelope)
    plotted_apps = []
    for app in applications:
        point = app.raw_payload.get("point")
        coords = _parse_point_wkt(point)
        if coords and _coords_match_filters(
            coords,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_km=radius_km,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ):
            plotted_apps.append(
                {
                    "application_id": app.application_id,
                    "reference": app.source_reference,
                    "address": app.site.address,
                    "authority": app.authority.name,
                    "status": app.status,
                    "proposal_category": app.proposal_category,
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                }
            )

    brownfield = fetch_overlay_dataset("brownfield-land", limit=200)
    plotted_brownfield = []
    for item in brownfield["records"]:
        coords = _parse_point_wkt(item.get("point"))
        if coords and _coords_match_filters(
            coords,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_km=radius_km,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ):
            plotted_brownfield.append(
                {
                    "reference": item.get("reference"),
                    "name": item.get("name") or item.get("reference"),
                    "site_address": item.get("site-address"),
                    "hectares": item.get("hectares"),
                    "deliverable": item.get("deliverable"),
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                }
            )

    green_belt = fetch_overlay_dataset("green-belt", limit=200)
    plotted_green_belt = []
    for item in green_belt["records"]:
        coords = _parse_point_wkt(item.get("point"))
        if coords and _coords_match_filters(
            coords,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_km=radius_km,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ):
            plotted_green_belt.append(
                {
                    "reference": item.get("reference"),
                    "name": item.get("name") or item.get("reference"),
                    "green_belt_core": item.get("green-belt-core"),
                    "local_authority_district": item.get("local-authority-district"),
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                }
            )

    developer_agreements = fetch_overlay_dataset("developer-agreement", limit=200)
    plotted_developer_agreements = []
    for item in developer_agreements["records"]:
        coords = _parse_point_wkt(item.get("point"))
        if coords and _coords_match_filters(
            coords,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_km=radius_km,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        ):
            plotted_developer_agreements.append(
                {
                    "reference": item.get("reference"),
                    "planning_application": item.get("planning-application"),
                    "developer_agreement_type": item.get("developer-agreement-type"),
                    "document_url": item.get("document-url"),
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                }
            )

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "applications": plotted_apps[:150],
        "brownfield": plotted_brownfield[:150],
        "green_belt": plotted_green_belt[:150],
        "developer_agreements": plotted_developer_agreements[:150],
        "application_count": len(plotted_apps),
        "brownfield_count": len(plotted_brownfield),
        "green_belt_count": len(plotted_green_belt),
        "developer_agreement_count": len(plotted_developer_agreements),
        "filter": {
            "center_lat": center_lat,
            "center_lon": center_lon,
            "radius_km": radius_km,
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
        },
    }


def _record_usage(context: AuthenticatedContext | None, metric: str) -> None:
    if context:
        record_usage_event(context.organization_id, metric)


def _bearer_token(authorization: str | None) -> str | None:
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    return None


def _find_application(application_id: str):
    for application in _load_applications():
        if application.application_id == application_id:
            return application
    return None


def _find_application_document(application_id: str, document_id: str):
    application = _find_application(application_id)
    if not application:
        return None, None
    for document in application.documents:
        if document.document_id == document_id:
            return application, document
    return application, None


def _build_document_summary(application, document) -> str:
    event_titles = [event.title for event in application.change_history[:3]]
    event_text = "; ".join(event_titles) if event_titles else "No major events recorded."
    published = document.published_date.isoformat() if document.published_date else "unknown date"
    doc_type = document.document_type or "planning document"
    return (
        f"{document.title} is a {doc_type} published {published}. "
        f"It relates to {application.site.address} under {application.source_reference}. "
        f"Proposal: {application.proposal_text}. Current status: {application.status}. "
        f"Recent context: {event_text}"
    )


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coordinates_from_application(application) -> dict[str, float] | None:
    latitude = _float_or_none(application.site.latitude)
    longitude = _float_or_none(application.site.longitude)
    if latitude is not None and longitude is not None:
        return {"lat": latitude, "lon": longitude}

    raw = application.raw_payload or {}
    for point_key in ("point", "geometry", "wkt"):
        point = _parse_point_wkt(raw.get(point_key))
        if point:
            return point

    latitude = None
    longitude = None
    for key in ("latitude", "lat", "y"):
        latitude = _float_or_none(raw.get(key))
        if latitude is not None:
            break
    for key in ("longitude", "lon", "lng", "x"):
        longitude = _float_or_none(raw.get(key))
        if longitude is not None:
            break
    if latitude is not None and longitude is not None:
        return {"lat": latitude, "lon": longitude}
    return None


def _spatial_entries_from_applications(applications) -> list[dict]:
    entries = []
    for application in applications:
        coordinates = _coordinates_from_application(application)
        if not coordinates:
            continue
        entries.append(
            {
                "application_id": application.application_id,
                "latitude": coordinates["lat"],
                "longitude": coordinates["lon"],
                "authority_name": application.authority.name,
                "address": application.site.address,
                "source_system": application.source_system,
            }
        )
    return entries


def _run_ingestion_job(job_id: str, payload: IngestionJobCreateRequest):
    try:
        if payload.source == "planning_applications":
            envelope = fetch_planning_data(
                area_id=payload.area_id,
                limit=payload.limit,
                use_sample_fallback=payload.use_sample_fallback,
            )
            normalized = normalize_envelope(envelope)
            result = {
                "source": envelope.source,
                "raw_count": len(envelope.records),
                "normalized_count": len(normalized),
                "total_available": envelope.total_available,
                "sample_fallback_allowed": payload.use_sample_fallback,
            }
        elif payload.source == "authority_index":
            authorities = fetch_authorities_live()
            result = {
                "source": authorities["source"],
                "authority_count": authorities["count"],
            }
        else:
            datasets = ["article-4-direction", "brownfield-land", "green-belt", "developer-agreement"]
            loaded = {}
            for dataset in datasets:
                overlay = fetch_overlay_dataset(dataset, limit=min(payload.limit, 500))
                loaded[dataset] = {
                    "loaded_count": overlay["loaded_count"],
                    "total_available": overlay["total_available"],
                }
            result = {"datasets": loaded}
        return complete_ingestion_job(job_id, result=result)
    except Exception as exc:
        return fail_ingestion_job(job_id, failure_reason=str(exc))


@app.on_event("startup")
def startup() -> None:
    init_db()
    start_scheduler()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "product": "PlanSignal"}


@app.get("/", response_class=HTMLResponse)
def landing() -> str:
    return _render_template("index.html")


@app.get("/product", response_class=HTMLResponse)
def product_page() -> str:
    return _render_template("product.html")


@app.get("/pricing", response_class=HTMLResponse)
def pricing_page() -> str:
    return _render_template("pricing.html")


@app.get("/developers", response_class=HTMLResponse)
def developers_page() -> str:
    return _render_template("developers.html")


@app.get("/sources", response_class=HTMLResponse)
def sources_page() -> str:
    return _render_template("sources.html")


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> str:
    return _render_template("dashboard.html")


@app.get("/map", response_class=HTMLResponse)
def map_page() -> str:
    return _render_template("map.html")


@app.get("/reports", response_class=HTMLResponse)
def reports_page() -> str:
    return _render_template("reports.html")


@app.get("/reports/{report_id}", response_class=HTMLResponse)
def report_detail_page(report_id: str) -> str:
    return _render_template("report_detail.html")


@app.get("/contact", response_class=HTMLResponse)
def contact_page() -> str:
    return _render_template("contact.html")


@app.get("/playground", response_class=HTMLResponse)
def playground_page() -> str:
    return _render_template("playground.html")


@app.get("/authorities")
def authorities(region: str | None = Query(default=None)) -> dict:
    payload = fetch_authorities_live()
    authorities = payload["authorities"]
    if region:
        authorities = [item for item in authorities if item.get("region", "").lower() == region.lower()]
    return {
        "source": payload["source"],
        "count": len(authorities),
        "authorities": authorities,
    }


@app.get("/blueprint/icp")
def blueprint_icp() -> dict:
    return get_icp_profile()


@app.get("/blueprint/stages")
def blueprint_stages() -> list[dict]:
    return get_stage_map()


@app.post("/auth/register")
def auth_register(payload: UserRegistrationRequest) -> dict:
    user = register_user(
        organization_name=payload.organization_name,
        full_name=payload.full_name,
        email=payload.email,
        password=payload.password,
    )
    session = login_user(payload.email, payload.password)
    return {
        "user": user.model_dump(mode="json"),
        "session": session.model_dump(mode="json") if session else None,
    }


@app.post("/site/waitlist")
def site_waitlist(payload: SiteWaitlistRequest) -> dict:
    entry = create_site_waitlist_entry(payload)
    return {
        "status": "queued",
        "entry": entry.model_dump(mode="json"),
        "message": "Waitlist request saved. PlanSignal can follow up with pilot access.",
    }


@app.post("/auth/login")
def auth_login(payload: UserLoginRequest) -> dict:
    session = login_user(payload.email, payload.password)
    if not session:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return session.model_dump(mode="json")


@app.post("/auth/logout")
def auth_logout(
    authorization: str | None = Header(default=None),
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    if context.auth_method != "session":
        raise HTTPException(status_code=403, detail="Log out requires a session")
    token = _bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=400, detail="Bearer token required for logout")
    return {"revoked": revoke_session(token)}


@app.post("/auth/accept-invite")
def auth_accept_invite(payload: InvitationAcceptRequest) -> dict:
    try:
        invitation, user, session = accept_organization_invitation(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "invitation": invitation.model_dump(mode="json"),
        "user": user.model_dump(mode="json"),
        "session": session.model_dump(mode="json"),
    }


@app.get("/me")
def me(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "me_requests")
    profile = get_user_profile(context.user_id) if context.user_id else None
    return {
        "context": context.model_dump(mode="json"),
        "user": profile.model_dump(mode="json") if profile else None,
    }


@app.get("/org/roles")
def org_roles(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "organization_role_policy_requests")
    require_admin_session_context(context, action="View organization role policies")
    return {
        "roles": ROLE_POLICIES,
        "current_user_role": context.user_role,
        "current_user_permissions": ROLE_POLICIES.get(context.user_role or "", []),
    }


@app.get("/org/users")
def organization_users(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "organization_user_list_requests")
    require_admin_session_context(context, action="List organization users")
    return [item.model_dump(mode="json") for item in list_organization_users(context.organization_id)]


@app.get("/org/invitations")
def organization_invitations(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "organization_invitation_list_requests")
    require_admin_session_context(context, action="List organization invitations")
    return [item.model_dump(mode="json") for item in list_organization_invitations(context.organization_id)]


@app.post("/org/invitations")
def create_org_invitation(
    payload: OrganizationInvitationCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "organization_invitation_create_requests")
    require_owner_session_context(context, action="Create organization invitations")
    try:
        invitation = create_organization_invitation(
            context.organization_id,
            payload,
            invited_by_user_id=context.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return invitation.model_dump(mode="json")


@app.post("/org/users")
def create_org_user(
    payload: OrganizationUserCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "organization_user_create_requests")
    require_owner_session_context(context, action="Create organization users")
    try:
        user = create_organization_user(context.organization_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return user.model_dump(mode="json")


@app.put("/org/users/{user_id}/role")
def update_org_user_role(
    user_id: str,
    payload: UserRoleUpdateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "organization_user_role_update_requests")
    require_owner_session_context(context, action="Update organization user roles")
    try:
        user = update_organization_user_role(
            context.organization_id,
            user_id,
            payload,
            acting_user_id=context.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not user:
        raise HTTPException(status_code=404, detail="Organization user not found")
    return user.model_dump(mode="json")


@app.delete("/org/users/{user_id}")
def delete_org_user(
    user_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "organization_user_delete_requests")
    require_owner_session_context(context, action="Delete organization users")
    try:
        deleted = delete_organization_user(
            context.organization_id,
            user_id,
            acting_user_id=context.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="Organization user not found")
    return {"user_id": user_id, "deleted": True}


@app.delete("/org/invitations/{invitation_id}")
def delete_org_invitation(
    invitation_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "organization_invitation_delete_requests")
    require_owner_session_context(context, action="Revoke organization invitations")
    try:
        invitation = revoke_organization_invitation(context.organization_id, invitation_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not invitation:
        raise HTTPException(status_code=404, detail="Organization invitation not found")
    return invitation.model_dump(mode="json")


@app.get("/api-keys")
def api_keys(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "api_key_list_requests")
    return [item.model_dump(mode="json") for item in list_api_keys(context.organization_id)]


@app.post("/api-keys")
def create_api_key(
    payload: ApiKeyCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "api_key_create_requests")
    require_admin_session_context(context, action="Create API keys")
    return create_api_key_record(context.organization_id, payload.label).model_dump(mode="json")


@app.get("/billing/subscription")
def billing_subscription(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "billing_subscription_requests")
    require_admin_session_context(context, action="View billing subscription")
    return get_billing_subscription(context.organization_id).model_dump(mode="json")


@app.post("/billing/checkout-session")
def billing_checkout_session(
    payload: BillingCheckoutRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "billing_checkout_session_requests")
    require_owner_session_context(context, action="Create billing checkout sessions")
    session = create_billing_checkout_session(context.organization_id, payload)
    return session.model_dump(mode="json")


@app.post("/billing/checkout-session/{session_id}/complete")
def billing_complete_checkout_session(
    session_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "billing_checkout_complete_requests")
    require_owner_session_context(context, action="Complete billing checkout sessions")
    subscription = complete_billing_checkout_session(context.organization_id, session_id)
    if not subscription:
        raise HTTPException(status_code=404, detail="Billing checkout session not found")
    return subscription.model_dump(mode="json")


@app.post("/billing/portal-session")
def billing_portal_session(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "billing_portal_session_requests")
    require_owner_session_context(context, action="Create billing portal sessions")
    return create_billing_portal_session(context.organization_id).model_dump(mode="json")


@app.post("/billing/webhook/stripe")
def billing_stripe_webhook(payload: dict) -> dict:
    event_type = payload.get("type") or payload.get("event")
    data_object = (payload.get("data") or {}).get("object") or payload
    metadata = data_object.get("metadata") or {}
    organization_id = metadata.get("organization_id") or data_object.get("client_reference_id")
    session_id = data_object.get("id") or data_object.get("session_id")
    if event_type == "checkout.session.completed" and organization_id and session_id:
        subscription = complete_billing_checkout_session(organization_id, session_id)
        return {
            "received": True,
            "applied": bool(subscription),
            "event_type": event_type,
            "subscription": subscription.model_dump(mode="json") if subscription else None,
        }
    return {"received": True, "applied": False, "event_type": event_type}


@app.get("/applications/raw")
def raw_applications(
    area_id: str | None = Query(default=None),
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "raw_application_requests")
    envelope = fetch_planning_data(area_id=area_id)
    return envelope.model_dump(mode="json")


@app.get("/official-context/summary")
def official_context_summary(
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "official_context_requests")
    return _official_context_summary()


@app.get("/map/data")
def map_data(
    center_lat: float | None = None,
    center_lon: float | None = None,
    radius_km: float | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lon: float | None = None,
    max_lon: float | None = None,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "map_data_requests")
    return _map_payload(
        center_lat=center_lat,
        center_lon=center_lon,
        radius_km=radius_km,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lon=min_lon,
        max_lon=max_lon,
    )


@app.get("/spatial/readiness")
def spatial_readiness(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "spatial_readiness_requests")
    entries = list_spatial_index_entries(context.organization_id)
    return {
        "mode": "sqlite-spatial-index",
        "postgis_ready": True,
        "entry_count": len(entries),
        "message": "Spatial rows are normalized into a relational index and can be migrated to PostGIS geometry columns later.",
    }


@app.post("/spatial/index/rebuild")
def spatial_index_rebuild(
    limit: int = Query(default=300, ge=1, le=5000),
    include_sample_if_empty: bool = Query(default=True),
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "spatial_index_rebuild_requests")
    require_admin_session_context(context, action="Rebuild spatial index")
    envelope = fetch_planning_data(limit=limit, use_sample_fallback=True)
    applications = normalize_envelope(envelope)
    count = replace_spatial_index(context.organization_id, _spatial_entries_from_applications(applications))
    source = envelope.source
    sample_fallback_used = False
    loaded_count = len(applications)

    if count == 0 and include_sample_if_empty:
        sample_envelope = fetch_sample_planning_data()
        sample_applications = normalize_envelope(sample_envelope)
        sample_entries = _spatial_entries_from_applications(sample_applications)
        if sample_entries:
            count = replace_spatial_index(context.organization_id, sample_entries)
            source = sample_envelope.source
            sample_fallback_used = True
            loaded_count = len(sample_applications)

    return {
        "indexed_count": count,
        "loaded_count": loaded_count,
        "skipped_no_coordinates": max(loaded_count - count, 0),
        "source": source,
        "sample_fallback_used": sample_fallback_used,
        "mode": "sqlite-spatial-index",
    }


@app.get("/spatial/index")
def spatial_index(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "spatial_index_requests")
    return [item.model_dump(mode="json") for item in list_spatial_index_entries(context.organization_id)]


@app.get("/applications/view/{application_id}", response_class=HTMLResponse)
def application_detail_page(application_id: str) -> str:
    return _render_template("application_detail.html")


@app.get("/applications")
def list_applications(
    area_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    applicant: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    proposal_category: str | None = Query(default=None),
    changed_since: str | None = Query(default=None),
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "application_list_requests")
    applications = _load_applications(area_id=area_id)
    changed_since_date = datetime.fromisoformat(changed_since).date() if changed_since else None
    applications = filter_applications(
        applications,
        area_id=area_id,
        status=status,
        proposal_category=proposal_category,
        applicant=applicant,
        keyword=keyword,
        changed_since=changed_since_date,
    )
    return [app.model_dump(mode="json") for app in applications]


@app.get("/applications/{application_id}")
def get_application(
    application_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "application_detail_requests")
    applications = _load_applications()

    for application in applications:
        if application.application_id == application_id:
            return application.model_dump(mode="json")

    raise HTTPException(status_code=404, detail="Application not found")


@app.get("/applications/{application_id}/context")
def get_application_context(
    application_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "application_context_requests")
    applications = _load_applications()
    for application in applications:
        if application.application_id == application_id:
            return {
                "application": {**application.model_dump(mode="json"), "source_kind": get_source_kind(application.source_system)},
                "context": _application_context(application),
            }
    raise HTTPException(status_code=404, detail="Application not found")


@app.get("/applications/{application_id}/history")
def get_application_history(
    application_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "application_history_requests")
    applications = _load_applications()

    for application in applications:
        if application.application_id == application_id:
            return [event.model_dump(mode="json") for event in application.change_history]

    raise HTTPException(status_code=404, detail="Application not found")


@app.get("/applications/{application_id}/documents")
def get_application_documents(
    application_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "application_document_requests")
    applications = _load_applications()

    for application in applications:
        if application.application_id == application_id:
            return [document.model_dump(mode="json") for document in application.documents]

    raise HTTPException(status_code=404, detail="Application not found")


@app.post("/applications/{application_id}/documents/{document_id}/summarize")
def summarize_application_document(
    application_id: str,
    document_id: str,
    payload: DocumentSummaryRequest | None = None,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "document_summary_requests")
    application, document = _find_application_document(application_id, document_id)
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    existing = get_latest_document_summary_job(
        context.organization_id,
        application_id=application_id,
        document_id=document_id,
    )
    if existing and existing.status == "ready" and not (payload and payload.force):
        return existing.model_dump(mode="json")
    job = create_document_summary_job(
        context.organization_id,
        application_id=application_id,
        document_id=document_id,
        source_url=document.url,
    )
    try:
        completed = complete_document_summary_job(job.job_id, summary=_build_document_summary(application, document))
    except Exception as exc:
        completed = fail_document_summary_job(job.job_id, failure_reason=str(exc))
    return completed.model_dump(mode="json") if completed else job.model_dump(mode="json")


@app.get("/document-summaries/jobs")
def document_summary_jobs(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "document_summary_job_list_requests")
    return [item.model_dump(mode="json") for item in list_document_summary_jobs(context.organization_id)]


@app.get("/document-summaries/jobs/{job_id}")
def document_summary_job(job_id: str, context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "document_summary_job_detail_requests")
    job = get_document_summary_job(job_id)
    if not job or job.organization_id != context.organization_id:
        raise HTTPException(status_code=404, detail="Document summary job not found")
    return job.model_dump(mode="json")


@app.get("/areas/{area_id}/activity")
def area_activity(
    area_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "area_activity_requests")
    applications = _load_applications(area_id=area_id)
    activity = build_area_activity(area_id, applications)
    return activity.model_dump(mode="json")


@app.get("/actors/{actor_id}/applications")
def get_actor_apps(
    actor_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "actor_application_requests")
    applications = _load_applications()
    return [app.model_dump(mode="json") for app in actor_applications(actor_id, applications)]


@app.post("/watchlists")
def create_watchlist(
    payload: WatchlistCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "watchlist_create_requests")
    if payload.delivery_mode == "webhook":
        require_admin_session_context(context, action="Create webhook watchlists")
        endpoint = get_webhook_endpoint(context.organization_id, payload.webhook_endpoint_id or "")
        if not endpoint:
            raise HTTPException(
                status_code=400,
                detail="webhook_endpoint_id must match an existing webhook endpoint for this organization.",
            )
    return create_watchlist_record(context.organization_id, payload).model_dump(mode="json")


@app.get("/watchlists")
def get_watchlists(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "watchlist_list_requests")
    return [
        watchlist.model_dump(mode="json")
        for watchlist in list_watchlist_records(context.organization_id)
    ]


@app.get("/watchlists/{watchlist_id}")
def fetch_watchlist(
    watchlist_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "watchlist_detail_requests")
    watchlist = get_watchlist_record(context.organization_id, watchlist_id)
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return watchlist.model_dump(mode="json")


@app.put("/watchlists/{watchlist_id}")
def update_watchlist(
    watchlist_id: str,
    payload: WatchlistUpdateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "watchlist_update_requests")
    if payload.delivery_mode == "webhook":
        require_admin_session_context(context, action="Update webhook watchlists")
        endpoint = get_webhook_endpoint(context.organization_id, payload.webhook_endpoint_id or "")
        if not endpoint:
            raise HTTPException(
                status_code=400,
                detail="webhook_endpoint_id must match an existing webhook endpoint for this organization.",
            )
    watchlist = update_watchlist_record(context.organization_id, watchlist_id, payload)
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return watchlist.model_dump(mode="json")


@app.delete("/watchlists/{watchlist_id}")
def delete_watchlist(
    watchlist_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "watchlist_delete_requests")
    require_admin_session_context(context, action="Delete watchlists")
    deleted = delete_watchlist_record(context.organization_id, watchlist_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return {"watchlist_id": watchlist_id, "deleted": True}


@app.post("/watchlists/{watchlist_id}/link-webhook")
def link_watchlist_to_webhook(
    watchlist_id: str,
    payload: WatchlistWebhookLinkRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "watchlist_webhook_link_requests")
    require_admin_session_context(context, action="Link webhooks")
    endpoint = get_webhook_endpoint(context.organization_id, payload.webhook_endpoint_id)
    if not endpoint:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    watchlist = link_watchlist_webhook(context.organization_id, watchlist_id, payload)
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return watchlist.model_dump(mode="json")


@app.get("/watchlists/{watchlist_id}/changes")
def get_watchlist_changes(
    watchlist_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "watchlist_change_requests")
    watchlist = get_watchlist_record(context.organization_id, watchlist_id)
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    applications = _load_applications()
    changes = watchlist_changes(watchlist, applications)
    return [change.model_dump(mode="json") for change in changes]


@app.post("/watchlists/{watchlist_id}/deliver")
def deliver_watchlist_webhook(
    watchlist_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "watchlist_webhook_deliver_requests")
    require_admin_session_context(context, action="Deliver watchlist webhooks")
    watchlist = get_watchlist_record(context.organization_id, watchlist_id)
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    if watchlist.delivery_mode != "webhook" or not watchlist.webhook_endpoint_id:
        raise HTTPException(status_code=400, detail="Watchlist not configured for webhook delivery")
    endpoint = get_webhook_endpoint(context.organization_id, watchlist.webhook_endpoint_id)
    if not endpoint:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    applications = [app for app in _load_applications() if get_source_kind(app.source_system) == "official"]
    changes = watchlist_changes(watchlist, applications)
    payload = build_watchlist_alert_payload(
        watchlist,
        applications,
        organization_id=context.organization_id,
    )
    if payload["summary"]["delivered_change_count"] == 0:
        return {
            "watchlist_id": watchlist.watchlist_id,
            "delivery_status": "skipped",
            "change_count": payload["summary"]["matched_change_count"],
            "delivered_change_count": 0,
            "reason": payload["summary"]["note"],
        }
    delivery = queue_webhook_delivery(
        context.organization_id,
        target_url=endpoint.target_url,
        event_type="watchlist.alert",
        payload=payload,
        payload_preview=f"{watchlist.name}: {payload['summary']['delivered_change_count']} delivered of {payload['summary']['matched_change_count']} matched",
        related_webhook_id=endpoint.webhook_id,
    )
    result = dispatch_webhook_delivery(
        delivery_id=delivery.delivery_id,
        organization_id=context.organization_id,
        target_url=endpoint.target_url,
        event_type="watchlist.alert",
        payload=payload,
        attempts_so_far=delivery.delivery_attempts,
        related_webhook_id=endpoint.webhook_id,
    )
    if result["delivery_status"] == "sent":
        mark_watchlist_webhook_sent(context.organization_id, watchlist.watchlist_id)
    return {
        "watchlist_id": watchlist.watchlist_id,
        "delivery_id": delivery.delivery_id,
        "delivery_status": result["delivery_status"],
        "change_count": payload["summary"]["matched_change_count"],
        "delivered_change_count": payload["summary"]["delivered_change_count"],
        "failure_reason": result["failure_reason"],
        "next_attempt_at": result["next_attempt_at"],
    }


@app.post("/alerts/test")
def test_alert(
    payload: AlertTestRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "alert_test_requests")
    target = payload.email or payload.webhook_url or payload.watchlist_id or "preview"
    channel = "email" if payload.email else "webhook" if payload.webhook_url else "watchlist"
    preview = f"PlanSignal test alert queued for {target}. Delivery mode {channel} ready for pilot workflows."
    if payload.webhook_url:
        delivery = queue_webhook_delivery(
            context.organization_id,
            target_url=payload.webhook_url,
            event_type="alert.test",
            payload={
                "event": "alert.test",
                "organization_id": context.organization_id,
                "preview": preview,
            },
            payload_preview=preview,
            related_webhook_id=None,
            signing_secret=(payload.webhook_secret or "").strip() or None,
        )
        result = dispatch_webhook_delivery(
            delivery_id=delivery.delivery_id,
            organization_id=context.organization_id,
            target_url=payload.webhook_url,
            event_type="alert.test",
            payload={
                "event": "alert.test",
                "organization_id": context.organization_id,
                "preview": preview,
            },
            attempts_so_far=delivery.delivery_attempts,
            signing_secret=(payload.webhook_secret or "").strip() or None,
        )
        return {
            "status": "queued" if result["delivery_status"] in {"sent", "queued"} else "simulated",
            "channel": channel,
            "target": target,
            "preview": preview,
            "webhook_delivery_id": delivery.delivery_id,
            "delivery_status": result["delivery_status"],
            "failure_reason": result["failure_reason"],
            "next_attempt_at": result["next_attempt_at"],
        }
    return {
        "status": "simulated",
        "channel": channel,
        "target": target,
        "preview": preview,
    }


@app.get("/usage")
def usage(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "usage_requests")
    return usage_snapshot(context.organization_id).model_dump(mode="json")


@app.get("/signals/high-priority")
def get_high_priority_signals(
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "high_priority_signal_requests")
    applications = _load_applications()
    return [signal.model_dump(mode="json") for signal in high_priority_signals(applications)]


@app.post("/screen/sites")
def post_screen_sites(
    payload: SiteScreenRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "site_screen_requests")
    applications = _load_applications()
    return [result.model_dump(mode="json") for result in screen_sites(payload.sites, applications)]


@app.get("/benchmark/boroughs")
def get_benchmark_boroughs(
    context: AuthenticatedContext = Depends(require_api_key),
) -> list[dict]:
    _record_usage(context, "benchmark_requests")
    applications = _load_applications()
    return [row.model_dump(mode="json") for row in benchmark_boroughs(applications)]


@app.post("/natural-language-query")
def post_natural_language_query(
    payload: NaturalLanguageQueryRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "natural_language_query_requests")
    applications = _load_applications()
    return natural_language_query(payload, applications).model_dump(mode="json")


@app.get("/dashboard/summary")
def dashboard_summary(
    area_id: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "dashboard_summary_requests")
    envelope = fetch_planning_data(area_id=area_id)
    applications = normalize_envelope(envelope)
    applications = filter_applications(
        applications,
        area_id=area_id,
        keyword=keyword,
    )
    applications = [app for app in applications if get_source_kind(app.source_system) == "official"]
    watchlists = list_watchlist_records(context.organization_id)
    profile = get_user_profile(context.user_id) if context.user_id else None
    official_context = _official_context_summary()
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "me": {
            "context": context.model_dump(mode="json"),
            "user": profile.model_dump(mode="json") if profile else None,
        },
        "data_sources": {
            "planning_applications": {
                "loaded_count": len(applications),
                "total_available": envelope.total_available,
            },
            "article_4": {
                "loaded_count": official_context["article_4"]["loaded_count"],
                "total_available": official_context["article_4"]["total_available"],
            },
            "brownfield": {
                "loaded_count": official_context["brownfield"]["loaded_count"],
                "total_available": official_context["brownfield"]["total_available"],
            },
            "green_belt": {
                "loaded_count": official_context["green_belt"]["loaded_count"],
                "total_available": official_context["green_belt"]["total_available"],
            },
            "developer_agreements": {
                "loaded_count": official_context["developer_agreements"]["loaded_count"],
                "total_available": official_context["developer_agreements"]["total_available"],
            },
        },
        "applications": [{**app.model_dump(mode="json"), "source_kind": "official"} for app in applications[:20]],
        "signals": [signal.model_dump(mode="json") for signal in high_priority_signals(applications)[:12]],
        "watchlists": [watchlist.model_dump(mode="json") for watchlist in watchlists[:8]],
        "usage": usage_snapshot(context.organization_id).model_dump(mode="json"),
        "benchmark": [row.model_dump(mode="json") for row in benchmark_boroughs(applications)[:6]],
        "api_keys": [item.model_dump(mode="json") for item in list_api_keys(context.organization_id)[:8]],
        "official_context": official_context,
    }


@app.get("/reports/summary")
def reports_summary(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "report_summary_requests")
    envelope = fetch_planning_data(limit=500)
    applications = [app for app in normalize_envelope(envelope) if get_source_kind(app.source_system) == "official"]
    signals = high_priority_signals(applications)
    official_context = _official_context_summary()
    by_status: dict[str, int] = {}
    by_category: dict[str, int] = {}
    for app in applications:
        by_status[app.status] = by_status.get(app.status, 0) + 1
        by_category[app.proposal_category] = by_category.get(app.proposal_category, 0) + 1
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "application_count": len(applications),
        "signal_count": len(signals),
        "status_counts": by_status,
        "category_counts": by_category,
        "top_boroughs": [row.model_dump(mode="json") for row in benchmark_boroughs(applications)[:10]],
        "official_context_counts": {
            "article_4": official_context["article_4"]["loaded_count"],
            "brownfield": official_context["brownfield"]["loaded_count"],
            "green_belt": official_context["green_belt"]["loaded_count"],
            "developer_agreements": official_context["developer_agreements"]["loaded_count"],
        },
    }


@app.post("/reports/save")
def save_report(
    payload: SavedReportCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "report_save_requests")
    envelope = fetch_planning_data(limit=500)
    applications = normalize_envelope(envelope)
    applications = filter_applications(applications, area_id=payload.area_id, keyword=payload.keyword)
    applications = [app for app in applications if get_source_kind(app.source_system) == "official"]
    signals = high_priority_signals(applications)
    entry = create_saved_report(
        context.organization_id,
        payload,
        application_count=len(applications),
        signal_count=len(signals),
    )
    return entry.model_dump(mode="json")


@app.get("/reports/saved")
def saved_reports(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "saved_report_list_requests")
    return [item.model_dump(mode="json") for item in list_saved_reports(context.organization_id)]


@app.put("/reports/saved/{report_id}")
def update_saved_report(
    report_id: str,
    payload: SavedReportUpdateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "saved_report_update_requests")
    require_admin_session_context(context, action="Update saved reports")
    entry = update_saved_report_record(context.organization_id, report_id, payload)
    if not entry:
        raise HTTPException(status_code=404, detail="Saved report not found")
    return entry.model_dump(mode="json")


@app.delete("/reports/saved/{report_id}")
def delete_saved_report(
    report_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "saved_report_delete_requests")
    require_admin_session_context(context, action="Delete saved reports")
    deleted = delete_saved_report_record(context.organization_id, report_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Saved report not found")
    return {"report_id": report_id, "deleted": True}


@app.get("/reports/saved/{report_id}")
def saved_report_detail(
    report_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "saved_report_detail_requests")
    entry = get_saved_report(context.organization_id, report_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Saved report not found")

    envelope = fetch_planning_data(limit=500)
    applications = normalize_envelope(envelope)
    applications = filter_applications(applications, area_id=entry.area_id, keyword=entry.keyword)
    applications = [app for app in applications if get_source_kind(app.source_system) == "official"]
    signals = high_priority_signals(applications)
    return {
        "report": entry.model_dump(mode="json"),
        "applications": [{**app.model_dump(mode="json"), "source_kind": "official"} for app in applications[:30]],
        "signals": [signal.model_dump(mode="json") for signal in signals[:20]],
    }


@app.post("/reports/scheduled")
def create_report_schedule(
    payload: ScheduledReportCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "scheduled_report_create_requests")
    require_admin_session_context(context, action="Create scheduled reports")
    entry = create_scheduled_report(context.organization_id, payload)
    return entry.model_dump(mode="json")


@app.get("/reports/scheduled")
def report_schedules(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "scheduled_report_list_requests")
    return [item.model_dump(mode="json") for item in list_scheduled_reports(context.organization_id)]


@app.put("/reports/scheduled/{schedule_id}")
def update_report_schedule(
    schedule_id: str,
    payload: ScheduledReportUpdateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "scheduled_report_update_requests")
    require_admin_session_context(context, action="Update scheduled reports")
    entry = update_scheduled_report_record(context.organization_id, schedule_id, payload)
    if not entry:
        raise HTTPException(status_code=404, detail="Scheduled report not found")
    return entry.model_dump(mode="json")


@app.delete("/reports/scheduled/{schedule_id}")
def delete_report_schedule(
    schedule_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "scheduled_report_delete_requests")
    require_admin_session_context(context, action="Delete scheduled reports")
    deleted = delete_scheduled_report_record(context.organization_id, schedule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Scheduled report not found")
    return {"schedule_id": schedule_id, "deleted": True}


@app.post("/reports/scheduled/{schedule_id}/run")
def run_report_schedule(
    schedule_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "scheduled_report_run_requests")
    require_admin_session_context(context, action="Run scheduled reports")
    schedule = get_scheduled_report(context.organization_id, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled report not found")
    result = run_schedule_now(schedule)
    email = next((item for item in list_email_outbox(context.organization_id) if item.email_id == result["queued_email_id"]), None)
    return {
        "schedule_id": schedule.schedule_id,
        "queued_email": email.model_dump(mode="json") if email else {"email_id": result["queued_email_id"]},
        "application_count": result["applications"],
        "delivery_status": result["delivery_status"],
    }


@app.get("/email/outbox")
def email_outbox(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "email_outbox_requests")
    return [item.model_dump(mode="json") for item in list_email_outbox(context.organization_id)]


@app.post("/webhooks/endpoints")
def create_webhook(
    payload: WebhookEndpointCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "webhook_endpoint_create_requests")
    require_admin_session_context(context, action="Create webhook endpoints")
    return create_webhook_endpoint(context.organization_id, payload).model_dump(mode="json")


@app.get("/webhooks/endpoints")
def webhook_endpoints(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "webhook_endpoint_list_requests")
    return [item.model_dump(mode="json") for item in list_webhook_endpoints(context.organization_id)]


@app.put("/webhooks/endpoints/{webhook_id}")
def update_webhook(
    webhook_id: str,
    payload: WebhookEndpointUpdateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "webhook_endpoint_update_requests")
    require_admin_session_context(context, action="Update webhook endpoints")
    endpoint = update_webhook_endpoint_record(context.organization_id, webhook_id, payload)
    if not endpoint:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    return endpoint.model_dump(mode="json")


@app.delete("/webhooks/endpoints/{webhook_id}")
def delete_webhook(
    webhook_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "webhook_endpoint_delete_requests")
    require_admin_session_context(context, action="Delete webhook endpoints")
    deleted = delete_webhook_endpoint_record(context.organization_id, webhook_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    return {"webhook_id": webhook_id, "deleted": True}


@app.post("/webhooks/endpoints/{webhook_id}/rotate-secret")
def rotate_webhook_secret(
    webhook_id: str,
    payload: WebhookEndpointSecretRotateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "webhook_endpoint_rotate_secret_requests")
    require_admin_session_context(context, action="Rotate webhook secrets")
    endpoint = rotate_webhook_endpoint_secret(
        context.organization_id,
        webhook_id,
        signing_secret=payload.signing_secret,
    )
    if not endpoint:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    return endpoint.model_dump(mode="json")


@app.post("/webhooks/endpoints/{webhook_id}/test")
def test_webhook_endpoint(
    webhook_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "webhook_endpoint_test_requests")
    require_admin_session_context(context, action="Test webhook endpoints")
    endpoint = get_webhook_endpoint(context.organization_id, webhook_id)
    if not endpoint:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    preview = f"PlanSignal webhook test for {endpoint.label}"
    delivery = queue_webhook_delivery(
        context.organization_id,
        target_url=endpoint.target_url,
        event_type="webhook.endpoint.test",
        payload={
            "event": "webhook.endpoint.test",
            "organization_id": context.organization_id,
            "webhook_id": endpoint.webhook_id,
            "label": endpoint.label,
            "preview": preview,
        },
        payload_preview=preview,
        related_webhook_id=endpoint.webhook_id,
    )
    result = dispatch_webhook_delivery(
        delivery_id=delivery.delivery_id,
        organization_id=context.organization_id,
        target_url=endpoint.target_url,
        event_type="webhook.endpoint.test",
        payload={
            "event": "webhook.endpoint.test",
            "organization_id": context.organization_id,
            "webhook_id": endpoint.webhook_id,
            "label": endpoint.label,
            "preview": preview,
        },
        attempts_so_far=delivery.delivery_attempts,
        related_webhook_id=endpoint.webhook_id,
    )
    return {
        "webhook_id": endpoint.webhook_id,
        "delivery_id": delivery.delivery_id,
        "delivery_status": result["delivery_status"],
        "target_url": endpoint.target_url,
        "failure_reason": result["failure_reason"],
        "next_attempt_at": result["next_attempt_at"],
    }


@app.get("/webhooks/deliveries")
def webhook_deliveries(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "webhook_delivery_list_requests")
    return [item.model_dump(mode="json") for item in list_webhook_deliveries(context.organization_id)]


@app.get("/ops/scheduler")
def scheduler_ops(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "scheduler_status_requests")
    return scheduler_status(context.organization_id).model_dump(mode="json")


@app.post("/ops/scheduler/run")
def scheduler_run(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "scheduler_run_requests")
    require_admin_session_context(context, action="Run scheduler")
    due_result = run_due_schedules_once()
    watchlist_result = run_watchlist_webhooks_once()
    webhook_delivery_result = deliver_pending_webhooks_once()
    delivery_result = deliver_pending_outbox_once()
    return {
        "scheduler": scheduler_status(context.organization_id).model_dump(mode="json"),
        "due_result": due_result,
        "watchlist_result": watchlist_result,
        "webhook_delivery_result": webhook_delivery_result,
        "delivery_result": delivery_result,
    }


@app.get("/ops/ingestion/jobs")
def ingestion_jobs(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "ingestion_job_list_requests")
    require_admin_session_context(context, action="List ingestion jobs")
    return [item.model_dump(mode="json") for item in list_ingestion_jobs(context.organization_id)]


@app.post("/ops/ingestion/backfill")
def ingestion_backfill(
    payload: IngestionJobCreateRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "ingestion_backfill_requests")
    require_admin_session_context(context, action="Run ingestion backfills")
    job = create_ingestion_job(context.organization_id, payload)
    completed = _run_ingestion_job(job.job_id, payload)
    return completed.model_dump(mode="json") if completed else job.model_dump(mode="json")


@app.get("/exports/applications.csv")
def export_applications_csv(context: AuthenticatedContext = Depends(require_api_key)) -> StreamingResponse:
    _record_usage(context, "application_export_requests")
    applications = [app for app in normalize_envelope(fetch_planning_data(limit=500)) if get_source_kind(app.source_system) == "official"]
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["application_id", "reference", "authority", "address", "status", "proposal_category", "decision", "submitted_date", "decision_date", "relevance_score", "materiality_score"])
    for app in applications:
        writer.writerow([
            app.application_id,
            app.source_reference,
            app.authority.name,
            app.site.address,
            app.status,
            app.proposal_category,
            app.decision or "",
            app.submitted_date.isoformat() if app.submitted_date else "",
            app.decision_date.isoformat() if app.decision_date else "",
            app.scores.relevance_score,
            app.scores.change_materiality_score,
        ])
    buffer.seek(0)
    return StreamingResponse(iter([buffer.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=plansignal-applications.csv"})


@app.get("/exports/signals.csv")
def export_signals_csv(context: AuthenticatedContext = Depends(require_api_key)) -> StreamingResponse:
    _record_usage(context, "signal_export_requests")
    signals = high_priority_signals([app for app in normalize_envelope(fetch_planning_data(limit=500)) if get_source_kind(app.source_system) == "official"])
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["application_id", "reference", "address", "borough", "priority_reason", "relevance_score", "materiality_score"])
    for signal in signals:
        writer.writerow([
            signal.application_id,
            signal.source_reference,
            signal.address,
            signal.borough or "",
            signal.priority_reason,
            signal.relevance_score,
            signal.change_materiality_score,
        ])
    buffer.seek(0)
    return StreamingResponse(iter([buffer.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=plansignal-signals.csv"})
