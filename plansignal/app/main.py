from __future__ import annotations

import csv
import math
from datetime import datetime
from io import StringIO

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.responses import StreamingResponse

from .blueprint import get_icp_profile, get_stage_map
from .config import TEMPLATES_DIR
from .schemas import (
    AlertTestRequest,
    ApiKeyCreateRequest,
    AuthenticatedContext,
    NaturalLanguageQueryRequest,
    SavedReportCreateRequest,
    ScheduledReportCreateRequest,
    SiteWaitlistRequest,
    SiteScreenRequest,
    UserLoginRequest,
    UserRegistrationRequest,
    WatchlistCreateRequest,
)
from .services.auth import require_api_key
from .services.db import create_api_key as create_api_key_record
from .services.db import create_saved_report
from .services.db import create_scheduled_report
from .services.db import create_site_waitlist_entry
from .services.db import create_watchlist as create_watchlist_record
from .services.db import get_watchlist as get_watchlist_record
from .services.db import get_saved_report
from .services.db import get_scheduled_report
from .services.db import get_user_profile
from .services.db import init_db
from .services.db import list_api_keys
from .services.db import list_email_outbox
from .services.db import list_scheduled_reports
from .services.db import list_saved_reports
from .services.db import list_watchlists as list_watchlist_records
from .services.db import login_user
from .services.db import mark_scheduled_report_ran
from .services.db import queue_email_delivery
from .services.db import record_usage as record_usage_event
from .services.db import register_user
from .services.db import usage_snapshot
from .services.ingestion import fetch_authorities_live, fetch_overlay_dataset, fetch_planning_data
from .services.normalizer import build_area_activity, get_source_kind, normalize_envelope
from .services.query import (
    actor_applications,
    benchmark_boroughs,
    filter_applications,
    high_priority_signals,
    natural_language_query,
    screen_sites,
    watchlist_changes,
)


app = FastAPI(
    title="PlanSignal API",
    version="0.1.0",
    description="England-first planning intelligence API scaffold for consultancies and land buyers.",
)


def _render_template(name: str) -> str:
    return (TEMPLATES_DIR / name).read_text(encoding="utf-8")


def _load_applications(*, area_id: str | None = None, live: bool = True):
    envelope = fetch_planning_data(area_id=area_id)
    return normalize_envelope(envelope)


def _official_context_summary(*, article_4_limit: int = 120, brownfield_limit: int = 120) -> dict:
    article_4 = fetch_overlay_dataset("article-4-direction", limit=article_4_limit)
    brownfield = fetch_overlay_dataset("brownfield-land", limit=brownfield_limit)

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
        if coords:
            if center_lat is not None and center_lon is not None and radius_km is not None:
                if _distance_km(center_lat, center_lon, coords["lat"], coords["lon"]) > radius_km:
                    continue
            if min_lat is not None and coords["lat"] < min_lat:
                continue
            if max_lat is not None and coords["lat"] > max_lat:
                continue
            if min_lon is not None and coords["lon"] < min_lon:
                continue
            if max_lon is not None and coords["lon"] > max_lon:
                continue
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
        if coords:
            if center_lat is not None and center_lon is not None and radius_km is not None:
                if _distance_km(center_lat, center_lon, coords["lat"], coords["lon"]) > radius_km:
                    continue
            if min_lat is not None and coords["lat"] < min_lat:
                continue
            if max_lat is not None and coords["lat"] > max_lat:
                continue
            if min_lon is not None and coords["lon"] < min_lon:
                continue
            if max_lon is not None and coords["lon"] > max_lon:
                continue
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

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "applications": plotted_apps[:150],
        "brownfield": plotted_brownfield[:150],
        "application_count": len(plotted_apps),
        "brownfield_count": len(plotted_brownfield),
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


@app.on_event("startup")
def startup() -> None:
    init_db()


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


@app.get("/me")
def me(context: AuthenticatedContext = Depends(require_api_key)) -> dict:
    _record_usage(context, "me_requests")
    profile = get_user_profile(context.user_id) if context.user_id else None
    return {
        "context": context.model_dump(mode="json"),
        "user": profile.model_dump(mode="json") if profile else None,
    }


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
    if context.auth_method != "session":
        raise HTTPException(status_code=403, detail="Create API keys with a user session")
    return create_api_key_record(context.organization_id, payload.label).model_dump(mode="json")


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


@app.post("/alerts/test")
def test_alert(
    payload: AlertTestRequest,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "alert_test_requests")
    target = payload.email or payload.webhook_url or payload.watchlist_id or "preview"
    channel = "email" if payload.email else "webhook" if payload.webhook_url else "watchlist"
    preview = f"PlanSignal test alert queued for {target}. Delivery mode {channel} ready for pilot workflows."
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
    entry = create_scheduled_report(context.organization_id, payload)
    return entry.model_dump(mode="json")


@app.get("/reports/scheduled")
def report_schedules(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "scheduled_report_list_requests")
    return [item.model_dump(mode="json") for item in list_scheduled_reports(context.organization_id)]


@app.post("/reports/scheduled/{schedule_id}/run")
def run_report_schedule(
    schedule_id: str,
    context: AuthenticatedContext = Depends(require_api_key),
) -> dict:
    _record_usage(context, "scheduled_report_run_requests")
    schedule = get_scheduled_report(context.organization_id, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled report not found")

    envelope = fetch_planning_data(limit=500)
    applications = normalize_envelope(envelope)
    applications = filter_applications(applications, area_id=schedule.area_id, keyword=schedule.keyword)
    applications = [app for app in applications if get_source_kind(app.source_system) == "official"]
    signals = high_priority_signals(applications)
    email = queue_email_delivery(
        context.organization_id,
        recipient=schedule.delivery_email,
        subject=f"PlanSignal scheduled report: {schedule.name}",
        body_preview=f"{len(applications)} applications, {len(signals)} signals for area={schedule.area_id or 'all'} keyword={schedule.keyword or 'none'}",
        related_schedule_id=schedule.schedule_id,
    )
    mark_scheduled_report_ran(context.organization_id, schedule.schedule_id)
    return {
        "schedule_id": schedule.schedule_id,
        "queued_email": email.model_dump(mode="json"),
        "application_count": len(applications),
        "signal_count": len(signals),
    }


@app.get("/email/outbox")
def email_outbox(context: AuthenticatedContext = Depends(require_api_key)) -> list[dict]:
    _record_usage(context, "email_outbox_requests")
    return [item.model_dump(mode="json") for item in list_email_outbox(context.organization_id)]


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
