from __future__ import annotations

from collections import defaultdict
from datetime import date
from statistics import mean

from ..schemas import (
    BoroughBenchmarkRow,
    HighPrioritySignal,
    NaturalLanguageQueryRequest,
    NaturalLanguageQueryResult,
    NormalizedApplication,
    SiteScreenResult,
    Watchlist,
    WatchlistChange,
)
from .normalizer import get_source_kind


def filter_applications(
    applications: list[NormalizedApplication],
    *,
    area_id: str | None = None,
    status: str | None = None,
    proposal_category: str | None = None,
    applicant: str | None = None,
    keyword: str | None = None,
    changed_since: date | None = None,
) -> list[NormalizedApplication]:
    filtered = applications

    if area_id:
        needle = area_id.lower()
        filtered = [
            app
            for app in filtered
            if app.authority.area_id.lower() == needle
            or app.authority.authority_id.lower() == needle
            or app.authority.name.lower() == needle
            or (app.site.borough and app.site.borough.lower() == needle)
        ]
    if status:
        filtered = [app for app in filtered if app.status == status.lower()]
    if proposal_category:
        filtered = [app for app in filtered if app.proposal_category == proposal_category.lower()]
    if applicant:
        needle = applicant.lower()
        filtered = [
            app
            for app in filtered
            if app.applicant and needle in app.applicant.normalized_name
        ]
    if keyword:
        needle = keyword.lower()
        filtered = [
            app
            for app in filtered
            if needle in app.proposal_text.lower() or needle in app.summary.lower()
        ]
    if changed_since:
        filtered = [
            app
            for app in filtered
            if any(event.event_date and event.event_date >= changed_since for event in app.change_history)
        ]

    return filtered


def watchlist_changes(
    watchlist: Watchlist,
    applications: list[NormalizedApplication],
) -> list[WatchlistChange]:
    filters = watchlist.filters
    filtered = applications

    if filters.area_ids:
        filtered = [app for app in filtered if app.authority.area_id in filters.area_ids]
    if filters.statuses:
        allowed = {value.lower() for value in filters.statuses}
        filtered = [app for app in filtered if app.status in allowed]
    if filters.proposal_categories:
        allowed = {value.lower() for value in filters.proposal_categories}
        filtered = [app for app in filtered if app.proposal_category in allowed]
    if filters.applicant_keywords:
        needles = [value.lower() for value in filters.applicant_keywords]
        filtered = [
            app
            for app in filtered
            if app.applicant and any(needle in app.applicant.normalized_name for needle in needles)
        ]
    if filters.keywords:
        needles = [value.lower() for value in filters.keywords]
        filtered = [
            app
            for app in filtered
            if any(
                needle in app.proposal_text.lower() or needle in app.summary.lower()
                for needle in needles
            )
        ]
    if filters.min_relevance_score is not None:
        filtered = [
            app for app in filtered if app.scores.relevance_score >= filters.min_relevance_score
        ]
    if filters.changed_since:
        filtered = [
            app
            for app in filtered
            if any(event.event_date and event.event_date >= filters.changed_since for event in app.change_history)
        ]

    changes = []
    for app in filtered:
        latest_change = max(
            (event.event_date for event in app.change_history if event.event_date is not None),
            default=None,
        )
        changes.append(
            WatchlistChange(
                watchlist_id=watchlist.watchlist_id,
                application_id=app.application_id,
                source_reference=app.source_reference,
                title=f"{app.proposal_category.replace('_', ' ').title()} in {app.site.borough or 'target area'}",
                summary=app.summary,
                materiality_score=app.scores.change_materiality_score,
                changed_on=latest_change,
                status=app.status,
                proposal_category=app.proposal_category,
            )
        )

    return sorted(changes, key=lambda item: item.materiality_score, reverse=True)


def decision_ready_watchlist_changes(
    watchlist: Watchlist,
    applications: list[NormalizedApplication],
    *,
    max_changes: int = 5,
) -> list[WatchlistChange]:
    changes = watchlist_changes(watchlist, applications)

    filtered = [
        change
        for change in changes
        if change.status != "unknown"
        and change.changed_on is not None
        and change.materiality_score >= 0.45
    ]

    if not filtered:
        filtered = [
            change
            for change in changes
            if change.status != "unknown" and change.materiality_score >= 0.45
        ]

    filtered.sort(
        key=lambda item: (
            item.changed_on or date.min,
            item.materiality_score,
        ),
        reverse=True,
    )
    return filtered[:max_changes]


def high_priority_signals(applications: list[NormalizedApplication]) -> list[HighPrioritySignal]:
    signals: list[HighPrioritySignal] = []
    for app in applications:
        if get_source_kind(app.source_system) != "official":
            continue
        if (
            app.scores.relevance_score >= 0.65
            or "high_priority_site_match" in app.tags
            or "major_development" in app.tags
            or app.status == "approved"
        ):
            reason = []
            if "high_priority_site_match" in app.tags:
                reason.append("watchlist site match")
            if "major_development" in app.tags:
                reason.append("major development")
            if app.status in {"approved", "validated"}:
                reason.append(f"status {app.status}")
            if app.scores.relevance_score >= 0.65:
                reason.append("strong relevance score")
            signals.append(
                HighPrioritySignal(
                    application_id=app.application_id,
                    source_reference=app.source_reference,
                    source_system=app.source_system,
                    source_kind=get_source_kind(app.source_system),
                    address=app.site.address,
                    borough=app.site.borough,
                    summary=app.summary,
                    priority_reason=", ".join(reason) or "high score",
                    relevance_score=app.scores.relevance_score,
                    change_materiality_score=app.scores.change_materiality_score,
                )
            )
    return sorted(
        signals,
        key=lambda item: (item.relevance_score, item.change_materiality_score),
        reverse=True,
    )


def build_watchlist_alert_payload(
    watchlist: Watchlist,
    applications: list[NormalizedApplication],
    *,
    organization_id: str,
    max_changes: int = 5,
    max_signals: int = 3,
) -> dict:
    all_changes = watchlist_changes(watchlist, applications)
    top_changes = decision_ready_watchlist_changes(
        watchlist,
        applications,
        max_changes=max_changes,
    )
    delivered_ids = {change.application_id for change in top_changes}
    delivered_apps = [
        app
        for app in applications
        if app.application_id in delivered_ids and app.status != "unknown"
    ]
    top_signal_items = high_priority_signals(delivered_apps)[:max_signals]

    status_counts: dict[str, int] = defaultdict(int)
    category_counts: dict[str, int] = defaultdict(int)
    for change in top_changes:
        status_counts[change.status] += 1
        category_counts[change.proposal_category] += 1

    return {
        "event": "watchlist.alert",
        "organization_id": organization_id,
        "watchlist": {
            "watchlist_id": watchlist.watchlist_id,
            "name": watchlist.name,
            "customer_name": watchlist.customer_name,
        },
        "summary": {
            "matched_change_count": len(all_changes),
            "delivered_change_count": len(top_changes),
            "signal_count": len(top_signal_items),
            "top_statuses": dict(sorted(status_counts.items(), key=lambda item: item[1], reverse=True)[:5]),
            "top_categories": dict(sorted(category_counts.items(), key=lambda item: item[1], reverse=True)[:5]),
            "note": "No decision-ready changes met delivery threshold." if not top_changes else None,
        },
        "signals": [signal.model_dump(mode="json") for signal in top_signal_items],
        "changes": [change.model_dump(mode="json") for change in top_changes],
    }


def actor_applications(actor_id: str, applications: list[NormalizedApplication]) -> list[NormalizedApplication]:
    return [
        app
        for app in applications
        if (app.applicant and app.applicant.actor_id == actor_id)
        or (app.agent and app.agent.actor_id == actor_id)
    ]


def screen_sites(queries: list[str], applications: list[NormalizedApplication]) -> list[SiteScreenResult]:
    results: list[SiteScreenResult] = []
    for query in queries:
        needle = query.lower()
        matches = [
            app
            for app in applications
            if needle in app.site.address.lower()
            or (app.site.postcode and needle in app.site.postcode.lower())
            or (app.site.uprn and needle in app.site.uprn.lower())
            or (app.site.borough and needle in app.site.borough.lower())
        ]
        latest_status = matches[0].status if matches else None
        notable_categories = sorted({app.proposal_category for app in matches})
        top_matches = [app.application_id for app in matches[:3]]
        results.append(
            SiteScreenResult(
                site_query=query,
                matched_application_count=len(matches),
                latest_status=latest_status,
                notable_categories=notable_categories,
                top_matches=top_matches,
            )
        )
    return results


def benchmark_boroughs(applications: list[NormalizedApplication]) -> list[BoroughBenchmarkRow]:
    grouped: dict[str, list[NormalizedApplication]] = defaultdict(list)
    for app in applications:
        if get_source_kind(app.source_system) != "official":
            continue
        grouped[app.authority.area_id].append(app)

    rows = []
    for area_id, scoped in grouped.items():
        rows.append(
            BoroughBenchmarkRow(
                area_id=area_id,
                area_name=scoped[0].authority.name,
                application_count=len(scoped),
                approved_count=sum(1 for app in scoped if app.status == "approved"),
                major_development_count=sum(1 for app in scoped if "major_development" in app.tags),
                average_relevance_score=mean(app.scores.relevance_score for app in scoped),
            )
        )
    return sorted(rows, key=lambda row: row.average_relevance_score, reverse=True)


def natural_language_query(
    request: NaturalLanguageQueryRequest,
    applications: list[NormalizedApplication],
) -> NaturalLanguageQueryResult:
    query = request.query.lower()
    interpreted_filters: dict[str, str] = {}
    filtered = applications

    if "camden" in query:
        interpreted_filters["area_id"] = "camden-cluster"
        filtered = [app for app in filtered if app.authority.area_id == "camden-cluster"]
    if "southwark" in query:
        interpreted_filters["area_id"] = "southwark-cluster"
        filtered = [app for app in filtered if app.authority.area_id == "southwark-cluster"]
    if "hackney" in query:
        interpreted_filters["area_id"] = "hackney-cluster"
        filtered = [app for app in filtered if app.authority.area_id == "hackney-cluster"]
    if "approved" in query:
        interpreted_filters["status"] = "approved"
        filtered = [app for app in filtered if app.status == "approved"]
    if "residential" in query:
        interpreted_filters["proposal_category"] = "residential"
        filtered = [app for app in filtered if app.proposal_category == "residential"]
    if "office" in query or "workspace" in query:
        interpreted_filters["proposal_category"] = "office"
        filtered = [app for app in filtered if app.proposal_category == "office"]
    if "high priority" in query or "major" in query:
        interpreted_filters["priority"] = "high"
        filtered = [app for app in filtered if app.scores.relevance_score >= 0.7]

    matching_ids = [app.application_id for app in filtered]
    summary = f"Matched {len(filtered)} applications for interpreted filters: {interpreted_filters or {'mode': 'broad search'}}."
    return NaturalLanguageQueryResult(
        query=request.query,
        interpreted_filters=interpreted_filters,
        matching_application_ids=matching_ids,
        summary=summary,
    )
