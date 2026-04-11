from __future__ import annotations

import re
from collections import Counter
from datetime import UTC, date, datetime

from ..schemas import (
    Actor,
    ApplicationScores,
    AreaActivity,
    Authority,
    NormalizedApplication,
    PlanningDocument,
    PlanningEvent,
    RawApplicationEnvelope,
    Site,
)


STATUS_MAP = {
    "received": "received",
    "application received": "received",
    "validated": "validated",
    "pending consideration": "pending",
    "pending": "pending",
    "approved": "approved",
    "granted": "approved",
    "refused": "refused",
    "withdrawn": "withdrawn",
}

DECISION_MAP = {
    "approved": "approved",
    "granted": "approved",
    "refused": "refused",
    "withdrawn": "withdrawn",
}

PROPOSAL_RULES = [
    ("residential", ["dwelling", "residential", "house", "flat", "apartment"]),
    ("student", ["student accommodation", "student"]),
    ("office", ["office", "workspace", "commercial office"]),
    ("industrial", ["warehouse", "industrial", "logistics"]),
    ("retail", ["shop", "retail", "storefront"]),
    ("mixed_use", ["mixed use", "mixed-use"]),
]


def get_source_kind(source_system: str | None) -> str:
    value = (source_system or "").lower()
    if "planning.data.gov.uk" in value:
        return "official"
    return "local"


def _slug(value: str, *, default: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or default


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_status(value: str | None) -> str:
    if not value:
        return "unknown"
    lowered = value.strip().lower()
    return STATUS_MAP.get(lowered, lowered)


def _normalize_decision(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.strip().lower()
    return DECISION_MAP.get(lowered, lowered)


def _categorize_proposal(text: str) -> str:
    lowered = text.lower()
    for category, keywords in PROPOSAL_RULES:
        if any(keyword in lowered for keyword in keywords):
            return category
    return "other"


def _normalize_actor(name: str | None, actor_type: str, reference: str) -> Actor | None:
    if not name:
        return None
    normalized_name = " ".join(name.split()).strip()
    actor_key = _slug(normalized_name, default=f"{actor_type}-{reference.lower()}")
    return Actor(
        actor_id=f"{actor_type}-{actor_key}",
        actor_type=actor_type,  # type: ignore[arg-type]
        name=normalized_name,
        normalized_name=normalized_name.lower(),
        source_name=name,
    )


def _build_documents(reference: str, raw_documents: list[dict]) -> list[PlanningDocument]:
    documents: list[PlanningDocument] = []
    for index, document in enumerate(raw_documents, start=1):
        documents.append(
            PlanningDocument(
                document_id=f"{reference}-doc-{index}",
                title=document.get("title", f"Document {index}"),
                url=document.get("url"),
                published_date=_parse_date(document.get("published_date")),
                document_type=document.get("document_type"),
                summary_status="ready" if document.get("summary") else "not_started",
            )
        )
    return documents


def _build_change_history(reference: str, raw: dict) -> list[PlanningEvent]:
    events: list[PlanningEvent] = [
        PlanningEvent(
            event_id=f"{reference}-received",
            event_type="application_received",
            event_date=_parse_date(raw.get("received_date")),
            title="Application received",
            source_value=raw.get("received_date"),
        )
    ]

    validated_date = _parse_date(raw.get("validated_date"))
    if validated_date:
        events.append(
            PlanningEvent(
                event_id=f"{reference}-validated",
                event_type="validated",
                event_date=validated_date,
                title="Application validated",
                source_value=raw.get("validated_date"),
            )
        )

    current_status = raw.get("application_status")
    previous_status = raw.get("previous_status")
    if current_status:
        events.append(
            PlanningEvent(
                event_id=f"{reference}-status",
                event_type="status_changed",
                event_date=_parse_date(raw.get("status_date")) or _parse_date(raw.get("decision_date")),
                title="Status changed",
                detail=f"{previous_status or 'unknown'} -> {current_status}",
                source_value=current_status,
            )
        )

    decision_date = _parse_date(raw.get("decision_date"))
    if decision_date and raw.get("decision"):
        events.append(
            PlanningEvent(
                event_id=f"{reference}-decision",
                event_type="decision_issued",
                event_date=decision_date,
                title="Decision issued",
                detail=raw.get("decision"),
                source_value=raw.get("decision"),
            )
        )

    return events


def _build_tags(proposal_category: str, raw: dict) -> list[str]:
    tags = {proposal_category, raw.get("priority_tag", "watch")}
    if raw.get("article_4_area"):
        tags.add("article_4")
    if raw.get("green_belt"):
        tags.add("green_belt")
    if raw.get("major_development"):
        tags.add("major_development")
    return sorted(tag for tag in tags if tag)


def _score_application(proposal_category: str, raw: dict, history: list[PlanningEvent]) -> ApplicationScores:
    relevance = 0.55
    commercial = 0.45
    materiality = 0.35
    confidence = 0.8

    if proposal_category in {"residential", "mixed_use", "industrial"}:
        relevance += 0.15
        commercial += 0.2
    if raw.get("major_development"):
        relevance += 0.1
        commercial += 0.15
        materiality += 0.25
    if any(event.event_type == "decision_issued" for event in history):
        materiality += 0.2
    if raw.get("article_4_area") or raw.get("green_belt"):
        materiality += 0.1
    if not raw.get("coordinates"):
        confidence -= 0.2

    return ApplicationScores(
        relevance_score=min(relevance, 1.0),
        commercial_potential_score=min(commercial, 1.0),
        change_materiality_score=min(materiality, 1.0),
        confidence_score=max(min(confidence, 1.0), 0.0),
    )


def _display_address(raw: dict, reference: str) -> str:
    for key in ("address", "site-address", "name"):
        value = (raw.get(key) or "").strip()
        if value:
            return value

    authority_name = (raw.get("authority_name") or "").strip()
    if authority_name:
        return f"Reference {reference} • {authority_name}"

    return f"Reference {reference}"


def normalize_application(raw: dict) -> NormalizedApplication:
    reference = raw["reference"]
    proposal_text = raw.get("proposal") or raw.get("description") or "No proposal text supplied"
    proposal_category = _categorize_proposal(proposal_text)
    history = _build_change_history(reference, raw)
    documents = _build_documents(reference, raw.get("documents", []))
    coordinates = raw.get("coordinates") or {}

    authority = Authority(
        authority_id=raw.get("authority_id", _slug(raw.get("authority_name", "unknown"), default="unknown")),
        name=raw.get("authority_name", "Unknown authority"),
        area_id=raw.get("area_id", "unknown-area"),
    )
    display_address = _display_address(raw, reference)

    site = Site(
        site_id=f"site-{_slug(display_address, default=reference.lower())}",
        address=display_address,
        uprn=raw.get("uprn"),
        latitude=coordinates.get("lat"),
        longitude=coordinates.get("lon"),
        postcode=raw.get("postcode"),
        borough=raw.get("borough"),
        lpa_name=raw.get("authority_name"),
    )

    applicant = _normalize_actor(raw.get("applicant_name"), "applicant", reference)
    agent = _normalize_actor(raw.get("agent_name"), "agent", reference)

    return NormalizedApplication(
        application_id=f"app-{reference.lower()}",
        source_reference=reference,
        source_system=raw.get("source_system", "planning.data.gov.uk"),
        authority=authority,
        site=site,
        applicant=applicant,
        agent=agent,
        proposal_text=proposal_text,
        proposal_category=proposal_category,
        status=_normalize_status(raw.get("application_status")),
        decision=_normalize_decision(raw.get("decision")),
        submitted_date=_parse_date(raw.get("received_date")),
        validated_date=_parse_date(raw.get("validated_date")),
        decision_date=_parse_date(raw.get("decision_date")),
        tags=_build_tags(proposal_category, raw),
        summary=raw.get(
            "summary",
            f"{proposal_category.replace('_', ' ').title()} application in {raw.get('borough', 'target area')} with status {_normalize_status(raw.get('application_status'))}.",
        ),
        documents=documents,
        change_history=history,
        scores=_score_application(proposal_category, raw, history),
        last_seen_at=datetime.now(UTC),
        raw_payload=raw,
    )


def normalize_envelope(envelope: RawApplicationEnvelope) -> list[NormalizedApplication]:
    return [normalize_application(record) for record in envelope.records]


def build_area_activity(area_id: str, applications: list[NormalizedApplication]) -> AreaActivity:
    scoped = [app for app in applications if app.authority.area_id == area_id]
    status_counts = Counter(app.status for app in scoped)
    category_counts = Counter(app.proposal_category for app in scoped)
    latest_change = None

    dated_events = [
        event.event_date
        for app in scoped
        for event in app.change_history
        if event.event_date is not None
    ]
    if dated_events:
        latest_change = max(dated_events)

    area_name = scoped[0].authority.name if scoped else area_id
    return AreaActivity(
        area_id=area_id,
        area_name=area_name,
        application_count=len(scoped),
        status_counts=dict(status_counts),
        proposal_category_counts=dict(category_counts),
        latest_change_at=latest_change,
    )
