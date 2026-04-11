from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class Authority(BaseModel):
    authority_id: str
    name: str
    area_id: str


class Site(BaseModel):
    site_id: str
    address: str
    uprn: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    postcode: str | None = None
    borough: str | None = None
    lpa_name: str | None = None


class Actor(BaseModel):
    actor_id: str
    actor_type: Literal["applicant", "agent", "developer"]
    name: str
    normalized_name: str
    source_name: str


class PlanningDocument(BaseModel):
    document_id: str
    title: str
    url: str | None = None
    published_date: date | None = None
    document_type: str | None = None
    summary_status: Literal["not_started", "ready"] = "not_started"


class PlanningEvent(BaseModel):
    event_id: str
    event_type: Literal["application_received", "validated", "status_changed", "decision_issued"]
    event_date: date | None = None
    title: str
    detail: str | None = None
    source_value: str | None = None


class ApplicationScores(BaseModel):
    relevance_score: float = Field(ge=0, le=1)
    commercial_potential_score: float = Field(ge=0, le=1)
    change_materiality_score: float = Field(ge=0, le=1)
    confidence_score: float = Field(ge=0, le=1)


class NormalizedApplication(BaseModel):
    application_id: str
    source_reference: str
    source_system: str
    authority: Authority
    site: Site
    applicant: Actor | None = None
    agent: Actor | None = None
    proposal_text: str
    proposal_category: str
    status: str
    decision: str | None = None
    submitted_date: date | None = None
    validated_date: date | None = None
    decision_date: date | None = None
    tags: list[str]
    summary: str
    documents: list[PlanningDocument]
    change_history: list[PlanningEvent]
    scores: ApplicationScores
    last_seen_at: datetime
    raw_payload: dict


class RawApplicationEnvelope(BaseModel):
    source: str
    fetched_at: datetime
    records: list[dict]
    total_available: int | None = None


class AreaActivity(BaseModel):
    area_id: str
    area_name: str
    application_count: int
    status_counts: dict[str, int]
    proposal_category_counts: dict[str, int]
    latest_change_at: date | None = None


class WatchlistFilters(BaseModel):
    area_ids: list[str] = []
    statuses: list[str] = []
    proposal_categories: list[str] = []
    applicant_keywords: list[str] = []
    keywords: list[str] = []
    min_relevance_score: float | None = Field(default=None, ge=0, le=1)
    changed_since: date | None = None


class WatchlistCreateRequest(BaseModel):
    name: str
    customer_name: str
    delivery_mode: Literal["dashboard", "email", "webhook", "api"] = "dashboard"
    filters: WatchlistFilters


class Watchlist(BaseModel):
    watchlist_id: str
    name: str
    customer_name: str
    delivery_mode: Literal["dashboard", "email", "webhook", "api"]
    filters: WatchlistFilters
    created_at: datetime


class WatchlistChange(BaseModel):
    watchlist_id: str
    application_id: str
    source_reference: str
    title: str
    summary: str
    materiality_score: float = Field(ge=0, le=1)
    changed_on: date | None = None
    status: str
    proposal_category: str


class AlertTestRequest(BaseModel):
    watchlist_id: str | None = None
    email: str | None = None
    webhook_url: str | None = None


class AlertTestResult(BaseModel):
    status: Literal["queued", "simulated"]
    channel: str
    target: str
    preview: str


class UsageSnapshot(BaseModel):
    generated_at: datetime
    counters: dict[str, int]


class HighPrioritySignal(BaseModel):
    application_id: str
    source_reference: str
    source_system: str
    source_kind: str
    address: str
    borough: str | None = None
    summary: str
    priority_reason: str
    relevance_score: float = Field(ge=0, le=1)
    change_materiality_score: float = Field(ge=0, le=1)


class SiteScreenRequest(BaseModel):
    sites: list[str]


class SiteScreenResult(BaseModel):
    site_query: str
    matched_application_count: int
    latest_status: str | None = None
    notable_categories: list[str]
    top_matches: list[str]


class BoroughBenchmarkRow(BaseModel):
    area_id: str
    area_name: str
    application_count: int
    approved_count: int
    major_development_count: int
    average_relevance_score: float = Field(ge=0, le=1)


class NaturalLanguageQueryRequest(BaseModel):
    query: str


class NaturalLanguageQueryResult(BaseModel):
    query: str
    interpreted_filters: dict[str, str]
    matching_application_ids: list[str]
    summary: str


class Organization(BaseModel):
    organization_id: str
    name: str
    plan_tier: str
    created_at: datetime


class AuthenticatedContext(BaseModel):
    organization_id: str
    organization_name: str
    api_key_prefix: str
    user_id: str | None = None
    user_email: str | None = None
    auth_method: Literal["api_key", "session"] = "api_key"


class UserRegistrationRequest(BaseModel):
    organization_name: str
    full_name: str
    email: str
    password: str = Field(min_length=8)


class UserLoginRequest(BaseModel):
    email: str
    password: str


class SessionTokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    organization_id: str
    organization_name: str
    user_id: str
    user_email: str


class UserProfile(BaseModel):
    user_id: str
    organization_id: str
    full_name: str
    email: str
    role: str
    created_at: datetime


class ApiKeyInfo(BaseModel):
    key_id: str
    key_prefix: str
    label: str
    created_at: datetime


class ApiKeyCreateRequest(BaseModel):
    label: str


class ApiKeyCreateResponse(BaseModel):
    key_id: str
    label: str
    api_key: str
    key_prefix: str
    created_at: datetime


class SiteWaitlistRequest(BaseModel):
    full_name: str
    company_name: str
    email: str
    use_case: str
    target_geography: str | None = None


class SiteWaitlistEntry(BaseModel):
    waitlist_id: str
    full_name: str
    company_name: str
    email: str
    use_case: str
    target_geography: str | None = None
    created_at: datetime


class SavedReportCreateRequest(BaseModel):
    name: str
    notes: str | None = None
    area_id: str | None = None
    keyword: str | None = None


class SavedReportEntry(BaseModel):
    report_id: str
    organization_id: str
    name: str
    notes: str | None = None
    area_id: str | None = None
    keyword: str | None = None
    application_count: int
    signal_count: int
    created_at: datetime


class ScheduledReportCreateRequest(BaseModel):
    name: str
    delivery_email: str
    frequency: Literal["daily", "weekly", "monthly"] = "weekly"
    area_id: str | None = None
    keyword: str | None = None


class ScheduledReportEntry(BaseModel):
    schedule_id: str
    organization_id: str
    name: str
    delivery_email: str
    frequency: Literal["daily", "weekly", "monthly"]
    area_id: str | None = None
    keyword: str | None = None
    created_at: datetime
    last_run_at: datetime | None = None


class EmailOutboxEntry(BaseModel):
    email_id: str
    organization_id: str
    recipient: str
    subject: str
    body_preview: str
    related_schedule_id: str | None = None
    created_at: datetime
