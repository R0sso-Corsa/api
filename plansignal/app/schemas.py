from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator


def _clean_text(value: str, *, field_name: str) -> str:
    cleaned = " ".join(value.split()).strip()
    if not cleaned:
        raise ValueError(f"{field_name} is required.")
    return cleaned


def _clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def _clean_list(values: list[str]) -> list[str]:
    cleaned_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = " ".join(value.split()).strip()
        if not cleaned:
            continue
        marker = cleaned.casefold()
        if marker in seen:
            continue
        seen.add(marker)
        cleaned_values.append(cleaned)
    return cleaned_values


def _validate_http_url(value: str, *, field_name: str) -> str:
    cleaned = _clean_text(value, field_name=field_name)
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field_name} must be a valid http:// or https:// URL.")
    return cleaned


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

    @field_validator("area_ids", "statuses", "proposal_categories", "applicant_keywords", "keywords", mode="before")
    @classmethod
    def _default_list(cls, value):
        return value or []

    @field_validator("area_ids", "statuses", "proposal_categories", "applicant_keywords", "keywords")
    @classmethod
    def _clean_lists(cls, values: list[str]) -> list[str]:
        return _clean_list(values)

    @field_validator("changed_since")
    @classmethod
    def _validate_changed_since(cls, value: date | None) -> date | None:
        if value and value > date.today():
            raise ValueError("changed_since cannot be in the future.")
        return value

    def has_any_filter(self) -> bool:
        return bool(
            self.area_ids
            or self.statuses
            or self.proposal_categories
            or self.applicant_keywords
            or self.keywords
            or self.min_relevance_score is not None
            or self.changed_since is not None
        )


class WatchlistCreateRequest(BaseModel):
    name: str
    customer_name: str
    delivery_mode: Literal["dashboard", "email", "webhook", "api"] = "dashboard"
    webhook_endpoint_id: str | None = None
    filters: WatchlistFilters

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _clean_text(value, field_name="Watchlist name")

    @field_validator("customer_name")
    @classmethod
    def _validate_customer_name(cls, value: str) -> str:
        return _clean_text(value, field_name="Customer name")

    @field_validator("webhook_endpoint_id")
    @classmethod
    def _validate_webhook_endpoint_id(cls, value: str | None) -> str | None:
        return _clean_optional_text(value)

    @model_validator(mode="after")
    def _validate_webhook_delivery(self):
        if self.delivery_mode == "webhook" and not self.webhook_endpoint_id:
            raise ValueError("webhook_endpoint_id is required when delivery_mode is webhook.")
        if self.delivery_mode == "webhook" and not self.filters.has_any_filter():
            raise ValueError("At least one watchlist filter is required for webhook alerts.")
        return self


class WatchlistUpdateRequest(WatchlistCreateRequest):
    pass


class Watchlist(BaseModel):
    watchlist_id: str
    name: str
    customer_name: str
    delivery_mode: Literal["dashboard", "email", "webhook", "api"]
    webhook_endpoint_id: str | None = None
    last_webhook_sent_at: datetime | None = None
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
    webhook_secret: str | None = None

    @field_validator("watchlist_id", "email", "webhook_secret")
    @classmethod
    def _clean_optional_fields(cls, value: str | None) -> str | None:
        return _clean_optional_text(value)

    @field_validator("webhook_url")
    @classmethod
    def _validate_webhook_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_http_url(value, field_name="webhook_url")

    @model_validator(mode="after")
    def _validate_target(self):
        if not any((self.watchlist_id, self.email, self.webhook_url)):
            raise ValueError("One alert target is required: watchlist_id, email, or webhook_url.")
        if self.webhook_secret and not self.webhook_url:
            raise ValueError("webhook_secret can only be used with webhook_url.")
        return self


class AlertTestResult(BaseModel):
    status: Literal["queued", "simulated"]
    channel: str
    target: str
    preview: str


class WatchlistWebhookLinkRequest(BaseModel):
    webhook_endpoint_id: str

    @field_validator("webhook_endpoint_id")
    @classmethod
    def _validate_link_id(cls, value: str) -> str:
        return _clean_text(value, field_name="webhook_endpoint_id")


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
    user_role: str | None = None
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
    user_role: str


class UserProfile(BaseModel):
    user_id: str
    organization_id: str
    full_name: str
    email: str
    role: str
    created_at: datetime


class OrganizationUserCreateRequest(BaseModel):
    full_name: str
    email: str
    password: str = Field(min_length=8)
    role: Literal["admin", "member"] = "member"

    @field_validator("full_name")
    @classmethod
    def _validate_full_name(cls, value: str) -> str:
        return _clean_text(value, field_name="Full name")

    @field_validator("email")
    @classmethod
    def _validate_email(cls, value: str) -> str:
        cleaned = _clean_text(value, field_name="email").lower()
        if "@" not in cleaned or "." not in cleaned.split("@")[-1]:
            raise ValueError("email must be a valid email address.")
        return cleaned


class UserRoleUpdateRequest(BaseModel):
    role: Literal["owner", "admin", "member"]


class OrganizationInvitationInfo(BaseModel):
    invitation_id: str
    organization_id: str
    email: str
    role: Literal["admin", "member"]
    status: Literal["pending", "accepted", "revoked", "expired"]
    invited_by_user_id: str | None = None
    invite_token: str
    created_at: datetime
    expires_at: datetime
    accepted_at: datetime | None = None


class OrganizationInvitationCreateRequest(BaseModel):
    email: str
    role: Literal["admin", "member"] = "member"

    @field_validator("email")
    @classmethod
    def _validate_email(cls, value: str) -> str:
        cleaned = _clean_text(value, field_name="email").lower()
        if "@" not in cleaned or "." not in cleaned.split("@")[-1]:
            raise ValueError("email must be a valid email address.")
        return cleaned


class InvitationAcceptRequest(BaseModel):
    invite_token: str
    full_name: str
    password: str = Field(min_length=8)

    @field_validator("invite_token")
    @classmethod
    def _validate_invite_token(cls, value: str) -> str:
        return _clean_text(value, field_name="invite_token")

    @field_validator("full_name")
    @classmethod
    def _validate_full_name(cls, value: str) -> str:
        return _clean_text(value, field_name="Full name")


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

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _clean_text(value, field_name="Report name")

    @field_validator("notes", "area_id", "keyword")
    @classmethod
    def _clean_optional_fields(cls, value: str | None) -> str | None:
        return _clean_optional_text(value)


class SavedReportUpdateRequest(SavedReportCreateRequest):
    pass


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

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _clean_text(value, field_name="Schedule name")

    @field_validator("delivery_email")
    @classmethod
    def _validate_delivery_email(cls, value: str) -> str:
        cleaned = _clean_text(value, field_name="delivery_email").lower()
        if "@" not in cleaned or "." not in cleaned.split("@")[-1]:
            raise ValueError("delivery_email must be a valid email address.")
        return cleaned

    @field_validator("area_id", "keyword")
    @classmethod
    def _clean_optional_fields(cls, value: str | None) -> str | None:
        return _clean_optional_text(value)


class ScheduledReportUpdateRequest(ScheduledReportCreateRequest):
    pass


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
    delivery_status: Literal["queued", "sent", "failed"] = "queued"
    delivery_attempts: int = 0
    last_attempt_at: datetime | None = None
    sent_at: datetime | None = None
    failure_reason: str | None = None
    created_at: datetime


class SchedulerStatus(BaseModel):
    enabled: bool
    smtp_enabled: bool
    poll_seconds: int
    queued_email_count: int
    queued_webhook_count: int
    schedule_count: int
    generated_at: datetime


class WebhookEndpointCreateRequest(BaseModel):
    label: str
    target_url: str
    signing_secret: str | None = None

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        return _clean_text(value, field_name="Webhook label")

    @field_validator("target_url")
    @classmethod
    def _validate_target_url(cls, value: str) -> str:
        return _validate_http_url(value, field_name="target_url")

    @field_validator("signing_secret")
    @classmethod
    def _validate_signing_secret(cls, value: str | None) -> str | None:
        cleaned = _clean_optional_text(value)
        if cleaned and len(cleaned) < 12:
            raise ValueError("signing_secret must be at least 12 characters.")
        return cleaned


class WebhookEndpointUpdateRequest(BaseModel):
    label: str
    target_url: str

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        return _clean_text(value, field_name="Webhook label")

    @field_validator("target_url")
    @classmethod
    def _validate_target_url(cls, value: str) -> str:
        return _validate_http_url(value, field_name="target_url")


class WebhookEndpointEntry(BaseModel):
    webhook_id: str
    organization_id: str
    label: str
    target_url: str
    signing_secret: str
    created_at: datetime


class WebhookEndpointSecretRotateRequest(BaseModel):
    signing_secret: str | None = None

    @field_validator("signing_secret")
    @classmethod
    def _validate_signing_secret(cls, value: str | None) -> str | None:
        cleaned = _clean_optional_text(value)
        if cleaned and len(cleaned) < 12:
            raise ValueError("signing_secret must be at least 12 characters.")
        return cleaned


class WebhookDeliveryEntry(BaseModel):
    delivery_id: str
    organization_id: str
    target_url: str
    event_type: str
    payload_preview: str
    related_webhook_id: str | None = None
    delivery_status: Literal["queued", "sent", "failed"] = "queued"
    delivery_attempts: int = 0
    last_attempt_at: datetime | None = None
    sent_at: datetime | None = None
    next_attempt_at: datetime | None = None
    failure_reason: str | None = None
    created_at: datetime
