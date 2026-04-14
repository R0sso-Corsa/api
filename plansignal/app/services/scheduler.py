from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta

from ..config import SCHEDULER_POLL_SECONDS, WEBHOOK_MAX_ATTEMPTS, WEBHOOK_RETRY_BASE_SECONDS
from ..schemas import SchedulerStatus
from .db import (
    count_pending_webhook_deliveries,
    get_webhook_endpoint,
    list_all_scheduled_reports,
    list_all_watchlists,
    list_pending_email_outbox,
    list_pending_webhook_deliveries,
    list_scheduled_reports,
    mark_email_delivery_result,
    mark_scheduled_report_ran,
    mark_watchlist_webhook_sent,
    mark_webhook_delivery_result,
    queue_email_delivery,
    queue_webhook_delivery,
)
from .email_delivery import build_scheduled_report_body, build_scheduled_report_preview, send_email, smtp_enabled
from .ingestion import fetch_planning_data
from .normalizer import get_source_kind, normalize_envelope
from .query import build_watchlist_alert_payload, filter_applications, watchlist_changes
from .webhook_delivery import send_webhook

_scheduler_lock = threading.Lock()
_scheduler_started = False


def _is_due(last_run_at: datetime | None, frequency: str, now: datetime) -> bool:
    if last_run_at is None:
        return True
    interval = {
        "daily": timedelta(days=1),
        "weekly": timedelta(days=7),
        "monthly": timedelta(days=30),
    }[frequency]
    return last_run_at + interval <= now


def _run_schedule(schedule) -> None:
    run_schedule_now(schedule)


def _next_webhook_retry_time(attempts_so_far: int) -> datetime | None:
    next_attempt_number = attempts_so_far + 1
    if next_attempt_number >= WEBHOOK_MAX_ATTEMPTS:
        return None
    delay_seconds = WEBHOOK_RETRY_BASE_SECONDS * (2 ** max(next_attempt_number - 1, 0))
    return datetime.now(UTC) + timedelta(seconds=delay_seconds)


def _resolve_webhook_secret(
    organization_id: str,
    related_webhook_id: str | None,
    signing_secret: str | None,
) -> tuple[str | None, str | None]:
    if related_webhook_id:
        endpoint = get_webhook_endpoint(organization_id, related_webhook_id)
        if not endpoint:
            return None, "Webhook endpoint missing for queued delivery."
        return endpoint.signing_secret, None
    return signing_secret, None


def dispatch_webhook_delivery(
    *,
    delivery_id: str,
    organization_id: str,
    target_url: str,
    event_type: str,
    payload: dict,
    attempts_so_far: int,
    related_webhook_id: str | None = None,
    signing_secret: str | None = None,
) -> dict[str, object]:
    resolved_secret, resolve_error = _resolve_webhook_secret(organization_id, related_webhook_id, signing_secret)
    if resolve_error:
        mark_webhook_delivery_result(
            delivery_id,
            status="failed",
            failure_reason=resolve_error,
            next_attempt_at=None,
        )
        return {
            "delivery_status": "failed",
            "failure_reason": resolve_error,
            "next_attempt_at": None,
        }

    success, failure_reason = send_webhook(
        target_url,
        payload,
        signing_secret=resolved_secret,
        event_type=event_type,
    )
    if success:
        mark_webhook_delivery_result(
            delivery_id,
            status="sent",
            failure_reason=None,
            next_attempt_at=None,
        )
        return {
            "delivery_status": "sent",
            "failure_reason": None,
            "next_attempt_at": None,
        }

    next_attempt_at = _next_webhook_retry_time(attempts_so_far)
    if next_attempt_at is not None:
        mark_webhook_delivery_result(
            delivery_id,
            status="queued",
            failure_reason=failure_reason,
            next_attempt_at=next_attempt_at,
        )
        return {
            "delivery_status": "queued",
            "failure_reason": failure_reason,
            "next_attempt_at": next_attempt_at,
        }

    mark_webhook_delivery_result(
        delivery_id,
        status="failed",
        failure_reason=failure_reason,
        next_attempt_at=None,
    )
    return {
        "delivery_status": "failed",
        "failure_reason": failure_reason,
        "next_attempt_at": None,
    }


def run_schedule_now(schedule) -> dict:
    applications = normalize_envelope(fetch_planning_data(limit=500))
    applications = filter_applications(applications, area_id=schedule.area_id, keyword=schedule.keyword)
    applications = [app for app in applications if get_source_kind(app.source_system) == "official"]
    preview = build_scheduled_report_preview(schedule, applications)
    body = build_scheduled_report_body(schedule, applications)
    email = queue_email_delivery(
        schedule.organization_id,
        recipient=schedule.delivery_email,
        subject=f"PlanSignal scheduled report: {schedule.name}",
        body_preview=preview,
        related_schedule_id=schedule.schedule_id,
    )
    success, failure_reason = send_email(email, body)
    if success:
        mark_email_delivery_result(email.email_id, status="sent", failure_reason=None)
    elif failure_reason != "SMTP not configured":
        mark_email_delivery_result(email.email_id, status="failed", failure_reason=failure_reason)
    mark_scheduled_report_ran(schedule.organization_id, schedule.schedule_id)
    return {
        "schedule_id": schedule.schedule_id,
        "queued_email_id": email.email_id,
        "delivery_status": "sent" if success else "queued" if failure_reason == "SMTP not configured" else "failed",
        "applications": len(applications),
    }


def run_due_schedules_once() -> dict[str, int]:
    now = datetime.now(UTC)
    run_count = 0
    for schedule in list_all_scheduled_reports():
        if _is_due(schedule.last_run_at, schedule.frequency, now):
            _run_schedule(schedule)
            run_count += 1
    return {"run_count": run_count}


def run_watchlist_webhooks_once() -> dict[str, int]:
    sent = 0
    queued = 0
    failed = 0
    skipped = 0
    applications = [app for app in normalize_envelope(fetch_planning_data(limit=500)) if get_source_kind(app.source_system) == "official"]
    for organization_id, watchlist in list_all_watchlists():
        if watchlist.delivery_mode != "webhook" or not watchlist.webhook_endpoint_id:
            continue
        endpoint = get_webhook_endpoint(organization_id, watchlist.webhook_endpoint_id)
        if not endpoint:
            skipped += 1
            continue
        changes = watchlist_changes(watchlist, applications)
        if not changes:
            skipped += 1
            continue
        latest_change = max((change.changed_on for change in changes if change.changed_on is not None), default=None)
        if watchlist.last_webhook_sent_at and latest_change and latest_change <= watchlist.last_webhook_sent_at.date():
            skipped += 1
            continue
        payload = build_watchlist_alert_payload(
            watchlist,
            applications,
            organization_id=organization_id,
        )
        if payload["summary"]["delivered_change_count"] == 0:
            skipped += 1
            continue
        delivery = queue_webhook_delivery(
            organization_id,
            target_url=endpoint.target_url,
            event_type="watchlist.alert",
            payload=payload,
            payload_preview=f"{watchlist.name}: {payload['summary']['delivered_change_count']} delivered of {payload['summary']['matched_change_count']} matched",
            related_webhook_id=endpoint.webhook_id,
        )
        result = dispatch_webhook_delivery(
            delivery_id=delivery.delivery_id,
            organization_id=organization_id,
            target_url=endpoint.target_url,
            event_type="watchlist.alert",
            payload=payload,
            attempts_so_far=delivery.delivery_attempts,
            related_webhook_id=endpoint.webhook_id,
        )
        if result["delivery_status"] == "sent":
            mark_watchlist_webhook_sent(organization_id, watchlist.watchlist_id)
            sent += 1
        elif result["delivery_status"] == "queued":
            queued += 1
        else:
            failed += 1
    return {"sent": sent, "queued": queued, "failed": failed, "skipped": skipped}


def deliver_pending_webhooks_once() -> dict[str, int]:
    sent = 0
    queued = 0
    failed = 0
    for entry in list_pending_webhook_deliveries(limit=25):
        try:
            payload = json.loads(entry["payload_json"])
        except Exception:
            mark_webhook_delivery_result(
                entry["delivery_id"],
                status="failed",
                failure_reason="Stored webhook payload is invalid.",
                next_attempt_at=None,
            )
            failed += 1
            continue
        result = dispatch_webhook_delivery(
            delivery_id=entry["delivery_id"],
            organization_id=entry["organization_id"],
            target_url=entry["target_url"],
            event_type=entry["event_type"],
            payload=payload,
            attempts_so_far=int(entry["delivery_attempts"]),
            related_webhook_id=entry["related_webhook_id"],
            signing_secret=entry["signing_secret"],
        )
        if result["delivery_status"] == "sent":
            sent += 1
        elif result["delivery_status"] == "queued":
            queued += 1
        else:
            failed += 1
    return {"sent": sent, "queued": queued, "failed": failed}


def deliver_pending_outbox_once() -> dict[str, int]:
    delivered = 0
    failed = 0
    for entry in list_pending_email_outbox(limit=25):
        success, failure_reason = send_email(entry, entry.body_preview)
        if success:
            mark_email_delivery_result(entry.email_id, status="sent", failure_reason=None)
            delivered += 1
        elif failure_reason != "SMTP not configured":
            mark_email_delivery_result(entry.email_id, status="failed", failure_reason=failure_reason)
            failed += 1
    return {"delivered": delivered, "failed": failed}


def _scheduler_loop() -> None:
    while True:
        try:
            run_due_schedules_once()
            run_watchlist_webhooks_once()
            deliver_pending_webhooks_once()
            deliver_pending_outbox_once()
        except Exception:
            pass
        time.sleep(SCHEDULER_POLL_SECONDS)


def start_scheduler() -> None:
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        thread = threading.Thread(target=_scheduler_loop, name="plansignal-scheduler", daemon=True)
        thread.start()
        _scheduler_started = True


def scheduler_status(organization_id: str) -> SchedulerStatus:
    return SchedulerStatus(
        enabled=_scheduler_started,
        smtp_enabled=smtp_enabled(),
        poll_seconds=SCHEDULER_POLL_SECONDS,
        queued_email_count=len([item for item in list_pending_email_outbox(limit=500) if item.organization_id == organization_id]),
        queued_webhook_count=count_pending_webhook_deliveries(organization_id),
        schedule_count=len(list_scheduled_reports(organization_id)),
        generated_at=datetime.now(UTC),
    )
