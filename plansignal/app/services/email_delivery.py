from __future__ import annotations

import smtplib
from email.message import EmailMessage

from ..config import SMTP_FROM_EMAIL, SMTP_HOST, SMTP_PASSWORD, SMTP_PORT, SMTP_USERNAME, SMTP_USE_TLS
from ..schemas import EmailOutboxEntry, NormalizedApplication, ScheduledReportEntry
from .query import high_priority_signals


def smtp_enabled() -> bool:
    return bool(SMTP_HOST)


def build_scheduled_report_preview(
    schedule: ScheduledReportEntry,
    applications: list[NormalizedApplication],
) -> str:
    signals = high_priority_signals(applications)
    return (
        f"{len(applications)} applications, {len(signals)} signals "
        f"for area={schedule.area_id or 'all'} keyword={schedule.keyword or 'none'}"
    )


def build_scheduled_report_body(
    schedule: ScheduledReportEntry,
    applications: list[NormalizedApplication],
) -> str:
    signals = high_priority_signals(applications)
    lines = [
        f"PlanSignal scheduled report: {schedule.name}",
        f"Frequency: {schedule.frequency}",
        f"Area: {schedule.area_id or 'all'}",
        f"Keyword: {schedule.keyword or 'none'}",
        "",
        f"Applications loaded: {len(applications)}",
        f"High-priority signals: {len(signals)}",
        "",
        "Top signals:",
    ]
    if not signals:
        lines.append("- No high-priority signals in this run.")
    else:
        for signal in signals[:10]:
            lines.append(
                f"- {signal.source_reference} | {signal.address} | {signal.priority_reason} "
                f"| relevance {signal.relevance_score:.2f} | materiality {signal.change_materiality_score:.2f}"
            )
    return "\n".join(lines)


def send_email(entry: EmailOutboxEntry, body: str) -> tuple[bool, str | None]:
    if not smtp_enabled():
        return False, "SMTP not configured"

    message = EmailMessage()
    message["Subject"] = entry.subject
    message["From"] = SMTP_FROM_EMAIL
    message["To"] = entry.recipient
    message.set_content(body)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as client:
            if SMTP_USE_TLS:
                client.starttls()
            if SMTP_USERNAME:
                client.login(SMTP_USERNAME, SMTP_PASSWORD or "")
            client.send_message(message)
        return True, None
    except Exception as exc:  # pragma: no cover - network/SMTP dependent
        return False, str(exc)
