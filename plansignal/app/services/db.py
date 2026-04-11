from __future__ import annotations

import json
import hashlib
import secrets
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from uuid import uuid4

from ..config import (
    DEFAULT_SQLITE_DB,
    DEMO_API_KEY,
    DEMO_ORGANIZATION_NAME,
    DEMO_USER_EMAIL,
    DEMO_USER_PASSWORD,
    STORE_DIR,
)
from ..schemas import ApiKeyCreateResponse, ApiKeyInfo, EmailOutboxEntry, SavedReportCreateRequest, SavedReportEntry, ScheduledReportCreateRequest, ScheduledReportEntry, SessionTokenResponse, SiteWaitlistEntry, SiteWaitlistRequest, UsageSnapshot, UserProfile, Watchlist, WatchlistCreateRequest


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    chosen_salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), chosen_salt.encode("utf-8"), 100_000)
    return chosen_salt, digest.hex()


def _verify_password(password: str, salt: str, expected_hash: str) -> bool:
    _, digest = _hash_password(password, salt)
    return secrets.compare_digest(digest, expected_hash)


def _connect() -> sqlite3.Connection:
    STORE_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DEFAULT_SQLITE_DB)
    connection.row_factory = sqlite3.Row
    return connection


def _table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _ensure_column(cursor: sqlite3.Cursor, table_name: str, column_name: str, ddl: str) -> None:
    if column_name not in _table_columns(cursor, table_name):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")


def init_db() -> None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS organizations (
                organization_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                plan_tier TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                full_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS api_keys (
                key_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                api_key TEXT NOT NULL UNIQUE,
                key_prefix TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                access_token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            );

            CREATE TABLE IF NOT EXISTS watchlists (
                watchlist_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                name TEXT NOT NULL,
                customer_name TEXT NOT NULL,
                delivery_mode TEXT NOT NULL,
                filters_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS usage_events (
                event_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                increment_value INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS site_waitlist (
                waitlist_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                company_name TEXT NOT NULL,
                email TEXT NOT NULL,
                use_case TEXT NOT NULL,
                target_geography TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS saved_reports (
                report_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                name TEXT NOT NULL,
                notes TEXT,
                area_id TEXT,
                keyword TEXT,
                application_count INTEGER NOT NULL,
                signal_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS scheduled_reports (
                schedule_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                name TEXT NOT NULL,
                delivery_email TEXT NOT NULL,
                frequency TEXT NOT NULL,
                area_id TEXT,
                keyword TEXT,
                created_at TEXT NOT NULL,
                last_run_at TEXT,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS email_outbox (
                email_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                recipient TEXT NOT NULL,
                subject TEXT NOT NULL,
                body_preview TEXT NOT NULL,
                related_schedule_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );
            """
        )
        _ensure_column(cursor, "api_keys", "label", "label TEXT NOT NULL DEFAULT 'Migrated key'")
        connection.commit()

    ensure_demo_org()


def ensure_demo_org() -> None:
    now = _utc_now()
    salt, password_hash = _hash_password(DEMO_USER_PASSWORD, "plansignal-demo-salt")
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO organizations (organization_id, name, plan_tier, created_at)
            VALUES (?, ?, ?, ?)
            """,
            ("org-demo", DEMO_ORGANIZATION_NAME, "pilot", now),
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO users (user_id, organization_id, full_name, email, password_salt, password_hash, role, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("user-demo", "org-demo", "PlanSignal Demo User", DEMO_USER_EMAIL, salt, password_hash, "owner", now),
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO api_keys (key_id, organization_id, api_key, key_prefix, label, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("key-demo", "org-demo", DEMO_API_KEY, DEMO_API_KEY[:12], "Default demo key", now),
        )
        connection.commit()


def authenticate_api_key(api_key: str) -> sqlite3.Row | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT organizations.organization_id, organizations.name, api_keys.key_prefix
            FROM api_keys
            JOIN organizations ON organizations.organization_id = api_keys.organization_id
            WHERE api_keys.api_key = ?
            """,
            (api_key,),
        )
        return cursor.fetchone()


def authenticate_session(access_token: str) -> sqlite3.Row | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT organizations.organization_id, organizations.name, users.user_id, users.email
            FROM sessions
            JOIN organizations ON organizations.organization_id = sessions.organization_id
            JOIN users ON users.user_id = sessions.user_id
            WHERE sessions.access_token = ?
            """,
            (access_token,),
        )
        return cursor.fetchone()


def register_user(organization_name: str, full_name: str, email: str, password: str) -> UserProfile:
    organization_id = f"org-{uuid4().hex[:10]}"
    user_id = f"user-{uuid4().hex[:10]}"
    salt, password_hash = _hash_password(password)
    now = _utc_now()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO organizations (organization_id, name, plan_tier, created_at) VALUES (?, ?, ?, ?)",
            (organization_id, organization_name, "starter", now),
        )
        cursor.execute(
            """
            INSERT INTO users (user_id, organization_id, full_name, email, password_salt, password_hash, role, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, organization_id, full_name, email.lower(), salt, password_hash, "owner", now),
        )
        connection.commit()
    return UserProfile(
        user_id=user_id,
        organization_id=organization_id,
        full_name=full_name,
        email=email.lower(),
        role="owner",
        created_at=datetime.fromisoformat(now),
    )


def login_user(email: str, password: str) -> SessionTokenResponse | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT users.user_id, users.organization_id, users.email, users.password_salt, users.password_hash, organizations.name
            FROM users
            JOIN organizations ON organizations.organization_id = users.organization_id
            WHERE users.email = ?
            """,
            (email.lower(),),
        )
        record = cursor.fetchone()
        if not record:
            return None
        if not _verify_password(password, record["password_salt"], record["password_hash"]):
            return None

        token = f"psess_{secrets.token_urlsafe(24)}"
        cursor.execute(
            """
            INSERT INTO sessions (session_id, organization_id, user_id, access_token, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (f"session-{uuid4().hex[:10]}", record["organization_id"], record["user_id"], token, _utc_now()),
        )
        connection.commit()
        return SessionTokenResponse(
            access_token=token,
            organization_id=record["organization_id"],
            organization_name=record["name"],
            user_id=record["user_id"],
            user_email=record["email"],
        )


def get_user_profile(user_id: str) -> UserProfile | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT user_id, organization_id, full_name, email, role, created_at
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return UserProfile.model_validate(dict(row))


def list_api_keys(organization_id: str) -> list[ApiKeyInfo]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT key_id, key_prefix, label, created_at
            FROM api_keys
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [ApiKeyInfo.model_validate(dict(row)) for row in cursor.fetchall()]


def create_api_key(organization_id: str, label: str) -> ApiKeyCreateResponse:
    api_key = f"psk_{secrets.token_urlsafe(24)}"
    now = _utc_now()
    payload = ApiKeyCreateResponse(
        key_id=f"key-{uuid4().hex[:10]}",
        label=label,
        api_key=api_key,
        key_prefix=api_key[:12],
        created_at=datetime.fromisoformat(now),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO api_keys (key_id, organization_id, api_key, key_prefix, label, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (payload.key_id, organization_id, payload.api_key, payload.key_prefix, payload.label, now),
        )
        connection.commit()
    return payload


def create_watchlist(organization_id: str, payload: WatchlistCreateRequest) -> Watchlist:
    watchlist = Watchlist(
        watchlist_id=f"watch-{uuid4().hex[:10]}",
        name=payload.name,
        customer_name=payload.customer_name,
        delivery_mode=payload.delivery_mode,
        filters=payload.filters,
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO watchlists (watchlist_id, organization_id, name, customer_name, delivery_mode, filters_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                watchlist.watchlist_id,
                organization_id,
                watchlist.name,
                watchlist.customer_name,
                watchlist.delivery_mode,
                watchlist.filters.model_dump_json(),
                watchlist.created_at.isoformat(),
            ),
        )
        connection.commit()
    return watchlist


def _row_to_watchlist(row: sqlite3.Row) -> Watchlist:
    return Watchlist.model_validate(
        {
            "watchlist_id": row["watchlist_id"],
            "name": row["name"],
            "customer_name": row["customer_name"],
            "delivery_mode": row["delivery_mode"],
            "filters": json.loads(row["filters_json"]),
            "created_at": row["created_at"],
        }
    )


def list_watchlists(organization_id: str) -> list[Watchlist]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT watchlist_id, name, customer_name, delivery_mode, filters_json, created_at
            FROM watchlists
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [_row_to_watchlist(row) for row in cursor.fetchall()]


def get_watchlist(organization_id: str, watchlist_id: str) -> Watchlist | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT watchlist_id, name, customer_name, delivery_mode, filters_json, created_at
            FROM watchlists
            WHERE organization_id = ? AND watchlist_id = ?
            """,
            (organization_id, watchlist_id),
        )
        row = cursor.fetchone()
        return _row_to_watchlist(row) if row else None


def record_usage(organization_id: str, metric: str, increment: int = 1) -> None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO usage_events (event_id, organization_id, metric, increment_value, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (f"usage-{uuid4().hex[:12]}", organization_id, metric, increment, datetime.now(UTC).isoformat()),
        )
        connection.commit()


def usage_snapshot(organization_id: str) -> UsageSnapshot:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT metric, SUM(increment_value) AS total
            FROM usage_events
            WHERE organization_id = ?
            GROUP BY metric
            ORDER BY metric
            """,
            (organization_id,),
        )
        counters = {row["metric"]: int(row["total"]) for row in cursor.fetchall()}
    return UsageSnapshot(generated_at=datetime.now(UTC), counters=counters)


def create_site_waitlist_entry(payload: SiteWaitlistRequest) -> SiteWaitlistEntry:
    entry = SiteWaitlistEntry(
        waitlist_id=f"sitewait-{uuid4().hex[:10]}",
        full_name=payload.full_name,
        company_name=payload.company_name,
        email=payload.email.lower(),
        use_case=payload.use_case,
        target_geography=payload.target_geography,
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO site_waitlist (waitlist_id, full_name, company_name, email, use_case, target_geography, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.waitlist_id,
                entry.full_name,
                entry.company_name,
                entry.email,
                entry.use_case,
                entry.target_geography,
                entry.created_at.isoformat(),
            ),
        )
        connection.commit()
    return entry


def create_saved_report(
    organization_id: str,
    payload: SavedReportCreateRequest,
    *,
    application_count: int,
    signal_count: int,
) -> SavedReportEntry:
    entry = SavedReportEntry(
        report_id=f"report-{uuid4().hex[:10]}",
        organization_id=organization_id,
        name=payload.name,
        notes=payload.notes,
        area_id=payload.area_id,
        keyword=payload.keyword,
        application_count=application_count,
        signal_count=signal_count,
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO saved_reports (report_id, organization_id, name, notes, area_id, keyword, application_count, signal_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.report_id,
                entry.organization_id,
                entry.name,
                entry.notes,
                entry.area_id,
                entry.keyword,
                entry.application_count,
                entry.signal_count,
                entry.created_at.isoformat(),
            ),
        )
        connection.commit()
    return entry


def list_saved_reports(organization_id: str) -> list[SavedReportEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT report_id, organization_id, name, notes, area_id, keyword, application_count, signal_count, created_at
            FROM saved_reports
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [SavedReportEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def get_saved_report(organization_id: str, report_id: str) -> SavedReportEntry | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT report_id, organization_id, name, notes, area_id, keyword, application_count, signal_count, created_at
            FROM saved_reports
            WHERE organization_id = ? AND report_id = ?
            """,
            (organization_id, report_id),
        )
        row = cursor.fetchone()
        return SavedReportEntry.model_validate(dict(row)) if row else None


def create_scheduled_report(organization_id: str, payload: ScheduledReportCreateRequest) -> ScheduledReportEntry:
    entry = ScheduledReportEntry(
        schedule_id=f"schedule-{uuid4().hex[:10]}",
        organization_id=organization_id,
        name=payload.name,
        delivery_email=payload.delivery_email.lower(),
        frequency=payload.frequency,
        area_id=payload.area_id,
        keyword=payload.keyword,
        created_at=datetime.now(UTC),
        last_run_at=None,
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO scheduled_reports (schedule_id, organization_id, name, delivery_email, frequency, area_id, keyword, created_at, last_run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.schedule_id,
                entry.organization_id,
                entry.name,
                entry.delivery_email,
                entry.frequency,
                entry.area_id,
                entry.keyword,
                entry.created_at.isoformat(),
                None,
            ),
        )
        connection.commit()
    return entry


def list_scheduled_reports(organization_id: str) -> list[ScheduledReportEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT schedule_id, organization_id, name, delivery_email, frequency, area_id, keyword, created_at, last_run_at
            FROM scheduled_reports
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [ScheduledReportEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def get_scheduled_report(organization_id: str, schedule_id: str) -> ScheduledReportEntry | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT schedule_id, organization_id, name, delivery_email, frequency, area_id, keyword, created_at, last_run_at
            FROM scheduled_reports
            WHERE organization_id = ? AND schedule_id = ?
            """,
            (organization_id, schedule_id),
        )
        row = cursor.fetchone()
        return ScheduledReportEntry.model_validate(dict(row)) if row else None


def queue_email_delivery(
    organization_id: str,
    *,
    recipient: str,
    subject: str,
    body_preview: str,
    related_schedule_id: str | None = None,
) -> EmailOutboxEntry:
    entry = EmailOutboxEntry(
        email_id=f"email-{uuid4().hex[:10]}",
        organization_id=organization_id,
        recipient=recipient.lower(),
        subject=subject,
        body_preview=body_preview,
        related_schedule_id=related_schedule_id,
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO email_outbox (email_id, organization_id, recipient, subject, body_preview, related_schedule_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.email_id,
                entry.organization_id,
                entry.recipient,
                entry.subject,
                entry.body_preview,
                entry.related_schedule_id,
                entry.created_at.isoformat(),
            ),
        )
        connection.commit()
    return entry


def list_email_outbox(organization_id: str) -> list[EmailOutboxEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT email_id, organization_id, recipient, subject, body_preview, related_schedule_id, created_at
            FROM email_outbox
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [EmailOutboxEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def mark_scheduled_report_ran(organization_id: str, schedule_id: str) -> None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE scheduled_reports
            SET last_run_at = ?
            WHERE organization_id = ? AND schedule_id = ?
            """,
            (datetime.now(UTC).isoformat(), organization_id, schedule_id),
        )
        connection.commit()
