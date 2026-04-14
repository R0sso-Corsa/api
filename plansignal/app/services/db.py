from __future__ import annotations

import json
import hashlib
import secrets
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from ..config import (
    DEFAULT_SQLITE_DB,
    DEMO_API_KEY,
    DEMO_ORGANIZATION_NAME,
    DEMO_USER_EMAIL,
    DEMO_USER_PASSWORD,
    STORE_DIR,
)
from ..schemas import (
    ApiKeyCreateResponse,
    ApiKeyInfo,
    EmailOutboxEntry,
    InvitationAcceptRequest,
    OrganizationInvitationCreateRequest,
    OrganizationInvitationInfo,
    OrganizationUserCreateRequest,
    SavedReportCreateRequest,
    SavedReportEntry,
    SavedReportUpdateRequest,
    ScheduledReportCreateRequest,
    ScheduledReportEntry,
    ScheduledReportUpdateRequest,
    SessionTokenResponse,
    SiteWaitlistEntry,
    SiteWaitlistRequest,
    UsageSnapshot,
    UserProfile,
    UserRoleUpdateRequest,
    Watchlist,
    WatchlistCreateRequest,
    WatchlistUpdateRequest,
    WatchlistWebhookLinkRequest,
    WebhookDeliveryEntry,
    WebhookEndpointCreateRequest,
    WebhookEndpointEntry,
    WebhookEndpointUpdateRequest,
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    chosen_salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), chosen_salt.encode("utf-8"), 100_000)
    return chosen_salt, digest.hex()


def _verify_password(password: str, salt: str, expected_hash: str) -> bool:
    _, digest = _hash_password(password, salt)
    return secrets.compare_digest(digest, expected_hash)


def _generate_webhook_signing_secret() -> str:
    return f"pswhsec_{secrets.token_urlsafe(24)}"


def _generate_invite_token() -> str:
    return f"psinvite_{secrets.token_urlsafe(24)}"


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

            CREATE TABLE IF NOT EXISTS organization_invitations (
                invitation_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL,
                status TEXT NOT NULL,
                invited_by_user_id TEXT,
                invite_token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                accepted_at TEXT,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id),
                FOREIGN KEY (invited_by_user_id) REFERENCES users (user_id)
            );

            CREATE TABLE IF NOT EXISTS watchlists (
                watchlist_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                name TEXT NOT NULL,
                customer_name TEXT NOT NULL,
                delivery_mode TEXT NOT NULL,
                webhook_endpoint_id TEXT,
                last_webhook_sent_at TEXT,
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
                delivery_status TEXT NOT NULL DEFAULT 'queued',
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                sent_at TEXT,
                failure_reason TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS webhook_endpoints (
                webhook_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                label TEXT NOT NULL,
                target_url TEXT NOT NULL,
                signing_secret TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );

            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                delivery_id TEXT PRIMARY KEY,
                organization_id TEXT NOT NULL,
                target_url TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_preview TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                related_webhook_id TEXT,
                signing_secret TEXT,
                delivery_status TEXT NOT NULL DEFAULT 'queued',
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                sent_at TEXT,
                next_attempt_at TEXT,
                failure_reason TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (organization_id) REFERENCES organizations (organization_id)
            );
            """
        )
        _ensure_column(cursor, "api_keys", "label", "label TEXT NOT NULL DEFAULT 'Migrated key'")
        _ensure_column(cursor, "watchlists", "webhook_endpoint_id", "webhook_endpoint_id TEXT")
        _ensure_column(cursor, "watchlists", "last_webhook_sent_at", "last_webhook_sent_at TEXT")
        _ensure_column(cursor, "email_outbox", "delivery_status", "delivery_status TEXT NOT NULL DEFAULT 'queued'")
        _ensure_column(cursor, "email_outbox", "delivery_attempts", "delivery_attempts INTEGER NOT NULL DEFAULT 0")
        _ensure_column(cursor, "email_outbox", "last_attempt_at", "last_attempt_at TEXT")
        _ensure_column(cursor, "email_outbox", "sent_at", "sent_at TEXT")
        _ensure_column(cursor, "email_outbox", "failure_reason", "failure_reason TEXT")
        _ensure_column(cursor, "webhook_endpoints", "signing_secret", "signing_secret TEXT NOT NULL DEFAULT ''")
        _ensure_column(cursor, "webhook_deliveries", "payload_json", "payload_json TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(cursor, "webhook_deliveries", "signing_secret", "signing_secret TEXT")
        _ensure_column(cursor, "webhook_deliveries", "next_attempt_at", "next_attempt_at TEXT")
        cursor.execute(
            """
            SELECT webhook_id
            FROM webhook_endpoints
            WHERE signing_secret IS NULL OR signing_secret = ''
            """
        )
        for row in cursor.fetchall():
            cursor.execute(
                """
                UPDATE webhook_endpoints
                SET signing_secret = ?
                WHERE webhook_id = ?
                """,
                (_generate_webhook_signing_secret(), row["webhook_id"]),
            )
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
            SELECT organizations.organization_id, organizations.name, users.user_id, users.email, users.role
            FROM sessions
            JOIN organizations ON organizations.organization_id = sessions.organization_id
            JOIN users ON users.user_id = sessions.user_id
            WHERE sessions.access_token = ?
            """,
            (access_token,),
        )
        return cursor.fetchone()


def _create_session_response(
    cursor: sqlite3.Cursor,
    *,
    organization_id: str,
    organization_name: str,
    user_id: str,
    user_email: str,
    user_role: str,
) -> SessionTokenResponse:
    token = f"psess_{secrets.token_urlsafe(24)}"
    cursor.execute(
        """
        INSERT INTO sessions (session_id, organization_id, user_id, access_token, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (f"session-{uuid4().hex[:10]}", organization_id, user_id, token, _utc_now()),
    )
    return SessionTokenResponse(
        access_token=token,
        organization_id=organization_id,
        organization_name=organization_name,
        user_id=user_id,
        user_email=user_email,
        user_role=user_role,
    )


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
            SELECT users.user_id, users.organization_id, users.email, users.role, users.password_salt, users.password_hash, organizations.name
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

        session = _create_session_response(
            cursor,
            organization_id=record["organization_id"],
            organization_name=record["name"],
            user_id=record["user_id"],
            user_email=record["email"],
            user_role=record["role"],
        )
        connection.commit()
        return session


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


def list_organization_users(organization_id: str) -> list[UserProfile]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT user_id, organization_id, full_name, email, role, created_at
            FROM users
            WHERE organization_id = ?
            ORDER BY created_at ASC
            """,
            (organization_id,),
        )
        return [UserProfile.model_validate(dict(row)) for row in cursor.fetchall()]


def _refresh_invitation_statuses(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        UPDATE organization_invitations
        SET status = 'expired'
        WHERE status = 'pending' AND expires_at < ?
        """,
        (_utc_now(),),
    )


def _build_invitation_info(row: sqlite3.Row | dict) -> OrganizationInvitationInfo:
    return OrganizationInvitationInfo.model_validate(dict(row))


def list_organization_invitations(organization_id: str) -> list[OrganizationInvitationInfo]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        _refresh_invitation_statuses(cursor)
        cursor.execute(
            """
            SELECT invitation_id, organization_id, email, role, status, invited_by_user_id, invite_token, created_at, expires_at, accepted_at
            FROM organization_invitations
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        items = [_build_invitation_info(row) for row in cursor.fetchall()]
        connection.commit()
        return items


def _owner_count(cursor: sqlite3.Cursor, organization_id: str) -> int:
    cursor.execute(
        """
        SELECT COUNT(*) AS owner_count
        FROM users
        WHERE organization_id = ? AND role = 'owner'
        """,
        (organization_id,),
    )
    row = cursor.fetchone()
    return int(row["owner_count"]) if row else 0


def create_organization_invitation(
    organization_id: str,
    payload: OrganizationInvitationCreateRequest,
    *,
    invited_by_user_id: str | None,
) -> OrganizationInvitationInfo:
    invitation_id = f"invite-{uuid4().hex[:10]}"
    now = _utc_now()
    expires_at = (datetime.now(UTC) + timedelta(days=7)).isoformat()
    invite_token = _generate_invite_token()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        _refresh_invitation_statuses(cursor)
        cursor.execute("SELECT user_id FROM users WHERE email = ?", (payload.email.lower(),))
        if cursor.fetchone():
            raise ValueError("A user with that email already exists.")
        cursor.execute(
            """
            SELECT invitation_id
            FROM organization_invitations
            WHERE organization_id = ? AND email = ? AND status = 'pending'
            """,
            (organization_id, payload.email.lower()),
        )
        if cursor.fetchone():
            raise ValueError("A pending invitation for that email already exists.")
        cursor.execute(
            """
            INSERT INTO organization_invitations (
                invitation_id,
                organization_id,
                email,
                role,
                status,
                invited_by_user_id,
                invite_token,
                created_at,
                expires_at,
                accepted_at
            )
            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, NULL)
            """,
            (
                invitation_id,
                organization_id,
                payload.email.lower(),
                payload.role,
                invited_by_user_id,
                invite_token,
                now,
                expires_at,
            ),
        )
        connection.commit()
    return OrganizationInvitationInfo(
        invitation_id=invitation_id,
        organization_id=organization_id,
        email=payload.email.lower(),
        role=payload.role,
        status="pending",
        invited_by_user_id=invited_by_user_id,
        invite_token=invite_token,
        created_at=datetime.fromisoformat(now),
        expires_at=datetime.fromisoformat(expires_at),
        accepted_at=None,
    )


def create_organization_user(organization_id: str, payload: OrganizationUserCreateRequest) -> UserProfile:
    user_id = f"user-{uuid4().hex[:10]}"
    salt, password_hash = _hash_password(payload.password)
    now = _utc_now()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO users (user_id, organization_id, full_name, email, password_salt, password_hash, role, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, organization_id, payload.full_name, payload.email.lower(), salt, password_hash, payload.role, now),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("A user with that email already exists.") from exc
        connection.commit()
    return UserProfile(
        user_id=user_id,
        organization_id=organization_id,
        full_name=payload.full_name,
        email=payload.email.lower(),
        role=payload.role,
        created_at=datetime.fromisoformat(now),
    )


def revoke_organization_invitation(organization_id: str, invitation_id: str) -> OrganizationInvitationInfo | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        _refresh_invitation_statuses(cursor)
        cursor.execute(
            """
            SELECT invitation_id, organization_id, email, role, status, invited_by_user_id, invite_token, created_at, expires_at, accepted_at
            FROM organization_invitations
            WHERE organization_id = ? AND invitation_id = ?
            """,
            (organization_id, invitation_id),
        )
        row = cursor.fetchone()
        if not row:
            return None
        if row["status"] != "pending":
            raise ValueError("Only pending invitations can be revoked.")
        cursor.execute(
            """
            UPDATE organization_invitations
            SET status = 'revoked'
            WHERE organization_id = ? AND invitation_id = ?
            """,
            (organization_id, invitation_id),
        )
        connection.commit()
    updated = dict(row)
    updated["status"] = "revoked"
    return _build_invitation_info(updated)


def update_organization_user_role(
    organization_id: str,
    user_id: str,
    payload: UserRoleUpdateRequest,
    *,
    acting_user_id: str | None,
) -> UserProfile | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT user_id, role
            FROM users
            WHERE organization_id = ? AND user_id = ?
            """,
            (organization_id, user_id),
        )
        target = cursor.fetchone()
        if not target:
            return None
        if acting_user_id and acting_user_id == user_id:
            raise ValueError("You cannot change your own role.")
        if target["role"] == "owner" and payload.role != "owner" and _owner_count(cursor, organization_id) <= 1:
            raise ValueError("At least one owner must remain in the organization.")
        cursor.execute(
            """
            UPDATE users
            SET role = ?
            WHERE organization_id = ? AND user_id = ?
            """,
            (payload.role, organization_id, user_id),
        )
        connection.commit()
    return get_user_profile(user_id)


def delete_organization_user(
    organization_id: str,
    user_id: str,
    *,
    acting_user_id: str | None,
) -> bool:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT user_id, role
            FROM users
            WHERE organization_id = ? AND user_id = ?
            """,
            (organization_id, user_id),
        )
        target = cursor.fetchone()
        if not target:
            return False
        if acting_user_id and acting_user_id == user_id:
            raise ValueError("You cannot delete your own account.")
        if target["role"] == "owner" and _owner_count(cursor, organization_id) <= 1:
            raise ValueError("At least one owner must remain in the organization.")
        cursor.execute(
            """
            DELETE FROM sessions
            WHERE organization_id = ? AND user_id = ?
            """,
            (organization_id, user_id),
        )
        cursor.execute(
            """
            DELETE FROM users
            WHERE organization_id = ? AND user_id = ?
            """,
            (organization_id, user_id),
        )
        deleted = cursor.rowcount > 0
        connection.commit()
    return deleted


def accept_organization_invitation(
    payload: InvitationAcceptRequest,
) -> tuple[OrganizationInvitationInfo, UserProfile, SessionTokenResponse]:
    now = _utc_now()
    user_id = f"user-{uuid4().hex[:10]}"
    salt, password_hash = _hash_password(payload.password)
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        _refresh_invitation_statuses(cursor)
        cursor.execute(
            """
            SELECT
                organization_invitations.invitation_id,
                organization_invitations.organization_id,
                organization_invitations.email,
                organization_invitations.role,
                organization_invitations.status,
                organization_invitations.invited_by_user_id,
                organization_invitations.invite_token,
                organization_invitations.created_at,
                organization_invitations.expires_at,
                organization_invitations.accepted_at,
                organizations.name AS organization_name
            FROM organization_invitations
            JOIN organizations ON organizations.organization_id = organization_invitations.organization_id
            WHERE organization_invitations.invite_token = ?
            """,
            (payload.invite_token,),
        )
        invitation = cursor.fetchone()
        if not invitation or invitation["status"] != "pending":
            raise ValueError("Invitation is invalid or no longer pending.")
        cursor.execute("SELECT user_id FROM users WHERE email = ?", (invitation["email"],))
        if cursor.fetchone():
            raise ValueError("A user with that email already exists.")
        cursor.execute(
            """
            INSERT INTO users (user_id, organization_id, full_name, email, password_salt, password_hash, role, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                invitation["organization_id"],
                payload.full_name,
                invitation["email"],
                salt,
                password_hash,
                invitation["role"],
                now,
            ),
        )
        cursor.execute(
            """
            UPDATE organization_invitations
            SET status = 'accepted', accepted_at = ?
            WHERE invitation_id = ?
            """,
            (now, invitation["invitation_id"]),
        )
        session = _create_session_response(
            cursor,
            organization_id=invitation["organization_id"],
            organization_name=invitation["organization_name"],
            user_id=user_id,
            user_email=invitation["email"],
            user_role=invitation["role"],
        )
        connection.commit()
    user = UserProfile(
        user_id=user_id,
        organization_id=invitation["organization_id"],
        full_name=payload.full_name,
        email=invitation["email"],
        role=invitation["role"],
        created_at=datetime.fromisoformat(now),
    )
    accepted_invitation = _build_invitation_info(
        {
            "invitation_id": invitation["invitation_id"],
            "organization_id": invitation["organization_id"],
            "email": invitation["email"],
            "role": invitation["role"],
            "status": "accepted",
            "invited_by_user_id": invitation["invited_by_user_id"],
            "invite_token": invitation["invite_token"],
            "created_at": invitation["created_at"],
            "expires_at": invitation["expires_at"],
            "accepted_at": now,
        }
    )
    return accepted_invitation, user, session


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
        webhook_endpoint_id=payload.webhook_endpoint_id,
        last_webhook_sent_at=None,
        filters=payload.filters,
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO watchlists (watchlist_id, organization_id, name, customer_name, delivery_mode, webhook_endpoint_id, last_webhook_sent_at, filters_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                watchlist.watchlist_id,
                organization_id,
                watchlist.name,
                watchlist.customer_name,
                watchlist.delivery_mode,
                watchlist.webhook_endpoint_id,
                watchlist.last_webhook_sent_at.isoformat() if watchlist.last_webhook_sent_at else None,
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
            "webhook_endpoint_id": row["webhook_endpoint_id"],
            "last_webhook_sent_at": row["last_webhook_sent_at"],
            "filters": json.loads(row["filters_json"]),
            "created_at": row["created_at"],
        }
    )


def list_watchlists(organization_id: str) -> list[Watchlist]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT watchlist_id, name, customer_name, delivery_mode, webhook_endpoint_id, last_webhook_sent_at, filters_json, created_at
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
            SELECT watchlist_id, name, customer_name, delivery_mode, webhook_endpoint_id, last_webhook_sent_at, filters_json, created_at
            FROM watchlists
            WHERE organization_id = ? AND watchlist_id = ?
            """,
            (organization_id, watchlist_id),
        )
        row = cursor.fetchone()
        return _row_to_watchlist(row) if row else None


def update_watchlist(organization_id: str, watchlist_id: str, payload: WatchlistUpdateRequest) -> Watchlist | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE watchlists
            SET name = ?,
                customer_name = ?,
                delivery_mode = ?,
                webhook_endpoint_id = ?,
                filters_json = ?
            WHERE organization_id = ? AND watchlist_id = ?
            """,
            (
                payload.name,
                payload.customer_name,
                payload.delivery_mode,
                payload.webhook_endpoint_id,
                payload.filters.model_dump_json(),
                organization_id,
                watchlist_id,
            ),
        )
        if cursor.rowcount == 0:
            connection.commit()
            return None
        connection.commit()
    return get_watchlist(organization_id, watchlist_id)


def delete_watchlist(organization_id: str, watchlist_id: str) -> bool:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            DELETE FROM watchlists
            WHERE organization_id = ? AND watchlist_id = ?
            """,
            (organization_id, watchlist_id),
        )
        deleted = cursor.rowcount > 0
        connection.commit()
    return deleted


def list_all_watchlists() -> list[tuple[str, Watchlist]]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT organization_id, watchlist_id, name, customer_name, delivery_mode, webhook_endpoint_id, last_webhook_sent_at, filters_json, created_at
            FROM watchlists
            ORDER BY created_at DESC
            """
        )
        rows = cursor.fetchall()
        return [(row["organization_id"], _row_to_watchlist(row)) for row in rows]


def link_watchlist_webhook(
    organization_id: str,
    watchlist_id: str,
    payload: WatchlistWebhookLinkRequest,
) -> Watchlist | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE watchlists
            SET delivery_mode = 'webhook',
                webhook_endpoint_id = ?
            WHERE organization_id = ? AND watchlist_id = ?
            """,
            (payload.webhook_endpoint_id, organization_id, watchlist_id),
        )
        connection.commit()
    return get_watchlist(organization_id, watchlist_id)


def mark_watchlist_webhook_sent(organization_id: str, watchlist_id: str) -> None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE watchlists
            SET last_webhook_sent_at = ?
            WHERE organization_id = ? AND watchlist_id = ?
            """,
            (datetime.now(UTC).isoformat(), organization_id, watchlist_id),
        )
        connection.commit()


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


def update_saved_report(
    organization_id: str,
    report_id: str,
    payload: SavedReportUpdateRequest,
) -> SavedReportEntry | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE saved_reports
            SET name = ?,
                notes = ?,
                area_id = ?,
                keyword = ?
            WHERE organization_id = ? AND report_id = ?
            """,
            (
                payload.name,
                payload.notes,
                payload.area_id,
                payload.keyword,
                organization_id,
                report_id,
            ),
        )
        if cursor.rowcount == 0:
            connection.commit()
            return None
        connection.commit()
    return get_saved_report(organization_id, report_id)


def delete_saved_report(organization_id: str, report_id: str) -> bool:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            DELETE FROM saved_reports
            WHERE organization_id = ? AND report_id = ?
            """,
            (organization_id, report_id),
        )
        deleted = cursor.rowcount > 0
        connection.commit()
    return deleted


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


def update_scheduled_report(
    organization_id: str,
    schedule_id: str,
    payload: ScheduledReportUpdateRequest,
) -> ScheduledReportEntry | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE scheduled_reports
            SET name = ?,
                delivery_email = ?,
                frequency = ?,
                area_id = ?,
                keyword = ?
            WHERE organization_id = ? AND schedule_id = ?
            """,
            (
                payload.name,
                payload.delivery_email.lower(),
                payload.frequency,
                payload.area_id,
                payload.keyword,
                organization_id,
                schedule_id,
            ),
        )
        if cursor.rowcount == 0:
            connection.commit()
            return None
        connection.commit()
    return get_scheduled_report(organization_id, schedule_id)


def delete_scheduled_report(organization_id: str, schedule_id: str) -> bool:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            DELETE FROM scheduled_reports
            WHERE organization_id = ? AND schedule_id = ?
            """,
            (organization_id, schedule_id),
        )
        deleted = cursor.rowcount > 0
        connection.commit()
    return deleted


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
        delivery_status="queued",
        delivery_attempts=0,
        last_attempt_at=None,
        sent_at=None,
        failure_reason=None,
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO email_outbox (
                email_id, organization_id, recipient, subject, body_preview, related_schedule_id,
                delivery_status, delivery_attempts, last_attempt_at, sent_at, failure_reason, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.email_id,
                entry.organization_id,
                entry.recipient,
                entry.subject,
                entry.body_preview,
                entry.related_schedule_id,
                entry.delivery_status,
                entry.delivery_attempts,
                entry.last_attempt_at.isoformat() if entry.last_attempt_at else None,
                entry.sent_at.isoformat() if entry.sent_at else None,
                entry.failure_reason,
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
            SELECT email_id, organization_id, recipient, subject, body_preview, related_schedule_id,
                   delivery_status, delivery_attempts, last_attempt_at, sent_at, failure_reason, created_at
            FROM email_outbox
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [EmailOutboxEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def list_pending_email_outbox(limit: int = 25) -> list[EmailOutboxEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT email_id, organization_id, recipient, subject, body_preview, related_schedule_id,
                   delivery_status, delivery_attempts, last_attempt_at, sent_at, failure_reason, created_at
            FROM email_outbox
            WHERE delivery_status = 'queued'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        return [EmailOutboxEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def mark_email_delivery_result(
    email_id: str,
    *,
    status: str,
    failure_reason: str | None = None,
) -> None:
    now = datetime.now(UTC).isoformat()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE email_outbox
            SET delivery_status = ?,
                delivery_attempts = delivery_attempts + 1,
                last_attempt_at = ?,
                sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END,
                failure_reason = ?
            WHERE email_id = ?
            """,
            (status, now, status, now, failure_reason, email_id),
        )
        connection.commit()


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


def list_all_scheduled_reports() -> list[ScheduledReportEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT schedule_id, organization_id, name, delivery_email, frequency, area_id, keyword, created_at, last_run_at
            FROM scheduled_reports
            ORDER BY created_at DESC
            """
        )
        return [ScheduledReportEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def create_webhook_endpoint(organization_id: str, payload: WebhookEndpointCreateRequest) -> WebhookEndpointEntry:
    provided_secret = (payload.signing_secret or "").strip()
    entry = WebhookEndpointEntry(
        webhook_id=f"webhook-{uuid4().hex[:10]}",
        organization_id=organization_id,
        label=payload.label,
        target_url=payload.target_url,
        signing_secret=provided_secret or _generate_webhook_signing_secret(),
        created_at=datetime.now(UTC),
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO webhook_endpoints (webhook_id, organization_id, label, target_url, signing_secret, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entry.webhook_id,
                entry.organization_id,
                entry.label,
                entry.target_url,
                entry.signing_secret,
                entry.created_at.isoformat(),
            ),
        )
        connection.commit()
    return entry


def rotate_webhook_endpoint_secret(
    organization_id: str,
    webhook_id: str,
    *,
    signing_secret: str | None = None,
) -> WebhookEndpointEntry | None:
    next_secret = signing_secret or _generate_webhook_signing_secret()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE webhook_endpoints
            SET signing_secret = ?
            WHERE organization_id = ? AND webhook_id = ?
            """,
            (next_secret, organization_id, webhook_id),
        )
        if cursor.rowcount == 0:
            connection.commit()
            return None
        connection.commit()
    return get_webhook_endpoint(organization_id, webhook_id)


def list_webhook_endpoints(organization_id: str) -> list[WebhookEndpointEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT webhook_id, organization_id, label, target_url, signing_secret, created_at
            FROM webhook_endpoints
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [WebhookEndpointEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def get_webhook_endpoint(organization_id: str, webhook_id: str) -> WebhookEndpointEntry | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT webhook_id, organization_id, label, target_url, signing_secret, created_at
            FROM webhook_endpoints
            WHERE organization_id = ? AND webhook_id = ?
            """,
            (organization_id, webhook_id),
        )
        row = cursor.fetchone()
        return WebhookEndpointEntry.model_validate(dict(row)) if row else None


def update_webhook_endpoint(
    organization_id: str,
    webhook_id: str,
    payload: WebhookEndpointUpdateRequest,
) -> WebhookEndpointEntry | None:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE webhook_endpoints
            SET label = ?,
                target_url = ?
            WHERE organization_id = ? AND webhook_id = ?
            """,
            (payload.label, payload.target_url, organization_id, webhook_id),
        )
        if cursor.rowcount == 0:
            connection.commit()
            return None
        cursor.execute(
            """
            UPDATE webhook_deliveries
            SET target_url = ?
            WHERE organization_id = ?
              AND related_webhook_id = ?
              AND delivery_status = 'queued'
            """,
            (payload.target_url, organization_id, webhook_id),
        )
        connection.commit()
    return get_webhook_endpoint(organization_id, webhook_id)


def delete_webhook_endpoint(organization_id: str, webhook_id: str) -> bool:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE watchlists
            SET delivery_mode = 'dashboard',
                webhook_endpoint_id = NULL
            WHERE organization_id = ? AND webhook_endpoint_id = ?
            """,
            (organization_id, webhook_id),
        )
        cursor.execute(
            """
            UPDATE webhook_deliveries
            SET delivery_status = 'failed',
                failure_reason = 'Webhook endpoint deleted.',
                next_attempt_at = NULL
            WHERE organization_id = ?
              AND related_webhook_id = ?
              AND delivery_status = 'queued'
            """,
            (organization_id, webhook_id),
        )
        cursor.execute(
            """
            DELETE FROM webhook_endpoints
            WHERE organization_id = ? AND webhook_id = ?
            """,
            (organization_id, webhook_id),
        )
        deleted = cursor.rowcount > 0
        connection.commit()
    return deleted


def queue_webhook_delivery(
    organization_id: str,
    *,
    target_url: str,
    event_type: str,
    payload: dict,
    payload_preview: str,
    related_webhook_id: str | None = None,
    signing_secret: str | None = None,
) -> WebhookDeliveryEntry:
    now = datetime.now(UTC)
    entry = WebhookDeliveryEntry(
        delivery_id=f"whd-{uuid4().hex[:10]}",
        organization_id=organization_id,
        target_url=target_url,
        event_type=event_type,
        payload_preview=payload_preview,
        related_webhook_id=related_webhook_id,
        next_attempt_at=now,
        delivery_status="queued",
        delivery_attempts=0,
        last_attempt_at=None,
        sent_at=None,
        failure_reason=None,
        created_at=now,
    )
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO webhook_deliveries (
                delivery_id, organization_id, target_url, event_type, payload_preview, payload_json, related_webhook_id,
                signing_secret, delivery_status, delivery_attempts, last_attempt_at, sent_at, next_attempt_at, failure_reason, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.delivery_id,
                entry.organization_id,
                entry.target_url,
                entry.event_type,
                entry.payload_preview,
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
                entry.related_webhook_id,
                signing_secret,
                entry.delivery_status,
                entry.delivery_attempts,
                entry.last_attempt_at.isoformat() if entry.last_attempt_at else None,
                entry.sent_at.isoformat() if entry.sent_at else None,
                entry.next_attempt_at.isoformat() if entry.next_attempt_at else None,
                entry.failure_reason,
                entry.created_at.isoformat(),
            ),
        )
        connection.commit()
    return entry


def list_webhook_deliveries(organization_id: str) -> list[WebhookDeliveryEntry]:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT delivery_id, organization_id, target_url, event_type, payload_preview, related_webhook_id,
                   delivery_status, delivery_attempts, last_attempt_at, sent_at, next_attempt_at, failure_reason, created_at
            FROM webhook_deliveries
            WHERE organization_id = ?
            ORDER BY created_at DESC
            """,
            (organization_id,),
        )
        return [WebhookDeliveryEntry.model_validate(dict(row)) for row in cursor.fetchall()]


def list_pending_webhook_deliveries(limit: int = 25) -> list[dict]:
    now = datetime.now(UTC).isoformat()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT delivery_id, organization_id, target_url, event_type, payload_preview, payload_json, related_webhook_id,
                   signing_secret, delivery_status, delivery_attempts, last_attempt_at, sent_at, next_attempt_at, failure_reason, created_at
            FROM webhook_deliveries
            WHERE delivery_status = 'queued'
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, limit),
        )
        return [dict(row) for row in cursor.fetchall()]


def count_pending_webhook_deliveries(organization_id: str) -> int:
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) AS pending_count
            FROM webhook_deliveries
            WHERE organization_id = ?
              AND delivery_status = 'queued'
            """,
            (organization_id,),
        )
        row = cursor.fetchone()
        return int(row["pending_count"]) if row else 0


def mark_webhook_delivery_result(
    delivery_id: str,
    *,
    status: str,
    failure_reason: str | None = None,
    next_attempt_at: datetime | None = None,
) -> None:
    now = datetime.now(UTC).isoformat()
    with closing(_connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE webhook_deliveries
            SET delivery_status = ?,
                delivery_attempts = delivery_attempts + 1,
                last_attempt_at = ?,
                sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END,
                next_attempt_at = ?,
                failure_reason = ?
            WHERE delivery_id = ?
            """,
            (
                status,
                now,
                status,
                now,
                next_attempt_at.isoformat() if next_attempt_at else None,
                failure_reason,
                delivery_id,
            ),
        )
        connection.commit()
