from __future__ import annotations

from fastapi import Header, HTTPException

from ..schemas import AuthenticatedContext
from .db import authenticate_api_key, authenticate_session

ADMIN_SESSION_ROLES = {"owner", "admin"}
OWNER_SESSION_ROLES = {"owner"}


def require_api_key(
    x_api_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> AuthenticatedContext:
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        record = authenticate_session(token)
        if record:
            return AuthenticatedContext(
                organization_id=record["organization_id"],
                organization_name=record["name"],
                api_key_prefix="session",
                user_id=record["user_id"],
                user_email=record["email"],
                user_role=record["role"],
                auth_method="session",
            )

    if x_api_key:
        record = authenticate_api_key(x_api_key)
        if record:
            return AuthenticatedContext(
                organization_id=record["organization_id"],
                organization_name=record["name"],
                api_key_prefix=record["key_prefix"],
                auth_method="api_key",
            )

    raise HTTPException(status_code=401, detail="Missing valid X-API-Key or Bearer token")


def require_admin_session_context(context: AuthenticatedContext, *, action: str) -> AuthenticatedContext:
    if context.auth_method != "session":
        raise HTTPException(status_code=403, detail=f"{action} with an owner or admin session")
    if context.user_role not in ADMIN_SESSION_ROLES:
        raise HTTPException(status_code=403, detail=f"{action} requires an owner or admin session")
    return context


def require_owner_session_context(context: AuthenticatedContext, *, action: str) -> AuthenticatedContext:
    if context.auth_method != "session":
        raise HTTPException(status_code=403, detail=f"{action} with an owner session")
    if context.user_role not in OWNER_SESSION_ROLES:
        raise HTTPException(status_code=403, detail=f"{action} requires an owner session")
    return context
