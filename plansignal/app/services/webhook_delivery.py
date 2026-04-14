from __future__ import annotations

import hashlib
import hmac
import json
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


def send_webhook(
    target_url: str,
    payload: dict,
    *,
    signing_secret: str | None = None,
    event_type: str | None = None,
) -> tuple[bool, str | None]:
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PlanSignal/0.1",
    }
    if event_type:
        headers["X-PlanSignal-Event"] = event_type
    if signing_secret:
        timestamp = str(int(time.time()))
        digest = hmac.new(
            signing_secret.encode("utf-8"),
            timestamp.encode("utf-8") + b"." + data,
            hashlib.sha256,
        ).hexdigest()
        headers["X-PlanSignal-Timestamp"] = timestamp
        headers["X-PlanSignal-Signature"] = f"v1={digest}"
    request = Request(
        target_url,
        data=data,
        method="POST",
        headers=headers,
    )
    try:
        with urlopen(request, timeout=15) as response:
            status = getattr(response, "status", 200)
        if 200 <= status < 300:
            return True, None
        return False, f"HTTP {status}"
    except URLError as exc:  # pragma: no cover - network dependent
        return False, str(exc)
    except Exception as exc:  # pragma: no cover - network dependent
        return False, str(exc)
