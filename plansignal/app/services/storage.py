from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ..config import DEFAULT_USAGE_STORE, DEFAULT_WATCHLIST_STORE, STORE_DIR
from ..schemas import UsageSnapshot, Watchlist


def _ensure_store() -> None:
    STORE_DIR.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: object) -> object:
    _ensure_store()
    if not path.exists():
        path.write_text(json.dumps(default, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        path.write_text(json.dumps(default, indent=2), encoding="utf-8")
        return default


def _write_json(path: Path, payload: object) -> None:
    _ensure_store()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def list_watchlists() -> list[Watchlist]:
    payload = _read_json(DEFAULT_WATCHLIST_STORE, [])
    return [Watchlist.model_validate(item) for item in payload]


def save_watchlist(watchlist: Watchlist) -> Watchlist:
    watchlists = list_watchlists()
    watchlists.append(watchlist)
    _write_json(DEFAULT_WATCHLIST_STORE, [item.model_dump(mode="json") for item in watchlists])
    return watchlist


def get_watchlist(watchlist_id: str) -> Watchlist | None:
    for watchlist in list_watchlists():
        if watchlist.watchlist_id == watchlist_id:
            return watchlist
    return None


def record_usage(metric: str, increment: int = 1) -> UsageSnapshot:
    payload = _read_json(
        DEFAULT_USAGE_STORE,
        {"generated_at": datetime.now(UTC).isoformat(), "counters": {}},
    )
    counters = payload.get("counters", {})
    counters[metric] = int(counters.get(metric, 0)) + increment
    snapshot = UsageSnapshot(
        generated_at=datetime.now(UTC),
        counters=counters,
    )
    _write_json(DEFAULT_USAGE_STORE, snapshot.model_dump(mode="json"))
    return snapshot


def get_usage() -> UsageSnapshot:
    payload = _read_json(
        DEFAULT_USAGE_STORE,
        {"generated_at": datetime.now(UTC).isoformat(), "counters": {}},
    )
    return UsageSnapshot.model_validate(payload)
