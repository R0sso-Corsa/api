from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from ..config import (
    DEFAULT_SAMPLE_FILE,
    LIVE_OVERLAY_DEFAULT_LIMIT,
    LIVE_PLANNING_DEFAULT_LIMIT,
    PLANNING_DATA_AUTHORITY_CSV_URL,
    PLANNING_DATA_BASE_URL,
)
from ..schemas import RawApplicationEnvelope


def _load_sample(path: Path = DEFAULT_SAMPLE_FILE) -> RawApplicationEnvelope:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return RawApplicationEnvelope.model_validate(payload)


def fetch_sample_planning_data(path: Path = DEFAULT_SAMPLE_FILE) -> RawApplicationEnvelope:
    return _load_sample(path)


def _merge_envelopes(*envelopes: RawApplicationEnvelope) -> RawApplicationEnvelope:
    records = []
    sources = []
    fetched_at = envelopes[0].fetched_at
    for envelope in envelopes:
        records.extend(envelope.records)
        sources.append(envelope.source)
        if envelope.fetched_at > fetched_at:
            fetched_at = envelope.fetched_at
    return RawApplicationEnvelope(
        source="+".join(sources),
        fetched_at=fetched_at,
        records=records,
        total_available=envelopes[0].total_available,
    )


def _fetch_entity_dataset(dataset: str, *, limit: int) -> tuple[list[dict[str, Any]], int | None]:
    records: list[dict[str, Any]] = []
    total_available: int | None = None
    for offset in range(0, limit, 50):
        page_limit = min(50, limit - offset)
        query = {"dataset": dataset, "limit": page_limit, "offset": offset}
        url = f"{PLANNING_DATA_BASE_URL}?{urlencode(query)}"
        with urlopen(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if total_available is None:
            total_available = payload.get("count")
        page_records = payload.get("results") or payload.get("entities") or []
        if not page_records:
            break
        records.extend(page_records)
    return records, total_available


def _fetch_authority_lookup() -> dict[str, dict[str, str]]:
    records, _ = _fetch_entity_dataset("local-authority", limit=500)
    lookup: dict[str, dict[str, str]] = {}
    for item in records:
        organisation_entity = str(item.get("organisation-entity") or "").strip()
        if not organisation_entity:
            continue
        lookup[organisation_entity] = {
            "authority_name": item.get("name") or "Unknown authority",
            "area_id": item.get("local-planning-authority") or item.get("reference") or organisation_entity,
            "authority_id": str(item.get("entity") or organisation_entity),
        }
    return lookup


def fetch_planning_data(
    *,
    area_id: str | None = None,
    limit: int = LIVE_PLANNING_DEFAULT_LIMIT,
    use_sample_fallback: bool = False,
) -> RawApplicationEnvelope:
    try:
        records, total_available = _fetch_entity_dataset("planning-application", limit=limit)
        authority_lookup = _fetch_authority_lookup()
        enriched_records: list[dict[str, Any]] = []
        for record in records:
            enriched = dict(record)
            organisation_entity = str(record.get("organisation-entity") or "").strip()
            authority = authority_lookup.get(organisation_entity)
            if authority:
                enriched.setdefault("authority_name", authority["authority_name"])
                enriched.setdefault("area_id", authority["area_id"])
                enriched.setdefault("authority_id", authority["authority_id"])
            enriched_records.append(enriched)
        return RawApplicationEnvelope(
            source="planning.data.gov.uk",
            fetched_at=datetime.now(timezone.utc),
            records=enriched_records,
            total_available=total_available,
        )
    except (URLError, TimeoutError, OSError, ValueError):
        if not use_sample_fallback:
            raise
        return _load_sample()


def fetch_overlay_dataset(dataset: str, *, limit: int = LIVE_OVERLAY_DEFAULT_LIMIT) -> dict[str, Any]:
    records, total_available = _fetch_entity_dataset(dataset, limit=limit)
    return {
        "source": "planning.data.gov.uk",
        "dataset": dataset,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "loaded_count": len(records),
        "total_available": total_available,
        "records": records,
    }


def fetch_authorities_live() -> dict[str, Any]:
    csv.field_size_limit(10_000_000)
    with urlopen(PLANNING_DATA_AUTHORITY_CSV_URL, timeout=30) as response:
        decoded = response.read().decode("utf-8").splitlines()
    rows = []
    for row in csv.DictReader(decoded):
        rows.append(
            {
                "entity": row["entity"],
                "name": row["name"],
                "reference": row["reference"],
                "organisation": row["organisation"],
                "region": row["region"],
                "typology": row["typology"],
            }
        )
    rows.sort(key=lambda item: item["name"])
    return {
        "source": "planning.data.gov.uk",
        "count": len(rows),
        "authorities": rows,
    }
