from __future__ import annotations

from typing import Any

from .config import PRIMARY_ICP, STAGE_MAP


def get_icp_profile() -> dict[str, Any]:
    return PRIMARY_ICP


def get_stage_map() -> list[dict[str, Any]]:
    return STAGE_MAP
