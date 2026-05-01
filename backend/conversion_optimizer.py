"""Conversion optimizer utilities.

This does not and cannot guarantee a 20% conversion rate. It adds code-level
controls that improve the odds: instant response, deterministic keyword links,
urgency/follow-up SLA, offer routing, and measurement.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

TARGET_DAILY_CONVERSION_RATE = 0.20


def variant_for_contact(contact_id: str, channel: str = "") -> str:
    """Stable A/B assignment by contact/channel."""
    s = f"{contact_id}|{channel}|bureau-bullies-v1".encode()
    return "A" if int(hashlib.sha256(s).hexdigest(), 16) % 2 == 0 else "B"


def lead_heat_score(payload: Dict[str, Any], message: str = "") -> int:
    """0-100 practical sales heat score for routing and dashboards."""
    txt = (message or "") + " " + " ".join(str(v) for v in (payload or {}).values() if isinstance(v, str))
    t = txt.lower()
    score = 15
    hot_terms = ["equifax", "delete", "removed", "lawsuit", "sue", "funding", "mortgage", "car", "denied", "charge off", "collection", "repossession"]
    for term in hot_terms:
        if term in t:
            score += 8
    if any(x in t for x in ["send link", "link", "buy", "price", "$27", "$66", "$17"]):
        score += 15
    if any(x in t for x in ["today", "now", "asap", "urgent"]):
        score += 10
    return max(0, min(score, 100))


def should_alert_owner(score: int) -> bool:
    return score >= 70


def daily_target_note() -> str:
    return (
        "Target: 20% daily conversion is a KPI, not a guarantee. "
        "The system should measure it daily and escalate leads when it falls short."
    )
