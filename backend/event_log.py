"""Lightweight production event log for Bureau Bullies.

Writes JSONL to /var/data/events.jsonl by default so every important inbound,
outbound, scheduler, and conversion event has an audit trail that survives
Render redeploys. If BB_EVENT_LOG_PATH is set, that path is used instead.

This is intentionally dependency-free. It can be shipped before Postgres/Supabase
is configured, then later ETL'd into a warehouse.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

_LOCK = threading.Lock()


def _default_path() -> Path:
    if Path("/var/data").exists() and os.access("/var/data", os.W_OK):
        return Path("/var/data/events.jsonl")
    return Path("/tmp/bureau_bullies_events.jsonl")


EVENT_LOG_PATH = Path(os.getenv("BB_EVENT_LOG_PATH", str(_default_path())))
MAX_VALUE_CHARS = int(os.getenv("BB_EVENT_LOG_MAX_VALUE_CHARS", "1500"))


def _safe(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        if isinstance(v, str) and len(v) > MAX_VALUE_CHARS:
            return v[:MAX_VALUE_CHARS] + "…"
        return v
    if isinstance(v, dict):
        return {str(k): _safe(val) for k, val in list(v.items())[:80]}
    if isinstance(v, (list, tuple)):
        return [_safe(x) for x in list(v)[:80]]
    return str(v)[:MAX_VALUE_CHARS]


def log_event(event_type: str, **fields: Any) -> None:
    """Append one structured event. Never raises into application flow."""
    try:
        row = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "unix_ms": int(time.time() * 1000),
            "event_type": event_type,
        }
        row.update({k: _safe(v) for k, v in fields.items()})
        EVENT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _LOCK:
            with EVENT_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        return


def read_events(limit: int = 200, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return recent events newest-first."""
    try:
        if not EVENT_LOG_PATH.exists():
            return []
        with EVENT_LOG_PATH.open("r", encoding="utf-8") as f:
            lines = f.readlines()[-max(limit * 3, limit):]
        out: List[Dict[str, Any]] = []
        for line in reversed(lines):
            try:
                row = json.loads(line)
            except Exception:
                continue
            if event_type and row.get("event_type") != event_type:
                continue
            out.append(row)
            if len(out) >= limit:
                break
        return out
    except Exception:
        return []
