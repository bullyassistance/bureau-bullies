"""
Bureau Bullies — Per-Contact Persistent Memory
-----------------------------------------------
Each conversation builds up over time. GHL only stores raw messages, but Bully
AI needs richer memory: a rolling summary, the user's stated pain points,
collections we've discussed, the goal they named, and any facts we promised
to act on.

This module gives every contact their own JSON file on the persistent disk
(/var/data/contact_memory/{contact_id}.json). It survives every redeploy.
Capped at ~200 turns of history per contact (reverse-trim oldest), with a
rolling summary that compresses older context.

Functions:
  - load_memory(contact_id) → dict with 'history', 'summary', 'facts'
  - append_turn(contact_id, role, content) → adds a message to history
  - update_summary(contact_id, summary) → replaces the rolling summary
  - record_fact(contact_id, key, value) → stores a structured fact (e.g. 'top_collection': 'Portfolio Recovery $4200')
  - format_for_prompt(contact_id) → string injectable into Bully AI's system prompt
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("bureau-bullies.contact_memory")


def _memory_dir() -> Path:
    """Pick the persistent /var/data path if available, else /tmp (last resort)."""
    if Path("/var/data").exists() and os.access("/var/data", os.W_OK):
        return Path("/var/data/contact_memory")
    return Path("/tmp/contact_memory")


MEM_DIR = _memory_dir()
MEM_DIR.mkdir(parents=True, exist_ok=True)

# Cap turns per contact so the file stays small
MAX_TURNS = int(os.getenv("BB_MEMORY_MAX_TURNS", "200"))
MAX_FACTS = int(os.getenv("BB_MEMORY_MAX_FACTS", "50"))

# One lock per file path to avoid concurrent-write tearing
_locks: dict[str, threading.Lock] = {}
_locks_global = threading.Lock()


def _lock_for(contact_id: str) -> threading.Lock:
    with _locks_global:
        if contact_id not in _locks:
            _locks[contact_id] = threading.Lock()
        return _locks[contact_id]


def _path_for(contact_id: str) -> Path:
    safe = "".join(c for c in contact_id if c.isalnum() or c in "_-")[:64] or "unknown"
    return MEM_DIR / f"{safe}.json"


def _empty_memory() -> dict:
    return {
        "version": 1,
        "history": [],          # [{role, content, ts}]
        "summary": "",          # rolling summary of the whole conversation
        "facts": {},            # {key: value} — biggest_debt, goal, sentiment, etc
        "last_updated_at": None,
        "purchase_history": [], # [{product, amount, ts}] — for notifications
    }


def load_memory(contact_id: str) -> dict:
    """Load the contact's memory file, or return an empty memory if none exists."""
    if not contact_id:
        return _empty_memory()
    p = _path_for(contact_id)
    if not p.exists():
        return _empty_memory()
    try:
        return json.loads(p.read_text() or "{}") or _empty_memory()
    except Exception as e:
        logger.warning("contact_memory load failed for %s: %s", contact_id, e)
        return _empty_memory()


def _save_memory(contact_id: str, mem: dict) -> None:
    if not contact_id:
        return
    p = _path_for(contact_id)
    mem["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(mem, ensure_ascii=False, indent=2))
        tmp.replace(p)
    except Exception as e:
        logger.warning("contact_memory save failed for %s: %s", contact_id, e)


def append_turn(contact_id: str, role: str, content: str) -> None:
    """Add a turn to the contact's history. Trims oldest if over MAX_TURNS."""
    if not contact_id or not content:
        return
    role = (role or "user").lower()
    if role not in ("user", "assistant", "system"):
        role = "user"
    with _lock_for(contact_id):
        mem = load_memory(contact_id)
        mem["history"].append({
            "role": role,
            "content": str(content)[:4000],
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        if len(mem["history"]) > MAX_TURNS:
            mem["history"] = mem["history"][-MAX_TURNS:]
        _save_memory(contact_id, mem)


def update_summary(contact_id: str, summary: str) -> None:
    """Replace the rolling summary."""
    if not contact_id:
        return
    with _lock_for(contact_id):
        mem = load_memory(contact_id)
        mem["summary"] = (summary or "").strip()[:2000]
        _save_memory(contact_id, mem)


def record_fact(contact_id: str, key: str, value: Any) -> None:
    """Record a structured fact (e.g. 'top_collection': 'Midland $4200')."""
    if not contact_id or not key:
        return
    with _lock_for(contact_id):
        mem = load_memory(contact_id)
        if not isinstance(mem.get("facts"), dict):
            mem["facts"] = {}
        mem["facts"][str(key)[:80]] = str(value)[:500]
        # Trim if too many facts
        if len(mem["facts"]) > MAX_FACTS:
            keys = list(mem["facts"].keys())
            for k in keys[:-MAX_FACTS]:
                mem["facts"].pop(k, None)
        _save_memory(contact_id, mem)


def record_purchase(contact_id: str, product: str, amount: str = "") -> None:
    """Record a purchase event for notification + future reference."""
    if not contact_id:
        return
    with _lock_for(contact_id):
        mem = load_memory(contact_id)
        if not isinstance(mem.get("purchase_history"), list):
            mem["purchase_history"] = []
        mem["purchase_history"].append({
            "product": product,
            "amount": amount,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        _save_memory(contact_id, mem)


def format_for_prompt(contact_id: str, max_history_chars: int = 6000) -> str:
    """Return a string ready to inject into Bully AI's system prompt.
    Shows the rolling summary, key facts, and recent turns (newest last).
    """
    if not contact_id:
        return ""
    mem = load_memory(contact_id)
    parts = []
    if mem.get("summary"):
        parts.append("=== ROLLING SUMMARY OF THIS CONTACT ===\n" + mem["summary"])
    if mem.get("facts"):
        facts_lines = [f"- {k}: {v}" for k, v in mem["facts"].items()]
        if facts_lines:
            parts.append("=== KEY FACTS ABOUT THIS CONTACT ===\n" + "\n".join(facts_lines))
    if mem.get("purchase_history"):
        ph_lines = [f"- {p['ts'][:10]}: {p.get('product','')} {p.get('amount','')}".strip()
                    for p in mem["purchase_history"][-5:]]
        parts.append("=== PAST PURCHASES ===\n" + "\n".join(ph_lines))
    # Recent turns (last ~6000 chars worth)
    if mem.get("history"):
        # Walk history newest-to-oldest collecting until we hit the char budget
        rev = list(reversed(mem["history"]))
        budget = max_history_chars
        chosen = []
        for turn in rev:
            line = f"[{turn.get('ts', '')[:16]}] {turn.get('role','user').upper()}: {turn.get('content','')}"
            if budget - len(line) < 0 and chosen:
                break
            chosen.append(line)
            budget -= len(line)
        chosen.reverse()
        if chosen:
            parts.append("=== RECENT CONVERSATION (chronological) ===\n" + "\n".join(chosen))
    return "\n\n".join(parts)


def history_as_anthropic_messages(contact_id: str, max_turns: int = 30) -> list[dict]:
    """Return the most recent N turns as Anthropic message format
    [{role: 'user'|'assistant', content: '...'}]. Used by chat() to provide
    real conversation continuity instead of just the running summary."""
    mem = load_memory(contact_id)
    hist = mem.get("history") or []
    return [
        {"role": t.get("role", "user"), "content": t.get("content", "")}
        for t in hist[-max_turns:]
        if t.get("role") in ("user", "assistant") and t.get("content")
    ]
