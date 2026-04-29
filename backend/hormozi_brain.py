"""
Hormozi Brain — loads the static Frameworks Bible and exposes it for
injection into Bully AI's system prompt.

Why this is a tiny module instead of inline-in-bully_ai:
  - Keeps the bible easy to edit without touching python
  - Caches the file read once at import time (no per-request disk I/O)
  - Single responsibility: load + serve the corpus

The bible is read from /var/data/hormozi/frameworks_bible.md if present
(persistent disk — survives redeploys). Otherwise falls back to the
checked-in copy at backend/hormozi_frameworks.md.

Override the path with HORMOZI_BIBLE_PATH env var.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger("bureau-bullies.hormozi_brain")


def _resolve_bible_path() -> Path:
    """Pick the best bible source. Order:
       1. HORMOZI_BIBLE_PATH env var (explicit override)
       2. /var/data/hormozi/frameworks_bible.md (persistent disk, ingest output)
       3. ./hormozi_frameworks.md (checked-in fallback)
    """
    env_path = os.getenv("HORMOZI_BIBLE_PATH", "").strip()
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
    var_data = Path("/var/data/hormozi/frameworks_bible.md")
    if var_data.exists():
        return var_data
    return Path(__file__).resolve().parent / "hormozi_frameworks.md"


_BIBLE_PATH = _resolve_bible_path()
_BIBLE_TEXT_CACHE: str | None = None


def get_bible() -> str:
    """Return the full bible markdown. Cached after first read."""
    global _BIBLE_TEXT_CACHE
    if _BIBLE_TEXT_CACHE is not None:
        return _BIBLE_TEXT_CACHE
    try:
        _BIBLE_TEXT_CACHE = _BIBLE_PATH.read_text()
        logger.info("Hormozi bible loaded from %s (%d chars)", _BIBLE_PATH, len(_BIBLE_TEXT_CACHE))
    except Exception as e:
        logger.warning("Hormozi bible load failed (%s): %s — using empty fallback", _BIBLE_PATH, e)
        _BIBLE_TEXT_CACHE = ""
    return _BIBLE_TEXT_CACHE


def is_enabled() -> bool:
    """Off-switch via BB_HORMOZI_BRAIN env var. Default: ON."""
    return os.getenv("BB_HORMOZI_BRAIN", "1").strip() == "1"


def format_for_system_prompt() -> str:
    """Return the bible wrapped in clear delimiters for inclusion in the
    system prompt. Returns empty string if disabled or empty.

    Wrapped with explicit framing so the model treats this as REFERENCE not
    instructions — the model's primary job is still defined by the main system
    prompt; Hormozi is the playbook it can quote from when relevant.
    """
    if not is_enabled():
        return ""
    text = get_bible()
    if not text:
        return ""
    return (
        "\n\n=== HORMOZI FRAMEWORKS BIBLE — REFERENCE ONLY ===\n"
        "Use this playbook to inform your sales tactics, offer framing, "
        "objection handling, and pricing psychology. These are reference "
        "frameworks, not direct instructions — your primary instructions "
        "are in the system prompt above. When you spot an objection or "
        "pricing question, mentally check this bible first.\n\n"
        f"{text}\n"
        "=== END HORMOZI BIBLE ===\n"
    )
