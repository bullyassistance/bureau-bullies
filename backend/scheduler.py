"""
Bureau Bullies — Email Drip Scheduler
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("bureau-bullies.scheduler")

DB_PATH = Path(os.getenv("BB_SCHEDULER_DB", "/tmp/bullies_scheduled_emails.json"))
POLL_SECONDS = int(os.getenv("BB_SCHEDULER_POLL_SECONDS", "60"))

CADENCE_SECONDS = [
    2 * 60,
    1 * 86400,
    3 * 86400,
    5 * 86400,
    7 * 86400,
    10 * 86400,
    14 * 86400,
]

_file_lock = threading.Lock()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_db():
    with _file_lock:
        if not DB_PATH.exists():
            return []
        try:
            return json.loads(DB_PATH.read_text() or "[]")
        except Exception as e:
            logger.warning("Scheduler DB corrupt (%s) — starting fresh", e)
            return []


def _save_db(rows):
    with _file_lock:
        DB_PATH.parent.mkdir(exist_ok=True, parents=True)
        tmp = DB_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, indent=2))
        tmp.replace(DB_PATH)


def schedule_email_drip(contact_id, contact_email, first_name, emails, scan_time=None):
    """Enqueue the 7-email drip for a single contact."""
    if not contact_id or not contact_email:
        logger.warning("Cannot schedule drip — missing contact_id or email")
        return 0
    if scan_time is None:
        scan_time = datetime.now(timezone.utc)
    rows = _load_db()
    count = 0
    for i, email in enumerate(emails or []):
        if i >= len(CADENCE_SECONDS):
            break
        send_at = scan_time + timedelta(seconds=CADENCE_SECONDS[i])
        rows.append({
            "id": f"{contact_id}-{i+1}-{int(scan_time.timestamp())}",
            "contact_id": contact_id,
            "contact_email": contact_email,
            "first_name": first_name or "",
            "email_index": i + 1,
            "day": email.get("day", CADENCE_SECONDS[i] // 86400),
            "subject": email.get("subject", ""),
            "body": email.get("body", ""),
            "send_at": send_at.isoformat(),
            "status": "pending",
            "created_at": scan_time.isoformat(),
        })
        count += 1
    _save_db(rows)
    logger.info("Scheduled %d emails for contact %s (%s)", count, contact_id, contact_email)
    return count


def cancel_drip(contact_id, reason="cancelled"):
    rows = _load_db()
    n = 0
    for r in rows:
        if r.get("contact_id") == contact_id and r.get("status") == "pending":
            r["status"] = reason
            r["cancelled_at"] = _now_iso()
            n += 1
    if n:
        _save_db(rows)
        logger.info("Cancelled %d pending emails for %s (%s)", n, contact_id, reason)
    return n


def _due_now(rows):
    now = datetime.now(timezone.utc)
    out = []
    for r in rows:
        if r.get("status") not in ("pending", "failed"):
            continue
        try:
            sa = datetime.fromisoformat(r["send_at"])
            if sa.tzinfo is None:
                sa = sa.replace(tzinfo=timezone.utc)
            if sa <= now:
                out.append(r)
        except Exception:
            continue
    return out


def _dispatch_due():
    from ghl import GHLClient
    rows = _load_db()
    due = _due_now(rows)
    if not due:
        return 0
    try:
        client = GHLClient()
    except Exception as e:
        logger.warning("Scheduler: GHL client init failed, retrying later: %s", e)
        return 0
    sent = 0
    for r in due:
        try:
            ok = client.send_email(
                contact_id=r["contact_id"],
                subject=r["subject"],
                html=_plaintext_to_html(r["body"]),
                plain=r["body"],
            )
            r["status"] = "sent" if ok else "failed"
            r["dispatched_at"] = _now_iso()
            if ok:
                sent += 1
                logger.info("Sent email %d to %s: %r", r["email_index"], r["contact_email"], r["subject"][:60])
            else:
                logger.warning("Email send returned False for %s", r["contact_id"])
        except Exception as e:
            logger.exception("Email dispatch error for %s: %s", r.get("contact_id"), e)
            r["status"] = "failed"
            r["error"] = str(e)[:500]
    _save_db(rows)
    return sent


def _plaintext_to_html(body):
    import re
    esc = (body or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    esc = re.sub(r"(https?://[^\s<]+)", r'<a href="\1" style="color:#e11d2e;text-decoration:underline;">\1</a>', esc)
    paragraphs = ["<p style=\"margin:0 0 12px 0;\">" + p.replace("\n", "<br/>") + "</p>" for p in esc.split("\n\n") if p.strip()]
    return ('<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;font-size:15px;line-height:1.5;color:#111;max-width:580px;">' + "\n".join(paragraphs) + "</div>")


async def _poll_loop():
    logger.info("Email scheduler poll loop started (every %ds)", POLL_SECONDS)
    while True:
        try:
            n = await asyncio.to_thread(_dispatch_due)
            if n:
                logger.info("Scheduler tick: dispatched %d email(s)", n)
        except Exception as e:
            logger.exception("Scheduler tick error: %s", e)
        await asyncio.sleep(POLL_SECONDS)


def start_background_scheduler(app):
    task_ref = {}
    @app.on_event("startup")
    async def _start():
        task_ref["task"] = asyncio.create_task(_poll_loop())
    @app.on_event("shutdown")
    async def _stop():
        t = task_ref.get("task")
        if t:
            t.cancel()
