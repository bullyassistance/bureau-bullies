"""
Bureau Bullies — Email Drip Scheduler
--------------------------------------
Simple persistent job queue that dispatches scheduled emails to GHL contacts
without requiring a GHL workflow.

Why this exists:
  - The GHL workflow editor doesn't always load in embedded browser contexts,
    so we can't guarantee a per-scan email drip via pure GHL workflow actions.
  - Instead, the backend stores each scheduled email in a JSON file and a
    background task polls it every 60 seconds, sending what's due.

Flow:
  1. On /api/scan, backend pre-generates 7 tailored emails (already shipping).
  2. It also calls `schedule_email_drip(contact_id, contact_email, emails)` —
     this enqueues 7 rows into scheduled_emails.json with send-at timestamps
     based on the SEQUENCE cadence (0, 1, 3, 5, 7, 10, 14 days).
  3. On FastAPI startup, a background task is started that polls the queue
     every POLL_SECONDS and dispatches any emails whose send_at < now.
  4. After an email sends, it's marked status=sent (kept for audit) so it
     never double-fires.

Exit conditions honored:
  - Contact tag includes "purchased-toolkit" or "unsubscribed" → cancel future
  - Reply received → cancel future (flipped via /webhooks/ghl/sms-reply)

Storage: /tmp/bullies_scheduled_emails.json by default. Override with
  BB_SCHEDULER_DB env var.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("bureau-bullies.scheduler")

def _default_db_path() -> str:
    """Pick a persistent path if /var/data exists (Render disk mount), else /tmp.
    /tmp gets wiped on every redeploy, so /var/data is strongly preferred."""
    if Path("/var/data").exists() and os.access("/var/data", os.W_OK):
        return "/var/data/bullies_scheduled_emails.json"
    return "/tmp/bullies_scheduled_emails.json"


DB_PATH = Path(os.getenv("BB_SCHEDULER_DB", _default_db_path()))
POLL_SECONDS = int(os.getenv("BB_SCHEDULER_POLL_SECONDS", "60"))

# Cadence in seconds from scan time for each of the 7 emails.
# Day 0 is 2 minutes to give GHL time to create the contact first.
CADENCE_SECONDS = [
    2 * 60,                  # Email 1 — 2 minutes
    1 * 86400,               # Email 2 — 1 day
    3 * 86400,               # Email 3 — 3 days
    5 * 86400,               # Email 4 — 5 days
    7 * 86400,               # Email 5 — 7 days
    10 * 86400,              # Email 6 — 10 days
    14 * 86400,              # Email 7 — 14 days
]

# Thread-safe lock around the JSON file
_file_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_db() -> List[dict]:
    with _file_lock:
        if not DB_PATH.exists():
            return []
        try:
            return json.loads(DB_PATH.read_text() or "[]")
        except Exception as e:
            logger.warning("Scheduler DB corrupt (%s) — starting fresh", e)
            return []


def _save_db(rows: List[dict]) -> None:
    with _file_lock:
        DB_PATH.parent.mkdir(exist_ok=True, parents=True)
        tmp = DB_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, indent=2))
        tmp.replace(DB_PATH)


def schedule_email_drip(
    contact_id: str,
    contact_email: str,
    first_name: str,
    emails: List[dict],
    scan_time: Optional[datetime] = None,
) -> int:
    """
    Enqueue the 7-email drip for a single contact.

    emails: list of {"day": int, "subject": str, "body": str} (output from
            email_generator.generate_full_sequence)
    Returns: number of emails enqueued.
    """
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


def cancel_drip(contact_id: str, reason: str = "cancelled") -> int:
    """Cancel all pending emails for a contact (e.g., they bought or unsubscribed)."""
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


MAX_RETRIES = 5


def _due_now(rows: List[dict]) -> List[dict]:
    """Return pending rows AND failed rows (for retry, up to MAX_RETRIES) that are due."""
    now = datetime.now(timezone.utc)
    out = []
    for r in rows:
        status = r.get("status")
        if status not in ("pending", "failed"):
            continue
        if status == "failed" and r.get("retry_count", 0) >= MAX_RETRIES:
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


def reset_all_failed() -> int:
    """Utility — reset every 'failed' row back to 'pending' so the next tick retries them.
    Used after swapping GHL tokens or fixing the send endpoint."""
    rows = _load_db()
    n = 0
    for r in rows:
        if r.get("status") == "failed":
            r["status"] = "pending"
            r["retry_count"] = 0
            r.pop("error", None)
            n += 1
    if n:
        _save_db(rows)
        logger.info("Reset %d failed rows back to pending", n)
    return n


def _dispatch_due() -> int:
    """
    Called by the polling loop. Sends any due emails and marks them sent.
    Returns number dispatched this tick.
    """
    from ghl import GHLClient  # local import to avoid circular

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
            body_with_sig = _append_signature_and_footer(r["body"])
            ok = client.send_email(
                contact_id=r["contact_id"],
                subject=r["subject"],
                html=_plaintext_to_html(body_with_sig),
                plain=body_with_sig,
            )
            r["status"] = "sent" if ok else "failed"
            r["dispatched_at"] = _now_iso()
            if ok:
                r.pop("error", None)
                r["retry_count"] = r.get("retry_count", 0)
                sent += 1
                logger.info("Sent email %d to %s: %r", r["email_index"], r["contact_email"], r["subject"][:60])
            else:
                r["retry_count"] = r.get("retry_count", 0) + 1
                logger.warning("Email send returned False for %s (retry %d/%d)",
                               r["contact_id"], r["retry_count"], MAX_RETRIES)
        except Exception as e:
            logger.exception("Email dispatch error for %s: %s", r.get("contact_id"), e)
            r["status"] = "failed"
            r["retry_count"] = r.get("retry_count", 0) + 1
            r["error"] = str(e)[:500]

    _save_db(rows)
    return sent


SIGNATURE_PLAIN = (
    "\n\n— Umar\n"
    "Bully AI · The Bureau Bullies\n"
    "Reply to this email if you want me to walk you through your plan."
)

FOOTER_PLAIN = (
    "\n\n---\n"
    "Sent because you scanned your credit report at bullyaiagent.com. "
    "Not interested? Reply STOP and I won't email again.\n"
    "The Bureau Bullies LLC · Edgemoor, GA"
)


def _append_signature_and_footer(body: str) -> str:
    """Add a human signature + CAN-SPAM-compliant footer to any email body."""
    if not body:
        return body
    # Avoid double-appending if already present
    if "Bully AI · The Bureau Bullies" in body:
        return body
    return body.rstrip() + SIGNATURE_PLAIN + FOOTER_PLAIN


def _plaintext_to_html(body: str) -> str:
    """Lightweight plain-text → HTML conversion preserving line breaks + links."""
    import re
    esc = (body or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    # auto-link http(s) URLs
    esc = re.sub(
        r"(https?://[^\s<]+)",
        r'<a href="\1" style="color:#e11d2e;text-decoration:underline;">\1</a>',
        esc,
    )
    paragraphs = ["<p style=\"margin:0 0 12px 0;\">" + p.replace("\n", "<br/>") + "</p>"
                  for p in esc.split("\n\n") if p.strip()]
    return (
        '<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;'
        'font-size:15px;line-height:1.5;color:#111;max-width:580px;">'
        + "\n".join(paragraphs)
        + "</div>"
    )


def _auto_rebuild_queue_from_ghl() -> int:
    """If the local scheduler queue is empty (e.g. redeploy wiped /tmp), rebuild
    it from GHL contacts who already have the cr_email_N_subject/body fields
    populated from past scans.

    This is the bullet-proof persistence layer: even if /tmp gets nuked on
    every redeploy, the per-contact email schedule is durable on the GHL
    contact itself, and we self-heal on every app start.

    Returns the number of contacts re-enqueued.
    """
    rows = _load_db()
    pending = sum(1 for r in rows if r.get("status") == "pending")
    if pending > 0:
        logger.info("Scheduler boot: %d pending rows already present, no rebuild needed", pending)
        return 0

    logger.info("Scheduler boot: queue empty, rebuilding from GHL contact custom fields...")
    try:
        from ghl import GHLClient
        client = GHLClient()
    except Exception as e:
        logger.warning("Scheduler boot: GHL client init failed, can't auto-rebuild: %s", e)
        return 0

    try:
        contacts = client.search_contacts_by_tag("bureau-scan") if hasattr(client, "search_contacts_by_tag") else []
    except Exception as e:
        logger.warning("Scheduler boot: search_contacts_by_tag failed: %s", e)
        return 0

    enqueued = 0
    skipped = 0
    for c in (contacts or []):
        cid = c.get("id") or c.get("_id")
        email_addr = c.get("email") or c.get("emailAddress")
        if not cid or not email_addr:
            skipped += 1
            continue
        fn = c.get("firstName") or c.get("first_name") or ""
        cf = c.get("customFields") or c.get("custom_field") or []
        cf_map = {}
        for item in cf:
            k = item.get("fieldKey") or item.get("key") or item.get("name") or ""
            v = item.get("value", "")
            cf_map[k.replace("contact.", "")] = v
        emails = []
        for i in range(1, 8):
            subj = cf_map.get(f"cr_email_{i}_subject", "")
            body = cf_map.get(f"cr_email_{i}_body", "")
            if subj and body:
                emails.append({"day": [0, 1, 3, 5, 7, 10, 14][i - 1], "subject": subj, "body": body})
        if not emails:
            skipped += 1
            continue

        # Use the contact's scan_completed_at if available, otherwise approximate
        # from createdAt so the cadence offsets land on the correct dates.
        scan_at_iso = cf_map.get("scan_completed_at") or cf_map.get("cr_scan_completed_at") or c.get("dateAdded") or c.get("createdAt") or ""
        scan_dt = None
        if scan_at_iso:
            try:
                scan_dt = datetime.fromisoformat(str(scan_at_iso).replace("Z", "+00:00"))
                if scan_dt.tzinfo is None:
                    scan_dt = scan_dt.replace(tzinfo=timezone.utc)
            except Exception:
                scan_dt = None

        try:
            n = schedule_email_drip(cid, email_addr, fn, emails, scan_time=scan_dt)
            if n:
                enqueued += 1
        except Exception as e:
            logger.warning("Auto-rebuild: schedule_email_drip failed for %s: %s", cid, e)

    logger.info("Scheduler boot: auto-rebuild done — enqueued=%d skipped=%d total_contacts=%d",
                enqueued, skipped, len(contacts or []))
    return enqueued


async def _poll_loop():
    """Background async loop that dispatches due emails every POLL_SECONDS."""
    logger.info("Email scheduler poll loop started (every %ds)", POLL_SECONDS)
    # On first tick, if the queue is empty, rebuild from GHL — this self-heals
    # the queue after any redeploy that wipes /tmp.
    try:
        await asyncio.to_thread(_auto_rebuild_queue_from_ghl)
    except Exception as e:
        logger.warning("Scheduler boot: auto-rebuild raised %s — continuing", e)
    while True:
        try:
            n = await asyncio.to_thread(_dispatch_due)
            if n:
                logger.info("Scheduler tick: dispatched %d email(s)", n)
        except Exception as e:
            logger.exception("Scheduler tick error: %s", e)
        await asyncio.sleep(POLL_SECONDS)


def start_background_scheduler(app) -> None:
    """Attach the scheduler to a FastAPI app's startup/shutdown events."""
    task_ref = {}

    @app.on_event("startup")
    async def _start():
        task_ref["task"] = asyncio.create_task(_poll_loop())

    @app.on_event("shutdown")
    async def _stop():
        t = task_ref.get("task")
        if t:
            t.cancel()
