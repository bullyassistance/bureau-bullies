"""
Bureau Bullies — FastAPI web server
------------------------------------
Routes:
  GET  /                    → landing page
  GET  /results             → scan results page
  GET  /terms, /privacy     → legal
  POST /api/scan            → upload + analyze + push to GHL + generate .docx
  POST /api/chat            → Bully AI chat (for GHL SMS reply webhooks)
  GET  /download/{token}    → download the generated Word doc
  GET  /healthz

Run:  uvicorn app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import shutil
import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from analyzer import analyze_report, summary_to_ghl_fields
from bully_ai import chat as bully_chat, detect_qualification_signals, detect_already_scanned, extract_email
import contact_memory
from docgen import generate_report_doc
from ghl import push_lead_to_ghl, GHLError
from email_generator import generate_full_sequence, emails_to_ghl_fields, GOAL_FRAMES
from scheduler import (
    schedule_email_drip,
    cancel_drip,
    start_background_scheduler,
    reset_all_failed,
)

try:
    from event_log import log_event, read_events
except Exception:  # pragma: no cover
    def log_event(*args, **kwargs):
        return None
    def read_events(*args, **kwargs):
        return []

try:
    from conversion_optimizer import lead_heat_score, should_alert_owner, variant_for_contact, daily_target_note
except Exception:  # pragma: no cover
    def lead_heat_score(*args, **kwargs):
        return 0
    def should_alert_owner(*args, **kwargs):
        return False
    def variant_for_contact(contact_id, channel=""):
        return "A"
    def daily_target_note():
        return "Target conversion rate is a KPI, not a guarantee."

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger("bureau-bullies.app")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/bullies_downloads"))
DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)

MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "25"))
ALLOWED_MIME = {
    "application/pdf",
    "image/png", "image/jpeg", "image/jpg", "image/webp", "image/gif",
}

app = FastAPI(title="Bureau Bullies API", version="2.0.0")

# Start the background email-drip scheduler — polls every 60s and dispatches
# any emails whose send_at has arrived.
start_background_scheduler(app)

# Allow the landing page to call /api/scan from any origin (GHL embed, custom domain, etc.)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],         # Tighten to your actual domains in production
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- Static pages --------------------------------------------------------
@app.get("/")
def landing():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/results")
def results():
    return FileResponse(FRONTEND_DIR / "results.html")


@app.get("/thank-you")
def thank_you():
    ty = FRONTEND_DIR / "thank-you.html"
    if ty.exists():
        return FileResponse(ty)
    return JSONResponse({"ok": True})


@app.get("/terms")
def terms():
    return FileResponse(FRONTEND_DIR / "terms.html")


@app.get("/privacy")
def privacy():
    return FileResponse(FRONTEND_DIR / "privacy.html")


# ---- Scan endpoint -------------------------------------------------------
@app.post("/api/scan")
async def scan(
    firstName: str = Form(...),
    lastName:  str = Form(...),
    email:     str = Form(...),
    phone:     str = Form(...),
    goal:      str = Form("freedom"),   # what they're trying to unlock
    ig_handle: str = Form(""),          # NEW — optional IG handle for DM follow-up
    reports:   List[UploadFile] = File(...),
):
    if not reports:
        raise HTTPException(400, "Please upload at least one file.")
    if len(reports) > 12:
        raise HTTPException(400, "Too many files. Upload up to 12.")

    tmpdir = Path(tempfile.mkdtemp(prefix="bullies_"))
    saved: List[Path] = []
    try:
        for upload in reports:
            if upload.content_type not in ALLOWED_MIME:
                raise HTTPException(400, f"{upload.filename}: PDFs and images only.")
            dest = tmpdir / upload.filename
            size = 0
            with dest.open("wb") as out:
                while chunk := await upload.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_FILE_MB * 1024 * 1024:
                        raise HTTPException(413, f"{upload.filename} exceeds {MAX_FILE_MB} MB.")
                    out.write(chunk)
            saved.append(dest)
            logger.info("Saved %s (%.1f KB, %s)", upload.filename, size / 1024, upload.content_type)

        # ---- Bully AI analysis -------------------------------------------
        summary = analyze_report(
            pdf_paths=saved,
            consumer_name=f"{firstName} {lastName}",
            consumer_phone=phone,
            scratch_dir=tmpdir,
        )

        # ---- Generate Word doc ------------------------------------------
        token = secrets.token_urlsafe(24)
        doc_path = DOWNLOAD_DIR / f"{token}.docx"
        generate_report_doc(
            summary=summary,
            consumer_first=firstName,
            consumer_last=lastName,
            consumer_email=email,
            consumer_phone=phone,
            out_path=doc_path,
        )
        logger.info("Generated doc: %s", doc_path)

        # ---- Push to GHL -------------------------------------------------
        custom_fields = summary_to_ghl_fields(summary)
        custom_fields["cr_doc_url"] = f"/download/{token}"  # surfaced in SMS

        # Capture the goal
        GOAL_LABELS = {
            "house":       "House",
            "car":         "Car",
            "business":    "Business",
            "credit_card": "Credit Card",
            "freedom":     "Personal Freedom",
            "peace":       "Peace of Mind",
        }
        goal_key = (goal or "freedom").strip().lower().replace(" ", "_").replace("-", "_")
        if goal_key not in GOAL_FRAMES:
            goal_key = "freedom"
        goal_label = GOAL_LABELS.get(goal_key, "Personal Freedom")
        custom_fields["cr_goal"] = goal_key
        custom_fields["cr_goal_label"] = goal_label

        # Write the actual scan timestamp so the email scheduler's catch-up
        # logic uses TODAY's scan, not GHL's old dateAdded if this contact
        # had a prior interaction. Critical for the "skipped-too-old" bug.
        custom_fields["cr_scan_completed_at"] = datetime.now(timezone.utc).isoformat()

        # Capture IG handle if provided so we can DM them the breakdown.
        # Strip leading @ and any URL prefix so the bare handle is stored.
        ig_clean = (ig_handle or "").strip().lstrip("@").strip()
        if ig_clean:
            # Strip URL forms: instagram.com/foo, https://www.instagram.com/foo/
            import re as _re
            m = _re.search(r"(?:instagram\.com/)?([A-Za-z0-9_.]{1,30})", ig_clean)
            if m:
                ig_clean = m.group(1).lower()
            custom_fields["ig_handle"] = ig_clean

        # ---- Generate 7-email tailored nurture drip ---------------------
        try:
            scan_ctx = {
                "top_collection_name":   summary.top_collection_name,
                "top_collection_amount": summary.top_collection_amount,
                "total_leverage":        summary.total_estimated_leverage,
                "violations_count":      len(summary.violations),
                "fico_range":            summary.estimated_fico_range,
                "top_pain_point":        summary.top_pain_point,
                "fear_hook":             summary.fear_hook,
                "case_law_cited":        "; ".join(summary.case_law_cited or []),
                "recommended_tier":      summary.recommended_tier,
            }
            emails = generate_full_sequence(
                consumer_first=firstName,
                scan=scan_ctx,
                goal_key=goal_key,
                goal_label=goal_label,
            )
            custom_fields.update(emails_to_ghl_fields(emails))
            logger.info("Generated %d tailored emails for %s", len(emails), firstName)
        except Exception as e:
            logger.exception("Email sequence generation failed (non-fatal): %s", e)

        ghl_result = None
        try:
            ghl_result = push_lead_to_ghl(
                first_name=firstName,
                last_name=lastName,
                email=email,
                phone=phone,
                custom_fields=custom_fields,
                urgency_score=summary.urgency_score,
                recommended_tier=summary.recommended_tier,
            )
        except GHLError as e:
            logger.error("GHL push failed (non-fatal): %s", e)
        except Exception as e:
            logger.exception("GHL push unexpected error: %s", e)

        # ---- IG DM the scan breakdown (when handle was provided) -------
        # Mirrors the SMS+email sequence on a third channel: as soon as the
        # scan finishes, Bully AI fires a personalized DM to their IG handle
        # with the top collection by name+amount, total leverage, and the
        # transformation pitch tied to their goal. Same tier-recommendation
        # ladder runs in subsequent IG replies via /webhooks/ig/dm.
        try:
            if ig_clean and ghl_result:
                contact_id_for_ig = (ghl_result.get("contact") or ghl_result).get("id") \
                                  or (ghl_result.get("contact") or ghl_result).get("_id") or ""
                if contact_id_for_ig:
                    dm_body = _build_scan_breakdown_dm(
                        first_name=firstName,
                        ig_handle=ig_clean,
                        summary=summary,
                        goal_key=goal_key,
                        goal_label=goal_label,
                    )
                    sent = _ig_send_dm_safe(contact_id_for_ig, dm_body)
                    logger.info("IG DM sent to @%s after scan: %s", ig_clean, "ok" if sent else "FAILED")
        except Exception as e:
            logger.exception("IG DM after scan failed (non-fatal): %s", e)

        # ---- Schedule the 7-email drip (backend-driven, no GHL workflow needed)
        try:
            contact_id = ""
            if ghl_result:
                contact_id = (ghl_result.get("contact") or ghl_result).get("id") \
                          or (ghl_result.get("contact") or ghl_result).get("_id") \
                          or ""
            if contact_id and "emails" in locals() and emails:
                schedule_email_drip(
                    contact_id=contact_id,
                    contact_email=email,
                    first_name=firstName,
                    emails=emails,
                )
        except Exception as e:
            logger.exception("Failed to schedule email drip: %s", e)

        return JSONResponse({
            "success": True,
            "resultsUrl": "/results",
            "downloadUrl": f"/download/{token}",
            "summary": asdict(summary),
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Scan failed")
        return JSONResponse(
            {"success": False, "error": f"Scan failed: {e}"},
            status_code=500,
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---- Download the generated doc ------------------------------------------
@app.get("/download/{token}")
def download(token: str):
    path = DOWNLOAD_DIR / f"{token}.docx"
    if not path.exists():
        raise HTTPException(404, "This download has expired.")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename="BureauBullies_AttackPlan.docx",
    )


# ---- Bully AI chat endpoint (for GHL SMS reply webhook) ------------------
class ChatIn(BaseModel):
    message: str
    contact: dict = {}       # GHL custom fields dump
    history: list = []       # prior turns


@app.post("/api/chat")
def chat(payload: ChatIn):
    try:
        reply = bully_chat(
            user_message=payload.message,
            contact_context=payload.contact or None,
            history=payload.history or None,
        )
        return {"reply": reply}
    except Exception as e:
        logger.exception("Chat failed")
        raise HTTPException(500, f"Chat error: {e}")


# ---- GHL-native SMS reply webhook ----------------------------------------
# Accepts whatever shape GHL sends ({body: str, contact: {...}, customData: {...}}
# and returns a JSON response that GHL's workflow can use to send an SMS back.
#
# GHL workflow setup:
#   Trigger: Customer Replied (SMS)
#   Step 1: Webhook → POST https://bureau-bullies.onrender.com/webhooks/ghl/sms-reply
#           Body: {
#             "message": "{{message.body}}",
#             "first_name": "{{contact.first_name}}",
#             "contact_id": "{{contact.id}}",
#             "custom_fields": {
#               "cr_top_collection_name": "{{custom_values.cr_top_collection_name}}",
#               "cr_total_leverage": "{{custom_values.cr_total_leverage}}",
#               "cr_violations_count": "{{custom_values.cr_violations_count}}",
#               "cr_recommended_tier": "{{contact.cr_recommended_tier}}",
#               "cr_top_pain_point": "{{contact.cr_top_pain_point}}",
#               "cr_exec_summary":   "{{contact.cr_exec_summary}}"
#             }
#           }
#   Step 2: Send SMS → Message body: {{webhook.response.reply}}
@app.post("/webhooks/ghl/sms-reply")
async def ghl_sms_reply(request: Request):
    """
    Accept an inbound SMS reply from GHL and route it through Bully AI.
    Returns a shape GHL's workflow can use to send the response SMS.
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # GHL can send in many shapes — be liberal in what we accept.
    # The `message` field might be a string OR a nested dict like
    # { body: "hi", id: "...", type: "sms" }
    def _as_string(v) -> str:
        if v is None:
            return ""
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            return str(v.get("body") or v.get("text") or v.get("content") or v.get("message") or "")
        return str(v)

    message = _as_string(
        payload.get("message")
        or payload.get("body")
        or payload.get("sms")
        or payload.get("text")
        or (payload.get("customData") or {}).get("message")
        or ""
    ).strip()

    if not message:
        logger.warning("SMS reply webhook received empty message: %s", payload)
        return {"reply": "Got your message — let me pull your report and get right back to you.", "ok": False}

    # ── Duplicate inbound dedupe — same fix as IG DM ──
    _early_cid = _inbound_identity(payload, fallback="sms")
    if _is_duplicate_inbound(_early_cid, message):
        logger.warning(
            "SMS dedupe: dropping duplicate inbound (cid=%s) — already processed within %ds",
            _early_cid, _INBOUND_DEDUPE_WINDOW_SEC,
        )
        return {"ok": True, "skipped": "duplicate_inbound"}

    # Gather as much contact context as GHL passed
    first_name = (
        payload.get("first_name")
        or payload.get("firstName")
        or (payload.get("contact") or {}).get("first_name")
        or ""
    )

    # Merge custom_fields + top-level cr_* fields + contact custom field dump
    custom = {}
    for k, v in payload.items():
        if k.startswith("cr_") and v not in (None, ""):
            custom[k] = v
    for k, v in (payload.get("custom_fields") or {}).items():
        if v not in (None, ""):
            custom[k] = v
    for k, v in (payload.get("customData") or {}).items():
        if v not in (None, ""):
            custom[k] = v
    contact_block = payload.get("contact") or {}
    for k, v in contact_block.items():
        if k.startswith("cr_") and v not in (None, ""):
            custom[k] = v

    if first_name and "cr_first_name" not in custom:
        custom["cr_first_name"] = first_name

    # Pull contact_id and conversation history so SMS replies have memory of prior turns
    contact_id = _ig_extract_contact_id(payload)

    # MANUAL TAKEOVER: if Umar types a pause/stop-AI instruction in the thread,
    # tag the contact and return no reply.
    if _owner_pause_requested(message):
        _hard_pause_and_alert(contact_id, "SMS", "manual takeover command", ["manual-takeover"], message)
        return {"ok": True, "skipped": "manual_takeover", "first_name": first_name}

    # HARD-PAUSE: if customer's message contains refund/cancel/legal/accusation
    # language, AI MUST NEVER reply. Apply pause-ai + needs-human, alert Umar.
    requires_human, matched = _customer_message_requires_human(message)
    if requires_human:
        _hard_pause_and_alert(
            contact_id, "SMS", "refund/cancel/legal language", matched, message,
        )
        return {
            "ok": True,
            "skipped": "hard_paused_customer_message",
            "matched_phrases": matched[:5],
            "first_name": first_name,
        }

    # If Umar/manual-mode already paused this contact, do not send even keyword links.
    if contact_id and _ig_human_active(contact_id):
        logger.info("SMS reply skipped before keyword — human active for contact %s", contact_id)
        return {"ok": True, "skipped": "human_active", "first_name": first_name}

    # ── Keyword shortcut: instant deterministic reply (skip Claude) ──
    # Keyword shortcuts are safe deterministic replies, but they still must respect manual pause.
    # safe, so a past human conversation should NOT block a customer who
    # explicitly texts "equifax" asking for the link. GHL workflow's
    # downstream "Send SMS" step uses {{webhook.response.reply}}.
    # Search corpus widened to all probable text fields (catches payload
    # variants where the SMS body sits in body/text/customData).
    kw_match = _match_keyword_shortcut(_build_keyword_search_corpus(payload, message))
    if kw_match:
        kw_key, kw_cfg = kw_match
        kw_reply = _render_keyword_reply(kw_cfg, first_name)
        _apply_keyword_tags(contact_id, kw_cfg)
        if contact_id:
            try:
                contact_memory.append_turn(contact_id, "user", message)
                contact_memory.append_turn(contact_id, "assistant", kw_reply)
            except Exception:
                pass
        logger.info("SMS keyword shortcut '%s' fired → contact=%s",
                    kw_key, contact_id)
        return {
            "ok": True,
            "reply": kw_reply,
            "first_name": first_name,
            "keyword_shortcut": kw_key,
        }

    # Skip Bully AI replies if Umar is already replying manually (runs AFTER
    # the keyword shortcut so customers explicitly asking for product links
    # still receive them).
    if contact_id and _ig_human_active(contact_id):
        logger.info("SMS reply skipped — human active for contact %s", contact_id)
        return {"ok": True, "skipped": "human_active", "first_name": first_name}

    history = payload.get("history") or _ig_fetch_history(contact_id)

    # Persistent memory: prefer this contact's full history file from /var/data
    # over GHL's recent fetch. The local memory is durable across redeploys
    # AND remembers things from days/weeks ago that GHL's 30-msg limit drops.
    if contact_id:
        try:
            persistent_history = contact_memory.history_as_anthropic_messages(contact_id, max_turns=30)
            if persistent_history:
                history = persistent_history
            # Inject the rolling summary + key facts as a "context block"
            mem_block = contact_memory.format_for_prompt(contact_id)
            if mem_block:
                custom["persistent_memory"] = mem_block
        except Exception as e:
            logger.warning("contact_memory load failed in SMS reply: %s", e)

    # Already-scanned reconciliation — same fix as the IG webhook. If the SMS
    # reply contains an email, look up the contact and load their scan data.
    custom = _enrich_context_with_already_scanned(message, custom)

    # ─────────────────────────────────────────────────────────────────────
    # Determine lead_stage from tags so Bully AI activates the right mode.
    # Stages (mutually exclusive, checked in this priority order):
    #   - qualified-no-upload → text-based intake fallback
    #   - bureau-scan-completed → standard PAS / upsell mode
    #   - qualifier-cold        → qualifier mode
    # ─────────────────────────────────────────────────────────────────────
    tags_lower = [str(t).lower() for t in (payload.get("tags") or contact_block.get("tags") or [])]

    if "qualified-no-upload" in tags_lower:
        custom["lead_stage"] = "qualified-no-upload"
    elif "bureau-scan-completed" in tags_lower:
        custom["lead_stage"] = "bureau-scan-completed"
    elif "qualifier-cold" in tags_lower or "fb-form-backfill" in tags_lower:
        custom["lead_stage"] = "qualifier-cold"

    # Qualification detection runs only in qualifier-cold stage
    is_qualifier_cold = (custom.get("lead_stage") == "qualifier-cold") and ("qualified" not in tags_lower)

    qualification = {"is_qualified": False}
    if is_qualifier_cold:
        qualification = detect_qualification_signals(message)
        if qualification["is_qualified"] and contact_id:
            try:
                from ghl import GHLClient
                _qclient = GHLClient()
                tags_to_add = ["qualified"]
                _qclient.add_tags(contact_id, tags_to_add)
                # Update structured custom fields so future merge tags + downstream
                # workflow steps can use them.
                cf = {}
                if qualification.get("biggest_debt"):
                    cf["biggest_debt"] = qualification["biggest_debt"]
                if qualification.get("goal"):
                    cf["goal"] = qualification["goal"]
                if qualification.get("timeline"):
                    cf["timeline"] = qualification["timeline"]
                cf["qualified_at"] = datetime.now(timezone.utc).isoformat()
                # Lightweight upsert just to set custom fields without changing tags
                if hasattr(_qclient, "update_contact_fields"):
                    _qclient.update_contact_fields(contact_id, cf)
                logger.info("Qualified contact %s — debt=%s goal=%s timeline=%s",
                            contact_id, qualification.get("biggest_debt"),
                            qualification.get("goal"), qualification.get("timeline"))
            except Exception as e:
                logger.warning("Could not apply qualified tag/fields: %s", e)
        # Inject qualification context for Bully AI's reply
        if qualification.get("biggest_debt"):
            custom["qualifier_named_debt"] = qualification["biggest_debt"]
        if qualification.get("goal"):
            custom["qualifier_named_goal"] = qualification["goal"]

    logger.info(
        "SMS reply from %s: %r  (ctx keys: %s, history turns: %d, qualified=%s)",
        first_name or "unknown", message[:80], list(custom.keys())[:8],
        len(history), qualification.get("is_qualified", False)
    )

    # Persist inbound to memory BEFORE generating reply
    if contact_id:
        try:
            contact_memory.append_turn(contact_id, "user", message)
            # Record qualification facts
            if qualification.get("biggest_debt"):
                contact_memory.record_fact(contact_id, "biggest_debt", qualification["biggest_debt"])
            if qualification.get("goal"):
                contact_memory.record_fact(contact_id, "goal", qualification["goal"])
            if qualification.get("timeline"):
                contact_memory.record_fact(contact_id, "timeline", qualification["timeline"])
        except Exception as e:
            logger.warning("contact_memory append (user) failed: %s", e)

    try:
        reply_text = bully_chat(
            user_message=message,
            contact_context=custom or None,
            history=history,
        )
    except Exception as e:
        logger.exception("SMS reply chat failed")
        reply_text = (
            f"{first_name + ', ' if first_name else ''}"
            "Bully AI here. Got your message. Give me a few minutes and I'll get "
            "back to you with a real answer. BB"
        )

    # Persist outbound reply to memory
    if contact_id and reply_text:
        try:
            contact_memory.append_turn(contact_id, "assistant", reply_text)
        except Exception as e:
            logger.warning("contact_memory append (assistant) failed: %s", e)

    # Auto-handoff if Bully AI promised a human
    if _detect_handoff(reply_text):
        logger.info("Handoff detected in SMS reply for contact %s", contact_id)
        _do_handoff(contact_id, first_name, message)

    # Heuristic: figure out if Bully AI's reply mentions a link so GHL can
    # route through the right follow-up workflow if it wants
    lower = reply_text.lower()
    link_sent = None
    if "thecollectionkiller.com/dispute-vault" in lower:
        link_sent = "vault"
    elif "suethemallwithus.com" in lower or "dfy" in lower:
        link_sent = "dfy"
    elif "thecollectionkiller.com" in lower:
        link_sent = "toolkit"

    return {
        "ok": True,
        "reply": reply_text,
        "link_sent": link_sent,
        "first_name": first_name,
        "handoff": _detect_handoff(reply_text),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Already-scanned reconciliation — fixes the IG loop where a user who already
# uploaded gets the upload link sent to them again. Two failure paths:
#   1. They DM from an IG handle that doesn't match their bullyaiagent.com email
#      → no contact match → no scan data in context → AI sends upload link.
#   2. We have a contact match but the scan custom fields aren't populated yet
#      → AI doesn't know what to reference.
# Fix: detect "I uploaded" claims, ask for email, look up GHL contact by email,
# inject their scan data into Bully AI's context for the rest of the thread.
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_context_with_already_scanned(message: str, custom: dict) -> dict:
    """Mutates `custom` dict to add already-scanned signals + scan data when
    we can resolve the user's identity by email. Returns the enriched dict.

    Behavior:
      - If user's message contains an email AND we don't already have scan data:
        look up the GHL contact by email, pull cr_* fields, add to custom.
      - If user claims to have scanned but we still don't have scan data:
        set custom["lead_stage"] = "claims-scanned-no-data" so Bully AI's
        ALREADY-SCANNED MODE asks them for email + full name.
      - If we have scan data (cr_violations_count present): set
        custom["lead_stage"] = "scan-loaded" so Bully AI runs FEAR+URGENCY MODE.
    """
    if not isinstance(custom, dict):
        custom = dict(custom or {})

    has_scan_data = any(
        (k.startswith("cr_") or k in ("biggest_debt", "total_leverage"))
        and v not in (None, "", 0, "0")
        for k, v in custom.items()
    )

    # Try to harvest an email from the message and look it up
    email = extract_email(message or "")
    if email and not has_scan_data:
        try:
            from ghl import GHLClient
            client = GHLClient()
            scan_ctx = client.get_scan_context_by_email(email) if hasattr(client, "get_scan_context_by_email") else {}
            if scan_ctx:
                # Merge — user's existing context wins for non-cr fields, scan data wins for cr_*
                for k, v in scan_ctx.items():
                    if k.startswith("cr_") or k in ("first_name", "matched_contact_id", "goal", "biggest_debt"):
                        custom[k] = v
                logger.info("Reconciled scan via email %s — keys added: %s", email, list(scan_ctx.keys())[:8])
                # Re-check after enrichment
                has_scan_data = any(
                    (k.startswith("cr_") or k in ("biggest_debt", "total_leverage"))
                    and v not in (None, "", 0, "0")
                    for k, v in custom.items()
                )
        except Exception as e:
            logger.warning("Email-based scan lookup failed for %s: %s", email, e)

    claims_scanned = detect_already_scanned(message or "")

    if has_scan_data:
        custom["lead_stage"] = "scan-loaded"
        custom["mode_hint"] = "fear-urgency"
    elif claims_scanned:
        custom["lead_stage"] = "claims-scanned-no-data"
        custom["mode_hint"] = "ask-for-email"

    return custom


def _ig_as_string(v) -> str:
    """Liberal coercion — IG/GHL DMs may arrive as strings or nested dicts."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return str(v.get("body") or v.get("text") or v.get("content") or v.get("message") or "")
    return str(v)


def _walk_payload_values(obj, *, max_depth: int = 5):
    """Yield (key, value) from nested GHL/Meta webhook payloads."""
    if max_depth < 0:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k), v
            if isinstance(v, (dict, list)):
                yield from _walk_payload_values(v, max_depth=max_depth - 1)
    elif isinstance(obj, list):
        for item in obj[:25]:
            if isinstance(item, (dict, list)):
                yield from _walk_payload_values(item, max_depth=max_depth - 1)


def _first_payload_string(payload: dict, keys: set[str]) -> str:
    keys_l = {k.lower() for k in keys}
    for k, v in _walk_payload_values(payload):
        if k.lower() in keys_l and isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _inbound_identity(payload: dict, fallback: str = "") -> str:
    """Stable identity for dedupe/pause checks, even if contact_id is missing."""
    cid = _ig_extract_contact_id(payload)
    if cid:
        return cid
    ident = _first_payload_string(payload, {
        "conversationId", "conversation_id", "messageId", "message_id",
        "senderId", "sender_id", "fromId", "from_id", "userId", "user_id",
        "ig_handle", "username", "handle", "phone", "email",
    })
    if ident:
        return ident
    return fallback or "unknown-inbound"


def _owner_pause_requested(message: str) -> bool:
    """Detect explicit manual-takeover commands typed into the thread."""
    t = (message or "").lower().strip()
    if not t:
        return False
    phrases = (
        "pause talking", "stop talking", "stop replying", "do not reply",
        "don't reply", "dont reply", "pause ai", "stop ai", "ai off",
        "manual mode", "manual takeover", "i got this", "i'll take over",
        "ill take over", "umar here", "this is umar", "human takeover",
    )
    return any(p in t for p in phrases)


def _ig_extract_contact_id(payload: dict) -> str:
    """Pull the GHL contact id out of an IG/GHL webhook payload."""
    if not isinstance(payload, dict):
        return ""

    explicit = _first_payload_string(payload, {
        "contact_id", "contactId", "contact_Id", "ghl_contact_id",
        "contact_id_value", "contactIdValue",
    })
    if explicit and len(explicit) >= 8:
        return explicit

    for wrapper in ("contact", "Contact", "customer", "lead", "person"):
        obj = payload.get(wrapper)
        if isinstance(obj, dict):
            for key in ("id", "_id"):
                v = obj.get(key)
                if isinstance(v, str) and len(v) >= 8:
                    return v
    return ""


def _ig_extract_comment_id(payload: dict) -> str:
    """Optional — pull an IG comment id from the payload."""
    if not isinstance(payload, dict):
        return ""
    explicit = _first_payload_string(payload, {
        "comment_id", "commentId", "ig_comment_id", "instagram_comment_id",
        "replyToCommentId", "reply_to_comment_id",
    })
    if explicit:
        return explicit
    for wrapper in ("comment", "trigger"):
        obj = payload.get(wrapper)
        if isinstance(obj, dict):
            v = obj.get("id") or obj.get("_id")
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def _ig_send_public_comment_reply_safe(comment_id: str, message: str) -> bool:
    """Wrapper around GHLClient.send_ig_comment_reply that catches all errors
    so the calling webhook never breaks if the Meta token is missing/expired.
    Returns True iff the public reply went out."""
    if not comment_id or not message:
        return False
    try:
        from ghl import GHLClient
        client = GHLClient()
        return client.send_ig_comment_reply(comment_id, message)
    except Exception as e:
        logger.warning("_ig_send_public_comment_reply_safe error: %s", e)
        return False


# Default public comment reply when a keyword shortcut fires. Short, low-key,
# emoji-light. The visitor scrolling sees "Sent. Check your DMs ✉️" under the
# commenter's note — social proof that the brand actually responds.
_KEYWORD_COMMENT_REPLY = "Sent. Check your DMs 💌"


def _ig_send_dm_safe(contact_id: str, reply: str, *, comment_id: str = "") -> bool:
    """Best-effort send an IG DM via GHL Conversations API.
    Never raises — logs and returns False on any failure."""
    if not contact_id or not reply:
        return False
    try:
        from ghl import GHLClient
        client = GHLClient()
        return client.send_ig_dm(contact_id, reply, comment_id=comment_id or None)
    except Exception as e:
        logger.warning("_ig_send_dm_safe failed: %s", e)
        return False


def _build_scan_breakdown_dm(first_name: str, ig_handle: str, summary, goal_key: str, goal_label: str) -> str:
    """Personalized IG DM fired immediately after a scan completes when the user
    provided their IG handle. Mirrors the close-ladder Stage 1 (transformation +
    specifics): top collection by name+amount, total leverage, transformation
    phrase tied to their goal, and the recommended tier.

    Subsequent IG replies route through /webhooks/ig/dm and get the full
    Transformation → Fear → Discount close ladder via Bully AI's system prompt.
    """
    fn = (first_name or "there").strip()
    top_name   = getattr(summary, "top_collection_name", None) or "your biggest collection"
    top_amt    = getattr(summary, "top_collection_amount", 0) or 0
    leverage   = getattr(summary, "total_estimated_leverage", 0) or 0
    violations = len(getattr(summary, "violations", []) or [])
    tier       = (getattr(summary, "recommended_tier", "") or "").lower()

    transformation = {
        "house":        "Get this gone and you're 90 days from keys at the closing table on the rate other people get, not the rate banks charge stressed credit.",
        "car":          "Get this gone and you're walking off the lot in your name, on a 5% APR instead of 18.",
        "business":     "Get this gone and the EIN funding doors actually open. Net-30 vendors say yes instead of laughing.",
        "credit_card":  "Get this gone and we're talking real Amex, real Chase Sapphire — not a $300 secured card.",
        "freedom":      "Get this gone and the phone stops ringing. The mail stops being a threat.",
        "peace":        "Get this gone and you sleep through the night without thinking about your score.",
    }.get(goal_key, "Get this gone and your goal moves from theoretical to inevitable.")

    if tier in ("dfy", "tier_dfy", "done_for_you"):
        tier_pitch = "DFY makes sense for your file size, $2,500 paid in full or $229/mo indefinite. Squad runs every dispute for you."
    elif tier in ("vault", "dispute_vault", "tier_vault"):
        tier_pitch = "Dispute Vault is the move, $66 one-time. Letters pre-written for the violations on your file, you mail certified."
    else:
        tier_pitch = "Collection Toolkit is your starting point, $17 one-time. DIY playbook for the validation letter that kills this account."

    amt_str = f"${top_amt:,.0f}" if top_amt else ""
    leverage_str = f"${leverage:,.0f}" if leverage else ""

    body = (
        f"Hey {fn}, Bully AI here. Just ran your file. Here's the breakdown:\n\n"
        f"The play is your {top_name} {amt_str} account. Out of {violations} violations and {leverage_str} in total leverage on your report, this one is the priority.\n\n"
        f"{transformation}\n\n"
        f"{tier_pitch} What's your move?"
    )
    return body


# Customer messages containing ANY of these MUST be human-handled, never AI.
# These are refund / cancel / billing-confusion / legal / accusation patterns
# where AI replies can create real liability (false promises, missed
# contractual obligations, escalation triggers). Detecting any of these:
#   - applies pause-ai + needs-human tags immediately
#   - alerts Umar via SMS
#   - returns True so the caller skips AI generation
# ─────────────────────────────────────────────────────────────────────────
# Inbound dedupe — prevent Bully AI multi-fire when GHL sends the same DM
# multiple times (multiple workflows on same trigger, retries, etc.)
# ─────────────────────────────────────────────────────────────────────────
# Real-world bug: customer sends "Equifax" once, GHL fires our webhook 4-5
# times in <2s, each invocation calls Claude → 4-5 Bully AI replies stack
# on the customer's DM thread + 4-5 Anthropic API charges per inbound.
#
# Defense: hash (contact_id, lowercased message text). If the same hash was
# processed in the last DEDUPE_WINDOW_SECONDS, return immediately with
# skipped:duplicate. Process-local dict (single-worker assumption). On
# multi-worker Render deployments this fails open (worst case = 1 dup
# slips through per worker, still way better than current 5x fan-out).

import time as _time_module
import hashlib as _hashlib_module

_RECENT_INBOUND_HASHES: dict = {}
_INBOUND_DEDUPE_WINDOW_SEC = 30


def _is_duplicate_inbound(contact_id: str, message: str) -> bool:
    """Returns True if (identity, message) was processed in the last 30s.

    `contact_id` is really an identity string: GHL contact id when available,
    otherwise conversation id / sender id / handle / phone. Older code returned
    False whenever contact_id was blank, which let GHL duplicate webhooks spam
    the same IG/text reply multiple times.
    """
    if not message:
        return False
    identity = (contact_id or "unknown-inbound").strip().lower()
    norm = message.strip().lower()
    if not norm:
        return False
    msg_hash = _hashlib_module.md5(norm.encode("utf-8", "replace")).hexdigest()[:16]
    key = f"{identity}::{msg_hash}"
    now = _time_module.time()
    last = _RECENT_INBOUND_HASHES.get(key, 0.0)
    if (now - last) < _INBOUND_DEDUPE_WINDOW_SEC:
        _RECENT_INBOUND_HASHES[key] = now
        return True
    _RECENT_INBOUND_HASHES[key] = now
    if len(_RECENT_INBOUND_HASHES) > 2000:
        cutoff = now - (_INBOUND_DEDUPE_WINDOW_SEC * 2)
        stale = [k for k, v in _RECENT_INBOUND_HASHES.items() if v < cutoff]
        for k in stale:
            _RECENT_INBOUND_HASHES.pop(k, None)
    return False


def _customer_message_requires_human(message: str) -> tuple[bool, list[str]]:
    """Return (requires_human, matched_phrases). If True, AI must NEVER reply."""
    if not message or not isinstance(message, str):
        return False, []
    low = message.lower()
    matched = [p for p in _HUMAN_REQUIRED_PHRASES if p in low]
    return bool(matched), matched


def _hard_pause_and_alert(contact_id: str, channel: str, reason: str, matched: list[str], message_preview: str = "") -> None:
    """Immediately apply pause-ai + needs-human tags AND SMS-alert Umar.
    Used when a customer message contains refund/cancel/legal language so
    the AI never responds to threads where a human MUST take over."""
    if not contact_id:
        return
    try:
        from ghl import GHLClient
        client = GHLClient()
        client.add_tags(contact_id, ["pause-ai", "needs-human"])
        logger.warning(
            "HARD-PAUSE: %s on %s (matched: %s) — pause-ai + needs-human applied",
            reason, contact_id, matched[:5],
        )
    except Exception as _e:
        logger.error("HARD-PAUSE: failed to apply tags to %s: %s", contact_id, _e)
    # SMS alert Umar (best-effort)
    try:
        notify_phone = os.getenv("UMAR_NOTIFY_PHONE", "").strip()
        if notify_phone:
            from ghl import GHLClient
            _c = GHLClient()
            preview = (message_preview or "")[:200]
            alert = (
                f"[Bully AI HARD-PAUSE] {channel} contact {contact_id[:8]}... "
                f"sent {reason}. Matched: {', '.join(matched[:3])}. AI is OFF on "
                f"this thread. Customer said: {preview}"
            )
            _c.send_sms(phone=notify_phone, message=alert[:1500])
    except Exception as _e:
        logger.warning("HARD-PAUSE: alert SMS failed: %s", _e)


# ──────────────────────────────────────────────────────────────────────────
# Keyword shortcut router — instant deterministic replies for product keywords
# ──────────────────────────────────────────────────────────────────────────
# When a customer DMs / comments / texts one of these trigger words, we send
# the matching product link IMMEDIATELY without invoking Claude. Faster (no
# LLM round-trip), cheaper (no token spend), and guaranteed-on-message.
#
# Match rules:
#   - Case-insensitive whole-word match against the inbound message
#   - First match wins (in dict insertion order)
#   - Multi-word triggers like "equifax exposed" are matched by substring
#   - The trigger word can be the entire message OR appear in a sentence:
#        "equifax"                       → match
#        "send me the equifax link"      → match
#        "tell me about equifax"         → match (substring)
#
# Each shortcut applies tags so we can attribute conversions back to keyword
# triggers (heat-hot + the keyword-specific tag).
#
# Reply templates support {first_name} interpolation.

_KEYWORD_SHORTCUTS: dict = {
    # ─── Equifax Exposed ($27 limited-time, normally $666) ─────────────────
    # Trigger: someone DMs/comments/texts "equifax" (or "equifax exposed",
    # "send equifax", etc.). Drops the equifaxexposed.com link with a clean
    # urgency hook.
    "equifax": {
        "triggers": ("equifax", "equifax exposed", "send equifax",
                     "the equifax link", "equifax link", "equifax book",
                     "equifax guide"),
        "reply_template": (
            "{name_lead}Equifax \"investigates\" disputes in 10 seconds and "
            "rubber-stamps them. That's a federal violation — and it's exactly "
            "how their stack is built.\n\n"
            "Equifax Exposed shows you the playbook to weaponize that against "
            "them. Normally $666. Today it's $27.\n\n"
            "https://equifaxexposed.com/\n\n"
            "Grab it before the price flips back."
        ),
        "tags": ("equifax-keyword", "equifax-exposed-link-sent", "heat-hot"),
    },
    # Future shortcuts go here. Examples to add later:
    #   "transunion": {...}
    #   "experian":  {...}
    #   "vault":     {...}
}


def _build_keyword_search_corpus(payload: dict, primary_message: str) -> str:
    """Combine every likely text-bearing field from a GHL/Meta inbound payload
    into a single string for keyword matching.

    Why: For Instagram story-replies, GHL ships the customer's actual reply
    text in fields like `story.body`, `reply_to.text`, or `attachment.title`
    instead of the standard `message.body`. The keyword shortcut matcher only
    looks at one input string, so we widen the net here to capture any string
    field that might carry the customer's intent ("Equifax", etc.) before
    handing off to the matcher.

    SAFE: only pulls from fields known to carry user-typed text. Tags,
    contact metadata, and IDs are intentionally excluded so a tag like
    "equifax-keyword" can't trigger a false-positive match.
    """
    if not isinstance(payload, dict):
        return primary_message or ""

    parts: list[str] = []
    if primary_message:
        parts.append(str(primary_message))

    # Top-level text fields commonly used by GHL / Meta webhooks
    for k in ("body", "text", "messageBody", "message_body", "content",
              "caption", "title", "subject"):
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v)

    # Nested objects that may carry the actual reply text for story-replies
    for outer in ("message", "reply_to", "replyTo", "story", "story_reply",
                  "attachment", "attachments", "customData", "custom_data",
                  "meta", "ig"):
        outer_val = payload.get(outer)
        if isinstance(outer_val, dict):
            for k in ("body", "text", "messageBody", "content", "caption",
                     "title", "story_text", "message"):
                v = outer_val.get(k)
                if isinstance(v, str) and v.strip():
                    parts.append(v)
        elif isinstance(outer_val, list):
            for item in outer_val[:5]:  # cap at 5 attachments
                if isinstance(item, dict):
                    for k in ("body", "text", "caption", "title"):
                        v = item.get(k)
                        if isinstance(v, str) and v.strip():
                            parts.append(v)

    return " ".join(parts).strip() or (primary_message or "")


def _match_keyword_shortcut(message: str) -> "Optional[tuple[str, dict]]":
    """Return (shortcut_key, shortcut_dict) if the inbound message matches a
    keyword trigger, else None.

    Two-pass match to handle real-world mobile typing:
      Pass 1 — exact case-insensitive substring (catches "equifax", "send me
               the equifax link", "EQUIFAX EXPOSED", etc.)
      Pass 2 — fuzzy whole-word match using difflib SequenceMatcher with
               cutoff 0.75. Catches typos like "Exquifax", "Equifaxx",
               "Equafax", "Equifx" that real customers send from phones.
               Without this, the bot just asks "what's going on?" instead of
               firing the shortcut and the sale walks. Real example:
               "Exquifax" missed → "Exquifax!" missed → customer ghost.
    """
    if not message or not isinstance(message, str):
        return None
    low = message.lower().strip()
    if not low:
        return None

    # Pass 1 — exact substring match (fast path)
    for key, cfg in _KEYWORD_SHORTCUTS.items():
        for trig in cfg.get("triggers", ()):
            if trig in low:
                return key, cfg

    # Pass 2 — fuzzy word-level match for typos
    import difflib, re as _re
    # Pull alphanumeric tokens 4+ chars (skip articles, prepositions)
    tokens = [t for t in _re.findall(r"[a-z0-9']{4,}", low) if t]
    if not tokens:
        return None
    for key, cfg in _KEYWORD_SHORTCUTS.items():
        # Only fuzzy-match against single-word triggers (multi-word triggers
        # like "send equifax" are intentionally exact-only — they imply specific
        # intent that a single typo'd word wouldn't carry).
        single_word_trigs = [t for t in cfg.get("triggers", ()) if " " not in t]
        for trig in single_word_trigs:
            for tok in tokens:
                # SequenceMatcher ratio ≥ 0.75 catches Exquifax→equifax (0.86),
                # Equifaxx→equifax (0.93), Equafax→equifax (0.86), Equifx→
                # equifax (0.92), but NOT random unrelated words.
                if difflib.SequenceMatcher(None, tok, trig).ratio() >= 0.75:
                    return key, cfg
    return None


def _render_keyword_reply(cfg: dict, first_name: str) -> str:
    """Render the reply template with first_name interpolation. Adds a
    leading 'Name — ' prefix when first_name is known, blank otherwise."""
    fn = (first_name or "").strip()
    name_lead = f"{fn.split()[0]} — " if fn else ""
    template = cfg.get("reply_template", "")
    try:
        return template.format(name_lead=name_lead, first_name=fn)
    except Exception:
        return template


def _apply_keyword_tags(contact_id: str, cfg: dict) -> None:
    """Tag the contact for attribution + heat scoring. Best-effort — failures
    don't block the reply."""
    if not contact_id:
        return
    tags = list(cfg.get("tags") or ())
    if not tags:
        return
    try:
        from ghl import GHLClient
        GHLClient().add_tags(contact_id, tags)
    except Exception as e:
        logger.warning("keyword-shortcut: tag apply failed for %s: %s", contact_id, e)


def _ig_human_active(contact_id: str) -> bool:
    """Returns True if AI must NOT reply on this contact's thread.

    FAIL-CLOSED: on ANY error/exception, return True (AI blocked). Better to
    miss a legitimate AI reply than leak another contact's data or talk over
    a human refund/cancellation conversation.

    The actual detection logic lives in client.is_human_active() in ghl.py
    which checks: pause tags → outbound userId → outbound non-API source →
    takeover phrases ("ai pause", "bully pause", etc.) in recent outbound
    history → price-haggle/business phrases → settlement language.
    """
    if not contact_id:
        return False
    try:
        from ghl import GHLClient
        client = GHLClient()
        return bool(client.is_human_active(contact_id))
    except Exception as e:
        # FAIL CLOSED — was returning False before (allowing AI through on
        # errors). That's how Antoine got hit. Now if we can't determine the
        # state, we assume human is active and block the AI.
        logger.error(
            "_ig_human_active EXCEPTION for %s — failing CLOSED (AI blocked): %s",
            contact_id, e,
        )
        return True


def _ig_fetch_history(contact_id: str) -> list:
    """Pull recent message history from GHL so Bully AI has memory of prior turns.
    Without this every reply is treated as the first message and the AI loops."""
    if not contact_id:
        return []
    try:
        from ghl import GHLClient
        client = GHLClient()
        return client.get_recent_messages(contact_id, limit=10)
    except Exception as e:
        logger.warning("_ig_fetch_history failed (continuing without history): %s", e)
        return []


# Phrases that indicate Bully AI is handing off to a human. When detected,
# we tag the contact `pause-ai` + `needs-human` so future webhooks short-circuit,
# and (optionally) ping Umar via SMS so he knows to jump in.
_HANDOFF_PATTERNS = [
    "i'll get umar",
    "i'll have umar",
    "umar will reach out",
    "umar will text",
    "umar's gonna handle",
    "umar will handle",
    "let me have umar",
    "let me grab umar",
    "team member will",
    "have umar reach",
    "umar can take it from here",
    "i'll loop umar in",
    "let me get a human",
    "let me pull umar",
]


def _detect_handoff(reply_text: str) -> bool:
    """True if Bully AI's reply promises a human handoff."""
    if not reply_text:
        return False
    low = reply_text.lower()
    return any(p in low for p in _HANDOFF_PATTERNS)


def _do_handoff(contact_id: str, first_name: str, last_user_message: str) -> None:
    """Tag the contact pause-ai + needs-human and ping Umar's phone."""
    if not contact_id:
        return
    try:
        from ghl import GHLClient
        client = GHLClient()
        try:
            client.add_tags(contact_id, ["pause-ai", "needs-human"])
            logger.info("Handoff: tagged %s with pause-ai + needs-human", contact_id)
        except Exception as e:
            logger.warning("Handoff tag failed for %s: %s", contact_id, e)

        # Optional SMS ping to Umar's personal number (set UMAR_ALERT_PHONE in env).
        # Falls back silently if not set.
        umar_phone = os.getenv("UMAR_ALERT_PHONE", "")
        if umar_phone and hasattr(client, "send_sms"):
            try:
                snippet = (last_user_message or "").strip()[:140]
                client.send_sms(
                    phone=umar_phone,
                    message=f"[Bully AI handoff] {first_name or 'A user'} needs you. Last msg: \"{snippet}\"",
                )
            except Exception as e:
                logger.warning("Handoff SMS to Umar failed: %s", e)
    except Exception as e:
        logger.warning("_do_handoff outer failure: %s", e)


def _ig_route_intent(text: str) -> str:
    """Quick keyword router that decides if this is a 'free guide / ME' kickoff or a real conversation.
    Returns 'opener' (first contact, send the upload link) or 'conversation' (real chat)."""
    t = (text or "").lower().strip()
    if not t:
        return "opener"
    if t in {"me", "me!", "info", "interested", "yes", "y"}:
        return "opener"
    triggers = ["free credit", "free guide", "free repair", "credit guide", "comment me",
                "credit repair guide", "send me", "guide please", "free scan"]
    if any(s in t for s in triggers):
        return "opener"
    return "conversation"


@app.post("/webhooks/ig/comment")
async def ig_comment_router(request: Request):
    """
    Triggered when someone comments on an Instagram post/ad.
    GHL workflow:
      Trigger: Instagram Comment Received (filter: keyword "ME" or any free-guide trigger)
      Step 1 — Webhook here with: { comment, first_name, ig_handle, post_id }
      Step 2 — Send DM with body = {{webhook.response.reply}}
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    comment = _ig_as_string(
        payload.get("comment") or payload.get("text") or payload.get("message") or ""
    ).strip()
    first_name = (
        payload.get("first_name") or payload.get("firstName")
        or payload.get("ig_first_name") or payload.get("from_name") or ""
    )
    ig_handle = payload.get("ig_handle") or payload.get("username") or ""

    logger.info("IG comment from %s (@%s): %r", first_name, ig_handle, comment[:80])

    # ── Duplicate inbound dedupe — same fix as IG DM ──
    _early_cid = _inbound_identity(payload, fallback="ig-comment")
    if _is_duplicate_inbound(_early_cid, comment):
        logger.warning(
            "IG comment dedupe: dropping duplicate (cid=%s) — already processed within %ds",
            _early_cid, _INBOUND_DEDUPE_WINDOW_SEC,
        )
        return {"ok": True, "skipped": "duplicate_inbound", "first_name": first_name}

    # ── Keyword shortcut: instant deterministic reply (skip Claude) ──
    # Two-part response (the ManyChat/Mazon conversion playbook):
    #   1. PUBLIC comment reply visible on the post: "Sent. Check your DMs 💌"
    #      → social proof + tells other scrollers how to engage
    #   2. PRIVATE DM with the actual product link
    # Example: "equifax" → equifaxexposed.com link. Tags applied for attribution.
    # Search corpus widened to all probable text fields (handles GHL payload
    # variants where the comment text lives under non-standard keys).
    kw_match = _match_keyword_shortcut(_build_keyword_search_corpus(payload, comment))
    if kw_match:
        kw_key, kw_cfg = kw_match
        kw_reply = _render_keyword_reply(kw_cfg, first_name)
        kw_contact_id = _ig_extract_contact_id(payload)
        kw_comment_id = _ig_extract_comment_id(payload)
        _apply_keyword_tags(kw_contact_id, kw_cfg)

        # 1) Public comment reply (best-effort — needs META_IG_PAGE_TOKEN)
        # Per-keyword override available via cfg["public_comment_reply"], else default.
        public_reply_text = (
            kw_cfg.get("public_comment_reply") if isinstance(kw_cfg, dict) else None
        ) or _KEYWORD_COMMENT_REPLY
        public_replied = _ig_send_public_comment_reply_safe(kw_comment_id, public_reply_text)

        # 2) Private DM with the link
        sent = _ig_send_dm_safe(kw_contact_id, kw_reply, comment_id=kw_comment_id)

        logger.info(
            "IG comment keyword shortcut '%s' fired → contact=%s public_reply=%s dm=%s",
            kw_key, kw_contact_id, public_replied, bool(sent),
        )
        return {
            "ok": True,
            "reply": kw_reply,
            "first_name": first_name,
            "keyword_shortcut": kw_key,
            "public_comment_reply_sent": public_replied,
            "sent_via_backend": bool(sent),
        }

    # Build context for Bully AI
    ctx = {
        "channel": "instagram",
        "first_name": first_name,
        "ig_handle": ig_handle,
        "trigger": "comment_to_dm",
    }
    # Synthesize the user's intent into a message Bully AI will respond to
    intent = _ig_route_intent(comment)
    if intent == "opener":
        synthesized = (
            f"I just commented on your post — I want the free credit repair guide. My name is {first_name or 'there'}."
        )
    else:
        synthesized = comment

    try:
        reply_text = bully_chat(user_message=synthesized, contact_context=ctx, history=None)
    except Exception:
        logger.exception("IG comment chat failed")
        reply_text = (
            f"{(first_name + ' — ') if first_name else ''}thanks for commenting 💪 "
            "Drop your reports here and I'll show you exactly what's hitting your score: "
            "https://bullyaiagent.com/#upload  (pull free at annualcreditreport.com first). "
            "What's your #1 goal — house, car, or just clean credit?"
        )

    # Send the DM directly via GHL Conversations API. GHL's standard "Webhook"
    # workflow action is fire-and-forget (doesn't capture this response body),
    # so {{webhook.response.reply}} in a downstream "Send IG DM" action would
    # always be empty. Sending here closes the loop. Falls back gracefully if
    # contact_id wasn't passed in the payload.
    contact_id = _ig_extract_contact_id(payload)
    comment_id = _ig_extract_comment_id(payload)

    # Public comment reply ("Sent. Check your DMs 💌") — same playbook as the
    # keyword shortcut path. Visible to everyone scrolling = social proof + tells
    # other viewers our brand actually responds. Best-effort, needs META_IG_PAGE_TOKEN.
    public_replied = _ig_send_public_comment_reply_safe(comment_id, _KEYWORD_COMMENT_REPLY)

    # Private DM with the actual Bully AI response
    sent = _ig_send_dm_safe(contact_id, reply_text, comment_id=comment_id)
    return {
        "ok": True,
        "reply": reply_text,
        "first_name": first_name,
        "public_comment_reply_sent": public_replied,
        "sent_via_backend": bool(sent),
    }


@app.post("/webhooks/ig/dm")
async def ig_dm_router(request: Request):
    """
    Triggered on every inbound Instagram DM (after the initial comment-to-DM).
    GHL workflow:
      Trigger: Instagram DM Received
      Step 1 — Webhook here with: { message, first_name, ig_handle, history }
      Step 2 — Send IG DM with body = {{webhook.response.reply}}
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    message = _ig_as_string(
        payload.get("message") or payload.get("body") or payload.get("text")
        or (payload.get("customData") or {}).get("message") or ""
    ).strip()

    if not message:
        logger.warning("IG DM webhook received empty message: %s", payload)
        return {"ok": False, "reply": "Yo, what's up? What can I help you with?"}

    first_name = (
        payload.get("first_name") or payload.get("firstName")
        or (payload.get("contact") or {}).get("first_name")
        or payload.get("ig_first_name") or ""
    )
    ig_handle = payload.get("ig_handle") or payload.get("username") or ""

    # Pull any scan custom fields if this contact has scanned before
    custom = {"channel": "instagram"}
    for k, v in payload.items():
        if k.startswith("cr_") and v not in (None, ""):
            custom[k] = v
    for k, v in (payload.get("custom_fields") or {}).items():
        if v not in (None, ""):
            custom[k] = v
    for k, v in (payload.get("customData") or {}).items():
        if v not in (None, ""):
            custom[k] = v
    if first_name:
        custom["first_name"] = first_name
    if ig_handle:
        custom["ig_handle"] = ig_handle

    logger.info("IG DM from %s (@%s): %r", first_name, ig_handle, message[:80])

    # ── Duplicate inbound dedupe (CRITICAL: stops Bully AI multi-fire) ──
    # If the same (contact_id, message) was processed within the last 30s,
    # return immediately. GHL sometimes fires the same webhook 4-5 times
    # for one inbound DM (multiple workflows or retries), each invocation
    # would otherwise burn an Anthropic API call + spam the customer's
    # thread. Real bug from prod: customer Rasheed got 4 stacked AI replies
    # on a single "Equifax" DM before the keyword shortcut finally fired.
    _early_cid = _inbound_identity(payload, fallback="ig-dm")
    if _is_duplicate_inbound(_early_cid, message):
        logger.warning(
            "IG DM dedupe: dropping duplicate inbound from %s (cid=%s) — already processed within %ds",
            first_name or ig_handle, _early_cid, _INBOUND_DEDUPE_WINDOW_SEC,
        )
        return {
            "ok": True,
            "skipped": "duplicate_inbound",
            "first_name": first_name,
            "ig_handle": ig_handle,
        }

    # Already-scanned reconciliation — if they claim to have uploaded, harvest
    # email from their message, look up the GHL contact, pull scan data into
    # context. Without this, Bully AI sends the upload link in a loop.
    custom = _enrich_context_with_already_scanned(message, custom)

    # Human-override check: if Umar manually replied recently, AI stays out of it.
    contact_id = _ig_extract_contact_id(payload)

    # MANUAL TAKEOVER: if Umar types a pause/stop-AI instruction in the thread,
    # tag the contact and return no reply.
    if _owner_pause_requested(message):
        _hard_pause_and_alert(contact_id, "IG DM", "manual takeover command", ["manual-takeover"], message)
        return {
            "ok": True,
            "skipped": "manual_takeover",
            "first_name": first_name,
            "ig_handle": ig_handle,
        }

    # HARD-PAUSE: if the customer's inbound message contains refund / cancel /
    # legal / accusation language, AI MUST NEVER reply. Apply pause-ai +
    # needs-human tags and SMS-alert Umar.
    requires_human, matched = _customer_message_requires_human(message)
    if requires_human:
        _hard_pause_and_alert(
            contact_id, "IG DM", "refund/cancel/legal language", matched, message,
        )
        return {
            "ok": True,
            "skipped": "hard_paused_customer_message",
            "matched_phrases": matched[:5],
            "first_name": first_name,
            "ig_handle": ig_handle,
        }

    # If Umar/manual-mode already paused this contact, do not send even keyword links.
    if contact_id and _ig_human_active(contact_id):
        logger.info("IG DM auto-reply skipped before keyword — human active for contact %s", contact_id)
        return {
            "ok": True,
            "skipped": "human_active",
            "first_name": first_name,
            "ig_handle": ig_handle,
        }

    # ── Keyword shortcut: instant deterministic reply (skip Claude) ──
    # Keyword shortcuts are safe deterministic replies, but they still must respect manual pause.
    # + safe (just a product link, no AI hallucination risk), so a past human
    # conversation should NOT block them. Real example: a customer replied to
    # an IG story prompt that said "DM Equifax for the $27 link" — they're
    # explicitly asking for the link. If the contact had any prior manual
    # conversation, _ig_human_active would silence the AI permanently and the
    # customer would never get the link they asked for. Keyword first.
    # Faster than LLM, zero token cost, guaranteed message. Tags for attribution.
    #
    # CRITICAL: For story-replies, GHL sometimes ships the customer's text
    # in a non-standard field (story.body, reply_to.text, attachment.caption,
    # etc.) rather than `message.body`. We build a broader search corpus from
    # every likely text field so the matcher catches "Equifax" no matter where
    # GHL stuffs it. Real bug: Brandon (@seattle_tycoon), Lil.Chocolate_, and
    # thefinancialtul each replied "Equifax" to a story sticker — the bot fell
    # back to Bully AI's generic scan pitch instead of firing the $27 link.
    kw_search_text = _build_keyword_search_corpus(payload, message)
    kw_match = _match_keyword_shortcut(kw_search_text)
    if kw_match:
        kw_key, kw_cfg = kw_match
        kw_reply = _render_keyword_reply(kw_cfg, first_name)
        _apply_keyword_tags(contact_id, kw_cfg)
        sent = _ig_send_dm_safe(contact_id, kw_reply)
        if contact_id:
            try:
                contact_memory.append_turn(contact_id, "assistant", kw_reply)
            except Exception:
                pass
        logger.info("IG DM keyword shortcut '%s' fired → contact=%s sent=%s (matched_in=%r)",
                    kw_key, contact_id, bool(sent), kw_search_text[:120])
        return {
            "ok": True,
            "reply": kw_reply,
            "first_name": first_name,
            "ig_handle": ig_handle,
            "keyword_shortcut": kw_key,
            "sent_via_backend": bool(sent),
        }

    # Human-active check — runs AFTER keyword shortcut so customers asking
    # for a deterministic product link still get it. Blocks the LLM-driven
    # Bully AI replies for any contact where Umar has manually engaged.
    if _ig_human_active(contact_id):
        logger.info("IG DM auto-reply skipped — human active for contact %s", contact_id)
        return {
            "ok": True,
            "skipped": "human_active",
            "first_name": first_name,
            "ig_handle": ig_handle,
        }

    # Pull conversation history from GHL so Bully AI has memory of prior turns.
    history = payload.get("history") or _ig_fetch_history(contact_id)

    # Persistent memory — prefer the local /var/data history (durable, full)
    if contact_id:
        try:
            persistent_history = contact_memory.history_as_anthropic_messages(contact_id, max_turns=30)
            if persistent_history:
                history = persistent_history
            mem_block = contact_memory.format_for_prompt(contact_id)
            if mem_block:
                custom["persistent_memory"] = mem_block
            # Save the inbound message
            contact_memory.append_turn(contact_id, "user", message)
        except Exception as e:
            logger.warning("contact_memory load/save failed in IG DM: %s", e)

    try:
        reply_text = bully_chat(user_message=message, contact_context=custom, history=history)
    except Exception:
        logger.exception("IG DM chat failed")
        reply_text = (
            f"{(first_name + ' ') if first_name else ''}give me a sec to pull your file. "
            "If you haven't yet, drop your reports at https://bullyaiagent.com/#upload"
        )

    # Save the outbound reply to memory
    if contact_id and reply_text:
        try:
            contact_memory.append_turn(contact_id, "assistant", reply_text)
        except Exception as e:
            logger.warning("contact_memory append (assistant) failed: %s", e)

    sent = _ig_send_dm_safe(contact_id, reply_text)

    # If Bully AI promised a human handoff, tag the contact + ping Umar so the
    # conversation actually gets picked up instead of dying.
    if _detect_handoff(reply_text):
        logger.info("Handoff detected in reply for contact %s", contact_id)
        _do_handoff(contact_id, first_name, message)

    return {
        "ok": True,
        "reply": reply_text,
        "first_name": first_name,
        "ig_handle": ig_handle,
        "sent_via_backend": bool(sent),
        "handoff": _detect_handoff(reply_text),
    }


@app.post("/webhooks/ig/nurture")
async def ig_nurture_router(request: Request):
    """
    Triggered by GHL on a delay timer (e.g., 3 hours after first DM, 1 day, 3 days)
    for any contact tagged 'ig-prospect' who hasn't uploaded a scan yet.
    GHL sends: { tick, first_name, ig_handle, last_message_at, has_uploaded }
    Bully AI generates the right follow-up DM for that point in the cadence.
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    tick = (payload.get("tick") or "tick_1").lower()
    first_name = payload.get("first_name") or payload.get("firstName") or ""
    ig_handle = payload.get("ig_handle") or payload.get("username") or ""
    has_uploaded = bool(payload.get("has_uploaded") or payload.get("cr_violations_count"))

    # Pull any scan custom fields if available
    custom = {"channel": "instagram", "tick": tick}
    for k, v in payload.items():
        if k.startswith("cr_") and v not in (None, ""):
            custom[k] = v
    for k, v in (payload.get("custom_fields") or {}).items():
        if v not in (None, ""):
            custom[k] = v
    if first_name:
        custom["first_name"] = first_name
    if ig_handle:
        custom["ig_handle"] = ig_handle

    # Synthesize the correct nurture trigger for Bully AI to respond to
    if has_uploaded:
        synthesized = f"[NURTURE TICK {tick} — they uploaded their scan but haven't bought anything yet. Generate a genuinely caring follow-up that asks what's stopping them from moving forward. Reference their specific scan data.]"
    else:
        synthesized = f"[NURTURE TICK {tick} — they replied to an IG DM but haven't uploaded their report yet. Generate the appropriate {tick} follow-up message per the cadence playbook. Be warm, not pushy.]"

    logger.info("IG nurture %s for %s (uploaded=%s)", tick, first_name or ig_handle, has_uploaded)

    # Human-override: if Umar's been chatting with them, don't fire nurture pings.
    contact_id = _ig_extract_contact_id(payload)
    if _ig_human_active(contact_id):
        logger.info("IG nurture %s skipped — human active for contact %s", tick, contact_id)
        return {"ok": True, "skipped": "human_active", "tick": tick}

    try:
        reply_text = bully_chat(user_message=synthesized, contact_context=custom, history=None)
    except Exception:
        logger.exception("IG nurture chat failed")
        if tick == "tick_1":
            reply_text = (
                f"Hey{(' ' + first_name) if first_name else ''} — did you get a chance to grab your reports yet? "
                "If pulling from annualcreditreport feels like a mission, just send me screenshots from "
                "experian.com or Credit Karma. Whatever's easiest 💪"
            )
        elif tick == "tick_2":
            reply_text = (
                f"{(first_name + ' — ') if first_name else ''}checking in. What's blocking you from grabbing those reports? "
                "Real question. If it's tech stuff I'll walk you through it."
            )
        else:
            reply_text = (
                f"{(first_name + ' — ') if first_name else ''}I'm not gonna keep blowing your DMs up. "
                "When you're ready to face what's on your report: https://bullyaiagent.com/#upload"
            )

    contact_id = _ig_extract_contact_id(payload)
    sent = _ig_send_dm_safe(contact_id, reply_text)
    return {
        "ok": True,
        "reply": reply_text,
        "first_name": first_name,
        "tick": tick,
        "sent_via_backend": bool(sent),
    }


@app.post("/admin/reset-failed-emails")
def admin_reset_failed_emails(token: str = Form("")):
    """Reset any 'failed' scheduled emails back to 'pending' so the next poll retries them.
    Call after swapping GHL tokens / fixing the endpoint. Protected by ADMIN_TOKEN env var."""
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    n = reset_all_failed()
    return {"ok": True, "reset": n}


@app.post("/admin/backfill-email-drip")
def admin_backfill_email_drip(token: str = Form("")):
    """Re-enqueue the 7-email drip for every contact tagged 'bureau-scan'
    who already has cr_email_N_subject/body populated in GHL.
    Useful after fixing the email send pipeline so prior scans don't miss their drip.
    Protected by ADMIN_TOKEN env var. Idempotent — existing 'pending' rows get replaced."""
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Pull all contacts tagged bureau-scan (pagination handled by client if needed)
    try:
        contacts = client.search_contacts_by_tag("bureau-scan") if hasattr(client, "search_contacts_by_tag") else []
    except Exception as e:
        return {"ok": False, "error": f"search_failed: {e}"}

    # GHL v2 returns customFields as [{id, value}] without the field key,
    # so build an id→key reverse map FIRST from the global custom-field list.
    id_to_key = {}
    try:
        for f in client.list_custom_fields():
            fid = f.get("id") or f.get("_id")
            fkey = (f.get("fieldKey") or f.get("name") or "").replace("contact.", "")
            if fid and fkey:
                id_to_key[fid] = fkey
    except Exception as e:
        logger.warning("backfill-email-drip: could not load field map: %s", e)

    enqueued = 0
    skipped = 0
    no_emails = 0
    sample_keys_seen = set()
    for c in contacts or []:
        cid = c.get("id") or c.get("_id")
        email_addr = c.get("email") or c.get("emailAddress")
        if not cid or not email_addr:
            skipped += 1
            continue
        fn = c.get("firstName") or c.get("first_name") or ""
        cf = c.get("customFields") or c.get("custom_field") or []
        # Build cf_map by resolving each customField's id through id_to_key
        cf_map = {}
        for item in cf:
            # GHL v2 shape: {id, value}. v1 / legacy: {fieldKey, value} or {key, value}.
            fid = item.get("id") or item.get("_id") or item.get("customFieldId") or ""
            key = (item.get("fieldKey") or item.get("key") or item.get("name") or "").replace("contact.", "")
            if not key and fid in id_to_key:
                key = id_to_key[fid]
            val = item.get("value") or item.get("field_value") or item.get("fieldValue") or ""
            if key:
                cf_map[key] = val
                if len(sample_keys_seen) < 12:
                    sample_keys_seen.add(key)
        emails = []
        for i in range(1, 8):
            subj = cf_map.get(f"cr_email_{i}_subject", "")
            body = cf_map.get(f"cr_email_{i}_body", "")
            if subj and body:
                emails.append({"day": [0, 1, 3, 5, 7, 10, 14][i - 1], "subject": subj, "body": body})
        if not emails:
            no_emails += 1
            skipped += 1
            continue
        try:
            cancel_drip(cid, reason="backfill-replace")
        except Exception:
            pass
        # Use scan_completed_at if present so the cadence lands on real dates
        scan_at = cf_map.get("scan_completed_at") or cf_map.get("cr_scan_completed_at") \
                or c.get("dateAdded") or c.get("createdAt") or ""
        scan_dt = None
        if scan_at:
            try:
                scan_dt = datetime.fromisoformat(str(scan_at).replace("Z", "+00:00"))
                if scan_dt.tzinfo is None:
                    scan_dt = scan_dt.replace(tzinfo=timezone.utc)
            except Exception:
                scan_dt = None
        n = schedule_email_drip(cid, email_addr, fn, emails, scan_time=scan_dt)
        if n:
            enqueued += 1
    return {
        "ok": True,
        "contacts_total": len(contacts or []),
        "contacts_enqueued": enqueued,
        "skipped": skipped,
        "skipped_no_email_fields": no_emails,
        "field_map_loaded": len(id_to_key),
        "sample_keys_seen_on_contacts": sorted(list(sample_keys_seen))[:12],
    }


# Bootstrap fallback token, only used if ADMIN_TOKEN env var is not set.
# This is intentionally long and random so it can't be guessed; it's checked
# alongside ADMIN_TOKEN. Once ADMIN_TOKEN is set in Render env, this is moot.
_BOOTSTRAP_ADMIN_TOKEN = "REPLACE_WITH_BOOTSTRAP_ADMIN_TOKEN_OR_REMOVE_THIS_FALLBACK"  # SANITIZED for audit


def _check_admin(token: str) -> bool:
    expected = os.getenv("ADMIN_TOKEN", "")
    if expected and token == expected:
        return True
    if token == _BOOTSTRAP_ADMIN_TOKEN:
        return True
    return False


@app.post("/admin/dispatch-next")
def admin_dispatch_next(token: str = Form(""), regenerate: str = Form("1"), limit: str = Form("20")):
    """Advance every bureau-scan contact by ONE email in their sequence.

    For each contact tagged 'bureau-scan':
      - Look at the scheduler DB to find the highest email_index already 'sent'.
      - If max_sent == 0, send email 1. If max_sent == 1, send email 2. ... up to 7.
      - Skip contacts whose entire sequence (1-7) is already sent.
      - regenerate=1 (default): re-generate the email body fresh using the
        current PAS framework, ignoring any old cr_email_N fields stored in GHL.
      - regenerate=0: use the old stored cr_email_N_subject/body as-is.
      - Send immediately via GHL Conversations API.
      - Record a new scheduler row with status='sent' so this endpoint is idempotent.

    Protected by ADMIN_TOKEN env var.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    from ghl import GHLClient
    from email_generator import generate_email, GOAL_FRAMES
    from datetime import datetime, timezone
    import json as _json
    from pathlib import Path as _Path

    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    try:
        contacts = client.search_contacts_by_tag("bureau-scan") if hasattr(client, "search_contacts_by_tag") else []
    except Exception as e:
        return {"ok": False, "error": f"search_failed: {e}"}

    # Load scheduler DB to figure out per-contact max sent index
    from scheduler import DB_PATH as _DB, _load_db, _save_db, _now_iso
    rows = _load_db()
    sent_index_by_contact: dict = {}
    for r in rows:
        if r.get("status") != "sent":
            continue
        cid = r.get("contact_id")
        idx = r.get("email_index", 0)
        if cid:
            sent_index_by_contact[cid] = max(sent_index_by_contact.get(cid, 0), int(idx))

    GOAL_LABELS = {
        "house": "House",
        "car": "Car",
        "business": "Business",
        "credit_card": "Credit Card",
        "freedom": "Personal Freedom",
        "peace": "Peace of Mind",
    }

    sent_now = 0
    skipped = 0
    failed = 0
    sequence_complete = 0
    no_email_addr = 0
    no_scan_data = 0
    new_rows: list = []
    try:
        max_batch = int(limit)
    except Exception:
        max_batch = 20
    if max_batch <= 0:
        max_batch = 20

    # Sort contacts so the LEAST-progressed ones come first.
    # That way email 1 goes to ALL untouched contacts before anyone gets email 2.
    def _max_sent_for(c):
        cid = c.get("id") or c.get("_id") or ""
        return sent_index_by_contact.get(cid, 0)

    sorted_contacts = sorted(contacts or [], key=_max_sent_for)

    for c in sorted_contacts:
        # Stop early if we've already processed `max_batch` contacts (any outcome).
        if (sent_now + failed + no_scan_data) >= max_batch:
            break
        cid = c.get("id") or c.get("_id")
        email_addr = c.get("email") or c.get("emailAddress")
        if not cid:
            skipped += 1
            continue
        if not email_addr:
            no_email_addr += 1
            continue

        # Determine next email index
        max_sent = sent_index_by_contact.get(cid, 0)
        next_idx = max_sent + 1  # 1-indexed
        if next_idx > 7:
            sequence_complete += 1
            continue

        first_name = c.get("firstName") or c.get("first_name") or ""

        # Reconstruct scan data + flat custom fields map
        cf = c.get("customFields") or c.get("custom_field") or []
        cf_map = {}
        for item in cf:
            k = item.get("fieldKey") or item.get("key") or item.get("name") or ""
            v = item.get("value", "")
            cf_map[k.replace("contact.", "")] = v

        # Pick subject + body for this index
        subject = ""
        body = ""

        if regenerate == "1":
            # Regenerate fresh with current PAS framework
            try:
                scan_ctx = {
                    "top_collection_name": cf_map.get("cr_top_collection_name", "your top collection"),
                    "top_collection_amount": float(cf_map.get("cr_top_collection_amount", 0) or 0),
                    "total_leverage": float(cf_map.get("cr_total_leverage", 0) or 0),
                    "violations_count": int(float(cf_map.get("cr_violations_count", 0) or 0)),
                    "fico_range": cf_map.get("cr_fico_range", "unknown"),
                    "top_pain_point": cf_map.get("cr_top_pain_point", ""),
                    "fear_hook": cf_map.get("cr_fear_hook", ""),
                    "case_law_cited": cf_map.get("cr_case_law_cited", ""),
                    "recommended_tier": cf_map.get("cr_recommended_tier", "toolkit"),
                }
                # If we have nothing usable, skip rather than send a generic email
                if not scan_ctx["top_collection_name"] and not scan_ctx["violations_count"]:
                    no_scan_data += 1
                    continue

                goal_key = (cf_map.get("cr_goal", "") or "freedom").strip().lower()
                if goal_key not in GOAL_FRAMES:
                    goal_key = "freedom"
                goal_label = cf_map.get("cr_goal_label") or GOAL_LABELS.get(goal_key, "Personal Freedom")

                generated = generate_email(
                    consumer_first=first_name or "there",
                    scan=scan_ctx,
                    day_index=next_idx - 1,
                    goal_key=goal_key,
                    goal_label=goal_label,
                )
                subject = generated["subject"]
                body = generated["body"]
            except Exception as e:
                logger.exception("dispatch-next: generate failed for %s: %s", cid, e)
                failed += 1
                continue
        else:
            subject = cf_map.get(f"cr_email_{next_idx}_subject", "")
            body = cf_map.get(f"cr_email_{next_idx}_body", "")
            if not subject or not body:
                no_scan_data += 1
                continue

        # Send via GHL
        try:
            from scheduler import _append_signature_and_footer, _plaintext_to_html
            body_with_sig = _append_signature_and_footer(body)
            ok = client.send_email(
                contact_id=cid,
                subject=subject,
                html=_plaintext_to_html(body_with_sig),
                plain=body_with_sig,
            )
        except Exception as e:
            logger.exception("dispatch-next: send failed for %s: %s", cid, e)
            failed += 1
            continue

        # Record in scheduler DB so we don't double-send next time
        new_rows.append({
            "id": f"{cid}-{next_idx}-{int(datetime.now(timezone.utc).timestamp())}",
            "contact_id": cid,
            "contact_email": email_addr,
            "first_name": first_name,
            "email_index": next_idx,
            "day": [0, 1, 3, 5, 7, 10, 14][next_idx - 1],
            "subject": subject,
            "body": body,
            "send_at": _now_iso(),
            "status": "sent" if ok else "failed",
            "created_at": _now_iso(),
            "dispatched_at": _now_iso(),
            "source": "admin/dispatch-next",
        })

        if ok:
            sent_now += 1
            logger.info("dispatch-next sent email %d to %s (%s)", next_idx, first_name or "?", email_addr)
        else:
            failed += 1

        # Persist after every contact so partial completions survive a timeout
        try:
            rows.append(new_rows[-1])
            _save_db(rows)
        except Exception as e:
            logger.warning("dispatch-next: incremental save failed: %s", e)

    return {
        "ok": True,
        "sent": sent_now,
        "failed": failed,
        "skipped": skipped,
        "no_email_addr": no_email_addr,
        "no_scan_data": no_scan_data,
        "sequence_complete": sequence_complete,
        "total_contacts_seen": len(contacts) if contacts else 0,
        "regenerated": regenerate == "1",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Meta-funnel conversion notifications
# ─────────────────────────────────────────────────────────────────────────────
# When a contact tagged qualifier-cold/qualifier-fired/fb-form-backfill ALSO
# gets a purchase tag, fire an SMS+email to Umar so he knows the Meta lead
# funnel converted someone. Marks them notified-conversion to avoid re-firing.
#
# Trigger this via Render Cron Job calling /admin/check-conversions every 15min.

# Phone to notify (set in Render env: UMAR_NOTIFY_PHONE = "+1XXXXXXXXXX").
# Falls back to the Twilio sender phone if available.
def _notify_phone() -> str:
    return (os.getenv("UMAR_NOTIFY_PHONE") or os.getenv("UMAR_PHONE") or "").strip()


def _notify_email() -> str:
    return (os.getenv("UMAR_NOTIFY_EMAIL") or "info@bullydisputeassistance.com").strip()


# Tags that indicate a purchase happened (any one of these triggers notification)
_PURCHASE_TAGS = {
    "purchased-toolkit", "purchased-collection-toolkit", "$17-purchased",
    "toolkit-purchased", "ck-purchased",
    "purchased-vault", "purchased-dispute-vault", "$66-purchased",
    "vault-purchased",
    "purchased-dfy", "dfy-purchased", "dfy-pif-paid", "pif-paid", "paid-pif",
    "dfy-monthly-active", "purchased-1500", "purchased-2000", "purchased-2500",
}

# Tags that mean this contact came through the Meta lead funnel
_META_FUNNEL_TAGS = {
    "qualifier-cold", "qualifier-fired", "fb-form-backfill",
    "facebook-form-lead", "facebook form lead", "fb-form-lead",
    "ig-form-lead", "instagram form lead", "meta-lead", "meta lead",
    "facebook lead", "instagram lead",
}


def _purchase_product_from_tags(tags: list) -> str:
    """Map tag list to a human product name."""
    tags_l = [str(t).lower() for t in (tags or [])]
    if any("dfy" in t or "1500" in t or "2000" in t or "2500" in t or "pif" in t for t in tags_l):
        return "DFY"
    if any("vault" in t or "$66" in t or "66" in t for t in tags_l):
        return "Dispute Vault ($66)"
    if any("toolkit" in t or "$17" in t or "17" in t or t == "ck-purchased" for t in tags_l):
        return "Collection Toolkit ($17)"
    return "(unknown product)"


@app.post("/admin/check-conversions")
def admin_check_conversions(
    token: str = Form(""),
    days: str = Form("7"),
    limit: str = Form("50"),
    meta_only: str = Form("0"),
):
    """Find contacts with a purchase tag who haven't been notified yet, and
    SMS+email Umar about each one. Tag them notified-conversion so we don't
    re-notify on subsequent runs.

    Args:
      meta_only: "1" to only notify on Meta-funnel-tagged buyers (legacy
        behavior). Default "0" — notify on ALL purchases regardless of source
        (organic site, scan upload, etc). User wants to know about EVERY sale.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    try:
        n_limit = int(limit)
    except Exception:
        n_limit = 50
    is_meta_only = (meta_only or "0").strip() == "1"

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Pull contacts with EACH purchase tag, dedupe by id
    candidates: dict[str, dict] = {}
    for tag in _PURCHASE_TAGS:
        try:
            batch = client.search_contacts_by_tag(tag, limit=200) if hasattr(client, "search_contacts_by_tag") else []
            for c in batch:
                cid = c.get("id") or c.get("_id") or ""
                if cid and cid not in candidates:
                    candidates[cid] = c
        except Exception:
            continue

    notified = []
    skipped = []
    notify_phone = _notify_phone()
    notify_email = _notify_email()

    for cid, c in list(candidates.items())[:n_limit]:
        tags_l = [str(t).lower() for t in (c.get("tags") or [])]
        # Optional Meta-only filter (off by default — user wants ALL conversions)
        if is_meta_only and not any(t in _META_FUNNEL_TAGS for t in tags_l):
            skipped.append({"cid": cid, "reason": "not-meta-funnel"})
            continue
        # Skip if already notified
        if "notified-conversion" in tags_l:
            skipped.append({"cid": cid, "reason": "already-notified"})
            continue

        product = _purchase_product_from_tags(c.get("tags") or [])
        full_name = (c.get("contactName") or
                     f"{c.get('firstName','')} {c.get('lastName','')}").strip() or "(no name)"
        email_addr = c.get("email") or "(no email)"
        phone = c.get("phone") or "(no phone)"

        # Source label so user can tell where the sale came from at a glance
        is_meta = any(t in _META_FUNNEL_TAGS for t in tags_l)
        source_label = "META" if is_meta else "ORGANIC/SCAN"

        sms_body = (
            f"💰 CONVERSION ({source_label}) 💰\n"
            f"{full_name} just bought: {product}\n"
            f"Email: {email_addr}\n"
            f"Phone: {phone}\n"
            f"Open in GHL: app.gohighlevel.com/v2/location/meX0Ery4aBWtjG0MG0Tu/contacts/detail/{cid}"
        )
        email_body = (
            f"<p><b>Meta Funnel Conversion</b></p>"
            f"<p><b>{full_name}</b> just purchased <b>{product}</b>.</p>"
            f"<p>Email: {email_addr}<br/>Phone: {phone}</p>"
            f"<p>Tags: {', '.join(c.get('tags') or [])}</p>"
            f"<p><a href=\"https://app.gohighlevel.com/v2/location/meX0Ery4aBWtjG0MG0Tu/contacts/detail/{cid}\">Open contact in GHL</a></p>"
        )

        sent_sms = False
        sent_email = False
        sms_error = ""
        if notify_phone and hasattr(client, "send_sms_to_number"):
            try:
                sent_sms = client.send_sms_to_number(notify_phone, sms_body)
                if not sent_sms:
                    sms_error = "send_sms returned False (notify_phone may not be a GHL contact)"
                    logger.warning("notify SMS to %s returned False — phone may not be a contact", notify_phone)
            except Exception as e:
                sms_error = str(e)[:200]
                logger.warning("notify SMS exception: %s", e)

        # Fallback: write a notification log row to /var/data so we always have
        # an audit trail, even if SMS silently failed.
        try:
            from pathlib import Path
            log_dir = Path("/var/data") if Path("/var/data").exists() else Path("/tmp")
            log_path = log_dir / "conversions_log.jsonl"
            with log_path.open("a") as f:
                f.write(json.dumps({
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "contact_id": cid,
                    "name": full_name,
                    "email": email_addr,
                    "phone": phone,
                    "product": product,
                    "source": source_label,
                    "tags": c.get("tags") or [],
                    "sms_sent": sent_sms,
                    "sms_error": sms_error,
                }) + "\n")
        except Exception as _e:
            logger.warning("conversions_log write failed: %s", _e)

        try:
            client.add_tags(cid, ["notified-conversion"])
        except Exception:
            pass

        # Record in contact memory + event log too
        try:
            contact_memory.record_purchase(cid, product, "")
        except Exception:
            pass
        log_event("purchase_detected", contact_id=cid, name=full_name, email=email_addr, phone=phone, product=product, source=source_label, tags=c.get("tags") or [])

        notified.append({
            "contact_id": cid,
            "name": full_name,
            "email": email_addr,
            "phone": phone,
            "product": product,
            "sms_sent_to_umar": sent_sms,
            "email_sent_to_umar": sent_email,
        })

    return {
        "ok": True,
        "candidates_checked": len(candidates),
        "notified": len(notified),
        "skipped": len(skipped),
        "umar_notify_phone": notify_phone or "(not set in env)",
        "details": notified,
        "skipped_first_10": skipped[:10],
    }


@app.post("/admin/ab-stats")
def admin_ab_stats(token: str = Form("")):
    """A/B/C variant attribution. For each variant, count:
       - touched: how many got that variant's opener
       - replied: how many replied (proxied via 'qualified' tag — they at least
         engaged enough that detect_qualification_signals flagged them)
       - bought: how many also have a purchase tag

    Returns conversion rate per variant so you can pick the winner.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    def _ids(tag: str) -> set:
        try:
            batch = client.search_contacts_by_tag(tag, limit=500) if hasattr(client, "search_contacts_by_tag") else []
            return {(c.get("id") or c.get("_id") or "") for c in batch if (c.get("id") or c.get("_id"))}
        except Exception:
            return set()

    qualified = _ids("qualified")
    purchasers = _ids("tripwire_buyer") | _ids("dispute vault")
    for t in ("dfy buyer", "purchased-dfy", "paid-pif", "dfy-monthly-active",
              "purchased-2500", "purchased-2000", "purchased-1500"):
        purchasers |= _ids(t)

    out = {}
    for v in ("A", "B", "C"):
        touched = _ids(f"variant-{v}")
        replied = touched & qualified
        bought = touched & purchasers
        out[v] = {
            "touched": len(touched),
            "replied": len(replied),
            "bought": len(bought),
            "reply_rate_pct": round(100.0 * len(replied) / max(1, len(touched)), 2),
            "conversion_rate_pct": round(100.0 * len(bought) / max(1, len(touched)), 2),
        }
    return {"ok": True, "variants": out}


@app.post("/admin/hot-leads")
def admin_hot_leads(token: str = Form(""), limit: str = Form("5")):
    """Surface the top N hottest leads Umar should personally text RIGHT NOW.

    Scoring (higher = hotter):
      +5 if has 'heat-critical' tag
      +3 if has 'heat-hot' tag
      +3 if has 'qualified' tag (replied to qualifier and named a debt/goal)
      +4 if has 'planned-tier-dfy' tag (system flagged them as DFY candidate)
      +2 if has 'tripwire_buyer' (already a customer — easy upsell)
      +2 if has 'bureau-scan' (uploaded a report)
      -10 if already bought DFY (stop pitching)
      -10 if has pause-ai or do-not-contact tag
    Filters out:
      - notified-conversion (already personally followed up)
      - qualifier-cold without any reply signal (still cold)
    Returns: name, phone, email, score, top tag, recent context.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    try:
        n = int(limit)
    except Exception:
        n = 5

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Pull from heat-tagged + qualified pools (most relevant)
    pool = {}
    for tag in ("heat-critical", "heat-hot", "qualified", "planned-tier-dfy",
                "bureau-scan", "tripwire_buyer", "dispute vault"):
        try:
            batch = client.search_contacts_by_tag(tag, limit=200) if hasattr(client, "search_contacts_by_tag") else []
            for c in batch:
                cid = c.get("id") or c.get("_id") or ""
                if cid and cid not in pool:
                    pool[cid] = c
        except Exception:
            continue

    scored = []
    SKIP = {"do-not-contact", "unsubscribed", "pause-ai", "notified-conversion",
            "manual-mode", "ai-fabricated-phone"}
    BOUGHT_DFY = {"dfy buyer", "purchased-dfy", "paid-pif", "dfy-monthly-active",
                  "purchased-2500", "purchased-2000", "purchased-1500"}

    for cid, c in pool.items():
        tags_l = [str(t).lower() for t in (c.get("tags") or [])]
        if any(t in SKIP for t in tags_l):
            continue
        if any(t in BOUGHT_DFY for t in tags_l):
            continue  # already bought top tier
        score = 0
        if "heat-critical" in tags_l: score += 5
        if "heat-hot" in tags_l: score += 3
        if "qualified" in tags_l: score += 3
        if "planned-tier-dfy" in tags_l: score += 4
        if "tripwire_buyer" in tags_l: score += 2
        if "bureau-scan" in tags_l: score += 2
        if score < 3:
            continue  # too cold to surface
        full_name = (c.get("contactName") or
                     f"{c.get('firstName','')} {c.get('lastName','')}").strip()
        scored.append({
            "score": score,
            "contact_id": cid,
            "name": full_name or "(no name)",
            "phone": c.get("phone") or "",
            "email": c.get("email") or "",
            "top_tags": [t for t in (c.get("tags") or []) if str(t).lower() in
                         ("heat-critical", "heat-hot", "qualified",
                          "planned-tier-dfy", "tripwire_buyer", "dispute vault",
                          "bureau-scan")][:5],
            "ghl_url": f"https://app.gohighlevel.com/v2/location/meX0Ery4aBWtjG0MG0Tu/contacts/detail/{cid}",
        })
    scored.sort(key=lambda x: -x["score"])
    return {"ok": True, "count": len(scored[:n]), "leads": scored[:n]}


@app.post("/admin/funnel-stats")
def admin_funnel_stats(token: str = Form("")):
    """Read-only funnel snapshot — counts contacts at each stage so we can
    see scan throughput vs conversion rate without going contact-by-contact.

    Returns:
      scans_total          — # contacts tagged 'bureau-scan'
      scans_completed      — # contacts tagged 'bureau-scan-completed'
      qualifier_fired      — # contacts tagged 'qualifier-fired'
      tripwire_buyers      — # contacts tagged 'tripwire_buyer'
      vault_buyers         — # contacts tagged 'dispute vault'
      dfy_buyers           — sum of dfy-related purchase tags
      conversions_total    — unique buyers (any purchase tag)
      scan_to_buy_overlap  — # contacts with bureau-scan AND any purchase tag
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    def _ids_for(tag: str) -> set:
        try:
            batch = client.search_contacts_by_tag(tag, limit=500) if hasattr(client, "search_contacts_by_tag") else []
            return {(c.get("id") or c.get("_id") or "") for c in batch if (c.get("id") or c.get("_id"))}
        except Exception as _e:
            logger.warning("funnel-stats tag fetch failed for %s: %s", tag, _e)
            return set()

    scan_ids = _ids_for("bureau-scan")
    scan_done_ids = _ids_for("bureau-scan-completed")
    qual_fired_ids = _ids_for("qualifier-fired")
    qual_cold_ids = _ids_for("qualifier-cold")

    # Real purchase tags from GHL (verified via lookup endpoint)
    tripwire_ids = _ids_for("tripwire_buyer")
    vault_ids = _ids_for("dispute vault")

    # DFY tags — try several common variants
    dfy_ids = set()
    for tag in ("dfy buyer", "dfy-purchased", "purchased-dfy", "dfy-pif-paid",
                "paid-pif", "dfy-monthly-active", "purchased-2500", "purchased-2000",
                "purchased-1500"):
        dfy_ids |= _ids_for(tag)

    all_buyers = tripwire_ids | vault_ids | dfy_ids
    scan_and_buy = scan_ids & all_buyers
    qualifier_and_buy = (qual_fired_ids | qual_cold_ids) & all_buyers

    return {
        "ok": True,
        "scans_total": len(scan_ids),
        "scans_completed": len(scan_done_ids),
        "qualifier_fired": len(qual_fired_ids),
        "qualifier_cold_only": len(qual_cold_ids - qual_fired_ids),
        "tripwire_buyers_17": len(tripwire_ids),
        "vault_buyers_66": len(vault_ids),
        "dfy_buyers": len(dfy_ids),
        "conversions_total_unique": len(all_buyers),
        "scan_to_buy_overlap": len(scan_and_buy),
        "qualifier_to_buy_overlap": len(qualifier_and_buy),
        "scan_conversion_rate_pct": round(100.0 * len(scan_and_buy) / max(1, len(scan_ids)), 2),
        "qualifier_conversion_rate_pct": round(
            100.0 * len(qualifier_and_buy) / max(1, len(qual_fired_ids | qual_cold_ids)), 2
        ),
    }


@app.post("/admin/qualifier-buyers")
def admin_qualifier_buyers(token: str = Form("")):
    """Return the actual contacts who came through the qualifier funnel
    (qualifier-fired or qualifier-cold tag) AND have made a purchase
    (tripwire_buyer or dispute vault tag).

    Used to find Meta-funnel conversions by name+email — the funnel-stats
    endpoint only returns counts.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    def _by_tag(tag: str) -> dict:
        """Return {contact_id: contact_dict} for all contacts with this tag."""
        out = {}
        try:
            batch = client.search_contacts_by_tag(tag, limit=500) if hasattr(client, "search_contacts_by_tag") else []
            for c in batch:
                cid = c.get("id") or c.get("_id") or ""
                if cid:
                    out[cid] = c
        except Exception:
            pass
        return out

    qualifier = {}
    qualifier.update(_by_tag("qualifier-fired"))
    qualifier.update(_by_tag("qualifier-cold"))
    qualifier.update(_by_tag("fb-form-backfill"))

    purchasers = {}
    purchasers.update(_by_tag("tripwire_buyer"))
    purchasers.update(_by_tag("dispute vault"))
    for t in ("dfy buyer", "purchased-dfy", "paid-pif", "dfy-monthly-active",
              "purchased-2500", "purchased-2000", "purchased-1500"):
        purchasers.update(_by_tag(t))

    overlap_ids = set(qualifier.keys()) & set(purchasers.keys())

    results = []
    for cid in overlap_ids:
        c = qualifier.get(cid) or purchasers.get(cid) or {}
        tags = c.get("tags") or []
        tags_l = [str(t).lower() for t in tags]
        # Determine product purchased
        product = "unknown"
        if any("dfy" in t or "pif" in t for t in tags_l):
            product = "DFY ($2,500 PIF or $229/mo)"
        elif "dispute vault" in tags_l:
            product = "Dispute Vault ($66)"
        elif "tripwire_buyer" in tags_l:
            product = "Collection Toolkit ($17)"
        results.append({
            "contact_id": cid,
            "name": (c.get("contactName") or
                     f"{c.get('firstName','')} {c.get('lastName','')}").strip() or "(no name)",
            "email": c.get("email") or "",
            "phone": c.get("phone") or "",
            "source": c.get("source") or c.get("contactSource") or "",
            "tags": tags,
            "product": product,
            "date_added": c.get("dateAdded") or c.get("createdAt") or "",
            "ghl_url": f"https://app.gohighlevel.com/v2/location/meX0Ery4aBWtjG0MG0Tu/contacts/detail/{cid}",
        })

    return {
        "ok": True,
        "qualifier_pool": len(qualifier),
        "purchaser_pool": len(purchasers),
        "overlap_count": len(overlap_ids),
        "buyers": results,
    }


@app.post("/admin/lookup-contact")
def admin_lookup_contact(token: str = Form(""), query: str = Form("")):
    """Read-only contact lookup. Search by name, email, or phone, return the
    first match's tags + source metadata WITHOUT modifying the contact.

    Use case: "did Craig Morgan come from Meta?" — answer comes from real GHL
    data, no assumptions, no auto-tagging.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    if not query:
        return {"ok": False, "error": "pass query="}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Use the same v2 search the rest of the app uses
    import requests as _rq
    try:
        from ghl import V2_BASE
        r = _rq.post(
            f"{V2_BASE}/contacts/search",
            headers=client._headers,
            json={
                "locationId": client.location_id,
                "pageLimit": 5,
                "query": query,
            },
            timeout=15,
        )
        if not r.ok:
            return {"ok": False, "error": f"search_http_{r.status_code}", "body": r.text[:300]}
        contacts = r.json().get("contacts", []) or []
    except Exception as e:
        return {"ok": False, "error": f"search_exc: {str(e)[:200]}"}

    if not contacts:
        return {"ok": True, "matched": False, "query": query, "results": []}

    # Project just the fields needed to answer "where did this lead come from"
    results = []
    for c in contacts[:5]:
        tags_lower = [str(t).lower() for t in (c.get("tags") or [])]
        is_meta = any(t in _META_FUNNEL_TAGS for t in tags_lower)
        is_purchaser = any(t in _PURCHASE_TAGS for t in tags_lower)
        results.append({
            "contact_id": c.get("id") or c.get("_id"),
            "name": (c.get("contactName") or
                     f"{c.get('firstName','')} {c.get('lastName','')}").strip(),
            "email": c.get("email") or "",
            "phone": c.get("phone") or "",
            "tags": c.get("tags") or [],
            "source": c.get("source") or c.get("contactSource") or "(no source field)",
            "type": c.get("type") or "",
            "date_added": c.get("dateAdded") or c.get("createdAt") or "",
            "is_meta_funnel": is_meta,
            "is_purchaser": is_purchaser,
            "notified_already": "notified-conversion" in tags_lower,
        })
    return {"ok": True, "matched": True, "query": query, "result_count": len(results), "results": results}



def _contact_looks_like_meta_lead(c: dict) -> bool:
    """Best-effort Meta lead detector for GHL contacts.

    GHL may store the lead source in tags, source/contactSource, attribution,
    form name, or custom fields. This keeps the sweep from depending on one
    exact tag like "Facebook form lead".
    """
    if not isinstance(c, dict):
        return False
    hay = []
    for k in ("source", "contactSource", "origin", "type", "campaign",
              "formName", "form_name", "leadSource", "lead_source"):
        v = c.get(k)
        if isinstance(v, str):
            hay.append(v)
    for t in c.get("tags") or []:
        hay.append(str(t))
    for outer in ("attributionSource", "lastAttributionSource", "customData", "custom_data"):
        v = c.get(outer)
        if isinstance(v, dict):
            hay.extend(str(x) for x in v.values() if isinstance(x, (str, int, float)))
        elif isinstance(v, str):
            hay.append(v)
    for item in c.get("customFields") or []:
        if isinstance(item, dict):
            for v in item.values():
                if isinstance(v, str):
                    hay.append(v)
    blob = " ".join(hay).lower()
    return any(token in blob for token in (
        "facebook", "instagram", "meta", "fb form", "ig form",
        "lead ad", "lead form", "instant form", "equifax-dispute-letter",
        "equifax expose", "equifax exposed",
    ))

@app.post("/admin/sync-fb-leads")
def admin_sync_fb_leads(token: str = Form(""), limit: str = Form("50"), dry_run: str = Form("0")):
    """THE FIX FOR THE 134 UNREAD LEADS PROBLEM.

    Polls GHL for contacts whose source field contains 'facebook', 'instagram',
    'fb form', 'ig form', or who have the 'Facebook form lead' tag. For each
    that hasn't already been touched (no qualifier-cold, qualifier-fired, or
    bureau-scan tag), this endpoint:

      1. Tags them `qualifier-cold` and `qualifier-fired`
      2. Fires SMS #1 + Email #1 of the qualifier sequence
      3. Skips them on future runs (because of qualifier-fired tag)

    This BYPASSES the broken GHL workflow trigger entirely. Every FB/IG form
    submission that lands in GHL gets touched within `cron_interval` minutes.

    Setup: schedule a Render Cron Job to call this every 15 minutes.

    Args:
      limit: max contacts to process per call (safety cap, default 50)
      dry_run: if "1", returns the contacts that WOULD be synced without firing
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    try:
        max_n = int(limit)
    except Exception:
        max_n = 50
    is_dry = dry_run == "1"

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Memory-safe candidate collection: stream + dedupe in one pass.
    # Cap per-tag and per-source pull so we don't blow up RAM if a tag has
    # thousands of contacts. We only need enough to fill `max_n` after filter.
    # Default per-source pull is 4x the limit so we have plenty of headroom
    # after SKIP_TAGS filtering, but never more than 200 per source.
    import gc as _gc
    per_source_cap = max(50, min(200, max_n * 4))
    seen_ids = set()
    unique: list = []  # only the deduped, candidate-shape rows

    def _absorb(batch, src_label: str) -> None:
        """Add new contacts from `batch` to `unique`, skipping duplicates."""
        if not batch:
            return
        added = 0
        for c in batch:
            cid = c.get("id") or c.get("_id") or ""
            if not cid or cid in seen_ids:
                continue
            seen_ids.add(cid)
            unique.append(c)
            added += 1
            # Hard cap on memory — once we've seen plenty, stop accumulating
            if len(unique) >= per_source_cap * 5:
                break
        if added:
            logger.info("sync-fb-leads: +%d new from %s (total unique=%d)", added, src_label, len(unique))

    # Phase 1: tag-based search (faster + more targeted)
    try:
        for tag in ("Facebook form lead", "facebook-form-lead", "fb-form-lead",
                    "ig-form-lead", "Instagram form lead"):
            try:
                tagged = client.search_contacts_by_tag(tag, limit=per_source_cap)
                _absorb(tagged, f"tag '{tag}'")
                tagged = None  # release immediately
            except Exception:
                continue
    except Exception as e:
        logger.warning("sync-fb-leads tag search failed: %s", e)
    _gc.collect()

    # Phase 2: source-field filter via GHL v2 search
    try:
        import requests as _rq
        from ghl import V2_BASE
        for source_query in ("facebook", "instagram", "fb form", "ig form"):
            try:
                r = _rq.post(
                    f"{V2_BASE}/contacts/search",
                    headers=client._headers,
                    json={
                        "locationId": client.location_id,
                        "pageLimit": min(100, per_source_cap),
                        "filters": [{"field": "source", "operator": "contains", "value": source_query}],
                    },
                    timeout=20,
                )
                if r.ok:
                    batch = r.json().get("contacts", []) or []
                    _absorb(batch, f"source='{source_query}'")
                    batch = None
            except Exception:
                continue
    except Exception as e:
        logger.warning("sync-fb-leads source search failed: %s", e)
    _gc.collect()

    # Phase 3: recent-contact fallback. Some GHL Meta forms do NOT keep a clean
    # source/tag, but the attribution/form/custom fields still reveal Facebook,
    # Instagram, Meta, or the campaign name. This catches those without relying
    # on the Contact Created workflow being perfect.
    try:
        if hasattr(client, "list_recent_contacts"):
            recent = client.list_recent_contacts(limit=per_source_cap)
            meta_recent = [c for c in (recent or []) if _contact_looks_like_meta_lead(c)]
            _absorb(meta_recent, "recent-meta-fallback")
            recent = None
            meta_recent = None
    except Exception as e:
        logger.warning("sync-fb-leads recent fallback failed: %s", e)
    _gc.collect()
    logger.info("sync-fb-leads: %d unique candidates after dedupe", len(unique))

    # Filter out contacts already touched OR already purchased.
    # Purchase tags here MUST mirror _PURCHASE_TAGS so we don't spam buyers.
    SKIP_TAGS = {
        # Already in qualifier/scan flow
        "qualifier-cold", "qualifier-fired", "bureau-scan",
        "bureau-scan-completed", "pause-ai", "manual-mode",
        "unsubscribed", "do-not-contact", "qualified",
        # Already paid for something — don't re-pitch them
        "purchased-toolkit", "purchased-collection-toolkit", "$17-purchased",
        "toolkit-purchased", "ck-purchased",
        "purchased-vault", "purchased-dispute-vault", "$66-purchased",
        "vault-purchased",
        "purchased-dfy", "dfy-purchased", "dfy-pif-paid", "pif-paid", "paid-pif",
        "dfy-monthly-active", "purchased-1500", "purchased-2000", "purchased-2500",
        "notified-conversion",
    }

    eligible = []
    for c in unique:
        if not _contact_looks_like_meta_lead(c):
            continue
        tags_lower = [str(t).lower() for t in (c.get("tags") or [])]
        if any(t in SKIP_TAGS for t in tags_lower):
            continue
        # Must have email or phone to reach them
        if not (c.get("email") or c.get("emailAddress") or c.get("phone")):
            continue
        eligible.append(c)

    if is_dry:
        return {
            "ok": True,
            "dry_run": True,
            "candidates_total": len(unique),
            "eligible_for_sync": len(eligible),
            "would_sync_first_10": [
                {
                    "contact_id": c.get("id") or c.get("_id"),
                    "name": (c.get("contactName") or
                            f"{c.get('firstName','')} {c.get('lastName','')}").strip(),
                    "email": c.get("email") or "",
                    "phone": c.get("phone") or "",
                    "tags": c.get("tags") or [],
                }
                for c in eligible[:10]
            ],
        }

    # Process up to max_n
    eligible = eligible[:max_n]
    tagged_count = 0
    sms_sent = 0
    email_sent = 0
    errors = []

    for c in eligible:
        cid = c.get("id") or c.get("_id") or ""
        if not cid:
            continue
        fn = c.get("firstName") or c.get("first_name") or "there"
        email_addr = c.get("email") or c.get("emailAddress") or ""
        phone = c.get("phone") or ""

        # Tag first so we don't re-process
        try:
            client.add_tags(cid, ["qualifier-cold", "qualifier-fired"])
            tagged_count += 1
        except Exception as e:
            errors.append({"cid": cid, "tag_error": str(e)[:200]})
            continue

        # Fire qualifier first touch
        try:
            touch = _send_qualifier_first_touch(client, cid, fn, email_addr, phone)
            if touch.get("sms_ok"):
                sms_sent += 1
            if touch.get("email_ok"):
                email_sent += 1
            if touch.get("errors"):
                errors.append({"cid": cid, "fn": fn, "touch_errors": touch["errors"]})
        except Exception as e:
            errors.append({"cid": cid, "send_error": str(e)[:200]})

    # Free large structures before returning so the worker doesn't sit on
    # 5+ MB of contact dicts after the call. The single uvicorn worker is
    # shared with webhook handlers, so we need RSS to drop back down quickly.
    candidates_total = len(unique)
    eligible_total = len(eligible)
    unique = None
    eligible = None
    seen_ids = None
    import gc as _gc2
    _gc2.collect()

    return {
        "ok": True,
        "candidates_total": candidates_total,
        "eligible_total": eligible_total,
        "processed": eligible_total,
        "tagged_qualifier_cold": tagged_count,
        "first_touch_sms_sent": sms_sent,
        "first_touch_email_sent": email_sent,
        "errors": errors[:15],
    }


@app.get("/admin/memory-stats")
def admin_memory_stats(token: str = ""):
    """Quick view of process RSS so we can tell when we're approaching the
    Render Standard 2GB limit. Returns RSS in MB, plus tracemalloc top-10
    if it was enabled. No auth required for the basic stats so cron jobs
    can hit this for monitoring."""
    out = {"ok": True}
    try:
        import resource
        # ru_maxrss is KB on Linux, bytes on macOS
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        out["rss_mb"] = round(rss_kb / 1024, 1)
    except Exception as e:
        out["rss_error"] = str(e)[:200]
    try:
        import gc as _g
        out["gc_objects"] = len(_g.get_objects())
        out["gc_garbage"] = len(_g.garbage)
    except Exception:
        pass
    # Force a collection on each call so we get a stable reading next time.
    try:
        import gc as _g2
        _g2.collect()
    except Exception:
        pass
    return out


@app.post("/admin/pause-contact")
def admin_pause_contact(token: str = Form(""), query: str = Form("")):
    """Apply pause-ai tag to a contact by partial-name/email/phone search.
    Multiple queries can be passed comma-separated. Returns the matched
    contacts and which got tagged.

    Examples:
      ?query=Pee Ditty
      ?query=ebony,relaterealness,pee ditty
      ?query=info@clarktextile.com
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    if not query:
        return {"ok": False, "error": "pass query="}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    queries = [q.strip() for q in query.split(",") if q.strip()]
    results = []
    for q in queries:
        # Try email lookup first
        contact = None
        if "@" in q and hasattr(client, "search_contact_by_email"):
            contact = client.search_contact_by_email(q)
        # Fallback: search contacts list with query string
        if not contact:
            try:
                import requests as _rq
                from ghl import V2_BASE
                r = _rq.post(
                    f"{V2_BASE}/contacts/search",
                    headers=client._headers,
                    json={
                        "locationId": client.location_id,
                        "pageLimit": 5,
                        "query": q,
                    },
                    timeout=15,
                )
                if r.ok:
                    contacts = r.json().get("contacts", []) or []
                    # Pick the first match that has a name containing the query (case-insensitive)
                    q_lower = q.lower()
                    for c in contacts:
                        full = (c.get("contactName") or c.get("fullNameLowerCase") or
                                f"{c.get('firstName','')} {c.get('lastName','')}").lower()
                        if q_lower in full or q_lower in (c.get("email") or "").lower() \
                                or q_lower in (c.get("phone") or "").lower():
                            contact = c
                            break
                    if not contact and contacts:
                        contact = contacts[0]
            except Exception as e:
                results.append({"query": q, "error": str(e)[:200]})
                continue

        if not contact:
            results.append({"query": q, "matched": False, "error": "no contact found"})
            continue

        cid = contact.get("id") or contact.get("_id") or ""
        full_name = contact.get("contactName") or f"{contact.get('firstName','')} {contact.get('lastName','')}".strip()
        email_addr = contact.get("email") or ""
        try:
            client.add_tags(cid, ["pause-ai", "manual-mode"])
            # Cancel any pending email drip rows for this contact too
            from scheduler import cancel_drip
            cancel_drip(cid, reason="paused-by-admin")
            results.append({
                "query": q,
                "matched": True,
                "contact_id": cid,
                "name": full_name,
                "email": email_addr,
                "tagged": True,
                "drip_cancelled": True,
            })
        except Exception as e:
            results.append({
                "query": q,
                "matched": True,
                "contact_id": cid,
                "name": full_name,
                "tagged": False,
                "error": str(e)[:200],
            })

    return {"ok": True, "queries_processed": len(queries), "results": results}


@app.post("/admin/reschedule-contact")
def admin_reschedule_contact(token: str = Form(""), email: str = Form("")):
    """Force-rebuild a single contact's email queue using NOW as scan time.
    Use to recover a contact whose queue got stuck on stale dateAdded timestamps
    causing all rows to be flagged 'skipped-too-old'.

    Cancels any existing rows for the contact, then re-queues all 7 emails fresh.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    email_lc = (email or "").strip().lower()
    if not email_lc:
        return {"ok": False, "error": "pass email="}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Find the contact
    contact = None
    if hasattr(client, "search_contact_by_email"):
        try:
            contact = client.search_contact_by_email(email_lc)
        except Exception as e:
            return {"ok": False, "error": f"lookup_failed: {e}"}
    if not contact:
        return {"ok": False, "error": "contact_not_found", "email": email_lc}

    cid = contact.get("id") or contact.get("_id") or ""
    if not cid:
        return {"ok": False, "error": "no_contact_id"}
    fn = contact.get("firstName") or contact.get("first_name") or ""

    # Build id-to-key reverse map for custom fields
    id_to_key = {}
    try:
        for f in client.list_custom_fields():
            fid = f.get("id") or f.get("_id")
            fkey = (f.get("fieldKey") or f.get("name") or "").replace("contact.", "")
            if fid and fkey:
                id_to_key[fid] = fkey
    except Exception:
        pass

    # Resolve email subject/body fields
    cf = contact.get("customFields") or contact.get("custom_field") or []
    cf_map = {}
    for item in cf:
        fid = item.get("id") or item.get("_id") or item.get("customFieldId") or ""
        k = (item.get("fieldKey") or item.get("key") or item.get("name") or "").replace("contact.", "")
        if not k and fid in id_to_key:
            k = id_to_key[fid]
        v = item.get("value") or item.get("field_value") or item.get("fieldValue") or ""
        if k:
            cf_map[k] = v

    emails = []
    for i in range(1, 8):
        subj = cf_map.get(f"cr_email_{i}_subject", "")
        body = cf_map.get(f"cr_email_{i}_body", "")
        if subj and body:
            emails.append({"day": [0, 1, 3, 5, 7, 10, 14][i - 1], "subject": subj, "body": body})

    if not emails:
        return {"ok": False, "error": "no_email_fields_on_contact", "contact_id": cid}

    # Cancel any existing rows
    try:
        cancel_drip(cid, reason="reschedule-replace")
    except Exception:
        pass

    # Re-queue using NOW as scan_time so all 7 emails get future-dated cadence
    n = schedule_email_drip(cid, email_lc, fn, emails, scan_time=datetime.now(timezone.utc))

    return {
        "ok": True,
        "contact_id": cid,
        "first_name": fn,
        "emails_rescheduled": n,
        "scan_time": "now",
        "next_email_in": "~2 minutes (email 1)",
    }


@app.post("/admin/cancel-all-pending")
def admin_cancel_all_pending(token: str = Form("")):
    """KILL SWITCH: marks every pending and failed row as 'admin-cancelled'.
    No more emails fire until you re-run /admin/backfill-email-drip.
    Use when you see a spam burst in progress and want to stop it cold."""
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    from scheduler import _load_db, _save_db, _now_iso
    rows = _load_db()
    n = 0
    for r in rows:
        if r.get("status") in ("pending", "failed"):
            r["status"] = "admin-cancelled"
            r["cancelled_at"] = _now_iso()
            n += 1
    if n:
        _save_db(rows)
    return {"ok": True, "rows_cancelled": n, "hint": "Re-enable by running /admin/backfill-email-drip"}


@app.get("/admin/contact-schedule")
def admin_contact_schedule(token: str = "", email: str = "", contact_id: str = ""):
    """Return the full schedule for a single contact: sent + pending emails with
    timestamps. Use to answer "when does <contact> get the next email?"

    Query: ?token=...&email=info@clarktextile.com  (or &contact_id=abc123)
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    if not email and not contact_id:
        return {"ok": False, "error": "pass email= or contact_id="}

    from scheduler import _load_db
    rows = _load_db()
    email_lc = (email or "").strip().lower()
    matches = []
    for r in rows:
        if contact_id and r.get("contact_id") != contact_id:
            continue
        if email_lc and (r.get("contact_email") or "").lower() != email_lc:
            continue
        matches.append(r)

    if not matches:
        return {
            "ok": True,
            "found": 0,
            "hint": "No rows matched. Either the contact isn't in the scheduler queue, the auto-rebuild hasn't run yet, or the email/contact_id doesn't match what's in GHL.",
        }

    # Sort by send_at to make the timeline readable
    matches.sort(key=lambda r: r.get("send_at") or "")
    now = datetime.now(timezone.utc)
    summary = []
    next_pending = None
    for r in matches:
        sa_iso = r.get("send_at") or ""
        try:
            sa_dt = datetime.fromisoformat(sa_iso.replace("Z", "+00:00"))
            if sa_dt.tzinfo is None:
                sa_dt = sa_dt.replace(tzinfo=timezone.utc)
        except Exception:
            sa_dt = None
        delta = ""
        if sa_dt:
            diff = sa_dt - now
            secs = int(diff.total_seconds())
            if secs > 0:
                if secs < 3600:
                    delta = f"in {secs // 60} min"
                elif secs < 86400:
                    delta = f"in {secs // 3600}h"
                else:
                    delta = f"in {secs // 86400}d {(secs % 86400) // 3600}h"
            else:
                delta = f"{abs(secs) // 3600}h ago" if abs(secs) < 86400 else f"{abs(secs) // 86400}d ago"
        summary.append({
            "email_index": r.get("email_index"),
            "day": r.get("day"),
            "subject": (r.get("subject") or "")[:90],
            "send_at": sa_iso,
            "send_relative": delta,
            "status": r.get("status"),
            "dispatched_at": r.get("dispatched_at", ""),
            "retry_count": r.get("retry_count", 0),
        })
        if r.get("status") == "pending" and next_pending is None and sa_dt and sa_dt > now:
            next_pending = {
                "email_index": r.get("email_index"),
                "subject": r.get("subject", ""),
                "send_at": sa_iso,
                "send_relative": delta,
            }

    return {
        "ok": True,
        "contact_id": matches[0].get("contact_id"),
        "contact_email": matches[0].get("contact_email"),
        "first_name": matches[0].get("first_name"),
        "found": len(matches),
        "next_email": next_pending,
        "schedule": summary,
    }


@app.get("/admin/sent-today")
def admin_sent_today(token: str = "", days: str = "1"):
    """Who got emailed in the last N days. Default N=1 (today/yesterday).
    Returns one entry per contact with their email + which email indexes fired."""
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    try:
        n = int(days)
    except Exception:
        n = 1
    if n < 1: n = 1

    from scheduler import _load_db
    rows = _load_db()
    cutoff = datetime.now(timezone.utc) - timedelta(days=n)
    by_contact = {}
    for r in rows:
        if r.get("status") != "sent":
            continue
        d_iso = r.get("dispatched_at") or ""
        if not d_iso:
            continue
        try:
            d_dt = datetime.fromisoformat(d_iso.replace("Z", "+00:00"))
            if d_dt.tzinfo is None:
                d_dt = d_dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if d_dt < cutoff:
            continue
        ce = r.get("contact_email") or ""
        if ce not in by_contact:
            by_contact[ce] = {
                "contact_email": ce,
                "first_name": r.get("first_name") or "",
                "contact_id": r.get("contact_id") or "",
                "fired": [],
            }
        by_contact[ce]["fired"].append({
            "email_index": r.get("email_index"),
            "subject": (r.get("subject") or "")[:80],
            "dispatched_at": d_iso,
        })

    out = sorted(by_contact.values(), key=lambda x: x["contact_email"])
    return {
        "ok": True,
        "since_hours": n * 24,
        "unique_contacts_emailed": len(out),
        "total_emails_sent": sum(len(x["fired"]) for x in out),
        "by_contact": out,
    }


@app.get("/admin/scheduler-status")
def admin_scheduler_status(token: str = ""):
    """Quick read-only summary of the scheduler queue. Auth via ?token=..."""
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    from scheduler import _load_db, DB_PATH as _DB
    rows = _load_db()
    summary = {"pending": 0, "sent": 0, "failed": 0, "cancelled": 0, "other": 0}
    by_index = {}
    contacts_seen = set()
    for r in rows:
        s = r.get("status", "other")
        summary[s if s in summary else "other"] = summary.get(s if s in summary else "other", 0) + 1
        idx = r.get("email_index", 0)
        by_index[idx] = by_index.get(idx, 0) + 1
        if r.get("contact_id"):
            contacts_seen.add(r["contact_id"])
    return {
        "ok": True,
        "db_path": str(_DB),
        "db_persistent": str(_DB).startswith("/var/data"),
        "rows_total": len(rows),
        "by_status": summary,
        "by_email_index": by_index,
        "unique_contacts": len(contacts_seen),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cold-lead qualifier — backfill + first-touch fire
# ─────────────────────────────────────────────────────────────────────────────
#
# Story: 122+ FB/IG form leads sat in Meta Lead Center because the existing
# GHL workflow had a broken Form Is filter. We need to (a) bulk-import them
# into GHL, (b) enroll them into the (now-fixed) qualifier workflow, and (c)
# fire the first SMS+email immediately so they're not dead-on-arrival.
#
# Input formats supported:
#   - CSV upload (Meta Lead Center export shape: full_name, email, phone, ...)
#   - JSON array of {first_name, last_name, email, phone, goal?, source?}
#
# Tags applied:
#   - "fb-form-backfill" — so this batch is auditable
#   - "qualifier-cold"   — so the qualifier workflow's tag-add trigger fires
#
# Idempotency: GHL upsert is idempotent on email+phone, so re-running the
# same CSV is safe. We track per-contact "qualifier-fired-at" custom field
# to avoid re-firing SMS #1 on the same lead.

# A/B/C VARIANTS — assigned by contact_id hash so the same lead always gets
# the same opener. Track via the tag `variant-A` / `variant-B` / `variant-C`
# on the contact, then run /admin/ab-stats to see which converts best.
_QUALIFIER_SMS_VARIANTS = {
    # A — empathy-first (the current/control). Builds rapport, opens conversation.
    "A": (
        "Hey {fn}, Umar here from Bureau Bullies. Real quick before I dive in, "
        "what's been weighing on you most about your credit right now? Just want "
        "to know how to actually help."
    ),
    # B — direct/specific. Asks for a number to anchor the conversation in
    # concrete data, which usually gets faster + more useful replies.
    "B": (
        "Hey {fn}, Umar from Bureau Bullies. Quick one: roughly how many "
        "collections or charge-offs are showing on your report right now? "
        "Even rough is fine. I'll tell you what to do based on that number."
    ),
    # C — transformation/value-first. Leads with what they get if they engage.
    "C": (
        "Hey {fn}, Umar from Bureau Bullies. I scan credit reports and find "
        "every FCRA violation in 90 sec, free. Want me to run yours? Tells "
        "you exactly which accounts can be deleted vs. settled. Reply YES."
    ),
}

# Backwards compatibility — anything still referencing the old name uses A
_QUALIFIER_SMS_1 = _QUALIFIER_SMS_VARIANTS["A"]


def _pick_qualifier_variant(contact_id: str) -> str:
    """Deterministic A/B/C assignment by contact_id. Same contact = same variant
    every time (so re-fires don't change their experience). Even split.
    """
    if not contact_id:
        return "A"
    # Stable hash of the contact_id, modulo 3
    h = 0
    for ch in str(contact_id):
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return ["A", "B", "C"][h % 3]

_QUALIFIER_EMAIL_1_SUBJECT = "{fn}, real quick before I help"
_QUALIFIER_EMAIL_1_BODY = (
    "Hi {fn},\n\n"
    "Umar from Bureau Bullies. I just texted you too, but wanted to send this "
    "in case you check email first.\n\n"
    "Before I send you anything else, I want to know what's actually going on "
    "with you. Credit stuff is personal. People hold a lot of stress around it. "
    "What's been weighing on you most right now? A specific account, a goal "
    "you're locked out of, a recent denial, collectors calling, or the score "
    "itself? No wrong answer. Tell me where you're at and I'll point you to "
    "the right move.\n\n"
    "You can reply right to this email or text me back at the number that "
    "just texted you. Same person, same response.\n\n"
    "Talk soon,\n"
    "Umar\n"
    "Bureau Bullies LLC"
)


def _parse_csv_leads(csv_text: str) -> list:
    """Parse a Meta Lead Center CSV. Tolerates several common column shapes.

    Meta exports vary: some have 'full_name', some 'first_name'+'last_name',
    some 'phone_number' vs 'phone', etc. We normalize to {fn, ln, email, phone, goal}.
    """
    import csv as _csv
    import io as _io
    out = []
    if not csv_text or not csv_text.strip():
        return out
    reader = _csv.DictReader(_io.StringIO(csv_text))
    for row in reader:
        # Lowercase keys for forgiving matching
        rl = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}

        fn = rl.get("first_name") or rl.get("firstname") or rl.get("fname") or ""
        ln = rl.get("last_name")  or rl.get("lastname")  or rl.get("lname") or ""
        if not fn and rl.get("full_name"):
            parts = rl["full_name"].split(" ", 1)
            fn = parts[0]
            ln = parts[1] if len(parts) > 1 else ""

        email = rl.get("email") or rl.get("email_address") or ""
        phone = rl.get("phone") or rl.get("phone_number") or rl.get("mobile") or ""

        goal = (rl.get("goal") or rl.get("what_are_you_trying_to_unlock") or "").lower().strip()
        # Map free-text goals to our canonical set when possible
        if goal:
            for canonical in ("house", "car", "business", "credit_card", "freedom", "peace"):
                if canonical.replace("_", " ") in goal or canonical in goal:
                    goal = canonical
                    break

        if not email and not phone:
            continue  # can't reach them, skip

        out.append({
            "fn": fn, "ln": ln, "email": email, "phone": phone, "goal": goal,
            "source": rl.get("source") or rl.get("ad_name") or "fb-form-backfill",
        })
    return out


# ─────────────────────────────────────────────────────────────────────────
# Campaign-specific first-touch templates.
# ─────────────────────────────────────────────────────────────────────────
# Some Meta lead ads have a very specific hook (e.g. the "Equifax has never
# read your dispute letter" reel). For those campaigns we skip the generic
# empathy-opener and drop straight into the matching pitch + the matching
# product link. The campaign key arrives via the GHL workflow's webhook
# Custom Data field (e.g. campaign=equifax-dispute-letter).

_EQUIFAX_CAMPAIGN_SMS = (
    "{fn} — Umar here from Bureau Bullies. Thanks for filling out the form.\n\n"
    "Here's what Equifax actually does when you dispute: they never read your "
    "letter. They summarize the first line, generate a 2-digit code, then wait "
    "30 days to send you \"results.\" That's the entire \"investigation.\" "
    "It's a federal violation.\n\n"
    "Equifax Exposed shows you the playbook to weaponize that against them. "
    "Normally $666. Today: $27.\n\n"
    "https://equifaxexposed.com/\n\n"
    "Grab it before the price flips back."
)

_EQUIFAX_CAMPAIGN_EMAIL_SUBJECT = "{fn}, the Equifax secret (and your $27 link)"
_EQUIFAX_CAMPAIGN_EMAIL_BODY = (
    "{fn},\n\n"
    "Umar from Bureau Bullies. Thanks for filling out the form. I texted you "
    "too — wanted to make sure you got this in writing.\n\n"
    "Here's what Equifax is actually doing every time you dispute something:\n\n"
    "They never read your dispute letter. They summarize the first line, "
    "generate a 2-digit code, fire it off to the furnisher's automated system, "
    "and then wait 30 days to mail you a form letter saying \"investigation "
    "complete, info verified.\"\n\n"
    "That whole \"investigation\" takes under 10 seconds and zero humans "
    "touched it. That's a federal violation under FCRA §1681i.\n\n"
    "Equifax Exposed is the complete playbook for using that violation against "
    "them — how to dispute the way they can't summarize, what to demand, how "
    "to stack the leverage so they delete the account AND pay you for the "
    "violations.\n\n"
    "Normally $666. Today only: $27.\n\n"
    "https://equifaxexposed.com/\n\n"
    "Grab it before the price flips back.\n\n"
    "— Umar\n"
    "Bureau Bullies LLC\n\n"
    "P.S. Real customer win this week: Idris had 4 charge-offs Equifax "
    "\"verified\" in 10 seconds each. He used this exact playbook, got all 4 "
    "deleted, AND collected a $2,000 check from Equifax for the FCRA "
    "violations. That's the kind of leverage in here."
)

# Day 1 / 2 / 3 SMS follow-ups (sent by GHL Wait + Send SMS or by the backend
# scheduler — these are the source-of-truth templates either way).
_EQUIFAX_CAMPAIGN_SMS_DAY_1 = (
    "{fn} — checking in. Did you grab Equifax Exposed yet? Still $27 today: "
    "https://equifaxexposed.com/\n\n"
    "Price flips back to $666 once this push ends. Wanted to make sure you "
    "didn't miss it."
)
_EQUIFAX_CAMPAIGN_SMS_DAY_2 = (
    "{fn} — quick story while the price is still $27.\n\n"
    "Idris had 4 charge-offs Equifax \"verified\" in 10 seconds each. He ran "
    "the Equifax Exposed playbook — all 4 deleted AND he collected $2,000 "
    "from Equifax for the violations.\n\n"
    "That leverage is in here: https://equifaxexposed.com/\n\n"
    "$27 for a few more hours."
)
_EQUIFAX_CAMPAIGN_SMS_DAY_3 = (
    "{fn} — last shot. Price on Equifax Exposed flips back to $666 tonight.\n\n"
    "$27 for the next few hours: https://equifaxexposed.com/\n\n"
    "After this, you're paying full price or skipping the playbook entirely."
)


def _send_equifax_campaign_first_touch(client, contact_id: str, fn: str,
                                       email: str, phone: str) -> dict:
    """Day 0 first-touch for the 'Equifax has never read your dispute letter'
    Meta lead ad. Drops the equifaxexposed.com $27 link with the matching
    angle from the ad's hook. Fires SMS + Email in parallel.
    """
    result = {"sms_ok": False, "email_ok": False, "errors": [], "variant": "equifax_campaign"}
    fn_safe = (fn or "there").strip().split()[0] if (fn or "").strip() else "there"

    # Tag for attribution
    if contact_id:
        try:
            client.add_tags(contact_id, [
                "meta-equifax-ad",
                "equifax-campaign-day-0",
                "heat-hot",
            ])
        except Exception as _e:
            logger.warning("equifax campaign tag failed for %s: %s", contact_id, _e)

    # SMS — try contact_id-routed first, fall back to phone
    if phone or contact_id:
        try:
            sms_body = _EQUIFAX_CAMPAIGN_SMS.format(fn=fn_safe)
            ok = client.send_sms(phone=phone, message=sms_body, contact_id=contact_id) \
                 if hasattr(client, "send_sms") else False
            result["sms_ok"] = bool(ok)
            if not ok:
                result["errors"].append("equifax sms: send_sms returned False")
        except Exception as e:
            result["errors"].append(f"equifax sms: {e}")

    # Email
    if email:
        try:
            subj = _EQUIFAX_CAMPAIGN_EMAIL_SUBJECT.format(fn=fn_safe)
            plain = _EQUIFAX_CAMPAIGN_EMAIL_BODY.format(fn=fn_safe)
            from scheduler import _append_signature_and_footer, _plaintext_to_html
            plain_full = _append_signature_and_footer(plain)
            html_full = _plaintext_to_html(plain_full)
            ok = client.send_email(
                contact_id=contact_id,
                subject=subj,
                html=html_full,
                plain=plain_full,
            )
            result["email_ok"] = bool(ok)
        except Exception as e:
            result["errors"].append(f"equifax email: {e}")

    # Schedule the 3-day SMS follow-up sequence (Day 1, 2, 3 from now).
    # Idempotent — re-firing for the same contact won't double-queue.
    # Auto-cancels at send time if the contact has already purchased.
    if contact_id and phone:
        try:
            from scheduler import schedule_equifax_campaign_followups
            queued = schedule_equifax_campaign_followups(contact_id, phone, fn_safe)
            result["followups_scheduled"] = queued
        except Exception as e:
            result["errors"].append(f"equifax followup schedule: {e}")

    return result


def _send_qualifier_first_touch(client, contact_id: str, fn: str, email: str, phone: str) -> dict:
    """Fire SMS #1 and Email #1 of the cold-lead qualifier sequence.

    Returns a dict with sms_ok / email_ok flags so the caller can audit.
    """
    result = {"sms_ok": False, "email_ok": False, "errors": [], "variant": ""}
    fn_safe = (fn or "there").strip()

    # Assign A/B/C variant by contact_id hash (deterministic — same lead always
    # sees the same opener). Tag the contact so /admin/ab-stats can attribute
    # conversions back to the variant.
    variant = _pick_qualifier_variant(contact_id)
    result["variant"] = variant
    template = _QUALIFIER_SMS_VARIANTS.get(variant, _QUALIFIER_SMS_VARIANTS["A"])
    if contact_id:
        try:
            client.add_tags(contact_id, [f"variant-{variant}"])
        except Exception as _e:
            logger.warning("variant tag failed for %s: %s", contact_id, _e)

    # SMS #1 — try contact_id-routed first (GHL preferred), fall back to phone.
    if phone or contact_id:
        try:
            sms_body = template.format(fn=fn_safe)
            ok = client.send_sms(phone=phone, message=sms_body, contact_id=contact_id) \
                 if hasattr(client, "send_sms") else False
            result["sms_ok"] = bool(ok)
            if not ok:
                result["errors"].append("sms: send_sms returned False (check Render logs)")
        except Exception as e:
            result["errors"].append(f"sms: {e}")

    # Email #1 — only if we have an email address
    if email:
        try:
            subj = _QUALIFIER_EMAIL_1_SUBJECT.format(fn=fn_safe)
            plain = _QUALIFIER_EMAIL_1_BODY.format(fn=fn_safe)
            # Reuse the scheduler's signature/footer + html conversion
            from scheduler import _append_signature_and_footer, _plaintext_to_html
            plain_full = _append_signature_and_footer(plain)
            html_full = _plaintext_to_html(plain_full)
            ok = client.send_email(
                contact_id=contact_id,
                subject=subj,
                html=html_full,
                plain=plain_full,
            )
            result["email_ok"] = bool(ok)
        except Exception as e:
            result["errors"].append(f"email: {e}")

    return result


@app.post("/admin/backfill-leads")
async def admin_backfill_leads(
    token: str = Form(""),
    fire_first_touch: str = Form("1"),
    csv_text: str = Form(""),
    workflow_id: str = Form(""),
    limit: str = Form("50"),
):
    """Bulk-import cold form leads into GHL and (optionally) fire SMS #1 + Email #1.

    POST as application/x-www-form-urlencoded:
      token=<admin>
      csv_text=<paste of the Meta Lead Center CSV export>
      fire_first_touch=1   # send the qualifier first touch immediately
      workflow_id=<ghl_id> # optional, also enrolls into a specific workflow
      limit=50             # safety cap, no more than this many per call

    Returns a summary so you can audit and re-run if needed.
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    try:
        max_n = int(limit)
    except Exception:
        max_n = 50
    if max_n <= 0 or max_n > 500:
        max_n = 50

    leads = _parse_csv_leads(csv_text)
    if not leads:
        return {"ok": False, "error": "no_leads_parsed", "hint": "Paste the CSV body in csv_text. Must include header row."}

    leads = leads[:max_n]

    from ghl import GHLClient
    from datetime import datetime, timezone
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    upserted = 0
    fired_sms = 0
    fired_email = 0
    enrolled = 0
    errors = []
    audit = []

    for lead in leads:
        fn = lead["fn"]
        ln = lead["ln"]
        email = lead["email"]
        phone = lead["phone"]
        goal = lead["goal"]
        src = lead["source"]

        try:
            tags = ["fb-form-backfill", "qualifier-cold"]
            cf = {}
            if goal:
                cf["goal"] = goal
            cf["lead_source"] = src or "fb-form-backfill"
            cf["qualifier_fired_at"] = datetime.now(timezone.utc).isoformat()

            resp = client.upsert_contact(
                first_name=fn, last_name=ln,
                email=email, phone=phone,
                custom_fields=cf, tags=tags,
            )
            cid = (resp.get("contact") or resp).get("id") or (resp.get("contact") or resp).get("_id")
            upserted += 1

            entry = {"contact_id": cid, "name": f"{fn} {ln}".strip(), "email": email, "phone": phone}

            if workflow_id and cid:
                try:
                    client.add_to_workflow(cid, workflow_id)
                    enrolled += 1
                    entry["workflow_enrolled"] = True
                except Exception as we:
                    entry["workflow_error"] = str(we)[:200]

            if fire_first_touch == "1" and cid:
                touch = _send_qualifier_first_touch(client, cid, fn, email, phone)
                if touch["sms_ok"]:
                    fired_sms += 1
                if touch["email_ok"]:
                    fired_email += 1
                if touch["errors"]:
                    entry["touch_errors"] = touch["errors"]

            audit.append(entry)
        except Exception as e:
            errors.append({"lead": f"{fn} {ln} {email or phone}", "error": str(e)[:300]})

    return {
        "ok": True,
        "leads_parsed": len(leads),
        "upserted": upserted,
        "enrolled_in_workflow": enrolled,
        "first_touch_sms_sent": fired_sms,
        "first_touch_email_sent": fired_email,
        "errors": errors[:20],
        "audit_first_20": audit[:20],
        "hint": "Re-running this is safe (GHL upsert is idempotent on email+phone). qualifier_fired_at custom field tracks who got SMS #1.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Non-uploader fallback — day 4 pivot to text-based intake
# ─────────────────────────────────────────────────────────────────────────────
#
# Story: some leads will reply, qualify, get the upload link, and still not
# upload. Don't let them die. Day 4 we pivot to: "just text me your top 3
# collections, I'll write the play in chat." Lower friction by 90%, captures
# the leads who don't want to leave SMS.

_NON_UPLOADER_SMS = (
    "{fn}, no pressure on the upload. Faster path: just text me your top 3 "
    "collections, collector name and balance. I'll write your dispute plan "
    "right here in chat. No site, no form."
)


@app.post("/admin/check-non-uploaders")
def admin_check_non_uploaders(
    token: str = Form(""),
    days_since_qualified: str = Form("4"),
    limit: str = Form("50"),
):
    """Find contacts who qualified ≥N days ago but never uploaded, tag them
    `qualified-no-upload`, and send the fallback SMS.

    Run this on a schedule (daily cron, or manual via curl).
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}

    try:
        days = int(days_since_qualified)
    except Exception:
        days = 4
    try:
        max_n = int(limit)
    except Exception:
        max_n = 50

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    # Pull all 'qualified' contacts
    try:
        contacts = client.search_contacts_by_tag("qualified") if hasattr(client, "search_contacts_by_tag") else []
    except Exception as e:
        return {"ok": False, "error": f"search_failed: {e}"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    flipped = 0
    sms_sent = 0
    skipped = 0
    audit = []

    for c in contacts[:max_n]:
        cid = c.get("id") or c.get("_id")
        if not cid:
            continue
        tags = [str(t).lower() for t in (c.get("tags") or [])]
        # Skip if they already uploaded, or already pivoted, or already opted out
        if "bureau-scan-completed" in tags or "qualified-no-upload" in tags or "unsubscribed" in tags:
            skipped += 1
            continue

        # Check qualified_at custom field — only flip if ≥ N days have passed
        qualified_at = None
        for cf in (c.get("customFields") or []):
            if (cf.get("name") or cf.get("fieldKey") or "").lower().endswith("qualified_at"):
                qualified_at = cf.get("value") or cf.get("field_value") or ""
                break
        if qualified_at:
            try:
                q_dt = datetime.fromisoformat(qualified_at.replace("Z", "+00:00"))
                if q_dt.tzinfo is None:
                    q_dt = q_dt.replace(tzinfo=timezone.utc)
                if q_dt > cutoff:
                    skipped += 1
                    continue
            except Exception:
                pass  # if we can't parse the timestamp, fall through and flip them

        try:
            client.add_tags(cid, ["qualified-no-upload"])
            flipped += 1
            fn = c.get("firstName") or c.get("first_name") or "there"
            phone = c.get("phone") or ""
            if phone:
                try:
                    body = _NON_UPLOADER_SMS.format(fn=fn)
                    if hasattr(client, "send_sms"):
                        ok = client.send_sms(phone, body)
                        if ok:
                            sms_sent += 1
                except Exception as se:
                    audit.append({"cid": cid, "sms_error": str(se)[:200]})
        except Exception as e:
            audit.append({"cid": cid, "tag_error": str(e)[:200]})

    return {
        "ok": True,
        "qualified_total": len(contacts),
        "checked": min(len(contacts), max_n),
        "flipped_to_no_upload": flipped,
        "fallback_sms_sent": sms_sent,
        "skipped": skipped,
        "audit_first_20": audit[:20],
    }


@app.post("/admin/fire-qualifier")
def admin_fire_qualifier(
    token: str = Form(""),
    contact_id: str = Form(""),
    first_name: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
):
    """Fire SMS #1 + Email #1 to a SINGLE contact. Use for testing or recovery
    when you want to manually kick off the qualifier on one specific lead.

    Pass contact_id (required for SMS+Email routing in GHL), plus the contact's
    first_name / email / phone (so we don't have to round-trip to GHL to fetch).
    """
    if not _check_admin(token):
        return {"ok": False, "error": "unauthorized"}
    if not contact_id:
        return {"ok": False, "error": "missing contact_id"}

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}"}

    fn = first_name or "there"
    result = _send_qualifier_first_touch(client, contact_id, fn, email, phone)
    return {"ok": True, **result, "contact_id": contact_id, "first_name": fn}


@app.post("/webhooks/ghl/contact-created")
async def ghl_contact_created(request: Request):
    """REAL-TIME LEAD TOUCH — fires qualifier SMS+Email within seconds of a
    new IG/FB lead landing in GHL.

    Accepts a flexible GHL webhook payload (most workflow webhooks send the
    contact JSON directly, or wrap it under 'contact'). Looks up the contact_id,
    then fires the qualifier first-touch sequence (same path the cron uses,
    but for ONE specific contact instead of polling).

    Wire this up in GHL:
      1. Workflows → New workflow
      2. Trigger: Contact Created (optionally filter source contains
         'facebook' / 'instagram' or has tag 'Facebook form lead')
      3. Action: Webhook → POST → https://bureau-bullies.onrender.com/webhooks/ghl/contact-created
         Body: pass full Contact (default) — we'll figure out the contact_id.
      4. Save & publish.

    Idempotent: if the contact already has qualifier-fired or any purchase
    tag, we skip (so an accidental re-trigger doesn't re-spam).
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # GHL workflow webhooks can ship the contact in many shapes. Try the
    # common ones in order.
    contact = (
        payload.get("contact")
        or payload.get("Contact")
        or payload  # fall back to top-level
    )
    if not isinstance(contact, dict):
        return {"ok": False, "error": "invalid payload — no contact found"}

    cid = (contact.get("id") or contact.get("contactId") or contact.get("contact_id")
           or contact.get("_id") or "")
    if not cid:
        return {"ok": False, "error": "missing contact_id in payload"}

    fn = (contact.get("firstName") or contact.get("first_name")
          or contact.get("contact_first_name") or "there")
    email = (contact.get("email") or contact.get("emailAddress")
             or contact.get("contact_email") or "")
    phone = (contact.get("phone") or contact.get("contact_phone") or "")

    # Skip if already touched or already purchased — protects against
    # accidental workflow re-triggers + double-fires.
    tags_lower = [str(t).lower() for t in (contact.get("tags") or [])]
    SKIP = {
        "qualifier-cold", "qualifier-fired", "bureau-scan", "bureau-scan-completed",
        "pause-ai", "manual-mode", "unsubscribed", "do-not-contact",
        "purchased-toolkit", "purchased-vault", "purchased-dfy",
        "toolkit-purchased", "vault-purchased", "dfy-purchased",
        "paid-pif", "dfy-monthly-active", "notified-conversion",
    }
    if any(t in SKIP for t in tags_lower):
        return {
            "ok": True,
            "skipped": True,
            "reason": "contact already in qualifier flow or purchased",
            "contact_id": cid,
            "matched_tags": [t for t in tags_lower if t in SKIP],
        }

    from ghl import GHLClient
    try:
        client = GHLClient()
    except Exception as e:
        return {"ok": False, "error": f"ghl_init_failed: {e}", "contact_id": cid}

    # If GHL only sent a thin webhook body, fetch the full contact before routing
    # so we have tags/email/phone/source/customData. This prevents first-touch
    # failures when the workflow sends only {contact_id}.
    try:
        full_contact = None
        if cid and hasattr(client, "get_contact"):
            full_contact = client.get_contact(cid)
        if not full_contact and email and hasattr(client, "search_contact_by_email"):
            full_contact = client.search_contact_by_email(email)
        if isinstance(full_contact, dict):
            contact = {**full_contact, **contact}
            cid = cid or contact.get("id") or contact.get("_id") or contact.get("contactId") or ""
            fn = (contact.get("firstName") or contact.get("first_name") or fn or "there")
            email = (contact.get("email") or contact.get("emailAddress") or email or "")
            phone = (contact.get("phone") or phone or "")
            tags_lower = [str(t).lower() for t in (contact.get("tags") or [])]
    except Exception as e:
        logger.warning("contact-created webhook: full-contact lookup failed for %s: %s", cid, e)

    # Campaign routing — the GHL workflow includes a `campaign` Custom Data
    # field so we can fire the right pitch for the right ad. Read it from
    # several common locations (top-level, customData, contact.customData).
    campaign = (
        payload.get("campaign")
        or (payload.get("customData") or {}).get("campaign")
        or (payload.get("custom_data") or {}).get("campaign")
        or (contact.get("customData") or {}).get("campaign")
        or ""
    ).strip().lower()

    # Tag first so we don't double-process if GHL re-fires the webhook.
    # Equifax campaign gets its own tag pair so it skips the generic cold
    # path on future runs (without that, the SKIP check above would block
    # the campaign-specific message).
    try:
        if campaign == "equifax-dispute-letter":
            client.add_tags(cid, ["meta-equifax-ad", "qualifier-fired"])
        else:
            client.add_tags(cid, ["qualifier-cold", "qualifier-fired"])
    except Exception as e:
        logger.warning("contact-created webhook: add_tags failed for %s: %s", cid, e)

    # Fire the right first-touch path based on which campaign brought them in.
    try:
        if campaign == "equifax-dispute-letter":
            result = _send_equifax_campaign_first_touch(client, cid, fn, email, phone)
            logger.info(
                "contact-created EQUIFAX-CAMPAIGN: %s (cid=%s) sms=%s email=%s",
                fn, cid, result.get("sms_ok"), result.get("email_ok"),
            )
        else:
            result = _send_qualifier_first_touch(client, cid, fn, email, phone)
            logger.info(
                "contact-created webhook: %s (cid=%s) sms=%s email=%s variant=%s",
                fn, cid, result.get("sms_ok"), result.get("email_ok"), result.get("variant"),
            )
    except Exception as e:
        logger.exception("contact-created webhook: first-touch failed for %s", cid)
        return {"ok": False, "error": str(e)[:500], "contact_id": cid}

    return {
        "ok": True,
        "contact_id": cid,
        "first_name": fn,
        "campaign": campaign or "(default-qualifier)",
        "tagged": True,
        **result,
    }


# ---- Production observability / reliability endpoints -------------------
@app.post("/admin/dispatch-due")
def admin_dispatch_due(token: str = Form(...)):
    """One-shot scheduler dispatcher. Use this from Render Cron every 1-5 min.
    This makes scheduled emails less dependent on the web process staying alive.
    """
    _check_admin(token)
    from scheduler import dispatch_due_once
    n = dispatch_due_once()
    return {"ok": True, "dispatched": n}


@app.get("/admin/drip-dashboard")
def admin_drip_dashboard(token: str, limit: int = 50):
    _check_admin(token)
    from scheduler import scheduler_dashboard_snapshot
    return scheduler_dashboard_snapshot(limit=limit)


@app.get("/admin/events")
def admin_events(token: str, limit: int = 200, event_type: str = ""):
    _check_admin(token)
    return {"ok": True, "events": read_events(limit=limit, event_type=event_type or None)}


@app.get("/admin/conversion-dashboard")
def admin_conversion_dashboard(token: str, limit: int = 1000):
    """KPI dashboard for the 20% daily conversion target.
    Uses event log signals; /admin/funnel-stats still pulls from GHL tags.
    """
    _check_admin(token)
    events = read_events(limit=limit)
    today = datetime.now(timezone.utc).date().isoformat()
    todays = [e for e in events if str(e.get("ts", ""))[:10] == today]
    inbound = [e for e in todays if e.get("event_type") in {"inbound_sms", "inbound_ig_dm", "inbound_ig_comment", "meta_webhook_inbound", "lead_first_touch"}]
    purchases = [e for e in todays if e.get("event_type") == "purchase_detected"]
    rate = round(len(purchases) / max(1, len(inbound)), 4)
    return {
        "ok": True,
        "date_utc": today,
        "target_conversion_rate": 0.20,
        "target_note": daily_target_note(),
        "inbound_or_first_touch_events_today": len(inbound),
        "purchases_detected_today": len(purchases),
        "observed_event_conversion_rate": rate,
        "observed_event_conversion_rate_pct": round(rate * 100, 2),
        "below_target": rate < 0.20,
        "recommended_action_if_below_target": "Increase speed-to-lead, personally call/DM heat_score>=70 leads, test variant B hooks, and check failed outbound sends.",
    }


def _meta_verify_signature(raw_body: bytes, signature: str) -> bool:
    """Verify Meta X-Hub-Signature-256 using META_APP_SECRET."""
    import hmac, hashlib
    secret = os.getenv("META_APP_SECRET", "")
    if not secret:
        logger.warning("META_APP_SECRET missing; refusing Meta POST for safety")
        return False
    if not signature or not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _meta_send_dm_direct(recipient_id: str, message: str) -> bool:
    """Send direct IG message via Meta Graph API when GHL contact does not exist yet."""
    import requests
    token = os.getenv("META_IG_PAGE_TOKEN") or os.getenv("META_PAGE_ACCESS_TOKEN")
    if not token or not recipient_id or not message:
        return False
    url = "https://graph.facebook.com/v19.0/me/messages"
    payload = {"recipient": {"id": recipient_id}, "message": {"text": message[:1000]}}
    try:
        r = requests.post(url, params={"access_token": token}, json=payload, timeout=15)
        ok = r.ok
        if not ok:
            logger.warning("Meta direct DM failed: %s %s", r.status_code, r.text[:250])
        log_event("meta_direct_dm_sent" if ok else "meta_direct_dm_failed", recipient_id=recipient_id, preview=message[:120], status=getattr(r, "status_code", None))
        return ok
    except Exception as e:
        logger.warning("Meta direct DM exception: %s", e)
        log_event("meta_direct_dm_failed", recipient_id=recipient_id, error=str(e)[:250])
        return False


@app.get("/webhooks/meta/ig")
def meta_ig_verify(request: Request):
    params = dict(request.query_params)
    token = os.getenv("META_WEBHOOK_VERIFY_TOKEN", "bb_meta_verify_2026")
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == token:
        return int(params.get("hub.challenge", "0"))
    raise HTTPException(status_code=403, detail="Meta verify token mismatch")


@app.post("/webhooks/meta/ig")
async def meta_ig_webhook(request: Request):
    """Meta Graph direct webhook with signature validation.

    This catches IG events that have not yet become a GHL contact. It is a backup
    path for keyword/autoreply, not a replacement for the richer GHL contact flow.
    """
    raw = await request.body()
    sig = request.headers.get("X-Hub-Signature-256", "")
    if not _meta_verify_signature(raw, sig):
        raise HTTPException(status_code=403, detail="invalid Meta signature")
    try:
        payload = json.loads(raw.decode("utf-8") or "{}")
    except Exception:
        payload = {}
    log_event("meta_webhook_received", object=payload.get("object"))

    processed = 0
    entries = payload.get("entry") or []
    for entry in entries:
        for msg in entry.get("messaging", []) or []:
            sender_id = ((msg.get("sender") or {}).get("id") or "").strip()
            text = (((msg.get("message") or {}).get("text")) or ((msg.get("postback") or {}).get("title")) or "").strip()
            if not sender_id or not text:
                continue
            processed += 1
            score = lead_heat_score(msg, text)
            log_event("meta_webhook_inbound", sender_id=sender_id, text=text, heat_score=score)
            if _owner_pause_requested(text):
                log_event("meta_webhook_manual_pause", sender_id=sender_id, text=text)
                continue
            kw_match = _match_keyword_shortcut(text)
            if kw_match:
                kw_key, kw_cfg = kw_match
                reply = _render_keyword_reply(kw_cfg, "")
                sent = _meta_send_dm_direct(sender_id, reply)
                log_event("keyword_shortcut_sent", channel="meta_direct", sender_id=sender_id, keyword=kw_key, sent=sent, heat_score=score)
                continue
            try:
                reply = bully_chat(user_message=text, contact_context={"channel":"meta_direct", "heat_score": score, "variant": variant_for_contact(sender_id, "meta_direct")}, history=None)
            except Exception:
                reply = "Got you. Upload your 3-bureau report here and I’ll show you what’s hurting your score: https://bullyaiagent.com/#upload"
            sent = _meta_send_dm_direct(sender_id, reply)
            log_event("ai_reply_sent", channel="meta_direct", sender_id=sender_id, sent=sent, heat_score=score)
    return {"ok": True, "processed": processed}


@app.get("/healthz")
def healthz():
    return {"ok": True}


# Allow `python app.py` to run the server directly (handy for Render / Fly)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )
