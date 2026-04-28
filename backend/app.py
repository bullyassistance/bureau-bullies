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
from docgen import generate_report_doc
from ghl import push_lead_to_ghl, GHLError
from email_generator import generate_full_sequence, emails_to_ghl_fields, GOAL_FRAMES
from scheduler import (
    schedule_email_drip,
    cancel_drip,
    start_background_scheduler,
    reset_all_failed,
)

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
    contact_id = (
        payload.get("contact_id") or payload.get("contactId")
        or (payload.get("contact") or {}).get("id") or ""
    )

    # Skip if Umar is already replying manually
    if contact_id and _ig_human_active(contact_id):
        logger.info("SMS reply skipped — human active for contact %s", contact_id)
        return {"ok": True, "skipped": "human_active", "first_name": first_name}

    history = payload.get("history") or _ig_fetch_history(contact_id)

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

    try:
        reply_text = bully_chat(
            user_message=message,
            contact_context=custom or None,
            history=history,
        )
    except Exception as e:
        logger.exception("SMS reply chat failed")
        # Graceful fallback, never return 500 to GHL so the workflow doesn't break
        reply_text = (
            f"{first_name + ', ' if first_name else ''}"
            "Bully AI here. Got your message. Give me a few minutes and I'll get "
            "back to you with a real answer. BB"
        )

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


def _ig_extract_contact_id(payload: dict) -> str:
    """Pull the GHL contact id out of an IG webhook payload, regardless of
    which token name the user wired in the GHL workflow's Custom Data."""
    if not isinstance(payload, dict):
        return ""
    candidates = [
        payload.get("contact_id"),
        payload.get("contactId"),
        payload.get("contact_Id"),
        (payload.get("contact") or {}).get("id") if isinstance(payload.get("contact"), dict) else None,
        (payload.get("contact") or {}).get("_id") if isinstance(payload.get("contact"), dict) else None,
        (payload.get("customData") or {}).get("contact_id") if isinstance(payload.get("customData"), dict) else None,
        (payload.get("customData") or {}).get("contactId") if isinstance(payload.get("customData"), dict) else None,
    ]
    for c in candidates:
        if c and isinstance(c, str) and len(c) >= 8:
            return c
    return ""


def _ig_extract_comment_id(payload: dict) -> str:
    """Optional — pull an IG comment id from the payload so GHL can send a
    'reply to comment via DM' which bypasses the 24hr engagement rule."""
    if not isinstance(payload, dict):
        return ""
    candidates = [
        payload.get("comment_id"),
        payload.get("commentId"),
        payload.get("ig_comment_id"),
        (payload.get("trigger") or {}).get("comment_id") if isinstance(payload.get("trigger"), dict) else None,
        (payload.get("customData") or {}).get("comment_id") if isinstance(payload.get("customData"), dict) else None,
    ]
    for c in candidates:
        if c and isinstance(c, str):
            return c
    return ""


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


def _ig_human_active(contact_id: str) -> bool:
    """Returns True if Umar has manually replied recently or the contact is
    flagged pause-ai. Used to short-circuit auto-replies."""
    if not contact_id:
        return False
    try:
        from ghl import GHLClient
        client = GHLClient()
        return bool(client.is_human_active(contact_id))
    except Exception as e:
        logger.warning("_ig_human_active failed (allowing AI to reply): %s", e)
        return False


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
    sent = _ig_send_dm_safe(contact_id, reply_text, comment_id=comment_id)
    return {
        "ok": True,
        "reply": reply_text,
        "first_name": first_name,
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

    # Already-scanned reconciliation — if they claim to have uploaded, harvest
    # email from their message, look up the GHL contact, pull scan data into
    # context. Without this, Bully AI sends the upload link in a loop.
    custom = _enrich_context_with_already_scanned(message, custom)

    # Human-override check: if Umar manually replied recently, AI stays out of it.
    contact_id = _ig_extract_contact_id(payload)
    if _ig_human_active(contact_id):
        logger.info("IG DM auto-reply skipped — human active for contact %s", contact_id)
        return {
            "ok": True,
            "skipped": "human_active",
            "first_name": first_name,
            "ig_handle": ig_handle,
        }

    # Pull conversation history from GHL so Bully AI has memory of prior turns.
    # GHL webhooks don't pass history, so without this every reply restarts the convo.
    history = payload.get("history") or _ig_fetch_history(contact_id)

    try:
        reply_text = bully_chat(user_message=message, contact_context=custom, history=history)
    except Exception:
        logger.exception("IG DM chat failed")
        reply_text = (
            f"{(first_name + ' ') if first_name else ''}give me a sec to pull your file. "
            "If you haven't yet, drop your reports at https://bullyaiagent.com/#upload"
        )

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
_BOOTSTRAP_ADMIN_TOKEN = "bb_bootstrap_b9f3e1c4a7d2ae5b16f4938c0e2d77c8"


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

_QUALIFIER_SMS_1 = (
    "Hey {fn}, Umar from Bureau Bullies. Saw you grabbed the 3 Day Challenge. "
    "Quick question before I send anything — what's the #1 thing on your "
    "credit report you want gone? Collection, late, charge-off, repo, or "
    "something else?"
)

_QUALIFIER_EMAIL_1_SUBJECT = "{fn}, before I help you, one question"
_QUALIFIER_EMAIL_1_BODY = (
    "Hi {fn},\n\n"
    "Umar from Bureau Bullies. You filled out the 3 Day Challenge a few minutes "
    "ago, so I just sent you a quick text too.\n\n"
    "Before I send you anything else, I need to know one thing — what's the #1 "
    "item on your credit report you want gone? Most people I work with have "
    "one specific account that's eating their score: a collection from "
    "Portfolio Recovery, a charge-off from Capital One, a repo, a 90-day late "
    "from a hospital bill. Tell me yours and I'll send you the exact playbook "
    "for that account.\n\n"
    "You can reply right to this email, or text me back at the number that "
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


def _send_qualifier_first_touch(client, contact_id: str, fn: str, email: str, phone: str) -> dict:
    """Fire SMS #1 and Email #1 of the cold-lead qualifier sequence.

    Returns a dict with sms_ok / email_ok flags so the caller can audit.
    """
    result = {"sms_ok": False, "email_ok": False, "errors": []}
    fn_safe = (fn or "there").strip()

    # SMS #1 — only if we have a phone number
    if phone:
        try:
            sms_body = _QUALIFIER_SMS_1.format(fn=fn_safe)
            ok = client.send_sms(contact_id=contact_id, message=sms_body) if hasattr(client, "send_sms") else False
            result["sms_ok"] = bool(ok)
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
                        ok = client.send_sms(contact_id=cid, message=body)
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
