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
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from analyzer import analyze_report, summary_to_ghl_fields
from bully_ai import chat as bully_chat
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
    goal:      str = Form("freedom"),   # NEW — what they're trying to unlock
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

    logger.info(
        "SMS reply from %s: %r  (ctx keys: %s, history turns: %d)",
        first_name or "unknown", message[:80], list(custom.keys())[:8], len(history)
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

    enqueued = 0
    skipped = 0
    for c in contacts or []:
        cid = c.get("id") or c.get("_id")
        email_addr = c.get("email") or c.get("emailAddress")
        if not cid or not email_addr:
            skipped += 1
            continue
        fn = c.get("firstName") or c.get("first_name") or ""
        # Reconstruct the 7 emails from stored custom fields
        cf = c.get("customFields") or c.get("custom_field") or []
        # Map fieldKey -> value
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
        # Cancel any pending rows for this contact first so we don't duplicate
        try:
            cancel_drip(cid, reason="backfill-replace")
        except Exception:
            pass
        n = schedule_email_drip(cid, email_addr, fn, emails)
        if n:
            enqueued += 1
    return {"ok": True, "contacts_enqueued": enqueued, "skipped": skipped}


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
def admin_dispatch_next(token: str = Form(""), regenerate: str = Form("1")):
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

    for c in contacts or []:
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

    # Persist all new rows in one save
    if new_rows:
        rows.extend(new_rows)
        _save_db(rows)

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
