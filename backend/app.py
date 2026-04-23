"""
Bureau Bullies — FastAPI web server
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
from scheduler import schedule_email_drip, cancel_drip, start_background_scheduler

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")
logger = logging.getLogger("bureau-bullies.app")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/bullies_downloads"))
DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)

MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "25"))
ALLOWED_MIME = {"application/pdf", "image/png", "image/jpeg", "image/jpg", "image/webp", "image/gif"}

app = FastAPI(title="Bureau Bullies API", version="2.1.0")

start_background_scheduler(app)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])


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


@app.post("/api/scan")
async def scan(
    firstName: str = Form(...),
    lastName: str = Form(...),
    email: str = Form(...),
    phone: str = Form(...),
    goal: str = Form("freedom"),
    reports: List[UploadFile] = File(...),
):
    if not reports:
        raise HTTPException(400, "Please upload at least one file.")
    if len(reports) > 12:
        raise HTTPException(400, "Too many files. Upload up to 12.")

    tmpdir = Path(tempfile.mkdtemp(prefix="bullies_"))
    saved = []
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

        summary = analyze_report(pdf_paths=saved, consumer_name=f"{firstName} {lastName}", consumer_phone=phone, scratch_dir=tmpdir)

        token = secrets.token_urlsafe(24)
        doc_path = DOWNLOAD_DIR / f"{token}.docx"
        generate_report_doc(summary=summary, consumer_first=firstName, consumer_last=lastName, consumer_email=email, consumer_phone=phone, out_path=doc_path)
        logger.info("Generated doc: %s", doc_path)

        custom_fields = summary_to_ghl_fields(summary)
        custom_fields["cr_doc_url"] = f"/download/{token}"

        GOAL_LABELS = {"house": "House", "car": "Car", "business": "Business", "credit_card": "Credit Card", "freedom": "Personal Freedom", "peace": "Peace of Mind"}
        goal_key = (goal or "freedom").strip().lower().replace(" ", "_").replace("-", "_")
        if goal_key not in GOAL_FRAMES:
            goal_key = "freedom"
        goal_label = GOAL_LABELS.get(goal_key, "Personal Freedom")
        custom_fields["cr_goal"] = goal_key
        custom_fields["cr_goal_label"] = goal_label

        emails = []
        try:
            scan_ctx = {
                "top_collection_name": summary.top_collection_name,
                "top_collection_amount": summary.top_collection_amount,
                "total_leverage": summary.total_estimated_leverage,
                "violations_count": len(summary.violations),
                "fico_range": summary.estimated_fico_range,
                "top_pain_point": summary.top_pain_point,
                "fear_hook": summary.fear_hook,
                "case_law_cited": "; ".join(summary.case_law_cited or []),
                "recommended_tier": summary.recommended_tier,
            }
            emails = generate_full_sequence(consumer_first=firstName, scan=scan_ctx, goal_key=goal_key, goal_label=goal_label)
            custom_fields.update(emails_to_ghl_fields(emails))
            logger.info("Generated %d tailored emails for %s", len(emails), firstName)
        except Exception as e:
            logger.exception("Email sequence generation failed (non-fatal): %s", e)

        ghl_result = None
        try:
            ghl_result = push_lead_to_ghl(first_name=firstName, last_name=lastName, email=email, phone=phone, custom_fields=custom_fields, urgency_score=summary.urgency_score, recommended_tier=summary.recommended_tier)
        except GHLError as e:
            logger.error("GHL push failed (non-fatal): %s", e)
        except Exception as e:
            logger.exception("GHL push unexpected error: %s", e)

        try:
            contact_id = ""
            if ghl_result:
                contact_id = (ghl_result.get("contact") or ghl_result).get("id") or (ghl_result.get("contact") or ghl_result).get("_id") or ""
            if contact_id and emails:
                schedule_email_drip(contact_id=contact_id, contact_email=email, first_name=firstName, emails=emails)
        except Exception as e:
            logger.exception("Failed to schedule email drip: %s", e)

        return JSONResponse({"success": True, "resultsUrl": "/results", "downloadUrl": f"/download/{token}", "summary": asdict(summary)})
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Scan failed")
        return JSONResponse({"success": False, "error": f"Scan failed: {e}"}, status_code=500)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@app.get("/download/{token}")
def download(token):
    path = DOWNLOAD_DIR / f"{token}.docx"
    if not path.exists():
        raise HTTPException(404, "This download has expired.")
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", filename="BureauBullies_AttackPlan.docx")


class ChatIn(BaseModel):
    message: str
    contact: dict = {}
    history: list = []


@app.post("/api/chat")
def chat(payload):
    try:
        reply = bully_chat(user_message=payload.message, contact_context=payload.contact or None, history=payload.history or None)
        return {"reply": reply}
    except Exception as e:
        logger.exception("Chat failed")
        raise HTTPException(500, f"Chat error: {e}")


def _as_string(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return str(v.get("body") or v.get("text") or v.get("content") or v.get("message") or "")
    return str(v)


@app.post("/webhooks/ghl/sms-reply")
async def ghl_sms_reply(request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    message = _as_string(payload.get("message") or payload.get("body") or payload.get("sms") or payload.get("text") or (payload.get("customData") or {}).get("message") or "").strip()
    if not message:
        logger.warning("SMS reply webhook received empty message: %s", str(payload)[:300])
        return {"reply": "Got your message — let me pull your report and get right back to you.", "ok": False}
    first_name = payload.get("first_name") or payload.get("firstName") or (payload.get("contact") or {}).get("first_name") or ""
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
    history = payload.get("history") or []
    logger.info("SMS reply from %s: %r  (ctx keys: %s)", first_name or "unknown", message[:80], list(custom.keys())[:8])
    try:
        reply_text = bully_chat(user_message=message, contact_context=custom or None, history=history)
    except Exception as e:
        logger.exception("SMS reply chat failed")
        reply_text = f"{first_name + ' — ' if first_name else ''}Bully AI here. Got your message. Give me a few minutes and I'll get back to you. — BB"
    # Cancel the email drip if they replied STOP or DONE
    try:
        lo = message.lower().strip()
        if lo in ("stop", "done", "unsubscribe", "remove"):
            contact_id = (payload.get("contact_id") or (payload.get("contact") or {}).get("id") or "")
            if contact_id:
                cancel_drip(contact_id, reason="unsubscribed")
    except Exception:
        pass
    lower = reply_text.lower()
    link_sent = None
    if "thecollectionkiller.com/dispute-vault" in lower:
        link_sent = "vault"
    elif "suethemallwithus.com" in lower or "dfy" in lower:
        link_sent = "dfy"
    elif "thebureaubullies.com/ck" in lower or "thecollectionkiller.com" in lower:
        link_sent = "toolkit"
    return {"ok": True, "reply": reply_text, "link_sent": link_sent, "first_name": first_name}


@app.post("/webhooks/ghl/email-reply")
async def ghl_email_reply(request):
    """Route inbound email replies through Bully AI, same as SMS."""
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    message = _as_string(payload.get("message") or payload.get("body") or payload.get("text") or (payload.get("email") or {}).get("body") or "").strip()
    if not message:
        return {"reply": "Got your note — I'll circle back shortly.", "ok": False}
    first_name = payload.get("first_name") or payload.get("firstName") or (payload.get("contact") or {}).get("first_name") or ""
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
    try:
        reply_text = bully_chat(user_message=message, contact_context=custom or None, history=[])
    except Exception as e:
        logger.exception("Email reply chat failed")
        reply_text = f"{first_name + ' — ' if first_name else ''}Bully AI here. Got your email. I'll look at your file again and respond shortly."
    # Cancel drip on unsubscribe words
    try:
        lo = message.lower().strip()
        if any(w in lo for w in ("unsubscribe", "remove me", "stop emailing", "stop sending", "take me off", "done")):
            contact_id = (payload.get("contact_id") or (payload.get("contact") or {}).get("id") or "")
            if contact_id:
                cancel_drip(contact_id, reason="unsubscribed")
    except Exception:
        pass
    # Build subject
    subject = "Re: your report"
    if first_name:
        subject = f"{first_name} — re: your report"
    return {"ok": True, "subject": subject, "reply": reply_text, "first_name": first_name}


@app.get("/healthz")
def healthz():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")))
