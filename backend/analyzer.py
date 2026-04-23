"""
Bully AI — Credit Report Analyzer
----------------------------------
Reads uploaded credit reports in ANY format:
  - Text-based PDFs (annualcreditreport.com, SmartCredit, IdentityIQ)
  - Scanned-image PDFs (rasterized pages sent to vision)
  - Phone screenshots (PNG/JPG/WEBP)

Extracts text where possible, falls back to Claude vision for image content,
then sends everything to Bully AI for a deep-dive violation hunt that cites
real federal case law (Hinkle, Gorman, Johnson, Saunders, Cushman, Ramirez).

Returns a structured JSON object the GHL integration pushes as custom fields.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional, Tuple

import pdfplumber
from anthropic import Anthropic

logger = logging.getLogger("bureau-bullies.analyzer")

# ---- Config --------------------------------------------------------------
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
MAX_REPORT_CHARS = int(os.getenv("MAX_REPORT_CHARS", "180000"))
MIN_TEXT_PER_PAGE = 120  # if a PDF page has less than this, fall back to vision


# ---- JSON repair helpers -------------------------------------------------
def _repair_truncated_json(raw: str) -> str:
    """
    Try to salvage JSON that got cut off mid-string / mid-array by Claude hitting
    the max_tokens ceiling. Walks the string tracking open braces/brackets/quotes
    and appends whatever closers are needed to make it parseable.

    Not perfect — the last partial object will be dropped — but enough to recover
    ~90% of cases where the truncation happened inside a late violations[] entry.
    """
    # Strip trailing junk after the last full }
    s = raw.strip()
    if not s.startswith("{"):
        return s  # nothing we can do

    in_string = False
    escape = False
    stack = []
    last_safe_cut = 0

    for i, ch in enumerate(s):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch in "{[":
            stack.append(ch)
        elif ch == "}":
            if stack and stack[-1] == "{":
                stack.pop()
                if not stack:
                    last_safe_cut = i + 1
        elif ch == "]":
            if stack and stack[-1] == "[":
                stack.pop()

    if in_string:
        j = len(s) - 1
        while j >= 0:
            if s[j] == '"' and (j == 0 or s[j-1] != "\\"):
                s = s[:j]
                break
            j -= 1
        return _repair_truncated_json(s + '"')

    s = re.sub(r",\s*$", "", s)

    closers = []
    for opener in reversed(stack):
        closers.append("]" if opener == "[" else "}")
    return s + "".join(closers)


# ---- Data contracts ------------------------------------------------------
@dataclass
class Violation:
    creditor: str
    account_last4: str
    bureau: str
    violation_type: str
    description: str
    dollar_leverage: float
    severity: str
    case_law_citation: str = ""


@dataclass
class ReportSummary:
    full_name: Optional[str]
    estimated_fico_range: str
    total_negative_items: int
    total_collections_value: float
    total_charge_offs_value: float
    total_late_payments: int
    hard_inquiries: int
    total_estimated_leverage: float
    top_pain_point: str
    top_collection_name: str
    top_collection_amount: float
    urgency_score: int
    violations: List[Violation] = field(default_factory=list)
    recommended_tier: str = "toolkit"
    fear_hook: str = ""
    urgency_hook: str = ""
    executive_summary: str = ""
    case_law_cited: List[str] = field(default_factory=list)


def _is_image(path: Path) -> bool:
    return path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".gif"}


def _is_pdf(path: Path) -> bool:
    return path.suffix.lower() == ".pdf"


def _image_media_type(path: Path) -> str:
    ext = path.suffix.lower().lstrip(".")
    return {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
            "webp": "image/webp", "gif": "image/gif"}.get(ext, "image/png")


def _file_to_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def extract_pdf_text(pdf_path: Path) -> Tuple[str, bool]:
    chunks = []
    low_text_pages = 0
    total_pages = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                total_pages += 1
                text = page.extract_text() or ""
                if len(text.strip()) < MIN_TEXT_PER_PAGE:
                    low_text_pages += 1
                if text.strip():
                    chunks.append(f"\n--- Page {i} ---\n{text}")
    except Exception as e:
        logger.exception("PDF extraction failed for %s", pdf_path)
        return "", True

    joined = "\n".join(chunks)
    needs_vision = total_pages > 0 and (low_text_pages / total_pages) > 0.5
    return joined, needs_vision


def detect_bureau(text: str) -> str:
    t = text.lower()
    if t.count("experian") > 3: return "Experian"
    if t.count("equifax") > 3: return "Equifax"
    if t.count("transunion") > 3: return "TransUnion"
    return "Unknown"


def pdf_to_images(pdf_path: Path, out_dir: Path, dpi: int = 150) -> List[Path]:
    try:
        import pypdfium2 as pdfium
    except ImportError:
        logger.warning("pypdfium2 not installed")
        return []
    out_paths = []
    pdf = pdfium.PdfDocument(str(pdf_path))
    for i, page in enumerate(pdf):
        pil = page.render(scale=dpi / 72).to_pil()
        if max(pil.size) > 1568:
            ratio = 1568 / max(pil.size)
            pil = pil.resize((int(pil.size[0] * ratio), int(pil.size[1] * ratio)))
        out = out_dir / f"{pdf_path.stem}_p{i+1}.png"
        pil.save(out, format="PNG", optimize=True)
        out_paths.append(out)
        if i >= 20: break
    return out_paths


def build_content_blocks(pdf_paths, scratch_dir):
    blocks = []
    summary_parts = []
    text_parts = []
    for p in pdf_paths:
        if _is_image(p):
            blocks.append({"type": "image", "source": {"type": "base64", "media_type": _image_media_type(p), "data": _file_to_b64(p)}})
            summary_parts.append(f"IMAGE: {p.name}")
            continue
        if _is_pdf(p):
            text, needs_vision = extract_pdf_text(p)
            if text and not needs_vision:
                bureau = detect_bureau(text)
                text_parts.append(f"\n\n=== FILE: {p.name}  (BUREAU: {bureau}) ===\n{text}")
                summary_parts.append(f"PDF-TEXT: {p.name} ({bureau})")
            else:
                images = pdf_to_images(p, scratch_dir)
                for img in images:
                    blocks.append({"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": _file_to_b64(img)}})
                summary_parts.append(f"PDF-SCAN: {p.name} ({len(images)} pages)")
    if text_parts:
        combined = "".join(text_parts)
        if len(combined) > MAX_REPORT_CHARS:
            combined = combined[:MAX_REPORT_CHARS] + "\n\n[... truncated ...]"
        blocks.insert(0, {"type": "text", "text": combined})
    return blocks, " | ".join(summary_parts)


SYSTEM_PROMPT = """You are Bully AI — the in-house AI credit assassin for The Bureau Bullies, LLC (Wilmington, DE). You are a forensic credit analyst and FCRA/FDCPA/Metro 2 specialist.

When you speak, you speak as "Bully AI" — confident, aggressive, specific, zero-fluff. Never mention Claude, Anthropic, or any underlying model.

Your job: read the consumer's 3-bureau credit report and produce an aggressive, dollar-weighted deep-dive analysis.

YOU MUST:
1. Identify EVERY negative tradeline (collections, charge-offs, late payments, repos, bankruptcies, judgments, tax liens).
2. For each negative item, hunt violations of: FCRA §§ 609, 611, 615; 15 U.S.C. § 1681s-2(b); FDCPA §§ 807, 809, 812; Metro 2 compliance.
3. Compare the same account across all three bureaus.
4. For EACH violation, cite a federal case in "case_law_citation": Hinkle v. Midland (rubber-stamp), Johnson v. MBNA (failure to investigate), Gorman v. Wolpoff (private right), TransUnion v. Ramirez (standing), Saunders v. BB&T (emotional distress), Cushman v. Trans Union (furnisher standards).
5. Assign each violation a conservative "dollar_leverage": FCRA $500-1000, FDCPA $1000, Metro 2 $500-1500.
6. total_estimated_leverage = sum of dollar_leverage.
7. urgency_score: 80-100 (5+ collections, $10K+), 60-79 (3-4, $5-10K), 40-59 (1-2, $2-5K), 0-39 (minor).
8. recommended_tier: toolkit (0-2 negatives), accelerator (3-6), dfy (7+ or $10K+).
9. fear_hook AND urgency_hook — one-liners personalized to the worst item.
10. executive_summary — 3-4 sentences naming specific creditors, dollar amounts, and ONE case.
11. case_law_cited — unique case citations used.

OUTPUT FORMAT: Respond ONLY with valid JSON. No preamble, no markdown fences. CRITICAL: keep every "description" under 180 characters and limit violations[] to the 8 most important. Be specific but concise."""


USER_TEMPLATE = """Here is the consumer's 3-bureau credit report. Analyze it now.

CONSUMER NAME: {consumer_name}
CONSUMER PHONE: {consumer_phone}
FILES PROCESSED: {files_summary}

Return JSON matching this EXACT schema:

{{
  "full_name": "string or null",
  "estimated_fico_range": "e.g. 540-580",
  "total_negative_items": 0,
  "total_collections_value": 0.0,
  "total_charge_offs_value": 0.0,
  "total_late_payments": 0,
  "hard_inquiries": 0,
  "total_estimated_leverage": 0.0,
  "top_pain_point": "one-line",
  "top_collection_name": "Creditor",
  "top_collection_amount": 0.0,
  "urgency_score": 0,
  "recommended_tier": "toolkit|accelerator|dfy",
  "fear_hook": "personalized",
  "urgency_hook": "personalized",
  "executive_summary": "3-4 sentences with creditor names and one case",
  "case_law_cited": ["Hinkle v. Midland"],
  "violations": [
    {{"creditor": "Capital One", "account_last4": "4532", "bureau": "Experian", "violation_type": "15 U.S.C. § 1681s-2(b)", "description": "short", "dollar_leverage": 1000.0, "severity": "high", "case_law_citation": "Hinkle v. Midland"}}
  ]
}}"""


def analyze_report(pdf_paths, consumer_name, consumer_phone, scratch_dir=None, api_key=None):
    scratch = scratch_dir or Path("/tmp/bullies_scratch")
    scratch.mkdir(exist_ok=True, parents=True)
    content_blocks, files_summary = build_content_blocks(pdf_paths, scratch)
    if not content_blocks:
        raise ValueError("No readable content extracted.")
    content_blocks.append({"type": "text", "text": USER_TEMPLATE.format(consumer_name=consumer_name, consumer_phone=consumer_phone, files_summary=files_summary)})
    client = Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
    logger.info("Calling Bully AI (%s) — %d content blocks", CLAUDE_MODEL, len(content_blocks))
    resp = client.messages.create(model=CLAUDE_MODEL, max_tokens=16000, system=SYSTEM_PROMPT, messages=[{"role": "user", "content": content_blocks}])
    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    stop_reason = getattr(resp, "stop_reason", None)
    logger.info("Bully AI raw length=%d, stop_reason=%s", len(raw), stop_reason)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed (%s). Attempting repair.", e)
        repaired = _repair_truncated_json(raw)
        try:
            data = json.loads(repaired)
            logger.info("JSON repaired successfully")
        except json.JSONDecodeError as e2:
            logger.warning("Repair failed (%s). Retrying Claude with tighter output.", e2)
            retry_content = list(content_blocks)
            retry_content.append({"type": "text", "text": "Your previous response was cut off. Return ONLY the JSON object. Limit to 8 most important violations max. Keep every description under 150 characters."})
            retry = client.messages.create(model=CLAUDE_MODEL, max_tokens=16000, system=SYSTEM_PROMPT, messages=[{"role": "user", "content": retry_content}])
            raw2 = retry.content[0].text.strip()
            raw2 = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw2, flags=re.MULTILINE).strip()
            try:
                data = json.loads(raw2)
            except json.JSONDecodeError as e3:
                try:
                    data = json.loads(_repair_truncated_json(raw2))
                except Exception:
                    logger.error("All JSON recovery failed. First 500 chars: %s", raw[:500])
                    raise RuntimeError("Bully AI returned invalid JSON even after repair + retry. Try uploading one bureau at a time.") from e3

    violations = []
    for v in data.pop("violations", []) or []:
        if not isinstance(v, dict):
            continue
        try:
            violations.append(Violation(
                creditor=str(v.get("creditor", "Unknown")),
                account_last4=str(v.get("account_last4", "")),
                bureau=str(v.get("bureau", "")),
                violation_type=str(v.get("violation_type", "")),
                description=str(v.get("description", ""))[:400],
                dollar_leverage=float(v.get("dollar_leverage", 0) or 0),
                severity=str(v.get("severity", "medium")),
                case_law_citation=str(v.get("case_law_citation", ""))[:400],
            ))
        except Exception as ve:
            logger.warning("Skipping malformed violation: %s", ve)

    summary = ReportSummary(
        full_name=data.get("full_name") or None,
        estimated_fico_range=str(data.get("estimated_fico_range") or "Unknown"),
        total_negative_items=int(data.get("total_negative_items") or len(violations)),
        total_collections_value=float(data.get("total_collections_value") or 0),
        total_charge_offs_value=float(data.get("total_charge_offs_value") or 0),
        total_late_payments=int(data.get("total_late_payments") or 0),
        hard_inquiries=int(data.get("hard_inquiries") or 0),
        total_estimated_leverage=float(data.get("total_estimated_leverage") or sum(v.dollar_leverage for v in violations)),
        top_pain_point=str(data.get("top_pain_point") or ""),
        top_collection_name=str(data.get("top_collection_name") or ""),
        top_collection_amount=float(data.get("top_collection_amount") or 0),
        urgency_score=int(data.get("urgency_score") or 50),
        violations=violations,
        recommended_tier=str(data.get("recommended_tier") or "toolkit"),
        fear_hook=str(data.get("fear_hook") or ""),
        urgency_hook=str(data.get("urgency_hook") or ""),
        executive_summary=str(data.get("executive_summary") or ""),
        case_law_cited=list(data.get("case_law_cited") or []),
    )
    logger.info("Analysis complete: %d violations, $%.2f leverage, tier=%s", len(violations), summary.total_estimated_leverage, summary.recommended_tier)
    return summary


def summary_to_ghl_fields(summary):
    return {
        "cr_full_name": summary.full_name or "",
        "cr_fico_range": summary.estimated_fico_range,
        "cr_negative_items": summary.total_negative_items,
        "cr_collections_value": round(summary.total_collections_value, 2),
        "cr_chargeoffs_value": round(summary.total_charge_offs_value, 2),
        "cr_late_payments": summary.total_late_payments,
        "cr_inquiries": summary.hard_inquiries,
        "cr_total_leverage": round(summary.total_estimated_leverage, 2),
        "cr_top_pain_point": summary.top_pain_point,
        "cr_top_collection_name": summary.top_collection_name,
        "cr_top_collection_amount": round(summary.top_collection_amount, 2),
        "cr_urgency_score": summary.urgency_score,
        "cr_recommended_tier": summary.recommended_tier,
        "cr_fear_hook": summary.fear_hook,
        "cr_urgency_hook": summary.urgency_hook,
        "cr_exec_summary": summary.executive_summary,
        "cr_case_law_cited": "; ".join(summary.case_law_cited)[:500],
        "cr_violations_count": len(summary.violations),
        "cr_violations_json": json.dumps([asdict(v) for v in summary.violations])[:2500],
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    paths = [Path(p) for p in sys.argv[1:]]
    result = analyze_report(paths, "Test User", "+15555550000")
    print(json.dumps(asdict(result), indent=2))
