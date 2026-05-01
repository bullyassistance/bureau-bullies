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
    stack: List[str] = []
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
                # Record a position where the outer object was valid
                if not stack:
                    last_safe_cut = i + 1
        elif ch == "]":
            if stack and stack[-1] == "[":
                stack.pop()

    # If we ended mid-string, drop to the last quote + remove trailing junk
    if in_string:
        # Find last unescaped quote before EOF
        j = len(s) - 1
        while j >= 0:
            if s[j] == '"' and (j == 0 or s[j-1] != "\\"):
                s = s[:j]
                break
            j -= 1
        # Rebuild stack from scratch on the truncated string — easier than undoing
        return _repair_truncated_json(s + '"')

    # Remove trailing comma that would otherwise break JSON
    s = re.sub(r",\s*$", "", s)

    # Close open arrays/objects
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
    case_law_citation: str = ""   # NEW — which case supports this violation


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
    case_law_cited: List[str] = field(default_factory=list)  # NEW


# ---- PDF / Image handling ------------------------------------------------
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
    """Extract text from a PDF. Returns (text, needs_vision_fallback)."""
    chunks: List[str] = []
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
    # If more than half the pages are near-empty, it's probably a scanned PDF
    needs_vision = total_pages > 0 and (low_text_pages / total_pages) > 0.5
    return joined, needs_vision


def detect_bureau(text: str) -> str:
    t = text.lower()
    if t.count("experian") > 3:
        return "Experian"
    if t.count("equifax") > 3:
        return "Equifax"
    if t.count("transunion") > 3:
        return "TransUnion"
    return "Unknown"


def pdf_to_images(pdf_path: Path, out_dir: Path, dpi: int = 150) -> List[Path]:
    """Render each PDF page as PNG so we can send scanned pages to Claude vision."""
    try:
        import pypdfium2 as pdfium
    except ImportError:
        logger.warning("pypdfium2 not installed — scanned PDFs will be skipped")
        return []

    out_paths: List[Path] = []
    pdf = pdfium.PdfDocument(str(pdf_path))
    for i, page in enumerate(pdf):
        pil = page.render(scale=dpi / 72).to_pil()
        # Downscale if huge — vision payload limits
        if max(pil.size) > 1568:
            ratio = 1568 / max(pil.size)
            pil = pil.resize((int(pil.size[0] * ratio), int(pil.size[1] * ratio)))
        out = out_dir / f"{pdf_path.stem}_p{i+1}.png"
        pil.save(out, format="PNG", optimize=True)
        out_paths.append(out)
        if i >= 20:  # cap at 20 pages per PDF for vision
            break
    return out_paths


# ---- Prepare Claude message content --------------------------------------
def build_content_blocks(
    pdf_paths: List[Path],
    scratch_dir: Path,
) -> Tuple[list, str]:
    """
    Returns (list of Claude content blocks, summary of what was processed).
    Mixed text + image blocks so a single call handles PDFs, scans, and phone
    screenshots all together.
    """
    blocks: List[dict] = []
    summary_parts: List[str] = []
    text_parts: List[str] = []

    for p in pdf_paths:
        if _is_image(p):
            blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": _image_media_type(p),
                    "data": _file_to_b64(p),
                },
            })
            summary_parts.append(f"IMAGE: {p.name}")
            logger.info("Image queued for vision: %s", p.name)
            continue

        if _is_pdf(p):
            text, needs_vision = extract_pdf_text(p)
            if text and not needs_vision:
                bureau = detect_bureau(text)
                text_parts.append(
                    f"\n\n=== FILE: {p.name}  (BUREAU: {bureau}) ===\n{text}"
                )
                summary_parts.append(f"PDF-TEXT: {p.name} ({bureau})")
                logger.info("Text-extracted: %s (%d chars)", p.name, len(text))
            else:
                # Scanned PDF — rasterize pages and send as images
                images = pdf_to_images(p, scratch_dir)
                for img in images:
                    blocks.append({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": _file_to_b64(img),
                        },
                    })
                summary_parts.append(f"PDF-SCAN: {p.name} ({len(images)} pages rasterized)")
                logger.info("Scanned PDF → %d images: %s", len(images), p.name)

    # Prepend consolidated text block (if any)
    if text_parts:
        combined = "".join(text_parts)
        if len(combined) > MAX_REPORT_CHARS:
            combined = combined[:MAX_REPORT_CHARS] + "\n\n[... truncated ...]"
        blocks.insert(0, {"type": "text", "text": combined})

    return blocks, " | ".join(summary_parts)


# ---- The system prompt — Bully AI voice, cites case law ------------------
SYSTEM_PROMPT = """You are Bully AI — the in-house AI credit assassin for The Bureau Bullies, LLC (Wilmington, DE). You are a forensic credit analyst and FCRA/FDCPA/Metro 2 specialist. You have a name, a personality, and a job: demolish negative items on consumer credit reports by hunting violations and weaponizing leverage.

When you speak, you speak as "Bully AI" — confident, aggressive, specific, zero-fluff. You never mention Claude, Anthropic, or any underlying model. You are Bully AI, period.

Your job right now: read the consumer's 3-bureau credit report (either as text, as rasterized PDF pages, or as screenshot images) and produce an aggressive, dollar-weighted deep-dive analysis.

YOU MUST:

1. Identify EVERY negative tradeline (collections, charge-offs, late payments, repos, bankruptcies, judgments, tax liens).

2. For each negative item, hunt for violations of:
   - FCRA (Fair Credit Reporting Act) — §§ 609, 611, 615
   - **15 U.S.C. § 1681s-2(b)** — the FURNISHER duties (A–E). This is the weapon. Flag every instance where a furnisher's reporting looks like an auto-verify or rubber stamp.
   - FDCPA (Fair Debt Collection Practices Act) — §§ 807 (false/misleading), 809 (validation), 812 (deceptive forms)
   - Metro 2 Compliance — inconsistent reporting across bureaus, missing DOFD, re-aged accounts, duplicate accounts, missing data fields

3. Compare the same account across all three bureaus. Inconsistency = gold.

4. For EACH violation, cite a relevant federal case from this list in the "case_law_citation" field (use the short form):
   - Hinkle v. Midland Credit Management, 800 F.3d 1295 (11th Cir. 2015) — for rubber-stamp / internal-database verification
   - Johnson v. MBNA America Bank, 357 F.3d 426 (4th Cir. 2004) — for failure to investigate the specific claim
   - Gorman v. Wolpoff & Abramson, 584 F.3d 1147 (9th Cir. 2009) — for private right of action & jury questions
   - TransUnion LLC v. Ramirez, 594 U.S. 413 (2021) — for concrete harm / standing
   - Saunders v. Branch Banking & Trust, 526 F.3d 142 (4th Cir. 2008) — for emotional distress damages
   - Cushman v. Trans Union Corp., 115 F.3d 220 (3d Cir. 1997) — for furnisher investigation standards

5. Assign each violation a conservative "dollar_leverage" value:
   - FCRA reporting error: $500–$1,000
   - FDCPA statutory: $1,000 per violation
   - Metro 2 inconsistency across bureaus: $500–$1,500
   - Willful FCRA: $1,000+ plus actual damages

6. Calculate total_estimated_leverage = sum of all dollar_leverage.

7. urgency_score (0-100) — how "hot" this lead is:
   - 80-100: 5+ collections or charge-offs, $10K+ leverage
   - 60-79: 3-4 collections, $5K-$10K leverage
   - 40-59: 1-2 collections, $2K-$5K leverage
   - 0-39: Minor issues, <$2K leverage

8. recommended_tier:
   - "toolkit" ($17 Collection Killer) — clean report or 1-2 negatives
   - "accelerator" ($66 Dispute Vault) — 3-6 violations, needs the full toolkit
   - "dfy" ($229/mo or $2,500) — 7+ violations or $10K+ leverage

9. fear_hook AND urgency_hook — one-liners personalized to the worst item. Examples:
   fear_hook: "That $5,127 Midland Funding collection from 2023 isn't going anywhere — it'll stay on your report 7 more years and shred every approval."
   urgency_hook: "You have 3 clean § 1681s-2(b) violations on Capital One we can dispute this week — every day Capital One re-ages this account, your leverage shrinks."

10. executive_summary — 3-4 sentences that name specific creditors, dollar amounts, and cite ONE case. Example: "You're sitting on 7 negatives worth ~$14,500 in leverage. The $5,127 Midland collection is a textbook Hinkle violation — they're reporting without original documentation. If they rubber-stamp your dispute (and they will), you have a clean federal case."

11. case_law_cited — list of unique case citations used in the violations array.

OUTPUT FORMAT: Respond ONLY with valid JSON that matches the schema. No preamble, no markdown fences. Pure JSON.

Be aggressive. Be specific. Use real creditor names, real dollar amounts, real account endings (last 4). If the report is clean, say so honestly — don't invent violations. If the report is messy, surface EVERYTHING."""


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
  "top_pain_point": "one-line string",
  "top_collection_name": "Creditor name",
  "top_collection_amount": 0.0,
  "urgency_score": 0,
  "recommended_tier": "toolkit|accelerator|dfy",
  "fear_hook": "personalized fear-based one-liner",
  "urgency_hook": "personalized urgency-based one-liner",
  "executive_summary": "3-4 sentences with creditor names, dollar amounts, and one case citation",
  "case_law_cited": ["Hinkle v. Midland", "Saunders v. BB&T"],
  "violations": [
    {{
      "creditor": "Capital One",
      "account_last4": "4532",
      "bureau": "Experian",
      "violation_type": "15 U.S.C. § 1681s-2(b)(1)(A) — Failure to conduct reasonable investigation",
      "description": "Payment history differs between EX and EQ for May 2024. Furnisher rubber-stamped prior disputes.",
      "dollar_leverage": 1000.0,
      "severity": "high",
      "case_law_citation": "Hinkle v. Midland Credit Management, 800 F.3d 1295 (11th Cir. 2015)"
    }}
  ]
}}"""


# ---- Claude call ---------------------------------------------------------
def analyze_report(
    pdf_paths: List[Path],
    consumer_name: str,
    consumer_phone: str,
    scratch_dir: Optional[Path] = None,
    api_key: Optional[str] = None,
) -> ReportSummary:
    """Full pipeline: extract → Claude vision/text analysis → ReportSummary."""
    scratch = scratch_dir or Path("/tmp/bullies_scratch")
    scratch.mkdir(exist_ok=True, parents=True)

    content_blocks, files_summary = build_content_blocks(pdf_paths, scratch)

    if not content_blocks:
        raise ValueError("No readable content extracted. Check file formats.")

    # Append the user instructions as the final text block
    content_blocks.append({
        "type": "text",
        "text": USER_TEMPLATE.format(
            consumer_name=consumer_name,
            consumer_phone=consumer_phone,
            files_summary=files_summary,
        ),
    })

    client = Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))

    logger.info("Calling Bully AI (%s) — %d content blocks", CLAUDE_MODEL, len(content_blocks))
    # Use a generous token budget — violation-heavy reports can produce large JSON.
    # Claude Sonnet 4.6 supports up to 64K output tokens.
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=16000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content_blocks}],
    )

    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    stop_reason = getattr(resp, "stop_reason", None)
    logger.info("Bully AI raw length=%d, stop_reason=%s", len(raw), stop_reason)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        # Attempt 1: response got truncated (max_tokens hit). Try to repair by
        # closing open strings/arrays/objects so we can salvage a partial result.
        logger.warning("Bully AI JSON parse failed (%s). Attempting repair.", e)
        repaired = _repair_truncated_json(raw)
        try:
            data = json.loads(repaired)
            logger.info("JSON repaired successfully — recovered %d chars", len(repaired))
        except json.JSONDecodeError as e2:
            # Attempt 2: retry Claude with explicit "be more concise" instruction
            logger.warning("Repair failed (%s). Retrying Claude with tighter output.", e2)
            retry_content = list(content_blocks)
            retry_content.append({
                "type": "text",
                "text": (
                    "Your previous response was cut off. Return ONLY the JSON object, "
                    "nothing else. Limit to 8 most important violations max. Keep every "
                    "description under 180 characters. Do not repeat yourself."
                ),
            })
            retry = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=16000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": retry_content}],
            )
            raw2 = retry.content[0].text.strip()
            raw2 = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw2, flags=re.MULTILINE).strip()
            try:
                data = json.loads(raw2)
            except json.JSONDecodeError as e3:
                # Last resort: try repair on the retry too
                try:
                    data = json.loads(_repair_truncated_json(raw2))
                except Exception:
                    logger.error("All JSON recovery attempts failed. First 500 chars: %s", raw[:500])
                    raise RuntimeError(
                        "Bully AI returned invalid JSON even after repair + retry. "
                        "This usually means the report was unusually complex. "
                        "Try uploading one bureau at a time."
                    ) from e3

    # Build Violation objects defensively — truncated JSON may leave a partial
    # last violation with missing fields.
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
            logger.warning("Skipping malformed violation: %s (%s)", ve, str(v)[:120])
            continue

    # Build the ReportSummary with safe defaults so a partial response still works
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
    logger.info(
        "Analysis complete: %d violations, $%.2f leverage, tier=%s, urgency=%d",
        len(violations), summary.total_estimated_leverage,
        summary.recommended_tier, summary.urgency_score,
    )
    return summary


# ---- Flatten for GHL custom fields ---------------------------------------
def summary_to_ghl_fields(summary: ReportSummary) -> dict:
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
