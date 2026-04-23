"""
Bureau Bullies — Email Drip Generator
--------------------------------------
After a scan, this module uses Claude to generate 7 fully tailored email
subject + body pairs that will be dripped out over 14 days.

Each email is personalized with:
  - Their scan data (top collection, dollar amount, violations, case law)
  - Their stated goal (house / car / business / credit card / personal freedom)
  - The exact pain point the collection blocks
  - Amazon-style loss-aversion framing ("you might miss out on the house")

CRITICAL RULES:
  - Never use the word "killer" (carrier spam trigger)
  - Never use "Collection Killer" — always say "Collection Toolkit"
  - The $17 link is always https://thebureaubullies.com/ck
  - Subject lines 6-10 words max
  - Body 80-140 words for emails 1-3, 120-180 for emails 4-7
  - Each email references specific scan facts and the goal
  - Close every email with ONE clear CTA
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional

from anthropic import Anthropic

logger = logging.getLogger("bureau-bullies.email_gen")

CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

GOAL_FRAMES = {
    "house": "buying a house (lenders will deny this and they WILL pull hard)",
    "car":   "buying a car (you'll pay 12-18% APR instead of 4-5%)",
    "business": "starting a business (no business credit, no SBA loan, no vendor terms)",
    "credit_card": "getting a real credit card (auto-decline from anything above a $500 secured limit)",
    "freedom": "freedom from the collectors, the garnishments, the constant phone calls",
    "peace": "peace of mind and financial freedom",
}

SEQUENCE = [
    {"day": 0, "intent": "Welcome + attack plan delivery. Reference their specific collection and leverage total. CTA = reply or click to walk through the plan.", "pitch": "neutral informational"},
    {"day": 1, "intent": "Connect their GOAL to their specific collection. Loss-aversion framing. CTA = $17 Collection Toolkit.", "pitch": "goal punch — $17 Collection Toolkit at https://thebureaubullies.com/ck"},
    {"day": 3, "intent": "Statute-of-limitations fear. Their collection has X years left to sue. Cite one federal case. CTA = $17 Collection Toolkit.", "pitch": "fear SOL + $17 Toolkit"},
    {"day": 5, "intent": "Case-law proof. Walk through specific violation + how Hinkle/Gorman/Saunders works. Social proof. CTA = $17 Toolkit or upsell $66 Vault.", "pitch": "proof + toolkit OR vault upgrade"},
    {"day": 7, "intent": "Dispute Vault upsell. 'You got the toolkit (or you didn't) — the $66 Vault has letters PRE-WRITTEN for your report.' CTA = $66 Vault.", "pitch": "$66 Dispute Vault upsell"},
    {"day": 10, "intent": "DFY lifeline for high-leverage leads. 'You've got $X sitting there — let our squad sue them WITH you.' CTA = DFY link + call.", "pitch": "DFY $229/mo or $2,500"},
    {"day": 14, "intent": "Last email. Soft ultimatum. Three options or reply DONE to leave list.", "pitch": "triage — all three tiers or unsubscribe"},
]


SYSTEM_PROMPT = """You are Bully AI — the in-house credit assassin for The Bureau Bullies, LLC (Wilmington, DE). You write marketing emails that convert.

YOUR VOICE:
- Confident, specific, zero fluff. Like a credit attorney who has seen this game.
- First-person. "I'm Bully AI" / "I pulled your report"
- Urgency without clickbait. Fear framing that's honest.
- Never say "Collection Killer" — always "Collection Toolkit" (carriers flag "killer" as spam).
- Never mention Claude, Anthropic, or the underlying model.

YOUR JOB:
Generate EXACTLY ONE email — subject + body — for the day/intent given. The email must:
1. Open with the consumer's first name.
2. Reference their SPECIFIC top collection by name and dollar amount.
3. Reference their SPECIFIC goal (house/car/business/etc.) — tie the collection directly to what it's blocking.
4. Reference their actual leverage $ and violation count.
5. Cite one real federal case from their scan when relevant (Hinkle, Gorman, Saunders, Cushman, Johnson, Ramirez).
6. Have ONE clear CTA at the end.
7. Subject line: 6–10 words, includes first name OR a dollar amount, urgent not spammy.

THE PRODUCTS:
- $17 Collection Toolkit — https://thebureaubullies.com/ck
- $66 Dispute Vault — https://thecollectionkiller.com/dispute-vault
- DFY — https://suethemallwithus.com/upgrade-credit-repair-67-off

OUTPUT FORMAT:
Respond ONLY with valid JSON:
{"subject": "...", "body": "Hi {first_name},\\n\\n..."}
NO preamble, NO markdown fences. Body uses \\n for line breaks. Length 80-180 words."""


def _scrub_killer(text):
    return re.sub(r"\bCollection Killer\b", "Collection Toolkit", text or "")


def generate_email(consumer_first, scan, day_index, goal_key, goal_label, api_key=None):
    if day_index < 0 or day_index >= len(SEQUENCE):
        raise ValueError(f"day_index out of range: {day_index}")
    step = SEQUENCE[day_index]
    goal_frame = GOAL_FRAMES.get(goal_key) or goal_label or "financial freedom"
    user_msg = f"""Write email #{day_index + 1} of 7 in the nurture sequence.

DAY: {step['day']}
INTENT: {step['intent']}
PITCH FOCUS: {step['pitch']}

CONSUMER:
  First name: {consumer_first}
  Goal: {goal_label}   ({goal_frame})

SCAN DATA:
  Top collection: {scan.get('top_collection_name', 'N/A')} (${scan.get('top_collection_amount', 0):.0f})
  Total leverage: ${scan.get('total_leverage', 0):.0f}
  Violations: {scan.get('violations_count', 0)}
  FICO range: {scan.get('fico_range', 'unknown')}
  Top pain point: {scan.get('top_pain_point', '')}
  Fear hook: {scan.get('fear_hook', '')}
  Case law: {scan.get('case_law_cited', '')}
  Recommended tier: {scan.get('recommended_tier', 'toolkit')}

RULES:
- Subject 6-10 words, name or dollar amount, urgent not clickbait.
- Body 80-180 words. First line addresses {consumer_first} by name.
- Cite the specific collection + dollar + goal.
- Exactly ONE CTA with the right URL.
- Never say "Collection Killer" — use "Collection Toolkit".
- Return JSON only."""
    client = Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
    resp = client.messages.create(model=CLAUDE_MODEL, max_tokens=900, system=SYSTEM_PROMPT, messages=[{"role": "user", "content": user_msg}])
    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("Email %d JSON parse failed (%s). Raw: %s", day_index, e, raw[:200])
        data = {
            "subject": f"{consumer_first} — your ${scan.get('total_leverage', 0):.0f} attack plan",
            "body": f"Hi {consumer_first},\n\nI pulled your report and flagged {scan.get('violations_count', 0)} violations worth about ${scan.get('total_leverage', 0):.0f} in leverage. That {scan.get('top_collection_name', 'top item')} account alone is blocking you from {goal_frame}.\n\nHit reply if you want me to walk you through the plan.\n\n— Bully AI",
        }
    subject = _scrub_killer(str(data.get("subject", "")).strip())[:160]
    body = _scrub_killer(str(data.get("body", "")).strip())
    if not subject or not body:
        raise RuntimeError(f"Email {day_index} generated empty subject/body")
    return {"subject": subject, "body": body}


def generate_full_sequence(consumer_first, scan, goal_key, goal_label, api_key=None):
    out = []
    for i, step in enumerate(SEQUENCE):
        try:
            email = generate_email(consumer_first, scan, i, goal_key, goal_label, api_key)
            out.append({"day": step["day"], "subject": email["subject"], "body": email["body"]})
            logger.info("Email %d/7 generated (day %d): %r", i + 1, step["day"], email["subject"][:60])
        except Exception as e:
            logger.exception("Failed to generate email %d: %s", i, e)
            out.append({
                "day": step["day"],
                "subject": f"{consumer_first} — quick follow-up on your scan",
                "body": f"Hi {consumer_first},\n\nFollowing up on the report scan. I found {scan.get('violations_count', 0)} violations worth about ${scan.get('total_leverage', 0):.0f} in leverage — real negotiating power against {scan.get('top_collection_name', 'these collectors')}.\n\nWant me to walk you through how to use it? Reply YES.\n\n— Bully AI",
            })
    return out


def emails_to_ghl_fields(emails):
    out = {}
    for i, email in enumerate(emails, start=1):
        out[f"cr_email_{i}_subject"] = email.get("subject", "")
        out[f"cr_email_{i}_body"] = email.get("body", "")
    return out
