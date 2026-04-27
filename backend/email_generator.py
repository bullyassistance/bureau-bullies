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

# Goal-to-pain-framing map. Used in the prompt to frame loss aversion AND
# the transformation we're selling them on.
GOAL_FRAMES = {
    "house": "buying a house (lenders pull all 3 bureaus and that collection alone disqualifies them from FHA approval)",
    "car":   "buying a car (12-18% APR on a $35K loan instead of 4-5% — that's $200/month of pure penalty for that collection)",
    "business": "starting a business (no SBA loan, no vendor net-30, no business line of credit, every door slammed because the bureaus see them as a risk)",
    "credit_card": "getting a real credit card (every auto-decline above $500 secured is that collection blocking them — Chase, Amex, Capital One all see it)",
    "freedom": "real freedom (no more 855 numbers, no more wondering if today is the day they get served, no more mailbox dread)",
    "peace": "peace of mind (sleeping through the night without the credit anxiety eating at the back of their head)",
}

# Goal-to-transformation map. The "after" picture for PAS.
GOAL_TRANSFORMATION = {
    "house":       "keys in their hand, mortgage approved, walking through their front door",
    "car":         "driving off the lot with a real car, real APR, no co-signer needed",
    "business":    "EIN funded, SBA approved, vendors saying yes, business credit profile that opens doors",
    "credit_card": "approved for premium cards, real limits, real points, real respect when they swipe",
    "freedom":     "phone silent, mailbox clean, score in the 700s, options instead of obstacles",
    "peace":       "credit reports clean, score climbing, no more anxiety every time a bill comes",
}

# The 7-email sequence cadence + intent. Every email runs through the
# PAS + Transformation framework defined in SYSTEM_PROMPT.
SEQUENCE = [
    {
        "day": 0,
        "intent": (
            "PAIN: open by naming their #1 collection (creditor + dollar). "
            "AGITATE: that creditor is reporting to all 3 bureaus right now and is inside SOL. "
            "SOLVE: their personalized attack plan doc is ready, walk them through it. "
            "TRANSFORMATION: tie it to their goal (house/car/business/etc.)."
        ),
        "pitch": "deliver the attack plan doc, drive them to reply or click for a walkthrough",
    },
    {
        "day": 1,
        "intent": (
            "PAIN: that one specific collector is what's blocking them from their goal, period. "
            "AGITATE: list one concrete consequence specific to the goal (mortgage denial, 18% APR, SBA rejection, auto-decline). "
            "SOLVE: the $17 Collection Toolkit teaches them exactly how to use 15 USC 1681s-2(b) to delete it. "
            "TRANSFORMATION: paint the after-picture for their picked goal in one line."
        ),
        "pitch": "$17 Collection Toolkit via https://thebureaubullies.com/ck",
    },
    {
        "day": 3,
        "intent": (
            "PAIN: that creditor still has X years left on the SOL clock and they're profitable on accounts this size. "
            "AGITATE: cite one real federal case from their scan and explain that this collector files lawsuits regularly, paint the served-with-papers scenario. "
            "SOLVE: $17 Collection Toolkit walks them through the validation letter that stops 70% of these dead. "
            "TRANSFORMATION: reframe the goal as the prize on the other side of one validation letter."
        ),
        "pitch": "fear + SOL clock + $17 Collection Toolkit",
    },
    {
        "day": 5,
        "intent": (
            "PAIN: name the specific 1681s-2(b) violation Bully AI flagged on this collector. "
            "AGITATE: that violation gives them statutory damages of $100 to $1,000 per occurrence per Hinkle / Gorman / Saunders. The collector knows this and bets on consumer silence. "
            "SOLVE: $17 Collection Toolkit if DIY, or $66 Dispute Vault for pre-written letters with the case law baked in. "
            "TRANSFORMATION: ones who pull the trigger get to the goal, the rest stay stuck."
        ),
        "pitch": "case-law proof + toolkit OR vault upgrade",
    },
    {
        "day": 7,
        "intent": (
            "PAIN: a week has passed and the collection is still hitting their score every day they don't act. "
            "AGITATE: every morning they wake up, the bureau scoring algorithm is recalculating against them. Late fee on the lease application is real money. "
            "SOLVE: $66 Dispute Vault has every letter pre-written for THEIR collector, plug in their info, mail certified, watch deletion in 30-60 days. "
            "TRANSFORMATION: tie cleared collection directly to the goal, the door that opens once the bureau sees it gone."
        ),
        "pitch": "$66 Dispute Vault upsell",
    },
    {
        "day": 10,
        "intent": (
            "PAIN: by now they've either acted or watched 10 days slip by while the collection earned interest against them. "
            "AGITATE: they have $X sitting in leverage they could be using right now. Most consumers leave it on the table forever, the collector wins by default. "
            "SOLVE: DFY service (45% off) means our squad sues them WITH them. We pull, audit, dispute, escalate, file. They show up to consults, we do the war. "
            "TRANSFORMATION: their goal becomes inevitable instead of theoretical, we don't lose."
        ),
        "pitch": "DFY $229/mo or $2,500 one-time, 45% off",
    },
    {
        "day": 14,
        "intent": (
            "PAIN: 14 days, the same collection is still there, the goal is still on hold. "
            "AGITATE: this is the last email of this sequence. After this we either start working together or they fade back into the 80% who scan and never act. "
            "SOLVE: three doors open, $17 Toolkit (DIY), $66 Vault (templates), DFY (we run it for them). Or reply DONE to leave the list. "
            "TRANSFORMATION: the goal is a choice they make today, not a hope they have."
        ),
        "pitch": "triage all three tiers, or unsubscribe",
    },
]


SYSTEM_PROMPT = """You are Bully AI — the in-house credit assassin for The Bureau Bullies, LLC (Wilmington, DE). You write marketing emails that convert by following the PAS + Transformation framework.

YOUR VOICE:
- Confident, specific, zero fluff. You talk like someone who has seen every angle of this game and gives real legal leverage.
- First-person. "I'm Bully AI" / "I pulled your report"
- Urgency without clickbait. Fear framing that's honest, the collector IS inside the SOL, they CAN sue, etc.
- Never say "Collection Killer", always "Collection Toolkit" (carriers flag "killer" as spam).
- Never mention Claude, Anthropic, or the underlying model.
- PLAIN TEXT ONLY. No markdown asterisks, no underscores, no backticks. NO em dashes. Use commas, periods, or "and" instead.

==================================================================
THE PAS + TRANSFORMATION FRAMEWORK (every email follows this)
==================================================================

Every body MUST move through these four beats in this order:

BEAT 1: PAIN (open with their specific pain by name)
  - Name their #1 collector by name and exact dollar amount.
  - Make them feel the weight of THAT specific debt. Not "your collections", but "$3,500 sitting with Portfolio Recovery."
  - This is the line they need to feel in their chest. Specific. Personal. Real.

BEAT 2: AGITATE (show how that creditor is destroying their life)
  - Name what that specific creditor is DOING to them right now:
    * They're inside the statute of limitations and they CAN sue (and they do, regularly, for accounts as small as $200).
    * They report to all 3 bureaus, killing the score every month.
    * They blocked the last loan / car / apartment / card application without the consumer even knowing it was them.
    * They sell the debt, then re-buy it, then re-age it, then sell it again.
  - Make it visceral. The collector is profiting on the consumer's silence.
  - Pick ONE concrete consequence specific to their goal: lawsuit, garnishment, denied mortgage, 18% APR, business loan rejection.

BEAT 3: SOLVE (introduce the path out, tied to the product for this email)
  - This email's product (Collection Toolkit / Dispute Vault / DFY) is the weapon.
  - Reference 15 USC 1681s-2(b) or one of their specific cited cases (Hinkle, Gorman, Saunders, Cushman, Johnson, Ramirez) when relevant.
  - Show this is REAL legal leverage, not magic, not credit-repair-snake-oil.
  - State the specific outcome: "remove that account permanently", "force them to delete or pay you statutory damages", "stop the lawsuit before it starts".

BEAT 4: TRANSFORMATION (sell the AFTER picture, centered on their picked goal)
  - Paint the goal-specific transformation:
    * If goal=house: keys, mortgage approved, walking through their front door.
    * If goal=car: real APR, no co-signer, real car, real freedom on the road.
    * If goal=business: EIN funded, SBA approved, vendor terms, doors opening.
    * If goal=credit_card: approved for premium cards, real limits, real respect.
    * If goal=freedom: silent phone, clean mailbox, options instead of obstacles.
  - Connect dots: that one collection removed, score moves up, doors that were closed are open.
  - End with the CTA. ONE link. ONE action. Match the day's pitch focus (toolkit / vault / DFY).

==================================================================
SUBJECT LINE RULES (critical, the whole sequence lives or dies on these)
==================================================================
- MUST start with the consumer's first name followed by a comma (NOT an em-dash, never use em-dashes).
- MUST reference ONE specific thing from THEIR actual report: the top creditor name, a specific dollar amount, their exact violation count, a specific bureau, or a specific case citation.
- NEVER use product names in the subject ("Collection Toolkit", "Dispute Vault", "DFY"). Save product names for the body.
- Length: 6 to 10 words, lowercase conversational feel, never ALL CAPS.
- Voice: an insider who just looked at their file and is texting them privately, not a marketing blast.
- Good examples:
    "Koby, I just pulled your Experian Equifax and TransUnion"
    "Koby, that $7,000 Portfolio Recovery is your real problem"
    "Koby, Portfolio Recovery could sue you this month"
    "Koby, Hinkle v. Midland is basically your Portfolio Recovery case"
    "Koby, I already wrote all 9 of your dispute letters"
- Bad examples (never do these):
    "URGENT: Save $14,500 today!"
    "Your Collection Killer is ready"
    "Buy the Dispute Vault now"

==================================================================
THE PRODUCTS (pick the right one for this email's pitch focus)
==================================================================
- $17 Collection Toolkit (ebook), https://thebureaubullies.com/ck, for DIY, first impulse buy.
- $66 Dispute Vault, https://thebureaubullies.com/dispute-vault, pre-written dispute letters, case law cheat sheet, tracker.
- DFY, https://suethemallwithus.com/upgrade-credit-repair-67-off, $229/mo or $2,500 one-time (45% off), our squad handles everything.

==================================================================
OUTPUT FORMAT
==================================================================
Respond ONLY with valid JSON:
{
  "subject": "...",
  "body": "Hi {first_name},\\n\\n..."
}

NO preamble, NO markdown fences, just the JSON. Body uses \\n for line breaks. Keep total body length 120-220 words. Each PAS beat gets its own paragraph (4 paragraphs minimum)."""


def _scrub_killer(text: str) -> str:
    """Safety net: remove any accidental 'Killer' wording that Claude might produce."""
    return re.sub(r"\bCollection Killer\b", "Collection Toolkit", text or "")


def generate_email(
    consumer_first: str,
    scan: dict,
    day_index: int,
    goal_key: str,
    goal_label: str,
    api_key: Optional[str] = None,
) -> dict:
    """
    Generate one email in the sequence.

    Args:
      consumer_first: first name
      scan: dict of scan data (top_collection_name, top_collection_amount,
            total_leverage, violations_count, fico_range, case_law_cited,
            top_pain_point, fear_hook, recommended_tier)
      day_index: 0..6
      goal_key: one of the GOAL_FRAMES keys
      goal_label: human-readable goal ("House", "Car", etc.)
    Returns:
      {"subject": "...", "body": "..."}
    """
    if day_index < 0 or day_index >= len(SEQUENCE):
        raise ValueError(f"day_index out of range: {day_index}")

    step = SEQUENCE[day_index]
    goal_frame = GOAL_FRAMES.get(goal_key) or goal_label or "financial freedom"
    goal_transformation = GOAL_TRANSFORMATION.get(goal_key) or "the financial freedom they're chasing"

    user_msg = f"""Write email #{day_index + 1} of 7 in the nurture sequence using the PAS + Transformation framework.

DAY: {step['day']}
INTENT (4-beat plan for THIS email): {step['intent']}
PITCH FOCUS: {step['pitch']}

CONSUMER:
  First name: {consumer_first}
  Goal label: {goal_label}
  Goal pain frame (what the goal is REALLY about): {goal_frame}
  Goal transformation (the AFTER picture): {goal_transformation}

SCAN DATA (use this to make pain SPECIFIC):
  Top collection: {scan.get('top_collection_name', 'N/A')} (${scan.get('top_collection_amount', 0):.0f})
  Total leverage: ${scan.get('total_leverage', 0):.0f}
  Violations: {scan.get('violations_count', 0)}
  FICO range: {scan.get('fico_range', 'unknown')}
  Top pain point: {scan.get('top_pain_point', '')}
  Fear hook: {scan.get('fear_hook', '')}
  Case law cited on their report: {scan.get('case_law_cited', '')}
  Recommended tier: {scan.get('recommended_tier', 'toolkit')}

EMAIL STRUCTURE (all four beats required, in order, one paragraph each):

  PARAGRAPH 1 - PAIN:
    Open with "Hi {consumer_first}," then ONE punchy line that names the
    top collection by name and dollar amount. Make them feel the weight.

  PARAGRAPH 2 - AGITATE:
    Show how that SPECIFIC creditor is destroying them. Pick ONE concrete
    consequence specific to their goal ({goal_label}). Reference the SOL clock
    or lawsuit risk where it fits. Make it visceral.

  PARAGRAPH 3 - SOLVE:
    Introduce this email's product as the WEAPON. Cite 15 USC 1681s-2(b)
    or one of the case law names from their scan when relevant. State the
    specific outcome (delete the account, force damages, stop the lawsuit).

  PARAGRAPH 4 - TRANSFORMATION + CTA:
    Paint the AFTER picture using the transformation: "{goal_transformation}".
    Connect dots: collection cleared, score moves, doors open. End with ONE
    line that contains the CTA URL for this email's pitch focus.

HARD RULES:
- Subject 6-10 words, starts with "{consumer_first}," then ONE specific report fact (creditor / dollar / case / bureau).
- Body 120-220 words. NO em dashes anywhere. Use commas or periods.
- NO markdown formatting. No asterisks, no underscores, no bold.
- Do NOT say "Collection Killer", use "Collection Toolkit".
- Exactly ONE CTA URL matching this email's pitch focus.
- Return JSON only, no preamble."""

    client = Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=900,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("Email %d JSON parse failed (%s). Raw: %s", day_index, e, raw[:200])
        # Graceful fallback — build a minimal email from scan data so the drip still fires
        data = {
            "subject": f"{consumer_first} — your ${scan.get('total_leverage', 0):.0f} attack plan",
            "body": (
                f"Hi {consumer_first},\n\n"
                f"I pulled your report and flagged {scan.get('violations_count', 0)} violations "
                f"worth about ${scan.get('total_leverage', 0):.0f} in leverage. "
                f"That {scan.get('top_collection_name', 'top item')} account alone is "
                f"blocking you from {goal_frame}.\n\n"
                f"Hit reply if you want me to walk you through the plan.\n\n"
                "— Bully AI"
            ),
        }

    subject = _scrub_killer(str(data.get("subject", "")).strip())[:160]
    body = _scrub_killer(str(data.get("body", "")).strip())

    if not subject or not body:
        raise RuntimeError(f"Email {day_index} generated empty subject/body")

    return {"subject": subject, "body": body}


def generate_full_sequence(
    consumer_first: str,
    scan: dict,
    goal_key: str,
    goal_label: str,
    api_key: Optional[str] = None,
) -> list:
    """
    Generate all 7 emails in the sequence. Each email gets its own Claude call
    so they can focus properly and stay within per-email token budgets.

    Returns a list of 7 dicts: [{"day": 0, "subject": "...", "body": "..."}, ...]
    """
    out = []
    for i, step in enumerate(SEQUENCE):
        try:
            email = generate_email(
                consumer_first=consumer_first,
                scan=scan,
                day_index=i,
                goal_key=goal_key,
                goal_label=goal_label,
                api_key=api_key,
            )
            out.append({
                "day": step["day"],
                "subject": email["subject"],
                "body": email["body"],
            })
            logger.info("Email %d/7 generated (day %d): %r", i + 1, step["day"], email["subject"][:60])
        except Exception as e:
            logger.exception("Failed to generate email %d: %s", i, e)
            # Fallback so the drip always has 7 entries
            out.append({
                "day": step["day"],
                "subject": f"{consumer_first} — quick follow-up on your scan",
                "body": (
                    f"Hi {consumer_first},\n\n"
                    f"Following up on the report scan I ran for you. "
                    f"I found {scan.get('violations_count', 0)} violations worth about "
                    f"${scan.get('total_leverage', 0):.0f} in leverage — which means real "
                    f"negotiating power against {scan.get('top_collection_name', 'these collectors')}.\n\n"
                    f"Want me to walk you through how to use it? Reply YES.\n\n"
                    "— Bully AI"
                ),
            })
    return out


def emails_to_ghl_fields(emails: list) -> dict:
    """
    Flatten the 7 emails into cr_email_1_subject, cr_email_1_body, ...
    cr_email_7_subject, cr_email_7_body GHL custom fields.
    """
    out = {}
    for i, email in enumerate(emails, start=1):
        out[f"cr_email_{i}_subject"] = email.get("subject", "")
        out[f"cr_email_{i}_body"] = email.get("body", "")
    return out
