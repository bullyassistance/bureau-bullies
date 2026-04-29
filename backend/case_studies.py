"""
Real customer wins — used as social proof in the post-scan email drip and
the qualifier email sequence.

Source: real Bureau Bullies clients. Update this list as new wins close.

Format for each case:
  - first_name: customer's first name only (privacy)
  - headline:   one-line outcome (what got deleted + dollar impact)
  - detail:     1-sentence elaboration that makes it concrete

The drip rotates these so every email has a different proof point.
"""

from __future__ import annotations

from typing import Optional

CASE_STUDIES: list[dict] = [
    {
        "first_name": "Idris",
        "headline": "killed 4 charge-offs and pulled $2,000 from Equifax",
        "detail": (
            "Idris had 4 charge-offs dragging his Equifax report. After the dispute "
            "playbook went out, all 4 came off and Equifax cut a $2,000 check for "
            "the FCRA violations. That's not a settlement — that's the bureau "
            "writing him a check."
        ),
    },
    {
        "first_name": "Bryan",
        "headline": "got an Amex charge-off deleted plus $3,700 paid back",
        "detail": (
            "Bryan was sitting on an Amex charge-off he thought was untouchable. "
            "We disputed the reporting, Amex couldn't validate the way it was "
            "furnished, and the account got deleted. He also collected $3,700 "
            "directly from Amex on top of the deletion."
        ),
    },
    {
        "first_name": "Delores",
        "headline": "killed a $75,000 debt collector and netted a $5,000 settlement",
        "detail": (
            "Delores had a $75,000 debt collector account on her file that was "
            "blocking everything. After the dispute + settlement playbook ran, "
            "the collector deleted the account and paid Delores a $5,000 "
            "settlement to walk away. $75K cleared, $5K in pocket."
        ),
    },
]


def get_for_email(email_index: int) -> Optional[dict]:
    """Return one case study to include in the body of email N (1-7).

    Rotates through the list so a contact sees all 3 across the 7-email drip:
      Email 1 → Idris
      Email 2 → Bryan
      Email 3 → Delores
      Email 4 → Idris
      Email 5 → Bryan
      Email 6 → Delores
      Email 7 → Idris

    If CASE_STUDIES is empty, returns None.
    """
    if not CASE_STUDIES:
        return None
    idx = max(0, (email_index - 1)) % len(CASE_STUDIES)
    return CASE_STUDIES[idx]


def format_for_email(email_index: int) -> str:
    """Return a plain-text P.S. block ready to append to an email body.
    Returns empty string if no case study is available."""
    cs = get_for_email(email_index)
    if not cs:
        return ""
    return (
        "\n\n"
        "P.S. Real customer win this week:\n"
        f"{cs['first_name']} {cs['headline']}.\n"
        f"{cs['detail']}"
    )
