"""
Bureau Bullies — One-time GHL setup script
-------------------------------------------
Run this once after you set your .env keys. It will:

  1. Test your Anthropic API key (sends "hello" to Bully AI)
  2. Test your GHL API key (lists your existing custom fields)
  3. Auto-create the 20 required custom fields in your GHL sub-account
     (skips any that already exist by the same key)

Usage:
  cd backend
  python setup_ghl.py
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")


def step(n: str, title: str):
    print()
    print("=" * 64)
    print(f" STEP {n}  —  {title}")
    print("=" * 64)


def ok(msg: str):   print(f"  ✓ {msg}")
def warn(msg: str): print(f"  ! {msg}")
def err(msg: str):  print(f"  ✗ {msg}")


def test_anthropic():
    step("1", "Testing Anthropic API key (Bully AI)")
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = client.messages.create(
            model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6"),
            max_tokens=40,
            messages=[{"role": "user", "content": "Reply with exactly: Bully AI online and ready."}],
        )
        reply = resp.content[0].text.strip()
        ok(f"Claude replied: {reply}")
        return True
    except Exception as e:
        err(f"Anthropic test failed: {e}")
        return False


def test_ghl():
    step("2", "Testing GHL API key")
    try:
        from ghl import GHLClient
        g = GHLClient()
        ok(f"Auth type: {g.version}")
        ok(f"Location ID: {g.location_id}")
        fields = g.list_custom_fields()
        ok(f"Reached GHL successfully — {len(fields)} existing custom fields on your account")
        return g, fields
    except Exception as e:
        err(f"GHL test failed: {e}")
        return None, None


def create_fields(client, existing_fields):
    step("3", "Creating 20 Bureau Bullies custom fields in GHL")
    from ghl import REQUIRED_FIELDS, GHLError

    # Build set of existing keys for dedupe
    existing_keys = set()
    for f in existing_fields or []:
        k = (f.get("fieldKey") or f.get("name") or "").replace("contact.", "")
        existing_keys.add(k)

    created = 0
    skipped = 0
    failed  = 0

    for field_key, name, dtype in REQUIRED_FIELDS:
        if field_key in existing_keys or f"contact.{field_key}" in existing_keys:
            warn(f"Skipping '{name}' — already exists")
            skipped += 1
            continue
        try:
            client.create_custom_field(name=name, field_key=field_key, data_type=dtype)
            ok(f"Created '{name}' ({field_key}, {dtype})")
            created += 1
        except GHLError as e:
            err(f"Failed '{name}': {e}")
            failed += 1

    print()
    print(f"  Summary:  {created} created  ·  {skipped} skipped  ·  {failed} failed")
    return failed == 0


def main():
    ok("Loaded .env")

    a = test_anthropic()
    g, fields = test_ghl()

    if not (a and g):
        print()
        err("Fix the errors above before continuing.")
        sys.exit(1)

    success = create_fields(g, fields)

    print()
    print("=" * 64)
    if success:
        print(" ALL DONE — Bully AI stack is connected and ready to fire.")
        print("=" * 64)
        print()
        print(" Next steps:")
        print("   1. Run the backend:    uvicorn app:app --reload --port 8000")
        print("   2. Visit:              http://localhost:8000")
        print("   3. Build 3 workflows in GHL (see docs/GHL_WORKFLOW.md)")
        print("   4. Paste workflow IDs into .env (GHL_WORKFLOW_*)")
        print("   5. Deploy to Railway (see docs/DEPLOY_TO_GHL.md)")
    else:
        print(" Setup completed with some errors — review above.")
        print("=" * 64)


if __name__ == "__main__":
    main()
