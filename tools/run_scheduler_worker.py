#!/usr/bin/env python3
"""Dedicated scheduler worker for Render Cron/Worker.

Usage on Render Cron:
  cd backend && python ../tools/run_scheduler_worker.py

This dispatches due scheduler rows once and exits. Running it every 1-5 minutes
is more reliable than relying only on the FastAPI in-process loop.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
sys.path.insert(0, str(BACKEND))

from scheduler import dispatch_due_once  # noqa: E402

if __name__ == "__main__":
    n = dispatch_due_once()
    print({"ok": True, "dispatched": n})
