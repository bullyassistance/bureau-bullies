#!/bin/bash
# =====================================================================
# BUREAU BULLIES — one-click local setup
# Double-click this file in Finder (may ask for permission first time).
# =====================================================================
cd "$(dirname "$0")/backend"

echo ""
echo "=============================================="
echo " BUREAU BULLIES — Bully AI setup"
echo "=============================================="
echo ""

# Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
  echo "Creating Python virtual environment..."
  python3 -m venv .venv
fi

source .venv/bin/activate

echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

echo ""
echo "Running GHL setup (testing keys + creating custom fields)..."
echo ""
python setup_ghl.py

echo ""
echo "=============================================="
echo " Starting local server on http://localhost:8000"
echo " Press Ctrl+C to stop."
echo "=============================================="
echo ""

uvicorn app:app --host 0.0.0.0 --port 8000 --reload
