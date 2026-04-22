# The Bureau Bullies — Claude-Powered Credit AI

A complete rebuild of your Bully AI stack. Upload a 3-bureau credit report → Claude reads every tradeline, flags FCRA/FDCPA/Metro 2 violations, calculates leverage, and pushes a fully-enriched lead into GoHighLevel for the SMS funnel to close.

```
bureau-bullies/
├── frontend/
│   ├── index.html          # Tesla-grade glassmorphism landing page with 3D shield
│   └── thank-you.html      # Post-scan confirmation
├── backend/
│   ├── app.py              # FastAPI server — routes, uploads, orchestration
│   ├── analyzer.py         # PDF extraction + Claude API deep-dive
│   ├── ghl.py              # GoHighLevel API v2 integration
│   ├── requirements.txt
│   └── .env.example
└── docs/
    └── GHL_WORKFLOW.md     # Full SMS sequence, custom fields, branching logic
```

## 1. What this does

1. **Landing page** captures lead (name, email, phone) + 1-3 PDF credit reports.
2. **FastAPI backend** receives the upload, runs `analyzer.py`:
   - Extracts PDF text with `pdfplumber`
   - Combines all 3 bureaus into one context
   - Sends to Claude with a forensic-analyst system prompt
   - Claude returns structured JSON: violations list, leverage $ value, fear/urgency hooks, recommended tier
3. **GHL integration** upserts contact, populates 18 custom fields with the analysis, tags by tier + urgency, drops the lead into the right workflow.
4. **GHL workflows** (you build these in GHL — spec in `docs/GHL_WORKFLOW.md`) fire a 10-message SMS drip personalized to the contact's specific report.

## 2. Setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# fill in ANTHROPIC_API_KEY, GHL_API_KEY, GHL_LOCATION_ID
```

## 3. Get your credentials

### Anthropic (Claude)
1. Go to https://console.anthropic.com
2. Billing → add payment method, settings → API Keys → create key.
3. Paste into `.env` as `ANTHROPIC_API_KEY`.

### GoHighLevel
1. In your sub-account: **Settings → API → Private Integrations → Create**.
2. Give it these scopes minimum: `contacts.readonly`, `contacts.write`, `locations/customFields.readonly`, `workflows.readonly`.
3. Copy the token → `GHL_API_KEY` in `.env`.
4. Sub-account ID is in the URL when you're in Settings → it's the `GHL_LOCATION_ID`.

### Create the custom fields
Follow the table in `docs/GHL_WORKFLOW.md` section 1. Create all 18 `cr_*` fields in GHL **before** running your first scan.

### Build the workflows
`docs/GHL_WORKFLOW.md` section 2–4 has the full branching logic and SMS copy. Copy each message in verbatim. Replace `[Toolkit URL]`, `[Accel URL]`, and `[Calendar URL]` with your actual checkout/booking links.

Grab each workflow ID from its URL in GHL and paste into `.env` under `GHL_WORKFLOW_TOOLKIT`, `GHL_WORKFLOW_ACCELERATOR`, `GHL_WORKFLOW_DFY`.

## 4. Run locally

```bash
cd backend
uvicorn app:app --reload --port 8000
```

Visit `http://localhost:8000`. Drop in a test credit report PDF. Watch the logs — you should see Claude analyze it and GHL upsert the contact.

## 5. Deploy

### Option A — Railway (fastest)
```bash
# in the backend/ folder
railway init
railway up
railway variables set ANTHROPIC_API_KEY=... GHL_API_KEY=... GHL_LOCATION_ID=...
```

### Option B — Render
Create a new Web Service from this repo, root directory `backend`, build command `pip install -r requirements.txt`, start command `uvicorn app:app --host 0.0.0.0 --port $PORT`.

### Option C — Drop-in on your GHL funnel
If you want the landing page served by GHL and just the `/api/scan` endpoint hosted externally:
1. Upload `frontend/index.html` to GHL as a custom funnel page.
2. Change the `fetch('/api/scan', ...)` call at the bottom of `index.html` to `fetch('https://your-api.com/api/scan', ...)`.
3. Host just the `backend/` folder on Railway/Render — it'll serve the API and the `/thank-you` page.

## 6. What Claude actually returns

A single scan produces something like:

```json
{
  "full_name": "Marcus Harris",
  "estimated_fico_range": "540-580",
  "total_negative_items": 7,
  "total_collections_value": 8327.00,
  "total_chargeoffs_value": 3200.00,
  "total_late_payments": 9,
  "hard_inquiries": 12,
  "total_estimated_leverage": 14500.00,
  "top_pain_point": "$5,127 Midland Funding collection from 2023 destroying your score",
  "top_collection_name": "Midland Funding LLC",
  "top_collection_amount": 5127.00,
  "urgency_score": 87,
  "recommended_tier": "accelerator",
  "fear_hook": "That $5,127 Midland collection from '23 isn't going anywhere — it'll stay 7 more years.",
  "urgency_hook": "3 Metro 2 violations on Capital One we can dispute this week while they're vulnerable.",
  "executive_summary": "Marcus has 7 serious negatives across all 3 bureaus...",
  "violations": [
    {
      "creditor": "Capital One",
      "account_last4": "4532",
      "bureau": "Experian",
      "violation_type": "Metro 2 — Inconsistent reporting",
      "description": "Payment history for May 2024 differs from Equifax and TU.",
      "dollar_leverage": 1000.0,
      "severity": "high"
    }
  ]
}
```

Every one of those fields lands in a GHL custom field and can be merged into any SMS message.

## 7. Tuning

- **`CLAUDE_MODEL`** — swap to `claude-opus-4-6` for the deepest analysis (slower, pricier). Sonnet is the sweet spot.
- **`MAX_REPORT_CHARS`** — bump up if you're hitting truncation. Claude's context is big.
- **`MAX_FILE_MB`** — raise if clients have bulky scanned PDFs.
- **System prompt** — lives in `analyzer.py`. This is where you lean into voice: more aggressive, more specific, more legal-citation-heavy. Edit it freely.

## 8. Known limits

- Scanned-image PDFs (no text layer) won't extract. Add OCR with `pytesseract` if you start seeing these.
- GHL rate-limits at 100 req/10s per location — fine for 99% of use, but if you're running a mass-import from an old list you'll want to add throttling.
- The `cr_violations_json` field is capped at 2,500 chars. For bigger reports, dump the full JSON to S3 and store just the URL.
