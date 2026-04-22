# Deploying Bureau Bullies to GHL (Replacing bullyaiagent.com)

This guide walks through shipping the new Bully AI stack and replacing your existing `bullyaiagent.com` funnel on GoHighLevel.

## The Architecture

```
   Consumer uploads report
         │
         ▼
 ┌─────────────────────┐
 │  Landing Page       │  ← hosted inside GHL funnel (HTML embed)
 │  (index.html)       │     OR pointed at your external host
 └─────────────────────┘
         │ POST /api/scan
         ▼
 ┌─────────────────────┐
 │  FastAPI Backend    │  ← hosted on Railway / Render
 │  (external)         │     Reads PDFs + images with Claude vision
 └─────────────────────┘
         │
         ├──▶ Generates .docx
         ├──▶ Pushes to GHL API (contact + 20 custom fields)
         └──▶ Returns results to landing page
         │
         ▼
 ┌─────────────────────┐
 │  GHL Workflow       │  ← your SMS sequence fires
 │  (tag: bureau-scan) │     Bully AI handles replies via /api/chat
 └─────────────────────┘
```

Since the current `bullyaiagent.com` is already running inside GHL as a funnel page, the cleanest path is:
1. Host the FastAPI backend externally (Railway)
2. Replace the HTML on the existing GHL funnel page with our new `index.html`
3. Update the fetch URL in the landing page to point at your backend
4. Rebuild the GHL workflows using the spec in `docs/GHL_WORKFLOW.md`

---

## Step 1 — Get your API keys

You'll need three:

### Anthropic (powers Bully AI)
1. Sign in at https://console.anthropic.com
2. Settings → API Keys → "Create Key"
3. Give it a name like "Bureau Bullies Production"
4. Copy the `sk-ant-...` key

### GoHighLevel (Private Integration token)
1. Log in to your GHL sub-account (the one running `bullyaiagent.com`)
2. Settings → **API → Private Integrations**
3. Click **Create**
4. Name: "Bureau Bullies Backend"
5. Scopes needed (minimum):
   - `contacts.readonly`
   - `contacts.write`
   - `locations/customFields.readonly`
   - `locations/customFields.write`
   - `workflows.readonly`
6. Save → copy the token (starts with `pit-...`)

### GHL Location ID
Settings → **Company → Business Profile** → you'll see the Location ID near the top. Copy it.

---

## Step 2 — Create the 20 custom fields in GHL

Settings → **Custom Fields** → Create each one from the table in `docs/GHL_WORKFLOW.md` section 1. Use the exact field key (e.g., `cr_top_collection_name`). This has to match or the API push will drop the field.

---

## Step 3 — Deploy the backend to Railway

```bash
cd bureau-bullies/backend

# Railway CLI, if you don't have it:
npm i -g @railway/cli
railway login

# From the backend/ directory:
railway init
railway up
```

Then set the environment variables in Railway's web UI (**Variables** tab):

| Key | Value |
|---|---|
| `ANTHROPIC_API_KEY` | `sk-ant-...` |
| `CLAUDE_MODEL` | `claude-sonnet-4-6` |
| `GHL_API_KEY` | `pit-...` |
| `GHL_LOCATION_ID` | your location id |
| `GHL_WORKFLOW_TOOLKIT` | (fill in after Step 5) |
| `GHL_WORKFLOW_ACCELERATOR` | (fill in after Step 5) |
| `GHL_WORKFLOW_DFY` | (fill in after Step 5) |
| `MAX_FILE_MB` | `25` |

Railway will give you a URL like `bureau-bullies-production.up.railway.app`. Test it:

```bash
curl https://your-railway-url/healthz
# → {"ok": true}
```

Optional: add a custom domain like `api.bureaubullies.com` in Railway settings.

---

## Step 4 — Replace the existing GHL funnel page

1. In GHL, go to **Sites → Funnels** (or **Websites**, depending on your setup)
2. Find the `bullyaiagent.com` funnel
3. Edit the landing page
4. Delete the current content
5. Add a **Custom HTML/Code** block set to full-width, full-page
6. Open `frontend/index.html` from this project
7. **Before pasting**: find-and-replace these two lines:
   - Find: `fetch('/api/scan'`
     Replace with: `fetch('https://YOUR-RAILWAY-URL/api/scan'`
   - Find: `'results.html?demo=1'`
     Replace with: `'https://YOUR-RAILWAY-URL/results?demo=1'` (optional — only if you keep the demo fallback)
8. Copy the entire edited HTML and paste it into the GHL Custom HTML block
9. Save the funnel page

Do the same for `results.html` → save as a separate funnel page (e.g., `/scan-results`). Same find-and-replace adjustments if any.

Do the same for `terms.html` and `privacy.html` — these can be regular GHL pages at `/terms` and `/privacy`.

---

## Step 5 — Build the three GHL workflows

Open `docs/GHL_WORKFLOW.md`. Build one workflow per tier:

1. **Toolkit ($17) Funnel** — triggers on `tag-added: tier-toolkit`
2. **Accelerator ($66) Funnel** — triggers on `tag-added: tier-accelerator`
3. **DFY Funnel** — triggers on `tag-added: tier-dfy`

Copy the SMS copy from the workflow doc verbatim. Use GHL's merge tag picker to insert `{{custom_values.cr_top_collection_name}}` etc.

After each workflow is built, grab its ID from the URL (`workflows/edit/XXXXX`) and paste back into Railway env vars:
- `GHL_WORKFLOW_TOOLKIT`
- `GHL_WORKFLOW_ACCELERATOR`
- `GHL_WORKFLOW_DFY`

---

## Step 6 — Wire up Bully AI SMS replies (the chat bot)

In GHL: **Settings → Automations → Triggers**

1. Create a trigger: **"Customer Replied via SMS"**
2. Filter: tag contains `bureau-scan`
3. Action: **Webhook** →
   - URL: `https://YOUR-RAILWAY-URL/api/chat`
   - Method: POST
   - Body (JSON):
     ```json
     {
       "message": "{{sms.body}}",
       "contact": {
         "cr_top_collection_name": "{{contact.cr_top_collection_name}}",
         "cr_total_leverage": "{{contact.cr_total_leverage}}",
         "cr_violations_count": "{{contact.cr_violations_count}}",
         "cr_recommended_tier": "{{contact.cr_recommended_tier}}",
         "cr_fear_hook": "{{contact.cr_fear_hook}}",
         "cr_urgency_hook": "{{contact.cr_urgency_hook}}",
         "first_name": "{{contact.first_name}}"
       },
       "history": []
     }
     ```
4. Next step: **Send SMS** using the webhook response as the message body.

Now any reply that isn't a keyword (YES/VAULT/DFY/STOP) gets routed through Bully AI, who replies in character using the ebook knowledge base.

---

## Step 7 — Test end-to-end

1. Open the new funnel page in incognito
2. Upload a sample credit report (or any PDF to test)
3. You should be redirected to `/results` showing leverage, violations, case law
4. You should receive SMS #1 within 60 seconds
5. Reply "YES" — should get the $17 checkout link
6. Check GHL contact record — all 20 custom fields should be populated

---

## Step 8 — DNS swap (optional, when ready to go live)

If `bullyaiagent.com` is currently pointed at GHL, you've already swapped — just switch the funnel page. If you want a fresh domain (`thebureaubullies.com`):

1. GHL → **Domains** → add new domain
2. Update your DNS A/CNAME records to GHL's provided values
3. Map the new domain to your funnel
4. Keep `bullyaiagent.com` as a 301 redirect to the new domain (GHL can do this in the same Domains panel)

---

## Troubleshooting

**"Scan button doesn't do anything"**
- Backend isn't reachable. Check Railway is running (`curl https://YOUR-URL/healthz`).
- CORS: GHL's embedded HTML has a different origin. Add to `app.py`:
  ```python
  from fastapi.middleware.cors import CORSMiddleware
  app.add_middleware(CORSMiddleware, allow_origins=["https://bullyaiagent.com", "https://thebureaubullies.com"], allow_methods=["*"], allow_headers=["*"])
  ```

**"GHL push failed — customField not found"**
- A `cr_*` field is missing in GHL. Go to Settings → Custom Fields and create it.

**"Bully AI replies in SMS are generic / don't reference their data"**
- The contact custom fields aren't being passed in the webhook body. Check the trigger action's JSON payload.

**"PDF upload fails for large files"**
- Raise `MAX_FILE_MB` in Railway env vars.
- Check Railway's HTTP body size limits (Pro plan = unlimited).

---

## Costs (monthly estimate)

- Railway: $5–$20/mo depending on traffic
- Anthropic API: ~$0.03–$0.15 per scan (Sonnet). Budget $50–$300/mo depending on volume.
- GHL: your existing subscription
- SMS (GHL): your existing 10DLC A2P plan

Budget ~$100/mo at 500 scans. Scales linearly.
