# SHIP IT — 3 Deploy Paths (pick the easiest one)

You don't need to run anything on your Mac. Pick one of the paths below and you're live in 5 minutes.

Your API keys (paste into the host's env-vars panel when prompted):
```
ANTHROPIC_API_KEY   = sk-ant-api03-xDg0nLH2…   (already in your .env)
GHL_API_KEY         = eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9…
GHL_LOCATION_ID     = meX0Ery4aBWtjG0MG0Tu
CLAUDE_MODEL        = claude-sonnet-4-6
MAX_FILE_MB         = 25
```

---

## ⭐ Path 1 — Render.com (EASIEST, free tier available)

1. Go to [render.com](https://render.com) → sign up with GitHub or email
2. Click **New → Blueprint**
3. Either:
   - Upload the zip file I gave you, OR
   - Push the `bureau-bullies/` folder to a GitHub repo and point Render at it
4. Render sees `render.yaml`, auto-provisions everything
5. On the env vars page, paste your 3 keys (above)
6. Click **Apply**
7. Wait ~3 minutes for Docker build
8. Copy your Render URL (e.g. `bureau-bullies.onrender.com`)
9. **That's your backend.** Done.

---

## Path 2 — Railway (good dev UX, ~$5/mo)

1. Install Railway CLI once: `npm i -g @railway/cli`
2. `cd` into the `bureau-bullies/` folder
3. Run:
   ```bash
   railway login
   railway init
   railway up
   railway variables set \
     ANTHROPIC_API_KEY=sk-ant-… \
     GHL_API_KEY=eyJhbG… \
     GHL_LOCATION_ID=meX0Ery4aBWtjG0MG0Tu \
     CLAUDE_MODEL=claude-sonnet-4-6
   ```
4. Railway gives you a URL. Done.

---

## Path 3 — Fly.io (best performance, free tier)

```bash
# one-time: install flyctl
brew install flyctl
flyctl auth signup

# deploy
cd bureau-bullies
flyctl launch --copy-config --no-deploy
flyctl secrets set ANTHROPIC_API_KEY=sk-ant-… GHL_API_KEY=eyJhbG… GHL_LOCATION_ID=meX0Ery4aBWtjG0MG0Tu
flyctl deploy
```

Fly gives you `bureau-bullies.fly.dev`. Done.

---

## After Deploy — 3 Steps to Replace bullyaiagent.com on GHL

### Step A — Point landing page at your new backend

In your deployed `frontend/index.html`, find-and-replace:
- `/api/scan` → `https://YOUR-DEPLOY-URL.com/api/scan`

(Or just keep using the self-hosted URL as the landing page — that works too.)

### Step B — Swap the GHL funnel page

1. GHL → Sites → Funnels → bullyaiagent.com → Edit
2. Delete current landing page content
3. Add a **Redirect** or **Custom HTML** block
4. Either iframe-embed `https://YOUR-DEPLOY-URL.com` or paste the HTML and fix the fetch URL

### Step C — Build the 3 workflows in GHL

Open `docs/GHL_WORKFLOW.md` — copy every SMS word-for-word. Use the merge tags like `{{custom_values.cr_top_collection_name}}`. The 20 custom fields are already in your GHL sub-account (you saw 20 created earlier).

Grab each workflow's ID from the URL and paste into your host's env vars:
```
GHL_WORKFLOW_TOOLKIT=...
GHL_WORKFLOW_ACCELERATOR=...
GHL_WORKFLOW_DFY=...
```

Redeploy (or re-sync env vars).

### Step D — Wire the SMS reply webhook

GHL → Automations → Triggers → "Customer Replied via SMS" →
- Webhook URL: `https://YOUR-DEPLOY-URL.com/api/chat`
- Method: POST
- Body: (see docs/DEPLOY_TO_GHL.md section 6)

Now Bully AI replies to SMS in character, using the ebook knowledge base.

---

## Local Testing (ONLY if you want to preview before deploying)

If you want to see the site running on your Mac first:

```bash
cd bureau-bullies/backend
source .venv/bin/activate
uvicorn app:app --host 127.0.0.1 --port 8000
```

Then visit `http://127.0.0.1:8000` in **Chrome** (Safari can be weird with plain-http localhost).

If the page is blank in your browser but the server is running, it's almost always a macOS firewall or Safari issue. The deployed version has no such problems.

---

## Troubleshooting

- **"Port 8000 already in use"** → run `lsof -ti:8000 | xargs kill -9` then retry.
- **"Module not found: fastapi"** → you're in the wrong Python. Run `source .venv/bin/activate` first.
- **"Connection refused to Anthropic"** → double-check `ANTHROPIC_API_KEY` doesn't have extra spaces.
- **"GHL push failed: 401"** → your JWT is expired. Generate a new one.
- **Safari shows blank page on localhost** → use Chrome, or deploy and use a real URL.
