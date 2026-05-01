# Bureau Bullies — Audit Handoff

Snapshot of the system as of May 1, 2026. Written for an outside engineer or auditor inheriting the codebase.

## What this app is supposed to do

Bureau Bullies LLC is a credit-repair business. The app is the lead-to-revenue automation layer behind it.

End-to-end flow:
1. A potential customer sees a Meta IG/FB ad (or organic IG content), clicks through to a lead form, or comments/DMs the IG account.
2. Their contact lands in GoHighLevel (GHL). A webhook hits this backend within seconds.
3. The backend fires a "qualifier" SMS and Email (real-time touch), tags the contact, and queues a multi-day follow-up sequence.
4. If they reply or DM with intent ("Equifax", "send link", etc.) a deterministic keyword shortcut fires the right product link instantly. Otherwise their inbound goes through Bully AI (Anthropic Claude) which holds a conversation tuned to convert.
5. Bully AI pushes the lead to upload their 3-bureau credit report at `bullyaiagent.com`. The backend analyzes it, writes the violations + scan summary into GHL custom fields, then routes the contact into one of three tiered offers — Toolkit ($17), Vault ($66), or DFY ($2,500 one-time / $229/mo). A separate Equifax Exposed product ($27, normally $666) is sold via its own keyword shortcut.
6. On purchase, GHL fires a conversion webhook back; the backend stops drip messages and notifies Umar via SMS.

## The AI agent's intended workflow

For every inbound message (IG DM, IG comment, GHL SMS reply, eventually Meta-direct):

1. **Dedupe** — drop the message if the same `(contact_id, message_hash)` was processed in the last 30 seconds (GHL fires duplicate webhooks).
2. **Hard-pause check** — if the customer's message contains refund/cancel/legal/accusation language, tag `pause-ai`, alert Umar, do not reply.
3. **Keyword shortcut** — scan the entire payload (not just `message` — also `story.body`, `reply_to.text`, etc.) for trigger keywords. If matched, send a deterministic templated reply (e.g. "equifax" → equifaxexposed.com $27 pitch), tag for attribution, return immediately.
4. **Human-active check** — if Umar has manually replied to this contact in the last 24h, AI stays silent.
5. **Already-scanned reconciliation** — if the customer claims to have uploaded already, look up by email or contact ID and pull their scan data into the conversation context so Bully AI doesn't re-pitch the upload.
6. **Bully AI call** — load conversation history (from GHL + persistent local memory at `/var/data/memory/`), call Anthropic Claude with a long system prompt that covers the playbook, FCRA voice, settlement frameworks (50/50 split rule), upsell ladder. Send the reply via GHL Conversations API.
7. **Persist** — append the turn to `/var/data/memory/{contact_id}.json` so memory survives Render redeploys.

## User roles

- **Customer (lead)** — receives DMs, SMS, emails. Uploads credit report. Pays for products.
- **Umar (admin)** — business owner. Can manually reply at any time, which auto-pauses AI on that thread. Receives SMS notifications on purchases. Owns all admin endpoints.
- **Bully AI** — Anthropic Claude wrapped in a domain-specific system prompt. Identity is "Bully AI", not Claude.

## Frontend pages

All in `frontend/`:
- `index.html` — main landing page with the credit-report upload form (glassmorphism design, IG handle field, goal dropdown). Submits to `/api/scan`.
- `results.html` — instant-conversion results screen shown after a scan. Renders violations, accounts, and pricing.
- `terms.html`, `privacy.html` — legal pages.

Hosted at `bullyaiagent.com` (primary domain on Render), and reused at `bureau-bullies.onrender.com`.

Adjacent properties (separate hosting, NOT in this codebase):
- `equifaxexposed.com` — $27 Equifax product landing page.
- `thebureaubullies.com` — main brand site (BGP2025 product).

## Backend routes

All in `backend/app.py`. FastAPI.

Public:
- `GET /` — serves `frontend/index.html`.
- `GET /results`, `/terms`, `/privacy` — corresponding HTML pages.
- `POST /api/scan` — upload credit report, run Bully AI analysis, write scan data to GHL, kick off Toolkit drip sequence.
- `POST /api/chat` — Bully AI chat endpoint (used by the in-page chat widget).
- `GET /healthz` — health check.

Inbound webhooks:
- `POST /webhooks/ig/dm` — inbound IG direct message (via GHL).
- `POST /webhooks/ig/comment` — inbound IG comment (via GHL).
- `POST /webhooks/ghl/sms-reply` — inbound SMS reply.
- `POST /webhooks/ghl/contact-created` — new contact landed in GHL. Routes by `campaign` field (e.g. `equifax-dispute-letter` → Equifax Day 0 sequence; otherwise generic qualifier).
- `GET/POST /webhooks/meta/ig` — Meta Graph API direct webhook (deployed but waiting on `META_IG_PAGE_TOKEN`).

Admin (token-gated via `_check_admin`):
- `POST /admin/sync-fb-leads` — pull all FB/IG-source contacts from GHL, fire qualifier on the untouched ones.
- `POST /admin/dispatch-next` — advance every bureau-scan contact by one email in their drip.
- `POST /admin/check-conversions` — scan GHL for new purchases, notify Umar, stop drips.
- `POST /admin/backfill-leads` — manual single-shot qualifier fire on a specific contact list.
- `POST /admin/backfill-email-drip` — rebuild the persistent email queue from scratch.
- `POST /admin/setup-custom-fields` — idempotently create the 20 GHL custom fields the backend writes to.
- `POST /admin/cancel-emails` — kill all pending email rows.

## APIs used

- **Anthropic Claude** (`claude-sonnet-4-6`) — Bully AI conversational replies and credit-report analysis.
- **GoHighLevel v2** — Contacts, Conversations (SMS + IG DM send), Tags, Custom Fields, Workflows. Auth via Personal Integration Token (PIT).
- **Meta Graph API** — Send public IG comment replies ("Sent. Check your DMs 💌"). Will also handle direct IG DMs once `META_IG_PAGE_TOKEN` is configured.
- **Render persistent disk** at `/var/data` — scheduler queue + per-contact memory, both must survive redeploys.

## Current bugs

Open or partially mitigated:
1. **External Render cron `bb-cron-sync-fb-leads` returns `{"ok":false,"error":"unauthorized"}`** despite sending the bootstrap admin token. The internal scheduler tick (calling the same endpoint via `127.0.0.1` every 60s) authorizes correctly with the same token, so the cron is redundant — but the curl-vs-FastAPI-Form parsing quirk is unresolved.
2. **`META_IG_PAGE_TOKEN` not configured.** The new `/webhooks/meta/ig` endpoint exists but won't receive events until the Meta app is published, the IG account is added as a Tester, and an access token is generated. Until then, IG DMs from people with no GHL contact (e.g. "Individuals Pursuing evolution") never reach this backend.
3. **WhatsApp PII leak** — bot historically sent Umar's personal WhatsApp number (`+1 267 234 8189`) to a customer. A code-level phone-number block was added (#119) but full code-path audit is incomplete.
4. **GHL multi-fire** — GHL fires the same inbound webhook 4–5 times for one DM, causing 4 stacked Bully AI replies. Mitigated by `_RECENT_INBOUND_HASHES` (30-second dedupe window). Root cause in GHL workflow config not fully traced.
5. **Email drip retry loop** — when GHL returns "user has unsubscribed", scheduler keeps retrying up to 5 times before giving up. Should fail-closed on unsub, not retry.
6. **Meta webhook signature validation NOT implemented** — `/webhooks/meta/ig` does not yet verify `X-Hub-Signature-256`. A `META_APP_SECRET` env var should be added and verified per Meta's spec.
7. **No rate limit on GHL API calls** — large lead syncs could hit 429.

Recently fixed (verified live):
- Story-reply keyword shortcut — was failing because GHL ships story-reply text in `story.body` not `message.body`. Fix at commit `ad3c4a3` introduces `_build_keyword_search_corpus` that scans every probable text field.
- Empty-message early-return — webhook used to short-circuit when `message` was empty; now uses the corpus to override.

## Current deployment setup

- **Render web service:** `bureau-bullies` (`srv-d7kifrfavr4c73bkbodg`), Python 3, auto-deploys from `bullyassistance/bureau-bullies` `main` branch on every push.
- **Render cron jobs:**
  - `bb-cron-sync-fb-leads` — every 5 minutes (currently auth-broken, see bug #1).
  - `bb-cron-check-conversions` — every 15 minutes.
- **Persistent disk** mounted at `/var/data` (1 GB) — holds `scheduler.json` and `memory/{contact_id}.json`.
- **Custom domains:** `bullyaiagent.com` (primary), `bureau-bullies.onrender.com` (fallback). `equifaxexposed.com` and `thebureaubullies.com` are separate properties.
- **Required env vars** (set in Render dashboard):
  - `ANTHROPIC_API_KEY`
  - `CLAUDE_MODEL` (defaults to `claude-sonnet-4-6`)
  - `GHL_API_KEY` (PIT token)
  - `GHL_LOCATION_ID`
  - `GHL_WORKFLOW_TOOLKIT`, `GHL_WORKFLOW_ACCELERATOR`, `GHL_WORKFLOW_DFY` (workflow IDs)
  - `MAX_FILE_MB` (defaults to 25)
  - `MAX_REPORT_CHARS` (truncation cap for credit reports)
  - `UMAR_NOTIFY_PHONE` (purchase notifications)
  - `ADMIN_TOKEN` (admin endpoint auth — added today)
  - `META_IG_PAGE_TOKEN` (NOT YET SET)
  - `META_WEBHOOK_VERIFY_TOKEN` (defaults to `bb_meta_verify_2026` if unset)
  - `META_APP_SECRET` (NOT YET SET — required for signature validation)

## Files that control the AI agent

- `backend/bully_ai.py` — Anthropic client wrapper. Holds the system prompt that defines Bully AI's voice, framework, and refusal rules. The system prompt is the single biggest lever on conversion quality.
- `backend/email_generator.py` — generates per-contact tailored email bodies (PAS framework + transformation copy) for the drip sequence.
- `backend/contact_memory.py` — persistent per-contact conversation history. Loads/saves JSON files at `/var/data/memory/`.
- `backend/app.py` — keyword shortcut config (`_KEYWORD_SHORTCUTS`), keyword matching (`_match_keyword_shortcut`, `_build_keyword_search_corpus`), human-takeover detection (`_ig_human_active`), inbound dedupe (`_is_duplicate_inbound`), the Bully AI dispatch logic in each `/webhooks/...` handler.

## Files that control the website UI

- `frontend/index.html` — landing page, upload form, embedded chat widget.
- `frontend/results.html` — post-scan results screen.
- `frontend/terms.html`, `frontend/privacy.html` — legal pages.

The frontend is plain HTML/CSS/JS; no build step. Files are served by FastAPI as static assets.

## Files that control database operations

There is no traditional database — the system uses GHL contact custom fields as the system of record, plus a persistent JSON queue.

- `backend/scheduler.py` — owns `/var/data/scheduler.json`, the queue of pending email/SMS sends with retry counts, scheduled times, and idempotency keys. Also runs the in-process poll loop (`_poll_loop`) that fires due rows every 60 seconds and the in-process Meta lead sweep (`_meta_lead_sweep_tick`).
- `backend/contact_memory.py` — owns `/var/data/memory/{contact_id}.json`, the per-contact rolling conversation history (last 30 turns).
- `backend/ghl.py` — the only source-of-truth client for the actual customer data. Reads/writes contact fields, tags, conversations.
- `backend/app.py` — `_RECENT_INBOUND_HASHES` is an in-memory dict for inbound dedupe (NOT persisted; resets on redeploy, which is fine because the 30-second window is much shorter than deploy time).

## Files that control authentication

- `backend/app.py`:
  - `_check_admin(token)` — checks `ADMIN_TOKEN` env var first, then a hardcoded `_BOOTSTRAP_ADMIN_TOKEN` fallback. All `/admin/*` routes call this.
  - `_BOOTSTRAP_ADMIN_TOKEN = "<REDACTED-bootstrap-token>"` — hardcoded in source. Risky (see "unclear/risky" below).
- `backend/ghl.py` — handles GHL PIT token authentication (`Authorization: Bearer <token>` for v2 API) and falls back to v1 JWT for legacy endpoints.
- Meta webhook auth (when implemented): `META_APP_SECRET` for HMAC-SHA256 signature validation on `/webhooks/meta/ig` POST requests, `META_WEBHOOK_VERIFY_TOKEN` for the GET handshake.
- Render env vars are the only secret store. There is no `.env` file in version control.

## Unclear or risky parts of the code

1. **Hardcoded bootstrap admin token in source.** `_BOOTSTRAP_ADMIN_TOKEN` is in `app.py`. Anyone with read access to the repo (including any future engineer or anyone who clones a leaked copy) can hit every admin endpoint. Should be removed once `ADMIN_TOKEN` env is reliably set everywhere.

2. **Meta webhook has no signature validation.** `/webhooks/meta/ig` will accept any POST without verifying it's from Meta. This means a bad actor who guesses the URL could spoof inbound events and trigger keyword shortcut sends. Add `X-Hub-Signature-256` HMAC validation against `META_APP_SECRET` before going live.

3. **`_RECENT_INBOUND_HASHES` is process-local.** If Render scales to >1 web instance, dedupe will fail across processes. Currently fine because the service runs single-instance, but worth noting if scaling.

4. **No locking on `/var/data/memory/{contact_id}.json` writes.** Two concurrent webhook handlers for the same contact could corrupt the file. Race window is small but exists.

5. **GHL webhook fan-out is opaque.** GHL sometimes fires the same trigger 4–5 times for one inbound — root cause unknown, mitigated by dedupe. If GHL changes timing or shape, the dedupe window may need tuning.

6. **The keyword fuzzy matcher uses `difflib.SequenceMatcher` with cutoff 0.75.** Catches "Exquifax" / "Equafax" / "Equifx" but could over-trigger on rare 4-char tokens. No false-positive monitoring.

7. **Email drip retry behavior on unsubscribed users.** When GHL returns 400 "user has unsubscribed", the scheduler retries up to 5 times. Should fail-closed on first unsub error.

8. **No circuit breaker on Anthropic API failures.** If Anthropic returns 5xx for a stretch, every inbound message will hit the API and burn tokens with no backoff.

9. **`/admin/sync-fb-leads` response timing varies wildly.** Empty result = 4 ms (auth fail or all-already-tagged). Full processing with GHL searches = 4–30 s. The Render cron has a 90 s timeout which is probably enough, but a slow GHL day could blow it.

10. **GHL Conversations API rate limits are not respected.** No `Retry-After` handling, no token-bucket throttling. A spike of 100 inbound messages would hit GHL's rate limit and start dropping sends silently.

11. **The `/api/scan` endpoint accepts up to `MAX_FILE_MB=25` of arbitrary file input** (PDF, image, text). The OCR + Anthropic call chain on a hostile file could time out. There's no scanning for malicious PDFs.

12. **Meta lead-form trigger filter is brittle.** The Equifax-Ad workflow in GHL is filtered by exact form name "EQUIFAX EXPOSE…". If the form name changes by even one character, leads stop enrolling and the bug is silent (zero enrollments looks identical to "no submissions yet").

13. **`_meta_lead_sweep_tick` calls `/admin/sync-fb-leads` via `127.0.0.1`** every 60 s. If the FastAPI server is busy, this blocks the scheduler thread for up to 45 s. Should be moved to a background task.

14. **In-memory `_RECENT_INBOUND_HASHES` is GC'd at 2000 entries** with a coarse cutoff. On a big spike day this could let through duplicates if GC runs mid-burst.

15. **Three different commit-via-Chrome pathways are used to deploy** (Chrome MCP file_upload, GitHub upload UI, CodeMirror dispatch). The CodeMirror-dispatch path requires `userEvent: 'input.paste'` annotation to register as user input — not obvious, and undocumented in the repo. New engineers will struggle to deploy.

16. **The Equifax keyword shortcut and the bullyaiagent.com Bully AI scan pitch are mutually exclusive** by intent, but the routing depends on whether the keyword corpus matches. If the corpus builder ever fails (returns empty for a story-reply), Bully AI fires the wrong message — exactly the bug fixed today.
