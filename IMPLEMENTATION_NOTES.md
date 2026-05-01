# Bureau Bullies Reliability + Conversion Patch

## What this patch adds

1. **Dedicated scheduler worker / cron support**
   - New file: `tools/run_scheduler_worker.py`
   - New admin route: `POST /admin/dispatch-due`
   - This dispatches due emails once and exits, so Render Cron can run it every 1-5 minutes.

2. **Optional Postgres/Supabase scheduler storage**
   - New file: `backend/scheduler_store.py`
   - Default remains JSON.
   - To move scheduler rows to Postgres/Supabase, set:
     - `BB_SCHEDULER_BACKEND=postgres`
     - `DATABASE_URL=<your Supabase pooler or Postgres URL>`
   - Added `psycopg[binary]` to requirements.

3. **Event log**
   - New file: `backend/event_log.py`
   - Logs structured JSONL events to `/var/data/events.jsonl` by default.
   - New admin route: `GET /admin/events?token=...`

4. **Drip dashboard**
   - New admin route: `GET /admin/drip-dashboard?token=...`
   - Shows pending, failed, recently sent, cancelled, and storage backend.

5. **Conversion dashboard**
   - New admin route: `GET /admin/conversion-dashboard?token=...`
   - Tracks a 20% daily conversion KPI from event log signals.
   - This is a KPI and measurement system, not a guarantee.

6. **Meta direct webhook with signature validation**
   - New routes:
     - `GET /webhooks/meta/ig`
     - `POST /webhooks/meta/ig`
   - Requires:
     - `META_APP_SECRET`
     - `META_IG_PAGE_TOKEN`
     - `META_WEBHOOK_VERIFY_TOKEN`
   - Handles direct Meta messages that do not yet exist as GHL contacts.

7. **GHL rate-limit retries**
   - `GHLClient._request_with_rate_limit()` retries outbound sends on 429/5xx.
   - Honors `Retry-After` when GHL sends it.
   - Env: `GHL_MAX_RETRIES=3`

8. **Conversion optimizer utilities**
   - New file: `backend/conversion_optimizer.py`
   - Adds heat scoring, A/B variant assignment, and 20% KPI note.

## Recommended Render setup

Minimum:
- Keep web service.
- Add persistent disk `/var/data`.
- Add cron job every 5 minutes:

```bash
cd /app/backend && python ../tools/run_scheduler_worker.py
```

Best:
- Add Supabase/Postgres.
- Set:

```env
BB_SCHEDULER_BACKEND=postgres
DATABASE_URL=postgresql://...
```

## Reality check on 20% conversion

The code now tracks and optimizes toward a 20% daily conversion KPI. It cannot guarantee 20% because conversion depends on offer, lead quality, ad targeting, response speed, price, trust, testimonials, and sales follow-up. The patch gives the app the infrastructure to measure and react.
