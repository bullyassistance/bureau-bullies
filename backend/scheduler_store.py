"""Optional scheduler storage adapter.

Default remains the JSON file because it is dependency-free. Set:

  BB_SCHEDULER_BACKEND=postgres
  DATABASE_URL=postgresql://...

or a Supabase pooler URL in DATABASE_URL, and scheduler rows move into Postgres.
The adapter keeps the existing list[dict] scheduler API so the production logic
can be upgraded without rewriting every scheduler call.
"""
from __future__ import annotations

import json
import os
from typing import List


def postgres_enabled() -> bool:
    return (os.getenv("BB_SCHEDULER_BACKEND", "json").lower() in {"postgres", "supabase"}
            and bool(os.getenv("DATABASE_URL")))


def _connect():
    try:
        import psycopg
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Install psycopg[binary] or unset BB_SCHEDULER_BACKEND=postgres") from e
    return psycopg.connect(os.getenv("DATABASE_URL"), autocommit=True)


def ensure_schema() -> None:
    with _connect() as conn:
        conn.execute(
            """
            create table if not exists bb_scheduler_rows (
              id text primary key,
              row_json jsonb not null,
              status text generated always as (row_json->>'status') stored,
              contact_id text generated always as (row_json->>'contact_id') stored,
              send_at timestamptz generated always as ((row_json->>'send_at')::timestamptz) stored,
              updated_at timestamptz not null default now()
            );
            create index if not exists idx_bb_scheduler_status_send_at
              on bb_scheduler_rows (status, send_at);
            create index if not exists idx_bb_scheduler_contact
              on bb_scheduler_rows (contact_id);
            """
        )


def load_rows() -> List[dict]:
    ensure_schema()
    with _connect() as conn:
        rows = conn.execute("select row_json from bb_scheduler_rows order by send_at nulls last, id").fetchall()
    return [dict(r[0]) for r in rows]


def save_rows(rows: List[dict]) -> None:
    ensure_schema()
    with _connect() as conn:
        with conn.transaction():
            conn.execute("delete from bb_scheduler_rows")
            for row in rows:
                rid = str(row.get("id") or "")
                if not rid:
                    continue
                conn.execute(
                    "insert into bb_scheduler_rows (id, row_json, updated_at) values (%s, %s::jsonb, now()) "
                    "on conflict (id) do update set row_json=excluded.row_json, updated_at=now()",
                    (rid, json.dumps(row)),
                )
