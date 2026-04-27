"""
Bureau Bullies — GoHighLevel (GHL) API integration
---------------------------------------------------
Supports BOTH auth methods:
  - v1 API  — JWT token (eyJ...) → base: https://rest.gohighlevel.com/v1
  - v2 API  — Private Integration token (pit-...) → base: https://services.leadconnectorhq.com

Auto-detects based on token prefix. Exposes a unified interface:
  - upsert_contact
  - list_custom_fields
  - create_custom_field
  - add_to_workflow
  - add_tags

Docs:
  v1: https://highlevel.stoplight.io/docs/integrations/YXBpOjE3Mjg3MjU3MzgtYXBpLXYx-ghl-rest-api-v1
  v2: https://highlevel.stoplight.io/docs/integrations/
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import requests

logger = logging.getLogger("bureau-bullies.ghl")

V1_BASE = "https://rest.gohighlevel.com/v1"
V2_BASE = "https://services.leadconnectorhq.com"
V2_API_VERSION = "2021-07-28"


class GHLError(Exception):
    pass


def _detect_version(token: str) -> str:
    """JWT tokens (eyJ...) = v1. pit-... tokens = v2."""
    if token.startswith("pit-"):
        return "v2"
    if token.startswith("eyJ"):
        return "v1"
    # default — try v2 since it's the future
    return "v2"


class GHLClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        location_id: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("GHL_API_KEY")
        self.location_id = location_id or os.getenv("GHL_LOCATION_ID")
        if not self.api_key or not self.location_id:
            raise GHLError("GHL_API_KEY and GHL_LOCATION_ID must be set")
        self.version = _detect_version(self.api_key)
        logger.info("GHL client initialized (API %s)", self.version)

    # ---- Headers ---------------------------------------------------------
    @property
    def _headers(self) -> dict:
        h = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.version == "v2":
            h["Version"] = V2_API_VERSION
        return h

    @property
    def base(self) -> str:
        return V2_BASE if self.version == "v2" else V1_BASE

    # ---- Ping / sanity ---------------------------------------------------
    def ping(self) -> dict:
        """Quick sanity check — list custom fields (smallest endpoint)."""
        return self.list_custom_fields()

    # ---- Custom fields ---------------------------------------------------
    _field_cache: dict = {}

    def list_custom_fields(self) -> list:
        if self.version == "v1":
            url = f"{V1_BASE}/custom-fields/"
            r = requests.get(url, headers=self._headers, timeout=20)
        else:
            url = f"{V2_BASE}/locations/{self.location_id}/customFields"
            r = requests.get(url, headers=self._headers, timeout=20)
        if not r.ok:
            raise GHLError(f"list_custom_fields: {r.status_code} {r.text[:300]}")
        data = r.json()
        fields = data.get("customFields", []) or data.get("custom_fields", [])
        return fields

    def create_custom_field(
        self,
        name: str,
        field_key: str,
        data_type: str = "TEXT",
    ) -> dict:
        """
        data_type options (GHL):
          v1:  TEXT, LARGE_TEXT, NUMERICAL, PHONE, MONETORY, CHECKBOX, ...
          v2:  TEXT, LARGE_TEXT, NUMERICAL, MONETORY, CHECKBOX, ...
        """
        if self.version == "v1":
            url = f"{V1_BASE}/custom-fields/"
            payload = {
                "name": name,
                "fieldKey": field_key,
                "dataType": data_type,
            }
        else:
            url = f"{V2_BASE}/locations/{self.location_id}/customFields"
            payload = {
                "name": name,
                "dataType": data_type,
                "fieldKey": field_key,
                "model": "contact",
            }
        r = requests.post(url, headers=self._headers, json=payload, timeout=20)
        if not r.ok:
            raise GHLError(f"create_custom_field({name}): {r.status_code} {r.text[:300]}")
        return r.json()

    # ---- Contact upsert --------------------------------------------------
    def ensure_field(self, key: str, display_name: str = None, data_type: str = None) -> str:
        """
        Make sure a custom field exists in GHL. Returns its ID. Creates it if missing.
        Also updates the in-memory cache.
        """
        if not self._field_cache:
            # Force a cache refresh if it's empty
            for f in self.list_custom_fields():
                k = (f.get("fieldKey", "") or f.get("name", "")).replace("contact.", "")
                if k:
                    self._field_cache[k] = f.get("id") or f.get("_id")

        if key in self._field_cache:
            return self._field_cache[key]

        # Not found — try to find a matching REQUIRED_FIELDS entry
        name = display_name
        dt = data_type or "TEXT"
        try:
            for k, nm, t in REQUIRED_FIELDS:
                if k == key:
                    name = name or nm
                    dt = t
                    break
        except NameError:
            pass

        name = name or key.replace("_", " ").title()
        logger.info("Creating missing custom field: %s (%s, %s)", key, name, dt)
        try:
            created = self.create_custom_field(name=name, field_key=key, data_type=dt)
            fid = (created.get("customField") or created).get("id") or (created.get("customField") or created).get("_id")
            if fid:
                self._field_cache[key] = fid
                return fid
        except Exception as e:
            logger.warning("Could not auto-create field %s: %s", key, e)
        return ""

    def upsert_contact(
        self,
        first_name: str,
        last_name: str,
        email: str,
        phone: str,
        custom_fields: dict,
        tags: list,
    ) -> dict:
        # Resolve custom field name -> id
        field_map = self._field_cache
        if not field_map:
            for f in self.list_custom_fields():
                # Both APIs use fieldKey OR name
                key = f.get("fieldKey", "") or f.get("name", "")
                key = key.replace("contact.", "")
                if key:
                    field_map[key] = f.get("id") or f.get("_id")
            self._field_cache = field_map
            logger.info("Loaded %d existing GHL custom fields", len(field_map))

        # Auto-create any missing fields that are being pushed this request
        for k in list(custom_fields.keys()):
            if k not in field_map:
                self.ensure_field(k)
                field_map = self._field_cache  # refresh after create

        if self.version == "v1":
            # v1 uses a different payload shape
            gh_customs = {}
            for key, value in custom_fields.items():
                fid = field_map.get(key)
                if not fid:
                    logger.warning("GHL v1: custom field '%s' not found — skipping", key)
                    continue
                gh_customs[fid] = value
            payload = {
                "firstName": first_name,
                "lastName":  last_name,
                "name":      f"{first_name} {last_name}".strip(),
                "email":     email,
                "phone":     phone,
                "tags":      tags,
                "source":    "bureaubullies.com scan",
                "customField": gh_customs,
            }
            url = f"{V1_BASE}/contacts/"
            r = requests.post(url, headers=self._headers, json=payload, timeout=25)
        else:
            # v2 — array of {id, field_value}
            gh_customs = []
            for key, value in custom_fields.items():
                fid = field_map.get(key)
                if not fid:
                    logger.warning("GHL v2: custom field '%s' not found — skipping", key)
                    continue
                gh_customs.append({"id": fid, "field_value": value})
            payload = {
                "firstName":    first_name,
                "lastName":     last_name,
                "name":         f"{first_name} {last_name}".strip(),
                "email":        email,
                "phone":        phone,
                "locationId":   self.location_id,
                "customFields": gh_customs,
                "tags":         tags,
                "source":       "bureaubullies.com scan",
            }
            url = f"{V2_BASE}/contacts/upsert"
            r = requests.post(url, headers=self._headers, json=payload, timeout=25)

        if not r.ok:
            raise GHLError(f"upsert_contact: {r.status_code} {r.text[:500]}")
        resp = r.json()
        cid = (resp.get("contact") or resp).get("id") or (resp.get("contact") or resp).get("_id")
        logger.info("GHL upsert OK — contactId=%s", cid)
        return resp

    # ---- Workflow add ----------------------------------------------------
    def add_to_workflow(self, contact_id: str, workflow_id: str) -> None:
        if self.version == "v1":
            url = f"{V1_BASE}/contacts/{contact_id}/workflow/{workflow_id}"
        else:
            url = f"{V2_BASE}/contacts/{contact_id}/workflow/{workflow_id}"
        r = requests.post(url, headers=self._headers, json={}, timeout=20)
        if not r.ok:
            raise GHLError(f"add_to_workflow: {r.status_code} {r.text[:300]}")

    # ---- Email send (direct via GHL Conversations API) -------------------
    def send_email(self, contact_id: str, subject: str, html: str, plain: str = "") -> bool:
        """
        Send an email to a contact via GHL's Conversations/Messages API.
        Returns True on success, False on any non-2xx response (logged, not raised).

        Used by scheduler.py so scheduled emails dispatch without a GHL workflow.
        """
        if not contact_id or not subject:
            logger.warning("send_email: missing contact_id or subject")
            return False

        if self.version == "v1":
            # v1 has /v1/conversations/messages for outbound
            url = f"{V1_BASE}/conversations/messages"
            payload = {
                "type": "Email",
                "contactId": contact_id,
                "subject": subject,
                "html": html or plain,
                "message": plain or "",
            }
        else:
            # v2 Conversations API
            url = f"{V2_BASE}/conversations/messages"
            payload = {
                "type": "Email",
                "contactId": contact_id,
                "subject": subject,
                "html": html or plain,
                "message": plain or "",
            }

        try:
            r = requests.post(url, headers=self._headers, json=payload, timeout=25)
        except Exception as e:
            logger.warning("send_email network error: %s", e)
            return False

        if r.ok:
            logger.info("send_email OK → contact %s: %s", contact_id, subject[:60])
            return True

        # Fallback — some GHL accounts use /v1/contacts/{id}/emails
        alt_url = (
            f"{V1_BASE}/contacts/{contact_id}/emails"
            if self.version == "v1"
            else f"{V2_BASE}/contacts/{contact_id}/emails"
        )
        try:
            r2 = requests.post(alt_url, headers=self._headers, json=payload, timeout=25)
            if r2.ok:
                logger.info("send_email OK via /contacts/emails → %s", contact_id)
                return True
            logger.warning(
                "send_email failed: primary %s %s | alt %s %s",
                r.status_code, r.text[:200],
                r2.status_code, r2.text[:200],
            )
        except Exception as e:
            logger.warning("send_email alt path error: %s", e)
        return False

    # ---- SMS send (direct via GHL Conversations API) --------------------
    def send_sms(self, phone: str, message: str) -> bool:
        """Send a one-off SMS to a phone number via GHL. Used for handoff alerts to Umar.
        Returns True on success, False on failure (logged, not raised)."""
        if not phone or not message:
            return False
        if self.version == "v1":
            url = f"{V1_BASE}/conversations/messages"
        else:
            url = f"{V2_BASE}/conversations/messages"
        payload = {
            "type": "SMS",
            "phone": phone,
            "message": message[:1500],
            "locationId": self.location_id,
        }
        try:
            r = requests.post(url, headers=self._headers, json=payload, timeout=15)
        except Exception as e:
            logger.warning("send_sms network error: %s", e)
            return False
        if r.ok:
            logger.info("send_sms OK -> %s: %s", phone[-4:], message[:60])
            return True
        logger.warning("send_sms failed (%s): %s", r.status_code, r.text[:200])
        return False

    # ---- Instagram DM send (direct via GHL Conversations API) -----------
    def send_ig_dm(
        self,
        contact_id: str,
        message: str,
        *,
        comment_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
    ) -> bool:
        """
        Send an Instagram DM to a contact via GHL's Conversations/Messages API.
        Returns True on success, False on any non-2xx response (logged, not raised).

        Used by the IG webhooks so GHL workflows can stop relying on the
        fire-and-forget "Webhook" action (which doesn't capture the response
        body, leaving {{webhook.response.reply}} empty).

        Args:
          contact_id:      GHL contact id (must have IG handle on file)
          message:         DM body (Meta hard-caps at 1000 chars)
          comment_id:      if set, send as "reply to comment via DM" (bypasses
                           the 24-hr engagement window — required for cold
                           reach-outs to people who only commented)
          conversation_id: optional — if you already know the IG conversation
        """
        if not contact_id or not message:
            logger.warning("send_ig_dm: missing contact_id or message")
            return False

        message = message[:1000]  # Meta cap

        if self.version == "v1":
            url = f"{V1_BASE}/conversations/messages"
        else:
            url = f"{V2_BASE}/conversations/messages"

        # GHL has used several "type" names for IG over time. Try each until
        # one sticks. Order matters — most current to most legacy.
        type_candidates = ["IG", "Instagram"]
        last_err = ""

        for t in type_candidates:
            payload = {
                "type": t,
                "contactId": contact_id,
                "message": message,
            }
            if conversation_id:
                payload["conversationId"] = conversation_id
            if comment_id:
                # GHL accepts several names for the comment-reply binding
                payload["replyToCommentId"] = comment_id
                payload["commentId"] = comment_id

            try:
                r = requests.post(url, headers=self._headers, json=payload, timeout=20)
            except Exception as e:
                last_err = f"network: {e}"
                continue

            if r.ok:
                logger.info(
                    "send_ig_dm OK (type=%s, comment_id=%s) → contact %s: %s",
                    t, comment_id, contact_id, message[:60],
                )
                return True

            last_err = f"{t}: {r.status_code} {r.text[:200]}"
            # 401/403/5xx — auth/server problem, no point trying other types
            if r.status_code in (401, 403, 500, 502, 503, 504):
                break

        logger.warning("send_ig_dm failed for %s: %s", contact_id, last_err)
        return False

    # ---- Human-override detection ---------------------------------------
    def is_human_active(self, contact_id: str) -> bool:
        """
        Returns True if a human (Umar) has been active in this conversation
        and the AI should stand down. Two signals:

          1. Contact has a "pause-ai" / "manual-mode" / "human-active" tag
          2. The most recent OUTBOUND message in the conversation was sent
             by a human user (source != api/automation/integration)

        If either is True, we don't auto-reply.
        """
        if not contact_id:
            return False

        # ---- Signal 1: tag check ----
        try:
            if self.version == "v1":
                url = f"{V1_BASE}/contacts/{contact_id}"
            else:
                url = f"{V2_BASE}/contacts/{contact_id}"
            r = requests.get(url, headers=self._headers, timeout=10)
            if r.ok:
                contact = r.json().get("contact") or r.json() or {}
                tags = contact.get("tags") or []
                pause_tags = {"pause-ai", "manual-mode", "human-active", "ai-off"}
                if any(t.lower() in pause_tags for t in tags):
                    logger.info("is_human_active: pause-ai tag found on %s", contact_id)
                    return True
        except Exception as e:
            logger.warning("is_human_active tag check failed: %s", e)

        # ---- Signal 2: last outbound message source ----
        try:
            # Find conversation
            if self.version == "v2":
                conv_url = f"{V2_BASE}/conversations/search"
                r = requests.post(
                    conv_url,
                    headers=self._headers,
                    json={"locationId": self.location_id, "contactId": contact_id, "limit": 1},
                    timeout=10,
                )
            else:
                # v1 doesn't have a clean conv search by contact, skip signal 2
                return False

            if not r.ok:
                return False
            conversations = r.json().get("conversations", []) or []
            if not conversations:
                return False
            conv_id = conversations[0].get("id") or conversations[0].get("_id")
            if not conv_id:
                return False

            # Get last 10 messages
            msg_url = f"{V2_BASE}/conversations/{conv_id}/messages"
            r = requests.get(msg_url, headers=self._headers, params={"limit": 10}, timeout=10)
            if not r.ok:
                return False
            data = r.json()
            messages = (data.get("messages") or {}).get("messages") or data.get("messages") or []

            # Look at outbound messages from newest to oldest
            for msg in messages[:10]:
                direction = (msg.get("direction") or msg.get("type") or "").lower()
                source = (msg.get("source") or msg.get("messageType") or "").lower()
                user_id = msg.get("userId") or msg.get("user_id") or ""
                if "outbound" not in direction:
                    continue
                # AI sends via API → source contains 'api' / 'integration' / 'automation' / 'workflow'
                ai_sources = {"api", "integration", "automation", "workflow", "bot"}
                if any(s in source for s in ai_sources):
                    # AI sent this — keep looking; if first outbound is AI, allow auto-reply
                    return False
                # Otherwise this was a human (agent/manual/UI send) → pause AI
                logger.info(
                    "is_human_active: human outbound found (source=%s, userId=%s) — pausing AI",
                    source, user_id,
                )
                return True
        except Exception as e:
            logger.warning("is_human_active conversation check failed: %s", e)

        return False

    # ---- Conversation history fetch -------------------------------------
    def get_recent_messages(self, contact_id: str, limit: int = 10) -> list:
        """
        Fetch the last N messages in this contact's conversation, formatted
        as Anthropic-style history: [{role: "user"|"assistant", content: "..."}].

        Used by the IG/SMS webhook so Bully AI has memory of prior turns.
        Returns oldest-first (so the model sees them in chronological order).
        Returns [] on any failure (caller falls back to no-history mode).
        """
        if not contact_id or self.version != "v2":
            return []
        try:
            # Find the conversation
            r = requests.post(
                f"{V2_BASE}/conversations/search",
                headers=self._headers,
                json={"locationId": self.location_id, "contactId": contact_id, "limit": 1},
                timeout=10,
            )
            if not r.ok:
                return []
            conversations = r.json().get("conversations", []) or []
            if not conversations:
                return []
            conv_id = conversations[0].get("id") or conversations[0].get("_id")
            if not conv_id:
                return []

            # Fetch messages
            r = requests.get(
                f"{V2_BASE}/conversations/{conv_id}/messages",
                headers=self._headers,
                params={"limit": limit},
                timeout=10,
            )
            if not r.ok:
                return []
            data = r.json()
            messages = (data.get("messages") or {}).get("messages") or data.get("messages") or []

            # Convert to Anthropic format. GHL returns newest-first, so reverse.
            history = []
            for msg in reversed(messages[:limit]):
                direction = (msg.get("direction") or msg.get("type") or "").lower()
                body = (msg.get("body") or msg.get("message") or "").strip()
                if not body:
                    continue
                # inbound = user said it, outbound = assistant (or human Umar) said it
                if "inbound" in direction:
                    role = "user"
                elif "outbound" in direction:
                    role = "assistant"
                else:
                    continue
                history.append({"role": role, "content": body[:1500]})

            # Anthropic requires alternating user/assistant. Collapse runs.
            collapsed = []
            for m in history:
                if collapsed and collapsed[-1]["role"] == m["role"]:
                    collapsed[-1]["content"] += "\n" + m["content"]
                else:
                    collapsed.append(m)

            # Drop trailing assistant turn (the last user msg will be appended by caller)
            if collapsed and collapsed[-1]["role"] == "assistant":
                pass  # OK to leave it
            return collapsed[-10:]  # cap at 10 turns
        except Exception as e:
            logger.warning("get_recent_messages failed: %s", e)
            return []

    # ---- Contact search by tag ------------------------------------------
    def search_contacts_by_tag(self, tag: str, limit: int = 500) -> list:
        """Return list of contacts that carry the given tag. Paginates up to `limit`."""
        results = []
        if self.version == "v1":
            # v1 has /contacts with ?query=tag:xxx or we paginate through /contacts/
            page = 0
            while len(results) < limit:
                url = f"{V1_BASE}/contacts/"
                r = requests.get(
                    url,
                    headers=self._headers,
                    params={"query": tag, "limit": 100, "startAfter": page * 100},
                    timeout=25,
                )
                if not r.ok:
                    break
                batch = r.json().get("contacts", [])
                if not batch:
                    break
                results.extend([c for c in batch if tag in (c.get("tags") or [])])
                if len(batch) < 100:
                    break
                page += 1
        else:
            # v2 search endpoint
            url = f"{V2_BASE}/contacts/search"
            try:
                r = requests.post(
                    url,
                    headers=self._headers,
                    json={
                        "locationId": self.location_id,
                        "pageLimit": 100,
                        "filters": [{"field": "tags", "operator": "contains", "value": tag}],
                    },
                    timeout=25,
                )
                if r.ok:
                    results = r.json().get("contacts", []) or []
            except Exception as e:
                logger.warning("search_contacts_by_tag v2 failed: %s", e)
            if not results:
                # Fallback: /contacts/ list with tag filter
                try:
                    r = requests.get(
                        f"{V2_BASE}/contacts/",
                        headers=self._headers,
                        params={"locationId": self.location_id, "query": tag, "limit": 100},
                        timeout=25,
                    )
                    if r.ok:
                        results = r.json().get("contacts", []) or []
                except Exception:
                    pass
        return results[:limit]

    # ---- Tag helper ------------------------------------------------------
    def add_tags(self, contact_id: str, tags: list) -> None:
        if self.version == "v1":
            url = f"{V1_BASE}/contacts/{contact_id}/tags/"
        else:
            url = f"{V2_BASE}/contacts/{contact_id}/tags"
        r = requests.post(url, headers=self._headers, json={"tags": tags}, timeout=20)
        if not r.ok:
            raise GHLError(f"add_tags: {r.status_code} {r.text[:300]}")

    # ---- Global Custom Values (for {{custom_values.X}} merge tags in SMS)
    _cv_cache: dict = {}

    def list_custom_values(self) -> list:
        """List global custom values (used in SMS templates as {{custom_values.X}})."""
        if self.version == "v1":
            url = f"{V1_BASE}/custom-values/"
        else:
            url = f"{V2_BASE}/locations/{self.location_id}/customValues"
        r = requests.get(url, headers=self._headers, timeout=20)
        if not r.ok:
            raise GHLError(f"list_custom_values: {r.status_code} {r.text[:300]}")
        data = r.json()
        return data.get("customValues") or data.get("custom_values") or []

    def upsert_custom_value(self, name: str, value: str) -> None:
        """Create or update a global custom value by name. Safe to call repeatedly."""
        # Cache existing values on first call
        if not self._cv_cache:
            try:
                for cv in self.list_custom_values():
                    k = cv.get("name") or cv.get("fieldKey", "")
                    if k:
                        self._cv_cache[k] = cv.get("id") or cv.get("_id")
            except Exception as e:
                logger.warning("Could not load custom values cache: %s", e)

        value_str = "" if value is None else str(value)
        cv_id = self._cv_cache.get(name)

        try:
            if cv_id:
                # UPDATE existing
                if self.version == "v1":
                    url = f"{V1_BASE}/custom-values/{cv_id}"
                else:
                    url = f"{V2_BASE}/locations/{self.location_id}/customValues/{cv_id}"
                r = requests.put(url, headers=self._headers, json={"name": name, "value": value_str}, timeout=15)
            else:
                # CREATE new
                if self.version == "v1":
                    url = f"{V1_BASE}/custom-values/"
                else:
                    url = f"{V2_BASE}/locations/{self.location_id}/customValues"
                r = requests.post(url, headers=self._headers, json={"name": name, "value": value_str}, timeout=15)

            if r.ok:
                body = r.json() if r.content else {}
                new_id = (body.get("customValue") or body).get("id") or (body.get("customValue") or body).get("_id")
                if new_id and not cv_id:
                    self._cv_cache[name] = new_id
                logger.info("Custom value upsert OK: %s = %s", name, value_str[:40])
            else:
                logger.warning("Custom value upsert failed (%s): %s — %s", name, r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("Custom value upsert error (%s): %s", name, e)

    def push_scan_custom_values(self, custom_fields: dict) -> None:
        """
        Push scan data into global custom values so SMS templates using
        {{custom_values.cr_top_collection_name}} etc. will actually resolve.

        NOTE: global custom values are shared across the sub-account. Every new
        scan overwrites the previous scanner's values. This is acceptable for
        launch-phase volume (scans are seconds apart, SMS fires within 60 sec).
        """
        # ---- Sanitize the opener so the SMS never says nonsense ----------
        # The SMS template reads:
        #   "That {{cr_top_collection_name}} collection (${{cr_top_collection_amount}})
        #    is still inside the statute of limitations..."
        # If there are no collections we need the opener to pivot to charge-offs
        # or late payments so Bully AI isn't saying "sue no one".
        sanitized = dict(custom_fields)  # don't mutate caller's dict

        top_name = str(sanitized.get("cr_top_collection_name", "") or "").strip()
        top_amt  = float(sanitized.get("cr_top_collection_amount", 0) or 0)

        # If analyzer returned placeholder text or $0, pivot to next-worst item
        bad_name = (
            not top_name
            or top_name.upper().startswith("N/A")
            or "no collection" in top_name.lower()
            or top_amt <= 0
        )
        if bad_name:
            # Re-derive the opener from what's actually on the report
            chargeoffs = float(sanitized.get("cr_chargeoffs_value", 0) or 0)
            late_count = int(sanitized.get("cr_late_payments", 0) or 0)
            neg_count  = int(sanitized.get("cr_negative_items", 0) or 0)

            if chargeoffs > 0:
                # Pivot to charge-off language
                sanitized["cr_top_collection_name"] = (
                    f"${int(chargeoffs):,} in charge-offs — those are lawsuit bait"
                )
                sanitized["cr_top_collection_amount"] = chargeoffs
            elif late_count >= 3:
                sanitized["cr_top_collection_name"] = (
                    f"{late_count} late payments blocking every approval"
                )
                sanitized["cr_top_collection_amount"] = 0
            elif neg_count > 0:
                sanitized["cr_top_collection_name"] = (
                    f"{neg_count} negative items dragging your score down"
                )
                sanitized["cr_top_collection_amount"] = 0
            else:
                # Nothing bad? Short-circuit — set an honest, non-fearmongering line
                sanitized["cr_top_collection_name"] = (
                    "your report is cleaner than most — let's optimize it"
                )
                sanitized["cr_top_collection_amount"] = 0

        # ---- Also pre-build a complete, coherent SMS opener ---------------
        # This can be used as {{custom_values.cr_sms_opener}} in future SMS
        # templates that want a single intelligent sentence instead of a
        # mad-libs-style fill-in.
        first = sanitized.get("cr_full_name") or ""
        violations = int(sanitized.get("cr_violations_count", 0) or 0)
        leverage = float(sanitized.get("cr_total_leverage", 0) or 0)
        top_name_final = sanitized.get("cr_top_collection_name", "")
        top_amt_final  = float(sanitized.get("cr_top_collection_amount", 0) or 0)

        if top_amt_final > 0 and "charge-off" not in top_name_final.lower() and "late" not in top_name_final.lower() and "negative" not in top_name_final.lower() and "cleaner" not in top_name_final.lower():
            # It's a real collection
            opener = (
                f"That {top_name_final} collection (${top_amt_final:,.0f}) is "
                f"inside the statute of limitations — they CAN sue you if you "
                f"don't act. And they do."
            )
        elif "cleaner" in top_name_final.lower():
            opener = (
                "Good news — no major collections, but I found "
                f"{violations} technical violations worth about ${leverage:,.0f} "
                "in leverage you can use."
            )
        else:
            opener = (
                f"I just analyzed your report — {top_name_final}. "
                f"Stack that with {violations} federal violations worth "
                f"~${leverage:,.0f} in leverage and you've got real teeth."
            )
        sanitized["cr_sms_opener"] = opener

        # Fields that appear in SMS templates — push each
        SMS_RELEVANT = [
            "cr_top_collection_name",
            "cr_top_collection_amount",
            "cr_violations_count",
            "cr_total_leverage",
            "cr_fico_range",
            "cr_urgency_hook",
            "cr_fear_hook",
            "cr_case_law_cited",
            "cr_top_pain_point",
            "cr_exec_summary",
            "cr_recommended_tier",
            "cr_sms_opener",
        ]
        for key in SMS_RELEVANT:
            if key in sanitized:
                self.upsert_custom_value(key, sanitized[key])


# ---- High-level: push an analyzed lead end-to-end ------------------------
def push_lead_to_ghl(
    first_name: str,
    last_name: str,
    email: str,
    phone: str,
    custom_fields: dict,
    *,
    urgency_score: int,
    recommended_tier: str,
) -> dict:
    client = GHLClient()

    # CORRECT FUNNEL LOGIC:
    # Every new lead enters the $17 Toolkit Drip first — the cheap impulse buy.
    # The analyzer's recommendation (accelerator / dfy) is stored as a
    # "planned-tier-*" tag so we can upsell later (Day 3 Vault push,
    # Day 7-14 DFY push for high-leverage leads) without triggering the
    # wrong workflow on day 0.
    tags = ["bureau-scan", "tier-toolkit", f"planned-tier-{recommended_tier}"]
    if urgency_score >= 80:
        tags.append("heat-critical")
    elif urgency_score >= 60:
        tags.append("heat-hot")
    elif urgency_score >= 40:
        tags.append("heat-warm")
    else:
        tags.append("heat-cold")

    result = client.upsert_contact(
        first_name=first_name,
        last_name=last_name,
        email=email,
        phone=phone,
        custom_fields=custom_fields,
        tags=tags,
    )

    contact_id = (result.get("contact") or result).get("id") or (result.get("contact") or result).get("_id")

    # Mirror scan data into GLOBAL custom values so SMS templates using
    # {{custom_values.cr_top_collection_name}} etc. actually resolve.
    # (Workaround: per-contact custom fields would require editing every SMS
    # template from {{custom_values.cr_*}} to {{contact.cr_*}}.)
    try:
        client.push_scan_custom_values(custom_fields)
    except Exception as e:
        logger.warning("push_scan_custom_values failed: %s", e)

    workflow_map = {
        "toolkit":     os.getenv("GHL_WORKFLOW_TOOLKIT"),
        "accelerator": os.getenv("GHL_WORKFLOW_ACCELERATOR"),
        "dfy":         os.getenv("GHL_WORKFLOW_DFY"),
    }
    wf_id = workflow_map.get(recommended_tier)
    if wf_id and contact_id:
        try:
            client.add_to_workflow(contact_id, wf_id)
        except GHLError as e:
            logger.warning("Could not add to workflow: %s", e)

    return result


# ---- Canonical custom field schema ---------------------------------------
# Used by setup_ghl.py to auto-create all fields
REQUIRED_FIELDS = [
    ("cr_full_name",            "CR Full Name",              "TEXT"),
    ("cr_fico_range",           "CR FICO Range",             "TEXT"),
    ("cr_negative_items",       "CR Negative Items",         "NUMERICAL"),
    ("cr_collections_value",    "CR Collections Value",      "MONETORY"),
    ("cr_chargeoffs_value",     "CR Charge-offs Value",      "MONETORY"),
    ("cr_late_payments",        "CR Late Payments",          "NUMERICAL"),
    ("cr_inquiries",            "CR Hard Inquiries",         "NUMERICAL"),
    ("cr_total_leverage",       "CR Total Leverage",         "MONETORY"),
    ("cr_top_pain_point",       "CR Top Pain Point",         "LARGE_TEXT"),
    ("cr_top_collection_name",  "CR Top Collection Name",    "TEXT"),
    ("cr_top_collection_amount","CR Top Collection Amount",  "MONETORY"),
    ("cr_urgency_score",        "CR Urgency Score",          "NUMERICAL"),
    ("cr_recommended_tier",     "CR Recommended Tier",       "TEXT"),
    ("cr_fear_hook",            "CR Fear Hook",              "LARGE_TEXT"),
    ("cr_urgency_hook",         "CR Urgency Hook",           "LARGE_TEXT"),
    ("cr_exec_summary",         "CR Executive Summary",      "LARGE_TEXT"),
    ("cr_case_law_cited",       "CR Case Law Cited",         "LARGE_TEXT"),
    ("cr_violations_count",     "CR Violations Count",       "NUMERICAL"),
    ("cr_violations_json",      "CR Violations (JSON)",      "LARGE_TEXT"),
    ("cr_doc_url",              "CR Attack Plan Doc URL",    "TEXT"),
    # Goal captured on the landing page form
    ("cr_goal",                 "CR Goal",                   "TEXT"),
    ("cr_goal_label",           "CR Goal Label",             "TEXT"),
    # 7-email tailored nurture drip — generated per scan by email_generator.py
    ("cr_email_1_subject",      "CR Email 1 Subject",        "TEXT"),
    ("cr_email_1_body",         "CR Email 1 Body",           "LARGE_TEXT"),
    ("cr_email_2_subject",      "CR Email 2 Subject",        "TEXT"),
    ("cr_email_2_body",         "CR Email 2 Body",           "LARGE_TEXT"),
    ("cr_email_3_subject",      "CR Email 3 Subject",        "TEXT"),
    ("cr_email_3_body",         "CR Email 3 Body",           "LARGE_TEXT"),
    ("cr_email_4_subject",      "CR Email 4 Subject",        "TEXT"),
    ("cr_email_4_body",         "CR Email 4 Body",           "LARGE_TEXT"),
    ("cr_email_5_subject",      "CR Email 5 Subject",        "TEXT"),
    ("cr_email_5_body",         "CR Email 5 Body",           "LARGE_TEXT"),
    ("cr_email_6_subject",      "CR Email 6 Subject",        "TEXT"),
    ("cr_email_6_body",         "CR Email 6 Body",           "LARGE_TEXT"),
    ("cr_email_7_subject",      "CR Email 7 Subject",        "TEXT"),
    ("cr_email_7_body",         "CR Email 7 Body",           "LARGE_TEXT"),
]
