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
        ]
        for key in SMS_RELEVANT:
            if key in custom_fields:
                self.upsert_custom_value(key, custom_fields[key])


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

    tags = ["bureau-scan", f"tier-{recommended_tier}"]
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
]
