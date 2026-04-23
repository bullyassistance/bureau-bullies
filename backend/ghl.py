"""
Bureau Bullies — GoHighLevel (GHL) API integration
---------------------------------------------------
Supports BOTH auth methods:
  - v1 API  — JWT token (eyJ...) → base: https://rest.gohighlevel.com/v1
  - v2 API  — Private Integration token (pit-...) → base: https://services.leadconnectorhq.com
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


def _detect_version(token):
    if token.startswith("pit-"):
        return "v2"
    if token.startswith("eyJ"):
        return "v1"
    return "v2"


class GHLClient:
    def __init__(self, api_key=None, location_id=None):
        self.api_key = api_key or os.getenv("GHL_API_KEY")
        self.location_id = location_id or os.getenv("GHL_LOCATION_ID")
        if not self.api_key or not self.location_id:
            raise GHLError("GHL_API_KEY and GHL_LOCATION_ID must be set")
        self.version = _detect_version(self.api_key)
        logger.info("GHL client initialized (API %s)", self.version)

    @property
    def _headers(self):
        h = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json", "Accept": "application/json"}
        if self.version == "v2":
            h["Version"] = V2_API_VERSION
        return h

    @property
    def base(self):
        return V2_BASE if self.version == "v2" else V1_BASE

    def ping(self):
        return self.list_custom_fields()

    _field_cache = {}

    def list_custom_fields(self):
        if self.version == "v1":
            url = f"{V1_BASE}/custom-fields/"
        else:
            url = f"{V2_BASE}/locations/{self.location_id}/customFields"
        r = requests.get(url, headers=self._headers, timeout=20)
        if not r.ok:
            raise GHLError(f"list_custom_fields: {r.status_code} {r.text[:300]}")
        data = r.json()
        return data.get("customFields", []) or data.get("custom_fields", [])

    def create_custom_field(self, name, field_key, data_type="TEXT"):
        if self.version == "v1":
            url = f"{V1_BASE}/custom-fields/"
            payload = {"name": name, "fieldKey": field_key, "dataType": data_type}
        else:
            url = f"{V2_BASE}/locations/{self.location_id}/customFields"
            payload = {"name": name, "dataType": data_type, "fieldKey": field_key, "model": "contact"}
        r = requests.post(url, headers=self._headers, json=payload, timeout=20)
        if not r.ok:
            raise GHLError(f"create_custom_field({name}): {r.status_code} {r.text[:300]}")
        return r.json()

    def ensure_field(self, key, display_name=None, data_type=None):
        """Make sure a custom field exists in GHL. Creates it if missing. Returns its ID."""
        if not self._field_cache:
            for f in self.list_custom_fields():
                k = (f.get("fieldKey", "") or f.get("name", "")).replace("contact.", "")
                if k:
                    self._field_cache[k] = f.get("id") or f.get("_id")
        if key in self._field_cache:
            return self._field_cache[key]
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

    def upsert_contact(self, first_name, last_name, email, phone, custom_fields, tags):
        field_map = self._field_cache
        if not field_map:
            for f in self.list_custom_fields():
                key = (f.get("fieldKey", "") or f.get("name", "")).replace("contact.", "")
                if key:
                    field_map[key] = f.get("id") or f.get("_id")
            self._field_cache = field_map
            logger.info("Loaded %d existing GHL custom fields", len(field_map))
        # Auto-create any missing fields
        for k in list(custom_fields.keys()):
            if k not in field_map:
                self.ensure_field(k)
                field_map = self._field_cache
        if self.version == "v1":
            gh_customs = {}
            for key, value in custom_fields.items():
                fid = field_map.get(key)
                if not fid:
                    logger.warning("GHL v1: custom field '%s' not found — skipping", key)
                    continue
                gh_customs[fid] = value
            payload = {"firstName": first_name, "lastName": last_name, "name": f"{first_name} {last_name}".strip(), "email": email, "phone": phone, "tags": tags, "source": "bureaubullies.com scan", "customField": gh_customs}
            url = f"{V1_BASE}/contacts/"
            r = requests.post(url, headers=self._headers, json=payload, timeout=25)
        else:
            gh_customs = []
            for key, value in custom_fields.items():
                fid = field_map.get(key)
                if not fid:
                    logger.warning("GHL v2: custom field '%s' not found — skipping", key)
                    continue
                gh_customs.append({"id": fid, "field_value": value})
            payload = {"firstName": first_name, "lastName": last_name, "name": f"{first_name} {last_name}".strip(), "email": email, "phone": phone, "locationId": self.location_id, "customFields": gh_customs, "tags": tags, "source": "bureaubullies.com scan"}
            url = f"{V2_BASE}/contacts/upsert"
            r = requests.post(url, headers=self._headers, json=payload, timeout=25)
        if not r.ok:
            raise GHLError(f"upsert_contact: {r.status_code} {r.text[:500]}")
        resp = r.json()
        cid = (resp.get("contact") or resp).get("id") or (resp.get("contact") or resp).get("_id")
        logger.info("GHL upsert OK — contactId=%s", cid)
        return resp

    def add_to_workflow(self, contact_id, workflow_id):
        if self.version == "v1":
            url = f"{V1_BASE}/contacts/{contact_id}/workflow/{workflow_id}"
        else:
            url = f"{V2_BASE}/contacts/{contact_id}/workflow/{workflow_id}"
        r = requests.post(url, headers=self._headers, json={}, timeout=20)
        if not r.ok:
            raise GHLError(f"add_to_workflow: {r.status_code} {r.text[:300]}")

    def add_tags(self, contact_id, tags):
        if self.version == "v1":
            url = f"{V1_BASE}/contacts/{contact_id}/tags/"
        else:
            url = f"{V2_BASE}/contacts/{contact_id}/tags"
        r = requests.post(url, headers=self._headers, json={"tags": tags}, timeout=20)
        if not r.ok:
            raise GHLError(f"add_tags: {r.status_code} {r.text[:300]}")

    _cv_cache = {}

    def list_custom_values(self):
        if self.version == "v1":
            url = f"{V1_BASE}/custom-values/"
        else:
            url = f"{V2_BASE}/locations/{self.location_id}/customValues"
        r = requests.get(url, headers=self._headers, timeout=20)
        if not r.ok:
            raise GHLError(f"list_custom_values: {r.status_code} {r.text[:300]}")
        data = r.json()
        return data.get("customValues") or data.get("custom_values") or []

    def upsert_custom_value(self, name, value):
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
                if self.version == "v1":
                    url = f"{V1_BASE}/custom-values/{cv_id}"
                else:
                    url = f"{V2_BASE}/locations/{self.location_id}/customValues/{cv_id}"
                r = requests.put(url, headers=self._headers, json={"name": name, "value": value_str}, timeout=15)
            else:
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

    def push_scan_custom_values(self, custom_fields):
        sanitized = dict(custom_fields)
        top_name = str(sanitized.get("cr_top_collection_name", "") or "").strip()
        top_amt = float(sanitized.get("cr_top_collection_amount", 0) or 0)
        bad_name = (not top_name or top_name.upper().startswith("N/A") or "no collection" in top_name.lower() or top_amt <= 0)
        if bad_name:
            chargeoffs = float(sanitized.get("cr_chargeoffs_value", 0) or 0)
            late_count = int(sanitized.get("cr_late_payments", 0) or 0)
            neg_count = int(sanitized.get("cr_negative_items", 0) or 0)
            if chargeoffs > 0:
                sanitized["cr_top_collection_name"] = f"${int(chargeoffs):,} in charge-offs — those are lawsuit bait"
                sanitized["cr_top_collection_amount"] = chargeoffs
            elif late_count >= 3:
                sanitized["cr_top_collection_name"] = f"{late_count} late payments blocking every approval"
                sanitized["cr_top_collection_amount"] = 0
            elif neg_count > 0:
                sanitized["cr_top_collection_name"] = f"{neg_count} negative items dragging your score down"
                sanitized["cr_top_collection_amount"] = 0
            else:
                sanitized["cr_top_collection_name"] = "your report is cleaner than most — let's optimize it"
                sanitized["cr_top_collection_amount"] = 0
        violations = int(sanitized.get("cr_violations_count", 0) or 0)
        leverage = float(sanitized.get("cr_total_leverage", 0) or 0)
        top_name_final = sanitized.get("cr_top_collection_name", "")
        top_amt_final = float(sanitized.get("cr_top_collection_amount", 0) or 0)
        if top_amt_final > 0 and "charge-off" not in top_name_final.lower() and "late" not in top_name_final.lower() and "negative" not in top_name_final.lower() and "cleaner" not in top_name_final.lower():
            opener = f"That {top_name_final} collection (${top_amt_final:,.0f}) is inside the statute of limitations — they CAN sue you if you don't act. And they do."
        elif "cleaner" in top_name_final.lower():
            opener = f"Good news — no major collections, but I found {violations} technical violations worth about ${leverage:,.0f} in leverage you can use."
        else:
            opener = f"I just analyzed your report — {top_name_final}. Stack that with {violations} federal violations worth ~${leverage:,.0f} in leverage and you've got real teeth."
        sanitized["cr_sms_opener"] = opener
        SMS_RELEVANT = ["cr_top_collection_name", "cr_top_collection_amount", "cr_violations_count", "cr_total_leverage", "cr_fico_range", "cr_urgency_hook", "cr_fear_hook", "cr_case_law_cited", "cr_top_pain_point", "cr_exec_summary", "cr_recommended_tier", "cr_sms_opener"]
        for key in SMS_RELEVANT:
            if key in sanitized:
                self.upsert_custom_value(key, sanitized[key])


def push_lead_to_ghl(first_name, last_name, email, phone, custom_fields, *, urgency_score, recommended_tier):
    client = GHLClient()
    tags = ["bureau-scan", "tier-toolkit", f"planned-tier-{recommended_tier}"]
    if urgency_score >= 80:
        tags.append("heat-critical")
    elif urgency_score >= 60:
        tags.append("heat-hot")
    elif urgency_score >= 40:
        tags.append("heat-warm")
    else:
        tags.append("heat-cold")
    result = client.upsert_contact(first_name=first_name, last_name=last_name, email=email, phone=phone, custom_fields=custom_fields, tags=tags)
    contact_id = (result.get("contact") or result).get("id") or (result.get("contact") or result).get("_id")
    try:
        client.push_scan_custom_values(custom_fields)
    except Exception as e:
        logger.warning("push_scan_custom_values failed: %s", e)
    workflow_map = {"toolkit": os.getenv("GHL_WORKFLOW_TOOLKIT"), "accelerator": os.getenv("GHL_WORKFLOW_ACCELERATOR"), "dfy": os.getenv("GHL_WORKFLOW_DFY")}
    wf_id = workflow_map.get(recommended_tier)
    if wf_id and contact_id:
        try:
            client.add_to_workflow(contact_id, wf_id)
        except GHLError as e:
            logger.warning("Could not add to workflow: %s", e)
    return result


REQUIRED_FIELDS = [
    ("cr_full_name", "CR Full Name", "TEXT"),
    ("cr_fico_range", "CR FICO Range", "TEXT"),
    ("cr_negative_items", "CR Negative Items", "NUMERICAL"),
    ("cr_collections_value", "CR Collections Value", "MONETORY"),
    ("cr_chargeoffs_value", "CR Charge-offs Value", "MONETORY"),
    ("cr_late_payments", "CR Late Payments", "NUMERICAL"),
    ("cr_inquiries", "CR Hard Inquiries", "NUMERICAL"),
    ("cr_total_leverage", "CR Total Leverage", "MONETORY"),
    ("cr_top_pain_point", "CR Top Pain Point", "LARGE_TEXT"),
    ("cr_top_collection_name", "CR Top Collection Name", "TEXT"),
    ("cr_top_collection_amount", "CR Top Collection Amount", "MONETORY"),
    ("cr_urgency_score", "CR Urgency Score", "NUMERICAL"),
    ("cr_recommended_tier", "CR Recommended Tier", "TEXT"),
    ("cr_fear_hook", "CR Fear Hook", "LARGE_TEXT"),
    ("cr_urgency_hook", "CR Urgency Hook", "LARGE_TEXT"),
    ("cr_exec_summary", "CR Executive Summary", "LARGE_TEXT"),
    ("cr_case_law_cited", "CR Case Law Cited", "LARGE_TEXT"),
    ("cr_violations_count", "CR Violations Count", "NUMERICAL"),
    ("cr_violations_json", "CR Violations (JSON)", "LARGE_TEXT"),
    ("cr_doc_url", "CR Attack Plan Doc URL", "TEXT"),
    ("cr_goal", "CR Goal", "TEXT"),
    ("cr_goal_label", "CR Goal Label", "TEXT"),
    ("cr_email_1_subject", "CR Email 1 Subject", "TEXT"),
    ("cr_email_1_body", "CR Email 1 Body", "LARGE_TEXT"),
    ("cr_email_2_subject", "CR Email 2 Subject", "TEXT"),
    ("cr_email_2_body", "CR Email 2 Body", "LARGE_TEXT"),
    ("cr_email_3_subject", "CR Email 3 Subject", "TEXT"),
    ("cr_email_3_body", "CR Email 3 Body", "LARGE_TEXT"),
    ("cr_email_4_subject", "CR Email 4 Subject", "TEXT"),
    ("cr_email_4_body", "CR Email 4 Body", "LARGE_TEXT"),
    ("cr_email_5_subject", "CR Email 5 Subject", "TEXT"),
    ("cr_email_5_body", "CR Email 5 Body", "LARGE_TEXT"),
    ("cr_email_6_subject", "CR Email 6 Subject", "TEXT"),
    ("cr_email_6_body", "CR Email 6 Body", "LARGE_TEXT"),
    ("cr_email_7_subject", "CR Email 7 Subject", "TEXT"),
    ("cr_email_7_body", "CR Email 7 Body", "LARGE_TEXT"),
]
