# GHL Workflow & SMS Spec — Bureau Bullies Conversion Machine

Blueprint for the GoHighLevel SMS sequence that fires the moment Bully AI finishes the scan. Every message is personalized to the consumer's specific report using GHL custom field merge tags.

---

## 1. Custom Fields to Create in GHL

**Settings → Custom Fields**, create these (Contact-level):

| Field Key | Type | Example |
|---|---|---|
| `cr_full_name` | Text | "Marcus Harris" |
| `cr_fico_range` | Text | "540-580" |
| `cr_negative_items` | Number | 7 |
| `cr_collections_value` | Number | 5127.00 |
| `cr_chargeoffs_value` | Number | 3200.00 |
| `cr_late_payments` | Number | 9 |
| `cr_inquiries` | Number | 12 |
| `cr_total_leverage` | Number | 14500.00 |
| `cr_top_pain_point` | Text | "$5,127 LVNV collection killing your score" |
| `cr_top_collection_name` | Text | "LVNV Funding LLC" |
| `cr_top_collection_amount` | Number | 5127.00 |
| `cr_urgency_score` | Number | 87 |
| `cr_recommended_tier` | Text | "accelerator" |
| `cr_fear_hook` | Text (Large) | Personalized fear opener |
| `cr_urgency_hook` | Text (Large) | Personalized urgency opener |
| `cr_exec_summary` | Text (Large) | Bully AI's verdict |
| `cr_case_law_cited` | Text | "Hinkle; Saunders; Gorman" |
| `cr_violations_count` | Number | 14 |
| `cr_violations_json` | Text (Large) | JSON dump |
| `cr_doc_url` | URL | `/download/{token}` — the Word doc |

**Tags auto-applied by the backend:** `bureau-scan`, `tier-toolkit` / `tier-accelerator` / `tier-dfy`, `heat-critical` / `heat-hot` / `heat-warm` / `heat-cold`.

---

## 2. Three Workflows (one per tier)

Trigger: **Tag Added = `bureau-scan`** → branch by tier tag. Each workflow runs the full SMS drip below.

---

## 3. THE SMS SEQUENCE

### **SMS 1 — Instant (0 min). The named-collection punch.**

```
{{contact.first_name}}, Bully AI here.

That {{custom_values.cr_top_collection_name}} collection ({{custom_values.cr_top_collection_amount}}) is still inside the statute of limitations in your state — which means they CAN sue you if you don't take action. And they do.

I just flagged {{custom_values.cr_violations_count}} violations on your report worth about ${{custom_values.cr_total_leverage}} in leverage.

Your full attack plan (plus your dispute letters in a Word doc) is ready. Want me to walk you through it?

Reply YES and I'll send the $17 Collection Killer — it's the exact playbook I built this whole company on.
```

### **SMS 2 — (+15 min). The specific stab + $17 pitch.**

```
Real talk {{contact.first_name}} —

{{custom_values.cr_top_collection_name}} bought your alleged debt for pennies. They profit off silence.

But under 15 U.S.C. § 1681s-2(b), if they can't produce original documentation, they MUST delete. Most can't.

The Collection Killer ebook walks you through the exact sequence I used to settle a collection for $5,000 (before I started The Bureau Bullies).

$17. One time. Deploy tonight:
👉 [Toolkit Link]
```

### **SMS 3 — (+1 hr). The evidence backup.**

```
Bully AI again.

Your report referenced these federal cases: {{custom_values.cr_case_law_cited}}

These are REAL federal court decisions that make § 1681s-2(b) enforceable. Not theories — precedent.

Your Word doc has your custom dispute letter, your validation letter, and your demand letter — pre-filled with your data.

👉 Grab the Collection Killer for $17 and the full playbook drops in your inbox: [Toolkit Link]
```

### **SMS 4 — Day 1. The math.**

```
{{contact.first_name}} — quick math.

Your FICO range: {{custom_values.cr_fico_range}}
Your leverage: ~${{custom_values.cr_total_leverage}}
Cost of doing nothing: denied approvals, 9%+ APRs, rejected applications for 7 more years.

$17 fixes the first problem today.
👉 [Toolkit Link]
```

### **SMS 5 — Day 2. Urgency drop.**

```
{{custom_values.cr_urgency_hook}}

Every day you wait, {{custom_values.cr_top_collection_name}} gets more time to claim they "verified" it.

Strike while they're vulnerable.
👉 [Toolkit Link]
```

### **SMS 6 — DAY 3 — The "how is it going?" check-in (FIRST CRITICAL MOMENT)**

**Conditional:** Only send if tag `intent-toolkit` or purchase event present (i.e. they bought $17).

```
{{contact.first_name}}, Bully AI. Checking in.

It's been 3 days since you got the Collection Killer. How's it going? Did you send your validation letter yet?

If you're stuck or want me to draft the custom letters for you (no template-work), the Dispute Vault has every letter pre-built for YOUR report. $66 once.

Reply:
  - NEED HELP → I'll upgrade you
  - GOT IT → keep going, I'm here if you get stuck
  - STUCK [on what] → tell me what's blocking you
```

### **SMS 7 — Day 3 (alternate).** For people who DIDN'T buy the $17 yet.

```
{{contact.first_name}} — still thinking about it?

I get it. But here's the thing: {{custom_values.cr_top_pain_point}} is still sitting there. Still reporting. Still dragging your score.

For $17 you get the exact sequence that's deleted thousands of these.

Or if you want the whole toolkit done-with-you — the Dispute Vault is $66. Letters pre-written, case law cheat sheet, tracker.

Toolkit → [link]
Vault → [link]
```

### **SMS 8 — Day 5. Push to $66 Vault.**

```
{{contact.first_name}}, real talk.

The Collection Killer is the playbook. The Dispute Vault is the weapon.

You've got {{custom_values.cr_violations_count}} violations. Writing every letter yourself will take 20+ hours. The Vault has them pre-built for YOUR accounts.

$66 once. Upgrade: [Vault Link]
```

### **SMS 9 — Day 7. The DFY push.** (Only for `tier-dfy` or `heat-critical` tags)

```
{{contact.first_name}}, I'll be straight with you.

With ${{custom_values.cr_total_leverage}} in leverage and {{custom_values.cr_violations_count}} violations, you shouldn't be doing this yourself.

Our DFY squad handles everything — we pull, we dispute, we demand, we sue violators WITH you.

45% off right now:
  - $229/month  OR
  - $2,500 one-time (reg. $4,545)

Book a 15-min strategy call: [Calendar Link]
```

### **SMS 10 — Day 10. Ghost breaker.**

```
{{contact.first_name}} — still there?

Your free scan expires in 24 hours. After that I clear the violation data from my system.

Before it goes:
  Toolkit $17 → [link]
  Vault $66 → [link]
  DFY 45% off → [link]

Your call.
```

### **SMS 11 — Day 14. Last shot.**

```
Last message {{contact.first_name}}.

I've tried. Your ${{custom_values.cr_total_leverage}} in leverage is still sitting there, getting weaker every week.

If you ever change your mind: thebureaubullies.com

— Bully AI
```

---

## 4. Keyword Reply Triggers

Set up in **Automations → Triggers → SMS Reply**:

| Reply | Action |
|---|---|
| `YES` or `TOOLKIT` | Send $17 Stripe link, tag `intent-toolkit` |
| `VAULT` or `ACCEL` | Send $66 Stripe link, tag `intent-accel` |
| `DFY` | Send calendar + pricing link, tag `intent-dfy` |
| `NEED HELP` | Tag `intent-upsell` + send $66 link |
| `STUCK` | Route to Bully AI chat (via /api/chat webhook) — AI responds |
| `STOP` | TCPA unsubscribe (GHL automatic) |
| anything else | Route to Bully AI chat (POST to /api/chat with history) |

---

## 5. Bully AI Chat Webhook

Connect GHL "SMS Received" webhook to:

```
POST https://yourdomain.com/api/chat
Content-Type: application/json

{
  "message": "{{sms.body}}",
  "contact": {
    "cr_top_collection_name": "{{contact.cr_top_collection_name}}",
    "cr_total_leverage": "{{contact.cr_total_leverage}}",
    "cr_violations_count": "{{contact.cr_violations_count}}",
    "cr_recommended_tier": "{{contact.cr_recommended_tier}}",
    "cr_fear_hook": "{{contact.cr_fear_hook}}"
  },
  "history": []
}
```

Response `reply` field → send back as SMS.

---

## 6. Branching Logic (inside each workflow)

```
Trigger: Tag Added = bureau-scan
  ├─ Has tag "tier-toolkit" → Toolkit sequence (SMS 1-11)
  ├─ Has tag "tier-accelerator" → Accelerator sequence (SMS 1-11, skip 9)
  └─ Has tag "tier-dfy" OR "heat-critical" → DFY sequence (emphasize SMS 9)

At each step check:
  ├─ Purchase event → remove from drip, add to post-purchase nurture
  ├─ Reply STOP → unsubscribe
  ├─ Reply YES/TOOLKIT → send $17 checkout
  ├─ Reply VAULT → send $66 checkout
  ├─ Reply DFY → send calendar
  ├─ Reply "NEED HELP" → tag intent-upsell, send Vault link
  └─ Any other reply → Bully AI chat webhook
```

---

## 7. Compliance Notes

- **TCPA** — consent checkbox on landing page covers this.
- **10DLC** — phone number must be A2P registered in GHL.
- **Quiet hours** — no SMS before 9am or after 9pm local time.
- **Frequency cap** — max 2 SMS/day per contact.
- **CROA** — consumers have a 3-business-day right to cancel any credit-repair contract. Honor it.
- **Honor STOP** — GHL does this automatically.
