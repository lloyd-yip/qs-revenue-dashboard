---
tags: [expenses, p&l, xero, seed, vendor_classification, format]
created: 2026-05-21
---

# Expense & Revenue Data — Canonical Format Rules (Non-Negotiable)

The single source of truth for expense format is `sync/seed_expenses_monthly.py`.
Every monthly sync must replicate that exact structure. Never load raw Xero P&L account names directly.

---

## Section Taxonomy (Every Line Item Must Land in Exactly One)

| Dashboard Section | Bucket Value | What Goes Here |
|---|---|---|
| Sales Salaries | `sales` | Individual rep salaries + sales-specific SaaS (GHL, Stripe fees, Whop) |
| Marketing Salaries | `marketing_salaries` | Individual marketing staff salaries |
| Tech & Tools | `tech_tools` | Revenue-team-relevant software only (see below) |
| Digital Advertising | `advertising` | Ad spend — Facebook/Meta, agency retainers for paid ads |
| Experiments | `experiments` | One-off tests, VA pre-training, new channel pilots |
| Non-Revenue (hidden) | `non_revenue` | Delivery team costs, infra, bank fees, ops software — stored but never shown on P&L |

---

## Section 1 — Cash Collect (Revenue / Income)

Income rows use these exact `product_type` values — no variations:

| product_type | What it represents |
|---|---|
| `high_ticket_upfront` | Full-pay program enrollments |
| `high_ticket_installment` | Installment plan collections |
| `saas` | SaaS / subscription revenue |
| `referral` | Referral partner commissions received |
| `refunds` | Refunds issued (negative amount) |
| `splitit_balance` | Splitit installment balance (negative = liability) |

POST to: `POST /xero/sync-revenue?month=YYYY-MM`

---

## Section 2 — Sales Salaries (`bucket: "sales"`)

### Salary lines
- Break out by **individual person** — one line per rep
- Format: `"vendor": "First Last [Role]"`
- Example: `"vendor": "Ryan Matsumori [AE]"`
- Known sales reps: Ryan Matsumori, Melissa Fredericks, Alex Gessel, Jason Bern, James Caddick, Mathieu Hutin, Ryan McNichol, Princewill Chinedu Ejiogu

### Supporting sales costs (always include when present)
```
GoHighLevel (CRM)                        ← Sales CRM platform
Stripe Collection Fees                   ← notes: "Payment processing on collected revenue"
Stripe Billing Usage Fee                 ← Stripe subscription billing overhead
Whop Fees                                ← notes: "Platform fees on low-ticket / community revenue"
Commissions                              ← When commissions are paid out separately
```

---

## Section 3 — Marketing Salaries (`bucket: "marketing_salaries"`)

- Break out by **individual person** — one line per staff member
- Format: `"vendor": "First Last [Role]"`
- Example: `"vendor": "Lloyd Yip [Head of Marketing]"`
- Known marketing staff: Lloyd Yip, Angel Hernandez, Brooks Golden, Gergo Nagy, Maria Coutiño, Santiago Acevedo, Tatiana Herrera

---

## Section 4 — Tech & Tools (`bucket: "tech_tools"`)

### Revenue-team relevant tools ONLY — include these
Each line must have a bracket explaining what it is:

```
GoHighLevel (CRM)                        ← Sales CRM; also ok under sales bucket
WebinarGeek [Webinar Platform]
Kajabi [Course & Community Platform]
Squarespace / Webflow [Website & Landing Pages]
Calendly [Booking]
JustCall [Sales Calls]
SmartLead [Outbound Email Sequences]
Clay Labs [Lead Enrichment]
Vidalytics [Video Hosting – Sales Pages]
HeyGen [AI Video – Marketing Content]
Canva [Marketing Creative]
Midjourney [AI Creative – Marketing]
Loom [Async Video – Sales & Marketing]
Descript [Video Editing – Content]
Fireflies (50%) [Call Recording – 50% revenue team]     ← Always 50% allocation
ClickUp (50%) [Project Management – 50% revenue team]   ← Always 50% allocation
Zapier [Marketing & Sales Automation]
Twilio [SMS / Communications]
ClickSend [SMS Marketing]
Phantombuster [Lead Scraping – Sales]
Apify [Lead Scraping – Sales]
Trustmary [Testimonials & Social Proof]
People Data Labs [Lead Enrichment Data]
Ampleleads [Lead Data]
Tool – AI [AI tools – revenue team use]
Tool – CyberSecurity [Security tools]
```

### Aggregate sub-groupings (when individual amounts not available from Xero)
If Xero only provides category totals, use these aggregates with notes:
```
"vendor": "Tools – Funnel",      "notes": "Webinar, landing pages, booking"
"vendor": "Tool – Marketing",    "notes": "Includes paid ads mgr retainers (Nick, Juan)"
"vendor": "Tools – Automation",  "notes": "Zapier, Clay, outbound automation"
```

### NOT revenue-team relevant — exclude from dashboard (mark as non_revenue or exclude entirely)
```
Supabase          ← Dev database infrastructure
Render.com        ← Dev hosting infrastructure
Cloudflare        ← Web/dev infrastructure
Atlassian         ← Dev project management
Microsoft         ← General office, not revenue-specific
Lucid Software    ← Diagramming, dev/ops use
WebShare          ← Proxy/scraping, dev use
PostmarkApp       ← Email infrastructure, dev use
APPS.EMTA.EE      ← Estonian tax compliance, ops
Xero              ← Accounting software, ops
BitWarden         ← Password manager, delivery team
Tailscale         ← VPN/infra, delivery team
Make.com          ← General automation, not revenue-specific
LiteMail AI       ← Email warmup, not revenue-specific
```

---

## Section 5 — Digital Advertising (`bucket: "advertising"`)

```
Facebook / Meta Ads    ← Direct ad spend
Tropex Marketing       ← Paid ads management agency
[Agency Name] [Paid Ads Management – Context]
```

---

## Section 6 — Experiments (`bucket: "experiments"`)

One-off tests and pilots that don't fit established categories:
```
VA Pre-Training        ← Sales/marketing VA onboarding costs
Offer Workshop Exp     ← notes: "Classify with Lloyd"
CrowdTamers            ← Growth experiment
```

---

## Bracketing Rule

Any line item that isn't self-explanatory to an auditor must have a bracket or notes field.
Format: `"vendor": "Vendor Name [What it is]"` or `"notes": "Plain English description"`

Required brackets:
- All salary lines → `[Role]`
- All tech tools → `[What it does]`
- Any 50% allocation → `(50%)` in name + `"50% allocated to revenue team"` in notes
- Any ambiguous experiment → notes explaining what it was testing

---

## Currency & Upsert Rules

- All Xero amounts are in **EUR** — multiply by `EUR_USD_RATES[month]` before posting
- April 2026 rate: **1.1706** — rates stored in `EUR_USD_RATES` dict in seed script
- Always use `"replace": true` — wipes the period before inserting (idempotent)
- POST to: `POST /api/dashboard/expenses/upsert`

---

## What Never Goes in the Seed Script

- Raw Xero P&L account names: "Salaries - Sales", "Tool - AI" (Xero uses hyphen, seed uses em dash –)
- Wise (confirmed by accountant: not in Xero P&L — stale data)
- COLUMN NATIONAL ASSOCIATION (Doug, delivery team)
- Any delivery team salary or cost
- Bank fees, Payoneer fees, foreign currency gains/losses, accounting fees → `non_revenue` only
