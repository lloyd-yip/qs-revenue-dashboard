# QS Revenue Dashboard — Claude Code Context

## What This Project Is

Sales performance analytics dashboard for Quantum Scaling. Syncs deal data from GHL,
matches against Stripe payments and Xero invoices, surfaces rep-level metrics,
funnel economics, and weekly/monthly revenue reports.

**Tech stack:** Python / FastAPI
**Database:** PostgreSQL via SQLAlchemy + psycopg2
**Auth:** Bearer token
**Has Stripe:** Yes — read-only charge/customer lookups (NO webhooks, no write operations)
**Has AI endpoints:** No

## Project State File

All phase tracking, decisions, milestones, and resume points live at:
`project-control/project_state.json`

Read this file at the start of every session. Update on confirmed decisions and milestones.

## Shared Repo — CRITICAL

Geri (external developer) pushes directly to `lloyd-yip/qs-revenue-dashboard`.
**NEVER force-push to the qs-dashboard remote.**
Always `git fetch qs-dashboard` before pushing. If rejected, fetch and merge first.

---

## ARCHITECTURE RULES — NON-NEGOTIABLE

### Module Boundaries — Already Well-Structured ✅

This project has the correct directory structure. Maintain it:

```
/api/routers/   → HTTP handlers ONLY. No direct DB calls. Call services or db/queries/ directly.
/db/queries/    → All database queries. Functions return plain Python values.
/db/models.py   → SQLAlchemy ORM models. No logic.
/sync/          → Data synchronisation pipelines (GHL, Stripe, Xero, Wise). Not web handlers.
/api/schemas/   → Pydantic request/response schemas.
```

**The real violation here is file SIZE, not structure.** The boundaries are correct — files are too large.

### File Size Limits

Known oversized files — fix as you touch them:

🔴 Critical (extract when next in these files):
- `api/routers/xero_auth.py` — 1,107 lines (split OAuth flow from token management)
- `api/routers/dashboard.py` — 1,025 lines (split by metric domain: funnel, rep, revenue)
- `sync/match_deals_whop.py` — 950 lines (extract matching logic to services/)

🟡 High (reduce when working nearby):
- `sync/sync_engine.py` — 454 lines
- `db/models.py` — 445 lines (split by domain)
- `api/schemas/responses.py` — 441 lines (split by response category)

Note: `db/queries/*.py` files can legitimately be longer — complex SQL is unavoidable.
The limit applies more strictly to route handlers and sync logic.

### DRY Enforcement

The sync pipeline and report generation have duplicated metric calculation logic.
Before writing any new metric or aggregation, grep db/queries/ first.

---

## SECURITY REQUIREMENTS — NON-NEGOTIABLE

### Authentication
- All endpoints use bearer token auth. Maintain this on every new endpoint.

### Stripe
- Stripe usage is READ-ONLY (charge/customer lookups only).
- No Stripe webhook handler exists and none should be added without signature verification.
- Stripe secret key is in environment variables only.

### Secrets
- Stripe, Xero, GHL, Wise credentials all in environment variables.
- Never hardcode any API key or OAuth secret.

---

## TECH STACK SPECIFICS

- Async FastAPI throughout.
- SQLAlchemy 2.0 style (select() not query()).
- Alembic for migrations — always write reversible migrations.
- Pydantic v2 for all schemas.

---

## CLEAN-AS-YOU-GO RULE

When editing any of the oversized files above: extract the largest coherent block before
adding new code. One extraction per session per file. `dashboard.py` and `xero_auth.py`
are the priority — they are the most actively edited and most dangerous to keep growing.

---

## VERIFICATION CHECKLIST (before declaring any feature done)

- [ ] No new file exceeds 300 lines
- [ ] All new endpoints have bearer auth
- [ ] No hardcoded credentials
- [ ] New Alembic migrations are reversible
- [ ] No force-push to qs-dashboard remote
