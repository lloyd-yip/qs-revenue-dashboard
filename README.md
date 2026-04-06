# QS Revenue Dashboard

Internal analytics dashboard for **Quantum Scaling** sales performance — built with FastAPI, SQLAlchemy (async), and vanilla HTML/JS frontends. Data is synced from GoHighLevel (GHL) and enriched with Fireflies.ai transcripts.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│  GHL API     │────▶│  Sync Engine │────▶│  PostgreSQL   │
│  Fireflies   │     │  (scheduler) │     │  (Supabase)   │
└──────────────┘     └──────────────┘     └──────┬────────┘
                                                  │
                                           ┌──────▼────────┐
                                           │  FastAPI       │
                                           │  REST API      │
                                           └──────┬────────┘
                                                  │
                                           ┌──────▼────────┐
                                           │  Static HTML   │
                                           │  Dashboards    │
                                           └───────────────┘
```

### Key components

| Directory       | Purpose |
|-----------------|---------|
| `api/`          | FastAPI app, routers (`dashboard`, `metrics`, `sync`), Pydantic schemas |
| `db/`           | SQLAlchemy models, async session, query modules |
| `sync/`         | GHL client, Fireflies client, sync engine, appointment resolver, scheduler |
| `migrations/`   | Alembic migration scripts |
| `static/`       | Browser-facing HTML pages (dashboard, debug drilldown, data quality) |
| `tests/`        | Integration / critical-path tests |
| `config.py`     | Pydantic settings (loads `.env`) |

### Frontend pages

| Route            | File                     | Description |
|------------------|--------------------------|-------------|
| `/`              | `static/dashboard.html`  | Main analytics dashboard — KPIs, charts, rep breakdown, compliance |
| `/debug`         | `static/debug.html`      | Debug drilldown — per-opportunity detail for any KPI metric |
| `/data-quality`  | `static/data-quality.html` | Data quality audit — anomaly detection across opportunities |

---

## Prerequisites

- **Python 3.11+**
- **PostgreSQL** (or a Supabase managed database)
- A **GHL API key** with access to the target location & pipeline
- A **Fireflies API key** (for appointment resolution)

---

## Local Development Setup

### 1. Clone the repo

```bash
git clone git@github.com:lloyd-yip/qs-revenue-dashboard.git
cd qs-revenue-dashboard
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable            | Required | Description |
|---------------------|----------|-------------|
| `DATABASE_URL`      | ✅       | PostgreSQL connection string using `postgresql+asyncpg://` driver |
| `API_BEARER_TOKEN`  | ✅       | Static bearer token for protected API endpoints |
| `GHL_API_KEY`       | ✅       | GoHighLevel API key (starts with `pit-`) |
| `GHL_LOCATION_ID`   | ✅       | GHL location ID (default: `G7ZOWCq78JrzUjlLMCxt`) |
| `GHL_PIPELINE_ID`   | ✅       | GHL pipeline ID (default: `zbI8YxmB9qhk1h4cInnq`) |
| `FIREFLIES_API_KEY` | ✅       | Fireflies.ai API key |
| `GHL_PAGE_DELAY_MS` | —        | Delay between paginated GHL calls in ms (default: `150`) |
| `GHL_PAGE_SIZE`     | —        | GHL page size (default: `100`) |
| `DAILY_SYNC_HOUR`   | —        | UTC hour for daily sync (default: `2`) |
| `DAILY_SYNC_MINUTE` | —        | Minute past the hour (default: `0`) |
| `FULL_SYNC_DAY_OF_WEEK` | —   | Day for weekly full sync (default: `sun`) |

> **Tip:** Generate a secure bearer token with:
> ```bash
> python3 -c "import secrets; print(secrets.token_urlsafe(32))"
> ```

### 5. Run database migrations

Make sure `DATABASE_URL` is set, then:

```bash
alembic upgrade head
```

> **Note:** Alembic uses `psycopg2` (sync driver) for migrations. The `DATABASE_URL` in `.env` uses `asyncpg`, and `migrations/env.py` automatically swaps the driver for migration commands.

### 6. Start the dev server

```bash
uvicorn api.main:app --reload --port 8000
```

The app will be available at **http://localhost:8000**

- Dashboard: http://localhost:8000/
- Debug drilldown: http://localhost:8000/debug
- Data quality: http://localhost:8000/data-quality
- API docs (Swagger): http://localhost:8000/docs

On first startup, if no previous sync exists, the app automatically triggers a full GHL sync in the background.

---

## Sync & Scheduler

The app runs an APScheduler with three jobs:

| Job | Schedule | Description |
|-----|----------|-------------|
| Incremental sync | Every 15 min | Re-fetches recently modified GHL opportunities |
| Appointment resolver | Daily 11 PM UTC | Matches Fireflies transcripts to auto-flip `call1_appointment_status` |
| Full sync | Weekly (Sunday 2 AM UTC) | Full re-sync of all GHL pipeline opportunities |

### Manual sync via API

```bash
# Trigger incremental sync
curl -X POST http://localhost:8000/api/sync/trigger \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"mode": "incremental"}'

# Trigger full sync
curl -X POST http://localhost:8000/api/sync/trigger \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"mode": "full"}'
```

---

## Database Migrations

This project uses **Alembic** for schema migrations.

```bash
# Apply all pending migrations
alembic upgrade head

# Create a new migration after changing models
alembic revision --autogenerate -m "description_of_change"

# Downgrade one step
alembic downgrade -1

# Check current revision
alembic current
```

---

## API Reference

All `/api/metrics/*` and `/api/sync/*` endpoints require a `Bearer` token. Dashboard endpoints (`/api/dashboard/*`) are unauthenticated (browser-facing, read-only).

| Endpoint | Auth | Description |
|----------|------|-------------|
| `GET /api/health` | — | Health check (Railway uses this) |
| `GET /api/dashboard/summary` | — | KPI summary (booked, shows, rates, closes) |
| `GET /api/dashboard/time-series` | — | Show rate over time |
| `GET /api/dashboard/by-rep` | — | Per-rep breakdown |
| `GET /api/dashboard/reps` | — | Rep list for filter dropdown |
| `GET /api/dashboard/daily-activity` | — | 7-day activity table |
| `GET /api/dashboard/compliance` | — | Compliance summary & failures |
| `GET /api/dashboard/drilldown` | — | Debug drilldown data |
| `GET /api/dashboard/data-quality` | — | Data quality issues |
| `GET /api/dashboard/followup-by-quality` | — | Follow-up show rate by lead quality |
| `POST /api/sync/trigger` | 🔒 | Trigger manual sync |
| `GET /api/metrics/summary` | 🔒 | Same as dashboard summary (authenticated) |
| `GET /api/metrics/by-rep` | 🔒 | Same as dashboard by-rep (authenticated) |

Full interactive docs at `/docs` (Swagger UI).

---

## Deployment (Railway)

The app is deployed on **Railway** using Nixpacks.

**`railway.toml`** configures:
- **Build:** Nixpacks (auto-detects Python)
- **Start command:** `alembic upgrade head && uvicorn api.main:app --host 0.0.0.0 --port $PORT`
- **Health check:** `GET /api/health` (120s timeout)
- **Restart policy:** On failure (max 3 retries)

### Environment variables on Railway

Set the same variables from `.env.example` in Railway's **Variables** tab. Railway injects `PORT` automatically.

### Deploy workflow

1. Push to `main` → Railway auto-deploys
2. Alembic runs migrations on startup
3. Uvicorn starts serving the app
4. Railway runs a health check at `/api/health`

---

## Project Structure

```
qs-revenue-dashboard/
├── api/
│   ├── main.py                  # FastAPI app, lifespan, auth, static routes
│   ├── routers/
│   │   ├── dashboard.py         # Unauthenticated dashboard API endpoints
│   │   ├── metrics.py           # Authenticated metrics API endpoints
│   │   └── sync.py              # Sync trigger & status endpoints
│   └── schemas/
│       └── responses.py         # Pydantic response models
├── db/
│   ├── models.py                # SQLAlchemy ORM models (Opportunity, SyncRun, etc.)
│   ├── session.py               # Async engine & session factory
│   └── queries/                 # Query modules (one per domain)
│       ├── common.py            # Shared filters & helpers
│       ├── compliance.py        # Compliance queries
│       ├── data_quality.py      # Data quality anomaly detection
│       ├── debug_drilldown.py   # Per-opportunity debug data
│       ├── followup_quality.py  # Follow-up show rate by lead quality
│       ├── insights.py          # Insight / analysis queries
│       ├── lead_source.py       # Lead source & channel breakdown
│       ├── metrics_by_rep.py    # Rep breakdown, daily activity, closes
│       ├── metrics_summary.py   # Top-level KPI summary
│       ├── reps.py              # Rep list (for dropdown)
│       ├── sync_status.py       # DB health & last sync timestamp
│       └── time_series.py       # Time series data
├── sync/
│   ├── ghl_client.py            # GoHighLevel API client
│   ├── fireflies_client.py      # Fireflies.ai GraphQL client
│   ├── sync_engine.py           # Full & incremental sync logic
│   ├── normalizer.py            # Raw GHL data → ORM model mapping
│   ├── appointment_resolver.py  # Auto-flip appointment status via transcripts
│   └── scheduler.py             # APScheduler job definitions
├── migrations/
│   ├── env.py                   # Alembic environment config
│   └── versions/                # Migration scripts
├── static/
│   ├── dashboard.html           # Main dashboard UI
│   ├── debug.html               # Debug drilldown UI
│   └── data-quality.html        # Data quality audit UI
├── tests/
│   └── test_critical_paths.py   # Integration tests
├── config.py                    # Pydantic settings (loads .env)
├── alembic.ini                  # Alembic configuration
├── railway.toml                 # Railway deployment config
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variable template
└── .gitignore
```
