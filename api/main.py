"""FastAPI application entry point."""
# noqa: force-redeploy v2

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, text

from api.routers import metrics, sync as sync_router
from api.routers import connectors as connectors_router
from api.routers import dashboard as dashboard_router
from api.routers import rep_settings as rep_settings_router
from api.routers import whop_live as whop_live_router
from api.routers import xero_auth as xero_auth_router
from api.routers import xero_expenses as xero_expenses_router
from api.routers import xero_invoices as xero_invoices_router
from api.schemas.responses import HealthResponse
from config import settings
from db.advisory_lock import try_acquire_scheduler_leadership
from db.models import SyncRun
from db.queries.sync_status import check_db_health
from db.session import AsyncSessionLocal, engine
from sync.scheduler import create_scheduler
from sync.sync_engine import cancel_active_syncs, run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_scheduler = None

# --- Auth ---

_bearer_scheme = HTTPBearer(auto_error=False)


async def verify_token(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> None:
    """Validate the static bearer token on every protected request."""
    if credentials is None or credentials.credentials != settings.api_bearer_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


# --- App lifecycle ---

async def _start_leader() -> None:
    """Leader-only startup: heal orphaned runs, resume a checkpointed one, start the
    scheduler, and kick off the first-ever full sync."""
    global _scheduler

    # This process is now the SOLE scheduler, so any sync_run still 'running' must be
    # orphaned by a previous process (a killed deploy container, a crash). Heal them so
    # a young stuck row can't wedge the concurrency guard — EXCEPT a resumable one (has
    # a checkpoint), which we continue from where it left off instead of failing.
    resumable = None
    try:
        async with AsyncSessionLocal() as session:
            resumable = (await session.execute(text("""
                SELECT id FROM sync_runs
                WHERE status = 'running' AND checkpoint IS NOT NULL
                ORDER BY started_at DESC LIMIT 1
            """))).scalar_one_or_none()
            healed = (await session.execute(text("""
                UPDATE sync_runs
                SET status = 'failed', completed_at = now(),
                    error_details = CAST(:d AS jsonb), checkpoint = NULL
                WHERE status = 'running'
                  AND (CAST(:keep AS text) IS NULL OR id != CAST(:keep AS uuid))
            """), {
                "d": json.dumps([{"error": "orphaned by a restart — healed by the new scheduler process", "fatal": True}]),
                "keep": str(resumable) if resumable else None,
            })).rowcount
            await session.commit()
        if healed:
            logger.warning("Startup: healed %d orphaned 'running' sync run(s) → failed", healed)
    except Exception as exc:
        logger.error("Startup: orphan-sync heal failed — %s", exc)

    if resumable:
        logger.info("Startup: resuming orphaned sync run %s from its checkpoint", resumable)
        asyncio.create_task(run_sync(resume_run_id=str(resumable)))

    _scheduler = create_scheduler()
    _scheduler.start()
    logger.info("Scheduler started (leader) — 6-hourly incremental sync, daily appointment resolver, weekly full sync")

    # First-ever startup: no syncs yet → kick off the initial full sync.
    try:
        async with AsyncSessionLocal() as session:
            has_any = (await session.execute(select(SyncRun).limit(1))).scalar_one_or_none()
        if has_any is None:
            logger.info("No previous sync found — triggering initial full sync")
            asyncio.create_task(run_sync("full"))
    except Exception as exc:
        logger.error("Startup: initial-sync check failed — %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    app.state.leader_conn = None

    # ── Single-scheduler election ───────────────────────────────────────────────
    # Multiple app processes can coexist — a Railway deploy overlaps old+new, a
    # container can outlive its deploy, or the app may run on a second host/replica.
    # If each ran its own scheduler they'd each fire the incremental sync on its own
    # boot schedule: the root cause of the duplicate hourly syncs. A Postgres advisory
    # lock elects ONE scheduler process. NOTE: this is best-effort (a transaction-pooled
    # connection can't hold a session lock); the atomic guard inside run_sync is the
    # real protection against overlapping syncs, so leadership never needs to be perfect.
    try:
        app.state.leader_conn = await try_acquire_scheduler_leadership()
    except Exception as exc:
        logger.error("Startup: scheduler-leadership check failed — %s", exc)
        app.state.leader_conn = None

    if app.state.leader_conn is not None:
        await _start_leader()
    else:
        logger.warning("Startup: another process holds scheduler leadership — this process serves HTTP only (no scheduler)")

    yield

    # ── Graceful shutdown ───────────────────────────────────────────────────────
    # Stop scheduling, then cancel any in-flight sync so THIS process exits promptly
    # instead of lingering as a zombie that keeps firing its scheduler. A cancelled run
    # keeps its 'running' row + checkpoint; the next leader resumes it on boot.
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        except Exception as exc:
            logger.error("Shutdown: scheduler stop failed — %s", exc)
    try:
        n = await cancel_active_syncs()
        if n:
            logger.info("Shutdown: cancelled %d in-flight sync task(s)", n)
    except Exception as exc:
        logger.error("Shutdown: cancelling in-flight syncs failed — %s", exc)
    if app.state.leader_conn is not None:
        try:
            await app.state.leader_conn.close()  # releases the scheduler advisory lock
        except Exception as exc:
            logger.error("Shutdown: releasing scheduler leadership failed — %s", exc)
    await engine.dispose()
    logger.info("Application shutdown complete")


# --- App ---

app = FastAPI(
    title="QS Analytics Dashboard API",
    description="Internal analytics API for Quantum Scaling sales performance data.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Protected routers — all /api/metrics/* and /api/sync/* require bearer token
app.include_router(metrics.router, dependencies=[Depends(verify_token)])
app.include_router(sync_router.router, dependencies=[Depends(verify_token)])

# Dashboard router — no auth, browser-facing read-only analytics
app.include_router(dashboard_router.router)

# Live Whop Revenue router — GET /api/dashboard/pnl/whop-live, no auth (browser-facing)
app.include_router(whop_live_router.router)

# Rep comp settings — GET/PUT /api/dashboard/rep-settings, no auth (browser-facing,
# same convention as the dashboard router's period-input save endpoints)
app.include_router(rep_settings_router.router)

# Xero OAuth router — /xero/auth and /xero/callback are public (OAuth flow requires it)
# /xero/sync-revenue is protected (bearer token checked inside the router itself)
app.include_router(xero_auth_router.router)

# Xero invoice sync — POST /xero/sync-invoices, bearer-protected via verify_bearer dependency
app.include_router(xero_invoices_router.router)

# Xero expense sync — POST /xero/sync-expenses, bearer-protected via verify_bearer dependency
app.include_router(xero_expenses_router.router)

# Settings → Connectors — /api/settings/connectors/*, bearer-protected (secrets live here)
app.include_router(connectors_router.router, dependencies=[Depends(verify_token)])

# Serve dashboard.html at root
_STATIC_DIR = Path(__file__).parent.parent / "static"
_templates = Jinja2Templates(directory=str(_STATIC_DIR))

@app.get("/", include_in_schema=False)
@app.get("/dashboard", include_in_schema=False)
async def serve_dashboard():
    return FileResponse(
        _STATIC_DIR / "dashboard.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/debug", include_in_schema=False)
async def serve_debug():
    return FileResponse(
        _STATIC_DIR / "debug.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/data-quality", include_in_schema=False)
async def serve_data_quality():
    return FileResponse(
        _STATIC_DIR / "data-quality.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/sync-history", include_in_schema=False)
async def serve_sync_history(request: Request):
    return _templates.TemplateResponse(
        "sync-history.html",
        {"request": request, "api_token": settings.api_bearer_token},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/channels/slwa", include_in_schema=False)
async def serve_slwa_dashboard():
    return FileResponse(
        _STATIC_DIR / "slwa-dashboard.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/expenses", include_in_schema=False)
async def serve_expenses():
    return FileResponse(
        _STATIC_DIR / "expenses.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/pnl", include_in_schema=False)
async def serve_pnl(request: Request):
    return _templates.TemplateResponse(
        "pnl.html",
        {"request": request, "api_token": settings.api_bearer_token},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/deals", include_in_schema=False)
async def serve_deals(request: Request):
    return _templates.TemplateResponse(
        "deals.html",
        {"request": request, "api_token": settings.api_bearer_token},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/settings", include_in_schema=False)
async def serve_settings(request: Request):
    return _templates.TemplateResponse(
        "settings.html",
        {"request": request, "api_token": settings.api_bearer_token},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


# Serve static assets (favicon, CSS, JS) — must come after named routes
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

# Health check is exempt from auth — Railway uses it without credentials
@app.get("/api/health", response_model=HealthResponse, tags=["health"])
async def health():
    """Health check — confirms DB connection and returns last sync timestamp."""
    async with AsyncSessionLocal() as session:
        db_ok, last_sync_at = await check_db_health(session)

    return HealthResponse(
        status="ok" if db_ok else "degraded",
        db_connected=db_ok,
        last_sync_at=last_sync_at,
    )
