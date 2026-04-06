"""FastAPI application entry point."""
# noqa: force-redeploy v2

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select

from api.routers import metrics, sync as sync_router
from api.routers import dashboard as dashboard_router
from api.schemas.responses import HealthResponse
from config import settings
from db.models import SyncRun
from db.queries.sync_status import check_db_health
from db.session import AsyncSessionLocal, engine
from sync.scheduler import create_scheduler
from sync.sync_engine import run_sync

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler

    _scheduler = create_scheduler()
    _scheduler.start()
    logger.info("Scheduler started — hourly incremental sync, daily appointment resolver, full sync on Sundays")

    # Trigger initial full sync if this is the first startup
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(SyncRun).limit(1))
        if result.scalar_one_or_none() is None:
            logger.info("No previous sync found — triggering initial full sync")
            asyncio.create_task(run_sync("full"))

    yield

    if _scheduler:
        _scheduler.shutdown(wait=False)
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

# Serve dashboard.html at root
_STATIC_DIR = Path(__file__).parent.parent / "static"

@app.get("/", include_in_schema=False)
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
async def serve_sync_history():
    return FileResponse(
        _STATIC_DIR / "sync-history.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


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
