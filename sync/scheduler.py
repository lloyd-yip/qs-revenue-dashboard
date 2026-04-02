"""APScheduler job definitions — 15-min compliance sync + daily/weekly full sync."""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import settings
from sync.appointment_resolver import resolve_appointments
from sync.sync_engine import run_sync

logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")

    # 15-minute compliance sync — keeps compliance board fresh for reps.
    # Only re-fetches GHL opps modified in the last ~16 minutes, so it's fast and cheap.
    scheduler.add_job(
        _run_incremental,
        trigger=IntervalTrigger(minutes=15),
        id="compliance_sync_15min",
        name="15-min incremental GHL sync (compliance freshness)",
        replace_existing=True,
        misfire_grace_time=60,
        max_instances=1,  # Never overlap — if one is still running, skip the next fire
    )

    # Daily appointment resolver — 7pm EST (midnight UTC) — auto-flips call1_appointment_status
    # via Fireflies transcript matching. Runs after business hours so all transcripts are ready.
    scheduler.add_job(
        _run_appointment_resolver,
        trigger=CronTrigger(hour=23, minute=0, timezone="UTC"),  # 7pm EST / 8pm EDT
        id="daily_appointment_resolver",
        name="Daily Fireflies appointment resolver",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
    )

    # Weekly full sync on Sundays at configured hour — catches any data GHL didn't surface incrementally
    scheduler.add_job(
        _run_full,
        trigger=CronTrigger(
            day_of_week=settings.full_sync_day_of_week,
            hour=settings.daily_sync_hour,
            minute=settings.daily_sync_minute,
        ),
        id="weekly_full_sync",
        name="Weekly full GHL sync",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
    )

    return scheduler


async def _run_appointment_resolver() -> None:
    logger.info("Scheduler: starting daily appointment resolver")
    try:
        summary = await resolve_appointments(lookback_days=3)
        logger.info("Scheduler: appointment resolver complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: appointment resolver failed — %s", exc)


async def _run_incremental() -> None:
    logger.info("Scheduler: starting daily incremental sync")
    try:
        summary = await run_sync("incremental")
        logger.info("Scheduler: incremental sync complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: incremental sync failed — %s", exc)


async def _run_full() -> None:
    logger.info("Scheduler: starting weekly full sync")
    try:
        summary = await run_sync("full")
        logger.info("Scheduler: full sync complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: full sync failed — %s", exc)
