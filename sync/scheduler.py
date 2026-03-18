"""APScheduler job definitions — daily incremental + weekly full sync."""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import settings
from sync.sync_engine import run_sync

logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Daily incremental sync at configured hour (default 2 AM UTC)
    scheduler.add_job(
        _run_incremental,
        trigger=CronTrigger(
            hour=settings.daily_sync_hour,
            minute=settings.daily_sync_minute,
        ),
        id="daily_incremental_sync",
        name="Daily incremental GHL sync",
        replace_existing=True,
        misfire_grace_time=300,  # 5-minute grace if server was down
    )

    # Weekly full sync on Sundays at same hour
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
    )

    return scheduler


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
