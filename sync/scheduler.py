"""APScheduler job definitions — 15-min compliance sync + daily/weekly full sync."""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import settings
from db.queries.sync_status import reap_stale_sync_runs
from db.session import AsyncSessionLocal
from sync.appointment_resolver import resolve_appointments
from sync.stripe_sync import sync_stripe_charges
from sync.sync_engine import run_sync
from sync.whop_stats_sync import sync_whop_stats
from sync.whop_refresh import refresh_current_month_payment_metrics
from sync.xero_keepalive import keepalive_xero_token

logger = logging.getLogger(__name__)


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Incremental sync every 6 hours — keeps dashboard data reasonably fresh.
    # Was hourly, but each run makes per-opportunity GHL calls (~35 min, thousands
    # of requests) and 24 runs/day exhausted the 200k/day GHL quota (2026-07-14),
    # blocking manual full syncs entirely. 4 runs/day leaves ample quota headroom.
    # Fixed clock times (not boot-relative interval) so deploys don't re-phase the cadence.
    scheduler.add_job(
        _run_incremental,
        trigger=CronTrigger(hour="1,7,13,19", minute=10, timezone="UTC"),
        id="incremental_sync_6h",
        name="6-hourly incremental GHL sync",
        replace_existing=True,
        misfire_grace_time=600,
        max_instances=1,  # Never overlap — if one is still running, skip the next fire
    )

    # Stale-sync reaper — every 30 min. Heals any sync_run orphaned at 'running' (e.g. a
    # process restart killed a run before it could write its status), so stuck rows can't
    # pile up between deploys. Cheap single UPDATE; safe to run alongside a live sync.
    scheduler.add_job(
        _run_stale_sync_reaper,
        trigger=IntervalTrigger(minutes=30),
        id="stale_sync_reaper_30min",
        name="Reap orphaned 'running' sync runs",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
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

    # Daily EOD Whop refresh — 22:00 UTC (~6pm EST). Re-fetches current-month Whop
    # payments so the Live Whop Revenue P&L section reflects last night's cash.
    scheduler.add_job(
        _run_whop_refresh,
        trigger=CronTrigger(hour=22, minute=0, timezone="UTC"),
        id="daily_whop_refresh",
        name="Daily EOD Whop payment refresh",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
    )

    # Daily Stripe inflow refresh — 21:45 UTC (just before the Whop refresh). Pulls
    # succeeded Stripe charges (incl. GHL sub-account subscriptions + commissions) so
    # the Company dashboard's live Stripe channel reflects last night's card cash.
    scheduler.add_job(
        _run_stripe_refresh,
        trigger=CronTrigger(hour=21, minute=45, timezone="UTC"),
        id="daily_stripe_refresh",
        name="Daily Stripe inflow refresh",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
    )

    # Daily Whop MRR/ARR snapshot — 22:15 UTC (after the Whop payment refresh). Stores
    # one MRR/ARR row per day so the Company dashboard can chart recurring revenue.
    scheduler.add_job(
        _run_whop_stats,
        trigger=CronTrigger(hour=22, minute=15, timezone="UTC"),
        id="daily_whop_stats",
        name="Daily Whop MRR/ARR snapshot",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
    )

    # Weekly Xero token keep-alive — Mondays 06:00 UTC. Xero refresh tokens die after
    # 60 days without use; syncs are manual/monthly, so this rotation keeps the
    # connection alive even when nobody syncs for a while.
    scheduler.add_job(
        _run_xero_keepalive,
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0),
        id="weekly_xero_keepalive",
        name="Weekly Xero refresh-token keep-alive",
        replace_existing=True,
        misfire_grace_time=3600,
        max_instances=1,
    )

    # 6-hourly Retell voice-call sync — offset 30 min after the GHL incremental (:10) so the
    # two don't overlap. Retell is a separate API (no GHL quota impact). Incremental: only
    # pulls calls since the latest stored one. No-op when no Retell API key is configured.
    scheduler.add_job(
        _run_retell_sync,
        trigger=CronTrigger(hour="1,7,13,19", minute=40, timezone="UTC"),
        id="retell_sync_6h",
        name="6-hourly Retell voice-call sync",
        replace_existing=True,
        misfire_grace_time=600,
        max_instances=1,
    )

    # Weekly Appointwise SMS sweep — Saturdays 04:00 UTC. Reaches the full messaged audience
    # (contacts with no opp), which the opportunity-driven sync misses. GHL-heavy, so weekly.
    scheduler.add_job(
        _run_appointwise_sweep,
        trigger=CronTrigger(day_of_week="sat", hour=4, minute=0, timezone="UTC"),
        id="weekly_appointwise_sms_sweep",
        name="Weekly Appointwise SMS sweep",
        replace_existing=True,
        misfire_grace_time=1800,
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
        summary = await resolve_appointments(lookback_days=14)
        logger.info("Scheduler: appointment resolver complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: appointment resolver failed — %s", exc)


async def _run_whop_refresh() -> None:
    logger.info("Scheduler: starting daily EOD Whop payment refresh")
    try:
        stats = await refresh_current_month_payment_metrics()
        logger.info("Scheduler: Whop refresh complete — %s", stats)
    except Exception as exc:
        logger.error("Scheduler: Whop refresh failed — %s", exc)


async def _run_stripe_refresh() -> None:
    logger.info("Scheduler: starting daily Stripe inflow refresh")
    try:
        stats = await sync_stripe_charges()
        logger.info("Scheduler: Stripe inflow refresh complete — %s", stats)
    except Exception as exc:
        logger.error("Scheduler: Stripe inflow refresh failed — %s", exc)


async def _run_whop_stats() -> None:
    logger.info("Scheduler: starting daily Whop MRR/ARR snapshot")
    try:
        stats = await sync_whop_stats()
        logger.info("Scheduler: Whop MRR/ARR snapshot complete — %s", stats)
    except Exception as exc:
        logger.error("Scheduler: Whop MRR/ARR snapshot failed — %s", exc)


async def _run_xero_keepalive() -> None:
    logger.info("Scheduler: starting weekly Xero token keep-alive")
    try:
        result = await keepalive_xero_token()
        logger.info("Scheduler: Xero keep-alive complete — %s", result)
    except Exception as exc:
        logger.error("Scheduler: Xero keep-alive FAILED — token may expire if unused 60 days. "
                     "Reconnect via Settings → Connectors if syncs start failing. Error: %s", exc)


async def _run_stale_sync_reaper() -> None:
    try:
        async with AsyncSessionLocal() as session:
            reaped = await reap_stale_sync_runs(session, settings.sync_stale_reap_minutes)
        if reaped:
            logger.warning("Scheduler: reaped %d stuck 'running' sync run(s) → failed", reaped)
    except Exception as exc:
        logger.error("Scheduler: stale-sync reaper failed — %s", exc)


async def _run_incremental() -> None:
    logger.info("Scheduler: starting daily incremental sync")
    try:
        summary = await run_sync("incremental")
        logger.info("Scheduler: incremental sync complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: incremental sync failed — %s", exc)


async def _run_retell_sync() -> None:
    logger.info("Scheduler: starting 6-hourly Retell voice-call sync (incremental)")
    try:
        from sync.retell_sync import run_retell_sync
        summary = await run_retell_sync(incremental=True)
        logger.info("Scheduler: Retell sync complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: Retell sync failed — %s", exc)


async def _run_appointwise_sweep() -> None:
    logger.info("Scheduler: starting weekly Appointwise SMS sweep")
    try:
        from sync.appointwise_sweep import run_appointwise_sms_sweep
        summary = await run_appointwise_sms_sweep()
        logger.info("Scheduler: Appointwise SMS sweep complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: Appointwise SMS sweep failed — %s", exc)


async def _run_full() -> None:
    logger.info("Scheduler: starting weekly full sync")
    try:
        summary = await run_sync("full")
        logger.info("Scheduler: full sync complete — %s", summary)
    except Exception as exc:
        logger.error("Scheduler: full sync failed — %s", exc)
