"""Deal-Whop match queries — read/write for the deals reconciliation table."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import DealWhopMatch, Opportunity
from sync.ghl_client import DEAL_WON_STAGE_ID


async def get_won_deals(session: AsyncSession) -> list[Opportunity]:
    """Return all closed-won, non-excluded opportunities that have a close_date.

    Used by the matching engine as its input set.
    """
    rows = (await session.execute(
        select(Opportunity)
        .where(Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID)
        .where(Opportunity.is_excluded == False)  # noqa: E712
        .where(Opportunity.close_date.isnot(None))
        .order_by(Opportunity.close_date.desc())
    )).scalars().all()
    return list(rows)


async def get_existing_match(
    session: AsyncSession, ghl_opportunity_id: str
) -> Optional[DealWhopMatch]:
    """Return existing match row for a GHL opportunity, or None."""
    row = (await session.execute(
        select(DealWhopMatch)
        .where(DealWhopMatch.ghl_opportunity_id == ghl_opportunity_id)
    )).scalar_one_or_none()
    return row


async def purge_orphan_matches(session: AsyncSession, won_opportunity_ids: set[str]) -> int:
    """Delete match rows whose opportunity is no longer a closed-won deal.

    Deals merged or deleted in GHL are removed from `opportunities` by the
    full-sync reconcile, but their match rows would otherwise linger — keeping
    their membership claims (blocking the surviving deal from folding those
    payments) and still counting in the Deals-page live view. Returns rows removed.
    """
    from sqlalchemy import delete

    if not won_opportunity_ids:
        return 0
    result = await session.execute(
        delete(DealWhopMatch).where(
            DealWhopMatch.ghl_opportunity_id.notin_(won_opportunity_ids)
        )
    )
    await session.commit()
    return result.rowcount or 0


async def demote_duplicate_match(session: AsyncSession, ghl_opportunity_id: str) -> None:
    """Strip the Whop match (identity + payment metrics) off a row whose membership
    was re-claimed by a better-scoring deal.

    One membership's payment stream can only belong to ONE deal — two GHL won
    deals matched to the same membership means one is a duplicate opportunity.
    The demoted row shows as unmatched with method 'duplicate_membership' so it
    stops counting cash and is visible for GHL cleanup. Never called for
    is_confirmed rows (manual matches always win).
    """
    await session.execute(
        update(DealWhopMatch)
        .where(DealWhopMatch.ghl_opportunity_id == ghl_opportunity_id)
        .values(
            match_confidence="unmatched",
            match_method="duplicate_membership",
            match_score=0,
            whop_membership_id=None,
            whop_email=None,
            whop_name=None,
            whop_product_id=None,
            whop_plan_name=None,
            whop_created_at=None,
            upfront_cash=None,
            total_paid=None,
            payment_count=None,
            total_installments=None,
            is_splitit=None,
            is_claritypay=None,
            provider_fee_pct=None,
            net_cash_collected=None,
            plan_months_flag=None,
            remaining_ar=None,
            is_financing=None,
            first_payment_date=None,
            updated_at=func.now(),
        )
    )
    await session.commit()


async def upsert_deal_match(session: AsyncSession, data: dict) -> None:
    """Insert or update a deal match row.

    Idempotency gate: if an existing row has is_confirmed=True, this
    function does nothing — the manual match is never overwritten.

    Plain English: "upsert" = insert if new, update if exists. The
    is_confirmed gate is the lock — once a human confirms a match,
    the robot can't change it.
    """
    ghl_opp_id = data["ghl_opportunity_id"]

    # Check idempotency gate before touching DB
    existing = await get_existing_match(session, ghl_opp_id)
    if existing and existing.is_confirmed:
        return  # Never overwrite a confirmed manual match

    stmt = (
        pg_insert(DealWhopMatch)
        .values(
            ghl_opportunity_id=ghl_opp_id,
            ghl_close_date=data.get("ghl_close_date"),
            ghl_opportunity_name=data.get("ghl_opportunity_name"),
            ghl_owner_name=data.get("ghl_owner_name"),
            ghl_contact_id=data.get("ghl_contact_id"),
            ghl_contact_email=data.get("ghl_contact_email"),
            ghl_contact_name=data.get("ghl_contact_name"),
            ghl_monetary_value=data.get("ghl_monetary_value"),
            ghl_cash_collected=data.get("ghl_cash_collected"),
            whop_membership_id=data.get("whop_membership_id"),
            whop_email=data.get("whop_email"),
            whop_name=data.get("whop_name"),
            whop_product_id=data.get("whop_product_id"),
            whop_plan_name=data.get("whop_plan_name"),
            whop_created_at=data.get("whop_created_at"),
            match_confidence=data.get("match_confidence", "unmatched"),
            match_score=data.get("match_score", 0.0),
            match_method=data.get("match_method", "none"),
            upfront_cash=data.get("upfront_cash"),
            total_paid=data.get("total_paid"),
            total_contract_value=data.get("total_contract_value"),
            remaining_ar=data.get("remaining_ar"),
            is_financing=data.get("is_financing"),
            payment_count=data.get("payment_count"),
            is_splitit=data.get("is_splitit"),
            first_payment_date=data.get("first_payment_date"),
            total_installments=data.get("total_installments"),
            is_claritypay=data.get("is_claritypay"),
            provider_fee_pct=data.get("provider_fee_pct"),
            net_cash_collected=data.get("net_cash_collected"),
            plan_months_flag=data.get("plan_months_flag"),
            matched_at=func.now(),
            metrics_updated_at=func.now() if data.get("total_paid") is not None else None,
        )
        .on_conflict_do_update(
            index_elements=["ghl_opportunity_id"],
            # Never touch is_confirmed / confirmed_by / confirmed_at — those
            # are manual fields and must survive an auto-match re-run.
            set_={
                "ghl_close_date": data.get("ghl_close_date"),
                "ghl_opportunity_name": data.get("ghl_opportunity_name"),
                "ghl_owner_name": data.get("ghl_owner_name"),
                "ghl_contact_id": data.get("ghl_contact_id"),
                "ghl_contact_email": data.get("ghl_contact_email"),
                "ghl_contact_name": data.get("ghl_contact_name"),
                "ghl_monetary_value": data.get("ghl_monetary_value"),
                "ghl_cash_collected": data.get("ghl_cash_collected"),
                "whop_membership_id": data.get("whop_membership_id"),
                "whop_email": data.get("whop_email"),
                "whop_name": data.get("whop_name"),
                "whop_product_id": data.get("whop_product_id"),
                "whop_plan_name": data.get("whop_plan_name"),
                "whop_created_at": data.get("whop_created_at"),
                "match_confidence": data.get("match_confidence", "unmatched"),
                "match_score": data.get("match_score", 0.0),
                "match_method": data.get("match_method", "none"),
                "upfront_cash": data.get("upfront_cash"),
                "total_paid": data.get("total_paid"),
                "total_contract_value": data.get("total_contract_value"),
                "remaining_ar": data.get("remaining_ar"),
                "is_financing": data.get("is_financing"),
                "payment_count": data.get("payment_count"),
                "is_splitit": data.get("is_splitit"),
                "first_payment_date": data.get("first_payment_date"),
                "total_installments": data.get("total_installments"),
                "is_claritypay": data.get("is_claritypay"),
                "provider_fee_pct": data.get("provider_fee_pct"),
                "net_cash_collected": data.get("net_cash_collected"),
                "plan_months_flag": data.get("plan_months_flag"),
                "matched_at": func.now(),
                "metrics_updated_at": func.now() if data.get("total_paid") is not None else None,
                "updated_at": func.now(),
            },
        )
    )
    await session.execute(stmt)
    await session.commit()


async def enrich_deal_match_payments(
    session: AsyncSession,
    ghl_opportunity_id: str,
    payment_data: dict,
) -> bool:
    """Update ONLY payment metrics on an existing match row.

    Used by Stripe enrichment to fill in missing payment data
    without overwriting match-method or Whop-sourced fields.
    Only updates fields that are currently NULL or zero.

    Returns True if any field was actually updated.
    """
    existing = await get_existing_match(session, ghl_opportunity_id)
    if not existing:
        return False

    updates: dict = {}

    if (existing.upfront_cash is None or float(existing.upfront_cash or 0) == 0) \
            and payment_data.get("upfront_cash"):
        updates["upfront_cash"] = payment_data["upfront_cash"]

    if (existing.total_paid is None or float(existing.total_paid or 0) == 0) \
            and payment_data.get("total_paid"):
        updates["total_paid"] = payment_data["total_paid"]

    if existing.total_contract_value is None and payment_data.get("total_contract_value"):
        updates["total_contract_value"] = payment_data["total_contract_value"]

    if existing.remaining_ar is None and payment_data.get("remaining_ar") is not None:
        updates["remaining_ar"] = payment_data["remaining_ar"]

    if existing.is_financing is None and payment_data.get("is_financing") is not None:
        updates["is_financing"] = payment_data["is_financing"]

    if (existing.payment_count is None or existing.payment_count == 0) \
            and payment_data.get("payment_count"):
        updates["payment_count"] = payment_data["payment_count"]

    if existing.is_splitit is None and payment_data.get("is_splitit") is not None:
        updates["is_splitit"] = payment_data["is_splitit"]

    if existing.first_payment_date is None and payment_data.get("first_payment_date"):
        updates["first_payment_date"] = payment_data["first_payment_date"]

    if existing.total_installments is None and payment_data.get("total_installments"):
        updates["total_installments"] = payment_data["total_installments"]

    if existing.net_cash_collected is None and payment_data.get("net_cash_collected") is not None:
        updates["net_cash_collected"] = payment_data["net_cash_collected"]

    if existing.is_claritypay is None and payment_data.get("is_claritypay") is not None:
        updates["is_claritypay"] = payment_data["is_claritypay"]

    if existing.provider_fee_pct is None and payment_data.get("provider_fee_pct") is not None:
        updates["provider_fee_pct"] = payment_data["provider_fee_pct"]

    if existing.plan_months_flag is None and payment_data.get("plan_months_flag") is not None:
        updates["plan_months_flag"] = payment_data["plan_months_flag"]

    if not updates:
        return False

    updates["metrics_updated_at"] = func.now()
    updates["updated_at"] = func.now()

    stmt = (
        update(DealWhopMatch)
        .where(DealWhopMatch.ghl_opportunity_id == ghl_opportunity_id)
        .values(**updates)
    )
    await session.execute(stmt)
    await session.commit()
    return True


async def get_deal_matches(
    session: AsyncSession,
    month_start: Optional[date] = None,
    month_end: Optional[date] = None,
    owner_name: Optional[str] = None,
    confidence: Optional[str] = None,
) -> list[dict]:
    """Return deal match rows, optionally filtered by close date range, rep, or confidence.

    Returns list of dicts ready for JSON serialisation.
    """
    # Sort by first_payment_date (authoritative Whop date) when available,
    # fall back to ghl_close_date for unmatched deals.
    from sqlalchemy import func as sa_func
    query = select(DealWhopMatch).order_by(
        sa_func.coalesce(DealWhopMatch.first_payment_date, DealWhopMatch.ghl_close_date).desc().nullslast()
    )

    if month_start:
        query = query.where(DealWhopMatch.ghl_close_date >= month_start)
    if month_end:
        query = query.where(DealWhopMatch.ghl_close_date <= month_end)
    if owner_name:
        query = query.where(DealWhopMatch.ghl_owner_name == owner_name)
    if confidence:
        query = query.where(DealWhopMatch.match_confidence == confidence)

    rows = (await session.execute(query)).scalars().all()

    def _projected(r) -> Optional[float]:
        """Python mirror of common.whop_projected_total_expr() — payment-verified
        projected full contract for one matched deal (keeps the Deals page in step
        with the Projected Total shown on the dashboard/debug pages)."""
        if not r.total_paid:
            return None
        paid = float(r.total_paid)
        if r.is_splitit or r.is_claritypay:
            return paid  # financed → settles 100% upfront
        if r.total_installments and r.total_installments > 0 and r.payment_count and r.payment_count > 0:
            return paid / r.payment_count * r.total_installments
        return paid  # pay-in-full or plan length unknown

    return [
        {
            "ghl_opportunity_id": r.ghl_opportunity_id,
            "ghl_close_date": str(r.ghl_close_date) if r.ghl_close_date else None,
            "ghl_opportunity_name": r.ghl_opportunity_name,
            "ghl_owner_name": r.ghl_owner_name,
            "ghl_contact_email": r.ghl_contact_email,
            "ghl_contact_name": r.ghl_contact_name,
            "ghl_monetary_value": float(r.ghl_monetary_value) if r.ghl_monetary_value else None,
            "ghl_cash_collected": float(r.ghl_cash_collected) if r.ghl_cash_collected else None,
            "whop_membership_id": r.whop_membership_id,
            "whop_email": r.whop_email,
            "whop_name": r.whop_name,
            "whop_product_id": r.whop_product_id,
            "whop_plan_name": r.whop_plan_name,
            "match_confidence": r.match_confidence,
            "match_score": float(r.match_score) if r.match_score else 0.0,
            "match_method": r.match_method,
            "is_confirmed": r.is_confirmed,
            "confirmed_by": r.confirmed_by,
            "upfront_cash": float(r.upfront_cash) if r.upfront_cash else None,
            "total_paid": float(r.total_paid) if r.total_paid else None,
            "total_contract_value": float(r.total_contract_value) if r.total_contract_value else None,
            "remaining_ar": float(r.remaining_ar) if r.remaining_ar else None,
            "is_financing": r.is_financing,
            "payment_count": r.payment_count,
            "is_splitit": r.is_splitit,
            "is_claritypay": r.is_claritypay,
            "provider_fee_pct": float(r.provider_fee_pct) if r.provider_fee_pct else None,
            "net_cash_collected": float(r.net_cash_collected) if r.net_cash_collected else None,
            "plan_months_flag": r.plan_months_flag,
            "whop_projected": _projected(r),
            "first_payment_date": str(r.first_payment_date) if r.first_payment_date else None,
            "total_installments": r.total_installments,
            "matched_at": r.matched_at.isoformat() if r.matched_at else None,
        }
        for r in rows
    ]


async def get_deal_match_summary(session: AsyncSession) -> dict:
    """Aggregate stats for the deals page header cards."""
    rows = (await session.execute(select(DealWhopMatch))).scalars().all()

    total = len(rows)
    by_confidence: dict[str, int] = {"high": 0, "medium": 0, "low": 0, "unmatched": 0}
    total_contract = 0.0
    total_paid_sum = 0.0
    total_ar = 0.0

    for r in rows:
        conf = r.match_confidence or "unmatched"
        by_confidence[conf] = by_confidence.get(conf, 0) + 1
        if r.total_contract_value:
            total_contract += float(r.total_contract_value)
        if r.total_paid:
            total_paid_sum += float(r.total_paid)
        if r.remaining_ar and r.remaining_ar > 0:
            total_ar += float(r.remaining_ar)

    matched = by_confidence["high"] + by_confidence["medium"]
    match_rate = round(matched / total * 100, 1) if total else 0.0

    return {
        "total_deals": total,
        "matched_high": by_confidence["high"],
        "matched_medium": by_confidence["medium"],
        "matched_low": by_confidence["low"],
        "unmatched": by_confidence["unmatched"],
        "match_rate_pct": match_rate,
        "total_contract_value": round(total_contract, 2),
        "total_paid": round(total_paid_sum, 2),
        "total_remaining_ar": round(total_ar, 2),
    }


async def get_last_match_run(session: AsyncSession) -> Optional[str]:
    """Return the most recent matched_at timestamp across all rows."""
    result = await session.execute(
        select(func.max(DealWhopMatch.matched_at))
    )
    ts = result.scalar_one_or_none()
    return ts.isoformat() if ts else None
