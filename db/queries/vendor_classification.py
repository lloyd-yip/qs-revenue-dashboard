"""Vendor → bucket auto-classification for expense line items.

When expenses are upserted, each vendor name is looked up here. If found,
the mapped bucket overrides whatever the caller sent. Unknown vendors pass
through unchanged.

To add a vendor:    add one line to the correct _register() block.
To reclassify:      move the line to a different block.
To hide from P&L:   move to the non_revenue block.
"""

import re

# ── Internals ────────────────────────────────────────────────────────────────

VENDOR_BUCKET_MAP: dict[str, str] = {}


def _normalise_vendor(name: str) -> str:
    """Lowercase, collapse whitespace, strip — for case-insensitive matching."""
    return re.sub(r"\s+", " ", name.strip().lower())


def _register(bucket: str, vendors: list[str]) -> None:
    for v in vendors:
        VENDOR_BUCKET_MAP[_normalise_vendor(v)] = bucket


# ── Sales ────────────────────────────────────────────────────────────────────
_register("sales", [
    "Alexander Gessel",
    "James Caddick",
    "Jason Bern",
    "Mathieu Hutin",
    "Melissa Fredericks",
    "Princewill Chinedu Ejiogu",
    "Ryan Matsumori",
    "Ryan McNichol",
])

# ── Marketing Salaries ───────────────────────────────────────────────────────
_register("marketing_salaries", [
    "Angel Hernandez",
    "Brooks Golden",
    "Gergo Nagy",
    "Lloyd Yip",
    "Maria Coutiño",
    "Santiago Acevedo",
    "Tatiana Herrera",
])

# ── Tech & Tools ─────────────────────────────────────────────────────────────
_register("tech_tools", [
    "APIFY",
    "APPOINTWISE.IO",
    "ATLASSIAN",
    "Amazon",
    "Ampleleads",
    "Anthropic Ireland",
    "Attractscal",
    "CLAUDE.AI",
    "CLAY LABS INC",
    "CLICKSEND",
    "CLICKUP (50%)",
    "CLIENTACQUISITIONIO",
    "COLUMN NATIONAL ASSOCIATION",
    "Calendly",
    "Canva",
    "Cheapcom",
    "Cloudflare",
    "DESCRIPT",
    "DSC",
    "F2abpp",
    "FINDYLEAD.COM",
    "FIREFLIES (50%)",
    "Finding Freedom",
    "Fiver",
    "Flexxbuy",
    "Go High Level",
    "Gojiberry Ai",
    "Google",
    "HEYGEN TECHNOLOGY",
    "Highlevel Inc",
    "ILOVEPDF.COM",
    "JUSTCALL.IO",
    "Kajabi",
    "Kodeful LLC",
    "LEADWAVE",
    "LIGHTXEDITOR",
    "LOOM",
    "Lemsqzy* Corsproxy",
    "Lucid Software",
    "MIDJOURNEY",
    "MILLION VERIFIER",
    "MINDVALLEY",
    "Madhu Raj",
    "Manus Ai",
    "Md Toukir Ahmed",
    "Microsoft",
    "Name-Cheap.com",
    "Nicholas Goulart",
    "Numlookupapi.com",
    "OPENAI",
    "POLYMERSEARCH",
    "POSTMARKAPP.COM",
    "Payouts",
    "People Data Labs",
    "Phantombuster",
    "Pinnacleent",
    "RENDER.COM",
    "ROW ZERO",
    "Render",
    "SIGNOW",
    "SLIGHTEDGE PARTNERS",
    "SMARTLEAD",
    "SQSP* DOMAIN#228474769",
    "SUPABASE",
    "Screenz LLC",
    "Skarpe",
    "Skool",
    "Sold Out Sales Fun",
    "Squarespace",
    "TRUSTMARY",
    "TWILIO",
    "Testimonia.io",
    "Upwork",
    "VIDALYTICS",
    "WARMY.IO",
    "WEBINARGEEK",
    "WEBSHARE",
    "Webflow",
    "Wise",
    "Zapier",
    "Zenrows",
    "Zoetermeer",
    "Zoom",
    "glocksoft",
    "zonkafeedback",
])

# ── Digital Advertising ──────────────────────────────────────────────────────
_register("advertising", [
    "Facebook",
    "TROPEX MARKETING",
])

# ── Experiments ──────────────────────────────────────────────────────────────
_register("experiments", [
    "CROWDTAMERS",
])

# ── Non-revenue (stored but hidden from P&L display and totals) ──────────────
_register("non_revenue", [
    "SendinBlue",
    "MAKE.COM",
    "LITEMAIL AI",
    "APPS.EMTA.EE",
])


# ── Public API ───────────────────────────────────────────────────────────────

def classify_vendor(vendor: str, fallback_bucket: str | None = None) -> str:
    """Return the correct bucket for a vendor.

    Known vendors get their mapped bucket. Unknown vendors go to 'unclassified'
    so they show up on the P&L for Lloyd to review and assign.
    """
    return VENDOR_BUCKET_MAP.get(_normalise_vendor(vendor), "unclassified")
