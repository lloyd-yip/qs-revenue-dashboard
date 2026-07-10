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


# ── Payee aliases ────────────────────────────────────────────────────────────
# Some people invoice under a company name. Canonicalise to the person's name
# BEFORE classification so the P&L shows the person and rep-comp matching
# (expense vendor == GHL rep name) works.

VENDOR_ALIASES: dict[str, str] = {}


def _alias(canonical: str, variants: list[str]) -> None:
    for v in variants:
        VENDOR_ALIASES[_normalise_vendor(v)] = canonical


_alias("Armando Valencia", [
    "J.A Valencia Enterprices LLC",
    "J.A. Valencia Enterprices LLC",
    "J.A Valencia Enterprises LLC",
    "JA Valencia Enterprices LLC",
])


def canonicalise_vendor(name: str) -> str:
    """Return the canonical payee name for a vendor (aliases resolved)."""
    return VENDOR_ALIASES.get(_normalise_vendor(name), name)


# ── Sales ────────────────────────────────────────────────────────────────────
# NOTE: Alexander Gessel is intentionally NOT registered here — he's paid from
# "Salaries - Management", so he inherits that account's bucket (marketing_salaries).
_register("sales", [
    # Individual names (transaction-level loads)
    "Armando Valencia",
    "James Caddick",
    "Jason Bern",
    "Mathieu Hutin",
    "Melissa Fredericks",
    "Princewill Chinedu Ejiogu",
    "Ryan Matsumori",
    "Ryan McNichol",
    # Xero P&L account names
    "Salaries - Sales",
])

# ── Marketing Salaries ───────────────────────────────────────────────────────
# Whitelist confirmed by Lloyd 2026-07-10: only these people are marketing
# salaries. Other payees on the salary accounts are hidden (non_revenue below).
_register("marketing_salaries", [
    # Individual names (transaction-level loads)
    "Angel Hernandez",
    "Brooks Golden",
    "Gergo Nagy",
    "Gonzalo Ariel Guitar",
    "Lloyd Yip",
    "Maria Coutiño",
    "Santiago Acevedo",
    "Tatiana Herrera",
    # Xero P&L account names
    "Salaries - Management",
    "Salaries - Marketing",
    "Salaries - Assistant",
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
    "Zapier",
    "Zenrows",
    "Zoetermeer",
    "Zoom",
    "glocksoft",
    "zonkafeedback",
    # Xero P&L account names
    "Tool - AI",
    "Tool - CRM",
    "Tool - CyberSecurity",
    "Tool - Marketing",
    "Tool - Project Management",
    "Tools - Automation",
    "Tools - Funnel",
    "Stripe Billing Usage Fee",
])

# ── Digital Advertising ──────────────────────────────────────────────────────
_register("advertising", [
    "Facebook",
    "TROPEX MARKETING",
    # Xero P&L account names
    "Paid Ads",
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
    "COLUMN NATIONAL ASSOCIATION",  # Doug — delivery team, not sales/marketing
    "James Walter",                  # Paid from Salaries - Sales but not a sales rep (Lloyd, 2026-07-10)
    # Salary-account payees excluded from the marketing whitelist (Lloyd, 2026-07-10)
    "Alexander Gessel",              # Founder comp — not a sales/marketing cost
    "Alex's Bancolombia",            # Alex's bank account — same person
    "Carlos Duque International",
    "Andrea Ochoa Velez",
    "Madeline Celine Gesell",
    "ISABEL GRISALES CARMONA",
    "Sandra Stringer",
    "Wise",                          # Bank transfer account — not an expense; confirmed not in Xero P&L
    # Xero P&L account names — hidden from dashboard
    "Salaries - Operations",         # Delivery team
    "Salaries - Hermes",             # Lloyd OS server costs
    "Bank Fees",
    "Payoneer Bank Fees",
    "Stripe Collection Fees",
    "Whop Fees",                     # Payment processing
    "Splitit fee Whop",              # Payment processing (Splitit fees routed via Whop)
    "Foreign Currency Gains and Losses",
    "Consulting & Accounting",       # Accountant fees
])


# ── Public API ───────────────────────────────────────────────────────────────

def classify_vendor(vendor: str, fallback_bucket: str | None = None) -> str:
    """Return the correct bucket for a vendor.

    Known vendors get their mapped bucket. Unknown vendors fall back to the
    caller-supplied bucket (e.g. from the seed script), or 'unclassified' if
    no fallback was provided — so they appear on the P&L for review.
    """
    return VENDOR_BUCKET_MAP.get(_normalise_vendor(vendor), fallback_bucket or "unclassified")
