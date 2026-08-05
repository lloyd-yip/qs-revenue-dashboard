from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str  # postgresql+asyncpg://user:pass@host/db

    # API auth — static bearer token. Set in Railway env vars. Keep secret.
    api_bearer_token: str

    # GHL
    ghl_api_key: str
    ghl_location_id: str = "G7ZOWCq78JrzUjlLMCxt"
    ghl_pipeline_id: str = "zbI8YxmB9qhk1h4cInnq"
    ghl_api_base_url: str = "https://services.leadconnectorhq.com"

    # Sync settings
    # Delay between paginated GHL API calls (ms) to stay within rate limits
    ghl_page_delay_ms: int = 150
    ghl_page_size: int = 100
    # Overall hard cap on a single sync run (seconds). A run that exceeds this is
    # cancelled and recorded as 'failed' rather than hanging forever at 'running'.
    # Default 2700s (45 min) — well above a normal ~20-30 min run, but finite.
    sync_timeout_s: int = 2700
    # Any sync_run still 'running' older than this (minutes) is a stuck/orphaned run
    # (e.g. the process died mid-sync before it could write its status) and is reaped
    # to 'failed'. Must be comfortably above sync_timeout_s.
    sync_stale_reap_minutes: int = 90

    # Whop — optional, only needed for /deals/run-match
    # Add WHOP_API_KEY to Railway env vars to enable deal matching.
    # Verify it's set: Railway → qs-revenue-dashboard → Variables → WHOP_API_KEY
    # Silent failure: if missing, POST /deals/run-match returns 401 from Whop API
    whop_api_key: str = ""

    # Stripe — optional, enables second-pass enrichment of deal payment data.
    # Matches unmatched deals via Stripe metadata (GHL contactId) and email.
    # Fills missing payment metrics (upfront_cash, total_paid) on Whop-matched deals.
    # Add STRIPE_SECRET_KEY to Railway env vars to enable.
    stripe_secret_key: str = ""

    # Xero OAuth — optional, enables P&L sync and Wise bank transfer reconciliation.
    # XERO_CLIENT_SECRET: get from Xero developer portal → "Automate accounting" app → Configuration
    # After setting, visit /xero/auth on Railway to complete the OAuth flow (stores refresh token in DB).
    xero_client_secret: str = ""

    # Wise — optional, enables bank transfer reconciliation.
    # Add WISE_API_KEY + WISE_PRIVATE_KEY to Railway env vars to enable.
    # WISE_PRIVATE_KEY = the RSA private key PEM (multi-line — use Railway's
    # multi-line env var support). The corresponding public key must be registered
    # in Wise → Settings → Developer → Strong Customer Authentication.
    # Verify: POST /api/dashboard/deals/sync-wise → should return incoming transactions.
    # Silent failure: if WISE_PRIVATE_KEY is missing, sync returns 0 transactions.
    wise_api_key: str = ""
    wise_private_key: str = ""

    # Fireflies
    fireflies_api_key: str

    # Retell — optional, enables AI voice-call sync + contact source-checking.
    # Preferred path is to save the key in-app (Settings → Connectors → Retell,
    # stored in app_settings); this env var is only a legacy/bootstrap fallback.
    retell_api_key: str = ""

    # Appointwise — optional. Stored in-app (Settings → Connectors). Appointwise is a
    # GHL-native SMS app with no documented public analytics API, so SMS metrics are read
    # from GHL Conversations; the key/client-id are kept for any future Appointwise API use.
    appointwise_api_key: str = ""
    appointwise_client_id: str = ""
    # Fetch GHL Conversation SMS + compute engagement for Appointwise-tagged contacts during
    # the main sync (merged, not a separate pass). Set false to disable if it strains quota.
    sms_sync_enabled: bool = True

    # Public base URL of this app — used to show the Appointwise webhook URL in Settings.
    public_base_url: str = "https://qs-revenue-dashboard-production.up.railway.app"

    # Scheduler
    daily_sync_hour: int = 2    # 2 AM UTC
    daily_sync_minute: int = 0
    full_sync_day_of_week: str = "sun"  # Weekly full sync on Sundays


settings = Settings()


# ── Pipeline IDs ──────────────────────────────────────────────────────────────
SALES_PIPELINE_ID  = "zbI8YxmB9qhk1h4cInnq"
UPSELL_PIPELINE_ID = "NjidsHukHHUpYtTcQefX"

# ── Excluded Whop products ──────────────────────────────────────────────────────
# Whop products that are NOT QS coaching revenue — sold as separate offers and must
# be excluded from every dashboard metric/calculation. Deals whose only Whop presence
# is one of these products are flagged is_excluded=True by the matcher and dropped
# from all views. Add product IDs here as new separate offers launch.
#   prod_bgKXIW0Wly5R1 — Calendar Automation (separate subscription offer)
#   prod_QRBbImc0avOvr — Calendar Automation (40 Accts) — a second CA product ID
# NOTE: prefer EXCLUDED_WHOP_PRODUCT_NAME_PATTERNS below — it auto-catches every
# tier/variant of an offer by NAME so new product IDs don't need adding here by hand.
EXCLUDED_WHOP_PRODUCT_IDS: frozenset[str] = frozenset({
    "prod_bgKXIW0Wly5R1",
    "prod_QRBbImc0avOvr",
})

# Product NAME substrings (lower-cased, matched against the Whop product title) that
# mark a separate, non-coaching offer. The matcher resolves every product's title
# once per run and excludes any membership whose product title contains one of these
# — so a new Calendar Automation tier is excluded automatically, without editing the
# ID set above. Keep patterns specific to avoid excluding real coaching products.
EXCLUDED_WHOP_PRODUCT_NAME_PATTERNS: tuple[str, ...] = (
    "calendar automation",
)

# An UNMATCHED Whop membership (no GHL deal) counts as a "coaching orphan" — worth
# surfacing for review — when it paid at least this much and isn't an excluded
# product. Real coaching starts ~$2,667, so a $1,000 floor cleanly separates it
# from community subs / low-ticket products.
ORPHAN_COACHING_FLOOR: float = 1000.0

# Upsell stage IDs — Client Delivery Revenue Pipeline
UPSELL_STAGE_OUTREACH_SENT  = "e08df229-3462-4b6c-aa5a-7a16d6b18773"
UPSELL_STAGE_CALL_SCHEDULED = "859efac7-0f23-4d7e-8b53-032e95b69c58"
UPSELL_STAGE_CLOSED_WON     = "eafe20aa-00cd-445e-9f3b-95a21ed6f41a"
UPSELL_STAGE_CLOSED_LOST    = "8dc4ee8c-4150-4060-9503-61d0a9fbe37d"
UPSELL_STAGE_DEAL_VALUE     = "9a387dab-44d3-4e4e-a541-6b856b84fc93"


# ── Rep Roster ────────────────────────────────────────────────────────────────
# Categories:
#   active     — currently taking sales calls
#   inactive   — past sales reps, no longer at the company (or firing)
#   other      — assistants, tech coaches, non-sales staff
#
# Team analytics ("All Team" totals, averages, insights) include active + inactive.
# "Other" reps are excluded from team totals so they don't skew metrics.
# All three groups appear in the dropdown, sorted into <optgroup> sections.

REP_ROSTER: dict[str, str] = {
    # Active Sales Reps
    "Ryan Matsumori": "active",
    "Melissa Fredericks": "active",
    "Armando Valencia": "active",       # Closer, added 2026-05-07
    "Alex Amor Gesell": "active",       # Founder — takes calls to review quality
    "Lloyd Yip": "active",              # CRO — takes calls to check quality
    # Inactive Sales Reps
    "Jason Bern": "inactive",
    "Ryan McNichol": "inactive",
    "James Caddick": "inactive",
    "Darrin Glesser": "inactive",
    "Mathieu Hutin": "inactive",
    "Scottie Schneider": "inactive",
    # Other (non-sales staff)
    "Santiago Acevedo": "other",
    "Veronica Vaida": "other",
    "Jose Velez": "other",
    "Juan Lopera": "other",
    "Juan Rivera": "other",
    "Gonzalo Guitar": "other",
}


def _normalize_name(name: str) -> str:
    """Collapse multiple spaces so 'Melissa  Fredericks' matches 'Melissa Fredericks'."""
    return " ".join(name.split())


# Normalized lookup: maps both the canonical name and any whitespace variants
# to the category.  Used by reps.py for classification.
_REP_ROSTER_NORMALIZED: dict[str, str] = {
    _normalize_name(n): c for n, c in REP_ROSTER.items()
}


def get_rep_category(name: str) -> str:
    """Return the roster category for a rep name, handling whitespace variants."""
    return _REP_ROSTER_NORMALIZED.get(_normalize_name(name), "other")

# Derived sets for quick lookups
ACTIVE_REP_NAMES = frozenset(n for n, c in REP_ROSTER.items() if c == "active")
INACTIVE_REP_NAMES = frozenset(n for n, c in REP_ROSTER.items() if c == "inactive")
OTHER_REP_NAMES = frozenset(n for n, c in REP_ROSTER.items() if c == "other")

# Names included in team analytics (active + inactive sales reps)
SALES_REP_NAMES = ACTIVE_REP_NAMES | INACTIVE_REP_NAMES

# All known names that appear in the dropdown (all 3 groups)
ALL_KNOWN_REP_NAMES = SALES_REP_NAMES | OTHER_REP_NAMES

# DB-safe name sets — GHL sometimes stores names with extra spaces
# (e.g. "Melissa  Fredericks"). We need both variants for SQL IN clauses.
# Approach: we build the "exclude" set (OTHER) and use NOT IN, since there
# are fewer variants to track and we control those names exactly.
# If a name is unknown (not in roster), it's treated as sales rep by default.
_DB_OTHER_NAMES: frozenset[str] = OTHER_REP_NAMES
