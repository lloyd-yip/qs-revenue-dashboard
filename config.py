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

    # Fireflies
    fireflies_api_key: str

    # Scheduler
    daily_sync_hour: int = 2    # 2 AM UTC
    daily_sync_minute: int = 0
    full_sync_day_of_week: str = "sun"  # Weekly full sync on Sundays


settings = Settings()


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
