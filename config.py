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

    # Scheduler
    daily_sync_hour: int = 2    # 2 AM UTC
    daily_sync_minute: int = 0
    full_sync_day_of_week: str = "sun"  # Weekly full sync on Sundays


settings = Settings()
