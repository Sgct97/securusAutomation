"""
Configuration management using Pydantic Settings.
Loads from environment variables and .env file with validation.
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # =========================================================================
    # SECURUS CREDENTIALS
    # =========================================================================
    securus_email: str = Field(
        default="info@eliteinmates.com",
        description="Email for Securus login"
    )
    securus_password: str = Field(
        default="",
        description="Password for Securus login"
    )
    securus_login_url: str = Field(
        default="https://securustech.online/#/login",
        description="Securus login page URL"
    )
    
    # =========================================================================
    # DATABASE
    # =========================================================================
    database_url: str = Field(
        default="sqlite+aiosqlite:///./data/inmates.db",
        description="Database connection URL (async driver required)"
    )
    
    # =========================================================================
    # PIPELINE SETTINGS (Linda: adjust these to change daily behavior)
    # =========================================================================
    daily_message_limit: int = Field(
        default=25,
        ge=0,
        le=60,
        description="Max outreach messages to send per day (set 0 to pause)"
    )
    scrape_interval_days: int = Field(
        default=7,
        ge=1,
        le=30,
        description="Days between scraper runs (1=daily, 7=weekly)"
    )
    states_to_scrape: str = Field(
        default="WA,OK,NY,CA,AR",
        description="Comma-separated list of states to scrape"
    )
    induction_lag_days: int = Field(
        default=7,
        ge=0,
        le=90,
        description=(
            "Min days an inmate must be in our DB before we attempt outreach. "
            "Securus often doesn't have brand-new bookings yet."
        ),
    )
    contact_not_found_retry_days: int = Field(
        default=30,
        ge=1,
        le=180,
        description=(
            "If Securus returns 'contact not found', keep retrying for this "
            "many days before marking permanent. Gives new inmates time to "
            "propagate into Securus' system."
        ),
    )
    excluded_facility_keywords: str = Field(
        default="waiting list,county jail,cnty waiting,co.309,cty 309,cc sentences",
        description=(
            "Comma-separated keywords — any inmate whose facility contains "
            "one of these is skipped (e.g. county-jail waiting lists aren't "
            "in Securus eMessaging)."
        ),
    )
    
    # =========================================================================
    # STAMP BUYING (Linda: adjust these to manage stamp purchasing)
    # =========================================================================
    stamp_auto_buy: bool = Field(
        default=False,
        description="Enable automatic stamp purchasing (set True after dry-run testing)"
    )
    stamp_buffer_per_state: int = Field(
        default=5,
        ge=0,
        le=30,
        description="Extra stamps to keep per state above planned sends"
    )
    daily_stamp_purchase_limit: int = Field(
        default=60,
        ge=0,
        le=120,
        description="Max stamps to purchase in a single day (Securus limit)"
    )
    
    # =========================================================================
    # RATE LIMITING
    # =========================================================================
    securus_action_delay: int = Field(
        default=15,
        ge=5,
        le=120,
        description="Seconds to wait between Securus actions"
    )
    securus_max_messages_per_hour: int = Field(
        default=30,
        ge=1,
        le=100,
        description="Maximum messages to send per hour"
    )
    scraper_request_delay: int = Field(
        default=3,
        ge=1,
        le=30,
        description="Seconds to wait between scraper requests"
    )
    
    # =========================================================================
    # BROWSER SETTINGS
    # =========================================================================
    headless: bool = Field(
        default=True,
        description="Run browser in headless mode"
    )
    browser_timeout: int = Field(
        default=30000,
        ge=5000,
        le=120000,
        description="Browser timeout in milliseconds"
    )
    
    # =========================================================================
    # LOGGING
    # =========================================================================
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level"
    )
    
    # =========================================================================
    # REDIS (for job queue)
    # =========================================================================
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for job queue"
    )
    
    # =========================================================================
    # API SETTINGS
    # =========================================================================
    api_host: str = Field(
        default="0.0.0.0",
        description="API server host"
    )
    api_port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="API server port"
    )
    
    # =========================================================================
    # PATHS
    # =========================================================================
    @property
    def data_dir(self) -> Path:
        """Directory for data files (database, exports)."""
        path = Path("./data")
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def logs_dir(self) -> Path:
        """Directory for log files."""
        path = Path("./logs")
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @field_validator("securus_password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        """Warn if password is empty (will fail at runtime)."""
        if not v:
            import warnings
            warnings.warn(
                "SECURUS_PASSWORD is not set. "
                "Set it in .env or environment variables."
            )
        return v


# Global settings instance (lazy loaded)
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


# Convenience function for quick access
settings = get_settings()

