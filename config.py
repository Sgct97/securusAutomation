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

