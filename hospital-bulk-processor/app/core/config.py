from functools import lru_cache

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables and .env."""

    REDIS_URL: str = "redis://localhost:6379/0"
    HOSPITAL_API_URL: AnyUrl = "https://hospital-directory.onrender.com"
    MAX_HOSPITALS_PER_CSV: int = Field(default=20, ge=1, le=20)
    HTTP_TIMEOUT: float = Field(default=10.0, gt=0)
    MAX_RETRIES: int = Field(default=3, ge=0)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

