from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, read from environment variables (upper-case field names)."""

    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    service_name: str = "geotrack"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    database_url: SecretStr
    redis_url: SecretStr

    jwt_secret: SecretStr = Field(min_length=32)
    jwt_ttl_seconds: int = Field(default=86_400, ge=60, le=30 * 86_400)
    ingest_api_key: SecretStr = Field(min_length=16)

    ingest_shards: int = Field(default=8, ge=1, le=256)
    ingest_max_batch: int = Field(default=1_000, ge=1, le=10_000)
    ingest_backlog_high: int = Field(default=200_000, ge=1)
    ingest_backlog_low: int = Field(default=100_000, ge=0)
    ingest_backlog_poll_ms: int = Field(default=250, ge=10)
    ingest_max_future_skew_s: int = Field(default=300, ge=0)
    history_retention_days: int = Field(default=7, ge=1, le=365)

    db_pool_size: int = Field(default=10, ge=1, le=200)
    db_pool_timeout_s: float = Field(default=5.0, gt=0)
    db_statement_timeout_ms: int = Field(default=5_000, ge=100)

    processor_batch_size: int = Field(default=1_000, ge=1, le=10_000)
    processor_block_ms: int = Field(default=1_000, ge=10, le=4_000)
    processor_lease_ttl_ms: int = Field(default=15_000, ge=1_000)
    processor_http_port: int = Field(default=9100, ge=1, le=65_535)

    ws_tick_ms: int = Field(default=250, ge=20, le=5_000)
    ws_control_queue_max: int = Field(default=1_000, ge=1)
    ws_send_timeout_s: float = Field(default=10.0, gt=0)
    ws_device_stale_s: int = Field(default=300, ge=5)
    ws_grid_cell_deg: float = Field(default=0.05, gt=0, le=10)
    ws_max_sessions_per_user: int = Field(default=20, ge=1)

    geozone_quota_per_user: int = Field(default=500, ge=1)

    @field_validator("database_url")
    @classmethod
    def _require_asyncpg(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith("postgresql+asyncpg://"):
            raise ValueError("database_url must use the postgresql+asyncpg:// scheme")
        return value

    @field_validator("redis_url")
    @classmethod
    def _require_redis_scheme(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().startswith(("redis://", "rediss://")):
            raise ValueError("redis_url must use the redis:// or rediss:// scheme")
        return value

    @model_validator(mode="after")
    def _check_backlog_watermarks(self) -> Self:
        if self.ingest_backlog_low >= self.ingest_backlog_high:
            raise ValueError("ingest_backlog_low must be lower than ingest_backlog_high")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
