from typing import Any

from geotrack.settings import Settings

TEST_JWT_SECRET = "test-jwt-secret-with-enough-entropy-0123456789"
TEST_INGEST_KEY = "test-ingest-key-0123456789"


def make_settings(**overrides: Any) -> Settings:
    """Settings for tests; infrastructure URLs are placeholders unless overridden."""
    values: dict[str, Any] = {
        "database_url": "postgresql+asyncpg://geotrack:geotrack@127.0.0.1:5432/geotrack",
        "redis_url": "redis://127.0.0.1:6379/0",
        "jwt_secret": TEST_JWT_SECRET,
        "ingest_api_key": TEST_INGEST_KEY,
        "log_format": "console",
    }
    values.update(overrides)
    return Settings(**values)
