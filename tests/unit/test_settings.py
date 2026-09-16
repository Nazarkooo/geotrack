from typing import Any

import pytest
from pydantic import ValidationError

from geotrack.settings import Settings

BASE: dict[str, Any] = {
    "database_url": "postgresql+asyncpg://u:p@localhost:5432/db",
    "redis_url": "redis://localhost:6379/0",
    "jwt_secret": "x" * 32,
    "ingest_api_key": "k" * 16,
}


def test_defaults_are_sane() -> None:
    settings = Settings(**BASE)

    assert settings.ingest_shards == 8
    assert settings.ingest_backlog_low < settings.ingest_backlog_high
    assert settings.jwt_secret.get_secret_value() == "x" * 32


def test_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in BASE.items():
        monkeypatch.setenv(key.upper(), value)
    monkeypatch.setenv("INGEST_SHARDS", "16")

    assert Settings().ingest_shards == 16


@pytest.mark.parametrize(
    ("override", "field"),
    [
        ({"jwt_secret": "short"}, "jwt_secret"),
        ({"ingest_api_key": "short"}, "ingest_api_key"),
        ({"ingest_backlog_low": 500, "ingest_backlog_high": 500}, "ingest_backlog_low"),
        ({"ingest_shards": 0}, "ingest_shards"),
        ({"database_url": "mysql://nope"}, "database_url"),
    ],
)
def test_rejects_unsafe_values(override: dict[str, Any], field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        Settings(**(BASE | override))
