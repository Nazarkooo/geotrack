"""The retry logic keys off SQLSTATE codes, so the mapping is verified against the
real driver rather than assumed from documentation.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.db.errors import (
    FOREIGN_KEY_VIOLATION,
    RETRYABLE_SQLSTATES,
    UNIQUE_VIOLATION,
    sqlstate_of,
)
from geotrack.ids import new_uuid


async def test_foreign_key_violation_is_reported_as_23503(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError) as excinfo:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO geozones (id, user_id, name, color, center, radius_m) "
                    "VALUES (:id, :user_id, 'z', '#3fb1ff', "
                    "ST_SetSRID(ST_MakePoint(30.5, 50.4), 4326)::geography, 100)"
                ),
                {"id": new_uuid(), "user_id": new_uuid()},
            )

    assert sqlstate_of(excinfo.value) == FOREIGN_KEY_VIOLATION
    assert FOREIGN_KEY_VIOLATION in RETRYABLE_SQLSTATES


async def test_unique_violation_is_reported_as_23505(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO users (id, username) VALUES (:id, 'dupe')"), {"id": new_uuid()}
        )

    with pytest.raises(IntegrityError) as excinfo:
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO users (id, username) VALUES (:id, 'dupe')"), {"id": new_uuid()}
            )

    assert sqlstate_of(excinfo.value) == UNIQUE_VIOLATION
    assert UNIQUE_VIOLATION not in RETRYABLE_SQLSTATES
