"""Query plans behind the read endpoints.

The repositories claim a particular access path. A comment cannot go stale silently if
the planner is asked to confirm it, so these tests seed enough rows — and enough variety
— for the planner to have a real choice, then read the plan it picked.
"""

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.db.repositories.geozones import DEVICES_INSIDE_SQL

DEVICE_COUNT = 20_000
ALERT_COUNT = 50_000
ACCOUNT_COUNT = 200


async def _plan(engine: AsyncEngine, statement: str, params: dict[str, Any]) -> str:
    async with engine.connect() as conn:
        raw = await conn.scalar(text(f"EXPLAIN (FORMAT JSON) {statement}"), params)
    return json.dumps(raw if isinstance(raw, list) else json.loads(str(raw)))


async def test_devices_inside_a_zone_ride_the_position_index(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO device_positions (device_id, position, reported_at, received_at) "
                "SELECT 'dev-' || i, "
                "       ST_SetSRID(ST_MakePoint(30.0 + (i % 1000) / 1000.0, "
                "                               50.0 + (i / 1000) / 1000.0), 4326)::geography, "
                "       now(), now() "
                "FROM generate_series(1, :count) AS i"
            ),
            {"count": DEVICE_COUNT},
        )
        await conn.execute(text("ANALYZE device_positions"))

    plan = await _plan(
        engine,
        DEVICES_INSIDE_SQL,
        {"latitude": 50.5, "longitude": 30.5, "radius_m": 300.0, "limit": 500},
    )

    # A per-row radius could not do this; a constant one lets ST_DWithin become a
    # bounding-box probe answered from the GiST index.
    assert "ix_device_positions_position" in plan
    assert "Seq Scan" not in plan


async def test_the_alert_feed_walks_its_keyset_index_backwards(engine: AsyncEngine) -> None:
    # Alerts from many accounts, which is what makes the composite index worth having:
    # with a single account the primary key alone would already be in the right order.
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users (id, username) "
                "SELECT gen_random_uuid(), 'account' || i FROM generate_series(1, :count) AS i"
            ),
            {"count": ACCOUNT_COUNT},
        )
        await conn.execute(
            text(
                "INSERT INTO alerts (user_id, zone_id, zone_name, device_id, kind, position, "
                "occurred_at) SELECT u.id, NULL, 'z', 'dev-' || i, 'enter', "
                "ST_SetSRID(ST_MakePoint(30.5, 50.5), 4326)::geography, now() "
                "FROM generate_series(1, :count) AS i "
                "JOIN LATERAL ("
                "  SELECT id FROM users ORDER BY username OFFSET (i % :accounts) LIMIT 1"
                ") u ON true"
            ),
            {"count": ALERT_COUNT, "accounts": ACCOUNT_COUNT},
        )
        await conn.execute(text("ANALYZE alerts"))
        user_id = await conn.scalar(text("SELECT id FROM users WHERE username = 'account1'"))

    plan = await _plan(
        engine,
        "SELECT id FROM alerts WHERE user_id = :user_id AND id < :before_id "
        "ORDER BY id DESC LIMIT :limit",
        {"user_id": user_id, "before_id": ALERT_COUNT - 1_000, "limit": 100},
    )

    # No Sort node: a page comes off the index in the order it is already stored in, so
    # paging cost does not grow with how far back the reader has scrolled.
    assert "ix_alerts_user_id_id" in plan
    assert '"Node Type": "Sort"' not in plan
