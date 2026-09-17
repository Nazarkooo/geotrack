"""The batch statement's execution plan.

The zone hit test is the only part of a batch whose cost grows with the number of
zones, and it is index-assisted only as long as the planner keeps using the stored
search polygon. This asserts the execution plan rather than trusting that it still holds.
"""

import random

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.clock import now_ms
from geotrack.messaging.codec import LocationRecord
from geotrack.processor.batch import APPLY_BATCH_SQL, statement_params
from tests.integration.processor_fixtures import create_user

ZONES = 5_000
REPORTS = 1_000
# A dense square over Kyiv, so that every report really does fall into many zones.
SOUTH, NORTH = 50.35, 50.55
WEST, EAST = 30.30, 30.75


async def _seed_zones(engine: AsyncEngine) -> None:
    user_id = await create_user(engine, "planner")
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO geozones (id, user_id, name, color, center, radius_m)
                SELECT gen_random_uuid(), :user_id, 'zone ' || g, '#3fb1ff',
                       ST_SetSRID(
                           ST_MakePoint(
                               CAST(:west AS float8) + random() * CAST(:lon_span AS float8),
                               CAST(:south AS float8) + random() * CAST(:lat_span AS float8)
                           ),
                           4326
                       )::geography,
                       200 + random() * 2000
                FROM generate_series(1, CAST(:zones AS int)) g
                """
            ),
            {
                "user_id": user_id,
                "zones": ZONES,
                "west": WEST,
                "lon_span": EAST - WEST,
                "south": SOUTH,
                "lat_span": NORTH - SOUTH,
            },
        )
        await conn.execute(text("ANALYZE geozones"))


def _batch() -> list[LocationRecord]:
    now = now_ms()
    return [
        LocationRecord(
            device_id=f"dev-{index:05d}",
            lat=SOUTH + random.random() * (NORTH - SOUTH),
            lon=WEST + random.random() * (EAST - WEST),
            reported_ms=now,
            received_ms=now,
        )
        for index in range(REPORTS)
    ]


async def test_the_zone_hit_test_runs_as_a_gist_index_scan(engine: AsyncEngine) -> None:
    await _seed_zones(engine)

    async with engine.begin() as conn:
        plan = "\n".join(
            (
                await conn.execute(
                    text("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + str(APPLY_BATCH_SQL)),
                    statement_params(_batch()),
                )
            ).scalars()
        )

    hit_test = [line for line in plan.splitlines() if "ix_geozones_search_area" in line]
    assert len(hit_test) == 1, plan
    assert "Index Scan using ix_geozones_search_area on geozones" in hit_test[0], plan
    # One index probe per report: the cost of a batch grows with the batch, not with the
    # number of zones. A sequential scan here is what the search polygon exists to avoid.
    assert f"loops={REPORTS}" in hit_test[0], plan
    assert "Index Cond: (search_area && " in plan, plan


async def test_the_stale_guard_is_a_primary_key_conflict_filter(engine: AsyncEngine) -> None:
    """The upsert's WHERE clause is what discards an out-of-order report."""
    await _seed_zones(engine)

    async with engine.begin() as conn:
        plan = "\n".join(
            (
                await conn.execute(
                    text("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + str(APPLY_BATCH_SQL)),
                    statement_params(_batch()),
                )
            ).scalars()
        )

    assert "Conflict Arbiter Indexes: device_positions_pkey" in plan, plan
    assert "Conflict Filter: (dp.reported_at < excluded.reported_at)" in plan, plan
    assert "Conflict Arbiter Indexes: pk_zone_presence" in plan, plan
