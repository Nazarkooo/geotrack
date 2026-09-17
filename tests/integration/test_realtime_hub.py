"""The hub's warm start against real PostGIS.

A replica that comes up mid-flight must already know where the fleet is: without this
its first clients would see an empty map until every device happened to report again.
"""

from datetime import timedelta

import orjson
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from geotrack.clock import utc_now
from geotrack.geo import BBox
from geotrack.realtime.grid import rects_for
from geotrack.realtime.hub import PositionHub

STALE_S = 300
WORLD = BBox(west=-180.0, south=-90.0, east=180.0, north=90.0)

_INSERT = text(
    """
    INSERT INTO device_positions (device_id, position, reported_at, received_at)
    VALUES (
        :device_id,
        ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography,
        :reported_at,
        :reported_at
    )
    """
)


def loaded_devices(hub: PositionHub) -> set[str]:
    chunks = b",".join(hub.snapshot_chunks(rects_for(WORLD, 0.05)))
    if not chunks:
        return set()
    return {item[0] for item in orjson.loads(b"[" + chunks + b"]")}


async def test_warm_start_loads_recent_positions_only(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = utc_now()
    rows = [
        {"device_id": "fresh-1", "latitude": 50.45, "longitude": 30.52, "reported_at": now},
        {
            "device_id": "fresh-2",
            "latitude": -33.87,
            "longitude": 151.21,
            "reported_at": now - timedelta(seconds=STALE_S - 30),
        },
        {
            "device_id": "long-gone",
            "latitude": 0.0,
            "longitude": 0.0,
            "reported_at": now - timedelta(seconds=STALE_S + 60),
        },
    ]
    async with session_factory() as session:
        await session.execute(_INSERT, rows)
        await session.commit()

    hub = PositionHub(cell_size_deg=0.05, stale_after_s=STALE_S)
    async with session_factory() as session:
        loaded = await hub.warm_start(session)

    assert loaded == 2
    assert hub.device_count == 2
    assert loaded_devices(hub) == {"fresh-1", "fresh-2"}


async def test_warm_start_is_state_rather_than_a_delta(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await session.execute(
            _INSERT,
            [{"device_id": "dev-1", "latitude": 1.0, "longitude": 2.0, "reported_at": utc_now()}],
        )
        await session.commit()

    hub = PositionHub(cell_size_deg=0.05, stale_after_s=STALE_S)
    async with session_factory() as session:
        await hub.warm_start(session)

    # Nobody was connected while it loaded, so there is nothing to send as a change.
    assert hub.drain(t_ms=1).is_empty


async def test_warm_start_on_an_empty_database_is_harmless(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    hub = PositionHub(cell_size_deg=0.05, stale_after_s=STALE_S)

    async with session_factory() as session:
        assert await hub.warm_start(session) == 0

    assert hub.device_count == 0
