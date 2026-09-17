"""Shared building blocks for the processor and batch integration tests."""

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import orjson
from prometheus_client import REGISTRY
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.clock import to_epoch_ms, utc_now
from geotrack.ids import new_uuid
from geotrack.messaging.codec import LocationRecord
from geotrack.messaging.redis import close_redis, create_redis

type StreamEntry = tuple[bytes, dict[bytes, bytes]]

# Somewhere unambiguous and far from the antimeridian; the spatial suite covers the
# awkward parts of the globe, these tests care about state transitions.
KYIV = (50.4501, 30.5234)


async def create_user(engine: AsyncEngine, username: str) -> UUID:
    user_id = new_uuid()
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO users (id, username) VALUES (:id, :username)"),
            {"id": user_id, "username": username},
        )
    return user_id


async def create_zone(
    engine: AsyncEngine,
    *,
    user_id: UUID,
    lat: float = KYIV[0],
    lon: float = KYIV[1],
    radius_m: float = 500.0,
    name: str = "zone",
    color: str = "#3fb1ff",
    alert_on_enter: bool = True,
    alert_on_exit: bool = True,
    dwell_alert_interval_s: int | None = None,
) -> UUID:
    zone_id = new_uuid()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO geozones (id, user_id, name, color, center, radius_m,
                                      alert_on_enter, alert_on_exit, dwell_alert_interval_s)
                VALUES (:id, :user_id, :name, :color,
                        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography, :radius_m,
                        :alert_on_enter, :alert_on_exit, :dwell)
                """
            ),
            {
                "id": zone_id,
                "user_id": user_id,
                "name": name,
                "color": color,
                "lat": lat,
                "lon": lon,
                "radius_m": radius_m,
                "alert_on_enter": alert_on_enter,
                "alert_on_exit": alert_on_exit,
                "dwell": dwell_alert_interval_s,
            },
        )
    return zone_id


def report(
    device_id: str,
    lat: float,
    lon: float,
    *,
    at: datetime | None = None,
    received: datetime | None = None,
) -> LocationRecord:
    moment = at or utc_now()
    return LocationRecord(
        device_id=device_id,
        lat=lat,
        lon=lon,
        reported_ms=to_epoch_ms(moment),
        received_ms=to_epoch_ms(received or moment),
    )


def inside(device_id: str, *, at: datetime | None = None) -> LocationRecord:
    return report(device_id, KYIV[0], KYIV[1], at=at)


def outside(device_id: str, *, at: datetime | None = None) -> LocationRecord:
    """Roughly 11 km north of the centre: outside any zone these tests create."""
    return report(device_id, KYIV[0] + 0.1, KYIV[1], at=at)


def seconds_ago(seconds: float) -> datetime:
    return datetime.now(UTC) - timedelta(seconds=seconds)


def counter(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels or None) or 0.0


async def stream_entries(client: Redis, stream: str) -> list[StreamEntry]:
    """redis-py types stream replies as the raw protocol union; narrow it once here."""
    return cast(list[StreamEntry], await client.xrange(stream))


@asynccontextmanager
async def subscription(url: str, *channels: str) -> AsyncIterator[PubSub]:
    """A pub/sub connection of its own, as the gateway would open."""
    client = create_redis(url, purpose="pubsub", max_connections=4)
    pubsub = client.pubsub()
    await pubsub.subscribe(*channels)
    try:
        yield pubsub
    finally:
        await pubsub.aclose()  # type: ignore[no-untyped-call]
        await close_redis(client)


async def collect(pubsub: PubSub, *, count: int, timeout_s: float = 10.0) -> list[bytes]:
    received: list[bytes] = []
    async with asyncio.timeout(timeout_s):
        while len(received) < count:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)
            if message is not None:
                received.append(bytes(message["data"]))
            await asyncio.sleep(0)
    return received


def frame_types(frames: list[bytes]) -> set[str]:
    """Positions frames are the only ones without a ``type`` the tests care about."""
    return {orjson.loads(frame).get("type", "positions") for frame in frames}


async def wait_for(
    probe: Callable[[], Coroutine[Any, Any, Any]], *, timeout_s: float = 10.0
) -> Any:
    """Poll until the probe returns something truthy, or fail with what it last saw."""
    deadline = time.monotonic() + timeout_s
    result: Any = None
    while time.monotonic() < deadline:
        result = await probe()
        if result:
            return result
        await asyncio.sleep(0.02)
    raise AssertionError(f"condition not reached within {timeout_s}s, last value: {result!r}")
