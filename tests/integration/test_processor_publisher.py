"""Fan-out of a committed batch onto the pub/sub channels the gateway listens on."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import orjson
import pytest
from redis.asyncio import Redis

from geotrack.clock import utc_now
from geotrack.db.models import AlertKind
from geotrack.ids import new_uuid
from geotrack.messaging.codec import decode_positions
from geotrack.messaging.keys import POSITIONS_CHANNEL, user_channel
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.processor.batch import AlertRow, BatchResult
from geotrack.processor.publisher import PUBLISH_CHUNK, ResultPublisher
from tests.integration.processor_fixtures import collect, frame_types, subscription


def an_alert(user_id: UUID, **overrides: Any) -> AlertRow:
    fields: dict[str, Any] = {
        "id": 7,
        "user_id": user_id,
        "zone_id": new_uuid(),
        "zone_name": "depot",
        "device_id": "dev-1",
        "kind": AlertKind.ENTER,
        "latitude": 50.4501,
        "longitude": 30.5234,
        "occurred_at": utc_now(),
        "created_at": utc_now(),
    }
    fields.update(overrides)
    return AlertRow(**fields)


async def test_accepted_positions_reach_the_positions_channel(
    redis_client: Redis, redis_url: str
) -> None:
    async with subscription(redis_url, POSITIONS_CHANNEL) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(
                accepted=[("dev-1", 50.4501, 30.5234, 1_700_000_000_000)],
                alerts=[],
                stale=0,
                rejected=[],
            )
        )

        payload = (await collect(pubsub, count=1))[0]

    assert decode_positions(payload) == [("dev-1", 50.4501, 30.5234, 1_700_000_000_000)]


async def test_an_alert_reaches_only_its_own_user_channel(
    redis_client: Redis, redis_url: str
) -> None:
    alice, bob = new_uuid(), new_uuid()
    alert = an_alert(alice)

    async with subscription(redis_url, user_channel(alice), user_channel(bob)) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(accepted=[], alerts=[alert], stale=0, rejected=[])
        )

        frame = orjson.loads((await collect(pubsub, count=1))[0])
        with pytest.raises(TimeoutError):
            await collect(pubsub, count=1, timeout_s=0.3)

    assert frame["type"] == "alert"
    assert frame["alert"]["id"] == 7
    assert frame["alert"]["kind"] == "enter"
    assert frame["alert"]["zone"] == {"id": str(alert.zone_id), "name": "depot"}


async def test_the_published_frame_matches_the_rest_representation(
    redis_client: Redis, redis_url: str
) -> None:
    """The browser uses one code path for live alerts and for the REST backfill.

    The expected shape is written out by hand on purpose: comparing the frame against
    ``alert_payload`` would compare the publisher with itself and would keep passing
    through a rename that breaks every REST client.
    """
    user_id = new_uuid()
    zone_id = new_uuid()
    occurred_at = datetime(2026, 3, 1, 10, 30, tzinfo=UTC)
    created_at = datetime(2026, 3, 1, 10, 30, 1, 250_000, tzinfo=UTC)
    alert = an_alert(
        user_id,
        id=4_242,
        zone_id=zone_id,
        kind=AlertKind.DWELL,
        occurred_at=occurred_at,
        created_at=created_at,
    )

    async with subscription(redis_url, user_channel(user_id)) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(accepted=[], alerts=[alert], stale=0, rejected=[])
        )
        frame = orjson.loads((await collect(pubsub, count=1))[0])

    assert frame == {
        "type": "alert",
        "alert": {
            "id": 4_242,
            "kind": "dwell",
            "zone": {"id": str(zone_id), "name": "depot"},
            "device_id": "dev-1",
            "latitude": 50.4501,
            "longitude": 30.5234,
            "occurred_at": "2026-03-01T10:30:00Z",
            "created_at": "2026-03-01T10:30:01.250000Z",
        },
    }


async def test_a_deleted_zone_still_publishes_a_readable_alert(
    redis_client: Redis, redis_url: str
) -> None:
    user_id = new_uuid()

    async with subscription(redis_url, user_channel(user_id)) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(
                accepted=[],
                alerts=[an_alert(user_id, zone_id=None, zone_name="removed depot")],
                stale=0,
                rejected=[],
            )
        )
        frame = orjson.loads((await collect(pubsub, count=1))[0])

    assert frame["alert"]["zone"] == {"id": None, "name": "removed depot"}


async def test_an_empty_batch_publishes_nothing(redis_client: Redis, redis_url: str) -> None:
    async with subscription(redis_url, POSITIONS_CHANNEL) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(accepted=[], alerts=[], stale=3, rejected=[])
        )

        with pytest.raises(TimeoutError):
            await collect(pubsub, count=1, timeout_s=0.3)


async def test_a_burst_larger_than_one_chunk_is_delivered_whole(
    redis_client: Redis, redis_url: str
) -> None:
    """A zone drawn over a depot alerts on every device inside it at once."""
    user_id = new_uuid()
    alerts = [an_alert(user_id, id=index) for index in range(PUBLISH_CHUNK * 2 + 17)]

    async with subscription(redis_url, user_channel(user_id)) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(accepted=[], alerts=alerts, stale=0, rejected=[])
        )
        frames = await collect(pubsub, count=len(alerts))

    assert [orjson.loads(frame)["alert"]["id"] for frame in frames] == list(range(len(alerts)))


async def test_a_broken_redis_does_not_fail_the_batch() -> None:
    """The data is already committed; a failed broadcast must not replay the batch."""
    broken = create_redis("redis://127.0.0.1:1/0", purpose="commands")
    try:
        await ResultPublisher(broken).publish(
            BatchResult(accepted=[("dev-1", 1.0, 2.0, 3)], alerts=[], stale=0, rejected=[])
        )
    finally:
        await close_redis(broken)


async def test_an_alert_no_frame_can_be_built_for_does_not_sink_the_rest_of_the_batch(
    redis_client: Redis, redis_url: str
) -> None:
    """The fan-out runs after the commit, so one unrenderable alert must cost only itself.

    ``device_id`` is the seam: the stream only promises a non-empty string, while the
    client frame is the REST model. If the two ever disagree the batch is already in the
    database, and losing the whole broadcast over it would be the expensive mistake.
    """
    user_id = new_uuid()
    unrenderable = an_alert(user_id, id=1, device_id="dev 1")
    ordinary = an_alert(user_id, id=2)

    async with subscription(redis_url, POSITIONS_CHANNEL, user_channel(user_id)) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(
                accepted=[("dev-1", 50.4501, 30.5234, 1_700_000_000_000)],
                alerts=[unrenderable, ordinary],
                stale=0,
                rejected=[],
            )
        )
        frames = await collect(pubsub, count=2)
        with pytest.raises(TimeoutError):
            await collect(pubsub, count=1, timeout_s=0.3)

    assert frame_types(frames) == {"positions", "alert"}
    alerts = [orjson.loads(frame) for frame in frames if b'"alert"' in frame]
    assert [payload["alert"]["id"] for payload in alerts] == [2]


async def test_a_publisher_that_cannot_build_any_frame_still_returns(
    redis_client: Redis, redis_url: str
) -> None:
    """Nothing in the fan-out may reach the consumer loop: the batch is already durable."""
    user_id = new_uuid()

    async with subscription(redis_url, user_channel(user_id)) as pubsub:
        await ResultPublisher(redis_client).publish(
            BatchResult(
                accepted=[],
                alerts=[an_alert(user_id, device_id="dev 1")],
                stale=0,
                rejected=[],
            )
        )

        with pytest.raises(TimeoutError):
            await collect(pubsub, count=1, timeout_s=0.3)
