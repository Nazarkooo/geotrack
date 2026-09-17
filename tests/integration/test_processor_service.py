"""A processor replica end to end: leases, consumption, fan-out and shutdown."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast

import orjson
import pytest
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.clock import now_ms
from geotrack.messaging.codec import PositionItem, decode_positions, encode_record
from geotrack.messaging.keys import (
    MAINTENANCE_LEASE_KEY,
    POSITIONS_CHANNEL,
    PROCESSORS_ZSET,
    SHARDS_META_KEY,
    STREAM_FIELD,
    ingest_stream,
    shard_lease_key,
    user_channel,
)
from geotrack.processor import service as service_module
from geotrack.processor.consumer import ShardConsumer
from geotrack.processor.service import ProcessorService, ShardCountMismatchError, _Running
from geotrack.settings import Settings
from geotrack.sharding import shard_for
from tests.conftest import make_settings
from tests.integration.processor_fixtures import (
    KYIV,
    create_user,
    create_zone,
    inside,
    subscription,
    wait_for,
)

SHARDS = 4


@pytest.fixture
def service_settings(migrated_database: str, redis_url: str) -> Settings:
    return make_settings(
        database_url=migrated_database,
        redis_url=redis_url,
        ingest_shards=SHARDS,
        processor_block_ms=50,
        # The floor the settings allow: renewals every 333 ms keep the tests quick.
        processor_lease_ttl_ms=1_000,
    )


@pytest.fixture
async def service(service_settings: Settings) -> AsyncIterator[ProcessorService]:
    service = ProcessorService(service_settings, instance_id="replica-a")
    try:
        yield service
    finally:
        await service.stop()


async def started(service: ProcessorService) -> ProcessorService:
    await service.start()
    await wait_for(lambda: _owns_everything(service))
    return service


async def _owns_everything(service: ProcessorService) -> bool:
    return len(service.owned_shards) == SHARDS


async def submit(redis_client: Redis, device_id: str) -> None:
    record = inside(device_id)
    await redis_client.xadd(
        ingest_stream(shard_for(device_id, SHARDS)), {STREAM_FIELD: encode_record(record)}
    )


async def alert_count(engine: AsyncEngine) -> int:
    async with engine.connect() as conn:
        count = await conn.scalar(text("SELECT count(*) FROM alerts"))
    return int(count or 0)


async def test_a_lone_replica_takes_every_shard(service: ProcessorService) -> None:
    await started(service)

    assert service.owned_shards == set(range(SHARDS))
    assert await service.is_ready() is True


async def test_reports_flow_from_the_stream_to_the_database_and_out_again(
    engine: AsyncEngine, redis_client: Redis, redis_url: str, service: ProcessorService
) -> None:
    devices = [f"dev-{index}" for index in range(12)]
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id, name="depot")

    async with subscription(redis_url, POSITIONS_CHANNEL, user_channel(user_id)) as pubsub:
        await started(service)
        for device_id in devices:
            await submit(redis_client, device_id)

        await wait_for(lambda: _all_alerts_raised(engine, expected=12))
        positions, alerts = await _collect_broadcast(pubsub, devices)

    assert await alert_count(engine) == 12

    # The frames are what the gateway forwards to a browser unchanged, so their contents
    # are the contract, not just their shape. Both channels have to carry every device:
    # the map layer reads one, the alert feed the other.
    assert all(
        (round(latitude, 4), round(longitude, 4)) == (round(KYIV[0], 4), round(KYIV[1], 4))
        for _, latitude, longitude, _ in positions.values()
    )
    assert {alert["kind"] for alert in alerts.values()} == {"enter"}
    assert {alert["zone"]["name"] for alert in alerts.values()} == {"depot"}
    assert all(
        (round(alert["latitude"], 4), round(alert["longitude"], 4))
        == (round(KYIV[0], 4), round(KYIV[1], 4))
        for alert in alerts.values()
    )


async def _collect_broadcast(
    pubsub: PubSub, devices: list[str], *, timeout_s: float = 15.0
) -> tuple[dict[str, PositionItem], dict[str, Any]]:
    """Read both channels until every device has been seen on each of them.

    How many frames that takes is not fixed: the shards are consumed in parallel and a
    shard's devices may arrive in one batch or in several.
    """
    positions: dict[str, PositionItem] = {}
    alerts: dict[str, Any] = {}
    expected = set(devices)
    async with asyncio.timeout(timeout_s):
        while positions.keys() != expected or alerts.keys() != expected:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)
            if message is not None:
                payload = bytes(message["data"])
                frame = orjson.loads(payload)
                if frame.get("type") == "alert":
                    alerts[frame["alert"]["device_id"]] = frame["alert"]
                else:
                    positions.update({item[0]: item for item in decode_positions(payload)})
            await asyncio.sleep(0)
    return positions, alerts


async def _all_alerts_raised(engine: AsyncEngine, *, expected: int) -> bool:
    return await alert_count(engine) == expected


async def test_two_replicas_split_the_shards(
    service_settings: Settings, service: ProcessorService
) -> None:
    await started(service)
    second = ProcessorService(service_settings, instance_id="replica-b")
    try:
        await second.start()
        await wait_for(lambda: _both_settled(service, second))
        # Read the ownership before shutting the second replica down, which gives its
        # shards back.
        first_shards, second_shards = service.owned_shards, second.owned_shards
    finally:
        await second.stop()

    assert len(first_shards) == 2
    assert len(second_shards) == 2
    assert first_shards | second_shards == set(range(SHARDS))
    assert first_shards.isdisjoint(second_shards)


async def _both_settled(first: ProcessorService, second: ProcessorService) -> bool:
    return len(first.owned_shards) == 2 and len(second.owned_shards) == 2


async def test_a_stolen_lease_stops_the_consumer_that_lost_it(
    redis_client: Redis, service: ProcessorService
) -> None:
    await started(service)

    # Another replica took over shard 1 while this one was not looking.
    await redis_client.set(shard_lease_key(1), "thief", px=60_000)

    await wait_for(lambda: _released(service, shard=1))
    assert 1 not in service.owned_shards
    assert await redis_client.get(shard_lease_key(1)) == b"thief"


async def _released(service: ProcessorService, *, shard: int) -> bool:
    return shard not in service.owned_shards


async def test_a_shard_whose_consumer_stopped_is_taken_up_again(
    engine: AsyncEngine, redis_client: Redis, service: ProcessorService
) -> None:
    """Belt and braces: a shard owned by a reader that no longer reads is a black hole.

    The consumer loop is written never to end on its own, so the failure is simulated
    here by cancelling the task behind the service's back.
    """
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    await started(service)
    victim = next(shard for shard in range(SHARDS) if service.is_consuming(shard))
    service._consumers[victim].task.cancel()

    await wait_for(lambda: _consuming_again(service, victim))

    device_id = next(
        f"dev-{index}" for index in range(500) if shard_for(f"dev-{index}", SHARDS) == victim
    )
    await submit(redis_client, device_id)
    await wait_for(lambda: _all_alerts_raised(engine, expected=1))


async def _consuming_again(service: ProcessorService, shard: int) -> bool:
    return service.is_consuming(shard)


async def test_shutting_down_releases_the_leases_and_leaves_the_membership_set(
    redis_client: Redis, service_settings: Settings
) -> None:
    service = ProcessorService(service_settings, instance_id="replica-c")
    await started(service)

    await service.stop()

    for shard in range(SHARDS):
        assert await redis_client.get(shard_lease_key(shard)) is None
    assert await redis_client.zscore(PROCESSORS_ZSET, "replica-c") is None
    assert await service.is_ready() is False


async def test_a_replica_refuses_to_start_against_a_different_shard_count(
    redis_client: Redis, service_settings: Settings
) -> None:
    """Re-sharding would send one device's reports to two consumers at once."""
    await redis_client.set(SHARDS_META_KEY, SHARDS * 2)
    service = ProcessorService(service_settings, instance_id="replica-d")

    with pytest.raises(ShardCountMismatchError, match="running 8"):
        await service.start()

    assert service.owned_shards == set()


async def test_the_recorded_shard_count_is_published_on_first_start(
    redis_client: Redis, service: ProcessorService
) -> None:
    await started(service)

    assert await redis_client.get(SHARDS_META_KEY) == str(SHARDS).encode()


async def test_entries_queued_before_the_replica_started_are_still_processed(
    engine: AsyncEngine, redis_client: Redis, service: ProcessorService
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    for index in range(5):
        await submit(redis_client, f"dev-{index}")

    await started(service)

    await wait_for(lambda: _all_applied(engine, expected=5))
    assert await alert_count(engine) == 5


async def _all_applied(engine: AsyncEngine, *, expected: int) -> bool:
    async with engine.connect() as conn:
        count = await conn.scalar(text("SELECT count(*) FROM device_positions"))
    return int(count or 0) == expected


class _ConsumerThatWillNotStop:
    """A consumer wedged inside a batch: it is told to stop and does not."""

    def __init__(self) -> None:
        self.stop_requested = False

    def request_stop(self) -> None:
        self.stop_requested = True


def _wedge(
    service: ProcessorService, shard: int
) -> tuple[_ConsumerThatWillNotStop, asyncio.Task[None]]:
    """Replace a live consumer with one that answers a stop request by ignoring it."""
    running = service._consumers[shard]
    running.task.cancel()
    wedged = _ConsumerThatWillNotStop()
    stuck: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(3_600), name=f"wedged-{shard}")
    service._consumers[shard] = _Running(consumer=cast(ShardConsumer, wedged), task=stuck)
    return wedged, stuck


async def test_a_consumer_that_will_not_stop_does_not_cost_the_other_leases(
    monkeypatch: pytest.MonkeyPatch, redis_client: Redis, service: ProcessorService
) -> None:
    """Handing one shard back must never be able to hand the rest to another replica.

    A lease lives for a fraction of the time a batch is allowed to take, so waiting for
    a consumer to stop on the same task that renews the other leases is enough to lose
    all of them: they expire, other replicas pick them up, and this one is still reading.
    """
    monkeypatch.setattr(service_module, "CONSUMER_STOP_TIMEOUT_S", 3.0)
    monkeypatch.setattr(service_module, "CANCEL_GRACE_S", 0.5)
    monkeypatch.setattr(service_module, "SHUTDOWN_TIMEOUT_S", 1.0)
    await started(service)
    victim = 1
    others = set(range(SHARDS)) - {victim}
    wedged, stuck = _wedge(service, victim)

    # Another replica takes the wedged shard, which is what makes the renewal loop try
    # to stop that consumer in the first place.
    await redis_client.set(shard_lease_key(victim), "thief", px=60_000)

    # Several renewal rounds at a one second lease: without the renewals, every one of
    # the other leases would be gone by now.
    await asyncio.sleep(2.0)

    # It was asked to stop and is still going: the drain is in flight, not the loops.
    assert wedged.stop_requested is True
    assert stuck.done() is False
    for shard in others:
        assert await redis_client.get(shard_lease_key(shard)) == b"replica-a"
    assert service.owned_shards == others
    assert await redis_client.get(shard_lease_key(victim)) == b"thief"


async def test_a_start_that_fails_leaves_the_fleet_exactly_as_it_found_it(
    redis_client: Redis, redis_url: str
) -> None:
    """A replica that cannot finish starting must not keep the fleet's maintenance lease.

    The container exits and restarts; the next process finds the lease taken, skips the
    round and consumes anyway, so for as long as the lease lives nobody in the fleet
    creates history partitions.
    """
    doomed = make_settings(
        # Nothing listens there: the first statement of the first maintenance round fails.
        database_url="postgresql+asyncpg://geotrack:geotrack@127.0.0.1:1/geotrack",
        redis_url=redis_url,
        ingest_shards=SHARDS,
    )
    service = ProcessorService(doomed, instance_id="replica-doomed")

    with pytest.raises(ConnectionRefusedError):
        await service.start()

    assert await redis_client.get(MAINTENANCE_LEASE_KEY) is None
    assert await redis_client.zscore(PROCESSORS_ZSET, "replica-doomed") is None
    assert service.owned_shards == set()
    assert await service.is_ready() is False


async def test_a_replica_that_refuses_to_start_closes_what_it_opened(
    redis_client: Redis, service_settings: Settings
) -> None:
    """The ASGI lifespan never reaches its shutdown half when start() raises."""
    await redis_client.set(SHARDS_META_KEY, SHARDS * 2)
    service = ProcessorService(service_settings, instance_id="replica-e")

    with pytest.raises(ShardCountMismatchError):
        await service.start()

    assert await service.is_ready() is False
    # Idempotent: the lifespan calls stop() as well, whether or not start() got that far.
    await service.stop()


async def test_a_shard_that_is_winding_down_keeps_its_lease_until_it_is_handed_over(
    monkeypatch: pytest.MonkeyPatch, redis_client: Redis, service: ProcessorService
) -> None:
    """Handing a shard back is not the same moment as letting go of it.

    The consumer still has a batch in flight, and a lease that quietly expired underneath
    it would hand the shard to another replica while this one is still writing.
    """
    monkeypatch.setattr(service_module, "CONSUMER_STOP_TIMEOUT_S", 2.0)
    monkeypatch.setattr(service_module, "CANCEL_GRACE_S", 0.5)
    monkeypatch.setattr(service_module, "SHUTDOWN_TIMEOUT_S", 1.0)
    await started(service)
    # The highest shards are handed back first, so this is the one a second replica takes.
    victim = SHARDS - 1
    _wedge(service, victim)

    # A second replica joins the fleet: this one is now over its fair share of the ring.
    await redis_client.zadd(PROCESSORS_ZSET, {"replica-b": now_ms()})
    await wait_for(lambda: _released(service, shard=victim))

    # Well past the one second lease: only a renewal can keep it.
    await asyncio.sleep(1.5)
    assert await redis_client.get(shard_lease_key(victim)) == b"replica-a"

    # And once the consumer is finally done with it, the shard is free for the taking.
    await wait_for(lambda: _lease_is_free(redis_client, victim), timeout_s=5)


async def _lease_is_free(redis_client: Redis, shard: int) -> bool:
    return await redis_client.get(shard_lease_key(shard)) is None
