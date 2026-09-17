"""The shard consumer against a real stream: delivery, handover and poison entries."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from urllib.parse import urlsplit

import orjson
import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from geotrack.messaging.codec import LocationRecord, encode_record
from geotrack.messaging.keys import (
    DLQ_STREAM,
    INGEST_GROUP,
    POSITIONS_CHANNEL,
    STREAM_FIELD,
    consumer_name,
    ingest_stream,
    shard_lease_key,
    user_channel,
)
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.processor import consumer as consumer_module
from geotrack.processor.batch import BatchProcessor, BatchResult
from geotrack.processor.consumer import ShardConsumer
from geotrack.processor.leases import ShardLeaseManager
from geotrack.processor.publisher import ResultPublisher
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.processor_fixtures import (
    KYIV,
    collect,
    counter,
    create_user,
    create_zone,
    frame_types,
    inside,
    outside,
    report,
    seconds_ago,
    stream_entries,
    subscription,
    wait_for,
)

SHARD = 2
STREAM = ingest_stream(SHARD)


@pytest.fixture
def fast_settings(migrated_database: str, redis_url: str) -> Settings:
    # A short block keeps the loop responsive to a stop request inside a test.
    return make_settings(database_url=migrated_database, redis_url=redis_url, processor_block_ms=50)


@pytest.fixture
async def lease(redis_client: Redis, fast_settings: Settings) -> ShardLeaseManager:
    """This replica owns the shard, as it does before a consumer is ever started."""
    manager = ShardLeaseManager(
        redis_client,
        shards=fast_settings.ingest_shards,
        ttl_ms=fast_settings.processor_lease_ttl_ms,
        instance_id="replica-under-test",
    )
    assert await manager.acquire(SHARD) is True
    return manager


@pytest.fixture
def consumer(
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    fast_settings: Settings,
    lease: ShardLeaseManager,
) -> ShardConsumer:
    return ShardConsumer(
        SHARD,
        redis=redis_client,
        batch_processor=BatchProcessor(session_factory, settings=fast_settings),
        publisher=ResultPublisher(redis_client),
        settings=fast_settings,
        lease=lease,
    )


@asynccontextmanager
async def running(consumer: ShardConsumer) -> AsyncIterator[None]:
    task = asyncio.create_task(consumer.run(), name="consumer-under-test")
    try:
        yield
    finally:
        consumer.request_stop()
        await asyncio.wait_for(task, timeout=10)


async def submit(redis_client: Redis, records: Sequence[LocationRecord]) -> list[bytes]:
    return [
        cast(bytes, await redis_client.xadd(STREAM, {STREAM_FIELD: encode_record(record)}))
        for record in records
    ]


async def submit_raw(redis_client: Redis, payload: bytes) -> bytes:
    return cast(bytes, await redis_client.xadd(STREAM, {STREAM_FIELD: payload}))


async def ensure_group(redis_client: Redis) -> None:
    await redis_client.xgroup_create(STREAM, INGEST_GROUP, id="0", mkstream=True)


async def pending_count(redis_client: Redis) -> int:
    try:
        pending = await redis_client.xpending(STREAM, INGEST_GROUP)
    except ResponseError:
        # The group is created by whichever consumer starts first.
        return 0
    return int(pending["pending"])


async def stream_is_drained(redis_client: Redis) -> bool:
    if await redis_client.xlen(STREAM):
        return False
    return await pending_count(redis_client) == 0


async def rows(engine: AsyncEngine, statement: str) -> Sequence[object]:
    async with engine.connect() as conn:
        return (await conn.execute(text(statement))).all()


async def test_entries_are_applied_and_removed_from_the_stream(
    engine: AsyncEngine, redis_client: Redis, consumer: ShardConsumer
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    await submit(redis_client, [inside("dev-1"), inside("dev-2")])

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    assert len(await rows(engine, "SELECT device_id FROM device_positions")) == 2
    assert len(await rows(engine, "SELECT id FROM alerts")) == 2


async def test_the_backlog_gauge_follows_the_stream_length(
    redis_client: Redis, consumer: ShardConsumer
) -> None:
    await submit(redis_client, [inside(f"dev-{index}") for index in range(5)])

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    assert counter("geotrack_processor_shard_backlog", shard=str(SHARD)) == 0


async def test_an_undecodable_entry_is_dead_lettered_and_acknowledged(
    redis_client: Redis, consumer: ShardConsumer
) -> None:
    before = counter("geotrack_processor_dead_letters_total")
    bad_id = await submit_raw(redis_client, b"{not json at all")
    await submit(redis_client, [inside("dev-1")])

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    dead = await stream_entries(redis_client, DLQ_STREAM)
    assert len(dead) == 1
    fields = dead[0][1]
    assert fields[b"id"] == bad_id
    assert fields[b"shard"] == str(SHARD).encode()
    assert fields[b"raw"] == b"{not json at all"
    assert b"invalid json" in fields[b"error"]
    assert counter("geotrack_processor_dead_letters_total") == before + 1


async def test_a_batch_of_only_bad_entries_still_drains_the_stream(
    redis_client: Redis, consumer: ShardConsumer
) -> None:
    await submit_raw(redis_client, b"[1,2,3]")
    await submit_raw(redis_client, orjson.dumps(["dev-1", 999.0, 0.0, 1, 1]))

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    assert len(await stream_entries(redis_client, DLQ_STREAM)) == 2


async def test_a_report_outside_the_retention_window_is_dead_lettered(
    engine: AsyncEngine, redis_client: Redis, consumer: ShardConsumer
) -> None:
    """The API rejects these; if one still arrives it must not wedge the shard."""
    ancient = report("dev-old", *KYIV, at=datetime.now(UTC) - timedelta(days=400))
    entry_ids = await submit(redis_client, [ancient, inside("dev-ok")])

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    dead = await stream_entries(redis_client, DLQ_STREAM)
    assert len(dead) == 1
    assert b"retention window" in dead[0][1][b"error"]
    # The dead letter points back at the exact stream entry it came from.
    assert dead[0][1][b"id"] == entry_ids[0]
    assert len(await rows(engine, "SELECT device_id FROM device_positions")) == 1


async def test_a_device_id_the_schema_cannot_hold_is_dead_lettered(
    engine: AsyncEngine, redis_client: Redis, consumer: ShardConsumer
) -> None:
    """A check violation would fail identically on every retry and stall the shard."""
    await submit(redis_client, [report("d" * 80, *KYIV), inside("dev-ok")])

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    dead = await stream_entries(redis_client, DLQ_STREAM)
    assert len(dead) == 1
    assert b"device_id must be between" in dead[0][1][b"error"]
    assert len(await rows(engine, "SELECT device_id FROM device_positions")) == 1


async def test_a_consumer_whose_redis_is_unreachable_keeps_its_shard_and_stops_cleanly(
    fast_settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    lease: ShardLeaseManager,
) -> None:
    """A dead task would leave the shard leased by a replica that never reads it."""
    broken = create_redis("redis://127.0.0.1:1/0", purpose="streams")
    consumer = ShardConsumer(
        SHARD,
        redis=broken,
        batch_processor=BatchProcessor(session_factory, settings=fast_settings),
        publisher=ResultPublisher(broken),
        settings=fast_settings,
        lease=lease,
    )
    task = asyncio.create_task(consumer.run())
    try:
        await asyncio.sleep(0.6)
        assert task.done() is False

        consumer.request_stop()
        await asyncio.wait_for(task, timeout=5)
    finally:
        await close_redis(broken)

    assert task.exception() is None


async def test_a_new_owner_finishes_what_the_previous_one_left_pending(
    engine: AsyncEngine,
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    fast_settings: Settings,
    consumer: ShardConsumer,
) -> None:
    """A replica that commits a batch and dies before acknowledging.

    The entries stay in the pending list of the shard's consumer name, so the next
    owner re-reads them. Replaying them must not produce a second alert.
    """
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    records = [inside("dev-1"), inside("dev-2")]
    await submit(redis_client, records)
    await ensure_group(redis_client)

    # The dead replica: it claimed the entries and committed them, then vanished.
    claimed = await redis_client.xreadgroup(
        INGEST_GROUP, consumer_name(SHARD), {STREAM: ">"}, count=10, block=10
    )
    assert await pending_count(redis_client) == 2
    assert claimed is not None
    await BatchProcessor(session_factory, settings=fast_settings).apply(SHARD, records)
    assert len(await rows(engine, "SELECT id FROM alerts")) == 2

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    assert len(await rows(engine, "SELECT id FROM alerts")) == 2


async def test_entries_stay_pending_when_the_batch_cannot_be_applied(
    redis_client: Redis, fast_settings: Settings, lease: ShardLeaseManager
) -> None:
    """Nothing is acknowledged before the transaction commits."""

    class _BrokenProcessor:
        async def apply(self, shard: int, records: Sequence[LocationRecord]) -> BatchResult:
            raise RuntimeError("database is away")

    consumer = ShardConsumer(
        SHARD,
        redis=redis_client,
        batch_processor=cast(BatchProcessor, _BrokenProcessor()),
        publisher=ResultPublisher(redis_client),
        settings=fast_settings,
        lease=lease,
    )
    await submit(redis_client, [inside("dev-1")])

    async with running(consumer):
        await wait_for(lambda: pending_count(redis_client))

    # Still in the stream and still claimed, so the next owner will pick it up.
    assert await redis_client.xlen(STREAM) == 1
    assert await pending_count(redis_client) == 1


async def test_a_consumer_recovers_once_the_database_answers_again(
    engine: AsyncEngine,
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    fast_settings: Settings,
    lease: ShardLeaseManager,
) -> None:
    real = BatchProcessor(session_factory, settings=fast_settings)

    class _FlakyProcessor:
        def __init__(self) -> None:
            self.calls = 0

        async def apply(self, shard: int, records: Sequence[LocationRecord]) -> BatchResult:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("database is away")
            return await real.apply(shard, records)

    flaky = _FlakyProcessor()
    consumer = ShardConsumer(
        SHARD,
        redis=redis_client,
        batch_processor=cast(BatchProcessor, flaky),
        publisher=ResultPublisher(redis_client),
        settings=fast_settings,
        lease=lease,
    )
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    await submit(redis_client, [inside("dev-1")])

    async with running(consumer):
        await wait_for(lambda: stream_is_drained(redis_client))

    assert flaky.calls >= 2
    assert len(await rows(engine, "SELECT id FROM alerts")) == 1


async def test_a_committed_batch_is_broadcast_to_the_gateway_channels(
    engine: AsyncEngine, redis_client: Redis, redis_url: str, consumer: ShardConsumer
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)

    async with subscription(redis_url, POSITIONS_CHANNEL, user_channel(user_id)) as pubsub:
        await submit(redis_client, [inside("dev-1")])
        async with running(consumer):
            await wait_for(lambda: stream_is_drained(redis_client))

        frames = await collect(pubsub, count=2)

    assert frame_types(frames) == {"positions", "alert"}


async def test_a_stop_request_ends_the_loop_without_losing_entries(
    redis_client: Redis, consumer: ShardConsumer
) -> None:
    task = asyncio.create_task(consumer.run())
    await submit(redis_client, [outside("dev-1", at=seconds_ago(5))])
    await wait_for(lambda: stream_is_drained(redis_client))

    consumer.request_stop()
    await asyncio.wait_for(task, timeout=5)

    assert task.done()
    assert await redis_client.xlen(STREAM) == 0


@dataclass
class _Relay:
    """A TCP relay in front of Redis that can be told to stop answering.

    A stalled node, a paused container and a half-open NAT mapping all look like this
    from the client: the socket is open, the command goes out, and the reply never
    arrives. It is the one failure a socket-level error cannot signal.
    """

    url: str = ""
    flowing: asyncio.Event = field(default_factory=asyncio.Event)

    def stall(self) -> None:
        self.flowing.clear()


async def _pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, relay: _Relay) -> None:
    try:
        while data := await reader.read(65_536):
            await relay.flowing.wait()
            writer.write(data)
            await writer.drain()
    except ConnectionResetError, BrokenPipeError:
        pass
    finally:
        writer.close()


@asynccontextmanager
async def a_relay_in_front_of(redis_url: str) -> AsyncIterator[_Relay]:
    parts = urlsplit(redis_url)
    relay = _Relay()
    relay.flowing.set()
    pumps: set[asyncio.Task[None]] = set()

    async def handle(
        client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        server_reader, server_writer = await asyncio.open_connection(
            parts.hostname, parts.port or 6379
        )
        for reader, writer in ((client_reader, server_writer), (server_reader, client_writer)):
            pumps.add(asyncio.create_task(_pump(reader, writer, relay)))

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    relay.url = f"redis://127.0.0.1:{server.sockets[0].getsockname()[1]}/0"
    try:
        yield relay
    finally:
        relay.flowing.set()
        server.close()
        for pump in pumps:
            pump.cancel()
        with suppress(TimeoutError):
            async with asyncio.timeout(2):
                await server.wait_closed()


async def test_a_redis_that_stops_answering_does_not_park_the_shard(
    monkeypatch: pytest.MonkeyPatch,
    redis_client: Redis,
    redis_url: str,
    session_factory: async_sessionmaker[AsyncSession],
    fast_settings: Settings,
    lease: ShardLeaseManager,
) -> None:
    """A stop request is only ever seen between two awaits, so every await needs a ceiling.

    Without one a shard is parked forever with its lease still held: nothing fails,
    nothing retries, and shutdown waits for a task that will never come back.
    """
    monkeypatch.setattr(consumer_module, "REDIS_OP_TIMEOUT_S", 0.3)

    async with a_relay_in_front_of(redis_url) as relay:
        stalling = create_redis(relay.url, purpose="streams")
        consumer = ShardConsumer(
            SHARD,
            redis=stalling,
            batch_processor=BatchProcessor(session_factory, settings=fast_settings),
            publisher=ResultPublisher(redis_client),
            settings=fast_settings,
            lease=lease,
        )
        task = asyncio.create_task(consumer.run())
        try:
            await submit(redis_client, [inside("dev-1")])
            await wait_for(lambda: stream_is_drained(redis_client))

            relay.stall()
            await asyncio.sleep(0.6)
            # Still owns the shard: a task that ended would leave it leased by a reader.
            assert task.done() is False

            consumer.request_stop()
            await asyncio.wait_for(task, timeout=5)
        finally:
            task.cancel()
            await close_redis(stalling)

    assert task.cancelled() is False
    assert task.exception() is None


async def test_a_consumer_stops_itself_when_another_replica_takes_its_lease(
    redis_client: Redis,
    consumer: ShardConsumer,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The lease is checked by the consumer itself, not only by the control loop.

    Waiting for the service to notice means reading and committing for up to a third of
    a lease under a shard somebody else already owns.
    """
    task = asyncio.create_task(consumer.run())
    try:
        await redis_client.set(shard_lease_key(SHARD), "another-replica", px=60_000)
        await submit(redis_client, [inside("dev-1")])

        await asyncio.wait_for(task, timeout=5)
    finally:
        task.cancel()

    # The entry is left for whoever owns the shard now, unapplied and unacknowledged.
    assert await redis_client.xlen(STREAM) == 1
    async with session_factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM device_positions")) == 0


class _ServerWithoutXackdel:
    """Redis before 8.2, where XACKDEL does not exist."""

    def __init__(self, inner: Redis) -> None:
        self._inner = inner
        self.attempts = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def xackdel(self, *args: Any, **kwargs: Any) -> Any:
        self.attempts += 1
        raise ResponseError("unknown command 'XACKDEL'")


class _ServerThatRefusesTwice:
    """A reply that is not about the command existing: a group being recreated."""

    def __init__(self, inner: Redis, *, refusals: int) -> None:
        self._inner = inner
        self._refusals = refusals
        self.attempts = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def xackdel(self, *args: Any, **kwargs: Any) -> Any:
        self.attempts += 1
        if self.attempts <= self._refusals:
            raise ResponseError("NOGROUP No such consumer group 'processors'")
        return await self._inner.xackdel(*args, **kwargs)


def _consumer_over(
    redis: Any,
    *,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    lease: ShardLeaseManager,
) -> ShardConsumer:
    return ShardConsumer(
        SHARD,
        redis=cast(Redis, redis),
        batch_processor=BatchProcessor(session_factory, settings=settings),
        publisher=ResultPublisher(cast(Redis, redis)),
        settings=settings,
        lease=lease,
    )


async def test_a_server_without_xackdel_falls_back_once_and_stays_on_the_fallback(
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    fast_settings: Settings,
    lease: ShardLeaseManager,
) -> None:
    """Two round trips instead of one is the price on an older server, paid once."""
    old_server = _ServerWithoutXackdel(redis_client)
    consumer = _consumer_over(
        old_server, settings=fast_settings, session_factory=session_factory, lease=lease
    )

    async with running(consumer):
        await submit(redis_client, [inside("dev-1")])
        await wait_for(lambda: stream_is_drained(redis_client))
        await submit(redis_client, [inside("dev-2")])
        await wait_for(lambda: stream_is_drained(redis_client))

    assert old_server.attempts == 1


async def test_a_transient_refusal_does_not_cost_xackdel_for_the_life_of_the_process(
    redis_client: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    fast_settings: Settings,
    lease: ShardLeaseManager,
) -> None:
    """Only "the server does not have this command" is a reason to stop using it.

    Anything else — a stream trimmed away and its group being recreated — is transient,
    and latching on it would silently double the cost of every acknowledgement from then
    on, with nothing to show that it happened.
    """
    flaky = _ServerThatRefusesTwice(redis_client, refusals=2)
    consumer = _consumer_over(
        flaky, settings=fast_settings, session_factory=session_factory, lease=lease
    )

    async with running(consumer):
        await submit(redis_client, [inside("dev-1")])
        await wait_for(lambda: stream_is_drained(redis_client))

    # It kept trying XACKDEL through both refusals instead of giving up on it.
    assert flaky.attempts == 3


async def test_a_consumer_rebuilds_its_group_when_the_stream_is_taken_away(
    engine: AsyncEngine, redis_client: Redis, consumer: ShardConsumer
) -> None:
    """A stream that was trimmed or deleted takes its consumer group with it.

    Reads then come back as NOGROUP rather than as a transport error, and the shard has
    to recover on its own: nothing else in the replica is watching the stream.
    """
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)

    async with running(consumer):
        await submit(redis_client, [inside("dev-1")])
        await wait_for(lambda: stream_is_drained(redis_client))

        await redis_client.delete(STREAM)

        await submit(redis_client, [inside("dev-2")])
        await wait_for(lambda: stream_is_drained(redis_client))

    assert len(await rows(engine, "SELECT device_id FROM device_positions")) == 2
