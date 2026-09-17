"""The pub/sub bridge against a real Redis."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any, cast
from uuid import UUID

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from geotrack.ids import new_uuid
from geotrack.messaging.codec import PositionItem, encode_positions
from geotrack.messaging.keys import POSITIONS_CHANNEL, user_channel
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.realtime.bridge import RedisBridge
from tests.waiting import wait_until


class FlakyPubSub:
    """A real pub/sub connection whose next ``failures`` subscribes are refused."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.failures = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def subscribe(self, *channels: str) -> None:
        if self.failures:
            self.failures -= 1
            raise RedisError("LOADING Redis is loading the dataset in memory")
        await self._inner.subscribe(*channels)


class FlakyRedis:
    """A real client that hands out one ``FlakyPubSub``, the way the bridge asks."""

    def __init__(self, inner: Redis) -> None:
        self._inner = inner
        self.pubsub_calls = FlakyPubSub(inner.pubsub())

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def pubsub(self) -> FlakyPubSub:
        return self.pubsub_calls


class Received:
    def __init__(self) -> None:
        self.positions: list[PositionItem] = []
        self.frames: list[tuple[UUID, bytes]] = []

    def on_positions(self, items: Sequence[PositionItem]) -> None:
        self.positions.extend(items)

    def on_user_frame(self, user_id: UUID, frame: bytes) -> None:
        self.frames.append((user_id, frame))


async def wait_for_subscribers(client: Redis, channel: str, *, count: int) -> None:
    """Wait until Redis itself reports the subscription, not just that we sent it."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5.0
    while loop.time() < deadline:
        numbers = await client.pubsub_numsub(channel)
        if (int(numbers[0][1]) if numbers else 0) == count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{channel} did not reach {count} subscribers")


@pytest.fixture
async def received() -> Received:
    return Received()


@pytest.fixture
async def bridge(redis_url: str, received: Received) -> AsyncIterator[RedisBridge]:
    client = create_redis(redis_url, purpose="pubsub", max_connections=4)
    bridge = RedisBridge(
        client, on_positions=received.on_positions, on_user_frame=received.on_user_frame
    )
    await bridge.start()
    try:
        yield bridge
    finally:
        await bridge.stop()
        await close_redis(client)


async def test_positions_are_decoded_and_handed_over(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=1)
    items: list[PositionItem] = [("dev-1", 50.45, 30.52, 1_700_000_000_000)]

    await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))

    await wait_until(lambda: received.positions == items, what="the positions callback")


async def test_user_frames_arrive_only_while_subscribed(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    user_id = new_uuid()
    channel = user_channel(user_id)

    await redis_client.publish(channel, b'{"type":"alert","alert":{"id":1}}')
    await asyncio.sleep(0.1)
    assert received.frames == []

    await bridge.subscribe_user(user_id)
    await wait_for_subscribers(redis_client, channel, count=1)
    await redis_client.publish(channel, b'{"type":"alert","alert":{"id":2}}')

    await wait_until(lambda: len(received.frames) == 1, what="the user frame")
    assert received.frames[0] == (user_id, b'{"type":"alert","alert":{"id":2}}')


async def test_unsubscribing_stops_the_stream_for_that_user(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    user_id = new_uuid()
    channel = user_channel(user_id)
    await bridge.subscribe_user(user_id)
    await wait_for_subscribers(redis_client, channel, count=1)

    await bridge.unsubscribe_user(user_id)
    await wait_for_subscribers(redis_client, channel, count=0)

    await redis_client.publish(channel, b'{"type":"alert","alert":{"id":3}}')
    await asyncio.sleep(0.1)

    assert received.frames == []


async def test_an_unreadable_payload_does_not_stop_the_reader(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=1)
    items: list[PositionItem] = [("dev-2", 1.0, 2.0, 3)]

    await redis_client.publish(POSITIONS_CHANNEL, b"{not json")
    await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))

    await wait_until(lambda: received.positions == items, what="the good payload")


async def test_the_reader_recovers_when_its_connection_is_dropped(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    user_id = new_uuid()
    channel = user_channel(user_id)
    await bridge.subscribe_user(user_id)
    await wait_for_subscribers(redis_client, channel, count=1)

    killed = [
        await redis_client.client_kill_filter(_id=str(client["id"]))
        for client in await redis_client.client_list(_type="pubsub")
    ]
    assert sum(killed) == 1

    # The reader notices the dropped connection, reconnects and restores both channels.
    await wait_for_subscribers(redis_client, channel, count=1)
    await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=1)
    await redis_client.publish(channel, b'{"type":"alert","alert":{"id":4}}')

    await wait_until(
        lambda: [frame for _, frame in received.frames] == [b'{"type":"alert","alert":{"id":4}}'],
        timeout_s=5.0,
        what="delivery after the reconnect",
    )


async def test_a_malformed_position_item_does_not_stop_the_reader(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    """The shape a producer bug or a version skew actually takes.

    ``b"{not json"`` is the easy case. An items array whose latitude is a string is
    the one that used to end the reader task and, with it, every live update this
    replica had left to deliver — positions, alerts, zone events and session lists.
    """
    user_id = new_uuid()
    channel = user_channel(user_id)
    await bridge.subscribe_user(user_id)
    await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=1)
    await wait_for_subscribers(redis_client, channel, count=1)
    items: list[PositionItem] = [("dev-3", 4.0, 5.0, 6)]

    await redis_client.publish(POSITIONS_CHANNEL, b'{"items": [["dev-1", "north", 30.0, 1]]}')
    await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))
    await redis_client.publish(channel, b'{"type":"alert","alert":{"id":7}}')

    await wait_until(lambda: received.positions == items, what="the next good payload")
    await wait_until(lambda: len(received.frames) == 1, what="the next user frame")
    assert bridge.dropped_messages == 1


async def test_a_user_frame_that_is_not_text_is_dropped_instead_of_relayed(
    bridge: RedisBridge, redis_client: Redis, received: Received
) -> None:
    user_id = new_uuid()
    channel = user_channel(user_id)
    await bridge.subscribe_user(user_id)
    await wait_for_subscribers(redis_client, channel, count=1)

    await redis_client.publish(channel, b'{"type":"alert","alert":{"name":"\xff"}}')
    await redis_client.publish(channel, b'{"type":"alert","alert":{"id":8}}')

    await wait_until(lambda: len(received.frames) == 1, what="the readable frame")
    assert received.frames[0][1] == b'{"type":"alert","alert":{"id":8}}'
    assert bridge.dropped_messages == 1


async def test_a_subscribe_that_redis_refuses_repairs_itself(
    redis_url: str, redis_client: Redis, received: Received
) -> None:
    """A failed SUBSCRIBE used to be repaired only by a reader crash that never came.

    The bridge believed it held the channel, the reader stayed healthy on the
    positions traffic, and that user's alerts were dropped for the whole session with
    nothing about the connection looking wrong.
    """
    client = create_redis(redis_url, purpose="pubsub", max_connections=4)
    flaky = FlakyRedis(client)
    bridge = RedisBridge(
        cast(Redis, flaky), on_positions=received.on_positions, on_user_frame=received.on_user_frame
    )
    await bridge.start()
    try:
        user_id = new_uuid()
        channel = user_channel(user_id)
        flaky.pubsub_calls.failures = 1

        await bridge.subscribe_user(user_id)
        assert flaky.pubsub_calls.failures == 0, "the test never exercised a failed subscribe"
        assert not bridge.in_sync

        # No reconnect, no reader failure: the bridge notices the gap by itself.
        await wait_for_subscribers(redis_client, channel, count=1)
        await redis_client.publish(channel, b'{"type":"alert","alert":{"id":9}}')

        await wait_until(
            lambda: (
                [frame for _, frame in received.frames] == [b'{"type":"alert","alert":{"id":9}}']
            ),
            timeout_s=5.0,
            what="the alert that follows the repair",
        )
        assert bridge.in_sync
    finally:
        await bridge.stop()
        await close_redis(client)
