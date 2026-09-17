"""What a replica does when the fan-out plumbing misbehaves.

These are the failure modes that make a replica look healthy while every websocket on
it has gone quiet, which is worse than a replica that refuses to serve.
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from geotrack.api.app import create_app
from geotrack.messaging.codec import encode_positions
from geotrack.messaging.keys import POSITIONS_CHANNEL
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.realtime.bridge import RedisBridge
from geotrack.settings import Settings


class _AngryPubSub:
    """A pub/sub object that fails the way redis-py does when no subscribe landed."""

    def __init__(self, inner: Any, failures: int) -> None:
        self._inner = inner
        self._failures = failures
        self.reads = 0

    async def get_message(self, **kwargs: Any) -> Any:
        self.reads += 1
        if self._failures > 0:
            self._failures -= 1
            # Not a RedisError and not an OSError: redis-py raises this bare RuntimeError
            # when the first subscribe never established a connection.
            raise RuntimeError("pubsub connection not set")
        return await self._inner.get_message(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@pytest.fixture
async def publisher(redis_url: str) -> AsyncIterator[Redis]:
    client = create_redis(redis_url, purpose="commands")
    yield client
    await close_redis(client)


async def test_the_reader_survives_a_failure_that_is_not_a_redis_error(
    redis_url: str, publisher: Redis
) -> None:
    received: list[tuple[str, float, float, int]] = []
    client = create_redis(redis_url, purpose="pubsub")
    bridge = RedisBridge(client, on_positions=received.extend, on_user_frame=lambda *_: None)
    # Fault injection: the bridge keeps talking to whatever object it was given.
    bridge._pubsub = cast(Any, _AngryPubSub(bridge._pubsub, failures=3))

    await bridge.start()
    try:
        assert bridge.delivering

        async def delivered() -> bool:
            for _ in range(60):
                await publisher.publish(
                    POSITIONS_CHANNEL, encode_positions([("dev-1", 1.0, 2.0, 5)])
                )
                await asyncio.sleep(0.1)
                if received:
                    return True
            return False

        assert await delivered(), "the reader never recovered from the injected failure"
        assert bridge.delivering
    finally:
        await bridge.stop()
        await close_redis(client)


async def test_stopping_does_not_raise_what_killed_the_reader(redis_url: str) -> None:
    client = create_redis(redis_url, purpose="pubsub")
    bridge = RedisBridge(client, on_positions=lambda _: None, on_user_frame=lambda *_: None)
    await bridge.start()
    try:
        # A reader that died for good: shutting down must still be quiet.
        bridge._running = False  # forcing the dead-reader state
        task = bridge._task
        assert task is not None
        await asyncio.wait_for(task, timeout=5)
        bridge._running = True
        assert not bridge.delivering
        await bridge.stop()
    finally:
        await close_redis(client)


async def test_readiness_reports_a_replica_whose_fan_out_is_gone(settings: Settings) -> None:
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            assert (await client.get("/health/ready")).status_code == 200

            gateway = app.state.resources.gateway
            await gateway._bridge.stop()  # simulating a dead reader

            response = await client.get("/health/ready")
            assert response.status_code == 503
            body = response.json()
            assert body["code"] == "not_ready"
            assert "fan-out" in body["detail"]
