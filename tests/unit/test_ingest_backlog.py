"""The backlog monitor is the only thing between a burst of devices and an unbounded
Redis stream, so its state machine is pinned down here against a fake client.
"""

import asyncio
from typing import Any, Self

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from geotrack.ingest.backlog import BacklogMonitor
from geotrack.messaging.keys import ingest_stream


class FakePipeline:
    def __init__(self, owner: FakeRedis) -> None:
        self._owner = owner
        self.requested: list[str] = []

    def xlen(self, name: str) -> Self:
        self.requested.append(name)
        return self

    async def execute(self) -> list[int]:
        if self._owner.fail:
            raise RedisConnectionError("redis is unreachable")
        self._owner.executed.append(list(self.requested))
        return [self._owner.lengths.get(name, 0) for name in self.requested]


class FakeRedis:
    def __init__(self, lengths: dict[str, int] | None = None) -> None:
        self.lengths = lengths or {}
        self.executed: list[list[str]] = []
        self.fail = False

    def pipeline(self, transaction: bool = True, **_: Any) -> FakePipeline:
        return FakePipeline(self)


def make_monitor(redis: FakeRedis, **overrides: Any) -> BacklogMonitor:
    options: dict[str, Any] = {"shards": 4, "high": 100, "low": 40, "poll_ms": 10}
    options.update(overrides)
    return BacklogMonitor(redis, **options)  # type: ignore[arg-type]


async def test_sums_every_shard_in_one_round_trip() -> None:
    redis = FakeRedis({ingest_stream(shard): 5 for shard in range(4)})
    monitor = make_monitor(redis)

    total = await monitor.refresh()

    assert total == 20
    assert monitor.backlog == 20
    assert redis.executed == [[ingest_stream(shard) for shard in range(4)]]


async def test_throttles_above_the_high_watermark_and_stays_closed_until_low() -> None:
    redis = FakeRedis()
    monitor = make_monitor(redis)

    redis.lengths = {ingest_stream(0): 100}
    await monitor.refresh()
    assert monitor.throttled is True

    # Hysteresis: still throttled while the backlog sits between the watermarks.
    redis.lengths = {ingest_stream(0): 60}
    await monitor.refresh()
    assert monitor.throttled is True

    redis.lengths = {ingest_stream(0): 40}
    await monitor.refresh()
    assert monitor.throttled is False


async def test_a_throttled_monitor_asks_for_a_retry_no_sooner_than_it_can_reopen() -> None:
    # The number goes to the device as ``retry_after_ms``. Advising a retry sooner than
    # the next sample would send the whole fleet back before anything can have changed.
    monitor = make_monitor(FakeRedis({ingest_stream(0): 500}), poll_ms=800)
    await monitor.refresh()

    assert monitor.throttled is True
    assert monitor.retry_after_ms >= 1_000
    assert monitor.retry_after_ms >= 800


async def test_a_redis_failure_keeps_the_last_known_state() -> None:
    redis = FakeRedis({ingest_stream(0): 500})
    monitor = make_monitor(redis)
    await monitor.refresh()
    redis.fail = True

    with pytest.raises(RedisConnectionError):
        await monitor.refresh()

    assert monitor.throttled is True
    assert monitor.backlog == 500


async def test_background_loop_tracks_changes_and_survives_failures() -> None:
    redis = FakeRedis({ingest_stream(0): 0})
    monitor = make_monitor(redis)
    await monitor.start()
    try:
        assert monitor.throttled is False
        redis.fail = True
        redis.lengths = {ingest_stream(0): 500}
        await asyncio.sleep(0.05)
        assert monitor.throttled is False  # a failed poll must not change the verdict

        redis.fail = False
        await asyncio.sleep(0.1)  # ~10 poll intervals
        assert monitor.throttled is True
    finally:
        await monitor.stop()


async def test_rejects_watermarks_that_cannot_produce_hysteresis() -> None:
    with pytest.raises(ValueError, match="low"):
        make_monitor(FakeRedis(), high=10, low=10)
