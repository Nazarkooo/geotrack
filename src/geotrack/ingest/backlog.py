"""Queue-depth backpressure.

Devices are faster than PostGIS, so the honest question is not "are we busy?" but
"how far behind are the processors?". The answer is the number of entries still
sitting on the ingest streams, which this monitor samples in the background: the
request path only ever reads an in-memory flag, never Redis.
"""

import asyncio

import structlog
from redis.asyncio import Redis

from geotrack.messaging.keys import ingest_stream
from geotrack.observability.metrics import ingest_backlog, ingest_throttled

logger = structlog.get_logger(__name__)


class BacklogMonitor:
    """Tracks Σ XLEN over the shard streams and opens/closes the ingest gate.

    Two watermarks rather than one: a single threshold makes a system at the limit
    flap between accepting and rejecting on every poll, which is worse for a device
    than a clear "stop" followed by a clear "go".
    """

    def __init__(self, redis: Redis, *, shards: int, high: int, low: int, poll_ms: int) -> None:
        if low >= high:
            raise ValueError("backlog low watermark must be below the high watermark")
        self._redis = redis
        self._streams = [ingest_stream(shard) for shard in range(shards)]
        self._high = high
        self._low = low
        self._poll_s = poll_ms / 1000.0
        # Read on every request and written only by the poller, so a request never waits
        # on Redis to find out whether it may proceed.
        self._throttled = False
        self._task: asyncio.Task[None] | None = None
        self.backlog = 0

    @property
    def throttled(self) -> bool:
        return self._throttled

    @property
    def retry_after_ms(self) -> int:
        """How long a rejected client should wait before trying again."""
        return max(1_000, int(self._poll_s * 2_000))

    async def refresh(self) -> int:
        """Sample every shard in one pipeline and apply the watermarks."""
        pipe = self._redis.pipeline(transaction=False)
        for stream in self._streams:
            pipe.xlen(stream)
        total = sum(int(length) for length in await pipe.execute())

        self.backlog = total
        ingest_backlog.set(total)
        if total >= self._high and not self._throttled:
            logger.warning("ingest throttled", backlog=total, high=self._high)
            self._throttled = True
        elif total <= self._low and self._throttled:
            logger.info("ingest resumed", backlog=total, low=self._low)
            self._throttled = False
        ingest_throttled.set(1 if self._throttled else 0)
        return total

    async def start(self) -> None:
        if self._task is not None:
            return
        # Sample once up front so the first request sees a real number rather than 0.
        try:
            await self.refresh()
        except Exception as exc:
            logger.warning("initial backlog sample failed", error=str(exc))
        self._task = asyncio.create_task(self._run(), name="ingest-backlog-monitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._poll_s)
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Keep the previous verdict: a blind guess either drops traffic the
                # system could still take, or floods a queue nobody is draining.
                logger.warning("backlog sample failed", error=str(exc))
