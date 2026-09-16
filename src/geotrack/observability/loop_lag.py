"""Event loop lag monitor.

The service promises never to block the loop; this measures whether it keeps that
promise in production instead of relying on code review.
"""

import asyncio
import time

import structlog

from geotrack.observability.metrics import event_loop_lag_seconds

logger = structlog.get_logger(__name__)


class EventLoopLagMonitor:
    def __init__(self, *, interval_s: float = 0.25, warn_after_s: float = 0.1) -> None:
        self._interval_s = interval_s
        self._warn_after_s = warn_after_s
        self._task: asyncio.Task[None] | None = None
        self.last_lag_s = 0.0

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="event-loop-lag-monitor")

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
            started = time.perf_counter()
            await asyncio.sleep(self._interval_s)
            lag = time.perf_counter() - started - self._interval_s
            self.last_lag_s = max(lag, 0.0)
            event_loop_lag_seconds.observe(self.last_lag_s)
            if self.last_lag_s >= self._warn_after_s:
                logger.warning("event loop lag", lag_s=round(self.last_lag_s, 4))
