"""Fan-out of a committed batch.

Publishing happens after the transaction commits, never before: a subscriber must
never be told about a position or an alert that a rollback would take back. The
gateway receives alerts as finished client frames, so a fan-out to a thousand
websocket sessions serialises the alert once, here, instead of once per session.

Everything in this module is best effort by construction. By the time it runs the
batch is durable, so the only thing a failure here can still cost is a broadcast —
and anything that escaped would cost the whole batch instead, because the consumer
would leave its entries pending and replay them as stale.
"""

import structlog
from redis.asyncio import Redis

from geotrack.messaging.codec import encode_positions
from geotrack.messaging.keys import POSITIONS_CHANNEL, user_channel
from geotrack.processor.batch import AlertRow, BatchResult
from geotrack.realtime.protocol import alert_frame
from geotrack.schemas.alerts import AlertOut, AlertZoneRef

logger = structlog.get_logger(__name__)

# Building an alert frame costs a few microseconds; a zone drawn over a depot can put
# thousands of them in one batch. Flushing in chunks keeps the run of synchronous work
# between two awaits in the sub-millisecond range, so a burst of alerts cannot delay a
# websocket tick, and keeps the pipeline buffer bounded.
PUBLISH_CHUNK = 256


def alert_payload(row: AlertRow) -> AlertOut:
    """The REST representation of an alert.

    Live and backfilled alerts share one shape so the browser has one code path for
    both, which is also why this goes through the response model rather than an
    ad-hoc dict.
    """
    return AlertOut(
        id=row.id,
        kind=row.kind,
        zone=AlertZoneRef(id=row.zone_id, name=row.zone_name),
        device_id=row.device_id,
        latitude=row.latitude,
        longitude=row.longitude,
        occurred_at=row.occurred_at,
        created_at=row.created_at,
    )


class ResultPublisher:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(self, result: BatchResult) -> None:
        """Broadcast a committed batch. Never raises."""
        if result.is_empty:
            return
        try:
            await self._fan_out(result)
        except Exception as exc:
            # Dropping the broadcast costs a client one refresh (positions arrive again
            # on the next report, alerts through the REST backfill), while failing here
            # would replay the whole batch.
            logger.warning(
                "failed to publish batch result",
                error=str(exc),
                positions=len(result.accepted),
                alerts=len(result.alerts),
            )

    async def _fan_out(self, result: BatchResult) -> None:
        pending: list[tuple[str, bytes]] = []
        if result.accepted:
            pending.append((POSITIONS_CHANNEL, encode_positions(result.accepted)))
        for alert in result.alerts:
            frame = _frame_for(alert)
            if frame is not None:
                pending.append((user_channel(alert.user_id), frame))
            if len(pending) >= PUBLISH_CHUNK:
                await self._flush(pending)
                pending = []
        if pending:
            await self._flush(pending)

    async def _flush(self, frames: list[tuple[str, bytes]]) -> None:
        async with self._redis.pipeline(transaction=False) as pipe:
            for channel, payload in frames:
                pipe.publish(channel, payload)
            await pipe.execute()


def _frame_for(alert: AlertRow) -> bytes | None:
    """Render one alert, or ``None`` when it cannot be rendered.

    The stream promises less about a ``device_id`` than the client frame does — the
    codec only asks for a non-empty string, the REST model for the documented shape —
    so the two can in principle disagree on a row that is already committed. Isolating
    the failure per alert is what keeps that disagreement from costing the whole batch
    its broadcast.
    """
    try:
        return alert_frame(alert_payload(alert))
    except Exception as exc:
        logger.error(
            "could not build an alert frame",
            error=str(exc),
            alert_id=alert.id,
            device_id=alert.device_id,
        )
        return None
