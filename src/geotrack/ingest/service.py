"""Acceptance of device reports.

The edge does three things and nothing else: check the gate, stamp arrival time and
append to the shard stream. No database connection is involved, so ingestion latency
is independent of how busy PostGIS is.
"""

from collections.abc import Sequence
from typing import Literal

from redis.asyncio import Redis

from geotrack.clock import now_ms, to_epoch_ms
from geotrack.ingest.backlog import BacklogMonitor
from geotrack.messaging.codec import LocationRecord, encode_record
from geotrack.messaging.keys import STREAM_FIELD, ingest_stream
from geotrack.observability.metrics import ingest_rejected_total, ingest_reports_total
from geotrack.schemas.ingest import LocationReport
from geotrack.settings import Settings
from geotrack.sharding import shard_for

type Transport = Literal["http", "ws"]
type RejectReason = Literal["validation", "backpressure", "window", "auth"]


class BackpressureError(Exception):
    """The processors are too far behind to accept more reports right now."""

    def __init__(self, retry_after_ms: int) -> None:
        super().__init__(f"ingestion is throttled, retry in {retry_after_ms} ms")
        self.retry_after_ms = retry_after_ms


class IngestService:
    def __init__(self, redis: Redis, backlog: BacklogMonitor, *, settings: Settings) -> None:
        self._redis = redis
        self._backlog = backlog
        self._shards = settings.ingest_shards

    async def submit(self, reports: Sequence[LocationReport], *, transport: Transport) -> int:
        """Append a validated batch to its shard streams; returns how many were taken."""
        if not reports:
            return 0
        if self._backlog.throttled:
            self.count_rejected(len(reports), transport=transport, reason="backpressure")
            raise BackpressureError(self._backlog.retry_after_ms)

        # One timestamp for the whole batch: it marks arrival at the edge, and the
        # processor subtracts it to report true end-to-end latency.
        received_ms = now_ms()
        # No MULTI/EXEC: the entries are independent, so an all-or-nothing batch would
        # buy nothing while costing a server-side transaction.
        pipe = self._redis.pipeline(transaction=False)
        for report in reports:
            record = LocationRecord(
                device_id=report.device_id,
                lat=report.latitude,
                lon=report.longitude,
                reported_ms=to_epoch_ms(report.timestamp),
                received_ms=received_ms,
            )
            shard = shard_for(report.device_id, self._shards)
            pipe.xadd(ingest_stream(shard), {STREAM_FIELD: encode_record(record)})
        await pipe.execute()

        ingest_reports_total.labels(transport).inc(len(reports))
        return len(reports)

    def count_rejected(self, count: int, *, transport: Transport, reason: RejectReason) -> None:
        """Record reports the system refused, so rejection rates are visible per cause.

        The unit is reports, the same unit accepted traffic is counted in, so the two can
        be compared. A refusal decided before the payload was read cannot know how many
        reports it turned away and must not guess: the only one of those left on this
        counter is ``auth``, which is per attempt because a wrong key is a property of the
        device rather than of a batch, and is the single signal a misconfigured fleet
        produces on the websocket transport.
        """
        if count > 0:
            ingest_rejected_total.labels(transport, reason).inc(count)
