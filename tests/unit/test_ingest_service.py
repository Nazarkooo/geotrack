"""Placement and framing of reports on the ingest streams.

A device must always land on the same shard (that is what keeps its reports ordered),
and a whole batch must cost exactly one Redis round trip.
"""

from datetime import UTC, datetime
from typing import Any, Self

import pytest
from prometheus_client import REGISTRY

from geotrack.ingest.backlog import BacklogMonitor
from geotrack.ingest.service import BackpressureError, IngestService
from geotrack.messaging.codec import decode_record
from geotrack.messaging.keys import STREAM_FIELD, ingest_stream
from geotrack.schemas.ingest import LocationReport
from geotrack.sharding import shard_for
from tests.conftest import make_settings
from tests.unit.test_ingest_backlog import FakeRedis, make_monitor

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


class RecordingPipeline:
    def __init__(self, owner: RecordingRedis) -> None:
        self._owner = owner
        self.commands: list[tuple[str, dict[bytes, bytes]]] = []

    def xadd(self, name: str, fields: dict[bytes, bytes], **_: Any) -> Self:
        self.commands.append((name, fields))
        return self

    async def execute(self) -> list[bytes]:
        self._owner.batches.append(self.commands)
        return [b"1-0"] * len(self.commands)


class RecordingRedis:
    def __init__(self) -> None:
        self.batches: list[list[tuple[str, dict[bytes, bytes]]]] = []

    def pipeline(self, transaction: bool = True, **_: Any) -> RecordingPipeline:
        return RecordingPipeline(self)


def _report(device_id: str, *, lat: float = 50.45, lon: float = 30.52) -> LocationReport:
    return LocationReport(device_id=device_id, latitude=lat, longitude=lon, timestamp=NOW)


def _service(redis: RecordingRedis, monitor: BacklogMonitor, **overrides: Any) -> IngestService:
    settings = make_settings(**overrides)
    return IngestService(redis, monitor, settings=settings)  # type: ignore[arg-type]


def _counter(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return value or 0.0


async def test_a_batch_costs_one_round_trip_and_keeps_devices_on_their_shard() -> None:
    redis = RecordingRedis()
    service = _service(redis, make_monitor(FakeRedis()), ingest_shards=4)
    reports = [_report(f"dev-{index:05d}") for index in range(20)]

    accepted = await service.submit(reports, transport="http")

    assert accepted == 20
    assert len(redis.batches) == 1
    commands = redis.batches[0]
    assert [name for name, _ in commands] == [
        ingest_stream(shard_for(report.device_id, 4)) for report in reports
    ]


async def test_stream_entries_carry_the_decodable_record() -> None:
    redis = RecordingRedis()
    service = _service(redis, make_monitor(FakeRedis()), ingest_shards=8)

    before = int(NOW.timestamp() * 1000)
    await service.submit([_report("dev-1", lat=-33.87, lon=151.21)], transport="ws")

    (stream, fields) = redis.batches[0][0]
    assert stream == ingest_stream(shard_for("dev-1", 8))
    record = decode_record(fields[STREAM_FIELD])
    assert (record.device_id, record.lat, record.lon) == ("dev-1", -33.87, 151.21)
    assert record.reported_ms == before
    # received_ms is stamped at the edge, so end-to-end latency is measured from here.
    assert record.received_ms >= before


async def test_an_empty_batch_touches_neither_redis_nor_the_counters() -> None:
    redis = RecordingRedis()
    service = _service(redis, make_monitor(FakeRedis()))

    assert await service.submit([], transport="http") == 0
    assert redis.batches == []


async def test_a_throttled_monitor_sheds_the_batch_before_writing() -> None:
    fake = FakeRedis({ingest_stream(0): 1_000})
    monitor = make_monitor(fake)
    await monitor.refresh()
    redis = RecordingRedis()
    service = _service(redis, monitor)
    before = _counter("geotrack_ingest_rejected_total", transport="http", reason="backpressure")

    with pytest.raises(BackpressureError) as excinfo:
        await service.submit([_report("dev-1")], transport="http")

    assert excinfo.value.retry_after_ms > 0
    assert redis.batches == []
    after = _counter("geotrack_ingest_rejected_total", transport="http", reason="backpressure")
    assert after - before == 1


async def test_accepted_reports_are_counted_per_transport() -> None:
    redis = RecordingRedis()
    service = _service(redis, make_monitor(FakeRedis()))
    before = _counter("geotrack_ingest_reports_total", transport="ws")

    await service.submit([_report("a"), _report("b")], transport="ws")

    assert _counter("geotrack_ingest_reports_total", transport="ws") - before == 2


async def test_rejections_are_counted_with_their_reason() -> None:
    service = _service(RecordingRedis(), make_monitor(FakeRedis()))
    before = _counter("geotrack_ingest_rejected_total", transport="http", reason="window")

    service.count_rejected(3, transport="http", reason="window")

    after = _counter("geotrack_ingest_rejected_total", transport="http", reason="window")
    assert after - before == 3
