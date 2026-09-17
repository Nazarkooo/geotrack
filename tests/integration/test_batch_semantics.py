"""Batch mechanics: ordering, idempotency, storage and the retry path.

These are the properties that make the processor safe to restart, safe to run twice by
accident, and safe to point at a stream that replays entries after a crash.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from geotrack.clock import to_epoch_ms
from geotrack.db.models import AlertKind
from geotrack.ids import new_uuid
from geotrack.processor.batch import BatchProcessor
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.processor_fixtures import (
    KYIV,
    counter,
    create_user,
    create_zone,
    inside,
    outside,
    report,
    seconds_ago,
)

SHARD = 5


@pytest.fixture
def processor(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> BatchProcessor:
    return BatchProcessor(session_factory, settings=settings)


async def stored_position(engine: AsyncEngine, device_id: str) -> tuple[float, float, datetime]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT ST_Y(position::geometry) AS lat, ST_X(position::geometry) AS lon, "
                    "reported_at FROM device_positions WHERE device_id = :device_id"
                ),
                {"device_id": device_id},
            )
        ).one()
    return row.lat, row.lon, row.reported_at


async def count_of(engine: AsyncEngine, table: str) -> int:
    async with engine.connect() as conn:
        return cast(int, await conn.scalar(text(f"SELECT count(*) FROM {table}")))  # noqa: S608


async def test_an_older_report_never_overwrites_a_newer_one(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    before = counter("geotrack_processor_stale_reports_total")

    newest = inside("dev-1", at=seconds_ago(10))
    await processor.apply(SHARD, [newest])
    late = await processor.apply(SHARD, [outside("dev-1", at=seconds_ago(60))])

    assert late.accepted == []
    assert late.alerts == []
    assert late.stale == 1
    assert counter("geotrack_processor_stale_reports_total") == before + 1

    latitude, _, reported_at = await stored_position(engine, "dev-1")
    assert round(latitude, 6) == KYIV[0]
    assert to_epoch_ms(reported_at) == newest.reported_ms


async def test_a_report_with_the_same_timestamp_is_treated_as_stale(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """Equal timestamps are how a replayed entry arrives; it must change nothing."""
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    record = inside("dev-1", at=seconds_ago(10))

    await processor.apply(SHARD, [record])
    repeat = await processor.apply(SHARD, [record])

    assert (repeat.accepted, repeat.alerts, repeat.stale) == ([], [], 1)


async def test_a_device_reporting_twice_in_one_batch_keeps_only_its_newest_report(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    newest = inside("dev-1", at=seconds_ago(10))
    before = counter("geotrack_processor_stale_reports_total")

    result = await processor.apply(
        SHARD, [outside("dev-1", at=seconds_ago(60)), newest, outside("dev-1", at=seconds_ago(30))]
    )

    assert [item[0] for item in result.accepted] == ["dev-1"]
    assert result.accepted[0][3] == newest.reported_ms
    assert [a.kind for a in result.alerts] == [AlertKind.ENTER]
    # Both duplicates are archived: the raw track is the stream, not the accepted subset.
    assert await count_of(engine, "location_history") == 3
    # A report dropped because a newer one was stored is exactly what the metric counts,
    # whether the newer one came from an earlier batch or from this one.
    assert result.stale == 2
    assert counter("geotrack_processor_stale_reports_total") == before + 2


async def test_in_batch_duplicates_are_counted_as_stale_under_backlog(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """Backlog is the only time the metric matters, and the only time duplicates appear."""
    before = counter("geotrack_processor_stale_reports_total")
    batch = [report(f"dev-{index % 4}", *KYIV, at=seconds_ago(120 - index)) for index in range(20)]

    result = await processor.apply(SHARD, batch)

    assert len(result.accepted) == 4
    assert result.stale == 16
    assert counter("geotrack_processor_stale_reports_total") == before + 16


async def test_replaying_a_batch_produces_no_new_alerts_and_no_new_history(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    batch = [inside(f"dev-{index}", at=seconds_ago(20)) for index in range(25)]

    first = await processor.apply(SHARD, batch)
    second = await processor.apply(SHARD, batch)

    assert len(first.alerts) == 25
    assert second.alerts == []
    assert second.accepted == []
    assert await count_of(engine, "alerts") == 25
    assert await count_of(engine, "location_history") == 25


async def test_presence_is_created_on_entry_and_removed_on_exit(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id)

    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(40))])
    async with engine.connect() as conn:
        entered = (
            await conn.execute(
                text(
                    "SELECT zone_id, device_id, entered_at, last_seen_at, last_alert_at "
                    "FROM zone_presence"
                )
            )
        ).all()
    assert [(row.zone_id, row.device_id) for row in entered] == [(zone_id, "dev-1")]
    assert entered[0].entered_at == entered[0].last_seen_at == entered[0].last_alert_at

    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(30))])
    async with engine.connect() as conn:
        staying = (
            await conn.execute(text("SELECT entered_at, last_seen_at FROM zone_presence"))
        ).one()
    assert staying.last_seen_at > staying.entered_at

    await processor.apply(SHARD, [outside("dev-1", at=seconds_ago(20))])
    assert await count_of(engine, "zone_presence") == 0


async def test_history_lands_in_the_partition_for_its_own_day(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """A row is routed by its own timestamp, which is what the expectation is built from.

    Reading the clock again at assertion time would make this test fail on its own,
    with no code change, for any run that crosses UTC midnight — hence the deliberate
    case a fraction of a second before one.
    """
    moments = {
        "dev-now": datetime.now(UTC),
        "dev-yesterday": datetime.now(UTC) - timedelta(days=1),
        "dev-before-midnight": (datetime.now(UTC) - timedelta(days=1)).replace(
            hour=23, minute=59, second=59, microsecond=950_000
        ),
    }

    await processor.apply(
        SHARD, [report(device_id, *KYIV, at=at) for device_id, at in moments.items()]
    )

    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT device_id, tableoid::regclass::text AS partition "
                    "FROM location_history ORDER BY device_id"
                )
            )
        ).all()
    partitions = {row.device_id: row.partition for row in rows}
    assert partitions == {
        device_id: f"location_history_p{at:%Y%m%d}" for device_id, at in moments.items()
    }


async def test_history_is_readable_through_the_parent_table(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    await processor.apply(
        SHARD,
        [
            report("dev-1", *KYIV, at=seconds_ago(60)),
            report("dev-1", KYIV[0] + 0.001, KYIV[1], at=seconds_ago(30)),
        ],
    )

    async with engine.connect() as conn:
        track = (
            await conn.execute(
                text(
                    "SELECT ST_Y(position::geometry) AS lat FROM location_history "
                    "WHERE device_id = 'dev-1' ORDER BY reported_at"
                )
            )
        ).all()
    assert [round(row.lat, 4) for row in track] == [round(KYIV[0], 4), round(KYIV[0] + 0.001, 4)]


async def test_reports_outside_the_retention_window_are_rejected_not_stored(
    engine: AsyncEngine, session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    """History has no partition for them, so applying them would wedge the shard."""
    processor = BatchProcessor(session_factory, settings=settings)
    ancient = report("dev-old", *KYIV, at=datetime.now(UTC) - timedelta(days=90))
    far_future = report("dev-future", *KYIV, at=datetime.now(UTC) + timedelta(hours=3))
    fine = inside("dev-ok")

    result = await processor.apply(SHARD, [ancient, fine, far_future])

    assert [r.record.device_id for r in result.rejected] == ["dev-old", "dev-future"]
    assert [r.index for r in result.rejected] == [0, 2]
    assert "older than the history retention window" in result.rejected[0].reason
    assert "too far in the future" in result.rejected[1].reason
    assert [item[0] for item in result.accepted] == ["dev-ok"]
    assert await count_of(engine, "location_history") == 1


async def test_a_batch_of_only_rejected_reports_touches_nothing(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    ancient = report("dev-old", *KYIV, at=datetime.now(UTC) - timedelta(days=90))

    result = await processor.apply(SHARD, [ancient])

    assert len(result.rejected) == 1
    assert (result.accepted, result.alerts, result.stale) == ([], [], 0)
    assert await count_of(engine, "device_positions") == 0


async def test_an_empty_batch_is_a_no_op(processor: BatchProcessor) -> None:
    result = await processor.apply(SHARD, [])

    assert (result.accepted, result.alerts, result.stale, result.rejected) == ([], [], 0, [])


async def test_the_shard_lock_keeps_two_concurrent_batches_from_double_alerting(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """Two owners of one shard overlap while a lease changes hands.

    The per-shard transaction lock makes their batches run in sequence, so the second
    one sees the first one's presence row instead of raising a second entry alert.
    """
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)

    older, newer = inside("dev-1", at=seconds_ago(40)), inside("dev-1", at=seconds_ago(20))
    results = await asyncio.gather(processor.apply(SHARD, [older]), processor.apply(SHARD, [newer]))

    assert sum(len(result.alerts) for result in results) == 1
    assert await count_of(engine, "alerts") == 1
    _, _, reported_at = await stored_position(engine, "dev-1")
    assert to_epoch_ms(reported_at) == newer.reported_ms


class _BrokenSession:
    """A session whose transaction fails before it can execute anything."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def __aenter__(self) -> Self:
        raise self._error

    async def __aexit__(self, *_: object) -> bool:
        return False


class _FailingFactory:
    """Wraps the real factory and breaks the first ``failures`` transactions."""

    def __init__(
        self, inner: async_sessionmaker[AsyncSession], error: BaseException, failures: int
    ) -> None:
        self._inner = inner
        self._error = error
        self._failures = failures
        self.attempts = 0

    def __call__(self) -> Any:
        self.attempts += 1
        if self.attempts <= self._failures:
            return _BrokenSession(self._error)
        return self._inner()


async def captured_foreign_key_error(engine: AsyncEngine) -> IntegrityError:
    """A real driver error, so the retry logic is tested against real SQLSTATE plumbing."""
    with pytest.raises(IntegrityError) as excinfo:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO geozones (id, user_id, name, color, center, radius_m) "
                    "VALUES (:id, :user_id, 'z', '#3fb1ff', "
                    "ST_SetSRID(ST_MakePoint(30.5, 50.4), 4326)::geography, 100)"
                ),
                {"id": new_uuid(), "user_id": new_uuid()},
            )
    return excinfo.value


async def test_a_zone_deleted_mid_batch_is_retried_and_the_batch_completes(
    engine: AsyncEngine, session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    factory = _FailingFactory(session_factory, await captured_foreign_key_error(engine), failures=2)
    processor = BatchProcessor(cast(async_sessionmaker[AsyncSession], factory), settings=settings)
    before = counter("geotrack_processor_retries_total", sqlstate="23503")

    result = await processor.apply(SHARD, [inside("dev-1")])

    assert factory.attempts == 3
    assert [a.kind for a in result.alerts] == [AlertKind.ENTER]
    assert counter("geotrack_processor_retries_total", sqlstate="23503") == before + 2


async def test_retries_give_up_after_the_attempt_cap(
    engine: AsyncEngine, session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    factory = _FailingFactory(
        session_factory, await captured_foreign_key_error(engine), failures=99
    )
    processor = BatchProcessor(
        cast(async_sessionmaker[AsyncSession], factory), settings=settings, max_attempts=3
    )

    with pytest.raises(IntegrityError):
        await processor.apply(SHARD, [inside("dev-1")])

    assert factory.attempts == 3


async def test_an_error_that_a_retry_cannot_fix_is_raised_immediately(
    engine: AsyncEngine, session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO users (id, username) VALUES (:id, 'clash')"), {"id": new_uuid()}
        )
    with pytest.raises(IntegrityError) as excinfo:
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO users (id, username) VALUES (:id, 'clash')"), {"id": new_uuid()}
            )

    factory = _FailingFactory(session_factory, excinfo.value, failures=99)
    processor = BatchProcessor(cast(async_sessionmaker[AsyncSession], factory), settings=settings)

    with pytest.raises(DBAPIError):
        await processor.apply(SHARD, [inside("dev-1")])

    assert factory.attempts == 1


async def test_deleting_a_zone_while_batches_run_never_breaks_the_processor(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """The live race, rather than an injected error: zones come and go under load."""
    user_id = await create_user(engine, "owner")
    zone_ids = [
        await create_zone(engine, user_id=user_id, name=f"zone {index}") for index in range(6)
    ]

    async def drop_zones() -> None:
        for zone_id in zone_ids:
            async with engine.begin() as conn:
                await conn.execute(text("DELETE FROM geozones WHERE id = :id"), {"id": zone_id})
            await asyncio.sleep(0)

    dropper = asyncio.create_task(drop_zones())
    results = await asyncio.gather(
        *(
            processor.apply(SHARD, [inside(f"dev-{index}", at=seconds_ago(60 - index))])
            for index in range(6)
        )
    )
    await dropper

    assert all(len(result.accepted) == 1 for result in results)
    # Cascading deletes take the presence rows with them; nothing may be left pointing
    # at a zone that no longer exists.
    async with engine.connect() as conn:
        orphans = await conn.scalar(
            text(
                "SELECT count(*) FROM zone_presence p "
                "LEFT JOIN geozones z ON z.id = p.zone_id WHERE z.id IS NULL"
            )
        )
    assert orphans == 0


async def test_a_long_retention_window_still_rejects_reports_from_the_far_future(
    session_factory: async_sessionmaker[AsyncSession], migrated_database: str, redis_url: str
) -> None:
    settings = make_settings(
        database_url=migrated_database,
        redis_url=redis_url,
        ingest_max_future_skew_s=0,
        history_retention_days=365,
    )
    processor = BatchProcessor(session_factory, settings=settings)

    result = await processor.apply(
        SHARD, [report("dev-1", *KYIV, at=datetime.now(UTC) + timedelta(minutes=1))]
    )

    assert [r.record.device_id for r in result.rejected] == ["dev-1"]


async def test_a_device_id_the_schema_cannot_hold_is_rejected_not_applied(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """The check constraint would fail for every retry, so the batch must not carry it."""
    result = await processor.apply(
        SHARD, [report("d" * 65, *KYIV), report("", *KYIV), inside("dev-ok")]
    )

    assert [r.index for r in result.rejected] == [0, 1]
    assert all("device_id must be between" in r.reason for r in result.rejected)
    assert [item[0] for item in result.accepted] == ["dev-ok"]
    assert await count_of(engine, "location_history") == 1


async def test_a_device_id_no_alert_frame_could_carry_is_rejected(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """The check constraint would take it, but nothing downstream could describe it.

    A device id outside the documented shape reaches the database happily and then
    breaks on the way out, after the row is committed and can no longer be refused.
    """
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)

    result = await processor.apply(
        SHARD, [report("dev 1", *KYIV), report("dev/1", *KYIV), inside("dev-ok")]
    )

    assert [r.record.device_id for r in result.rejected] == ["dev 1", "dev/1"]
    assert all("device_id" in r.reason for r in result.rejected)
    assert [item[0] for item in result.accepted] == ["dev-ok"]
    assert await count_of(engine, "location_history") == 1
