"""Geofence semantics: which movements produce which alerts, for whom."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from geotrack.db.models import AlertKind
from geotrack.processor.batch import BatchProcessor
from geotrack.settings import Settings
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

SHARD = 3


@pytest.fixture
def processor(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> BatchProcessor:
    return BatchProcessor(session_factory, settings=settings)


async def test_a_device_reporting_inside_a_zone_raises_an_enter_alert(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id, name="depot")

    result = await processor.apply(SHARD, [inside("dev-1")])

    assert [(a.kind, a.zone_id, a.device_id) for a in result.alerts] == [
        (AlertKind.ENTER, zone_id, "dev-1")
    ]
    alert = result.alerts[0]
    assert alert.user_id == user_id
    assert alert.zone_name == "depot"
    assert (round(alert.latitude, 6), round(alert.longitude, 6)) == KYIV
    assert alert.id > 0


async def test_staying_inside_a_zone_raises_no_further_alert(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)

    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(30))])
    second = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(20))])

    assert second.alerts == []
    assert len(second.accepted) == 1


async def test_leaving_a_zone_raises_an_exit_alert(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id)

    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(30))])
    result = await processor.apply(SHARD, [outside("dev-1", at=seconds_ago(20))])

    assert [(a.kind, a.zone_id) for a in result.alerts] == [(AlertKind.EXIT, zone_id)]
    # The exit alert carries where the device actually was when it was found outside.
    assert round(result.alerts[0].latitude, 4) == round(KYIV[0] + 0.1, 4)


async def test_a_device_that_was_never_inside_raises_nothing_on_leaving(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)

    result = await processor.apply(SHARD, [outside("dev-1")])

    assert result.alerts == []
    assert len(result.accepted) == 1


async def test_enter_alerts_can_be_switched_off_per_zone(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id, alert_on_enter=False)

    entering = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(30))])
    leaving = await processor.apply(SHARD, [outside("dev-1", at=seconds_ago(20))])

    assert entering.alerts == []
    # Presence is still tracked, so the exit is still reported.
    assert [(a.kind, a.zone_id) for a in leaving.alerts] == [(AlertKind.EXIT, zone_id)]


async def test_exit_alerts_can_be_switched_off_per_zone(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id, alert_on_exit=False)

    entering = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(30))])
    leaving = await processor.apply(SHARD, [outside("dev-1", at=seconds_ago(20))])

    assert [a.kind for a in entering.alerts] == [AlertKind.ENTER]
    assert leaving.alerts == []


async def test_dwell_alerts_wait_for_the_configured_interval(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id, dwell_alert_interval_s=60)

    entered = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(300))])
    too_soon = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(259))])
    on_time = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(240))])

    assert [a.kind for a in entered.alerts] == [AlertKind.ENTER]
    assert too_soon.alerts == []
    assert [(a.kind, a.zone_id) for a in on_time.alerts] == [(AlertKind.DWELL, zone_id)]


async def test_a_dwell_alert_restarts_the_dwell_clock(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id, dwell_alert_interval_s=60)

    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(300))])
    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(240))])
    too_soon = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(200))])
    again = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(180))])

    assert too_soon.alerts == []
    assert [a.kind for a in again.alerts] == [AlertKind.DWELL]

    async with engine.connect() as conn:
        entered_at, last_alert_at = (
            await conn.execute(
                text(
                    "SELECT entered_at, last_alert_at FROM zone_presence "
                    "WHERE zone_id = :zone_id AND device_id = 'dev-1'"
                ),
                {"zone_id": zone_id},
            )
        ).one()
    # Entering is remembered from the first report; the dwell clock has moved on.
    assert last_alert_at > entered_at


async def test_a_silent_zone_still_paces_its_dwell_reminders(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """With both toggles off the dwell clock starts on entry, not on the first report."""
    user_id = await create_user(engine, "owner")
    await create_zone(
        engine,
        user_id=user_id,
        alert_on_enter=False,
        alert_on_exit=False,
        dwell_alert_interval_s=60,
    )

    entered = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(300))])
    too_soon = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(280))])
    on_time = await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(230))])

    assert (entered.alerts, too_soon.alerts) == ([], [])
    assert [a.kind for a in on_time.alerts] == [AlertKind.DWELL]


async def test_a_device_inside_two_overlapping_zones_raises_two_alerts(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    first = await create_zone(engine, user_id=user_id, name="inner", radius_m=200)
    second = await create_zone(engine, user_id=user_id, name="outer", radius_m=5_000)

    result = await processor.apply(SHARD, [inside("dev-1")])

    assert {(a.kind, a.zone_id) for a in result.alerts} == {
        (AlertKind.ENTER, first),
        (AlertKind.ENTER, second),
    }


async def test_an_alert_reaches_only_the_owner_of_the_zone(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    alice = await create_user(engine, "alice")
    bob = await create_user(engine, "bob")
    await create_zone(engine, user_id=alice, name="alice depot")
    # Bob watches a different city, so the same report cannot touch his zone.
    await create_zone(engine, user_id=bob, lat=49.8397, lon=24.0297, name="bob depot")

    result = await processor.apply(SHARD, [inside("dev-1")])

    assert {a.user_id for a in result.alerts} == {alice}

    async with engine.connect() as conn:
        rows = (await conn.execute(text("SELECT user_id, zone_name FROM alerts ORDER BY id"))).all()
    assert [(row.user_id, row.zone_name) for row in rows] == [(alice, "alice depot")]


async def test_the_exact_predicate_rejects_what_the_prefilter_lets_through(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    """A point 110 m due east of a 100 m zone still falls inside the padded search
    polygon's bounding box, so the GiST prefilter offers it as a candidate. The exact
    geodesic test is what must reject it — otherwise the padding would leak alerts.
    """
    user_id = await create_user(engine, "owner")
    zone_id = await create_zone(engine, user_id=user_id, radius_m=100)

    async with engine.connect() as conn:
        latitude, longitude, prefilter_hit, exact_hit = (
            await conn.execute(
                text(
                    """
                    SELECT ST_Y(probe::geometry), ST_X(probe::geometry),
                           search_area && probe, ST_DWithin(center, probe, radius_m)
                    FROM geozones,
                         LATERAL (
                             SELECT ST_Project(center, 110, radians(90))::geography
                         ) AS p(probe)
                    WHERE id = :zone_id
                    """
                ),
                {"zone_id": zone_id},
            )
        ).one()
    assert (prefilter_hit, exact_hit) == (True, False)

    result = await processor.apply(SHARD, [report("dev-1", latitude, longitude)])

    assert result.alerts == []


async def test_alert_counters_are_labelled_by_kind(
    engine: AsyncEngine, processor: BatchProcessor
) -> None:
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    before_enter = counter("geotrack_processor_alerts_total", kind="enter")
    before_exit = counter("geotrack_processor_alerts_total", kind="exit")

    await processor.apply(SHARD, [inside("dev-1", at=seconds_ago(30))])
    await processor.apply(SHARD, [outside("dev-1", at=seconds_ago(20))])

    assert counter("geotrack_processor_alerts_total", kind="enter") == before_enter + 1
    assert counter("geotrack_processor_alerts_total", kind="exit") == before_exit + 1
