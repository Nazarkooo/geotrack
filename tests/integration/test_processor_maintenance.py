"""Partition maintenance: the history table always has somewhere to put a report."""

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from geotrack.messaging.keys import MAINTENANCE_LEASE_KEY
from geotrack.processor.maintenance import DAYS_AHEAD, LEASE_TTL_MS, PartitionMaintenance


async def partition_days(engine: AsyncEngine) -> set[str]:
    async with engine.connect() as conn:
        names = (
            await conn.execute(
                text(
                    "SELECT c.relname FROM pg_inherits i "
                    "JOIN pg_class c ON c.oid = i.inhrelid "
                    "JOIN pg_class p ON p.oid = i.inhparent "
                    "WHERE p.relname = 'location_history'"
                )
            )
        ).scalars()
    return {name.removeprefix("location_history_p") for name in names}


def day(offset: int) -> str:
    return f"{datetime.now(UTC) + timedelta(days=offset):%Y%m%d}"


@pytest.fixture
def maintenance(
    session_factory: async_sessionmaker[AsyncSession], redis_client: Redis
) -> PartitionMaintenance:
    return PartitionMaintenance(
        session_factory, redis_client, retention_days=7, instance_id="alice"
    )


async def test_the_window_is_covered_from_retention_to_two_days_ahead(
    engine: AsyncEngine, maintenance: PartitionMaintenance
) -> None:
    outcome = await maintenance.run_once()

    assert outcome.led is True
    days = await partition_days(engine)
    assert {day(offset) for offset in range(-7, DAYS_AHEAD + 1)} <= days


async def test_a_second_round_creates_nothing_new(
    maintenance: PartitionMaintenance, redis_client: Redis
) -> None:
    await maintenance.run_once()
    await redis_client.delete(MAINTENANCE_LEASE_KEY)

    outcome = await maintenance.run_once()

    assert (outcome.led, outcome.created) == (True, 0)


async def test_only_one_replica_does_the_work_per_round(
    session_factory: async_sessionmaker[AsyncSession], redis_client: Redis
) -> None:
    alice = PartitionMaintenance(
        session_factory, redis_client, retention_days=7, instance_id="alice"
    )
    bob = PartitionMaintenance(session_factory, redis_client, retention_days=7, instance_id="bob")

    first = await alice.run_once()
    second = await bob.run_once()

    assert (first.led, second.led) == (True, False)


async def test_partitions_older_than_the_retention_window_are_dropped(
    engine: AsyncEngine, maintenance: PartitionMaintenance
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "SELECT geotrack_ensure_history_partitions("
                "(now() AT TIME ZONE 'UTC')::date - 30, (now() AT TIME ZONE 'UTC')::date - 25)"
            )
        )
    assert day(-30) in await partition_days(engine)

    outcome = await maintenance.run_once()

    assert outcome.dropped >= 6
    days = await partition_days(engine)
    assert day(-30) not in days
    assert day(-7) in days


async def test_the_lease_is_held_for_the_length_of_one_round(
    maintenance: PartitionMaintenance, redis_client: Redis
) -> None:
    await maintenance.run_once()

    ttl_ms = await redis_client.pttl(MAINTENANCE_LEASE_KEY)
    assert 500_000 < ttl_ms <= LEASE_TTL_MS


async def test_a_round_that_fails_hands_the_lead_straight_back(
    session_factory: async_sessionmaker[AsyncSession], redis_client: Redis
) -> None:
    """Taking the lead is a promise to do the round, not a claim on the next ten minutes.

    A replica that dies on its first round — the database is not up yet — would otherwise
    leave the fleet with a leader that never ran, and nobody creating partitions until
    the lease expires on its own.
    """

    class _NoDatabase:
        def __call__(self) -> object:
            raise ConnectionRefusedError("the database is not up yet")

    maintenance = PartitionMaintenance(
        cast(async_sessionmaker[AsyncSession], _NoDatabase()),
        redis_client,
        retention_days=7,
        instance_id="alice",
    )

    with pytest.raises(ConnectionRefusedError):
        await maintenance.run_once()

    assert await redis_client.get(MAINTENANCE_LEASE_KEY) is None
    # And the next replica gets its turn straight away.
    successor = PartitionMaintenance(
        session_factory, redis_client, retention_days=7, instance_id="bob"
    )
    assert (await successor.run_once()).led is True


async def test_the_lease_key_and_its_lifetime_can_be_chosen(
    session_factory: async_sessionmaker[AsyncSession], redis_client: Redis
) -> None:
    maintenance = PartitionMaintenance(
        session_factory,
        redis_client,
        retention_days=7,
        instance_id="alice",
        lease_key="geo:lease:maintenance:test",
        ttl_ms=30_000,
    )

    assert (await maintenance.run_once()).led is True

    assert await redis_client.get(MAINTENANCE_LEASE_KEY) is None
    assert 0 < await redis_client.pttl("geo:lease:maintenance:test") <= 30_000
