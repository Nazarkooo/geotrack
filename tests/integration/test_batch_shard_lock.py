"""The per-shard handover lock, against the engine the processor really runs with.

The shared test engine is built without ``statement_timeout``, so nothing else in the
suite would ever see the lock wait being cancelled. These tests build the engine the way
``ProcessorService`` does, because the whole point of the lock is that it is allowed to
wait for the other owner of a shard while everything around it is not.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from geotrack.db.engine import create_engine, create_session_factory
from geotrack.processor.batch import (
    LOCK_NOT_AVAILABLE,
    LOCK_WAIT_STATEMENTS,
    BatchProcessor,
    take_shard_lock,
)
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.processor_fixtures import counter, create_user, create_zone, inside

SHARD = 6
STATEMENT_TIMEOUT_MS = 1_000
LOCK_SQL = text("SELECT pg_advisory_xact_lock(hashtext('geotrack.shard'), CAST(:shard AS int))")


@pytest.fixture
def strict_settings(migrated_database: str, redis_url: str) -> Settings:
    return make_settings(
        database_url=migrated_database,
        redis_url=redis_url,
        db_statement_timeout_ms=STATEMENT_TIMEOUT_MS,
    )


@pytest.fixture
async def strict_sessions(
    strict_settings: Settings,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(strict_settings, pool_size=4, application_name="geotrack-test-processor")
    yield create_session_factory(engine)
    await engine.dispose()


@asynccontextmanager
async def the_other_owner_holding_the_lock(engine: AsyncEngine) -> AsyncIterator[None]:
    """A second replica in the middle of its own batch for this shard."""
    async with engine.connect() as conn:
        await conn.execute(LOCK_SQL, {"shard": SHARD})
        try:
            yield
        finally:
            await conn.rollback()


async def test_the_lock_waits_for_the_other_owner_instead_of_being_cancelled(
    engine: AsyncEngine,
    strict_settings: Settings,
    strict_sessions: async_sessionmaker[AsyncSession],
) -> None:
    """A thousand reports against heavily overlapping zones take about a second to apply.

    A handover on top of that means waiting longer than one statement's budget, and the
    lock that exists to serialise the handover must not be the first thing to break.
    """
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    processor = BatchProcessor(strict_sessions, settings=strict_settings)
    held_s = 2.0
    assert held_s > STATEMENT_TIMEOUT_MS / 1_000
    assert held_s < LOCK_WAIT_STATEMENTS * STATEMENT_TIMEOUT_MS / 1_000

    async with the_other_owner_holding_the_lock(engine):
        applying = asyncio.create_task(processor.apply(SHARD, [inside("dev-1")]))
        await asyncio.sleep(held_s)
        assert applying.done() is False

    result = await asyncio.wait_for(applying, timeout=10)

    assert [item[0] for item in result.accepted] == ["dev-1"]
    assert [alert.device_id for alert in result.alerts] == ["dev-1"]


async def test_the_batch_runs_under_its_own_budget_again_once_the_lock_is_taken(
    strict_sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Lifting the budget for the wait must not lift it for the work that follows."""
    async with strict_sessions() as session, session.begin():
        await take_shard_lock(session, SHARD, wait_ms=30_000, statement_ms=750)

        assert await session.scalar(text("SELECT current_setting('statement_timeout')")) == "750ms"
        # Left armed on purpose: a row-lock conflict in the statements that follow comes
        # back as a retry rather than as an unbounded wait.
        assert await session.scalar(text("SELECT current_setting('lock_timeout')")) == "30s"


async def test_the_connection_goes_back_to_the_pool_with_the_budget_it_arrived_with(
    strict_sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Both settings are transaction-local, so nothing leaks into the next borrower."""
    async with strict_sessions() as session, session.begin():
        await take_shard_lock(session, SHARD, wait_ms=30_000, statement_ms=750)

    async with strict_sessions() as session, session.begin():
        assert await session.scalar(text("SELECT current_setting('statement_timeout')")) == "1s"
        assert await session.scalar(text("SELECT current_setting('lock_timeout')")) == "0"


async def test_an_owner_that_never_lets_go_is_retried_rather_than_failing_the_batch(
    engine: AsyncEngine,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Past the budget the other side is wedged, and backing off beats waiting."""
    user_id = await create_user(engine, "owner")
    await create_zone(engine, user_id=user_id)
    processor = BatchProcessor(session_factory, settings=settings, lock_wait_ms=200)
    before = counter("geotrack_processor_retries_total", sqlstate=LOCK_NOT_AVAILABLE)

    async with the_other_owner_holding_the_lock(engine):
        applying = asyncio.create_task(processor.apply(SHARD, [inside("dev-1")]))
        await asyncio.sleep(0.6)

    result = await asyncio.wait_for(applying, timeout=10)

    assert [item[0] for item in result.accepted] == ["dev-1"]
    assert counter("geotrack_processor_retries_total", sqlstate=LOCK_NOT_AVAILABLE) > before
