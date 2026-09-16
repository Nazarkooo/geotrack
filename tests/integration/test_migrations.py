from datetime import UTC, date, datetime, timedelta

import asyncpg
from alembic import command
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.db.models import Base
from tests.integration.conftest import alembic_config

EXPECTED_INDEXES = {
    "ix_geozones_search_area",
    "ix_geozones_user_id_created_at",
    "ix_device_positions_position",
    "ix_device_positions_reported_at",
    "ix_zone_presence_device_id",
    "ix_alerts_user_id_id",
    "ix_location_history_reported_at",
}


async def test_schema_has_all_tables_and_indexes(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        tables = set(
            (
                await conn.execute(
                    text(
                        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                        "AND tablename NOT LIKE 'location_history_p%'"
                    )
                )
            ).scalars()
        )
        indexes = set(
            (
                await conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE schemaname='public'")
                )
            ).scalars()
        )

    assert set(Base.metadata.tables) <= tables
    assert indexes >= EXPECTED_INDEXES


async def test_orm_columns_match_database(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public'"
                )
            )
        ).all()
    db_columns = {(table, column) for table, column in rows}

    for table in Base.metadata.sorted_tables:
        for column in table.columns:
            assert (table.name, column.name) in db_columns, f"{table.name}.{column.name}"
        table_db_columns = {c for t, c in db_columns if t == table.name}
        assert table_db_columns == {c.name for c in table.columns}, table.name


async def test_history_partitions_cover_retention_window(engine: AsyncEngine) -> None:
    today = datetime.now(UTC).date()
    async with engine.connect() as conn:
        names = set(
            (
                await conn.execute(
                    text(
                        "SELECT c.relname FROM pg_inherits i "
                        "JOIN pg_class c ON c.oid = i.inhrelid "
                        "JOIN pg_class p ON p.oid = i.inhparent "
                        "WHERE p.relname = 'location_history'"
                    )
                )
            ).scalars()
        )

    expected = {
        f"location_history_p{(today + timedelta(days=offset)):%Y%m%d}" for offset in range(-7, 3)
    }
    assert expected <= names


async def test_partition_functions_are_idempotent_and_drop_old_days(engine: AsyncEngine) -> None:
    start, end = date(2020, 1, 1), date(2020, 1, 5)
    async with engine.begin() as conn:
        created = await conn.scalar(
            text("SELECT geotrack_ensure_history_partitions(:start, :end)"),
            {"start": start, "end": end},
        )
        created_again = await conn.scalar(
            text("SELECT geotrack_ensure_history_partitions(:start, :end)"),
            {"start": start, "end": end},
        )
        await conn.execute(
            text(
                "INSERT INTO location_history (device_id, position, reported_at, received_at) "
                "VALUES ('dev-1', 'SRID=4326;POINT(30.5 50.4)', '2020-01-02T12:00:00Z', now())"
            )
        )
        dropped = await conn.scalar(
            text("SELECT geotrack_drop_history_partitions(:before)"), {"before": date(2020, 1, 4)}
        )
        remaining = await conn.scalar(
            text(
                "SELECT count(*) FROM pg_class "
                "WHERE relkind = 'r' AND relname LIKE 'location_history_p202001%'"
            )
        )

    assert (created, created_again, dropped, remaining) == (5, 0, 3, 2)


async def test_partition_bounds_are_utc_days(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT geotrack_ensure_history_partitions('2021-03-01', '2021-03-01')")
        )
        bound = await conn.scalar(
            text(
                "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c "
                "WHERE c.relname = 'location_history_p20210301'"
            )
        )

    assert bound == "FOR VALUES FROM ('2021-03-01 00:00:00+00') TO ('2021-03-02 00:00:00+00')"


async def test_downgrade_and_upgrade_round_trip(migrated_database: str) -> None:
    dsn = migrated_database.replace("postgresql+asyncpg://", "postgresql://", 1)
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute("DROP DATABASE IF EXISTS geotrack_roundtrip")
        await admin.execute("CREATE DATABASE geotrack_roundtrip")
    finally:
        await admin.close()

    base, _, _ = migrated_database.rpartition("/")
    roundtrip_url = f"{base}/geotrack_roundtrip"
    config = alembic_config(roundtrip_url)

    # Alembic's env.py runs its own event loop, so it must not run on the test loop.
    import asyncio

    await asyncio.to_thread(command.upgrade, config, "head")
    await asyncio.to_thread(command.downgrade, config, "base")
    await asyncio.to_thread(command.upgrade, config, "head")

    conn = await asyncpg.connect(roundtrip_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        count = await conn.fetchval("SELECT count(*) FROM geozones")
    finally:
        await conn.close()
    assert count == 0
