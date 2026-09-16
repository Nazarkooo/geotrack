"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-09-16
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Executed one statement at a time: the asyncpg dialect sends every statement as a
# prepared statement, and a prepared statement cannot contain several commands.
UPGRADE_STATEMENTS: tuple[str, ...] = (
    """
    CREATE EXTENSION IF NOT EXISTS postgis
    """,
    """
    CREATE TABLE users (
        id uuid PRIMARY KEY,
        username text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT uq_users_username UNIQUE (username),
        CONSTRAINT ck_users_username_format
            CHECK (username = lower(username) AND username ~ '^[a-z0-9_.-]{3,32}$')
    )
    """,
    """
    CREATE TABLE geozones (
        id uuid PRIMARY KEY,
        user_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        name text NOT NULL,
        color text NOT NULL,
        center geography(Point, 4326) NOT NULL,
        radius_m double precision NOT NULL,
        -- Circumscribing polygon used only as an index-assisted candidate filter; the exact
        -- test is ST_DWithin(center, point, radius_m). A per-row radius in ST_DWithin cannot
        -- drive a GiST index scan, a stored bounding shape can.
        search_area geography(Polygon, 4326) NOT NULL
            GENERATED ALWAYS AS (ST_Buffer(center, radius_m * 1.02 + 1.0, 'quad_segs=8')) STORED,
        alert_on_enter boolean NOT NULL DEFAULT true,
        alert_on_exit boolean NOT NULL DEFAULT true,
        dwell_alert_interval_s integer,
        version integer NOT NULL DEFAULT 1,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ck_geozones_name_length CHECK (char_length(name) BETWEEN 1 AND 80),
        CONSTRAINT ck_geozones_color_format CHECK (color ~ '^#[0-9a-f]{6}$'),
        CONSTRAINT ck_geozones_radius_range CHECK (radius_m >= 10 AND radius_m <= 50000),
        CONSTRAINT ck_geozones_dwell_range
            CHECK (dwell_alert_interval_s IS NULL OR dwell_alert_interval_s BETWEEN 10 AND 86400)
    )
    """,
    """
    CREATE INDEX ix_geozones_search_area ON geozones USING gist (search_area)
    """,
    """
    CREATE INDEX ix_geozones_user_id_created_at ON geozones (user_id, created_at)
    """,
    """
    CREATE TABLE device_positions (
        device_id text PRIMARY KEY,
        position geography(Point, 4326) NOT NULL,
        reported_at timestamptz NOT NULL,
        received_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT ck_device_positions_device_id_length CHECK (char_length(device_id) BETWEEN 1 AND 64)
    )
    """,
    """
    CREATE INDEX ix_device_positions_position ON device_positions USING gist (position)
    """,
    """
    CREATE INDEX ix_device_positions_reported_at ON device_positions (reported_at)
    """,
    """
    CREATE TABLE zone_presence (
        zone_id uuid NOT NULL REFERENCES geozones (id) ON DELETE CASCADE,
        device_id text NOT NULL,
        entered_at timestamptz NOT NULL,
        last_seen_at timestamptz NOT NULL,
        last_alert_at timestamptz NOT NULL,
        CONSTRAINT pk_zone_presence PRIMARY KEY (zone_id, device_id)
    )
    """,
    """
    CREATE INDEX ix_zone_presence_device_id ON zone_presence (device_id)
    """,
    """
    CREATE TYPE alert_kind AS ENUM ('enter', 'exit', 'dwell')
    """,
    """
    CREATE TABLE alerts (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        zone_id uuid REFERENCES geozones (id) ON DELETE SET NULL,
        zone_name text NOT NULL,
        device_id text NOT NULL,
        kind alert_kind NOT NULL,
        position geography(Point, 4326) NOT NULL,
        occurred_at timestamptz NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE INDEX ix_alerts_user_id_id ON alerts (user_id, id DESC)
    """,
    """
    CREATE TABLE location_history (
        device_id text NOT NULL,
        position geography(Point, 4326) NOT NULL,
        reported_at timestamptz NOT NULL,
        received_at timestamptz NOT NULL,
        CONSTRAINT pk_location_history PRIMARY KEY (device_id, reported_at)
    ) PARTITION BY RANGE (reported_at)
    """,
    """
    CREATE INDEX ix_location_history_reported_at ON location_history USING brin (reported_at)
    """,
    """
    -- Daily partitions, created ahead of time by the processor's maintenance leader.
    -- The advisory lock makes concurrent callers (e.g. two processors starting at once)
    -- safe without relying on catalog error handling.
    CREATE FUNCTION geotrack_ensure_history_partitions(p_from date, p_to date)
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    DECLARE
        partition_day date := p_from;
        created integer := 0;
        partition_name text;
    BEGIN
        IF p_from > p_to THEN
            RAISE EXCEPTION 'p_from (%) must not be after p_to (%)', p_from, p_to;
        END IF;
        PERFORM pg_advisory_xact_lock(hashtext('geotrack.location_history.partitions'));
        WHILE partition_day <= p_to LOOP
            partition_name := format('location_history_p%s', to_char(partition_day, 'YYYYMMDD'));
            IF to_regclass(partition_name) IS NULL THEN
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF location_history FOR VALUES FROM (%L) TO (%L)',
                    partition_name,
                    partition_day::timestamp AT TIME ZONE 'UTC',
                    (partition_day + 1)::timestamp AT TIME ZONE 'UTC'
                );
                created := created + 1;
            END IF;
            partition_day := partition_day + 1;
        END LOOP;
        RETURN created;
    END;
    $$
    """,
    """
    CREATE FUNCTION geotrack_drop_history_partitions(p_before date)
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    DECLARE
        child record;
        dropped integer := 0;
    BEGIN
        PERFORM pg_advisory_xact_lock(hashtext('geotrack.location_history.partitions'));
        FOR child IN
            SELECT c.relname
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = 'location_history'
              AND c.relname ~ '^location_history_p[0-9]{8}$'
              AND to_date(right(c.relname, 8), 'YYYYMMDD') < p_before
        LOOP
            EXECUTE format('DROP TABLE %I', child.relname);
            dropped := dropped + 1;
        END LOOP;
        RETURN dropped;
    END;
    $$
    """,
    """
    SELECT geotrack_ensure_history_partitions(
        (now() AT TIME ZONE 'UTC')::date - 7,
        (now() AT TIME ZONE 'UTC')::date + 2
    )
    """,
)

DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    """
    DROP FUNCTION IF EXISTS geotrack_drop_history_partitions(date)
    """,
    """
    DROP FUNCTION IF EXISTS geotrack_ensure_history_partitions(date, date)
    """,
    """
    DROP TABLE IF EXISTS location_history CASCADE
    """,
    """
    DROP TABLE IF EXISTS alerts
    """,
    """
    DROP TYPE IF EXISTS alert_kind
    """,
    """
    DROP TABLE IF EXISTS zone_presence
    """,
    """
    DROP TABLE IF EXISTS device_positions
    """,
    """
    DROP TABLE IF EXISTS geozones
    """,
    """
    DROP TABLE IF EXISTS users
    """,
)


def upgrade() -> None:
    for statement in UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_STATEMENTS:
        op.execute(statement)
