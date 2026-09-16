"""The spatial contract: the GiST prefilter must never lose a point the exact
predicate accepts, and the zone-matching join must use the index.
"""

from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.ids import new_uuid

# Worst cases for a projected buffer: the equator, mid latitudes, high latitudes,
# the antimeridian and the smallest/largest radii the API accepts.
ZONE_SITES = [
    (0.0, 0.0),
    (50.4501, 30.5234),
    (69.6492, 18.9553),
    (-54.8019, -68.3030),
    (12.0, 179.995),
    (-33.8688, 151.2093),
]
RADII = [10.0, 75.0, 1_000.0, 12_500.0, 50_000.0]


async def _create_user(engine: AsyncEngine, username: str = "spatial") -> UUID:
    user_id = new_uuid()
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO users (id, username) VALUES (:id, :username)"),
            {"id": user_id, "username": username},
        )
    return user_id


async def _create_zones(engine: AsyncEngine, user_id: UUID) -> list[dict[str, Any]]:
    zones: list[dict[str, Any]] = []
    for lat, lon in ZONE_SITES:
        for radius in RADII:
            zones.append(
                {
                    "id": new_uuid(),
                    "user_id": user_id,
                    "name": f"zone {lat} {lon} {radius}",
                    "color": "#3fb1ff",
                    "lat": lat,
                    "lon": lon,
                    "radius": radius,
                }
            )
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO geozones (id, user_id, name, color, center, radius_m) "
                "VALUES (:id, :user_id, :name, :color, "
                "ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography, :radius)"
            ),
            zones,
        )
    return zones


async def test_prefilter_never_drops_a_point_inside_the_zone(engine: AsyncEngine) -> None:
    user_id = await _create_user(engine)
    await _create_zones(engine, user_id)

    # 72 azimuths per zone, one millimetre inside and one millimetre outside the radius.
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    """
                    WITH probes AS (
                        SELECT z.id AS zone_id,
                               inside,
                               ST_Project(
                                   z.center,
                                   CASE WHEN inside
                                        THEN z.radius_m - 0.001
                                        ELSE z.radius_m + 0.001 END,
                                   radians(azimuth)
                               )::geography AS point
                        FROM geozones z
                        CROSS JOIN generate_series(0, 355, 5) AS azimuth
                        CROSS JOIN (VALUES (true), (false)) AS s(inside)
                    )
                    SELECT
                        count(*) AS probes,
                        count(*) FILTER (
                            WHERE p.inside
                              AND NOT ST_DWithin(z.center, p.point, z.radius_m)
                        ) AS exact_false_negatives,
                        count(*) FILTER (
                            WHERE NOT p.inside
                              AND ST_DWithin(z.center, p.point, z.radius_m)
                        ) AS exact_false_positives,
                        count(*) FILTER (
                            WHERE ST_DWithin(z.center, p.point, z.radius_m)
                              AND NOT (z.search_area && p.point)
                        ) AS prefilter_misses
                    FROM probes p
                    JOIN geozones z ON z.id = p.zone_id
                    """
                )
            )
        ).one()

    probes, exact_false_negatives, exact_false_positives, prefilter_misses = row
    assert probes == len(ZONE_SITES) * len(RADII) * 72 * 2
    assert (exact_false_negatives, exact_false_positives, prefilter_misses) == (0, 0, 0)


async def test_prefilter_rejects_points_well_outside_the_zone(engine: AsyncEngine) -> None:
    """The padding must not be so generous that the filter stops filtering.

    ``&&`` compares bounding boxes, so a point at 1.5x the radius can still sit in a
    corner of the box; at 3x the radius nothing may survive the filter.
    """
    user_id = await _create_user(engine)
    await _create_zones(engine, user_id)

    async with engine.connect() as conn:
        survivors = await conn.scalar(
            text(
                """
                WITH probes AS (
                    SELECT z.id AS zone_id,
                           ST_Project(z.center, z.radius_m * 3,
                                      radians(azimuth))::geography AS point
                    FROM geozones z
                    CROSS JOIN generate_series(0, 350, 10) AS azimuth
                )
                SELECT count(*) FILTER (WHERE z.search_area && p.point)
                FROM probes p JOIN geozones z ON z.id = p.zone_id
                """
            )
        )

    assert survivors == 0


async def test_zone_matching_join_uses_the_gist_index(engine: AsyncEngine) -> None:
    user_id = await _create_user(engine)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO geozones (id, user_id, name, color, center, radius_m)
                SELECT gen_random_uuid(), :user_id, 'zone ' || g, '#3fb1ff',
                       ST_SetSRID(ST_MakePoint(30.20 + random() * 0.65,
                                               50.30 + random() * 0.30), 4326)::geography,
                       50 + random() * 2950
                FROM generate_series(1, 5000) g
                """
            ),
            {"user_id": user_id},
        )
        await conn.execute(text("ANALYZE geozones"))

    async with engine.connect() as conn:
        plan = "\n".join(
            (
                await conn.execute(
                    text(
                        """
                        EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
                        WITH batch AS (
                            SELECT ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS position
                            FROM unnest(CAST(:lons AS float8[]), CAST(:lats AS float8[]))
                                 AS t(lon, lat)
                        )
                        SELECT count(*)
                        FROM batch b
                        JOIN geozones z
                          ON z.search_area && b.position
                         AND ST_DWithin(z.center, b.position, z.radius_m)
                        """
                    ),
                    {
                        "lons": [30.2 + i * 0.0006 for i in range(1000)],
                        "lats": [50.3 + i * 0.0003 for i in range(1000)],
                    },
                )
            ).scalars()
        )

    assert "ix_geozones_search_area" in plan, plan
    assert "Seq Scan on geozones" not in plan, plan

    # The index scan must also do most of the filtering: few candidates reach ST_DWithin.
    async with engine.connect() as conn:
        candidates, exact = (
            await conn.execute(
                text(
                    """
                    WITH batch AS (
                        SELECT ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS position
                        FROM unnest(CAST(:lons AS float8[]), CAST(:lats AS float8[])) AS t(lon, lat)
                    ),
                    candidates AS (
                        SELECT z.center, z.radius_m, b.position
                        FROM batch b JOIN geozones z ON z.search_area && b.position
                    )
                    SELECT count(*),
                           count(*) FILTER (WHERE ST_DWithin(center, position, radius_m))
                    FROM candidates
                    """
                ),
                {
                    "lons": [30.2 + i * 0.0006 for i in range(1000)],
                    "lats": [50.3 + i * 0.0003 for i in range(1000)],
                },
            )
        ).one()

    assert exact > 0
    assert candidates <= exact * 2, (
        f"prefilter let through {candidates} candidates for {exact} hits"
    )


@pytest.mark.parametrize(
    ("radius", "should_fail"),
    [(9.0, True), (10.0, False), (50_000.0, False), (50_001.0, True)],
)
async def test_radius_bounds_are_enforced_by_the_database(
    engine: AsyncEngine, radius: float, should_fail: bool
) -> None:
    user_id = await _create_user(engine)
    statement = text(
        "INSERT INTO geozones (id, user_id, name, color, center, radius_m) "
        "VALUES (:id, :user_id, 'z', '#3fb1ff', "
        "ST_SetSRID(ST_MakePoint(30.5, 50.4), 4326)::geography, :radius)"
    )
    params = {"id": new_uuid(), "user_id": user_id, "radius": radius}

    if should_fail:
        with pytest.raises(Exception, match="ck_geozones_radius_range"):
            async with engine.begin() as conn:
                await conn.execute(statement, params)
    else:
        async with engine.begin() as conn:
            await conn.execute(statement, params)
