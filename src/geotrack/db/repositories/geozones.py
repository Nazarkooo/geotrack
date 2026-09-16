"""Geozone storage.

Every statement is filtered by ``user_id`` as well as by the primary key: a zone id
from a URL is an untrusted string, and the difference between "not yours" and "does
not exist" must never be observable.

The statements are assembled from module-level constants only — column lists, fixed
fragments and bind placeholders. Nothing from a request is ever concatenated into SQL,
which is what the suppressions on those literals record.
"""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from geotrack.ids import new_uuid
from geotrack.schemas.common import default_zone_color
from geotrack.schemas.devices import DevicePositionOut
from geotrack.schemas.geozones import GeozoneCreate, GeozoneOut

# Projected once here so the API only ever sees plain floats, never WKB.
_COLUMNS = (
    "id, name, color, "
    "ST_Y(center::geometry) AS latitude, ST_X(center::geometry) AS longitude, "
    "radius_m, alert_on_enter, alert_on_exit, dwell_alert_interval_s, "
    "version, created_at, updated_at"
)
_POINT = "ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography"

# Bound the quota check and the "did I hit the version?" re-read to a known shape.
_VERSION_GUARD = "(CAST(:expected_version AS integer) IS NULL OR version = :expected_version)"


class UnknownOwnerError(Exception):
    """The token is valid but the account behind it is gone."""


class QuotaExceededError(Exception):
    """The account already holds as many zones as it is allowed."""

    def __init__(self, quota: int) -> None:
        super().__init__(f"the zone quota of {quota} is already used up")
        self.quota = quota


class VersionMismatchError(Exception):
    """The zone exists but is not at the version the caller expected."""

    def __init__(self, current_version: int) -> None:
        super().__init__(f"the zone is at version {current_version}")
        self.current_version = current_version


def _zone(row: object) -> GeozoneOut:
    return GeozoneOut.model_validate(row, from_attributes=True)


async def count_for_user(session: AsyncSession, user_id: UUID) -> int:
    total = await session.scalar(
        text("SELECT count(*) FROM geozones WHERE user_id = :user_id"), {"user_id": user_id}
    )
    return int(total or 0)


async def list_for_user(
    session: AsyncSession, user_id: UUID, *, limit: int, offset: int
) -> list[GeozoneOut]:
    rows = await session.execute(
        text(
            f"SELECT {_COLUMNS} FROM geozones WHERE user_id = :user_id "  # noqa: S608
            "ORDER BY created_at, id LIMIT :limit OFFSET :offset"
        ),
        {"user_id": user_id, "limit": limit, "offset": offset},
    )
    return [_zone(row) for row in rows]


async def get_for_user(session: AsyncSession, zone_id: UUID, user_id: UUID) -> GeozoneOut | None:
    row = (
        await session.execute(
            text(f"SELECT {_COLUMNS} FROM geozones WHERE id = :id AND user_id = :user_id"),  # noqa: S608
            {"id": zone_id, "user_id": user_id},
        )
    ).first()
    return _zone(row) if row is not None else None


async def create(
    session: AsyncSession, *, user_id: UUID, payload: GeozoneCreate, quota: int
) -> GeozoneOut:
    """Insert a zone, refusing once the account is at its quota.

    The owner row is locked first so that two simultaneous creates cannot both read a
    count below the quota and both insert; under read-committed a bare count-then-insert
    would let the quota drift upwards under load. The lock doubles as an existence check,
    turning a still-valid token for a removed account into a clean answer rather than a
    foreign key violation three statements later.
    """
    owner = (
        await session.execute(
            text("SELECT 1 FROM users WHERE id = :user_id FOR UPDATE"), {"user_id": user_id}
        )
    ).first()
    if owner is None:
        raise UnknownOwnerError(f"no account {user_id}")
    used = await count_for_user(session, user_id)
    if used >= quota:
        raise QuotaExceededError(quota)

    row = (
        await session.execute(
            text(
                "INSERT INTO geozones (id, user_id, name, color, center, radius_m, "  # noqa: S608
                "alert_on_enter, alert_on_exit, dwell_alert_interval_s) "
                f"VALUES (:id, :user_id, :name, :color, {_POINT}, :radius_m, "
                ":alert_on_enter, :alert_on_exit, :dwell_alert_interval_s) "
                f"RETURNING {_COLUMNS}"
            ),
            {
                "id": new_uuid(),
                "user_id": user_id,
                "name": payload.name,
                # Consecutive zones get distinct colours so a fresh map stays readable.
                "color": payload.color or default_zone_color(used),
                "latitude": payload.latitude,
                "longitude": payload.longitude,
                "radius_m": payload.radius_m,
                "alert_on_enter": payload.alert_on_enter,
                "alert_on_exit": payload.alert_on_exit,
                "dwell_alert_interval_s": payload.dwell_alert_interval_s,
            },
        )
    ).one()
    return _zone(row)


async def replace(
    session: AsyncSession,
    *,
    zone_id: UUID,
    user_id: UUID,
    payload: GeozoneCreate,
    expected_version: int | None,
) -> GeozoneOut | None:
    row = (
        await session.execute(
            text(
                "UPDATE geozones SET name = :name, "  # noqa: S608
                "color = COALESCE(CAST(:color AS text), color), "
                f"center = {_POINT}, radius_m = :radius_m, "
                "alert_on_enter = :alert_on_enter, alert_on_exit = :alert_on_exit, "
                "dwell_alert_interval_s = :dwell_alert_interval_s, "
                "version = version + 1, updated_at = now() "
                f"WHERE id = :id AND user_id = :user_id AND {_VERSION_GUARD} "
                f"RETURNING {_COLUMNS}"
            ),
            {
                "id": zone_id,
                "user_id": user_id,
                "expected_version": expected_version,
                "name": payload.name,
                "color": payload.color,
                "latitude": payload.latitude,
                "longitude": payload.longitude,
                "radius_m": payload.radius_m,
                "alert_on_enter": payload.alert_on_enter,
                "alert_on_exit": payload.alert_on_exit,
                "dwell_alert_interval_s": payload.dwell_alert_interval_s,
            },
        )
    ).first()
    if row is None:
        await _explain_missing_row(session, zone_id=zone_id, user_id=user_id)
        return None
    return _zone(row)


# Only these columns can be written by a partial update, and each maps to a fixed
# fragment: field names never reach the SQL string.
_PATCH_FRAGMENTS = {
    "name": "name = :name",
    "color": "color = :color",
    "radius_m": "radius_m = :radius_m",
    "alert_on_enter": "alert_on_enter = :alert_on_enter",
    "alert_on_exit": "alert_on_exit = :alert_on_exit",
    "dwell_alert_interval_s": "dwell_alert_interval_s = :dwell_alert_interval_s",
}
# Moving a zone may change only one coordinate, so the untouched one is read back from
# the row being updated instead of costing an extra round trip.
_PATCH_CENTER = (
    "center = ST_SetSRID(ST_MakePoint("
    "COALESCE(CAST(:longitude AS double precision), ST_X(center::geometry)), "
    "COALESCE(CAST(:latitude AS double precision), ST_Y(center::geometry))"
    "), 4326)::geography"
)


async def patch(
    session: AsyncSession,
    *,
    zone_id: UUID,
    user_id: UUID,
    changes: dict[str, object],
    expected_version: int | None,
) -> GeozoneOut | None:
    assignments = [_PATCH_FRAGMENTS[field] for field in changes if field in _PATCH_FRAGMENTS]
    if "latitude" in changes or "longitude" in changes:
        assignments.append(_PATCH_CENTER)
    if not assignments:
        return await get_for_user(session, zone_id, user_id)

    params: dict[str, object] = {
        "id": zone_id,
        "user_id": user_id,
        "expected_version": expected_version,
        "latitude": changes.get("latitude"),
        "longitude": changes.get("longitude"),
    }
    params.update({field: changes[field] for field in changes if field in _PATCH_FRAGMENTS})

    row = (
        await session.execute(
            text(
                f"UPDATE geozones SET {', '.join(assignments)}, "  # noqa: S608
                "version = version + 1, updated_at = now() "
                f"WHERE id = :id AND user_id = :user_id AND {_VERSION_GUARD} "
                f"RETURNING {_COLUMNS}"
            ),
            params,
        )
    ).first()
    if row is None:
        await _explain_missing_row(session, zone_id=zone_id, user_id=user_id)
        return None
    return _zone(row)


async def delete(
    session: AsyncSession, *, zone_id: UUID, user_id: UUID, expected_version: int | None
) -> bool:
    row = (
        await session.execute(
            text(
                "DELETE FROM geozones WHERE id = :id AND user_id = :user_id "  # noqa: S608
                f"AND {_VERSION_GUARD} RETURNING id"
            ),
            {"id": zone_id, "user_id": user_id, "expected_version": expected_version},
        )
    ).first()
    if row is None:
        await _explain_missing_row(session, zone_id=zone_id, user_id=user_id)
        return False
    return True


async def _explain_missing_row(session: AsyncSession, *, zone_id: UUID, user_id: UUID) -> None:
    """Raise if a conditional write missed because of the version, stay quiet otherwise.

    A guarded statement affecting no rows means either "no such zone for this user" or
    "someone else changed it first"; only a second look can tell the two apart, and it
    runs on the failure path only.
    """
    current = await session.scalar(
        text("SELECT version FROM geozones WHERE id = :id AND user_id = :user_id"),
        {"id": zone_id, "user_id": user_id},
    )
    if current is not None:
        raise VersionMismatchError(int(current))


# Exposed by name because a test runs EXPLAIN over exactly this statement: the claim
# that it is index-assisted has to be checked against the planner, not against a comment.
DEVICES_INSIDE_SQL = (
    "SELECT device_id, ST_Y(position::geometry) AS latitude, "  # noqa: S608
    "ST_X(position::geometry) AS longitude, reported_at, received_at "
    "FROM device_positions "
    f"WHERE ST_DWithin(position, {_POINT}, :radius_m) "
    "ORDER BY device_id LIMIT :limit"
)


async def devices_inside(
    session: AsyncSession, *, latitude: float, longitude: float, radius_m: float, limit: int
) -> list[DevicePositionOut]:
    """Devices whose latest position is inside a circle.

    The radius is a constant here rather than a column, so PostGIS rewrites the predicate
    into a bounding-box test it can answer from ``ix_device_positions_position``.
    """
    rows = await session.execute(
        text(DEVICES_INSIDE_SQL),
        {"latitude": latitude, "longitude": longitude, "radius_m": radius_m, "limit": limit},
    )
    return [DevicePositionOut.model_validate(row, from_attributes=True) for row in rows]


async def presence_for_user(
    session: AsyncSession, user_id: UUID, *, max_devices: int
) -> dict[UUID, list[str]]:
    """Which devices the processor currently considers inside each of the user's zones.

    Every zone of the user is present in the result, empty ones included, so the map can
    clear a highlight it no longer needs. The device rows are capped as a whole: a user
    with hundreds of wide zones could otherwise ask for millions of pairs in one call.
    """
    rows = await session.execute(
        text(
            "WITH owned AS (SELECT id FROM geozones WHERE user_id = :user_id), "
            "inside AS ("
            "  SELECT p.zone_id, p.device_id FROM zone_presence p "
            "  JOIN owned o ON o.id = p.zone_id "
            "  ORDER BY p.zone_id, p.device_id LIMIT :limit"
            ") "
            "SELECT o.id AS zone_id, i.device_id FROM owned o "
            "LEFT JOIN inside i ON i.zone_id = o.id "
            "ORDER BY o.id, i.device_id"
        ),
        {"user_id": user_id, "limit": max_devices},
    )
    presence: dict[UUID, list[str]] = {}
    for row in rows:
        devices = presence.setdefault(row.zone_id, [])
        if row.device_id is not None:
            devices.append(row.device_id)
    return presence
