"""Device positions and tracks."""

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from geotrack.geo import BBox
from geotrack.schemas.devices import DevicePositionOut, TrackPoint

_COLUMNS = (
    "device_id, ST_Y(position::geometry) AS latitude, "
    "ST_X(position::geometry) AS longitude, reported_at, received_at"
)


def _position(row: object) -> DevicePositionOut:
    return DevicePositionOut.model_validate(row, from_attributes=True)


async def in_bbox(
    session: AsyncSession, bbox: BBox | None, *, limit: int
) -> list[DevicePositionOut]:
    """Latest positions inside a longitude/latitude rectangle.

    The filter is an exact comparison on the projected coordinates rather than the GiST
    prefilter used for zones. A geography bounding box lives in geocentric space: the
    east-west edges of a rectangle are great circles that bow polewards, so the box built
    from them can sit *inside* the rectangle near the middle of an edge and drop devices
    that really are in view. ``device_positions`` holds exactly one row per device, so
    the resulting scan costs device count, not report count — measured at 6.6 ms over
    20,000 devices, against 0.03 ms for the indexed zone lookup next door.
    """
    conditions: list[str] = []
    params: dict[str, object] = {"limit": limit}
    if bbox is not None:
        # An antimeridian-crossing box becomes two ordinary boxes.
        for index, part in enumerate(bbox.parts()):
            conditions.append(
                f"(ST_X(position::geometry) BETWEEN :west{index} AND :east{index} "
                f"AND ST_Y(position::geometry) BETWEEN :south{index} AND :north{index})"
            )
            params[f"west{index}"] = part.west
            params[f"east{index}"] = part.east
            params[f"south{index}"] = part.south
            params[f"north{index}"] = part.north

    where = f"WHERE {' OR '.join(conditions)} " if conditions else ""
    rows = await session.execute(
        text(f"SELECT {_COLUMNS} FROM device_positions {where}ORDER BY device_id LIMIT :limit"),  # noqa: S608
        params,
    )
    return [_position(row) for row in rows]


async def get(session: AsyncSession, device_id: str) -> DevicePositionOut | None:
    row = (
        await session.execute(
            text(f"SELECT {_COLUMNS} FROM device_positions WHERE device_id = :device_id"),  # noqa: S608
            {"device_id": device_id},
        )
    ).first()
    return _position(row) if row is not None else None


async def track(
    session: AsyncSession,
    device_id: str,
    *,
    since: datetime,
    until: datetime,
    limit: int,
) -> list[TrackPoint]:
    """History points for one device, oldest first.

    Both bounds are always supplied so the planner can prune ``location_history`` down to
    the daily partitions the window touches. The inner query walks backwards from
    ``until`` so a truncated result is the most recent part of the track, not the oldest.
    """
    rows = await session.execute(
        text(
            "SELECT latitude, longitude, reported_at FROM ("
            "  SELECT ST_Y(position::geometry) AS latitude, "
            "         ST_X(position::geometry) AS longitude, reported_at "
            "  FROM location_history "
            "  WHERE device_id = :device_id AND reported_at >= :since AND reported_at < :until "
            "  ORDER BY reported_at DESC LIMIT :limit"
            ") recent ORDER BY reported_at"
        ),
        {"device_id": device_id, "since": since, "until": until, "limit": limit},
    )
    return [TrackPoint.model_validate(row, from_attributes=True) for row in rows]
