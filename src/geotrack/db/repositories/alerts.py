"""Alert history.

Paged by keyset on ``(user_id, id DESC)`` rather than by offset: the feed is append-heavy
and a client that scrolls while alerts arrive would otherwise see rows shift under it.
"""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from geotrack.schemas.alerts import AlertOut, AlertPage, AlertZoneRef

_COLUMNS = (
    "id, kind, zone_id, zone_name, device_id, "
    "ST_Y(position::geometry) AS latitude, ST_X(position::geometry) AS longitude, "
    "occurred_at, created_at"
)

# Each optional filter contributes a fixed fragment; nothing from the request is ever
# concatenated into the statement, only its bound value.
_FILTERS = {
    "before_id": "id < :before_id",
    "after_id": "id > :after_id",
    "zone_id": "zone_id = :zone_id",
    "device_id": "device_id = :device_id",
}


async def page(
    session: AsyncSession,
    user_id: UUID,
    *,
    before_id: int | None = None,
    after_id: int | None = None,
    zone_id: UUID | None = None,
    device_id: str | None = None,
    limit: int = 100,
) -> AlertPage:
    params: dict[str, object] = {"user_id": user_id, "limit": limit}
    conditions = ["user_id = :user_id"]
    for name, value in (
        ("before_id", before_id),
        ("after_id", after_id),
        ("zone_id", zone_id),
        ("device_id", device_id),
    ):
        if value is not None:
            conditions.append(_FILTERS[name])
            params[name] = value

    rows = (
        await session.execute(
            text(
                f"SELECT {_COLUMNS} FROM alerts WHERE {' AND '.join(conditions)} "  # noqa: S608
                "ORDER BY id DESC LIMIT :limit"
            ),
            params,
        )
    ).all()

    items = [
        AlertOut(
            id=row.id,
            kind=row.kind,
            zone=AlertZoneRef(id=row.zone_id, name=row.zone_name),
            device_id=row.device_id,
            latitude=row.latitude,
            longitude=row.longitude,
            occurred_at=row.occurred_at,
            created_at=row.created_at,
        )
        for row in rows
    ]
    # Only hand out a cursor when the page was full: otherwise the client has the tail.
    next_before_id = items[-1].id if len(items) == limit else None
    return AlertPage(items=items, next_before_id=next_before_id)
