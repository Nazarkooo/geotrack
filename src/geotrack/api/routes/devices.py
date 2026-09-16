from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Path, Query

from geotrack.api.deps import CurrentUser, Resources, Session
from geotrack.api.problems import ProblemError
from geotrack.api.routes.auth import DOCUMENTED_AUTH, UNAUTHORIZED
from geotrack.clock import utc_now
from geotrack.db.repositories import devices as repo
from geotrack.geo import BBox
from geotrack.schemas.devices import DeviceList, DevicePositionOut, TrackOut

router = APIRouter(
    prefix="/api/v1/devices",
    tags=["devices"],
    dependencies=DOCUMENTED_AUTH,
    responses=UNAUTHORIZED,
)

DevicePath = Annotated[
    str, Path(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._:-]+$", description="Device id")
]
DEFAULT_TRACK_WINDOW = timedelta(hours=1)


def _as_utc(moment: datetime | None) -> datetime | None:
    """A bound without a zone is read as UTC, matching how reports are accepted."""
    if moment is None:
        return None
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


@router.get("", response_model=DeviceList, summary="Latest known position of each device")
async def list_devices(
    session: Session,
    current_user: CurrentUser,
    bbox: Annotated[
        str | None,
        Query(description="west,south,east,north; west > east crosses the antimeridian"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=20_000)] = 2_000,
) -> DeviceList:
    """Positions are global: devices belong to the fleet, zones belong to accounts."""
    parsed: BBox | None = None
    if bbox is not None:
        try:
            parsed = BBox.parse(bbox)
        except ValueError as exc:
            raise ProblemError(
                422, "Request validation failed", code="validation_error", detail=str(exc)
            ) from exc
    items = await repo.in_bbox(session, parsed, limit=limit)
    return DeviceList(items=items)


@router.get(
    "/{device_id}",
    response_model=DevicePositionOut,
    responses={404: {"description": "The device has never reported"}},
    summary="Latest position of one device",
)
async def get_device(
    device_id: DevicePath, session: Session, current_user: CurrentUser
) -> DevicePositionOut:
    position = await repo.get(session, device_id)
    if position is None:
        raise ProblemError(
            404,
            "Not found",
            code="not_found",
            detail=f"Device {device_id!r} has no stored position.",
        )
    return position


@router.get("/{device_id}/track", response_model=TrackOut, summary="Recent track of one device")
async def get_device_track(
    device_id: DevicePath,
    session: Session,
    resources: Resources,
    current_user: CurrentUser,
    since: Annotated[datetime | None, Query(description="Defaults to one hour ago")] = None,
    until: Annotated[datetime | None, Query(description="Defaults to now")] = None,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
) -> TrackOut:
    """History points in ``[since, until)``, oldest first.

    Both ends are always bound — defaulted here when the caller omits them — because the
    history table is partitioned by day and an open-ended range would read every
    partition instead of the one or two the window actually touches.
    """
    now = utc_now()
    window_end = _as_utc(until) or now
    window_start = _as_utc(since) or (window_end - DEFAULT_TRACK_WINDOW)
    retention = timedelta(days=resources.settings.history_retention_days)
    window_start = max(window_start, now - retention)
    if window_end <= window_start:
        raise ProblemError(
            422,
            "Request validation failed",
            code="validation_error",
            detail="'until' must be later than 'since', and within the retention window.",
        )
    points = await repo.track(session, device_id, since=window_start, until=window_end, limit=limit)
    return TrackOut(device_id=device_id, points=points)
