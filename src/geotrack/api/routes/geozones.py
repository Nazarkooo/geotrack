"""Geozone CRUD.

Two rules run through every handler. Ownership is enforced in the SQL, and a zone that
belongs to somebody else answers 404 rather than 403, so the API never confirms that an
id exists. And every successful mutation is announced on the owner's channel, so the
other sessions of the same account redraw without polling.
"""

from typing import Annotated, Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Header, Path, Query, Response, status
from redis.exceptions import RedisError

from geotrack.api.deps import CurrentUser, Resources, Session
from geotrack.api.problems import ProblemError
from geotrack.api.resources import AppResources
from geotrack.api.routes.auth import DOCUMENTED_AUTH, UNAUTHORIZED
from geotrack.db.repositories import geozones as repo
from geotrack.messaging.keys import user_channel
from geotrack.realtime.protocol import ZoneOp, zone_frame
from geotrack.schemas.devices import DeviceList
from geotrack.schemas.geozones import (
    GeozoneCreate,
    GeozoneList,
    GeozoneOut,
    GeozonePatch,
    GeozoneReplace,
    ZonePresenceOut,
)

logger = structlog.get_logger(__name__)
router = APIRouter(
    prefix="/api/v1/geozones",
    tags=["geozones"],
    dependencies=DOCUMENTED_AUTH,
    responses=UNAUTHORIZED,
)

ZoneId = Annotated[UUID, Path(description="Identifier of a zone owned by the caller")]
IfMatch = Annotated[
    str | None,
    Header(description='Zone version to update, quoted as returned in ETag (e.g. "3")'),
]

# A caller asking for presence across hundreds of wide zones could otherwise request
# millions of pairs in one response.
PRESENCE_DEVICE_LIMIT = 20_000

# Fields a partial update may not blank out; only the dwell interval is nullable.
_REQUIRED_ON_PATCH = (
    "name",
    "color",
    "latitude",
    "longitude",
    "radius_m",
    "alert_on_enter",
    "alert_on_exit",
)

_NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"description": "No such zone for this account"}
}
_CONDITIONAL: dict[int | str, dict[str, Any]] = {
    400: {"description": "Malformed If-Match header"},
    404: {"description": "No such zone for this account"},
    412: {"description": "The zone changed since the version in If-Match"},
}


def _not_found(zone_id: UUID) -> ProblemError:
    return ProblemError(
        404,
        "Not found",
        code="not_found",
        detail=f"No zone {zone_id} belongs to this account.",
    )


# Versions live in a 32-bit column and start at 1, so nothing outside this range can
# ever be a zone's current version.
_VERSION_RANGE = range(-(2**31), 2**31)
_NO_SUCH_VERSION = 0


def parse_if_match(raw: str | None) -> int | None:
    """Read the zone version out of an ``If-Match`` header.

    ``*`` means "any current version", which is the same as sending no header at all.
    """
    if raw is None:
        return None
    candidate = raw.strip()
    if candidate == "*":
        return None
    if candidate.startswith("W/"):
        candidate = candidate[2:]
    try:
        version = int(candidate.strip('"'))
    except ValueError as exc:
        raise ProblemError(
            400,
            "Bad request",
            code="bad_request",
            detail='If-Match must carry a quoted zone version, for example If-Match: "3".',
        ) from exc
    # A number the column could not hold is still a well-formed validator that does not
    # match, and that is what the caller is owed: 412 for a zone they own, 404 for one
    # they do not. Passing it on as a bind parameter instead would reach the driver and
    # come back as a 500, which any client round-tripping an opaque ETag could trigger.
    return version if version in _VERSION_RANGE else _NO_SUCH_VERSION


def _tag(response: Response, zone: GeozoneOut) -> GeozoneOut:
    response.headers["ETag"] = f'"{zone.version}"'
    return zone


async def _announce(
    resources: AppResources, user_id: UUID, op: ZoneOp, zone: GeozoneOut | UUID
) -> None:
    """Push the change to every session of this account, on any replica.

    The write is already committed, so a publish failure must not turn a successful
    mutation into an error: the sessions fall back to their next REST read.
    """
    try:
        await resources.redis.publish(user_channel(user_id), zone_frame(op, zone))
    except RedisError as exc:
        logger.warning("zone event not published", op=op, error=str(exc))


@router.get("", response_model=GeozoneList, summary="List the caller's zones")
async def list_zones(
    session: Session,
    current_user: CurrentUser,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> GeozoneList:
    items = await repo.list_for_user(session, current_user.user_id, limit=limit, offset=offset)
    total = await repo.count_for_user(session, current_user.user_id)
    return GeozoneList(items=items, total=total)


@router.post(
    "",
    response_model=GeozoneOut,
    status_code=status.HTTP_201_CREATED,
    responses={409: {"description": "The account is at its zone quota"}},
    summary="Create a zone",
)
async def create_zone(
    payload: GeozoneCreate,
    session: Session,
    resources: Resources,
    current_user: CurrentUser,
    response: Response,
) -> GeozoneOut:
    """Create a circular zone. Omitting ``color`` picks the next colour of the palette."""
    try:
        zone = await repo.create(
            session,
            user_id=current_user.user_id,
            payload=payload,
            quota=resources.settings.geozone_quota_per_user,
        )
    except repo.UnknownOwnerError as exc:
        raise ProblemError(
            401,
            "Authentication required",
            code="unauthorized",
            detail="The account this token belongs to no longer exists. Sign in again.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except repo.QuotaExceededError as exc:
        raise ProblemError(
            409,
            "Conflict",
            code="zone_quota_exceeded",
            detail=f"This account already holds {exc.quota} zones. Delete one first.",
        ) from exc
    await session.commit()
    await _announce(resources, current_user.user_id, "created", zone)
    response.headers["Location"] = f"{router.prefix}/{zone.id}"
    return _tag(response, zone)


@router.get(
    "/presence",
    response_model=ZonePresenceOut,
    summary="Devices currently inside each of the caller's zones",
)
async def zone_presence(session: Session, current_user: CurrentUser) -> ZonePresenceOut:
    """Server-authoritative presence, the same state the alert engine works from.

    Declared before ``/{zone_id}`` so the literal path wins the match.
    """
    zones = await repo.presence_for_user(
        session, current_user.user_id, max_devices=PRESENCE_DEVICE_LIMIT
    )
    return ZonePresenceOut(zones=zones)


@router.get("/{zone_id}", response_model=GeozoneOut, responses=_NOT_FOUND, summary="Read one zone")
async def get_zone(
    zone_id: ZoneId, session: Session, current_user: CurrentUser, response: Response
) -> GeozoneOut:
    zone = await repo.get_for_user(session, zone_id, current_user.user_id)
    if zone is None:
        raise _not_found(zone_id)
    return _tag(response, zone)


@router.put(
    "/{zone_id}", response_model=GeozoneOut, responses=_CONDITIONAL, summary="Replace a zone"
)
async def replace_zone(
    zone_id: ZoneId,
    payload: GeozoneReplace,
    session: Session,
    resources: Resources,
    current_user: CurrentUser,
    response: Response,
    if_match: IfMatch = None,
) -> GeozoneOut:
    """Overwrite every field of a zone. An omitted ``color`` keeps the current one."""
    expected = parse_if_match(if_match)
    try:
        zone = await repo.replace(
            session,
            zone_id=zone_id,
            user_id=current_user.user_id,
            payload=payload,
            expected_version=expected,
        )
    except repo.VersionMismatchError as exc:
        raise _stale(exc) from exc
    if zone is None:
        raise _not_found(zone_id)
    await session.commit()
    await _announce(resources, current_user.user_id, "updated", zone)
    return _tag(response, zone)


@router.patch(
    "/{zone_id}",
    response_model=GeozoneOut,
    responses=_CONDITIONAL,
    summary="Update part of a zone",
)
async def patch_zone(
    zone_id: ZoneId,
    payload: GeozonePatch,
    session: Session,
    resources: Resources,
    current_user: CurrentUser,
    response: Response,
    if_match: IfMatch = None,
) -> GeozoneOut:
    """Apply only the fields present in the body.

    Sending ``{"dwell_alert_interval_s": null}`` switches dwell alerts off, while leaving
    the key out keeps them as they are.
    """
    expected = parse_if_match(if_match)
    changes = payload.changes()
    blanked = [field for field in _REQUIRED_ON_PATCH if changes.get(field, ...) is None]
    if blanked:
        raise ProblemError(
            422,
            "Request validation failed",
            code="validation_error",
            detail=f"These fields cannot be set to null: {', '.join(blanked)}.",
        )

    if not changes:
        # Nothing to write, but the caller still deserves the conditional answer.
        zone = await repo.get_for_user(session, zone_id, current_user.user_id)
        if zone is None:
            raise _not_found(zone_id)
        if expected is not None and expected != zone.version:
            raise _stale(repo.VersionMismatchError(zone.version))
        return _tag(response, zone)

    try:
        zone = await repo.patch(
            session,
            zone_id=zone_id,
            user_id=current_user.user_id,
            changes=changes,
            expected_version=expected,
        )
    except repo.VersionMismatchError as exc:
        raise _stale(exc) from exc
    if zone is None:
        raise _not_found(zone_id)
    await session.commit()
    await _announce(resources, current_user.user_id, "updated", zone)
    return _tag(response, zone)


@router.delete(
    "/{zone_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=_CONDITIONAL,
    summary="Delete a zone",
)
async def delete_zone(
    zone_id: ZoneId,
    session: Session,
    resources: Resources,
    current_user: CurrentUser,
    if_match: IfMatch = None,
) -> Response:
    """Delete a zone. Its presence rows go with it, so no exit alerts are raised."""
    expected = parse_if_match(if_match)
    try:
        deleted = await repo.delete(
            session, zone_id=zone_id, user_id=current_user.user_id, expected_version=expected
        )
    except repo.VersionMismatchError as exc:
        raise _stale(exc) from exc
    if not deleted:
        raise _not_found(zone_id)
    await session.commit()
    await _announce(resources, current_user.user_id, "deleted", zone_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{zone_id}/devices",
    response_model=DeviceList,
    responses=_NOT_FOUND,
    summary="Devices inside one zone right now",
)
async def devices_in_zone(
    zone_id: ZoneId,
    session: Session,
    current_user: CurrentUser,
    limit: Annotated[int, Query(ge=1, le=5_000)] = 500,
) -> DeviceList:
    """Evaluated against the latest stored positions, not against presence state.

    Reading the zone first keeps the radius a constant in the distance predicate, which
    is what lets the position index drive the scan.
    """
    zone = await repo.get_for_user(session, zone_id, current_user.user_id)
    if zone is None:
        raise _not_found(zone_id)
    items = await repo.devices_inside(
        session,
        latitude=zone.latitude,
        longitude=zone.longitude,
        radius_m=zone.radius_m,
        limit=limit,
    )
    return DeviceList(items=items)


def _stale(exc: repo.VersionMismatchError) -> ProblemError:
    return ProblemError(
        412,
        "Precondition failed",
        code="precondition_failed",
        detail="The zone has changed since the version you sent; re-read it and retry.",
        headers={"ETag": f'"{exc.current_version}"'},
        extra={"current_version": exc.current_version},
    )
