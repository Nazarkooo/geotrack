from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from geotrack.api.deps import CurrentUser, Session
from geotrack.api.routes.auth import DOCUMENTED_AUTH, UNAUTHORIZED
from geotrack.db.repositories import alerts as repo
from geotrack.schemas.alerts import AlertPage
from geotrack.schemas.common import DeviceId

router = APIRouter(
    prefix="/api/v1/alerts",
    tags=["alerts"],
    dependencies=DOCUMENTED_AUTH,
    responses=UNAUTHORIZED,
)


@router.get("", response_model=AlertPage, summary="Alert history for the caller")
async def list_alerts(
    session: Session,
    current_user: CurrentUser,
    before_id: Annotated[
        int | None, Query(ge=1, description="Return alerts older than this id")
    ] = None,
    after_id: Annotated[
        int | None, Query(ge=0, description="Return alerts newer than this id")
    ] = None,
    zone_id: Annotated[UUID | None, Query()] = None,
    device_id: Annotated[DeviceId | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AlertPage:
    """Newest first, paged by id.

    Every page holds at most ``limit`` alerts and ``next_before_id`` is the cursor to the
    one after it, or ``null`` once the range is exhausted. Scrolling back through history
    passes that cursor as ``before_id``.

    A dashboard that lost its websocket passes ``after_id`` with the last alert it
    rendered, which bounds the range from below; it still gets the newest page of that
    range first, so a gap wider than ``limit`` is only fully recovered by keeping
    ``after_id`` and following ``next_before_id`` until it comes back ``null``. Alerts
    have the same shape here as in the live ``alert`` frame, so both feed one rendering
    routine.
    """
    return await repo.page(
        session,
        current_user.user_id,
        before_id=before_id,
        after_id=after_id,
        zone_id=zone_id,
        device_id=device_id,
        limit=limit,
    )
