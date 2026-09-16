from datetime import datetime
from uuid import UUID

from geotrack.db.models import AlertKind
from geotrack.schemas.common import DeviceId, Schema

__all__ = ["AlertKind", "AlertOut", "AlertPage", "AlertZoneRef"]


class AlertZoneRef(Schema):
    # ``id`` is null once the zone is deleted; the name is kept on the alert so history
    # stays readable.
    id: UUID | None
    name: str


class AlertOut(Schema):
    id: int
    kind: AlertKind
    zone: AlertZoneRef
    device_id: DeviceId
    latitude: float
    longitude: float
    occurred_at: datetime
    created_at: datetime


class AlertPage(Schema):
    items: list[AlertOut]
    next_before_id: int | None
