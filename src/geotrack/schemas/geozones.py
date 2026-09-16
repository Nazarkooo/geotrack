from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import Field, field_validator

from geotrack.geo import MAX_RADIUS_M, MIN_RADIUS_M
from geotrack.schemas.common import (
    DeviceId,
    Latitude,
    Longitude,
    Payload,
    Schema,
    ZoneColor,
)

ZoneName = Annotated[str, Field(min_length=1, max_length=80)]
ZoneRadius = Annotated[float, Field(ge=MIN_RADIUS_M, le=MAX_RADIUS_M, allow_inf_nan=False)]
DwellInterval = Annotated[int, Field(ge=10, le=86_400)]


class GeozoneCreate(Payload):
    name: ZoneName
    latitude: Latitude
    longitude: Longitude
    radius_m: ZoneRadius
    color: ZoneColor | None = None
    alert_on_enter: bool = True
    alert_on_exit: bool = True
    dwell_alert_interval_s: DwellInterval | None = None

    @field_validator("name")
    @classmethod
    def _strip(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("name must not be blank")
        return stripped

    @field_validator("color")
    @classmethod
    def _lowercase(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else None


class GeozoneReplace(GeozoneCreate):
    """Full replacement; identical shape to creation."""


class GeozonePatch(Payload):
    """Partial update. Only fields present in the request body are applied, so
    ``{"dwell_alert_interval_s": null}`` turns dwell alerts off while an absent key
    leaves them untouched."""

    name: ZoneName | None = None
    latitude: Latitude | None = None
    longitude: Longitude | None = None
    radius_m: ZoneRadius | None = None
    color: ZoneColor | None = None
    alert_on_enter: bool | None = None
    alert_on_exit: bool | None = None
    dwell_alert_interval_s: DwellInterval | None = None

    @field_validator("name")
    @classmethod
    def _strip(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("name must not be blank")
        return stripped

    @field_validator("color")
    @classmethod
    def _lowercase(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else None

    def changes(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.model_fields_set}


class GeozoneOut(Schema):
    id: UUID
    name: str
    color: str
    latitude: float
    longitude: float
    radius_m: float
    alert_on_enter: bool
    alert_on_exit: bool
    dwell_alert_interval_s: int | None
    version: int
    created_at: datetime
    updated_at: datetime


class GeozoneList(Schema):
    items: list[GeozoneOut]
    total: int


class ZonePresenceOut(Schema):
    """Devices currently inside each of the caller's zones."""

    zones: dict[UUID, list[DeviceId]]
