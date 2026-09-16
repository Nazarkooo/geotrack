from datetime import datetime

from geotrack.schemas.common import DeviceId, Schema


class DevicePositionOut(Schema):
    device_id: DeviceId
    latitude: float
    longitude: float
    reported_at: datetime
    received_at: datetime


class DeviceList(Schema):
    items: list[DevicePositionOut]


class TrackPoint(Schema):
    latitude: float
    longitude: float
    reported_at: datetime


class TrackOut(Schema):
    device_id: DeviceId
    points: list[TrackPoint]
