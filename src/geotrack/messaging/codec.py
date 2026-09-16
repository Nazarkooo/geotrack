"""Wire formats for stream entries and pub/sub payloads.

Reports travel as compact JSON arrays rather than objects: at 5k reports per second
the key names would be the bulk of the payload.
"""

import math
from dataclasses import dataclass

import orjson

type PositionItem = tuple[str, float, float, int]


class CodecError(ValueError):
    """Raised for payloads that cannot be decoded; the entry goes to the dead-letter stream."""


@dataclass(frozen=True, slots=True)
class LocationRecord:
    device_id: str
    lat: float
    lon: float
    reported_ms: int
    received_ms: int

    @property
    def position_item(self) -> PositionItem:
        return (self.device_id, self.lat, self.lon, self.reported_ms)


def encode_record(record: LocationRecord) -> bytes:
    return orjson.dumps(
        [record.device_id, record.lat, record.lon, record.reported_ms, record.received_ms]
    )


def decode_record(raw: bytes) -> LocationRecord:
    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise CodecError(f"invalid json: {exc}") from exc
    if not isinstance(payload, list) or len(payload) != 5:
        raise CodecError("record must be a 5-element array")
    device_id, lat, lon, reported_ms, received_ms = payload
    if not isinstance(device_id, str) or not device_id:
        raise CodecError("device_id must be a non-empty string")
    if not isinstance(lat, int | float) or not isinstance(lon, int | float):
        raise CodecError("latitude and longitude must be numbers")
    if not math.isfinite(lat) or not math.isfinite(lon):
        raise CodecError("latitude and longitude must be finite")
    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        raise CodecError("latitude or longitude out of range")
    if not isinstance(reported_ms, int) or not isinstance(received_ms, int):
        raise CodecError("timestamps must be integers")
    return LocationRecord(
        device_id=device_id,
        lat=float(lat),
        lon=float(lon),
        reported_ms=reported_ms,
        received_ms=received_ms,
    )


def encode_positions(items: list[PositionItem]) -> bytes:
    return orjson.dumps({"items": items})


def decode_positions(raw: bytes) -> list[PositionItem]:
    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise CodecError(f"invalid json: {exc}") from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise CodecError("positions payload must carry an items array")
    decoded: list[PositionItem] = []
    for item in items:
        if not isinstance(item, list) or len(item) != 4:
            raise CodecError("position item must be a 4-element array")
        device_id, lat, lon, reported_ms = item
        if not isinstance(device_id, str):
            raise CodecError("device_id must be a string")
        decoded.append((device_id, float(lat), float(lon), int(reported_ms)))
    return decoded
