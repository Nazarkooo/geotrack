"""The websocket wire protocol, version 1.

Every frame is UTF-8 JSON. Control frames are built from pydantic models; position
frames are assembled from pre-serialised per-cell chunks, so one tick's items are
serialised once and reused for every client that can see them.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Self
from uuid import UUID

import orjson

from geotrack.geo import BBox
from geotrack.messaging.codec import PositionItem
from geotrack.schemas.alerts import AlertOut
from geotrack.schemas.auth import UserOut
from geotrack.schemas.geozones import GeozoneOut

PROTOCOL_VERSION = 1

ZoneOp = Literal["created", "updated", "deleted"]

# Close codes. 1013 ("try again later") is the standard way to shed a client that
# cannot keep up; the 44xx range is application specific.
CLOSE_SLOW_CONSUMER = 1013
CLOSE_UNAUTHORIZED = 4401
CLOSE_TOO_MANY_SESSIONS = 4429
CLOSE_PROTOCOL_ERROR = 1008


class ProtocolError(ValueError):
    """A client sent something that is not a valid protocol message."""


@dataclass(frozen=True, slots=True)
class SessionInfo:
    id: UUID
    label: str
    connected_at: datetime


@dataclass(frozen=True, slots=True)
class ViewportMessage:
    bbox: BBox


@dataclass(frozen=True, slots=True)
class PingMessage:
    t: int


type ClientMessage = ViewportMessage | PingMessage


@dataclass(frozen=True, slots=True)
class IngestMessage:
    """A device frame: the payload is validated by the ingest schemas, not here."""

    raw: bytes

    @classmethod
    def of(cls, raw: bytes | str) -> Self:
        return cls(raw.encode() if isinstance(raw, str) else raw)


def _dumps(payload: object) -> bytes:
    return orjson.dumps(payload, default=str)


def hello_frame(*, session_id: UUID, user: UserOut, tick_ms: int, server_t: int) -> bytes:
    return _dumps(
        {
            "type": "hello",
            "protocol": PROTOCOL_VERSION,
            "session_id": str(session_id),
            "user": user.model_dump(mode="json"),
            "tick_ms": tick_ms,
            "server_t": server_t,
        }
    )


def encode_position_item(item: PositionItem) -> bytes:
    """One device's latest position: ``["dev-1", lat, lon, reported_ms]``."""
    return _dumps(item)


def positions_frame(
    *,
    full: bool,
    t_ms: int,
    item_chunks: Iterable[bytes],
    removed: Sequence[str] = (),
) -> bytes:
    body = b",".join(item_chunks)
    return b"".join(
        (
            b'{"type":"positions","full":',
            b"true" if full else b"false",
            b',"t":',
            str(t_ms).encode(),
            b',"items":[',
            body,
            b'],"removed":',
            _dumps(removed),
            b"}",
        )
    )


def alert_frame(alert: AlertOut) -> bytes:
    return _dumps({"type": "alert", "alert": alert.model_dump(mode="json")})


def zone_frame(op: ZoneOp, zone: GeozoneOut | UUID) -> bytes:
    body = {"id": str(zone)} if isinstance(zone, UUID) else zone.model_dump(mode="json")
    return _dumps({"type": "zone", "op": op, "zone": body})


def sessions_frame(sessions: Sequence[SessionInfo]) -> bytes:
    return _dumps(
        {
            "type": "sessions",
            "count": len(sessions),
            "sessions": [
                {
                    "id": str(session.id),
                    "label": session.label,
                    "connected_at": session.connected_at.isoformat(),
                }
                for session in sessions
            ],
        }
    )


def stats_frame(
    *, t_ms: int, devices: int, updates_per_s: float, connections: int, backlog: int
) -> bytes:
    return _dumps(
        {
            "type": "stats",
            "t": t_ms,
            "devices": devices,
            "updates_per_s": round(updates_per_s, 1),
            "connections": connections,
            "backlog": backlog,
        }
    )


def pong_frame(*, t: int, server_t: int) -> bytes:
    return _dumps({"type": "pong", "t": t, "server_t": server_t})


def error_frame(code: str, detail: str, *, seq: int | None = None) -> bytes:
    payload: dict[str, object] = {"type": "error", "code": code, "detail": detail}
    if seq is not None:
        payload["seq"] = seq
    return _dumps(payload)


def ack_frame(*, seq: int, accepted: int) -> bytes:
    return _dumps({"type": "ack", "seq": seq, "accepted": accepted})


def throttle_frame(*, retry_after_ms: int) -> bytes:
    return _dumps({"type": "throttle", "retry_after_ms": retry_after_ms})


def parse_client_message(raw: bytes | str) -> ClientMessage:
    data = raw.encode() if isinstance(raw, str) else raw
    try:
        payload = orjson.loads(data)
    except orjson.JSONDecodeError as exc:
        raise ProtocolError(f"invalid json: {exc}") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("message must be a json object")

    match payload.get("type"):
        case "viewport":
            bbox = payload.get("bbox")
            if not isinstance(bbox, list) or len(bbox) != 4:
                raise ProtocolError("viewport requires bbox [west, south, east, north]")
            try:
                west, south, east, north = (float(value) for value in bbox)
                return ViewportMessage(BBox(west=west, south=south, east=east, north=north))
            except (TypeError, ValueError) as exc:
                raise ProtocolError(f"invalid bbox: {exc}") from exc
        case "ping":
            t = payload.get("t", 0)
            if not isinstance(t, int):
                raise ProtocolError("ping requires an integer t")
            return PingMessage(t)
        case unknown:
            raise ProtocolError(f"unsupported message type: {unknown!r}")
