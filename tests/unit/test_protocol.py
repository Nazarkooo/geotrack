from datetime import UTC, datetime
from uuid import UUID

import orjson
import pytest

from geotrack.geo import BBox
from geotrack.realtime.protocol import (
    PingMessage,
    ProtocolError,
    SessionInfo,
    ViewportMessage,
    alert_frame,
    encode_position_item,
    hello_frame,
    parse_client_message,
    positions_frame,
    sessions_frame,
    zone_frame,
)
from geotrack.schemas.alerts import AlertKind, AlertOut, AlertZoneRef
from geotrack.schemas.auth import UserOut
from geotrack.schemas.geozones import GeozoneOut

USER = UserOut(id=UUID("01929f6e-0000-7000-8000-000000000001"), username="nazar")
ZONE_ID = UUID("01929f6e-0000-7000-8000-000000000002")
MOMENT = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def test_hello_frame_carries_session_and_user() -> None:
    frame = orjson.loads(
        hello_frame(session_id=ZONE_ID, user=USER, tick_ms=250, server_t=1_789_646_400_000)
    )

    assert frame["type"] == "hello"
    assert frame["protocol"] == 1
    assert frame["session_id"] == str(ZONE_ID)
    assert frame["user"] == {"id": str(USER.id), "username": "nazar"}
    assert frame["tick_ms"] == 250


@pytest.mark.parametrize("count", [0, 1, 3])
def test_positions_frame_is_valid_json_for_any_chunk_count(count: int) -> None:
    items = [(f"dev-{i}", 50.0 + i, 30.0 + i, 1_000 + i) for i in range(count)]
    chunks = [encode_position_item(item) for item in items]

    frame = orjson.loads(positions_frame(full=False, t_ms=42, item_chunks=chunks, removed=["gone"]))

    assert frame["type"] == "positions"
    assert frame["full"] is False
    assert frame["t"] == 42
    assert frame["removed"] == ["gone"]
    assert frame["items"] == [list(item) for item in items]


def test_positions_frame_marks_snapshots() -> None:
    frame = orjson.loads(positions_frame(full=True, t_ms=1, item_chunks=[]))

    assert frame["full"] is True
    assert frame["items"] == []
    assert frame["removed"] == []


def test_alert_frame_uses_the_rest_representation() -> None:
    alert = AlertOut(
        id=12,
        kind=AlertKind.ENTER,
        zone=AlertZoneRef(id=ZONE_ID, name="Depot"),
        device_id="dev-1",
        latitude=50.45,
        longitude=30.52,
        occurred_at=MOMENT,
        created_at=MOMENT,
    )

    frame = orjson.loads(alert_frame(alert))

    assert frame["type"] == "alert"
    assert frame["alert"]["kind"] == "enter"
    assert frame["alert"]["zone"] == {"id": str(ZONE_ID), "name": "Depot"}
    assert frame["alert"]["latitude"] == 50.45


def test_zone_frame_supports_create_and_delete() -> None:
    zone = GeozoneOut(
        id=ZONE_ID,
        name="Depot",
        color="#3fb1ff",
        latitude=50.45,
        longitude=30.52,
        radius_m=500.0,
        alert_on_enter=True,
        alert_on_exit=True,
        dwell_alert_interval_s=None,
        version=1,
        created_at=MOMENT,
        updated_at=MOMENT,
    )

    created = orjson.loads(zone_frame("created", zone))
    deleted = orjson.loads(zone_frame("deleted", ZONE_ID))

    assert created["op"] == "created"
    assert created["zone"]["radius_m"] == 500.0
    assert deleted == {"type": "zone", "op": "deleted", "zone": {"id": str(ZONE_ID)}}


def test_sessions_frame_lists_every_session() -> None:
    frame = orjson.loads(
        sessions_frame([SessionInfo(id=ZONE_ID, label="Chrome on macOS", connected_at=MOMENT)])
    )

    assert frame["count"] == 1
    assert frame["sessions"][0]["label"] == "Chrome on macOS"


def test_parses_viewport_and_ping() -> None:
    viewport = parse_client_message('{"type":"viewport","bbox":[30.2,50.3,30.8,50.6]}')
    ping = parse_client_message(b'{"type":"ping","t":17}')

    assert viewport == ViewportMessage(BBox(west=30.2, south=50.3, east=30.8, north=50.6))
    assert ping == PingMessage(17)


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"[]",
        b'{"type":"unknown"}',
        b'{"type":"viewport"}',
        b'{"type":"viewport","bbox":[1,2,3]}',
        b'{"type":"viewport","bbox":[1,2,3,"x"]}',
        b'{"type":"viewport","bbox":[1,91,3,92]}',
        b'{"type":"ping","t":"soon"}',
    ],
)
def test_rejects_invalid_client_messages(raw: bytes) -> None:
    with pytest.raises(ProtocolError):
        parse_client_message(raw)
