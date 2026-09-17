import orjson
import pytest

from geotrack.geo import BBox
from geotrack.messaging.codec import PositionItem
from geotrack.realtime.grid import cell_of, rects_for
from geotrack.realtime.hub import PositionHub, TickDelta

SIZE = 0.05
STALE_S = 300


@pytest.fixture
def hub() -> PositionHub:
    return PositionHub(cell_size_deg=SIZE, stale_after_s=STALE_S)


def items_of(delta: TickDelta) -> list[PositionItem]:
    decoded: list[PositionItem] = []
    for chunk in delta.chunks.values():
        for device_id, lat, lon, ts in orjson.loads(b"[" + chunk + b"]"):
            decoded.append((device_id, lat, lon, ts))
    return sorted(decoded)


def test_apply_keeps_the_newest_report(hub: PositionHub) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    hub.apply([("dev-1", 50.46, 30.53, 2_000)])
    hub.apply([("dev-1", 0.0, 0.0, 1_500)])
    hub.apply([("dev-1", 1.0, 1.0, 2_000)])

    delta = hub.drain(t_ms=9)

    assert hub.device_count == 1
    assert items_of(delta) == [("dev-1", 50.46, 30.53, 2_000)]
    assert hub.updates == 2


def test_drain_reports_only_what_changed(hub: PositionHub) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000), ("dev-2", 10.0, 10.0, 1_000)])
    hub.drain(t_ms=1)

    hub.apply([("dev-2", 10.001, 10.001, 2_000)])
    delta = hub.drain(t_ms=2)

    assert items_of(delta) == [("dev-2", 10.001, 10.001, 2_000)]
    assert hub.drain(t_ms=3).is_empty


def test_devices_in_one_cell_share_a_chunk(hub: PositionHub) -> None:
    hub.apply([("dev-1", 50.451, 30.521, 1_000), ("dev-2", 50.452, 30.522, 1_000)])

    delta = hub.drain(t_ms=1)

    assert len(delta.chunks) == 1
    assert len(items_of(delta)) == 2


def test_moving_to_another_cell_leaves_a_forwarding_departure(hub: PositionHub) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    origin = cell_of(50.45, 30.52, SIZE)
    hub.drain(t_ms=1)

    hub.apply([("dev-1", 50.45, 31.52, 2_000)])
    delta = hub.drain(t_ms=2)
    destination = cell_of(50.45, 31.52, SIZE)

    assert [(d.device_id, d.moved_to) for d in delta.departures[origin]] == [("dev-1", destination)]
    assert destination in delta.chunks
    assert delta.removed == ()


def test_a_device_that_returns_within_one_tick_does_not_leave_its_own_cell(
    hub: PositionHub,
) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    home = cell_of(50.45, 30.52, SIZE)
    away = cell_of(50.45, 31.52, SIZE)
    hub.drain(t_ms=1)

    hub.apply([("dev-1", 50.45, 31.52, 2_000), ("dev-1", 50.45, 30.52, 3_000)])
    delta = hub.drain(t_ms=2)

    assert home not in delta.departures
    assert [(d.device_id, d.moved_to) for d in delta.departures[away]] == [("dev-1", home)]
    assert items_of(delta) == [("dev-1", 50.45, 30.52, 3_000)]


def test_sweep_drops_devices_that_stopped_reporting(hub: PositionHub) -> None:
    now = 10_000_000
    hub.apply([("old", 50.45, 30.52, now - STALE_S * 1_000 - 1), ("fresh", 50.45, 30.52, now)])
    cell = cell_of(50.45, 30.52, SIZE)
    hub.drain(t_ms=1)

    hub.sweep(now)
    delta = hub.drain(t_ms=2)

    assert hub.device_count == 1
    assert delta.removed == ("old",)
    assert [(d.device_id, d.moved_to) for d in delta.departures[cell]] == [("old", None)]


def test_snapshot_covers_the_viewport_only(hub: PositionHub) -> None:
    hub.apply([("kyiv", 50.45, 30.52, 1_000), ("sydney", -33.87, 151.21, 1_000)])
    rects = rects_for(BBox(west=30.0, south=50.0, east=31.0, north=51.0), SIZE)

    chunks = list(hub.snapshot_chunks(rects))

    assert [i[0] for i in orjson.loads(b"[" + b",".join(chunks) + b"]")] == ["kyiv"]


def test_snapshot_chunks_are_reused_until_the_cell_changes(hub: PositionHub) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    rects = rects_for(BBox(west=30.0, south=50.0, east=31.0, north=51.0), SIZE)

    first = list(hub.snapshot_chunks(rects))
    second = list(hub.snapshot_chunks(rects))

    assert first[0] is second[0]

    hub.apply([("dev-1", 50.451, 30.521, 2_000)])
    third = list(hub.snapshot_chunks(rects))

    assert third[0] is not first[0]
    assert orjson.loads(b"[" + third[0] + b"]")[0][3] == 2_000


def test_discarding_a_tick_drops_the_delta_and_keeps_the_picture(hub: PositionHub) -> None:
    """What a tick nobody is watching costs: the delta, and nothing else."""
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])

    hub.discard_pending()

    assert hub.drain(t_ms=1).is_empty
    assert hub.device_count == 1
    rects = rects_for(BBox(west=30.0, south=50.0, east=31.0, north=51.0), SIZE)
    assert [i[0] for i in orjson.loads(b"[" + b",".join(hub.snapshot_chunks(rects)) + b"]")] == [
        "dev-1"
    ]


def test_an_empty_hub_yields_no_snapshot_chunks(hub: PositionHub) -> None:
    rects = rects_for(BBox(west=-180.0, south=-90.0, east=180.0, north=90.0), SIZE)

    assert list(hub.snapshot_chunks(rects)) == []
    assert hub.drain(t_ms=1).is_empty
