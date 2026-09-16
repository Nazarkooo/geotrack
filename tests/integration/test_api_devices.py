"""Device positions and tracks: bbox filtering, including the awkward boxes."""

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.clock import utc_now
from tests.integration.conftest import login


@pytest.fixture
async def viewer(api: AsyncClient) -> dict[str, str]:
    return await login(api, "viewer")


async def _store(engine: AsyncEngine, device_id: str, *, lat: float, lon: float) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO device_positions (device_id, position, reported_at, received_at) "
                "VALUES (:device_id, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography, "
                "now(), now())"
            ),
            {"device_id": device_id, "lat": lat, "lon": lon},
        )


async def _history(
    engine: AsyncEngine, device_id: str, moments: list[datetime], *, lon: float = 30.5
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO location_history (device_id, position, reported_at, received_at) "
                "VALUES (:device_id, ST_SetSRID(ST_MakePoint(:lon, 50.45), 4326)::geography, "
                ":moment, :moment)"
            ),
            [{"device_id": device_id, "lon": lon, "moment": moment} for moment in moments],
        )


async def test_bbox_selects_only_the_devices_inside_it(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    await _store(engine, "kyiv", lat=50.45, lon=30.52)
    await _store(engine, "sydney", lat=-33.87, lon=151.21)

    response = await api.get(
        "/api/v1/devices", headers=viewer, params={"bbox": "30.0,50.0,31.0,51.0"}
    )

    assert response.status_code == 200
    assert [item["device_id"] for item in response.json()["items"]] == ["kyiv"]
    assert response.json()["items"][0]["latitude"] == pytest.approx(50.45)


async def test_a_wide_bbox_keeps_devices_near_the_middle_of_its_edges(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    # A device sitting on the southern edge of a 90-degree-wide box. The geodetic
    # bounding box of that rectangle bows polewards between the corners, so a GiST
    # prefilter built from it would exclude this device although it is plainly inside.
    await _store(engine, "edge", lat=10.0, lon=0.0)

    response = await api.get("/api/v1/devices", headers=viewer, params={"bbox": "-45,10,45,20"})

    assert [item["device_id"] for item in response.json()["items"]] == ["edge"]


async def test_a_bbox_across_the_antimeridian_is_split(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    await _store(engine, "west-of-line", lat=12.0, lon=179.9)
    await _store(engine, "east-of-line", lat=12.0, lon=-179.9)
    await _store(engine, "greenwich", lat=12.0, lon=0.0)

    response = await api.get(
        "/api/v1/devices", headers=viewer, params={"bbox": "179.0,11.0,-179.0,13.0"}
    )

    assert sorted(item["device_id"] for item in response.json()["items"]) == [
        "east-of-line",
        "west-of-line",
    ]


async def test_without_a_bbox_everything_is_returned_up_to_the_limit(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    for index in range(5):
        await _store(engine, f"dev-{index}", lat=50.0 + index, lon=30.0)

    unfiltered = await api.get("/api/v1/devices", headers=viewer)
    limited = await api.get("/api/v1/devices", headers=viewer, params={"limit": 2})

    assert len(unfiltered.json()["items"]) == 5
    assert [item["device_id"] for item in limited.json()["items"]] == ["dev-0", "dev-1"]


@pytest.mark.parametrize("bbox", ["1,2,3", "a,b,c,d", "0,0,0,200", "0,90,10,-90"])
async def test_a_broken_bbox_is_a_validation_problem(
    api: AsyncClient, viewer: dict[str, str], bbox: str
) -> None:
    response = await api.get("/api/v1/devices", headers=viewer, params={"bbox": bbox})

    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"


async def test_an_unknown_device_is_a_not_found_problem(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    await _store(engine, "known", lat=50.45, lon=30.52)

    known = await api.get("/api/v1/devices/known", headers=viewer)
    unknown = await api.get("/api/v1/devices/ghost", headers=viewer)

    assert known.status_code == 200
    assert known.json()["device_id"] == "known"
    assert unknown.status_code == 404
    assert unknown.json()["code"] == "not_found"


async def test_a_device_id_outside_the_allowed_alphabet_is_rejected(
    api: AsyncClient, viewer: dict[str, str]
) -> None:
    response = await api.get("/api/v1/devices/has%20space", headers=viewer)

    assert response.status_code == 422


async def test_track_is_ordered_oldest_first_and_bounded(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    now = utc_now().replace(microsecond=0)
    moments = [now - timedelta(minutes=minutes) for minutes in (30, 20, 10, 5)]
    await _history(engine, "tracked", moments)

    full = await api.get("/api/v1/devices/tracked/track", headers=viewer)
    clipped = await api.get("/api/v1/devices/tracked/track", headers=viewer, params={"limit": 2})

    stamps = [point["reported_at"] for point in full.json()["points"]]
    assert stamps == sorted(stamps)
    assert len(stamps) == 4
    # A truncated track keeps the newest points: a stale head is useless on a map.
    assert [point["reported_at"] for point in clipped.json()["points"]] == stamps[-2:]


async def test_track_respects_the_requested_window(
    api: AsyncClient, viewer: dict[str, str], engine: AsyncEngine
) -> None:
    now = utc_now().replace(microsecond=0)
    await _history(engine, "tracked", [now - timedelta(hours=hours) for hours in (5, 3, 1)])

    response = await api.get(
        "/api/v1/devices/tracked/track",
        headers=viewer,
        params={"since": (now - timedelta(hours=4)).isoformat()},
    )

    assert len(response.json()["points"]) == 2


async def test_track_rejects_an_inverted_window(api: AsyncClient, viewer: dict[str, str]) -> None:
    now = datetime.now(UTC)
    response = await api.get(
        "/api/v1/devices/tracked/track",
        headers=viewer,
        params={"since": now.isoformat(), "until": (now - timedelta(hours=1)).isoformat()},
    )

    assert response.status_code == 422


async def test_a_device_with_no_history_returns_an_empty_track(
    api: AsyncClient, viewer: dict[str, str]
) -> None:
    response = await api.get("/api/v1/devices/silent/track", headers=viewer)

    assert response.status_code == 200
    assert response.json() == {"device_id": "silent", "points": []}


async def test_device_endpoints_require_a_token(api: AsyncClient) -> None:
    assert (await api.get("/api/v1/devices")).status_code == 401
    assert (await api.get("/api/v1/devices/anything")).status_code == 401
    assert (await api.get("/api/v1/devices/anything/track")).status_code == 401
