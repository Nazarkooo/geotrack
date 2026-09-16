"""Geozone CRUD: ownership, validation, optimistic concurrency and live zone events."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import orjson
import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from geotrack.api.problems import MEDIA_TYPE
from geotrack.messaging.keys import user_channel
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.conftest import login, running_app

KYIV = {"latitude": 50.4501, "longitude": 30.5234}
ZONE = {"name": "Depot", "radius_m": 500.0, **KYIV}


@pytest.fixture
async def owner(api: AsyncClient) -> dict[str, str]:
    return await login(api, "owner")


@pytest.fixture
async def stranger(api: AsyncClient) -> dict[str, str]:
    return await login(api, "stranger")


async def _create(api: AsyncClient, headers: dict[str, str], **overrides: Any) -> dict[str, Any]:
    response = await api.post("/api/v1/geozones", headers=headers, json=ZONE | overrides)
    assert response.status_code == 201, response.text
    zone: dict[str, Any] = response.json()
    return zone


async def test_create_read_update_delete(api: AsyncClient, owner: dict[str, str]) -> None:
    created = await api.post("/api/v1/geozones", headers=owner, json=ZONE)
    assert created.status_code == 201
    zone = created.json()
    assert created.headers["Location"] == f"/api/v1/geozones/{zone['id']}"
    assert created.headers["ETag"] == '"1"'
    # A zone created without a colour still gets one, so the map never draws a blank.
    assert zone["color"].startswith("#")
    assert (zone["latitude"], zone["longitude"]) == (KYIV["latitude"], KYIV["longitude"])

    read = await api.get(f"/api/v1/geozones/{zone['id']}", headers=owner)
    assert read.status_code == 200
    assert read.json() == zone

    patched = await api.patch(
        f"/api/v1/geozones/{zone['id']}", headers=owner, json={"radius_m": 900.0}
    )
    assert patched.status_code == 200
    assert patched.json()["radius_m"] == 900.0
    assert patched.json()["version"] == 2
    assert patched.json()["name"] == "Depot"  # untouched fields survive

    replaced = await api.put(
        f"/api/v1/geozones/{zone['id']}",
        headers=owner,
        json=ZONE | {"name": "Yard", "radius_m": 1_200.0, "alert_on_exit": False},
    )
    assert replaced.status_code == 200
    assert replaced.json()["version"] == 3
    assert replaced.json()["alert_on_exit"] is False

    deleted = await api.delete(f"/api/v1/geozones/{zone['id']}", headers=owner)
    assert deleted.status_code == 204
    assert (await api.get(f"/api/v1/geozones/{zone['id']}", headers=owner)).status_code == 404


async def test_patch_moves_only_the_coordinate_it_was_given(
    api: AsyncClient, owner: dict[str, str]
) -> None:
    zone = await _create(api, owner)

    moved = await api.patch(
        f"/api/v1/geozones/{zone['id']}", headers=owner, json={"latitude": 49.8397}
    )

    assert moved.status_code == 200
    assert moved.json()["latitude"] == pytest.approx(49.8397)
    assert moved.json()["longitude"] == pytest.approx(KYIV["longitude"])


async def test_patch_can_clear_the_dwell_interval_but_not_a_required_field(
    api: AsyncClient, owner: dict[str, str]
) -> None:
    zone = await _create(api, owner, dwell_alert_interval_s=60)

    cleared = await api.patch(
        f"/api/v1/geozones/{zone['id']}", headers=owner, json={"dwell_alert_interval_s": None}
    )
    blanked = await api.patch(f"/api/v1/geozones/{zone['id']}", headers=owner, json={"name": None})

    assert cleared.status_code == 200
    assert cleared.json()["dwell_alert_interval_s"] is None
    assert blanked.status_code == 422
    assert "name" in blanked.json()["detail"]


async def test_an_empty_patch_changes_nothing(api: AsyncClient, owner: dict[str, str]) -> None:
    zone = await _create(api, owner)

    response = await api.patch(f"/api/v1/geozones/{zone['id']}", headers=owner, json={})

    assert response.status_code == 200
    assert response.json() == zone


async def test_listing_is_paged_and_counted(api: AsyncClient, owner: dict[str, str]) -> None:
    for index in range(5):
        await _create(api, owner, name=f"zone {index}")

    page = await api.get("/api/v1/geozones", headers=owner, params={"limit": 2, "offset": 2})

    assert page.status_code == 200
    assert page.json()["total"] == 5
    assert [item["name"] for item in page.json()["items"]] == ["zone 2", "zone 3"]


@pytest.mark.parametrize(
    "payload",
    [
        {"radius_m": 9.0},
        {"radius_m": 50_001.0},
        {"latitude": 91.0},
        {"longitude": -181.0},
        {"name": "   "},
        {"name": "x" * 81},
        {"color": "red"},
        {"dwell_alert_interval_s": 5},
        {"unknown_field": 1},
    ],
)
async def test_invalid_zone_bodies_are_rejected(
    api: AsyncClient, owner: dict[str, str], payload: dict[str, Any]
) -> None:
    response = await api.post("/api/v1/geozones", headers=owner, json=ZONE | payload)

    assert response.status_code == 422
    assert response.headers["content-type"].startswith(MEDIA_TYPE)


async def test_another_account_cannot_see_or_touch_the_zone(
    api: AsyncClient, owner: dict[str, str], stranger: dict[str, str]
) -> None:
    zone = await _create(api, owner)
    url = f"/api/v1/geozones/{zone['id']}"

    responses = [
        await api.get(url, headers=stranger),
        await api.put(url, headers=stranger, json=ZONE),
        await api.patch(url, headers=stranger, json={"name": "stolen"}),
        await api.delete(url, headers=stranger),
        await api.get(f"{url}/devices", headers=stranger),
    ]

    # 404 rather than 403 everywhere: a stranger must not learn that the id exists.
    assert [response.status_code for response in responses] == [404, 404, 404, 404, 404]
    assert (await api.get("/api/v1/geozones", headers=stranger)).json()["total"] == 0
    assert (await api.get(url, headers=owner)).status_code == 200


async def test_if_match_guards_concurrent_edits(api: AsyncClient, owner: dict[str, str]) -> None:
    zone = await _create(api, owner)
    url = f"/api/v1/geozones/{zone['id']}"

    accepted = await api.patch(url, headers=owner | {"If-Match": '"1"'}, json={"radius_m": 800.0})
    stale = await api.patch(url, headers=owner | {"If-Match": '"1"'}, json={"radius_m": 700.0})

    assert accepted.status_code == 200
    assert stale.status_code == 412
    assert stale.json()["current_version"] == 2
    assert stale.headers["ETag"] == '"2"'
    # The losing write left nothing behind.
    assert (await api.get(url, headers=owner)).json()["radius_m"] == 800.0


async def test_if_match_is_honoured_on_put_and_delete(
    api: AsyncClient, owner: dict[str, str]
) -> None:
    zone = await _create(api, owner)
    url = f"/api/v1/geozones/{zone['id']}"

    put = await api.put(url, headers=owner | {"If-Match": '"7"'}, json=ZONE)
    delete = await api.delete(url, headers=owner | {"If-Match": 'W/"7"'})
    wildcard = await api.delete(url, headers=owner | {"If-Match": "*"})

    assert put.status_code == 412
    assert delete.status_code == 412
    assert wildcard.status_code == 204


@pytest.mark.parametrize("tag", ['"99999999999"', '"-4294967296"'])
async def test_an_if_match_no_version_could_hold_is_a_precondition_failure(
    api: AsyncClient, owner: dict[str, str], tag: str
) -> None:
    # Versions live in a 32-bit column. A tag outside that range is still a well-formed
    # validator that does not match, and a client round-tripping an opaque ETag — or a
    # proxy rewriting one — must not be able to turn the 412 it deserves into a 500.
    zone = await _create(api, owner)
    url = f"/api/v1/geozones/{zone['id']}"

    patched = await api.patch(url, headers=owner | {"If-Match": tag}, json={"radius_m": 800.0})
    replaced = await api.put(url, headers=owner | {"If-Match": tag}, json=ZONE)
    deleted = await api.delete(url, headers=owner | {"If-Match": tag})

    assert [patched.status_code, replaced.status_code, deleted.status_code] == [412, 412, 412]
    assert patched.json()["current_version"] == 1
    assert (await api.get(url, headers=owner)).json()["radius_m"] == ZONE["radius_m"]


async def test_an_if_match_no_version_could_hold_still_hides_another_account(
    api: AsyncClient, owner: dict[str, str], stranger: dict[str, str]
) -> None:
    zone = await _create(api, owner)

    response = await api.delete(
        f"/api/v1/geozones/{zone['id']}", headers=stranger | {"If-Match": '"99999999999"'}
    )

    # Still 404: the header must not become a way to ask whether an id exists.
    assert response.status_code == 404


async def test_a_malformed_if_match_is_a_bad_request(
    api: AsyncClient, owner: dict[str, str]
) -> None:
    zone = await _create(api, owner)

    response = await api.patch(
        f"/api/v1/geozones/{zone['id']}",
        headers=owner | {"If-Match": '"not-a-version"'},
        json={"radius_m": 800.0},
    )

    assert response.status_code == 400
    assert response.json()["code"] == "bad_request"


@pytest.fixture
async def small_quota_api(settings: Settings) -> AsyncIterator[AsyncClient]:
    async with running_app(
        make_settings(
            database_url=settings.database_url.get_secret_value(),
            redis_url=settings.redis_url.get_secret_value(),
            geozone_quota_per_user=3,
        )
    ) as client:
        yield client


async def test_the_quota_is_enforced_even_under_concurrent_creates(
    small_quota_api: AsyncClient,
) -> None:
    headers = await login(small_quota_api, "hoarder")

    responses = await asyncio.gather(
        *(small_quota_api.post("/api/v1/geozones", headers=headers, json=ZONE) for _ in range(8))
    )

    codes = [response.status_code for response in responses]
    assert codes.count(201) == 3
    assert codes.count(409) == 5
    rejected = next(response for response in responses if response.status_code == 409)
    assert rejected.json()["code"] == "zone_quota_exceeded"
    assert (await small_quota_api.get("/api/v1/geozones", headers=headers)).json()["total"] == 3


async def test_a_token_for_a_removed_account_cannot_create_zones(
    api: AsyncClient, owner: dict[str, str], engine: AsyncEngine
) -> None:
    user_id = (await api.get("/api/v1/auth/me", headers=owner)).json()["id"]
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})

    response = await api.post("/api/v1/geozones", headers=owner, json=ZONE)

    # The token is still validly signed, so this has to be a deliberate answer rather
    # than a foreign key violation escaping as a 500.
    assert response.status_code == 401
    assert response.json()["code"] == "unauthorized"


async def test_mutations_are_announced_on_the_owner_channel(
    api: AsyncClient, owner: dict[str, str], redis_client: Redis
) -> None:
    user_id = (await api.get("/api/v1/auth/me", headers=owner)).json()["id"]
    pubsub: Any = redis_client.pubsub()
    await pubsub.subscribe(user_channel(UUID(user_id)))
    await _drain_subscribe_confirmation(pubsub)

    zone = await _create(api, owner)
    await api.patch(f"/api/v1/geozones/{zone['id']}", headers=owner, json={"name": "Renamed"})
    await api.delete(f"/api/v1/geozones/{zone['id']}", headers=owner)

    frames = [orjson.loads(await _next_message(pubsub)) for _ in range(3)]
    await pubsub.aclose()

    assert [frame["op"] for frame in frames] == ["created", "updated", "deleted"]
    assert frames[0]["zone"]["id"] == zone["id"]
    assert frames[1]["zone"]["name"] == "Renamed"
    # A delete carries only the id: there is no zone left to describe.
    assert frames[2]["zone"] == {"id": zone["id"]}


async def test_presence_and_devices_only_report_the_callers_zones(
    api: AsyncClient,
    owner: dict[str, str],
    stranger: dict[str, str],
    engine: AsyncEngine,
) -> None:
    inside = await _create(api, owner, name="inside", radius_m=1_000.0)
    empty = await _create(api, owner, name="empty", latitude=-33.8688, longitude=151.2093)
    foreign = await _create(api, stranger, name="foreign")
    await _store_position(engine, "dev-inside", lat=50.4505, lon=30.5240)
    await _store_position(engine, "dev-far", lat=10.0, lon=10.0)
    await _mark_presence(engine, inside["id"], "dev-inside")
    await _mark_presence(engine, foreign["id"], "dev-foreign")

    presence = await api.get("/api/v1/geozones/presence", headers=owner)
    devices = await api.get(f"/api/v1/geozones/{inside['id']}/devices", headers=owner)

    assert presence.status_code == 200
    assert presence.json()["zones"] == {inside["id"]: ["dev-inside"], empty["id"]: []}
    assert [item["device_id"] for item in devices.json()["items"]] == ["dev-inside"]


async def _store_position(engine: AsyncEngine, device_id: str, *, lat: float, lon: float) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO device_positions (device_id, position, reported_at, received_at) "
                "VALUES (:device_id, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography, "
                "now(), now())"
            ),
            {"device_id": device_id, "lat": lat, "lon": lon},
        )


async def _mark_presence(engine: AsyncEngine, zone_id: str, device_id: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO zone_presence (zone_id, device_id, entered_at, last_seen_at, "
                "last_alert_at) VALUES (:zone_id, :device_id, now(), now(), now())"
            ),
            {"zone_id": zone_id, "device_id": device_id},
        )


async def _drain_subscribe_confirmation(pubsub: Any) -> None:
    async with asyncio.timeout(5):
        while True:
            message = await pubsub.get_message(timeout=1.0)
            if message is not None and message["type"] == "subscribe":
                return


async def _next_message(pubsub: Any) -> bytes:
    async with asyncio.timeout(5):
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is not None:
                data: bytes = message["data"]
                return data
