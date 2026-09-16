"""Alert paging: stable under concurrent inserts, filtered, and never cross-account."""

from typing import Any
from uuid import UUID

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.conftest import login

ZONE = {"name": "Depot", "latitude": 50.4501, "longitude": 30.5234, "radius_m": 500.0}


async def _insert_alert(
    engine: AsyncEngine,
    *,
    user_id: UUID,
    zone_id: UUID | None,
    device_id: str,
    kind: str = "enter",
) -> int:
    async with engine.begin() as conn:
        alert_id = await conn.scalar(
            text(
                "INSERT INTO alerts (user_id, zone_id, zone_name, device_id, kind, position, "
                "occurred_at) VALUES (:user_id, :zone_id, 'Depot', :device_id, CAST(:kind AS "
                "alert_kind), ST_SetSRID(ST_MakePoint(30.52, 50.45), 4326)::geography, now()) "
                "RETURNING id"
            ),
            {"user_id": user_id, "zone_id": zone_id, "device_id": device_id, "kind": kind},
        )
    return int(alert_id or 0)


async def _account(api: AsyncClient, username: str) -> tuple[dict[str, str], UUID]:
    headers = await login(api, username)
    me = await api.get("/api/v1/auth/me", headers=headers)
    return headers, UUID(me.json()["id"])


async def _zone_of(api: AsyncClient, headers: dict[str, str], **overrides: Any) -> UUID:
    response = await api.post("/api/v1/geozones", headers=headers, json=ZONE | overrides)
    return UUID(response.json()["id"])


async def test_alerts_come_back_newest_first_with_the_rest_shape(
    api: AsyncClient, engine: AsyncEngine
) -> None:
    headers, user_id = await _account(api, "watcher")
    zone_id = await _zone_of(api, headers)
    ids = [
        await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id=f"dev-{i}")
        for i in range(3)
    ]

    response = await api.get("/api/v1/alerts", headers=headers)

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == sorted(ids, reverse=True)
    assert items[0]["zone"] == {"id": str(zone_id), "name": "Depot"}
    assert (items[0]["latitude"], items[0]["longitude"]) == (50.45, 30.52)
    assert items[0]["kind"] == "enter"


async def test_paging_stays_stable_while_new_alerts_arrive(
    api: AsyncClient, engine: AsyncEngine
) -> None:
    headers, user_id = await _account(api, "pager")
    zone_id = await _zone_of(api, headers)
    older = [
        await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id=f"old-{i}")
        for i in range(6)
    ]

    first = await api.get("/api/v1/alerts", headers=headers, params={"limit": 2})
    cursor = first.json()["next_before_id"]
    # Alerts keep arriving between the two page requests; an offset-based pager would
    # now repeat rows, a keyset pager cannot.
    await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id="fresh")
    second = await api.get(
        "/api/v1/alerts", headers=headers, params={"limit": 2, "before_id": cursor}
    )

    assert [item["id"] for item in first.json()["items"]] == older[-1:-3:-1]
    assert [item["id"] for item in second.json()["items"]] == older[-3:-5:-1]


async def test_the_last_page_reports_no_cursor(api: AsyncClient, engine: AsyncEngine) -> None:
    headers, user_id = await _account(api, "tail")
    zone_id = await _zone_of(api, headers)
    for index in range(3):
        await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id=f"dev-{index}")

    full = await api.get("/api/v1/alerts", headers=headers, params={"limit": 3})
    short = await api.get("/api/v1/alerts", headers=headers, params={"limit": 10})

    assert full.json()["next_before_id"] is not None
    assert short.json()["next_before_id"] is None


async def test_after_id_backfills_what_a_dropped_session_missed(
    api: AsyncClient, engine: AsyncEngine
) -> None:
    headers, user_id = await _account(api, "reconnect")
    zone_id = await _zone_of(api, headers)
    seen = await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id="dev-a")
    missed = [
        await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id=f"dev-{i}")
        for i in range(2)
    ]

    response = await api.get("/api/v1/alerts", headers=headers, params={"after_id": seen})

    assert [item["id"] for item in response.json()["items"]] == sorted(missed, reverse=True)


async def test_a_backfill_wider_than_one_page_is_walked_with_the_cursor(
    api: AsyncClient, engine: AsyncEngine
) -> None:
    # ``after_id`` bounds the range from below; the page is still the newest slice of it,
    # so a session that was away for longer than one page has to follow the cursor to see
    # everything. This is the procedure the endpoint documents, walked end to end.
    headers, user_id = await _account(api, "longgap")
    zone_id = await _zone_of(api, headers)
    seen = await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id="dev-a")
    missed = [
        await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id=f"dev-{i}")
        for i in range(5)
    ]

    collected: list[int] = []
    params: dict[str, Any] = {"after_id": seen, "limit": 2}
    for _ in range(len(missed) + 1):
        page = (await api.get("/api/v1/alerts", headers=headers, params=params)).json()
        collected += [item["id"] for item in page["items"]]
        if page["next_before_id"] is None:
            break
        params = params | {"before_id": page["next_before_id"]}

    assert collected == sorted(missed, reverse=True)
    # And the first page on its own is only the newest two, which is why the walk exists.
    assert collected[:2] == sorted(missed, reverse=True)[:2]


async def test_filters_narrow_by_zone_and_device(api: AsyncClient, engine: AsyncEngine) -> None:
    headers, user_id = await _account(api, "filterer")
    depot = await _zone_of(api, headers)
    yard = await _zone_of(api, headers, name="Yard")
    await _insert_alert(engine, user_id=user_id, zone_id=depot, device_id="truck-1")
    await _insert_alert(engine, user_id=user_id, zone_id=yard, device_id="truck-2")
    await _insert_alert(engine, user_id=user_id, zone_id=yard, device_id="truck-1", kind="exit")

    by_zone = await api.get("/api/v1/alerts", headers=headers, params={"zone_id": str(yard)})
    by_device = await api.get("/api/v1/alerts", headers=headers, params={"device_id": "truck-1"})
    both = await api.get(
        "/api/v1/alerts", headers=headers, params={"zone_id": str(yard), "device_id": "truck-1"}
    )

    assert len(by_zone.json()["items"]) == 2
    assert len(by_device.json()["items"]) == 2
    assert [item["kind"] for item in both.json()["items"]] == ["exit"]


async def test_another_accounts_alerts_are_invisible(api: AsyncClient, engine: AsyncEngine) -> None:
    mine, my_id = await _account(api, "mine")
    theirs, their_id = await _account(api, "theirs")
    my_zone = await _zone_of(api, mine)
    their_zone = await _zone_of(api, theirs, name="Theirs")
    await _insert_alert(engine, user_id=my_id, zone_id=my_zone, device_id="dev-a")
    await _insert_alert(engine, user_id=their_id, zone_id=their_zone, device_id="dev-b")

    response = await api.get("/api/v1/alerts", headers=mine)

    assert [item["device_id"] for item in response.json()["items"]] == ["dev-a"]


async def test_a_deleted_zone_leaves_its_alerts_readable(
    api: AsyncClient, engine: AsyncEngine
) -> None:
    headers, user_id = await _account(api, "historian")
    zone_id = await _zone_of(api, headers)
    await _insert_alert(engine, user_id=user_id, zone_id=zone_id, device_id="dev-a")

    await api.delete(f"/api/v1/geozones/{zone_id}", headers=headers)
    response = await api.get("/api/v1/alerts", headers=headers)

    # The reference is dropped but the name stays, so the feed still reads sensibly.
    assert response.json()["items"][0]["zone"] == {"id": None, "name": "Depot"}


async def test_alerts_require_a_token(api: AsyncClient) -> None:
    assert (await api.get("/api/v1/alerts")).status_code == 401
