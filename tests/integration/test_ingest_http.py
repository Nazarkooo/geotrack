"""HTTP ingestion against a real Redis: what lands where, and what is refused."""

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta, timezone
from typing import Any

import orjson
import pytest
from httpx import AsyncClient
from prometheus_client import REGISTRY
from redis.asyncio import Redis
from redis.exceptions import RedisError

from geotrack.clock import utc_now
from geotrack.ingest.service import IngestService
from geotrack.messaging.keys import STREAM_FIELD, ingest_stream
from geotrack.settings import Settings
from geotrack.sharding import shard_for
from tests.conftest import TEST_INGEST_KEY, make_settings
from tests.integration.conftest import queued_records, running_app

URL = "/api/v1/ingest/locations"
KEY = {"X-Ingest-Key": TEST_INGEST_KEY}


def _report(device_id: str, **overrides: Any) -> dict[str, Any]:
    return {
        "device_id": device_id,
        "latitude": 50.4501,
        "longitude": 30.5234,
        "timestamp": utc_now().isoformat(),
    } | overrides


def _rejected(reason: str) -> float:
    value = REGISTRY.get_sample_value(
        "geotrack_ingest_rejected_total", {"transport": "http", "reason": reason}
    )
    return value or 0.0


async def test_a_single_report_lands_on_its_device_shard(
    api: AsyncClient, redis_client: Redis
) -> None:
    response = await api.post(URL, headers=KEY, json=_report("truck-7"))

    assert response.status_code == 202
    assert response.json() == {"accepted": 1}
    queued = await queued_records(redis_client)
    assert list(queued) == [ingest_stream(shard_for("truck-7", 8))]
    record = queued[ingest_stream(shard_for("truck-7", 8))][0]
    assert (record.device_id, record.lat, record.lon) == ("truck-7", 50.4501, 30.5234)
    assert record.received_ms >= record.reported_ms - 1_000


async def test_arrays_and_envelopes_are_both_accepted(
    api: AsyncClient, redis_client: Redis
) -> None:
    array = await api.post(URL, headers=KEY, json=[_report("a"), _report("b")])
    envelope = await api.post(URL, headers=KEY, json={"seq": 4, "items": [_report("c")]})

    assert (array.status_code, array.json()) == (202, {"accepted": 2})
    assert (envelope.status_code, envelope.json()) == (202, {"accepted": 1})
    queued = await queued_records(redis_client)
    assert sorted(r.device_id for records in queued.values() for r in records) == ["a", "b", "c"]


async def test_reports_are_spread_across_the_shards_they_belong_to(
    api: AsyncClient, redis_client: Redis
) -> None:
    devices = [f"dev-{index:05d}" for index in range(40)]

    response = await api.post(URL, headers=KEY, json=[_report(device) for device in devices])

    assert response.json() == {"accepted": 40}
    queued = await queued_records(redis_client)
    assert len(queued) > 1  # a single stream would serialise the whole fleet
    for stream, records in queued.items():
        for record in records:
            assert ingest_stream(shard_for(record.device_id, 8)) == stream


async def test_an_unknown_key_is_rejected_before_anything_is_queued(
    api: AsyncClient, redis_client: Redis
) -> None:
    missing = await api.post(URL, json=_report("a"))
    wrong = await api.post(URL, headers={"X-Ingest-Key": "nope"}, json=_report("a"))

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert await queued_records(redis_client) == {}


async def test_a_key_outside_ascii_is_refused_and_not_a_server_error(
    api: AsyncClient, redis_client: Redis
) -> None:
    # Header values are bytes; anything above 0x7f is a wrong key, not a reason to hand
    # the caller a stack trace on the busiest endpoint in the system.
    response = await api.post(URL, headers={b"X-Ingest-Key": "ключ".encode()}, json=_report("a"))

    assert response.status_code == 401
    assert response.json()["code"] == "unauthorized"
    assert await queued_records(redis_client) == {}


async def test_a_key_outside_ascii_still_admits_the_fleet_configured_with_it(
    settings: Settings, redis_client: Redis
) -> None:
    key = "ключ-достатньої-довжини"

    async with running_app(
        make_settings(
            database_url=settings.database_url.get_secret_value(),
            redis_url=settings.redis_url.get_secret_value(),
            ingest_api_key=key,
        )
    ) as client:
        accepted = await client.post(
            URL, headers={b"X-Ingest-Key": key.encode()}, json=_report("kyiv")
        )
        refused = await client.post(
            URL, headers={b"X-Ingest-Key": "інший-ключ-достатньої".encode()}, json=_report("kyiv")
        )

    assert accepted.status_code == 202
    assert refused.status_code == 401
    queued = await queued_records(redis_client)
    assert [record.device_id for records in queued.values() for record in records] == ["kyiv"]


@pytest.mark.parametrize(
    "payload",
    [
        {"device_id": "a"},  # no coordinates at all
        _report("a") | {"latitude": 91.0},
        _report("a") | {"longitude": 200.0},
        _report("a") | {"timestamp": "yesterday"},
        _report("a") | {"extra": "field"},
        _report("has space"),
        {"items": []},
    ],
)
async def test_malformed_reports_are_validation_problems(
    api: AsyncClient, redis_client: Redis, payload: dict[str, Any]
) -> None:
    response = await api.post(URL, headers=KEY, json=payload)

    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"
    assert await queued_records(redis_client) == {}


async def test_a_body_that_is_not_json_is_refused(api: AsyncClient) -> None:
    response = await api.post(URL, headers=KEY, content=b"not json at all")

    assert response.status_code == 422


async def test_a_batch_over_the_limit_is_refused_whole(
    api: AsyncClient, redis_client: Redis
) -> None:
    oversized = [_report(f"dev-{index}") for index in range(1_001)]

    response = await api.post(URL, headers=KEY, json=oversized)

    assert response.status_code == 422
    assert "limit is 1000" in response.json()["detail"]
    assert await queued_records(redis_client) == {}


async def test_an_oversized_batch_is_refused_before_its_reports_are_validated(
    api: AsyncClient, redis_client: Redis
) -> None:
    # The last report is invalid as well. A parser that validated every item first would
    # answer about the latitude; the batch limit has to win, because it is the check that
    # costs nothing and the one that bounds everything the request can go on to cost.
    oversized = [_report(f"dev-{index}") for index in range(1_000)]
    oversized.append(_report("bad", latitude=999.0))
    before = _rejected("validation")

    response = await api.post(URL, headers=KEY, json=oversized)

    assert response.status_code == 422
    assert "limit is 1000" in response.json()["detail"]
    assert "latitude" not in response.text
    # Counted in reports, the unit accepted batches are counted in.
    assert _rejected("validation") - before == 1_001
    assert await queued_records(redis_client) == {}


async def test_the_body_ceiling_follows_the_batch_limit(settings: Settings) -> None:
    # However a client formats them, ten reports cannot fill 64 KiB, so the request is
    # refused by its declared length without reading or parsing a byte of it.
    async with running_app(
        make_settings(
            database_url=settings.database_url.get_secret_value(),
            redis_url=settings.redis_url.get_secret_value(),
            ingest_max_batch=10,
        )
    ) as client:
        response = await client.post(
            URL,
            headers=KEY | {"Content-Type": "application/json", "Content-Length": str(64 * 1024)},
            content=orjson.dumps(_report("a")),
        )

    assert response.status_code == 413
    assert "10 reports" in response.json()["detail"]


async def test_an_oversized_body_is_refused_by_its_declared_length(api: AsyncClient) -> None:
    response = await api.post(
        URL,
        headers=KEY | {"Content-Type": "application/json", "Content-Length": str(3 * 1024 * 1024)},
        content=orjson.dumps(_report("a")),
    )

    assert response.status_code == 413
    assert response.json()["code"] == "payload_too_large"


async def test_a_chunked_body_is_cut_off_at_the_ceiling(
    api: AsyncClient, redis_client: Redis
) -> None:
    async def endless() -> AsyncIterator[bytes]:
        # No Content-Length to inspect, so the only defence is measuring while reading.
        yield b"["
        while True:
            yield orjson.dumps(_report("flood")) + b","

    response = await api.post(
        URL, headers=KEY | {"Content-Type": "application/json"}, content=endless()
    )

    assert response.status_code == 413
    assert await queued_records(redis_client) == {}


async def test_a_content_length_that_is_not_a_number_is_a_bad_request(api: AsyncClient) -> None:
    response = await api.post(
        URL,
        headers=KEY | {"Content-Type": "application/json", "Content-Length": "huge"},
        content=orjson.dumps(_report("a")),
    )

    assert response.status_code == 400
    assert response.json()["code"] == "bad_request"


async def test_every_documented_timestamp_format_reaches_the_queue(
    api: AsyncClient, redis_client: Redis
) -> None:
    # The endpoint advertises ISO-8601, epoch seconds and epoch milliseconds, and reads a
    # timestamp without a zone as UTC. Devices in the field send all four.
    moment = utc_now().replace(microsecond=0)
    epoch_ms = int(moment.timestamp() * 1_000)
    batch = [
        _report("iso", timestamp=moment.isoformat()),
        _report(
            "iso-offset", timestamp=moment.astimezone(timezone(timedelta(hours=3))).isoformat()
        ),
        _report("naive", timestamp=moment.replace(tzinfo=None).isoformat()),
        _report("seconds", timestamp=epoch_ms // 1_000),
        _report("millis", timestamp=epoch_ms),
    ]

    response = await api.post(URL, headers=KEY, json=batch)

    assert response.json() == {"accepted": 5}
    queued = await queued_records(redis_client)
    stamped = {r.device_id: r.reported_ms for records in queued.values() for r in records}
    # Whole seconds throughout, so every spelling lands on the same instant.
    assert stamped == dict.fromkeys(("iso", "iso-offset", "naive", "seconds", "millis"), epoch_ms)


@pytest.mark.parametrize(
    ("offset", "index"),
    [(timedelta(days=-9), 1), (timedelta(minutes=10), 1)],
)
async def test_timestamps_outside_the_window_name_the_offending_item(
    api: AsyncClient, redis_client: Redis, offset: timedelta, index: int
) -> None:
    now = utc_now()
    batch = [
        _report("good", timestamp=now.isoformat()),
        _report("bad", timestamp=(now + offset).isoformat()),
    ]

    response = await api.post(URL, headers=KEY, json=batch)

    assert response.status_code == 422
    assert f"items[{index}]" in response.json()["detail"]
    # The whole batch is refused: a partially accepted batch is impossible to reason about.
    assert await queued_records(redis_client) == {}


@pytest.fixture
async def throttled_api(settings: Settings, redis_client: Redis) -> AsyncIterator[AsyncClient]:
    """An application whose backlog watermark is already exceeded."""
    await redis_client.xadd(ingest_stream(0), {STREAM_FIELD: b"[]"})
    async with running_app(
        make_settings(
            database_url=settings.database_url.get_secret_value(),
            redis_url=settings.redis_url.get_secret_value(),
            ingest_backlog_high=1,
            ingest_backlog_low=0,
            ingest_backlog_poll_ms=20,
        )
    ) as client:
        yield client


async def test_a_full_backlog_answers_503_with_retry_after(throttled_api: AsyncClient) -> None:
    before = _rejected("backpressure")

    response = await throttled_api.post(URL, headers=KEY, json=[_report("a"), _report("b")])

    assert response.status_code == 503
    assert response.json()["code"] == "ingest_throttled"
    assert int(response.headers["Retry-After"]) >= 1
    assert response.json()["retry_after_ms"] > 0
    # The body is deliberately never read, so how many reports were behind this refusal
    # is unknowable. Charging the report counter one per request would undercount a shed
    # fleet by three orders of magnitude in the only situation where the number matters;
    # the request-level view is geotrack_http_requests_total with status 503.
    assert _rejected("backpressure") == before


async def test_ingestion_resumes_once_the_backlog_drains(
    throttled_api: AsyncClient, redis_client: Redis
) -> None:
    assert (await throttled_api.post(URL, headers=KEY, json=_report("a"))).status_code == 503

    await redis_client.delete(ingest_stream(0))
    async with asyncio.timeout(5):
        while True:
            response = await throttled_api.post(URL, headers=KEY, json=_report("a"))
            if response.status_code == 202:
                break
            await asyncio.sleep(0.02)

    assert response.json() == {"accepted": 1}


async def test_an_unreachable_queue_is_a_503_rather_than_a_dropped_report(
    api: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Redis going away is the one failure this endpoint cannot hide: the device has to
    # learn that its batch was not taken, so it can hold it and retry.
    async def unreachable(*_: Any, **__: Any) -> int:
        raise RedisError("connection reset by peer")

    monkeypatch.setattr(IngestService, "submit", unreachable)

    response = await api.post(URL, headers=KEY, json=_report("a"))

    assert response.status_code == 503
    assert response.json()["code"] == "ingest_unavailable"
    assert response.headers["Retry-After"] == "1"
