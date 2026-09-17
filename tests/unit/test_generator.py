"""Load generator: movement model, payload shape, scheduling and configuration.

No sockets are opened here. The transports are exercised against a real server in the
load runs documented in the README; what these tests pin down is everything that would
silently produce wrong numbers or a payload the API rejects.
"""

import ast
import asyncio
import contextlib
import itertools
import math
import random
import re
import resource
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Self, cast

import httpx
import orjson
import pytest

import generator
from geotrack.realtime.protocol import ack_frame, error_frame, throttle_frame
from geotrack.schemas.ingest import check_report_window, parse_ingest_payload

GENERATOR_PATH = Path(generator.__file__)

KYIV = generator.Area(lat=50.4501, lon=30.5234, radius_m=25_000.0)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Independent distance check, so the movement model cannot grade its own homework."""
    radius = 6_371_008.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def car_state(lat: float = 50.45, lon: float = 30.52) -> generator.DeviceState:
    return generator.DeviceState(
        device_id="dev-00000",
        lat=lat,
        lon=lon,
        heading_deg=45.0,
        speed_mps=16.0,
        profile=generator.PROFILES_BY_NAME["car"],
    )


# --- movement model ------------------------------------------------------------


def test_device_actually_moves() -> None:
    state = car_state()
    rng = random.Random(7)
    start = (state.lat, state.lon)

    for _ in range(20):
        generator.advance(state, dt=3.0, area=KYIV, rng=rng)

    assert (state.lat, state.lon) != start
    assert haversine_m(*start, state.lat, state.lon) > 100.0


def test_step_length_matches_the_profile_speed() -> None:
    state = car_state()
    rng = random.Random(11)
    profile = state.profile
    steps: list[float] = []

    for _ in range(200):
        before = (state.lat, state.lon)
        generator.advance(state, dt=1.0, area=KYIV, rng=rng)
        step = haversine_m(*before, state.lat, state.lon)
        steps.append(step)
        # The step is taken at the speed the tick settled on; the local flat projection
        # used to convert metres to degrees is worth well under a percent at this scale.
        assert step == pytest.approx(state.speed_mps * 1.0, rel=0.01, abs=0.05)

    mean_step = sum(steps) / len(steps)
    assert profile.min_speed_mps <= mean_step <= profile.max_speed_mps


def test_speed_stays_inside_the_profile_band() -> None:
    for name, profile in generator.PROFILES_BY_NAME.items():
        state = generator.DeviceState(
            device_id=f"dev-{name}",
            lat=KYIV.lat,
            lon=KYIV.lon,
            heading_deg=0.0,
            speed_mps=(profile.min_speed_mps + profile.max_speed_mps) / 2,
            profile=profile,
        )
        rng = random.Random(3)

        for _ in range(500):
            generator.advance(state, dt=3.0, area=KYIV, rng=rng)
            assert profile.min_speed_mps <= state.speed_mps <= profile.max_speed_mps


def test_fleet_stays_inside_the_configured_radius() -> None:
    area = generator.Area(lat=50.4501, lon=30.5234, radius_m=3_000.0)
    fleet = generator.build_fleet(count=40, prefix="dev-", area=area, seed=5)
    rngs = [generator.device_rng(5, state.device_id) for state in fleet]

    for _ in range(400):
        for state, rng in zip(fleet, rngs, strict=True):
            generator.advance(state, dt=3.0, area=area, rng=rng)
            distance = haversine_m(area.lat, area.lon, state.lat, state.lon)
            # Reflection happens after the step, so a device is never outside once the
            # tick returns; the margin only absorbs the projection difference.
            assert distance <= area.radius_m * 1.01 + 10.0


def test_reflection_turns_a_device_that_leaves_the_area_back_inside() -> None:
    area = generator.Area(lat=0.0, lon=0.0, radius_m=500.0)
    state = generator.DeviceState(
        device_id="dev-edge",
        lat=0.0,
        lon=0.00448,  # ~499 m east of the centre
        heading_deg=90.0,  # heading straight out
        speed_mps=18.0,
        profile=generator.PROFILES_BY_NAME["car"],
    )

    generator.advance(state, dt=3.0, area=area, rng=random.Random(1))

    assert haversine_m(area.lat, area.lon, state.lat, state.lon) <= area.radius_m * 1.01
    # Specular reflection off the boundary: an eastbound device now heads west.
    assert 180.0 < state.heading_deg < 360.0


def test_movement_is_reproducible_for_a_fixed_seed() -> None:
    def run(seed: int) -> list[tuple[float, float]]:
        fleet = generator.build_fleet(count=5, prefix="dev-", area=KYIV, seed=seed)
        rngs = [generator.device_rng(seed, state.device_id) for state in fleet]
        for _ in range(50):
            for state, rng in zip(fleet, rngs, strict=True):
                generator.advance(state, dt=3.0, area=KYIV, rng=rng)
        return [(state.lat, state.lon) for state in fleet]

    assert run(42) == run(42)
    assert run(42) != run(43)


def test_device_rng_is_independent_of_the_order_devices_are_served() -> None:
    # Devices run as concurrent tasks, so a shared RNG would make --seed meaningless.
    first = generator.device_rng(9, "dev-00007").random()
    second = generator.device_rng(9, "dev-00007").random()

    assert first == second
    assert first != generator.device_rng(9, "dev-00008").random()


def test_fleet_ids_are_unique_and_spread_over_the_area() -> None:
    fleet = generator.build_fleet(count=1_000, prefix="veh:", area=KYIV, seed=2)

    assert len({state.device_id for state in fleet}) == 1_000
    assert fleet[0].device_id == "veh:00000"
    assert {state.profile.name for state in fleet} == set(generator.PROFILES_BY_NAME)
    for state in fleet:
        assert haversine_m(KYIV.lat, KYIV.lon, state.lat, state.lon) <= KYIV.radius_m * 1.01


# --- projection ----------------------------------------------------------------


def test_longitude_step_does_not_explode_at_high_latitude() -> None:
    area = generator.Area(lat=69.65, lon=18.96, radius_m=20_000.0)  # Tromsø
    state = car_state(lat=area.lat, lon=area.lon)
    rng = random.Random(13)

    for _ in range(500):
        before_lon = state.lon
        generator.advance(state, dt=3.0, area=area, rng=rng)
        assert abs(state.lon - before_lon) < 0.01
        assert -90.0 <= state.lat <= 90.0
        assert -180.0 <= state.lon <= 180.0


def test_metres_per_degree_of_longitude_never_reaches_zero() -> None:
    assert generator.m_per_deg_lon(0.0) == pytest.approx(111_320.0, rel=1e-9)
    assert generator.m_per_deg_lon(60.0) == pytest.approx(55_660.0, rel=1e-3)
    assert generator.m_per_deg_lon(90.0) >= 1.0


# --- payloads ------------------------------------------------------------------


def test_payload_matches_what_the_ingest_endpoint_accepts() -> None:
    fleet = generator.build_fleet(count=3, prefix="dev-", area=KYIV, seed=1)
    moment = datetime.now(UTC)
    reports = [generator.report_payload(state, moment=moment) for state in fleet]

    seq, parsed = parse_ingest_payload(generator.ingest_envelope(reports, seq=17), max_items=1_000)

    assert seq == 17
    assert [report.device_id for report in parsed] == [state.device_id for state in fleet]
    assert parsed[0].latitude == pytest.approx(fleet[0].lat, abs=1e-6)
    check_report_window(
        parsed, now=moment, max_age=timedelta(days=7), max_future=timedelta(minutes=5)
    )


def test_payload_without_a_sequence_number_is_a_plain_batch() -> None:
    state = car_state()
    raw = generator.ingest_envelope([generator.report_payload(state, moment=datetime.now(UTC))])

    seq, parsed = parse_ingest_payload(raw, max_items=10)

    assert seq is None
    assert len(parsed) == 1


def test_payload_coordinates_are_trimmed_to_centimetre_precision() -> None:
    state = car_state(lat=50.123456789, lon=30.987654321)

    payload = generator.report_payload(state, moment=datetime.now(UTC))

    assert payload["latitude"] == 50.1234568
    assert payload["longitude"] == 30.9876543


# --- scheduling and statistics -------------------------------------------------


def test_interval_jitter_stays_within_twenty_percent() -> None:
    rng = random.Random(4)
    draws = [generator.jittered_interval(3.0, rng) for _ in range(2_000)]

    assert min(draws) >= 3.0 * 0.8
    assert max(draws) <= 3.0 * 1.2
    assert sum(draws) / len(draws) == pytest.approx(3.0, rel=0.02)


def test_schedule_returns_devices_in_due_order_and_nothing_early() -> None:
    schedule = generator.Schedule()
    schedule.push(0, 10.0)
    schedule.push(1, 5.0)
    schedule.push(2, 7.5)

    assert schedule.pop_due(4.0) == []
    assert schedule.next_delay(4.0) == pytest.approx(1.0)
    assert schedule.pop_due(7.5) == [1, 2]
    assert schedule.pop_due(10.0) == [0]
    assert schedule.next_delay(10.0) == pytest.approx(generator.IDLE_SLEEP_S)


def test_schedule_takes_a_device_back_after_it_has_been_served() -> None:
    schedule = generator.Schedule()
    schedule.push(0, 1.0)

    assert schedule.pop_due(1.0) == [0]
    schedule.push(0, 9.0)

    assert schedule.pop_due(8.999) == []
    assert schedule.pop_due(9.0) == [0]


def test_percentile_on_a_known_sample() -> None:
    values = [float(v) for v in range(1, 101)]

    assert generator.percentile(values, 0.0) == pytest.approx(1.0)
    assert generator.percentile(values, 0.5) == pytest.approx(50.5)
    assert generator.percentile(values, 0.95) == pytest.approx(95.05)
    assert generator.percentile(values, 0.99) == pytest.approx(99.01)
    assert generator.percentile(values, 1.0) == pytest.approx(100.0)
    assert generator.percentile([], 0.5) is None


def test_reservoir_is_bounded_but_keeps_counting() -> None:
    reservoir = generator.Reservoir(capacity=100, rng=random.Random(8))

    for value in range(10_000):
        reservoir.add(float(value))

    assert reservoir.count == 10_000
    assert len(reservoir.values) == 100
    # A uniform sample of 0..9999 should land near the middle, not at the first values.
    median = generator.percentile(reservoir.values, 0.5)
    assert median is not None
    assert 3_000 <= median <= 7_000


def test_reservoir_keeps_every_sample_while_it_fits() -> None:
    reservoir = generator.Reservoir(capacity=10, rng=random.Random(1))

    for value in (3.0, 1.0, 2.0):
        reservoir.add(value)

    assert sorted(reservoir.values) == [1.0, 2.0, 3.0]
    assert generator.percentile(reservoir.values, 0.5) == pytest.approx(2.0)


def test_stats_snapshot_reports_window_and_overall_rates() -> None:
    stats = generator.Stats(rng=random.Random(1))
    stats.started_at = 0.0
    stats.sent = 100
    stats.accepted = 90

    snapshot = stats.snapshot(now=10.0)

    assert snapshot["sent"] == 100
    assert snapshot["accepted"] == 90
    assert snapshot["sent_per_s"] == pytest.approx(10.0)
    assert snapshot["window_per_s"] == pytest.approx(10.0)

    stats.sent = 150
    later = stats.snapshot(now=20.0)

    assert later["sent_per_s"] == pytest.approx(7.5)
    assert later["window_per_s"] == pytest.approx(5.0)


# --- retry handling ------------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [("2", 2.0), ("0", 0.0), (None, None), ("", None), ("later", None), ("-5", 0.0)],
)
def test_retry_after_seconds(header: str | None, expected: float | None) -> None:
    assert generator.parse_retry_after(header, now=datetime(2026, 9, 17, tzinfo=UTC)) == expected


def test_retry_after_http_date() -> None:
    now = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)

    delay = generator.parse_retry_after("Thu, 17 Sep 2026 12:00:30 GMT", now=now)

    assert delay == pytest.approx(30.0)


def test_retry_after_in_the_past_is_zero() -> None:
    now = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)

    assert generator.parse_retry_after("Thu, 17 Sep 2026 11:59:00 GMT", now=now) == 0.0


# --- configuration -------------------------------------------------------------

BASE_ENV = {"INGEST_API_KEY": "key-from-env"}


def test_defaults() -> None:
    config = generator.parse_config([], env=BASE_ENV)

    assert config.url == "http://localhost:8080"
    assert config.transport == "ws"
    assert config.devices == 10_000
    assert config.interval == pytest.approx(3.0)
    assert config.ingest_key == "key-from-env"
    assert config.duration == 0.0
    assert config.device_prefix == "dev-"
    assert config.area.radius_m == pytest.approx(25_000.0)
    assert config.http_url == "http://localhost:8080/api/v1/ingest/locations"
    assert config.ws_url == "ws://localhost:8080/ws/ingest"


def test_environment_variables_are_the_fallback_and_flags_win() -> None:
    env = {
        "GEOTRACK_URL": "https://geo.example.com/",
        "INGEST_API_KEY": "key-from-env",
        "DEVICES": "250",
        "TRANSPORT": "http",
        "INTERVAL": "1.5",
        "RAMP_UP": "7",
        "SEED": "99",
    }

    config = generator.parse_config(["--devices", "42"], env=env)

    assert config.url == "https://geo.example.com"
    assert config.ws_url == "wss://geo.example.com/ws/ingest"
    assert config.devices == 42  # the flag beats DEVICES
    assert config.transport == "http"
    assert config.interval == pytest.approx(1.5)
    assert config.ramp_up == pytest.approx(7.0)
    assert config.seed == 99


def test_websocket_connections_default_to_one_socket_per_device() -> None:
    config = generator.parse_config(["--devices", "500"], env=BASE_ENV)

    assert config.connections == 500


def test_websocket_connections_are_capped_and_devices_are_multiplexed() -> None:
    config = generator.parse_config(["--devices", "1000", "--max-connections", "100"], env=BASE_ENV)

    assert config.connections == 100


def test_http_transport_defaults_to_a_bounded_pool() -> None:
    config = generator.parse_config(["--transport", "http", "--devices", "10000"], env=BASE_ENV)

    assert config.connections == generator.DEFAULT_HTTP_CONNECTIONS


def test_center_parsing() -> None:
    config = generator.parse_config(["--center", "-33.87,151.21"], env=BASE_ENV)

    assert config.area.lat == pytest.approx(-33.87)
    assert config.area.lon == pytest.approx(151.21)


@pytest.mark.parametrize("raw", ["", "50.45", "50.45,30.52,1", "north,east", "95,30", "50,200"])
def test_center_parsing_errors(raw: str) -> None:
    with pytest.raises(ValueError, match="center"):
        generator.parse_center(raw)


@pytest.mark.parametrize(
    "argv",
    [
        ["--center", "nowhere"],
        ["--devices", "0"],
        ["--interval", "0"],
        ["--transport", "carrier-pigeon"],
        ["--batch-size", "0"],
        ["--radius-km", "-1"],
    ],
)
def test_invalid_arguments_exit(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        generator.parse_config(argv, env=BASE_ENV)


def test_missing_ingest_key_is_rejected() -> None:
    with pytest.raises(SystemExit):
        generator.parse_config([], env={})


def test_batch_size_cannot_exceed_the_server_limit() -> None:
    with pytest.raises(SystemExit):
        generator.parse_config(["--batch-size", "5000"], env=BASE_ENV)


# --- process limits ------------------------------------------------------------


def test_file_limit_check_accepts_a_small_run() -> None:
    assert generator.ensure_file_limit(8) >= 8 + generator.FD_HEADROOM


def test_file_limit_check_fails_fast_when_it_cannot_be_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Pretend the process is stuck at 64 descriptors: the run must say so before it
    # opens the first socket, not fail halfway through the ramp-up.
    monkeypatch.setattr(resource, "getrlimit", lambda _: (64, 128))
    monkeypatch.setattr(resource, "setrlimit", lambda *_: None)

    with pytest.raises(RuntimeError, match="file descriptor"):
        generator.ensure_file_limit(10_000)


def test_file_limit_check_reports_a_refused_increase(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_: object) -> None:
        raise ValueError("not permitted")

    monkeypatch.setattr(resource, "getrlimit", lambda _: (64, 128))
    monkeypatch.setattr(resource, "setrlimit", refuse)

    with pytest.raises(RuntimeError, match="file descriptor"):
        generator.ensure_file_limit(10_000)


# --- device scheduling ---------------------------------------------------------


def make_devices(*, count: int, ramp_up: float = 0.0) -> tuple[generator.Devices, generator.Config]:
    config = generator.parse_config(
        ["--devices", str(count), "--ramp-up", str(ramp_up), "--interval", "3"], env=BASE_ENV
    )
    fleet = generator.build_fleet(
        count=count, prefix=config.device_prefix, area=config.area, seed=config.seed
    )
    return generator.Devices(fleet, config=config, rng=random.Random(1)), config


def test_every_device_gets_exactly_one_first_deadline() -> None:
    # A device listed twice in the schedule would silently report at twice the rate.
    devices, config = make_devices(count=8, ramp_up=4.0)

    devices.start(100.0, spread=config.ramp_up)

    assert len(devices.schedule) == 8
    assert devices.schedule.pop_due(99.9) == []
    served = devices.schedule.pop_due(100.0 + config.ramp_up + config.interval * 1.2)
    assert sorted(served) == list(range(8))
    assert len(devices.schedule) == 0


def test_serving_a_device_puts_it_back_exactly_once() -> None:
    devices, _ = make_devices(count=4)
    devices.start(0.0)
    moment = datetime.now(UTC)

    for round_number in range(1, 11):
        now = float(round_number) * 3.0
        for index in devices.schedule.pop_due(now + 10.0):
            devices.step(index, moment)
            devices.reschedule(index, now)
        assert len(devices.schedule) == 4


def test_served_devices_produce_reports_the_api_accepts() -> None:
    devices, _ = make_devices(count=3)
    devices.start(0.0)
    moment = datetime.now(UTC)

    reports = [devices.step(index, moment) for index in devices.schedule.pop_due(10.0)]

    _, parsed = parse_ingest_payload(generator.ingest_envelope(reports), max_items=10)
    assert {report.device_id for report in parsed} == {"dev-00000", "dev-00001", "dev-00002"}


def test_a_long_stall_does_not_teleport_a_device() -> None:
    # A socket that was down for an hour must not produce an hour-long jump when it comes
    # back. The step integrates the interval the device was scheduled for, so an outage
    # costs reports, never a kilometre-long leap that the server would see as a new place.
    config = generator.parse_config(
        ["--devices", "1", "--radius-km", "500", "--interval", "3"], env=BASE_ENV
    )
    state = generator.DeviceState(
        device_id="dev-00000",
        lat=config.area.lat,
        lon=config.area.lon,
        heading_deg=90.0,
        speed_mps=20.0,
        profile=generator.PROFILES_BY_NAME["car"],
    )
    devices = generator.Devices([state], config=config, rng=random.Random(1))
    devices.start(0.0)

    devices.reschedule(0, 0.0)
    devices.step(0, datetime.now(UTC))  # served an hour late

    travelled = haversine_m(config.area.lat, config.area.lon, state.lat, state.lon)
    assert travelled <= 20.0 * config.interval * (1.0 + generator.JITTER) * 1.05


# --- http batching -------------------------------------------------------------


def a_report(index: int) -> generator.Report:
    return {"device_id": f"dev-{index}", "latitude": 50.0, "longitude": 30.0, "timestamp": 1}


def test_batcher_sends_a_batch_as_soon_as_it_is_full() -> None:
    batcher = generator.Batcher(size=3, window=10.0)

    for index in range(2):
        batcher.add(a_report(index), 0.0)
    assert not batcher.ready(0.0)

    batcher.add(a_report(2), 0.0)

    assert batcher.ready(0.0)
    assert len(batcher.take()) == 3
    assert not batcher.ready(0.0)
    assert len(batcher) == 0


def test_batcher_sends_a_partial_batch_when_the_window_closes() -> None:
    batcher = generator.Batcher(size=100, window=0.2)

    batcher.add(a_report(0), 10.0)

    assert not batcher.ready(10.19)
    assert batcher.remaining(10.1) == pytest.approx(0.1)
    assert batcher.ready(10.2)


def test_batcher_window_runs_from_the_oldest_report() -> None:
    # Otherwise a steady trickle of reports would keep pushing the deadline away and
    # the first report would wait forever.
    batcher = generator.Batcher(size=100, window=0.2)

    batcher.add(a_report(0), 10.0)
    batcher.add(a_report(1), 10.15)

    assert batcher.ready(10.2)
    assert len(batcher.take()) == 2


def test_batcher_keeps_the_overflow_for_the_next_request() -> None:
    # A tick can serve far more devices than one request may carry; the reports over the
    # limit have to wait for the next request, not fall on the floor.
    batcher = generator.Batcher(size=3, window=10.0)
    for index in range(7):
        batcher.add(a_report(index), 0.0)

    taken: list[generator.Report] = []
    while batcher.ready(0.0):
        batch = batcher.take()
        assert len(batch) <= 3
        taken.extend(batch)

    assert [report["device_id"] for report in taken] == [f"dev-{index}" for index in range(6)]
    # The remainder waits for its own window rather than travelling in a short request,
    # and it keeps the deadline of the reports it arrived with.
    assert len(batcher) == 1
    assert batcher.ready(10.0)
    assert [report["device_id"] for report in batcher.take()] == ["dev-6"]


def test_an_empty_batcher_has_no_deadline() -> None:
    batcher = generator.Batcher(size=10, window=1.0)

    assert batcher.remaining(0.0) is None
    assert not batcher.ready(1_000.0)


# --- websocket worker ----------------------------------------------------------


class FakeSocket:
    """Stands in for a websocket: records what the device sent and answers it."""

    def __init__(self, reply: Callable[[dict[str, Any]], bytes] | None = None) -> None:
        self.sent: list[str] = []
        self._reply = reply
        self._inbox: asyncio.Queue[bytes] = asyncio.Queue()

    async def send(self, message: str) -> None:
        self.sent.append(message)
        if self._reply is not None:
            self._inbox.put_nowait(self._reply(orjson.loads(message)))

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        return await self._inbox.get()


class BrokenSocket:
    """A socket whose peer reset the connection: every send raises."""

    def __init__(self) -> None:
        self.sends = 0

    async def send(self, message: str) -> None:
        self.sends += 1
        raise ConnectionResetError("peer went away")

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        await asyncio.sleep(3_600)
        raise StopAsyncIteration


class FakeConnect[S]:
    def __init__(self, socket: S) -> None:
        self._socket = socket

    async def __aenter__(self) -> S:
        return self._socket

    async def __aexit__(self, *_: object) -> bool:
        return False


def make_worker(
    monkeypatch: pytest.MonkeyPatch,
    socket: FakeSocket | BrokenSocket,
    *,
    devices: int,
    interval: str,
    extra: Sequence[str] = (),
) -> tuple[generator.WebsocketWorker, generator.Stats]:
    monkeypatch.setattr(generator, "ws_connect", lambda *_, **__: FakeConnect(socket))
    config = generator.parse_config(
        [
            "--devices",
            str(devices),
            "--connections",
            "1",
            "--interval",
            interval,
            "--ramp-up",
            "0",
            *extra,
        ],
        env=BASE_ENV,
    )
    stats = generator.Stats(rng=random.Random(1), started_at=time.monotonic())
    worker = generator.WebsocketWorker(
        index=0,
        states=generator.build_fleet(
            count=devices, prefix=config.device_prefix, area=config.area, seed=config.seed
        ),
        config=config,
        stats=stats,
        console=generator.Console(verbose=False),
    )
    return worker, stats


def start_worker(
    monkeypatch: pytest.MonkeyPatch, socket: FakeSocket, *, devices: int, interval: str
) -> tuple[asyncio.Task[None], generator.Stats]:
    worker, stats = make_worker(monkeypatch, socket, devices=devices, interval=interval)
    return asyncio.create_task(worker.run()), stats


async def wait_until(condition: Callable[[], bool], *, limit: float = 3.0) -> None:
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("the condition was never met")


async def stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_websocket_worker_sends_payloads_the_api_accepts_and_counts_acks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = FakeSocket(
        lambda payload: ack_frame(
            seq=cast(int, payload["seq"]), accepted=len(cast(list[object], payload["items"]))
        )
    )
    task, stats = start_worker(monkeypatch, socket, devices=3, interval="0.05")

    try:
        await wait_until(lambda: stats.accepted >= 3)
    finally:
        await stop(task)

    assert stats.sent >= 3
    assert stats.produced == stats.sent  # nothing was made and then lost
    assert stats.dropped == 0
    assert stats.latency.count >= 3
    # The gauge has to come back down when the socket goes away, or a long run with
    # reconnects would report more connections than it has.
    assert stats.in_flight == 0
    for frame in socket.sent:
        seq, items = parse_ingest_payload(frame, max_items=1_000)
        assert seq is not None
        assert items


async def test_websocket_worker_holds_back_reports_while_throttled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = FakeSocket(lambda _: throttle_frame(retry_after_ms=1_000))
    task, stats = start_worker(monkeypatch, socket, devices=2, interval="0.02")

    try:
        await wait_until(lambda: stats.throttles >= 1)
        await asyncio.sleep(0.05)  # let the iteration that was already in flight finish
        settled = stats.sent
        await asyncio.sleep(0.3)  # still well inside the second the server asked for

        assert stats.sent == settled
        assert stats.accepted == 0
    finally:
        await stop(task)


async def test_websocket_worker_survives_a_frame_it_cannot_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = FakeSocket(lambda _: b"this is not json")
    task, stats = start_worker(monkeypatch, socket, devices=1, interval="0.02")

    try:
        await wait_until(lambda: stats.failures >= 2)

        assert stats.sent >= 2  # the device keeps reporting despite the noise
        # An unreadable frame is a transport fault, not the server's verdict on a report.
        assert stats.rejected == 0
    finally:
        await stop(task)


# --- http runner ---------------------------------------------------------------


def make_http_runner(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], Any],
    *,
    devices: int = 4,
    extra: Sequence[str] = (),
) -> tuple[generator.HttpRunner, generator.Stats]:
    client_class = httpx.AsyncClient

    def with_mock_transport(**kwargs: Any) -> httpx.AsyncClient:
        return client_class(**kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(httpx, "AsyncClient", with_mock_transport)
    config = generator.parse_config(
        [
            "--transport",
            "http",
            "--devices",
            str(devices),
            "--connections",
            "1",
            "--interval",
            "0.05",
            "--batch-size",
            "2",
            "--batch-window-ms",
            "10",
            "--ramp-up",
            "0",
            *extra,
        ],
        env=BASE_ENV,
    )
    stats = generator.Stats(rng=random.Random(1), started_at=time.monotonic())
    runner = generator.HttpRunner(
        states=generator.build_fleet(
            count=devices, prefix=config.device_prefix, area=config.area, seed=config.seed
        ),
        config=config,
        stats=stats,
        console=generator.Console(verbose=False),
    )
    return runner, stats


def start_http_runner(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], Any],
    *,
    devices: int = 4,
) -> tuple[asyncio.Task[None], generator.Stats]:
    runner, stats = make_http_runner(monkeypatch, handler, devices=devices)
    return asyncio.create_task(runner.run()), stats


async def test_http_runner_posts_batches_the_api_accepts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        assert request.headers["x-ingest-key"] == BASE_ENV["INGEST_API_KEY"]
        _, items = parse_ingest_payload(request.content, max_items=1_000)
        return httpx.Response(202, json={"accepted": len(items)})

    task, stats = start_http_runner(monkeypatch, handler)

    try:
        await wait_until(lambda: stats.accepted >= 4)
    finally:
        await stop(task)

    assert stats.sent == stats.accepted
    assert stats.rejected == 0
    assert stats.failures == 0
    assert bodies
    for body in bodies:
        seq, items = parse_ingest_payload(body, max_items=1_000)
        assert seq is None  # the HTTP transport has no acknowledgements to correlate
        assert 1 <= len(items) <= 2  # --batch-size


async def test_http_runner_honours_retry_after_and_then_gives_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503, json={"detail": "busy"}, headers={"Retry-After": "0"})

    task, stats = start_http_runner(monkeypatch, handler)

    try:
        await wait_until(lambda: stats.dropped > 0)
    finally:
        await stop(task)

    # Every batch is tried the agreed number of times and then shed, so a service that
    # keeps saying "later" costs a bounded amount of memory and work.
    assert stats.throttles >= generator.HTTP_ATTEMPTS
    assert attempts >= generator.HTTP_ATTEMPTS
    assert stats.accepted == 0
    # A server that accepted nothing must not show up as throughput. Attempts outnumber
    # the reports that were ever offered; only the offered ones may reach a counter.
    assert stats.sent == 0
    assert stats.dropped <= stats.produced
    assert stats.latency.count == 0  # percentiles of refusals are not ingest latency


async def test_http_runner_does_not_retry_a_rejected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(422, json={"detail": "bad report"})

    task, stats = start_http_runner(monkeypatch, handler, devices=2)

    try:
        await wait_until(lambda: stats.rejected > 0)
        sent_batches = attempts
        await asyncio.sleep(0.05)

        assert attempts > sent_batches  # new batches keep flowing
        assert stats.dropped == 0  # nothing was retried into the ground
        # A refused report is counted in reports, the same unit as every other report
        # counter, and never in the same number as a connection that failed.
        assert stats.rejected == stats.sent
        assert stats.failures == 0
    finally:
        await stop(task)


# --- accounting: a retried batch is one batch ----------------------------------


async def test_a_retried_batch_is_counted_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # Counting per attempt inflates the headline throughput by up to HTTP_ATTEMPTS-fold
    # in exactly the regime a load run exists to measure: the server shedding load.
    answers = [503, 503, 202]
    delivered: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = answers.pop(0) if answers else 202
        items = cast(list[object], orjson.loads(request.content)["items"])
        if status != 202:
            return httpx.Response(503, json={"detail": "busy"}, headers={"Retry-After": "0"})
        delivered.append(len(items))
        return httpx.Response(202, json={"accepted": len(items)})

    runner, stats = make_http_runner(monkeypatch, handler)
    task = asyncio.create_task(runner.run())
    try:
        await wait_until(lambda: stats.accepted > 0)
    finally:
        await stop(task)

    assert stats.sent == stats.accepted
    assert stats.sent == sum(delivered)  # not three times the first batch
    assert stats.latency.count == len(delivered)  # and the refusals timed nothing


async def test_latency_percentiles_ignore_responses_that_refused_the_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A refusal is fast for reasons that say nothing about how long ingestion takes.
    accepted = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal accepted
        items = cast(list[object], orjson.loads(request.content)["items"])
        if accepted >= 2:
            return httpx.Response(429, json={"detail": "slow down"}, headers={"Retry-After": "0"})
        accepted += 1
        await asyncio.sleep(0.02)
        return httpx.Response(202, json={"accepted": len(items)})

    runner, stats = make_http_runner(monkeypatch, handler)
    task = asyncio.create_task(runner.run())
    try:
        await wait_until(lambda: stats.throttles >= generator.HTTP_ATTEMPTS)
    finally:
        await stop(task)

    assert stats.latency.count == 2
    p50 = generator.percentile(stats.latency.values, 0.5)
    assert p50 is not None
    assert p50 >= 15.0  # the accepted responses, not the instant refusals


async def test_report_counters_partition_everything_the_fleet_produced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    answers = itertools.cycle([202, 422, 503, 503, 503])

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(answers)
        items = cast(list[object], orjson.loads(request.content)["items"])
        if status == 202:
            return httpx.Response(202, json={"accepted": len(items)})
        if status == 422:
            return httpx.Response(422, json={"detail": "bad report"})
        return httpx.Response(503, json={"detail": "busy"}, headers={"Retry-After": "0"})

    runner, stats = make_http_runner(monkeypatch, handler)
    task = asyncio.create_task(runner.run())
    try:
        await wait_until(lambda: stats.accepted > 0 and stats.rejected > 0 and stats.dropped > 0)
    finally:
        await stop(task)

    # Every report is produced once and then settles exactly once: sent — and within that
    # accepted or rejected — or dropped. Anything else and the summary cannot be read.
    assert stats.sent + stats.dropped <= stats.produced
    assert stats.accepted + stats.rejected <= stats.sent


# --- ownership: one fleet, one driver -------------------------------------------


def test_re_arming_a_fleet_keeps_exactly_one_deadline_per_device() -> None:
    # supervise() re-enters a worker after any escaping failure. A fleet that was seeded
    # again on every entry reports at twice, then three times, the configured rate.
    devices, _ = make_devices(count=8)

    devices.start(0.0)
    devices.start(10.0)
    devices.start(20.0)

    assert len(devices.schedule) == 8
    assert sorted(devices.schedule.pop_due(1e9)) == list(range(8))


def test_re_arming_brings_back_a_device_that_was_in_flight() -> None:
    # A device is out of the schedule between being served and being put back; if the
    # task dies in that window the device must not stop reporting for the whole run.
    devices, _ = make_devices(count=4)
    devices.start(0.0)
    assert sorted(devices.schedule.pop_due(1e9)) == [0, 1, 2, 3]

    devices.start(100.0)

    assert sorted(devices.schedule.pop_due(1e9)) == [0, 1, 2, 3]


async def test_a_fleet_refuses_a_second_driver_and_can_be_handed_on() -> None:
    devices, _ = make_devices(count=6)
    claimed = asyncio.Event()

    async def claim() -> None:
        with devices.driving(time.monotonic()):
            claimed.set()
            await asyncio.sleep(0.05)

    racers = [asyncio.create_task(claim()) for _ in range(8)]
    await claimed.wait()
    outcomes = await asyncio.gather(*racers, return_exceptions=True)

    assert sum(outcome is None for outcome in outcomes) == 1
    assert all(
        isinstance(outcome, RuntimeError) and "already being driven" in str(outcome)
        for outcome in outcomes
        if outcome is not None
    )
    # One driver's worth of devices, not eight, and the fleet is free again afterwards.
    assert len(devices.schedule) == 6
    with devices.driving(time.monotonic()):
        assert len(devices.schedule) == 6


async def test_a_restarted_websocket_worker_does_not_double_the_fleet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = BrokenSocket()
    worker, _ = make_worker(monkeypatch, socket, devices=4, interval="0.05")

    for _ in range(3):
        task = asyncio.create_task(worker.run())
        try:
            await wait_until(lambda: socket.sends > 0)
        finally:
            await stop(task)
        assert len(worker.devices.schedule) == 4

    assert sorted(worker.devices.schedule.pop_due(1e9)) == [0, 1, 2, 3]


async def test_a_restarted_http_runner_does_not_double_the_fleet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        items = cast(list[object], orjson.loads(request.content)["items"])
        return httpx.Response(202, json={"accepted": len(items)})

    runner, stats = make_http_runner(monkeypatch, handler)

    def more_than(before: int) -> Callable[[], bool]:
        return lambda: stats.produced > before

    for _ in range(3):
        task = asyncio.create_task(runner.run())
        try:
            await wait_until(more_than(stats.produced))
        finally:
            await stop(task)
        assert len(runner.devices.schedule) == 4

    assert sorted(runner.devices.schedule.pop_due(1e9)) == [0, 1, 2, 3]


def test_a_reader_that_walked_away_does_not_end_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `generator.py ... | head`, or a compose log consumer going away: the error handler
    # reporting a failed connection must not raise out of the worker and be "restarted".
    class ClosedPipe:
        def write(self, _: str) -> int:
            raise BrokenPipeError(32, "Broken pipe")

        def flush(self) -> None:
            raise BrokenPipeError(32, "Broken pipe")

    monkeypatch.setattr(sys, "stderr", ClosedPipe())
    console = generator.Console(verbose=True)

    console.error("the service is down")  # must not raise


# --- websocket losses -----------------------------------------------------------


async def test_reports_that_never_reached_the_socket_are_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without this the counters only balance because the losses were never recorded.
    socket = BrokenSocket()
    worker, stats = make_worker(monkeypatch, socket, devices=5, interval="0.02")
    task = asyncio.create_task(worker.run())
    try:
        await wait_until(lambda: socket.sends >= 1)
        await asyncio.sleep(0.05)
    finally:
        await stop(task)

    assert stats.produced > 0
    assert stats.sent == 0
    assert stats.dropped == stats.produced
    assert stats.failures >= 1  # and the session failure is counted in its own unit


async def test_a_frame_the_server_refuses_counts_its_reports_as_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = FakeSocket(
        lambda payload: error_frame(
            "out_of_window", "timestamp is too old", seq=cast(int, payload["seq"])
        )
    )
    worker, stats = make_worker(monkeypatch, socket, devices=3, interval="0.05")
    task = asyncio.create_task(worker.run())
    try:
        await wait_until(lambda: stats.rejected >= 3)
    finally:
        await stop(task)

    assert stats.rejected == stats.sent
    assert stats.accepted == 0
    assert stats.failures == 0  # a refusal is the server's verdict, not a broken socket


async def test_a_websocket_frame_never_exceeds_the_servers_batch_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The server rejects an oversized payload as a whole, so one report over the limit
    # costs every report in the frame.
    socket = FakeSocket(
        lambda payload: ack_frame(
            seq=cast(int, payload["seq"]), accepted=len(cast(list[object], payload["items"]))
        )
    )
    worker, stats = make_worker(
        monkeypatch, socket, devices=40, interval="0.001", extra=["--max-batch", "5"]
    )

    task = asyncio.create_task(worker.run())
    try:
        await wait_until(lambda: len(socket.sent) > 8)
    finally:
        await stop(task)

    sizes = [len(parse_ingest_payload(frame, max_items=1_000)[1]) for frame in socket.sent]
    assert max(sizes) <= 5
    assert max(sizes) > 1  # a tick really did serve more devices than one frame holds
    assert stats.sent >= sum(sizes)


# --- backpressure is bounded -----------------------------------------------------


@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [(None, 0.5), (2.0, 2.0), (0.0, 0.0), (3_600.0, generator.RETRY_AFTER_MAX_S)],
)
def test_a_retry_pause_is_bounded(retry_after: float | None, expected: float) -> None:
    assert generator.retry_pause(retry_after, fallback=0.5) == pytest.approx(expected)


async def test_a_huge_retry_after_does_not_park_a_sender(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An intermediary error page answering `Retry-After: 3600` would otherwise take that
    # sender offline for an hour while the run still prints stats lines.
    monkeypatch.setattr(generator, "RETRY_AFTER_MAX_S", 0.05)
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"detail": "busy"}, headers={"Retry-After": "3600"})

    task, _ = start_http_runner(monkeypatch, handler)
    try:
        await wait_until(lambda: calls >= 3, limit=2.0)
    finally:
        await stop(task)

    assert calls >= 3


async def test_a_huge_websocket_throttle_does_not_park_a_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(generator, "RETRY_AFTER_MAX_S", 0.05)
    socket = FakeSocket(lambda _: throttle_frame(retry_after_ms=3_600_000))
    worker, stats = make_worker(monkeypatch, socket, devices=2, interval="0.02")
    task = asyncio.create_task(worker.run())
    try:
        await wait_until(lambda: stats.throttles >= 1)
        held = stats.sent
        await wait_until(lambda: stats.sent > held, limit=2.0)
    finally:
        await stop(task)

    assert stats.throttles >= 1


# --- the live gauge says what it counts ------------------------------------------


def test_the_live_gauge_is_named_after_what_each_transport_can_measure() -> None:
    # "connections: 0" next to a busy 32-connection pool is worse than no column at all:
    # a websocket run has sockets to count, an HTTP run has requests in flight and a
    # sender queue whose depth is the reason batches get dropped.
    ws = generator.parse_config(["--devices", "500"], env=BASE_ENV)
    http = generator.parse_config(
        ["--transport", "http", "--devices", "500", "--connections", "8"], env=BASE_ENV
    )
    stats = generator.Stats(rng=random.Random(1))
    stats.in_flight, stats.queued = 7, 19

    snapshot = stats.snapshot(now=1.0)

    assert generator.live_gauge(snapshot, ws) == "conn 7/500"
    assert generator.live_gauge(snapshot, http) == "busy 7/8 q 19/32"
    assert "conn 7/500" in generator.format_stats(snapshot, ws)
    assert "busy 7/8 q 19/32" in generator.format_stats(snapshot, http)


async def test_the_queue_depth_follows_the_backlog_the_senders_cannot_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        items = cast(list[object], orjson.loads(request.content)["items"])
        await asyncio.sleep(0.2)  # one sender, far slower than the fleet
        return httpx.Response(202, json={"accepted": len(items)})

    runner, stats = make_http_runner(monkeypatch, handler, devices=40)
    task = asyncio.create_task(runner.run())
    try:
        await wait_until(lambda: stats.dropped > 0)
        assert stats.queued > 0  # the shed batches had somewhere visible to pile up
    finally:
        await stop(task)


async def test_requests_in_flight_are_visible_while_the_server_is_slow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        items = cast(list[object], orjson.loads(request.content)["items"])
        await asyncio.sleep(0.05)
        return httpx.Response(202, json={"accepted": len(items)})

    runner, stats = make_http_runner(monkeypatch, handler)
    task = asyncio.create_task(runner.run())
    try:
        await wait_until(lambda: stats.in_flight > 0)
        assert stats.in_flight >= 1
        await wait_until(lambda: stats.accepted > 0)
    finally:
        await stop(task)

    assert stats.in_flight == 0  # the gauge comes back down


def test_the_summary_names_the_unit_of_every_counter() -> None:
    config = generator.parse_config(["--transport", "http", "--devices", "10"], env=BASE_ENV)
    stats = generator.Stats(rng=random.Random(1))
    stats.produced, stats.sent, stats.accepted = 100, 90, 80
    stats.rejected, stats.dropped, stats.failures = 10, 10, 2

    summary = generator.format_summary(stats.snapshot(now=10.0), config)

    assert "produced     100 reports" in summary
    assert "rejected     10 reports" in summary
    assert "dropped      10 reports" in summary
    assert "failures     2 transport failures" in summary


# --- a seeded run replays ---------------------------------------------------------


def drive(
    seed: int, gaps: Sequence[float], *, restart_after: int | None = None
) -> dict[str, list[Any]]:
    """What each device reported, in order, from a fleet driven on a clock we choose.

    Keyed by device rather than flattened: how many reports a run gets through and the
    order they leave in belong to the timing, and no seed can promise those. What a seed
    does promise is the n-th report of each device.
    """
    config = generator.parse_config(
        ["--devices", "3", "--interval", "1", "--seed", str(seed)], env=BASE_ENV
    )
    fleet = generator.build_fleet(count=3, prefix="dev-", area=config.area, seed=seed)
    devices = generator.Devices(fleet, config=config, rng=random.Random(f"{seed}:ws:0"))
    devices.start(0.0)
    moment = datetime(2026, 9, 17, tzinfo=UTC)
    trace: dict[str, list[Any]] = {state.device_id: [] for state in fleet}
    now = 0.0
    for tick, gap in enumerate(gaps):
        now += gap
        if tick == restart_after:
            devices.start(now)  # the supervisor restarted the worker here
        for index in devices.schedule.pop_due(now + 100.0):
            report = devices.step(index, moment)
            devices.reschedule(index, now)
            trace[cast(str, report["device_id"])].append((report["latitude"], report["longitude"]))
    return trace


def assert_same_prefix(left: dict[str, list[Any]], right: dict[str, list[Any]]) -> None:
    assert left.keys() == right.keys()
    for device_id, reports in left.items():
        other = right[device_id]
        shared = min(len(reports), len(other))
        assert shared > 0
        assert reports[:shared] == other[:shared], device_id


def test_a_seeded_run_replays_identically_however_the_clock_behaved() -> None:
    steady = drive(7, [1.0] * 12)
    stalled = drive(7, [0.3, 4.0, 0.2, 9.0, 1.0, 0.1, 2.0, 30.0, 0.5, 1.0, 1.0, 1.0])

    assert_same_prefix(steady, stalled)
    assert steady != drive(8, [1.0] * 12)


def test_a_seeded_run_replays_identically_across_a_restart() -> None:
    # A reconnect or a supervised restart re-arms the schedule, so a device may lose or
    # gain a slot; the reports it does make are the same ones it would have made anyway.
    assert_same_prefix(drive(7, [1.0] * 12), drive(7, [1.0] * 12, restart_after=5))


def test_a_seeded_device_reports_the_same_however_the_fleet_is_sliced() -> None:
    # One socket per device or four devices per socket: dev-00005's n-th report is its own.
    def trace(connections: int) -> list[Any]:
        config = generator.parse_config(
            ["--devices", "8", "--connections", str(connections), "--interval", "1", "--seed", "3"],
            env=BASE_ENV,
        )
        fleet = generator.build_fleet(count=8, prefix="dev-", area=config.area, seed=3)
        moment = datetime(2026, 9, 17, tzinfo=UTC)
        seen: list[Any] = []
        for index in range(connections):
            devices = generator.Devices(
                fleet[index::connections], config=config, rng=random.Random(f"3:ws:{index}")
            )
            devices.start(0.0)
            for _ in range(6):
                for slot in range(len(devices)):
                    report = devices.step(slot, moment)
                    devices.reschedule(slot, 0.0)
                    if report["device_id"] == "dev-00005":
                        seen.append((report["latitude"], report["longitude"]))
        return seen

    assert len(trace(1)) == 6
    assert trace(1) == trace(4)


# --- the script itself -----------------------------------------------------------


def test_the_script_runs_on_every_python_its_header_claims() -> None:
    # generator.py is the one file handed to someone to run standalone. Syntax newer than
    # their interpreter fails at import, before argparse prints so much as a usage line,
    # and the inline script header is the promise this checks against.
    source = GENERATOR_PATH.read_text(encoding="utf-8")
    floor = re.search(r'requires-python = ">=3\.(\d+)"', source)
    assert floor is not None

    ast.parse(source, feature_version=(3, int(floor.group(1))))
    # Annotations are evaluated eagerly before 3.14, so a forward reference in a signature
    # would be a NameError at import on exactly the interpreters the header promises.
    assert "from __future__ import annotations" in source


# --- configuration: the compose service needs no command line --------------------


def test_the_compose_environment_names_configure_the_run() -> None:
    config = generator.parse_config(
        [],
        env={
            "INGEST_API_KEY": "key",
            "GENERATOR_BASE_URL": "http://nginx:8080",
            "GENERATOR_DEVICES": "250",
            "GENERATOR_INTERVAL_S": "5",
            "GENERATOR_BATCH_SIZE": "250",
            "GENERATOR_CONCURRENCY": "32",
            "GENERATOR_DURATION_S": "60",
            "GENERATOR_TRANSPORT": "http",
        },
    )

    assert config.url == "http://nginx:8080"
    assert config.http_url == "http://nginx:8080/api/v1/ingest/locations"
    assert config.devices == 250
    assert config.transport == "http"
    assert config.interval == pytest.approx(5.0)
    assert config.batch_size == 250
    assert config.connections == 32
    assert config.duration == pytest.approx(60.0)


def test_the_flag_name_still_wins_over_the_compose_name() -> None:
    env = {"INGEST_API_KEY": "key", "DEVICES": "10", "GENERATOR_DEVICES": "999"}

    assert generator.parse_config([], env=env).devices == 10


def test_the_server_batch_ceiling_is_configurable() -> None:
    # INGEST_MAX_BATCH is an environment setting on the server; nothing else tells the
    # generator how large a frame the deployment it is pointed at will accept.
    assert generator.parse_config(["--max-batch", "200"], env=BASE_ENV).max_batch == 200
    assert generator.parse_config([], env={**BASE_ENV, "INGEST_MAX_BATCH": "200"}).max_batch == 200
    assert generator.parse_config([], env=BASE_ENV).max_batch == generator.DEFAULT_MAX_BATCH


def test_a_batch_larger_than_the_ceiling_is_rejected() -> None:
    with pytest.raises(SystemExit):
        generator.parse_config(["--batch-size", "500", "--max-batch", "200"], env=BASE_ENV)
    with pytest.raises(SystemExit):
        generator.parse_config(["--max-batch", "200"], env={**BASE_ENV, "BATCH_SIZE": "500"})


def test_a_lower_server_ceiling_trims_the_default_request_size() -> None:
    # Nobody chose the default batch size, so pointing the run at a deployment with a
    # lower INGEST_MAX_BATCH must produce requests that fit, not a refusal to start.
    config = generator.parse_config(["--transport", "http", "--max-batch", "50"], env=BASE_ENV)

    assert config.batch_size == 50
    assert generator.DEFAULT_BATCH_SIZE > 50
