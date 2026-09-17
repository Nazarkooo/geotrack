#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx>=0.28.1",
#     "orjson>=3.12.0",
#     "uvloop>=0.22.1; sys_platform != 'win32'",
#     "websockets>=17.1",
# ]
# ///
"""Load generator: a fleet of devices drifting around a point and reporting positions.

Three ways to run it, all equivalent:

    uv run generator.py --devices 10000 --ingest-key "$INGEST_API_KEY"
    python generator.py --devices 10000            # inside the application image
    docker compose --profile loadtest run --rm generator

Every option also reads an environment variable (``GEOTRACK_URL``, ``INGEST_API_KEY``,
``DEVICES``, ...), so the compose service needs no command line at all.

Two transports. ``--transport ws`` opens one websocket per device, which is what "10,000
concurrent devices" means for the server: 10,000 sockets, each with its own schedule,
acknowledgements and reconnects. ``--transport http`` micro-batches reports from the whole
fleet over a small connection pool, which is how a device gateway would forward them.

Movement replays exactly under ``--seed``. Every device owns two private random streams
keyed by its id — one for the trajectory, one for the interval between its reports — and
the step integrates the interval the device was scheduled for rather than the wall clock.
The n-th report of a device therefore carries the same coordinates in every run with that
seed, whatever the loop, the network, a reconnect or ``--connections`` did in between.
What is not reproducible, and cannot be: the timestamps, and how many reports a run gets
through before it stops.

Reports are accounted for as a partition, so the summary balances rather than merely
looking plausible: every report the fleet produces ends up either sent (and then accepted
or rejected by the server) or dropped (queue shed, retries exhausted, socket refused it).
A retried batch is counted once, not once per attempt.
"""

# Annotations stay lazy so the module imports on any interpreter that can parse it:
# this file is handed over and run standalone, and deferred evaluation is only the
# default from 3.14 on.
from __future__ import annotations

import argparse
import asyncio
import contextlib
import heapq
import math
import os
import random
import resource
import signal
import sys
import time
import warnings
from collections.abc import Callable, Coroutine, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Literal, TextIO

import httpx
import orjson
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

# --- constants -----------------------------------------------------------------

M_PER_DEG_LAT = 111_320.0

DEFAULT_URL = "http://localhost:8080"
DEFAULT_DEVICES = 10_000
DEFAULT_INTERVAL_S = 3.0
DEFAULT_CENTER = "50.4501,30.5234"  # Kyiv
DEFAULT_RADIUS_KM = 25.0
DEFAULT_RAMP_UP_S = 30.0
DEFAULT_STATS_INTERVAL_S = 5.0
DEFAULT_MAX_CONNECTIONS = 10_000
DEFAULT_HTTP_CONNECTIONS = 32
DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_WINDOW_MS = 100
DEFAULT_SEED = 1
DEFAULT_PREFIX = "dev-"

# Mirrors INGEST_MAX_BATCH on the server, which rejects an oversized payload as a whole:
# one report over the limit costs the whole frame, so the generator never builds one.
DEFAULT_MAX_BATCH = 1_000
JITTER = 0.2  # ±20% around the reporting interval

IDLE_SLEEP_S = 0.25
FD_HEADROOM = 256
LATENCY_SAMPLES = 4_096
PENDING_ACKS_MAX = 1_024
RECONNECT_BASE_S = 0.5
RECONNECT_MAX_S = 30.0
HTTP_ATTEMPTS = 3
HTTP_TIMEOUT_S = 15.0
# A server, or an error page from something in front of it, may name any Retry-After it
# likes. Honouring an hour would park a sender for an hour while the run still looks
# alive, so the pause is bounded and the backlog is shed instead.
RETRY_AFTER_MAX_S = 30.0
WS_OPEN_TIMEOUT_S = 20.0
WS_PING_INTERVAL_S = 20.0
WS_MAX_FRAME_BYTES = 1 << 20
QUEUE_DEPTH_FACTOR = 4
ERROR_INTERVAL_S = 10.0
VERBOSE_ERROR_INTERVAL_S = 0.5

type Report = dict[str, object]
type Transport = Literal["ws", "http"]


# --- geometry ------------------------------------------------------------------


def m_per_deg_lon(lat: float) -> float:
    """Metres per degree of longitude at ``lat``.

    Clamped away from zero: without it a device close to a pole would turn a metre of
    eastward drift into a jump across half the planet.
    """
    return max(M_PER_DEG_LAT * math.cos(math.radians(lat)), 1.0)


def _wrap_deg(delta: float) -> float:
    return (delta + 180.0) % 360.0 - 180.0


@dataclass(frozen=True, slots=True)
class Area:
    """The demo area: a circle devices are kept inside.

    Positions are converted to metres in a flat frame centred on the area. Over tens of
    kilometres the error against a spheroid is a fraction of a percent, and using one
    projection for both the step and the containment check keeps them consistent — a
    device can never be "inside" for the step and "outside" for the boundary test.
    """

    lat: float
    lon: float
    radius_m: float

    def offsets(self, lat: float, lon: float) -> tuple[float, float]:
        """Metres east and north of the centre."""
        north = (lat - self.lat) * M_PER_DEG_LAT
        east = _wrap_deg(lon - self.lon) * m_per_deg_lon((lat + self.lat) / 2.0)
        return east, north

    def position(self, east: float, north: float) -> tuple[float, float]:
        """Inverse of :meth:`offsets`."""
        lat = self.lat + north / M_PER_DEG_LAT
        lat = min(max(lat, -89.9), 89.9)
        lon = _wrap_deg(self.lon + east / m_per_deg_lon((lat + self.lat) / 2.0))
        return lat, lon


# --- movement model ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MotionProfile:
    """How one class of device moves. Sigmas are per square root of a second, so the
    behaviour does not change when the reporting interval does."""

    name: str
    share: float
    min_speed_mps: float
    max_speed_mps: float
    heading_sigma_deg: float
    speed_sigma_mps: float


PROFILES = (
    MotionProfile("stationary", 0.20, 0.0, 0.4, 30.0, 0.08),
    MotionProfile("pedestrian", 0.35, 0.6, 2.0, 12.0, 0.20),
    MotionProfile("cyclist", 0.20, 3.0, 6.5, 8.0, 0.50),
    MotionProfile("car", 0.25, 12.0, 20.0, 5.0, 1.20),
)
PROFILES_BY_NAME = {profile.name: profile for profile in PROFILES}


@dataclass(slots=True)
class DeviceState:
    device_id: str
    lat: float
    lon: float
    heading_deg: float
    speed_mps: float
    profile: MotionProfile


def device_rng(seed: int, device_id: str) -> random.Random:
    """A private random stream per device, so concurrency cannot reorder the fleet."""
    return random.Random(f"{seed}:{device_id}")


def report_rng(seed: int, device_id: str) -> random.Random:
    """The stream that jitters a device's reporting interval.

    Kept apart from the movement stream on purpose: the interval is drawn exactly once
    per report, so the n-th step of a device is the same length in every seeded run even
    if this one reconnected, was throttled or had its worker restarted in between.
    """
    return random.Random(f"{seed}:interval:{device_id}")


def profile_for(rng: random.Random) -> MotionProfile:
    draw = rng.random()
    cumulative = 0.0
    for profile in PROFILES:
        cumulative += profile.share
        if draw < cumulative:
            return profile
    return PROFILES[-1]


def build_fleet(*, count: int, prefix: str, area: Area, seed: int) -> list[DeviceState]:
    fleet: list[DeviceState] = []
    for index in range(count):
        device_id = f"{prefix}{index:05d}"
        rng = device_rng(seed, device_id)
        profile = profile_for(rng)
        # sqrt keeps the sample uniform over the disc instead of crowding the centre.
        distance = area.radius_m * math.sqrt(rng.random())
        bearing = rng.uniform(0.0, 2.0 * math.pi)
        lat, lon = area.position(distance * math.sin(bearing), distance * math.cos(bearing))
        fleet.append(
            DeviceState(
                device_id=device_id,
                lat=lat,
                lon=lon,
                heading_deg=rng.uniform(0.0, 360.0),
                speed_mps=rng.uniform(profile.min_speed_mps, profile.max_speed_mps),
                profile=profile,
            )
        )
    return fleet


def advance(state: DeviceState, *, dt: float, area: Area, rng: random.Random) -> None:
    """Move a device forward by ``dt`` seconds, in place."""
    profile = state.profile
    scale = math.sqrt(dt)

    state.heading_deg = (
        state.heading_deg + rng.gauss(0.0, profile.heading_sigma_deg * scale)
    ) % 360.0
    drifted = state.speed_mps + rng.gauss(0.0, profile.speed_sigma_mps * scale)
    state.speed_mps = min(max(drifted, profile.min_speed_mps), profile.max_speed_mps)

    heading = math.radians(state.heading_deg)
    distance = state.speed_mps * dt
    east, north = area.offsets(state.lat, state.lon)
    east += distance * math.sin(heading)
    north += distance * math.cos(heading)

    radius = math.hypot(east, north)
    if radius > area.radius_m:
        east, north, state.heading_deg = _reflect(
            east, north, radius=radius, limit=area.radius_m, heading_deg=state.heading_deg
        )

    state.lat, state.lon = area.position(east, north)


def _reflect(
    east: float, north: float, *, radius: float, limit: float, heading_deg: float
) -> tuple[float, float, float]:
    """Bounce a device off the boundary circle the way a billiard ball leaves a cushion.

    The radial component of both the position and the heading is mirrored while the
    tangential one is kept, so the fleet stays in the demo area without every device
    ending up glued to the edge.
    """
    scale = max(2.0 * limit - radius, 0.0) / radius
    normal_deg = math.degrees(math.atan2(east, north))
    return east * scale, north * scale, (2.0 * normal_deg + 180.0 - heading_deg) % 360.0


# --- payloads ------------------------------------------------------------------


def report_payload(state: DeviceState, *, moment: datetime) -> Report:
    """One report exactly as ``POST /api/v1/ingest/locations`` accepts it."""
    return {
        "device_id": state.device_id,
        # Seven decimals is about a centimetre; more only inflates every frame.
        "latitude": round(state.lat, 7),
        "longitude": round(state.lon, 7),
        "timestamp": moment,
    }


def ingest_envelope(reports: Sequence[Report], *, seq: int | None = None) -> bytes:
    if seq is None:
        return orjson.dumps({"items": reports})
    return orjson.dumps({"seq": seq, "items": reports})


def chunked(reports: Sequence[Report], size: int) -> Iterable[Sequence[Report]]:
    for start in range(0, len(reports), size):
        yield reports[start : start + size]


# --- scheduling ----------------------------------------------------------------


def jittered_interval(interval: float, rng: random.Random) -> float:
    """A reporting interval jittered ±20%, so the fleet never reports in lockstep."""
    return interval * rng.uniform(1.0 - JITTER, 1.0 + JITTER)


class Schedule:
    """Per-device deadlines on the monotonic clock, ordered by a heap.

    One wake-up serves every device that has come due, which is what keeps 10,000
    devices from turning into 3,300 timer callbacks a second.
    """

    __slots__ = ("_heap",)

    def __init__(self) -> None:
        self._heap: list[tuple[float, int]] = []

    def push(self, index: int, due: float) -> None:
        heapq.heappush(self._heap, (due, index))

    def pop_due(self, now: float) -> list[int]:
        due: list[int] = []
        while self._heap and self._heap[0][0] <= now:
            due.append(heapq.heappop(self._heap)[1])
        return due

    def next_delay(self, now: float) -> float:
        if not self._heap:
            return IDLE_SLEEP_S
        return max(self._heap[0][0] - now, 0.0)

    def clear(self) -> None:
        self._heap.clear()

    def __len__(self) -> int:
        return len(self._heap)


class Batcher:
    """Groups reports into one request each.

    Batching has to happen where the reports are produced. Letting every sender pull
    from a shared queue looks equivalent but is not: an idle sender always wins the
    next report, so batches never fill and the fleet turns into one request per report.
    """

    __slots__ = ("_oldest_at", "_reports", "size", "window")

    def __init__(self, *, size: int, window: float) -> None:
        self.size = size
        self.window = window
        self._reports: list[Report] = []
        self._oldest_at = 0.0

    def __len__(self) -> int:
        return len(self._reports)

    def add(self, report: Report, now: float) -> None:
        if not self._reports:
            self._oldest_at = now
        self._reports.append(report)

    def ready(self, now: float) -> bool:
        remaining = self.remaining(now)
        if remaining is None:
            return False
        # Asking remaining() rather than recomputing the difference keeps the deadline
        # the sender waits for and the deadline the batch leaves on bit-for-bit equal.
        return len(self._reports) >= self.size or remaining <= 0.0

    def take(self) -> list[Report]:
        """One request's worth of reports.

        Reports beyond the size limit stay for the next request and keep the deadline of
        the batch that just left, so a tick that serves thousands of devices drains in
        several full requests instead of losing the tail or sending an oversized body.
        """
        if len(self._reports) <= self.size:
            batch, self._reports = self._reports, []
            return batch
        batch, self._reports = self._reports[: self.size], self._reports[self.size :]
        return batch

    def remaining(self, now: float) -> float | None:
        """Seconds until the current batch has to leave, or None while it is empty."""
        if not self._reports:
            return None
        return max(self._oldest_at + self.window - now, 0.0)


# --- statistics ----------------------------------------------------------------


class Reservoir:
    """A bounded uniform sample of every value observed (Vitter's algorithm R).

    Latency percentiles over a ten minute run must not cost a list of two million
    floats; this keeps a fixed number of samples and still counts them all.
    """

    __slots__ = ("_rng", "capacity", "count", "values")

    def __init__(self, *, capacity: int, rng: random.Random) -> None:
        self.capacity = capacity
        self.count = 0
        self.values: list[float] = []
        self._rng = rng

    def add(self, value: float) -> None:
        self.count += 1
        if len(self.values) < self.capacity:
            self.values.append(value)
            return
        index = self._rng.randrange(self.count)
        if index < self.capacity:
            self.values[index] = value


def percentile(values: Sequence[float], q: float) -> float | None:
    """Linear interpolation between the closest ranks, as numpy and most tools do."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


@dataclass(slots=True)
class Stats:
    """Counters shared by every task. The event loop is single threaded, so plain
    integers are already atomic enough.

    Report counters are a partition of ``produced``, which is what lets the summary be
    checked rather than believed: a report is either ``sent`` — the server answered for
    it, and then counted it ``accepted`` or ``rejected`` — or ``dropped``, and each of
    them exactly once however many attempts the delivery took. ``failures``,
    ``throttles`` and ``reconnects`` count events, not reports; mixing the two units into
    one number is what makes "12,000 errors" impossible to read.
    """

    rng: random.Random
    started_at: float = 0.0
    produced: int = 0  # reports the fleet generated
    sent: int = 0  # reports the server answered for, counted once each
    accepted: int = 0  # reports the server confirmed
    rejected: int = 0  # reports the server refused: 4xx, or a websocket error frame
    dropped: int = 0  # reports that never got through: shed, retried out, send failed
    failures: int = 0  # transport failures: connect, request, session, undecodable frame
    reconnects: int = 0
    throttles: int = 0
    # Open websockets, or — for the HTTP transport — requests in flight.
    in_flight: int = 0
    # HTTP only: batches waiting for a sender. The one number that explains ``dropped``.
    queued: int = 0
    latency: Reservoir = field(init=False)
    _window_at: float = field(init=False, default=0.0)
    _window_sent: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self.latency = Reservoir(capacity=LATENCY_SAMPLES, rng=self.rng)
        self._window_at = self.started_at

    def snapshot(self, *, now: float) -> dict[str, Any]:
        elapsed = max(now - self.started_at, 1e-9)
        window = max(now - self._window_at, 1e-9)
        window_sent = self.sent - self._window_sent
        self._window_at = now
        self._window_sent = self.sent
        return {
            "elapsed_s": round(now - self.started_at, 1),
            "in_flight": self.in_flight,
            "queued": self.queued,
            "produced": self.produced,
            "sent": self.sent,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "dropped": self.dropped,
            "failures": self.failures,
            "reconnects": self.reconnects,
            "throttles": self.throttles,
            "sent_per_s": self.sent / elapsed,
            "produced_per_s": self.produced / elapsed,
            "window_per_s": window_sent / window,
            "latency_ms": {
                "p50": percentile(self.latency.values, 0.5),
                "p95": percentile(self.latency.values, 0.95),
                "p99": percentile(self.latency.values, 0.99),
                "samples": self.latency.count,
            },
        }


def _format_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}"


def live_gauge(snapshot: Mapping[str, Any], config: Config) -> str:
    """What the run has open right now — which is not the same thing on both transports.

    A websocket run has one socket per device, and "connected" is the number that matters.
    The HTTP transport's pool size is a setting, not a measurement, so reporting it as a
    gauge tells the reader nothing; what varies is how many requests are in flight and how
    deep the sender queue has grown, and the second of those is what explains ``drop``.
    """
    if config.transport == "ws":
        return f"conn {snapshot['in_flight']}/{config.connections}"
    depth = config.connections * QUEUE_DEPTH_FACTOR
    return f"busy {snapshot['in_flight']}/{config.connections} q {snapshot['queued']}/{depth}"


def format_stats(snapshot: Mapping[str, Any], config: Config) -> str:
    latency = snapshot["latency_ms"]
    return (
        f"[{snapshot['elapsed_s']:7.1f}s] "
        f"{live_gauge(snapshot, config):<22} "
        f"sent {snapshot['sent']:<9} "
        f"({snapshot['window_per_s']:.0f}/s now, {snapshot['sent_per_s']:.0f}/s avg)  "
        f"ok {snapshot['accepted']:<9} "
        f"made {snapshot['produced']:<9} "
        f"rej {snapshot['rejected']:<7} "
        f"drop {snapshot['dropped']:<7} "
        f"fail {snapshot['failures']:<5} "
        f"reconn {snapshot['reconnects']:<5} "
        f"thr {snapshot['throttles']:<5} "
        f"ack p50 {_format_ms(latency['p50'])} "
        f"p95 {_format_ms(latency['p95'])} "
        f"p99 {_format_ms(latency['p99'])} ms"
    )


def format_summary(snapshot: Mapping[str, Any], config: Config) -> str:
    latency = snapshot["latency_ms"]
    pool = "sockets" if config.transport == "ws" else "pooled connections"
    rows = (
        ("transport", config.transport),
        ("devices", str(config.devices)),
        ("connections", f"{config.connections} {pool}"),
        ("elapsed", f"{snapshot['elapsed_s']:.1f} s"),
        (
            "produced",
            f"{snapshot['produced']} reports ({snapshot['produced_per_s']:.0f}/s offered)",
        ),
        ("sent", f"{snapshot['sent']} reports ({snapshot['sent_per_s']:.0f}/s on the wire)"),
        ("accepted", f"{snapshot['accepted']} reports"),
        ("rejected", f"{snapshot['rejected']} reports"),
        ("dropped", f"{snapshot['dropped']} reports"),
        ("failures", f"{snapshot['failures']} transport failures"),
        ("reconnects", str(snapshot["reconnects"])),
        ("throttles", str(snapshot["throttles"])),
        (
            "ack latency",
            f"p50 {_format_ms(latency['p50'])} ms  p95 {_format_ms(latency['p95'])} ms  "
            f"p99 {_format_ms(latency['p99'])} ms  ({latency['samples']} accepted responses)",
        ),
    )
    body = "\n".join(f"  {name:<12} {value}" for name, value in rows)
    return f"\n--- load summary ---\n{body}\n  every produced report is either sent or dropped"


class Console:
    """Human facing output. Errors are rate limited: a server that is down must not
    turn into a million identical lines."""

    __slots__ = ("_interval", "_next_error_at")

    def __init__(self, *, verbose: bool) -> None:
        self._interval = VERBOSE_ERROR_INTERVAL_S if verbose else ERROR_INTERVAL_S
        self._next_error_at = 0.0

    def line(self, message: str) -> None:
        _write(message, sys.stdout)

    def error(self, message: str) -> None:
        now = time.monotonic()
        if now < self._next_error_at:
            return
        self._next_error_at = now + self._interval
        _write(message, sys.stderr)


def _write(message: str, stream: TextIO) -> None:
    """Print, surviving a reader that walked away.

    ``generator.py ... | head`` or a compose log consumer going away turns the next write
    into a BrokenPipeError. Raised from inside the handler that was reporting a failure it
    escapes the worker, the supervisor restarts it, and the run quietly goes on at a
    multiple of the configured rate. Output is the one thing that must not end a run.
    """
    with contextlib.suppress(OSError, ValueError):
        print(message, file=stream, flush=True)


# --- retry helpers -------------------------------------------------------------


def parse_retry_after(value: str | None, *, now: datetime) -> float | None:
    """``Retry-After`` in either allowed form: delay seconds or an HTTP date."""
    if not value:
        return None
    raw = value.strip()
    try:
        return max(float(int(raw)), 0.0)
    except ValueError:
        pass
    try:
        moment = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return max((moment - now).total_seconds(), 0.0)


def retry_pause(retry_after: float | None, *, fallback: float) -> float:
    """How long a sender may be held back after backpressure.

    ``Retry-After`` comes from the other side of the wire — possibly from an intermediary
    error page rather than the service — and a sender that obeys ``Retry-After: 3600``
    takes a share of the fleet's load offline for an hour while the run still prints
    stats. Waiting the bounded amount and then trying again is the honest behaviour: the
    reports that do not fit are shed, and ``dropped`` says so.
    """
    return min(fallback if retry_after is None else retry_after, RETRY_AFTER_MAX_S)


# --- configuration -------------------------------------------------------------


def parse_center(raw: str) -> tuple[float, float]:
    parts = raw.split(",")
    if len(parts) != 2:
        raise ValueError(f"center must be LAT,LON: {raw!r}")
    try:
        lat, lon = (float(part) for part in parts)
    except ValueError as exc:
        raise ValueError(f"center must be LAT,LON with numbers: {raw!r}") from exc
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        raise ValueError(f"center is outside [-90,90],[-180,180]: {raw!r}")
    return lat, lon


@dataclass(frozen=True, slots=True)
class Config:
    url: str
    transport: Transport
    devices: int
    interval: float
    connections: int
    batch_size: int
    batch_window_s: float
    max_batch: int
    area: Area
    duration: float
    ramp_up: float
    seed: int
    ingest_key: str
    device_prefix: str
    stats_interval: float
    json_summary: Path | None
    verbose: bool

    @property
    def http_url(self) -> str:
        return f"{self.url}/api/v1/ingest/locations"

    @property
    def ws_url(self) -> str:
        scheme, _, rest = self.url.partition("://")
        return f"{'wss' if scheme == 'https' else 'ws'}://{rest}/ws/ingest"


def _build_parser() -> argparse.ArgumentParser:
    """Every option defaults to ``None`` so the environment can fill the gaps after
    parsing; that is what makes flags beat variables beat built-in defaults."""
    parser = argparse.ArgumentParser(
        prog="generator.py",
        description="Simulate a fleet of drifting devices reporting to GeoTrack.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment fallbacks: GEOTRACK_URL, INGEST_API_KEY, TRANSPORT, DEVICES, "
            "INTERVAL, CONNECTIONS, MAX_CONNECTIONS, BATCH_SIZE, BATCH_WINDOW_MS, MAX_BATCH, "
            "CENTER, RADIUS_KM, DURATION, RAMP_UP, SEED, DEVICE_PREFIX, STATS_INTERVAL, "
            "JSON_SUMMARY, VERBOSE. The compose spellings are accepted too "
            "(GENERATOR_BASE_URL, GENERATOR_DEVICES, GENERATOR_INTERVAL_S, "
            "GENERATOR_BATCH_SIZE, GENERATOR_CONCURRENCY, GENERATOR_DURATION_S, ...), "
            "so the loadtest service needs no command line.\n\n"
            "Counters: a report is produced, then either sent — and counted accepted or "
            "rejected by the server — or dropped. Each exactly once, whatever the retries; "
            "the latency percentiles are of accepted responses only."
        ),
    )
    parser.add_argument("--url", help=f"base URL of the service (default {DEFAULT_URL})")
    parser.add_argument("--transport", choices=("ws", "http"), help="ingest transport (default ws)")
    parser.add_argument(
        "--devices", type=int, help=f"number of simulated devices (default {DEFAULT_DEVICES})"
    )
    parser.add_argument(
        "--interval",
        type=float,
        help=f"mean seconds between reports, jittered ±20%% (default {DEFAULT_INTERVAL_S})",
    )
    parser.add_argument(
        "--connections",
        type=int,
        help="ws: sockets to open (default one per device); http: connection pool size",
    )
    parser.add_argument(
        "--max-connections",
        type=int,
        help=f"upper bound on websockets (default {DEFAULT_MAX_CONNECTIONS})",
    )
    parser.add_argument(
        "--batch-size", type=int, help=f"http: reports per request (default {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--batch-window-ms",
        type=int,
        help=f"http: how long a batch waits to fill (default {DEFAULT_BATCH_WINDOW_MS})",
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        help=(
            f"the server's INGEST_MAX_BATCH; no request or frame carries more reports "
            f"than this (default {DEFAULT_MAX_BATCH})"
        ),
    )
    parser.add_argument(
        "--center", type=parse_center, help=f"LAT,LON of the area (default {DEFAULT_CENTER})"
    )
    parser.add_argument(
        "--radius-km", type=float, help=f"radius of the area (default {DEFAULT_RADIUS_KM})"
    )
    parser.add_argument("--duration", type=float, help="seconds to run, 0 = until Ctrl-C")
    parser.add_argument(
        "--ramp-up",
        type=float,
        help=f"spread connects and first reports over N seconds (default {DEFAULT_RAMP_UP_S})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help=(
            f"movement seed (default {DEFAULT_SEED}); the n-th report of a device carries "
            f"the same coordinates in every run with this seed, whatever the timing, the "
            f"reconnects or --connections did — not the timestamps, and not how many "
            f"reports a run gets through"
        ),
    )
    parser.add_argument("--ingest-key", help="value of the X-Ingest-Key header")
    parser.add_argument("--device-prefix", help=f"device id prefix (default {DEFAULT_PREFIX!r})")
    parser.add_argument(
        "--stats-interval",
        type=float,
        help=f"seconds between progress lines (default {DEFAULT_STATS_INTERVAL_S})",
    )
    parser.add_argument("--json-summary", type=Path, help="write the final summary to this file")
    parser.add_argument("--verbose", action="store_true", default=None, help="log every failure")
    return parser


# The compose loadtest service names its variables after the service rather than after
# the flags. Accepting both spellings is what makes `docker compose --profile loadtest run
# generator` configurable without a command line, and it costs one lookup.
ENV_ALIASES: Mapping[str, tuple[str, ...]] = {
    "GEOTRACK_URL": ("GENERATOR_BASE_URL", "GENERATOR_URL"),
    "TRANSPORT": ("GENERATOR_TRANSPORT",),
    "DEVICES": ("GENERATOR_DEVICES",),
    "INTERVAL": ("GENERATOR_INTERVAL_S", "GENERATOR_INTERVAL"),
    "CONNECTIONS": ("GENERATOR_CONCURRENCY", "GENERATOR_CONNECTIONS"),
    "MAX_CONNECTIONS": ("GENERATOR_MAX_CONNECTIONS",),
    "BATCH_SIZE": ("GENERATOR_BATCH_SIZE",),
    "BATCH_WINDOW_MS": ("GENERATOR_BATCH_WINDOW_MS",),
    "MAX_BATCH": ("GENERATOR_MAX_BATCH", "INGEST_MAX_BATCH"),
    "CENTER": ("GENERATOR_CENTER",),
    "RADIUS_KM": ("GENERATOR_RADIUS_KM",),
    "DURATION": ("GENERATOR_DURATION_S", "GENERATOR_DURATION"),
    "RAMP_UP": ("GENERATOR_RAMP_UP_S", "GENERATOR_RAMP_UP"),
    "SEED": ("GENERATOR_SEED",),
    "DEVICE_PREFIX": ("GENERATOR_DEVICE_PREFIX",),
    "STATS_INTERVAL": ("GENERATOR_STATS_INTERVAL",),
    "JSON_SUMMARY": ("GENERATOR_JSON_SUMMARY",),
    "VERBOSE": ("GENERATOR_VERBOSE",),
}


def _env_raw(env: Mapping[str, str], key: str) -> str:
    for name in (key, *ENV_ALIASES.get(key, ())):
        value = env.get(name, "").strip()
        if value:
            return value
    return ""


def _env_str(env: Mapping[str, str], key: str, fallback: str) -> str:
    return _env_raw(env, key) or fallback


def _number[T: (int, float)](
    parser: argparse.ArgumentParser,
    flag: T | None,
    env: Mapping[str, str],
    key: str,
    fallback: T,
    cast: Callable[[str], T],
) -> T:
    """A flag beats an environment variable beats the built-in default.

    The flag is compared against ``None`` and not truth-tested: ``--devices 0`` is wrong
    and must be reported, not silently replaced by the default.
    """
    if flag is not None:
        return flag
    raw = _env_raw(env, key)
    if not raw:
        return fallback
    try:
        return cast(raw)
    except ValueError:
        parser.error(f"{key}={raw!r} is not a number")


def parse_config(
    argv: Sequence[str] | None = None, *, env: Mapping[str, str] | None = None
) -> Config:
    environ = os.environ if env is None else env
    parser = _build_parser()
    args = parser.parse_args(argv)

    url = (args.url or _env_str(environ, "GEOTRACK_URL", DEFAULT_URL)).rstrip("/")
    if not url.startswith(("http://", "https://")):
        parser.error(f"--url must start with http:// or https://, got {url!r}")

    transport: Transport = args.transport or _env_str(environ, "TRANSPORT", "ws")  # type: ignore[assignment]
    if transport not in ("ws", "http"):
        parser.error(f"--transport must be ws or http, got {transport!r}")

    devices = _number(parser, args.devices, environ, "DEVICES", DEFAULT_DEVICES, int)
    interval = _number(parser, args.interval, environ, "INTERVAL", DEFAULT_INTERVAL_S, float)
    max_connections = _number(
        parser, args.max_connections, environ, "MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS, int
    )
    batch_size = _number(parser, args.batch_size, environ, "BATCH_SIZE", DEFAULT_BATCH_SIZE, int)
    batch_window_ms = _number(
        parser, args.batch_window_ms, environ, "BATCH_WINDOW_MS", DEFAULT_BATCH_WINDOW_MS, int
    )
    max_batch = _number(parser, args.max_batch, environ, "MAX_BATCH", DEFAULT_MAX_BATCH, int)
    radius_km = _number(parser, args.radius_km, environ, "RADIUS_KM", DEFAULT_RADIUS_KM, float)
    duration = _number(parser, args.duration, environ, "DURATION", 0.0, float)
    ramp_up = _number(parser, args.ramp_up, environ, "RAMP_UP", DEFAULT_RAMP_UP_S, float)
    seed = _number(parser, args.seed, environ, "SEED", DEFAULT_SEED, int)
    stats_interval = _number(
        parser, args.stats_interval, environ, "STATS_INTERVAL", DEFAULT_STATS_INTERVAL_S, float
    )
    requested = _number(parser, args.connections, environ, "CONNECTIONS", 0, int)

    center = args.center or parse_center(_env_str(environ, "CENTER", DEFAULT_CENTER))
    prefix = args.device_prefix or _env_str(environ, "DEVICE_PREFIX", DEFAULT_PREFIX)
    ingest_key = args.ingest_key or _env_str(environ, "INGEST_API_KEY", "")
    summary = _env_str(environ, "JSON_SUMMARY", "")
    summary_path = args.json_summary or (Path(summary) if summary else None)
    verbose = bool(
        args.verbose
        if args.verbose is not None
        else _env_str(environ, "VERBOSE", "").lower() in ("1", "true", "yes", "on")
    )

    if devices < 1:
        parser.error("--devices must be at least 1")
    if interval <= 0:
        parser.error("--interval must be positive")
    if max_batch < 1:
        parser.error("--max-batch must be at least 1")
    if batch_size < 1:
        parser.error("--batch-size must be at least 1")
    if batch_size > max_batch:
        if args.batch_size is not None or _env_raw(environ, "BATCH_SIZE"):
            parser.error(f"--batch-size must not exceed --max-batch ({max_batch})")
        # Nobody asked for the default request size, so a deployment with a lower ceiling
        # gets requests that fit rather than a refusal to start over a setting for the
        # other transport.
        batch_size = max_batch
    if batch_window_ms < 0:
        parser.error("--batch-window-ms must not be negative")
    if radius_km <= 0:
        parser.error("--radius-km must be positive")
    if duration < 0 or ramp_up < 0:
        parser.error("--duration and --ramp-up must not be negative")
    if stats_interval <= 0:
        parser.error("--stats-interval must be positive")
    if max_connections < 1:
        parser.error("--max-connections must be at least 1")
    if requested < 0:
        parser.error("--connections must not be negative")
    if not ingest_key:
        parser.error("--ingest-key (or INGEST_API_KEY) is required")

    if requested == 0:
        # One socket per device is what the brief means by concurrent devices; the HTTP
        # transport instead shares a small pool, because batches, not sockets, carry it.
        requested = devices if transport == "ws" else DEFAULT_HTTP_CONNECTIONS
    connections = max(min(requested, devices, max_connections), 1)

    return Config(
        url=url,
        transport=transport,
        devices=devices,
        interval=interval,
        connections=connections,
        batch_size=batch_size,
        batch_window_s=batch_window_ms / 1000.0,
        max_batch=max_batch,
        area=Area(lat=center[0], lon=center[1], radius_m=radius_km * 1000.0),
        duration=duration,
        ramp_up=ramp_up,
        seed=seed,
        ingest_key=ingest_key,
        device_prefix=prefix,
        stats_interval=stats_interval,
        json_summary=summary_path,
        verbose=verbose,
    )


# --- process limits ------------------------------------------------------------


def ensure_file_limit(connections: int) -> int:
    """Make room for one descriptor per socket, or explain why the run cannot start."""
    required = connections + FD_HEADROOM
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= required:
        return soft

    target = required if hard == resource.RLIM_INFINITY else min(required, hard)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"the file descriptor limit is {soft}, this run needs {required} and raising it "
            f"failed ({exc}). Try `ulimit -n {required}`, fewer --connections, "
            f"or run the generator in its container."
        ) from exc

    raised, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if raised < required:
        raise RuntimeError(
            f"the file descriptor limit is {raised}, this run needs {required}. "
            f"Lower --connections or raise the hard limit."
        )
    return raised


# --- transports ----------------------------------------------------------------


class Devices:
    """A slice of the fleet with its own schedule; shared by both transports.

    The schedule and the per-device clocks are plain mutable state, correct only while a
    single task drives them. :meth:`driving` says so in code: it hands the fleet to one
    owner, refuses a second, and re-arms rather than re-seeds when the owner comes back.
    Without that, a supervisor restarting a worker gives every device a second entry in
    the schedule and the fleet reports at twice the configured rate for the rest of the
    run — silently, because every counter still adds up.
    """

    __slots__ = (
        "_area",
        "_clocks",
        "_driving",
        "_dt",
        "_interval",
        "_rngs",
        "_states",
        "rng",
        "schedule",
    )

    def __init__(self, states: Sequence[DeviceState], *, config: Config, rng: random.Random):
        self._states = states
        self._rngs = [device_rng(config.seed, state.device_id) for state in states]
        self._clocks = [report_rng(config.seed, state.device_id) for state in states]
        # Seconds a device will have been moving when its next report is due. The first
        # one is the plain interval so that report n of a device is a function of the
        # seed and n alone, never of how often this run happened to be interrupted.
        self._dt = [config.interval] * len(states)
        self._area = config.area
        self._interval = config.interval
        self._driving = False
        self.rng = rng
        self.schedule = Schedule()

    def __len__(self) -> int:
        return len(self._states)

    @contextlib.contextmanager
    def driving(self, now: float, *, spread: float = 0.0) -> Iterator[None]:
        """Claim the fleet for the calling task, arm it, and release it on the way out."""
        if self._driving:
            raise RuntimeError(
                f"a fleet of {len(self)} devices is already being driven; one slice of the "
                f"fleet belongs to exactly one task"
            )
        self._driving = True
        try:
            self.start(now, spread=spread)
            yield
        finally:
            self._driving = False

    def start(self, now: float, *, spread: float = 0.0) -> None:
        """Give every device exactly one deadline, whether this is the first call or not.

        First time round the deadlines are laid out over the ramp-up window and jittered
        across one interval, so the fleet does not start in lockstep. On a later call —
        a reconnect, or a supervisor restarting the worker — the schedule is rebuilt
        rather than added to: every device keeps exactly one entry, a device that was in
        flight when the task died comes back, and the fleet that waited out an outage
        spreads its catch-up over an interval instead of arriving as one burst.
        """
        count = len(self._states)
        self.schedule.clear()
        for index in range(count):
            offset = spread * index / count
            self.schedule.push(index, now + offset + self.rng.random() * self._interval)

    def step(self, index: int, moment: datetime) -> Report:
        """Move a device over the interval it was scheduled for and build its report.

        The step deliberately does not measure the wall clock. Real elapsed time makes
        every trajectory depend on how the loop, the network and the server behaved, which
        is what would leave `--seed` guaranteeing nothing beyond the starting positions;
        it also turns a stalled connection into a kilometre-long jump.
        """
        state = self._states[index]
        advance(state, dt=self._dt[index], area=self._area, rng=self._rngs[index])
        return report_payload(state, moment=moment)

    def reschedule(self, index: int, now: float) -> None:
        """Put a device back for its next report, one interval from now."""
        delay = jittered_interval(self._interval, self._clocks[index])
        self._dt[index] = delay
        self.schedule.push(index, now + delay)

    def defer(self, index: int, until: float) -> None:
        """Hold a device back without consuming its next reporting interval.

        Used when the server asked for a pause: the device has not reported, so its clock
        must not advance, and the spread comes from the schedule's own stream so that
        backpressure cannot change where any device ends up.
        """
        self.schedule.push(index, until + jittered_interval(self._interval, self.rng))


class WebsocketWorker:
    """One websocket carrying one or more devices."""

    __slots__ = (
        "_config",
        "_connected_once",
        "_console",
        "_devices",
        "_pause_until",
        "_pending",
        "_seq",
        "_start_delay",
        "_stats",
    )

    def __init__(
        self,
        *,
        index: int,
        states: Sequence[DeviceState],
        config: Config,
        stats: Stats,
        console: Console,
    ) -> None:
        self._config = config
        self._stats = stats
        self._console = console
        self._devices = Devices(
            states, config=config, rng=random.Random(f"{config.seed}:ws:{index}")
        )
        self._start_delay = config.ramp_up * index / max(config.connections, 1)
        # seq -> (sent at, reports in the frame), so an ack can be timed and an error
        # frame can say how many reports the server threw away.
        self._pending: dict[int, tuple[float, int]] = {}
        self._pause_until = 0.0
        self._seq = 0
        self._connected_once = False

    @property
    def devices(self) -> Devices:
        """The slice of the fleet this worker owns, for as long as it runs."""
        return self._devices

    async def run(self) -> None:
        await asyncio.sleep(self._start_delay)
        # The fleet is claimed for as long as this worker runs. A restart re-arms the
        # same devices instead of adding a second copy of each to the schedule.
        with self._devices.driving(time.monotonic()):
            delay = RECONNECT_BASE_S
            while True:
                delay = await self._session(delay)

    async def _session(self, delay: float) -> float:
        """One connection attempt. Returns the backoff to use if it has to be repeated."""
        try:
            async with ws_connect(
                self._config.ws_url,
                additional_headers={"X-Ingest-Key": self._config.ingest_key},
                open_timeout=WS_OPEN_TIMEOUT_S,
                ping_interval=WS_PING_INTERVAL_S,
                ping_timeout=WS_PING_INTERVAL_S,
                close_timeout=5.0,
                max_size=WS_MAX_FRAME_BYTES,
                compression=None,
            ) as socket:
                if self._connected_once:
                    self._stats.reconnects += 1
                self._connected_once = True
                self._stats.in_flight += 1
                # A socket that opened means the service is back: the next failure has
                # to start its backoff from the bottom again, not from where it stopped.
                delay = RECONNECT_BASE_S
                receiver = asyncio.create_task(self._receive(socket))
                try:
                    await self._send_forever(socket)
                finally:
                    self._stats.in_flight -= 1
                    receiver.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await receiver
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._stats.failures += 1
            self._console.error(f"websocket: {exc!r}; retrying in {delay:.1f}s")
            await asyncio.sleep(delay * (0.5 + self._devices.rng.random()))
            return min(delay * 2.0, RECONNECT_MAX_S)
        return delay

    async def _send_forever(self, socket: ClientConnection) -> None:
        devices = self._devices
        while True:
            now = time.monotonic()
            due = devices.schedule.pop_due(now)
            if not due:
                await asyncio.sleep(devices.schedule.next_delay(now))
                continue

            pause = self._pause_until - now
            if pause > 0:
                # Throttled: hold the reports back rather than answer backpressure with
                # a burst the moment the window reopens.
                for index in due:
                    devices.defer(index, now + pause)
                await asyncio.sleep(pause)
                continue

            moment = datetime.now(UTC)
            reports = [devices.step(index, moment) for index in due]
            for index in due:
                devices.reschedule(index, now)
            self._stats.produced += len(reports)
            await self._send(socket, reports)

    async def _send(self, socket: ClientConnection, reports: list[Report]) -> None:
        """Put one tick's reports on the wire, in frames the server will accept whole."""
        undelivered = len(reports)
        try:
            for chunk in chunked(reports, self._config.max_batch):
                self._seq += 1
                self._remember(self._seq, time.monotonic(), len(chunk))
                await socket.send(ingest_envelope(chunk, seq=self._seq).decode())
                self._stats.sent += len(chunk)
                undelivered -= len(chunk)
        except BaseException:
            # A peer that reset mid-send, or a shutdown: these reports existed and never
            # reached anyone. Counting them is the difference between a summary that
            # balances and one where "sent == accepted" only because the losses vanished.
            self._stats.dropped += undelivered
            raise

    def _remember(self, seq: int, at: float, size: int) -> None:
        pending = self._pending
        pending[seq] = (at, size)
        if len(pending) > PENDING_ACKS_MAX:
            # Sequence numbers only grow, so the lowest keys are the abandoned ones.
            for stale in sorted(pending)[: len(pending) - PENDING_ACKS_MAX]:
                del pending[stale]

    def _forget(self, seq: int) -> int:
        """Drop a frame from the outstanding set and say how many reports it carried."""
        outstanding = self._pending.pop(seq, None)
        return 0 if outstanding is None else outstanding[1]

    async def _receive(self, socket: ClientConnection) -> None:
        async for message in socket:
            self._handle(message)

    def _handle(self, message: bytes | str) -> None:
        try:
            frame = orjson.loads(message)
        except orjson.JSONDecodeError:
            self._stats.failures += 1
            return
        if not isinstance(frame, dict):
            return

        match frame.get("type"):
            case "ack":
                seq = frame.get("seq")
                outstanding = self._pending.pop(seq, None) if isinstance(seq, int) else None
                if outstanding is not None:
                    self._stats.latency.add((time.monotonic() - outstanding[0]) * 1000.0)
                accepted = frame.get("accepted")
                self._stats.accepted += accepted if isinstance(accepted, int) else 0
            case "throttle":
                retry_ms = frame.get("retry_after_ms")
                retry_s = (retry_ms if isinstance(retry_ms, int | float) else 1_000) / 1000.0
                pause = retry_pause(retry_s, fallback=RECONNECT_BASE_S)
                self._pause_until = max(self._pause_until, time.monotonic() + pause)
                self._stats.throttles += 1
            case "error":
                # The server refused a frame, so every report in it is gone. Which frame
                # it was is in `seq`; without one the last frame sent is the only sane
                # guess, and guessing zero would quietly unbalance the summary.
                seq = frame.get("seq")
                self._stats.rejected += self._forget(seq if isinstance(seq, int) else self._seq)
                self._console.error(f"websocket rejected a batch: {frame.get('detail')}")


class HttpRunner:
    """The whole fleet over a bounded connection pool, micro-batched."""

    __slots__ = ("_config", "_console", "_devices", "_queue", "_stats")

    def __init__(
        self,
        *,
        states: Sequence[DeviceState],
        config: Config,
        stats: Stats,
        console: Console,
    ) -> None:
        self._config = config
        self._stats = stats
        self._console = console
        self._devices = Devices(states, config=config, rng=random.Random(f"{config.seed}:http"))
        # Bounded in batches, so the memory a slow service can cost is bounded too.
        self._queue: asyncio.Queue[list[Report]] = asyncio.Queue(
            maxsize=config.connections * QUEUE_DEPTH_FACTOR
        )

    @property
    def devices(self) -> Devices:
        """The fleet, owned by the single producer task this runner keeps alive."""
        return self._devices

    async def run(self) -> None:
        limits = httpx.Limits(
            max_connections=self._config.connections,
            max_keepalive_connections=self._config.connections,
            keepalive_expiry=30.0,
        )
        async with (
            httpx.AsyncClient(
                limits=limits,
                timeout=httpx.Timeout(HTTP_TIMEOUT_S),
                headers={
                    "X-Ingest-Key": self._config.ingest_key,
                    "Content-Type": "application/json",
                },
            ) as client,
            asyncio.TaskGroup() as group,
        ):
            group.create_task(supervise("fleet", self._produce, self._stats, self._console))
            for index in range(self._config.connections):
                group.create_task(
                    supervise(
                        f"sender-{index}", lambda: self._consume(client), self._stats, self._console
                    )
                )

    async def _produce(self) -> None:
        devices = self._devices
        batcher = Batcher(size=self._config.batch_size, window=self._config.batch_window_s)
        # One producer owns the fleet. Being restarted by the supervisor re-arms it; a
        # second producer on the same fleet is a bug and says so rather than doubling the
        # offered load for the rest of the run.
        with devices.driving(time.monotonic(), spread=self._config.ramp_up):
            while True:
                now = time.monotonic()
                due = devices.schedule.pop_due(now)
                if due:
                    moment = datetime.now(UTC)
                    for index in due:
                        batcher.add(devices.step(index, moment), now)
                        devices.reschedule(index, now)
                    self._stats.produced += len(due)
                while batcher.ready(now):
                    self._offer(batcher.take())
                await asyncio.sleep(self._sleep_for(batcher, now))

    def _sleep_for(self, batcher: Batcher, now: float) -> float:
        """Wake for whichever comes first: the next device or the batch window closing."""
        delay = self._devices.schedule.next_delay(now)
        pending = batcher.remaining(now)
        return delay if pending is None else min(delay, pending)

    def _offer(self, batch: list[Report]) -> None:
        try:
            self._queue.put_nowait(batch)
        except asyncio.QueueFull:
            # The service is slower than the fleet. Shed the oldest batch — it holds the
            # least interesting reports — and count it, rather than grow without bound.
            with contextlib.suppress(asyncio.QueueEmpty):
                self._stats.dropped += len(self._queue.get_nowait())
            with contextlib.suppress(asyncio.QueueFull):
                self._queue.put_nowait(batch)
        self._stats.queued = self._queue.qsize()

    async def _consume(self, client: httpx.AsyncClient) -> None:
        while True:
            batch = await self._queue.get()
            self._stats.queued = self._queue.qsize()
            await self._post(client, batch)

    async def _post(self, client: httpx.AsyncClient, batch: list[Report]) -> None:
        """Deliver one batch, retrying congestion but counting the reports once.

        A batch is a fixed set of reports however many attempts it takes to place it, so
        it moves the counters exactly once, at the attempt that settles it: accepted,
        rejected, or — when the server never stops saying "later" — dropped. Counting per
        attempt is what inflates the headline throughput threefold in precisely the
        backpressure regime the run exists to measure, and fills the latency reservoir
        with the response times of refusals.
        """
        payload = ingest_envelope(batch)
        delay = RECONNECT_BASE_S
        for _ in range(HTTP_ATTEMPTS):
            started = time.monotonic()
            self._stats.in_flight += 1
            try:
                response = await self._request(client, payload)
            finally:
                self._stats.in_flight -= 1
            if response is None:
                await asyncio.sleep(delay)
                delay = min(delay * 2.0, RECONNECT_MAX_S)
                continue

            if response.status_code < 300:
                # Only an accepted response times the ingest path; a refusal is fast for
                # reasons that say nothing about how long ingestion takes.
                self._stats.latency.add((time.monotonic() - started) * 1000.0)
                self._stats.sent += len(batch)
                self._stats.accepted += _accepted_count(response, len(batch))
                return
            if response.status_code in (429, 503):
                self._stats.throttles += 1
                retry_after = parse_retry_after(
                    response.headers.get("retry-after"), now=datetime.now(UTC)
                )
                await asyncio.sleep(retry_pause(retry_after, fallback=delay))
                delay = min(delay * 2.0, RECONNECT_MAX_S)
                continue
            # 4xx is a bug in the payload, not congestion: retrying would only repeat it.
            self._stats.sent += len(batch)
            self._stats.rejected += len(batch)
            self._console.error(f"ingest returned {response.status_code}: {response.text[:200]}")
            return
        self._stats.dropped += len(batch)

    async def _request(self, client: httpx.AsyncClient, payload: bytes) -> httpx.Response | None:
        try:
            return await client.post(self._config.http_url, content=payload)
        except httpx.HTTPError as exc:
            self._stats.failures += 1
            self._console.error(f"ingest request failed: {exc!r}")
            return None


def _accepted_count(response: httpx.Response, fallback: int) -> int:
    try:
        body = orjson.loads(response.content)
    except orjson.JSONDecodeError:
        return fallback
    if isinstance(body, dict) and isinstance(body.get("accepted"), int):
        return int(body["accepted"])
    return fallback


# --- runner --------------------------------------------------------------------


async def supervise(
    name: str,
    factory: Callable[[], Coroutine[Any, Any, None]],
    stats: Stats,
    console: Console,
) -> None:
    """Keep a task alive: one device losing an argument with the network must not end
    the run, and a crashed sender must come back instead of silently halving the load."""
    delay = RECONNECT_BASE_S
    while True:
        if await _run_once(name, factory, stats, console):
            return
        await asyncio.sleep(delay)
        delay = min(delay * 2.0, RECONNECT_MAX_S)


async def _run_once(
    name: str,
    factory: Callable[[], Coroutine[Any, Any, None]],
    stats: Stats,
    console: Console,
) -> bool:
    try:
        await factory()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        stats.failures += 1
        console.error(f"{name} failed: {exc!r}; restarting")
        return False
    return True


async def _report_loop(stats: Stats, config: Config, console: Console) -> None:
    while True:
        await asyncio.sleep(config.stats_interval)
        console.line(format_stats(stats.snapshot(now=time.monotonic()), config))


async def _wait_for_stop(stop: asyncio.Event, duration: float) -> None:
    if duration <= 0:
        await stop.wait()
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), duration)


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # uvloop 0.22 still inspects the callback with asyncio.iscoroutinefunction, which
        # 3.14 deprecates: the warning belongs to the event loop, not to this call site.
        with (
            contextlib.suppress(NotImplementedError),
            warnings.catch_warnings(action="ignore", category=DeprecationWarning),
        ):
            loop.add_signal_handler(sig, stop.set)


async def run(config: Config, console: Console) -> dict[str, Any]:
    fleet = build_fleet(
        count=config.devices, prefix=config.device_prefix, area=config.area, seed=config.seed
    )
    stats = Stats(rng=random.Random(config.seed), started_at=time.monotonic())
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    tasks: list[asyncio.Task[None]] = []
    if config.transport == "ws":
        for index in range(config.connections):
            worker = WebsocketWorker(
                index=index,
                states=fleet[index :: config.connections],
                config=config,
                stats=stats,
                console=console,
            )
            tasks.append(
                asyncio.create_task(supervise(f"device-{index}", worker.run, stats, console))
            )
    else:
        runner = HttpRunner(states=fleet, config=config, stats=stats, console=console)
        tasks.append(asyncio.create_task(supervise("http", runner.run, stats, console)))
    tasks.append(
        asyncio.create_task(
            supervise("stats", lambda: _report_loop(stats, config, console), stats, console)
        )
    )

    try:
        await _wait_for_stop(stop, config.duration)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return stats.snapshot(now=time.monotonic())


def _loop_factory() -> Callable[[], asyncio.AbstractEventLoop] | None:
    try:
        import uvloop
    except ModuleNotFoundError:
        return None
    return uvloop.new_event_loop


def main(argv: Sequence[str] | None = None) -> int:
    config = parse_config(argv)
    console = Console(verbose=config.verbose)
    try:
        ensure_file_limit(config.connections)
    except RuntimeError as exc:
        print(f"generator: {exc}", file=sys.stderr)
        return 2

    target = config.ws_url if config.transport == "ws" else config.http_url
    console.line(
        f"generator: {config.devices} devices -> {target} "
        f"({config.connections} connections, interval {config.interval:g}s ±20%, "
        f"seed {config.seed}, ramp-up {config.ramp_up:g}s)"
    )

    summary = asyncio.run(run(config, console), loop_factory=_loop_factory())
    # Print before writing: a bad output path must not cost the numbers of a long run.
    console.line(format_summary(summary, config))
    if config.json_summary is None:
        return 0

    report = {
        **summary,
        "config": {
            "url": config.url,
            "transport": config.transport,
            "devices": config.devices,
            "connections": config.connections,
            "interval_s": config.interval,
            "seed": config.seed,
        },
    }
    try:
        config.json_summary.write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    except OSError as exc:
        print(f"generator: could not write {config.json_summary}: {exc}", file=sys.stderr)
        return 1
    console.line(f"  wrote {config.json_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
