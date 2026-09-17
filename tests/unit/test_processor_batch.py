"""Pure decisions taken before a batch reaches the database."""

import pytest
from pydantic import TypeAdapter, ValidationError

from geotrack.db.errors import (
    DEADLOCK_DETECTED,
    FOREIGN_KEY_VIOLATION,
    SERIALIZATION_FAILURE,
    UNIQUE_VIOLATION,
)
from geotrack.messaging.codec import LocationRecord
from geotrack.processor.batch import (
    DEVICE_ID_RE,
    LOCK_NOT_AVAILABLE,
    MAX_ATTEMPTS,
    RETRY_BASE_DELAY_S,
    latest_per_device,
    retry_delay,
    should_retry,
)
from geotrack.schemas.common import DeviceId

_API_DEVICE_ID = TypeAdapter(DeviceId)


def record(device_id: str, reported_ms: int, *, lat: float = 50.0) -> LocationRecord:
    return LocationRecord(
        device_id=device_id, lat=lat, lon=30.0, reported_ms=reported_ms, received_ms=reported_ms
    )


def test_latest_per_device_keeps_the_newest_report() -> None:
    records = [
        record("a", 1_000, lat=10.0),
        record("b", 5_000, lat=20.0),
        record("a", 3_000, lat=30.0),
        record("a", 2_000, lat=40.0),
    ]

    kept = latest_per_device(records)

    assert [(r.device_id, r.reported_ms, r.lat) for r in kept] == [
        ("a", 3_000, 30.0),
        ("b", 5_000, 20.0),
    ]


def test_latest_per_device_keeps_the_first_of_equally_recent_reports() -> None:
    kept = latest_per_device([record("a", 1_000, lat=1.0), record("a", 1_000, lat=2.0)])

    assert [(r.device_id, r.lat) for r in kept] == [("a", 1.0)]


def test_latest_per_device_preserves_first_appearance_order() -> None:
    kept = latest_per_device([record("c", 1), record("a", 2), record("b", 3), record("c", 4)])

    assert [r.device_id for r in kept] == ["c", "a", "b"]


def test_latest_per_device_of_nothing_is_nothing() -> None:
    assert latest_per_device([]) == []


@pytest.mark.parametrize(
    ("sqlstate", "attempt", "expected"),
    [
        (SERIALIZATION_FAILURE, 1, True),
        (DEADLOCK_DETECTED, 1, True),
        # A zone deleted mid-batch: re-reading the zones makes the retry succeed.
        (FOREIGN_KEY_VIOLATION, 1, True),
        # The other owner of the shard was still holding the handover lock.
        (LOCK_NOT_AVAILABLE, 1, True),
        (LOCK_NOT_AVAILABLE, MAX_ATTEMPTS, False),
        (SERIALIZATION_FAILURE, MAX_ATTEMPTS - 1, True),
        (SERIALIZATION_FAILURE, MAX_ATTEMPTS, False),
        (UNIQUE_VIOLATION, 1, False),
        ("23514", 1, False),
        # A statement that ran away is not a race, and repeating it would only run away
        # again; the lock wait has a budget of its own so it never lands here.
        ("57014", 1, False),
        (None, 1, False),
    ],
)
def test_retry_decision_table(sqlstate: str | None, attempt: int, expected: bool) -> None:
    assert should_retry(sqlstate, attempt) is expected


def test_retry_delay_grows_exponentially_and_stays_jittered() -> None:
    for attempt in range(1, MAX_ATTEMPTS):
        ceiling = RETRY_BASE_DELAY_S * 2 ** (attempt - 1)
        delays = {retry_delay(attempt) for _ in range(200)}
        assert all(ceiling / 2 <= delay <= ceiling for delay in delays)
        # Jitter exists so that replicas retrying the same shard do not march in step.
        assert len(delays) > 1


@pytest.mark.parametrize(
    "device_id",
    [
        "dev-1",
        "d",
        "d" * 64,
        "a.b:c-d",
        "01234",
        "",
        "d" * 65,
        # The seam the alert frame falls through: the stream codec accepts all of these.
        "dev 1",
        "dev/1",
        "dev_1",
        "dev\n",
        "пристрій",
    ],
)
def test_the_storable_device_id_rule_agrees_with_the_api_model(device_id: str) -> None:
    """One definition of a device id across the system.

    The processor is the last place that can refuse a report, and it is the only one
    that learns about a bad id after the row is already committed. Storing an id the
    REST model cannot carry would mean an alert nobody can be told about.
    """
    try:
        _API_DEVICE_ID.validate_python(device_id)
    except ValidationError:
        accepted_by_the_api = False
    else:
        accepted_by_the_api = True

    assert bool(DEVICE_ID_RE.fullmatch(device_id)) is accepted_by_the_api
