"""What an oversized payload costs.

The batch limit is only worth having if refusing a payload is cheaper than accepting
one; otherwise the limit is an invitation to send the largest body the endpoint will
read and let the server pay for parsing it.
"""

import time

import orjson
import pytest

from geotrack.schemas.ingest import (
    MAX_REPORT_BYTES,
    BatchTooLargeError,
    max_payload_bytes,
    parse_ingest_payload,
)

LIMIT = 1_000
MINIMAL = {"device_id": "a", "latitude": 1, "longitude": 2, "timestamp": 1789560000}
LARGEST = {
    "device_id": "d" * 64,
    "latitude": -33.868819999999,
    "longitude": 151.209290000001,
    "timestamp": "2026-09-16T12:00:00.123456+03:00",
}


def _array(count: int, report: dict[str, object] | None = None) -> bytes:
    return orjson.dumps([report or MINIMAL] * count)


def _fastest(payload: bytes, rounds: int = 5) -> float:
    """Best of several runs: the floor is what an attacker gets to repeat."""
    best = float("inf")
    for _ in range(rounds):
        started = time.perf_counter()
        with pytest.raises((BatchTooLargeError, ValueError)):
            parse_ingest_payload(payload, max_items=LIMIT)
        best = min(best, time.perf_counter() - started)
    return best


def test_the_largest_legal_report_fits_the_budget_the_ceiling_is_built_on() -> None:
    # The body ceiling assumes a report cannot be much larger than this; if it could, a
    # legitimate full batch would be refused by its length.
    assert len(orjson.dumps(LARGEST)) + 1 <= MAX_REPORT_BYTES


def test_a_full_batch_fits_the_ceiling_with_room_for_formatting() -> None:
    compact = len(_array(LIMIT, LARGEST))

    assert compact < max_payload_bytes(LIMIT)
    # Indented JSON from a hand-written client still gets through.
    assert len(orjson.dumps([LARGEST] * LIMIT, option=orjson.OPT_INDENT_2)) < max_payload_bytes(
        LIMIT
    )


@pytest.mark.parametrize("shape", ["array", "envelope"])
def test_an_oversized_batch_is_refused_with_the_number_it_offered(shape: str) -> None:
    items = [MINIMAL] * (LIMIT + 5)
    raw = orjson.dumps(items if shape == "array" else {"seq": 1, "items": items})

    with pytest.raises(BatchTooLargeError) as excinfo:
        parse_ingest_payload(raw, max_items=LIMIT)

    assert excinfo.value.offered == LIMIT + 5
    assert excinfo.value.limit == LIMIT
    assert "limit is 1000" in str(excinfo.value)


def test_the_batch_limit_is_read_before_the_reports_are() -> None:
    # The last report would fail validation. The limit still wins, because it is decided
    # from the length of the array rather than from the contents of it.
    items = [MINIMAL] * LIMIT + [MINIMAL | {"latitude": 999}]

    with pytest.raises(BatchTooLargeError):
        parse_ingest_payload(orjson.dumps(items), max_items=LIMIT)


def test_refusing_an_oversized_payload_costs_no_more_than_accepting_a_legal_one() -> None:
    # The worst payload the ceiling lets through, against the batch the service exists to
    # take. Parse-then-check made the first of these an order of magnitude dearer than
    # the second, which is a free way to burn an event loop.
    worst = _array(max_payload_bytes(LIMIT) // (len(orjson.dumps(MINIMAL)) + 1))
    assert len(worst) <= max_payload_bytes(LIMIT)

    legal = _array(LIMIT)
    accepted = float("inf")
    for _ in range(5):
        started = time.perf_counter()
        parse_ingest_payload(legal, max_items=LIMIT)
        accepted = min(accepted, time.perf_counter() - started)

    assert _fastest(worst) < accepted * 5
