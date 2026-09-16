from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from geotrack.schemas.ingest import (
    LocationReport,
    ReportWindowError,
    check_report_window,
    parse_ingest_payload,
)

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def test_parses_single_report() -> None:
    seq, items = parse_ingest_payload(
        b'{"device_id":"dev-1","latitude":50.45,"longitude":30.52,'
        b'"timestamp":"2026-09-16T12:00:00Z"}',
        max_items=10,
    )

    assert seq is None
    assert items == [
        LocationReport(device_id="dev-1", latitude=50.45, longitude=30.52, timestamp=NOW)
    ]


def test_parses_array_and_envelope() -> None:
    array = b'[{"device_id":"a","latitude":1,"longitude":2,"timestamp":1789560000}]'
    envelope = (
        b'{"seq":7,"items":[{"device_id":"a","latitude":1,"longitude":2,"timestamp":1789560000}]}'
    )

    seq_array, items_array = parse_ingest_payload(array, max_items=10)
    seq_envelope, items_envelope = parse_ingest_payload(envelope, max_items=10)

    assert (seq_array, len(items_array)) == (None, 1)
    assert (seq_envelope, len(items_envelope)) == (7, 1)
    assert items_array == items_envelope


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ('"2026-09-16T12:00:00Z"', NOW),
        ('"2026-09-16T15:00:00+03:00"', NOW),
        ('"2026-09-16T12:00:00"', NOW),  # naive is read as UTC
        ("1789560000", NOW),  # epoch seconds
        ("1789560000000", NOW),  # epoch milliseconds
    ],
)
def test_accepts_common_timestamp_formats(timestamp: str, expected: datetime) -> None:
    raw = (
        b'{"device_id":"dev-1","latitude":50.0,"longitude":30.0,"timestamp":'
        + timestamp.encode()
        + b"}"
    )

    _, items = parse_ingest_payload(raw, max_items=10)

    assert items[0].timestamp == expected


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"   ",
        b"not json",
        b'{"device_id":"dev 1","latitude":1,"longitude":2,"timestamp":1789560000}',
        b'{"device_id":"dev-1","latitude":91,"longitude":2,"timestamp":1789560000}',
        b'{"device_id":"dev-1","latitude":1,"longitude":181,"timestamp":1789560000}',
        b'{"device_id":"dev-1","latitude":NaN,"longitude":2,"timestamp":1789560000}',
        b'{"device_id":"dev-1","latitude":1,"longitude":2}',
        b'{"device_id":"dev-1","latitude":1,"longitude":2,"timestamp":1789560000,"extra":1}',
        b'{"items":[]}',
    ],
)
def test_rejects_invalid_payloads(raw: bytes) -> None:
    with pytest.raises((ValidationError, ValueError)):
        parse_ingest_payload(raw, max_items=10)


def test_rejects_batches_over_the_limit() -> None:
    report = b'{"device_id":"a","latitude":1,"longitude":2,"timestamp":1789560000}'
    raw = b"[" + b",".join([report] * 3) + b"]"

    with pytest.raises(ValueError, match="limit is 2"):
        parse_ingest_payload(raw, max_items=2)


def _report(offset: timedelta) -> LocationReport:
    return LocationReport(device_id="dev-1", latitude=1.0, longitude=2.0, timestamp=NOW + offset)


def test_window_accepts_reports_inside_the_retention_window() -> None:
    check_report_window(
        [_report(timedelta(0)), _report(timedelta(days=-6)), _report(timedelta(minutes=4))],
        now=NOW,
        max_age=timedelta(days=7),
        max_future=timedelta(minutes=5),
    )


@pytest.mark.parametrize(
    ("offset", "reason"),
    [(timedelta(days=-8), "older"), (timedelta(minutes=6), "future")],
)
def test_window_rejects_reports_outside_it(offset: timedelta, reason: str) -> None:
    with pytest.raises(ReportWindowError) as excinfo:
        check_report_window(
            [_report(timedelta(0)), _report(offset)],
            now=NOW,
            max_age=timedelta(days=7),
            max_future=timedelta(minutes=5),
        )

    assert excinfo.value.index == 1
    assert reason in excinfo.value.reason
