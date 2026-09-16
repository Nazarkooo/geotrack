"""Device report parsing.

Payloads are validated straight from raw bytes with pydantic-core's JSON parser: at
several thousand reports per second the difference against decoding to Python objects
first and validating afterwards is CPU we cannot spend on the event loop.

The batch limit belongs to the parser rather than to a check on its result. Bound by the
schema, an oversized batch is refused as soon as the array has been read and before a
single report becomes a model; bound only afterwards, the cheapest request to send is
the most expensive one to refuse, which is the wrong way round for a public endpoint.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Annotated, Any

from pydantic import Field, TypeAdapter, ValidationError, create_model, field_validator

from geotrack.schemas.common import DeviceId, Latitude, Longitude, Payload, Schema

# What one report can weigh on the wire: a 64-character device id, two coordinates at
# full precision and a timestamp with an offset come to roughly 190 bytes. The margin on
# top covers formatting; the point is that a body ceiling derived from it can never hold
# many times the batch limit.
MAX_REPORT_BYTES = 256
# Even a service configured to take one report at a time should accept a comfortable body.
MIN_PAYLOAD_BYTES = 8 * 1024


class LocationReport(Payload):
    device_id: DeviceId
    latitude: Latitude
    longitude: Longitude
    # Accepts ISO-8601 (with or without a zone) and epoch seconds/milliseconds.
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def _as_utc(cls, value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class IngestEnvelope(Payload):
    seq: int | None = None
    # Unbounded here on purpose: the batch limit is applied when the payload is parsed,
    # and an empty list is answered by ``parse_ingest_payload`` in one voice for all
    # three payload shapes rather than in pydantic's.
    items: list[LocationReport]


class IngestAccepted(Schema):
    accepted: int


class ReportWindowError(ValueError):
    """A report's timestamp is outside the window the system accepts."""

    def __init__(self, index: int, reason: str) -> None:
        super().__init__(f"items[{index}]: {reason}")
        self.index = index
        self.reason = reason


class BatchTooLargeError(ValueError):
    """The payload holds more reports than one request or frame may carry."""

    def __init__(self, offered: int, limit: int) -> None:
        super().__init__(f"payload contains {offered} reports, the limit is {limit}")
        self.offered = offered
        self.limit = limit


def max_payload_bytes(max_items: int) -> int:
    """The largest body or frame that could still be a batch within the limit.

    Refusing anything bigger by its length alone keeps the work a rejected request costs
    within a small multiple of the work an accepted one costs, which is the only version
    of this endpoint that cannot be used to burn an event loop.
    """
    return max(MIN_PAYLOAD_BYTES, max_items * MAX_REPORT_BYTES)


_REPORT_ADAPTER = TypeAdapter(LocationReport)


@dataclass(frozen=True, slots=True)
class _BatchAdapters:
    items: TypeAdapter[list[LocationReport]]
    envelope: TypeAdapter[IngestEnvelope]


@lru_cache(maxsize=4)
def _batch_adapters(max_items: int) -> _BatchAdapters:
    """Parsers carrying the batch limit, built once per limit a process ever sees."""
    bounded: Any = Annotated[list[LocationReport], Field(max_length=max_items)]
    envelope = create_model("BoundedIngestEnvelope", __base__=IngestEnvelope, items=(bounded, ...))
    return _BatchAdapters(items=TypeAdapter(bounded), envelope=TypeAdapter(envelope))


def _batch_limit_error(exc: ValidationError, limit: int) -> BatchTooLargeError | None:
    """Translate pydantic's length error into one a device can act on."""
    for error in exc.errors(include_url=False):
        if error["type"] == "too_long":
            context: dict[str, Any] = error.get("ctx") or {}
            return BatchTooLargeError(int(context.get("actual_length", limit + 1)), limit)
    return None


def parse_ingest_payload(
    raw: bytes | str, *, max_items: int
) -> tuple[int | None, list[LocationReport]]:
    """Accept a single report, an array of reports, or ``{"seq": n, "items": [...]}``."""
    data = raw.encode() if isinstance(raw, str) else raw
    stripped = data.lstrip()
    if not stripped:
        raise ValueError("empty payload")

    adapters = _batch_adapters(max_items)
    seq: int | None = None
    try:
        if stripped[:1] == b"[":
            items = adapters.items.validate_json(data)
        elif b'"items"' in stripped:
            envelope = adapters.envelope.validate_json(data)
            seq, items = envelope.seq, envelope.items
        else:
            items = [_REPORT_ADAPTER.validate_json(data)]
    except ValidationError as exc:
        if (too_large := _batch_limit_error(exc, max_items)) is not None:
            raise too_large from exc
        raise

    if not items:
        raise ValueError("payload contains no reports")
    return seq, items


def check_report_window(
    reports: list[LocationReport], *, now: datetime, max_age: timedelta, max_future: timedelta
) -> None:
    """Reject timestamps the storage layer cannot hold.

    History is partitioned by day and partitions exist only for the retention window,
    so a report outside it would fail at insert time — deep inside the processor, where
    the device can no longer be told about it.
    """
    oldest = now - max_age
    newest = now + max_future
    for index, report in enumerate(reports):
        if report.timestamp < oldest:
            raise ReportWindowError(index, f"timestamp is older than {max_age}")
        if report.timestamp > newest:
            raise ReportWindowError(index, f"timestamp is more than {max_future} in the future")


__all__ = [
    "MAX_REPORT_BYTES",
    "BatchTooLargeError",
    "IngestAccepted",
    "IngestEnvelope",
    "LocationReport",
    "ReportWindowError",
    "ValidationError",
    "check_report_window",
    "max_payload_bytes",
    "parse_ingest_payload",
]
