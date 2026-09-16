import orjson
import pytest

from geotrack.messaging.codec import (
    CodecError,
    LocationRecord,
    decode_positions,
    decode_record,
    encode_positions,
    encode_record,
)

RECORD = LocationRecord(
    device_id="dev-00042", lat=50.4501, lon=30.5234, reported_ms=1_789_000_000_000, received_ms=1
)


def test_record_round_trip() -> None:
    assert decode_record(encode_record(RECORD)) == RECORD
    assert RECORD.position_item == ("dev-00042", 50.4501, 30.5234, 1_789_000_000_000)


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        orjson.dumps({"device_id": "dev"}),
        orjson.dumps(["dev", 50.0, 30.0, 1]),
        orjson.dumps(["", 50.0, 30.0, 1, 2]),
        orjson.dumps(["dev", "50", 30.0, 1, 2]),
        orjson.dumps(["dev", 91.0, 30.0, 1, 2]),
        orjson.dumps(["dev", 50.0, 181.0, 1, 2]),
        orjson.dumps(["dev", 50.0, 30.0, 1.5, 2]),
    ],
)
def test_decode_record_rejects_malformed_payloads(payload: bytes) -> None:
    with pytest.raises(CodecError):
        decode_record(payload)


@pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
def test_decode_record_rejects_non_finite_coordinates(literal: bytes) -> None:
    with pytest.raises(CodecError):
        decode_record(b'["dev",' + literal + b",30.0,1,2]")


def test_positions_round_trip() -> None:
    items = [("dev-1", 50.0, 30.0, 1), ("dev-2", -33.9, 151.2, 2)]

    assert decode_positions(encode_positions(items)) == items


@pytest.mark.parametrize(
    "payload",
    [b"{}", orjson.dumps({"items": "no"}), orjson.dumps({"items": [["dev", 1.0, 2.0]]})],
)
def test_decode_positions_rejects_malformed_payloads(payload: bytes) -> None:
    with pytest.raises(CodecError):
        decode_positions(payload)
