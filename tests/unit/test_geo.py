import pytest

from geotrack.geo import BBox


def test_parse_valid_bbox() -> None:
    bbox = BBox.parse("30.2,50.3,30.85,50.6")

    assert bbox == BBox(west=30.2, south=50.3, east=30.85, north=50.6)
    assert not bbox.crosses_antimeridian
    assert bbox.parts() == (bbox,)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "1,2,3",
        "a,b,c,d",
        "30,50,31,49",  # south above north
        "30,-91,31,50",
        "30,50,31,91",
        "-181,50,31,51",
        "30,50,181,51",
        "nan,50,31,51",
        "inf,50,31,51",
    ],
)
def test_parse_rejects_invalid_bbox(raw: str) -> None:
    with pytest.raises(ValueError, match="bbox"):
        BBox.parse(raw)


def test_antimeridian_bbox_is_split_into_two_parts() -> None:
    bbox = BBox(west=170.0, south=-10.0, east=-170.0, north=10.0)

    assert bbox.crosses_antimeridian
    assert bbox.parts() == (
        BBox(west=170.0, south=-10.0, east=180.0, north=10.0),
        BBox(west=-180.0, south=-10.0, east=-170.0, north=10.0),
    )


def test_contains_handles_antimeridian() -> None:
    bbox = BBox(west=170.0, south=-10.0, east=-170.0, north=10.0)

    assert bbox.contains(lat=0.0, lon=175.0)
    assert bbox.contains(lat=0.0, lon=-175.0)
    assert not bbox.contains(lat=0.0, lon=0.0)
    assert not bbox.contains(lat=20.0, lon=175.0)
