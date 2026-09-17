import pytest

from geotrack.geo import BBox
from geotrack.realtime.grid import CellRect, cell_of, covers, rects_for

SIZE = 0.05


def test_neighbouring_points_share_a_cell() -> None:
    assert cell_of(50.4501, 30.5201, SIZE) == cell_of(50.4502, 30.5202, SIZE)
    assert cell_of(50.45, 30.52, SIZE) != cell_of(50.45, 30.62, SIZE)


def test_longitude_wraps_around_the_antimeridian() -> None:
    assert cell_of(0.0, 180.0, SIZE) == cell_of(0.0, -180.0, SIZE)
    assert cell_of(0.0, 185.0, SIZE) == cell_of(0.0, -175.0, SIZE)


def test_latitude_is_clamped_to_the_poles() -> None:
    assert cell_of(95.0, 10.0, SIZE) == cell_of(90.0, 10.0, SIZE)
    assert cell_of(-95.0, 10.0, SIZE) == cell_of(-90.0, 10.0, SIZE)


@pytest.mark.parametrize(
    ("lat", "lon"),
    [(50.3, 30.2), (50.45, 30.52), (50.6, 30.8), (50.59, 30.79)],
)
def test_viewport_covers_every_point_inside_it(lat: float, lon: float) -> None:
    rects = rects_for(BBox(west=30.2, south=50.3, east=30.8, north=50.6), SIZE)

    assert covers(rects, cell_of(lat, lon, SIZE))


@pytest.mark.parametrize(
    ("lat", "lon"),
    [(50.45, 31.5), (49.0, 30.5), (-33.9, 151.2)],
)
def test_viewport_ignores_cells_outside_it(lat: float, lon: float) -> None:
    rects = rects_for(BBox(west=30.2, south=50.3, east=30.8, north=50.6), SIZE)

    assert not covers(rects, cell_of(lat, lon, SIZE))


def test_antimeridian_viewport_covers_both_sides_only() -> None:
    rects = rects_for(BBox(west=179.0, south=-5.0, east=-179.0, north=5.0), SIZE)

    assert len(rects) == 2
    assert covers(rects, cell_of(0.0, 179.5, SIZE))
    assert covers(rects, cell_of(0.0, -179.5, SIZE))
    assert not covers(rects, cell_of(0.0, 178.5, SIZE))
    assert not covers(rects, cell_of(0.0, 0.0, SIZE))


def test_viewport_ending_on_the_antimeridian_stays_east() -> None:
    rects = rects_for(BBox(west=170.0, south=-5.0, east=180.0, north=5.0), SIZE)

    assert covers(rects, cell_of(0.0, 179.99, SIZE))
    assert not covers(rects, cell_of(0.0, -179.99, SIZE))
    assert not covers(rects, cell_of(0.0, 0.0, SIZE))


@pytest.mark.parametrize(
    ("lat", "lon"),
    [(0.0, 0.0), (89.9, 179.9), (-89.9, -179.9), (90.0, 180.0), (-90.0, -180.0)],
)
def test_world_viewport_covers_every_cell(lat: float, lon: float) -> None:
    rects = rects_for(BBox(west=-180.0, south=-90.0, east=180.0, north=90.0), SIZE)

    assert covers(rects, cell_of(lat, lon, SIZE))


def test_a_client_without_a_viewport_sees_nothing() -> None:
    assert not covers((), (0, 0))


def test_rect_bounds_follow_the_bounding_box() -> None:
    (rect,) = rects_for(BBox(west=-10.0, south=-10.0, east=10.0, north=10.0), 1.0)

    assert rect == CellRect(min_x=-10, min_y=-10, max_x=10, max_y=10)
    assert rect.contains((0, 0))
    assert not rect.contains((11, 0))
