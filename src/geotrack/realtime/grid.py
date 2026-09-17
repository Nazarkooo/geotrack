"""Cell maths for viewport filtering.

Device positions are bucketed into fixed-degree cells so that one tick's updates
are serialised once per cell and then routed to every client whose viewport touches
that cell. A viewport is kept as integer rectangles rather than a set of cells: a
world-wide view at 0.05 degrees would otherwise materialise 25 million tuples.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass

from geotrack.geo import BBox

type Cell = tuple[int, int]


@dataclass(frozen=True, slots=True)
class CellRect:
    """An inclusive range of cell indices; a viewport is one or two of these."""

    min_x: int
    min_y: int
    max_x: int
    max_y: int

    def contains(self, cell: Cell) -> bool:
        x, y = cell
        return self.min_x <= x <= self.max_x and self.min_y <= y <= self.max_y


def cell_of(lat: float, lon: float, size_deg: float) -> Cell:
    """The cell a position belongs to, for any coordinate a device can report."""
    return (math.floor(_wrap_lon(lon) / size_deg), _row_of(lat, size_deg))


def rects_for(bbox: BBox, size_deg: float) -> tuple[CellRect, ...]:
    """Cell rectangles covering a bounding box; two of them when it crosses 180."""
    return tuple(_rect(part, size_deg) for part in bbox.parts())


def covers(rects: Sequence[CellRect], cell: Cell) -> bool:
    return any(rect.contains(cell) for rect in rects)


def _rect(part: BBox, size_deg: float) -> CellRect:
    return CellRect(
        min_x=_column_of(part.west, size_deg),
        min_y=_row_of(part.south, size_deg),
        max_x=_column_of(part.east, size_deg),
        max_y=_row_of(part.north, size_deg),
    )


def _wrap_lon(lon: float) -> float:
    return ((lon + 180.0) % 360.0) - 180.0


def _row_of(lat: float, size_deg: float) -> int:
    return math.floor(min(max(lat, -90.0), 90.0) / size_deg)


def _column_of(lon: float, size_deg: float) -> int:
    """The column of a viewport edge, which is not quite the column of a position.

    A position at 180 wraps to -180, but an edge at 180 means "as far east as it
    goes": wrapping it would turn a viewport reaching the antimeridian into one
    spanning the whole world.
    """
    if lon >= 180.0:
        return math.floor(math.nextafter(180.0, 0.0) / size_deg)
    return math.floor(_wrap_lon(lon) / size_deg)
