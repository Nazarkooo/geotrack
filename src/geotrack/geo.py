"""Geographic value types shared by the API, the gateway and the processor.

Distance math deliberately does not live here: every metric computation runs in
PostGIS on geography columns so that results match the database exactly.
"""

import math
from dataclasses import dataclass
from typing import Self

MIN_RADIUS_M = 10.0
MAX_RADIUS_M = 50_000.0


@dataclass(frozen=True, slots=True)
class BBox:
    """A WGS84 bounding box. ``west > east`` means it crosses the antimeridian."""

    west: float
    south: float
    east: float
    north: float

    def __post_init__(self) -> None:
        values = (self.west, self.south, self.east, self.north)
        if not all(math.isfinite(v) for v in values):
            raise ValueError("bbox values must be finite numbers")
        if not (-180.0 <= self.west <= 180.0 and -180.0 <= self.east <= 180.0):
            raise ValueError("bbox longitudes must be within [-180, 180]")
        if not (-90.0 <= self.south <= 90.0 and -90.0 <= self.north <= 90.0):
            raise ValueError("bbox latitudes must be within [-90, 90]")
        if self.south > self.north:
            raise ValueError("bbox south must not be greater than north")

    @classmethod
    def parse(cls, raw: str) -> Self:
        """Parse ``west,south,east,north`` (the order used by GeoJSON and most map SDKs)."""
        parts = raw.split(",")
        if len(parts) != 4:
            raise ValueError("bbox must have four comma-separated numbers: west,south,east,north")
        try:
            west, south, east, north = (float(p) for p in parts)
        except ValueError as exc:
            raise ValueError("bbox values must be numbers") from exc
        return cls(west=west, south=south, east=east, north=north)

    @property
    def crosses_antimeridian(self) -> bool:
        return self.west > self.east

    def parts(self) -> tuple[BBox, ...]:
        """Split into boxes that never cross the antimeridian."""
        if not self.crosses_antimeridian:
            return (self,)
        return (
            BBox(west=self.west, south=self.south, east=180.0, north=self.north),
            BBox(west=-180.0, south=self.south, east=self.east, north=self.north),
        )

    def contains(self, *, lat: float, lon: float) -> bool:
        if not self.south <= lat <= self.north:
            return False
        if self.crosses_antimeridian:
            return lon >= self.west or lon <= self.east
        return self.west <= lon <= self.east
