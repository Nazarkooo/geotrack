from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

Latitude = Annotated[float, Field(ge=-90.0, le=90.0, allow_inf_nan=False)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0, allow_inf_nan=False)]
DeviceId = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._:-]+$")]
Username = Annotated[str, Field(min_length=3, max_length=32, pattern=r"^[A-Za-z0-9_.-]+$")]
ZoneColor = Annotated[str, Field(pattern=r"^#[0-9a-fA-F]{6}$")]

# Palette used when a zone is created without an explicit colour; readable on a dark map.
ZONE_PALETTE = (
    "#3fb1ff",
    "#ff4d4f",
    "#38d39f",
    "#ffb020",
    "#b980ff",
    "#ff7ab6",
    "#4ddbd0",
    "#f2f26b",
)


def default_zone_color(index: int) -> str:
    return ZONE_PALETTE[index % len(ZONE_PALETTE)]


class Schema(BaseModel):
    """Base for response models: immutable and safe to build from ORM rows or mappings."""

    model_config = ConfigDict(frozen=True, from_attributes=True, extra="forbid")


class Payload(BaseModel):
    """Base for request bodies: unknown fields are an error, not silently ignored."""

    model_config = ConfigDict(extra="forbid")
