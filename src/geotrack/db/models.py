"""ORM mapping of the schema created by migration ``0001_initial``.

The migration is the source of truth (it carries generated columns, partitions and
GiST indexes written by hand); these classes map the same tables for typed access.
"""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from geoalchemy2 import Geography, WKBElement
from sqlalchemy import (
    BigInteger,
    Boolean,
    Computed,
    DateTime,
    Double,
    Enum,
    ForeignKey,
    Identity,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Mirrors the generated column in the migration. The 2% + 1 m padding makes the
# buffered 32-gon circumscribe the true geodesic circle, so the GiST prefilter on
# this column never drops a point that ST_DWithin would accept.
SEARCH_AREA_EXPRESSION = "ST_Buffer(center, radius_m * 1.02 + 1.0, 'quad_segs=8')"


class AlertKind(StrEnum):
    ENTER = "enter"
    EXIT = "exit"
    DWELL = "dwell"


def _geography(kind: str) -> Geography:
    # Spatial indexes are created explicitly in the migration, not implicitly here.
    return Geography(geometry_type=kind, srid=4326, spatial_index=False)


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_N_name)s",
            "uq": "uq_%(table_name)s_%(column_0_N_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class User(Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(Text, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Geozone(Base):
    __tablename__ = "geozones"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(Text)
    color: Mapped[str] = mapped_column(Text)
    center: Mapped[WKBElement] = mapped_column(_geography("POINT"))
    radius_m: Mapped[float] = mapped_column(Double)
    search_area: Mapped[WKBElement] = mapped_column(
        _geography("POLYGON"), Computed(SEARCH_AREA_EXPRESSION, persisted=True)
    )
    alert_on_enter: Mapped[bool] = mapped_column(Boolean, server_default="true")
    alert_on_exit: Mapped[bool] = mapped_column(Boolean, server_default="true")
    dwell_alert_interval_s: Mapped[int | None] = mapped_column(Integer)
    version: Mapped[int] = mapped_column(Integer, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DevicePosition(Base):
    __tablename__ = "device_positions"

    device_id: Mapped[str] = mapped_column(Text, primary_key=True)
    position: Mapped[WKBElement] = mapped_column(_geography("POINT"))
    reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ZonePresence(Base):
    __tablename__ = "zone_presence"
    __table_args__ = (PrimaryKeyConstraint("zone_id", "device_id"),)

    zone_id: Mapped[UUID] = mapped_column(ForeignKey("geozones.id", ondelete="CASCADE"))
    device_id: Mapped[str] = mapped_column(Text)
    entered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_alert_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    zone_id: Mapped[UUID | None] = mapped_column(ForeignKey("geozones.id", ondelete="SET NULL"))
    zone_name: Mapped[str] = mapped_column(Text)
    device_id: Mapped[str] = mapped_column(Text)
    kind: Mapped[AlertKind] = mapped_column(
        Enum(
            AlertKind,
            name="alert_kind",
            values_callable=lambda kinds: [k.value for k in kinds],
            create_type=False,
        )
    )
    position: Mapped[WKBElement] = mapped_column(_geography("POINT"))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class LocationHistory(Base):
    __tablename__ = "location_history"
    __table_args__ = (
        PrimaryKeyConstraint("device_id", "reported_at"),
        {"postgresql_partition_by": "RANGE (reported_at)"},
    )

    device_id: Mapped[str] = mapped_column(Text)
    position: Mapped[WKBElement] = mapped_column(_geography("POINT"))
    reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
