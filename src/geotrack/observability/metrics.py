"""Prometheus metrics for both services.

Defined in one module so that dashboards, alerts and tests have a single list of
names to rely on. Metrics an individual process never touches simply stay at zero.
"""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine

SECONDS_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
BATCH_SIZE_BUCKETS = (1, 5, 10, 50, 100, 250, 500, 1_000, 2_500, 5_000)

# --- ingestion -----------------------------------------------------------------
ingest_reports_total = Counter(
    "geotrack_ingest_reports_total", "Accepted location reports", ["transport"]
)
ingest_rejected_total = Counter(
    "geotrack_ingest_rejected_total", "Rejected location reports", ["transport", "reason"]
)
ingest_shed_frames_total = Counter(
    "geotrack_ingest_shed_frames_total",
    "Websocket frames dropped unparsed while shedding load (frames, not reports)",
    ["transport"],
)
ingest_backlog = Gauge("geotrack_ingest_backlog", "Unprocessed entries across ingest shards")
ingest_throttled = Gauge("geotrack_ingest_throttled", "1 while ingestion is shedding load")

# --- websocket gateway ---------------------------------------------------------
ws_connections = Gauge("geotrack_ws_connections", "Open websocket connections", ["kind"])
ws_messages_sent_total = Counter(
    "geotrack_ws_messages_sent_total", "Frames sent to clients", ["type"]
)
ws_position_frames_dropped_total = Counter(
    "geotrack_ws_position_frames_dropped_total",
    "Position frames dropped because the client was behind (replaced by a snapshot)",
)
ws_slow_consumer_disconnects_total = Counter(
    "geotrack_ws_slow_consumer_disconnects_total",
    "Connections closed because their control queue overflowed or a send timed out",
)
ws_send_seconds = Histogram(
    "geotrack_ws_send_seconds", "Time spent writing one frame", buckets=SECONDS_BUCKETS
)
hub_devices = Gauge("geotrack_hub_devices", "Devices tracked in the in-memory position hub")
hub_updates_total = Counter("geotrack_hub_updates_total", "Position updates applied to the hub")

# --- http ----------------------------------------------------------------------
http_requests_total = Counter(
    "geotrack_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
http_request_duration_seconds = Histogram(
    "geotrack_http_request_duration_seconds",
    "HTTP request duration",
    ["method", "route"],
    buckets=SECONDS_BUCKETS,
)

# --- processor -----------------------------------------------------------------
processor_batches_total = Counter("geotrack_processor_batches_total", "Batches committed")
processor_reports_total = Counter("geotrack_processor_reports_total", "Reports applied")
processor_stale_reports_total = Counter(
    "geotrack_processor_stale_reports_total", "Reports ignored because a newer one was stored"
)
processor_batch_size = Histogram(
    "geotrack_processor_batch_size", "Reports per batch", buckets=BATCH_SIZE_BUCKETS
)
processor_batch_seconds = Histogram(
    "geotrack_processor_batch_seconds", "Time to apply one batch", buckets=SECONDS_BUCKETS
)
processor_end_to_end_seconds = Histogram(
    "geotrack_processor_end_to_end_seconds",
    "Delay between accepting a report and committing it",
    buckets=SECONDS_BUCKETS,
)
processor_alerts_total = Counter("geotrack_processor_alerts_total", "Alerts emitted", ["kind"])
processor_retries_total = Counter("geotrack_processor_retries_total", "Batch retries", ["sqlstate"])
processor_dead_letters_total = Counter(
    "geotrack_processor_dead_letters_total", "Entries moved to the dead-letter stream"
)
processor_shards_owned = Gauge("geotrack_processor_shards_owned", "Shards leased by this replica")
processor_shard_backlog = Gauge(
    "geotrack_processor_shard_backlog", "Unprocessed entries per shard", ["shard"]
)

# --- runtime -------------------------------------------------------------------
event_loop_lag_seconds = Histogram(
    "geotrack_event_loop_lag_seconds",
    "How late the event loop wakes a timer",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
db_pool_checked_out = Gauge("geotrack_db_pool_checked_out", "Connections currently checked out")
db_pool_size = Gauge("geotrack_db_pool_size", "Configured connection pool size")


def render_metrics() -> tuple[bytes, str]:
    """Current metrics in the Prometheus text format."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


def bind_pool_metrics(engine: AsyncEngine, *, pool_size: int) -> None:
    """Track pool usage so that exhaustion shows up on a dashboard before it hurts."""
    db_pool_size.set(pool_size)
    sync_engine = engine.sync_engine

    @event.listens_for(sync_engine, "checkout")
    def _on_checkout(*_: object) -> None:
        db_pool_checked_out.inc()

    @event.listens_for(sync_engine, "checkin")
    def _on_checkin(*_: object) -> None:
        db_pool_checked_out.dec()
