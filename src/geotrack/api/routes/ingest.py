"""HTTP ingestion.

The order of the checks is the point of this module: the gate is read before the body
is touched, so a service that is already behind spends nothing on parsing work it is
about to throw away.
"""

from datetime import timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Security, status
from fastapi.security import APIKeyHeader
from pydantic import ValidationError
from redis.exceptions import RedisError
from starlette.requests import ClientDisconnect

from geotrack.api.deps import Resources, verify_ingest_key
from geotrack.api.problems import ProblemError
from geotrack.clock import utc_now
from geotrack.ingest.service import BackpressureError
from geotrack.observability.metrics import ingest_rejected_total
from geotrack.schemas.ingest import (
    BatchTooLargeError,
    IngestAccepted,
    LocationReport,
    ReportWindowError,
    check_report_window,
    max_payload_bytes,
    parse_ingest_payload,
)
from geotrack.settings import Settings

# Declared as a security scheme rather than a plain header so the documentation offers
# the field and the reader can see at a glance that devices authenticate differently
# from dashboards.
ingest_key_scheme = APIKeyHeader(
    name="X-Ingest-Key",
    auto_error=False,
    scheme_name="Device ingestion key",
    description="Shared key configured as INGEST_API_KEY",
)


async def require_ingest_key(
    resources: Resources,
    provided: Annotated[str | None, Security(ingest_key_scheme)] = None,
) -> None:
    """Device authentication, counted so a misconfigured fleet is visible on a dashboard."""
    try:
        verify_ingest_key(resources, provided)
    except ProblemError:
        ingest_rejected_total.labels("http", "auth").inc()
        raise


router = APIRouter(
    prefix="/api/v1/ingest", tags=["ingest"], dependencies=[Depends(require_ingest_key)]
)


def _report_schema() -> dict[str, Any]:
    schema: dict[str, Any] = LocationReport.model_json_schema()
    # The model accepts epoch numbers as well as ISO-8601; say so, instead of letting the
    # generated schema claim strings only.
    schema["properties"]["timestamp"] = {
        "description": "ISO-8601 timestamp, epoch seconds, or epoch milliseconds",
        "anyOf": [{"type": "string", "format": "date-time"}, {"type": "number"}],
    }
    return schema


def _request_body() -> dict[str, Any]:
    item = _report_schema()
    array = {"type": "array", "minItems": 1, "items": item}
    envelope = {
        "type": "object",
        "required": ["items"],
        "properties": {
            "seq": {"type": "integer", "description": "Echoed back over the websocket transport"},
            "items": array,
        },
    }
    return {
        "required": True,
        "content": {"application/json": {"schema": {"oneOf": [item, array, envelope]}}},
    }


def _validation_problem(detail: str, *, errors: list[dict[str, Any]] | None = None) -> ProblemError:
    return ProblemError(
        422,
        "Request validation failed",
        code="validation_error",
        detail=detail,
        extra={"errors": errors} if errors else None,
    )


def readable_errors(exc: ValidationError) -> list[dict[str, Any]]:
    """Pydantic's raw errors can carry exception objects in ``ctx``; keep only what
    always serialises, so an error response can never itself fail to render."""
    return [
        {"loc": [str(part) for part in error["loc"]], "msg": error["msg"], "type": error["type"]}
        for error in exc.errors(include_url=False)
    ]


def describe_errors(exc: ValidationError) -> str:
    """One line a device can log: ``items.0.latitude: Input should be less than 90``."""
    parts = []
    for error in readable_errors(exc):
        location = ".".join(error["loc"])
        parts.append(f"{location}: {error['msg']}" if location else error["msg"])
    return "; ".join(parts)


def throttled_problem(retry_after_ms: int) -> ProblemError:
    return ProblemError(
        503,
        "Service unavailable",
        code="ingest_throttled",
        detail="The ingestion backlog is above its high watermark. Retry shortly.",
        headers={"Retry-After": str(max(1, round(retry_after_ms / 1000)))},
        extra={"retry_after_ms": retry_after_ms},
    )


def _too_large(ceiling: int, max_batch: int) -> ProblemError:
    return ProblemError(
        413,
        "Payload too large",
        code="payload_too_large",
        detail=(
            f"Bodies above {ceiling} bytes are refused; a request carries at most "
            f"{max_batch} reports."
        ),
    )


def _check_declared_length(request: Request, *, ceiling: int, max_batch: int) -> None:
    declared = request.headers.get("content-length")
    if declared is None:
        return
    try:
        length = int(declared)
    except ValueError as exc:
        raise ProblemError(
            400, "Bad request", code="bad_request", detail="Content-Length is not a number."
        ) from exc
    if length > ceiling:
        raise _too_large(ceiling, max_batch)


async def read_bounded_body(request: Request, *, max_batch: int) -> bytes:
    """Read the body, giving up the moment it passes the ceiling.

    The ceiling follows the batch limit rather than being a round number of its own: a
    body too large to hold a legal batch however it is formatted costs nothing to refuse
    here, and refusing it keeps the work a rejected request can demand within a small
    multiple of the work an accepted one costs.

    A chunked request declares no length, so the header check above cannot see it coming;
    consuming the stream and measuring afterwards would mean buffering whatever the
    client felt like sending.
    """
    ceiling = max_payload_bytes(max_batch)
    _check_declared_length(request, ceiling=ceiling, max_batch=max_batch)
    body = bytearray()
    try:
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > ceiling:
                raise _too_large(ceiling, max_batch)
    except ClientDisconnect as exc:
        # A fleet that gives up on a slow request leaves half-sent bodies behind. There
        # is nobody left to answer, so this is a normal outcome of load, not a fault:
        # answering it as a 408 keeps it out of the 5xx count and out of the error log.
        raise ProblemError(
            408,
            "Request incomplete",
            code="client_disconnected",
            detail="The connection closed before the whole body arrived.",
        ) from exc
    return bytes(body)


def report_window(settings: Settings) -> tuple[timedelta, timedelta]:
    """How far back and forward a timestamp may sit.

    The lower bound is the history retention window, because ``location_history`` only
    has partitions for those days; the upper bound catches devices with a broken clock.
    """
    return (
        timedelta(days=settings.history_retention_days),
        timedelta(seconds=settings.ingest_max_future_skew_s),
    )


@router.post(
    "/locations",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IngestAccepted,
    openapi_extra={"requestBody": _request_body()},
    responses={
        401: {"description": "Missing or wrong X-Ingest-Key"},
        413: {"description": "Body too large to hold a batch within the limit"},
        422: {"description": "Malformed report, oversized batch or out-of-window timestamp"},
        503: {"description": "Ingestion is throttled; honour Retry-After"},
    },
    summary="Submit device locations",
)
async def ingest_locations(request: Request, resources: Resources) -> IngestAccepted:
    """Accept one report, an array of reports, or ``{"seq": n, "items": [...]}``.

    A 202 means the batch is durably queued, not that it has reached PostGIS. The
    processors apply it within milliseconds, and the distance between the two is
    measured rather than assumed: see ``geotrack_processor_end_to_end_seconds``.
    """
    settings = resources.settings
    service = resources.ingest

    if resources.backlog.throttled:
        # Nothing is counted on ``ingest_rejected_total`` here: that counter is in
        # reports, and the body this refusal sheds is deliberately never read, so the
        # number behind it is unknowable. Charging it one per request would undercount a
        # shed fleet a thousandfold exactly when the figure matters. The request-level
        # view is geotrack_http_requests_total with status 503, next to the
        # geotrack_ingest_throttled gauge.
        raise throttled_problem(resources.backlog.retry_after_ms)

    raw = await read_bounded_body(request, max_batch=settings.ingest_max_batch)

    try:
        _, reports = parse_ingest_payload(raw, max_items=settings.ingest_max_batch)
    except BatchTooLargeError as exc:
        service.count_rejected(exc.offered, transport="http", reason="validation")
        raise _validation_problem(str(exc)) from exc
    except ValidationError as exc:
        service.count_rejected(1, transport="http", reason="validation")
        raise _validation_problem(
            "One or more reports did not pass validation.", errors=readable_errors(exc)
        ) from exc
    except ValueError as exc:
        service.count_rejected(1, transport="http", reason="validation")
        raise _validation_problem(str(exc)) from exc

    max_age, max_future = report_window(settings)
    try:
        check_report_window(reports, now=utc_now(), max_age=max_age, max_future=max_future)
    except ReportWindowError as exc:
        service.count_rejected(len(reports), transport="http", reason="window")
        raise _validation_problem(str(exc)) from exc

    try:
        accepted = await service.submit(reports, transport="http")
    except BackpressureError as exc:
        raise throttled_problem(exc.retry_after_ms) from exc
    except RedisError as exc:
        raise ProblemError(
            503,
            "Service unavailable",
            code="ingest_unavailable",
            detail="The ingestion queue is not reachable. Retry shortly.",
            headers={"Retry-After": "1"},
        ) from exc
    return IngestAccepted(accepted=accepted)
