# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.12.15-python3.14-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependencies resolve from the lock file alone, so this layer is reused whenever
# only application code changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=README.md,target=README.md \
    uv sync --locked --no-install-project --no-editable

COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY src ./src
COPY migrations ./migrations

RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-editable


FROM python:3.14.7-slim-trixie AS runtime

RUN groupadd --system --gid 10001 geotrack \
    && useradd --system --uid 10001 --gid geotrack --home-dir /app --no-create-home geotrack

WORKDIR /app

COPY --from=builder --chown=geotrack:geotrack /app/.venv /app/.venv
COPY --chown=geotrack:geotrack alembic.ini ./
COPY --chown=geotrack:geotrack migrations ./migrations

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER geotrack
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD ["python", "-c", "import sys,urllib.request as u; sys.exit(0 if u.urlopen('http://127.0.0.1:8000/health/live', timeout=2).status == 200 else 1)"]

CMD ["uvicorn", "geotrack.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", \
     "--loop", "uvloop", "--http", "httptools", "--ws", "websockets-sansio", \
     "--ws-per-message-deflate", "false", "--ws-max-size", "1048576", \
     "--proxy-headers", "--forwarded-allow-ips", "*", "--no-server-header", \
     "--timeout-graceful-shutdown", "15"]
