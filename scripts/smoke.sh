#!/usr/bin/env bash
# Run the end-to-end smoke check against the stack started by `make up`.
# Reads the ingest key and the published port from .env.
#
# Three variables tune the check itself. They are not part of the stack's configuration,
# so they are deliberately absent from .env.example; export them here or in the shell:
#   SMOKE_BASE_URL         where to aim (default http://127.0.0.1:$HTTP_PORT)
#   SMOKE_READY_TIMEOUT_S  how long to wait for /health/ready (default 60)
#   SMOKE_ALERT_TIMEOUT_S  how long to wait for the enter alert (default 30)
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
    echo "no .env found; run scripts/bootstrap-env.sh or make up first" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

export SMOKE_BASE_URL="${SMOKE_BASE_URL:-http://127.0.0.1:${HTTP_PORT:-8080}}"

echo "[smoke] target ${SMOKE_BASE_URL}"
exec uv run --quiet python scripts/smoke.py
