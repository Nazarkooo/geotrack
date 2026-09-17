#!/usr/bin/env bash
# Prove the monitoring half of the stack is actually wired up, not merely running:
# Prometheus has scraped every replica of both tiers, and Grafana has provisioned the
# datasource and the dashboard that points at it.
#
# Expects `docker compose --profile monitoring up -d --wait` to have completed.
# Reads GRAFANA_PORT and the admin credentials from .env.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
    echo "no .env found; run scripts/bootstrap-env.sh or make up-monitoring first" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

COMPOSE=(docker compose --profile monitoring)
GRAFANA="http://127.0.0.1:${GRAFANA_PORT:-3000}"
DEADLINE=$((SECONDS + 120))

# Prometheus is not published: its own container is the only place that can ask it.
prometheus_api() {
    "${COMPOSE[@]}" exec -T prometheus wget -q -O - "http://127.0.0.1:9090$1"
}

echo "[monitoring] waiting for every scrape target to report up"
while :; do
    if targets=$(prometheus_api "/api/v1/targets?state=active" 2>/dev/null); then
        summary=$(
            python3 - "$targets" <<'PY'
import json
import sys
from collections import Counter

targets = json.loads(sys.argv[1])["data"]["activeTargets"]
up = Counter(t["labels"]["job"] for t in targets if t["health"] == "up")
down = [t["scrapeUrl"] for t in targets if t["health"] != "up"]
print(json.dumps({"up": dict(up), "down": down}))
PY
        )
        if python3 -c "
import json, sys
state = json.loads(sys.argv[1])
expected = {'api': 2, 'processor': 2, 'prometheus': 1}
sys.exit(0 if not state['down'] and state['up'] == expected else 1)
" "$summary"; then
            echo "[monitoring] targets: $summary"
            break
        fi
    fi
    if ((SECONDS > DEADLINE)); then
        echo "[monitoring] Prometheus never reached a full set of healthy targets: ${summary:-none}" >&2
        prometheus_api "/api/v1/targets?state=active" >&2 || true
        exit 1
    fi
    sleep 3
done

echo "[monitoring] checking the Grafana datasource and dashboard"
auth=("-u" "${GRAFANA_ADMIN_USER:-admin}:${GRAFANA_ADMIN_PASSWORD}")

uid=$(curl -fsS "${auth[@]}" "$GRAFANA/api/datasources" | python3 -c "
import json, sys
sources = json.load(sys.stdin)
assert len(sources) == 1, sources
print(sources[0]['uid'])
")

# Grafana only answers this once the datasource can actually reach Prometheus.
curl -fsS "${auth[@]}" "$GRAFANA/api/datasources/uid/$uid/health" | python3 -c "
import json, sys
health = json.load(sys.stdin)
assert health['status'] == 'OK', health
print(f\"[monitoring] datasource {health['message']}\")
"

curl -fsS "${auth[@]}" "$GRAFANA/api/search?type=dash-db" | python3 -c "
import json, sys, os
boards = json.load(sys.stdin)
assert boards, 'no dashboard was provisioned'
for board in boards:
    print(f\"[monitoring] dashboard {board['title']} at {board['url']}\")
"

echo "[monitoring] OK"
