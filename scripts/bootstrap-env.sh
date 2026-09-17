#!/usr/bin/env bash
# Create .env from .env.example, replacing every placeholder secret with a random value.
# Existing files are never overwritten.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
    echo ".env already exists, leaving it untouched"
    exit 0
fi

random_secret() {
    local length="${1:-48}"
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 $((length * 2)) | tr -dc 'A-Za-z0-9' | cut -c "1-${length}"
    else
        # head closing the pipe early would trip pipefail, so relax it here only.
        (set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c "$length")
    fi
}

cp .env.example .env

for var in POSTGRES_PASSWORD REDIS_PASSWORD JWT_SECRET INGEST_API_KEY GRAFANA_ADMIN_PASSWORD; do
    secret=$(random_secret 48)
    # BSD and GNU sed disagree about -i, so rewrite through a temporary file.
    awk -v key="$var" -v value="$secret" \
        'BEGIN { FS = "=" } $1 == key { print key "=" value; next } { print }' .env > .env.tmp
    mv .env.tmp .env
done

chmod 600 .env
echo "wrote .env with freshly generated secrets"
