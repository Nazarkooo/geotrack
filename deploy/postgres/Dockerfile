# syntax=docker/dockerfile:1
# Thin PostGIS layer over the official multi-arch postgres image. The upstream
# postgis/postgis image is amd64-only, which would run under emulation on arm64.
FROM postgres:18.6-trixie

ARG POSTGIS_VERSION=3.6.4+dfsg-2.pgdg13+1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        "postgresql-18-postgis-3=${POSTGIS_VERSION}" \
        "postgresql-18-postgis-3-scripts=${POSTGIS_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

COPY initdb/ /docker-entrypoint-initdb.d/
