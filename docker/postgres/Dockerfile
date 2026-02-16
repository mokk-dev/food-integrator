# ============================================
# PostgreSQL 14 + TimescaleDB
# ============================================

FROM timescale/timescaledb:2.11.0-pg14

LABEL maintainer="Cardapioweb Integrator"
LABEL description="PostgreSQL 14 with TimescaleDB for delivery analytics"

RUN apk add --no-cache \
    postgresql14-contrib \
    && rm -rf /var/cache/apk/*

COPY postgresql.conf /etc/postgresql/postgresql.conf
