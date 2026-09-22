# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# ---

# Debian rather than alpine: Dagster's dependencies ship manylinux wheels only.
FROM python:3.13-slim-bookworm

WORKDIR /app

RUN groupadd -r bridge && useradd -r -g bridge bridge

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
# alembic.ini resolves script_location relative to the working directory, so the
# migrate job's `alembic upgrade head` needs it next to src/.
COPY alembic.ini ./

ENV PATH="/app/.venv/bin:$PATH" \
    DAGSTER_HOME=/app/dagster_home

# /app is root-owned and the process runs as bridge, so DAGSTER_HOME has to be
# a directory that exists and is writable before Dagster starts.
RUN mkdir -p /app/dagster_home && chown bridge:bridge /app/dagster_home

USER bridge

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "geometry_car.definitions"]
