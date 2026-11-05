# Pipeline image: Python, Java (Spark runs on the JVM), and the locked dependencies.
FROM python:3.13-slim-bookworm

# Java 17 for Spark, the same major version as on the dev machine.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# uv, pinned to the version used in pre-commit and CI.
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Dependencies first, so code changes don't reinstall them. The cache mount keeps uv's
# download cache on the build machine, out of the image.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY src ./src
COPY db ./db
COPY tests/sql ./tests/sql
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

# Sedona's Spark jars, downloaded at build time so a run needs no Maven access.
ENV SEDONA_JARS_DIR=/opt/sedona-jars
RUN python -c "from quebec_train_to_trail.ingest.osm import sedona_jars; sedona_jars()"
CMD ["python", "-m", "quebec_train_to_trail.pipeline"]
