# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.14.7
ARG UV_VERSION=0.12.19

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# ---- Builder: resolve the locked dependencies into a virtualenv ----
FROM python:${PYTHON_VERSION}-slim-trixie AS builder

COPY --from=uv /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

# Dependencies first (cached until pyproject.toml/uv.lock change).
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --no-install-project --no-editable

# Then the project itself, plus pydantic for the bundled examples
# (pinned to the version locked in the dev group).
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable && \
    uv pip install --python /opt/venv/bin/python "pydantic==2.13.5"

# ---- Runtime: slim image with only the virtualenv and examples ----
FROM python:${PYTHON_VERSION}-slim-trixie

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

# Build-time user/group IDs for non-root runtime (override with --build-arg)
ARG APP_UID=1000
ARG APP_GID=1000

RUN groupadd --gid "${APP_GID}" app && \
    useradd --uid "${APP_UID}" --gid app --create-home --shell /bin/sh app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY examples ./examples

RUN mkdir -p /app/data && chown app:app /app/data

VOLUME ["/app/data"]

USER app

# Default command runs the quotes spider
CMD ["python", "examples/quotes_spider.py"]
