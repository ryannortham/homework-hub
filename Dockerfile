# syntax=docker/dockerfile:1.7
#
# Homework Hub — runtime container.
#
# Multi-stage:
#   1. builder: install uv, sync the locked dependency set into a venv.
#   2. runtime: copy the venv + source into a slim Python image, install
#      Chromium for Playwright, and run the daemon (APScheduler + FastAPI
#      /health).
#
# Both Edrolo and Classroom use Playwright. Edrolo only needs cookies at
# runtime (httpx replay), but Classroom blocks third-party OAuth at the
# Workspace level, so we scrape the rendered DOM with headless Chromium
# every sync. That dictates the Chromium install below.

# --------------------------------------------------------------------------- #
# Builder
# --------------------------------------------------------------------------- #
FROM python:3.12-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# uv is small + reproducible; pinned by sha at build time would be stricter,
# but we follow the pattern used elsewhere in the homelab (latest stable).
COPY --from=ghcr.io/astral-sh/uv:0.5.0 /uv /usr/local/bin/uv

WORKDIR /build

# Install deps first (cache-friendly), then copy source and install the
# project itself. ``--no-install-project`` on the first sync so the cache
# layer is invalidated only by lockfile changes.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# --------------------------------------------------------------------------- #
# Runtime
# --------------------------------------------------------------------------- #
FROM python:3.12-slim AS runtime

# Bitwarden CLI is needed at runtime for the BitwardenCLI wrapper.
# `bw` is distributed as a self-contained x86_64 Linux binary; TrueNAS is
# x86_64 so we don't multi-arch this image. (To build on Apple Silicon
# locally for testing, pass `--platform=linux/amd64`.)
ARG BW_VERSION=2024.9.0
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl unzip tini; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL -o /tmp/bw.zip \
        "https://github.com/bitwarden/clients/releases/download/cli-v${BW_VERSION}/bw-linux-${BW_VERSION}.zip"; \
    unzip -q /tmp/bw.zip -d /usr/local/bin/; \
    chmod +x /usr/local/bin/bw; \
    rm /tmp/bw.zip; \
    apt-get purge -y --auto-remove curl unzip

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOMEWORK_HUB_CONFIG_DIR=/config \
    HOMEWORK_HUB_TOKENS_DIR=/config/tokens \
    HOMEWORK_HUB_STATE_DB=/config/state.db \
    HOMEWORK_HUB_LOG_DIR=/logs \
    PLAYWRIGHT_BROWSERS_PATH=/opt/playwright

# Install Chromium + its system deps via Playwright. The image gains ~450MB
# but it's the price of bypassing the school's OAuth block on Classroom.
# We pin to whatever Chromium ships with the Playwright version in uv.lock.
RUN set -eux; \
    mkdir -p /opt/playwright; \
    /opt/venv/bin/python -m playwright install --with-deps chromium; \
    chown -R 568:568 /opt/playwright

# Match the homelab convention: PUID/PGID 568 (apps user on TrueNAS).
RUN groupadd --system --gid 568 app \
    && useradd --system --uid 568 --gid 568 --home /app --shell /usr/sbin/nologin app \
    && mkdir -p /config /config/tokens /logs /app \
    && chown -R 568:568 /config /logs /app

WORKDIR /app
COPY --from=builder --chown=568:568 /opt/venv /opt/venv

USER 568

EXPOSE 30062

# tini reaps zombies and forwards signals so APScheduler shuts down cleanly
# when Portainer stops the container.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "homework_hub"]
