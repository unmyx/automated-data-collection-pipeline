# syntax=docker/dockerfile:1
# Multi-stage image for the ADCP service: dependencies are resolved once in the
# builder stage and copied into a slim, non-root runtime stage.

# --- build stage: resolve dependencies into a self-contained virtualenv -------
FROM python:3.13-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=60

WORKDIR /build

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN python -m pip install --upgrade pip

# Copy only what the build backend needs so Docker can cache this layer.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN python -m pip install .

# --- runtime stage: no compiler, no build cache, non-root user ---------------
FROM python:3.13-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ADCP_ENV=prod \
    ADCP_LOG_FORMAT=json

COPY --from=builder /opt/venv /opt/venv

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin adcp

WORKDIR /app
RUN chown -R adcp:adcp /app
USER adcp

# The healthcheck proves the CLI is importable and the image is sane. It stays
# deliberately cheap: `adcp health` (a freshness-threshold command) is designed
# but not implemented, see docs/PLAN.md section 17.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD adcp --version || exit 1

CMD ["adcp", "--help"]
