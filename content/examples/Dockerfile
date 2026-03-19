# syntax=docker/dockerfile:1
# ==============================================================================
# OPTIMIZED DOCKER MULTI-STAGE FOR DATA PIPELINES
# ==============================================================================

# --- STAGE 1: BUILDER ---
FROM python:3.12-slim-bookworm AS builder

# UV Configuration
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Get uv binary
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

WORKDIR /app

# Install dependencies using cache mounts for extreme speed
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project --no-editable

# --- STAGE 2: RUNTIME ---
FROM python:3.12-slim-bookworm AS runtime

# Documentation: Metadata for the image
LABEL maintainer="Data Engineering Team"
LABEL architecture="Agnostic"

# Security: Non-root user
RUN groupadd -r datauser && useradd -r -g datauser datauser

WORKDIR /app

# Copy only the virtual environment
COPY --from=builder --chown=datauser:datauser /app/.venv /app/.venv

# Copy source code with correct ownership
COPY --chown=datauser:datauser src/ ./src/

# Environment setup
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1 
ENV PYTHONUNBUFFERED=1

USER datauser

# Using 'exec' form for better signal handling (SIGTERM)
ENTRYPOINT ["python", "-m", "src.main"]