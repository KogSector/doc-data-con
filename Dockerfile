# =============================================================================
# Data Connector Service - Dockerfile
# Port: 8081
# Role: Source integration, intelligent routing, and event publishing
# Architecture: Event-driven with Kafka
# =============================================================================

# Stage 1: Builder
FROM python:3.12-slim AS builder

WORKDIR /app

# Install system dependencies for building packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies into a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

# Stage 2: Runtime
FROM python:3.12-slim AS runtime

WORKDIR /app

# Install runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user before changing ownership
RUN useradd --create-home app

# Copy application code with strict ownership
COPY --chown=app:app app/ ./app/

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PORT=8081
ENV PYTHONPATH=/app

# Ensure correct permissions for the working directory
RUN chown -R app:app /app

# Switch to non-root user
USER app

EXPOSE 8081

# Health check optimized for Render
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8081}/health || exit 1

# Use dumb-init as PID 1 for proper signal handling
ENTRYPOINT ["dumb-init", "--"]

CMD sh -c "python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8081}"
