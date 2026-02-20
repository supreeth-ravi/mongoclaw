# MongoClaw Dockerfile
# Multi-stage build for optimized production image

# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package management
RUN pip install uv

# Copy project files
COPY pyproject.toml ./
COPY src ./src

# Install dependencies
RUN uv pip install --system --no-cache .

# Production stage
FROM python:3.11-slim as production

WORKDIR /app

# Create non-root user
RUN groupadd -r mongoclaw && useradd -r -g mongoclaw mongoclaw

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/mongoclaw /usr/local/bin/mongoclaw

# Copy source code
COPY --from=builder /app/src /app/src

# Copy config examples
COPY configs /app/configs

# Set ownership
RUN chown -R mongoclaw:mongoclaw /app

# Switch to non-root user
USER mongoclaw

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["mongoclaw", "server", "start", "--host", "0.0.0.0", "--port", "8000"]

# Labels
LABEL org.opencontainers.image.title="MongoClaw"
LABEL org.opencontainers.image.description="Declarative AI agents framework for MongoDB"
LABEL org.opencontainers.image.source="https://github.com/your-org/mongoclaw"
