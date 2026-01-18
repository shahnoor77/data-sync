FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies
COPY pyproject.toml poetry.lock* ./

# Install Poetry and dependencies
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-root

# Copy application
COPY src/ ./src/
COPY config/ ./config/

# Create required directories
RUN mkdir -p /app/data/offsets /app/data/dlq /app/logs

# Copy entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9090


WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies first (for better caching)
COPY pyproject.toml poetry.lock* ./

# Install Poetry and dependencies
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-root

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Create required directories
RUN mkdir -p /app/data/offsets /app/data/dlq /app/logs


# Copy entrypoint from root to /app/
COPY entrypoint.sh ./entrypoint.sh

# Fix potential Windows line ending issues and set permissions
RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Set environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 9090

# Realistic healthcheck (checks if process is running)
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD pgrep -f "python" || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]