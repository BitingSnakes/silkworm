# Use Python 3.13 slim image for smaller size
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Build-time user/group IDs for non-root runtime (override with --build-arg)
ARG APP_UID=1000
ARG APP_GID=1000

# Create unprivileged runtime user
RUN groupadd --gid "${APP_GID}" app && \
    useradd --uid "${APP_UID}" --gid app --create-home --shell /bin/sh app

# Set working directory
WORKDIR /app

# Copy all source files
COPY . .

# Install pip, the package, and runtime deps used by bundled examples.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e . "pydantic==2.12.5"

# Create data directory for spider output
RUN mkdir -p /app/data && \
    chown -R app:app /app

# Set the data directory as a volume
VOLUME ["/app/data"]

# Run as non-root user
USER app

# Default command runs the quotes spider
CMD ["python", "examples/quotes_spider.py"]
