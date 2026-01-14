# Use Python 3.13 slim image for smaller size
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Set working directory
WORKDIR /app

# Copy all source files
COPY . .

# Install pip and the package
# Using --pre to allow prerelease versions like rnet
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --pre -e .

# Create data directory for spider output
RUN mkdir -p /app/data

# Set the data directory as a volume
VOLUME ["/app/data"]

# Default command runs the quotes spider
CMD ["python", "examples/quotes_spider.py"]
