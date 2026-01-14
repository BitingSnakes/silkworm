# Docker Setup for Silkworm

This guide explains how to run Silkworm spiders in Docker containers.

## Prerequisites

- Docker Engine 20.10+ or Docker Desktop
- Docker Compose V2 (included with Docker Desktop)

## Quick Start

### Build the Docker image

```bash
docker build -t silkworm-rs:latest .
```

### Run the default quotes spider

```bash
docker compose up quotes
```

Output will be saved to `./data/quotes.jl` on your host machine.

## Using Docker Compose

### Available Services

The `compose.yaml` file defines several pre-configured spider services:

#### 1. Quotes Spider (default)
Scrapes quotes from quotes.toscrape.com

```bash
docker compose up quotes
```

#### 2. HackerNews Spider
Scrapes latest posts from Hacker News (5 pages by default)

```bash
docker compose up --profile hackernews hackernews
```

#### 3. Lobsters Spider
Scrapes posts from lobste.rs (2 pages by default)

```bash
docker compose up --profile lobsters lobsters
```

#### 4. Custom Spider
Run any spider from the examples directory

```bash
docker compose run --rm custom python examples/your_spider.py
```

### Customizing Spider Parameters

You can override the command to change spider behavior:

```bash
# Run HackerNews spider with 10 pages
docker compose run --rm hackernews python examples/hackernews_spider.py --pages 10

# Run Lobsters spider with 5 pages
docker compose run --rm lobsters python examples/lobsters_spider.py --pages 5
```

### Environment Variables

Set the log level using environment variables:

```bash
# Run with DEBUG logging
docker compose run -e SILKWORM_LOG_LEVEL=DEBUG quotes
```

## Running Without Docker Compose

### Build the image

```bash
docker build -t silkworm-rs:latest .
```

### Run a spider

```bash
# Run quotes spider
docker run --rm -v $(pwd)/data:/app/data silkworm-rs:latest python examples/quotes_spider.py

# Run with custom spider
docker run --rm -v $(pwd)/data:/app/data silkworm-rs:latest python examples/hackernews_spider.py --pages 10

# Run with environment variable
docker run --rm -v $(pwd)/data:/app/data -e SILKWORM_LOG_LEVEL=DEBUG silkworm-rs:latest python examples/quotes_spider.py
```

## Data Persistence

All scraped data is saved to the `/app/data` directory inside the container. This directory is mounted as a volume to `./data` on your host machine, so your scraped data persists after the container stops.

## Customizing the Dockerfile

The Dockerfile is designed to be minimal and easy to customize:

- **Base image**: Python 3.13-slim for small size
- **Dependencies**: Installed via pip with `--pre` flag to allow prerelease versions (required for rnet)
- **Source code**: Copied from host to container
- **Data volume**: `/app/data` for spider output

### Example: Adding Extra Dependencies

If you need additional Python packages, modify the Dockerfile:

```dockerfile
# After the pip install line, add:
RUN pip install --no-cache-dir your-package-name
```

### Example: Using a Different Python Version

Change the first line of the Dockerfile:

```dockerfile
FROM python:3.14-slim  # or any other Python 3.13+ version
```

## Troubleshooting

### Build fails with network errors

If you encounter network timeouts during build, try:

```bash
# Use Docker BuildKit with better caching
DOCKER_BUILDKIT=1 docker build -t silkworm-rs:latest .
```

### Permission issues with data directory

If you get permission errors when writing to the data directory:

```bash
# Create the directory with proper permissions
mkdir -p ./data
chmod 777 ./data
```

### Container exits immediately

Check the logs to see what went wrong:

```bash
docker compose logs quotes
```

## Example Workflows

### Scrape quotes and save to JSON Lines

```bash
docker compose up quotes
cat ./data/quotes.jl
```

### Scrape HackerNews and analyze with jq

```bash
docker compose up --profile hackernews hackernews
cat ./data/hackernews.jl | jq '.title'
```

### Run multiple spiders in parallel

```bash
# Start all spiders in detached mode
docker compose up -d quotes
docker compose up -d --profile hackernews hackernews
docker compose up -d --profile lobsters lobsters

# Check status
docker compose ps

# View logs
docker compose logs -f
```

### Clean up

```bash
# Stop and remove containers
docker compose down

# Remove image
docker rmi silkworm-rs:latest

# Clean up data
rm -rf ./data
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: Run Spider

on:
  schedule:
    - cron: '0 0 * * *'  # Run daily
  workflow_dispatch:

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Build Docker image
        run: docker build -t silkworm-rs:latest .
      
      - name: Run spider
        run: docker run --rm -v $(pwd)/data:/app/data silkworm-rs:latest python examples/quotes_spider.py
      
      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: scraped-data
          path: ./data/
```

### GitLab CI Example

```yaml
spider-job:
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -t silkworm-rs:latest .
    - docker run --rm -v $(pwd)/data:/app/data silkworm-rs:latest python examples/quotes_spider.py
  artifacts:
    paths:
      - data/
```

## Advanced Usage

### Multi-stage builds for smaller images

You can optimize the Dockerfile with multi-stage builds to reduce the final image size:

```dockerfile
# Builder stage
FROM python:3.13-slim as builder
WORKDIR /app
COPY . .
RUN pip install --user --pre -e .

# Runtime stage
FROM python:3.13-slim
WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY examples/ ./examples/
ENV PATH=/root/.local/bin:$PATH
CMD ["python", "examples/quotes_spider.py"]
```

### Using docker-compose.override.yaml

Create a `docker-compose.override.yaml` file for local development:

```yaml
services:
  quotes:
    volumes:
      - ./src:/app/src  # Mount source code for development
    environment:
      - SILKWORM_LOG_LEVEL=DEBUG
```

This file is automatically loaded by Docker Compose and overrides settings from `compose.yaml`.

## Security Considerations

- The container runs as root by default. For production, consider adding a non-root user.
- Sensitive data (API keys, credentials) should be passed via environment variables or Docker secrets, never hardcoded.
- Keep the base image updated to get security patches.

## Support

For issues related to Docker setup, please open an issue at https://github.com/BitingSnakes/silkworm/issues
