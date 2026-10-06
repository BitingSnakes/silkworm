# Docker Setup for Silkworm

This guide explains how to run Silkworm spiders in Docker containers.

## Prerequisites

- Docker Engine 23+ (BuildKit) or Docker Desktop
- Docker Compose V2.20+ (included with Docker Desktop)

## Quick Start

### Build the Docker image

```bash
docker build -t silkworm-rs:latest .
```

Optional: match container user/group IDs to your host user for bind-mounted volume writes.

```bash
docker build \
  --build-arg APP_UID="$(id -u)" \
  --build-arg APP_GID="$(id -g)" \
  -t silkworm-rs:latest .
```

### Run the default quotes spider

```bash
docker compose up quotes
```

Output will be saved to `./data/quotes.jl` on your host machine.

## Using Docker Compose

### Optional: Build with host UID/GID

To align the image runtime user with your host user, set:

```bash
export APP_UID="$(id -u)"
export APP_GID="$(id -g)"
docker compose build
```

### Available Services

The `compose.yaml` file defines several pre-configured spider services. All but
`quotes` sit behind a profile; naming a service on the command line enables its
profile automatically.

#### 1. Quotes Spider (default)
Scrapes quotes from quotes.toscrape.com

```bash
docker compose up quotes
```

#### 2. Production Quotes Spider
Validated, resumable crawl that exits non-zero when its checks fail

```bash
docker compose up production
```

#### 3. HackerNews Spider
Scrapes latest posts from Hacker News (5 pages by default)

```bash
docker compose up hackernews
```

#### 4. Lobsters Spider
Scrapes posts from lobste.rs (2 pages by default)

```bash
docker compose up lobsters
```

#### 5. Custom Spider
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
docker compose run --rm -e SILKWORM_LOG_LEVEL=DEBUG quotes

# Or for every service started from this shell
SILKWORM_LOG_LEVEL=DEBUG docker compose up quotes
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

The Dockerfile is a two-stage build:

- **Builder**: `python:3.14.7-slim-trixie` with [uv](https://docs.astral.sh/uv/) installs the dependencies pinned in `uv.lock` (`uv sync --locked`) into `/opt/venv`
- **Runtime**: the same slim base with only `/opt/venv` and `examples/` copied in, so no build tooling or source tree ships in the image
- **Runtime user**: Runs as non-root user `app` (UID/GID configurable via build args)
- **Data volume**: `/app/data` for spider output

### Example: Adding Extra Dependencies

If you need additional Python packages, modify the Dockerfile:

Add them in the builder stage, after the `uv sync` step:

```dockerfile
RUN uv pip install --python /opt/venv/bin/python your-package-name
```

Optional extras from `pyproject.toml` can be enabled on the `uv sync` lines instead, e.g. `uv sync --locked --no-dev --no-editable --extra uvloop`.

### Example: Using a Different Python Version

Pass the `PYTHON_VERSION` build arg (any supported Python release with a `-slim-trixie` image):

```bash
docker build --build-arg PYTHON_VERSION=3.13.14 -t silkworm-rs:py313 .
```

`UV_VERSION` works the same way for the uv release used in the builder stage.

### Example: Customizing Runtime UID/GID

Build with host-matching IDs so bind-mounted `./data` stays writable without permissive permissions:

```bash
docker build \
  --build-arg APP_UID="$(id -u)" \
  --build-arg APP_GID="$(id -g)" \
  -t silkworm-rs:latest .
```

## Troubleshooting

### Build fails with network errors

If you encounter network timeouts during build, try:

```bash
# Retry; the uv download cache is kept between builds (BuildKit cache mount)
docker build -t silkworm-rs:latest .
```

### Permission issues with data directory

If you get permission errors when writing to the data directory:

```bash
# Create the directory and give ownership to your user/group
mkdir -p ./data
chown "$(id -u):$(id -g)" ./data
```

If you previously ran containers as root (for example with `sudo docker compose`), fix existing ownership once:

```bash
sudo chown -R "$(id -u):$(id -g)" ./data
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
docker compose up hackernews
cat ./data/hackernews.jl | jq '.title'
```

### Run multiple spiders in parallel

```bash
# Start all spiders in detached mode
docker compose up -d quotes
docker compose up -d hackernews
docker compose up -d lobsters

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
      - uses: actions/checkout@v5
      
      - name: Build Docker image
        run: docker build -t silkworm-rs:latest .
      
      - name: Run spider
        run: docker run --rm -v $(pwd)/data:/app/data silkworm-rs:latest python examples/quotes_spider.py
      
      - name: Upload results
        uses: actions/upload-artifact@v5
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

### Using compose.override.yaml

Create a `compose.override.yaml` file for local development:

```yaml
services:
  quotes:
    volumes:
      - ./examples:/app/examples  # Edit spiders without rebuilding
    environment:
      - SILKWORM_LOG_LEVEL=DEBUG
```

This file is automatically loaded by Docker Compose and overrides settings from `compose.yaml`.

## Security Considerations

- Sensitive data (API keys, credentials) should be passed via environment variables or Docker secrets, never hardcoded.
- Keep the base image updated to get security patches.

## Support

For issues related to Docker setup, please open an issue at https://github.com/BitingSnakes/silkworm/issues
