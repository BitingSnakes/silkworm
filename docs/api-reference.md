# API Reference

This page lists the public API exports from [src/silkworm/__init__.py](../src/silkworm/__init__.py) with links to their implementation.

## Core Types
- **`Request`**: Slotted request dataclass with URL/method/headers/body/meta/callback/errback fields. [src/silkworm/request.py](../src/silkworm/request.py)
- **`Response`**: Base response with `text`, `encoding`, `url_join`, `follow`, `follow_all`, `close`. [src/silkworm/response.py](../src/silkworm/response.py)
- **`HTMLResponse`**: Response with async selectors. [src/silkworm/response.py](../src/silkworm/response.py)
- **`Spider`**: Base spider class. [src/silkworm/spiders.py](../src/silkworm/spiders.py)
- **`Engine`**: Crawl orchestrator. [src/silkworm/engine.py](../src/silkworm/engine.py)
- **`EngineLogger`**: Logging controls for URL, status, middleware, and pipeline event visibility. [src/silkworm/engine.py](../src/silkworm/engine.py)

## Runner Helpers
- **`crawl(...)`**: Async entrypoint that runs a spider. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider(...)`**: Sync wrapper around `crawl`. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_rsloop(...)`**: `run_spider` with rsloop. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_uvloop(...)`**: `run_spider` with uvloop. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_winloop(...)`**: `run_spider` with winloop. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_trio(...)`**: Trio runner using trio-asyncio. [src/silkworm/runner.py](../src/silkworm/runner.py)

## Convenience Helpers
- **`fetch_html(...)`**: Fetch HTML text and a scraper-rs `AsyncDocument`. [src/silkworm/api.py](../src/silkworm/api.py)
- **`fetch_html_cdp(...)`**: Fetch rendered HTML text and a scraper-rs `AsyncDocument` via CDP. [src/silkworm/api.py](../src/silkworm/api.py)
- **`fetch_html_servo(...)`**: Fetch rendered HTML text and a scraper-rs `AsyncDocument` via Servo/servofetch. [src/silkworm/api.py](../src/silkworm/api.py)
- **`get_logger(...)`**: Configured logly logger. [src/silkworm/logging.py](../src/silkworm/logging.py)

## Public Client Adapters
- **`OnionLinkClient`**: Optional client for Tor v3 `.onion` services (requires `onionlink` extra). [src/silkworm/onionlink.py](../src/silkworm/onionlink.py)
- **`ServoFetchClient`**: Client adapter for Servo-rendered HTML fetches through `servofetch`. `servofetch` is distributed separately from the package extras. [src/silkworm/servo.py](../src/silkworm/servo.py)

## Public Middlewares
- **`CookiesMiddleware`**: Stateful cookie jar middleware with per-request controls and Netscape/Mozilla cookie file `save(...)`/`load(...)`. [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)
- **`RequestResponseStreamMiddleware`**: Streams request/response telemetry to an HTTP collector. [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)
- **`RobotsTxtDelayMiddleware`**: Downloads robots.txt for a site origin and applies `Crawl-delay` or `Request-rate` to matching-origin requests. [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)

## Optional CDP Export
- **`CDPClient`**: CDP client for browser-driven fetches (available when `cdp` extra is installed). [src/silkworm/cdp.py](../src/silkworm/cdp.py)

## Exceptions
- **`SilkwormError`**: Base exception. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
- **`HttpError`**: HTTP failures. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
- **`SpiderError`**: Callback failures. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
- **`SelectorError`**: Selector failures. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
