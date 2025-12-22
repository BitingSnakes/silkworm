# API Reference

This page lists the public API exports from [src/silkworm/__init__.py](../src/silkworm/__init__.py) with links to their implementation.

## Core Types
- **`Request`**: Immutable request dataclass. [src/silkworm/request.py](../src/silkworm/request.py)
 - **`Response`**: Base response with `text`, `encoding`, `url_join`, `follow`, `follow_all`, `close`. [src/silkworm/response.py](../src/silkworm/response.py)
- **`HTMLResponse`**: Response with async selectors. [src/silkworm/response.py](../src/silkworm/response.py)
- **`Spider`**: Base spider class. [src/silkworm/spiders.py](../src/silkworm/spiders.py)
- **`Engine`**: Crawl orchestrator. [src/silkworm/engine.py](../src/silkworm/engine.py)

## Runner Helpers
- **`crawl(...)`**: Async entrypoint that runs a spider. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider(...)`**: Sync wrapper around `crawl`. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_uvloop(...)`**: `run_spider` with uvloop. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_winloop(...)`**: `run_spider` with winloop. [src/silkworm/runner.py](../src/silkworm/runner.py)
- **`run_spider_trio(...)`**: Trio runner using trio-asyncio. [src/silkworm/runner.py](../src/silkworm/runner.py)

## Convenience Helpers
- **`fetch_html(...)`**: Fetch HTML text and a scraper-rs Document. [src/silkworm/api.py](../src/silkworm/api.py)
- **`get_logger(...)`**: Configured logly logger. [src/silkworm/logging.py](../src/silkworm/logging.py)

## Exceptions
- **`SilkwormError`**: Base exception. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
- **`HttpError`**: HTTP failures. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
- **`SpiderError`**: Callback failures. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
- **`SelectorError`**: Selector failures. [src/silkworm/exceptions.py](../src/silkworm/exceptions.py)
