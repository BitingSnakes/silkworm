# silkworm-rs

[![PyPI - Version](https://img.shields.io/pypi/v/silkworm-rs)](https://pypi.org/project/silkworm-rs/)
[![Tests](https://github.com/BitingSnakes/silkworm/actions/workflows/tests.yml/badge.svg)](https://github.com/BitingSnakes/silkworm/actions/workflows/tests.yml)

Async-first web scraping framework built on [rnet](https://github.com/0x676e67/rnet) (HTTP with browser impersonation) and [scraper-rs](https://github.com/RustedBytes/scraper-rs) (fast HTML parsing). Silkworm gives you a minimal Spider/Request/Response model, middlewares, and pipelines so you can script quick scrapes or build larger crawlers without boilerplate.

## Features
- Async engine with configurable concurrency, bounded queue backpressure (defaults to `concurrency * 10`), and per-request timeouts.
- rnet-powered HTTP client: browser impersonation, redirect following with loop detection, query merging, and proxy support via `request.meta["proxy"]`.
- Typed spiders and callbacks that can return items or `Request` objects; `HTMLResponse` ships helper methods plus `Response.follow` to reuse callbacks.
- Middlewares: User-Agent rotation/default, proxy rotation, retry with exponential backoff + optional sleep codes, flexible delays (fixed/random/custom), and `SkipNonHTMLMiddleware` to drop non-HTML callbacks.
- Pipelines: JSON Lines, SQLite, XML (nested data preserved), and CSV (flattens dicts and lists) out of the box.
- Structured logging via `logly` (`SILKWORM_LOG_LEVEL=DEBUG`), plus periodic/final crawl statistics (requests/sec, queue size, memory, seen URLs).

## Installation

From PyPI:

```bash
pip install silkworm-rs
```

From source:

```bash
uv venv  # install uv from https://docs.astral.sh/uv/getting-started/ if needed
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -e .
```

Targets Python 3.11+; dependencies are pinned in `pyproject.toml`.

## Quick start
Define a spider by subclassing `Spider`, implementing `parse`, and yielding items or follow-up `Request` objects. This example writes quotes to `data/quotes.jl` and enables basic user agent, retry, and non-HTML filtering middlewares.

```python
from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import (
    RetryMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        html = response
        for quote in await html.css(".quote"):
            yield {
                "text": quote.select_first(".text").text,
                "author": quote.select_first(".author").text,
                "tags": [t.text for t in quote.select(".tag")],
            }

        if next_link := await html.find("li.next > a"):
            yield html.follow(next_link.attr("href"), callback=self.parse)


if __name__ == "__main__":
    run_spider(
        QuotesSpider,
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[
            SkipNonHTMLMiddleware(),
            RetryMiddleware(max_times=3, sleep_http_codes=[429, 503]),
        ],
        item_pipelines=[JsonLinesPipeline("data/quotes.jl")],
        concurrency=16,
        request_timeout=10,
        log_stats_interval=30,
    )
```

`run_spider`/`crawl` knobs:
- `concurrency`: number of concurrent HTTP requests; default 16.
- `max_pending_requests`: queue bound to avoid unbounded memory use (defaults to `concurrency * 10`).
- `request_timeout`: per-request timeout (seconds).
- `keep_alive`: reuse HTTP connections when supported by the underlying client (sends `Connection: keep-alive`).
- `html_max_size_bytes`: limit HTML parsed into `Document` to avoid huge payloads.
- `log_stats_interval`: seconds between periodic stats logs; final stats are always emitted.
- `request_middlewares` / `response_middlewares` / `item_pipelines`: plug-ins run on every request/response/item.
- use `run_spider_uvloop(...)` instead of `run_spider(...)` to run under uvloop (requires `pip install silkworm-rs[uvloop]`).

## Built-in middlewares and pipelines

```python
from silkworm.middlewares import (
    DelayMiddleware,
    ProxyMiddleware,
    RetryMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import (
    CSVPipeline,
    JsonLinesPipeline,
    MsgPackPipeline,  # requires: pip install silkworm-rs[msgpack]
    SQLitePipeline,
    XMLPipeline,
    TaskiqPipeline,  # requires: pip install silkworm-rs[taskiq]
)

run_spider(
    QuotesSpider,
    request_middlewares=[
        UserAgentMiddleware(),  # rotate/custom user agent
        DelayMiddleware(min_delay=0.3, max_delay=1.2),  # polite throttling
        # ProxyMiddleware(["http://user:pass@proxy1:8080", "http://proxy2:8080"]),
    ],
    response_middlewares=[
        RetryMiddleware(max_times=3, sleep_http_codes=[403, 429]),  # backoff + retry
        SkipNonHTMLMiddleware(),  # drop callbacks for images/APIs/etc
    ],
    item_pipelines=[
        JsonLinesPipeline("data/quotes.jl"),
        SQLitePipeline("data/quotes.db", table="quotes"),
        XMLPipeline("data/quotes.xml", root_element="quotes", item_element="quote"),
        CSVPipeline("data/quotes.csv", fieldnames=["author", "text", "tags"]),
        MsgPackPipeline("data/quotes.msgpack"),
    ],
)
```

- `DelayMiddleware` strategies: `delay=1.0` (fixed), `min_delay/max_delay` (random), or `delay_func` (custom).
- `RetryMiddleware` backs off with `asyncio.sleep`; any status in `sleep_http_codes` is retried even if not in `retry_http_codes`.
- `SkipNonHTMLMiddleware` checks `Content-Type` and optionally sniffs the body (`sniff_bytes`) to avoid running HTML callbacks on binary/API responses.
- `CSVPipeline` flattens nested dicts (e.g., `{"user": {"name": "Alice"}}` -> `user_name`) and joins lists with commas; `XMLPipeline` preserves nesting.
- `MsgPackPipeline` writes items in binary MessagePack format using [ormsgpack](https://github.com/aviramha/ormsgpack) for fast and compact serialization (requires `pip install silkworm-rs[msgpack]`).
- `TaskiqPipeline` sends items to a [Taskiq](https://taskiq-python.github.io/) queue for distributed processing (requires `pip install silkworm-rs[taskiq]`).

## Streaming items to a queue with TaskiqPipeline
Stream scraped items to a [Taskiq](https://taskiq-python.github.io/) queue for distributed processing:

```python
from taskiq import InMemoryBroker
from silkworm.pipelines import TaskiqPipeline

broker = InMemoryBroker()

@broker.task
async def process_item(item):
    # Your item processing logic here
    print(f"Processing: {item}")
    # Save to database, send to another service, etc.

pipeline = TaskiqPipeline(broker, task=process_item)
run_spider(MySpider, item_pipelines=[pipeline])
```

This enables distributed processing, retries, rate limiting, and other Taskiq features. See `examples/taskiq_quotes_spider.py` for a complete example.

## Handling non-HTML responses
Keep crawls cheap when URLs mix HTML and binaries/APIs:

```python
response_middlewares=[SkipNonHTMLMiddleware(sniff_bytes=1024)]
# Tighten HTML parsing size (bytes) to avoid loading huge bodies into scraper-rs
run_spider(MySpider, html_max_size_bytes=1_000_000)
```

## Performance optimization with uvloop
For improved async performance, enable uvloop (a fast, drop-in replacement for asyncio's event loop):

```bash
pip install silkworm-rs[uvloop]
```

Then call `run_spider_uvloop` (same signature as `run_spider`):

```python
from silkworm import run_spider_uvloop

run_spider_uvloop(
    QuotesSpider,
    concurrency=32,
)
```

uvloop can provide 2-4x performance improvement for I/O-bound workloads.

## Running spiders with trio
If you prefer trio over asyncio, you can use `run_spider_trio` instead of `run_spider`:

```bash
pip install silkworm-rs[trio]
```

Then use `run_spider_trio`:

```python
from silkworm import run_spider_trio

run_spider_trio(
    QuotesSpider,
    concurrency=16,
    request_timeout=10,
)
```

This runs your spider using trio as the async backend via trio-asyncio compatibility layer.

## Logging and crawl statistics
- Structured logs via `logly`; set `SILKWORM_LOG_LEVEL=DEBUG` for verbose request/response/middleware output.
- Periodic statistics with `log_stats_interval`; final stats always include elapsed time, queue size, requests/sec, seen URLs, items scraped, errors, and memory MB.

## Examples
- `python examples/quotes_spider.py` → `data/quotes.jl`
- `python examples/quotes_spider_trio.py` → `data/quotes_trio.jl` (demonstrates trio backend)
- `python examples/hackernews_spider.py --pages 5` → `data/hackernews.jl`
- `python examples/lobsters_spider.py --pages 2` → `data/lobsters.jl`
- `python examples/url_titles_spider.py --urls-file data/url_titles.jl --output data/titles.jl` (includes `SkipNonHTMLMiddleware` and stricter HTML size limits)
- `python examples/export_formats_demo.py --pages 2` → JSONL, XML, and CSV outputs in `data/`
- `python examples/taskiq_quotes_spider.py --pages 2` → demonstrates TaskiqPipeline for queue-based processing

## Convenience API
For one-off fetches without a full spider, use `fetch_html`:

```python
import asyncio
from silkworm import fetch_html

async def main():
    text, doc = await fetch_html("https://example.com")
    print(doc.select_first("title").text)

asyncio.run(main())
```
