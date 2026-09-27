# silkworm-rs

[![PyPI - Version](https://img.shields.io/pypi/v/silkworm-rs)](https://pypi.org/project/silkworm-rs/)
[![Tests](https://github.com/BitingSnakes/silkworm/actions/workflows/tests.yml/badge.svg)](https://github.com/BitingSnakes/silkworm/actions/workflows/tests.yml)
[![Docs](https://github.com/BitingSnakes/silkworm/actions/workflows/docs.yml/badge.svg)](https://bitingsnakes.github.io/silkworm/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/silkworm-rs?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/silkworm-rs)

Async-first web scraping framework built on [wreq](https://github.com/0x676e67/wreq-python) (HTTP with browser impersonation) and [scraper-rs](https://github.com/RustedBytes/scraper-rs) (fast HTML parsing). Silkworm gives you a minimal Spider/Request/Response model, middlewares, and pipelines so you can script quick scrapes or build larger crawlers without boilerplate.

📖 **Documentation:** https://bitingsnakes.github.io/silkworm/

## Features
- Async engine with configurable concurrency, priority-aware queueing, bounded, deadlock-free backpressure (defaults to `concurrency * 10`; hard for seeding, soft when several callbacks produce requests at once), and per-request timeouts.
- wreq-powered HTTP client: browser impersonation, redirect following with loop detection, query merging, and proxy support via `request.meta["proxy"]`.
- Optional OnionLink client integration for scraping Tor v3 `.onion` sites without routing through wreq.
- Optional Servo rendering via `ServoFetchClient` for JavaScript-rendered pages without changing the default HTTP client.
- Typed async spiders with a push-style callback API: `await self.emit(item)` streams items to pipelines and `await self.follow(...)` / `await response.follow(href)` schedule requests, both with backpressure; `HTMLResponse` ships selector helpers.
- Optional declarative extraction with compiled `Item`, `Text`, and `Attr` field plans while keeping the callback API available.
- HTML-to-Markdown conversion via `fast-h2m`, including rich `full`, lean `minimal`, and streaming modes.
- Middlewares: User-Agent rotation/default, proxy rotation, cookie jars with save/load, retries of error statuses and transient network failures with exponential backoff, per-host AutoThrottle, robots.txt enforcement (`Disallow` and `Crawl-delay`), flexible delays, `SkipNonHTMLMiddleware` to drop non-HTML callbacks, and `CloudflareCrawlMiddleware` for Browser Rendering crawl jobs.
- Pipelines: JSON Lines, SQLite, XML (nested data preserved), and CSV (flattens dicts and lists) out of the box, plus schema validation with `ValidationPipeline`.
- Production controls: a `CrawlResult` with a failure policy (error rate, minimum items, drop rate) so broken spiders fail loudly; stop limits (`max_items`, `max_requests`, `max_depth`, `max_duration`, `max_errors`); `allowed_domains`; per-domain concurrency; graceful SIGINT/SIGTERM shutdown; response size limits; pause/resume with `job_dir`; an on-disk HTTP cache; Prometheus metrics; and layered settings (environment, `custom_settings`, CLI).
- A `silkworm` command line (`crawl`, `parse`, `fetch`) and `silkworm.testing` helpers for testing callbacks offline.
- Structured logging via the standard library (`SILKWORM_LOG_LEVEL=DEBUG`), plus periodic/final crawl statistics with per-status, per-domain, and per-error breakdowns.

## Installation

From PyPI with pip:

```bash
pip install silkworm-rs
```

From PyPI with uv (recommended for faster installs):

```bash
uv pip install silkworm-rs
# or if using uv's project management:
uv add silkworm-rs
```

From source:

```bash
uv venv  # install uv from https://docs.astral.sh/uv/getting-started/ if needed
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -e .
```

Targets Python 3.13+; dependencies are pinned in `pyproject.toml`.

## Quick start
Define a spider by subclassing `Spider` and implementing an async `parse` that reports items with `await self.emit(...)` and schedules follow-up pages with `await response.follow(...)`. This example writes quotes to `data/quotes.jl` and enables basic user agent, retry, and non-HTML filtering middlewares.

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

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        html = response
        for quote in await html.select(".quote"):
            text_el = await quote.select_first(".text")
            author_el = await quote.select_first(".author")
            if text_el is None or author_el is None:
                continue
            tags = await quote.select(".tag")
            await self.emit(
                {
                    "text": text_el.text,
                    "author": author_el.text,
                    "tags": [t.text for t in tags],
                }
            )

        if next_link := await html.select_first("li.next > a"):
            if href := next_link.attr("href"):
                await html.follow(href, callback=self.parse)


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

### Upgrading from 0.10
Silkworm 0.11 replaced `yield`/`return`-based callbacks with `emit`/`follow`.
Callbacks, errbacks and `start_requests()` are now `async` functions returning
`None`: turn `yield item` into `await self.emit(item)`, `yield request` into
`await self.follow(request)`, and `yield response.follow(href)` into
`await response.follow(href)`. Legacy generator callbacks fail with a
`SpiderError` that explains the fix. See the
[migration table](https://bitingsnakes.github.io/silkworm/core-concepts.html#migrating-from-0-10-yield-based-callbacks).

### Upgrading to 0.12
- Runners and `Engine.run()` return a `CrawlResult` instead of `None`.
- The default deduplication key is the request fingerprint (method, canonical URL
  with `params`, body) instead of the raw URL, so `?a=1&b=2` and `?b=2&a=1` are
  one page while POSTs with different bodies are not.
- `items_scraped` counts items that passed every pipeline; pipelines can raise
  `DropItem` to discard items (counted as `items_dropped`).
- Responses carry the exact downloaded bytes (earlier versions re-encoded bodies
  as UTF-8 text, corrupting binary files and non-UTF-8 pages), and bodies over
  `max_response_size_bytes` (default 50 MB) fail with `ResponseTooLargeError`.
- Timeouts and connection failures raise `HttpTimeoutError`/`HttpConnectionError`
  (both `HttpError` subclasses), which `RetryMiddleware` now retries.
- Requests record their link depth in `meta["depth"]`, and the new counters'
  names (such as `retries`) are reserved in `Spider.stats_payload`.
- The sync runners stop gracefully on SIGINT/SIGTERM (`handle_signals=False` opts out).

## Production crawling

Declare what a successful crawl means, stop safely, stay polite, and resume after
interruptions:

```python
from silkworm import run_spider
from silkworm.middlewares import AutoThrottleMiddleware, RetryMiddleware, RobotsTxtMiddleware
from silkworm.pipelines import JsonLinesPipeline, ValidationPipeline

throttle = AutoThrottleMiddleware(start_delay=0.5, max_delay=30)
result = run_spider(
    QuotesSpider,
    request_middlewares=[RobotsTxtMiddleware(), throttle],
    response_middlewares=[throttle, RetryMiddleware(max_times=3)],
    item_pipelines=[ValidationPipeline(Quote), JsonLinesPipeline("data/quotes.jl")],
    request_timeout=30,
    concurrency_per_domain=4,
    max_depth=5,
    max_error_rate=0.05,  # raise CrawlFailedError above 5% failed requests
    min_items=50,  # ...or when fewer than 50 valid items were scraped
    job_dir="state/quotes",  # Ctrl+C, then run again to resume
    metrics_port=9410,  # Prometheus metrics at http://127.0.0.1:9410/metrics
)
print(result.close_reason, result.items_scraped, result.error_rate)
```

Or from the command line, with settings from `-s`, `SILKWORM_*` environment
variables, or `Spider.custom_settings`:

```bash
silkworm crawl examples/quotes_spider.py -o data/quotes.jl -s max_items=100 --job-dir state/quotes
silkworm parse https://quotes.toscrape.com/ --spider examples/quotes_spider.py
```

`silkworm crawl` exits with status 1 when the failure policy is violated, so cron
jobs and CI notice broken spiders. See the
[Production Crawling guide](https://bitingsnakes.github.io/silkworm/production.html)
and the [CLI reference](https://bitingsnakes.github.io/silkworm/cli.html).

## Declarative extraction

Use `silkworm.declarative` when the extraction is regular and keep ordinary
callbacks for pagination or site-specific behavior:

```python
from silkworm import HTMLResponse, Response, Spider
from silkworm.declarative import Attr, Item, Text


def parse_price(value: str) -> float:
    return float(value.removeprefix("$").strip())


class Product(Item):
    __selector__ = ".product"

    title: str = Text("h2", strip=True)
    price: float = Text(".price", transform=parse_price)
    url: str = Attr("a", "href", absolute=True)
    image: str | None = Attr("img", "src", absolute=True)
    tags: list[str] = Text(".tag")


class ProductsSpider(Spider):
    start_urls = ("https://shop.example.com/products/",)

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        async for product in Product.extract(response):
            # Existing pipelines consume JSON-compatible values.
            await self.emit(product.to_dict())
```

The annotation controls selector cardinality:

| Annotation | Selector operation | Missing result |
| --- | --- | --- |
| `T` | `select_first()` | raises `MissingFieldError` |
| `T \| None` | `select_first()` | `None` |
| `list[T]` | `select()` | `[]` |

Fields support these options:

- `default=...` supplies the final scalar value when an element or attribute is missing.
- `transform=...` applies a synchronous conversion to each extracted string. Declarative extraction does not implicitly coerce annotations.
- `Text(..., strip=True)` strips surrounding whitespace before transformation.
- `Attr(..., absolute=True)` resolves the attribute through `response.url_join()`.

Plans are compiled from annotations once per `Item` class and then cached. A
required-field or transform failure includes the item, field, selector, response
URL, and root index. `after_extract()` is available for the irregular part of an
otherwise declarative extraction:

```python
class Article(Item):
    title: str = Text("h1")

    async def after_extract(self, response: HTMLResponse) -> None:
        self.title = self.title.strip()
```

`Item.extract()` intentionally does not replace `Spider`, callbacks, requests,
middlewares, or pipelines. See `examples/declarative_quotes_spider.py` for a
complete spider with pagination.

`run_spider`/`crawl` knobs:
- `concurrency`: number of concurrent HTTP requests; default 16; must be positive.
- `max_pending_requests`: queue bound to avoid unbounded memory use (defaults to `concurrency * 10`); if provided, must be positive. `start_requests()` always respects it, and so does a single producing callback; when several callbacks produce requests at once, they enqueue past it rather than parking workers or deadlocking.
- `request_timeout`: per-request timeout (seconds).
- `keep_alive`: reuse HTTP connections when supported by the underlying client (sends `Connection: keep-alive`).
- `http_client`: use a custom client instance such as `OnionLinkClient(...)` or `ServoFetchClient(...)` instead of the default wreq-backed client.
- `dedup_key`: optional `Callable[[Request], str]` used for request deduplication; defaults to `lambda req: req.url`.
- `html_max_size_bytes`: limit HTML parsed into `AsyncDocument` to avoid huge payloads.
- `log_stats_interval`: seconds between periodic stats logs; final stats are always emitted.
- `request_middlewares` / `response_middlewares` / `item_pipelines`: plug-ins run on every request/response/item.
- use `run_spider_rsloop(...)` instead of `run_spider(...)` to run under rsloop (requires `pip install silkworm-rs[rsloop]`).
- use `run_spider_uvloop(...)` instead of `run_spider(...)` to run under uvloop (requires `pip install silkworm-rs[uvloop]`).
- use `run_spider_winloop(...)` instead of `run_spider(...)` to run under winloop on Windows (requires `pip install silkworm-rs[winloop]`).

## Built-in middlewares and pipelines

```python
from silkworm.middlewares import (
    CloudflareCrawlMiddleware,
    CookiesMiddleware,
    DelayMiddleware,
    ProxyMiddleware,
    RequestResponseStreamMiddleware,
    RetryMiddleware,
    RobotsTxtDelayMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import (
    CallbackPipeline,  # invoke a custom callback function on each item
    CSVPipeline,
    JsonLinesPipeline,
    MsgPackPipeline,  # requires: pip install silkworm-rs[msgpack]
    RssPipeline,
    SQLitePipeline,
    XMLPipeline,
    TaskiqPipeline,  # requires: pip install silkworm-rs[taskiq]
    ZenohPipeline,  # requires: pip install silkworm-rs[zenoh]
    PolarsPipeline,  # requires: pip install silkworm-rs[polars]
    ExcelPipeline,  # requires: pip install silkworm-rs[excel]
    YAMLPipeline,  # requires: pip install silkworm-rs[yaml]
    AvroPipeline,  # requires: pip install silkworm-rs[avro]
    ElasticsearchPipeline,  # requires: pip install silkworm-rs[elasticsearch]
    MongoDBPipeline,  # requires: pip install silkworm-rs[mongodb]
    MySQLPipeline,  # requires: pip install silkworm-rs[mysql]
    PostgreSQLPipeline,  # requires: pip install silkworm-rs[postgresql]
    S3JsonLinesPipeline,  # requires: pip install silkworm-rs[s3]
    VortexPipeline,  # requires: pip install silkworm-rs[vortex]
    WebhookPipeline,  # sends items to webhook endpoints using wreq
    GoogleSheetsPipeline,  # requires: pip install silkworm-rs[gsheets]
    SnowflakePipeline,  # requires: pip install silkworm-rs[snowflake]
    FTPPipeline,  # requires: pip install silkworm-rs[ftp]
    SFTPPipeline,  # requires: pip install silkworm-rs[sftp]
    CassandraPipeline,  # requires: pip install silkworm-rs[cassandra]
    CouchDBPipeline,  # requires: pip install silkworm-rs[couchdb]
    DynamoDBPipeline,  # requires: pip install silkworm-rs[dynamodb]
    DuckDBPipeline,  # requires: pip install silkworm-rs[duckdb]
)

run_spider(
    QuotesSpider,
    request_middlewares=[
        UserAgentMiddleware(),  # rotate/custom user agent
        DelayMiddleware(min_delay=0.3, max_delay=1.2),  # polite throttling
        # Read Crawl-delay/Request-rate from robots.txt and serialize same-origin requests
        # RobotsTxtDelayMiddleware("https://quotes.toscrape.com", user_agent="silkworm"),
        # ProxyMiddleware with round-robin selection (default)
        # ProxyMiddleware(proxies=["http://user:pass@proxy1:8080", "http://proxy2:8080"]),
        # ProxyMiddleware with random selection
        # ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"], random_selection=True),
        # ProxyMiddleware from file with random selection
        # ProxyMiddleware(proxy_file="proxies.txt", random_selection=True),
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
- `RobotsTxtDelayMiddleware("https://example.com", user_agent="silkworm")` downloads `https://example.com/robots.txt`, applies `Crawl-delay` or `Request-rate`, and serializes matching-origin requests so concurrency cannot bypass the configured spacing. Use `fallback_delay=...` to keep a conservative delay when robots.txt cannot be fetched.
- `ProxyMiddleware` supports three modes:
  - **Round-robin (default)**: `ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"])` cycles through proxies in order.
  - **Random selection**: `ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"], random_selection=True)` randomly selects a proxy for each request.
  - **From file**: `ProxyMiddleware(proxy_file="proxies.txt")` loads proxies from a file (one proxy per line, blank lines ignored). Combine with `random_selection=True` for random selection from the file.
- `CookiesMiddleware` stores `Set-Cookie` response headers, applies matching `Cookie` request headers, supports named jars via `request.meta["cookiejar"]`, per-request cookies via `request.meta["cookies"]`, opt-out via `request.meta["dont_merge_cookies"]`, and Netscape/Mozilla cookie file `save(...)`/`load(...)`. Use the same instance in `request_middlewares` and `response_middlewares`.
- `RetryMiddleware` backs off with `asyncio.sleep`; any status in `sleep_http_codes` is retried even if not in `retry_http_codes`.
- `SkipNonHTMLMiddleware` checks `Content-Type` and optionally sniffs the body (`sniff_bytes`) to avoid running HTML callbacks on binary/API responses.
- `CloudflareCrawlMiddleware` is opt-in per request via `request.meta["cloudflare_crawl"]`; it submits a Cloudflare Browser Rendering crawl job, polls until completion, and hands your callback a synthetic JSON `Response` with the final API payload.
- `RequestResponseStreamMiddleware` streams paired request/response telemetry events to a collector endpoint; use the same instance in both middleware lists.
- `JsonLinesPipeline` writes items to a local JSON Lines file and, when `opendal` is installed, appends asynchronously via the filesystem backend (`use_opendal=False` to stick to a regular file handle).
- `CSVPipeline` flattens nested dicts (e.g., `{"user": {"name": "Alice"}}` -> `user_name`) and joins lists with commas; `XMLPipeline` preserves nesting.
- `RssPipeline` writes buffered RSS 2.0 feeds from items with configurable title/link/description fields.
- `MsgPackPipeline` writes items in binary MessagePack format using [ormsgpack](https://github.com/aviramha/ormsgpack) for fast and compact serialization (requires `pip install silkworm-rs[msgpack]`).
- `TaskiqPipeline` sends items to a [Taskiq](https://taskiq-python.github.io/) queue for distributed processing (requires `pip install silkworm-rs[taskiq]`).
- `ZenohPipeline` publishes JSON items to a static or dynamically resolved [Zenoh](https://zenoh.io/) key expression (requires `pip install silkworm-rs[zenoh]`).
- `PolarsPipeline` writes items to a Parquet file using Polars for efficient columnar storage (requires `pip install silkworm-rs[polars]`).
- `ExcelPipeline` writes items to an Excel .xlsx file (requires `pip install silkworm-rs[excel]`).
- `YAMLPipeline` writes items to a YAML file (requires `pip install silkworm-rs[yaml]`).
- `AvroPipeline` writes items to an Avro file with optional schema (requires `pip install silkworm-rs[avro]`).
- `ElasticsearchPipeline` sends items to an Elasticsearch index (requires `pip install silkworm-rs[elasticsearch]`).
- `MongoDBPipeline` sends items to a MongoDB collection (requires `pip install silkworm-rs[mongodb]`).
- `MySQLPipeline` sends items to a MySQL database table as JSON (requires `pip install silkworm-rs[mysql]`).
- `PostgreSQLPipeline` sends items to a PostgreSQL database table as JSONB (requires `pip install silkworm-rs[postgresql]`).
- `S3JsonLinesPipeline` writes items to AWS S3 in JSON Lines format using async OpenDAL (requires `pip install silkworm-rs[s3]`).
- `VortexPipeline` writes items to a [Vortex](https://github.com/spiraldb/vortex) file for high-performance columnar storage with 100x faster random access and 10-20x faster scans compared to Parquet (requires `pip install silkworm-rs[vortex]`).
- `WebhookPipeline` sends items to webhook endpoints via HTTP POST/PUT using wreq (same HTTP client as the spider) with support for batching and custom headers.
- `GoogleSheetsPipeline` appends items to Google Sheets with automatic flattening of nested data structures (requires `pip install silkworm-rs[gsheets]` and service account credentials).
- `SnowflakePipeline` sends items to Snowflake data warehouse tables as JSON (requires `pip install silkworm-rs[snowflake]`).
- `FTPPipeline` writes items to an FTP server in JSON Lines format (requires `pip install silkworm-rs[ftp]`).
- `SFTPPipeline` writes items to an SFTP server in JSON Lines format with support for password or key-based authentication (requires `pip install silkworm-rs[sftp]`).
- `CassandraPipeline` sends items to Apache Cassandra database tables (requires `pip install silkworm-rs[cassandra]`).
- `CouchDBPipeline` sends items to CouchDB databases as documents (requires `pip install silkworm-rs[couchdb]`).
- `DynamoDBPipeline` sends items to AWS DynamoDB tables with automatic table creation (requires `pip install silkworm-rs[dynamodb]`).
- `DuckDBPipeline` sends items to a DuckDB database table as JSON (requires `pip install silkworm-rs[duckdb]`).
- `CallbackPipeline` invokes a custom callback function (sync or async) on each item, enabling inline processing logic without creating a full pipeline class. See example below.

## Using CallbackPipeline for custom processing
Process items with custom callback functions without creating a full pipeline class:

```python
from silkworm.pipelines import CallbackPipeline

# Sync callback
def print_item(item, spider):
    print(f"[{spider.name}] {item}")
    return item

# Async callback
async def validate_item(item, spider):
    # Could do async operations like database checks
    if len(item.get("text", "")) < 10:
        print(f"Warning: Short text in item")
    return item

# Modifying callback
def enrich_item(item, spider):
    item["spider_name"] = spider.name
    item["processed"] = True
    return item

run_spider(
    QuotesSpider,
    item_pipelines=[
        CallbackPipeline(callback=print_item),
        CallbackPipeline(callback=validate_item),
        CallbackPipeline(callback=enrich_item),
    ],
)
```

Callbacks receive `(item, spider)` and should return the processed item (or `None` to return the original item unchanged).

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

## Publishing items with ZenohPipeline
Publish every item immediately to a Zenoh key expression:

```python
from silkworm.pipelines import ZenohPipeline

pipeline = ZenohPipeline("scraping/quotes")
run_spider(MySpider, item_pipelines=[pipeline])
```

Use a sync or async resolver when items need separate keys:

```python
async def item_key(item, spider):
    return f"scraping/{spider.name}/{item['category']}"

pipeline = ZenohPipeline(item_key)
```

The pipeline opens and closes its own session by default. Pass a configured
`zenoh.Config` instance to customize that session, or `session=existing_session` to
reuse a caller-owned session.
Injected sessions remain open when the pipeline closes. Publisher options include `encoding`,
`congestion_control`, `priority`, `express`, `reliability`, and `allowed_destination`.
Dynamic publishers are cached by key until pipeline shutdown.

## Handling non-HTML responses
Keep crawls cheap when URLs mix HTML and binaries/APIs:

```python
response_middlewares=[SkipNonHTMLMiddleware(sniff_bytes=1024)]
# Tighten HTML parsing size (bytes) to avoid loading huge bodies into scraper-rs
run_spider(MySpider, html_max_size_bytes=1_000_000)
```

## Performance optimization with rsloop
For improved async performance, enable rsloop as a drop-in replacement for asyncio's event loop:

```bash
pip install silkworm-rs[rsloop]
# or with uv:
uv pip install silkworm-rs[rsloop]
```

Then call `run_spider_rsloop` (same signature as `run_spider`):

```python
from silkworm import run_spider_rsloop

run_spider_rsloop(
    QuotesSpider,
    concurrency=32,
)
```

## Performance optimization with uvloop
For improved async performance, enable uvloop (a fast, drop-in replacement for asyncio's event loop):

```bash
pip install silkworm-rs[uvloop]
# or with uv:
uv pip install silkworm-rs[uvloop]
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

## Performance optimization with winloop (Windows)
For Windows users who want improved async performance, enable winloop (a Windows-compatible alternative to uvloop):

```bash
pip install silkworm-rs[winloop]
# or with uv:
uv pip install silkworm-rs[winloop]
```

Then call `run_spider_winloop` (same signature as `run_spider`):

```python
from silkworm import run_spider_winloop

run_spider_winloop(
    QuotesSpider,
    concurrency=32,
)
```

winloop provides significant performance improvements on Windows, similar to what uvloop offers on Unix-like systems.

## Running spiders with trio
If you prefer trio over asyncio, you can use `run_spider_trio` instead of `run_spider`:

```bash
pip install silkworm-rs[trio]
# or with uv:
uv pip install silkworm-rs[trio]
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
The Trio runner currently requires Python 3.13 because trio-asyncio 0.16 is not
compatible with Python 3.14 or newer.

## JavaScript rendering with Servo
For pages that need JavaScript execution but do not require driving an external browser process, install the optional Servo renderer and pass `ServoFetchClient` as the spider HTTP client.

Install a wheel from this page: https://github.com/RustedBytes/servofetch-py/releases

```python
from silkworm import HTMLResponse, Response, ServoFetchClient, Spider, run_spider


class RenderedSpider(Spider):
    name = "rendered"
    start_urls = ("https://example.com/",)

    async def parse(self, response: Response) -> None:
        if isinstance(response, HTMLResponse):
            title = await response.select_first("title")
            await self.emit({"title": title.text if title else ""})


run_spider(RenderedSpider, http_client=ServoFetchClient(settle_ms=500))
```

Per-request render options live in `Request.meta`: `servo_javascript`, `servo_settle_ms`, `servo_user_agent`, `servo_screenshot`, and `servo_full_page`. `Request.timeout` overrides the client timeout for that request.

`ServoFetchClient` embeds Servo through `servofetch`; the existing CDP client connects to an external Lightpanda/Chrome-compatible browser over WebSocket. Use the default wreq client when pages do not need client-side rendering.

## JavaScript rendering with Lightpanda (CDP)
For pages that require JavaScript execution, you can use Lightpanda (or any CDP-compatible browser) instead of the standard HTTP client. This uses the Chrome DevTools Protocol (CDP) to control a browser.

### Installation
```bash
pip install silkworm-rs[cdp]
# or with uv:
uv pip install silkworm-rs[cdp]
```

### Starting Lightpanda
```bash
lightpanda --remote-debugging-port=9222
```

Or use Chrome/Chromium:
```bash
chromium --remote-debugging-port=9222 --headless
```

The same `ws://127.0.0.1:9222` endpoint works for both. Chrome only accepts its per-session `ws://.../devtools/browser/<id>` URL, so when a bare `host:port` endpoint is refused, `CDPClient` looks that URL up at `http://host:port/json/version` and connects to it (keeping your host and port). You can also pass the full URL yourself.

### Using CDP in your spider
There are two ways to use CDP: the convenience API or custom spider integration.

#### Convenience API (simple one-off fetches)
```python
import asyncio
from silkworm import fetch_html_cdp

async def main():
    # Fetch HTML with JavaScript rendering
    text, doc = await fetch_html_cdp(
        "https://example.com",
        ws_endpoint="ws://127.0.0.1:9222",
        timeout=30.0
    )
    
    # Extract data from rendered page
    title = await doc.select_first("title")
    print(title.text if title else "No title")

asyncio.run(main())
```

#### Full Spider Integration
```python
from silkworm import HTMLResponse, Request, Response, Spider
from silkworm.cdp import CDPClient

class LightpandaSpider(Spider):
    name = "lightpanda"
    start_urls = ("https://example.com/",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cdp_client = None

    async def start_requests(self) -> None:
        # Connect to CDP endpoint
        self._cdp_client = CDPClient(
            ws_endpoint="ws://127.0.0.1:9222",
            timeout=30.0
        )
        await self._cdp_client.connect()
        
        for url in self.start_urls:
            await self.follow(url, callback=self.parse)

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return
        
        # Extract links from JavaScript-rendered page
        for link in await response.select("a"):
            href = link.attr("href")
            if href:
                await self.emit({"url": href})

    async def close(self):
        if self._cdp_client:
            await self._cdp_client.close()
```

See `examples/lightpanda_simple.py` and `examples/lightpanda_spider.py` for complete working examples.

**Note:** CDP support is experimental. For production use, consider using dedicated browser automation tools or the standard HTTP client when JavaScript rendering is not required.

## Onion services with OnionLink
For Tor v3 `.onion` sites, install the optional OnionLink extra and pass `OnionLinkClient` as the spider HTTP client:

```bash
pip install "silkworm-rs[onionlink]"
```

```python
from silkworm import HTMLResponse, OnionLinkClient, Response, Spider, run_spider


class OnionSpider(Spider):
    name = "onion"
    start_urls = ("http://exampleexampleexampleexampleexampleexampleexampleexampleexampleexample.onion/",)

    async def parse(self, response: Response) -> None:
        if isinstance(response, HTMLResponse):
            title = await response.select_first("title")
            await self.emit({"title": title.text if title else ""})


run_spider(
    OnionSpider,
    http_client=OnionLinkClient(concurrency=4, timeout=30),
)
```

`OnionLinkClient` supports Silkworm `Request` headers, `params`, body/data, JSON payloads, redirects, HTML detection, and `request.meta["redirect_times"]`. Override OnionLink's response byte cap per request with `request.meta["onionlink_response_limit"]`.

## Logging and crawl statistics
- Structured logs via the standard library; set `SILKWORM_LOG_LEVEL=DEBUG` for verbose request/response/middleware output.
- Periodic statistics with `log_stats_interval`; final stats always include the close reason, elapsed time, queue size, requests/sec, seen URLs, items scraped and dropped, errors, retries, filtered requests, memory MB, and per-status/domain/error breakdowns. The same data is returned as a `CrawlResult` and can be served as Prometheus metrics (`metrics_port`).

## Limitations
- By default, HTTP fetches are wreq-based without JavaScript execution; pages requiring client-side rendering can use the optional CDP integration (see "JavaScript rendering with Lightpanda" section) or external browser automation tools. Tor v3 `.onion` sites can use the optional OnionLink integration.
- Request deduplication uses the request fingerprint (method, canonical URL with `params`, and body); headers and `meta` are ignored, so requests that differ only in headers are dropped unless you set `dont_filter=True` or pass a custom `dedup_key`.
- Redirects are followed inside the HTTP client, so `allowed_domains` filters requests before they are sent but a redirect can still land on another host.
- HTML parsing auto-detects encoding (BOM, HTTP headers/meta, charset detection fallback) but still enforces a `html_max_size_bytes`/`doc_max_size_bytes` cap (default 5 MB) in `scraper-rs` selectors, so very large pages may need a higher limit or preprocessing.
- Several pipelines buffer all items in memory until close (PolarsPipeline, ExcelPipeline, YAMLPipeline, AvroPipeline, VortexPipeline, S3JsonLinesPipeline, FTPPipeline, SFTPPipeline), which can bloat RAM on long crawls; prefer streaming pipelines like JsonLines/CSV/SQLite for high-volume runs.
- Many destination pipelines rely on optional extras; CassandraPipeline is disabled on Windows because `cassandra-driver` depends on libev there.

## Examples
- `python examples/quotes_spider.py` → `data/quotes.jl`
- `python examples/quotes_spider_trio.py` → `data/quotes_trio.jl` (demonstrates trio backend)
- `python examples/quotes_spider_winloop.py` → `data/quotes_winloop.jl` (demonstrates winloop backend for Windows)
- `python examples/hackernews_spider.py --pages 5` → `data/hackernews.jl`
- `python examples/lobsters_spider.py --pages 2` → `data/lobsters.jl`
- `python examples/start_urls_from_file_spider.py --urls-file data/start_urls.txt --output data/start_urls_from_file.jl` (reads one URL per line and schedules custom requests with `await self.follow(...)` in `start_requests`)
- `python examples/url_titles_spider.py --urls-file data/url_titles.jl --output data/titles.jl` (includes `SkipNonHTMLMiddleware` and stricter HTML size limits)
- `python examples/exception_handling_spider.py` → `data/exception_handling.jl` (demonstrates `process_exception` and request `errback`)
- `python examples/cookie_reuse_spiders.py` → `data/cookie_reuse.jl` and `data/cookies.txt` (captures cookies in one run, saves them, then loads them for a second run)
- `SILKWORM_LOG_LEVEL=DEBUG python examples/logging_controls_demo.py --mode noisy` then `--mode quiet` → demonstrates noisy pipeline/URL logging and the quieter `EngineLogger` + pipeline `log_level=None` setup
- `python examples/export_formats_demo.py --pages 2` → JSONL, XML, and CSV outputs in `data/`
- `python examples/taskiq_quotes_spider.py --pages 2` → demonstrates TaskiqPipeline for queue-based processing
- `python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml --pages 50` → `data/sitemap_meta.jl` (extracts meta tags and Open Graph data from sitemap URLs)
- `python examples/request_response_stream_spider.py --collector-url https://collector.example.com/events` → streams request/response telemetry while writing quotes output
- `CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=... python examples/cloudflare_crawl_spider.py https://example.com --limit 10` → submits a Cloudflare Browser Rendering crawl job
- `python examples/lightpanda_simple.py` → demonstrates CDP/Lightpanda for JavaScript rendering (requires `pip install silkworm-rs[cdp]` and running Lightpanda)
- `python examples/lightpanda_spider.py` → full spider example using CDP/Lightpanda
- `python examples/servo_spider.py` → full spider example using `ServoFetchClient` and a `servofetch` wheel

## Convenience API
For one-off fetches without a full spider:

### Standard HTTP fetch
```python
import asyncio
from silkworm import fetch_html

async def main():
    text, doc = await fetch_html("https://example.com")
    title = await doc.select_first("title")
    print(title.text if title else "No title")

asyncio.run(main())
```

### CDP-based fetch (with JavaScript rendering)
```python
import asyncio
from silkworm import fetch_html_cdp

async def main():
    # Requires Lightpanda/Chrome running with CDP enabled
    text, doc = await fetch_html_cdp("https://example.com")
    title = await doc.select_first("title")
    print(title.text if title else "No title")

asyncio.run(main())
```

### Servo-based fetch
```python
import asyncio
from silkworm import fetch_html_servo

async def main():
    # Requires a compatible servofetch wheel.
    text, doc = await fetch_html_servo("https://example.com", settle_ms=500)
    title = await doc.select_first("title")
    print(title.text if title else "No title")

asyncio.run(main())
```

### HTML to Markdown
```python
from silkworm import HTMLResponse, Response, Spider, html_to_markdown, stream_html_to_markdown

markdown = html_to_markdown("<h1>Hello</h1><p>World</p>", mode="minimal")
streamed = stream_html_to_markdown(["<h1>Hello</h1>", "<p>World</p>"])


class MarkdownSpider(Spider):
    name = "markdown"
    start_urls = ("https://example.com",)

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        await self.emit(
            {
                "url": response.url,
                "markdown": await response.to_markdown(mode="full"),
            }
        )
```

Modes are `full` for rich conversion, `minimal` for the lean Fast DOM path, and `mdream` for the mdream-backed converter. `to_markdown_result(...)` and `convert_html_to_markdown(...)` return `fast-h2m`'s structured result.

## Contributing
Pull requests and issues are welcome. To set up a dev environment, install [uv](https://docs.astral.sh/uv/getting-started/), create a Python 3.13 virtualenv, and sync dev dependencies:

```bash
uv venv --python python3.13
uv sync --group dev
```

Run the checks before opening a PR:

```bash
just fmt && just lint && just typecheck && just test
```

## Acknowledgements
Silkworm is built on top of excellent open-source projects:

- [wreq](https://github.com/0x676e67/wreq-python) - HTTP client with browser impersonation capabilities
- [onionlink](https://github.com/RustedBytes/onionlink-rs) - Tor v3 onion-service client
- [servofetch](https://github.com/RustedBytes/servofetch-py) - Bindings to the Servo browser
- [scraper-rs](https://github.com/RustedBytes/scraper-rs) - Fast HTML parsing library
- [fast-h2m](https://github.com/RustedBytes/fast-h2m) - Fast HTML-to-Markdown conversion
- [rxml](https://github.com/nephi-dev/rxml) - XML parsing and writing

We are grateful to the maintainers and contributors of these projects for their work.

## License
MIT License. See `LICENSE` for details.
