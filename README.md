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

Targets Python 3.13+; dependencies are pinned in `pyproject.toml`.

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
        for quote in await html.select(".quote"):
            yield {
                "text": quote.select_first(".text").text,
                "author": quote.select_first(".author").text,
                "tags": [t.text for t in quote.select(".tag")],
            }

        if next_link := await html.select_first("li.next > a"):
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
- use `run_spider_winloop(...)` instead of `run_spider(...)` to run under winloop on Windows (requires `pip install silkworm-rs[winloop]`).

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
    CallbackPipeline,  # invoke a custom callback function on each item
    CSVPipeline,
    JsonLinesPipeline,
    MsgPackPipeline,  # requires: pip install silkworm-rs[msgpack]
    SQLitePipeline,
    XMLPipeline,
    TaskiqPipeline,  # requires: pip install silkworm-rs[taskiq]
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
    WebhookPipeline,  # sends items to webhook endpoints using rnet
    GoogleSheetsPipeline,  # requires: pip install silkworm-rs[gsheets]
    SnowflakePipeline,  # requires: pip install silkworm-rs[snowflake]
    FTPPipeline,  # requires: pip install silkworm-rs[ftp]
    SFTPPipeline,  # requires: pip install silkworm-rs[sftp]
    CassandraPipeline,  # requires: pip install silkworm-rs[cassandra]
    CouchDBPipeline,  # requires: pip install silkworm-rs[couchdb]
    DynamoDBPipeline,  # requires: pip install silkworm-rs[dynamodb]
)

run_spider(
    QuotesSpider,
    request_middlewares=[
        UserAgentMiddleware(),  # rotate/custom user agent
        DelayMiddleware(min_delay=0.3, max_delay=1.2),  # polite throttling
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
- `ProxyMiddleware` supports three modes:
  - **Round-robin (default)**: `ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"])` cycles through proxies in order.
  - **Random selection**: `ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"], random_selection=True)` randomly selects a proxy for each request.
  - **From file**: `ProxyMiddleware(proxy_file="proxies.txt")` loads proxies from a file (one proxy per line, blank lines ignored). Combine with `random_selection=True` for random selection from the file.
- `RetryMiddleware` backs off with `asyncio.sleep`; any status in `sleep_http_codes` is retried even if not in `retry_http_codes`.
- `SkipNonHTMLMiddleware` checks `Content-Type` and optionally sniffs the body (`sniff_bytes`) to avoid running HTML callbacks on binary/API responses.
- `JsonLinesPipeline` writes items to a local JSON Lines file and, when `opendal` is installed, appends asynchronously via the filesystem backend (`use_opendal=False` to stick to a regular file handle).
- `CSVPipeline` flattens nested dicts (e.g., `{"user": {"name": "Alice"}}` -> `user_name`) and joins lists with commas; `XMLPipeline` preserves nesting.
- `MsgPackPipeline` writes items in binary MessagePack format using [ormsgpack](https://github.com/aviramha/ormsgpack) for fast and compact serialization (requires `pip install silkworm-rs[msgpack]`).
- `TaskiqPipeline` sends items to a [Taskiq](https://taskiq-python.github.io/) queue for distributed processing (requires `pip install silkworm-rs[taskiq]`).
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
- `WebhookPipeline` sends items to webhook endpoints via HTTP POST/PUT using rnet (same HTTP client as the spider) with support for batching and custom headers.
- `GoogleSheetsPipeline` appends items to Google Sheets with automatic flattening of nested data structures (requires `pip install silkworm-rs[gsheets]` and service account credentials).
- `SnowflakePipeline` sends items to Snowflake data warehouse tables as JSON (requires `pip install silkworm-rs[snowflake]`).
- `FTPPipeline` writes items to an FTP server in JSON Lines format (requires `pip install silkworm-rs[ftp]`).
- `SFTPPipeline` writes items to an SFTP server in JSON Lines format with support for password or key-based authentication (requires `pip install silkworm-rs[sftp]`).
- `CassandraPipeline` sends items to Apache Cassandra database tables (requires `pip install silkworm-rs[cassandra]`).
- `CouchDBPipeline` sends items to CouchDB databases as documents (requires `pip install silkworm-rs[couchdb]`).
- `DynamoDBPipeline` sends items to AWS DynamoDB tables with automatic table creation (requires `pip install silkworm-rs[dynamodb]`).
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

## Performance optimization with winloop (Windows)
For Windows users who want improved async performance, enable winloop (a Windows-compatible alternative to uvloop):

```bash
pip install silkworm-rs[winloop]
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

## Limitations
- HTTP fetches are rnet-based only; there is no browser or JavaScript execution, so pages that require client-side rendering need external tooling.
- Request deduplication keys only on `Request.url`; query params, HTTP method, and body are ignored, so same-URL requests with different params/data are dropped unless you set `dont_filter=True` or make the URL unique yourself.
- HTML parsing auto-detects encoding (BOM, HTTP headers/meta, charset detection fallback) but still enforces a `html_max_size_bytes`/`doc_max_size_bytes` cap (default 5 MB) in `scraper-rs` selectors, so very large pages may need a higher limit or preprocessing.
- Several pipelines buffer all items in memory until close (PolarsPipeline, ExcelPipeline, YAMLPipeline, AvroPipeline, VortexPipeline, S3JsonLinesPipeline, FTPPipeline, SFTPPipeline), which can bloat RAM on long crawls; prefer streaming pipelines like JsonLines/CSV/SQLite for high-volume runs.
- Many destination pipelines rely on optional extras; CassandraPipeline is disabled on Windows because `cassandra-driver` depends on libev there.

## Examples
- `python examples/quotes_spider.py` → `data/quotes.jl`
- `python examples/quotes_spider_trio.py` → `data/quotes_trio.jl` (demonstrates trio backend)
- `python examples/quotes_spider_winloop.py` → `data/quotes_winloop.jl` (demonstrates winloop backend for Windows)
- `python examples/hackernews_spider.py --pages 5` → `data/hackernews.jl`
- `python examples/lobsters_spider.py --pages 2` → `data/lobsters.jl`
- `python examples/url_titles_spider.py --urls-file data/url_titles.jl --output data/titles.jl` (includes `SkipNonHTMLMiddleware` and stricter HTML size limits)
- `python examples/export_formats_demo.py --pages 2` → JSONL, XML, and CSV outputs in `data/`
- `python examples/taskiq_quotes_spider.py --pages 2` → demonstrates TaskiqPipeline for queue-based processing
- `python examples/sitemap_spider.py --sitemap-url https://example.com/sitemap.xml --pages 50` → `data/sitemap_meta.jl` (extracts meta tags and Open Graph data from sitemap URLs)

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

- [rnet](https://github.com/0x676e67/rnet) - HTTP client with browser impersonation capabilities
- [scraper-rs](https://github.com/RustedBytes/scraper-rs) - Fast HTML parsing library
- [logly](https://github.com/egorsmkv/logly) - Structured logging
- [rxml](https://github.com/RazrFalcon/rxml) - XML parsing and writing

We are grateful to the maintainers and contributors of these projects for their work.

## License
MIT License. See `LICENSE` for details.
