# silkworm-rs

![PyPI - Version](https://img.shields.io/pypi/v/silkworm-rs)

Async-first web scraping framework built on [rnet](https://github.com/0x676e67/rnet) for HTTP and [scraper-rs](https://github.com/RustedBytes/scraper-rs) for fast HTML parsing. Silkworm gives you a minimal, typed Spider/Request/Response model plus middlewares and pipelines so you can script simple scrapes or build more involved crawlers.

## Features
- Async engine with configurable concurrency and request timeouts.
- Typed spiders and callbacks that yield `Request` objects or scraped items.
- Request/response middlewares (user agent rotation, proxies, retry with backoff).
- Item pipelines for JSON Lines and SQLite out of the box.
- Structured logging via `logly` with optional `SILKWORM_LOG_LEVEL` override.

## Installation

Using PyPI:

```bash
pip install silkworm-rs
```

From source:

```bash
uv venv  # install uv from https://docs.astral.sh/uv/getting-started/ if needed
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
uv pip install -e .
```

Silkworm targets Python 3.10+. Dependencies are pinned in `pyproject.toml`.

## Quick start
Define a spider by subclassing `Spider`, implementing `parse`, and yielding items or follow-up `Request` objects. Then run it with `run_spider`.

```python
from silkworm import HTMLResponse, Spider, run_spider


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response: HTMLResponse):
        for quote in response.css(".quote"):
            yield {
                "text": quote.select(".text")[0].text,
                "author": quote.select(".author")[0].text,
                "tags": [t.text for t in quote.select(".tag")],
            }

        next_link = response.find("li.next > a")
        if next_link:
            yield response.follow(next_link.attr("href"), callback=self.parse)


if __name__ == "__main__":
    run_spider(QuotesSpider, request_timeout=10)
```

## Middlewares and pipelines
Silkworm ships with a few building blocks:
- `UserAgentMiddleware` rotates or defaults a User-Agent header.
- `ProxyMiddleware` cycles through a list of proxies.
- `RetryMiddleware` retries on common transient HTTP codes with backoff.
- `DelayMiddleware` adds configurable delays between requests for rate limiting or polite scraping.
- `JsonLinesPipeline` writes scraped items to a JSONL file.
- `SQLitePipeline` stores items in a SQLite table.
- `XMLPipeline` exports scraped items to an XML file.
- `CSVPipeline` exports scraped items to a CSV file.

Attach them when invoking `run_spider`:

```python
from silkworm.middlewares import DelayMiddleware, RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline

run_spider(
    QuotesSpider,
    request_middlewares=[
        UserAgentMiddleware(),
        DelayMiddleware(delay=1.0),  # Wait 1 second between requests
    ],
    response_middlewares=[RetryMiddleware(max_times=3)],
    item_pipelines=[JsonLinesPipeline("data/quotes.jl")],
    request_timeout=10,
)
```

### DelayMiddleware usage
The `DelayMiddleware` supports flexible delay strategies:

```python
# Fixed delay of 1 second between all requests
DelayMiddleware(delay=1.0)

# Random delay between 0.5 and 2 seconds
DelayMiddleware(min_delay=0.5, max_delay=2.0)

# Custom delay function based on request/spider
def smart_delay(request, spider):
    # Faster for API endpoints, slower for pages
    return 0.5 if '/api/' in request.url else 2.0

DelayMiddleware(delay_func=smart_delay)
```

Configure which status codes should back off (sleep) before retrying with
`RetryMiddleware`:

```python
# Sleep and retry on 403/429 responses (and retry other server errors immediately)
RetryMiddleware(max_times=3, sleep_http_codes=[403, 429])
```
Any code listed in `sleep_http_codes` is automatically retried even if it's not in
`retry_http_codes`.

### Pipeline usage
The built-in pipelines provide different export formats for your scraped data:

```python
from silkworm.pipelines import CSVPipeline, JsonLinesPipeline, SQLitePipeline, XMLPipeline

# JSON Lines - one JSON object per line
JsonLinesPipeline("data/quotes.jl")

# SQLite database
SQLitePipeline("data/quotes.db", table="quotes")

# XML format with customizable elements
XMLPipeline("data/quotes.xml", root_element="quotes", item_element="quote")

# CSV format with automatic field detection or custom fieldnames
CSVPipeline("data/quotes.csv")  # Auto-detect fields from first item
CSVPipeline("data/quotes.csv", fieldnames=["author", "text", "tags"])  # Custom order
```

The `XMLPipeline` automatically converts nested dictionaries and lists to proper XML structure. The `CSVPipeline` flattens nested dictionaries (e.g., `{"user": {"name": "Alice"}}` becomes `user_name`) and converts lists to comma-separated strings.

## Example spiders
- `python examples/quotes_spider.py` writes quotes to `data/quotes.jl`.
- `python examples/hackernews_spider.py --pages 5` (default 5) writes stories to `data/hackernews.jl`.
- `python examples/lobsters_spider.py --pages 2` (default 1) writes front-page stories to `data/lobsters.jl`.

Both examples include basic validation with Pydantic and demonstrate pagination, logging, middleware, and pipeline usage.

## Convenience API
For one-off fetches without a full spider, use `fetch_html`:

```python
import asyncio
from silkworm import fetch_html

async def main():
    text, doc = await fetch_html("https://example.com")
    print(doc.select("title")[0].text)

asyncio.run(main())
```

## Developer tips
- Callbacks can yield as usual or simply return a single `Request`/item (sync or async); the engine will normalize the result.
- `RetryMiddleware` now backs off with `asyncio.sleep` so other concurrent requests keep flowing.
- The internal HTTP client is closed when the crawl finishes to avoid dangling resources.

## Logging
Logs are emitted through `logly`. Set `SILKWORM_LOG_LEVEL=DEBUG` to see verbose output (e.g., headers, retries, middleware activity). Logs are flushed when the engine completes a crawl.

### Statistics logging
Enable periodic statistics logging during a crawl by passing `log_stats_interval` (in seconds) to `run_spider`:

```python
run_spider(
    QuotesSpider,
    log_stats_interval=30,  # Log statistics every 30 seconds
    request_timeout=10,
)
```

### HTML parsing limits
Cap the size of HTML parsed into `scraper-rs` documents by passing
`html_max_size_bytes` to `run_spider`/`crawl`:

```python
run_spider(QuotesSpider, html_max_size_bytes=10_000_000)
```

Statistics include:
- **elapsed_seconds**: Time since the crawl started
- **requests_sent**: Total number of requests sent
- **responses_received**: Total number of responses received
- **items_scraped**: Total number of items yielded by the spider
- **errors**: Total number of errors encountered
- **queue_size**: Current number of requests waiting to be processed
- **requests_per_second**: Average request rate

Final statistics are always logged when the spider completes, regardless of the `log_stats_interval` setting.

To keep memory usage predictable on very large crawls, Silkworm bounds the pending
request queue by default (`concurrency * 10`). Tune it with
`max_pending_requests` on `run_spider`/`crawl` (e.g., `max_pending_requests=100`)
to reduce how many requests are buffered at once.
