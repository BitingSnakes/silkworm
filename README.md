Silkworm
========

Async-first web scraping framework built on [rnet](https://github.com/0x676e67/rnet) for HTTP and [scraper-rs](https://github.com/RustedBytes/scraper-rs) for fast HTML parsing. Silkworm gives you a minimal, typed Spider/Request/Response model plus middlewares and pipelines so you can script simple scrapes or build more involved crawlers.

## Features
- Async engine with configurable concurrency and request timeouts.
- Typed spiders and callbacks that yield `Request` objects or scraped items.
- Request/response middlewares (user agent rotation, proxies, retry with backoff).
- Item pipelines for JSON Lines and SQLite out of the box.
- Structured logging via `logly` with optional `SILKWORM_LOG_LEVEL` override.

## Installation
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -e .
```

Silkworm targets Python 3.10+. Dependencies are pinned in `pyproject.toml`; the `scraper-rs` dependency is pulled from GitHub.

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
- `JsonLinesPipeline` writes scraped items to a JSONL file.
- `SQLitePipeline` stores items in a SQLite table.

Attach them when invoking `run_spider`:

```python
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline

run_spider(
    QuotesSpider,
    request_middlewares=[UserAgentMiddleware()],
    response_middlewares=[RetryMiddleware(max_times=3)],
    item_pipelines=[JsonLinesPipeline("data/quotes.jl")],
    request_timeout=10,
)
```

## Example spiders
- `python examples/quotes_spider.py` writes quotes to `data/quotes.jl`.
- `python examples/hackernews_spider.py --pages 5` (default 5) writes stories to `data/hackernews.jl`.

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

## Logging
Logs are emitted through `logly`. Set `SILKWORM_LOG_LEVEL=DEBUG` to see verbose output (e.g., headers, retries, middleware activity). Logs are flushed when the engine completes a crawl.
