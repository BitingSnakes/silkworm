# Silkworm

**Silkworm** is an async-first web scraping framework built on `wreq` (HTTP client with browser impersonation) and [scraper-rs](https://github.com/RustedBytes/scraper-rs) (fast HTML parsing). It provides a small, typed Spider/Request/Response model, middlewares, and pipelines so you can ship scrapers quickly without boilerplate.

## Features
- **Async engine** with configurable concurrency, bounded queue backpressure, request priorities, deduplication, and crawl statistics.
- **wreq-powered HTTP client** with browser impersonation, redirect handling, keep-alive, proxies, and HTML detection.
- **Typed spiders and callbacks** with `HTMLResponse` CSS/XPath selectors, `follow` helpers, and flexible callback outputs.
- **Declarative extraction** with `Item`, `Text`, and `Attr` field plans.
- **Middlewares** for user agents, proxies, cookies, delays, robots.txt, retries, telemetry streaming, and Cloudflare crawl jobs.
- **Pipelines** for files (JSON Lines, CSV, XML, Parquet, Excel, ...), databases, queues, and cloud storage.
- **Alternative fetchers** for JavaScript-rendered pages (CDP, Servo) and Tor onion services.
- **Structured logging** via the standard library, plus HTML-to-Markdown conversion.

## Quick Start
Install the package with `pip install silkworm-rs`, then run a spider. The example below mirrors [examples/quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/quotes_spider.py) and shows the core flow.

```python
from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        html = response
        for el in await html.select(".quote"):
            text_el = await el.select_first(".text")
            author_el = await el.select_first(".author")
            if text_el is None or author_el is None:
                continue
            tags = await el.select(".tag")
            yield {
                "text": text_el.text,
                "author": author_el.text,
                "tags": [t.text for t in tags],
            }

        if next_link := await html.select_first("li.next > a"):
            if href := next_link.attr("href"):
                yield html.follow(href, callback=self.parse)


run_spider(
    QuotesSpider,
    request_middlewares=[UserAgentMiddleware()],
    response_middlewares=[RetryMiddleware(max_times=3)],
    item_pipelines=[JsonLinesPipeline("data/quotes.jl")],
    concurrency=16,
    request_timeout=10,
    log_stats_interval=30,
)
```

> **Tip:** If you are new to Silkworm, read [Core Concepts](core-concepts.md) first, then use [Pipelines](pipelines.md) to pick your export format.

```{toctree}
:hidden:
:caption: User Guide

getting-started
core-concepts
declarative
engine-and-http
middlewares
pipelines
runners
logging-and-stats
```

```{toctree}
:hidden:
:caption: Recipes

examples
docker
```

```{toctree}
:hidden:
:caption: Reference

api/index
```

```{toctree}
:hidden:
:caption: Project

GitHub <https://github.com/BitingSnakes/silkworm>
PyPI <https://pypi.org/project/silkworm-rs/>
```
