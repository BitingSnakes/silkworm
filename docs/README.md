# Silkworm Documentation

**Silkworm** is an async-first web scraping framework built on [rnet](https://github.com/0x676e67/rnet) (HTTP client with browser impersonation) and [scraper-rs](https://github.com/RustedBytes/scraper-rs) (fast HTML parsing). It provides a small, typed Spider/Request/Response model, middlewares, and pipelines so you can ship scrapers quickly without boilerplate.

## Feature Map (Code Links)
- **Engine and concurrency**: async workers, queue backpressure, stats tracking in [src/silkworm/engine.py](../src/silkworm/engine.py)
- **HTTP client**: emulation, redirects, keep-alive, timeouts, HTML detection in [src/silkworm/http.py](../src/silkworm/http.py)
- **Core types**: Spider, Request, Response, HTMLResponse in [src/silkworm/spiders.py](../src/silkworm/spiders.py), [src/silkworm/request.py](../src/silkworm/request.py), [src/silkworm/response.py](../src/silkworm/response.py)
- **Middlewares**: request/response hooks in [src/silkworm/middlewares.py](../src/silkworm/middlewares.py)
- **Pipelines**: export formats and integrations in [src/silkworm/pipelines.py](../src/silkworm/pipelines.py)
- **Runner helpers**: asyncio, uvloop, winloop, trio entrypoints in [src/silkworm/runner.py](../src/silkworm/runner.py)
- **Logging**: structured logs via logly in [src/silkworm/logging.py](../src/silkworm/logging.py)
- **Convenience API**: one-off HTML fetch in [src/silkworm/api.py](../src/silkworm/api.py)
- **Examples**: real spiders in [examples/](../examples)

## Docs Index
- [Getting Started](getting-started.md)
- [Core Concepts](core-concepts.md)
- [Engine and HTTP Client](engine-and-http.md)
- [Middlewares](middlewares.md)
- [Pipelines](pipelines.md)
- [Runners](runners.md)
- [Logging and Stats](logging-and-stats.md)
- [Examples](examples.md)
- [API Reference](api-reference.md)

## Quick Start
The example below mirrors [examples/quotes_spider.py](../examples/quotes_spider.py) and shows the core flow.

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
            yield html.follow(next_link.attr("href"), callback=self.parse)


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
