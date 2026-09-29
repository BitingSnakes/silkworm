# silkworm-rs

[![PyPI - Version](https://img.shields.io/pypi/v/silkworm-rs)](https://pypi.org/project/silkworm-rs/)
[![Tests](https://github.com/BitingSnakes/silkworm/actions/workflows/tests.yml/badge.svg)](https://github.com/BitingSnakes/silkworm/actions/workflows/tests.yml)
[![Docs](https://github.com/BitingSnakes/silkworm/actions/workflows/docs.yml/badge.svg)](https://bitingsnakes.github.io/silkworm/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/silkworm-rs?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/silkworm-rs)

Silkworm is an async-first Python web scraping framework built on
[wreq](https://github.com/0x676e67/wreq-python) and
[scraper-rs](https://github.com/RustedBytes/scraper-rs). It combines a small,
typed Spider/Request/Response API with middleware, output pipelines, and
production crawl controls.

**[Documentation](https://bitingsnakes.github.io/silkworm/)** ·
**[Getting started](https://bitingsnakes.github.io/silkworm/getting-started.html)** ·
**[Examples](https://bitingsnakes.github.io/silkworm/examples.html)** ·
**[API reference](https://bitingsnakes.github.io/silkworm/api/index.html)**

## Highlights

- Async crawling with concurrency, priorities, request deduplication, timeouts,
  and deadlock-free queue backpressure.
- Browser-impersonating HTTP through wreq, with redirects, proxies, cookies,
  retries, throttling, and robots.txt support.
- Push-style typed callbacks using `await self.emit(...)` and
  `await response.follow(...)`.
- Async CSS/XPath selection and optional declarative `Item`, `Text`, and `Attr`
  extraction.
- File, database, cloud, queue, and message-stream pipelines, with batch
  processing available on every built-in pipeline.
- Production controls for failure policies, stop limits, pause/resume, caching,
  graceful shutdown, metrics, and structured crawl statistics.
- Optional CDP, Servo, and OnionLink clients for rendered or onion-service pages.

## Install

Silkworm supports Python 3.13–3.15.

```bash
pip install silkworm-rs
```

With uv:

```bash
uv add silkworm-rs
```

Integrations are installed as optional extras. For example:

```bash
pip install "silkworm-rs[rsloop,polars]"
```

See [Getting Started](https://bitingsnakes.github.io/silkworm/getting-started.html)
for the complete extras and Python-version compatibility table.

## Quick start

```python
from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        for quote in await response.select(".quote"):
            text = await quote.select_first(".text")
            author = await quote.select_first(".author")
            if text is not None and author is not None:
                await self.emit({"text": text.text, "author": author.text})

        next_link = await response.select_first("li.next > a")
        if next_link is not None and (href := next_link.attr("href")):
            await response.follow(href, callback=self.parse)


run_spider(
    QuotesSpider,
    item_pipelines=[JsonLinesPipeline("data/quotes.jl")],
)
```

Callbacks return `None`; they report items and requests with `emit` and `follow`,
which apply backpressure while the callback is running.

## Command line

Run a spider module or test its parser without writing a runner script:

```bash
silkworm crawl examples/quotes_spider.py -o data/quotes.jl -s max_items=100
silkworm parse https://quotes.toscrape.com/ --spider examples/quotes_spider.py
```

See the [CLI reference](https://bitingsnakes.github.io/silkworm/cli.html) for
configuration, output formats, and exit codes.

## Documentation

| Topic | Guide |
| --- | --- |
| Installation, extras, and first spider | [Getting Started](https://bitingsnakes.github.io/silkworm/getting-started.html) |
| Spider, request, response, selectors, and callbacks | [Core Concepts](https://bitingsnakes.github.io/silkworm/core-concepts.html) |
| Declarative item extraction | [Declarative Extraction](https://bitingsnakes.github.io/silkworm/declarative.html) |
| Engine, HTTP, CDP, Servo, and OnionLink | [Engine and HTTP Client](https://bitingsnakes.github.io/silkworm/engine-and-http.html) |
| Request and response middleware | [Middlewares](https://bitingsnakes.github.io/silkworm/middlewares.html) |
| Output destinations and batch processing | [Pipelines](https://bitingsnakes.github.io/silkworm/pipelines.html) |
| asyncio, rsloop, uvloop, winloop, and Trio | [Runners](https://bitingsnakes.github.io/silkworm/runners.html) |
| Resumable and observable crawls | [Production Crawling](https://bitingsnakes.github.io/silkworm/production.html) |
| Version changes | [Migration Guide](https://bitingsnakes.github.io/silkworm/migration.html) |
| Constraints and workarounds | [Limitations](https://bitingsnakes.github.io/silkworm/limitations.html) |

## Development

```bash
uv venv --python python3.13
uv sync --group dev
just fmt && just lint && just typecheck && just test
```

See the [development workflow](https://bitingsnakes.github.io/silkworm/getting-started.html#development-workflow)
and open an issue or pull request on GitHub.

## Acknowledgements

Silkworm builds on wreq, scraper-rs, fast-h2m, rxml, and optional integrations
including OnionLink and Servo. Thank you to their maintainers and contributors.

## License

MIT. See [LICENSE](LICENSE).
