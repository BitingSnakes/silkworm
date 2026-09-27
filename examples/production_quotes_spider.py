"""
A production-style crawl: polite, validated, resumable, and able to fail loudly.

`quotes_spider.py` shows how to scrape. This example shows how to run a spider
unattended (from cron, CI, or a container) so that problems are noticed:

What you will learn:
- Obey robots.txt and adapt the crawl speed to the site (`RobotsTxtMiddleware`,
  `AutoThrottleMiddleware`), and retry flaky requests (`RetryMiddleware`).
- Stay on one site (`allowed_domains`) and bound the crawl (`max_depth`,
  `max_duration`).
- Validate items with Pydantic (`ValidationPipeline`) and declare what success
  means (`min_items`, `max_error_rate`, `max_item_drop_rate`): if the site's
  HTML changes and extraction breaks, the crawl raises `CrawlFailedError`.
- Save progress in a job directory so Ctrl+C (or a crash) can be resumed by
  running the script again.
- Read the returned `CrawlResult`.

How to run:
    python examples/production_quotes_spider.py
    python examples/production_quotes_spider.py --pages 2 --metrics-port 9410

    # The same spider from the command line:
    silkworm crawl examples/production_quotes_spider.py -o data/quotes.jl \\
        -s max_items=20

Output:
    data/production-quotes.jl, and a summary line; the exit status is 1 when
    the crawl fails its checks.
"""

from __future__ import annotations

import argparse
import sys

from pydantic import BaseModel, Field

from silkworm import CrawlFailedError, HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import (
    AutoThrottleMiddleware,
    RetryMiddleware,
    RobotsTxtMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline, ValidationPipeline


class Quote(BaseModel):
    """The shape every scraped item must have; invalid items are dropped."""

    text: str = Field(min_length=1)
    author: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=list[str])


class ProductionQuotesSpider(Spider):
    name = "production-quotes"
    start_urls = ("https://quotes.toscrape.com/",)
    # Links to any other site are dropped before they are requested.
    allowed_domains = ("quotes.toscrape.com",)
    # Defaults for this spider; `-s` options and SILKWORM_* variables can
    # override them (see docs/production.md#settings).
    custom_settings = {  # noqa: RUF012
        "request_timeout": 20,
        "concurrency": 4,
        "concurrency_per_domain": 2,
    }

    async def parse(self, response: Response) -> None:
        if not isinstance(response, HTMLResponse):
            return

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            # Emit what we found even if a field is missing: ValidationPipeline
            # drops invalid items and the drop rate is part of the crawl checks.
            await self.emit(
                {
                    "text": text_el.text.strip() if text_el else "",
                    "author": author_el.text.strip() if author_el else "",
                    "tags": [tag.text for tag in await quote_el.select(".tag")],
                }
            )

        next_link = await response.select_first("li.next > a")
        if next_link is not None and (href := next_link.attr("href")):
            await response.follow(href, callback=self.parse)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polite, validated, resumable quotes crawl."
    )
    parser.add_argument("--pages", type=int, default=3, help="pages to crawl (depth)")
    parser.add_argument("--job-dir", default="data/jobs/production-quotes")
    parser.add_argument("--metrics-port", type=int, default=None)
    args = parser.parse_args()

    # One throttle instance works as both a request and a response middleware.
    throttle = AutoThrottleMiddleware(start_delay=0.2, max_delay=10.0)
    try:
        result = run_spider(
            ProductionQuotesSpider,
            request_middlewares=[
                UserAgentMiddleware(),
                RobotsTxtMiddleware(),
                throttle,
            ],
            response_middlewares=[throttle, RetryMiddleware(max_times=3)],
            item_pipelines=[
                ValidationPipeline(Quote),
                JsonLinesPipeline("data/production-quotes.jl"),
            ],
            # Stop conditions: never wander deeper than --pages, never run long.
            max_depth=args.pages - 1,
            max_duration=300,
            # Success criteria: raise CrawlFailedError when they are not met.
            min_items=10,
            max_error_rate=0.2,
            max_item_drop_rate=0.2,
            # Ctrl+C stops gracefully; run again to continue where it stopped.
            job_dir=args.job_dir,
            metrics_port=args.metrics_port,
        )
    except CrawlFailedError as exc:
        print(f"Crawl failed: {'; '.join(exc.result.failures)}", file=sys.stderr)
        return 1

    print(
        f"{result.close_reason}: {result.items_scraped} quotes, "
        f"{result.items_dropped} dropped, {result.requests_sent} requests, "
        f"{result.errors} errors in {result.elapsed_seconds:.1f}s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
