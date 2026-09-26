"""
Send scraped items to a task queue (Taskiq) instead of writing them to a file.

Why use a queue? Scraping and processing are then separated: the spider only
downloads pages, and one or more workers process the items (save to a database,
call another API, ...) at their own pace.

This demo uses Taskiq's `InMemoryBroker`, so everything runs in one process and
you don't need Redis or RabbitMQ. In a real project you would swap it for a
real broker.

Install first:
    pip install "silkworm-rs[taskiq]"

How to run:
    python examples/taskiq_quotes_spider.py            # scrape 2 pages
    python examples/taskiq_quotes_spider.py --pages 5
"""

from __future__ import annotations

import argparse

from taskiq import InMemoryBroker  # type: ignore[import-not-found]

from silkworm import HTMLResponse, Response, Spider, get_logger, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import TaskiqPipeline

# Step 1: create a broker. The broker is the "queue" that holds tasks.
broker = InMemoryBroker()


# Step 2: define a task. Taskiq calls this function once for every item.
@broker.task
async def process_quote(item):
    """Process one scraped quote."""
    logger = get_logger(component="QuoteProcessor")
    logger.info(
        "Processing quote",
        author=item.get("author"),
        text_length=len(item.get("text", "")),
    )
    # In a real project you might save it to a database here.
    return item


# Step 3: a normal spider. It doesn't know anything about Taskiq.
class TaskiqQuotesSpider(Spider):
    name = "taskiq_quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, max_pages: int = 2, **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_pages = max_pages
        self.pages_scraped = 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.pages_scraped += 1
        self.log.info("Parsing page", page=self.pages_scraped, url=response.url)

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            if text_el is None or author_el is None:
                continue

            tag_els = await quote_el.select(".tag")
            yield {
                "text": text_el.text,
                "author": author_el.text,
                "tags": [tag.text for tag in tag_els],
            }

        # Stop after `max_pages` pages.
        if self.pages_scraped >= self.max_pages:
            return

        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape quotes into a Taskiq queue")
    parser.add_argument(
        "--pages",
        type=int,
        default=2,
        help="Maximum number of pages to scrape (default: 2)",
    )
    args = parser.parse_args()

    # Step 4: connect the spider to the queue with TaskiqPipeline.
    # Every item the spider yields is sent to `process_quote`.
    pipeline = TaskiqPipeline(broker, task=process_quote)

    run_spider(
        TaskiqQuotesSpider(max_pages=args.pages),
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[pipeline],
        request_timeout=10,
        log_stats_interval=10,
        concurrency=8,
    )


if __name__ == "__main__":
    main()
