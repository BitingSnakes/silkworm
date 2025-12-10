"""
Example demonstrating TaskiqPipeline for streaming scraped items to a queue.

This example shows how to use TaskiqPipeline to send scraped items to a Taskiq
broker/queue for asynchronous processing, instead of writing directly to a file.

Usage:
    python examples/taskiq_quotes_spider.py

Requirements:
    pip install silkworm-rs taskiq
"""

from taskiq import InMemoryBroker  # type: ignore[import-not-found]

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import TaskiqPipeline

# Create a Taskiq broker
broker = InMemoryBroker()


# Define a task to process items
@broker.task
async def process_quote(item):
    """Process a scraped quote item."""
    logger = get_logger(component="QuoteProcessor")
    logger.info(
        "Processing quote",
        author=item.get("author"),
        text_length=len(item.get("text", "")),
        tags_count=len(item.get("tags", [])),
    )
    # In a real application, you might:
    # - Save to database
    # - Send to another service
    # - Perform data enrichment
    # - Apply transformations
    return item


class TaskiqQuotesSpider(Spider):
    name = "taskiq_quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, max_pages: int = 2, **kwargs):
        super().__init__(**kwargs)
        self.max_pages = max_pages
        self.pages_scraped = 0
        self.logger = get_logger(component="TaskiqQuotesSpider", spider=self.name)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        self.pages_scraped += 1
        self.log.info(
            "Parsing page",
            page_number=self.pages_scraped,
            url=response.url,
        )

        for quote in await html.select(".quote"):
            yield {
                "text": quote.select(".text")[0].text,
                "author": quote.select(".author")[0].text,
                "tags": [t.text for t in quote.select(".tag")],
            }

        # Follow next page link if we haven't reached max_pages
        if self.pages_scraped < self.max_pages:
            next_link = await html.select_first("li.next > a")
            if next_link:
                href = next_link.attr("href")
                if href:
                    yield html.follow(href, callback=self.parse)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape quotes and send to Taskiq queue"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=2,
        help="Maximum number of pages to scrape (default: 2)",
    )
    args = parser.parse_args()

    # Configure the pipeline to use Taskiq - pass the task directly
    pipeline = TaskiqPipeline(broker, task=process_quote)

    run_spider(
        TaskiqQuotesSpider,
        max_pages=args.pages,
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[pipeline],
        request_timeout=10,
        log_stats_interval=10,
        concurrency=8,
    )
