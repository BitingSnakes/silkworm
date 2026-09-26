"""
Process scraped items with your own functions using CallbackPipeline.

A pipeline is something that receives every item your spider yields.
`CallbackPipeline` lets you write a plain function and use it as a pipeline,
without creating a pipeline class.

Pipelines run in order, like an assembly line: whatever one callback returns
is passed to the next one.

    spider -> print_item -> check_item -> add_extra_fields

How to run:
    python examples/callback_pipeline_demo.py

Output:
    Printed to the console (only the first page is scraped).
"""

from __future__ import annotations

from typing import Any

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import UserAgentMiddleware
from silkworm.pipelines import CallbackPipeline


class QuotesSpider(Spider):
    name = "quotes_callback"
    start_urls = ("https://quotes.toscrape.com/page/1/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            tag_els = await quote_el.select(".tag")

            yield {
                "text": text_el.text if text_el else "",
                "author": author_el.text if author_el else "",
                "tags": [tag.text for tag in tag_els],
            }

        # We don't follow the next page here, to keep the demo short.


# Each callback receives the item and the spider, and returns the item.
# A callback can be a normal function or an `async` function.
# (`item` is typed as `Any` because an item can be any JSON-like value.)


def print_item(item: Any, spider: Spider) -> Any:
    """Print a short preview of each quote."""
    print(f"[{spider.name}] Got quote by {item['author']}: {item['text'][:50]}...")
    return item


async def check_item(item: Any, spider: Spider) -> Any:
    """
    Warn about very short quotes.

    This one is `async`, so it could also `await` things like a database lookup.
    """
    if len(item["text"]) < 10:
        print(f"Warning: short quote from {item['author']}")
    return item


def add_extra_fields(item: Any, spider: Spider) -> Any:
    """Add a couple of extra fields to the item."""
    item["spider_name"] = spider.name
    item["tag_count"] = len(item["tags"])
    return item


if __name__ == "__main__":
    run_spider(
        QuotesSpider,
        request_middlewares=[UserAgentMiddleware()],
        item_pipelines=[
            # Items go through these one by one, top to bottom.
            CallbackPipeline(callback=print_item),
            CallbackPipeline(callback=check_item),
            CallbackPipeline(callback=add_extra_fields),
        ],
        concurrency=4,
        request_timeout=10,
    )
