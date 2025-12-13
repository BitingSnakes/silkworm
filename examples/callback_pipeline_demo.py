"""
Example demonstrating the CallbackPipeline usage.

This shows how to use CallbackPipeline to process items with custom callback functions.
"""

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import UserAgentMiddleware
from silkworm.pipelines import CallbackPipeline


class QuotesSpider(Spider):
    name = "quotes_callback"
    start_urls = ("https://quotes.toscrape.com/page/1/",)

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

        # Only scrape first page for demo
        return


# Define callback functions for processing items
def print_item(item, spider):
    """Simple callback that prints each item."""
    print(f"[{spider.name}] Got quote by {item['author']}: {item['text'][:50]}...")
    return item


async def async_validate_item(item, spider):
    """Async callback that validates items."""
    # Could do async validation like checking against a database
    if len(item.get("text", "")) < 10:
        print(f"Warning: Short quote from {item['author']}")
    return item


def enrich_item(item, spider):
    """Callback that adds metadata to items."""
    item["spider_name"] = spider.name
    item["tag_count"] = len(item.get("tags", []))
    return item


if __name__ == "__main__":
    # You can chain multiple CallbackPipelines to process items in sequence
    run_spider(
        QuotesSpider,
        request_middlewares=[UserAgentMiddleware()],
        item_pipelines=[
            CallbackPipeline(callback=print_item),
            CallbackPipeline(callback=async_validate_item),
            CallbackPipeline(callback=enrich_item),
        ],
        concurrency=4,
        request_timeout=10,
    )
