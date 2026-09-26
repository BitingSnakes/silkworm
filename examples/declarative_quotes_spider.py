"""
Scrape quotes by *describing* the data instead of writing selector code.

Compare this file with `quotes_spider.py`. There, we call `select_first()` for
every field by hand. Here, we declare a `Quote` class that says where each field
lives on the page, and Silkworm does the extraction for us.

What you will learn:
- How to define an `Item` with `Text(...)` and `Attr(...)` fields.
- How to extract all items from a page with `Quote.extract(response)`.

How to run:
    python examples/declarative_quotes_spider.py

Output:
    data/declarative-quotes.jl
"""

from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.declarative import Attr, Item, Text
from silkworm.middlewares import RetryMiddleware, SkipNonHTMLMiddleware
from silkworm.pipelines import JsonLinesPipeline


class Quote(Item):
    # Each element matching this CSS selector becomes one Quote.
    __selector__ = ".quote"

    # Text(selector) reads the text inside the matching element.
    text: str = Text(".text", strip=True)
    author: str = Text(".author", strip=True)

    # Attr(selector, attribute) reads an HTML attribute, here the link's "href".
    # absolute=True turns "/author/Albert-Einstein" into a full URL.
    author_url: str = Attr("a[href*='/author/']", "href", absolute=True)

    # Because the type is list[str], we get ALL matching tags, not just one.
    tags: list[str] = Text(".tag", strip=True)


class DeclarativeQuotesSpider(Spider):
    name = "declarative-quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        # Extract every Quote on the page and yield it as a dict.
        async for quote in Quote.extract(response):
            yield quote.to_dict()

        # Follow the "Next" button to the next page.
        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


if __name__ == "__main__":
    run_spider(
        DeclarativeQuotesSpider,
        response_middlewares=[
            SkipNonHTMLMiddleware(),  # Ignore images, PDFs, etc.
            RetryMiddleware(max_times=3),  # Retry failed requests.
        ],
        item_pipelines=[JsonLinesPipeline("data/declarative-quotes.jl")],
    )
