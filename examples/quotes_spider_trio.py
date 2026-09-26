"""
The quotes spider again, but running on the Trio async library.

Silkworm runs on asyncio by default. If your project uses Trio instead, the only
change is calling `run_spider_trio(...)` instead of `run_spider(...)`.
The spider code itself stays exactly the same.

Install Trio support first:
    pip install "silkworm-rs[trio]"

How to run:
    python examples/quotes_spider_trio.py

Output:
    data/quotes_trio.jl
"""

from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider_trio
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    name = "quotes_trio"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        # Scrape every quote on the page.
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

        # Go to the next page, if there is one.
        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


if __name__ == "__main__":
    # The only Trio-specific line in this file:
    run_spider_trio(
        QuotesSpider,
        concurrency=16,  # Download up to 16 pages at the same time.
        request_timeout=10,
        item_pipelines=[
            JsonLinesPipeline("data/quotes_trio.jl", use_opendal=False),
        ],
    )
