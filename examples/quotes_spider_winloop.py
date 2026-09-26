"""
The quotes spider again, but running on winloop (a faster event loop for Windows).

The only change compared to a normal spider is calling `run_spider_winloop(...)`
instead of `run_spider(...)`. The spider code itself stays exactly the same.
(On Linux/macOS, the equivalent is `run_spider_uvloop(...)`.)

Install winloop support first:
    pip install "silkworm-rs[winloop]"

How to run:
    python examples/quotes_spider_winloop.py

Output:
    data/quotes_winloop.jl
"""

from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider_winloop
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    name = "quotes_winloop"
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
    # The only winloop-specific line in this file:
    run_spider_winloop(
        QuotesSpider,
        concurrency=16,  # Download up to 16 pages at the same time.
        request_timeout=10,
        item_pipelines=[
            JsonLinesPipeline("data/quotes_winloop.jl", use_opendal=False),
        ],
    )
