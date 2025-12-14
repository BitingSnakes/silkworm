"""Example spider demonstrating winloop support."""

from silkworm import HTMLResponse, Response, Spider, run_spider_winloop
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    """Simple spider to scrape quotes using winloop backend."""

    name = "quotes_winloop"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        """Parse quotes from the page."""
        if not isinstance(response, HTMLResponse):
            return

        html = response
        for quote in await html.select(".quote"):
            yield {
                "text": quote.select(".text")[0].text,
                "author": quote.select(".author")[0].text,
                "tags": [t.text for t in quote.select(".tag")],
            }

        next_link = await html.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield html.follow(href, callback=self.parse)


if __name__ == "__main__":
    # Run spider with winloop backend (optimized for Windows)
    # Install winloop support: pip install silkworm-rs[winloop]
    run_spider_winloop(
        QuotesSpider,
        concurrency=16,
        request_timeout=10,
        item_pipelines=[
            JsonLinesPipeline("data/quotes_winloop.jl"),
        ],
    )
