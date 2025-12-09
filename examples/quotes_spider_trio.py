"""Example spider demonstrating trio support."""

from silkworm import HTMLResponse, Spider, run_spider_trio
from silkworm.pipelines import JsonLinesPipeline


class QuotesSpider(Spider):
    """Simple spider to scrape quotes using trio backend."""

    name = "quotes_trio"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response: HTMLResponse):
        """Parse quotes from the page."""
        for quote in response.css(".quote"):
            yield {
                "text": quote.select(".text")[0].text,
                "author": quote.select(".author")[0].text,
                "tags": [t.text for t in quote.select(".tag")],
            }

        next_link = response.find("li.next > a")
        if next_link:
            yield response.follow(next_link.attr("href"), callback=self.parse)


if __name__ == "__main__":
    # Run spider with trio backend
    # Install trio support: pip install silkworm-rs[trio]
    run_spider_trio(
        QuotesSpider,
        concurrency=16,
        request_timeout=10,
        item_pipelines=[
            JsonLinesPipeline("data/quotes_trio.jl"),
        ],
    )
