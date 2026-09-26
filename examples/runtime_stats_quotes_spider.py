"""
Show your own numbers in Silkworm's crawl statistics log.

While a spider runs, Silkworm regularly logs "Crawl statistics" (requests sent,
items scraped, and so on). You can add your own numbers to that log by putting
them in `self.stats_payload`, which is a normal dict.

In this example we count pages, quotes and unique authors.

How to run:
    python examples/runtime_stats_quotes_spider.py

Output:
    data/runtime_stats_quotes.jl, plus stats in the log every 5 seconds.
"""

from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline


class RuntimeStatsQuotesSpider(Spider):
    name = "runtime_stats_quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, max_pages: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_pages = max_pages

        # Our own counters, stored as normal attributes.
        self.pages_crawled = 0
        self.quotes_seen = 0
        self.longest_quote_chars = 0
        self.authors: set[str] = set()  # A set keeps each author only once.

        self.update_stats()

    def update_stats(self) -> None:
        """Copy our counters into `stats_payload` so they appear in the log."""
        self.stats_payload["pages_crawled"] = self.pages_crawled
        self.stats_payload["quotes_seen"] = self.quotes_seen
        self.stats_payload["authors_seen"] = len(self.authors)
        self.stats_payload["longest_quote_chars"] = self.longest_quote_chars

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.pages_crawled += 1

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            if text_el is None or author_el is None:
                continue

            text = text_el.text.strip()
            author = author_el.text.strip()

            # Update our counters.
            self.quotes_seen += 1
            self.authors.add(author)
            self.longest_quote_chars = max(self.longest_quote_chars, len(text))

            yield {"text": text, "author": author, "page": self.pages_crawled}

        self.update_stats()
        self.log.info(
            "Processed quotes page",
            page=self.pages_crawled,
            authors_seen=len(self.authors),
        )

        # Stop after `max_pages` pages.
        if self.pages_crawled >= self.max_pages:
            return

        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
    run_spider(
        RuntimeStatsQuotesSpider(max_pages=3),
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[
            JsonLinesPipeline("data/runtime_stats_quotes.jl", use_opendal=False),
        ],
        request_timeout=10,
        log_stats_interval=5,  # Log statistics (including ours) every 5 seconds.
    )


if __name__ == "__main__":
    main()
