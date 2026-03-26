from __future__ import annotations

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class RuntimeStatsQuotesSpider(Spider):
    """
    Example spider that publishes custom crawl stats at runtime.

    The engine keeps its own reserved metrics, while the spider can expose
    additional fields through ``self.stats_payload``. Those keys are merged into
    periodic and final "Crawl statistics" log entries.
    """

    name = "runtime_stats_quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, max_pages: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_pages = max(1, max_pages)
        self.pages_seen = 0
        self._authors_seen: set[str] = set()
        self.logger = get_logger(component="RuntimeStatsQuotesSpider", spider=self.name)

        self.stats_payload.update(
            {
                "pages_crawled": 0,
                "quotes_seen": 0,
                "authors_seen": 0,
                "longest_quote_chars": 0,
                "oldest_page_seen": 0,
            }
        )

    @staticmethod
    def _stat_int(value: object) -> int:
        return value if isinstance(value, int) else 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        self.pages_seen += 1
        self.stats_payload["pages_crawled"] = self.pages_seen
        self.stats_payload["oldest_page_seen"] = self.pages_seen

        quotes_on_page = 0
        for quote in await html.select(".quote"):
            text_el = await quote.select_first(".text")
            author_el = await quote.select_first(".author")
            if text_el is None or author_el is None:
                continue

            quotes_on_page += 1
            text = text_el.text.strip()
            author = author_el.text.strip()
            self._authors_seen.add(author)

            total_quotes = self._stat_int(self.stats_payload["quotes_seen"]) + 1
            self.stats_payload["quotes_seen"] = total_quotes
            self.stats_payload["authors_seen"] = len(self._authors_seen)
            self.stats_payload["longest_quote_chars"] = max(
                self._stat_int(self.stats_payload["longest_quote_chars"]),
                len(text),
            )

            yield {
                "text": text,
                "author": author,
                "page": self.pages_seen,
            }

        self.log.info(
            "Processed quotes page",
            page=self.pages_seen,
            quotes_on_page=quotes_on_page,
            authors_seen=self.stats_payload["authors_seen"],
        )

        next_link = await html.select_first("li.next > a")
        if next_link is None or self.pages_seen >= self.max_pages:
            return

        href = next_link.attr("href")
        if href:
            yield html.follow(href, callback=self.parse)


def main() -> None:
    request_mw: list[RequestMiddleware] = [UserAgentMiddleware()]
    response_mw: list[ResponseMiddleware] = [RetryMiddleware(max_times=3)]
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/runtime_stats_quotes.jl", use_opendal=False),
    ]

    run_spider(
        RuntimeStatsQuotesSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=10,
        log_stats_interval=5,
        max_pages=3,
    )


if __name__ == "__main__":
    main()
