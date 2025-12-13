from pydantic import BaseModel, ValidationError, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
    # DelayMiddleware,
    # ProxyMiddleware,
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import (
    ItemPipeline,
    JsonLinesPipeline,
    # SQLitePipeline
)


class Quote(BaseModel):
    text: str
    author: str
    tags: list[str]

    @field_validator("text", "author")
    @classmethod
    def validate_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, value: list[str]) -> list[str]:
        cleaned = [tag.strip() for tag in value if tag.strip()]
        if not cleaned:
            raise ValueError("at least one tag required")
        return cleaned


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger(component="QuotesSpider", spider=self.name)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        for el in await html.select(".quote"):
            try:
                text_el = el.select_first(".text")
                author_el = el.select_first(".author")
                if text_el is None or author_el is None:
                    self.log.warning("Skipping quote with missing fields")
                    continue

                quote = Quote(
                    text=text_el.text,
                    author=author_el.text,
                    tags=[t.text for t in el.select(".tag")],
                )
                self.log.debug(
                    "Scraped quote",
                    author=quote.author,
                    tags=len(quote.tags),
                )
                # Pipelines expect dict-like items; ensure conversion regardless of pydantic version.
                yield quote.model_dump()
            except ValidationError as exc:
                self.log.warning("Skipping invalid quote", errors=exc.errors())
                continue

        next_link = await html.select_first("li.next > a")
        if next_link:
            href = next_link.attr("href")
            if href:
                yield html.follow(href, callback=self.parse)


if __name__ == "__main__":
    request_mw: list[RequestMiddleware] = [
        UserAgentMiddleware(),
        # ProxyMiddleware with round-robin selection (default)
        # ProxyMiddleware(proxies=["http://user:pass@proxy1:8080", "http://proxy2:8080"]),
        # ProxyMiddleware with random selection
        # ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"], random_selection=True),
        # ProxyMiddleware from file with random selection
        # ProxyMiddleware(proxy_file="proxies.txt", random_selection=True),
        # Add delay between requests to be polite to the server
        # DelayMiddleware(delay=0.5),  # Fixed 0.5s delay
        # DelayMiddleware(min_delay=0.3, max_delay=1.0),  # Random delay
    ]
    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3),
    ]
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/quotes.jl"),
        # SQLitePipeline("data/quotes.db", table="quotes"),
    ]

    run_spider(
        QuotesSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=10,
        log_stats_interval=10,  # Log statistics every 10 seconds
    )
