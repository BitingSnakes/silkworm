"""
Your first Silkworm spider: scrape every quote from quotes.toscrape.com.

What you will learn:
- How a Spider works: it starts at `start_urls` and Silkworm calls `parse()`
  with each downloaded page.
- How to pick elements out of HTML with CSS selectors.
- How to `yield` scraped items (dicts) and follow-up requests (next page).
- How to validate data with Pydantic before saving it.
- How to plug in middlewares (retry, user agent) and pipelines (save to file).

How to run:
    python examples/quotes_spider.py

Output:
    data/quotes.jl  (JSON Lines: one JSON object per line)
"""

from __future__ import annotations

from pydantic import BaseModel, ValidationError, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import (
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class Quote(BaseModel):
    """
    The shape of one scraped quote.

    Pydantic checks the data for us. If a field is invalid, creating a `Quote`
    raises `ValidationError`, and we can skip that quote instead of saving bad data.
    """

    text: str
    author: str
    tags: list[str]

    @field_validator("text", "author")
    @classmethod
    def must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("tags")
    @classmethod
    def clean_tags(cls, value: list[str]) -> list[str]:
        # Remove extra spaces and drop empty tags.
        cleaned = [tag.strip() for tag in value if tag.strip()]
        if not cleaned:
            raise ValueError("at least one tag required")
        return cleaned


class QuotesSpider(Spider):
    # A short name used in logs.
    name = "quotes"

    # Silkworm downloads these pages first and passes each one to `parse()`.
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        # Only HTML pages have selector helpers like `select()`.
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        # Step 1: find every quote block on the page.
        # Each quote on the page looks like:
        #   <div class="quote">
        #     <span class="text">...</span>
        #     <small class="author">...</small>
        #     <a class="tag">...</a> <a class="tag">...</a>
        #   </div>
        for quote_el in await response.select(".quote"):
            # Step 2: find the parts inside this one quote block.
            # `select_first` returns the first match, or None if nothing matched.
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            tag_els = await quote_el.select(".tag")

            if text_el is None or author_el is None:
                self.log.warning("Skipping quote with missing fields")
                continue

            # Step 3: validate the data.
            try:
                quote = Quote(
                    text=text_el.text,
                    author=author_el.text,
                    tags=[tag.text for tag in tag_els],
                )
            except ValidationError as exc:
                self.log.warning("Skipping invalid quote", errors=exc.errors())
                continue

            # Step 4: yield the item. Pipelines expect plain dicts, so convert it.
            yield quote.model_dump()

        # Step 5: go to the next page, if there is one.
        # `follow()` turns a relative link like "/page/2/" into a full Request.
        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
    # Request middlewares change each request BEFORE it is sent.
    request_middlewares: list[RequestMiddleware] = [
        UserAgentMiddleware(),  # Sends a realistic browser User-Agent header.
        # Want to slow down to be polite to the website? Add a delay:
        #   DelayMiddleware(delay=0.5)                     # always wait 0.5s
        #   DelayMiddleware(min_delay=0.3, max_delay=1.0)  # wait a random time
        # Want to use proxies?
        #   ProxyMiddleware(proxies=["http://proxy1:8080", "http://proxy2:8080"])
        # (import DelayMiddleware / ProxyMiddleware from silkworm.middlewares)
    ]

    # Response middlewares look at each response AFTER it arrives.
    response_middlewares: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3),  # Retry failed requests up to 3 times.
    ]

    # Pipelines receive every item your spider yields, e.g. to save it.
    item_pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/quotes.jl", use_opendal=False),
        # Want a database instead? Try:
        #   SQLitePipeline("data/quotes.db", table="quotes")
    ]

    run_spider(
        QuotesSpider,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        request_timeout=10,  # Give up on a request after 10 seconds.
        log_stats_interval=10,  # Print crawl statistics every 10 seconds.
    )


if __name__ == "__main__":
    main()
