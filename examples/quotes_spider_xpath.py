"""
Example spider using XPath selectors instead of CSS selectors.

This demonstrates the XPath functionality integrated from scraper-rs.
"""

from pydantic import BaseModel, ValidationError, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline


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


class QuotesSpiderXPath(Spider):
    name = "quotes_xpath"
    start_urls = ("https://quotes.toscrape.com/",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger(component="QuotesSpiderXPath", spider=self.name)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response

        # Use XPath to select quote elements
        for el in await html.xpath("//div[@class='quote']"):
            try:
                # Use XPath on elements to get nested data
                text_el = el.xpath_first(".//span[@class='text']")
                author_el = el.xpath_first(".//span[@class='author']")

                if text_el is None or author_el is None:
                    self.log.warning("Skipping quote with missing fields")
                    continue

                # Extract tags using XPath
                tag_elements = el.xpath(".//a[@class='tag']")

                quote = Quote(
                    text=text_el.text,
                    author=author_el.text,
                    tags=[t.text for t in tag_elements],
                )

                self.log.debug(
                    "Scraped quote with XPath",
                    author=quote.author,
                    tags=len(quote.tags),
                )

                # Pipelines expect dict-like items
                yield quote.model_dump()
            except ValidationError as exc:
                self.log.warning("Skipping invalid quote", errors=exc.errors())
                continue

        # Use XPath to find the next page link
        next_link = await html.xpath_first("//li[@class='next']/a")
        if next_link:
            href = next_link.attr("href")
            if href:
                self.log.info("Following next page", href=href)
                yield html.follow(href, callback=self.parse)


if __name__ == "__main__":
    run_spider(
        QuotesSpiderXPath,
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[JsonLinesPipeline("data/quotes_xpath.jl")],
        request_timeout=10,
        log_stats_interval=10,
    )
