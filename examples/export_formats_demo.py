"""Demo spider showing all export formats: JSON Lines, SQLite, XML, and CSV."""

from pydantic import BaseModel, field_validator

from silkworm import HTMLResponse, Spider, run_spider
from silkworm.logging import get_logger
from silkworm.middlewares import (
    RequestMiddleware,
    ResponseMiddleware,
    RetryMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import (
    CSVPipeline,
    ItemPipeline,
    JsonLinesPipeline,
    XMLPipeline,
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


class ExportFormatsSpider(Spider):
    name = "export_formats"
    start_urls = ["https://quotes.toscrape.com/page/1/"]

    def __init__(self, max_pages: int = 2, **kwargs):
        super().__init__(**kwargs)
        self.max_pages = max_pages
        self.pages_scraped = 0
        self.logger = get_logger(component="ExportFormatsSpider", spider=self.name)

    async def parse(self, response: HTMLResponse):
        self.logger.info(
            "Parsing page", url=response.url, pages_scraped=self.pages_scraped
        )

        for el in response.css(".quote"):
            try:
                quote = Quote(
                    text=el.select(".text")[0].text,
                    author=el.select(".author")[0].text,
                    tags=[t.text for t in el.select(".tag")],
                )
                self.logger.debug("Scraped quote", author=quote.author)
                yield (
                    quote.model_dump() if hasattr(quote, "model_dump") else quote.dict()
                )
            except Exception as exc:
                self.logger.warning("Skipping invalid quote", error=str(exc))
                continue

        self.pages_scraped += 1

        # Follow pagination up to max_pages
        if self.pages_scraped < self.max_pages:
            next_link = response.find("li.next > a")
            if next_link:
                href = next_link.attr("href")
                yield response.follow(href, callback=self.parse)
        else:
            self.logger.info("Reached max pages", max_pages=self.max_pages)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Demo spider showing all export formats"
    )
    parser.add_argument(
        "--pages", type=int, default=2, help="Number of pages to scrape (default: 2)"
    )
    args = parser.parse_args()

    request_mw: list[RequestMiddleware] = [
        UserAgentMiddleware(),
    ]
    response_mw: list[ResponseMiddleware] = [
        RetryMiddleware(max_times=3),
    ]

    # Export to multiple formats simultaneously
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/quotes_demo.jl"),
        XMLPipeline(
            "data/quotes_demo.xml", root_element="quotes", item_element="quote"
        ),
        CSVPipeline("data/quotes_demo.csv", fieldnames=["author", "text", "tags"]),
    ]

    print(f"Starting spider to scrape {args.pages} page(s)...")
    print("Exporting to:")
    print("  - data/quotes_demo.jl (JSON Lines)")
    print("  - data/quotes_demo.xml (XML)")
    print("  - data/quotes_demo.csv (CSV)")

    run_spider(
        ExportFormatsSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=10,
        max_pages=args.pages,
    )

    print("\nDone! Check the data/ directory for output files.")
