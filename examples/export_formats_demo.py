"""Demo spider showing all export formats: JSON Lines, SQLite, XML, and CSV."""

from pydantic import BaseModel, field_validator

from silkworm import HTMLResponse, Response, Spider, run_spider
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

# Try to import MsgPackPipeline (requires optional dependency)
try:
    from silkworm.pipelines import MsgPackPipeline

    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False


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
    start_urls = ("https://quotes.toscrape.com/page/1/",)

    def __init__(self, max_pages: int = 2, **kwargs):
        super().__init__(**kwargs)
        self.max_pages = max_pages
        self.pages_scraped = 0
        self.logger = get_logger(component="ExportFormatsSpider", spider=self.name)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        self.log.info("Parsing page", url=html.url, pages_scraped=self.pages_scraped)

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
                self.log.debug("Scraped quote", author=quote.author)
                yield quote.model_dump()
            except Exception as exc:
                self.log.warning("Skipping invalid quote", error=str(exc))
                continue

        self.pages_scraped += 1

        # Follow pagination up to max_pages
        if self.pages_scraped < self.max_pages:
            next_link = await html.select_first("li.next > a")
            if next_link:
                href = next_link.attr("href")
                if href:
                    yield html.follow(href, callback=self.parse)
        else:
            self.log.info("Reached max pages", max_pages=self.max_pages)


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

    # Add MsgPack pipeline if available
    if MSGPACK_AVAILABLE:
        pipelines.append(MsgPackPipeline("data/quotes_demo.msgpack"))

    print(f"Starting spider to scrape {args.pages} page(s)...")
    print("Exporting to:")
    print("  - data/quotes_demo.jl (JSON Lines)")
    print("  - data/quotes_demo.xml (XML)")
    print("  - data/quotes_demo.csv (CSV)")
    if MSGPACK_AVAILABLE:
        print("  - data/quotes_demo.msgpack (MsgPack)")

    run_spider(
        ExportFormatsSpider,
        request_middlewares=request_mw,
        response_middlewares=response_mw,
        item_pipelines=pipelines,
        request_timeout=10,
        max_pages=args.pages,
    )

    print("\nDone! Check the data/ directory for output files.")
