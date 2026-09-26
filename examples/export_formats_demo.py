"""
Save the same scraped data to several file formats at once.

You can give a spider more than one pipeline. Every item goes to all of them,
so one crawl can produce a JSON Lines file, an XML file and a CSV file together.

How to run:
    python examples/export_formats_demo.py            # scrape 2 pages
    python examples/export_formats_demo.py --pages 5  # scrape 5 pages

Output (in the data/ folder):
    quotes_demo.jl       JSON Lines
    quotes_demo.xml      XML
    quotes_demo.csv      CSV
    quotes_demo.msgpack  MessagePack (only if `silkworm-rs[msgpack]` is installed)
"""

from __future__ import annotations

import argparse

from silkworm import HTMLResponse, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import (
    ORMSGPACK_AVAILABLE,
    CSVPipeline,
    ItemPipeline,
    JsonLinesPipeline,
    MsgPackPipeline,
    XMLPipeline,
)


class ExportFormatsSpider(Spider):
    name = "export_formats"
    start_urls = ("https://quotes.toscrape.com/page/1/",)

    def __init__(self, max_pages: int = 2, **kwargs) -> None:
        # Always call the parent __init__ first when you override __init__.
        super().__init__(**kwargs)
        self.max_pages = max_pages
        self.pages_scraped = 0

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.pages_scraped += 1
        self.log.info("Parsing page", url=response.url, page=self.pages_scraped)

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            tag_els = await quote_el.select(".tag")
            if text_el is None or author_el is None:
                continue

            yield {
                "text": text_el.text.strip(),
                "author": author_el.text.strip(),
                "tags": [tag.text.strip() for tag in tag_els],
            }

        # Stop once we have scraped enough pages.
        if self.pages_scraped >= self.max_pages:
            self.log.info("Reached max pages", max_pages=self.max_pages)
            return

        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
    parser = argparse.ArgumentParser(description="Save quotes to several formats.")
    parser.add_argument(
        "--pages", type=int, default=2, help="Number of pages to scrape (default: 2)"
    )
    args = parser.parse_args()

    # Every item is sent to EVERY pipeline in this list.
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline("data/quotes_demo.jl", use_opendal=False),
        XMLPipeline(
            "data/quotes_demo.xml", root_element="quotes", item_element="quote"
        ),
        CSVPipeline("data/quotes_demo.csv", fieldnames=["author", "text", "tags"]),
    ]

    # MessagePack needs an optional extra package, so only add it if installed.
    if ORMSGPACK_AVAILABLE:
        pipelines.append(MsgPackPipeline("data/quotes_demo.msgpack"))

    print(f"Scraping {args.pages} page(s). Output files will be in data/")

    run_spider(
        ExportFormatsSpider(max_pages=args.pages),
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=pipelines,
        request_timeout=10,
    )

    print("Done! Check the data/ folder for the output files.")


if __name__ == "__main__":
    main()
