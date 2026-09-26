"""
Read the list of pages to crawl from a text file.

Instead of hard-coding `start_urls`, this spider overrides `start_requests()`
to read URLs from a file. That gives you full control over each Request, for
example to add headers or attach extra information with `meta`.

The input file has one URL per line. Blank lines and lines starting with "#"
are skipped:

    # my_urls.txt
    https://example.com/
    https://quotes.toscrape.com/

How to run:
    python examples/start_urls_from_file_spider.py --urls-file my_urls.txt

Output:
    data/start_urls_from_file.jl  (change it with --output)
"""

from __future__ import annotations

import argparse
from pathlib import Path

from silkworm import HTMLResponse, Request, Response, Spider, run_spider
from silkworm.middlewares import RetryMiddleware, UserAgentMiddleware
from silkworm.pipelines import JsonLinesPipeline


class StartUrlsFromFileSpider(Spider):
    name = "start_urls_from_file"

    def __init__(self, urls_file: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.urls_file = Path(urls_file)
        # Fail early with a clear message if the file doesn't exist.
        if not self.urls_file.exists():
            raise FileNotFoundError(f"URLs file not found: {self.urls_file}")

    async def start_requests(self):
        """Create one Request per URL in the file."""
        with self.urls_file.open("r", encoding="utf-8") as file:
            # enumerate(..., 1) gives us line numbers starting at 1.
            for line_number, line in enumerate(file, 1):
                url = line.strip()
                if not url or url.startswith("#"):
                    continue  # Skip blank lines and comments.

                yield Request(
                    url=url,
                    callback=self.parse_page,  # Which method handles the response.
                    headers={"Accept": "text/html,application/xhtml+xml"},
                    # `meta` is a dict that travels with the request, so we can
                    # read it back later from `response.request.meta`.
                    meta={"source_line": line_number},
                )

    async def parse_page(self, response: Response):
        # Read the page <title>, if this is an HTML page.
        title = ""
        if isinstance(response, HTMLResponse):
            title_el = await response.select_first("title")
            if title_el is not None:
                title = title_el.text.strip()

        yield {
            "url": response.url,
            "status": response.status,
            "title": title,
            "source_file": str(self.urls_file),
            "source_line": response.request.meta.get("source_line"),
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read start URLs from a text file and fetch each page.",
    )
    parser.add_argument(
        "--urls-file",
        required=True,
        help="Text file with one URL per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--output",
        default="data/start_urls_from_file.jl",
        help="Output JSON Lines path.",
    )
    args = parser.parse_args()

    run_spider(
        StartUrlsFromFileSpider(urls_file=args.urls_file),
        request_middlewares=[UserAgentMiddleware()],
        response_middlewares=[RetryMiddleware(max_times=3)],
        item_pipelines=[JsonLinesPipeline(args.output, use_opendal=False)],
        concurrency=16,
        request_timeout=10,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
