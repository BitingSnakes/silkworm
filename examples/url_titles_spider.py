"""
Fetch the <title> of many web pages listed in a JSON Lines file.

The input file has one JSON object per line, and each object needs a "url" key.
Any other keys are copied unchanged into the output:

    {"url": "https://example.com/", "source": "newsletter"}
    {"url": "https://python.org/"}

Tip: the output of `lobsters_spider.py` (data/lobsters.jl) works as input.

What you will learn:
- Reading input records in `start_requests()` and skipping bad lines.
- Passing data from a request to its response with `meta`.
- Tuning a spider for many different websites (high concurrency, short timeout).
- Choosing an event loop: asyncio (default), uvloop, or Trio.

How to run:
    python examples/url_titles_spider.py --urls-file data/lobsters.jl
    python examples/url_titles_spider.py --urls-file data/lobsters.jl --use_uvloop 1

Output:
    data/url_titles.jl  (change it with --output)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from silkworm import (
    HTMLResponse,
    Request,
    Response,
    Spider,
    run_spider,
    run_spider_trio,
    run_spider_uvloop,
)
from silkworm.middlewares import (
    RetryMiddleware,
    SkipNonHTMLMiddleware,
    UserAgentMiddleware,
)
from silkworm.pipelines import JsonLinesPipeline


class UrlTitlesSpider(Spider):
    name = "url_titles_from_file"

    def __init__(self, urls_file: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.urls_file = Path(urls_file)
        if not self.urls_file.exists():
            raise FileNotFoundError(f"URLs file not found: {self.urls_file}")

    def read_records(self):
        """
        Yield one dict per valid line of the input file.

        Bad lines (invalid JSON, not an object, or no "url") are logged and skipped.
        """
        with self.urls_file.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.log.warning(
                        "Skipping invalid JSON line",
                        line_number=line_number,
                        error=str(exc),
                    )
                    continue

                if not isinstance(record, dict):
                    self.log.warning(
                        "Skipping non-object line", line_number=line_number
                    )
                    continue

                url = str(record.get("url", "")).strip()
                if not url:
                    self.log.warning(
                        "Skipping line without url", line_number=line_number
                    )
                    continue

                record["url"] = url
                yield record

    async def start_requests(self):
        count = 0
        for record in self.read_records():
            count += 1
            yield Request(
                url=record["url"],
                callback=self.parse,
                # Keep the original record so we can copy its fields to the output.
                meta={"record": record},
                # Normally Silkworm skips URLs it has already seen.
                # dont_filter=True fetches every line, even duplicates.
                dont_filter=True,
            )
        self.log.info("Loaded URLs", count=count, path=str(self.urls_file))

    async def parse(self, response: Response):
        # Get back the record we attached in start_requests().
        record = response.request.meta.get("record")
        if not isinstance(record, dict):
            record = {}

        page_title = ""
        if isinstance(response, HTMLResponse):
            title_el = await response.select_first("title")
            if title_el is not None:
                page_title = title_el.text.strip()

        # `**record` copies all fields from the input line into the new dict.
        yield {
            **record,
            "page_title": page_title,
            "final_url": response.url,  # May differ from "url" after redirects.
            "status": response.status,
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch page titles for URLs listed in a JSONL file."
    )
    parser.add_argument(
        "--urls-file",
        type=str,
        required=True,
        help="Path to a JSON Lines file with one object per line that includes a 'url' field.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/url_titles.jl",
        help="Where to write scraped results (JSON Lines).",
    )
    parser.add_argument(
        "--use_trio",
        type=bool,
        default=False,
        help="Pass any non-empty value (e.g. 1) to run on the Trio event loop.",
    )
    parser.add_argument(
        "--use_uvloop",
        type=bool,
        default=False,
        help="Pass any non-empty value (e.g. 1) to run on the uvloop event loop.",
    )
    args = parser.parse_args()

    spider = UrlTitlesSpider(urls_file=args.urls_file)

    # These settings are the same whichever event loop we use.
    request_middlewares = [UserAgentMiddleware()]
    response_middlewares = [
        RetryMiddleware(max_times=3, sleep_http_codes=[403, 429]),
        SkipNonHTMLMiddleware(),  # Don't download PDFs, images, etc.
    ]
    item_pipelines = [JsonLinesPipeline(args.output, use_opendal=False)]

    # Pick the runner function based on the command-line flags.
    if args.use_trio:
        runner = run_spider_trio
    elif args.use_uvloop:
        runner = run_spider_uvloop
    else:
        runner = run_spider

    runner(
        spider,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        # Every URL is usually on a different website, so we can safely
        # download many pages at the same time.
        concurrency=128,
        request_timeout=5,  # Don't wait long for slow sites.
        log_stats_interval=10,
        html_max_size_bytes=1_000_000,  # Only parse the first 1 MB of HTML.
        keep_alive=True,  # Reuse network connections when possible.
    )
    print("Crawling completed.")


if __name__ == "__main__":
    main()
