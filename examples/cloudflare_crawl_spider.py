"""
Let Cloudflare crawl a website for you, then save the results.

Cloudflare's Browser Rendering service has a "/crawl" API: you give it a
starting URL, it crawls the site on Cloudflare's servers, and returns one
record per page. `CloudflareCrawlMiddleware` handles the API for you:
it starts the crawl job, waits until it is finished, and hands the final JSON
to your spider's `parse()` method as a normal response.

You need a Cloudflare account ID and an API token with Browser Rendering access.

How to run:
    export CLOUDFLARE_ACCOUNT_ID=...
    export CLOUDFLARE_API_TOKEN=...
    python examples/cloudflare_crawl_spider.py https://example.com --limit 10
    python examples/cloudflare_crawl_spider.py https://example.com --render --markdown

Output:
    data/cloudflare_crawl.jl  (one line per crawled page)
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from silkworm import Request, Response, Spider, run_spider
from silkworm.middlewares import CloudflareCrawlMiddleware
from silkworm.pipelines import JsonLinesPipeline


class CloudflareCrawlSpider(Spider):
    name = "cloudflare_crawl"

    def __init__(
        self,
        start_url: str,
        crawl_options: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.start_url = start_url
        self.crawl_options = crawl_options or {}

    async def start_requests(self):
        yield Request(
            url=self.start_url,
            callback=self.parse,
            # This meta key tells CloudflareCrawlMiddleware to handle the request.
            # Its value holds the crawl options (or True for the defaults).
            meta={"cloudflare_crawl": self.crawl_options or True},
            dont_filter=True,
        )

    async def parse(self, response: Response):
        data = json.loads(response.text)
        records = find_records(data)
        if records is None:
            self.log.warning(
                "No records found in Cloudflare response", url=response.url
            )
            return

        self.log.info("Got Cloudflare crawl results", records=len(records))

        # Each record describes one crawled page; save each one as an item.
        for record in records:
            if isinstance(record, dict):
                yield record


def find_records(data: object) -> list[Any] | None:
    """
    Find the list of page records in Cloudflare's JSON response.

    Depending on the API version, the list can be in a few places:
        {"result": {"records": [...]}}
        {"job": {"records": [...]}}
        {"records": [...]}
    """
    if not isinstance(data, dict):
        return None

    for container in (data.get("result"), data.get("job"), data):
        if isinstance(container, dict):
            records = container.get("records")
            if isinstance(records, list):
                return records
    return None


def build_crawl_options(args: argparse.Namespace) -> dict[str, Any]:
    """Turn the command-line flags into Cloudflare crawl options."""
    options: dict[str, Any] = {"render": args.render}

    if args.limit is not None:
        options["limit"] = args.limit
    if args.depth is not None:
        options["depth"] = args.depth

    formats = []
    if args.markdown:
        formats.append("markdown")
    if args.html:
        formats.append("html")
    if args.json_output:
        formats.append("json")
    if formats:
        options["formats"] = formats

    return options


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl a site through Cloudflare Browser Rendering /crawl."
    )
    parser.add_argument("url", help="Starting URL for the Cloudflare crawl job.")
    parser.add_argument(
        "--output", default="data/cloudflare_crawl.jl", help="Output JSON Lines path."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Maximum number of pages to crawl."
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Maximum number of links to follow deep.",
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="Render pages in a real browser (runs JavaScript).",
    )
    parser.add_argument(
        "--markdown", action="store_true", help="Include Markdown in the output."
    )
    parser.add_argument(
        "--html", action="store_true", help="Include HTML in the output."
    )
    parser.add_argument(
        "--json-output", action="store_true", help="Include structured JSON output."
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between 'is the job finished yet?' checks.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Maximum seconds to wait for the crawl job to finish.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Read secrets from environment variables, never hard-code them.
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not account_id or not api_token:
        raise SystemExit("Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN first.")

    cloudflare = CloudflareCrawlMiddleware(
        account_id=account_id,
        api_token=api_token,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )

    run_spider(
        CloudflareCrawlSpider(
            start_url=args.url,
            crawl_options=build_crawl_options(args),
        ),
        request_middlewares=[cloudflare],
        item_pipelines=[JsonLinesPipeline(args.output, use_opendal=False)],
        concurrency=1,  # We only send one crawl job.
        request_timeout=args.timeout,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
