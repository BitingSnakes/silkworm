from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from typing import Any

from silkworm import Response, Spider, run_spider
from silkworm.middlewares import CloudflareCrawlMiddleware, RequestMiddleware
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline
from silkworm.request import Request


class CloudflareCrawlSpider(Spider):
    """
    Submit a single Cloudflare Browser Rendering crawl job and emit one item per page.
    """

    name = "cloudflare_crawl"

    def __init__(
        self,
        *,
        start_url: str,
        crawl_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.start_url = start_url
        self.crawl_options = crawl_options or {}

    async def start_requests(self):
        yield Request(
            url=self.start_url,
            callback=self.parse,
            meta={"cloudflare_crawl": self.crawl_options or True},
            dont_filter=True,
        )

    async def parse(self, response: Response):
        payload = json.loads(response.text)
        result = payload.get("result")
        if not isinstance(result, Mapping):
            self.log.warning(
                "Cloudflare crawl payload missing result", url=response.url
            )
            return

        pages = result.get("pages")
        if not isinstance(pages, list):
            self.log.warning("Cloudflare crawl payload missing pages", url=response.url)
            return

        self.log.info(
            "Processing Cloudflare crawl payload",
            url=response.url,
            pages=len(pages),
        )

        for page in pages:
            if not isinstance(page, Mapping):
                continue
            yield dict(page)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl a site through Cloudflare Browser Rendering /crawl."
    )
    parser.add_argument(
        "url",
        help="Starting URL for the Cloudflare crawl job.",
    )
    parser.add_argument(
        "--output",
        default="data/cloudflare_crawl.jl",
        help="Output JSON Lines path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of pages to crawl.",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Optional maximum crawl depth.",
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="Use browser rendering instead of static mode.",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Request Markdown in the crawl output when supported.",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        help="Request HTML in the crawl output when supported.",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Request structured JSON output when supported.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between Cloudflare job status polls.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Maximum time to wait for the crawl job to finish.",
    )
    return parser.parse_args()


def build_crawl_options(args: argparse.Namespace) -> dict[str, Any]:
    crawl_options: dict[str, Any] = {"render": args.render}
    if args.limit is not None:
        crawl_options["limit"] = args.limit
    if args.depth is not None:
        crawl_options["maxDepth"] = args.depth

    formats: list[str] = []
    if args.markdown:
        formats.append("markdown")
    if args.html:
        formats.append("html")
    if args.json_output:
        formats.append("json")
    if formats:
        crawl_options["formats"] = formats

    return crawl_options


def main() -> None:
    args = parse_args()
    account_id = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    api_token = os.environ["CLOUDFLARE_API_TOKEN"]
    crawl_options = build_crawl_options(args)

    request_mw: list[RequestMiddleware] = [
        CloudflareCrawlMiddleware(
            account_id=account_id,
            api_token=api_token,
            poll_interval=args.poll_interval,
            timeout=args.timeout,
        )
    ]
    pipelines: list[ItemPipeline] = [
        JsonLinesPipeline(args.output, use_opendal=False),
    ]

    run_spider(
        CloudflareCrawlSpider,
        start_url=args.url,
        crawl_options=crawl_options,
        request_middlewares=request_mw,
        item_pipelines=pipelines,
        concurrency=1,
        request_timeout=args.timeout,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
