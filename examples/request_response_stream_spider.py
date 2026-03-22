from __future__ import annotations

import argparse
import os

from silkworm import (
    HTMLResponse,
    RequestResponseStreamMiddleware,
    Response,
    Spider,
    run_spider,
)
from silkworm.middlewares import RequestMiddleware, ResponseMiddleware
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class RequestResponseStreamSpider(Spider):
    name = "request_response_stream"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        for quote in await html.select(".quote"):
            text_el = await quote.select_first(".text")
            author_el = await quote.select_first(".author")
            if text_el is None or author_el is None:
                continue

            yield {
                "text": text_el.text.strip(),
                "author": author_el.text.strip(),
            }

        next_link = await html.select_first("li.next > a")
        if next_link is None:
            return

        href = next_link.attr("href")
        if href:
            yield html.follow(href, callback=self.parse)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a spider while streaming request/response telemetry."
    )
    parser.add_argument(
        "--collector-url",
        default=os.environ.get("SILKWORM_STREAM_URL"),
        help="Remote collector URL. Defaults to SILKWORM_STREAM_URL.",
    )
    parser.add_argument(
        "--collector-token",
        default=os.environ.get("SILKWORM_STREAM_TOKEN"),
        help="Optional bearer token. Defaults to SILKWORM_STREAM_TOKEN.",
    )
    parser.add_argument(
        "--output",
        default="data/request_response_stream_quotes.jl",
        help="Output JSON Lines path for scraped items.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="How many telemetry events to send per collector request.",
    )
    parser.add_argument(
        "--body-limit",
        type=int,
        default=8_192,
        help="Maximum request/response body bytes to include in telemetry.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.collector_url:
        msg = "Collector URL required. Pass --collector-url or set SILKWORM_STREAM_URL."
        raise SystemExit(msg)

    stream = RequestResponseStreamMiddleware(
        args.collector_url,
        auth_token=args.collector_token,
        batch_size=args.batch_size,
        max_body_bytes=args.body_limit,
    )

    request_middlewares: list[RequestMiddleware] = [stream]
    response_middlewares: list[ResponseMiddleware] = [stream]
    item_pipelines: list[ItemPipeline] = [
        JsonLinesPipeline(args.output, use_opendal=False),
    ]

    run_spider(
        RequestResponseStreamSpider,
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        request_timeout=10,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
