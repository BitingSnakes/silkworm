"""
Send a copy of every request and response to a remote server while crawling.

`RequestResponseStreamMiddleware` records each request and response (URL,
status, headers, and part of the body) and POSTs them in batches to a
"collector" URL that you provide. This is handy for debugging or monitoring
a crawl from another service.

The same middleware object is added to BOTH middleware lists, because it needs
to see the requests going out and the responses coming back.

You need a collector URL. For a quick test you can use a request-bin service
such as https://webhook.site.

How to run:
    python examples/request_response_stream_spider.py --collector-url https://webhook.site/<your-id>

    # Or with environment variables:
    SILKWORM_STREAM_URL=https://... python examples/request_response_stream_spider.py

Output:
    data/request_response_stream_quotes.jl (the scraped quotes)
"""

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
from silkworm.pipelines import JsonLinesPipeline


class RequestResponseStreamSpider(Spider):
    """A normal quotes spider. It doesn't need to know about the streaming."""

    name = "request_response_stream"
    start_urls = ("https://quotes.toscrape.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        for quote_el in await response.select(".quote"):
            text_el = await quote_el.select_first(".text")
            author_el = await quote_el.select_first(".author")
            if text_el is None or author_el is None:
                continue

            yield {
                "text": text_el.text.strip(),
                "author": author_el.text.strip(),
            }

        next_link = await response.select_first("li.next > a")
        if next_link is not None:
            href = next_link.attr("href")
            if href:
                yield response.follow(href, callback=self.parse)


def main() -> None:
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
        help="How many events to send per collector request.",
    )
    parser.add_argument(
        "--body-limit",
        type=int,
        default=8_192,
        help="Maximum request/response body bytes to include in each event.",
    )
    args = parser.parse_args()

    if not args.collector_url:
        raise SystemExit(
            "Collector URL required. Pass --collector-url or set SILKWORM_STREAM_URL."
        )

    stream = RequestResponseStreamMiddleware(
        args.collector_url,
        auth_token=args.collector_token,  # Sent as "Authorization: Bearer ...".
        batch_size=args.batch_size,
        max_body_bytes=args.body_limit,  # Cut long bodies to keep events small.
    )

    run_spider(
        RequestResponseStreamSpider,
        # The same object in both lists: it sees requests AND responses.
        request_middlewares=[stream],
        response_middlewares=[stream],
        item_pipelines=[JsonLinesPipeline(args.output, use_opendal=False)],
        request_timeout=10,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
