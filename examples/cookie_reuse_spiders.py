from __future__ import annotations

import argparse
import json
from pathlib import Path

from silkworm import CookiesMiddleware, Response, Spider, run_spider
from silkworm.middlewares import RequestMiddleware, ResponseMiddleware
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class CaptureCookiesSpider(Spider):
    name = "capture_cookies"

    async def parse(self, response: Response):
        yield {
            "phase": "capture",
            "url": response.url,
            "status": response.status,
            "set_cookie": response.headers.get("set-cookie"),
        }


class ReuseCookiesSpider(Spider):
    name = "reuse_cookies"

    async def parse(self, response: Response):
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            payload = {"body": response.text[:500]}

        yield {
            "phase": "reuse",
            "url": response.url,
            "status": response.status,
            "payload": payload,
            "sent_cookie": response.request.headers.get("Cookie"),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture cookies in one spider run and reuse them in another."
    )
    parser.add_argument(
        "--capture-url",
        default=(
            "https://httpbin.org/response-headers?"
            "Set-Cookie=demo_session%3Dsilkworm%3B%20Path%3D%2F"
        ),
        help="URL that returns a Set-Cookie header.",
    )
    parser.add_argument(
        "--reuse-url",
        default="https://httpbin.org/cookies",
        help="URL to request after loading the saved cookies.",
    )
    parser.add_argument(
        "--cookie-file",
        default="data/cookies.txt",
        help="Netscape/Mozilla cookie file path.",
    )
    parser.add_argument(
        "--output",
        default="data/cookie_reuse.jl",
        help="Output JSON Lines path for both spider runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cookie_file = Path(args.cookie_file)

    capture_cookies = CookiesMiddleware()
    request_middlewares: list[RequestMiddleware] = [capture_cookies]
    response_middlewares: list[ResponseMiddleware] = [capture_cookies]
    item_pipelines: list[ItemPipeline] = [
        JsonLinesPipeline(args.output, use_opendal=False),
    ]

    run_spider(
        CaptureCookiesSpider,
        start_urls=[args.capture_url],
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        concurrency=1,
        request_timeout=10,
    )
    capture_cookies.save(cookie_file)

    reuse_cookies = CookiesMiddleware()
    reuse_cookies.load(cookie_file)

    run_spider(
        ReuseCookiesSpider,
        start_urls=[args.reuse_url],
        request_middlewares=[reuse_cookies],
        response_middlewares=[reuse_cookies],
        item_pipelines=[JsonLinesPipeline(args.output, use_opendal=False)],
        concurrency=1,
        request_timeout=10,
    )


if __name__ == "__main__":
    main()
