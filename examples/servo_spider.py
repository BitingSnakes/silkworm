"""
Example spider using Servo rendering through servofetch.

ServoFetchClient embeds Servo via the optional servofetch package, so it does not
require a separate browser process like the CDP/Lightpanda examples.

Prerequisites:
   pip install "silkworm-rs[servo]"

Usage:
   python examples/servo_spider.py
"""

from __future__ import annotations

from typing import Any, cast

from silkworm import (
    HTMLResponse,
    Request,
    Response,
    ServoFetchClient,
    Spider,
    run_spider,
)
from silkworm.pipelines import JsonLinesPipeline


class ServoRenderedSpider(Spider):
    """
    Render a page with Servo and extract a small sample of links.
    """

    name = "servo_rendered"
    start_urls = ("https://wikipedia.com/",)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    async def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url=url,
                callback=self.parse,
                meta={
                    "servo_settle_ms": 500,
                    "servo_javascript": "document.title",
                },
            )

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        title_el = await response.select_first("title")
        links = []
        for link in await response.select("a"):
            href = link.attr("href")
            label = link.text.strip()
            if href and href.startswith("http"):
                links.append({"href": href, "label": label})
            if len(links) >= 20:
                break

        yield {
            "url": response.url,
            "title": title_el.text.strip() if title_el else "",
            "render_engine": response.headers.get("x-silkworm-render-engine"),
            "links": links,
            "link_count": len(links),
        }


def main() -> None:
    try:
        client = ServoFetchClient(
            concurrency=2,
            timeout=30.0,
            settle_ms=500,
            html_max_size_bytes=10_000_000,
        )
    except ImportError as exc:
        print(exc)
        return

    run_spider(
        ServoRenderedSpider,
        http_client=cast(Any, client),
        item_pipelines=[JsonLinesPipeline("data/servo_links.jl")],
        request_timeout=30,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
