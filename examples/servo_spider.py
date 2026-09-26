"""
Render JavaScript pages with the Servo browser engine, built into Python.

Like `lightpanda_spider.py`, this spider uses a browser to run each page's
JavaScript before parsing it. The difference: `ServoFetchClient` embeds the
Servo engine through the `servofetch` package, so you don't need to start a
separate browser program.

Before running:
    pip install servofetch

How to run:
    python examples/servo_spider.py

Output:
    data/servo_links.jl
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
    name = "servo_rendered"
    start_urls = ("https://wikipedia.com/",)

    async def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url=url,
                callback=self.parse,
                meta={
                    # Wait 500 ms after the page loads, so scripts can finish.
                    "servo_settle_ms": 500,
                    # Run this JavaScript in the page after it loads.
                    "servo_javascript": "document.title",
                },
            )

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        title_el = await response.select_first("title")

        # Keep the first 20 absolute links, with their text.
        links = []
        for link_el in await response.select("a"):
            href = link_el.attr("href")
            if href and href.startswith("http"):
                links.append({"href": href, "label": link_el.text.strip()})
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
            concurrency=2,  # Render at most 2 pages at the same time.
            timeout=30.0,
            settle_ms=500,
            html_max_size_bytes=10_000_000,
        )
    except ImportError as exc:
        # Raised when the servofetch package isn't installed.
        print(exc)
        return

    run_spider(
        ServoRenderedSpider,
        # Use Servo instead of the normal HTTP client for every request.
        http_client=cast(Any, client),  # cast: ServoFetchClient works like HttpClient.
        item_pipelines=[JsonLinesPipeline("data/servo_links.jl")],
        request_timeout=30,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    main()
