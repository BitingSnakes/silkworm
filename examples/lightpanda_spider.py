"""
Run a whole spider through a headless browser, for JavaScript-heavy websites.

Normally Silkworm downloads pages with a plain HTTP client. Here we swap that
client for `CDPClient`, which asks a real (headless) browser to load each page
and run its JavaScript. The spider code itself doesn't change at all: it still
receives an `HTMLResponse` in `parse()`.

See `lightpanda_simple.py` for a smaller example that fetches just one page.

Before running:
1. Install CDP support:
       pip install "silkworm-rs[cdp]"
2. Start a browser with CDP enabled, in another terminal:
       lightpanda --remote-debugging-port=9222
   or:
       chromium --headless --remote-debugging-port=9222

How to run:
    python examples/lightpanda_spider.py

Output:
    data/lightpanda_links.jl
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from silkworm import HTMLResponse, Response, Spider, crawl
from silkworm.cdp import CDPClient
from silkworm.exceptions import HttpError
from silkworm.pipelines import JsonLinesPipeline

BROWSER_URL = "ws://127.0.0.1:9222"


class LightpandaSpider(Spider):
    name = "lightpanda"
    start_urls = ("https://wikipedia.com/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        self.log.info("Parsing page", url=response.url)

        # Keep the first 20 absolute links on the page.
        links = []
        for link_el in await response.select("a"):
            href = link_el.attr("href")
            if href and href.startswith("http"):
                links.append(href)
            if len(links) >= 20:
                break

        yield {
            "source_url": response.url,
            "links": links,
            "link_count": len(links),
        }


async def main() -> None:
    # Step 1: create the browser client and connect to the browser.
    client = CDPClient(
        ws_endpoint=BROWSER_URL,
        timeout=30.0,
        html_max_size_bytes=10_000_000,
    )
    try:
        await client.connect()
    except HttpError as exc:
        print(f"Could not connect to the browser at {BROWSER_URL}: {exc}")
        print("Start it with: lightpanda --remote-debugging-port=9222")
        return

    # Step 2: run the spider with `http_client=client`, so every page is
    # loaded by the browser. Silkworm closes the client when the crawl ends.
    # We use `crawl()` (the async version of run_spider) because we are
    # already inside an async function.
    await crawl(
        LightpandaSpider,
        http_client=cast(Any, client),  # cast: CDPClient works like HttpClient.
        item_pipelines=[JsonLinesPipeline("data/lightpanda_links.jl")],
        request_timeout=30,
        log_stats_interval=10,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Spider stopped by user.")
