from __future__ import annotations

import asyncio

from silkworm import HTMLResponse, Request, Response, Spider, get_logger
from silkworm.cdp import CDPClient
from silkworm.pipelines import JsonLinesPipeline

"""
Example spider using Lightpanda via CDP (Chrome DevTools Protocol).

Lightpanda is a lightweight browser that supports CDP, allowing you to scrape
JavaScript-rendered pages. This example demonstrates how to use the CDPClient
to fetch pages from Lightpanda.

Prerequisites:
1. Install silkworm with CDP support:
   pip install silkworm-rs[cdp]
   or
   uv pip install --prerelease=allow silkworm-rs[cdp]

2. Start Lightpanda with CDP enabled:
   lightpanda --remote-debugging-port=9222

   Or use another CDP-compatible browser like Chrome/Chromium:
   chromium --remote-debugging-port=9222 --headless

Usage:
   python examples/lightpanda_spider.py
"""


class LightpandaSpider(Spider):
    """
    Example spider using Lightpanda via CDP to scrape Wikipedia.

    This spider demonstrates:
    - Connecting to Lightpanda via CDP
    - Navigating to pages with JavaScript rendering
    - Extracting links from the rendered page
    """

    name = "lightpanda"
    start_urls = ("https://wikipedia.com/",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger(component="LightpandaSpider", spider=self.name)
        self._cdp_client: CDPClient | None = None

    async def start_requests(self):
        """Initialize CDP client and yield start requests."""
        # Create and connect to CDP endpoint
        self._cdp_client = CDPClient(
            ws_endpoint="ws://127.0.0.1:9222",
            timeout=30.0,
            html_max_size_bytes=10_000_000,
        )

        try:
            await self._cdp_client.connect()
            self.log.info("Connected to Lightpanda CDP endpoint")
        except Exception as exc:
            self.log.error("Failed to connect to CDP endpoint", error=str(exc))
            self.log.info(
                "Make sure Lightpanda is running with: lightpanda --remote-debugging-port=9222"
            )
            return

        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)

    async def parse(self, response: Response):
        """Parse the response and extract links."""
        if not isinstance(response, HTMLResponse):
            self.log.warning("Skipping non-HTML response", url=response.url)
            return

        html = response
        self.log.info("Parsing page", url=response.url)

        # Extract all links from the page
        links = await html.select("a")

        extracted_links = []
        for link in links[:20]:  # Limit to first 20 links for demo
            href = link.attr("href")
            if href and href.startswith("http"):
                extracted_links.append(href)
                self.log.debug("Found link", href=href)

        if extracted_links:
            yield {
                "source_url": response.url,
                "links": extracted_links,
                "link_count": len(extracted_links),
            }

    async def close(self):
        """Clean up CDP client when spider is done."""
        if self._cdp_client:
            await self._cdp_client.close()
            self.log.info("Closed CDP client")


async def main():
    """Run the spider with CDP client."""
    from silkworm import Engine

    spider = LightpandaSpider()
    pipelines = [JsonLinesPipeline("data/lightpanda_links.jl")]

    # Create custom engine that uses CDP client for fetching
    engine = Engine(
        spider=spider,
        item_pipelines=pipelines,
        request_timeout=30,
        log_stats_interval=10,
    )

    # Override the HTTP client with CDP client
    if spider._cdp_client:
        engine._http = spider._cdp_client  # type: ignore[assignment]

    try:
        await engine.run()
    finally:
        # Ensure cleanup
        if spider._cdp_client:
            await spider._cdp_client.close()


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Lightpanda CDP Spider Example")
    print("=" * 80)
    print("\nMake sure Lightpanda is running:")
    print("  lightpanda --remote-debugging-port=9222")
    print("\nOr use Chrome/Chromium:")
    print("  chromium --remote-debugging-port=9222 --headless")
    print("\nPress Ctrl+C to stop the spider.")
    print("=" * 80 + "\n")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nSpider stopped by user.")
    except Exception as exc:
        print(f"\n\nError: {exc}")
        import traceback

        traceback.print_exc()
