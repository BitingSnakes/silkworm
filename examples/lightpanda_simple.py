from __future__ import annotations

import asyncio

from silkworm import fetch_html_cdp

"""
Simple example using Lightpanda CDP client - similar to the Node.js example.

This example closely follows the Node.js Puppeteer example from the issue:
- Connect to Lightpanda CDP endpoint
- Navigate to a page
- Extract links
- Cleanup

Prerequisites:
1. Install silkworm with CDP support:
   pip install silkworm-rs[cdp]

2. Start Lightpanda with CDP enabled:
   lightpanda --remote-debugging-port=9222

Usage:
   python examples/lightpanda_simple.py
"""


async def main():
    """
    Simple example demonstrating CDP usage similar to the Node.js Puppeteer example.
    """
    print("Connecting to Lightpanda at ws://127.0.0.1:9222...")

    try:
        # Fetch HTML using CDP (similar to puppeteer.connect + page.goto)
        text, doc = await fetch_html_cdp(
            "https://wikipedia.com/",
            ws_endpoint="ws://127.0.0.1:9222",
            timeout=30.0,
        )

        print(f"\nSuccessfully fetched page ({len(text)} bytes)")

        # Extract all links (similar to page.evaluate)
        links = []
        for element in doc.select("a"):
            href = element.attr("href")
            if href:
                links.append(href)

        print(f"\nFound {len(links)} links:")
        for i, link in enumerate(links[:10], 1):  # Show first 10 links
            print(f"  {i}. {link}")

        if len(links) > 10:
            print(f"  ... and {len(links) - 10} more")

    except ImportError:
        print("\nError: websockets package not installed.")
        print("Install with: pip install silkworm-rs[cdp]")
    except Exception as exc:
        print(f"\nError: {exc}")
        print("\nMake sure Lightpanda is running:")
        print("  lightpanda --remote-debugging-port=9222")
        print("\nOr use Chrome/Chromium:")
        print("  chromium --remote-debugging-port=9222 --headless")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Lightpanda CDP Simple Example")
    print("=" * 80 + "\n")

    asyncio.run(main())

    print("\n" + "=" * 80)
    print("Done!")
    print("=" * 80 + "\n")
