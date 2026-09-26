"""
Fetch one JavaScript-rendered page with a headless browser (no spider needed).

Some websites build their pages with JavaScript. A normal HTTP request only
gets the empty "skeleton" HTML. A headless browser runs the JavaScript first,
so you get the finished page.

`fetch_html_cdp()` connects to a browser through CDP (Chrome DevTools
Protocol), opens the URL, and returns the rendered HTML. It works with
Lightpanda (a small, fast headless browser) or with Chrome/Chromium.

Before running:
1. Install CDP support:
       pip install "silkworm-rs[cdp]"
2. Start a browser with CDP enabled, in another terminal:
       lightpanda --remote-debugging-port=9222
   or:
       chromium --headless --remote-debugging-port=9222

How to run:
    python examples/lightpanda_simple.py
"""

from __future__ import annotations

import asyncio

from silkworm import fetch_html_cdp
from silkworm.exceptions import HttpError

BROWSER_URL = "ws://127.0.0.1:9222"
PAGE_URL = "https://wikipedia.com/"


async def main() -> None:
    print(f"Connecting to the browser at {BROWSER_URL}...")

    try:
        # `text` is the raw HTML string, `doc` is a parsed document you can query.
        text, doc = await fetch_html_cdp(
            PAGE_URL, ws_endpoint=BROWSER_URL, timeout=30.0
        )
    except ImportError:
        print("The websockets package is missing. Install it with:")
        print('    pip install "silkworm-rs[cdp]"')
        return
    except HttpError as exc:
        print(f"Error: {exc}")
        print("Is the browser running? Start it with:")
        print("    lightpanda --remote-debugging-port=9222")
        return

    print(f"Fetched {len(text)} characters of HTML")

    # Collect the href of every <a> link on the page.
    links = []
    for link_el in await doc.select("a"):
        href = link_el.attr("href")
        if href:
            links.append(href)

    print(f"Found {len(links)} links. The first 10:")
    for number, link in enumerate(links[:10], 1):
        print(f"  {number}. {link}")


if __name__ == "__main__":
    asyncio.run(main())
