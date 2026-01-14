from __future__ import annotations

import inspect
from datetime import timedelta
from typing import Any, cast

from rnet import Client, Emulation  # type: ignore[import]
from scraper_rs import Document  # type: ignore[import]


async def fetch_html(
    url: str,
    *,
    emulation: Emulation = Emulation.Firefox139,
    timeout: float | timedelta | None = None,
) -> tuple[str, Document]:
    """
    Fetch HTML from a URL using the rnet HTTP client.

    Returns a tuple of (text, Document) where Document is a scraper_rs Document
    for CSS/XPath selection.
    """
    client = cast(Any, Client)(emulation=emulation)
    try:
        if timeout is not None:
            if not isinstance(timeout, timedelta):
                timeout = timedelta(seconds=float(timeout))
            resp = await client.get(url, timeout=timeout)
        else:
            resp = await client.get(url)
        text = await resp.text()
        return text, Document(text)
    finally:
        closer = getattr(client, "aclose", None) or getattr(client, "close", None)
        if closer and callable(closer):
            result = closer()
            if inspect.isawaitable(result):
                await result


async def fetch_html_cdp(
    url: str,
    *,
    ws_endpoint: str = "ws://127.0.0.1:9222",
    timeout: float | None = None,
) -> tuple[str, Document]:
    """
    Fetch HTML from a URL using CDP (Chrome DevTools Protocol).

    This function connects to a CDP-compatible browser (like Lightpanda, Chrome, or Chromium)
    and fetches the rendered HTML after JavaScript execution.

    Args:
        url: The URL to fetch
        ws_endpoint: WebSocket endpoint for CDP connection (default: ws://127.0.0.1:9222)
        timeout: Optional timeout in seconds

    Returns:
        A tuple of (text, Document) where Document is a scraper_rs Document

    Raises:
        ImportError: If websockets package is not installed
        HttpError: If the request fails

    Example:
        >>> import asyncio
        >>> from silkworm import fetch_html_cdp
        >>>
        >>> async def main():
        ...     text, doc = await fetch_html_cdp("https://example.com")
        ...     title = doc.select_first("title")
        ...     print(title.text if title else "No title")
        >>>
        >>> asyncio.run(main())
    """
    from .cdp import CDPClient
    from .request import Request

    client = CDPClient(
        ws_endpoint=ws_endpoint,
        timeout=timeout,
    )

    try:
        await client.connect()
        req = Request(url=url)
        response = await client.fetch(req)
        text = response.text
        return text, Document(text)
    finally:
        await client.close()
