"""One-shot helpers for fetching and parsing HTML without defining a spider."""

from __future__ import annotations

import sys
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from scraper_rs.asyncio import AsyncDocument, parse
from wreq import Client

from ._resources import close_resource, raise_cleanup_errors
from .http import DEFAULT_EMULATION

if TYPE_CHECKING:
    from wreq import Emulation, Profile


async def fetch_html(
    url: str,
    *,
    emulation: Emulation | Profile | None = DEFAULT_EMULATION,
    timeout: float | timedelta | None = None,
) -> tuple[str, AsyncDocument]:
    """Fetch and asynchronously parse one HTML document with ``wreq``.

    Args:
        url: Absolute URL to fetch.
        emulation: Browser profile to impersonate, or ``None`` to disable it.
        timeout: Request timeout in seconds or as a ``timedelta``.

    Returns:
        A ``(text, AsyncDocument)`` tuple with awaitable selector helpers.

    Note:
        This convenience API does not apply spider middleware, retries,
        deduplication, or pipelines.
    """
    client = cast(Any, Client)(emulation=emulation)
    response: Any = None
    try:
        if timeout is not None:
            if not isinstance(timeout, timedelta):
                timeout = timedelta(seconds=float(timeout))
            response = await client.get(url, timeout=timeout)
        else:
            response = await client.get(url)
        text = await response.text()
        return text, await parse(text)
    finally:
        primary = sys.exception()
        cleanup_errors: list[BaseException] = []
        for resource in (response, client):
            try:
                await close_resource(resource)
            except BaseException as cleanup_exc:  # noqa: BLE001
                cleanup_errors.append(cleanup_exc)
        if primary is not None:
            for cleanup_error in cleanup_errors:
                primary.add_note(
                    "Fetch cleanup failed with "
                    f"{cleanup_error.__class__.__name__}: {cleanup_error}"
                )
        else:
            raise_cleanup_errors("Fetch resource cleanup failed", cleanup_errors)


async def fetch_html_cdp(
    url: str,
    *,
    ws_endpoint: str = "ws://127.0.0.1:9222",
    timeout: float | None = None,
) -> tuple[str, AsyncDocument]:
    """
    Fetch HTML from a URL using CDP (Chrome DevTools Protocol).

    This function connects to a CDP-compatible browser (like Lightpanda, Chrome, or Chromium)
    and fetches the rendered HTML after JavaScript execution.

    Args:
        url: The URL to fetch
        ws_endpoint: WebSocket endpoint for CDP connection (default: ws://127.0.0.1:9222)
        timeout: Optional timeout in seconds

    Returns:
        A tuple of (text, AsyncDocument) with awaitable selector helpers.

    Raises:
        ImportError: If websockets package is not installed
        HttpError: If the request fails

    Example:
        >>> import asyncio
        >>> from silkworm import fetch_html_cdp
        >>>
        >>> async def main():
        ...     text, doc = await fetch_html_cdp("https://example.com")
        ...     title = await doc.select_first("title")
        ...     print(title.text if title else "No title")
        >>>
        >>> asyncio.run(main())
    """
    from .cdp import CDPClient
    from .request import Request

    async with CDPClient(ws_endpoint=ws_endpoint, timeout=timeout) as client:
        req = Request(url=url)
        with await client.fetch(req) as response:
            text = response.text
        return text, await parse(text)


async def fetch_html_servo(
    url: str,
    *,
    timeout: float | timedelta | None = None,
    settle_ms: int = 0,
    user_agent: str | None = None,
    javascript: str | None = None,
    allow_private_addresses: bool = False,
) -> tuple[str, AsyncDocument]:
    """Fetch and parse rendered HTML with ``servofetch`` and Servo.

    Args:
        url: Absolute URL to render.
        timeout: Render timeout in seconds or as a ``timedelta``.
        settle_ms: Delay after loading before capturing the document.
        user_agent: Optional browser user agent override.
        javascript: Optional JavaScript evaluated by the rendered-page client.
        allow_private_addresses: Permit navigation to private network addresses.

    Returns:
        A ``(text, AsyncDocument)`` tuple with awaitable selector helpers.

    Raises:
        ImportError: If a compatible ``servofetch`` build is unavailable.
        HttpError: If rendering fails.
    """
    from ._types import MetaData
    from .request import Request
    from .servo import SERVO_JAVASCRIPT_META_KEY, ServoFetchClient

    async with ServoFetchClient(
        timeout=timeout,
        settle_ms=settle_ms,
        user_agent=user_agent,
        allow_private_addresses=allow_private_addresses,
    ) as client:
        meta: MetaData = (
            {SERVO_JAVASCRIPT_META_KEY: javascript} if javascript is not None else {}
        )
        with await client.fetch(Request(url=url, meta=meta)) as response:
            text = response.text
        return text, await parse(text)
