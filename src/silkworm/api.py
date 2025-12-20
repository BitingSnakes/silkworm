from __future__ import annotations
import inspect
from datetime import timedelta

from typing import Any, cast
from scraper_rs import Document  # type: ignore[import]
from rnet import Client, Emulation  # type: ignore[import]


async def fetch_html(
    url: str,
    *,
    emulation: Emulation = Emulation.Firefox139,
    timeout: float | timedelta | None = None,
) -> tuple[str, Document]:
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
