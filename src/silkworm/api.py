from __future__ import annotations
import inspect
from datetime import timedelta

from scraper_rs import Document  # type: ignore[import]
from rnet import Client, Emulation  # type: ignore[import]
from typing import Any, cast


async def fetch_html(
    url: str,
    *,
    emulation: Emulation = Emulation.Firefox139,
    timeout: float | None = None,
) -> tuple[str, Document]:
    client = cast(Any, Client)(emulation=emulation)
    try:
        if timeout is not None:
            try:
                resp = await client.get(url, timeout=timeout)
            except TypeError as exc:
                if (
                    isinstance(timeout, (int, float))
                    and not isinstance(timeout, bool)
                    and "timedelta" in str(exc).lower()
                ):
                    resp = await client.get(
                        url,
                        timeout=timedelta(seconds=float(timeout)),
                    )
                else:
                    raise
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
