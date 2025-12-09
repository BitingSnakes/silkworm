from __future__ import annotations
import inspect

from scraper_rs import Document  # type: ignore[import]
from rnet import Client, Impersonate  # type: ignore[import]


async def fetch_html(
    url: str,
    *,
    impersonate: Impersonate = Impersonate.Firefox139,
    timeout: float | None = None,
) -> tuple[str, Document]:
    client = Client(impersonate=impersonate)
    try:
        kwargs = {"timeout": timeout} if timeout is not None else {}
        resp = await client.get(url, **kwargs)
        text = await resp.text()
        return text, Document(text)
    finally:
        closer = getattr(client, "aclose", None) or getattr(client, "close", None)
        if closer and callable(closer):
            result = closer()
            if inspect.isawaitable(result):
                await result
