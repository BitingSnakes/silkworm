from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urljoin
from asyncio import to_thread

from scraper_rs import Document, Element  # type: ignore[import]

if TYPE_CHECKING:
    from .request import Callback, Request


def extract_select(html: str, max_size_bytes: int, selector: str) -> list[Element]:
    doc = Document(html, max_size_bytes=max_size_bytes)
    elements = doc.select(selector)
    doc.close()
    return elements


def extract_find(html: str, max_size_bytes: int, selector: str) -> Element | None:
    doc = Document(html, max_size_bytes=max_size_bytes)
    elements = doc.find(selector)
    doc.close()
    return elements


@dataclass(slots=True)
class Response:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    request: "Request"
    _closed: bool = field(default=False, init=False, repr=False, compare=False)

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def follow(
        self, href: str, callback: "Callback | None" = None, **kwargs: object
    ) -> "Request":
        from .request import Request  # local import to avoid cycle

        url = urljoin(self.url, href)
        return Request(
            url=url,
            callback=callback or self.request.callback,
            **kwargs,  # type: ignore[arg-type]
        )

    def close(self) -> None:
        """
        Release payload references so responses don't pin memory if they linger.
        """
        if self._closed:
            return

        self._closed = True
        self.body = b""
        self.headers.clear()


@dataclass(slots=True)
class HTMLResponse(Response):
    doc_max_size_bytes: int = 5_000_000

    async def css(self, selector: str) -> list[Element]:
        return await to_thread(
            extract_select, self.text, self.doc_max_size_bytes, selector
        )

    async def find(self, selector: str) -> Element | None:
        return await to_thread(
            extract_find, self.text, self.doc_max_size_bytes, selector
        )

    def follow(
        self, href: str, callback: "Callback | None" = None, **kwargs: object
    ) -> "Request":
        # Explicit base call avoids zero-arg super issues with slotted dataclasses.
        return Response.follow(self, href, callback=callback, **kwargs)

    def close(self) -> None:
        """
        Release the underlying Document when it is no longer needed.
        """
        if self._closed:
            return

        # Explicitly call base class to avoid zero-arg super issues with slotted dataclasses.
        Response.close(self)
