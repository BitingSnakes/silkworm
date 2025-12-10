from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from scraper_rs.asyncio import (
    select,
    # select_first,
    xpath as xpath_async,
    # xpath_first as xpath_first_async,
)


if TYPE_CHECKING:
    from scraper_rs import Element, Document  # type: ignore[import]
    from .request import Callback, Request


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
    _doc: Document | None = field(default=None, init=False, repr=False, compare=False)

    @property
    def doc(self) -> Document:
        """
        Lazily parse and cache the HTML document.
        """
        if self._doc is None:
            self._doc = Document(self.text, max_size_bytes=self.doc_max_size_bytes)
        return self._doc

    async def css(self, selector: str) -> list[Element]:
        return await select(self.text, selector, max_size_bytes=self.doc_max_size_bytes)

    async def find(self, selector: str) -> Element | None:
        data = await select(self.text, selector, max_size_bytes=self.doc_max_size_bytes)
        return data[0] if data else None

    async def xpath(self, xpath: str) -> list[Element]:
        return await xpath_async(
            self.text, xpath, max_size_bytes=self.doc_max_size_bytes
        )

    async def xpath_first(self, xpath: str) -> Element | None:
        data = await xpath_async(
            self.text, xpath, max_size_bytes=self.doc_max_size_bytes
        )
        return data[0] if data else None

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

        if self._doc is not None:
            # Ensure the underlying document releases any resources it may hold.
            close_doc = getattr(self._doc, "close", None)
            if close_doc is not None:
                close_doc()
            self._doc = None

        # Explicitly call base class to avoid zero-arg super issues with slotted dataclasses.
        Response.close(self)
