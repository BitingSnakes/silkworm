from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from scraper_rs.asyncio import (  # type: ignore[import-untyped]
    select as select_async,
    select_first as select_first_async,
    xpath as xpath_async,
    xpath_first as xpath_first_async,
)


if TYPE_CHECKING:
    from scraper_rs import Element  # type: ignore[import]
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

    async def select(self, selector: str) -> list[Element]:
        return await select_async(
            self.text, selector, max_size_bytes=self.doc_max_size_bytes
        )

    async def select_first(self, selector: str) -> Element | None:
        return await select_first_async(
            self.text, selector, max_size_bytes=self.doc_max_size_bytes
        )

    async def xpath(self, xpath: str) -> list[Element]:
        return await xpath_async(
            self.text, xpath, max_size_bytes=self.doc_max_size_bytes
        )

    async def xpath_first(self, xpath: str) -> Element | None:
        return await xpath_first_async(
            self.text, xpath, max_size_bytes=self.doc_max_size_bytes
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
