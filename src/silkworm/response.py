from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from scraper_rs import Document  # type: ignore[import]

if TYPE_CHECKING:
    from .request import Request


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

    def follow(self, href: str, callback=None, **kwargs) -> "Request":
        from .request import Request  # local import to avoid cycle

        url = urljoin(self.url, href)
        return Request(
            url=url,
            callback=callback or self.request.callback,
            **kwargs,
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
    _doc: Document | None = None
    doc_max_size_bytes: int = 5_000_000

    @property
    def doc(self) -> Document:
        if self._doc is None:
            self._doc = Document(self.text, max_size_bytes=self.doc_max_size_bytes)
        return self._doc

    # shortcuts
    def css(self, selector: str):
        return self.doc.select(selector)

    def find(self, selector: str):
        return self.doc.find(selector)

    def follow(self, href: str, callback=None, **kwargs) -> "Request":
        return super().follow(href, callback=callback, **kwargs)

    def close(self) -> None:
        """
        Release the underlying Document when it is no longer needed.
        """
        if self._closed:
            return

        doc = self._doc
        self._doc = None
        if doc is not None:
            closer = getattr(doc, "close", None)
            if closer and callable(closer):
                try:
                    closer()
                except Exception:
                    # Best-effort cleanup; avoid surfacing close errors.
                    pass

        # Explicitly call base class to avoid zero-arg super issues with slotted dataclasses.
        Response.close(self)
