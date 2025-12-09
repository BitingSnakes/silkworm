from __future__ import annotations
from dataclasses import dataclass
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
        Hook for subclasses that manage additional resources.
        """
        return None


@dataclass(slots=True)
class HTMLResponse(Response):
    _doc: Document | None = None

    @property
    def doc(self) -> Document:
        if self._doc is None:
            self._doc = Document(self.text)
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
        if self._doc is None:
            return

        doc = self._doc
        self._doc = None
        closer = getattr(doc, "close", None)
        if closer and callable(closer):
            try:
                closer()
            except Exception:
                # Best-effort cleanup; avoid surfacing close errors.
                pass
