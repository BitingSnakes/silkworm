from __future__ import annotations
from typing import Any, AsyncIterator, Iterable

from .request import Request
from .response import HTMLResponse


class Spider:
    name: str = "spider"
    start_urls: Iterable[str] = ()
    custom_settings: dict[str, Any] = {}

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    async def start_requests(self) -> AsyncIterator[Request]:
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)  # type: ignore

    async def parse(self, response: HTMLResponse):
        raise NotImplementedError

    # hooks for pipelines / engine if desired later
    async def open(self) -> None: ...

    async def close(self) -> None: ...
