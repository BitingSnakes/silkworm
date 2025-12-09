from __future__ import annotations
from collections.abc import AsyncIterator, Iterable
from ._types import MetaData
from .request import CallbackOutput, Request
from .response import Response


class Spider:
    name: str = "spider"
    start_urls: tuple[str, ...] = ()
    custom_settings: MetaData = {}

    def __init__(
        self,
        *,
        name: str | None = None,
        start_urls: Iterable[str] | None = None,
        custom_settings: MetaData | None = None,
    ) -> None:
        self.name = name if name is not None else self.name
        self.start_urls = (
            tuple(start_urls) if start_urls is not None else tuple(self.start_urls)
        )
        base_settings = (
            custom_settings if custom_settings is not None else self.custom_settings
        )
        # Copy to avoid mutating a shared mapping.
        self.custom_settings = dict(base_settings)

    async def start_requests(self) -> AsyncIterator[Request]:
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)

    async def parse(self, response: Response) -> CallbackOutput:
        raise NotImplementedError

    # hooks for pipelines / engine if desired later
    async def open(self) -> None: ...

    async def close(self) -> None: ...
