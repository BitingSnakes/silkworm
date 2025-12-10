from __future__ import annotations
from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING
from ._types import MetaData
from .logging import get_logger
from .request import CallbackOutput, Request
from .response import Response

if TYPE_CHECKING:
    from .logging import _Logger


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
        logger: "_Logger | dict[str, object] | None" = None,
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

        # Configure logger if provided
        if logger is None:
            self.logger: "_Logger | None" = None
        elif isinstance(logger, dict):
            # If logger is a dict, use it as context for get_logger
            self.logger = get_logger(**logger)
        else:
            # If logger is already a _Logger instance, use it directly
            self.logger = logger

    @property
    def log(self) -> "_Logger":
        """
        Convenience accessor that always returns a logger.
        Falls back to a default logger bound to the spider name when none set.
        """
        if self.logger is None:
            self.logger = get_logger(spider=self.name)
        return self.logger

    async def start_requests(self) -> AsyncIterator[Request]:
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse)

    async def parse(self, response: Response) -> CallbackOutput:
        raise NotImplementedError

    # hooks for pipelines / engine if desired later
    async def open(self) -> None: ...

    async def close(self) -> None: ...
