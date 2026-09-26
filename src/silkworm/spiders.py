"""Spider base class and protected custom-statistics mapping."""

from __future__ import annotations

from typing import TYPE_CHECKING, overload, override

from ._scope import current_scope
from ._types import JSONValue
from .logging import get_logger
from .request import Request

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ._types import JSONLike, MetaData
    from .logging import Logger
    from .request import Callback
    from .response import Response


_RESERVED_STATS_KEYS = frozenset(
    {
        "spider",
        "event_loop",
        "elapsed_seconds",
        "requests_sent",
        "responses_received",
        "items_scraped",
        "errors",
        "queue_size",
        "requests_per_second",
        "seen_requests",
        "memory_mb",
    }
)


class StatsPayloadDict(dict[str, JSONValue]):
    """Mutable user statistics merged into periodic and final crawl summaries.

    Keys beginning with an underscore and keys reserved for engine statistics
    are rejected to prevent collisions.
    """

    __slots__ = ("_reserved_keys",)

    def __init__(self, reserved_keys: frozenset[str], /) -> None:
        super().__init__()
        self._reserved_keys = reserved_keys

    def _validate_key(self, key: str) -> None:
        if key.startswith("_"):
            raise KeyError(f"Spider stats key cannot start with '_': {key}")
        if key in self._reserved_keys:
            raise KeyError(f"Spider stats key is reserved by the engine: {key}")

    @override
    def __setitem__(self, key: str, value: JSONValue) -> None:
        self._validate_key(key)
        super().__setitem__(key, value)

    @override
    def update(self, *args: object, **kwargs: JSONValue) -> None:
        """Validate and merge statistics from a mapping, iterable, or keywords."""
        updates = dict(*args, **kwargs)
        for key, value in updates.items():
            self[key] = value

    @overload
    def setdefault(self, key: str, default: None = None) -> None: ...

    @overload
    def setdefault(self, key: str, default: JSONValue) -> JSONValue: ...

    @override
    def setdefault(
        self, key: str, default: JSONValue | None = None
    ) -> JSONValue | None:
        self._validate_key(key)
        return super().setdefault(key, default)


class Spider:
    """Base class defining a crawl's initial requests and response callback.

    Subclasses usually set :attr:`name` and :attr:`start_urls`, then override
    :meth:`parse`. Callbacks are ``async`` functions returning ``None``; they
    report results by awaiting :meth:`emit` for items and :meth:`follow` for
    further requests. Both apply backpressure, so each ``await`` completes once
    the item passed every pipeline or the request entered the queue.

    Args:
        name: Per-instance name overriding the class attribute.
        start_urls: Per-instance starting URLs.
        custom_settings: JSON-compatible settings copied for this instance.
        logger: Existing structured logger or context mapping used to create
            one lazily.

    Attributes:
        stats_payload: User-defined statistics included in engine summaries.

    Example:
        >>> from silkworm import HTMLResponse, Response, Spider
        >>> class TitlesSpider(Spider):
        ...     name = "titles"
        ...     start_urls = ("https://example.com",)
        ...
        ...     async def parse(self, response: Response) -> None:
        ...         if isinstance(response, HTMLResponse):
        ...             title = await response.select_first("title")
        ...             if title is not None:
        ...                 await self.emit({"title": title.text})
    """

    name: str = "spider"
    start_urls: tuple[str, ...] = ()
    custom_settings: MetaData = {}  # noqa: RUF012  # instances override this

    def __init__(
        self,
        *,
        name: str | None = None,
        start_urls: Iterable[str] | None = None,
        custom_settings: MetaData | None = None,
        logger: Logger | dict[str, object] | None = None,
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
            self.logger: Logger | None = None
        elif isinstance(logger, dict):
            # If logger is a dict, use it as context for get_logger
            self.logger = get_logger(**logger)
        else:
            # If logger is already a Logger instance, use it directly
            self.logger = logger

        self.stats_payload: StatsPayloadDict = StatsPayloadDict(_RESERVED_STATS_KEYS)

    @property
    def log(self) -> Logger:
        """Return the spider logger, creating one bound to its name if needed."""
        if self.logger is None:
            self.logger = get_logger(spider=self.name)
        return self.logger

    async def start_requests(self) -> None:
        """Schedule one request per :attr:`start_urls` entry.

        Override this hook to customize methods, headers, metadata, or callbacks
        for initial requests; schedule each one with :meth:`follow`.
        """
        for url in self.start_urls:
            await self.follow(Request(url=url, callback=self.parse))

    async def parse(self, response: Response) -> None:
        """Process a starting response.

        Subclasses must implement this coroutine and report results with
        :meth:`emit` and :meth:`follow`. The engine always passes an
        :class:`~silkworm.HTMLResponse` to this callback.

        Raises:
            NotImplementedError: When the base implementation is called.
        """
        raise NotImplementedError

    async def emit(self, item: JSONLike) -> None:
        """Send a scraped item through the item pipelines.

        Returns after every pipeline processed the item, so pipeline errors
        surface at this call site.

        Raises:
            SpiderError: If awaited outside an engine-run callback, or after
                the callback that owns the current scope finished.
            TypeError: If ``item`` is a :class:`~silkworm.Request`.
        """
        await current_scope("emit").emit(item)

    async def follow(
        self,
        target: Request | str,
        callback: Callback | None = None,
        **kwargs: object,
    ) -> None:
        """Schedule a request for crawling.

        ``target`` is either a ready :class:`~silkworm.Request` or a URL. Inside
        a response callback a URL is resolved against the response and inherits
        its callback, exactly like :meth:`silkworm.Response.follow`. Requests
        pass engine deduplication and wait for queue capacity.

        Args:
            target: Request to schedule, or an absolute or relative URL.
            callback: Response callback for a URL target.
            **kwargs: Additional :class:`~silkworm.Request` fields for a URL
                target.

        Raises:
            SpiderError: If awaited outside an engine-run callback, or after
                the callback that owns the current scope finished.
            TypeError: If request fields are combined with a ``Request`` target.
        """
        scope = current_scope("follow")
        if isinstance(target, Request):
            if callback is not None or kwargs:
                raise TypeError(
                    "follow() accepts callback and request fields only with a URL; "
                    "use Request.replace() to adjust an existing request"
                )
            await scope.follow(target)
        elif scope.response is not None:
            await scope.response.follow(target, callback=callback, **kwargs)
        else:
            await scope.follow(Request(url=target, callback=callback, **kwargs))  # type: ignore[arg-type]

    async def follow_all(
        self,
        targets: Iterable[Request | str | None],
        callback: Callback | None = None,
        **kwargs: object,
    ) -> None:
        """Schedule every non-``None`` target in input order via :meth:`follow`."""
        for target in targets:
            if target is not None:
                await self.follow(target, callback=callback, **kwargs)

    # hooks for pipelines / engine if desired later
    async def open(self) -> None:
        """Run once after middleware setup and before initial requests enqueue."""

    async def close(self) -> None:
        """Run once after pipelines close during normal engine shutdown."""
