from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypedDict

try:
    import zenoh  # type: ignore[import-not-found]

    ZENOH_AVAILABLE = True
except ImportError:
    zenoh = None
    ZENOH_AVAILABLE = False

from .._resources import raise_cleanup_errors
from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from zenoh import (  # type: ignore[import-not-found]
        Config,
        CongestionControl,
        Encoding,
        Locality,
        Priority,
        Publisher,
        Reliability,
        Session,
    )

    from .._types import JSONValue
    from ..spiders import Spider

type ZenohKeyResolver = Callable[
    [JSONValue, Spider],
    str | Awaitable[str],
]
"""Sync or async callback that chooses a Zenoh key for one scraped item."""


class _PublisherOptions(TypedDict, total=False):
    encoding: str | Encoding
    congestion_control: CongestionControl
    priority: Priority
    express: bool
    reliability: Reliability
    allowed_destination: Locality


class ZenohPipeline:
    """
    Pipeline that publishes JSON-serialized items to Zenoh.

    Args:
        key_expr: Static Zenoh key expression or a sync/async resolver receiving
            ``(item, spider)`` and returning one.
        config: Configuration used to open a pipeline-owned session. Mutually
            exclusive with ``session``; a default configuration is used when
            both are omitted.
        session: Existing session to reuse. The pipeline never closes an
            injected session.
        encoding: Encoding declared for published payloads.
        congestion_control: Behavior when Zenoh's transmission queue is full.
        priority: Transmission priority for published items.
        express: Whether messages should bypass batching where possible.
        reliability: Reliability requested from declared publishers.
        allowed_destination: Locality constraint for published items.

    Publishers created by this pipeline are cached by key expression and
    undeclared during :meth:`close`. Zenoh's synchronous calls run in worker
    threads so they do not block Silkworm's event loop.

    Example:
        from silkworm.pipelines import ZenohPipeline

        pipeline = ZenohPipeline("scraping/items")
    """

    def __init__(
        self,
        key_expr: str | ZenohKeyResolver,
        *,
        config: Config | None = None,
        session: Session | None = None,
        encoding: str | Encoding = "application/json",
        congestion_control: CongestionControl | None = None,
        priority: Priority | None = None,
        express: bool | None = None,
        reliability: Reliability | None = None,
        allowed_destination: Locality | None = None,
    ) -> None:
        """Initialize a Zenoh publisher pipeline."""
        if not ZENOH_AVAILABLE:
            raise ImportError(
                "eclipse-zenoh is required for ZenohPipeline. "
                "Install it with: pip install silkworm-rs[zenoh]",
            )
        if config is not None and session is not None:
            raise ValueError("'config' and 'session' are mutually exclusive")
        if not isinstance(key_expr, str) and not callable(key_expr):
            raise TypeError("key_expr must be a string or callable")

        self.key_expr = key_expr
        self.config = config
        self._provided_session = session
        self._session: Session | None = None
        self._publishers: dict[str, Publisher] = {}
        self._publisher_lock = asyncio.Lock()
        self._publisher_options = _PublisherOptions(encoding=encoding)
        if congestion_control is not None:
            self._publisher_options["congestion_control"] = congestion_control
        if priority is not None:
            self._publisher_options["priority"] = priority
        if express is not None:
            self._publisher_options["express"] = express
        if reliability is not None:
            self._publisher_options["reliability"] = reliability
        if allowed_destination is not None:
            self._publisher_options["allowed_destination"] = allowed_destination
        self.logger: Logger = get_logger(component="ZenohPipeline")

    async def open(self, spider: Spider) -> None:
        """Open or attach to a session and declare a static publisher."""
        if self._session is not None:
            raise RuntimeError("ZenohPipeline already opened")

        owns_session = self._provided_session is None
        if owns_session:
            config = self.config
            if config is None:
                config = zenoh.Config()  # type: ignore[union-attr]
            session = await asyncio.to_thread(zenoh.open, config)  # type: ignore[union-attr]
        else:
            session = self._provided_session

        assert session is not None
        self._session = session
        try:
            if isinstance(self.key_expr, str):
                await self._declare_publisher(self.key_expr)
        except BaseException as exc:
            self._session = None
            self._publishers.clear()
            if owns_session:
                try:
                    await asyncio.to_thread(session.close)
                except BaseException as cleanup_exc:  # noqa: BLE001
                    exc.add_note(f"Zenoh rollback failed: {cleanup_exc}")
            raise

        self.logger.info(
            "Opened Zenoh pipeline",
            key_expr=self.key_expr if isinstance(self.key_expr, str) else "dynamic",
            owns_session=owns_session,
        )

    async def close(self, spider: Spider) -> None:
        """Undeclare publishers and close a pipeline-owned session."""
        session = self._session
        publishers = list(self._publishers.values())
        owns_session = self._provided_session is None
        self._session = None
        self._publishers.clear()

        errors: list[BaseException] = []
        for publisher in publishers:
            try:
                await asyncio.to_thread(publisher.undeclare)
            except BaseException as exc:  # noqa: BLE001 - finish all cleanup
                errors.append(exc)

        if session is not None and owns_session:
            try:
                await asyncio.to_thread(session.close)
            except BaseException as exc:  # noqa: BLE001 - preserve all failures
                errors.append(exc)

        self.logger.info(
            "Closed Zenoh pipeline",
            publishers=len(publishers),
            owns_session=owns_session,
        )
        raise_cleanup_errors("Zenoh pipeline cleanup failed", errors)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Serialize and publish one item, then pass it to the next pipeline."""
        if self._session is None:
            raise RuntimeError("ZenohPipeline not opened")

        key_expr = await self._resolve_key(item, spider)
        publisher = await self._get_publisher(key_expr)
        payload = json.dumps(item, ensure_ascii=False)
        await asyncio.to_thread(publisher.put, payload)
        log_pipeline_item(
            self,
            "Published item to Zenoh",
            key_expr=key_expr,
            spider=spider.name,
        )
        return item

    async def _resolve_key(self, item: JSONValue, spider: Spider) -> str:
        """Resolve and validate the key expression for one item."""
        if isinstance(self.key_expr, str):
            return self.key_expr

        resolved = self.key_expr(item, spider)
        if inspect.isawaitable(resolved):
            resolved = await resolved
        if not isinstance(resolved, str):
            raise TypeError("Zenoh key resolver must return a string")
        return resolved

    async def _get_publisher(self, key_expr: str) -> Publisher:
        """Return a cached publisher, declaring it once when necessary."""
        publisher = self._publishers.get(key_expr)
        if publisher is not None:
            return publisher

        async with self._publisher_lock:
            publisher = self._publishers.get(key_expr)
            if publisher is None:
                publisher = await self._declare_publisher(key_expr)
            return publisher

    async def _declare_publisher(self, key_expr: str) -> Publisher:
        """Declare and cache a publisher for ``key_expr``."""
        session = self._session
        if session is None:
            raise RuntimeError("ZenohPipeline not opened")
        publisher = await asyncio.to_thread(
            session.declare_publisher,
            key_expr,
            **self._publisher_options,
        )
        self._publishers[key_expr] = publisher
        return publisher
