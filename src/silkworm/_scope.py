"""Per-callback crawl scope routing ``emit``/``follow`` calls to the engine."""

from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .exceptions import SpiderError
from .request import Request

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from ._types import JSONLike
    from .response import Response


@dataclass(slots=True)
class CrawlScope:
    """Engine sinks available to one running callback, errback, or seed hook.

    Tasks spawned by the callback (for example with ``asyncio.TaskGroup``)
    inherit the scope through context-variable copying. The scope closes when
    the callback returns: new calls from detached tasks fail loudly, and calls
    already in progress are drained before the engine marks the request done,
    so they never race pipeline or engine shutdown.
    """

    owner: str
    emit_item: Callable[[JSONLike], Awaitable[Awaitable[None] | None]]
    schedule_request: Callable[[Request], Awaitable[None]]
    flush_items: Callable[[], None] | None = None
    response: Response | None = None
    closed: bool = False
    _in_flight: int = field(default=0, init=False, repr=False)
    _idle: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
    _pending_items: list[Awaitable[None]] = field(
        default_factory=list, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._idle.set()

    async def emit(self, item: JSONLike) -> None:
        self._ensure_open("emit")
        if isinstance(item, Request):
            raise TypeError("emit() received a Request; use follow() to schedule it")
        self._in_flight += 1
        self._idle.clear()
        try:
            completion = await self.emit_item(item)
            if completion is not None:
                self._pending_items.append(completion)
        finally:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._idle.set()

    async def follow(self, request: Request) -> None:
        self._ensure_open("follow")
        if not isinstance(request, Request):
            raise TypeError(
                f"follow() expects a Request or URL, got {type(request).__name__}"
            )
        await self._track(self.schedule_request(request))

    async def drain(self) -> None:
        """Close the scope and wait for ``emit``/``follow`` calls in progress."""
        self.closed = True
        await self._idle.wait()
        if self._pending_items:
            if self.flush_items is not None:
                self.flush_items()
            pending, self._pending_items = self._pending_items, []
            await asyncio.gather(*pending)

    async def _track(self, operation: Awaitable[None]) -> None:
        self._in_flight += 1
        self._idle.clear()
        try:
            await operation
        finally:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._idle.set()

    def _ensure_open(self, action: str) -> None:
        if self.closed:
            raise SpiderError(
                f"{action}() was called after callback '{self.owner}' finished; "
                "await spawned tasks (e.g. with asyncio.TaskGroup) before returning"
            )


class CallbackContractError(SpiderError):
    """A callback is not an ``async`` function returning ``None``."""


async def invoke_callback(invoke: Callable[[], object], name: str) -> None:
    """Call ``invoke`` and await it, enforcing the callback contract.

    Exceptions raised by the callback itself propagate unchanged; a callback
    that yields, is synchronous, or returns a value raises
    :class:`CallbackContractError` with a migration hint.
    """
    produced = invoke()
    if inspect.isasyncgen(produced):
        raise CallbackContractError(
            f"Spider callback '{name}' is an async generator; callbacks "
            "must not yield. Replace `yield item` with "
            "`await self.emit(item)` and `yield request` with "
            "`await self.follow(request)`",
        )
    if not inspect.isawaitable(produced):
        raise CallbackContractError(
            f"Spider callback '{name}' must be an async function, "
            f"got a {type(produced).__name__} result",
        )
    returned: object = await produced
    if returned is not None:
        raise CallbackContractError(
            f"Spider callback '{name}' returned a {type(returned).__name__}; "
            "callbacks must return None and report results with "
            "`await self.emit(item)` / `await self.follow(request)`",
        )


_CURRENT_SCOPE: ContextVar[CrawlScope | None] = ContextVar(
    "silkworm_crawl_scope",
    default=None,
)


def current_scope(action: str) -> CrawlScope:
    """Return the active scope or explain where ``action`` may be awaited."""
    scope = _CURRENT_SCOPE.get()
    if scope is None:
        raise SpiderError(
            f"{action}() can only be awaited while the engine runs "
            "start_requests(), a request callback, or an errback"
        )
    return scope


@asynccontextmanager
async def enter_scope(scope: CrawlScope) -> AsyncIterator[CrawlScope]:
    """Activate ``scope`` for the current task, then close and drain it on exit.

    Draining is skipped on cancellation so shutdown is never blocked.
    """
    token = _CURRENT_SCOPE.set(scope)
    try:
        yield scope
    except Exception:
        await scope.drain()
        raise
    else:
        await scope.drain()
    finally:
        scope.closed = True
        _CURRENT_SCOPE.reset(token)
