"""Per-callback crawl scope routing ``emit``/``follow`` calls to the engine."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .exceptions import SpiderError
from .request import Request

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

    from ._types import JSONLike
    from .response import Response


@dataclass(slots=True)
class CrawlScope:
    """Engine sinks available to one running callback, errback, or seed hook.

    Tasks spawned by the callback (for example with ``asyncio.TaskGroup``)
    inherit the scope through context-variable copying. The scope closes when
    the callback returns, so late calls from detached tasks fail loudly instead
    of racing engine shutdown.
    """

    owner: str
    emit_item: Callable[[JSONLike], Awaitable[None]]
    schedule_request: Callable[[Request], Awaitable[None]]
    response: Response | None = None
    closed: bool = False

    async def emit(self, item: JSONLike) -> None:
        self._ensure_open("emit")
        if isinstance(item, Request):
            raise TypeError("emit() received a Request; use follow() to schedule it")
        await self.emit_item(item)

    async def follow(self, request: Request) -> None:
        self._ensure_open("follow")
        if not isinstance(request, Request):
            raise TypeError(
                f"follow() expects a Request or URL, got {type(request).__name__}"
            )
        await self.schedule_request(request)

    def _ensure_open(self, action: str) -> None:
        if self.closed:
            raise SpiderError(
                f"{action}() was called after callback '{self.owner}' finished; "
                "await spawned tasks (e.g. with asyncio.TaskGroup) before returning"
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


@contextmanager
def enter_scope(scope: CrawlScope) -> Iterator[CrawlScope]:
    """Activate ``scope`` for the current task and close it on exit."""
    token = _CURRENT_SCOPE.set(scope)
    try:
        yield scope
    finally:
        scope.closed = True
        _CURRENT_SCOPE.reset(token)
