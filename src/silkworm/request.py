"""Request objects and callback type contracts used by spiders and the engine."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Self

from ._types import (
    BodyData,
    Headers,
    JSONLike,
    JSONValue,
    MetaData,
    QueryParams,
    QueryValue,
)

if TYPE_CHECKING:
    from .response import Response


@dataclass(slots=True)
class Request:
    """Describe one HTTP request and how its result should be handled.

    Args:
        url: Absolute request URL. Relative links can be resolved with
            :meth:`silkworm.Response.follow`.
        method: HTTP method name.
        headers: Per-request headers merged over client defaults.
        params: Query parameters merged with any query already in ``url``.
        data: Form, byte, or text request body.
        json: JSON-compatible request body. Do not combine with ``data``.
        meta: Framework and user metadata propagated with the request.
        timeout: Per-request timeout in seconds or as a ``timedelta``.
        callback: Callable that consumes the response. When omitted on a
            followed request, the parent request's callback is reused.
        errback: Callable invoked when request processing fails.
        dont_filter: Bypass engine request deduplication when true.
        priority: Scheduling priority; larger values are processed first.

    Note:
        Built-in metadata keys include ``proxy``, ``retry_times``,
        ``allow_non_html``, and ``redirect_times``. Applications may store
        additional JSON-compatible values alongside them.
    """

    url: str
    method: str = "GET"
    headers: Headers = field(default_factory=dict[str, str])
    params: QueryParams = field(default_factory=dict[str, QueryValue])
    data: BodyData = None
    json: JSONValue | None = None
    meta: MetaData = field(default_factory=dict[str, JSONValue])
    timeout: float | timedelta | None = None
    callback: Callback | None = None
    errback: Errback | None = None
    dont_filter: bool = False
    priority: int = 0

    def replace(self, **kwargs: object) -> Self:
        """Return a shallow copy with the named dataclass fields replaced.

        Unspecified mutable fields are shared with the original request.

        Raises:
            TypeError: If a keyword is not a request field.
        """
        return replace(self, **kwargs)


type CallbackOutput = (
    Request
    | JSONLike
    | Iterable[Request | JSONLike]
    | AsyncIterable[Request | JSONLike]
    | AsyncIterator[Request | JSONLike]
    | None
)
type CallbackResult = CallbackOutput | Awaitable[CallbackOutput]
type Callback = Callable[["Response"], CallbackResult]
type Errback = Callable[[Request, Exception], CallbackResult]

__all__ = ["Callback", "CallbackOutput", "CallbackResult", "Errback", "Request"]
