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
        """
        Return a new Request with the provided fields replaced.
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
