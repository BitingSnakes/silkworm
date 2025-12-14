from __future__ import annotations
from dataclasses import dataclass, field, replace
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Self

from ._types import BodyData, Headers, JSONValue, MetaData, QueryParams

if TYPE_CHECKING:
    from .response import Response


@dataclass(slots=True)
class Request:
    url: str
    method: str = "GET"
    headers: Headers = field(default_factory=dict)
    params: QueryParams = field(default_factory=dict)
    data: BodyData = None
    json: JSONValue | None = None
    meta: MetaData = field(default_factory=dict)
    timeout: float | None = None
    callback: "Callback | None" = None
    dont_filter: bool = False
    priority: int = 0

    def replace(self, **kwargs: object) -> Self:
        """
        Return a new Request with the provided fields replaced.
        """
        return replace(self, **kwargs)  # type: ignore[arg-type]


# PEP 695: Type Parameter Syntax (Python 3.13+)
type CallbackOutput = (
    Request
    | JSONValue
    | Iterable[Request | JSONValue]
    | AsyncIterable[Request | JSONValue]
    | AsyncIterator[Request | JSONValue]
    | None
)
type CallbackResult = CallbackOutput | Awaitable[CallbackOutput]
type Callback = Callable[["Response"], CallbackResult]

__all__ = ["Callback", "CallbackOutput", "CallbackResult", "Request"]
