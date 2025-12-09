from __future__ import annotations
from dataclasses import dataclass, field, replace
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, TypeAlias

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

    def replace(self, **kwargs: object) -> "Request":
        """
        Return a new Request with the provided fields replaced.
        """
        return replace(self, **kwargs)  # type: ignore[arg-type]


CallbackOutput: TypeAlias = (
    Request
    | JSONValue
    | Iterable[Request | JSONValue]
    | AsyncIterable[Request | JSONValue]
    | AsyncIterator[Request | JSONValue]
    | None
)
CallbackResult: TypeAlias = CallbackOutput | Awaitable[CallbackOutput]
Callback: TypeAlias = Callable[["Response"], CallbackResult]

__all__ = ["Callback", "CallbackOutput", "CallbackResult", "Request"]
