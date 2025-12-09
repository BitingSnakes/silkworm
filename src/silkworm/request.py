from __future__ import annotations
from dataclasses import dataclass, field
from collections.abc import Awaitable, Callable
from typing import Any

from .response import Response  # type: ignore[import]

Callback = Callable[["Response"], Awaitable[Any]]  # type: ignore[name-defined]


@dataclass(slots=True)
class Request:
    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    data: Any | None = None
    json: Any | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    timeout: float | None = None
    callback: Callback | None = None
    dont_filter: bool = False
    priority: int = 0

    def replace(self, **kwargs) -> "Request":
        data = {field: getattr(self, field) for field in self.__dataclass_fields__}  # type: ignore[attr-defined]
        data.update(kwargs)
        return Request(**data)
