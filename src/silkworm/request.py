from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

from .response import Response  # type: ignore[import]

Callback = Callable[["Response"], Awaitable[Any]]  # type: ignore[name-defined]


@dataclass(slots=True)
class Request:
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    data: Any | None = None
    json: Any | None = None
    meta: Dict[str, Any] = field(default_factory=dict)
    timeout: float | None = None
    callback: Optional[Callback] = None
    dont_filter: bool = False
    priority: int = 0

    def replace(self, **kwargs) -> "Request":
        data = {field: getattr(self, field) for field in self.__dataclass_fields__}  # type: ignore[attr-defined]
        data.update(kwargs)
        return Request(**data)
