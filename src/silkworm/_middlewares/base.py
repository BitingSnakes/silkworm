from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from ..request import Request
from ..response import Response

if TYPE_CHECKING:
    from ..spiders import Spider


class RequestMiddleware(Protocol):
    async def process_request(self, request: Request, spider: Spider) -> Request: ...


class ResponseMiddleware(Protocol):
    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request: ...


class ExceptionMiddleware(Protocol):
    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None: ...


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def callback_name(callback: object) -> str | None:
    if callback is None:
        return None
    return getattr(callback, "__name__", callback.__class__.__name__)
