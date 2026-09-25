from __future__ import annotations

from datetime import datetime, timedelta, timezone
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


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timeout_seconds(timeout: float | timedelta | None) -> float | None:
    if timeout is None:
        return None
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout)


def _callback_name(callback: object) -> str | None:
    if callback is None:
        return None
    return getattr(callback, "__name__", callback.__class__.__name__)
