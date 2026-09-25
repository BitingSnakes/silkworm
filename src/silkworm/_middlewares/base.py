from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from ..request import Request
from ..response import Response

if TYPE_CHECKING:
    from ..spiders import Spider


class RequestMiddleware(Protocol):
    """Protocol for middleware applied before an HTTP request is sent."""

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Return the request to send, optionally modified or replaced."""
        ...


class ResponseMiddleware(Protocol):
    """Protocol for middleware applied after an HTTP response is received."""

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        """Return a response to dispatch or a request to enqueue instead."""
        ...


class ExceptionMiddleware(Protocol):
    """Protocol for middleware that may recover from request failures."""

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        """Return a retry request, or ``None`` to leave the error unhandled."""
        ...


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def callback_name(callback: object) -> str | None:
    if callback is None:
        return None
    return getattr(callback, "__name__", callback.__class__.__name__)
