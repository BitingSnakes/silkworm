from __future__ import annotations

from typing import TYPE_CHECKING

from ..logging import Logger, get_logger
from ..request import Request
from ..response import HTMLResponse, Response

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..spiders import Spider


class SkipNonHTMLMiddleware:
    """
    Response middleware that drops callbacks for non-HTML payloads.

    It checks the Content-Type header first, then falls back to a quick body
    sniff for "<html". Non-HTML responses keep flowing through the engine but
    execute a no-op callback so spider parse methods are skipped.
    Set `request.meta["allow_non_html"] = True` to bypass filtering for a request
    (useful for XML sitemaps, robots.txt fetches, etc.).

    Args:
        allowed_types: Lowercase tokens accepted in the Content-Type header.
        sniff_bytes: Leading body bytes inspected for an HTML tag when headers
            are inconclusive.
    """

    def __init__(
        self,
        allowed_types: Iterable[str] | None = None,
        sniff_bytes: int = 2048,
    ) -> None:
        if sniff_bytes < 0:
            msg = "sniff_bytes must be non-negative"
            raise ValueError(msg)

        self.allowed_types: list[str] = [t.lower() for t in (allowed_types or ["html"])]
        self.sniff_bytes = sniff_bytes
        self.logger: Logger = get_logger(component="SkipNonHTMLMiddleware")

    async def _skip_response(self, response: Response) -> None:
        return None

    def _looks_like_html(self, response: Response) -> bool:
        if isinstance(response, HTMLResponse):
            return True

        content_type = response.headers.get("content-type", "").lower()
        if any(token in content_type for token in self.allowed_types):
            return True

        if self.sniff_bytes == 0:
            return False

        snippet = response.body[: self.sniff_bytes].lower()
        return b"<html" in snippet

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        """Replace the callback with a no-op when the payload is not HTML."""
        # Allow opt-out for requests that intentionally fetch non-HTML content
        if response.request.meta.get("allow_non_html"):
            return response

        if self._looks_like_html(response):
            return response

        self.logger.info(
            "Skipping non-HTML response",
            url=response.url,
            status=response.status,
            content_type=response.headers.get("content-type", "unknown"),
        )
        response.request = response.request.replace(callback=self._skip_response)
        return response
