from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from wreq import Client, Method  # type: ignore[import]

from .._timeouts import to_seconds
from ..exceptions import HttpError
from ..logging import get_logger
from ..request import Request

if TYPE_CHECKING:
    from ..spiders import Spider


type RobotsTxtFetcher = Callable[[str], Awaitable[str]]
type RobotsOrigin = tuple[str, str, int | None]


class RobotsTxtDelayMiddleware:
    """
    Request middleware that loads robots.txt and applies its delay directives.

    The middleware currently uses `Crawl-delay` first and falls back to
    `Request-rate` when present. Delays are scoped to the origin that served the
    robots.txt file, and concurrent requests are serialized so the configured
    spacing is respected under engine concurrency.
    """

    def __init__(
        self,
        website_url: str,
        *,
        user_agent: str = "*",
        fallback_delay: float | None = None,
        timeout: float | timedelta | None = 10.0,
        ignore_fetch_errors: bool = True,
        fetcher: RobotsTxtFetcher | None = None,
    ) -> None:
        if not website_url.strip():
            msg = "website_url must not be empty"
            raise ValueError(msg)
        if not user_agent.strip():
            msg = "user_agent must not be empty"
            raise ValueError(msg)
        if fallback_delay is not None and fallback_delay < 0:
            msg = "fallback_delay must be non-negative"
            raise ValueError(msg)
        timeout_seconds = to_seconds(timeout)
        if timeout_seconds is not None and timeout_seconds < 0:
            msg = "timeout must be non-negative"
            raise ValueError(msg)

        self.robots_url, self._origin = self._normalize_robots_url(website_url)
        self.user_agent = user_agent
        self.fallback_delay = fallback_delay
        self.timeout = timeout
        self.ignore_fetch_errors = ignore_fetch_errors
        self._fetcher = fetcher or self._fetch_robots_txt
        self._load_lock = asyncio.Lock()
        self._delay_lock = asyncio.Lock()
        self._loaded = False
        self._delay_seconds: float | None = None
        self._delay_source: str | None = None
        self._next_request_at = 0.0
        self.logger = get_logger(component="RobotsTxtDelayMiddleware")

    async def open(self, spider: Spider) -> None:
        await self._ensure_loaded(spider)

    async def process_request(self, request: Request, spider: Spider) -> Request:
        await self._ensure_loaded(spider)
        delay = self._delay_seconds
        if delay is None or delay <= 0 or not self._matches_origin(request.url):
            return request

        loop = asyncio.get_running_loop()
        async with self._delay_lock:
            now = loop.time()
            wait_seconds = max(0.0, self._next_request_at - now)
            if wait_seconds > 0:
                self.logger.debug(
                    "Delaying request from robots.txt",
                    url=request.url,
                    delay=round(wait_seconds, 3),
                    source=self._delay_source,
                )
                await asyncio.sleep(wait_seconds)
                now = loop.time()
            self._next_request_at = now + delay

        return request

    async def _ensure_loaded(self, spider: Spider) -> None:
        if self._loaded:
            return
        async with self._load_lock:
            if self._loaded:
                return
            try:
                robots_txt = await self._fetcher(self.robots_url)
                self._delay_seconds, self._delay_source = self._parse_delay(robots_txt)
                self.logger.info(
                    "Loaded robots.txt delay settings",
                    spider=spider.name,
                    robots_url=self.robots_url,
                    user_agent=self.user_agent,
                    delay=self._delay_seconds,
                    source=self._delay_source,
                )
            except Exception as exc:
                if not self.ignore_fetch_errors:
                    raise
                self._delay_seconds = self.fallback_delay
                self._delay_source = (
                    "fallback" if self.fallback_delay is not None else None
                )
                self.logger.warning(
                    "Failed to load robots.txt delay settings",
                    spider=spider.name,
                    robots_url=self.robots_url,
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                    fallback_delay=self.fallback_delay,
                )
            self._loaded = True

    def _parse_delay(self, robots_txt: str) -> tuple[float | None, str | None]:
        parser = RobotFileParser()
        parser.set_url(self.robots_url)
        parser.parse(robots_txt.splitlines())

        crawl_delay = parser.crawl_delay(self.user_agent)
        if crawl_delay is not None:
            return float(crawl_delay), "crawl-delay"

        request_rate = parser.request_rate(self.user_agent)
        if request_rate is not None and request_rate.requests > 0:
            return request_rate.seconds / request_rate.requests, "request-rate"

        if self.fallback_delay is not None:
            return self.fallback_delay, "fallback"
        return None, None

    async def _fetch_robots_txt(self, robots_url: str) -> str:
        client = cast(Any, Client)()
        response: Any = None
        try:
            kwargs: dict[str, object] = {}
            request_timeout = to_seconds(self.timeout)
            if request_timeout is not None:
                kwargs["timeout"] = timedelta(seconds=request_timeout)

            response = await client.request(Method.GET, robots_url, **kwargs)
            status = getattr(response, "status", 200)
            if isinstance(status, int) and status >= 400:
                raise HttpError(f"robots.txt request failed with status {status}")

            text = getattr(response, "text", None)
            if callable(text):
                result = text()
                if inspect.isawaitable(result):
                    result = await result
                return str(result)

            read = getattr(response, "read", None)
            if callable(read):
                body = read()
                if inspect.isawaitable(body):
                    body = await body
                if isinstance(body, bytes):
                    return body.decode("utf-8", errors="replace")
                return str(body)

            return ""
        finally:
            if response is not None:
                await self._close_async_resource(response)
            await self._close_async_resource(client)

    async def _close_async_resource(self, resource: object) -> None:
        closer = getattr(resource, "aclose", None) or getattr(resource, "close", None)
        if closer and callable(closer):
            result = closer()
            if inspect.isawaitable(result):
                await result

    def _matches_origin(self, url: str) -> bool:
        try:
            parts = urlsplit(url)
            origin = self._origin_from_parts(parts)
        except ValueError:
            return False
        return origin == self._origin

    def _normalize_robots_url(self, website_url: str) -> tuple[str, RobotsOrigin]:
        parts = urlsplit(website_url)
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            msg = "website_url must be an absolute http or https URL"
            raise ValueError(msg)

        origin = self._origin_from_parts(parts)
        if origin is None:
            msg = "website_url must include a valid host"
            raise ValueError(msg)

        robots_url = urlunsplit(
            (parts.scheme.lower(), parts.netloc, "/robots.txt", "", ""),
        )
        return robots_url, origin

    def _origin_from_parts(self, parts: Any) -> RobotsOrigin | None:
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            return None
        return (
            parts.scheme.lower(),
            parts.hostname.lower(),
            parts.port or self._default_port(parts.scheme),
        )

    def _default_port(self, scheme: str) -> int | None:
        match scheme.lower():
            case "http":
                return 80
            case "https":
                return 443
            case _:
                return None
