from __future__ import annotations
import asyncio
import random
from enum import Enum, auto
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Protocol, assert_never

from .request import Request
from .response import HTMLResponse, Response
from .logging import get_logger

from .spiders import Spider


class RequestMiddleware(Protocol):
    async def process_request(self, request: Request, spider: "Spider") -> Request: ...


class ResponseMiddleware(Protocol):
    async def process_response(
        self, response: Response, spider: "Spider"
    ) -> Response | Request: ...


class UserAgentMiddleware:
    def __init__(
        self, user_agents: Sequence[str] | None = None, *, default: str | None = None
    ) -> None:
        self.user_agents = list(user_agents or [])
        self.default = default or "silkworm/0.1"
        self.logger = get_logger(component="UserAgentMiddleware")

    async def process_request(self, request: Request, spider: "Spider") -> Request:
        ua = None
        if self.user_agents:
            ua = random.choice(self.user_agents)
        else:
            ua = self.default
        request.headers.setdefault("User-Agent", ua)
        self.logger.debug("Assigned user agent", user_agent=ua, url=request.url)
        return request


class ProxyMiddleware:
    def __init__(
        self,
        proxies: Iterable[str] | None = None,
        proxy_file: str | Path | None = None,
        random_selection: bool = False,
    ) -> None:
        if proxies is not None and proxy_file is not None:
            raise ValueError(
                "Cannot specify both 'proxies' and 'proxy_file'. Use one or the other."
            )
        if proxies is None and proxy_file is None:
            raise ValueError(
                "Must provide either 'proxies' (iterable) or 'proxy_file' (path)."
            )

        if proxy_file is not None:
            proxy_path = Path(proxy_file)
            if not proxy_path.exists():
                raise FileNotFoundError(f"Proxy file not found: {proxy_file}")
            with proxy_path.open("r", encoding="utf-8") as f:
                self.proxies = [line.strip() for line in f if line.strip()]
        else:
            # At this point, proxies is guaranteed to be not None due to the check above
            assert proxies is not None
            self.proxies = list(proxies)

        if not self.proxies:
            raise ValueError("ProxyMiddleware requires at least one proxy.")

        self.random_selection = random_selection
        self._idx = 0
        self.logger = get_logger(component="ProxyMiddleware")

    async def process_request(self, request: Request, spider: "Spider") -> Request:
        if self.random_selection:
            proxy = random.choice(self.proxies)
        else:
            proxy = self.proxies[self._idx]
            self._idx = (self._idx + 1) % len(self.proxies)
        request.meta.setdefault("proxy", proxy)
        self.logger.debug("Assigned proxy", proxy=proxy, url=request.url)
        return request


class RetryMiddleware:
    def __init__(
        self,
        max_times: int = 3,
        retry_http_codes: Iterable[int] | None = None,
        backoff_base: float = 0.5,
        sleep_http_codes: Iterable[int] | None = None,
    ) -> None:
        self.max_times = max_times
        base_retry_codes = set(
            retry_http_codes or {500, 502, 503, 504, 522, 524, 408, 429}
        )
        sleep_codes = (
            set(sleep_http_codes)
            if sleep_http_codes is not None
            else set(base_retry_codes)
        )
        # Any code we sleep on should also be retried even if it was not
        # included in retry_http_codes.
        self.retry_http_codes = base_retry_codes | sleep_codes
        self.sleep_http_codes = sleep_codes
        self.backoff_base = backoff_base
        self.logger = get_logger(component="RetryMiddleware")

    async def process_response(
        self, response: Response, spider: "Spider"
    ) -> Response | Request:
        request = response.request
        if response.status not in self.retry_http_codes:
            return response

        retry_raw = request.meta.get("retry_times", 0)
        retry_times = retry_raw if isinstance(retry_raw, int) else 0
        if retry_times >= self.max_times:
            return response  # give up

        retry_times += 1
        request = request.replace(dont_filter=True)
        request.meta["retry_times"] = retry_times

        delay = self.backoff_base * (2 ** (retry_times - 1))
        self.logger.warning(
            "Retrying request",
            url=request.url,
            delay=round(delay, 2),
            attempt=retry_times,
            status=response.status,
        )
        if response.status in self.sleep_http_codes and delay > 0:
            # non-blocking sleep to avoid stalling other concurrent fetches
            await asyncio.sleep(delay)

        return request


class _DelayStrategy(Enum):
    """Internal enum to track which delay strategy is configured."""

    FIXED = auto()
    RANDOM = auto()
    CUSTOM = auto()


class DelayMiddleware:
    """
    Middleware to add configurable delays between requests.

    Supports three delay strategies:
    1. Fixed delay: Always wait the same amount of time
    2. Random delay: Wait a random time between min and max
    3. Custom delay: Use a callable that returns delay duration

    Args:
        delay: Fixed delay in seconds, or None if using delay_func
        min_delay: Minimum delay for random strategy (requires max_delay)
        max_delay: Maximum delay for random strategy (requires min_delay)
        delay_func: Custom callable that returns delay in seconds.
                   Called with (request, spider) and should return float.

    Examples:
        Fixed delay of 1 second:
            DelayMiddleware(delay=1.0)

        Random delay between 0.5 and 2 seconds:
            DelayMiddleware(min_delay=0.5, max_delay=2.0)

        Custom delay function:
            def my_delay(request, spider):
                return 1.0 if "fast" in request.url else 2.0
            DelayMiddleware(delay_func=my_delay)
    """

    def __init__(
        self,
        delay: float | None = None,
        min_delay: float | None = None,
        max_delay: float | None = None,
        delay_func: Callable[[Request, "Spider"], float] | None = None,
    ) -> None:
        # Validate configuration and determine strategy
        self._delay_func: Callable[[Request, "Spider"], float] | None = None
        self._min_delay: float | None = None
        self._max_delay: float | None = None
        self._fixed_delay: float | None = None

        if delay_func is not None:
            if delay is not None or min_delay is not None or max_delay is not None:
                raise ValueError(
                    "delay_func cannot be used with delay, min_delay, or max_delay"
                )
            self._strategy = _DelayStrategy.CUSTOM
            self._delay_func = delay_func
        elif min_delay is not None or max_delay is not None:
            if delay is not None:
                raise ValueError("Cannot use both delay and min_delay/max_delay")
            if min_delay is None or max_delay is None:
                raise ValueError("Both min_delay and max_delay must be provided")
            if min_delay < 0 or max_delay < 0:
                raise ValueError("min_delay and max_delay must be non-negative")
            if min_delay > max_delay:
                raise ValueError("min_delay must be less than or equal to max_delay")
            self._strategy = _DelayStrategy.RANDOM
            self._min_delay = min_delay
            self._max_delay = max_delay
        elif delay is not None:
            if delay < 0:
                raise ValueError("delay must be non-negative")
            self._strategy = _DelayStrategy.FIXED
            self._fixed_delay = delay
        else:
            raise ValueError(
                "Must provide one of: delay, min_delay/max_delay, or delay_func"
            )

        self.logger = get_logger(component="DelayMiddleware")

    async def process_request(self, request: Request, spider: "Spider") -> Request:
        """Calculate and apply delay before processing the request."""
        match self._strategy:
            case _DelayStrategy.CUSTOM:
                assert self._delay_func is not None
                delay = self._delay_func(request, spider)
            case _DelayStrategy.RANDOM:
                assert self._min_delay is not None and self._max_delay is not None
                delay = random.uniform(self._min_delay, self._max_delay)
            case _DelayStrategy.FIXED:
                assert self._fixed_delay is not None
                delay = self._fixed_delay
            case other:
                assert_never(other)

        if delay > 0:
            self.logger.debug(
                "Delaying request",
                url=request.url,
                delay=round(delay, 3),
            )
            await asyncio.sleep(delay)

        return request


class SkipNonHTMLMiddleware:
    """
    Response middleware that drops callbacks for non-HTML payloads.

    It checks the Content-Type header first, then falls back to a quick body
    sniff for "<html". Non-HTML responses keep flowing through the engine but
    execute a no-op callback so spider parse methods are skipped.
    Set `request.meta["allow_non_html"] = True` to bypass filtering for a request
    (useful for XML sitemaps, robots.txt fetches, etc.).
    """

    def __init__(
        self,
        allowed_types: Iterable[str] | None = None,
        sniff_bytes: int = 2048,
    ) -> None:
        if sniff_bytes < 0:
            raise ValueError("sniff_bytes must be non-negative")

        self.allowed_types = [t.lower() for t in (allowed_types or ["html"])]
        self.sniff_bytes = sniff_bytes
        self.logger = get_logger(component="SkipNonHTMLMiddleware")

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
        self, response: Response, spider: "Spider"
    ) -> Response | Request:
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
