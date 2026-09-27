from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

from .._domains import url_host
from ..logging import Logger, get_logger
from ..request import Request
from ..response import Response

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..spiders import Spider

# Request meta key holding the monotonic time a request was released.
_START_META_KEY = "_autothrottle_start"


@dataclass(slots=True)
class _HostState:
    delay: float
    next_request_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AutoThrottleMiddleware:
    """Space requests per host and adapt the spacing to the host's latency.

    Register the same instance as both a request and a response middleware::

        throttle = AutoThrottleMiddleware(start_delay=1.0, max_delay=30.0)
        run_spider(
            MySpider,
            request_middlewares=[throttle],
            response_middlewares=[throttle],
        )

    Requests to one host are released at least ``delay`` seconds apart. After
    each response the delay moves toward ``latency / target_concurrency`` (an
    average of the previous and new estimate), so slow hosts are crawled more
    gently and fast hosts more quickly. Throttling statuses (429/503 by default)
    double the delay and honour a ``Retry-After`` header; other error responses
    never lower it. The delay always stays within ``[min_delay, max_delay]``.

    Args:
        start_delay: Initial per-host delay in seconds.
        min_delay: Lower bound for the delay.
        max_delay: Upper bound for the delay.
        target_concurrency: Average number of requests to keep in flight per
            host; higher values crawl faster.
        backoff_statuses: Statuses that signal the host is overloaded.
    """

    def __init__(
        self,
        *,
        start_delay: float = 1.0,
        min_delay: float = 0.0,
        max_delay: float = 60.0,
        target_concurrency: float = 1.0,
        backoff_statuses: Iterable[int] = (429, 503),
    ) -> None:
        if min_delay < 0:
            msg = "min_delay must be non-negative"
            raise ValueError(msg)
        if max_delay < min_delay:
            msg = "max_delay must be greater than or equal to min_delay"
            raise ValueError(msg)
        if not min_delay <= start_delay <= max_delay:
            msg = "start_delay must be between min_delay and max_delay"
            raise ValueError(msg)
        if target_concurrency <= 0:
            msg = "target_concurrency must be positive"
            raise ValueError(msg)
        self.start_delay: float = start_delay
        self.min_delay: float = min_delay
        self.max_delay: float = max_delay
        self.target_concurrency: float = target_concurrency
        self.backoff_statuses: frozenset[int] = frozenset(backoff_statuses)
        self._hosts: dict[str, _HostState] = {}
        self.logger: Logger = get_logger(component="AutoThrottleMiddleware")

    def delay_for(self, url: str) -> float:
        """Return the current delay for ``url``'s host (the start delay if unseen)."""
        state = self._hosts.get(url_host(url))
        return state.delay if state is not None else self.start_delay

    def _state(self, url: str) -> _HostState:
        host = url_host(url)
        state = self._hosts.get(host)
        if state is None:
            state = self._hosts[host] = _HostState(delay=self.start_delay)
        return state

    def _clamp(self, delay: float) -> float:
        return min(self.max_delay, max(self.min_delay, delay))

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Wait for the host's next slot, then release the request."""
        state = self._state(request.url)
        async with state.lock:
            wait = state.next_request_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            now = time.monotonic()
            state.next_request_at = now + state.delay
        return request.replace(meta={**request.meta, _START_META_KEY: now})

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        """Adjust the host's delay from the response latency and status."""
        state = self._state(response.request.url)
        started = response.request.meta.get(_START_META_KEY)
        now = time.monotonic()
        previous = state.delay

        if response.status in self.backoff_statuses:
            state.delay = self._clamp(max(state.delay * 2, self.start_delay, 0.1))
            retry_after = self._retry_after(response)
            if retry_after is not None:
                state.next_request_at = max(state.next_request_at, now + retry_after)
        elif isinstance(started, (int, float)) and not isinstance(started, bool):
            latency = max(0.0, now - float(started))
            target = latency / self.target_concurrency
            new_delay = (state.delay + target) / 2
            # Error responses are often fast; never let them speed up crawling.
            if response.status < 400 or new_delay > state.delay:
                state.delay = self._clamp(new_delay)

        if state.delay != previous:
            self.logger.debug(
                "Adjusted host delay",
                host=url_host(response.request.url),
                status=response.status,
                delay=round(state.delay, 3),
                previous_delay=round(previous, 3),
            )
        return response

    def _retry_after(self, response: Response) -> float | None:
        raw = response.headers.get("retry-after", "").strip()
        if not raw:
            return None
        if raw.isdigit():
            return min(float(raw), self.max_delay)
        try:
            retry_at = parsedate_to_datetime(raw).timestamp()
        except (TypeError, ValueError):
            return None
        return min(max(0.0, retry_at - time.time()), self.max_delay)
