from __future__ import annotations
import asyncio
import random
from typing import Iterable, Protocol, Sequence

from .request import Request
from .response import Response
from .logging import get_logger

from .spiders import Spider  # type: ignore


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
    def __init__(self, proxies: Iterable[str]) -> None:
        self.proxies = list(proxies)
        if not self.proxies:
            raise ValueError("ProxyMiddleware requires at least one proxy.")
        self._idx = 0
        self.logger = get_logger(component="ProxyMiddleware")

    async def process_request(self, request: Request, spider: "Spider") -> Request:
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
    ) -> None:
        self.max_times = max_times
        self.retry_http_codes = set(
            retry_http_codes or {500, 502, 503, 504, 522, 524, 408, 429}
        )
        self.backoff_base = backoff_base
        self.logger = get_logger(component="RetryMiddleware")

    async def process_response(
        self, response: Response, spider: "Spider"
    ) -> Response | Request:
        request = response.request
        if response.status not in self.retry_http_codes:
            return response

        retry_times = request.meta.get("retry_times", 0)
        if retry_times >= self.max_times:
            return response  # give up

        retry_times += 1
        request = request.replace()
        request.meta["retry_times"] = retry_times

        delay = self.backoff_base * (2 ** (retry_times - 1))
        self.logger.warning(
            "Retrying request",
            url=request.url,
            delay=round(delay, 2),
            attempt=retry_times,
        )
        # non-blocking sleep to avoid stalling other concurrent fetches
        await asyncio.sleep(delay)

        return request
