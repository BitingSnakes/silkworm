from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

from wreq import Client, Method

from .._timeouts import to_seconds
from ..exceptions import HttpError, IgnoreRequest
from ..http import normalize_status
from ..logging import Logger, get_logger
from ..request import Request

if TYPE_CHECKING:
    from ..spiders import Spider


RobotsTxtFetcher: TypeAlias = Callable[[str], Awaitable[str]]
RobotsOrigin: TypeAlias = tuple[str, str, int | None]


class RobotsTxtDelayMiddleware:
    """
    Request middleware that loads robots.txt and applies its delay directives.

    The middleware currently uses `Crawl-delay` first and falls back to
    `Request-rate` when present. Delays are scoped to the origin that served the
    robots.txt file, and concurrent requests are serialized so the configured
    spacing is respected under engine concurrency.

    Args:
        website_url: Absolute HTTP(S) site URL whose origin is throttled.
        user_agent: robots.txt group used to resolve directives.
        fallback_delay: Delay used when no directive exists or loading fails.
        timeout: robots.txt fetch timeout.
        ignore_fetch_errors: Apply the fallback instead of propagating errors.
        fetcher: Optional async robots.txt loader for custom transports or tests.
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

        robots_url, origin = self._normalize_robots_url(website_url)
        self.robots_url: str = robots_url
        self._origin = origin
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
        self.logger: Logger = get_logger(component="RobotsTxtDelayMiddleware")

    async def open(self, spider: Spider) -> None:
        """Load and parse robots.txt before crawl requests begin."""
        await self._ensure_loaded(spider)

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Apply origin-scoped spacing from the loaded robots.txt directives."""
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
            status = normalize_status(getattr(response, "status", 200))
            if status >= 400:
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


class _RobotsFetchError(HttpError):
    """robots.txt could not be fetched; ``status`` is set for HTTP failures."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(slots=True)
class _OriginRules:
    parser: RobotFileParser | None  # None: no restrictions
    disallow_all: bool = False
    delay: float | None = None
    next_request_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class RobotsTxtMiddleware:
    """Obey robots.txt rules for every site the crawl visits.

    Each origin's ``/robots.txt`` is fetched once, on its first request.
    Disallowed requests are dropped with
    :class:`~silkworm.exceptions.IgnoreRequest` (counted as
    ``ignored_requests`` with reason ``robots_txt``); with ``obey_crawl_delay``
    the origin's ``Crawl-delay``/``Request-rate`` also spaces its requests.
    Requests for ``robots.txt`` itself and requests with
    ``meta["dont_obey_robotstxt"]`` are never blocked.

    Following RFC 9309, a 4xx robots.txt response means "no restrictions".
    Server errors and unreachable robots.txt files follow ``on_unavailable``:
    ``"allow"`` (the default, logged as a warning) or ``"disallow"`` to skip the
    origin entirely, as the RFC recommends.

    Args:
        user_agent: Product token matched against robots.txt groups.
        obey_crawl_delay: Apply crawl delays per origin.
        on_unavailable: Policy when robots.txt cannot be fetched.
        timeout: robots.txt fetch timeout.
        fetcher: Optional async loader returning robots.txt text; raise an
            ``HttpError`` subclass with a ``status`` attribute for HTTP errors.
    """

    def __init__(
        self,
        *,
        user_agent: str = "*",
        obey_crawl_delay: bool = True,
        on_unavailable: Literal["allow", "disallow"] = "allow",
        timeout: float | timedelta | None = 10.0,
        fetcher: RobotsTxtFetcher | None = None,
    ) -> None:
        if not user_agent.strip():
            msg = "user_agent must not be empty"
            raise ValueError(msg)
        if on_unavailable not in {"allow", "disallow"}:
            msg = "on_unavailable must be 'allow' or 'disallow'"
            raise ValueError(msg)
        self.user_agent = user_agent
        self.obey_crawl_delay = obey_crawl_delay
        self.on_unavailable = on_unavailable
        self.timeout = timeout
        self._fetcher = fetcher or self._fetch_robots_txt
        self._rules: dict[RobotsOrigin, _OriginRules] = {}
        self._loading: dict[RobotsOrigin, asyncio.Lock] = {}
        self.logger: Logger = get_logger(component="RobotsTxtMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Drop requests robots.txt disallows and apply crawl delays."""
        parts = urlsplit(request.url)
        origin = _origin(parts)
        if (
            origin is None
            or parts.path == "/robots.txt"
            or request.meta.get("dont_obey_robotstxt")
        ):
            return request

        rules = await self._rules_for(origin, parts.scheme, parts.netloc)
        if rules.disallow_all or (
            rules.parser is not None
            and not rules.parser.can_fetch(self.user_agent, request.url)
        ):
            raise IgnoreRequest(
                f"robots.txt disallows {request.url}", reason="robots_txt"
            )

        if self.obey_crawl_delay and rules.delay:
            loop = asyncio.get_running_loop()
            async with rules.lock:
                wait = rules.next_request_at - loop.time()
                if wait > 0:
                    await asyncio.sleep(wait)
                rules.next_request_at = loop.time() + rules.delay
        return request

    async def _rules_for(
        self, origin: RobotsOrigin, scheme: str, netloc: str
    ) -> _OriginRules:
        rules = self._rules.get(origin)
        if rules is not None:
            return rules
        lock = self._loading.setdefault(origin, asyncio.Lock())
        async with lock:
            rules = self._rules.get(origin)
            if rules is None:
                rules = await self._load(scheme, netloc)
                self._rules[origin] = rules
                self._loading.pop(origin, None)
        return rules

    async def _load(self, scheme: str, netloc: str) -> _OriginRules:
        robots_url = urlunsplit((scheme.lower(), netloc, "/robots.txt", "", ""))
        try:
            robots_txt = await self._fetcher(robots_url)
        except Exception as exc:  # noqa: BLE001 - every failure maps to a policy
            status = getattr(exc, "status", None)
            if isinstance(status, int) and 400 <= status < 500:
                self.logger.debug(
                    "robots.txt not available; no restrictions",
                    robots_url=robots_url,
                    status=status,
                )
                return _OriginRules(parser=None)
            self.logger.warning(
                "Failed to load robots.txt",
                robots_url=robots_url,
                error=str(exc),
                error_type=exc.__class__.__name__,
                policy=self.on_unavailable,
            )
            return _OriginRules(
                parser=None, disallow_all=self.on_unavailable == "disallow"
            )

        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(robots_txt.splitlines())
        delay = parse_crawl_delay(robots_txt, self.user_agent)
        if delay is None:
            rate = parser.request_rate(self.user_agent)
            if rate is not None and rate.requests > 0:
                delay = rate.seconds / rate.requests
        self.logger.info(
            "Loaded robots.txt",
            robots_url=robots_url,
            user_agent=self.user_agent,
            crawl_delay=delay,
        )
        return _OriginRules(parser=parser, delay=delay)

    async def _fetch_robots_txt(self, robots_url: str) -> str:
        client = cast(Any, Client)()
        response: Any = None
        try:
            kwargs: dict[str, object] = {}
            request_timeout = to_seconds(self.timeout)
            if request_timeout is not None:
                kwargs["timeout"] = timedelta(seconds=request_timeout)
            response = await client.request(Method.GET, robots_url, **kwargs)
            status_code = normalize_status(getattr(response, "status", 200))
            if status_code >= 400:
                raise _RobotsFetchError(
                    f"robots.txt request failed with status {status_code}",
                    status=status_code,
                )
            body = await response.bytes()
            return bytes(body).decode("utf-8", errors="replace")
        finally:
            for resource in (response, client):
                if resource is None:
                    continue
                closer = getattr(resource, "aclose", None) or getattr(
                    resource, "close", None
                )
                if callable(closer):
                    result = closer()
                    if inspect.isawaitable(result):
                        await result


def parse_crawl_delay(robots_txt: str, user_agent: str) -> float | None:
    """Return the ``Crawl-delay`` for ``user_agent``, accepting decimal values.

    ``urllib.robotparser`` ignores non-integer delays such as ``0.5``. Groups
    are matched like ``robotparser``: a group naming the user agent's product
    token wins over the ``*`` group.
    """
    token = user_agent.split("/", 1)[0].strip().lower()
    groups: list[tuple[list[str], float | None]] = []
    agents: list[str] = []
    delay: float | None = None
    in_rules = False
    for raw_line in robots_txt.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        field, value = (part.strip() for part in line.split(":", 1))
        field = field.lower()
        if field == "user-agent":
            if in_rules:
                groups.append((agents, delay))
                agents, delay, in_rules = [], None, False
            agents.append(value.lower())
        elif agents:
            in_rules = True
            if field == "crawl-delay":
                try:
                    parsed = float(value)
                except ValueError:
                    continue
                if parsed >= 0:
                    delay = parsed
    if agents:
        groups.append((agents, delay))

    default: float | None = None
    for group_agents, group_delay in groups:
        if any(agent != "*" and agent in token for agent in group_agents):
            return group_delay
        if "*" in group_agents and default is None:
            default = group_delay
    return default


def _origin(parts: Any) -> RobotsOrigin | None:
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"} or not parts.hostname:
        return None
    try:
        port = parts.port
    except ValueError:
        return None
    return (scheme, parts.hostname.lower(), port or (443 if scheme == "https" else 80))
