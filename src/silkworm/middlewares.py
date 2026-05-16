from __future__ import annotations

import asyncio
import base64
import json
import random
import re
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from email.message import Message
from enum import Enum, auto
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy, MozillaCookieJar
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast, assert_never
from urllib.parse import urlsplit
from uuid import uuid4

from wreq import Client, Method  # type: ignore[import]

from ._types import JSONValue
from .exceptions import HttpError
from .http import HttpClient, MOCK_RESPONSE_META_KEY
from .logging import get_logger
from .request import Request
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from .spiders import Spider


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


class RequestResponseStreamMiddleware:
    """
    Stream request/response telemetry to a remote HTTP endpoint.

    Each outbound request receives a unique `exchange_id`. The middleware emits
    a `request` event before fetch and a `response` event after fetch, both
    carrying the same identifier so downstream analysis can join them.

    Use the same middleware instance in both `request_middlewares` and
    `response_middlewares` so a single sender queue can stream the full
    exchange lifecycle:

        stream = RequestResponseStreamMiddleware(
            "https://collector.example.com/events",
            auth_token="secret-token",
            batch_size=50,
        )

        run_spider(
            MySpider,
            request_middlewares=[stream],
            response_middlewares=[stream],
        )
    """

    _EXCHANGE_ID_META_KEY = "_stream_exchange_id"
    _PARENT_EXCHANGE_ID_META_KEY = "_stream_parent_exchange_id"
    _STARTED_AT_META_KEY = "_stream_started_at"
    _STOP = object()

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float | timedelta | None = 10.0,
        max_body_bytes: int = 64_000,
        queue_size: int = 1_000,
        auth_token: str | None = None,
        auth_scheme: str = "Bearer",
        batch_size: int = 1,
        batch_envelope_key: str = "events",
    ) -> None:
        if not url.strip():
            msg = "url must not be empty"
            raise ValueError(msg)
        if max_body_bytes < 0:
            msg = "max_body_bytes must be non-negative"
            raise ValueError(msg)
        if queue_size <= 0:
            msg = "queue_size must be greater than 0"
            raise ValueError(msg)
        if auth_token is not None and not auth_token.strip():
            msg = "auth_token must not be empty when provided"
            raise ValueError(msg)
        if not auth_scheme.strip():
            msg = "auth_scheme must not be empty"
            raise ValueError(msg)
        if batch_size <= 0:
            msg = "batch_size must be greater than 0"
            raise ValueError(msg)
        if not batch_envelope_key.strip():
            msg = "batch_envelope_key must not be empty"
            raise ValueError(msg)

        self.url = url
        self.method = method
        self.headers = dict(headers or {})
        self.timeout = timeout
        self.max_body_bytes = max_body_bytes
        self.queue_size = queue_size
        self.auth_token = auth_token
        self.auth_scheme = auth_scheme
        self.batch_size = batch_size
        self.batch_envelope_key = batch_envelope_key

        self._client: Client | None = None  # type: ignore[name-defined]
        self._queue: asyncio.Queue[JSONValue | object] | None = None
        self._sender_task: asyncio.Task[None] | None = None
        self._dropped_events = 0
        self.logger = get_logger(component="RequestResponseStreamMiddleware")

        if self.auth_token is not None and "Authorization" not in self.headers:
            self.headers["Authorization"] = (
                f"{self.auth_scheme.strip()} {self.auth_token}"
            )

    async def open(self, spider: Spider) -> None:
        await self._ensure_started()
        self.logger.info(
            "Opened request/response stream middleware",
            spider=spider.name,
            url=self.url,
            method=self.method,
            queue_size=self.queue_size,
            batch_size=self.batch_size,
        )

    async def close(self, spider: Spider) -> None:
        queue = self._queue
        sender_task = self._sender_task

        if queue is not None and sender_task is not None:
            await queue.put(self._STOP)
            await sender_task

        self._sender_task = None
        self._queue = None
        self._dropped_events = 0

        client = self._client
        self._client = None
        if client is not None:
            closer = getattr(client, "aclose", None) or getattr(client, "close", None)
            if closer and callable(closer):
                result = closer()
                if hasattr(result, "__await__"):
                    await result  # type: ignore[misc]

        self.logger.info(
            "Closed request/response stream middleware",
            spider=spider.name,
            url=self.url,
        )

    async def process_request(self, request: Request, spider: Spider) -> Request:
        previous_exchange_id = request.meta.get(self._EXCHANGE_ID_META_KEY)
        exchange_id = uuid4().hex
        request.meta[self._EXCHANGE_ID_META_KEY] = exchange_id
        request.meta[self._STARTED_AT_META_KEY] = time.perf_counter()
        if isinstance(previous_exchange_id, str):
            request.meta[self._PARENT_EXCHANGE_ID_META_KEY] = previous_exchange_id

        await self._enqueue(
            self._request_event(request, spider, exchange_id, previous_exchange_id),
        )
        return request

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        exchange_id = response.request.meta.get(self._EXCHANGE_ID_META_KEY)
        if not isinstance(exchange_id, str):
            exchange_id = uuid4().hex
            response.request.meta[self._EXCHANGE_ID_META_KEY] = exchange_id

        await self._enqueue(self._response_event(response, spider, exchange_id))
        return response

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        exchange_id = request.meta.get(self._EXCHANGE_ID_META_KEY)
        if not isinstance(exchange_id, str):
            exchange_id = uuid4().hex
            request.meta[self._EXCHANGE_ID_META_KEY] = exchange_id

        await self._enqueue(
            {
                "event": "request_error",
                "exchange_id": exchange_id,
                "parent_exchange_id": self._parent_exchange_id(request),
                "timestamp": _utc_timestamp(),
                "spider": spider.name,
                "duration_ms": self._duration_ms(request),
                "error": str(exception),
                "error_type": exception.__class__.__name__,
                "request": self._serialize_request(request),
            },
        )
        return None

    async def _ensure_started(self) -> None:
        if self._client is None:
            self._client = Client()  # type: ignore[misc]
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self.queue_size)
        if self._sender_task is None:
            self._sender_task = asyncio.create_task(self._run_sender())

    async def _enqueue(self, payload: JSONValue) -> None:
        await self._ensure_started()
        queue = self._queue
        if queue is None:
            return

        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            self._dropped_events += 1
            self.logger.warning(
                "Dropping stream event because queue is full",
                dropped_events=self._dropped_events,
                url=self.url,
            )

    async def _run_sender(self) -> None:
        queue = self._queue
        if queue is None:
            return
        batch: list[JSONValue] = []

        while True:
            payload = await queue.get()
            try:
                if payload is self._STOP:
                    if batch:
                        await self._send_payload(self._build_payload(batch))
                    return
                if not isinstance(payload, dict):
                    continue

                batch.append(payload)
                if len(batch) >= self.batch_size:
                    await self._send_payload(self._build_payload(batch))
                    batch.clear()
            finally:
                queue.task_done()

    async def _send_payload(self, payload: JSONValue | object) -> None:
        client = self._client
        if client is None or payload is self._STOP:
            return

        method_upper = self.method.upper()
        if not hasattr(Method, method_upper):  # type: ignore[attr-defined]
            raise ValueError(
                f"Invalid HTTP method '{self.method}'. Must be one of: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
            )

        method = getattr(Method, method_upper)  # type: ignore[attr-defined]
        request_kwargs: dict[str, object] = {
            "headers": self.headers,
            "json": payload,
        }

        timeout = _timeout_seconds(self.timeout)
        if timeout is not None:
            request_kwargs["timeout"] = timedelta(seconds=timeout)

        response = None
        try:
            response = await cast(Any, client).request(
                method, self.url, **request_kwargs
            )
            status = getattr(response, "status", None)
            self.logger.debug(
                "Streamed telemetry event",
                url=self.url,
                status=status,
                event_type=payload.get("event") if isinstance(payload, dict) else None,
            )
        except Exception as exc:
            self.logger.warning(
                "Failed to stream telemetry event",
                url=self.url,
                error=str(exc),
                error_type=exc.__class__.__name__,
            )
        finally:
            if response is not None:
                closer = getattr(response, "aclose", None) or getattr(
                    response,
                    "close",
                    None,
                )
                if closer and callable(closer):
                    result = closer()
                    if hasattr(result, "__await__"):
                        await result  # type: ignore[misc]

    def _build_payload(self, events: list[JSONValue]) -> JSONValue:
        if len(events) == 1:
            return events[0]
        return {
            self.batch_envelope_key: list(events),
            "count": len(events),
        }

    def _request_event(
        self,
        request: Request,
        spider: Spider,
        exchange_id: str,
        previous_exchange_id: JSONValue,
    ) -> dict[str, JSONValue]:
        return {
            "event": "request",
            "exchange_id": exchange_id,
            "parent_exchange_id": (
                previous_exchange_id if isinstance(previous_exchange_id, str) else None
            ),
            "timestamp": _utc_timestamp(),
            "spider": spider.name,
            "request": self._serialize_request(request),
        }

    def _response_event(
        self,
        response: Response,
        spider: Spider,
        exchange_id: str,
    ) -> dict[str, JSONValue]:
        return {
            "event": "response",
            "exchange_id": exchange_id,
            "parent_exchange_id": self._parent_exchange_id(response.request),
            "timestamp": _utc_timestamp(),
            "spider": spider.name,
            "duration_ms": self._duration_ms(response.request),
            "request": self._serialize_request(response.request),
            "response": self._serialize_response(response),
        }

    def _parent_exchange_id(self, request: Request) -> str | None:
        parent = request.meta.get(self._PARENT_EXCHANGE_ID_META_KEY)
        return parent if isinstance(parent, str) else None

    def _duration_ms(self, request: Request) -> float | None:
        started_at = request.meta.get(self._STARTED_AT_META_KEY)
        if not isinstance(started_at, (int, float)):
            return None
        return round((time.perf_counter() - float(started_at)) * 1000, 3)

    def _serialize_request(self, request: Request) -> dict[str, JSONValue]:
        return {
            "url": request.url,
            "method": request.method,
            "headers": dict(request.headers),
            "params": self._serialize_params(request.params),
            "body": self._serialize_request_body(request),
            "meta": self._serialize_meta(request.meta),
            "timeout_seconds": _timeout_seconds(request.timeout),
            "callback": _callback_name(request.callback),
            "dont_filter": request.dont_filter,
            "priority": request.priority,
        }

    def _serialize_response(self, response: Response) -> dict[str, JSONValue]:
        return {
            "url": response.url,
            "status": response.status,
            "headers": dict(response.headers),
            "body": self._serialize_bytes(response.body),
            "encoding": response.encoding,
        }

    def _serialize_request_body(self, request: Request) -> JSONValue:
        if request.json is not None:
            return {
                "kind": "json",
                "value": request.json,
            }

        body = request.data
        if body is None:
            return None
        if isinstance(body, str):
            return self._serialize_text(body)
        if isinstance(body, Mapping):
            return {
                "kind": "mapping",
                "value": self._serialize_mapping(cast(Mapping[object, object], body)),
            }
        if isinstance(body, (bytes, bytearray, memoryview)):
            return self._serialize_bytes(bytes(body))

        try:
            items = list(body)
        except TypeError:
            return {"kind": "repr", "value": repr(body)}

        serialized_items: list[JSONValue] = []
        for item in items:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
                serialized_items.append([str(key), str(value)])
            else:
                serialized_items.append(str(item))

        return {
            "kind": "sequence",
            "value": serialized_items,
        }

    def _serialize_params(self, params: Mapping[str, object]) -> dict[str, JSONValue]:
        return {key: self._coerce_json_value(value) for key, value in params.items()}

    def _serialize_meta(self, meta: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
        return {
            key: value
            for key, value in meta.items()
            if key
            not in {
                self._EXCHANGE_ID_META_KEY,
                self._PARENT_EXCHANGE_ID_META_KEY,
                self._STARTED_AT_META_KEY,
            }
        }

    def _serialize_mapping(
        self, mapping: Mapping[object, object]
    ) -> dict[str, JSONValue]:
        return {
            str(key): self._coerce_json_value(value) for key, value in mapping.items()
        }

    def _coerce_json_value(self, value: object) -> JSONValue:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Mapping):
            return self._serialize_mapping(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self._serialize_bytes(bytes(value))
        if isinstance(value, list):
            return [self._coerce_json_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._coerce_json_value(item) for item in value]
        if isinstance(value, set):
            return [self._coerce_json_value(item) for item in value]
        return repr(value)

    def _serialize_text(self, value: str) -> dict[str, JSONValue]:
        encoded = value.encode("utf-8", errors="replace")
        payload = self._serialize_bytes(encoded)
        payload["kind"] = "text"
        return payload

    def _serialize_bytes(self, body: bytes) -> dict[str, JSONValue]:
        truncated = body[: self.max_body_bytes]
        encoded = base64.b64encode(truncated).decode("ascii")
        return {
            "kind": "base64",
            "base64": encoded,
            "size": len(body),
            "truncated": len(truncated) < len(body),
        }


class UserAgentMiddleware:
    def __init__(
        self,
        user_agents: Sequence[str] | None = None,
        *,
        default: str | None = None,
    ) -> None:
        self.user_agents = list(user_agents or [])
        self.default = default or "silkworm/0.1"
        self.logger = get_logger(component="UserAgentMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        ua = None
        if self.user_agents:
            ua = random.choice(self.user_agents)
        else:
            ua = self.default
        request.headers.setdefault("User-Agent", ua)
        self.logger.debug("Assigned user agent", user_agent=ua, url=request.url)
        return request


class ProxyMiddleware:
    _FAILED_PROXIES_META_KEY = "_proxy_failed_proxies"
    _PROXY_RETRY_TIMES_META_KEY = "_proxy_retry_times"

    def __init__(
        self,
        proxies: Iterable[str] | None = None,
        proxy_file: str | Path | None = None,
        random_selection: bool = False,
    ) -> None:
        if proxies is not None and proxy_file is not None:
            msg = (
                "Cannot specify both 'proxies' and 'proxy_file'. Use one or the other."
            )
            raise ValueError(msg)
        if proxies is None and proxy_file is None:
            msg = "Must provide either 'proxies' (iterable) or 'proxy_file' (path)."
            raise ValueError(msg)

        if proxy_file is not None:
            proxy_path = Path(proxy_file)
            if not proxy_path.exists():
                msg = f"Proxy file not found: {proxy_file}"
                raise FileNotFoundError(msg)
            with proxy_path.open("r", encoding="utf-8") as f:
                self.proxies = [line.strip() for line in f if line.strip()]
        else:
            # At this point, proxies is guaranteed to be not None due to the check above
            assert proxies is not None
            self.proxies = list(proxies)

        if not self.proxies:
            msg = "ProxyMiddleware requires at least one proxy."
            raise ValueError(msg)

        self.random_selection = random_selection
        self._idx = 0
        self.logger = get_logger(component="ProxyMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        proxy = request.meta.get("proxy")
        if isinstance(proxy, str):
            self.logger.debug("Using existing proxy", proxy=proxy, url=request.url)
            return request

        proxy = self._select_proxy()
        if proxy is None:
            return request

        request.meta.setdefault("proxy", proxy)
        self.logger.debug("Assigned proxy", proxy=proxy, url=request.url)
        return request

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        current_proxy = request.meta.get("proxy")
        if not isinstance(current_proxy, str):
            return None

        failed_proxies = self._get_failed_proxies(request)
        failed_proxies.append(current_proxy)
        next_proxy = self._select_proxy(excluded=failed_proxies)
        if next_proxy is None:
            self.logger.warning(
                "No proxy left to retry failed request",
                url=request.url,
                error=str(exception),
                error_type=exception.__class__.__name__,
                failed_proxies=failed_proxies,
            )
            return None

        retry_request = request.replace(
            meta={**request.meta},
            dont_filter=True,
        )
        retry_request.meta["proxy"] = next_proxy
        failed_proxies_meta: list[JSONValue] = [proxy for proxy in failed_proxies]
        retry_request.meta[self._FAILED_PROXIES_META_KEY] = failed_proxies_meta

        retry_raw = retry_request.meta.get(self._PROXY_RETRY_TIMES_META_KEY, 0)
        retry_times = retry_raw if isinstance(retry_raw, int) else 0
        retry_request.meta[self._PROXY_RETRY_TIMES_META_KEY] = retry_times + 1

        self.logger.warning(
            "Retrying failed request with another proxy",
            url=request.url,
            error=str(exception),
            error_type=exception.__class__.__name__,
            old_proxy=current_proxy,
            new_proxy=next_proxy,
            attempt=retry_times + 1,
        )
        return retry_request

    def _select_proxy(self, *, excluded: Sequence[str] = ()) -> str | None:
        available_proxies = [proxy for proxy in self.proxies if proxy not in excluded]
        if not available_proxies:
            return None

        if self.random_selection:
            return random.choice(available_proxies)

        for offset in range(len(self.proxies)):
            candidate_idx = (self._idx + offset) % len(self.proxies)
            proxy = self.proxies[candidate_idx]
            if proxy in excluded:
                continue
            self._idx = (candidate_idx + 1) % len(self.proxies)
            return proxy

        return None

    def _get_failed_proxies(self, request: Request) -> list[str]:
        failed_raw = request.meta.get(self._FAILED_PROXIES_META_KEY, [])
        if not isinstance(failed_raw, list):
            return []
        return [proxy for proxy in failed_raw if isinstance(proxy, str)]


class CookiesMiddleware:
    """
    Stateful cookie middleware using Python's standards-compliant CookieJar.

    By default all requests share one cookie jar. Set `request.meta["cookiejar"]`
    to a string or integer to isolate sessions, set `request.meta["cookies"]` to
    add per-request cookies, and set `request.meta["dont_merge_cookies"] = True`
    to bypass cookie handling for a request/response pair.
    """

    _COOKIEJAR_META_KEY = "cookiejar"
    _COOKIES_META_KEY = "cookies"
    _DONT_MERGE_META_KEY = "dont_merge_cookies"
    _DEFAULT_JAR_KEY = "default"
    _COOKIE_PAIR_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+=")

    def __init__(
        self,
        *,
        cookies: Mapping[str, object] | None = None,
        enabled: bool = True,
        allow_domains: Iterable[str] | None = None,
        block_domains: Iterable[str] | None = None,
        rfc2965: bool = False,
        hide_cookie_header: bool = True,
    ) -> None:
        self.enabled = enabled
        self.hide_cookie_header = hide_cookie_header
        self._policy = DefaultCookiePolicy(
            allowed_domains=self._normalize_domain_list(allow_domains),
            blocked_domains=self._normalize_domain_list(block_domains),
            rfc2965=rfc2965,
        )
        self._jars: dict[str | int, CookieJar] = {}
        self._lock = asyncio.Lock()
        self.logger = get_logger(component="CookiesMiddleware")

        if cookies:
            jar = self._jar_for(self._DEFAULT_JAR_KEY)
            for name, value in cookies.items():
                if value is None:
                    continue
                jar.set_cookie(
                    self._make_cookie(
                        name=str(name),
                        value=str(value),
                        domain="",
                        path="/",
                        secure=False,
                    ),
                )

    async def process_request(self, request: Request, spider: Spider) -> Request:
        if not self.enabled or self._dont_merge(request):
            return request

        async with self._lock:
            jar = self._jar_for_request(request)
            self._store_request_cookies(jar, request)
            cookie_request = _CookieRequest(
                request, hide_cookie=self.hide_cookie_header
            )
            cast(Any, jar).add_cookie_header(cookie_request)
            cookie_request.apply()

        if self._has_cookie_header(request.headers):
            self.logger.debug("Applied cookies", url=request.url)
        return request

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        if not self.enabled or self._dont_merge(response.request):
            return response

        set_cookie_headers = list(self._iter_set_cookie_headers(response.headers))
        if not set_cookie_headers:
            return response

        async with self._lock:
            jar = self._jar_for_request(response.request)
            cast(Any, jar).extract_cookies(
                _CookieResponse(set_cookie_headers),
                _CookieRequest(response.request, hide_cookie=False),
            )

        self.logger.debug(
            "Stored response cookies",
            url=response.url,
            count=len(set_cookie_headers),
        )
        return response

    def clear(self, cookiejar: str | int | None = None) -> None:
        """Clear all cookies, or only the named cookie jar."""
        if cookiejar is None:
            self._jars.clear()
            return
        self._jars.pop(cookiejar, None)

    def clear_session_cookies(self, cookiejar: str | int | None = None) -> None:
        """Discard session cookies from all jars, or a single named jar."""
        if cookiejar is not None:
            jar = self._jars.get(cookiejar)
            if jar is not None:
                jar.clear_session_cookies()
            return

        for jar in self._jars.values():
            jar.clear_session_cookies()

    def set_cookie(
        self,
        name: str,
        value: str,
        *,
        domain: str,
        path: str = "/",
        secure: bool = False,
        expires: int | None = None,
        cookiejar: str | int | None = None,
    ) -> None:
        """Add or replace one cookie in the selected jar."""
        self._jar_for(cookiejar or self._DEFAULT_JAR_KEY).set_cookie(
            self._make_cookie(
                name=name,
                value=value,
                domain=domain,
                path=path,
                secure=secure,
                expires=expires,
            ),
        )

    def save(
        self,
        path: str | Path,
        *,
        cookiejar: str | int | None = None,
        ignore_discard: bool = True,
        ignore_expires: bool = True,
    ) -> None:
        """Save cookies from one jar to a Netscape/Mozilla cookie file."""
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        file_jar = MozillaCookieJar(str(output_path))
        file_jar.set_policy(self._policy)
        for cookie in self._jar_for(cookiejar or self._DEFAULT_JAR_KEY):
            file_jar.set_cookie(cookie)
        file_jar.save(
            str(output_path),
            ignore_discard=ignore_discard,
            ignore_expires=ignore_expires,
        )

    def load(
        self,
        path: str | Path,
        *,
        cookiejar: str | int | None = None,
        ignore_discard: bool = True,
        ignore_expires: bool = True,
        clear_existing: bool = False,
    ) -> None:
        """Load cookies from a Netscape/Mozilla cookie file into one jar."""
        input_path = Path(path)
        file_jar = MozillaCookieJar(str(input_path))
        file_jar.set_policy(self._policy)
        file_jar.load(
            str(input_path),
            ignore_discard=ignore_discard,
            ignore_expires=ignore_expires,
        )

        target = self._jar_for(cookiejar or self._DEFAULT_JAR_KEY)
        if clear_existing:
            target.clear()
        for cookie in file_jar:
            target.set_cookie(cookie)

    def _jar_for_request(self, request: Request) -> CookieJar:
        raw_key = request.meta.get(self._COOKIEJAR_META_KEY, self._DEFAULT_JAR_KEY)
        key: str | int
        if isinstance(raw_key, int):
            key = raw_key
        elif isinstance(raw_key, str) and raw_key:
            key = raw_key
        else:
            key = self._DEFAULT_JAR_KEY
        return self._jar_for(key)

    def _jar_for(self, key: str | int) -> CookieJar:
        jar = self._jars.get(key)
        if jar is None:
            jar = CookieJar(policy=self._policy)
            self._jars[key] = jar
        return jar

    def _store_request_cookies(self, jar: CookieJar, request: Request) -> None:
        raw_cookies = request.meta.get(self._COOKIES_META_KEY)
        if not isinstance(raw_cookies, Mapping):
            return

        split = urlsplit(request.url)
        host = split.hostname or ""
        path = self._default_cookie_path(split.path)
        secure = split.scheme.lower() == "https"
        for name, value in raw_cookies.items():
            if not isinstance(name, str) or value is None:
                continue
            jar.set_cookie(
                self._make_cookie(
                    name=name,
                    value=str(value),
                    domain=host,
                    path=path,
                    secure=secure,
                ),
            )

    def _iter_set_cookie_headers(
        self,
        headers: Mapping[str, object],
    ) -> Iterable[str]:
        for key, raw_value in headers.items():
            if str(key).lower() != "set-cookie":
                continue
            if isinstance(raw_value, (list, tuple)):
                for value in raw_value:
                    yield from self._split_set_cookie_header(str(value))
            else:
                yield from self._split_set_cookie_header(str(raw_value))

    def _split_set_cookie_header(self, value: str) -> Iterable[str]:
        value = value.strip()
        if not value:
            return []

        parts: list[str] = []
        start = 0
        for index, char in enumerate(value):
            if char != ",":
                continue
            candidate = value[index + 1 :].lstrip()
            if self._COOKIE_PAIR_RE.match(candidate):
                parts.append(value[start:index].strip())
                start = index + 1
        parts.append(value[start:].strip())
        return [part for part in parts if part]

    def _make_cookie(
        self,
        *,
        name: str,
        value: str,
        domain: str,
        path: str,
        secure: bool,
        expires: int | None = None,
    ) -> Cookie:
        domain_specified = bool(domain)
        return Cookie(
            version=0,
            name=name,
            value=value,
            port=None,
            port_specified=False,
            domain=domain,
            domain_specified=domain_specified,
            domain_initial_dot=domain.startswith("."),
            path=path or "/",
            path_specified=True,
            secure=secure,
            expires=expires,
            discard=expires is None,
            comment=None,
            comment_url=None,
            rest={},
            rfc2109=False,
        )

    def _dont_merge(self, request: Request) -> bool:
        return request.meta.get(self._DONT_MERGE_META_KEY) is True

    def _has_cookie_header(self, headers: Mapping[str, object]) -> bool:
        return any(str(key).lower() == "cookie" for key in headers)

    def _default_cookie_path(self, path: str) -> str:
        if not path or not path.startswith("/"):
            return "/"
        if path.count("/") <= 1:
            return "/"
        return path.rsplit("/", 1)[0] or "/"

    def _normalize_domain_list(
        self,
        domains: Iterable[str] | None,
    ) -> tuple[str, ...] | None:
        if domains is None:
            return None
        return tuple(domain.strip().lower() for domain in domains if domain.strip())


class _CookieRequest:
    def __init__(self, request: Request, *, hide_cookie: bool) -> None:
        self._request = request
        self._hide_cookie = hide_cookie
        split = urlsplit(request.url)
        self.type = split.scheme
        self.host = split.netloc
        self.origin_req_host = split.hostname or split.netloc
        self.unverifiable = False
        self._headers = dict(request.headers)

    def get_full_url(self) -> str:
        return self._request.url

    def get_host(self) -> str:
        return self.host

    def has_header(self, name: str) -> bool:
        if self._hide_cookie and name.lower() == "cookie":
            return False
        return any(key.lower() == name.lower() for key in self._headers)

    def get_header(self, name: str, default: str | None = None) -> str | None:
        for key, value in self._headers.items():
            if key.lower() == name.lower():
                return value
        return default

    def header_items(self) -> list[tuple[str, str]]:
        return list(self._headers.items())

    def add_unredirected_header(self, name: str, value: str) -> None:
        for key in list(self._headers):
            if key.lower() == name.lower():
                del self._headers[key]
        self._headers[name] = value

    def apply(self) -> None:
        for key in list(self._request.headers):
            if key.lower() == "cookie":
                del self._request.headers[key]
        cookie = self.get_header("Cookie")
        if cookie:
            self._request.headers["Cookie"] = cookie


class _CookieResponse:
    def __init__(self, set_cookie_headers: Iterable[str]) -> None:
        self._message = Message()
        for value in set_cookie_headers:
            self._message.add_header("Set-Cookie", value)

    def info(self) -> Message:
        return self._message


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
            retry_http_codes or {500, 502, 503, 504, 522, 524, 408, 429},
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
        self,
        response: Response,
        spider: Spider,
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
        delay_func: Callable[[Request, Spider], float] | None = None,
    ) -> None:
        # Validate configuration and determine strategy
        self._delay_func: Callable[[Request, Spider], float] | None = None
        self._min_delay: float | None = None
        self._max_delay: float | None = None
        self._fixed_delay: float | None = None

        if delay_func is not None:
            if delay is not None or min_delay is not None or max_delay is not None:
                msg = "delay_func cannot be used with delay, min_delay, or max_delay"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.CUSTOM
            self._delay_func = delay_func
        elif min_delay is not None or max_delay is not None:
            if delay is not None:
                msg = "Cannot use both delay and min_delay/max_delay"
                raise ValueError(msg)
            if min_delay is None or max_delay is None:
                msg = "Both min_delay and max_delay must be provided"
                raise ValueError(msg)
            if min_delay < 0 or max_delay < 0:
                msg = "min_delay and max_delay must be non-negative"
                raise ValueError(msg)
            if min_delay > max_delay:
                msg = "min_delay must be less than or equal to max_delay"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.RANDOM
            self._min_delay = min_delay
            self._max_delay = max_delay
        elif delay is not None:
            if delay < 0:
                msg = "delay must be non-negative"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.FIXED
            self._fixed_delay = delay
        else:
            msg = "Must provide one of: delay, min_delay/max_delay, or delay_func"
            raise ValueError(msg)

        self.logger = get_logger(component="DelayMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
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
            msg = "sniff_bytes must be non-negative"
            raise ValueError(msg)

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
        self,
        response: Response,
        spider: Spider,
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


class CloudflareCrawlMiddleware:
    """
    Route opt-in requests through Cloudflare Browser Rendering's crawl API.

    Set `request.meta["cloudflare_crawl"] = True` to crawl a URL with the
    middleware defaults, or assign a dict to provide per-request crawl options.
    The spider callback receives a synthetic JSON `Response` containing the
    final Cloudflare API payload.
    """

    _TRIGGER_META_KEY = "cloudflare_crawl"
    _SKIP_META_KEY = "_cloudflare_crawl_applied"
    _DONE_STATES = {"completed", "complete", "done", "finished", "success"}
    _FAILED_STATES = {"cancelled", "canceled", "error", "failed"}

    def __init__(
        self,
        account_id: str,
        api_token: str,
        *,
        crawl_options: Mapping[str, JSONValue] | None = None,
        api_base_url: str = "https://api.cloudflare.com/client/v4",
        poll_interval: float = 1.0,
        timeout: float = 300.0,
        api_timeout: float = 30.0,
    ) -> None:
        if not account_id.strip():
            msg = "account_id must not be empty"
            raise ValueError(msg)
        if not api_token.strip():
            msg = "api_token must not be empty"
            raise ValueError(msg)
        if poll_interval <= 0:
            msg = "poll_interval must be greater than 0"
            raise ValueError(msg)
        if timeout <= 0:
            msg = "timeout must be greater than 0"
            raise ValueError(msg)
        if api_timeout <= 0:
            msg = "api_timeout must be greater than 0"
            raise ValueError(msg)

        self.account_id = account_id
        self.api_token = api_token
        self.crawl_options = dict(crawl_options or {})
        self.api_base_url = api_base_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._client = HttpClient(concurrency=1, timeout=api_timeout)
        self.logger = get_logger(component="CloudflareCrawlMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        crawl_settings = self._resolve_crawl_settings(request)
        if crawl_settings is None:
            return request

        self.logger.info("Submitting Cloudflare crawl", url=request.url)
        payload = await self._run_crawl(request.url, crawl_settings)
        serialized_payload = json.dumps(payload, ensure_ascii=True)

        meta = dict(request.meta)
        meta[self._SKIP_META_KEY] = True
        meta[MOCK_RESPONSE_META_KEY] = {
            "url": request.url,
            "status": 200,
            "headers": {
                "content-type": "application/json; charset=utf-8",
                "x-silkworm-source": "cloudflare-crawl",
            },
            "body": serialized_payload,
        }

        result = payload.get("result")
        record_count: int | None = None
        if isinstance(result, dict):
            records = result.get("records")
            if isinstance(records, list):
                record_count = len(records)
        self.logger.info(
            "Cloudflare crawl completed",
            url=request.url,
            records=record_count,
        )
        return request.replace(meta=meta)

    async def close(self, spider: Spider) -> None:
        await self._client.close()

    def _resolve_crawl_settings(self, request: Request) -> dict[str, JSONValue] | None:
        if request.meta.get(self._SKIP_META_KEY):
            return None

        raw_setting = request.meta.get(self._TRIGGER_META_KEY)
        match raw_setting:
            case True:
                return dict(self.crawl_options)
            case dict() as per_request:
                settings = dict(self.crawl_options)
                settings.update(per_request)
                return settings
            case None | False:
                return None
            case _:
                msg = (
                    "request.meta['cloudflare_crawl'] must be True, False, None, "
                    "or a dict of crawl options"
                )
                raise TypeError(msg)

    async def _run_crawl(
        self,
        url: str,
        crawl_settings: Mapping[str, JSONValue],
    ) -> dict[str, JSONValue]:
        start_payload = {"url": url, **crawl_settings}
        submission = await self._api_request(
            "POST",
            f"/accounts/{self.account_id}/browser-rendering/crawl",
            json_payload=start_payload,
        )
        job_id = self._extract_job_id(submission)
        if job_id is None:
            state = self._extract_job_state(submission)
            if state in self._DONE_STATES:
                return submission

            msg = (
                "Cloudflare crawl submission did not include a recognizable job ID. "
                f"Response keys: {sorted(submission.keys())}"
            )
            raise HttpError(msg)

        deadline = time.monotonic() + self.timeout
        while True:
            status_payload = await self._api_request(
                "GET",
                f"/accounts/{self.account_id}/browser-rendering/crawl/{job_id}?limit=1",
            )
            state = self._extract_job_state(status_payload)
            if state in self._DONE_STATES:
                return await self._api_request(
                    "GET",
                    f"/accounts/{self.account_id}/browser-rendering/crawl/{job_id}",
                )
            if state in self._FAILED_STATES:
                msg = f"Cloudflare crawl job {job_id} failed with state '{state}'"
                raise HttpError(msg)
            if time.monotonic() >= deadline:
                msg = f"Cloudflare crawl job {job_id} timed out after {self.timeout}s"
                raise HttpError(msg)
            await asyncio.sleep(self.poll_interval)

    async def _api_request(
        self,
        method: str,
        path: str,
        *,
        json_payload: Mapping[str, JSONValue] | None = None,
    ) -> dict[str, JSONValue]:
        request = Request(
            url=f"{self.api_base_url}{path}",
            method=method,
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=dict(json_payload) if json_payload is not None else None,
        )
        response = await self._client.fetch(request)
        if response.status >= 400:
            msg = (
                f"Cloudflare crawl API request failed with status {response.status}: "
                f"{response.text[:200]}"
            )
            raise HttpError(msg)

        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError as exc:
            msg = "Cloudflare crawl API returned invalid JSON"
            raise HttpError(msg) from exc

        if not isinstance(payload, dict):
            msg = "Cloudflare crawl API returned a non-object response"
            raise HttpError(msg)
        if payload.get("success") is False:
            errors = payload.get("errors")
            detail = errors if isinstance(errors, list) else payload
            msg = f"Cloudflare crawl API reported an error: {detail}"
            raise HttpError(msg)
        return payload

    def _extract_job_id(self, payload: Mapping[str, JSONValue]) -> str | None:
        candidates = self._mapping_candidates(payload)
        candidates.extend(self._mapping_candidates(payload.get("job")))
        result = payload.get("result")
        if isinstance(result, str) and result:
            return result
        if isinstance(result, (int, float)):
            return str(result)
        if isinstance(result, Mapping):
            candidates.append(result)
            candidates.extend(self._mapping_candidates(result.get("job")))

        for candidate in candidates:
            for key in ("job_id", "jobId", "id"):
                value = candidate.get(key)
                if isinstance(value, str) and value:
                    return value
                if isinstance(value, (int, float)):
                    return str(value)
        return None

    def _extract_job_state(self, payload: Mapping[str, JSONValue]) -> str:
        candidates = self._mapping_candidates(payload)
        result = payload.get("result")
        if isinstance(result, Mapping):
            candidates.append(result)
            candidates.extend(self._mapping_candidates(result.get("state")))
            candidates.extend(self._mapping_candidates(result.get("job")))
        candidates.extend(self._mapping_candidates(payload.get("state")))
        candidates.extend(self._mapping_candidates(payload.get("job")))

        for candidate in candidates:
            for key in ("status", "state"):
                value = candidate.get(key)
                if isinstance(value, str) and value:
                    return value.lower()
        return "pending" if self._extract_job_id(payload) is not None else "completed"

    def _mapping_candidates(
        self,
        value: Mapping[str, JSONValue] | JSONValue | None,
    ) -> list[Mapping[str, JSONValue]]:
        if isinstance(value, Mapping):
            return [value]
        return []
