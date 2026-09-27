"""Concurrency-limited ``wreq`` client adapted to Silkworm responses."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, Callable, Iterable, Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, Self, cast, runtime_checkable
from urllib.parse import urljoin

from wreq import Client, Emulation, Method, Proxy

from ._timeouts import to_seconds
from ._urls import merge_query_params
from ._validation import require_positive_int
from .exceptions import (
    HttpConnectionError,
    HttpError,
    HttpTimeoutError,
    ResponseTooLargeError,
)
from .logging import Logger, get_logger
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from wreq import Profile

    from ._types import Headers
    from .request import Request


MOCK_RESPONSE_META_KEY = "_silkworm_mock_response"

# Browser profile impersonated by default; pass ``emulation=None`` to disable.
DEFAULT_EMULATION = Emulation.Firefox139

# Default per-request timeout in seconds. A finite default keeps a server that
# stops responding without closing the connection from holding a worker forever.
DEFAULT_REQUEST_TIMEOUT = 60.0

# Largest response body downloaded by default; see ``max_response_size_bytes``.
DEFAULT_MAX_RESPONSE_SIZE_BYTES = 50_000_000

# Request ``meta`` key overriding the client's body size limit for one request
# (a positive number of bytes, or ``None`` for no limit).
MAX_RESPONSE_SIZE_META_KEY = "max_response_size"

try:
    from wreq import exceptions as _wreq_exceptions
except ImportError:  # pragma: no cover - wreq without an exceptions module
    _wreq_exceptions = None


def _wreq_error_types(*names: str) -> tuple[type[BaseException], ...]:
    """Return the named ``wreq.exceptions`` classes that exist in this version."""
    found: list[type[BaseException]] = []
    for name in names:
        candidate = getattr(_wreq_exceptions, name, None)
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            found.append(candidate)
    return tuple(found)


# ``wreq`` raises native exception classes (their ``__module__`` is not
# ``wreq``), so match the classes themselves rather than names. ``RequestError``
# means sending the request failed (connect errors, connections closed before a
# response); building, TLS, redirect, and decoding failures have their own
# classes and stay plain ``HttpError``.
_WREQ_TIMEOUT_ERRORS = _wreq_error_types("TimeoutError")
_WREQ_CONNECTION_ERRORS = _wreq_error_types(
    "ConnectionError",
    "ProxyConnectionError",
    "ConnectionResetError",
    "BodyError",
    "RequestError",
)


def _classify_transport_error(exc: Exception) -> type[HttpError]:
    """Return the ``HttpError`` subclass describing a transport exception."""
    if isinstance(exc, (TimeoutError, *_WREQ_TIMEOUT_ERRORS)):
        return HttpTimeoutError
    if isinstance(exc, (OSError, *_WREQ_CONNECTION_ERRORS)):
        return HttpConnectionError
    return HttpError


def normalize_status(raw_status: object) -> int:
    """Coerce a status code (int, enum, or ``wreq`` ``StatusCode``) to an ``int``.

    Raises:
        TypeError: If ``raw_status`` has no integer representation.
    """
    if isinstance(raw_status, int):
        return raw_status

    for attr in ("value", "code"):
        candidate = getattr(raw_status, attr, None)
        if isinstance(candidate, int):
            return candidate

    for converter_name in ("as_int", "as_integer", "as_u16"):
        converter = getattr(raw_status, converter_name, None)
        if callable(converter):
            try:
                candidate = converter()
            except (TypeError, ValueError, OverflowError):
                continue
            if isinstance(candidate, int):
                return candidate

    try:
        return int(cast("Any", raw_status))
    except (TypeError, ValueError):
        pass
    # e.g. "404 Not Found"
    text = str(raw_status).strip()
    head = text.split(maxsplit=1)[0] if text else ""
    if head.isdigit():
        return int(head)
    raise TypeError(f"Invalid status code type: {type(raw_status).__name__}")


def looks_like_html(headers: Mapping[str, str], body: bytes) -> bool:
    """Return whether a response should be parsed as HTML.

    ``headers`` must use lowercase names, as produced by :class:`HttpClient`.
    """
    content_type = headers.get("content-type", "").lower()
    snippet = body[:2048]
    snippet_lower = snippet.lower()
    return (
        "html" in content_type
        or b"<html" in snippet_lower
        or b"<!doctype" in snippet_lower
        or (content_type.startswith("text/") and b"\x00" not in snippet)
    )


def build_response(
    *,
    url: str,
    status: int,
    headers: dict[str, str],
    body: bytes,
    request: Request,
    html_max_size_bytes: int,
) -> Response:
    """Return an :class:`HTMLResponse` for HTML payloads, else a :class:`Response`."""
    if looks_like_html(headers, body):
        return HTMLResponse(
            url=url,
            status=status,
            headers=headers,
            body=body,
            request=request,
            doc_max_size_bytes=html_max_size_bytes,
        )
    return Response(url=url, status=status, headers=headers, body=body, request=request)


class FetchClient(Protocol):
    """Interface the engine needs from an HTTP client.

    :class:`HttpClient`, :class:`~silkworm.CDPClient`,
    :class:`~silkworm.ServoFetchClient`, :class:`~silkworm.OnionLinkClient`, and
    :class:`~silkworm.httpcache.CachingHttpClient` implement it; pass any
    conforming object as ``http_client``.
    """

    @property
    def concurrency(self) -> int:
        """Return the maximum number of requests in flight."""
        ...

    @property
    def html_max_size_bytes(self) -> int:
        """Return the HTML document parsing limit in bytes."""
        ...

    async def fetch(self, req: Request) -> Response:
        """Send ``req`` and return its response."""
        ...

    async def close(self) -> None:
        """Release transport resources."""
        ...


@runtime_checkable
class _HeaderEntry(Protocol):
    """A header object exposing ``name`` and ``value`` attributes."""

    name: object
    value: object


class HttpClient:
    """Send Silkworm requests through a browser-impersonating ``wreq`` client.

    Args:
        concurrency: Maximum requests in flight.
        emulation: Browser profile to impersonate, or ``None`` to disable it.
        default_headers: Headers merged below per-request headers.
        timeout: Default request timeout in seconds or as a ``timedelta``
            (:data:`DEFAULT_REQUEST_TIMEOUT`, 60 seconds, unless given);
            ``None`` disables it. ``Request.timeout`` overrides it per request.
            The budget covers sending the request and downloading the whole
            body, restarts for each redirect hop, and excludes time spent
            waiting for a concurrency slot.
        html_max_size_bytes: Maximum document size parsed by HTML selectors.
        follow_redirects: Follow redirect responses internally.
        max_redirects: Maximum redirect hops.
        keep_alive: Request connection reuse when the installed ``wreq``
            version supports it.
        max_response_size_bytes: Largest body downloaded, or ``None`` for no
            limit. A larger ``Content-Length`` fails before the body is read,
            and streamed bodies stop as soon as they exceed the limit. Override
            it per request with ``request.meta["max_response_size"]``.
        **client_kwargs: Additional options forwarded to ``wreq.Client``.

    Requests are converted to :class:`~silkworm.HTMLResponse` when headers or a
    small body sniff indicate HTML; all other payloads become
    :class:`~silkworm.Response`.
    """

    def __init__(
        self,
        *,
        concurrency: int = 16,
        emulation: Emulation | Profile | None = DEFAULT_EMULATION,
        default_headers: Headers | None = None,
        timeout: float | timedelta | None = DEFAULT_REQUEST_TIMEOUT,
        html_max_size_bytes: int = 5_000_000,
        follow_redirects: bool = True,
        max_redirects: int = 10,
        keep_alive: bool = False,
        max_response_size_bytes: int | None = DEFAULT_MAX_RESPONSE_SIZE_BYTES,
        **client_kwargs: object,
    ) -> None:
        require_positive_int(concurrency, "concurrency")
        if max_response_size_bytes is not None:
            require_positive_int(max_response_size_bytes, "max_response_size_bytes")
        if max_redirects < 0:
            msg = "max_redirects must be non-negative"
            raise ValueError(msg)
        client_options: dict[str, object] = {"emulation": emulation}
        if keep_alive and self._supports_kwarg(Client, "keep_alive"):
            client_options["keep_alive"] = True
        client_options.update(client_kwargs)

        client_factory = cast(Any, Client)
        self._client: Any = client_factory(**client_options)
        self._concurrency = concurrency
        self._sem = asyncio.Semaphore(concurrency)
        self._default_headers = default_headers or {}
        self._timeout = timeout
        self._html_max_size_bytes = html_max_size_bytes
        self._follow_redirects = follow_redirects
        self._max_redirects = max_redirects
        self._keep_alive = keep_alive
        self._max_response_size_bytes = max_response_size_bytes
        self._supports_keep_alive_kwarg = self._supports_kwarg(
            getattr(self._client, "request", None),
            "keep_alive",
        )
        self._closed = False
        self.logger: Logger = get_logger(component="http")

    @property
    def concurrency(self) -> int:
        """Return the maximum number of requests allowed in flight."""
        return self._concurrency

    @property
    def html_max_size_bytes(self) -> int:
        """Return the HTML document parsing limit in bytes."""
        return self._html_max_size_bytes

    @property
    def max_response_size_bytes(self) -> int | None:
        """Return the default response body size limit in bytes."""
        return self._max_response_size_bytes

    async def __aenter__(self) -> Self:
        """Return this initialized client for use in an async context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        """Close the underlying transport on context exit."""
        try:
            await self.close()
        except BaseException as cleanup_exc:
            if exc is None:
                raise
            exc.add_note(f"HTTP client cleanup failed: {cleanup_exc}")

    async def fetch(self, req: Request) -> Response:
        """Send one request, follow redirects, and return a normalized response.

        The per-request timeout and proxy metadata override client defaults.
        Synthetic response metadata is honored for middleware integrations.

        Raises:
            HttpTimeoutError: If the request times out.
            HttpConnectionError: If the connection fails or is reset.
            ResponseTooLargeError: If the body exceeds the size limit.
            HttpError: If redirects loop or exceed the configured limit, or the
                request fails for another reason.
        """
        if self._closed:
            raise HttpError("HTTP client is closed")

        mocked_response = self._build_mock_response(req)
        if mocked_response is not None:
            self.logger.debug(
                "Using synthetic response",
                url=mocked_response.url,
                status=mocked_response.status,
            )
            return mocked_response

        proxy = self._normalize_proxy(req.meta.get("proxy"))
        size_limit = self._size_limit(req)
        current_req = req
        redirects_followed = 0
        visited_urls: set[str] = set()
        total_start = asyncio.get_running_loop().time()

        # Response data captured from the final request in any redirect chain
        body: bytes = b""
        status: int = 0
        headers: dict[str, str] = {}
        elapsed: float = 0.0

        while True:
            resp: Any = None
            method = self._normalize_method(current_req.method)
            url = self._build_url(current_req)
            visited_urls.add(url)
            timeout_raw: float | timedelta | None = None
            timeout_seconds: float | None = None

            try:
                async with self._sem:
                    timeout_raw = (
                        current_req.timeout
                        if current_req.timeout is not None
                        else self._timeout
                    )
                    timeout_seconds = to_seconds(timeout_raw)
                    headers = {**self._default_headers, **current_req.headers}
                    if self._keep_alive and not self._has_connection_header(headers):
                        headers["Connection"] = "keep-alive"
                    request_kwargs: dict[str, object] = {
                        "headers": headers,
                        "data": current_req.data,
                        "json": current_req.json,
                        "proxy": proxy,
                    }
                    client_timeout = self._as_timedelta(timeout_raw)
                    if client_timeout is not None:
                        request_kwargs["timeout"] = client_timeout
                    if self._keep_alive and self._supports_keep_alive_kwarg:
                        request_kwargs["keep_alive"] = True

                    async with asyncio.timeout(timeout_seconds):
                        # Adjust keyword arguments to the actual wreq Client.request signature
                        resp = await self._send_request(method, url, request_kwargs)

                        status = self._normalize_status(resp.status)
                        headers = self._normalize_headers(resp.headers)

                        if self._should_follow_redirect(status, headers):
                            if redirects_followed >= self._max_redirects:
                                raise HttpError(
                                    f"Exceeded maximum redirects ({self._max_redirects})",
                                )

                            redirect_url = self._resolve_redirect_url(
                                url,
                                headers.get("location", ""),
                            )
                            if redirect_url in visited_urls:
                                raise HttpError("Redirect loop detected")

                            redirects_followed += 1
                            self.logger.debug(
                                "Following redirect",
                                from_url=url,
                                to_url=redirect_url,
                                status=status,
                            )
                            current_req = self._redirect_request(
                                current_req,
                                redirect_url,
                                status,
                                method,
                            )
                            await self._close_response(resp)
                            resp = None
                            continue

                        body = await self._read_body(resp, headers, size_limit, req.url)
                        elapsed = (
                            asyncio.get_running_loop().time() - total_start
                        ) * 1000
                break
            except TimeoutError as exc:
                suffix = (
                    f" after {timeout_seconds} seconds"
                    if timeout_seconds is not None
                    else ""
                )
                raise HttpTimeoutError(
                    f"Request to {req.url} timed out{suffix}"
                ) from exc
            except HttpError:
                raise
            except Exception as exc:
                detail = str(exc)
                suffix = f": {detail}" if detail else ""
                error_type = _classify_transport_error(exc)
                if error_type is HttpTimeoutError:
                    raise error_type(f"Request to {req.url} timed out{suffix}") from exc
                raise error_type(f"Request to {req.url} failed{suffix}") from exc
            finally:
                await self._close_response(resp)

        self.logger.debug(
            "HTTP response",
            url=url,
            status=status,
            elapsed_ms=round(elapsed, 2),
            proxy=bool(proxy),
            redirects=redirects_followed,
        )
        return build_response(
            url=url,
            status=status,
            headers=headers,
            body=body,
            request=current_req,
            html_max_size_bytes=self._html_max_size_bytes,
        )

    def _size_limit(self, req: Request) -> int | None:
        if MAX_RESPONSE_SIZE_META_KEY not in req.meta:
            return self._max_response_size_bytes
        override = req.meta[MAX_RESPONSE_SIZE_META_KEY]
        if override is None:
            return None
        if isinstance(override, bool) or not isinstance(override, int) or override <= 0:
            msg = (
                f"request.meta[{MAX_RESPONSE_SIZE_META_KEY!r}] must be a positive "
                f"integer or None, got {override!r}"
            )
            raise ValueError(msg)
        return override

    def _build_mock_response(self, req: Request) -> Response | None:
        raw_response = req.meta.get(MOCK_RESPONSE_META_KEY)
        if not isinstance(raw_response, Mapping):
            return None

        url_raw = raw_response.get("url", req.url)
        headers = self._normalize_headers(raw_response.get("headers"))
        body = self._ensure_bytes(raw_response.get("body"))
        status = self._normalize_status(raw_response.get("status", 200))
        url = url_raw if isinstance(url_raw, str) else req.url

        content_type = headers.get("content-type", "").lower()
        snippet = body[:2048].lower()
        looks_html = (
            "html" in content_type or b"<html" in snippet or b"<!doctype" in snippet
        )
        if looks_html:
            return HTMLResponse(
                url=url,
                status=status,
                headers=headers,
                body=body,
                request=req,
                doc_max_size_bytes=self._html_max_size_bytes,
            )

        return Response(
            url=url,
            status=status,
            headers=headers,
            body=body,
            request=req,
        )

    async def _maybe_await(self, value: object) -> object:
        return await value if inspect.isawaitable(value) else value

    def _as_timedelta(self, timeout: float | timedelta | None) -> timedelta | None:
        if timeout is None:
            return None
        if isinstance(timeout, timedelta):
            return timeout
        return timedelta(seconds=float(timeout))

    async def _read_body(
        self,
        resp: object,
        headers: Mapping[str, str],
        limit: int | None,
        url: str,
    ) -> bytes:
        """Read the raw response body, enforcing ``limit`` bytes.

        ``wreq`` responses are streamed so an oversized body is abandoned as
        soon as it crosses the limit; other client objects fall back to
        ``bytes()``/``read()`` and are checked after reading.
        """
        if limit is not None:
            declared = headers.get("content-length", "").strip()
            if declared.isdigit() and int(declared) > limit:
                raise ResponseTooLargeError(
                    f"Response from {url} declares {declared} bytes, "
                    f"exceeding the {limit}-byte limit"
                )

        # Look methods up on the type: ``unittest.mock`` objects fabricate any
        # attribute on the instance, but a real ``stream`` lives on the class.
        if callable(getattr(type(resp), "stream", None)):
            stream = cast("Any", resp).stream()
            if isinstance(stream, AsyncIterable):
                chunks: list[bytes] = []
                size = 0
                async for chunk in cast("AsyncIterable[object]", stream):
                    data = self._ensure_bytes(chunk)
                    size += len(data)
                    if limit is not None and size > limit:
                        raise ResponseTooLargeError(
                            f"Response from {url} exceeded the {limit}-byte limit"
                        )
                    chunks.append(data)
                return b"".join(chunks)

        body = await self._read_whole_body(resp)
        if limit is not None and len(body) > limit:
            raise ResponseTooLargeError(
                f"Response from {url} is {len(body)} bytes, "
                f"exceeding the {limit}-byte limit"
            )
        return body

    async def _read_whole_body(self, resp: object) -> bytes:
        """Read a body from client objects that do not support streaming."""
        raw_bytes = getattr(type(resp), "bytes", None)
        if callable(raw_bytes):
            return self._ensure_bytes(await self._maybe_await(raw_bytes(resp)))

        reader = getattr(resp, "read", None)
        if callable(reader):
            return self._ensure_bytes(await self._maybe_await(reader()))

        for attr in ("content", "body", "text"):
            candidate = getattr(resp, attr, None)
            if candidate is None:
                continue
            if callable(candidate):
                candidate = candidate()
            candidate = await self._maybe_await(candidate)
            try:
                return self._ensure_bytes(candidate)
            except (TypeError, ValueError):
                continue

        msg = "Unable to read response body"
        raise TypeError(msg)

    async def _close_response(self, resp: object | None) -> None:
        """Release the underlying HTTP response if it exposes a close hook."""
        if resp is None:
            return

        closer = getattr(resp, "aclose", None) or getattr(resp, "close", None)
        if closer and callable(closer):
            try:
                await self._maybe_await(closer())
            except Exception:
                # Best-effort cleanup; avoid surfacing close errors.
                self.logger.debug("Failed to close response", exc_info=True)

    def _ensure_bytes(self, data: object) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8", errors="replace")
        if isinstance(data, (bytearray, memoryview)):
            return bytes(cast("bytearray | memoryview[int]", data))
        if data is None:
            return b""
        try:
            return bytes(data)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            return str(data).encode("utf-8", errors="replace")

    def _normalize_proxy(self, proxy: object) -> Proxy | None:
        if proxy is None:
            return None
        if isinstance(proxy, Proxy):
            return proxy
        if isinstance(proxy, str):
            return Proxy.all(proxy)

        msg = f"Unsupported proxy type: {type(proxy).__name__}"
        raise TypeError(msg)

    def _has_connection_header(self, headers: Mapping[str, object]) -> bool:
        return any(str(k).lower() == "connection" for k in headers)

    def _supports_kwarg(self, func: Callable[..., object] | None, name: str) -> bool:
        if func is None:
            return False

        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError):
            return False

        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                return True
            if param.name == name:
                return True
        return False

    async def _send_request(
        self,
        method: Method | str,
        url: str,
        kwargs: dict[str, object],
    ) -> object:
        try:
            return await self._client.request(method, url, **kwargs)
        except TypeError as exc:
            if self._keep_alive and kwargs.pop("keep_alive", None) is not None:
                self._supports_keep_alive_kwarg = False
                self.logger.debug(
                    "HTTP client rejected keep_alive argument; retrying without",
                    error=str(exc),
                )
                return await self._client.request(method, url, **kwargs)
            raise

    @staticmethod
    def _textify(value: object) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        return str(value)

    def _normalize_headers(self, raw_headers: object) -> dict[str, str]:
        """
        wreq's Response.headers may be a mapping or a list of raw header lines;
        coerce both shapes into a plain dict without raising.
        """

        if raw_headers is None:
            return {}

        if isinstance(raw_headers, Mapping):
            mapping = cast("Mapping[object, object]", raw_headers)
            return {
                self._textify(k).strip().lower(): self._textify(v).strip()
                for k, v in mapping.items()
            }

        header_map = self._normalize_header_map(raw_headers)
        if header_map:
            return header_map

        headers: Headers = {}
        if isinstance(raw_headers, Sequence) and not isinstance(
            raw_headers,
            (str, bytes, bytearray),
        ):
            for entry in cast("Sequence[object]", raw_headers):
                k: object
                v: object
                pair = cast("Sequence[object]", entry)
                if isinstance(entry, Sequence) and len(pair) == 2:
                    k, v = pair
                elif isinstance(entry, (bytes, str)):
                    text = self._textify(entry)
                    if ":" not in text:
                        continue
                    k, v = text.split(":", 1)
                elif isinstance(entry, _HeaderEntry):
                    k = entry.name
                    v = entry.value
                else:
                    continue
                headers[self._textify(k).strip().lower()] = self._textify(v).strip()
            if headers:
                return headers

        try:
            mapping = cast(Mapping[object, object], raw_headers)
            return {
                self._textify(k).strip().lower(): self._textify(v).strip()
                for k, v in mapping.items()
            }
        except (AttributeError, TypeError, ValueError):
            return {}

    def _normalize_header_map(self, raw_headers: object) -> dict[str, str]:
        keys = getattr(raw_headers, "keys", None)
        getter = getattr(raw_headers, "get_all", None) or getattr(
            raw_headers, "get", None
        )
        if not callable(keys) or not callable(getter):
            return {}

        headers: Headers = {}
        try:
            raw_keys = cast("Iterable[object]", keys())
        except (TypeError, ValueError):
            return {}

        for key in raw_keys:
            name = self._textify(key).strip().lower()
            if not name:
                continue

            try:
                raw_values = getter(key)
            except (KeyError, TypeError, ValueError):
                try:
                    raw_values = getter(name)
                except (KeyError, TypeError, ValueError):
                    continue

            if isinstance(raw_values, Sequence) and not isinstance(
                raw_values,
                (str, bytes, bytearray),
            ):
                value = ", ".join(
                    self._textify(raw_value).strip()
                    for raw_value in cast("Sequence[object]", raw_values)
                )
            else:
                value = self._textify(raw_values).strip()
            headers[name] = value

        return headers

    def _normalize_status(self, raw_status: Any) -> int:
        """Coerce a transport status object into a plain integer."""
        return normalize_status(raw_status)

    def _build_url(self, req: Request) -> str:
        return merge_query_params(req.url, req.params)

    def _normalize_method(self, method: str | Method) -> Method | str:
        if isinstance(method, Method):
            return method

        upper = method.upper()
        member = getattr(Method, upper, None)
        if member is not None:
            return member

        try:
            return Method[upper]
        except (KeyError, TypeError):
            # Fallback to the uppercased string for test doubles or alternative
            # Method implementations that are not subscriptable.
            return upper

    def _method_name(self, method: Method | str) -> str:
        return getattr(method, "name", str(method))

    def _should_follow_redirect(self, status: int, headers: dict[str, str]) -> bool:
        if not self._follow_redirects:
            return False

        return status in {301, 302, 303, 307, 308} and "location" in headers

    def _resolve_redirect_url(self, current_url: str, location: str) -> str:
        return urljoin(current_url, location.strip())

    def _redirect_request(
        self,
        req: Request,
        redirect_url: str,
        status: int,
        method: Method | str,
    ) -> Request:
        method_name = self._method_name(method).upper()
        new_method = method_name
        new_data = req.data
        new_json = req.json

        if status in {301, 302, 303} and method_name not in {"GET", "HEAD"}:
            new_method = "GET"
            new_data = None
            new_json = None

        updated = req.replace(
            url=redirect_url,
            method=new_method,
            data=new_data,
            json=new_json,
            meta={**req.meta},
            params={},  # don't re-append original query params to redirect targets
        )

        raw_redirects = updated.meta.get("redirect_times", 0)
        redirects = raw_redirects if isinstance(raw_redirects, int) else 0
        updated.meta["redirect_times"] = redirects + 1
        return updated

    async def close(self) -> None:
        """Close the underlying transport."""
        if self._closed:
            return
        self._closed = True
        closer = getattr(self._client, "aclose", None) or getattr(
            self._client,
            "close",
            None,
        )
        if closer is None or not callable(closer):
            return

        try:
            result = closer()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            self.logger.debug(
                "Failed to close HTTP client cleanly", error=str(exc), exc_info=True
            )
            raise
