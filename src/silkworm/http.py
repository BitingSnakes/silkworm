from __future__ import annotations
import asyncio
import inspect
from contextlib import asynccontextmanager
from collections.abc import Callable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit
from typing import Any, AsyncIterator, cast

from rnet import Client, Emulation, Method  # type: ignore[import]

from .exceptions import HttpError
from ._types import Headers, QueryValue
from .request import Request
from .response import Response, HTMLResponse
from .logging import get_logger


class HttpClient:
    def __init__(
        self,
        *,
        concurrency: int = 16,
        emulation: Emulation = Emulation.Firefox139,
        default_headers: Headers | None = None,
        timeout: float | None = None,
        html_max_size_bytes: int = 5_000_000,
        follow_redirects: bool = True,
        max_redirects: int = 10,
        keep_alive: bool = False,
        **client_kwargs: object,
    ) -> None:
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
        if max_redirects < 0:
            raise ValueError("max_redirects must be non-negative")
        self._max_redirects = max_redirects
        self._keep_alive = keep_alive
        self._supports_keep_alive_kwarg = self._supports_kwarg(
            getattr(self._client, "request", None), "keep_alive"
        )
        self.logger = get_logger(component="http")

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def html_max_size_bytes(self) -> int:
        return self._html_max_size_bytes

    async def fetch(self, req: Request) -> Response:
        proxy = req.meta.get("proxy")
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
            timeout: float | None = None

            try:
                async with self._sem:
                    timeout = (
                        current_req.timeout
                        if current_req.timeout is not None
                        else self._timeout
                    )
                    headers = {**self._default_headers, **current_req.headers}
                    if self._keep_alive and not self._has_connection_header(headers):
                        headers["Connection"] = "keep-alive"
                    request_kwargs: dict[str, object] = dict(
                        headers=headers,
                        data=current_req.data,
                        json=current_req.json,
                        proxy=proxy,
                    )
                    if timeout is not None:
                        request_kwargs["timeout"] = timeout
                    if self._keep_alive and self._supports_keep_alive_kwarg:
                        request_kwargs["keep_alive"] = True

                    async with self._request_timeout(timeout):
                        # Adjust keyword arguments to actual rnet.Client.request signature
                        resp = await self._send_request(method, url, request_kwargs)

                        status = self._normalize_status(resp.status)
                        headers = self._normalize_headers(resp.headers)

                        if self._should_follow_redirect(status, headers):
                            if redirects_followed >= self._max_redirects:
                                raise HttpError(
                                    f"Exceeded maximum redirects ({self._max_redirects})"
                                )

                            redirect_url = self._resolve_redirect_url(
                                url, headers.get("location", "")
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
                                current_req, redirect_url, status, method
                            )
                            await self._close_response(resp)
                            resp = None
                            continue

                        body = await self._read_body(resp)
                        elapsed = (
                            asyncio.get_running_loop().time() - total_start
                        ) * 1000
                break
            except TimeoutError as exc:
                suffix = f" after {timeout} seconds" if timeout is not None else ""
                raise HttpError(f"Request to {req.url} timed out{suffix}") from exc
            except HttpError:
                raise
            except Exception as exc:
                detail = str(exc)
                suffix = f": {detail}" if detail else ""
                raise HttpError(f"Request to {req.url} failed{suffix}") from exc
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
        content_type = headers.get("content-type", "").lower()
        snippet = body[:2048]
        snippet_lower = snippet.lower()
        looks_textual = b"\x00" not in snippet
        is_html = (
            "html" in content_type
            or b"<html" in snippet_lower
            or b"<!doctype" in snippet_lower
            or (content_type.startswith("text/") and looks_textual)
        )
        if is_html:
            return HTMLResponse(
                url=url,
                status=status,
                headers=headers,
                body=body,
                request=current_req,
                doc_max_size_bytes=self._html_max_size_bytes,
            )

        return Response(
            url=url,
            status=status,
            headers=headers,
            body=body,
            request=current_req,
        )

    async def _maybe_await(self, value: object) -> object:
        return await value if inspect.isawaitable(value) else value

    @asynccontextmanager
    async def _request_timeout(self, timeout: float | None) -> AsyncIterator[None]:
        if timeout is None:
            yield
            return
        async with asyncio.timeout(timeout):
            yield

    async def _read_body(self, resp: object) -> bytes:
        """
        rnet responses may expose the payload differently; try common attributes.
        """
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

        raise TypeError("Unable to read response body")

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
            return bytes(data)
        if data is None:
            return b""
        try:
            return bytes(data)  # type: ignore[call-overload]
        except Exception:
            return str(data).encode("utf-8", errors="replace")

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
        self, method: Method | str, url: str, kwargs: dict[str, object]
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
        rnet's Response.headers may be a mapping or a list of raw header lines;
        coerce both shapes into a plain dict without raising.
        """

        if raw_headers is None:
            return {}

        if isinstance(raw_headers, Mapping):
            return {
                self._textify(k).strip().lower(): self._textify(v).strip()
                for k, v in raw_headers.items()
            }

        headers: Headers = {}
        if isinstance(raw_headers, Sequence) and not isinstance(
            raw_headers, (str, bytes, bytearray)
        ):
            for entry in raw_headers:
                if isinstance(entry, Sequence) and len(entry) == 2:
                    k, v = entry
                elif isinstance(entry, (bytes, str)):
                    text = self._textify(entry)
                    if ":" not in text:
                        continue
                    k, v = text.split(":", 1)
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
        except Exception:
            return {}

    def _normalize_status(self, raw_status: Any) -> int:
        """
        Coerce various status code representations (ints, enums, rnet StatusCode)
        into a plain integer for consistent comparison and hashing.
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
                except Exception:
                    continue
                if isinstance(candidate, int):
                    return candidate

        try:
            return int(raw_status)
        except Exception as exc:
            raise TypeError(
                f"Invalid status code type: {type(raw_status).__name__}"
            ) from exc

    def _build_url(self, req: Request) -> str:
        if not req.params:
            return req.url

        parts = urlsplit(req.url)
        existing: dict[str, QueryValue] = dict(
            parse_qsl(parts.query, keep_blank_values=True)
        )
        existing.update(req.params)
        query = urlencode(cast(Mapping[str, object], existing), doseq=True)
        return parts._replace(query=query).geturl()

    def _normalize_method(self, method: str | Method) -> Method | str:
        if isinstance(method, Method):
            return method

        upper = method.upper()
        member = getattr(Method, upper, None)
        if member is not None:
            return member

        try:
            return Method[upper]  # type: ignore[index]
        except Exception:
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
        self, req: Request, redirect_url: str, status: int, method: Method | str
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
            params={},  # don't re-append original query params to redirect targets
        )

        raw_redirects = updated.meta.get("redirect_times", 0)
        redirects = raw_redirects if isinstance(raw_redirects, int) else 0
        updated.meta["redirect_times"] = redirects + 1
        return updated

    async def close(self) -> None:
        closer = getattr(self._client, "aclose", None) or getattr(
            self._client, "close", None
        )
        if closer is None or not callable(closer):
            return

        try:
            result = closer()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            # Best-effort cleanup; suppress shutdown errors so the engine can exit.
            self.logger.debug("Failed to close HTTP client cleanly", error=str(exc))
