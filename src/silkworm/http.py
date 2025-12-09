from __future__ import annotations
import asyncio
import inspect
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

from rnet import Client, Impersonate, Method  # type: ignore[import]

from .exceptions import HttpError
from .request import Request
from .response import Response, HTMLResponse
from .logging import get_logger


class HttpClient:
    def __init__(
        self,
        *,
        concurrency: int = 16,
        impersonate: Impersonate = Impersonate.Firefox139,
        default_headers: dict[str, str] | None = None,
        timeout: float | None = None,
        **client_kwargs: Any,
    ) -> None:
        self._client = Client(impersonate=impersonate, **client_kwargs)
        self._sem = asyncio.Semaphore(concurrency)
        self._default_headers = default_headers or {}
        self._timeout = timeout
        self.logger = get_logger(component="http")

    async def fetch(self, req: Request) -> Response:
        headers = {**self._default_headers, **req.headers}
        proxy = req.meta.get("proxy")
        method = self._normalize_method(req.method)
        url = self._build_url(req)
        start = asyncio.get_running_loop().time()
        resp: Any | None = None

        try:
            async with self._sem:
                timeout = req.timeout if req.timeout is not None else self._timeout
                request_kwargs = dict(
                    headers=headers,
                    data=req.data,
                    json=req.json,
                    proxy=proxy,
                )
                if timeout is not None:
                    request_kwargs["timeout"] = timeout

                # Adjust keyword arguments to actual rnet.Client.request signature
                resp = await self._client.request(method, url, **request_kwargs)

                status = resp.status
                headers = self._normalize_headers(resp.headers)
                body = await self._read_body(resp)
                elapsed = (asyncio.get_running_loop().time() - start) * 1000
        except Exception as exc:
            raise HttpError(f"Request to {req.url} failed") from exc
        finally:
            await self._close_response(resp)

        self.logger.debug(
            "HTTP response",
            url=req.url,
            status=status,
            elapsed_ms=round(elapsed, 2),
            proxy=bool(proxy),
        )
        content_type = headers.get("content-type", "")
        if "html" in content_type:
            return HTMLResponse(
                url=req.url,
                status=status,
                headers=headers,
                body=body,
                request=req,
            )

        return Response(
            url=req.url,
            status=status,
            headers=headers,
            body=body,
            request=req,
        )

    async def _read_body(self, resp: Any) -> bytes:
        """
        rnet responses may expose the payload differently; try common attributes.
        """
        # preferred path: read() coroutine or function
        if hasattr(resp, "read"):
            reader = resp.read
            if callable(reader):
                result = reader()
                if inspect.isawaitable(result):
                    result = await result
                return self._ensure_bytes(result)

        # fallbacks: content/body attributes or callables
        for attr in ("content", "body"):
            if hasattr(resp, attr):
                value = getattr(resp, attr)
                if callable(value):
                    value = value()
                if inspect.isawaitable(value):
                    value = await value
                return self._ensure_bytes(value)

        # last resort: text/aread style
        for attr in ("text",):
            if hasattr(resp, attr):
                value = getattr(resp, attr)
                if callable(value):
                    value = value()
                if inspect.isawaitable(value):
                    value = await value
                return self._ensure_bytes(value)

        raise TypeError("Unable to read response body")

    async def _close_response(self, resp: Any | None) -> None:
        """Release the underlying HTTP response if it exposes a close hook."""
        if resp is None:
            return

        closer = getattr(resp, "aclose", None) or getattr(resp, "close", None)
        if closer and callable(closer):
            try:
                result = closer()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                # Best-effort cleanup; avoid surfacing close errors.
                self.logger.debug("Failed to close response", exc_info=True)

    def _ensure_bytes(self, data: Any) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8", errors="replace")
        if data is None:
            return b""
        return bytes(data)

    def _normalize_headers(self, raw_headers: Any) -> dict[str, str]:
        """
        rnet's Response.headers may be a mapping or a list of raw header lines;
        coerce both shapes into a plain dict without raising.
        """

        def _as_str(val: Any) -> str:
            if isinstance(val, bytes):
                return val.decode("utf-8", errors="ignore")
            return str(val)

        if raw_headers is None:
            return {}

        # Best effort if it already looks like a mapping
        if isinstance(raw_headers, dict):
            return {_as_str(k).lower(): _as_str(v) for k, v in raw_headers.items()}
        if hasattr(raw_headers, "items"):
            try:
                return {_as_str(k).lower(): _as_str(v) for k, v in raw_headers.items()}
            except Exception:
                pass

        headers: dict[str, str] = {}
        if isinstance(raw_headers, (list, tuple)):
            for entry in raw_headers:
                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                    k, v = entry
                elif isinstance(entry, (bytes, str)):
                    text = (
                        entry.decode("utf-8", errors="ignore")
                        if isinstance(entry, bytes)
                        else entry
                    )
                    if ":" not in text:
                        continue
                    k, v = text.split(":", 1)
                else:
                    continue
                k = _as_str(k).strip().lower()
                v = _as_str(v).strip()
                headers[k] = v
            if headers:
                return headers

        try:
            return {
                _as_str(k).lower(): _as_str(v) for k, v in dict(raw_headers).items()
            }
        except Exception:
            return {}

    def _build_url(self, req: Request) -> str:
        if not req.params:
            return req.url

        parts = urlsplit(req.url)
        existing = dict(parse_qsl(parts.query, keep_blank_values=True))
        existing.update(req.params)
        query = urlencode(existing, doseq=True)
        return parts._replace(query=query).geturl()

    def _normalize_method(self, method: str | Method) -> Method:
        if isinstance(method, Method):
            return method

        upper = method.upper()
        if hasattr(Method, upper):
            return getattr(Method, upper)

        raise ValueError(f"Unsupported HTTP method: {method!r}")

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
