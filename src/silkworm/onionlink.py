from __future__ import annotations

import asyncio
import importlib
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ._validation import require_positive_int
from .exceptions import HttpError
from .http import HttpClient
from .logging import get_logger
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from ._types import BodyData, Headers, QueryValue
    from .request import Request


ONIONLINK_RESPONSE_LIMIT_META_KEY = "onionlink_response_limit"


class OnionLinkClient(HttpClient):
    """
    HTTP client adapter that fetches Tor v3 onion services through onionlink.

    onionlink exposes an AsyncSession API whose request methods use native
    awaitables when available and fall back to an executor internally.
    """

    def __init__(
        self,
        *,
        concurrency: int = 16,
        default_headers: Headers | None = None,
        timeout: float | timedelta | None = None,
        html_max_size_bytes: int = 5_000_000,
        follow_redirects: bool = True,
        max_redirects: int = 10,
        bootstrap: str = "128.31.0.39:9131",
        consensus_file: str = "",
        verbose: bool = False,
        response_limit: int = 4 * 1024 * 1024,
    ) -> None:
        require_positive_int(concurrency, "concurrency")
        try:
            module = importlib.import_module("onionlink")
        except ImportError as err:
            msg = (
                "onionlink is not installed. Install it with: "
                'pip install "silkworm-rs[onionlink]"'
            )
            raise ImportError(msg) from err

        try:
            session_cls = getattr(module, "AsyncSession")
        except AttributeError as err:
            msg = (
                "onionlink>=0.1.2 is required for OnionLinkClient. Install it with: "
                'pip install "silkworm-rs[onionlink]"'
            )
            raise ImportError(msg) from err

        timeout_ms = self._timeout_ms(timeout)
        self._client: Any = session_cls(
            bootstrap=bootstrap,
            consensus_file=consensus_file,
            timeout_ms=timeout_ms,
            verbose=verbose,
        )
        self._concurrency = concurrency
        self._sem = asyncio.Semaphore(concurrency)
        self._default_headers = default_headers or {}
        self._timeout = timeout
        self._html_max_size_bytes = html_max_size_bytes
        self._follow_redirects = follow_redirects
        if max_redirects < 0:
            msg = "max_redirects must be non-negative"
            raise ValueError(msg)
        self._max_redirects = max_redirects
        self._response_limit = response_limit
        self.logger = get_logger(component="onionlink")

    async def fetch(self, req: Request) -> Response:
        mocked_response = self._build_mock_response(req)
        if mocked_response is not None:
            self.logger.debug(
                "Using synthetic response",
                url=mocked_response.url,
                status=mocked_response.status,
            )
            return mocked_response

        current_req = req
        redirects_followed = 0
        visited_urls: set[str] = set()
        total_start = asyncio.get_running_loop().time()

        body: bytes = b""
        status = 0
        headers: dict[str, str] = {}
        elapsed = 0.0
        url = req.url

        while True:
            timeout_raw = (
                current_req.timeout
                if current_req.timeout is not None
                else self._timeout
            )
            timeout_seconds = self._timeout_seconds(timeout_raw)
            onion_request = self._build_onion_request(current_req)
            url = onion_request.url
            visited_urls.add(url)

            try:
                async with self._sem:
                    async with self._request_timeout(timeout_seconds):
                        resp = await self._send_onion_request(
                            current_req,
                            onion_request,
                        )

                    status = self._normalize_status(resp.status_code)
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
                            "Following onion redirect",
                            from_url=url,
                            to_url=redirect_url,
                            status=status,
                        )
                        current_req = self._redirect_request(
                            current_req,
                            redirect_url,
                            status,
                            current_req.method,
                        )
                        continue

                    body = self._ensure_bytes(resp.body)
                    elapsed = (asyncio.get_running_loop().time() - total_start) * 1000
                break
            except TimeoutError as exc:
                suffix = (
                    f" after {timeout_seconds} seconds"
                    if timeout_seconds is not None
                    else ""
                )
                raise HttpError(f"Request to {req.url} timed out{suffix}") from exc
            except HttpError:
                raise
            except Exception as exc:
                detail = str(exc)
                suffix = f": {detail}" if detail else ""
                raise HttpError(f"Request to {req.url} failed{suffix}") from exc

        self.logger.debug(
            "OnionLink response",
            url=url,
            status=status,
            elapsed_ms=round(elapsed, 2),
            redirects=redirects_followed,
        )
        return self._response_from_parts(
            url=url,
            status=status,
            headers=headers,
            body=body,
            request=current_req,
        )

    async def _send_onion_request(
        self,
        req: Request,
        onion_request: _OnionRequest,
    ) -> Any:
        headers = {**self._default_headers, **req.headers}
        data, form = self._onion_body_kwargs(req.data)
        return await self._client.request(
            req.method.upper(),
            onion_request.onion,
            port=onion_request.port,
            path=onion_request.path,
            headers=headers,
            data=data,
            json=req.json,
            form=form,
            response_limit=self._response_limit_for(req),
        )

    def _build_onion_request(self, req: Request) -> _OnionRequest:
        parts = urlsplit(req.url)
        if parts.scheme not in {"http", "https"}:
            msg = f"OnionLinkClient requires http(s) URLs, got {req.url!r}"
            raise HttpError(msg)

        onion = (parts.hostname or "").lower()
        if not onion.endswith(".onion"):
            msg = f"OnionLinkClient only supports .onion hosts, got {req.url!r}"
            raise HttpError(msg)

        existing: dict[str, QueryValue] = dict(
            parse_qsl(parts.query, keep_blank_values=True),
        )
        existing.update(req.params)
        query = urlencode(existing, doseq=True)
        path = parts.path or "/"
        if query:
            path = f"{path}?{query}"

        port = parts.port or (443 if parts.scheme == "https" else 80)
        url = urlunsplit((parts.scheme, parts.netloc, parts.path or "/", query, ""))
        return _OnionRequest(onion=onion, port=port, path=path, url=url)

    def _onion_body_kwargs(self, data: BodyData) -> tuple[object | None, object | None]:
        if data is None:
            return None, None
        if isinstance(data, bytes | bytearray | memoryview | str):
            return data, None
        return None, data

    def _response_limit_for(self, req: Request) -> int:
        raw = req.meta.get(ONIONLINK_RESPONSE_LIMIT_META_KEY, self._response_limit)
        if not isinstance(raw, int):
            msg = f"{ONIONLINK_RESPONSE_LIMIT_META_KEY} must be an integer byte limit"
            raise TypeError(msg)
        return raw

    def _response_from_parts(
        self,
        *,
        url: str,
        status: int,
        headers: dict[str, str],
        body: bytes,
        request: Request,
    ) -> Response:
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
                request=request,
                doc_max_size_bytes=self._html_max_size_bytes,
            )

        return Response(
            url=url,
            status=status,
            headers=headers,
            body=body,
            request=request,
        )

    def _timeout_ms(self, timeout: float | timedelta | None) -> int:
        seconds = self._timeout_seconds(timeout)
        if seconds is None:
            return 30_000
        return max(0, int(seconds * 1000))

    async def close(self) -> None:
        return None


@dataclass(slots=True)
class _OnionRequest:
    onion: str
    port: int
    path: str
    url: str
