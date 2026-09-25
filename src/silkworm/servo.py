"""Servo-backed rendered-page client and request metadata controls."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from datetime import timedelta
from importlib import import_module
from typing import TYPE_CHECKING, Any, Self

from ._timeouts import to_seconds
from ._validation import require_positive_int
from .exceptions import HttpError
from .logging import Logger, get_logger
from .response import HTMLResponse

if TYPE_CHECKING:
    from .request import Request


SERVO_JAVASCRIPT_META_KEY = "servo_javascript"
SERVO_SETTLE_MS_META_KEY = "servo_settle_ms"
SERVO_USER_AGENT_META_KEY = "servo_user_agent"
SERVO_SCREENSHOT_META_KEY = "servo_screenshot"
SERVO_FULL_PAGE_META_KEY = "servo_full_page"


class ServoFetchClient:
    """Render pages with ``servofetch`` for use as an engine HTTP client.

    Args:
        concurrency: Maximum simultaneous renders.
        timeout: Default render timeout.
        settle_ms: Default delay after page load before capture.
        user_agent: Default browser user agent.
        allow_private_addresses: Permit navigation to private network addresses.
        html_max_size_bytes: Maximum rendered document size parsed by selectors.
        onion_bootstrap: Optional Tor bootstrap endpoint forwarded to Servo.
        onion_consensus_file: Optional cached Tor consensus file.
        onion_verbose: Enable verbose Tor integration output.
        onion_response_limit: Maximum Tor response size in bytes.

    Request metadata can override JavaScript, settle delay, user agent, and
    screenshot behavior through the exported ``SERVO_*_META_KEY`` constants.

    Raises:
        ImportError: If a compatible ``servofetch`` build is unavailable.
    """

    def __init__(
        self,
        *,
        concurrency: int = 16,
        timeout: float | timedelta | None = None,
        settle_ms: int = 0,
        user_agent: str | None = None,
        allow_private_addresses: bool = False,
        html_max_size_bytes: int = 5_000_000,
        onion_bootstrap: str | None = None,
        onion_consensus_file: str | None = None,
        onion_verbose: bool = False,
        onion_response_limit: int = 4 * 1024 * 1024,
    ) -> None:
        require_positive_int(concurrency, "concurrency")
        try:
            servofetch = import_module("servofetch")
        except ImportError as err:
            msg = (
                "servofetch is not installed. Install silkworm-rs[servo] "
                "or a wheel from this page: "
                "https://github.com/RustedBytes/servofetch-py/releases"
            )
            raise ImportError(msg) from err

        try:
            browser_cls = servofetch.AsyncBrowser
        except AttributeError as err:
            msg = (
                "servofetch>=0.1.4 is required for ServoFetchClient. Install a wheel from this page: "
                "https://github.com/RustedBytes/servofetch-py/releases"
            )
            raise ImportError(msg) from err

        browser_kwargs: dict[str, object] = {
            "settle_ms": settle_ms,
            "user_agent": user_agent,
            "allow_private_addresses": allow_private_addresses,
            "onion_bootstrap": onion_bootstrap,
            "onion_consensus_file": onion_consensus_file,
            "onion_verbose": onion_verbose,
            "onion_response_limit": onion_response_limit,
        }
        timeout_seconds = to_seconds(timeout)
        if timeout_seconds is not None:
            browser_kwargs["timeout"] = timeout_seconds

        self._browser: Any = browser_cls(**browser_kwargs)
        self._concurrency = concurrency
        self._sem = asyncio.Semaphore(concurrency)
        self._timeout = timeout
        self._settle_ms = settle_ms
        self._user_agent = user_agent
        self._html_max_size_bytes = html_max_size_bytes
        self._closed = False
        self.logger: Logger = get_logger(component="servo")

    @property
    def concurrency(self) -> int:
        """Return the maximum number of simultaneous renders."""
        return self._concurrency

    @property
    def html_max_size_bytes(self) -> int:
        """Return the rendered HTML parsing limit in bytes."""
        return self._html_max_size_bytes

    async def __aenter__(self) -> Self:
        """Return this initialized browser client."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        """Close the browser on context exit."""
        try:
            await self.close()
        except BaseException as cleanup_exc:
            if exc is None:
                raise
            exc.add_note(f"Servo client cleanup failed: {cleanup_exc}")

    async def fetch(self, req: Request) -> HTMLResponse:
        """Render ``req.url`` and return its HTML response.

        Per-request timeout and supported Servo metadata override client
        defaults. Screenshot requests still return the page HTML and expose
        screenshot metadata through synthetic response headers.

        Raises:
            TypeError: If a recognized metadata value has the wrong type.
            HttpError: If rendering fails or no HTML is returned.
        """
        if self._closed:
            raise HttpError("Servo client is closed")

        timeout_seconds = to_seconds(
            req.timeout if req.timeout is not None else self._timeout,
        )
        settle_ms = self._meta_int(req.meta, SERVO_SETTLE_MS_META_KEY, self._settle_ms)
        user_agent = self._meta_str(
            req.meta,
            SERVO_USER_AGENT_META_KEY,
            self._user_agent,
        )
        javascript = self._meta_str(req.meta, SERVO_JAVASCRIPT_META_KEY, None)
        screenshot = self._meta_bool(req.meta, SERVO_SCREENSHOT_META_KEY, False)
        full_page = self._meta_bool(req.meta, SERVO_FULL_PAGE_META_KEY, True)

        start_time = asyncio.get_running_loop().time()
        try:
            async with self._sem:
                if screenshot:
                    page = await self._browser.screenshot(
                        req.url,
                        full_page=full_page,
                        timeout=timeout_seconds,
                        settle_ms=settle_ms,
                        user_agent=user_agent,
                    )
                else:
                    page = await self._browser.fetch(
                        req.url,
                        timeout=timeout_seconds,
                        settle_ms=settle_ms,
                        user_agent=user_agent,
                        javascript=javascript,
                    )
        except Exception as exc:
            detail = str(exc)
            suffix = f": {detail}" if detail else ""
            raise HttpError(f"Servo request to {req.url} failed{suffix}") from exc

        html = self._page_html(page)
        body = html.encode("utf-8")
        final_url = self._page_url(page, req.url)
        headers = self._response_headers(page, screenshot=screenshot)
        elapsed_ms = (asyncio.get_running_loop().time() - start_time) * 1000

        self.logger.debug(
            "Servo response",
            url=final_url,
            elapsed_ms=round(elapsed_ms, 2),
            content_length=len(body),
            screenshot=screenshot,
        )

        return HTMLResponse(
            url=final_url,
            status=200,
            headers=headers,
            body=body,
            request=req,
            doc_max_size_bytes=self._html_max_size_bytes,
        )

    async def close(self) -> None:
        """Close the underlying Servo browser using its available close hook."""
        if self._closed:
            return
        self._closed = True
        closer = getattr(self._browser, "aclose", None) or getattr(
            self._browser,
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
                "Failed to close Servo browser cleanly", error=str(exc), exc_info=True
            )
            raise

    def _response_headers(self, page: object, *, screenshot: bool) -> dict[str, str]:
        headers = {
            "content-type": "text/html; charset=utf-8",
            "x-silkworm-render-engine": "servofetch",
        }
        title = self._clean_header_value(getattr(page, "title", None))
        if title:
            headers["x-silkworm-servo-title"] = title

        if screenshot:
            headers["x-silkworm-servo-screenshot"] = "true"
            screenshot_len = getattr(page, "screenshot_len", None)
            if isinstance(screenshot_len, int):
                headers["x-silkworm-servo-screenshot-len"] = str(screenshot_len)

        return headers

    def _page_html(self, page: object) -> str:
        html = getattr(page, "html", None)
        if isinstance(html, str):
            return html

        msg = "servofetch page did not expose rendered HTML"
        raise HttpError(msg)

    def _page_url(self, page: object, fallback: str) -> str:
        url = getattr(page, "url", None)
        return url if isinstance(url, str) and url else fallback

    def _clean_header_value(self, value: object) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = " ".join(value.split())
        return cleaned[:512] if cleaned else None

    def _meta_str(
        self,
        meta: Mapping[str, object],
        key: str,
        default: str | None,
    ) -> str | None:
        raw = meta.get(key, default)
        if raw is None or isinstance(raw, str):
            return raw
        msg = f"{key} must be a string"
        raise TypeError(msg)

    def _meta_int(
        self,
        meta: Mapping[str, object],
        key: str,
        default: int,
    ) -> int:
        raw = meta.get(key, default)
        if isinstance(raw, int) and not isinstance(raw, bool):
            return raw
        msg = f"{key} must be an integer"
        raise TypeError(msg)

    def _meta_bool(
        self,
        meta: Mapping[str, object],
        key: str,
        default: bool,
    ) -> bool:
        raw = meta.get(key, default)
        if isinstance(raw, bool):
            return raw
        msg = f"{key} must be a boolean"
        raise TypeError(msg)


__all__ = [
    "SERVO_FULL_PAGE_META_KEY",
    "SERVO_JAVASCRIPT_META_KEY",
    "SERVO_SCREENSHOT_META_KEY",
    "SERVO_SETTLE_MS_META_KEY",
    "SERVO_USER_AGENT_META_KEY",
    "ServoFetchClient",
]
