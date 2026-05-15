from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from datetime import timedelta
from importlib import import_module
from typing import TYPE_CHECKING, Any

from .exceptions import HttpError
from .logging import get_logger
from .response import HTMLResponse

if TYPE_CHECKING:
    from .request import Request


SERVO_JAVASCRIPT_META_KEY = "servo_javascript"
SERVO_SETTLE_MS_META_KEY = "servo_settle_ms"
SERVO_USER_AGENT_META_KEY = "servo_user_agent"
SERVO_SCREENSHOT_META_KEY = "servo_screenshot"
SERVO_FULL_PAGE_META_KEY = "servo_full_page"


class ServoFetchClient:
    """
    HTTP client adapter that renders pages with servofetch.

    servofetch embeds Servo and exposes an AsyncBrowser API for fetching rendered
    page HTML. Silkworm keeps this integration opt-in; pass an instance as the
    engine or runner ``http_client``.
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
        try:
            servofetch = import_module("servofetch")
        except ImportError as err:
            msg = (
                "servofetch is not installed. Install a wheel from this page: "
                'https://github.com/RustedBytes/servofetch-py/releases'
            )
            raise ImportError(msg) from err

        try:
            browser_cls = getattr(servofetch, "AsyncBrowser")
        except AttributeError as err:
            msg = (
                "servofetch>=0.1.4 is required for ServoFetchClient. Install a wheel from this page: "
                'https://github.com/RustedBytes/servofetch-py/releases'
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
        timeout_seconds = self._timeout_seconds(timeout)
        if timeout_seconds is not None:
            browser_kwargs["timeout"] = timeout_seconds

        self._browser: Any = browser_cls(**browser_kwargs)
        self._concurrency = concurrency
        self._sem = asyncio.Semaphore(concurrency)
        self._timeout = timeout
        self._settle_ms = settle_ms
        self._user_agent = user_agent
        self._html_max_size_bytes = html_max_size_bytes
        self.logger = get_logger(component="servo")

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def html_max_size_bytes(self) -> int:
        return self._html_max_size_bytes

    async def fetch(self, req: Request) -> HTMLResponse:
        timeout_seconds = self._timeout_seconds(
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
            self.logger.debug("Failed to close Servo browser cleanly", error=str(exc))

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

    def _timeout_seconds(self, timeout: float | timedelta | None) -> float | None:
        if timeout is None:
            return None
        if isinstance(timeout, timedelta):
            return timeout.total_seconds()
        return float(timeout)

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
