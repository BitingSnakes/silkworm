from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any, AsyncIterator

from .exceptions import HttpError
from .logging import get_logger
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from .request import Request

try:
    import websockets  # type: ignore[import-not-found]
    from websockets.asyncio.client import ClientConnection  # type: ignore[import-not-found]

    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    ClientConnection = Any  # type: ignore[misc,assignment]


class CDPClient:
    """
    Chrome DevTools Protocol client for connecting to Lightpanda or other CDP-compatible browsers.

    Usage:
        client = CDPClient(ws_endpoint="ws://127.0.0.1:9222")
        await client.connect()
        try:
            response = await client.fetch(request)
        finally:
            await client.close()
    """

    def __init__(
        self,
        *,
        ws_endpoint: str = "ws://127.0.0.1:9222",
        concurrency: int = 16,
        timeout: float | None = None,
        html_max_size_bytes: int = 5_000_000,
    ) -> None:
        if not HAS_WEBSOCKETS:
            msg = "websockets package required for CDP support. Install with: pip install silkworm-rs[cdp]"
            raise ImportError(msg)

        self._ws_endpoint = ws_endpoint
        self._concurrency = concurrency
        self._sem = asyncio.Semaphore(concurrency)
        self._timeout = timeout
        self._html_max_size_bytes = html_max_size_bytes
        self._ws: ClientConnection | None = None
        self._message_id = 0
        self._pending_responses: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._target_id: str | None = None
        self._session_id: str | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self._page_load_future: asyncio.Future[None] | None = None
        self.logger = get_logger(component="cdp")

    @property
    def concurrency(self) -> int:
        return self._concurrency

    @property
    def html_max_size_bytes(self) -> int:
        return self._html_max_size_bytes

    async def connect(self) -> None:
        """Establish WebSocket connection to CDP endpoint."""
        if self._ws is not None:
            return

        try:
            # Increase max_size so CDP responses (e.g., full HTML) aren't capped at the
            # websockets default of 1 MiB. Use the HTML max size budget as the cap.
            self._ws = await websockets.connect(  # type: ignore[attr-defined]
                self._ws_endpoint,
                max_size=self._html_max_size_bytes,
            )
        except Exception as exc:
            raise HttpError(
                f"Failed to connect to CDP endpoint {self._ws_endpoint}"
            ) from exc

        # Start background task to receive messages
        self._recv_task = asyncio.create_task(self._receive_loop())

        # Create a new browser context and page
        await self._create_target()

    def _fail_pending(self, exc: Exception) -> None:
        """Fail all pending command futures with the given exception."""
        for future in self._pending_responses.values():
            if not future.done():
                future.set_exception(exc)
        self._pending_responses.clear()

    async def _receive_loop(self) -> None:
        """Background task to receive and dispatch CDP messages."""
        if self._ws is None:
            return

        try:
            async for message in self._ws:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")

                try:
                    data = json.loads(message)

                    # Handle CDP command responses
                    msg_id = data.get("id")
                    if msg_id is not None and msg_id in self._pending_responses:
                        future = self._pending_responses.pop(msg_id)
                        if "error" in data:
                            error_msg = data["error"].get(
                                "message", "Unknown CDP error"
                            )
                            future.set_exception(HttpError(f"CDP error: {error_msg}"))
                        else:
                            future.set_result(data.get("result", {}))

                    # Handle CDP events
                    method = data.get("method")
                    if method == "Page.loadEventFired":
                        # Page has finished loading
                        if self._page_load_future and not self._page_load_future.done():
                            self._page_load_future.set_result(None)

                except json.JSONDecodeError:
                    self.logger.warning(
                        "Received invalid JSON from CDP", json_message=message[:200]
                    )
                except Exception as exc:
                    self.logger.warning("Error processing CDP message", error=str(exc))
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.logger.error("CDP receive loop error", error=str(exc))
            self._fail_pending(HttpError(f"CDP connection error: {exc}"))
        finally:
            # If the socket closed unexpectedly, unblock any waiters.
            ws_closed = bool(getattr(self._ws, "closed", False)) if self._ws else False
            if ws_closed:
                close_reason = getattr(self._ws, "close_reason", None)
                close_code = getattr(self._ws, "close_code", None)
                error_detail = (
                    f" (code={close_code}, reason={close_reason})"
                    if close_code is not None or close_reason
                    else ""
                )
                self._fail_pending(
                    HttpError(f"CDP connection closed unexpectedly{error_detail}")
                )

    async def _send_command(
        self,
        method: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send a CDP command and wait for response."""
        if self._ws is None:
            raise HttpError("CDP client not connected")

        self._message_id += 1
        msg_id = self._message_id

        message = {
            "id": msg_id,
            "method": method,
            "params": params or {},
        }

        if self._session_id:
            message["sessionId"] = self._session_id

        future: asyncio.Future[dict[str, Any]] = asyncio.Future()
        self._pending_responses[msg_id] = future

        try:
            await self._ws.send(json.dumps(message))

            if self._timeout:
                return await asyncio.wait_for(future, timeout=self._timeout)
            return await future
        except asyncio.TimeoutError as exc:
            self._pending_responses.pop(msg_id, None)
            raise HttpError(f"CDP command {method} timed out") from exc
        except Exception as exc:
            self._pending_responses.pop(msg_id, None)
            raise HttpError(f"CDP command {method} failed: {exc}") from exc

    async def _create_target(self) -> None:
        """Create a new browser context and page target."""
        # Create a new target (page)
        result = await self._send_command(
            "Target.createTarget",
            {"url": "about:blank"},
        )
        self._target_id = result.get("targetId")

        if not self._target_id:
            raise HttpError("Failed to create CDP target")

        # Attach to the target to get a session
        result = await self._send_command(
            "Target.attachToTarget",
            {"targetId": self._target_id, "flatten": True},
        )
        self._session_id = result.get("sessionId")

        if not self._session_id:
            raise HttpError("Failed to attach to CDP target")

        # Enable necessary CDP domains
        await self._send_command("Page.enable")
        await self._send_command("Runtime.enable")
        await self._send_command("Network.enable")

    async def fetch(self, req: Request) -> Response:
        """
        Fetch a URL using CDP and return an HTMLResponse.

        This navigates to the URL and waits for the page to load, then extracts
        the HTML content and creates an HTMLResponse.
        """
        if self._ws is None or self._session_id is None:
            raise HttpError("CDP client not connected")

        url = req.url
        timeout_raw = req.timeout if req.timeout is not None else self._timeout
        timeout = self._timeout_seconds(timeout_raw)

        async with self._sem:
            start_time = asyncio.get_running_loop().time()

            try:
                # Navigate to the URL
                async with self._request_timeout(timeout):
                    # Create a future to track page load
                    load_future: asyncio.Future[None] = asyncio.Future()
                    self._page_load_future = load_future

                    await self._send_command(
                        "Page.navigate",
                        {"url": url},
                    )

                    # Wait for page load with fallback timeout
                    # The receive loop will set the load_future when Page.loadEventFired is received
                    try:
                        await asyncio.wait_for(load_future, timeout=timeout or 30.0)
                    except asyncio.TimeoutError:
                        # Page didn't finish loading, but proceed anyway
                        self.logger.debug(
                            "Page load timeout, proceeding with content extraction",
                            url=url,
                        )

                    # Get the document content
                    result = await self._send_command(
                        "Runtime.evaluate",
                        {
                            "expression": "document.documentElement.outerHTML",
                            "returnByValue": True,
                        },
                    )

                    html_content = result.get("result", {}).get("value", "")

                    if not html_content:
                        raise HttpError(f"Failed to retrieve HTML content from {url}")

                    final_url = url

                    # Try to detect the final URL. Some CDP backends (e.g. Lightpanda)
                    # do not implement Page.getNavigationHistory, so fall back to
                    # document.location when the command is unsupported.
                    nav_result: dict[str, Any] | None = None
                    try:
                        nav_result = await self._send_command(
                            "Page.getNavigationHistory"
                        )
                    except HttpError as exc:
                        self.logger.debug(
                            "CDP getNavigationHistory not available; falling back to document.location",
                            error=str(exc),
                            url=url,
                        )

                    if nav_result is not None:
                        current_index = nav_result.get("currentIndex", 0)
                        entries = nav_result.get("entries", [])
                        if entries and current_index < len(entries):
                            final_url = entries[current_index].get("url", url)
                    else:
                        try:
                            location_result = await self._send_command(
                                "Runtime.evaluate",
                                {
                                    "expression": "document.location.href",
                                    "returnByValue": True,
                                },
                            )
                            location_value = location_result.get("result", {}).get(
                                "value"
                            )
                            if isinstance(location_value, str) and location_value:
                                final_url = location_value
                        except HttpError as exc:
                            self.logger.debug(
                                "CDP document.location fallback failed",
                                error=str(exc),
                                url=url,
                            )

                    elapsed_ms = (asyncio.get_running_loop().time() - start_time) * 1000

                    self.logger.debug(
                        "CDP response",
                        url=final_url,
                        elapsed_ms=round(elapsed_ms, 2),
                        content_length=len(html_content),
                    )

                    body = html_content.encode("utf-8")
                    return HTMLResponse(
                        url=final_url,
                        status=200,  # CDP doesn't easily expose HTTP status
                        headers={"content-type": "text/html; charset=utf-8"},
                        body=body,
                        request=req,
                        doc_max_size_bytes=self._html_max_size_bytes,
                    )

            except asyncio.TimeoutError as exc:
                suffix = f" after {timeout} seconds" if timeout else ""
                raise HttpError(f"CDP request to {url} timed out{suffix}") from exc
            except HttpError:
                raise
            except Exception as exc:
                detail = str(exc)
                suffix = f": {detail}" if detail else ""
                raise HttpError(f"CDP request to {url} failed{suffix}") from exc

    def _timeout_seconds(self, timeout: float | timedelta | None) -> float | None:
        """Convert timeout to seconds."""
        if timeout is None:
            return None
        if isinstance(timeout, timedelta):
            return timeout.total_seconds()
        return float(timeout)

    @asynccontextmanager
    async def _request_timeout(self, timeout: float | None) -> AsyncIterator[None]:
        if timeout is None:
            yield
            return
        async with asyncio.timeout(timeout):
            yield

    async def close(self) -> None:
        """Close the CDP connection and cleanup resources."""
        # Cancel receive task
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        # Close the target
        if self._target_id:
            try:
                await self._send_command(
                    "Target.closeTarget",
                    {"targetId": self._target_id},
                )
            except Exception:
                pass

        # Close WebSocket
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        self._target_id = None
        self._session_id = None
        self._pending_responses.clear()
