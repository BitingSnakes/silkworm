"""Minimal asynchronous Chrome DevTools Protocol rendered-page client."""

from __future__ import annotations

import asyncio
import json
from importlib import import_module
from typing import TYPE_CHECKING, Any

from ._timeouts import to_seconds
from ._validation import require_positive_int
from .exceptions import HttpError
from .logging import Logger, get_logger
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from .request import Request

websockets: Any | None

try:
    websockets = import_module("websockets")
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    websockets = None


class CDPClient:
    """Fetch rendered pages through a CDP-compatible browser.

    Args:
        ws_endpoint: Browser WebSocket endpoint.
        concurrency: Maximum simultaneous page fetches.
        timeout: Default command and navigation timeout in seconds.
        html_max_size_bytes: Maximum rendered document size accepted by the
            HTML parser and WebSocket transport.

    Raises:
        ImportError: If the ``cdp`` extra is not installed.

    Example:
        >>> client = CDPClient(ws_endpoint="ws://127.0.0.1:9222")
        >>> await client.connect()
        >>> try:
        ...     response = await client.fetch(request)
        ... finally:
        ...     await client.close()
    """

    def __init__(
        self,
        *,
        ws_endpoint: str = "ws://127.0.0.1:9222",
        concurrency: int = 16,
        timeout: float | None = None,
        html_max_size_bytes: int = 5_000_000,
    ) -> None:
        require_positive_int(concurrency, "concurrency")
        if not HAS_WEBSOCKETS:
            msg = "websockets package required for CDP support. Install with: pip install silkworm-rs[cdp]"
            raise ImportError(msg)

        self._ws_endpoint = ws_endpoint
        self._concurrency = concurrency
        self._sem = asyncio.Semaphore(concurrency)
        self._timeout = timeout
        self._html_max_size_bytes = html_max_size_bytes
        self._ws: Any | None = None
        self._message_id = 0
        self._pending_responses: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._target_id: str | None = None
        self._session_id: str | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self._page_load_future: asyncio.Future[None] | None = None
        self.logger: Logger = get_logger(component="cdp")

    @property
    def concurrency(self) -> int:
        """Return the maximum number of simultaneous page fetches."""
        return self._concurrency

    @property
    def html_max_size_bytes(self) -> int:
        """Return the rendered HTML size limit in bytes."""
        return self._html_max_size_bytes

    async def connect(self) -> None:
        """Connect to the browser and create an isolated page target.

        Calling this method more than once is harmless.

        Raises:
            HttpError: If connection or target initialization fails.
        """
        if self._ws is not None:
            return

        try:
            if websockets is None:
                raise HttpError(
                    "websockets package required for CDP support. Install with: pip install silkworm-rs[cdp]"
                )
            # Increase max_size so CDP responses (e.g., full HTML) aren't capped at the
            # websockets default of 1 MiB. Use the HTML max size budget as the cap.
            self._ws = await websockets.connect(
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
                    if (
                        method == "Page.loadEventFired"
                        and self._page_load_future
                        and not self._page_load_future.done()
                    ):
                        # Page has finished loading
                        self._page_load_future.set_result(None)

                except json.JSONDecodeError:
                    self.logger.warning(
                        "Received invalid JSON from CDP", json_message=message[:200]
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Error processing CDP message", error=str(exc), exc_info=True
                    )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.logger.exception("CDP receive loop error", error=str(exc))
            self._fail_pending(HttpError(f"CDP connection error: {exc}"))
        finally:
            # If the socket closed unexpectedly, unblock any waiters.
            # websockets' asyncio connection has no ``closed`` attribute; its
            # ``close_code`` stays None until the closing handshake completes.
            close_code = getattr(self._ws, "close_code", None) if self._ws else None
            ws_closed = close_code is not None or bool(
                getattr(self._ws, "closed", False)
            )
            if ws_closed:
                close_reason = getattr(self._ws, "close_reason", None)
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
        except TimeoutError as exc:
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
        """Navigate to a URL and return its rendered HTML.

        The response status is reported as 200 because CDP does not reliably
        expose the navigation status. The final document URL is detected after
        redirects when supported by the browser.

        Raises:
            HttpError: If the client is disconnected, navigation times out, or
                rendered HTML cannot be retrieved.
        """
        if self._ws is None or self._session_id is None:
            raise HttpError("CDP client not connected")

        url = req.url
        timeout_raw = req.timeout if req.timeout is not None else self._timeout
        timeout = to_seconds(timeout_raw)

        async with self._sem:
            start_time = asyncio.get_running_loop().time()

            try:
                # Navigate to the URL
                async with asyncio.timeout(timeout):
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
                    except TimeoutError:
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

            except TimeoutError as exc:
                suffix = f" after {timeout} seconds" if timeout else ""
                raise HttpError(f"CDP request to {url} timed out{suffix}") from exc
            except HttpError:
                raise
            except Exception as exc:
                detail = str(exc)
                suffix = f": {detail}" if detail else ""
                raise HttpError(f"CDP request to {url} failed{suffix}") from exc

    async def close(self) -> None:
        """Cancel background work and close the page target and WebSocket."""
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
                # Best-effort cleanup; the browser may already have dropped the target.
                self.logger.debug("Failed to close CDP target", exc_info=True)

        # Close WebSocket
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                self.logger.debug("Failed to close CDP WebSocket", exc_info=True)
            self._ws = None

        self._target_id = None
        self._session_id = None
        self._pending_responses.clear()
