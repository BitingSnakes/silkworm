from __future__ import annotations

import sys
from datetime import timedelta
from typing import TYPE_CHECKING

try:
    from wreq import Client, Method

    WREQ_AVAILABLE = True
except ImportError:
    Client = None
    Method = None
    WREQ_AVAILABLE = False

from .._resources import close_resource, raise_cleanup_errors
from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class WebhookPipeline:
    """
    Pipeline that sends items to a webhook endpoint using the wreq HTTP client.

    This pipeline uses the same HTTP client (wreq) as the spider itself for
    sending data to webhooks, ensuring consistent behavior and browser impersonation.

    Args:
        url: Webhook endpoint URL.
        method: HTTP method used for deliveries.
        headers: Headers included with every delivery.
        timeout: Per-delivery timeout, either in seconds or as a duration.
        batch_size: Number of items per request. A value of ``1`` sends items
            immediately; a partial final batch is sent by :meth:`close`.

    Example::

        from silkworm.pipelines import WebhookPipeline

        pipeline = WebhookPipeline(
            url="https://webhook.site/unique-id",
            method="POST",
            headers={"Authorization": "Bearer token123"},
        )
    """

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float | timedelta | None = 30.0,
        batch_size: int = 1,
    ) -> None:
        """
        Initialize WebhookPipeline.

        Args:
            url: Webhook endpoint URL
            method: HTTP method (default: "POST")
            headers: Optional HTTP headers to send with each request
            timeout: Request timeout in seconds (default: 30.0)
            batch_size: Number of items to batch before sending (default: 1 for immediate sending)
        """
        if not WREQ_AVAILABLE:
            raise ImportError(
                "wreq is required for WebhookPipeline but appears to be unavailable. "
                "This should not happen as wreq is a core dependency.",
            )

        self.url = url
        self.method = method
        self.headers: dict[str, str] = headers or {}
        self.timeout = timeout
        self.batch_size = batch_size
        self._client: Client | None = None  # type: ignore[name-defined]
        self._batch: list[JSONValue] = []
        self.logger: Logger = get_logger(component="WebhookPipeline")

    async def open(self, spider: Spider) -> None:
        """Create the webhook client and reset the item batch."""
        self._client = Client()  # type: ignore[misc]
        self._batch = []
        self.logger.info(
            "Opened Webhook pipeline",
            url=self.url,
            method=self.method,
            batch_size=self.batch_size,
        )

    async def close(self, spider: Spider) -> None:
        """Send a partial batch and close the webhook client."""
        client = self._client
        self._client = None
        errors: list[BaseException] = []
        try:
            if self._batch:
                # Keep the client visible while the final batch is sent.
                self._client = client
                await self._send_batch()
        except BaseException as exc:  # noqa: BLE001 - close client after cancellation
            errors.append(exc)
        finally:
            self._client = None

        try:
            await close_resource(client)
        except BaseException as exc:  # noqa: BLE001 - preserve flush failure
            errors.append(exc)

        self.logger.info("Closed Webhook pipeline", url=self.url)
        raise_cleanup_errors("Webhook pipeline cleanup failed", errors)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Buffer one item and send when ``batch_size`` is reached."""
        if not self._client:
            raise RuntimeError("WebhookPipeline not opened")

        self._batch.append(item)

        if len(self._batch) >= self.batch_size:
            await self._send_batch()

        return item

    async def _send_batch(self) -> None:
        """Send the current batch of items to the webhook."""
        if not self._batch:
            return
        client = self._client
        if client is None:
            return

        # Prepare payload
        payload = self._batch[0] if len(self._batch) == 1 else self._batch

        response: object | None = None
        try:
            # Use the wreq client to send the request
            method_upper = self.method.upper()
            if not hasattr(Method, method_upper):
                raise ValueError(
                    f"Invalid HTTP method '{self.method}'. Must be one of: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
                )
            method_enum = getattr(Method, method_upper)
            timeout: timedelta | None = None
            if self.timeout is not None:
                timeout = (
                    self.timeout
                    if isinstance(self.timeout, timedelta)
                    else timedelta(seconds=float(self.timeout))
                )
            if timeout is None:
                response = await client.request(
                    method_enum,
                    self.url,
                    headers=self.headers,
                    json=payload,
                )
            else:
                response = await client.request(
                    method_enum,
                    self.url,
                    headers=self.headers,
                    json=payload,
                    timeout=timeout,
                )

            # Try to get status code
            status = getattr(response, "status", None)
            if status is None:
                status = getattr(response, "status_code", None)
            if status is not None and hasattr(status, "value"):
                status = status.value

            log_pipeline_item(
                self,
                "Sent items to webhook",
                url=self.url,
                count=len(self._batch),
                status=status,
            )
        except Exception as exc:
            self.logger.error(
                "Failed to send items to webhook",
                url=self.url,
                count=len(self._batch),
                error=str(exc),
            )
            raise
        finally:
            primary = sys.exception()
            try:
                await close_resource(response)
            except BaseException as cleanup_exc:
                self.logger.debug(
                    "Failed to close webhook response cleanly", exc_info=True
                )
                if primary is None:
                    raise
                primary.add_note(f"Webhook response cleanup failed: {cleanup_exc}")

        # Clear the batch after sending
        self._batch = []
