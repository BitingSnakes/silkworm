from __future__ import annotations

import inspect
from datetime import timedelta
from typing import TYPE_CHECKING

try:
    from wreq import Client, Method  # type: ignore[import]

    WREQ_AVAILABLE = True
except ImportError:
    Client = None  # type: ignore
    Method = None  # type: ignore
    WREQ_AVAILABLE = False

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

    Example:
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
        self._client = Client()  # type: ignore[misc]
        self._batch = []
        self.logger.info(
            "Opened Webhook pipeline",
            url=self.url,
            method=self.method,
            batch_size=self.batch_size,
        )

    async def close(self, spider: Spider) -> None:
        # Send any remaining batched items
        if self._batch:
            await self._send_batch()

        if self._client:
            closer = getattr(self._client, "aclose", None) or getattr(
                self._client,
                "close",
                None,
            )
            if closer and callable(closer):
                try:
                    result = closer()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    # Best-effort cleanup; a failed close must not fail the pipeline.
                    self.logger.debug(
                        "Failed to close webhook client cleanly", exc_info=True
                    )
            self._client = None

        self.logger.info("Closed Webhook pipeline", url=self.url)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
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

        try:
            # Use the wreq client to send the request
            method_upper = self.method.upper()
            if not hasattr(Method, method_upper):  # type: ignore[attr-defined]
                raise ValueError(
                    f"Invalid HTTP method '{self.method}'. Must be one of: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
                )
            method_enum = getattr(Method, method_upper)  # type: ignore[attr-defined]
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

            # Close response if possible
            closer = getattr(response, "aclose", None) or getattr(
                response,
                "close",
                None,
            )
            if closer and callable(closer):
                try:
                    result = closer()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    # Best-effort cleanup; a failed close must not fail the pipeline.
                    self.logger.debug(
                        "Failed to close webhook client cleanly", exc_info=True
                    )

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

        # Clear the batch after sending
        self._batch = []
