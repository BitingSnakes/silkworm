from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from ..logging import Logger, LogLevel, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


type ItemCallback = Callable[
    [JSONValue, Spider], JSONValue | Awaitable[JSONValue | None] | None
]
"""Callback for :class:`CallbackPipeline`; returning ``None`` keeps the item."""


class CallbackPipeline:
    """
    Pipeline that invokes a callback function to process each item.

    This pipeline allows you to define custom item processing logic using a simple
    callback function, making it easy to handle items without creating a full pipeline class.

    The callback function can be either synchronous or asynchronous and receives the item
    and spider as arguments.

    Args:
        callback: Synchronous or asynchronous callable receiving ``(item, spider)``.
            Returning ``None`` preserves the original item; any other result is
            forwarded to the next pipeline.
        log_level: Severity used for per-item processing logs.

    Example:
        from silkworm.pipelines import CallbackPipeline

        def process_item(item, spider):
            # Your custom processing logic
            print(f"Processing item from {spider.name}: {item}")
            return item

        pipeline = CallbackPipeline(callback=process_item)

        # Or with an async callback:
        async def async_process_item(item, spider):
            # Your async processing logic
            await some_async_operation(item)
            return item

        pipeline = CallbackPipeline(callback=async_process_item)
    """

    def __init__(
        self, callback: ItemCallback, *, log_level: LogLevel = "DEBUG"
    ) -> None:
        """
        Initialize CallbackPipeline.

        Args:
            callback: A callable that takes (item, spider) and returns the processed item.
                     Can be either synchronous or asynchronous.
        """
        if not callable(callback):
            msg = "callback must be callable"
            raise TypeError(msg)

        self.callback: ItemCallback = callback
        self.log_level: LogLevel = log_level
        self.logger: Logger = get_logger(component="CallbackPipeline")

    async def open(self, spider: Spider) -> None:
        """Open the pipeline."""
        callback_name = getattr(self.callback, "__name__", str(self.callback))
        self.logger.info("Opened Callback pipeline", callback=callback_name)

    async def close(self, spider: Spider) -> None:
        """Close the pipeline."""
        callback_name = getattr(self.callback, "__name__", str(self.callback))
        self.logger.info("Closed Callback pipeline", callback=callback_name)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Process an item using the callback function."""
        # Sync callbacks return the item; async ones return an awaitable of it.
        result = self.callback(item, spider)
        if inspect.isawaitable(result):
            result = await result

        # If callback returns None, return the original item
        if result is None:
            return item

        log_pipeline_item(
            self,
            "Processed item with callback",
            spider=spider.name,
        )
        return result
