from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, cast

from ..logging import LogLevel, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class CallbackPipeline:
    """
    Pipeline that invokes a callback function to process each item.

    This pipeline allows you to define custom item processing logic using a simple
    callback function, making it easy to handle items without creating a full pipeline class.

    The callback function can be either synchronous or asynchronous and receives the item
    and spider as arguments.

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

    def __init__(self, callback, *, log_level: LogLevel = "DEBUG") -> None:
        """
        Initialize CallbackPipeline.

        Args:
            callback: A callable that takes (item, spider) and returns the processed item.
                     Can be either synchronous or asynchronous.
        """
        if not callable(callback):
            msg = "callback must be callable"
            raise TypeError(msg)

        self.callback = callback
        self.log_level = log_level
        self.logger = get_logger(component="CallbackPipeline")

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
        # Call the callback - handle both sync and async functions
        if inspect.iscoroutinefunction(self.callback):
            result = await self.callback(item, spider)
        else:
            result = self.callback(item, spider)

        # If callback returns None, return the original item
        if result is None:
            return item

        log_pipeline_item(
            self,
            "Processed item with callback",
            spider=spider.name,
        )
        return cast("JSONValue", result)
