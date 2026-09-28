from __future__ import annotations

import re
from typing import TYPE_CHECKING, Protocol, cast

from ..logging import Logger, LogLevel, log_at_level

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class _LoggingPipeline(Protocol):
    @property
    def logger(self) -> Logger: ...


class _LevelledPipeline(Protocol):
    log_level: LogLevel


def log_pipeline_item(
    pipeline: _LoggingPipeline,
    message: str,
    **context: object,
) -> None:
    logger = pipeline.logger
    log_level = cast("LogLevel", getattr(pipeline, "log_level", "DEBUG"))
    log_at_level(logger, log_level, message, **context)


def validate_table_name(table: str) -> str:
    """Validate table name to prevent SQL injection."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(
            f"Invalid table name '{table}'. Table names must start with a letter or underscore "
            "and contain only alphanumeric characters and underscores.",
        )
    return table


class ItemPipeline(Protocol):
    """Protocol implemented by ordered item-processing stages."""

    async def open(self, spider: Spider) -> None:
        """Allocate resources once before the first item is processed."""
        ...

    async def close(self, spider: Spider) -> None:
        """Flush buffered data and release resources after the crawl."""
        ...

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Process and return the item passed to the next pipeline."""
        ...


class BatchItemPipeline(ItemPipeline, Protocol):
    """Item pipeline that can process an explicit batch in one call."""

    native_batch: bool

    async def process_items(
        self,
        items: list[JSONValue],
        spider: Spider,
    ) -> list[JSONValue]:
        """Process a batch in order and return items for the next pipeline."""
        ...


class _BatchPipelineMixin:
    """Provide ordered batch processing through a pipeline's single-item API."""

    native_batch = False

    async def process_items(
        self,
        items: list[JSONValue],
        spider: Spider,
    ) -> list[JSONValue]:
        """Process a batch sequentially, preserving single-item semantics."""
        pipeline = cast("ItemPipeline", self)
        return [await pipeline.process_item(item, spider) for item in items]


class LoggedPipeline(_BatchPipelineMixin):
    """
    Wrap any item pipeline and control its per-item log level.

    Use ``log_level=None`` to suppress noisy per-item pipeline messages.
    """

    def __init__(
        self, pipeline: ItemPipeline, *, log_level: LogLevel = "DEBUG"
    ) -> None:
        self.pipeline = pipeline
        self.log_level: LogLevel = log_level
        # Any pipeline may carry a log level; LoggedPipeline controls it.
        cast("_LevelledPipeline", self.pipeline).log_level = log_level

    @property
    def native_batch(self) -> bool:
        """Whether the wrapped pipeline opts into engine-managed batching."""
        return bool(getattr(self.pipeline, "native_batch", False))

    async def open(self, spider: Spider) -> None:
        """Open the wrapped pipeline."""
        await self.pipeline.open(spider)

    async def close(self, spider: Spider) -> None:
        """Close the wrapped pipeline."""
        await self.pipeline.close(spider)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Apply the configured log level and delegate item processing."""
        cast("_LevelledPipeline", self.pipeline).log_level = self.log_level
        return await self.pipeline.process_item(item, spider)

    async def process_items(
        self,
        items: list[JSONValue],
        spider: Spider,
    ) -> list[JSONValue]:
        """Delegate native batching when available, otherwise process in order."""
        cast("_LevelledPipeline", self.pipeline).log_level = self.log_level
        batch_processor = getattr(self.pipeline, "process_items", None)
        if batch_processor is not None:
            return await batch_processor(items, spider)
        return await super().process_items(items, spider)
