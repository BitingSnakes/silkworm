from __future__ import annotations

import re
from typing import TYPE_CHECKING, Protocol, cast

from ..logging import LogLevel, _Logger, log_at_level

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class _LoggingPipeline(Protocol):
    @property
    def logger(self) -> _Logger: ...


class _LevelledPipeline(Protocol):
    log_level: LogLevel


def _log_pipeline_item(
    pipeline: _LoggingPipeline,
    message: str,
    **context: object,
) -> None:
    logger = pipeline.logger
    log_level = cast("LogLevel", getattr(pipeline, "log_level", "DEBUG"))
    log_at_level(logger, log_level, message, **context)


def _validate_table_name(table: str) -> str:
    """Validate table name to prevent SQL injection."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(
            f"Invalid table name '{table}'. Table names must start with a letter or underscore "
            "and contain only alphanumeric characters and underscores.",
        )
    return table


class ItemPipeline(Protocol):
    async def open(self, spider: Spider) -> None: ...
    async def close(self, spider: Spider) -> None: ...
    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue: ...


class LoggedPipeline:
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

    async def open(self, spider: Spider) -> None:
        await self.pipeline.open(spider)

    async def close(self, spider: Spider) -> None:
        await self.pipeline.close(spider)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        cast("_LevelledPipeline", self.pipeline).log_level = self.log_level
        return await self.pipeline.process_item(item, spider)
