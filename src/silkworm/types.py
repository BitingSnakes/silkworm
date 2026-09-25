"""Public type aliases and protocols for annotating silkworm code.

Import from here rather than from private modules::

    from silkworm.types import Callback, JSONValue, Logger

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue: ...
"""

from __future__ import annotations

from ._types import (
    BodyData,
    Headers,
    JSONLike,
    JSONScalar,
    JSONValue,
    MetaData,
    QueryParams,
    QueryValue,
)
from .engine import DedupKey, EngineOptions
from .logging import Logger, LogLevel
from .middlewares import ExceptionMiddleware, RequestMiddleware, ResponseMiddleware
from .pipelines import ItemCallback, ItemPipeline
from .request import Callback, CallbackOutput, CallbackResult, Errback
from .runner import LoopFactory

__all__ = [
    "BodyData",
    "Callback",
    "CallbackOutput",
    "CallbackResult",
    "DedupKey",
    "EngineOptions",
    "Errback",
    "ExceptionMiddleware",
    "Headers",
    "ItemCallback",
    "ItemPipeline",
    "JSONLike",
    "JSONScalar",
    "JSONValue",
    "LogLevel",
    "Logger",
    "LoopFactory",
    "MetaData",
    "QueryParams",
    "QueryValue",
    "RequestMiddleware",
    "ResponseMiddleware",
]
