"""Public type aliases and protocols for annotating silkworm code.

Import from here rather than from private modules::

    from silkworm.types import Callback, JSONValue, Logger

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue: ...
"""

from __future__ import annotations

from ._stats import CrawlResult
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
from .http import FetchClient
from .logging import Logger, LogLevel
from .middlewares import ExceptionMiddleware, RequestMiddleware, ResponseMiddleware
from .pipelines import (
    BatchItemPipeline,
    ItemCallback,
    ItemPipeline,
    ItemSchema,
    ItemValidator,
    ModelSchema,
    ZenohKeyResolver,
)
from .request import Callback, Errback
from .runner import LoopFactory

__all__ = [
    "BatchItemPipeline",
    "BodyData",
    "Callback",
    "CrawlResult",
    "DedupKey",
    "EngineOptions",
    "Errback",
    "ExceptionMiddleware",
    "FetchClient",
    "Headers",
    "ItemCallback",
    "ItemPipeline",
    "ItemSchema",
    "ItemValidator",
    "JSONLike",
    "JSONScalar",
    "JSONValue",
    "LogLevel",
    "Logger",
    "LoopFactory",
    "MetaData",
    "ModelSchema",
    "QueryParams",
    "QueryValue",
    "RequestMiddleware",
    "ResponseMiddleware",
    "ZenohKeyResolver",
]
