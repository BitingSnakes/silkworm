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
from .engine import DedupKey
from .logging import Logger, LogLevel
from .middlewares import ExceptionMiddleware, RequestMiddleware, ResponseMiddleware
from .pipelines import ItemPipeline
from .request import Callback, CallbackOutput, CallbackResult, Errback

__all__ = [
    "BodyData",
    "Callback",
    "CallbackOutput",
    "CallbackResult",
    "DedupKey",
    "Errback",
    "ExceptionMiddleware",
    "Headers",
    "ItemPipeline",
    "JSONLike",
    "JSONScalar",
    "JSONValue",
    "LogLevel",
    "Logger",
    "MetaData",
    "QueryParams",
    "QueryValue",
    "RequestMiddleware",
    "ResponseMiddleware",
]
