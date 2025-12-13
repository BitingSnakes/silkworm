from __future__ import annotations

from collections.abc import Iterable, Mapping

# PEP 695: Type Parameter Syntax (Python 3.13+)
type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | dict[str, JSONValue] | list[JSONValue]

type Headers = dict[str, str]
type QueryValue = (
    str | int | float | bool | None | Iterable[str | int | float | bool | None]
)
type QueryParams = dict[str, QueryValue]
type MetaData = dict[str, JSONValue]
type BodyData = (
    bytes
    | bytearray
    | memoryview
    | str
    | Mapping[str, JSONValue]
    | Iterable[tuple[str, str]]
    | list[JSONValue]
    | None
)

__all__ = [
    "BodyData",
    "Headers",
    "JSONScalar",
    "JSONValue",
    "MetaData",
    "QueryParams",
    "QueryValue",
]
